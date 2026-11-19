//! Overlay 12 size-bucket columns onto baseline_data_1min day files.
//!
//! One definition, shared with the live gRPC bars (see
//! `cn_futures_l2::size_thresholds`): a trade is classed large / medium / small
//! by `P50` / `P90` of the interval amounts of the DOMINANT contract by trading
//! day (merged across rolls) over the COMPLETE PRECEDING Shanghai calendar month
//! of the trade's own timestamp. Amounts themselves are not adjusted.
//!
//! An explicit `--start` and `--end` are required and only day files inside
//! that range are rewritten: files of earlier days keep whatever rule filled
//! them. Published minutes from 2024 onward use the preceding-month rule
//! where an applicable threshold exists.
//! A product/day whose prior-month thresholds do not exist (new listing, no
//! dominant trades) is left exactly as it is and reported as
//! `MISSING-THRESHOLDS`; thresholds are never invented.
//!
//! `--export-month YYYYMM --product P` only ensures the threshold file of that
//! source month (used by the monthly job); no parquet is touched.

use anyhow::{bail, Context, Result};
use chrono::{Datelike, Duration, NaiveDate, TimeZone};
use chrono_tz::Asia::Shanghai;
use clap::Parser;
use cn_futures_core::segments::trad_day_from_ts;
use cn_futures_l2::baseline_1min::{minute_left_sec, SizeBuckets};
use cn_futures_l2::codec::{decode_key, decode_trade, encode_key, KIND_TRADE};
use cn_futures_l2::db::{
    open_rocksdb_read_only, L2Db, DEFAULT_ROCKSDB_DIR, FORBIDDEN_ROCKSDB_MARK,
};
use cn_futures_l2::export_1min::{read_day_parquet, write_day_parquet};
use cn_futures_l2::export_1s::parse_product_cf;
use cn_futures_l2::size_thresholds::{
    self as thresholds, month_of_ns, prior_month, DEFAULT_DIR, DEFAULT_DOMINANT_DIR,
};
use cn_futures_l2::source::parse_day;
use cn_futures_l2::universe::is_maintained_product;
use rocksdb::{Direction, IteratorMode};
use std::collections::{BTreeSet, HashMap};
use std::fs;
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;

const DEFAULT_ROOT: &str = "/mnt/hdd-raid5-72t/liang_torch/cn_futures_data/baseline_data_1min";

#[derive(Parser, Debug)]
struct Args {
    #[arg(long, default_value = DEFAULT_ROCKSDB_DIR)]
    rocksdb_dir: PathBuf,
    #[arg(long, default_value = DEFAULT_ROOT)]
    root: PathBuf,
    #[arg(long, default_value_t = 4)]
    workers: usize,
    #[arg(long)]
    product: Option<String>,
    /// First TradDay to overlay (YYYY-MM-DD or YYYYMMDD). Required.
    #[arg(long)]
    start: Option<String>,
    /// Last TradDay to overlay. Required; days outside the range are never rewritten.
    #[arg(long)]
    end: Option<String>,
    /// Directory of the shared threshold files (`dominant_by_day/<YYYYMM>/<P>.json`).
    #[arg(long, default_value = DEFAULT_DIR)]
    threshold_dir: PathBuf,
    /// Roll replay results (`<exchange>_dominant.csv`) that define the dominant contract.
    #[arg(long, default_value = DEFAULT_DOMINANT_DIR)]
    dominant_dir: PathBuf,
    /// Only ensure the threshold file whose SOURCE month is this YYYYMM (needs --product).
    #[arg(long)]
    export_month: Option<u32>,
    /// With --export-month: recompute even when a valid file exists.
    #[arg(long)]
    force: bool,
}

fn day_of(path: &Path) -> Option<NaiveDate> {
    NaiveDate::parse_from_str(path.file_stem()?.to_str()?, "%Y%m%d").ok()
}

fn list_product_days(
    root: &Path,
    exchange: &str,
    product: &str,
    range: (NaiveDate, NaiveDate),
) -> Result<Vec<(NaiveDate, PathBuf)>> {
    let dir = root.join(exchange).join(product);
    if !dir.is_dir() {
        return Ok(Vec::new());
    }
    let mut out = Vec::new();
    for entry in fs::read_dir(&dir)? {
        let path = entry?.path();
        if path.extension().and_then(|ext| ext.to_str()) != Some("parquet") {
            continue;
        }
        match day_of(&path) {
            Some(day) if day >= range.0 && day <= range.1 => out.push((day, path)),
            _ => {}
        }
    }
    out.sort();
    Ok(out)
}

fn exchange_of_product(root: &Path, product: &str) -> Result<Option<String>> {
    for exchange in fs::read_dir(root)? {
        let path = exchange?.path();
        if !path.is_dir() {
            continue;
        }
        let Some(exchange) = path.file_name().and_then(|s| s.to_str()) else {
            continue;
        };
        if exchange.starts_with('_') {
            continue;
        }
        if path.join(product).is_dir() {
            return Ok(Some(exchange.to_string()));
        }
    }
    Ok(None)
}

fn midnight_ns(day: NaiveDate) -> u64 {
    Shanghai
        .from_local_datetime(&day.and_hms_opt(0, 0, 0).expect("midnight"))
        .single()
        .expect("Shanghai has no DST")
        .timestamp_nanos_opt()
        .unwrap_or(0)
        .max(0) as u64
}

/// Column families of a product that can hold trades of `[first, last]`
/// (a trade's timestamp is at most 4 days before its trading day).
fn cfs_for(all: &[(i32, String)], first: NaiveDate, last: NaiveDate) -> Vec<String> {
    let lo = (first - Duration::days(4)).year();
    let hi = (last + Duration::days(1)).year();
    let mut names: Vec<String> = all
        .iter()
        .filter(|(y, _)| *y >= lo && *y <= hi)
        .map(|(_, n)| n.clone())
        .collect();
    names.sort();
    names
}

/// Trades of the overlay range, bucketed per (instrument, minute); days whose
/// prior-month thresholds are missing are reported in the second value.
#[allow(clippy::too_many_arguments)]
fn scan_range(
    db: &L2Db,
    product: &str,
    all_cfs: &[(i32, String)],
    range: (NaiveDate, NaiveDate),
    threshold_dir: &Path,
    dominant_dir: &Path,
) -> Result<(
    HashMap<(String, i64), SizeBuckets>,
    HashMap<NaiveDate, BTreeSet<u32>>,
)> {
    let cf_names = cfs_for(all_cfs, range.0, range.1);
    let lo = midnight_ns(range.0 - Duration::days(4));
    let hi = midnight_ns(range.1 + Duration::days(1));
    let mut cache: HashMap<u32, Option<thresholds::Thr>> = HashMap::new();
    let mut buckets: HashMap<(String, i64), SizeBuckets> = HashMap::new();
    let mut missing: HashMap<NaiveDate, BTreeSet<u32>> = HashMap::new();
    for cf_name in &cf_names {
        let cf = db
            .cf_handle(cf_name)
            .ok_or_else(|| anyhow::anyhow!("missing cf {cf_name}"))?;
        let start = encode_key(KIND_TRADE, "", 0, 0)?;
        for item in db.iterator_cf(&cf, IteratorMode::From(&start, Direction::Forward)) {
            let (key, value) = item.context("iterate trade")?;
            let (kind, id, ts_ns, _) = decode_key(&key)?;
            if kind != KIND_TRADE {
                break;
            }
            if ts_ns < lo || ts_ns >= hi {
                continue;
            }
            let day = trad_day_from_ts((ts_ns / 1_000_000_000) as i64);
            if day < range.0 || day > range.1 {
                continue;
            }
            let rec = decode_trade(&value)?;
            let amount = rec.turnover.unwrap_or(rec.price * rec.volume);
            if !(amount.is_finite() && amount > 0.0) {
                continue;
            }
            let source = prior_month(month_of_ns(ts_ns));
            if let std::collections::hash_map::Entry::Vacant(slot) = cache.entry(source) {
                let (first, last) = thresholds::month_days(source)?;
                let ensure_cfs = cfs_for(all_cfs, first, last);
                slot.insert(thresholds::ensure(
                    db,
                    &ensure_cfs,
                    product,
                    source,
                    threshold_dir,
                    dominant_dir,
                    false,
                )?);
            }
            let Some(thr) = cache[&source].as_ref() else {
                missing.entry(day).or_default().insert(source);
                continue;
            };
            buckets
                .entry((id, minute_left_sec(ts_ns)))
                .or_default()
                .add(amount, rec.aggressor, thr.p50, thr.p90);
        }
    }
    Ok((buckets, missing))
}

fn overlay_day(path: &Path, buckets: &HashMap<(String, i64), SizeBuckets>) -> Result<u64> {
    let mut rows = read_day_parquet(path)?;
    if rows.is_empty() {
        return Ok(0);
    }
    let mut hit = 0u64;
    for row in &mut rows {
        let key = (row.contract_id.clone(), row.ts);
        if let Some(sizes) = buckets.get(&key) {
            row.large_order = sizes.large_order;
            row.medium_order = sizes.medium_order;
            row.small_order = sizes.small_order;
            row.large_buy = sizes.large_buy;
            row.large_sell = sizes.large_sell;
            row.medium_buy = sizes.medium_buy;
            row.medium_sell = sizes.medium_sell;
            row.small_buy = sizes.small_buy;
            row.small_sell = sizes.small_sell;
            let (nl, nm, ns) = sizes.nets();
            row.net_buy_large = nl;
            row.net_buy_medium = nm;
            row.net_buy_small = ns;
            hit += 1;
        } else {
            row.large_order = 0.0;
            row.medium_order = 0.0;
            row.small_order = 0.0;
            row.large_buy = 0.0;
            row.large_sell = 0.0;
            row.medium_buy = 0.0;
            row.medium_sell = 0.0;
            row.small_buy = 0.0;
            row.small_sell = 0.0;
            row.net_buy_large = 0.0;
            row.net_buy_medium = 0.0;
            row.net_buy_small = 0.0;
        }
    }
    write_day_parquet(path, &rows)?;
    Ok(hit)
}

fn main() -> Result<()> {
    let args = Args::parse();
    if args.root.to_string_lossy().contains(FORBIDDEN_ROCKSDB_MARK)
        || args
            .rocksdb_dir
            .to_string_lossy()
            .contains(FORBIDDEN_ROCKSDB_MARK)
    {
        bail!("refusing to touch a CME RocksDB path");
    }
    if args.workers == 0 {
        bail!("--workers must be positive");
    }
    let db = open_rocksdb_read_only(&args.rocksdb_dir)?;
    let names = L2Db::list_cf(&rocksdb::Options::default(), &args.rocksdb_dir)?;
    let mut by_product: HashMap<String, Vec<(i32, String)>> = HashMap::new();
    for name in names {
        let Some((year, product)) = parse_product_cf(&name) else {
            continue;
        };
        if !is_maintained_product(&product) {
            continue;
        }
        if let Some(filter) = &args.product {
            if product != filter.to_ascii_uppercase() {
                continue;
            }
        }
        by_product.entry(product).or_default().push((year, name));
    }
    let mut jobs: Vec<(String, Vec<(i32, String)>)> = by_product.into_iter().collect();
    jobs.sort_by(|a, b| a.0.cmp(&b.0));

    if let Some(month) = args.export_month {
        anyhow::ensure!(
            args.product.is_some() && jobs.len() == 1,
            "--export-month requires one explicit product"
        );
        anyhow::ensure!(
            args.start.is_none() && args.end.is_none(),
            "--export-month does not take --start/--end"
        );
        let (first, last) = thresholds::month_days(month)?;
        let today = chrono::Utc::now().with_timezone(&Shanghai).date_naive();
        anyhow::ensure!(last < today, "month {month} is not finished");
        let (product, all_cfs) = &jobs[0];
        let cfs = cfs_for(all_cfs, first, last);
        let thr = thresholds::ensure(
            &db,
            &cfs,
            product,
            month,
            &args.threshold_dir,
            &args.dominant_dir,
            args.force,
        )?
        .with_context(|| format!("no dominant trades for {product} in {month}"))?;
        println!(
            "{}",
            serde_json::json!({"product":product,"source_month":month,"basis":thresholds::BASIS,
                "p50":thr.p50,"p90":thr.p90,"samples":thr.samples,"days":thr.days})
        );
        return Ok(());
    }

    let (Some(start), Some(end)) = (&args.start, &args.end) else {
        bail!(
            "explicit --start and --end are required (only day files inside the range are \
             rewritten; there is no whole-history mode)"
        );
    };
    let range = {
        let (s, e) = (parse_day(start)?, parse_day(end)?);
        if s > e {
            bail!("--start must not be after --end");
        }
        (s, e)
    };

    let db = Arc::new(db);
    let root = Arc::new(args.root);
    let threshold_dir = Arc::new(args.threshold_dir);
    let dominant_dir = Arc::new(args.dominant_dir);
    let (tx, rx) = crossbeam_channel::unbounded::<(String, Vec<(i32, String)>)>();
    for job in jobs {
        tx.send(job).expect("enqueue");
    }
    drop(tx);
    let files = Arc::new(AtomicU64::new(0));
    let hits = Arc::new(AtomicU64::new(0));
    let skipped = Arc::new(AtomicU64::new(0));
    let mut handles = Vec::new();
    for _ in 0..args.workers {
        let rx = rx.clone();
        let db = Arc::clone(&db);
        let root = Arc::clone(&root);
        let threshold_dir = Arc::clone(&threshold_dir);
        let dominant_dir = Arc::clone(&dominant_dir);
        let (files, hits, skipped) = (Arc::clone(&files), Arc::clone(&hits), Arc::clone(&skipped));
        handles.push(std::thread::spawn(move || -> Result<()> {
            while let Ok((product, all_cfs)) = rx.recv() {
                let Some(exchange) = exchange_of_product(&root, &product)? else {
                    eprintln!("skip {product}: no baseline_data_1min directory");
                    continue;
                };
                let days = list_product_days(&root, &exchange, &product, range)?;
                if days.is_empty() {
                    continue;
                }
                let (buckets, missing) = scan_range(
                    &db,
                    &product,
                    &all_cfs,
                    range,
                    &threshold_dir,
                    &dominant_dir,
                )?;
                let mut product_hits = 0u64;
                for (day, path) in &days {
                    if let Some(months) = missing.get(day) {
                        skipped.fetch_add(1, Ordering::Relaxed);
                        eprintln!(
                            "MISSING-THRESHOLDS {exchange} {product} day={day} source_months={months:?} \
                             (file left unchanged)"
                        );
                        continue;
                    }
                    product_hits += overlay_day(path, &buckets)?;
                    files.fetch_add(1, Ordering::Relaxed);
                }
                hits.fetch_add(product_hits, Ordering::Relaxed);
                eprintln!(
                    "{exchange} {product} days={} traded_minutes={} overlay_hits={product_hits}",
                    days.len(),
                    buckets.len()
                );
            }
            Ok(())
        }));
    }
    for handle in handles {
        handle.join().expect("size-bucket worker")?;
    }
    eprintln!(
        "fill_1min_size_buckets ok files={} overlay_hits={} skipped_missing_thresholds={}",
        files.load(Ordering::Relaxed),
        hits.load(Ordering::Relaxed),
        skipped.load(Ordering::Relaxed)
    );
    Ok(())
}
