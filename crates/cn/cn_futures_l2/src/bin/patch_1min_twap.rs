//! Patch only the TWAP column of published CN 1-minute baseline parquet.
//!
//! Raw rows keep their existing grid and all non-TWAP values. A raw TWAP is
//! rewritten only when the RocksDB prints reproduce its published value under
//! the previous definition (uncovered segment-open time weighted as zero);
//! rows whose source prints differ are left untouched and counted. HFQ rows
//! take the patched dominant raw TWAP plus the already-published additive gap,
//! only where that raw TWAP was rewritten.

use anyhow::{bail, Context, Result};
use chrono::NaiveDate;
use clap::Parser;
use cn_futures_l2::baseline_1min::{
    exact_twap_one_bucket, minute_left_sec, PrintTrade, TwapGridState,
};
use cn_futures_l2::codec::{decode_key, decode_trade, encode_key, KIND_TRADE};
use cn_futures_l2::db::{open_rocksdb_read_only, L2Db, DEFAULT_ROCKSDB_DIR};
use cn_futures_l2::export_1min::{read_day_parquet, write_day_parquet};
use cn_futures_l2::export_1s::parse_product_cf;
use cn_futures_l2::hfq::{
    apply_hfq_series, gap_before, index_dominants, instrument_match_key, load_adjustments,
    load_dominants,
};
use cn_futures_l2::source::parse_day;
use std::collections::{BTreeMap, HashMap};
use std::fs;
use std::path::{Path, PathBuf};

const DEFAULT_RAW_ROOT: &str = "/mnt/hdd-raid5-72t/liang_torch/cn_futures_data/baseline_data_1min";
const DEFAULT_HFQ_ROOT: &str =
    "/mnt/hdd-raid5-72t/liang_torch/cn_futures_data/baseline_data_1min_hfq";
const DEFAULT_ROLL_ROOT: &str =
    "/mnt/nvme-raid0-28t/fanghaizhou/cn_roll_replay/2019_20260826/result_no_rollback";

#[derive(Parser, Debug)]
#[command(about = "Patch CN raw and HFQ 1-minute TWAP columns into staging roots")]
struct Args {
    #[arg(long, default_value = DEFAULT_ROCKSDB_DIR)]
    rocksdb_dir: PathBuf,
    #[arg(long, default_value = DEFAULT_RAW_ROOT)]
    raw_input_root: PathBuf,
    #[arg(long)]
    raw_output_root: PathBuf,
    #[arg(long, default_value = DEFAULT_HFQ_ROOT)]
    hfq_input_root: PathBuf,
    #[arg(long)]
    hfq_output_root: PathBuf,
    #[arg(long, default_value = DEFAULT_ROLL_ROOT)]
    roll_root: PathBuf,
    #[arg(long)]
    start: String,
    #[arg(long)]
    end: String,
    #[arg(long)]
    product: Option<String>,
    #[arg(long)]
    overwrite: bool,
    #[arg(long)]
    dry_run: bool,
}

#[derive(Clone)]
struct DayFile {
    day: NaiveDate,
    path: PathBuf,
}

fn main() -> Result<()> {
    let args = Args::parse();
    let start = parse_day(&args.start)?;
    let end = parse_day(&args.end)?;
    if end < start {
        bail!("end precedes start");
    }
    for (label, input, output) in [
        ("raw", &args.raw_input_root, &args.raw_output_root),
        ("hfq", &args.hfq_input_root, &args.hfq_output_root),
    ] {
        if input == output {
            bail!("{label} input and output roots must differ");
        }
        if output.to_string_lossy().contains("cme_tas_rocksdb") {
            bail!("refusing CME RocksDB output root {}", output.display());
        }
    }
    let products = args.product.as_deref().map(parse_products).transpose()?;
    let db = open_rocksdb_read_only(&args.rocksdb_dir)?;
    let cf_names = L2Db::list_cf(&rocksdb::Options::default(), &args.rocksdb_dir)?;
    let raw = patch_raw(&args, &db, &cf_names, start, end, products.as_deref())?;
    let hfq = patch_hfq(&args, start, end, products.as_deref())?;
    eprintln!(
        "patch_1min_twap complete raw_files={} raw_changed={} hfq_files={} hfq_changed={} dry_run={}",
        raw.0, raw.1, hfq.0, hfq.1, args.dry_run
    );
    Ok(())
}

fn parse_products(text: &str) -> Result<Vec<String>> {
    let values = text
        .split(',')
        .map(|value| value.trim().to_ascii_uppercase())
        .filter(|value| !value.is_empty())
        .collect::<Vec<_>>();
    if values.is_empty() {
        bail!("--product contains no product names");
    }
    Ok(values)
}

fn list_days(root: &Path, exchange: &str, product: &str) -> Result<Vec<DayFile>> {
    let dir = root.join(exchange).join(product);
    let mut files = Vec::new();
    if !dir.is_dir() {
        return Ok(files);
    }
    for entry in fs::read_dir(&dir).with_context(|| format!("read {}", dir.display()))? {
        let path = entry?.path();
        if path.extension().and_then(|value| value.to_str()) != Some("parquet") {
            continue;
        }
        let stem = path
            .file_stem()
            .and_then(|value| value.to_str())
            .unwrap_or("");
        if let Ok(day) = NaiveDate::parse_from_str(stem, "%Y%m%d") {
            files.push(DayFile { day, path });
        }
    }
    files.sort_by_key(|file| file.day);
    Ok(files)
}

fn product_jobs(root: &Path, filter: Option<&[String]>) -> Result<Vec<(String, String)>> {
    let mut jobs = Vec::new();
    for exchange in fs::read_dir(root).with_context(|| format!("read {}", root.display()))? {
        let exchange = exchange?.path();
        if !exchange.is_dir() {
            continue;
        }
        let Some(exchange_name) = exchange.file_name().and_then(|value| value.to_str()) else {
            continue;
        };
        if exchange_name.starts_with('_') {
            continue;
        }
        for product in fs::read_dir(&exchange)? {
            let product = product?.path();
            if !product.is_dir() {
                continue;
            }
            let Some(product_name) = product.file_name().and_then(|value| value.to_str()) else {
                continue;
            };
            if filter.is_some_and(|values| !values.iter().any(|value| value == product_name)) {
                continue;
            }
            jobs.push((exchange_name.to_string(), product_name.to_string()));
        }
    }
    jobs.sort();
    Ok(jobs)
}

fn scan_trades(
    db: &L2Db,
    cf_names: &[String],
    product: &str,
    contract_id: &str,
    start: i64,
    end_exclusive: i64,
) -> Result<Vec<PrintTrade>> {
    let start_ns = u64::try_from(start)?.saturating_mul(1_000_000_000);
    let end_ns = u64::try_from(end_exclusive)?.saturating_mul(1_000_000_000);
    let mut trades = Vec::new();
    for name in cf_names {
        let Some((_, cf_product)) = parse_product_cf(name) else {
            continue;
        };
        if cf_product != product {
            continue;
        }
        let cf = db
            .cf_handle(name)
            .with_context(|| format!("missing column family {name}"))?;
        let key = encode_key(KIND_TRADE, contract_id, start_ns, 0)?;
        for item in db.iterator_cf(
            &cf,
            rocksdb::IteratorMode::From(&key, rocksdb::Direction::Forward),
        ) {
            let (key, value) = item?;
            let (kind, id, ts_ns, _) = decode_key(&key)?;
            if kind != KIND_TRADE || id != contract_id {
                break;
            }
            if ts_ns >= end_ns {
                break;
            }
            trades.push(PrintTrade::from(&decode_trade(&value)?));
        }
    }
    Ok(trades)
}

/// TWAP of one grid minute under the current definition and under the
/// previous one, which divided an unseeded minute by the full bucket.
#[derive(Clone, Copy, Debug, PartialEq)]
struct GridTwap {
    current: Option<f64>,
    previous: Option<f64>,
}

/// Same grid walk and carry as `twap_for_existing_grid`, returning both
/// definitions from the same prints.
fn grid_twaps(
    minutes: &[i64],
    trades: &[PrintTrade],
    state: &mut TwapGridState,
) -> Result<Vec<GridTwap>> {
    let mut by_minute: BTreeMap<i64, (Vec<i64>, Vec<f64>)> = BTreeMap::new();
    for trade in trades {
        let entry = by_minute
            .entry(minute_left_sec(trade.ts_utc_ns))
            .or_default();
        entry.0.push((trade.ts_utc_ns / 1_000) as i64);
        entry.1.push(trade.price);
    }
    let mut out = Vec::with_capacity(minutes.len());
    for &ts in minutes {
        if ts.rem_euclid(60) != 0 {
            bail!("TWAP grid minute {ts} is not minute aligned");
        }
        if let Some(previous) = state.last_minute {
            if ts <= previous {
                bail!("TWAP grid is not strictly increasing: {previous} then {ts}");
            }
            if ts != previous + 60 {
                state.last_price = None;
            }
        }
        let start_us = ts * 1_000_000;
        let end_us = (ts + 60) * 1_000_000;
        let (timestamps_us, prices) = by_minute
            .get(&ts)
            .map(|(t, p)| (t.as_slice(), p.as_slice()))
            .unwrap_or((&[], &[]));
        let current =
            exact_twap_one_bucket(timestamps_us, prices, start_us, end_us, state.last_price)?;
        let mut valid = timestamps_us
            .iter()
            .zip(prices)
            .filter(|(t, p)| (start_us..end_us).contains(*t) && p.is_finite() && **p > 0.0)
            .map(|(t, p)| (*t, *p))
            .collect::<Vec<_>>();
        valid.sort_by_key(|(t, _)| *t);
        let previous = match (current, state.last_price, valid.first()) {
            (Some(value), None, Some(&(first_us, _))) => {
                Some(value * (end_us - first_us) as f64 / (end_us - start_us) as f64)
            }
            _ => current,
        };
        if let Some(&(_, price)) = valid.last() {
            state.last_price = Some(price);
        }
        state.last_minute = Some(ts);
        out.push(GridTwap { current, previous });
    }
    Ok(out)
}

/// The published value was produced from these same prints.
fn reproduced(published: Option<f64>, previous: Option<f64>) -> bool {
    match (published, previous) {
        (None, None) => true,
        (Some(a), Some(b)) => (a - b).abs() <= 1e-9 * b.abs().max(1.0),
        _ => false,
    }
}

fn patch_raw(
    args: &Args,
    db: &L2Db,
    cf_names: &[String],
    start: NaiveDate,
    end: NaiveDate,
    filter: Option<&[String]>,
) -> Result<(u64, u64)> {
    let mut files = 0u64;
    let mut changed = 0u64;
    for (exchange, product) in product_jobs(&args.raw_input_root, filter)? {
        let mut states = HashMap::<String, TwapGridState>::new();
        for file in list_days(&args.raw_input_root, &exchange, &product)? {
            if file.day > end {
                continue;
            }
            let mut rows = read_day_parquet(&file.path)?;
            let mut grids = BTreeMap::<String, Vec<(usize, i64)>>::new();
            for (index, row) in rows.iter().enumerate() {
                grids
                    .entry(row.contract_id.clone())
                    .or_default()
                    .push((index, row.ts));
            }
            let mut file_changed = 0u64;
            let mut file_kept = 0u64;
            for (contract_id, mut grid) in grids {
                grid.sort_by_key(|(_, ts)| *ts);
                let minutes = grid.iter().map(|(_, ts)| *ts).collect::<Vec<_>>();
                let first = *minutes.first().context("empty TWAP grid")?;
                let last = *minutes.last().context("empty TWAP grid")?;
                let trades = scan_trades(db, cf_names, &product, &contract_id, first, last + 60)?;
                let values = grid_twaps(&minutes, &trades, states.entry(contract_id).or_default())?;
                for ((index, _), value) in grid.into_iter().zip(values) {
                    if !reproduced(rows[index].twap, value.previous) {
                        file_kept += 1;
                    } else if rows[index].twap != value.current {
                        file_changed += 1;
                        rows[index].twap = value.current;
                    }
                }
            }
            if file.day >= start {
                let relative = file.path.strip_prefix(&args.raw_input_root)?;
                let output = args.raw_output_root.join(relative);
                if output.exists() && !args.overwrite {
                    bail!("refusing to overwrite {}", output.display());
                }
                if !args.dry_run {
                    write_day_parquet(&output, &rows)?;
                }
                files += 1;
                changed += file_changed;
                eprintln!(
                    "raw_twap {} {} {} rows={} changed={file_changed} kept_unreproduced={file_kept}",
                    exchange,
                    product,
                    file.day,
                    rows.len()
                );
            } else {
                eprintln!(
                    "raw_twap_warmup {} {} {} rows={}",
                    exchange,
                    product,
                    file.day,
                    rows.len()
                );
            }
        }
    }
    Ok((files, changed))
}

fn patch_hfq(
    args: &Args,
    start: NaiveDate,
    end: NaiveDate,
    filter: Option<&[String]>,
) -> Result<(u64, u64)> {
    let mut files = 0u64;
    let mut changed = 0u64;
    for (exchange, product) in product_jobs(&args.hfq_input_root, filter)? {
        let dominant_path = args.roll_root.join(format!("{exchange}_dominant.csv"));
        let adjustment_path = args
            .roll_root
            .join(format!("{exchange}_adjustment_factor.csv"));
        let dominants = index_dominants(&load_dominants(&dominant_path)?, &product);
        let adjustments = load_adjustments(&adjustment_path)?
            .into_iter()
            .filter(|row| row.product_id == product)
            .collect::<Vec<_>>();
        for file in list_days(&args.hfq_input_root, &exchange, &product)? {
            if file.day < start || file.day > end {
                continue;
            }
            let dominant = dominants
                .get(&file.day)
                .with_context(|| format!("missing dominant {exchange} {product} {}", file.day))?;
            let gap = gap_before(&adjustments, file.day)?.with_context(|| {
                format!("missing adjustment path {exchange} {product} {}", file.day)
            })?;
            let raw_path = args
                .raw_output_root
                .join(&exchange)
                .join(&product)
                .join(file.path.file_name().context("HFQ path has no file name")?);
            let dominant_rows = |path: &Path| -> Result<Vec<_>> {
                let mut rows = read_day_parquet(path)
                    .with_context(|| format!("read raw {}", path.display()))?
                    .into_iter()
                    .filter(|row| {
                        instrument_match_key(&row.contract_id, file.day)
                            == instrument_match_key(dominant, file.day)
                    })
                    .collect::<Vec<_>>();
                rows.sort_by_key(|row| row.ts);
                Ok(rows)
            };
            let published: BTreeMap<_, _> = dominant_rows(
                &args
                    .raw_input_root
                    .join(&exchange)
                    .join(&product)
                    .join(file.path.file_name().context("HFQ path has no file name")?),
            )?
            .into_iter()
            .map(|row| (row.ts, row.twap))
            .collect();
            let raw = dominant_rows(&raw_path)?;
            // Only a rewritten raw TWAP replaces the published HFQ value.
            let rewritten: BTreeMap<_, _> = raw
                .iter()
                .map(|row| (row.ts, published.get(&row.ts) != Some(&row.twap)))
                .collect();
            let raw_by_ts = apply_hfq_series(raw, gap)
                .into_iter()
                .map(|row| (row.ts, row.twap))
                .collect::<BTreeMap<_, _>>();
            let mut rows = read_day_parquet(&file.path)?;
            let mut file_changed = 0u64;
            for row in &mut rows {
                let value = raw_by_ts.get(&row.ts).with_context(|| {
                    format!(
                        "missing patched raw TWAP at {} {} {}",
                        exchange, product, row.ts
                    )
                })?;
                if rewritten.get(&row.ts) == Some(&true) && row.twap != *value {
                    file_changed += 1;
                    row.twap = *value;
                }
            }
            if raw_by_ts.len() != rows.len() {
                bail!(
                    "HFQ/raw row count mismatch {exchange} {product} {}: {} != {}",
                    file.day,
                    rows.len(),
                    raw_by_ts.len()
                );
            }
            let relative = file.path.strip_prefix(&args.hfq_input_root)?;
            let output = args.hfq_output_root.join(relative);
            if output.exists() && !args.overwrite {
                bail!("refusing to overwrite {}", output.display());
            }
            if !args.dry_run {
                write_day_parquet(&output, &rows)?;
            }
            files += 1;
            changed += file_changed;
            eprintln!(
                "hfq_twap {} {} {} rows={} changed={file_changed}",
                exchange,
                product,
                file.day,
                rows.len()
            );
        }
    }
    Ok((files, changed))
}

#[cfg(test)]
mod tests {
    use super::*;

    fn print(second: u64, nanos: u64, price: f64) -> PrintTrade {
        PrintTrade {
            ts_utc_ns: second * 1_000_000_000 + nanos,
            price,
            volume: 1.0,
            amount: price,
            aggressor: 1,
        }
    }

    #[test]
    fn previous_definition_is_reproduced_only_for_unseeded_minutes() {
        // Segment open at 0 with the first print 0.5 s in, then a seeded minute.
        let trades = [print(0, 500_000_000, 100.0), print(70, 0, 101.0)];
        let mut state = TwapGridState::default();
        let values = grid_twaps(&[0, 60], &trades, &mut state).unwrap();
        assert_eq!(values[0].current, Some(100.0));
        assert!((values[0].previous.unwrap() - 100.0 * 59.5 / 60.0).abs() < 1e-9);
        assert_eq!(values[1].current, values[1].previous);
        assert!((values[1].current.unwrap() - (100.0 * 10.0 + 101.0 * 50.0) / 60.0).abs() < 1e-9);
        assert_eq!(state.last_price, Some(101.0));
        // A grid gap resets the carry: the minute after it is unseeded again.
        let values = grid_twaps(&[180], &[print(190, 0, 102.0)], &mut state).unwrap();
        assert_eq!(values[0].current, Some(102.0));
        assert!((values[0].previous.unwrap() - 102.0 * 50.0 / 60.0).abs() < 1e-9);
    }

    #[test]
    fn only_reproduced_published_values_are_rewritten() {
        assert!(reproduced(None, None));
        assert!(reproduced(
            Some(99.166_666_666_7),
            Some(99.166_666_666_666_67)
        ));
        assert!(!reproduced(Some(99.0), None));
        assert!(!reproduced(None, Some(99.0)));
        assert!(!reproduced(Some(98.0), Some(99.0)));
    }
}
