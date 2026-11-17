//! Fixed-frequency downsampling of LSEG Normalized LL2 daily packages to Parquet.
//! Rules: `docs/lseg/l2_slice.md`.
use anyhow::{bail, ensure, Context, Result};
use clap::{Parser, Subcommand};
use flate2::read::MultiGzDecoder;
use lseg_delivery::{
    l2_slice::{day_number, slice_stream, Layout, Params, ParquetSink, DAY_NS},
    limits,
};
use serde_json::json;
use std::{
    collections::BTreeMap,
    fs::File,
    io::BufReader,
    path::{Path, PathBuf},
    time::Instant,
};

#[derive(Parser)]
#[command(about = "LSEG Normalized LL2 daily packages -> fixed-frequency downsampled Parquet")]
struct Args {
    #[command(subcommand)]
    command: Command,
}
#[derive(Subcommand)]
enum Command {
    /// Slice `<raw-root>/<PRODUCT>/<PRODUCT>_<day>_normll2.csv.gz` for products x days.
    Run {
        #[arg(
            long,
            default_value = "/mnt/hdd-raid5-72t/liang_torch/lseg_data/future_daily/normalisedLL2"
        )]
        raw_root: PathBuf,
        /// Output root with the delivery layout `<exchange>/<UTC year>/<RIC>/<UTC day>/l2_<freq>ms.parquet`.
        #[arg(long)]
        out_root: PathBuf,
        /// Lines `RIC<TAB>exchange-directory`; RICs missing here need --default-exchange.
        #[arg(long)]
        exchange_map: Option<PathBuf>,
        #[arg(long)]
        default_exchange: Option<String>,
        #[arg(long, value_delimiter = ',', required = true)]
        products: Vec<String>,
        /// UTC days `YYYY-MM-DD`.
        #[arg(long, value_delimiter = ',', required = true)]
        days: Vec<String>,
        /// Grid step in milliseconds; must divide one UTC day.
        #[arg(long, default_value_t = 250)]
        freq_ms: u64,
        /// Silence (seconds) after which the next update is a session anchor.
        #[arg(long, default_value_t = 1800)]
        session_gap_s: u64,
        /// Files processed at the same time (fixed pool, 1..=8; default 2). More than 2
        /// is for one-off backfills only (limits::L2_SLICE_WORKERS, docs/lseg/l2_slice.md).
        #[arg(long, default_value_t = limits::AGGREGATION_WORKERS)]
        workers: usize,
        /// Replace existing output files (default: keep them).
        #[arg(long)]
        force: bool,
    },
}

fn load_map(path: &Path) -> Result<BTreeMap<String, String>> {
    let mut map = BTreeMap::new();
    for (n, line) in std::fs::read_to_string(path)?.lines().enumerate() {
        if line.trim().is_empty() || line.starts_with('#') {
            continue;
        }
        let (ric, ex) = line
            .split_once('\t')
            .with_context(|| format!("{}:{}: expected RIC<TAB>exchange", path.display(), n + 1))?;
        if let Some(old) = map.insert(ric.trim().to_string(), ex.trim().to_string()) {
            ensure!(old == ex.trim(), "conflicting exchange for RIC {ric}");
        }
    }
    Ok(map)
}

fn peak_rss_mib() -> u64 {
    std::fs::read_to_string("/proc/self/status")
        .ok()
        .and_then(|s| {
            s.lines().find(|l| l.starts_with("VmHWM:")).and_then(|l| {
                l.split_whitespace()
                    .nth(1)
                    .and_then(|v| v.parse::<u64>().ok())
            })
        })
        .map_or(0, |kb| kb / 1024)
}

#[allow(clippy::too_many_arguments)]
fn one(
    raw_root: &Path,
    out_root: &Path,
    map: &BTreeMap<String, String>,
    default_exchange: &Option<String>,
    p: Params,
    product: &str,
    day: &str,
    force: bool,
) -> Result<serde_json::Value> {
    let name = format!("{product}_{day}_normll2.csv.gz");
    let path = raw_root.join(product).join(&name);
    if !path.exists() {
        return Ok(json!({"product": product, "day": day, "status": "missing_source"}));
    }
    let started = Instant::now();
    let day_start = day_number(day)? * DAY_NS;
    let layout = Layout {
        out_root: out_root.to_path_buf(),
        exchange_of: map.clone(),
        default_exchange: default_exchange.clone(),
        day: day.to_string(),
        force,
    };
    let bytes_in = std::fs::metadata(&path)?.len();
    let reader = BufReader::with_capacity(1 << 20, MultiGzDecoder::new(File::open(&path)?));
    let mut sink = ParquetSink::new(&layout, p, day_start);
    let stats = slice_stream(reader, day_start, &p, &mut sink)
        .with_context(|| format!("{}", path.display()))?;
    let grid_day = (DAY_NS / p.freq_ns) as u64;
    let updates: u64 = stats.iter().map(|s| s.updates).sum();
    let buckets: u64 = stats.iter().map(|s| s.buckets).sum();
    let emitted: u64 = stats.iter().map(|s| s.emitted).sum();
    let anchors: u64 = stats.iter().map(|s| s.anchors).sum();
    let sessions: u64 = stats.iter().map(|s| s.sessions).sum();
    let active: u64 = stats.iter().map(|s| s.active_grid_points).sum();
    let out_bytes: u64 = sink.outputs.iter().map(|o| o.bytes).sum();
    let out_rows: u64 = sink.outputs.iter().map(|o| o.rows).sum();
    Ok(json!({
        "product": product, "day": day, "status": "ok",
        "rics": stats.len(), "written": sink.outputs.len(), "kept_existing": sink.skipped.len(),
        "input_gz_bytes": bytes_in, "updates": updates, "nonempty_grid_points": buckets,
        "emitted_rows": emitted, "anchor_rows": anchors, "sessions": sessions,
        "written_rows": out_rows, "parquet_bytes": out_bytes,
        "active_grid_points": active, "full_day_grid_points": grid_day * stats.len() as u64,
        "emitted_per_active_grid": if active > 0 { emitted as f64 / active as f64 } else { 0.0 },
        "emitted_per_full_day_grid": emitted as f64 / (grid_day * stats.len().max(1) as u64) as f64,
        "seconds": (started.elapsed().as_secs_f64() * 100.0).round() / 100.0,
        "max_rows_per_ric": stats.iter().map(|s| s.emitted).max().unwrap_or(0),
    }))
}

fn main() -> Result<()> {
    let args = Args::parse();
    match args.command {
        Command::Run {
            raw_root,
            out_root,
            exchange_map,
            default_exchange,
            products,
            days,
            freq_ms,
            session_gap_s,
            workers,
            force,
        } => {
            ensure!(
                (1..=limits::L2_SLICE_WORKERS).contains(&workers),
                "--workers must be 1..={}",
                limits::L2_SLICE_WORKERS
            );
            let p = Params::new(freq_ms, session_gap_s)?;
            let map = match &exchange_map {
                Some(m) => load_map(m)?,
                None => BTreeMap::new(),
            };
            ensure!(
                !map.is_empty() || default_exchange.is_some(),
                "give --exchange-map or --default-exchange"
            );
            for d in &days {
                day_number(d)?;
            }
            let jobs: Vec<(String, String)> = products
                .iter()
                .flat_map(|pr| days.iter().map(move |d| (pr.clone(), d.clone())))
                .collect();
            let pool = rayon::ThreadPoolBuilder::new()
                .num_threads(workers)
                .build()
                .context("thread pool")?;
            let started = Instant::now();
            let results: Vec<Result<serde_json::Value>> = pool.install(|| {
                use rayon::prelude::*;
                jobs.par_iter()
                    .map(|(pr, d)| {
                        let r = one(
                            &raw_root,
                            &out_root,
                            &map,
                            &default_exchange,
                            p,
                            pr,
                            d,
                            force,
                        );
                        if let Ok(v) = &r {
                            println!("{v}");
                        }
                        r.map_err(|e| e.context(format!("{pr} {d}")))
                    })
                    .collect()
            });
            let failed: Vec<String> = results
                .iter()
                .filter_map(|r| r.as_ref().err().map(|e| format!("{e:#}")))
                .collect();
            println!(
                "{}",
                json!({"summary": true, "jobs": jobs.len(), "failed": failed.len(),
                       "seconds": started.elapsed().as_secs(), "peak_rss_mib": peak_rss_mib(),
                       "workers": workers, "freq_ms": freq_ms})
            );
            for f in &failed {
                eprintln!("FAILED: {f}");
            }
            if !failed.is_empty() {
                bail!("{} of {} product-days failed", failed.len(), jobs.len());
            }
            Ok(())
        }
    }
}
