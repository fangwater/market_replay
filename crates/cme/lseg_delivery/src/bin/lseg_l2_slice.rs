//! Fixed-frequency downsampling of LSEG Normalized LL2 daily packages to Parquet.
//! Rules: `docs/lseg/l2_slice.md`.
use anyhow::{bail, ensure, Context, Result};
use clap::{Parser, Subcommand};
use flate2::{bufread::GzDecoder, read::MultiGzDecoder};
use lseg_delivery::{
    l2_batch::{run_batch, BatchConfig},
    l2_extract::{run_extract, ExtractConfig},
    l2_slice::{day_number, slice_stream, Layout, NullSink, Params, ParquetSink, DAY_NS},
    limits,
};
use serde_json::json;
use std::{
    collections::BTreeMap,
    fs::File,
    io::{BufRead, BufReader, Read},
    path::{Path, PathBuf},
    sync::Arc,
    time::Instant,
};

const DEFAULT_DB_ROOT: &str = "/mnt/nvme-raid0-28t/fanghaizhou/lseg_data/runtime/l2_db";

#[derive(Parser)]
#[command(about = "LSEG Normalized LL2 daily packages -> fixed-frequency downsampled Parquet")]
struct Args {
    #[command(subcommand)]
    command: Command,
}
#[derive(Subcommand)]
enum Command {
    /// Stages 1+2: decompress `<raw-root>/<PRODUCT>/<PRODUCT>_<day>_normll2.csv.gz`
    /// once into RIC-aligned zstd shards and wash them into the day RocksDB
    /// (every update, no frequency). Shards are deleted as soon as they are washed.
    Wash {
        #[arg(
            long,
            default_value = "/mnt/hdd-raid5-72t/liang_torch/lseg_data/future_daily/normalisedLL2"
        )]
        raw_root: PathBuf,
        /// One RocksDB per UTC day below this root.
        #[arg(long, default_value = DEFAULT_DB_ROOT)]
        db_root: PathBuf,
        /// Temporary shards (NVMe).
        #[arg(
            long,
            default_value = "/mnt/nvme-raid0-28t/fanghaizhou/lseg_data/runtime/l2_shards"
        )]
        shard_root: PathBuf,
        #[arg(long, value_delimiter = ',', required = true)]
        products: Vec<String>,
        /// UTC days `YYYY-MM-DD`.
        #[arg(long, value_delimiter = ',', required = true)]
        days: Vec<String>,
        /// Threads in total, both stages share them (fixed pool, 1..=8; default 2).
        /// More than 2 is for one-off backfills only (limits::L2_SLICE_WORKERS,
        /// docs/lseg/l2_slice.md).
        #[arg(long, default_value_t = limits::AGGREGATION_WORKERS)]
        workers: usize,
        /// A shard holds at least this many rows and ends at the next RIC boundary.
        #[arg(long, default_value_t = 4_000_000)]
        shard_rows: u64,
        #[arg(long, default_value_t = 1)]
        zstd_level: i32,
        /// Finished-but-unwashed shards never exceed this many GiB (compressed).
        #[arg(long, default_value_t = 16)]
        shard_budget_gib: u64,
        /// Wash again although the product-day already has its completion marker.
        #[arg(long)]
        force: bool,
    },
    /// Stage 3: slices of one frequency from the washed day databases.
    Extract {
        #[arg(long, default_value = DEFAULT_DB_ROOT)]
        db_root: PathBuf,
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
        #[arg(long, value_delimiter = ',', required = true)]
        days: Vec<String>,
        /// Grid step in milliseconds; must divide one UTC day.
        #[arg(long, default_value_t = 250)]
        freq_ms: u64,
        /// Silence (seconds) after which the next update is a session anchor.
        #[arg(long, default_value_t = 1800)]
        session_gap_s: u64,
        #[arg(long, default_value_t = limits::AGGREGATION_WORKERS)]
        workers: usize,
        /// Block cache shared by all open day databases.
        #[arg(long, default_value_t = 256)]
        cache_mb: usize,
        /// Replace existing output files (default: keep them).
        #[arg(long)]
        force: bool,
    },
    /// Single-threaded cost of one source file, stage by stage (each stage is a
    /// separate pass; differences give the share of each step):
    /// 0 gunzip only, 1 + line split, 2 + time stamp/bucket logic (no book parse),
    /// 3 + book parse and compare (rows discarded), 4 + parquet encode and write.
    Profile {
        #[arg(long)]
        input: PathBuf,
        /// UTC day of the file, `YYYY-MM-DD`.
        #[arg(long)]
        day: String,
        #[arg(long, value_delimiter = ',', default_value = "0,1,2,3,4")]
        stages: Vec<u8>,
        /// Scratch output root for stage 4 (deleted by the caller).
        #[arg(long)]
        scratch_out: Option<PathBuf>,
        #[arg(long)]
        exchange_map: Option<PathBuf>,
        #[arg(long)]
        default_exchange: Option<String>,
        #[arg(long, default_value_t = 250)]
        freq_ms: u64,
        #[arg(long, default_value_t = 1800)]
        session_gap_s: u64,
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

fn cpu_seconds() -> (f64, f64) {
    // SAFETY: getrusage writes into the zero-initialised struct we pass.
    let mut u: libc::rusage = unsafe { std::mem::zeroed() };
    unsafe { libc::getrusage(libc::RUSAGE_SELF, &mut u) };
    let t = |v: libc::timeval| v.tv_sec as f64 + v.tv_usec as f64 / 1e6;
    (t(u.ru_utime), t(u.ru_stime))
}

#[allow(clippy::too_many_arguments)]
fn profile(
    input: &Path,
    day: &str,
    stages: &[u8],
    scratch_out: &Option<PathBuf>,
    map: &BTreeMap<String, String>,
    default_exchange: &Option<String>,
    p: Params,
) -> Result<()> {
    let gz_bytes = std::fs::metadata(input)?.len();
    let day_start = day_number(day)? * DAY_NS;
    for &stage in stages {
        let (u0, s0) = cpu_seconds();
        let t0 = Instant::now();
        let mut text_bytes = 0u64;
        let mut lines = 0u64;
        let mut members = 0u64;
        let mut emitted = 0u64;
        match stage {
            0 => {
                // Member loop: counts the gzip members and never concatenates them.
                let mut src = BufReader::with_capacity(1 << 20, File::open(input)?);
                let mut buf = vec![0u8; 1 << 20];
                loop {
                    let mut d = GzDecoder::new(&mut src);
                    loop {
                        let n = d.read(&mut buf)?;
                        if n == 0 {
                            break;
                        }
                        text_bytes += n as u64;
                    }
                    members += 1;
                    if src.fill_buf()?.is_empty() {
                        break;
                    }
                }
            }
            1 => {
                let mut r =
                    BufReader::with_capacity(1 << 20, MultiGzDecoder::new(File::open(input)?));
                let mut line = Vec::with_capacity(1024);
                loop {
                    line.clear();
                    let n = r.read_until(b'\n', &mut line)?;
                    if n == 0 {
                        break;
                    }
                    text_bytes += n as u64;
                    lines += 1;
                }
            }
            2 | 3 => {
                let r = BufReader::with_capacity(1 << 20, MultiGzDecoder::new(File::open(input)?));
                let q = if stage == 2 {
                    p.without_book_parse()
                } else {
                    p
                };
                let stats = slice_stream(r, day_start, &q, &mut NullSink)?;
                lines = stats.iter().map(|s| s.updates).sum();
                emitted = stats.iter().map(|s| s.emitted).sum();
            }
            4 => {
                let out = scratch_out.clone().context("stage 4 needs --scratch-out")?;
                let layout = Layout {
                    out_root: out,
                    exchange_of: Arc::new(map.clone()),
                    default_exchange: default_exchange.clone(),
                    day: day.to_string(),
                    force: true,
                };
                let r = BufReader::with_capacity(1 << 20, MultiGzDecoder::new(File::open(input)?));
                let mut sink = ParquetSink::new(&layout, p, day_start);
                let stats = slice_stream(r, day_start, &p, &mut sink)?;
                lines = stats.iter().map(|s| s.updates).sum();
                emitted = stats.iter().map(|s| s.emitted).sum();
            }
            other => bail!("unknown stage {other}"),
        }
        let wall = t0.elapsed().as_secs_f64();
        let (u1, s1) = cpu_seconds();
        println!(
            "{}",
            json!({"stage": stage, "wall_s": (wall * 10.0).round() / 10.0,
                   "user_s": ((u1 - u0) * 10.0).round() / 10.0, "sys_s": ((s1 - s0) * 10.0).round() / 10.0,
                   "waiting_s": ((wall - (u1 - u0) - (s1 - s0)).max(0.0) * 10.0).round() / 10.0,
                   "gz_bytes": gz_bytes, "text_bytes": text_bytes, "lines": lines,
                   "gzip_members": members, "emitted_rows": emitted})
        );
    }
    Ok(())
}

fn main() -> Result<()> {
    let args = Args::parse();
    match args.command {
        Command::Profile {
            input,
            day,
            stages,
            scratch_out,
            exchange_map,
            default_exchange,
            freq_ms,
            session_gap_s,
        } => {
            let p = Params::new(freq_ms, session_gap_s)?;
            let map = match &exchange_map {
                Some(m) => load_map(m)?,
                None => BTreeMap::new(),
            };
            profile(
                &input,
                &day,
                &stages,
                &scratch_out,
                &map,
                &default_exchange,
                p,
            )
        }
        Command::Wash {
            raw_root,
            db_root,
            shard_root,
            products,
            days,
            workers,
            shard_rows,
            zstd_level,
            shard_budget_gib,
            force,
        } => {
            ensure!(
                (1..=limits::L2_SLICE_WORKERS).contains(&workers),
                "--workers must be 1..={}",
                limits::L2_SLICE_WORKERS
            );
            ensure!(shard_rows >= 1, "--shard-rows must be >= 1");
            ensure!(
                (1..=19).contains(&zstd_level),
                "--zstd-level must be 1..=19"
            );
            let jobs: Vec<(String, String)> = products
                .iter()
                .flat_map(|pr| days.iter().map(move |d| (pr.clone(), d.clone())))
                .collect();
            let cfg = BatchConfig {
                raw_root,
                shard_root,
                db_root,
                workers,
                rows_per_shard: shard_rows,
                zstd_level,
                shard_budget_bytes: shard_budget_gib << 30,
                force,
            };
            let started = Instant::now();
            let report = run_batch(&cfg, jobs.clone())?;
            let washed = report
                .results
                .iter()
                .filter(|v| v["status"] == "washed")
                .count();
            println!(
                "{}",
                json!({"summary": true, "stage": "wash", "jobs": jobs.len(), "washed_files": washed,
                       "failed": report.failed.is_some(), "seconds": started.elapsed().as_secs(),
                       "peak_rss_mib": peak_rss_mib(), "workers": workers,
                       "peak_unwashed_shard_gib": (report.peak_queued_bytes as f64 / (1u64 << 30) as f64 * 100.0).round() / 100.0,
                       "db_sst_bytes_by_day": report.db_sst_bytes})
            );
            if let Some(e) = report.failed {
                eprintln!("FAILED: {e}");
                bail!("wash failed");
            }
            Ok(())
        }
        Command::Extract {
            db_root,
            out_root,
            exchange_map,
            default_exchange,
            products,
            days,
            freq_ms,
            session_gap_s,
            workers,
            cache_mb,
            force,
        } => {
            ensure!(
                (1..=limits::L2_SLICE_WORKERS).contains(&workers),
                "--workers must be 1..={}",
                limits::L2_SLICE_WORKERS
            );
            let params = Params::new(freq_ms, session_gap_s)?;
            let map = match &exchange_map {
                Some(m) => load_map(m)?,
                None => BTreeMap::new(),
            };
            ensure!(
                !map.is_empty() || default_exchange.is_some(),
                "give --exchange-map or --default-exchange"
            );
            let jobs: Vec<(String, String)> = products
                .iter()
                .flat_map(|pr| days.iter().map(move |d| (pr.clone(), d.clone())))
                .collect();
            let cfg = ExtractConfig {
                db_root,
                out_root,
                exchange_of: Arc::new(map),
                default_exchange,
                params,
                force,
                workers,
                cache_mb,
            };
            let started = Instant::now();
            let report = run_extract(&cfg, jobs.clone())?;
            for v in &report.results {
                println!("{v}");
            }
            println!(
                "{}",
                json!({"summary": true, "stage": "extract", "jobs": jobs.len(),
                       "failed": report.failed.is_some(), "seconds": started.elapsed().as_secs(),
                       "peak_rss_mib": peak_rss_mib(), "workers": workers, "freq_ms": freq_ms})
            );
            if let Some(e) = report.failed {
                eprintln!("FAILED: {e}");
                bail!("extract failed");
            }
            Ok(())
        }
    }
}
