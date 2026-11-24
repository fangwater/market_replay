//! Fixed-frequency downsampling of LSEG Normalized LL2 daily packages to Parquet.
//! Rules: `docs/lseg/l2_slice.md`.
use anyhow::{bail, ensure, Context, Result};
use clap::{Parser, Subcommand};
use flate2::{bufread::GzDecoder, read::MultiGzDecoder};
use lseg_delivery::{
    l2_backfill::{self, Gate},
    l2_batch::{run_batch, BatchConfig},
    l2_db, l2_deliver,
    l2_extract::{run_extract, run_extract_rics, ExtractConfig},
    l2_raw,
    l2_report::{run_report, ReportConfig},
    l2_slice::{day_number, slice_stream, Layout, NullSink, Params, ParquetSink, DAY_NS},
    l2_stream::{run_stream, Progress, StreamConfig, Unit, UnitKind},
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

const DEFAULT_DB_ROOT: &str = "/mnt/hdd-raid5-72t/fanghaizhou/lseg_data/l2_db";
const DEFAULT_TARGET_ROOT: &str = "/mnt/nfs/30.100_cta_data/lseg";

#[derive(Parser)]
#[command(about = "LSEG Normalized LL2 daily packages -> fixed-frequency downsampled Parquet")]
struct Args {
    #[command(subcommand)]
    command: Command,
}
#[derive(Subcommand)]
enum Command {
    /// Verification and report of the backfill (counts from the markers vs a scan of
    /// every update record, overlaps between inputs, sizes). Read-only.
    Report {
        #[arg(long, default_value = DEFAULT_DB_ROOT)]
        db_root: PathBuf,
        #[arg(
            long,
            default_value = "/mnt/hdd-raid5-72t/liang_torch/lseg_data/future_daily/normalisedLL2"
        )]
        raw_root: PathBuf,
        #[arg(long, default_value = "2026-06-01")]
        daily_start: String,
        #[arg(long, default_value = "2026-11-12")]
        daily_end: String,
        #[arg(long)]
        gaps: Option<PathBuf>,
        #[arg(long)]
        spot_checks: Option<PathBuf>,
        #[arg(long)]
        log: Option<PathBuf>,
        #[arg(long)]
        out_json: PathBuf,
        #[arg(long)]
        out_md: PathBuf,
        #[arg(long, default_value_t = limits::L2_SLICE_WORKERS)]
        workers: usize,
        #[arg(long, default_value_t = 256)]
        cache_mb: usize,
    },
    /// One-off backfill into RocksDB: the period packages (each one DB, parts read
    /// back to back as one stream) and the daily files (one DB per UTC day), all
    /// products, resumable (finished units are skipped), paused while the server's
    /// jobs run. Run it from a copy of the binary with `nice -n 10`.
    Backfill {
        #[arg(
            long,
            default_value = "/mnt/hdd-raid5-72t/liang_torch/lseg_data/future_daily/normalisedLL2"
        )]
        raw_root: PathBuf,
        #[arg(
            long,
            default_value = "/mnt/hdd-raid5-72t/liang_torch/lseg_data/future/normalisedLL2"
        )]
        package_root: PathBuf,
        /// Which period packages: p2025, p2026h1, s2025 (the 2025 supplement).
        #[arg(long, value_delimiter = ',', default_value = "p2026h1,p2025,s2025")]
        packages: Vec<String>,
        /// Explicit stream units `id=part1,part2,…` (replaces --packages; for dry runs).
        #[arg(long)]
        stream: Vec<String>,
        #[arg(long, default_value = "2026-06-01")]
        daily_start: String,
        #[arg(long, default_value = "2026-11-12")]
        daily_end: String,
        /// Default: every product directory below --raw-root.
        #[arg(long, value_delimiter = ',')]
        daily_products: Vec<String>,
        #[arg(long)]
        no_daily: bool,
        /// Day databases below this root; stream databases in `<db-root>/_stream/<id>`.
        #[arg(long, default_value = DEFAULT_DB_ROOT)]
        db_root: PathBuf,
        #[arg(long, default_value_t = limits::L2_SLICE_WORKERS)]
        workers: usize,
        /// Files read at the same time.
        #[arg(long, default_value_t = 4)]
        producers: usize,
        #[arg(long, default_value_t = 6)]
        sst_zstd_level: i32,
        /// Text bytes per washed segment (= per SST file); big on purpose: a package
        /// of hundreds of GB would otherwise ingest millions of tiny SST files.
        #[arg(long, default_value_t = 512)]
        segment_mib: usize,
        /// Progress JSON (rewritten every 10 s).
        #[arg(long)]
        progress: PathBuf,
        /// Source gaps (missing / empty / still downloading files) are written here.
        #[arg(long)]
        gaps: PathBuf,
        /// host:port of the server whose `/api/admin/status` gates the run.
        #[arg(long, default_value = "127.0.0.1:18313")]
        status_addr: String,
        #[arg(long)]
        no_gate: bool,
        /// Raw LL2 mode: the `--stream` units are Raw LL2 packages (`Legacy Level 2`);
        /// they are converted to Normalized rows (docs/lseg/l2_raw.md) and only the
        /// RICs of this file (one per line) are washed. Databases go to `_stream/<id>`;
        /// use an id starting with `raw`.
        #[arg(long)]
        raw_ll2_rics: Option<PathBuf>,
        /// With --raw-ll2-rics: the parts of a --stream are separately cut segments
        /// (`scripts/lseg/rawll2_segments.py`), not one continuous stream.
        #[arg(long)]
        raw_ll2_segments: bool,
    },
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
        /// `stream` (default): one in-memory pass, no shard files; `shards`: the
        /// TAS-style temporary zstd shard files (kept for comparison).
        #[arg(long, default_value = "stream")]
        mode: String,
        /// Threads (of --workers) that decompress in `stream` mode.
        #[arg(long, default_value_t = 2)]
        producers: usize,
        /// zstd level of the stored blocks (default: the production level 9).
        #[arg(long)]
        sst_zstd_level: Option<i32>,
    },
    /// Stage 3 for explicit RICs: slices of one frequency for (RIC, UTC day)
    /// out of one or several databases (a stream/package database, or day databases).
    ExtractRics {
        /// Database directories, chronological order.
        #[arg(long, value_delimiter = ',', required = true)]
        db_dirs: Vec<PathBuf>,
        #[arg(long)]
        out_root: PathBuf,
        #[arg(long)]
        exchange_map: Option<PathBuf>,
        #[arg(long)]
        default_exchange: Option<String>,
        #[arg(long, value_delimiter = ',', required = true)]
        rics: Vec<String>,
        #[arg(long, value_delimiter = ',', required = true)]
        days: Vec<String>,
        #[arg(long, default_value_t = 250)]
        freq_ms: u64,
        #[arg(long, default_value_t = 1800)]
        session_gap_s: u64,
        #[arg(long, default_value_t = 2)]
        workers: usize,
        #[arg(long, default_value_t = 256)]
        cache_mb: usize,
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
    /// Throughput test of the real publish routine (`lseg_delivery::publish`): the
    /// staged L2 files below `--stage-root` are published into a THROWAWAY directory
    /// (same share, outside the delivery tree), disjoint subsets per run.
    PublishBench {
        /// Output root of `extract` (`<exchange>/<year>/<RIC>/<day>/l2_<freq>ms.parquet`).
        #[arg(long)]
        stage_root: PathBuf,
        #[arg(long)]
        target_root: PathBuf,
        /// Thread counts, one run each, in this order (use e.g. 1,4,8,8,4,1 to see drift).
        #[arg(long, value_delimiter = ',', default_value = "1,4,8")]
        threads: Vec<usize>,
        #[arg(long, default_value_t = 700)]
        files_per_run: usize,
    },
    /// Delivers the washed databases into the NFS delivery tree (resumable,
    /// idempotent, gated on the server's job state). See `lseg_delivery::l2_deliver`.
    Deliver {
        /// One database per UTC day below this root (the daily packages).
        #[arg(long)]
        db_root: Option<PathBuf>,
        /// Stream databases (period packages), chronological.
        #[arg(long, value_delimiter = ',')]
        stream_dbs: Vec<PathBuf>,
        /// First and last UTC day, YYYY-MM-DD (inclusive).
        #[arg(long)]
        from: String,
        #[arg(long)]
        to: String,
        /// Lines `RIC<TAB>exchange-directory` (scripts/lseg/l2_exchange_map.py ALL).
        #[arg(long)]
        exchange_map: PathBuf,
        /// Lines `RIC<TAB>detail` of RICs whose exchange directory is contradictory
        /// between sources: recorded as problems, not delivered.
        #[arg(long)]
        exchange_conflicts: Option<PathBuf>,
        /// Only these exchange directories (default: all).
        #[arg(long, value_delimiter = ',')]
        exchanges: Vec<String>,
        /// Only the RICs listed in this file (one per line); other RICs of the
        /// databases are neither counted nor delivered (supplement packages: their
        /// RICs that the main package already holds are left to the main one).
        #[arg(long)]
        only_rics: Option<PathBuf>,
        /// Source of the databases: `normalized` (default) or `raw` (Raw LL2, docs/lseg/l2_raw.md):
        /// raw RIC-year directories get the marker file `l2_source`, which the catalog turns into
        /// `l2_directories.source = 'raw'`; a RIC-year never mixes the two.
        #[arg(long, default_value = "normalized", value_parser = ["normalized", "raw"])]
        l2_source: String,
        /// With `--l2-source raw`: RIC-years that already hold Normalized L2 days get their missing days from
        /// Raw; the marker says `mixed` and lists the Raw days (catalog: `l2_directories.source = 'mixed'`).
        #[arg(long)]
        allow_mixed: bool,
        #[arg(long, default_value = DEFAULT_TARGET_ROOT)]
        target_root: PathBuf,
        /// Local staging directory (NVMe); removed at the end.
        #[arg(long)]
        staging: PathBuf,
        #[arg(long, default_value_t = 250)]
        freq_ms: u64,
        #[arg(long, default_value_t = 1800)]
        session_gap_s: u64,
        #[arg(long, default_value_t = limits::L2_SLICE_WORKERS)]
        extract_workers: usize,
        #[arg(long, default_value_t = limits::IO_THREADS)]
        publish_threads: usize,
        /// RIC-days per batch (also the unit of staging).
        #[arg(long, default_value_t = 3000)]
        batch_files: usize,
        #[arg(long, default_value_t = 256)]
        cache_mb: usize,
        #[arg(long)]
        progress: PathBuf,
        #[arg(long, default_value = "127.0.0.1:18313")]
        status_addr: String,
        #[arg(long)]
        no_gate: bool,
        /// Enumerate and count only.
        #[arg(long)]
        dry_run: bool,
        /// Stop after this many files (tests).
        #[arg(long)]
        max_files: Option<u64>,
    },
    /// In-memory micro benchmark of the row parsers on the first `rows` rows of
    /// a source file (min of 3 repeats, CPU seconds): old `parse_book`,
    /// `BookParser`, time stamp parser, delta encoder.
    BenchParse {
        #[arg(long)]
        input: PathBuf,
        #[arg(long, default_value_t = 2_000_000)]
        rows: usize,
        /// skip this many rows first (to get into a busy part of the day)
        #[arg(long, default_value_t = 0)]
        skip: usize,
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

fn cpu_total() -> f64 {
    let (u, s) = cpu_seconds();
    ((u + s) * 10.0).round() / 10.0
}

fn bench_parse(input: &Path, rows: usize, skip: usize) -> Result<()> {
    use lseg_delivery::{
        l2_layout::Codec2,
        l2_slice::{parse_book, Book, BookParser, TsParser, BOOK_FIELDS},
    };
    let mut r = BufReader::with_capacity(1 << 20, MultiGzDecoder::new(File::open(input)?));
    let mut line = Vec::new();
    r.read_until(b'\n', &mut line)?; // header
    let mut lines: Vec<Vec<u8>> = Vec::with_capacity(rows);
    let mut seen = 0usize;
    while lines.len() < rows {
        line.clear();
        if r.read_until(b'\n', &mut line)? == 0 {
            break;
        }
        seen += 1;
        if seen <= skip {
            continue;
        }
        while matches!(line.last(), Some(b'\n' | b'\r')) {
            line.pop();
        }
        lines.push(line.clone());
    }
    let bytes: usize = lines.iter().map(|l| l.len()).sum();
    println!(
        "{}",
        json!({"rows": lines.len(), "avg_line_bytes": bytes / lines.len().max(1)})
    );
    let cpu = || {
        let (u, s) = cpu_seconds();
        u + s
    };
    let best = |f: &mut dyn FnMut()| {
        let mut b = f64::MAX;
        for _ in 0..3 {
            let t = cpu();
            f();
            b = b.min(cpu() - t);
        }
        b
    };
    let n = lines.len() as f64;
    let ns = |secs: f64| (secs / n * 1e9 * 10.0).round() / 10.0;
    let mut book: Book = [f64::NAN; BOOK_FIELDS];
    let t_old = best(&mut || {
        for l in &lines {
            parse_book(l, &mut book).unwrap();
        }
    });
    let mut bp = BookParser::new();
    let t_new = best(&mut || {
        for l in &lines {
            bp.parse(l, &mut book).unwrap();
        }
    });
    let mut ts = TsParser::new();
    let t_ts = best(&mut || {
        for l in &lines {
            let ts_b = l.splitn(4, |b| *b == b',').nth(2).unwrap();
            ts.parse(ts_b).unwrap();
        }
    });
    let mut books: Vec<Book> = Vec::with_capacity(lines.len());
    let mut bp2 = BookParser::new();
    for l in &lines {
        bp2.parse(l, &mut book).unwrap();
        books.push(book);
    }
    let mut out = Vec::with_capacity(1 << 20);
    let mut codec = Codec2::new();
    let t_enc = best(&mut || {
        out.clear();
        codec = Codec2::new();
        for (i, b) in books.iter().enumerate() {
            codec.encode(b, i == 0, &mut out);
            if out.len() > (1 << 20) {
                out.clear();
            }
        }
    });
    println!(
        "{}",
        json!({"ns_per_row": {"parse_book_old": ns(t_old), "book_parser": ns(t_new),
               "ts_parse": ns(t_ts), "delta_encode": ns(t_enc)}})
    );
    Ok(())
}

fn main() -> Result<()> {
    let args = Args::parse();
    match args.command {
        Command::PublishBench {
            stage_root,
            target_root,
            threads,
            files_per_run,
        } => {
            ensure!(
                threads.iter().all(|t| (1..=limits::IO_THREADS).contains(t)),
                "thread counts must be 1..={}",
                limits::IO_THREADS
            );
            let file_name = Params::new(250, 1800)?.file_name();
            let mut staged = l2_deliver::scan_staged(&stage_root, &file_name)?;
            ensure!(
                staged.len() >= threads.len() * files_per_run,
                "{} staged files are not enough for {} runs of {files_per_run}",
                staged.len(),
                threads.len()
            );
            // interleave so that every run sees the same mix of big and small files
            staged.sort_by(|a, b| a.part.path.cmp(&b.part.path));
            let runs = threads.len();
            for (k, t) in threads.iter().enumerate() {
                let subset: Vec<_> = staged
                    .iter()
                    .skip(k)
                    .step_by(runs)
                    .take(files_per_run)
                    .cloned()
                    .collect();
                let label = format!("run{k}_t{t}");
                let r = l2_deliver::bench_publish(&subset, &target_root, &label, *t);
                println!("{r}");
            }
            Ok(())
        }
        Command::Deliver {
            db_root,
            stream_dbs,
            from,
            to,
            exchange_map,
            exchange_conflicts,
            l2_source,
            allow_mixed,
            exchanges,
            only_rics,
            target_root,
            staging,
            freq_ms,
            session_gap_s,
            extract_workers,
            publish_threads,
            batch_files,
            cache_mb,
            progress,
            status_addr,
            no_gate,
            dry_run,
            max_files,
        } => {
            let source = match (db_root, stream_dbs.is_empty()) {
                (Some(r), true) => l2_deliver::Source::DayDbs(r),
                (None, false) => l2_deliver::Source::Streams(stream_dbs),
                _ => bail!("give either --db-root (day databases) or --stream-dbs"),
            };
            let cfg = l2_deliver::DeliverConfig {
                source,
                from: day_number(&from)?,
                to: day_number(&to)?,
                exchange_of: Arc::new(l2_deliver::load_exchange_map(&exchange_map)?),
                exchanges: exchanges.into_iter().collect(),
                only_rics: match &only_rics {
                    Some(f) => std::fs::read_to_string(f)
                        .with_context(|| format!("read {}", f.display()))?
                        .lines()
                        .map(|l| l.trim().to_string())
                        .filter(|l| !l.is_empty())
                        .collect(),
                    None => Default::default(),
                },
                conflicts: match &exchange_conflicts {
                    Some(f) => l2_deliver::load_exchange_map(f)?,
                    None => BTreeMap::new(),
                },
                staging,
                target_root,
                params: Params::new(freq_ms, session_gap_s)?,
                extract_workers,
                publish_threads,
                batch_files,
                cache_mb,
                progress,
                status_addr: (!no_gate).then_some(status_addr),
                dry_run,
                max_files,
                source_raw: l2_source == "raw",
                allow_mixed,
            };
            let summary = l2_deliver::run_deliver(cfg)?;
            println!("{summary}");
            Ok(())
        }
        Command::BenchParse { input, rows, skip } => bench_parse(&input, rows, skip),
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
        Command::Report {
            db_root,
            raw_root,
            daily_start,
            daily_end,
            gaps,
            spot_checks,
            log,
            out_json,
            out_md,
            workers,
            cache_mb,
        } => {
            ensure!(
                (1..=limits::L2_SLICE_WORKERS).contains(&workers),
                "--workers must be 1..={}",
                limits::L2_SLICE_WORKERS
            );
            let rep = run_report(&ReportConfig {
                db_root,
                raw_root,
                daily_start,
                daily_end,
                gaps,
                spot_checks,
                log,
                out_json,
                out_md,
                workers,
                cache_mb,
            })?;
            println!("{}", json!({"report": true, "ok": rep["ok"]}));
            ensure!(rep["ok"] == true, "report found problems");
            Ok(())
        }
        Command::Backfill {
            raw_root,
            package_root,
            packages,
            stream,
            daily_start,
            daily_end,
            daily_products,
            no_daily,
            db_root,
            workers,
            producers,
            sst_zstd_level,
            segment_mib,
            progress,
            gaps,
            status_addr,
            no_gate,
            raw_ll2_rics,
            raw_ll2_segments,
        } => {
            ensure!(
                (2..=limits::L2_SLICE_WORKERS).contains(&workers),
                "--workers must be 2..={}",
                limits::L2_SLICE_WORKERS
            );
            l2_db::set_sst_zstd_level(sst_zstd_level);
            let mut units: Vec<Unit> = Vec::new();
            if stream.is_empty() {
                units.extend(l2_backfill::package_units(&package_root, &packages)?);
            } else {
                for spec in &stream {
                    let (id, list) = spec.split_once('=').context("--stream id=part1,part2")?;
                    let parts: Vec<PathBuf> = list.split(',').map(PathBuf::from).collect();
                    let gz_bytes = parts
                        .iter()
                        .map(|p| std::fs::metadata(p).map_or(0, |m| m.len()))
                        .sum();
                    units.push(Unit {
                        kind: UnitKind::Stream {
                            id: id.to_string(),
                            parts,
                        },
                        gz_bytes,
                    });
                }
            }
            let mut gaps_json = json!({});
            if !no_daily {
                let days = l2_backfill::days_between(&daily_start, &daily_end)?;
                let prods = (!daily_products.is_empty()).then_some(daily_products.as_slice());
                let scan = l2_backfill::daily_units(&raw_root, prods, &days)?;
                gaps_json = json!({
                    "days": [daily_start, daily_end],
                    "missing_files": scan.missing.len(), "missing": scan.missing,
                    "zero_byte_placeholders": scan.empty.len(), "zero_byte": scan.empty,
                    "still_downloading": scan.too_new.len(), "downloading": scan.too_new,
                    "products_without_directory": scan.no_dir,
                });
                units.extend(scan.units);
            }
            l2_backfill::write_json_atomic(&gaps, &gaps_json)?;
            let total = units.len();
            let gate = Gate::new();
            let progress_state = Arc::new(Progress::default());
            let gate_thread =
                (!no_gate).then(|| l2_backfill::spawn_gate(Arc::clone(&gate), status_addr.clone()));
            let monitor = l2_backfill::spawn_monitor(
                Arc::clone(&progress_state),
                Arc::clone(&gate),
                progress,
                json!({"units_requested": total, "workers": workers, "producers": producers,
                       "sst_zstd_level": sst_zstd_level, "db_root": db_root,
                       "started_utc": chrono::Utc::now().to_rfc3339()}),
            );
            let g2 = Arc::clone(&gate);
            let cfg = StreamConfig {
                raw_root,
                stream_db_root: db_root.join("_stream"),
                db_root,
                workers,
                producers,
                force: false,
                segment_bytes: segment_mib << 20,
                block_bytes: lseg_delivery::l2_stream::BLOCK_BYTES,
                gate: Arc::new(move || g2.wait()),
                progress: Arc::clone(&progress_state),
                raw_segments: raw_ll2_segments,
                raw_ll2: raw_ll2_rics
                    .as_deref()
                    .map(l2_raw::read_ric_list)
                    .transpose()?
                    .map(Arc::new),
            };
            let started = Instant::now();
            let rep = run_stream(&cfg, units);
            gate.stop.store(true, std::sync::atomic::Ordering::Relaxed);
            let _ = monitor.join();
            if let Some(t) = gate_thread {
                let _ = t.join();
            }
            let rep = rep?;
            let count = |st: &str| rep.results.iter().filter(|v| v["status"] == st).count();
            println!(
                "{}",
                json!({"summary": true, "stage": "backfill", "units_requested": total,
                       "washed": count("washed"), "kept_existing": count("kept_existing"),
                       "failed_units": count("failed"), "missing_source": count("missing_source"),
                       "failed": rep.failed.is_some(), "seconds": started.elapsed().as_secs(),
                       "cpu_seconds": cpu_total(), "peak_rss_mib": peak_rss_mib(),
                       "negative_counts_read_as_empty": lseg_delivery::l2_slice::negative_counts()})
            );
            if let Some(e) = rep.failed {
                eprintln!("FAILED: {e}");
                bail!("backfill failed");
            }
            ensure!(count("failed") == 0, "{} units failed", count("failed"));
            Ok(())
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
            mode,
            producers,
            sst_zstd_level,
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
            if let Some(l) = sst_zstd_level {
                l2_db::set_sst_zstd_level(l);
            }
            let started = Instant::now();
            if mode == "stream" {
                ensure!(workers >= 2, "--workers must be >= 2 in stream mode");
                let units: Vec<Unit> = jobs
                    .iter()
                    .map(|(p, d)| Unit {
                        gz_bytes: std::fs::metadata(
                            raw_root.join(p).join(format!("{p}_{d}_normll2.csv.gz")),
                        )
                        .map_or(0, |m| m.len()),
                        kind: UnitKind::Daily {
                            product: p.clone(),
                            day: d.clone(),
                        },
                    })
                    .collect();
                let cfg = StreamConfig {
                    raw_root,
                    db_root,
                    stream_db_root: shard_root.join("_unused"),
                    workers,
                    producers,
                    force,
                    segment_bytes: lseg_delivery::l2_stream::SEGMENT_BYTES,
                    block_bytes: lseg_delivery::l2_stream::BLOCK_BYTES,
                    gate: Arc::new(|| {}),
                    progress: Arc::new(Progress::default()),
                    raw_ll2: None,
                    raw_segments: false,
                };
                let rep = run_stream(&cfg, units)?;
                let washed = rep
                    .results
                    .iter()
                    .filter(|v| v["status"] == "washed")
                    .count();
                let failed_units = rep
                    .results
                    .iter()
                    .filter(|v| v["status"] == "failed")
                    .count();
                println!(
                    "{}",
                    json!({"summary": true, "stage": "wash", "mode": "stream", "jobs": jobs.len(),
                           "washed_files": washed, "failed_units": failed_units,
                           "failed": rep.failed.is_some(), "seconds": started.elapsed().as_secs(),
                           "cpu_seconds": cpu_total(), "peak_rss_mib": peak_rss_mib(),
                           "workers": workers, "producers": producers,
                           "db_sst_bytes_by_day": rep.db_sst_bytes})
                );
                if let Some(e) = rep.failed {
                    eprintln!("FAILED: {e}");
                    bail!("wash failed");
                }
                ensure!(failed_units == 0, "{failed_units} units failed");
                return Ok(());
            }
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
            let report = run_batch(&cfg, jobs.clone())?;
            let washed = report
                .results
                .iter()
                .filter(|v| v["status"] == "washed")
                .count();
            println!(
                "{}",
                json!({"summary": true, "stage": "wash", "mode": "shards", "jobs": jobs.len(), "washed_files": washed,
                       "failed": report.failed.is_some(), "seconds": started.elapsed().as_secs(),
                       "cpu_seconds": cpu_total(), "peak_rss_mib": peak_rss_mib(), "workers": workers,
                       "peak_unwashed_shard_gib": (report.peak_queued_bytes as f64 / (1u64 << 30) as f64 * 100.0).round() / 100.0,
                       "db_sst_bytes_by_day": report.db_sst_bytes})
            );
            if let Some(e) = report.failed {
                eprintln!("FAILED: {e}");
                bail!("wash failed");
            }
            Ok(())
        }
        Command::ExtractRics {
            db_dirs,
            out_root,
            exchange_map,
            default_exchange,
            rics,
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
            let jobs: Vec<(String, String)> = rics
                .iter()
                .flat_map(|r| days.iter().map(move |d| (r.clone(), d.clone())))
                .collect();
            let cfg = ExtractConfig {
                db_root: PathBuf::new(),
                out_root,
                exchange_of: Arc::new(map),
                default_exchange,
                params,
                force,
                workers,
                cache_mb,
            };
            let rep = run_extract_rics(&cfg, &db_dirs, jobs)?;
            for v in &rep.results {
                println!("{v}");
            }
            if let Some(e) = rep.failed {
                eprintln!("FAILED: {e}");
                bail!("extract-rics failed");
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
