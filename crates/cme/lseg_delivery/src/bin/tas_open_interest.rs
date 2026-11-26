//! Open interest from LSEG TAS source gzips into PostgreSQL (`lseg_open_interest`).
//! See docs/lseg/open_interest.md.
//!
//! - `scan`: one gzip to CSV, no database (checks and measurements).
//! - `init-schema`: apply `sql/lseg_open_interest.sql` (idempotent).
//! - `sync`: find every source gzip under the given roots that is not loaded yet, scan it and
//!   load it together with its `sources` row in one transaction. Sources already loaded with the
//!   same size and mtime are skipped; a changed source is an error unless `--replace`.
use anyhow::{bail, ensure, Context, Result};
use clap::{Parser, Subcommand};
use lseg_delivery::{
    limits::{AGGREGATION_WORKERS, OI_BACKFILL_WORKERS},
    open_interest::{read_header, scan_gz, Event, Stats, RULE_VERSION},
};
use serde::Deserialize;
use sqlx::{postgres::PgPoolOptions, PgConnection, PgPool};
use std::{
    collections::HashMap,
    fs::{self, File},
    io::{BufWriter, Read},
    path::{Path, PathBuf},
    sync::{
        atomic::{AtomicUsize, Ordering},
        Mutex,
    },
    time::{Instant, UNIX_EPOCH},
};

const SCHEMA_SQL: &str = include_str!("../../sql/lseg_open_interest.sql");
const DATABASE_URL_ENV: &str = "LSEG_OI_DATABASE_URL";

#[derive(Parser)]
struct Cli {
    #[command(subcommand)]
    cmd: Cmd,
}

#[derive(Subcommand)]
enum Cmd {
    /// Scan one gzip and write its observations / rejects as CSV (no database).
    Scan {
        #[arg(long)]
        input: PathBuf,
        /// Header source for parts without a header (part 0 of the package).
        #[arg(long)]
        header_from: Option<PathBuf>,
        #[arg(long)]
        output: PathBuf,
        /// Read at most this many compressed bytes (measurements only).
        #[arg(long)]
        max_compressed_bytes: Option<u64>,
    },
    /// Create or update the PostgreSQL schema.
    InitSchema {
        /// Default: $LSEG_OI_DATABASE_URL.
        #[arg(long)]
        database_url: Option<String>,
    },
    /// Load every source gzip that is not loaded yet.
    Sync {
        #[arg(long)]
        database_url: Option<String>,
        /// Directory of period packages (`<package>/merged-Data-part-*.csv.gz` or
        /// `<package>/merged-Data.csv.gz`).
        #[arg(long)]
        period_root: Vec<PathBuf>,
        /// Directory of daily files (`<PRODUCT>/<PRODUCT>_<date>_tick.csv.gz` plus the
        /// downloader's `.download.json`).
        #[arg(long)]
        daily_root: Vec<PathBuf>,
        /// Only these period packages / daily products (default: all).
        #[arg(long)]
        only: Vec<String>,
        /// Local scratch for the per-source staging CSVs (removed after each load).
        #[arg(long)]
        work_dir: PathBuf,
        #[arg(long, default_value_t = AGGREGATION_WORKERS)]
        workers: usize,
        /// One-off backfill: allows up to `OI_BACKFILL_WORKERS` (user approval 2026-11-25).
        #[arg(long)]
        backfill: bool,
        /// Only period packages whose start date (`..._<start>_<end>` in the name) is on or
        /// after this date; daily files are not affected.
        #[arg(long)]
        period_from: Option<chrono::NaiveDate>,
        /// Reload sources whose size or mtime changed since they were loaded.
        #[arg(long)]
        replace: bool,
        /// Stop after this many sources (largest first).
        #[arg(long)]
        max_sources: Option<usize>,
        /// Only list what would be loaded.
        #[arg(long)]
        dry_run: bool,
    },
}

#[derive(Clone, Debug)]
struct Source {
    path: PathBuf,
    kind: &'static str,
    package: String,
    header_from: PathBuf,
    bytes: u64,
    mtime_ns: i64,
}

fn main() -> Result<()> {
    env_logger::Builder::from_env(env_logger::Env::default().default_filter_or("info")).init();
    match Cli::parse().cmd {
        Cmd::Scan {
            input,
            header_from,
            output,
            max_compressed_bytes,
        } => scan_to_csv(
            &input,
            header_from.as_deref(),
            &output,
            max_compressed_bytes,
        ),
        Cmd::InitSchema { database_url } => runtime()?.block_on(async {
            let pool = connect(database_url, 1).await?;
            sqlx::raw_sql(SCHEMA_SQL)
                .execute(&pool)
                .await
                .context("apply schema")?;
            println!("schema lseg_open_interest applied");
            Ok(())
        }),
        Cmd::Sync {
            database_url,
            period_root,
            daily_root,
            only,
            work_dir,
            workers,
            backfill,
            period_from,
            replace,
            max_sources,
            dry_run,
        } => {
            let cap = if backfill {
                OI_BACKFILL_WORKERS
            } else {
                AGGREGATION_WORKERS
            };
            ensure!(
                (1..=cap).contains(&workers),
                "--workers must be 1..={cap} (fixed budget, see limits.rs)"
            );
            ensure!(
                !period_root.is_empty() || !daily_root.is_empty(),
                "give --period-root and/or --daily-root"
            );
            let mut sources = Vec::new();
            let (mut not_ready, mut placeholders) = (0, 0);
            for root in &period_root {
                sources.extend(discover_period(root)?);
            }
            for root in &daily_root {
                let (found, waiting, empty) = discover_daily(root)?;
                sources.extend(found);
                not_ready += waiting;
                placeholders += empty;
            }
            if !only.is_empty() {
                sources.retain(|s| only.contains(&s.package));
            }
            if let Some(from) = period_from {
                let mut kept = Vec::with_capacity(sources.len());
                for s in sources {
                    if s.kind != "period" || package_start(&s.package)? >= from {
                        kept.push(s);
                    }
                }
                sources = kept;
            }
            let rt = runtime()?;
            let pool = rt.block_on(connect(database_url, workers as u32))?;
            let loaded = rt.block_on(loaded_sources(&pool))?;
            let mut todo = Vec::new();
            let mut changed = Vec::new();
            let mut skipped = 0;
            for s in sources {
                match loaded.get(&s.path.display().to_string()) {
                    Some(&(b, m)) if b == s.bytes as i64 && m == s.mtime_ns => skipped += 1,
                    Some(_) if !replace => changed.push(s.path.display().to_string()),
                    _ => todo.push(s),
                }
            }
            todo.sort_by(|a, b| b.bytes.cmp(&a.bytes).then(a.path.cmp(&b.path)));
            if let Some(n) = max_sources {
                todo.truncate(n);
            }
            let todo_bytes: u64 = todo.iter().map(|s| s.bytes).sum();
            log::info!(
                "sources: {} to load ({:.1} GiB), {} already loaded, {} changed, {} daily not ready, {} empty placeholders",
                todo.len(),
                todo_bytes as f64 / (1u64 << 30) as f64,
                skipped,
                changed.len(),
                not_ready,
                placeholders
            );
            for c in &changed {
                log::error!("source changed since it was loaded (use --replace): {c}");
            }
            if dry_run {
                for s in &todo {
                    println!("{}\t{}\t{}", s.kind, s.bytes, s.path.display());
                }
                ensure!(changed.is_empty(), "{} changed sources", changed.len());
                return Ok(());
            }
            fs::create_dir_all(&work_dir)?;
            let failures = run_sync(&rt, &pool, &todo, &work_dir, workers, replace);
            let summary = serde_json::json!({
                "loaded": todo.len() - failures.len(), "failed": failures.len(),
                "already_loaded": skipped, "changed": changed.len(), "daily_not_ready": not_ready,
                "empty_placeholders": placeholders,
            });
            println!("{summary}");
            for (path, e) in &failures {
                log::error!("{path}: {e:#}");
            }
            ensure!(
                failures.is_empty() && changed.is_empty(),
                "{} sources failed, {} changed",
                failures.len(),
                changed.len()
            );
            Ok(())
        }
    }
}

/// Start date of a period package named `..._<YYYY-MM-DD>_<YYYY-MM-DD>`.
fn package_start(name: &str) -> Result<chrono::NaiveDate> {
    let mut parts = name.rsplit('_');
    let (_end, start) = (parts.next(), parts.next());
    start
        .and_then(|d| chrono::NaiveDate::parse_from_str(d, "%Y-%m-%d").ok())
        .with_context(|| format!("package {name} has no _<start>_<end> dates"))
}

fn runtime() -> Result<tokio::runtime::Runtime> {
    Ok(tokio::runtime::Builder::new_multi_thread()
        .worker_threads(1)
        .enable_all()
        .build()?)
}

async fn connect(url: Option<String>, connections: u32) -> Result<PgPool> {
    let url = match url {
        Some(u) => u,
        None => std::env::var(DATABASE_URL_ENV)
            .with_context(|| format!("give --database-url or set {DATABASE_URL_ENV}"))?,
    };
    PgPoolOptions::new()
        .max_connections(connections)
        .connect(&url)
        .await
        .context("connect to PostgreSQL")
}

async fn loaded_sources(pool: &PgPool) -> Result<HashMap<String, (i64, i64)>> {
    let rows: Vec<(String, i64, i64)> = sqlx::query_as(
        "SELECT source_path, source_bytes, source_mtime_ns FROM lseg_open_interest.sources",
    )
    .fetch_all(pool)
    .await
    .context("read lseg_open_interest.sources (run init-schema first)")?;
    Ok(rows.into_iter().map(|(p, b, m)| (p, (b, m))).collect())
}

fn identity(path: &Path) -> Result<(u64, i64)> {
    let meta = fs::metadata(path).with_context(|| format!("stat {}", path.display()))?;
    let mtime = meta.modified()?.duration_since(UNIX_EPOCH)?.as_nanos() as i64;
    Ok((meta.len(), mtime))
}

fn sorted_dir(dir: &Path) -> Result<Vec<PathBuf>> {
    let mut v: Vec<PathBuf> = fs::read_dir(dir)
        .with_context(|| format!("list {}", dir.display()))?
        .map(|e| e.map(|e| e.path()))
        .collect::<std::io::Result<_>>()?;
    v.sort();
    Ok(v)
}

fn discover_period(root: &Path) -> Result<Vec<Source>> {
    let mut out = Vec::new();
    for dir in sorted_dir(root)? {
        if !dir.is_dir() {
            continue;
        }
        let package = dir.file_name().unwrap().to_string_lossy().into_owned();
        let files: Vec<PathBuf> = sorted_dir(&dir)?
            .into_iter()
            .filter(|p| {
                let n = p.file_name().unwrap().to_string_lossy();
                n == "merged-Data.csv.gz"
                    || (n.starts_with("merged-Data-part-") && n.ends_with(".csv.gz"))
            })
            .collect();
        if files.is_empty() {
            log::warn!("no merged-Data gzip in {}", dir.display());
            continue;
        }
        let single = dir.join("merged-Data.csv.gz");
        let header_from = if files == [single.clone()] {
            single
        } else {
            let part0 = dir.join("merged-Data-part-000000.csv.gz");
            ensure!(
                files.contains(&part0) && !files.contains(&single),
                "{}: parts without part 0, or parts mixed with merged-Data.csv.gz",
                dir.display()
            );
            for (i, f) in files.iter().enumerate() {
                ensure!(
                    *f == dir.join(format!("merged-Data-part-{i:06}.csv.gz")),
                    "{}: part numbers are not contiguous at {}",
                    dir.display(),
                    f.display()
                );
            }
            part0
        };
        for path in files {
            let (bytes, mtime_ns) = identity(&path)?;
            out.push(Source {
                path,
                kind: "period",
                package: package.clone(),
                header_from: header_from.clone(),
                bytes,
                mtime_ns,
            });
        }
    }
    Ok(out)
}

#[derive(Deserialize)]
struct DownloadRecord {
    status: String,
    bytes: u64,
}

/// Daily files the downloader has finished (`.download.json` with the verified status and the
/// same size); the others are counted as not ready and left for a later run.
fn discover_daily(root: &Path) -> Result<(Vec<Source>, usize, usize)> {
    let mut out = Vec::new();
    let (mut not_ready, mut placeholders) = (0, 0);
    for dir in sorted_dir(root)? {
        if !dir.is_dir() {
            continue;
        }
        let product = dir.file_name().unwrap().to_string_lossy().into_owned();
        for path in sorted_dir(&dir)? {
            let name = path.file_name().unwrap().to_string_lossy().into_owned();
            let Some(date) = name
                .strip_prefix(&format!("{product}_"))
                .and_then(|r| r.strip_suffix("_tick.csv.gz"))
            else {
                continue;
            };
            ensure!(
                chrono::NaiveDate::parse_from_str(date, "%Y-%m-%d").is_ok(),
                "unexpected daily file name {}",
                path.display()
            );
            let (bytes, mtime_ns) = identity(&path)?;
            let record = path.with_file_name(format!("{name}.download.json"));
            let status = fs::read(&record)
                .ok()
                .and_then(|b| serde_json::from_slice::<DownloadRecord>(&b).ok())
                .filter(|r| r.bytes == bytes)
                .map(|r| r.status);
            match status.as_deref() {
                Some("download_complete_header_verified") => {}
                // The remote file itself is an empty placeholder: nothing to read.
                Some("source_zero_byte_placeholder_downloaded") => {
                    placeholders += 1;
                    continue;
                }
                _ => {
                    not_ready += 1;
                    continue;
                }
            }
            out.push(Source {
                header_from: path.clone(),
                path,
                kind: "daily",
                package: product.clone(),
                bytes,
                mtime_ns,
            });
        }
    }
    Ok((out, not_ready, placeholders))
}

struct Scanned {
    stats: Stats,
    members: u64,
    seconds: f64,
}

fn run_sync(
    rt: &tokio::runtime::Runtime,
    pool: &PgPool,
    todo: &[Source],
    work_dir: &Path,
    workers: usize,
    replace: bool,
) -> Vec<(String, anyhow::Error)> {
    let next = AtomicUsize::new(0);
    let done = AtomicUsize::new(0);
    let failures = Mutex::new(Vec::new());
    let started = Instant::now();
    let done_bytes = Mutex::new(0u64);
    std::thread::scope(|scope| {
        for _ in 0..workers {
            scope.spawn(|| loop {
                let i = next.fetch_add(1, Ordering::SeqCst);
                let Some(s) = todo.get(i) else { break };
                let obs = work_dir.join(format!("{i:06}.observations.csv"));
                let rej = work_dir.join(format!("{i:06}.rejects.csv"));
                let result = scan_source(s, &obs, &rej).and_then(|scanned| {
                    rt.block_on(load(pool, s, &scanned, &obs, &rej, replace))?;
                    Ok(scanned)
                });
                let _ = fs::remove_file(&obs);
                let _ = fs::remove_file(&rej);
                let n = done.fetch_add(1, Ordering::SeqCst) + 1;
                match result {
                    Ok(sc) => {
                        let total = {
                            let mut b = done_bytes.lock().unwrap();
                            *b += s.bytes;
                            *b
                        };
                        log::info!(
                            "[{n}/{}] {} rows={} oi_rows={} observations={} rejects={} {:.0}s {:.0} MB/s (run {:.0} MB/s)",
                            todo.len(),
                            s.path.display(),
                            sc.stats.data_rows,
                            sc.stats.oi_rows,
                            sc.stats.observations,
                            sc.stats.rejects,
                            sc.seconds,
                            s.bytes as f64 / sc.seconds / 1e6,
                            total as f64 / started.elapsed().as_secs_f64() / 1e6
                        );
                    }
                    Err(e) => {
                        log::error!("[{n}/{}] {} failed: {e:#}", todo.len(), s.path.display());
                        failures.lock().unwrap().push((s.path.display().to_string(), e));
                    }
                }
            });
        }
    });
    failures.into_inner().unwrap()
}

fn scan_source(s: &Source, obs_path: &Path, rej_path: &Path) -> Result<Scanned> {
    let header = read_header(&s.header_from)?;
    let source = s.path.display().to_string();
    let csv_writer = |p: &Path| -> Result<csv::Writer<BufWriter<File>>> {
        Ok(csv::WriterBuilder::new()
            .has_headers(false)
            .from_writer(BufWriter::new(File::create(p)?)))
    };
    let mut obs = csv_writer(obs_path)?;
    let mut rej = csv_writer(rej_path)?;
    let t = Instant::now();
    let stats = scan_gz(&s.path, &header, None, &mut |e| {
        match e {
            Event::Observation(o) => obs.write_record([
                o.ric.as_str(),
                &o.oi_date.map(|d| d.to_string()).unwrap_or_default(),
                &o.open_interest.to_string(),
                &o.row_type,
                &o.first_published_ns.to_string(),
                &o.last_published_ns.to_string(),
                &o.rows.to_string(),
                &source,
            ])?,
            Event::Reject(r) => rej.write_record([
                r.ric.as_str(),
                &r.published_ns.map(|v| v.to_string()).unwrap_or_default(),
                &r.raw_value,
                &r.raw_date,
                r.reason,
                &source,
            ])?,
        }
        Ok(())
    })?;
    obs.flush()?;
    rej.flush()?;
    // The source must not change while it is read.
    ensure!(
        identity(&s.path)? == (s.bytes, s.mtime_ns),
        "{} changed during the scan",
        s.path.display()
    );
    Ok(Scanned {
        members: stats.gzip_members,
        stats,
        seconds: t.elapsed().as_secs_f64(),
    })
}

async fn copy_file(conn: &mut PgConnection, statement: &str, path: &Path) -> Result<u64> {
    let mut copy = conn.copy_in_raw(statement).await?;
    let mut file = File::open(path)?;
    let mut buf = vec![0u8; 1 << 20];
    loop {
        let n = file.read(&mut buf)?;
        if n == 0 {
            break;
        }
        copy.send(&buf[..n]).await?;
    }
    Ok(copy.finish().await?)
}

async fn load(
    pool: &PgPool,
    s: &Source,
    sc: &Scanned,
    obs: &Path,
    rej: &Path,
    replace: bool,
) -> Result<()> {
    let path = s.path.display().to_string();
    let mut tx = pool.begin().await?;
    let existing: Option<(i64,)> = sqlx::query_as(
        "SELECT source_bytes FROM lseg_open_interest.sources WHERE source_path = $1 FOR UPDATE",
    )
    .bind(&path)
    .fetch_optional(&mut *tx)
    .await?;
    if existing.is_some() {
        if !replace {
            bail!("loaded by another run meanwhile");
        }
        sqlx::query("DELETE FROM lseg_open_interest.sources WHERE source_path = $1")
            .bind(&path)
            .execute(&mut *tx)
            .await?;
    }
    let st = &sc.stats;
    sqlx::query(
        "INSERT INTO lseg_open_interest.sources (source_path, source_kind, package, source_bytes,
            source_mtime_ns, gzip_members, data_rows, quoted_rows, oi_rows, observation_rows,
            reject_rows, min_published_ns, max_published_ns, rule_version, tool_version, scan_seconds)
         VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9,$10,$11,$12,$13,$14,$15,$16)",
    )
    .bind(&path)
    .bind(s.kind)
    .bind(&s.package)
    .bind(s.bytes as i64)
    .bind(s.mtime_ns)
    .bind(sc.members as i64)
    .bind(st.data_rows as i64)
    .bind(st.quoted_rows as i64)
    .bind(st.oi_rows as i64)
    .bind(st.observations as i64)
    .bind(st.rejects as i64)
    .bind(st.min_published_ns)
    .bind(st.max_published_ns)
    .bind(RULE_VERSION)
    .bind(concat!("tas_open_interest ", env!("CARGO_PKG_VERSION")))
    .bind(sc.seconds)
    .execute(&mut *tx)
    .await?;
    let n_obs = copy_file(
        &mut tx,
        "COPY lseg_open_interest.observations (ric, oi_date, open_interest, row_type,
            first_published_ns, last_published_ns, publication_rows, source_path)
         FROM STDIN (FORMAT csv, FORCE_NOT_NULL (row_type))",
        obs,
    )
    .await
    .context("copy observations")?;
    let n_rej = copy_file(
        &mut tx,
        "COPY lseg_open_interest.rejects (ric, published_ns, raw_value, raw_date, reason, source_path)
         FROM STDIN (FORMAT csv, FORCE_NOT_NULL (ric, raw_value, raw_date))",
        rej,
    )
    .await
    .context("copy rejects")?;
    ensure!(
        n_obs == st.observations && n_rej == st.rejects,
        "copied {n_obs} observations / {n_rej} rejects, scanned {} / {}",
        st.observations,
        st.rejects
    );
    tx.commit().await?;
    Ok(())
}

fn scan_to_csv(
    input: &Path,
    header_from: Option<&Path>,
    output: &Path,
    max_compressed_bytes: Option<u64>,
) -> Result<()> {
    let header = read_header(header_from.unwrap_or(input))?;
    let mut obs = csv::Writer::from_writer(BufWriter::new(File::create(output)?));
    let mut rej_path = output.as_os_str().to_owned();
    rej_path.push(".rejects");
    let mut rej = csv::Writer::from_writer(BufWriter::new(File::create(&rej_path)?));
    let t = Instant::now();
    let stats = scan_gz(input, &header, max_compressed_bytes, &mut |e| {
        match e {
            Event::Observation(o) => obs.serialize(o)?,
            Event::Reject(r) => rej.serialize(r)?,
        }
        Ok(())
    })?;
    obs.flush()?;
    rej.flush()?;
    let secs = t.elapsed().as_secs_f64();
    println!(
        "{}",
        serde_json::json!({
            "input": input, "stats": stats, "seconds": secs,
            "rows_per_second": stats.data_rows as f64 / secs,
        })
    );
    Ok(())
}
