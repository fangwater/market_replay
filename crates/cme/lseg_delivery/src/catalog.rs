//! Indexed key-prefix probes, a small SQLite catalogue, and conservative expiry cleanup.
//! No full tick scan, daily data inventory, service, or market-event JSON.
use crate::native_kline::{
    component,
    engine::{Config, Job, Metadata},
    io, DAY,
};
use crate::tasdb::{self, Tuning};
use anyhow::{bail, ensure, Context, Result};
use clap::Args;
use cme_tas_replay::product::encode_all_key;
use parquet::arrow::{
    arrow_reader::{ParquetRecordBatchReaderBuilder, RowSelection, RowSelector},
    ProjectionMask,
};
use parquet::file::statistics::Statistics;
use rocksdb::{ReadOptions, DB};
use serde::Deserialize;
use sqlx::{
    sqlite::{SqliteConnectOptions, SqliteJournalMode, SqliteSynchronous},
    Connection, Row, SqliteConnection,
};
use std::{
    collections::BTreeMap,
    fs::{self, File},
    path::{Path, PathBuf},
};

const ROOT: &str = "/mnt/nfs/30.100_cta_data/lseg";
const WORK: &str = "/mnt/nvme-raid0-28t/fanghaizhou/lseg_data/lseg_native_klines_20261112";
/// Written by `ib_kline_export` after every run (one entry per published RIC).
const IB_MANIFEST: &str =
    "/mnt/nvme-raid0-28t/fanghaizhou/lseg_data/runtime/ib_kline_export/ib_contracts.json";
const PLAN: &str =
    "/mnt/nvme-raid0-28t/fanghaizhou/lseg_data/artifacts/lseg_full_rocksdb_20261112/plan.json";

#[derive(Args)]
pub struct Build {
    #[arg(long, default_value = ROOT)]
    pub root: PathBuf,
    #[arg(long, default_value = WORK)]
    pub work: PathBuf,
    #[arg(long, default_value = PLAN)]
    pub plan: PathBuf,
    #[arg(long, default_value = "/mnt/nvme-raid0-28t/fanghaizhou/lseg_data")]
    pub project: PathBuf,
    #[arg(long)]
    pub output: Option<PathBuf>,
    #[arg(long)]
    pub year: Option<i32>,
    #[arg(long)]
    pub product: Option<String>,
    /// Remove only later UTC dates containing exclusively redundant expired status files.
    #[arg(long)]
    pub prune_expired_status: bool,
    /// IB contract manifest imported into `ib_contracts` (skipped if absent).
    #[arg(long, default_value = IB_MANIFEST)]
    pub ib_manifest: PathBuf,
}
/// Refresh only the IB tables of an existing catalog (no RocksDB access).
#[derive(Args)]
pub struct IbSync {
    #[arg(long, default_value = ROOT)]
    pub root: PathBuf,
    /// Defaults to <root>/meta/catalog.sqlite.
    #[arg(long)]
    pub database: Option<PathBuf>,
    #[arg(long, default_value = IB_MANIFEST)]
    pub ib_manifest: PathBuf,
    #[arg(long, default_value = WORK)]
    pub work: PathBuf,
}
#[derive(Args)]
pub struct Query {
    #[arg(
        long,
        default_value = "/mnt/nfs/30.100_cta_data/lseg/meta/catalog.sqlite"
    )]
    pub database: PathBuf,
    #[arg(long)]
    pub product: Option<String>,
    #[arg(long)]
    pub ric: Option<String>,
    #[arg(long)]
    pub year: Option<i32>,
    #[arg(long)]
    pub active: bool,
}

const SCHEMA: &str = r#"
CREATE TABLE contracts (
 source_year INTEGER NOT NULL, product TEXT NOT NULL, ric TEXT NOT NULL, exchange TEXT NOT NULL,
 first_trade_ns INTEGER, last_trade_ns INTEGER,
 first_quote_ns INTEGER, last_quote_ns INTEGER,
 first_status_ns INTEGER, last_status_ns INTEGER,
 first_trade_utc TEXT, last_trade_utc TEXT, first_quote_utc TEXT, last_quote_utc TEXT,
 last_status_utc TEXT, last_market_date TEXT, last_session_state TEXT NOT NULL DEFAULT 'UNKNOWN',
 expired_at_utc TEXT, expiry_evidence_utc TEXT, data_directory TEXT,
 PRIMARY KEY(source_year,product,ric)
);
CREATE INDEX contracts_product ON contracts(product,source_year,ric);
CREATE INDEX contracts_ric ON contracts(ric,source_year);
CREATE TABLE directories (
 source_year INTEGER NOT NULL, product TEXT NOT NULL, ric TEXT NOT NULL,
 utc_year INTEGER NOT NULL, directory TEXT NOT NULL,
 PRIMARY KEY(source_year,product,ric,utc_year)
);
CREATE VIEW product_latest AS
 WITH y AS (SELECT product,MAX(source_year) source_year FROM contracts
             WHERE last_market_date IS NOT NULL GROUP BY product)
 SELECT c.product,c.source_year,MAX(c.last_market_date) as_of_utc_date
 FROM contracts c JOIN y ON c.product=y.product AND c.source_year=y.source_year
 GROUP BY c.product,c.source_year;
CREATE VIEW active_contracts AS
 SELECT c.*,p.as_of_utc_date FROM contracts c JOIN product_latest p
 ON c.product=p.product AND c.source_year=p.source_year
 WHERE c.last_market_date=p.as_of_utc_date AND c.last_session_state<>'EXPIRED';
CREATE TABLE conventions (key TEXT PRIMARY KEY, value TEXT NOT NULL);
INSERT INTO conventions VALUES
 ('schema_version','1'),
 ('scope','futures TAS only; original case-sensitive RICs; source_year separates historical cycles'),
 ('active_definition','trade or quote on the product latest observed market UTC day in its latest source year, and no confirmed terminal expiry; data snapshot activity, not live trading state'),
 ('trade_clock','RocksDB Trade source-publication UTC time; not a scan for last exchange execution'),
 ('quote_clock','RocksDB sampled Quote UTC second bucket'),
 ('directory_usage','SELECT directory FROM directories WHERE product=?; read daily Parquet inside each returned directory'),
 ('status_cleanup','only dates strictly after an explicit EXPIRE; all rows EXPIRED/NO_CHANGE; directory has only status/session_status; source RocksDB retained'),
 ('history','Source-year namespaces are retained; repeated one-digit RIC years are not merged');
"#;

/// IB minute bars (`kline_ib.parquet`) are a separate, pure export from IB
/// MySQL. `tas_data = 0` marks IB-only RICs: no TAS for the RIC or any alias,
/// exchange directory taken from the product, directory holds only
/// `kline_ib.parquet`.
const IB_SCHEMA: &str = r#"
CREATE TABLE IF NOT EXISTS ib_contracts (
 ric TEXT PRIMARY KEY, con_id TEXT NOT NULL, ib_symbol TEXT NOT NULL, ib_local_symbol TEXT NOT NULL,
 ib_exchange TEXT NOT NULL, maturity_date TEXT NOT NULL, product TEXT NOT NULL, exchange TEXT NOT NULL,
 exchange_basis TEXT NOT NULL, tas_data INTEGER NOT NULL, first_day TEXT, last_day TEXT
);
CREATE INDEX IF NOT EXISTS ib_contracts_product ON ib_contracts(product,ric);
CREATE TABLE IF NOT EXISTS ib_directories (
 ric TEXT NOT NULL, utc_year INTEGER NOT NULL, directory TEXT NOT NULL, PRIMARY KEY(ric,utc_year)
);
CREATE VIEW IF NOT EXISTS ib_only_contracts AS SELECT * FROM ib_contracts WHERE tas_data=0;
INSERT OR REPLACE INTO conventions VALUES
 ('ib_scope','ib_contracts: IB MySQL minute bars exported as-is to kline_ib.parquet (comparison only; no fill or trim)'),
 ('ib_only','tas_data=0 (view ib_only_contracts): no TAS for the RIC or its aliases; exchange directory from the product; only kline_ib.parquet exists'),
 ('ib_directory_usage','SELECT directory FROM ib_directories WHERE ric=?; read <directory>/<UTC YYYYMMDD>/kline_ib.parquet');
"#;

#[derive(Deserialize)]
struct IbEntry {
    ric: String,
    con_id: String,
    ib_symbol: String,
    ib_local_symbol: String,
    ib_exchange: String,
    maturity_date: String,
    product: String,
    exchange: String,
    exchange_basis: String,
    tas_data: bool,
    first_day: String,
    last_day: String,
}

/// Replaces the IB tables from the manifest. Returns (contracts, IB-only).
async fn load_ib(
    conn: &mut SqliteConnection,
    root: &Path,
    manifest: &Path,
) -> Result<(usize, usize)> {
    let entries: Vec<IbEntry> = serde_json::from_slice(
        &fs::read(manifest).with_context(|| format!("read {}", manifest.display()))?,
    )?;
    sqlx::raw_sql(IB_SCHEMA).execute(&mut *conn).await?;
    sqlx::query("BEGIN").execute(&mut *conn).await?;
    sqlx::query("DELETE FROM ib_contracts")
        .execute(&mut *conn)
        .await?;
    sqlx::query("DELETE FROM ib_directories")
        .execute(&mut *conn)
        .await?;
    let mut ib_only = 0;
    for e in &entries {
        ib_only += usize::from(!e.tas_data);
        let day = |d: &str| (!d.is_empty()).then(|| d.to_string());
        sqlx::query("INSERT INTO ib_contracts VALUES (?,?,?,?,?,?,?,?,?,?,?,?)")
            .bind(&e.ric)
            .bind(&e.con_id)
            .bind(&e.ib_symbol)
            .bind(&e.ib_local_symbol)
            .bind(&e.ib_exchange)
            .bind(&e.maturity_date)
            .bind(&e.product)
            .bind(&e.exchange)
            .bind(&e.exchange_basis)
            .bind(e.tas_data)
            .bind(day(&e.first_day))
            .bind(day(&e.last_day))
            .execute(&mut *conn)
            .await?;
        let year = |d: &str| d.get(..4).and_then(|y| y.parse::<i32>().ok());
        if let (Some(lo), Some(hi)) = (year(&e.first_day), year(&e.last_day)) {
            for y in lo..=hi {
                let dir = root
                    .join(&e.exchange)
                    .join(y.to_string())
                    .join(component(&e.ric));
                sqlx::query("INSERT INTO ib_directories VALUES (?,?,?)")
                    .bind(&e.ric)
                    .bind(y)
                    .bind(dir.to_string_lossy().as_ref())
                    .execute(&mut *conn)
                    .await?;
            }
        }
    }
    sqlx::query("COMMIT").execute(&mut *conn).await?;
    Ok((entries.len(), ib_only))
}

/// Copies `local` next to `output` and renames it into place.
fn publish_sqlite(local: &Path, output: &Path) -> Result<()> {
    fs::create_dir_all(output.parent().context("catalog parent")?)?;
    let temp = output.with_extension("sqlite.partial");
    fs::copy(local, &temp)?;
    File::open(&temp)?.sync_all()?;
    fs::rename(&temp, output)?;
    File::open(output.parent().unwrap())?.sync_all()?;
    Ok(())
}

/// Exclusive, non-blocking advisory lock per catalog file: `build`, `ib-sync` and `l2-sync` all
/// copy the file, modify the copy and replace the original, so two of them must never overlap.
/// Held until the returned file is dropped. (The lock does not stop a binary without it.)
fn lock_catalog(work: &Path, output: &Path) -> Result<File> {
    fs::create_dir_all(work)?;
    let name: String = output
        .to_string_lossy()
        .chars()
        .map(|c| if c.is_ascii_alphanumeric() { c } else { '_' })
        .collect();
    let path = work.join(format!("catalog_writer{name}.lock"));
    let file = File::create(&path)?;
    if let Err(e) = file.try_lock() {
        anyhow::bail!(
            "another catalog writer holds {} ({e}); not touching {}",
            path.display(),
            output.display()
        );
    }
    Ok(file)
}

pub async fn ib_sync(a: IbSync) -> Result<()> {
    let output = a
        .database
        .clone()
        .unwrap_or_else(|| a.root.join("meta/catalog.sqlite"));
    let _lock = lock_catalog(&a.work, &output)?;
    fs::create_dir_all(&a.work)?;
    let local = a.work.join("catalog_ib_sync.sqlite.partial");
    fs::copy(&output, &local).with_context(|| format!("copy {}", output.display()))?;
    let mut conn = SqliteConnection::connect_with(
        &SqliteConnectOptions::new()
            .filename(&local)
            .journal_mode(SqliteJournalMode::Delete)
            .synchronous(SqliteSynchronous::Full),
    )
    .await?;
    let (n, ib_only) = load_ib(&mut conn, &a.root, &a.ib_manifest).await?;
    let integrity: String = sqlx::query_scalar("PRAGMA integrity_check")
        .fetch_one(&mut conn)
        .await?;
    ensure!(integrity == "ok", "SQLite integrity failure: {integrity}");
    conn.close().await?;
    publish_sqlite(&local, &output)?;
    fs::remove_file(&local)?;
    println!("{}: {n} IB contracts ({ib_only} IB-only)", output.display());
    Ok(())
}

fn utc(ns: u64) -> Result<String> {
    Ok(chrono::DateTime::from_timestamp(
        i64::try_from(ns / 1_000_000_000)?,
        (ns % 1_000_000_000) as u32,
    )
    .context("UTC timestamp")?
    .to_rfc3339_opts(chrono::SecondsFormat::Nanos, true))
}
fn year(ns: u64) -> Result<i32> {
    use chrono::Datelike;
    Ok(
        chrono::DateTime::from_timestamp(i64::try_from(ns / 1_000_000_000)?, 0)
            .context("UTC year")?
            .year(),
    )
}
fn upper(kind: u8, ric: &str) -> Result<Vec<u8>> {
    let mut out = encode_all_key(kind, ric, 0, 0, 0)?[..17].to_vec();
    for v in out.iter_mut().rev() {
        if *v != 255 {
            *v += 1;
            return Ok(out);
        }
        *v = 0;
    }
    anyhow::bail!("RIC prefix overflow")
}
fn key(key: &[u8], kind: u8) -> Result<(&str, u64)> {
    ensure!(
        key.len() == 31 && key[0] == kind,
        "invalid kind={kind} RocksDB key"
    );
    let b = &key[1..17];
    let len = b.iter().position(|v| *v == 0).unwrap_or(b.len());
    ensure!(b[len..].iter().all(|v| *v == 0), "invalid padded RIC");
    Ok((
        std::str::from_utf8(&b[..len])?,
        u64::from_be_bytes(key[17..25].try_into()?),
    ))
}
fn open(path: &Path, jobs: &[Job]) -> Result<DB> {
    tasdb::open_read_only(
        path,
        std::iter::once("default").chain(jobs.iter().map(|j| j.cf.as_str())),
        Tuning::default(),
    )
}

/// The last normalized row, projecting one row group rather than reading a whole status day.
fn last_state(path: &Path, ric: &str) -> Result<Option<String>> {
    if !path.try_exists()? {
        return Ok(None);
    }
    let b = ParquetRecordBatchReaderBuilder::try_new(File::open(path)?)?;
    let n = usize::try_from(b.metadata().file_metadata().num_rows())?;
    ensure!(n > 0, "empty status file {}", path.display());
    let projection = ProjectionMask::roots(
        b.parquet_schema(),
        [
            b.schema().index_of("ric")?,
            b.schema().index_of("session_state")?,
            b.schema().index_of("ts_utc_ns")?,
        ],
    );
    let reader = b
        .with_projection(projection)
        .with_row_selection(RowSelection::from(vec![
            RowSelector::skip(n - 1),
            RowSelector::select(1),
        ]))
        .with_batch_size(1)
        .build()?;
    for b in reader {
        let b = b?;
        if b.num_rows() == 0 {
            continue;
        }
        ensure!(
            io::string(&b, 0, "ric")? == ric,
            "status RIC mismatch {}",
            path.display()
        );
        ensure!(
            io::date(io::uint(&b, 0, "ts_utc_ns")?)?
                == path
                    .parent()
                    .and_then(Path::file_name)
                    .and_then(|v| v.to_str())
                    .context("status UTC directory")?,
            "status UTC date mismatch {}",
            path.display()
        );
        return Ok(Some(io::string(&b, 0, "session_state")?.into()));
    }
    anyhow::bail!("missing final status row {}", path.display())
}

fn dates(root: &Path, meta: &Metadata, ric: &str, lo: u64, hi: u64) -> Result<Vec<PathBuf>> {
    let a = year(lo)?;
    let b = year(hi)?;
    ensure!(
        (0..=2).contains(&(b - a)),
        "unexpected multi-cycle status range for {ric}"
    );
    let low = io::date(lo)?;
    let high = io::date(hi)?;
    let mut out = Vec::new();
    for y in a..=b {
        let dir = root
            .join(meta.venue(y, ric))
            .join(y.to_string())
            .join(component(ric));
        if !dir.try_exists()? {
            continue;
        }
        for e in fs::read_dir(dir)? {
            let e = e?;
            let name = e.file_name();
            let Some(name) = name.to_str() else {
                continue;
            };
            if name.len() == 8
                && name.bytes().all(|v| v.is_ascii_digit())
                && name >= low.as_str()
                && name <= high.as_str()
            {
                ensure!(e.file_type()?.is_dir(), "daily path is not a directory");
                out.push(e.path());
            }
        }
    }
    out.sort();
    ensure!(out.len() <= 1100, "unexpected daily directory count");
    Ok(out)
}

fn state_before(
    days: &[PathBuf],
    index: usize,
    ric: &str,
    cache: &mut BTreeMap<usize, Option<String>>,
) -> Result<Option<String>> {
    for i in (0..=index).rev() {
        if let std::collections::btree_map::Entry::Vacant(e) = cache.entry(i) {
            e.insert(last_state(&days[i].join("session_status.parquet"), ric)?);
        }
        if let Some(state) = &cache[&i] {
            return Ok(Some(state.clone()));
        }
    }
    Ok(None)
}

/// Locate the explicit terminal transition without scanning every day or using HRD guesses.
fn expiry(days: &[PathBuf], ric: &str) -> Result<Option<u64>> {
    if days.is_empty() {
        return Ok(None);
    }
    let mut cache = BTreeMap::new();
    if state_before(days, days.len() - 1, ric, &mut cache)?.as_deref() != Some("EXPIRED") {
        return Ok(None);
    }
    let (mut lo, mut hi) = (0, days.len() - 1);
    while lo < hi {
        let mid = (lo + hi) / 2;
        if state_before(days, mid, ric, &mut cache)?.as_deref() == Some("EXPIRED") {
            hi = mid
        } else {
            lo = mid + 1
        }
    }
    let path = days[lo].join("session_status.parquet");
    let b = ParquetRecordBatchReaderBuilder::try_new(File::open(&path)?)?;
    let projection = ProjectionMask::roots(
        b.parquet_schema(),
        ["ric", "ts_utc_ns", "session_state", "action"]
            .into_iter()
            .map(|n| b.schema().index_of(n))
            .collect::<std::result::Result<Vec<_>, _>>()?,
    );
    let mut at = None;
    for batch in b
        .with_projection(projection)
        .with_batch_size(8192)
        .build()?
    {
        let b = batch?;
        for row in 0..b.num_rows() {
            ensure!(io::string(&b, row, "ric")? == ric, "expiry RIC mismatch");
            if io::string(&b, row, "session_state")? == "EXPIRED"
                && io::string(&b, row, "action")? == "EXPIRE"
            {
                let t = io::uint(&b, row, "ts_utc_ns")?;
                ensure!(
                    io::date(t)?
                        == days[lo]
                            .file_name()
                            .unwrap()
                            .to_str()
                            .context("date encoding")?,
                    "expiry UTC date mismatch"
                );
                at = Some(at.map_or(t, |x: u64| x.min(t)));
            }
        }
    }
    Ok(at)
}

fn text_stat(
    b: &parquet::arrow::arrow_reader::ArrowReaderMetadata,
    column: usize,
    value: &[u8],
) -> bool {
    b.metadata().row_groups().iter().all(|g|matches!(g.column(column).statistics(),Some(Statistics::ByteArray(s)) if s.null_count_opt()==Some(0) && s.min_opt().is_some_and(|x|x.data()==value) && s.max_opt().is_some_and(|x|x.data()==value)))
}

/// Footer proof for *every* row group; absent/mixed statistics cause retention, not deletion.
fn valid_status_file(path: &Path, ric: &str, date: &str, terminal: bool) -> Result<bool> {
    if fs::symlink_metadata(path)?.file_type().is_symlink() {
        return Ok(false);
    }
    let file = File::open(path)?;
    let m = parquet::arrow::arrow_reader::ArrowReaderMetadata::load(&file, Default::default())?;
    if m.metadata().file_metadata().num_rows() <= 0 {
        return Ok(false);
    }
    let schema = m.schema();
    if !text_stat(&m, schema.index_of("ric")?, ric.as_bytes()) {
        return Ok(false);
    }
    let col = schema.index_of("ts_utc_ns")?;
    let start = u64::try_from(
        chrono::NaiveDate::parse_from_str(date, "%Y%m%d")?
            .and_hms_opt(0, 0, 0)
            .unwrap()
            .and_utc()
            .timestamp_nanos_opt()
            .context("day clock")?,
    )?;
    if !m.metadata().row_groups().iter().all(|g|matches!(g.column(col).statistics(),Some(Statistics::Int64(s)) if s.null_count_opt()==Some(0) && s.min_opt().is_some_and(|x| *x>=0 && (*x as u64)>=start) && s.max_opt().is_some_and(|x| *x>=0 && (*x as u64)<start+DAY))){return Ok(false);}
    Ok(!terminal
        || text_stat(&m, schema.index_of("session_state")?, b"EXPIRED")
            && text_stat(&m, schema.index_of("action")?, b"NO_CHANGE"))
}

fn prune(days: &[PathBuf], ric: &str, expired: u64) -> Result<u64> {
    let cutoff = io::date(expired)?;
    let mut removed = 0;
    for dir in days {
        let date = dir.file_name().unwrap().to_str().context("date encoding")?;
        if date <= cutoff.as_str() {
            continue;
        }
        let files = fs::read_dir(dir)?
            .map(|e| e.map(|x| x.path()))
            .collect::<std::io::Result<Vec<_>>>()?;
        if files.is_empty()
            || files.iter().any(|p| {
                !matches!(
                    p.file_name().and_then(|x| x.to_str()),
                    Some("status.parquet" | "session_status.parquet")
                )
            })
        {
            continue;
        }
        if !files
            .iter()
            .any(|p| p.file_name().unwrap() == "session_status.parquet")
        {
            continue;
        }
        let mut valid = true;
        let mut identities = Vec::new();
        for p in &files {
            let before = fs::symlink_metadata(p)?;
            valid &= valid_status_file(
                p,
                ric,
                date,
                p.file_name().unwrap() == "session_status.parquet",
            )?;
            identities.push((before.len(), before.modified()?));
        }
        if !valid {
            continue;
        }
        // Recheck contents and file identity immediately before removing only these known files.
        if fs::read_dir(dir)?.count() != files.len() {
            continue;
        }
        for (p, (size, modified)) in files.iter().zip(identities.iter()) {
            let m = fs::symlink_metadata(p)?;
            ensure!(
                !m.file_type().is_symlink() && m.len() == *size && m.modified()? == *modified,
                "status file changed before cleanup"
            );
        }
        for p in &files {
            fs::remove_file(p)?;
        }
        fs::remove_dir(dir)?;
        removed += 1;
    }
    Ok(removed)
}

/// Copies `l2_directories` and the `l2_*` convention rows of the catalog that this build replaces
/// (written by `l2-sync`; the build itself never walks the share). Nothing happens if there is no
/// such file or it has no such table. Returns the number of carried rows.
async fn carry_l2(conn: &mut SqliteConnection, old: &Path) -> Result<i64> {
    if !old.try_exists()? {
        return Ok(0);
    }
    sqlx::query("ATTACH DATABASE ? AS old")
        .bind(old.to_string_lossy().as_ref())
        .execute(&mut *conn)
        .await?;
    let has: i64 = sqlx::query_scalar(
        "SELECT COUNT(*) FROM old.sqlite_master WHERE type='table' AND name='l2_directories'",
    )
    .persistent(false)
    .fetch_one(&mut *conn)
    .await?;
    let mut n = 0;
    if has > 0 {
        sqlx::query("BEGIN").execute(&mut *conn).await?;
        let schema = L2_SCHEMA.split("INSERT OR REPLACE").next().unwrap_or("");
        sqlx::raw_sql(schema).execute(&mut *conn).await?;
        sqlx::query("DELETE FROM main.l2_directories")
            .execute(&mut *conn)
            .await?;
        let old_source: i64 = sqlx::query_scalar(
            "SELECT COUNT(*) FROM pragma_table_info('l2_directories','old') WHERE name='source'",
        )
        .persistent(false)
        .fetch_one(&mut *conn)
        .await?;
        let copy = if old_source > 0 {
            "INSERT INTO main.l2_directories(ric,utc_year,directory,first_day,last_day,days,source) \
             SELECT ric,utc_year,directory,first_day,last_day,days,source FROM old.l2_directories"
        } else {
            "INSERT INTO main.l2_directories(ric,utc_year,directory,first_day,last_day,days,source) \
             SELECT ric,utc_year,directory,first_day,last_day,days,'normalized' FROM old.l2_directories"
        };
        sqlx::query(copy).execute(&mut *conn).await?;
        sqlx::query(
            "INSERT OR REPLACE INTO main.conventions SELECT key,value FROM old.conventions WHERE key LIKE 'l2\\_%' ESCAPE '\\'",
        )
        .execute(&mut *conn)
        .await?;
        n = sqlx::query_scalar("SELECT COUNT(*) FROM main.l2_directories")
            .fetch_one(&mut *conn)
            .await?;
        sqlx::query("COMMIT").execute(&mut *conn).await?;
    }
    sqlx::query("DETACH DATABASE old")
        .execute(&mut *conn)
        .await?;
    Ok(n)
}

pub async fn build(a: Build) -> Result<()> {
    let output = a
        .output
        .clone()
        .unwrap_or_else(|| a.root.join("meta/catalog.sqlite"));
    ensure!(
        !(a.year.is_some() || a.product.is_some()) || a.output.is_some(),
        "filtered builds require a separate --output"
    );
    let _lock = lock_catalog(&a.work, &output)?;
    let cfg = Config {
        project: a.project.clone(),
        plan: a.plan.clone(),
        cache: PathBuf::new(),
        work: a.work.clone(),
        output: a.root.clone(),
        year: None,
        product: None,
        ric: None,
        workers: 1,
        publisher: None,
        replace: None,
        witnesses: None,
    };
    let meta = Metadata::load(&cfg)?;
    let plan = io::read_json(&a.plan)?;
    let jobs: Vec<Job> = serde_json::from_value(plan["jobs"].clone())?;
    let mut groups: BTreeMap<PathBuf, Vec<Job>> = BTreeMap::new();
    for j in jobs {
        if a.year.is_none_or(|y| j.year == y) && a.product.as_ref().is_none_or(|p| *p == j.product)
        {
            groups.entry(j.db.clone()).or_default().push(j);
        }
    }
    ensure!(!groups.is_empty(), "no selected source jobs");
    fs::create_dir_all(&a.work)?;
    std::env::set_var("SQLITE_TMPDIR", &a.work);
    let local = a.work.join("catalog_build.sqlite.partial");
    if local.try_exists()? {
        fs::remove_file(&local)?;
    }
    let options = SqliteConnectOptions::new()
        .filename(&local)
        .create_if_missing(true)
        .journal_mode(SqliteJournalMode::Delete)
        .synchronous(SqliteSynchronous::Full);
    let mut conn = SqliteConnection::connect_with(&options).await?;
    sqlx::raw_sql("PRAGMA cache_size=-16384; PRAGMA temp_store=FILE;")
        .execute(&mut conn)
        .await?;
    sqlx::raw_sql(SCHEMA).execute(&mut conn).await?;
    let mut done = 0;
    for (path, jobs) in groups {
        let before = fs::read(path.join("CURRENT"))?;
        for batch in jobs.chunks(64) {
            let db = open(&path, batch)?;
            sqlx::query("BEGIN").execute(&mut conn).await?;
            for job in batch {
                let cf = db.cf_handle(&job.cf).context("source CF")?;
                for kind in [1u8, 3, 13] {
                    let mut ro = ReadOptions::default();
                    ro.fill_cache(false);
                    ro.set_iterate_upper_bound(vec![kind + 1]);
                    let mut it = db.raw_iterator_cf_opt(cf, ro);
                    it.seek([kind]);
                    while let Some(k) = it.key() {
                        if k[0] != kind {
                            break;
                        }
                        let (ric, first) = key(k, kind)?;
                        let ric = ric.to_owned();
                        let high = upper(kind, &ric)?;
                        it.seek_for_prev(&high);
                        if it.key().is_some_and(|k| k >= high.as_slice()) {
                            it.prev();
                        }
                        let (last_ric, last) = key(it.key().context("last prefix key")?, kind)?;
                        ensure!(last_ric == ric && first <= last, "prefix range mismatch");
                        let sql = match kind {
                        1 => "INSERT INTO contracts(source_year,product,ric,exchange,first_trade_ns,last_trade_ns) VALUES(?,?,?,?,?,?) ON CONFLICT(source_year,product,ric) DO UPDATE SET first_trade_ns=MIN(COALESCE(first_trade_ns,excluded.first_trade_ns),excluded.first_trade_ns),last_trade_ns=MAX(COALESCE(last_trade_ns,excluded.last_trade_ns),excluded.last_trade_ns)",
                        3 => "INSERT INTO contracts(source_year,product,ric,exchange,first_quote_ns,last_quote_ns) VALUES(?,?,?,?,?,?) ON CONFLICT(source_year,product,ric) DO UPDATE SET first_quote_ns=MIN(COALESCE(first_quote_ns,excluded.first_quote_ns),excluded.first_quote_ns),last_quote_ns=MAX(COALESCE(last_quote_ns,excluded.last_quote_ns),excluded.last_quote_ns)",
                        _ => "INSERT INTO contracts(source_year,product,ric,exchange,first_status_ns,last_status_ns) VALUES(?,?,?,?,?,?) ON CONFLICT(source_year,product,ric) DO UPDATE SET first_status_ns=MIN(COALESCE(first_status_ns,excluded.first_status_ns),excluded.first_status_ns),last_status_ns=MAX(COALESCE(last_status_ns,excluded.last_status_ns),excluded.last_status_ns)",
                    };
                        sqlx::query(sql)
                            .bind(job.year)
                            .bind(&job.product)
                            .bind(&ric)
                            .bind(meta.venue(job.year, &ric))
                            .bind(i64::try_from(first)?)
                            .bind(i64::try_from(last)?)
                            .execute(&mut conn)
                            .await?;
                        it.seek(&high);
                    }
                    it.status()?;
                }
                done += 1;
                if done % 100 == 0 {
                    eprintln!("indexed {done} source CFs (prefix endpoints only)");
                }
            }
            sqlx::query("COMMIT").execute(&mut conn).await?;
            drop(db);
        }
        ensure!(
            before == fs::read(path.join("CURRENT"))?,
            "source CURRENT changed: {}",
            path.display()
        );
    }
    let mut offset = 0i64;
    let mut cursor = 0i64;
    let mut cleaned = 0u64;
    loop {
        let rows = sqlx::query(
            "SELECT rowid AS catalog_rowid,* FROM contracts WHERE rowid>? ORDER BY rowid LIMIT 256",
        )
        .bind(cursor)
        .fetch_all(&mut conn)
        .await?;
        if rows.is_empty() {
            break;
        }
        sqlx::query("BEGIN").execute(&mut conn).await?;
        for row in &rows {
            let source_year: i32 = row.get("source_year");
            let product: String = row.get("product");
            let ric: String = row.get("ric");
            let n = |s: &str| row.get::<Option<i64>, _>(s).map(|v| v as u64);
            let time = |s: &str| n(s).map(utc).transpose();
            let first_status = n("first_status_ns");
            let last_status = n("last_status_ns");
            let first = [n("first_trade_ns"), n("first_quote_ns"), first_status]
                .into_iter()
                .flatten()
                .min()
                .context("first observation")?;
            let last = [n("last_trade_ns"), n("last_quote_ns"), last_status]
                .into_iter()
                .flatten()
                .max()
                .context("last observation")?;
            let mut primary = None;
            let (first_year, last_year) = (year(first)?, year(last)?);
            ensure!(
                (0..=2).contains(&(last_year - first_year)),
                "unexpected multi-cycle observation range for {ric}"
            );
            for y in first_year..=last_year {
                let p = a
                    .root
                    .join(meta.venue(y, &ric))
                    .join(y.to_string())
                    .join(component(&ric));
                if p.try_exists()? {
                    sqlx::query("INSERT INTO directories VALUES(?,?,?,?,?)")
                        .bind(source_year)
                        .bind(&product)
                        .bind(&ric)
                        .bind(y)
                        .bind(p.to_string_lossy().as_ref())
                        .execute(&mut conn)
                        .await?;
                    if y == source_year {
                        primary = Some(p.to_string_lossy().to_string());
                    }
                }
            }
            let mut state = "UNKNOWN".to_string();
            let mut expired = None;
            let mut days = None;
            if let (Some(lo), Some(hi)) = (first_status, last_status) {
                let p = a
                    .root
                    .join(meta.venue(year(hi)?, &ric))
                    .join(year(hi)?.to_string())
                    .join(component(&ric))
                    .join(io::date(hi)?)
                    .join("session_status.parquet");
                if let Some(s) = last_state(&p, &ric)? {
                    state = s;
                } else {
                    let d = dates(&a.root, &meta, &ric, lo, hi)?;
                    if !d.is_empty() {
                        state = state_before(&d, d.len() - 1, &ric, &mut BTreeMap::new())?
                            .unwrap_or(state);
                    }
                    days = Some(d);
                }
                if state == "EXPIRED" {
                    let d = match days.take() {
                        Some(d) => d,
                        None => dates(&a.root, &meta, &ric, lo, hi)?,
                    };
                    expired = expiry(&d, &ric)?;
                    if a.prune_expired_status {
                        if let Some(at) = expired {
                            cleaned += prune(&d, &ric, at)?;
                        }
                    }
                }
            }
            let market = [n("last_trade_ns"), n("last_quote_ns")]
                .into_iter()
                .flatten()
                .max()
                .map(|v| utc(v).map(|s| s[..10].to_owned()))
                .transpose()?;
            let evidence = if state == "EXPIRED" {
                time("last_status_ns")?
            } else {
                None
            };
            sqlx::query("UPDATE contracts SET first_trade_utc=?,last_trade_utc=?,first_quote_utc=?,last_quote_utc=?,last_status_utc=?,last_market_date=?,last_session_state=?,expired_at_utc=?,expiry_evidence_utc=?,data_directory=? WHERE source_year=? AND product=? AND ric=?")
              .bind(time("first_trade_ns")?).bind(time("last_trade_ns")?).bind(time("first_quote_ns")?).bind(time("last_quote_ns")?).bind(time("last_status_ns")?)
              .bind(market).bind(state).bind(expired.map(utc).transpose()?).bind(evidence).bind(primary).bind(source_year).bind(&product).bind(&ric).execute(&mut conn).await?;
        }
        sqlx::query("COMMIT").execute(&mut conn).await?;
        offset += rows.len() as i64;
        cursor = rows.last().unwrap().get("catalog_rowid");
        eprintln!("catalogued {offset} source-year/RIC entries; removed {cleaned} redundant post-expiry status dates");
    }
    let count: i64 = sqlx::query_scalar("SELECT COUNT(*) FROM contracts")
        .fetch_one(&mut conn)
        .await?;
    let active: i64 = sqlx::query_scalar("SELECT COUNT(*) FROM active_contracts")
        .fetch_one(&mut conn)
        .await?;
    let integrity: String = sqlx::query_scalar("PRAGMA integrity_check")
        .fetch_one(&mut conn)
        .await?;
    ensure!(integrity == "ok", "SQLite integrity failure: {integrity}");
    if a.ib_manifest.exists() {
        let (n, ib_only) = load_ib(&mut conn, &a.root, &a.ib_manifest).await?;
        eprintln!("imported {n} IB contracts ({ib_only} IB-only)");
    }
    let l2 = carry_l2(&mut conn, &output).await?;
    if l2 > 0 {
        eprintln!("carried {l2} l2_directories rows from the replaced catalog");
    }
    conn.close().await?;
    publish_sqlite(&local, &output)?;
    println!("{}: {count} source-year/RIC entries; {active} active at product snapshot dates; {cleaned} redundant status-only dates removed",output.display());
    Ok(())
}

pub async fn list(a: Query) -> Result<()> {
    let mut conn = SqliteConnection::connect_with(
        &SqliteConnectOptions::new()
            .filename(&a.database)
            .read_only(true),
    )
    .await?;
    let q = if a.active {
        "SELECT source_year,exchange,product,ric,last_trade_utc,last_quote_utc,last_session_state,expired_at_utc,data_directory FROM active_contracts WHERE (? IS NULL OR product=?) AND (? IS NULL OR ric=?) AND (? IS NULL OR source_year=?) ORDER BY source_year,product,ric"
    } else {
        "SELECT source_year,exchange,product,ric,last_trade_utc,last_quote_utc,last_session_state,expired_at_utc,data_directory FROM contracts WHERE (? IS NULL OR product=?) AND (? IS NULL OR ric=?) AND (? IS NULL OR source_year=?) ORDER BY source_year,product,ric"
    };
    use futures_util::TryStreamExt;
    let mut stream = sqlx::query(q)
        .bind(&a.product)
        .bind(&a.product)
        .bind(&a.ric)
        .bind(&a.ric)
        .bind(a.year)
        .bind(a.year)
        .fetch(&mut conn);
    let stdout = std::io::stdout();
    let mut w = csv::Writer::from_writer(stdout.lock());
    w.write_record([
        "source_year",
        "exchange",
        "product",
        "ric",
        "last_trade_utc",
        "last_quote_utc",
        "last_session_state",
        "expired_at_utc",
        "data_directory",
    ])?;
    while let Some(row) = stream.try_next().await? {
        let mut out = vec![row.get::<i32, _>(0).to_string()];
        for c in 1..9 {
            out.push(row.get::<Option<String>, _>(c).unwrap_or_default());
        }
        w.write_record(out)?;
    }
    w.flush()?;
    Ok(())
}

/// Refresh only the L2 table of an existing catalog: which (RIC, UTC year) directories hold
/// `l2_<freq>ms.parquet` and for which days. Stat-only walk of the share, one thread.
#[derive(Args)]
pub struct L2Sync {
    #[arg(long, default_value = ROOT)]
    pub root: PathBuf,
    /// Defaults to <root>/meta/catalog.sqlite.
    #[arg(long)]
    pub database: Option<PathBuf>,
    #[arg(long, default_value = "l2_250ms.parquet")]
    pub file_name: String,
    #[arg(long, default_value = WORK)]
    pub work: PathBuf,
}

/// Added after the first release of the table (older catalogs lack it).
const L2_SOURCE_COLUMN: &str =
    "ALTER TABLE l2_directories ADD COLUMN source TEXT NOT NULL DEFAULT 'normalized'";

/// Marker file in a RIC-year directory whose L2 files were built from Raw LL2.
pub const L2_SOURCE_FILE: &str = "l2_source";

/// Adds `l2_directories.source` to a catalog that has the table without it.
async fn ensure_l2_source_column(conn: &mut SqliteConnection) -> Result<()> {
    let has: i64 = sqlx::query_scalar(
        "SELECT COUNT(*) FROM pragma_table_info('l2_directories') WHERE name='source'",
    )
    .persistent(false)
    .fetch_one(&mut *conn)
    .await?;
    if has == 0 {
        sqlx::query(L2_SOURCE_COLUMN).execute(&mut *conn).await?;
    }
    Ok(())
}

const L2_SCHEMA: &str = r#"
CREATE TABLE IF NOT EXISTS l2_directories (
 ric TEXT NOT NULL, utc_year INTEGER NOT NULL, directory TEXT NOT NULL,
 first_day TEXT NOT NULL, last_day TEXT NOT NULL, days INTEGER NOT NULL,
 source TEXT NOT NULL DEFAULT 'normalized' CHECK (source IN ('normalized','raw')), PRIMARY KEY(ric,utc_year)
);
INSERT OR REPLACE INTO conventions VALUES
 ('l2_scope','l2_directories: L2 (10 levels) sampled on a 250 ms grid, l2_250ms.parquet; only rows where the book changed plus anchor rows; includes RICs without TAS (e.g. cfe weeklies); the source of every RIC-year is in column source'),
 ('l2_source','l2_directories.source: normalized = Normalized LL2 (the book as the vendor delivers it); raw = Raw LL2 (Legacy Level 2 slot updates, book rebuilt by us: slots never assigned are empty, a REFRESH drops earlier knowledge; see docs/lseg/l2_raw.md). A raw RIC-year has the file <directory>/l2_source with the text raw. A RIC-year has exactly one source'),
 ('l2_directory_usage','SELECT directory FROM l2_directories WHERE ric=?; read <directory>/<UTC YYYYMMDD>/l2_250ms.parquet (days between first_day and last_day may have gaps)');
"#;

/// `raw` if the RIC-year directory carries the marker file, else `normalized`.
fn l2_source_of(dir: &Path) -> Result<&'static str> {
    let marker = dir.join(L2_SOURCE_FILE);
    match fs::read_to_string(&marker) {
        Ok(t) if t.trim() == "raw" => Ok("raw"),
        Ok(t) => bail!("{}: unknown L2 source {:?}", marker.display(), t.trim()),
        Err(e) if e.kind() == std::io::ErrorKind::NotFound => Ok("normalized"),
        Err(e) => Err(e).with_context(|| format!("read {}", marker.display())),
    }
}

struct L2Dir {
    ric: String,
    year: i32,
    dir: PathBuf,
    first: String,
    last: String,
    days: i64,
    source: &'static str,
}

fn l2_walk(root: &Path, file_name: &str) -> Result<Vec<L2Dir>> {
    let names = |p: &Path| -> Result<Vec<String>> {
        let mut v = Vec::new();
        for e in fs::read_dir(p)? {
            v.push(e?.file_name().to_string_lossy().into_owned());
        }
        v.sort();
        Ok(v)
    };
    let mut out = Vec::new();
    for ex in names(root)? {
        if ex == "meta" || ex.starts_with('.') {
            continue;
        }
        let ex_dir = root.join(&ex);
        if !ex_dir.is_dir() {
            continue;
        }
        for year in names(&ex_dir)? {
            let Ok(y) = year.parse::<i32>() else { continue };
            let year_dir = ex_dir.join(&year);
            for ric_dir in names(&year_dir)? {
                let dir = year_dir.join(&ric_dir);
                let mut first = String::new();
                let mut last = String::new();
                let mut days = 0i64;
                for day in names(&dir)? {
                    if day.len() == 8 && dir.join(&day).join(file_name).is_file() {
                        if first.is_empty() {
                            first = day.clone();
                        }
                        last = day;
                        days += 1;
                    }
                }
                if days > 0 {
                    let source = l2_source_of(&dir)?;
                    out.push(L2Dir {
                        ric: tasdb::unescape_component(&ric_dir),
                        year: y,
                        dir,
                        first,
                        last,
                        days,
                        source,
                    });
                }
            }
        }
    }
    Ok(out)
}

pub async fn l2_sync(a: L2Sync) -> Result<()> {
    let output = a
        .database
        .clone()
        .unwrap_or_else(|| a.root.join("meta/catalog.sqlite"));
    let _lock = lock_catalog(&a.work, &output)?;
    let found = l2_walk(&a.root, &a.file_name)?;
    fs::create_dir_all(&a.work)?;
    let local = a.work.join("catalog_l2_sync.sqlite.partial");
    fs::copy(&output, &local).with_context(|| format!("copy {}", output.display()))?;
    let mut conn = SqliteConnection::connect_with(
        &SqliteConnectOptions::new()
            .filename(&local)
            .journal_mode(SqliteJournalMode::Delete)
            .synchronous(SqliteSynchronous::Full),
    )
    .await?;
    sqlx::raw_sql(L2_SCHEMA).execute(&mut conn).await?;
    ensure_l2_source_column(&mut conn).await?;
    sqlx::query("BEGIN").execute(&mut conn).await?;
    sqlx::query("DELETE FROM l2_directories")
        .execute(&mut conn)
        .await?;
    for d in &found {
        sqlx::query(
            "INSERT INTO l2_directories(ric,utc_year,directory,first_day,last_day,days,source) VALUES (?,?,?,?,?,?,?)",
        )
            .bind(&d.ric)
            .bind(d.year)
            .bind(d.dir.to_string_lossy().as_ref())
            .bind(&d.first)
            .bind(&d.last)
            .bind(d.days)
            .bind(d.source)
            .execute(&mut conn)
            .await?;
    }
    sqlx::query("COMMIT").execute(&mut conn).await?;
    let integrity: String = sqlx::query_scalar("PRAGMA integrity_check")
        .fetch_one(&mut conn)
        .await?;
    ensure!(integrity == "ok", "SQLite integrity failure: {integrity}");
    conn.close().await?;
    publish_sqlite(&local, &output)?;
    fs::remove_file(&local)?;
    println!(
        "{}: {} L2 directories, {} files",
        output.display(),
        found.len(),
        found.iter().map(|d| d.days).sum::<i64>()
    );
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::native_kline::{
        io::StatusRow,
        session::{Decoder, State},
    };
    use parquet::arrow::ArrowWriter;

    #[test]
    fn catalog_writers_exclude_each_other_per_file() {
        let t = tempfile::tempdir().unwrap();
        let (a, b) = (t.path().join("a.sqlite"), t.path().join("b.sqlite"));
        let first = lock_catalog(t.path(), &a).unwrap();
        assert!(lock_catalog(t.path(), &a).is_err());
        let _other = lock_catalog(t.path(), &b).unwrap();
        drop(first);
        assert!(lock_catalog(t.path(), &a).is_ok());
    }

    #[tokio::test(flavor = "current_thread")]
    async fn build_carries_over_l2_rows_only_when_present() {
        let t = tempfile::tempdir().unwrap();
        let old = t.path().join("old.sqlite");
        // No old file: nothing happens.
        // A file database like the build's (an in-memory one is shared-cache and keeps the
        // attached file's schema).
        let mut c = SqliteConnection::connect_with(
            &SqliteConnectOptions::new()
                .filename(t.path().join("new.sqlite"))
                .create_if_missing(true)
                .journal_mode(SqliteJournalMode::Delete),
        )
        .await
        .unwrap();
        sqlx::raw_sql(SCHEMA).execute(&mut c).await.unwrap();
        assert_eq!(carry_l2(&mut c, &old).await.unwrap(), 0);
        // Old catalog without the table: nothing happens.
        let mut o = SqliteConnection::connect_with(
            &SqliteConnectOptions::new()
                .filename(&old)
                .create_if_missing(true)
                .journal_mode(SqliteJournalMode::Delete),
        )
        .await
        .unwrap();
        sqlx::raw_sql(SCHEMA).execute(&mut o).await.unwrap();
        o.close().await.unwrap();
        assert_eq!(carry_l2(&mut c, &old).await.unwrap(), 0);
        let none: i64 =
            sqlx::query_scalar("SELECT COUNT(*) FROM sqlite_master WHERE name='l2_directories'")
                .fetch_one(&mut c)
                .await
                .unwrap();
        assert_eq!(none, 0);
        // Old catalog with L2 rows and conventions: carried exactly.
        let mut o = SqliteConnection::connect_with(
            &SqliteConnectOptions::new()
                .filename(&old)
                .journal_mode(SqliteJournalMode::Delete),
        )
        .await
        .unwrap();
        sqlx::raw_sql(L2_SCHEMA).execute(&mut o).await.unwrap();
        sqlx::query("INSERT INTO l2_directories(ric,utc_year,directory,first_day,last_day,days) VALUES ('VXZ26',2026,'/r/cfe/2026/VXZ26','20260105','20261110',7)")
            .execute(&mut o)
            .await
            .unwrap();
        o.close().await.unwrap();
        assert_eq!(carry_l2(&mut c, &old).await.unwrap(), 1);
        let row: (String, i64, String, String) =
            sqlx::query_as("SELECT ric,days,last_day,source FROM l2_directories")
                .fetch_one(&mut c)
                .await
                .unwrap();
        assert_eq!(
            row,
            (
                "VXZ26".to_string(),
                7,
                "20261110".to_string(),
                "normalized".to_string()
            )
        );
        let conv: Vec<String> =
            sqlx::query_scalar("SELECT key FROM conventions WHERE key LIKE 'l2%' ORDER BY key")
                .fetch_all(&mut c)
                .await
                .unwrap();
        assert_eq!(conv, vec!["l2_directory_usage", "l2_scope", "l2_source"]);
        // The regular conventions are untouched.
        let n: i64 =
            sqlx::query_scalar("SELECT COUNT(*) FROM conventions WHERE key NOT LIKE 'l2%'")
                .fetch_one(&mut c)
                .await
                .unwrap();
        assert!(n >= 8);
    }

    #[tokio::test(flavor = "current_thread")]
    async fn ib_manifest_import_marks_ib_only_and_directories() {
        let t = tempfile::tempdir().unwrap();
        let manifest = t.path().join("ib_contracts.json");
        fs::write(&manifest, r#"[
 {"ric":"ESZ26","con_id":"1","ib_symbol":"ES","ib_local_symbol":"ESZ6","ib_exchange":"CME","maturity_date":"20261218",
  "product":"ES","exchange":"cme","exchange_basis":"ric","tas_data":true,"first_day":"20251218","last_day":"20261116"},
 {"ric":"X/1","con_id":"2","ib_symbol":"X","ib_local_symbol":"X1","ib_exchange":"EUREX","maturity_date":"20270101",
  "product":"X","exchange":"eurex","exchange_basis":"product","tas_data":false,"first_day":"20261110","last_day":"20261116"}
]"#).unwrap();
        let mut c = SqliteConnection::connect("sqlite::memory:").await.unwrap();
        sqlx::raw_sql(SCHEMA).execute(&mut c).await.unwrap();
        let (n, only) = load_ib(&mut c, Path::new("/r"), &manifest).await.unwrap();
        assert_eq!((n, only), (2, 1));
        // Re-import replaces rather than duplicates.
        load_ib(&mut c, Path::new("/r"), &manifest).await.unwrap();
        let ib_only: Vec<String> = sqlx::query_scalar("SELECT ric FROM ib_only_contracts")
            .fetch_all(&mut c)
            .await
            .unwrap();
        assert_eq!(ib_only, vec!["X/1".to_string()]);
        let dirs: Vec<String> =
            sqlx::query_scalar("SELECT directory FROM ib_directories ORDER BY ric,utc_year")
                .fetch_all(&mut c)
                .await
                .unwrap();
        assert_eq!(
            dirs,
            vec![
                "/r/cme/2025/ESZ26",
                "/r/cme/2026/ESZ26",
                "/r/eurex/2026/X%2F1"
            ]
        );
    }

    fn status(dir: &Path, ric: &str, at: u64, previous: State, qualifier: &str) -> Result<()> {
        fs::create_dir_all(dir)?;
        let row = StatusRow {
            at,
            before: previous,
            decoded: Decoder::new(true).decode(qualifier, previous),
            qualifiers: qualifier.into(),
        };
        let batch = io::status_batch("JY", ric, &[row])?;
        let mut w = ArrowWriter::try_new(
            File::create(dir.join("session_status.parquet"))?,
            batch.schema(),
            None,
        )?;
        w.write(&batch)?;
        w.close()?;
        Ok(())
    }

    #[test]
    fn prune_retains_expiry_day_quotes_and_uncertain_status() -> Result<()> {
        let base = PathBuf::from(WORK).join(format!("catalog_test_{}", std::process::id()));
        let ricdir = base.join("cme/2026/JYQ26");
        if base.exists() {
            fs::remove_dir_all(&base)?;
        }
        let at = io::parse_ns("2026-08-17T14:16:00Z").unwrap();
        let expiry_day = ricdir.join("20260817");
        status(
            &expiry_day,
            "JYQ26",
            at,
            State::Trading,
            "4[HALT_REASN];6[SECUR_ST];NH[HALT_RSN]",
        )?;
        let redundant = ricdir.join("20260818");
        status(
            &redundant,
            "JYQ26",
            at + DAY,
            State::Expired,
            "0[HALT_REASN]",
        )?;
        fs::copy(
            redundant.join("session_status.parquet"),
            redundant.join("status.parquet"),
        )?;
        let quote = ricdir.join("20260819");
        status(
            &quote,
            "JYQ26",
            at + 2 * DAY,
            State::Expired,
            "0[HALT_REASN]",
        )?;
        fs::write(quote.join("quote.parquet"), b"must stay")?;
        let unknown = ricdir.join("20260820");
        status(&unknown, "JYQ26", at + 3 * DAY, State::Unknown, "")?;
        let wrong_ric = ricdir.join("20260821");
        status(
            &wrong_ric,
            "OTHER",
            at + 4 * DAY,
            State::Expired,
            "0[HALT_REASN]",
        )?;
        let wrong_day = ricdir.join("20260822");
        status(
            &wrong_day,
            "JYQ26",
            at + 3 * DAY,
            State::Expired,
            "0[HALT_REASN]",
        )?;
        assert_eq!(
            expiry(
                &[expiry_day.clone(), redundant.clone(), quote.clone()],
                "JYQ26"
            )?,
            Some(at)
        );
        assert_eq!(
            prune(
                &[
                    expiry_day.clone(),
                    redundant.clone(),
                    quote.clone(),
                    unknown.clone(),
                    wrong_ric.clone(),
                    wrong_day.clone()
                ],
                "JYQ26",
                at
            )?,
            1
        );
        assert!(!redundant.exists());
        for p in [expiry_day, quote, unknown, wrong_ric, wrong_day] {
            assert!(p.join("session_status.parquet").exists());
        }
        fs::remove_dir_all(base)?;
        Ok(())
    }

    #[tokio::test(flavor = "current_thread")]
    async fn active_view_uses_market_day_and_keeps_historical_cycles_separate() -> Result<()> {
        let mut c = SqliteConnection::connect("sqlite::memory:").await?;
        sqlx::raw_sql(SCHEMA).execute(&mut c).await?;
        for (year, ric, day, state) in [
            (2010, "JYZ0", "2010-12-01", "UNKNOWN"),
            (2020, "JYZ0", "2020-12-01", "UNKNOWN"),
            (2026, "JYQ26", "2026-08-17", "EXPIRED"),
            (2026, "JYZ26", "2026-11-06", "SCHEDULED_CLOSED"),
            (2026, "JYF27", "2026-11-06", "UNKNOWN"),
            (2026, "JYG27", "2026-11-05", "UNKNOWN"),
        ] {
            sqlx::query("INSERT INTO contracts(source_year,product,ric,exchange,last_market_date,last_session_state) VALUES(?,'JY',?,'cme',?,?)").bind(year).bind(ric).bind(day).bind(state).execute(&mut c).await?;
        }
        sqlx::query("INSERT INTO contracts(source_year,product,ric,exchange,last_session_state) VALUES(2026,'JY','JYM29','cme','PRE_OPEN')").execute(&mut c).await?;
        let r: Vec<String> = sqlx::query_scalar("SELECT ric FROM active_contracts ORDER BY ric")
            .fetch_all(&mut c)
            .await?;
        assert_eq!(r, vec!["JYF27", "JYZ26"]);
        let n: i64 = sqlx::query_scalar("SELECT COUNT(*) FROM contracts WHERE ric='JYZ0'")
            .fetch_one(&mut c)
            .await?;
        assert_eq!(n, 2);
        Ok(())
    }

    #[tokio::test(flavor = "current_thread")]
    async fn l2_source_marker_column_and_old_catalogs() {
        // A catalog written before the column existed: carried with source 'normalized'.
        let t = tempfile::tempdir().unwrap();
        let old = t.path().join("old.sqlite");
        let mut o = SqliteConnection::connect_with(
            &SqliteConnectOptions::new()
                .filename(&old)
                .create_if_missing(true)
                .journal_mode(SqliteJournalMode::Delete),
        )
        .await
        .unwrap();
        sqlx::raw_sql(SCHEMA).execute(&mut o).await.unwrap();
        sqlx::raw_sql(
            "CREATE TABLE l2_directories (ric TEXT NOT NULL, utc_year INTEGER NOT NULL, directory TEXT NOT NULL, \
             first_day TEXT NOT NULL, last_day TEXT NOT NULL, days INTEGER NOT NULL, PRIMARY KEY(ric,utc_year));\
             INSERT INTO l2_directories VALUES ('ESH24',2024,'/r/cme/2024/ESH24','20240102','20240315',50);",
        )
        .execute(&mut o)
        .await
        .unwrap();
        // the in-place migration of a catalog that has the old table
        ensure_l2_source_column(&mut o).await.unwrap();
        ensure_l2_source_column(&mut o).await.unwrap(); // idempotent
        let src: String = sqlx::query_scalar("SELECT source FROM l2_directories")
            .fetch_one(&mut o)
            .await
            .unwrap();
        assert_eq!(src, "normalized");
        o.close().await.unwrap();

        // the marker decides the source of a walked RIC-year directory
        let d = t.path().join("cme/2024/CLF24");
        fs::create_dir_all(&d).unwrap();
        assert_eq!(l2_source_of(&d).unwrap(), "normalized");
        fs::write(d.join(L2_SOURCE_FILE), "raw\n").unwrap();
        assert_eq!(l2_source_of(&d).unwrap(), "raw");
        fs::write(d.join(L2_SOURCE_FILE), "other\n").unwrap();
        assert!(l2_source_of(&d).is_err());
    }
}
