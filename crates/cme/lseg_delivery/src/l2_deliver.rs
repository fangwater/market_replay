//! Delivery of the washed L2 databases into the NFS delivery tree as
//! `<exchange>/<UTC year>/<RIC>/<UTC YYYYMMDD>/l2_<freq>ms.parquet`.
//!
//! The only publish routine is `crate::publish` (shared with `lseg-daily`):
//! temp file + fsync + footer check + no-overwrite hard link. Per batch the
//! driver
//!   1. skips (RIC, day)s whose target already is a valid L2 file (footer
//!      checks, no re-extract), and records a differing file as a problem
//!      without touching it,
//!   2. extracts the rest into a local NVMe staging directory (the slices were
//!      verified value for value against an independent recomputation),
//!   3. publishes the staged files with a fixed pool of threads (first / middle
//!      / last of the batch get a full SHA-256 and a read-back from the share),
//!   4. deletes the staging directory.
//!
//! Extraction of batch k+1 overlaps the publishing of batch k (at most two
//! batches are staged). Only problems are written to the share
//! (`meta/problems/l2_deliver/problems.csv`); progress lives in a JSON file on
//! the local disk. The run pauses while one of the server's jobs runs, is
//! queued or is due within ten minutes (`l2_backfill::Gate`).
use crate::l2_backfill::{spawn_gate, write_json_atomic, Gate};
use crate::l2_db::{self, update_prefix};
use crate::l2_extract::{extract_rics_with, open_dbs, ExtractConfig};
use crate::l2_slice::{schema, Layout, Params};
use crate::publish::{check_footer, copy_one_timed, sha256_file, CopyTiming, Part};
use crate::tasdb::{SharedCache, Tuning};
use anyhow::{bail, ensure, Context, Result};
use parquet::file::reader::{FileReader, SerializedFileReader};
use parquet::file::statistics::Statistics;
use rocksdb::{IteratorMode, ReadOptions, DB};
use serde_json::{json, Value};
use std::collections::{BTreeMap, BTreeSet, HashSet, VecDeque};
use std::fs::{self, File};
use std::io::Write;
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicBool, AtomicU64, AtomicUsize, Ordering};
use std::sync::{mpsc, Arc, Mutex};
use std::time::{Duration, Instant};

pub fn load_exchange_map(path: &Path) -> Result<BTreeMap<String, String>> {
    let mut map = BTreeMap::new();
    for (n, line) in fs::read_to_string(path)?.lines().enumerate() {
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

/// `YYYY-MM-DD` of a day number (days since 1970-01-01).
pub fn day_string(dn: i64) -> String {
    let (y, m, d) = l2_db::civil_from_days(dn);
    format!("{y:04}-{m:02}-{d:02}")
}

// ------------------------------------------------------------ footers

pub struct FooterInfo {
    pub rows: u64,
    pub ric: Option<String>,
    pub names: Vec<String>,
}

pub fn footer_info(path: &Path) -> Result<FooterInfo> {
    let reader = SerializedFileReader::new(File::open(path)?)?;
    let md = reader.metadata();
    let names = md
        .file_metadata()
        .schema_descr()
        .root_schema()
        .get_fields()
        .iter()
        .map(|f| f.name().to_string())
        .collect::<Vec<_>>();
    let mut ric = None;
    if md.num_row_groups() > 0 {
        for col in md.row_group(0).columns() {
            if col.column_descr().name() == "ric" {
                if let Some(Statistics::ByteArray(s)) = col.statistics() {
                    ric = s
                        .min_opt()
                        .map(|v| String::from_utf8_lossy(v.data()).into_owned());
                }
            }
        }
    }
    Ok(FooterInfo {
        rows: md.file_metadata().num_rows() as u64,
        ric,
        names,
    })
}

pub fn expected_names() -> Vec<String> {
    schema().fields().iter().map(|f| f.name().clone()).collect()
}

#[derive(Debug, PartialEq, Eq)]
pub enum Existing {
    Missing,
    /// A complete L2 file of this RIC-day is there: nothing to do.
    Valid,
    /// Something else is at the target: recorded as a problem, never touched.
    Invalid(String),
}

/// Cheap check of an already published target (stat + footer only).
pub fn existing(target: &Path, ric: &str, day8: &str, names: &[String]) -> Existing {
    if !target.exists() {
        return Existing::Missing;
    }
    let check = || -> Result<()> {
        let f = footer_info(target)?;
        ensure!(f.rows > 0, "no rows");
        ensure!(f.names == names, "another column set");
        let part = Part {
            path: String::new(),
            ric: ric.to_string(),
            kind: 0,
            rows: f.rows,
            bytes: 0,
            utc_date: day8.to_string(),
        };
        check_footer(target, &part, true)?;
        Ok(())
    };
    match check() {
        Ok(()) => Existing::Valid,
        Err(e) => Existing::Invalid(format!("{e:#}")),
    }
}

// ------------------------------------------------------------ staged files

#[derive(Clone, Debug)]
pub struct Staged {
    pub path: PathBuf,
    pub part: Part,
}

fn subdirs(p: &Path) -> Result<Vec<PathBuf>> {
    let mut v = Vec::new();
    if p.is_dir() {
        for e in fs::read_dir(p)? {
            let e = e?;
            if e.file_type()?.is_dir() {
                v.push(e.path());
            }
        }
    }
    v.sort();
    Ok(v)
}

/// The finished files of a staging directory laid out as
/// `<exchange>/<year>/<RIC>/<day8>/<file_name>`; `.partial` files are ignored.
pub fn scan_staged(root: &Path, file_name: &str) -> Result<Vec<Staged>> {
    let mut out = Vec::new();
    for ex in subdirs(root)? {
        for year in subdirs(&ex)? {
            for ricdir in subdirs(&year)? {
                for day in subdirs(&ricdir)? {
                    let f = day.join(file_name);
                    if !f.is_file() {
                        continue;
                    }
                    let info = footer_info(&f)?;
                    let ric = info
                        .ric
                        .with_context(|| format!("{f:?}: no RIC statistics"))?;
                    let rel = f.strip_prefix(root)?.to_string_lossy().into_owned();
                    out.push(Staged {
                        part: Part {
                            path: rel,
                            ric,
                            kind: 0,
                            rows: info.rows,
                            bytes: fs::metadata(&f)?.len(),
                            utc_date: day.file_name().unwrap().to_string_lossy().into_owned(),
                        },
                        path: f,
                    });
                }
            }
        }
    }
    Ok(out)
}

// ------------------------------------------------------------ publish pool

#[derive(Default)]
pub struct PublishStats {
    pub published: u64,
    pub already_there: u64,
    pub bytes: u64,
    pub sha_checked: u64,
    pub timing: CopyTiming,
    /// (index in the list, kind, detail)
    pub problems: Vec<(usize, &'static str, String)>,
}

fn is_conflict(e: &str) -> bool {
    e.contains("not replaced")
}

/// Publishes `list` into `target_root` with `threads` threads. `full` holds the
/// indexes that also get a SHA-256 and a read-back from the share.
pub fn publish_list(
    list: &[Staged],
    target_root: &Path,
    threads: usize,
    full: &HashSet<usize>,
    live: Option<&Counters>,
) -> PublishStats {
    let next = AtomicUsize::new(0);
    let acc = Mutex::new(PublishStats::default());
    std::thread::scope(|scope| {
        for _ in 0..threads.max(1) {
            scope.spawn(|| loop {
                let i = next.fetch_add(1, Ordering::Relaxed);
                let Some(st) = list.get(i) else { break };
                let target = target_root.join(&st.part.path);
                let want_full = full.contains(&i);
                let mut result = copy_one_timed(&st.path, &target, &st.part, want_full, true);
                if let Err(e) = &result {
                    if !is_conflict(&format!("{e:#}")) {
                        // one retry of a transient share error
                        std::thread::sleep(Duration::from_secs(2));
                        result = copy_one_timed(&st.path, &target, &st.part, want_full, true);
                    }
                }
                let mut a = acc.lock().expect("publish acc");
                match result {
                    Ok((created, t)) => {
                        a.timing.add(&t);
                        if created {
                            a.published += 1;
                            a.bytes += st.part.bytes;
                            if let Some(c) = live {
                                c.published.fetch_add(1, Ordering::Relaxed);
                                c.bytes.fetch_add(st.part.bytes, Ordering::Relaxed);
                            }
                        } else {
                            a.already_there += 1;
                            if let Some(c) = live {
                                c.skipped.fetch_add(1, Ordering::Relaxed);
                            }
                        }
                        if want_full {
                            // read back from the share
                            match (sha256_file(&target), sha256_file(&st.path)) {
                                (Ok(x), Ok(y)) if x == y => a.sha_checked += 1,
                                (x, y) => a.problems.push((
                                    i,
                                    "readback_differs",
                                    format!("{target:?}: {:?} vs {:?}", x.ok(), y.ok()),
                                )),
                            }
                        }
                    }
                    Err(e) => {
                        let msg = format!("{e:#}");
                        let kind = if is_conflict(&msg) {
                            "existing_file_differs"
                        } else {
                            "publish_failed"
                        };
                        a.problems.push((i, kind, msg));
                    }
                }
            });
        }
    });
    acc.into_inner().expect("publish acc")
}

/// First / middle / last of a list.
pub fn sample_indexes(n: usize) -> HashSet<usize> {
    let mut s = HashSet::new();
    if n > 0 {
        s.extend([0, n / 2, n - 1]);
    }
    s
}

// ------------------------------------------------------------ throughput test

/// Publishes `files` (disjoint subsets for different runs) below
/// `<target_root>/<label>/` and reports the throughput and where the time went.
pub fn bench_publish(files: &[Staged], target_root: &Path, label: &str, threads: usize) -> Value {
    let root = target_root.join(label);
    let started = Instant::now();
    let st = publish_list(files, &root, threads, &sample_indexes(files.len()), None);
    let wall = started.elapsed().as_secs_f64();
    let n = (st.published + st.already_there).max(1) as f64;
    let ms = |d: Duration| (d.as_secs_f64() * 1000.0 / n * 100.0).round() / 100.0;
    let t = &st.timing;
    json!({
        "label": label, "threads": threads, "files": files.len(),
        "published": st.published, "already_there": st.already_there,
        "mb": (st.bytes as f64 / 1e6 * 10.0).round() / 10.0,
        "wall_s": (wall * 100.0).round() / 100.0,
        "files_per_s": (files.len() as f64 / wall * 10.0).round() / 10.0,
        "mb_per_s": (st.bytes as f64 / 1e6 / wall * 100.0).round() / 100.0,
        "problems": st.problems.len(),
        "per_file_ms (thread time, mean)": {
            "source_footer": ms(t.source_footer), "exists_check": ms(t.exists_check),
            "mkdir": ms(t.mkdir), "write": ms(t.write), "fsync": ms(t.fsync),
            "footer_check_on_share": ms(t.footer_check), "hard_link": ms(t.link),
            "cleanup": ms(t.cleanup),
        },
        "sha_readback_ok": st.sha_checked,
    })
}

// ------------------------------------------------------------ problems

/// `meta/problems/l2_deliver/problems.csv` on the share: created on the first
/// problem only (no success receipts).
pub struct Problems {
    path: PathBuf,
    count: AtomicU64,
    file: Mutex<Option<File>>,
    write: bool,
}

impl Problems {
    /// `write_to_share = false` (dry runs): problems are only counted and printed.
    pub fn new(target_root: &Path, write_to_share: bool) -> Self {
        Self {
            path: target_root.join("meta/problems/l2_deliver/problems.csv"),
            count: AtomicU64::new(0),
            file: Mutex::new(None),
            write: write_to_share,
        }
    }
    pub fn count(&self) -> u64 {
        self.count.load(Ordering::Relaxed)
    }
    pub fn record(&self, day: &str, ric: &str, kind: &str, detail: &str) {
        self.count.fetch_add(1, Ordering::Relaxed);
        if !self.write {
            eprintln!("deliver (not written): {day} {ric} {kind} {detail}");
            return;
        }
        let mut g = self.file.lock().expect("problems lock");
        if g.is_none() {
            let new = !self.path.exists();
            if let Some(dir) = self.path.parent() {
                let _ = fs::create_dir_all(dir);
            }
            if let Ok(mut f) = File::options().create(true).append(true).open(&self.path) {
                if new {
                    let _ = writeln!(f, "time_utc,day,ric,kind,detail");
                }
                *g = Some(f);
            }
        }
        let esc = |s: &str| format!("\"{}\"", s.replace('"', "\"\"").replace('\n', " "));
        if let Some(f) = g.as_mut() {
            let _ = writeln!(
                f,
                "{},{},{},{},{}",
                chrono::Utc::now().format("%Y-%m-%dT%H:%M:%SZ"),
                day,
                esc(ric),
                kind,
                esc(detail)
            );
            let _ = f.flush();
        }
    }
}

// ------------------------------------------------------------ enumeration

/// RICs with data in a day database (union of the product markers).
pub fn day_marker_rics(db: &DB) -> Result<Vec<String>> {
    let mut out = BTreeSet::new();
    for item in db.iterator(IteratorMode::From(b"M", rocksdb::Direction::Forward)) {
        let (k, v) = item?;
        if k.first() != Some(&b'M') {
            break;
        }
        let m: Value = serde_json::from_slice(&v).context("marker json")?;
        for r in m["rics"].as_array().context("marker rics")? {
            out.insert(r[0].as_str().context("marker ric")?.to_string());
        }
    }
    Ok(out.into_iter().collect())
}

/// Every RIC of a stream database with the UTC day numbers that hold keys
/// (one seek per RIC-day; keys are `'U' | RIC | 0x00 | UTC second`).
pub fn scan_ric_days(db: &DB) -> Result<BTreeMap<String, Vec<i64>>> {
    let mut ro = ReadOptions::default();
    ro.fill_cache(false);
    let mut it = db.raw_iterator_opt(ro);
    it.seek(b"U");
    let mut out = BTreeMap::new();
    while it.valid() {
        let k = it.key().context("key")?.to_vec();
        if k.first() != Some(&b'U') {
            break;
        }
        let z = k
            .iter()
            .position(|b| *b == 0)
            .context("key without separator")?;
        let ric = String::from_utf8(k[1..z].to_vec())?;
        let prefix = update_prefix(&ric);
        let mut days: Vec<i64> = Vec::new();
        loop {
            let k = it.key().context("key")?;
            if !k.starts_with(&prefix) {
                break;
            }
            ensure!(k.len() == prefix.len() + 4, "bad key length for {ric}");
            let sec = u32::from_be_bytes(k[prefix.len()..].try_into().expect("4"));
            let day = i64::from(sec / 86_400);
            days.push(day);
            let mut next = prefix.clone();
            next.extend_from_slice(&(((day + 1) * 86_400) as u32).to_be_bytes());
            it.seek(&next);
            if !it.valid() {
                break;
            }
        }
        it.status()?;
        out.insert(ric, days);
    }
    Ok(out)
}

// ------------------------------------------------------------ driver

pub enum Source {
    /// One database per UTC day below this root (`YYYYMMDD`).
    DayDbs(PathBuf),
    /// Stream (period package) databases, chronological; all are searched.
    Streams(Vec<PathBuf>),
}

pub struct DeliverConfig {
    pub source: Source,
    /// Inclusive UTC day numbers.
    pub from: i64,
    pub to: i64,
    pub exchange_of: Arc<BTreeMap<String, String>>,
    /// Only these exchange directories (empty = all).
    pub exchanges: BTreeSet<String>,
    /// Only these RICs (empty = all); the others are skipped before anything is counted.
    pub only_rics: BTreeSet<String>,
    /// RICs whose exchange directory is contradictory between two sources:
    /// recorded as a problem and not delivered until resolved (RIC -> detail).
    pub conflicts: BTreeMap<String, String>,
    pub staging: PathBuf,
    pub target_root: PathBuf,
    pub params: Params,
    pub extract_workers: usize,
    pub publish_threads: usize,
    pub batch_files: usize,
    pub cache_mb: usize,
    pub progress: PathBuf,
    /// `host:port` of the server whose `/api/admin/status` gates the run.
    pub status_addr: Option<String>,
    /// Count and list only (no extraction, no publish).
    pub dry_run: bool,
    /// Stop after this many files (tests).
    pub max_files: Option<u64>,
    /// The databases hold Raw LL2 books (`l2_raw`): every delivered RIC-year directory gets the
    /// marker file `l2_source` (text `raw`), which `lseg_catalog l2-sync` turns into
    /// `l2_directories.source = 'raw'`. A RIC-year never mixes sources.
    pub source_raw: bool,
    /// With `source_raw`: a RIC-year that already holds Normalized L2 days may get its missing days from Raw;
    /// the marker file then says `mixed` and lists the Raw days.
    pub allow_mixed: bool,
}

/// Source guard and marker: a RIC-year directory holds L2 of exactly one source. Raw delivery
/// refuses a directory that already has Normalized L2 files and writes the marker; Normalized
/// delivery refuses a directory that carries the Raw marker.
fn guard_and_mark_sources(
    cfg: &DeliverConfig,
    work: &BTreeMap<i64, Vec<String>>,
    file_name: &str,
) -> Result<u64> {
    let mut dirs: BTreeMap<PathBuf, BTreeSet<String>> = BTreeMap::new();
    for (dn, rics) in work {
        let day = day_string(*dn);
        for ric in rics {
            let ex = cfg.exchange_of.get(ric).context("exchange of a work RIC")?;
            dirs.entry(
                cfg.target_root
                    .join(ex)
                    .join(&day[..4])
                    .join(crate::tasdb::component(ric)),
            )
            .or_default()
            .insert(day.replace('-', ""));
        }
    }
    let mut marked = 0;
    for (dir, ric_days) in &dirs {
        let marker = dir.join(crate::catalog::L2_SOURCE_FILE);
        let has_marker = marker.is_file();
        if !cfg.source_raw {
            ensure!(
                !has_marker,
                "{} holds Raw LL2 (marker {}): not delivering Normalized L2 into it",
                dir.display(),
                marker.display()
            );
            continue;
        }
        let old = if has_marker {
            fs::read_to_string(&marker)?
        } else {
            String::new()
        };
        if old.starts_with("raw") {
            continue; // a pure Raw RIC-year: nothing to add
        }
        let mixed_before = old.starts_with("mixed");
        let mut normalized_days = false;
        if !mixed_before && dir.is_dir() {
            for day in subdirs(dir)? {
                if day.join(file_name).is_file() {
                    normalized_days = true;
                    break;
                }
            }
        }
        if normalized_days {
            ensure!(
                cfg.allow_mixed,
                "{} already holds Normalized L2: not mixing sources (--allow-mixed fills its missing days from Raw)",
                dir.display()
            );
        }
        let text = if normalized_days || mixed_before {
            // Raw days = the days of this run that have no L2 file yet (those of earlier runs stay listed)
            let mut days: BTreeSet<String> = old
                .lines()
                .filter_map(|l| l.strip_prefix("raw_day ").map(str::to_string))
                .collect();
            for d in ric_days {
                if !dir.join(d).join(file_name).is_file() {
                    days.insert(d.clone());
                }
            }
            let mut t = String::from("mixed\n");
            for d in days {
                t.push_str(&format!("raw_day {d}\n"));
            }
            t
        } else {
            "raw\n".to_string()
        };
        fs::create_dir_all(dir)?;
        let tmp = dir.join(format!(
            ".{}.{}",
            crate::catalog::L2_SOURCE_FILE,
            std::process::id()
        ));
        fs::write(&tmp, text.as_bytes())?;
        if has_marker {
            fs::rename(&tmp, &marker)?; // only the marker is replaced (mixed lists grow); data files never are
            marked += 1;
            continue;
        }
        let linked = fs::hard_link(&tmp, &marker);
        fs::remove_file(&tmp)?;
        match linked {
            Ok(()) => marked += 1,
            Err(e) if e.kind() == std::io::ErrorKind::AlreadyExists => {}
            Err(e) => return Err(e).with_context(|| format!("write {}", marker.display())),
        }
    }
    Ok(marked)
}

#[derive(Default)]
pub struct Counters {
    pub jobs_total: AtomicU64,
    pub jobs_done: AtomicU64,
    pub published: AtomicU64,
    pub skipped: AtomicU64,
    pub bytes: AtomicU64,
    pub batches_done: AtomicU64,
    pub extract_ms: AtomicU64,
    pub publish_ms: AtomicU64,
}

struct Batch {
    jobs: Vec<(String, String)>,
    /// Number of jobs this batch accounts for (incl. skipped).
    accounted: u64,
    dir: PathBuf,
    staged: Vec<Staged>,
    day: String,
}

pub fn run_deliver(cfg: DeliverConfig) -> Result<Value> {
    ensure!(
        (1..=8).contains(&cfg.extract_workers) && (1..=8).contains(&cfg.publish_threads),
        "worker and thread counts must be 1..=8"
    );
    ensure!(cfg.from <= cfg.to, "empty day range");
    let names = expected_names();
    let file_name = cfg.params.file_name();
    let cache = SharedCache::new(Tuning::new(cfg.cache_mb, 64));
    let problems = Arc::new(Problems::new(&cfg.target_root, !cfg.dry_run));
    let counters = Arc::new(Counters::default());
    let gate = Gate::new();
    let gate_thread = cfg
        .status_addr
        .clone()
        .map(|addr| spawn_gate(gate.clone(), addr));
    let started = Instant::now();

    // ---- enumerate the work: day -> RICs (newest day first)
    let stream_dbs: Vec<Arc<DB>> = match &cfg.source {
        Source::Streams(dirs) => open_dbs(dirs, &cache)?,
        Source::DayDbs(_) => Vec::new(),
    };
    let mut work: BTreeMap<i64, Vec<String>> = BTreeMap::new();
    let mut unmapped: BTreeMap<String, u64> = BTreeMap::new();
    let mut conflicted: BTreeMap<String, u64> = BTreeMap::new();
    let mut per_ric: BTreeMap<String, u64> = BTreeMap::new(); // every RIC-day with data
    let mut overlaps = 0u64;
    let mut with_data: BTreeMap<String, u64> = BTreeMap::new(); // month -> RIC-days (mapped or not)
    let mut note = |dn: i64, ric: &str, work: &mut BTreeMap<i64, Vec<String>>| {
        if !cfg.only_rics.is_empty() && !cfg.only_rics.contains(ric) {
            return;
        }
        *with_data
            .entry(day_string(dn)[..7].to_string())
            .or_default() += 1;
        *per_ric.entry(ric.to_string()).or_default() += 1;
        if cfg.conflicts.contains_key(ric) {
            *conflicted.entry(ric.to_string()).or_default() += 1;
            return;
        }
        match cfg.exchange_of.get(ric) {
            None => *unmapped.entry(ric.to_string()).or_default() += 1,
            Some(ex) if !cfg.exchanges.is_empty() && !cfg.exchanges.contains(ex) => {}
            Some(_) => work.entry(dn).or_default().push(ric.to_string()),
        }
    };
    match &cfg.source {
        Source::DayDbs(root) => {
            for dn in cfg.from..=cfg.to {
                let dir = l2_db::day_dir(root, &day_string(dn));
                if !dir.join("CURRENT").exists() {
                    continue;
                }
                let db = l2_db::open_read(&dir, &cache)?;
                for ric in day_marker_rics(&db)? {
                    note(dn, &ric, &mut work);
                }
            }
        }
        Source::Streams(dirs) => {
            let mut seen: BTreeMap<(String, i64), usize> = BTreeMap::new();
            for (di, db) in stream_dbs.iter().enumerate() {
                eprintln!("deliver: scanning {}", dirs[di].display());
                for (ric, days) in scan_ric_days(db)? {
                    for dn in days {
                        if dn < cfg.from || dn > cfg.to {
                            continue;
                        }
                        *seen.entry((ric.clone(), dn)).or_default() += 1;
                        if seen[&(ric.clone(), dn)] > 1 {
                            overlaps += 1;
                            problems.record(
                                &day_string(dn),
                                &ric,
                                "in_two_databases",
                                "the RIC-day has data in more than one stream database; not delivered",
                            );
                            if let Some(v) = work.get_mut(&dn) {
                                v.retain(|r| r != &ric);
                            }
                            continue;
                        }
                        note(dn, &ric, &mut work);
                    }
                }
            }
        }
    }
    for (ric, n) in &conflicted {
        problems.record(
            "",
            ric,
            "exchange_conflict",
            &format!(
                "{n} RIC-days not delivered: {}",
                cfg.conflicts.get(ric).map_or("", String::as_str)
            ),
        );
    }
    for (ric, n) in &unmapped {
        problems.record(
            "",
            ric,
            "no_exchange_directory",
            &format!("{n} RIC-days not delivered: the RIC is in neither the delivery catalog nor the approved exchange plan"),
        );
    }
    let total: u64 = work.values().map(|v| v.len() as u64).sum();
    counters.jobs_total.store(total, Ordering::Relaxed);
    let sources_marked = if cfg.dry_run {
        0
    } else {
        guard_and_mark_sources(&cfg, &work, &file_name)?
    };
    let enumeration = json!({
        "raw_markers_written": sources_marked,
        "ric_days_with_data_by_month": with_data,
        "ric_days_to_deliver": total,
        "days": work.len(),
        "unmapped_rics": unmapped.len(),
        "unmapped_ric_days": unmapped.values().sum::<u64>(),
        "conflicting_rics": conflicted.len(),
        "conflicting_ric_days": conflicted.values().sum::<u64>(),
        "in_two_databases": overlaps,
    });
    eprintln!("deliver: {enumeration}");
    if cfg.dry_run {
        gate.stop.store(true, Ordering::Relaxed);
        if let Some(t) = gate_thread {
            let _ = t.join();
        }
        return Ok(
            json!({"dry_run": true, "enumeration": enumeration, "ric_days_by_ric": per_ric}),
        );
    }

    // ---- batches, newest day first
    let mut batches: VecDeque<(i64, Vec<String>)> = VecDeque::new();
    for (dn, rics) in work.iter().rev() {
        for chunk in rics.chunks(cfg.batch_files.max(1)) {
            batches.push_back((*dn, chunk.to_vec()));
        }
    }
    fs::create_dir_all(&cfg.staging)?;
    let batches = Mutex::new(batches);
    let stop = Arc::new(AtomicBool::new(false));
    let current = Arc::new(Mutex::new(String::new()));
    let timing = Mutex::new(CopyTiming::default());
    let sha_ok = AtomicU64::new(0);
    let monitor = {
        let (c, p, g, path, cur, stop) = (
            counters.clone(),
            problems.clone(),
            gate.clone(),
            cfg.progress.clone(),
            current.clone(),
            stop.clone(),
        );
        let meta = json!({"from": day_string(cfg.from), "to": day_string(cfg.to),
                          "freq_ms": cfg.params.freq_ns / 1_000_000,
                          "target_root": cfg.target_root, "enumeration": enumeration});
        std::thread::spawn(move || {
            let t0 = Instant::now();
            let mut samples: VecDeque<(f64, u64)> = VecDeque::new();
            loop {
                let t = t0.elapsed().as_secs_f64();
                let done = c.published.load(Ordering::Relaxed) + c.skipped.load(Ordering::Relaxed);
                samples.push_back((t, done));
                while samples.front().is_some_and(|s| t - s.0 > 600.0) {
                    samples.pop_front();
                }
                let rate = match (samples.front(), samples.back()) {
                    (Some(a), Some(b)) if b.0 > a.0 => (b.1 - a.1) as f64 / (b.0 - a.0),
                    _ => 0.0,
                };
                let total = c.jobs_total.load(Ordering::Relaxed);
                let handled = c.jobs_done.load(Ordering::Relaxed);
                let (paused, reason) = g.state();
                let v = json!({
                    "updated_utc": chrono::Utc::now().to_rfc3339(),
                    "state": if stop.load(Ordering::Relaxed) { "finished" } else if paused { "paused" } else { "running" },
                    "pause_reason": reason,
                    "elapsed_s": t as u64,
                    "ric_days_total": total, "ric_days_done": handled,
                    "pct": if total > 0 { (handled as f64 / total as f64 * 1000.0).round() / 10.0 } else { 0.0 },
                    "published_files": c.published.load(Ordering::Relaxed),
                    "skipped_existing": c.skipped.load(Ordering::Relaxed),
                    "published_gb": (c.bytes.load(Ordering::Relaxed) as f64 / 1e9 * 100.0).round() / 100.0,
                    "files_per_s_10min": (rate * 10.0).round() / 10.0,
                    "eta_s": (rate > 0.05).then(|| ((total.saturating_sub(handled)) as f64 / rate) as u64),
                    "problems": p.count(),
                    "batches_done": c.batches_done.load(Ordering::Relaxed),
                    "extract_s": c.extract_ms.load(Ordering::Relaxed) / 1000,
                    "publish_s": c.publish_ms.load(Ordering::Relaxed) / 1000,
                    "current": *cur.lock().expect("current"),
                    "run": meta,
                });
                let _ = write_json_atomic(&path, &v);
                if stop.load(Ordering::Relaxed) {
                    break;
                }
                for _ in 0..10 {
                    if stop.load(Ordering::Relaxed) {
                        break;
                    }
                    std::thread::sleep(Duration::from_secs(1));
                }
            }
        })
    };

    let stage_seq = AtomicU64::new(0);
    let fatal: Mutex<Option<String>> = Mutex::new(None);
    let result = std::thread::scope(|scope| -> Result<()> {
        let (tx, rx) = mpsc::sync_channel::<Batch>(0);
        // producer: existence check + extraction into staging
        scope.spawn(|| {
            let tx = tx;
            loop {
                let Some((dn, rics)) = batches.lock().expect("batches").pop_front() else {
                    break;
                };
                gate.wait();
                if let Some(max) = cfg.max_files {
                    if counters.published.load(Ordering::Relaxed)
                        + counters.skipped.load(Ordering::Relaxed)
                        >= max
                    {
                        break;
                    }
                }
                let day = day_string(dn);
                let day8 = day.replace('-', "");
                *current.lock().expect("current") = format!("{day}: {} RICs", rics.len());
                let layout = Layout {
                    out_root: cfg.target_root.clone(),
                    exchange_of: cfg.exchange_of.clone(),
                    default_exchange: None,
                    day: day.clone(),
                    force: false,
                };
                // 1. what is already on the share (footer checks, 8 threads)
                let flags: Vec<Mutex<Option<Existing>>> =
                    rics.iter().map(|_| Mutex::new(None)).collect();
                let next = AtomicUsize::new(0);
                std::thread::scope(|s2| {
                    for _ in 0..cfg.publish_threads {
                        s2.spawn(|| loop {
                            let i = next.fetch_add(1, Ordering::Relaxed);
                            let Some(ric) = rics.get(i) else { break };
                            let e = match layout.final_path(ric, &cfg.params) {
                                Ok(p) => existing(&p, ric, &day8, &names),
                                Err(e) => Existing::Invalid(format!("{e:#}")),
                            };
                            *flags[i].lock().expect("flag") = Some(e);
                        });
                    }
                });
                let mut jobs = Vec::new();
                let mut skipped = 0u64;
                for (ric, f) in rics.iter().zip(flags) {
                    match f.into_inner().expect("flag").expect("checked") {
                        Existing::Valid => skipped += 1,
                        Existing::Invalid(why) => {
                            problems.record(&day, ric, "existing_file_invalid", &why);
                            skipped += 1;
                        }
                        Existing::Missing => jobs.push((ric.clone(), day.clone())),
                    }
                }
                counters.skipped.fetch_add(skipped, Ordering::Relaxed);
                // 2. extract the rest into staging
                let dir = cfg
                    .staging
                    .join(format!("b{:06}", stage_seq.fetch_add(1, Ordering::Relaxed)));
                let mut staged = Vec::new();
                if !jobs.is_empty() {
                    let t = Instant::now();
                    let ecfg = ExtractConfig {
                        db_root: PathBuf::new(),
                        out_root: dir.clone(),
                        exchange_of: cfg.exchange_of.clone(),
                        default_exchange: None,
                        params: cfg.params,
                        force: false,
                        workers: cfg.extract_workers,
                        cache_mb: cfg.cache_mb,
                    };
                    let dbs: Vec<Arc<DB>> = match &cfg.source {
                        Source::Streams(_) => stream_dbs.clone(),
                        Source::DayDbs(root) => {
                            match open_dbs(&[l2_db::day_dir(root, &day)], &cache) {
                                Ok(d) => d,
                                Err(e) => {
                                    *fatal.lock().expect("fatal") = Some(format!("{e:#}"));
                                    break;
                                }
                            }
                        }
                    };
                    let rep = extract_rics_with(&dbs, &ecfg, &jobs);
                    counters
                        .extract_ms
                        .fetch_add(t.elapsed().as_millis() as u64, Ordering::Relaxed);
                    if let Some(f) = rep.failed {
                        *fatal.lock().expect("fatal") = Some(format!("extract {day}: {f}"));
                        break;
                    }
                    match scan_staged(&dir, &file_name) {
                        Ok(s) => staged = s,
                        Err(e) => {
                            *fatal.lock().expect("fatal") = Some(format!("{e:#}"));
                            break;
                        }
                    }
                }
                let b = Batch {
                    jobs,
                    accounted: skipped,
                    dir,
                    staged,
                    day,
                };
                if tx.send(b).is_err() {
                    break;
                }
            }
        });
        // consumer: publish, read back, delete the staging directory
        for b in rx {
            gate.wait();
            let t = Instant::now();
            let full = sample_indexes(b.staged.len());
            let st = publish_list(
                &b.staged,
                &cfg.target_root,
                cfg.publish_threads,
                &full,
                Some(&counters),
            );
            counters
                .publish_ms
                .fetch_add(t.elapsed().as_millis() as u64, Ordering::Relaxed);
            timing.lock().expect("timing").add(&st.timing);
            sha_ok.fetch_add(st.sha_checked, Ordering::Relaxed);
            for (i, kind, detail) in &st.problems {
                let s = &b.staged[*i];
                problems.record(&b.day, &s.part.ric, kind, detail);
            }
            let staged_n = b.staged.len() as u64;
            let published_n = st.published + st.already_there;
            if published_n + st.problems.len() as u64 != staged_n {
                *fatal.lock().expect("fatal") = Some(format!(
                    "{}: {} staged, {} published, {} problems",
                    b.day,
                    staged_n,
                    published_n,
                    st.problems.len()
                ));
            }
            // RIC-days without data after extraction (no file) still count as handled
            counters
                .jobs_done
                .fetch_add(b.accounted + b.jobs.len() as u64, Ordering::Relaxed);
            let _ = fs::remove_dir_all(&b.dir);
            counters.batches_done.fetch_add(1, Ordering::Relaxed);
            if fatal.lock().expect("fatal").is_some() {
                break;
            }
        }
        Ok(())
    });
    stop.store(true, Ordering::Relaxed);
    gate.stop.store(true, Ordering::Relaxed);
    let _ = monitor.join();
    if let Some(t) = gate_thread {
        let _ = t.join();
    }
    let _ = fs::remove_dir_all(&cfg.staging);
    result?;
    if let Some(f) = fatal.into_inner().expect("fatal") {
        bail!("{f}");
    }
    let t = timing.into_inner().expect("timing");
    let published = counters.published.load(Ordering::Relaxed);
    Ok(json!({
        "enumeration": enumeration,
        "published_files": published,
        "skipped_existing": counters.skipped.load(Ordering::Relaxed),
        "published_gb": counters.bytes.load(Ordering::Relaxed) as f64 / 1e9,
        "problems": problems.count(),
        "batches": counters.batches_done.load(Ordering::Relaxed),
        "readbacks_ok": sha_ok.load(Ordering::Relaxed),
        "wall_s": started.elapsed().as_secs(),
        "publish_thread_seconds": {
            "write": t.write.as_secs(), "fsync": t.fsync.as_secs(),
            "footer_check": t.footer_check.as_secs(), "link": t.link.as_secs(),
        },
    }))
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::l2_slice::{day_number, write_all, Params};

    #[test]
    fn raw_marker_guards_and_marks_ric_year_directories() {
        let t = tempfile::tempdir().unwrap();
        let mk = |raw: bool, allow: bool| DeliverConfig {
            source: Source::Streams(Vec::new()),
            from: 0,
            to: 0,
            exchange_of: Arc::new(BTreeMap::from([
                ("CLF24".to_string(), "nymex".to_string()),
                ("ESH24".to_string(), "cme".to_string()),
            ])),
            exchanges: BTreeSet::new(),
            only_rics: BTreeSet::new(),
            conflicts: BTreeMap::new(),
            staging: t.path().join("stg"),
            target_root: t.path().join("root"),
            params: Params::new(250, 1800).unwrap(),
            extract_workers: 1,
            publish_threads: 1,
            batch_files: 10,
            cache_mb: 8,
            progress: t.path().join("p.json"),
            status_addr: None,
            dry_run: false,
            max_files: None,
            source_raw: raw,
            allow_mixed: allow,
        };
        let dn = day_number("2024-01-04").unwrap();
        let work = BTreeMap::from([(dn, vec!["CLF24".to_string(), "ESH24".to_string()])]);
        // ESH24 already has Normalized L2 files: raw delivery must refuse.
        let day = t.path().join("root/cme/2024/ESH24/20240104");
        fs::create_dir_all(&day).unwrap();
        fs::write(day.join("l2_250ms.parquet"), b"x").unwrap();
        assert!(guard_and_mark_sources(&mk(true, false), &work, "l2_250ms.parquet").is_err());
        // Only CLF24 (TAS files but no L2): marked once, idempotent.
        let only_cl = BTreeMap::from([(dn, vec!["CLF24".to_string()])]);
        fs::create_dir_all(t.path().join("root/nymex/2024/CLF24/20240104")).unwrap();
        fs::write(
            t.path()
                .join("root/nymex/2024/CLF24/20240104/trade.parquet"),
            b"x",
        )
        .unwrap();
        assert_eq!(
            guard_and_mark_sources(&mk(true, false), &only_cl, "l2_250ms.parquet").unwrap(),
            1
        );
        assert_eq!(
            guard_and_mark_sources(&mk(true, false), &only_cl, "l2_250ms.parquet").unwrap(),
            0
        );
        let marker = t
            .path()
            .join("root/nymex/2024/CLF24")
            .join(crate::catalog::L2_SOURCE_FILE);
        assert_eq!(fs::read_to_string(&marker).unwrap(), "raw\n");
        // Normalized delivery into a marked directory is refused, into an unmarked one it is fine.
        assert!(guard_and_mark_sources(&mk(false, false), &only_cl, "l2_250ms.parquet").is_err());
        let only_es = BTreeMap::from([(dn, vec!["ESH24".to_string()])]);
        assert_eq!(
            guard_and_mark_sources(&mk(false, false), &only_es, "l2_250ms.parquet").unwrap(),
            0
        );
        // ESH24 has Normalized days: refused unless --allow-mixed, then a `mixed` marker lists the Raw days
        let dn2 = day_number("2024-01-05").unwrap();
        let two = BTreeMap::from([
            (dn, vec!["ESH24".to_string()]),
            (dn2, vec!["ESH24".to_string()]),
        ]);
        assert!(guard_and_mark_sources(&mk(true, false), &two, "l2_250ms.parquet").is_err());
        assert_eq!(
            guard_and_mark_sources(&mk(true, true), &two, "l2_250ms.parquet").unwrap(),
            1
        );
        let es_marker = t
            .path()
            .join("root/cme/2024/ESH24")
            .join(crate::catalog::L2_SOURCE_FILE);
        // 20240104 has a Normalized file, only 20240105 comes from Raw
        assert_eq!(
            fs::read_to_string(&es_marker).unwrap(),
            "mixed\nraw_day 20240105\n"
        );
        // a pure Raw directory is left alone by a later run
        assert_eq!(
            guard_and_mark_sources(&mk(true, true), &only_cl, "l2_250ms.parquet").unwrap(),
            0
        );
        assert_eq!(fs::read_to_string(&marker).unwrap(), "raw\n");
    }

    #[test]
    fn day_strings_round_trip() {
        let dn = day_number("2026-11-10").unwrap();
        assert_eq!(day_string(dn), "2026-11-10");
        assert_eq!(day_string(dn + 22), "2026-12-02");
    }

    #[test]
    fn samples_are_first_middle_last() {
        assert_eq!(sample_indexes(0).len(), 0);
        assert_eq!(sample_indexes(1).len(), 1);
        assert_eq!(sample_indexes(10), HashSet::from([0, 5, 9]));
    }

    #[test]
    fn existing_is_missing_valid_or_invalid() {
        let t = tempfile::tempdir().unwrap();
        let names = expected_names();
        let f = t.path().join("x.parquet");
        assert_eq!(existing(&f, "ESZ26", "20261110", &names), Existing::Missing);
        write_all(&f, b"not parquet").unwrap();
        assert!(matches!(
            existing(&f, "ESZ26", "20261110", &names),
            Existing::Invalid(_)
        ));
        let _ = Params::new(250, 1800).unwrap();
    }

    #[test]
    fn problems_file_appears_only_with_the_first_problem() {
        let t = tempfile::tempdir().unwrap();
        let p = Problems::new(t.path(), true);
        assert!(!t
            .path()
            .join("meta/problems/l2_deliver/problems.csv")
            .exists());
        p.record("2026-11-10", "ES\"Z26", "publish_failed", "a\nb");
        let text =
            fs::read_to_string(t.path().join("meta/problems/l2_deliver/problems.csv")).unwrap();
        let lines: Vec<&str> = text.lines().collect();
        assert_eq!(lines[0], "time_utc,day,ric,kind,detail");
        assert!(lines[1].contains("\"ES\"\"Z26\"") && lines[1].contains("\"a b\""));
        assert_eq!(p.count(), 1);
    }
}
