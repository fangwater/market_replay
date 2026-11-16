//! Day-file publication for `<root>/<exchange>/<UTC year>/<RIC>/<YYYYMMDD>/kline_ib.parquet`.
//!
//! * new file: temp file in the target directory, fsync, footer check, then a
//!   hard link that never overwrites (an existing target is an error);
//! * existing file with identical content (schema incl. metadata + all values): skipped;
//! * existing file that differs: the new temp file is written and checked first,
//!   the old file is copied to a local backup (fsync, byte-compared), and only then
//!   renamed over. Nothing is ever deleted without such a backup;
//! * every published/kept file gets a footer check (schema, row count, per row
//!   group RIC and UTC-day min/max); full value readback only for sampled files.
use anyhow::{anyhow, bail, ensure, Context, Result};
use arrow_array::{Array, Int64Array, RecordBatch};
use arrow_schema::Schema;
use crossbeam_channel::{bounded, Receiver, Sender};
use parquet::{
    arrow::{arrow_reader::ParquetRecordBatchReaderBuilder, ArrowWriter},
    basic::{Compression, ZstdLevel},
    file::{properties::WriterProperties, statistics::Statistics},
};
use std::{
    collections::VecDeque,
    fs::{self, File},
    io::Read,
    path::{Path, PathBuf},
    sync::{
        atomic::{AtomicU64, Ordering},
        Arc,
    },
    thread::JoinHandle,
};

pub const DAY_NS: i64 = 86_400_000_000_000;
pub const FILE_NAME: &str = "kline_ib.parquet";
/// Publication queue depth (global) and per-RIC outstanding acknowledgments.
pub const QUEUE: usize = 16;
pub const MAX_PUBLISH_THREADS: usize = 8;

/// RIC directory component; same escaping as the LSEG writers
/// (`urllib.parse.quote(ric, safe='_-.^')` / `native_kline::component`).
pub fn component(value: &str) -> String {
    if value == "." || value == ".." {
        return value.bytes().map(|_| "%2E").collect();
    }
    value
        .bytes()
        .map(|b| {
            if b.is_ascii_alphanumeric() || b"_-.^".contains(&b) {
                (b as char).to_string()
            } else {
                format!("%{b:02X}")
            }
        })
        .collect()
}

pub fn day_label(day_start: i64) -> Result<String> {
    let d = chrono::DateTime::from_timestamp_nanos(day_start);
    ensure!(
        day_start.rem_euclid(DAY_NS) == 0,
        "not a UTC midnight: {day_start}"
    );
    Ok(d.format("%Y%m%d").to_string())
}

pub fn day_path(root: &Path, exchange: &str, ric: &str, day_start: i64) -> Result<PathBuf> {
    ensure!(
        !exchange.is_empty()
            && exchange
                .bytes()
                .all(|b| b.is_ascii_lowercase() || b.is_ascii_digit() || b == b'_'),
        "unsafe exchange directory {exchange:?}"
    );
    let day = day_label(day_start)?;
    Ok(root
        .join(exchange)
        .join(&day[..4])
        .join(component(ric))
        .join(day)
        .join(FILE_NAME))
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum Outcome {
    Created,
    Identical,
    Replaced,
    /// Existing file holds rows newer than this source; left untouched (problem).
    KeptNewer,
}

#[derive(Clone, Debug, Default, serde::Serialize, serde::Deserialize, PartialEq, Eq)]
pub struct Tally {
    pub created: u64,
    pub identical: u64,
    pub replaced: u64,
    pub kept_newer: u64,
    pub rows: u64,
    pub sampled_readbacks: u64,
}
impl Tally {
    pub fn add(&mut self, o: &Tally) {
        self.created += o.created;
        self.identical += o.identical;
        self.replaced += o.replaced;
        self.kept_newer += o.kept_newer;
        self.rows += o.rows;
        self.sampled_readbacks += o.sampled_readbacks;
    }
}

pub struct DayFile {
    pub path: PathBuf,
    pub ric: String,
    pub day_start: i64,
    pub batch: RecordBatch,
    /// Highest source id the new content may contain (snapshot / run cutoff).
    pub max_source_id: i64,
    pub sampled: bool,
}

pub struct Publisher {
    pub delivery_root: PathBuf,
    /// Local (NVMe) backup directory of replaced files for this run.
    pub backup_root: PathBuf,
}

static SERIAL: AtomicU64 = AtomicU64::new(0);

fn check_identity_path(path: &Path, ric: &str, day_start: i64) -> Result<()> {
    let day_dir = path.parent().context("day directory")?;
    let ric_dir = day_dir.parent().context("RIC directory")?;
    let year_dir = ric_dir.parent().context("year directory")?;
    let day = day_label(day_start)?;
    ensure!(
        day_dir.file_name().and_then(|v| v.to_str()) == Some(day.as_str())
            && year_dir.file_name().and_then(|v| v.to_str()) == Some(&day[..4])
            && ric_dir.file_name().and_then(|v| v.to_str()) == Some(component(ric).as_str()),
        "directory identity differs from RIC {ric} / UTC day {day}: {}",
        path.display()
    );
    Ok(())
}

/// Cheap complete check from the already-loaded footer only (no data pages).
pub fn footer_check(
    file: File,
    path: &Path,
    expected: &Schema,
    rows: u64,
    ric: &str,
    day_start: i64,
) -> Result<()> {
    check_identity_path(path, ric, day_start)?;
    let b = ParquetRecordBatchReaderBuilder::try_new(file)
        .with_context(|| format!("footer {}", path.display()))?;
    ensure!(
        b.metadata().file_metadata().num_rows() as u64 == rows,
        "row count {} != {rows}: {}",
        b.metadata().file_metadata().num_rows(),
        path.display()
    );
    ensure!(
        b.schema().as_ref() == expected,
        "schema/metadata differs: {}",
        path.display()
    );
    let ric_column = expected.index_of("ric")?;
    let time_column = expected.index_of("ts_utc_ns")?;
    ensure!(
        b.metadata().num_row_groups() > 0 || rows == 0,
        "no row groups: {}",
        path.display()
    );
    for group in b.metadata().row_groups() {
        match group.column(ric_column).statistics() {
            Some(Statistics::ByteArray(s)) => {
                let lo = s.min_opt().context("missing RIC min statistic")?.data();
                let hi = s.max_opt().context("missing RIC max statistic")?.data();
                ensure!(
                    s.null_count_opt() == Some(0) && lo == hi && lo == ric.as_bytes(),
                    "footer RIC differs from {ric}: {}",
                    path.display()
                );
            }
            _ => bail!("missing RIC footer statistics: {}", path.display()),
        }
        match group.column(time_column).statistics() {
            Some(Statistics::Int64(s)) => {
                let lo = *s.min_opt().context("missing time min statistic")?;
                let hi = *s.max_opt().context("missing time max statistic")?;
                ensure!(
                    s.null_count_opt() == Some(0)
                        && day_start <= lo
                        && lo <= hi
                        && hi < day_start + DAY_NS,
                    "footer time outside UTC day: {}",
                    path.display()
                );
            }
            _ => bail!("missing time footer statistics: {}", path.display()),
        }
    }
    Ok(())
}

pub fn read_all(path: &Path) -> Result<RecordBatch> {
    let b = ParquetRecordBatchReaderBuilder::try_new(File::open(path)?)?;
    let schema = b.schema().clone();
    let batches = b
        .with_batch_size(8192)
        .build()?
        .collect::<Result<Vec<_>, _>>()?;
    Ok(arrow_select::concat::concat_batches(&schema, &batches)?)
}

fn max_id(batch: &RecordBatch) -> Option<i64> {
    let ids = batch
        .column_by_name("id")?
        .as_any()
        .downcast_ref::<Int64Array>()?;
    (0..ids.len())
        .filter(|i| ids.is_valid(*i))
        .map(|i| ids.value(i))
        .max()
}

fn properties() -> Result<WriterProperties> {
    Ok(WriterProperties::builder()
        .set_compression(Compression::ZSTD(ZstdLevel::try_new(3)?))
        .set_max_row_group_size(65536)
        .build())
}

/// Removes an unpublished temp file on every exit path.
struct Temp(Option<PathBuf>);
impl Drop for Temp {
    fn drop(&mut self) {
        if let Some(p) = self.0.take() {
            let _ = fs::remove_file(p);
        }
    }
}

fn same_bytes(a: &Path, b: &Path) -> Result<bool> {
    let (mut x, mut y) = (Vec::new(), Vec::new());
    File::open(a)?.read_to_end(&mut x)?;
    File::open(b)?.read_to_end(&mut y)?;
    Ok(x == y)
}

fn sync_dir(path: &Path) {
    // Directory fsync is best effort on NFS.
    if let Ok(d) = File::open(path) {
        let _ = d.sync_all();
    }
}

impl Publisher {
    pub fn publish(&self, f: &DayFile) -> Result<Outcome> {
        let rows = f.batch.num_rows() as u64;
        ensure!(rows > 0, "empty day file {}", f.path.display());
        ensure!(
            max_id(&f.batch).is_some_and(|m| m <= f.max_source_id),
            "new content exceeds source cutoff: {}",
            f.path.display()
        );
        check_identity_path(&f.path, &f.ric, f.day_start)?;
        let schema = f.batch.schema();
        let existing = f.path.exists();
        if existing {
            let old = read_all(&f.path)
                .with_context(|| format!("reading existing {}", f.path.display()))?;
            if old == f.batch {
                footer_check(
                    File::open(&f.path)?,
                    &f.path,
                    &schema,
                    rows,
                    &f.ric,
                    f.day_start,
                )?;
                return Ok(Outcome::Identical);
            }
            if max_id(&old).is_some_and(|m| m > f.max_source_id) {
                return Ok(Outcome::KeptNewer);
            }
        }
        let dir = f.path.parent().context("day directory")?;
        fs::create_dir_all(dir)?;
        let temp_path = dir.join(format!(
            ".{FILE_NAME}.{}.{}.partial",
            std::process::id(),
            SERIAL.fetch_add(1, Ordering::Relaxed)
        ));
        let file = File::options()
            .read(true)
            .write(true)
            .create_new(true)
            .open(&temp_path)?;
        let mut temp = Temp(Some(temp_path.clone()));
        let mut writer = ArrowWriter::try_new(file, schema.clone(), Some(properties()?))?;
        writer.write(&f.batch)?;
        writer.finish()?;
        writer.inner().sync_all()?;
        // Footer of the very inode about to be published (dup'd descriptor).
        footer_check(
            writer.inner().try_clone()?,
            &f.path,
            &schema,
            rows,
            &f.ric,
            f.day_start,
        )
        .context("temp file footer")?;
        drop(writer);
        let outcome = if !existing {
            match fs::hard_link(&temp_path, &f.path) {
                Ok(()) => {}
                Err(e) if e.kind() == std::io::ErrorKind::AlreadyExists => {
                    bail!(
                        "target appeared concurrently, not overwritten: {}",
                        f.path.display()
                    )
                }
                Err(e) => return Err(e.into()),
            }
            Outcome::Created
        } else {
            let relative = f.path.strip_prefix(&self.delivery_root).with_context(|| {
                format!(
                    "{} is outside delivery root {}",
                    f.path.display(),
                    self.delivery_root.display()
                )
            })?;
            let backup = self.backup_root.join(relative);
            fs::create_dir_all(backup.parent().unwrap())?;
            {
                let mut src = File::open(&f.path)?;
                let mut dst = File::options()
                    .write(true)
                    .create_new(true)
                    .open(&backup)
                    .with_context(|| {
                        format!(
                            "backup already exists (one replacement per file per run): {}",
                            backup.display()
                        )
                    })?;
                std::io::copy(&mut src, &mut dst)?;
                dst.sync_all()?;
            }
            sync_dir(backup.parent().unwrap());
            ensure!(
                same_bytes(&f.path, &backup)?,
                "backup differs from original: {}",
                backup.display()
            );
            fs::rename(&temp_path, &f.path)?;
            temp.0 = None;
            Outcome::Replaced
        };
        sync_dir(dir);
        drop(temp);
        footer_check(
            File::open(&f.path)?,
            &f.path,
            &schema,
            rows,
            &f.ric,
            f.day_start,
        )?;
        if f.sampled {
            ensure!(
                read_all(&f.path)? == f.batch,
                "sampled readback differs: {}",
                f.path.display()
            );
        }
        Ok(outcome)
    }
}

type Task = Box<dyn FnOnce() + Send>;

/// Fixed-size publication threads with a bounded global queue.
pub struct Pool {
    sender: Option<Sender<Task>>,
    workers: Vec<JoinHandle<()>>,
}
impl Pool {
    pub fn new(threads: usize) -> Result<Arc<Self>> {
        ensure!(
            (1..=MAX_PUBLISH_THREADS).contains(&threads),
            "publish threads must be 1..={MAX_PUBLISH_THREADS}"
        );
        let (sender, receiver) = bounded::<Task>(QUEUE);
        let mut workers = Vec::new();
        for i in 0..threads {
            let r: Receiver<Task> = receiver.clone();
            workers.push(
                std::thread::Builder::new()
                    .name(format!("ib-publish-{i}"))
                    .spawn(move || {
                        while let Ok(task) = r.recv() {
                            task();
                        }
                    })?,
            );
        }
        Ok(Arc::new(Self {
            sender: Some(sender),
            workers,
        }))
    }
}
impl Drop for Pool {
    fn drop(&mut self) {
        drop(self.sender.take());
        for w in self.workers.drain(..) {
            let _ = w.join();
        }
    }
}

pub struct Done {
    pub path: PathBuf,
    pub day_start: i64,
    pub rows: u64,
    pub sampled: bool,
    pub result: Result<Outcome>,
}

/// Per-RIC outstanding publications; at most QUEUE in flight, all drained
/// before the RIC (or run) is considered finished, also on early errors.
pub struct Pending {
    pool: Arc<Pool>,
    publisher: Arc<Publisher>,
    waiting: VecDeque<Receiver<Done>>,
    pub done: Vec<Done>,
}
impl Pending {
    pub fn new(pool: Arc<Pool>, publisher: Arc<Publisher>) -> Self {
        Self {
            pool,
            publisher,
            waiting: VecDeque::new(),
            done: Vec::new(),
        }
    }
    pub fn submit(&mut self, f: DayFile) -> Result<()> {
        if self.waiting.len() >= QUEUE {
            self.wait_one()?;
        }
        let (tx, rx) = bounded(1);
        let publisher = self.publisher.clone();
        let task: Task = Box::new(move || {
            let result =
                std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| publisher.publish(&f)))
                    .unwrap_or_else(|_| Err(anyhow!("publication panicked")));
            let _ = tx.send(Done {
                path: f.path.clone(),
                day_start: f.day_start,
                rows: f.batch.num_rows() as u64,
                sampled: f.sampled,
                result,
            });
        });
        self.pool
            .sender
            .as_ref()
            .context("pool closed")?
            .send(task)
            .map_err(|_| anyhow!("publication threads stopped"))?;
        self.waiting.push_back(rx);
        Ok(())
    }
    fn wait_one(&mut self) -> Result<()> {
        if let Some(rx) = self.waiting.pop_front() {
            self.done
                .push(rx.recv().context("publication acknowledgment lost")?);
        }
        Ok(())
    }
    pub fn finish(&mut self) -> Result<Vec<Done>> {
        while !self.waiting.is_empty() {
            self.wait_one()?;
        }
        Ok(std::mem::take(&mut self.done))
    }
}
impl Drop for Pending {
    fn drop(&mut self) {
        while let Some(rx) = self.waiting.pop_front() {
            let _ = rx.recv();
        }
    }
}

/// First / middle / last indices of `n` day files (sampled full readback).
pub fn sampled(n: usize, i: usize) -> bool {
    n > 0 && (i == 0 || i == n / 2 || i == n - 1)
}

#[cfg(test)]
pub mod tests {
    use super::*;
    use crate::ib_minute::{
        batch, schema,
        testutil::{job, midnight, row},
    };

    pub fn temp_root(name: &str) -> PathBuf {
        let p = std::env::temp_dir().join(format!("ib_kline_{name}_{}", std::process::id()));
        let _ = fs::remove_dir_all(&p);
        fs::create_dir_all(&p).unwrap();
        p
    }
    fn file(root: &Path, ids: &[(i64, &str)], max: i64) -> DayFile {
        let j = job("ESZ26");
        let rows: Vec<_> = ids.iter().map(|(i, t)| row(*i, t)).collect();
        let d = midnight(ids[0].1);
        DayFile {
            path: day_path(root, "cme", "ESZ26", d).unwrap(),
            ric: "ESZ26".into(),
            day_start: d,
            batch: batch(&rows, &j, schema(&j)).unwrap(),
            max_source_id: max,
            sampled: true,
        }
    }

    #[test]
    fn create_identical_replace_with_backup_and_keep_newer() {
        let root = temp_root("publish");
        let backup = temp_root("publish_backup");
        let p = Publisher {
            delivery_root: root.clone(),
            backup_root: backup.clone(),
        };
        let partial = file(
            &root,
            &[(1, "2026-11-03 00:00:00"), (2, "2026-11-03 05:13:00")],
            10,
        );
        assert!(partial
            .path
            .ends_with("cme/2026/ESZ26/20261103/kline_ib.parquet"));
        assert_eq!(p.publish(&partial).unwrap(), Outcome::Created);
        let old_bytes = fs::read(&partial.path).unwrap();
        assert_eq!(p.publish(&partial).unwrap(), Outcome::Identical);
        // A later complete day replaces the partial one; old bytes kept in the backup.
        let full = file(
            &root,
            &[
                (1, "2026-11-03 00:00:00"),
                (2, "2026-11-03 05:13:00"),
                (7, "2026-11-03 21:59:00"),
            ],
            10,
        );
        assert_eq!(p.publish(&full).unwrap(), Outcome::Replaced);
        let saved = backup.join("cme/2026/ESZ26/20261103/kline_ib.parquet");
        assert_eq!(fs::read(&saved).unwrap(), old_bytes);
        assert_eq!(read_all(&full.path).unwrap(), full.batch);
        // An older source (cutoff below ids already published) never downgrades it.
        let older = file(&root, &[(1, "2026-11-03 00:00:00")], 5);
        assert_eq!(p.publish(&older).unwrap(), Outcome::KeptNewer);
        assert_eq!(read_all(&full.path).unwrap(), full.batch);
        // Content beyond the declared cutoff is refused.
        assert!(p
            .publish(&file(&root, &[(99, "2026-11-04 00:00:00")], 10))
            .is_err());
        // A second replacement of the same file in one run would overwrite its backup: refused.
        let again = file(&root, &[(1, "2026-11-03 00:00:00")], 10);
        assert!(p.publish(&again).is_err());
        assert_eq!(read_all(&full.path).unwrap(), full.batch);
        // No temp files left behind.
        let left: Vec<_> = fs::read_dir(full.path.parent().unwrap())
            .unwrap()
            .map(|e| e.unwrap().file_name())
            .collect();
        assert_eq!(left, vec![std::ffi::OsString::from(FILE_NAME)]);
        fs::remove_dir_all(root).unwrap();
        fs::remove_dir_all(backup).unwrap();
    }

    #[test]
    fn footer_rejects_wrong_ric_day_rows_or_metadata() {
        let root = temp_root("footer");
        let p = Publisher {
            delivery_root: root.clone(),
            backup_root: root.join("_b"),
        };
        let f = file(
            &root,
            &[(1, "2026-11-03 00:00:00"), (2, "2026-11-03 23:59:00")],
            10,
        );
        p.publish(&f).unwrap();
        let s = f.batch.schema();
        let open = || File::open(&f.path).unwrap();
        footer_check(open(), &f.path, &s, 2, "ESZ26", f.day_start).unwrap();
        assert!(footer_check(open(), &f.path, &s, 3, "ESZ26", f.day_start).is_err());
        // Same schema and row count, but filed under another RIC / UTC day.
        let other_ric = root.join("cme/2026/ESH27/20261103/kline_ib.parquet");
        fs::create_dir_all(other_ric.parent().unwrap()).unwrap();
        fs::copy(&f.path, &other_ric).unwrap();
        assert!(footer_check(
            File::open(&other_ric).unwrap(),
            &other_ric,
            &s,
            2,
            "ESH27",
            f.day_start
        )
        .is_err());
        let next = midnight("2026-11-04");
        let other_day = day_path(&root, "cme", "ESZ26", next).unwrap();
        fs::create_dir_all(other_day.parent().unwrap()).unwrap();
        fs::copy(&f.path, &other_day).unwrap();
        assert!(footer_check(
            File::open(&other_day).unwrap(),
            &other_day,
            &s,
            2,
            "ESZ26",
            next
        )
        .is_err());
        // Schema metadata is part of the identity.
        let mut j = job("ESZ26");
        j.cutoff_id = 1;
        assert!(footer_check(open(), &f.path, &schema(&j), 2, "ESZ26", f.day_start).is_err());
        fs::remove_dir_all(root).unwrap();
    }

    #[test]
    fn paths_and_sampling() {
        let d = midnight("2024-02-29");
        assert_eq!(
            day_path(Path::new("/r"), "cfe", "VX48X6", d).unwrap(),
            PathBuf::from("/r/cfe/2024/VX48X6/20240229/kline_ib.parquet")
        );
        assert_eq!(component("A/B c^1"), "A%2FB%20c^1");
        assert!(day_path(Path::new("/r"), "../x", "A", d).is_err());
        assert!(day_label(d + 1).is_err());
        let picks: Vec<_> = (0..7).filter(|i| sampled(7, *i)).collect();
        assert_eq!(picks, vec![0, 3, 6]);
        assert_eq!((0..1).filter(|i| sampled(1, *i)).count(), 1);
    }
}
