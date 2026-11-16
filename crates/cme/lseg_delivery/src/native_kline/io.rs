pub use super::publication::{PendingFiles, PublicationPool};
use super::{
    session::{Decoded, State},
    BATCH, DAY,
};
use anyhow::{ensure, Context, Result};
use arrow_array::{
    Array, ArrayRef, BooleanArray, Float64Array, Int64Array, LargeStringArray, RecordBatch,
    StringArray, UInt64Array,
};
use arrow_schema::{DataType, Field, Schema};
use parquet::{
    arrow::{
        arrow_reader::{ParquetRecordBatchReader, ParquetRecordBatchReaderBuilder},
        ArrowWriter,
    },
    basic::{Compression, ZstdLevel},
    file::properties::WriterProperties,
};
use serde::{Deserialize, Serialize};
use std::{
    collections::{BTreeMap, HashMap, HashSet},
    ffi::OsString,
    fs::{self, File},
    io::BufReader,
    path::{Path, PathBuf},
    sync::{
        atomic::{AtomicU64, Ordering},
        Arc,
    },
};

pub type Meta = HashMap<String, String>;
pub fn read_json(path: &Path) -> Result<serde_json::Value> {
    Ok(serde_json::from_reader(BufReader::with_capacity(
        64 * 1024,
        File::open(path)?,
    ))?)
}
pub use crate::tasdb::save_json as save;
pub fn string<'a>(b: &'a RecordBatch, row: usize, name: &str) -> Result<&'a str> {
    let a = b
        .column_by_name(name)
        .with_context(|| format!("missing {name}"))?;
    ensure!(!a.is_null(row), "null {name}");
    if let Some(a) = a.as_any().downcast_ref::<StringArray>() {
        return Ok(a.value(row));
    }
    if let Some(a) = a.as_any().downcast_ref::<LargeStringArray>() {
        return Ok(a.value(row));
    }
    anyhow::bail!("{name}: expected UTF8")
}
pub fn uint(b: &RecordBatch, row: usize, name: &str) -> Result<u64> {
    let a = b
        .column_by_name(name)
        .with_context(|| format!("missing {name}"))?;
    ensure!(!a.is_null(row), "null {name}");
    if let Some(a) = a.as_any().downcast_ref::<UInt64Array>() {
        return Ok(a.value(row));
    }
    if let Some(a) = a.as_any().downcast_ref::<Int64Array>() {
        return Ok(u64::try_from(a.value(row))?);
    }
    anyhow::bail!("{name}: expected uint64")
}
pub fn integer(b: &RecordBatch, row: usize, name: &str) -> Result<i64> {
    ensure!(
        !b.column_by_name(name)
            .context("integer column")?
            .is_null(row),
        "null {name}"
    );
    Ok(b.column_by_name(name)
        .context("integer column")?
        .as_any()
        .downcast_ref::<Int64Array>()
        .context("expected int64")?
        .value(row))
}
pub fn batches(path: &Path) -> Result<ParquetRecordBatchReader> {
    Ok(ParquetRecordBatchReaderBuilder::try_new(File::open(path)?)?
        .with_batch_size(BATCH)
        .build()?)
}
pub struct Rows {
    reader: ParquetRecordBatchReader,
    batch: Option<RecordBatch>,
    at: usize,
}
impl Rows {
    pub fn new(path: &Path) -> Result<Self> {
        Ok(Self {
            reader: batches(path)?,
            batch: None,
            at: 0,
        })
    }
    pub fn next_row(&mut self) -> Result<Option<(&RecordBatch, usize)>> {
        while self.batch.as_ref().is_none_or(|b| self.at >= b.num_rows()) {
            self.batch = self.reader.next().transpose()?;
            self.at = 0;
            if self.batch.is_none() {
                return Ok(None);
            }
        }
        let at = self.at;
        self.at += 1;
        Ok(Some((self.batch.as_ref().unwrap(), at)))
    }
}
pub fn date(at: u64) -> Result<String> {
    Ok(
        chrono::DateTime::from_timestamp(i64::try_from(at / 1_000_000_000)?, 0)
            .context("invalid date")?
            .format("%Y%m%d")
            .to_string(),
    )
}
pub fn year_start(year: i32) -> Result<u64> {
    use chrono::TimeZone;
    Ok(u64::try_from(
        chrono::Utc
            .with_ymd_and_hms(year, 1, 1, 0, 0, 0)
            .single()
            .context("invalid year")?
            .timestamp_nanos_opt()
            .context("year out of ns range")?,
    )?)
}
pub fn parse_ns(text: &str) -> Option<u64> {
    if let Ok(d) = chrono::DateTime::parse_from_rfc3339(text) {
        return d.timestamp_nanos_opt().and_then(|v| v.try_into().ok());
    }
    chrono::NaiveDate::parse_from_str(text, "%Y-%m-%d")
        .ok()?
        .and_hms_opt(0, 0, 0)?
        .and_utc()
        .timestamp_nanos_opt()?
        .try_into()
        .ok()
}
pub fn status_schema() -> Arc<Schema> {
    let fields = vec![
        ("product", DataType::Utf8),
        ("ric", DataType::Utf8),
        ("ts_utc_ns", DataType::UInt64),
        ("published_ns", DataType::UInt64),
        ("effective_ns", DataType::UInt64),
        ("action", DataType::Utf8),
        ("session_state", DataType::Utf8),
        ("is_trading", DataType::Boolean),
        ("reason", DataType::Utf8),
        ("is_transition", DataType::Boolean),
        ("source", DataType::Utf8),
        ("source_url", DataType::Utf8),
        ("qualifiers", DataType::Utf8),
    ];
    Arc::new(Schema::new_with_metadata(
        fields
            .into_iter()
            .map(|(n, t)| Field::new(n, t, true))
            .collect::<Vec<_>>(),
        Meta::from([
            ("source".into(), "LSEG TAS Mkt. Condition qualifiers".into()),
            ("session_status_schema_version".into(), "1".into()),
            ("decoder_policy_version".into(), "full1".into()),
            (
                "time_basis".into(),
                "exact LSEG publication nanoseconds; source patch order; duplicates retained"
                    .into(),
            ),
        ]),
    ))
}
pub fn kline_schema() -> Arc<Schema> {
    let mut fields = vec![
        Field::new("ric", DataType::Utf8, true),
        Field::new("ts_utc_ns", DataType::UInt64, true),
    ];
    for n in ["open", "high", "low", "close"] {
        fields.push(Field::new(n, DataType::Float64, true))
    }
    for n in ["volume", "trade_count"] {
        fields.push(Field::new(n, DataType::UInt64, true))
    }
    fields.push(Field::new("is_ffill", DataType::Boolean, true));
    fields.push(Field::new("status", DataType::Utf8, true));
    Arc::new(Schema::new_with_metadata(fields,Meta::from([("source".into(),"LSEG TAS; native direct RocksDB streaming".into()),("kline_status_policy".into(),"lseg_only_full_v1".into()),("kline_schema_version".into(),"5".into()),("price_scale".into(),"1; nullable Float64 actual prices".into()),("trade_time_basis".into(),"exchange clock nearest UTC date within 12h; publication fallback when missing".into()),("empty_minutes".into(),"continuous UTC minutes within LSEG lifetime; own previous close across all breaks".into()),("correction_scope".into(),"unlinked kind=12 recorded as source_witness_pending; original 237 revised RICs reused unchanged".into())])))
}
#[derive(Clone, Debug)]
pub struct StatusRow {
    pub at: u64,
    pub before: State,
    pub decoded: Decoded,
    pub qualifiers: String,
}
pub fn status_batch(product: &str, ric: &str, rows: &[StatusRow]) -> Result<RecordBatch> {
    let n = rows.len();
    let s = |f: fn(&StatusRow) -> &str| -> ArrayRef {
        Arc::new(StringArray::from_iter_values(rows.iter().map(f)))
    };
    let a = vec![
        Arc::new(StringArray::from_iter_values(std::iter::repeat_n(
            product, n,
        ))) as ArrayRef,
        Arc::new(StringArray::from_iter_values(std::iter::repeat_n(ric, n))),
        Arc::new(UInt64Array::from_iter_values(rows.iter().map(|r| r.at))),
        Arc::new(UInt64Array::from_iter_values(rows.iter().map(|r| r.at))),
        Arc::new(UInt64Array::from_iter_values(rows.iter().map(|r| r.at))),
        s(|r| r.decoded.action),
        s(|r| r.decoded.state.name()),
        Arc::new(BooleanArray::from(
            rows.iter().map(|r| r.decoded.trading).collect::<Vec<_>>(),
        )),
        s(|r| r.decoded.reason),
        Arc::new(BooleanArray::from(
            rows.iter()
                .map(|r| r.before != r.decoded.state)
                .collect::<Vec<_>>(),
        )),
        Arc::new(StringArray::from_iter_values(std::iter::repeat_n(
            "LSEG_TAS", n,
        ))),
        Arc::new(StringArray::from(vec![None::<&str>; n])),
        s(|r| r.qualifiers.as_str()),
    ];
    Ok(RecordBatch::try_new(status_schema(), a)?)
}
#[derive(Clone, Debug)]
pub struct Kline {
    pub at: u64,
    pub prices: [Option<i64>; 4],
    pub volume: u64,
    pub count: u64,
    pub ffill: bool,
    pub status: &'static str,
}
pub fn kline_batch(ric: &str, rows: &[Kline]) -> Result<RecordBatch> {
    let mut a: Vec<ArrayRef> = vec![
        Arc::new(StringArray::from_iter_values(std::iter::repeat_n(
            ric,
            rows.len(),
        ))),
        Arc::new(UInt64Array::from_iter_values(rows.iter().map(|r| r.at))),
    ];
    for i in 0..4 {
        a.push(Arc::new(Float64Array::from(
            rows.iter()
                .map(|r| r.prices[i].map(|p| p as f64 / 1e9))
                .collect::<Vec<_>>(),
        )))
    }
    a.push(Arc::new(UInt64Array::from_iter_values(
        rows.iter().map(|r| r.volume),
    )));
    a.push(Arc::new(UInt64Array::from_iter_values(
        rows.iter().map(|r| r.count),
    )));
    a.push(Arc::new(BooleanArray::from(
        rows.iter().map(|r| r.ffill).collect::<Vec<_>>(),
    )));
    a.push(Arc::new(StringArray::from_iter_values(
        rows.iter().map(|r| r.status),
    )));
    Ok(RecordBatch::try_new(kline_schema(), a)?)
}

fn light(path: &Path, schema: &Schema, rows: u64) -> Result<()> {
    light_file(File::open(path)?, path, schema, rows)
}
fn light_file(file: File, path: &Path, schema: &Schema, rows: u64) -> Result<()> {
    let b = ParquetRecordBatchReaderBuilder::try_new(file)?;
    ensure!(
        b.metadata().file_metadata().num_rows() as u64 == rows,
        "row count mismatch: {}",
        path.display()
    );
    ensure!(
        b.schema().fields().len() == schema.fields().len(),
        "schema width mismatch"
    );
    for (a, e) in b.schema().fields().iter().zip(schema.fields()) {
        ensure!(
            a.name() == e.name() && a.data_type() == e.data_type(),
            "schema mismatch: {}",
            path.display()
        )
    }
    // Formal day files carry the same RIC and UTC day in every row group.
    // Reuse already-loaded footer statistics; do not read data pages here.
    let parent = path.parent().context("daily file parent")?;
    let ric_dir = parent.parent().context("RIC directory")?;
    let year = ric_dir
        .parent()
        .and_then(Path::file_name)
        .and_then(|v| v.to_str());
    let day = parent.file_name().and_then(|v| v.to_str());
    if let (Some(year), Some(day)) = (year, day) {
        if year.len() == 4
            && year.bytes().all(|v| v.is_ascii_digit())
            && day.len() == 8
            && day.bytes().all(|v| v.is_ascii_digit())
        {
            ensure!(
                day.starts_with(year),
                "UTC date/year directory mismatch: {}",
                path.display()
            );
            let date = chrono::NaiveDate::parse_from_str(day, "%Y%m%d")?;
            let start = u64::try_from(
                date.and_hms_opt(0, 0, 0)
                    .context("UTC midnight")?
                    .and_utc()
                    .timestamp_nanos_opt()
                    .context("UTC day range")?,
            )?;
            let ric = ric_dir
                .file_name()
                .and_then(|v| v.to_str())
                .context("RIC directory encoding")?;
            let ric_column = b.schema().index_of("ric")?;
            let time_column = b.schema().index_of("ts_utc_ns")?;
            for group in b.metadata().row_groups() {
                use parquet::file::statistics::Statistics;
                match group.column(ric_column).statistics() {
                    Some(Statistics::ByteArray(s)) => {
                        let lo = s.min_opt().context("missing RIC min statistic")?.data();
                        let hi = s.max_opt().context("missing RIC max statistic")?.data();
                        ensure!(
                            s.null_count_opt() == Some(0)
                                && lo == hi
                                && super::component(std::str::from_utf8(lo)?) == ric,
                            "footer RIC differs from directory: {}",
                            path.display()
                        );
                    }
                    _ => anyhow::bail!("missing RIC footer statistics: {}", path.display()),
                }
                match group.column(time_column).statistics() {
                    Some(Statistics::Int64(s)) => {
                        let lo =
                            u64::try_from(*s.min_opt().context("missing time min statistic")?)?;
                        let hi =
                            u64::try_from(*s.max_opt().context("missing time max statistic")?)?;
                        ensure!(
                            s.null_count_opt() == Some(0)
                                && start <= lo
                                && lo <= hi
                                && hi < start + DAY,
                            "footer time outside UTC day: {}",
                            path.display()
                        );
                    }
                    _ => anyhow::bail!("missing time footer statistics: {}", path.display()),
                }
            }
        }
    }
    Ok(())
}
/// A single RIC's date directories. NFS directory entries are listed once,
/// then shared by its status and minute writers, rather than probing mkdir
/// and stat for each file. No market rows or open files are retained here.
pub struct DayDirectories {
    root: PathBuf,
    known: HashSet<OsString>,
}
const MAX_DAY_DIRECTORIES: usize = 400;
impl DayDirectories {
    fn new(root: PathBuf) -> Result<Self> {
        let mut known = HashSet::new();
        match fs::read_dir(&root) {
            Ok(entries) => {
                for entry in entries {
                    let name = entry?.file_name();
                    if name
                        .to_str()
                        .is_some_and(|s| s.len() == 8 && s.bytes().all(|b| b.is_ascii_digit()))
                    {
                        ensure!(
                            known.len() < MAX_DAY_DIRECTORIES,
                            "too many date directories in one RIC/year: {}",
                            root.display()
                        );
                        known.insert(name);
                    }
                }
            }
            Err(e) if e.kind() == std::io::ErrorKind::NotFound => {}
            Err(e) => {
                return Err(e)
                    .with_context(|| format!("listing RIC date directories: {}", root.display()))
            }
        }
        Ok(Self { root, known })
    }
    fn plan(&mut self, path: &Path) -> Result<bool> {
        let parent = path.parent().context("output parent")?;
        ensure!(
            parent.parent() == Some(self.root.as_path()),
            "date directory cache belongs to another RIC"
        );
        let day = parent
            .file_name()
            .context("UTC day directory")?
            .to_os_string();
        ensure!(
            self.known.len() < MAX_DAY_DIRECTORIES || self.known.contains(&day),
            "RIC date directory cache limit exceeded"
        );
        Ok(!self.known.insert(day))
    }
}

pub struct Sink {
    pub path: PathBuf,
    temporary: Option<PathBuf>,
    writer: Option<ArrowWriter<File>>,
    schema: Arc<Schema>,
    pub rows: u64,
}
impl Sink {
    pub fn new(path: PathBuf, schema: Arc<Schema>) -> Result<Self> {
        Self::new_impl(path, schema, false)
    }
    pub fn new_in(
        path: PathBuf,
        schema: Arc<Schema>,
        directories: &mut DayDirectories,
    ) -> Result<Self> {
        let known = directories.plan(&path)?;
        Self::new_impl(path, schema, known)
    }
    fn new_impl(path: PathBuf, schema: Arc<Schema>, parent_known: bool) -> Result<Self> {
        static SERIAL: AtomicU64 = AtomicU64::new(0);
        let mut out = Self {
            path,
            temporary: None,
            writer: None,
            schema,
            rows: 0,
        };
        if !out.path.exists() {
            if !parent_known {
                fs::create_dir_all(out.path.parent().context("output parent")?)?;
            }
            let p = out.path.with_file_name(format!(
                ".{}.{}.{}.partial",
                out.path.file_name().unwrap().to_string_lossy(),
                std::process::id(),
                SERIAL.fetch_add(1, Ordering::Relaxed)
            ));
            let props = WriterProperties::builder()
                .set_max_row_group_size(BATCH)
                .set_compression(Compression::ZSTD(ZstdLevel::try_new(3)?))
                .build();
            let create = || {
                File::options()
                    .read(true)
                    .write(true)
                    .create_new(true)
                    .open(&p)
            };
            let file = match create() {
                Ok(file) => file,
                // Another process may remove an empty date directory after
                // enumeration. Recover without weakening no-overwrite publish.
                Err(e) if parent_known && e.kind() == std::io::ErrorKind::NotFound => {
                    fs::create_dir_all(out.path.parent().context("output parent")?)?;
                    create()?
                }
                Err(e) => return Err(e.into()),
            };
            out.writer = Some(ArrowWriter::try_new(file, out.schema.clone(), Some(props))?);
            out.temporary = Some(p)
        }
        Ok(out)
    }
    pub fn write(&mut self, batch: &RecordBatch) -> Result<()> {
        if let Some(w) = &mut self.writer {
            w.write(batch)?
        }
        self.rows += batch.num_rows() as u64;
        Ok(())
    }
    pub fn finish(mut self) -> Result<PathBuf> {
        if let Some(mut w) = self.writer.take() {
            w.finish()?;
            w.inner().sync_all()?;
            // The finished writer still owns the verified temporary inode.
            // dup its descriptor for the footer check, avoiding another NFS
            // pathname lookup/open for every newly generated daily file.
            light_file(w.inner().try_clone()?, &self.path, &self.schema, self.rows)?;
            drop(w);
            let p = self.temporary.as_ref().unwrap();
            let published = match fs::hard_link(p, &self.path) {
                Ok(()) => true,
                Err(e) if e.kind() == std::io::ErrorKind::AlreadyExists => false,
                Err(e) => return Err(e.into()),
            };
            fs::remove_file(p)?;
            self.temporary = None;
            // A successful hard link publishes the same inode whose complete
            // footer was just verified. Existing competing targets still need
            // their own check; sampled content readback remains in the caller.
            if published {
                return Ok(self.path.clone());
            }
        }
        light(&self.path, &self.schema, self.rows)?;
        Ok(self.path.clone())
    }
    fn finish_queued(self, pending: &mut PendingFiles) -> Result<PathBuf> {
        let path = self.path.clone();
        pending.execute(move || self.finish())?;
        Ok(path)
    }
}
impl Drop for Sink {
    fn drop(&mut self) {
        if let Some(p) = &self.temporary {
            let _ = fs::remove_file(p);
        }
    }
}
#[derive(Clone)]
pub struct StatusFile {
    pub path: PathBuf,
    pub day: u64,
    pub rows: u64,
    pub before: State,
}
pub struct StatusWriter {
    root: PathBuf,
    product: String,
    ric: String,
    buffer: Vec<StatusRow>,
    sink: Option<Sink>,
    day: u64,
    before: State,
    pub files: Vec<StatusFile>,
    directories: DayDirectories,
    pending: PendingFiles,
}
impl StatusWriter {
    pub fn new(root: PathBuf, product: &str, ric: &str) -> Result<Self> {
        Self::new_with_pool(root, product, ric, None)
    }
    pub fn new_with_pool(
        root: PathBuf,
        product: &str,
        ric: &str,
        pool: Option<Arc<PublicationPool>>,
    ) -> Result<Self> {
        let directories = DayDirectories::new(root.clone())?;
        Ok(Self {
            root,
            product: product.into(),
            ric: ric.into(),
            buffer: Vec::with_capacity(BATCH),
            sink: None,
            day: 0,
            before: State::Unknown,
            files: Vec::new(),
            directories,
            pending: PendingFiles::new(pool),
        })
    }
    fn flush(&mut self) -> Result<()> {
        if !self.buffer.is_empty() {
            self.sink.as_mut().unwrap().write(&status_batch(
                &self.product,
                &self.ric,
                &self.buffer,
            )?)?;
            self.buffer.clear()
        }
        Ok(())
    }
    fn close(&mut self) -> Result<()> {
        self.flush()?;
        if let Some(sink) = self.sink.take() {
            let rows = sink.rows;
            let path = sink.finish_queued(&mut self.pending)?;
            self.files.push(StatusFile {
                path,
                day: self.day,
                rows,
                before: self.before,
            });
        }
        Ok(())
    }
    pub fn push(&mut self, row: StatusRow) -> Result<()> {
        let day = row.at / DAY;
        if self.sink.is_none() || day != self.day {
            self.close()?;
            self.day = day;
            self.before = row.before;
            self.sink = Some(Sink::new_in(
                self.root
                    .join(date(day * DAY)?)
                    .join("session_status.parquet"),
                status_schema(),
                &mut self.directories,
            )?)
        }
        self.buffer.push(row);
        if self.buffer.len() == BATCH {
            self.flush()?
        }
        Ok(())
    }
    pub fn finish(self) -> Result<Vec<StatusFile>> {
        self.finish_with_directories().map(|(files, _)| files)
    }
    pub fn finish_with_directories(mut self) -> Result<(Vec<StatusFile>, DayDirectories)> {
        self.close()?;
        self.pending.finish()?;
        Ok((std::mem::take(&mut self.files), self.directories))
    }
}

pub fn write_kline(
    path: PathBuf,
    batch: RecordBatch,
    directories: &mut DayDirectories,
    pending: &mut PendingFiles,
    sample: bool,
) -> Result<()> {
    let known = directories.plan(&path)?;
    pending.execute(move || {
        let mut sink = Sink::new_impl(path.clone(), kline_schema(), known)?;
        sink.write(&batch)?;
        sink.finish()?;
        if sample {
            verify_kline(&path, &batch)?;
        }
        Ok(path)
    })
}

#[cfg(test)]
mod performance_tests {
    use super::*;
    use crate::native_kline::{session::Decoder, MINUTE};
    use std::{
        sync::atomic::{AtomicBool, AtomicU64, Ordering},
        time::{Duration, Instant},
    };

    #[test]
    fn footer_rejects_same_schema_and_count_with_wrong_ric_or_day() -> Result<()> {
        let nonce = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)?
            .as_nanos();
        let root = PathBuf::from(format!(
            "/mnt/nvme-raid0-28t/fanghaizhou/cargo-target/market_replay/tests/footer_{}_{nonce}",
            std::process::id()
        ));
        let start = year_start(2026)?;
        let batch = |ric, at| {
            kline_batch(
                ric,
                &[Kline {
                    at,
                    prices: [Some(100_000_000_000); 4],
                    volume: 1,
                    count: 1,
                    ffill: false,
                    status: "TRADING",
                }],
            )
        };
        let good = root.join("cme/2026/X/20260101/kline.parquet");
        let mut sink = Sink::new(good.clone(), kline_schema())?;
        sink.write(&batch("X", start)?)?;
        sink.finish()?;
        for (name, day, ric, at, message) in [
            ("wrong_ric", "20260101", "Y", start, "RIC"),
            ("wrong_day", "20260102", "X", start, "UTC day"),
        ] {
            // Simulate a pre-existing file from another writer: schema and row
            // count match, so only its footer identity exposes the conflict.
            let path = root
                .join(name)
                .join("cme/2026/X")
                .join(day)
                .join("kline.parquet");
            fs::create_dir_all(path.parent().unwrap())?;
            let mut writer = ArrowWriter::try_new(File::create(&path)?, kline_schema(), None)?;
            writer.write(&batch(ric, at)?)?;
            writer.close()?;
            let original = fs::read(&path)?;
            let mut sink = Sink::new(path.clone(), kline_schema())?;
            sink.write(&batch(
                "X",
                if day == "20260101" {
                    start
                } else {
                    start + DAY
                },
            )?)?;
            assert!(sink.finish().unwrap_err().to_string().contains(message));
            assert_eq!(fs::read(&path)?, original);
        }
        fs::remove_dir_all(root)?;
        Ok(())
    }

    #[test]
    #[ignore = "private temporary NFS performance measurement; not a routine data readback"]
    fn bounded_publication_benchmark() -> Result<()> {
        let nonce = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)?
            .as_nanos();
        let root = PathBuf::from(format!(
            "/mnt/nfs/30.100_cta_data/lseg/.io_benchmark_{}_{nonce}",
            std::process::id()
        ));
        fs::create_dir(&root)?;
        struct Cleanup(PathBuf);
        impl Drop for Cleanup {
            fn drop(&mut self) {
                let _ = fs::remove_dir_all(&self.0);
            }
        }
        let _cleanup = Cleanup(root.clone());
        let mut baseline = 0.0;
        for workers in [0, 2, 8, 16] {
            let pool = if workers == 0 {
                None
            } else {
                Some(PublicationPool::new(workers)?)
            };
            let active = Arc::new(AtomicBool::new(true));
            let threads = Arc::new(AtomicU64::new(0));
            let rss = Arc::new(AtomicU64::new(0));
            let monitor = {
                let (active, threads, rss) = (active.clone(), threads.clone(), rss.clone());
                std::thread::spawn(move || {
                    while active.load(Ordering::Relaxed) {
                        if let Ok(status) = fs::read_to_string("/proc/self/status") {
                            for line in status.lines() {
                                let mut fields = line.split_whitespace();
                                let name = fields.next().unwrap_or("");
                                if let Some(value) =
                                    fields.next().and_then(|v| v.parse::<u64>().ok())
                                {
                                    match name {
                                        "Threads:" => {
                                            threads.fetch_max(value, Ordering::Relaxed);
                                        }
                                        "VmRSS:" => {
                                            rss.fetch_max(value, Ordering::Relaxed);
                                        }
                                        _ => {}
                                    }
                                }
                            }
                        }
                        std::thread::sleep(Duration::from_millis(25));
                    }
                })
            };
            let begin = Instant::now();
            let result = std::thread::scope(|scope| -> Result<()> {
                let mut handles = Vec::new();
                for worker in 0..2 {
                    let pool = pool.clone();
                    let root = root.join(format!("io_{workers}/ric_{worker}"));
                    handles.push(scope.spawn(move || -> Result<()> {
                        let start = year_start(2026)?;
                        let decoded = Decoder::new(true).decode("15[TRD_TYPE]", State::Unknown);
                        let mut status =
                            StatusWriter::new_with_pool(root.clone(), "X", "X", pool.clone())?;
                        for day in 0..32 {
                            for offset in [0, MINUTE] {
                                status.push(StatusRow {
                                    at: start + day * DAY + offset,
                                    before: State::Unknown,
                                    decoded: decoded.clone(),
                                    qualifiers: "15[TRD_TYPE]".into(),
                                })?;
                            }
                        }
                        let (_, mut directories) = status.finish_with_directories()?;
                        let mut pending = PendingFiles::new(pool);
                        for day in 0..32 {
                            let start = start + day * DAY;
                            let rows = (0..1440)
                                .map(|minute| Kline {
                                    at: start + minute * MINUTE,
                                    prices: [Some(100_000_000_000); 4],
                                    volume: 1,
                                    count: 1,
                                    ffill: false,
                                    status: "TRADING",
                                })
                                .collect::<Vec<_>>();
                            let path = root.join(date(start)?).join("kline.parquet");
                            write_kline(
                                path,
                                kline_batch("X", &rows)?,
                                &mut directories,
                                &mut pending,
                                [0, 16, 31].contains(&day),
                            )?;
                        }
                        pending.finish()
                    }));
                }
                for handle in handles {
                    handle
                        .join()
                        .map_err(|_| anyhow::anyhow!("benchmark worker panicked"))??;
                }
                Ok(())
            });
            active.store(false, Ordering::Relaxed);
            monitor
                .join()
                .map_err(|_| anyhow::anyhow!("benchmark monitor panicked"))?;
            result?;
            let elapsed = begin.elapsed().as_secs_f64();
            if workers == 0 {
                baseline = elapsed;
            }
            eprintln!("io_workers={workers}; files=128; seconds={elapsed:.3}; files_per_second={:.2}; baseline_speedup={:.2}; peak_threads={}; peak_rss_kib={}",
                128.0 / elapsed, baseline / elapsed, threads.load(Ordering::Relaxed), rss.load(Ordering::Relaxed));
            drop(pool);
        }
        Ok(())
    }
}
pub fn compare_status(
    batch: &RecordBatch,
    i: usize,
    product: &str,
    ric: &str,
    r: &StatusRow,
) -> Result<()> {
    for (name, expected) in [
        ("product", product),
        ("ric", ric),
        ("action", r.decoded.action),
        ("session_state", r.decoded.state.name()),
        ("reason", r.decoded.reason),
        ("source", "LSEG_TAS"),
        ("qualifiers", r.qualifiers.as_str()),
    ] {
        ensure!(
            string(batch, i, name)? == expected,
            "sample status value mismatch: {name}"
        )
    }
    for name in ["ts_utc_ns", "published_ns", "effective_ns"] {
        ensure!(
            uint(batch, i, name)? == r.at,
            "sample status clock mismatch"
        )
    }
    let a = batch
        .column_by_name("is_trading")
        .unwrap()
        .as_any()
        .downcast_ref::<BooleanArray>()
        .context("is_trading type")?;
    ensure!(
        if a.is_null(i) { None } else { Some(a.value(i)) } == r.decoded.trading,
        "sample trading flag mismatch"
    );
    let a = batch
        .column_by_name("is_transition")
        .unwrap()
        .as_any()
        .downcast_ref::<BooleanArray>()
        .unwrap();
    ensure!(
        a.value(i) == (r.before != r.decoded.state),
        "sample transition mismatch"
    );
    ensure!(
        batch.column_by_name("source_url").unwrap().is_null(i),
        "unexpected source URL"
    );
    Ok(())
}
pub fn verify_kline(path: &Path, expected: &RecordBatch) -> Result<()> {
    let mut actual = Rows::new(path)?;
    for row in 0..expected.num_rows() {
        let (batch, i) = actual.next_row()?.context("sample missing kline")?;
        for column in 0..expected.num_columns() {
            ensure!(
                batch.column(column).slice(i, 1).to_data()
                    == expected.column(column).slice(row, 1).to_data(),
                "sample kline mismatch: {} row {row}",
                path.display()
            )
        }
    }
    ensure!(actual.next_row()?.is_none(), "sample extra kline rows");
    Ok(())
}

#[derive(Serialize, Deserialize, Clone, Debug, Default)]
pub struct Stats {
    pub rics: u64,
    #[serde(default)]
    pub status_only_rics: u64,
    pub reused_rics: u64,
    pub trades: u64,
    pub volume: u64,
    pub kline_rows: u64,
    pub files: u64,
    pub status_files: u64,
    #[serde(default)]
    pub status_rows: u64,
}
impl Stats {
    pub fn add(&mut self, other: &Self) {
        self.rics += other.rics;
        self.status_only_rics += other.status_only_rics;
        self.reused_rics += other.reused_rics;
        self.trades += other.trades;
        self.volume += other.volume;
        self.kline_rows += other.kline_rows;
        self.files += other.files;
        self.status_files += other.status_files;
        self.status_rows += other.status_rows
    }
}
pub struct Problems {
    path: PathBuf,
    writer: Option<csv::Writer<File>>,
    pub count: u64,
}
impl Problems {
    pub fn new(path: PathBuf) -> Self {
        Self {
            path,
            writer: None,
            count: 0,
        }
    }
    pub fn record(
        &mut self,
        ric: &str,
        issue: &str,
        start: u64,
        end: u64,
        count: u64,
        detail: &str,
    ) -> Result<()> {
        if self.writer.is_none() {
            fs::create_dir_all(self.path.parent().unwrap())?;
            let mut w = csv::Writer::from_path(self.path.with_extension("csv.partial"))?;
            w.write_record(["ric", "issue", "start_ns", "end_ns", "count", "detail"])?;
            self.writer = Some(w)
        }
        self.writer.as_mut().unwrap().write_record([
            ric,
            issue,
            &start.to_string(),
            &end.to_string(),
            &count.to_string(),
            detail,
        ])?;
        self.count += 1;
        Ok(())
    }
    pub fn finish(mut self) -> Result<()> {
        if let Some(mut w) = self.writer.take() {
            w.flush()?;
            drop(w);
            fs::rename(self.path.with_extension("csv.partial"), self.path)?
        } else if self.path.exists() {
            fs::remove_file(self.path)?
        }
        Ok(())
    }
}
pub fn sample_indices(n: usize) -> BTreeMap<usize, ()> {
    [0, n / 2, n.saturating_sub(1)]
        .into_iter()
        .filter(|_| n > 0)
        .map(|i| (i, ()))
        .collect()
}

pub fn compare(left: &Path, right: &Path) -> Result<()> {
    let pairs = if left.is_file() && right.is_file() {
        vec![(left.to_owned(), right.to_owned())]
    } else {
        let days = |root: &Path| -> Result<BTreeMap<String, PathBuf>> {
            let mut out = BTreeMap::new();
            for entry in fs::read_dir(root)? {
                let e = entry?;
                let p = e.path().join("kline.parquet");
                if p.is_file() {
                    out.insert(e.file_name().to_string_lossy().into_owned(), p);
                }
            }
            Ok(out)
        };
        let a = days(left)?;
        let b = days(right)?;
        ensure!(
            a.keys().eq(b.keys()),
            "UTC day sets differ: left={},right={}",
            a.len(),
            b.len()
        );
        let items = a.into_iter().collect::<Vec<_>>();
        sample_indices(items.len())
            .into_keys()
            .map(|i| {
                let (day, path) = &items[i];
                (path.clone(), b[day].clone())
            })
            .collect()
    };
    ensure!(!pairs.is_empty(), "no Parquet comparison inputs");
    let mut count = 0;
    for (left, right) in &pairs {
        let mut a = Rows::new(left)?;
        let mut b = Rows::new(right)?;
        while let Some((x, i)) = a.next_row()? {
            let (y, j) = b.next_row()?.context("right side has fewer rows")?;
            ensure!(x.num_columns() == y.num_columns(), "schema width mismatch");
            for column in 0..x.num_columns() {
                ensure!(
                    x.schema().field(column).name() == y.schema().field(column).name()
                        && x.column(column).slice(i, 1).to_data()
                            == y.column(column).slice(j, 1).to_data(),
                    "{}: mismatch at row {count}, column {}",
                    left.display(),
                    x.schema().field(column).name()
                )
            }
            count += 1;
        }
        ensure!(b.next_row()?.is_none(), "right side has extra rows");
    }
    println!(
        "matched {count} rows across {} sampled Parquet file pairs",
        pairs.len()
    );
    Ok(())
}
