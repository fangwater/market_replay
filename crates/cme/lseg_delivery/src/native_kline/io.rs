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
/// Per-file inputs of the kline metadata.
#[derive(Clone, Debug, Default)]
pub struct KlineInfo {
    /// Group source signature (same digest as the Python full build).
    pub input_signature: String,
    /// Unresolved kind=12 controls of this RIC-year (all of them without a
    /// witness ledger).
    pub correction_pending_count: usize,
    /// `correction_plan_sha256` when a witness ledger was applied.
    pub correction_plan_sha256: Option<String>,
}

/// The production key set of `lseg_only_full_v1` kline.parquet, with the
/// descriptive texts copied from delivered files. Netted RICs additionally
/// carry the `correction_policy_version=1` keys.
pub fn kline_metadata(info: &KlineInfo) -> Meta {
    let mut m = Meta::from([
        ("source", "LSEG TAS trades, corrections and Mkt. Condition qualifiers only"),
        ("kline_status_policy", "lseg_only_full_v1"),
        ("kline_schema_version", "5"),
        ("fill_policy_version", "5"),
        ("frequency", "1min; UTC minute-start labels"),
        ("price_scale", "1; open/high/low/close are nullable Float64 in actual price units, same units as baseline and IB"),
        ("trade_time_basis", "exchange time, nearest UTC date within 12h of publication; missing exchange time falls back to publication"),
        ("empty_minutes", "continuous UTC grid in valid covered instrument lifetime; all no-trade prices use own previous close, volume/trade_count=0; null if no own prior price; is_ffill indicates an actual fill"),
        ("is_ffill", "true only when no trade occurred and OHLC was filled from the previous close; unpriced empty rows remain null and false; volume/trade_count are never filled"),
        ("status", "TRADING/SCHEDULED_CLOSED/HALTED/PARTIAL/UNKNOWN; detailed LSEG phases/qualifiers in session_status.parquet"),
        ("status_scope", "CME-native phase semantics only for LSEG-verified CME Group venue; other venues explicit halt flag only, otherwise UNKNOWN"),
        ("status_time_basis", "LSEG publication time; same-time source patch order; missing archive resets UNKNOWN"),
        ("lifetime_source", "LSEG HRD consistent with observed source lifetime; native CME expiry; otherwise last observed LSEG evidence"),
    ]
    .map(|(k, v)| (k.to_owned(), v.to_owned())));
    m.insert("input_signature".into(), info.input_signature.clone());
    m.insert(
        "correction_pending_count".into(),
        info.correction_pending_count.to_string(),
    );
    match &info.correction_plan_sha256 {
        None => {
            m.insert("correction_scope".into(), "raw Trade aggregation; unlinked kind=12 controls retained as problems; no speculative deletion".into());
            m.insert("exception_policy".into(), "raw prints aggregated without revision netting; non-trading/unknown-state prints retained; diagnostic details under meta".into());
        }
        Some(sha) => {
            m.insert("correction_scope".into(), "witnessed kind=12 cancels/corrections/inserts netted (correction_policy_version=1); unlinked kind=12 controls retained as problems; no speculative deletion".into());
            m.insert("exception_policy".into(), "raw prints immutable; reliably linked historical cancels/corrections/inserts applied; all remaining prints retained including non-trading/unknown-state prints; unresolved links under meta/problems".into());
            m.insert(
                "correction_policy_version".into(),
                super::netting::VERSION.into(),
            );
            m.insert("correction_plan_sha256".into(), sha.clone());
            m.insert("correction_pending_scope".into(), "pending at ledger evaluation; current unresolved issues are in meta/problems/native_kline; ledger witnesses retained locally by hash".into());
            m.insert("trade_revisions".into(), "historical as-of-source netting of reliably linked cancellations/corrections/inserts; raw ticks immutable; unresolved links retained under meta/problems".into());
        }
    }
    m
}

fn kline_fields() -> Vec<Field> {
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
    fields
}
/// Columns with the per-file metadata of `info`.
pub fn kline_schema_for(info: &KlineInfo) -> Arc<Schema> {
    Arc::new(Schema::new_with_metadata(
        kline_fields(),
        kline_metadata(info),
    ))
}
/// Columns with metadata for an unidentified source (tests, comparisons).
pub fn kline_schema() -> Arc<Schema> {
    kline_schema_for(&KlineInfo::default())
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
    kline_batch_with(kline_schema(), ric, rows)
}
pub fn kline_batch_with(schema: Arc<Schema>, ric: &str, rows: &[Kline]) -> Result<RecordBatch> {
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
    Ok(RecordBatch::try_new(schema, a)?)
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

/// Replacement of already-delivered kline / session_status day files.
/// Without it (the default) an existing file is only footer-checked.
pub struct Replace {
    pub delivery_root: PathBuf,
    pub backup_root: PathBuf,
    /// Existing files of earlier UTC days are only footer-checked.
    pub compare_from: u64,
    log: std::sync::Mutex<File>,
}
impl Replace {
    pub fn new(
        delivery_root: PathBuf,
        backup_root: PathBuf,
        compare_from: u64,
        log: &Path,
    ) -> Result<Arc<Self>> {
        ensure!(
            !backup_root.starts_with(&delivery_root) && !log.starts_with(&delivery_root),
            "backups and the replacement log must not be on the delivery disk"
        );
        fs::create_dir_all(log.parent().context("log parent")?)?;
        let log = File::options().create(true).append(true).open(log)?;
        Ok(Arc::new(Self {
            delivery_root,
            backup_root,
            compare_from,
            log: std::sync::Mutex::new(log),
        }))
    }
    fn record(&self, value: serde_json::Value) -> Result<()> {
        use std::io::Write;
        let mut f = self.log.lock().unwrap();
        writeln!(f, "{value}")?;
        f.sync_data()?;
        Ok(())
    }
}

/// UTC day start of `<...>/<YYYYMMDD>/<file>`.
fn day_of(path: &Path) -> Option<u64> {
    let d = path.parent()?.file_name()?.to_str()?;
    let date = chrono::NaiveDate::parse_from_str(d, "%Y%m%d").ok()?;
    u64::try_from(date.and_hms_opt(0, 0, 0)?.and_utc().timestamp_nanos_opt()?).ok()
}

pub fn read_all(path: &Path) -> Result<RecordBatch> {
    let b = ParquetRecordBatchReaderBuilder::try_new(File::open(path)?)?;
    let schema = b.schema().clone();
    let batches = b
        .with_batch_size(BATCH)
        .build()?
        .collect::<std::result::Result<Vec<_>, _>>()?;
    Ok(arrow_select::concat::concat_batches(&schema, &batches)?)
}

/// Same columns (name, type) and values; file/schema metadata is ignored,
/// since delivered files may carry descriptive keys added later.
pub fn same_values(a: &RecordBatch, b: &RecordBatch) -> bool {
    a.num_rows() == b.num_rows()
        && a.num_columns() == b.num_columns()
        && a.schema()
            .fields()
            .iter()
            .zip(b.schema().fields())
            .all(|(x, y)| x.name() == y.name() && x.data_type() == y.data_type())
        && (0..a.num_columns()).all(|i| a.column(i).to_data() == b.column(i).to_data())
}

/// Rows kept in memory to compare one existing day file (far above any
/// observed day: 1440 minutes, or a few thousand status messages).
const COMPARE_ROWS: u64 = 4_000_000;

#[derive(PartialEq)]
enum Mode {
    /// New file: temp + footer check + no-overwrite hard link.
    Create,
    /// Existing file: footer check only.
    Keep,
    /// Existing file of the earlier Python generation of the 237 revised
    /// 2026 RICs outside the compare window: left as delivered. Their seed /
    /// lifetime policy differs for a few RICs (row counts may differ).
    Legacy,
    /// Existing file: compare values; replace with backup if different.
    Compare,
}

pub struct Sink {
    pub path: PathBuf,
    temporary: Option<PathBuf>,
    writer: Option<ArrowWriter<File>>,
    schema: Arc<Schema>,
    pub rows: u64,
    mode: Mode,
    policy: Option<Arc<Replace>>,
    buffered: Vec<RecordBatch>,
}
fn writer_properties() -> Result<WriterProperties> {
    Ok(WriterProperties::builder()
        .set_max_row_group_size(BATCH)
        .set_compression(Compression::ZSTD(ZstdLevel::try_new(3)?))
        .build())
}
fn open_temp(path: &Path, parent_known: bool) -> Result<(PathBuf, File)> {
    static SERIAL: AtomicU64 = AtomicU64::new(0);
    if !parent_known {
        fs::create_dir_all(path.parent().context("output parent")?)?;
    }
    let p = path.with_file_name(format!(
        ".{}.{}.{}.partial",
        path.file_name().unwrap().to_string_lossy(),
        std::process::id(),
        SERIAL.fetch_add(1, Ordering::Relaxed)
    ));
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
            fs::create_dir_all(path.parent().context("output parent")?)?;
            create()?
        }
        Err(e) => return Err(e.into()),
    };
    Ok((p, file))
}
impl Sink {
    pub fn new(path: PathBuf, schema: Arc<Schema>) -> Result<Self> {
        Self::new_impl(path, schema, false, None)
    }
    pub fn new_in(
        path: PathBuf,
        schema: Arc<Schema>,
        directories: &mut DayDirectories,
    ) -> Result<Self> {
        Self::new_with(path, schema, directories, None)
    }
    pub fn new_with(
        path: PathBuf,
        schema: Arc<Schema>,
        directories: &mut DayDirectories,
        policy: Option<Arc<Replace>>,
    ) -> Result<Self> {
        let known = directories.plan(&path)?;
        Self::new_impl(path, schema, known, policy)
    }
    fn new_impl(
        path: PathBuf,
        schema: Arc<Schema>,
        parent_known: bool,
        policy: Option<Arc<Replace>>,
    ) -> Result<Self> {
        let mut out = Self {
            path,
            temporary: None,
            writer: None,
            schema,
            rows: 0,
            mode: Mode::Create,
            policy,
            buffered: Vec::new(),
        };
        if out.path.exists() {
            out.mode = if out
                .policy
                .as_ref()
                .is_some_and(|p| day_of(&out.path).is_some_and(|d| d >= p.compare_from))
            {
                Mode::Compare
            } else {
                Mode::Keep
            };
            return Ok(out);
        }
        let (p, file) = open_temp(&out.path, parent_known)?;
        out.writer = Some(ArrowWriter::try_new(
            file,
            out.schema.clone(),
            Some(writer_properties()?),
        )?);
        out.temporary = Some(p);
        Ok(out)
    }
    pub fn write(&mut self, batch: &RecordBatch) -> Result<()> {
        if let Some(w) = &mut self.writer {
            w.write(batch)?
        }
        if self.mode == Mode::Compare {
            ensure!(
                self.rows + batch.num_rows() as u64 <= COMPARE_ROWS,
                "day file too large to compare in memory: {}",
                self.path.display()
            );
            self.buffered.push(batch.clone());
        }
        self.rows += batch.num_rows() as u64;
        Ok(())
    }
    /// Existing file differs: new temp file (footer checked), byte-verified
    /// local backup of the old one, then rename over it.
    fn compare_replace(&mut self) -> Result<()> {
        let policy = self.policy.clone().context("replace policy")?;
        let new = arrow_select::concat::concat_batches(&self.schema, &self.buffered)?;
        let old = read_all(&self.path)
            .with_context(|| format!("reading existing {}", self.path.display()))?;
        if same_values(&old, &new) {
            return light(&self.path, &self.schema, self.rows);
        }
        let prefix =
            old.num_rows() < new.num_rows() && same_values(&old, &new.slice(0, old.num_rows()));
        let (temp, file) = open_temp(&self.path, true)?;
        self.temporary = Some(temp.clone());
        let mut w = ArrowWriter::try_new(file, self.schema.clone(), Some(writer_properties()?))?;
        w.write(&new)?;
        w.finish()?;
        w.inner().sync_all()?;
        light_file(w.inner().try_clone()?, &self.path, &self.schema, self.rows)?;
        drop(w);
        let backup = safe_replace::replace_with_backup(
            &temp,
            &self.path,
            &policy.delivery_root,
            &policy.backup_root,
        )?;
        self.temporary = None;
        light(&self.path, &self.schema, self.rows)?;
        ensure!(
            same_values(&read_all(&self.path)?, &new),
            "replaced file readback differs: {}",
            self.path.display()
        );
        policy.record(serde_json::json!({
            "path": self.path.strip_prefix(&policy.delivery_root).unwrap_or(&self.path),
            "backup": backup,
            "old_rows": old.num_rows(),
            "new_rows": new.num_rows(),
            "old_is_prefix": prefix,
        }))
    }
    pub fn finish(mut self) -> Result<PathBuf> {
        match self.mode {
            Mode::Compare => {
                self.compare_replace()?;
                return Ok(self.path.clone());
            }
            Mode::Legacy => return Ok(self.path.clone()),
            Mode::Keep | Mode::Create => {}
        }
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
    policy: Option<Arc<Replace>>,
    /// Post-expiry rule (same as `lseg_catalog --prune-expired-status`).
    kline_last_day: Option<u64>,
    expired_day: Option<u64>,
    day_open: bool,
    day_expire: bool,
    day_redundant: bool,
    holding: bool,
    held: Vec<(u64, State, Vec<StatusRow>)>,
    held_rows: usize,
    last_state: State,
    skipped: u64,
}
/// Rows of redundant post-expiry status days kept in memory until the
/// RIC's final state is known; beyond this they are simply written.
const HELD_ROWS: usize = 1_000_000;
impl StatusWriter {
    /// Existing day files: compare/replace under `policy`.
    pub fn set_policy(&mut self, policy: Option<Arc<Replace>>) {
        self.policy = policy;
    }
    /// Last UTC day index that also gets a kline.parquet (`None`: no kline).
    /// Such days are never treated as status-only.
    pub fn set_kline_last_day(&mut self, day: Option<u64>) {
        self.kline_last_day = day;
    }
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
            policy: None,
            kline_last_day: None,
            expired_day: None,
            day_open: false,
            day_expire: false,
            day_redundant: true,
            holding: false,
            held: Vec::new(),
            held_rows: 0,
            last_state: State::Unknown,
            skipped: 0,
        })
    }
    fn open_sink(&mut self) -> Result<()> {
        self.sink = Some(Sink::new_with(
            self.root
                .join(date(self.day * DAY)?)
                .join("session_status.parquet"),
            status_schema(),
            &mut self.directories,
            self.policy.clone(),
        )?);
        Ok(())
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
    fn close_sink(&mut self) -> Result<()> {
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
    /// Write one held day as an ordinary file.
    fn write_held(&mut self, day: u64, before: State, rows: Vec<StatusRow>) -> Result<()> {
        self.day = day;
        self.before = before;
        self.open_sink()?;
        for chunk in rows.chunks(BATCH) {
            self.buffer.extend_from_slice(chunk);
            self.flush()?;
        }
        self.close_sink()
    }
    fn release_held(&mut self) -> Result<()> {
        let (day, before) = (self.day, self.before);
        for (d, b, rows) in std::mem::take(&mut self.held) {
            self.write_held(d, b, rows)?
        }
        self.held_rows = 0;
        (self.day, self.before) = (day, before);
        Ok(())
    }
    fn close(&mut self) -> Result<()> {
        if !self.day_open {
            return Ok(());
        }
        if self.holding {
            self.holding = false;
            let rows = std::mem::take(&mut self.buffer);
            self.held_rows += rows.len();
            self.held.push((self.day, self.before, rows));
            if self.held_rows > HELD_ROWS {
                self.release_held()?
            }
        } else {
            self.close_sink()?;
        }
        if self.expired_day.is_none() && self.day_expire && self.last_state == State::Expired {
            self.expired_day = Some(self.day)
        }
        self.day_open = false;
        Ok(())
    }
    pub fn push(&mut self, row: StatusRow) -> Result<()> {
        let day = row.at / DAY;
        if !self.day_open || day != self.day {
            self.close()?;
            self.day = day;
            self.before = row.before;
            self.day_open = true;
            self.day_expire = false;
            self.day_redundant = true;
            // Candidate: strictly after the explicit expiry day, no kline.
            self.holding = self.expired_day.is_some_and(|e| day > e)
                && self.kline_last_day.is_none_or(|k| day > k);
            if !self.holding {
                self.open_sink()?
            }
        }
        if row.decoded.action == "EXPIRE" && row.decoded.state == State::Expired {
            self.day_expire = true
        }
        self.day_redundant &=
            row.decoded.state == State::Expired && row.decoded.action == "NO_CHANGE";
        self.last_state = row.decoded.state;
        if self.holding && (!self.day_redundant || self.buffer.len() >= HELD_ROWS) {
            // Real content after expiry: an ordinary file.
            self.holding = false;
            self.open_sink()?;
            let held = std::mem::take(&mut self.buffer);
            for chunk in held.chunks(BATCH) {
                self.buffer.extend_from_slice(chunk);
                self.flush()?;
            }
        }
        self.buffer.push(row);
        if !self.holding && self.buffer.len() == BATCH {
            self.flush()?
        }
        Ok(())
    }
    pub fn finish(self) -> Result<Vec<StatusFile>> {
        self.finish_with_directories().map(|(files, _, _)| files)
    }
    /// Files written, the directory cache, and the number of redundant
    /// post-expiry status-only days not written.
    pub fn finish_with_directories(mut self) -> Result<(Vec<StatusFile>, DayDirectories, u64)> {
        self.close()?;
        if self.last_state == State::Expired {
            for (day, before, rows) in std::mem::take(&mut self.held) {
                if only_status_files(&self.root.join(date(day * DAY)?))? {
                    self.skipped += 1;
                } else {
                    self.write_held(day, before, rows)?
                }
            }
        } else {
            self.release_held()?
        }
        self.pending.finish()?;
        self.files.sort_by_key(|f| f.day);
        Ok((
            std::mem::take(&mut self.files),
            self.directories,
            self.skipped,
        ))
    }
}

/// The day directory holds nothing but status files (or does not exist).
fn only_status_files(dir: &Path) -> Result<bool> {
    let entries = match fs::read_dir(dir) {
        Ok(e) => e,
        Err(e) if e.kind() == std::io::ErrorKind::NotFound => return Ok(true),
        Err(e) => return Err(e.into()),
    };
    for entry in entries {
        let name = entry?.file_name();
        if !matches!(
            name.to_str(),
            Some("status.parquet" | "session_status.parquet")
        ) {
            return Ok(false);
        }
    }
    Ok(true)
}

pub fn write_kline(
    path: PathBuf,
    batch: RecordBatch,
    directories: &mut DayDirectories,
    pending: &mut PendingFiles,
    sample: bool,
    policy: Option<Arc<Replace>>,
    legacy: bool,
) -> Result<()> {
    let known = directories.plan(&path)?;
    pending.execute(move || {
        let mut sink = Sink::new_impl(path.clone(), batch.schema(), known, policy)?;
        if legacy && sink.mode == Mode::Keep {
            sink.mode = Mode::Legacy
        }
        let kept = sink.mode == Mode::Legacy;
        sink.write(&batch)?;
        sink.finish()?;
        if sample && !kept {
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
                        let (_, mut directories, _) = status.finish_with_directories()?;
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
                                None,
                                false,
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
    /// Redundant post-expiry status-only days not written.
    #[serde(default)]
    pub status_days_skipped: u64,
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
        self.status_rows += other.status_rows;
        self.status_days_skipped += other.status_days_skipped
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

#[cfg(test)]
mod replace_tests {
    use super::*;

    fn day(minutes: u64, close: i64) -> RecordBatch {
        let start = 1_794_009_600_000_000_000u64; // 2026-11-07 00:00 UTC
        let rows: Vec<Kline> = (0..minutes)
            .map(|i| Kline {
                at: start + i * crate::native_kline::MINUTE,
                prices: [Some(close); 4],
                volume: 0,
                count: 0,
                ffill: true,
                status: "UNKNOWN",
            })
            .collect();
        kline_batch("X", &rows).unwrap()
    }

    fn sink(path: &Path, policy: Option<Arc<Replace>>, schema: Arc<Schema>) -> Sink {
        let ric_dir = path.parent().unwrap().parent().unwrap();
        let mut d = DayDirectories::new(ric_dir.into()).unwrap();
        Sink::new_with(path.into(), schema, &mut d, policy).unwrap()
    }

    fn write(path: &Path, b: &RecordBatch, policy: Option<Arc<Replace>>) {
        let mut s = sink(path, policy, b.schema());
        s.write(b).unwrap();
        s.finish().unwrap();
    }

    #[test]
    fn differing_existing_day_is_backed_up_and_replaced() {
        let t = tempfile::tempdir().unwrap();
        let root = t.path().join("delivery");
        let path = root.join("review/2026/X/20261107/kline.parquet");
        write(&path, &day(601, 1), None);
        let original = fs::read(&path).unwrap();
        // Without a policy an existing file is kept (row-count check fails).
        let mut keep = sink(&path, None, kline_schema());
        keep.write(&day(1440, 1)).unwrap();
        assert!(keep.finish().is_err());
        let log = t.path().join("replaced.jsonl");
        let policy = Replace::new(root.clone(), t.path().join("backup"), 0, &log).unwrap();
        // Identical values: untouched, nothing logged.
        write(&path, &day(601, 1), Some(policy.clone()));
        assert_eq!(fs::read(&path).unwrap(), original);
        assert_eq!(fs::read_to_string(&log).unwrap(), "");
        // Metadata alone (e.g. the unified key set) never triggers a replacement.
        let info = KlineInfo {
            input_signature: "abc".into(),
            correction_pending_count: 3,
            correction_plan_sha256: Some("def".into()),
        };
        let relabelled =
            RecordBatch::try_new(kline_schema_for(&info), day(601, 1).columns().to_vec()).unwrap();
        write(&path, &relabelled, Some(policy.clone()));
        assert_eq!(fs::read(&path).unwrap(), original);
        assert_eq!(fs::read_to_string(&log).unwrap(), "");
        // Tail completion: replaced, old bytes kept in the backup.
        write(&path, &day(1440, 1), Some(policy.clone()));
        assert!(same_values(&read_all(&path).unwrap(), &day(1440, 1)));
        let backup = t.path().join("backup/review/2026/X/20261107/kline.parquet");
        assert_eq!(fs::read(backup).unwrap(), original);
        let line: serde_json::Value =
            serde_json::from_str(fs::read_to_string(&log).unwrap().trim()).unwrap();
        assert_eq!(line["old_rows"], 601);
        assert_eq!(line["new_rows"], 1440);
        assert_eq!(line["old_is_prefix"], true);
        // A value correction with the same row count is also replaced.
        let p2 = Replace::new(root.clone(), t.path().join("backup2"), 0, &log).unwrap();
        write(&path, &day(1440, 2), Some(p2));
        assert!(same_values(&read_all(&path).unwrap(), &day(1440, 2)));
        // Days before compare_from are only footer-checked, never replaced.
        let later = Replace::new(root, t.path().join("backup3"), u64::MAX, &log).unwrap();
        write(&path, &day(1440, 3), Some(later));
        assert!(same_values(&read_all(&path).unwrap(), &day(1440, 2)));
    }
}

#[cfg(test)]
mod status_expiry_tests {
    use super::*;
    use crate::native_kline::session::Decoder;

    const D0: u64 = 1_793_923_200_000_000_000 / DAY; // 2026-11-06

    /// Status rows of one RIC: day 0 trades then expires; later days only
    /// repeat the terminal state.
    fn rows(days: u64, revive_on: Option<u64>) -> Vec<StatusRow> {
        let mut d = Decoder::new(true);
        let mut previous = State::Unknown;
        let mut out = Vec::new();
        let mut push = |at: u64, q: &str, previous: &mut State| {
            let decoded = d.decode(q, *previous);
            let before = *previous;
            *previous = decoded.state;
            out.push(StatusRow {
                at,
                before,
                decoded,
                qualifiers: q.into(),
            });
        };
        push(D0 * DAY + 10, "15[TRD_TYPE]", &mut previous);
        push(D0 * DAY + 20, "4[HALT_REASN]", &mut previous);
        for day in 1..days {
            let q = if revive_on == Some(day) {
                "8[HALT_REASN]"
            } else {
                "15[TRD_TYPE]"
            };
            push((D0 + day) * DAY + 5, q, &mut previous);
        }
        out
    }
    fn written(root: &Path) -> Vec<u64> {
        let mut days: Vec<u64> = fs::read_dir(root)
            .unwrap()
            .filter_map(|e| {
                let p = e.unwrap().path();
                p.join("session_status.parquet")
                    .exists()
                    .then(|| day_of(&p.join("x")).unwrap() / DAY - D0)
            })
            .collect();
        days.sort();
        days
    }

    #[test]
    fn redundant_post_expiry_status_days_are_not_written() {
        let rows = rows(5, None);
        assert_eq!(rows[1].decoded.action, "EXPIRE");
        assert!(rows[2..]
            .iter()
            .all(|r| r.decoded.state == State::Expired && r.decoded.action == "NO_CHANGE"));
        let t = tempfile::tempdir().unwrap();
        let root = t.path().join("cme/2026/X");
        // The kline grid covers days 0-1; day 2 keeps another delivered file.
        fs::create_dir_all(root.join(date((D0 + 2) * DAY).unwrap())).unwrap();
        fs::write(
            root.join(date((D0 + 2) * DAY).unwrap())
                .join("trade.parquet"),
            b"",
        )
        .unwrap();
        let mut w = StatusWriter::new(root.clone(), "X", "X").unwrap();
        w.set_kline_last_day(Some(D0 + 1));
        for r in rows {
            w.push(r).unwrap()
        }
        let (files, _, skipped) = w.finish_with_directories().unwrap();
        // Kline days and the day with other files; not days 3 and 4.
        assert_eq!(written(&root), [0, 1, 2]);
        assert_eq!(files.len(), 3);
        assert_eq!(skipped, 2);
        assert_eq!(files[1].rows, 1);
    }

    #[test]
    fn post_expiry_days_are_kept_unless_the_ric_stays_expired() {
        // CME expiry is terminal: a later phase message stays EXPIRED.
        let rows_cme = rows(4, Some(3));
        assert_eq!(rows_cme.last().unwrap().decoded.state, State::Expired);
        // If the RIC is not finally EXPIRED (e.g. a non-terminal venue
        // reopens it), no day is dropped, as in the catalog prune rule.
        let mut rows = rows(4, None);
        let mut reopened = rows.last().unwrap().clone();
        reopened.at += DAY;
        reopened.before = State::Expired;
        reopened.decoded.state = State::Trading;
        reopened.decoded.action = "OPEN";
        rows.push(reopened);
        let t = tempfile::tempdir().unwrap();
        let root = t.path().join("cme/2026/X");
        let mut w = StatusWriter::new(root.clone(), "X", "X").unwrap();
        for r in rows {
            w.push(r).unwrap()
        }
        let (files, _, skipped) = w.finish_with_directories().unwrap();
        assert_eq!((written(&root), skipped), (vec![0, 1, 2, 3, 4], 0));
        assert_eq!(files.len(), 5);
        assert!(files.windows(2).all(|w| w[0].day < w[1].day));
    }

    #[test]
    fn kline_metadata_has_the_production_key_set() {
        let plain = kline_metadata(&KlineInfo {
            input_signature: "sig".into(),
            correction_pending_count: 7,
            correction_plan_sha256: None,
        });
        let mut keys: Vec<_> = plain.keys().map(String::as_str).collect();
        keys.sort();
        assert_eq!(
            keys,
            [
                "correction_pending_count",
                "correction_scope",
                "empty_minutes",
                "exception_policy",
                "fill_policy_version",
                "frequency",
                "input_signature",
                "is_ffill",
                "kline_schema_version",
                "kline_status_policy",
                "lifetime_source",
                "price_scale",
                "source",
                "status",
                "status_scope",
                "status_time_basis",
                "trade_time_basis"
            ]
        );
        assert_eq!(plain["fill_policy_version"], "5");
        assert_eq!(plain["correction_pending_count"], "7");
        assert_eq!(plain["input_signature"], "sig");
        assert!(plain["exception_policy"].contains("without revision netting"));
        let netted = kline_metadata(&KlineInfo {
            input_signature: "sig".into(),
            correction_pending_count: 2,
            correction_plan_sha256: Some("abc".into()),
        });
        assert_eq!(netted.len(), plain.len() + 4);
        assert_eq!(netted["correction_policy_version"], "1");
        assert_eq!(netted["correction_plan_sha256"], "abc");
        assert!(netted["exception_policy"].contains("reliably linked"));
        // Columns never depend on metadata.
        assert_eq!(
            kline_schema_for(&KlineInfo::default()).fields(),
            kline_schema().fields()
        );
    }
}
