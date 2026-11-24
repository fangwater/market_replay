use anyhow::{bail, Context, Result};
use arrow_array::{
    Array, ArrayRef, Int64Array, RecordBatch, StringArray, UInt32Array, UInt64Array, UInt8Array,
};
use arrow_schema::{DataType, Field, Schema};
use clap::{Parser, ValueEnum};
use cme_tas_replay::{
    decode_cme_quote, decode_cme_trade, decode_ric, encode_cme_quote, encode_cme_trade, SlimQuote,
    SlimTrade, KIND_CME_TRADE, MISSING_PRICE as PRICE_MISSING,
};
use lseg_delivery::tasdb::{self, component, Tuning};
use parquet::{
    arrow::{arrow_reader::ParquetRecordBatchReaderBuilder, ArrowWriter},
    basic::{Compression, ZstdLevel},
    file::{metadata::KeyValue, properties::WriterProperties},
};
use rayon::prelude::*;
use rocksdb::{Options, ReadOptions, DB};
use serde::{Deserialize, Serialize};
use sha2::{Digest, Sha256};
use std::{
    collections::{BTreeMap, HashMap},
    fs::{self, File, OpenOptions},
    path::{Path, PathBuf},
    sync::{
        atomic::{AtomicU64, Ordering},
        Arc,
    },
    time::Instant,
};

const DB_PATH: &str = "/mnt/nvme-raid0-28t/fanghaizhou/lseg_data/cme_tas_rocksdb_all_products";

#[derive(Clone, Copy, Debug, PartialEq, Eq, ValueEnum)]
enum Layout {
    Legacy,
    RicDay,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq, ValueEnum)]
enum Readback {
    Full,
    Sampled,
}

#[derive(Parser)]
struct Args {
    #[arg(long, default_value = DB_PATH)]
    db: PathBuf,
    #[arg(long)]
    year: u16,
    /// Source period start; defaults to January 1 of --year.
    #[arg(long)]
    period_start: Option<String>,
    #[arg(long)]
    period_end: String,
    #[arg(long)]
    output: PathBuf,
    /// Final daily layout: lseg/<exchange>/<year>/<RIC>/<UTC YYYYMMDD>/<kind>.parquet.
    #[arg(long, value_enum, default_value_t = Layout::Legacy)]
    layout: Layout,
    /// JSON object mapping each raw RIC to its classified exchange directory.
    #[arg(long)]
    exchange_map: Option<PathBuf>,
    #[arg(skip)]
    exchanges: HashMap<String, String>,
    /// Full value readback, or three files per kind/CF covering first/middle/last.
    #[arg(long, value_enum, default_value_t = Readback::Sampled)]
    readback: Readback,
    #[arg(long, default_value_t = 2)]
    workers: usize,
    #[arg(long, default_value_t = 131072)]
    batch_rows: usize,
    #[arg(long, default_value_t = 4194304)]
    file_rows: usize,
    #[arg(long, value_delimiter = ',')]
    products: Vec<String>,
    /// Bounded validation output only; never a complete export.
    #[arg(long)]
    sample_rows_per_kind: Option<u64>,
    #[arg(long)]
    verify: bool,
}

#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
struct Row {
    ric: String,
    ts_utc_ns: u64,
    exch_hms_ns: u64,
    price: i64,
    volume: u32,
    bid: i64,
    bid_size: u32,
    ask: i64,
    ask_size: u32,
    aggressor: u8,
}
/// Strict decode through the shared record layout (cme_tas_replay); trades
/// must also carry price and volume.
fn decode(b: &[u8], kind: u8) -> Result<Row> {
    if kind == KIND_CME_TRADE {
        let t = decode_cme_trade(b)?;
        if t.price == PRICE_MISSING || t.volume == u32::MAX {
            bail!("invalid trade fields");
        }
        Ok(Row {
            ric: t.ric,
            ts_utc_ns: t.ts_utc_ns,
            exch_hms_ns: t.exch_hms_ns,
            price: t.price,
            volume: t.volume,
            bid: t.bid,
            bid_size: t.bid_size,
            ask: t.ask,
            ask_size: t.ask_size,
            aggressor: t.aggressor,
        })
    } else {
        let q = decode_cme_quote(b)?;
        Ok(Row {
            ric: q.ric,
            ts_utc_ns: q.ts_utc_ns,
            exch_hms_ns: q.exch_hms_ns,
            price: PRICE_MISSING,
            volume: u32::MAX,
            bid: q.bid,
            bid_size: q.bid_size,
            ask: q.ask,
            ask_size: q.ask_size,
            aggressor: 0,
        })
    }
}
fn encode(r: &Row, kind: u8) -> Result<Vec<u8>> {
    Ok(if kind == KIND_CME_TRADE {
        encode_cme_trade(&SlimTrade {
            ric: r.ric.clone(),
            ts_utc_ns: r.ts_utc_ns,
            exch_hms_ns: r.exch_hms_ns,
            price: r.price,
            volume: r.volume,
            bid: r.bid,
            bid_size: r.bid_size,
            ask: r.ask,
            ask_size: r.ask_size,
            aggressor: r.aggressor,
        })?
        .to_vec()
    } else {
        encode_cme_quote(&SlimQuote {
            ric: r.ric.clone(),
            ts_utc_ns: r.ts_utc_ns,
            exch_hms_ns: r.exch_hms_ns,
            bid: r.bid,
            bid_size: r.bid_size,
            ask: r.ask,
            ask_size: r.ask_size,
        })?
        .to_vec()
    })
}
fn schema(kind: u8) -> Arc<Schema> {
    let mut fields = vec![
        Field::new("ric", DataType::Utf8, false),
        Field::new("ts_utc_ns", DataType::UInt64, false),
        Field::new("exch_hms_ns", DataType::UInt64, false),
    ];
    if kind == 1 {
        fields.extend([
            Field::new("price", DataType::Int64, false),
            Field::new("volume", DataType::UInt32, false),
        ]);
    }
    fields.extend([
        Field::new("bid", DataType::Int64, false),
        Field::new("bid_size", DataType::UInt32, false),
        Field::new("ask", DataType::Int64, false),
        Field::new("ask_size", DataType::UInt32, false),
    ]);
    if kind == 1 {
        fields.push(Field::new("aggressor", DataType::UInt8, false));
    }
    let metadata = HashMap::from([
        ("price_scale".into(), "1000000000".into()),
        ("missing_price".into(), i64::MIN.to_string()),
        ("missing_quantity".into(), u32::MAX.to_string()),
        ("missing_exch_hms_ns".into(), u64::MAX.to_string()),
        (
            "quote_semantics".into(),
            "existing sparse per-RIC 1s BBO; no additional sampling or filling".into(),
        ),
    ]);
    Arc::new(Schema::new_with_metadata(fields, metadata))
}
fn batch(rows: &[Row], kind: u8) -> Result<RecordBatch> {
    let mut cols: Vec<ArrayRef> = vec![
        Arc::new(StringArray::from_iter_values(
            rows.iter().map(|r| r.ric.as_str()),
        )),
        Arc::new(UInt64Array::from_iter_values(
            rows.iter().map(|r| r.ts_utc_ns),
        )),
        Arc::new(UInt64Array::from_iter_values(
            rows.iter().map(|r| r.exch_hms_ns),
        )),
    ];
    if kind == 1 {
        cols.extend([
            Arc::new(Int64Array::from_iter_values(rows.iter().map(|r| r.price))) as ArrayRef,
            Arc::new(UInt32Array::from_iter_values(rows.iter().map(|r| r.volume))),
        ]);
    }
    cols.extend([
        Arc::new(Int64Array::from_iter_values(rows.iter().map(|r| r.bid))) as ArrayRef,
        Arc::new(UInt32Array::from_iter_values(
            rows.iter().map(|r| r.bid_size),
        )),
        Arc::new(Int64Array::from_iter_values(rows.iter().map(|r| r.ask))),
        Arc::new(UInt32Array::from_iter_values(
            rows.iter().map(|r| r.ask_size),
        )),
    ]);
    if kind == 1 {
        cols.push(Arc::new(UInt8Array::from_iter_values(
            rows.iter().map(|r| r.aggressor),
        )));
    }
    Ok(RecordBatch::try_new(schema(kind), cols)?)
}
fn row_from_batch(b: &RecordBatch, i: usize, kind: u8) -> Row {
    let string = |n| {
        b.column(n)
            .as_any()
            .downcast_ref::<StringArray>()
            .unwrap()
            .value(i)
            .to_string()
    };
    let u64v = |n| {
        b.column(n)
            .as_any()
            .downcast_ref::<UInt64Array>()
            .unwrap()
            .value(i)
    };
    let i64v = |n| {
        b.column(n)
            .as_any()
            .downcast_ref::<Int64Array>()
            .unwrap()
            .value(i)
    };
    let u32v = |n| {
        b.column(n)
            .as_any()
            .downcast_ref::<UInt32Array>()
            .unwrap()
            .value(i)
    };
    let n = if kind == 1 { 5 } else { 3 };
    Row {
        ric: string(0),
        ts_utc_ns: u64v(1),
        exch_hms_ns: u64v(2),
        price: if kind == 1 { i64v(3) } else { PRICE_MISSING },
        volume: if kind == 1 { u32v(4) } else { u32::MAX },
        bid: i64v(n),
        bid_size: u32v(n + 1),
        ask: i64v(n + 2),
        ask_size: u32v(n + 3),
        aggressor: if kind == 1 {
            b.column(9)
                .as_any()
                .downcast_ref::<UInt8Array>()
                .unwrap()
                .value(i)
        } else {
            0
        },
    }
}
#[derive(Serialize, Deserialize)]
struct Part {
    path: PathBuf,
    ric: String,
    kind: u8,
    rows: u64,
    bytes: u64,
    value_sha256: String,
    first: Row,
    last: Row,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    utc_date: Option<String>,
}

fn utc_date(ts_utc_ns: u64) -> Result<String> {
    let seconds = i64::try_from(ts_utc_ns / 1_000_000_000)?;
    let date = chrono::DateTime::from_timestamp(seconds, 0).context("invalid UTC timestamp")?;
    Ok(date.format("%Y%m%d").to_string())
}
#[derive(Serialize, Deserialize)]
struct CfReport {
    cf: String,
    trade_rows: u64,
    quote_rows: u64,
    parts: Vec<Part>,
    verified: bool,
    #[serde(default)]
    validation_mode: String,
    #[serde(default)]
    full_readback_paths: Vec<PathBuf>,
}

fn sampled_parts(parts: &[Part]) -> Vec<usize> {
    let mut selected = Vec::new();
    for kind in [1, 3] {
        let mut indices: Vec<usize> = (0..parts.len())
            .filter(|i| parts[*i].kind == kind)
            .collect();
        indices.sort_by_key(|i| (&parts[*i].utc_date, &parts[*i].ric, &parts[*i].path));
        if !indices.is_empty() {
            for offset in [0, indices.len() / 2, indices.len() - 1] {
                let index = indices[offset];
                if !selected.contains(&index) {
                    selected.push(index);
                }
            }
        }
    }
    selected
}

struct Sink<'a> {
    args: &'a Args,
    kind: u8,
    ric: String,
    rows: Vec<Row>,
    writer: Option<ArrowWriter<File>>,
    path: PathBuf,
    first: Option<Row>,
    last: Option<Row>,
    count: u64,
    index: usize,
    hash: Sha256,
    utc_date: Option<String>,
}
impl<'a> Sink<'a> {
    fn new(args: &'a Args, kind: u8, ric: String, utc_date: Option<String>) -> Self {
        Self {
            args,
            kind,
            ric,
            rows: if args.layout == Layout::Legacy {
                Vec::with_capacity(args.batch_rows)
            } else {
                Vec::new()
            },
            writer: None,
            path: PathBuf::new(),
            first: None,
            last: None,
            count: 0,
            index: 0,
            hash: Sha256::new(),
            utc_date,
        }
    }
    fn add(&mut self, row: Row, source: &[u8]) -> Result<()> {
        if self.writer.is_none() {
            let kind = if self.kind == 1 { "trade" } else { "quote" };
            let dir = if self.args.layout == Layout::RicDay {
                let exchange = self
                    .args
                    .exchanges
                    .get(&self.ric)
                    .context("RIC absent from exchange map")?;
                self.args
                    .output
                    .join("lseg")
                    .join(exchange)
                    .join(self.args.year.to_string())
                    .join(component(&self.ric))
                    .join(self.utc_date.as_ref().context("missing UTC day")?)
            } else {
                self.args
                    .output
                    .join(kind)
                    .join(format!("ric={}", component(&self.ric)))
            };
            fs::create_dir_all(&dir)?;
            self.path = dir.join(if self.args.layout == Layout::RicDay {
                format!("{kind}.parquet")
            } else {
                format!("part-{:05}.parquet", self.index)
            });
            if self.path.exists() {
                bail!("output already exists: {}", self.path.display());
            }
            let file = OpenOptions::new()
                .write(true)
                .create_new(true)
                .open(self.path.with_extension("parquet.partial"))?;
            let props = WriterProperties::builder()
                .set_compression(Compression::ZSTD(ZstdLevel::try_new(3)?))
                .set_max_row_group_size(self.args.batch_rows)
                .set_key_value_metadata(Some(vec![KeyValue::new(
                    "source_kind".into(),
                    Some(
                        if self.kind == 1 {
                            "lseg_trade"
                        } else {
                            "lseg_quote"
                        }
                        .into(),
                    ),
                )]))
                .build();
            self.writer = Some(ArrowWriter::try_new(file, schema(self.kind), Some(props))?);
            self.first = Some(row.clone());
        }
        self.hash.update(source);
        self.last = Some(row.clone());
        self.count += 1;
        self.rows.push(row);
        if self.rows.len() >= self.args.batch_rows {
            self.flush()?;
        }
        Ok(())
    }
    fn flush(&mut self) -> Result<()> {
        if !self.rows.is_empty() {
            self.writer
                .as_mut()
                .unwrap()
                .write(&batch(&self.rows, self.kind)?)?;
            if self.args.layout == Layout::RicDay {
                self.rows = Vec::new();
            } else {
                self.rows.clear();
            }
        }
        Ok(())
    }
    fn close(&mut self) -> Result<Option<Part>> {
        if self.writer.is_none() {
            return Ok(None);
        }
        self.flush()?;
        self.writer.take().unwrap().close()?;
        let partial = self.path.with_extension("parquet.partial");
        File::open(&partial)?.sync_all()?;
        fs::rename(partial, &self.path)?;
        let part = Part {
            path: self.path.strip_prefix(&self.args.output)?.to_path_buf(),
            ric: self.ric.clone(),
            kind: self.kind,
            rows: self.count,
            bytes: fs::metadata(&self.path)?.len(),
            value_sha256: format!("{:x}", self.hash.clone().finalize()),
            first: self.first.take().unwrap(),
            last: self.last.take().unwrap(),
            utc_date: self.utc_date.clone(),
        };
        self.count = 0;
        self.last = None;
        self.hash = Sha256::new();
        self.index += 1;
        Ok(Some(part))
    }
}
fn verify_part(root: &Path, part: &Part) -> Result<()> {
    let path = root.join(&part.path);
    if fs::metadata(&path)?.len() != part.bytes {
        bail!("file size mismatch: {}", path.display());
    }
    let reader = ParquetRecordBatchReaderBuilder::try_new(File::open(path)?)?;
    if reader.schema().fields() != schema(part.kind).fields() {
        bail!("Parquet schema mismatch");
    }
    let mut count = 0;
    let mut hash = Sha256::new();
    let mut first = None;
    let mut last = None;
    let expected_day = part
        .utc_date
        .as_ref()
        .map(|date| -> Result<u64> {
            let date = chrono::NaiveDate::parse_from_str(date, "%Y%m%d")?;
            Ok(u64::try_from(date.and_hms_opt(0, 0, 0).unwrap().and_utc().timestamp())? / 86400)
        })
        .transpose()?;
    for batch in reader.with_batch_size(65536).build()? {
        let batch = batch?;
        if batch.columns().iter().any(|a| a.null_count() != 0) {
            bail!("unexpected null in raw-value export");
        }
        for i in 0..batch.num_rows() {
            let row = row_from_batch(&batch, i, part.kind);
            if row.ric != part.ric {
                bail!("RIC mismatch");
            }
            if let Some(day) = expected_day {
                if row.ts_utc_ns / 86_400_000_000_000 != day {
                    bail!("UTC date mismatch");
                }
            }
            hash.update(encode(&row, part.kind)?);
            if first.is_none() {
                first = Some(row.clone());
            }
            last = Some(row);
            count += 1;
        }
    }
    if count != part.rows
        || format!("{:x}", hash.finalize()) != part.value_sha256
        || first.as_ref() != Some(&part.first)
        || last.as_ref() != Some(&part.last)
    {
        bail!("readback mismatch: {}", part.path.display());
    }
    Ok(())
}
fn export_cf(db: &DB, name: &str, a: &Args, progress: &AtomicU64) -> Result<CfReport> {
    let report_path = a
        .output
        .join("_audit")
        .join(format!("{}.json", component(name)));
    if report_path.exists() {
        bail!("CF already exported; use --verify on existing output: {name}");
    }
    let started = Instant::now();
    let cf = db.cf_handle(name).context("missing CF")?;
    let mut report = CfReport {
        cf: name.into(),
        trade_rows: 0,
        quote_rows: 0,
        parts: vec![],
        verified: false,
        validation_mode: if a.readback == Readback::Full {
            "full"
        } else {
            "sampled"
        }
        .into(),
        full_readback_paths: vec![],
    };
    for kind in [1, 3] {
        let mut read = ReadOptions::default();
        read.fill_cache(false);
        read.set_readahead_size(4 * 1024 * 1024);
        let mut iter = db.raw_iterator_cf_opt(&cf, read);
        iter.seek([kind]);
        let mut sink: Option<Sink> = None;
        let mut daily_sinks: BTreeMap<u64, Sink> = BTreeMap::new();
        let mut daily_ric: Option<String> = None;
        let mut previous_day: Option<u64> = None;
        let mut seen = 0;
        let mut previous: Option<Vec<u8>> = None;
        while iter.valid() {
            let key = iter.key().context("iterator key")?;
            if key[0] != kind {
                break;
            }
            if key.len() != 31 {
                bail!("invalid key length");
            }
            if previous.as_ref().is_some_and(|k| k.as_slice() >= key) {
                bail!("unordered TAS key");
            }
            let value = iter.value().context("iterator value")?;
            let row = decode(value, kind)?;
            if row.ric != decode_ric(&key[1..17])? {
                bail!("key/value RIC mismatch");
            }
            if kind == 1 && row.ts_utc_ns != u64::from_be_bytes(key[17..25].try_into()?) {
                bail!("trade key timestamp mismatch");
            }
            if kind == 3
                && (u64::from_be_bytes(key[17..25].try_into()?) % 1_000_000_000 != 0
                    || key[25..].iter().any(|v| *v != 0))
            {
                bail!("invalid Quote second key");
            }
            if a.layout == Layout::RicDay {
                let day = row.ts_utc_ns / 86_400_000_000_000;
                if daily_ric.as_ref().is_some_and(|r| r != &row.ric) {
                    for (_, mut s) in std::mem::take(&mut daily_sinks) {
                        if let Some(p) = s.close()? {
                            if a.readback == Readback::Full {
                                verify_part(&a.output, &p)?;
                            }
                            report.parts.push(p);
                        }
                    }
                    previous_day = None;
                }
                daily_ric = Some(row.ric.clone());
                if previous_day.is_some_and(|d| d != day) {
                    daily_sinks
                        .get_mut(&previous_day.unwrap())
                        .unwrap()
                        .flush()?;
                }
                if let std::collections::btree_map::Entry::Vacant(e) = daily_sinks.entry(day) {
                    let date = utc_date(row.ts_utc_ns)?;
                    if !date.starts_with(&a.year.to_string()) {
                        bail!("UTC day outside requested year");
                    }
                    e.insert(Sink::new(a, kind, row.ric.clone(), Some(date)));
                }
                daily_sinks.get_mut(&day).unwrap().add(row, value)?;
                previous_day = Some(day);
            } else {
                if sink.as_ref().is_some_and(|s| s.ric != row.ric) {
                    if let Some(p) = sink.as_mut().unwrap().close()? {
                        if a.readback == Readback::Full {
                            verify_part(&a.output, &p)?;
                        }
                        report.parts.push(p);
                    }
                    sink = None;
                }
                if sink.is_none() {
                    sink = Some(Sink::new(a, kind, row.ric.clone(), None));
                }
                let s = sink.as_mut().unwrap();
                s.add(row, value)?;
                if s.count >= a.file_rows as u64 {
                    if let Some(p) = s.close()? {
                        if a.readback == Readback::Full {
                            verify_part(&a.output, &p)?;
                        }
                        report.parts.push(p);
                    }
                }
            }
            previous = Some(key.to_vec());
            seen += 1;
            if seen % 1_000_000 == 0 {
                progress.fetch_add(1_000_000, Ordering::Relaxed);
                eprintln!(
                    "progress cf={name} kind={kind} rows={seen} total_millions={} seconds={:.1}",
                    progress.load(Ordering::Relaxed) / 1_000_000,
                    started.elapsed().as_secs_f64()
                );
            }
            if a.sample_rows_per_kind.is_some_and(|limit| seen >= limit) {
                break;
            }
            iter.next();
        }
        iter.status()?;
        if let Some(s) = sink.as_mut() {
            if let Some(p) = s.close()? {
                if a.readback == Readback::Full {
                    verify_part(&a.output, &p)?;
                }
                report.parts.push(p);
            }
        }
        for (_, mut s) in daily_sinks {
            if let Some(p) = s.close()? {
                if a.readback == Readback::Full {
                    verify_part(&a.output, &p)?;
                }
                report.parts.push(p);
            }
        }
        if kind == 1 {
            report.trade_rows = seen;
        } else {
            report.quote_rows = seen;
        }
    }
    if a.readback == Readback::Sampled {
        for index in sampled_parts(&report.parts) {
            verify_part(&a.output, &report.parts[index])?;
            report
                .full_readback_paths
                .push(report.parts[index].path.clone());
        }
    } else {
        report.full_readback_paths = report.parts.iter().map(|p| p.path.clone()).collect();
    }
    report.verified = true;
    tasdb::save_json(&report_path, &report)?;
    eprintln!(
        "complete cf={name} trade={} quote={} files={} seconds={:.1}",
        report.trade_rows,
        report.quote_rows,
        report.parts.len(),
        started.elapsed().as_secs_f64()
    );
    Ok(report)
}
fn main() -> Result<()> {
    let mut a = Args::parse();
    if a.layout == Layout::RicDay {
        let path = a
            .exchange_map
            .as_ref()
            .context("--exchange-map required for --layout ric-day")?;
        a.exchanges = serde_json::from_slice(&fs::read(path)?)?;
        for exchange in a.exchanges.values() {
            if exchange.is_empty()
                || !exchange
                    .bytes()
                    .all(|b| b.is_ascii_lowercase() || b.is_ascii_digit() || b == b'_')
            {
                bail!("exchange directories must be lowercase ASCII names");
            }
        }
    }
    let period_start = a
        .period_start
        .clone()
        .unwrap_or_else(|| format!("{}-01-01", a.year));
    let start_date = chrono::NaiveDate::parse_from_str(&period_start, "%Y-%m-%d")
        .context("invalid --period-start")?;
    let end_date = chrono::NaiveDate::parse_from_str(&a.period_end, "%Y-%m-%d")
        .context("invalid --period-end")?;
    if !period_start.starts_with(&format!("{}-", a.year)) || start_date >= end_date {
        bail!("source period start must belong to --year and precede its end");
    }
    if a.workers == 0
        || a.workers > lseg_delivery::limits::TAS_EXPORT_WORKERS
        || a.batch_rows == 0
        || a.file_rows < a.batch_rows
        || a.sample_rows_per_kind == Some(0)
    {
        bail!("invalid limits");
    }
    let started = Instant::now();
    let pool = rayon::ThreadPoolBuilder::new()
        .num_threads(a.workers)
        .build()?;
    if a.verify {
        let audit = a.output.join("_audit");
        let reports: Vec<CfReport> = fs::read_dir(audit)?
            .filter_map(|e| e.ok())
            .filter(|e| e.file_name().to_string_lossy().starts_with("p%3A"))
            .map(|e| Ok(serde_json::from_slice(&fs::read(e.path())?)?))
            .collect::<Result<_>>()?;
        pool.install(|| {
            reports
                .par_iter()
                .try_for_each(|r| r.parts.iter().try_for_each(|p| verify_part(&a.output, p)))
        })?;
        println!("verified {} CF reports", reports.len());
        return Ok(());
    }
    if a.output.exists() {
        bail!("output must not exist: {}", a.output.display());
    }
    let source_current = fs::read_to_string(a.db.join("CURRENT"))?;
    let prefix = format!("p:{}:", a.year);
    let mut names: Vec<String> = DB::list_cf(&Options::default(), &a.db)?
        .into_iter()
        .filter(|n| {
            n.starts_with(&prefix)
                && (a.products.is_empty()
                    || a.products.iter().any(|p| n == &format!("{prefix}{p}")))
        })
        .collect();
    names.sort();
    if names.is_empty() {
        bail!("no matching product CF");
    }
    let mut open_names = vec!["default".to_string(), "replay_meta".to_string()];
    open_names.extend(names.clone());
    eprintln!("opening {} product CFs read-only", names.len());
    let db = tasdb::open_read_only(&a.db, &open_names, Tuning::new(256, 1024))?;
    let meta = db.cf_handle("replay_meta").context("replay_meta")?;
    let period = format!("{}_{}", period_start, a.period_end);
    if db.get_cf(&meta, format!("period:{period}"))?.as_deref() != Some(b"done") {
        bail!("source period is not done: {period}");
    }
    fs::create_dir_all(a.output.join("_audit"))?;
    tasdb::save_json(
        &a.output.join("_audit/run.json"),
        &serde_json::json!({"status":"running","source":a.db,"source_current":source_current,"source_period":period,"products":names,"sample":a.sample_rows_per_kind.is_some(),"row_order":"source RocksDB key order; no deduplication","price_scale":1000000000u64,"missing_values":"original sentinels retained","schema_trade":schema(1).fields().iter().map(|f|f.name()).collect::<Vec<_>>(),"schema_quote":schema(3).fields().iter().map(|f|f.name()).collect::<Vec<_>>()}),
    )?;
    let progress = AtomicU64::new(0);
    let reports: Vec<CfReport> = pool.install(|| {
        names
            .par_iter()
            .map(|n| export_cf(&db, n, &a, &progress))
            .collect::<Result<_>>()
    })?;
    let trades: u64 = reports.iter().map(|r| r.trade_rows).sum();
    let quotes: u64 = reports.iter().map(|r| r.quote_rows).sum();
    let files: usize = reports.iter().map(|r| r.parts.len()).sum();
    let checked_files: usize = reports.iter().map(|r| r.full_readback_paths.len()).sum();
    let summary = serde_json::json!({"status":if a.sample_rows_per_kind.is_some(){"sample_complete"}else{"complete"},"layout":if a.layout == Layout::RicDay {"ric_utc_day_v1"} else {"legacy"},"date_basis":"ts_utc_ns UTC calendar date","readback_mode":if a.readback == Readback::Full {"full"} else {"sampled"},"full_readback_files":checked_files,"readback_sample":"per CF and kind: first, middle, last by UTC date / RIC / path; at most three files per kind","source_period":period,"source_current":source_current,"source_current_after":fs::read_to_string(a.db.join("CURRENT"))?,"product_cfs":names.len(),"trade_rows":trades,"quote_rows":quotes,"files":files,"bytes":reports.iter().flat_map(|r|&r.parts).map(|p|p.bytes).sum::<u64>(),"full_parquet_readback":if a.readback == Readback::Full {"all output files"} else {"selected files only; other files receive metadata checks"},"elapsed_seconds":started.elapsed().as_secs_f64()});
    tasdb::save_json(&a.output.join("_SUCCESS.json"), &summary)?;
    println!("{}", serde_json::to_string_pretty(&summary)?);
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    fn row() -> Row {
        Row {
            ric: "ESH26".into(),
            ts_utc_ns: 10,
            exch_hms_ns: u64::MAX,
            price: 1,
            volume: 0,
            bid: i64::MIN,
            bid_size: u32::MAX,
            ask: 2,
            ask_size: 0,
            aggressor: 0,
        }
    }
    #[test]
    fn both_layouts_preserve_sentinels_and_integer_extremes() {
        for kind in [1, 3] {
            let mut r = row();
            if kind == 3 {
                r.price = PRICE_MISSING;
                r.volume = u32::MAX;
            }
            assert_eq!(decode(&encode(&r, kind).unwrap(), kind).unwrap(), r);
            let b = batch(&[r.clone()], kind).unwrap();
            assert_eq!(row_from_batch(&b, 0, kind), r);
        }
    }
    #[test]
    fn quote_merge_keeps_old_complete_side_and_updates_timestamp() {
        let a = row();
        let mut b = a.clone();
        b.ts_utc_ns = 20;
        b.ask = PRICE_MISSING;
        b.ask_size = u32::MAX;
        b.bid = 3;
        b.bid_size = 4;
        let r = decode(
            &cme_tas_replay::product::later_quote_value(
                &encode(&a, 3).unwrap(),
                &encode(&b, 3).unwrap(),
            ),
            3,
        )
        .unwrap();
        assert_eq!(
            (r.bid, r.bid_size, r.ask, r.ask_size, r.ts_utc_ns),
            (3, 4, 2, 0, 20)
        );
    }
    #[test]
    fn filenames_preserve_case_and_escape_distinct_codes() {
        assert_ne!(component("Cc1"), component("CC1"));
        assert_ne!(component("A/B"), component("A%2FB"));
        assert_eq!(component(".NSEI"), ".NSEI");
    }
    #[test]
    fn daily_export_reads_db_and_preserves_duplicates_and_revisited_dates() {
        let folder =
            PathBuf::from("/mnt/nvme-raid0-28t/fanghaizhou/cargo-target/market_replay/validation")
                .join(format!(
                    "ric_day_{}_{}",
                    std::process::id(),
                    std::time::SystemTime::now()
                        .duration_since(std::time::UNIX_EPOCH)
                        .unwrap()
                        .as_nanos()
                ));
        fs::create_dir_all(&folder).unwrap();
        let mut options = Options::default();
        options.create_if_missing(true);
        options.create_missing_column_families(true);
        let db = DB::open_cf(&options, folder.join("db"), ["p:2026:ES"]).unwrap();
        let cf = db.cf_handle("p:2026:ES").unwrap();
        let day1 = chrono::NaiveDate::from_ymd_opt(2026, 6, 1)
            .unwrap()
            .and_hms_opt(0, 0, 0)
            .unwrap()
            .and_utc()
            .timestamp() as u64
            * 1_000_000_000;
        let day2 = day1 + 86_400_000_000_000;
        let put = |kind: u8, ric: &str, timestamp: u64, key_time: u64, sequence: u32| {
            let mut row = row();
            row.ric = ric.into();
            row.ts_utc_ns = timestamp;
            let mut key = vec![kind];
            let mut slot = [0; 16];
            slot[..ric.len()].copy_from_slice(ric.as_bytes());
            key.extend_from_slice(&slot);
            key.extend_from_slice(&key_time.to_be_bytes());
            key.extend_from_slice(&0u16.to_be_bytes());
            key.extend_from_slice(&sequence.to_be_bytes());
            db.put_cf(&cf, key, encode(&row, kind).unwrap()).unwrap();
        };
        for sequence in 0..4 {
            put(1, "ESH26", day1 + 1, day1 + 1, sequence);
        }
        put(1, "ESH26", day2 + 1, day2 + 1, 0);
        put(1, "Cc1", day1 + 1, day1 + 1, 0);
        for (i, value_time) in [day2 + 1, day1 + 1, day2 + 2].into_iter().enumerate() {
            put(3, "ESH26", value_time, day1 + (i as u64) * 1_000_000_000, 0);
        }
        let output = folder.join("output");
        fs::create_dir_all(output.join("_audit")).unwrap();
        let mut args = Args::parse_from([
            "export",
            "--year",
            "2026",
            "--period-end",
            "2026-06-03",
            "--output",
            output.to_str().unwrap(),
            "--layout",
            "ric-day",
            "--batch-rows",
            "1",
            "--file-rows",
            "2",
        ]);
        args.exchanges =
            HashMap::from([("ESH26".into(), "cme".into()), ("Cc1".into(), "cme".into())]);
        let report = export_cf(&db, "p:2026:ES", &args, &AtomicU64::new(0)).unwrap();
        assert_eq!(report.validation_mode, "sampled");
        assert_eq!(report.full_readback_paths.len(), 5);
        assert_eq!(
            (report.trade_rows, report.quote_rows, report.parts.len()),
            (6, 3, 5)
        );
        let trade = report
            .parts
            .iter()
            .find(|p| p.path == Path::new("lseg/cme/2026/ESH26/20260601/trade.parquet"))
            .unwrap();
        assert_eq!(trade.rows, 4); // Same-day duplicates survive beyond the legacy file limit.
        let quote = report
            .parts
            .iter()
            .find(|p| p.kind == 3 && p.utc_date.as_deref() == Some("20260602"))
            .unwrap();
        assert_eq!(quote.rows, 2); // A later key can revisit an earlier/later UTC day.
        assert!(output
            .join("lseg/cme/2026/Cc1/20260601/trade.parquet")
            .exists());
        for part in &report.parts {
            verify_part(&output, part).unwrap();
        }
        drop(db);
        fs::remove_dir_all(folder).unwrap();
    }
}
