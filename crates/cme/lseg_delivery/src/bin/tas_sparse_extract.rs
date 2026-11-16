//! One-pass, read-only per-CF minute aggregates and raw revision/status ranges.
//! No venue, session, calendar or price filter. Raw databases are immutable.
use anyhow::{ensure, Context, Result};
use arrow_array::{ArrayRef, Int64Array, RecordBatch, StringArray, UInt64Array};
use arrow_schema::{DataType, Field, Schema};
use clap::Parser;
use cme_tas_replay::{decode_cme_correction, decode_cme_status, decode_cme_trade};
use lseg_delivery::tasdb::{self, event_time, Tuning};
use parquet::{
    arrow::ArrowWriter,
    basic::{Compression, ZstdLevel},
    file::properties::WriterProperties,
};
use rocksdb::ReadOptions;
use serde::{Deserialize, Serialize};
use serde_json::{json, Value};
use std::{
    collections::BTreeMap,
    fs::{self, File},
    io::{BufWriter, Write},
    path::PathBuf,
    sync::Arc,
};

const MINUTE: u64 = 60_000_000_000;
#[derive(Parser)]
struct Args {
    #[arg(long)]
    plan: PathBuf,
    #[arg(long)]
    job: String,
    #[arg(long)]
    output: PathBuf,
}
#[derive(Clone, Debug, Serialize, Deserialize)]
struct Bar {
    ric: String,
    ts: u64,
    open: i64,
    high: i64,
    low: i64,
    close: i64,
    volume: u64,
    count: u64,
    first: (u64, u64, u64),
    last: (u64, u64, u64),
    missing: u64,
    late: u64,
}

impl Bar {
    fn add(
        &mut self,
        price: i64,
        volume: u32,
        event: u64,
        published: u64,
        order: u64,
        missing: bool,
    ) {
        let clock = (event, published, order);
        if clock < self.first {
            self.first = clock;
            self.open = price;
        }
        if clock > self.last {
            self.last = clock;
            self.close = price;
        }
        self.high = self.high.max(price);
        self.low = self.low.min(price);
        self.volume += u64::from(volume);
        self.count += 1;
        self.missing += u64::from(missing);
        self.late += u64::from(published / MINUTE > event / MINUTE);
    }
}
fn schema() -> Arc<Schema> {
    let mut fields = vec![
        Field::new("ric", DataType::Utf8, false),
        Field::new("ts_utc_ns", DataType::UInt64, false),
    ];
    for name in ["open", "high", "low", "close"] {
        fields.push(Field::new(name, DataType::Int64, false));
    }
    for name in [
        "volume",
        "trade_count",
        "first_event_ns",
        "first_published_ns",
        "first_order",
        "last_event_ns",
        "last_published_ns",
        "last_order",
        "missing_exchange_time_trade_count",
        "late_trade_count",
    ] {
        fields.push(Field::new(name, DataType::UInt64, false));
    }
    Arc::new(Schema::new(fields))
}
fn write_bars(writer: &mut ArrowWriter<File>, bars: &BTreeMap<u64, Bar>) -> Result<()> {
    if bars.is_empty() {
        return Ok(());
    }
    let rows: Vec<_> = bars.values().collect();
    for rows in rows.chunks(8192) {
        let mut cols: Vec<ArrayRef> = vec![
            Arc::new(StringArray::from(
                rows.iter().map(|r| r.ric.as_str()).collect::<Vec<_>>(),
            )),
            Arc::new(UInt64Array::from(
                rows.iter().map(|r| r.ts).collect::<Vec<_>>(),
            )),
        ];
        for f in [
            |r: &Bar| r.open,
            |r: &Bar| r.high,
            |r: &Bar| r.low,
            |r: &Bar| r.close,
        ] {
            cols.push(Arc::new(Int64Array::from(
                rows.iter().map(|r| f(r)).collect::<Vec<_>>(),
            )));
        }
        for f in [
            |r: &Bar| r.volume,
            |r: &Bar| r.count,
            |r: &Bar| r.first.0,
            |r: &Bar| r.first.1,
            |r: &Bar| r.first.2,
            |r: &Bar| r.last.0,
            |r: &Bar| r.last.1,
            |r: &Bar| r.last.2,
            |r: &Bar| r.missing,
            |r: &Bar| r.late,
        ] {
            cols.push(Arc::new(UInt64Array::from(
                rows.iter().map(|r| f(r)).collect::<Vec<_>>(),
            )));
        }
        writer.write(&RecordBatch::try_new(schema(), cols)?)?;
    }
    Ok(())
}

/// Preserve raw values, duplicates and order with constant serialization memory.
struct JsonArray<W: Write> {
    writer: W,
    count: u64,
}
impl<W: Write> JsonArray<W> {
    fn new(mut writer: W) -> Result<Self> {
        writer.write_all(b"[")?;
        Ok(Self { writer, count: 0 })
    }
    fn push<T: Serialize>(&mut self, value: &T) -> Result<()> {
        if self.count > 0 {
            self.writer.write_all(b",")?;
        }
        serde_json::to_writer(&mut self.writer, value)?;
        self.count += 1;
        Ok(())
    }
    fn finish(mut self) -> Result<(W, u64)> {
        self.writer.write_all(b"]\n")?;
        self.writer.flush()?;
        Ok((self.writer, self.count))
    }
}

fn main() -> Result<()> {
    let args = Args::parse();
    let plan: Value = serde_json::from_slice(&fs::read(&args.plan)?)?;
    let job = plan["jobs"]
        .as_array()
        .context("jobs")?
        .iter()
        .find(|j| j["id"].as_str() == Some(&args.job))
        .context("job")?;
    fs::create_dir_all(&args.output)?;
    let path = PathBuf::from(job["db"].as_str().context("db")?);
    let current = fs::read(path.join("CURRENT"))?;
    let target = args.output.join("minutes.parquet");
    let checkpoint = args.output.join("state.json");
    let identity =
        json!({"job":job,"current":String::from_utf8(current.clone())?,"extractor_version":1});
    if target.exists() && checkpoint.exists() {
        let prior: Value = serde_json::from_slice(&fs::read(&checkpoint)?)?;
        ensure!(prior["identity"] == identity, "source changed");
        return Ok(());
    }
    let name = job["cf"].as_str().context("cf")?;
    let db = tasdb::open_read_only(&path, ["default", name], Tuning::default())?;
    let cf = db.cf_handle(name).context("CF")?;
    let tmp = target.with_extension("parquet.partial");
    let props = WriterProperties::builder()
        .set_compression(Compression::ZSTD(ZstdLevel::try_new(3)?))
        .build();
    let mut writer = ArrowWriter::try_new(File::create(&tmp)?, schema(), Some(props))?;
    let mut ro = ReadOptions::default();
    ro.fill_cache(false);
    ro.set_iterate_upper_bound(vec![2]);
    let mut it = db.raw_iterator_cf_opt(cf, ro);
    it.seek([1]);
    let mut bars = BTreeMap::new();
    let mut current_ric = String::new();
    let mut total = 0u64;
    let mut volume = 0u64;
    let mut minutes = 0u64;
    let mut rics = Vec::new();
    while let Some(key) = it.key() {
        if key[0] != 1 {
            break;
        }
        ensure!(key.len() == 31, "trade key length");
        let row = decode_cme_trade(it.value().context("trade value")?)?;
        ensure!(
            row.price != i64::MIN && row.volume != u32::MAX,
            "missing trade price/volume"
        );
        if row.ric != current_ric {
            if !current_ric.is_empty() {
                minutes += bars.len() as u64;
                write_bars(&mut writer, &bars)?;
                bars.clear();
            }
            current_ric = row.ric.clone();
            rics.push(current_ric.clone());
        }
        let event = event_time(row.ts_utc_ns, row.exch_hms_ns)?;
        let minute = event / MINUTE * MINUTE;
        let order = (u16::from_be_bytes(key[25..27].try_into()?) as u64) << 32
            | u32::from_be_bytes(key[27..31].try_into()?) as u64;
        let bar = bars.entry(minute).or_insert_with(|| Bar {
            ric: row.ric,
            ts: minute,
            open: row.price,
            high: row.price,
            low: row.price,
            close: row.price,
            volume: 0,
            count: 0,
            first: (event, row.ts_utc_ns, order),
            last: (event, row.ts_utc_ns, order),
            missing: 0,
            late: 0,
        });
        bar.add(
            row.price,
            row.volume,
            event,
            row.ts_utc_ns,
            order,
            row.exch_hms_ns == u64::MAX,
        );
        total += 1;
        volume += u64::from(row.volume);
        if total % 10_000_000 == 0 {
            eprintln!("{}: {} trades", args.job, total);
        }
        it.next();
    }
    it.status()?;
    drop(it);
    minutes += bars.len() as u64;
    write_bars(&mut writer, &bars)?;
    writer.close()?;
    File::open(&tmp)?.sync_all()?;
    bars.clear();
    let mut event_counts = BTreeMap::new();
    let mut event_files = Vec::new();
    for kind in [12u8, 13] {
        let file_name = if kind == 13 {
            "status.json"
        } else {
            "corrections.json"
        };
        let destination = args.output.join(file_name);
        let partial = destination.with_extension("json.partial");
        let mut events =
            JsonArray::new(BufWriter::with_capacity(64 * 1024, File::create(&partial)?))?;
        let mut ro = ReadOptions::default();
        ro.fill_cache(false);
        ro.set_iterate_upper_bound(vec![kind + 1]);
        let mut it = db.raw_iterator_cf_opt(cf, ro);
        it.seek([kind]);
        while let Some(key) = it.key() {
            if key[0] != kind {
                break;
            }
            ensure!(key.len() == 31, "event key length");
            let order = (u16::from_be_bytes(key[25..27].try_into()?) as u64) << 32
                | u32::from_be_bytes(key[27..31].try_into()?) as u64;
            if kind == 13 {
                let row = decode_cme_status(it.value().context("status")?)?;
                events.push(&json!({"ric":row.ric,"product":job["product"],"ts_utc_ns":row.ts_utc_ns,"exch_hms_ns":row.exch_hms_ns,"qualifiers":row.qualifiers,"source_order":order}))?;
            } else {
                let row = decode_cme_correction(it.value().context("correction")?)?;
                events.push(&json!({"ric":row.ric,"published_ns":row.ts_utc_ns,"exch_hms_ns":row.exch_hms_ns,"price":row.price,"volume":row.volume,"original_price":row.original_price,"original_volume":row.original_volume,"original_seq":row.original_seq,"qualifiers":row.qualifiers,"source_order":order,"source_cf":name,"source_db":path}))?;
            }
            it.next();
        }
        it.status()?;
        let (buffer, count) = events.finish()?;
        buffer.get_ref().sync_all()?;
        event_counts.insert(kind, count);
        event_files.push((partial, destination));
    }
    drop(db);
    ensure!(
        current == fs::read(path.join("CURRENT"))?,
        "production CURRENT changed"
    );
    for (partial, destination) in event_files {
        fs::rename(partial, destination)?;
    }
    fs::rename(tmp, target)?;
    let state = json!({"identity":identity,"trade_rows":total,"volume":volume,"minute_rows":minutes,"rics":rics,"status_rows":event_counts[&13],"correction_rows":event_counts[&12]});
    let temporary = checkpoint.with_extension("json.partial");
    fs::write(&temporary, serde_json::to_vec_pretty(&state)?)?;
    fs::rename(temporary, checkpoint)?;
    eprintln!("{}: {} trades -> {} minutes", args.job, total, minutes);
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use lseg_delivery::tasdb::DAY_NS as DAY;
    #[test]
    fn streamed_json_preserves_values_duplicates_and_order() {
        let mut writer = JsonArray::new(Vec::new()).unwrap();
        let source: Vec<_> = (0..10_000)
            .map(|i| json!({"ric":"测试\"\\","n":i/2,"nullable":null,"ns":u64::MAX}))
            .collect();
        for row in &source {
            writer.push(row).unwrap();
        }
        let (bytes, count) = writer.finish().unwrap();
        assert_eq!(count, 10_000);
        assert_eq!(
            serde_json::from_slice::<Vec<Value>>(&bytes).unwrap(),
            source
        );
        let (empty, count) = JsonArray::new(Vec::new()).unwrap().finish().unwrap();
        assert_eq!(count, 0);
        assert_eq!(empty, b"[]\n");
    }
    #[test]
    fn midnight_clock() {
        assert_eq!(event_time(DAY + 100, DAY - 100).unwrap(), DAY - 100);
        assert_eq!(event_time(DAY - 100, 100).unwrap(), DAY + 100);
        assert_eq!(event_time(DAY, u64::MAX).unwrap(), DAY);
        assert!(event_time(DAY, DAY).is_err());
    }
    #[test]
    fn out_of_order_event_clock() {
        let mut b = Bar {
            ric: "X".into(),
            ts: 0,
            open: 10,
            high: 10,
            low: 10,
            close: 10,
            volume: 0,
            count: 0,
            first: (5, 9, 0),
            last: (5, 9, 0),
            missing: 0,
            late: 0,
        };
        b.add(10, 2, 5, 9, 0, false);
        b.add(20, 3, 2, 11, 0, false);
        b.add(15, 4, 7, 12, 0, false);
        assert_eq!(
            (b.open, b.high, b.low, b.close, b.volume, b.count),
            (20, 20, 10, 15, 9, 3)
        );
    }
}
