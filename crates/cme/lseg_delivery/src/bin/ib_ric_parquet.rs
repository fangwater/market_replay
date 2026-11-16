//! Read-only IB minute export: one actual RIC per lossless Parquet file.
use anyhow::{ensure, Context, Result};
use arrow_array::{
    Array, ArrayRef, BooleanArray, Float64Array, Int64Array, RecordBatch, StringArray,
    TimestampNanosecondArray, UInt16Array,
};
use arrow_schema::{DataType, Field, Schema, TimeUnit};
use chrono::{Datelike, NaiveDateTime};
use clap::Parser;
use futures_util::TryStreamExt;
use parquet::{
    arrow::{arrow_reader::ParquetRecordBatchReaderBuilder, ArrowWriter},
    basic::{Compression, ZstdLevel},
    file::properties::WriterProperties,
};
use serde::{Deserialize, Serialize};
use sha2::{Digest, Sha256};
use sqlx::{
    mysql::{MySqlConnectOptions, MySqlRow},
    Connection, MySqlConnection, Row,
};
use std::{
    collections::{BTreeMap, HashMap},
    fs::{self, File},
    io::{BufWriter, Write},
    path::{Path, PathBuf},
    sync::{
        atomic::{AtomicBool, AtomicUsize, Ordering},
        Arc,
    },
    time::Instant,
};

#[derive(Parser)]
struct Args {
    #[arg(long)]
    plan: PathBuf,
    #[arg(long)]
    output: PathBuf,
    #[arg(long, default_value = ".env.local")]
    env: PathBuf,
    #[arg(long, default_value_t = 4)]
    workers: usize,
    #[arg(long, default_value_t = 65536)]
    batch_rows: usize,
    #[arg(long)]
    max_jobs: Option<usize>,
}
#[derive(Clone, Serialize, Deserialize)]
struct Job {
    ric: String,
    con_id: String,
    symbol: String,
    exchange: String,
    maturity_date: String,
    local_symbol: String,
    currency: String,
    multiplier: String,
    ib_first_utc: String,
    ib_last_utc: String,
    cutoff_id: i64,
    #[serde(flatten)]
    extra: BTreeMap<String, serde_json::Value>,
}
#[derive(Deserialize)]
struct Plan {
    cutoff_id: i64,
    jobs: Vec<Job>,
}
#[derive(Clone, Serialize)]
struct SourceRow {
    id: i64,
    time_cst: Option<String>,
    symbol: Option<String>,
    maturity_date: Option<String>,
    exchange: Option<String>,
    con_id: String,
    open: Option<f64>,
    high: Option<f64>,
    low: Option<f64>,
    close: Option<f64>,
    volume: Option<f64>,
    utc_time: String,
    timestamp: Option<i64>,
    sec_type: String,
    created_at: Option<String>,
}
#[derive(Serialize, Deserialize)]
struct Audit {
    status: String,
    ric: String,
    con_id: String,
    symbol: String,
    path: PathBuf,
    cutoff_id: i64,
    rows: u64,
    rejected_invalid_utc: u64,
    first_utc: String,
    last_utc: String,
    first_ts_utc_ns: i64,
    last_ts_utc_ns: i64,
    source_value_sha256: String,
    file_sha256: String,
    bytes: u64,
    time_flag_rows: u64,
    ohlcv_flag_rows: u64,
    identity_mismatch_rows: u64,
    zero_volume_rows: u64,
    elapsed_seconds: f64,
}
fn json_write(path: &Path, v: &impl Serialize) -> Result<()> {
    let tmp = path.with_extension("json.partial");
    let mut f = File::create(&tmp)?;
    serde_json::to_writer_pretty(&mut f, v)?;
    f.write_all(b"\n")?;
    f.sync_all()?;
    fs::rename(tmp, path)?;
    Ok(())
}
fn file_hash(path: &Path) -> Result<String> {
    let mut h = Sha256::new();
    std::io::copy(&mut File::open(path)?, &mut h)?;
    Ok(format!("{:x}", h.finalize()))
}
fn time_ns(s: &str) -> Option<i64> {
    let d = NaiveDateTime::parse_from_str(s, "%Y-%m-%d %H:%M:%S%.f").ok()?;
    if d.year() < 1970 {
        return None;
    }
    d.and_utc().timestamp_nanos_opt()
}
fn flags(r: &SourceRow, ts: i64) -> (u16, u16) {
    let mut t = 0;
    if r.timestamp.is_none() {
        t |= 1;
    }
    if r.timestamp
        .is_some_and(|x| Some(x) != ts.checked_div(1_000_000))
    {
        t |= 2;
    }
    match r.time_cst.as_deref().and_then(time_ns) {
        None => t |= 4,
        Some(c) if c - ts != 28_800_000_000_000 => t |= 8,
        _ => {}
    }
    if ts % 60_000_000_000 != 0 {
        t |= 16;
    }
    let prices = [r.open, r.high, r.low, r.close];
    let mut q = 0;
    if prices.iter().any(|x| x.is_none_or(|v| !v.is_finite())) {
        q |= 1;
    }
    if let (Some(o), Some(h), Some(l), Some(c)) = (r.open, r.high, r.low, r.close) {
        if l > h || o < l || o > h || c < l || c > h {
            q |= 2;
        }
    }
    if r.volume.is_none_or(|v| !v.is_finite()) {
        q |= 4;
    }
    if r.volume.is_some_and(|v| v < 0.0) {
        q |= 8;
    }
    (t, q)
}
fn identity(r: &SourceRow, j: &Job) -> bool {
    r.symbol.as_deref() == Some(&j.symbol)
        && r.exchange.as_deref() == Some(&j.exchange)
        && r.maturity_date.as_deref() == Some(&j.maturity_date)
        && r.con_id == j.con_id
        && r.sec_type.eq_ignore_ascii_case("fut")
}
fn schema(j: &Job) -> Arc<Schema> {
    let s = DataType::Utf8;
    let f = DataType::Float64;
    let i = DataType::Int64;
    let utc = DataType::Timestamp(TimeUnit::Nanosecond, Some("UTC".into()));
    let local = DataType::Timestamp(TimeUnit::Nanosecond, None);
    Arc::new(Schema::new_with_metadata(vec![
        Field::new("ric",s.clone(),false),Field::new("ts_utc_ns",i.clone(),false),
        Field::new("open",f.clone(),true),Field::new("high",f.clone(),true),Field::new("low",f.clone(),true),Field::new("close",f.clone(),true),Field::new("volume",f,true),
        Field::new("con_id",s.clone(),false),Field::new("symbol",s.clone(),true),Field::new("exchange",s.clone(),true),Field::new("maturity_date",s.clone(),true),Field::new("id",i.clone(),false),
        Field::new("utc_time",utc.clone(),false),Field::new("time_cst",local,true),Field::new("timestamp",i,true),Field::new("sec_type",s.clone(),false),Field::new("created_at",utc,true),
        Field::new("utc_time_raw",s.clone(),false),Field::new("time_cst_raw",s.clone(),true),Field::new("created_at_raw",s,true),
        Field::new("time_quality_flags",DataType::UInt16,false),Field::new("ohlcv_quality_flags",DataType::UInt16,false),Field::new("identity_matches_metadata",DataType::Boolean,false),
    ],HashMap::from([
        ("source".into(),"ib_data.minute_data".into()),("ib_contract".into(),serde_json::to_string(j).unwrap()),
        ("timestamp_basis".into(),"UTC DATETIME column; original millisecond timestamp retained unchanged".into()),
        ("bar_label_semantics".into(),"original IB labels retained; open/end convention not reinterpreted".into()),
        ("price_and_volume".into(),"source nullable FLOAT64; no scaling, filling, adjustment, or volume filter".into()),
        ("time_quality_flags".into(),"1=missing epoch;2=epoch mismatch;4=invalid CST;8=CST offset mismatch;16=not minute-aligned".into()),
        ("ohlcv_quality_flags".into(),"1=null/nonfinite OHLC;2=OHLC constraints;4=null/nonfinite volume;8=negative volume".into()),
    ])))
}
fn batch(rows: &[SourceRow], j: &Job, s: Arc<Schema>) -> Result<RecordBatch> {
    let times: Vec<i64> = rows.iter().map(|r| time_ns(&r.utc_time).unwrap()).collect();
    let flags: Vec<_> = rows.iter().zip(&times).map(|(r, t)| flags(r, *t)).collect();
    let floats = |get: fn(&SourceRow) -> Option<f64>| -> ArrayRef {
        Arc::new(Float64Array::from_iter(rows.iter().map(get)))
    };
    let strings = |get: fn(&SourceRow) -> Option<&str>| -> ArrayRef {
        Arc::new(StringArray::from_iter(rows.iter().map(get)))
    };
    let cols: Vec<ArrayRef> = vec![
        Arc::new(StringArray::from_iter_values(
            rows.iter().map(|_| j.ric.as_str()),
        )),
        Arc::new(Int64Array::from(times.clone())),
        floats(|r| r.open),
        floats(|r| r.high),
        floats(|r| r.low),
        floats(|r| r.close),
        floats(|r| r.volume),
        strings(|r| Some(&r.con_id)),
        strings(|r| r.symbol.as_deref()),
        strings(|r| r.exchange.as_deref()),
        strings(|r| r.maturity_date.as_deref()),
        Arc::new(Int64Array::from_iter_values(rows.iter().map(|r| r.id))),
        Arc::new(TimestampNanosecondArray::from(times).with_timezone("UTC")),
        Arc::new(TimestampNanosecondArray::from_iter(
            rows.iter().map(|r| r.time_cst.as_deref().and_then(time_ns)),
        )),
        Arc::new(Int64Array::from_iter(rows.iter().map(|r| r.timestamp))),
        strings(|r| Some(&r.sec_type)),
        Arc::new(
            TimestampNanosecondArray::from_iter(
                rows.iter()
                    .map(|r| r.created_at.as_deref().and_then(time_ns)),
            )
            .with_timezone("UTC"),
        ),
        strings(|r| Some(&r.utc_time)),
        strings(|r| r.time_cst.as_deref()),
        strings(|r| r.created_at.as_deref()),
        Arc::new(UInt16Array::from_iter_values(flags.iter().map(|x| x.0))),
        Arc::new(UInt16Array::from_iter_values(flags.iter().map(|x| x.1))),
        Arc::new(BooleanArray::from_iter(
            rows.iter().map(|r| Some(identity(r, j))),
        )),
    ];
    Ok(RecordBatch::try_new(s, cols)?)
}
fn decode(r: MySqlRow) -> Result<SourceRow> {
    Ok(SourceRow {
        id: r.try_get("id")?,
        time_cst: r.try_get("time_cst")?,
        symbol: r.try_get("symbol")?,
        maturity_date: r.try_get("maturity_date")?,
        exchange: r.try_get("exchange")?,
        con_id: r.try_get("con_id")?,
        open: r.try_get("open")?,
        high: r.try_get("high")?,
        low: r.try_get("low")?,
        close: r.try_get("close")?,
        volume: r.try_get("volume")?,
        utc_time: r.try_get("utc_time")?,
        timestamp: r.try_get("timestamp")?,
        sec_type: r.try_get("sec_type")?,
        created_at: r.try_get("created_at")?,
    })
}
fn value_hash(h: &mut Sha256, r: &SourceRow) -> Result<()> {
    h.update(serde_json::to_vec(r)?);
    h.update(b"\n");
    Ok(())
}
fn verify(path: &Path, j: &Job, expected: &str, count: u64) -> Result<()> {
    let reader = ParquetRecordBatchReaderBuilder::try_new(File::open(path)?)?
        .with_batch_size(65536)
        .build()?;
    let mut hash = Sha256::new();
    let mut n = 0;
    let mut previous = None;
    for b in reader {
        let b = b?;
        let st = |c: usize, k: usize| -> Option<String> {
            let a = b.column(c).as_any().downcast_ref::<StringArray>().unwrap();
            if a.is_null(k) {
                None
            } else {
                Some(a.value(k).into())
            }
        };
        let fl = |c: usize, k: usize| -> Option<f64> {
            let a = b.column(c).as_any().downcast_ref::<Float64Array>().unwrap();
            if a.is_null(k) {
                None
            } else {
                Some(a.value(k))
            }
        };
        let ints = |c: usize| b.column(c).as_any().downcast_ref::<Int64Array>().unwrap();
        let tf = b.column(20).as_any().downcast_ref::<UInt16Array>().unwrap();
        let qf = b.column(21).as_any().downcast_ref::<UInt16Array>().unwrap();
        let idf = b
            .column(22)
            .as_any()
            .downcast_ref::<BooleanArray>()
            .unwrap();
        for k in 0..b.num_rows() {
            let r = SourceRow {
                id: ints(11).value(k),
                time_cst: st(18, k),
                symbol: st(8, k),
                maturity_date: st(10, k),
                exchange: st(9, k),
                con_id: st(7, k).unwrap(),
                open: fl(2, k),
                high: fl(3, k),
                low: fl(4, k),
                close: fl(5, k),
                volume: fl(6, k),
                utc_time: st(17, k).unwrap(),
                timestamp: if ints(14).is_null(k) {
                    None
                } else {
                    Some(ints(14).value(k))
                },
                sec_type: st(15, k).unwrap(),
                created_at: st(19, k),
            };
            ensure!(
                st(0, k).as_deref() == Some(&j.ric) && r.con_id == j.con_id,
                "RIC/con_id mismatch"
            );
            let ts = time_ns(&r.utc_time).context("bad output UTC")?;
            ensure!(
                ts == ints(1).value(k) && previous.is_none_or(|p| ts > p),
                "time order/normalization mismatch"
            );
            previous = Some(ts);
            ensure!(
                flags(&r, ts) == (tf.value(k), qf.value(k)) && identity(&r, j) == idf.value(k),
                "quality flags differ"
            );
            for (col, expected) in [
                (12, Some(ts)),
                (13, r.time_cst.as_deref().and_then(time_ns)),
                (16, r.created_at.as_deref().and_then(time_ns)),
            ] {
                let a = b
                    .column(col)
                    .as_any()
                    .downcast_ref::<TimestampNanosecondArray>()
                    .unwrap();
                ensure!(
                    (if a.is_null(k) { None } else { Some(a.value(k)) }) == expected,
                    "typed timestamp differs"
                );
            }
            value_hash(&mut hash, &r)?;
            n += 1;
        }
    }
    ensure!(
        n == count && format!("{:x}", hash.finalize()) == expected,
        "source/readback checksum mismatch"
    );
    Ok(())
}
async fn export(
    conn: &mut MySqlConnection,
    j: &Job,
    out: &Path,
    batch_rows: usize,
) -> Result<Audit> {
    let start = Instant::now();
    let data = out.join("data");
    let audit = out.join("_audit/jobs");
    let path = data.join(format!("{}.parquet", j.ric));
    let report = audit.join(format!("{}.json", j.ric));
    if report.exists() {
        let a: Audit = serde_json::from_reader(File::open(report)?)?;
        ensure!(
            a.status == "passed"
                && a.cutoff_id == j.cutoff_id
                && a.con_id == j.con_id
                && file_hash(&a.path)? == a.file_sha256,
            "resume mismatch"
        );
        return Ok(a);
    }
    ensure!(
        !path.exists(),
        "unreceipted output exists: {}",
        path.display()
    );
    let partial = path.with_extension("parquet.partial");
    let s = schema(j);
    let prop = WriterProperties::builder()
        .set_compression(Compression::ZSTD(ZstdLevel::try_new(3)?))
        .set_max_row_group_size(batch_rows)
        .build();
    let mut writer = ArrowWriter::try_new(File::create(&partial)?, s.clone(), Some(prop))?;
    let mut rejected = BufWriter::new(File::create(
        out.join("_audit/rejected").join(format!("{}.jsonl", j.ric)),
    )?);
    let sql="SELECT t.`id`,CAST(t.`time_cst` AS CHAR) AS `time_cst`,t.`symbol`,t.`maturity_date`,t.`exchange`,t.`con_id`,t.`open`,t.`high`,t.`low`,t.`close`,t.`volume`,CAST(t.`utc_time` AS CHAR) AS `utc_time`,t.`timestamp`,t.`sec_type`,CAST(t.`created_at` AS CHAR) AS `created_at` FROM `minute_data` t FORCE INDEX(uk_conid_utctime) WHERE t.`con_id`=? AND t.`id`<=? ORDER BY t.`utc_time`";
    let mut stream = sqlx::query(sql)
        .bind(&j.con_id)
        .bind(j.cutoff_id)
        .fetch(&mut *conn);
    let mut rows = Vec::with_capacity(batch_rows);
    let mut digest = Sha256::new();
    let mut a = Audit {
        status: "writing".into(),
        ric: j.ric.clone(),
        con_id: j.con_id.clone(),
        symbol: j.symbol.clone(),
        path: path.clone(),
        cutoff_id: j.cutoff_id,
        rows: 0,
        rejected_invalid_utc: 0,
        first_utc: String::new(),
        last_utc: String::new(),
        first_ts_utc_ns: 0,
        last_ts_utc_ns: 0,
        source_value_sha256: String::new(),
        file_sha256: String::new(),
        bytes: 0,
        time_flag_rows: 0,
        ohlcv_flag_rows: 0,
        identity_mismatch_rows: 0,
        zero_volume_rows: 0,
        elapsed_seconds: 0.0,
    };
    let mut prev = None;
    while let Some(row) = stream.try_next().await? {
        let r = decode(row)?;
        ensure!(
            r.con_id == j.con_id && r.id <= j.cutoff_id,
            "source identity/watermark mismatch"
        );
        let Some(ts) = time_ns(&r.utc_time) else {
            serde_json::to_writer(&mut rejected, &r)?;
            rejected.write_all(b"\n")?;
            a.rejected_invalid_utc += 1;
            continue;
        };
        ensure!(
            prev.is_none_or(|p| ts > p),
            "source timestamps not strictly increasing"
        );
        prev = Some(ts);
        if a.rows == 0 {
            a.first_utc = r.utc_time.clone();
            a.first_ts_utc_ns = ts;
        }
        a.last_utc = r.utc_time.clone();
        a.last_ts_utc_ns = ts;
        let (t, q) = flags(&r, ts);
        a.time_flag_rows += u64::from(t != 0);
        a.ohlcv_flag_rows += u64::from(q != 0);
        a.identity_mismatch_rows += u64::from(!identity(&r, j));
        a.zero_volume_rows += u64::from(r.volume == Some(0.0));
        value_hash(&mut digest, &r)?;
        a.rows += 1;
        rows.push(r);
        if rows.len() == batch_rows {
            writer.write(&batch(&rows, j, s.clone())?)?;
            rows.clear();
        }
    }
    if !rows.is_empty() {
        writer.write(&batch(&rows, j, s)?)?;
    }
    writer.close()?;
    File::open(&partial)?.sync_all()?;
    rejected.flush()?;
    ensure!(
        a.rows > 0 && a.first_utc == j.ib_first_utc && a.last_utc == j.ib_last_utc,
        "source endpoints changed from plan for {}",
        j.ric
    );
    a.source_value_sha256 = format!("{:x}", digest.finalize());
    verify(&partial, j, &a.source_value_sha256, a.rows)?;
    fs::rename(&partial, &path)?;
    a.file_sha256 = file_hash(&path)?;
    a.bytes = fs::metadata(&path)?.len();
    a.status = "passed".into();
    a.elapsed_seconds = start.elapsed().as_secs_f64();
    json_write(&report, &a)?;
    eprintln!(
        "finished ric={} con_id={} rows={} rejected={} seconds={:.2}",
        a.ric, a.con_id, a.rows, a.rejected_invalid_utc, a.elapsed_seconds
    );
    Ok(a)
}
fn main() -> Result<()> {
    let a = Args::parse();
    ensure!(a.workers > 0 && a.batch_rows > 0, "invalid workers/batch");
    let mut p: Plan = serde_json::from_reader(File::open(&a.plan)?)?;
    if let Some(n) = a.max_jobs {
        p.jobs.truncate(n);
    }
    ensure!(!p.jobs.is_empty(), "no export jobs");
    ensure!(
        !a.output.join("_SUCCESS.json").exists(),
        "output already completed"
    );
    for d in ["data", "_audit/jobs", "_audit/rejected"] {
        fs::create_dir_all(a.output.join(d))?;
    }
    let env: BTreeMap<String, String> = fs::read_to_string(&a.env)?
        .lines()
        .filter(|s| !s.is_empty() && !s.starts_with('#'))
        .map(|s| {
            let (k, v) = s.split_once('=').unwrap();
            (k.into(), v.into())
        })
        .collect();
    let config = MySqlConnectOptions::new()
        .host(&env["IB_MYSQL_HOST"])
        .port(env["IB_MYSQL_PORT"].parse()?)
        .username(&env["IB_MYSQL_USER"])
        .password(&env["IB_MYSQL_PASSWORD"])
        .database(&env["IB_MYSQL_DATABASE"]);
    let next = AtomicUsize::new(0);
    let stop = AtomicBool::new(false);
    let started = Instant::now();
    let result = std::thread::scope(|scope| -> Result<Vec<Audit>> {
        let mut handles = Vec::new();
        for _ in 0..a.workers.min(p.jobs.len()) {
            let config = config.clone();
            let jobs = &p.jobs;
            let out = &a.output;
            let next = &next;
            let stop = &stop;
            let chunk = a.batch_rows;
            handles.push(scope.spawn(move || -> Result<Vec<Audit>> {
                let rt = tokio::runtime::Builder::new_current_thread()
                    .enable_all()
                    .build()?;
                rt.block_on(async {
                    let mut conn = MySqlConnection::connect_with(&config).await?;
                    for sql in [
                        "SET SESSION time_zone='+00:00'",
                        "SET SESSION MAX_EXECUTION_TIME=300000",
                        "START TRANSACTION WITH CONSISTENT SNAPSHOT, READ ONLY",
                    ] {
                        sqlx::raw_sql(sql).execute(&mut conn).await?;
                    }
                    let mut done = Vec::new();
                    loop {
                        if stop.load(Ordering::Relaxed) {
                            break;
                        }
                        let i = next.fetch_add(1, Ordering::Relaxed);
                        let Some(j) = jobs.get(i) else {
                            break;
                        };
                        match export(&mut conn, j, out, chunk).await {
                            Ok(r) => done.push(r),
                            Err(e) => {
                                stop.store(true, Ordering::Relaxed);
                                return Err(e.context(format!("export {}", j.ric)));
                            }
                        }
                    }
                    sqlx::raw_sql("ROLLBACK").execute(&mut conn).await?;
                    Ok(done)
                })
            }));
        }
        let mut done = Vec::new();
        let mut error = None;
        for h in handles {
            match h.join() {
                Ok(Ok(r)) => done.extend(r),
                Ok(Err(e)) => error = Some(e),
                Err(_) => error = Some(anyhow::anyhow!("export thread panic")),
            }
        }
        if let Some(e) = error {
            return Err(e);
        }
        Ok(done)
    })?;
    ensure!(result.len() == p.jobs.len(), "incomplete export");
    let summary = serde_json::json!({"status":"complete_verified","completed_at":chrono::Utc::now().to_rfc3339(),"output":a.output,"plan":a.plan,"plan_sha256":file_hash(&a.plan)?,"cutoff_id":p.cutoff_id,"files":result.len(),"rows":result.iter().map(|r|r.rows).sum::<u64>(),"rejected_invalid_utc":result.iter().map(|r|r.rejected_invalid_utc).sum::<u64>(),"time_flag_rows":result.iter().map(|r|r.time_flag_rows).sum::<u64>(),"ohlcv_flag_rows":result.iter().map(|r|r.ohlcv_flag_rows).sum::<u64>(),"identity_mismatch_rows":result.iter().map(|r|r.identity_mismatch_rows).sum::<u64>(),"bytes":result.iter().map(|r|r.bytes).sum::<u64>(),"partial_validation":a.max_jobs.is_some(),"elapsed_seconds":started.elapsed().as_secs_f64()});
    json_write(&a.output.join("_SUCCESS.json"), &summary)?;
    println!("{}", serde_json::to_string_pretty(&summary)?);
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    #[test]
    fn utc_authority_preserves_zero_epoch_and_zero_volume() {
        let r = SourceRow {
            id: 1,
            time_cst: Some("2026-01-02 08:00:00".into()),
            symbol: None,
            maturity_date: None,
            exchange: None,
            con_id: "1".into(),
            open: Some(-1.0),
            high: Some(0.0),
            low: Some(-2.0),
            close: Some(-1.0),
            volume: Some(0.0),
            utc_time: "2026-01-02 00:00:00".into(),
            timestamp: Some(0),
            sec_type: "fut".into(),
            created_at: None,
        };
        assert_eq!(flags(&r, time_ns(&r.utc_time).unwrap()), (2, 0));
        assert_eq!(r.timestamp, Some(0));
        assert!(time_ns("0000-00-00 00:00:00").is_none());
    }
}
