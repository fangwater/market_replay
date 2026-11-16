//! IB `minute_data` row model and the lossless 23-column Parquet conversion.
//! Copied verbatim (logic, field order, schema metadata) from
//! the retired `ly_cme_data` `ib_ric_parquet` bin, which produced the
//! 2026-11-03 per-RIC snapshot, so incremental day files use the same schema.
use anyhow::Result;
use arrow_array::{
    ArrayRef, BooleanArray, Float64Array, Int64Array, RecordBatch, StringArray,
    TimestampNanosecondArray, UInt16Array,
};
use arrow_schema::{DataType, Field, Schema, TimeUnit};
use chrono::{Datelike, NaiveDateTime};
use serde::{Deserialize, Serialize};
use sqlx::{mysql::MySqlRow, Row};
use std::{
    collections::{BTreeMap, HashMap},
    sync::Arc,
};

/// Source columns in the order `decode` expects; the caller appends FROM/WHERE.
pub const SELECT_COLUMNS: &str = "t.`id`,CAST(t.`time_cst` AS CHAR) AS `time_cst`,t.`symbol`,t.`maturity_date`,t.`exchange`,t.`con_id`,t.`open`,t.`high`,t.`low`,t.`close`,t.`volume`,CAST(t.`utc_time` AS CHAR) AS `utc_time`,t.`timestamp`,t.`sec_type`,CAST(t.`created_at` AS CHAR) AS `created_at`";

/// Contract identity serialized into the `ib_contract` schema metadata.
/// Field order and the flattened (sorted) extras must stay as in the snapshot.
#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct Job {
    pub ric: String,
    pub con_id: String,
    pub symbol: String,
    pub exchange: String,
    pub maturity_date: String,
    pub local_symbol: String,
    pub currency: String,
    pub multiplier: String,
    pub ib_first_utc: String,
    pub ib_last_utc: String,
    pub cutoff_id: i64,
    #[serde(flatten)]
    pub extra: BTreeMap<String, serde_json::Value>,
}
#[derive(Clone, Debug, PartialEq, Serialize)]
pub struct SourceRow {
    pub id: i64,
    pub time_cst: Option<String>,
    pub symbol: Option<String>,
    pub maturity_date: Option<String>,
    pub exchange: Option<String>,
    pub con_id: String,
    pub open: Option<f64>,
    pub high: Option<f64>,
    pub low: Option<f64>,
    pub close: Option<f64>,
    pub volume: Option<f64>,
    pub utc_time: String,
    pub timestamp: Option<i64>,
    pub sec_type: String,
    pub created_at: Option<String>,
}
pub fn time_ns(s: &str) -> Option<i64> {
    let d = NaiveDateTime::parse_from_str(s, "%Y-%m-%d %H:%M:%S%.f").ok()?;
    if d.year() < 1970 {
        return None;
    }
    d.and_utc().timestamp_nanos_opt()
}
pub fn flags(r: &SourceRow, ts: i64) -> (u16, u16) {
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
pub fn identity(r: &SourceRow, j: &Job) -> bool {
    r.symbol.as_deref() == Some(&j.symbol)
        && r.exchange.as_deref() == Some(&j.exchange)
        && r.maturity_date.as_deref() == Some(&j.maturity_date)
        && r.con_id == j.con_id
        && r.sec_type.eq_ignore_ascii_case("fut")
}
pub fn schema(j: &Job) -> Arc<Schema> {
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
pub fn batch(rows: &[SourceRow], j: &Job, s: Arc<Schema>) -> Result<RecordBatch> {
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
pub fn decode(r: MySqlRow) -> Result<SourceRow> {
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

#[cfg(test)]
pub mod testutil {
    use super::*;
    pub fn job(ric: &str) -> Job {
        let mut extra = BTreeMap::new();
        extra.insert("tas_product".into(), serde_json::json!("ES"));
        extra.insert("tas_aliases".into(), serde_json::json!([ric]));
        Job {
            ric: ric.into(),
            con_id: "515416632".into(),
            symbol: "ES".into(),
            exchange: "CME".into(),
            maturity_date: "20261218".into(),
            local_symbol: "ESZ6".into(),
            currency: "USD".into(),
            multiplier: "50".into(),
            ib_first_utc: "2026-02-06 15:01:00".into(),
            ib_last_utc: "2026-11-03 05:13:00".into(),
            cutoff_id: 1000,
            extra,
        }
    }
    /// One source row at a UTC wall time "YYYY-MM-DD HH:MM:SS".
    pub fn row(id: i64, utc: &str) -> SourceRow {
        let ts = time_ns(utc).unwrap();
        let cst = chrono::DateTime::from_timestamp_nanos(ts + 28_800_000_000_000)
            .format("%Y-%m-%d %H:%M:%S")
            .to_string();
        SourceRow {
            id,
            time_cst: Some(cst),
            symbol: Some("ES".into()),
            maturity_date: Some("20261218".into()),
            exchange: Some("CME".into()),
            con_id: "515416632".into(),
            open: Some(100.0),
            high: Some(101.0),
            low: Some(99.0),
            close: Some(100.5),
            volume: Some(id as f64),
            utc_time: utc.into(),
            timestamp: Some(ts / 1_000_000),
            sec_type: "FUT".into(),
            created_at: Some(utc.into()),
        }
    }
    pub fn midnight(utc: &str) -> i64 {
        time_ns(&format!("{} 00:00:00", &utc[..10])).unwrap()
    }
}

#[cfg(test)]
mod tests {
    use super::{testutil::*, *};
    #[test]
    fn utc_authority_preserves_zero_epoch_and_zero_volume() {
        // Same case as ib_ric_parquet's own test: the UTC column is authoritative.
        let mut r = row(1, "2026-01-02 00:00:00");
        r.timestamp = Some(0);
        r.volume = Some(0.0);
        assert_eq!(flags(&r, time_ns(&r.utc_time).unwrap()), (2, 0));
        assert!(time_ns("0000-00-00 00:00:00").is_none());
        let j = job("ESZ26");
        let b = batch(&[r], &j, schema(&j)).unwrap();
        assert_eq!(b.num_columns(), 23);
        assert_eq!(b.schema().metadata()["source"], "ib_data.minute_data");
    }
}
