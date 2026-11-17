//! Size-class thresholds (large / medium / small trades), ONE definition shared
//! by `baseline_data_1min` step 4 (`fill_1min_size_buckets`) and the live gRPC
//! bars of `cn_kline_server`.
//!
//! `P50` / `P90` (linear, numpy default) of the inferred interval amounts of the
//! DOMINANT (主力) contract of each trading day of the COMPLETE PRECEDING
//! Shanghai calendar month (days are merged across rolls: before a roll the
//! old dominant's trades count, after it the new one's), applied per trade by
//! the month of the trade's Shanghai timestamp: a trade of month `M` is classed
//! with the thresholds of month `prior(M)`.
//!
//! One JSON per product and source month, written atomically:
//! `<dir>/dominant_by_day/<YYYYMM>/<PRODUCT>.json`. Missing thresholds are never
//! invented: callers leave the size columns unset / untouched.

use anyhow::{Context, Result};
use chrono::{Datelike, Duration, NaiveDate, TimeZone};
use chrono_tz::Asia::Shanghai;
use rocksdb::{Direction, IteratorMode};
use serde_json::json;
use std::collections::BTreeMap;
use std::path::{Path, PathBuf};

use crate::baseline_1min::linear_percentile;
use crate::codec::{decode_key, decode_trade, encode_key, KIND_TRADE};
use crate::db::L2Db;
use crate::dominant_table::{canonical, yyyymmdd};
use cn_futures_core::segments::trad_day_from_ts;

pub const BASIS: &str = "dominant_by_day";
pub const DEFAULT_DIR: &str = "/mnt/nvme-raid0-28t/fanghaizhou/cn_futures_data/size_thresholds";
pub const DEFAULT_DOMINANT_DIR: &str =
    "/mnt/nvme-raid0-28t/fanghaizhou/cn_futures_data/roll/result";

#[derive(Clone, Debug, PartialEq)]
pub struct Thr {
    pub source_month: u32,
    pub p50: f64,
    pub p90: f64,
    pub samples: usize,
    pub days: usize,
}

/// Shanghai `YYYYMM` of a UTC-nanosecond timestamp.
pub fn month_of_ns(ts_ns: u64) -> u32 {
    let local = Shanghai
        .timestamp_opt((ts_ns / 1_000_000_000) as i64, 0)
        .single()
        .expect("unix second");
    local.year() as u32 * 100 + local.month()
}

/// The month before `YYYYMM`.
pub fn prior_month(month: u32) -> u32 {
    let (y, m) = (month / 100, month % 100);
    if m == 1 {
        (y - 1) * 100 + 12
    } else {
        y * 100 + m - 1
    }
}

/// First and last calendar day of `YYYYMM`.
pub fn month_days(month: u32) -> Result<(NaiveDate, NaiveDate)> {
    let first = NaiveDate::from_ymd_opt((month / 100) as i32, month % 100, 1)
        .with_context(|| format!("bad month {month}"))?;
    let next = if first.month() == 12 {
        NaiveDate::from_ymd_opt(first.year() + 1, 1, 1)
    } else {
        NaiveDate::from_ymd_opt(first.year(), first.month() + 1, 1)
    }
    .context("next month")?;
    Ok((first, next.pred_opt().context("last day")?))
}

pub fn file_path(dir: &Path, month: u32, product: &str) -> PathBuf {
    dir.join(BASIS)
        .join(month.to_string())
        .join(format!("{}.json", product.to_ascii_uppercase()))
}

/// Parses and validates a threshold file for `product` / `month`.
pub fn parse(bytes: &[u8], product: &str, month: u32) -> Result<Thr> {
    let v: serde_json::Value = serde_json::from_slice(bytes).context("threshold json")?;
    anyhow::ensure!(v["basis"] == BASIS, "threshold basis is not {BASIS}");
    anyhow::ensure!(
        v["product"]
            .as_str()
            .is_some_and(|p| p.eq_ignore_ascii_case(product)),
        "threshold product mismatch"
    );
    anyhow::ensure!(
        v["source_month"].as_u64() == Some(u64::from(month)),
        "threshold source_month mismatch"
    );
    let num = |k: &str| v[k].as_f64().filter(|x| x.is_finite());
    let (p50, p90) = (num("p50").context("p50")?, num("p90").context("p90")?);
    let samples = v["samples"].as_u64().context("samples")? as usize;
    anyhow::ensure!(samples > 0 && p50 > 0.0 && p90 >= p50, "invalid thresholds");
    Ok(Thr {
        source_month: month,
        p50,
        p90,
        samples,
        days: v["days"].as_u64().unwrap_or(0) as usize,
    })
}

/// The stored thresholds if the file exists and is valid.
pub fn load_valid(dir: &Path, product: &str, month: u32) -> Option<Thr> {
    let bytes = std::fs::read(file_path(dir, month, product)).ok()?;
    parse(&bytes, product, month).ok()
}

/// Atomically writes the threshold file (temp name + rename).
pub fn write(
    dir: &Path,
    product: &str,
    thr: &Thr,
    column_families: &[String],
) -> Result<serde_json::Value> {
    let path = file_path(dir, thr.source_month, product);
    let doc = json!({
        "basis": BASIS,
        "product": product.to_ascii_uppercase(),
        "source_month": thr.source_month,
        "p50": thr.p50,
        "p90": thr.p90,
        "samples": thr.samples,
        "days": thr.days,
        "source": "cn_futures_l2 inferred interval turnover of the dominant contract by trading day",
        "column_families": column_families,
    });
    std::fs::create_dir_all(path.parent().context("threshold dir")?)?;
    let tmp = path.with_extension("json.tmp");
    std::fs::write(&tmp, serde_json::to_vec_pretty(&doc)?)?;
    std::fs::rename(&tmp, &path)?;
    Ok(doc)
}

/// Interval amounts of the dominant contract of every trading day of `month`.
/// `dominant`: trading day -> canonical dominant contract (see
/// `dominant_table::product_days`). Also returns the number of trading days
/// that contributed at least one trade.
pub fn dominant_amounts(
    db: &L2Db,
    cf_names: &[String],
    month: u32,
    dominant: &BTreeMap<NaiveDate, String>,
) -> Result<(Vec<f64>, usize)> {
    let (first, last) = month_days(month)?;
    // A trade's trading day can lie up to 3 calendar days after its timestamp
    // (Friday night -> Monday); nothing after `last` midnight can belong to it.
    let lo = shanghai_midnight_ns(first - Duration::days(4));
    let hi = shanghai_midnight_ns(last + Duration::days(1));
    let mut amounts = Vec::new();
    let mut days = std::collections::BTreeSet::new();
    for cf_name in cf_names {
        let cf = db
            .cf_handle(cf_name)
            .with_context(|| format!("missing cf {cf_name}"))?;
        let start = encode_key(KIND_TRADE, "", 0, 0)?;
        for item in db.iterator_cf(&cf, IteratorMode::From(&start, Direction::Forward)) {
            let (key, value) = item.context("iterate trade")?;
            let (kind, id, ts_ns, _) = decode_key(&key)?;
            if kind != KIND_TRADE {
                break;
            }
            if ts_ns < lo || ts_ns >= hi {
                continue;
            }
            let Some(day) = dominant_trade_day(dominant, &id, ts_ns, first, last) else {
                continue;
            };
            let rec = decode_trade(&value)?;
            let amount = rec.turnover.unwrap_or(rec.price * rec.volume);
            if amount.is_finite() && amount > 0.0 {
                amounts.push(amount);
                days.insert(day);
            }
        }
    }
    Ok((amounts, days.len()))
}

/// The trading day of a trade when it belongs to the dominant sample of
/// `[first, last]`: the trade's trading day lies in the range and its contract
/// is that day's dominant (compared in canonical form, so CZCE `YMM` and
/// `YYMM` agree). A roll therefore switches contracts at the trading-day
/// boundary: the Friday night session counts for Monday's dominant.
pub fn dominant_trade_day(
    dominant: &BTreeMap<NaiveDate, String>,
    instrument: &str,
    ts_ns: u64,
    first: NaiveDate,
    last: NaiveDate,
) -> Option<NaiveDate> {
    let day = trad_day_from_ts((ts_ns / 1_000_000_000) as i64);
    if day < first || day > last {
        return None;
    }
    let want = dominant.get(&day)?;
    (&canonical(instrument, yyyymmdd(day)) == want).then_some(day)
}

fn shanghai_midnight_ns(day: NaiveDate) -> u64 {
    Shanghai
        .from_local_datetime(&day.and_hms_opt(0, 0, 0).expect("midnight"))
        .single()
        .expect("Shanghai has no DST")
        .timestamp_nanos_opt()
        .unwrap_or(0)
        .max(0) as u64
}

/// P50 / P90 of `amounts` (linear), `None` when there are none.
pub fn percentiles(mut amounts: Vec<f64>, days: usize, month: u32) -> Option<Thr> {
    amounts.sort_by(|a, b| a.partial_cmp(b).unwrap_or(std::cmp::Ordering::Equal));
    let p50 = linear_percentile(&amounts, 0.5)?;
    let p90 = linear_percentile(&amounts, 0.9)?;
    Some(Thr {
        source_month: month,
        p50,
        p90,
        samples: amounts.len(),
        days,
    })
}

/// The thresholds of `month` for `product`: the stored file when valid, else
/// computed from RocksDB + the dominant table and stored. `Ok(None)` when the
/// month has no usable dominant trades (new listing, no samples).
pub fn ensure(
    db: &L2Db,
    cf_names: &[String],
    product: &str,
    month: u32,
    dir: &Path,
    dominant_dir: &Path,
    force: bool,
) -> Result<Option<Thr>> {
    if !force {
        if let Some(thr) = load_valid(dir, product, month) {
            return Ok(Some(thr));
        }
    }
    let dominant = crate::dominant_table::product_days(dominant_dir, product)?;
    let (amounts, days) = dominant_amounts(db, cf_names, month, &dominant)?;
    let Some(thr) = percentiles(amounts, days, month) else {
        return Ok(None);
    };
    anyhow::ensure!(
        thr.p50 > 0.0 && thr.p90 >= thr.p50,
        "{product} {month}: invalid computed thresholds"
    );
    write(dir, product, &thr, cf_names)?;
    Ok(Some(thr))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn months_roll_over_years_and_cover_complete_ranges() {
        assert_eq!(prior_month(202611), 202610);
        assert_eq!(prior_month(202601), 202512);
        let (a, b) = month_days(202602).unwrap();
        assert_eq!(a.to_string(), "2026-02-01");
        assert_eq!(b.to_string(), "2026-02-28");
        let (_, dec) = month_days(202512).unwrap();
        assert_eq!(dec.to_string(), "2025-12-31");
    }

    #[test]
    fn trade_month_uses_the_shanghai_timestamp() {
        // 2026-10-31 16:30 UTC is 2026-11-01 00:30 Shanghai.
        let ts = chrono::Utc
            .with_ymd_and_hms(2026, 10, 31, 16, 30, 0)
            .unwrap()
            .timestamp() as u64
            * 1_000_000_000;
        assert_eq!(month_of_ns(ts), 202611);
        assert_eq!(month_of_ns(ts - 3_600 * 1_000_000_000), 202610);
    }

    #[test]
    fn file_validation_rejects_wrong_basis_month_product_and_values() {
        let good = br#"{"basis":"dominant_by_day","product":"SN","source_month":202610,"p50":10.0,"p90":20.0,"samples":5,"days":3}"#;
        let thr = parse(good, "sn", 202610).unwrap();
        assert_eq!(
            (thr.p50, thr.p90, thr.samples, thr.days),
            (10.0, 20.0, 5, 3)
        );
        assert!(parse(good, "SN", 202609).is_err(), "other month");
        assert!(parse(good, "CU", 202610).is_err(), "other product");
        let legacy = br#"{"product":"SN","source_month":202610,"p50":10.0,"p90":20.0,"samples":5}"#;
        assert!(
            parse(legacy, "SN", 202610).is_err(),
            "no basis = not this definition"
        );
        let bad = br#"{"basis":"dominant_by_day","product":"SN","source_month":202610,"p50":30.0,"p90":20.0,"samples":5}"#;
        assert!(parse(bad, "SN", 202610).is_err(), "p90 < p50");
    }

    #[test]
    fn a_roll_switches_the_dominant_sample_at_the_trading_day_boundary() {
        let d = |m, day| NaiveDate::from_ymd_opt(2026, m, day).unwrap();
        // Dominant sn2611 up to Friday 10-09, rolled to sn2612 for Monday 10-12.
        let dominant: BTreeMap<NaiveDate, String> = [
            (d(10, 8), "sn2611"),
            (d(10, 9), "sn2611"),
            (d(10, 12), "sn2612"),
            (d(10, 13), "sn2612"),
        ]
        .iter()
        .map(|(day, c)| (*day, c.to_string()))
        .collect();
        let at = |m: u32, day: u32, h: u32, min: u32| -> u64 {
            Shanghai
                .with_ymd_and_hms(2026, m, day, h, min, 0)
                .unwrap()
                .timestamp() as u64
                * 1_000_000_000
        };
        let (first, last) = (d(10, 1), d(10, 31));
        let pick = |id: &str, ts: u64| dominant_trade_day(&dominant, id, ts, first, last);
        // Friday day session: old dominant counts, the new contract does not.
        assert_eq!(pick("sn2611", at(10, 9, 10, 0)), Some(d(10, 9)));
        assert_eq!(pick("sn2612", at(10, 9, 10, 0)), None);
        // Friday NIGHT session belongs to Monday's trading day: new dominant counts.
        assert_eq!(pick("sn2612", at(10, 9, 21, 30)), Some(d(10, 12)));
        assert_eq!(pick("sn2611", at(10, 9, 21, 30)), None);
        // After midnight on Saturday: still Monday's trading day.
        assert_eq!(pick("sn2612", at(10, 10, 0, 30)), Some(d(10, 12)));
        // Monday day session: only the new dominant.
        assert_eq!(pick("sn2611", at(10, 12, 10, 0)), None);
        assert_eq!(pick("SN2612", at(10, 12, 10, 0)), Some(d(10, 12)));
        // A day without a dominant row, and a day outside the month, never count.
        assert_eq!(pick("sn2612", at(10, 14, 10, 0)), None);
        assert_eq!(pick("sn2611", at(9, 30, 10, 0)), None);
        assert_eq!(pick("sn2611", at(11, 2, 10, 0)), None);
    }

    #[test]
    fn czce_codes_match_the_dominant_in_either_digit_form() {
        let day = NaiveDate::from_ymd_opt(2026, 10, 12).unwrap();
        let dominant: BTreeMap<NaiveDate, String> =
            [(day, "cf2701".to_string())].into_iter().collect();
        let ts = Shanghai
            .with_ymd_and_hms(2026, 10, 12, 10, 0, 0)
            .unwrap()
            .timestamp() as u64
            * 1_000_000_000;
        let (first, last) = (NaiveDate::from_ymd_opt(2026, 10, 1).unwrap(), day);
        for code in ["CF701", "CF2701", "cf701"] {
            assert_eq!(
                dominant_trade_day(&dominant, code, ts, first, last),
                Some(day)
            );
        }
        assert_eq!(
            dominant_trade_day(&dominant, "CF703", ts, first, last),
            None
        );
    }

    #[test]
    fn percentiles_are_linear_and_empty_is_none() {
        let thr = percentiles(vec![4.0, 1.0, 3.0, 2.0, 5.0], 2, 202610).unwrap();
        assert_eq!((thr.p50, thr.p90, thr.samples), (3.0, 4.6, 5));
        assert!(percentiles(Vec::new(), 0, 202610).is_none());
    }

    #[test]
    fn atomic_write_round_trips_through_load_valid() {
        let dir = tempfile::tempdir().unwrap();
        let thr = Thr {
            source_month: 202610,
            p50: 758300.0,
            p90: 2658420.0,
            samples: 1124572,
            days: 22,
        };
        write(dir.path(), "SN", &thr, &["p:2026:SN".to_string()]).unwrap();
        assert_eq!(load_valid(dir.path(), "SN", 202610), Some(thr));
        assert_eq!(load_valid(dir.path(), "SN", 202609), None);
    }
}
