//! Dominant (主力) contract per product and trading day, read from the roll
//! replay outputs `<roll_dir>/<exchange>_dominant.csv` (columns `trad_day`,
//! `product_id`, `instrument_id`; see docs/cn_daily_and_kline.md), and the
//! contract-code canonicalisation shared with the kline server.
//!
//! `instrument_id` is the dominant contract in effect on `trad_day`; it is
//! empty before the first dominant is scheduled (such days have none).

use anyhow::{Context, Result};
use chrono::NaiveDate;
use std::collections::BTreeMap;
use std::path::Path;

pub const EXCHANGES: [&str; 6] = ["ccfx", "xdce", "xgfe", "xsge", "xsie", "xzce"];

/// Lower-case; CZCE `YMM` widened to `YYMM` using the trading day's decade
/// (`trading_day` is `YYYYMMDD`, 0 = unknown: no widening).
pub fn canonical(code: &str, trading_day: u32) -> String {
    let lower = code.trim().to_ascii_lowercase();
    let letters = lower
        .chars()
        .take_while(|c| c.is_ascii_alphabetic())
        .count();
    let tail = &lower[letters..];
    if tail.len() == 3 && tail.bytes().all(|b| b.is_ascii_digit()) && trading_day > 0 {
        let year = (trading_day / 10_000) as i32;
        let mut full = year / 10 * 10 + (tail.as_bytes()[0] - b'0') as i32;
        if full < year - 1 {
            full += 10;
        }
        return format!("{}{:02}{}", &lower[..letters], full % 100, &tail[1..]);
    }
    lower
}

/// `YYYYMMDD` as a number.
pub fn yyyymmdd(day: NaiveDate) -> u32 {
    use chrono::Datelike;
    day.year() as u32 * 10_000 + day.month() * 100 + day.day()
}

/// The dominant contract of one product per trading day, already
/// canonicalised for that day (compare with `canonical(trade_instrument, day)`).
pub fn product_days(roll_dir: &Path, product: &str) -> Result<BTreeMap<NaiveDate, String>> {
    let product = product.to_ascii_uppercase();
    let mut out = BTreeMap::new();
    for exchange in EXCHANGES {
        let path = roll_dir.join(format!("{exchange}_dominant.csv"));
        let mut reader = match csv::Reader::from_path(&path) {
            Ok(r) => r,
            Err(e) if matches!(e.kind(), csv::ErrorKind::Io(io) if io.kind() == std::io::ErrorKind::NotFound) => {
                continue
            }
            Err(e) => return Err(e).with_context(|| format!("open {path:?}")),
        };
        let headers = reader.headers()?.clone();
        let col = |name: &str| {
            headers
                .iter()
                .position(|h| h == name)
                .with_context(|| format!("{path:?} has no column {name}"))
        };
        let (td, prod, inst) = (col("trad_day")?, col("product_id")?, col("instrument_id")?);
        for record in reader.records() {
            let r = record?;
            if !r.get(prod).unwrap_or("").eq_ignore_ascii_case(&product) {
                continue;
            }
            let instrument = r.get(inst).unwrap_or("").trim();
            if instrument.is_empty() {
                continue;
            }
            let day = NaiveDate::parse_from_str(r.get(td).unwrap_or(""), "%Y-%m-%d")
                .with_context(|| format!("{path:?}: bad trad_day {:?}", r.get(td)))?;
            out.insert(day, canonical(instrument, yyyymmdd(day)));
        }
    }
    Ok(out)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn czce_three_digit_codes_are_widened_by_trading_day() {
        assert_eq!(canonical("AP601", 20251105), "ap2601");
        assert_eq!(canonical("ap2601", 20251105), "ap2601");
        assert_eq!(canonical("SN2701", 20261117), "sn2701");
        assert_eq!(canonical("CF701", 20261117), "cf2701");
    }

    #[test]
    fn dominant_days_skip_empty_and_other_products() {
        let dir = tempfile::tempdir().unwrap();
        std::fs::write(
            dir.path().join("xsge_dominant.csv"),
            "trad_day,product_id,instrument_id,roll_flag,decision_open_int,scheduled_next,rule\n\
             2026-10-08,SN,,0,,sn2611,r\n\
             2026-10-09,SN,sn2611,0,1.0,sn2611,r\n\
             2026-10-12,SN,sn2612,1,1.0,sn2612,r\n\
             2026-10-12,CU,cu2611,0,1.0,cu2611,r\n",
        )
        .unwrap();
        let got = product_days(dir.path(), "sn").unwrap();
        assert_eq!(got.len(), 2);
        assert_eq!(
            got[&NaiveDate::from_ymd_opt(2026, 10, 9).unwrap()],
            "sn2611"
        );
        assert_eq!(
            got[&NaiveDate::from_ymd_opt(2026, 10, 12).unwrap()],
            "sn2612"
        );
    }
}
