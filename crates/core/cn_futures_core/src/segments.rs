//! Pure continuous-segment and calendar helpers (moved from cn_futures_l2::export_1s).

use chrono::{Datelike, Duration, NaiveDate, Timelike, Weekday};

use crate::codec::is_product_cf_name;
use crate::session::{shanghai, skip_output, Exchange};

pub const SESSION_BREAK_SEC: i64 = 1_800;
const TEA_MIN_GAP_SEC: i64 = 480;

fn is_tea_break(prev: i64, next: i64) -> bool {
    if next - prev < TEA_MIN_GAP_SEC {
        return false;
    }
    let a = shanghai(prev);
    let b = shanghai(next);
    a.hour() == 10
        && (10..20).contains(&a.minute())
        && b.hour() == 10
        && (25..36).contains(&b.minute())
}

pub fn is_session_break(prev: i64, next: i64) -> bool {
    let gap = next - prev;
    gap >= SESSION_BREAK_SEC || is_tea_break(prev, next)
}

fn on_or_next_weekday(day: NaiveDate) -> NaiveDate {
    match day.weekday() {
        Weekday::Sat => day + Duration::days(2),
        Weekday::Sun => day + Duration::days(1),
        _ => day,
    }
}

pub fn trad_day_from_ts(ts_sec: i64) -> NaiveDate {
    let local = shanghai(ts_sec);
    let day = local.date_naive();
    let hour = local.hour();
    if hour >= 20 {
        on_or_next_weekday(day + Duration::days(1))
    } else if hour < 8 {
        on_or_next_weekday(day)
    } else {
        day
    }
}

pub fn book_valid(bid0p: f64, bid0v: f64, ask0p: f64, ask0v: f64) -> bool {
    bid0p.is_finite()
        && ask0p.is_finite()
        && bid0v.is_finite()
        && ask0v.is_finite()
        && bid0p > 0.0
        && ask0p >= bid0p
        && bid0v >= 0.0
        && ask0v >= 0.0
}

pub fn split_depth_segments(
    secs: &[i64],
    exchange: Exchange,
    contract_id: &str,
) -> Vec<(i64, i64)> {
    let kept: Vec<i64> = secs
        .iter()
        .copied()
        .filter(|ts| !skip_output(*ts, exchange, contract_id))
        .collect();
    if kept.is_empty() {
        return Vec::new();
    }
    let mut out = Vec::new();
    let mut start = kept[0];
    let mut prev = kept[0];
    for &ts in &kept[1..] {
        if is_session_break(prev, ts) {
            out.push((start, prev));
            start = ts;
        }
        prev = ts;
    }
    out.push((start, prev));
    out
}

pub fn exchange_of(product: &str) -> Option<&'static str> {
    Some(match product {
        "IC" | "IF" | "IH" | "IM" | "T" | "TF" | "TL" | "TS" => "ccfx",
        "A" | "B" | "BB" | "BZ" | "C" | "CS" | "EB" | "EG" | "FB" | "I" | "J" | "JD" | "JM"
        | "L" | "LG" | "LH" | "M" | "P" | "PG" | "PP" | "RR" | "V" | "Y" => "xdce",
        "LC" | "PD" | "PS" | "PT" | "SI" => "xgfe",
        "AD" | "AG" | "AL" | "AO" | "AU" | "BR" | "BU" | "CU" | "FU" | "HC" | "NI" | "OP"
        | "PB" | "RB" | "RU" | "SN" | "SP" | "SS" | "WR" | "ZN" => "xsge",
        "BC" | "EC" | "LU" | "NR" | "SC" => "xsie",
        "AP" | "CF" | "CJ" | "CY" | "FG" | "JR" | "LR" | "MA" | "OI" | "PF" | "PK" | "PL"
        | "PM" | "PR" | "PX" | "RI" | "RM" | "RS" | "SA" | "SF" | "SH" | "SM" | "SR" | "TA"
        | "UR" | "WH" | "ZC" => "xzce",
        _ => return None,
    })
}

pub fn parse_product_cf(name: &str) -> Option<(i32, String)> {
    if !is_product_cf_name(name) {
        return None;
    }
    let rest = name.strip_prefix("p:")?;
    let (year, product) = rest.split_once(':')?;
    let year: i32 = year.parse().ok()?;
    Some((year, product.to_string()))
}
