//! Calendar helpers. Days are `u32` `YYYYMMDD`; Shanghai has no DST, so UTC+8 is exact.

use anyhow::{bail, Result};
use std::time::{SystemTime, UNIX_EPOCH};

pub const MS_PER_DAY: u64 = 86_400_000;
const SHANGHAI_OFFSET_S: i64 = 8 * 3600;

/// Accepts `YYYYMMDD` or `YYYY-MM-DD`.
pub fn parse(text: &str) -> Result<u32> {
    let digits: String = text.chars().filter(|c| *c != '-').collect();
    if digits.len() != 8 || !digits.bytes().all(|b| b.is_ascii_digit()) {
        bail!("invalid date {text:?}; expected YYYYMMDD or YYYY-MM-DD");
    }
    let day: u32 = digits.parse()?;
    let (y, m, d) = split(day);
    if !(1..=12).contains(&m) || d == 0 || d > days_in_month(y, m) {
        bail!("invalid calendar date {text:?}");
    }
    Ok(day)
}

pub fn split(day: u32) -> (i64, u32, u32) {
    ((day / 10_000) as i64, day / 100 % 100, day % 100)
}

fn days_in_month(y: i64, m: u32) -> u32 {
    match m {
        1 | 3 | 5 | 7 | 8 | 10 | 12 => 31,
        4 | 6 | 9 | 11 => 30,
        _ if (y % 4 == 0 && y % 100 != 0) || y % 400 == 0 => 29,
        _ => 28,
    }
}

/// Days since 1970-01-01 (Howard Hinnant's civil algorithm).
pub fn to_epoch_days(day: u32) -> i64 {
    let (y, m, d) = split(day);
    let y = if m <= 2 { y - 1 } else { y };
    let era = y.div_euclid(400);
    let yoe = y - era * 400;
    let m = m as i64;
    let doy = (153 * (if m > 2 { m - 3 } else { m + 9 }) + 2) / 5 + d as i64 - 1;
    let doe = yoe * 365 + yoe / 4 - yoe / 100 + doy;
    era * 146_097 + doe - 719_468
}

pub fn from_epoch_days(z: i64) -> u32 {
    let z = z + 719_468;
    let era = z.div_euclid(146_097);
    let doe = z - era * 146_097;
    let yoe = (doe - doe / 1460 + doe / 36_524 - doe / 146_096) / 365;
    let doy = doe - (365 * yoe + yoe / 4 - yoe / 100);
    let mp = (5 * doy + 2) / 153;
    let d = doy - (153 * mp + 2) / 5 + 1;
    let m = if mp < 10 { mp + 3 } else { mp - 9 };
    let y = yoe + era * 400 + i64::from(m <= 2);
    (y * 10_000 + m * 100 + d) as u32
}

pub fn add_days(day: u32, n: i64) -> u32 {
    from_epoch_days(to_epoch_days(day) + n)
}

/// Current Asia/Shanghai wall clock as (day, ms of day).
pub fn shanghai_now() -> (u32, u64) {
    let now = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap_or_default();
    let local_ms = now.as_millis() as i64 + SHANGHAI_OFFSET_S * 1000;
    let days = local_ms.div_euclid(MS_PER_DAY as i64);
    (
        from_epoch_days(days),
        local_ms.rem_euclid(MS_PER_DAY as i64) as u64,
    )
}

/// A natural-day directory is final once the Saturday-morning night tail (02:30)
/// of the following day could no longer belong to it: Shanghai D+1 03:00.
pub fn directory_final(dir_day: u32) -> bool {
    let (today, ms) = shanghai_now();
    let next = add_days(dir_day, 1);
    today > next || (today == next && ms >= 3 * 3_600_000)
}

pub fn format_iso(day: u32) -> String {
    let (y, m, d) = split(day);
    format!("{y:04}-{m:02}-{d:02}")
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn epoch_roundtrip() {
        assert_eq!(to_epoch_days(19700101), 0);
        assert_eq!(from_epoch_days(to_epoch_days(20261113)), 20261113);
        assert_eq!(add_days(20240301, -1), 20240229);
        assert_eq!(add_days(20261231, 1), 20270101);
        assert_eq!(parse("2026-11-13").unwrap(), 20261113);
        assert!(parse("20260230").is_err());
    }
}
