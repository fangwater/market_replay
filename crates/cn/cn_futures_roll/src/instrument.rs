//! Port of `cn_futures/instrument.py` and `cn_futures/universe.py`.

use chrono::{Datelike, NaiveDate};

/// `universe.EXCLUDED_RESEARCH_PRODUCTS`.
pub const EXCLUDED_RESEARCH_PRODUCTS: &[&str] =
    &["BB", "JR", "LR", "PM", "RI", "RS", "WH", "WR", "ZC"];

/// `universe.is_maintained_product`: non-empty after `strip().upper()` and not excluded.
pub fn is_maintained_product(product_id: &str) -> bool {
    let resolved = product_id.trim().to_uppercase();
    !resolved.is_empty() && !EXCLUDED_RESEARCH_PRODUCTS.contains(&resolved.as_str())
}

/// `(letters, digits)` for a plain month contract, else `None`.
pub fn split_instrument_id(instrument_id: &str) -> Option<(String, String)> {
    let text = instrument_id.trim();
    if text.is_empty() {
        return None;
    }
    let mut letters = String::new();
    let mut digits = String::new();
    let mut digits_started = false;
    for ch in text.chars() {
        if ch.is_alphabetic() {
            if digits_started {
                return None;
            }
            letters.push(ch);
        } else if ch.is_ascii_digit() {
            digits_started = true;
            digits.push(ch);
        } else {
            return None;
        }
    }
    if letters.is_empty() || digits.is_empty() {
        return None;
    }
    Some((letters, digits))
}

/// `YYYYMM` for a 4-digit month code, else `None`.
pub fn delivery_yyyymm(instrument_id: &str) -> Option<i64> {
    let (_, digits) = split_instrument_id(instrument_id)?;
    if digits.len() != 4 {
        return None;
    }
    let month: i64 = digits[2..].parse().ok()?;
    if !(1..=12).contains(&month) {
        return None;
    }
    let year: i64 = digits[..2].parse().ok()?;
    Some((2000 + year) * 100 + month)
}

/// True only when both ids parse and `candidate` expires after `current`.
pub fn is_later_delivery(candidate_id: &str, current_id: &str) -> bool {
    match (delivery_yyyymm(candidate_id), delivery_yyyymm(current_id)) {
        (Some(candidate), Some(current)) => candidate > current,
        _ => false,
    }
}

/// Expand a Zhengzhou 3-digit `YMM` to 4-digit `YYMM` using `trad_day`
/// (nearest listing within ±10 years; ties prefer the future month).
pub fn canonicalize_instrument_id(instrument_id: &str, trad_day: NaiveDate) -> String {
    let Some((letters, digits)) = split_instrument_id(instrument_id) else {
        return instrument_id.trim().to_owned();
    };
    if digits.len() != 3 {
        return instrument_id.trim().to_owned();
    }
    let month: i64 = digits[1..].parse().expect("ascii digits");
    if !(1..=12).contains(&month) {
        return instrument_id.trim().to_owned();
    }
    let year_ones: i64 = digits[..1].parse().expect("ascii digit");
    let trad_year = i64::from(trad_day.year());
    let decade = trad_year.div_euclid(10) * 10;
    let trad_months = trad_year * 12 + i64::from(trad_day.month());
    let mut best: Option<((i64, i64), i64)> = None;
    for adj in [-10, 0, 10] {
        let year = decade + year_ones + adj;
        if !(1990..=2100).contains(&year) {
            continue;
        }
        let delta = year * 12 + month - trad_months;
        let key = (delta.abs(), if delta >= 0 { 0 } else { 1 });
        if best.is_none_or(|(best_key, _)| key < best_key) {
            best = Some((key, year));
        }
    }
    match best {
        Some((_, year)) => format!("{letters}{:02}{month:02}", year.rem_euclid(100)),
        None => instrument_id.trim().to_owned(),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn d(y: i32, m: u32, day: u32) -> NaiveDate {
        NaiveDate::from_ymd_opt(y, m, day).unwrap()
    }

    #[test]
    fn four_digit_and_other_exchanges_unchanged() {
        let day = d(2025, 11, 3);
        assert_eq!(canonicalize_instrument_id("AP2601", day), "AP2601");
        assert_eq!(canonicalize_instrument_id("rb2505", day), "rb2505");
        assert_eq!(canonicalize_instrument_id(" IF2503 ", day), "IF2503");
    }

    #[test]
    fn zhengzhou_ymm_expands_with_trad_day_decade() {
        assert_eq!(
            canonicalize_instrument_id("AP601", d(2025, 11, 4)),
            "AP2601"
        );
        assert_eq!(
            canonicalize_instrument_id("AP511", d(2025, 11, 4)),
            "AP2511"
        );
        assert_eq!(
            canonicalize_instrument_id("SA601", d(2025, 11, 4)),
            "SA2601"
        );
        // Same month as TradDay: delta 0.
        assert_eq!(
            canonicalize_instrument_id("CF511", d(2025, 11, 4)),
            "CF2511"
        );
    }

    #[test]
    fn year_ones_wraps_to_the_nearest_decade() {
        assert_eq!(
            canonicalize_instrument_id("AP001", d(2029, 12, 2)),
            "AP3001"
        );
        assert_eq!(canonicalize_instrument_id("AP912", d(2020, 1, 6)), "AP1912");
        // Exactly 5 years either way: the future listing wins the tie.
        assert_eq!(canonicalize_instrument_id("AP006", d(2025, 6, 1)), "AP3006");
    }

    #[test]
    fn invalid_or_combo_ids_pass_through() {
        let day = d(2025, 11, 4);
        assert_eq!(
            canonicalize_instrument_id("rb2505&rb2510", day),
            "rb2505&rb2510"
        );
        assert_eq!(canonicalize_instrument_id("", day), "");
        assert_eq!(canonicalize_instrument_id("AP613", day), "AP613");
        assert_eq!(canonicalize_instrument_id("AP600", day), "AP600");
        assert_eq!(canonicalize_instrument_id("601", day), "601");
    }

    #[test]
    fn delivery_month_parsing() {
        assert_eq!(delivery_yyyymm("bc2506"), Some(202506));
        assert_eq!(delivery_yyyymm("AP2601"), Some(202601));
        assert_eq!(delivery_yyyymm("AP601"), None);
        assert_eq!(delivery_yyyymm("rbefp"), None);
        assert_eq!(delivery_yyyymm("rb2513"), None);
        assert!(is_later_delivery("bc2507", "bc2506"));
        assert!(!is_later_delivery("bc2505", "bc2506"));
        assert!(!is_later_delivery("bc2506", "bc2506"));
        assert!(!is_later_delivery("AP601", "AP2601"));
    }

    #[test]
    fn universe_excludes_frozen_and_thin_products() {
        assert!(is_maintained_product("AP"));
        assert!(is_maintained_product("rb"));
        assert!(!is_maintained_product("WR"));
        assert!(!is_maintained_product(" wr "));
        assert!(!is_maintained_product(""));
    }
}
