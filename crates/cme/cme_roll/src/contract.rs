//! CME contract identity from LSEG outright RICs: `<root><month code><year digits>`.
use anyhow::{bail, Context, Result};
use chrono::{Datelike, NaiveDate};
use std::fmt;

/// The 16 research products: LSEG RIC root and risk-day class.
pub const PRODUCTS: [(&str, RiskClass); 16] = [
    ("ES", RiskClass::LastTradingDay),
    ("NQ", RiskClass::LastTradingDay),
    ("RTY", RiskClass::LastTradingDay),
    ("YM", RiskClass::LastTradingDay),
    ("CL", RiskClass::LastTradingDay),
    ("URO", RiskClass::LastTradingDay),
    ("JY", RiskClass::LastTradingDay),
    ("GC", RiskClass::FirstPositionDay),
    ("SI", RiskClass::FirstPositionDay),
    ("HG", RiskClass::FirstPositionDay),
    ("TU", RiskClass::FirstIntentionDay),
    ("FV", RiskClass::FirstIntentionDay),
    ("TY", RiskClass::FirstIntentionDay),
    ("TN", RiskClass::FirstIntentionDay),
    ("US", RiskClass::FirstIntentionDay),
    ("AUL", RiskClass::FirstIntentionDay),
];

/// Which exchange date a contract's risk day is (see docs/cme_roll.md "期限保护").
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum RiskClass {
    /// Cash-settled or expiry-delivered: the last trading day.
    LastTradingDay,
    /// COMEX metals: first position day (second business day before the first business day of
    /// the delivery month), when long positions can first be assigned.
    FirstPositionDay,
    /// CBOT Treasuries: first intention day (same rule), the first day a short may declare.
    FirstIntentionDay,
}

impl RiskClass {
    pub fn as_str(self) -> &'static str {
        match self {
            Self::LastTradingDay => "last_trading_day",
            Self::FirstPositionDay => "first_position_day",
            Self::FirstIntentionDay => "first_intention_day",
        }
    }
}

pub fn risk_class(product: &str) -> Option<RiskClass> {
    PRODUCTS
        .iter()
        .find(|(p, _)| *p == product)
        .map(|(_, c)| *c)
}

/// Delivery month as `year * 12 + month0`.
#[derive(Clone, Copy, Debug, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct Month(pub i32);

impl Month {
    pub fn new(year: i32, month: u32) -> Self {
        Self(year * 12 + month as i32 - 1)
    }
    pub fn year(self) -> i32 {
        self.0.div_euclid(12)
    }
    pub fn month(self) -> u32 {
        (self.0.rem_euclid(12) + 1) as u32
    }
    /// `YYYY-MM`.
    pub fn parse(text: &str) -> Result<Self> {
        let (y, m) = text.split_once('-').context("month is not YYYY-MM")?;
        let (y, m): (i32, u32) = (y.parse()?, m.parse()?);
        if !(1..=12).contains(&m) || y < 1900 {
            bail!("bad month {text}");
        }
        Ok(Self::new(y, m))
    }
    pub fn of(day: NaiveDate) -> Self {
        Self::new(day.year(), day.month())
    }
    /// Standard RIC spelling with a two-digit year (`ESU26`).
    pub fn ric(self, root: &str) -> String {
        format!(
            "{root}{}{:02}",
            MONTH_CODES[self.month() as usize - 1],
            self.year() % 100
        )
    }
}

impl fmt::Display for Month {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{:04}-{:02}", self.year(), self.month())
    }
}

pub const MONTH_CODES: [char; 12] = ['F', 'G', 'H', 'J', 'K', 'M', 'N', 'Q', 'U', 'V', 'X', 'Z'];

pub fn month_of_code(c: char) -> Option<u32> {
    MONTH_CODES
        .iter()
        .position(|&m| m == c)
        .map(|i| i as u32 + 1)
}

/// Delivery month of an outright RIC of `root` (`TYZ26`, `TYZ6`), or `None` for anything else
/// (spreads, chains, other roots). `seen` is a month in which the contract was observed (an
/// OI trade date). A one-digit year is the first year that puts the delivery month no earlier
/// than three months before `seen`: positions can stay open after the delivery month during
/// delivery (TU: until the third business day of the following month), never longer.
pub fn parse_ric(root: &str, ric: &str, seen: Month) -> Option<Month> {
    let rest = ric.strip_prefix(root)?;
    let mut chars = rest.chars();
    let month = month_of_code(chars.next()?)?;
    let digits = chars.as_str();
    if digits.is_empty() || digits.len() > 2 || !digits.bytes().all(|b| b.is_ascii_digit()) {
        return None;
    }
    let d: i32 = digits.parse().ok()?;
    if digits.len() == 2 {
        return Some(Month::new(2000 + d, month));
    }
    let floor = Month(seen.0 - 3);
    let mut year = floor.year() - floor.year().rem_euclid(10) + d;
    while Month::new(year, month) < floor {
        year += 10;
    }
    Some(Month::new(year, month))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn parses_outrights_only() {
        let m = |y, mo| Month::new(y, mo);
        assert_eq!(parse_ric("TY", "TYZ26", m(2026, 11)), Some(m(2026, 12)));
        assert_eq!(parse_ric("TY", "TYZ6", m(2026, 11)), Some(m(2026, 12)));
        assert_eq!(parse_ric("TY", "TYH7", m(2026, 11)), Some(m(2027, 3)));
        assert_eq!(parse_ric("CL", "CLF1", m(2020, 12)), Some(m(2021, 1)));
        assert_eq!(parse_ric("ES", "ESH0", m(2020, 1)), Some(m(2020, 3)));
        assert_eq!(parse_ric("ES", "ESH0", m(2021, 1)), Some(m(2030, 3)));
        // A December contract still in delivery in January.
        assert_eq!(parse_ric("TU", "TUZ1", m(2022, 1)), Some(m(2021, 12)));
        assert_eq!(parse_ric("US", "UROZ6", m(2026, 1)), None);
        assert_eq!(parse_ric("URO", "UROZ6", m(2026, 1)), Some(m(2026, 12)));
        assert_eq!(parse_ric("ES", "ESZ6-H7", m(2026, 1)), None);
        assert_eq!(parse_ric("ES", "ESc1", m(2026, 1)), None);
        assert_eq!(parse_ric("ES", "ESZ", m(2026, 1)), None);
        assert_eq!(parse_ric("ES", "ESZ123", m(2026, 1)), None);
    }

    #[test]
    fn month_round_trips() {
        let m = Month::parse("2026-09").unwrap();
        assert_eq!(
            (m.year(), m.month(), m.to_string(), m.ric("ES")),
            (2026, 9, "2026-09".into(), "ESU26".into())
        );
        assert!(Month::parse("2026-13").is_err());
    }
}
