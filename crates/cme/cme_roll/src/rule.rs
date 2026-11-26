//! The unified CME dominant rule (docs/cme_roll.md), as a pure day-by-day engine.
//!
//! For trading day E the decision cutoff is `session_open(E) - 5 min`, with
//! `session_open(E)` = 17:00 America/Chicago on the calendar day before E. The caller feeds
//! every OI observation published at or before the cutoff (`observe`) and then calls
//! `decide`; nothing later can reach a decision. Each contract's value is compared on one
//! common trade date D*: the latest date the current dominant has a published value for.
//!
//! - **OI switch:** a later, deadline-safe contract with OI(D*) > threshold x current OI(D*)
//!   becomes dominant on E (largest OI; tie -> nearer month). Only later months; never back.
//! - **Deadline guard:** from the trading day `guard_days` before the current contract's risk
//!   day, the largest-OI later safe contract takes over without the threshold.
use crate::contract::Month;
use crate::risk::RiskTable;
use chrono::{Duration, NaiveDate, NaiveTime, TimeZone};
use chrono_tz::America::Chicago;
use serde::Serialize;
use std::collections::{BTreeMap, HashMap};

#[derive(Clone, Debug)]
pub struct RuleConfig {
    pub threshold: f64,
    pub guard_days: usize,
    /// Off only for the counterfactual "OI alone" path of the evidence report.
    pub guard: bool,
    /// Minutes before the session open at which the decision is taken.
    pub cutoff_minutes: i64,
    /// Without the guard: a current contract whose latest value is older than this many
    /// trading days is treated as gone (expired) and replaced.
    pub stale_days: usize,
    /// Only the next listed later month is a candidate (instead of the largest-OI later
    /// month). For CL, whose December contracts carry OI as large as the front month but trade about a tenth as much (docs/cme_roll.md section 4).
    pub next_only: bool,
}

impl Default for RuleConfig {
    fn default() -> Self {
        Self {
            threshold: 1.1,
            guard_days: 1,
            guard: true,
            cutoff_minutes: 5,
            stale_days: 5,
            next_only: false,
        }
    }
}

/// Trading days, ascending.
#[derive(Clone, Debug)]
pub struct Calendar(pub Vec<NaiveDate>);

impl Calendar {
    pub fn index_of(&self, d: NaiveDate) -> Option<usize> {
        self.0.binary_search(&d).ok()
    }
    /// The `n`-th trading day strictly before `d` (n >= 1).
    pub fn nth_before(&self, d: NaiveDate, n: usize) -> Option<NaiveDate> {
        let idx = self.0.partition_point(|x| *x < d);
        (n >= 1 && idx >= n).then(|| self.0[idx - n])
    }
}

/// 17:00 America/Chicago on the calendar day before trading day `e`, as UTC ns.
pub fn session_open_ns(e: NaiveDate) -> i64 {
    let local = (e - Duration::days(1)).and_time(NaiveTime::from_hms_opt(17, 0, 0).unwrap());
    Chicago
        .from_local_datetime(&local)
        .single()
        .expect("17:00 Chicago is never ambiguous")
        .timestamp_nanos_opt()
        .unwrap()
}

pub fn cutoff_ns(cfg: &RuleConfig, e: NaiveDate) -> i64 {
    session_open_ns(e) - cfg.cutoff_minutes * 60_000_000_000
}

#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum Reason {
    /// First day: largest OI among safe contracts.
    Seed,
    Hold,
    /// OI of a later contract exceeded threshold x current.
    Oi,
    /// Deadline guard reached.
    Guard,
    /// Counterfactual path only: the current contract stopped publishing (expired).
    Expired,
    /// The guard is reached but no later safe contract has a value: no switch possible.
    Blocked,
}

#[derive(Clone, Debug, PartialEq)]
pub struct Decision {
    pub day: NaiveDate,
    pub contract: Option<Month>,
    pub reason: Reason,
    /// Common trade date the comparison used.
    pub oi_date: Option<NaiveDate>,
    /// Trading days from `oi_date` to the decided day (2 when nothing is missing).
    pub lag: Option<usize>,
    pub current_oi: Option<i64>,
    pub best: Option<(Month, i64)>,
}

pub struct Engine {
    cfg: RuleConfig,
    product: String,
    /// Latest published value per (contract, trade date); only recent dates are kept.
    known: HashMap<Month, BTreeMap<NaiveDate, i64>>,
    current: Option<Month>,
}

const KEEP_DATES: usize = 16;

impl Engine {
    pub fn new(product: &str, cfg: RuleConfig) -> Self {
        Self {
            cfg,
            product: product.into(),
            known: HashMap::new(),
            current: None,
        }
    }

    pub fn current(&self) -> Option<Month> {
        self.current
    }

    /// One observation published at or before the next decision's cutoff. A later
    /// publication for the same (contract, date) replaces the earlier one (final after
    /// preliminary).
    pub fn observe(&mut self, month: Month, oi_date: NaiveDate, value: i64) {
        let dates = self.known.entry(month).or_default();
        dates.insert(oi_date, value);
        while dates.len() > KEEP_DATES {
            dates.pop_first();
        }
    }

    /// The day `guard_days` trading days before the contract's risk day. A risk day after the
    /// end of the calendar is returned as is: its guard day is later than every calendar day.
    pub fn guard_day(&self, cal: &Calendar, risk: &RiskTable, m: Month) -> Option<NaiveDate> {
        let b = risk.get(&self.product, m)?;
        if cal.0.last().is_none_or(|last| b > *last) {
            return Some(b);
        }
        cal.nth_before(b, self.cfg.guard_days)
    }

    pub fn decide(&mut self, cal: &Calendar, e_idx: usize, risk: &RiskTable) -> Decision {
        let e = cal.0[e_idx];
        // A contract without a known risk day cannot be guarded and is never chosen.
        let safe = |m: Month| self.guard_day(cal, risk, m).is_some_and(|r| e < r);
        let value_at = |m: Month, d: NaiveDate| self.known.get(&m).and_then(|v| v.get(&d)).copied();
        let lag_of = |d: NaiveDate| cal.index_of(d).map(|i| e_idx.saturating_sub(i));
        let best_at = |d: NaiveDate, after: Option<Month>| {
            let mut best: Option<(Month, i64)> = None;
            for (&m, dates) in &self.known {
                if after.is_some_and(|c| m <= c) || !safe(m) {
                    continue;
                }
                if let Some(&v) = dates.get(&d) {
                    let better = if self.cfg.next_only {
                        best.is_none_or(|(bm, _)| m < bm)
                    } else {
                        best.is_none_or(|(bm, bv)| v > bv || (v == bv && m < bm))
                    };
                    if better {
                        best = Some((m, v));
                    }
                }
            }
            best
        };
        let Some(cur) = self.current else {
            // Seed: the latest trade date any safe contract has a value for.
            let latest = self
                .known
                .iter()
                .filter(|(m, _)| safe(**m))
                .filter_map(|(_, d)| d.keys().next_back().copied())
                .max();
            let best = latest.and_then(|d| best_at(d, None));
            self.current = best.map(|(m, _)| m);
            return Decision {
                day: e,
                contract: self.current,
                reason: if best.is_some() {
                    Reason::Seed
                } else {
                    Reason::Blocked
                },
                oi_date: latest,
                lag: latest.and_then(lag_of),
                current_oi: best.map(|(_, v)| v),
                best,
            };
        };
        let d_star = self
            .known
            .get(&cur)
            .and_then(|v| v.keys().next_back().copied());
        let lag = d_star.and_then(lag_of);
        let current_oi = d_star.and_then(|d| value_at(cur, d));
        let best = d_star.and_then(|d| best_at(d, Some(cur)));
        let guarded = self.cfg.guard && self.guard_day(cal, risk, cur).is_none_or(|r| e >= r);
        let stale = lag.is_none_or(|l| l > self.cfg.stale_days);
        let oi_met = matches!((best, current_oi),
            (Some((_, v)), Some(c)) if (v as f64) > self.cfg.threshold * c as f64);
        // `Guard` only when the guard changes the outcome; on a day both hold it is `Oi`.
        let reason = if oi_met {
            Reason::Oi
        } else if guarded {
            if best.is_some() {
                Reason::Guard
            } else {
                Reason::Blocked
            }
        } else if !self.cfg.guard && stale {
            if best.is_some() {
                Reason::Expired
            } else {
                Reason::Blocked
            }
        } else {
            Reason::Hold
        };
        if matches!(reason, Reason::Oi | Reason::Guard | Reason::Expired) {
            self.current = best.map(|(m, _)| m);
        }
        Decision {
            day: e,
            contract: self.current,
            reason,
            oi_date: d_star,
            lag,
            current_oi,
            best,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::risk::RiskDay;

    fn d(s: &str) -> NaiveDate {
        NaiveDate::parse_from_str(s, "%Y-%m-%d").unwrap()
    }

    fn setup() -> (Calendar, RiskTable) {
        let cal = Calendar(
            [
                "2026-08-20",
                "2026-08-21",
                "2026-08-24",
                "2026-08-25",
                "2026-08-26",
                "2026-08-27",
                "2026-08-28",
                "2026-08-31",
            ]
            .iter()
            .map(|s| d(s))
            .collect(),
        );
        let mut risk = RiskTable::default();
        for (m, r) in [("2026-09", "2026-08-28"), ("2026-12", "2026-11-27")] {
            risk.insert(RiskDay {
                product: "TY".into(),
                delivery_month: m.into(),
                risk_day: d(r),
                kind: "first_intention_day".into(),
                source: "t".into(),
            })
            .unwrap();
        }
        (cal, risk)
    }

    #[test]
    fn cutoff_is_five_minutes_before_17_chicago_previous_day() {
        // 2026-08-27 trading day opens 2026-08-26 17:00 CDT = 22:00 UTC.
        let open = session_open_ns(d("2026-08-27"));
        assert_eq!(
            open,
            d("2026-08-26")
                .and_hms_opt(22, 0, 0)
                .unwrap()
                .and_utc()
                .timestamp_nanos_opt()
                .unwrap()
        );
        assert_eq!(
            cutoff_ns(&RuleConfig::default(), d("2026-08-27")),
            open - 300_000_000_000
        );
        // Winter: CST, 23:00 UTC.
        let w = session_open_ns(d("2026-12-01"));
        assert_eq!(
            w,
            d("2026-11-30")
                .and_hms_opt(23, 0, 0)
                .unwrap()
                .and_utc()
                .timestamp_nanos_opt()
                .unwrap()
        );
    }

    #[test]
    fn oi_threshold_and_forward_only() {
        let (cal, risk) = setup();
        let (u, z) = (Month::new(2026, 9), Month::new(2026, 12));
        let mut e = Engine::new("TY", RuleConfig::default());
        e.observe(u, d("2026-08-20"), 100);
        e.observe(z, d("2026-08-20"), 50);
        assert_eq!(e.decide(&cal, 2, &risk).reason, Reason::Seed);
        assert_eq!(e.current(), Some(u));
        // 1.1x exactly is not enough.
        e.observe(u, d("2026-08-21"), 100);
        e.observe(z, d("2026-08-21"), 110);
        let x = e.decide(&cal, 3, &risk);
        assert_eq!(
            (x.reason, x.oi_date, x.lag),
            (Reason::Hold, Some(d("2026-08-21")), Some(2))
        );
        e.observe(u, d("2026-08-24"), 100);
        e.observe(z, d("2026-08-24"), 111);
        let x = e.decide(&cal, 4, &risk);
        assert_eq!((x.reason, x.contract), (Reason::Oi, Some(z)));
        // Never back to an earlier month even if its OI is larger again.
        e.observe(u, d("2026-08-25"), 1000);
        e.observe(z, d("2026-08-25"), 10);
        assert_eq!(e.decide(&cal, 5, &risk).contract, Some(z));
    }

    #[test]
    fn guard_forces_the_switch_one_trading_day_before_the_risk_day() {
        let (cal, risk) = setup();
        let (u, z) = (Month::new(2026, 9), Month::new(2026, 12));
        let mut e = Engine::new("TY", RuleConfig::default());
        e.observe(u, d("2026-08-20"), 100);
        e.observe(z, d("2026-08-20"), 10);
        e.decide(&cal, 2, &risk);
        for (i, day) in [(3, "2026-08-21"), (4, "2026-08-24")] {
            e.observe(u, d(day), 100);
            e.observe(z, d(day), 20);
            assert_eq!(e.decide(&cal, i, &risk).reason, Reason::Hold);
        }
        // Risk day 08-28 -> guard day 08-27 (index 5): switch without the threshold.
        let x = e.decide(&cal, 5, &risk);
        assert_eq!(
            (x.day, x.reason, x.contract),
            (d("2026-08-27"), Reason::Guard, Some(z))
        );
    }

    #[test]
    fn next_only_ignores_a_larger_farther_month() {
        let (cal, mut risk) = setup();
        risk.insert(RiskDay {
            product: "TY".into(),
            delivery_month: "2027-03".into(),
            risk_day: d("2027-02-25"),
            kind: "first_intention_day".into(),
            source: "t".into(),
        })
        .unwrap();
        let (u, z, h) = (
            Month::new(2026, 9),
            Month::new(2026, 12),
            Month::new(2027, 3),
        );
        for next_only in [false, true] {
            let cfg = RuleConfig {
                next_only,
                ..RuleConfig::default()
            };
            let mut e = Engine::new("TY", cfg);
            e.observe(u, d("2026-08-20"), 100);
            e.decide(&cal, 2, &risk);
            e.observe(u, d("2026-08-21"), 100);
            e.observe(z, d("2026-08-21"), 120);
            e.observe(h, d("2026-08-21"), 150);
            let x = e.decide(&cal, 3, &risk);
            assert_eq!(
                x.contract,
                Some(if next_only { z } else { h }),
                "next_only={next_only}"
            );
        }
    }

    #[test]
    fn a_contract_past_its_guard_day_is_never_a_candidate() {
        let (cal, risk) = setup();
        let (u, z) = (Month::new(2026, 9), Month::new(2026, 12));
        let mut e = Engine::new("TY", RuleConfig::default());
        e.observe(u, d("2026-08-26"), 1000);
        e.observe(z, d("2026-08-26"), 10);
        // Seed on 08-28: U is past its guard day 08-27, so Z is chosen despite smaller OI.
        let x = e.decide(&cal, 6, &risk);
        assert_eq!((x.reason, x.contract), (Reason::Seed, Some(z)));
    }

    #[test]
    fn without_guard_an_expired_contract_is_replaced() {
        let (cal, risk) = setup();
        let (u, z) = (Month::new(2026, 9), Month::new(2026, 12));
        let cfg = RuleConfig {
            guard: false,
            stale_days: 2,
            ..RuleConfig::default()
        };
        let mut e = Engine::new("TY", cfg);
        e.observe(u, d("2026-08-20"), 100);
        e.observe(z, d("2026-08-20"), 10);
        e.decide(&cal, 1, &risk);
        assert_eq!(e.decide(&cal, 2, &risk).reason, Reason::Hold);
        // Same values, but now 08-20 is 4 trading days old: U is treated as gone.
        let x = e.decide(&cal, 5, &risk);
        assert_eq!((x.reason, x.contract), (Reason::Expired, Some(z)));
    }
}
