//! Per-contract risk days (the exchange date a held contract must be out of; docs/cme_roll.md).
use crate::contract::{month_of_code, risk_class, Month, RiskClass};
use anyhow::{bail, ensure, Context, Result};
use chrono::{Datelike, NaiveDate};
use serde::{Deserialize, Serialize};
use std::{collections::BTreeMap, path::Path};

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct RiskDay {
    pub product: String,
    /// `YYYY-MM`
    pub delivery_month: String,
    pub risk_day: NaiveDate,
    pub kind: String,
    pub source: String,
}

/// Risk days keyed by (product, delivery month).
#[derive(Clone, Debug, Default)]
pub struct RiskTable(pub BTreeMap<(String, Month), RiskDay>);

impl RiskTable {
    pub fn get(&self, product: &str, month: Month) -> Option<NaiveDate> {
        self.0
            .get(&(product.to_string(), month))
            .map(|r| r.risk_day)
    }

    pub fn insert(&mut self, r: RiskDay) -> Result<()> {
        let class =
            risk_class(&r.product).with_context(|| format!("unknown product {}", r.product))?;
        ensure!(
            class.as_str() == r.kind,
            "{} {}: kind {} is not {}",
            r.product,
            r.delivery_month,
            r.kind,
            class.as_str()
        );
        let key = (r.product.clone(), Month::parse(&r.delivery_month)?);
        if let Some(old) = self.0.get(&key) {
            bail!(
                "duplicate risk day {} {}: {} and {}",
                r.product,
                r.delivery_month,
                old.risk_day,
                r.risk_day
            );
        }
        self.0.insert(key, r);
        Ok(())
    }

    pub fn read_csv(path: &Path) -> Result<Self> {
        let mut t = Self::default();
        for r in csv::Reader::from_path(path)
            .with_context(|| format!("open {}", path.display()))?
            .deserialize()
        {
            t.insert(r?)?;
        }
        Ok(t)
    }

    pub fn write_csv(&self, path: &Path) -> Result<()> {
        let mut w = csv::Writer::from_path(path)?;
        for r in self.0.values() {
            w.serialize(r)?;
        }
        w.flush()?;
        Ok(())
    }
}

#[derive(Deserialize)]
struct EvidenceContract {
    product: String,
    delivery_month: String,
    risks: Vec<EvidenceRisk>,
}
#[derive(Deserialize)]
struct EvidenceRisk {
    risk_date: NaiveDate,
    source: String,
}

/// The ten products audited in preprocess (`deadline_path_review_v4/deadline_evidence.json`):
/// rule-derived dates with archived CME sources. Contracts without a date (years the holiday
/// table does not cover) are skipped.
pub fn from_deadline_evidence(
    path: &Path,
    months: (Month, Month),
    out: &mut RiskTable,
) -> Result<usize> {
    let all: Vec<EvidenceContract> = serde_json::from_reader(std::fs::File::open(path)?)
        .with_context(|| format!("parse {}", path.display()))?;
    let mut n = 0;
    for c in all {
        let month = Month::parse(&c.delivery_month)?;
        if month < months.0 || month > months.1 {
            continue;
        }
        let Some(first) = c.risks.iter().min_by_key(|r| r.risk_date) else {
            continue;
        };
        let class = risk_class(&c.product).with_context(|| format!("product {}", c.product))?;
        out.insert(RiskDay {
            product: c.product,
            delivery_month: c.delivery_month,
            risk_day: first.risk_date,
            kind: class.as_str().into(),
            source: format!(
                "{} (date audited 2026-10-25 from the rule and the Federal Reserve holiday table)",
                first.source.split(';').next().unwrap_or("").trim()
            ),
        })?;
        n += 1;
    }
    Ok(n)
}

/// Last trading days of ES / NQ / RTY / YM / CL from LSEG Historical Reference Data. The RIC's
/// decade is resolved from the date itself (equity index: last trading day in the delivery
/// month; CL: in the month before). Several HRD rows for one contract must agree.
pub fn from_hrd_last_trading(
    path: &Path,
    products: &[&str],
    months: (Month, Month),
    out: &mut RiskTable,
) -> Result<usize> {
    let mut reader =
        csv::Reader::from_path(path).with_context(|| format!("open {}", path.display()))?;
    let h = reader.headers()?.clone();
    let col = |name: &str| {
        h.iter()
            .position(|c| c == name)
            .with_context(|| format!("HRD has no {name:?}"))
    };
    let (ric_i, root_i, ltd_i) = (col("RIC")?, col("RIC Root")?, col("Last Trading Day")?);
    let mut found: BTreeMap<(String, Month), NaiveDate> = BTreeMap::new();
    for row in reader.records() {
        let row = row?;
        let root = &row[root_i];
        if !products.contains(&root) || row[ltd_i].is_empty() {
            continue;
        }
        let base = row[ric_i].split('^').next().unwrap_or("");
        let Some(rest) = base.strip_prefix(root) else {
            continue;
        };
        let mut chars = rest.chars();
        let Some(m) = chars.next().and_then(month_of_code) else {
            continue;
        };
        let digits = chars.as_str();
        if digits.is_empty() || digits.len() > 2 || !digits.bytes().all(|b| b.is_ascii_digit()) {
            continue;
        }
        let ltd = NaiveDate::parse_from_str(&row[ltd_i], "%m/%d/%Y")
            .with_context(|| format!("HRD {base} Last Trading Day {}", &row[ltd_i]))?;
        // Only the requested range is checked (old HRD rows have known inconsistencies, e.g.
        // CLH7 with a 2007-01-22 last trading day).
        if Month::of(ltd).0 + 1 < months.0 .0 || Month::of(ltd) > months.1 {
            continue;
        }
        // The delivery month implied by the last trading day.
        let implied = if root == "CL" {
            Month(Month::of(ltd).0 + 1)
        } else {
            Month::of(ltd)
        };
        let d: i32 = digits.parse()?;
        let year_ok = if digits.len() == 2 {
            implied.year() % 100 == d
        } else {
            implied.year() % 10 == d
        };
        if implied.month() != m || !year_ok {
            bail!("HRD {base}: last trading day {ltd} does not fit the RIC month");
        }
        if implied < months.0 || implied > months.1 {
            continue;
        }
        let key = (root.to_string(), implied);
        if let Some(prev) = found.insert(key, ltd) {
            ensure!(
                prev == ltd,
                "HRD {base}: last trading days {prev} and {ltd} disagree"
            );
        }
    }
    let n = found.len();
    for ((product, month), ltd) in found {
        out.insert(RiskDay {
            product,
            delivery_month: month.to_string(),
            risk_day: ltd,
            kind: RiskClass::LastTradingDay.as_str().into(),
            source: "LSEG HRD Last Trading Day (hrd.csv, extracted 2026-06-06)".into(),
        })?;
    }
    Ok(n)
}

/// ES / NQ / RTY / YM quarterly contracts expire on the same day (same CME rule), so a month
/// missing from HRD for one of them takes the date of the others, after checking that the
/// products HRD has agree on every month. Returns the months filled.
pub fn fill_from_siblings(out: &mut RiskTable, products: &[&str]) -> Result<Vec<String>> {
    let mut by_month: BTreeMap<Month, BTreeMap<String, NaiveDate>> = BTreeMap::new();
    for ((p, m), r) in &out.0 {
        if products.contains(&p.as_str()) {
            by_month
                .entry(*m)
                .or_default()
                .insert(p.clone(), r.risk_day);
        }
    }
    let mut filled = Vec::new();
    for (m, have) in by_month {
        let dates: Vec<NaiveDate> = have.values().copied().collect();
        ensure!(
            dates.windows(2).all(|w| w[0] == w[1]),
            "{m}: equity index last trading days disagree: {have:?}"
        );
        for p in products {
            if !have.contains_key(*p) {
                let (src, d) = have.iter().next().unwrap();
                out.insert(RiskDay {
                    product: (*p).into(),
                    delivery_month: m.to_string(),
                    risk_day: *d,
                    kind: RiskClass::LastTradingDay.as_str().into(),
                    source: format!("same expiry as {src} {m} (LSEG HRD; CME equity index quarterly rule); {p} missing in HRD"),
                })?;
                filled.push(format!("{p} {m}"));
            }
        }
    }
    Ok(filled)
}

/// GC: COMEX Rule 7 defines the first position day the same way for every COMEX metal, so it
/// is HG's date for the same delivery month.
pub fn gc_from_hg(out: &mut RiskTable) -> Result<usize> {
    let hg: Vec<RiskDay> = out
        .0
        .values()
        .filter(|r| r.product == "HG")
        .cloned()
        .collect();
    for r in &hg {
        out.insert(RiskDay {
            product: "GC".into(),
            delivery_month: r.delivery_month.clone(),
            risk_day: r.risk_day,
            kind: RiskClass::FirstPositionDay.as_str().into(),
            source: format!(
                "COMEX Rule 7 first position day, same as HG {}",
                r.delivery_month
            ),
        })?;
    }
    Ok(hg.len())
}

/// First intention day (CBOT Treasuries) / first position day (COMEX metals): the second
/// business day before the first business day of the delivery month. `business_days` is the
/// exchange trading-day calendar (ascending). `None` if the calendar does not cover the month.
pub fn second_business_day_before_month(
    business_days: &[NaiveDate],
    month: Month,
) -> Option<NaiveDate> {
    let start = NaiveDate::from_ymd_opt(month.year(), month.month(), 1)?;
    let i = business_days.partition_point(|d| *d < start);
    if i >= business_days.len() || i < 2 || Month::of(business_days[i]) != month {
        return None;
    }
    Some(business_days[i - 2])
}

/// CME FX (URO, JY): trading terminates two business days before the third Wednesday of the
/// contract month. `None` if the calendar does not cover it.
pub fn fx_last_trading_day(business_days: &[NaiveDate], month: Month) -> Option<NaiveDate> {
    let first = NaiveDate::from_ymd_opt(month.year(), month.month(), 1)?;
    let to_wed = (2 + 7 - first.weekday().num_days_from_monday()) % 7;
    let wed = first + chrono::Duration::days((to_wed + 14) as i64);
    let i = business_days.partition_point(|d| *d < wed);
    if i < 2 || business_days.last().is_none_or(|l| *l < wed) {
        return None;
    }
    Some(business_days[i - 2])
}

/// The rule that defines a product's risk day from a business-day calendar, if any.
pub fn rule_for(
    product: &str,
) -> Option<(&'static str, fn(&[NaiveDate], Month) -> Option<NaiveDate>)> {
    match risk_class(product)? {
        RiskClass::FirstIntentionDay | RiskClass::FirstPositionDay => Some((
            "second business day before the first business day of the delivery month",
            second_business_day_before_month,
        )),
        RiskClass::LastTradingDay if product == "URO" || product == "JY" => Some((
            "two business days before the third Wednesday of the delivery month",
            fx_last_trading_day,
        )),
        RiskClass::LastTradingDay => None,
    }
}

/// Fills the months of `product` that have no date yet with its rule, but only if the rule
/// reproduces every existing (audited) date of that product the calendar covers.
/// Returns (dates checked, mismatches, dates added).
pub fn fill_by_rule(
    out: &mut RiskTable,
    product: &str,
    business_days: &[NaiveDate],
    months: &[Month],
) -> Result<(usize, Vec<String>, usize)> {
    let class = risk_class(product).context("product")?;
    let (text, rule) =
        rule_for(product).with_context(|| format!("{product} has no calendar rule"))?;
    let mut checked = 0;
    let mut mismatches = Vec::new();
    for ((p, m), r) in &out.0 {
        if p != product {
            continue;
        }
        if let Some(d) = rule(business_days, *m) {
            checked += 1;
            if d != r.risk_day {
                mismatches.push(format!("{p} {m}: rule {d}, audited {}", r.risk_day));
            }
        }
    }
    let mut added = 0;
    if mismatches.is_empty() {
        for &m in months {
            if out.get(product, m).is_some() {
                continue;
            }
            if let Some(d) = rule(business_days, m) {
                out.insert(RiskDay {
                    product: product.into(),
                    delivery_month: m.to_string(),
                    risk_day: d,
                    kind: class.as_str().into(),
                    source: format!(
                        "rule: {text} (business days = weekdays minus Federal Reserve holidays and Good Friday); reproduces all {checked} audited dates"
                    ),
                })?;
                added += 1;
            }
        }
    }
    Ok((checked, mismatches, added))
}

/// Third Friday of a month (equity index last trading day rule), for cross-checks.
pub fn third_friday(month: Month) -> NaiveDate {
    let first = NaiveDate::from_ymd_opt(month.year(), month.month(), 1).unwrap();
    let to_friday = (4 + 7 - first.weekday().num_days_from_monday()) % 7;
    first + chrono::Duration::days((to_friday + 14) as i64)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn fx_rule() {
        let cal = crate::calendar::projected_business_days(
            NaiveDate::from_ymd_opt(2026, 9, 1).unwrap(),
            NaiveDate::from_ymd_opt(2026, 9, 30).unwrap(),
        );
        // Third Wednesday 2026-09-16 -> Monday 09-14 (as audited for UROU26).
        assert_eq!(
            fx_last_trading_day(&cal, Month::new(2026, 9)),
            NaiveDate::from_ymd_opt(2026, 9, 14)
        );
    }

    #[test]
    fn third_fridays() {
        assert_eq!(
            third_friday(Month::new(2026, 9)),
            NaiveDate::from_ymd_opt(2026, 9, 18).unwrap()
        );
        assert_eq!(
            third_friday(Month::new(2026, 5)),
            NaiveDate::from_ymd_opt(2026, 5, 15).unwrap()
        );
        assert_eq!(
            third_friday(Month::new(2020, 3)),
            NaiveDate::from_ymd_opt(2020, 3, 20).unwrap()
        );
    }

    #[test]
    fn table_rejects_duplicates_and_wrong_kind() {
        let mut t = RiskTable::default();
        let r = RiskDay {
            product: "TY".into(),
            delivery_month: "2026-09".into(),
            risk_day: NaiveDate::from_ymd_opt(2026, 8, 28).unwrap(),
            kind: "first_intention_day".into(),
            source: "x".into(),
        };
        t.insert(r.clone()).unwrap();
        assert!(t.insert(r.clone()).is_err());
        let mut wrong = r;
        wrong.delivery_month = "2026-12".into();
        wrong.kind = "last_trading_day".into();
        assert!(t.insert(wrong).is_err());
        assert_eq!(
            t.get("TY", Month::new(2026, 9)),
            NaiveDate::from_ymd_opt(2026, 8, 28)
        );
    }
}
