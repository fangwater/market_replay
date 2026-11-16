//! Port of `cn_futures/adjustment.py` and `cn_futures/continuous.py`.

use crate::dominant::{ContractDay, DominantDay};
use chrono::NaiveDate;
use std::collections::HashMap;

pub const ADJUST_METHOD: &str = "prev_close_spread";

/// One roll's additive 复权因子 (米筐 prev_close_spread).
#[derive(Clone, Debug, PartialEq)]
pub struct AdjustmentFactor {
    pub product_id: String,
    pub effective_trading_day: NaiveDate,
    pub from_instrument: String,
    pub to_instrument: String,
    pub reference_trading_day: NaiveDate,
    pub old_reference_price: Option<f64>,
    pub new_reference_price: Option<f64>,
    pub adjustment_method: &'static str,
    pub adjustment_value: Option<f64>,
    pub cumulative_factor: Option<f64>,
    pub skipped: bool,
    pub skip_reason: &'static str,
}

#[derive(Clone, Debug, PartialEq)]
pub struct ContinuousPrice {
    pub trading_day: NaiveDate,
    pub product_id: String,
    pub instrument_id: String,
    pub raw_close: Option<f64>,
    pub price_adjustment: Option<f64>,
    pub adjusted_close: Option<f64>,
    pub adjustment_complete: bool,
}

#[derive(Clone, Debug, PartialEq)]
pub struct ContinuityStatus {
    pub product_id: String,
    pub first_trading_day: Option<NaiveDate>,
    pub last_trading_day: Option<NaiveDate>,
    pub first_assigned_trading_day: Option<NaiveDate>,
    pub observed_days: usize,
    pub assigned_days: usize,
    pub unassigned_prefix_days: usize,
    pub unassigned_after_start_days: usize,
    pub rolls: usize,
    pub adjustment_rows: usize,
    pub skipped_adjustments: usize,
    pub missing_dominant_closes: usize,
    pub mapping_continuous: bool,
    pub price_continuous: bool,
    pub continuous: bool,
    pub first_failure_day: Option<NaiveDate>,
    pub failure_reason: &'static str,
}

fn finite(value: Option<f64>) -> bool {
    matches!(value, Some(v) if !v.is_nan())
}

fn finite_positive(value: Option<f64>) -> bool {
    matches!(value, Some(v) if !v.is_nan() && v > 0.0)
}

/// `{(trad_day, instrument_id): close}`; keys are unique after canonicalization.
fn close_index(rows: &[ContractDay]) -> HashMap<(NaiveDate, &str), Option<f64>> {
    rows.iter()
        .map(|row| ((row.trad_day, row.instrument_id.as_str()), row.close))
        .collect()
}

fn resolve_product(product_id: &str, dominants: &[DominantDay]) -> String {
    if !product_id.is_empty() {
        return product_id.to_owned();
    }
    dominants
        .iter()
        .find(|day| !day.product_id.is_empty())
        .map(|day| day.product_id.clone())
        .unwrap_or_default()
}

/// Emit a 复权因子 at each roll; the latest segment stays unmoved.
pub fn compute_adjustment_factors(
    rows: &[ContractDay],
    dominants: &[DominantDay],
    product_id: &str,
) -> Vec<AdjustmentFactor> {
    let resolved_product = resolve_product(product_id, dominants);
    let closes = close_index(rows);
    // `_close_on`: the close if finite, else None (also None when absent).
    let close_on = |day: NaiveDate, instrument: &str| -> Option<f64> {
        closes
            .get(&(day, instrument))
            .copied()
            .flatten()
            .filter(|v| !v.is_nan())
    };
    let mut rolls: Vec<AdjustmentFactor> = Vec::new();
    let mut previous: Option<&DominantDay> = None;
    for day in dominants {
        if let (Some(prev), Some(to)) = (previous, day.instrument_id.as_deref()) {
            if let Some(from) = prev.instrument_id.as_deref() {
                if !to.is_empty() && !from.is_empty() && to != from {
                    let old_px = close_on(prev.trad_day, from);
                    let new_px = close_on(prev.trad_day, to);
                    let valid =
                        matches!((old_px, new_px), (Some(o), Some(n)) if o > 0.0 && n > 0.0);
                    rolls.push(AdjustmentFactor {
                        product_id: resolved_product.clone(),
                        effective_trading_day: day.trad_day,
                        from_instrument: from.to_owned(),
                        to_instrument: to.to_owned(),
                        reference_trading_day: prev.trad_day,
                        old_reference_price: old_px,
                        new_reference_price: new_px,
                        adjustment_method: ADJUST_METHOD,
                        adjustment_value: if valid {
                            Some(new_px.unwrap() - old_px.unwrap())
                        } else {
                            None
                        },
                        cumulative_factor: None,
                        skipped: !valid,
                        skip_reason: if valid { "" } else { "missing_reference_price" },
                    });
                }
            }
        }
        previous = Some(day);
    }

    // Sum from the latest roll backwards; a skipped later roll makes every
    // earlier cumulative value unknown.
    let mut running = 0.0_f64;
    let mut complete = true;
    for factor in rolls.iter_mut().rev() {
        if factor.skipped {
            complete = false;
            continue;
        }
        if complete {
            running += factor.adjustment_value.unwrap();
        }
        factor.cumulative_factor = complete.then_some(running);
    }
    rolls
}

/// Additive shift that leaves the latest segment unmoved (forward-order sum).
pub fn cumulative_gap_before(factors: &[AdjustmentFactor], trad_day: NaiveDate) -> f64 {
    let mut shift = 0.0_f64;
    for factor in factors {
        if factor.skipped || !finite(factor.adjustment_value) {
            continue;
        }
        if trad_day < factor.effective_trading_day {
            shift += factor.adjustment_value.unwrap();
        }
    }
    shift
}

pub fn adjustment_complete_before(factors: &[AdjustmentFactor], trading_day: NaiveDate) -> bool {
    !factors
        .iter()
        .any(|factor| factor.skipped && trading_day < factor.effective_trading_day)
}

pub fn build_continuous_prices(
    rows: &[ContractDay],
    dominants: &[DominantDay],
    factors: &[AdjustmentFactor],
    product_id: &str,
) -> Vec<ContinuousPrice> {
    let closes = close_index(rows);
    let resolved_product = resolve_product(product_id, dominants);
    let mut out = Vec::new();
    for dominant in dominants {
        let Some(instrument) = dominant.instrument_id.as_deref().filter(|v| !v.is_empty()) else {
            continue;
        };
        let raw_close = closes
            .get(&(dominant.trad_day, instrument))
            .copied()
            .flatten();
        let complete = adjustment_complete_before(factors, dominant.trad_day);
        let adjustment = complete.then(|| cumulative_gap_before(factors, dominant.trad_day));
        let positive = finite_positive(raw_close);
        let adjusted_close = match adjustment {
            Some(shift) if positive => Some(raw_close.unwrap() + shift),
            _ => None,
        };
        out.push(ContinuousPrice {
            trading_day: dominant.trad_day,
            product_id: resolved_product.clone(),
            instrument_id: instrument.to_owned(),
            raw_close: raw_close.filter(|_| positive),
            price_adjustment: adjustment,
            adjusted_close,
            adjustment_complete: complete,
        });
    }
    out
}

/// `prices` must be `build_continuous_prices` of the same inputs (Python
/// recomputes them inside `audit_continuity`).
pub fn audit_continuity(
    dominants: &[DominantDay],
    factors: &[AdjustmentFactor],
    prices: &[ContinuousPrice],
    product_id: &str,
) -> ContinuityStatus {
    let resolved_product = resolve_product(product_id, dominants);
    let assigned = |day: &DominantDay| day.instrument_id.as_deref().is_some_and(|v| !v.is_empty());
    let first_day = dominants.first().map(|day| day.trad_day);
    let last_day = dominants.last().map(|day| day.trad_day);
    let first_assigned_index = dominants.iter().position(assigned);
    let assigned_days = dominants.iter().filter(|day| assigned(day)).count();
    let prefix = first_assigned_index.unwrap_or(dominants.len());
    let after_start_unassigned = first_assigned_index.map_or(0, |index| {
        dominants[index..]
            .iter()
            .filter(|day| !assigned(day))
            .count()
    });

    let mut failures: Vec<(NaiveDate, &'static str)> = Vec::new();
    if first_assigned_index.is_none() {
        if let Some(first) = first_day {
            failures.push((first, "no_dominant_contract_assignment"));
        }
    }
    if after_start_unassigned > 0 {
        let day = dominants[first_assigned_index.unwrap_or(0)..]
            .iter()
            .find(|day| !assigned(day))
            .unwrap()
            .trad_day;
        failures.push((day, "unassigned_after_initial"));
    }

    let mut expected_rolls: Vec<(&DominantDay, &DominantDay)> = Vec::new();
    let mut previous: Option<&DominantDay> = None;
    for day in dominants {
        if let Some(prev) = previous {
            if assigned(prev) && assigned(day) && prev.instrument_id != day.instrument_id {
                expected_rolls.push((prev, day));
            }
        }
        previous = Some(day);
    }

    let factor_by_effective: HashMap<NaiveDate, &AdjustmentFactor> = factors
        .iter()
        .map(|factor| (factor.effective_trading_day, factor))
        .collect();
    if factors.len() != expected_rolls.len() {
        let failure_day = expected_rolls
            .first()
            .map(|(_, current)| current.trad_day)
            .or(first_day)
            .unwrap_or_else(|| NaiveDate::from_ymd_opt(1, 1, 1).unwrap()); // date.min
        failures.push((failure_day, "roll_adjustment_count_mismatch"));
    }
    for (prev, current) in &expected_rolls {
        let Some(factor) = factor_by_effective.get(&current.trad_day) else {
            failures.push((current.trad_day, "missing_adjustment_row"));
            continue;
        };
        if Some(factor.from_instrument.as_str()) != prev.instrument_id.as_deref()
            || Some(factor.to_instrument.as_str()) != current.instrument_id.as_deref()
            || factor.reference_trading_day != prev.trad_day
        {
            failures.push((current.trad_day, "adjustment_mapping_mismatch"));
            continue;
        }
        if factor.skipped
            || !finite_positive(factor.old_reference_price)
            || !finite_positive(factor.new_reference_price)
        {
            failures.push((current.trad_day, "missing_reference_price"));
            continue;
        }
        // Python: `value is None or abs(old + value - new) > 1e-9` fails
        // (a NaN residual compares false, so it does not fail).
        let does_not_close = factor.adjustment_value.is_none_or(|value| {
            (factor.old_reference_price.unwrap() + value - factor.new_reference_price.unwrap())
                .abs()
                > 1e-9
        });
        if does_not_close {
            failures.push((current.trad_day, "reference_spread_does_not_close"));
        }
    }

    let missing_closes = prices
        .iter()
        .filter(|price| price.raw_close.is_none())
        .count();
    for price in prices {
        if price.raw_close.is_none() {
            failures.push((price.trading_day, "missing_dominant_close"));
        }
    }

    let mapping_continuous = first_assigned_index.is_some()
        && after_start_unassigned == 0
        && factors.len() == expected_rolls.len();
    let price_continuous = first_assigned_index.is_some()
        && !failures.iter().any(|(_, reason)| {
            matches!(
                *reason,
                "no_dominant_contract_assignment"
                    | "missing_adjustment_row"
                    | "adjustment_mapping_mismatch"
                    | "missing_reference_price"
                    | "reference_spread_does_not_close"
                    | "missing_dominant_close"
            )
        });
    failures.sort();
    ContinuityStatus {
        product_id: resolved_product,
        first_trading_day: first_day,
        last_trading_day: last_day,
        first_assigned_trading_day: first_assigned_index.map(|index| dominants[index].trad_day),
        observed_days: dominants.len(),
        assigned_days,
        unassigned_prefix_days: prefix,
        unassigned_after_start_days: after_start_unassigned,
        rolls: expected_rolls.len(),
        adjustment_rows: factors.len(),
        skipped_adjustments: factors.iter().filter(|factor| factor.skipped).count(),
        missing_dominant_closes: missing_closes,
        mapping_continuous,
        price_continuous,
        continuous: mapping_continuous && price_continuous,
        first_failure_day: failures.first().map(|(day, _)| *day),
        failure_reason: failures.first().map_or("", |(_, reason)| reason),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::dominant::{select_dominants, Rule};

    fn d(y: i32, m: u32, day: u32) -> NaiveDate {
        NaiveDate::from_ymd_opt(y, m, day).unwrap()
    }

    fn row(day: NaiveDate, instrument: &str, open_int: f64, close: Option<f64>) -> ContractDay {
        ContractDay {
            trad_day: day,
            instrument_id: instrument.to_owned(),
            open_int: Some(open_int),
            volume: Some(0.0),
            close,
            product_id: "RB".to_owned(),
            expired: false,
        }
    }

    fn run(
        rows: &[ContractDay],
    ) -> (
        Vec<AdjustmentFactor>,
        Vec<ContinuousPrice>,
        ContinuityStatus,
    ) {
        let dominants = select_dominants(rows, "RB", Rule::Rqdata0);
        let factors = compute_adjustment_factors(rows, &dominants, "RB");
        let prices = build_continuous_prices(rows, &dominants, &factors, "RB");
        let status = audit_continuity(&dominants, &factors, &prices, "RB");
        (factors, prices, status)
    }

    #[test]
    fn two_rolls_accumulate_backwards_and_latest_segment_is_unmoved() {
        let (t1, t2, t3, t4, t5, t6) = (
            d(2025, 1, 2),
            d(2025, 1, 3),
            d(2025, 1, 6),
            d(2025, 1, 7),
            d(2025, 1, 8),
            d(2025, 1, 9),
        );
        let rows = vec![
            row(t1, "rb2501", 100.0, Some(3500.0)),
            row(t1, "rb2505", 10.0, Some(3600.0)),
            row(t2, "rb2501", 100.0, Some(3510.0)),
            row(t2, "rb2505", 200.0, Some(3610.5)),
            row(t3, "rb2505", 200.0, Some(3620.0)),
            row(t3, "rb2510", 300.0, Some(3700.25)),
            row(t4, "rb2505", 200.0, Some(3630.0)),
            row(t4, "rb2510", 300.0, Some(3710.0)),
            row(t5, "rb2510", 300.0, Some(3720.0)),
            row(t6, "rb2510", 300.0, Some(3730.0)),
        ];
        let (factors, prices, status) = run(&rows);
        assert_eq!(factors.len(), 2);
        assert_eq!(factors[0].adjustment_value, Some(100.5));
        assert_eq!(factors[1].adjustment_value, Some(80.25));
        assert_eq!(factors[1].cumulative_factor, Some(80.25));
        assert_eq!(factors[0].cumulative_factor, Some(180.75));
        let last = prices.last().unwrap();
        assert_eq!(last.price_adjustment, Some(0.0));
        assert_eq!(last.adjusted_close, Some(3730.0));
        let first = &prices[0];
        assert_eq!(first.instrument_id, "rb2501");
        assert_eq!(first.price_adjustment, Some(180.75));
        assert!(status.continuous, "{status:?}");
        assert_eq!(status.rolls, 2);
    }

    #[test]
    fn missing_reference_price_skips_factor_and_blocks_earlier_history() {
        let (t1, t2, t3) = (d(2025, 1, 2), d(2025, 1, 3), d(2025, 1, 6));
        let rows = vec![
            row(t1, "rb2501", 100.0, Some(3500.0)),
            row(t1, "rb2505", 10.0, Some(3600.0)),
            row(t2, "rb2501", 100.0, Some(3510.0)),
            row(t2, "rb2505", 200.0, None),
            row(t3, "rb2505", 200.0, Some(3620.0)),
        ];
        let (factors, prices, status) = run(&rows);
        assert_eq!(factors.len(), 1);
        assert!(factors[0].skipped);
        assert_eq!(factors[0].skip_reason, "missing_reference_price");
        assert_eq!(factors[0].old_reference_price, Some(3510.0));
        assert_eq!(factors[0].new_reference_price, None);
        assert_eq!(factors[0].cumulative_factor, None);
        assert!(!prices[0].adjustment_complete);
        assert_eq!(prices[0].price_adjustment, None);
        assert_eq!(prices[0].adjusted_close, None);
        assert!(prices[1].adjustment_complete);
        assert!(!status.price_continuous);
        assert!(status.mapping_continuous);
        assert_eq!(status.failure_reason, "missing_reference_price");
        assert_eq!(status.first_failure_day, Some(t3));
    }
}
