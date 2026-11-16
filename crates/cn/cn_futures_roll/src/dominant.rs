//! Port of `cn_futures/dominant.py`: next-day dominant selection.

use crate::instrument::is_later_delivery;
use chrono::NaiveDate;
use std::cmp::Ordering;
use std::collections::HashSet;

pub const SWITCH_MULTIPLE: f64 = 1.1;
pub const RULE_RQDATA_0: &str = "local_rqdata_rule0";
pub const RULE_TQSDK: &str = "local_tqsdk_oi_and_volume";
pub const SUPPORTED_RULES: &[&str] = &[RULE_RQDATA_0, RULE_TQSDK];

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum Rule {
    Rqdata0,
    Tqsdk,
}

impl Rule {
    pub fn parse(value: &str) -> Option<Self> {
        match value {
            RULE_RQDATA_0 => Some(Self::Rqdata0),
            RULE_TQSDK => Some(Self::Tqsdk),
            _ => None,
        }
    }

    pub fn name(self) -> &'static str {
        match self {
            Self::Rqdata0 => RULE_RQDATA_0,
            Self::Tqsdk => RULE_TQSDK,
        }
    }
}

/// One listed month on one TradDay.
#[derive(Clone, Debug, PartialEq)]
pub struct ContractDay {
    pub trad_day: NaiveDate,
    pub instrument_id: String,
    pub open_int: Option<f64>,
    pub volume: Option<f64>,
    pub close: Option<f64>,
    pub product_id: String,
    pub expired: bool,
}

/// Dominant assigned to a TradDay from information available before that day.
#[derive(Clone, Debug, PartialEq)]
pub struct DominantDay {
    pub trad_day: NaiveDate,
    pub product_id: String,
    pub instrument_id: Option<String>,
    pub roll_flag: i64,
    pub decision_open_int: Option<f64>,
    pub scheduled_next: Option<String>,
    pub rule: Rule,
}

fn finite_nonneg(value: Option<f64>) -> bool {
    matches!(value, Some(v) if !v.is_nan() && v >= 0.0)
}

/// Python float comparison inside a tuple: `0.0 == -0.0`; NaN never reaches here.
fn cmp_f64(left: f64, right: f64) -> Ordering {
    left.partial_cmp(&right).unwrap_or(Ordering::Equal)
}

fn volume_or_neg(row: &ContractDay) -> f64 {
    if finite_nonneg(row.volume) {
        row.volume.unwrap()
    } else {
        -1.0
    }
}

/// `(open_int, volume, instrument_id)`; `open_int` is a candidate's, so present.
fn cmp_oi_volume(left: &ContractDay, right: &ContractDay) -> Ordering {
    cmp_f64(left.open_int.unwrap(), right.open_int.unwrap())
        .then_with(|| cmp_f64(volume_or_neg(left), volume_or_neg(right)))
        .then_with(|| left.instrument_id.cmp(&right.instrument_id))
}

/// `(volume, open_int, instrument_id)`.
fn cmp_volume_oi(left: &ContractDay, right: &ContractDay) -> Ordering {
    cmp_f64(volume_or_neg(left), volume_or_neg(right))
        .then_with(|| cmp_f64(left.open_int.unwrap(), right.open_int.unwrap()))
        .then_with(|| left.instrument_id.cmp(&right.instrument_id))
}

/// Python `max(iterable, key=...)`: the first maximal element wins ties.
fn py_max<'a, I, F>(rows: I, cmp: F) -> Option<&'a ContractDay>
where
    I: IntoIterator<Item = &'a ContractDay>,
    F: Fn(&ContractDay, &ContractDay) -> Ordering,
{
    let mut best: Option<&ContractDay> = None;
    for row in rows {
        if best.is_none_or(|current| cmp(row, current) == Ordering::Greater) {
            best = Some(row);
        }
    }
    best
}

fn is_candidate(row: &ContractDay) -> bool {
    !row.expired && !row.instrument_id.is_empty() && finite_nonneg(row.open_int)
}

fn candidates(rows: &[ContractDay]) -> impl Iterator<Item = &ContractDay> {
    rows.iter().filter(|row| is_candidate(row))
}

fn max_by<F>(rows: &[ContractDay], cmp: F) -> Option<&ContractDay>
where
    F: Fn(&ContractDay, &ContractDay) -> Ordering,
{
    py_max(candidates(rows), cmp)
}

fn listed<'a>(rows: &'a [ContractDay], instrument: Option<&str>) -> Option<&'a ContractDay> {
    let instrument = instrument.filter(|value| !value.is_empty())?;
    candidates(rows).find(|row| row.instrument_id == instrument)
}

fn later_candidates<'a>(rows: &'a [ContractDay], current: &str) -> Vec<&'a ContractDay> {
    candidates(rows)
        .filter(|row| is_later_delivery(&row.instrument_id, current))
        .collect()
}

fn max_later<'a>(
    rows: &'a [ContractDay],
    current: &str,
    prefer_ids: Option<&HashSet<String>>,
) -> Option<&'a ContractDay> {
    if let Some(prefer) = prefer_ids.filter(|ids| !ids.is_empty()) {
        let overlapping = py_max(
            later_candidates(rows, current)
                .into_iter()
                .filter(|row| prefer.contains(&row.instrument_id)),
            cmp_oi_volume,
        );
        if overlapping.is_some() {
            return overlapping;
        }
    }
    py_max(later_candidates(rows, current), cmp_oi_volume)
}

fn rqdata_challenger<'a>(
    rows: &'a [ContractDay],
    current: &str,
    current_open_int: f64,
) -> Option<&'a ContractDay> {
    let threshold = current_open_int * SWITCH_MULTIPLE;
    py_max(
        later_candidates(rows, current)
            .into_iter()
            .filter(|row| row.open_int.unwrap() > threshold),
        cmp_oi_volume,
    )
}

/// Contract that is jointly first in open interest and volume.
fn tqsdk_leader(rows: &[ContractDay]) -> Option<&ContractDay> {
    let by_oi = max_by(rows, cmp_oi_volume)?;
    let by_volume = max_by(rows, cmp_volume_oi)?;
    (by_oi.instrument_id == by_volume.instrument_id).then_some(by_oi)
}

fn schedule_next(
    rule: Rule,
    day_rows: &[ContractDay],
    instrument: Option<&str>,
    current_open_int: Option<f64>,
    floor_instrument: Option<&str>,
) -> Option<String> {
    match rule {
        Rule::Rqdata0 => {
            let Some(instrument) = instrument else {
                return match floor_instrument {
                    None => max_by(day_rows, cmp_oi_volume).map(|row| row.instrument_id.clone()),
                    Some(floor) => {
                        max_later(day_rows, floor, None).map(|row| row.instrument_id.clone())
                    }
                };
            };
            let Some(current_open_int) = current_open_int else {
                return Some(instrument.to_owned());
            };
            Some(
                rqdata_challenger(day_rows, instrument, current_open_int)
                    .map_or(instrument, |row| row.instrument_id.as_str())
                    .to_owned(),
            )
        }
        Rule::Tqsdk => {
            let Some(leader) = tqsdk_leader(day_rows) else {
                return instrument.map(str::to_owned);
            };
            let floor = instrument.or(floor_instrument);
            if let Some(floor) = floor {
                if leader.instrument_id != floor && !is_later_delivery(&leader.instrument_id, floor)
                {
                    return instrument.map(str::to_owned);
                }
            }
            Some(leader.instrument_id.clone())
        }
    }
}

/// `rows` must be one product sorted by `(trad_day, instrument_id)`.
pub fn select_dominants(rows: &[ContractDay], product_id: &str, rule: Rule) -> Vec<DominantDay> {
    let resolved_product = if product_id.is_empty() {
        rows.iter()
            .find(|row| !row.product_id.is_empty())
            .map(|row| row.product_id.clone())
            .unwrap_or_default()
    } else {
        product_id.to_owned()
    };
    let mut scheduled: Option<String> = None;
    let mut previous_instrument: Option<String> = None;
    let mut last_assigned: Option<String> = None;
    let mut previous_ids: HashSet<String> = HashSet::new();
    let mut out = Vec::new();

    for day_rows in rows.chunk_by(|a, b| a.trad_day == b.trad_day) {
        let trad_day = day_rows[0].trad_day;
        let mut instrument = scheduled.clone();
        let mut listed_row = listed(day_rows, instrument.as_deref());
        if instrument.is_some() && listed_row.is_none() {
            let successor = max_later(
                day_rows,
                instrument.as_deref().unwrap(),
                Some(&previous_ids),
            );
            instrument = successor.map(|row| row.instrument_id.clone());
            listed_row = successor;
        } else if instrument.is_none() {
            if let Some(last) = last_assigned.as_deref() {
                let successor = max_later(day_rows, last, Some(&previous_ids));
                instrument = successor.map(|row| row.instrument_id.clone());
                listed_row = successor;
            }
        }

        let roll_flag = i64::from(matches!(
            (&instrument, &previous_instrument),
            (Some(now), Some(before)) if now != before
        ));
        let decision_open_int = listed_row.map(|row| row.open_int.unwrap());
        scheduled = schedule_next(
            rule,
            day_rows,
            instrument.as_deref(),
            decision_open_int,
            last_assigned.as_deref(),
        );
        out.push(DominantDay {
            trad_day,
            product_id: resolved_product.clone(),
            instrument_id: instrument.clone(),
            roll_flag,
            decision_open_int,
            scheduled_next: scheduled.clone(),
            rule,
        });
        if instrument.is_some() {
            last_assigned.clone_from(&instrument);
        }
        previous_instrument = instrument;
        previous_ids = day_rows
            .iter()
            .map(|row| row.instrument_id.clone())
            .collect();
    }
    out
}

#[cfg(test)]
mod tests {
    use super::*;

    fn d(y: i32, m: u32, day: u32) -> NaiveDate {
        NaiveDate::from_ymd_opt(y, m, day).unwrap()
    }

    fn row(day: NaiveDate, instrument: &str, open_int: f64, volume: f64) -> ContractDay {
        ContractDay {
            trad_day: day,
            instrument_id: instrument.to_owned(),
            open_int: Some(open_int),
            volume: Some(volume),
            close: Some(1.0),
            product_id: "RB".to_owned(),
            expired: false,
        }
    }

    fn sorted(mut rows: Vec<ContractDay>) -> Vec<ContractDay> {
        rows.sort_by(|a, b| (a.trad_day, &a.instrument_id).cmp(&(b.trad_day, &b.instrument_id)));
        rows
    }

    fn ids(days: &[DominantDay]) -> Vec<Option<&str>> {
        days.iter()
            .map(|day| day.instrument_id.as_deref())
            .collect()
    }

    #[test]
    fn rule0_switch_needs_strictly_more_than_one_point_one_and_is_next_day() {
        let (t1, t2, t3, t4) = (d(2025, 1, 2), d(2025, 1, 3), d(2025, 1, 6), d(2025, 1, 7));
        let rows = sorted(vec![
            row(t1, "rb2501", 100.0, 0.0),
            row(t1, "rb2505", 40.0, 0.0),
            row(t2, "rb2501", 100.0, 0.0),
            // 100 * 1.1 = 110.00000000000001 in f64, so 110 does not switch.
            row(t2, "rb2505", 110.0, 0.0),
            row(t3, "rb2501", 100.0, 0.0),
            row(t3, "rb2505", 111.0, 0.0),
            row(t4, "rb2501", 90.0, 0.0),
            row(t4, "rb2505", 99.0, 0.0),
        ]);
        let days = select_dominants(&rows, "RB", Rule::Rqdata0);
        assert_eq!(
            ids(&days),
            vec![None, Some("rb2501"), Some("rb2501"), Some("rb2505")]
        );
        assert_eq!(days[0].scheduled_next.as_deref(), Some("rb2501"));
        assert_eq!(days[2].scheduled_next.as_deref(), Some("rb2505"));
        assert_eq!(
            days.iter().map(|day| day.roll_flag).collect::<Vec<_>>(),
            vec![0, 0, 0, 1]
        );
        assert_eq!(days[1].decision_open_int, Some(100.0));
    }

    #[test]
    fn rule0_open_int_tie_breaks_on_volume_then_instrument_id() {
        let t1 = d(2025, 1, 2);
        let rows = sorted(vec![
            row(t1, "rb2501", 100.0, 5.0),
            row(t1, "rb2505", 100.0, 9.0),
            row(t1, "rb2510", 100.0, 9.0),
        ]);
        let days = select_dominants(&rows, "RB", Rule::Rqdata0);
        assert_eq!(days[0].scheduled_next.as_deref(), Some("rb2510"));

        let mut no_volume = rows.clone();
        no_volume[2].volume = None;
        let days = select_dominants(&no_volume, "RB", Rule::Rqdata0);
        assert_eq!(days[0].scheduled_next.as_deref(), Some("rb2505"));
    }

    #[test]
    fn rule0_does_not_switch_back_to_an_earlier_month() {
        let (t1, t2, t3, t4, t5) = (
            d(2025, 5, 14),
            d(2025, 5, 15),
            d(2025, 5, 16),
            d(2025, 5, 19),
            d(2025, 5, 20),
        );
        let rows = sorted(vec![
            row(t1, "bc2506", 100.0, 10.0),
            row(t1, "bc2505", 40.0, 4.0),
            row(t1, "bc2507", 10.0, 1.0),
            row(t2, "bc2506", 100.0, 10.0),
            row(t2, "bc2505", 40.0, 4.0),
            row(t2, "bc2507", 12.0, 1.0),
            row(t3, "bc2506", 50.0, 8.0),
            row(t3, "bc2505", 80.0, 20.0),
            row(t3, "bc2507", 20.0, 2.0),
            row(t4, "bc2505", 200.0, 30.0),
            row(t4, "bc2507", 30.0, 3.0),
            row(t5, "bc2505", 210.0, 31.0),
            row(t5, "bc2507", 140.0, 40.0),
        ]);
        let days = select_dominants(&rows, "BC", Rule::Rqdata0);
        assert_eq!(
            ids(&days),
            vec![
                None,
                Some("bc2506"),
                Some("bc2506"),
                Some("bc2507"),
                Some("bc2507")
            ]
        );
        assert_eq!(days[2].scheduled_next.as_deref(), Some("bc2506"));
        assert_eq!(days[3].roll_flag, 1);
        assert_eq!(days[4].roll_flag, 0);
    }

    #[test]
    fn delisted_current_prefers_later_month_listed_yesterday() {
        let (t1, t2, t3) = (d(2023, 12, 14), d(2023, 12, 15), d(2023, 12, 18));
        let rows = sorted(vec![
            row(t1, "bb2312", 10.0, 0.0),
            row(t1, "bb2401", 0.0, 0.0),
            row(t2, "bb2312", 0.0, 0.0),
            row(t2, "bb2401", 0.0, 0.0),
            row(t3, "bb2401", 0.0, 0.0),
            row(t3, "bb2412", 80.0, 0.0),
        ]);
        let days = select_dominants(&rows, "BB", Rule::Rqdata0);
        assert_eq!(ids(&days), vec![None, Some("bb2312"), Some("bb2401")]);
        assert_eq!(days[2].roll_flag, 1);
    }

    #[test]
    fn delisted_current_stays_unassigned_without_a_later_month() {
        let (t1, t2, t3) = (d(2023, 12, 14), d(2023, 12, 15), d(2023, 12, 18));
        let rows = sorted(vec![
            row(t1, "bb2312", 10.0, 0.0),
            row(t2, "bb2312", 10.0, 0.0),
            row(t3, "bb2301", 80.0, 0.0),
        ]);
        let days = select_dominants(&rows, "BB", Rule::Rqdata0);
        assert_eq!(ids(&days), vec![None, Some("bb2312"), None]);
        assert_eq!(days[2].scheduled_next, None);
        assert_eq!(days[2].roll_flag, 0);
    }

    #[test]
    fn tqsdk_requires_joint_lead_and_never_rolls_back() {
        let (t1, t2, t3, t4) = (d(2025, 8, 4), d(2025, 8, 5), d(2025, 8, 6), d(2025, 8, 7));
        let rows = sorted(vec![
            row(t1, "rb2501", 100.0, 800.0),
            row(t1, "rb2505", 40.0, 200.0),
            row(t2, "rb2501", 100.0, 810.0),
            row(t2, "rb2505", 120.0, 220.0),
            row(t3, "rb2501", 90.0, 400.0),
            row(t3, "rb2505", 130.0, 900.0),
            row(t4, "rb2501", 80.0, 300.0),
            row(t4, "rb2505", 140.0, 950.0),
        ]);
        let days = select_dominants(&rows, "RB", Rule::Tqsdk);
        assert_eq!(
            ids(&days),
            vec![None, Some("rb2501"), Some("rb2501"), Some("rb2505")]
        );
        assert_eq!(days[1].scheduled_next.as_deref(), Some("rb2501"));
        assert_eq!(days[3].roll_flag, 1);
        assert_eq!(days[3].rule.name(), RULE_TQSDK);

        let (t1, t2, t3, t4) = (
            d(2025, 5, 14),
            d(2025, 5, 15),
            d(2025, 5, 16),
            d(2025, 5, 19),
        );
        let rows = sorted(vec![
            row(t1, "bc2506", 100.0, 800.0),
            row(t1, "bc2505", 40.0, 200.0),
            row(t2, "bc2506", 100.0, 810.0),
            row(t2, "bc2505", 40.0, 210.0),
            row(t3, "bc2506", 50.0, 100.0),
            row(t3, "bc2505", 200.0, 900.0),
            row(t4, "bc2506", 40.0, 80.0),
            row(t4, "bc2505", 210.0, 950.0),
        ]);
        let days = select_dominants(&rows, "BC", Rule::Tqsdk);
        assert_eq!(
            ids(&days),
            vec![None, Some("bc2506"), Some("bc2506"), Some("bc2506")]
        );
        assert_eq!(days[2].scheduled_next.as_deref(), Some("bc2506"));
    }

    #[test]
    fn tqsdk_split_leaders_keep_current() {
        let (t1, t2, t3) = (d(2025, 8, 18), d(2025, 8, 19), d(2025, 8, 20));
        let rows = sorted(vec![
            row(t1, "rb2501", 100.0, 800.0),
            row(t1, "rb2505", 40.0, 200.0),
            row(t2, "rb2501", 100.0, 200.0),
            row(t2, "rb2505", 150.0, 100.0),
            row(t3, "rb2501", 90.0, 180.0),
            row(t3, "rb2505", 160.0, 90.0),
        ]);
        let days = select_dominants(&rows, "RB", Rule::Tqsdk);
        assert_eq!(ids(&days), vec![None, Some("rb2501"), Some("rb2501")]);
        assert_eq!(days[1].scheduled_next.as_deref(), Some("rb2501"));
    }
}
