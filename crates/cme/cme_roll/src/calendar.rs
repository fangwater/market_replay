//! US business days on which CME risk days are defined.
//!
//! The roll's trading days are the observed trade dates (CME published OI, holiday copies
//! removed). Risk days, however, are defined on business days: weekdays minus the Federal
//! Reserve holidays (a Sunday holiday is observed on Monday; a Saturday holiday is not
//! observed, the Fed is open the Friday before) and Good Friday. This reproduces every
//! audited risk day and also covers future months; docs/cme_roll.md lists the days where it
//! differs from the trade dates.
use chrono::{Datelike, Duration, NaiveDate, Weekday};

fn nth_weekday(year: i32, month: u32, wd: Weekday, n: u32) -> NaiveDate {
    let first = NaiveDate::from_ymd_opt(year, month, 1).unwrap();
    let shift = (7 + wd.num_days_from_monday() - first.weekday().num_days_from_monday()) % 7;
    first + Duration::days((shift + 7 * (n - 1)) as i64)
}

fn last_weekday(year: i32, month: u32, wd: Weekday) -> NaiveDate {
    let next = if month == 12 {
        NaiveDate::from_ymd_opt(year + 1, 1, 1)
    } else {
        NaiveDate::from_ymd_opt(year, month + 1, 1)
    }
    .unwrap();
    let last = next - Duration::days(1);
    let back = (7 + last.weekday().num_days_from_monday() - wd.num_days_from_monday()) % 7;
    last - Duration::days(back as i64)
}

/// Federal Reserve observance: Sunday -> Monday; Saturday -> not observed.
fn observed(d: NaiveDate) -> Option<NaiveDate> {
    match d.weekday() {
        Weekday::Sat => None,
        Weekday::Sun => Some(d + Duration::days(1)),
        _ => Some(d),
    }
}

/// Western Easter Sunday (anonymous Gregorian algorithm).
fn easter(year: i32) -> NaiveDate {
    let a = year % 19;
    let b = year / 100;
    let c = year % 100;
    let d = b / 4;
    let e = b % 4;
    let f = (b + 8) / 25;
    let g = (b - f + 1) / 3;
    let h = (19 * a + b - d - g + 15) % 30;
    let i = c / 4;
    let k = c % 4;
    let l = (32 + 2 * e + 2 * i - h - k) % 7;
    let m = (a + 11 * h + 22 * l) / 451;
    let month = (h + l - 7 * m + 114) / 31;
    let day = (h + l - 7 * m + 114) % 31 + 1;
    NaiveDate::from_ymd_opt(year, month as u32, day as u32).unwrap()
}

/// Federal Reserve holidays (observed) and Good Friday of `year`.
pub fn us_holidays(year: i32) -> Vec<NaiveDate> {
    let ymd = |m, d| NaiveDate::from_ymd_opt(year, m, d).unwrap();
    let mut v: Vec<NaiveDate> = [
        observed(ymd(1, 1)),
        Some(nth_weekday(year, 1, Weekday::Mon, 3)),
        Some(nth_weekday(year, 2, Weekday::Mon, 3)),
        Some(easter(year) - Duration::days(2)),
        Some(last_weekday(year, 5, Weekday::Mon)),
        if year >= 2021 {
            observed(ymd(6, 19))
        } else {
            None
        },
        observed(ymd(7, 4)),
        Some(nth_weekday(year, 9, Weekday::Mon, 1)),
        Some(nth_weekday(year, 10, Weekday::Mon, 2)),
        observed(ymd(11, 11)),
        Some(nth_weekday(year, 11, Weekday::Thu, 4)),
        observed(ymd(12, 25)),
    ]
    .into_iter()
    .flatten()
    .collect();
    v.sort();
    v
}

/// Projected business days in `[from, to]`.
pub fn projected_business_days(from: NaiveDate, to: NaiveDate) -> Vec<NaiveDate> {
    let mut holidays = Vec::new();
    for y in from.year() - 1..=to.year() + 1 {
        holidays.extend(us_holidays(y));
    }
    let mut out = Vec::new();
    let mut d = from;
    while d <= to {
        if d.weekday().num_days_from_monday() < 5 && !holidays.contains(&d) {
            out.push(d);
        }
        d += Duration::days(1);
    }
    out
}

#[cfg(test)]
mod tests {
    use super::*;

    fn d(s: &str) -> NaiveDate {
        NaiveDate::parse_from_str(s, "%Y-%m-%d").unwrap()
    }

    #[test]
    fn holidays() {
        assert_eq!(easter(2024), d("2024-03-31"));
        assert_eq!(easter(2027), d("2027-03-28"));
        let h = us_holidays(2027);
        for x in [
            "2027-01-01",
            "2027-01-18",
            "2027-02-15",
            "2027-03-26",
            "2027-05-31",
            "2027-07-05",
            "2027-09-06",
            "2027-10-11",
            "2027-11-11",
            "2027-11-25",
        ] {
            assert!(h.contains(&d(x)), "{x}");
        }
        // Saturday holidays are not observed (Juneteenth / Christmas 2027, New Year 2022).
        for x in ["2027-06-18", "2027-12-24"] {
            assert!(!h.contains(&d(x)), "{x}");
        }
        assert!(!us_holidays(2021).contains(&d("2021-12-31")));
        assert!(us_holidays(2020)
            .iter()
            .all(|x| x.month() != 6 || x.day() != 19));
    }

    #[test]
    fn projected_days_skip_weekends_and_holidays() {
        let v = projected_business_days(d("2027-02-24"), d("2027-03-02"));
        assert_eq!(
            v,
            [
                "2027-02-24",
                "2027-02-25",
                "2027-02-26",
                "2027-03-01",
                "2027-03-02"
            ]
            .map(d)
        );
    }
}
