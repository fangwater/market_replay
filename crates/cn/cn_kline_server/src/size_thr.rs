//! Lazily loaded, cached size-class thresholds for the in-progress day.
//!
//! The files are the shared ones written by `fill_1min_size_buckets` /
//! `scripts/export_size_thresholds.sh` (`<dir>/dominant_by_day/<YYYYMM>/<P>.json`,
//! see `cn_futures_l2::size_thresholds`): a trade of Shanghai month `M` is
//! classed with the thresholds of month `prior(M)`, chosen per trade so a
//! session crossing a month end is right. A missing or invalid file is never
//! an error: it yields `None` and the bars stay without size buckets. Absent
//! files are looked up again after `RETRY` (the monthly job may create them).

use cn_futures_l2::size_thresholds::{self as shared, month_of_ns, prior_month};
use std::collections::HashMap;
use std::path::PathBuf;
use std::sync::Mutex;
use std::time::{Duration, Instant};

const RETRY: Duration = Duration::from_secs(300);

struct Slot {
    loaded: Instant,
    thr: Option<(f64, f64)>,
}

pub struct SizeThresholds {
    dir: PathBuf,
    cache: Mutex<HashMap<(String, u32), Slot>>,
}

impl SizeThresholds {
    pub fn new(dir: PathBuf) -> Self {
        Self {
            dir,
            cache: Mutex::new(HashMap::new()),
        }
    }

    /// `(p50, p90)` of `product` computed over `source_month`, if its file is valid.
    pub fn lookup(&self, product: &str, source_month: u32) -> Option<(f64, f64)> {
        let key = (product.to_ascii_uppercase(), source_month);
        let mut cache = self.cache.lock().ok()?;
        if let Some(slot) = cache.get(&key) {
            if slot.thr.is_some() || slot.loaded.elapsed() < RETRY {
                return slot.thr;
            }
        }
        let thr = shared::load_valid(&self.dir, &key.0, source_month).map(|t| (t.p50, t.p90));
        cache.insert(
            key,
            Slot {
                loaded: Instant::now(),
                thr,
            },
        );
        thr
    }

    /// Thresholds for a trade at `ts_ns` (UTC ns): those of the month before the
    /// trade's own Shanghai month.
    pub fn for_trade(&self, product: &str, ts_ns: u64) -> Option<(f64, f64)> {
        self.lookup(product, prior_month(month_of_ns(ts_ns)))
    }

    /// `(applies_month, p50, p90)` for trades of the current Shanghai month
    /// (`now_ms`) and, only when its file already exists, of the next month (a
    /// night session can cross a month end). A month without thresholds has no
    /// entry.
    pub fn entries(&self, product: &str, now_ms: i64) -> Vec<(u32, f64, f64)> {
        let current = month_of_ns(now_ms.max(0) as u64 * 1_000_000);
        let next = if current % 100 == 12 {
            (current / 100 + 1) * 100 + 1
        } else {
            current + 1
        };
        [current, next]
            .into_iter()
            .filter_map(|applies| {
                self.lookup(product, prior_month(applies))
                    .map(|(p50, p90)| (applies, p50, p90))
            })
            .collect()
    }

    /// The source month used for a minute opening at `minute_ts_sec`, when its
    /// thresholds exist (the bar's `threshold_month`).
    pub fn month_for_minute(&self, product: &str, minute_ts_sec: i64) -> Option<u32> {
        let source = prior_month(month_of_ns(minute_ts_sec.max(0) as u64 * 1_000_000_000));
        self.lookup(product, source).map(|_| source)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use chrono::TimeZone;
    use chrono_tz::Asia::Shanghai;
    use cn_futures_l2::size_thresholds::{write, Thr};

    fn at(m: u32, d: u32, h: u32, mi: u32, s: u32) -> u64 {
        Shanghai
            .with_ymd_and_hms(2026, m, d, h, mi, s)
            .unwrap()
            .timestamp() as u64
            * 1_000_000_000
    }

    fn thr(month: u32, p50: f64, p90: f64) -> Thr {
        Thr {
            source_month: month,
            p50,
            p90,
            samples: 10,
            days: 3,
        }
    }

    #[test]
    fn a_trade_uses_the_month_before_its_own_shanghai_month() {
        let dir = tempfile::tempdir().unwrap();
        write(dir.path(), "SN", &thr(202609, 1.0, 2.0), &[]).unwrap();
        write(dir.path(), "SN", &thr(202610, 10.0, 20.0), &[]).unwrap();
        let t = SizeThresholds::new(dir.path().to_path_buf());
        // Friday night 2026-10-30 21:00 and Saturday 00:30 are October trades: September's file.
        assert_eq!(t.for_trade("SN", at(10, 30, 21, 0, 0)), Some((1.0, 2.0)));
        assert_eq!(t.for_trade("sn", at(10, 31, 23, 59, 59)), Some((1.0, 2.0)));
        // The first second of November already uses October's file.
        assert_eq!(t.for_trade("SN", at(11, 1, 0, 0, 0)), Some((10.0, 20.0)));
        assert_eq!(t.for_trade("SN", at(11, 17, 10, 0, 0)), Some((10.0, 20.0)));
        // Nothing exported for December's source month.
        assert_eq!(t.for_trade("SN", at(12, 3, 10, 0, 0)), None);
    }

    #[test]
    fn missing_or_invalid_files_never_invent_thresholds() {
        let dir = tempfile::tempdir().unwrap();
        let t = SizeThresholds::new(dir.path().to_path_buf());
        assert_eq!(t.lookup("SO", 202610), None, "no file");
        let bad = dir.path().join("dominant_by_day/202610");
        std::fs::create_dir_all(&bad).unwrap();
        std::fs::write(bad.join("CU.json"), b"{not json").unwrap();
        assert_eq!(t.lookup("CU", 202610), None, "garbage");
        std::fs::write(
            bad.join("AL.json"),
            br#"{"product":"AL","source_month":202610,"p50":1.0,"p90":2.0,"samples":4}"#,
        )
        .unwrap();
        assert_eq!(
            t.lookup("AL", 202610),
            None,
            "legacy file without the basis"
        );
    }

    #[test]
    fn response_entries_cover_the_current_month_and_the_next_only_when_known() {
        let dir = tempfile::tempdir().unwrap();
        write(dir.path(), "SN", &thr(202610, 10.0, 20.0), &[]).unwrap();
        let t = SizeThresholds::new(dir.path().to_path_buf());
        let ms = |m, d, h| (at(m, d, h, 0, 0) / 1_000_000) as i64;
        // Mid November: November trades use October; December's file is unknown.
        assert_eq!(t.entries("SN", ms(11, 17, 10)), vec![(202611, 10.0, 20.0)]);
        // Products without files get no entries (the client must not classify).
        assert!(t.entries("SO", ms(11, 17, 10)).is_empty());
        // No file for the current month's source: only a later month could exist.
        assert!(t.entries("SN", ms(12, 3, 10)).is_empty());
        // Month end: the next month's file exists once the month is complete, so a
        // night session crossing the month end sees both entries.
        let dir2 = tempfile::tempdir().unwrap();
        write(dir2.path(), "SN", &thr(202609, 1.0, 2.0), &[]).unwrap();
        write(dir2.path(), "SN", &thr(202610, 10.0, 20.0), &[]).unwrap();
        let t2 = SizeThresholds::new(dir2.path().to_path_buf());
        assert_eq!(
            t2.entries("SN", ms(10, 30, 21)),
            vec![(202610, 1.0, 2.0), (202611, 10.0, 20.0)]
        );
    }

    #[test]
    fn december_rolls_into_january_of_the_next_year() {
        let dir = tempfile::tempdir().unwrap();
        write(dir.path(), "SN", &thr(202511, 1.0, 2.0), &[]).unwrap();
        write(dir.path(), "SN", &thr(202512, 3.0, 4.0), &[]).unwrap();
        let t = SizeThresholds::new(dir.path().to_path_buf());
        let ts = Shanghai
            .with_ymd_and_hms(2025, 12, 31, 21, 0, 0)
            .unwrap()
            .timestamp_millis();
        assert_eq!(
            t.entries("SN", ts),
            vec![(202512, 1.0, 2.0), (202601, 3.0, 4.0)]
        );
    }

    #[test]
    fn minute_month_reports_the_source_month_only_when_it_exists() {
        let dir = tempfile::tempdir().unwrap();
        write(dir.path(), "SN", &thr(202610, 10.0, 20.0), &[]).unwrap();
        let t = SizeThresholds::new(dir.path().to_path_buf());
        let minute = (at(11, 17, 10, 0, 0) / 1_000_000_000) as i64;
        assert_eq!(t.month_for_minute("SN", minute), Some(202610));
        assert_eq!(t.month_for_minute("CU", minute), None);
    }
}
