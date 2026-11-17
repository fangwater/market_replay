//! Independent recomputation of the dominant-by-day size thresholds, compared
//! with an exported threshold file. Different code path from the exporter:
//! its own CSV parsing, per-instrument key seeks (not a whole-CF scan) and its
//! own linear percentile (numpy default).
//!
//! usage: dominant_thresholds_check <rocksdb_dir> <roll_result_dir> <PRODUCT> <YYYYMM> <threshold_dir>
use chrono::{Datelike, NaiveDate, TimeZone};
use chrono_tz::Asia::Shanghai;
use cn_futures_core::codec::{decode_key, decode_trade, encode_key, KIND_TRADE};
use cn_futures_core::segments::trad_day_from_ts;
use cn_futures_l2::db::{open_rocksdb_read_only, L2Db};
use cn_futures_l2::export_1s::{list_instruments, parse_product_cf};
use rocksdb::{Direction, IteratorMode};
use std::collections::BTreeMap;
use std::path::Path;

fn canon(code: &str, day: NaiveDate) -> String {
    let lower = code.trim().to_ascii_lowercase();
    let letters = lower
        .chars()
        .take_while(|c| c.is_ascii_alphabetic())
        .count();
    let tail = &lower[letters..];
    if tail.len() == 3 && tail.bytes().all(|b| b.is_ascii_digit()) {
        let year = day.year();
        let mut full = year / 10 * 10 + (tail.as_bytes()[0] - b'0') as i32;
        if full < year - 1 {
            full += 10;
        }
        return format!("{}{:02}{}", &lower[..letters], full % 100, &tail[1..]);
    }
    lower
}

fn percentile(sorted: &[f64], p: f64) -> f64 {
    let pos = p * (sorted.len() - 1) as f64;
    let (lo, hi) = (pos.floor() as usize, pos.ceil() as usize);
    sorted[lo] + (sorted[hi] - sorted[lo]) * (pos - lo as f64)
}

fn main() -> anyhow::Result<()> {
    let a: Vec<String> = std::env::args().collect();
    let (rdb, roll, product, ym, dir) = (&a[1], &a[2], a[3].to_ascii_uppercase(), &a[4], &a[5]);
    let month: u32 = ym.parse()?;
    let first = NaiveDate::from_ymd_opt((month / 100) as i32, month % 100, 1).unwrap();
    let last = if first.month() == 12 {
        NaiveDate::from_ymd_opt(first.year() + 1, 1, 1)
    } else {
        NaiveDate::from_ymd_opt(first.year(), first.month() + 1, 1)
    }
    .unwrap()
    .pred_opt()
    .unwrap();

    // Dominant contract per trading day, straight from the CSVs.
    let mut dominant: BTreeMap<NaiveDate, String> = BTreeMap::new();
    for ex in ["ccfx", "xdce", "xgfe", "xsge", "xsie", "xzce"] {
        let path = Path::new(roll).join(format!("{ex}_dominant.csv"));
        let Ok(text) = std::fs::read_to_string(&path) else {
            continue;
        };
        for line in text.lines().skip(1) {
            let f: Vec<&str> = line.split(',').collect();
            if f.len() > 2 && f[1].eq_ignore_ascii_case(&product) && !f[2].is_empty() {
                let day = NaiveDate::parse_from_str(f[0], "%Y-%m-%d")?;
                if day >= first && day <= last {
                    dominant.insert(day, canon(f[2], day));
                }
            }
        }
    }
    let rolls = dominant
        .values()
        .collect::<std::collections::BTreeSet<_>>()
        .len();

    let db = open_rocksdb_read_only(Path::new(rdb))?;
    let names = L2Db::list_cf(&rocksdb::Options::default(), rdb)?;
    let mut amounts = Vec::new();
    let mut days_with_trades = std::collections::BTreeSet::new();
    for name in names {
        let Some((_year, p)) = parse_product_cf(&name) else {
            continue;
        };
        if p != product {
            continue;
        }
        let cf = db.cf_handle(&name).unwrap();
        let instruments = list_instruments(&db, &name)?;
        for (day, want) in &dominant {
            // Instrument ids of this contract (CZCE may be stored with 3 digits).
            for id in instruments.iter().filter(|i| &canon(i, *day) == want) {
                // The trading day's trades lie within [day-4d, day] by timestamp.
                let lo = Shanghai
                    .from_local_datetime(
                        &(*day - chrono::Duration::days(4))
                            .and_hms_opt(0, 0, 0)
                            .unwrap(),
                    )
                    .unwrap()
                    .timestamp() as u64
                    * 1_000_000_000;
                let hi = Shanghai
                    .from_local_datetime(
                        &(*day + chrono::Duration::days(1))
                            .and_hms_opt(0, 0, 0)
                            .unwrap(),
                    )
                    .unwrap()
                    .timestamp() as u64
                    * 1_000_000_000;
                let start = encode_key(KIND_TRADE, id, lo, 0)?;
                for item in db.iterator_cf(&cf, IteratorMode::From(&start, Direction::Forward)) {
                    let (k, v) = item?;
                    let (kind, inst, ts, _) = decode_key(&k)?;
                    if kind != KIND_TRADE || &inst != id || ts >= hi {
                        break;
                    }
                    if trad_day_from_ts((ts / 1_000_000_000) as i64) != *day {
                        continue;
                    }
                    let rec = decode_trade(&v)?;
                    let amount = rec.turnover.unwrap_or(rec.price * rec.volume);
                    if amount.is_finite() && amount > 0.0 {
                        amounts.push(amount);
                        days_with_trades.insert(*day);
                    }
                }
            }
        }
    }
    amounts.sort_by(|x, y| x.partial_cmp(y).unwrap());
    let (p50, p90) = (percentile(&amounts, 0.5), percentile(&amounts, 0.9));
    let file = Path::new(dir)
        .join("dominant_by_day")
        .join(ym)
        .join(format!("{product}.json"));
    let exported: serde_json::Value = serde_json::from_slice(&std::fs::read(&file)?)?;
    let (ep50, ep90, es) = (
        exported["p50"].as_f64().unwrap(),
        exported["p90"].as_f64().unwrap(),
        exported["samples"].as_u64().unwrap(),
    );
    println!(
        "{product} {ym}: dominant contracts={rolls} days={} samples={} p50={p50} p90={p90}",
        days_with_trades.len(),
        amounts.len()
    );
    println!("exporter file: samples={es} p50={ep50} p90={ep90}");
    let same = es as usize == amounts.len() && ep50 == p50 && ep90 == p90;
    println!("{}", if same { "IDENTICAL" } else { "DIFFERENT" });
    anyhow::ensure!(same, "independent recomputation differs");
    Ok(())
}
