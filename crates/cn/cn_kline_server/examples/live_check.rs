//! Rebuild a finished trading day with the live (NFS tail) chain and compare
//! it with the production parquet.
//! usage: live_check <msg_root> <baseline_root> <exchange_dir> <product> <YYYYMMDD> <multiplier>
use cn_futures_l2::export_1min::read_day_parquet;
use cn_futures_l2::source::{read_tail, source_files_for_day, TailState};
use cn_futures_l2::Exchange;
use cn_kline_server::live::compute;
use std::collections::BTreeMap;
use std::path::Path;

fn main() -> anyhow::Result<()> {
    let a: Vec<String> = std::env::args().collect();
    let (msg, base, exd, product) = (&a[1], &a[2], &a[3], &a[4]);
    let day = chrono::NaiveDate::parse_from_str(&a[5], "%Y%m%d")?;
    let mult: f64 = a[6].parse()?;
    let ex = Exchange::parse(exd).unwrap();
    let sessions = cn_futures_trim::sessions::ContinuousSessions::fetch(
        "http://127.0.0.1:8765/api/v1/trading-sessions",
        Some(&std::env::var("AS_OF")?),
    )?;
    let files = source_files_for_day(ex, day, None, Some(Path::new(msg)), None, 16)?;
    let mut snaps: BTreeMap<String, Vec<_>> = BTreeMap::new();
    let t = std::time::Instant::now();
    for (order, f) in files.iter().enumerate() {
        let mut out = Vec::new();
        read_tail(
            &f.path,
            day,
            order as u32,
            std::slice::from_ref(product),
            &mut TailState::default(),
            &mut out,
        )?;
        for s in out {
            snaps.entry(s.instrument_id.clone()).or_default().push(s);
        }
    }
    eprintln!("read {} files in {:?}", files.len(), t.elapsed());
    let prod = read_day_parquet(
        &Path::new(base)
            .join(exd)
            .join(product)
            .join(format!("{}.parquet", a[5])),
    )?;
    let (mut bars, mut bad, mut extra, mut missing) = (0, 0, 0, 0);
    for (id, mut v) in snaps {
        let live = compute(&id, &mut v, ex, mult, day, &sessions)?;
        let want: BTreeMap<i64, _> = prod
            .iter()
            .filter(|r| r.contract_id == id)
            .map(|r| (r.ts, r))
            .collect();
        let got: BTreeMap<i64, _> = live.iter().map(|r| (r.ts, r)).collect();
        for (ts, w) in &want {
            match got.get(ts) {
                None => {
                    missing += 1;
                    if missing < 6 {
                        println!("{id} missing ts {ts}");
                    }
                }
                Some(g) => {
                    bars += 1;
                    let eq = |x: f64, y: f64| {
                        (x.is_nan() && y.is_nan()) || (x - y).abs() <= 1e-9 * (1.0 + y.abs())
                    };
                    let oe = |x: Option<f64>, y: Option<f64>| match (x, y) {
                        (Some(x), Some(y)) => eq(x, y),
                        (None, None) => true,
                        _ => false,
                    };
                    let ok = eq(g.volume, w.volume)
                        && eq(g.amount, w.amount)
                        && oe(g.open, w.open)
                        && oe(g.high, w.high)
                        && oe(g.low, w.low)
                        && oe(g.close, w.close)
                        && oe(g.twap, w.twap)
                        && oe(g.mid_price, w.mid_price)
                        && eq(g.buy_volume, w.buy_volume)
                        && eq(g.sell_volume, w.sell_volume);
                    if !ok {
                        bad += 1;
                        if bad < 6 {
                            println!(
                                "{id} ts {ts} differs: live v={} a={} / prod v={} a={}",
                                g.volume, g.amount, w.volume, w.amount
                            );
                        }
                    }
                }
            }
        }
        for (k, g) in got.iter().filter(|(k, _)| !want.contains_key(k)) {
            if g.volume > 0.0 {
                println!(
                    "extra with volume {id} {} vol={}",
                    chrono::DateTime::from_timestamp(*k + 8 * 3600, 0)
                        .unwrap()
                        .format("%H:%M"),
                    g.volume
                );
            }
        }
        extra += got.keys().filter(|k| !want.contains_key(k)).count();
    }
    println!("compared={bars} mismatched={bad} missing_in_live={missing} extra_in_live={extra}");
    Ok(())
}
