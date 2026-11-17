//! Rebuild a finished trading day with the live chains and compare with the
//! production parquet:
//!  * one-shot chain (`live::compute`), as before;
//!  * segmented chain: the day's snapshots (in file order) go through
//!    `ContractState` (cuts at >= 30 min gaps, carry between segments), each
//!    segment is written to parquet and read back like a persisted segment;
//!  * event level: concatenated segment trades / depths / open interest equal
//!    the one-shot machine output.
//!
//! Usage (env `AS_OF=<session_as_of>`, optional `SIZE_DIR=<size threshold dir>`):
//!
//! `live_check <msg_root> <baseline_root> <exchange_dir> <product> <YYYYMMDD> <multiplier>`
use cn_futures_core::events::{
    process_instrument, process_instrument_resumable, InstrumentCarry, Snapshot,
};
use cn_futures_l2::baseline_1min::BaselineMinute;
use cn_futures_l2::export_1min::{read_day_parquet, write_day_parquet};
use cn_futures_l2::source::{read_tail, source_files_for_day, TailState};
use cn_futures_l2::Exchange;
use cn_kline_server::live::compute;
use cn_kline_server::live_seg::{minute_ceil, ContractState, Ctx, Params, Row};
use std::collections::BTreeMap;
use std::path::Path;

fn same(g: &BaselineMinute, w: &BaselineMinute, with_size: bool) -> bool {
    let eq = |x: f64, y: f64| (x.is_nan() && y.is_nan()) || (x - y).abs() <= 1e-9 * (1.0 + y.abs());
    let oe = |x: Option<f64>, y: Option<f64>| match (x, y) {
        (Some(x), Some(y)) => eq(x, y),
        (None, None) => true,
        _ => false,
    };
    let sizes_ok = !with_size
        || [
            (g.large_order, w.large_order),
            (g.medium_order, w.medium_order),
            (g.small_order, w.small_order),
            (g.large_buy, w.large_buy),
            (g.large_sell, w.large_sell),
            (g.medium_buy, w.medium_buy),
            (g.medium_sell, w.medium_sell),
            (g.small_buy, w.small_buy),
            (g.small_sell, w.small_sell),
            (g.net_buy_large, w.net_buy_large),
            (g.net_buy_medium, w.net_buy_medium),
            (g.net_buy_small, w.net_buy_small),
        ]
        .iter()
        .all(|(x, y)| eq(*x, *y));
    sizes_ok
        && eq(g.volume, w.volume)
        && eq(g.amount, w.amount)
        && oe(g.open, w.open)
        && oe(g.high, w.high)
        && oe(g.low, w.low)
        && oe(g.close, w.close)
        && oe(g.twap, w.twap)
        && oe(g.mid_price, w.mid_price)
        && eq(g.buy_volume, w.buy_volume)
        && eq(g.sell_volume, w.sell_volume)
}

#[derive(Default)]
struct Tally {
    compared: usize,
    mismatched: usize,
    missing: usize,
    extra: usize,
}

fn compare(
    id: &str,
    label: &str,
    got: &[BaselineMinute],
    want: &BTreeMap<i64, &BaselineMinute>,
    t: &mut Tally,
    with_size: bool,
) {
    let got: BTreeMap<i64, &BaselineMinute> = got.iter().map(|r| (r.ts, r)).collect();
    for (ts, w) in want {
        match got.get(ts) {
            None => {
                t.missing += 1;
                if t.missing < 6 {
                    println!("{label} {id} missing ts {ts}");
                }
            }
            Some(g) => {
                t.compared += 1;
                if !same(g, w, with_size) {
                    t.mismatched += 1;
                    if t.mismatched < 6 {
                        println!(
                            "{label} {id} ts {ts} differs: v={} a={} / prod v={} a={}",
                            g.volume, g.amount, w.volume, w.amount
                        );
                    }
                }
            }
        }
    }
    t.extra += got.keys().filter(|k| !want.contains_key(k)).count();
}

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
    // SIZE_DIR=<threshold dir>: classify trades with the shared prior-month
    // dominant-by-day thresholds exactly like the live path and compare the 12
    // size-bucket columns too (the baseline file must then be filled by the new tool).
    let thresholds = std::env::var("SIZE_DIR")
        .ok()
        .map(|d| cn_kline_server::size_thr::SizeThresholds::new(d.into()));
    let with_size = thresholds.is_some();
    let size_fn = |ts: u64| thresholds.as_ref().and_then(|t| t.for_trade(product, ts));
    let size: Option<&dyn Fn(u64) -> Option<(f64, f64)>> = with_size.then_some(&size_fn);
    let files = source_files_for_day(ex, day, None, Some(Path::new(msg)), None, 16)?;
    let mut snaps: BTreeMap<String, Vec<Snapshot>> = BTreeMap::new();
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
    let venue = cn_futures_trim::sessions::reference_venue(exd)?;
    let matcher = sessions.matcher(venue, product)?;
    let keep = |ts: i64| matcher.contains_ts(ts);
    let tmp = tempfile::tempdir()?;
    let (mut one, mut seg) = (Tally::default(), Tally::default());
    let (mut events_ok, mut events_bad, mut multi_seg, mut max_rows_after) = (0, 0, 0, 0usize);
    for (id, v) in snaps {
        let want: BTreeMap<i64, &BaselineMinute> = prod
            .iter()
            .filter(|r| r.contract_id == id)
            .map(|r| (r.ts, r))
            .collect();
        // one-shot
        let live = compute(&id, &mut v.clone(), ex, mult, day, &sessions, size)?;
        compare(&id, "one-shot", &live, &want, &mut one, with_size);

        // segmented, snapshots in file order
        let mut st = ContractState::default();
        for s in &v {
            st.push(Row::of(s), false);
        }
        let p = Params {
            margin_ns: 0,
            tail_quiet_ns: 0,
            delay_ns: i64::MAX,
            tail_grace_ns: 0,
        };
        let cuts = st.plan_cuts(i64::MAX / 4, i64::MAX / 4, true, p, &|_| None);
        let ctx = Ctx {
            instrument: &id,
            exchange: ex,
            multiplier: mult,
            day,
            in_session: &keep,
            size,
        };
        let (segments, _) = st.compute_segments(&cuts, &ctx)?;
        if segments.len() > 1 {
            multi_seg += 1;
        }
        let mut got = Vec::new();
        for (i, s) in segments.iter().enumerate() {
            let path = tmp.path().join(format!("{id}_{i}.parquet"));
            write_day_parquet(&path, &s.minutes)?;
            got.extend(read_day_parquet(&path)?);
        }
        compare(&id, "segmented", &got, &want, &mut seg, with_size);

        // event level: sliced + carry == one pass
        let mut all: Vec<Snapshot> = v.clone();
        let whole = process_instrument(&mut all, ex)?;
        let mut sorted: Vec<Snapshot> = v.clone();
        sorted.sort_by(|a, b| a.sort_key().cmp(&b.sort_key()));
        let mut carry = InstrumentCarry::default();
        let (mut tr, mut de, mut oi) = (Vec::new(), Vec::new(), Vec::new());
        let mut start = 0usize;
        for &(c, _) in &cuts {
            let end = start
                + sorted[start..]
                    .partition_point(|s| s.event_time.timestamp_nanos_opt().unwrap() < c);
            let mut slice = sorted[start..end].to_vec();
            let out = process_instrument_resumable(&mut slice, ex, &mut carry, true)?;
            tr.extend(out.trades);
            de.extend(out.depths);
            oi.extend(out.open_ints);
            max_rows_after = max_rows_after.max(end - start);
            start = end;
        }
        let ok = format!("{tr:?}") == format!("{:?}", whole.trades)
            && format!("{de:?}") == format!("{:?}", whole.depths)
            && format!("{oi:?}") == format!("{:?}", whole.open_ints);
        if ok {
            events_ok += 1;
        } else {
            events_bad += 1;
            println!("event-level mismatch {id}");
        }
        let _ = minute_ceil(0);
    }
    println!(
        "one-shot : compared={} mismatched={} missing={} extra={}",
        one.compared, one.mismatched, one.missing, one.extra
    );
    println!(
        "segmented: compared={} mismatched={} missing={} extra={} (contracts with >1 segment: {multi_seg}, largest slice {max_rows_after} rows)",
        seg.compared, seg.mismatched, seg.missing, seg.extra
    );
    println!("events   : contracts identical={events_ok} different={events_bad}");
    Ok(())
}
