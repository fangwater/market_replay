//! Per-contract state of the in-progress day, cut into continuous segments.
//!
//! A contract's day is a sequence of segments separated by gaps of at least
//! `SESSION_BREAK_SEC` (night / morning / afternoon, the same breaks the nightly
//! export derives from the data). Once such a gap is proven, the segment before
//! it can never change: its minutes are computed with the same chain as the
//! nightly export, persisted by the caller, and its snapshots are dropped. Only
//! the snapshot->trade machine state (`InstrumentCarry`) travels on, so memory
//! holds the current segment only and a restart can resume from the carry.
//!
//! Cut rule. Take consecutive snapshots (event-time order) `a`, `b` with
//! `b - a >= 1800 s`. The kept depth seconds lie inside `[a, b]`, so their gap
//! is `>= 1800 s` as well: a genuine break. The cut `C = floor_minute(a) + 60 s`
//! lies in that gap, hence segments, minute rows and the machine's per-second
//! depth buckets on either side of `C` are independent (`fill_session_minutes`
//! restarts its fill state and TWAP seed per segment; `synthesize_minutes`
//! accumulates per minute). The slice before `C` is processed with
//! `flush_pending = true`, which is what the single pass does when the next
//! snapshot (>= 1800 s later) arrives.
use anyhow::{bail, Context, Result};
use chrono::{NaiveDate, TimeZone};
use cn_futures_core::events::{
    process_instrument_resumable, Book, Direction, InstrumentCarry, InstrumentOutput, Snapshot,
};
use cn_futures_core::segments::{book_valid, trad_day_from_ts, SESSION_BREAK_SEC};
use cn_futures_core::universe::product_id;
use cn_futures_l2::baseline_1min::{
    fill_session_minutes_with_twap, minute_segments_from_seconds, synthesize_minutes,
    BaselineMinute, PrintTrade,
};
use cn_futures_l2::{split_depth_segments, Exchange};
use serde_json::{json, Value};

pub const NS: i64 = 1_000_000_000;
pub const BREAK_NS: i64 = SESSION_BREAK_SEC * NS;

/// One snapshot, stored compactly (216 bytes vs ~540 for `Snapshot` with two
/// heap strings and 24 `Option<f64>`); missing numbers are NaN (the parser
/// only ever yields finite values). The instrument is the map key and the
/// trading day is the day state's `day`.
#[derive(Clone)]
pub struct Row {
    pub ts_ns: i64,
    pub seq: i64,
    pub source_row: u32,
    pub file_order: u32,
    /// volume, turnover, last, open interest, bid px x5, bid size x5, ask px x5, ask size x5.
    pub v: [f64; 24],
}

fn nan(x: Option<f64>) -> f64 {
    x.unwrap_or(f64::NAN)
}
fn opt(x: f64) -> Option<f64> {
    (!x.is_nan()).then_some(x)
}

impl Row {
    pub fn of(s: &Snapshot) -> Self {
        let mut v = [f64::NAN; 24];
        v[0] = nan(s.volume);
        v[1] = nan(s.turnover);
        v[2] = nan(s.last_price);
        v[3] = nan(s.open_int);
        for i in 0..5 {
            v[4 + i] = nan(s.bid_prices[i]);
            v[9 + i] = nan(s.bid_sizes[i]);
            v[14 + i] = nan(s.ask_prices[i]);
            v[19 + i] = nan(s.ask_sizes[i]);
        }
        Self {
            ts_ns: s.event_time.timestamp_nanos_opt().expect("ns range"),
            seq: s.seq,
            source_row: s.source_row,
            file_order: s.file_order,
            v,
        }
    }

    pub fn snapshot(&self, instrument: &str, day: NaiveDate) -> Snapshot {
        let lv = |o: usize| -> [Option<f64>; 5] { std::array::from_fn(|i| opt(self.v[o + i])) };
        Snapshot {
            trad_day: day,
            instrument_id: instrument.to_string(),
            product_id: product_id(instrument).unwrap_or_default(),
            event_time: chrono_tz::Asia::Shanghai.timestamp_nanos(self.ts_ns),
            seq: self.seq,
            source_row: self.source_row,
            file_order: self.file_order,
            volume: opt(self.v[0]),
            turnover: opt(self.v[1]),
            last_price: opt(self.v[2]),
            open_int: opt(self.v[3]),
            bid_prices: lv(4),
            bid_sizes: lv(9),
            ask_prices: lv(14),
            ask_sizes: lv(19),
        }
    }

    fn key(&self) -> (i64, i64, u32, u32) {
        (self.ts_ns, self.seq, self.source_row, self.file_order)
    }
}

/// Outcome of offering a row to a contract.
#[derive(Debug, PartialEq, Eq)]
pub enum Push {
    Kept,
    /// Replay of a row a persisted segment already covers.
    Dropped,
    /// A row that would have changed an already persisted segment: the
    /// persisted data of this exchange is invalid and must be rebuilt.
    Late,
}

#[derive(Clone, Copy, Debug)]
pub struct Params {
    /// A jump-type cut needs `through >= C + margin` (exchange-wide NFS clock).
    pub margin_ns: i64,
    /// Wall-clock silence after which the last segment is cut with no later row
    /// (must exceed the break length plus the NFS delivery lag).
    pub tail_quiet_ns: i64,
    /// Scheduled tail cut: wall-clock delay after the contract's last row (the
    /// NFS copy must have been quiet and caught up for this long) ...
    pub delay_ns: i64,
    /// ... and the cut lies this far after the window end, so closing
    /// snapshots written just after the end still belong to the segment.
    pub tail_grace_ns: i64,
}

/// A finished segment: the minute rows of one continuous stretch.
pub struct Segment {
    pub first_ts: i64,
    pub minutes: Vec<BaselineMinute>,
}

#[derive(Clone, Debug, Default)]
pub struct ContractState {
    pub rows: Vec<Row>,
    /// Machine state after every snapshot before `persisted_until_ns`.
    pub carry: InstrumentCarry,
    pub persisted_until_ns: i64,
    /// Event time of the last snapshot before the latest cut.
    pub cut_last_ns: i64,
    pub max_ts_ns: i64,
    maybe_cut: bool,
}

impl std::fmt::Debug for Row {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "Row({})", self.ts_ns)
    }
}

pub fn minute_ceil(ts_ns: i64) -> i64 {
    ts_ns.div_euclid(60 * NS) * 60 * NS + 60 * NS
}

impl ContractState {
    /// A contract resumed from a manifest: no retained rows, only the carry.
    pub fn restored(
        carry: InstrumentCarry,
        persisted_until_ns: i64,
        cut_last_ns: i64,
        max_ts_ns: i64,
    ) -> Self {
        Self {
            carry,
            persisted_until_ns,
            cut_last_ns,
            max_ts_ns,
            ..Self::default()
        }
    }

    pub fn push(&mut self, row: Row, replay: bool) -> Push {
        if replay {
            if row.ts_ns < self.persisted_until_ns {
                return Push::Dropped;
            }
        } else if self.cut_last_ns > 0 && row.ts_ns < self.cut_last_ns + BREAK_NS {
            return Push::Late;
        }
        if !self.rows.is_empty() && row.ts_ns - self.max_ts_ns >= BREAK_NS {
            self.maybe_cut = true;
        }
        self.max_ts_ns = self.max_ts_ns.max(row.ts_ns);
        self.rows.push(row);
        Push::Kept
    }

    /// Ask for a re-check of the retained rows (after a restart replay).
    pub fn recheck(&mut self) {
        self.maybe_cut = !self.rows.is_empty();
    }

    /// Cuts `(C, last snapshot before the gap)` that are provable now. The
    /// silence-based tail cut additionally needs `caught_up`: the files have
    /// been read to their current end, otherwise a contract's last row read so
    /// far only looks old.
    ///
    /// `break_end(ts_sec)` answers whether `ts_sec` is at the end of a
    /// scheduled window followed by a break of at least `SESSION_BREAK_SEC`
    /// and returns that end (epoch s). The last segment is then cut
    /// `delay_ns` after the last row (and `tail_grace_ns` after the window end)
    /// instead of waiting for the long silence rule, which stays as a fallback.
    pub fn plan_cuts(
        &mut self,
        through_ns: i64,
        now_ns: i64,
        caught_up: bool,
        p: Params,
        break_end: &dyn Fn(i64) -> Option<i64>,
    ) -> Vec<(i64, i64)> {
        if self.rows.is_empty() {
            return Vec::new();
        }
        let quiet = now_ns - self.max_ts_ns;
        let scheduled = if caught_up && quiet >= p.delay_ns {
            break_end(self.max_ts_ns.div_euclid(NS))
        } else {
            None
        };
        let tail_due = caught_up && (quiet >= p.tail_quiet_ns || scheduled.is_some());
        if !self.maybe_cut && !tail_due {
            return Vec::new();
        }
        self.rows.sort_by_key(Row::key);
        let mut cuts = Vec::new();
        let mut deferred = false;
        for w in self.rows.windows(2) {
            if w[1].ts_ns - w[0].ts_ns >= BREAK_NS {
                let c = minute_ceil(w[0].ts_ns);
                if c + p.margin_ns <= through_ns {
                    cuts.push((c, w[0].ts_ns));
                } else {
                    deferred = true;
                }
            }
        }
        self.maybe_cut = deferred;
        if tail_due {
            let last = self.rows.last().expect("non-empty").ts_ns;
            let at = match scheduled {
                Some(end) => minute_ceil(last).max(end * NS + p.tail_grace_ns),
                None => minute_ceil(last),
            };
            cuts.push((at, last));
        }
        cuts
    }

    /// Minute rows of every cut segment, plus the carry after the last cut.
    /// Pure: nothing is dropped until [`ContractState::commit`].
    pub fn compute_segments(
        &self,
        cuts: &[(i64, i64)],
        ctx: &Ctx,
    ) -> Result<(Vec<Segment>, InstrumentCarry)> {
        let mut carry = self.carry.clone();
        let mut segments = Vec::new();
        let mut start = 0usize;
        for &(c, _) in cuts {
            let end = start + self.rows[start..].partition_point(|r| r.ts_ns < c);
            let mut snaps: Vec<Snapshot> = self.rows[start..end]
                .iter()
                .map(|r| r.snapshot(ctx.instrument, ctx.day))
                .collect();
            let out = process_instrument_resumable(&mut snaps, ctx.exchange, &mut carry, true)?;
            let minutes = minutes_from_output(ctx, out)?;
            if let Some(first) = minutes.first() {
                segments.push(Segment {
                    first_ts: first.ts,
                    minutes,
                });
            }
            start = end;
        }
        Ok((segments, carry))
    }

    /// Drops the rows before the last cut once its segments are persisted.
    pub fn commit(&mut self, cuts: &[(i64, i64)], carry: InstrumentCarry) {
        let Some(&(c, last)) = cuts.last() else {
            return;
        };
        let end = self.rows.partition_point(|r| r.ts_ns < c);
        self.rows.drain(..end);
        self.rows.shrink_to_fit();
        self.carry = carry;
        self.persisted_until_ns = c;
        self.cut_last_ns = last;
    }

    /// Minute rows of the retained (not yet persisted) snapshots.
    pub fn open_minutes(&self, ctx: &Ctx) -> Result<Vec<BaselineMinute>> {
        if self.rows.is_empty() {
            return Ok(Vec::new());
        }
        let mut snaps: Vec<Snapshot> = self
            .rows
            .iter()
            .map(|r| r.snapshot(ctx.instrument, ctx.day))
            .collect();
        let mut carry = self.carry.clone();
        let out = process_instrument_resumable(&mut snaps, ctx.exchange, &mut carry, true)?;
        minutes_from_output(ctx, out)
    }
}

/// What turns machine output into minute rows.
pub struct Ctx<'a> {
    pub instrument: &'a str,
    pub exchange: Exchange,
    pub multiplier: f64,
    pub day: NaiveDate,
    /// Continuous-session filter on a minute's UTC second (the archive's trim).
    pub in_session: &'a dyn Fn(i64) -> bool,
}

/// Same chain as `export_1min::export_product`, minus RocksDB.
pub fn minutes_from_output(ctx: &Ctx, out: InstrumentOutput) -> Result<Vec<BaselineMinute>> {
    let mut depths = Vec::new();
    let mut depth_secs = Vec::new();
    for rec in out.depths {
        let (Some(bp), Some(ap)) = (rec.bid_prices[0], rec.ask_prices[0]) else {
            continue;
        };
        let bv = rec.bid_sizes[0].unwrap_or(f64::NAN);
        let av = rec.ask_sizes[0].unwrap_or(f64::NAN);
        if !book_valid(bp, bv, ap, av) {
            continue;
        }
        depth_secs.push((rec.ts_utc_ns / 1_000_000_000) as i64);
        depths.push(rec);
    }
    let trades: Vec<PrintTrade> = out
        .trades
        .iter()
        .map(|(_, t)| PrintTrade::from(t))
        .collect();
    if depth_secs.is_empty() && trades.is_empty() {
        return Ok(Vec::new());
    }
    let sec_segs = split_depth_segments(&depth_secs, ctx.exchange, ctx.instrument);
    let min_segs = minute_segments_from_seconds(&sec_segs);
    let sparse = synthesize_minutes(ctx.instrument, &trades, &depths, ctx.multiplier)?;
    let filled = fill_session_minutes_with_twap(ctx.instrument, sparse, &min_segs, &trades)?;
    Ok(filled
        .into_iter()
        .filter(|r| trad_day_from_ts(r.ts) == ctx.day && (ctx.in_session)(r.ts))
        .collect())
}

// ---- carry persistence (JSON; f64 round-trips exactly) ----

fn of(x: Option<f64>) -> Value {
    x.map_or(Value::Null, |v| json!(v))
}
fn arr(a: &[Option<f64>; 5]) -> Value {
    Value::Array(a.iter().map(|x| of(*x)).collect())
}
fn book_json(b: &Book) -> Value {
    json!({"bp": arr(&b.bid_prices), "bs": arr(&b.bid_sizes), "ap": arr(&b.ask_prices), "as": arr(&b.ask_sizes)})
}
fn get_f(v: &Value) -> Result<Option<f64>> {
    match v {
        Value::Null => Ok(None),
        Value::Number(n) => Ok(Some(n.as_f64().context("number")?)),
        other => bail!("bad number {other}"),
    }
}
fn get_arr(v: &Value) -> Result<[Option<f64>; 5]> {
    let a = v.as_array().context("array")?;
    if a.len() != 5 {
        bail!("level array must have 5 entries");
    }
    let mut out = [None; 5];
    for (slot, x) in out.iter_mut().zip(a) {
        *slot = get_f(x)?;
    }
    Ok(out)
}
fn book_from(v: &Value) -> Result<Book> {
    Ok(Book {
        bid_prices: get_arr(&v["bp"])?,
        bid_sizes: get_arr(&v["bs"])?,
        ask_prices: get_arr(&v["ap"])?,
        ask_sizes: get_arr(&v["as"])?,
    })
}

/// The carry of a persisted cut. `pending_depth` is always empty there.
pub fn carry_to_json(c: &InstrumentCarry) -> Value {
    assert!(c.pending_depth.is_none(), "cut carries are flushed");
    json!({
        "vol_prev": of(c.vol_prev), "to_prev": of(c.to_prev), "last_px": of(c.last_px),
        "last_dir": match c.last_dir { Some(Direction::Buy) => "buy", Some(Direction::Sell) => "sell", None => "" },
        "book": c.book.as_ref().map_or(Value::Null, book_json),
        "trade_ts": c.trade_ts, "trade_seq": c.trade_seq,
        "last_oi": of(c.last_oi), "oi_ts": c.oi_ts, "oi_seq": c.oi_seq,
    })
}

pub fn carry_from_json(v: &Value) -> Result<InstrumentCarry> {
    Ok(InstrumentCarry {
        vol_prev: get_f(&v["vol_prev"])?,
        to_prev: get_f(&v["to_prev"])?,
        last_px: get_f(&v["last_px"])?,
        last_dir: match v["last_dir"].as_str().unwrap_or("") {
            "buy" => Some(Direction::Buy),
            "sell" => Some(Direction::Sell),
            _ => None,
        },
        book: match &v["book"] {
            Value::Null => None,
            b => Some(book_from(b)?),
        },
        pending_depth: None,
        trade_ts: v["trade_ts"].as_u64().context("trade_ts")?,
        trade_seq: v["trade_seq"].as_u64().context("trade_seq")? as u32,
        last_oi: get_f(&v["last_oi"])?,
        oi_ts: v["oi_ts"].as_u64().context("oi_ts")?,
        oi_seq: v["oi_seq"].as_u64().context("oi_seq")? as u32,
    })
}

#[cfg(test)]
mod tests {
    use super::*;
    use chrono::NaiveTime;
    use cn_futures_core::events::process_instrument;

    fn day() -> NaiveDate {
        NaiveDate::from_ymd_opt(2026, 2, 10).unwrap()
    }

    fn snap(h: u32, m: u32, s: u32, ms: u32, vol: f64, last: f64, bid: f64, ask: f64) -> Snapshot {
        let t = NaiveTime::from_hms_milli_opt(h, m, s, ms).unwrap();
        Snapshot {
            trad_day: day(),
            instrument_id: "rb2605".into(),
            product_id: "RB".into(),
            event_time: chrono_tz::Asia::Shanghai
                .from_local_datetime(&day().and_time(t))
                .single()
                .unwrap(),
            seq: 1,
            source_row: 1,
            file_order: 0,
            volume: Some(vol),
            turnover: Some(vol * last * 10.0),
            last_price: Some(last),
            open_int: Some(100.0 + vol),
            bid_prices: [Some(bid), Some(bid - 1.0), None, None, None],
            bid_sizes: [Some(3.0), Some(2.0), None, None, None],
            ask_prices: [Some(ask), Some(ask + 1.0), None, None, None],
            ask_sizes: [Some(4.0), Some(1.0), None, None, None],
        }
    }

    /// Two sessions 09:00-09:10 and 13:30-13:40 with prints that depend on the
    /// previous direction (flat price inside the spread).
    fn stream() -> Vec<Snapshot> {
        let mut v = Vec::new();
        let mut vol = 0.0;
        for (h, m0) in [(9u32, 0u32), (13, 30)] {
            for m in m0..m0 + 10 {
                for s in 0..60 {
                    for half in 0..2u32 {
                        vol += if (s + m + half) % 3 == 0 { 1.0 } else { 0.0 };
                        let last = 3380.0 + ((s / 7 + m) % 3) as f64;
                        v.push(snap(h, m, s, half * 500, vol, last, 3379.0, 3382.0));
                    }
                }
            }
        }
        v
    }

    fn ctx<'a>(keep: &'a dyn Fn(i64) -> bool) -> Ctx<'a> {
        Ctx {
            instrument: "rb2605",
            exchange: Exchange::Xsge,
            multiplier: 10.0,
            day: day(),
            in_session: keep,
        }
    }

    fn rows(snaps: &[Snapshot]) -> Vec<Row> {
        snaps.iter().map(Row::of).collect()
    }

    #[test]
    fn segmented_minutes_equal_one_pass() {
        let keep = |_: i64| true;
        let c = ctx(&keep);
        let all = stream();
        let one = minutes_from_output(
            &c,
            process_instrument(&mut all.clone(), c.exchange).unwrap(),
        )
        .unwrap();
        assert!(one.len() > 15);

        let mut st = ContractState::default();
        for r in rows(&all) {
            assert_eq!(st.push(r, false), Push::Kept);
        }
        let p = Params {
            margin_ns: 0,
            tail_quiet_ns: i64::MAX,
            delay_ns: i64::MAX,
            tail_grace_ns: 0,
        };
        let through = all
            .last()
            .unwrap()
            .event_time
            .timestamp_nanos_opt()
            .unwrap();
        let cuts = st.plan_cuts(through + 3600 * NS, 0, true, p, &|_| None);
        assert_eq!(cuts.len(), 1, "one 09:10 -> 13:30 break");
        let (segs, carry) = st.compute_segments(&cuts, &c).unwrap();
        assert_eq!(segs.len(), 1);
        st.commit(&cuts, carry);
        assert!(st.rows.len() < all.len() && st.persisted_until_ns > 0);
        let mut got: Vec<BaselineMinute> = segs.into_iter().flat_map(|s| s.minutes).collect();
        got.extend(st.open_minutes(&c).unwrap());
        assert_eq!(format!("{got:?}"), format!("{one:?}"));
    }

    #[test]
    fn tail_cut_needs_quiet_and_late_rows_are_flagged() {
        let keep = |_: i64| true;
        let c = ctx(&keep);
        let first: Vec<Snapshot> = stream().into_iter().take(1200).collect();
        let mut st = ContractState::default();
        for r in rows(&first) {
            st.push(r, false);
        }
        let last = st.max_ts_ns;
        let p = Params {
            margin_ns: 60 * NS,
            tail_quiet_ns: 7200 * NS,
            delay_ns: 600 * NS,
            tail_grace_ns: 300 * NS,
        };
        let no_break = |_: i64| None;
        assert!(
            st.plan_cuts(last, last + 3600 * NS, true, p, &no_break)
                .is_empty(),
            "not quiet long enough"
        );
        assert!(
            st.plan_cuts(last, last + 9000 * NS, false, p, &no_break)
                .is_empty(),
            "files not caught up"
        );
        let cuts = st.plan_cuts(last, last + 7200 * NS, true, p, &no_break);
        assert_eq!(cuts, vec![(minute_ceil(last), last)]);
        let (segs, carry) = st.compute_segments(&cuts, &c).unwrap();
        assert!(!segs.is_empty());
        st.commit(&cuts, carry);
        assert!(st.rows.is_empty());
        let late = Row::of(&snap(9, 5, 0, 0, 1.0, 3380.0, 3379.0, 3382.0));
        assert_eq!(st.push(late.clone(), false), Push::Late);
        assert_eq!(st.push(late, true), Push::Dropped);
        let next = Row::of(&snap(13, 30, 0, 0, 1.0, 3380.0, 3379.0, 3382.0));
        assert_eq!(st.push(next, false), Push::Kept);
    }

    #[test]
    fn scheduled_tail_cut_waits_for_delay_and_a_proven_break() {
        let keep = |_: i64| true;
        let c = ctx(&keep);
        let first: Vec<Snapshot> = stream().into_iter().take(1200).collect();
        let mut st = ContractState::default();
        for r in rows(&first) {
            st.push(r, false);
        }
        let last = st.max_ts_ns;
        let end = last.div_euclid(NS) + 20; // pretend the window ends 20 s later
        let at_end = move |_: i64| Some(end);
        let not_end = |_: i64| None;
        let p = Params {
            margin_ns: 60 * NS,
            tail_quiet_ns: 7200 * NS,
            delay_ns: 600 * NS,
            tail_grace_ns: 300 * NS,
        };
        // A tea-break-like last row (no proven break) is never cut early.
        assert!(st
            .plan_cuts(last, last + 900 * NS, true, p, &not_end)
            .is_empty());
        // Proven break, but the 10 minute delay has not elapsed.
        assert!(st
            .plan_cuts(last, last + 599 * NS, true, p, &at_end)
            .is_empty());
        // Proven break, delay elapsed, but the files are not caught up.
        assert!(st
            .plan_cuts(last, last + 900 * NS, false, p, &at_end)
            .is_empty());
        let cuts = st.plan_cuts(last, last + 600 * NS, true, p, &at_end);
        assert_eq!(
            cuts,
            vec![(end * NS + 300 * NS, last)],
            "cut after the window end + grace"
        );
        let (segs, carry) = st.compute_segments(&cuts, &c).unwrap();
        assert!(!segs.is_empty());
        st.commit(&cuts, carry);
        // A closing snapshot written just after the window end is still late.
        let closing = Row {
            ts_ns: (end + 2) * NS,
            ..Row::of(&snap(9, 5, 0, 0, 1.0, 3380.0, 3379.0, 3382.0))
        };
        assert_eq!(st.push(closing, false), Push::Late);
    }

    #[test]
    fn restart_from_manifest_state_replays_without_double_counting() {
        let keep = |_: i64| true;
        let c = ctx(&keep);
        let all = stream();
        let one = minutes_from_output(
            &c,
            process_instrument(&mut all.clone(), c.exchange).unwrap(),
        )
        .unwrap();
        let mut st = ContractState::default();
        for r in rows(&all) {
            st.push(r, false);
        }
        let p = Params {
            margin_ns: 0,
            tail_quiet_ns: i64::MAX,
            delay_ns: i64::MAX,
            tail_grace_ns: 0,
        };
        let cuts = st.plan_cuts(i64::MAX / 4, 0, true, p, &|_| None);
        let (segs, carry) = st.compute_segments(&cuts, &c).unwrap();
        st.commit(&cuts, carry);

        // "Restart": only the manifest state survives; the files are replayed
        // from the start, so rows a persisted segment covers must be skipped.
        let manifest = carry_to_json(&st.carry).to_string();
        let mut back = ContractState::restored(
            carry_from_json(&serde_json::from_str(&manifest).unwrap()).unwrap(),
            st.persisted_until_ns,
            st.cut_last_ns,
            st.max_ts_ns,
        );
        let mut dropped = 0;
        for r in rows(&all) {
            match back.push(r, true) {
                Push::Dropped => dropped += 1,
                Push::Kept => {}
                Push::Late => panic!("replay never reports late rows"),
            }
        }
        assert!(dropped > 0 && back.rows.len() == st.rows.len());
        let mut got: Vec<BaselineMinute> = segs.into_iter().flat_map(|s| s.minutes).collect();
        got.extend(back.open_minutes(&c).unwrap());
        assert_eq!(format!("{got:?}"), format!("{one:?}"));
    }

    #[test]
    fn persisted_parquet_round_trips_the_minutes_exactly() {
        let keep = |_: i64| true;
        let c = ctx(&keep);
        let mut all = stream();
        let minutes =
            minutes_from_output(&c, process_instrument(&mut all, c.exchange).unwrap()).unwrap();
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("seg.parquet");
        cn_futures_l2::export_1min::write_day_parquet(&path, &minutes).unwrap();
        let back = cn_futures_l2::export_1min::read_day_parquet(&path).unwrap();
        assert_eq!(format!("{back:?}"), format!("{minutes:?}"));
    }

    #[test]
    fn carry_json_round_trips_exactly() {
        let mut all = stream();
        let mut carry = InstrumentCarry::default();
        process_instrument_resumable(&mut all, Exchange::Xsge, &mut carry, true).unwrap();
        assert!(carry.book.is_some() && carry.last_dir.is_some());
        let json = carry_to_json(&carry);
        let back = carry_from_json(&serde_json::from_str(&json.to_string()).unwrap()).unwrap();
        assert_eq!(back, carry);
    }
}
