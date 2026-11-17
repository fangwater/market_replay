//! Virtual-time matching, delayed private reports, and bounded calendar-roll execution.
use crate::{
    config::{Config, Execution, Task},
    input::InputRow,
    metadata::{cny, money, Metadata, Spec},
    output::{Row, Sink},
};
use anyhow::{ensure, Result};
use serde::{Deserialize, Serialize};
use std::{
    cmp::Ordering,
    collections::{BTreeMap, BinaryHeap},
};

#[derive(Clone, Debug)]
pub struct Quote {
    pub ts: i64,
    pub exchange_ts: i64,
    pub cursor: u64,
    pub day: u32,
    pub contract: String,
    pub bid: [Option<i64>; 5],
    pub ask: [Option<i64>; 5],
    pub bid_qty: [i64; 5],
    pub ask_qty: [i64; 5],
    pub last: Option<i64>,
    pub volume: Option<u64>,
    pub end: Option<i64>,
}
impl Quote {
    pub fn mid(&self, s: &Spec) -> Option<f64> {
        let (b, a) = (self.bid[0]?, self.ask[0]?);
        (b <= a).then(|| s.price(b + a) / 2.0)
    }
}
#[derive(Clone, Debug, Default)]
struct Bucket {
    qty: i64,
    cost: i128,
}
#[derive(Clone, Debug, Default)]
struct Position {
    buckets: BTreeMap<u32, Bucket>,
}
impl Position {
    fn lots(&self) -> i64 {
        self.buckets.values().map(|b| b.qty).sum()
    }
    fn apply(&mut self, delta: i64, price: f64, day: u32, s: &Spec) -> (i128, Option<i128>) {
        let sign = delta.signum();
        let mut left = delta.abs();
        let mut pnl = 0;
        let mut fee = s.fees.as_ref().map(|_| 0);
        for (opened, b) in &mut self.buckets {
            if left == 0 || b.qty == 0 || b.qty.signum() == sign {
                continue;
            }
            let n = left.min(b.qty.abs());
            let basis = b.cost * n as i128 / b.qty.abs() as i128;
            pnl += b.qty.signum() as i128 * (money(price * s.multiplier * n as f64) - basis.abs());
            if let (Some(total), Some(f)) = (&mut fee, &s.fees) {
                *total += if *opened == day {
                    &f.close_today
                } else {
                    &f.close
                }
                .calculate(price * s.multiplier * n as f64, n);
            }
            b.qty -= b.qty.signum() * n;
            b.cost -= basis;
            left -= n;
        }
        self.buckets.retain(|_, b| b.qty != 0);
        if left > 0 {
            let b = self.buckets.entry(day).or_default();
            b.qty += sign * left;
            b.cost += sign as i128 * money(price * s.multiplier * left as f64);
            if let (Some(total), Some(f)) = (&mut fee, &s.fees) {
                *total += f.open.calculate(price * s.multiplier * left as f64, left);
            }
        }
        (pnl, fee)
    }
    fn unrealized(&self, price: f64, s: &Spec) -> i128 {
        self.buckets
            .values()
            .map(|b| money(b.qty as f64 * price * s.multiplier) - b.cost)
            .sum()
    }
}
#[derive(Clone, Debug)]
struct Target {
    lots: i64,
    reference: f64,
    ts: i64,
    amount: f64,
}
#[derive(Clone, Debug)]
struct Order {
    id: u64,
    contract: String,
    side: i64,
    remaining: i64,
    local_remaining: i64,
    limit: i64,
    execution: Execution,
    created: i64,
    cursor: u64,
    live: bool,
    cancel_sent: bool,
    reference: f64,
    reason: String,
}
#[derive(Clone, Debug)]
enum Report {
    Fill {
        id: u64,
        contract: String,
        delta: i64,
    },
    Done {
        id: u64,
        reason: String,
    },
}
#[derive(Clone, Debug)]
enum Event {
    Input(InputRow),
    Visible(Box<Quote>),
    Arrive(u64),
    Cancel(u64),
    Report(Report),
    Wake,
    EndSession(String, i64),
}
struct Scheduled {
    ts: i64,
    phase: u8,
    seq: u64,
    event: Event,
}
impl PartialEq for Scheduled {
    fn eq(&self, o: &Self) -> bool {
        (self.ts, self.phase, self.seq) == (o.ts, o.phase, o.seq)
    }
}
impl Eq for Scheduled {}
impl PartialOrd for Scheduled {
    fn partial_cmp(&self, o: &Self) -> Option<Ordering> {
        Some(self.cmp(o))
    }
}
impl Ord for Scheduled {
    fn cmp(&self, o: &Self) -> Ordering {
        (o.ts, o.phase, o.seq).cmp(&(self.ts, self.phase, self.seq))
    }
}

#[derive(Clone, Debug)]
struct Batch {
    a: i64,
    b: i64,
    started: i64,
}
#[derive(Clone, Debug)]
struct RollState {
    old: String,
    new: String,
    initial: i64,
    initial_new: i64,
    goal: i64,
    amount: f64,
    reference: Option<f64>,
    batch: Option<Batch>,
    seeded: bool,
    standalone: bool,
    begin: i64,
    deadline: i64,
    completed: bool,
}
#[derive(Clone, Debug, Default, Serialize, Deserialize)]
pub struct Summary {
    pub model: String,
    pub frames: u64,
    pub fills: u64,
    pub rejected_orders: u64,
    pub filled_lots: u64,
    pub known_fees_cny: f64,
    pub fee_unknown_fills: u64,
    pub fees_cny: Option<f64>,
    pub realized_pnl_cny: f64,
    pub unrealized_pnl_cny: f64,
    pub equity_cny: Option<f64>,
    pub execution_shortfall_cny: f64,
    pub turnover_cny: f64,
    pub execution_cost_bps: Option<f64>,
    pub positions: BTreeMap<String, i64>,
    pub targets: BTreeMap<String, i64>,
    pub unfilled_lots: u64,
    pub max_exposure_deviation_cny: f64,
    pub exposure_deviation_cny_seconds: f64,
    pub exposure_limit_breach_seconds: f64,
    pub roll_complete: bool,
    pub source_complete: bool,
    pub pending_events_peak: usize,
    pub gross_pnl_cny: f64,
    pub net_pnl_cny: Option<f64>,
    pub max_drawdown_cny: f64,
}
pub struct Engine<S: Sink> {
    pub cfg: Config,
    pub metadata: Metadata,
    pub sink: S,
    pub now: i64,
    pub summary: Summary,
    heap: BinaryHeap<Scheduled>,
    seq: u64,
    next_id: u64,
    pub day: u32,
    markets: BTreeMap<String, Quote>,
    visible: BTreeMap<String, Quote>,
    orders: BTreeMap<u64, Order>,
    positions: BTreeMap<String, Position>,
    known: BTreeMap<String, i64>,
    targets: BTreeMap<String, Target>,
    theory: BTreeMap<String, Position>,
    selected_contracts: BTreeMap<String, String>,
    amounts: BTreeMap<String, (f64, i64)>,
    pending: BTreeMap<String, (f64, i64)>,
    rolls: BTreeMap<String, RollState>,
    segment_ends: BTreeMap<String, i64>,
    pnl: i128,
    fees: i128,
    shortfall: i128,
    unknown_fees: u64,
    last_trace: i64,
    last_risk: BTreeMap<String, (i64, f64)>,
    peak_equity: f64,
    cutoff: i64,
    disabled: bool,
}
impl<S: Sink> Engine<S> {
    pub fn new(cfg: Config, metadata: Metadata, sink: S) -> Result<Self> {
        cfg.validate()?;
        let mut e = Self {
            peak_equity: cfg.capital_cny,
            cfg,
            metadata,
            sink,
            now: 0,
            summary: Summary {
                model: "strict_trade_through_full_fill/v1;maker_maker_roll_v1".into(),
                source_complete: true,
                ..Default::default()
            },
            heap: BinaryHeap::new(),
            seq: 0,
            next_id: 0,
            day: 0,
            markets: BTreeMap::new(),
            visible: BTreeMap::new(),
            orders: BTreeMap::new(),
            positions: BTreeMap::new(),
            known: BTreeMap::new(),
            targets: BTreeMap::new(),
            theory: BTreeMap::new(),
            selected_contracts: BTreeMap::new(),
            amounts: BTreeMap::new(),
            pending: BTreeMap::new(),
            rolls: BTreeMap::new(),
            segment_ends: BTreeMap::new(),
            pnl: 0,
            fees: 0,
            shortfall: 0,
            unknown_fees: 0,
            last_trace: 0,
            last_risk: BTreeMap::new(),
            cutoff: i64::MAX,
            disabled: false,
        };
        for p in e.cfg.initial_positions.clone() {
            let s = e.metadata.spec(&p.contract)?;
            let c = p.contract.to_ascii_lowercase();
            ensure!(
                p.cost_price > 0.0 && s.ticks(p.cost_price).is_some(),
                "invalid initial cost"
            );
            let mut pos = Position::default();
            pos.apply(
                p.lots,
                p.cost_price,
                crate::day_number(&p.opened_trading_day)?,
                &s,
            );
            e.positions.insert(c.clone(), pos.clone());
            e.theory.insert(c.clone(), pos);
            e.known.insert(c, p.lots);
        }
        if e.cfg.task == Task::RollCost {
            let r = &e.cfg.roll;
            let old = r.old_contract.to_ascii_lowercase();
            let new = r.new_contract.to_ascii_lowercase();
            e.metadata.spec(&old)?;
            e.metadata.spec(&new)?;
            let begin = r
                .begin
                .as_deref()
                .map(crate::timestamp)
                .transpose()?
                .unwrap_or(0);
            let deadline = r
                .deadline
                .as_deref()
                .map(crate::timestamp)
                .transpose()?
                .unwrap_or(i64::MAX);
            ensure!(deadline > begin, "roll deadline must follow begin");
            e.cutoff = deadline;
            e.rolls.insert(
                crate::product(&old),
                RollState {
                    old,
                    new,
                    initial: r.initial_lots,
                    initial_new: 0,
                    goal: r.target_lots.unwrap_or(0),
                    amount: r.target_cny.unwrap_or(0.0),
                    reference: None,
                    batch: None,
                    seeded: false,
                    standalone: true,
                    begin,
                    deadline,
                    completed: false,
                },
            );
        }
        Ok(e)
    }
    fn emit(&mut self, table: &str, mut row: Row) -> Result<()> {
        row.timestamp_us = self.now;
        self.sink.emit(table, row)
    }
    fn push(&mut self, ts: i64, phase: u8, event: Event) -> Result<()> {
        ensure!(
            self.heap.len() < self.cfg.max_pending_events,
            "pending-event budget exceeded; reduce latency/product scope"
        );
        self.seq += 1;
        self.heap.push(Scheduled {
            ts,
            phase,
            seq: self.seq,
            event,
        });
        self.summary.pending_events_peak = self.summary.pending_events_peak.max(self.heap.len());
        Ok(())
    }
    pub fn input(&mut self, row: InputRow) -> Result<()> {
        self.push(row.ts, 2, Event::Input(row))
    }
    /// The API-selected contract may move either forward or backward in maturity.
    pub fn begin_day(
        &mut self,
        day: u32,
        selected_contracts: BTreeMap<String, String>,
    ) -> Result<()> {
        self.day = day;
        if self.cfg.task != Task::RollCost {
            for (p, new) in &selected_contracts {
                if self.selected_contracts.get(p) == Some(new) {
                    continue;
                }
                let obsolete: std::collections::BTreeSet<_> = self
                    .targets
                    .keys()
                    .chain(self.known.keys())
                    .chain(self.orders.values().map(|o| &o.contract))
                    .filter(|c| *c != new && crate::product(c) == *p)
                    .cloned()
                    .collect();
                self.cancel_contract(new)?;
                for old in &obsolete {
                    self.cancel_contract(old)?;
                    let spec = self.metadata.spec(old)?;
                    let reference = self
                        .markets
                        .get(old)
                        .and_then(|q| q.mid(&spec))
                        .unwrap_or(0.0);
                    self.targets.insert(
                        old.clone(),
                        Target {
                            lots: 0,
                            reference,
                            ts: self.now,
                            amount: 0.0,
                        },
                    );
                }
                let held: Vec<_> = obsolete
                    .iter()
                    .filter(|old| self.known.get(*old).is_some_and(|q| *q != 0))
                    .collect();
                ensure!(held.len()<=1,"{p}: selection changed with multiple unfinished old contracts; reconcile the previous roll before continuing");
                self.rolls.remove(p);
                if let Some(old) = held.first() {
                    self.rolls.insert(
                        p.clone(),
                        RollState {
                            old: (*old).clone(),
                            new: new.clone(),
                            initial: self.known[*old],
                            initial_new: *self.known.get(new).unwrap_or(&0),
                            goal: 0,
                            amount: self.amounts.get(p).map_or(0.0, |x| x.0),
                            reference: None,
                            batch: None,
                            seeded: true,
                            standalone: false,
                            begin: 0,
                            deadline: i64::MAX,
                            completed: false,
                        },
                    );
                }
            }
        }
        self.selected_contracts = selected_contracts;
        for (p, (a, t)) in self.amounts.clone() {
            self.pending.insert(p, (a, t));
        }
        Ok(())
    }
    pub fn quote(&mut self, q: Quote) -> Result<()> {
        if q.ts > self.cutoff {
            self.advance(self.cutoff, true)?;
            return Ok(());
        }
        self.advance(q.ts, false)?;
        ensure!(q.ts >= self.now, "source clock went backwards");
        self.now = q.ts;
        self.summary.frames += 1;
        let s = self.metadata.spec(&q.contract)?;
        let delta = self
            .markets
            .get(&q.contract)
            .and_then(|prev| {
                if prev.day != q.day
                    || prev.end != q.end
                    || q.ts - prev.ts > self.cfg.quote_age_ms as i64 * 1000
                {
                    return None;
                }
                q.volume?.checked_sub(prev.volume?)
            })
            .unwrap_or(0);
        if let Some(r) = self.rolls.get_mut(&s.product) {
            if r.standalone && !r.seeded && r.old == q.contract && q.ts >= r.begin {
                if let Some(mid) = q.mid(&s) {
                    let mut p = Position::default();
                    p.apply(r.initial, mid, q.day.saturating_sub(1), &s);
                    self.positions.insert(q.contract.clone(), p.clone());
                    self.theory.insert(q.contract.clone(), p);
                    self.known.insert(q.contract.clone(), r.initial);
                    r.seeded = true;
                }
            }
        }
        self.markets.insert(q.contract.clone(), q.clone());
        if let Some(end) = q.end.filter(|end| *end > q.ts) {
            if self.segment_ends.get(&q.contract) != Some(&end) {
                self.segment_ends.insert(q.contract.clone(), end);
                self.push(end, 0, Event::EndSession(q.contract.clone(), end))?;
            }
        }
        if q.ts <= self.cutoff
            && q.end.is_some_and(|end| q.ts < end)
            && s.segment_end(q.exchange_ts).is_some()
            && delta > 0
        {
            let ids: Vec<_> = self
                .orders
                .values()
                .filter(|o| {
                    o.live
                        && o.execution == Execution::Maker
                        && o.contract == q.contract
                        && q.cursor > o.cursor
                        && q.last
                            .is_some_and(|p| if o.side > 0 { p < o.limit } else { p > o.limit })
                })
                .map(|o| o.id)
                .collect();
            for id in ids {
                let n = self.orders[&id].remaining;
                self.fill(id, n, q.cursor)?;
            }
        }
        self.resolve(&s.product)?;
        self.push(
            q.ts + self.cfg.md_delay_ms as i64 * 1000,
            3,
            Event::Visible(Box::new(q)),
        )?;
        self.trace(false)?;
        Ok(())
    }
    pub fn advance(&mut self, ts: i64, inclusive: bool) -> Result<()> {
        while self
            .heap
            .peek()
            .is_some_and(|e| if inclusive { e.ts <= ts } else { e.ts < ts })
        {
            let e = self.heap.pop().unwrap();
            self.now = e.ts;
            match e.event {
                Event::Input(r) => {
                    for (p, v) in r.values {
                        let a = self.cfg.amount(v);
                        ensure!(a.is_finite(), "input amount overflow");
                        self.amounts.insert(p.clone(), (a, r.ts));
                        self.pending.insert(p.clone(), (a, r.ts));
                        self.resolve(&p)?;
                    }
                    self.control()?;
                }
                Event::Visible(q) => {
                    self.visible.insert(q.contract.clone(), *q);
                    self.control()?;
                }
                Event::Arrive(id) => self.arrive(id)?,
                Event::Cancel(id) => self.done(id, "cancelled")?,
                Event::Report(r) => {
                    match r {
                        Report::Fill {
                            id,
                            contract,
                            delta,
                        } => {
                            *self.known.entry(contract).or_default() += delta;
                            if let Some(o) = self.orders.get_mut(&id) {
                                o.local_remaining -= delta.abs();
                            }
                        }
                        Report::Done { id, reason } => {
                            if let Some(o) = self.orders.remove(&id) {
                                self.emit(
                                    "orders",
                                    Row {
                                        kind: "reported_terminal".into(),
                                        contract: o.contract,
                                        order_id: Some(id),
                                        reason,
                                        ..Default::default()
                                    },
                                )?;
                            }
                        }
                    }
                    self.control()?;
                }
                Event::Wake => {
                    self.control()?;
                }
                Event::EndSession(c, end) => {
                    let ids: Vec<_> = self
                        .orders
                        .values()
                        .filter(|o| {
                            o.contract == c && o.live && self.segment_ends.get(&c) == Some(&end)
                        })
                        .map(|o| o.id)
                        .collect();
                    for id in ids {
                        self.done(id, "session_expired")?;
                    }
                }
            }
        }
        Ok(())
    }
    fn fresh<'a>(&'a self, c: &str, view: &'a BTreeMap<String, Quote>) -> Option<&'a Quote> {
        view.get(c).filter(|q| {
            q.ts <= self.now
                && self.now - q.ts <= self.cfg.quote_age_ms as i64 * 1000
                && q.end.is_some_and(|e| self.now < e)
        })
    }
    fn resolve(&mut self, product: &str) -> Result<()> {
        let Some(&(amount, input_ts)) = self.pending.get(product) else {
            return Ok(());
        };
        let Some(contract) = self.selected_contracts.get(product).cloned() else {
            return Ok(());
        };
        let s = self.metadata.spec(&contract)?;
        let q = self.fresh(&contract, &self.markets);
        let reference = q.and_then(|q| q.mid(&s));
        if reference.is_none() && amount != 0.0 {
            return Ok(());
        }
        let price = reference.unwrap_or(0.0);
        let raw_lots = if amount == 0.0 {
            0.0
        } else {
            amount / (price * s.multiplier)
        };
        ensure!(
            raw_lots.is_finite() && raw_lots.abs() < 1_000_000_000.0,
            "invalid target size {contract}"
        );
        let lots = raw_lots.trunc() as i64;
        let ref_ts = q.map(|q| q.ts);
        let cursor = q.map(|q| q.cursor);
        self.pending.remove(product);
        let previous = self.targets.insert(
            contract.clone(),
            Target {
                lots,
                reference: price,
                ts: input_ts,
                amount,
            },
        );
        self.emit(
            "target_lots",
            Row {
                kind: "target".into(),
                product: product.into(),
                contract: contract.clone(),
                target_lots: Some(lots),
                price: reference,
                reference_price: reference,
                multiplier: Some(s.multiplier),
                target_cny: Some(amount),
                value: Some(amount - lots as f64 * price * s.multiplier),
                input_ts_us: Some(input_ts),
                reference_ts_us: ref_ts,
                source_cursor: cursor,
                reason: "capital-to-notional;truncate-toward-zero;producer-clock".into(),
                ..Default::default()
            },
        )?;
        let theoretical = self.theory.entry(contract.clone()).or_default();
        let delta = lots - theoretical.lots();
        if delta != 0 {
            let estimate = q_price(
                &self.markets,
                &contract,
                &s,
                delta.signum(),
                self.cfg.execution,
            )
            .unwrap_or(price);
            if estimate > 0.0 {
                let (_, fee) = theoretical.apply(delta, estimate, self.day, &s);
                self.emit(
                    "cost_estimate",
                    Row {
                        kind: "assumed_full_completion".into(),
                        product: product.into(),
                        contract: contract.clone(),
                        side: Some(delta.signum()),
                        lots: Some(delta.abs()),
                        price: Some(estimate),
                        fee_cny: fee.map(cny),
                        shortfall_cny: Some(delta as f64 * s.multiplier * (estimate - price)),
                        target_lots: Some(lots),
                        reason: s.fee_source.clone(),
                        ..Default::default()
                    },
                )?;
            }
        }
        if let Some(r) = self.rolls.get_mut(product) {
            if !r.standalone {
                r.goal = lots;
                r.amount = amount;
            }
        }
        if previous.as_ref().is_some_and(|p| p.lots != lots) {
            self.cancel_contract(&contract)?;
        }
        Ok(())
    }
    fn submit(
        &mut self,
        c: &str,
        delta: i64,
        mode: Execution,
        reference: f64,
        reason: &str,
    ) -> Result<Option<u64>> {
        if delta == 0 || self.disabled || self.now >= self.cutoff {
            return Ok(None);
        }
        let Some(q) = self.fresh(c, &self.visible).cloned() else {
            return Ok(None);
        };
        let s = self.metadata.spec(c)?;
        let (side, n) = (delta.signum(), delta.abs().min(self.cfg.max_child_lots));
        let Some(base) = (if mode == Execution::Maker {
            if side > 0 {
                q.bid[0]
            } else {
                q.ask[0]
            }
        } else if side > 0 {
            q.ask[0]
        } else {
            q.bid[0]
        }) else {
            return Ok(None);
        };
        let offset = if mode == Execution::Maker {
            -self.cfg.maker_offset_ticks
        } else {
            self.cfg.protection_ticks
        };
        let limit = base + side * offset;
        ensure!(limit > 0, "non-positive limit");
        self.next_id += 1;
        let id = self.next_id;
        self.orders.insert(
            id,
            Order {
                id,
                contract: c.into(),
                side,
                remaining: n,
                local_remaining: n,
                limit,
                execution: mode,
                created: self.now,
                cursor: q.cursor,
                live: false,
                cancel_sent: false,
                reference,
                reason: reason.into(),
            },
        );
        self.emit(
            "orders",
            Row {
                kind: "submitted".into(),
                product: s.product.clone(),
                contract: c.into(),
                order_id: Some(id),
                side: Some(side),
                lots: Some(n),
                price: Some(s.price(limit)),
                reference_price: Some(reference),
                reason: reason.into(),
                ..Default::default()
            },
        )?;
        self.push(
            self.now + (self.cfg.compute_delay_ms + self.cfg.order_delay_ms) as i64 * 1000,
            4,
            Event::Arrive(id),
        )?;
        self.push(self.now + self.cfg.reprice_ms as i64 * 1000, 5, Event::Wake)?;
        Ok(Some(id))
    }
    fn cancel_contract(&mut self, c: &str) -> Result<()> {
        let ids: Vec<_> = self
            .orders
            .values()
            .filter(|o| o.contract == c && !o.cancel_sent)
            .map(|o| o.id)
            .collect();
        for id in ids {
            self.orders.get_mut(&id).unwrap().cancel_sent = true;
            self.emit(
                "orders",
                Row {
                    kind: "cancel_requested".into(),
                    contract: c.into(),
                    order_id: Some(id),
                    ..Default::default()
                },
            )?;
            self.push(
                self.now + self.cfg.cancel_delay_ms as i64 * 1000,
                4,
                Event::Cancel(id),
            )?;
        }
        Ok(())
    }
    fn done(&mut self, id: u64, reason: &str) -> Result<()> {
        let Some(o) = self.orders.get_mut(&id) else {
            return Ok(());
        };
        // A terminal report has already been scheduled when remaining becomes zero.
        if o.remaining == 0 {
            return Ok(());
        }
        let c = o.contract.clone();
        o.live = false;
        o.remaining = 0;
        if reason.contains("reject") {
            self.summary.rejected_orders += 1;
        }
        self.emit(
            "orders",
            Row {
                kind: reason.into(),
                contract: c,
                order_id: Some(id),
                ..Default::default()
            },
        )?;
        self.push(
            self.now + self.cfg.report_delay_ms as i64 * 1000,
            1,
            Event::Report(Report::Done {
                id,
                reason: reason.into(),
            }),
        )
    }
    fn arrive(&mut self, id: u64) -> Result<()> {
        let Some(o) = self.orders.get(&id).cloned() else {
            return Ok(());
        };
        if o.remaining == 0 {
            return Ok(());
        }
        if self.now >= self.cutoff {
            return self.done(id, "deadline_expired");
        }
        let Some(q) = self.fresh(&o.contract, &self.markets).cloned() else {
            return self.done(id, "reject_stale_book");
        };
        let Some(opposite) = (if o.side > 0 { q.ask[0] } else { q.bid[0] }) else {
            return self.done(id, "reject_missing_book");
        };
        if o.execution == Execution::Maker {
            if o.side * (o.limit - opposite) >= 0 {
                return self.done(id, "reject_post_only");
            }
            let x = self.orders.get_mut(&id).unwrap();
            x.live = true;
            x.cursor = x.cursor.max(q.cursor);
            self.emit(
                "orders",
                Row {
                    kind: "live".into(),
                    contract: o.contract,
                    order_id: Some(id),
                    source_cursor: Some(q.cursor),
                    ..Default::default()
                },
            )?;
        } else {
            for level in 0..5 {
                let n = self.orders[&id].remaining;
                if n == 0 {
                    break;
                }
                let book = self.markets.get_mut(&o.contract).unwrap();
                let (p, qty) = if o.side > 0 {
                    (book.ask[level], &mut book.ask_qty[level])
                } else {
                    (book.bid[level], &mut book.bid_qty[level])
                };
                let Some(p) = p else {
                    continue;
                };
                if o.side * (p - o.limit) > 0 {
                    break;
                }
                let take = n.min(*qty);
                *qty -= take;
                if take > 0 {
                    self.fill_at(id, take, p, q.cursor)?;
                }
            }
            self.done(id, "ioc_cancelled")?;
        }
        Ok(())
    }
    fn fill(&mut self, id: u64, n: i64, cursor: u64) -> Result<()> {
        let p = self.orders[&id].limit;
        self.fill_at(id, n, p, cursor)
    }
    fn fill_at(&mut self, id: u64, n: i64, price_ticks: i64, cursor: u64) -> Result<()> {
        let o = self.orders[&id].clone();
        ensure!(n > 0 && n <= o.remaining, "invalid fill size");
        let s = self.metadata.spec(&o.contract)?;
        let price = s.price(price_ticks);
        let day = self.markets.get(&o.contract).map_or(self.day, |q| q.day);
        let (pnl, fee) =
            self.positions
                .entry(o.contract.clone())
                .or_default()
                .apply(o.side * n, price, day, &s);
        self.pnl += pnl;
        if let Some(f) = fee {
            self.fees += f;
        } else {
            self.unknown_fees += 1;
        }
        let shortfall = money(o.side as f64 * n as f64 * s.multiplier * (price - o.reference));
        self.shortfall += shortfall;
        self.summary.fills += 1;
        self.summary.filled_lots += n as u64;
        self.summary.turnover_cny += n as f64 * price * s.multiplier;
        let x = self.orders.get_mut(&id).unwrap();
        x.remaining -= n;
        let terminal = x.remaining == 0;
        if terminal {
            x.live = false;
        }
        self.emit(
            "fills",
            Row {
                kind: format!("{:?}", o.execution).to_ascii_lowercase(),
                product: s.product.clone(),
                contract: o.contract.clone(),
                order_id: Some(id),
                side: Some(o.side),
                lots: Some(n),
                price: Some(price),
                reference_price: Some(o.reference),
                multiplier: Some(s.multiplier),
                fee_cny: fee.map(cny),
                pnl_cny: Some(cny(pnl)),
                shortfall_cny: Some(cny(shortfall)),
                source_cursor: Some(cursor),
                reason: o.reason,
                ..Default::default()
            },
        )?;
        self.push(
            self.now + self.cfg.report_delay_ms as i64 * 1000,
            1,
            Event::Report(Report::Fill {
                id,
                contract: o.contract.clone(),
                delta: o.side * n,
            }),
        )?;
        if terminal {
            self.push(
                self.now + self.cfg.report_delay_ms as i64 * 1000,
                1,
                Event::Report(Report::Done {
                    id,
                    reason: "filled".into(),
                }),
            )?;
        }
        self.trace(true)?;
        Ok(())
    }
    fn working(&self, c: &str) -> bool {
        self.orders.values().any(|o| o.contract == c)
    }
    fn control(&mut self) -> Result<()> {
        if self.disabled || self.cfg.task == Task::Convert || self.now >= self.cutoff {
            return Ok(());
        }
        let active: Vec<_> = self.rolls.keys().cloned().collect();
        for p in active {
            let mut r = self.rolls.remove(&p).unwrap();
            self.roll_control(&p, &mut r)?;
            self.rolls.insert(p, r);
        }
        let targets: Vec<_> = self
            .targets
            .iter()
            .map(|(c, t)| (c.clone(), t.clone()))
            .collect();
        for (c, t) in targets {
            let p = crate::product(&c);
            if self.rolls.get(&p).is_some_and(|r| !r.completed) {
                continue;
            }
            let known = *self.known.get(&c).unwrap_or(&0);
            if self.working(&c) {
                self.reprice(&c, self.cfg.reprice_ms)?;
                continue;
            }
            let mut delta = t.lots - known;
            if known != 0 && delta.signum() != known.signum() {
                delta = delta.signum() * delta.abs().min(known.abs());
            }
            // Finish obsolete contracts before increasing the replacement position.
            if known == 0
                && t.lots != 0
                && self
                    .known
                    .iter()
                    .any(|(other, q)| other != &c && crate::product(other) == p && *q != 0)
            {
                continue;
            }
            if delta != 0 {
                self.submit(
                    &c,
                    delta,
                    self.cfg.execution,
                    t.reference,
                    "target_rebalance",
                )?;
            }
        }
        Ok(())
    }
    fn reprice(&mut self, c: &str, ms: u64) -> Result<()> {
        if self
            .orders
            .values()
            .any(|o| o.contract == c && !o.cancel_sent && self.now - o.created >= ms as i64 * 1000)
        {
            self.cancel_contract(c)?;
        }
        Ok(())
    }
    fn risk_interval(&self, r: &RollState) -> Option<(f64, f64)> {
        let mut e = 0.0;
        let mut lo = 0.0;
        let mut hi = 0.0;
        for c in [&r.old, &r.new] {
            let s = self.metadata.spec(c).ok()?;
            let p = self.fresh(c, &self.visible)?.mid(&s)?;
            e += *self.known.get(c).unwrap_or(&0) as f64 * p * s.multiplier;
            for o in self.orders.values().filter(|o| &o.contract == c) {
                let d = o.side as f64 * o.local_remaining as f64 * p * s.multiplier;
                lo += d.min(0.0);
                hi += d.max(0.0);
            }
        }
        Some((e + lo, e + hi))
    }
    fn risk_allows(&self, r: &RollState, c: &str, delta: i64) -> bool {
        let Some((lo, hi)) = self.risk_interval(r) else {
            return false;
        };
        let Ok(s) = self.metadata.spec(c) else {
            return false;
        };
        let Some(p) = self.fresh(c, &self.visible).and_then(|q| q.mid(&s)) else {
            return false;
        };
        let d = delta as f64 * p * s.multiplier;
        let before = (lo - r.amount).abs().max((hi - r.amount).abs());
        let after = (lo + d.min(0.0) - r.amount)
            .abs()
            .max((hi + d.max(0.0) - r.amount).abs());
        after <= self.cfg.roll.max_exposure_deviation_cny + 1e-6 || after < before - 1e-6
    }
    fn roll_control(&mut self, p: &str, r: &mut RollState) -> Result<()> {
        if !r.seeded || self.now < r.begin || self.now >= r.deadline || r.completed {
            return Ok(());
        }
        let (Some(a), Some(b)) = (
            self.fresh(&r.old, &self.visible).cloned(),
            self.fresh(&r.new, &self.visible).cloned(),
        ) else {
            return Ok(());
        };
        if (a.ts - b.ts).abs() > self.cfg.roll.max_quote_skew_ms as i64 * 1000 {
            return Ok(());
        }
        let (sa, sb) = (self.metadata.spec(&r.old)?, self.metadata.spec(&r.new)?);
        let (Some(pa), Some(pb)) = (a.mid(&sa), b.mid(&sb)) else {
            return Ok(());
        };
        if r.reference.is_none() {
            ensure!(
                (sa.multiplier - sb.multiplier).abs() < 1e-8,
                "roll multiplier mismatch"
            );
            r.reference = Some(pb - pa);
            if r.standalone {
                if r.amount == 0.0 {
                    r.amount = r.initial as f64 * pa * sa.multiplier;
                }
                if self.cfg.roll.target_lots.is_none() {
                    r.goal = (r.amount / (pb * sb.multiplier)).trunc() as i64;
                }
            }
            self.targets.insert(
                r.old.clone(),
                Target {
                    lots: 0,
                    reference: pa,
                    ts: self.now,
                    amount: 0.0,
                },
            );
            self.targets.insert(
                r.new.clone(),
                Target {
                    lots: r.goal,
                    reference: pb,
                    ts: self.now,
                    amount: r.amount,
                },
            );
            self.emit(
                "roll_events",
                Row {
                    kind: "begin".into(),
                    product: p.into(),
                    contract: r.old.clone(),
                    actual_lots: Some(r.initial),
                    target_lots: Some(r.goal),
                    target_cny: Some(r.amount),
                    value: r.reference,
                    reason: r.new.clone(),
                    ..Default::default()
                },
            )?;
        }
        let ka = *self.known.get(&r.old).unwrap_or(&0);
        let kb = *self.known.get(&r.new).unwrap_or(&0);
        if ka == 0 && kb == r.goal && !self.working(&r.old) && !self.working(&r.new) {
            r.completed = true;
            self.emit(
                "roll_events",
                Row {
                    kind: "completed".into(),
                    product: p.into(),
                    contract: r.new.clone(),
                    actual_lots: Some(kb),
                    target_cny: Some(r.amount),
                    ..Default::default()
                },
            )?;
            return Ok(());
        }
        if r.goal != 0 && r.goal.signum() != r.initial.signum() {
            self.cancel_contract(&r.old)?;
            self.cancel_contract(&r.new)?;
            r.completed = true;
            return Ok(());
        }
        if let Some(batch) = &r.batch {
            if ka == batch.a && kb == batch.b && !self.working(&r.old) && !self.working(&r.new) {
                self.emit(
                    "roll_batches",
                    Row {
                        kind: "batch_completed".into(),
                        product: p.into(),
                        contract: r.new.clone(),
                        actual_lots: Some(kb),
                        value: Some((self.now - batch.started) as f64 / 1000.0),
                        ..Default::default()
                    },
                )?;
                r.batch = None;
            }
        }
        if r.batch.is_none() {
            if self.working(&r.old) || self.working(&r.new) {
                self.reprice(&r.old, self.cfg.reprice_ms)?;
                self.reprice(&r.new, self.cfg.reprice_ms)?;
                return Ok(());
            }
            let sign = r.initial.signum();
            let quote_spread = if sign > 0 {
                sb.price(b.bid[0].unwrap()) - sa.price(a.ask[0].unwrap())
            } else {
                sb.price(b.ask[0].unwrap()) - sa.price(a.bid[0].unwrap())
            };
            let improvement = self.cfg.roll.spread_improvement_ticks * sa.tick;
            if self.cfg.execution == Execution::Maker
                && self.cfg.roll.use_spread_gate
                && (quote_spread - r.reference.unwrap()) * sign as f64 > -improvement + 1e-9
            {
                return Ok(());
            }
            let new_delta = r.goal - r.initial_new;
            let denom = r.initial.abs().max(new_delta.abs()).max(1) as f64;
            let new_progress = if new_delta == 0 {
                0.0
            } else {
                (kb - r.initial_new) as f64 / new_delta as f64
            };
            let progress = (1.0 - ka.abs() as f64 / r.initial.abs() as f64)
                .max(new_progress)
                .clamp(0.0, 1.0);
            let next = (progress + self.cfg.roll.batch_lots as f64 / denom).min(1.0);
            let next_a = sign * (r.initial.abs() as f64 * (1.0 - next)).round() as i64;
            let next_b = r.initial_new + (new_delta as f64 * next).round() as i64;
            r.batch = Some(Batch {
                a: next_a,
                b: next_b,
                started: self.now,
            });
            self.emit(
                "roll_batches",
                Row {
                    kind: "batch_started".into(),
                    product: p.into(),
                    contract: r.old.clone(),
                    actual_lots: Some(next_a),
                    target_lots: Some(next_b),
                    value: Some(quote_spread),
                    reference_price: r.reference,
                    ..Default::default()
                },
            )?;
        }
        let batch = r.batch.as_ref().unwrap().clone();
        let mut deltas = vec![
            (r.old.clone(), batch.a - ka, pa),
            (r.new.clone(), batch.b - kb, pb),
        ];
        // Prioritize the leg reducing the current confirmed exposure error.
        let confirmed = ka as f64 * pa * sa.multiplier + kb as f64 * pb * sb.multiplier - r.amount;
        deltas.sort_by_key(|(_, d, _)| if (*d as f64 * confirmed) < 0.0 { 0 } else { 1 });
        for (c, delta, reference) in deltas {
            if self.working(&c) {
                let ms =
                    if self.now - batch.started >= self.cfg.roll.repair_timeout_ms as i64 * 1000 {
                        self.cfg
                            .reprice_ms
                            .min(self.cfg.roll.repair_timeout_ms.max(1))
                    } else {
                        self.cfg.reprice_ms
                    };
                self.reprice(&c, ms)?;
                continue;
            }
            let d = delta.signum()
                * delta
                    .abs()
                    .min(self.cfg.max_child_lots)
                    .min(self.cfg.roll.batch_lots);
            if d != 0 && self.risk_allows(r, &c, d) {
                self.submit(&c, d, self.cfg.execution, reference, "calendar_roll")?;
            }
        }
        Ok(())
    }
    fn trace(&mut self, force: bool) -> Result<()> {
        if !force && self.now - self.last_trace < 1_000_000 {
            return Ok(());
        }
        self.last_trace = self.now;
        let mut unreal = 0;
        let mut marks_complete = true;
        for (c, p) in &self.positions {
            if p.lots() == 0 {
                continue;
            }
            let s = self.metadata.spec(c)?;
            if let Some(px) = self.markets.get(c).and_then(|q| q.mid(&s)) {
                unreal += p.unrealized(px, &s);
            } else {
                marks_complete = false;
            }
        }
        let eq = self.cfg.capital_cny + cny(self.pnl + unreal - self.fees);
        self.peak_equity = self.peak_equity.max(eq);
        self.summary.max_drawdown_cny = self.summary.max_drawdown_cny.max(self.peak_equity - eq);
        self.summary.unrealized_pnl_cny = cny(unreal);
        self.summary.equity_cny = (self.unknown_fees == 0 && marks_complete).then_some(eq);
        self.emit(
            "equity",
            Row {
                kind: "mtm".into(),
                value: Some(eq),
                pnl_cny: Some(cny(self.pnl + unreal)),
                fee_cny: (self.unknown_fees == 0).then(|| cny(self.fees)),
                reason: if marks_complete {
                    "last_known_marks"
                } else {
                    "missing_marks"
                }
                .into(),
                ..Default::default()
            },
        )?;
        let rolls: Vec<_> = self
            .rolls
            .iter()
            .map(|(p, r)| (p.clone(), r.clone()))
            .collect();
        for (p, r) in rolls {
            if r.reference.is_none() || r.completed {
                continue;
            }
            let mut exposure = 0.0;
            for c in [&r.old, &r.new] {
                let s = self.metadata.spec(c)?;
                if let Some(px) = self.markets.get(c).and_then(|q| q.mid(&s)) {
                    exposure +=
                        self.positions.get(c).map_or(0, Position::lots) as f64 * px * s.multiplier;
                }
            }
            let deviation = exposure - r.amount;
            if let Some((prev_ts, prev)) = self.last_risk.insert(p.clone(), (self.now, deviation)) {
                let dt = (self.now - prev_ts) as f64 / 1_000_000.0;
                self.summary.exposure_deviation_cny_seconds += prev.abs() * dt;
                if prev.abs() > self.cfg.roll.max_exposure_deviation_cny {
                    self.summary.exposure_limit_breach_seconds += dt;
                }
            }
            self.summary.max_exposure_deviation_cny =
                self.summary.max_exposure_deviation_cny.max(deviation.abs());
            self.emit(
                "roll_exposure",
                Row {
                    kind: "actual".into(),
                    product: p,
                    exposure_cny: Some(exposure),
                    target_cny: Some(r.amount),
                    value: Some(deviation),
                    ..Default::default()
                },
            )?;
        }
        Ok(())
    }
    pub fn required_contracts(&self) -> Vec<String> {
        self.positions
            .iter()
            .filter(|(_, p)| p.lots() != 0)
            .map(|(c, _)| c.clone())
            .chain(self.orders.values().map(|o| o.contract.clone()))
            .collect()
    }
    pub fn finish(&mut self, cutoff: i64) -> Result<Summary> {
        self.cutoff = self.cutoff.min(cutoff);
        self.advance(self.cutoff, true)?;
        self.now = self.cutoff;
        self.disabled = true;
        let ids: Vec<_> = self.orders.keys().copied().collect();
        for id in ids {
            self.done(id, "run_expired")?;
        }
        // Drain delayed reports, but never execute arrivals or create trades beyond the cutoff.
        while let Some(e) = self.heap.pop() {
            if let Event::Report(r) = e.event {
                self.now = e.ts;
                match r {
                    Report::Fill {
                        id,
                        contract,
                        delta,
                    } => {
                        *self.known.entry(contract).or_default() += delta;
                        if let Some(o) = self.orders.get_mut(&id) {
                            o.local_remaining -= delta.abs();
                        }
                    }
                    Report::Done { id, .. } => {
                        self.orders.remove(&id);
                    }
                }
            }
        }
        self.now = self.cutoff;
        self.trace(true)?;
        for (c, t) in self.targets.clone() {
            let actual = self.positions.get(&c).map_or(0, Position::lots);
            self.emit(
                "positions",
                Row {
                    kind: "final".into(),
                    product: crate::product(&c),
                    contract: c.clone(),
                    target_lots: Some(t.lots),
                    actual_lots: Some(actual),
                    target_cny: Some(t.amount),
                    input_ts_us: Some(t.ts),
                    ..Default::default()
                },
            )?;
            self.summary.targets.insert(c, t.lots);
        }
        self.summary.positions = self
            .positions
            .iter()
            .map(|(c, p)| (c.clone(), p.lots()))
            .collect();
        self.summary.unfilled_lots = self
            .targets
            .iter()
            .map(|(c, t)| (t.lots - self.positions.get(c).map_or(0, Position::lots)).unsigned_abs())
            .sum();
        self.summary.roll_complete = self.rolls.values().all(|r| {
            self.positions.get(&r.old).map_or(0, Position::lots) == 0
                && self.positions.get(&r.new).map_or(0, Position::lots) == r.goal
                && r.reference.is_some()
        });
        self.summary.known_fees_cny = cny(self.fees);
        self.summary.fee_unknown_fills = self.unknown_fees;
        self.summary.fees_cny = (self.unknown_fees == 0).then(|| cny(self.fees));
        self.summary.realized_pnl_cny = cny(self.pnl);
        self.summary.gross_pnl_cny = cny(self.pnl) + self.summary.unrealized_pnl_cny;
        self.summary.net_pnl_cny =
            (self.unknown_fees == 0).then(|| self.summary.gross_pnl_cny - cny(self.fees));
        self.summary.execution_shortfall_cny = cny(self.shortfall);
        self.summary.execution_cost_bps =
            if self.summary.turnover_cny > 0.0 && self.unknown_fees == 0 {
                Some((cny(self.shortfall + self.fees)) / self.summary.turnover_cny * 10000.0)
            } else {
                None
            };
        Ok(self.summary.clone())
    }
}
fn q_price(
    view: &BTreeMap<String, Quote>,
    c: &str,
    s: &Spec,
    side: i64,
    mode: Execution,
) -> Option<f64> {
    let q = view.get(c)?;
    let p = if (side > 0) == (mode == Execution::Taker) {
        q.ask[0]?
    } else {
        q.bid[0]?
    };
    Some(s.price(p))
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::{
        metadata::{Fee, Fees, Window},
        output::MemorySink,
    };
    fn start() -> i64 {
        crate::timestamp("2026-11-13 09:00:00").unwrap()
    }
    fn metadata() -> Metadata {
        let s = Spec {
            product: "RB".into(),
            exchange: "xsge".into(),
            venue: "SHFE".into(),
            multiplier: 10.0,
            tick: 1.0,
            fees: Some(Fees {
                open: Fee {
                    rate: 0.0,
                    per_lot: 1.0,
                },
                close: Fee {
                    rate: 0.0,
                    per_lot: 2.0,
                },
                close_today: Fee {
                    rate: 0.0,
                    per_lot: 3.0,
                },
            }),
            fee_source: "fixture".into(),
            windows: vec![Window {
                begin: 0,
                end: 86399,
            }],
        };
        Metadata {
            products: BTreeMap::from([("RB".into(), s)]),
            contracts: BTreeMap::new(),
            historical_exact: false,
            reference_as_of: "2026-11-17".into(),
            provenance: serde_json::Value::Null,
        }
    }
    fn config() -> Config {
        Config {
            products: vec!["RB".into()],
            input_mode: "notional_amount".into(),
            reprice_ms: 10000,
            max_child_lots: 10,
            order_delay_ms: 0,
            report_delay_ms: 0,
            cancel_delay_ms: 0,
            ..Default::default()
        }
    }
    fn engine(cfg: Config) -> Engine<MemorySink> {
        let mut e = Engine::new(cfg, metadata(), MemorySink::default()).unwrap();
        e.begin_day(20261113, BTreeMap::from([("RB".into(), "rb2701".into())]))
            .unwrap();
        e
    }
    fn quote(ms: i64, seq: u64, c: &str, bid: i64, ask: i64, last: i64, volume: u64) -> Quote {
        let ts = start() + ms * 1000;
        Quote {
            ts,
            exchange_ts: ts,
            cursor: seq,
            day: 20261113,
            contract: c.into(),
            bid: [Some(bid), None, None, None, None],
            ask: [Some(ask), None, None, None, None],
            bid_qty: [100, 0, 0, 0, 0],
            ask_qty: [100, 0, 0, 0, 0],
            last: Some(last),
            volume: Some(volume),
            end: Some(start() + 60_000_000),
        }
    }
    fn input(e: &mut Engine<MemorySink>, ms: i64, amount: f64) {
        e.input(InputRow {
            ts: start() + ms * 1000,
            values: BTreeMap::from([("RB".into(), amount)]),
        })
        .unwrap();
    }
    fn feed(e: &mut Engine<MemorySink>, ms: i64, seq: u64, last: i64, vol: u64) {
        e.quote(quote(ms, seq, "rb2701", 99, 101, last, vol))
            .unwrap();
        e.advance(start() + ms * 1000, true).unwrap();
    }
    #[test]
    fn positive_and_negative_amounts_truncate_toward_zero() {
        for (amount, want) in [(2999.0, 2), (-2999.0, -2), (0.0, 0)] {
            let mut e = engine(Config {
                task: Task::Convert,
                ..config()
            });
            input(&mut e, 0, amount);
            feed(&mut e, 0, 1, 100, 100);
            assert_eq!(e.targets["rb2701"].lots, want);
        }
    }
    #[test]
    fn default_capital_is_five_million_and_raw_values_are_not_normalized() {
        assert_eq!(Config::default().amount(0.02), 100_000.0);
        assert_eq!(Config::default().amount(-0.02), -100_000.0);
    }

    #[test]
    fn changing_to_nearer_contract_cancels_unfilled_old_target() {
        let mut e = engine(config());
        input(&mut e, 0, 1000.0);
        feed(&mut e, 0, 1, 100, 100);
        assert!(e.working("rb2701"));
        e.begin_day(20261116, BTreeMap::from([("RB".into(), "rb2612".into())]))
            .unwrap();
        assert_eq!(e.targets["rb2701"].lots, 0);
        assert!(e
            .orders
            .values()
            .filter(|o| o.contract == "rb2701")
            .all(|o| o.cancel_sent));
        e.advance(start() + 1, true).unwrap();
        e.quote(quote(100, 2, "rb2612", 99, 101, 100, 100)).unwrap();
        e.advance(start() + 100_000, true).unwrap();
        assert_eq!(e.targets["rb2612"].lots, 1);
        assert!(!e.working("rb2701"));
        assert!(e.working("rb2612"));
    }

    #[test]
    fn changing_to_nearer_contract_rolls_existing_far_position() {
        let mut e = engine(Config {
            execution: Execution::Taker,
            ..config()
        });
        input(&mut e, 0, 1000.0);
        feed(&mut e, 0, 1, 100, 100);
        assert_eq!(e.known["rb2701"], 1);
        e.begin_day(20261116, BTreeMap::from([("RB".into(), "rb2612".into())]))
            .unwrap();
        assert_eq!(e.rolls["RB"].old, "rb2701");
        assert_eq!(e.rolls["RB"].new, "rb2612");
        e.quote(quote(100, 2, "rb2612", 99, 101, 100, 100)).unwrap();
        e.advance(start() + 100_000, true).unwrap();
        let summary = e.finish(start() + 200_000).unwrap();
        assert_eq!(summary.positions["rb2701"], 0);
        assert_eq!(summary.positions["rb2612"], 1);
        assert!(summary.roll_complete);
    }
    #[test]
    fn maker_touch_is_not_a_fill_but_strict_trade_through_fills_at_limit() {
        let mut e = engine(config());
        input(&mut e, 0, 3000.0);
        feed(&mut e, 0, 1, 100, 100);
        assert_eq!(e.summary.fills, 0);
        feed(&mut e, 100, 2, 99, 101);
        assert_eq!(e.summary.fills, 0);
        feed(&mut e, 200, 3, 98, 102);
        let s = e.finish(start() + 300000).unwrap();
        assert_eq!(s.filled_lots, 3);
        assert_eq!(s.positions["rb2701"], 3);
        let f = e.sink.0.iter().find(|(t, _)| t == "fills").unwrap();
        assert_eq!(f.1.price, Some(99.0));
    }
    #[test]
    fn quote_without_new_volume_cannot_fill_maker() {
        let mut e = engine(config());
        input(&mut e, 0, 1000.0);
        feed(&mut e, 0, 1, 100, 100);
        feed(&mut e, 100, 2, 98, 100);
        assert_eq!(e.finish(start() + 200000).unwrap().fills, 0);
    }
    #[test]
    fn source_frame_that_creates_order_cannot_fill_it() {
        let mut e = engine(config());
        input(&mut e, 0, 1000.0);
        feed(&mut e, 0, 1, 98, 100);
        assert_eq!(e.finish(start() + 1000).unwrap().fills, 0);
    }
    #[test]
    fn taker_respects_depth_and_price_protection() {
        let mut e = engine(Config {
            execution: Execution::Taker,
            protection_ticks: 1,
            report_delay_ms: 1000,
            ..config()
        });
        input(&mut e, 0, 5000.0);
        let mut q = quote(0, 1, "rb2701", 99, 101, 100, 100);
        q.ask = [Some(101), Some(102), Some(103), None, None];
        q.ask_qty = [1, 2, 100, 0, 0];
        e.quote(q).unwrap();
        e.advance(start(), true).unwrap();
        let s = e.finish(start() + 500000).unwrap();
        assert_eq!(s.filled_lots, 3);
        assert_eq!(s.unfilled_lots, 2);
    }
    #[test]
    fn delayed_market_data_changes_fill_and_does_not_expose_future_book() {
        let run = |delay| {
            let mut e = engine(Config {
                execution: Execution::Taker,
                md_delay_ms: delay,
                order_delay_ms: 50,
                report_delay_ms: 10,
                ..config()
            });
            input(&mut e, 0, 1000.0);
            feed(&mut e, 0, 1, 100, 100);
            e.quote(quote(100, 2, "rb2701", 199, 201, 200, 101))
                .unwrap();
            e.finish(start() + 190000).unwrap()
        };
        assert_eq!(run(0).filled_lots, 1);
        assert_eq!(run(100).filled_lots, 0);
    }
    #[test]
    fn post_only_rejects_order_crossed_during_transit() {
        let mut e = engine(Config {
            order_delay_ms: 200,
            ..config()
        });
        input(&mut e, 0, 1000.0);
        feed(&mut e, 0, 1, 100, 100);
        e.quote(quote(100, 2, "rb2701", 96, 98, 97, 101)).unwrap();
        let s = e.finish(start() + 210000).unwrap();
        assert_eq!(s.fills, 0);
        assert!(s.rejected_orders > 0);
    }
    #[test]
    fn cancel_in_transit_can_still_fill() {
        let mut e = engine(Config {
            cancel_delay_ms: 200,
            ..config()
        });
        input(&mut e, 0, 1000.0);
        feed(&mut e, 0, 1, 100, 100);
        input(&mut e, 100, 0.0);
        feed(&mut e, 150, 2, 98, 101);
        let s = e.finish(start() + 400000).unwrap();
        assert_eq!(s.positions["rb2701"], 1);
        assert_eq!(s.unfilled_lots, 1);
    }
    #[test]
    fn delayed_fill_report_reserves_quantity_and_prevents_duplicate_orders() {
        let mut e = engine(Config {
            report_delay_ms: 1000,
            ..config()
        });
        input(&mut e, 0, 1000.0);
        feed(&mut e, 0, 1, 100, 100);
        feed(&mut e, 100, 2, 98, 101);
        feed(&mut e, 200, 3, 98, 102);
        assert_eq!(
            e.sink
                .0
                .iter()
                .filter(|(t, r)| t == "orders" && r.kind == "submitted")
                .count(),
            1
        );
    }
    #[test]
    fn no_trades_after_run_cutoff() {
        let mut e = engine(Config {
            execution: Execution::Taker,
            order_delay_ms: 1000,
            ..config()
        });
        input(&mut e, 0, 1000.0);
        feed(&mut e, 0, 1, 100, 100);
        assert_eq!(e.finish(start() + 500000).unwrap().fills, 0);
    }
    #[test]
    fn no_trade_on_session_end_snapshot() {
        let mut e = engine(config());
        input(&mut e, 0, 1000.0);
        feed(&mut e, 0, 1, 100, 100);
        let mut q = quote(100, 2, "rb2701", 99, 101, 98, 101);
        q.end = None;
        e.quote(q).unwrap();
        assert_eq!(e.finish(start() + 200000).unwrap().fills, 0);
    }
    #[test]
    fn yesterday_and_today_fees_and_futures_pnl_are_accounted_without_buying_notional() {
        let s = metadata().spec("rb2701").unwrap();
        let mut p = Position::default();
        p.apply(2, 100.0, 20261112, &s);
        let (pnl, fee) = p.apply(-1, 110.0, 20261113, &s);
        assert_eq!(cny(pnl), 100.0);
        assert_eq!(fee.map(cny), Some(2.0));
        p.apply(2, 120.0, 20261113, &s);
        let (pnl, fee) = p.apply(-3, 130.0, 20261113, &s);
        assert_eq!(cny(pnl), 500.0);
        assert_eq!(fee.map(cny), Some(8.0));
        assert_eq!(p.lots(), 0);
    }
    #[test]
    fn short_close_profit_has_correct_sign() {
        let s = metadata().spec("rb2701").unwrap();
        let mut p = Position::default();
        p.apply(-2, 100.0, 20261112, &s);
        assert_eq!(cny(p.apply(2, 90.0, 20261113, &s).0), 200.0);
    }
    #[test]
    fn missing_fees_remain_unknown() {
        let mut e = engine(Config {
            execution: Execution::Taker,
            ..config()
        });
        e.metadata.products.get_mut("RB").unwrap().fees = None;
        input(&mut e, 0, 1000.0);
        feed(&mut e, 0, 1, 100, 100);
        let s = e.finish(start() + 100000).unwrap();
        assert_eq!(s.fee_unknown_fills, 1);
        assert_eq!(s.net_pnl_cny, None);
    }
    fn roll_engine(short: bool, budget: f64, report: u64) -> Engine<MemorySink> {
        let mut cfg = Config {
            task: Task::RollCost,
            max_child_lots: 1,
            report_delay_ms: report,
            ..config()
        };
        cfg.roll.old_contract = "rb2701".into();
        cfg.roll.new_contract = "rb2705".into();
        cfg.roll.initial_lots = if short { -2 } else { 2 };
        cfg.roll.target_lots = Some(if short { -2 } else { 2 });
        cfg.roll.max_exposure_deviation_cny = budget;
        engine(cfg)
    }
    fn roll_start(e: &mut Engine<MemorySink>) {
        e.quote(quote(0, 1, "rb2701", 99, 101, 100, 100)).unwrap();
        e.quote(quote(0, 2, "rb2705", 101, 103, 102, 100)).unwrap();
        e.advance(start(), true).unwrap();
    }
    #[test]
    fn maker_roll_legs_fill_independently_and_track_actual_exposure() {
        let mut e = roll_engine(false, 1100.0, 0);
        roll_start(&mut e);
        assert_eq!(e.orders.len(), 2);
        e.quote(quote(100, 3, "rb2701", 99, 101, 102, 101)).unwrap();
        e.advance(start() + 100000, true).unwrap();
        assert_eq!(e.positions["rb2701"].lots(), 1);
        assert!(!e.positions.contains_key("rb2705"));
        e.quote(quote(200, 4, "rb2705", 101, 103, 100, 101))
            .unwrap();
        let s = e.finish(start() + 210000).unwrap();
        assert_eq!(s.positions["rb2705"], 1);
        assert!(s.max_exposure_deviation_cny >= 1000.0);
        assert!(!s.roll_complete);
    }
    #[test]
    fn insufficient_exposure_budget_does_not_fabricate_atomic_pair_fill() {
        let mut e = roll_engine(false, 100.0, 0);
        roll_start(&mut e);
        assert!(e.orders.is_empty());
        let s = e.finish(start() + 100000).unwrap();
        assert_eq!(s.fills, 0);
        assert!(!s.roll_complete);
    }
    #[test]
    fn short_roll_orders_have_reverse_sides() {
        let mut e = roll_engine(true, 1100.0, 0);
        roll_start(&mut e);
        assert!(e
            .orders
            .values()
            .any(|o| o.contract == "rb2701" && o.side == 1));
        assert!(e
            .orders
            .values()
            .any(|o| o.contract == "rb2705" && o.side == -1));
    }
    #[test]
    fn outstanding_roll_reports_block_the_next_batch() {
        let mut e = roll_engine(false, 1100.0, 1000);
        roll_start(&mut e);
        e.quote(quote(100, 3, "rb2701", 99, 101, 102, 101)).unwrap();
        e.quote(quote(200, 4, "rb2705", 101, 103, 100, 101))
            .unwrap();
        e.advance(start() + 300000, true).unwrap();
        assert_eq!(
            e.sink
                .0
                .iter()
                .filter(|(t, r)| t == "orders" && r.kind == "submitted")
                .count(),
            2
        );
    }
    #[test]
    fn positive_roll_threshold_can_wait_and_report_unfinished() {
        let mut e = roll_engine(false, 1100.0, 0);
        e.cfg.roll.spread_improvement_ticks = 3.0;
        roll_start(&mut e);
        assert!(e.orders.is_empty());
        assert!(!e.finish(start() + 100000).unwrap().roll_complete);
    }
}
