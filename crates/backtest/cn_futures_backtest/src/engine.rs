//! Virtual-time matching, delayed private reports, and bounded calendar-roll execution.
mod maker_taker;

use crate::{
    config::{Config, Execution, MakerFillModel, RollAlgorithm, Task},
    input::InputRow,
    metadata::{cny, money, Metadata, Spec},
    output::{Row, Sink},
};
use anyhow::{ensure, Result};
use serde::{Deserialize, Serialize};
use std::{
    cmp::Ordering,
    collections::{BTreeMap, BinaryHeap, VecDeque},
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
    fn limit(&self, side: i64, mode: Execution, cfg: &Config) -> Option<i64> {
        let (base, offset) = match (mode, side > 0) {
            (Execution::Maker, true) => (self.bid[0]?, -cfg.maker_offset_ticks),
            (Execution::Maker, false) => (self.ask[0]?, cfg.maker_offset_ticks),
            (Execution::Taker, true) => (self.ask[0]?, cfg.protection_ticks),
            (Execution::Taker, false) => (self.bid[0]?, -cfg.protection_ticks),
        };
        base.checked_add(offset)
    }

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
    live_since: Option<i64>,
    cancel_sent: bool,
    reference: f64,
    reason: String,
    hedge_basis: Option<maker_taker::HedgeBasis>,
}
#[derive(Clone, Debug)]
enum Report {
    Fill {
        id: u64,
        contract: String,
        delta: i64,
        price: f64,
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
    arrival: Option<(f64, f64)>,
    maker_taker: maker_taker::State,
    batch: Option<Batch>,
    seeded: bool,
    standalone: bool,
    begin: i64,
    deadline: i64,
    completed: bool,
    succeeded: bool,
    taker_old: VecDeque<(i64, f64, u64)>,
    taker_new: VecDeque<(i64, f64, u64)>,
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
    #[serde(default)]
    pub roll_paired_lots: u64,
    #[serde(default)]
    pub roll_lead_taker_lots: u64,
    #[serde(default)]
    pub roll_spread_cost_cny: Option<f64>,
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
    // One child per contract, reserved until its private terminal report.
    order_by_contract: BTreeMap<String, u64>,
    // An immediate IOC remainder or rejection cannot retry the identical action
    // indefinitely while both virtual time and the visible book are unchanged.
    failed_immediate: BTreeMap<String, (i64, u64, i64, Execution)>,
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
        let model = format!(
            "{};{}",
            cfg.maker_fill_model.model(),
            cfg.roll_algorithm().model()
        );
        let mut e = Self {
            peak_equity: cfg.capital_cny,
            cfg,
            metadata,
            sink,
            now: 0,
            summary: Summary {
                model,
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
            order_by_contract: BTreeMap::new(),
            failed_immediate: BTreeMap::new(),
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
                    arrival: None,
                    maker_taker: Default::default(),
                    batch: None,
                    seeded: false,
                    standalone: true,
                    begin,
                    deadline,
                    completed: false,
                    succeeded: false,
                    taker_old: VecDeque::new(),
                    taker_new: VecDeque::new(),
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
                            arrival: None,
                            maker_taker: Default::default(),
                            batch: None,
                            seeded: true,
                            standalone: false,
                            begin: 0,
                            deadline: i64::MAX,
                            completed: false,
                            succeeded: false,
                            taker_old: VecDeque::new(),
                            taker_new: VecDeque::new(),
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
        let s = self.metadata.spec_ref(&q.contract)?;
        let maker_fill = self.trade_through(&q, s);
        if let Some(r) = self.rolls.get_mut(&s.product) {
            if r.standalone && !r.seeded && r.old == q.contract && q.ts >= r.begin {
                if let Some(mid) = q.mid(s) {
                    let mut p = Position::default();
                    p.apply(r.initial, mid, q.day.saturating_sub(1), s);
                    self.positions.insert(q.contract.clone(), p.clone());
                    self.theory.insert(q.contract.clone(), p);
                    self.known.insert(q.contract.clone(), r.initial);
                    r.seeded = true;
                }
            }
        }
        let pending_product = self
            .pending
            .contains_key(&s.product)
            .then(|| s.product.clone());
        self.markets.insert(q.contract.clone(), q.clone());
        if let Some(end) = q.end.filter(|end| *end > q.ts) {
            if self.segment_ends.get(&q.contract) != Some(&end) {
                self.segment_ends.insert(q.contract.clone(), end);
                self.push(end, 0, Event::EndSession(q.contract.clone(), end))?;
            }
        }
        if let Some((id, n)) = maker_fill {
            self.fill(id, n, q.cursor)?;
        }
        if let Some(product) = pending_product {
            self.resolve(&product)?;
        }
        self.push(
            q.ts + self.cfg.md_delay_ms as i64 * 1000,
            3,
            Event::Visible(Box::new(q)),
        )?;
        self.trace(false)?;
        Ok(())
    }
    fn trade_through(&self, q: &Quote, spec: &Spec) -> Option<(u64, i64)> {
        let o = self.contract_order(&q.contract)?;
        let live_since = o.live_since?;
        let last = q.last?;
        let crossed = if o.side > 0 {
            last < o.limit
        } else {
            last > o.limit
        };
        if q.cursor <= o.cursor || !crossed {
            return None;
        }
        let prev = self.markets.get(&q.contract)?;
        let exchange_end = spec.segment_end(q.exchange_ts)?;
        if q.ts >= self.cutoff
            || q.ts >= q.end?
            || prev.day != q.day
            || prev.end != q.end
            || q.ts - prev.ts > self.cfg.quote_age_ms as i64 * 1000
        {
            return None;
        }
        let delta = q.volume?.checked_sub(prev.volume?)?;
        if delta == 0 {
            return None;
        }
        let lots = match self.cfg.maker_fill_model {
            MakerFillModel::FullOnThrough => o.remaining,
            MakerFillModel::IntervalVolume => {
                // The last print may come from anywhere inside the capture interval.
                if live_since > prev.ts
                    || q.ts == prev.ts
                    || q.exchange_ts < prev.exchange_ts
                    || spec.segment_end(prev.exchange_ts) != Some(exchange_end)
                    || spec.segment_end(q.ts) != Some(exchange_end)
                {
                    return None;
                }
                delta.min(o.remaining as u64) as i64
            }
        };
        Some((o.id, lots))
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
                            price,
                        } => {
                            self.confirm_roll_fill(id, &contract, delta, price);
                            *self.known.entry(contract).or_default() += delta;
                            if let Some(o) = self.orders.get_mut(&id) {
                                o.local_remaining -= delta.abs();
                            }
                        }
                        Report::Done { id, reason } => {
                            if let Some(o) = self.remove_order(id) {
                                if self.now == o.created
                                    && matches!(
                                        reason.as_str(),
                                        "ioc_cancelled" | "reject_stale_book"
                                    )
                                {
                                    self.failed_immediate.insert(
                                        o.contract.clone(),
                                        (self.now, o.cursor, o.side, o.execution),
                                    );
                                }
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
                    if self.segment_ends.get(&c) == Some(&end) {
                        if let Some(o) = self.contract_order(&c).filter(|o| o.live_since.is_some())
                        {
                            self.done(o.id, "session_expired")?;
                        }
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
        if delta == 0 || self.disabled || self.now >= self.cutoff || self.working(c) {
            return Ok(None);
        }
        let Some(q) = self.fresh(c, &self.visible) else {
            return Ok(None);
        };
        let (side, n) = (delta.signum(), delta.abs().min(self.cfg.max_child_lots));
        let cursor = q.cursor;
        if self.failed_immediate.get(c) == Some(&(self.now, cursor, side, mode)) {
            return Ok(None);
        }
        let Some(limit) = q.limit(side, mode, &self.cfg) else {
            return Ok(None);
        };
        ensure!(limit > 0, "non-positive limit");
        let s = self.metadata.spec_ref(c)?;
        self.next_id += 1;
        let id = self.next_id;
        self.order_by_contract.insert(c.into(), id);
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
                cursor,
                live_since: None,
                cancel_sent: false,
                reference,
                reason: reason.into(),
                hedge_basis: None,
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
        if let Some(id) = self
            .contract_order(c)
            .filter(|o| !o.cancel_sent)
            .map(|o| o.id)
        {
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
        o.live_since = None;
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
        let Some(cursor) = self.fresh(&o.contract, &self.markets).map(|q| q.cursor) else {
            return self.done(id, "reject_stale_book");
        };
        // Both order intents are ordinary limit orders on arrival. The IOC
        // cancels its remainder; a resting limit keeps it at the original price.
        for level in 0..5 {
            let n = self.orders[&id].remaining;
            if n == 0 {
                return Ok(());
            }
            let book = self.markets.get_mut(&o.contract).unwrap();
            let (p, qty) = if o.side > 0 {
                (book.ask[level], &mut book.ask_qty[level])
            } else {
                (book.bid[level], &mut book.bid_qty[level])
            };
            let Some(p) = p else { continue };
            if o.side * (p - o.limit) > 0 {
                break;
            }
            let take = n.min(*qty);
            *qty -= take;
            if take > 0 {
                self.fill_at(id, take, p, cursor, Execution::Taker)?;
            }
        }
        if self.orders[&id].remaining == 0 {
            return Ok(());
        }
        if o.execution == Execution::Taker {
            return self.done(id, "ioc_cancelled");
        }
        let x = self.orders.get_mut(&id).unwrap();
        x.live_since = Some(self.now);
        x.cursor = x.cursor.max(cursor);
        let remaining = x.remaining;
        self.emit(
            "orders",
            Row {
                kind: "live".into(),
                contract: o.contract,
                lots: Some(remaining),
                order_id: Some(id),
                source_cursor: Some(cursor),
                ..Default::default()
            },
        )?;
        Ok(())
    }
    fn fill(&mut self, id: u64, n: i64, cursor: u64) -> Result<()> {
        let p = self.orders[&id].limit;
        self.fill_at(id, n, p, cursor, Execution::Maker)
    }
    fn record_taker_roll_fill(
        &mut self,
        order: &Order,
        price: f64,
        lots: i64,
        spec: &Spec,
    ) -> Result<()> {
        if self.cfg.roll.algorithm != Some(RollAlgorithm::TakerTaker)
            || order.reason != "calendar_roll"
        {
            return Ok(());
        }
        let Some(roll) = self.rolls.get_mut(&spec.product) else {
            return Ok(());
        };
        let sign = roll.initial.signum();
        let queue = if order.contract == roll.old && order.side == -sign {
            &mut roll.taker_old
        } else if order.contract == roll.new && order.side == sign {
            &mut roll.taker_new
        } else {
            return Ok(());
        };
        queue.push_back((lots, price, order.id));
        let Some((old_ref, new_ref)) = roll.arrival else {
            return Ok(());
        };
        let mut pairs = Vec::new();
        while let (Some(old), Some(new)) = (roll.taker_old.front_mut(), roll.taker_new.front_mut())
        {
            let matched = old.0.min(new.0);
            pairs.push((matched, old.1, new.1, old.2, new.2));
            old.0 -= matched;
            new.0 -= matched;
            if old.0 == 0 {
                roll.taker_old.pop_front();
            }
            if new.0 == 0 {
                roll.taker_new.pop_front();
            }
        }
        for (matched, old_price, new_price, old_id, new_id) in pairs {
            let spread = new_price - old_price;
            let reference = new_ref - old_ref;
            let cost = cny(money(
                (spread - reference) * sign as f64 * matched as f64 * spec.multiplier,
            ));
            self.summary.roll_paired_lots += matched as u64;
            *self.summary.roll_spread_cost_cny.get_or_insert(0.0) += cost;
            self.emit(
                "roll_batches",
                Row {
                    kind: "paired_taker_fill".into(),
                    product: spec.product.clone(),
                    contract: self.rolls[&spec.product].new.clone(),
                    order_id: Some(new_id),
                    lots: Some(matched),
                    price: Some(new_price),
                    value: Some(spread),
                    reference_price: Some(reference),
                    shortfall_cny: Some(cost),
                    reason: format!("old_order_id={old_id};old_price={old_price}"),
                    ..Default::default()
                },
            )?;
        }
        Ok(())
    }
    fn fill_at(
        &mut self,
        id: u64,
        n: i64,
        price_ticks: i64,
        cursor: u64,
        liquidity: Execution,
    ) -> Result<()> {
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
        self.record_roll_fill(&o, price, n, liquidity, &s)?;
        self.record_taker_roll_fill(&o, price, n, &s)?;
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
            x.live_since = None;
        }
        self.emit(
            "fills",
            Row {
                kind: format!("{liquidity:?}").to_ascii_lowercase(),
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
                price,
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
        self.order_by_contract.contains_key(c)
    }
    fn contract_order(&self, c: &str) -> Option<&Order> {
        self.order_by_contract.get(c).map(|id| &self.orders[id])
    }
    fn remove_order(&mut self, id: u64) -> Option<Order> {
        let order = self.orders.remove(&id)?;
        self.order_by_contract.remove(&order.contract);
        Some(order)
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
            .contract_order(c)
            .is_some_and(|o| !o.cancel_sent && self.now - o.created >= ms as i64 * 1000)
        {
            self.cancel_contract(c)?;
        }
        Ok(())
    }
    fn risk_interval(&self, r: &RollState) -> Option<(f64, f64)> {
        let notional = |c: &str| {
            if self.known.get(c).copied().unwrap_or(0) == 0 && !self.working(c) {
                return Some(0.0);
            }
            let s = self.metadata.spec(c).ok()?;
            Some(self.fresh(c, &self.visible)?.mid(&s)? * s.multiplier)
        };
        Some(self.marked_risk_interval(r, notional(&r.old)?, notional(&r.new)?))
    }
    fn marked_risk_interval(&self, r: &RollState, old_unit: f64, new_unit: f64) -> (f64, f64) {
        let mut e = 0.0;
        let mut lo = 0.0;
        let mut hi = 0.0;
        for (c, unit) in [(&r.old, old_unit), (&r.new, new_unit)] {
            e += *self.known.get(c).unwrap_or(&0) as f64 * unit;
            if let Some(o) = self.contract_order(c) {
                let d = o.side as f64 * o.local_remaining as f64 * unit;
                lo += d.min(0.0);
                hi += d.max(0.0);
            }
        }
        (e + lo, e + hi)
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
    fn roll_books(&self, r: &RollState) -> Option<(&Quote, &Quote)> {
        let old = self.fresh(&r.old, &self.visible)?;
        let new = self.fresh(&r.new, &self.visible)?;
        (old.day == new.day
            && old.ts.abs_diff(new.ts) <= self.cfg.roll.max_quote_skew_ms.saturating_mul(1000))
        .then_some((old, new))
    }
    fn roll_control(&mut self, p: &str, r: &mut RollState) -> Result<()> {
        if !r.seeded || self.now < r.begin || self.now >= r.deadline || r.completed {
            return Ok(());
        }
        let books = self.roll_books(r).map(|(a, b)| (a.clone(), b.clone()));
        let (sa, sb) = (self.metadata.spec(&r.old)?, self.metadata.spec(&r.new)?);
        let mids = books
            .as_ref()
            .and_then(|(a, b)| Some((a.mid(&sa)?, b.mid(&sb)?)));
        if r.arrival.is_none() {
            let Some((pa, pb)) = mids else { return Ok(()) };
            ensure!(
                (sa.multiplier - sb.multiplier).abs() < 1e-8,
                "roll multiplier mismatch"
            );
            r.arrival = Some((pa, pb));
            if r.standalone {
                if self.cfg.roll.target_cny.is_none() {
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
                    value: Some(pb - pa),
                    reason: r.new.clone(),
                    ..Default::default()
                },
            )?;
        }
        let ka = *self.known.get(&r.old).unwrap_or(&0);
        let kb = *self.known.get(&r.new).unwrap_or(&0);
        if ka == 0 && kb == r.goal && !self.working(&r.old) && !self.working(&r.new) {
            r.completed = true;
            r.succeeded = true;
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
        let mode = match self.cfg.roll_algorithm() {
            RollAlgorithm::MakerTaker => return self.maker_taker_control(r, books.as_ref()),
            RollAlgorithm::MakerMaker => Execution::Maker,
            RollAlgorithm::TakerTaker => Execution::Taker,
        };
        let (arrival_old, arrival_new) = r.arrival.unwrap();
        let reference_spread = arrival_new - arrival_old;
        if r.goal != 0 && r.goal.signum() != r.initial.signum() {
            self.cancel_contract(&r.old)?;
            self.cancel_contract(&r.new)?;
            r.completed = true;
            return Ok(());
        }
        let (Some((a, b)), Some((pa, pb))) = (books, mids) else {
            // When only one leg remains, a fresh book on that leg suffices.
            // Keep the other leg's in-flight orders reserved until terminal.
            let (target_old, target_new) = r.batch.as_ref().map_or((0, r.goal), |b| (b.a, b.b));
            let (contract, delta, reference) =
                if ka != target_old && kb == target_new && !self.working(&r.new) {
                    (&r.old, target_old - ka, arrival_old)
                } else if ka == target_old && kb != target_new && !self.working(&r.old) {
                    (&r.new, target_new - kb, arrival_new)
                } else {
                    return Ok(());
                };
            if self.working(contract) {
                self.reprice(contract, self.cfg.reprice_ms)?;
            } else {
                let qty = delta.signum()
                    * delta
                        .abs()
                        .min(self.cfg.max_child_lots)
                        .min(self.cfg.roll.batch_lots);
                if self.risk_allows(r, contract, qty) {
                    self.submit(contract, qty, mode, reference, "calendar_roll")?;
                }
            }
            return Ok(());
        };
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
            let paired = (next_a - ka) * sign < 0 && (next_b - kb) * sign > 0;
            let explicit_taker = self.cfg.roll.algorithm == Some(RollAlgorithm::TakerTaker);
            let (old_price, new_price, old_qty, new_qty) =
                if mode == Execution::Taker && explicit_taker {
                    if sign > 0 {
                        (a.bid[0], b.ask[0], a.bid_qty[0], b.ask_qty[0])
                    } else {
                        (a.ask[0], b.bid[0], a.ask_qty[0], b.bid_qty[0])
                    }
                } else if sign > 0 {
                    (a.ask[0], b.bid[0], a.ask_qty[0], b.bid_qty[0])
                } else {
                    (a.bid[0], b.ask[0], a.bid_qty[0], b.ask_qty[0])
                };
            let quote_spread = sb.price(new_price.unwrap()) - sa.price(old_price.unwrap());
            let gate = mode == Execution::Maker || (explicit_taker && paired);
            let improvement = self.cfg.roll.spread_improvement_ticks * sa.tick;
            if gate
                && self.cfg.roll.use_spread_gate
                && (quote_spread - reference_spread) * sign as f64 > -improvement + 1e-9
            {
                return Ok(());
            }
            if explicit_taker && paired && (old_qty <= 0 || new_qty <= 0) {
                return Ok(());
            }
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
                    reference_price: Some(reference_spread),
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
                self.submit(&c, d, mode, reference, "calendar_roll")?;
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
            if r.arrival.is_none() || r.completed {
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
                        ..
                    } => {
                        *self.known.entry(contract).or_default() += delta;
                        if let Some(o) = self.orders.get_mut(&id) {
                            o.local_remaining -= delta.abs();
                        }
                    }
                    Report::Done { id, .. } => {
                        self.remove_order(id);
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
            r.succeeded
                || (self.positions.get(&r.old).map_or(0, Position::lots) == 0
                    && self.positions.get(&r.new).map_or(0, Position::lots) == r.goal
                    && r.arrival.is_some()
                    && !r.completed)
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
    fn interval_volume_requires_a_full_live_interval_and_caps_partial_fills() {
        for side in [-1, 1] {
            let mut e = engine(Config {
                maker_fill_model: MakerFillModel::IntervalVolume,
                order_delay_ms: 10,
                report_delay_ms: 50,
                ..config()
            });
            let through = 100 - side * 2;
            input(&mut e, 0, 4000.0 * side as f64);
            feed(&mut e, 0, 1, 100, 100);
            feed(&mut e, 50, 2, through, 200); // Arrived 10 ms into this interval.
            feed(&mut e, 50, 3, through, 201); // Another frame at the same capture time.
            feed(&mut e, 70, 4, 100 - side, 205); // Touch only.
            assert_eq!(e.summary.filled_lots, 0);
            feed(&mut e, 80, 5, through, 207);
            assert_eq!(e.contract_order("rb2701").unwrap().remaining, 2);
            assert_eq!(e.known.get("rb2701"), None);
            feed(&mut e, 90, 6, through, 6); // Cumulative counter reset.
            feed(&mut e, 100, 7, through, 7);
            feed(&mut e, 110, 8, through, 40);
            assert_eq!(e.summary.filled_lots, 4);
            assert!(e.working("rb2701")); // Private terminal report still in flight.
            e.advance(start() + 170_000, true).unwrap();
            assert_eq!(e.known["rb2701"], side * 4);
            assert!(!e.working("rb2701"));
            let fills: Vec<_> = e
                .sink
                .0
                .iter()
                .filter(|(t, _)| t == "fills")
                .map(|(_, r)| (r.source_cursor.unwrap(), r.lots.unwrap()))
                .collect();
            assert_eq!(fills, [(5, 2), (7, 1), (8, 1)]);
        }
    }
    #[test]
    fn zero_latency_ioc_waits_for_new_liquidity_after_partial_fill() {
        let mut e = engine(Config {
            execution: Execution::Taker,
            ..config()
        });
        input(&mut e, 0, 3000.0);
        let mut q = quote(0, 1, "rb2701", 99, 101, 100, 10);
        q.ask_qty[0] = 1;
        e.quote(q).unwrap();
        e.advance(start(), true).unwrap();
        assert_eq!(e.next_id, 1);
        assert_eq!(e.known["rb2701"], 1);
        feed(&mut e, 100, 2, 100, 10);
        assert_eq!(e.known["rb2701"], 3);
        assert_eq!(e.next_id, 2);
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
    fn marketable_limit_takes_depth_then_rests_at_its_original_limit() {
        let mut e = engine(Config {
            order_delay_ms: 200,
            ..config()
        });
        input(&mut e, 0, 4000.0);
        feed(&mut e, 0, 1, 100, 100);
        let mut q = quote(100, 2, "rb2701", 96, 98, 97, 101);
        q.ask = [Some(98), Some(99), Some(100), None, None];
        q.ask_qty = [1, 1, 100, 0, 0];
        e.quote(q).unwrap();
        e.advance(start() + 200_000, true).unwrap();
        let o = e.orders.values().next().unwrap();
        assert!(o.live_since.is_some());
        assert_eq!((o.limit, o.remaining), (99, 2));
        feed(&mut e, 250, 3, 99, 102);
        feed(&mut e, 300, 4, 98, 103);
        let s = e.finish(start() + 400_000).unwrap();
        assert_eq!((s.filled_lots, s.rejected_orders), (4, 0));
        let fills: Vec<_> = e
            .sink
            .0
            .iter()
            .filter(|(t, _)| t == "fills")
            .map(|(_, r)| (r.kind.as_str(), r.price.unwrap(), r.lots.unwrap()))
            .collect();
        assert_eq!(
            fills,
            [("taker", 98.0, 1), ("taker", 99.0, 1), ("maker", 99.0, 2)]
        );
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

    #[test]
    fn explicit_taker_roll_uses_directional_executable_spread_gate() {
        for short in [false, true] {
            let mut cfg = roll_engine(short, 20_000.0, 0).cfg;
            cfg.roll.algorithm = Some(RollAlgorithm::TakerTaker);
            cfg.execution = Execution::Taker;
            cfg.roll.use_spread_gate = true;
            cfg.roll.spread_improvement_ticks = 1.0;
            let mut e = engine(cfg);
            roll_start(&mut e);
            assert_eq!(e.summary.fills, 0);
            assert!(!e.working("rb2701"));
            let (bid, ask) = if short { (104, 106) } else { (98, 100) };
            e.quote(quote(100, 3, "rb2705", bid, ask, 102, 100))
                .unwrap();
            e.advance(start() + 100_000, true).unwrap();
            let batch = e
                .sink
                .0
                .iter()
                .find(|(table, row)| table == "roll_batches" && row.kind == "batch_started")
                .unwrap();
            assert_eq!(batch.1.reference_price, Some(2.0));
            assert_eq!(batch.1.value, Some(if short { 3.0 } else { 1.0 }));
            assert!(e.summary.filled_lots > 0);
        }
    }

    #[test]
    fn taker_gate_needs_both_depths_and_skips_unpaired_liquidation() {
        let mut cfg = roll_engine(false, 20_000.0, 0).cfg;
        cfg.roll.algorithm = Some(RollAlgorithm::TakerTaker);
        cfg.execution = Execution::Taker;
        let mut e = engine(cfg.clone());
        e.quote(quote(0, 1, "rb2701", 99, 101, 100, 100)).unwrap();
        let mut next = quote(0, 2, "rb2705", 101, 103, 102, 100);
        next.ask_qty[0] = 0;
        e.quote(next).unwrap();
        e.advance(start(), true).unwrap();
        let mut favorable = quote(100, 3, "rb2705", 99, 101, 100, 100);
        favorable.ask_qty[0] = 0;
        e.quote(favorable).unwrap();
        e.advance(start() + 100_000, true).unwrap();
        assert_eq!(e.summary.fills, 0);
        let mut old = quote(200, 4, "rb2701", 99, 101, 100, 100);
        old.bid_qty[0] = 0;
        e.quote(old).unwrap();
        e.advance(start() + 200_000, true).unwrap();
        assert_eq!(e.summary.fills, 0);
        e.quote(quote(250, 5, "rb2705", 99, 101, 100, 100)).unwrap();
        e.advance(start() + 250_000, true).unwrap();
        assert_eq!(e.summary.fills, 0);
        e.quote(quote(300, 6, "rb2701", 99, 101, 100, 100)).unwrap();
        e.advance(start() + 300_000, true).unwrap();
        assert!(e.summary.fills > 0);

        cfg.roll.target_lots = Some(0);
        let mut e = engine(cfg);
        roll_start(&mut e);
        assert!(e.summary.fills > 0);
    }

    #[test]
    fn explicit_taker_gate_can_be_disabled_and_implicit_taker_stays_compatible() {
        for algorithm in [None, Some(RollAlgorithm::TakerTaker)] {
            let mut cfg = roll_engine(false, 20_000.0, 0).cfg;
            cfg.roll.algorithm = algorithm;
            cfg.execution = Execution::Taker;
            cfg.roll.use_spread_gate = algorithm.is_none();
            let mut e = engine(cfg);
            roll_start(&mut e);
            assert!(e.summary.fills > 0);
        }
    }

    #[test]
    fn taker_unpaired_liquidation_only_requires_held_contract_quote() {
        let mut cfg = roll_engine(false, 20_000.0, 0).cfg;
        cfg.roll.algorithm = Some(RollAlgorithm::TakerTaker);
        cfg.execution = Execution::Taker;
        let mut e = engine(cfg);
        roll_start(&mut e);
        assert_eq!(e.summary.fills, 0);
        e.rolls.get_mut("RB").unwrap().goal = 0;
        e.visible.remove("rb2705");
        e.control().unwrap();
        e.advance(start(), true).unwrap();
        assert_eq!(e.positions["rb2701"].lots(), 0);
        assert!(!e.positions.contains_key("rb2705"));
    }

    #[test]
    fn taker_roll_pairs_real_fills_and_keeps_completion_after_target_change() {
        for short in [false, true] {
            let mut cfg = roll_engine(short, 20_000.0, 0).cfg;
            cfg.roll.algorithm = Some(RollAlgorithm::TakerTaker);
            cfg.roll.use_spread_gate = false;
            cfg.execution = Execution::Taker;
            let mut e = engine(cfg);
            roll_start(&mut e);
            assert_eq!(e.summary.roll_paired_lots, 2);
            assert_eq!(e.summary.roll_spread_cost_cny, Some(40.0));
            assert!(e.rolls["RB"].succeeded);
            let roll = e.rolls.get_mut("RB").unwrap();
            roll.standalone = false;
            roll.goal = 0;
            e.targets.get_mut("rb2705").unwrap().lots = 0;
            let summary = e.finish(start() + 1).unwrap();
            assert!(summary.roll_complete);
            assert_eq!(summary.unfilled_lots, 2);
            assert_eq!(summary.positions["rb2705"].abs(), 2);
            let pairs: Vec<_> = e
                .sink
                .0
                .iter()
                .filter(|(t, r)| t == "roll_batches" && r.kind == "paired_taker_fill")
                .collect();
            assert_eq!(pairs.len(), 2);
            for (_, pair) in pairs {
                let (_, fill) = e
                    .sink
                    .0
                    .iter()
                    .find(|(table, row)| table == "fills" && row.order_id == pair.order_id)
                    .unwrap();
                assert_eq!(pair.contract, "rb2705");
                assert_eq!((&pair.contract, pair.price), (&fill.contract, fill.price));
            }
        }
    }

    fn spread_engine(
        short: bool,
        leg: crate::config::MakerLeg,
        lots: i64,
        delay: u64,
        report: u64,
    ) -> Engine<MemorySink> {
        let mut cfg = roll_engine(short, 20_000.0, report).cfg;
        cfg.roll.algorithm = Some(RollAlgorithm::MakerTaker);
        cfg.roll.maker_leg = leg;
        cfg.roll.initial_lots = if short { -lots } else { lots };
        cfg.roll.target_lots = Some(cfg.roll.initial_lots);
        cfg.roll.batch_lots = lots;
        cfg.max_child_lots = lots;
        cfg.order_delay_ms = delay;
        engine(cfg)
    }
    #[test]
    fn spread_roll_hedges_only_reported_fills_for_both_directions_and_lead_legs() {
        use crate::config::MakerLeg::*;
        for (short, leg, maker, hedge, side, bid, ask) in [
            (false, Old, "rb2701", "rb2705", -1, 99, 101),
            (true, Old, "rb2701", "rb2705", 1, 99, 101),
            (false, New, "rb2705", "rb2701", 1, 101, 103),
            (true, New, "rb2705", "rb2701", -1, 101, 103),
        ] {
            let mut e = spread_engine(short, leg, 1, 0, 50);
            roll_start(&mut e);
            assert_eq!(e.orders.len(), 1);
            let o = e.orders.values().next().unwrap();
            assert_eq!(
                (o.contract.as_str(), o.side, o.execution),
                (maker, side, Execution::Maker)
            );
            e.quote(quote(
                100,
                3,
                maker,
                bid,
                ask,
                if side > 0 { bid - 1 } else { ask + 1 },
                101,
            ))
            .unwrap();
            e.advance(start() + 149_999, true).unwrap();
            assert!(!e.working(hedge));
            let summary = e.finish(start() + 210_000).unwrap();
            assert!(summary.roll_complete);
            assert_eq!(summary.roll_paired_lots, 1);
            assert_eq!(summary.roll_spread_cost_cny, Some(0.0));
            let fills: Vec<_> = e
                .sink
                .0
                .iter()
                .filter(|(t, _)| t == "fills")
                .map(|(_, r)| (r.contract.as_str(), r.kind.as_str(), r.timestamp_us))
                .collect();
            assert_eq!(
                fills,
                [
                    (maker, "maker", start() + 100_000),
                    (hedge, "taker", start() + 150_000)
                ]
            );
        }
    }

    #[test]
    fn spread_gate_observes_direction_and_withdraws_an_unfavorable_resting_price() {
        for (short, better) in [(false, -1), (true, 1)] {
            let mut e = spread_engine(short, crate::config::MakerLeg::Old, 1, 0, 0);
            e.cfg.roll.spread_improvement_ticks = 1.0;
            roll_start(&mut e);
            assert!(e.orders.is_empty());
            e.quote(quote(
                100,
                3,
                "rb2705",
                101 + better,
                103 + better,
                102 + better,
                100,
            ))
            .unwrap();
            e.advance(start() + 100_000, true).unwrap();
            assert!(e.working("rb2701"));
            e.quote(quote(200, 4, "rb2705", 101, 103, 102, 100))
                .unwrap();
            e.advance(start() + 200_000, true).unwrap();
            assert!(e.orders.is_empty());
            let observed: Vec<_> = e
                .sink
                .0
                .iter()
                .filter(|(t, _)| t == "roll_spread")
                .map(|(_, r)| r.kind.as_str())
                .collect();
            assert_eq!(observed, ["waiting", "eligible", "waiting"]);
        }
    }
    #[test]
    fn capped_roll_hedges_each_reported_partial_fill() {
        let mut cfg = spread_engine(false, crate::config::MakerLeg::Old, 3, 0, 50).cfg;
        cfg.maker_fill_model = MakerFillModel::IntervalVolume;
        let mut e = engine(cfg);
        roll_start(&mut e);
        feed(&mut e, 100, 3, 102, 101);
        e.advance(start() + 149_999, true).unwrap();
        assert_eq!(e.summary.roll_paired_lots, 0);
        e.advance(start() + 150_000, true).unwrap();
        assert_eq!(e.summary.roll_paired_lots, 1);
        assert_eq!(e.contract_order("rb2701").unwrap().remaining, 2);
        feed(&mut e, 210, 4, 102, 103);
        let s = e.finish(start() + 400_000).unwrap();
        assert!(s.roll_complete);
        assert_eq!((s.roll_paired_lots, s.filled_lots), (3, 6));
    }
    #[test]
    fn adverse_spread_and_partial_ioc_retry_only_the_unhedged_lots() {
        let mut e = spread_engine(false, crate::config::MakerLeg::Old, 2, 0, 50);
        roll_start(&mut e);
        feed(&mut e, 100, 3, 102, 101);
        let mut h = quote(120, 4, "rb2705", 104, 106, 105, 101);
        h.ask_qty[0] = 1;
        e.quote(h).unwrap();
        e.advance(start() + 240_000, true).unwrap();
        assert_eq!(e.known["rb2705"], 1);
        let sent = e
            .sink
            .0
            .iter()
            .filter(|(t, r)| t == "orders" && r.reason == "roll_ioc_hedge")
            .count();
        assert_eq!(sent, 1);
        e.quote(quote(250, 5, "rb2705", 103, 105, 104, 102))
            .unwrap();
        let s = e.finish(start() + 310_000).unwrap();
        assert_eq!(
            (s.roll_paired_lots, s.roll_spread_cost_cny),
            (2, Some(50.0))
        );
        assert!(s.roll_complete);
    }
    #[test]
    fn crossing_lead_prices_are_paired_fifo_and_remainder_can_fill_passively() {
        let mut e = spread_engine(false, crate::config::MakerLeg::Old, 3, 20, 5);
        roll_start(&mut e);
        let mut m = quote(10, 3, "rb2701", 103, 105, 104, 101);
        m.bid = [Some(103), Some(102), Some(100), None, None];
        m.bid_qty = [1, 1, 10, 0, 0];
        e.quote(m).unwrap();
        e.advance(start() + 99_000, true).unwrap();
        assert_eq!(e.known["rb2705"], 2);
        feed(&mut e, 100, 4, 102, 102);
        let s = e.finish(start() + 200_000).unwrap();
        assert!(s.roll_complete);
        assert_eq!(s.roll_spread_cost_cny, Some(-30.0));
        assert_eq!(s.roll_lead_taker_lots, 2);
        assert_eq!(s.execution_shortfall_cny, -30.0);
        let lead: Vec<_> = e
            .sink
            .0
            .iter()
            .filter(|(t, r)| t == "fills" && r.reason == "roll_spread_maker")
            .map(|(_, r)| (r.kind.as_str(), r.price.unwrap()))
            .collect();
        assert_eq!(lead, [("taker", 103.0), ("taker", 102.0), ("maker", 101.0)]);
        let pairs: Vec<_> = e
            .sink
            .0
            .iter()
            .filter(|(t, r)| t == "roll_batches" && r.kind == "paired_fill")
            .map(|(_, r)| (r.value.unwrap(), r.maker_order_id.unwrap()))
            .collect();
        assert_eq!(pairs, [(0.0, 1), (1.0, 1), (2.0, 1)]);
    }
    #[test]
    fn spread_budget_stale_books_and_deadline_limit_execution() {
        let mut e = spread_engine(false, crate::config::MakerLeg::Old, 3, 0, 50);
        e.cfg.roll.max_exposure_deviation_cny = 1100.0;
        e.cfg.quote_age_ms = 200;
        roll_start(&mut e);
        assert_eq!(e.orders.values().next().unwrap().remaining, 1);
        e.advance(start() + 300_000, true).unwrap();
        assert!(e.orders.is_empty());
        e.quote(quote(400, 3, "rb2701", 99, 101, 102, 101)).unwrap();
        e.advance(start() + 400_000, true).unwrap();
        assert!(e.orders.is_empty());
        e.quote(quote(420, 4, "rb2705", 101, 103, 102, 101))
            .unwrap();
        feed(&mut e, 430, 5, 102, 102);
        let s = e.finish(start() + 450_000).unwrap();
        assert_eq!(
            (s.roll_paired_lots, s.filled_lots, s.roll_complete),
            (0, 1, false)
        );
        assert_eq!(s.unfilled_lots, 5);
    }
    #[test]
    fn changing_selected_contract_uses_configured_spread_algorithm() {
        let mut cfg = Config {
            execution: Execution::Taker,
            ..config()
        };
        cfg.roll.algorithm = Some(RollAlgorithm::MakerTaker);
        let mut e = engine(cfg);
        input(&mut e, 0, 1100.0);
        feed(&mut e, 0, 1, 100, 100);
        assert_eq!(e.known["rb2701"], 1);
        let monday = crate::timestamp("2026-11-16 09:00:00").unwrap();
        e.advance(monday, false).unwrap();
        e.begin_day(20261116, BTreeMap::from([("RB".into(), "rb2705".into())]))
            .unwrap();
        let next = |ms, seq, contract, bid, ask, last, volume| {
            let mut q = quote(ms, seq, contract, bid, ask, last, volume);
            q.day = 20261116;
            q.ts += monday - start();
            q.exchange_ts = q.ts;
            q.end = Some(monday + 60_000_000);
            q
        };
        e.quote(next(0, 1, "rb2701", 99, 101, 100, 1)).unwrap();
        e.quote(next(0, 2, "rb2705", 101, 103, 102, 1)).unwrap();
        e.advance(monday, true).unwrap();
        assert!(e.working("rb2701") && !e.working("rb2705"));
        e.quote(next(100, 3, "rb2701", 99, 101, 102, 2)).unwrap();
        let s = e.finish(monday + 200_000).unwrap();
        assert_eq!(
            (s.roll_paired_lots, s.unfilled_lots, s.roll_complete),
            (1, 0, true)
        );
    }
    #[test]
    fn spread_cancel_in_transit_still_generates_a_hedge_and_rechecks_exposure() {
        let mut e = spread_engine(false, crate::config::MakerLeg::Old, 2, 0, 5);
        e.cfg.roll.max_exposure_deviation_cny = 1020.0;
        e.cfg.cancel_delay_ms = 100;
        roll_start(&mut e);
        e.quote(quote(40, 3, "rb2701", 96, 98, 97, 100)).unwrap();
        e.advance(start() + 40_000, true).unwrap();
        assert!(e.orders.values().next().unwrap().cancel_sent);
        e.quote(quote(50, 4, "rb2705", 104, 106, 105, 101)).unwrap();
        feed(&mut e, 80, 5, 102, 101);
        let s = e.finish(start() + 150_000).unwrap();
        assert_eq!(
            (s.roll_paired_lots, s.roll_spread_cost_cny),
            (1, Some(30.0))
        );
        assert_eq!(s.positions["rb2705"], 1);
    }
}
