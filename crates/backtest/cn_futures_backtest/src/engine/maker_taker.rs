//! A confirmed maker lot authorizes at most one opposite IOC lot in the same batch.
use super::*;
use crate::config::MakerLeg;

const MAKER: &str = "roll_spread_maker";
const HEDGE: &str = "roll_ioc_hedge";
type Frame = (u32, u64);

impl MakerLeg {
    // This permutation works in both directions: old/new <-> maker/hedge.
    fn order<T>(self, old: T, new: T) -> (T, T) {
        match self {
            Self::Old => (old, new),
            Self::New => (new, old),
        }
    }
}

#[derive(Clone, Debug, Default)]
pub(super) struct State {
    pair: Option<Pair>,
    last_entry: Option<(Frame, Frame)>,
    last_observation: Option<(i64, bool)>,
}
#[derive(Clone, Debug)]
struct Pair {
    maker_id: u64,
    confirmed_prices: Vec<(i64, f64)>,
    maker_start: i64,
    hedge_start: i64,
    side: i64,
    lots: i64,
    maker_price: f64,
    last_hedge: Option<(Frame, i64)>,
    check_at: i64,
}

// Capture the actual confirmed maker prices on submission. An in-flight hedge
// keeps its attribution even across a target or contract selection change.
#[derive(Clone, Debug)]
pub(super) struct HedgeBasis {
    maker_id: u64,
    confirmed_prices: Vec<(i64, f64)>,
    previously_hedged: i64,
    submitted: i64,
    direction: i64,
    reference: f64,
}

fn hedge_top(q: &Quote, maker_side: i64) -> Option<(i64, i64)> {
    if maker_side > 0 {
        Some((q.bid[0]?, q.bid_qty[0]))
    } else {
        Some((q.ask[0]?, q.ask_qty[0]))
    }
}

impl<S: Sink> Engine<S> {
    pub(super) fn confirm_roll_fill(&mut self, id: u64, contract: &str, delta: i64, price: f64) {
        if let Some(pair) = self
            .rolls
            .get_mut(&crate::product(contract))
            .and_then(|r| r.maker_taker.pair.as_mut())
            .filter(|p| p.maker_id == id)
        {
            pair.confirmed_prices.push((delta.abs(), price));
        }
    }

    // The proposed fill extends only one end of the pending-order exposure interval.
    fn roll_capacity(&self, r: &RollState, units: (f64, f64), unit: f64, delta: i64) -> i64 {
        let (lo, hi) = self.marked_risk_interval(r, units.0, units.1);
        let budget = self.cfg.roll.max_exposure_deviation_cny;
        let room = if delta > 0 {
            r.amount + budget - hi
        } else {
            lo - (r.amount - budget)
        };
        let lots = ((room + 1e-6) / unit).floor().max(0.0) as i64;
        delta.signum()
            * delta
                .abs()
                .min(lots)
                .min(self.cfg.roll.batch_lots)
                .min(self.cfg.max_child_lots)
    }

    fn entry_spread(
        &self,
        r: &RollState,
        maker_price: f64,
        hedge_price: f64,
        tick: f64,
    ) -> (f64, bool) {
        let (old, new) = self.cfg.roll.maker_leg.order(maker_price, hedge_price);
        let (old_mid, new_mid) = r.arrival.unwrap();
        let spread = new - old;
        let improvement = ((new_mid - old_mid) - spread) * r.initial.signum() as f64;
        (
            spread,
            !self.cfg.roll.use_spread_gate
                || improvement + tick * 1e-9 >= self.cfg.roll.spread_improvement_ticks * tick,
        )
    }

    pub(super) fn maker_taker_control(
        &mut self,
        r: &mut RollState,
        books: Option<&(Quote, Quote)>,
    ) -> Result<()> {
        let leg = self.cfg.roll.maker_leg;
        let (maker, hedge) = leg.order(r.old.as_str(), r.new.as_str());
        let (ms, hs) = (self.metadata.spec(maker)?, self.metadata.spec(hedge)?);
        let tick = if leg == MakerLeg::Old {
            ms.tick
        } else {
            hs.tick
        };
        let (maker_ref, hedge_ref) = {
            let (a, b) = r.arrival.unwrap();
            leg.order(a, b)
        };
        let known = |c: &str| self.known.get(c).copied().unwrap_or(0);
        let (km, kh) = (known(maker), known(hedge));
        let (tm, th) = leg.order(0, r.goal);
        let (dm, dh) = (tm - km, th - kh);
        let quotes = books.and_then(|(old, new)| {
            let (m, h) = leg.order(old, new);
            Some((
                m,
                h,
                m.mid(&ms)? * ms.multiplier,
                h.mid(&hs)? * hs.multiplier,
            ))
        });

        let side = r.maker_taker.pair.as_ref().map_or(dm.signum(), |p| p.side);
        if let Some((m, h, _, _)) = quotes.filter(|_| side != 0) {
            if let (Some(limit), Some((px, depth))) = (
                m.limit(side, Execution::Maker, &self.cfg),
                hedge_top(h, side),
            ) {
                let maker_price = r
                    .maker_taker
                    .pair
                    .as_ref()
                    .map_or(ms.price(limit), |p| p.maker_price);
                let (spread, pass) = self.entry_spread(r, maker_price, hs.price(px), tick);
                let pass = pass && depth > 0;
                if r.maker_taker
                    .last_observation
                    .is_none_or(|(ts, previous)| previous != pass || self.now - ts >= 1_000_000)
                {
                    r.maker_taker.last_observation = Some((self.now, pass));
                    let (a, b) = r.arrival.unwrap();
                    self.emit(
                        "roll_spread",
                        Row {
                            kind: if pass { "eligible" } else { "waiting" }.into(),
                            product: ms.product.clone(),
                            contract: maker.into(),
                            price: Some(maker_price),
                            value: Some(spread),
                            reference_price: Some(b - a),
                            lots: Some(depth),
                            source_cursor: Some(h.cursor),
                            ..Default::default()
                        },
                    )?;
                }
            }
        }

        if let Some(mut pair) = r.maker_taker.pair.take() {
            let maker_filled = (km - pair.maker_start) * pair.side;
            let hedge_filled = (pair.hedge_start - kh) * pair.side;
            if self.working(maker) && maker_filled < pair.lots {
                let remaining = pair.lots - maker_filled;
                let valid = quotes.is_some_and(|(_, h, maker_unit, hedge_unit)| {
                    let (old_unit, new_unit) = leg.order(maker_unit, hedge_unit);
                    let (lo, hi) = self.marked_risk_interval(r, old_unit, new_unit);
                    let budget = self.cfg.roll.max_exposure_deviation_cny;
                    let inside = if pair.side > 0 {
                        hi <= r.amount + budget + 1e-6
                    } else {
                        lo >= r.amount - budget - 1e-6
                    };
                    inside
                        && hedge_top(h, pair.side).is_some_and(|(px, qty)| {
                            qty >= remaining
                                && self.entry_spread(r, pair.maker_price, hs.price(px), tick).1
                        })
                });
                if !valid
                    || dm * pair.side < remaining
                    || -dh * pair.side < remaining + maker_filled - hedge_filled
                {
                    self.cancel_contract(maker)?;
                } else {
                    self.reprice(maker, self.cfg.reprice_ms)?;
                    // Withdraw even if both quote streams go silent.
                    if self.now >= pair.check_at {
                        let (m, h, _, _) = quotes.unwrap();
                        pair.check_at = self.spread_expiry(m, h);
                        self.push(pair.check_at, 5, Event::Wake)?;
                    }
                }
            }
            let due = (maker_filled - hedge_filled).min(-dh * pair.side).max(0);
            if due > 0 && !self.working(hedge) {
                self.hedge_confirmed(r, &mut pair, due, hedge_filled)?;
            }
            if due > 0 || self.working(maker) || self.working(hedge) {
                r.maker_taker.pair = Some(pair);
                return Ok(());
            }
        }

        // Outstanding residual orders and orders inherited at a selection change
        // must reach a terminal report before starting a pair.
        if self.working(maker) || self.working(hedge) {
            self.reprice(maker, self.cfg.reprice_ms)?;
            self.reprice(hedge, self.cfg.reprice_ms)?;
            return Ok(());
        }
        let Some((m, h, maker_unit, hedge_unit)) = quotes else {
            return Ok(());
        };
        let frames = ((m.day, m.cursor), (h.day, h.cursor));
        if r.maker_taker.last_entry == Some(frames) {
            return Ok(());
        }
        let units = leg.order(maker_unit, hedge_unit);
        if dm == 0 || dh == 0 || dm.signum() == dh.signum() {
            // Only opposing lots form a calendar spread. Reconcile any rounding
            // or strategy target change as a separately attributed outright order.
            for (c, d, unit, reference) in [
                (maker, dm, maker_unit, maker_ref),
                (hedge, dh, hedge_unit, hedge_ref),
            ] {
                let qty = self.roll_capacity(r, units, unit, d);
                if self
                    .submit(
                        c,
                        qty,
                        self.cfg.execution,
                        reference,
                        "roll_target_adjustment",
                    )?
                    .is_some()
                {
                    r.maker_taker.last_entry = Some(frames);
                    break;
                }
            }
            return Ok(());
        }
        let side = dm.signum();
        let Some((hedge_ticks, available)) = hedge_top(h, side) else {
            return Ok(());
        };
        let Some(limit) = m.limit(side, Execution::Maker, &self.cfg) else {
            return Ok(());
        };
        let maker_price = ms.price(limit);
        let (spread, pass) = self.entry_spread(r, maker_price, hs.price(hedge_ticks), tick);
        if !pass {
            return Ok(());
        };
        let lots = dm.abs().min(dh.abs()).min(available);
        let qty = self.roll_capacity(r, units, maker_unit, side * lots);
        // Reserve enough risk budget for both possible stages at the observed marks.
        let net_unit = maker_unit - hedge_unit;
        let qty = if net_unit.abs() > f64::EPSILON {
            let net_side = if net_unit > 0.0 { side } else { -side };
            let capacity = self.roll_capacity(r, units, net_unit.abs(), net_side * qty.abs());
            side * capacity.abs()
        } else {
            qty
        };
        let Some(id) = self.submit(maker, qty, Execution::Maker, maker_ref, MAKER)? else {
            return Ok(());
        };
        let check_at = self.spread_expiry(m, h);
        self.push(check_at, 5, Event::Wake)?;
        r.maker_taker.last_entry = Some(frames);
        r.maker_taker.pair = Some(Pair {
            maker_id: id,
            confirmed_prices: Vec::new(),
            maker_start: km,
            hedge_start: kh,
            side,
            lots: qty.abs(),
            maker_price,
            last_hedge: None,
            check_at,
        });
        let (old_mid, new_mid) = r.arrival.unwrap();
        self.emit(
            "roll_batches",
            Row {
                kind: "maker_posted".into(),
                product: ms.product,
                contract: maker.into(),
                order_id: Some(id),
                side: Some(side),
                lots: Some(qty.abs()),
                price: Some(maker_price),
                value: Some(spread),
                reference_price: Some(new_mid - old_mid),
                reason: hedge.into(),
                ..Default::default()
            },
        )
    }

    fn hedge_confirmed(
        &mut self,
        r: &RollState,
        pair: &mut Pair,
        due: i64,
        already_hedged: i64,
    ) -> Result<()> {
        let leg = self.cfg.roll.maker_leg;
        let (maker, hedge) = leg.order(r.old.as_str(), r.new.as_str());
        let Some(h) = self.fresh(hedge, &self.visible) else {
            return Ok(());
        };
        let confirmed = pair.confirmed_prices.iter().map(|&(qty, _)| qty).sum();
        let frame = ((h.day, h.cursor), confirmed);
        if pair.last_hedge == Some(frame) {
            return Ok(());
        }
        let (ms, hs) = (self.metadata.spec(maker)?, self.metadata.spec(hedge)?);
        let Some(hedge_mid) = h.mid(&hs) else {
            return Ok(());
        };
        // The hedge requires a fresh book. A confirmed lead fill can mark
        // that leg for risk even when its latest two-sided book has expired.
        let maker_mid = self
            .fresh(maker, &self.visible)
            .and_then(|q| q.mid(&ms))
            .unwrap_or(pair.confirmed_prices.last().expect("confirmed lead fill").1);
        let unit = hedge_mid * hs.multiplier;
        let qty = self.roll_capacity(
            r,
            leg.order(maker_mid * ms.multiplier, unit),
            unit,
            -pair.side * due,
        );
        let (old_mid, new_mid) = r.arrival.unwrap();
        let (_, reference) = leg.order(old_mid, new_mid);
        if let Some(id) = self.submit(hedge, qty, Execution::Taker, reference, HEDGE)? {
            self.orders.get_mut(&id).unwrap().hedge_basis = Some(HedgeBasis {
                maker_id: pair.maker_id,
                confirmed_prices: pair.confirmed_prices.clone(),
                previously_hedged: already_hedged,
                submitted: qty.abs(),
                direction: r.initial.signum(),
                reference: new_mid - old_mid,
            });
            pair.last_hedge = Some(frame);
        }
        Ok(())
    }
    fn spread_expiry(&self, m: &Quote, h: &Quote) -> i64 {
        let age = self.cfg.quote_age_ms as i64 * 1000;
        (m.ts.min(h.ts) + age + 1)
            .min(m.end.unwrap())
            .min(h.end.unwrap())
    }

    pub(super) fn record_roll_fill(
        &mut self,
        o: &Order,
        price: f64,
        lots: i64,
        liquidity: Execution,
        spec: &Spec,
    ) -> Result<()> {
        if o.reason == MAKER && liquidity == Execution::Taker {
            self.summary.roll_lead_taker_lots += lots as u64;
        }
        let Some(basis) = &o.hedge_basis else {
            return Ok(());
        };
        let mut skip = basis.previously_hedged + basis.submitted - o.remaining;
        let mut needed = lots;
        let mut notional = 0.0;
        for &(qty, px) in &basis.confirmed_prices {
            let matched = (qty - skip).clamp(0, needed);
            notional += matched as f64 * px;
            needed -= matched;
            skip = (skip - qty).max(0);
            if needed == 0 {
                break;
            }
        }
        ensure!(needed == 0, "hedge exceeds confirmed maker lots");
        let (old, new) = self.cfg.roll.maker_leg.order(notional / lots as f64, price);
        let spread = new - old;
        let reference = basis.reference;
        let cost = cny(money(
            (spread - reference) * basis.direction as f64 * lots as f64 * spec.multiplier,
        ));
        self.summary.roll_paired_lots += lots as u64;
        *self.summary.roll_spread_cost_cny.get_or_insert(0.0) += cost;
        self.emit(
            "roll_batches",
            Row {
                kind: "paired_fill".into(),
                product: spec.product.clone(),
                contract: o.contract.clone(),
                order_id: Some(o.id),
                maker_order_id: Some(basis.maker_id),
                lots: Some(lots),
                price: Some(price),
                value: Some(spread),
                reference_price: Some(reference),
                shortfall_cny: Some(cost),
                ..Default::default()
            },
        )
    }
}
