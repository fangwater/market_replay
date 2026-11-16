//! Queries every contract of one trading day and applies the
//! cn_futures_cta_manager candle checks (OHLC > 0 and ordered, volume/turnover
//! consistent, OHLC on the tick grid, VWAP within [low, high]).
//! Also checks that every baseline field carried by MinuteBar equals the
//! parquet value exactly (size buckets, buy/sell splits, twap, mid, book5).
//! Usage: kline_check <endpoint> <baseline_root> <YYYYMMDD>
use cn_market_proto::cn_futures_market_client::CnFuturesMarketClient;
use cn_market_proto::{get_minute_bars_request::Range, Exchange, GetMinuteBarsRequest};

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    let a: Vec<String> = std::env::args().collect();
    let (endpoint, root, day) = (a[1].clone(), a[2].clone(), a[3].parse::<u32>()?);
    let mut client = CnFuturesMarketClient::connect(endpoint).await?;
    let (mut contracts, mut bars, mut widened, mut bad, mut errors) = (0, 0, 0, 0, 0);
    let (mut with_mid, mut without_mid, mut bad_mid) = (0u64, 0u64, 0u64);
    let (mut field_mismatch, mut with_book) = (0u64, 0u64);
    for (dir, ex) in [
        ("ccfx", Exchange::Cffex),
        ("xsge", Exchange::Shfe),
        ("xsie", Exchange::Ine),
        ("xzce", Exchange::Czce),
        ("xdce", Exchange::Dce),
        ("xgfe", Exchange::Gfex),
    ] {
        for prod in std::fs::read_dir(format!("{root}/{dir}"))? {
            let path = prod?.path().join(format!("{day}.parquet"));
            if !path.exists() {
                continue;
            }
            let rows = cn_futures_l2::export_1min::read_day_parquet(&path)?;
            let mut ids: Vec<String> = rows.iter().map(|r| r.contract_id.clone()).collect();
            ids.sort();
            ids.dedup();
            for id in ids {
                let req = GetMinuteBarsRequest {
                    exchange: ex as i32,
                    contract: id.clone(),
                    range: Some(Range::TradingDay(day)),
                };
                let resp = match client.get_minute_bars(req).await {
                    Ok(r) => r.into_inner(),
                    Err(s) => {
                        errors += 1;
                        eprintln!("{id}: {} {}", s.code(), s.message());
                        continue;
                    }
                };
                contracts += 1;
                let c = resp.contract.unwrap();
                let by_ts: std::collections::HashMap<
                    i64,
                    &cn_futures_core::baseline_1min::BaselineMinute,
                > = rows
                    .iter()
                    .filter(|r| r.contract_id == id)
                    .map(|r| (r.ts * 1000, r))
                    .collect();
                for b in &resp.bars {
                    let r = by_ts[&b.open_time_ms];
                    let sb = b.size_buckets.as_ref().unwrap();
                    let fin = |v: Option<f64>| v.filter(|x| x.is_finite());
                    let mut same = b.mid_price == fin(r.mid_price)
                        && b.twap == fin(r.twap)
                        && b.buy_vwap == fin(r.buy_vwap)
                        && b.sell_vwap == fin(r.sell_vwap)
                        && b.net_buy_pct == fin(r.net_buy_pct)
                        && [
                            b.buy_volume_lots,
                            b.sell_volume_lots,
                            b.buy_turnover_cny,
                            b.sell_turnover_cny,
                            b.buy_snapshot_trades,
                            b.sell_snapshot_trades,
                            b.avg_interval_turnover_cny,
                            b.net_buy_turnover_cny,
                            b.net_buy_volume_lots,
                        ] == [
                            r.buy_volume,
                            r.sell_volume,
                            r.buy_amount,
                            r.sell_amount,
                            r.buy_count,
                            r.sell_count,
                            r.avg_amount,
                            r.net_buy_amount,
                            r.net_buy_volume,
                        ]
                        && [
                            sb.large_turnover_cny,
                            sb.medium_turnover_cny,
                            sb.small_turnover_cny,
                            sb.large_buy_cny,
                            sb.large_sell_cny,
                            sb.medium_buy_cny,
                            sb.medium_sell_cny,
                            sb.small_buy_cny,
                            sb.small_sell_cny,
                            sb.net_buy_large_cny,
                            sb.net_buy_medium_cny,
                            sb.net_buy_small_cny,
                        ] == [
                            r.large_order,
                            r.medium_order,
                            r.small_order,
                            r.large_buy,
                            r.large_sell,
                            r.medium_buy,
                            r.medium_sell,
                            r.small_buy,
                            r.small_sell,
                            r.net_buy_large,
                            r.net_buy_medium,
                            r.net_buy_small,
                        ];
                    match (&b.book, &r.book) {
                        (Some(g), Some(p)) => {
                            with_book += 1;
                            for i in 0..5 {
                                same &= g.bids[i].price == fin(p.bid_prices[i])
                                    && g.bids[i].volume_lots == fin(p.bid_sizes[i])
                                    && g.asks[i].price == fin(p.ask_prices[i])
                                    && g.asks[i].volume_lots == fin(p.ask_sizes[i]);
                            }
                        }
                        (None, None) => {}
                        _ => same = false,
                    }
                    if !same {
                        field_mismatch += 1;
                        if field_mismatch <= 3 {
                            eprintln!("FIELD MISMATCH {id} {}", b.open_time_ms);
                        }
                    }
                    bars += 1;
                    widened += b.widened as u32;
                    let on_tick = |p: f64| {
                        let t = p / c.price_tick;
                        (t - t.round()).abs() <= t.abs().max(1.0) * 1e-9
                    };
                    let vw = b.turnover_cny / b.volume_lots / c.volume_multiple;
                    let tol = b.high.abs().max(1.0) * 1e-8;
                    let ok = [b.open, b.high, b.low, b.close]
                        .iter()
                        .all(|v| v.is_finite() && *v > 0.0)
                        && b.low <= b.open
                        && b.low <= b.close
                        && b.high >= b.open
                        && b.high >= b.close
                        && b.volume_lots > 0.0
                        && b.turnover_cny > 0.0
                        && vw >= b.low - tol
                        && vw <= b.high + tol
                        && [b.open, b.high, b.low, b.close].iter().all(|p| on_tick(*p));
                    match b.mid_price {
                        Some(m) => {
                            with_mid += 1;
                            // Mid is a half-tick grid price, positive and finite.
                            let t = 2.0 * m / c.price_tick;
                            if !(m.is_finite()
                                && m > 0.0
                                && (t - t.round()).abs() <= t.abs().max(1.0) * 1e-9)
                            {
                                bad_mid += 1;
                            }
                        }
                        None => without_mid += 1,
                    }
                    if !ok {
                        bad += 1;
                        if bad <= 5 {
                            eprintln!("BAD {id} {b:?} tick={}", c.price_tick);
                        }
                    }
                }
            }
        }
    }
    println!("day {day}: contracts {contracts} bars {bars} widened {widened} ({:.2}%) cta_check_failures {bad} rpc_errors {errors}",
             widened as f64 * 100.0 / bars.max(1) as f64);
    println!("mid_price: set {with_mid} unset {without_mid} off_half_tick {bad_mid}");
    println!(
        "baseline fields vs parquet: mismatches {field_mismatch}; bars with book5 {with_book}"
    );
    Ok(())
}
