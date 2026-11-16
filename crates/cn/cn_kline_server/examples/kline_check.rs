//! Queries every contract of one trading day and applies the
//! cn_futures_cta_manager candle checks (OHLC > 0 and ordered, volume/turnover
//! consistent, OHLC on the tick grid, VWAP within [low, high]).
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
                for b in &resp.bars {
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
    Ok(())
}
