//! live_query <endpoint> <contract> <YYYYMMDD> <exchange enum 1..6> [cutoff_ms]
//! Prints the bars, then the exchange's data state, the product's size
//! thresholds and its trading sessions (the three separate RPCs).
use cn_market_proto::cn_futures_market_client::CnFuturesMarketClient;
use cn_market_proto::{
    get_minute_bars_request::Range, GetDataStateRequest, GetMinuteBarsRequest,
    GetSizeThresholdsRequest, GetTradingSessionsRequest,
};

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    let a: Vec<String> = std::env::args().collect();
    let mut client = CnFuturesMarketClient::connect(a[1].clone()).await?;
    let exchange = a[4].parse::<i32>()?;
    let product: String = a[2]
        .chars()
        .take_while(|c| c.is_ascii_alphabetic())
        .collect::<String>()
        .to_ascii_uppercase();
    let t = std::time::Instant::now();
    let r = client
        .get_minute_bars(GetMinuteBarsRequest {
            exchange,
            contract: a[2].clone(),
            range: Some(Range::TradingDay(a[3].parse()?)),
            include_empty_minutes: false,
        })
        .await?
        .into_inner();
    let ms = |t: i64| {
        chrono::DateTime::from_timestamp_millis(t).map(|d| {
            d.with_timezone(&chrono_tz::Asia::Shanghai)
                .format("%H:%M:%S")
                .to_string()
        })
    };
    println!("took {:?}, bars={}", t.elapsed(), r.bars.len());
    if let (Some(f), Some(l)) = (r.bars.first(), r.bars.last()) {
        println!(
            "first {:?} last {:?} last.provisional={} size_buckets={}",
            ms(f.open_time_ms),
            ms(l.open_time_ms),
            l.provisional,
            l.size_buckets.is_some()
        );
    }
    let with = r.bars.iter().filter(|b| b.size_buckets.is_some()).count();
    println!("bars with size_buckets: {with}/{}", r.bars.len());
    // Digest of the bars opened before `cutoff_ms` (arg 5), to compare servers.
    if let Some(cut) = a.get(5).and_then(|c| c.parse::<i64>().ok()) {
        use std::hash::{Hash, Hasher};
        let mut h = std::collections::hash_map::DefaultHasher::new();
        let n = r
            .bars
            .iter()
            .filter(|b| b.open_time_ms < cut)
            .inspect(|b| format!("{b:?}").hash(&mut h))
            .count();
        println!("digest bars<cutoff: n={n} hash={:016x}", h.finish());
    }

    // GetDataState for the exchange (cheap to poll).
    let d = client
        .get_data_state(GetDataStateRequest { exchange })
        .await?
        .into_inner();
    println!(
        "data_state: latest_trading_day={} live_trading_day={} live_through={:?} session_snapshot_as_of={}",
        d.latest_trading_day,
        d.live_trading_day,
        ms(d.live_through_ms),
        d.session_snapshot_as_of
    );

    // GetSizeThresholds for the product, and the count for the whole exchange.
    let one = client
        .get_size_thresholds(GetSizeThresholdsRequest {
            exchange,
            product: product.clone(),
        })
        .await?
        .into_inner();
    for p in &one.products {
        if p.thresholds.is_empty() {
            println!("size_thresholds {}: none", p.product);
        }
        for t in &p.thresholds {
            println!(
                "size_threshold {} applies_month={} p50={} p90={}",
                p.product, t.applies_month, t.p50_turnover_cny, t.p90_turnover_cny
            );
        }
    }
    let all = client
        .get_size_thresholds(GetSizeThresholdsRequest {
            exchange,
            product: String::new(),
        })
        .await?
        .into_inner();
    let with_thr = all
        .products
        .iter()
        .filter(|p| !p.thresholds.is_empty())
        .count();
    println!(
        "size_thresholds exchange total: {} products, {with_thr} with thresholds",
        all.products.len()
    );

    // GetTradingSessions for the product.
    let s = client
        .get_trading_sessions(GetTradingSessionsRequest { exchange, product })
        .await?
        .into_inner();
    println!("sessions snapshot_as_of={}", s.snapshot_as_of);
    let clock = |s: u32| format!("{:02}:{:02}:{:02}", s / 3600, s / 60 % 60, s % 60);
    for p in &s.products {
        for w in &p.windows {
            println!(
                "session {} {} -> {}{}",
                p.product,
                clock(w.begin_sec),
                clock(w.end_sec),
                if w.end_sec <= w.begin_sec {
                    " (past midnight)"
                } else {
                    ""
                }
            );
        }
    }
    Ok(())
}
