//! live_query <endpoint> <contract> <YYYYMMDD> <exchange enum 1..6>: prints the bars and data state.
use cn_market_proto::cn_futures_market_client::CnFuturesMarketClient;
use cn_market_proto::{get_minute_bars_request::Range, GetMinuteBarsRequest};

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    let a: Vec<String> = std::env::args().collect();
    let mut client = CnFuturesMarketClient::connect(a[1].clone()).await?;
    let t = std::time::Instant::now();
    let r = client
        .get_minute_bars(GetMinuteBarsRequest {
            exchange: a[4].parse::<i32>()?,
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
    let d = r.data_state.unwrap();
    println!(
        "latest_trading_day={} live_trading_day={} live_through={:?}",
        d.latest_trading_day,
        d.live_trading_day,
        ms(d.live_through_ms)
    );
    Ok(())
}
