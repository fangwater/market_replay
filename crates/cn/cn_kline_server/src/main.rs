//! `cn-kline-server`: gRPC `cn_market.v1.CnFuturesMarket` over the production
//! `baseline_data_1min` archive. No separate kline store: every request reads
//! the day parquet files.
//!
//! - Only minutes with volume > 0 are returned (the archive forward-fills empty
//!   minutes inside continuous segments; consumers must not see fabricated bars).
//! - high/low are widened on the tick grid to contain the minute VWAP: snapshot
//!   OHLC misses trades between 2/4 Hz snapshots (and DCE combo legs), while the
//!   true traded range always contains the VWAP and lies on the tick grid.

use anyhow::{bail, ensure, Context, Result};
use chrono::{Duration, NaiveDate, TimeZone, Utc};
use chrono_tz::Asia::Shanghai;
use clap::Parser;
use cn_futures_l2::export_1min::read_day_parquet;
use cn_futures_l2::multipliers::{load_multiplier_catalog, ProductMultiplier};
use cn_market_proto::cn_futures_market_server::{CnFuturesMarket, CnFuturesMarketServer};
use cn_market_proto::{
    get_minute_bars_request::Range, BookLevel, Contract, DataState, Exchange, GetDataStateRequest,
    GetMinuteBarsRequest, GetMinuteBarsResponse, MinuteBar, SizeBuckets,
};
use serde::Deserialize;
use std::collections::{BTreeSet, HashMap};
use std::path::{Path, PathBuf};
use std::sync::{Arc, RwLock};
use tonic::{Request, Response, Status};

mod dominant;

/// A time-range request may span at most this many calendar days.
const MAX_RANGE_DAYS: i64 = 31;

#[derive(Debug, Clone, Deserialize)]
#[serde(deny_unknown_fields)]
struct Config {
    listen: String,
    baseline_root: PathBuf,
    /// cn-daily state file (latest processed trading day).
    daily_state_file: PathBuf,
    /// Reference service base URL, e.g. http://127.0.0.1:8765
    reference_api: String,
    session_as_of: String,
    #[serde(default = "d_refresh")]
    metadata_refresh_s: u64,
    /// HTTP JSON endpoint for dominant-contract queries (not gRPC).
    #[serde(default)]
    http_listen: Option<String>,
    /// Directory with `<exchange>_dominant.csv` from the daily roll replay.
    #[serde(default)]
    roll_dir: Option<PathBuf>,
}
fn d_refresh() -> u64 {
    3600
}

#[derive(Parser)]
#[command(name = "cn-kline-server")]
struct Cli {
    #[arg(long)]
    config: PathBuf,
}

fn archive_exchange(e: Exchange) -> Option<(&'static str, &'static str)> {
    Some(match e {
        Exchange::Cffex => ("ccfx", "CFFEX"),
        Exchange::Shfe => ("xsge", "SHFE"),
        Exchange::Ine => ("xsie", "INE"),
        Exchange::Czce => ("xzce", "CZCE"),
        Exchange::Dce => ("xdce", "DCE"),
        Exchange::Gfex => ("xgfe", "GFEX"),
        Exchange::Unspecified => return None,
    })
}

fn product_of(code: &str) -> String {
    code.chars()
        .take_while(|c| c.is_ascii_alphabetic())
        .collect::<String>()
        .to_ascii_uppercase()
}

/// Lower-case; CZCE `YMM` widened to `YYMM` using the trading day's decade.
pub fn canonical(code: &str, trading_day: u32) -> String {
    let lower = code.trim().to_ascii_lowercase();
    let letters = lower
        .chars()
        .take_while(|c| c.is_ascii_alphabetic())
        .count();
    let tail = &lower[letters..];
    if tail.len() == 3 && tail.bytes().all(|b| b.is_ascii_digit()) && trading_day > 0 {
        let year = (trading_day / 10_000) as i32;
        let mut full = year / 10 * 10 + (tail.as_bytes()[0] - b'0') as i32;
        if full < year - 1 {
            full += 10;
        }
        return format!("{}{:02}{}", &lower[..letters], full % 100, &tail[1..]);
    }
    lower
}

/// Tick table from the reference service: exact instrument, else product-wide.
#[derive(Default)]
struct Ticks {
    by_instrument: HashMap<(String, String), f64>,
    by_product: HashMap<(String, String), BTreeSet<u64>>,
}

impl Ticks {
    fn insert(&mut self, venue: &str, instrument: &str, product: &str, tick: f64) {
        self.by_instrument
            .insert((venue.into(), canonical(instrument, 0)), tick);
        self.by_product
            .entry((venue.into(), product.to_ascii_uppercase()))
            .or_default()
            .insert(tick.to_bits());
    }

    fn lookup(&self, venue: &str, native: &str, trading_day: u32) -> Result<(f64, &'static str)> {
        // Reference ids are native (CZCE 3-digit); compare both spellings.
        for key in [canonical(native, 0), canonical(native, trading_day)] {
            if let Some(t) = self.by_instrument.get(&(venue.to_string(), key)) {
                return Ok((*t, "instrument"));
            }
        }
        let product = product_of(native);
        match self.by_product.get(&(venue.to_string(), product.clone())) {
            Some(set) if set.len() == 1 => Ok((f64::from_bits(*set.iter().next().unwrap()), "product")),
            Some(set) => bail!(
                "price tick of {venue} {native} unknown and product {product} has {} different ticks",
                set.len()
            ),
            None => bail!("no price tick for {venue} {native} (product {product}) in reference data"),
        }
    }
}

async fn load_ticks(base: &str) -> Result<Ticks> {
    let client = reqwest::Client::builder()
        .no_proxy()
        .timeout(std::time::Duration::from_secs(30))
        .build()?;
    let mut ticks = Ticks::default();
    for venue in ["CFFEX", "SHFE", "INE", "CZCE", "DCE", "GFEX"] {
        let url = format!("{}/api/v1/exchanges/{venue}", base.trim_end_matches('/'));
        let v: serde_json::Value = client
            .get(&url)
            .send()
            .await
            .with_context(|| format!("GET {url}"))?
            .error_for_status()?
            .json()
            .await?;
        let exchanges = v["exchanges"]
            .as_array()
            .context("reference: no exchanges array")?;
        for ex in exchanges {
            for inst in ex["instruments"].as_array().into_iter().flatten() {
                let (Some(id), Some(product), Some(tick)) = (
                    inst["instrumentId"].as_str(),
                    inst["productId"].as_str(),
                    inst["priceTick"]
                        .as_str()
                        .and_then(|t| t.parse::<f64>().ok()),
                ) else {
                    continue;
                };
                if tick.is_finite() && tick > 0.0 {
                    ticks.insert(venue, id, product, tick);
                }
            }
        }
    }
    ensure!(
        !ticks.by_instrument.is_empty(),
        "reference service returned no price ticks"
    );
    Ok(ticks)
}

/// Rounds to the tick's decimal precision to remove binary noise from n * tick.
fn snap(x: f64, tick: f64) -> f64 {
    let mut decimals = 0;
    let mut t = tick;
    while decimals < 10 && (t - t.round()).abs() > 1e-9 {
        t *= 10.0;
        decimals += 1;
    }
    let p = 10f64.powi(decimals);
    (x * p).round() / p
}

/// Widens [low, high] on the tick grid so that it contains `vwap`.
fn widen(high: f64, low: f64, vwap: f64, tick: f64) -> (f64, f64, bool) {
    let tol = high.abs().max(1.0) * 1e-8;
    let (mut h, mut l, mut widened) = (high, low, false);
    if vwap > high + tol {
        h = snap((vwap / tick - 1e-9).ceil() * tick, tick);
        widened = true;
    }
    if vwap < low - tol {
        l = snap((vwap / tick + 1e-9).floor() * tick, tick);
        widened = true;
    }
    (h, l, widened)
}

fn yyyymmdd(d: NaiveDate) -> u32 {
    d.format("%Y%m%d").to_string().parse().unwrap_or(0)
}

fn day_of_file(p: &Path) -> Option<NaiveDate> {
    NaiveDate::parse_from_str(p.file_stem()?.to_str()?, "%Y%m%d").ok()
}

struct Metadata {
    multipliers: HashMap<String, ProductMultiplier>,
    ticks: Ticks,
}

struct Service {
    cfg: Config,
    meta: Arc<RwLock<Metadata>>,
}

impl Service {
    fn latest_trading_day(&self) -> u32 {
        std::fs::read(&self.cfg.daily_state_file)
            .ok()
            .and_then(|b| serde_json::from_slice::<serde_json::Value>(&b).ok())
            .and_then(|v| v["last_done"].as_str().map(str::to_string))
            .and_then(|s| NaiveDate::parse_from_str(&s, "%Y-%m-%d").ok())
            .map_or(0, yyyymmdd)
    }

    fn data_state(&self) -> DataState {
        DataState {
            source: "baseline_data_1min".into(),
            latest_trading_day: self.latest_trading_day(),
            session_snapshot_as_of: self.cfg.session_as_of.clone(),
        }
    }

    fn bars(&self, req: GetMinuteBarsRequest) -> Result<GetMinuteBarsResponse, Status> {
        let exchange = Exchange::try_from(req.exchange).unwrap_or(Exchange::Unspecified);
        let (dir_name, venue) = archive_exchange(exchange)
            .ok_or_else(|| Status::invalid_argument("exchange is required"))?;
        let contract = req.contract.trim();
        let product = product_of(contract);
        if product.is_empty()
            || contract.len() > 31
            || !contract.chars().all(|c| c.is_ascii_alphanumeric())
        {
            return Err(Status::invalid_argument("invalid contract code"));
        }
        let dir = self.cfg.baseline_root.join(dir_name).join(&product);
        if !dir.is_dir() {
            return Err(Status::not_found(format!(
                "no archive for {venue} product {product}"
            )));
        }
        // Candidate day files and an optional open-time filter.
        let (days, window): (Vec<NaiveDate>, Option<(i64, i64)>) = match req.range {
            Some(Range::TradingDay(d)) => {
                let day = NaiveDate::parse_from_str(&d.to_string(), "%Y%m%d")
                    .map_err(|_| Status::invalid_argument("trading_day must be YYYYMMDD"))?;
                (vec![day], None)
            }
            Some(Range::Time(t)) => {
                if t.end_ms < t.start_ms {
                    return Err(Status::invalid_argument("end_ms precedes start_ms"));
                }
                let local = |ms: i64| {
                    Utc.timestamp_millis_opt(ms)
                        .single()
                        .map(|t| t.with_timezone(&Shanghai).date_naive())
                };
                let (Some(a), Some(b)) = (local(t.start_ms), local(t.end_ms)) else {
                    return Err(Status::invalid_argument("time out of range"));
                };
                if (b - a).num_days() > MAX_RANGE_DAYS {
                    return Err(Status::invalid_argument(format!(
                        "range exceeds {MAX_RANGE_DAYS} days"
                    )));
                }
                // Night minutes of natural day N belong to a later trading day:
                // look ahead past weekends/holidays.
                let hi = b + Duration::days(10);
                let mut days: Vec<NaiveDate> = std::fs::read_dir(&dir)
                    .map_err(|e| Status::internal(e.to_string()))?
                    .filter_map(|e| e.ok())
                    .filter_map(|e| day_of_file(&e.path()))
                    .filter(|d| *d >= a && *d <= hi)
                    .collect();
                days.sort_unstable();
                (days, Some((t.start_ms, t.end_ms)))
            }
            None => {
                return Err(Status::invalid_argument(
                    "trading_day or time range is required",
                ))
            }
        };
        let meta = self
            .meta
            .read()
            .map_err(|_| Status::internal("metadata lock poisoned"))?;
        let multiplier = meta
            .multipliers
            .get(&product)
            .filter(|m| m.verified && m.volume_multiple.is_finite() && m.volume_multiple > 0.0)
            .map(|m| m.volume_multiple)
            .ok_or_else(|| {
                Status::failed_precondition(format!("no verified volume_multiple for {product}"))
            })?;
        let mut out_contract: Option<Contract> = None;
        let mut bars = Vec::new();
        for day in days {
            let path = dir.join(format!("{}.parquet", day.format("%Y%m%d")));
            if !path.exists() {
                continue;
            }
            let td = yyyymmdd(day);
            let want = canonical(contract, td);
            let rows = read_day_parquet(&path).map_err(|e| Status::internal(format!("{e:#}")))?;
            for r in rows
                .iter()
                .filter(|r| canonical(&r.contract_id, td) == want)
            {
                if out_contract.is_none() {
                    let (tick, source) = meta
                        .ticks
                        .lookup(venue, &r.contract_id, td)
                        .map_err(|e| Status::failed_precondition(format!("{e:#}")))?;
                    out_contract = Some(Contract {
                        exchange: exchange as i32,
                        product: product.clone(),
                        native_code: r.contract_id.clone(),
                        canonical_code: want.clone(),
                        volume_multiple: multiplier,
                        price_tick: tick,
                        price_tick_source: source.into(),
                    });
                }
                if r.volume <= 0.0 {
                    continue;
                }
                let open_ms = r.ts * 1000;
                if window.is_some_and(|(a, b)| open_ms < a || open_ms > b) {
                    continue;
                }
                let (Some(o), Some(h), Some(l), Some(c)) = (r.open, r.high, r.low, r.close) else {
                    return Err(Status::data_loss(format!(
                        "{} {} traded minute without OHLC",
                        r.contract_id, r.ts
                    )));
                };
                let tick = out_contract.as_ref().expect("set above").price_tick;
                let vwap = r.amount / r.volume / multiplier;
                let (hh, ll, widened) = widen(h, l, vwap, tick);
                bars.push(MinuteBar {
                    open_time_ms: open_ms,
                    trading_day: td,
                    open: o,
                    high: hh,
                    low: ll,
                    close: c,
                    snapshot_high: h,
                    snapshot_low: l,
                    volume_lots: r.volume,
                    turnover_cny: r.amount,
                    vwap,
                    snapshot_trades: r.count.max(0.0) as u32,
                    widened,
                    mid_price: finite(r.mid_price),
                    twap: finite(r.twap),
                    buy_volume_lots: r.buy_volume,
                    sell_volume_lots: r.sell_volume,
                    buy_turnover_cny: r.buy_amount,
                    sell_turnover_cny: r.sell_amount,
                    buy_vwap: finite(r.buy_vwap),
                    sell_vwap: finite(r.sell_vwap),
                    buy_snapshot_trades: r.buy_count,
                    sell_snapshot_trades: r.sell_count,
                    avg_interval_turnover_cny: r.avg_amount,
                    net_buy_turnover_cny: r.net_buy_amount,
                    net_buy_volume_lots: r.net_buy_volume,
                    net_buy_pct: finite(r.net_buy_pct),
                    size_buckets: Some(SizeBuckets {
                        large_turnover_cny: r.large_order,
                        medium_turnover_cny: r.medium_order,
                        small_turnover_cny: r.small_order,
                        large_buy_cny: r.large_buy,
                        large_sell_cny: r.large_sell,
                        medium_buy_cny: r.medium_buy,
                        medium_sell_cny: r.medium_sell,
                        small_buy_cny: r.small_buy,
                        small_sell_cny: r.small_sell,
                        net_buy_large_cny: r.net_buy_large,
                        net_buy_medium_cny: r.net_buy_medium,
                        net_buy_small_cny: r.net_buy_small,
                    }),
                    book: r.book.as_ref().map(book5),
                });
            }
        }
        bars.sort_by_key(|b| b.open_time_ms);
        bars.dedup_by_key(|b| b.open_time_ms);
        let contract = out_contract.ok_or_else(|| {
            Status::not_found(format!(
                "{venue} {contract}: no rows in the requested range"
            ))
        })?;
        Ok(GetMinuteBarsResponse {
            contract: Some(contract),
            bars,
            data_state: Some(self.data_state()),
        })
    }
}

struct Grpc(Arc<Service>);

fn finite(v: Option<f64>) -> Option<f64> {
    v.filter(|x| x.is_finite())
}

fn book5(b: &cn_futures_core::baseline_1min::Book5) -> cn_market_proto::Book5 {
    let side = |p: &[Option<f64>; 5], v: &[Option<f64>; 5]| {
        (0..5)
            .map(|i| BookLevel {
                price: finite(p[i]),
                volume_lots: finite(v[i]),
            })
            .collect()
    };
    cn_market_proto::Book5 {
        bids: side(&b.bid_prices, &b.bid_sizes),
        asks: side(&b.ask_prices, &b.ask_sizes),
    }
}

#[tonic::async_trait]
impl CnFuturesMarket for Grpc {
    async fn get_minute_bars(
        &self,
        request: Request<GetMinuteBarsRequest>,
    ) -> Result<Response<GetMinuteBarsResponse>, Status> {
        let svc = self.0.clone();
        let req = request.into_inner();
        tokio::task::spawn_blocking(move || svc.bars(req))
            .await
            .map_err(|e| Status::internal(e.to_string()))?
            .map(Response::new)
    }

    async fn get_data_state(
        &self,
        _request: Request<GetDataStateRequest>,
    ) -> Result<Response<DataState>, Status> {
        Ok(Response::new(self.0.data_state()))
    }
}

async fn load_metadata(cfg: &Config) -> Result<Metadata> {
    let multipliers = tokio::task::spawn_blocking(load_multiplier_catalog)
        .await?
        .context("load PostgreSQL multipliers")?;
    let ticks = load_ticks(&cfg.reference_api)
        .await
        .context("load price ticks")?;
    Ok(Metadata { multipliers, ticks })
}

#[tokio::main]
async fn main() -> Result<()> {
    let cli = Cli::parse();
    let cfg: Config = toml::from_str(&std::fs::read_to_string(&cli.config)?)
        .with_context(|| format!("parse {:?}", cli.config))?;
    let meta = Arc::new(RwLock::new(load_metadata(&cfg).await?));
    {
        let m = meta.read().unwrap();
        eprintln!(
            "cn-kline-server: {} multipliers, {} instrument ticks; listening on {}",
            m.multipliers.len(),
            m.ticks.by_instrument.len(),
            cfg.listen
        );
    }
    let refresh = {
        let (cfg, meta) = (cfg.clone(), meta.clone());
        tokio::spawn(async move {
            let mut every = tokio::time::interval(std::time::Duration::from_secs(
                cfg.metadata_refresh_s.max(60),
            ));
            every.tick().await;
            loop {
                every.tick().await;
                match load_metadata(&cfg).await {
                    Ok(m) => *meta.write().unwrap() = m,
                    Err(e) => eprintln!(
                        "cn-kline-server: metadata refresh failed, keeping previous: {e:#}"
                    ),
                }
            }
        })
    };
    let http = match (&cfg.http_listen, &cfg.roll_dir) {
        (Some(listen), Some(dir)) => {
            let store = Arc::new(dominant::DominantStore::new(dir.clone()));
            store.refresh().context("load dominant tables")?;
            let listener = tokio::net::TcpListener::bind(listen)
                .await
                .with_context(|| format!("bind {listen}"))?;
            eprintln!("cn-kline-server: dominant HTTP on {listen} from {dir:?}");
            Some(tokio::spawn(async move {
                if let Err(e) = axum::serve(listener, dominant::router(store)).await {
                    eprintln!("cn-kline-server: HTTP server stopped: {e}");
                }
            }))
        }
        (None, None) => None,
        _ => anyhow::bail!("http_listen and roll_dir must be set together"),
    };
    let addr = cfg.listen.parse().context("listen must be host:port")?;
    let svc = Grpc(Arc::new(Service { cfg, meta }));
    tonic::transport::Server::builder()
        .add_service(CnFuturesMarketServer::new(svc))
        .serve_with_shutdown(addr, async {
            let _ = tokio::signal::ctrl_c().await;
        })
        .await?;
    refresh.abort();
    if let Some(h) = http {
        h.abort();
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn widening_stays_on_tick_grid_and_contains_vwap() {
        // AU-style case: two prints at different prices, snapshot shows one.
        let (h, l, w) = widen(1002.04, 1002.04, 1002.03, 0.02);
        assert!(w && h == 1002.04 && l == 1002.02, "{h} {l}");
        // EG-style DCE combo-leg case: vwap 18 ticks above the snapshot high.
        let (h, l, w) = widen(5371.0, 5368.0, 5389.15, 1.0);
        assert!(w && h == 5390.0 && l == 5368.0);
        // Inside: untouched.
        assert_eq!(widen(3040.0, 3030.0, 3035.5, 1.0), (3040.0, 3030.0, false));
        // IF tick 0.2: result must be an exact multiple at 1 decimal.
        let (h, _, _) = widen(4865.0, 4864.0, 4865.07, 0.2);
        assert_eq!(h, 4865.2);
    }

    #[test]
    fn czce_codes_compare_equal() {
        assert_eq!(canonical("AP601", 20251120), "ap2601");
        assert_eq!(canonical("AP2601", 20251120), "ap2601");
        assert_eq!(canonical("rb2701", 20261113), "rb2701");
    }

    #[test]
    fn tick_lookup_prefers_instrument_then_unanimous_product() {
        let mut t = Ticks::default();
        t.insert("CZCE", "AP701", "AP", 1.0);
        t.insert("SHFE", "rb2701", "rb", 1.0);
        t.insert("SHFE", "rb2705", "rb", 1.0);
        t.insert("CFFEX", "T2612", "T", 0.005);
        t.insert("CFFEX", "T2703", "T", 0.01);
        assert_eq!(
            t.lookup("CZCE", "AP701", 20261113).unwrap(),
            (1.0, "instrument")
        );
        assert_eq!(
            t.lookup("SHFE", "rb2609", 20260826).unwrap(),
            (1.0, "product")
        );
        assert!(t.lookup("CFFEX", "T2609", 20260826).is_err());
        assert!(t.lookup("DCE", "i2701", 20261113).is_err());
    }
}
