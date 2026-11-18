//! CN futures market service (library; served by `cn-kline-server` and by
//! `market-replay-server`): gRPC `cn_market.v1.CnFuturesMarket` over the production
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
use cn_futures_l2::export_1min::read_day_parquet;
use cn_futures_l2::multipliers::{load_multiplier_catalog, ProductMultiplier};
use cn_market_proto::cn_futures_market_server::{CnFuturesMarket, CnFuturesMarketServer};
use cn_market_proto::{
    get_minute_bars_request::Range, BookLevel, Contract, DataState, Exchange, GetDataStateRequest,
    GetMinuteBarsRequest, GetMinuteBarsResponse, GetSizeThresholdsRequest,
    GetSizeThresholdsResponse, GetTradingSessionsRequest, GetTradingSessionsResponse, MinuteBar,
    ProductSessions, ProductThresholds, SizeBuckets,
};
use serde::Deserialize;
use std::collections::{BTreeSet, HashMap};
use std::path::{Path, PathBuf};
use std::sync::{Arc, RwLock};
use tonic::{Request, Response, Status};

pub mod dominant;
pub mod intraday;
pub mod live;
pub mod live_seg;
pub mod minute_cache;
pub mod replay_api;
pub mod selection;
pub mod size_thr;

/// A time-range request may span at most this many calendar days.
const MAX_RANGE_DAYS: i64 = 31;

#[derive(Debug, Clone, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct Config {
    pub listen: String,
    pub baseline_root: PathBuf,
    /// cn-daily state file (latest processed trading day).
    pub daily_state_file: PathBuf,
    /// Reference service base URL, e.g. http://127.0.0.1:8765
    pub reference_api: String,
    pub session_as_of: String,
    #[serde(default = "d_refresh")]
    pub metadata_refresh_s: u64,
    /// HTTP JSON endpoint for dominant-contract queries (not gRPC).
    #[serde(default)]
    pub http_listen: Option<String>,
    /// Directory with `<exchange>_dominant.csv` from the daily roll replay.
    #[serde(default)]
    pub roll_dir: Option<PathBuf>,
    #[serde(default)]
    pub intraday: Option<intraday::Config>,
    #[serde(default)]
    pub minute_cache: Option<minute_cache::Config>,
    /// In-progress day rebuilt from the NFS MDL files.
    #[serde(default)]
    pub live: Option<live::Config>,
    #[serde(default)]
    pub replay: Option<replay_api::Config>,
}

impl Config {
    pub fn load(path: &Path) -> Result<Self> {
        toml::from_str(&std::fs::read_to_string(path)?).with_context(|| format!("parse {path:?}"))
    }
}
fn d_refresh() -> u64 {
    3600
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

/// Upper-case product directories of an exchange in the archive, sorted.
fn products_in(dir: &Path) -> Vec<String> {
    let mut out: Vec<String> = std::fs::read_dir(dir)
        .into_iter()
        .flatten()
        .filter_map(|e| e.ok())
        .filter(|e| e.path().is_dir())
        .filter_map(|e| e.file_name().to_str().map(str::to_ascii_uppercase))
        .filter(|n| !n.starts_with('_'))
        .collect();
    out.sort();
    out
}

/// The requested product (empty = all) out of the exchange's products;
/// an unknown product is `NOT_FOUND`.
fn select_products(all: Vec<String>, product: &str) -> Result<Vec<String>, Status> {
    let product = product.trim();
    if product.is_empty() {
        return Ok(all);
    }
    let wanted = product.to_ascii_uppercase();
    if all.contains(&wanted) {
        Ok(vec![wanted])
    } else {
        Err(Status::not_found(format!(
            "no product {wanted} on this exchange"
        )))
    }
}

fn product_of(code: &str) -> String {
    code.chars()
        .take_while(|c| c.is_ascii_alphabetic())
        .collect::<String>()
        .to_ascii_uppercase()
}

/// Lower-case; CZCE `YMM` widened to `YYMM` using the trading day's decade.
pub fn canonical(code: &str, trading_day: u32) -> String {
    cn_futures_l2::dominant_table::canonical(code, trading_day)
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

fn minute_prices(
    open: Option<f64>,
    high: Option<f64>,
    low: Option<f64>,
    close: Option<f64>,
    volume: f64,
) -> Result<(f64, f64, f64, f64), Status> {
    if volume > 0.0
        && [open, high, low, close]
            .iter()
            .any(|v| v.is_none_or(|v| !v.is_finite()))
    {
        return Err(Status::data_loss(
            "traded baseline minute without finite OHLC",
        ));
    }
    Ok((
        open.unwrap_or(f64::NAN),
        high.unwrap_or(f64::NAN),
        low.unwrap_or(f64::NAN),
        close.unwrap_or(f64::NAN),
    ))
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

fn last_done(state_file: &Path) -> u32 {
    std::fs::read(state_file)
        .ok()
        .and_then(|b| serde_json::from_slice::<serde_json::Value>(&b).ok())
        .and_then(|v| v["last_done"].as_str().map(str::to_string))
        .and_then(|s| NaiveDate::parse_from_str(&s, "%Y-%m-%d").ok())
        .map_or(0, yyyymmdd)
}

struct Service {
    selection_slots: Arc<tokio::sync::Semaphore>,
    replay: Option<replay_api::ReplayService>,
    intraday: Option<Arc<intraday::Cache>>,
    minute_cache: Option<minute_cache::Reader>,
    live: Option<Arc<live::Live>>,
    cfg: Config,
    meta: Arc<RwLock<Metadata>>,
}

impl Drop for Service {
    fn drop(&mut self) {
        if let Some(cache) = &self.intraday {
            cache.stop();
        }
    }
}

impl Service {
    fn latest_trading_day(&self) -> u32 {
        last_done(&self.cfg.daily_state_file)
    }

    fn data_state(&self) -> DataState {
        DataState {
            source: "baseline_data_1min".into(),
            latest_trading_day: self.latest_trading_day(),
            session_snapshot_as_of: self.cfg.session_as_of.clone(),
            live_trading_day: 0,
            live_through_ms: 0,
        }
    }

    /// `DataState`; with an exchange, its in-progress day and how far NFS has
    /// been read (both 0 when no day is open or the exchange is unset).
    fn data_state_of(&self, exchange: Exchange) -> DataState {
        let (live_trading_day, live_through_ms) = match (archive_exchange(exchange), &self.live) {
            (Some((dir, _)), Some(live)) => live.exchange_progress(dir),
            _ => (0, 0),
        };
        DataState {
            live_trading_day,
            live_through_ms,
            ..self.data_state()
        }
    }

    /// Products of an exchange as listed in the archive (upper-case).
    fn exchange_products(&self, dir_name: &str) -> Vec<String> {
        products_in(&self.cfg.baseline_root.join(dir_name))
    }

    fn size_thresholds(
        &self,
        req: GetSizeThresholdsRequest,
    ) -> Result<GetSizeThresholdsResponse, Status> {
        let exchange = Exchange::try_from(req.exchange).unwrap_or(Exchange::Unspecified);
        let (dir_name, _) =
            archive_exchange(exchange).ok_or_else(|| Status::not_found("unknown exchange"))?;
        let products = select_products(self.exchange_products(dir_name), &req.product)?;
        let now_ms = Utc::now().timestamp_millis();
        Ok(GetSizeThresholdsResponse {
            products: products
                .into_iter()
                .map(|product| ProductThresholds {
                    thresholds: self
                        .live
                        .as_ref()
                        .map(|l| l.size_thresholds(&product, now_ms))
                        .unwrap_or_default(),
                    product,
                })
                .collect(),
        })
    }

    fn trading_sessions(
        &self,
        req: GetTradingSessionsRequest,
    ) -> Result<GetTradingSessionsResponse, Status> {
        let exchange = Exchange::try_from(req.exchange).unwrap_or(Exchange::Unspecified);
        let (dir_name, _) =
            archive_exchange(exchange).ok_or_else(|| Status::not_found("unknown exchange"))?;
        let live = self
            .live
            .as_ref()
            .ok_or_else(|| Status::unavailable("trading sessions are not loaded (no [live])"))?;
        let products = select_products(self.exchange_products(dir_name), &req.product)?;
        Ok(GetTradingSessionsResponse {
            snapshot_as_of: self.cfg.session_as_of.clone(),
            products: products
                .into_iter()
                .map(|product| ProductSessions {
                    windows: live.session_windows(dir_name, &product),
                    product,
                })
                .collect(),
        })
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
        let req_day = match &req.range {
            Some(Range::TradingDay(d)) => Some(*d),
            _ => None,
        };
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
                    out_contract = Some(make_contract(
                        &meta,
                        exchange,
                        venue,
                        &product,
                        &r.contract_id,
                        td,
                        &want,
                        multiplier,
                    )?);
                }
                let tick = out_contract.as_ref().expect("set above").price_tick;
                if let Some(bar) = make_bar(
                    r,
                    td,
                    multiplier,
                    tick,
                    false,
                    req.include_empty_minutes,
                    window,
                    false,
                )? {
                    bars.push(bar);
                }
            }
        }
        // In-progress day: rebuilt from the NFS MDL files (see `live`).
        let mut live_state = (0u32, 0i64);
        let mut unavailable = None;
        if let Some(live) = self.live.as_ref().filter(|l| l.serves(&product)) {
            let last_done = self.latest_trading_day();
            let want_of = |id: &str, td: u32| canonical(id, td) == canonical(contract, td);
            match live.bars(dir_name, &product, last_done, want_of, multiplier) {
                Ok(Some(found)) => {
                    for lb in found {
                        let td = yyyymmdd(lb.day);
                        if req_day.is_some_and(|d| d != td) {
                            continue;
                        }
                        live_state = (td, lb.through_ms);
                        if out_contract.is_none() {
                            let want = canonical(contract, td);
                            out_contract = Some(make_contract(
                                &meta,
                                exchange,
                                venue,
                                &product,
                                &lb.native_code,
                                td,
                                &want,
                                multiplier,
                            )?);
                        }
                        let tick = out_contract.as_ref().expect("set above").price_tick;
                        let mut live_bars = Vec::new();
                        for r in &lb.minutes {
                            if let Some(bar) = make_bar(
                                r,
                                td,
                                multiplier,
                                tick,
                                true,
                                true,
                                None,
                                live.has_size_thresholds(&product, r.ts),
                            )? {
                                live_bars.push(bar);
                            }
                        }
                        if let Some(cache) = &self.minute_cache {
                            match cache.bars(td, exchange, &lb.native_code, &self.cfg.session_as_of)
                            {
                                Ok(cached) if !cached.is_empty() => {
                                    let check =
                                        minute_cache::compare(&live_bars, &cached, lb.through_ms);
                                    if check.first_difference_ms.is_some() {
                                        eprintln!(
                                            "{}",
                                            serde_json::json!({
                                                "event": "minute_cache_comparison", "exchange": venue,
                                                "contract": lb.native_code, "trading_day": td,
                                                "matched": check.matched, "different": check.different,
                                                "cache_only": check.cache_only, "mdl_only": check.mdl_only,
                                                "first_difference_ms": check.first_difference_ms,
                                            })
                                        );
                                    }
                                    live_state.1 = minute_cache::extend(
                                        &mut live_bars,
                                        cached,
                                        lb.through_ms,
                                        |ms| live.in_session(dir_name, &product, ms / 1000),
                                    );
                                }
                                Ok(_) => {}
                                Err(error) => eprintln!(
                                    "cn-market: minute cache {} unavailable: {error:#}",
                                    lb.native_code
                                ),
                            }
                        }
                        bars.extend(live_bars.into_iter().filter(|bar| {
                            (req.include_empty_minutes || bar.volume_lots > 0.)
                                && window.is_none_or(|(a, b)| (a..=b).contains(&bar.open_time_ms))
                        }));
                    }
                }
                Ok(None) => {}
                Err(e) => unavailable = Some(e),
            }
            if live_state.0 == 0 {
                if let Some(cache) = &self.minute_cache {
                    match cache.current(exchange, contract, &self.cfg.session_as_of) {
                        Ok(Some((td, native, cached)))
                            if td > last_done && req_day.is_none_or(|d| d == td) =>
                        {
                            if let Some((through, minutes)) = minute_cache::from_open(
                                cached,
                                |ms| live.first_session_minute(dir_name, &product, ms / 1000),
                                |ms| live.in_session(dir_name, &product, ms / 1000),
                            ) {
                                if out_contract.is_none() {
                                    out_contract = Some(make_contract(
                                        &meta,
                                        exchange,
                                        venue,
                                        &product,
                                        &native,
                                        td,
                                        &canonical(contract, td),
                                        multiplier,
                                    )?);
                                }
                                live_state = (td, through);
                                bars.extend(minutes.into_iter().filter(|bar| {
                                    (req.include_empty_minutes || bar.volume_lots > 0.)
                                        && window.is_none_or(|(a, b)| {
                                            (a..=b).contains(&bar.open_time_ms)
                                        })
                                }));
                            }
                        }
                        Ok(_) => {}
                        Err(error) => {
                            eprintln!("cn-market: minute cache {contract} unavailable: {error:#}")
                        }
                    }
                }
            }
            if let Some(error) = unavailable {
                if live_state.0 == 0 && req_day.is_some_and(|d| d > last_done) {
                    return Err(Status::unavailable(format!(
                        "in-progress day unavailable: {error:#}"
                    )));
                }
                eprintln!("cn-market: live day failed: {error:#}");
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
            data_state: Some(DataState {
                live_trading_day: live_state.0,
                live_through_ms: live_state.1,
                ..self.data_state()
            }),
        })
    }
}

pub struct Grpc(Arc<Service>);

#[allow(clippy::too_many_arguments)]
fn make_contract(
    meta: &Metadata,
    exchange: Exchange,
    venue: &str,
    product: &str,
    native: &str,
    td: u32,
    canonical_code: &str,
    multiplier: f64,
) -> Result<Contract, Status> {
    let (tick, source) = meta
        .ticks
        .lookup(venue, native, td)
        .map_err(|e| Status::failed_precondition(format!("{e:#}")))?;
    Ok(Contract {
        exchange: exchange as i32,
        product: product.to_string(),
        native_code: native.to_string(),
        canonical_code: canonical_code.to_string(),
        volume_multiple: multiplier,
        price_tick: tick,
        price_tick_source: source.into(),
    })
}

/// The `size_buckets` of one minute. Archive minutes always carry their columns
/// (from the file). A live minute carries them only when the product's
/// thresholds exist and the minute is classified: an empty minute, or one whose
/// trades were classed (`large + medium + small > 0`); otherwise unset.
fn size_buckets_for(
    r: &cn_futures_core::baseline_1min::BaselineMinute,
    provisional: bool,
    thresholds: bool,
) -> Option<SizeBuckets> {
    if provisional {
        if !thresholds {
            return None;
        }
        let classed = r.large_order + r.medium_order + r.small_order;
        if r.volume > 0.0 && (classed <= 0.0 || !classed.is_finite()) {
            return None;
        }
    }
    Some(SizeBuckets {
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
    })
}

/// One archive/live minute row -> proto bar. `None` when the minute is
/// filtered (empty minute not requested, or outside the time window).
/// `thresholds`: the product's size thresholds exist (live bars only).
#[allow(clippy::too_many_arguments)]
fn make_bar(
    r: &cn_futures_core::baseline_1min::BaselineMinute,
    td: u32,
    multiplier: f64,
    tick: f64,
    provisional: bool,
    include_empty: bool,
    window: Option<(i64, i64)>,
    thresholds: bool,
) -> Result<Option<MinuteBar>, Status> {
    if r.volume <= 0.0 && !include_empty {
        return Ok(None);
    }
    let open_ms = r.ts * 1000;
    if window.is_some_and(|(a, b)| open_ms < a || open_ms > b) {
        return Ok(None);
    }
    let (o, h, l, c) = minute_prices(r.open, r.high, r.low, r.close, r.volume)?;
    let vwap = if r.volume > 0.0 {
        r.amount / r.volume / multiplier
    } else {
        r.vwap.unwrap_or(f64::NAN)
    };
    let (hh, ll, widened) = if r.volume > 0.0 {
        widen(h, l, vwap, tick)
    } else {
        (h, l, false)
    };
    Ok(Some(MinuteBar {
        provisional,
        size_buckets: size_buckets_for(r, provisional, thresholds),
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
        book: r.book.as_ref().map(book5),
    }))
}

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
    type StreamReplayFramesStream =
        tokio_stream::wrappers::ReceiverStream<Result<cn_market_proto::ReplayFrameBatch, Status>>;
    async fn stream_replay_frames(
        &self,
        request: Request<cn_market_proto::GetReplayFramesRequest>,
    ) -> Result<Response<Self::StreamReplayFramesStream>, Status> {
        self.0
            .replay
            .as_ref()
            .ok_or_else(|| Status::unavailable("historical replay disabled"))?
            .stream(request.into_inner())
            .map(Response::new)
    }
    async fn get_contract_selections(
        &self,
        request: Request<cn_market_proto::GetContractSelectionsRequest>,
    ) -> Result<Response<cn_market_proto::GetContractSelectionsResponse>, Status> {
        let req = request.into_inner();
        selection::validate(&req).map_err(|e| Status::invalid_argument(format!("{e:#}")))?;
        let root = self
            .0
            .cfg
            .roll_dir
            .clone()
            .ok_or_else(|| Status::unavailable("contract selection disabled"))?;
        let permit = self
            .0
            .selection_slots
            .clone()
            .try_acquire_owned()
            .map_err(|_| Status::resource_exhausted("contract selection busy; retry"))?;
        tokio::task::spawn_blocking(move || {
            let _permit = permit;
            selection::query(&root, req)
        })
        .await
        .map_err(|e| Status::internal(e.to_string()))?
        .map(Response::new)
        .map_err(|e| Status::failed_precondition(format!("{e:#}")))
    }
    async fn get_product_multipliers(
        &self,
        request: Request<cn_market_proto::GetProductMultipliersRequest>,
    ) -> Result<Response<cn_market_proto::GetProductMultipliersResponse>, Status> {
        let wanted: BTreeSet<_> = request
            .into_inner()
            .products
            .into_iter()
            .map(|p| p.to_ascii_uppercase())
            .collect();
        let meta = self.0.meta.read().unwrap();
        let mut products = Vec::new();
        for (p, m) in &meta.multipliers {
            if !wanted.is_empty() && !wanted.contains(p) {
                continue;
            }
            if !m.verified || !m.volume_multiple.is_finite() || m.volume_multiple <= 0.0 {
                if wanted.contains(p) {
                    return Err(Status::failed_precondition(format!(
                        "unverified multiplier {p}"
                    )));
                }
                continue;
            }
            products.push(cn_market_proto::VerifiedProductMultiplier {
                product: p.clone(),
                exchange: selection::exchange(&m.exchange)
                    .map_err(|e| Status::internal(e.to_string()))? as i32,
                volume_multiple: m.volume_multiple,
            });
        }
        if !wanted.is_empty() && products.len() != wanted.len() {
            return Err(Status::not_found("missing requested multiplier"));
        }
        products.sort_by(|a, b| a.product.cmp(&b.product));
        Ok(Response::new(
            cn_market_proto::GetProductMultipliersResponse {
                products,
                source: "market_metadata.public.domestic_future_product_multipliers;verified=true"
                    .into(),
            },
        ))
    }
    type StreamIntradayFramesStream =
        tokio_stream::wrappers::ReceiverStream<Result<cn_market_proto::IntradayFrameBatch, Status>>;
    async fn stream_intraday_frames(
        &self,
        request: Request<cn_market_proto::GetIntradayFramesRequest>,
    ) -> Result<Response<Self::StreamIntradayFramesStream>, Status> {
        let cache = self
            .0
            .intraday
            .clone()
            .ok_or_else(|| Status::unavailable("intraday cache disabled"))?;
        cache.stream(request.into_inner()).await.map(Response::new)
    }

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
        request: Request<GetDataStateRequest>,
    ) -> Result<Response<DataState>, Status> {
        let exchange =
            Exchange::try_from(request.into_inner().exchange).unwrap_or(Exchange::Unspecified);
        Ok(Response::new(self.0.data_state_of(exchange)))
    }

    async fn get_size_thresholds(
        &self,
        request: Request<GetSizeThresholdsRequest>,
    ) -> Result<Response<GetSizeThresholdsResponse>, Status> {
        let svc = self.0.clone();
        let req = request.into_inner();
        tokio::task::spawn_blocking(move || svc.size_thresholds(req))
            .await
            .map_err(|e| Status::internal(e.to_string()))?
            .map(Response::new)
    }

    async fn get_trading_sessions(
        &self,
        request: Request<GetTradingSessionsRequest>,
    ) -> Result<Response<GetTradingSessionsResponse>, Status> {
        let svc = self.0.clone();
        let req = request.into_inner();
        tokio::task::spawn_blocking(move || svc.trading_sessions(req))
            .await
            .map_err(|e| Status::internal(e.to_string()))?
            .map(Response::new)
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

/// The running pieces of the service: the gRPC service, the optional HTTP
/// router (dominant contracts) and the metadata refresh task.
pub struct Components {
    pub grpc: CnFuturesMarketServer<Grpc>,
    pub http: Option<axum::Router>,
    pub refresh: tokio::task::JoinHandle<()>,
}

/// Waits (quietly, 0.5 s polls) until the reference service answers its health
/// endpoint. Under `market-replay-server` that service is a sibling child that
/// starts a few seconds after this process, so the first metadata load would
/// otherwise fail with `Connection refused` and cost a 5 s retry. Returns
/// whether it became ready; callers continue either way and let the metadata
/// load report the real error.
pub async fn wait_reference_api(base: &str, max_wait: std::time::Duration) -> bool {
    let url = format!("{}/api/v1/health", base.trim_end_matches('/'));
    let Ok(client) = reqwest::Client::builder()
        .timeout(std::time::Duration::from_secs(2))
        .build()
    else {
        return false;
    };
    let start = std::time::Instant::now();
    loop {
        if client
            .get(&url)
            .send()
            .await
            .is_ok_and(|r| r.status().is_success())
        {
            return true;
        }
        if start.elapsed() >= max_wait {
            eprintln!("cn-market: reference API {url} not ready after {max_wait:?}");
            return false;
        }
        tokio::time::sleep(std::time::Duration::from_millis(500)).await;
    }
}

pub async fn components(cfg: Config) -> Result<Components> {
    wait_reference_api(&cfg.reference_api, std::time::Duration::from_secs(120)).await;
    let meta = Arc::new(RwLock::new(load_metadata(&cfg).await?));
    {
        let m = meta.read().unwrap();
        eprintln!(
            "cn-market: {} multipliers, {} instrument ticks",
            m.multipliers.len(),
            m.ticks.by_instrument.len(),
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
                    Err(e) => {
                        eprintln!("cn-market: metadata refresh failed, keeping previous: {e:#}")
                    }
                }
            }
        })
    };
    let http = match &cfg.roll_dir {
        Some(dir) => {
            let store = Arc::new(dominant::DominantStore::new(dir.clone()));
            store.refresh().context("load dominant tables")?;
            Some(dominant::router(store))
        }
        None => None,
    };
    let intraday = match cfg.intraday.clone() {
        Some(config) => Some(intraday::Cache::start(config).await?),
        None => None,
    };
    let live = match cfg.live.clone() {
        Some(live_cfg) => {
            let (api, as_of) = (cfg.reference_api.clone(), cfg.session_as_of.clone());
            let sessions = tokio::task::spawn_blocking(move || {
                cn_futures_trim::sessions::ContinuousSessions::fetch(
                    &format!("{}/api/v1/trading-sessions", api.trim_end_matches('/')),
                    Some(&as_of),
                )
            })
            .await?
            .context("load continuous sessions for the live day")?;
            let period = live_cfg.background_s;
            let live = Arc::new(live::Live::new(live_cfg, Arc::new(sessions)));
            if period > 0 {
                let (live, state_file) = (live.clone(), cfg.daily_state_file.clone());
                let meta = meta.clone();
                tokio::spawn(async move {
                    loop {
                        let (l, f, m) = (live.clone(), state_file.clone(), meta.clone());
                        match tokio::task::spawn_blocking(move || {
                            let multiplier = |product: &str| {
                                m.read().ok().and_then(|g| {
                                    g.multipliers
                                        .get(product)
                                        .filter(|x| {
                                            x.verified
                                                && x.volume_multiple.is_finite()
                                                && x.volume_multiple > 0.0
                                        })
                                        .map(|x| x.volume_multiple)
                                })
                            };
                            l.refresh_all(
                                last_done(&f),
                                &live::Finalizer {
                                    multiplier: &multiplier,
                                },
                            )
                        })
                        .await
                        {
                            Ok(errors) => errors
                                .iter()
                                .for_each(|e| eprintln!("cn-market: live refresh failed: {e}")),
                            Err(e) => eprintln!("cn-market: live refresh task: {e}"),
                        }
                        tokio::time::sleep(std::time::Duration::from_secs(period)).await;
                    }
                });
            }
            Some(live)
        }
        None => None,
    };
    let minute_cache = cfg
        .minute_cache
        .clone()
        .map(minute_cache::Reader::new)
        .transpose()?;
    let grpc = CnFuturesMarketServer::new(Grpc(Arc::new(Service {
        selection_slots: Arc::new(tokio::sync::Semaphore::new(2)),
        replay: cfg.replay.clone().map(replay_api::ReplayService::new),
        live,
        cfg,
        meta,
        intraday,
        minute_cache,
    })));
    Ok(Components {
        grpc,
        http,
        refresh,
    })
}

/// Standalone serving (`cn-kline-server`): gRPC on `listen`, the dominant
/// HTTP router on `http_listen`.
pub async fn serve(cfg: Config) -> Result<()> {
    let addr = cfg.listen.parse().context("listen must be host:port")?;
    let http_listen = cfg.http_listen.clone();
    ensure!(
        http_listen.is_some() == cfg.roll_dir.is_some(),
        "http_listen and roll_dir must be set together"
    );
    let c = components(cfg).await?;
    let http = match (http_listen, c.http) {
        (Some(listen), Some(router)) => {
            let listener = tokio::net::TcpListener::bind(&listen)
                .await
                .with_context(|| format!("bind {listen}"))?;
            eprintln!("cn-kline-server: dominant HTTP on {listen}");
            Some(tokio::spawn(async move {
                if let Err(e) = axum::serve(listener, router).await {
                    eprintln!("cn-kline-server: HTTP server stopped: {e}");
                }
            }))
        }
        _ => None,
    };
    eprintln!("cn-kline-server: gRPC on {addr}");
    tonic::transport::Server::builder()
        .add_service(c.grpc)
        .serve_with_shutdown(addr, async {
            let _ = tokio::signal::ctrl_c().await;
        })
        .await?;
    c.refresh.abort();
    if let Some(h) = http {
        h.abort();
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    fn minute(
        volume: f64,
        large: f64,
        medium: f64,
        small: f64,
    ) -> cn_futures_core::baseline_1min::BaselineMinute {
        let trade = cn_futures_core::baseline_1min::PrintTrade {
            ts_utc_ns: 1_794_880_800 * 1_000_000_000,
            price: 1.0,
            volume: volume.max(1.0),
            amount: 1.0,
            aggressor: 1,
        };
        let mut row =
            cn_futures_core::baseline_1min::synthesize_minutes("sn2701", &[trade], &[], 1.0)
                .unwrap()
                .remove(0);
        row.volume = volume;
        (row.large_order, row.medium_order, row.small_order) = (large, medium, small);
        row
    }

    const SESSIONS_JSON: &str = r#"{"dataState":{"as_of":"2026-11-15","latest_start_ts":"x",
        "latest_update_ts":"x","session_count":3,"product_count":2,"venue_count":1,"version_count":3},
        "timezone":"Asia/Shanghai","sessions":[
        {"venue":"SHFE","product_id":"SN","segment_no":2,"time_begin":"09:00:00","time_end":"10:15:00","crosses_midnight":false,"is_active":true},
        {"venue":"SHFE","product_id":"SN","segment_no":1,"time_begin":"21:00:00","time_end":"01:00:00","crosses_midnight":true,"is_active":true},
        {"venue":"SHFE","product_id":"CU","segment_no":1,"time_begin":"09:00:00","time_end":"10:15:00","crosses_midnight":false,"is_active":true}]}"#;

    /// A service over a temp archive with products SN and CU on SHFE (and an
    /// ignored `_provenance` directory); `live` adds the live config with the
    /// given threshold directory.
    fn test_service(live: bool) -> (tempfile::TempDir, tempfile::TempDir, Service) {
        let archive = tempfile::tempdir().unwrap();
        for p in ["xsge/SN", "xsge/CU", "xsge/_tmp", "_provenance"] {
            std::fs::create_dir_all(archive.path().join(p)).unwrap();
        }
        let thresholds = tempfile::tempdir().unwrap();
        let cfg: Config = toml::from_str(&format!(
            "listen=\"x\"\nbaseline_root=\"{}\"\ndaily_state_file=\"/none\"\nreference_api=\"http://x\"\nsession_as_of=\"2026-11-15\"\n",
            archive.path().display()
        ))
        .unwrap();
        let live = live.then(|| {
            let live_cfg: live::Config = toml::from_str(&format!(
                "msg_root=\"/none\"\nproducts=[\"*\"]\nsize_threshold_dir=\"{}\"\n",
                thresholds.path().display()
            ))
            .unwrap();
            let sessions =
                cn_futures_trim::sessions::ContinuousSessions::parse(SESSIONS_JSON).unwrap();
            Arc::new(live::Live::new(live_cfg, Arc::new(sessions)))
        });
        let meta = Arc::new(RwLock::new(Metadata {
            multipliers: HashMap::new(),
            ticks: Ticks::default(),
        }));
        (
            archive,
            thresholds,
            Service {
                selection_slots: Arc::new(tokio::sync::Semaphore::new(2)),
                replay: None,
                intraday: None,
                minute_cache: None,
                live,
                cfg,
                meta,
            },
        )
    }

    fn write_prior_month_thresholds(dir: &Path, product: &str, p50: f64, p90: f64) -> (u32, u32) {
        let now = Utc::now().timestamp_millis();
        let applies = cn_futures_l2::size_thresholds::month_of_ns(now as u64 * 1_000_000);
        let source = cn_futures_l2::size_thresholds::prior_month(applies);
        cn_futures_l2::size_thresholds::write(
            dir,
            product,
            &cn_futures_l2::size_thresholds::Thr {
                source_month: source,
                p50,
                p90,
                samples: 7,
                days: 2,
            },
            &[],
        )
        .unwrap();
        (applies, source)
    }

    #[test]
    fn size_thresholds_rpc_lists_products_and_leaves_missing_ones_empty() {
        let (_a, thr, svc) = test_service(true);
        let (applies, _) = write_prior_month_thresholds(thr.path(), "SN", 100.0, 900.0);
        let req = |product: &str| GetSizeThresholdsRequest {
            exchange: Exchange::Shfe as i32,
            product: product.into(),
        };
        // Empty product = every product of the exchange (sorted, `_` dirs ignored).
        let all = svc.size_thresholds(req("")).unwrap().products;
        assert_eq!(
            all.iter().map(|p| p.product.as_str()).collect::<Vec<_>>(),
            vec!["CU", "SN"]
        );
        assert!(
            all[0].thresholds.is_empty(),
            "CU has no thresholds: empty list"
        );
        assert_eq!(all[1].thresholds.len(), 1);
        let t = &all[1].thresholds[0];
        assert_eq!(
            (t.applies_month, t.p50_turnover_cny, t.p90_turnover_cny),
            (applies, 100.0, 900.0)
        );
        // One product, case-insensitive.
        let one = svc.size_thresholds(req("sn")).unwrap().products;
        assert_eq!(one.len(), 1);
        assert_eq!(one[0].product, "SN");
        // Unknown product / unknown or unspecified exchange: NOT_FOUND.
        assert_eq!(
            svc.size_thresholds(req("ZZ")).unwrap_err().code(),
            tonic::Code::NotFound
        );
        let none = GetSizeThresholdsRequest {
            exchange: 0,
            product: String::new(),
        };
        assert_eq!(
            svc.size_thresholds(none).unwrap_err().code(),
            tonic::Code::NotFound
        );
        // Without a [live] section there are no thresholds at all, still a valid answer.
        let (_a2, _t2, bare) = test_service(false);
        let r = bare.size_thresholds(req("SN")).unwrap().products;
        assert!(r[0].thresholds.is_empty());
    }

    #[test]
    fn trading_sessions_rpc_returns_windows_in_segment_order() {
        let (_a, _t, svc) = test_service(true);
        let req = |product: &str| GetTradingSessionsRequest {
            exchange: Exchange::Shfe as i32,
            product: product.into(),
        };
        let all = svc.trading_sessions(req("")).unwrap();
        assert_eq!(all.snapshot_as_of, "2026-11-15");
        assert_eq!(all.products.len(), 2);
        let sn = all.products.iter().find(|p| p.product == "SN").unwrap();
        let pairs: Vec<(u32, u32)> = sn
            .windows
            .iter()
            .map(|w| (w.begin_sec, w.end_sec))
            .collect();
        // Segment 1 is the night window (end <= begin: past midnight), then segment 2.
        assert_eq!(pairs, vec![(21 * 3600, 3600), (9 * 3600, 10 * 3600 + 900)]);
        assert_eq!(svc.trading_sessions(req("cu")).unwrap().products.len(), 1);
        assert_eq!(
            svc.trading_sessions(req("ZZ")).unwrap_err().code(),
            tonic::Code::NotFound
        );
        let (_a2, _t2, bare) = test_service(false);
        assert_eq!(
            bare.trading_sessions(req("SN")).unwrap_err().code(),
            tonic::Code::Unavailable
        );
    }

    #[test]
    fn data_state_describes_the_requested_exchange_only() {
        let (_a, _t, svc) = test_service(true);
        // No live day open: zeros, with or without an exchange.
        let d = svc.data_state_of(Exchange::Shfe);
        assert_eq!((d.live_trading_day, d.live_through_ms), (0, 0));
        assert_eq!(svc.data_state_of(Exchange::Unspecified).live_trading_day, 0);
        assert_eq!(d.session_snapshot_as_of, "2026-11-15");
        // An open day on SHFE shows for SHFE only.
        svc.live
            .as_ref()
            .unwrap()
            .set_progress_for_tests("xsge", 20261117, 1_794_890_000_000);
        let shfe = svc.data_state_of(Exchange::Shfe);
        assert_eq!(
            (shfe.live_trading_day, shfe.live_through_ms),
            (20261117, 1_794_890_000_000)
        );
        assert_eq!(svc.data_state_of(Exchange::Dce).live_trading_day, 0);
        assert_eq!(svc.data_state_of(Exchange::Unspecified).live_through_ms, 0);
    }

    #[test]
    fn live_minutes_carry_size_buckets_only_when_thresholds_exist_and_are_classified() {
        // No thresholds: unset, whatever the minute holds.
        assert!(size_buckets_for(&minute(5.0, 0.0, 9.0, 0.0), true, false).is_none());
        // Thresholds + classified trades: set.
        let b = size_buckets_for(&minute(5.0, 0.0, 9.0, 0.0), true, true).unwrap();
        assert_eq!(b.medium_turnover_cny, 9.0);
        // Thresholds but a traded minute with nothing classified (e.g. segment
        // persisted before the files existed): unset rather than fake zeros.
        assert!(size_buckets_for(&minute(5.0, 0.0, 0.0, 0.0), true, true).is_none());
        // An empty minute is a genuine zero once thresholds exist.
        let empty = size_buckets_for(&minute(0.0, 0.0, 0.0, 0.0), true, true).unwrap();
        assert_eq!(
            empty.large_turnover_cny + empty.medium_turnover_cny + empty.small_turnover_cny,
            0.0
        );
    }

    #[test]
    fn archive_minutes_always_keep_the_columns_of_their_file() {
        let b = size_buckets_for(&minute(5.0, 1.0, 2.0, 3.0), false, false).unwrap();
        assert_eq!(
            (
                b.large_turnover_cny,
                b.medium_turnover_cny,
                b.small_turnover_cny
            ),
            (1.0, 2.0, 3.0)
        );
    }

    #[test]
    fn requested_empty_minutes_preserve_carried_or_missing_prices() {
        assert_eq!(
            minute_prices(Some(100.), Some(101.), Some(99.), Some(100.), 0.).unwrap(),
            (100., 101., 99., 100.)
        );
        let missing = minute_prices(None, None, None, None, 0.).unwrap();
        assert!(
            missing.0.is_nan() && missing.1.is_nan() && missing.2.is_nan() && missing.3.is_nan()
        );
        assert!(minute_prices(None, Some(101.), Some(99.), Some(100.), 1.).is_err());
    }

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
