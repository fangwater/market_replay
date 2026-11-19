//! Completed ZMQ baseline minutes written by cn-minute-cache, read by gRPC.
use crate::Config as MarketConfig;
use anyhow::{ensure, Context, Result};
use chrono::{NaiveDate, Utc};
use cn_futures_core::{
    baseline_1min::{self, BaselineMinute, Book5, PrintTrade},
    events::{process_instrument_resumable, InstrumentCarry},
    mdl::{self, MdlQuote},
    segments::{is_session_break, trad_day_from_ts},
    session::Exchange as Venue,
    universe::product_id,
};
use cn_futures_trim::sessions::{reference_venue, ContinuousSessions, SessionMatcher};
use cn_market_proto::{Exchange, MinuteBar};
use prost::Message;
use rocksdb::{Direction, IteratorMode, Options, WriteBatch, WriteOptions, DB};
use serde::Deserialize;
use std::{
    collections::HashMap,
    path::{Path, PathBuf},
    sync::{
        atomic::{AtomicBool, Ordering},
        Arc, Mutex,
    },
    time::{Duration, Instant},
};

const MINUTE_MS: i64 = 60_000;
const NS: i64 = 1_000_000_000;
const DAY_KEY: &[u8] = b"native-trading-day";
const SETTINGS_KEY: &[u8] = b"session-snapshot";
const BAR_KEY: u8 = b'b';
const PUBLISHED_DAY: &str = "cache-day";
const MAX_QUOTES_PER_MINUTE: usize = 2048;

#[derive(Clone, Debug, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct Config {
    pub directory: PathBuf,
    /// Scratch directory used only by Market Replay's secondary instance.
    pub secondary_directory: PathBuf,
    pub zmq_endpoint: String,
}

impl Config {
    fn validate(&self) -> Result<()> {
        ensure!(
            self.directory.is_absolute()
                && self.secondary_directory.is_absolute()
                && !self.secondary_directory.starts_with(&self.directory),
            "minute cache and its secondary require separate absolute paths"
        );
        ensure!(
            self.zmq_endpoint.starts_with("tcp://"),
            "expected a ZMQ TCP endpoint"
        );
        Ok(())
    }
}

#[derive(Clone, PartialEq, Message)]
pub(crate) struct CachedMinute {
    #[prost(int64, tag = "1")]
    generation: i64,
    #[prost(message, optional, tag = "2")]
    bar: Option<MinuteBar>,
}

fn options() -> Options {
    let mut opts = Options::default();
    opts.set_max_background_jobs(2);
    opts.set_compression_type(rocksdb::DBCompressionType::Lz4);
    opts
}

fn day_key(day: u32) -> [u8; 5] {
    let mut key = [BAR_KEY; 5];
    key[1..].copy_from_slice(&day.to_be_bytes());
    key
}

fn prefix(day: u32, exchange: Exchange, contract: &str) -> Vec<u8> {
    let mut key = day_key(day).to_vec();
    key.push(exchange as u8);
    key.extend_from_slice(contract.as_bytes());
    key.push(0);
    key
}

fn encode_day(day: u32) -> [u8; 4] {
    day.to_be_bytes()
}

fn read_day(db: &DB) -> Result<u32> {
    match db.get(DAY_KEY)? {
        Some(value) => Ok(u32::from_be_bytes(value.as_slice().try_into()?)),
        None => Ok(0),
    }
}

struct Writer {
    db: DB,
    directory: PathBuf,
    day: u32,
}

impl Writer {
    fn open(cfg: &Config, session_snapshot: &str) -> Result<Self> {
        let mut opts = options();
        opts.create_if_missing(true);
        std::fs::create_dir_all(&cfg.directory)?;
        let db = DB::open(&opts, &cfg.directory)?;
        match db.get(SETTINGS_KEY)? {
            Some(old) if old != session_snapshot.as_bytes() => {
                // A different session snapshot has a different minute grid.
                let mut batch = WriteBatch::default();
                batch.delete_range(day_key(0), day_key(u32::MAX));
                batch.put(DAY_KEY, encode_day(0));
                batch.put(SETTINGS_KEY, session_snapshot);
                db.write(batch)?;
            }
            None => db.put(SETTINGS_KEY, session_snapshot)?,
            _ => {}
        }
        let day = read_day(&db)?;
        let writer = Self {
            db,
            directory: cfg.directory.clone(),
            day,
        };
        writer.publish(day)?;
        Ok(writer)
    }

    fn publish(&self, day: u32) -> Result<()> {
        let next = self.directory.join("cache-day.next");
        std::fs::write(&next, encode_day(day))?;
        std::fs::rename(next, self.directory.join(PUBLISHED_DAY))?;
        Ok(())
    }

    fn write(&mut self, day: u32, bars: &mut Vec<(Exchange, String, CachedMinute)>) -> Result<()> {
        if day < self.day || bars.is_empty() {
            bars.clear();
            return Ok(());
        }
        let mut batch = WriteBatch::default();
        if day > self.day {
            batch.delete_range(day_key(0), day_key(day));
            batch.put(DAY_KEY, encode_day(day));
        }
        for (venue, symbol, minute) in bars.drain(..) {
            let bar = minute.bar.as_ref().context("cache bar missing")?;
            let mut key = prefix(day, venue, &symbol);
            key.extend_from_slice(&bar.open_time_ms.to_be_bytes());
            batch.put(key, minute.encode_to_vec());
        }
        let mut opts = WriteOptions::default();
        opts.set_sync(true);
        self.db.write_opt(batch, &opts)?;
        if self.day != day {
            self.publish(day)?;
            self.day = day;
        }
        Ok(())
    }

    fn prune(&mut self, archived: u32) -> Result<()> {
        if self.day != 0 && self.day <= archived {
            // Hide the expired day before RocksDB drops its old files. A
            // secondary might still be holding the previous version.
            self.publish(0)?;
            let mut batch = WriteBatch::default();
            batch.delete_range(day_key(0), day_key(self.day + 1));
            batch.put(DAY_KEY, encode_day(0));
            self.db.write(batch)?;
            self.db.flush()?;
            self.db
                .compact_range(Some(day_key(0)), Some(day_key(self.day + 1)));
            self.day = 0;
        }
        Ok(())
    }
}

pub(crate) struct Reader {
    cfg: Config,
    db: Mutex<Option<(u32, DB)>>,
}

impl Reader {
    pub(crate) fn new(cfg: Config) -> Result<Self> {
        cfg.validate()?;
        Ok(Self {
            cfg,
            db: Mutex::new(None),
        })
    }

    fn published_day(&self) -> Result<u32> {
        match std::fs::read(self.cfg.directory.join(PUBLISHED_DAY)) {
            Ok(bytes) => Ok(u32::from_be_bytes(bytes.as_slice().try_into()?)),
            Err(error) if error.kind() == std::io::ErrorKind::NotFound => Ok(0),
            Err(error) => Err(error.into()),
        }
    }

    fn read<T>(
        &self,
        session_snapshot: &str,
        f: impl FnOnce(&DB) -> Result<T>,
    ) -> Result<Option<T>> {
        let mut slot = self
            .db
            .lock()
            .map_err(|_| anyhow::anyhow!("minute cache reader lock poisoned"))?;
        let day = self.published_day()?;
        if day == 0 {
            *slot = None;
            return Ok(None);
        }
        if slot.as_ref().is_none_or(|(old, _)| *old != day) {
            *slot = None;
            std::fs::create_dir_all(&self.cfg.secondary_directory)?;
            let mut opts = options();
            opts.set_max_open_files(-1);
            *slot = Some((
                day,
                DB::open_as_secondary(&opts, &self.cfg.directory, &self.cfg.secondary_directory)?,
            ));
        }
        let (_, db) = slot.as_ref().expect("opened above");
        let result = (|| {
            db.try_catch_up_with_primary()?;
            if db.get(SETTINGS_KEY)?.as_deref() != Some(session_snapshot.as_bytes()) {
                return Ok(None);
            }
            ensure!(
                read_day(db)? == day,
                "minute cache secondary is catching up with its trading day"
            );
            let value = f(db)?;
            Ok((self.published_day()? == day).then_some(value))
        })();
        if result.is_err() {
            *slot = None;
        }
        result
    }

    pub(crate) fn bars(
        &self,
        day: u32,
        exchange: Exchange,
        native: &str,
        session_snapshot: &str,
    ) -> Result<Vec<CachedMinute>> {
        self.read(session_snapshot, |db| series(db, day, exchange, native))
            .map(Option::unwrap_or_default)
    }

    pub(crate) fn current(
        &self,
        exchange: Exchange,
        contract: &str,
        session_snapshot: &str,
    ) -> Result<Option<(u32, String, Vec<CachedMinute>)>> {
        self.read(session_snapshot, |db| {
            let day = read_day(db)?;
            let Some(product) = product_id(contract) else {
                return Ok(None);
            };
            if day == 0 {
                return Ok(None);
            }
            let native = match exchange {
                Exchange::Cffex | Exchange::Czce => {
                    let suffix = &contract[product.len()..];
                    let digits = if exchange == Exchange::Czce && suffix.len() == 4 {
                        &suffix[1..]
                    } else {
                        suffix
                    };
                    format!("{product}{digits}")
                }
                _ => contract.to_ascii_lowercase(),
            };
            if crate::canonical(&native, day) != crate::canonical(contract, day) {
                return Ok(None);
            }
            let bars = series(db, day, exchange, &native)?;
            Ok((!bars.is_empty()).then_some((day, native, bars)))
        })
        .map(Option::flatten)
    }
}

fn series(db: &DB, day: u32, exchange: Exchange, contract: &str) -> Result<Vec<CachedMinute>> {
    if day != read_day(db)? {
        return Ok(Vec::new());
    }
    let key = prefix(day, exchange, contract);
    let mut bars = Vec::new();
    for row in db.iterator(IteratorMode::From(&key, Direction::Forward)) {
        let (stored_key, value) = row?;
        if !stored_key.starts_with(&key) {
            break;
        }
        let minute = CachedMinute::decode(value.as_ref())?;
        let bar = minute
            .bar
            .as_ref()
            .context("minute cache value has no bar")?;
        ensure!(
            bar.trading_day == day
                && bar.open_time_ms % MINUTE_MS == 0
                && stored_key.get(key.len()..) == Some(&bar.open_time_ms.to_be_bytes()),
            "minute cache record does not match its key"
        );
        bars.push(minute);
    }
    Ok(bars)
}

fn agrees(a: &MinuteBar, b: &MinuteBar) -> bool {
    let mut lhs = a.clone();
    let mut rhs = b.clone();
    lhs.provisional = false;
    rhs.provisional = false;
    // Compare the protobuf representation so missing prices (NaN on both
    // sources) agree without collapsing an absent price to zero.
    lhs.encode_to_vec() == rhs.encode_to_vec()
}

pub(crate) struct Comparison {
    pub matched: usize,
    pub different: usize,
    pub cache_only: usize,
    pub mdl_only: usize,
    pub first_difference_ms: Option<i64>,
}

pub(crate) fn compare(mdl: &[MinuteBar], cached: &[CachedMinute], through_ms: i64) -> Comparison {
    let mut result = Comparison {
        matched: 0,
        different: 0,
        cache_only: 0,
        mdl_only: 0,
        first_difference_ms: None,
    };
    let (Some(first), Some(last)) = (cached.first(), cached.last()) else {
        return result;
    };
    let lo = first.bar.as_ref().unwrap().open_time_ms;
    let hi = last
        .bar
        .as_ref()
        .unwrap()
        .open_time_ms
        .min(through_ms - MINUTE_MS);
    let mut a = mdl
        .iter()
        .filter(|b| (lo..=hi).contains(&b.open_time_ms))
        .peekable();
    let mut b = cached
        .iter()
        .filter_map(|c| c.bar.as_ref())
        .filter(|b| b.open_time_ms <= hi)
        .peekable();
    loop {
        match (a.peek(), b.peek()) {
            (Some(x), Some(y)) if x.open_time_ms == y.open_time_ms => {
                if agrees(x, y) {
                    result.matched += 1;
                } else {
                    result.different += 1;
                    result.first_difference_ms.get_or_insert(x.open_time_ms);
                }
                a.next();
                b.next();
            }
            (Some(x), Some(y)) if x.open_time_ms < y.open_time_ms => {
                result.mdl_only += 1;
                result.first_difference_ms.get_or_insert(x.open_time_ms);
                a.next();
            }
            (_, Some(y)) => {
                result.cache_only += 1;
                result.first_difference_ms.get_or_insert(y.open_time_ms);
                b.next();
            }
            (Some(x), None) => {
                result.mdl_only += 1;
                result.first_difference_ms.get_or_insert(x.open_time_ms);
                a.next();
            }
            (None, None) => break,
        }
    }
    result
}

pub(crate) fn extend(
    mdl: &mut Vec<MinuteBar>,
    cached: Vec<CachedMinute>,
    through_ms: i64,
    in_session: impl Fn(i64) -> bool,
) -> i64 {
    let Some(anchor) = mdl
        .iter()
        .rev()
        .find(|b| b.open_time_ms + MINUTE_MS <= through_ms)
    else {
        return through_ms;
    };
    let Ok(i) = cached.binary_search_by_key(&anchor.open_time_ms, |c| {
        c.bar.as_ref().unwrap().open_time_ms
    }) else {
        return through_ms;
    };
    if !agrees(anchor, cached[i].bar.as_ref().unwrap()) {
        return through_ms;
    }
    let mut end = anchor.open_time_ms + MINUTE_MS;
    let generation = cached[i].generation;
    let tail = contiguous(
        cached.into_iter().skip(i + 1),
        generation,
        &mut end,
        in_session,
    );
    if end > through_ms && !tail.is_empty() {
        mdl.retain(|b| b.open_time_ms + MINUTE_MS <= through_ms);
        mdl.extend(tail);
        end
    } else {
        through_ms
    }
}

fn contiguous(
    cached: impl IntoIterator<Item = CachedMinute>,
    generation: i64,
    end: &mut i64,
    in_session: impl Fn(i64) -> bool,
) -> Vec<MinuteBar> {
    let mut bars = Vec::new();
    for minute in cached {
        let mut bar = minute.bar.expect("validated cache bar");
        if minute.generation != generation
            || bar.open_time_ms < *end
            || bar.open_time_ms - *end > 48 * 60 * MINUTE_MS
        {
            break;
        }
        if (*end..bar.open_time_ms)
            .step_by(MINUTE_MS as usize)
            .any(&in_session)
        {
            break;
        }
        *end = bar.open_time_ms + MINUTE_MS;
        bar.provisional = true;
        bars.push(bar);
    }
    bars
}

pub(crate) fn from_open(
    cached: Vec<CachedMinute>,
    is_open: impl FnOnce(i64) -> bool,
    in_session: impl Fn(i64) -> bool,
) -> Option<(i64, Vec<MinuteBar>)> {
    let first = cached.first()?;
    let mut end = first.bar.as_ref()?.open_time_ms;
    if !is_open(end) {
        return None;
    }
    let generation = first.generation;
    let bars = contiguous(cached, generation, &mut end, in_session);
    Some((end, bars))
}

struct Contract {
    venue: Venue,
    symbol: String,
    product: String,
    day: NaiveDate,
    trading_day: u32,
    tick: f64,
    multiplier: f64,
    carry: InstrumentCarry,
    rows: Vec<crate::live_seg::Row>,
    minute: i64,
    first_minute: i64,
    last_identity: (u32, u64),
    last_quote_ns: i64,
    previous: Option<(i64, BaselineMinute)>,
    generation: i64,
    last_received: Instant,
}

impl Contract {
    fn ingest(
        &mut self,
        q: MdlQuote<'_>,
        sessions: &SessionMatcher<'_>,
        size: Option<&crate::size_thr::SizeThresholds>,
    ) -> Result<Option<MinuteBar>> {
        let row = crate::live_seg::Row::from_mdl(q)?;
        let identity = (q.action_day, q.source_sequence);
        if identity == self.last_identity {
            if self.rows.is_empty() {
                return Ok(None);
            }
            ensure!(
                self.rows.last().is_some_and(|prev| prev.ts_ns == row.ts_ns
                    && prev
                        .v
                        .iter()
                        .zip(row.v)
                        .all(|(&a, b)| a == b || (a.is_nan() && b.is_nan()))),
                "conflicting duplicate native quote"
            );
            return Ok(None);
        }
        ensure!(
            identity > self.last_identity && row.ts_ns >= self.last_quote_ns,
            "native quote order regressed"
        );
        let minute = row.ts_ns.div_euclid(60 * NS) * 60;
        ensure!(
            self.previous
                .as_ref()
                .is_none_or(|(_, previous)| previous.ts < minute),
            "native update arrived after its minute closed"
        );
        let closed = if self.minute != minute {
            let closed = self.close(sessions, size)?;
            self.minute = minute;
            closed
        } else {
            None
        };
        ensure!(
            self.rows.len() < MAX_QUOTES_PER_MINUTE,
            "too many native quotes in a minute"
        );
        self.last_identity = identity;
        self.last_quote_ns = row.ts_ns;
        self.last_received = Instant::now();
        self.rows.push(row);
        Ok(closed)
    }

    fn close(
        &mut self,
        sessions: &SessionMatcher<'_>,
        size: Option<&crate::size_thr::SizeThresholds>,
    ) -> Result<Option<MinuteBar>> {
        if self.rows.is_empty() {
            return Ok(None);
        }
        let mut snaps: Vec<_> = self
            .rows
            .drain(..)
            .map(|r| r.snapshot(&self.symbol, self.day))
            .collect();
        let output = process_instrument_resumable(&mut snaps, self.venue, &mut self.carry, true)?;
        let mut depth_secs = output
            .depths
            .iter()
            .filter(|d| Book5::from_depth(d).is_some())
            .map(|d| (d.ts_utc_ns / NS as u64) as i64)
            .filter(|&ts| !cn_futures_core::session::skip_output(ts, self.venue, &self.symbol));
        let Some(first_depth) = depth_secs.next() else {
            self.previous = None;
            return Ok(None);
        };
        let last_depth = depth_secs.next_back().unwrap_or(first_depth);
        let mut trades: Vec<_> = output
            .trades
            .iter()
            .map(|(_, t)| PrintTrade::from(t))
            .collect();
        let mut sparse = baseline_1min::synthesize_minutes(
            &self.symbol,
            &trades,
            &output.depths,
            self.multiplier,
        )?;
        // The previous baseline row holds the exact fill state of the current
        // segment. Its final print seeds TWAP for this minute.
        let start = if let Some((_, previous)) = self.previous.take().filter(|(last, previous)| {
            previous.ts + 60 == self.minute && !is_session_break(*last, first_depth)
        }) {
            if let Some(price) = previous.close {
                trades.insert(
                    0,
                    PrintTrade {
                        ts_utc_ns: previous.ts as u64 * NS as u64,
                        price,
                        volume: 0.,
                        amount: 0.,
                        aggressor: 0,
                    },
                );
            }
            let start = previous.ts;
            sparse.insert(0, previous);
            start
        } else {
            self.minute
        };
        let filled = baseline_1min::fill_session_minutes_with_twap(
            &self.symbol,
            sparse,
            &[(start, self.minute)],
            &trades,
        )?;
        let Some(mut row) = filled.into_iter().last().filter(|r| r.ts == self.minute) else {
            return Ok(None);
        };
        if !sessions.contains_ts(self.minute) {
            self.previous = None;
            return Ok(None);
        }
        let has_thresholds =
            size.is_some_and(|th| th.month_for_minute(&self.product, row.ts).is_some());
        if let Some(th) = size {
            crate::live_seg::apply_size_buckets(std::slice::from_mut(&mut row), &trades, &|t| {
                th.for_trade(&self.product, t)
            });
        }
        let publish = self.minute > self.first_minute && trad_day_from_ts(self.minute) == self.day;
        self.previous = Some((last_depth, row));
        if !publish {
            return Ok(None);
        }
        crate::make_bar(
            &self.previous.as_ref().unwrap().1,
            self.trading_day,
            self.multiplier,
            Some(self.tick),
            true,
            true,
            None,
            has_thresholds,
        )
        .map_err(anyhow::Error::from)
    }
}

fn future_identity(frame: &[u8]) -> Option<(Exchange, Venue, &str, String)> {
    let (exchange, venue) = match frame.get(..10)? {
        b"mdl.21.1.*" => (Exchange::Cffex, Venue::Ccfx),
        b"mdl.22.1.*" => (Exchange::Shfe, Venue::Xsge),
        b"mdl.22.3.*" => (Exchange::Ine, Venue::Xsie),
        b"mdl.23.1.*" => (Exchange::Czce, Venue::Xzce),
        b"mdl.24.1.*" => (Exchange::Dce, Venue::Xdce),
        b"mdl.26.1.*" => (Exchange::Gfex, Venue::Xgfe),
        _ => return None,
    };
    if frame.get(10..14)? != b" || " {
        return None;
    }
    let symbol_index = if matches!(venue, Venue::Xsge | Venue::Xsie) {
        0
    } else {
        3
    };
    let symbol = std::str::from_utf8(frame[14..].split(|&b| b == b',').nth(symbol_index)?).ok()?;
    let product = product_id(symbol)?;
    let digits = &symbol[product.len()..];
    let months = if venue == Venue::Xzce { 3 } else { 4 };
    if digits.len() != months
        || !digits[months - 2..]
            .parse::<u8>()
            .is_ok_and(|m| (1..=12).contains(&m))
    {
        return None;
    }
    Some((exchange, venue, symbol, product))
}

fn send_bar(
    pending: &mut Vec<(Exchange, String, CachedMinute)>,
    exchange: Exchange,
    c: &Contract,
    bar: MinuteBar,
) {
    pending.push((
        exchange,
        c.symbol.clone(),
        CachedMinute {
            generation: c.generation,
            bar: Some(bar),
        },
    ));
}

pub fn check(path: &Path) -> Result<()> {
    let market = MarketConfig::load(path)?;
    market
        .minute_cache
        .context("missing [minute_cache]")?
        .validate()?;
    ensure!(
        market.live.is_some(),
        "minute cache requires [live] reference settings"
    );
    Ok(())
}

pub fn run(path: &Path) -> Result<()> {
    check(path)?;
    let market = MarketConfig::load(path)?;
    let config = market.minute_cache.as_ref().expect("validated");
    let runtime = tokio::runtime::Builder::new_multi_thread()
        .worker_threads(2)
        .enable_all()
        .build()?;
    let stopping = Arc::new(AtomicBool::new(false));
    let signal_stop = stopping.clone();
    let mut terminate = {
        let _entered = runtime.enter();
        tokio::signal::unix::signal(tokio::signal::unix::SignalKind::terminate())?
    };
    runtime.spawn(async move {
        tokio::select! {
            _ = terminate.recv() => {}
            _ = tokio::signal::ctrl_c() => {}
        }
        signal_stop.store(true, Ordering::Release);
    });
    let metadata = runtime.block_on(crate::load_metadata(&market))?;
    let sessions = ContinuousSessions::fetch(
        &format!(
            "{}/api/v1/trading-sessions",
            market.reference_api.trim_end_matches('/')
        ),
        Some(&market.session_as_of),
    )?;
    let size = market
        .live
        .as_ref()
        .and_then(|l| l.size_threshold_dir.clone())
        .map(crate::size_thr::SizeThresholds::new);
    let mut writer = Writer::open(config, &market.session_as_of)?;
    let context = zmq::Context::new();
    let socket = context.socket(zmq::SUB)?;
    socket.set_linger(0)?;
    socket.set_rcvhwm(131072)?;
    socket.set_maxmsgsize(65536)?;
    socket.set_rcvtimeo(100)?;
    socket.monitor(
        "inproc://minute-cache-monitor",
        zmq::SocketEvent::CONNECTED as i32 | zmq::SocketEvent::DISCONNECTED as i32,
    )?;
    let monitor = context.socket(zmq::PAIR)?;
    monitor.set_linger(0)?;
    monitor.connect("inproc://minute-cache-monitor")?;
    for topic in mdl::TOPICS {
        socket.set_subscribe(topic.as_bytes())?;
    }
    socket.connect(&config.zmq_endpoint)?;
    let mut states = HashMap::<(Exchange, String), Contract>::new();
    let mut unavailable = std::collections::HashSet::new();
    let mut pending = Vec::new();
    let mut active_day = writer.day;
    let mut last_flush = Instant::now();
    let mut last_prune = Instant::now() - Duration::from_secs(60);
    let mut archived = 0;
    let mut connected = false;
    while !stopping.load(Ordering::Acquire) {
        while let Ok(event) = monitor.recv_msg(zmq::DONTWAIT) {
            ensure!(
                event.len() == 6 && monitor.get_rcvmore()?,
                "invalid ZMQ monitor event"
            );
            monitor.recv_msg(0)?;
            writer.write(active_day, &mut pending)?;
            states.clear();
            connected =
                u16::from_ne_bytes([event[0], event[1]]) == zmq::SocketEvent::CONNECTED.to_raw();
            eprintln!("cn-minute-cache: ZMQ connected={connected}");
        }
        let raw = match socket.recv_msg(0) {
            Ok(raw) => Some(raw),
            Err(zmq::Error::EAGAIN | zmq::Error::EINTR) => None,
            Err(error) => return Err(error.into()),
        };
        if let Some(raw) = raw {
            if socket.get_rcvmore()? {
                while socket.get_rcvmore()? {
                    socket.recv_msg(0)?;
                }
                states.clear();
                continue;
            }
            if !connected {
                continue;
            }
            let Some((exchange, venue, symbol, product)) = future_identity(&raw) else {
                continue;
            };
            let quote = match mdl::parse_frame(&raw) {
                Ok(quote)
                    if quote.exchange_ts_us > 0
                        && quote.action_day != 0
                        && quote.source_sequence < i64::MAX as u64 =>
                {
                    quote
                }
                other => {
                    if states.remove(&(exchange, symbol.to_owned())).is_some() {
                        eprintln!(
                            "cn-minute-cache: invalid {} {symbol} quote breaks coverage: {:?}",
                            venue.as_str(),
                            other.err()
                        );
                    }
                    continue;
                }
            };
            if quote.trading_day <= archived || quote.trading_day < active_day {
                continue;
            }
            let key = (exchange, symbol.to_owned());
            if unavailable.contains(&key) {
                continue;
            }
            let Ok(matcher) = sessions.matcher(quote.venue, &product) else {
                eprintln!(
                    "cn-minute-cache: missing continuous sessions for {} {product}",
                    quote.venue
                );
                unavailable.insert(key);
                continue;
            };
            let min = quote.exchange_ts_us.div_euclid(60_000_000) * 60;
            if !matcher.contains_ts(min)
                && !cn_futures_core::session::is_auction_ts(min, venue)
                && !matcher.contains_ts(min - 60)
            {
                continue;
            }
            if quote.trading_day > active_day {
                writer.write(active_day, &mut pending)?;
                active_day = quote.trading_day;
                states.clear();
                unavailable.clear();
            }
            if !states.contains_key(&key) {
                let multiplier = metadata
                    .multipliers
                    .get(&product)
                    .filter(|m| {
                        m.verified && m.volume_multiple.is_finite() && m.volume_multiple > 0.
                    })
                    .map(|m| m.volume_multiple);
                let tick = metadata
                    .ticks
                    .lookup(quote.venue, symbol, quote.trading_day);
                let (Some(multiplier), Ok((tick, _))) = (multiplier, tick) else {
                    eprintln!(
                        "cn-minute-cache: missing verified multiplier or tick for {} {symbol}",
                        quote.venue
                    );
                    unavailable.insert(key);
                    continue;
                };
                states.insert(
                    key.clone(),
                    Contract {
                        venue,
                        symbol: symbol.into(),
                        product,
                        day: NaiveDate::parse_from_str(&quote.trading_day.to_string(), "%Y%m%d")?,
                        trading_day: quote.trading_day,
                        tick,
                        multiplier,
                        carry: InstrumentCarry::default(),
                        rows: Vec::new(),
                        minute: min,
                        first_minute: min,
                        last_identity: (0, 0),
                        last_quote_ns: 0,
                        previous: None,
                        generation: Utc::now().timestamp_micros(),
                        last_received: Instant::now(),
                    },
                );
            }
            let state = states.get_mut(&key).expect("initialized");
            match state.ingest(quote, &matcher, size.as_ref()) {
                Ok(Some(bar)) => send_bar(&mut pending, exchange, state, bar),
                Ok(None) => {}
                Err(error) => {
                    eprintln!(
                        "cn-minute-cache: {} {symbol} continuity lost: {error:#}",
                        quote.venue
                    );
                    states.remove(&key);
                }
            }
        } else {
            let now_ns = Utc::now().timestamp_nanos_opt().unwrap_or(0);
            for ((exchange, _), state) in &mut states {
                if state.rows.is_empty() || state.last_received.elapsed() > Duration::from_secs(5) {
                    continue;
                }
                let end = (state.minute + 60) * NS;
                if state.last_quote_ns < end - NS
                    || now_ns < end + 2 * NS
                    || now_ns - state.last_quote_ns > 5 * NS
                {
                    continue;
                }
                let venue = reference_venue(state.venue.as_str())?;
                let matcher = sessions.matcher(venue, &state.product)?;
                if matcher.contains_ts(state.minute) && !matcher.contains_ts(state.minute + 60) {
                    if let Some(bar) = state.close(&matcher, size.as_ref())? {
                        send_bar(&mut pending, *exchange, state, bar);
                    }
                }
            }
        }
        if pending.len() >= 2048 || last_flush.elapsed() >= Duration::from_millis(100) {
            writer.write(active_day, &mut pending)?;
            last_flush = Instant::now();
        }
        if last_prune.elapsed() >= Duration::from_secs(60) {
            archived = crate::last_done(&market.daily_state_file);
            writer.prune(archived)?;
            last_prune = Instant::now();
        }
    }
    writer.write(active_day, &mut pending)
}

#[cfg(test)]
mod tests {
    use super::*;
    use chrono::TimeZone;

    fn bar(t: i64, day: u32, volume: f64) -> MinuteBar {
        MinuteBar {
            trading_day: day,
            open_time_ms: t,
            volume_lots: volume,
            provisional: true,
            close: f64::NAN,
            ..Default::default()
        }
    }

    fn cached(t: i64, day: u32, volume: f64, generation: i64) -> CachedMinute {
        CachedMinute {
            generation,
            bar: Some(bar(t, day, volume)),
        }
    }

    #[test]
    fn joins_at_a_matching_closed_minute_and_stops_at_a_gap_or_reconnect() {
        let day = 20261118;
        let mdl = vec![
            bar(0, day, 1.0),
            bar(60_000, day, 2.0),
            bar(120_000, day, 9.0),
        ];
        let cache = vec![
            cached(0, day, 1.0, 1),
            cached(60_000, day, 2.0, 1),
            cached(120_000, day, 3.0, 1),
            cached(180_000, day, 4.0, 2),
        ];
        assert_eq!(compare(&mdl, &cache, 125_000).matched, 2);
        let mut joined = mdl.clone();
        assert_eq!(
            extend(&mut joined, cache.clone(), 125_000, |_| true),
            180_000
        );
        assert_eq!(joined.len(), 3);
        assert_eq!(joined[2].volume_lots, 3.0);

        let mut conflict = cache.clone();
        conflict[1].bar.as_mut().unwrap().volume_lots = 20.0;
        assert_eq!(compare(&mdl, &conflict, 125_000).different, 1);
        assert_eq!(
            extend(&mut mdl.clone(), conflict, 125_000, |_| true),
            125_000
        );

        let mut missing = cache;
        missing.remove(2);
        missing[2].generation = 1;
        assert_eq!(
            extend(&mut mdl.clone(), missing.clone(), 125_000, |_| true),
            125_000
        );
        assert_eq!(
            extend(&mut mdl.clone(), missing, 125_000, |_| false),
            240_000
        );
    }

    #[test]
    fn secondary_reads_live_protobuf_and_follows_rotation_and_archival_cleanup() {
        let temp = tempfile::tempdir().unwrap();
        let cfg = Config {
            directory: temp.path().join("primary"),
            secondary_directory: temp.path().join("secondary"),
            zmq_endpoint: "tcp://127.0.0.1:1".into(),
        };
        let mut writer = Writer::open(&cfg, "2026-11-15").unwrap();
        let reader = Reader::new(cfg).unwrap();
        let first = 20261118;
        let next = 20261119;
        let write = |writer: &mut Writer, day| {
            writer
                .write(
                    day,
                    &mut vec![
                        (Exchange::Shfe, "sn2612".into(), cached(60_000, day, 1.0, 1)),
                        (Exchange::Dce, "m2701".into(), cached(120_000, day, 2.0, 1)),
                    ],
                )
                .unwrap();
        };
        write(&mut writer, first);
        assert_eq!(
            reader
                .bars(first, Exchange::Shfe, "sn2612", "2026-11-15")
                .unwrap()
                .len(),
            1
        );
        assert!(reader
            .bars(first, Exchange::Ine, "sn2612", "2026-11-15")
            .unwrap()
            .is_empty());
        assert!(reader
            .bars(first, Exchange::Shfe, "sn2612", "2026-11-14")
            .unwrap()
            .is_empty());
        write(&mut writer, next);
        assert!(reader
            .bars(first, Exchange::Shfe, "sn2612", "2026-11-15")
            .unwrap()
            .is_empty());
        writer.prune(first).unwrap();
        assert_eq!(
            reader
                .current(Exchange::Dce, "M2701", "2026-11-15")
                .unwrap()
                .unwrap()
                .0,
            next
        );
        writer.prune(next).unwrap();
        assert!(reader
            .current(Exchange::Dce, "m2701", "2026-11-15")
            .unwrap()
            .is_none());
    }

    #[test]
    fn live_topics_map_to_the_six_native_exchanges() {
        for (i, exchange, native) in [
            (0, Exchange::Cffex, "IF2612"),
            (1, Exchange::Shfe, "sn2612"),
            (2, Exchange::Ine, "sc2701"),
            (3, Exchange::Czce, "SR701"),
            (4, Exchange::Dce, "m2701"),
            (5, Exchange::Gfex, "lc2701"),
        ] {
            let payload = if [1, 2].contains(&i) {
                format!("{native},other,fields")
            } else {
                format!("other,other,other,{native},fields")
            };
            let raw = format!("{} || {payload}", mdl::TOPICS[i]);
            assert_eq!(
                future_identity(raw.as_bytes()).map(|(e, _, s, _)| (e, s)),
                Some((exchange, native))
            );
        }
        assert!(future_identity(b"mdl.22.1.* || sn2612C300000,fields").is_none());
    }

    #[test]
    fn streaming_minutes_match_the_whole_segment_including_empty_minutes() {
        let date = NaiveDate::from_ymd_opt(2026, 11, 18).unwrap();
        let start = chrono_tz::Asia::Shanghai
            .with_ymd_and_hms(2026, 11, 18, 9, 0, 0)
            .unwrap()
            .timestamp();
        let json = serde_json::json!({
            "timezone": "Asia/Shanghai",
            "dataState": {"as_of": "2026-11-15", "latest_start_ts": "", "latest_update_ts": "",
                "session_count": 1, "product_count": 1, "venue_count": 1, "version_count": 1},
            "sessions": [{"venue": "SHFE", "product_id": "SN", "segment_no": 1, "time_begin": "09:00:00", "time_end": "10:15:00", "crosses_midnight": false, "is_active": true}]
        });
        let sessions = ContinuousSessions::parse(&json.to_string()).unwrap();
        let matcher = sessions.matcher("SHFE", "SN").unwrap();
        let mut c = Contract {
            venue: Venue::Xsge,
            symbol: "sn2612".into(),
            product: "SN".into(),
            day: date,
            trading_day: 20261118,
            tick: 10.,
            multiplier: 1.,
            carry: InstrumentCarry::default(),
            rows: Vec::new(),
            minute: start,
            first_minute: start,
            last_identity: (0, 0),
            last_quote_ns: 0,
            previous: None,
            generation: 1,
            last_received: Instant::now(),
        };
        let mut raw = Vec::new();
        let mut actual = Vec::new();
        for (i, (offset, volume, price)) in [
            (1, 1, 380000.),
            (50, 3, 380010.),
            (70, 3, 380010.),
            (111, 3, 380010.),
            (120, 4, 380020.),
            (145, 7, 380030.),
            (181, 7, 380030.),
        ]
        .into_iter()
        .enumerate()
        {
            let bid = mdl::MdlLevel {
                price: price - 10.,
                quantity: 10,
            };
            let ask = mdl::MdlLevel {
                price: price + 10.,
                quantity: 10,
            };
            let q = MdlQuote {
                venue: "SHFE",
                symbol: b"sn2612",
                trading_day: 20261118,
                action_day: 20261118,
                exchange_ts_us: (start + offset) * 1_000_000,
                source_sequence: i as u64 + 1,
                last_price: price,
                volume,
                turnover: price * volume as f64,
                open_interest: 100.,
                bid: [bid; 5],
                ask: [ask; 5],
            };
            raw.push(
                crate::live_seg::Row::from_mdl(q)
                    .unwrap()
                    .snapshot("sn2612", date),
            );
            if let Some(row) = c.ingest(q, &matcher, None).unwrap() {
                actual.push(row);
            }
        }
        let expected =
            crate::live::compute("sn2612", &mut raw, Venue::Xsge, 1., date, &sessions, None)
                .unwrap();
        let expected: Vec<_> = expected
            .iter()
            .filter(|m| m.ts > start && m.ts < start + 180)
            .filter_map(|r| {
                crate::make_bar(r, 20261118, 1., Some(10.), true, true, None, false).unwrap()
            })
            .collect();
        assert_eq!(actual.len(), 2);
        assert_eq!(expected.len(), actual.len());
        for (got, want) in actual.iter().zip(&expected) {
            assert!(agrees(got, want), "got {got:?}\nwant {want:?}");
        }
    }
}
