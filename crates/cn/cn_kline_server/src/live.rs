//! In-progress trading day, rebuilt straight from the NFS `tonglian_msg` CSVs.
//!
//! The daily parquet only exists after `cn-daily` (16:30). During the day this
//! module tails the exchange's `mdl_*` file (the NFS copy grows in delayed
//! batches), keeps the configured products' snapshots in memory, and for each
//! request re-runs the same chain as the nightly export (snapshot -> trade
//! state machine -> minute accumulation -> session fill) on that contract.
//! No database, no ZMQ: ZMQ stays the live feed and this only covers what NFS
//! has already received (e.g. after a ZMQ-to-disk failure or restart). State
//! lives in memory only, so a restart simply re-reads the files.
//!
//! Limits: every bar is provisional (the last minutes may still change as NFS
//! catches up) and carries no `size_buckets` (monthly thresholds are unknown
//! until month end).
use anyhow::{Context, Result};
use chrono::NaiveDate;
use cn_futures_core::segments::{book_valid, trad_day_from_ts};
use cn_futures_l2::baseline_1min::{
    fill_session_minutes_with_twap, minute_segments_from_seconds, synthesize_minutes,
    BaselineMinute, PrintTrade,
};
use cn_futures_l2::events::{process_instrument, Snapshot};
use cn_futures_l2::source::{last_trad_day, read_tail, source_files_for_day, TailState};
use cn_futures_l2::{split_depth_segments, Exchange};
use cn_futures_trim::sessions::{reference_venue, ContinuousSessions};
use serde::Deserialize;
use std::collections::HashMap;
use std::path::PathBuf;
use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant};

#[derive(Clone, Debug, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct Config {
    pub msg_root: PathBuf,
    /// Product ids served from the in-progress day (e.g. ["SN"]); `["*"]` or
    /// an empty list means every maintained product (all six exchanges).
    pub products: Vec<String>,
    #[serde(default = "d_refresh_ms")]
    pub min_refresh_ms: u64,
    /// Background refresh period; 0 disables it (requests refresh lazily).
    #[serde(default = "d_background_s")]
    pub background_s: u64,
    #[serde(default = "d_lookback")]
    pub lookback_days: u64,
}
fn d_refresh_ms() -> u64 {
    2000
}
fn d_background_s() -> u64 {
    5
}
fn d_lookback() -> u64 {
    16
}

struct DayState {
    day: NaiveDate,
    files: HashMap<PathBuf, TailState>,
    snaps: HashMap<String, Vec<Snapshot>>,
    through_ms: i64,
    checked: Instant,
}

const ALL_EXCHANGES: [Exchange; 6] = [
    Exchange::Ccfx,
    Exchange::Xdce,
    Exchange::Xgfe,
    Exchange::Xsge,
    Exchange::Xsie,
    Exchange::Xzce,
];

pub struct Live {
    cfg: Config,
    sessions: Arc<ContinuousSessions>,
    /// One lock per exchange so a cold read of one file never blocks another.
    state: HashMap<&'static str, Mutex<Option<DayState>>>,
}

/// One contract's in-progress minutes.
pub struct LiveBars {
    pub day: NaiveDate,
    pub native_code: String,
    pub minutes: Vec<BaselineMinute>,
    /// Newest snapshot (UTC ms) read from NFS for this exchange.
    pub through_ms: i64,
}

impl Live {
    /// `sessions` must be the same continuous-session snapshot the archive
    /// was trimmed with (`session_as_of`), so live bars match the final file.
    pub fn new(cfg: Config, sessions: Arc<ContinuousSessions>) -> Self {
        Self {
            cfg,
            sessions,
            state: ALL_EXCHANGES
                .iter()
                .map(|e| (e.as_str(), Mutex::new(None)))
                .collect(),
        }
    }

    pub fn serves(&self, product: &str) -> bool {
        self.cfg.products.is_empty()
            || self
                .cfg
                .products
                .iter()
                .any(|p| p == "*" || p.eq_ignore_ascii_case(product))
    }

    /// The in-progress trading day of this exchange (newer than `last_done`),
    /// read from the last row of the newest msg directory.
    fn current_day(&self, exchange: Exchange, last_done: u32) -> Result<Option<NaiveDate>> {
        let mut dirs: Vec<(NaiveDate, PathBuf)> = std::fs::read_dir(&self.cfg.msg_root)
            .with_context(|| format!("read {:?}", self.cfg.msg_root))?
            .filter_map(|e| e.ok())
            .filter_map(|e| {
                let d = NaiveDate::parse_from_str(e.file_name().to_str()?, "%Y%m%d").ok()?;
                Some((d, e.path()))
            })
            .collect();
        dirs.sort_by_key(|d| std::cmp::Reverse(d.0));
        for (_, dir) in dirs.into_iter().take(4) {
            let file = dir.join(exchange.msg_stem());
            if file.is_file() {
                let day = last_trad_day(&file)?;
                return Ok(day.filter(|d| yyyymmdd(*d) > last_done));
            }
        }
        Ok(None)
    }

    fn refresh(
        &self,
        exchange: Exchange,
        last_done: u32,
        st: &mut Option<DayState>,
    ) -> Result<bool> {
        if let Some(s) = st.as_ref() {
            if s.checked.elapsed() < Duration::from_millis(self.cfg.min_refresh_ms)
                && yyyymmdd(s.day) > last_done
            {
                return Ok(true);
            }
        }
        let Some(day) = self.current_day(exchange, last_done)? else {
            *st = None;
            return Ok(false);
        };
        let fresh = || DayState {
            day,
            files: HashMap::new(),
            snaps: HashMap::new(),
            through_ms: 0,
            checked: Instant::now(),
        };
        if st.as_ref().is_none_or(|s| s.day != day) {
            *st = Some(fresh());
        }
        let s = st.as_mut().expect("set above");
        let files = source_files_for_day(
            exchange,
            day,
            None,
            Some(&self.cfg.msg_root),
            None,
            self.cfg.lookback_days,
        )?;
        for attempt in 0..2 {
            let mut shrank = false;
            for (order, f) in files.iter().enumerate() {
                let tail = s.files.entry(f.path.clone()).or_default();
                let mut new = Vec::new();
                shrank |= read_tail(
                    &f.path,
                    day,
                    order as u32,
                    &self.cfg.products,
                    tail,
                    &mut new,
                )?;
                if shrank {
                    break;
                }
                for snap in new {
                    s.through_ms = s.through_ms.max(snap.event_time.timestamp_millis());
                    s.snaps
                        .entry(snap.instrument_id.clone())
                        .or_default()
                        .push(snap);
                }
            }
            if !shrank || attempt == 1 {
                break;
            }
            *s = fresh(); // a file was rewritten: rebuild from scratch
        }
        s.checked = Instant::now();
        Ok(true)
    }

    /// Reads whatever NFS has gained on every exchange (background warm-up,
    /// so requests normally find the state already current). Per-exchange
    /// errors are returned together, not fatal for the other exchanges.
    pub fn refresh_all(&self, last_done: u32) -> Vec<String> {
        let mut errors = Vec::new();
        for exchange in ALL_EXCHANGES {
            let result = self.state[exchange.as_str()]
                .lock()
                .map_err(|_| anyhow::anyhow!("live lock poisoned"))
                .and_then(|mut st| self.refresh(exchange, last_done, &mut st));
            if let Err(e) = result {
                errors.push(format!("{}: {e:#}", exchange.as_str()));
            }
        }
        errors
    }

    /// In-progress minutes of every instrument of `product` accepted by
    /// `matches`, or `None` when no trading day newer than `last_done` is
    /// open on this exchange.
    pub fn bars(
        &self,
        exchange_dir: &str,
        product: &str,
        last_done: u32,
        matches: impl Fn(&str, u32) -> bool,
        multiplier: f64,
    ) -> Result<Option<Vec<LiveBars>>> {
        if !self.serves(product) {
            return Ok(None);
        }
        let exchange = Exchange::parse(exchange_dir).context("exchange")?;
        let (day, through_ms, picked) = {
            let mut st = self.state[exchange.as_str()]
                .lock()
                .map_err(|_| anyhow::anyhow!("live lock poisoned"))?;
            if !self.refresh(exchange, last_done, &mut st)? {
                return Ok(None);
            }
            let s = st.as_ref().expect("refreshed");
            let td = yyyymmdd(s.day);
            let picked: Vec<(String, Vec<Snapshot>)> = s
                .snaps
                .iter()
                .filter(|(id, _)| matches(id, td))
                .map(|(id, v)| (id.clone(), v.clone()))
                .collect();
            (s.day, s.through_ms, picked)
        };
        let mut out = Vec::new();
        for (id, mut snaps) in picked {
            let minutes = compute(&id, &mut snaps, exchange, multiplier, day, &self.sessions)?;
            out.push(LiveBars {
                day,
                native_code: id,
                minutes,
                through_ms,
            });
        }
        Ok(Some(out))
    }
}

fn yyyymmdd(d: NaiveDate) -> u32 {
    use chrono::Datelike;
    (d.year() * 10000) as u32 + d.month() * 100 + d.day()
}

/// Same chain as `export_1min::export_product`, minus RocksDB.
pub fn compute(
    instrument: &str,
    snaps: &mut [Snapshot],
    exchange: Exchange,
    multiplier: f64,
    day: NaiveDate,
    sessions: &ContinuousSessions,
) -> Result<Vec<BaselineMinute>> {
    let out = process_instrument(snaps, exchange)?;
    let mut depths = Vec::new();
    let mut depth_secs = Vec::new();
    for rec in out.depths {
        let (Some(bp), Some(ap)) = (rec.bid_prices[0], rec.ask_prices[0]) else {
            continue;
        };
        let bv = rec.bid_sizes[0].unwrap_or(f64::NAN);
        let av = rec.ask_sizes[0].unwrap_or(f64::NAN);
        if !book_valid(bp, bv, ap, av) {
            continue;
        }
        depth_secs.push((rec.ts_utc_ns / 1_000_000_000) as i64);
        depths.push(rec);
    }
    let trades: Vec<PrintTrade> = out
        .trades
        .iter()
        .map(|(_, t)| PrintTrade::from(t))
        .collect();
    if depth_secs.is_empty() && trades.is_empty() {
        return Ok(Vec::new());
    }
    let sec_segs = split_depth_segments(&depth_secs, exchange, instrument);
    let min_segs = minute_segments_from_seconds(&sec_segs);
    let sparse = synthesize_minutes(instrument, &trades, &depths, multiplier)?;
    let filled = fill_session_minutes_with_twap(instrument, sparse, &min_segs, &trades)?;
    let product: String = instrument
        .chars()
        .take_while(|c| c.is_ascii_alphabetic())
        .collect();
    let matcher = sessions.matcher(reference_venue(exchange.as_str())?, &product)?;
    Ok(filled
        .into_iter()
        .filter(|r| trad_day_from_ts(r.ts) == day && matcher.contains_ts(r.ts))
        .collect())
}
