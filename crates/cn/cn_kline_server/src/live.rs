//! In-progress trading day, rebuilt straight from the NFS `tonglian_msg` CSVs.
//!
//! The daily parquet only exists after `cn-daily` (16:30). During the day this
//! module tails each exchange's `mdl_*` file (the NFS copy grows in delayed
//! batches) and keeps, per contract, only the snapshots of the segment that is
//! still open (see [`crate::live_seg`]). Finished segments (night, morning,
//! afternoon) are computed once with the nightly-export chain, written to
//! `segment_dir` as parquet, and their snapshots dropped, so memory stays flat
//! over the day and a restart resumes from a manifest instead of re-reading the
//! whole file. A request serves persisted segments plus the open segment
//! recomputed from the carried machine state.
//!
//! No database, no ZMQ: ZMQ stays the live feed and this only covers what NFS
//! has already received (e.g. after a ZMQ-to-disk failure or restart).
//!
//! Limits: every bar is provisional (the open segment's last minutes may still
//! change as NFS catches up) and carries no `size_buckets` (monthly thresholds
//! are unknown until month end). Without `segment_dir` nothing is persisted and
//! the whole day stays in memory (the previous behaviour).
use crate::live_seg::{
    carry_from_json, carry_to_json, minutes_from_output, ContractState, Ctx, Params, Push, Row, NS,
};
use anyhow::{bail, Context, Result};
use chrono::NaiveDate;
use cn_futures_core::segments::SESSION_BREAK_SEC;
use cn_futures_core::universe::product_id;
use cn_futures_l2::baseline_1min::BaselineMinute;
use cn_futures_l2::events::{process_instrument, Snapshot};
use cn_futures_l2::export_1min::{read_day_parquet, write_day_parquet};
use cn_futures_l2::source::{last_trad_day, read_tail_limited, source_files_for_day, TailState};
use cn_futures_l2::Exchange;
use cn_futures_trim::sessions::{reference_venue, ContinuousSessions};
use serde::Deserialize;
use serde_json::{json, Value};
use std::collections::{HashMap, VecDeque};
use std::path::{Path, PathBuf};
use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant, SystemTime, UNIX_EPOCH};

#[derive(Clone, Debug, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct Config {
    pub msg_root: PathBuf,
    /// Product ids served from the in-progress day (e.g. ["SN"]); `["*"]` or
    /// an empty list means every maintained product (all six exchanges).
    pub products: Vec<String>,
    #[serde(default = "d_refresh_ms")]
    pub min_refresh_ms: u64,
    /// Background refresh period; 0 disables it (requests refresh lazily, and
    /// nothing is finalized: finishing segments happens in the background).
    #[serde(default = "d_background_s")]
    pub background_s: u64,
    #[serde(default = "d_lookback")]
    pub lookback_days: u64,
    /// Where finished segments and the restart manifest are written
    /// (`<dir>/<YYYYMMDD>/<exchange>/...`). Unset: keep the whole day in memory.
    #[serde(default)]
    pub segment_dir: Option<PathBuf>,
    /// A segment is only finished when the NFS clock (newest snapshot of the
    /// exchange) is this far past its end.
    #[serde(default = "d_margin")]
    pub finalize_margin_s: u64,
    /// The last segment of a contract is finished after this much wall-clock
    /// silence. It must exceed the 30 min break plus the NFS delivery lag.
    #[serde(default = "d_tail")]
    pub tail_quiet_s: u64,
    /// Scheduled tail cut: a contract whose last row sits at the end of a
    /// scheduled window followed by a break of at least 30 minutes is finished
    /// this many seconds (wall clock) after that row, once the files are
    /// caught up, so the NFS writes of the close have completed.
    #[serde(default = "d_delay")]
    pub delay_s: u64,
    /// The scheduled cut lies this many seconds after the window end.
    #[serde(default = "d_grace")]
    pub tail_grace_s: u64,
    /// At most this many bytes of one file are parsed per step (bounds the
    /// memory of the first load of a long day).
    #[serde(default = "d_step_mb")]
    pub step_mb: u64,
    /// The restart manifest is rewritten at least this often.
    #[serde(default = "d_manifest_s")]
    pub manifest_every_s: u64,
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
fn d_margin() -> u64 {
    60
}
fn d_delay() -> u64 {
    600
}
fn d_grace() -> u64 {
    300
}
fn d_tail() -> u64 {
    7200
}
fn d_step_mb() -> u64 {
    256
}
fn d_manifest_s() -> u64 {
    300
}

/// Contracts finished per lock hold (requests for the exchange wait at most one
/// chunk).
const FINALIZE_CHUNK: usize = 24;
const MAX_CHECKPOINTS: usize = 4096;
/// An unfinished last line shorter than this does not hold back the next file.
const PARTIAL_LINE_SLACK: u64 = 16 * 1024;

struct FileState {
    path: PathBuf,
    tail: TailState,
}

/// A read position at a moment when nothing older than `max_ts_ns` followed.
struct Checkpoint {
    pos: Vec<(u64, u32)>,
    max_ts_ns: i64,
}

struct DayState {
    day: NaiveDate,
    files: Vec<FileState>,
    contracts: HashMap<String, ContractState>,
    through_ms: i64,
    max_read_ns: i64,
    checkpoints: VecDeque<Checkpoint>,
    /// While set, rows are the replay of a range the manifest already covers:
    /// per file, the offset to stop at.
    replay: Option<HashMap<PathBuf, u64>>,
    manifest_dirty: bool,
    last_manifest: Instant,
    checked: Instant,
}

impl DayState {
    fn fresh(day: NaiveDate) -> Self {
        Self {
            day,
            files: Vec::new(),
            contracts: HashMap::new(),
            through_ms: 0,
            max_read_ns: 0,
            checkpoints: VecDeque::new(),
            replay: None,
            manifest_dirty: false,
            last_manifest: Instant::now(),
            checked: Instant::now(),
        }
    }
}

/// Supplies the volume multiplier used to finish segments in the background.
pub struct Finalizer<'a> {
    pub multiplier: &'a dyn Fn(&str) -> Option<f64>,
}

pub struct Live {
    cfg: Config,
    sessions: Arc<ContinuousSessions>,
    /// One lock per exchange so a cold read of one file never blocks another.
    state: HashMap<&'static str, Mutex<Option<DayState>>>,
}

const ALL_EXCHANGES: [Exchange; 6] = [
    Exchange::Ccfx,
    Exchange::Xdce,
    Exchange::Xgfe,
    Exchange::Xsge,
    Exchange::Xsie,
    Exchange::Xzce,
];

/// One contract's in-progress minutes.
pub struct LiveBars {
    pub day: NaiveDate,
    pub native_code: String,
    pub minutes: Vec<BaselineMinute>,
    /// Newest snapshot (UTC ms) read from NFS for this exchange.
    pub through_ms: i64,
}

fn now_ns() -> i64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map_or(0, |d| d.as_nanos() as i64)
}

fn yyyymmdd(d: NaiveDate) -> u32 {
    use chrono::Datelike;
    (d.year() * 10000) as u32 + d.month() * 100 + d.day()
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

    fn params(&self) -> Params {
        Params {
            margin_ns: self.cfg.finalize_margin_s as i64 * NS,
            tail_quiet_ns: self.cfg.tail_quiet_s as i64 * NS,
            delay_ns: self.cfg.delay_s as i64 * NS,
            tail_grace_ns: self.cfg.tail_grace_s as i64 * NS,
        }
    }

    fn day_dir(&self, day: NaiveDate, exchange: Exchange) -> Option<PathBuf> {
        self.cfg.segment_dir.as_ref().map(|d| {
            d.join(day.format("%Y%m%d").to_string())
                .join(exchange.as_str())
        })
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

    /// A day state, restored from the manifest when one exists.
    fn open_state(&self, exchange: Exchange, day: NaiveDate) -> DayState {
        let mut st = DayState::fresh(day);
        if let Some(dir) = self.day_dir(day, exchange) {
            match load_manifest(&dir.join("manifest.json"), day) {
                Ok(Some(m)) => {
                    st.through_ms = m.through_ms;
                    st.files = m
                        .files
                        .iter()
                        .map(|(p, off, rows)| FileState {
                            path: p.clone(),
                            tail: if *off == 0 {
                                TailState::default()
                            } else {
                                TailState::resume_at(*off, *rows)
                            },
                        })
                        .collect();
                    st.checkpoints.push_back(Checkpoint {
                        pos: m.files.iter().map(|(_, off, rows)| (*off, *rows)).collect(),
                        max_ts_ns: -1,
                    });
                    st.replay = Some(m.read_end);
                    st.contracts = m.contracts;
                }
                Ok(None) => {}
                Err(e) => eprintln!("cn-market: live manifest {dir:?} unusable, rebuilding: {e:#}"),
            }
            if st.replay.is_none() {
                let _ = std::fs::remove_dir_all(&dir);
            }
        }
        st
    }

    /// Discards persisted segments and state of an exchange (violation or
    /// unusable manifest) so the next step rebuilds from the files.
    fn reset(&self, exchange: Exchange, st: &mut Option<DayState>) {
        if let Some(s) = st.as_ref() {
            if let Some(dir) = self.day_dir(s.day, exchange) {
                let _ = std::fs::remove_dir_all(dir);
            }
            *st = Some(DayState::fresh(s.day));
        }
    }

    /// One unit of work for an exchange: align the state with the current day,
    /// read at most `step_mb` per file, then (with `fin`) finish a chunk of
    /// contracts and save the manifest when due. Returns `Some(more)`; `None`
    /// when no trading day newer than `last_done` is open here.
    fn step(
        &self,
        exchange: Exchange,
        last_done: u32,
        fin: Option<&Finalizer>,
        st: &mut Option<DayState>,
    ) -> Result<Option<bool>> {
        let Some(day) = self.current_day(exchange, last_done)? else {
            *st = None;
            return Ok(None);
        };
        if st.as_ref().is_none_or(|s| s.day != day) {
            *st = Some(self.open_state(exchange, day));
        }
        let located = source_files_for_day(
            exchange,
            day,
            None,
            Some(&self.cfg.msg_root),
            None,
            self.cfg.lookback_days,
        )?;
        let s = st.as_mut().expect("set above");
        // File list in source order, keeping known read positions.
        let mut files = Vec::with_capacity(located.len());
        for f in &located {
            let known = s.files.iter().position(|k| k.path == f.path);
            files.push(match known {
                Some(i) => s.files.swap_remove(i),
                None => FileState {
                    path: f.path.clone(),
                    tail: TailState::default(),
                },
            });
        }
        s.files = files;

        let step = self.cfg.step_mb << 20;
        let mut late_info: Option<String> = None;
        let mut moved = false;
        let mut reset_needed = false;
        let replaying = s.replay.is_some();
        for (order, f) in s.files.iter_mut().enumerate() {
            let before = f.tail.offset();
            let cap = before + step;
            let limit = match s.replay.as_ref().and_then(|r| r.get(&f.path)) {
                Some(end) => cap.min(*end),
                None => cap,
            };
            if replaying
                && limit <= before
                && s.replay.as_ref().is_some_and(|r| r.contains_key(&f.path))
            {
                continue; // replay of this file already complete
            }
            let mut new = Vec::new();
            let rewritten = read_tail_limited(
                &f.path,
                day,
                order as u32,
                &self.cfg.products,
                &mut f.tail,
                Some(limit),
                &mut new,
            )?;
            if rewritten {
                reset_needed = true;
                break;
            }
            moved |= f.tail.offset() > before;
            let replay_row = replaying
                && s.replay
                    .as_ref()
                    .and_then(|r| r.get(&f.path))
                    .is_some_and(|end| before < *end);
            for snap in new {
                let row = Row::of(&snap);
                s.through_ms = s.through_ms.max(snap.event_time.timestamp_millis());
                s.max_read_ns = s.max_read_ns.max(row.ts_ns);
                let ts_ns = row.ts_ns;
                let c = s.contracts.entry(snap.instrument_id.clone()).or_default();
                let (cut_last, until) = (c.cut_last_ns, c.persisted_until_ns);
                if c.push(row, replay_row) == Push::Late && late_info.is_none() {
                    late_info = Some(format!(
                        "{} at {ts_ns} (last snapshot before the cut {cut_last}, cut {until})",
                        snap.instrument_id
                    ));
                }
            }
            // Files are one time-ordered stream (previous day's night rows end
            // the earlier file): never read ahead into the next file before this
            // one is consumed, or its later rows would precede this file's
            // remaining earlier rows.
            let end = match s.replay.as_ref().and_then(|r| r.get(&f.path)) {
                Some(end) if replay_row => *end,
                _ => std::fs::metadata(&f.path)?.len(),
            };
            if f.tail.offset() + PARTIAL_LINE_SLACK < end {
                break;
            }
        }
        let late = late_info.is_some();
        if reset_needed || late {
            eprintln!(
                "cn-market: live {} {day}: {} -> rebuilding from the files",
                exchange.as_str(),
                if let Some(info) = &late_info {
                    format!("snapshot older than a persisted segment: {info}")
                } else {
                    "msg file was rewritten".to_string()
                }
            );
            self.reset(exchange, st);
            return Ok(Some(true));
        }
        if moved {
            s.checkpoints.push_back(Checkpoint {
                pos: s
                    .files
                    .iter()
                    .map(|f| (f.tail.offset(), f.tail.rows()))
                    .collect(),
                max_ts_ns: s.max_read_ns,
            });
            if s.checkpoints.len() > MAX_CHECKPOINTS {
                // Thin out the older half (any retained checkpoint stays valid).
                let keep: VecDeque<Checkpoint> = s
                    .checkpoints
                    .drain(..)
                    .enumerate()
                    .filter(|(i, _)| i % 2 == 1)
                    .map(|(_, c)| c)
                    .collect();
                s.checkpoints = keep;
            }
        }
        if let Some(r) = &s.replay {
            let done = s
                .files
                .iter()
                .all(|f| r.get(&f.path).is_none_or(|end| f.tail.offset() >= *end));
            if done {
                s.replay = None;
                for c in s.contracts.values_mut() {
                    c.recheck();
                }
            }
        }
        s.checked = Instant::now();
        let mut more = moved;
        if let (Some(fin), Some(_)) = (fin, self.cfg.segment_dir.as_ref()) {
            if s.replay.is_none() {
                more |= self.finalize_some(exchange, s, fin, !moved)?;
                self.save_manifest(exchange, s, false)?;
            }
        }
        Ok(Some(more))
    }

    /// Finishes ready segments for up to `FINALIZE_CHUNK` contracts. Returns
    /// whether more may remain.
    fn finalize_some(
        &self,
        exchange: Exchange,
        s: &mut DayState,
        fin: &Finalizer,
        caught_up: bool,
    ) -> Result<bool> {
        let Some(dir) = self.day_dir(s.day, exchange) else {
            return Ok(false);
        };
        let venue = reference_venue(exchange.as_str())?;
        let (through_ns, now) = (s.through_ms * 1_000_000, now_ns());
        let ids: Vec<String> = s
            .contracts
            .iter()
            .filter(|(_, c)| !c.rows.is_empty())
            .map(|(id, _)| id.clone())
            .collect();
        let mut done = 0usize;
        let mut first_error: Option<String> = None;
        for id in ids {
            let Some(product) = product_id(&id) else {
                continue;
            };
            let c = s.contracts.get_mut(&id).expect("listed");
            let Ok(matcher) = self.sessions.matcher(venue, &product) else {
                first_error.get_or_insert(format!("no session snapshot for {venue}.{product}"));
                continue;
            };
            let at_break = |ts: i64| matcher.break_end(ts, 180, SESSION_BREAK_SEC);
            let cuts = c.plan_cuts(through_ns, now, caught_up, self.params(), &at_break);
            if cuts.is_empty() {
                continue;
            }
            let Some(multiplier) = (fin.multiplier)(&product) else {
                continue;
            };
            let keep = |ts: i64| matcher.contains_ts(ts);
            let ctx = Ctx {
                instrument: &id,
                exchange,
                multiplier,
                day: s.day,
                in_session: &keep,
            };
            let finished = c
                .compute_segments(&cuts, &ctx)
                .and_then(|(segments, carry)| {
                    let contract_dir = dir.join(&id);
                    for seg in &segments {
                        persist_segment(&contract_dir, seg.first_ts, &seg.minutes)?;
                    }
                    Ok(carry)
                });
            match finished {
                Ok(carry) => c.commit(&cuts, carry),
                Err(e) => {
                    first_error.get_or_insert(format!("{id}: {e:#}"));
                    continue;
                }
            }
            s.manifest_dirty = true;
            done += 1;
            if done >= FINALIZE_CHUNK {
                return Ok(true);
            }
        }
        match first_error {
            Some(e) => bail!("finishing segments: {e}"),
            None => Ok(false),
        }
    }

    fn save_manifest(&self, exchange: Exchange, s: &mut DayState, force: bool) -> Result<()> {
        let Some(dir) = self.day_dir(s.day, exchange) else {
            return Ok(());
        };
        let due = s.last_manifest.elapsed() >= Duration::from_secs(self.cfg.manifest_every_s);
        if !(force || s.manifest_dirty || due) || s.contracts.is_empty() {
            return Ok(());
        }
        if !s.contracts.values().any(|c| c.persisted_until_ns > 0) {
            return Ok(()); // nothing persisted yet: nothing to resume from
        }
        // Resume point: the newest read position before which every snapshot is
        // already covered by a persisted segment of its contract.
        let need_from = s
            .contracts
            .values()
            .map(|c| c.persisted_until_ns)
            .min()
            .unwrap_or(0);
        let chosen = s.checkpoints.iter().rposition(|c| c.max_ts_ns < need_from);
        let resume: Vec<(u64, u32)> = match chosen {
            Some(i) => {
                let pos = s.checkpoints[i].pos.clone();
                s.checkpoints.drain(..i);
                pos
            }
            None => Vec::new(),
        };
        let files: Vec<(PathBuf, u64, u32, u64)> = s
            .files
            .iter()
            .enumerate()
            .map(|(i, f)| {
                let (off, rows) = resume.get(i).copied().unwrap_or((0, 0));
                (f.path.clone(), off, rows, f.tail.offset())
            })
            .collect();
        let doc = manifest_doc(s.day, s.through_ms, &files, &s.contracts);
        std::fs::create_dir_all(&dir)?;
        let tmp = dir.join(".manifest.tmp");
        std::fs::write(&tmp, serde_json::to_vec(&doc)?)?;
        std::fs::rename(&tmp, dir.join("manifest.json"))?;
        s.manifest_dirty = false;
        s.last_manifest = Instant::now();
        Ok(())
    }

    /// Background work for every exchange: read what NFS gained, finish closed
    /// segments, save manifests, delete the partial files of published days.
    /// Per-exchange errors are returned together, not fatal for the others.
    pub fn refresh_all(&self, last_done: u32, fin: &Finalizer) -> Vec<String> {
        let mut errors = Vec::new();
        for exchange in ALL_EXCHANGES {
            loop {
                let result = self.state[exchange.as_str()]
                    .lock()
                    .map_err(|_| anyhow::anyhow!("live lock poisoned"))
                    .and_then(|mut st| self.step(exchange, last_done, Some(fin), &mut st));
                match result {
                    Ok(Some(true)) => continue,
                    Ok(_) => break,
                    Err(e) => {
                        errors.push(format!("{}: {e:#}", exchange.as_str()));
                        break;
                    }
                }
            }
        }
        if let Err(e) = self.cleanup(last_done) {
            errors.push(format!("cleanup: {e:#}"));
        }
        errors
    }

    /// Removes partial files of days that `cn-daily` has published.
    fn cleanup(&self, last_done: u32) -> Result<()> {
        let Some(root) = &self.cfg.segment_dir else {
            return Ok(());
        };
        let Ok(entries) = std::fs::read_dir(root) else {
            return Ok(());
        };
        for e in entries.filter_map(|e| e.ok()) {
            let published = e
                .file_name()
                .to_str()
                .and_then(|n| n.parse::<u32>().ok())
                .is_some_and(|d| d <= last_done);
            if published {
                std::fs::remove_dir_all(e.path())?;
            }
        }
        Ok(())
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
        type Picked = (String, ContractState);
        let (day, through_ms, picked): (NaiveDate, i64, Vec<Picked>) = {
            let mut st = self.state[exchange.as_str()]
                .lock()
                .map_err(|_| anyhow::anyhow!("live lock poisoned"))?;
            let fresh = st.as_ref().is_some_and(|s| {
                s.checked.elapsed() < Duration::from_millis(self.cfg.min_refresh_ms)
                    && yyyymmdd(s.day) > last_done
            });
            if !fresh {
                // Read what NFS gained (no finishing here: that is background work).
                loop {
                    match self.step(exchange, last_done, None, &mut st)? {
                        None => return Ok(None),
                        Some(true) => continue,
                        Some(false) => break,
                    }
                }
            }
            let s = st.as_ref().expect("refreshed");
            let td = yyyymmdd(s.day);
            // Clone state first, list persisted segments after: a segment
            // finished in between then shows up twice (identical, deduped),
            // never zero times.
            let picked = s
                .contracts
                .iter()
                .filter(|(id, _)| matches(id, td))
                .map(|(id, c)| (id.clone(), c.clone()))
                .collect();
            (s.day, s.through_ms, picked)
        };
        let venue = reference_venue(exchange.as_str())?;
        let matcher = self.sessions.matcher(venue, product)?;
        let keep = |ts: i64| matcher.contains_ts(ts);
        let mut out = Vec::new();
        for (id, state) in picked {
            let ctx = Ctx {
                instrument: &id,
                exchange,
                multiplier,
                day,
                in_session: &keep,
            };
            let mut minutes = match self.day_dir(day, exchange) {
                Some(dir) => read_persisted(&dir.join(&id))?,
                None => Vec::new(),
            };
            minutes.extend(state.open_minutes(&ctx)?);
            minutes.sort_by_key(|m| m.ts);
            minutes.dedup_by_key(|m| m.ts);
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

fn persist_segment(dir: &Path, first_ts: i64, minutes: &[BaselineMinute]) -> Result<()> {
    std::fs::create_dir_all(dir)?;
    let tmp = dir.join(format!(".{first_ts}.tmp"));
    write_day_parquet(&tmp, minutes)?;
    std::fs::rename(&tmp, dir.join(format!("{first_ts}.parquet")))?;
    Ok(())
}

fn read_persisted(dir: &Path) -> Result<Vec<BaselineMinute>> {
    let mut out = Vec::new();
    let Ok(entries) = std::fs::read_dir(dir) else {
        return Ok(out);
    };
    for e in entries.filter_map(|e| e.ok()) {
        let p = e.path();
        if p.extension().and_then(|x| x.to_str()) == Some("parquet") {
            out.extend(read_day_parquet(&p)?);
        }
    }
    Ok(out)
}

/// The restart manifest: per file the resume position (`offset`, data `rows`
/// before it) and how far it had been read (`read_end`), and per contract the
/// state after its last persisted cut.
fn manifest_doc(
    day: NaiveDate,
    through_ms: i64,
    files: &[(PathBuf, u64, u32, u64)],
    contracts: &HashMap<String, ContractState>,
) -> Value {
    let files: Vec<Value> = files
        .iter()
        .map(|(path, off, rows, end)| {
            json!({"path": path, "offset": off, "rows": rows, "read_end": end})
        })
        .collect();
    let contracts: serde_json::Map<String, Value> = contracts
        .iter()
        .filter(|(_, c)| c.persisted_until_ns > 0)
        .map(|(id, c)| {
            (
                id.clone(),
                json!({"until": c.persisted_until_ns, "cut_last": c.cut_last_ns,
                       "max_ts": c.max_ts_ns, "carry": carry_to_json(&c.carry)}),
            )
        })
        .collect();
    json!({
        "version": 1, "day": day.format("%Y%m%d").to_string(),
        "through_ms": through_ms, "files": files, "contracts": contracts,
    })
}

struct Manifest {
    through_ms: i64,
    /// (path, resume offset, data rows before it)
    files: Vec<(PathBuf, u64, u32)>,
    read_end: HashMap<PathBuf, u64>,
    contracts: HashMap<String, ContractState>,
}

fn load_manifest(path: &Path, day: NaiveDate) -> Result<Option<Manifest>> {
    let bytes = match std::fs::read(path) {
        Ok(b) => b,
        Err(e) if e.kind() == std::io::ErrorKind::NotFound => return Ok(None),
        Err(e) => return Err(e.into()),
    };
    let v: Value = serde_json::from_slice(&bytes).context("manifest json")?;
    if v["version"] != 1 || v["day"].as_str() != Some(&day.format("%Y%m%d").to_string()) {
        bail!("manifest version/day mismatch");
    }
    let mut files = Vec::new();
    let mut read_end = HashMap::new();
    for f in v["files"].as_array().context("files")? {
        let p = PathBuf::from(f["path"].as_str().context("path")?);
        files.push((
            p.clone(),
            f["offset"].as_u64().context("offset")?,
            f["rows"].as_u64().context("rows")? as u32,
        ));
        read_end.insert(p, f["read_end"].as_u64().context("read_end")?);
    }
    let mut contracts = HashMap::new();
    for (id, c) in v["contracts"].as_object().context("contracts")? {
        let st = ContractState::restored(
            carry_from_json(&c["carry"])?,
            c["until"].as_i64().context("until")?,
            c["cut_last"].as_i64().context("cut_last")?,
            c["max_ts"].as_i64().context("max_ts")?,
        );
        contracts.insert(id.clone(), st);
    }
    Ok(Some(Manifest {
        through_ms: v["through_ms"].as_i64().context("through_ms")?,
        files,
        read_end,
        contracts,
    }))
}

/// One-shot minutes of a whole day's snapshots (the nightly chain, no segment
/// cuts): the reference the segmented path must reproduce.
pub fn compute(
    instrument: &str,
    snaps: &mut [Snapshot],
    exchange: Exchange,
    multiplier: f64,
    day: NaiveDate,
    sessions: &ContinuousSessions,
) -> Result<Vec<BaselineMinute>> {
    let product = product_id(instrument).context("product of instrument")?;
    let matcher = sessions.matcher(reference_venue(exchange.as_str())?, &product)?;
    let keep = |ts: i64| matcher.contains_ts(ts);
    let ctx = Ctx {
        instrument,
        exchange,
        multiplier,
        day,
        in_session: &keep,
    };
    minutes_from_output(&ctx, process_instrument(snaps, exchange)?)
}

#[cfg(test)]
mod tests {
    use super::*;
    use cn_futures_core::events::InstrumentCarry;

    #[test]
    fn manifest_round_trips_resume_positions_and_contract_state() {
        let day = NaiveDate::from_ymd_opt(2026, 11, 17).unwrap();
        let carry = InstrumentCarry {
            vol_prev: Some(12.5),
            trade_seq: 3,
            ..InstrumentCarry::default()
        };
        let mut contracts = HashMap::new();
        contracts.insert(
            "sn2701".to_string(),
            ContractState::restored(carry.clone(), 1_000, 900, 950),
        );
        contracts.insert("cu2701".to_string(), ContractState::default());
        let files = vec![
            (PathBuf::from("/nfs/a.csv"), 4096u64, 17u32, 9000u64),
            (PathBuf::from("/nfs/b.csv"), 0, 0, 50),
        ];
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("manifest.json");
        std::fs::write(
            &path,
            serde_json::to_vec(&manifest_doc(day, 777, &files, &contracts)).unwrap(),
        )
        .unwrap();
        let m = load_manifest(&path, day).unwrap().unwrap();
        assert_eq!(m.through_ms, 777);
        assert_eq!(m.files[0], (PathBuf::from("/nfs/a.csv"), 4096, 17));
        assert_eq!(m.read_end[Path::new("/nfs/a.csv")], 9000);
        assert_eq!(
            m.contracts.len(),
            1,
            "contracts without a cut are not stored"
        );
        let c = &m.contracts["sn2701"];
        assert_eq!(
            (c.persisted_until_ns, c.cut_last_ns, c.max_ts_ns),
            (1_000, 900, 950)
        );
        assert_eq!(c.carry, carry);
        let other = NaiveDate::from_ymd_opt(2026, 11, 18).unwrap();
        assert!(
            load_manifest(&path, other).is_err(),
            "another day's manifest"
        );
        assert!(load_manifest(&dir.path().join("none.json"), day)
            .unwrap()
            .is_none());
    }

    #[test]
    fn persisted_segments_are_listed_per_contract_and_ignore_temp_files() {
        let dir = tempfile::tempdir().unwrap();
        let trade = cn_futures_l2::baseline_1min::PrintTrade {
            ts_utc_ns: 600 * NS as u64,
            price: 1.5,
            volume: 3.0,
            amount: 45.0,
            aggressor: 1,
        };
        let base = cn_futures_l2::baseline_1min::synthesize_minutes("sn2701", &[trade], &[], 1.0)
            .unwrap()
            .remove(0);
        let minute = |ts: i64| BaselineMinute { ts, ..base.clone() };
        persist_segment(dir.path(), 600, &[minute(600), minute(660)]).unwrap();
        persist_segment(dir.path(), 7200, &[minute(7200)]).unwrap();
        std::fs::write(dir.path().join(".9999.tmp"), b"half written").unwrap();
        let mut got = read_persisted(dir.path()).unwrap();
        got.sort_by_key(|m| m.ts);
        assert_eq!(
            got.iter().map(|m| m.ts).collect::<Vec<_>>(),
            vec![600, 660, 7200]
        );
        assert!(read_persisted(&dir.path().join("missing"))
            .unwrap()
            .is_empty());
    }
}
