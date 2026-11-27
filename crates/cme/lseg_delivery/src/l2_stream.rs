//! Stages 1 and 2 of the L2 pipeline in ONE in-memory pass (no temporary text
//! shards on disk).
//!
//! * **producer** threads decompress a sorted CSV stream (a daily file, or the
//!   parts of a period package read back to back as one logical stream), find the
//!   RIC blocks and cut every block into *segments* of about `segment_bytes`;
//! * **consumer** threads wash segments (`l2_db::wash_segment`): parse, delta/bucket
//!   encode, write an SST, ingest it into RocksDB.
//!
//! Segments of one RIC are independent: the cuts are made only where the
//! sequential writer starts a self-contained record anyway (the first row of a
//! RIC, of a UTC day, of every 256th non-empty second of a day), so any number of
//! consumers can wash the segments of the same RIC at the same time, in any
//! order, and the database holds the bytes of a sequential pass. The parallelism
//! therefore does not depend on how big a RIC is, and a block of any size needs
//! bounded memory: the bytes of queued segments are capped by a global budget.
//! A consumer never waits for a producer (a segment is a complete job), so there
//! is no producer/consumer deadlock.
//!
//! The fixed pool (`workers` threads, `producers` of them decompress) never grows.
//! The shard-file variant of `l2_batch` is kept for comparison.
//!
//! Units
//! * `Daily { product, day }`: one gz file = one UTC day; every RIC goes into the
//!   day's database (`l2_db::day_dir`), completion marker `M|<product>` as before.
//! * `Stream { id, parts }`: the parts are ONE sorted stream (a RIC can continue
//!   from one part into the next without any special case); one database per
//!   unit holds all of it; `R|<RIC>` marks a finished RIC and `M|<id>` the unit.
//!   Only the first part carries a `#RIC` header line.
//! * A failing unit is reported and gets no marker; the other units go on.
use crate::l2_db::{self, RicOutcome, WashTarget, WashedRic};
use crate::l2_raw::RawConverter;
use crate::l2_slice::{day_number, expected_header};
use anyhow::{bail, ensure, Context, Result};
use flate2::read::MultiGzDecoder;
use rocksdb::{WriteOptions, DB};
use serde_json::{json, Value};
use std::{
    collections::{BTreeMap, HashMap, HashSet, VecDeque},
    fs::{self, File},
    io::{BufRead, BufReader, Read},
    path::{Path, PathBuf},
    sync::{
        atomic::{AtomicU64, AtomicUsize, Ordering},
        Arc, Condvar, Mutex,
    },
    time::{Duration, Instant},
};

pub const BLOCK_BYTES: usize = 8 << 20;
pub const SEGMENT_BYTES: usize = 32 << 20;
const MAX_LINE_BYTES: usize = 1 << 20;
/// The sequential writer restarts a self-contained record every this many
/// non-empty seconds (`l2_layout::CHECKPOINT_BUCKETS`).
const CHECKPOINT: u64 = crate::l2_layout::CHECKPOINT_BUCKETS as u64;

#[derive(Clone, Debug)]
pub enum UnitKind {
    Daily {
        product: String,
        day: String,
    },
    Stream {
        id: String,
        /// Parts in stream order.
        parts: Vec<PathBuf>,
    },
}

#[derive(Clone, Debug)]
pub struct Unit {
    pub kind: UnitKind,
    pub gz_bytes: u64,
}

impl Unit {
    pub fn id(&self) -> String {
        match &self.kind {
            UnitKind::Daily { product, day } => format!("{product}:{day}"),
            UnitKind::Stream { id, .. } => id.clone(),
        }
    }
}

/// Counters shared with the driver's progress file.
#[derive(Default)]
pub struct Progress {
    pub gz_bytes_read: AtomicU64,
    pub rows: AtomicU64,
    pub units_total: AtomicUsize,
    pub units_done: AtomicUsize,
    pub gz_bytes_total: AtomicU64,
    pub gz_bytes_done_units: AtomicU64,
    pub current: Mutex<Vec<String>>,
    pub errors: Mutex<Vec<String>>,
}

#[derive(Clone)]
pub struct StreamConfig {
    pub raw_root: PathBuf,
    /// Per-day databases of the daily files: `<db_root>/<YYYYMMDD>/`.
    pub db_root: PathBuf,
    /// One database per stream unit: `<stream_db_root>/<unit id>/`.
    pub stream_db_root: PathBuf,
    /// Threads in total (producers + consumers).
    pub workers: usize,
    pub producers: usize,
    pub force: bool,
    /// A segment is cut at the first checkpoint second after this many bytes.
    pub segment_bytes: usize,
    /// Size of one read from the decompressor (whole lines are cut out of it).
    pub block_bytes: usize,
    /// Called by producers before each block; blocks while the run is paused.
    pub gate: Arc<dyn Fn() + Send + Sync>,
    pub progress: Arc<Progress>,
    /// Raw LL2 mode: the stream units are Raw LL2 packages, converted into
    /// Normalized rows (`l2_raw`) before washing; only these RICs are washed.
    pub raw_ll2: Option<Arc<std::collections::BTreeSet<String>>>,
    /// Raw LL2 mode: the parts are separately cut segments of a package (`rawll2_segments.py`),
    /// not one continuous stream: every part starts a new message boundary.
    pub raw_segments: bool,
}

#[derive(Debug, Default)]
pub struct StreamReport {
    pub results: Vec<Value>,
    pub failed: Option<String>,
    pub db_sst_bytes: BTreeMap<String, u64>,
}

// ---------------------------------------------------------------- targets

type DbMap = Mutex<HashMap<String, Arc<DB>>>;

struct DayTarget<'a> {
    root: &'a Path,
    dbs: &'a DbMap,
    day: i64,
}

fn day_name(day: i64) -> String {
    let (y, m, d) = l2_db::civil_from_days(day);
    format!("{y}-{m:02}-{d:02}")
}

fn day_db(root: &Path, dbs: &DbMap, day_text: &str) -> Result<Arc<DB>> {
    let mut m = dbs.lock().expect("dbs lock");
    if let Some(db) = m.get(day_text) {
        return Ok(Arc::clone(db));
    }
    let db = Arc::new(l2_db::open_write(&l2_db::day_dir(root, day_text))?);
    m.insert(day_text.to_string(), Arc::clone(&db));
    Ok(db)
}

impl WashTarget for DayTarget<'_> {
    fn db(&self, day: i64) -> Result<Arc<DB>> {
        day_db(self.root, self.dbs, &day_name(day))
    }
    fn tmp_dir(&self, day: i64) -> PathBuf {
        // opening the database creates (and empties) its ingest_tmp directory
        let _ = self.db(day);
        l2_db::day_dir(self.root, &day_name(day)).join("ingest_tmp")
    }
    fn per_day(&self) -> bool {
        true
    }
    fn only_day(&self) -> Option<i64> {
        Some(self.day)
    }
    fn clean_ric(&self, ric: &str) -> Result<()> {
        let db = self.db(self.day)?;
        l2_db::clean_ric(&db, &l2_db::update_prefix(ric))
    }
}

struct StreamTarget {
    db: Arc<DB>,
    tmp: PathBuf,
}

impl WashTarget for StreamTarget {
    fn db(&self, _day: i64) -> Result<Arc<DB>> {
        Ok(Arc::clone(&self.db))
    }
    fn tmp_dir(&self, _day: i64) -> PathBuf {
        self.tmp.clone()
    }
    fn per_day(&self) -> bool {
        false
    }
    fn only_day(&self) -> Option<i64> {
        None
    }
    fn clean_ric(&self, ric: &str) -> Result<()> {
        l2_db::clean_ric(&self.db, &l2_db::update_prefix(ric))
    }
}

// ---------------------------------------------------------------- reading

struct Counting<R> {
    inner: R,
    n: Arc<Progress>,
}

impl<R: Read> Read for Counting<R> {
    fn read(&mut self, buf: &mut [u8]) -> std::io::Result<usize> {
        let k = self.inner.read(buf)?;
        self.n.gz_bytes_read.fetch_add(k as u64, Ordering::Relaxed);
        Ok(k)
    }
}

type PartRead = BufReader<MultiGzDecoder<Counting<BufReader<File>>>>;

/// The parts of a stream read back to back; a leading `#RIC` header line of a
/// part is checked and dropped, a part without one starts with data.
struct PartReader {
    parts: Vec<PathBuf>,
    next_part: usize,
    cur: Option<PartRead>,
    pending: Option<Vec<u8>>,
    /// Bytes after the last newline of the previous block (a line cut by the block limit).
    carry: Vec<u8>,
    progress: Arc<Progress>,
    header: String,
    /// Raw LL2 mode: raw text lines in, Normalized rows out.
    raw: Option<(RawConverter, Vec<u8>)>,
    /// Raw mode: every part is its own segment (`RawConverter::discontinuity` at each open).
    segments: bool,
}

impl PartReader {
    fn new(
        parts: Vec<PathBuf>,
        start: usize,
        progress: Arc<Progress>,
        raw: Option<Arc<std::collections::BTreeSet<String>>>,
        segments: bool,
    ) -> Self {
        let header = if raw.is_some() {
            cme_tas_replay::raw_ll2::HEADER.to_string()
        } else {
            expected_header()
        };
        Self {
            parts,
            next_part: start,
            cur: None,
            pending: None,
            carry: Vec::new(),
            progress,
            header,
            raw: raw.map(|w| (RawConverter::new(Some(w)), Vec::new())),
            segments,
        }
    }

    fn open_next(&mut self) -> Result<bool> {
        let Some(path) = self.parts.get(self.next_part) else {
            return Ok(false);
        };
        let file = File::open(path).with_context(|| format!("open {}", path.display()))?;
        let counted = Counting {
            inner: BufReader::with_capacity(4 << 20, file),
            n: Arc::clone(&self.progress),
        };
        let mut rd = BufReader::with_capacity(4 << 20, MultiGzDecoder::new(counted));
        let mut first = Vec::new();
        if rd.read_until(b'\n', &mut first)? > 0 {
            if first.first() == Some(&b'#') {
                ensure!(
                    String::from_utf8_lossy(trim_eol(&first)) == self.header,
                    "{}: unexpected header",
                    path.display()
                );
            } else {
                self.pending = Some(first);
            }
        }
        self.cur = Some(rd);
        self.next_part += 1;
        if self.segments {
            if let Some((conv, _)) = self.raw.as_mut() {
                conv.discontinuity();
            }
        }
        Ok(true)
    }

    /// Replaces `out` by whole lines, about `limit` bytes (read in big reads, not
    /// line by line); false when the stream is over and nothing was read. In Raw
    /// mode the lines are converted Normalized rows (a block that yields none is
    /// skipped).
    fn read_block(&mut self, out: &mut Vec<u8>, limit: usize) -> Result<bool> {
        if self.raw.is_none() {
            return self.read_text_block(out, limit);
        }
        let mut text = std::mem::take(&mut self.raw.as_mut().expect("raw").1);
        let result = (|| loop {
            out.clear();
            if !self.read_text_block(&mut text, limit)? {
                self.raw.as_ref().expect("raw").0.finish()?;
                return Ok(false);
            }
            self.raw.as_mut().expect("raw").0.convert(&text, out)?;
            if !out.is_empty() {
                return Ok(true);
            }
        })();
        self.raw.as_mut().expect("raw").1 = text;
        result
    }

    fn read_text_block(&mut self, out: &mut Vec<u8>, limit: usize) -> Result<bool> {
        loop {
            out.clear();
            out.append(&mut self.carry);
            let mut eof = false;
            while out.len() < limit {
                if self.cur.is_none() && !self.open_next()? {
                    eof = true;
                    break;
                }
                if let Some(p) = self.pending.take() {
                    out.extend_from_slice(&p);
                }
                let rd = self.cur.as_mut().expect("open part");
                let want = (limit - out.len()).max(1) as u64;
                let n = rd.by_ref().take(want).read_to_end(out)?;
                if n == 0 {
                    // end of this part: a last line without a newline is complete
                    if !out.is_empty() && out.last() != Some(&b'\n') {
                        out.push(b'\n');
                    }
                    self.cur = None;
                }
            }
            if eof {
                return Ok(!out.is_empty());
            }
            if let Some(i) = memchr::memrchr(b'\n', out) {
                self.carry.extend_from_slice(&out[i + 1..]);
                out.truncate(i + 1);
                return Ok(true);
            }
            // no newline in a whole block: a line longer than the block
            ensure!(
                out.len() <= MAX_LINE_BYTES,
                "line longer than {MAX_LINE_BYTES} bytes"
            );
            self.carry.append(out);
        }
    }
}

fn trim_eol(mut b: &[u8]) -> &[u8] {
    while matches!(b.last(), Some(b'\n' | b'\r')) {
        b = &b[..b.len() - 1];
    }
    b
}

/// RIC of the first data line of a part (cheap: reads the start of the gzip).
pub fn first_ric_of_part(path: &Path) -> Result<String> {
    let mut rd = BufReader::new(MultiGzDecoder::new(File::open(path)?));
    let mut line = Vec::new();
    loop {
        line.clear();
        ensure!(
            rd.read_until(b'\n', &mut line)? > 0,
            "{}: no data row",
            path.display()
        );
        if matches!(line.first(), Some(b'#' | b',')) || trim_eol(&line).is_empty() {
            continue; // header, blank, or the FID rows of a Raw LL2 message
        }
        let ric = line.split(|b| *b == b',').next().unwrap_or(&[]);
        return Ok(String::from_utf8_lossy(ric).into_owned());
    }
}

// ---------------------------------------------------------------- runtime

struct Job {
    unit: usize,
    ric_idx: usize,
    seg: u32,
    ric: String,
    data: Vec<u8>,
}

#[derive(Default)]
struct RicAgg {
    ric: String,
    created: u32,
    done: u32,
    closed: bool,
    finished: bool,
    parts: Vec<(u32, RicOutcome)>,
}

#[derive(Default)]
struct UnitState {
    started: Option<Instant>,
    rics: Vec<RicAgg>,
    jobs_created: usize,
    jobs_done: usize,
    producer_done: bool,
    finished: bool,
    skipped_rics: usize,
    outcomes: Vec<RicOutcome>,
    /// First error of this unit: the unit gets no completion marker, the other
    /// units go on.
    failed: Option<String>,
    first_unfinished: usize,
    cursor_written: Option<Instant>,
}

enum Target<'a> {
    Day(DayTarget<'a>),
    Stream(StreamTarget),
}

impl Target<'_> {
    fn as_dyn(&self) -> &dyn WashTarget {
        match self {
            Target::Day(t) => t,
            Target::Stream(t) => t,
        }
    }
}

struct UnitRt<'a> {
    unit: Unit,
    target: Target<'a>,
    state: Mutex<UnitState>,
    prod: Mutex<Option<Prod>>,
    /// Stream only: RICs already finished by an earlier run.
    done_before: HashSet<String>,
    stream_dir: Option<PathBuf>,
}

struct Queue {
    /// Units not started yet.
    units: VecDeque<usize>,
    jobs: VecDeque<Job>,
    /// Units being read: (unit, a thread is reading a block of it right now).
    active: Vec<(usize, bool)>,
    max_active: usize,
    failed: Option<String>,
}

struct Rt<'a> {
    cfg: &'a StreamConfig,
    units: Vec<UnitRt<'a>>,
    q: Mutex<Queue>,
    cv: Condvar,
    results: Mutex<Vec<Value>>,
    dbs: &'a DbMap,
    /// Washed segment buffers, reused (a fresh 32 MiB buffer costs page faults).
    pool: Mutex<Vec<Vec<u8>>>,
}

impl Rt<'_> {
    fn failed(&self) -> bool {
        self.q.lock().expect("queue lock").failed.is_some()
    }
    fn unit_failed(&self, ui: usize) -> bool {
        self.units[ui]
            .state
            .lock()
            .expect("unit lock")
            .failed
            .is_some()
            || self.failed()
    }
    fn fail_unit(&self, ui: usize, msg: String) {
        let id = self.units[ui].unit.id();
        let mut st = self.units[ui].state.lock().expect("unit lock");
        if st.failed.is_none() {
            let m = format!("{id}: {msg}");
            self.cfg
                .progress
                .errors
                .lock()
                .expect("errors lock")
                .push(m.clone());
            st.failed = Some(m);
        }
        drop(st);
        self.cv.notify_all();
    }
    /// An infrastructure error: everything stops.
    fn fail(&self, msg: String) {
        let mut q = self.q.lock().expect("queue lock");
        if q.failed.is_none() {
            self.cfg
                .progress
                .errors
                .lock()
                .expect("errors lock")
                .push(msg.clone());
            q.failed = Some(msg);
        }
        self.cv.notify_all();
    }
}

fn cursor_path(dir: &Path) -> PathBuf {
    dir.join("cursor.json")
}

fn write_json_atomic(path: &Path, v: &Value) -> Result<()> {
    let tmp = path.with_extension("tmp");
    fs::write(&tmp, serde_json::to_vec_pretty(v)?)?;
    fs::rename(&tmp, path)?;
    Ok(())
}

/// Stream units only: resume point = first part that still can hold an
/// unfinished RIC, from the cursor of the earlier run.
fn start_part(unit: &Unit, dir: &Path) -> usize {
    let UnitKind::Stream { parts, .. } = &unit.kind else {
        return 0;
    };
    let Ok(text) = fs::read_to_string(cursor_path(dir)) else {
        return 0;
    };
    let Ok(v) = serde_json::from_str::<Value>(&text) else {
        return 0;
    };
    let Some(frontier) = v["frontier"].as_str() else {
        return 0;
    };
    let firsts: Vec<String> = match parts.iter().map(|p| first_ric_of_part(p)).collect() {
        Ok(f) => f,
        Err(_) => return 0,
    };
    if firsts.windows(2).any(|w| w[0] > w[1]) {
        return 0; // not sorted by RIC: no part skipping
    }
    firsts
        .iter()
        .rposition(|f| f.as_str() < frontier)
        .unwrap_or(0)
}

/// Tracks, for the RIC being cut into segments, the non-empty seconds of the
/// current UTC day and tells where the sequential writer has a checkpoint.
#[derive(Default)]
struct Checkpoints {
    last_sec: [u8; 19],
    have: bool,
    nonempty: u64,
}

impl Checkpoints {
    /// `line` is a data row of the RIC (`ric_len` bytes before the first comma).
    /// True if this row starts a self-contained record of the sequential writer.
    fn is_checkpoint(&mut self, line: &[u8], ric_len: usize) -> Result<bool> {
        let c1 = ric_len + 1 + memchr::memchr(b',', &line[ric_len + 1..]).context("short row")?;
        let start = c1 + 1;
        ensure!(line.len() >= start + 19, "bad Date-Time in row");
        let sec = &line[start..start + 19];
        if self.have && sec == self.last_sec {
            return Ok(false);
        }
        let day_changed = !self.have || sec[..10] != self.last_sec[..10];
        self.nonempty = if day_changed { 1 } else { self.nonempty + 1 };
        self.last_sec.copy_from_slice(sec);
        self.have = true;
        Ok(day_changed || (self.nonempty - 1).is_multiple_of(CHECKPOINT))
    }
}

fn new_seg(rt: &Rt) -> Vec<u8> {
    rt.pool
        .lock()
        .expect("pool lock")
        .pop()
        .unwrap_or_else(|| Vec::with_capacity(rt.cfg.segment_bytes + rt.cfg.block_bytes))
}

fn enqueue(rt: &Rt, ui: usize, ric_idx: usize, seg: u32, ric: &str, data: Vec<u8>) -> Result<()> {
    {
        let mut st = rt.units[ui].state.lock().expect("unit lock");
        st.rics[ric_idx].created += 1;
        st.jobs_created += 1;
    }
    let mut q = rt.q.lock().expect("queue lock");
    q.jobs.push_back(Job {
        unit: ui,
        ric_idx,
        seg,
        ric: ric.to_string(),
        data,
    });
    drop(q);
    rt.cv.notify_one();
    Ok(())
}

fn close_ric(rt: &Rt, ui: usize, ric_idx: usize) -> Result<()> {
    rt.units[ui].state.lock().expect("unit lock").rics[ric_idx].closed = true;
    try_finish_ric(rt, ui, ric_idx)
}

/// Reading state of one unit: a thread takes it for one block at a time.
struct Prod {
    rd: PartReader,
    block: Vec<u8>,
    seg: Vec<u8>,
    cur: Vec<u8>,
    active: bool,
    ric_idx: usize,
    seg_no: u32,
    cps: Checkpoints,
    seen: HashSet<Vec<u8>>,
}

fn init_prod(rt: &Rt, ui: usize) -> Prod {
    let cfg = rt.cfg;
    let u = &rt.units[ui];
    let (parts, start) = match &u.unit.kind {
        UnitKind::Daily { product, day } => (
            vec![cfg
                .raw_root
                .join(product)
                .join(format!("{product}_{day}_normll2.csv.gz"))],
            0,
        ),
        UnitKind::Stream { parts, .. } => (
            parts.clone(),
            u.stream_dir
                .as_deref()
                .map_or(0, |d| start_part(&u.unit, d)),
        ),
    };
    u.state.lock().expect("unit lock").started = Some(Instant::now());
    Prod {
        rd: PartReader::new(
            parts,
            start,
            Arc::clone(&cfg.progress),
            cfg.raw_ll2.clone(),
            cfg.raw_segments,
        ),
        block: Vec::with_capacity(cfg.block_bytes + (1 << 20)),
        seg: Vec::new(),
        cur: Vec::new(),
        active: false,
        ric_idx: 0,
        seg_no: 0,
        cps: Checkpoints::default(),
        seen: HashSet::new(),
    }
}

/// Reads and cuts ONE block of the unit; true when the unit is read to its end
/// (and finalized if everything is washed).
fn produce_step(rt: &Rt, ui: usize, p: &mut Prod) -> Result<bool> {
    let cfg = rt.cfg;
    let u = &rt.units[ui];
    (cfg.gate)();
    if rt.unit_failed(ui) {
        bail!("aborted: another task failed");
    }
    let Prod {
        rd,
        block,
        seg,
        cur,
        active,
        ric_idx,
        seg_no,
        cps,
        seen,
    } = p;
    if !rd.read_block(block, cfg.block_bytes)? {
        if *active {
            let name = String::from_utf8_lossy(cur).into_owned();
            enqueue(
                rt,
                ui,
                *ric_idx,
                *seg_no,
                &name,
                std::mem::replace(seg, new_seg(rt)),
            )?;
            close_ric(rt, ui, *ric_idx)?;
            *active = false;
        }
        u.state.lock().expect("unit lock").producer_done = true;
        try_finalize(rt, ui)?;
        return Ok(true);
    }
    let (mut run_start, mut pos) = (0usize, 0usize);
    while pos < block.len() {
        let eol = memchr::memchr(b'\n', &block[pos..]).map_or(block.len(), |i| pos + i);
        let line = &block[pos..eol];
        let line_start = pos;
        pos = (eol + 1).min(block.len());
        if line.is_empty() || line == b"\r" {
            continue;
        }
        let c0 = memchr::memchr(b',', line).unwrap_or(line.len());
        let ric = &line[..c0];
        ensure!(!ric.is_empty(), "empty RIC");
        if ric != cur.as_slice() {
            if *active {
                seg.extend_from_slice(&block[run_start..line_start]);
                let name = String::from_utf8_lossy(cur).into_owned();
                enqueue(
                    rt,
                    ui,
                    *ric_idx,
                    *seg_no,
                    &name,
                    std::mem::replace(seg, new_seg(rt)),
                )?;
                close_ric(rt, ui, *ric_idx)?;
            }
            run_start = line_start;
            ensure!(
                seen.insert(ric.to_vec()),
                "RIC {} appears in more than one block of the stream",
                String::from_utf8_lossy(ric)
            );
            cur.clear();
            cur.extend_from_slice(ric);
            let ric_s = String::from_utf8_lossy(ric).into_owned();
            *active = !u.done_before.contains(&ric_s);
            if *active {
                u.target.as_dyn().clean_ric(&ric_s)?;
                let mut st = u.state.lock().expect("unit lock");
                st.rics.push(RicAgg {
                    ric: ric_s,
                    ..Default::default()
                });
                *ric_idx = st.rics.len() - 1;
                *seg_no = 0;
                seg.clear();
                *cps = Checkpoints::default();
            } else {
                u.state.lock().expect("unit lock").skipped_rics += 1;
            }
        }
        if *active {
            let cp = cps.is_checkpoint(line, c0)?;
            let have = seg.len() + (line_start - run_start);
            if cp && have >= cfg.segment_bytes {
                seg.extend_from_slice(&block[run_start..line_start]);
                run_start = line_start;
                let name = String::from_utf8_lossy(cur).into_owned();
                enqueue(
                    rt,
                    ui,
                    *ric_idx,
                    *seg_no,
                    &name,
                    std::mem::replace(seg, new_seg(rt)),
                )?;
                *seg_no += 1;
            }
        }
    }
    if *active {
        seg.extend_from_slice(&block[run_start..]);
    }
    Ok(false)
}

fn count_r_markers(db: &DB) -> Result<usize> {
    let mut n = 0;
    let mut it = db.iterator(rocksdb::IteratorMode::From(
        b"R|",
        rocksdb::Direction::Forward,
    ));
    for item in it.by_ref() {
        let (k, _) = item?;
        if !k.starts_with(b"R|") {
            break;
        }
        n += 1;
    }
    Ok(n)
}

fn finalize_marker(rt: &Rt, ui: usize, st: &mut UnitState) -> Result<Value> {
    let u = &rt.units[ui];
    let secs = st.started.map_or(0.0, |t| t.elapsed().as_secs_f64());
    let mut months: BTreeMap<u32, u64> = BTreeMap::new();
    let mut rics: Vec<WashedRic> = Vec::new();
    for o in &st.outcomes {
        for (m, n) in &o.months {
            *months.entry(*m).or_default() += n;
        }
        rics.push(o.washed.clone());
    }
    rics.sort_by(|a, b| a.ric.cmp(&b.ric));
    let sum = |g: fn(&WashedRic) -> u64| rics.iter().map(g).sum::<u64>();
    let r = |x: f64| (x * 100.0).round() / 100.0;
    let mut v = json!({
        "unit": u.unit.id(), "status": "washed", "rics": rics.len(),
        "skipped_rics": st.skipped_rics, "updates": sum(|x| x.updates),
        "key_bytes": sum(|x| x.key_bytes), "value_bytes": sum(|x| x.value_bytes),
        "sst_bytes": sum(|x| x.sst_bytes), "input_gz_bytes": u.unit.gz_bytes,
        "seconds": r(secs), "months": months,
        "first_ts": rics.iter().map(|x| x.first_ts).min(), "last_ts": rics.iter().map(|x| x.last_ts).max(),
    });
    match (&u.unit.kind, &u.target) {
        (UnitKind::Daily { product, day }, Target::Day(t)) => {
            let db = day_db(t.root, t.dbs, day)?;
            l2_db::write_marker(&db, product, day, &rics)?;
            v["product"] = json!(product);
            v["day"] = json!(day);
        }
        (UnitKind::Stream { id, .. }, Target::Stream(t)) => {
            // RICs finished by earlier runs of this unit are counted from their R markers
            v["rics_total_in_db"] = json!(count_r_markers(&t.db)?);
            t.db.flush()?;
            let mut wo = WriteOptions::default();
            wo.set_sync(true);
            t.db.put_opt(l2_db::marker_key(id), serde_json::to_vec(&v)?, &wo)?;
            if let Some(d) = &u.stream_dir {
                write_json_atomic(
                    &cursor_path(d),
                    &json!({"frontier": null, "finished": true}),
                )?;
            }
        }
        _ => bail!("target does not match the unit"),
    }
    Ok(v)
}

fn try_finalize(rt: &Rt, ui: usize) -> Result<()> {
    let u = &rt.units[ui];
    let mut st = u.state.lock().expect("unit lock");
    // A consumer counts its job done and only then (outside the lock) finishes the
    // RIC, so "every job done" alone can be seen while a RIC is still unfinished:
    // the marker would miss it. Wait for every RIC too (that consumer calls us again).
    if st.finished
        || !st.producer_done
        || st.jobs_done != st.jobs_created
        || (st.failed.is_none() && st.rics.iter().any(|a| !a.finished))
    {
        return Ok(());
    }
    st.finished = true;
    let id = u.unit.id();
    let p = &rt.cfg.progress;
    if let Some(e) = st.failed.clone() {
        let v = json!({"unit": id, "status": "failed", "error": e,
                       "rics_done": st.outcomes.len()});
        println!("{v}");
        rt.results.lock().expect("results lock").push(v);
        drop(st);
        p.current.lock().expect("current lock").retain(|c| *c != id);
        return Ok(());
    }
    let v = finalize_marker(rt, ui, &mut st)?;
    println!("{v}");
    rt.results.lock().expect("results lock").push(v);
    drop(st);
    p.units_done.fetch_add(1, Ordering::Relaxed);
    p.gz_bytes_done_units
        .fetch_add(u.unit.gz_bytes, Ordering::Relaxed);
    p.current.lock().expect("current lock").retain(|c| *c != id);
    Ok(())
}

/// When every segment of a closed RIC is washed: merge, mark, advance the cursor.
fn try_finish_ric(rt: &Rt, ui: usize, ric_idx: usize) -> Result<()> {
    let u = &rt.units[ui];
    let cursor = {
        let mut st = u.state.lock().expect("unit lock");
        let a = &mut st.rics[ric_idx];
        if a.finished || !a.closed || a.done != a.created {
            return Ok(());
        }
        a.finished = true;
        let ric = a.ric.clone();
        let parts = std::mem::take(&mut a.parts);
        let outcome = match l2_db::merge_segments(parts) {
            Ok(o) => o,
            Err(e) => {
                drop(st);
                rt.fail_unit(ui, format!("RIC {ric}: {e:#}"));
                return Ok(());
            }
        };
        if let Target::Stream(t) = &u.target {
            let w = &outcome.washed;
            let v = json!({"updates": w.updates, "first_ts": w.first_ts, "last_ts": w.last_ts,
                           "months": outcome.months, "value_bytes": w.value_bytes,
                           "crc_sum": w.crc_sum,
                           "days": outcome.days.iter().map(|(d, x)| json!([d, x.count, x.first_ts, x.last_ts, x.crc_sum])).collect::<Vec<_>>(),
                           "key_bytes": w.key_bytes, "sst_bytes": w.sst_bytes});
            let mut key = b"R|".to_vec();
            key.extend_from_slice(ric.as_bytes());
            t.db.put(key, serde_json::to_vec(&v)?)?;
        }
        st.outcomes.push(outcome);
        while st.first_unfinished < st.rics.len() && st.rics[st.first_unfinished].finished {
            st.first_unfinished += 1;
        }
        let due = st
            .cursor_written
            .is_none_or(|t| t.elapsed() > Duration::from_secs(20));
        if due && u.stream_dir.is_some() {
            st.cursor_written = Some(Instant::now());
            Some(st.rics.get(st.first_unfinished).map(|a| a.ric.clone()))
        } else {
            None
        }
    };
    if let (Some(frontier), Some(d)) = (cursor, &u.stream_dir) {
        write_json_atomic(
            &cursor_path(d),
            &json!({"frontier": frontier, "finished": false}),
        )?;
    }
    Ok(())
}

fn consume(rt: &Rt, job: Job) -> Result<()> {
    let u = &rt.units[job.unit];
    if rt.unit_failed(job.unit) {
        u.state.lock().expect("unit lock").jobs_done += 1;
        return try_finalize(rt, job.unit);
    }
    let res = l2_db::wash_segment(u.target.as_dyn(), &job.ric, job.seg, &job.data);
    if let Ok(o) = &res {
        rt.cfg
            .progress
            .rows
            .fetch_add(o.washed.updates, Ordering::Relaxed);
    }
    let mut data = job.data;
    data.clear();
    {
        let mut pool = rt.pool.lock().expect("pool lock");
        if pool.len() < 4 {
            pool.push(data);
        }
    }
    match res {
        Ok(outcome) => {
            {
                let mut st = u.state.lock().expect("unit lock");
                let a = &mut st.rics[job.ric_idx];
                a.parts.push((job.seg, outcome));
                a.done += 1;
                st.jobs_done += 1;
            }
            try_finish_ric(rt, job.unit, job.ric_idx)?;
        }
        Err(e) => {
            rt.fail_unit(job.unit, format!("RIC {}: {e:#}", job.ric));
            u.state.lock().expect("unit lock").jobs_done += 1;
        }
    }
    try_finalize(rt, job.unit)
}

/// Every thread washes a queued segment if there is one; otherwise it reads the
/// next block of an active unit (at most `max_active` units are read at a time,
/// one thread per unit) or starts a new unit. Reading is cheap next to washing,
/// so the threads settle into the mix the work needs.
fn worker(rt: &Rt) {
    enum Task {
        Job(Job),
        Step(usize, bool),
    }
    loop {
        let task = {
            let mut q = rt.q.lock().expect("queue lock");
            loop {
                if q.failed.is_some() {
                    break None;
                }
                if let Some(j) = q.jobs.pop_front() {
                    break Some(Task::Job(j));
                }
                if let Some(i) = q.active.iter().position(|a| !a.1) {
                    q.active[i].1 = true;
                    break Some(Task::Step(q.active[i].0, false));
                }
                if q.active.len() < q.max_active {
                    if let Some(ui) = q.units.pop_front() {
                        q.active.push((ui, true));
                        break Some(Task::Step(ui, true));
                    }
                }
                if q.active.is_empty() && q.units.is_empty() {
                    break None;
                }
                q = rt.cv.wait(q).expect("queue cv");
            }
        };
        let Some(task) = task else {
            rt.cv.notify_all();
            return;
        };
        let r = match task {
            Task::Job(j) => consume(rt, j),
            Task::Step(ui, first) => {
                let u = &rt.units[ui];
                let mut prod = u.prod.lock().expect("prod lock").take();
                if first {
                    rt.cfg
                        .progress
                        .current
                        .lock()
                        .expect("current lock")
                        .push(u.unit.id());
                }
                let step = match prod.as_mut() {
                    Some(p) => produce_step(rt, ui, p),
                    None => {
                        let mut p = init_prod(rt, ui);
                        let r = produce_step(rt, ui, &mut p);
                        prod = Some(p);
                        r
                    }
                };
                let (done, r) = match step {
                    Ok(done) => (done, Ok(())),
                    Err(e) => {
                        // the unit fails, the others go on
                        rt.fail_unit(ui, format!("{e:#}"));
                        u.state.lock().expect("unit lock").producer_done = true;
                        (true, try_finalize(rt, ui))
                    }
                };
                if done {
                    drop(prod);
                } else {
                    *u.prod.lock().expect("prod lock") = prod;
                }
                let mut q = rt.q.lock().expect("queue lock");
                if done {
                    q.active.retain(|a| a.0 != ui);
                } else if let Some(a) = q.active.iter_mut().find(|a| a.0 == ui) {
                    a.1 = false;
                }
                drop(q);
                rt.cv.notify_all();
                r
            }
        };
        if let Err(e) = r {
            rt.fail(format!("{e:#}"));
        }
    }
}

/// Runs `units` through the pipeline. Units that already have their completion
/// marker are reported as `kept_existing` (unless `force`); missing source
/// files as `missing_source`.
pub fn run_stream(cfg: &StreamConfig, units: Vec<Unit>) -> Result<StreamReport> {
    ensure!(cfg.workers >= 1, "workers must be >= 1");
    let producers = cfg.producers.clamp(1, cfg.workers);
    let dbs: DbMap = Mutex::new(HashMap::new());
    let mut rts: Vec<UnitRt> = Vec::new();
    let mut results = Vec::new();
    for unit in units {
        let rt_unit = match &unit.kind {
            UnitKind::Daily { product, day } => {
                let path = cfg
                    .raw_root
                    .join(product)
                    .join(format!("{product}_{day}_normll2.csv.gz"));
                if !path.exists() {
                    let v = json!({"unit": unit.id(), "status": "missing_source"});
                    println!("{v}");
                    results.push(v);
                    continue;
                }
                let dir = l2_db::day_dir(&cfg.db_root, day);
                if !cfg.force && dir.join("CURRENT").exists() {
                    let db = day_db(&cfg.db_root, &dbs, day)?;
                    if l2_db::read_marker(&db, product)?.is_some() {
                        let v = json!({"unit": unit.id(), "status": "kept_existing"});
                        println!("{v}");
                        results.push(v);
                        continue;
                    }
                }
                let dn = day_number(day)?;
                UnitRt {
                    unit,
                    target: Target::Day(DayTarget {
                        root: &cfg.db_root,
                        dbs: &dbs,
                        day: dn,
                    }),
                    state: Mutex::new(UnitState::default()),
                    prod: Mutex::new(None),
                    done_before: HashSet::new(),
                    stream_dir: None,
                }
            }
            UnitKind::Stream { id, parts } => {
                for p in parts {
                    ensure!(p.exists(), "missing part {}", p.display());
                }
                let dir = cfg.stream_db_root.join(id);
                let db = Arc::new(l2_db::open_write(&dir)?);
                if !cfg.force && db.get(l2_db::marker_key(id))?.is_some() {
                    let v = json!({"unit": unit.id(), "status": "kept_existing"});
                    println!("{v}");
                    results.push(v);
                    continue;
                }
                let mut done_before = HashSet::new();
                if !cfg.force {
                    let mut it = db.iterator(rocksdb::IteratorMode::From(
                        b"R|",
                        rocksdb::Direction::Forward,
                    ));
                    for item in it.by_ref() {
                        let (k, _) = item?;
                        if !k.starts_with(b"R|") {
                            break;
                        }
                        done_before.insert(String::from_utf8_lossy(&k[2..]).into_owned());
                    }
                }
                let tmp = dir.join("ingest_tmp");
                UnitRt {
                    unit,
                    target: Target::Stream(StreamTarget { db, tmp }),
                    state: Mutex::new(UnitState::default()),
                    prod: Mutex::new(None),
                    done_before,
                    stream_dir: Some(dir),
                }
            }
        };
        rts.push(rt_unit);
    }
    // largest source first
    let mut order: Vec<usize> = (0..rts.len()).collect();
    order.sort_by(|&a, &b| rts[b].unit.gz_bytes.cmp(&rts[a].unit.gz_bytes));
    cfg.progress.units_total.store(rts.len(), Ordering::Relaxed);
    cfg.progress
        .gz_bytes_total
        .store(rts.iter().map(|r| r.unit.gz_bytes).sum(), Ordering::Relaxed);
    let rt = Rt {
        cfg,
        units: rts,
        q: Mutex::new(Queue {
            units: order.into(),
            jobs: VecDeque::new(),
            active: Vec::new(),
            max_active: producers,
            failed: None,
        }),
        cv: Condvar::new(),
        results: Mutex::new(results),
        dbs: &dbs,
        pool: Mutex::new(Vec::new()),
    };
    std::thread::scope(|scope| {
        for _ in 0..cfg.workers {
            let rt = &rt;
            scope.spawn(move || worker(rt));
        }
    });
    let failed = rt.q.lock().expect("queue lock").failed.take();
    let results = std::mem::take(&mut *rt.results.lock().expect("results lock"));
    let mut db_sst_bytes = BTreeMap::new();
    for (day, db) in rt.dbs.lock().expect("dbs lock").iter() {
        let b = db
            .property_int_value("rocksdb.total-sst-files-size")
            .ok()
            .flatten()
            .unwrap_or(0);
        db_sst_bytes.insert(day.clone(), b);
        // nothing of ours stays in the temp dir (it is empty after a clean run)
        let _ = fs::remove_dir(l2_db::day_dir(&cfg.db_root, day).join("ingest_tmp"));
    }
    for u in &rt.units {
        if let Target::Stream(t) = &u.target {
            let b =
                t.db.property_int_value("rocksdb.total-sst-files-size")
                    .ok()
                    .flatten()
                    .unwrap_or(0);
            db_sst_bytes.insert(u.unit.id(), b);
            let _ = fs::remove_dir_all(&t.tmp);
        }
    }
    Ok(StreamReport {
        results,
        failed,
        db_sst_bytes,
    })
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::l2_extract::{run_extract, run_extract_rics, ExtractConfig};
    use crate::l2_slice::{slice_stream, Layout, Params, ParquetSink, CSV_COLUMNS, DAY_NS};
    use flate2::{write::GzEncoder, Compression};
    use std::io::Write;

    const D1: &str = "2024-10-04";
    const D2: &str = "2024-10-05";
    const F: i64 = 250_000_000;
    const SEC: i64 = 1_000_000_000;

    fn day0(d: &str) -> i64 {
        day_number(d).unwrap() * DAY_NS
    }
    fn ts_text(d: &str, off_ns: i64) -> String {
        let t = day0(d) + off_ns;
        let dt = chrono::DateTime::from_timestamp(t.div_euclid(1_000_000_000), 0).unwrap();
        format!(
            "{}.{:09}Z",
            dt.format("%Y-%m-%dT%H:%M:%S"),
            t.rem_euclid(1_000_000_000)
        )
    }
    fn row(ric: &str, d: &str, off: i64, bid: f64, size: u32, ask: f64) -> String {
        let mut f = vec![String::new(); CSV_COLUMNS];
        f[0] = ric.into();
        f[1] = "Market Price".into();
        f[2] = ts_text(d, off);
        f[3] = "-6".into();
        f[4] = "Normalized LL2".into();
        f[5] = format!("{bid}");
        f[6] = format!("{size}");
        f[7] = "1".into();
        f[8] = format!("{ask}");
        f[9] = "5".into();
        f[10] = "2".into();
        f[CSV_COLUMNS - 1] = "00:00:00.000000000".into();
        f.join(",")
    }
    /// (day, row text) of three RICs over two UTC days; RICA has ties, a long
    /// silence, more than 256 non-empty seconds (so there are checkpoints inside
    /// the day) and runs into the second day; RICB and RICC only on the second.
    fn rows() -> Vec<(&'static str, String)> {
        let mut v = Vec::new();
        for k in 1..=40i64 {
            v.push((
                D1,
                row(
                    "RICA",
                    D1,
                    k * F / 2,
                    100.0 + (k % 5) as f64 * 0.25,
                    1 + (k % 3) as u32,
                    101.0,
                ),
            ));
        }
        v.push((D1, row("RICA", D1, 40 * F, 100.0, 1, 101.0)));
        v.push((D1, row("RICA", D1, 40 * F, 100.5, 2, 101.0)));
        v.push((
            D1,
            row("RICA", D1, 40 * F + 1_800_000_000_000, 100.5, 2, 101.0),
        ));
        // 700 consecutive non-empty seconds, two rows in most of them
        for k in 1..=700i64 {
            let base = 2_000 * SEC + k * SEC;
            v.push((
                D1,
                row(
                    "RICA",
                    D1,
                    base,
                    101.0 + (k % 7) as f64 * 0.25,
                    1 + (k % 5) as u32,
                    103.0,
                ),
            ));
            if k % 3 != 0 {
                v.push((
                    D1,
                    row("RICA", D1, base + F, 101.0 + (k % 4) as f64 * 0.5, 2, 103.5),
                ));
            }
        }
        for k in 1..=30i64 {
            v.push((
                D2,
                row(
                    "RICA",
                    D2,
                    k * F / 3,
                    99.0 + (k % 4) as f64 * 0.5,
                    2 + (k % 2) as u32,
                    102.0,
                ),
            ));
        }
        for k in 1..=7i64 {
            v.push((D2, row("RICB", D2, k * F, 5.0 + k as f64, 3, 9.0)));
        }
        v.push((D2, row("RICC", D2, 3 * F, 1.0, 1, 2.0)));
        v
    }
    fn text(header: bool, rows: &[&str]) -> Vec<u8> {
        let mut s = String::new();
        if header {
            s.push_str(&expected_header());
            s.push('\n');
        }
        for r in rows {
            s.push_str(r);
            s.push('\n');
        }
        s.into_bytes()
    }
    fn gz(data: &[u8]) -> Vec<u8> {
        let mut e = GzEncoder::new(Vec::new(), Compression::fast());
        e.write_all(data).unwrap();
        e.finish().unwrap()
    }
    fn tree(root: &Path) -> Vec<(String, String)> {
        let mut v = Vec::new();
        let mut stack = vec![root.to_path_buf()];
        while let Some(d) = stack.pop() {
            for e in fs::read_dir(d).unwrap().flatten() {
                let p = e.path();
                if p.is_dir() {
                    stack.push(p);
                } else {
                    v.push((
                        p.strip_prefix(root).unwrap().display().to_string(),
                        crate::tasdb::file_sha256(&p).unwrap(),
                    ));
                }
            }
        }
        v.sort();
        v
    }
    /// Direct slices of the rows of one day.
    fn direct(t: &Path, name: &str, d: &str, rows: &[&str], params: Params) -> PathBuf {
        let out = t.join(name);
        let layout = Layout {
            out_root: out.clone(),
            exchange_of: Arc::new(BTreeMap::new()),
            default_exchange: Some("fx".into()),
            day: d.into(),
            force: false,
        };
        let mut sink = ParquetSink::new(&layout, params, day0(d));
        slice_stream(
            std::io::Cursor::new(text(true, rows)),
            day0(d),
            &params,
            &mut sink,
        )
        .unwrap();
        out
    }
    fn cfg(t: &Path, workers: usize, producers: usize, segment_bytes: usize) -> StreamConfig {
        StreamConfig {
            raw_root: t.join("raw"),
            db_root: t.join("db"),
            stream_db_root: t.join("sdb"),
            workers,
            producers,
            force: false,
            segment_bytes,
            block_bytes: 4_097, // cuts lines in the middle: exercises the carry
            gate: Arc::new(|| {}),
            progress: Arc::new(Progress::default()),
            raw_ll2: None,
            raw_segments: false,
        }
    }
    fn ecfg(t: &Path, db_root: PathBuf, out: &str, params: Params) -> ExtractConfig {
        ExtractConfig {
            db_root,
            out_root: t.join(out),
            exchange_of: Arc::new(BTreeMap::new()),
            default_exchange: Some("fx".into()),
            params,
            force: false,
            workers: 2,
            cache_mb: 16,
        }
    }
    fn dump(db: &DB) -> Vec<(Vec<u8>, Vec<u8>)> {
        let mut v = Vec::new();
        for item in db.iterator(rocksdb::IteratorMode::Start) {
            let (k, val) = item.unwrap();
            if k.starts_with(b"U") {
                v.push((k.to_vec(), val.to_vec()));
            }
        }
        v
    }

    #[test]
    fn daily_units_washed_in_memory_equal_the_direct_slices() {
        let t = tempfile::tempdir().unwrap();
        let params = Params::new(250, 1800).unwrap();
        let all = rows();
        let d1: Vec<&str> = all
            .iter()
            .filter(|r| r.0 == D1)
            .map(|r| r.1.as_str())
            .collect();
        let reference = tree(&direct(t.path(), "direct", D1, &d1, params));
        assert_eq!(reference.len(), 1);
        fs::create_dir_all(t.path().join("raw/FIX")).unwrap();
        fs::write(
            t.path().join(format!("raw/FIX/FIX_{D1}_normll2.csv.gz")),
            gz(&text(true, &d1)),
        )
        .unwrap();
        for (workers, producers, seg) in [(2usize, 1usize, 1usize << 30), (3, 1, 600), (8, 2, 1)] {
            let c = cfg(t.path(), workers, producers, seg);
            let _ = fs::remove_dir_all(&c.db_root);
            let unit = Unit {
                kind: UnitKind::Daily {
                    product: "FIX".into(),
                    day: D1.into(),
                },
                gz_bytes: 1,
            };
            let missing = Unit {
                kind: UnitKind::Daily {
                    product: "NOPE".into(),
                    day: D1.into(),
                },
                gz_bytes: 1,
            };
            let rep = run_stream(&c, vec![unit.clone(), missing]).unwrap();
            assert!(rep.failed.is_none(), "{:?}", rep.failed);
            assert!(rep
                .results
                .iter()
                .any(|v| v["status"] == "washed" && v["updates"] == d1.len() as u64));
            assert!(rep.results.iter().any(|v| v["status"] == "missing_source"));
            let again = run_stream(&c, vec![unit]).unwrap();
            assert!(again.results.iter().all(|v| v["status"] == "kept_existing"));
            let out = format!("out{workers}");
            let e = run_extract(
                &ecfg(t.path(), c.db_root.clone(), &out, params),
                vec![("FIX".into(), D1.into())],
            )
            .unwrap();
            assert!(e.failed.is_none(), "{:?}", e.failed);
            assert_eq!(
                tree(&t.path().join(&out)),
                reference,
                "workers={workers} seg={seg}"
            );
            assert!(!l2_db::day_dir(&c.db_root, D1).join("ingest_tmp").exists());
        }
    }

    /// The claim behind the parallelism: any segmentation stores exactly the
    /// bytes of a sequential pass.
    #[test]
    fn any_segmentation_stores_the_bytes_of_a_sequential_pass() {
        let t = tempfile::tempdir().unwrap();
        let all = rows();
        let ordered: Vec<&str> = all.iter().map(|r| r.1.as_str()).collect();
        fs::create_dir_all(t.path().join("raw")).unwrap();
        let p0 = t.path().join("raw/only.csv.gz");
        fs::write(&p0, gz(&text(true, &ordered))).unwrap();
        let mut dumps = Vec::new();
        for (workers, seg) in [(2usize, 1usize << 30), (4, 40_000), (8, 3_000), (8, 1)] {
            let mut c = cfg(t.path(), workers, 1, seg);
            c.stream_db_root = t.path().join(format!("sdb{workers}_{seg}"));
            let unit = Unit {
                kind: UnitKind::Stream {
                    id: "pkg".into(),
                    parts: vec![p0.clone()],
                },
                gz_bytes: 1,
            };
            let rep = run_stream(&c, vec![unit]).unwrap();
            assert!(rep.failed.is_none(), "{:?}", rep.failed);
            let w = rep
                .results
                .iter()
                .find(|v| v["status"] == "washed")
                .unwrap();
            assert_eq!(w["updates"], ordered.len() as u64, "{w}");
            let db = l2_db::open_write(&c.stream_db_root.join("pkg")).unwrap();
            dumps.push((seg, dump(&db)));
        }
        for (seg, d) in &dumps[1..] {
            assert_eq!(
                *d, dumps[0].1,
                "segment size {seg} differs from the sequential pass"
            );
        }
        assert!(dumps[0].1.len() > 700, "the test data must span many keys");
    }

    #[test]
    fn a_stream_of_two_parts_with_a_ric_crossing_the_boundary_equals_the_direct_slices() {
        let t = tempfile::tempdir().unwrap();
        let params = Params::new(250, 1800).unwrap();
        let all = rows();
        let by = |d: &str, ric: &str| -> Vec<&str> {
            all.iter()
                .filter(|r| r.0 == d && r.1.starts_with(ric))
                .map(|r| r.1.as_str())
                .collect()
        };
        // reference: every (RIC, day) sliced on its own
        let mut want = Vec::new();
        for (d, rics) in [(D1, vec!["RICA"]), (D2, vec!["RICA", "RICB", "RICC"])] {
            let rows_of_day: Vec<&str> = rics.iter().flat_map(|r| by(d, r)).collect();
            want.extend(tree(&direct(
                t.path(),
                &format!("direct{d}"),
                d,
                &rows_of_day,
                params,
            )));
        }
        want.sort();
        // the stream: RICA (both days, cut in the middle of the second day, between
        // two rows) then RICB, RICC; part 1 has no header line
        let ordered: Vec<&str> = by(D1, "RICA")
            .into_iter()
            .chain(by(D2, "RICA"))
            .chain(by(D2, "RICB"))
            .chain(by(D2, "RICC"))
            .collect();
        let cut = by(D1, "RICA").len() + 11;
        fs::create_dir_all(t.path().join("raw")).unwrap();
        let p0 = t.path().join("raw/part0.csv.gz");
        let p1 = t.path().join("raw/part1.csv.gz");
        fs::write(&p0, gz(&text(true, &ordered[..cut]))).unwrap();
        fs::write(&p1, gz(&text(false, &ordered[cut..]))).unwrap();
        for (workers, producers, seg) in [(2usize, 1usize, 1usize << 30), (4, 2, 5_000)] {
            let c = cfg(t.path(), workers, producers, seg);
            let _ = fs::remove_dir_all(&c.stream_db_root);
            let unit = Unit {
                kind: UnitKind::Stream {
                    id: "pkg".into(),
                    parts: vec![p0.clone(), p1.clone()],
                },
                gz_bytes: 1,
            };
            let rep = run_stream(&c, vec![unit.clone()]).unwrap();
            assert!(rep.failed.is_none(), "{:?}", rep.failed);
            assert!(rep.results.iter().any(|v| v["status"] == "washed"
                && v["rics"] == 3
                && v["updates"] == ordered.len() as u64));
            let again = run_stream(&c, vec![unit]).unwrap();
            assert!(again.results.iter().all(|v| v["status"] == "kept_existing"));
            let out = format!("sout{workers}");
            let jobs = vec![
                ("RICA".to_string(), D1.to_string()),
                ("RICA".to_string(), D2.to_string()),
                ("RICB".to_string(), D2.to_string()),
                ("RICC".to_string(), D2.to_string()),
            ];
            let e = run_extract_rics(
                &ecfg(t.path(), c.db_root.clone(), &out, params),
                &[c.stream_db_root.join("pkg")],
                jobs,
            )
            .unwrap();
            assert!(e.failed.is_none(), "{:?}", e.failed);
            assert_eq!(tree(&t.path().join(&out)), want, "workers={workers}");
        }
    }

    #[test]
    fn an_interrupted_stream_resumes_and_redoes_only_the_missing_rics() {
        let t = tempfile::tempdir().unwrap();
        let params = Params::new(250, 1800).unwrap();
        let all = rows();
        let ordered: Vec<&str> = all.iter().map(|r| r.1.as_str()).collect();
        fs::create_dir_all(t.path().join("raw")).unwrap();
        let p0 = t.path().join("raw/only.csv.gz");
        fs::write(&p0, gz(&text(true, &ordered))).unwrap();
        let c = cfg(t.path(), 3, 1, 4_000);
        let unit = Unit {
            kind: UnitKind::Stream {
                id: "pkg".into(),
                parts: vec![p0.clone()],
            },
            gz_bytes: 1,
        };
        run_stream(&c, vec![unit.clone()]).unwrap();
        // forget the unit marker and one RIC's marker (as after a crash)
        {
            let db = l2_db::open_write(&c.stream_db_root.join("pkg")).unwrap();
            db.delete(l2_db::marker_key("pkg")).unwrap();
            db.delete(b"R|RICB").unwrap();
            db.flush().unwrap();
        }
        let rep = run_stream(&c, vec![unit]).unwrap();
        assert!(rep.failed.is_none(), "{:?}", rep.failed);
        let v = rep
            .results
            .iter()
            .find(|v| v["status"] == "washed")
            .unwrap();
        assert_eq!(v["rics"], 1, "only RICB is redone: {v}");
        assert_eq!(v["skipped_rics"], 2);
        let jobs = vec![
            ("RICB".to_string(), D2.to_string()),
            ("RICA".to_string(), D2.to_string()),
        ];
        let e = run_extract_rics(
            &ecfg(t.path(), c.db_root.clone(), "o", params),
            &[c.stream_db_root.join("pkg")],
            jobs,
        )
        .unwrap();
        assert!(e.failed.is_none(), "{:?}", e.failed);
    }

    #[test]
    fn a_bad_unit_fails_alone_and_gets_no_marker() {
        let t = tempfile::tempdir().unwrap();
        let params = Params::new(250, 1800).unwrap();
        let all = rows();
        let good: Vec<&str> = all
            .iter()
            .filter(|r| r.0 == D1)
            .map(|r| r.1.as_str())
            .collect();
        fs::create_dir_all(t.path().join("raw/GOOD")).unwrap();
        fs::create_dir_all(t.path().join("raw/BAD")).unwrap();
        fs::write(
            t.path().join(format!("raw/GOOD/GOOD_{D1}_normll2.csv.gz")),
            gz(&text(true, &good)),
        )
        .unwrap();
        // time goes backwards inside one RIC, with enough rows to pass several segments
        let mut bad: Vec<String> = (1..=300i64)
            .map(|k| row("RICX", D1, k * F, 1.0, 1, 2.0))
            .collect();
        bad.push(row("RICX", D1, 5 * F, 1.0, 1, 2.0));
        let bad_refs: Vec<&str> = bad.iter().map(|s| s.as_str()).collect();
        fs::write(
            t.path().join(format!("raw/BAD/BAD_{D1}_normll2.csv.gz")),
            gz(&text(true, &bad_refs)),
        )
        .unwrap();
        let c = cfg(t.path(), 3, 1, 2_000);
        let units = ["GOOD", "BAD"].map(|p| Unit {
            kind: UnitKind::Daily {
                product: p.into(),
                day: D1.into(),
            },
            gz_bytes: 1,
        });
        let rep = run_stream(&c, units.to_vec()).unwrap();
        assert!(rep.failed.is_none(), "{:?}", rep.failed);
        assert!(rep
            .results
            .iter()
            .any(|v| v["unit"] == "GOOD:2024-10-04" && v["status"] == "washed"));
        let bad = rep
            .results
            .iter()
            .find(|v| v["unit"] == "BAD:2024-10-04")
            .unwrap();
        assert_eq!(bad["status"], "failed", "{bad}");
        assert!(
            bad["error"].as_str().unwrap().contains("goes backwards"),
            "{bad}"
        );
        let e = run_extract(
            &ecfg(t.path(), c.db_root.clone(), "o", params),
            vec![("GOOD".into(), D1.into()), ("BAD".into(), D1.into())],
        )
        .unwrap();
        assert!(e.failed.is_none());
        assert!(e
            .results
            .iter()
            .any(|v| v["product"] == "GOOD" && v["status"] == "ok"));
        assert!(e
            .results
            .iter()
            .any(|v| v["product"] == "BAD" && v["status"] == "not_washed"));
    }

    #[test]
    fn checkpoints_follow_the_sequential_writer() {
        let mut cps = Checkpoints::default();
        let line = |d: &str, s: i64| row("R", d, s * SEC, 1.0, 1, 2.0);
        let mut cp_seconds = Vec::new();
        for s in 0..600i64 {
            for _ in 0..2 {
                if cps.is_checkpoint(line(D1, s).as_bytes(), 1).unwrap() {
                    cp_seconds.push(s);
                }
            }
        }
        assert_eq!(
            cp_seconds,
            [0, 256, 512],
            "first row of the day, then every 256th non-empty second"
        );
        // a day change is a checkpoint and restarts the count
        assert!(cps.is_checkpoint(line(D2, 5).as_bytes(), 1).unwrap());
        assert!(!cps.is_checkpoint(line(D2, 6).as_bytes(), 1).unwrap());
    }
}
