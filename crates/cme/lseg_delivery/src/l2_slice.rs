//! Fixed-frequency downsampling of the LSEG Normalized LL2 daily packages.
//!
//! Source: `<PRODUCT>_<YYYY-MM-DD>_normll2.csv.gz`, one UTC day, the rows of one
//! RIC contiguous and in non-decreasing `Date-Time`; every row is the complete
//! L1..L10 book after one update (empty field = no level / no value).
//!
//! Rules (all deterministic, see `docs/lseg/l2_slice.md`):
//! * Grid: `T = k * freq`, anchored at the UTC day start. Right-closed as-of:
//!   the book at `T` is the last update with `Date-Time <= T`; an update exactly
//!   on `T` belongs to `T`. Equal timestamps keep file order (the last wins).
//! * Downsampling only: a row is emitted for a grid point `T` that has at least
//!   one update in `(T - freq, T]` and whose book (all 60 price/size/order-count
//!   fields) differs from the previously emitted row of that RIC-day. Nothing
//!   is forward filled, so emitted rows are at least `freq` apart.
//! * Anchors, emitted even if the book equals the previous row: the first row of
//!   the RIC-day and the first row after a silence of at least `gap` (a session
//!   start; the source has no status marker, see the doc).
//! * An update belongs to the UTC day of its source file; its grid point may be
//!   the next midnight (`T == day end`, inclusive upper bound).
use anyhow::{bail, ensure, Context, Result};
use arrow_array::{
    builder::{BooleanBuilder, Float64Builder, StringBuilder, UInt32Builder, UInt64Builder},
    ArrayRef, RecordBatch,
};
use arrow_schema::{DataType, Field, Schema};
use parquet::{
    arrow::ArrowWriter,
    basic::{Compression, ZstdLevel},
    file::{
        properties::{EnabledStatistics, WriterProperties},
        reader::FileReader,
        serialized_reader::SerializedFileReader,
        statistics::Statistics,
    },
    format::KeyValue,
};
use std::{
    collections::{BTreeMap, HashSet},
    fs::{self, File},
    io::{BufRead, Write},
    path::{Path, PathBuf},
    sync::Arc,
};

pub const DAY_NS: i64 = 86_400_000_000_000;
pub const LEVELS: usize = 10;
/// Per level: bid price, bid size, bid orders, ask price, ask size, ask orders
/// (the source column order).
pub const BOOK_FIELDS: usize = LEVELS * 6;
pub const CSV_COLUMNS: usize = 5 + BOOK_FIELDS + 1;
const ROW_GROUP_ROWS: usize = 65_536;
const FIELD_NAMES: [&str; 6] = [
    "bid",
    "bid_size",
    "bid_orders",
    "ask",
    "ask_size",
    "ask_orders",
];
const SOURCE_NAMES: [&str; 6] = [
    "BidPrice", "BidSize", "BuyNo", "AskPrice", "AskSize", "SellNo",
];

/// One book: `f64::NAN` marks an empty field. Sizes and order counts are exact
/// integers (checked while parsing).
pub type Book = [f64; BOOK_FIELDS];

#[derive(Clone, Copy, Debug)]
pub struct Params {
    pub freq_ns: i64,
    pub gap_ns: i64,
    /// Always true in production; `false` only for the `profile` stage that
    /// measures time stamp parsing and bucketing without the book parse.
    pub parse_books: bool,
}
impl Params {
    /// `freq_ms` must divide one UTC day so that the grid repeats every day.
    pub fn new(freq_ms: u64, gap_s: u64) -> Result<Self> {
        ensure!(freq_ms >= 1, "freq-ms must be >= 1");
        ensure!(
            86_400_000 % freq_ms == 0,
            "freq-ms {freq_ms} does not divide one UTC day (86,400,000 ms)"
        );
        ensure!(gap_s >= 1, "session-gap-s must be >= 1");
        Ok(Self {
            freq_ns: freq_ms as i64 * 1_000_000,
            gap_ns: gap_s as i64 * 1_000_000_000,
            parse_books: true,
        })
    }
    /// Profiling only: skip parsing the book of a finished grid point (no rows).
    pub fn without_book_parse(mut self) -> Self {
        self.parse_books = false;
        self
    }
    pub fn file_name(&self) -> String {
        format!("l2_{}ms.parquet", self.freq_ns / 1_000_000)
    }
}

fn same(a: &Book, b: &Book) -> bool {
    a.iter().zip(b).all(|(x, y)| x.to_bits() == y.to_bits())
}

/// Days since 1970-01-01 of a civil date (Howard Hinnant's algorithm).
fn days_from_civil(y: i64, m: i64, d: i64) -> i64 {
    let y = if m <= 2 { y - 1 } else { y };
    let era = if y >= 0 { y } else { y - 399 } / 400;
    let yoe = y - era * 400;
    let doy = (153 * (m + if m > 2 { -3 } else { 9 }) + 2) / 5 + d - 1;
    let doe = yoe * 365 + yoe / 4 - yoe / 100 + doy;
    era * 146_097 + doe - 719_468
}

pub fn day_number(day: &str) -> Result<i64> {
    let b = day.as_bytes();
    ensure!(
        b.len() == 10 && b[4] == b'-' && b[7] == b'-',
        "bad day {day:?}"
    );
    let n = |r: std::ops::Range<usize>| -> Result<i64> {
        std::str::from_utf8(&b[r])?
            .parse::<i64>()
            .with_context(|| format!("bad day {day:?}"))
    };
    let (y, m, d) = (n(0..4)?, n(5..7)?, n(8..10)?);
    ensure!(
        (1..=12).contains(&m) && (1..=31).contains(&d),
        "bad day {day:?}"
    );
    Ok(days_from_civil(y, m, d))
}

pub struct TsParser {
    date: [u8; 10],
    days: i64,
}
impl Default for TsParser {
    fn default() -> Self {
        Self::new()
    }
}
impl TsParser {
    pub fn new() -> Self {
        Self {
            date: [0; 10],
            days: 0,
        }
    }
    /// `YYYY-MM-DDTHH:MM:SS[.f{1,9}]Z` -> UTC nanoseconds.
    pub fn parse(&mut self, s: &[u8]) -> Result<i64> {
        ensure!(
            s.len() >= 20
                && s[10] == b'T'
                && s[13] == b':'
                && s[16] == b':'
                && s[s.len() - 1] == b'Z',
            "bad Date-Time {:?}",
            String::from_utf8_lossy(s)
        );
        if self.date[..] != s[..10] {
            let d = std::str::from_utf8(&s[..10])?;
            self.days = day_number(d)?;
            self.date.copy_from_slice(&s[..10]);
        }
        let two = |i: usize| -> Result<i64> {
            let (a, b) = (s[i], s[i + 1]);
            ensure!(a.is_ascii_digit() && b.is_ascii_digit(), "bad Date-Time");
            Ok(((a - b'0') * 10 + (b - b'0')) as i64)
        };
        let (h, mi, sec) = (two(11)?, two(14)?, two(17)?);
        ensure!(h < 24 && mi < 60 && sec < 61, "bad Date-Time");
        let mut nanos = 0i64;
        if s.len() > 20 {
            ensure!(s[19] == b'.', "bad Date-Time");
            let frac = &s[20..s.len() - 1];
            ensure!(
                !frac.is_empty() && frac.len() <= 9,
                "bad Date-Time fraction"
            );
            for &c in frac {
                ensure!(c.is_ascii_digit(), "bad Date-Time fraction");
                nanos = nanos * 10 + (c - b'0') as i64;
            }
            for _ in frac.len()..9 {
                nanos *= 10;
            }
        } else {
            ensure!(s[19] == b'Z' || s.len() == 20, "bad Date-Time");
        }
        Ok(self.days * DAY_NS + ((h * 60 + mi) * 60 + sec) * 1_000_000_000 + nanos)
    }
}

/// One book field of a row: `k` is the slot (0..BOOK_FIELDS), price slots are
/// `k % 3 == 0`, the others are sizes / order counts (u32, no sign, no point).
fn parse_book_field(k: usize, f: &[u8]) -> Result<f64> {
    Ok(if f.is_empty() {
        f64::NAN
    } else if k.is_multiple_of(3) {
        if let Some(x) = fast_decimal(f) {
            return Ok(x);
        }
        let x: f64 = std::str::from_utf8(f)?
            .parse()
            .with_context(|| format!("bad price {:?}", String::from_utf8_lossy(f)))?;
        ensure!(x.is_finite(), "non-finite price");
        x
    } else {
        let mut x = 0u64;
        for &c in f {
            ensure!(
                c.is_ascii_digit(),
                "size/order count {:?} is not a non-negative integer",
                String::from_utf8_lossy(f)
            );
            x = x * 10 + (c - b'0') as u64;
            ensure!(x <= u32::MAX as u64, "size/order count exceeds u32");
        }
        x as f64
    })
}

const POW10: [f64; 23] = [
    1e0, 1e1, 1e2, 1e3, 1e4, 1e5, 1e6, 1e7, 1e8, 1e9, 1e10, 1e11, 1e12, 1e13, 1e14, 1e15, 1e16,
    1e17, 1e18, 1e19, 1e20, 1e21, 1e22,
];

/// `[-]digits[.digits]` with at most 15 digits: the integer `m` and the power
/// of ten are both exact in f64, so the single IEEE division is the correctly
/// rounded result, i.e. exactly what `str::parse::<f64>` returns (Clinger's
/// fast path). Anything else (exponent, sign `+`, more digits) is `None` and
/// goes through `str::parse`.
fn fast_decimal(f: &[u8]) -> Option<f64> {
    let (neg, d) = match f.first()? {
        b'-' => (true, &f[1..]),
        _ => (false, f),
    };
    let (mut m, mut digits, mut frac, mut seen_dot) = (0u64, 0usize, 0usize, false);
    for &c in d {
        match c {
            b'0'..=b'9' => {
                m = m * 10 + (c - b'0') as u64;
                digits += 1;
                if digits > 15 {
                    return None;
                }
                frac += seen_dot as usize;
            }
            b'.' if !seen_dot => seen_dot = true,
            _ => return None,
        }
    }
    if digits == 0 {
        return None;
    }
    let x = m as f64 / POW10[frac];
    Some(if neg { -x } else { x })
}

pub fn parse_book(line: &[u8], out: &mut Book) -> Result<()> {
    ensure!(!line.contains(&b'"'), "quoted fields are not supported");
    let mut n = 0usize;
    for (i, f) in line.split(|b| *b == b',').enumerate() {
        n += 1;
        if !(5..5 + BOOK_FIELDS).contains(&i) {
            continue;
        }
        let k = i - 5;
        out[k] = parse_book_field(k, f)?;
    }
    ensure!(
        n == CSV_COLUMNS,
        "row has {n} columns, expected {CSV_COLUMNS}"
    );
    Ok(())
}

/// `parse_book` that reuses work between consecutive rows. Consecutive updates
/// of a RIC differ in a few of the 60 book fields only (about 3 on real data),
/// so the row is compared with the previous one: only the stretch between the
/// shared prefix and the shared suffix is split into fields, and a field whose
/// text equals the same field of the previous row keeps the value parsed from
/// that text (identical text parses to the identical value). The result is
/// bit-for-bit what `parse_book` returns, errors included: whatever the
/// incremental path does not handle (first row, structural change, bad field)
/// goes through the full parse.
pub struct BookParser {
    prev: Vec<u8>,
    /// Start of field 3 in `prev` (the book fields come after the time stamp).
    s_prev: usize,
    book: Book,
    valid: bool,
}

impl Default for BookParser {
    fn default() -> Self {
        Self::new()
    }
}

impl BookParser {
    pub fn new() -> Self {
        Self {
            prev: Vec::with_capacity(1024),
            s_prev: 0,
            book: [f64::NAN; BOOK_FIELDS],
            valid: false,
        }
    }

    /// Same contract and same errors as `parse_book`.
    pub fn parse(&mut self, line: &[u8], out: &mut Book) -> Result<()> {
        if self.valid {
            if let Some(r) = self.parse_incremental(line) {
                if r.is_err() {
                    self.valid = false;
                }
                r?;
                *out = self.book;
                return Ok(());
            }
        }
        self.valid = false;
        self.parse_full(line)?;
        *out = self.book;
        Ok(())
    }

    fn parse_full(&mut self, line: &[u8]) -> Result<()> {
        ensure!(
            memchr::memchr(b'"', line).is_none(),
            "quoted fields are not supported"
        );
        let n = memchr::memchr_iter(b',', line).count() + 1;
        ensure!(
            n == CSV_COLUMNS,
            "row has {n} columns, expected {CSV_COLUMNS}"
        );
        for (i, f) in line.split(|b| *b == b',').enumerate() {
            if (5..5 + BOOK_FIELDS).contains(&i) {
                self.book[i - 5] = parse_book_field(i - 5, f)?;
            }
        }
        self.s_prev = memchr::memchr_iter(b',', line).nth(2).expect("66 columns") + 1;
        self.prev.clear();
        self.prev.extend_from_slice(line);
        self.valid = true;
        Ok(())
    }

    /// `None` = not applicable (use the full parse); on `Some(Err)` the caller
    /// invalidates the state.
    fn parse_incremental(&mut self, line: &[u8]) -> Option<Result<()>> {
        const FIRST: usize = 3; // fields before this (RIC, domain, time) are never compared
        let c2 = memchr::memchr_iter(b',', line).nth(2)?;
        let s_line = c2 + 1;
        let (rl, rp) = (&line[s_line..], &self.prev[self.s_prev..]);
        let (pre, suf) = common_affixes(rl, rp);
        if pre == rl.len() && rl.len() == rp.len() {
            // nothing but the head of the row changed
            self.prev.clear();
            self.prev.extend_from_slice(line);
            self.s_prev = s_line;
            return Some(Ok(()));
        }
        let q = rp.len() - suf; // first byte of the shared suffix in `rp`
                                // the stretch of whole fields that contains every differing byte
        let a0 = memchr::memrchr(b',', &rl[..pre]).map_or(0, |i| i + 1);
        let k0 = FIRST + memchr::memchr_iter(b',', &rl[..a0]).count();
        let lc = q.saturating_sub(1).max(pre).min(rp.len());
        let e_prev = lc + memchr::memchr(b',', &rp[lc..]).unwrap_or(rp.len() - lc);
        if e_prev < q {
            return None; // a delimiter itself changed
        }
        let e_line = rl.len() - (rp.len() - e_prev);
        if e_line < a0 {
            return None;
        }
        let (region_prev, region_line) = (&rp[a0..e_prev], &rl[a0..e_line]);
        if memchr::memchr(b'"', region_line).is_some() {
            return None; // the full parse reports it
        }
        let mut pieces_prev = region_prev.split(|b| *b == b',');
        let mut pieces_line = region_line.split(|b| *b == b',');
        let mut j = k0;
        loop {
            match (pieces_prev.next(), pieces_line.next()) {
                (Some(fp), Some(fl)) => {
                    if (5..5 + BOOK_FIELDS).contains(&j) && fp != fl {
                        match parse_book_field(j - 5, fl) {
                            Ok(x) => self.book[j - 5] = x,
                            Err(e) => return Some(Err(e)),
                        }
                    }
                    j += 1;
                }
                (None, None) => break,
                _ => return None, // the number of fields changed
            }
        }
        self.prev.clear();
        self.prev.extend_from_slice(line);
        self.s_prev = s_line;
        Some(Ok(()))
    }
}

/// Lengths of the longest common prefix and, without overlapping it, the
/// longest common suffix of two byte strings (8 bytes at a time).
fn common_affixes(a: &[u8], b: &[u8]) -> (usize, usize) {
    let max = a.len().min(b.len());
    let mut pre = 0;
    while pre + 8 <= max
        && u64::from_ne_bytes(a[pre..pre + 8].try_into().expect("8"))
            == u64::from_ne_bytes(b[pre..pre + 8].try_into().expect("8"))
    {
        pre += 8;
    }
    while pre < max && a[pre] == b[pre] {
        pre += 1;
    }
    let max_suf = max - pre;
    let (la, lb) = (a.len(), b.len());
    let mut suf = 0;
    while suf + 8 <= max_suf
        && u64::from_ne_bytes(a[la - suf - 8..la - suf].try_into().expect("8"))
            == u64::from_ne_bytes(b[lb - suf - 8..lb - suf].try_into().expect("8"))
    {
        suf += 8;
    }
    while suf < max_suf && a[la - 1 - suf] == b[lb - 1 - suf] {
        suf += 1;
    }
    (pre, suf)
}

pub fn expected_header() -> String {
    let mut h = vec![
        "#RIC".to_string(),
        "Domain".into(),
        "Date-Time".into(),
        "GMT Offset".into(),
        "Type".into(),
    ];
    for l in 1..=LEVELS {
        for s in SOURCE_NAMES {
            h.push(format!("L{l}-{s}"));
        }
    }
    h.push("Exch Time".into());
    h.join(",")
}

/// Per RIC-day counters.
#[derive(Clone, Debug, Default)]
pub struct RicStats {
    pub ric: String,
    pub updates: u64,
    pub buckets: u64,
    pub emitted: u64,
    pub anchors: u64,
    pub sessions: u64,
    /// Grid points between the first and last update of every session.
    pub active_grid_points: u64,
    pub first_update_ns: i64,
    pub last_update_ns: i64,
}

/// Discards every row (profiling).
pub struct NullSink;
impl Sink for NullSink {
    fn begin_ric(&mut self, _: &str) -> Result<bool> {
        Ok(true)
    }
    fn row(&mut self, _: u64, _: u64, _: bool, _: &Book) -> Result<()> {
        Ok(())
    }
    fn end_ric(&mut self, _: &RicStats) -> Result<()> {
        Ok(())
    }
}

pub trait Sink {
    /// `false` = skip this RIC (its rows are not parsed).
    fn begin_ric(&mut self, ric: &str) -> Result<bool>;
    fn row(&mut self, ts: u64, last_update: u64, anchor: bool, book: &Book) -> Result<()>;
    fn end_ric(&mut self, stats: &RicStats) -> Result<()>;
}

struct Open {
    ric: String,
    wanted: bool,
    stats: RicStats,
    prev_t: Option<i64>,
    bucket: i64,
    bucket_anchor: bool,
    line: Vec<u8>,
    line_t: i64,
    /// Updates decoded from the RocksDB carry their book; text rows keep the line.
    pbook: Book,
    has_pbook: bool,
    has_bucket: bool,
    emitted: Option<Book>,
    session_first_g: i64,
    session_last_g: i64,
}

impl Default for Open {
    fn default() -> Self {
        Self {
            ric: String::new(),
            wanted: false,
            stats: RicStats::default(),
            prev_t: None,
            bucket: 0,
            bucket_anchor: false,
            line: Vec::new(),
            line_t: 0,
            pbook: [f64::NAN; BOOK_FIELDS],
            has_pbook: false,
            has_bucket: false,
            emitted: None,
            session_first_g: 0,
            session_last_g: 0,
        }
    }
}

fn finish_bucket(o: &mut Open, p: &Params, sink: &mut dyn Sink, book: &mut Book) -> Result<()> {
    if !o.has_bucket {
        return Ok(());
    }
    o.stats.buckets += 1;
    if o.wanted && p.parse_books {
        if o.has_pbook {
            *book = o.pbook;
        } else {
            parse_book(&o.line, book)
                .with_context(|| format!("RIC {} at update {}", o.ric, o.line_t))?;
        }
        let changed = o.emitted.as_ref().is_none_or(|e| !same(e, book));
        if changed || o.bucket_anchor {
            sink.row(
                (o.bucket * p.freq_ns) as u64,
                o.line_t as u64,
                o.bucket_anchor,
                book,
            )?;
            o.stats.emitted += 1;
            if o.bucket_anchor {
                o.stats.anchors += 1;
            }
            o.emitted = Some(*book);
        }
    }
    o.has_bucket = false;
    Ok(())
}

fn close_ric(o: &mut Open, p: &Params, sink: &mut dyn Sink, book: &mut Book) -> Result<()> {
    finish_bucket(o, p, sink, book)?;
    if o.stats.updates > 0 {
        o.stats.active_grid_points += (o.session_last_g - o.session_first_g + 1) as u64;
    }
    if o.wanted {
        sink.end_ric(&o.stats)?;
    }
    Ok(())
}

/// Bookkeeping of one update (time order, grid bucket, session, counters).
/// The caller then stores the update's book (text line or decoded book).
fn register_update(
    o: &mut Open,
    t: i64,
    p: &Params,
    sink: &mut dyn Sink,
    book: &mut Book,
) -> Result<()> {
    if let Some(prev) = o.prev_t {
        ensure!(
            t >= prev,
            "RIC {}: Date-Time goes backwards ({t} < {prev})",
            o.ric
        );
    }
    let g = (t + p.freq_ns - 1).div_euclid(p.freq_ns);
    let first_ever = o.prev_t.is_none();
    let after_gap = o.prev_t.is_some_and(|prev| t - prev >= p.gap_ns);
    if o.has_bucket && g != o.bucket {
        finish_bucket(o, p, sink, book)?;
    }
    if first_ever {
        o.stats.first_update_ns = t;
        o.session_first_g = g;
        o.stats.sessions = 1;
    } else if after_gap {
        o.stats.active_grid_points += (o.session_last_g - o.session_first_g + 1) as u64;
        o.session_first_g = g;
        o.stats.sessions += 1;
    }
    if !o.has_bucket {
        o.has_bucket = true;
        o.bucket = g;
        o.bucket_anchor = first_ever || after_gap;
    }
    o.session_last_g = g;
    o.stats.updates += 1;
    o.stats.last_update_ns = t;
    o.prev_t = Some(t);
    Ok(())
}

/// One RIC being sliced from already decoded updates (the RocksDB extract).
pub struct RicRun {
    open: Open,
    book: Book,
}

impl RicRun {
    pub fn new(ric: &str, sink: &mut dyn Sink) -> Result<Self> {
        let wanted = sink.begin_ric(ric)?;
        Ok(Self {
            open: Open {
                ric: ric.to_string(),
                wanted,
                stats: RicStats {
                    ric: ric.to_string(),
                    ..Default::default()
                },
                has_pbook: true,
                ..Default::default()
            },
            book: [f64::NAN; BOOK_FIELDS],
        })
    }
    /// False when the sink skips this RIC (existing output): no need to decode.
    pub fn wanted(&self) -> bool {
        self.open.wanted
    }
    pub fn update(&mut self, t: i64, book: &Book, p: &Params, sink: &mut dyn Sink) -> Result<()> {
        register_update(&mut self.open, t, p, sink, &mut self.book)?;
        if self.open.wanted {
            self.open.pbook = *book;
            self.open.line_t = t;
        }
        Ok(())
    }
    pub fn finish(mut self, p: &Params, sink: &mut dyn Sink) -> Result<RicStats> {
        close_ric(&mut self.open, p, sink, &mut self.book)?;
        Ok(self.open.stats)
    }
}

/// Streams one source file (UTC day `day_start_ns .. +DAY_NS`) through `sink`.
pub fn slice_stream<R: BufRead>(
    mut reader: R,
    day_start_ns: i64,
    p: &Params,
    sink: &mut dyn Sink,
) -> Result<Vec<RicStats>> {
    let mut line = Vec::with_capacity(1024);
    ensure!(
        reader.read_until(b'\n', &mut line)? > 0,
        "empty source file"
    );
    let header = String::from_utf8_lossy(&line).trim_end().to_string();
    ensure!(
        header == expected_header(),
        "unexpected header: {}",
        &header[..header.len().min(120)]
    );
    let day_end_ns = day_start_ns + DAY_NS;
    let mut ts = TsParser::new();
    let mut done: HashSet<String> = HashSet::new();
    let mut all = Vec::new();
    let mut open = Open::default();
    let mut have_open = false;
    let mut book: Book = [f64::NAN; BOOK_FIELDS];
    loop {
        line.clear();
        if reader.read_until(b'\n', &mut line)? == 0 {
            break;
        }
        while matches!(line.last(), Some(b'\n' | b'\r')) {
            line.pop();
        }
        if line.is_empty() {
            continue;
        }
        let mut it = line.splitn(4, |b| *b == b',');
        let ric_b = it.next().unwrap_or(&[]);
        let _domain = it.next();
        let ts_b = it.next().with_context(|| {
            format!(
                "short row {:?}",
                String::from_utf8_lossy(&line[..line.len().min(80)])
            )
        })?;
        ensure!(!ric_b.is_empty(), "empty RIC");
        if !have_open || open.ric.as_bytes() != ric_b {
            if have_open {
                close_ric(&mut open, p, sink, &mut book)?;
                all.push(open.stats.clone());
                done.insert(open.ric.clone());
            }
            let ric = std::str::from_utf8(ric_b)?.to_string();
            ensure!(
                !done.contains(&ric),
                "RIC {ric} appears in more than one block of the file"
            );
            let wanted = sink.begin_ric(&ric)?;
            open = Open {
                ric: ric.clone(),
                wanted,
                stats: RicStats {
                    ric,
                    ..Default::default()
                },
                ..Default::default()
            };
            have_open = true;
        }
        let t = ts.parse(ts_b)?;
        ensure!(
            t >= day_start_ns && t < day_end_ns,
            "RIC {}: Date-Time {} is outside the file's UTC day",
            open.ric,
            String::from_utf8_lossy(ts_b)
        );
        register_update(&mut open, t, p, sink, &mut book)?;
        if open.wanted {
            open.line.clear();
            open.line.extend_from_slice(&line);
            open.line_t = t;
        }
    }
    if have_open {
        close_ric(&mut open, p, sink, &mut book)?;
        all.push(open.stats.clone());
    }
    Ok(all)
}

pub fn schema() -> Schema {
    let mut f = vec![
        Field::new("ric", DataType::Utf8, false),
        Field::new("ts_utc_ns", DataType::UInt64, false),
        Field::new("last_update_ts_ns", DataType::UInt64, false),
        Field::new("anchor", DataType::Boolean, false),
    ];
    for l in 1..=LEVELS {
        for (k, n) in FIELD_NAMES.iter().enumerate() {
            let t = if k % 3 == 0 {
                DataType::Float64
            } else {
                DataType::UInt32
            };
            f.push(Field::new(format!("l{l}_{n}"), t, true));
        }
    }
    Schema::new(f)
}

/// Footer key/value metadata in a fixed order (a HashMap in the Arrow schema
/// would serialize in a random order and make the files non-reproducible).
fn file_metadata(p: &Params) -> Vec<KeyValue> {
    let kv = |k: &str, v: String| KeyValue::new(k.to_string(), v);
    vec![
        kv("l2_slice_schema_version", "1".into()),
        kv("source", "LSEG Normalized LL2 daily package (normalisedLL2)".into()),
        kv("freq_ms", (p.freq_ns / 1_000_000).to_string()),
        kv(
            "grid",
            "ts_utc_ns = k*freq (UTC day anchored), right-closed as-of: book = last update with Date-Time <= ts_utc_ns".into(),
        ),
        kv(
            "emission",
            "downsampling only: a grid point is emitted when its book differs from the previous emitted row; no forward fill".into(),
        ),
        kv(
            "anchor",
            "true for the first row of the RIC-day and the first row after a silence of session_gap_s (emitted even when unchanged)".into(),
        ),
        kv("session_gap_s", (p.gap_ns / 1_000_000_000).to_string()),
        kv(
            "levels",
            "l1..l10: bid, bid_size, bid_orders (BuyNo), ask, ask_size, ask_orders (SellNo); null = empty field".into(),
        ),
    ]
}

fn writer_props(p: &Params) -> WriterProperties {
    WriterProperties::builder()
        .set_key_value_metadata(Some(file_metadata(p)))
        .set_compression(Compression::ZSTD(ZstdLevel::default()))
        .set_max_row_group_size(ROW_GROUP_ROWS)
        .set_statistics_enabled(EnabledStatistics::Chunk)
        .set_created_by("lseg_l2_slice".to_string())
        .build()
}

struct Builders {
    ric: StringBuilder,
    ts: UInt64Builder,
    last: UInt64Builder,
    anchor: BooleanBuilder,
    px: Vec<Float64Builder>,
    qty: Vec<UInt32Builder>,
    rows: usize,
}
impl Builders {
    fn new() -> Self {
        Self {
            ric: StringBuilder::new(),
            ts: UInt64Builder::new(),
            last: UInt64Builder::new(),
            anchor: BooleanBuilder::new(),
            px: (0..LEVELS * 2).map(|_| Float64Builder::new()).collect(),
            qty: (0..LEVELS * 4).map(|_| UInt32Builder::new()).collect(),
            rows: 0,
        }
    }
    fn push(&mut self, ric: &str, ts: u64, last: u64, anchor: bool, book: &Book) {
        self.ric.append_value(ric);
        self.ts.append_value(ts);
        self.last.append_value(last);
        self.anchor.append_value(anchor);
        let (mut pi, mut qi) = (0, 0);
        for (k, v) in book.iter().enumerate() {
            if k % 3 == 0 {
                self.px[pi].append_option((!v.is_nan()).then_some(*v));
                pi += 1;
            } else {
                self.qty[qi].append_option((!v.is_nan()).then_some(*v as u32));
                qi += 1;
            }
        }
        self.rows += 1;
    }
    fn batch(&mut self, schema: &Arc<Schema>) -> Result<RecordBatch> {
        let mut cols: Vec<ArrayRef> = vec![
            Arc::new(self.ric.finish()),
            Arc::new(self.ts.finish()),
            Arc::new(self.last.finish()),
            Arc::new(self.anchor.finish()),
        ];
        let (mut pi, mut qi) = (0, 0);
        for k in 0..BOOK_FIELDS {
            if k % 3 == 0 {
                cols.push(Arc::new(self.px[pi].finish()));
                pi += 1;
            } else {
                cols.push(Arc::new(self.qty[qi].finish()));
                qi += 1;
            }
        }
        self.rows = 0;
        Ok(RecordBatch::try_new(schema.clone(), cols)?)
    }
}

/// Where a RIC's rows go and which exchange directory it belongs to.
pub struct Layout {
    pub out_root: PathBuf,
    pub exchange_of: Arc<BTreeMap<String, String>>,
    pub default_exchange: Option<String>,
    pub day: String,
    pub force: bool,
}
impl Layout {
    pub fn final_path(&self, ric: &str, p: &Params) -> Result<PathBuf> {
        let ex = self
            .exchange_of
            .get(ric)
            .or(self.default_exchange.as_ref())
            .with_context(|| format!("no exchange directory for RIC {ric}"))?;
        let year = &self.day[..4];
        let day8 = self.day.replace('-', "");
        Ok(self
            .out_root
            .join(ex)
            .join(year)
            .join(crate::tasdb::component(ric))
            .join(day8)
            .join(p.file_name()))
    }
}

pub struct ParquetSink<'a> {
    pub layout: &'a Layout,
    pub params: Params,
    schema: Arc<Schema>,
    day_start: u64,
    day_end: u64,
    cur: Option<Cur>,
    pub outputs: Vec<Output>,
    pub skipped: Vec<String>,
}
struct Cur {
    ric: String,
    partial: PathBuf,
    final_path: PathBuf,
    writer: ArrowWriter<File>,
    b: Builders,
    rows: u64,
}
#[derive(Clone, Debug)]
pub struct Output {
    pub ric: String,
    pub path: PathBuf,
    pub rows: u64,
    pub bytes: u64,
}

impl<'a> ParquetSink<'a> {
    pub fn new(layout: &'a Layout, params: Params, day_start_ns: i64) -> Self {
        Self {
            schema: Arc::new(schema()),
            layout,
            params,
            day_start: day_start_ns as u64,
            day_end: (day_start_ns + DAY_NS) as u64,
            cur: None,
            outputs: Vec::new(),
            skipped: Vec::new(),
        }
    }
    fn flush(&mut self) -> Result<()> {
        let schema = self.schema.clone();
        if let Some(c) = self.cur.as_mut() {
            if c.b.rows > 0 {
                let batch = c.b.batch(&schema)?;
                c.writer.write(&batch)?;
                c.writer.flush()?;
            }
        }
        Ok(())
    }
}

impl Sink for ParquetSink<'_> {
    fn begin_ric(&mut self, ric: &str) -> Result<bool> {
        let final_path = self.layout.final_path(ric, &self.params)?;
        if final_path.exists() && !self.layout.force {
            self.skipped.push(ric.to_string());
            return Ok(false);
        }
        fs::create_dir_all(final_path.parent().context("parent")?)?;
        let partial = final_path.with_extension("parquet.partial");
        let file = File::create(&partial).with_context(|| format!("create {partial:?}"))?;
        let writer =
            ArrowWriter::try_new(file, self.schema.clone(), Some(writer_props(&self.params)))?;
        self.cur = Some(Cur {
            ric: ric.to_string(),
            partial,
            final_path,
            writer,
            b: Builders::new(),
            rows: 0,
        });
        Ok(true)
    }
    fn row(&mut self, ts: u64, last: u64, anchor: bool, book: &Book) -> Result<()> {
        let c = self.cur.as_mut().context("row without RIC")?;
        c.b.push(&c.ric, ts, last, anchor, book);
        c.rows += 1;
        let full = c.b.rows >= ROW_GROUP_ROWS;
        if full {
            self.flush()?;
        }
        Ok(())
    }
    fn end_ric(&mut self, stats: &RicStats) -> Result<()> {
        self.flush()?;
        let c = self.cur.take().context("end without RIC")?;
        c.writer.close()?;
        {
            let f = File::open(&c.partial)?;
            f.sync_all()?;
        }
        verify_footer(&c.partial, &c.ric, c.rows, self.day_start, self.day_end)
            .with_context(|| format!("footer check of {:?}", c.partial))?;
        ensure!(c.rows == stats.emitted, "row count mismatch");
        fs::rename(&c.partial, &c.final_path)?;
        self.outputs.push(Output {
            ric: c.ric,
            bytes: fs::metadata(&c.final_path)?.len(),
            path: c.final_path,
            rows: c.rows,
        });
        Ok(())
    }
}

/// Footer check like the delivery publish: row count, and for every row group
/// the RIC (min == max == `ric`) and the UTC time range (inclusive day end).
pub fn verify_footer(
    path: &Path,
    ric: &str,
    rows: u64,
    day_start: u64,
    day_end: u64,
) -> Result<()> {
    let reader = SerializedFileReader::new(File::open(path)?)?;
    let md = reader.metadata();
    ensure!(
        md.file_metadata().num_rows() as u64 == rows,
        "footer rows {} != {rows}",
        md.file_metadata().num_rows()
    );
    for rg in 0..md.num_row_groups() {
        let g = md.row_group(rg);
        for col in g.columns() {
            let name = col.column_descr().name();
            match (name, col.statistics()) {
                ("ric", Some(Statistics::ByteArray(s))) => {
                    ensure!(
                        s.min_opt().map(|v| v.data()) == Some(ric.as_bytes())
                            && s.max_opt().map(|v| v.data()) == Some(ric.as_bytes()),
                        "row group {rg}: RIC statistics differ from {ric}"
                    );
                }
                ("ts_utc_ns" | "last_update_ts_ns", Some(Statistics::Int64(s))) => {
                    let (lo, hi) = (
                        *s.min_opt().context("ts min")? as u64,
                        *s.max_opt().context("ts max")? as u64,
                    );
                    ensure!(
                        lo >= day_start && hi <= day_end,
                        "row group {rg}: {name} range {lo}..{hi} outside the UTC day"
                    );
                }
                ("ric" | "ts_utc_ns" | "last_update_ts_ns", _) => {
                    bail!("row group {rg}: missing statistics for {name}")
                }
                _ => {}
            }
        }
    }
    Ok(())
}

/// Writes `data` with `fsync`; used by tests and tools.
pub fn write_all(path: &Path, data: &[u8]) -> Result<()> {
    let mut f = File::create(path)?;
    f.write_all(data)?;
    f.sync_all()?;
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    /// Deterministic xorshift, no dependency.
    fn rng(state: &mut u64) -> u64 {
        *state ^= *state << 13;
        *state ^= *state >> 7;
        *state ^= *state << 17;
        *state
    }

    fn field(k: usize, r: u64) -> String {
        match (k.is_multiple_of(3), r % 7) {
            (_, 0) => String::new(),
            (true, v) => format!(
                "{}.{}",
                100 + (r >> 8) % 5,
                [25, 5, 125, 0, 625, 3, 75][v as usize]
            ),
            (false, _) => format!("{}", (r >> 4) % 90_000),
        }
    }

    /// Hard cases for the incremental path: first/last book field changing,
    /// fields growing and shrinking, fields turning empty and back, whole-row
    /// changes, repeated identical rows, rows differing only in the time stamp.
    #[test]
    fn fast_decimal_is_bit_identical_to_str_parse() {
        let mut st = 0x0123_4567_89ab_cdefu64;
        let mut cases: Vec<String> = [
            "0",
            "-0",
            "0.0",
            "-0.0",
            "5.",
            ".5",
            "-.5",
            "1",
            "100",
            "0.1",
            "0.3",
            "6989.25",
            "0.66785",
            "123456789012345",
            "1234567890123456",
            "12345678901234.5",
            "0.000000000000001",
            "1e5",
            "+1",
            "1.2.3",
            "-",
            ".",
        ]
        .iter()
        .map(|s| s.to_string())
        .collect();
        for _ in 0..200_000 {
            let int_digits = (rng(&mut st) % 9) as usize;
            let frac_digits = (rng(&mut st) % 12) as usize;
            let mut t = String::new();
            if rng(&mut st).is_multiple_of(9) {
                t.push('-');
            }
            for _ in 0..int_digits {
                t.push((b'0' + (rng(&mut st) % 10) as u8) as char);
            }
            if frac_digits > 0 || rng(&mut st).is_multiple_of(7) {
                t.push('.');
            }
            for _ in 0..frac_digits {
                t.push((b'0' + (rng(&mut st) % 10) as u8) as char);
            }
            cases.push(t);
        }
        let mut fast = 0u32;
        for t in &cases {
            let slow = t.parse::<f64>();
            if let Some(x) = fast_decimal(t.as_bytes()) {
                fast += 1;
                let y = slow.unwrap_or_else(|_| panic!("fast accepted {t:?}"));
                assert_eq!(x.to_bits(), y.to_bits(), "{t:?}");
            }
        }
        assert!(
            fast > 100_000,
            "the fast path must cover plain decimals ({fast})"
        );
    }

    #[test]
    fn book_parser_equals_parse_book_under_adversarial_changes() {
        let mut st = 0x1234_5678_9abc_def1u64;
        let mut cols: Vec<String> = (0..CSV_COLUMNS).map(|_| String::new()).collect();
        let mut parser = BookParser::new();
        let (mut a, mut b): (Book, Book) = ([f64::NAN; BOOK_FIELDS], [f64::NAN; BOOK_FIELDS]);
        let widths = [
            "",
            "1",
            "12",
            "1234",
            "99999",
            "1.5",
            "10.25",
            "100000.125",
            "7",
        ];
        for row in 0..30_000u32 {
            let mode = rng(&mut st) % 10;
            let count = match mode {
                0 => 0,
                1 => BOOK_FIELDS,
                2 => 8,
                _ => 1 + (rng(&mut st) % 3) as usize,
            };
            for c in 0..count {
                let k = match mode {
                    1 => c,
                    3 => 0,
                    4 => BOOK_FIELDS - 1,
                    5 => BOOK_FIELDS - 1 - (rng(&mut st) % 3) as usize,
                    _ => (rng(&mut st) % BOOK_FIELDS as u64) as usize,
                };
                let w = widths[(rng(&mut st) % widths.len() as u64) as usize];
                cols[5 + k] = if w.is_empty() || k % 3 == 0 {
                    w.to_string()
                } else {
                    w.replace('.', "")
                };
            }
            cols[0] = if row % 5000 == 4999 {
                "ESH7".into()
            } else {
                "ESZ6".into()
            };
            cols[2] = format!("2026-11-10T00:00:{:02}.{:09}Z", row % 60, row);
            cols[CSV_COLUMNS - 1] = format!("00:00:00.{:09}", row % 7);
            let line = cols.join(",");
            parse_book(line.as_bytes(), &mut a).unwrap();
            parser.parse(line.as_bytes(), &mut b).unwrap();
            for k in 0..BOOK_FIELDS {
                assert_eq!(
                    a[k].to_bits(),
                    b[k].to_bits(),
                    "row {row} field {k} mode {mode}"
                );
            }
        }
    }

    #[test]
    fn book_parser_equals_parse_book_row_by_row() {
        let mut st = 0x9e37_79b9_7f4a_7c15u64;
        let mut cols: Vec<String> = (0..CSV_COLUMNS).map(|_| String::new()).collect();
        let mut parser = BookParser::new();
        let (mut a, mut b): (Book, Book) = ([f64::NAN; BOOK_FIELDS], [f64::NAN; BOOK_FIELDS]);
        for row in 0..4000 {
            // change 0..4 random book fields (or all of them now and then)
            let changes = if row % 500 == 0 {
                BOOK_FIELDS
            } else {
                (rng(&mut st) % 5) as usize
            };
            for c in 0..changes {
                let k = if changes == BOOK_FIELDS {
                    c
                } else {
                    (rng(&mut st) % BOOK_FIELDS as u64) as usize
                };
                cols[5 + k] = field(k, rng(&mut st));
            }
            cols[0] = "ESZ6".into();
            cols[2] = format!("2026-11-10T00:00:{:02}.000000000Z", row % 60);
            let line = cols.join(",");
            parse_book(line.as_bytes(), &mut a).unwrap();
            parser.parse(line.as_bytes(), &mut b).unwrap();
            for k in 0..BOOK_FIELDS {
                assert_eq!(a[k].to_bits(), b[k].to_bits(), "row {row} field {k}");
            }
        }
    }

    #[test]
    fn book_parser_errors_like_parse_book_and_recovers_with_a_full_parse() {
        let good: Vec<String> = (0..CSV_COLUMNS)
            .map(|i| {
                if (5..65).contains(&i) {
                    if (i - 5) % 3 == 0 {
                        "1.5".into()
                    } else {
                        "7".into()
                    }
                } else {
                    "x".into()
                }
            })
            .collect();
        let line = good.join(",");
        let mut p = BookParser::new();
        let mut book: Book = [f64::NAN; BOOK_FIELDS];
        p.parse(line.as_bytes(), &mut book).unwrap();
        let mut bad = good.clone();
        bad[6] = "-3".into(); // a size that is not a non-negative integer
        let bad_line = bad.join(",");
        let e1 = parse_book(bad_line.as_bytes(), &mut book)
            .unwrap_err()
            .to_string();
        let e2 = p
            .parse(bad_line.as_bytes(), &mut book)
            .unwrap_err()
            .to_string();
        assert_eq!(e1, e2);
        // too few / too many columns, quotes
        assert!(p.parse(b"a,b,c", &mut book).is_err());
        assert!(p
            .parse(format!("{line},extra").as_bytes(), &mut book)
            .is_err());
        assert!(p.parse(format!("\"{line}").as_bytes(), &mut book).is_err());
        // after errors the next good row is parsed in full and is right
        let mut expect: Book = [f64::NAN; BOOK_FIELDS];
        parse_book(line.as_bytes(), &mut expect).unwrap();
        p.parse(line.as_bytes(), &mut book).unwrap();
        assert_eq!(expect.map(f64::to_bits), book.map(f64::to_bits));
    }
    use std::io::Cursor;

    const F: i64 = 250_000_000;
    const DAY0: i64 = 20_000 * DAY_NS; // 2024-10-04 UTC
    fn day() -> String {
        let d =
            chrono::NaiveDate::from_ymd_opt(1970, 1, 1).unwrap() + chrono::Duration::days(20_000);
        d.format("%Y-%m-%d").to_string()
    }
    fn ts_text(off_ns: i64) -> String {
        let t = DAY0 + off_ns;
        let secs = t.div_euclid(1_000_000_000);
        let nanos = t.rem_euclid(1_000_000_000);
        let dt = chrono::DateTime::from_timestamp(secs, 0).unwrap();
        format!("{}.{:09}Z", dt.format("%Y-%m-%dT%H:%M:%S"), nanos)
    }
    /// A row of RIC `r` at `off_ns` after the day start with bid1 price/size
    /// and ask1 price (the rest empty).
    fn row(r: &str, off_ns: i64, bid: f64, size: u32, ask: f64) -> String {
        let mut f = vec![String::new(); CSV_COLUMNS];
        f[0] = r.into();
        f[1] = "Market Price".into();
        f[2] = ts_text(off_ns);
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
    fn file(rows: &[String]) -> Vec<u8> {
        let mut s = expected_header();
        for r in rows {
            s.push('\n');
            s.push_str(r);
        }
        s.push('\n');
        s.into_bytes()
    }
    #[derive(Default)]
    struct Collect {
        rows: Vec<(String, u64, u64, bool, f64, f64)>,
        ric: String,
    }
    impl Sink for Collect {
        fn begin_ric(&mut self, ric: &str) -> Result<bool> {
            self.ric = ric.into();
            Ok(true)
        }
        fn row(&mut self, ts: u64, last: u64, anchor: bool, b: &Book) -> Result<()> {
            self.rows
                .push((self.ric.clone(), ts, last, anchor, b[0], b[1]));
            Ok(())
        }
        fn end_ric(&mut self, _: &RicStats) -> Result<()> {
            Ok(())
        }
    }
    fn run(rows: &[String]) -> Result<(Vec<(String, u64, u64, bool, f64, f64)>, Vec<RicStats>)> {
        let p = Params::new(250, 1800)?;
        let mut sink = Collect::default();
        let stats = slice_stream(Cursor::new(file(rows)), DAY0, &p, &mut sink)?;
        Ok((sink.rows, stats))
    }
    fn at(k: i64) -> i64 {
        k * F
    }

    #[test]
    fn update_exactly_on_the_grid_point_belongs_to_it() {
        let (r, _) = run(&[
            row("A", at(4), 10.0, 1, 11.0),
            row("A", at(8) + 1, 10.5, 1, 11.0),
        ])
        .unwrap();
        assert_eq!(r[0].1 as i64, DAY0 + at(4), "on T -> T");
        assert_eq!(r[1].1 as i64, DAY0 + at(9), "1 ns after T -> next point");
    }

    #[test]
    fn two_updates_in_one_interval_emit_once_with_the_later_state() {
        let (r, s) = run(&[
            row("A", at(4), 10.0, 1, 11.0),
            row("A", at(8) + 10, 10.5, 1, 11.0),
            row("A", at(8) + 20, 10.75, 1, 11.0),
        ])
        .unwrap();
        assert_eq!(r.len(), 2);
        assert_eq!(r[1].4, 10.75);
        assert_eq!(
            r[1].2 as i64,
            DAY0 + at(8) + 20,
            "last_update_ts of the later state"
        );
        assert_eq!(s[0].buckets, 2);
    }

    #[test]
    fn change_and_revert_inside_one_interval_emits_nothing() {
        let (r, s) = run(&[
            row("A", at(4), 10.0, 1, 11.0),
            row("A", at(8) + 10, 99.0, 1, 11.0),
            row("A", at(8) + 20, 10.0, 1, 11.0),
        ])
        .unwrap();
        assert_eq!(r.len(), 1, "only the day anchor");
        assert_eq!(s[0].buckets, 2);
    }

    #[test]
    fn unchanged_book_is_not_repeated_and_rows_are_a_grid_step_apart() {
        let mut rows = vec![];
        for k in 1..=10 {
            rows.push(row("A", at(k) - 5, 10.0, 1, 11.0));
        }
        rows.push(row("A", at(12), 10.0, 2, 11.0));
        let (r, _) = run(&rows).unwrap();
        assert_eq!(r.len(), 2);
        assert!(r[1].1 - r[0].1 >= F as u64);
    }

    #[test]
    fn first_row_and_rows_after_a_long_silence_are_anchors_even_if_unchanged() {
        let gap = 1800 * 1_000_000_000i64;
        let (r, s) = run(&[
            row("A", at(4), 10.0, 1, 11.0),
            row("A", at(6), 10.0, 1, 11.0),
            row("A", at(6) + gap, 10.0, 1, 11.0),
            row("A", at(8) + gap, 10.0, 1, 11.0),
        ])
        .unwrap();
        assert_eq!(r.len(), 2);
        assert!(r[0].3 && r[1].3);
        assert_eq!(r[1].1 as i64, DAY0 + at(6) + gap);
        assert_eq!(s[0].sessions, 2);
        // just below the gap is not a session start
        let (r, _) = run(&[
            row("A", at(4), 10.0, 1, 11.0),
            row("A", at(4) + gap - 1, 10.0, 1, 11.0),
        ])
        .unwrap();
        assert_eq!(r.len(), 1);
    }

    #[test]
    fn equal_timestamps_keep_file_order_and_blocks_are_independent() {
        let (r, _) = run(&[
            row("A", at(4), 1.0, 1, 2.0),
            row("A", at(4), 3.0, 1, 4.0),
            row("B", at(4), 7.0, 1, 8.0),
        ])
        .unwrap();
        assert_eq!(r.len(), 2);
        assert_eq!(
            (r[0].0.as_str(), r[0].4),
            ("A", 3.0),
            "last of equal timestamps wins"
        );
        assert_eq!(r[1].0, "B");
        assert!(r[1].3, "first row of the next RIC is an anchor");
    }

    #[test]
    fn source_violations_are_errors() {
        let e = |rows: &[String]| format!("{:#}", run(rows).unwrap_err());
        assert!(
            e(&[row("A", at(8), 1.0, 1, 2.0), row("A", at(4), 1.0, 1, 2.0)]).contains("backwards")
        );
        assert!(e(&[
            row("A", at(4), 1.0, 1, 2.0),
            row("B", at(4), 1.0, 1, 2.0),
            row("A", at(8), 1.0, 1, 2.0)
        ])
        .contains("more than one block"));
        assert!(e(&[row("A", -1, 1.0, 1, 2.0)]).contains("outside"));
        assert!(e(&[row("A", DAY_NS, 1.0, 1, 2.0)]).contains("outside"));
        let bad = row("A", at(4), 10.0, 7, 11.0).replacen(",7,", ",7.5,", 1);
        assert!(e(&[bad]).contains("integer"));
    }

    #[test]
    fn day_end_grid_point_is_kept_in_the_file_of_the_update() {
        let (r, _) = run(&[row("A", DAY_NS - 100, 1.0, 1, 2.0)]).unwrap();
        assert_eq!(r[0].1 as i64, DAY0 + DAY_NS, "T may be the next midnight");
    }

    #[test]
    fn params_validation() {
        assert!(Params::new(250, 1800).is_ok());
        assert!(Params::new(1000, 1800).is_ok());
        assert!(Params::new(3000, 1800).is_ok());
        assert!(Params::new(7, 1800).is_err());
        assert!(Params::new(0, 1800).is_err());
    }

    #[test]
    fn parquet_output_is_complete_verified_and_idempotent() {
        let rows = vec![
            row("A", at(4), 10.0, 1, 11.0),
            row("A", at(8), 10.5, 1, 11.0),
            row("B", at(5), 20.0, 3, 21.0),
        ];
        let data = file(&rows);
        let mut digests = vec![];
        for _ in 0..2 {
            let t = tempfile::tempdir().unwrap();
            let layout = Layout {
                out_root: t.path().to_path_buf(),
                exchange_of: Arc::new(BTreeMap::new()),
                default_exchange: Some("cme".into()),
                day: day(),
                force: false,
            };
            let p = Params::new(250, 1800).unwrap();
            let mut sink = ParquetSink::new(&layout, p, DAY0);
            slice_stream(Cursor::new(data.clone()), DAY0, &p, &mut sink).unwrap();
            assert_eq!(sink.outputs.len(), 2);
            let a = sink.outputs.iter().find(|o| o.ric == "A").unwrap();
            assert_eq!(a.rows, 2);
            assert!(a.path.ends_with(format!(
                "cme/2024/A/{}/l2_250ms.parquet",
                day().replace('-', "")
            )));
            digests.push(crate::tasdb::file_sha256(&a.path).unwrap());
            // second pass without --force skips existing files
            let mut again = ParquetSink::new(&layout, p, DAY0);
            slice_stream(Cursor::new(data.clone()), DAY0, &p, &mut again).unwrap();
            assert!(again.outputs.is_empty() && again.skipped.len() == 2);
            assert!(!t
                .path()
                .join("cme/2024/A")
                .join(day().replace('-', ""))
                .join("l2_250ms.parquet.partial")
                .exists());
        }
        assert_eq!(digests[0], digests[1], "byte-identical re-run");
    }
}
