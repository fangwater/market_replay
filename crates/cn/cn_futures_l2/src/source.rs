//! Locate Tonglian future-main-quote files and parse L2 snapshots.

use anyhow::{bail, Context, Result};
use chrono::{Days, NaiveDate, NaiveTime, TimeZone};
use chrono_tz::Asia::Shanghai;
use csv::{ReaderBuilder, StringRecord};
use std::collections::HashMap;
use std::fs::{self, File};
use std::io::{BufReader, Read};
use std::path::{Path, PathBuf};
use zip::ZipArchive;

use crate::codec::QUEUE_LEVELS;
use crate::events::{QueueSnapshot, Snapshot};
use crate::session::Exchange;
use crate::universe::{is_maintained_product, product_id};

pub const DEFAULT_LOOKBACK_DAYS: u64 = 16;
pub const COMM_L2_LOOKBACK_DAYS: u64 = 1;
pub const DEFAULT_OVERLAP_CUT: &str = "2025-11-03";

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum Layer {
    CommL2,
    Msg,
}

#[derive(Clone, Debug)]
pub struct LocatedFile {
    pub path: PathBuf,
    pub layer: Layer,
    pub directory_day: NaiveDate,
    pub kind: FileKind,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum FileKind {
    Future,
    OrderQueue,
}

#[derive(Clone, Debug)]
struct Columns {
    instrument: usize,
    trad_day: usize,
    action_day: Option<usize>,
    update_time: Option<usize>,
    volume: Option<usize>,
    turnover: Option<usize>,
    last_price: Option<usize>,
    open_int: Option<usize>,
    sequence: Option<usize>,
    bid_prices: [Option<usize>; 5],
    bid_sizes: [Option<usize>; 5],
    ask_prices: [Option<usize>; 5],
    ask_sizes: [Option<usize>; 5],
    is_combo: bool,
}

#[derive(Clone, Debug)]
struct QueueColumns {
    instrument: usize,
    trad_day: usize,
    action_day: Option<usize>,
    update_time: Option<usize>,
    sequence: Option<usize>,
    bid_price: Option<usize>,
    ask_price: Option<usize>,
    bid_qty: [Option<usize>; QUEUE_LEVELS],
    ask_qty: [Option<usize>; QUEUE_LEVELS],
}

impl Columns {
    fn from_header(header: &StringRecord) -> Option<Self> {
        let indexes: HashMap<String, usize> = header
            .iter()
            .enumerate()
            .map(|(index, name)| {
                (
                    name.trim_start_matches('\u{feff}')
                        .trim()
                        .to_ascii_lowercase(),
                    index,
                )
            })
            .collect();
        let required = |names: &[&str]| names.iter().find_map(|name| indexes.get(*name).copied());
        let optional = |names: &[&str]| required(names);
        let instrument = required(&["instruid", "instrument_id"])?;
        let trad_day = required(&["tradday", "trad_day"])?;
        let level = |prefix: &str, suffix: &str| {
            let mut slots = [None; 5];
            for (index, slot) in slots.iter_mut().enumerate() {
                let name = format!("{prefix}{}{suffix}", index + 1);
                *slot = indexes.get(&name).copied();
            }
            slots
        };
        Some(Self {
            instrument,
            trad_day,
            action_day: optional(&["actionday", "action_day"]),
            update_time: optional(&["updatetime", "update_time"]),
            volume: optional(&["volume"]),
            turnover: optional(&["turnover"]),
            last_price: optional(&["lastprice", "last_price"]),
            open_int: optional(&["openint", "open_int"]),
            sequence: optional(&["seqno", "seq_no"]),
            bid_prices: level("bidprice", ""),
            bid_sizes: level("bidvolume", ""),
            ask_prices: level("askprice", ""),
            ask_sizes: level("askvolume", ""),
            is_combo: ["cmbtypeid", "firstinstruid", "secondinstruid"]
                .iter()
                .any(|name| indexes.contains_key(*name)),
        })
    }
}

impl QueueColumns {
    fn from_header(header: &StringRecord) -> Option<Self> {
        let indexes: HashMap<String, usize> = header
            .iter()
            .enumerate()
            .map(|(index, name)| {
                (
                    name.trim_start_matches('\u{feff}')
                        .trim()
                        .to_ascii_lowercase(),
                    index,
                )
            })
            .collect();
        let required = |names: &[&str]| names.iter().find_map(|name| indexes.get(*name).copied());
        let optional = |names: &[&str]| required(names);
        let instrument = required(&["instruid", "instrument_id"])?;
        let trad_day = required(&["tradday", "trad_day"])?;
        if optional(&["bidorderqty1"]).is_none() && optional(&["askorderqty1"]).is_none() {
            return None;
        }
        let qty = |prefix: &str| {
            let mut slots = [None; QUEUE_LEVELS];
            for (index, slot) in slots.iter_mut().enumerate() {
                let name = format!("{prefix}{}", index + 1);
                *slot = indexes.get(&name).copied();
            }
            slots
        };
        Some(Self {
            instrument,
            trad_day,
            action_day: optional(&["actionday", "action_day"]),
            update_time: optional(&["updatetime", "update_time"]),
            sequence: optional(&["seqno", "seq_no"]),
            bid_price: optional(&["bidprice"]),
            ask_price: optional(&["askprice"]),
            bid_qty: qty("bidorderqty"),
            ask_qty: qty("askorderqty"),
        })
    }
}

pub fn parse_day(value: &str) -> Result<NaiveDate> {
    let compact = compact_day(value).ok_or_else(|| anyhow::anyhow!("invalid date: {value:?}"))?;
    NaiveDate::parse_from_str(&compact, "%Y%m%d").context("parse date")
}

pub fn compact_day(value: &str) -> Option<String> {
    let compact = value.trim().replace('-', "");
    (compact.len() == 8 && compact.bytes().all(|byte| byte.is_ascii_digit())).then_some(compact)
}

fn parse_directory_day(name: &str) -> Option<NaiveDate> {
    compact_day(name).and_then(|value| NaiveDate::parse_from_str(&value, "%Y%m%d").ok())
}

fn list_directories(
    root: &Path,
    start: NaiveDate,
    end: NaiveDate,
) -> Result<Vec<(NaiveDate, PathBuf)>> {
    let mut directories = Vec::new();
    if !root.is_dir() {
        return Ok(directories);
    }
    for entry in fs::read_dir(root).with_context(|| format!("read {root:?}"))? {
        let entry = entry?;
        if !entry.file_type()?.is_dir() {
            continue;
        }
        let name = entry.file_name();
        let Some(day) = parse_directory_day(&name.to_string_lossy()) else {
            continue;
        };
        if day >= start && day <= end {
            directories.push((day, entry.path()));
        }
    }
    directories.sort_by_key(|(day, _)| *day);
    Ok(directories)
}

/// CFFEX trades day sessions only, so its quote file marks a directory that
/// holds a full open day (Saturday directories carry only the SHFE/INE night tail).
const MSG_OPEN_DAY_MARKER: &str = "mdl_21_1_0.csv";

/// `tonglian_msg` directories that can hold rows of `trad_day`: the day's own
/// directory, back through Saturday / holiday tails, up to and including the
/// previous open day (whose evening opens the night session). Older
/// directories inside the lookback window cannot contain `trad_day` rows and
/// re-reading them only multiplies NFS IO. Without a marker in the window the
/// whole window is kept.
fn msg_directories(
    root: &Path,
    trad_day: NaiveDate,
    lookback_days: u64,
) -> Result<Vec<(NaiveDate, PathBuf)>> {
    let start = trad_day
        .checked_sub_days(Days::new(lookback_days))
        .unwrap_or(trad_day);
    let all = list_directories(root, start, trad_day)?;
    let cut = all
        .iter()
        .rposition(|(day, dir)| *day < trad_day && dir.join(MSG_OPEN_DAY_MARKER).is_file());
    Ok(match cut {
        Some(index) => all[index..].to_vec(),
        None => all,
    })
}

pub fn resolve_layer(
    trad_day: NaiveDate,
    l2_root: Option<&Path>,
    msg_root: Option<&Path>,
    overlap_cut: Option<NaiveDate>,
) -> (Option<PathBuf>, Option<PathBuf>) {
    match (l2_root, msg_root, overlap_cut) {
        (Some(l2), Some(_), Some(cut)) if trad_day <= cut => (Some(l2.to_path_buf()), None),
        (Some(_), Some(msg), Some(_)) => (None, Some(msg.to_path_buf())),
        (l2, msg, _) => (l2.map(Path::to_path_buf), msg.map(Path::to_path_buf)),
    }
}

pub fn source_files_for_day(
    exchange: Exchange,
    trad_day: NaiveDate,
    l2_root: Option<&Path>,
    msg_root: Option<&Path>,
    overlap_cut: Option<NaiveDate>,
    lookback_days: u64,
) -> Result<Vec<LocatedFile>> {
    let (layer_l2, layer_msg) = resolve_layer(trad_day, l2_root, msg_root, overlap_cut);
    let mut files = Vec::new();
    if let Some(root) = layer_l2 {
        let start = trad_day
            .checked_sub_days(Days::new(COMM_L2_LOOKBACK_DAYS))
            .unwrap_or(trad_day);
        for (day, directory) in list_directories(&root.join(exchange.as_str()), start, trad_day)? {
            for entry in fs::read_dir(directory)? {
                let entry = entry?;
                if !entry.file_type()?.is_file() {
                    continue;
                }
                let lower = entry.file_name().to_string_lossy().to_ascii_lowercase();
                if lower.starts_with("future_")
                    && (lower.ends_with(".zip") || lower.ends_with(".csv"))
                {
                    files.push(LocatedFile {
                        path: entry.path(),
                        layer: Layer::CommL2,
                        directory_day: day,
                        kind: FileKind::Future,
                    });
                }
            }
        }
    }
    if let Some(root) = layer_msg {
        let expected = exchange.msg_stem();
        for (day, directory) in msg_directories(&root, trad_day, lookback_days)? {
            let path = directory.join(expected);
            if path.is_file() {
                files.push(LocatedFile {
                    path,
                    layer: Layer::Msg,
                    directory_day: day,
                    kind: FileKind::Future,
                });
            }
        }
    }
    files.sort_by(|left, right| {
        (left.layer as u8, left.directory_day, &left.path).cmp(&(
            right.layer as u8,
            right.directory_day,
            &right.path,
        ))
    });
    Ok(files)
}

pub fn source_queue_files_for_day(
    exchange: Exchange,
    trad_day: NaiveDate,
    l2_root: Option<&Path>,
    msg_root: Option<&Path>,
    overlap_cut: Option<NaiveDate>,
    lookback_days: u64,
) -> Result<Vec<LocatedFile>> {
    if !exchange.has_order_queue() {
        return Ok(Vec::new());
    }
    let (layer_l2, layer_msg) = resolve_layer(trad_day, l2_root, msg_root, overlap_cut);
    let mut files = Vec::new();
    if let (Some(root), Some(prefix)) = (layer_l2, exchange.comm_l2_queue_prefix()) {
        let start = trad_day
            .checked_sub_days(Days::new(COMM_L2_LOOKBACK_DAYS))
            .unwrap_or(trad_day);
        for (day, directory) in list_directories(&root.join(exchange.as_str()), start, trad_day)? {
            for entry in fs::read_dir(directory)? {
                let entry = entry?;
                if !entry.file_type()?.is_file() {
                    continue;
                }
                let lower = entry.file_name().to_string_lossy().to_ascii_lowercase();
                if lower.starts_with(prefix) && (lower.ends_with(".zip") || lower.ends_with(".csv"))
                {
                    files.push(LocatedFile {
                        path: entry.path(),
                        layer: Layer::CommL2,
                        directory_day: day,
                        kind: FileKind::OrderQueue,
                    });
                }
            }
        }
    }
    if let (Some(root), Some(expected)) = (layer_msg, exchange.msg_queue_stem()) {
        for (day, directory) in msg_directories(&root, trad_day, lookback_days)? {
            let path = directory.join(expected);
            if path.is_file() {
                files.push(LocatedFile {
                    path,
                    layer: Layer::Msg,
                    directory_day: day,
                    kind: FileKind::OrderQueue,
                });
            }
        }
    }
    files.sort_by(|left, right| {
        (left.layer as u8, left.directory_day, &left.path).cmp(&(
            right.layer as u8,
            right.directory_day,
            &right.path,
        ))
    });
    Ok(files)
}

fn cell(record: &StringRecord, index: Option<usize>) -> &str {
    index.and_then(|value| record.get(value)).unwrap_or("")
}

fn parse_float(value: &str) -> Option<f64> {
    let text = value.trim();
    if text.is_empty() {
        return None;
    }
    text.parse::<f64>().ok().filter(|price| price.is_finite())
}

fn parse_update_time(value: &str) -> Option<NaiveTime> {
    let text = value.trim();
    if text.is_empty() {
        return None;
    }
    NaiveTime::parse_from_str(text, "%H:%M:%S%.f")
        .or_else(|_| NaiveTime::parse_from_str(text, "%H:%M:%S"))
        .ok()
}

fn sequence(value: &str, source_row: u32) -> i64 {
    value
        .trim()
        .parse::<i64>()
        .or_else(|_| value.trim().parse::<f64>().map(|value| value as i64))
        .unwrap_or(source_row as i64)
}

fn levels(record: &StringRecord, indexes: [Option<usize>; 5]) -> [Option<f64>; 5] {
    let mut out = [None; 5];
    for (slot, index) in out.iter_mut().zip(indexes) {
        *slot = parse_float(cell(record, index));
    }
    out
}

fn queue_levels(
    record: &StringRecord,
    indexes: [Option<usize>; QUEUE_LEVELS],
) -> [Option<f64>; QUEUE_LEVELS] {
    let mut out = [None; QUEUE_LEVELS];
    for (slot, index) in out.iter_mut().zip(indexes) {
        *slot = parse_float(cell(record, index)).filter(|value| *value != 0.0);
    }
    out
}

fn row_event(
    row: &StringRecord,
    instrument_idx: usize,
    trad_day_idx: usize,
    action_day: Option<usize>,
    update_time: Option<usize>,
    trad_day: NaiveDate,
) -> Option<(String, String, NaiveDate, chrono::DateTime<chrono_tz::Tz>)> {
    let instrument = cell(row, Some(instrument_idx)).trim();
    let product = product_id(instrument)?;
    if !is_maintained_product(&product) {
        return None;
    }
    let trad_key = compact_day(cell(row, Some(trad_day_idx)))?;
    let row_day = NaiveDate::parse_from_str(&trad_key, "%Y%m%d").ok()?;
    if row_day != trad_day {
        return None;
    }
    let action_key = action_day.and_then(|index| compact_day(cell(row, Some(index))))?;
    let action_day = NaiveDate::parse_from_str(&action_key, "%Y%m%d").ok()?;
    let clock = parse_update_time(cell(row, update_time))?;
    let event_time = Shanghai
        .from_local_datetime(&action_day.and_time(clock))
        .single()?;
    Some((instrument.to_owned(), product, row_day, event_time))
}

fn collect_snapshots<R: Read>(
    reader: R,
    trad_day: NaiveDate,
    file_order: u32,
    out: &mut Vec<Snapshot>,
) -> Result<()> {
    let mut csv = ReaderBuilder::new().flexible(true).from_reader(reader);
    let header = csv.headers()?.clone();
    let Some(columns) = Columns::from_header(&header) else {
        return Ok(());
    };
    collect_rows(&mut csv, &columns, trad_day, file_order, 0, None, out)?;
    Ok(())
}

/// Parses the data rows of one future-quote CSV into `out`. `row_base` is the
/// number of data rows already consumed before this reader (so `source_row`
/// stays the file-wide row number); `products` optionally keeps only those
/// product ids (compared case-insensitively). Returns the rows consumed.
fn collect_rows<R: Read>(
    csv: &mut csv::Reader<R>,
    columns: &Columns,
    trad_day: NaiveDate,
    file_order: u32,
    row_base: u32,
    products: Option<&[String]>,
    out: &mut Vec<Snapshot>,
) -> Result<u32> {
    if columns.is_combo {
        return Ok(0);
    }
    let mut consumed = 0u32;
    for (offset, row) in csv.records().enumerate() {
        let row = row?;
        consumed = (offset + 1) as u32;
        let source_row = row_base + consumed;
        let instrument = cell(&row, Some(columns.instrument)).trim();
        let Some(product) = product_id(instrument) else {
            continue;
        };
        if !is_maintained_product(&product) {
            continue;
        }
        if products.is_some_and(|keep| {
            !keep.is_empty()
                && !keep
                    .iter()
                    .any(|p| p == "*" || p.eq_ignore_ascii_case(&product))
        }) {
            continue;
        }
        let Some(trad_key) = compact_day(cell(&row, Some(columns.trad_day))) else {
            continue;
        };
        let row_day = NaiveDate::parse_from_str(&trad_key, "%Y%m%d")?;
        if row_day != trad_day {
            continue;
        }
        let Some(action_key) = columns
            .action_day
            .and_then(|index| compact_day(cell(&row, Some(index))))
        else {
            continue;
        };
        let Some(action_day) = NaiveDate::parse_from_str(&action_key, "%Y%m%d").ok() else {
            continue;
        };
        let Some(clock) = parse_update_time(cell(&row, columns.update_time)) else {
            continue;
        };
        let Some(event_time) = Shanghai
            .from_local_datetime(&action_day.and_time(clock))
            .single()
        else {
            continue;
        };
        out.push(Snapshot {
            trad_day: row_day,
            instrument_id: instrument.to_owned(),
            product_id: product,
            event_time,
            seq: sequence(cell(&row, columns.sequence), source_row),
            source_row,
            file_order,
            volume: parse_float(cell(&row, columns.volume)),
            turnover: parse_float(cell(&row, columns.turnover)),
            last_price: parse_float(cell(&row, columns.last_price)),
            open_int: parse_float(cell(&row, columns.open_int)),
            bid_prices: levels(&row, columns.bid_prices),
            bid_sizes: levels(&row, columns.bid_sizes),
            ask_prices: levels(&row, columns.ask_prices),
            ask_sizes: levels(&row, columns.ask_sizes),
        });
    }
    Ok(consumed)
}

pub fn read_located_file(
    located: &LocatedFile,
    trad_day: NaiveDate,
    file_order: u32,
    out: &mut Vec<Snapshot>,
) -> Result<()> {
    if located
        .path
        .extension()
        .is_some_and(|extension| extension.eq_ignore_ascii_case("zip"))
    {
        let file = File::open(&located.path).with_context(|| format!("open {:?}", located.path))?;
        let mut archive = ZipArchive::new(file)?;
        for index in 0..archive.len() {
            let member = archive.by_index(index)?;
            let name = member.name().to_ascii_lowercase();
            if !name.ends_with(".csv")
                || name.contains("futcmb")
                || name.contains("futtqs")
                || name.contains("futorderq")
            {
                continue;
            }
            collect_snapshots(member, trad_day, file_order, out)?;
        }
        return Ok(());
    }
    let file = File::open(&located.path).with_context(|| format!("open {:?}", located.path))?;
    collect_snapshots(BufReader::new(file), trad_day, file_order, out)
}

/// Read position inside one growing `msg` CSV: bytes consumed (always ending
/// on a line boundary), data rows consumed, and the parsed header.
#[derive(Debug, Default)]
pub struct TailState {
    offset: u64,
    rows: u32,
    columns: Option<Columns>,
}

impl TailState {
    /// Resume reading at a stored position: `offset` must be a line start and
    /// `rows` the number of data rows before it (so `source_row` stays the
    /// file-wide row number). The header is read again on the first call.
    pub fn resume_at(offset: u64, rows: u32) -> Self {
        Self {
            offset,
            rows,
            columns: None,
        }
    }

    pub fn offset(&self) -> u64 {
        self.offset
    }

    pub fn rows(&self) -> u32 {
        self.rows
    }
}

const TAIL_CHUNK: usize = 64 << 20;

/// Appends the snapshots of `trad_day` found in the complete lines written to
/// `path` since the previous call. A trailing partial line is left for the
/// next call; a file that shrank is re-read from the start (the caller must
/// discard its earlier snapshots in that case, signalled by the `true`
/// return). `products` keeps only those product ids.
pub fn read_tail(
    path: &Path,
    trad_day: NaiveDate,
    file_order: u32,
    products: &[String],
    state: &mut TailState,
    out: &mut Vec<Snapshot>,
) -> Result<bool> {
    read_tail_limited(path, trad_day, file_order, products, state, None, out)
}

/// [`read_tail`] that stops at byte `limit` (a stored line-boundary position)
/// instead of the current end of file: used to replay a known range, and to
/// bound how much one call parses (and holds) at a time.
pub fn read_tail_limited(
    path: &Path,
    trad_day: NaiveDate,
    file_order: u32,
    products: &[String],
    state: &mut TailState,
    limit: Option<u64>,
    out: &mut Vec<Snapshot>,
) -> Result<bool> {
    use std::io::{Seek, SeekFrom};
    let mut file = File::open(path).with_context(|| format!("open {path:?}"))?;
    let file_len = file.metadata()?.len();
    let len = limit.map_or(file_len, |l| l.min(file_len));
    let reset = file_len < state.offset;
    if reset {
        *state = TailState::default();
    }
    if state.columns.is_none() {
        file.seek(SeekFrom::Start(0))?;
        let mut head = Vec::new();
        let mut reader = BufReader::new(&file);
        std::io::BufRead::read_until(&mut reader, b'\n', &mut head)?;
        if head.last() != Some(&b'\n') {
            return Ok(reset);
        }
        let mut csv = ReaderBuilder::new()
            .has_headers(false)
            .flexible(true)
            .from_reader(head.as_slice());
        let header = csv.records().next().transpose()?.unwrap_or_default();
        let Some(columns) = Columns::from_header(&header) else {
            bail!("{path:?}: unrecognised MDL header");
        };
        state.columns = Some(columns);
        if state.offset == 0 {
            state.offset = head.len() as u64;
        }
    }
    let columns = state.columns.clone().expect("set above");
    while state.offset < len {
        file.seek(SeekFrom::Start(state.offset))?;
        let want = ((len - state.offset) as usize).min(TAIL_CHUNK);
        let mut buf = vec![0u8; want];
        file.read_exact(&mut buf)?;
        let Some(end) = buf.iter().rposition(|b| *b == b'\n') else {
            if want < TAIL_CHUNK {
                break; // only a partial line so far
            }
            bail!("{path:?}: line longer than {TAIL_CHUNK} bytes");
        };
        buf.truncate(end + 1);
        let mut csv = ReaderBuilder::new()
            .has_headers(false)
            .flexible(true)
            .from_reader(buf.as_slice());
        let n = collect_rows(
            &mut csv,
            &columns,
            trad_day,
            file_order,
            state.rows,
            Some(products),
            out,
        )?;
        state.rows += n;
        state.offset += buf.len() as u64;
    }
    Ok(reset)
}

/// Trading day of the last complete row of a `msg` CSV (reads only its tail).
pub fn last_trad_day(path: &Path) -> Result<Option<NaiveDate>> {
    use std::io::{Seek, SeekFrom};
    let mut file = File::open(path).with_context(|| format!("open {path:?}"))?;
    let len = file.metadata()?.len();
    let mut head = Vec::new();
    std::io::BufRead::read_until(&mut BufReader::new(&file), b'\n', &mut head)?;
    let mut csv = ReaderBuilder::new()
        .has_headers(false)
        .from_reader(head.as_slice());
    let Some(header) = csv.records().next().transpose()? else {
        return Ok(None);
    };
    let Some(columns) = Columns::from_header(&header) else {
        return Ok(None);
    };
    let from = len.saturating_sub(1 << 20).max(head.len() as u64);
    file.seek(SeekFrom::Start(from))?;
    let mut buf = Vec::new();
    file.take(len - from).read_to_end(&mut buf)?;
    let Some(end) = buf.iter().rposition(|b| *b == b'\n') else {
        return Ok(None);
    };
    let start = buf[..end]
        .iter()
        .rposition(|b| *b == b'\n')
        .map_or(0, |i| i + 1);
    let mut csv = ReaderBuilder::new()
        .has_headers(false)
        .flexible(true)
        .from_reader(&buf[start..=end]);
    let Some(row) = csv.records().next().transpose()? else {
        return Ok(None);
    };
    Ok(compact_day(cell(&row, Some(columns.trad_day)))
        .and_then(|key| NaiveDate::parse_from_str(&key, "%Y%m%d").ok()))
}

fn collect_queues<R: Read>(
    reader: R,
    trad_day: NaiveDate,
    file_order: u32,
    out: &mut Vec<QueueSnapshot>,
) -> Result<()> {
    let mut csv = ReaderBuilder::new().flexible(true).from_reader(reader);
    let header = csv.headers()?.clone();
    let Some(columns) = QueueColumns::from_header(&header) else {
        return Ok(());
    };
    for (offset, row) in csv.records().enumerate() {
        let row = row?;
        let source_row = (offset + 1) as u32;
        let Some((instrument, product, row_day, event_time)) = row_event(
            &row,
            columns.instrument,
            columns.trad_day,
            columns.action_day,
            columns.update_time,
            trad_day,
        ) else {
            continue;
        };
        let snap = QueueSnapshot {
            trad_day: row_day,
            instrument_id: instrument,
            product_id: product,
            event_time,
            seq: sequence(cell(&row, columns.sequence), source_row),
            source_row,
            file_order,
            bid_price: parse_float(cell(&row, columns.bid_price)).filter(|value| *value > 0.0),
            ask_price: parse_float(cell(&row, columns.ask_price)).filter(|value| *value > 0.0),
            bid_qty: queue_levels(&row, columns.bid_qty),
            ask_qty: queue_levels(&row, columns.ask_qty),
        };
        if snap.has_side() {
            out.push(snap);
        }
    }
    Ok(())
}

pub fn read_located_queue_file(
    located: &LocatedFile,
    trad_day: NaiveDate,
    file_order: u32,
    out: &mut Vec<QueueSnapshot>,
) -> Result<()> {
    if located
        .path
        .extension()
        .is_some_and(|extension| extension.eq_ignore_ascii_case("zip"))
    {
        let file = File::open(&located.path).with_context(|| format!("open {:?}", located.path))?;
        let mut archive = ZipArchive::new(file)?;
        for index in 0..archive.len() {
            let member = archive.by_index(index)?;
            let name = member.name().to_ascii_lowercase();
            if !name.ends_with(".csv") {
                continue;
            }
            collect_queues(member, trad_day, file_order, out)?;
        }
        return Ok(());
    }
    let file = File::open(&located.path).with_context(|| format!("open {:?}", located.path))?;
    collect_queues(BufReader::new(file), trad_day, file_order, out)
}

pub fn load_trad_day(
    exchange: Exchange,
    trad_day: NaiveDate,
    l2_root: Option<&Path>,
    msg_root: Option<&Path>,
    overlap_cut: Option<NaiveDate>,
    lookback_days: u64,
) -> Result<Vec<Snapshot>> {
    let files = source_files_for_day(
        exchange,
        trad_day,
        l2_root,
        msg_root,
        overlap_cut,
        lookback_days,
    )?;
    let mut snapshots = Vec::new();
    for (file_order, located) in files.iter().enumerate() {
        read_located_file(located, trad_day, file_order as u32, &mut snapshots)?;
    }
    Ok(snapshots)
}

pub fn load_queue_day(
    exchange: Exchange,
    trad_day: NaiveDate,
    l2_root: Option<&Path>,
    msg_root: Option<&Path>,
    overlap_cut: Option<NaiveDate>,
    lookback_days: u64,
) -> Result<Vec<QueueSnapshot>> {
    let files = source_queue_files_for_day(
        exchange,
        trad_day,
        l2_root,
        msg_root,
        overlap_cut,
        lookback_days,
    )?;
    let mut snapshots = Vec::new();
    for (file_order, located) in files.iter().enumerate() {
        read_located_queue_file(located, trad_day, file_order as u32, &mut snapshots)?;
    }
    Ok(snapshots)
}

pub fn iter_calendar_days(start: NaiveDate, end: NaiveDate) -> Result<Vec<NaiveDate>> {
    if end < start {
        bail!("end precedes start");
    }
    let mut days = Vec::new();
    let mut cursor = start;
    while cursor <= end {
        days.push(cursor);
        cursor = cursor
            .succ_opt()
            .ok_or_else(|| anyhow::anyhow!("date overflow after {cursor}"))?;
    }
    Ok(days)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn overlap_cut_picks_one_layer() {
        let cut = NaiveDate::from_ymd_opt(2025, 11, 3).unwrap();
        let l2 = Path::new("/l2");
        let msg = Path::new("/msg");
        let before = resolve_layer(cut, Some(l2), Some(msg), Some(cut));
        let after = resolve_layer(
            NaiveDate::from_ymd_opt(2025, 11, 4).unwrap(),
            Some(l2),
            Some(msg),
            Some(cut),
        );
        assert_eq!(before, (Some(l2.to_path_buf()), None));
        assert_eq!(after, (None, Some(msg.to_path_buf())));
    }
}

#[cfg(test)]
mod msg_directory_tests {
    use super::*;

    fn day(s: &str) -> NaiveDate {
        NaiveDate::parse_from_str(s, "%Y%m%d").unwrap()
    }

    #[test]
    fn monday_reads_friday_saturday_monday_only() {
        let t = tempfile::tempdir().unwrap();
        // Thu, Fri, Mon are open days; Sat holds only the SHFE night tail.
        for (d, open) in [
            ("20261112", true),
            ("20261113", true),
            ("20261114", false),
            ("20261116", true),
        ] {
            let dir = t.path().join(d);
            fs::create_dir_all(&dir).unwrap();
            fs::write(
                dir.join(if open {
                    MSG_OPEN_DAY_MARKER
                } else {
                    "mdl_22_1_0.csv"
                }),
                "",
            )
            .unwrap();
        }
        let got: Vec<NaiveDate> = msg_directories(t.path(), day("20261116"), 16)
            .unwrap()
            .into_iter()
            .map(|(d, _)| d)
            .collect();
        assert_eq!(got, vec![day("20261113"), day("20261114"), day("20261116")]);
        let fri: Vec<NaiveDate> = msg_directories(t.path(), day("20261113"), 16)
            .unwrap()
            .into_iter()
            .map(|(d, _)| d)
            .collect();
        assert_eq!(fri, vec![day("20261112"), day("20261113")]);
    }

    #[test]
    fn without_open_day_marker_the_whole_window_is_kept() {
        let t = tempfile::tempdir().unwrap();
        for d in ["20261110", "20261111", "20261112"] {
            fs::create_dir_all(t.path().join(d)).unwrap();
        }
        assert_eq!(
            msg_directories(t.path(), day("20261112"), 16)
                .unwrap()
                .len(),
            3
        );
    }
}

#[cfg(test)]
mod tail_tests {
    use super::*;
    use std::io::Write;

    const HEAD: &str =
        "InstruID,TradDay,ActionDay,UpdateTime,Volume,Turnover,LastPrice,OpenInt,SeqNo\n";

    fn day() -> NaiveDate {
        NaiveDate::from_ymd_opt(2026, 11, 17).unwrap()
    }

    #[test]
    fn tail_reads_only_complete_new_lines_and_filters_products() {
        let t = tempfile::tempdir().unwrap();
        let path = t.path().join("mdl_22_1_0.csv");
        fs::write(&path, format!("{HEAD}sn2701,20261117,20261117,09:00:00.5,1,10,100,5,1\ncu2701,20261117,20261117,09:00:00.5,1,10,100,5,2\nsn2701,20261117,20261117,09:00:01,2,20,10")).unwrap();
        let (mut st, mut out) = (TailState::default(), Vec::new());
        let products = ["SN".to_string()];
        assert!(!read_tail(&path, day(), 0, &products, &mut st, &mut out).unwrap());
        assert_eq!(out.len(), 1, "partial line and other products skipped");
        let mut f = fs::OpenOptions::new().append(true).open(&path).unwrap();
        f.write_all(b"1,5,3\nsn2701,20261116,20261116,21:00:00,3,30,100,5,4\n")
            .unwrap();
        out.clear();
        read_tail(&path, day(), 0, &products, &mut st, &mut out).unwrap();
        assert_eq!(
            out.len(),
            1,
            "completed line read once; other TradDay skipped"
        );
        assert_eq!(out[0].source_row, 3);
        out.clear();
        read_tail(&path, day(), 0, &products, &mut st, &mut out).unwrap();
        assert!(out.is_empty(), "nothing new");
        assert_eq!(
            last_trad_day(&path).unwrap(),
            Some(NaiveDate::from_ymd_opt(2026, 11, 16).unwrap())
        );
        fs::write(&path, HEAD).unwrap();
        assert!(
            read_tail(&path, day(), 0, &products, &mut st, &mut out).unwrap(),
            "shrunk file resets"
        );
    }

    #[test]
    fn limited_reads_and_resume_keep_file_wide_row_numbers() {
        let t = tempfile::tempdir().unwrap();
        let path = t.path().join("mdl_22_1_0.csv");
        let line = |n: u32| format!("sn2701,20261117,20261117,09:00:0{n},{n},10,100,5,{n}\n");
        let mut text = HEAD.to_string();
        for n in 1..=6 {
            text.push_str(&line(n));
        }
        fs::write(&path, &text).unwrap();
        let products = ["SN".to_string()];
        let after_three = (HEAD.len() + (1..=3).map(|n| line(n).len()).sum::<usize>()) as u64;

        let (mut st, mut out) = (TailState::default(), Vec::new());
        read_tail_limited(
            &path,
            day(),
            0,
            &products,
            &mut st,
            Some(after_three),
            &mut out,
        )
        .unwrap();
        assert_eq!(
            out.iter().map(|s| s.source_row).collect::<Vec<_>>(),
            [1, 2, 3]
        );
        assert_eq!((st.offset(), st.rows()), (after_three, 3));
        out.clear();
        read_tail(&path, day(), 0, &products, &mut st, &mut out).unwrap();
        assert_eq!(
            out.iter().map(|s| s.source_row).collect::<Vec<_>>(),
            [4, 5, 6]
        );

        // A restart resumes from the stored position (header re-read, rows kept).
        let (mut resumed, mut again) = (TailState::resume_at(after_three, 3), Vec::new());
        read_tail(&path, day(), 0, &products, &mut resumed, &mut again).unwrap();
        assert_eq!(
            again.iter().map(|s| s.source_row).collect::<Vec<_>>(),
            [4, 5, 6]
        );
        assert_eq!(again[0].seq, 4);
    }
}
