//! Fast, streaming L2 -> end-of-day cross-section extractor for CN roll tables.
//!
//! It intentionally stops at one EOD row per contract. Python remains the
//! source of truth for dominant selection, additive adjustment and auditing.

use anyhow::{bail, Context, Result};
use chrono::{Days, NaiveDate};
use clap::Parser;
use csv::{ReaderBuilder, StringRecord, WriterBuilder};
use rayon::prelude::*;
use std::cmp::Ordering;
use std::collections::{BTreeMap, HashMap};
use std::fs::{self, File};
use std::io::{BufReader, Read};
use std::path::{Path, PathBuf};
use zip::ZipArchive;

const EXCHANGES: &[&str] = &["ccfx", "xdce", "xgfe", "xsge", "xsie", "xzce"];

#[derive(Parser, Debug)]
#[command(about = "Stream Tonglian future files into an all-product EOD CSV")]
struct Args {
    #[arg(long)]
    l2_root: Option<PathBuf>,
    #[arg(long)]
    msg_root: Option<PathBuf>,
    #[arg(long)]
    exchange: String,
    #[arg(long)]
    start: String,
    #[arg(long)]
    end: String,
    #[arg(long)]
    output: PathBuf,
    #[arg(long, default_value = "2025-11-03")]
    overlap_cut: String,
    #[arg(long, default_value_t = 16)]
    lookback_days: u64,
    #[arg(long, default_value_t = 8)]
    workers: usize,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
enum Layer {
    CommL2,
    Msg,
}

#[derive(Clone, Debug)]
struct LocatedFile {
    path: PathBuf,
    layer: Layer,
    directory_day: NaiveDate,
}

#[derive(Clone, Debug, Eq, PartialEq)]
struct Rank {
    event_time: String,
    sequence: i64,
    source_row: usize,
    file_order: usize,
}

impl Ord for Rank {
    fn cmp(&self, other: &Self) -> Ordering {
        (
            &self.event_time,
            self.sequence,
            self.source_row,
            self.file_order,
        )
            .cmp(&(
                &other.event_time,
                other.sequence,
                other.source_row,
                other.file_order,
            ))
    }
}

impl PartialOrd for Rank {
    fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
        Some(self.cmp(other))
    }
}

#[derive(Clone, Debug)]
struct EodRecord {
    trad_day: NaiveDate,
    product_id: String,
    instrument_id: String,
    open_int: String,
    volume: String,
    set_price: String,
    close_price: String,
    last_price: String,
}

#[derive(Clone, Debug)]
struct Candidate {
    rank: Rank,
    record: EodRecord,
}

#[derive(Debug)]
struct Columns {
    instrument: usize,
    trad_day: usize,
    action_day: Option<usize>,
    update_time: Option<usize>,
    open_int: Option<usize>,
    volume: Option<usize>,
    set_price: Option<usize>,
    close_price: Option<usize>,
    last_price: Option<usize>,
    sequence: Option<usize>,
    is_combo: bool,
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
        let instrument = required(&["instruid", "instrument_id"])?;
        let trad_day = required(&["tradday", "trad_day"])?;
        let optional = |names: &[&str]| required(names);
        Some(Self {
            instrument,
            trad_day,
            action_day: optional(&["actionday", "action_day"]),
            update_time: optional(&["updatetime", "update_time"]),
            open_int: optional(&["openint", "open_int"]),
            volume: optional(&["volume"]),
            set_price: optional(&["setprice", "set_price"]),
            close_price: optional(&["closeprice", "close_price"]),
            last_price: optional(&["lastprice", "last_price"]),
            sequence: optional(&["seqno", "seq_no"]),
            is_combo: ["cmbtypeid", "firstinstruid", "secondinstruid"]
                .iter()
                .any(|name| indexes.contains_key(*name)),
        })
    }
}

fn parse_day(value: &str) -> Result<NaiveDate> {
    let compact = compact_day(value).ok_or_else(|| anyhow::anyhow!("invalid date: {value:?}"))?;
    NaiveDate::parse_from_str(&compact, "%Y%m%d").context("parse date")
}

fn compact_day(value: &str) -> Option<String> {
    let compact = value.trim().replace('-', "");
    (compact.len() == 8 && compact.bytes().all(|byte| byte.is_ascii_digit())).then_some(compact)
}

fn cell(record: &StringRecord, index: Option<usize>) -> &str {
    index.and_then(|value| record.get(value)).unwrap_or("")
}

fn product_id(instrument: &str) -> Option<String> {
    let text = instrument.trim();
    if text.is_empty()
        || text.to_ascii_lowercase().contains("efp")
        || text.contains(['&', '-', '_'])
    {
        return None;
    }
    let mut letters = String::new();
    let mut digits_started = false;
    for byte in text.bytes() {
        if byte.is_ascii_alphabetic() {
            if digits_started {
                return None;
            }
            letters.push(byte.to_ascii_uppercase() as char);
        } else if byte.is_ascii_digit() {
            digits_started = true;
        } else {
            return None;
        }
    }
    (!letters.is_empty() && digits_started).then_some(letters)
}

fn sequence(value: &str, source_row: usize) -> i64 {
    value
        .trim()
        .parse::<i64>()
        .or_else(|_| value.trim().parse::<f64>().map(|value| value as i64))
        .unwrap_or(source_row as i64)
}

#[allow(clippy::too_many_arguments)]
fn update_latest<R: Read>(
    reader: R,
    layer: Layer,
    file_order: usize,
    start: NaiveDate,
    end: NaiveDate,
    overlap_cut: NaiveDate,
    enforce_overlap_cut: bool,
    default_layer: Layer,
    product_cache: &mut HashMap<String, Option<String>>,
    latest: &mut HashMap<(NaiveDate, String), Candidate>,
) -> Result<()> {
    let mut csv = ReaderBuilder::new().flexible(true).from_reader(reader);
    let header = csv.headers()?.clone();
    let Some(columns) = Columns::from_header(&header) else {
        return Ok(());
    };
    if columns.is_combo {
        return Ok(());
    }
    for (offset, row) in csv.records().enumerate() {
        let row = row?;
        let source_row = offset + 1;
        let instrument = cell(&row, Some(columns.instrument)).trim();
        let product = product_cache
            .entry(instrument.to_owned())
            .or_insert_with(|| product_id(instrument));
        let Some(product) = product.as_ref() else {
            continue;
        };
        let Some(trad_key) = compact_day(cell(&row, Some(columns.trad_day))) else {
            continue;
        };
        let trad_day = NaiveDate::parse_from_str(&trad_key, "%Y%m%d")?;
        if trad_day < start || trad_day > end {
            continue;
        }
        let expected_layer = if enforce_overlap_cut && overlap_cut < trad_day {
            Layer::Msg
        } else {
            default_layer
        };
        if expected_layer != layer {
            continue;
        }
        let action_key = columns
            .action_day
            .and_then(|index| compact_day(cell(&row, Some(index))));
        let update_time = cell(&row, columns.update_time).trim();
        let rank = Rank {
            event_time: action_key
                .filter(|_| !update_time.is_empty())
                .map(|day| format!("{day}T{update_time}"))
                .unwrap_or_default(),
            sequence: sequence(cell(&row, columns.sequence), source_row),
            source_row,
            file_order,
        };
        let candidate = Candidate {
            rank,
            record: EodRecord {
                trad_day,
                product_id: product.clone(),
                instrument_id: instrument.to_owned(),
                open_int: cell(&row, columns.open_int).to_owned(),
                volume: cell(&row, columns.volume).to_owned(),
                set_price: cell(&row, columns.set_price).to_owned(),
                close_price: cell(&row, columns.close_price).to_owned(),
                last_price: cell(&row, columns.last_price).to_owned(),
            },
        };
        let key = (trad_day, instrument.to_owned());
        if latest
            .get(&key)
            .is_none_or(|previous| prefer_priced_candidate(&candidate, previous))
        {
            latest.insert(key, candidate);
        }
    }
    Ok(())
}

fn parse_directory_day(name: &str) -> Option<NaiveDate> {
    compact_day(name).and_then(|value| NaiveDate::parse_from_str(&value, "%Y%m%d").ok())
}

/// CFFEX trades day sessions only; its quote file marks a full open-day directory.
const MSG_OPEN_DAY_MARKER: &str = "mdl_21_1_0.csv";

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

fn msg_stem(exchange: &str) -> &'static str {
    match exchange {
        "ccfx" => "mdl_21_1_0.csv",
        "xdce" => "mdl_24_1_0.csv",
        "xgfe" => "mdl_26_1_0.csv",
        "xsge" => "mdl_22_1_0.csv",
        "xsie" => "mdl_22_3_0.csv",
        "xzce" => "mdl_23_1_0.csv",
        _ => unreachable!(),
    }
}

fn source_files(
    args: &Args,
    start: NaiveDate,
    end: NaiveDate,
    cut: NaiveDate,
) -> Result<Vec<LocatedFile>> {
    let mut files = Vec::new();
    if let Some(root) = &args.l2_root {
        let comm_end = if args.msg_root.is_some() {
            end.min(cut)
        } else {
            end
        };
        let comm_start = start.checked_sub_days(Days::new(1)).unwrap_or(start);
        for (day, directory) in list_directories(&root.join(&args.exchange), comm_start, comm_end)?
        {
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
                    });
                }
            }
        }
    }
    if let Some(root) = &args.msg_root {
        let msg_start = if args.l2_root.is_some() {
            start.max(cut + Days::new(1))
        } else {
            start
        };
        let lookup_start = msg_start
            .checked_sub_days(Days::new(args.lookback_days))
            .unwrap_or(msg_start);
        let expected = msg_stem(&args.exchange);
        let mut dirs = list_directories(root, lookup_start, end)?;
        // Rows of msg_start can only live in its own directory or back to the
        // previous open day (CFFEX file present), where its night session
        // opens; older lookback directories only multiply NFS reads.
        if let Some(cut) = dirs
            .iter()
            .rposition(|(day, dir)| *day < msg_start && dir.join(MSG_OPEN_DAY_MARKER).is_file())
        {
            dirs.drain(..cut);
        }
        for (day, directory) in dirs {
            let path = directory.join(expected);
            if path.is_file() {
                files.push(LocatedFile {
                    path,
                    layer: Layer::Msg,
                    directory_day: day,
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

#[allow(clippy::too_many_arguments)]
fn read_file(
    located: &LocatedFile,
    file_order: usize,
    start: NaiveDate,
    end: NaiveDate,
    cut: NaiveDate,
    enforce_overlap_cut: bool,
    default_layer: Layer,
    product_cache: &mut HashMap<String, Option<String>>,
    latest: &mut HashMap<(NaiveDate, String), Candidate>,
) -> Result<()> {
    if located
        .path
        .extension()
        .is_some_and(|extension| extension.eq_ignore_ascii_case("zip"))
    {
        let file = File::open(&located.path)?;
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
            update_latest(
                member,
                located.layer,
                file_order,
                start,
                end,
                cut,
                enforce_overlap_cut,
                default_layer,
                product_cache,
                latest,
            )?;
        }
        return Ok(());
    }
    let file = File::open(&located.path)?;
    update_latest(
        BufReader::new(file),
        located.layer,
        file_order,
        start,
        end,
        cut,
        enforce_overlap_cut,
        default_layer,
        product_cache,
        latest,
    )
}

fn main() -> Result<()> {
    let args = Args::parse();
    if !EXCHANGES.contains(&args.exchange.as_str()) {
        bail!("unsupported exchange: {}", args.exchange);
    }
    if args.l2_root.is_none() && args.msg_root.is_none() {
        bail!("--l2-root or --msg-root is required");
    }
    let start = parse_day(&args.start)?;
    let end = parse_day(&args.end)?;
    let cut = parse_day(&args.overlap_cut)?;
    if end < start {
        bail!("end precedes start");
    }
    if args.l2_root.is_some() && args.msg_root.is_some() && args.overlap_cut.is_empty() {
        bail!("--overlap-cut is required with both roots");
    }
    if args.workers == 0 {
        bail!("--workers must be positive");
    }

    let files = source_files(&args, start, end, cut)?;
    let enforce_overlap_cut = args.l2_root.is_some() && args.msg_root.is_some();
    let default_layer = if args.l2_root.is_some() {
        Layer::CommL2
    } else {
        Layer::Msg
    };
    let pool = rayon::ThreadPoolBuilder::new()
        .num_threads(args.workers)
        .build()
        .context("build Rayon worker pool")?;
    let parsed: Vec<Result<HashMap<(NaiveDate, String), Candidate>>> = pool.install(|| {
        files
            .par_iter()
            .enumerate()
            .map(|(file_order, located)| {
                let mut latest = HashMap::new();
                let mut product_cache = HashMap::new();
                read_file(
                    located,
                    file_order,
                    start,
                    end,
                    cut,
                    enforce_overlap_cut,
                    default_layer,
                    &mut product_cache,
                    &mut latest,
                )?;
                Ok(latest)
            })
            .collect()
    });
    let mut latest = HashMap::new();
    for partial in parsed {
        for (key, candidate) in partial? {
            if latest
                .get(&key)
                .is_none_or(|previous: &Candidate| prefer_priced_candidate(&candidate, previous))
            {
                latest.insert(key, candidate);
            }
        }
    }

    let row_count = latest.len();
    let mut sorted = BTreeMap::new();
    for ((day, instrument), candidate) in latest {
        sorted.insert((day, instrument), candidate.record);
    }
    let parent = args.output.parent().unwrap_or_else(|| Path::new("."));
    fs::create_dir_all(parent)?;
    let mut writer = WriterBuilder::new().from_path(&args.output)?;
    writer.write_record([
        "trad_day",
        "product_id",
        "instrument_id",
        "open_int",
        "volume",
        "close",
        "expired",
    ])?;
    for (_, record) in sorted {
        writer.write_record([
            record.trad_day.format("%Y-%m-%d").to_string(),
            record.product_id,
            record.instrument_id,
            record.open_int,
            record.volume,
            reference_close(record.set_price, record.close_price, record.last_price),
            "0".to_owned(),
        ])?;
    }
    writer.flush()?;
    eprintln!(
        "eod extract ok exchange={} files={} rows={}",
        args.exchange,
        files.len(),
        row_count
    );
    Ok(())
}

fn positive_price(value: &str) -> bool {
    value
        .trim()
        .parse::<f64>()
        .is_ok_and(|price| price.is_finite() && price > 0.0)
}

fn record_has_close(record: &EodRecord) -> bool {
    positive_price(&record.set_price)
        || positive_price(&record.close_price)
        || positive_price(&record.last_price)
}

fn prefer_priced_candidate(candidate: &Candidate, previous: &Candidate) -> bool {
    let candidate_ok = record_has_close(&candidate.record);
    let previous_ok = record_has_close(&previous.record);
    if candidate_ok != previous_ok {
        return candidate_ok;
    }
    candidate.rank >= previous.rank
}

fn reference_close(set_price: String, close_price: String, last_price: String) -> String {
    for value in [set_price, close_price, last_price] {
        if positive_price(&value) {
            return value;
        }
    }
    String::new()
}
