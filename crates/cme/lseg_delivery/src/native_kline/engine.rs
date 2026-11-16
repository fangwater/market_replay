use super::{
    component,
    io::{self, Kline, Problems, Stats, StatusRow},
    session::{self, Decoder, State, Timeline},
    DAY, MINUTE,
};
use crate::tasdb::{self, Tuning};
use anyhow::{ensure, Context, Result};
use cme_tas_replay::{
    decode_cme_correction, decode_cme_trade, product::encode_all_key, CME_STATUS_LEGACY_LEN,
    CME_STATUS_LEN, STATUS_QUALIFIER_LEGACY_LEN, STATUS_QUALIFIER_LEN,
};
use parquet::arrow::arrow_reader::ParquetRecordBatchReaderBuilder;
use parquet::arrow::{
    arrow_reader::{RowSelection, RowSelector},
    ProjectionMask,
};
use rocksdb::{DBRawIteratorWithThreadMode, Options, ReadOptions, DB};
use serde::{Deserialize, Serialize};
use serde_json::{json, Value};
use sha2::{Digest, Sha256};
use std::{
    collections::{BTreeMap, BTreeSet, HashMap, HashSet},
    fs::{self, File},
    path::{Path, PathBuf},
    sync::{Arc, Mutex},
    time::Instant,
};

#[path = "missing_status.rs"]
mod missing_status;
pub use missing_status::run as supplement_status;

#[derive(Clone, Debug, Deserialize, Serialize)]
pub struct Job {
    pub id: String,
    pub db: PathBuf,
    pub cf: String,
    pub year: i32,
    pub product: String,
    pub period: String,
    pub source: String,
    pub source_index: u32,
    #[serde(flatten)]
    pub extra: BTreeMap<String, Value>,
}

fn verify_jobs(jobs: &[Job]) -> Result<(usize, usize, BTreeSet<i32>)> {
    use cme_tas_replay::product::{is_product_cf_name, product_cf_name};
    let mut databases: BTreeMap<PathBuf, BTreeSet<String>> = BTreeMap::new();
    let mut ids = BTreeSet::new();
    let mut indices = BTreeSet::new();
    let mut years = BTreeSet::new();
    for job in jobs {
        ensure!(ids.insert(&job.id), "duplicate source job ID: {}", job.id);
        ensure!(
            indices.insert(job.source_index),
            "duplicate source order index: {}",
            job.source_index
        );
        ensure!(
            job.cf == product_cf_name(u16::try_from(job.year)?, &job.product)?,
            "source CF/year/product mismatch: {}",
            job.id
        );
        let db =
            fs::canonicalize(&job.db).with_context(|| format!("source DB {}", job.db.display()))?;
        ensure!(
            databases.entry(db).or_default().insert(job.cf.clone()),
            "source CF included twice: {} {}",
            job.db.display(),
            job.cf
        );
        years.insert(job.year);
    }
    ensure!(!databases.is_empty(), "empty source plan");
    let mut count = 0;
    for (db, expected) in &databases {
        let current = fs::read(db.join("CURRENT"))?;
        let names = DB::list_cf(&Options::default(), db)
            .with_context(|| format!("listing source CFs in {}", db.display()))?;
        ensure!(
            !names
                .iter()
                .any(|n| n.starts_with("p:") && !is_product_cf_name(n)),
            "unrecognized product CF in {}",
            db.display()
        );
        let actual: BTreeSet<_> = names
            .into_iter()
            .filter(|n| is_product_cf_name(n))
            .collect();
        let omitted: Vec<_> = actual.difference(expected).collect();
        let absent: Vec<_> = expected.difference(&actual).collect();
        ensure!(omitted.is_empty() && absent.is_empty(), "source plan does not cover live product CFs in {}: omitted={omitted:?}; absent={absent:?}", db.display());
        ensure!(
            current == fs::read(db.join("CURRENT"))?,
            "source changed while listing CFs: {}",
            db.display()
        );
        count += actual.len();
    }
    Ok((databases.len(), count, years))
}

/// Compare the complete plan with live CF names, without scanning stored ticks
/// or writing a normal-success report to the delivery disk.
pub fn check_plan(path: &Path) -> Result<()> {
    let plan = io::read_json(path)?;
    let jobs: Vec<Job> = serde_json::from_value(plan["jobs"].clone())?;
    let (databases, count, years) = verify_jobs(&jobs)?;
    println!(
        "source plan covers {count} live product CFs in {databases} DBs; source years={years:?}"
    );
    Ok(())
}
#[derive(Clone)]
pub struct Config {
    pub project: PathBuf,
    pub plan: PathBuf,
    pub cache: PathBuf,
    pub work: PathBuf,
    pub output: PathBuf,
    pub year: Option<i32>,
    pub product: Option<String>,
    pub ric: Option<String>,
    pub workers: usize,
    pub publisher: Option<Arc<io::PublicationPool>>,
    /// Compare existing kline / session_status day files and replace
    /// differing ones (with a local backup); `None` keeps them untouched.
    pub replace: Option<Arc<io::Replace>>,
}
impl Config {
    pub fn run_dir(&self) -> PathBuf {
        self.work.join("checkpoints")
    }
    pub fn formal(&self) -> bool {
        self.output == Path::new("/mnt/nfs/30.100_cta_data/lseg")
    }
}
struct Source {
    job: Job,
    identity: Value,
    sparse: Option<PathBuf>,
    rics: Vec<String>,
    range: Option<(u64, u64)>,
    control_range: Option<(u64, u64)>,
    totals: Mutex<Option<(u64, u64)>>,
    index: PathBuf,
    sparse_ranges: Mutex<Option<HashMap<String, (usize, usize)>>>,
}
fn open(job: &Job) -> Result<DB> {
    tasdb::open_read_only(&job.db, ["default", &job.cf], Tuning::default())
}
fn upper_ric(kind: u8, ric: &str) -> Result<Vec<u8>> {
    let key = encode_all_key(kind, ric, 0, 0, 0)?;
    let mut out = key[..17].to_vec();
    for byte in out.iter_mut().rev() {
        if *byte != 255 {
            *byte += 1;
            return Ok(out);
        }
        *byte = 0
    }
    anyhow::bail!("RIC upper bound overflow")
}
fn key_ric(key: &[u8]) -> Result<&str> {
    ensure!(key.len() == 31, "invalid source key size");
    let slot = &key[1..17];
    let end = slot.iter().position(|b| *b == 0).unwrap_or(16);
    Ok(std::str::from_utf8(&slot[..end])?)
}
fn key_time(key: &[u8]) -> u64 {
    u64::from_be_bytes(key[17..25].try_into().unwrap())
}
fn order(key: &[u8]) -> u64 {
    (u16::from_be_bytes(key[25..27].try_into().unwrap()) as u64) << 32
        | u32::from_be_bytes(key[27..31].try_into().unwrap()) as u64
}
fn source(cfg: &Config, job: Job) -> Result<Source> {
    let mut source = minute_source(cfg, job)?;
    let index = cfg
        .work
        .join("control_ranges")
        .join(format!("{}.json", source.job.id));
    if index.exists() {
        let saved = io::read_json(&index)?;
        ensure!(
            saved["identity"] == source.identity,
            "control source identity changed: {}",
            source.job.id
        );
        source.control_range = serde_json::from_value(saved["range"].clone())?;
        return Ok(source);
    }
    // Only visit the first/last key of each RIC prefix. This includes sources
    // without trades and does not decode or collect their control messages.
    let db = open(&source.job)?;
    let cf = db.cf_handle(&source.job.cf).context("control CF")?;
    let mut lo = u64::MAX;
    let mut hi = 0;
    for kind in [12, 13] {
        let mut options = ReadOptions::default();
        options.fill_cache(false);
        options.set_iterate_upper_bound(vec![kind + 1]);
        let mut iter = db.raw_iterator_cf_opt(cf, options);
        iter.seek([kind]);
        while let Some(key) = iter.key() {
            ensure!(key[0] == kind, "control kind seek mismatch");
            let ric = key_ric(key)?.to_owned();
            lo = lo.min(key_time(key));
            let upper = upper_ric(kind, &ric)?;
            iter.seek_for_prev(&upper);
            let key = iter.key().context("last RIC control event")?;
            ensure!(
                key[0] == kind && key_ric(key)? == ric,
                "control RIC seek mismatch"
            );
            hi = hi.max(key_time(key));
            iter.seek(upper);
        }
        iter.status()?;
    }
    ensure!(
        source.identity["current"] == fs::read_to_string(source.job.db.join("CURRENT"))?,
        "source changed while indexing control years: {}",
        source.job.id
    );
    source.control_range = (lo != u64::MAX).then_some((lo, hi.saturating_add(1)));
    io::save(
        &index,
        &json!({"identity":source.identity,"range":source.control_range}),
    )?;
    Ok(source)
}

fn minute_source(cfg: &Config, job: Job) -> Result<Source> {
    let current = fs::read_to_string(job.db.join("CURRENT"))?;
    let identity = json!({"job":job,"current":current,"extractor_version":1});
    let index = cfg.work.join("sources").join(format!("{}.json", job.id));
    let old = cfg.cache.join(&job.id);
    let state = old.join("state.json");
    let parquet = old.join("minutes.parquet");
    if state.exists() && parquet.exists() {
        let s = io::read_json(&state)?;
        ensure!(
            s["identity"] == identity,
            "source identity changed: {}",
            job.id
        );
        let p = ParquetRecordBatchReaderBuilder::try_new(File::open(&parquet)?)?;
        ensure!(
            p.metadata().file_metadata().num_rows() as u64
                == s["minute_rows"].as_u64().context("cached minutes")?,
            "cached minute count mismatch"
        );
        let rics = s["rics"]
            .as_array()
            .context("cached RICs")?
            .iter()
            .map(|r| r.as_str().context("RIC").map(str::to_owned))
            .collect::<Result<Vec<_>>>()?;
        let column = p.schema().index_of("ts_utc_ns")?;
        let mut lo = u64::MAX;
        let mut hi = 0;
        for group in p.metadata().row_groups() {
            match group.column(column).statistics() {
                Some(parquet::file::statistics::Statistics::Int64(v)) => {
                    if let (Some(a), Some(b)) = (v.min_opt(), v.max_opt()) {
                        lo = lo.min(*a as u64);
                        hi = hi.max(*b as u64 + MINUTE)
                    }
                }
                _ => anyhow::bail!("cached sparse minutes lack timestamp statistics"),
            }
        }
        return Ok(Source {
            job,
            identity,
            sparse: Some(parquet),
            rics,
            range: (lo != u64::MAX).then_some((lo, hi)),
            control_range: None,
            totals: Mutex::new(Some((
                s["trade_rows"].as_u64().context("trade count")?,
                s["volume"].as_u64().context("volume")?,
            ))),
            index,
            sparse_ranges: Mutex::new(None),
        });
    }
    if index.exists() {
        let s = io::read_json(&index)?;
        if s["identity"] == identity {
            return Ok(Source {
                job,
                identity,
                sparse: None,
                rics: serde_json::from_value(s["rics"].clone())?,
                range: serde_json::from_value(s["range"].clone())?,
                control_range: None,
                totals: Mutex::new(serde_json::from_value(s["totals"].clone())?),
                index,
                sparse_ranges: Mutex::new(None),
            });
        }
    }
    let db = open(&job)?;
    let cf = db.cf_handle(&job.cf).context("source CF")?;
    let mut ro = ReadOptions::default();
    ro.fill_cache(false);
    ro.set_iterate_upper_bound(vec![2]);
    let mut it = db.raw_iterator_cf_opt(cf, ro);
    it.seek([1]);
    let mut rics = Vec::new();
    let mut lo = u64::MAX;
    let mut hi = 0;
    while let Some(key) = it.key() {
        if key[0] != 1 {
            break;
        }
        let ric = key_ric(key)?.to_owned();
        lo = lo.min(key_time(key));
        let upper = upper_ric(1, &ric)?;
        it.seek_for_prev(&upper);
        let last = it.key().context("last RIC trade")?;
        ensure!(key_ric(last)? == ric, "RIC seek bound mismatch");
        hi = hi.max(key_time(last));
        rics.push(ric);
        it.seek(&upper)
    }
    it.status()?;
    drop(it);
    drop(db);
    let range = if lo == u64::MAX {
        None
    } else {
        Some((
            lo.saturating_sub(DAY / 2),
            hi.saturating_add(DAY / 2 + MINUTE),
        ))
    };
    let s = Source {
        job,
        identity,
        sparse: None,
        rics,
        range,
        control_range: None,
        totals: Mutex::new(if lo == u64::MAX { Some((0, 0)) } else { None }),
        index,
        sparse_ranges: Mutex::new(None),
    };
    s.save()?;
    Ok(s)
}

fn target_groups(cfg: &Config, sources: &[Arc<Source>]) -> Result<Vec<(i32, String)>> {
    use chrono::Datelike;
    let mut groups = BTreeSet::new();
    for source in sources {
        groups.insert((source.job.year, source.job.product.clone()));
        for (lo, hi) in [source.range, source.control_range].into_iter().flatten() {
            let a = chrono::DateTime::from_timestamp(i64::try_from(lo / 1_000_000_000)?, 0)
                .context("source time")?
                .year();
            let b = chrono::DateTime::from_timestamp(
                i64::try_from(hi.saturating_sub(1) / 1_000_000_000)?,
                0,
            )
            .context("source time")?
            .year();
            ensure!(
                (0..=2).contains(&(b - a)),
                "source time range spans unexpected years: {}",
                source.job.id
            );
            for year in a..=b {
                groups.insert((year, source.job.product.clone()));
            }
        }
    }
    let mut groups: Vec<_> = groups
        .into_iter()
        .filter(|(year, product)| {
            cfg.year.is_none_or(|y| y == *year) && cfg.product.as_ref().is_none_or(|p| p == product)
        })
        .collect();
    groups.sort_by_key(|(year, product)| (-*year, product.clone()));
    Ok(groups)
}
impl Source {
    fn sparse_range(&self, path: &Path, ric: &str) -> Result<Option<(usize, usize)>> {
        let mut saved = self.sparse_ranges.lock().unwrap();
        if saved.is_none() {
            let reader = ParquetRecordBatchReaderBuilder::try_new(File::open(path)?)?;
            let column = reader.schema().index_of("ric")?;
            let projection = ProjectionMask::roots(reader.parquet_schema(), [column]);
            let mut ranges: HashMap<String, (usize, usize)> = HashMap::new();
            let mut offset = 0usize;
            let mut previous = String::new();
            for batch in reader
                .with_projection(projection)
                .with_batch_size(super::BATCH)
                .build()?
            {
                let batch = batch?;
                for row in 0..batch.num_rows() {
                    let value = io::string(&batch, row, "ric")?;
                    if value != previous {
                        ensure!(
                            !ranges.contains_key(value),
                            "cached RIC rows are not contiguous"
                        );
                        ranges.insert(value.into(), (offset, 0));
                        previous = value.into();
                    }
                    ranges.get_mut(value).unwrap().1 += 1;
                    offset += 1;
                }
            }
            *saved = Some(ranges);
        }
        Ok(saved.as_ref().unwrap().get(ric).copied())
    }
    fn save(&self) -> Result<()> {
        io::save(
            &self.index,
            &json!({"identity":self.identity,"rics":self.rics,"range":self.range,"totals":*self.totals.lock().unwrap()}),
        )
    }
}

#[derive(Clone, Copy, Debug, Default)]
struct Bar {
    open: i64,
    high: i64,
    low: i64,
    close: i64,
    volume: u64,
    count: u64,
    first: (u64, u64, u32, u64),
    last: (u64, u64, u32, u64),
}
impl Bar {
    fn add(&mut self, other: Self) -> Result<()> {
        if self.count == 0 {
            *self = other;
            return Ok(());
        }
        if other.first < self.first {
            self.first = other.first;
            self.open = other.open
        }
        if other.last > self.last {
            self.last = other.last;
            self.close = other.close
        }
        self.high = self.high.max(other.high);
        self.low = self.low.min(other.low);
        self.volume = self
            .volume
            .checked_add(other.volume)
            .context("volume overflow")?;
        self.count = self
            .count
            .checked_add(other.count)
            .context("trade count overflow")?;
        Ok(())
    }
}
struct Trades {
    bars: BTreeMap<u64, Bar>,
    prior: Option<Bar>,
    owners: Vec<(usize, u64, u64)>,
}
fn trades(sources: &[&Source], dbs: &[DB], ric: &str, year: i32) -> Result<Trades> {
    let start = io::year_start(year)?;
    let end = io::year_start(year + 1)?;
    let mut out = Trades {
        bars: BTreeMap::new(),
        prior: None,
        owners: Vec::new(),
    };
    for (index, source) in sources.iter().enumerate() {
        let source_index = source.job.source_index;
        if let Some(path) = &source.sparse {
            let Some((offset, length)) = source.sparse_range(path, ric)? else {
                continue;
            };
            let builder = ParquetRecordBatchReaderBuilder::try_new(File::open(path)?)?;
            let total = builder.metadata().file_metadata().num_rows() as usize;
            let selected = RowSelection::from(vec![
                RowSelector::skip(offset),
                RowSelector::select(length),
                RowSelector::skip(total - offset - length),
            ]);
            for batch in builder
                .with_row_selection(selected)
                .with_batch_size(super::BATCH)
                .build()?
            {
                let b = batch?;
                for row in 0..b.num_rows() {
                    ensure!(
                        io::string(&b, row, "ric")? == ric,
                        "cached RIC row selection mismatch"
                    );
                    let at = io::uint(&b, row, "ts_utc_ns")?;
                    let bar = Bar {
                        open: io::integer(&b, row, "open")?,
                        high: io::integer(&b, row, "high")?,
                        low: io::integer(&b, row, "low")?,
                        close: io::integer(&b, row, "close")?,
                        volume: io::uint(&b, row, "volume")?,
                        count: io::uint(&b, row, "trade_count")?,
                        first: (
                            io::uint(&b, row, "first_event_ns")?,
                            io::uint(&b, row, "first_published_ns")?,
                            source_index,
                            io::uint(&b, row, "first_order")?,
                        ),
                        last: (
                            io::uint(&b, row, "last_event_ns")?,
                            io::uint(&b, row, "last_published_ns")?,
                            source_index,
                            io::uint(&b, row, "last_order")?,
                        ),
                    };
                    ensure!(
                        bar.count > 0
                            && [bar.open, bar.high, bar.low, bar.close]
                                .into_iter()
                                .all(|p| p != i64::MIN),
                        "invalid cached trade minute"
                    );
                    if at >= start && at < end {
                        out.bars.entry(at).or_default().add(bar)?
                    } else if at < start && out.prior.is_none_or(|p| bar.last > p.last) {
                        out.prior = Some(bar)
                    }
                }
            }
        } else {
            let db = &dbs[index];
            let cf = db.cf_handle(&source.job.cf).unwrap();
            let mut ro = ReadOptions::default();
            ro.fill_cache(false);
            ro.set_iterate_upper_bound(upper_ric(1, ric)?);
            let mut it = db.raw_iterator_cf_opt(cf, ro);
            let owner = source.job.year == year;
            let lower = if owner {
                0
            } else {
                start.saturating_sub(DAY / 2)
            };
            it.seek(encode_all_key(1, ric, lower, 0, 0)?);
            let mut count = 0;
            let mut volume = 0;
            while let Some(key) = it.key() {
                if key[0] != 1 || key_ric(key)? != ric {
                    break;
                }
                let published = key_time(key);
                if !owner && published >= end + DAY / 2 {
                    break;
                }
                let r = decode_cme_trade(it.value().context("trade value")?)?;
                ensure!(
                    r.ts_utc_ns == published,
                    "trade key/publication clock mismatch"
                );
                ensure!(
                    r.price != i64::MIN && r.volume != u32::MAX,
                    "missing trade price or volume"
                );
                let event = session::event_time(r.ts_utc_ns, r.exch_hms_ns)?;
                let at = event / MINUTE * MINUTE;
                let clock = (event, published, source_index, order(key));
                let bar = Bar {
                    open: r.price,
                    high: r.price,
                    low: r.price,
                    close: r.price,
                    volume: u64::from(r.volume),
                    count: 1,
                    first: clock,
                    last: clock,
                };
                if at >= start && at < end {
                    out.bars.entry(at).or_default().add(bar)?
                } else if at < start && out.prior.is_none_or(|p| bar.last > p.last) {
                    out.prior = Some(bar)
                }
                count += 1;
                volume += u64::from(r.volume);
                it.next()
            }
            it.status()?;
            if owner {
                out.owners.push((index, count, volume))
            }
            // Indexed reverse seed search; do not scan the full prior tick year.
            let mut ro = ReadOptions::default();
            ro.fill_cache(false);
            let mut back = db.raw_iterator_cf_opt(cf, ro);
            back.seek_for_prev(encode_all_key(1, ric, start + DAY / 2, 0, 0)?);
            while let Some(key) = back.key() {
                if key[0] != 1 || key_ric(key)? != ric {
                    break;
                }
                let published = key_time(key);
                if out
                    .prior
                    .is_some_and(|p| published.saturating_add(DAY / 2) < p.last.0)
                {
                    break;
                }
                let r = decode_cme_trade(back.value().unwrap())?;
                let event = session::event_time(r.ts_utc_ns, r.exch_hms_ns)?;
                let clock = (event, published, source_index, order(key));
                if event < start && out.prior.is_none_or(|p| clock > p.last) {
                    out.prior = Some(Bar {
                        close: r.price,
                        count: 1,
                        last: clock,
                        ..Bar::default()
                    })
                }
                back.prev()
            }
            back.status()?;
        }
    }
    ensure!(
        out.bars.len() <= 530_000,
        "more than a UTC year of minute buckets"
    );
    Ok(out)
}

struct Cursor<'a> {
    iter: DBRawIteratorWithThreadMode<'a, DB>,
    index: u32,
    ric: String,
    end: u64,
    kind: u8,
}
struct Events<'a> {
    cursors: Vec<Cursor<'a>>,
}
struct Event<'a> {
    at: u64,
    qualifiers: &'a str,
}
fn ascii_slot(bytes: &[u8]) -> Result<&str> {
    let end = bytes.iter().position(|b| *b == 0).unwrap_or(bytes.len());
    ensure!(
        bytes[end..].iter().all(|b| *b == 0),
        "invalid string padding"
    );
    ensure!(bytes[..end].is_ascii(), "non-ASCII source field");
    Ok(std::str::from_utf8(&bytes[..end])?)
}
fn status<'a>(key: &'a [u8], value: &'a [u8]) -> Result<Event<'a>> {
    let end = match value.len() {
        CME_STATUS_LEN => 36 + STATUS_QUALIFIER_LEN,
        CME_STATUS_LEGACY_LEN => 36 + STATUS_QUALIFIER_LEGACY_LEN,
        _ => anyhow::bail!("unsupported status codec length"),
    };
    ensure!(
        &value[..4] == b"CT\x01\x0d" && value[end..].iter().all(|b| *b == 0),
        "invalid status codec header/padding"
    );
    let ric = ascii_slot(&value[4..20])?;
    let q = ascii_slot(&value[36..end])?;
    ensure!(
        !q.is_empty() && key_ric(key)? == ric,
        "invalid status RIC/qualifiers"
    );
    let at = u64::from_le_bytes(value[20..28].try_into()?);
    ensure!(at == key_time(key), "status key/publication clock mismatch");
    Ok(Event { at, qualifiers: q })
}
impl<'a> Events<'a> {
    fn new(
        sources: &[&Source],
        dbs: &'a [DB],
        ric: &str,
        lower: u64,
        end: u64,
        kind: u8,
    ) -> Result<Self> {
        let mut cursors = Vec::new();
        for (source, db) in sources.iter().zip(dbs) {
            let cf = db.cf_handle(&source.job.cf).unwrap();
            let mut ro = ReadOptions::default();
            ro.fill_cache(false);
            ro.set_iterate_upper_bound(upper_ric(kind, ric)?);
            let mut it = db.raw_iterator_cf_opt(cf, ro);
            it.seek(encode_all_key(kind, ric, lower, 0, 0)?);
            cursors.push(Cursor {
                iter: it,
                index: source.job.source_index,
                ric: ric.into(),
                end,
                kind,
            });
        }
        Ok(Self { cursors })
    }
    fn next_index(&self) -> Result<Option<usize>> {
        let mut selected = None;
        let mut best = (u64::MAX, u32::MAX, u64::MAX);
        for (i, c) in self.cursors.iter().enumerate() {
            if let Some(key) = c.iter.key() {
                if key[0] != c.kind || key_ric(key)? != c.ric || key_time(key) >= c.end {
                    continue;
                }
                let clock = (key_time(key), c.index, order(key));
                if clock < best {
                    best = clock;
                    selected = Some(i)
                }
            } else {
                c.iter.status()?
            }
        }
        Ok(selected)
    }
    fn peek(&self, i: usize) -> Result<Event<'_>> {
        let c = &self.cursors[i];
        status(c.iter.key().unwrap(), c.iter.value().unwrap())
    }
    fn advance(&mut self, i: usize) {
        self.cursors[i].iter.next()
    }
}

#[derive(Clone)]
struct Reference {
    first: Option<i64>,
    expiry: Option<u64>,
}
#[derive(Default)]
pub struct Metadata {
    venues: HashMap<(i32, String), BTreeSet<String>>,
    reported: HashMap<(i32, String), u64>,
    references: HashMap<String, Vec<Reference>>,
    daily: HashMap<String, Vec<(u64, u64)>>,
    /// Per-job daily coverage from the plan (`coverage_days`), used by
    /// lseg-daily batches instead of the pinned 20261109 manifests.
    job_daily: HashMap<String, Vec<(u64, u64)>>,
    protected: HashMap<String, Value>,
}
impl Metadata {
    pub fn load(cfg: &Config) -> Result<Self> {
        let mut m = Self::default();
        let index = cfg
            .project
            .join("artifacts/lseg_exchange_groups_20261109/source_ric_details.parquet");
        for batch in io::batches(&index)? {
            let b = batch?;
            for i in 0..b.num_rows() {
                if io::string(&b, i, "asset_class")? != "futures"
                    || io::string(&b, i, "source_layer")? != "tas"
                {
                    continue;
                }
                let key = (
                    i32::try_from(io::integer(&b, i, "year")?)?,
                    io::string(&b, i, "ric")?.to_owned(),
                );
                m.venues
                    .entry(key.clone())
                    .or_default()
                    .insert(io::string(&b, i, "exchange")?.to_lowercase());
                if let Some(t) = io::string(&b, i, "source_end").ok().and_then(io::parse_ns) {
                    m.reported
                        .entry(key)
                        .and_modify(|v| *v = (*v).max(t))
                        .or_insert(t);
                }
            }
        }
        let approved = io::read_json(&cfg.project.join("artifacts/tas_status_20261109/plan.json"))?;
        for (ric, v) in approved["exchanges"]
            .as_object()
            .context("approved exchanges")?
        {
            m.venues.insert(
                (2026, ric.clone()),
                BTreeSet::from([v.as_str().context("venue")?.into()]),
            );
        }
        m.load_plan_overlays(cfg, &approved["exchanges"])?;
        let paths = [
            PathBuf::from(
                "/mnt/nvme-raid0-28t/fanghaizhou/lseg_data/cme_causal_roll_inputs_20261023/hrd.csv",
            ),
            cfg.project
                .join("artifacts/ib_tas_all_futures_20261103/supplement_hrd.csv"),
        ];
        for path in paths {
            let mut csv = csv::Reader::from_path(path)?;
            let headers = csv.headers()?.clone();
            let pos = |name: &str| headers.iter().position(|v| v == name).context("HRD column");
            let ric = pos("RIC")?;
            let first = pos("First Trading Date")?;
            let last = pos("Last Trading Day")?;
            let expiry = pos("Expiration Date")?;
            let parse = |s: &str| {
                chrono::NaiveDate::parse_from_str(s, "%m/%d/%Y")
                    .ok()?
                    .and_hms_opt(0, 0, 0)?
                    .and_utc()
                    .timestamp_nanos_opt()
            };
            for row in csv.records() {
                let row = row?;
                let ending = [parse(&row[last]), parse(&row[expiry])]
                    .into_iter()
                    .flatten()
                    .filter_map(|n| u64::try_from(n).ok())
                    .map(|n| n + DAY)
                    .min();
                m.references
                    .entry(row[ric].into())
                    .or_default()
                    .push(Reference {
                        first: parse(&row[first]),
                        expiry: ending,
                    });
            }
        }
        for entry in fs::read_dir(
            cfg.project
                .join("artifacts/lseg_only_klines_20261111/contracts"),
        )? {
            let p = entry?.path();
            if p.extension().is_some_and(|v| v == "json") {
                let v = io::read_json(&p)?;
                if v["kline_rows"].as_u64().unwrap_or(0) > 0 {
                    m.protected
                        .insert(v["ric"].as_str().context("protected RIC")?.into(), v);
                }
            }
        }
        for entry in fs::read_dir(
            cfg.project
                .join("artifacts/tas_daily_ingest_20261109/products"),
        )? {
            let entry = entry?;
            let receipt = entry.path().join("staging_receipt.json");
            if !receipt.exists() {
                continue;
            }
            let r = io::read_json(&receipt)?;
            let Some(path) = r["manifest"].as_str() else {
                continue;
            };
            let manifest = io::read_json(Path::new(path))?;
            let product = entry.file_name().to_string_lossy().into_owned();
            for row in manifest["sources"].as_array().context("manifest sources")? {
                if row["complete"] != true || row["compressed_bytes"].as_u64().unwrap_or(0) == 0 {
                    continue;
                }
                let name = Path::new(row["file"].as_str().context("source file")?)
                    .file_name()
                    .unwrap()
                    .to_string_lossy();
                let day = name
                    .get(product.len() + 1..product.len() + 11)
                    .and_then(io::parse_ns)
                    .context("daily source date")?;
                m.daily
                    .entry(product.clone())
                    .or_default()
                    .push((day, day + DAY));
            }
        }
        Ok(m)
    }
    /// Plan jobs written by lseg-daily carry their own coverage days and the
    /// exchange map used for their trade/quote/status files. RICs already in
    /// the approved 2026 map keep that venue; the map only adds new RICs.
    fn load_plan_overlays(&mut self, cfg: &Config, approved: &Value) -> Result<()> {
        let plan = io::read_json(&cfg.plan)?;
        let mut maps = BTreeSet::new();
        for job in plan["jobs"].as_array().context("plan jobs")? {
            let id = job["id"].as_str().context("job id")?;
            if let Some(days) = job.get("coverage_days").and_then(Value::as_array) {
                let mut ranges = Vec::with_capacity(days.len());
                for day in days {
                    let at = day
                        .as_str()
                        .and_then(io::parse_ns)
                        .with_context(|| format!("coverage day in {id}"))?;
                    ranges.push((at, at + DAY));
                }
                self.job_daily.insert(id.into(), ranges);
            }
            if let Some(path) = job.get("exchange_map").and_then(Value::as_str) {
                let year = i32::try_from(job["year"].as_i64().context("job year")?)?;
                maps.insert((year, PathBuf::from(path)));
            }
        }
        for (year, path) in maps {
            let map = io::read_json(&path)?;
            for (ric, v) in map.as_object().context("exchange map object")? {
                let venue = v.as_str().context("exchange map venue")?.to_lowercase();
                if year == 2026 && approved.get(ric).is_some() {
                    continue;
                }
                let entry = self.venues.entry((year, ric.clone())).or_default();
                entry.clear();
                entry.insert(venue);
            }
        }
        Ok(())
    }
    pub(crate) fn venue(&self, year: i32, ric: &str) -> String {
        let candidates = self.venues.get(&(year, ric.into()));
        match candidates {
            Some(v) if v.len() == 1 => v.first().unwrap().clone(),
            _ => "review".into(),
        }
    }
}
fn coverage(
    sources: &[&Source],
    metadata: &Metadata,
    year: i32,
    product: &str,
    first: u64,
    last: u64,
) -> Result<(u64, u64, Vec<(u64, u64)>)> {
    let lo = io::year_start(year)?;
    let hi = io::year_start(year + 1)?;
    let mut ranges = Vec::new();
    for s in sources {
        if s.job.year != year {
            continue;
        }
        if let Some(days) = metadata.job_daily.get(&s.job.id) {
            ranges.extend(days.iter().copied())
        } else if s.job.source == "daily" {
            ranges.extend(metadata.daily.get(product).into_iter().flatten().copied())
        } else {
            let (a, b) = s.job.period.split_once('_').context("source period")?;
            ranges.push((
                io::parse_ns(a).context("period start")?,
                io::parse_ns(b).context("period end")?,
            ));
        }
    }
    if ranges.is_empty() {
        return Ok((
            lo.max(first / MINUTE * MINUTE),
            hi.min((last / MINUTE + 1) * MINUTE),
            vec![],
        ));
    }
    ranges.sort();
    let start = ranges[0].0;
    let mut end = start;
    let mut gaps = Vec::new();
    for (a, b) in ranges {
        if a > end {
            gaps.push((end, a))
        }
        end = end.max(b)
    }
    Ok((lo.max(start), hi.min(end), gaps))
}
fn python_json(value: &Value) -> String {
    match value {
        Value::Array(v) => format!(
            "[{}]",
            v.iter().map(python_json).collect::<Vec<_>>().join(", ")
        ),
        Value::Object(v) => format!(
            "{{{}}}",
            {
                let mut keys = v.keys().collect::<Vec<_>>();
                keys.sort();
                keys
            }
            .into_iter()
            .map(|k| format!(
                "{}: {}",
                serde_json::to_string(k).unwrap(),
                python_json(&v[k])
            ))
            .collect::<Vec<_>>()
            .join(", ")
        ),
        _ => value.to_string(),
    }
}
fn signature(sources: &[&Source]) -> String {
    format!(
        "{:x}",
        Sha256::digest(
            python_json(&Value::Array(
                sources.iter().map(|s| s.identity.clone()).collect()
            ))
            .as_bytes()
        )
    )
}

fn group(
    cfg: &Config,
    all: &[Arc<Source>],
    meta: &Metadata,
    year: i32,
    product: &str,
) -> Result<Stats> {
    let start_year = io::year_start(year)?;
    let end_year = io::year_start(year + 1)?;
    let sources: Vec<_> = all
        .iter()
        .filter(|s| {
            s.job.product == product
                && ((s.job.year - year).abs() <= 1
                    || s.range.is_some_and(|(a, b)| a < end_year && b > start_year)
                    || s.control_range
                        .is_some_and(|(a, b)| a < end_year && b > start_year))
        })
        .map(Arc::as_ref)
        .collect();
    let signature = signature(&sources);
    let key = component(&format!("{year}:{product}"));
    let checkpoint = cfg.run_dir().join("groups").join(format!("{key}.json"));
    if cfg.ric.is_none() && checkpoint.exists() {
        let p = io::read_json(&checkpoint)?;
        ensure!(
            p["signature"] == signature && p["output"] == cfg.output.to_string_lossy().as_ref(),
            "native checkpoint identity changed"
        );
        return Ok(serde_json::from_value(p["stats"].clone())?);
    }
    if cfg.formal() && cfg.ric.is_none() {
        let prior = cfg
            .project
            .join("artifacts/lseg_full_rocksdb_20261112/groups")
            .join(format!("{key}.json"));
        // A legacy group whose sources have since grown (a new daily batch)
        // is recomputed; existing files are never overwritten by the sinks.
        let legacy = if prior.exists() {
            Some(io::read_json(&prior)?)
        } else {
            None
        };
        if let Some(p) = legacy.filter(|p| p["input_signature"] == signature) {
            let stats: Stats = serde_json::from_value(p)?;
            io::save(
                &checkpoint,
                &json!({"signature":signature,"output":cfg.output,"stats":stats,"reused_legacy_group":true}),
            )?;
            return Ok(stats);
        }
    }
    let dbs = sources
        .iter()
        .map(|s| open(&s.job))
        .collect::<Result<Vec<_>>>()?;
    for s in &sources {
        ensure!(
            s.identity["current"] == fs::read_to_string(s.job.db.join("CURRENT"))?,
            "source CURRENT changed before processing: {}",
            s.job.id
        )
    }
    let rics: BTreeSet<_> = sources
        .iter()
        .flat_map(|s| s.rics.iter())
        .filter(|r| cfg.ric.as_ref().is_none_or(|v| v == *r))
        .cloned()
        .collect();
    let mut total = Stats::default();
    let mut owners: HashMap<usize, (u64, u64)> = HashMap::new();
    for ric in rics {
        let data = trades(&sources, &dbs, &ric, year)?;
        for (i, n, v) in &data.owners {
            let t = owners.entry(*i).or_default();
            t.0 += n;
            t.1 += v
        }
        if data.bars.is_empty() {
            continue;
        }
        let n = data.bars.values().map(|b| b.count).sum::<u64>();
        let volume = data.bars.values().map(|b| b.volume).sum::<u64>();
        let rc = cfg
            .run_dir()
            .join("contracts")
            .join(&key)
            .join(format!("{}.json", component(&ric)));
        if rc.exists() {
            let p = io::read_json(&rc)?;
            ensure!(
                p["signature"] == signature && p["output"] == cfg.output.to_string_lossy().as_ref(),
                "RIC source identity changed"
            );
            let s: Stats = serde_json::from_value(p["stats"].clone())?;
            ensure!(
                s.trades == n && s.volume == volume,
                "RIC cached raw quantity changed"
            );
            total.add(&s);
            continue;
        }
        // Corrected RICs (correction_policy_version=1) keep their revised
        // files: unchanged sources are reused as a whole; with new trades only
        // days from the protected end on are generated, earlier existing
        // files are never compared or replaced.
        let mut frozen_before = 0u64;
        let mut protected_late = None;
        if (cfg.formal() || cfg.replace.is_some()) && year == 2026 {
            if let Some(p) = meta.protected.get(&ric).filter(|p| {
                p["source_trade_rows"].as_u64() != Some(n)
                    || p["source_volume"].as_u64() != Some(volume)
            }) {
                let end = p["end_ns"].as_u64().context("protected end_ns")?;
                // The revision was built from the base-plan sources only; they
                // must be unchanged. Newer (daily-batch) sources may add trades.
                // The revision was built from the base-plan sources only. Trades
                // on UTC days delivered later by lseg-daily batches (their
                // `coverage_days`; no other source covers those days) are new;
                // everything else must be exactly what was revised.
                let new_days: HashSet<u64> = sources
                    .iter()
                    .filter_map(|s| meta.job_daily.get(&s.job.id))
                    .flatten()
                    .map(|(a, _)| a / DAY)
                    .collect();
                let (mut pn, mut pv, mut ln, mut lv) = (0u64, 0u64, 0u64, 0u64);
                for (at, x) in data.bars.range(..end) {
                    if new_days.contains(&(at / DAY)) {
                        ln += x.count;
                        lv += x.volume;
                    } else {
                        pn += x.count;
                        pv += x.volume;
                    }
                }
                let after = data
                    .bars
                    .range(end..)
                    .filter(|(at, _)| !new_days.contains(&(*at / DAY)))
                    .count();
                ensure!(
                    p["source_trade_rows"].as_u64() == Some(pn)
                        && p["source_volume"].as_u64() == Some(pv)
                        && after == 0,
                    "protected revised RIC: its original sources changed: {ric} (now {pn}/{pv}, revised from {}/{})",
                    p["source_trade_rows"],
                    p["source_volume"]
                );
                if ln > 0 {
                    protected_late = Some((end, ln, lv));
                }
                frozen_before = end;
            } else if let Some(p) = meta.protected.get(&ric) {
                let s = Stats {
                    rics: 1,
                    reused_rics: 1,
                    trades: n,
                    volume,
                    kline_rows: p["kline_rows"].as_u64().unwrap_or(0),
                    ..Stats::default()
                };
                io::save(
                    &rc,
                    &json!({"signature":signature,"output":cfg.output,"stats":s,"protected":true}),
                )?;
                total.add(&s);
                continue;
            }
        }
        let venue = meta.venue(year, &ric);
        ensure!(
            !venue.is_empty()
                && venue
                    .bytes()
                    .all(|b| b.is_ascii_lowercase() || b.is_ascii_digit() || b == b'_'),
            "invalid exchange directory"
        );
        let cme = ["cme", "cbot", "comex", "nymex"].contains(&venue.as_str());
        let root = cfg
            .output
            .join(&venue)
            .join(year.to_string())
            .join(component(&ric));
        let mut problems = Problems::new(
            cfg.output
                .join("meta/problems/native_kline")
                .join(&key)
                .join(format!("{}.csv", component(&ric))),
        );
        if let Some((end, count, volume)) = protected_late {
            problems.record(
                &ric,
                "protected_late_trades_not_applied",
                0,
                end,
                count,
                &format!(
                    "{count} late trades / volume {volume} before the protected end; revised files kept (correction_policy_version=1 netting is not in the native engine)"
                ),
            )?
        }
        if venue == "review" {
            problems.record(
                &ric,
                "venue_unresolved",
                0,
                0,
                1,
                "original RIC retained; no prefix-based inference",
            )?
        }
        if !cme {
            problems.record(
                &ric,
                "status_protocol_not_calibrated",
                0,
                0,
                1,
                "only explicit H/N interpreted for this venue",
            )?
        }
        let first = data.bars.values().map(|b| b.first.0).min().unwrap();
        let last = data.bars.values().map(|b| b.last.0).max().unwrap();
        let (start, end, gaps) = coverage(&sources, meta, year, product, first, last)?;
        let gap_ranges = gaps.clone();
        let mut withheld = 0u64;
        let refs: Vec<_> = meta
            .references
            .get(&ric)
            .into_iter()
            .flatten()
            .filter(|r| {
                r.first
                    .is_some_and(|t| i128::from(t) <= i128::from(first) + i128::from(DAY))
                    && r.expiry.is_some_and(|t| t >= first.saturating_sub(DAY))
            })
            .collect();
        let expiries: BTreeSet<_> = refs.iter().filter_map(|r| r.expiry).collect();
        let trusted = expiries.len() == 1
            && refs.iter().any(|r| {
                r.first.is_some_and(|t| i128::from(t) < i128::from(start))
                    && r.expiry.is_some_and(|t| start < t)
            });
        let mature = refs
            .iter()
            .any(|r| r.first.is_some_and(|t| i128::from(t) < i128::from(start)));
        if expiries.len() > 1 {
            problems.record(
                &ric,
                "conflicting_lseg_lifetimes",
                0,
                0,
                expiries.len() as u64,
                &format!("{expiries:?}"),
            )?
        }
        let mut expiry = if expiries.len() == 1 {
            expiries.first().copied()
        } else {
            None
        };
        let mut first_phase = None;
        let mut prior_phase = false;
        let mut last_status = last;
        let mut decoder = Decoder::new(cme);
        let mut public_previous = State::Unknown;
        let mut timeline = Timeline::new(
            start.min(first / MINUTE * MINUTE),
            end.max((last / MINUTE + 1) * MINUTE),
            gaps,
        );
        let mut writer =
            io::StatusWriter::new_with_pool(root.clone(), product, &ric, cfg.publisher.clone())?;
        writer.set_policy(cfg.replace.clone(), frozen_before);
        let mut event = Events::new(
            &sources,
            &dbs,
            &ric,
            if trusted { 0 } else { start },
            end,
            13,
        )?;
        let mut issues: BTreeMap<(String, String), (u64, u64, u64)> = BTreeMap::new();
        let mut status_rows = 0u64;
        while let Some(i) = event.next_index()? {
            let e = event.peek(i)?;
            let baseline = decoder.decode(e.qualifiers, State::Unknown);
            if matches!(
                baseline.action,
                "OPEN" | "PRE_OPEN" | "CLOSE" | "HALT" | "NOT_AVAILABLE" | "EXPIRE"
            ) {
                if e.at < start {
                    prior_phase = true
                } else {
                    first_phase = Some(first_phase.map_or(e.at, |t: u64| t.min(e.at)))
                }
            }
            if baseline.action == "EXPIRE" {
                expiry = Some(expiry.map_or(e.at, |t| t.min(e.at)))
            }
            if e.at >= start {
                last_status = last_status.max(e.at)
            }
            timeline.before(e.at)?;
            let r = decoder.decode(e.qualifiers, timeline.previous);
            timeline.apply(&r);
            let r = decoder.decode(e.qualifiers, public_previous);
            let before = public_previous;
            public_previous = r.state;
            if e.at >= start_year && e.at < end_year {
                if let Some(issue) = r.issue.as_deref().filter(|s| *s != "uncalibrated_product") {
                    ensure!(
                        issues.len() < 100_000
                            || issues.contains_key(&(issue.to_owned(), e.qualifiers.to_owned())),
                        "too many distinct qualifier issues"
                    );
                    let t = issues
                        .entry((issue.into(), e.qualifiers.into()))
                        .or_insert((e.at, e.at, 0));
                    t.1 = e.at;
                    t.2 += 1;
                }
                writer.push(StatusRow {
                    at: e.at,
                    before,
                    decoded: r,
                    qualifiers: e.qualifiers.into(),
                })?;
                status_rows += 1;
            }
            event.advance(i);
            if status_rows > 0 && status_rows.is_multiple_of(100_000_000) {
                eprintln!("{year}/{product}/{ric}: {status_rows} status messages streamed");
            }
        }
        let intervals = timeline.finish()?;
        let (status_files, mut directories) = writer.finish_with_directories()?;
        for ((issue, q), (a, b, count)) in issues {
            problems.record(&ric, &issue, a, b, count, &q)?
        }
        for (index, source) in sources
            .iter()
            .enumerate()
            .filter(|(_, s)| s.job.year == year)
        {
            let db = &dbs[index];
            let cf = db.cf_handle(&source.job.cf).unwrap();
            let mut ro = ReadOptions::default();
            ro.fill_cache(false);
            ro.set_iterate_upper_bound(upper_ric(12, &ric)?);
            let mut it = db.raw_iterator_cf_opt(cf, ro);
            it.seek(encode_all_key(12, &ric, 0, 0, 0)?);
            while let Some(key) = it.key() {
                if key[0] != 12 || key_ric(key)? != ric {
                    break;
                }
                let row = decode_cme_correction(it.value().unwrap())?;
                problems.record(
                    &ric,
                    "source_witness_pending",
                    row.ts_utc_ns,
                    row.ts_utc_ns,
                    1,
                    &format!(
                        "{} {} order={}; {}",
                        source.job.db.display(),
                        source.job.cf,
                        order(key),
                        row.qualifiers
                    ),
                )?;
                it.next()
            }
            it.status()?;
        }
        let lo = if mature || prior_phase {
            start
        } else {
            start.max(first.min(first_phase.unwrap_or(first)) / MINUTE * MINUTE)
        }
        .min(first / MINUTE * MINUTE);
        let hi = if let Some(expiry) = expiry {
            end.min(expiry.div_ceil(MINUTE) * MINUTE)
        } else {
            if let Some(t) = meta.reported.get(&(year, ric.clone())) {
                last_status = last_status.max(*t)
            }
            problems.record(
                &ric,
                "no_unique_lseg_expiry",
                0,
                0,
                1,
                "padding through the last UTC day with LSEG evidence, within source coverage",
            )?;
            end.min((last_status / DAY + 1) * DAY)
        }
        .max((last / MINUTE + 1) * MINUTE);
        if expiry.is_some_and(|t| last >= t) {
            problems.record(
                &ric,
                "trade_after_lseg_expiry",
                expiry.unwrap(),
                last,
                1,
                "real prints retained",
            )?
        }
        for interval in &intervals {
            if interval.phase == session::Phase::Unknown && interval.end > lo && interval.start < hi
            {
                problems.record(
                    &ric,
                    "unresolved_session_state_or_cause",
                    interval.start.max(lo),
                    interval.end.min(hi),
                    1,
                    "",
                )?
            }
        }
        let mut previous = if trusted {
            data.prior.map(|p| p.close)
        } else {
            None
        };
        let days = if hi > lo {
            ((hi - 1) / DAY - lo / DAY + 1) as usize
        } else {
            0
        };
        let selected = io::sample_indices(days);
        let mut files = 0;
        let mut rows = 0;
        let mut written_count = 0;
        let mut written_volume = 0;
        let mut at = lo;
        let mut pending = io::PendingFiles::new(cfg.publisher.clone());
        while at < hi {
            let day = at / DAY;
            let day_end = ((day + 1) * DAY).min(hi);
            let mut out = Vec::with_capacity(1440);
            while at < day_end {
                let actual = data.bars.get(&at);
                if let Some(b) = actual {
                    previous = Some(b.close);
                    written_count += b.count;
                    written_volume += b.volume;
                }
                out.push(Kline {
                    at,
                    prices: if let Some(b) = actual {
                        [Some(b.open), Some(b.high), Some(b.low), Some(b.close)]
                    } else {
                        [previous; 4]
                    },
                    volume: actual.map_or(0, |b| b.volume),
                    count: actual.map_or(0, |b| b.count),
                    ffill: actual.is_none() && previous.is_some(),
                    status: session::label(&intervals, at),
                });
                at += MINUTE;
            }
            // Never publish a UTC day the delivered sources do not fully cover.
            // Real source periods and daily files are UTC-day aligned, so a day
            // is either fully covered or fully outside / inside a gap; the
            // trailing edge is handled by the day-aligned grid end above.
            let (d0, d1) = (day * DAY, (day + 1) * DAY);
            let uncovered =
                d1 <= start || d0 >= end || gap_ranges.iter().any(|&(a, b)| a <= d0 && d1 <= b);
            if uncovered {
                let trades: u64 = out.iter().map(|k| k.count).sum();
                if trades == 0 {
                    withheld += 1;
                    continue;
                }
                // Real trades are evidence: publish the day (late data may
                // correct it later) and record that the source manifest does
                // not cover it, e.g. a zero-byte daily placeholder whose
                // trades arrived through another source, or prints stamped
                // just past the last delivered day.
                problems.record(
                    &ric,
                    "trades_on_uncovered_day",
                    d0,
                    d1,
                    trades,
                    "UTC day not covered by the delivered source manifest but has trades; published",
                )?;
            }
            let batch = io::kline_batch(&ric, &out)?;
            let path = root.join(io::date(day * DAY)?).join("kline.parquet");
            io::write_kline(
                path,
                batch,
                &mut directories,
                &mut pending,
                selected.contains_key(&(files as usize)),
                cfg.replace.clone(),
                day * DAY < frozen_before,
            )?;
            files += 1;
            rows += out.len() as u64;
        }
        pending.finish()?;
        if withheld > 0 {
            eprintln!("{year}/{product}/{ric}: {withheld} UTC days not fully covered by delivered sources withheld");
        }
        ensure!(
            (written_count, written_volume) == (n, volume),
            "minute quantity conservation failed"
        );
        for (i, _) in io::sample_indices(status_files.len()) {
            let file = &status_files[i];
            let mut reader = io::Rows::new(&file.path)?;
            let mut raw = Events::new(
                &sources,
                &dbs,
                &ric,
                if trusted {
                    file.day * DAY
                } else {
                    start.max(file.day * DAY)
                },
                end.min((file.day + 1) * DAY),
                13,
            )?;
            let mut previous = file.before;
            let mut count = 0;
            while let Some(i) = raw.next_index()? {
                let e = raw.peek(i)?;
                let decoded = decoder.decode(e.qualifiers, previous);
                let before = previous;
                previous = decoded.state;
                let expected = StatusRow {
                    at: e.at,
                    before,
                    decoded,
                    qualifiers: e.qualifiers.into(),
                };
                let (b, row) = reader.next_row()?.context("sample missing status row")?;
                io::compare_status(b, row, product, &ric, &expected)?;
                count += 1;
                raw.advance(i)
            }
            ensure!(
                reader.next_row()?.is_none() && count == file.rows,
                "sample status row count mismatch"
            );
        }
        problems.finish()?;
        let s = Stats {
            rics: 1,
            trades: n,
            volume,
            kline_rows: rows,
            files,
            status_files: status_files.len() as u64,
            status_rows,
            ..Stats::default()
        };
        io::save(
            &rc,
            &json!({"signature":signature,"output":cfg.output,"stats":s}),
        )?;
        total.add(&s);
        eprintln!(
            "{year}/{product}/{ric}: {} minutes, {} status messages",
            s.kline_rows, s.status_rows
        );
    }
    for s in &sources {
        ensure!(
            s.identity["current"] == fs::read_to_string(s.job.db.join("CURRENT"))?,
            "source CURRENT changed during processing: {}",
            s.job.id
        );
    }
    if cfg.ric.is_none() {
        for (index, (n, v)) in owners {
            *sources[index].totals.lock().unwrap() = Some((n, v));
            sources[index].save()?;
        }
        io::save(
            &checkpoint,
            &json!({"signature":signature,"output":cfg.output,"stats":total}),
        )?;
    }
    Ok(total)
}

pub fn run(mut cfg: Config) -> Result<()> {
    use rayon::prelude::*;
    ensure!(
        (1..=2).contains(&cfg.workers),
        "workers must be 1..2; implicit pools fixed, total target <=32 threads"
    );
    fs::create_dir_all(cfg.run_dir())?;
    let lock = File::create(cfg.run_dir().join("run.lock"))?;
    use std::os::fd::AsRawFd;
    ensure!(
        unsafe { libc::flock(lock.as_raw_fd(), libc::LOCK_EX | libc::LOCK_NB) } == 0,
        "native task already running"
    );
    let plan = io::read_json(&cfg.plan)?;
    let mut jobs: Vec<Job> = serde_json::from_value(plan["jobs"].clone())?;
    if cfg.year.is_none() && cfg.product.is_none() && cfg.ric.is_none() {
        verify_jobs(&jobs)?;
    }
    jobs.retain(|j| cfg.product.as_ref().is_none_or(|p| p == &j.product));
    let pool = rayon::ThreadPoolBuilder::new()
        .num_threads(cfg.workers)
        .build()?;
    io::save(
        &cfg.run_dir().join("state.json"),
        &json!({"stage":"planning","source_jobs":jobs.len(),"workers":cfg.workers}),
    )?;
    let sources: Vec<Arc<Source>> = pool.install(|| {
        jobs.into_par_iter()
            .map(|job| {
                let id = job.id.clone();
                source(&cfg, job)
                    .with_context(|| format!("building source catalog for {id}"))
                    .map(Arc::new)
            })
            .collect::<Result<_>>()
    })?;
    let meta = Metadata::load(&cfg)?;
    let groups = target_groups(&cfg, &sources)?;
    cfg.publisher = Some(io::PublicationPool::new(crate::limits::IO_THREADS)?);
    let start = Instant::now();
    let count = groups.len();
    let progress = Mutex::new((0usize, Stats::default(), Vec::<Value>::new()));
    eprintln!(
        "native direct run: {} source jobs, {count} groups; workers={}; no Python or event JSON",
        sources.len(),
        cfg.workers
    );
    pool.install(|| {
        groups.par_iter().for_each(|(year,product)| {
            let result=group(&cfg,&sources,&meta,*year,product).and_then(|mut stats| {
                if cfg.ric.is_none() {
                    stats.add(&missing_status::group_status(&cfg, &sources, &meta, *year, product)?);
                }
                Ok(stats)
            });
            let failure=cfg.output.join("meta/problems/native_kline/failures").join(format!("{}.json",component(&format!("{year}:{product}"))));
            let mut p=progress.lock().unwrap();
            match result {
                Ok(stats)=>{
                    p.0+=1;p.1.add(&stats);
                    if failure.exists(){let _=fs::remove_file(&failure);}
                    eprintln!("{year}/{product}: complete ({}/{count})",p.0);
                },
                Err(error)=>{
                    let error=format!("{error:#}");eprintln!("{year}/{product}: {error}");
                    p.2.push(json!({"year":year,"product":product,"error":error}));
                    let _=io::save(&failure,p.2.last().unwrap());
                },
            }
            if let Err(error)=io::save(&cfg.run_dir().join("state.json"),&json!({"stage":"running","groups":count,"completed":p.0,"stats":p.1,"failed":p.2,"seconds":start.elapsed().as_secs(),"workers":cfg.workers})) {
                eprintln!("cannot save local progress checkpoint: {error:#}");
                p.2.push(json!({"year":year,"product":product,"error":format!("cannot save local progress: {error:#}")}));
            }
        });
    });
    let p = progress.into_inner().unwrap();
    let mut conservation = None;
    if cfg.year.is_none() && cfg.product.is_none() && cfg.ric.is_none() && p.2.is_empty() {
        let mut n = 0;
        let mut v = 0;
        for s in &sources {
            let value = *s.totals.lock().unwrap();
            let (a, b) = value
                .with_context(|| format!("source gross quantity not available: {}", s.job.id))?;
            n += a;
            v += b
        }
        ensure!(
            (p.1.trades, p.1.volume) == (n, v),
            "global raw trade conservation failed: output={},source={n}",
            p.1.trades
        );
        conservation = Some(json!({"trades":n,"volume":v}));
    }
    io::save(
        &cfg.run_dir().join("state.json"),
        &json!({"stage":if p.2.is_empty(){"complete"}else{"failed"},"groups":count,"completed":p.0,"stats":p.1,"failed":p.2,"seconds":start.elapsed().as_secs(),"conservation":conservation}),
    )?;
    ensure!(
        p.2.is_empty(),
        "some native groups failed; see actual problem records"
    );
    Ok(())
}

/// Bounded validation writes only to a scratch directory and never checkpoints
/// a sampled source as complete or touches the formal output.
pub fn check(
    plan: &Path,
    job_id: &str,
    ric: &str,
    limit: u64,
    output: &Path,
    cme: bool,
) -> Result<()> {
    let plan = io::read_json(plan)?;
    let jobs: Vec<Job> = serde_json::from_value(plan["jobs"].clone())?;
    let job = jobs
        .into_iter()
        .find(|j| j.id == job_id)
        .context("sample job")?;
    let product = job.product.clone();
    let identity = json!({"job":job});
    let source = Source {
        job,
        identity,
        sparse: None,
        rics: vec![],
        range: None,
        control_range: None,
        totals: Mutex::new(None),
        index: PathBuf::new(),
        sparse_ranges: Mutex::new(None),
    };
    let db = open(&source.job)?;
    let dbs = vec![db];
    let mut cursor = Events::new(&[&source], &dbs, ric, 0, u64::MAX, 13)?;
    let mut decoder = Decoder::new(cme);
    let mut previous = State::Unknown;
    let mut writer = io::StatusWriter::new(output.into(), &source.job.product, ric)?;
    let mut count = 0;
    while count < limit {
        let Some(i) = cursor.next_index()? else { break };
        let e = cursor.peek(i)?;
        let d = decoder.decode(e.qualifiers, previous);
        let before = previous;
        previous = d.state;
        writer.push(StatusRow {
            at: e.at,
            before,
            decoded: d,
            qualifiers: e.qualifiers.into(),
        })?;
        count += 1;
        cursor.advance(i)
    }
    let files = writer.finish()?;
    ensure!(
        count > 0,
        "no source status rows matched this validation RIC"
    );
    let mut checked = 0;
    let mut source = Events::new(&[&source], &dbs, ric, 0, u64::MAX, 13)?;
    let mut previous = State::Unknown;
    for file in &files {
        let mut read = io::Rows::new(&file.path)?;
        while let Some((batch, row)) = read.next_row()? {
            let i = source.next_index()?.context("sample lost source row")?;
            let raw = source.peek(i)?;
            let decoded = decoder.decode(raw.qualifiers, previous);
            let before = previous;
            previous = decoded.state;
            io::compare_status(
                batch,
                row,
                &product,
                ric,
                &StatusRow {
                    at: raw.at,
                    before,
                    decoded,
                    qualifiers: raw.qualifiers.into(),
                },
            )?;
            source.advance(i);
            checked += 1;
        }
    }
    ensure!(checked == count, "sample content count mismatch");
    println!("validated {checked} direct status rows in {} scratch daily Parquet files; source not marked complete",files.len());
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use rocksdb::ColumnFamilyDescriptor;
    #[test]
    fn aggregation_preserves_late_zero_and_negative() {
        let mut b = Bar::default();
        for (event, published, price, volume) in [(5, 9, 10, 2), (2, 11, -20, 3), (7, 12, 15, 0)] {
            b.add(Bar {
                open: price,
                high: price,
                low: price,
                close: price,
                volume,
                count: 1,
                first: (event, published, 0, 0),
                last: (event, published, 0, 0),
            })
            .unwrap()
        }
        assert_eq!(
            (b.open, b.high, b.low, b.close, b.volume, b.count),
            (-20, 15, -20, 15, 5, 3)
        );
    }
    #[test]
    fn direct_db_cross_year_late_print_zero_volume_and_status_fill() {
        use arrow_array::{Array, BooleanArray, Float64Array};
        use cme_tas_replay::{
            encode_cme_correction, encode_cme_status, encode_cme_trade, SlimCorrection, SlimStatus,
            SlimTrade,
        };
        let nonce = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap()
            .as_nanos();
        let root = PathBuf::from(format!(
            "/mnt/nvme-raid0-28t/fanghaizhou/cargo-target/market_replay/tests/native_{}_{nonce}",
            std::process::id()
        ));
        fs::create_dir_all(&root).unwrap();
        let path = root.join("db");
        let mut opts = Options::default();
        opts.create_if_missing(true);
        opts.create_missing_column_families(true);
        let db = DB::open_cf_descriptors(
            &opts,
            &path,
            ["default", "p:2025:X", "p:2026:X"]
                .into_iter()
                .map(|name| ColumnFamilyDescriptor::new(name, Options::default())),
        )
        .unwrap();
        let boundary = io::year_start(2026).unwrap();
        let second = 1_000_000_000;
        for (year, pub_at, hms, price, volume, seq) in [
            (2025, boundary - 4 * second, DAY - 5 * second, 100, 2, 0),
            (2025, boundary - second, DAY - second, 100, 3, 1),
            (2026, boundary + 2 * second, DAY - 10 * second, -10, 3, 0),
            (2026, boundary + 2 * second, second, 110, 0, 1),
        ] {
            let row = SlimTrade {
                ric: "X".into(),
                ts_utc_ns: pub_at,
                exch_hms_ns: hms,
                price: price * second as i64,
                volume,
                bid: i64::MIN,
                bid_size: u32::MAX,
                ask: i64::MIN,
                ask_size: u32::MAX,
                aggressor: 0,
            };
            db.put_cf(
                db.cf_handle(&format!("p:{year}:X")).unwrap(),
                encode_all_key(1, "X", pub_at, 0, seq).unwrap(),
                encode_cme_trade(&row).unwrap(),
            )
            .unwrap();
        }
        for (ric, at, q) in [
            ("X", boundary, "15[TRD_TYPE]"),
            (
                "X",
                boundary + 2 * MINUTE,
                "4[TRD_TYPE];C[INST_PHASE];0[HALT_REASN]",
            ),
            ("Y", boundary, "NH[HALT_RSN]"),
            ("Y", boundary + MINUTE, "15[TRD_TYPE]"),
            ("B", io::year_start(2027).unwrap(), "15[TRD_TYPE]"),
        ] {
            let row = SlimStatus {
                ric: ric.into(),
                ts_utc_ns: at,
                exch_hms_ns: u64::MAX,
                qualifiers: q.into(),
            };
            db.put_cf(
                db.cf_handle("p:2026:X").unwrap(),
                encode_all_key(13, ric, at, 0, 0).unwrap(),
                encode_cme_status(&row).unwrap(),
            )
            .unwrap();
        }
        let orphan = SlimCorrection {
            ric: "Z".into(),
            ts_utc_ns: boundary + MINUTE,
            exch_hms_ns: u64::MAX,
            price: i64::MIN,
            volume: u32::MAX,
            acc_volume: u32::MAX,
            original_price: 100,
            original_volume: 5,
            original_seq: 8,
            qualifiers: "611[CAN_COND]".into(),
        };
        db.put_cf(
            db.cf_handle("p:2026:X").unwrap(),
            encode_all_key(12, "Z", orphan.ts_utc_ns, 0, 0).unwrap(),
            encode_cme_correction(&orphan).unwrap(),
        )
        .unwrap();
        drop(db);
        let cfg = Config {
            project: root.clone(),
            plan: root.join("plan.json"),
            cache: root.join("no_cache"),
            work: root.join("work"),
            output: root.join("output"),
            year: None,
            product: None,
            ric: None,
            workers: 1,
            publisher: Some(io::PublicationPool::new(2).unwrap()),
            replace: None,
        };
        let formal = Config {
            output: PathBuf::from("/mnt/nfs/30.100_cta_data/lseg"),
            ric: Some("ESM26".into()),
            ..cfg.clone()
        };
        assert!(
            formal.formal(),
            "RIC filters must preserve revised-data protection"
        );
        assert!(!cfg.formal());
        let make = |year, period: &str, index| Job {
            id: format!("fixture{year}"),
            db: path.clone(),
            cf: format!("p:{year}:X"),
            year,
            product: "X".into(),
            period: period.into(),
            source: "main".into(),
            source_index: index,
            extra: BTreeMap::new(),
        };
        let sources = vec![
            Arc::new(source(&cfg, make(2025, "2025-12-31T23:59:00Z_2026-01-01", 0)).unwrap()),
            Arc::new(source(&cfg, make(2026, "2026-01-01_2026-01-01T00:03:00Z", 1)).unwrap()),
        ];
        let source_jobs: Vec<_> = sources.iter().map(|s| s.job.clone()).collect();
        assert_eq!(
            target_groups(&cfg, &sources).unwrap(),
            vec![(2027, "X".into()), (2026, "X".into()), (2025, "X".into())]
        );
        assert_eq!(
            verify_jobs(&source_jobs).unwrap(),
            (1, 2, BTreeSet::from([2025, 2026]))
        );
        assert!(format!("{:#}", verify_jobs(&source_jobs[..1]).unwrap_err()).contains("p:2026:X"));
        let mut meta = Metadata::default();
        meta.venues
            .insert((2025, "X".into()), BTreeSet::from(["cme".into()]));
        meta.venues
            .insert((2026, "X".into()), BTreeSet::from(["cme".into()]));
        meta.venues
            .insert((2026, "Y".into()), BTreeSet::from(["cme".into()]));
        let old = group(&cfg, &sources, &meta, 2025, "X").unwrap();
        let new = group(&cfg, &sources, &meta, 2026, "X").unwrap();
        assert_eq!((old.trades, new.trades, old.volume + new.volume), (3, 1, 8));
        assert_eq!((old.kline_rows, new.kline_rows), (1, 3));
        let old_file = cfg.output.join("cme/2025/X/20251231/kline.parquet");
        let b = io::batches(&old_file).unwrap().next().unwrap().unwrap();
        for (name, value) in [
            ("open", -10.0),
            ("high", 100.0),
            ("low", -10.0),
            ("close", 100.0),
        ] {
            assert_eq!(
                b.column_by_name(name)
                    .unwrap()
                    .as_any()
                    .downcast_ref::<Float64Array>()
                    .unwrap()
                    .value(0),
                value
            )
        }
        let file = cfg.output.join("cme/2026/X/20260101/kline.parquet");
        let b = io::batches(&file).unwrap().next().unwrap().unwrap();
        assert_eq!(io::uint(&b, 0, "trade_count").unwrap(), 1);
        assert_eq!(io::uint(&b, 0, "volume").unwrap(), 0);
        let fill = b
            .column_by_name("is_ffill")
            .unwrap()
            .as_any()
            .downcast_ref::<BooleanArray>()
            .unwrap();
        assert!(!fill.value(0));
        assert!(fill.value(1) && fill.value(2));
        assert_eq!(io::string(&b, 0, "status").unwrap(), "TRADING");
        assert_eq!(io::string(&b, 2, "status").unwrap(), "SCHEDULED_CLOSED");
        assert!(!b.column_by_name("close").unwrap().is_null(1));
        // Two writers can observe a missing target before either publishes.
        // The later hard-link collision must preserve the first file's data.
        let race = root.join("publish_race/kline.parquet");
        let mut first_writer = io::Sink::new(race.clone(), io::kline_schema()).unwrap();
        let mut later_writer = io::Sink::new(race.clone(), io::kline_schema()).unwrap();
        first_writer.write(&b).unwrap();
        let mut conflicting = b.columns().to_vec();
        conflicting[0] = Arc::new(arrow_array::StringArray::from_iter_values(
            (0..b.num_rows()).map(|_| "DIFFERENT"),
        ));
        let conflicting = arrow_array::RecordBatch::try_new(b.schema(), conflicting).unwrap();
        later_writer.write(&conflicting).unwrap();
        first_writer.finish().unwrap();
        later_writer.finish().unwrap();
        io::verify_kline(&race, &b).unwrap();
        let mut existing_writer = io::Sink::new(race.clone(), io::kline_schema()).unwrap();
        existing_writer.write(&b).unwrap();
        existing_writer.finish().unwrap();
        // A listed date directory may disappear before a file is opened.
        let cache_root = root.join("directory_cache");
        let cache_day = cache_root.join("20260101");
        fs::create_dir_all(&cache_day).unwrap();
        let empty_status = io::StatusWriter::new(cache_root.clone(), "X", "X").unwrap();
        let (_, mut directories) = empty_status.finish_with_directories().unwrap();
        fs::remove_dir(&cache_day).unwrap();
        let recovered = cache_day.join("kline.parquet");
        let mut recovered_writer =
            io::Sink::new_in(recovered.clone(), io::kline_schema(), &mut directories).unwrap();
        recovered_writer.write(&b).unwrap();
        recovered_writer.finish().unwrap();
        io::verify_kline(&recovered, &b).unwrap();
        assert_eq!(sources[0].totals.lock().unwrap().unwrap(), (2, 5));
        assert_eq!(sources[1].totals.lock().unwrap().unwrap(), (2, 3));
        assert!(!root.join("work/status.json").exists());
        // Compatibility with the HG pilot: a complete embedded contract list
        // can replace absent per-contract files, but inconsistent totals fail.
        let group_file = cfg.run_dir().join("groups/2026%3AX.json");
        let mut receipt = io::read_json(&group_file).unwrap();
        receipt["reused_legacy_group"] = json!(true);
        io::save(&group_file, &receipt).unwrap();
        let contract_dir = cfg.run_dir().join("contracts/2026%3AX");
        let contract_file = contract_dir.join("X.json");
        let mut embedded = io::read_json(&contract_file).unwrap()["stats"].clone();
        embedded["ric"] = json!("X");
        embedded["reused"] = json!(false);
        fs::remove_file(contract_file).unwrap();
        fs::remove_dir(contract_dir).unwrap();
        let mut legacy = receipt["stats"].clone();
        legacy["input_signature"] = receipt["signature"].clone();
        legacy["contracts"] = json!([embedded]);
        let legacy_file = cfg
            .project
            .join("artifacts/lseg_full_rocksdb_20261112/groups/2026%3AX.json");
        let mut bad = legacy.clone();
        bad["volume"] = json!(99);
        io::save(&legacy_file, &bad).unwrap();
        assert!(missing_status::group_status(&cfg, &sources, &meta, 2026, "X").is_err());
        io::save(&legacy_file, &legacy).unwrap();
        let supplementary = missing_status::group_status(&cfg, &sources, &meta, 2026, "X").unwrap();
        assert_eq!((supplementary.rics, supplementary.status_rows), (2, 2));
        let status_file = cfg
            .output
            .join("cme/2026/Y/20260101/session_status.parquet");
        let status = io::batches(&status_file).unwrap().next().unwrap().unwrap();
        assert_eq!(io::string(&status, 0, "session_state").unwrap(), "UNKNOWN");
        assert_eq!(io::string(&status, 1, "session_state").unwrap(), "TRADING");
        assert!(!cfg
            .output
            .join("cme/2026/Y/20260101/kline.parquet")
            .exists());
        let resumed = missing_status::group_status(&cfg, &sources, &meta, 2026, "X").unwrap();
        assert_eq!((resumed.rics, resumed.status_rows), (2, 2));
        let orphan_problem = fs::read_to_string(
            cfg.output
                .join("meta/problems/native_kline/status_only/2026%3AX/Z.csv"),
        )
        .unwrap();
        assert!(orphan_problem.contains("source_witness_pending"));
        assert!(!cfg
            .output
            .join("review/2026/Z/20260101/kline.parquet")
            .exists());
        // A control-only event outside its CF's labelled year must create a
        // target group even when no trade has that UTC year.
        let empty = group(&cfg, &sources, &meta, 2027, "X").unwrap();
        assert_eq!((empty.rics, empty.trades, empty.kline_rows), (0, 0, 0));
        let next_year = missing_status::group_status(&cfg, &sources, &meta, 2027, "X").unwrap();
        assert_eq!((next_year.rics, next_year.status_rows), (1, 1));
        let next_status = cfg
            .output
            .join("review/2027/B/20270101/session_status.parquet");
        assert_eq!(
            io::batches(&next_status)
                .unwrap()
                .next()
                .unwrap()
                .unwrap()
                .num_rows(),
            1
        );
        assert!(!next_status.with_file_name("kline.parquet").exists());
        let resumed_source = source(&cfg, source_jobs[1].clone()).unwrap();
        assert_eq!(resumed_source.control_range, sources[1].control_range);
        drop(sources);
        fs::remove_dir_all(root).unwrap();
    }
}
