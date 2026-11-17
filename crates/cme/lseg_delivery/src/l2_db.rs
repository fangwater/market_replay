//! RocksDB of ALL Normalized LL2 updates (no frequency baked in), one database
//! per UTC day: the "washed" layer between the shards and the slices.
//!
//! * key   `'U' | RIC | 0x00 | UTC second (u32 BE)`: RIC leads, one key per RIC and
//!   non-empty second, keys of one RIC contiguous and ascending.
//! * value all updates of that second in file order, each a delta record against
//!   the previous update of the RIC (`l2_layout`: changed-slot masks, integer price
//!   steps, varints; an unchanged book is one flag byte because slices need the
//!   time of every update). Every 256th non-empty second restarts from a
//!   self-contained record. zstd level 6, 256 KB blocks.
//! * key   `'M' | product`: completion marker, written after all RICs of the
//!   product-day were ingested (readers only trust marked product-days).
//! * Every RIC is written as one SST file and ingested (no write amplification,
//!   no compaction); a partial RIC of a failed run is deleted before re-ingest.
use crate::l2_layout::{decode_bucket, BucketWriter, Codec2};
use crate::l2_slice::{
    parse_book, Book, BookParser, Params, RicRun, RicStats, Sink, TsParser, BOOK_FIELDS, DAY_NS,
};
use crate::tasdb;
use anyhow::{ensure, Context, Result};
use rocksdb::{
    BlockBasedOptions, Cache, DBCompressionType, IngestExternalFileOptions, IteratorMode, Options,
    ReadOptions, SstFileWriter, WriteOptions, DB,
};
use serde_json::{json, Value};
use std::{
    collections::{BTreeMap, HashSet},
    fs::{self, File},
    io::{BufRead, BufReader},
    path::{Path, PathBuf},
    sync::Arc,
};

const FORMAT: &str = "l2db-2";
/// Default zstd level of the stored blocks (`set_sst_zstd_level` overrides it
/// for the profiler and the size/CPU comparison only).
pub const ZSTD_LEVEL: i32 = 6;
static SST_ZSTD_LEVEL: std::sync::atomic::AtomicI32 = std::sync::atomic::AtomicI32::new(ZSTD_LEVEL);

pub fn set_sst_zstd_level(level: i32) {
    SST_ZSTD_LEVEL.store(level, std::sync::atomic::Ordering::Relaxed);
}
const BLOCK_BYTES: usize = 256 * 1024;

pub fn update_prefix(ric: &str) -> Vec<u8> {
    let mut k = Vec::with_capacity(ric.len() + 2);
    k.push(b'U');
    k.extend_from_slice(ric.as_bytes());
    k.push(0);
    k
}

fn prefix_upper(prefix: &[u8]) -> Vec<u8> {
    let mut u = prefix.to_vec();
    *u.last_mut().expect("prefix") += 1; // 0x00 -> 0x01
    u
}

pub fn marker_key(product: &str) -> Vec<u8> {
    let mut k = vec![b'M'];
    k.extend_from_slice(product.as_bytes());
    k
}

pub fn day_dir(db_root: &Path, day: &str) -> PathBuf {
    db_root.join(day.replace('-', ""))
}

/// Opens (creating) the day database for writing. Compaction is off: the data
/// arrives as ingested SST files of disjoint key ranges.
pub fn open_write(dir: &Path) -> Result<DB> {
    fs::create_dir_all(dir)?;
    let mut o = Options::default();
    o.create_if_missing(true);
    o.set_compression_type(DBCompressionType::Zstd);
    o.set_compression_options(-14, ZSTD_LEVEL, 0, 0);
    o.set_disable_auto_compactions(true);
    o.set_max_background_jobs(2);
    o.set_max_open_files(256);
    let mut t = BlockBasedOptions::default();
    t.set_block_size(BLOCK_BYTES);
    t.set_block_cache(&Cache::new_lru_cache(64 << 20));
    o.set_block_based_table_factory(&t);
    let db = DB::open(&o, dir).with_context(|| format!("open {} for writing", dir.display()))?;
    if db.get(b"V")?.is_none() {
        db.put(b"V", FORMAT.as_bytes())?;
    }
    let tmp = dir.join("ingest_tmp");
    if tmp.exists() {
        fs::remove_dir_all(&tmp)?;
    }
    fs::create_dir_all(&tmp)?;
    Ok(db)
}

fn sst_options() -> Options {
    let mut o = Options::default();
    o.set_compression_type(DBCompressionType::Zstd);
    o.set_compression_options(
        -14,
        SST_ZSTD_LEVEL.load(std::sync::atomic::Ordering::Relaxed),
        0,
        0,
    );
    let mut t = BlockBasedOptions::default();
    t.set_block_size(BLOCK_BYTES);
    o.set_block_based_table_factory(&t);
    o
}

#[derive(Clone, Debug, Default)]
pub struct WashedRic {
    pub ric: String,
    pub updates: u64,
    pub first_ts: i64,
    pub last_ts: i64,
    pub key_bytes: u64,
    pub value_bytes: u64,
    /// Size of the finished SST file (compressed); 0 before the SST stage.
    pub sst_bytes: u64,
}

fn has_prefix(db: &DB, prefix: &[u8]) -> Result<bool> {
    let mut it = db.iterator(IteratorMode::From(prefix, rocksdb::Direction::Forward));
    Ok(match it.next() {
        Some(r) => r?.0.starts_with(prefix),
        None => false,
    })
}

struct RicWriter<'a> {
    ric: String,
    prefix: Vec<u8>,
    /// `None` only when profiling stops before the SST stage.
    sst: Option<SstFileWriter<'a>>,
    path: PathBuf,
    packer: BucketWriter,
    stats: WashedRic,
}

fn put_bucket(
    sst: &mut Option<SstFileWriter<'_>>,
    prefix: &[u8],
    stats: &mut WashedRic,
    bucket: u32,
    val: &[u8],
) -> Result<()> {
    let mut key = Vec::with_capacity(prefix.len() + 4);
    key.extend_from_slice(prefix);
    key.extend_from_slice(&bucket.to_be_bytes());
    if let Some(sst) = sst {
        sst.put(&key, val)?;
    }
    stats.key_bytes += key.len() as u64;
    stats.value_bytes += val.len() as u64;
    Ok(())
}

fn finish_ric(db: &DB, mut w: RicWriter<'_>) -> Result<WashedRic> {
    {
        let RicWriter {
            prefix,
            sst,
            packer,
            stats,
            ..
        } = &mut w;
        packer.finish(&mut |b, v| put_bucket(sst, prefix, stats, b, v))?;
    }
    let RicWriter {
        sst,
        path,
        stats,
        prefix,
        ..
    } = w;
    let Some(mut sst) = sst else {
        return Ok(stats);
    };
    sst.finish()
        .with_context(|| format!("finish {}", path.display()))?;
    let mut stats = stats;
    stats.sst_bytes = fs::metadata(&path)?.len();
    clean_ric(db, &prefix)?;
    ingest_sst(db, &path)?;
    Ok(stats)
}

/// Deletes whatever an interrupted earlier run left under this RIC's prefix.
pub fn clean_ric(db: &DB, prefix: &[u8]) -> Result<()> {
    if has_prefix(db, prefix)? {
        // (`cf_handle("default")` is None for a database opened with `DB::open`)
        let mut wb = rocksdb::WriteBatch::default();
        wb.delete_range(prefix, prefix_upper(prefix).as_slice());
        db.write(wb)?;
        db.flush()?;
    }
    Ok(())
}

/// Moves a finished SST into `db`.
fn ingest_sst(db: &DB, path: &Path) -> Result<()> {
    let mut io = IngestExternalFileOptions::default();
    io.set_move_files(true);
    db.ingest_external_file_opts(&io, vec![path])
        .with_context(|| format!("ingest {}", path.display()))?;
    Ok(())
}

/// Where `wash_segment` puts what it writes.
pub trait WashTarget: Sync {
    /// Database that receives the data of UTC day number `day` (days since 1970).
    fn db(&self, day: i64) -> Result<Arc<DB>>;
    /// Directory for the temporary SST files of that database.
    fn tmp_dir(&self, day: i64) -> PathBuf;
    /// true: one SST per (RIC, UTC day) into that day's database (daily files);
    /// false: one SST per RIC into a single database (long sorted streams).
    fn per_day(&self) -> bool;
    /// Every row must belong to this UTC day (daily files); `None` for streams.
    fn only_day(&self) -> Option<i64>;
    /// Removes what an interrupted earlier run left for this RIC (before the
    /// first segment of the RIC is washed).
    fn clean_ric(&self, ric: &str) -> Result<()>;
}

/// What one RIC contributed.
#[derive(Clone, Debug, Default)]
pub struct RicOutcome {
    pub washed: WashedRic,
    /// Update records by UTC month (`YYYYMM`).
    pub months: BTreeMap<u32, u64>,
}

/// (year, month, day) of a day number (days since 1970-01-01), proleptic Gregorian.
pub fn civil_from_days(days: i64) -> (i64, u32, u32) {
    let z = days + 719_468;
    let era = z.div_euclid(146_097);
    let doe = z.rem_euclid(146_097);
    let yoe = (doe - doe / 1460 + doe / 36_524 - doe / 146_096) / 365;
    let doy = doe - (365 * yoe + yoe / 4 - yoe / 100);
    let mp = (5 * doy + 2) / 153;
    let d = (doy - (153 * mp + 2) / 5 + 1) as u32;
    let m = if mp < 10 { mp + 3 } else { mp - 9 } as u32;
    (yoe + era * 400 + (m <= 2) as i64, m, d)
}

/// `YYYYMM` of a day number.
pub fn month_of_day(days: i64) -> u32 {
    let (y, m, _) = civil_from_days(days);
    y as u32 * 100 + m
}

struct OpenSst<'a> {
    sst: Option<SstFileWriter<'a>>,
    path: PathBuf,
    day: i64,
    packer: BucketWriter,
}

/// Stage 2: parses every row of one shard (RIC-aligned, see `l2_batch`) and
/// ingests one SST per RIC. Checks that rows stay in the file's UTC day and in
/// time order. Returns the per-RIC counters.
pub fn wash_shard(
    shard: &Path,
    day_start_ns: i64,
    db: &DB,
    tmp_dir: &Path,
) -> Result<Vec<WashedRic>> {
    let decoder = zstd::stream::read::Decoder::new(File::open(shard)?)?;
    let mut reader = BufReader::with_capacity(1 << 20, decoder);
    let mut line = Vec::with_capacity(1024);
    ensure!(reader.read_until(b'\n', &mut line)? > 0, "empty shard");
    ensure!(
        String::from_utf8_lossy(&line).trim_end() == crate::l2_slice::expected_header(),
        "unexpected shard header"
    );
    let day_end_ns = day_start_ns + DAY_NS;
    let mut ts = TsParser::new();
    let mut book: Book = [f64::NAN; BOOK_FIELDS];
    let mut done: HashSet<String> = HashSet::new();
    let mut out = Vec::new();
    let opts = sst_options();
    let mut cur: Option<RicWriter> = None;
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
        let ts_b = it.next().context("short row")?;
        ensure!(!ric_b.is_empty(), "empty RIC");
        if cur.as_ref().is_none_or(|w| w.ric.as_bytes() != ric_b) {
            if let Some(w) = cur.take() {
                done.insert(w.ric.clone());
                out.push(finish_ric(db, w)?);
            }
            let ric = std::str::from_utf8(ric_b)?.to_string();
            ensure!(
                !done.contains(&ric),
                "RIC {ric} appears in more than one block of the shard"
            );
            let path = tmp_dir.join(format!("{}.sst", tasdb::component(&ric)));
            let sst = SstFileWriter::create(&opts);
            sst.open(&path)
                .with_context(|| format!("create {}", path.display()))?;
            cur = Some(RicWriter {
                prefix: update_prefix(&ric),
                stats: WashedRic {
                    ric: ric.clone(),
                    ..Default::default()
                },
                ric,
                sst: Some(sst),
                path,
                packer: BucketWriter::new(),
            });
        }
        let w = cur.as_mut().expect("opened");
        let t = ts.parse(ts_b)?;
        ensure!(
            t >= day_start_ns && t < day_end_ns,
            "RIC {}: Date-Time {} is outside the file's UTC day",
            w.ric,
            String::from_utf8_lossy(ts_b)
        );
        if w.stats.updates > 0 {
            ensure!(
                t >= w.stats.last_ts,
                "RIC {}: Date-Time goes backwards ({})",
                w.ric,
                String::from_utf8_lossy(ts_b)
            );
        } else {
            w.stats.first_ts = t;
        }
        parse_book(&line, &mut book).with_context(|| format!("RIC {} at {t}", w.ric))?;
        {
            let RicWriter {
                prefix,
                sst,
                packer,
                stats,
                ..
            } = &mut *w;
            packer.push(t, &book, &mut |b, v| put_bucket(sst, prefix, stats, b, v))?;
        }
        w.stats.updates += 1;
        w.stats.last_ts = t;
    }
    if let Some(w) = cur.take() {
        out.push(finish_ric(db, w)?);
    }
    Ok(out)
}

/// Stage 2 for one *segment* of a RIC block, given as whole text lines (the
/// in-memory pipeline, `l2_stream`): same parse, same delta/bucket encoding and
/// the same SST ingestion as `wash_shard`. A segment starts where the sequential
/// writer would start a self-contained record anyway (the RIC's first row, the
/// first row of a UTC day, the first row of every 256th non-empty second of a
/// day), so washing the segments of a RIC independently, in any order and in
/// parallel, stores exactly the bytes a sequential pass would. The bucket packer
/// restarts at every UTC day, so a (RIC, day) is self-contained and decodes from
/// its first key (this is also what the per-day databases of daily files hold).
/// The caller removes leftovers of the RIC (`clean_ric`) before the first segment.
pub fn wash_segment(t: &dyn WashTarget, ric: &str, seg: u32, data: &[u8]) -> Result<RicOutcome> {
    let opts = sst_options();
    let prefix = update_prefix(ric);
    let mut ts = TsParser::new();
    let mut books = BookParser::new();
    let mut book: Book = [f64::NAN; BOOK_FIELDS];
    let mut out = RicOutcome {
        washed: WashedRic {
            ric: ric.to_string(),
            ..Default::default()
        },
        ..Default::default()
    };
    let mut cur: Option<OpenSst> = None;
    let mut cur_day = i64::MIN;
    let comp = tasdb::component(ric);
    let per_day = t.per_day();
    let mut line_no = 0u64;
    let (mut month_day, mut month_cnt) = (i64::MIN, 0u64);
    for raw in data.split(|b| *b == b'\n') {
        let mut line = raw;
        while let Some(b'\r') = line.last() {
            line = &line[..line.len() - 1];
        }
        if line.is_empty() {
            continue;
        }
        line_no += 1;
        ensure!(
            line.len() > ric.len() && line.starts_with(ric.as_bytes()) && line[ric.len()] == b',',
            "RIC {ric}: row {line_no} belongs to another RIC"
        );
        let c1 =
            ric.len() + 1 + memchr::memchr(b',', &line[ric.len() + 1..]).context("short row")?;
        let c2 = c1 + 1 + memchr::memchr(b',', &line[c1 + 1..]).context("short row")?;
        let ts_b = &line[c1 + 1..c2];
        let tn = ts.parse(ts_b)?;
        let day = tn.div_euclid(DAY_NS);
        if let Some(only) = t.only_day() {
            ensure!(
                day == only,
                "RIC {ric}: Date-Time {} is outside the file's UTC day",
                String::from_utf8_lossy(ts_b)
            );
        }
        if out.washed.updates > 0 {
            ensure!(
                tn >= out.washed.last_ts,
                "RIC {ric}: Date-Time goes backwards ({})",
                String::from_utf8_lossy(ts_b)
            );
        } else {
            out.washed.first_ts = tn;
        }
        if day != cur_day {
            if let Some(mut o) = cur.take() {
                o.packer
                    .finish(&mut |b, v| put_bucket(&mut o.sst, &prefix, &mut out.washed, b, v))?;
                if per_day {
                    o.sst.take().expect("sst").finish()?;
                    out.washed.sst_bytes += fs::metadata(&o.path)?.len();
                    let db = t.db(o.day)?;
                    ingest_sst(&db, &o.path)?;
                } else {
                    // same SST, fresh packer: the new day starts self-contained
                    o.packer = BucketWriter::new();
                    o.day = day;
                    cur = Some(o);
                }
            }
            cur_day = day;
            if cur.is_none() {
                let dir = t.tmp_dir(day);
                let path = dir.join(if per_day {
                    format!("{comp}.{seg}.{day}.sst")
                } else {
                    format!("{comp}.{seg}.sst")
                });
                let sst = SstFileWriter::create(&opts);
                sst.open(&path)
                    .with_context(|| format!("create {}", path.display()))?;
                cur = Some(OpenSst {
                    sst: Some(sst),
                    path,
                    day,
                    packer: BucketWriter::new(),
                });
            }
        }
        books
            .parse(line, &mut book)
            .with_context(|| format!("RIC {ric} at {tn}"))?;
        let o = cur.as_mut().expect("open");
        o.packer.push(tn, &book, &mut |b, v| {
            put_bucket(&mut o.sst, &prefix, &mut out.washed, b, v)
        })?;
        out.washed.updates += 1;
        out.washed.last_ts = tn;
        if day != month_day {
            if month_cnt > 0 {
                *out.months.entry(month_of_day(month_day)).or_default() += month_cnt;
            }
            month_day = day;
            month_cnt = 0;
        }
        month_cnt += 1;
    }
    if month_cnt > 0 {
        *out.months.entry(month_of_day(month_day)).or_default() += month_cnt;
    }
    if let Some(mut o) = cur.take() {
        o.packer
            .finish(&mut |b, v| put_bucket(&mut o.sst, &prefix, &mut out.washed, b, v))?;
        o.sst.take().expect("sst").finish()?;
        out.washed.sst_bytes += fs::metadata(&o.path)?.len();
        let db = t.db(o.day)?;
        ingest_sst(&db, &o.path)?;
    }
    Ok(out)
}

/// Joins the outcomes of the segments of one RIC (in segment order) into one;
/// the time must not go backwards from one segment to the next.
pub fn merge_segments(mut parts: Vec<(u32, RicOutcome)>) -> Result<RicOutcome> {
    parts.sort_by_key(|p| p.0);
    let mut it = parts
        .into_iter()
        .map(|p| p.1)
        .filter(|o| o.washed.updates > 0);
    let Some(mut acc) = it.next() else {
        return Ok(RicOutcome::default());
    };
    for o in it {
        ensure!(
            o.washed.first_ts >= acc.washed.last_ts,
            "RIC {}: Date-Time goes backwards across segments",
            acc.washed.ric
        );
        let (w, a) = (&o.washed, &mut acc.washed);
        a.updates += w.updates;
        a.last_ts = w.last_ts;
        a.key_bytes += w.key_bytes;
        a.value_bytes += w.value_bytes;
        a.sst_bytes += w.sst_bytes;
        for (m, n) in o.months {
            *acc.months.entry(m).or_default() += n;
        }
    }
    Ok(acc)
}

/// Completion marker of one product-day (synced).
pub fn write_marker(db: &DB, product: &str, day: &str, rics: &[WashedRic]) -> Result<()> {
    let v = json!({
        "format": FORMAT, "product": product, "day": day,
        "updates": rics.iter().map(|r| r.updates).sum::<u64>(),
        "rics": rics.iter().map(|r| json!([r.ric, r.updates, r.first_ts, r.last_ts])).collect::<Vec<_>>(),
    });
    db.flush()?;
    let mut wo = WriteOptions::default();
    wo.set_sync(true);
    db.put_opt(marker_key(product), serde_json::to_vec(&v)?, &wo)?;
    Ok(())
}

pub fn read_marker(db: &DB, product: &str) -> Result<Option<Value>> {
    match db.get(marker_key(product))? {
        Some(b) => Ok(Some(serde_json::from_slice(&b)?)),
        None => Ok(None),
    }
}

/// Opens a published day database read-only with the repo's fixed limits.
pub fn open_read(dir: &Path, cache: &tasdb::SharedCache) -> Result<DB> {
    cache.open(dir, ["default"])
}

/// Stage 3: decodes every update of `ric` in order and slices it.
pub fn extract_ric(db: &DB, ric: &str, p: &Params, sink: &mut dyn Sink) -> Result<RicStats> {
    let mut run = RicRun::new(ric, sink)?;
    if !run.wanted() {
        return run.finish(p, sink);
    }
    let prefix = update_prefix(ric);
    let upper = prefix_upper(&prefix);
    let mut ro = ReadOptions::default();
    ro.set_iterate_upper_bound(upper);
    ro.fill_cache(false);
    let mut codec = Codec2::new();
    let mut book: Book = [f64::NAN; BOOK_FIELDS];
    for item in db.iterator_opt(IteratorMode::From(&prefix, rocksdb::Direction::Forward), ro) {
        let (k, v) = item?;
        ensure!(k.len() == prefix.len() + 4, "bad key length for {ric}");
        let bucket = u32::from_be_bytes(k[prefix.len()..].try_into().expect("4"));
        decode_bucket(&mut codec, bucket, &v, &mut book, &mut |t, b| {
            run.update(t, b, p, sink)
        })
        .with_context(|| format!("RIC {ric} second {bucket}"))?;
    }
    run.finish(p, sink)
}

/// Stage 3 for one (RIC, UTC day) out of one or several databases given in
/// chronological order (a day database, or the databases of the period
/// packages): only the keys of that UTC day are read; the first record of every
/// (RIC, day) is self-contained (the writer restarts its packer each UTC day).
pub fn extract_ric_day(
    dbs: &[&DB],
    ric: &str,
    day: i64,
    p: &Params,
    sink: &mut dyn Sink,
) -> Result<RicStats> {
    let mut run = RicRun::new(ric, sink)?;
    if !run.wanted() {
        return run.finish(p, sink);
    }
    let prefix = update_prefix(ric);
    let key = |sec: i64| {
        let mut k = prefix.clone();
        k.extend_from_slice(&(sec as u32).to_be_bytes());
        k
    };
    let (from, to) = (key(day * 86_400), key((day + 1) * 86_400));
    for db in dbs {
        let mut ro = ReadOptions::default();
        ro.set_iterate_upper_bound(to.clone());
        ro.fill_cache(false);
        let mut codec = Codec2::new();
        let mut book: Book = [f64::NAN; BOOK_FIELDS];
        for item in db.iterator_opt(IteratorMode::From(&from, rocksdb::Direction::Forward), ro) {
            let (k, v) = item?;
            ensure!(k.len() == prefix.len() + 4, "bad key length for {ric}");
            let bucket = u32::from_be_bytes(k[prefix.len()..].try_into().expect("4"));
            decode_bucket(&mut codec, bucket, &v, &mut book, &mut |t, b| {
                run.update(t, b, p, sink)
            })
            .with_context(|| format!("RIC {ric} second {bucket}"))?;
        }
    }
    run.finish(p, sink)
}

/// Counts the update records of every key in `[from_sec, to_sec)` for `ric`
/// without decoding the records (the first varint of a bucket value is its count).
pub fn count_updates(db: &DB, ric: &str, from_sec: u32, to_sec: u32) -> Result<u64> {
    let prefix = update_prefix(ric);
    let key = |sec: u32| {
        let mut k = prefix.clone();
        k.extend_from_slice(&sec.to_be_bytes());
        k
    };
    let mut ro = ReadOptions::default();
    ro.set_iterate_upper_bound(key(to_sec));
    ro.fill_cache(false);
    let mut n = 0u64;
    for item in db.iterator_opt(
        IteratorMode::From(&key(from_sec), rocksdb::Direction::Forward),
        ro,
    ) {
        let (_, v) = item?;
        let mut pos = 0usize;
        n += crate::l2_layout::get_varint(&v, &mut pos)?;
    }
    Ok(n)
}
