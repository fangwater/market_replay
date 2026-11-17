//! RocksDB of ALL Normalized LL2 updates (no frequency baked in), one database
//! per UTC day: the "washed" layer between the shards and the slices.
//!
//! * key   `'U' | RIC | 0x00 | UTC second (u32 BE)`: RIC leads, one key per RIC and
//!   non-empty second, keys of one RIC contiguous and ascending.
//! * value all updates of that second in file order, each a delta record against
//!   the previous update of the RIC (`l2_layout`: changed-slot masks, integer price
//!   steps, varints; an unchanged book is one flag byte because slices need the
//!   time of every update). Every 256th non-empty second restarts from a
//!   self-contained record. zstd level 9, 256 KB blocks.
//! * key   `'M' | product`: completion marker, written after all RICs of the
//!   product-day were ingested (readers only trust marked product-days).
//! * Every RIC is written as one SST file and ingested (no write amplification,
//!   no compaction); a partial RIC of a failed run is deleted before re-ingest.
use crate::l2_layout::{decode_bucket, BucketWriter, Codec2};
use crate::l2_slice::{
    parse_book, Book, Params, RicRun, RicStats, Sink, TsParser, BOOK_FIELDS, DAY_NS,
};
use crate::tasdb;
use anyhow::{ensure, Context, Result};
use rocksdb::{
    BlockBasedOptions, Cache, DBCompressionType, IngestExternalFileOptions, IteratorMode, Options,
    ReadOptions, SstFileWriter, WriteOptions, DB,
};
use serde_json::{json, Value};
use std::{
    collections::HashSet,
    fs::{self, File},
    io::{BufRead, BufReader},
    path::{Path, PathBuf},
};

const FORMAT: &str = "l2db-2";
pub const ZSTD_LEVEL: i32 = 9;
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
    o.set_compression_options(-14, ZSTD_LEVEL, 0, 0);
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
    sst: SstFileWriter<'a>,
    path: PathBuf,
    packer: BucketWriter,
    stats: WashedRic,
}

fn put_bucket(
    sst: &mut SstFileWriter<'_>,
    prefix: &[u8],
    stats: &mut WashedRic,
    bucket: u32,
    val: &[u8],
) -> Result<()> {
    let mut key = Vec::with_capacity(prefix.len() + 4);
    key.extend_from_slice(prefix);
    key.extend_from_slice(&bucket.to_be_bytes());
    sst.put(&key, val)?;
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
        mut sst,
        path,
        stats,
        prefix,
        ..
    } = w;
    sst.finish()
        .with_context(|| format!("finish {}", path.display()))?;
    if has_prefix(db, &prefix)? {
        // leftover of an interrupted earlier run of this product-day
        let cf = db.cf_handle("default").context("default column family")?;
        db.delete_range_cf(&cf, prefix.as_slice(), prefix_upper(&prefix).as_slice())?;
        db.flush()?;
    }
    let mut io = IngestExternalFileOptions::default();
    io.set_move_files(true);
    db.ingest_external_file_opts(&io, vec![&path])
        .with_context(|| format!("ingest {}", path.display()))?;
    Ok(stats)
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
                sst,
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
