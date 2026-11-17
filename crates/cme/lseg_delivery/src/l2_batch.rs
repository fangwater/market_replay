//! Stages 1 and 2 of the L2 pipeline, the same structure as the TAS pipeline
//! (`cme_tas_shard`, then the replay into RocksDB):
//!
//! 1. **shard**: every source file is decompressed exactly once and split into
//!    independent zstd CSV shards whose boundaries are RIC boundaries;
//! 2. **wash**: the shards are parsed and every update is stored in the day's
//!    RocksDB (`l2_db`, no frequency baked in). Stage 3 (`extract`) later turns
//!    the database into slices of any frequency without touching the gz files.
//!
//! One fixed pool of `workers` threads does both stages (nothing else is spawned):
//! * a free worker washes a finished shard if there is one, otherwise it starts
//!   sharding the next (largest remaining) source file;
//! * finished shards wait on disk (`<shard_root>/<product>/<day>/`) and are
//!   deleted as soon as they are washed, so the disk use is bounded by
//!   `shard_budget_bytes` plus the shard being written. A worker that finds the
//!   queue over budget washes shards itself before it writes another one, so
//!   the pipeline cannot stall, even with one worker.
use crate::l2_db::{self, WashedRic};
use crate::l2_slice::{day_number, expected_header, DAY_NS};
use anyhow::{bail, ensure, Context, Result};
use cme_tas_replay::shard::ZstdShardWriter;
use flate2::read::MultiGzDecoder;
use rocksdb::DB;
use serde_json::{json, Value};
use std::{
    collections::{BTreeMap, HashMap, HashSet, VecDeque},
    fs::{self, File},
    io::{BufRead, BufReader, Seek},
    path::{Path, PathBuf},
    sync::{Arc, Condvar, Mutex},
    time::Instant,
};

const INPUT_BUFFER_BYTES: usize = 16 * 1024 * 1024;
const SHARD_PREFIX: &str = "shard-";

#[derive(Clone, Debug)]
pub struct ShardInfo {
    pub index: u32,
    pub path: PathBuf,
    pub rows: u64,
    pub data_bytes: u64,
    pub compressed_bytes: u64,
    pub first_ric: String,
    pub last_ric: String,
}

#[derive(Clone, Debug, Default)]
pub struct SourceSummary {
    pub rows: u64,
    pub data_bytes: u64,
    pub shards: u32,
    pub rics: usize,
}

fn shard_path(dir: &Path, index: u32) -> PathBuf {
    dir.join(format!("{SHARD_PREFIX}{index:05}.csv.zst"))
}

fn ric_of(line: &[u8]) -> &[u8] {
    line.split(|b| *b == b',').next().unwrap_or(&[])
}

fn trim_eol(mut b: &[u8]) -> &[u8] {
    while matches!(b.last(), Some(b'\n' | b'\r')) {
        b = &b[..b.len() - 1];
    }
    b
}

/// Stage 1: one pass over `source` (gzip, possibly several members) into shards
/// of at least `rows_per_shard` rows, cut only where the RIC changes. `on_shard`
/// is called as soon as a shard is published.
pub fn shard_source(
    source: &Path,
    shard_dir: &Path,
    rows_per_shard: u64,
    zstd_level: i32,
    on_shard: &mut dyn FnMut(ShardInfo) -> Result<()>,
) -> Result<SourceSummary> {
    ensure!(rows_per_shard >= 1, "rows_per_shard must be >= 1");
    let file = File::open(source).with_context(|| format!("open {}", source.display()))?;
    let size = file.metadata()?.len();
    let mut reader = BufReader::with_capacity(
        INPUT_BUFFER_BYTES,
        MultiGzDecoder::new(BufReader::with_capacity(INPUT_BUFFER_BYTES, file)),
    );
    let mut header = Vec::new();
    ensure!(
        reader.read_until(b'\n', &mut header)? > 0,
        "{}: empty source",
        source.display()
    );
    ensure!(
        String::from_utf8_lossy(trim_eol(&header)) == expected_header(),
        "{}: unexpected header",
        source.display()
    );
    let mut done: HashSet<Vec<u8>> = HashSet::new();
    let mut cur: Vec<u8> = Vec::new();
    let mut writer: Option<(ZstdShardWriter, String)> = None;
    let mut index = 0u32;
    let mut sum = SourceSummary::default();
    let mut line = Vec::with_capacity(1024);
    let finish = |w: (ZstdShardWriter, String),
                  last: &[u8],
                  index: u32,
                  on_shard: &mut dyn FnMut(ShardInfo) -> Result<()>|
     -> Result<()> {
        let written = w.0.finish()?;
        on_shard(ShardInfo {
            index,
            path: written.final_path,
            rows: written.rows,
            data_bytes: written.data_bytes,
            compressed_bytes: written.compressed_bytes,
            first_ric: w.1,
            last_ric: String::from_utf8_lossy(last).into_owned(),
        })
    };
    loop {
        line.clear();
        if reader.read_until(b'\n', &mut line)? == 0 {
            break;
        }
        if trim_eol(&line).is_empty() {
            continue;
        }
        let ric = ric_of(&line);
        ensure!(!ric.is_empty(), "{}: empty RIC", source.display());
        if ric != cur.as_slice() {
            let previous = std::mem::take(&mut cur);
            if !previous.is_empty() {
                done.insert(previous.clone());
            }
            ensure!(
                !done.contains(ric),
                "{}: RIC {} appears in more than one block",
                source.display(),
                String::from_utf8_lossy(ric)
            );
            sum.rics += 1;
            cur = ric.to_vec();
            if writer
                .as_ref()
                .is_some_and(|(w, _)| w.rows >= rows_per_shard)
            {
                let w = writer.take().expect("checked");
                finish(w, &previous, index, on_shard)?;
                index += 1;
                sum.shards += 1;
            }
        }
        if writer.is_none() {
            fs::create_dir_all(shard_dir)?;
            writer = Some((
                ZstdShardWriter::create(shard_path(shard_dir, index), &header, zstd_level)?,
                String::from_utf8_lossy(&cur).into_owned(),
            ));
        }
        writer.as_mut().expect("opened").0.write_row(&line)?;
        sum.rows += 1;
        sum.data_bytes += line.len() as u64;
    }
    if let Some(w) = writer.take() {
        finish(w, &cur, index, on_shard)?;
        sum.shards += 1;
    }
    // Every gzip member must have been consumed (a stray tail would be data loss).
    let mut inner = reader.into_inner().into_inner().into_inner();
    let position = inner.stream_position()?;
    ensure!(
        position >= size,
        "{}: gzip ended at byte {position} of {size}",
        source.display()
    );
    Ok(sum)
}

#[derive(Clone)]
pub struct BatchConfig {
    pub raw_root: PathBuf,
    pub shard_root: PathBuf,
    /// One RocksDB per UTC day under this root (`l2_db::day_dir`).
    pub db_root: PathBuf,
    pub workers: usize,
    pub rows_per_shard: u64,
    pub zstd_level: i32,
    /// Finished shards waiting to be washed never exceed this many compressed bytes.
    pub shard_budget_bytes: u64,
    /// Wash again although the product-day already has its completion marker.
    pub force: bool,
}

struct FileRun {
    product: String,
    day: String,
    gz_bytes: u64,
    dir: PathBuf,
    started: Option<Instant>,
    shard_secs: f64,
    shards_total: Option<u32>,
    shards_done: u32,
    shard_rows: u64,
    shard_bytes: u64,
    rics: Vec<WashedRic>,
    finished: bool,
}

#[derive(Default)]
struct State {
    pending: VecDeque<usize>,
    ready: VecDeque<(usize, ShardInfo)>,
    files: Vec<FileRun>,
    queued_bytes: u64,
    peak_queued_bytes: u64,
    busy: usize,
    failed: Option<String>,
    results: Vec<Value>,
}

type Dbs = Mutex<HashMap<String, Arc<DB>>>;

struct Ctx<'a> {
    cfg: &'a BatchConfig,
    state: Mutex<State>,
    cv: Condvar,
    dbs: Dbs,
}

#[derive(Debug, Default)]
pub struct BatchReport {
    pub results: Vec<Value>,
    pub failed: Option<String>,
    pub peak_queued_bytes: u64,
    /// `rocksdb.total-sst-files-size` of every day database touched, by day.
    pub db_sst_bytes: BTreeMap<String, u64>,
}

fn day_db(ctx: &Ctx, day: &str) -> Result<Arc<DB>> {
    let mut dbs = ctx.dbs.lock().expect("dbs lock");
    if let Some(db) = dbs.get(day) {
        return Ok(Arc::clone(db));
    }
    let db = Arc::new(l2_db::open_write(&l2_db::day_dir(&ctx.cfg.db_root, day))?);
    dbs.insert(day.to_string(), Arc::clone(&db));
    Ok(db)
}

fn lock<'a, 'b>(ctx: &'a Ctx<'b>) -> std::sync::MutexGuard<'a, State> {
    ctx.state.lock().expect("batch state lock")
}

fn fail(ctx: &Ctx, msg: String) {
    let mut st = lock(ctx);
    st.failed.get_or_insert(msg);
    ctx.cv.notify_all();
}

fn finalize(ctx: &Ctx, st: &mut State, fi: usize) -> Result<()> {
    let f = &mut st.files[fi];
    if f.finished || f.shards_total != Some(f.shards_done) {
        return Ok(());
    }
    f.finished = true;
    f.rics.sort_by(|a, b| a.ric.cmp(&b.ric));
    let db = day_db(ctx, &f.day)?;
    l2_db::write_marker(&db, &f.product, &f.day, &f.rics)?;
    let _ = fs::remove_dir(&f.dir); // empty by now; a leftover would show up as an error elsewhere
    let _ = fs::remove_dir(f.dir.parent().unwrap_or(&f.dir)); // product dir, only if empty
    let sum = |g: fn(&WashedRic) -> u64| f.rics.iter().map(g).sum::<u64>();
    let r = |x: f64| (x * 100.0).round() / 100.0;
    let v = json!({
        "product": f.product, "day": f.day, "status": "washed",
        "rics": f.rics.len(), "updates": sum(|x| x.updates),
        "key_bytes": sum(|x| x.key_bytes), "value_bytes": sum(|x| x.value_bytes),
        "input_gz_bytes": f.gz_bytes,
        "seconds": r(f.started.map_or(0.0, |t| t.elapsed().as_secs_f64())),
        "shard_seconds": r(f.shard_secs),
        "shards": f.shards_total, "shard_rows": f.shard_rows, "shard_zstd_bytes": f.shard_bytes,
    });
    println!("{v}");
    st.results.push(v);
    Ok(())
}

fn wash_one(ctx: &Ctx, fi: usize, shard: ShardInfo) -> Result<()> {
    let (day, product) = {
        let st = lock(ctx);
        (st.files[fi].day.clone(), st.files[fi].product.clone())
    };
    let day_start = day_number(&day)? * DAY_NS;
    let db = day_db(ctx, &day)?;
    let tmp = l2_db::day_dir(&ctx.cfg.db_root, &day).join("ingest_tmp");
    let rics = l2_db::wash_shard(&shard.path, day_start, &db, &tmp)
        .with_context(|| format!("{product} {day} shard {}", shard.index))?;
    fs::remove_file(&shard.path).with_context(|| format!("remove {}", shard.path.display()))?;
    let mut st = lock(ctx);
    st.queued_bytes -= shard.compressed_bytes;
    let f = &mut st.files[fi];
    f.rics.extend(rics);
    f.shards_done += 1;
    finalize(ctx, &mut st, fi)?;
    ctx.cv.notify_all();
    Ok(())
}

fn shard_one(ctx: &Ctx, fi: usize) -> Result<()> {
    let cfg = ctx.cfg;
    let (path, dir) = {
        let mut st = lock(ctx);
        let f = &mut st.files[fi];
        f.started = Some(Instant::now());
        (
            cfg.raw_root
                .join(&f.product)
                .join(format!("{}_{}_normll2.csv.gz", f.product, f.day)),
            f.dir.clone(),
        )
    };
    clean_dir(&dir)?;
    let t0 = Instant::now();
    let summary = shard_source(
        &path,
        &dir,
        cfg.rows_per_shard,
        cfg.zstd_level,
        &mut |info| {
            {
                let mut st = lock(ctx);
                st.queued_bytes += info.compressed_bytes;
                st.peak_queued_bytes = st.peak_queued_bytes.max(st.queued_bytes);
                let f = &mut st.files[fi];
                f.shard_rows += info.rows;
                f.shard_bytes += info.compressed_bytes;
                st.ready.push_back((fi, info));
                ctx.cv.notify_all();
            }
            // Backpressure: over budget -> this worker washes instead of writing more.
            loop {
                let next = {
                    let mut st = lock(ctx);
                    if st.failed.is_some() {
                        bail!("another task failed");
                    }
                    if st.queued_bytes > cfg.shard_budget_bytes {
                        st.ready.pop_front()
                    } else {
                        None
                    }
                };
                match next {
                    Some((fj, sh)) => wash_one(ctx, fj, sh)?,
                    None => break,
                }
            }
            Ok(())
        },
    )
    .with_context(|| format!("shard {}", path.display()))?;
    let mut st = lock(ctx);
    let f = &mut st.files[fi];
    f.shard_secs = t0.elapsed().as_secs_f64();
    f.shards_total = Some(summary.shards);
    finalize(ctx, &mut st, fi)?;
    ctx.cv.notify_all();
    Ok(())
}

/// Removes leftover shard files of a crashed run from `dir` (only our own names).
fn clean_dir(dir: &Path) -> Result<()> {
    if let Ok(rd) = fs::read_dir(dir) {
        for e in rd.flatten() {
            let name = e.file_name().to_string_lossy().into_owned();
            if name.starts_with(SHARD_PREFIX) {
                fs::remove_file(e.path())?;
            }
        }
    }
    Ok(())
}

fn worker(ctx: &Ctx) {
    enum Task {
        Slice(usize, ShardInfo),
        Shard(usize),
    }
    loop {
        let task = {
            let mut st = lock(ctx);
            loop {
                if st.failed.is_some() {
                    break None;
                }
                if let Some((fi, sh)) = st.ready.pop_front() {
                    st.busy += 1;
                    break Some(Task::Slice(fi, sh));
                }
                if let Some(fi) = st.pending.pop_front() {
                    st.busy += 1;
                    break Some(Task::Shard(fi));
                }
                if st.busy == 0 {
                    break None;
                }
                st = ctx.cv.wait(st).expect("batch condvar");
            }
        };
        let Some(task) = task else {
            ctx.cv.notify_all();
            return;
        };
        let r = match task {
            Task::Slice(fi, sh) => wash_one(ctx, fi, sh),
            Task::Shard(fi) => shard_one(ctx, fi),
        };
        if let Err(e) = r {
            fail(ctx, format!("{e:#}"));
        }
        let mut st = lock(ctx);
        st.busy -= 1;
        ctx.cv.notify_all();
    }
}

/// Runs stages 1 and 2 for `jobs` (product, UTC day); product-days that already
/// have their completion marker are skipped (unless `force`), missing source
/// files are reported as `missing_source`.
pub fn run_batch(cfg: &BatchConfig, jobs: Vec<(String, String)>) -> Result<BatchReport> {
    ensure!(cfg.workers >= 1, "workers must be >= 1");
    let ctx_dbs: Dbs = Mutex::new(HashMap::new());
    let mut files = Vec::new();
    let mut results = Vec::new();
    for (product, day) in jobs {
        day_number(&day)?;
        let path = cfg
            .raw_root
            .join(&product)
            .join(format!("{product}_{day}_normll2.csv.gz"));
        let Ok(meta) = fs::metadata(&path) else {
            let v = json!({"product": product, "day": day, "status": "missing_source"});
            println!("{v}");
            results.push(v);
            continue;
        };
        if !cfg.force {
            let dir = l2_db::day_dir(&cfg.db_root, &day);
            if dir.join("CURRENT").exists() {
                let db = {
                    let mut dbs = ctx_dbs.lock().expect("dbs lock");
                    if !dbs.contains_key(&day) {
                        dbs.insert(day.clone(), Arc::new(l2_db::open_write(&dir)?));
                    }
                    Arc::clone(&dbs[&day])
                };
                if l2_db::read_marker(&db, &product)?.is_some() {
                    let v = json!({"product": product, "day": day, "status": "kept_existing"});
                    println!("{v}");
                    results.push(v);
                    continue;
                }
            }
        }
        files.push(FileRun {
            dir: cfg.shard_root.join(&product).join(&day),
            product,
            day,
            gz_bytes: meta.len(),
            started: None,
            shard_secs: 0.0,
            shards_total: None,
            shards_done: 0,
            shard_rows: 0,
            shard_bytes: 0,
            rics: Vec::new(),
            finished: false,
        });
    }
    // Largest source file first: the giant files start at once instead of forming the tail.
    let mut order: Vec<usize> = (0..files.len()).collect();
    order.sort_by(|&a, &b| {
        files[b].gz_bytes.cmp(&files[a].gz_bytes).then_with(|| {
            (&files[a].product, &files[a].day).cmp(&(&files[b].product, &files[b].day))
        })
    });
    let ctx = Ctx {
        cfg,
        state: Mutex::new(State {
            pending: order.into(),
            files,
            results,
            ..State::default()
        }),
        cv: Condvar::new(),
        dbs: ctx_dbs,
    };
    std::thread::scope(|scope| {
        for _ in 0..cfg.workers {
            scope.spawn(|| worker(&ctx));
        }
    });
    let mut st = ctx.state.into_inner().expect("batch state lock");
    // Nothing of ours may stay on disk, also after a failure.
    for f in &st.files {
        let _ = clean_dir(&f.dir);
        let _ = fs::remove_dir(&f.dir);
        let _ = fs::remove_dir(f.dir.parent().unwrap_or(&f.dir));
    }
    let _ = fs::remove_dir(&cfg.shard_root);
    let mut db_sst_bytes = BTreeMap::new();
    for (day, db) in ctx.dbs.into_inner().expect("dbs lock") {
        let bytes = db
            .property_int_value("rocksdb.total-sst-files-size")
            .ok()
            .flatten()
            .unwrap_or(0);
        db_sst_bytes.insert(day, bytes);
    }
    Ok(BatchReport {
        results: std::mem::take(&mut st.results),
        failed: st.failed.take(),
        peak_queued_bytes: st.peak_queued_bytes,
        db_sst_bytes,
    })
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::l2_extract::{run_extract, ExtractConfig};
    use crate::l2_slice::{
        expected_header, slice_stream, Layout, Params, ParquetSink, CSV_COLUMNS, DAY_NS,
    };
    use flate2::{write::GzEncoder, Compression};
    use std::io::Write;

    const DAY: &str = "2024-10-04";
    const DAY0: i64 = 20_000 * DAY_NS;
    const F: i64 = 250_000_000;

    fn ts_text(off_ns: i64) -> String {
        let t = DAY0 + off_ns;
        let dt = chrono::DateTime::from_timestamp(t.div_euclid(1_000_000_000), 0).unwrap();
        format!(
            "{}.{:09}Z",
            dt.format("%Y-%m-%dT%H:%M:%S"),
            t.rem_euclid(1_000_000_000)
        )
    }
    fn row(ric: &str, off: i64, bid: f64, size: u32, ask: f64) -> String {
        let mut f = vec![String::new(); CSV_COLUMNS];
        f[0] = ric.into();
        f[1] = "Market Price".into();
        f[2] = ts_text(off);
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
    /// 3 RICs, ties, a long silence, a deep level on some rows.
    fn rows() -> Vec<String> {
        let mut v = Vec::new();
        for k in 1..=40i64 {
            v.push(row(
                "RICA",
                k * F / 2,
                100.0 + (k % 5) as f64 * 0.25,
                1 + (k % 3) as u32,
                101.0,
            ));
        }
        v.push(row("RICA", 40 * F, 100.0, 1, 101.0));
        v.push(row("RICA", 40 * F, 100.5, 2, 101.0)); // equal time, later wins
        v.push(row("RICA", 40 * F + 1_800_000_000_000, 100.5, 2, 101.0)); // silence -> anchor
        for k in 1..=7i64 {
            v.push(row("RICB", k * F, 5.0 + k as f64, 3, 9.0));
        }
        v.push(row("RICC", 3 * F, 1.0, 1, 2.0));
        v
    }
    fn text(rows: &[String]) -> Vec<u8> {
        let mut s = expected_header();
        for r in rows {
            s.push('\n');
            s.push_str(r);
        }
        s.push('\n');
        s.into_bytes()
    }
    /// gzip with two members (the real files have many).
    fn gz(data: &[u8]) -> Vec<u8> {
        let mid = data.iter().position(|b| *b == b'\n').unwrap() + 1;
        let mid = mid + data[mid..].iter().position(|b| *b == b'\n').unwrap() + 1;
        let mut out = Vec::new();
        for part in [&data[..mid], &data[mid..]] {
            let mut e = GzEncoder::new(Vec::new(), Compression::fast());
            e.write_all(part).unwrap();
            out.extend(e.finish().unwrap());
        }
        out
    }
    fn setup(t: &Path, rows: &[String]) {
        let dir = t.join("raw/FIX");
        fs::create_dir_all(&dir).unwrap();
        fs::write(
            dir.join(format!("FIX_{DAY}_normll2.csv.gz")),
            gz(&text(rows)),
        )
        .unwrap();
    }
    fn cfg(t: &Path, workers: usize, rows_per_shard: u64, budget: u64) -> BatchConfig {
        BatchConfig {
            raw_root: t.join("raw"),
            shard_root: t.join("shards"),
            db_root: t.join("db"),
            workers,
            rows_per_shard,
            zstd_level: 1,
            shard_budget_bytes: budget,
            force: false,
        }
    }
    fn tree_digests(root: &Path) -> Vec<(String, String)> {
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

    #[test]
    fn shards_are_cut_only_at_ric_boundaries() {
        let t = tempfile::tempdir().unwrap();
        setup(t.path(), &rows());
        let src = t.path().join(format!("raw/FIX/FIX_{DAY}_normll2.csv.gz"));
        let mut shards = Vec::new();
        let sum = shard_source(&src, &t.path().join("sh"), 10, 1, &mut |s| {
            shards.push(s);
            Ok(())
        })
        .unwrap();
        // RICA has 43 rows (>= 10) so it is one shard, RICB 7 + RICC 1 = 8 stay together
        assert_eq!(sum.rows, 51);
        assert_eq!(sum.rics, 3);
        assert_eq!(shards.len(), 2, "{shards:?}");
        assert_eq!(
            (shards[0].first_ric.as_str(), shards[0].last_ric.as_str()),
            ("RICA", "RICA")
        );
        assert_eq!(
            (shards[1].first_ric.as_str(), shards[1].last_ric.as_str()),
            ("RICB", "RICC")
        );
        assert_eq!(shards[0].rows + shards[1].rows, 51);
        // one RIC per shard when the limit is 1 row
        let mut n = 0;
        shard_source(&src, &t.path().join("sh1"), 1, 1, &mut |_| {
            n += 1;
            Ok(())
        })
        .unwrap();
        assert_eq!(n, 3);
    }

    #[test]
    fn a_ric_in_two_blocks_is_an_error() {
        let t = tempfile::tempdir().unwrap();
        let mut r = rows();
        r.push(row("RICA", 90 * F, 1.0, 1, 2.0));
        setup(t.path(), &r);
        let src = t.path().join(format!("raw/FIX/FIX_{DAY}_normll2.csv.gz"));
        let e = shard_source(&src, &t.path().join("sh"), 10, 1, &mut |_| Ok(())).unwrap_err();
        assert!(format!("{e:#}").contains("more than one block"));
    }

    fn direct_slices(t: &Path, rows: &[String], params: Params) -> PathBuf {
        let out = t.join("direct");
        let layout = Layout {
            out_root: out.clone(),
            exchange_of: Arc::new(BTreeMap::new()),
            default_exchange: Some("fx".into()),
            day: DAY.into(),
            force: false,
        };
        let mut sink = ParquetSink::new(&layout, params, DAY0);
        slice_stream(std::io::Cursor::new(text(rows)), DAY0, &params, &mut sink).unwrap();
        out
    }

    #[test]
    fn washed_and_extracted_slices_equal_the_direct_slices_at_any_worker_count() {
        let t = tempfile::tempdir().unwrap();
        let r = rows();
        setup(t.path(), &r);
        let params = Params::new(250, 1800).unwrap();
        let reference = tree_digests(&direct_slices(t.path(), &r, params));
        assert_eq!(reference.len(), 3);
        for (workers, shard_rows, budget) in
            [(1usize, 1u64, 1u64), (2, 10, 1 << 30), (8, 1, 1 << 30)]
        {
            let c = cfg(t.path(), workers, shard_rows, budget);
            let _ = fs::remove_dir_all(&c.db_root);
            let rep = run_batch(
                &c,
                vec![("FIX".into(), DAY.into()), ("NOPE".into(), DAY.into())],
            )
            .unwrap();
            assert!(rep.failed.is_none(), "{:?}", rep.failed);
            assert!(rep
                .results
                .iter()
                .any(|v| v["status"] == "washed" && v["updates"] == 51));
            assert!(rep.results.iter().any(|v| v["status"] == "missing_source"));
            assert!(
                !c.shard_root.exists(),
                "no shard may remain (workers={workers})"
            );
            // wash again: nothing to do
            let again = run_batch(&c, vec![("FIX".into(), DAY.into())]).unwrap();
            assert!(again.results.iter().all(|v| v["status"] == "kept_existing"));
            let out = t.path().join(format!("out{workers}"));
            let ec = ExtractConfig {
                db_root: c.db_root.clone(),
                out_root: out.clone(),
                exchange_of: Arc::new(BTreeMap::new()),
                default_exchange: Some("fx".into()),
                params,
                force: false,
                workers: workers.min(4),
                cache_mb: 16,
            };
            let rep = run_extract(
                &ec,
                vec![("FIX".into(), DAY.into()), ("OTHER".into(), DAY.into())],
            )
            .unwrap();
            assert!(rep.failed.is_none(), "{:?}", rep.failed);
            assert!(rep.results.iter().any(|v| v["status"] == "not_washed"));
            assert_eq!(
                tree_digests(&out),
                reference,
                "workers={workers} shard_rows={shard_rows}"
            );
            // another frequency is just another extract from the same database
            let p1 = Params::new(1000, 1800).unwrap();
            let out1 = t.path().join(format!("out1s{workers}"));
            run_extract(
                &ExtractConfig {
                    out_root: out1.clone(),
                    params: p1,
                    ..ec.clone()
                },
                vec![("FIX".into(), DAY.into())],
            )
            .unwrap();
            let one_second = direct_slices(&t.path().join(format!("d{workers}")), &r, p1);
            assert_eq!(tree_digests(&out1), tree_digests(&one_second));
            fs::remove_dir_all(&out).unwrap();
            fs::remove_dir_all(&out1).unwrap();
            fs::remove_dir_all(t.path().join(format!("d{workers}"))).unwrap();
        }
    }

    #[test]
    fn interrupted_run_leftovers_are_cleaned_and_redone() {
        let t = tempfile::tempdir().unwrap();
        setup(t.path(), &rows());
        let c = cfg(t.path(), 2, 10, 1 << 30);
        // a stale shard of a crashed run and an unmarked product-day
        let stale = c.shard_root.join("FIX").join(DAY);
        fs::create_dir_all(&stale).unwrap();
        fs::write(stale.join("shard-00000.csv.zst"), b"garbage").unwrap();
        let rep = run_batch(&c, vec![("FIX".into(), DAY.into())]).unwrap();
        assert!(rep.failed.is_none(), "{:?}", rep.failed);
        assert!(!c.shard_root.exists());
    }
}
