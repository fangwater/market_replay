//! Merge several finished TAS RocksDBs (lseg-daily daily batches) into one
//! new database with the same column families, then compact it fully and
//! verify it read-only.
//!
//! Rules (docs/lseg/lseg_daily.md, "月度合并"):
//! - every product CF is a streaming k-way merge of the sources in key order;
//!   at most one key and one value per source are held at a time;
//! - a key present in two sources fails the merge, except Quote (kind 3):
//!   its key is a one-second bucket with zero part/seq by design, and a
//!   single replay of both days would have folded the two values with the
//!   `quote_last` operator, so they are folded the same way (counted);
//! - two sources with records of the same kind, RIC and clock (key bytes
//!   0..25) also fail: the engine breaks such ties by source order, which a
//!   merged database no longer has;
//! - `replay_meta` is the union of the sources' entries (conflicts fail) plus
//!   `period:<merged period>` = done and `merged_from`;
//! - verification: per CF and kind, sources read = audited counts, merged =
//!   sources − folded quotes; the merged key/value stream (read back from the
//!   compacted, read-only database) hashes to exactly what was written.
use crate::tasdb::{self, SharedCache, Tuning};
use anyhow::{bail, ensure, Context, Result};
use cme_tas_replay::product::{
    is_product_cf_name, later_quote_value, quote_last_merge, ALL_KEY_LEN,
};
use cme_tas_replay::{CF_REPLAY_META, KIND_CME_QUOTE};
use rocksdb::{
    BlockBasedOptions, BottommostLevelCompaction, Cache, ColumnFamilyDescriptor, CompactOptions,
    DBCompressionType, FlushOptions, IteratorMode, Options, ReadOptions, WriteBatch, WriteOptions,
    DB,
};
use serde::{Deserialize, Serialize};
use sha2::{Digest, Sha256};
use std::cmp::Reverse;
use std::collections::{BTreeMap, BTreeSet, BinaryHeap};
use std::fs;
use std::path::{Path, PathBuf};
use std::time::Instant;

/// Record counts by kind (key byte 0).
pub type Counts = BTreeMap<u8, u64>;

/// Bytes of the key that identify (kind, RIC, clock); the rest is part/seq.
const CLOCK_PREFIX: usize = 25;
/// Flush the write batch at this many bytes.
const BATCH_BYTES: usize = 8 << 20;

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct SourceSpec {
    pub id: String,
    pub db: PathBuf,
    /// Product CFs of this source (must be all of its live product CFs).
    pub cfs: Vec<String>,
    /// Expected `CURRENT` content (identity of the version being merged).
    #[serde(default)]
    pub current: Option<String>,
    /// Expected per-CF counts by kind (from the source's audit / merge report).
    #[serde(default)]
    pub expected: Option<BTreeMap<String, Counts>>,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct Spec {
    pub target: PathBuf,
    /// `<first day>_<day after the last day>` of the merged coverage.
    pub period: String,
    /// Source order (lseg-daily plan order); folded quotes apply in this order.
    pub sources: Vec<SourceSpec>,
    /// RocksDB background jobs for flush + manual compaction (fixed, 1..4).
    #[serde(default = "two")]
    pub compaction_threads: i32,
}
fn two() -> i32 {
    2
}

#[derive(Clone, Debug, Default, Serialize, Deserialize, PartialEq)]
pub struct CfReport {
    /// Read from the sources (sum).
    pub sources: Counts,
    /// Per source (spec order).
    pub per_source: Vec<Counts>,
    pub written: Counts,
    /// Read back from the compacted read-only merged database.
    pub merged: Counts,
    pub quote_folded: u64,
    pub written_sha256: String,
    pub merged_sha256: String,
}

#[derive(Clone, Debug, Default, Serialize, Deserialize)]
pub struct DirStats {
    pub sst_files: u64,
    pub sst_bytes: u64,
    pub bytes: u64,
}

#[derive(Clone, Debug, Default, Serialize, Deserialize)]
pub struct Report {
    pub status: String,
    pub target: PathBuf,
    pub period: String,
    pub sources: Vec<SourceSpec>,
    pub source_dirs: Vec<DirStats>,
    pub target_dir: DirStats,
    pub cfs: BTreeMap<String, CfReport>,
    pub replay_meta: BTreeMap<String, String>,
    pub seconds_copy: f64,
    pub seconds_compact: f64,
    pub seconds_verify: f64,
}

/// A sorted key/value stream (a RocksDB raw iterator, or a vector in tests).
pub trait Stream {
    fn key(&self) -> Option<&[u8]>;
    fn value(&self) -> Option<&[u8]>;
    fn advance(&mut self);
    fn status(&self) -> Result<()>;
}

impl Stream for rocksdb::DBRawIteratorWithThreadMode<'_, DB> {
    fn key(&self) -> Option<&[u8]> {
        rocksdb::DBRawIteratorWithThreadMode::key(self)
    }
    fn value(&self) -> Option<&[u8]> {
        rocksdb::DBRawIteratorWithThreadMode::value(self)
    }
    fn advance(&mut self) {
        self.next()
    }
    fn status(&self) -> Result<()> {
        Ok(rocksdb::DBRawIteratorWithThreadMode::status(self)?)
    }
}

/// Order-sensitive digest of a key/value stream.
#[derive(Default)]
pub struct StreamHash(Sha256);
impl StreamHash {
    pub fn add(&mut self, key: &[u8], value: &[u8]) {
        self.0.update((key.len() as u32).to_le_bytes());
        self.0.update(key);
        self.0.update((value.len() as u32).to_le_bytes());
        self.0.update(value);
    }
    pub fn hex(self) -> String {
        format!("{:x}", self.0.finalize())
    }
}

fn kind_of(product: bool, key: &[u8]) -> u8 {
    if product {
        key[0]
    } else {
        0
    }
}

fn check_record(key: &[u8], value: &[u8]) -> Result<()> {
    ensure!(
        key.len() == ALL_KEY_LEN && value.len() >= 28,
        "malformed TAS key/value"
    );
    ensure!(
        value[3] == key[0] && key[1..17] == value[4..20],
        "key/value identity mismatch"
    );
    Ok(())
}

fn hex(b: &[u8]) -> String {
    b.iter().map(|x| format!("{x:02x}")).collect()
}

/// Streaming k-way merge of sorted `streams` into `emit` (key order). For a
/// product CF (`product`), enforces the collision rules of this module; for a
/// non-product CF (`default`), equal keys must carry equal values.
pub fn merge_streams<S: Stream>(
    cf: &str,
    product: bool,
    streams: &mut [S],
    report: &mut CfReport,
    mut emit: impl FnMut(&[u8], &[u8]) -> Result<()>,
) -> Result<()> {
    report.per_source = vec![Counts::new(); streams.len()];
    let mut heap = BinaryHeap::new();
    for (i, s) in streams.iter().enumerate() {
        if let Some(k) = s.key() {
            heap.push(Reverse((k.to_vec(), i)));
        }
        s.status()?;
    }
    let mut hash = StreamHash::default();
    let mut write = |key: &[u8], value: &[u8], report: &mut CfReport| -> Result<()> {
        *report.written.entry(kind_of(product, key)).or_default() += 1;
        hash.add(key, value);
        emit(key, value)
    };
    // The record waiting to be written (a folded quote may still change it).
    let mut pending: Option<(Vec<u8>, Vec<u8>, usize)> = None;
    while let Some(Reverse((key, i))) = heap.pop() {
        let value = streams[i]
            .value()
            .context("stream value at a valid key")?
            .to_vec();
        if product {
            check_record(&key, &value).with_context(|| format!("{cf} key {}", hex(&key)))?;
        }
        let kind = kind_of(product, &key);
        *report.sources.entry(kind).or_default() += 1;
        *report.per_source[i].entry(kind).or_default() += 1;
        streams[i].advance();
        if let Some(k) = streams[i].key() {
            ensure!(
                k > key.as_slice(),
                "{cf}: source {i} is not strictly sorted"
            );
            heap.push(Reverse((k.to_vec(), i)));
        }
        streams[i].status()?;
        match pending.take() {
            Some((pk, mut pv, pi)) if pk == key => {
                if !product {
                    ensure!(
                        pv == value,
                        "{cf}: key {} differs between sources {pi} and {i}",
                        hex(&key)
                    );
                } else if kind == KIND_CME_QUOTE {
                    pv = later_quote_value(&pv, &value);
                    report.quote_folded += 1;
                } else {
                    bail!(
                        "{cf}: key collision kind={kind} key={} in sources {pi} and {i}",
                        hex(&key)
                    );
                }
                pending = Some((pk, pv, i));
            }
            Some((pk, pv, pi)) => {
                if product
                    && kind != KIND_CME_QUOTE
                    && pi != i
                    && pk[..CLOCK_PREFIX] == key[..CLOCK_PREFIX]
                {
                    bail!(
                        "{cf}: cross-source clock tie kind={kind} key={} in sources {pi} and {i}",
                        hex(&key)
                    );
                }
                write(&pk, &pv, report)?;
                pending = Some((key, value, i));
            }
            None => pending = Some((key, value, i)),
        }
    }
    if let Some((k, v, _)) = pending {
        write(&k, &v, report)?;
    }
    report.written_sha256 = hash.hex();
    Ok(())
}

/// Count check of one CF: per source against its expected counts, and
/// merged = written = sources − folded quotes, with identical digests.
pub fn check_counts(cf: &str, r: &CfReport, expected: &[Option<Counts>]) -> Result<()> {
    ensure!(
        expected.len() == r.per_source.len(),
        "{cf}: source count mismatch"
    );
    for (i, (got, want)) in r.per_source.iter().zip(expected).enumerate() {
        if let Some(want) = want {
            let want: Counts = want
                .iter()
                .filter(|(_, n)| **n > 0)
                .map(|(k, n)| (*k, *n))
                .collect();
            ensure!(
                *got == want,
                "{cf}: source {i} counts {got:?} differ from its audit {want:?}"
            );
        }
    }
    let mut want = r.sources.clone();
    if r.quote_folded > 0 {
        let q = want.entry(KIND_CME_QUOTE).or_default();
        ensure!(*q >= r.quote_folded, "{cf}: more folded quotes than quotes");
        *q -= r.quote_folded;
    }
    want.retain(|_, n| *n > 0);
    ensure!(
        r.written == want,
        "{cf}: written {:?} != sources − folded {want:?}",
        r.written
    );
    ensure!(
        r.merged == r.written,
        "{cf}: merged {:?} != written {:?}",
        r.merged,
        r.written
    );
    ensure!(
        r.merged_sha256 == r.written_sha256,
        "{cf}: merged key/value stream differs from what was written"
    );
    Ok(())
}

/// Merges `meta` maps; the same key with different values fails.
pub fn union_meta(
    maps: &[BTreeMap<String, String>],
    extra: &[(String, String)],
) -> Result<BTreeMap<String, String>> {
    let mut out = BTreeMap::new();
    for (k, v) in maps
        .iter()
        .flatten()
        .chain(extra.iter().map(|(k, v)| (k, v)))
    {
        if let Some(old) = out.insert(k.clone(), v.clone()) {
            ensure!(old == *v, "replay_meta conflict for {k}: {old} vs {v}");
        }
    }
    for (k, v) in &out {
        if k.starts_with("period:") {
            ensure!(v == "done", "replay_meta {k} is {v}, not done");
        }
    }
    Ok(out)
}

pub fn dir_stats(path: &Path) -> Result<DirStats> {
    let mut s = DirStats::default();
    for e in fs::read_dir(path).with_context(|| format!("list {}", path.display()))? {
        let e = e?;
        let m = e.metadata()?;
        if !m.is_file() {
            continue;
        }
        s.bytes += m.len();
        if e.file_name().to_string_lossy().ends_with(".sst") {
            s.sst_files += 1;
            s.sst_bytes += m.len();
        }
    }
    Ok(s)
}

fn target_options(cache: &Cache, threads: i32) -> Options {
    let mut o = Options::default();
    o.create_if_missing(true);
    o.create_missing_column_families(true);
    o.set_compression_type(DBCompressionType::Lz4);
    o.set_write_buffer_size(64 << 20);
    o.set_max_write_buffer_number(2);
    o.set_merge_operator_associative("quote_last", quote_last_merge);
    // Bulk load: no automatic compaction; one full manual compaction at the end.
    o.set_disable_auto_compactions(true);
    o.set_level_zero_slowdown_writes_trigger(1 << 20);
    o.set_level_zero_stop_writes_trigger(1 << 20);
    o.set_max_background_jobs(threads);
    o.set_max_subcompactions(1);
    o.set_max_open_files(256);
    o.set_max_file_opening_threads(1);
    let mut table = BlockBasedOptions::default();
    table.set_block_cache(cache);
    table.set_cache_index_and_filter_blocks(true);
    o.set_block_based_table_factory(&table);
    o
}

fn read_meta(db: &DB) -> Result<BTreeMap<String, String>> {
    let cf = db.cf_handle(CF_REPLAY_META).context("replay_meta")?;
    let mut out = BTreeMap::new();
    for pair in db.iterator_cf(cf, IteratorMode::Start) {
        let (k, v) = pair?;
        out.insert(
            String::from_utf8(k.to_vec())?,
            String::from_utf8(v.to_vec())?,
        );
    }
    Ok(out)
}

fn scan_options() -> ReadOptions {
    let mut ro = ReadOptions::default();
    ro.fill_cache(false);
    ro.set_verify_checksums(true);
    ro.set_readahead_size(2 << 20);
    ro
}

/// Runs a merge spec end to end; the report is returned on success only.
pub fn merge(spec: &Spec) -> Result<Report> {
    ensure!(
        (1..=4).contains(&spec.compaction_threads),
        "compaction_threads must be 1..4"
    );
    ensure!(!spec.sources.is_empty(), "no sources");
    ensure!(
        !spec.target.exists(),
        "target {} already exists (every merge writes a fresh attempt)",
        spec.target.display()
    );
    let mut product_cfs = BTreeSet::new();
    let mut seen = BTreeSet::new();
    let mut currents = Vec::new();
    for s in &spec.sources {
        ensure!(
            seen.insert(fs::canonicalize(&s.db)?),
            "source {} listed twice",
            s.id
        );
        ensure!(
            !spec.target.starts_with(&s.db) && !s.db.starts_with(&spec.target),
            "target overlaps source {}",
            s.id
        );
        let current = fs::read_to_string(s.db.join("CURRENT"))
            .with_context(|| format!("source {} CURRENT", s.id))?;
        if let Some(want) = &s.current {
            ensure!(*want == current, "source {} changed (CURRENT)", s.id);
        }
        let live: BTreeSet<String> = tasdb::list_cfs(&s.db)?.into_iter().collect();
        let want: BTreeSet<String> = s.cfs.iter().cloned().collect();
        for n in &live {
            ensure!(
                n == "default" || n == CF_REPLAY_META || is_product_cf_name(n),
                "source {} has an unexpected CF {n}",
                s.id
            );
        }
        let live_products: BTreeSet<String> = live
            .iter()
            .filter(|n| is_product_cf_name(n))
            .cloned()
            .collect();
        ensure!(
            live_products == want && live.contains(CF_REPLAY_META),
            "source {} CFs {live:?} differ from the spec {want:?}",
            s.id
        );
        product_cfs.extend(want);
        currents.push(current);
    }
    let cache = SharedCache::new(Tuning {
        cache_bytes: 64 << 20,
        max_open_files: 64,
        cache_index_and_filter_blocks: true,
    });
    let sources = spec
        .sources
        .iter()
        .map(|s| {
            cache.open(
                &s.db,
                ["default", CF_REPLAY_META]
                    .into_iter()
                    .map(str::to_owned)
                    .chain(s.cfs.iter().cloned()),
            )
        })
        .collect::<Result<Vec<_>>>()?;
    let metas = sources.iter().map(read_meta).collect::<Result<Vec<_>>>()?;
    let merged_from = serde_json::to_string(
        &spec
            .sources
            .iter()
            .zip(&currents)
            .map(|(s, c)| serde_json::json!({"id": s.id, "db": s.db, "current": c.trim_end()}))
            .collect::<Vec<_>>(),
    )?;
    let meta = union_meta(
        &metas
            .iter()
            .map(|m| {
                m.iter()
                    .filter(|(k, _)| k.as_str() != "merged_from")
                    .map(|(k, v)| (k.clone(), v.clone()))
                    .collect()
            })
            .collect::<Vec<_>>(),
        &[
            (format!("period:{}", spec.period), "done".into()),
            ("merged_from".into(), merged_from),
        ],
    )?;

    fs::create_dir_all(spec.target.parent().context("target parent")?)?;
    let tcache = Cache::new_lru_cache(32 << 20);
    let opts = || target_options(&tcache, spec.compaction_threads);
    let all_cfs: Vec<String> = ["default".to_string(), CF_REPLAY_META.to_string()]
        .into_iter()
        .chain(product_cfs.iter().cloned())
        .collect();
    let target = DB::open_cf_descriptors(
        &opts(),
        &spec.target,
        all_cfs
            .iter()
            .map(|n| ColumnFamilyDescriptor::new(n, opts())),
    )
    .with_context(|| format!("create {}", spec.target.display()))?;
    let mut wo = WriteOptions::default();
    wo.disable_wal(true);

    let started = Instant::now();
    let mut report = Report {
        status: "copying".into(),
        target: spec.target.clone(),
        period: spec.period.clone(),
        sources: spec.sources.clone(),
        ..Report::default()
    };
    for name in std::iter::once("default").chain(product_cfs.iter().map(String::as_str)) {
        let product = name != "default";
        let members: Vec<usize> = (0..sources.len())
            .filter(|&i| !product || spec.sources[i].cfs.iter().any(|c| c == name))
            .collect();
        let mut streams = members
            .iter()
            .map(|&i| {
                let db = &sources[i];
                let mut it = db.raw_iterator_cf_opt(db.cf_handle(name).unwrap(), scan_options());
                it.seek_to_first();
                it
            })
            .collect::<Vec<_>>();
        let tcf = target.cf_handle(name).unwrap();
        let mut batch = WriteBatch::default();
        let mut r = CfReport::default();
        merge_streams(name, product, &mut streams, &mut r, |k, v| {
            batch.put_cf(&tcf, k, v);
            if batch.size_in_bytes() >= BATCH_BYTES {
                target.write_opt(std::mem::take(&mut batch), &wo)?;
            }
            Ok(())
        })?;
        target.write_opt(batch, &wo)?;
        drop(streams);
        // Per-source counts are reported against the full source list.
        let mut per = vec![Counts::new(); sources.len()];
        for (j, &i) in members.iter().enumerate() {
            per[i] = r.per_source[j].clone();
        }
        r.per_source = per;
        report.cfs.insert(name.to_string(), r);
    }
    {
        let mcf = target.cf_handle(CF_REPLAY_META).unwrap();
        let mut batch = WriteBatch::default();
        for (k, v) in &meta {
            batch.put_cf(&mcf, k, v);
        }
        target.write_opt(batch, &wo)?;
    }
    for (s, c) in spec.sources.iter().zip(&currents) {
        ensure!(
            fs::read_to_string(s.db.join("CURRENT"))? == *c,
            "source {} changed during the merge",
            s.id
        );
    }
    report.source_dirs = spec
        .sources
        .iter()
        .map(|s| dir_stats(&s.db))
        .collect::<Result<_>>()?;
    drop(sources);
    report.seconds_copy = started.elapsed().as_secs_f64();

    let t = Instant::now();
    let mut fo = FlushOptions::default();
    fo.set_wait(true);
    let handles: Vec<_> = all_cfs
        .iter()
        .map(|n| target.cf_handle(n).unwrap())
        .collect();
    target.flush_cfs_opt(&handles.iter().collect::<Vec<_>>(), &fo)?;
    let mut co = CompactOptions::default();
    co.set_exclusive_manual_compaction(true);
    co.set_bottommost_level_compaction(BottommostLevelCompaction::Force);
    for h in &handles {
        target.compact_range_cf_opt(h, None::<&[u8]>, None::<&[u8]>, &co);
    }
    drop(handles);
    drop(target);
    report.seconds_compact = t.elapsed().as_secs_f64();

    let t = Instant::now();
    verify_target(spec, &meta, &mut report)?;
    report.target_dir = dir_stats(&spec.target)?;
    report.seconds_verify = t.elapsed().as_secs_f64();
    for (name, r) in &report.cfs {
        let expected: Vec<Option<Counts>> = spec
            .sources
            .iter()
            .map(|s| {
                if name == "default" || !s.cfs.contains(name) {
                    return Some(Counts::new());
                }
                s.expected
                    .as_ref()
                    .map(|e| e.get(name).cloned().unwrap_or_default())
            })
            .collect();
        if name == "default" {
            // `default` carries no TAS records; equal keys may repeat.
            ensure!(r.merged == r.written, "default CF readback differs");
            ensure!(
                r.merged_sha256 == r.written_sha256,
                "default CF digest differs"
            );
            continue;
        }
        check_counts(name, r, &expected)?;
    }
    report.replay_meta = meta;
    report.status = "passed".into();
    Ok(report)
}

/// Read-only readback of the compacted target: counts, digest, strict order.
fn verify_target(spec: &Spec, meta: &BTreeMap<String, String>, report: &mut Report) -> Result<()> {
    let names = tasdb::list_cfs(&spec.target)?;
    let want: BTreeSet<&str> = report
        .cfs
        .keys()
        .map(String::as_str)
        .chain([CF_REPLAY_META])
        .collect();
    ensure!(
        names.iter().map(String::as_str).collect::<BTreeSet<_>>() == want,
        "merged CFs {names:?} differ"
    );
    let db = tasdb::open_read_only(
        &spec.target,
        &names,
        Tuning {
            cache_bytes: 64 << 20,
            max_open_files: 256,
            cache_index_and_filter_blocks: true,
        },
    )?;
    ensure!(read_meta(&db)? == *meta, "merged replay_meta differs");
    for (name, r) in report.cfs.iter_mut() {
        let product = name != "default";
        let mut it = db.raw_iterator_cf_opt(db.cf_handle(name).unwrap(), scan_options());
        it.seek_to_first();
        let mut hash = StreamHash::default();
        let mut last: Option<Vec<u8>> = None;
        r.merged.clear();
        while let (Some(k), Some(v)) = (it.key(), it.value()) {
            if product {
                check_record(k, v)?;
            }
            if let Some(l) = &last {
                ensure!(
                    k > l.as_slice(),
                    "{name}: merged keys not strictly increasing"
                );
            }
            *r.merged.entry(kind_of(product, k)).or_default() += 1;
            hash.add(k, v);
            last = Some(k.to_vec());
            it.next();
        }
        it.status()?;
        r.merged_sha256 = hash.hex();
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use cme_tas_replay::product::encode_all_key;

    struct VecStream(Vec<(Vec<u8>, Vec<u8>)>, usize);
    impl Stream for VecStream {
        fn key(&self) -> Option<&[u8]> {
            self.0.get(self.1).map(|r| r.0.as_slice())
        }
        fn value(&self) -> Option<&[u8]> {
            self.0.get(self.1).map(|r| r.1.as_slice())
        }
        fn advance(&mut self) {
            self.1 += 1
        }
        fn status(&self) -> Result<()> {
            Ok(())
        }
    }

    fn rec(kind: u8, ric: &str, ts: u64, part: u16, seq: u32, tag: u8) -> (Vec<u8>, Vec<u8>) {
        let key = encode_all_key(kind, ric, ts, part, seq).unwrap().to_vec();
        let mut value = vec![0u8; 28];
        value[3] = kind;
        value[4..20].copy_from_slice(&key[1..17]);
        value[20..28].copy_from_slice(&ts.to_le_bytes());
        value.push(tag);
        (key, value)
    }

    fn run(streams: Vec<Vec<(Vec<u8>, Vec<u8>)>>) -> Result<(CfReport, Vec<Vec<u8>>)> {
        let mut s: Vec<_> = streams.into_iter().map(|v| VecStream(v, 0)).collect();
        let mut r = CfReport::default();
        let mut keys = Vec::new();
        merge_streams("p:2026:ES", true, &mut s, &mut r, |k, _| {
            keys.push(k.to_vec());
            Ok(())
        })?;
        Ok((r, keys))
    }

    #[test]
    fn interleaved_sources_merge_in_key_order() {
        let a = vec![
            rec(1, "ESZ6", 10, 0, 0, 1),
            rec(1, "ESZ6", 30, 0, 0, 1),
            rec(13, "ESZ6", 10, 0, 0, 1),
        ];
        let b = vec![rec(1, "ESZ6", 20, 0, 0, 2), rec(3, "ESZ6", 20, 0, 0, 2)];
        let (r, keys) = run(vec![a, b]).unwrap();
        assert!(keys.windows(2).all(|w| w[0] < w[1]));
        assert_eq!(r.sources, Counts::from([(1, 3), (3, 1), (13, 1)]));
        assert_eq!(r.written, r.sources);
        assert_eq!(r.per_source[0], Counts::from([(1, 2), (13, 1)]));
        assert_eq!(r.quote_folded, 0);
    }

    #[test]
    fn duplicate_trade_key_fails() {
        let a = vec![rec(1, "ESZ6", 10, 0, 7, 1)];
        let b = vec![rec(1, "ESZ6", 10, 0, 7, 2)];
        let e = run(vec![a, b]).unwrap_err().to_string();
        assert!(e.contains("key collision kind=1"), "{e}");
        // Status and corrections too.
        for kind in [12, 13] {
            let e = run(vec![
                vec![rec(kind, "ESZ6", 5, 1, 1, 1)],
                vec![rec(kind, "ESZ6", 5, 1, 1, 1)],
            ])
            .unwrap_err()
            .to_string();
            assert!(e.contains("key collision"), "{e}");
        }
    }

    #[test]
    fn cross_source_clock_tie_fails_but_same_source_tie_is_fine() {
        // Same clock, different part/seq, different sources.
        let e = run(vec![
            vec![rec(1, "ESZ6", 10, 0, 1, 1)],
            vec![rec(1, "ESZ6", 10, 3, 0, 2)],
        ])
        .unwrap_err()
        .to_string();
        assert!(e.contains("cross-source clock tie"), "{e}");
        // Within one source the order key breaks the tie as before.
        let (r, _) = run(vec![
            vec![rec(1, "ESZ6", 10, 0, 1, 1), rec(1, "ESZ6", 10, 0, 2, 1)],
            vec![rec(1, "ESZ6", 11, 0, 0, 2)],
        ])
        .unwrap();
        assert_eq!(r.written[&1], 3);
        // Different RICs at the same clock are not a tie.
        run(vec![
            vec![rec(1, "ESZ6", 10, 0, 0, 1)],
            vec![rec(1, "ESH7", 10, 0, 0, 2)],
        ])
        .unwrap();
    }

    #[test]
    fn quote_bucket_collision_is_folded_and_counted() {
        let a = vec![rec(3, "ESZ6", 1_000_000_000, 0, 0, 1)];
        let b = vec![
            rec(3, "ESZ6", 1_000_000_000, 0, 0, 2),
            rec(3, "ESZ6", 2_000_000_000, 0, 0, 2),
        ];
        let (r, keys) = run(vec![a, b]).unwrap();
        assert_eq!(r.quote_folded, 1);
        assert_eq!(r.sources[&3], 3);
        assert_eq!(r.written[&3], 2);
        assert_eq!(keys.len(), 2);
        let mut ok = r.clone();
        ok.merged = ok.written.clone();
        ok.merged_sha256 = ok.written_sha256.clone();
        check_counts("cf", &ok, &[None, None]).unwrap();
    }

    #[test]
    fn count_check_catches_every_mismatch() {
        let a = vec![rec(1, "ESZ6", 10, 0, 0, 1), rec(13, "ESZ6", 10, 0, 0, 1)];
        let b = vec![rec(1, "ESZ6", 20, 0, 0, 2)];
        let (mut r, _) = run(vec![a, b]).unwrap();
        r.merged = r.written.clone();
        r.merged_sha256 = r.written_sha256.clone();
        let audit = |t, s| Some(Counts::from([(1, t), (13, s), (3, 0)]));
        check_counts("cf", &r, &[audit(1, 1), Some(Counts::from([(1, 1)]))]).unwrap();
        // An audit that saw more rows than the stream read.
        assert!(check_counts("cf", &r, &[audit(2, 1), None]).is_err());
        // A lost record in the merged readback.
        let mut lost = r.clone();
        *lost.merged.get_mut(&1).unwrap() -= 1;
        assert!(check_counts("cf", &lost, &[None, None]).is_err());
        // Same counts but different content.
        let mut changed = r.clone();
        changed.merged_sha256 = "0".into();
        assert!(check_counts("cf", &changed, &[None, None]).is_err());
    }

    #[test]
    fn meta_union_rejects_conflicts_and_unfinished_periods() {
        let m = |k: &str, v: &str| BTreeMap::from([(k.to_string(), v.to_string())]);
        let u = union_meta(
            &[m("period:a", "done"), m("period:b", "done")],
            &[("period:c".into(), "done".into())],
        )
        .unwrap();
        assert_eq!(u.len(), 3);
        assert!(union_meta(&[m("period:a", "done"), m("period:a", "writing")], &[]).is_err());
        assert!(union_meta(&[m("period:a", "writing")], &[]).is_err());
    }

    fn fixture(path: &Path, cf: &str, recs: &[(Vec<u8>, Vec<u8>)], period: &str) {
        let mut o = Options::default();
        o.create_if_missing(true);
        o.create_missing_column_families(true);
        o.set_merge_operator_associative("quote_last", quote_last_merge);
        let db = DB::open_cf(&o, path, ["default", CF_REPLAY_META, cf]).unwrap();
        for (k, v) in recs {
            db.put_cf(db.cf_handle(cf).unwrap(), k, v).unwrap();
        }
        db.put_cf(
            db.cf_handle(CF_REPLAY_META).unwrap(),
            format!("period:{period}"),
            "done",
        )
        .unwrap();
        db.flush_cf(db.cf_handle(cf).unwrap()).unwrap();
    }

    #[test]
    fn end_to_end_merge_and_readback() {
        let dir = tempfile::tempdir().unwrap();
        let a = dir.path().join("a");
        let b = dir.path().join("b");
        let cf = "p:2026:ES";
        fixture(
            &a,
            cf,
            &[rec(1, "ESZ6", 10, 0, 0, 1), rec(13, "ESZ6", 11, 0, 0, 1)],
            "2026-10-01_2026-10-02",
        );
        fixture(
            &b,
            cf,
            &[
                rec(1, "ESZ6", 20, 0, 0, 2),
                rec(3, "ESZ6", 2_000_000_000, 0, 0, 2),
            ],
            "2026-10-02_2026-10-03",
        );
        let src = |id: &str, db: &Path, n: Counts| SourceSpec {
            id: id.into(),
            db: db.into(),
            cfs: vec![cf.into()],
            current: None,
            expected: Some(BTreeMap::from([(cf.to_string(), n)])),
        };
        let spec = Spec {
            target: dir.path().join("m/attempt_1"),
            period: "2026-10-01_2026-10-03".into(),
            sources: vec![
                src("a", &a, Counts::from([(1, 1), (13, 1)])),
                src("b", &b, Counts::from([(1, 1), (3, 1)])),
            ],
            compaction_threads: 1,
        };
        let r = merge(&spec).unwrap();
        assert_eq!(r.status, "passed");
        assert_eq!(r.cfs[cf].merged, Counts::from([(1, 2), (3, 1), (13, 1)]));
        assert_eq!(r.replay_meta["period:2026-10-01_2026-10-03"], "done");
        assert_eq!(r.replay_meta["period:2026-10-02_2026-10-03"], "done");
        assert!(r.replay_meta["merged_from"].contains("\"a\""));
        // Fresh targets only.
        assert!(merge(&spec).is_err());
        // A wrong audit count fails the merge.
        let mut bad = spec.clone();
        bad.target = dir.path().join("m/attempt_2");
        bad.sources[0].expected = Some(BTreeMap::from([(cf.to_string(), Counts::from([(1, 2)]))]));
        assert!(merge(&bad).is_err());
        // An omitted CF of a source fails.
        let mut bad = spec.clone();
        bad.target = dir.path().join("m/attempt_3");
        bad.sources[0].cfs = vec![];
        assert!(merge(&bad).is_err());
    }
}
