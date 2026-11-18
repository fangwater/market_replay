//! Shared helpers over the TAS RocksDB layout owned by `cme_tas_replay`
//! (record codec, keys, `quote_last` merge) and small file utilities used
//! by every LSEG delivery tool.

use anyhow::{ensure, Context, Result};
use cme_tas_replay::product::quote_last_merge;
use rocksdb::{BlockBasedOptions, Cache, ColumnFamilyDescriptor, Options, DB};
use serde::Serialize;
use sha2::{Digest, Sha256};
use std::fs::{self, File};
use std::path::Path;

pub const DAY_NS: u64 = 86_400_000_000_000;

/// Read-only open knobs. They only affect speed and memory, never results.
#[derive(Clone, Copy)]
pub struct Tuning {
    pub cache_bytes: usize,
    pub max_open_files: i32,
    /// Keep index/filter blocks in the block cache (bounded memory for
    /// point probes over many CFs).
    pub cache_index_and_filter_blocks: bool,
}

impl Default for Tuning {
    fn default() -> Self {
        Self {
            cache_bytes: 64 << 20,
            max_open_files: 64,
            cache_index_and_filter_blocks: false,
        }
    }
}

impl Tuning {
    pub fn new(cache_mb: usize, max_open_files: i32) -> Self {
        Self {
            cache_bytes: cache_mb << 20,
            max_open_files,
            ..Self::default()
        }
    }
}

/// Opens a TAS database read-only with its own LRU block cache, the
/// `quote_last` merge operator and single-threaded background work.
pub fn open_read_only<I, S>(path: &Path, cfs: I, tuning: Tuning) -> Result<DB>
where
    I: IntoIterator<Item = S>,
    S: AsRef<str>,
{
    open_with_cache(path, cfs, tuning, &Cache::new_lru_cache(tuning.cache_bytes))
}

/// One bounded LRU block cache for many databases that are open at the same
/// time (a kline group holds every source of a product at once). Memory for
/// cached blocks then stays at `cache_bytes` in total instead of growing with
/// the number of databases; reads and results are unchanged.
pub struct SharedCache {
    cache: Cache,
    tuning: Tuning,
}

impl SharedCache {
    pub fn new(tuning: Tuning) -> Self {
        Self {
            cache: Cache::new_lru_cache(tuning.cache_bytes),
            tuning,
        }
    }
    /// [`open_read_only`] with this shared cache.
    pub fn open<I, S>(&self, path: &Path, cfs: I) -> Result<DB>
    where
        I: IntoIterator<Item = S>,
        S: AsRef<str>,
    {
        open_with_cache(path, cfs, self.tuning, &self.cache)
    }
    /// Bytes currently held by the shared cache.
    pub fn usage(&self) -> usize {
        self.cache.get_usage()
    }
}

/// Tuning of the cache shared by all sources of one kline group: index
/// blocks live in the bounded cache, so per-DB table-reader memory does not
/// grow with the number of daily databases.
pub const GROUP_TUNING: Tuning = Tuning {
    cache_bytes: 256 << 20,
    max_open_files: 64,
    cache_index_and_filter_blocks: true,
};

fn open_with_cache<I, S>(path: &Path, cfs: I, tuning: Tuning, cache: &Cache) -> Result<DB>
where
    I: IntoIterator<Item = S>,
    S: AsRef<str>,
{
    let options = || {
        let mut o = Options::default();
        o.set_max_open_files(tuning.max_open_files);
        o.set_max_background_jobs(1);
        o.set_max_file_opening_threads(1);
        o.set_skip_stats_update_on_db_open(true);
        o.set_merge_operator_associative("quote_last", quote_last_merge);
        let mut table = BlockBasedOptions::default();
        table.set_block_cache(cache);
        if tuning.cache_index_and_filter_blocks {
            table.set_cache_index_and_filter_blocks(true);
            table.set_pin_l0_filter_and_index_blocks_in_cache(false);
        }
        o.set_block_based_table_factory(&table);
        o
    };
    let descriptors: Vec<_> = cfs
        .into_iter()
        .map(|n| ColumnFamilyDescriptor::new(n.as_ref(), options()))
        .collect();
    DB::open_cf_descriptors_read_only(&options(), path, descriptors, false)
        .with_context(|| format!("open {} read-only", path.display()))
}

/// Column-family names present in the database at `path`.
pub fn list_cfs(path: &Path) -> Result<Vec<String>> {
    DB::list_cf(&Options::default(), path)
        .with_context(|| format!("list CFs of {}", path.display()))
}

/// Attaches the UTC day of `published` to an exchange time of day (the
/// nearest candidate within ±12h); a missing exchange time (`u64::MAX`)
/// falls back to `published`.
pub fn event_time(published: u64, hms: u64) -> Result<u64> {
    if hms == u64::MAX {
        return Ok(published);
    }
    ensure!(hms < DAY_NS, "invalid exchange clock");
    cme_tas_replay::product::exch_event_time_ns(published, hms)
}

/// Filesystem-safe, case-preserving path component for a RIC or key.
pub fn component(value: &str) -> String {
    if value == "." || value == ".." {
        return value.bytes().map(|_| "%2E").collect();
    }
    value
        .bytes()
        .map(|b| {
            if b.is_ascii_alphanumeric() || b"_-.^".contains(&b) {
                (b as char).to_string()
            } else {
                format!("%{b:02X}")
            }
        })
        .collect()
}

/// Inverse of [`component`]; a name that is not valid escaped UTF-8 is returned unchanged.
pub fn unescape_component(name: &str) -> String {
    let b = name.as_bytes();
    let mut out = Vec::with_capacity(b.len());
    let mut i = 0;
    while i < b.len() {
        if b[i] == b'%' && i + 2 < b.len() {
            if let Some(v) = std::str::from_utf8(&b[i + 1..i + 3])
                .ok()
                .and_then(|h| u8::from_str_radix(h, 16).ok())
            {
                out.push(v);
                i += 3;
                continue;
            }
        }
        out.push(b[i]);
        i += 1;
    }
    String::from_utf8(out).unwrap_or_else(|_| name.to_string())
}

/// Pretty JSON via `<name>.json.partial` + rename; creates the parent.
pub fn save_json(path: &Path, value: &impl Serialize) -> Result<()> {
    fs::create_dir_all(path.parent().context("json parent")?)?;
    let partial = path.with_extension("json.partial");
    fs::write(&partial, serde_json::to_vec_pretty(value)?)?;
    fs::rename(partial, path)?;
    Ok(())
}

pub fn file_sha256(path: &Path) -> Result<String> {
    let mut hash = Sha256::new();
    std::io::copy(&mut File::open(path)?, &mut hash)?;
    Ok(format!("{:x}", hash.finalize()))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn component_round_trips() {
        for r in ["ESZ26", "A/B", "VX^26", "a%b", "x y"] {
            assert_eq!(unescape_component(&component(r)), r);
        }
    }

    #[test]
    fn event_time_wraps_to_nearest_day_and_falls_back() {
        let d = 20_000 * DAY_NS;
        assert_eq!(event_time(d + 5, u64::MAX).unwrap(), d + 5);
        assert_eq!(
            event_time(d + 1_000, 23 * 3_600_000_000_000).unwrap(),
            d - DAY_NS + 23 * 3_600_000_000_000
        );
        assert_eq!(
            event_time(d + 23 * 3_600_000_000_000, 1_000).unwrap(),
            d + DAY_NS + 1_000
        );
        assert_eq!(event_time(d + 10, 20).unwrap(), d + 20);
        // Exactly half a day stays on the same date in both directions.
        assert_eq!(event_time(d, DAY_NS / 2).unwrap(), d + DAY_NS / 2);
        assert!(event_time(d, DAY_NS).is_err());
    }

    #[test]
    fn components_escape_and_keep_case() {
        assert_ne!(component("Cc1"), component("CC1"));
        assert_eq!(component("A/B"), "A%2FB");
        assert_eq!(component(".NSEI"), ".NSEI");
        assert_eq!(component(".."), "%2E%2E");
    }
}
