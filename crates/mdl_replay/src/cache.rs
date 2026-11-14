//! Lazily built, source-validated shard cache. NFS is read-only.

use crate::channel::Channel;
use crate::day;
use crate::shard::{self, Shard, ShardIndex};
use anyhow::{Context, Result};
use serde::Serialize;
use std::collections::HashMap;
use std::fs;
use std::path::{Path, PathBuf};
use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant, SystemTime};

/// Marker whose mtime records the last use of a cached natural day.
const LAST_USED: &str = ".last_used";
/// Days used this recently are never evicted (an active plan may need them).
const EVICT_GRACE: Duration = Duration::from_secs(600);
/// Interrupted builds leave temp dirs; older than this they are garbage.
const STALE_TMP: Duration = Duration::from_secs(3600);

type ShardKey = (u32, String);

pub struct Cache {
    pub msg_root: PathBuf,
    pub cache_root: PathBuf,
    pub build_threads: usize,
    loaded: Mutex<HashMap<ShardKey, Arc<Shard>>>,
    locks: Mutex<HashMap<ShardKey, Arc<Mutex<()>>>>,
    /// Bounds concurrent builds: each holds its whole source file in RAM.
    build_slots: (Mutex<usize>, std::sync::Condvar),
    /// LRU budget for `cache_root/v<N>`; 0 = unlimited.
    max_bytes: u64,
    touched: Mutex<HashMap<u32, Instant>>,
    evicting: Mutex<()>,
}

#[derive(Debug, Default, Serialize)]
pub struct EvictReport {
    pub max_bytes: u64,
    pub bytes_before: u64,
    pub bytes_after: u64,
    pub days_cached: usize,
    pub removed_days: Vec<u32>,
    pub removed_temp: usize,
}

pub const MAX_CONCURRENT_BUILDS: usize = 6;

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum Fetch {
    Hit,
    Built,
}

impl Cache {
    pub fn new(msg_root: PathBuf, cache_root: PathBuf, build_threads: usize) -> Self {
        Self {
            msg_root,
            cache_root,
            build_threads: build_threads.max(1),
            loaded: Mutex::new(HashMap::new()),
            locks: Mutex::new(HashMap::new()),
            build_slots: (Mutex::new(0), std::sync::Condvar::new()),
            max_bytes: 0,
            touched: Mutex::new(HashMap::new()),
            evicting: Mutex::new(()),
        }
    }

    pub fn with_max_bytes(mut self, max_bytes: u64) -> Self {
        self.max_bytes = max_bytes;
        self
    }

    fn version_root(&self) -> PathBuf {
        self.cache_root.join(format!("v{}", shard::FORMAT_VERSION))
    }

    /// Records use of a cached day (throttled to once a minute per day).
    fn touch(&self, dir_day: u32) {
        let now = Instant::now();
        {
            let mut t = self.touched.lock().unwrap();
            if t.get(&dir_day)
                .is_some_and(|at| now - *at < Duration::from_secs(60))
            {
                return;
            }
            t.insert(dir_day, now);
        }
        let path = self
            .version_root()
            .join(dir_day.to_string())
            .join(LAST_USED);
        if let Ok(f) = fs::File::create(&path) {
            let _ = f.set_modified(SystemTime::now());
        }
    }

    /// LRU eviction of whole natural days down to `max_bytes`, plus cleanup
    /// of abandoned temp/old dirs. Open mmaps of removed files stay valid.
    pub fn evict(&self) -> Result<EvictReport> {
        let _one = self.evicting.lock().unwrap();
        let root = self.version_root();
        let mut report = EvictReport {
            max_bytes: self.max_bytes,
            ..EvictReport::default()
        };
        if !root.exists() {
            return Ok(report);
        }
        let mut days: Vec<(SystemTime, u32, u64)> = Vec::new();
        for entry in fs::read_dir(&root)?.filter_map(|e| e.ok()) {
            let Some(day) = entry
                .file_name()
                .to_str()
                .and_then(|n| n.parse::<u32>().ok())
            else {
                continue;
            };
            let dir = entry.path();
            let mut bytes = 0u64;
            for ch in fs::read_dir(&dir)?.filter_map(|e| e.ok()) {
                let name = ch.file_name().to_string_lossy().into_owned();
                let path = ch.path();
                let stale = (name.contains(".tmp.") || name.contains(".old."))
                    && modified(&path).elapsed().unwrap_or_default() > STALE_TMP;
                if stale {
                    let _ = fs::remove_dir_all(&path);
                    report.removed_temp += 1;
                    continue;
                }
                bytes += dir_bytes(&path);
            }
            let used = fs::metadata(dir.join(LAST_USED))
                .and_then(|m| m.modified())
                .unwrap_or_else(|_| modified(&dir));
            days.push((used, day, bytes));
        }
        report.bytes_before = days.iter().map(|d| d.2).sum();
        report.bytes_after = report.bytes_before;
        days.sort();
        if self.max_bytes > 0 {
            let recent = |day: u32| {
                self.touched
                    .lock()
                    .unwrap()
                    .get(&day)
                    .is_some_and(|at| at.elapsed() < EVICT_GRACE)
            };
            for (used, day, bytes) in &days {
                if report.bytes_after <= self.max_bytes {
                    break;
                }
                if recent(*day) || used.elapsed().unwrap_or_default() < EVICT_GRACE {
                    continue;
                }
                let dir = root.join(day.to_string());
                // Rename first so a concurrent builder never sees a half-deleted day.
                let trash = root.join(format!(".evict.{day}.{}", std::process::id()));
                if fs::rename(&dir, &trash).is_ok() {
                    let _ = fs::remove_dir_all(&trash);
                    self.loaded.lock().unwrap().retain(|(d, _), _| d != day);
                    report.bytes_after -= bytes;
                    report.removed_days.push(*day);
                }
            }
        }
        report.days_cached = days.len() - report.removed_days.len();
        Ok(report)
    }

    /// Natural-day directories present on NFS, ascending.
    pub fn dirs(&self) -> Result<Vec<u32>> {
        let mut days: Vec<u32> = fs::read_dir(&self.msg_root)
            .with_context(|| format!("list {:?}", self.msg_root))?
            .filter_map(|e| e.ok())
            .filter_map(|e| {
                let name = e.file_name().into_string().ok()?;
                (name.len() == 8).then(|| day::parse(&name).ok()).flatten()
            })
            .collect();
        days.sort_unstable();
        Ok(days)
    }

    pub fn source_path(&self, dir_day: u32, channel: &Channel) -> PathBuf {
        self.msg_root
            .join(dir_day.to_string())
            .join(channel.file_name())
    }

    fn shard_path(&self, dir_day: u32, channel: &Channel) -> PathBuf {
        self.cache_root
            .join(format!("v{}", shard::FORMAT_VERSION))
            .join(dir_day.to_string())
            .join(&channel.name)
    }

    /// `None` when the channel file does not exist for that day (e.g. no GFEX before 2025).
    pub fn shard(
        &self,
        dir_day: u32,
        channel: &Channel,
        force: bool,
    ) -> Result<Option<(Arc<Shard>, Fetch)>> {
        let src = self.source_path(dir_day, channel);
        if !src.exists() {
            return Ok(None);
        }
        let key = (dir_day, channel.name.clone());
        let lock = self
            .locks
            .lock()
            .unwrap()
            .entry(key.clone())
            .or_default()
            .clone();
        let _guard = lock.lock().unwrap();
        let stamp = shard::source_stamp(&src)?;
        let fresh = |idx: &ShardIndex| {
            (idx.source_size, idx.source_mtime_ns) == stamp
                && (idx.complete || !day::directory_final(dir_day))
        };
        if !force {
            if let Some(s) = self.loaded.lock().unwrap().get(&key) {
                if fresh(&s.index) {
                    self.touch(dir_day);
                    return Ok(Some((s.clone(), Fetch::Hit)));
                }
            }
            let path = self.shard_path(dir_day, channel);
            if path.join("index.json").exists() {
                if let Ok(s) = Shard::open(&path) {
                    if fresh(&s.index) {
                        let s = Arc::new(s);
                        self.loaded.lock().unwrap().insert(key, s.clone());
                        self.touch(dir_day);
                        return Ok(Some((s, Fetch::Hit)));
                    }
                }
            }
        }
        let path = self.shard_path(dir_day, channel);
        let complete = day::directory_final(dir_day);
        {
            let (count, cv) = &self.build_slots;
            let mut n = cv
                .wait_while(count.lock().unwrap(), |n| *n >= MAX_CONCURRENT_BUILDS)
                .unwrap();
            *n += 1;
        }
        let built = shard::build(&src, &path, dir_day, channel, complete, self.build_threads);
        {
            let (count, cv) = &self.build_slots;
            *count.lock().unwrap() -= 1;
            cv.notify_one();
        }
        built.with_context(|| format!("build shard {src:?}"))?;
        let s = Arc::new(Shard::open(&path)?);
        self.loaded.lock().unwrap().insert(key, s.clone());
        self.touched.lock().unwrap().remove(&dir_day);
        self.touch(dir_day);
        if self.max_bytes > 0 {
            if let Err(e) = self.evict() {
                eprintln!("mdl-replay: cache eviction failed: {e:#}");
            }
        }
        Ok(Some((s, Fetch::Built)))
    }

    /// Channel files present in one directory.
    pub fn channels_in(&self, dir_day: u32) -> Result<Vec<Channel>> {
        let dir = self.msg_root.join(dir_day.to_string());
        let mut out: Vec<Channel> = fs::read_dir(&dir)
            .with_context(|| format!("list {dir:?}"))?
            .filter_map(|e| e.ok())
            .filter_map(|e| Channel::parse(&e.file_name().into_string().ok()?).ok())
            .collect();
        out.sort();
        Ok(out)
    }
}

fn modified(path: &Path) -> SystemTime {
    fs::metadata(path)
        .and_then(|m| m.modified())
        .unwrap_or(SystemTime::UNIX_EPOCH)
}

fn dir_bytes(path: &Path) -> u64 {
    match fs::read_dir(path) {
        Ok(rd) => rd
            .filter_map(|e| e.ok())
            .filter_map(|e| e.metadata().ok())
            .filter(|m| m.is_file())
            .map(|m| m.len())
            .sum(),
        Err(_) => fs::metadata(path).map(|m| m.len()).unwrap_or(0),
    }
}
