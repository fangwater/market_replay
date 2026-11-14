//! One NFS `mdl_*.csv` file re-sharded on local NVMe by `(InstruID, TradDay)`.
//!
//! `frames.bin` holds ready-to-send ZMQ frames (`<topic> || <original line>`),
//! grouped by contract and kept in source row order inside each group, so a
//! replay is a sequence of `zmq_send(mmap slice)` calls with no parsing.

use crate::channel::Channel;
use crate::line::{self, Layout, BAD_TIME};
use anyhow::{bail, ensure, Context, Result};
use memmap2::Mmap;
use serde::{Deserialize, Serialize};
use std::collections::HashMap;
use std::fs::{self, File};
use std::hash::{BuildHasherDefault, Hasher};
use std::io::Write;
use std::os::unix::fs::FileExt;
use std::path::{Path, PathBuf};
use std::time::{Instant, SystemTime, UNIX_EPOCH};

pub const FORMAT_VERSION: u32 = 1;
const MAX_SYMBOL: usize = 31;
const READ_BLOCK: usize = 64 << 20;

/// Row record in `rows.bin`; 24 bytes, little-endian, `repr(C)`.
#[repr(C)]
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub struct RowRec {
    /// Frame offset in `frames.bin`.
    pub off: u64,
    pub len: u32,
    /// Running max of `LocalTime` in source order (ms of day); monotone per file.
    pub ms: u32,
    /// 0-based data row in the source file (header excluded).
    pub row: u32,
    /// Raw `LocalTime`, `u32::MAX` if unparsable.
    pub raw_ms: u32,
}
const _: () = assert!(std::mem::size_of::<RowRec>() == 24);

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct Group {
    pub symbol: String,
    pub trade_day: u32,
    pub row_start: u64,
    pub row_count: u64,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct ShardIndex {
    pub version: u32,
    pub dir_day: u32,
    pub channel: String,
    pub topic: String,
    pub source_path: String,
    pub source_size: u64,
    pub source_mtime_ns: u64,
    /// Bytes of the source actually indexed (ends at the last complete line).
    pub indexed_bytes: u64,
    /// False while the directory may still be appended to by the vendor.
    pub complete: bool,
    pub rows: u64,
    pub frames_bytes: u64,
    pub bad_time_rows: u64,
    pub bad_day_rows: u64,
    pub header: String,
    pub groups: Vec<Group>,
    pub built_at_unix: u64,
    pub build_ms: BuildTiming,
}

#[derive(Clone, Copy, Debug, Default, Serialize, Deserialize)]
pub struct BuildTiming {
    pub read: u64,
    pub parse: u64,
    pub write: u64,
    pub total: u64,
}

pub struct Shard {
    pub index: ShardIndex,
    frames: Option<Mmap>,
    rows: Option<Mmap>,
}

impl Shard {
    pub fn open(dir: &Path) -> Result<Self> {
        let index: ShardIndex = serde_json::from_slice(
            &fs::read(dir.join("index.json"))
                .with_context(|| format!("read {dir:?}/index.json"))?,
        )?;
        ensure!(index.version == FORMAT_VERSION, "shard format changed");
        let map = |name: &str| -> Result<Option<Mmap>> {
            let file = File::open(dir.join(name))?;
            if file.metadata()?.len() == 0 {
                return Ok(None);
            }
            // SAFETY: cache files are written once into a temp dir and renamed in;
            // they are never modified in place.
            Ok(Some(unsafe { Mmap::map(&file)? }))
        };
        let shard = Self {
            frames: map("frames.bin")?,
            rows: map("rows.bin")?,
            index,
        };
        ensure!(
            shard.rows().len() as u64 == shard.index.rows
                && shard.frames().len() as u64 == shard.index.frames_bytes,
            "shard {dir:?} is truncated"
        );
        Ok(shard)
    }

    pub fn rows(&self) -> &[RowRec] {
        match &self.rows {
            // SAFETY: mmap is page aligned, RowRec is POD, length checked in open.
            Some(m) => unsafe {
                std::slice::from_raw_parts(m.as_ptr().cast(), m.len() / size_of::<RowRec>())
            },
            None => &[],
        }
    }

    pub fn frames(&self) -> &[u8] {
        self.frames.as_deref().unwrap_or(&[])
    }

    #[inline]
    pub fn frame(&self, row: &RowRec) -> &[u8] {
        &self.frames()[row.off as usize..row.off as usize + row.len as usize]
    }

    pub fn advise_willneed(&self) {
        if let Some(m) = &self.frames {
            let _ = m.advise(memmap2::Advice::WillNeed);
        }
    }
}

pub fn source_stamp(path: &Path) -> Result<(u64, u64)> {
    let meta = fs::metadata(path).with_context(|| format!("stat {path:?}"))?;
    let mtime = meta
        .modified()?
        .duration_since(UNIX_EPOCH)
        .unwrap_or_default()
        .as_nanos() as u64;
    Ok((meta.len(), mtime))
}

/// Parallel positional reads; NFS throughput scales with outstanding requests.
fn read_parallel(path: &Path, len: u64, threads: usize) -> Result<Vec<u8>> {
    let file = File::open(path).with_context(|| format!("open {path:?}"))?;
    let mut buf = vec![0u8; len as usize];
    let blocks: Vec<(usize, &mut [u8])> = buf
        .chunks_mut(READ_BLOCK)
        .enumerate()
        .map(|(i, c)| (i * READ_BLOCK, c))
        .collect();
    let next = std::sync::atomic::AtomicUsize::new(0);
    let blocks = std::sync::Mutex::new(blocks.into_iter().map(Some).collect::<Vec<_>>());
    let count = blocks.lock().unwrap().len();
    std::thread::scope(|s| -> Result<()> {
        let workers: Vec<_> = (0..threads.clamp(1, count.max(1)))
            .map(|_| {
                s.spawn(|| -> Result<()> {
                    loop {
                        let i = next.fetch_add(1, std::sync::atomic::Ordering::Relaxed);
                        if i >= count {
                            return Ok(());
                        }
                        let (off, chunk) = blocks.lock().unwrap()[i].take().unwrap();
                        file.read_exact_at(chunk, off as u64)
                            .with_context(|| format!("read {path:?} at {off}"))?;
                    }
                })
            })
            .collect();
        for w in workers {
            w.join().expect("reader panicked")?;
        }
        Ok(())
    })?;
    Ok(buf)
}

#[derive(Clone, Copy, PartialEq, Eq, Hash)]
struct Key {
    trade_day: u32,
    len: u8,
    symbol: [u8; MAX_SYMBOL],
}

impl Key {
    fn new(symbol: &[u8], trade_day: u32) -> Result<Self> {
        ensure!(
            symbol.len() <= MAX_SYMBOL,
            "InstruID longer than {MAX_SYMBOL} bytes"
        );
        let mut key = Self {
            trade_day,
            len: symbol.len() as u8,
            symbol: [0; MAX_SYMBOL],
        };
        key.symbol[..symbol.len()].copy_from_slice(symbol);
        Ok(key)
    }
    fn symbol(&self) -> &[u8] {
        &self.symbol[..self.len as usize]
    }
}

/// FxHash: the group key is trusted local data, SipHash would be pure overhead.
#[derive(Default)]
struct Fx(u64);
impl Hasher for Fx {
    fn finish(&self) -> u64 {
        self.0
    }
    fn write(&mut self, bytes: &[u8]) {
        for chunk in bytes.chunks(8) {
            let mut v = [0u8; 8];
            v[..chunk.len()].copy_from_slice(chunk);
            self.0 = (self.0.rotate_left(5) ^ u64::from_le_bytes(v))
                .wrapping_mul(0x51_7c_c1_b7_27_22_0a_95);
        }
    }
    fn write_u32(&mut self, v: u32) {
        self.0 = (self.0.rotate_left(5) ^ v as u64).wrapping_mul(0x51_7c_c1_b7_27_22_0a_95);
    }
    fn write_u8(&mut self, v: u8) {
        self.write_u32(v as u32)
    }
}
type FxMap<K, V> = HashMap<K, V, BuildHasherDefault<Fx>>;

struct TmpRow {
    off: u64,
    len: u32,
    raw_ms: u32,
    group: u32,
}

struct ChunkOut {
    rows: Vec<TmpRow>,
    keys: Vec<Key>,
    bad_day: u64,
}

fn parse_chunk(buf: &[u8], start: usize, end: usize, layout: &Layout) -> Result<ChunkOut> {
    let mut map: FxMap<Key, u32> = FxMap::default();
    let mut out = ChunkOut {
        rows: Vec::with_capacity((end - start) / 280 + 16),
        keys: Vec::new(),
        bad_day: 0,
    };
    let mut last: Option<(Key, u32)> = None;
    let mut pos = start;
    for nl in memchr::memchr_iter(b'\n', &buf[start..end]) {
        let line_end = start + nl;
        let line = &buf[pos..line_end];
        let line_start = pos;
        pos = line_end + 1;
        if line.is_empty() {
            continue;
        }
        let f = line::extract(line, layout);
        if f.trade_day == 0 {
            out.bad_day += 1;
        }
        let key =
            Key::new(f.symbol, f.trade_day).with_context(|| format!("row at byte {line_start}"))?;
        let group = match last {
            Some((k, g)) if k == key => g,
            _ => {
                let next = out.keys.len() as u32;
                let g = *map.entry(key).or_insert_with(|| {
                    out.keys.push(key);
                    next
                });
                last = Some((key, g));
                g
            }
        };
        out.rows.push(TmpRow {
            off: line_start as u64,
            len: u32::try_from(line.len()).context("line longer than 4 GiB")?,
            raw_ms: f.local_ms,
            group,
        });
    }
    Ok(out)
}

/// Builds the shard for `src` into `dst` (replaced atomically).
pub fn build(
    src: &Path,
    dst: &Path,
    dir_day: u32,
    channel: &Channel,
    complete: bool,
    threads: usize,
) -> Result<ShardIndex> {
    let t_start = Instant::now();
    let (size, mtime) = source_stamp(src)?;
    let buf = read_parallel(src, size, threads)?;
    let t_read = Instant::now();

    // Only complete lines: a growing file may end mid-row.
    let usable = memchr::memrchr(b'\n', &buf).map_or(0, |i| i + 1);
    let header_end = memchr::memchr(b'\n', &buf[..usable]).context("source has no header line")?;
    let header = &buf[..header_end];
    let layout = Layout::from_header(header).with_context(|| format!("header of {src:?}"))?;
    let body = header_end + 1;

    // Chunk boundaries on newlines.
    let parts = threads.max(1);
    let mut bounds = vec![body];
    for i in 1..parts {
        let target = body + (usable - body) * i / parts;
        let b = match memchr::memchr(b'\n', &buf[target.max(*bounds.last().unwrap())..usable]) {
            Some(p) => target.max(*bounds.last().unwrap()) + p + 1,
            None => usable,
        };
        bounds.push(b);
    }
    bounds.push(usable);
    bounds.dedup();

    let chunks: Vec<ChunkOut> = std::thread::scope(|s| {
        let handles: Vec<_> = bounds
            .windows(2)
            .map(|w| {
                let (a, b) = (w[0], w[1]);
                let buf = &buf;
                let layout = &layout;
                s.spawn(move || parse_chunk(buf, a, b, layout))
            })
            .collect();
        handles
            .into_iter()
            .map(|h| h.join().expect("parser panicked"))
            .collect::<Result<Vec<_>>>()
    })?;

    // Global groups, sorted by (symbol, trade day).
    let mut keys: Vec<Key> = Vec::new();
    let mut global: FxMap<Key, u32> = FxMap::default();
    let remaps: Vec<Vec<u32>> = chunks
        .iter()
        .map(|c| {
            c.keys
                .iter()
                .map(|k| {
                    *global.entry(*k).or_insert_with(|| {
                        keys.push(*k);
                        keys.len() as u32 - 1
                    })
                })
                .collect()
        })
        .collect();
    let mut order: Vec<u32> = (0..keys.len() as u32).collect();
    order.sort_by(|a, b| {
        let (ka, kb) = (&keys[*a as usize], &keys[*b as usize]);
        ka.symbol()
            .cmp(kb.symbol())
            .then(ka.trade_day.cmp(&kb.trade_day))
    });
    let mut rank = vec![0u32; keys.len()];
    for (r, g) in order.iter().enumerate() {
        rank[*g as usize] = r as u32;
    }

    let total_rows: usize = chunks.iter().map(|c| c.rows.len()).sum();
    ensure!(total_rows < u32::MAX as usize, "too many rows in {src:?}");
    let topic = channel.frame_prefix();
    let mut counts = vec![0u64; keys.len()];
    let mut group_of = Vec::with_capacity(total_rows);
    for (c, remap) in chunks.iter().zip(&remaps) {
        for r in &c.rows {
            let g = rank[remap[r.group as usize] as usize];
            counts[g as usize] += 1;
            group_of.push(g);
        }
    }
    let mut starts = vec![0u64; keys.len()];
    let mut acc = 0u64;
    for (s, c) in starts.iter_mut().zip(&counts) {
        *s = acc;
        acc += c;
    }

    // Place rows group-major, preserving source order; compute monotone ms.
    let mut recs = vec![RowRec::default(); total_rows];
    let mut src_span = vec![(0u64, 0u32); total_rows];
    let mut cursor = starts.clone();
    let mut running = 0u32;
    let mut seen_time = false;
    let mut bad_time = 0u64;
    let mut row = 0u32;
    for c in &chunks {
        for r in &c.rows {
            if r.raw_ms == BAD_TIME {
                bad_time += 1;
            } else if !seen_time || r.raw_ms > running {
                running = r.raw_ms;
                seen_time = true;
            }
            let g = group_of[row as usize] as usize;
            let slot = cursor[g] as usize;
            cursor[g] += 1;
            recs[slot] = RowRec {
                off: 0,
                len: r.len + topic.len() as u32,
                ms: running,
                row,
                raw_ms: r.raw_ms,
            };
            src_span[slot] = (r.off, r.len);
            row += 1;
        }
    }
    let mut off = 0u64;
    for rec in &mut recs {
        rec.off = off;
        off += rec.len as u64;
    }
    let frames_bytes = off;
    let t_parse = Instant::now();

    // Write into a sibling temp dir, then swap it in.
    let parent = dst.parent().context("shard path has no parent")?;
    fs::create_dir_all(parent)?;
    let tmp = parent.join(format!(
        ".{}.tmp.{}.{}",
        channel.name,
        std::process::id(),
        SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap_or_default()
            .as_nanos()
    ));
    fs::create_dir_all(&tmp)?;
    let result = (|| -> Result<ShardIndex> {
        let frames = File::create(tmp.join("frames.bin"))?;
        frames.set_len(frames_bytes)?;
        // Parallel pwrite: each worker owns a contiguous slot range.
        let per = total_rows.div_ceil(parts).max(1);
        std::thread::scope(|s| -> Result<()> {
            let handles: Vec<_> = (0..total_rows.div_ceil(per))
                .map(|i| {
                    let (recs, src_span, buf, frames) = (&recs, &src_span, &buf, &frames);
                    s.spawn(move || -> Result<()> {
                        let lo = i * per;
                        let hi = (lo + per).min(total_rows);
                        let mut out: Vec<u8> = Vec::with_capacity(8 << 20);
                        let mut at = recs[lo].off;
                        for &(o, l) in &src_span[lo..hi] {
                            out.extend_from_slice(topic);
                            out.extend_from_slice(&buf[o as usize..o as usize + l as usize]);
                            if out.len() >= 8 << 20 {
                                frames.write_all_at(&out, at)?;
                                at += out.len() as u64;
                                out.clear();
                            }
                        }
                        frames.write_all_at(&out, at)?;
                        Ok(())
                    })
                })
                .collect();
            for h in handles {
                h.join().expect("writer panicked")?;
            }
            Ok(())
        })?;
        // SAFETY: RowRec is POD repr(C).
        let rec_bytes = unsafe {
            std::slice::from_raw_parts(recs.as_ptr().cast::<u8>(), recs.len() * size_of::<RowRec>())
        };
        fs::write(tmp.join("rows.bin"), rec_bytes)?;
        let groups = order
            .iter()
            .enumerate()
            .map(|(r, g)| {
                let k = &keys[*g as usize];
                Ok(Group {
                    symbol: String::from_utf8(k.symbol().to_vec()).context("non-UTF-8 InstruID")?,
                    trade_day: k.trade_day,
                    row_start: starts[r],
                    row_count: counts[r],
                })
            })
            .collect::<Result<Vec<_>>>()?;
        let t_write = Instant::now();
        let ms = |a: Instant, b: Instant| (b - a).as_millis() as u64;
        let index = ShardIndex {
            version: FORMAT_VERSION,
            dir_day,
            channel: channel.name.clone(),
            topic: channel.topic.clone(),
            source_path: src.display().to_string(),
            source_size: size,
            source_mtime_ns: mtime,
            indexed_bytes: usable as u64,
            complete,
            rows: total_rows as u64,
            frames_bytes,
            bad_time_rows: bad_time,
            bad_day_rows: chunks.iter().map(|c| c.bad_day).sum(),
            header: String::from_utf8_lossy(header).into_owned(),
            groups,
            built_at_unix: SystemTime::now().duration_since(UNIX_EPOCH)?.as_secs(),
            build_ms: BuildTiming {
                read: ms(t_start, t_read),
                parse: ms(t_read, t_parse),
                write: ms(t_parse, t_write),
                total: ms(t_start, t_write),
            },
        };
        let mut f = File::create(tmp.join("index.json"))?;
        f.write_all(&serde_json::to_vec_pretty(&index)?)?;
        Ok(index)
    })();
    let index = match result {
        Ok(i) => i,
        Err(e) => {
            let _ = fs::remove_dir_all(&tmp);
            return Err(e);
        }
    };
    replace_dir(&tmp, dst)?;
    Ok(index)
}

/// Readers keep old mmaps valid after unlink, so swapping is safe.
fn replace_dir(tmp: &Path, dst: &Path) -> Result<()> {
    if dst.exists() {
        let trash: PathBuf = dst.with_extension(format!(
            "old.{}",
            SystemTime::now()
                .duration_since(UNIX_EPOCH)
                .unwrap_or_default()
                .as_nanos()
        ));
        fs::rename(dst, &trash)?;
        fs::rename(tmp, dst)?;
        let _ = fs::remove_dir_all(&trash);
    } else {
        fs::rename(tmp, dst)?;
    }
    if !dst.join("index.json").exists() {
        bail!("shard swap for {dst:?} failed");
    }
    Ok(())
}
