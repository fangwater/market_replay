//! Turns a selection (trade day / natural day, products, contracts, channels)
//! into a merged, ready-to-send row order.

use crate::cache::{Cache, Fetch};
use crate::channel::{Channel, DEFAULT_CHANNELS};
use crate::day;
use crate::line::{canonical_contract, product_of};
use crate::shard::{RowRec, Shard};
use anyhow::{bail, ensure, Result};
use serde::{Deserialize, Serialize};
use std::cmp::Reverse;
use std::collections::{BTreeMap, BinaryHeap, HashSet};
use std::sync::Arc;
use std::time::Instant;

/// Upper bound on how far back a night session of trade day D can start.
const TRADE_DAY_LOOKBACK: i64 = 7;
/// CFFEX trades day sessions only, so its file marks a directory that holds a
/// full open day (Saturday directories hold just the SHFE/INE night tail).
const DAY_SESSION_MARKER: &str = "mdl_21_1_0.csv";

#[derive(Clone, Debug, Default, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct Selection {
    /// `TradDay` to replay (default way to ask for "one day").
    #[serde(default)]
    pub trade_day: Option<String>,
    /// Alternatively, one NFS directory as-is (all trade days inside it).
    #[serde(default)]
    pub natural_day: Option<String>,
    /// Product codes, case-insensitive: `rb`, `IF`, `AP`.
    #[serde(default)]
    pub products: Vec<String>,
    /// Contracts, case-insensitive; CZCE `AP601` and `AP2601` both work.
    #[serde(default)]
    pub contracts: Vec<String>,
    /// Defaults to the six futures quote channels.
    #[serde(default)]
    pub channels: Vec<String>,
}

pub struct Stream {
    pub shard: u32,
    pub start: u64,
    pub count: u64,
}

#[derive(Clone, Copy, Debug)]
pub struct Entry {
    pub shard: u32,
    pub row: u32,
}

pub struct Plan {
    pub shards: Vec<Arc<Shard>>,
    pub entries: Vec<Entry>,
    pub summary: PlanSummary,
}

#[derive(Clone, Debug, Serialize)]
pub struct PlanSummary {
    pub trade_day: Option<u32>,
    pub natural_day: Option<u32>,
    pub dirs: Vec<u32>,
    pub rows: u64,
    pub bytes: u64,
    pub contracts: usize,
    pub per_channel: BTreeMap<String, u64>,
    /// True if any source directory may still be growing.
    pub partial: bool,
    pub shards_built: usize,
    pub shards_hit: usize,
    pub prepare_ms: u64,
    pub first_local: Option<String>,
    pub last_local: Option<String>,
}

impl Plan {
    #[inline]
    pub fn row(&self, e: Entry) -> (&Shard, &RowRec) {
        let s = &self.shards[e.shard as usize];
        (s, &s.rows()[e.row as usize])
    }
}

struct Matcher {
    products: HashSet<Vec<u8>>,
    contracts: HashSet<String>,
}

impl Matcher {
    fn new(sel: &Selection, ref_day: u32) -> Result<Self> {
        let products = sel
            .products
            .iter()
            .map(|p| {
                ensure!(
                    !p.is_empty() && p.bytes().all(|b| b.is_ascii_alphabetic()),
                    "invalid product {p:?}"
                );
                Ok(p.to_ascii_lowercase().into_bytes())
            })
            .collect::<Result<_>>()?;
        let contracts = sel
            .contracts
            .iter()
            .map(|c| canonical_contract(c.as_bytes(), ref_day))
            .collect::<Result<_>>()?;
        Ok(Self {
            products,
            contracts,
        })
    }

    fn all(&self) -> bool {
        self.products.is_empty() && self.contracts.is_empty()
    }

    fn matches(&self, symbol: &str, trade_day: u32) -> bool {
        if self.all() {
            return true;
        }
        let product = product_of(symbol.as_bytes()).to_ascii_lowercase();
        if self.products.contains(&product) {
            return true;
        }
        !self.contracts.is_empty()
            && canonical_contract(symbol.as_bytes(), trade_day)
                .map(|c| self.contracts.contains(&c))
                .unwrap_or(false)
    }
}

pub fn channels(sel: &Selection) -> Result<Vec<Channel>> {
    let names: Vec<&str> = if sel.channels.is_empty() {
        DEFAULT_CHANNELS.to_vec()
    } else {
        sel.channels.iter().map(String::as_str).collect()
    };
    let mut out = names
        .into_iter()
        .map(Channel::parse)
        .collect::<Result<Vec<_>>>()?;
    out.sort();
    out.dedup();
    Ok(out)
}

/// Loads (building if needed) every shard of the given dirs, channels in parallel.
/// (dir day, channel index, shard, cache outcome)
pub type Loaded = (u32, usize, Arc<Shard>, Fetch);

pub fn load_shards(
    cache: &Cache,
    dirs: &[u32],
    chans: &[Channel],
    force: bool,
) -> Result<Vec<Loaded>> {
    let jobs: Vec<(u32, usize)> = dirs
        .iter()
        .flat_map(|d| (0..chans.len()).map(move |c| (*d, c)))
        .collect();
    let results: Vec<Result<Option<Loaded>>> = std::thread::scope(|s| {
        let handles: Vec<_> = jobs
            .iter()
            .map(|&(d, c)| {
                let ch = &chans[c];
                s.spawn(move || Ok(cache.shard(d, ch, force)?.map(|(s, f)| (d, c, s, f))))
            })
            .collect();
        handles
            .into_iter()
            .map(|h| h.join().expect("shard loader panicked"))
            .collect()
    });
    let mut out = Vec::new();
    for r in results {
        if let Some(x) = r? {
            out.push(x);
        }
    }
    out.sort_by_key(|(d, c, _, _)| (*d, *c));
    Ok(out)
}

pub fn resolve_dirs(
    cache: &Cache,
    sel: &Selection,
) -> Result<(Option<u32>, Option<u32>, Vec<u32>)> {
    let all = cache.dirs()?;
    match (&sel.trade_day, &sel.natural_day) {
        (Some(td), None) => {
            let td = day::parse(td)?;
            let lo = day::add_days(td, -TRADE_DAY_LOOKBACK);
            // D's own dir, then back through Saturday tails to the previous open
            // day, whose evening opens D's night session.
            let mut dirs: Vec<u32> = all.iter().copied().filter(|d| *d == td).collect();
            for d in all.iter().rev().copied().filter(|d| *d < td && *d >= lo) {
                dirs.push(d);
                let marker = cache.msg_root.join(d.to_string()).join(DAY_SESSION_MARKER);
                if marker.exists() {
                    break;
                }
            }
            dirs.sort_unstable();
            ensure!(
                !dirs.is_empty(),
                "no NFS directory for trade day {td} or the week before"
            );
            Ok((Some(td), None, dirs))
        }
        (None, Some(nd)) => {
            let nd = day::parse(nd)?;
            ensure!(all.contains(&nd), "no NFS directory {nd}");
            Ok((None, Some(nd), vec![nd]))
        }
        _ => bail!("give exactly one of trade_day or natural_day"),
    }
}

pub fn prepare(cache: &Cache, sel: &Selection) -> Result<Plan> {
    let t0 = Instant::now();
    let (trade_day, natural_day, dirs) = resolve_dirs(cache, sel)?;
    let chans = channels(sel)?;
    let matcher = Matcher::new(sel, trade_day.or(natural_day).unwrap_or(0))?;
    let loaded = load_shards(cache, &dirs, &chans, false)?;

    let mut shards = Vec::new();
    let mut streams: Vec<(u32, Vec<Stream>)> = Vec::new(); // per dir rank
    let mut summary = PlanSummary {
        trade_day,
        natural_day,
        dirs: Vec::new(),
        rows: 0,
        bytes: 0,
        contracts: 0,
        per_channel: BTreeMap::new(),
        partial: false,
        shards_built: 0,
        shards_hit: 0,
        prepare_ms: 0,
        first_local: None,
        last_local: None,
    };
    let mut contracts = HashSet::new();
    for (dir, _c, shard, fetch) in loaded {
        match fetch {
            Fetch::Built => summary.shards_built += 1,
            Fetch::Hit => summary.shards_hit += 1,
        }
        let idx = shards.len() as u32;
        let mut picked = Vec::new();
        for g in &shard.index.groups {
            if trade_day.is_some_and(|td| g.trade_day != td)
                || !matcher.matches(&g.symbol, g.trade_day)
            {
                continue;
            }
            contracts.insert(g.symbol.clone());
            *summary
                .per_channel
                .entry(shard.index.channel.clone())
                .or_default() += g.row_count;
            summary.rows += g.row_count;
            let rows = &shard.rows()[g.row_start as usize..(g.row_start + g.row_count) as usize];
            summary.bytes += rows.iter().map(|r| r.len as u64).sum::<u64>();
            picked.push(Stream {
                shard: idx,
                start: g.row_start,
                count: g.row_count,
            });
        }
        if picked.is_empty() {
            continue;
        }
        summary.partial |= !shard.index.complete;
        shard.advise_willneed();
        shards.push(shard);
        match streams.last_mut() {
            Some((d, v)) if *d == dir => v.extend(picked),
            _ => streams.push((dir, picked)),
        }
    }
    summary.contracts = contracts.len();
    summary.dirs = streams.iter().map(|(d, _)| *d).collect();
    let entries = merge(&shards, &streams, summary.rows as usize);
    let plan = Plan {
        shards,
        entries,
        summary,
    };
    let fmt = |e: Option<&Entry>| {
        e.map(|e| {
            let (s, r) = plan.row(*e);
            format!("{} {}", day::format_iso(s.index.dir_day), fmt_ms(r.ms))
        })
    };
    let (first, last) = (fmt(plan.entries.first()), fmt(plan.entries.last()));
    let mut plan = plan;
    plan.summary.first_local = first;
    plan.summary.last_local = last;
    plan.summary.prepare_ms = t0.elapsed().as_millis() as u64;
    Ok(plan)
}

/// Dir-major k-way merge. Within a dir, rows are ordered by monotone receive
/// ms, then channel, then source row — so each channel keeps its exact file
/// order and channels interleave by `LocalTime`.
fn merge(shards: &[Arc<Shard>], streams: &[(u32, Vec<Stream>)], total: usize) -> Vec<Entry> {
    let mut out = Vec::with_capacity(total);
    for (_, dir_streams) in streams {
        let mut heap = BinaryHeap::with_capacity(dir_streams.len());
        let mut pos: Vec<u64> = dir_streams.iter().map(|s| s.start).collect();
        let key = |si: usize, at: u64| {
            let s = &dir_streams[si];
            let r = &shards[s.shard as usize].rows()[at as usize];
            Reverse((r.ms, s.shard, r.row, si as u32))
        };
        for (si, s) in dir_streams.iter().enumerate() {
            if s.count > 0 {
                heap.push(key(si, s.start));
            }
        }
        while let Some(Reverse((_, shard, _, si))) = heap.pop() {
            let si = si as usize;
            let at = pos[si];
            out.push(Entry {
                shard,
                row: at as u32,
            });
            pos[si] += 1;
            let s = &dir_streams[si];
            if pos[si] < s.start + s.count {
                heap.push(key(si, pos[si]));
            }
        }
    }
    out
}

pub fn fmt_ms(ms: u32) -> String {
    format!(
        "{:02}:{:02}:{:02}.{:03}",
        ms / 3_600_000,
        ms / 60_000 % 60,
        ms / 1000 % 60,
        ms % 1000
    )
}

/// Contracts available for a selection, with row counts (for discovery).
pub fn symbols(cache: &Cache, sel: &Selection) -> Result<BTreeMap<String, u64>> {
    let (trade_day, _, dirs) = resolve_dirs(cache, sel)?;
    let chans = channels(sel)?;
    let mut out = BTreeMap::new();
    for (_, _, shard, _) in load_shards(cache, &dirs, &chans, false)? {
        for g in &shard.index.groups {
            if trade_day.is_none_or(|td| g.trade_day == td) {
                *out.entry(format!("{}:{}", shard.index.channel, g.symbol))
                    .or_default() += g.row_count;
            }
        }
    }
    Ok(out)
}
