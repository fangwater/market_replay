//! Stage 3 of the L2 pipeline: slices of any frequency from the washed day
//! databases (`l2_db`). Nothing is decompressed or parsed from text here, so a
//! new frequency is just another extract. Fixed pool of `workers` threads, one
//! RIC per task, largest RIC first; the databases are opened read-only with the
//! repo's fixed budget (one shared bounded block cache, one background thread).
use crate::l2_db::{self, extract_ric};
use crate::l2_slice::{day_number, Layout, Params, ParquetSink, RicStats, DAY_NS};
use crate::tasdb::{SharedCache, Tuning};
use anyhow::{ensure, Context, Result};
use rocksdb::DB;
use serde_json::{json, Value};
use std::{
    collections::{BTreeMap, HashMap},
    path::PathBuf,
    sync::{
        atomic::{AtomicUsize, Ordering},
        Arc, Mutex,
    },
    time::Instant,
};

#[derive(Clone)]
pub struct ExtractConfig {
    pub db_root: PathBuf,
    pub out_root: PathBuf,
    pub exchange_of: Arc<BTreeMap<String, String>>,
    pub default_exchange: Option<String>,
    pub params: Params,
    pub force: bool,
    pub workers: usize,
    /// Total block cache of all open day databases.
    pub cache_mb: usize,
}

#[derive(Default)]
struct JobAcc {
    stats: Vec<RicStats>,
    written: usize,
    kept: usize,
    out_rows: u64,
    out_bytes: u64,
}

#[derive(Debug, Default)]
pub struct ExtractReport {
    pub results: Vec<Value>,
    pub failed: Option<String>,
}

struct Task {
    job: usize,
    ric: String,
    updates: u64,
}

pub fn run_extract(cfg: &ExtractConfig, jobs: Vec<(String, String)>) -> Result<ExtractReport> {
    ensure!(cfg.workers >= 1, "workers must be >= 1");
    let cache = SharedCache::new(Tuning::new(cfg.cache_mb, 64));
    let mut dbs: HashMap<String, Arc<DB>> = HashMap::new();
    let mut results = Vec::new();
    let mut tasks = Vec::new();
    let mut live: Vec<(String, String)> = Vec::new();
    for (product, day) in jobs {
        day_number(&day)?;
        let dir = l2_db::day_dir(&cfg.db_root, &day);
        if !dir.join("CURRENT").exists() {
            results.push(json!({"product": product, "day": day, "status": "missing_db"}));
            continue;
        }
        if !dbs.contains_key(&day) {
            dbs.insert(day.clone(), Arc::new(l2_db::open_read(&dir, &cache)?));
        }
        let Some(marker) = l2_db::read_marker(&dbs[&day], &product)? else {
            results.push(json!({"product": product, "day": day, "status": "not_washed"}));
            continue;
        };
        let job = live.len();
        for r in marker["rics"].as_array().context("marker rics")? {
            tasks.push(Task {
                job,
                ric: r[0].as_str().context("marker ric")?.to_string(),
                updates: r[1].as_u64().context("marker updates")?,
            });
        }
        live.push((product, day));
    }
    tasks.sort_by(|a, b| b.updates.cmp(&a.updates).then_with(|| a.ric.cmp(&b.ric)));
    let acc: Mutex<Vec<JobAcc>> = Mutex::new((0..live.len()).map(|_| JobAcc::default()).collect());
    let failed: Mutex<Option<String>> = Mutex::new(None);
    let next = AtomicUsize::new(0);
    let started = Instant::now();
    std::thread::scope(|scope| {
        for _ in 0..cfg.workers {
            scope.spawn(|| loop {
                if failed.lock().expect("failed lock").is_some() {
                    break;
                }
                let i = next.fetch_add(1, Ordering::Relaxed);
                let Some(t) = tasks.get(i) else { break };
                let (product, day) = &live[t.job];
                let r = (|| -> Result<()> {
                    let layout = Layout {
                        out_root: cfg.out_root.clone(),
                        exchange_of: Arc::clone(&cfg.exchange_of),
                        default_exchange: cfg.default_exchange.clone(),
                        day: day.clone(),
                        force: cfg.force,
                    };
                    let day_start = day_number(day)? * DAY_NS;
                    let mut sink = ParquetSink::new(&layout, cfg.params, day_start);
                    let stats = extract_ric(&dbs[day], &t.ric, &cfg.params, &mut sink)?;
                    let kept = sink.skipped.len();
                    ensure!(
                        kept == 1 || stats.updates == t.updates,
                        "{}: {} updates extracted, marker says {}",
                        t.ric,
                        stats.updates,
                        t.updates
                    );
                    let mut a = acc.lock().expect("acc lock");
                    let j = &mut a[t.job];
                    j.written += sink.outputs.len();
                    j.kept += kept;
                    j.out_rows += sink.outputs.iter().map(|o| o.rows).sum::<u64>();
                    j.out_bytes += sink.outputs.iter().map(|o| o.bytes).sum::<u64>();
                    j.stats.push(stats);
                    Ok(())
                })()
                .with_context(|| format!("{product} {day} {}", t.ric));
                if let Err(e) = r {
                    failed
                        .lock()
                        .expect("failed lock")
                        .get_or_insert(format!("{e:#}"));
                }
            });
        }
    });
    let secs = started.elapsed().as_secs_f64();
    let grid_day = (DAY_NS / cfg.params.freq_ns) as u64;
    for (job, a) in acc.into_inner().expect("acc lock").into_iter().enumerate() {
        let (product, day) = &live[job];
        let sum = |g: fn(&RicStats) -> u64| a.stats.iter().map(g).sum::<u64>();
        let (updates, buckets, emitted) =
            (sum(|s| s.updates), sum(|s| s.buckets), sum(|s| s.emitted));
        let (anchors, sessions, active) = (
            sum(|s| s.anchors),
            sum(|s| s.sessions),
            sum(|s| s.active_grid_points),
        );
        let rics = a.stats.len() as u64;
        results.push(json!({
            "product": product, "day": day, "status": "ok",
            "rics": rics, "written": a.written, "kept_existing": a.kept,
            "updates": updates, "nonempty_grid_points": buckets,
            "emitted_rows": emitted, "anchor_rows": anchors, "sessions": sessions,
            "written_rows": a.out_rows, "parquet_bytes": a.out_bytes,
            "active_grid_points": active, "full_day_grid_points": grid_day * rics,
            "emitted_per_active_grid": if active > 0 { emitted as f64 / active as f64 } else { 0.0 },
            "emitted_per_full_day_grid": emitted as f64 / (grid_day * rics.max(1)) as f64,
            "max_rows_per_ric": a.stats.iter().map(|s| s.emitted).max().unwrap_or(0),
        }));
    }
    results.push(json!({"extract_seconds": (secs * 100.0).round() / 100.0, "tasks": tasks.len()}));
    Ok(ExtractReport {
        results,
        failed: failed.into_inner().expect("failed lock"),
    })
}
