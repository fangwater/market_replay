//! Publishes a finished `lseg_tas_parquet --layout ric-day` export (TAS trade / quote parquet) into the
//! delivery tree with a fixed pool of threads: temp file + fsync + footer check + non-overwriting hard link
//! (`lseg_delivery::publish::copy_one_timed`), an existing identical file is left alone, first / middle /
//! last file of every (CF, kind) are compared by full SHA-256. Resumable and idempotent; gated on the server's jobs.
//!
//! `lseg_tas_publish --export <staged root> --target /mnt/nfs/30.100_cta_data/lseg --progress p.json`
use anyhow::{bail, ensure, Context, Result};
use clap::Parser;
use lseg_delivery::l2_backfill::{spawn_gate, Gate};
use lseg_delivery::publish::{copy_one_timed, Part};
use rayon::prelude::*;
use serde_json::{json, Value};
use std::collections::BTreeSet;
use std::fs;
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant};

#[derive(Parser)]
struct Args {
    /// Staged export root (`_SUCCESS.json`, `_audit/*.json`, `lseg/<exchange>/<year>/...`)
    #[arg(long)]
    export: PathBuf,
    /// Delivery root = the `lseg/` directory of the share
    #[arg(long, default_value = "/mnt/nfs/30.100_cta_data/lseg")]
    target: PathBuf,
    #[arg(long, default_value_t = 8)]
    threads: usize,
    #[arg(long)]
    progress: PathBuf,
    /// host:port of the server whose job state gates the run
    #[arg(long, default_value = "127.0.0.1:18313")]
    status_addr: String,
    #[arg(long)]
    no_gate: bool,
    /// Check and count only
    #[arg(long)]
    dry_run: bool,
}

fn read_json(path: &Path) -> Result<Value> {
    serde_json::from_slice(&fs::read(path).with_context(|| format!("read {path:?}"))?)
        .with_context(|| format!("parse {path:?}"))
}

struct Item {
    part: Part,
    full: bool,
}

fn collect(root: &Path) -> Result<(Vec<Item>, Value)> {
    let success = read_json(&root.join("_SUCCESS.json"))?;
    ensure!(success["status"] == "complete", "export is not complete");
    let mut items = Vec::new();
    let mut seen = BTreeSet::new();
    let mut reports = 0;
    for entry in fs::read_dir(root.join("_audit"))? {
        let path = entry?.path();
        let name = path.file_name().unwrap().to_string_lossy().into_owned();
        if !name.starts_with("p%3A") || !name.ends_with(".json") {
            continue;
        }
        let report = read_json(&path)?;
        ensure!(report["verified"] == true, "{name}: not verified");
        reports += 1;
        let parts: Vec<Part> = serde_json::from_value(report["parts"].clone())
            .with_context(|| format!("{name}: parts"))?;
        for kind in [1u8, 3] {
            let mut v: Vec<&Part> = parts.iter().filter(|p| p.kind == kind).collect();
            v.sort_by(|a, b| (&a.utc_date, &a.ric, &a.path).cmp(&(&b.utc_date, &b.ric, &b.path)));
            let sample: BTreeSet<&str> = if v.is_empty() {
                BTreeSet::new()
            } else {
                [0, v.len() / 2, v.len() - 1]
                    .iter()
                    .map(|&i| v[i].path.as_str())
                    .collect()
            };
            for p in v {
                ensure!(seen.insert(p.path.clone()), "duplicate part {}", p.path);
                ensure!(
                    root.join(&p.path).is_file(),
                    "missing staged file {}",
                    p.path
                );
                items.push(Item {
                    part: p.clone(),
                    full: sample.contains(p.path.as_str()),
                });
            }
        }
    }
    ensure!(reports > 0, "no CF report in {root:?}");
    let expected = success["files"]
        .as_u64()
        .or_else(|| success["delivery"]["files"].as_u64());
    if let Some(n) = expected {
        ensure!(
            n as usize == items.len(),
            "reports list {} files, _SUCCESS says {n}",
            items.len()
        );
    }
    Ok((items, success))
}

fn main() -> Result<()> {
    let a = Args::parse();
    ensure!((1..=8).contains(&a.threads), "--threads must be 1..=8");
    let (items, success) = collect(&a.export)?;
    let total_bytes: u64 = items.iter().map(|i| i.part.bytes).sum();
    eprintln!(
        "lseg_tas_publish: {} files, {:.1} GB, source period {}",
        items.len(),
        total_bytes as f64 / 1e9,
        success["source_period"]
    );
    if a.dry_run {
        return Ok(());
    }
    let gate = Gate::new();
    let gate_thread = (!a.no_gate).then(|| spawn_gate(Arc::clone(&gate), a.status_addr.clone()));
    let (created, same, sha, done) = (
        AtomicU64::new(0),
        AtomicU64::new(0),
        AtomicU64::new(0),
        AtomicU64::new(0),
    );
    let errors: Mutex<Vec<String>> = Mutex::new(Vec::new());
    let started = Instant::now();
    let finished = std::sync::atomic::AtomicBool::new(false);
    let pool = rayon::ThreadPoolBuilder::new()
        .num_threads(a.threads)
        .build()?;
    std::thread::scope(|s| {
        s.spawn(|| {
            while !finished.load(Ordering::Relaxed) {
                let (paused, reason) = gate.state();
                let v = json!({"state": if paused {"paused"} else {"running"}, "pause_reason": reason,
                    "files_total": items.len(), "files_done": done.load(Ordering::Relaxed),
                    "created": created.load(Ordering::Relaxed), "existing_identical": same.load(Ordering::Relaxed),
                    "sha256_compared": sha.load(Ordering::Relaxed), "errors": errors.lock().unwrap().len(),
                    "elapsed_s": started.elapsed().as_secs(), "export": a.export, "target": a.target});
                let _ = fs::write(&a.progress, serde_json::to_vec_pretty(&v).unwrap());
                std::thread::sleep(Duration::from_secs(10));
            }
        });
        pool.install(|| {
            items.par_iter().for_each(|it| {
                gate.wait();
                if !errors.lock().unwrap().is_empty() {
                    return;
                }
                let rel = Path::new(&it.part.path)
                    .strip_prefix("lseg")
                    .expect("lseg/ prefix");
                match copy_one_timed(
                    &a.export.join(&it.part.path),
                    &a.target.join(rel),
                    &it.part,
                    it.full,
                    false,
                ) {
                    Ok((new, _)) => {
                        if new {
                            created.fetch_add(1, Ordering::Relaxed)
                        } else {
                            same.fetch_add(1, Ordering::Relaxed)
                        };
                        sha.fetch_add(u64::from(it.full), Ordering::Relaxed);
                        done.fetch_add(1, Ordering::Relaxed);
                    }
                    Err(e) => errors
                        .lock()
                        .unwrap()
                        .push(format!("{}: {e:#}", it.part.path)),
                }
            });
        });
        finished.store(true, Ordering::Relaxed);
    });
    gate.stop.store(true, Ordering::Relaxed);
    if let Some(t) = gate_thread {
        let _ = t.join();
    }
    let errors = errors.into_inner().unwrap();
    let summary = json!({"summary": true, "files": items.len(), "created": created.load(Ordering::Relaxed),
        "existing_identical": same.load(Ordering::Relaxed), "sha256_compared": sha.load(Ordering::Relaxed),
        "bytes": total_bytes, "errors": errors.len(), "seconds": started.elapsed().as_secs()});
    println!("{summary}");
    if !errors.is_empty() {
        for e in errors.iter().take(10) {
            eprintln!("ERROR {e}");
        }
        bail!("{} file(s) failed", errors.len());
    }
    let _ = fs::write(
        &a.progress,
        serde_json::to_vec_pretty(&json!({"state": "finished", "summary": summary}))?,
    );
    Ok(())
}
