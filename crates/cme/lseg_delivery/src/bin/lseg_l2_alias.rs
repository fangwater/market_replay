//! COMEX metals L2 of the Globex RIC family (`1GC…`) for the standard RIC of the same contract.
//! See `lseg_delivery::l2_alias` and docs/lseg/l2_alias.md.
//!
//! - `plan` (read only): every `1` RIC-year under `<delivery>/comex/`, its standard RIC-year,
//!   the days the standard RIC has no L2 file, and the identity check on sample days
//!   (L1 vs the standard RIC's TAS quotes, with a different month as negative control).
//! - `deliver`: for RIC-years whose check passed, writes each missing day again with the
//!   standard RIC (`l2_alias_of` in the footer) and publishes it with the delivery rules
//!   (temporary file, fsync, footer check, hard link; never replacing a file).
use anyhow::{ensure, Context, Result};
use clap::{Parser, Subcommand};
use lseg_delivery::{
    l2_alias::{self, MAX_NEGATIVE, MIN_MATCH},
    limits::{AGGREGATION_WORKERS, IO_THREADS},
    publish::{self, component, Part},
    tasdb::unescape_component,
};
use rayon::prelude::*;
use serde::{Deserialize, Serialize};
use std::{
    fs,
    io::Write,
    path::{Path, PathBuf},
    sync::{
        atomic::{AtomicUsize, Ordering},
        Mutex,
    },
};

const FILE: &str = "l2_250ms.parquet";
const WRITERS: &[&str] = &[
    "lseg-daily",
    "lseg_l2_slice",
    "lseg_kline",
    "lseg_tas_publish",
    "lseg_tas_parquet",
    "tas_status_parquet",
    "lseg_catalog",
];

#[derive(Parser)]
struct Cli {
    #[command(subcommand)]
    cmd: Cmd,
}

#[derive(Subcommand)]
enum Cmd {
    Plan {
        #[arg(long, default_value = "/mnt/nfs/30.100_cta_data/lseg")]
        delivery: PathBuf,
        #[arg(long)]
        out: PathBuf,
        /// Only these alias RICs (default: all).
        #[arg(long, value_delimiter = ',')]
        only: Vec<String>,
        #[arg(long, default_value_t = AGGREGATION_WORKERS)]
        workers: usize,
    },
    Deliver {
        #[arg(long, default_value = "/mnt/nfs/30.100_cta_data/lseg")]
        delivery: PathBuf,
        /// `plan.csv` from `plan`.
        #[arg(long)]
        plan: PathBuf,
        /// Local staging directory (rewritten files before publication).
        #[arg(long)]
        stage: PathBuf,
        #[arg(long, default_value_t = AGGREGATION_WORKERS)]
        workers: usize,
        #[arg(long)]
        dry_run: bool,
    },
}

#[derive(Serialize, Deserialize, Clone)]
struct PlanRow {
    alias: String,
    standard: String,
    utc_year: i32,
    alias_days: usize,
    deliver_days: usize,
    sample_days: String,
    alias_rates: String,
    negative_rics: String,
    negative_rates: String,
    verdict: String,
}

fn ls(dir: &Path) -> Vec<String> {
    let mut v: Vec<String> = fs::read_dir(dir)
        .map(|rd| {
            rd.flatten()
                .map(|e| e.file_name().to_string_lossy().into_owned())
                .collect()
        })
        .unwrap_or_default();
    v.sort();
    v
}

fn days_with(dir: &Path, file: &str) -> Vec<String> {
    ls(dir)
        .into_iter()
        .filter(|d| {
            d.len() == 8 && d.bytes().all(|b| b.is_ascii_digit()) && dir.join(d).join(file).exists()
        })
        .collect()
}

fn writers_running() -> Vec<String> {
    let me = std::process::id();
    let mut out = Vec::new();
    for e in fs::read_dir("/proc").into_iter().flatten().flatten() {
        let Ok(pid) = e.file_name().to_string_lossy().parse::<u32>() else {
            continue;
        };
        if pid == me {
            continue;
        }
        let Ok(raw) = fs::read(e.path().join("cmdline")) else {
            continue;
        };
        let first = raw.split(|b| *b == 0).next().unwrap_or(&[]);
        let exe = String::from_utf8_lossy(first);
        let exe = exe.rsplit('/').next().unwrap_or("");
        if WRITERS.iter().any(|w| exe.starts_with(w)) {
            out.push(format!("{pid} {exe}"));
        }
    }
    out
}

fn plan_one(
    delivery: &Path,
    year: &str,
    alias: &str,
    standard: &str,
    siblings: &[String],
) -> Result<PlanRow> {
    let ydir = delivery.join("comex").join(year);
    let adir = ydir.join(component(alias));
    let sdir = ydir.join(component(standard));
    let alias_days = days_with(&adir, FILE);
    let mut row = PlanRow {
        alias: alias.into(),
        standard: standard.into(),
        utc_year: year.parse()?,
        alias_days: alias_days.len(),
        deliver_days: 0,
        sample_days: String::new(),
        alias_rates: String::new(),
        negative_rics: String::new(),
        negative_rates: String::new(),
        verdict: String::new(),
    };
    if !sdir.is_dir() {
        row.verdict = "no_standard_directory".into();
        return Ok(row);
    }
    row.deliver_days = alias_days
        .iter()
        .filter(|d| !sdir.join(d).join(FILE).exists())
        .count();
    let quoted: Vec<&String> = alias_days
        .iter()
        .filter(|d| sdir.join(d).join("quote.parquet").exists())
        .collect();
    if quoted.is_empty() {
        row.verdict = "no_tas_quote_to_check".into();
        return Ok(row);
    }
    let mut picks = vec![
        quoted[0],
        quoted[quoted.len() / 2],
        quoted[quoted.len() - 1],
    ];
    picks.dedup();
    let (mut days, mut rates, mut negs, mut neg_rates) = (vec![], vec![], vec![], vec![]);
    for d in picks {
        let l1 = l2_alias::l2_l1(&adir.join(d).join(FILE))?;
        let q = l2_alias::tas_quotes(&sdir.join(d).join("quote.parquet"))?;
        let Some(r) = l2_alias::match_rate(&l1, &q) else {
            continue;
        };
        days.push(d.clone());
        rates.push(r);
        // Negative control: another contract of the same root with TAS quotes that day.
        if let Some(n) = siblings
            .iter()
            .filter(|s| s.as_str() != standard)
            .find(|s| {
                ydir.join(component(s))
                    .join(d)
                    .join("quote.parquet")
                    .exists()
            })
        {
            let nq = l2_alias::tas_quotes(&ydir.join(component(n)).join(d).join("quote.parquet"))?;
            if let Some(nr) = l2_alias::match_rate(&l1, &nq) {
                negs.push(n.clone());
                neg_rates.push(nr);
            }
        }
    }
    let fmt = |v: &[f64]| {
        v.iter()
            .map(|x| format!("{x:.4}"))
            .collect::<Vec<_>>()
            .join(";")
    };
    row.sample_days = days.join(";");
    row.alias_rates = fmt(&rates);
    row.negative_rics = negs.join(";");
    row.negative_rates = fmt(&neg_rates);
    let mut sorted = rates.clone();
    sorted.sort_by(|a, b| a.partial_cmp(b).unwrap());
    let median = sorted.get(sorted.len() / 2).copied();
    row.verdict = match median {
        None => "no_comparable_day".into(),
        Some(_) if neg_rates.is_empty() => "no_negative_control".into(),
        Some(m) if m >= MIN_MATCH && neg_rates.iter().all(|n| *n <= MAX_NEGATIVE) => "pass".into(),
        Some(_) => "fail".into(),
    };
    Ok(row)
}

fn plan(delivery: &Path, out: &Path, only: &[String], workers: usize) -> Result<()> {
    ensure!(
        (1..=AGGREGATION_WORKERS).contains(&workers),
        "--workers must be 1..={AGGREGATION_WORKERS}"
    );
    let mut jobs = Vec::new();
    for year in ls(&delivery.join("comex")) {
        if year.parse::<i32>().is_err() {
            continue;
        }
        let names: Vec<String> = ls(&delivery.join("comex").join(&year))
            .iter()
            .map(|n| unescape_component(n))
            .collect();
        for n in &names {
            let Some(std) = l2_alias::standard_of(n) else {
                continue;
            };
            if !only.is_empty() && !only.contains(n) {
                continue;
            }
            let root = l2_alias::ROOTS
                .iter()
                .find(|r| std.starts_with(**r))
                .unwrap();
            let siblings: Vec<String> = names
                .iter()
                .filter(|s| {
                    !s.starts_with('1')
                        && l2_alias::standard_of(&format!("1{s}"))
                            .is_some_and(|x| x.starts_with(root))
                })
                .cloned()
                .collect();
            jobs.push((year.clone(), n.clone(), std, siblings));
        }
    }
    let done = AtomicUsize::new(0);
    let pool = rayon::ThreadPoolBuilder::new()
        .num_threads(workers)
        .build()?;
    let rows: Vec<Result<PlanRow>> = pool.install(|| {
        jobs.par_iter()
            .map(|(y, a, s, sib)| {
                let r = plan_one(delivery, y, a, s, sib).with_context(|| format!("{a} {y}"));
                let n = done.fetch_add(1, Ordering::SeqCst) + 1;
                if n.is_multiple_of(50) {
                    eprintln!("lseg_l2_alias: planned {n}/{}", jobs.len());
                }
                r
            })
            .collect()
    });
    fs::create_dir_all(out)?;
    let mut w = csv::Writer::from_path(out.join("plan.csv"))?;
    let mut summary = std::collections::BTreeMap::<String, (usize, usize)>::new();
    for r in rows {
        let r = r?;
        let e = summary.entry(r.verdict.clone()).or_default();
        e.0 += 1;
        e.1 += r.deliver_days;
        w.serialize(&r)?;
    }
    w.flush()?;
    println!(
        "{}",
        serde_json::json!({"alias_ric_years": jobs.len(), "by_verdict": summary})
    );
    Ok(())
}

fn deliver(
    delivery: &Path,
    plan: &Path,
    stage: &Path,
    workers: usize,
    dry_run: bool,
) -> Result<()> {
    ensure!(
        (1..=IO_THREADS).contains(&workers),
        "--workers must be 1..={IO_THREADS}"
    );
    let running = writers_running();
    ensure!(
        running.is_empty() || dry_run,
        "delivery writers are running: {running:?}"
    );
    let mut jobs = Vec::new();
    for r in csv::Reader::from_path(plan)?.deserialize::<PlanRow>() {
        let r = r?;
        if r.verdict != "pass" {
            continue;
        }
        let ydir = delivery.join("comex").join(r.utc_year.to_string());
        let (adir, sdir) = (
            ydir.join(component(&r.alias)),
            ydir.join(component(&r.standard)),
        );
        for d in days_with(&adir, FILE) {
            let target = sdir.join(&d).join(FILE);
            if !target.exists() {
                jobs.push((
                    r.alias.clone(),
                    r.standard.clone(),
                    r.utc_year,
                    d.clone(),
                    adir.join(&d).join(FILE),
                    target,
                ));
            }
        }
    }
    eprintln!("lseg_l2_alias: {} day files to deliver", jobs.len());
    if dry_run {
        return Ok(());
    }
    fs::create_dir_all(stage)?;
    let journal = Mutex::new(
        fs::OpenOptions::new()
            .create(true)
            .append(true)
            .open(stage.join("delivered.tsv"))?,
    );
    let years = Mutex::new(std::collections::BTreeSet::new());
    let (created, existing, done) = (
        AtomicUsize::new(0),
        AtomicUsize::new(0),
        AtomicUsize::new(0),
    );
    let failures = Mutex::new(Vec::new());
    let pool = rayon::ThreadPoolBuilder::new()
        .num_threads(workers)
        .build()?;
    pool.install(|| {
        jobs.par_iter()
            .enumerate()
            .for_each(|(i, (alias, standard, year, day, src, target))| {
                let staged = stage.join(format!("{i:07}.parquet"));
                let r = (|| -> Result<bool> {
                    let rows = l2_alias::rewrite(src, &staged, alias, standard)?;
                    let part = Part {
                        path: String::new(),
                        ric: standard.clone(),
                        kind: 0,
                        rows,
                        bytes: fs::metadata(&staged)?.len(),
                        utc_date: day.clone(),
                    };
                    publish::copy_one(&staged, target, &part, i.is_multiple_of(500), true)
                })();
                let _ = fs::remove_file(&staged);
                match r {
                    Ok(new) => {
                        if new {
                            created.fetch_add(1, Ordering::SeqCst);
                            let _ = writeln!(
                                journal.lock().unwrap(),
                                "{alias}\t{standard}\t{year}\t{day}\t{}",
                                target.display()
                            );
                        } else {
                            existing.fetch_add(1, Ordering::SeqCst);
                        }
                        years.lock().unwrap().insert((standard.clone(), *year));
                    }
                    Err(e) => failures
                        .lock()
                        .unwrap()
                        .push(format!("{} {day}: {e:#}", standard)),
                }
                let n = done.fetch_add(1, Ordering::SeqCst) + 1;
                if n.is_multiple_of(2000) {
                    eprintln!("lseg_l2_alias: {n}/{} files", jobs.len());
                }
            })
    });
    let mut f = fs::File::create(stage.join("ric_years.tsv"))?;
    for (r, y) in years.into_inner().unwrap() {
        writeln!(f, "{r}\t{y}")?;
    }
    let failures = failures.into_inner().unwrap();
    println!(
        "{}",
        serde_json::json!({"created": created.load(Ordering::SeqCst), "already_identical": existing.load(Ordering::SeqCst), "failed": failures.len()})
    );
    for e in failures.iter().take(20) {
        eprintln!("FAILED {e}");
    }
    ensure!(failures.is_empty(), "{} files failed", failures.len());
    Ok(())
}

fn main() -> Result<()> {
    match Cli::parse().cmd {
        Cmd::Plan {
            delivery,
            out,
            only,
            workers,
        } => plan(&delivery, &out, &only, workers),
        Cmd::Deliver {
            delivery,
            plan: p,
            stage,
            workers,
            dry_run,
        } => deliver(&delivery, &p, &stage, workers, dry_run),
    }
    .context("lseg_l2_alias")
}
