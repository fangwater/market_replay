//! Snapshot backfill: stream each `<snapshot>/data/<RIC>.parquet` and publish
//! one `kline_ib.parquet` per UTC day of `ts_utc_ns`, rows and schema unchanged.
use crate::{
    catalog::Catalog,
    ib_minute::{schema, Job},
    plan::{load_json, now_utc, save_json, Budget, Plan},
    problems::Problems,
    publish::{
        component, day_label, day_path, sampled, DayFile, Outcome, Pending, Pool, Publisher, Tally,
        DAY_NS,
    },
};
use anyhow::{bail, ensure, Context, Result};
use arrow_array::{Array, Int64Array, RecordBatch, StringArray};
use parquet::arrow::{arrow_reader::ParquetRecordBatchReaderBuilder, ProjectionMask};
use serde::{Deserialize, Serialize};
use std::{
    fs::File,
    os::unix::fs::MetadataExt,
    path::{Path, PathBuf},
    sync::{
        atomic::{AtomicUsize, Ordering},
        Arc, Mutex,
    },
    time::Instant,
};

pub const READ_BATCH: usize = 8192;

#[derive(Serialize, Deserialize, Clone, Debug, Default)]
pub struct RicReport {
    pub ric: String,
    pub con_id: String,
    pub exchange: String,
    pub basis: String,
    pub rows: u64,
    pub days: u64,
    pub tally: Tally,
    /// Existing different day files replaced (backup kept locally).
    pub replaced_days: Vec<String>,
    /// Existing files with rows newer than the snapshot cutoff, left untouched.
    pub kept_newer_days: Vec<String>,
    pub failed_days: Vec<String>,
    pub seconds: f64,
}

#[derive(Serialize, Deserialize)]
struct Checkpoint {
    delivery_root: PathBuf,
    source: PathBuf,
    source_size: u64,
    source_mtime_ns: i64,
    cutoff_id: i64,
    report: RicReport,
}

pub struct RunContext {
    pub delivery_root: PathBuf,
    pub state_dir: PathBuf,
    pub publisher: Arc<Publisher>,
    pub pool: Arc<Pool>,
    pub problems: Arc<Problems>,
    pub budget: Budget,
}

/// Day starts of a strictly increasing `ts_utc_ns` column (cheap single-column pass).
fn day_starts(source: &Path) -> Result<(Vec<i64>, u64)> {
    let b = ParquetRecordBatchReaderBuilder::try_new(File::open(source)?)?;
    let index = b.schema().index_of("ts_utc_ns")?;
    let mask = ProjectionMask::roots(b.parquet_schema(), [index]);
    let reader = b.with_projection(mask).with_batch_size(65536).build()?;
    let mut days = Vec::new();
    let mut previous: Option<i64> = None;
    let mut rows = 0u64;
    for batch in reader {
        let batch = batch?;
        let ts = batch
            .column(0)
            .as_any()
            .downcast_ref::<Int64Array>()
            .context("ts_utc_ns type")?;
        ensure!(
            ts.null_count() == 0,
            "null ts_utc_ns in {}",
            source.display()
        );
        for &t in ts.values().iter() {
            ensure!(
                previous.is_none_or(|p| t > p),
                "ts_utc_ns not strictly increasing in {}",
                source.display()
            );
            previous = Some(t);
            let d = t.div_euclid(DAY_NS) * DAY_NS;
            if days.last() != Some(&d) {
                days.push(d);
            }
        }
        rows += batch.num_rows() as u64;
    }
    Ok((days, rows))
}

pub fn absorb(
    report: &mut RicReport,
    done: Vec<crate::publish::Done>,
    problems: &Problems,
    con_id: &str,
    newer_is_problem: bool,
) {
    for d in done {
        let day = day_label(d.day_start).unwrap_or_default();
        match d.result {
            Ok(o) => {
                report.tally.rows += d.rows;
                report.tally.sampled_readbacks += u64::from(d.sampled);
                match o {
                    Outcome::Created => report.tally.created += 1,
                    Outcome::Identical => report.tally.identical += 1,
                    Outcome::Replaced => {
                        report.tally.replaced += 1;
                        report.replaced_days.push(day.clone());
                    }
                    Outcome::KeptNewer => {
                        report.tally.kept_newer += 1;
                        report.kept_newer_days.push(day.clone());
                        if newer_is_problem {
                            problems.record(
                                "existing_newer_than_source",
                                &report.ric,
                                con_id,
                                &day,
                                &d.path.display().to_string(),
                            );
                            continue;
                        }
                    }
                }
                problems.clear_day(&report.ric, &day);
            }
            Err(e) => {
                report.failed_days.push(day.clone());
                problems.record(
                    "publication_failure",
                    &report.ric,
                    con_id,
                    &day,
                    &format!("{}: {e:#}", d.path.display()),
                );
            }
        }
    }
}

fn process(
    ctx: &RunContext,
    job: &Job,
    exchange: &str,
    basis: &str,
    source: &Path,
    cutoff: i64,
) -> Result<RicReport> {
    let started = Instant::now();
    let meta = std::fs::metadata(source)
        .with_context(|| format!("snapshot source {}", source.display()))?;
    let checkpoint_path = ctx
        .state_dir
        .join("snapshot")
        .join(format!("{}.json", component(&job.ric)));
    if let Some(c) = load_json::<Checkpoint>(&checkpoint_path)? {
        if c.delivery_root == ctx.delivery_root
            && c.source == source
            && c.source_size == meta.len()
            && c.source_mtime_ns == meta.mtime() * 1_000_000_000 + meta.mtime_nsec()
            && c.cutoff_id == cutoff
            && c.report.failed_days.is_empty()
        {
            let mut r = c.report;
            r.seconds = 0.0;
            r.basis = format!("{} (checkpoint reused)", r.basis);
            return Ok(r);
        }
    }
    let expected = schema(job);
    let b = ParquetRecordBatchReaderBuilder::try_new(File::open(source)?)?;
    ensure!(
        b.schema().as_ref() == expected.as_ref(),
        "snapshot schema/metadata differs from plan job for {}",
        job.ric
    );
    let (days, rows) = day_starts(source)?;
    ensure!(
        rows == b.metadata().file_metadata().num_rows() as u64,
        "row count mismatch {}",
        source.display()
    );
    let reader = b.with_batch_size(READ_BATCH).build()?;
    let mut report = RicReport {
        ric: job.ric.clone(),
        con_id: job.con_id.clone(),
        exchange: exchange.into(),
        basis: basis.into(),
        rows,
        days: days.len() as u64,
        ..Default::default()
    };
    let mut pending = Pending::new(ctx.pool.clone(), ctx.publisher.clone());
    let mut current: Vec<RecordBatch> = Vec::new();
    let mut current_day: Option<i64> = None;
    let mut index = 0usize;
    let ric_index = expected.index_of("ric")?;
    let ts_index = expected.index_of("ts_utc_ns")?;
    let emit = |day: i64,
                parts: &mut Vec<RecordBatch>,
                index: &mut usize,
                pending: &mut Pending|
     -> Result<()> {
        ensure!(
            days.get(*index) == Some(&day),
            "day sequence differs between passes for {}",
            job.ric
        );
        let batch = arrow_select::concat::concat_batches(&expected, parts.iter())?;
        parts.clear();
        pending.submit(DayFile {
            path: day_path(&ctx.delivery_root, exchange, &job.ric, day)?,
            ric: job.ric.clone(),
            day_start: day,
            batch,
            max_source_id: cutoff,
            sampled: sampled(days.len(), *index),
        })?;
        *index += 1;
        Ok(())
    };
    for batch in reader {
        let batch = batch?;
        let rics = batch
            .column(ric_index)
            .as_any()
            .downcast_ref::<StringArray>()
            .context("ric type")?;
        ensure!(
            rics.null_count() == 0 && rics.iter().all(|r| r == Some(job.ric.as_str())),
            "foreign RIC rows in {}",
            source.display()
        );
        let ts = batch
            .column(ts_index)
            .as_any()
            .downcast_ref::<Int64Array>()
            .context("ts type")?;
        let mut start = 0usize;
        for i in 0..batch.num_rows() {
            let d = ts.value(i).div_euclid(DAY_NS) * DAY_NS;
            if current_day != Some(d) {
                if i > start {
                    current.push(batch.slice(start, i - start));
                }
                start = i;
                if let Some(prev) = current_day {
                    emit(prev, &mut current, &mut index, &mut pending)?;
                }
                current_day = Some(d);
            }
        }
        if batch.num_rows() > start {
            current.push(batch.slice(start, batch.num_rows() - start));
        }
    }
    if let Some(prev) = current_day {
        emit(prev, &mut current, &mut index, &mut pending)?;
    }
    ensure!(
        index == days.len(),
        "emitted {index} of {} days for {}",
        days.len(),
        job.ric
    );
    let done = pending.finish()?;
    absorb(&mut report, done, &ctx.problems, &job.con_id, false);
    report.seconds = started.elapsed().as_secs_f64();
    ctx.budget.check()?;
    if report.failed_days.is_empty() {
        save_json(
            &checkpoint_path,
            &Checkpoint {
                delivery_root: ctx.delivery_root.clone(),
                source: source.into(),
                source_size: meta.len(),
                source_mtime_ns: meta.mtime() * 1_000_000_000 + meta.mtime_nsec(),
                cutoff_id: cutoff,
                report: report.clone(),
            },
        )?;
    }
    Ok(report)
}

pub struct Selection {
    pub rics: Vec<String>,
    pub max_rics: Option<usize>,
}

pub fn run(
    ctx: &RunContext,
    snapshot_dir: &Path,
    catalog: &Catalog,
    select: &Selection,
    workers: usize,
) -> Result<serde_json::Value> {
    let started = Instant::now();
    let plan = Plan::load(&snapshot_dir.join("_audit/contract_plan.json"))?;
    let mut jobs: Vec<Job> = plan.jobs.clone();
    jobs.sort_by(|a, b| a.ric.cmp(&b.ric));
    if !select.rics.is_empty() {
        for r in &select.rics {
            ensure!(
                jobs.iter().any(|j| &j.ric == r),
                "RIC not in snapshot plan: {r}"
            );
        }
        jobs.retain(|j| select.rics.contains(&j.ric));
    }
    if let Some(n) = select.max_rics {
        jobs.truncate(n);
    }
    ensure!(!jobs.is_empty(), "no snapshot RICs selected");
    let mut work = Vec::new();
    let mut entries = Vec::new();
    let mut unresolved = 0;
    for j in jobs {
        match catalog.exchange(&j) {
            Ok((e, basis)) => {
                entries.push(crate::manifest::Entry::new(&j, &e, &basis));
                work.push((j, e, format!("{basis:?}")))
            }
            Err(reason) => {
                unresolved += 1;
                ctx.problems
                    .record("unresolved_exchange", &j.ric, &j.con_id, "", &reason);
            }
        }
    }
    let next = AtomicUsize::new(0);
    let reports = Mutex::new(Vec::new());
    let failures = Mutex::new(Vec::new());
    std::thread::scope(|scope| {
        for w in 0..workers.min(work.len()).max(1) {
            let (work, next, reports, failures) = (&work, &next, &reports, &failures);
            std::thread::Builder::new()
                .name(format!("ib-snapshot-{w}"))
                .spawn_scoped(scope, move || loop {
                    let i = next.fetch_add(1, Ordering::Relaxed);
                    let Some((job, exchange, basis)) = work.get(i) else { break };
                    let source = snapshot_dir.join("data").join(format!("{}.parquet", job.ric));
                    match process(ctx, job, exchange, basis, &source, plan.cutoff_id) {
                        Ok(r) => {
                            if !r.failed_days.is_empty() {
                                failures.lock().unwrap().push(format!("{}: {} day files failed", r.ric, r.failed_days.len()));
                            }
                            eprintln!(
                                "ric={} exchange={} rows={} days={} created={} identical={} replaced={} kept_newer={} failed={} seconds={:.2}",
                                r.ric, r.exchange, r.rows, r.days, r.tally.created, r.tally.identical, r.tally.replaced,
                                r.tally.kept_newer, r.failed_days.len(), r.seconds
                            );
                            reports.lock().unwrap().push(r);
                        }
                        Err(e) => {
                            let message = format!("{e:#}");
                            if message.contains("resource budget exceeded") {
                                next.store(usize::MAX / 2, Ordering::Relaxed);
                            }
                            ctx.problems.record("ric_failure", &job.ric, &job.con_id, "", &message);
                            failures.lock().unwrap().push(format!("{}: {message}", job.ric));
                        }
                    }
                })
                .expect("spawn snapshot worker");
        }
    });
    let reports = reports.into_inner().unwrap();
    let failures = failures.into_inner().unwrap();
    let mut total = Tally::default();
    for r in &reports {
        total.add(&r.tally);
    }
    ctx.problems.flush(&ctx.delivery_root, &now_utc())?;
    let published: std::collections::HashSet<&str> = reports
        .iter()
        .filter(|r| r.failed_days.is_empty())
        .map(|r| r.ric.as_str())
        .collect();
    crate::manifest::merge(
        &ctx.state_dir,
        entries
            .into_iter()
            .filter(|e| published.contains(e.ric.as_str())),
    )?;
    let summary = serde_json::json!({
        "mode": "snapshot",
        "delivery_root": ctx.delivery_root,
        "snapshot_cutoff_id": plan.cutoff_id,
        "rics_selected": work.len() + unresolved,
        "rics_done": reports.len(),
        "rics_unresolved_exchange": unresolved,
        "rics_failed": failures.len(),
        "rows": reports.iter().map(|r| r.rows).sum::<u64>(),
        "day_files": reports.iter().map(|r| r.days).sum::<u64>(),
        "tally": total,
        "replaced": reports.iter().filter(|r| !r.replaced_days.is_empty()).map(|r| (r.ric.clone(), r.replaced_days.clone())).collect::<std::collections::BTreeMap<_,_>>(),
        "kept_newer": reports.iter().filter(|r| !r.kept_newer_days.is_empty()).map(|r| (r.ric.clone(), r.kept_newer_days.clone())).collect::<std::collections::BTreeMap<_,_>>(),
        "problems": ctx.problems.kinds(),
        "failures": failures,
        "seconds": started.elapsed().as_secs_f64(),
        "peak_rss_bytes": Budget::peak_rss_bytes(),
    });
    if !summary["failures"].as_array().unwrap().is_empty() {
        println!("{}", serde_json::to_string_pretty(&summary)?);
        bail!(
            "{} RICs failed; see problems file",
            summary["failures"].as_array().unwrap().len()
        );
    }
    Ok(summary)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::{
        ib_minute::{
            batch,
            testutil::{job, row},
        },
        publish::{read_all, tests::temp_root},
    };
    use parquet::{arrow::ArrowWriter, file::properties::WriterProperties};

    #[test]
    fn utc_day_split_keeps_rows_schema_and_checkpoints() {
        let root = temp_root("snap_root");
        let state = temp_root("snap_state");
        let src_dir = temp_root("snap_src");
        let j = job("ESZ26");
        let s = schema(&j);
        // 8 UTC days x 1440 minutes, gaps on day 3, crossing the 8192-row read batch
        // and 1000-row source row groups at arbitrary points.
        let mut rows = Vec::new();
        let mut id = 0;
        for d in 0..8i64 {
            for m in 0..1440i64 {
                if d == 3 && m % 7 != 0 {
                    continue;
                }
                id += 1;
                let ts = crate::ib_minute::time_ns("2026-10-27 00:00:00").unwrap()
                    + d * DAY_NS
                    + m * 60_000_000_000;
                rows.push(row(
                    id,
                    &chrono::DateTime::from_timestamp_nanos(ts)
                        .format("%Y-%m-%d %H:%M:%S")
                        .to_string(),
                ));
            }
        }
        let all = batch(&rows, &j, s.clone()).unwrap();
        let source = src_dir.join("ESZ26.parquet");
        let props = WriterProperties::builder()
            .set_max_row_group_size(1000)
            .build();
        let mut w =
            ArrowWriter::try_new(File::create(&source).unwrap(), s.clone(), Some(props)).unwrap();
        w.write(&all).unwrap();
        w.close().unwrap();
        let ctx = RunContext {
            delivery_root: root.clone(),
            state_dir: state.clone(),
            publisher: Arc::new(Publisher {
                delivery_root: root.clone(),
                backup_root: state.join("backups"),
            }),
            pool: Pool::new(3).unwrap(),
            problems: Arc::new(Problems::default()),
            budget: Budget {
                max_rss_bytes: 4 << 30,
                max_threads: 32,
            },
        };
        // Source ids above the declared cutoff: every day fails, is a problem, no checkpoint.
        let checkpoint = state.join("snapshot/ESZ26.json");
        let bad = process(&ctx, &j, "cme", "Ric", &source, 1000).unwrap();
        assert_eq!(bad.failed_days.len(), 8);
        assert_eq!(ctx.problems.kinds()["publication_failure"], 8);
        assert!(!checkpoint.exists());
        assert!(!root
            .join("cme")
            .join("2026")
            .join("ESZ26")
            .join("20261027")
            .join("kline_ib.parquet")
            .exists());
        // Cutoff at or above every id: publishes.
        let ctx_ok = &ctx;
        let r = process(ctx_ok, &j, "cme", "Ric", &source, id).unwrap();
        assert_eq!(
            (r.rows, r.days, r.tally.created, r.tally.sampled_readbacks),
            (rows.len() as u64, 8, 8, 3)
        );
        let ts = all
            .column_by_name("ts_utc_ns")
            .unwrap()
            .as_any()
            .downcast_ref::<Int64Array>()
            .unwrap();
        let mut seen = 0;
        for d in 0..8i64 {
            let start = crate::ib_minute::time_ns("2026-10-27 00:00:00").unwrap() + d * DAY_NS;
            let idx: Vec<usize> = (0..ts.len())
                .filter(|i| ts.value(*i).div_euclid(DAY_NS) * DAY_NS == start)
                .collect();
            let want = all.slice(idx[0], idx.len());
            let got = read_all(&day_path(&root, "cme", "ESZ26", start).unwrap()).unwrap();
            assert_eq!(got, want, "day {d}");
            seen += got.num_rows();
        }
        assert_eq!(seen, rows.len());
        // Same source again: checkpoint reused, nothing touched.
        let again = process(ctx_ok, &j, "cme", "Ric", &source, id).unwrap();
        assert!(again.basis.contains("checkpoint"));
        // Without checkpoint: every existing file is identical, nothing replaced.
        std::fs::remove_file(&checkpoint).unwrap();
        let again = process(ctx_ok, &j, "cme", "Ric", &source, id).unwrap();
        assert_eq!(
            (
                again.tally.identical,
                again.tally.created,
                again.tally.replaced
            ),
            (8, 0, 0)
        );
        // A plan job whose metadata differs from the source schema is refused.
        let mut other = j.clone();
        other.cutoff_id = 7;
        std::fs::remove_file(&checkpoint).unwrap();
        assert!(process(ctx_ok, &other, "cme", "Ric", &source, id).is_err());
        for p in [root, state, src_dir] {
            std::fs::remove_dir_all(p).unwrap();
        }
    }
}
