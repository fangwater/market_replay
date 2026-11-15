//! Daily CN futures post-market job.
//!
//! For every complete trading day not yet processed:
//!   1. `cn_futures_l2`                      Tonglian msg -> production RocksDB (done days are skipped)
//!   2. `export_baseline_1min`               RocksDB -> staging parquet (untrimmed)
//!   3. `trim_cn_futures_continuous_sessions` staging -> production baseline_data_1min
//!      (fixed session snapshot, the one recorded in the archive's _provenance)
//!   4. `fill_1min_size_buckets`             month-to-date size buckets (whole-month thresholds)
//!   5. audit + DingTalk summary; any failure stops the run and is notified.

use anyhow::{bail, ensure, Context, Result};
use chrono::{Datelike, NaiveDate, NaiveTime, Utc};
use chrono_tz::Asia::Shanghai;
use clap::{Parser, Subcommand};
use cn_futures_l2::export_1min::read_day_parquet;
use cn_futures_l2::multipliers::{load_multiplier_catalog, ProductMultiplier};
use serde::{Deserialize, Serialize};
use std::collections::{BTreeMap, BTreeSet, HashMap};
use std::fs::{self, File, OpenOptions};
use std::io::{BufRead, BufReader, Write};
use std::path::{Path, PathBuf};
use std::process::{Command, Stdio};
use std::time::Instant;

/// CFFEX trades day sessions only: its file marks a directory of an open day.
const DAY_MARKER: &str = "mdl_21_1_0.csv";

#[derive(Debug, Clone, Deserialize)]
#[serde(deny_unknown_fields)]
struct Config {
    msg_root: PathBuf,
    rocksdb_dir: PathBuf,
    /// Production baseline_data_1min (trim output root, holds _provenance).
    baseline_root: PathBuf,
    /// Untrimmed export staging; day files are removed after success.
    staging_root: PathBuf,
    log_dir: PathBuf,
    state_file: PathBuf,
    /// Directory with the pipeline binaries; default: next to cn-daily.
    #[serde(default)]
    bin_dir: Option<PathBuf>,
    session_api: String,
    /// Must equal the snapshot in baseline_root/_provenance (trim refuses otherwise).
    session_as_of: String,
    #[serde(default = "d_replay_workers")]
    replay_workers: usize,
    #[serde(default = "d_workers")]
    export_workers: usize,
    #[serde(default = "d_workers")]
    fill_workers: usize,
    /// Shanghai wall time after which the current day counts as complete.
    #[serde(default = "d_ready")]
    ready_after: String,
    #[serde(default = "d_host")]
    host_tag: String,
    dingtalk_webhook_env: String,
    dingtalk_secret_env: String,
}
fn d_replay_workers() -> usize {
    32
}
fn d_workers() -> usize {
    8
}
fn d_ready() -> String {
    "16:00".into()
}
fn d_host() -> String {
    "market_replay".into()
}

#[derive(Debug, Default, Serialize, Deserialize)]
struct State {
    last_done: Option<NaiveDate>,
    #[serde(default)]
    history: Vec<RunRecord>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
struct RunRecord {
    day: NaiveDate,
    finished_utc: String,
    ok: bool,
    detail: String,
}

#[derive(Parser)]
#[command(
    name = "cn-daily",
    about = "Daily CN futures RocksDB + baseline_data_1min job"
)]
struct Cli {
    #[command(subcommand)]
    cmd: Cmd,
}

#[derive(Subcommand)]
enum Cmd {
    /// Process pending complete trading days (default), or an explicit range.
    Run {
        #[arg(long)]
        config: PathBuf,
        /// First day to process when there is no state yet (or to reprocess).
        #[arg(long)]
        start: Option<String>,
        #[arg(long)]
        end: Option<String>,
        /// Re-trim even if the production file exists.
        #[arg(long)]
        overwrite: bool,
        #[arg(long)]
        no_notify: bool,
        /// Print the plan only.
        #[arg(long)]
        dry_run: bool,
    },
    /// Audit already-produced days without running the pipeline.
    Audit {
        #[arg(long)]
        config: PathBuf,
        #[arg(long)]
        day: String,
    },
    /// Show state and the next pending days.
    Status {
        #[arg(long)]
        config: PathBuf,
    },
    /// Send a test DingTalk message.
    TestNotify {
        #[arg(long)]
        config: PathBuf,
    },
}

fn parse_day(s: &str) -> Result<NaiveDate> {
    let c = s.trim().replace('-', "");
    NaiveDate::parse_from_str(&c, "%Y%m%d").with_context(|| format!("invalid date {s:?}"))
}

fn load_config(path: &Path) -> Result<Config> {
    let cfg: Config =
        toml::from_str(&fs::read_to_string(path).with_context(|| format!("read {path:?}"))?)
            .with_context(|| format!("parse {path:?}"))?;
    ensure!(
        cfg.replay_workers > 0 && cfg.export_workers > 0 && cfg.fill_workers > 0,
        "workers must be positive"
    );
    ensure!(
        cfg.staging_root != cfg.baseline_root,
        "staging_root must differ from baseline_root"
    );
    parse_day(&cfg.session_as_of)?;
    NaiveTime::parse_from_str(&cfg.ready_after, "%H:%M").context("ready_after must be HH:MM")?;
    Ok(cfg)
}

fn load_state(path: &Path) -> Result<State> {
    if !path.exists() {
        return Ok(State::default());
    }
    serde_json::from_slice(&fs::read(path)?).with_context(|| format!("parse {path:?}"))
}

fn save_state(path: &Path, state: &State) -> Result<()> {
    if let Some(p) = path.parent() {
        fs::create_dir_all(p)?;
    }
    let tmp = path.with_extension("json.tmp");
    fs::write(&tmp, serde_json::to_vec_pretty(state)?)?;
    fs::rename(&tmp, path)?;
    Ok(())
}

/// Natural-day directories that hold a full open day.
fn trading_days(msg_root: &Path) -> Result<Vec<NaiveDate>> {
    let mut days: Vec<NaiveDate> = fs::read_dir(msg_root)
        .with_context(|| format!("list {msg_root:?}"))?
        .filter_map(|e| e.ok())
        .filter_map(|e| {
            let name = e.file_name().into_string().ok()?;
            let day = NaiveDate::parse_from_str(&name, "%Y%m%d").ok()?;
            e.path().join(DAY_MARKER).exists().then_some(day)
        })
        .collect();
    days.sort_unstable();
    Ok(days)
}

fn latest_complete(days: &[NaiveDate], ready_after: NaiveTime) -> Option<NaiveDate> {
    let now = Utc::now().with_timezone(&Shanghai);
    let today = now.date_naive();
    days.iter()
        .rev()
        .copied()
        .find(|d| *d < today || (*d == today && now.time() >= ready_after))
}

fn pending(
    cfg: &Config,
    state: &State,
    start: Option<NaiveDate>,
    end: Option<NaiveDate>,
) -> Result<Vec<NaiveDate>> {
    let days = trading_days(&cfg.msg_root)?;
    let ready = NaiveTime::parse_from_str(&cfg.ready_after, "%H:%M")?;
    let latest = latest_complete(&days, ready).context("no complete trading day on NFS")?;
    let end = end.unwrap_or(latest).min(latest);
    let from = match (start, state.last_done) {
        (Some(s), _) => s,
        (None, Some(last)) => last.succ_opt().context("date overflow")?,
        (None, None) => bail!("no state yet: pass --start for the first run"),
    };
    Ok(days
        .into_iter()
        .filter(|d| *d >= from && *d <= end)
        .collect())
}

/// A processed day: audit plus per-step timings (seconds).
type Done = (NaiveDate, DayAudit, Vec<(&'static str, f64)>);

struct Step {
    name: &'static str,
    bin: &'static str,
    args: Vec<String>,
}

fn steps(cfg: &Config, day: NaiveDate, overwrite: bool) -> Vec<Step> {
    let d = day.format("%Y-%m-%d").to_string();
    let p = |p: &Path| p.display().to_string();
    let mut trim = vec![
        "--dataset".into(),
        "baseline1min".into(),
        "--baseline-in-root".into(),
        p(&cfg.staging_root),
        "--baseline-out-root".into(),
        p(&cfg.baseline_root),
        "--start".into(),
        d.clone(),
        "--end".into(),
        d.clone(),
        "--session-api".into(),
        cfg.session_api.clone(),
        "--session-as-of".into(),
        cfg.session_as_of.clone(),
        "--workers".into(),
        cfg.export_workers.to_string(),
    ];
    if overwrite {
        trim.push("--overwrite".into());
    }
    vec![
        Step {
            name: "回放",
            bin: "cn_futures_l2",
            args: vec![
                "--msg-root".into(),
                p(&cfg.msg_root),
                "--rocksdb-dir".into(),
                p(&cfg.rocksdb_dir),
                "--start".into(),
                d.clone(),
                "--end".into(),
                d.clone(),
                "--workers".into(),
                cfg.replay_workers.to_string(),
            ],
        },
        Step {
            name: "导出",
            bin: "export_baseline_1min",
            args: vec![
                "--rocksdb-dir".into(),
                p(&cfg.rocksdb_dir),
                "--out-root".into(),
                p(&cfg.staging_root),
                "--start".into(),
                d.clone(),
                "--end".into(),
                d.clone(),
                "--workers".into(),
                cfg.export_workers.to_string(),
                "--overwrite".into(),
            ],
        },
        Step {
            name: "时段裁剪",
            bin: "trim_cn_futures_continuous_sessions",
            args: trim,
        },
    ]
}

/// Size buckets use whole-calendar-month thresholds: re-fill month-to-date
/// once per month touched by this run (cheap for daily runs, linear for backfills).
fn fill_step(cfg: &Config, month_start: NaiveDate, end: NaiveDate) -> Step {
    Step {
        name: "大小单分桶",
        bin: "fill_1min_size_buckets",
        args: vec![
            "--rocksdb-dir".into(),
            cfg.rocksdb_dir.display().to_string(),
            "--root".into(),
            cfg.baseline_root.display().to_string(),
            "--start".into(),
            month_start.format("%Y-%m-%d").to_string(),
            "--end".into(),
            end.format("%Y-%m-%d").to_string(),
            "--workers".into(),
            cfg.fill_workers.to_string(),
        ],
    }
}

/// (month start, last processed day) per calendar month, ascending.
fn fill_ranges(days: &[NaiveDate]) -> Vec<(NaiveDate, NaiveDate)> {
    let mut out: BTreeMap<NaiveDate, NaiveDate> = BTreeMap::new();
    for d in days {
        let m = d.with_day(1).expect("day 1");
        let e = out.entry(m).or_insert(*d);
        *e = (*e).max(*d);
    }
    out.into_iter().collect()
}

fn bin_dir(cfg: &Config) -> Result<PathBuf> {
    match &cfg.bin_dir {
        Some(d) => Ok(d.clone()),
        None => Ok(std::env::current_exe()?
            .parent()
            .context("exe has no dir")?
            .to_path_buf()),
    }
}

/// Runs one step, appending stdout/stderr to the day log; returns seconds.
fn run_step(bin: &Path, step: &Step, log: &Path) -> Result<f64> {
    let t = Instant::now();
    let mut f = OpenOptions::new().create(true).append(true).open(log)?;
    writeln!(
        f,
        "\n=== {} {} {}",
        Utc::now().to_rfc3339(),
        step.bin,
        step.args.join(" ")
    )?;
    let status = Command::new(bin.join(step.bin))
        .args(&step.args)
        .stdin(Stdio::null())
        .stdout(f.try_clone()?)
        .stderr(f.try_clone()?)
        .status()
        .with_context(|| format!("spawn {}", step.bin))?;
    ensure!(status.success(), "{} exited with {status}", step.bin);
    Ok(t.elapsed().as_secs_f64())
}

fn tail(path: &Path, n: usize) -> String {
    let Ok(f) = File::open(path) else {
        return String::new();
    };
    let lines: Vec<String> = BufReader::new(f).lines().map_while(Result::ok).collect();
    lines[lines.len().saturating_sub(n)..].join("\n")
}

#[derive(Debug, Default, Serialize)]
struct DayAudit {
    day: NaiveDate,
    files: usize,
    rows: usize,
    traded_minutes: usize,
    contracts: usize,
    per_exchange: BTreeMap<String, usize>,
    products: BTreeSet<String>,
    vwap_checked: usize,
    vwap_outside: usize,
    no_multiplier: BTreeSet<String>,
    missing_vs_prev: Vec<String>,
    new_vs_prev: Vec<String>,
}

fn day_files(root: &Path, day: NaiveDate) -> Result<Vec<(String, String, PathBuf)>> {
    let name = format!("{}.parquet", day.format("%Y%m%d"));
    let mut out = Vec::new();
    if !root.exists() {
        return Ok(out);
    }
    for ex in fs::read_dir(root)?.filter_map(|e| e.ok()) {
        let exn = ex.file_name().to_string_lossy().into_owned();
        if exn.starts_with('_') || !ex.path().is_dir() {
            continue;
        }
        for prod in fs::read_dir(ex.path())?.filter_map(|e| e.ok()) {
            let f = prod.path().join(&name);
            if f.exists() {
                out.push((
                    exn.clone(),
                    prod.file_name().to_string_lossy().into_owned(),
                    f,
                ));
            }
        }
    }
    out.sort();
    Ok(out)
}

fn audit(
    cfg: &Config,
    day: NaiveDate,
    prev: Option<NaiveDate>,
    catalog: &HashMap<String, ProductMultiplier>,
) -> Result<DayAudit> {
    let mut a = DayAudit {
        day,
        ..DayAudit::default()
    };
    let mut contracts = BTreeSet::new();
    for (ex, product, path) in day_files(&cfg.baseline_root, day)? {
        let rows = read_day_parquet(&path)?;
        a.files += 1;
        *a.per_exchange.entry(ex).or_default() += 1;
        a.products.insert(product.clone());
        let mult = catalog
            .get(&product.to_ascii_uppercase())
            .filter(|m| m.verified && m.volume_multiple > 0.0)
            .map(|m| m.volume_multiple);
        if mult.is_none() {
            a.no_multiplier.insert(product.clone());
        }
        for r in &rows {
            a.rows += 1;
            contracts.insert(r.contract_id.clone());
            if r.volume <= 0.0 {
                continue;
            }
            a.traded_minutes += 1;
            if let (Some(m), Some(h), Some(l)) = (mult, r.high, r.low) {
                let vwap = r.amount / r.volume / m;
                let tol = h.abs().max(1.0) * 1e-8;
                a.vwap_checked += 1;
                if !(vwap >= l - tol && vwap <= h + tol) {
                    a.vwap_outside += 1;
                }
            }
        }
    }
    a.contracts = contracts.len();
    if let Some(prev) = prev {
        let before: BTreeSet<String> = day_files(&cfg.baseline_root, prev)?
            .into_iter()
            .map(|(_, p, _)| p)
            .collect();
        if !before.is_empty() {
            a.missing_vs_prev = before.difference(&a.products).cloned().collect();
            a.new_vs_prev = a.products.difference(&before).cloned().collect();
        }
    }
    Ok(a)
}

fn summarize(a: &DayAudit, timings: &[(&str, f64)]) -> String {
    let pct = if a.vwap_checked > 0 {
        a.vwap_outside as f64 * 100.0 / a.vwap_checked as f64
    } else {
        0.0
    };
    let ex: Vec<String> = a
        .per_exchange
        .iter()
        .map(|(k, v)| format!("{k} {v}"))
        .collect();
    let mut s = format!(
        "TradDay {}：{} 个品种文件 / {} 行 / 有成交分钟 {} / 合约 {}\n各所：{}\nVWAP 落在快照高低价外：{:.2}%（{}/{}）",
        a.day, a.files, a.rows, a.traded_minutes, a.contracts, ex.join("，"), pct, a.vwap_outside, a.vwap_checked
    );
    if !a.missing_vs_prev.is_empty() {
        s.push_str(&format!(
            "\n⚠ 比上一交易日缺少品种：{}",
            a.missing_vs_prev.join(" ")
        ));
    }
    if !a.new_vs_prev.is_empty() {
        s.push_str(&format!("\n新增品种：{}", a.new_vs_prev.join(" ")));
    }
    if !a.no_multiplier.is_empty() {
        s.push_str(&format!(
            "\n⚠ 无已验证乘数：{}",
            a.no_multiplier
                .iter()
                .cloned()
                .collect::<Vec<_>>()
                .join(" ")
        ));
    }
    if !timings.is_empty() {
        let t: Vec<String> = timings
            .iter()
            .map(|(n, s)| format!("{n} {s:.0}s"))
            .collect();
        s.push_str(&format!("\n耗时：{}", t.join("，")));
    }
    s
}

fn remove_staging_day(root: &Path, day: NaiveDate) -> Result<usize> {
    let files = day_files(root, day)?;
    for (_, _, f) in &files {
        fs::remove_file(f)?;
    }
    Ok(files.len())
}

fn notify(cfg: &Config, enabled: bool, title: &str, body: &str) {
    let text = format!("[{}] 国内期货 1min 日处理 {title}\n{body}", cfg.host_tag);
    eprintln!("{text}");
    if !enabled {
        return;
    }
    match dingtalk::DingTalk::from_env(&cfg.dingtalk_webhook_env, &cfg.dingtalk_secret_env) {
        Ok(Some(d)) => {
            if let Err(e) = d.send_text(&text) {
                eprintln!("cn-daily: DingTalk send failed: {e:#}");
            }
        }
        Ok(None) => eprintln!(
            "cn-daily: {} unset; DingTalk notification skipped",
            cfg.dingtalk_webhook_env
        ),
        Err(e) => eprintln!("cn-daily: DingTalk config invalid: {e:#}"),
    }
}

fn run(
    cfg: &Config,
    start: Option<NaiveDate>,
    end: Option<NaiveDate>,
    overwrite: bool,
    notify_on: bool,
    dry_run: bool,
) -> Result<()> {
    fs::create_dir_all(&cfg.log_dir)?;
    fs::create_dir_all(&cfg.staging_root)?;
    let lock = File::create(cfg.staging_root.join(".cn_daily.lock"))?;
    if lock.try_lock().is_err() {
        bail!(
            "another cn-daily run holds {:?}",
            cfg.staging_root.join(".cn_daily.lock")
        );
    }
    let mut state = load_state(&cfg.state_file)?;
    let days = pending(cfg, &state, start, end)?;
    let all_days = trading_days(&cfg.msg_root)?;
    eprintln!(
        "cn-daily: pending {:?}",
        days.iter().map(|d| d.to_string()).collect::<Vec<_>>()
    );
    if dry_run {
        for d in &days {
            for s in steps(cfg, *d, overwrite) {
                println!("{d} {} {} {}", s.name, s.bin, s.args.join(" "));
            }
        }
        for (m, e) in fill_ranges(&days) {
            let s = fill_step(cfg, m, e);
            println!("{e} {} {} {}", s.name, s.bin, s.args.join(" "));
        }
        return Ok(());
    }
    if days.is_empty() {
        eprintln!("cn-daily: nothing to do");
        return Ok(());
    }
    let bins = bin_dir(cfg)?;
    let catalog = load_multiplier_catalog().context("load PostgreSQL multipliers")?;
    let mut done: Vec<Done> = Vec::new();
    let summary_of = |done: &[Done]| {
        done.iter()
            .map(|(_, a, t)| summarize(a, t))
            .collect::<Vec<_>>()
            .join("\n\n")
    };
    // Records the failure, notifies (with what already succeeded) and aborts.
    let fail = |state: &mut State,
                day: NaiveDate,
                step: &Step,
                err: anyhow::Error,
                log: &Path,
                done: &[Done]|
     -> Result<()> {
        let detail = format!(
            "{day} {} 失败：{err:#}\n日志 {}\n{}",
            step.name,
            log.display(),
            tail(log, 12)
        );
        state.history.push(RunRecord {
            day,
            finished_utc: Utc::now().to_rfc3339(),
            ok: false,
            detail: detail.clone(),
        });
        save_state(&cfg.state_file, state)?;
        let mut body = summary_of(done);
        if !body.is_empty() {
            body = format!("已完成但未标记（分桶前中断，重跑会自动跳过已完成部分）：\n{body}\n\n");
        }
        body.push_str(&detail);
        if step.bin == "cn_futures_l2" && detail.contains("marked writing") {
            body.push_str(
                "\n需要人工处理：上次回放中断，按 cn_futures_l2 文档 reset 该交易日后重跑。",
            );
        }
        notify(cfg, notify_on, "失败", &body);
        bail!("{day} {} failed", step.name)
    };
    for day in &days {
        let day = *day;
        let log = cfg
            .log_dir
            .join(format!("cn_daily_{}.log", day.format("%Y%m%d")));
        let mut timings = Vec::new();
        for step in steps(cfg, day, overwrite) {
            match run_step(&bins, &step, &log) {
                Ok(secs) => timings.push((step.name, secs)),
                Err(e) => return fail(&mut state, day, &step, e, &log, &done),
            }
        }
        let prev = all_days.iter().rev().copied().find(|d| *d < day);
        let a = audit(cfg, day, prev, &catalog)?;
        ensure!(a.files > 0, "{day}: no production files after trim");
        fs::write(
            cfg.log_dir
                .join(format!("cn_daily_{}_audit.json", day.format("%Y%m%d"))),
            serde_json::to_vec_pretty(&a)?,
        )?;
        eprintln!("cn-daily: {day} replay/export/trim ok");
        done.push((day, a, timings));
    }
    for (month_start, end) in fill_ranges(&days) {
        let step = fill_step(cfg, month_start, end);
        let log = cfg
            .log_dir
            .join(format!("cn_daily_fill_{}.log", end.format("%Y%m%d")));
        match run_step(&bins, &step, &log) {
            Ok(secs) => {
                // Attribute the month's fill time to its last processed day.
                if let Some((_, _, t)) = done.iter_mut().rev().find(|(d, _, _)| *d == end) {
                    t.push((step.name, secs));
                }
            }
            Err(e) => return fail(&mut state, end, &step, e, &log, &done),
        }
    }
    for (day, a, timings) in &done {
        let removed = remove_staging_day(&cfg.staging_root, *day)?;
        eprintln!("cn-daily: {day} done, staging files removed {removed}");
        state.last_done = Some(state.last_done.map_or(*day, |l| l.max(*day)));
        state.history.push(RunRecord {
            day: *day,
            finished_utc: Utc::now().to_rfc3339(),
            ok: true,
            detail: summarize(a, timings),
        });
    }
    let keep = state.history.len().saturating_sub(200);
    state.history.drain(..keep);
    save_state(&cfg.state_file, &state)?;
    // Keep DingTalk messages short: one message per run, newest days last.
    let n = done.len();
    let body = if n > 6 {
        format!(
            "共处理 {n} 个交易日，最近 3 天：\n\n{}",
            summary_of(&done[n - 3..])
        )
    } else {
        summary_of(&done)
    };
    notify(cfg, notify_on, "成功", &body);
    Ok(())
}

fn main() -> Result<()> {
    match Cli::parse().cmd {
        Cmd::Run {
            config,
            start,
            end,
            overwrite,
            no_notify,
            dry_run,
        } => {
            let cfg = load_config(&config)?;
            run(
                &cfg,
                start.as_deref().map(parse_day).transpose()?,
                end.as_deref().map(parse_day).transpose()?,
                overwrite,
                !no_notify,
                dry_run,
            )
        }
        Cmd::Audit { config, day } => {
            let cfg = load_config(&config)?;
            let day = parse_day(&day)?;
            let days = trading_days(&cfg.msg_root)?;
            let prev = days.iter().rev().copied().find(|d| *d < day);
            let a = audit(&cfg, day, prev, &load_multiplier_catalog()?)?;
            println!("{}", summarize(&a, &[]));
            Ok(())
        }
        Cmd::Status { config } => {
            let cfg = load_config(&config)?;
            let state = load_state(&cfg.state_file)?;
            println!("last_done: {:?}", state.last_done);
            for r in state.history.iter().rev().take(5) {
                println!(
                    "{} ok={} {}",
                    r.day,
                    r.ok,
                    r.detail.lines().next().unwrap_or("")
                );
            }
            match pending(&cfg, &state, None, None) {
                Ok(p) => println!(
                    "pending: {:?}",
                    p.iter().map(|d| d.to_string()).collect::<Vec<_>>()
                ),
                Err(e) => println!("pending: {e:#}"),
            }
            Ok(())
        }
        Cmd::TestNotify { config } => {
            let cfg = load_config(&config)?;
            let d =
                dingtalk::DingTalk::from_env(&cfg.dingtalk_webhook_env, &cfg.dingtalk_secret_env)?
                    .with_context(|| format!("{} is not set", cfg.dingtalk_webhook_env))?;
            d.send_text(&format!(
                "[{}] 国内期货 1min 日处理：钉钉连通性测试",
                cfg.host_tag
            ))?;
            println!("sent");
            Ok(())
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn trading_days_require_cffex_marker() {
        let t = tempfile::tempdir().unwrap();
        for (d, marker) in [("20261112", true), ("20261113", true), ("20261114", false)] {
            fs::create_dir_all(t.path().join(d)).unwrap();
            fs::write(
                t.path()
                    .join(d)
                    .join(if marker { DAY_MARKER } else { "mdl_22_1_0.csv" }),
                "",
            )
            .unwrap();
        }
        let days = trading_days(t.path()).unwrap();
        assert_eq!(
            days,
            vec![
                parse_day("20261112").unwrap(),
                parse_day("20261113").unwrap()
            ]
        );
    }

    #[test]
    fn steps_use_month_start_for_size_buckets() {
        let cfg: Config = toml::from_str(
            "msg_root=\"/m\"\nrocksdb_dir=\"/r\"\nbaseline_root=\"/b\"\nstaging_root=\"/s\"\n\
             log_dir=\"/l\"\nstate_file=\"/st.json\"\nsession_api=\"http://x\"\n\
             session_as_of=\"2026-09-08\"\ndingtalk_webhook_env=\"W\"\ndingtalk_secret_env=\"S\"\n",
        )
        .unwrap();
        let s = steps(&cfg, parse_day("20261113").unwrap(), false);
        assert_eq!(
            s.iter().map(|s| s.bin).collect::<Vec<_>>(),
            vec![
                "cn_futures_l2",
                "export_baseline_1min",
                "trim_cn_futures_continuous_sessions",
            ]
        );
        assert!(!s[2].args.contains(&"--overwrite".to_string()));
        let days: Vec<NaiveDate> = ["20261029", "20261030", "20261102", "20261113"]
            .iter()
            .map(|d| parse_day(d).unwrap())
            .collect();
        let r = fill_ranges(&days);
        assert_eq!(
            r,
            vec![
                (
                    parse_day("20261001").unwrap(),
                    parse_day("20261030").unwrap()
                ),
                (
                    parse_day("20261101").unwrap(),
                    parse_day("20261113").unwrap()
                ),
            ]
        );
        let fill = fill_step(&cfg, r[1].0, r[1].1).args;
        let i = fill.iter().position(|a| a == "--start").unwrap();
        assert_eq!(
            (fill[i + 1].as_str(), fill[i + 3].as_str()),
            ("2026-11-01", "2026-11-13")
        );
    }
}
