//! Daily LSEG TAS delivery job (`lseg-daily`).
//!
//! One run, strictly serial, every heavy step a subprocess with fixed workers:
//! 1. inventory: remote `future_daily/normalised/` listing (direct LAN HTTP);
//! 2. discover: formal files not yet delivered (HEAD: size/ETag/Last-Modified);
//! 3. per product: download, `tas_daily_stage`, `cme_tas_replay_all` (fresh
//!    DB), `tas_db_audit`, `lseg_tas_parquet --layout ric-day`, check and
//!    publish trade/quote, `tas_status_parquet`;
//! 4. kline: `lseg_kline run` per affected (year, product) group;
//! 5. open interest: `tas_open_interest sync` of the downloaded daily gzips into
//!    PostgreSQL `lseg_open_interest` (optional `[open_interest]`; only new files);
//! 6. catalog: `lseg_catalog build` (only after a clean run);
//! 7. DingTalk summary; problems only go to `<delivery>/meta/problems/lseg_daily`.
//!
//! State is a local JSON; every stage is resumable and a failed product does
//! not block the others. See docs/lseg/lseg_daily.md.
use anyhow::{bail, ensure, Context, Result};
use chrono::Utc;
use clap::{Parser, Subcommand};
use serde::Deserialize;
use serde_json::{json, Value};
use std::collections::{BTreeMap, BTreeSet, HashMap};
use std::fs::{self, File, OpenOptions};
use std::io::{BufRead, BufReader, Write};
use std::path::{Path, PathBuf};
use std::process::{Command, Stdio};
use std::time::Instant;

mod merge;
mod publish;
mod remote;
mod state;
mod venue;
mod venue_fix;

use state::{Batch, PlannedFile, ProductRun, RunRecord, Stage, State};

const PRODUCTION_ROOT: &str = "/mnt/nfs/30.100_cta_data/lseg";
const SHARD_PREFIX: &str = "shanghai_evolution_futures_time_and_sales_ric_list_0_tas_";

#[derive(Debug, Clone, Deserialize)]
#[serde(deny_unknown_fields)]
struct Workers {
    download: usize,
    stage: usize,
    replay: usize,
    parse: usize,
    export: usize,
    status: usize,
    /// Aggregation workers inside one lseg_kline run.
    kline: usize,
    /// lseg_kline runs (one per year/product group) in parallel; CPU use is
    /// about kline_groups x kline (user budget 2026-11-16: 18 cores).
    #[serde(default = "one")]
    kline_groups: usize,
    /// RocksDB background jobs of the monthly merge (flush + compaction).
    #[serde(default = "two")]
    merge_compaction: i32,
}

fn one() -> usize {
    1
}
fn two() -> i32 {
    2
}

#[derive(Debug, Clone, Deserialize)]
#[serde(deny_unknown_fields)]
struct Config {
    remote_root: String,
    /// Downloads go here (never overwritten).
    raw_root: PathBuf,
    /// Extra read-only roots whose recorded downloads are reused as-is.
    #[serde(default)]
    raw_read_roots: Vec<PathBuf>,
    /// Batches, logs, receipts, kline work, catalog work (local NVMe).
    work_root: PathBuf,
    /// Per-batch RocksDBs: <db_root>/<batch>/<product>/attempt_N.
    db_root: PathBuf,
    /// Monthly merged RocksDBs: <monthly_db_root>/<YYYY-MM>/<product>/attempt_N
    /// (default: `cme_tas_monthly` next to `db_root`).
    #[serde(default)]
    monthly_db_root: Option<PathBuf>,
    /// Delivered root the merge verification must reproduce (read only;
    /// default `delivery_root`).
    #[serde(default)]
    merge_compare_root: Option<PathBuf>,
    /// Local ric-day exports before publication.
    export_root: PathBuf,
    delivery_root: PathBuf,
    /// Must be true to write the production delivery root.
    #[serde(default)]
    production: bool,
    state_file: PathBuf,
    /// lseg_data project root (kline/catalog metadata inputs).
    project: PathBuf,
    column_rules: PathBuf,
    /// Earlier Python runs (downloads/*.json, products/*/completion.json).
    #[serde(default)]
    bootstrap_runs: Vec<PathBuf>,
    /// tas_status plan whose `exchanges` fixes the venue of known 2026 RICs.
    approved_exchanges: PathBuf,
    hrd: Vec<PathBuf>,
    kline_base_plan: PathBuf,
    kline_cache: PathBuf,
    /// Raw source witness store for trade-correction netting
    /// (`lseg_kline witness-import`; correction_policy_version=1).
    kline_witnesses: PathBuf,
    #[serde(default)]
    bin_dir: Option<PathBuf>,
    /// Remote files younger than this (Last-Modified) wait for the next run.
    #[serde(default = "d_settle")]
    settle_minutes: i64,
    /// A date after a missing remote day waits this long for the hole to be
    /// filled; afterwards the hole is recorded as a source gap.
    #[serde(default = "d_hole")]
    hole_wait_hours: i64,
    /// Cap on new files per run (0 = no cap); the rest waits.
    #[serde(default)]
    max_new_files: usize,
    #[serde(default = "d_nice")]
    nice: i32,
    workers: Workers,
    #[serde(default = "d_true")]
    catalog: bool,
    #[serde(default)]
    catalog_extra_args: Vec<String>,
    /// `lseg_catalog build --prune-expired-status` (removes later UTC dates that
    /// hold only redundant post-expiry status files, as production was built).
    #[serde(default)]
    catalog_prune_expired_status: bool,
    #[serde(default = "d_host")]
    host_tag: String,
    /// Link shown at the end of the DingTalk message (the dashboard).
    #[serde(default)]
    dashboard_url: Option<String>,
    dingtalk_webhook_env: String,
    dingtalk_secret_env: String,
    /// Open interest of the daily gzips into PostgreSQL (step skipped when absent).
    #[serde(default)]
    open_interest: Option<OpenInterest>,
}

#[derive(Debug, Clone, Deserialize)]
#[serde(deny_unknown_fields)]
struct OpenInterest {
    /// `market_metadata` over the local socket (peer auth; no secret in the URL).
    database_url: String,
    #[serde(default = "one")]
    workers: usize,
}
fn d_settle() -> i64 {
    60
}
fn d_hole() -> i64 {
    72
}
fn d_nice() -> i32 {
    10
}
fn d_true() -> bool {
    true
}
fn d_host() -> String {
    "market_replay".into()
}

fn load_config(path: &Path) -> Result<Config> {
    let cfg: Config =
        toml::from_str(&fs::read_to_string(path).with_context(|| format!("read {path:?}"))?)
            .with_context(|| format!("parse {path:?}"))?;
    let w = &cfg.workers;
    ensure!(
        (1..=8).contains(&w.download)
            && (1..=16).contains(&w.stage)
            && (2..=16).contains(&w.replay)
            && (1..=16).contains(&w.parse)
            && (1..=8).contains(&w.export)
            && (1..=8).contains(&w.status)
            && (1..=2).contains(&w.kline)
            && (1..=4).contains(&w.merge_compaction),
        "workers out of the fixed budget (replay needs >= 2, kline 1..2, merge_compaction 1..4)"
    );
    ensure!(
        cfg.production || cfg.delivery_root != Path::new(PRODUCTION_ROOT),
        "delivery_root is the production root; set production = true deliberately"
    );
    for p in [
        &cfg.raw_root,
        &cfg.work_root,
        &cfg.db_root,
        &cfg.export_root,
        &cfg.state_file,
        &merge::monthly_root(&cfg),
    ] {
        ensure!(
            p.is_absolute() && !p.starts_with(&cfg.delivery_root),
            "{p:?} must be absolute and off the delivery disk"
        );
    }
    Ok(cfg)
}

#[derive(Parser)]
#[command(name = "lseg-daily", about = "Daily LSEG TAS download -> delivery job")]
struct Cli {
    #[command(subcommand)]
    cmd: Cmd,
}

#[derive(Subcommand)]
enum Cmd {
    /// Resume open batches, then deliver newly published files.
    Run {
        #[arg(long)]
        config: PathBuf,
        /// Only these product directories (comma separated).
        #[arg(long, value_delimiter = ',')]
        products: Vec<String>,
        /// Only these source dates YYYY-MM-DD (comma separated).
        #[arg(long, value_delimiter = ',')]
        dates: Vec<String>,
        /// Do not discover new files; only resume open batches.
        #[arg(long)]
        no_new: bool,
        /// Skip steps: kline, open_interest, catalog (comma separated).
        #[arg(long, value_delimiter = ',')]
        skip: Vec<String>,
        #[arg(long)]
        no_notify: bool,
        /// Inventory + discovery only; print the plan, write nothing.
        #[arg(long)]
        dry_run: bool,
    },
    /// Merge a month's finished daily DBs into one DB per product, verify
    /// (record counts + klines reproduced in staging), then switch the plan.
    Merge {
        #[arg(long)]
        config: PathBuf,
        /// YYYY-MM, or "previous" (the UTC month before today).
        #[arg(long)]
        month: String,
        /// Only these product directories (comma separated).
        #[arg(long, value_delimiter = ',')]
        products: Vec<String>,
        /// List sources and targets only; write nothing.
        #[arg(long)]
        dry_run: bool,
        /// After both verifications pass, delete the retired source DBs.
        #[arg(long)]
        delete_sources: bool,
        #[arg(long)]
        no_notify: bool,
    },
    /// Show state, open batches and gaps.
    Status {
        #[arg(long)]
        config: PathBuf,
    },
    /// Send a DingTalk connectivity test.
    TestNotify {
        #[arg(long)]
        config: PathBuf,
    },
    /// Read-only: resolve every RIC-year under `<delivery>/review/` (rules in venue_fix.rs)
    /// and write `<out>/venue_plan.csv` plus a summary. Changes nothing.
    VenuesPlan {
        #[arg(long)]
        config: PathBuf,
        #[arg(long)]
        out: PathBuf,
    },
    /// Move the resolved RIC-years of a `venues-plan` from `<delivery>/review/` to their
    /// directory (hard links, never overwriting; the review copy is removed only after the
    /// link is verified), then update the driver state and the kline venue overrides.
    /// Without `--apply` it only checks every file and reports.
    VenuesMigrate {
        #[arg(long)]
        config: PathBuf,
        /// `venue_plan.csv` from `venues-plan`.
        #[arg(long)]
        plan: PathBuf,
        /// Local directory for the move journal and the state backup.
        #[arg(long)]
        journal: PathBuf,
        /// Only these RICs (default: every resolved row).
        #[arg(long, value_delimiter = ',')]
        only: Vec<String>,
        #[arg(long)]
        apply: bool,
    },
}

struct Opts {
    products: Vec<String>,
    dates: Vec<String>,
    no_new: bool,
    skip: Vec<String>,
    dry_run: bool,
}

/// Wall time per step name.
#[derive(Default)]
struct Timings(BTreeMap<&'static str, f64>);
impl Timings {
    fn add(&mut self, k: &'static str, s: f64) {
        *self.0.entry(k).or_default() += s;
    }
}

struct Ctx<'a> {
    cfg: &'a Config,
    bins: PathBuf,
    run_id: String,
    approved: Option<HashMap<String, String>>,
    t: Timings,
}

impl Ctx<'_> {
    fn bin(&self, name: &str) -> PathBuf {
        self.bins.join(name)
    }
    fn approved(&mut self) -> Result<&HashMap<String, String>> {
        if self.approved.is_none() {
            let v = read_json(&self.cfg.approved_exchanges)?;
            let m: HashMap<String, String> =
                serde_json::from_value(v["exchanges"].clone()).context("approved exchanges map")?;
            self.approved = Some(m);
        }
        Ok(self.approved.as_ref().unwrap())
    }
    /// Runs one subprocess, appending its output to `log`.
    fn run(&mut self, step: &'static str, bin: &str, args: &[String], log: &Path) -> Result<()> {
        let t = Instant::now();
        let r = run_tool(self.cfg, &self.bin(bin), bin, args, log);
        self.t.add(step, t.elapsed().as_secs_f64());
        r
    }
}

/// One tool subprocess (optionally under `nice`), output appended to `log`.
fn run_tool(cfg: &Config, exe: &Path, bin: &str, args: &[String], log: &Path) -> Result<()> {
    {
        if let Some(p) = log.parent() {
            fs::create_dir_all(p)?;
        }
        let mut f = OpenOptions::new().create(true).append(true).open(log)?;
        writeln!(
            f,
            "\n=== {} {bin} {}",
            Utc::now().to_rfc3339(),
            args.join(" ")
        )?;
        let mut cmd = if cfg.nice > 0 {
            let mut c = Command::new("nice");
            c.arg("-n").arg(cfg.nice.to_string()).arg(exe);
            c
        } else {
            Command::new(exe)
        };
        let status = cmd
            .args(args)
            .stdin(Stdio::null())
            .stdout(f.try_clone()?)
            .stderr(f.try_clone()?)
            .status()
            .with_context(|| format!("spawn {bin}"))?;
        ensure!(
            status.success(),
            "{bin} exited with {status}; log {}\n{}",
            log.display(),
            tail(log, 8)
        );
        Ok(())
    }
}

fn read_json(path: &Path) -> Result<Value> {
    serde_json::from_slice(&fs::read(path).with_context(|| format!("read {path:?}"))?)
        .with_context(|| format!("parse {path:?}"))
}

fn save_json(path: &Path, v: &impl serde::Serialize) -> Result<()> {
    fs::create_dir_all(path.parent().context("parent")?)?;
    let tmp = path.with_extension("json.partial");
    fs::write(&tmp, serde_json::to_vec_pretty(v)?)?;
    File::open(&tmp)?.sync_all()?;
    fs::rename(&tmp, path)?;
    Ok(())
}

fn tail(path: &Path, n: usize) -> String {
    let Ok(f) = File::open(path) else {
        return String::new();
    };
    let mut ring = std::collections::VecDeque::with_capacity(n);
    for l in BufReader::new(f).lines().map_while(Result::ok) {
        if ring.len() == n {
            ring.pop_front();
        }
        ring.push_back(l);
    }
    ring.into_iter().collect::<Vec<_>>().join("\n")
}

fn next_day(d: &str) -> Result<String> {
    Ok(chrono::NaiveDate::parse_from_str(d, "%Y-%m-%d")?
        .succ_opt()
        .context("date overflow")?
        .format("%Y-%m-%d")
        .to_string())
}

fn product_dir(cfg: &Config, batch: &str, product: &str) -> PathBuf {
    cfg.work_root.join("batches").join(batch).join(product)
}

/// Seeds delivered dates / RIC owners from the pinned Python runs.
fn bootstrap(cfg: &Config, st: &mut State) -> Result<()> {
    for run in &cfg.bootstrap_runs {
        for e in fs::read_dir(run.join("downloads"))? {
            let p = e?.path();
            if p.extension().is_none_or(|x| x != "json") {
                continue;
            }
            let v = read_json(&p)?;
            ensure!(
                v["status"] == "all_formal_files_downloaded",
                "{p:?} incomplete"
            );
            let product = v["product"].as_str().context("product")?.to_string();
            let done = read_json(&run.join("products").join(&product).join("completion.json"))?;
            ensure!(
                done["status"]
                    .as_str()
                    .is_some_and(|s| s.starts_with("complete")),
                "{product}: bootstrap run not complete"
            );
            for f in v["files"].as_array().context("files")? {
                // Zero-byte placeholders were gaps, not delivered days.
                if f["bytes"].as_u64() == Some(0) {
                    continue;
                }
                st.delivered
                    .entry(product.clone())
                    .or_default()
                    .insert(f["date"].as_str().context("date")?.to_string());
            }
            for r in done["rics"].as_array().into_iter().flatten() {
                st.owners
                    .insert(r.as_str().context("ric")?.to_string(), product.clone());
            }
        }
    }
    st.bootstrapped = true;
    Ok(())
}

/// A recorded local copy in the raw roots (reused only if unchanged).
fn local_copy(cfg: &Config, product: &str, name: &str) -> Option<PathBuf> {
    std::iter::once(&cfg.raw_root)
        .chain(cfg.raw_read_roots.iter())
        .map(|r| r.join(product).join(name))
        .find(|p| p.exists())
}

struct Discovery {
    planned: BTreeMap<String, Vec<PlannedFile>>,
    unsettled: usize,
    deferred_years: usize,
    capped: usize,
}

fn discover(
    cfg: &Config,
    st: &mut State,
    client: &reqwest::blocking::Client,
    inv: &remote::Inventory,
    opts: &Opts,
) -> Result<Discovery> {
    let now = Utc::now();
    let mut planned: BTreeMap<String, Vec<PlannedFile>> = BTreeMap::new();
    let (mut unsettled, mut capped, mut count, mut held) = (0, 0, 0, 0);
    let age_minutes = |fp: &remote::Fingerprint| {
        fp.last_modified
            .as_deref()
            .and_then(|s| chrono::DateTime::parse_from_rfc2822(s).ok())
            .map(|t| (now - t.with_timezone(&Utc)).num_minutes())
    };
    let day = |d: &str| chrono::NaiveDate::parse_from_str(d, "%Y-%m-%d");
    // Per product: last date delivered or in flight, and whether a newer
    // date must wait (klines of a missing day would be frozen as fill).
    let mut frontier: HashMap<String, Option<chrono::NaiveDate>> = HashMap::new();
    let mut blocked: BTreeSet<String> = BTreeSet::new();
    let explicit = !opts.dates.is_empty();
    for f in &inv.files {
        if st
            .delivered
            .get(&f.product)
            .is_some_and(|d| d.contains(&f.date))
            || st.in_open_batch(&f.product, &f.date)
            || (explicit && !opts.dates.contains(&f.date))
        {
            continue;
        }
        let front = *frontier.entry(f.product.clone()).or_insert_with(|| {
            let delivered = st.delivered.get(&f.product).and_then(|d| d.last().cloned());
            let open = st
                .batches
                .iter()
                .filter_map(|b| b.products.get(&f.product))
                .flat_map(|p| p.dates())
                .max();
            delivered
                .into_iter()
                .chain(open)
                .max()
                .and_then(|d| day(&d).ok())
        });
        let date = day(&f.date)?;
        let ahead = front.is_none_or(|fr| date > fr);
        if ahead && blocked.contains(&f.product) {
            held += 1;
            continue;
        }
        let key = format!("{} {}", f.product, f.date);
        let fp = remote::head(client, &f.url)?;
        if fp.bytes == 0 {
            st.gaps.insert(key, "远端正式文件名为零字节占位".into());
            continue;
        }
        if let Some(local) = local_copy(cfg, &f.product, &f.name) {
            if let Err(e) = remote::check_existing(&local, &fp) {
                st.gaps
                    .insert(key, format!("本地已有源文件与远端不一致，未覆盖：{e:#}"));
                continue;
            }
        }
        if age_minutes(&fp).is_none_or(|m| m < cfg.settle_minutes) {
            unsettled += 1;
            blocked.insert(f.product.clone());
            continue;
        }
        if let (Some(fr), false) = (front.filter(|_| ahead), explicit) {
            let missing: Vec<chrono::NaiveDate> =
                fr.iter_days().skip(1).take_while(|d| *d < date).collect();
            if !missing.is_empty() {
                if age_minutes(&fp).is_none_or(|m| m < cfg.hole_wait_hours * 60) {
                    held += 1;
                    blocked.insert(f.product.clone());
                    continue;
                }
                for d in missing {
                    st.gaps
                        .entry(format!("{} {}", f.product, d.format("%Y-%m-%d")))
                        .or_insert_with(|| "远端缺正式日包（超过等待时间后跳过）".into());
                }
            }
        }
        if cfg.max_new_files > 0 && count >= cfg.max_new_files {
            capped += 1;
            blocked.insert(f.product.clone());
            continue;
        }
        count += 1;
        st.gaps.remove(&key);
        if ahead {
            frontier.insert(f.product.clone(), Some(date));
        }
        planned
            .entry(f.product.clone())
            .or_default()
            .push(PlannedFile {
                date: f.date.clone(),
                name: f.name.clone(),
                url: f.url.clone(),
                remote: fp,
                source_file: None,
                sha256: None,
            });
    }
    // One CF year per product batch: later years wait for the next run.
    let mut deferred = 0;
    for files in planned.values_mut() {
        files.sort_by(|a, b| a.date.cmp(&b.date));
        let year = files[0].date[..4].to_string();
        let before = files.len();
        files.retain(|f| f.date[..4] == year);
        deferred += before - files.len();
    }
    Ok(Discovery {
        planned,
        unsettled,
        deferred_years: deferred,
        capped: capped + held,
    })
}

fn new_batch(run_id: &str, planned: BTreeMap<String, Vec<PlannedFile>>) -> Result<Batch> {
    let mut products = BTreeMap::new();
    for (p, files) in planned {
        let start = files.first().context("files")?.date.clone();
        let last = files.last().unwrap().date.clone();
        ensure!(files.len() < 65536, "{p}: too many files for u16 parts");
        products.insert(
            p,
            ProductRun {
                year: start[..4].parse()?,
                period: format!("{start}_{}", next_day(&last)?),
                files,
                stage: Stage::Planned,
                replay_attempt: 0,
                export_attempt: 0,
                db: None,
                audit: None,
                cfs: BTreeMap::new(),
                rics: 0,
                source_rows: 0,
                trade_rows: 0,
                quote_rows: 0,
                files_new: 0,
                files_existing: 0,
                plan_index: None,
                kline_done: false,
                prev_last: None,
                error: None,
                merged_into: None,
                db_deleted: false,
            },
        );
    }
    Ok(Batch {
        id: run_id.to_string(),
        created_utc: Utc::now().to_rfc3339(),
        products,
    })
}

// ---------------------------------------------------------------- stages

fn stage_download(
    ctx: &mut Ctx,
    client: &reqwest::blocking::Client,
    product: &str,
    pr: &mut ProductRun,
) -> Result<()> {
    let t = Instant::now();
    let cfg = ctx.cfg;
    let dir = cfg.raw_root.join(product);
    let jobs: Vec<usize> = (0..pr.files.len()).collect();
    let next = std::sync::atomic::AtomicUsize::new(0);
    let results = std::sync::Mutex::new(Vec::new());
    std::thread::scope(|s| {
        for _ in 0..cfg.workers.download.min(jobs.len()).max(1) {
            s.spawn(|| loop {
                let i = next.fetch_add(1, std::sync::atomic::Ordering::Relaxed);
                if i >= pr.files.len() {
                    break;
                }
                let f = &pr.files[i];
                let item = remote::Listed {
                    product: product.to_string(),
                    date: f.date.clone(),
                    name: f.name.clone(),
                    url: f.url.clone(),
                    listing_size: String::new(),
                };
                let r = (|| -> Result<remote::Sidecar> {
                    if let Some(local) = local_copy(cfg, product, &f.name) {
                        if let Some(info) = remote::check_existing(&local, &f.remote)? {
                            return Ok(info);
                        }
                    }
                    let mut last = None;
                    for attempt in 0..4u32 {
                        match remote::download(client, &item, &f.remote, &dir) {
                            Ok(info) => return Ok(info),
                            Err(e) => {
                                last = Some(e);
                                std::thread::sleep(std::time::Duration::from_secs(1 << attempt));
                            }
                        }
                    }
                    Err(last.unwrap())
                })();
                results.lock().unwrap().push((i, r));
            });
        }
    });
    for (i, r) in results.into_inner().unwrap() {
        let info = r.with_context(|| format!("download {}", pr.files[i].name))?;
        ensure!(
            info.bytes == pr.files[i].remote.bytes,
            "size mismatch {}",
            pr.files[i].name
        );
        pr.files[i].source_file = Some(info.source_file.clone());
        pr.files[i].sha256 = Some(info.sha256.clone());
    }
    ctx.t.add("下载", t.elapsed().as_secs_f64());
    Ok(())
}

fn staged_dir(dir: &Path, pr: &ProductRun) -> PathBuf {
    dir.join("shards")
        .join(format!("{SHARD_PREFIX}{}", pr.period))
}

fn stage_stage(ctx: &mut Ctx, dir: &Path, pr: &mut ProductRun) -> Result<()> {
    let downloads = dir.join("downloads.json");
    let files: Vec<Value> = pr
        .files
        .iter()
        .map(|f| {
            json!({"date": f.date, "source_url": f.url, "source_file": f.source_file,
                   "bytes": f.remote.bytes, "sha256": f.sha256, "remote": f.remote})
        })
        .collect();
    save_json(&downloads, &json!({"files": files}))?;
    let out = staged_dir(dir, pr);
    if !out.join("manifest.json").exists() {
        for stale in [out.clone(), out.with_extension("building")] {
            if stale.exists() {
                let mut n = stale.as_os_str().to_owned();
                n.push(format!(".interrupted_{}", Utc::now().timestamp()));
                fs::rename(&stale, PathBuf::from(n))?;
            }
        }
        let args = vec![
            "--downloads".into(),
            downloads.display().to_string(),
            "--period".into(),
            pr.period.clone(),
            "--output".into(),
            out.display().to_string(),
            "--workers".into(),
            ctx.cfg.workers.stage.to_string(),
        ];
        ctx.run("分片", "tas_daily_stage", &args, &dir.join("stage.log"))?;
    }
    let m = read_json(&out.join("manifest.json"))?;
    let sources = m["sources"].as_array().context("manifest sources")?;
    ensure!(m["complete"] == true, "stage manifest incomplete");
    ensure!(
        sources.len() == pr.files.len(),
        "stage source count differs"
    );
    for (s, f) in sources.iter().zip(&pr.files) {
        ensure!(
            s["file"].as_str().map(PathBuf::from) == f.source_file
                && s["compressed_bytes"].as_u64() == Some(f.remote.bytes),
            "stage manifest source differs: {}",
            f.name
        );
    }
    pr.source_rows = sources.iter().filter_map(|s| s["data_rows"].as_u64()).sum();
    // Symlinked inputs keep the period-named directory the replay expects.
    let input = dir
        .join("input")
        .join(format!("{SHARD_PREFIX}{}", pr.period));
    fs::create_dir_all(&input)?;
    for (i, f) in pr.files.iter().enumerate() {
        let src = f.source_file.as_ref().context("source file")?;
        let link = input.join(format!("merged-Data-part-{i:06}.csv.gz"));
        if link.is_symlink() {
            ensure!(
                fs::read_link(&link)? == *src,
                "unexpected input link {link:?}"
            );
        } else {
            ensure!(!link.exists(), "unexpected input file {link:?}");
            std::os::unix::fs::symlink(src, &link)?;
        }
    }
    Ok(())
}

fn counters(log: &Path) -> Result<BTreeMap<String, u64>> {
    let text = fs::read_to_string(log)?;
    let line = text
        .lines()
        .rev()
        .find(|l| l.contains("cme_tas_replay_all finished workers="))
        .context("replay did not finish")?;
    Ok(line
        .split_whitespace()
        .filter_map(|kv| {
            let (k, v) = kv.split_once('=')?;
            Some((k.to_string(), v.parse().ok()?))
        })
        .collect())
}

fn stage_replay(
    ctx: &mut Ctx,
    batch: &str,
    product: &str,
    dir: &Path,
    pr: &mut ProductRun,
) -> Result<()> {
    let cfg = ctx.cfg;
    // Never reuse an unfinished DB: every attempt starts from an empty one.
    let mut n = pr.replay_attempt + 1;
    let base = cfg.db_root.join(batch).join(product);
    while base.join(format!("attempt_{n}")).exists() {
        n += 1;
    }
    pr.replay_attempt = n;
    let db = base.join(format!("attempt_{n}"));
    fs::create_dir_all(&base)?;
    let p = |name: &str| dir.join(format!("{name}_{n}.log")).display().to_string();
    let conf = json!({
        "data_root": dir.join("input"), "shard_root": dir.join("shards"), "period": pr.period,
        "rocksdb_dir": db, "workers": cfg.workers.replay, "parse_workers": cfg.workers.parse,
        "parse_chunk_rows": 16384, "column_rules": cfg.column_rules, "progress_every": 5_000_000,
        "log_path": p("replay"), "unparsed_path": p("unparsed"), "unmatched_path": p("unmatched"),
        "quote_fallback_path": p("fallback"),
    });
    let toml: String = conf
        .as_object()
        .unwrap()
        .iter()
        .map(|(k, v)| format!("{k} = {v}\n"))
        .collect();
    let cfg_path = dir.join(format!("replay_{n}.toml"));
    fs::write(&cfg_path, toml)?;
    ctx.run(
        "回放",
        "cme_tas_replay_all",
        &["--config".into(), cfg_path.display().to_string()],
        &dir.join(format!("replay_{n}.stdout.log")),
    )?;
    let c = counters(Path::new(&p("replay")))?;
    ensure!(
        c.get("source_rows") == Some(&pr.source_rows),
        "replay source_rows {:?} != staged {}",
        c.get("source_rows"),
        pr.source_rows
    );
    for k in [
        "skipped_unparsed",
        "unmatched_product",
        "skipped_after_window",
        "skipped_ric_filter",
    ] {
        ensure!(c.get(k) == Some(&0), "replay {k}={:?}", c.get(k));
    }
    pr.db = Some(db);
    Ok(())
}

fn stage_audit(
    ctx: &mut Ctx,
    st: &State,
    product: &str,
    dir: &Path,
    pr: &mut ProductRun,
) -> Result<Vec<String>> {
    let n = pr.replay_attempt;
    let db = pr.db.clone().context("db")?;
    let out = dir.join(format!("db_audit_{n}.json"));
    if !out.exists() {
        let args = vec![
            "--db".into(),
            db.display().to_string(),
            "--require-done".into(),
            pr.period.clone(),
            "--output".into(),
            out.display().to_string(),
        ];
        ctx.run(
            "审计",
            "tas_db_audit",
            &args,
            &dir.join(format!("audit_{n}.log")),
        )?;
    }
    let a = read_json(&out)?;
    let c = counters(&dir.join(format!("replay_{n}.log")))?;
    ensure!(a["status"] == "passed", "audit not passed");
    ensure!(
        a["source_period"] == pr.period.as_str()
            && a["watermarks"][format!("period:{}", pr.period)] == "done",
        "audit period/watermark mismatch"
    );
    let kinds = &a["all"]["kinds"];
    let k = |s: &str| kinds[s].as_u64().unwrap_or(0);
    ensure!(
        k("1") == c["written_trades"],
        "trade count differs from replay"
    );
    ensure!(
        k("2") == c["written_specials"],
        "special count differs from replay"
    );
    ensure!(k("3") <= c["written_quotes"], "quote count exceeds replay");
    let rics: Vec<String> = a["rics"]
        .as_object()
        .context("audit rics")?
        .keys()
        .cloned()
        .collect();
    for r in &rics {
        if let Some(o) = st.owners.get(r) {
            ensure!(
                o == product,
                "RIC {r} already published from product {o}; blocked (no unreviewed cross-source sum)"
            );
        }
    }
    pr.cfs = a["products"]
        .as_object()
        .context("audit products")?
        .iter()
        .map(|(cf, v)| (cf.clone(), v["kinds"]["13"].as_u64().unwrap_or(0)))
        .collect();
    pr.trade_rows = k("1");
    pr.quote_rows = k("3");
    pr.rics = rics.len();
    pr.audit = Some(out);
    Ok(rics)
}

fn export_dir(cfg: &Config, batch: &str, product: &str, pr: &ProductRun) -> PathBuf {
    cfg.export_root
        .join(batch)
        .join(product)
        .join(format!("ric_day_attempt_{}", pr.export_attempt))
}

fn stage_export(
    ctx: &mut Ctx,
    batch: &str,
    product: &str,
    dir: &Path,
    pr: &mut ProductRun,
) -> Result<()> {
    if pr.trade_rows + pr.quote_rows == 0 {
        return Ok(());
    }
    if pr.export_attempt == 0 {
        pr.export_attempt = 1;
    }
    let out = export_dir(ctx.cfg, batch, product, pr);
    if !out.join("_SUCCESS.json").exists() {
        if out.exists() {
            let mut n = out.as_os_str().to_owned();
            n.push(format!(".interrupted_{}", Utc::now().timestamp()));
            fs::rename(&out, PathBuf::from(n))?;
        }
        let (start, end) = pr.period.split_once('_').context("period")?;
        let args = vec![
            "--db".into(),
            pr.db.as_ref().context("db")?.display().to_string(),
            "--year".into(),
            pr.year.to_string(),
            "--period-start".into(),
            start.into(),
            "--period-end".into(),
            end.into(),
            "--output".into(),
            out.display().to_string(),
            "--layout".into(),
            "ric-day".into(),
            "--exchange-map".into(),
            dir.join("exchange_map.json").display().to_string(),
            "--readback".into(),
            "sampled".into(),
            "--workers".into(),
            ctx.cfg.workers.export.to_string(),
        ];
        ctx.run("导出", "lseg_tas_parquet", &args, &dir.join("export.log"))?;
    }
    Ok(())
}

fn stage_publish(
    ctx: &mut Ctx,
    batch: &str,
    product: &str,
    dir: &Path,
    pr: &mut ProductRun,
) -> Result<()> {
    if pr.trade_rows + pr.quote_rows == 0 {
        return Ok(());
    }
    let t = Instant::now();
    let out = export_dir(ctx.cfg, batch, product, pr);
    let export = publish::check_export(&out)?;
    ensure!(
        (export.trade_rows, export.quote_rows) == (pr.trade_rows, pr.quote_rows),
        "export rows differ from the DB audit"
    );
    let venues: BTreeMap<String, String> =
        serde_json::from_value(read_json(&dir.join("exchange_map.json"))?)?;
    let p = publish::publish(&export, &ctx.cfg.delivery_root, &venues)?;
    ensure!(
        p.new_files + p.existing_identical == export.files && p.bytes == export.bytes,
        "published file set differs from the export"
    );
    pr.files_new = p.new_files;
    pr.files_existing = p.existing_identical;
    ctx.t.add("发布", t.elapsed().as_secs_f64());
    Ok(())
}

fn stage_status(
    ctx: &mut Ctx,
    batch: &str,
    product: &str,
    dir: &Path,
    pr: &mut ProductRun,
) -> Result<()> {
    if pr.cfs.values().sum::<u64>() == 0 {
        return Ok(());
    }
    let plan = dir.join("status_plan.json");
    let exchanges: BTreeMap<String, String> =
        serde_json::from_value(read_json(&dir.join("exchange_map.json"))?)?;
    save_json(
        &plan,
        &json!({"year": pr.year, "exchanges": exchanges,
                "jobs": [{"id": format!("daily{batch}_{product}"), "db": pr.db, "period": pr.period, "cfs": pr.cfs}]}),
    )?;
    let args = vec![
        "--plan".into(),
        plan.display().to_string(),
        "--output".into(),
        ctx.cfg.delivery_root.display().to_string(),
        "--checkpoint".into(),
        dir.join("status_checkpoints").display().to_string(),
        "--workers".into(),
        ctx.cfg.workers.status.to_string(),
    ];
    ctx.run("状态", "tas_status_parquet", &args, &dir.join("status.log"))
}

/// Advances one product as far as possible; state is saved between stages.
fn advance(
    ctx: &mut Ctx,
    st: &mut State,
    client: &reqwest::blocking::Client,
    bi: usize,
    product: &str,
) -> Result<()> {
    let batch = st.batches[bi].id.clone();
    let dir = product_dir(ctx.cfg, &batch, product);
    fs::create_dir_all(&dir)?;
    loop {
        let mut pr = st.batches[bi].products[product].clone();
        let next = match pr.stage {
            Stage::Planned => {
                stage_download(ctx, client, product, &mut pr)?;
                Stage::Downloaded
            }
            Stage::Downloaded => {
                stage_stage(ctx, &dir, &mut pr)?;
                if pr.source_rows == 0 {
                    pr.kline_done = true;
                    Stage::Done
                } else {
                    Stage::Staged
                }
            }
            Stage::Staged => {
                stage_replay(ctx, &batch, product, &dir, &mut pr)?;
                Stage::Replayed
            }
            Stage::Replayed => {
                let rics = stage_audit(ctx, st, product, &dir, &mut pr)?;
                let approved = ctx.approved()?.clone();
                let mut map = venue::assign(&rics, &st.venues, &approved, &ctx.cfg.hrd)?;
                follow_roots(ctx.cfg, &pr, &mut map)?;
                save_json(&dir.join("exchange_map.json"), &map)?;
                for (r, v) in &map {
                    // A venue that differs from the approved map (e.g. root_follow over an
                    // approved `review`) is pinned in the state, which wins from now on.
                    if approved.get(r) != Some(v) {
                        st.venues.insert(r.clone(), v.clone());
                    }
                    st.owners.insert(r.clone(), product.to_string());
                }
                Stage::Audited
            }
            Stage::Audited => {
                stage_export(ctx, &batch, product, &dir, &mut pr)?;
                Stage::Exported
            }
            Stage::Exported => {
                st.catalog_dirty = true;
                stage_publish(ctx, &batch, product, &dir, &mut pr)?;
                Stage::Published
            }
            Stage::Published => {
                stage_status(ctx, &batch, product, &dir, &mut pr)?;
                Stage::Done
            }
            Stage::Done => return Ok(()),
        };
        pr.stage = next;
        if next == Stage::Done {
            let d = st.delivered.entry(product.to_string()).or_default();
            d.extend(pr.dates());
            for f in &pr.files {
                st.gaps.remove(&format!("{product} {}", f.date));
            }
            if pr.db.is_some() && pr.plan_index.is_none() {
                let base = st.next_source_index.context("source index")?;
                pr.plan_index = Some(base);
                st.next_source_index = Some(base + u32::try_from(pr.cfs.len())?);
            }
        }
        st.batches[bi].products.insert(product.to_string(), pr);
        st.save(&ctx.cfg.state_file)?;
    }
}

/// Rule 2 of venue_fix.rs for RICs with no file under `<delivery>/review/`: an unresolved
/// (`review`) RIC takes the single exchange directory of its root within three years.
/// RICs that already have files under `review/` keep it (a RIC never changes directory
/// here); if they are now resolvable they are listed in
/// `meta/problems/lseg_daily/venues_resolvable.json` for `venues-migrate`, which moves
/// their files first.
fn follow_roots(cfg: &Config, pr: &ProductRun, map: &mut BTreeMap<String, String>) -> Result<()> {
    if !map.values().any(|v| v == "review") {
        return Ok(());
    }
    let year: i32 = pr
        .dates()
        .first()
        .and_then(|d| d.get(..4))
        .context("batch dates")?
        .parse()?;
    let years: BTreeSet<i32> = (year - 3..=year + 3).collect();
    let roots = venue_fix::root_venues(&cfg.delivery_root, &years)?;
    let first = chrono::NaiveDate::from_ymd_opt(year, 1, 1).unwrap();
    let mut resolvable = Vec::new();
    for (ric, v) in map.iter_mut() {
        if v != "review" {
            continue;
        }
        let (venue, rule, evidence) = venue_fix::resolve(ric, year, first, None, &roots);
        if venue == "review" {
            continue;
        }
        // Already delivered under review/: moving it is venues-migrate's job (files first).
        // Being listed as `review` in the driver state or the approved map without any
        // delivered file does not count (721 such approved RICs on 2026-11-26).
        if venue_fix::under_review(&cfg.delivery_root, ric) {
            resolvable
                .push(json!({"ric": ric, "venue": venue, "rule": rule, "evidence": evidence}));
        } else {
            eprintln!("lseg-daily: {ric} -> {venue} ({rule}: {evidence})");
            *v = venue;
        }
    }
    if !resolvable.is_empty() {
        record_problem(
            cfg,
            "venues_resolvable.json",
            &json!({"note": "delivered under review/ but now resolvable; run lseg-daily venues-migrate", "rics": resolvable}),
        );
    }
    Ok(())
}

// ---------------------------------------------------------------- kline / catalog

fn cf_parts(cf: &str) -> Result<(i32, String)> {
    let mut it = cf.splitn(3, ':');
    ensure!(it.next() == Some("p"), "CF {cf}");
    let y = it.next().context("CF year")?.parse()?;
    Ok((y, it.next().context("CF product")?.to_string()))
}

/// Base plan jobs verbatim, then one job per delivered daily CF.
fn kline_plan(cfg: &Config, st: &State) -> Result<Value> {
    let mut plan = read_json(&cfg.kline_base_plan)?;
    let jobs = plan["jobs"].as_array_mut().context("base plan jobs")?;
    for b in &st.batches {
        for (product, pr) in &b.products {
            let (Some(db), Some(base)) = (&pr.db, pr.plan_index) else {
                continue;
            };
            if pr.stage != Stage::Done || pr.merged_into.is_some() {
                continue;
            }
            let dir = product_dir(cfg, &b.id, product);
            for (i, cf) in pr.cfs.keys().enumerate() {
                let (year, cfp) = cf_parts(cf)?;
                jobs.push(json!({
                    "id": format!("daily{}_{}__{}", b.id, product, publish::component(cf)),
                    "db": db, "cf": cf, "year": year, "product": cfp, "period": pr.period,
                    "source": "daily", "source_index": base + i as u32,
                    "audit": pr.audit, "coverage_days": pr.dates(),
                    "exchange_map": dir.join("exchange_map.json"),
                }));
            }
        }
    }
    for m in st.merged.iter().filter(|m| m.retired_by.is_none()) {
        for (i, cf) in m.cfs.keys().enumerate() {
            let (year, cfp) = cf_parts(cf)?;
            jobs.push(json!({
                "id": format!("monthly_{}__{}", publish::component(&m.id), publish::component(cf)),
                "db": m.db, "cf": cf, "year": year, "product": cfp, "period": m.period,
                "source": "daily", "source_index": m.plan_index + i as u32,
                "audit": m.report, "coverage_days": m.coverage_days,
                "exchange_map": m.exchange_map,
                "merged_from": m.merged_from.iter().map(|f| f.id.as_str()).collect::<Vec<_>>(),
            }));
        }
    }
    Ok(plan)
}

fn base_next_index(cfg: &Config) -> Result<u32> {
    let plan = read_json(&cfg.kline_base_plan)?;
    let max = plan["jobs"]
        .as_array()
        .context("base plan jobs")?
        .iter()
        .filter_map(|j| j["source_index"].as_u64())
        .max()
        .unwrap_or(0);
    Ok(u32::try_from(max + 1)?)
}

#[derive(Default)]
struct KlineReport {
    groups_ok: Vec<String>,
    groups_failed: Vec<(String, String)>,
    replaced: BTreeMap<String, usize>,
}

/// What a group's pending batches add: the earliest new source date and the
/// earliest "late" date (before what was already delivered).
#[derive(Default)]
struct GroupNews {
    owners: Vec<(usize, String)>,
    first_new: Option<String>,
    late_days: BTreeSet<String>,
    prev_last: Option<String>,
}

/// Why an existing day file was replaced (`replaced.csv`).
fn reason(
    day: &str,
    late: &BTreeSet<String>,
    old_rows: u64,
    new_rows: u64,
    prefix: bool,
) -> &'static str {
    if late.contains(day) {
        "late_day"
    } else if prefix && new_rows > old_rows {
        "tail_completion"
    } else {
        "correction"
    }
}

fn run_kline(ctx: &mut Ctx, st: &mut State) -> Result<KlineReport> {
    let cfg = ctx.cfg;
    let mut groups: BTreeMap<(i32, String), GroupNews> = BTreeMap::new();
    for (bi, b) in st.batches.iter().enumerate() {
        for (product, pr) in &b.products {
            if pr.stage == Stage::Done && !pr.kline_done && pr.db.is_some() {
                let dates = pr.dates();
                for cf in pr.cfs.keys() {
                    let g = groups.entry(cf_parts(cf)?).or_default();
                    g.owners.push((bi, product.clone()));
                    let first = dates.first().cloned();
                    g.first_new = match (g.first_new.take(), first) {
                        (Some(a), Some(b)) => Some(a.min(b)),
                        (a, b) => a.or(b),
                    };
                    if let Some(prev) = &pr.prev_last {
                        g.late_days.extend(
                            dates
                                .iter()
                                .filter(|d| *d < prev)
                                .map(|d| d.replace('-', "")),
                        );
                        g.prev_last = Some(match g.prev_last.take() {
                            Some(p) => p.min(prev.clone()),
                            None => prev.clone(),
                        });
                    }
                }
            }
        }
    }
    let mut rep = KlineReport::default();
    if groups.is_empty() {
        return Ok(rep);
    }
    let work = cfg.work_root.join("kline").join(&ctx.run_id);
    let plan_path = work.join("plan.json");
    save_json(&plan_path, &kline_plan(cfg, st)?)?;
    let log = work.join("kline.log");
    let backups = cfg.work_root.join("backups").join(&ctx.run_id);
    ctx.run(
        "K线",
        "lseg_kline",
        &[
            "check-plan".into(),
            "--plan".into(),
            plan_path.display().to_string(),
        ],
        &log,
    )?;
    // Raw-CSV correction evidence of the new batches, so their kind=12
    // rows can be netted (one immutable witness segment per batch/product).
    let mut appended = BTreeSet::new();
    for news in groups.values() {
        for (bi, product) in &news.owners {
            if !appended.insert((*bi, product.clone())) {
                continue;
            }
            let b = &st.batches[*bi];
            let pr = &b.products[product];
            let files = pr
                .files
                .iter()
                .map(|f| {
                    f.source_file
                        .as_ref()
                        .map(|p| p.display().to_string())
                        .context("downloaded source file")
                })
                .collect::<Result<Vec<_>>>()?;
            if files.is_empty() {
                continue;
            }
            ctx.run(
                "改撤见证",
                "lseg_kline",
                &[
                    "witness-append".into(),
                    "--store".into(),
                    cfg.kline_witnesses.display().to_string(),
                    "--segment".into(),
                    format!("daily{}_{}", b.id, publish::component(product)),
                    "--year".into(),
                    pr.year.to_string(),
                    "--files".into(),
                    files.join(","),
                    "--workers".into(),
                    ctx.cfg.workers.stage.min(8).to_string(),
                ],
                &log,
            )?;
        }
    }
    let mut failed_products = BTreeSet::new();
    let mut rows = Vec::new();
    // One job per (year, product) group, each with its own work dir and log
    // so several lseg_kline runs can go in parallel (`workers.kline_groups`).
    struct GroupJob<'g> {
        key: String,
        name: String,
        news: &'g GroupNews,
        args: Vec<String>,
        replaced_log: PathBuf,
        log: PathBuf,
    }
    let mut jobs = Vec::new();
    for ((year, product), news) in &groups {
        let key = format!("{year}:{product}");
        let gdir = work
            .join("groups")
            .join(format!("{year}_{}", publish::component(product)));
        let replaced_log = gdir.join("replaced.jsonl");
        let mut args = vec![
            "run".into(),
            "--project".into(),
            cfg.project.display().to_string(),
            "--plan".into(),
            plan_path.display().to_string(),
            "--cache".into(),
            cfg.kline_cache.display().to_string(),
            "--witnesses".into(),
            cfg.kline_witnesses.display().to_string(),
            "--work".into(),
            gdir.display().to_string(),
            "--output".into(),
            cfg.delivery_root.display().to_string(),
            "--year".into(),
            year.to_string(),
            "--product".into(),
            product.clone(),
            "--workers".into(),
            cfg.workers.kline.to_string(),
            "--backup-root".into(),
            backups.display().to_string(),
            "--replaced-log".into(),
            replaced_log.display().to_string(),
        ];
        // The first run of a group compares every regenerated day (one-time
        // completion of truncated tails); later runs start one day before the
        // earliest new or previously-last date.
        if st.kline_compared.contains(&key) {
            let from = [news.first_new.clone(), news.prev_last.clone()]
                .into_iter()
                .flatten()
                .min()
                .context("group without dates")?;
            let from = chrono::NaiveDate::parse_from_str(&from, "%Y-%m-%d")?
                .pred_opt()
                .context("date underflow")?;
            args.push("--compare-from".into());
            args.push(from.format("%Y-%m-%d").to_string());
        }
        jobs.push(GroupJob {
            key,
            name: format!("{year}/{product}"),
            news,
            args,
            replaced_log,
            log: gdir.join("kline.log"),
        });
    }
    let exe = ctx.bin("lseg_kline");
    let parallel = cfg.workers.kline_groups.clamp(1, 16);
    let started = Instant::now();
    let next = std::sync::atomic::AtomicUsize::new(0);
    let results: Vec<std::sync::Mutex<Option<Result<()>>>> =
        jobs.iter().map(|_| std::sync::Mutex::new(None)).collect();
    std::thread::scope(|scope| {
        for _ in 0..parallel.min(jobs.len()) {
            scope.spawn(|| loop {
                let i = next.fetch_add(1, std::sync::atomic::Ordering::Relaxed);
                let Some(job) = jobs.get(i) else { break };
                let r = run_tool(cfg, &exe, "lseg_kline", &job.args, &job.log);
                eprintln!(
                    "lseg-daily: K线 {} {}",
                    job.name,
                    if r.is_ok() { "ok" } else { "failed" }
                );
                *results[i].lock().unwrap() = Some(r);
            });
        }
    });
    ctx.t.add("K线", started.elapsed().as_secs_f64());
    for (job, result) in jobs.iter().zip(results) {
        let result = result.into_inner().unwrap().context("group not run")?;
        let (news, replaced_log, name, key) = (
            job.news,
            &job.replaced_log,
            job.name.clone(),
            job.key.clone(),
        );
        // Replacements already happened even if the group failed later.
        if replaced_log.exists() {
            for line in fs::read_to_string(replaced_log)?.lines() {
                let v: Value = serde_json::from_str(line)?;
                let path = v["path"].as_str().unwrap_or_default().to_string();
                let day = Path::new(&path)
                    .parent()
                    .and_then(Path::file_name)
                    .map(|d| d.to_string_lossy().into_owned())
                    .unwrap_or_default();
                let (o, n) = (
                    v["old_rows"].as_u64().unwrap_or(0),
                    v["new_rows"].as_u64().unwrap_or(0),
                );
                let why = if path.ends_with("kline.parquet")
                    || path.ends_with("session_status.parquet")
                {
                    reason(&day, &news.late_days, o, n, v["old_is_prefix"] == true)
                } else {
                    "other"
                };
                *rep.replaced.entry(why.to_string()).or_default() += 1;
                rows.push(vec![
                    ctx.run_id.clone(),
                    path,
                    why.to_string(),
                    o.to_string(),
                    n.to_string(),
                    v["backup"].as_str().unwrap_or_default().to_string(),
                ]);
            }
        }
        match result {
            Ok(()) => {
                st.kline_compared.insert(key);
                rep.groups_ok.push(name)
            }
            Err(e) => {
                rep.groups_failed.push((name, format!("{e:#}")));
                failed_products.extend(news.owners.iter().cloned());
            }
        }
    }
    append_replaced(cfg, &rows)?;
    for news in groups.values() {
        for (bi, product) in &news.owners {
            if !failed_products.contains(&(*bi, product.clone())) {
                st.batches[*bi]
                    .products
                    .get_mut(product)
                    .unwrap()
                    .kline_done = true;
            }
        }
    }
    st.save(&cfg.state_file)?;
    Ok(rep)
}

/// `meta/problems/lseg_daily/replaced.csv`: one line per replaced delivery
/// file (cumulative; the old bytes are in the local backup named there).
fn append_replaced(cfg: &Config, rows: &[Vec<String>]) -> Result<()> {
    if rows.is_empty() {
        return Ok(());
    }
    let p = problems_dir(cfg).join("replaced.csv");
    let mut text = fs::read_to_string(&p)
        .unwrap_or_else(|_| "run,path,reason,old_rows,new_rows,backup\n".to_string());
    for r in rows {
        text.push_str(&r.join(","));
        text.push('\n');
    }
    fs::create_dir_all(p.parent().unwrap())?;
    let tmp = p.with_extension("csv.partial");
    fs::write(&tmp, &text)?;
    File::open(&tmp)?.sync_all()?;
    fs::rename(&tmp, &p)?;
    Ok(())
}

/// Loads the open interest of every finished daily gzip under `raw_root` that is not in
/// PostgreSQL yet (normally just this run's downloads; a failed day is retried next run).
fn run_open_interest(ctx: &mut Ctx) -> Result<()> {
    let cfg = ctx.cfg;
    let oi = cfg.open_interest.as_ref().context("open_interest config")?;
    let work = cfg.work_root.join("open_interest").join(&ctx.run_id);
    let args = vec![
        "sync".into(),
        "--daily-root".into(),
        cfg.raw_root.display().to_string(),
        "--database-url".into(),
        oi.database_url.clone(),
        "--work-dir".into(),
        work.join("staging").display().to_string(),
        "--workers".into(),
        oi.workers.to_string(),
    ];
    let r = ctx.run("持仓", "tas_open_interest", &args, &work.join("sync.log"));
    let _ = fs::remove_dir(work.join("staging"));
    r
}

fn run_catalog(ctx: &mut Ctx, st: &State) -> Result<()> {
    let cfg = ctx.cfg;
    let plan_path = cfg
        .work_root
        .join("kline")
        .join(&ctx.run_id)
        .join("plan.json");
    if !plan_path.exists() {
        save_json(&plan_path, &kline_plan(cfg, st)?)?;
    }
    let mut args = vec![
        "build".into(),
        "--root".into(),
        cfg.delivery_root.display().to_string(),
        "--plan".into(),
        plan_path.display().to_string(),
        "--project".into(),
        cfg.project.display().to_string(),
        "--work".into(),
        cfg.work_root.join("catalog").display().to_string(),
    ];
    if cfg.catalog_prune_expired_status {
        args.push("--prune-expired-status".into());
    }
    args.extend(cfg.catalog_extra_args.iter().cloned());
    ctx.run(
        "目录",
        "lseg_catalog",
        &args,
        &cfg.work_root.join("catalog/catalog.log"),
    )
}

// ---------------------------------------------------------------- problems

fn problems_dir(cfg: &Config) -> PathBuf {
    cfg.delivery_root.join("meta/problems/lseg_daily")
}

fn record_problem(cfg: &Config, rel: &str, v: &Value) {
    let p = problems_dir(cfg).join(rel);
    if let Err(e) = save_json(&p, v) {
        eprintln!("lseg-daily: cannot record problem {p:?}: {e:#}");
    }
}

fn clear_problem(cfg: &Config, rel: &str) {
    let p = problems_dir(cfg).join(rel);
    if p.exists() {
        let _ = fs::remove_file(&p);
        if let Some(d) = p.parent() {
            let _ = fs::remove_dir(d);
        }
    }
}

/// Current source gaps as one CSV; removed when there are none.
fn write_gaps(cfg: &Config, st: &State) -> Result<()> {
    let p = problems_dir(cfg).join("source_gaps.csv");
    let mut text = String::from("product,date,reason\n");
    for (k, reason) in &st.gaps {
        let (prod, date) = k.split_once(' ').unwrap_or((k, ""));
        text.push_str(&format!("{prod},{date},\"{}\"\n", reason.replace('"', "'")));
    }
    if st.gaps.is_empty() {
        if p.exists() {
            fs::remove_file(&p)?;
        }
        return Ok(());
    }
    if fs::read_to_string(&p).ok().as_deref() == Some(text.as_str()) {
        return Ok(());
    }
    fs::create_dir_all(p.parent().unwrap())?;
    let tmp = p.with_extension("csv.partial");
    fs::write(&tmp, &text)?;
    File::open(&tmp)?.sync_all()?;
    fs::rename(&tmp, &p)?;
    Ok(())
}

// ---------------------------------------------------------------- run

fn notify(cfg: &Config, enabled: bool, title: &str, body: &str) {
    notify_as(cfg, enabled, "LSEG 日更", title, body)
}

fn notify_as(cfg: &Config, enabled: bool, head: &str, title: &str, body: &str) {
    let text = format!("【{head}】{title}（{}）\n{body}", cfg.host_tag);
    eprintln!("{text}");
    if !enabled {
        return;
    }
    match dingtalk::DingTalk::from_env(&cfg.dingtalk_webhook_env, &cfg.dingtalk_secret_env) {
        Ok(Some(d)) => {
            if let Err(e) = d.send_text(&dingtalk::truncate(&text, 3500)) {
                eprintln!("lseg-daily: DingTalk send failed: {e:#}");
            }
        }
        Ok(None) => eprintln!(
            "lseg-daily: {} unset; DingTalk notification skipped",
            cfg.dingtalk_webhook_env
        ),
        Err(e) => eprintln!("lseg-daily: DingTalk config invalid: {e:#}"),
    }
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

#[derive(Default)]
struct Summary {
    new_files: usize,
    unsettled: usize,
    deferred: usize,
    products_done: Vec<String>,
    products_failed: Vec<(String, String)>,
    files_new: u64,
    files_existing: u64,
    trade_rows: u64,
    quote_rows: u64,
    dates: BTreeSet<String>,
    kline: KlineReport,
    open_interest: Option<Result<(), String>>,
    catalog: Option<Result<(), String>>,
}

fn run(cfg: &Config, opts: &Opts, notify_on: bool) -> Result<()> {
    fs::create_dir_all(&cfg.work_root)?;
    let lock = File::create(cfg.work_root.join(".lseg_daily.lock"))?;
    if lock.try_lock().is_err() {
        bail!(
            "another lseg-daily run holds {:?}",
            cfg.work_root.join(".lseg_daily.lock")
        );
    }
    let run_id = Utc::now().format("%Y%m%dT%H%M%SZ").to_string();
    let mut st = State::load(&cfg.state_file)?;
    if !st.bootstrapped {
        bootstrap(cfg, &mut st)?;
    }
    if st.next_source_index.is_none() {
        st.next_source_index = Some(base_next_index(cfg)?);
    }
    let mut ctx = Ctx {
        cfg,
        bins: bin_dir(cfg)?,
        run_id: run_id.clone(),
        approved: None,
        t: Timings::default(),
    };
    let started = Instant::now();
    let mut sum = Summary::default();
    let result = lseg(&mut ctx, &mut st, opts, &mut sum);
    if !opts.dry_run {
        if let Err(e) = write_gaps(cfg, &st) {
            eprintln!("lseg-daily: cannot write source gaps: {e:#}");
        }
    }
    if opts.dry_run {
        return result;
    }
    let ok = result.is_ok()
        && sum.products_failed.is_empty()
        && sum.kline.groups_failed.is_empty()
        && !matches!(sum.open_interest, Some(Err(_)))
        && !matches!(sum.catalog, Some(Err(_)));
    let body = summarize(
        &sum,
        &ctx.t,
        &st,
        started.elapsed().as_secs_f64(),
        result.as_ref().err(),
    );
    match &result {
        Ok(()) => clear_problem(cfg, "pipeline.json"),
        Err(e) => record_problem(
            cfg,
            "pipeline.json",
            &json!({"run": run_id, "error": format!("{e:#}"), "at": Utc::now().to_rfc3339()}),
        ),
    }
    st.history.push(RunRecord {
        run: run_id,
        finished_utc: Utc::now().to_rfc3339(),
        ok,
        detail: body.clone(),
    });
    let keep = st.history.len().saturating_sub(200);
    st.history.drain(..keep);
    st.save(&cfg.state_file)?;
    eprintln!("{body}");
    // Push only problems; a normal run (also "no new data") stays in the log.
    let text = problems_brief(
        &sum,
        &st,
        result.as_ref().err(),
        cfg.dashboard_url.as_deref(),
    );
    if !text.is_empty() {
        let title = if result.is_err() {
            "失败"
        } else if ok {
            "有问题"
        } else {
            "部分失败"
        };
        notify(cfg, notify_on, title, &text);
    }
    result?;
    ensure!(
        ok,
        "some LSEG daily steps failed; see problems and the DingTalk summary"
    );
    Ok(())
}

fn lseg(ctx: &mut Ctx, st: &mut State, opts: &Opts, sum: &mut Summary) -> Result<()> {
    let cfg = ctx.cfg;
    let client = remote::client()?;
    if !opts.no_new {
        let t = Instant::now();
        let inv = remote::inventory(&client, &cfg.remote_root, &opts.products)?;
        let d = discover(cfg, st, &client, &inv, opts)?;
        ctx.t.add("盘点", t.elapsed().as_secs_f64());
        sum.unsettled = d.unsettled;
        sum.deferred = d.deferred_years + d.capped;
        sum.new_files = d.planned.values().map(Vec::len).sum();
        let last = inv
            .files
            .iter()
            .map(|f| f.date.as_str())
            .max()
            .unwrap_or("-");
        eprintln!(
            "lseg-daily: remote {} products / {} formal files (latest {last}, {} .part); new {} in {} products; unsettled {}; deferred {}",
            inv.products.len(),
            inv.files.len(),
            inv.partials,
            sum.new_files,
            d.planned.len(),
            d.unsettled,
            sum.deferred
        );
        if opts.dry_run {
            for (p, files) in &d.planned {
                let dates: Vec<&str> = files.iter().map(|f| f.date.as_str()).collect();
                let bytes: u64 = files.iter().map(|f| f.remote.bytes).sum();
                println!(
                    "new {p}: {} files {bytes} bytes {}",
                    files.len(),
                    dates.join(",")
                );
            }
        } else if !d.planned.is_empty() {
            let mut id = ctx.run_id.clone();
            while st.batches.iter().any(|b| b.id == id) {
                id.push('x');
            }
            let mut batch = new_batch(&id, d.planned)?;
            for (p, pr) in batch.products.iter_mut() {
                pr.prev_last = st.delivered.get(p).and_then(|d| d.last().cloned());
            }
            st.batches.push(batch);
            st.save(&cfg.state_file)?;
        }
    }
    if opts.dry_run {
        for b in st.batches.iter().filter(|b| !b.finished()) {
            for (p, pr) in &b.products {
                if !pr.finished() {
                    println!(
                        "open {} {p}: stage {:?} kline_done={} period {}",
                        b.id, pr.stage, pr.kline_done, pr.period
                    );
                }
            }
        }
        return Ok(());
    }
    for bi in 0..st.batches.len() {
        if st.batches[bi].finished() {
            continue;
        }
        let id = st.batches[bi].id.clone();
        let products: Vec<String> = st.batches[bi]
            .products
            .iter()
            .filter(|(p, pr)| {
                pr.stage != Stage::Done && (opts.products.is_empty() || opts.products.contains(p))
            })
            .map(|(p, _)| p.clone())
            .collect();
        for p in products {
            let rel = format!("{id}/{p}.json");
            match advance(ctx, st, &client, bi, &p) {
                Ok(()) => {
                    let pr = st.batches[bi].products.get_mut(&p).unwrap();
                    pr.error = None;
                    clear_problem(cfg, &rel);
                    sum.products_done.push(p.clone());
                    sum.files_new += pr.files_new;
                    sum.files_existing += pr.files_existing;
                    sum.trade_rows += pr.trade_rows;
                    sum.quote_rows += pr.quote_rows;
                    sum.dates.extend(pr.dates());
                }
                Err(e) => {
                    let msg = format!("{e:#}");
                    eprintln!("lseg-daily: {id}/{p} failed: {msg}");
                    let pr = st.batches[bi].products.get_mut(&p).unwrap();
                    pr.error = Some(msg.clone());
                    record_problem(
                        cfg,
                        &rel,
                        &json!({"batch": id, "product": p, "stage": pr.stage, "dates": pr.dates(),
                                "error": msg, "at": Utc::now().to_rfc3339()}),
                    );
                    sum.products_failed.push((p.clone(), msg));
                }
            }
            st.save(&cfg.state_file)?;
        }
    }
    if !opts.skip.iter().any(|s| s == "kline") {
        sum.kline = run_kline(ctx, st)?;
    }
    if cfg.open_interest.is_some() && !opts.skip.iter().any(|s| s == "open_interest") {
        let r = run_open_interest(ctx).map_err(|e| format!("{e:#}"));
        match &r {
            Ok(()) => clear_problem(cfg, "open_interest.json"),
            Err(e) => record_problem(
                cfg,
                "open_interest.json",
                &json!({"run": ctx.run_id, "error": e, "at": Utc::now().to_rfc3339()}),
            ),
        }
        sum.open_interest = Some(r);
    }
    let clean = sum.products_failed.is_empty() && sum.kline.groups_failed.is_empty();
    if cfg.catalog && !opts.skip.iter().any(|s| s == "catalog") && st.catalog_dirty && clean {
        let r = run_catalog(ctx, st).map_err(|e| format!("{e:#}"));
        if r.is_ok() {
            st.catalog_dirty = false;
            st.save(&cfg.state_file)?;
        }
        sum.catalog = Some(r);
    }
    Ok(())
}

fn summarize(
    sum: &Summary,
    t: &Timings,
    st: &State,
    secs: f64,
    err: Option<&anyhow::Error>,
) -> String {
    let mut s = String::new();
    if let Some(e) = err {
        s.push_str(&format!("运行中断：{}\n", first_line(&format!("{e:#}"))));
    }
    let span = match (sum.dates.first(), sum.dates.last()) {
        (Some(a), Some(b)) if a == b => a.clone(),
        (Some(a), Some(b)) => format!("{a}..{b}"),
        _ => "-".into(),
    };
    s.push_str(&format!(
        "新源文件 {}（未稳定 {}，顺延 {}）\n完成品种 {}，源日期 {span}\nTrade {} 行 / Quote {} 行；新发布 {} 个文件（已存在且一致 {}）",
        sum.new_files,
        sum.unsettled,
        sum.deferred,
        sum.products_done.len(),
        sum.trade_rows,
        sum.quote_rows,
        sum.files_new,
        sum.files_existing
    ));
    if !sum.products_failed.is_empty() {
        s.push_str(&format!("\n⚠ 失败品种 {}：", sum.products_failed.len()));
        for (p, e) in sum.products_failed.iter().take(8) {
            s.push_str(&format!("\n  {p}: {}", first_line(e)));
        }
    }
    if !sum.kline.groups_ok.is_empty() || !sum.kline.groups_failed.is_empty() {
        s.push_str(&format!(
            "\nK线组 成功 {} / 失败 {}",
            sum.kline.groups_ok.len(),
            sum.kline.groups_failed.len()
        ));
        if !sum.kline.replaced.is_empty() {
            let r: Vec<String> = sum
                .kline
                .replaced
                .iter()
                .map(|(k, v)| format!("{k} {v}"))
                .collect();
            s.push_str(&format!(
                "\n已替换旧 K线/状态文件（备份在本机，见 meta/problems/lseg_daily/replaced.csv）：{}",
                r.join("，")
            ));
        }
        for (g, e) in sum.kline.groups_failed.iter().take(8) {
            s.push_str(&format!("\n  {g}: {}", first_line(e)));
        }
    }
    if let Some(Err(e)) = &sum.open_interest {
        s.push_str(&format!("\n⚠ 持仓量入库失败：{}", first_line(e)));
    }
    match &sum.catalog {
        Some(Ok(())) => s.push_str("\n目录 catalog.sqlite 已重建"),
        Some(Err(e)) => s.push_str(&format!("\n⚠ 目录重建失败：{}", first_line(e))),
        None if st.catalog_dirty => s.push_str("\n目录未重建（有失败或跳过），下次成功运行时重建"),
        None => {}
    }
    if !st.gaps.is_empty() {
        s.push_str(&format!(
            "\n源缺口 {} 项（meta/problems/lseg_daily/source_gaps.csv）",
            st.gaps.len()
        ));
    }
    let open: usize = st
        .batches
        .iter()
        .flat_map(|b| b.products.values())
        .filter(|p| !p.finished())
        .count();
    if open > 0 {
        s.push_str(&format!("\n未完成待续 {open} 个品种批次"));
    }
    let tt: Vec<String> = t.0.iter().map(|(k, v)| format!("{k} {v:.0}s")).collect();
    s.push_str(&format!("\n耗时 {secs:.0}s：{}", tt.join("，")));
    s
}

/// Short DingTalk text: what arrived, what is done, what needs attention.
/// Paths, counters and full errors stay in the run history / log / dashboard.
/// The push text: only what went wrong (nothing for a normal run, whose full
/// summary stays in the log and the state file).
fn problems_brief(
    sum: &Summary,
    st: &State,
    err: Option<&anyhow::Error>,
    url: Option<&str>,
) -> String {
    let mut s = Vec::new();
    if let Some(e) = err {
        s.push(format!("运行中断：{}", short(&format!("{e:#}"))));
    }
    if !sum.products_failed.is_empty() {
        s.push(format!(
            "{} 个品种处理失败：{}",
            sum.products_failed.len(),
            names(sum.products_failed.iter().map(|(p, _)| p.as_str()))
        ));
    }
    if !sum.kline.groups_failed.is_empty() {
        s.push(format!(
            "K 线失败 {} 个：{}（{}）",
            sum.kline.groups_failed.len(),
            names(sum.kline.groups_failed.iter().map(|(g, _)| g.as_str())),
            short(&sum.kline.groups_failed[0].1)
        ));
    }
    if let Some(Err(e)) = &sum.open_interest {
        s.push(format!("持仓量入库失败：{}", short(e)));
    }
    match &sum.catalog {
        Some(Err(e)) => s.push(format!("目录重建失败：{}", short(e))),
        None if st.catalog_dirty => s.push("目录待下次成功运行时重建".into()),
        _ => {}
    }
    if !s.is_empty() {
        if let Some(u) = url {
            s.push(format!("详情：{u}"));
        }
    }
    s.join("\n")
}

fn names<'a>(it: impl Iterator<Item = &'a str>) -> String {
    let v: Vec<&str> = it.collect();
    let head: Vec<&str> = v.iter().take(5).copied().collect();
    if v.len() > 5 {
        format!("{} 等 {} 个", head.join("、"), v.len())
    } else {
        head.join("、")
    }
}

fn short(s: &str) -> String {
    dingtalk::truncate(s.lines().next().unwrap_or(""), 120)
}

fn first_line(s: &str) -> String {
    let l = s.lines().next().unwrap_or("");
    dingtalk::truncate(l, 300)
}

fn status(cfg: &Config) -> Result<()> {
    let st = State::load(&cfg.state_file)?;
    let last = st
        .delivered
        .values()
        .filter_map(|d| d.last())
        .max()
        .cloned()
        .unwrap_or_default();
    println!(
        "bootstrapped={} products={} latest_delivered={last} gaps={} catalog_dirty={}",
        st.bootstrapped,
        st.delivered.len(),
        st.gaps.len(),
        st.catalog_dirty
    );
    for b in &st.batches {
        let done = b.products.values().filter(|p| p.finished()).count();
        println!(
            "batch {} created {}: {done}/{} finished",
            b.id,
            b.created_utc,
            b.products.len()
        );
        for (p, pr) in b.products.iter().filter(|(_, p)| !p.finished()) {
            println!(
                "  {p}: {:?} kline_done={} {} {}",
                pr.stage,
                pr.kline_done,
                pr.period,
                pr.error.as_deref().map(first_line).unwrap_or_default()
            );
        }
    }
    for r in st.history.iter().rev().take(5) {
        println!(
            "{} ok={} {}",
            r.run,
            r.ok,
            r.detail.lines().next().unwrap_or("")
        );
    }
    Ok(())
}

fn main() -> Result<()> {
    match Cli::parse().cmd {
        Cmd::Run {
            config,
            products,
            dates,
            no_new,
            skip,
            no_notify,
            dry_run,
        } => {
            let cfg = load_config(&config)?;
            for s in &skip {
                ensure!(
                    ["kline", "open_interest", "catalog"].contains(&s.as_str()),
                    "unknown --skip {s}"
                );
            }
            for d in &dates {
                chrono::NaiveDate::parse_from_str(d, "%Y-%m-%d")
                    .with_context(|| format!("--dates {d}"))?;
            }
            let opts = Opts {
                products,
                dates,
                no_new,
                skip,
                dry_run,
            };
            run(&cfg, &opts, !no_notify && !dry_run)
        }
        Cmd::Merge {
            config,
            month,
            products,
            dry_run,
            delete_sources,
            no_notify,
        } => {
            let cfg = load_config(&config)?;
            let month = merge::resolve_month(&month, Utc::now().date_naive())?;
            merge::run(
                &cfg,
                &merge::Opts {
                    month,
                    products,
                    dry_run,
                    delete_sources,
                },
                !no_notify && !dry_run,
            )
        }
        Cmd::Status { config } => status(&load_config(&config)?),
        Cmd::VenuesPlan { config, out } => venues_plan(&load_config(&config)?, &out),
        Cmd::VenuesMigrate {
            config,
            plan,
            journal,
            only,
            apply,
        } => venues_migrate(
            &load_config(&config)?,
            &plan,
            &journal,
            &only,
            apply,
            venue_fix::DELIVERY_WRITERS,
        ),
        Cmd::TestNotify { config } => {
            let cfg = load_config(&config)?;
            let d =
                dingtalk::DingTalk::from_env(&cfg.dingtalk_webhook_env, &cfg.dingtalk_secret_env)?
                    .with_context(|| format!("{} is not set", cfg.dingtalk_webhook_env))?;
            d.send_text(&format!(
                "[{}] LSEG TAS 日处理：钉钉连通性测试",
                cfg.host_tag
            ))?;
            println!("sent");
            Ok(())
        }
    }
}

/// `venues-plan`: see venue_fix.rs. Reads directory names on the delivery disk and the HRD.
fn venues_plan(cfg: &Config, out: &Path) -> Result<()> {
    let dirs = venue_fix::review_dirs(&cfg.delivery_root)?;
    let years: BTreeSet<i32> = dirs
        .iter()
        .flat_map(|(_, y, _, _)| (*y - 3)..=(*y + 3))
        .collect();
    let roots = venue_fix::root_venues(&cfg.delivery_root, &years)?;
    let bares: std::collections::HashSet<String> = dirs
        .iter()
        .map(|(r, ..)| venue_fix::bare(r).to_string())
        .collect();
    let versions = venue_fix::hrd_versions(&cfg.hrd, &bares)?;
    fs::create_dir_all(out)?;
    let mut w = csv::Writer::from_path(out.join("venue_plan.csv"))?;
    let mut summary: BTreeMap<(String, String), usize> = BTreeMap::new();
    for (ric, year, first, last) in &dirs {
        let (venue, rule, evidence) = venue_fix::resolve(
            ric,
            *year,
            *first,
            versions.get(venue_fix::bare(ric)),
            &roots,
        );
        *summary.entry((rule.clone(), venue.clone())).or_default() += 1;
        w.serialize(venue_fix::Resolution {
            ric: ric.clone(),
            utc_year: *year,
            first_day: first.to_string(),
            last_day: last.to_string(),
            venue,
            rule,
            evidence,
        })?;
    }
    w.flush()?;
    let s: Vec<Value> = summary
        .iter()
        .map(|((rule, venue), n)| json!({"rule": rule, "venue": venue, "ric_years": n}))
        .collect();
    save_json(
        &out.join("venue_plan_summary.json"),
        &json!({"review_ric_years": dirs.len(), "by_rule": s}),
    )?;
    println!("{} review RIC-years; plan in {}", dirs.len(), out.display());
    Ok(())
}

fn remove_empty_dirs(dir: &Path) -> Result<()> {
    if !dir.is_dir() {
        return Ok(());
    }
    for e in fs::read_dir(dir)? {
        let e = e?;
        if e.file_type()?.is_dir() {
            remove_empty_dirs(&e.path())?;
        }
    }
    if fs::read_dir(dir)?.next().is_none() {
        fs::remove_dir(dir)?;
    }
    Ok(())
}

/// `venues-migrate`: see the subcommand doc and docs/lseg/venue_review.md.
fn venues_migrate(
    cfg: &Config,
    plan: &Path,
    journal: &Path,
    only: &[String],
    apply: bool,
    writers: &[&str],
) -> Result<()> {
    use venue_fix::FileStep;
    fs::create_dir_all(journal)?;
    let lock = File::create(cfg.work_root.join(".lseg_daily.lock"))?;
    ensure!(lock.try_lock().is_ok(), "an lseg-daily run holds the lock");
    let writers = venue_fix::running_writers(writers);
    if !writers.is_empty() {
        ensure!(!apply, "delivery writers are running: {writers:?}");
        eprintln!("lseg-daily: note, delivery writers running (dry run only): {writers:?}");
    }
    let mut rows: Vec<venue_fix::PlanRow> = Vec::new();
    for r in csv::Reader::from_path(plan)?.deserialize::<venue_fix::PlanRow>() {
        let r = r?;
        if r.venue != "review" && (only.is_empty() || only.contains(&r.ric)) {
            rows.push(r);
        }
    }
    let mut log = OpenOptions::new()
        .create(true)
        .append(true)
        .open(journal.join("migrate.jsonl"))?;
    let (mut files_linked, mut files_same, mut already_moved) = (0u64, 0u64, 0u64);
    let mut files_kept_target = 0u64;
    let mut moved: Vec<venue_fix::PlanRow> = Vec::new();
    let mut conflicts = Vec::new();
    for row in rows {
        let comp = lseg_delivery::tasdb::component(&row.ric);
        let year = row.utc_year.to_string();
        let src = cfg.delivery_root.join("review").join(&year).join(&comp);
        let dst = cfg.delivery_root.join(&row.venue).join(&year).join(&comp);
        if !src.exists() {
            ensure!(
                dst.exists(),
                "{} {}: neither {} nor {} exists",
                row.ric,
                year,
                src.display(),
                dst.display()
            );
            already_moved += 1;
            moved.push(row);
            continue;
        }
        let mut steps = Vec::new();
        for f in venue_fix::files_under(&src)? {
            let step = venue_fix::file_step(&src.join(&f), &dst.join(&f))?;
            steps.push((f, step));
        }
        // A differing derived file (kline / session_status, recomputed after the move) keeps
        // the target copy; the review copy is backed up locally. Anything else blocks.
        let bad: Vec<String> = steps
            .iter()
            .filter(|(f, s)| *s == FileStep::Conflict && !venue_fix::derived(f))
            .map(|(f, _)| f.display().to_string())
            .collect();
        if !bad.is_empty() {
            conflicts.push(json!({"ric": row.ric, "utc_year": row.utc_year, "venue": row.venue, "differing_files": bad}));
            continue;
        }
        for (f, step) in &steps {
            match step {
                FileStep::Link => files_linked += 1,
                FileStep::Conflict => files_kept_target += 1,
                _ => files_same += 1,
            }
            if !apply {
                continue;
            }
            let (s, d) = (src.join(f), dst.join(f));
            if *step == FileStep::Conflict {
                let b = journal
                    .join("review_backup")
                    .join(&year)
                    .join(&comp)
                    .join(f);
                fs::create_dir_all(b.parent().context("parent")?)?;
                fs::copy(&s, &b)?;
                ensure!(
                    lseg_delivery::tasdb::file_sha256(&s)?
                        == lseg_delivery::tasdb::file_sha256(&b)?,
                    "backup check {}",
                    b.display()
                );
            }
            if *step == FileStep::Link {
                fs::create_dir_all(d.parent().context("parent")?)?;
                fs::hard_link(&s, &d)
                    .with_context(|| format!("link {} -> {}", s.display(), d.display()))?;
                ensure!(
                    venue_fix::file_step(&s, &d)? == FileStep::AlreadyLinked,
                    "link check failed for {}",
                    d.display()
                );
            }
            writeln!(
                log,
                "{}",
                json!({"at": Utc::now().to_rfc3339(), "ric": row.ric, "utc_year": row.utc_year,
                       "src": s, "dst": d, "step": format!("{step:?}")})
            )?;
        }
        if apply {
            log.flush()?;
            log.sync_all()?;
            for (f, _) in &steps {
                fs::remove_file(src.join(f))?;
            }
            remove_empty_dirs(&src)?;
            writeln!(
                log,
                "{}",
                json!({"at": Utc::now().to_rfc3339(), "removed": src})
            )?;
        }
        moved.push(row);
    }
    let summary = json!({
        "apply": apply, "ric_years_moved": moved.len(), "already_moved": already_moved,
        "files_linked": files_linked, "files_identical_at_target": files_same,
        "derived_files_kept_at_target": files_kept_target,
        "conflicts": conflicts.len(),
    });
    if apply {
        let stamp = Utc::now().format("%Y%m%dT%H%M%SZ");
        fs::copy(
            &cfg.state_file,
            journal.join(format!("state.before.{stamp}.json")),
        )?;
        let mut st = State::load(&cfg.state_file)?;
        let approved: HashMap<String, String> =
            serde_json::from_value(read_json(&cfg.approved_exchanges)?["exchanges"].clone())?;
        for row in &moved {
            if st.venues.contains_key(&row.ric) || approved.contains_key(&row.ric) {
                st.venues.insert(row.ric.clone(), row.venue.clone());
            }
        }
        st.catalog_dirty = true;
        st.save(&cfg.state_file)?;
        let path = cfg
            .project
            .join(lseg_delivery::native_kline::engine::VENUE_OVERRIDES);
        let mut all: BTreeMap<(String, i32), venue_fix::PlanRow> = BTreeMap::new();
        if path.exists() {
            for r in csv::Reader::from_path(&path)?.deserialize::<venue_fix::OverrideRow>() {
                let r = r?;
                all.insert(
                    (r.ric.clone(), r.utc_year),
                    venue_fix::PlanRow {
                        ric: r.ric,
                        utc_year: r.utc_year,
                        venue: r.venue,
                        rule: r.rule,
                        evidence: r.evidence,
                    },
                );
            }
        }
        for r in &moved {
            all.insert((r.ric.clone(), r.utc_year), r.clone());
        }
        fs::create_dir_all(path.parent().context("overrides dir")?)?;
        let tmp = path.with_extension("csv.partial");
        let mut w = csv::Writer::from_path(&tmp)?;
        for r in all.values() {
            w.serialize(venue_fix::OverrideRow {
                ric: r.ric.clone(),
                utc_year: r.utc_year,
                venue: r.venue.clone(),
                rule: r.rule.clone(),
                evidence: r.evidence.clone(),
            })?;
        }
        w.flush()?;
        drop(w);
        fs::rename(&tmp, &path)?;
        if !conflicts.is_empty() {
            record_problem(
                cfg,
                "venue_migration.json",
                &json!({"at": Utc::now().to_rfc3339(), "conflicts": conflicts}),
            );
        }
    }
    println!("{summary}");
    for c in &conflicts {
        println!("conflict: {c}");
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn push_text_is_only_problems() {
        let mut sum = Summary {
            new_files: 524,
            products_done: (0..117).map(|i| format!("P{i}")).collect(),
            trade_rows: 18_694_686,
            ..Summary::default()
        };
        let st = State::default();
        // A normal run (stats, replaced files, long run) has nothing to push.
        sum.kline.groups_ok = (0..115).map(|i| format!("2026/G{i}")).collect();
        sum.kline.replaced.insert("correction".into(), 1);
        assert_eq!(problems_brief(&sum, &st, None, Some("http://h:18313/")), "");
        sum.kline.groups_failed = (0..9)
            .map(|i| {
                (
                    format!("2026/F{i}"),
                    "lseg_kline exited with exit status: 1; log /x".into(),
                )
            })
            .collect();
        let text = problems_brief(&sum, &st, None, Some("http://h:18313/"));
        assert!(text.contains("K 线失败 9 个") && text.contains("等 9 个"));
        assert!(text.contains("详情：http://h:18313/"));
        assert!(!text.contains("万行") && !text.contains("耗时") && !text.contains("/mnt/"));
        assert!(text.lines().count() <= 3, "{text}");
    }

    pub(crate) fn cfg(root: &Path) -> Config {
        toml::from_str(&cfg_text(root)).unwrap()
    }

    fn cfg_text(root: &Path) -> String {
        format!(
            r#"remote_root = "http://x/"
raw_root = "{r}/raw"
work_root = "{r}/work"
db_root = "{r}/db"
export_root = "{r}/export"
delivery_root = "{r}/delivery"
state_file = "{r}/state.json"
project = "{r}"
column_rules = "{r}/rules.json"
approved_exchanges = "{r}/approved.json"
hrd = []
kline_base_plan = "{r}/plan.json"
kline_cache = "{r}/cache"
kline_witnesses = "{r}/witnesses"
dingtalk_webhook_env = "W"
dingtalk_secret_env = "S"
[workers]
download = 2
stage = 2
replay = 2
parse = 2
export = 1
status = 1
kline = 1
"#,
            r = root.display()
        )
    }

    #[test]
    fn venues_migrate_moves_verified_links_and_updates_state() {
        let t = tempfile::tempdir().unwrap();
        let root = t.path();
        let c = cfg(root);
        let del = &c.delivery_root;
        let put = |rel: &str, body: &[u8]| {
            let p = del.join(rel);
            fs::create_dir_all(p.parent().unwrap()).unwrap();
            fs::write(p, body).unwrap();
        };
        put("review/2022/TNH2/20220110/kline.parquet", b"k");
        put("review/2022/TNH2/20220110/trade.parquet", b"t");
        put("review/2022/TNH2/l2_source", b"raw");
        put("review/2026/TYH27/20261120/kline.parquet", b"y");
        put("cbot/2026/TYH27/20261120/kline.parquet", b"y"); // identical bytes already there
        put("review/2023/TNZ3/20230110/trade.parquet", b"a");
        put("cbot/2023/TNZ3/20230110/trade.parquet", b"b"); // conflict on a source file
        put(
            "review/2026/ESU31/20260617/session_status.parquet",
            b"unknown",
        );
        put("review/2026/ESU31/20261120/status.parquet", b"s");
        put("cme/2026/ESU31/20260617/session_status.parquet", b"decoded"); // derived: target wins
        fs::create_dir_all(&c.work_root).unwrap();
        fs::write(
            &c.approved_exchanges,
            r#"{"exchanges": {"TYH27": "review"}}"#,
        )
        .unwrap();
        State::default().save(&c.state_file).unwrap();
        let plan = root.join("plan.csv");
        fs::write(
            &plan,
            "ric,utc_year,first_day,last_day,venue,rule,evidence\n\
             TNH2,2022,2022-01-10,2022-01-10,cbot,hrd_by_date,e1\n\
             TYH27,2026,2026-11-20,2026-11-20,cbot,root_follow,e2\n\
             TNZ3,2023,2023-01-10,2023-01-10,cbot,hrd_by_date,e3\n\
             ESU31,2026,2026-06-17,2026-11-20,cme,root_follow,e5\n\
             QQH7,2027,2027-01-01,2027-01-01,review,unresolved,e4\n",
        )
        .unwrap();
        let journal = root.join("journal");
        // Dry run changes nothing.
        venues_migrate(&c, &plan, &journal, &[], false, &[]).unwrap();
        assert!(del.join("review/2022/TNH2/20220110/kline.parquet").exists());
        assert!(!del.join("cbot/2022/TNH2").exists());
        venues_migrate(&c, &plan, &journal, &[], true, &[]).unwrap();
        assert_eq!(
            fs::read(del.join("cbot/2022/TNH2/20220110/trade.parquet")).unwrap(),
            b"t"
        );
        assert_eq!(
            fs::read(del.join("cbot/2022/TNH2/l2_source")).unwrap(),
            b"raw"
        );
        assert!(!del.join("review/2022/TNH2").exists());
        assert!(!del.join("review/2026/TYH27").exists());
        // The conflicting RIC-year stays where it was.
        assert_eq!(
            fs::read(del.join("review/2023/TNZ3/20230110/trade.parquet")).unwrap(),
            b"a"
        );
        assert_eq!(
            fs::read(del.join("cbot/2023/TNZ3/20230110/trade.parquet")).unwrap(),
            b"b"
        );
        // Derived conflict: the decoded target copy stays, the review copy is backed up.
        assert_eq!(
            fs::read(del.join("cme/2026/ESU31/20260617/session_status.parquet")).unwrap(),
            b"decoded"
        );
        assert_eq!(
            fs::read(del.join("cme/2026/ESU31/20261120/status.parquet")).unwrap(),
            b"s"
        );
        assert!(!del.join("review/2026/ESU31").exists());
        assert_eq!(
            fs::read(journal.join("review_backup/2026/ESU31/20260617/session_status.parquet"))
                .unwrap(),
            b"unknown"
        );
        let st = State::load(&c.state_file).unwrap();
        assert_eq!(st.venues.get("TYH27").map(String::as_str), Some("cbot"));
        assert!(st.catalog_dirty);
        let ov = fs::read_to_string(
            c.project
                .join(lseg_delivery::native_kline::engine::VENUE_OVERRIDES),
        )
        .unwrap();
        assert!(ov.contains("TNH2,2022,cbot,hrd_by_date,e1") && ov.contains("TYH27,2026,cbot"));
        assert!(!ov.contains("TNZ3") && !ov.contains("QQH7"));
        // A second run is a no-op.
        venues_migrate(&c, &plan, &journal, &[], true, &[]).unwrap();
        assert!(fs::read_to_string(journal.join("migrate.jsonl"))
            .unwrap()
            .contains("removed"));
    }

    #[test]
    fn open_interest_section_is_optional() {
        let root = Path::new("/x");
        assert!(cfg(root).open_interest.is_none());
        let text = format!(
            "{}\n[open_interest]\ndatabase_url = \"postgresql:///m\"\n",
            cfg_text(root)
        );
        let oi = toml::from_str::<Config>(&text)
            .unwrap()
            .open_interest
            .unwrap();
        assert_eq!(
            (oi.database_url.as_str(), oi.workers),
            ("postgresql:///m", 1)
        );
    }

    #[test]
    fn shipped_configs_load_and_production_needs_flag() {
        let dir = Path::new(env!("CARGO_MANIFEST_DIR")).join("../../../config");
        let prod = dir.join("lseg_daily.toml");
        assert!(load_config(&prod).unwrap().production);
        assert!(
            !load_config(&dir.join("lseg_daily_staging.toml"))
                .unwrap()
                .production
        );
        let t = tempfile::tempdir().unwrap();
        let path = t.path().join("c.toml");
        let src = fs::read_to_string(&prod).unwrap();
        fs::write(
            &path,
            src.replace("production = true", "production = false"),
        )
        .unwrap();
        assert!(load_config(&path).is_err());
    }

    #[test]
    fn plan_appends_daily_jobs_after_base() {
        let t = tempfile::tempdir().unwrap();
        let c = cfg(t.path());
        fs::write(
            &c.kline_base_plan,
            r#"{"version":1,"jobs":[{"id":"main__p%3A2026%3AX","db":"/d","cf":"p:2026:X","year":2026,"product":"X","period":"2026-01-01_2026-06-01","source":"main","source_index":7}]}"#,
        )
        .unwrap();
        assert_eq!(base_next_index(&c).unwrap(), 8);
        let mut st = State::default();
        let mut b = new_batch(
            "B1",
            BTreeMap::from([(
                "XX".to_string(),
                vec![PlannedFile {
                    date: "2026-11-10".into(),
                    name: "XX_2026-11-10_tick.csv.gz".into(),
                    url: "u".into(),
                    remote: remote::Fingerprint {
                        bytes: 1,
                        etag: None,
                        last_modified: None,
                    },
                    source_file: None,
                    sha256: None,
                }],
            )]),
        )
        .unwrap();
        let pr = b.products.get_mut("XX").unwrap();
        assert_eq!(pr.period, "2026-11-10_2026-11-11");
        pr.stage = Stage::Done;
        pr.db = Some("/db/B1/XX/attempt_1".into());
        pr.plan_index = Some(8);
        pr.cfs = BTreeMap::from([("p:2026:X".into(), 3), ("p:2026:XTOT".into(), 0)]);
        st.batches.push(b);
        let plan = kline_plan(&c, &st).unwrap();
        let jobs = plan["jobs"].as_array().unwrap();
        assert_eq!(jobs.len(), 3);
        assert_eq!(jobs[1]["id"], "dailyB1_XX__p%3A2026%3AX");
        assert_eq!(jobs[1]["source_index"], 8);
        assert_eq!(jobs[2]["product"], "XTOT");
        assert_eq!(jobs[2]["source_index"], 9);
        assert_eq!(jobs[1]["coverage_days"], json!(["2026-11-10"]));
    }

    #[test]
    fn replacement_reasons() {
        let late = BTreeSet::from(["20261104".to_string()]);
        assert_eq!(reason("20261104", &late, 1440, 1440, false), "late_day");
        assert_eq!(
            reason("20261107", &late, 601, 1440, true),
            "tail_completion"
        );
        assert_eq!(reason("20261105", &late, 1440, 1440, false), "correction");
        assert_eq!(reason("20261105", &late, 601, 1440, false), "correction");
    }

    #[test]
    fn replay_counters_parse_finished_line() {
        let t = tempfile::tempdir().unwrap();
        let log = t.path().join("r.log");
        fs::write(
            &log,
            "x\n[2026] cme_tas_replay_all finished workers=8 parts=1 periods=[\"a\"] source_rows=12 written_trades=3 skipped_unparsed=0 elapsed_ms=5\n",
        )
        .unwrap();
        let c = counters(&log).unwrap();
        assert_eq!(
            (c["source_rows"], c["written_trades"], c["skipped_unparsed"]),
            (12, 3, 0)
        );
        assert!(!c.contains_key("periods"));
    }
}
