//! `lseg-daily merge`: monthly merge of finished daily TAS DBs.
//!
//! Per product directory (one at a time):
//! 1. sources: every finished daily DB (stage Done + kline_done, catalog
//!    clean) whose first delivered day falls in the month, plus an earlier
//!    merged DB of the same month if late days arrived since (re-merge);
//! 2. `tas_db_merge merge`: streaming k-way copy into
//!    `<monthly_db_root>/<YYYY-MM>/<product>/attempt_N`, full bottommost
//!    compaction, read-only count/digest verification against the audits;
//! 3. plan with the merged DB in place of its sources; `lseg_kline check-plan`;
//!    per (year, CF product) group: delivered kline/session_status files from
//!    the day before the first merged day are copied into a staging root,
//!    `lseg_kline run --backup-root` recomputes them there, and nothing may be
//!    replaced, added or changed (`tas_db_merge check-staging`); the group
//!    stats must equal the last production run of the group;
//! 4. only then the state switches: the sources are retired (`merged_into`,
//!    `<db>.retired.json`), and deleted only with `--delete-sources`.
//!
//! The 20261109 pinned batch is not merged: it is part of the base plan
//! (kept verbatim; its job JSON is the identity of the extract cache) and is
//! already one DB per product.
use anyhow::{bail, ensure, Context, Result};
use chrono::{Datelike, NaiveDate, Utc};
use serde_json::{json, Value};
use std::collections::{BTreeMap, BTreeSet};
use std::fs::{self, File};
use std::path::{Path, PathBuf};
use std::time::Instant;

use super::{
    bin_dir, cf_parts, kline_plan, next_day, notify_as, product_dir, publish, read_json, run_tool,
    save_json, short, Config,
};
use crate::state::{Merged, MergedFrom, State};

pub struct Opts {
    pub month: String,
    pub products: Vec<String>,
    pub dry_run: bool,
    pub delete_sources: bool,
}

pub fn monthly_root(cfg: &Config) -> PathBuf {
    cfg.monthly_db_root.clone().unwrap_or_else(|| {
        cfg.db_root
            .parent()
            .unwrap_or(Path::new("/"))
            .join("cme_tas_monthly")
    })
}

fn compare_root(cfg: &Config) -> &Path {
    cfg.merge_compare_root
        .as_deref()
        .unwrap_or(&cfg.delivery_root)
}

/// `YYYY-MM`, or `previous` = the month before `today`.
pub fn resolve_month(arg: &str, today: NaiveDate) -> Result<String> {
    if arg == "previous" {
        let first = today.with_day(1).context("month start")?;
        let prev = first.pred_opt().context("date underflow")?;
        return Ok(format!("{:04}-{:02}", prev.year(), prev.month()));
    }
    NaiveDate::parse_from_str(&format!("{arg}-01"), "%Y-%m-%d")
        .with_context(|| format!("--month {arg} (YYYY-MM or previous)"))?;
    ensure!(arg.len() == 7, "--month {arg} (YYYY-MM)");
    Ok(arg.to_string())
}

/// Month rule: a daily DB belongs, as a whole, to the UTC month of its first
/// delivered day. Batches are never split by key time: no key can be lost or
/// duplicated, quote keys (event-second buckets, possibly on the other side
/// of a day/month boundary from their publication) stay with their source,
/// and the coverage days are carried over unchanged.
pub fn month_of(days: &[String]) -> Option<String> {
    days.iter().min().map(|d| d[..7].to_string())
}

#[derive(Clone, Debug, PartialEq)]
pub enum Src {
    Daily { bi: usize, product: String },
    Merged(usize),
}

/// Merge sources per product directory, in plan order. A product appears
/// only if it has at least one daily DB not merged yet.
pub fn select(st: &State, month: &str, products: &[String]) -> Result<BTreeMap<String, Vec<Src>>> {
    let mut out: BTreeMap<String, Vec<(u32, Src)>> = BTreeMap::new();
    for (bi, b) in st.batches.iter().enumerate() {
        for (p, pr) in &b.products {
            if !products.is_empty() && !products.contains(p) {
                continue;
            }
            if !pr.finished()
                || pr.db.is_none()
                || pr.merged_into.is_some()
                || pr.db_deleted
                || month_of(&pr.dates()).as_deref() != Some(month)
            {
                continue;
            }
            let index = pr
                .plan_index
                .with_context(|| format!("{}/{p}: no plan index", b.id))?;
            out.entry(p.clone()).or_default().push((
                index,
                Src::Daily {
                    bi,
                    product: p.clone(),
                },
            ));
        }
    }
    for (i, m) in st.merged.iter().enumerate() {
        if m.month == month && m.retired_by.is_none() {
            if let Some(v) = out.get_mut(&m.product) {
                v.push((m.plan_index, Src::Merged(i)));
            }
        }
    }
    Ok(out
        .into_iter()
        .map(|(p, mut v)| {
            v.sort_by_key(|(i, _)| *i);
            (p, v.into_iter().map(|(_, s)| s).collect())
        })
        .collect())
}

/// What one source contributes (read from state).
struct SourceInfo {
    from: MergedFrom,
    cfs: BTreeMap<String, u64>,
    exchange_map: PathBuf,
    /// Per-CF counts by kind from the source's audit / merge report.
    expected: BTreeMap<String, BTreeMap<String, u64>>,
}

fn kinds(v: &Value) -> BTreeMap<String, u64> {
    v.as_object()
        .map(|o| {
            o.iter()
                .filter_map(|(k, n)| Some((k.clone(), n.as_u64()?)))
                .collect()
        })
        .unwrap_or_default()
}

fn source_info(cfg: &Config, st: &State, s: &Src) -> Result<SourceInfo> {
    match s {
        Src::Daily { bi, product } => {
            let b = &st.batches[*bi];
            let pr = &b.products[product];
            let db = pr.db.clone().context("db")?;
            let audit = read_json(pr.audit.as_ref().context("daily DB without audit")?)?;
            let expected = pr
                .cfs
                .keys()
                .map(|cf| (cf.clone(), kinds(&audit["products"][cf]["kinds"])))
                .collect();
            Ok(SourceInfo {
                from: MergedFrom {
                    id: format!("daily{}_{product}", b.id),
                    current: fs::read_to_string(db.join("CURRENT"))
                        .with_context(|| format!("{}: CURRENT", db.display()))?,
                    db,
                    coverage_days: pr.dates(),
                    batch: Some(b.id.clone()),
                },
                cfs: pr.cfs.clone(),
                exchange_map: product_dir(cfg, &b.id, product).join("exchange_map.json"),
                expected,
            })
        }
        Src::Merged(i) => {
            let m = &st.merged[*i];
            let report = read_json(&m.report)?;
            let expected = m
                .cfs
                .keys()
                .map(|cf| (cf.clone(), kinds(&report["cfs"][cf]["merged"])))
                .collect();
            Ok(SourceInfo {
                from: MergedFrom {
                    id: m.id.clone(),
                    current: fs::read_to_string(m.db.join("CURRENT"))?,
                    db: m.db.clone(),
                    coverage_days: m.coverage_days.clone(),
                    batch: None,
                },
                cfs: m.cfs.clone(),
                exchange_map: m.exchange_map.clone(),
                expected,
            })
        }
    }
}

/// Union of the sources' coverage days; any day delivered twice fails.
pub fn union_days(lists: &[&[String]]) -> Result<Vec<String>> {
    let mut all = BTreeSet::new();
    for l in lists {
        for d in *l {
            ensure!(all.insert(d.clone()), "day {d} is covered by two sources");
        }
    }
    Ok(all.into_iter().collect())
}

/// Union of exchange maps; a RIC with two venues fails.
pub fn union_maps(maps: &[BTreeMap<String, String>]) -> Result<BTreeMap<String, String>> {
    let mut out = BTreeMap::new();
    for m in maps {
        for (r, v) in m {
            if let Some(old) = out.insert(r.clone(), v.clone()) {
                ensure!(old == *v, "RIC {r} has two venues: {old} / {v}");
            }
        }
    }
    Ok(out)
}

/// Switches the plan to `m`: its sources are retired.
pub fn apply(st: &mut State, m: Merged, srcs: &[Src]) {
    for s in srcs {
        match s {
            Src::Daily { bi, product } => {
                st.batches[*bi]
                    .products
                    .get_mut(product)
                    .unwrap()
                    .merged_into = Some(m.id.clone())
            }
            Src::Merged(i) => st.merged[*i].retired_by = Some(m.id.clone()),
        }
    }
    st.merged.push(m);
}

fn day_before(d: &str) -> Result<String> {
    Ok(NaiveDate::parse_from_str(d, "%Y-%m-%d")?
        .pred_opt()
        .context("date underflow")?
        .format("%Y-%m-%d")
        .to_string())
}

/// A retired DB may only be deleted if it is under one of our DB roots, still
/// the version that was merged, and no plan job reads it.
pub fn deletable(cfg: &Config, db: &Path, current: &str, plan: &Value) -> Result<()> {
    let roots = [cfg.db_root.clone(), monthly_root(cfg)];
    ensure!(
        db.is_absolute()
            && roots
                .iter()
                .any(|r| db.starts_with(r) && db.components().count() > r.components().count() + 1),
        "{} is outside the lseg-daily DB roots",
        db.display()
    );
    ensure!(
        !db.components().any(|c| c.as_os_str() == ".."),
        "{} is not a plain path",
        db.display()
    );
    ensure!(
        fs::read_to_string(db.join("CURRENT"))? == current,
        "{} changed since it was merged",
        db.display()
    );
    let used = plan["jobs"]
        .as_array()
        .context("plan jobs")?
        .iter()
        .any(|j| j["db"].as_str().is_some_and(|p| Path::new(p) == db));
    ensure!(!used, "{} is still read by the plan", db.display());
    Ok(())
}

#[derive(Default)]
struct Sizes {
    dbs: u64,
    ssts: u64,
    bytes: u64,
}

fn du(path: &Path) -> Sizes {
    let mut s = Sizes {
        dbs: 1,
        ..Sizes::default()
    };
    for e in fs::read_dir(path).into_iter().flatten().flatten() {
        if let Ok(m) = e.metadata() {
            if m.is_file() {
                s.bytes += m.len();
                if e.file_name().to_string_lossy().ends_with(".sst") {
                    s.ssts += 1;
                }
            }
        }
    }
    s
}

#[derive(Default)]
pub struct Summary {
    pub month: String,
    pub ok: Vec<String>,
    pub failed: Vec<(String, String)>,
    pub before: (u64, u64, u64),
    pub after: (u64, u64, u64),
    pub groups: usize,
    pub quote_folded: u64,
    pub deleted: (u64, u64),
    pub retained: u64,
    pub blocked: Option<String>,
}

/// Result of one product's merge + verification (state not yet switched).
struct Done {
    merged: Merged,
    srcs: Vec<Src>,
    before: Sizes,
    after: Sizes,
    groups: usize,
    quote_folded: u64,
}

/// [`try_merge_product`]; a failed attempt's new DB (never referenced by
/// the state) is removed, its work directory with the logs is kept.
fn merge_product(
    cfg: &Config,
    st: &State,
    bins: &Path,
    month: &str,
    product: &str,
    srcs: &[Src],
) -> Result<Done> {
    let mut target = None;
    let r = try_merge_product(cfg, st, bins, month, product, srcs, &mut target);
    if let (Err(_), Some(t)) = (&r, &target) {
        if t.exists() {
            if let Err(e) = fs::remove_dir_all(t) {
                eprintln!(
                    "lseg-daily merge: cannot remove failed {}: {e:#}",
                    t.display()
                );
            }
        }
    }
    r
}

fn try_merge_product(
    cfg: &Config,
    st: &State,
    bins: &Path,
    month: &str,
    product: &str,
    srcs: &[Src],
    created: &mut Option<PathBuf>,
) -> Result<Done> {
    let infos = srcs
        .iter()
        .map(|s| source_info(cfg, st, s))
        .collect::<Result<Vec<_>>>()?;
    let days = union_days(
        &infos
            .iter()
            .map(|i| i.from.coverage_days.as_slice())
            .collect::<Vec<_>>(),
    )?;
    let first = days.first().context("no coverage days")?.clone();
    let period = format!("{first}_{}", next_day(days.last().unwrap())?);
    let base = monthly_root(cfg).join(month).join(product);
    let wbase = cfg.work_root.join("merge").join(month).join(product);
    let mut n = 1;
    while base.join(format!("attempt_{n}")).exists() || wbase.join(format!("attempt_{n}")).exists()
    {
        n += 1;
    }
    let id = format!("{month}/{product}/attempt_{n}");
    let target = base.join(format!("attempt_{n}"));
    let mdir = wbase.join(format!("attempt_{n}"));
    fs::create_dir_all(&mdir)?;
    *created = Some(target.clone());

    let maps = infos
        .iter()
        .map(|i| Ok(serde_json::from_value(read_json(&i.exchange_map)?)?))
        .collect::<Result<Vec<BTreeMap<String, String>>>>()?;
    let exchange_map = mdir.join("exchange_map.json");
    save_json(&exchange_map, &union_maps(&maps)?)?;

    let mut cfs: BTreeMap<String, u64> = BTreeMap::new();
    for i in &infos {
        for (cf, n) in &i.cfs {
            *cfs.entry(cf.clone()).or_default() += n;
        }
    }
    let spec = json!({
        "target": target, "period": period, "compaction_threads": cfg.workers.merge_compaction,
        "sources": infos.iter().map(|i| json!({
            "id": i.from.id, "db": i.from.db, "cfs": i.cfs.keys().collect::<Vec<_>>(),
            "current": i.from.current, "expected": i.expected,
        })).collect::<Vec<_>>(),
    });
    let spec_path = mdir.join("spec.json");
    save_json(&spec_path, &spec)?;
    let report_path = mdir.join("report.json");
    run_tool(
        cfg,
        &bins.join("tas_db_merge"),
        "tas_db_merge",
        &[
            "merge".into(),
            "--spec".into(),
            spec_path.display().to_string(),
            "--report".into(),
            report_path.display().to_string(),
        ],
        &mdir.join("merge.log"),
    )?;
    let report = read_json(&report_path)?;
    ensure!(report["status"] == "passed", "merge report not passed");
    let total = |kind: &str| -> u64 {
        report["cfs"]
            .as_object()
            .into_iter()
            .flatten()
            .map(|(_, c)| c["merged"][kind].as_u64().unwrap_or(0))
            .sum()
    };
    let quote_folded = report["cfs"]
        .as_object()
        .into_iter()
        .flatten()
        .map(|(_, c)| c["quote_folded"].as_u64().unwrap_or(0))
        .sum();

    // Tentative switch for the verification plan (same indices as the commit).
    let mut tentative = st.clone();
    let plan_index = tentative.next_source_index.context("source index")?;
    tentative.next_source_index = Some(plan_index + u32::try_from(cfs.len())?);
    let merged = Merged {
        id,
        month: month.into(),
        product: product.into(),
        db: target.clone(),
        cfs,
        plan_index,
        period,
        coverage_days: days,
        exchange_map,
        report: report_path,
        merged_from: infos.iter().map(|i| i.from.clone()).collect(),
        created_utc: Utc::now().to_rfc3339(),
        trade_rows: total("1"),
        quote_rows: total("3"),
        retired_by: None,
        db_deleted: false,
    };
    apply(&mut tentative, merged.clone(), srcs);
    let plan_path = mdir.join("plan.json");
    save_json(&plan_path, &kline_plan(cfg, &tentative)?)?;
    let log = mdir.join("verify.log");
    let kline = bins.join("lseg_kline");
    run_tool(
        cfg,
        &kline,
        "lseg_kline",
        &[
            "check-plan".into(),
            "--plan".into(),
            plan_path.display().to_string(),
        ],
        &log,
    )?;
    let from = day_before(&first)?;
    let mut groups = BTreeSet::new();
    for cf in merged.cfs.keys() {
        groups.insert(cf_parts(cf)?);
    }
    let mut checks = Vec::new();
    for (year, cfp) in &groups {
        checks.push(verify_group(
            cfg, bins, &plan_path, &mdir, *year, cfp, &from,
        )?);
    }
    save_json(
        &mdir.join("verify.json"),
        &json!({"status": "passed", "compare_from": from, "groups": checks}),
    )?;
    let before = infos.iter().fold(Sizes::default(), |mut a, i| {
        let s = du(&i.from.db);
        a.dbs += 1;
        a.ssts += s.ssts;
        a.bytes += s.bytes;
        a
    });
    Ok(Done {
        after: du(&target),
        merged,
        srcs: srcs.to_vec(),
        before,
        groups: groups.len(),
        quote_folded,
    })
}

/// Latest production checkpoint of a kline group (`stats`), if any.
fn production_stats(cfg: &Config, kind: &str, key: &str, year: i32, cfp: &str) -> Option<Value> {
    let mut runs: Vec<PathBuf> = fs::read_dir(cfg.work_root.join("kline"))
        .ok()?
        .flatten()
        .map(|e| e.path())
        .collect();
    runs.sort();
    for run in runs.iter().rev() {
        for p in [
            run.join("groups")
                .join(format!("{year}_{}", publish::component(cfp)))
                .join("checkpoints")
                .join(kind)
                .join(format!("{key}.json")),
            run.join("checkpoints")
                .join(kind)
                .join(format!("{key}.json")),
        ] {
            if let Ok(v) = read_json(&p) {
                return Some(v["stats"].clone());
            }
        }
    }
    None
}

fn verify_group(
    cfg: &Config,
    bins: &Path,
    plan: &Path,
    mdir: &Path,
    year: i32,
    cfp: &str,
    from: &str,
) -> Result<Value> {
    let vdir = mdir
        .join("verify")
        .join(format!("{year}_{}", publish::component(cfp)));
    ensure!(!vdir.exists(), "{} exists", vdir.display());
    let staging = vdir.join("delivery");
    let work = vdir.join("work");
    let replaced = vdir.join("replaced.jsonl");
    let seeded = vdir.join("seeded.json");
    let log = vdir.join("verify.log");
    let tool = bins.join("tas_db_merge");
    let s = |p: &Path| p.display().to_string();
    let t = Instant::now();
    run_tool(
        cfg,
        &tool,
        "tas_db_merge",
        &[
            "seed".into(),
            "--from-root".into(),
            s(compare_root(cfg)),
            "--staging".into(),
            s(&staging),
            "--year".into(),
            year.to_string(),
            "--product".into(),
            cfp.into(),
            "--from".into(),
            from.into(),
            "--list".into(),
            s(&seeded),
        ],
        &log,
    )?;
    let seed_s = t.elapsed().as_secs_f64();
    let t = Instant::now();
    run_tool(
        cfg,
        &bins.join("lseg_kline"),
        "lseg_kline",
        &[
            "run".into(),
            "--project".into(),
            s(&cfg.project),
            "--plan".into(),
            s(plan),
            "--cache".into(),
            s(&cfg.kline_cache),
            "--work".into(),
            s(&work),
            "--output".into(),
            s(&staging),
            "--year".into(),
            year.to_string(),
            "--product".into(),
            cfp.into(),
            "--workers".into(),
            cfg.workers.kline.to_string(),
            "--backup-root".into(),
            s(&vdir.join("backups")),
            "--compare-from".into(),
            from.into(),
            "--replaced-log".into(),
            s(&replaced),
        ],
        &log,
    )?;
    let kline_s = t.elapsed().as_secs_f64();
    let check = vdir.join("check.json");
    run_tool(
        cfg,
        &tool,
        "tas_db_merge",
        &[
            "check-staging".into(),
            "--staging".into(),
            s(&staging),
            "--year".into(),
            year.to_string(),
            "--product".into(),
            cfp.into(),
            "--from".into(),
            from.into(),
            "--list".into(),
            s(&seeded),
            "--replaced-log".into(),
            s(&replaced),
            "--output".into(),
            s(&check),
        ],
        &log,
    )?;
    let key = publish::component(&format!("{year}:{cfp}"));
    let mut stats = BTreeMap::new();
    for kind in ["groups", "status_only_groups"] {
        let mine = read_json(
            &work
                .join("checkpoints")
                .join(kind)
                .join(format!("{key}.json")),
        )
        .ok()
        .map(|v| v["stats"].clone());
        let prod = production_stats(cfg, kind, &key, year, cfp);
        if let (Some(a), Some(b)) = (&mine, &prod) {
            ensure!(
                a == b,
                "{year}/{cfp}: {kind} stats {a} differ from the last production run {b}"
            );
        }
        stats.insert(
            kind,
            json!({"staging": mine, "production": prod, "compared": mine.is_some() && prod.is_some()}),
        );
    }
    // The staging tree is a full year of day files; keep only the evidence.
    fs::remove_dir_all(&staging)?;
    Ok(
        json!({"year": year, "product": cfp, "check": read_json(&check)?,
              "stats": stats, "seconds": {"seed": seed_s, "kline": kline_s}}),
    )
}

fn retire_marker(db: &Path, by: &str) -> Result<()> {
    let mut name = db.file_name().context("db name")?.to_os_string();
    name.push(".retired.json");
    save_json(
        &db.with_file_name(name),
        &json!({"merged_into": by, "at": Utc::now().to_rfc3339(),
                "note": "retired by lseg-daily merge; not read by the kline plan"}),
    )
}

/// Deletes the retired sources of this month's merged DBs (verified merges
/// only; each source re-checked by [`deletable`]).
fn delete_sources(cfg: &Config, st: &mut State, month: &str, sum: &mut Summary) -> Result<()> {
    let plan = kline_plan(cfg, st)?;
    let mut todo = Vec::new();
    for m in st
        .merged
        .iter()
        .filter(|m| m.month == month && m.retired_by.is_none())
    {
        for f in &m.merged_from {
            todo.push((m.id.clone(), f.clone()));
        }
    }
    for (by, f) in todo {
        let gone = st
            .batches
            .iter()
            .flat_map(|b| b.products.values())
            .any(|p| p.db.as_deref() == Some(f.db.as_path()) && p.db_deleted)
            || st.merged.iter().any(|m| m.db == f.db && m.db_deleted);
        if gone || !f.db.exists() {
            continue;
        }
        deletable(cfg, &f.db, &f.current, &plan)?;
        let bytes = du(&f.db).bytes;
        fs::remove_dir_all(&f.db).with_context(|| format!("delete {}", f.db.display()))?;
        eprintln!(
            "lseg-daily merge: deleted {} (merged into {by})",
            f.db.display()
        );
        sum.deleted.0 += 1;
        sum.deleted.1 += bytes;
        for b in st.batches.iter_mut() {
            for p in b.products.values_mut() {
                if p.db.as_deref() == Some(f.db.as_path()) {
                    p.db_deleted = true;
                }
            }
        }
        for m in st.merged.iter_mut().filter(|m| m.db == f.db) {
            m.db_deleted = true;
        }
        st.save(&cfg.state_file)?;
    }
    Ok(())
}

pub fn run(cfg: &Config, opts: &Opts, notify_on: bool) -> Result<()> {
    fs::create_dir_all(&cfg.work_root)?;
    let lock = File::create(cfg.work_root.join(".lseg_daily.lock"))?;
    if lock.try_lock().is_err() {
        bail!("another lseg-daily run holds the lock; merge waits for the next slot");
    }
    let started = Instant::now();
    let mut st = State::load(&cfg.state_file)?;
    let mut sum = Summary {
        month: opts.month.clone(),
        ..Summary::default()
    };
    let result = (|| -> Result<()> {
        let open = st
            .batches
            .iter()
            .flat_map(|b| b.products.values())
            .filter(|p| {
                !p.finished() && month_of(&p.dates()).as_deref() == Some(opts.month.as_str())
            })
            .count();
        if st.catalog_dirty {
            sum.blocked = Some("目录尚未重建（等 lseg-daily 成功运行一次）".into());
            return Ok(());
        }
        let plan = select(&st, &opts.month, &opts.products)?;
        if opts.dry_run {
            println!(
                "merge {}: {} products; {open} unfinished daily runs of this month wait",
                opts.month,
                plan.len()
            );
            for (p, srcs) in &plan {
                let infos = srcs
                    .iter()
                    .map(|s| source_info(cfg, &st, s))
                    .collect::<Result<Vec<_>>>()?;
                let sz = infos
                    .iter()
                    .map(|i| du(&i.from.db))
                    .fold((0, 0), |a, s| (a.0 + s.ssts, a.1 + s.bytes));
                let days: Vec<&str> = infos
                    .iter()
                    .flat_map(|i| i.from.coverage_days.iter().map(String::as_str))
                    .collect();
                println!(
                    "  {p}: {} DBs ({} SST, {} bytes), days {}..{} -> {}",
                    srcs.len(),
                    sz.0,
                    sz.1,
                    days.iter().min().unwrap_or(&"-"),
                    days.iter().max().unwrap_or(&"-"),
                    monthly_root(cfg).join(&opts.month).join(p).display()
                );
            }
            return Ok(());
        }
        let bins = bin_dir(cfg)?;
        for (product, srcs) in &plan {
            eprintln!(
                "lseg-daily merge: {} {product}: {} sources",
                opts.month,
                srcs.len()
            );
            match merge_product(cfg, &st, &bins, &opts.month, product, srcs) {
                Ok(d) => {
                    let id = d.merged.id.clone();
                    let retire: Vec<PathBuf> =
                        d.merged.merged_from.iter().map(|f| f.db.clone()).collect();
                    let next = st.next_source_index.context("source index")?
                        + u32::try_from(d.merged.cfs.len())?;
                    apply(&mut st, d.merged, &d.srcs);
                    st.next_source_index = Some(next);
                    st.save(&cfg.state_file)?;
                    for db in &retire {
                        retire_marker(db, &id)?;
                    }
                    sum.ok.push(product.clone());
                    sum.before.0 += d.before.dbs;
                    sum.before.1 += d.before.ssts;
                    sum.before.2 += d.before.bytes;
                    sum.after.0 += 1;
                    sum.after.1 += d.after.ssts;
                    sum.after.2 += d.after.bytes;
                    sum.groups += d.groups;
                    sum.quote_folded += d.quote_folded;
                }
                Err(e) => {
                    let msg = format!("{e:#}");
                    eprintln!("lseg-daily merge: {product} failed: {msg}");
                    sum.failed.push((product.clone(), msg));
                }
            }
        }
        if opts.delete_sources {
            delete_sources(cfg, &mut st, &opts.month, &mut sum)?;
        }
        sum.retained = st
            .merged
            .iter()
            .filter(|m| m.month == opts.month && m.retired_by.is_none())
            .flat_map(|m| &m.merged_from)
            .filter(|f| f.db.exists())
            .count() as u64;
        Ok(())
    })();
    if opts.dry_run {
        return result;
    }
    let ok = result.is_ok() && sum.failed.is_empty();
    let title = if !ok {
        if sum.ok.is_empty() {
            "失败"
        } else {
            "部分失败"
        }
    } else if sum.ok.is_empty() {
        "无需合并"
    } else {
        "成功"
    };
    let text = brief(
        &sum,
        started.elapsed().as_secs_f64(),
        result.as_ref().err(),
        cfg.dashboard_url.as_deref(),
    );
    notify_as(
        cfg,
        notify_on,
        "LSEG 月度合并",
        &format!("{title} {}", sum.month),
        &text,
    );
    result?;
    ensure!(
        ok,
        "some products failed to merge; see the DingTalk summary and work_root/merge"
    );
    Ok(())
}

/// First line of an error without the log path that `run_tool` appends.
fn reason(e: &str) -> String {
    let l = e.lines().next().unwrap_or("");
    short(l.split("; log ").next().unwrap_or(l))
}

fn gb(b: u64) -> String {
    format!("{:.1} GB", b as f64 / 1e9)
}

pub fn brief(sum: &Summary, secs: f64, err: Option<&anyhow::Error>, url: Option<&str>) -> String {
    let mut s = Vec::new();
    if let Some(e) = err {
        s.push(format!("运行中断：{}", short(&format!("{e:#}"))));
    }
    if let Some(b) = &sum.blocked {
        s.push(format!("未合并：{b}"));
    } else if sum.ok.is_empty() && sum.failed.is_empty() {
        s.push("没有待合并的日库".into());
    }
    if !sum.ok.is_empty() {
        s.push(format!(
            "{} 个品种：日库 {} 个 → {} 个，SST {} → {}，占用 {} → {}",
            sum.ok.len(),
            sum.before.0,
            sum.after.0,
            sum.before.1,
            sum.after.1,
            gb(sum.before.2),
            gb(sum.after.2)
        ));
        s.push(format!(
            "校验通过：记录数逐类一致；K 线/状态与已发布文件逐值一致（{} 组）",
            sum.groups
        ));
    }
    if sum.deleted.0 > 0 {
        s.push(format!(
            "旧日库已删除 {} 个，释放 {}",
            sum.deleted.0,
            gb(sum.deleted.1)
        ));
    } else if sum.retained > 0 {
        s.push(format!("旧日库 {} 个已停用，暂保留", sum.retained));
    }
    if !sum.failed.is_empty() {
        s.push(format!(
            "⚠ {} 个品种失败（仍用日库）：{}；{}",
            sum.failed.len(),
            super::names(sum.failed.iter().map(|(p, _)| p.as_str())),
            reason(&sum.failed[0].1)
        ));
    }
    let mins = (secs / 60.0).round() as u64;
    s.push(if mins >= 60 {
        format!("耗时 {} 小时 {} 分", mins / 60, mins % 60)
    } else {
        format!("耗时 {mins} 分钟")
    });
    if let Some(u) = url {
        s.push(format!("详情：{u}"));
    }
    s.join("\n")
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::state::{Batch, ProductRun, Stage};

    fn pr(dates: &[&str], index: u32, done: bool) -> ProductRun {
        let mut p: ProductRun = serde_json::from_value(json!({
            "files": dates.iter().map(|d| json!({"date": d, "name": "n", "url": "u",
                "remote": {"bytes": 1, "etag": null, "last_modified": null}})).collect::<Vec<_>>(),
            "stage": "Done", "period": "x", "year": 2026, "db": format!("/db/{index}"),
            "cfs": {"p:2026:ES": 1}, "plan_index": index, "kline_done": done,
        }))
        .unwrap();
        p.stage = Stage::Done;
        p
    }

    fn state() -> State {
        let mut st = State::default();
        let batch = |id: &str, ps: Vec<(&str, ProductRun)>| Batch {
            id: id.into(),
            created_utc: String::new(),
            products: ps.into_iter().map(|(k, v)| (k.to_string(), v)).collect(),
        };
        st.batches = vec![
            batch(
                "B1",
                vec![
                    ("ES", pr(&["2026-10-29", "2026-10-30"], 10, true)),
                    ("CL", pr(&["2026-10-31"], 11, true)),
                ],
            ),
            // Spans the month boundary: belongs to October (first day).
            batch(
                "B2",
                vec![("ES", pr(&["2026-10-31", "2026-11-01"], 12, true))],
            ),
            batch(
                "B3",
                vec![
                    ("ES", pr(&["2026-11-02"], 13, true)),
                    ("CL", pr(&["2026-11-02"], 14, false)),
                ],
            ),
            // A late October day delivered in November.
            batch("B4", vec![("ES", pr(&["2026-10-15"], 15, true))]),
        ];
        st.next_source_index = Some(100);
        st
    }

    #[test]
    fn months_resolve_and_assign_by_first_day() {
        let d = NaiveDate::from_ymd_opt(2026, 12, 2).unwrap();
        assert_eq!(resolve_month("previous", d).unwrap(), "2026-11");
        let d = NaiveDate::from_ymd_opt(2027, 1, 2).unwrap();
        assert_eq!(resolve_month("previous", d).unwrap(), "2026-12");
        assert_eq!(resolve_month("2026-10", d).unwrap(), "2026-10");
        assert!(resolve_month("2026-13", d).is_err());
        assert!(resolve_month("2026-1", d).is_err());
        let days = |v: &[&str]| v.iter().map(|s| s.to_string()).collect::<Vec<_>>();
        assert_eq!(
            month_of(&days(&["2026-11-01", "2026-10-31"])).unwrap(),
            "2026-10"
        );
        assert_eq!(month_of(&[]), None);
    }

    #[test]
    fn select_takes_finished_month_sources_in_plan_order() {
        let st = state();
        let oct = select(&st, "2026-10", &[]).unwrap();
        let es: Vec<_> = oct["ES"]
            .iter()
            .map(|s| match s {
                Src::Daily { bi, .. } => *bi,
                _ => 99,
            })
            .collect();
        // B1, B2 (spans into November), B4 (late day); not B3.
        assert_eq!(es, vec![0, 1, 3]);
        assert_eq!(oct["CL"].len(), 1);
        let nov = select(&st, "2026-11", &[]).unwrap();
        // CL of B3 has no kline yet: not finished.
        assert!(!nov.contains_key("CL"));
        assert_eq!(nov["ES"].len(), 1);
        assert_eq!(select(&st, "2026-10", &["CL".into()]).unwrap().len(), 1);
    }

    fn merged(id: &str, month: &str, product: &str, index: u32) -> Merged {
        Merged {
            id: id.into(),
            month: month.into(),
            product: product.into(),
            db: format!("/m/{id}").into(),
            cfs: BTreeMap::from([("p:2026:ES".into(), 2)]),
            plan_index: index,
            period: "2026-10-01_2026-11-02".into(),
            coverage_days: vec!["2026-10-30".into()],
            exchange_map: "/w/x.json".into(),
            report: "/w/r.json".into(),
            merged_from: vec![],
            created_utc: String::new(),
            trade_rows: 0,
            quote_rows: 0,
            retired_by: None,
            db_deleted: false,
        }
    }

    #[test]
    fn switch_retires_sources_and_remerge_folds_the_old_merge() {
        let mut st = state();
        let srcs = select(&st, "2026-10", &["ES".into()]).unwrap()["ES"].clone();
        apply(
            &mut st,
            merged("2026-10/ES/attempt_1", "2026-10", "ES", 100),
            &srcs,
        );
        assert!(select(&st, "2026-10", &["ES".into()]).unwrap().is_empty());
        assert_eq!(
            st.batches[0].products["ES"].merged_into.as_deref(),
            Some("2026-10/ES/attempt_1")
        );
        // A further late October day: the earlier merge is a source again.
        st.batches.push(Batch {
            id: "B5".into(),
            created_utc: String::new(),
            products: BTreeMap::from([("ES".to_string(), pr(&["2026-10-16"], 120, true))]),
        });
        let again = select(&st, "2026-10", &["ES".into()]).unwrap()["ES"].clone();
        assert_eq!(
            again,
            vec![
                Src::Merged(0),
                Src::Daily {
                    bi: 4,
                    product: "ES".into()
                }
            ]
        );
        apply(
            &mut st,
            merged("2026-10/ES/attempt_2", "2026-10", "ES", 101),
            &again,
        );
        assert_eq!(
            st.merged[0].retired_by.as_deref(),
            Some("2026-10/ES/attempt_2")
        );
    }

    #[test]
    fn coverage_and_venues_must_not_conflict() {
        let a = vec!["2026-10-01".to_string(), "2026-10-02".into()];
        let b = vec!["2026-10-03".to_string()];
        assert_eq!(union_days(&[&a, &b]).unwrap().len(), 3);
        assert!(union_days(&[&a, &a[..1]]).is_err());
        let m = |r: &str, v: &str| BTreeMap::from([(r.to_string(), v.to_string())]);
        assert_eq!(
            union_maps(&[m("ESZ6", "cme"), m("ESZ6", "cme"), m("CLZ6", "nymex")])
                .unwrap()
                .len(),
            2
        );
        assert!(union_maps(&[m("ESZ6", "cme"), m("ESZ6", "cbot")]).is_err());
    }

    #[test]
    fn delete_guard() {
        let t = tempfile::tempdir().unwrap();
        let cfg = super::super::tests::cfg(t.path());
        let db = cfg.db_root.join("B1/ES/attempt_1");
        fs::create_dir_all(&db).unwrap();
        fs::write(db.join("CURRENT"), "MANIFEST-1\n").unwrap();
        let plan = json!({"jobs": [{"db": "/other"}]});
        deletable(&cfg, &db, "MANIFEST-1\n", &plan).unwrap();
        assert!(deletable(&cfg, &db, "MANIFEST-2\n", &plan).is_err());
        assert!(deletable(&cfg, &db, "MANIFEST-1\n", &json!({"jobs": [{"db": db}]})).is_err());
        // Never the roots themselves, a batch directory, or outside them.
        assert!(deletable(&cfg, &cfg.db_root.join("B1"), "x", &plan).is_err());
        assert!(deletable(
            &cfg,
            Path::new(
                "/mnt/nvme-raid0-28t/fanghaizhou/lseg_data/cme_tas_daily_20261109/ES/attempt_1"
            ),
            "x",
            &plan
        )
        .is_err());
        assert!(deletable(&cfg, &cfg.db_root.join("B1/ES/../../x/y"), "x", &plan).is_err());
    }

    #[test]
    fn plan_uses_the_merged_db_instead_of_its_sources() {
        let t = tempfile::tempdir().unwrap();
        let cfg = super::super::tests::cfg(t.path());
        fs::write(&cfg.kline_base_plan, r#"{"version":1,"jobs":[]}"#).unwrap();
        let mut st = state();
        let before = kline_plan(&cfg, &st).unwrap()["jobs"]
            .as_array()
            .unwrap()
            .len();
        let srcs = select(&st, "2026-10", &["ES".into()]).unwrap()["ES"].clone();
        apply(
            &mut st,
            merged("2026-10/ES/attempt_1", "2026-10", "ES", 100),
            &srcs,
        );
        let plan = kline_plan(&cfg, &st).unwrap();
        let jobs = plan["jobs"].as_array().unwrap();
        assert_eq!(jobs.len(), before - 3 + 1);
        let m = jobs
            .iter()
            .find(|j| j["db"] == "/m/2026-10/ES/attempt_1")
            .unwrap();
        assert_eq!(m["source_index"], 100);
        assert_eq!(m["coverage_days"], json!(["2026-10-30"]));
        assert_eq!(m["source"], "daily");
        assert!(jobs.iter().all(|j| j["db"] != "/db/10"));
    }

    #[test]
    fn merge_message_is_short_without_paths() {
        let sum = Summary {
            month: "2026-10".into(),
            ok: (0..117).map(|i| format!("P{i}")).collect(),
            failed: vec![(
                "CL".into(),
                "tas_db_merge exited with 1; log /mnt/x\nmore".into(),
            )],
            before: (2961, 14_000, 41_200_000_000),
            after: (117, 1_400, 38_000_000_000),
            groups: 123,
            retained: 2961,
            ..Summary::default()
        };
        let text = brief(&sum, 3900.0, None, None);
        assert!(text.contains("日库 2961 个 → 117 个"), "{text}");
        assert!(text.contains("41.2 GB → 38.0 GB"));
        assert!(text.contains("（123 组）"));
        assert!(text.contains("已停用"));
        assert!(text.contains("耗时 1 小时 5 分"));
        assert!(text.lines().count() <= 6, "{text}");
        assert!(!text.contains("/mnt/"), "{text}");
        let none = brief(&Summary::default(), 10.0, None, None);
        assert!(none.contains("没有待合并"));
    }
}
