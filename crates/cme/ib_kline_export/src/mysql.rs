//! Incremental export from MySQL `ib_data` (read-only).
//!
//! 1. scan `minute_data` rows with `cutoff - overlap < id <= new cutoff` (PK range,
//!    only con_id / utc_time) and collect the touched (con_id, UTC day) pairs;
//! 2. map con_id -> RIC: snapshot plan first, then contracts frozen in state,
//!    else the `delivery()` port + HRD evidence; unmapped contracts are problems;
//! 3. rewrite each touched day from ALL its rows with `id <= new cutoff`
//!    (partial days become complete), same conversion as the snapshot export;
//! 4. advance the NVMe cutoff only if every touched day was published.
use crate::{
    catalog::Catalog,
    ib_minute::{batch, decode, schema, time_ns, Job, SourceRow, SELECT_COLUMNS},
    mapping::{self, Contract},
    plan::{load_json, now_utc, save_json, Budget, Plan, State},
    problems::Problems,
    publish::{day_label, day_path, sampled, DayFile, Pending, Pool, Publisher, DAY_NS},
    snapshot::{absorb, RicReport},
};
use anyhow::{bail, ensure, Context, Result};
use futures_util::TryStreamExt;
use sqlx::{mysql::MySqlConnectOptions, Connection, MySqlConnection, Row};
use std::{
    collections::{BTreeMap, BTreeSet},
    path::{Path, PathBuf},
    sync::{
        atomic::{AtomicUsize, Ordering},
        Mutex,
    },
    time::Instant,
};

/// Bound on distinct touched (con_id, day) pairs held in memory.
const MAX_TOUCHED: usize = 2_000_000;

pub fn options(env: &Path) -> Result<MySqlConnectOptions> {
    let text =
        std::fs::read_to_string(env).with_context(|| format!("env file {}", env.display()))?;
    let mut kv = BTreeMap::new();
    for line in text.lines() {
        let line = line.trim();
        if line.is_empty() || line.starts_with('#') {
            continue;
        }
        let (k, v) = line
            .split_once('=')
            .context("malformed env line (value not shown)")?;
        kv.insert(k.trim().to_string(), v.trim().to_string());
    }
    let get = |k: &str| {
        kv.get(k)
            .cloned()
            .with_context(|| format!("{k} missing in env file"))
    };
    Ok(MySqlConnectOptions::new()
        .host(&get("IB_MYSQL_HOST")?)
        .port(get("IB_MYSQL_PORT")?.parse().context("IB_MYSQL_PORT")?)
        .username(&get("IB_MYSQL_USER")?)
        .password(&get("IB_MYSQL_PASSWORD")?)
        .database(&get("IB_MYSQL_DATABASE")?))
}

pub async fn connect(options: &MySqlConnectOptions) -> Result<MySqlConnection> {
    let mut conn = MySqlConnection::connect_with(options)
        .await
        .context("MySQL connect")?;
    // UTC session: TIMESTAMP created_at is read exactly as in the snapshot export.
    for sql in [
        "SET SESSION time_zone='+00:00'",
        "SET SESSION MAX_EXECUTION_TIME=1800000",
        "SET SESSION TRANSACTION READ ONLY",
    ] {
        sqlx::raw_sql(sql).execute(&mut conn).await?;
    }
    Ok(conn)
}

pub async fn contracts(conn: &mut MySqlConnection) -> Result<BTreeMap<String, Contract>> {
    let rows = sqlx::query(
        "SELECT `con_id`,`symbol`,`exchange`,`multiplier`,`maturity_date`,`instrument_type`,`local_symbol`,`currency`,`company_name` FROM `contract_info` WHERE `instrument_type`='FUT' ORDER BY `id`",
    )
    .fetch_all(&mut *conn)
    .await?;
    let mut out = BTreeMap::new();
    for r in rows {
        let c = Contract {
            con_id: r.try_get("con_id")?,
            symbol: r.try_get("symbol")?,
            exchange: r.try_get("exchange")?,
            multiplier: r.try_get("multiplier")?,
            maturity_date: r.try_get("maturity_date")?,
            instrument_type: r.try_get("instrument_type")?,
            local_symbol: r.try_get("local_symbol")?,
            currency: r.try_get("currency")?,
            company_name: r.try_get("company_name")?,
        };
        out.insert(c.con_id.clone(), c);
    }
    Ok(out)
}

fn utc_text(ns: i64) -> String {
    chrono::DateTime::from_timestamp_nanos(ns)
        .format("%Y-%m-%d %H:%M:%S")
        .to_string()
}

/// All rows of one con_id / UTC day with id <= cutoff, ordered by utc_time.
pub async fn day_rows(
    conn: &mut MySqlConnection,
    con_id: &str,
    day_start: i64,
    cutoff: i64,
) -> Result<Vec<SourceRow>> {
    let sql = format!(
        "SELECT {SELECT_COLUMNS} FROM `minute_data` t FORCE INDEX(uk_conid_utctime) WHERE t.`con_id`=? AND t.`utc_time`>=? AND t.`utc_time`<? AND t.`id`<=? ORDER BY t.`utc_time`"
    );
    let mut stream = sqlx::query(sqlx::AssertSqlSafe(sql.as_str()))
        .bind(con_id)
        .bind(utc_text(day_start))
        .bind(utc_text(day_start + DAY_NS))
        .bind(cutoff)
        .fetch(&mut *conn);
    let mut rows = Vec::new();
    let mut previous = None;
    while let Some(r) = stream.try_next().await? {
        let r = decode(r)?;
        let ts = time_ns(&r.utc_time)
            .with_context(|| format!("invalid utc_time in day query id={}", r.id))?;
        ensure!(
            r.con_id == con_id && ts.div_euclid(DAY_NS) * DAY_NS == day_start,
            "row outside requested con_id/day id={}",
            r.id
        );
        ensure!(
            previous.is_none_or(|p| ts > p),
            "source timestamps not strictly increasing id={}",
            r.id
        );
        ensure!(
            rows.len() < 100_000,
            "implausible minute count for one day con_id={con_id}"
        );
        previous = Some(ts);
        rows.push(r);
    }
    Ok(rows)
}

/// Contract identity for a contract mapped by the port (not in the snapshot plan).
fn new_job(c: &Contract, r: &mapping::Resolved, d: &mapping::Delivery, cutoff: i64) -> Job {
    let mut extra = BTreeMap::new();
    extra.insert("company_name".into(), serde_json::json!(c.company_name));
    extra.insert(
        "instrument_type".into(),
        serde_json::json!(c.instrument_type),
    );
    extra.insert("tas_product".into(), serde_json::json!(d.root));
    extra.insert("delivery_year".into(), serde_json::json!(d.year));
    extra.insert(
        "delivery_month".into(),
        serde_json::json!(mapping::MONTHS.find(d.month).unwrap() + 1),
    );
    extra.insert("tas_aliases".into(), serde_json::json!(r.aliases));
    extra.insert("hrd_evidence".into(), serde_json::json!(r.evidence));
    extra.insert(
        "mapping".into(),
        serde_json::json!("ib_kline_export delivery() port; first mapped at cutoff_id"),
    );
    Job {
        ric: r.ric.clone(),
        con_id: c.con_id.clone(),
        symbol: c.symbol.clone(),
        exchange: c.exchange.clone().unwrap_or_default(),
        maturity_date: c.maturity_date.clone().unwrap_or_default(),
        local_symbol: c.local_symbol.clone().unwrap_or_default(),
        currency: c.currency.clone().unwrap_or_default(),
        multiplier: c.multiplier.clone().unwrap_or_default(),
        ib_first_utc: String::new(),
        ib_last_utc: String::new(),
        cutoff_id: cutoff,
        extra,
    }
}

pub struct Args {
    pub delivery_root: PathBuf,
    pub env: PathBuf,
    pub plan: PathBuf,
    pub hrd: Vec<PathBuf>,
    pub state_dir: PathBuf,
    pub init_from_plan: bool,
    pub max_id: Option<i64>,
    pub overlap_ids: i64,
    pub dry_run: bool,
    pub workers: usize,
}

pub struct Shared {
    pub publisher: std::sync::Arc<Publisher>,
    pub pool: std::sync::Arc<Pool>,
    pub budget: Budget,
}

struct Work {
    job: Job,
    exchange: String,
    days: Vec<i64>,
}

pub fn run(
    a: &Args,
    catalog: &Catalog,
    shared: Option<&Shared>,
    problems: &Problems,
) -> Result<serde_json::Value> {
    let started = Instant::now();
    let state_path = a.state_dir.join("state.json");
    let plan = Plan::load(&a.plan)?;
    let mut state = match load_json::<State>(&state_path)? {
        Some(s) => {
            ensure!(
                !a.init_from_plan,
                "state already exists: {}",
                state_path.display()
            );
            ensure!(
                s.delivery_root == a.delivery_root,
                "state belongs to delivery root {}, not {}",
                s.delivery_root.display(),
                a.delivery_root.display()
            );
            s
        }
        None => {
            ensure!(
                a.init_from_plan,
                "no state at {}; pass --init-from-plan to start at the snapshot cutoff",
                state_path.display()
            );
            State {
                delivery_root: a.delivery_root.clone(),
                cutoff_id: plan.cutoff_id,
                updated_at_utc: now_utc(),
                new_contracts: BTreeMap::new(),
            }
        }
    };
    let plan_map = plan.mapping()?;
    let options = options(&a.env)?;
    let rt = tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()?;
    // Phase 1: touched days.
    let (new_cutoff, touched, invalid, contracts) = rt.block_on(async {
        let mut conn = connect(&options).await?;
        let max: Option<i64> = sqlx::query_scalar("SELECT MAX(`id`) FROM `minute_data`").fetch_one(&mut conn).await?;
        let max = max.context("empty minute_data")?;
        let new_cutoff = a.max_id.map_or(max, |m| m.min(max));
        let from = (state.cutoff_id - a.overlap_ids).max(0);
        let mut touched: BTreeMap<String, BTreeSet<i64>> = BTreeMap::new();
        let mut invalid: BTreeMap<String, (u64, i64, i64)> = BTreeMap::new();
        let mut count = 0usize;
        if new_cutoff > from {
            let mut stream = sqlx::query("SELECT `con_id`, CAST(`utc_time` AS CHAR) AS `utc_time`, `id` FROM `minute_data` WHERE `id`>? AND `id`<=?")
                .bind(from)
                .bind(new_cutoff)
                .fetch(&mut conn);
            while let Some(r) = stream.try_next().await? {
                let con_id: String = r.try_get(0)?;
                let utc: String = r.try_get(1)?;
                let id: i64 = r.try_get(2)?;
                match time_ns(&utc) {
                    Some(ts) => {
                        if touched.entry(con_id).or_default().insert(ts.div_euclid(DAY_NS) * DAY_NS) {
                            count += 1;
                            ensure!(count <= MAX_TOUCHED, "more than {MAX_TOUCHED} touched days; use --max-id to split the run");
                        }
                    }
                    None => {
                        let e = invalid.entry(con_id).or_insert((0, id, id));
                        e.0 += 1;
                        e.1 = e.1.min(id);
                        e.2 = e.2.max(id);
                    }
                }
            }
        }
        let contracts = contracts(&mut conn).await?;
        conn.close().await?;
        Ok::<_, anyhow::Error>((new_cutoff, touched, invalid, contracts))
    })?;
    for (con_id, (n, lo, hi)) in &invalid {
        problems.record(
            "invalid_utc_time",
            "",
            con_id,
            "",
            &format!("{n} rows with unparseable utc_time, id {lo}..={hi}; not exported"),
        );
    }
    // Phase 2: mapping.
    let mut jobs: BTreeMap<String, Job> = BTreeMap::new();
    let mut first_mapped: BTreeSet<String> = BTreeSet::new();
    let mut unmapped = Vec::new();
    let pending_port: Vec<&String> = touched
        .keys()
        .filter(|c| !plan_map.contains_key(*c) && !state.new_contracts.contains_key(*c))
        .collect();
    let mut taken: BTreeSet<String> = plan_map
        .values()
        .chain(state.new_contracts.values())
        .map(|j| j.ric.clone())
        .collect();
    if !pending_port.is_empty() {
        let mut candidates = Vec::new();
        let mut wanted = BTreeSet::new();
        for con_id in &pending_port {
            let Some(c) = contracts.get(*con_id) else {
                unmapped.push((
                    (*con_id).clone(),
                    "con_id_not_in_contract_info_FUT".to_string(),
                    String::new(),
                ));
                continue;
            };
            match mapping::candidate(c, &catalog.observed) {
                Ok(cand) => {
                    wanted.extend(cand.possible.iter().cloned());
                    candidates.push(cand);
                }
                Err(reason) => unmapped.push((c.con_id.clone(), reason, describe(c))),
            }
        }
        let hrd = if candidates.is_empty() {
            Default::default()
        } else {
            mapping::load_hrd(&a.hrd, &wanted)?
        };
        for cand in candidates {
            match mapping::resolve(&cand, &hrd, &catalog.observed) {
                Ok(r) if taken.contains(&r.ric) => unmapped.push((
                    cand.contract.con_id.clone(),
                    format!("ric_already_mapped_to_other_con_id:{}", r.ric),
                    describe(&cand.contract),
                )),
                Ok(r) => {
                    taken.insert(r.ric.clone());
                    let job = new_job(&cand.contract, &r, &cand.delivery, new_cutoff);
                    first_mapped.insert(job.con_id.clone());
                    state.new_contracts.insert(job.con_id.clone(), job);
                }
                Err(reason) => unmapped.push((
                    cand.contract.con_id.clone(),
                    reason,
                    describe(&cand.contract),
                )),
            }
        }
    }
    for (con_id, reason, what) in &unmapped {
        problems.record(
            "unmapped_contract",
            "",
            con_id,
            "",
            &format!(
                "{reason}; {what}; rows since id {} skipped",
                (state.cutoff_id - a.overlap_ids).max(0)
            ),
        );
    }
    for con_id in touched.keys() {
        if let Some(j) = plan_map
            .get(con_id)
            .or_else(|| state.new_contracts.get(con_id))
        {
            jobs.insert(con_id.clone(), j.clone());
        }
    }
    let mut work = Vec::new();
    let mut entries = Vec::new();
    let mut unresolved = 0;
    let rt2 = &rt;
    for (con_id, job) in &jobs {
        match catalog.exchange(job) {
            Ok((exchange, basis)) => {
                let mut days = touched[con_id].clone();
                if first_mapped.contains(con_id) {
                    // First mapping: publish the contract's whole history, not only new ids.
                    days.extend(rt2.block_on(async {
                        let mut conn = connect(&options).await?;
                        let rows = sqlx::query("SELECT DISTINCT CAST(DATE(`utc_time`) AS CHAR) FROM `minute_data` FORCE INDEX(uk_conid_utctime) WHERE `con_id`=? AND `id`<=?")
                            .bind(con_id)
                            .bind(new_cutoff)
                            .fetch_all(&mut conn)
                            .await?;
                        let mut out = Vec::new();
                        for r in rows {
                            let d: String = r.try_get(0)?;
                            if let Some(ts) = time_ns(&format!("{d} 00:00:00")) {
                                out.push(ts);
                            }
                        }
                        Ok::<_, anyhow::Error>(out)
                    })?);
                }
                let days: Vec<i64> = days.into_iter().collect();
                let mut entry = crate::manifest::Entry::new(job, &exchange, &basis);
                let labels: Vec<String> = days.iter().filter_map(|d| day_label(*d).ok()).collect();
                if let (Some(lo), Some(hi)) = (labels.iter().min(), labels.iter().max()) {
                    entry.first_day = lo.clone();
                    entry.last_day = hi.clone();
                }
                entries.push(entry);
                work.push(Work {
                    job: job.clone(),
                    exchange,
                    days,
                });
            }
            Err(reason) => {
                unresolved += 1;
                problems.record("unresolved_exchange", &job.ric, con_id, "", &reason);
            }
        }
    }
    work.sort_by(|a, b| a.job.ric.cmp(&b.job.ric));
    let touched_days: usize = work.iter().map(|w| w.days.len()).sum();
    let mut summary = serde_json::json!({
        "mode": if a.dry_run { "mysql_dry_run" } else { "mysql" },
        "delivery_root": a.delivery_root,
        "previous_cutoff_id": state.cutoff_id,
        "scan_from_id_exclusive": (state.cutoff_id - a.overlap_ids).max(0),
        "new_cutoff_id": new_cutoff,
        "touched_con_ids": touched.len(),
        "mapped_rics": work.len(),
        "newly_mapped_contracts": first_mapped.iter().map(|c| (c.clone(), state.new_contracts[c].ric.clone())).collect::<BTreeMap<_, _>>(),
        "unmapped_contracts": unmapped.len(),
        "unresolved_exchange": unresolved,
        "invalid_utc_con_ids": invalid.len(),
        "touched_day_files": touched_days,
        "touched": work.iter().take(50).map(|w| (w.job.ric.clone(), w.days.iter().map(|d| day_label(*d).unwrap()).collect::<Vec<_>>())).collect::<BTreeMap<_, _>>(),
    });
    if a.dry_run {
        summary["seconds"] = serde_json::json!(started.elapsed().as_secs_f64());
        summary["problems_not_written"] = serde_json::json!(problems.kinds());
        return Ok(summary);
    }
    let shared = shared.context("publication context")?;
    // Phase 3: publish.
    let next = AtomicUsize::new(0);
    let reports = Mutex::new(Vec::<RicReport>::new());
    let failures = Mutex::new(Vec::<String>::new());
    std::thread::scope(|scope| {
        for w in 0..a.workers.min(work.len()).max(1) {
            let (work, next, reports, failures, options) =
                (&work, &next, &reports, &failures, &options);
            std::thread::Builder::new()
                .name(format!("ib-mysql-{w}"))
                .spawn_scoped(scope, move || {
                    let rt = match tokio::runtime::Builder::new_current_thread().enable_all().build() {
                        Ok(rt) => rt,
                        Err(e) => return failures.lock().unwrap().push(format!("runtime: {e}")),
                    };
                    let mut conn = match rt.block_on(connect(options)) {
                        Ok(c) => c,
                        Err(e) => return failures.lock().unwrap().push(format!("{e:#}")),
                    };
                    loop {
                        let i = next.fetch_add(1, Ordering::Relaxed);
                        let Some(item) = work.get(i) else { break };
                        match publish_contract(&rt, &mut conn, shared, problems, item, new_cutoff) {
                            Ok(r) => {
                                if !r.failed_days.is_empty() {
                                    failures.lock().unwrap().push(format!("{}: {} day files failed", r.ric, r.failed_days.len()));
                                }
                                eprintln!(
                                    "ric={} days={} rows={} created={} identical={} replaced={} kept_newer={} failed={} seconds={:.2}",
                                    r.ric, r.days, r.tally.rows, r.tally.created, r.tally.identical, r.tally.replaced, r.tally.kept_newer, r.failed_days.len(), r.seconds
                                );
                                reports.lock().unwrap().push(r);
                            }
                            Err(e) => {
                                let message = format!("{e:#}");
                                if message.contains("resource budget exceeded") {
                                    next.store(usize::MAX / 2, Ordering::Relaxed);
                                }
                                problems.record("ric_failure", &item.job.ric, &item.job.con_id, "", &message);
                                failures.lock().unwrap().push(format!("{}: {message}", item.job.ric));
                            }
                        }
                    }
                    let _ = rt.block_on(conn.close());
                })
                .expect("spawn mysql worker");
        }
    });
    let reports = reports.into_inner().unwrap();
    let failures = failures.into_inner().unwrap();
    let mut tally = crate::publish::Tally::default();
    for r in &reports {
        tally.add(&r.tally);
    }
    problems.flush(&a.delivery_root, &now_utc())?;
    let published: std::collections::HashSet<&str> = reports
        .iter()
        .filter(|r| r.failed_days.is_empty())
        .map(|r| r.ric.as_str())
        .collect();
    crate::manifest::merge(
        &a.state_dir,
        entries
            .into_iter()
            .filter(|e| published.contains(e.ric.as_str())),
    )?;
    summary["tally"] = serde_json::json!(tally);
    summary["replaced"] = serde_json::json!(reports
        .iter()
        .filter(|r| !r.replaced_days.is_empty())
        .map(|r| (r.ric.clone(), r.replaced_days.clone()))
        .collect::<BTreeMap<_, _>>());
    summary["problems"] = serde_json::json!(problems.kinds());
    summary["failures"] = serde_json::json!(failures);
    summary["peak_rss_bytes"] = serde_json::json!(Budget::peak_rss_bytes());
    summary["seconds"] = serde_json::json!(started.elapsed().as_secs_f64());
    if !failures.is_empty() {
        println!("{}", serde_json::to_string_pretty(&summary)?);
        bail!(
            "{} RICs failed; cutoff not advanced (still {})",
            failures.len(),
            state.cutoff_id
        );
    }
    state.cutoff_id = new_cutoff;
    state.updated_at_utc = now_utc();
    save_json(&state_path, &state)?;
    summary["cutoff_saved"] = serde_json::json!(new_cutoff);
    Ok(summary)
}

fn describe(c: &Contract) -> String {
    format!(
        "{} {} {} {} mult={} cur={}",
        c.symbol,
        c.local_symbol.as_deref().unwrap_or(""),
        c.exchange.as_deref().unwrap_or(""),
        c.maturity_date.as_deref().unwrap_or(""),
        c.multiplier.as_deref().unwrap_or(""),
        c.currency.as_deref().unwrap_or("")
    )
}

fn publish_contract(
    rt: &tokio::runtime::Runtime,
    conn: &mut MySqlConnection,
    shared: &Shared,
    problems: &Problems,
    item: &Work,
    cutoff: i64,
) -> Result<RicReport> {
    let started = Instant::now();
    let job = &item.job;
    let s = schema(job);
    let mut report = RicReport {
        ric: job.ric.clone(),
        con_id: job.con_id.clone(),
        exchange: item.exchange.clone(),
        basis: "mysql".into(),
        days: item.days.len() as u64,
        ..Default::default()
    };
    let mut pending = Pending::new(shared.pool.clone(), shared.publisher.clone());
    for (i, &day) in item.days.iter().enumerate() {
        let rows = rt.block_on(day_rows(conn, &job.con_id, day, cutoff))?;
        if rows.is_empty() {
            problems.record(
                "touched_day_without_rows",
                &job.ric,
                &job.con_id,
                &day_label(day)?,
                "no rows with id <= cutoff on requery",
            );
            continue;
        }
        report.rows += rows.len() as u64;
        pending.submit(DayFile {
            path: day_path(
                &shared.publisher.delivery_root,
                &item.exchange,
                &job.ric,
                day,
            )?,
            ric: job.ric.clone(),
            day_start: day,
            batch: batch(&rows, job, s.clone())?,
            max_source_id: cutoff,
            sampled: sampled(item.days.len(), i),
        })?;
    }
    let done = pending.finish()?;
    absorb(&mut report, done, problems, &job.con_id, true);
    report.seconds = started.elapsed().as_secs_f64();
    shared.budget.check()?;
    Ok(report)
}
