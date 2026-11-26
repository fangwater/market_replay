//! Evidence for docs/cme_roll.md: builds the risk-day table, replays the unified rule day by
//! day for the 16 products from PostgreSQL `lseg_open_interest` (only observations published
//! before each decision's cutoff), runs the same rule without the deadline guard as the "OI
//! alone" counterfactual, and compares every roll with the existing preprocess tables.
//! Read-only: PostgreSQL and the old tables are only read; output goes to `--out`.
use anyhow::{Context, Result};
use chrono::{Datelike, NaiveDate};
use clap::Parser;
use cme_roll::{
    calendar::projected_business_days,
    contract::{parse_ric, Month, PRODUCTS},
    risk::{self, RiskTable},
    rule::{cutoff_ns, Calendar, Decision, Engine, Reason, RuleConfig},
};
use futures_util::TryStreamExt;
use serde::Serialize;
use sqlx::{postgres::PgPoolOptions, PgPool, Row};
use std::{
    collections::{BTreeMap, HashMap},
    path::{Path, PathBuf},
};

#[derive(Parser)]
struct Args {
    /// Default: $LSEG_OI_DATABASE_URL.
    #[arg(long)]
    database_url: Option<String>,
    #[arg(
        long,
        default_value = "/mnt/nvme-raid0-28t/fanghaizhou/lseg_data/cme_causal_roll_inputs_20261023/deadline_path_review_v4/deadline_evidence.json"
    )]
    deadline_evidence: PathBuf,
    #[arg(
        long,
        default_value = "/mnt/nvme-raid0-28t/fanghaizhou/lseg_data/cme_causal_roll_inputs_20261023/hrd.csv"
    )]
    hrd: PathBuf,
    /// Existing roll tables (`<exchange>/<product>/dominant.csv`) to compare with.
    #[arg(
        long,
        default_value = "/mnt/hdd-raid5-72t/liang_torch/lseg_data/cme_volume_roll_1d"
    )]
    old_root: PathBuf,
    #[arg(long)]
    out: PathBuf,
    #[arg(long, default_value = "2020-01-02")]
    start: NaiveDate,
    /// Only these products (default: all 16).
    #[arg(long, value_delimiter = ',')]
    products: Vec<String>,
    /// Sensitivity runs only: OI threshold (the rule is 1.1).
    #[arg(long, default_value_t = 1.1)]
    threshold: f64,
    /// Sensitivity runs only: decision minutes before the 17:00 CT open (the rule is 5; a
    /// negative value decides after the open, e.g. -240 = 21:00 CT, after the preliminary OI
    /// of the previous trade date is out — not achievable before the open).
    #[arg(long, default_value_t = 5, allow_hyphen_values = true)]
    cutoff_minutes: i64,
    /// Products whose candidate is only the next listed later month.
    #[arg(long, value_delimiter = ',')]
    next_only: Vec<String>,
    /// Comparison runs only: take the daily signal from this CSV (`ric,trade_date,volume,
    /// published_ns`, e.g. per-contract volume) instead of the OI in PostgreSQL. The trading
    /// calendar still comes from the OI.
    #[arg(long)]
    signal_csv: Option<PathBuf>,
}

#[derive(Serialize)]
struct DailyRow {
    product: String,
    trading_day: NaiveDate,
    ric: String,
    reason: Reason,
    oi_date: Option<NaiveDate>,
    lag: Option<usize>,
    current_oi: Option<i64>,
    best_ric: String,
    best_oi: Option<i64>,
}

#[derive(Serialize, Clone)]
struct RollRow {
    product: String,
    effective_day: NaiveDate,
    from_ric: String,
    to_ric: String,
    reason: Reason,
    oi_date: Option<NaiveDate>,
    from_oi: Option<i64>,
    to_oi: Option<i64>,
    risk_day: Option<NaiveDate>,
    guard_day: Option<NaiveDate>,
    /// Trading days from the effective day to the risk day (>= 1 when the guard holds).
    days_before_risk: Option<i64>,
    /// Day the same contract is left when only the OI rule applies (no guard).
    oi_only_day: Option<NaiveDate>,
    oi_only_reason: Option<Reason>,
    /// Trading days from the guard day to the OI-only day (> 0: OI alone is later than the guard).
    oi_only_after_guard: Option<i64>,
    /// Trading days from the OI-only day to the risk day (<= 0: OI alone holds into the risk day).
    oi_only_days_before_risk: Option<i64>,
    old_day: Option<NaiveDate>,
    old_to_ric: String,
    /// Trading days new - old (< 0: the new rule rolls earlier).
    new_minus_old: Option<i64>,
}

#[derive(Default, Serialize)]
struct Summary {
    trading_days: usize,
    rolls: usize,
    by_reason: BTreeMap<String, usize>,
    blocked_days: usize,
    lag_histogram: BTreeMap<String, usize>,
    oi_only_after_guard: usize,
    oi_only_on_or_after_risk_day: usize,
    oi_only_missing: usize,
    compared_with_old: usize,
    new_earlier: usize,
    same_day: usize,
    new_later: usize,
    different_target: usize,
    old_table: String,
    /// Trading days from the last trading day to the contract's last trade date with OI > 0
    /// (last-trading-day products): -1 = the day before, 0 = the same day, > 0 = during delivery.
    last_oi_vs_last_trading_day: BTreeMap<i64, usize>,
    /// Risk days filled by rule for months without an audited date (checked, mismatches, added).
    rule_filled: Option<(usize, Vec<String>, usize)>,
    unparsed_rics: usize,
    /// Observed calendar vs projected business days in history (rule products).
    projection_vs_observed: Vec<String>,
    /// Observations dated on a holiday copy (not a trade date), skipped.
    holiday_copy_rows: usize,
    /// Contracts with OI > 0 in range but no risk day (never chosen): RIC and max OI.
    contracts_without_risk_day: Vec<String>,
}

fn main() -> Result<()> {
    env_logger::Builder::from_env(env_logger::Env::default().default_filter_or("info")).init();
    let a = Args::parse();
    std::fs::create_dir_all(&a.out)?;
    let months = (Month::new(2019, 6), Month::new(2035, 12));
    let mut table = RiskTable::default();
    let n10 = risk::from_deadline_evidence(&a.deadline_evidence, months, &mut table)?;
    let n5 =
        risk::from_hrd_last_trading(&a.hrd, &["ES", "NQ", "RTY", "YM", "CL"], months, &mut table)?;
    let siblings = risk::fill_from_siblings(&mut table, &["ES", "NQ", "RTY", "YM"])?;
    log::info!("equity months missing in HRD, filled from the other index products: {siblings:?}");
    let ngc = risk::gc_from_hg(&mut table)?;
    log::info!("risk days: {n10} audited, {n5} from HRD, {ngc} GC from HG");
    // Equity index last trading day must be the third Friday.
    for ((p, m), r) in &table.0 {
        if ["ES", "NQ", "RTY", "YM"].contains(&p.as_str()) && r.risk_day != risk::third_friday(*m) {
            log::warn!(
                "{p} {m}: HRD last trading day {} is not the third Friday",
                r.risk_day
            );
        }
    }
    let rt = tokio::runtime::Builder::new_multi_thread()
        .worker_threads(1)
        .enable_all()
        .build()?;
    let url = match &a.database_url {
        Some(u) => u.clone(),
        None => std::env::var("LSEG_OI_DATABASE_URL")
            .context("--database-url or LSEG_OI_DATABASE_URL")?,
    };
    let pool = rt.block_on(PgPoolOptions::new().max_connections(1).connect(&url))?;
    // First intention / position days for months the audited evidence does not cover
    // (Treasuries before 2020-12), from the rule, validated against every audited date.
    let mut filled = BTreeMap::new();
    let mut projection_diffs: BTreeMap<String, Vec<String>> = BTreeMap::new();
    for (product, class) in PRODUCTS {
        let _ = class;
        if risk::rule_for(product).is_none() || product == "GC" {
            continue;
        }
        // Risk days follow business days (US federal holidays and Good Friday), not the days
        // Globex trades: e.g. Columbus Day trades but is not an FX settlement business day.
        let cal = rt.block_on(calendar(&pool, product, Month::new(2019, 6)))?;
        let last = *cal.0.last().context("empty calendar")?;
        let business = projected_business_days(
            NaiveDate::from_ymd_opt(2019, 1, 1).unwrap(),
            NaiveDate::from_ymd_opt(2035, 12, 31).unwrap(),
        );
        let diffs: Vec<String> = business
            .iter()
            .filter(|d| **d >= cal.0[0] && **d <= last && cal.index_of(**d).is_none())
            .map(|d| format!("{d} business day, no trade date"))
            .chain(
                cal.0
                    .iter()
                    .filter(|d| business.binary_search(d).is_err())
                    .map(|d| format!("{d} trade date, not a business day")),
            )
            .collect();
        projection_diffs.insert(product.to_string(), diffs);
        // Treasuries and FX list only the quarterly cycle.
        let quarterly = !matches!(product, "HG" | "SI");
        let all: Vec<Month> = (months.0 .0..=months.1 .0)
            .map(Month)
            .filter(|m| !quarterly || m.month() % 3 == 0)
            .collect();
        let r = risk::fill_by_rule(&mut table, product, &business, &all)?;
        log::info!(
            "{product}: rule reproduces {}/{} audited dates, {} added",
            r.0 - r.1.len(),
            r.0,
            r.2
        );
        for m in &r.1 {
            log::warn!("{m}");
        }
        filled.insert(product.to_string(), r);
    }
    // GC follows HG (COMEX Rule 7), including HG months filled by rule.
    let hg_filled: Vec<_> = table
        .0
        .values()
        .filter(|r| r.product == "HG" && r.source.starts_with("rule:"))
        .cloned()
        .collect();
    for r in hg_filled {
        if table.get("GC", Month::parse(&r.delivery_month)?).is_none() {
            table.insert(risk::RiskDay {
                product: "GC".into(),
                source: format!(
                    "COMEX Rule 7 first position day, same as HG {} ({})",
                    r.delivery_month, r.source
                ),
                ..r
            })?;
        }
    }
    table.write_csv(&a.out.join("risk_days.csv"))?;
    let mut daily = csv::Writer::from_path(a.out.join("daily.csv"))?;
    let mut rolls_out = csv::Writer::from_path(a.out.join("rolls.csv"))?;
    let mut summaries = BTreeMap::new();
    for (product, _) in PRODUCTS {
        if !a.products.is_empty() && !a.products.iter().any(|p| p == product) {
            continue;
        }
        let cfg = RuleConfig {
            threshold: a.threshold,
            cutoff_minutes: a.cutoff_minutes,
            next_only: a.next_only.iter().any(|p| p == product),
            ..RuleConfig::default()
        };
        let (sum, rolls) = rt.block_on(run_product(
            &pool,
            a.signal_csv.as_deref(),
            &cfg,
            product,
            a.start,
            &table,
            &a.old_root,
            &mut daily,
        ))?;
        for r in rolls {
            rolls_out.serialize(r)?;
        }
        let mut sum = sum;
        sum.rule_filled = filled.remove(product);
        sum.projection_vs_observed = projection_diffs.remove(product).unwrap_or_default();
        log::info!("{product}: {} rolls {:?}", sum.rolls, sum.by_reason);
        summaries.insert(product.to_string(), sum);
    }
    daily.flush()?;
    rolls_out.flush()?;
    std::fs::write(
        a.out.join("summary.json"),
        serde_json::to_string_pretty(&summaries)?,
    )?;
    Ok(())
}

struct Change {
    day: NaiveDate,
    from: Month,
    to: Month,
    d: Decision,
}

async fn run_product(
    pool: &PgPool,
    signal_csv: Option<&Path>,
    cfg: &RuleConfig,
    product: &str,
    start: NaiveDate,
    risk: &RiskTable,
    old_root: &Path,
    daily: &mut csv::Writer<std::fs::File>,
) -> Result<(Summary, Vec<RollRow>)> {
    let pattern = format!("^{product}[FGHJKMNQUVXZ][0-9]{{1,2}}$");
    let warm = start - chrono::Duration::days(14);
    let cal = calendar(pool, product, Month::of(warm)).await?;
    let mut sum = Summary::default();
    let mut guarded = Engine::new(product, cfg.clone());
    let mut alone = Engine::new(
        product,
        RuleConfig {
            guard: false,
            ..cfg.clone()
        },
    );
    let mut last_date: HashMap<Month, NaiveDate> = HashMap::new();
    let mut max_oi: BTreeMap<Month, i64> = BTreeMap::new();
    type Obs = (String, NaiveDate, i64, i64);
    let mut rows: std::pin::Pin<Box<dyn futures_util::Stream<Item = Result<Obs>> + Send + '_>> =
        match signal_csv {
            None => Box::pin(
                sqlx::query(
                    "SELECT ric, oi_date, open_interest, first_published_ns FROM lseg_open_interest.observations
                     WHERE ric ~ $1 AND oi_date >= $2 ORDER BY first_published_ns",
                )
                .bind(pattern.clone())
                .bind(warm)
                .fetch(pool)
                .map_err(anyhow::Error::from)
                .and_then(|r| async move {
                    Ok((r.try_get(0)?, r.try_get(1)?, r.try_get(2)?, r.try_get(3)?))
                }),
            ),
            Some(path) => {
                let mut v: Vec<Obs> = Vec::new();
                for rec in csv::Reader::from_path(path)?.deserialize::<Obs>() {
                    let rec = rec?;
                    if rec.0.starts_with(product) && rec.1 >= warm {
                        v.push(rec);
                    }
                }
                v.sort_by_key(|r| r.3);
                Box::pin(futures_util::stream::iter(v.into_iter().map(Ok)))
            }
        };
    let mut pending: Option<(String, NaiveDate, i64, i64)> = None;
    let mut changes_g: Vec<Change> = Vec::new();
    let mut changes_a: Vec<Change> = Vec::new();
    let first = cal.0.partition_point(|d| *d < start);
    for e_idx in first..cal.0.len() {
        let e = cal.0[e_idx];
        let cutoff = cutoff_ns(cfg, e);
        loop {
            if pending.is_none() {
                pending = match rows.try_next().await? {
                    Some(r) => Some(r),
                    None => break,
                };
            }
            let (ric, oi_date, value, published) = pending.as_ref().unwrap();
            if *published > cutoff {
                break;
            }
            let copy = cal.index_of(*oi_date).is_none()
                && *oi_date >= cal.0[0]
                && oi_date <= cal.0.last().unwrap();
            match parse_ric(product, ric, Month::of(*oi_date)) {
                Some(_) if copy => sum.holiday_copy_rows += 1,
                Some(m) => {
                    guarded.observe(m, *oi_date, *value);
                    alone.observe(m, *oi_date, *value);
                    let mx = max_oi.entry(m).or_insert(0);
                    *mx = (*mx).max(*value);
                    if *value > 0 {
                        let l = last_date.entry(m).or_insert(*oi_date);
                        *l = (*l).max(*oi_date);
                    }
                }
                None => sum.unparsed_rics += 1,
            }
            pending = None;
        }
        for (engine, changes, is_guarded) in [
            (&mut guarded, &mut changes_g, true),
            (&mut alone, &mut changes_a, false),
        ] {
            let before = engine.current();
            let d = engine.decide(&cal, e_idx, risk);
            if let (Some(from), Some(to)) = (before, d.contract) {
                if from != to {
                    changes.push(Change {
                        day: e,
                        from,
                        to,
                        d: d.clone(),
                    });
                }
            }
            if is_guarded {
                sum.trading_days += 1;
                if d.reason == Reason::Blocked {
                    sum.blocked_days += 1;
                }
                let lag = d.lag.map_or("none".to_string(), |l| l.to_string());
                *sum.lag_histogram.entry(lag).or_default() += 1;
                daily.serialize(DailyRow {
                    product: product.into(),
                    trading_day: e,
                    ric: d.contract.map(|m| m.ric(product)).unwrap_or_default(),
                    reason: d.reason,
                    oi_date: d.oi_date,
                    lag: d.lag,
                    current_oi: d.current_oi,
                    best_ric: d.best.map(|(m, _)| m.ric(product)).unwrap_or_default(),
                    best_oi: d.best.map(|(_, v)| v),
                })?;
            }
        }
    }
    // Last published trade date vs the last trading day (contracts that expired in range).
    for (m, last) in &last_date {
        if let Some(r) = risk.0.get(&(product.to_string(), *m)) {
            if r.kind == "last_trading_day"
                && r.risk_day >= start
                && r.risk_day < cal.0[cal.0.len() - 1]
            {
                let d = cal.0.partition_point(|x| x < last) as i64
                    - cal.0.partition_point(|x| *x < r.risk_day) as i64;
                *sum.last_oi_vs_last_trading_day.entry(d).or_default() += 1;
            }
        }
    }
    for (m, v) in &max_oi {
        if *v > 0 && risk.get(product, *m).is_none() {
            sum.contracts_without_risk_day
                .push(format!("{}:{v}", m.ric(product)));
        }
    }
    let old = read_old(old_root, product)?;
    sum.old_table = old.0.clone();
    let tdiff = |a: NaiveDate, b: NaiveDate| -> i64 {
        cal.0.partition_point(|d| *d < a) as i64 - cal.0.partition_point(|d| *d < b) as i64
    };
    let mut out = Vec::new();
    for c in &changes_g {
        let risk_day = risk.get(product, c.from);
        let guard_day = guarded.guard_day(&cal, risk, c.from);
        let alone_c = changes_a.iter().find(|x| x.from == c.from);
        let old_c = old.1.iter().find(|(from, _, _)| *from == c.from);
        let row = RollRow {
            product: product.into(),
            effective_day: c.day,
            from_ric: c.from.ric(product),
            to_ric: c.to.ric(product),
            reason: c.d.reason,
            oi_date: c.d.oi_date,
            from_oi: c.d.current_oi,
            to_oi: c.d.best.map(|(_, v)| v),
            risk_day,
            guard_day,
            days_before_risk: risk_day.map(|b| tdiff(b, c.day)),
            oi_only_day: alone_c.map(|x| x.day),
            oi_only_reason: alone_c.map(|x| x.d.reason),
            oi_only_after_guard: alone_c.zip(guard_day).map(|(x, g)| tdiff(x.day, g)),
            oi_only_days_before_risk: alone_c.zip(risk_day).map(|(x, b)| tdiff(b, x.day)),
            old_day: old_c.map(|x| x.2),
            old_to_ric: old_c.map(|x| x.1.ric(product)).unwrap_or_default(),
            new_minus_old: old_c.map(|x| tdiff(c.day, x.2)),
        };
        sum.rolls += 1;
        *sum.by_reason
            .entry(format!("{:?}", row.reason).to_lowercase())
            .or_default() += 1;
        match (row.oi_only_after_guard, row.oi_only_days_before_risk) {
            (Some(g), Some(b)) => {
                if g > 0 {
                    sum.oi_only_after_guard += 1;
                }
                if b <= 0 {
                    sum.oi_only_on_or_after_risk_day += 1;
                }
            }
            _ => sum.oi_only_missing += 1,
        }
        if let Some(x) = row.new_minus_old {
            sum.compared_with_old += 1;
            match x.signum() {
                -1 => sum.new_earlier += 1,
                0 => sum.same_day += 1,
                _ => sum.new_later += 1,
            }
            if row.old_to_ric != row.to_ric {
                sum.different_target += 1;
            }
        }
        out.push(row);
    }
    Ok((sum, out))
}

/// Trading days of a product: the weekday trade dates CME published OI for. Holiday
/// calendars are published in advance, so this is calendar knowledge, not market data.
async fn calendar(pool: &PgPool, product: &str, from: Month) -> Result<Calendar> {
    let pattern = format!("^{product}[FGHJKMNQUVXZ][0-9]{{1,2}}$");
    let from = NaiveDate::from_ymd_opt(from.year(), from.month(), 1).unwrap();
    // A holiday on which LSEG republishes the previous day's figures (e.g. Good Friday
    // 2024-03-29, published on Sunday) is not a trade date: the product's total OI over all
    // outrights is exactly the previous date's.
    let days: Vec<NaiveDate> = sqlx::query_scalar(
        "WITH v AS (
             SELECT ric, oi_date, (array_agg(open_interest ORDER BY first_published_ns DESC))[1] AS oi
             FROM lseg_open_interest.observations
             WHERE ric ~ $1 AND oi_date >= $2 GROUP BY 1, 2),
         t AS (SELECT oi_date, sum(oi) AS total FROM v GROUP BY 1),
         l AS (SELECT oi_date, total, lag(total) OVER (ORDER BY oi_date) AS prev FROM t)
         SELECT oi_date FROM l WHERE prev IS NULL OR total <> prev ORDER BY 1",
    )
    .bind(&pattern)
    .bind(from)
    .fetch_all(pool)
    .await?;
    Ok(Calendar(
        days.into_iter()
            .filter(|d| d.weekday().num_days_from_monday() < 5)
            .collect(),
    ))
}

/// Transitions (from, to, effective day) of the existing `dominant.csv` of a product.
fn read_old(root: &Path, product: &str) -> Result<(String, Vec<(Month, Month, NaiveDate)>)> {
    let mut path = None;
    for ex in ["CME", "CBOT", "COMEX", "NYMEX"] {
        let p = root.join(ex).join(product).join("dominant.csv");
        if p.exists() {
            path = Some(p);
        }
    }
    let Some(path) = path else {
        return Ok(("none".into(), Vec::new()));
    };
    let mut r = csv::Reader::from_path(&path)?;
    let h = r.headers()?.clone();
    let col = |n: &str| h.iter().position(|c| c == n);
    let (day_i, ric_i) = (
        col("trading_day").context("trading_day")?,
        col("ric").context("ric")?,
    );
    let kind = if col("roll_flag").is_some() {
        "legacy volume 2-day"
    } else {
        "causal deadline-first"
    };
    let mut out = Vec::new();
    let mut prev: Option<Month> = None;
    for row in r.records() {
        let row = row?;
        if row[ric_i].is_empty() {
            continue;
        }
        let day = NaiveDate::parse_from_str(&row[day_i], "%Y-%m-%d")?;
        let m = parse_ric(product, &row[ric_i], Month::of(day))
            .with_context(|| format!("{}: RIC {}", path.display(), &row[ric_i]))?;
        if let Some(p) = prev {
            if p != m {
                out.push((p, m, day));
            }
        }
        prev = Some(m);
    }
    Ok((format!("{kind} ({})", path.display()), out))
}
