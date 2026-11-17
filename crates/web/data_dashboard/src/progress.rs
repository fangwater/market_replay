//! Progress panels. Loader functions are blocking (filesystem / subprocess)
//! and run on the bounded blocking pool; parsers are pure and unit-tested.
use crate::config::{CnDaily, IbConfig, MdlReplay};
use crate::util::{dash_day, fmt_time, is_day_name, mtime, read_json};
use chrono::{Datelike, NaiveDate, NaiveTime, Utc, Weekday};
use serde_json::{json, Value};
use std::collections::BTreeMap;
use std::path::Path;

pub const OK: &str = "ok";
pub const LAG: &str = "lag";
pub const FAIL: &str = "fail";
pub const UNKNOWN: &str = "unknown";
pub const ABSENT: &str = "absent";

// ---------------------------------------------------------------- CN cn-daily

/// The latest TradDay that cn-daily should have finished by `now` (Shanghai):
/// weekday `tonglian_msg` directories (Saturday directories hold only Friday
/// night rows), today's only after `ready_after`.
pub fn expected_cn_day(
    msg_days: &[NaiveDate],
    today: NaiveDate,
    now: NaiveTime,
    ready_after: NaiveTime,
) -> Option<NaiveDate> {
    msg_days
        .iter()
        .copied()
        .filter(|d| is_weekday(*d))
        .filter(|d| *d < today || (*d == today && now >= ready_after))
        .max()
}

pub fn is_weekday(d: NaiveDate) -> bool {
    !matches!(d.weekday(), Weekday::Sat | Weekday::Sun)
}

/// Trading days (weekday msg directories) in `(done, expected]`.
pub fn cn_lag(
    msg_days: &[NaiveDate],
    done: Option<NaiveDate>,
    expected: NaiveDate,
) -> Vec<NaiveDate> {
    let mut v: Vec<NaiveDate> = msg_days
        .iter()
        .copied()
        .filter(|d| is_weekday(*d) && *d <= expected && done.is_none_or(|x| *d > x))
        .collect();
    v.sort();
    v
}

/// `cn_daily_state.json` -> last_done + newest-first history (≤ n runs).
pub fn summarize_cn_state(state: &Value, n: usize) -> Value {
    let hist: Vec<Value> = state["history"]
        .as_array()
        .map(|a| {
            a.iter()
                .rev()
                .take(n)
                .map(|h| {
                    let detail = h["detail"].as_str().unwrap_or("");
                    json!({
                        "day": h["day"],
                        "finished_utc": h["finished_utc"],
                        "ok": h["ok"].as_bool().unwrap_or(false),
                        "headline": detail.lines().next().unwrap_or(""),
                        "detail": truncate(detail, 4000),
                    })
                })
                .collect()
        })
        .unwrap_or_default();
    json!({ "last_done": state["last_done"], "history": hist })
}

/// Failed runs not followed by a successful run of the same day.
pub fn open_failures(state: &Value) -> Vec<Value> {
    let Some(a) = state["history"].as_array() else {
        return Vec::new();
    };
    let mut out = Vec::new();
    for (i, h) in a.iter().enumerate() {
        if h["ok"].as_bool() == Some(false) {
            let day = &h["day"];
            let fixed = a[i + 1..]
                .iter()
                .any(|x| &x["day"] == day && x["ok"].as_bool() == Some(true));
            if !fixed {
                out.push(json!({
                    "day": day,
                    "finished_utc": h["finished_utc"],
                    "headline": h["detail"].as_str().unwrap_or("").lines().next().unwrap_or(""),
                }));
            }
        }
    }
    out
}

pub fn summarize_audit(a: &Value) -> Value {
    let checked = a["vwap_checked"].as_f64().unwrap_or(0.0);
    let outside = a["vwap_outside"].as_f64().unwrap_or(0.0);
    json!({
        "day": a["day"],
        "files": a["files"],
        "rows": a["rows"],
        "traded_minutes": a["traded_minutes"],
        "contracts": a["contracts"],
        "per_exchange": a["per_exchange"],
        "vwap_outside_pct": if checked > 0.0 { Some((outside / checked * 10000.0).round() / 100.0) } else { None },
        "no_multiplier": a["no_multiplier"],
        "missing_vs_prev": a["missing_vs_prev"],
        "new_vs_prev": a["new_vs_prev"],
    })
}

pub fn list_msg_days(root: &Path) -> Vec<NaiveDate> {
    let mut out: Vec<NaiveDate> = std::fs::read_dir(root)
        .into_iter()
        .flatten()
        .flatten()
        .filter_map(|e| NaiveDate::parse_from_str(&e.file_name().to_string_lossy(), "%Y%m%d").ok())
        .collect();
    out.sort();
    out
}

/// baseline_data_1min: per exchange / product the day range (one readdir per
/// product directory on the local archive disk).
pub fn scan_baseline(root: &Path) -> BTreeMap<String, BTreeMap<String, (String, String, usize)>> {
    let mut out = BTreeMap::new();
    for ex in crate::progress::CN_EXCHANGES {
        let mut per = BTreeMap::new();
        for e in std::fs::read_dir(root.join(ex))
            .into_iter()
            .flatten()
            .flatten()
        {
            if !e.file_type().map(|t| t.is_dir()).unwrap_or(false) {
                continue;
            }
            let product = e.file_name().to_string_lossy().to_string();
            let mut days: Vec<String> = std::fs::read_dir(e.path())
                .into_iter()
                .flatten()
                .flatten()
                .filter_map(|f| {
                    let n = f.file_name().to_string_lossy().to_string();
                    let d = n.strip_suffix(".parquet")?;
                    is_day_name(d).then(|| d.to_string())
                })
                .collect();
            days.sort();
            if let (Some(a), Some(b)) = (days.first(), days.last()) {
                per.insert(product, (dash_day(a), dash_day(b), days.len()));
            }
        }
        out.insert(ex.to_string(), per);
    }
    out
}

pub const CN_EXCHANGES: [&str; 6] = ["ccfx", "xdce", "xgfe", "xsge", "xsie", "xzce"];

type Baseline = BTreeMap<String, BTreeMap<String, (String, String, usize)>>;

pub fn cn_progress(cfg: &CnDaily, baseline: &Baseline, dominant: Option<&Value>) -> Value {
    let sh = Utc::now().with_timezone(&chrono_tz::Asia::Shanghai);
    let ready = NaiveTime::parse_from_str(&cfg.ready_after, "%H:%M")
        .unwrap_or_else(|_| NaiveTime::from_hms_opt(16, 0, 0).unwrap());
    let mut problems: Vec<String> = Vec::new();
    let state = read_json(&cfg.state_file);
    let (summary, failures, last_done) = match &state {
        Ok(s) => (
            summarize_cn_state(s, 30),
            open_failures(s),
            s["last_done"]
                .as_str()
                .and_then(|d| NaiveDate::parse_from_str(d, "%Y-%m-%d").ok()),
        ),
        Err(e) => {
            problems.push(format!("读取状态文件失败：{e}"));
            (json!({}), Vec::new(), None)
        }
    };
    for f in &failures {
        problems.push(format!(
            "{} 运行失败未重跑成功：{}",
            f["day"].as_str().unwrap_or("?"),
            f["headline"].as_str().unwrap_or("")
        ));
    }
    let msg_days = list_msg_days(&cfg.msg_root);
    let expected = expected_cn_day(&msg_days, sh.date_naive(), sh.time(), ready);
    let lag = expected
        .map(|e| cn_lag(&msg_days, last_done, e))
        .unwrap_or_default();
    if !lag.is_empty() {
        problems.push(format!(
            "落后 {} 个交易日：{}",
            lag.len(),
            lag.iter()
                .map(|d| d.to_string())
                .collect::<Vec<_>>()
                .join("、")
        ));
    }

    // Audits (newest 20) + roll summary for the latest day.
    let mut audits: Vec<(String, std::path::PathBuf)> = Vec::new();
    let mut rolls: Vec<(String, std::path::PathBuf)> = Vec::new();
    for e in std::fs::read_dir(&cfg.log_dir)
        .into_iter()
        .flatten()
        .flatten()
    {
        let n = e.file_name().to_string_lossy().to_string();
        if let Some(d) = n
            .strip_prefix("cn_daily_")
            .and_then(|x| x.strip_suffix("_audit.json"))
        {
            if is_day_name(d) {
                audits.push((d.to_string(), e.path()));
            }
        } else if let Some(d) = n
            .strip_prefix("cn_daily_roll_")
            .and_then(|x| x.strip_suffix(".json"))
        {
            if is_day_name(d) {
                rolls.push((d.to_string(), e.path()));
            }
        }
    }
    audits.sort();
    rolls.sort();
    let audit_series: Vec<Value> = audits
        .iter()
        .rev()
        .take(20)
        .rev()
        .filter_map(|(_, p)| read_json(p).ok().map(|a| summarize_audit(&a)))
        .collect();
    if let Some(a) = audit_series.last() {
        for (k, label) in [
            ("no_multiplier", "缺乘数品种"),
            ("missing_vs_prev", "比前一日缺少的品种"),
        ] {
            if let Some(v) = a[k].as_array().filter(|v| !v.is_empty()) {
                problems.push(format!(
                    "{} {label}：{}",
                    a["day"].as_str().unwrap_or(""),
                    v.iter()
                        .filter_map(|x| x.as_str())
                        .collect::<Vec<_>>()
                        .join("、")
                ));
            }
        }
    }
    let roll = rolls.last().and_then(|(_, p)| read_json(p).ok());

    // Baseline per exchange.
    let mut per_exchange = Vec::new();
    for (ex, products) in baseline {
        let latest = products.values().map(|v| v.1.clone()).max();
        let mut behind = Vec::new();
        let mut stopped = Vec::new();
        if let Some(l) = &latest {
            let l_date = NaiveDate::parse_from_str(l, "%Y-%m-%d").ok();
            for (p, (_, last, _)) in products {
                if last < l {
                    let old = match (l_date, NaiveDate::parse_from_str(last, "%Y-%m-%d").ok()) {
                        (Some(a), Some(b)) => (a - b).num_days() > 30,
                        _ => false,
                    };
                    let item = json!({"product": p, "last": last});
                    if old {
                        stopped.push(item)
                    } else {
                        behind.push(item)
                    }
                }
            }
        }
        if let (Some(l), Some(d)) = (&latest, last_done) {
            if l.as_str() < d.to_string().as_str() {
                problems.push(format!("{ex} 生产 1 分钟表最新 {l}，落后于 last_done {d}"));
            }
        }
        for b in &behind {
            problems.push(format!(
                "{ex}/{} 最新文件 {}，未到本所最新日",
                b["product"].as_str().unwrap_or(""),
                b["last"].as_str().unwrap_or("")
            ));
        }
        per_exchange.push(json!({
            "exchange": ex,
            "exchange_cn": crate::names::cn_exchange_name(ex),
            "products": products.len(),
            "latest": latest,
            "behind": behind,
            "stopped": stopped,
        }));
    }

    // Dominant (主力) from the cn-kline-server HTTP API.
    let dominant_summary = match dominant {
        Some(d) => {
            let latest = d["latest_trading_day"].as_str().unwrap_or("").to_string();
            let mut rolled_today = Vec::new();
            let mut rolling_next = Vec::new();
            for p in d["products"].as_array().into_iter().flatten() {
                if p["last_roll_day"].as_str() == Some(latest.as_str()) {
                    rolled_today.push(format!(
                        "{} {}",
                        p["product"].as_str().unwrap_or(""),
                        p["dominant"].as_str().unwrap_or("")
                    ));
                }
                if p["rolls_next_day"].as_bool() == Some(true) {
                    rolling_next.push(format!(
                        "{} {}→{}",
                        p["product"].as_str().unwrap_or(""),
                        p["dominant"].as_str().unwrap_or(""),
                        p["scheduled_next"].as_str().unwrap_or("")
                    ));
                }
            }
            if let Some(d) = last_done {
                if latest < d.to_string() {
                    problems.push(format!("主力结果最新交易日 {latest}，落后于 last_done {d}"));
                }
            }
            json!({"available": true, "latest_trading_day": latest, "count": d["count"],
                   "rolled_today": rolled_today, "rolling_next": rolling_next})
        }
        None => {
            problems.push("主力接口 18313 不可用".into());
            json!({"available": false})
        }
    };

    let last_ok = summary["history"]
        .as_array()
        .and_then(|h| h.first())
        .map(|h| h["ok"].as_bool().unwrap_or(false));
    let status = if state.is_err() {
        UNKNOWN
    } else if last_ok == Some(false) || !failures.is_empty() {
        FAIL
    } else if !lag.is_empty() || !problems.is_empty() {
        LAG
    } else {
        OK
    };
    json!({
        "status": status,
        "last_done": summary["last_done"],
        "expected_day": expected.map(|d| d.to_string()),
        "lag_days": lag.iter().map(|d| d.to_string()).collect::<Vec<_>>(),
        "history": summary["history"],
        "audits": audit_series,
        "roll": roll,
        "baseline": per_exchange,
        "baseline_root": cfg.baseline_root,
        "session_as_of": cfg.session_as_of,
        "dominant": dominant_summary,
        "problems": problems,
    })
}

// ---------------------------------------------------------------- IB export

/// Summary JSON of one `ib_kline_export mysql` run log (the object between
/// the first line starting with `{` and the last `}`), as
/// `scripts/run_ib_kline_daily.sh` reads it.
pub fn parse_ib_log(text: &str) -> Option<Value> {
    let start = if text.starts_with('{') {
        0
    } else {
        text.find("\n{")? + 1
    };
    let end = text.rfind('}')? + 1;
    if end <= start {
        return None;
    }
    let v: Value = serde_json::from_str(&text[start..end]).ok()?;
    let tally = &v["tally"];
    Some(json!({
        "mode": v["mode"],
        "new_cutoff_id": v["new_cutoff_id"],
        "previous_cutoff_id": v["previous_cutoff_id"],
        "mapped_rics": v["mapped_rics"],
        "created": tally["created"],
        "replaced": tally["replaced"],
        "identical": tally["identical"],
        "rows": tally["rows"],
        "unmapped_contracts": v["unmapped_contracts"],
        "failures": v["failures"].as_array().map(|a| a.len()).unwrap_or(0),
        "failure_samples": v["failures"].as_array().map(|a| a.iter().take(5).cloned().collect::<Vec<_>>()).unwrap_or_default(),
        "seconds": v["seconds"],
        "catalog_synced": text[end..].contains("IB contracts"),
    }))
}

/// `kind` column counts of a problems CSV.
pub fn count_problem_kinds(text: &str) -> BTreeMap<String, usize> {
    let mut out = BTreeMap::new();
    let mut rdr = csv::ReaderBuilder::new()
        .flexible(true)
        .from_reader(text.as_bytes());
    let idx = rdr
        .headers()
        .ok()
        .and_then(|h| h.iter().position(|x| x == "kind"))
        .unwrap_or(0);
    for r in rdr.records().flatten() {
        *out.entry(r.get(idx).unwrap_or("").to_string()).or_default() += 1;
    }
    out
}

pub fn ib_progress(cfg: &IbConfig, catalog: Option<&crate::catalog::Catalog>) -> Value {
    let mut problems = Vec::new();
    let state = read_json(&cfg.state);
    let state_v = match &state {
        Ok(s) => json!({
            "cutoff_id": s["cutoff_id"],
            "updated_at_utc": s["updated_at_utc"],
            "new_contracts": s["new_contracts"].as_object().map(|o| o.len()).unwrap_or(0),
        }),
        Err(e) => {
            problems.push(format!("读取 state.json 失败：{e}"));
            Value::Null
        }
    };
    let mut logs: Vec<(String, std::path::PathBuf)> = std::fs::read_dir(&cfg.log_dir)
        .into_iter()
        .flatten()
        .flatten()
        .filter_map(|e| {
            let n = e.file_name().to_string_lossy().to_string();
            n.ends_with(".log").then(|| (n, e.path()))
        })
        .collect();
    logs.sort();
    let runs: Vec<Value> = logs
        .iter()
        .rev()
        .take(14)
        .map(|(n, p)| {
            let text = std::fs::read_to_string(p).unwrap_or_default();
            let s = parse_ib_log(&text);
            let ok = s
                .as_ref()
                .map(|s| s["failures"] == 0 && s["catalog_synced"] == true)
                .unwrap_or(false);
            json!({"log": n, "started_utc": log_time(n), "ok": ok, "summary": s,
                   "tail": if ok { Value::Null } else { json!(tail(&text, 800)) }})
        })
        .collect();
    if let Some(r) = runs.first() {
        if r["ok"] != true {
            problems.push(format!(
                "最近一次运行 {} 未成功",
                r["log"].as_str().unwrap_or("")
            ));
        }
    } else {
        problems.push("没有运行日志".into());
    }
    let problem_kinds = std::fs::read_to_string(&cfg.problems_csv)
        .map(|t| count_problem_kinds(&t))
        .unwrap_or_default();
    let cat = catalog.map(|c| {
        let only = c.ib.iter().filter(|k| !k.tas_data).count();
        let last = c.ib.iter().filter_map(|k| k.last_day.clone()).max();
        let mut syms: BTreeMap<&str, (usize, usize)> = BTreeMap::new();
        for k in &c.ib {
            let e = syms.entry(&k.ib_symbol).or_default();
            e.0 += 1;
            if !k.tas_data {
                e.1 += 1;
            }
        }
        json!({"contracts": c.ib.len(), "ib_only": only, "symbols": syms.len(),
               "latest_day": last.map(|d| dash_day(&d))})
    });
    let age_h = state
        .as_ref()
        .ok()
        .and_then(|s| s["updated_at_utc"].as_str())
        .and_then(|t| chrono::DateTime::parse_from_rfc3339(t).ok())
        .map(|t| (Utc::now() - t.with_timezone(&Utc)).num_minutes() as f64 / 60.0);
    if age_h.is_some_and(|h| h > 26.0) {
        problems.push(format!(
            "cutoff 已 {:.0} 小时未更新（每日 01:30 UTC 运行）",
            age_h.unwrap_or(0.0)
        ));
    }
    let status = if state.is_err() {
        UNKNOWN
    } else if runs.first().is_some_and(|r| r["ok"] != true) {
        FAIL
    } else if !problems.is_empty() {
        LAG
    } else {
        OK
    };
    json!({
        "status": status,
        "state": state_v,
        "state_age_hours": age_h.map(|h| (h * 10.0).round() / 10.0),
        "contracts_json_mtime": mtime(&cfg.contracts_json).map(fmt_time),
        "runs": runs,
        "catalog": cat,
        "problem_kinds": problem_kinds,
        "problems": problems,
    })
}

fn log_time(name: &str) -> String {
    // 20261116T102101Z.log
    let s = name.trim_end_matches(".log");
    if s.len() == 16 {
        format!(
            "{}-{}-{}T{}:{}:{}Z",
            &s[..4],
            &s[4..6],
            &s[6..8],
            &s[9..11],
            &s[11..13],
            &s[13..15]
        )
    } else {
        s.to_string()
    }
}

// ---------------------------------------------------------------- LSEG

/// lseg-daily driver state (crates/cme/lseg_daily `State`), read generically.
pub fn summarize_lseg_daily(s: &Value) -> Value {
    let mut latest: BTreeMap<String, String> = BTreeMap::new();
    let mut days_total = 0usize;
    if let Some(o) = s["delivered"].as_object() {
        for (product, dates) in o {
            if let Some(a) = dates.as_array() {
                days_total += a.len();
                if let Some(m) = a.iter().filter_map(|d| d.as_str()).max() {
                    latest.insert(product.clone(), m.to_string());
                }
            }
        }
    }
    let overall = latest.values().max().cloned();
    let gaps: Vec<Value> = s["gaps"]
        .as_object()
        .map(|o| {
            o.iter()
                .map(|(k, v)| json!({"key": k, "reason": v}))
                .collect()
        })
        .unwrap_or_default();
    let mut open_batches = Vec::new();
    for b in s["batches"].as_array().into_iter().flatten() {
        let mut stages: BTreeMap<String, usize> = BTreeMap::new();
        let mut errors = Vec::new();
        let mut all_done = true;
        for (p, pr) in b["products"].as_object().into_iter().flatten() {
            let stage = pr["stage"].as_str().unwrap_or("?").to_string();
            let done = stage == "Done" && pr["kline_done"].as_bool().unwrap_or(false);
            all_done &= done;
            *stages.entry(stage).or_default() += 1;
            if let Some(e) = pr["error"].as_str() {
                errors.push(json!({"product": p, "error": truncate(e, 400)}));
            }
        }
        if !all_done {
            open_batches.push(json!({"id": b["id"], "created_utc": b["created_utc"],
                                     "stages": stages, "errors": errors}));
        }
    }
    let history: Vec<Value> = s["history"]
        .as_array()
        .map(|a| {
            a.iter()
                .rev()
                .take(20)
                .map(|h| {
                    let d = h["detail"].as_str().unwrap_or("");
                    json!({"run": h["run"], "finished_utc": h["finished_utc"],
                           "ok": h["ok"].as_bool().unwrap_or(false),
                           "headline": d.lines().next().unwrap_or(""), "detail": truncate(d, 3000)})
                })
                .collect()
        })
        .unwrap_or_default();
    json!({
        "present": true,
        "bootstrapped": s["bootstrapped"],
        "products": latest.len(),
        "delivered_days": days_total,
        "latest_day": overall.map(|d| dash_day(&d)),
        "catalog_dirty": s["catalog_dirty"],
        "gaps": gaps.len(),
        "gap_samples": gaps.into_iter().rev().take(30).collect::<Vec<_>>(),
        "open_batches": open_batches,
        "history": history,
    })
}

pub fn list_problems_dir(dir: &Path) -> Vec<Value> {
    let mut out: Vec<Value> = std::fs::read_dir(dir)
        .into_iter()
        .flatten()
        .flatten()
        .map(|e| {
            let name = e.file_name().to_string_lossy().to_string();
            let md = e.metadata().ok();
            let is_dir = md.as_ref().is_some_and(|m| m.is_dir());
            let entries = if is_dir {
                std::fs::read_dir(e.path()).map(|r| r.count()).ok()
            } else {
                None
            };
            json!({
                "name": name,
                "dir": is_dir,
                "bytes": if is_dir { None } else { md.as_ref().map(|m| m.len()) },
                "entries": entries,
                "mtime": md.and_then(|m| m.modified().ok()).map(fmt_time),
            })
        })
        .collect();
    out.sort_by(|a, b| b["mtime"].as_str().cmp(&a["mtime"].as_str()));
    out
}

pub fn lseg_catalog_summary(c: &crate::catalog::Catalog) -> Value {
    #[derive(Default)]
    struct Ex {
        products: std::collections::BTreeSet<String>,
        active: usize,
        latest: Option<String>,
    }
    let mut ex: BTreeMap<String, Ex> = BTreeMap::new();
    let mut product_latest: BTreeMap<&str, &str> = BTreeMap::new();
    let mut active = 0;
    for k in &c.contracts {
        let e = ex.entry(k.exchange.clone()).or_default();
        e.products.insert(k.product.clone());
        if k.active {
            e.active += 1;
            active += 1;
        }
        if let Some(d) = &k.last_market_date {
            if e.latest.as_deref() < Some(d.as_str()) {
                e.latest = Some(d.clone());
            }
            let p = product_latest.entry(&k.product).or_insert("");
            if *p < d.as_str() {
                *p = d;
            }
        }
    }
    let overall = product_latest.values().max().map(|s| s.to_string());
    // Products by their latest data day (last 14 distinct days) for the chart.
    let mut by_day: BTreeMap<&str, usize> = BTreeMap::new();
    for d in product_latest.values() {
        *by_day.entry(d).or_default() += 1;
    }
    let histogram: Vec<Value> = by_day
        .iter()
        .rev()
        .take(14)
        .rev()
        .map(|(d, n)| json!({"day": d, "products": n}))
        .collect();
    json!({
        "contracts": c.contracts.len(),
        "products": c.by_product.len(),
        "active_contracts": active,
        "latest_day": overall,
        "loaded_at": c.loaded_at,
        "mtime": c.mtime.map(fmt_time),
        "per_exchange": ex.into_iter().map(|(k, v)| json!({
            "exchange": k, "products": v.products.len(), "active": v.active, "latest": v.latest,
        })).collect::<Vec<_>>(),
        "latest_histogram": histogram,
    })
}

/// Newest month directory (`YYYYMM`) of the size-threshold store (listing only).
pub fn size_threshold_months(dir: &Path) -> Value {
    let latest = std::fs::read_dir(dir)
        .into_iter()
        .flatten()
        .flatten()
        .filter(|e| e.file_type().map(|t| t.is_dir()).unwrap_or(false))
        .filter_map(|e| e.file_name().to_str().map(str::to_string))
        .filter(|n| n.len() == 6 && n.bytes().all(|b| b.is_ascii_digit()))
        .max();
    json!({"latest_month": latest})
}

// ---------------------------------------------------------------- services

pub fn mdl_cache(cfg: &MdlReplay) -> Value {
    let root = cfg.cache_root.join("v1");
    let mut days = Vec::new();
    let mut total = 0u64;
    for e in std::fs::read_dir(&root).into_iter().flatten().flatten() {
        let name = e.file_name().to_string_lossy().to_string();
        if !is_day_name(&name) {
            continue;
        }
        let mut bytes = 0u64;
        let mut files = 0usize;
        let mut channels = 0usize;
        let mut newest = None;
        for ch in std::fs::read_dir(e.path()).into_iter().flatten().flatten() {
            if ch.file_type().map(|t| t.is_dir()).unwrap_or(false) {
                channels += 1;
                for f in std::fs::read_dir(ch.path()).into_iter().flatten().flatten() {
                    if let Ok(m) = f.metadata() {
                        bytes += m.len();
                        files += 1;
                        let t = m.modified().ok();
                        if t > newest {
                            newest = t;
                        }
                    }
                }
            } else if let Ok(m) = ch.metadata() {
                bytes += m.len();
            }
        }
        total += bytes;
        days.push(
            json!({"day": dash_day(&name), "bytes": bytes, "channels": channels,
                         "files": files, "mtime": newest.map(fmt_time)}),
        );
    }
    days.sort_by(|a, b| a["day"].as_str().cmp(&b["day"].as_str()));
    let limit = cfg.cache_max_gb << 30;
    json!({
        "status": if limit > 0 && total > limit { LAG } else { OK },
        "cache_dir": root,
        "total_bytes": total,
        "limit_bytes": limit,
        "days": days,
    })
}

fn truncate(s: &str, n: usize) -> String {
    if s.len() <= n {
        return s.to_string();
    }
    let mut end = n;
    while !s.is_char_boundary(end) {
        end -= 1;
    }
    format!("{}…", &s[..end])
}

fn tail(s: &str, n: usize) -> String {
    if s.len() <= n {
        return s.to_string();
    }
    let mut start = s.len() - n;
    while !s.is_char_boundary(start) {
        start += 1;
    }
    s[start..].to_string()
}

#[cfg(test)]
mod tests {
    use super::*;

    fn d(s: &str) -> NaiveDate {
        NaiveDate::parse_from_str(s, "%Y-%m-%d").unwrap()
    }
    fn t(s: &str) -> NaiveTime {
        NaiveTime::parse_from_str(s, "%H:%M").unwrap()
    }

    #[test]
    fn expected_day_respects_ready_time_and_saturdays() {
        // Fri 11-13, Sat 11-14 (night rows only), Mon 11-16
        let days = vec![
            d("2026-11-12"),
            d("2026-11-13"),
            d("2026-11-14"),
            d("2026-11-16"),
        ];
        assert_eq!(
            expected_cn_day(&days, d("2026-11-16"), t("15:59"), t("16:00")),
            Some(d("2026-11-13"))
        );
        assert_eq!(
            expected_cn_day(&days, d("2026-11-16"), t("16:00"), t("16:00")),
            Some(d("2026-11-16"))
        );
        assert_eq!(
            expected_cn_day(&days, d("2026-11-15"), t("10:00"), t("16:00")),
            Some(d("2026-11-13"))
        );
        let lag = cn_lag(&days, Some(d("2026-11-12")), d("2026-11-16"));
        assert_eq!(lag, vec![d("2026-11-13"), d("2026-11-16")]);
        assert!(cn_lag(&days, Some(d("2026-11-16")), d("2026-11-16")).is_empty());
    }

    #[test]
    fn cn_state_failures_and_history() {
        let s = json!({"last_done": "2026-08-28", "history": [
            {"day": "2026-08-27", "finished_utc": "a", "ok": false, "detail": "2026-08-27 导出 失败\nmore"},
            {"day": "2026-08-27", "finished_utc": "b", "ok": true, "detail": "TradDay 2026-08-27：79 个品种文件"},
            {"day": "2026-08-28", "finished_utc": "c", "ok": false, "detail": "2026-08-28 回放 失败"},
        ]});
        let f = open_failures(&s);
        assert_eq!(f.len(), 1);
        assert_eq!(f[0]["day"], "2026-08-28");
        let sum = summarize_cn_state(&s, 2);
        assert_eq!(sum["last_done"], "2026-08-28");
        let h = sum["history"].as_array().unwrap();
        assert_eq!(h.len(), 2);
        assert_eq!(h[0]["finished_utc"], "c");
        assert_eq!(h[1]["headline"], "TradDay 2026-08-27：79 个品种文件");
    }

    #[test]
    fn audit_summary_ratio() {
        let a = json!({"day": "2026-11-16", "rows": 271669, "vwap_checked": 133165, "vwap_outside": 1401,
                       "no_multiplier": [], "missing_vs_prev": ["SO"]});
        let s = summarize_audit(&a);
        assert_eq!(s["vwap_outside_pct"], 1.05);
        assert_eq!(s["missing_vs_prev"][0], "SO");
    }

    #[test]
    fn ib_log_summary() {
        let text = "ric=ESZ26 days=1 rows=582\n{\n  \"mode\": \"mysql\",\n  \"failures\": [],\n  \"new_cutoff_id\": 5,\n  \"tally\": {\"created\": 1, \"rows\": 10},\n  \"touched\": {\"ESZ26\": [\"20261116\"]}\n}\n/mnt/x/catalog.sqlite: 1083 IB contracts (46 IB-only)\n";
        let s = parse_ib_log(text).unwrap();
        assert_eq!(s["new_cutoff_id"], 5);
        assert_eq!(s["created"], 1);
        assert_eq!(s["failures"], 0);
        assert_eq!(s["catalog_synced"], true);
        let no_sync = "x\n{\"mode\":\"mysql\",\"failures\":[{\"ric\":\"A\"}],\"tally\":{}}\n";
        let s = parse_ib_log(no_sync).unwrap();
        assert_eq!(s["failures"], 1);
        assert_eq!(s["catalog_synced"], false);
        assert!(parse_ib_log("Error: mysql down\n").is_none());
    }

    #[test]
    fn problem_kinds() {
        let k = count_problem_kinds("kind,ric\nunmapped_contract,\nunmapped_contract,\nother,X\n");
        assert_eq!(k["unmapped_contract"], 2);
        assert_eq!(k["other"], 1);
    }

    #[test]
    fn lseg_daily_state() {
        let s = json!({
            "bootstrapped": true,
            "delivered": {"ES": ["20261106", "20261109"], "CL": ["20261105"]},
            "gaps": {"CL 20261106": "zero-byte placeholder"},
            "batches": [
                {"id": "b1", "created_utc": "x", "products": {
                    "ES": {"stage": "Done", "kline_done": true},
                    "CL": {"stage": "Replayed", "kline_done": false, "error": "audit not passed"}}},
                {"id": "b0", "created_utc": "y", "products": {"ES": {"stage": "Done", "kline_done": true}}}
            ],
            "history": [{"run": "r1", "finished_utc": "z", "ok": false, "detail": "失败\n详情"}]
        });
        let v = summarize_lseg_daily(&s);
        assert_eq!(v["latest_day"], "2026-11-09");
        assert_eq!(v["delivered_days"], 3);
        assert_eq!(v["gaps"], 1);
        assert_eq!(v["open_batches"].as_array().unwrap().len(), 1);
        assert_eq!(v["open_batches"][0]["errors"][0]["product"], "CL");
        assert_eq!(v["history"][0]["headline"], "失败");
        // empty / default state is tolerated
        let v = summarize_lseg_daily(&json!({}));
        assert_eq!(v["products"], 0);
    }
    #[test]
    fn size_threshold_months_picks_the_newest_month_dir() {
        let t = tempfile::tempdir().unwrap();
        for m in ["202609", "202610", "notes"] {
            std::fs::create_dir_all(t.path().join(m)).unwrap();
        }
        std::fs::write(t.path().join("202611"), "a file, not a month dir").unwrap();
        assert_eq!(size_threshold_months(t.path())["latest_month"], "202610");
        assert_eq!(
            size_threshold_months(&t.path().join("nope"))["latest_month"],
            Value::Null
        );
    }
}
