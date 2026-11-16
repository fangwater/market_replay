//! Section refreshers and the product index builder.
use crate::catalog::Catalog;
use crate::names::{self, ChineseName, IbMatch};
use crate::progress;
use crate::search::{Index, Market, Product};
use crate::util::{dash_day, mtime};
use crate::AppState;
use serde_json::{json, Value};
use std::collections::{BTreeMap, BTreeSet, HashMap};
use std::sync::Arc;
use std::time::{Duration, Instant};

pub type Baseline = BTreeMap<String, BTreeMap<String, (String, String, usize)>>;

pub struct NameSources {
    pub products: BTreeMap<String, ChineseName>,
    pub ib_exchanges: BTreeMap<String, String>,
    pub exchange_summary: HashMap<String, String>,
    pub ib_matches: Vec<IbMatch>,
    pub ib_snapshot: HashMap<String, (String, String, String)>,
    pub lseg_desc: HashMap<String, String>,
    /// `config/product_names.toml`, priority over everything above.
    pub overrides: names::ProductNames,
    pub overrides_error: Option<String>,
}

fn load_overrides(n: &crate::config::NamesConfig) -> (names::ProductNames, Option<String>) {
    match &n.product_names_toml {
        None => (names::ProductNames::default(), None),
        Some(p) => match names::ProductNames::load(p) {
            Ok(t) => (t, None),
            Err(e) => (
                names::ProductNames::default(),
                Some(format!("品种中文名表载入失败，使用内置名称：{e:#}")),
            ),
        },
    }
}

/// Returns true when the product index should be rebuilt.
pub async fn refresh(st: &Arc<AppState>, name: &'static str) -> bool {
    let t = Instant::now();
    match name {
        "dominant" => {
            let url = format!(
                "{}/api/v1/dominant",
                st.cfg.cn.dominant_api.trim_end_matches('/')
            );
            let v = st.get_json(&url).await.ok().map(Arc::new);
            let changed = {
                let cur = st.dominant.read().await;
                cur.as_ref().map(|c| &c["latest_trading_day"])
                    != v.as_ref().map(|c| &c["latest_trading_day"])
            };
            *st.dominant.write().await = v;
            changed
        }
        "baseline" => {
            let Ok(cn) = st.cn_daily.clone() else {
                return false;
            };
            if let Ok(b) = st
                .blocking(move || progress::scan_baseline(&cn.baseline_root))
                .await
            {
                let changed = st.baseline.read().await.as_deref() != Some(&b);
                *st.baseline.write().await = Some(Arc::new(b));
                return changed;
            }
            false
        }
        "catalog" => {
            let path = st.cfg.lseg.catalog.clone();
            let p2 = path.clone();
            let m = st.blocking(move || mtime(&p2)).await.ok().flatten();
            let cur = st.catalog.read().await.as_ref().map(|c| c.mtime);
            if m.is_some() && cur != Some(m) {
                match Catalog::load(&path, m).await {
                    Ok(c) => {
                        *st.catalog.write().await = Some(Arc::new(c));
                        return true;
                    }
                    Err(e) => st.set("catalog", t, Err(format!("{e:#}"))).await,
                }
            }
            false
        }
        "cn" => {
            let r = match st.cn_daily.clone() {
                Ok(cn) => {
                    let baseline = st.baseline.read().await.clone().unwrap_or_default();
                    let dominant = st.dominant.read().await.clone();
                    st.blocking(move || progress::cn_progress(&cn, &baseline, dominant.as_deref()))
                        .await
                }
                Err(e) => Err(format!("读取 cn_daily 配置失败：{e}")),
            };
            st.set(name, t, r).await;
            false
        }
        "lseg" => {
            let cat = st.catalog.read().await.clone();
            let cfg = st.cfg.lseg.clone();
            let r = st
                .blocking(move || {
                    let mut problems = Vec::new();
                    let catalog = cat.as_deref().map(progress::lseg_catalog_summary);
                    if catalog.is_none() {
                        problems.push("catalog.sqlite 尚未载入".to_string());
                    }
                    let daily = match std::fs::read(&cfg.lseg_daily_state) {
                        Ok(b) => match serde_json::from_slice::<Value>(&b) {
                            Ok(v) => progress::summarize_lseg_daily(&v),
                            Err(e) => json!({"present": true, "error": e.to_string()}),
                        },
                        Err(_) => json!({"present": false, "path": cfg.lseg_daily_state}),
                    };
                    let latest = catalog
                        .as_ref()
                        .and_then(|c| c["latest_day"].as_str().map(str::to_string));
                    let lag = latest
                        .as_deref()
                        .and_then(|d| chrono::NaiveDate::parse_from_str(d, "%Y-%m-%d").ok())
                        .map(|d| (chrono::Utc::now().date_naive() - d).num_days());
                    if let Some(l) = lag.filter(|l| *l > 3) {
                        problems.push(format!("交付数据最新 UTC 日距今 {l} 天"));
                    }
                    if daily["present"] == true {
                        for b in daily["open_batches"].as_array().into_iter().flatten() {
                            for e in b["errors"].as_array().into_iter().flatten() {
                                problems.push(format!(
                                    "批次 {} {}：{}",
                                    b["id"].as_str().unwrap_or(""),
                                    e["product"].as_str().unwrap_or(""),
                                    e["error"].as_str().unwrap_or("")
                                ));
                            }
                        }
                        if daily["history"][0]["ok"] == false {
                            problems.push(format!(
                                "lseg-daily 最近一次运行失败：{}",
                                daily["history"][0]["headline"].as_str().unwrap_or("")
                            ));
                        }
                    }
                    let status = if catalog.is_none() {
                        progress::UNKNOWN
                    } else if daily["history"][0]["ok"] == false {
                        progress::FAIL
                    } else if !problems.is_empty() {
                        progress::LAG
                    } else {
                        progress::OK
                    };
                    json!({
                        "status": status,
                        "catalog_path": cfg.catalog,
                        "catalog": catalog,
                        "lag_days": lag,
                        "lseg_daily": daily,
                        "problems_dir": progress::list_problems_dir(&cfg.problems_dir),
                        "problems": problems,
                    })
                })
                .await;
            st.set(name, t, r).await;
            false
        }
        "ib" => {
            let cat = st.catalog.read().await.clone();
            let cfg = st.cfg.ib.clone();
            let r = st
                .blocking(move || progress::ib_progress(&cfg, cat.as_deref()))
                .await;
            st.set(name, t, r).await;
            false
        }
        "mdl_cache" => {
            let r = match st.mdl.clone() {
                Ok(m) => st.blocking(move || progress::mdl_cache(&m)).await,
                Err(e) => Err(e),
            };
            st.set(name, t, r).await;
            false
        }
        "reference" => {
            let url = format!(
                "{}/api/v1/data-state",
                st.cfg.services.reference_api.trim_end_matches('/')
            );
            let r = st.get_json(&url).await.map(|v| {
                let today = chrono::Utc::now()
                    .with_timezone(&chrono_tz::Asia::Shanghai)
                    .date_naive()
                    .to_string();
                let as_of = v["as_of"].as_str().unwrap_or("").to_string();
                json!({
                    "status": if as_of.as_str() >= today.as_str() { progress::OK } else { progress::LAG },
                    "as_of": as_of,
                    "latest_update_ts": v["latest_update_ts"],
                    "instrument_count": v["instrument_count"],
                    "version_count": v["version_count"],
                    "today_shanghai": today,
                })
            });
            st.set(name, t, r).await;
            false
        }
        _ => false,
    }
}

/// One read-only psql query (default_transaction_read_only=on); rows split
/// on the unit separator.
async fn psql(st: &AppState, sql: &str) -> Result<Vec<Vec<String>>, String> {
    let c = &st.cfg.cn;
    let _permit = st.io.acquire().await.map_err(|e| e.to_string())?;
    let mut cmd = tokio::process::Command::new(&c.psql);
    cmd.env("PGOPTIONS", "-c default_transaction_read_only=on")
        .env("PGCONNECT_TIMEOUT", "5")
        .args([
            "-X",
            "-q",
            "-A",
            "-t",
            "-v",
            "ON_ERROR_STOP=1",
            "-F",
            "\u{1f}",
        ])
        .args(["-h", &c.pg_host, "-p", &c.pg_port.to_string()])
        .args(["-U", &c.pg_user, "-d", &c.pg_db, "-c", sql])
        .stdin(std::process::Stdio::null())
        .kill_on_drop(true);
    let out = tokio::time::timeout(Duration::from_secs(20), cmd.output())
        .await
        .map_err(|_| "psql 超时".to_string())?
        .map_err(|e| format!("无法运行 psql：{e}"))?;
    if !out.status.success() {
        let msg = String::from_utf8_lossy(&out.stderr);
        return Err(format!("psql 失败：{}", msg.lines().next().unwrap_or("")));
    }
    Ok(String::from_utf8_lossy(&out.stdout)
        .lines()
        .filter(|l| !l.trim().is_empty())
        .map(|l| l.split('\u{1f}').map(|s| s.trim().to_string()).collect())
        .collect())
}

const CN_SQL: &str = "SELECT m.product, m.exchange, m.volume_multiple, m.verified, \
 coalesce(v.product_name, ''), coalesce(v.price_tick, ''), coalesce(v.contract_multiplier, ''), \
 coalesce(v.source_date::text, '') \
 FROM public.domestic_future_product_multipliers m \
 LEFT JOIN LATERAL (SELECT product_name, price_tick, contract_multiplier, source_date \
   FROM cn_futures_reference.instrument_versions iv \
   WHERE upper(iv.product_id) = m.product AND coalesce(iv.product_name, '') <> '' \
   ORDER BY iv.source_date DESC, iv.start_ts DESC LIMIT 1) v ON true \
 ORDER BY m.product";

async fn name_sources(st: &Arc<AppState>) -> Arc<NameSources> {
    st.names
        .get_or_init(|| async {
            let n = st.cfg.names.clone();
            st.blocking(move || {
                let (overrides, overrides_error) = load_overrides(&n);
                NameSources {
                    products: names::parse_products(names::CHECKLIST_SCRIPT),
                    ib_exchanges: names::parse_exchanges(names::CHECKLIST_SCRIPT),
                    exchange_summary: names::load_exchange_summary(&n.exchange_summary_csv),
                    ib_matches: names::load_ib_matches(&n.ib_matches_csv),
                    ib_snapshot: names::load_ib_snapshot(&n.ib_snapshot_json),
                    lseg_desc: names::load_lseg_descriptions(&n.ric_catalog_csv),
                    overrides,
                    overrides_error,
                }
            })
            .await
            .map(Arc::new)
            .unwrap_or_else(|_| {
                Arc::new(NameSources {
                    products: names::parse_products(names::CHECKLIST_SCRIPT),
                    ib_exchanges: names::parse_exchanges(names::CHECKLIST_SCRIPT),
                    exchange_summary: HashMap::new(),
                    ib_matches: Vec::new(),
                    ib_snapshot: HashMap::new(),
                    lseg_desc: HashMap::new(),
                    overrides: names::ProductNames::default(),
                    overrides_error: Some("品种中文名表未载入（后台任务失败）".into()),
                })
            })
        })
        .await
        .clone()
}

const SRC_CHECKLIST: &str =
    "IB期货与原始TAS中文清单（scripts/lseg/make_futures_checklist_excel.py）";

/// Builds the product index; returns it plus the name-coverage report.
pub async fn build_index(st: &Arc<AppState>) -> Result<(Index, Value), String> {
    let ns = name_sources(st).await;
    let cn_rows = psql(st, CN_SQL).await;
    let baseline = st.baseline.read().await.clone().unwrap_or_default();
    let dominant = st.dominant.read().await.clone();
    let catalog = st.catalog.read().await.clone();
    let mut products = Vec::new();
    let mut warnings = Vec::new();

    // ---- CN
    let dom: HashMap<String, Value> = dominant
        .as_deref()
        .and_then(|d| d["products"].as_array())
        .into_iter()
        .flatten()
        .filter_map(|p| Some((p["product"].as_str()?.to_string(), p.clone())))
        .collect();
    let mut cn_seen = BTreeSet::new();
    match &cn_rows {
        Ok(rows) => {
            for r in rows {
                if r.len() < 8 {
                    continue;
                }
                let (code, ex) = (r[0].clone(), r[1].clone());
                cn_seen.insert(code.clone());
                products.push(cn_product(&code, &ex, Some(r), &baseline, dom.get(&code)));
            }
        }
        Err(e) => warnings.push(format!("国内期货元数据：{e}")),
    }
    // Products present in the archive but not in the multiplier table (or
    // when PostgreSQL is unavailable).
    for (ex, ps) in baseline.iter() {
        for code in ps.keys() {
            if !cn_seen.contains(code) {
                cn_seen.insert(code.clone());
                products.push(cn_product(code, ex, None, &baseline, dom.get(code)));
            }
        }
    }

    // ---- LSEG + IB
    let mut lseg_to_ib: HashMap<String, String> = HashMap::new();
    for m in &ns.ib_matches {
        if !m.tas_root.is_empty() && m.status.starts_with("present") {
            lseg_to_ib
                .entry(m.tas_root.clone())
                .or_insert(m.ib_symbol.clone());
        }
    }
    let mut contract_links: Vec<(Market, String, String)> = Vec::new();
    if let Some(c) = catalog.as_deref() {
        // catalog ib_contracts with TAS data: LSEG product -> IB symbol
        let mut counts: HashMap<(&str, &str), usize> = HashMap::new();
        for k in c.ib.iter().filter(|k| k.tas_data) {
            *counts.entry((&k.product, &k.ib_symbol)).or_default() += 1;
        }
        let mut pairs: Vec<_> = counts.into_iter().collect();
        pairs.sort_by(|a, b| b.1.cmp(&a.1).then(a.0.cmp(&b.0)));
        for ((p, s), _) in pairs {
            lseg_to_ib.entry(p.to_string()).or_insert(s.to_string());
        }
        let mut codes: Vec<&String> = c.by_product.keys().collect();
        codes.sort();
        for code in codes {
            let ks: Vec<_> = c.product_contracts(code).collect();
            let exchanges: BTreeSet<String> = ks.iter().map(|k| k.exchange.clone()).collect();
            let latest = ks.iter().filter_map(|k| k.last_market_date.clone()).max();
            let mut active: Vec<&str> = ks
                .iter()
                .filter(|k| k.active)
                .map(|k| k.ric.as_str())
                .collect();
            active.sort();
            active.dedup();
            let years: Vec<i64> = ks.iter().map(|k| k.source_year).collect();
            let ib_sym = lseg_to_ib.get(code.as_str());
            let cn = ib_sym.and_then(|s| ns.products.get(s));
            for k in &ks {
                contract_links.push((Market::Lseg, code.clone(), k.ric.clone()));
            }
            products.push(Product {
                market: Market::Lseg,
                code: code.clone(),
                exchange_cn: exchanges
                    .iter()
                    .map(|e| {
                        ns.exchange_summary
                            .get(e)
                            .cloned()
                            .unwrap_or_else(|| e.clone())
                    })
                    .collect::<Vec<_>>()
                    .join("、"),
                exchanges: exchanges.into_iter().collect(),
                name_cn: cn.map(|c| c.name.clone()),
                name_cn_source: cn.map(|_| {
                    format!(
                        "{SRC_CHECKLIST}，经 IB {} 对应",
                        ib_sym.cloned().unwrap_or_default()
                    )
                }),
                category: cn.map(|c| c.category.clone()),
                name_en: ns.lseg_desc.get(code).cloned(),
                name_en_source: ns
                    .lseg_desc
                    .get(code)
                    .map(|_| "LSEG 合约描述（ric_catalog.csv，去掉月份）".to_string()),
                multiplier: None,
                tick: None,
                summary: json!({
                    "contracts": ks.len(),
                    "active_contracts": active.len(),
                    "active_rics": active.iter().take(8).collect::<Vec<_>>(),
                    "latest_day": latest,
                    "source_years": [years.iter().min(), years.iter().max()],
                    "ib_symbol": ib_sym,
                }),
            });
        }
        // IB symbols
        let mut by_sym: BTreeMap<&str, Vec<&crate::catalog::IbContract>> = BTreeMap::new();
        for k in &c.ib {
            by_sym.entry(&k.ib_symbol).or_default().push(k);
        }
        for (sym, ks) in by_sym {
            let exchanges: BTreeSet<String> = ks.iter().map(|k| k.ib_exchange.clone()).collect();
            let cn = ns.products.get(sym);
            let m = ns.ib_matches.iter().find(|m| m.ib_symbol == sym);
            let snap = ns.ib_snapshot.get(sym);
            let en = m
                .map(|m| m.ib_names.clone())
                .filter(|s| !s.is_empty())
                .or_else(|| snap.map(|s| s.2.clone()).filter(|s| !s.is_empty()));
            let first = ks.iter().filter_map(|k| k.first_day.clone()).min();
            let last = ks.iter().filter_map(|k| k.last_day.clone()).max();
            let current: Vec<&str> = ks
                .iter()
                .filter(|k| k.last_day.is_some() && k.last_day == last)
                .map(|k| k.ric.as_str())
                .take(8)
                .collect();
            let lseg_products: BTreeSet<&str> = ks
                .iter()
                .filter(|k| k.tas_data)
                .map(|k| k.product.as_str())
                .collect();
            for k in &ks {
                contract_links.push((Market::Ib, sym.to_string(), k.ric.clone()));
                contract_links.push((Market::Ib, sym.to_string(), k.ib_local_symbol.clone()));
            }
            products.push(Product {
                market: Market::Ib,
                code: sym.to_string(),
                exchange_cn: exchanges
                    .iter()
                    .map(|e| ns.ib_exchanges.get(e).cloned().unwrap_or_else(|| e.clone()))
                    .collect::<Vec<_>>()
                    .join("、"),
                exchanges: exchanges.into_iter().collect(),
                name_cn: cn.map(|c| c.name.clone()),
                name_cn_source: cn.map(|_| SRC_CHECKLIST.to_string()),
                category: cn.map(|c| c.category.clone()),
                name_en: en.clone(),
                name_en_source: en
                    .map(|_| "IB 合约元数据（all_ib_futures_tas_matches.csv）".to_string()),
                multiplier: snap
                    .filter(|s| !s.0.is_empty())
                    .map(|s| format!("{} {}", s.0, s.1).trim().to_string()),
                tick: None,
                summary: json!({
                    "contracts": ks.len(),
                    "ib_only": ks.iter().filter(|k| !k.tas_data).count(),
                    "first_day": first.map(|d| dash_day(&d)),
                    "last_day": last.map(|d| dash_day(&d)),
                    "current_rics": current,
                    "lseg_products": lseg_products,
                }),
            });
        }
    } else {
        warnings.push("catalog.sqlite 尚未载入，海外品种暂缺".into());
    }

    ns.overrides.apply(&mut products);
    if let Some(e) = &ns.overrides_error {
        warnings.push(e.clone());
    }
    let mut ix = Index::new(products);
    for (m, p, c) in contract_links {
        ix.add_contract(m, &p, &c);
    }
    // Coverage report.
    let mut coverage = serde_json::Map::new();
    for m in [Market::Cn, Market::Lseg, Market::Ib] {
        let all: Vec<&Product> = ix.products.iter().filter(|p| p.market == m).collect();
        let missing: Vec<Value> = all
            .iter()
            .filter(|p| p.name_cn.is_none())
            .map(|p| json!({"code": p.code, "exchanges": p.exchanges, "name_en": p.name_en}))
            .collect();
        coverage.insert(
            format!("{m:?}").to_lowercase(),
            json!({"products": all.len(), "with_chinese": all.len() - missing.len(), "missing": missing}),
        );
    }
    Ok((ix, json!({"coverage": coverage, "warnings": warnings})))
}

fn cn_product(
    code: &str,
    ex: &str,
    row: Option<&Vec<String>>,
    baseline: &Baseline,
    dom: Option<&Value>,
) -> Product {
    let nonempty = |s: &String| (!s.is_empty()).then(|| s.clone());
    let name = row.and_then(|r| nonempty(&r[4]));
    let b = baseline.get(ex).and_then(|m| m.get(code));
    Product {
        market: Market::Cn,
        code: code.to_string(),
        exchanges: vec![ex.to_string()],
        exchange_cn: names::cn_exchange_name(ex).to_string(),
        name_cn_source: name.as_ref().map(|_| {
            format!(
                "cn_futures_reference.instrument_versions（OpenCTP，{}）",
                row.map(|r| r[7].as_str()).unwrap_or("")
            )
        }),
        name_cn: name,
        category: None,
        name_en: None,
        name_en_source: None,
        multiplier: row.map(|r| {
            if r[3] == "t" {
                r[2].clone()
            } else {
                format!("{}（未核验）", r[2])
            }
        }),
        tick: row.and_then(|r| nonempty(&r[5])),
        summary: json!({
            "baseline": b.map(|(a, z, n)| json!({"first": a, "last": z, "days": n})),
            "dominant": dom.map(|d| json!({
                "dominant": d["dominant"], "scheduled_next": d["scheduled_next"],
                "trading_day": d["trading_day"], "last_roll_day": d["last_roll_day"],
                "rolls_next_day": d["rolls_next_day"],
            })),
            "multiplier_table": row.is_some(),
            "ctp_multiplier": row.and_then(|r| nonempty(&r[6])),
        }),
    }
}
