//! Product / contract drill-down. Bounded: IO under the semaphore, results in
//! a 256-entry / 5-minute LRU, NFS touched with a handful of readdirs only.
use crate::search::Market;
use crate::util::{dash_day, is_day_name, mtime};
use crate::{AppState, DrillQ};
use axum::http::StatusCode;
use serde_json::{json, Value};
use std::collections::{BTreeMap, HashMap};
use std::path::{Path, PathBuf};
use std::sync::Arc;
use std::time::SystemTime;

type R = Result<Value, (StatusCode, String)>;

fn nf(msg: impl Into<String>) -> (StatusCode, String) {
    (StatusCode::NOT_FOUND, msg.into())
}
fn ise(msg: impl Into<String>) -> (StatusCode, String) {
    (StatusCode::INTERNAL_SERVER_ERROR, msg.into())
}

/// Per exchange `roll/result/<ex>_eod.csv`, aggregated per instrument.
pub struct EodSummary {
    stamp: Option<SystemTime>,
    pub by_product: BTreeMap<String, Vec<EodContract>>,
}

#[derive(Clone, serde::Serialize)]
pub struct EodContract {
    pub instrument_id: String,
    pub first: String,
    pub last: String,
    pub days: usize,
    pub last_open_int: Option<f64>,
    pub last_volume: Option<f64>,
    pub last_close: Option<f64>,
    pub expired: bool,
}

fn eod_path(st: &AppState, ex: &str) -> Option<PathBuf> {
    let roll = st.cn_daily.as_ref().ok()?.roll_root.clone()?;
    Some(roll.join("result").join(format!("{ex}_eod.csv")))
}

pub fn parse_eod_summary(path: &Path) -> anyhow::Result<BTreeMap<String, Vec<EodContract>>> {
    let mut rdr = csv::Reader::from_path(path)?;
    let h = rdr.headers()?.clone();
    let col = |n: &str| h.iter().position(|x| x == n);
    let (Some(id), Some(ip), Some(ii)) = (col("trad_day"), col("product_id"), col("instrument_id"))
    else {
        anyhow::bail!("unexpected EOD header");
    };
    let (io, iv, ic, ie) = (col("open_int"), col("volume"), col("close"), col("expired"));
    let mut map: HashMap<String, (String, EodContract)> = HashMap::new();
    let mut rec = csv::StringRecord::new();
    while rdr.read_record(&mut rec)? {
        let day = rec.get(id).unwrap_or("");
        let inst = rec.get(ii).unwrap_or("");
        if inst.is_empty() {
            continue;
        }
        let f = |i: Option<usize>| {
            i.and_then(|i| rec.get(i))
                .and_then(|s| s.parse::<f64>().ok())
        };
        let e = map.entry(inst.to_string()).or_insert_with(|| {
            (
                rec.get(ip).unwrap_or("").to_string(),
                EodContract {
                    instrument_id: inst.to_string(),
                    first: day.to_string(),
                    last: day.to_string(),
                    days: 0,
                    last_open_int: None,
                    last_volume: None,
                    last_close: None,
                    expired: false,
                },
            )
        });
        let c = &mut e.1;
        c.days += 1;
        if day < c.first.as_str() {
            c.first = day.to_string();
        }
        if day >= c.last.as_str() {
            c.last = day.to_string();
            c.last_open_int = f(io);
            c.last_volume = f(iv);
            c.last_close = f(ic);
            c.expired = ie.and_then(|i| rec.get(i)) == Some("1");
        }
    }
    let mut out: BTreeMap<String, Vec<EodContract>> = BTreeMap::new();
    for (_, (p, c)) in map {
        out.entry(p).or_default().push(c);
    }
    for v in out.values_mut() {
        v.sort_by(|a, b| {
            b.last
                .cmp(&a.last)
                .then(a.instrument_id.cmp(&b.instrument_id))
        });
    }
    Ok(out)
}

async fn eod(st: &Arc<AppState>, ex: &str) -> Option<Arc<EodSummary>> {
    let path = eod_path(st, ex)?;
    let p2 = path.clone();
    let stamp = st.blocking(move || mtime(&p2)).await.ok().flatten();
    if let Some(cur) = st.eod.lock().ok()?.get(ex).cloned() {
        if cur.stamp == stamp {
            return Some(cur);
        }
    }
    let parsed = st
        .blocking(move || parse_eod_summary(&path))
        .await
        .ok()?
        .ok()?;
    let s = Arc::new(EodSummary {
        stamp,
        by_product: parsed,
    });
    st.eod.lock().ok()?.insert(ex.to_string(), s.clone());
    Some(s)
}

fn cached(st: &AppState, key: &str) -> Option<Value> {
    st.drill_cache.lock().ok()?.get(&key.to_string())
}
fn remember(st: &AppState, key: String, v: &Value) {
    if let Ok(mut c) = st.drill_cache.lock() {
        c.put(key, v.clone());
    }
}

pub async fn product(st: &Arc<AppState>, m: Market, code: &str) -> R {
    let ix = st.index.read().await.clone().ok_or_else(|| {
        (
            StatusCode::SERVICE_UNAVAILABLE,
            "品种索引正在构建".to_string(),
        )
    })?;
    let p = ix
        .get(m, code)
        .cloned()
        .ok_or_else(|| nf(format!("未找到品种 {code}")))?;
    let key = format!("p/{m:?}/{code}");
    if let Some(v) = cached(st, &key) {
        return Ok(v);
    }
    let v = match m {
        Market::Cn => {
            let ex = p.exchanges.first().cloned().unwrap_or_default();
            let contracts = eod(st, &ex)
                .await
                .and_then(|e| e.by_product.get(code).cloned())
                .unwrap_or_default();
            let url = format!(
                "{}/api/v1/dominant/{}",
                st.cfg.cn.dominant_api.trim_end_matches('/'),
                code
            );
            let rolls = match st.get_json(&url).await {
                Ok(h) => {
                    let rows = h["rows"].as_array().cloned().unwrap_or_default();
                    let mut ev: Vec<Value> = rows
                        .iter()
                        .filter(|r| r["roll_flag"] == true)
                        .map(|r| {
                            json!({"trad_day": r["trad_day"], "instrument_id": r["instrument_id"],
                                        "scheduled_next": r["scheduled_next"]})
                        })
                        .collect();
                    let n = ev.len();
                    ev = ev.into_iter().skip(n.saturating_sub(30)).rev().collect();
                    json!({"available": true, "rows": rows.len(), "rolls": ev,
                           "first_day": rows.first().map(|r| r["trad_day"].clone())})
                }
                Err(e) => json!({"available": false, "error": e}),
            };
            json!({"product": p, "contracts": contracts, "dominant_history": rolls})
        }
        Market::Lseg => {
            let cat = st
                .catalog
                .read()
                .await
                .clone()
                .ok_or_else(|| ise("catalog 尚未载入"))?;
            let contracts: Vec<Value> = cat
                .product_contracts(code)
                .map(|k| {
                    json!({"ric": k.ric, "source_year": k.source_year, "exchange": k.exchange,
                           "first_trade_utc": k.first_trade_utc.as_deref().map(|s| &s[..s.len().min(10)]),
                           "last_trade_utc": k.last_trade_utc.as_deref().map(|s| &s[..s.len().min(10)]),
                           "last_market_date": k.last_market_date, "state": k.last_session_state,
                           "expired": k.expired_at_utc.is_some(), "active": k.active,
                           "ib": cat.ib_by_code.contains_key(&k.ric.to_ascii_uppercase())})
                })
                .collect();
            json!({"product": p, "contracts": contracts})
        }
        Market::Ib => {
            let cat = st
                .catalog
                .read()
                .await
                .clone()
                .ok_or_else(|| ise("catalog 尚未载入"))?;
            let contracts: Vec<_> = cat
                .ib
                .iter()
                .filter(|k| k.ib_symbol == code)
                .cloned()
                .collect();
            json!({"product": p, "contracts": contracts})
        }
    };
    remember(st, key, &v);
    Ok(v)
}

/// Day directories of a contract (one readdir per UTC-year directory) and
/// the files of the latest days (one readdir each).
fn list_days(dirs: &[(i64, String)], detail_days: usize, day: Option<&str>) -> Value {
    let mut days: Vec<(String, String)> = Vec::new();
    let mut errors = Vec::new();
    for (_, d) in dirs {
        match std::fs::read_dir(d) {
            Ok(rd) => {
                for e in rd.flatten() {
                    let n = e.file_name().to_string_lossy().to_string();
                    if is_day_name(&n) {
                        days.push((n, d.clone()));
                    }
                }
            }
            Err(e) => errors.push(format!("{d}: {e}")),
        }
    }
    days.sort();
    days.dedup_by(|a, b| a.0 == b.0);
    let mut want: Vec<&(String, String)> = days.iter().rev().take(detail_days).collect();
    if let Some(dd) = day {
        if let Some(x) = days.iter().find(|x| x.0 == dd) {
            if !want.iter().any(|w| w.0 == dd) {
                want.push(x);
            }
        }
    }
    let files: BTreeMap<String, Vec<String>> = want
        .into_iter()
        .map(|(n, d)| {
            let mut fs: Vec<String> = std::fs::read_dir(Path::new(d).join(n))
                .into_iter()
                .flatten()
                .flatten()
                .map(|e| e.file_name().to_string_lossy().to_string())
                .collect();
            fs.sort();
            (dash_day(n), fs)
        })
        .collect();
    json!({
        "days": days.iter().map(|(n, _)| dash_day(n)).collect::<Vec<_>>(),
        "count": days.len(),
        "files": files,
        "errors": errors,
    })
}

pub async fn contract(st: &Arc<AppState>, m: Market, code: &str, q: &DrillQ) -> R {
    let day = q
        .day
        .as_deref()
        .map(|d| d.replace('-', ""))
        .filter(|d| is_day_name(d));
    let key = format!("c/{m:?}/{code}/{:?}/{:?}", q.year, day);
    if let Some(v) = cached(st, &key) {
        return Ok(v);
    }
    let v = match m {
        Market::Cn => {
            let (root, _) =
                crate::search::split_cn_contract(code).ok_or_else(|| nf("合约代码格式不对"))?;
            let product = root.to_ascii_uppercase();
            let ix = st
                .index
                .read()
                .await
                .clone()
                .ok_or_else(|| ise("品种索引正在构建"))?;
            let p = ix
                .get(Market::Cn, &product)
                .cloned()
                .ok_or_else(|| nf(format!("未找到品种 {product}")))?;
            let ex = p.exchanges.first().cloned().unwrap_or_default();
            let path = eod_path(st, &ex).ok_or_else(|| ise("未配置 roll_root"))?;
            let inst = code.to_string();
            let rows = st
                .blocking(move || -> anyhow::Result<Vec<Value>> {
                    let mut rdr = csv::Reader::from_path(&path)?;
                    let h = rdr.headers()?.clone();
                    let mut out = Vec::new();
                    let ii = h.iter().position(|x| x == "instrument_id").unwrap_or(2);
                    let mut rec = csv::StringRecord::new();
                    while rdr.read_record(&mut rec)? {
                        if rec.get(ii).is_some_and(|x| x.eq_ignore_ascii_case(&inst)) {
                            let o: serde_json::Map<String, Value> = h
                                .iter()
                                .zip(rec.iter())
                                .map(|(k, v)| (k.to_string(), json!(v)))
                                .collect();
                            out.push(Value::Object(o));
                        }
                    }
                    Ok(out)
                })
                .await
                .map_err(ise)?
                .map_err(|e| ise(e.to_string()))?;
            if rows.is_empty() {
                return Err(nf(format!("roll EOD 中没有合约 {code}")));
            }
            let id = rows[0]["instrument_id"].clone();
            json!({"market": "cn", "contract": id, "product": p,
                   "baseline": p.summary["baseline"],
                   "days": rows.iter().map(|r| r["trad_day"].clone()).collect::<Vec<_>>(),
                   "eod": rows.iter().rev().take(30).cloned().collect::<Vec<_>>(),
                   "note": "日期来自 roll/result EOD（与 baseline_data_1min 同源 RocksDB）；该品种当日的 1 分钟文件包含此合约"})
        }
        Market::Lseg => {
            let cat = st
                .catalog
                .read()
                .await
                .clone()
                .ok_or_else(|| ise("catalog 尚未载入"))?;
            let idxs = cat
                .by_ric
                .get(&code.to_ascii_uppercase())
                .ok_or_else(|| nf(format!("catalog 中没有 RIC {code}")))?;
            let rows: Vec<_> = idxs.iter().map(|&i| cat.contracts[i].clone()).collect();
            let chosen = q
                .year
                .and_then(|y| rows.iter().find(|r| r.source_year == y))
                .or_else(|| rows.iter().max_by_key(|r| r.source_year))
                .cloned()
                .ok_or_else(|| nf("无数据"))?;
            let dirs = cat
                .dirs
                .get(&(chosen.ric.clone(), chosen.source_year))
                .cloned()
                .unwrap_or_default();
            let ib = cat
                .ib_by_code
                .get(&code.to_ascii_uppercase())
                .map(|&i| cat.ib[i].clone());
            let d2 = dirs.clone();
            let listing = st
                .blocking(move || list_days(&d2, 10, day.as_deref()))
                .await
                .map_err(ise)?;
            json!({"market": "lseg", "contract": chosen.ric, "source_year": chosen.source_year,
                   "rows": rows, "directories": dirs, "ib": ib, "listing": listing})
        }
        Market::Ib => {
            let cat = st
                .catalog
                .read()
                .await
                .clone()
                .ok_or_else(|| ise("catalog 尚未载入"))?;
            let i = *cat
                .ib_by_code
                .get(&code.to_ascii_uppercase())
                .ok_or_else(|| nf(format!("catalog 中没有 IB 合约 {code}")))?;
            let k = cat.ib[i].clone();
            let dirs = cat.ib_dirs.get(&k.ric).cloned().unwrap_or_default();
            let d2 = dirs.clone();
            let listing = st
                .blocking(move || list_days(&d2, 10, day.as_deref()))
                .await
                .map_err(ise)?;
            let tas: Vec<_> = cat
                .by_ric
                .get(&k.ric.to_ascii_uppercase())
                .map(|v| v.iter().map(|&i| cat.contracts[i].source_year).collect())
                .unwrap_or_else(Vec::new);
            json!({"market": "ib", "contract": k.ric, "ib": k, "directories": dirs,
                   "tas_source_years": tas, "listing": listing})
        }
    };
    remember(st, key, &v);
    Ok(v)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn eod_summary_per_instrument() {
        let dir = tempfile::tempdir().unwrap();
        let p = dir.path().join("xsge_eod.csv");
        std::fs::write(
            &p,
            "trad_day,product_id,instrument_id,open_int,volume,close,expired\n\
             2026-11-13,RB,rb2701,100.0,5.0,3100.0,0\n\
             2026-11-13,RB,rb2610,10.0,1.0,3000.0,0\n\
             2026-11-16,RB,rb2701,120.0,6.0,3120.0,0\n\
             2026-11-16,AU,au2612,7.0,2.0,600.0,0\n",
        )
        .unwrap();
        let s = parse_eod_summary(&p).unwrap();
        let rb = &s["RB"];
        assert_eq!(rb.len(), 2);
        assert_eq!(rb[0].instrument_id, "rb2701");
        assert_eq!(rb[0].days, 2);
        assert_eq!(rb[0].first, "2026-11-13");
        assert_eq!(rb[0].last_open_int, Some(120.0));
        assert_eq!(s["AU"][0].last, "2026-11-16");
    }

    #[test]
    fn lists_day_directories() {
        let dir = tempfile::tempdir().unwrap();
        let y = dir.path().join("2026");
        for (d, f) in [
            ("20261112", "kline.parquet"),
            ("20261113", "kline_ib.parquet"),
        ] {
            std::fs::create_dir_all(y.join(d)).unwrap();
            std::fs::write(y.join(d).join(f), b"").unwrap();
        }
        std::fs::write(y.join("README"), b"").unwrap();
        let v = list_days(
            &[(2026, y.to_string_lossy().to_string())],
            1,
            Some("20261112"),
        );
        assert_eq!(v["count"], 2);
        assert_eq!(v["days"][1], "2026-11-13");
        assert_eq!(v["files"]["2026-11-13"][0], "kline_ib.parquet");
        assert_eq!(v["files"]["2026-11-12"][0], "kline.parquet");
    }
}
