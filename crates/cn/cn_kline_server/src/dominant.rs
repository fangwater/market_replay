//! Dominant (主力) contract lookup over the roll replay outputs
//! `<roll_dir>/<exchange>_dominant.csv` (one row per product per TradDay,
//! rule `local_rqdata_rule0`; see docs/cn_daily_and_kline.md).
//!
//! Semantics carried over from the roll replay: `instrument_id` is the
//! dominant contract in effect on `trad_day` (decided after the previous
//! close); `scheduled_next` is what the close of `trad_day` assigns to the
//! next trading day. Files are re-read when their size or mtime changes.

use anyhow::{Context, Result};
use axum::extract::{Path as UrlPath, Query, State};
use axum::http::StatusCode;
use axum::response::{IntoResponse, Response};
use axum::routing::get;
use axum::{Json, Router};
use serde::{Deserialize, Serialize};
use serde_json::json;
use std::collections::BTreeMap;
use std::path::{Path, PathBuf};
use std::sync::{Arc, RwLock};
use std::time::SystemTime;

pub const EXCHANGES: [&str; 6] = ["ccfx", "xdce", "xgfe", "xsge", "xsie", "xzce"];

#[derive(Clone, Debug, Serialize, PartialEq)]
pub struct DominantRow {
    pub trad_day: String,
    pub exchange: String,
    pub product: String,
    /// Empty before the first dominant is scheduled.
    pub instrument_id: String,
    pub roll_flag: bool,
    pub decision_open_int: Option<f64>,
    pub scheduled_next: String,
    pub rule: String,
}

#[derive(Default)]
struct Loaded {
    stamps: Vec<(PathBuf, Option<(u64, SystemTime)>)>,
    by_product: BTreeMap<String, Vec<DominantRow>>,
}

pub struct DominantStore {
    dir: PathBuf,
    inner: RwLock<Loaded>,
}

fn stamp(path: &Path) -> Option<(u64, SystemTime)> {
    let m = std::fs::metadata(path).ok()?;
    Some((m.len(), m.modified().ok()?))
}

pub fn parse_file(path: &Path, exchange: &str) -> Result<Vec<DominantRow>> {
    let mut reader = csv::Reader::from_path(path).with_context(|| format!("open {path:?}"))?;
    let headers = reader.headers()?.clone();
    let col = |name: &str| {
        headers
            .iter()
            .position(|h| h == name)
            .with_context(|| format!("{path:?} has no column {name}"))
    };
    let (td, prod, inst, roll, oi, next, rule) = (
        col("trad_day")?,
        col("product_id")?,
        col("instrument_id")?,
        col("roll_flag")?,
        col("decision_open_int")?,
        col("scheduled_next")?,
        col("rule")?,
    );
    let mut out = Vec::new();
    for record in reader.records() {
        let r = record?;
        let oi_text = r.get(oi).unwrap_or("");
        out.push(DominantRow {
            trad_day: r.get(td).unwrap_or("").to_string(),
            exchange: exchange.to_string(),
            product: r.get(prod).unwrap_or("").to_ascii_uppercase(),
            instrument_id: r.get(inst).unwrap_or("").to_string(),
            roll_flag: r.get(roll) == Some("1"),
            decision_open_int: if oi_text.is_empty() {
                None
            } else {
                Some(
                    oi_text
                        .parse()
                        .with_context(|| format!("bad decision_open_int {oi_text:?}"))?,
                )
            },
            scheduled_next: r.get(next).unwrap_or("").to_string(),
            rule: r.get(rule).unwrap_or("").to_string(),
        });
    }
    Ok(out)
}

impl DominantStore {
    pub fn new(dir: PathBuf) -> Self {
        Self {
            dir,
            inner: RwLock::new(Loaded::default()),
        }
    }

    fn paths(&self) -> Vec<(String, PathBuf)> {
        EXCHANGES
            .iter()
            .map(|e| (e.to_string(), self.dir.join(format!("{e}_dominant.csv"))))
            .collect()
    }

    /// Re-reads all files when any of them changed since the last load.
    pub fn refresh(&self) -> Result<()> {
        let current: Vec<_> = self
            .paths()
            .into_iter()
            .map(|(_, p)| {
                let s = stamp(&p);
                (p, s)
            })
            .collect();
        if self.inner.read().unwrap().stamps == current {
            return Ok(());
        }
        let mut by_product: BTreeMap<String, Vec<DominantRow>> = BTreeMap::new();
        for (exchange, path) in self.paths() {
            if !path.exists() {
                continue;
            }
            for row in parse_file(&path, &exchange)? {
                by_product.entry(row.product.clone()).or_default().push(row);
            }
        }
        for rows in by_product.values_mut() {
            rows.sort_by(|a, b| a.trad_day.cmp(&b.trad_day));
        }
        *self.inner.write().unwrap() = Loaded {
            stamps: current,
            by_product,
        };
        Ok(())
    }

    pub fn latest(&self) -> Vec<serde_json::Value> {
        let inner = self.inner.read().unwrap();
        inner
            .by_product
            .values()
            .filter_map(|rows| {
                let last = rows.last()?;
                let last_roll = rows.iter().rev().find(|r| r.roll_flag).map(|r| r.trad_day.clone());
                Some(json!({
                    "exchange": last.exchange,
                    "product": last.product,
                    "trading_day": last.trad_day,
                    "dominant": last.instrument_id,
                    "scheduled_next": last.scheduled_next,
                    "rolls_next_day": !last.scheduled_next.is_empty() && last.scheduled_next != last.instrument_id,
                    "last_roll_day": last_roll,
                    "decision_open_int": last.decision_open_int,
                    "rule": last.rule,
                }))
            })
            .collect()
    }

    pub fn history(
        &self,
        product: &str,
        start: Option<&str>,
        end: Option<&str>,
    ) -> Option<Vec<DominantRow>> {
        let inner = self.inner.read().unwrap();
        let rows = inner.by_product.get(&product.to_ascii_uppercase())?;
        Some(
            rows.iter()
                .filter(|r| {
                    start.is_none_or(|s| r.trad_day.as_str() >= s)
                        && end.is_none_or(|e| r.trad_day.as_str() <= e)
                })
                .cloned()
                .collect(),
        )
    }
}

#[derive(Deserialize)]
struct Range {
    start: Option<String>,
    end: Option<String>,
}

fn normalize_day(s: &str) -> Option<String> {
    let d = s.replace('-', "");
    (d.len() == 8 && d.bytes().all(|b| b.is_ascii_digit()))
        .then(|| format!("{}-{}-{}", &d[..4], &d[4..6], &d[6..]))
}

fn error(code: StatusCode, message: String) -> Response {
    (code, Json(json!({"error": message}))).into_response()
}

async fn latest(State(store): State<Arc<DominantStore>>) -> Response {
    if let Err(e) = store.refresh() {
        return error(StatusCode::INTERNAL_SERVER_ERROR, format!("{e:#}"));
    }
    let products = store.latest();
    let latest_day = products
        .iter()
        .filter_map(|p| p["trading_day"].as_str())
        .max()
        .map(str::to_string);
    Json(json!({
        "latest_trading_day": latest_day,
        "count": products.len(),
        "products": products,
        "note": "dominant = contract in effect on trading_day; scheduled_next = contract for the next trading day (decided at trading_day close)",
    }))
    .into_response()
}

async fn history(
    State(store): State<Arc<DominantStore>>,
    UrlPath(product): UrlPath<String>,
    Query(range): Query<Range>,
) -> Response {
    if let Err(e) = store.refresh() {
        return error(StatusCode::INTERNAL_SERVER_ERROR, format!("{e:#}"));
    }
    let start = match range.start.as_deref().map(normalize_day) {
        Some(None) => {
            return error(
                StatusCode::BAD_REQUEST,
                "start must be YYYYMMDD or YYYY-MM-DD".into(),
            )
        }
        other => other.flatten(),
    };
    let end = match range.end.as_deref().map(normalize_day) {
        Some(None) => {
            return error(
                StatusCode::BAD_REQUEST,
                "end must be YYYYMMDD or YYYY-MM-DD".into(),
            )
        }
        other => other.flatten(),
    };
    match store.history(&product, start.as_deref(), end.as_deref()) {
        Some(rows) => Json(
            json!({"product": product.to_ascii_uppercase(), "count": rows.len(), "rows": rows}),
        )
        .into_response(),
        None => error(StatusCode::NOT_FOUND, format!("unknown product {product}")),
    }
}

pub fn router(store: Arc<DominantStore>) -> Router {
    Router::new()
        .route("/api/v1/dominant", get(latest))
        .route("/api/v1/dominant/{product}", get(history))
        .route("/healthz", get(|| async { "ok" }))
        .with_state(store)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn latest_reports_effective_and_next_contracts() {
        let t = tempfile::tempdir().unwrap();
        std::fs::write(
            t.path().join("xsge_dominant.csv"),
            "trad_day,product_id,instrument_id,roll_flag,decision_open_int,scheduled_next,rule\n\
             2026-08-24,RB,rb2610,0,100.0,rb2610,local_rqdata_rule0\n\
             2026-08-25,RB,rb2610,0,90.0,rb2701,local_rqdata_rule0\n\
             2026-08-26,RB,rb2701,1,200.0,rb2701,local_rqdata_rule0\n\
             2026-08-26,AD,,0,,ad2611,local_rqdata_rule0\n",
        )
        .unwrap();
        let store = DominantStore::new(t.path().to_path_buf());
        store.refresh().unwrap();
        let latest = store.latest();
        let rb = latest.iter().find(|p| p["product"] == "RB").unwrap();
        assert_eq!(rb["dominant"], "rb2701");
        assert_eq!(rb["last_roll_day"], "2026-08-26");
        assert_eq!(rb["rolls_next_day"], false);
        let ad = latest.iter().find(|p| p["product"] == "AD").unwrap();
        assert_eq!(ad["dominant"], "");
        assert_eq!(ad["rolls_next_day"], true);
        let h = store.history("rb", Some("2026-08-25"), None).unwrap();
        assert_eq!(h.len(), 2);
        assert_eq!(normalize_day("20260825").as_deref(), Some("2026-08-25"));
    }
}
