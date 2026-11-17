//! data-dashboard: read-only web page 数据处理进度与品种查询.
//!
//! Library layout so the router can be mounted into a combined server:
//! ```ignore
//! let state = data_dashboard::AppState::new(cfg)?;
//! data_dashboard::spawn_background(state.clone()); // inside a tokio runtime
//! let app = axum::Router::new().nest("/dashboard", data_dashboard::router(state));
//! ```
//! UI at `/`, JSON under `/api/dashboard/...`; the page uses relative URLs
//! only. Inside `market-replay-server` the page also reads the server's own
//! `/api/admin/*` (services, jobs, runs, logs, manual run / restart); when
//! the standalone binary serves it, that panel reports the API as absent.
//!
//! All sources are read-only. Progress panels are refreshed by one background
//! task on fixed intervals (requests never trigger a scan); drill-down
//! requests are bounded by a semaphore and a small TTL/LRU cache.
pub mod catalog;
pub mod config;
mod drill;
pub mod names;
pub mod progress;
pub mod rolls;
pub mod search;
mod sources;
pub mod util;

use axum::extract::{Path as UrlPath, Query, State};
use axum::http::{header, StatusCode};
use axum::response::{IntoResponse, Response};
use axum::routing::get;
use axum::{Json, Router};
use serde::{Deserialize, Serialize};
use serde_json::{json, Value};
use std::collections::{BTreeMap, HashMap};
use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant};
use tokio::sync::{RwLock, Semaphore};

pub use config::Config;

const INDEX_HTML: &str = include_str!("assets/index.html");

#[derive(Clone, Serialize)]
pub struct Section {
    pub updated_at: String,
    pub took_ms: u128,
    pub ok: bool,
    pub data: Value,
    pub error: Option<String>,
}

pub struct AppState {
    pub cfg: Config,
    pub cn_daily: Result<config::CnDaily, String>,
    pub mdl: Result<config::MdlReplay, String>,
    http: reqwest::Client,
    io: Semaphore,
    sections: RwLock<BTreeMap<&'static str, Section>>,
    catalog: RwLock<Option<Arc<catalog::Catalog>>>,
    baseline: RwLock<Option<Arc<sources::Baseline>>>,
    dominant: RwLock<Option<Arc<Value>>>,
    index: RwLock<Option<Arc<search::Index>>>,
    names: tokio::sync::OnceCell<Arc<sources::NameSources>>,
    eod: Mutex<HashMap<String, Arc<drill::EodSummary>>>,
    drill_cache: Mutex<util::Lru<String, Value>>,
}

impl AppState {
    pub fn new(cfg: Config) -> anyhow::Result<Arc<Self>> {
        let cn_daily = config::CnDaily::load(&cfg.cn.cn_daily_config).map_err(|e| format!("{e:#}"));
        let mdl =
            config::MdlReplay::load(&cfg.services.mdl_replay_config).map_err(|e| format!("{e:#}"));
        let http = reqwest::Client::builder()
            .no_proxy()
            .timeout(Duration::from_secs(8))
            .build()?;
        let permits = cfg.io_permits.max(1);
        Ok(Arc::new(Self {
            cfg,
            cn_daily,
            mdl,
            http,
            io: Semaphore::new(permits),
            sections: RwLock::new(BTreeMap::new()),
            catalog: RwLock::new(None),
            baseline: RwLock::new(None),
            dominant: RwLock::new(None),
            index: RwLock::new(None),
            names: tokio::sync::OnceCell::new(),
            eod: Mutex::new(HashMap::new()),
            drill_cache: Mutex::new(util::Lru::new(256, Duration::from_secs(300))),
        }))
    }

    async fn set(&self, name: &'static str, started: Instant, r: Result<Value, String>) {
        let s = match r {
            Ok(data) => Section {
                updated_at: util::now_utc(),
                took_ms: started.elapsed().as_millis(),
                ok: true,
                data,
                error: None,
            },
            Err(e) => {
                // Keep the last good data, flag the error.
                let prev = self.sections.read().await.get(name).map(|s| s.data.clone());
                Section {
                    updated_at: util::now_utc(),
                    took_ms: started.elapsed().as_millis(),
                    ok: false,
                    data: prev.unwrap_or(Value::Null),
                    error: Some(e),
                }
            }
        };
        self.sections.write().await.insert(name, s);
    }

    /// Runs a blocking job under the IO semaphore on the blocking pool.
    async fn blocking<T: Send + 'static>(
        &self,
        f: impl FnOnce() -> T + Send + 'static,
    ) -> Result<T, String> {
        let _permit = self.io.acquire().await.map_err(|e| e.to_string())?;
        tokio::task::spawn_blocking(f)
            .await
            .map_err(|e| e.to_string())
    }

    async fn get_json(&self, url: &str) -> Result<Value, String> {
        let r = self
            .http
            .get(url)
            .send()
            .await
            .map_err(|e| format!("{url}: {e}"))?;
        if !r.status().is_success() {
            return Err(format!("{url}: HTTP {}", r.status()));
        }
        r.json::<Value>().await.map_err(|e| format!("{url}: {e}"))
    }
}

/// Background refresh schedule (seconds).
const SCHEDULE: [(&str, u64); 10] = [
    ("dominant", 60),
    ("baseline", 600),
    ("catalog", 60),
    ("cn", 60),
    ("lseg", 120),
    ("ib", 120),
    ("mdl_cache", 300),
    ("reference", 300),
    ("rolls", 60),
    ("size_thresholds", 600),
];

/// Starts the single refresher task. Must be called inside a tokio runtime.
pub fn spawn_background(state: Arc<AppState>) {
    tokio::spawn(async move {
        let mut last: HashMap<&'static str, Instant> = HashMap::new();
        let mut last_index: Option<Instant> = None;
        loop {
            let mut dirty = false;
            for (name, every) in SCHEDULE {
                let due = last
                    .get(name)
                    .is_none_or(|t| t.elapsed() >= Duration::from_secs(every));
                if due {
                    dirty |= sources::refresh(&state, name).await;
                    last.insert(name, Instant::now());
                }
            }
            if dirty || last_index.is_none_or(|t| t.elapsed() >= Duration::from_secs(600)) {
                let t = Instant::now();
                let r = sources::build_index(&state).await;
                match r {
                    Ok((ix, coverage)) => {
                        *state.index.write().await = Some(Arc::new(ix));
                        state.set("names", t, Ok(coverage)).await;
                    }
                    Err(e) => state.set("names", t, Err(e)).await,
                }
                last_index = Some(Instant::now());
            }
            tokio::time::sleep(Duration::from_secs(5)).await;
        }
    });
}

pub fn router(state: Arc<AppState>) -> Router {
    Router::new()
        .route("/", get(index_page))
        .route("/index.html", get(index_page))
        .route("/api/dashboard/healthz", get(|| async { "ok" }))
        .route("/api/dashboard/progress", get(progress_api))
        .route("/api/dashboard/search", get(search_api))
        .route("/api/dashboard/product/{market}/{code}", get(product_api))
        .route("/api/dashboard/contract/{market}/{code}", get(contract_api))
        .with_state(state)
}

async fn index_page() -> Response {
    (
        [
            (header::CONTENT_TYPE, "text/html; charset=utf-8"),
            (header::CACHE_CONTROL, "no-cache"),
        ],
        INDEX_HTML,
    )
        .into_response()
}

async fn progress_api(State(st): State<Arc<AppState>>) -> Json<Value> {
    let sections = st.sections.read().await.clone();
    Json(json!({
        "generated_at": util::now_utc(),
        "sections": sections,
    }))
}

#[derive(Deserialize)]
struct SearchQ {
    q: Option<String>,
    limit: Option<usize>,
}

async fn search_api(State(st): State<Arc<AppState>>, Query(q): Query<SearchQ>) -> Response {
    let Some(ix) = st.index.read().await.clone() else {
        return err(StatusCode::SERVICE_UNAVAILABLE, "品种索引正在构建，请稍候");
    };
    let query = q.q.unwrap_or_default();
    let query: String = query.chars().take(64).collect();
    let hits = ix.search(&query, q.limit.unwrap_or(30).clamp(1, 100));
    let mut groups: BTreeMap<search::Market, Vec<&search::Hit>> = BTreeMap::new();
    for h in &hits {
        groups.entry(h.product.market).or_default().push(h);
    }
    let groups: Vec<Value> = groups
        .into_iter()
        .map(|(m, v)| json!({"market": m, "label": m.label(), "hits": v}))
        .collect();
    Json(json!({"q": query, "total": hits.len(), "groups": groups})).into_response()
}

#[derive(Deserialize)]
pub struct DrillQ {
    pub year: Option<i64>,
    pub day: Option<String>,
}

async fn product_api(
    State(st): State<Arc<AppState>>,
    UrlPath((market, code)): UrlPath<(String, String)>,
) -> Response {
    let Some(m) = search::Market::parse(&market) else {
        return err(StatusCode::NOT_FOUND, "未知市场");
    };
    match drill::product(&st, m, &code).await {
        Ok(v) => Json(v).into_response(),
        Err((c, e)) => err(c, &e),
    }
}

async fn contract_api(
    State(st): State<Arc<AppState>>,
    UrlPath((market, code)): UrlPath<(String, String)>,
    Query(q): Query<DrillQ>,
) -> Response {
    let Some(m) = search::Market::parse(&market) else {
        return err(StatusCode::NOT_FOUND, "未知市场");
    };
    match drill::contract(&st, m, &code, &q).await {
        Ok(v) => Json(v).into_response(),
        Err((c, e)) => err(c, &e),
    }
}

fn err(code: StatusCode, msg: &str) -> Response {
    (code, Json(json!({"error": msg}))).into_response()
}
