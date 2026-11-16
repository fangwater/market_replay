//! Admin HTTP API (mounted on the shared HTTP port):
//!   GET  /api/admin/status                 services, jobs, queue
//!   GET  /api/admin/runs?job=&limit=       run history, newest first
//!   GET  /api/admin/log/{service|job}/{name}?bytes=   log tail (text)
//!   POST /api/admin/jobs/{name}/run        queue a job now
//!   POST /api/admin/services/{name}/restart
//! POST requires a loopback client, or header `X-Admin-Token` equal to the
//! `MARKET_REPLAY_ADMIN_TOKEN` environment variable when that is set.

use crate::manager::{recent_runs, Manager};
use crate::process;
use axum::extract::{ConnectInfo, Path, Query, State};
use axum::http::{HeaderMap, StatusCode};
use axum::response::{Html, IntoResponse, Response};
use axum::routing::{get, post};
use axum::{Json, Router};
use serde::Deserialize;
use serde_json::json;
use std::net::SocketAddr;
use std::sync::Arc;

type St = State<Arc<Manager>>;

fn authorized(addr: &SocketAddr, headers: &HeaderMap) -> bool {
    if addr.ip().is_loopback() {
        return true;
    }
    match std::env::var("MARKET_REPLAY_ADMIN_TOKEN") {
        Ok(t) if !t.is_empty() => headers
            .get("x-admin-token")
            .and_then(|v| v.to_str().ok())
            .is_some_and(|v| v == t),
        _ => false,
    }
}

fn err(code: StatusCode, msg: impl Into<String>) -> Response {
    (code, Json(json!({"error": msg.into()}))).into_response()
}

async fn status(State(m): St) -> Response {
    Json(m.snapshot()).into_response()
}

#[derive(Deserialize)]
struct RunsQ {
    job: Option<String>,
    limit: Option<usize>,
}

async fn runs(State(m): St, Query(q): Query<RunsQ>) -> Response {
    Json(recent_runs(
        m.config(),
        q.job.as_deref(),
        q.limit.unwrap_or(50).min(500),
    ))
    .into_response()
}

#[derive(Deserialize)]
struct LogQ {
    bytes: Option<u64>,
}

async fn log(
    State(m): St,
    Path((kind, name)): Path<(String, String)>,
    Query(q): Query<LogQ>,
) -> Response {
    let known = match kind.as_str() {
        "service" => m.config().services.iter().any(|s| s.name == name),
        "job" => m.config().jobs.iter().any(|j| j.name == name),
        _ => false,
    };
    if !known {
        return err(StatusCode::NOT_FOUND, format!("unknown {kind} {name}"));
    }
    let path = process::log_path(&m.config().state_dir, &kind, &name);
    process::tail(&path, q.bytes.unwrap_or(64 << 10).min(4 << 20)).into_response()
}

async fn run_job(
    State(m): St,
    ConnectInfo(addr): ConnectInfo<SocketAddr>,
    headers: HeaderMap,
    Path(name): Path<String>,
) -> Response {
    if !authorized(&addr, &headers) {
        return err(StatusCode::FORBIDDEN, "admin token required");
    }
    match m.enqueue(&name, "manual", None) {
        Ok(true) => Json(json!({"queued": name})).into_response(),
        Ok(false) => {
            Json(json!({"queued": null, "note": "already queued or running"})).into_response()
        }
        Err(e) => err(StatusCode::NOT_FOUND, e),
    }
}

async fn restart_service(
    State(m): St,
    ConnectInfo(addr): ConnectInfo<SocketAddr>,
    headers: HeaderMap,
    Path(name): Path<String>,
) -> Response {
    if !authorized(&addr, &headers) {
        return err(StatusCode::FORBIDDEN, "admin token required");
    }
    match m.restart_service(&name) {
        Ok(()) => Json(json!({"restarting": name})).into_response(),
        Err(e) => err(StatusCode::NOT_FOUND, e),
    }
}

async fn page() -> Html<&'static str> {
    Html(include_str!("admin.html"))
}

pub fn router(m: Arc<Manager>) -> Router {
    Router::new()
        .route("/admin", get(page))
        .route("/api/admin/status", get(status))
        .route("/api/admin/runs", get(runs))
        .route("/api/admin/log/{kind}/{name}", get(log))
        .route("/api/admin/jobs/{name}/run", post(run_job))
        .route("/api/admin/services/{name}/restart", post(restart_service))
        .with_state(m)
}
