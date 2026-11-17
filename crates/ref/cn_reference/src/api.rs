//! `/api/v1` HTTP surface (port of the handled routes of `api.py`). Anything
//! not handled here is proxied unchanged to the Python process, which still
//! owns the web UI, announcements, message fees, fee estimates and imports.
use crate::json::J;
use crate::models::{DataError, SOURCE_TYPE};
use crate::services;
use crate::storage::Db;
use anyhow::Result;
use axum::body::Body;
use axum::http::{
    header, HeaderMap, HeaderName, HeaderValue, Method, Request, Response, StatusCode,
};
use percent_encoding::percent_decode_str;
use std::collections::HashMap;
use std::sync::Arc;
use std::time::Duration;

const MAX_REQUEST_BYTES: usize = 16 * 1024 * 1024;

pub struct AppState {
    pub db: Db,
    /// Base URL of the Python fallback, e.g. `http://127.0.0.1:18766`.
    pub fallback: String,
    http: reqwest::Client,
}

impl AppState {
    pub fn new(db: Db, fallback: String) -> Result<Arc<Self>> {
        Ok(Arc::new(Self {
            db,
            fallback: fallback.trim_end_matches('/').to_string(),
            http: reqwest::Client::builder()
                .no_proxy()
                .timeout(Duration::from_secs(900))
                .redirect(reqwest::redirect::Policy::none())
                .build()?,
        }))
    }
}

pub fn router(state: Arc<AppState>) -> axum::Router {
    axum::Router::new().fallback(dispatch).with_state(state)
}

// ------------------------------------------------------------- helpers

/// `urllib.parse.parse_qs` (blank values dropped, first value wins).
fn parse_qs(query: &str) -> HashMap<String, String> {
    let mut out = HashMap::new();
    for pair in query.split('&') {
        let Some((k, v)) = pair.split_once('=') else {
            continue;
        };
        if v.is_empty() {
            continue;
        }
        let dec = |s: &str| {
            percent_decode_str(&s.replace('+', " "))
                .decode_utf8_lossy()
                .into_owned()
        };
        out.entry(dec(k)).or_insert_with(|| dec(v));
    }
    out
}

fn unquote(s: &str) -> String {
    percent_decode_str(s).decode_utf8_lossy().into_owned()
}

fn flag(value: Option<&String>, default: bool) -> bool {
    match value {
        None => default,
        Some(v) => matches!(v.to_lowercase().as_str(), "1" | "true" | "yes" | "on"),
    }
}

fn json_response(value: &J, status: StatusCode) -> Response<Body> {
    let body = value.dumps(false).into_bytes();
    Response::builder()
        .status(status)
        .header(header::CONTENT_TYPE, "application/json; charset=utf-8")
        .header(header::CACHE_CONTROL, "no-store")
        .body(Body::from(body))
        .unwrap()
}

fn error_response(status: StatusCode, message: &str) -> Response<Body> {
    json_response(
        &J::obj(vec![(
            "error",
            J::obj(vec![
                ("status", J::Int(status.as_u16().into())),
                ("message", J::s(message)),
            ]),
        )]),
        status,
    )
}

fn not_found() -> Response<Body> {
    error_response(StatusCode::NOT_FOUND, "not found")
}

fn into_response(result: Result<Response<Body>>) -> Response<Body> {
    match result {
        Ok(r) => r,
        Err(e) => {
            let status = if e.downcast_ref::<DataError>().is_some() {
                StatusCode::BAD_REQUEST
            } else {
                StatusCode::INTERNAL_SERVER_ERROR
            };
            error_response(status, &e.to_string())
        }
    }
}

// ------------------------------------------------------------ dispatch

async fn dispatch(
    axum::extract::State(st): axum::extract::State<Arc<AppState>>,
    req: Request<Body>,
) -> Response<Body> {
    let path = req.uri().path().to_string();
    let query = req.uri().query().unwrap_or("").to_string();
    match *req.method() {
        Method::GET if path.starts_with("/api/v1/") => {
            if let Some(result) = get_api(&st, &path, &parse_qs(&query)).await {
                return into_response(result);
            }
        }
        Method::POST if path == "/api/v1/sync" || path == "/api/v1/trading-sessions/sync" => {
            return into_response(post_sync(&st, &path, req).await);
        }
        _ => {}
    }
    proxy(&st, req).await
}

/// `None` when the path is not handled here (so it goes to Python).
async fn get_api(
    st: &AppState,
    path: &str,
    q: &HashMap<String, String>,
) -> Option<Result<Response<Body>>> {
    let value = |k: &str| q.get(k).map(String::as_str);
    let as_of = value("asOf");
    let options = flag(q.get("includeOptions"), true);
    let db = &st.db;
    let ok = |j: J| Ok(json_response(&j, StatusCode::OK));
    Some(match path {
        "/api/v1/health" => ok(J::obj(vec![
            ("status", J::s("ok")),
            ("sourceType", J::s(SOURCE_TYPE)),
        ])),
        "/api/v1/schema" => ok(schema_json()),
        "/api/v1/data-state" => db.data_state(as_of).await.and_then(ok),
        "/api/v1/trading-sessions" => {
            match services::continuous_sessions(db, as_of, value("venue"), value("product")).await {
                Ok((state, rows)) => ok(J::obj(vec![
                    ("dataState", state),
                    ("timezone", J::s("Asia/Shanghai")),
                    ("sessions", J::Arr(rows)),
                ])),
                Err(e) => Err(e),
            }
        }
        "/api/v1/overview" => services::overview(
            db,
            as_of,
            options,
            value("venue"),
            value("product"),
            value("search"),
        )
        .await
        .and_then(ok),
        "/api/v1/exports/exchange-info" => {
            services::export_document(db, as_of, value("venue"), value("product"), options)
                .await
                .and_then(ok)
        }
        "/api/v1/exports/records.csv" => {
            match services::records(db, as_of, value("venue"), value("product"), None, options)
                .await
            {
                Ok((_, rows)) => {
                    let mut body = vec![0xEF, 0xBB, 0xBF];
                    body.extend_from_slice(services::records_csv(&rows).as_bytes());
                    Ok(Response::builder()
                        .status(StatusCode::OK)
                        .header(header::CONTENT_TYPE, "text/csv; charset=utf-8")
                        .header(
                            header::CONTENT_DISPOSITION,
                            "attachment; filename=\"cn_futures_records.csv\"",
                        )
                        .body(Body::from(body))
                        .unwrap())
                }
                Err(e) => Err(e),
            }
        }
        p if p.starts_with("/api/v1/exchanges/") => {
            let venue = unquote(p.rsplit('/').next().unwrap_or(""));
            services::overview(
                db,
                as_of,
                options,
                Some(&venue),
                value("product"),
                value("search"),
            )
            .await
            .and_then(ok)
        }
        p if p.starts_with("/api/v1/instruments/") => {
            let parts: Vec<&str> = p.split('/').collect();
            if parts.len() != 6 {
                return Some(Ok(not_found()));
            }
            match services::one_record(db, as_of, &unquote(parts[4]), &unquote(parts[5])).await {
                Ok((state, record)) => ok(J::obj(vec![
                    ("dataState", state),
                    ("instrument", services::instrument_json(&record)),
                    ("exchangeInfo", services::exchange_info(&record)),
                ])),
                Err(e) => Err(e),
            }
        }
        _ => return None,
    })
}

fn schema_json() -> J {
    let endpoints = [
        "GET /api/v1/overview",
        "GET /api/v1/data-state",
        "GET /api/v1/exchanges/{venue}",
        "GET /api/v1/instruments/{venue}/{instrument}",
        "GET /api/v1/exports/exchange-info",
        "GET /api/v1/exports/records.csv",
        "POST /api/v1/sync",
        "GET /api/v1/trading-sessions",
        "POST /api/v1/trading-sessions/sync",
        "POST /api/v1/import",
        "POST /api/v1/overrides/import",
        "POST /api/v1/fees/estimate",
        "POST /api/v1/fees/round-trip",
        "GET /api/v1/announcements",
        "POST /api/v1/announcements/refresh",
        "GET /api/v1/announcements/{id}/raw",
        "GET /api/v1/announcement-assets/{id}",
        "GET /api/v1/message-fee-rules",
        "POST /api/v1/message-fee-rules/{id}/review",
        "POST /api/v1/message-fee-rules/import",
        "POST /api/v1/message-fee/calculate",
    ];
    J::obj(vec![
        ("schema", J::s("cn-futures-reference-api/v1")),
        ("storage", J::s("incremental-instrument-versions")),
        (
            "endpoints",
            J::Arr(endpoints.iter().map(|e| J::s(*e)).collect()),
        ),
    ])
}

// ------------------------------------------------------------- POST sync

/// `int(body.get(key, default))` with Python's coercions.
fn body_int(body: &serde_json::Value, key: &str, default: i64) -> Result<i64> {
    match body.get(key) {
        None => Ok(default),
        Some(serde_json::Value::Number(n)) => Ok(n
            .as_i64()
            .or_else(|| n.as_f64().map(|f| f as i64))
            .unwrap_or(default)),
        Some(serde_json::Value::String(s)) => s.trim().parse().map_err(|_| {
            anyhow::anyhow!(
                "invalid literal for int() with base 10: {}",
                crate::json::py_repr(s)
            )
        }),
        Some(serde_json::Value::Bool(b)) => Ok(*b as i64),
        Some(other) => Err(anyhow::anyhow!(
            "int() argument must be a string or a real number, not '{}'",
            match other {
                serde_json::Value::Null => "NoneType",
                serde_json::Value::Array(_) => "list",
                _ => "dict",
            }
        )),
    }
}

async fn post_sync(st: &AppState, path: &str, req: Request<Body>) -> Result<Response<Body>> {
    let bytes = axum::body::to_bytes(req.into_body(), MAX_REQUEST_BYTES + 1)
        .await
        .map_err(|_| DataError("request body is too large".into()))?;
    if bytes.len() > MAX_REQUEST_BYTES {
        return Err(DataError("request body is too large".into()).into());
    }
    let body: serde_json::Value = if bytes.is_empty() {
        serde_json::json!({})
    } else {
        serde_json::from_slice(&bytes)
            .map_err(|_| DataError("request body must be UTF-8 JSON".into()))?
    };
    if !body.is_object() {
        anyhow::bail!("request body must be a JSON object");
    }
    let text = |k: &str| {
        body.get(k)
            .and_then(|v| v.as_str())
            .filter(|s| !s.is_empty())
    };
    let outcome = if path == "/api/v1/sync" {
        services::sync(
            &st.db,
            text("sourceUrl"),
            text("sourceDate"),
            body_int(&body, "timeout", 30)?.max(0) as u64,
            body_int(&body, "retries", 2)?.max(0) as u32,
        )
        .await?
    } else {
        services::sync_continuous_sessions(
            &st.db,
            text("sourceUrl"),
            text("sourceDate"),
            body_int(&body, "timeout", 30)?.clamp(1, 120) as u64,
            body_int(&body, "retries", 2)?.clamp(0, 5) as u32,
        )
        .await?
    };
    let status = if outcome.inserted {
        StatusCode::CREATED
    } else {
        StatusCode::OK
    };
    Ok(json_response(&outcome.body, status))
}

// ---------------------------------------------------------------- proxy

const HOP_BY_HOP: [&str; 8] = [
    "connection",
    "keep-alive",
    "proxy-authenticate",
    "proxy-authorization",
    "te",
    "trailers",
    "transfer-encoding",
    "upgrade",
];

async fn proxy(st: &AppState, req: Request<Body>) -> Response<Body> {
    let (parts, body) = req.into_parts();
    let bytes = match axum::body::to_bytes(body, MAX_REQUEST_BYTES * 2).await {
        Ok(b) => b,
        Err(_) => return error_response(StatusCode::BAD_REQUEST, "request body is too large"),
    };
    let target = format!(
        "{}{}",
        st.fallback,
        parts.uri.path_and_query().map_or("/", |p| p.as_str())
    );
    let mut headers = reqwest::header::HeaderMap::new();
    for (name, value) in &parts.headers {
        let lower = name.as_str();
        if lower == "host"
            || lower == "content-length"
            || lower == "accept-encoding"
            || HOP_BY_HOP.contains(&lower)
        {
            continue;
        }
        if let (Ok(n), Ok(v)) = (
            reqwest::header::HeaderName::from_bytes(name.as_str().as_bytes()),
            reqwest::header::HeaderValue::from_bytes(value.as_bytes()),
        ) {
            headers.append(n, v);
        }
    }
    let method = reqwest::Method::from_bytes(parts.method.as_str().as_bytes())
        .unwrap_or(reqwest::Method::GET);
    let resp = match st
        .http
        .request(method, &target)
        .headers(headers)
        .body(bytes)
        .send()
        .await
    {
        Ok(r) => r,
        Err(e) => {
            return error_response(
                StatusCode::BAD_GATEWAY,
                &format!("reference fallback unavailable: {e}"),
            )
        }
    };
    let status = StatusCode::from_u16(resp.status().as_u16()).unwrap_or(StatusCode::BAD_GATEWAY);
    let mut out_headers = HeaderMap::new();
    for (name, value) in resp.headers() {
        let lower = name.as_str();
        if lower == "content-length"
            || lower == "date"
            || lower == "server"
            || HOP_BY_HOP.contains(&lower)
        {
            continue;
        }
        if let (Ok(n), Ok(v)) = (
            HeaderName::from_bytes(name.as_str().as_bytes()),
            HeaderValue::from_bytes(value.as_bytes()),
        ) {
            out_headers.append(n, v);
        }
    }
    match resp.bytes().await {
        Ok(b) => {
            let mut r = Response::new(Body::from(b));
            *r.status_mut() = status;
            *r.headers_mut() = out_headers;
            r
        }
        Err(e) => error_response(
            StatusCode::BAD_GATEWAY,
            &format!("reference fallback read failed: {e}"),
        ),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn query_parsing_matches_parse_qs() {
        let q = parse_qs("a=1&b=&c=x%20y&a=2&d=p+q&e");
        assert_eq!(q.get("a").map(String::as_str), Some("1"));
        assert!(!q.contains_key("b") && !q.contains_key("e"));
        assert_eq!(q.get("c").map(String::as_str), Some("x y"));
        assert_eq!(q.get("d").map(String::as_str), Some("p q"));
    }

    #[test]
    fn flags() {
        assert!(flag(None, true));
        assert!(!flag(Some(&"0".to_string()), true));
        assert!(flag(Some(&"YES".to_string()), false));
    }
}
