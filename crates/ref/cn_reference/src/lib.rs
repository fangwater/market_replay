//! CN futures reference data: OpenCTP fee / margin / tick table and
//! continuous-session sync into an incremental PostgreSQL store, plus the
//! `/api/v1` HTTP API (see `docs/market_replay_server.md`).
pub mod account_rates;
pub mod api;
pub mod dec;
pub mod json;
pub mod models;
pub mod services;
pub mod sessions;
pub mod source;
pub mod storage;

use anyhow::{Context, Result};
use std::path::Path;

pub const DATABASE_URL_ENV: &str = "CN_FUTURES_REF_DATABASE_URL";

/// The database URL from the environment, else from `KEY=VALUE` lines of
/// `env_file` (the Python service's `.env`). The value is never logged.
pub fn database_url(env_file: Option<&Path>) -> Result<String> {
    if let Ok(v) = std::env::var(DATABASE_URL_ENV) {
        if !v.trim().is_empty() {
            return Ok(v);
        }
    }
    let file = env_file.with_context(|| format!("{DATABASE_URL_ENV} is required"))?;
    let text = std::fs::read_to_string(file).with_context(|| format!("read {}", file.display()))?;
    for line in text.lines() {
        let line = line.trim();
        if let Some(rest) = line.strip_prefix(DATABASE_URL_ENV) {
            if let Some(v) = rest.trim_start().strip_prefix('=') {
                let v = v.trim().trim_matches(|c| c == '"' || c == '\'');
                if !v.is_empty() {
                    return Ok(v.to_string());
                }
            }
        }
    }
    anyhow::bail!("{DATABASE_URL_ENV} not found in {}", file.display())
}

/// Serves the API until `shutdown` resolves.
pub async fn serve(
    state: std::sync::Arc<api::AppState>,
    listen: &str,
    shutdown: impl std::future::Future<Output = ()> + Send + 'static,
) -> Result<()> {
    let listener = tokio::net::TcpListener::bind(listen)
        .await
        .with_context(|| format!("bind {listen}"))?;
    axum::serve(listener, api::router(state))
        .with_graceful_shutdown(shutdown)
        .await?;
    Ok(())
}
