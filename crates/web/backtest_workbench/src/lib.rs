use axum::{
    extract::{Path, Query, State},
    http::StatusCode,
    response::{Html, IntoResponse, Response},
    routing::{get, post},
    Json, Router,
};
use cn_futures_backtest::{store::Store, Config};
use serde::Deserialize;
use serde_json::{json, Value};
use std::{fs::File, sync::Arc};

pub struct AppState {
    pub template: Config,
    pub store: Store,
}
impl AppState {
    pub fn new(config: &std::path::Path) -> anyhow::Result<Arc<Self>> {
        let template = Config::load(config)?;
        let store = Store::new(template.output_root.clone())?;
        Ok(Arc::new(Self { template, store }))
    }
}
type Api = Result<Json<Value>, ApiError>;
struct ApiError(anyhow::Error);
impl<E: Into<anyhow::Error>> From<E> for ApiError {
    fn from(e: E) -> Self {
        Self(e.into())
    }
}
impl IntoResponse for ApiError {
    fn into_response(self) -> Response {
        (
            StatusCode::BAD_REQUEST,
            Json(json!({"error":format!("{:#}",self.0)})),
        )
            .into_response()
    }
}
pub fn router(state: Arc<AppState>) -> Router {
    Router::new()
        .route(
            "/backtest",
            get(|| async { Html(include_str!("index.html")) }),
        )
        .route(
            "/backtest/",
            get(|| async { Html(include_str!("index.html")) }),
        )
        .route("/api/backtest/config", get(config))
        .route("/api/backtest/inspect", post(inspect))
        .route("/api/backtest/runs", get(list).post(submit))
        .route("/api/backtest/runs/{id}", get(detail))
        .route("/api/backtest/runs/{id}/cancel", post(cancel))
        .route("/api/backtest/runs/{id}/tables/{table}", get(table))
        .with_state(state)
}
async fn config(State(s): State<Arc<AppState>>) -> Api {
    Ok(Json(serde_json::to_value(&s.template)?))
}
async fn list(State(s): State<Arc<AppState>>) -> Api {
    Ok(Json(serde_json::to_value(s.store.list(200)?)?))
}
async fn submit(State(s): State<Arc<AppState>>, Json(mut cfg): Json<Config>) -> Api {
    // The workspace owns destinations. A request cannot redirect task-state writes.
    cfg.output_root = s.template.output_root.clone();
    Ok(Json(serde_json::to_value(s.store.submit(&cfg)?)?))
}
async fn detail(State(s): State<Arc<AppState>>, Path(id): Path<String>) -> Api {
    let dir = s.store.path(&id)?;
    let status = s.store.status(&id)?;
    let cfg: Value = serde_json::from_reader(File::open(dir.join("config.json"))?)?;
    let manifest: Option<Value> = File::open(dir.join("manifest.json"))
        .ok()
        .map(serde_json::from_reader)
        .transpose()?;
    Ok(Json(
        json!({"status":status,"config":cfg,"manifest":manifest}),
    ))
}
async fn cancel(State(s): State<Arc<AppState>>, Path(id): Path<String>) -> Api {
    Ok(Json(serde_json::to_value(s.store.cancel(&id)?)?))
}
#[derive(Deserialize)]
struct Inspect {
    path: std::path::PathBuf,
}
async fn inspect(Json(request): Json<Inspect>) -> Api {
    let r = tokio::task::spawn_blocking(move || cn_futures_backtest::input::inspect(&request.path))
        .await??;
    Ok(Json(r))
}
#[derive(Deserialize)]
struct Page {
    #[serde(default)]
    offset: usize,
    #[serde(default = "limit")]
    limit: usize,
    sample: Option<usize>,
}
fn limit() -> usize {
    100
}
async fn table(
    State(s): State<Arc<AppState>>,
    Path((id, table)): Path<(String, String)>,
    Query(p): Query<Page>,
) -> Api {
    if !cn_futures_backtest::output::TABLES.contains(&table.as_str()) {
        return Err(anyhow::anyhow!("unknown audit table").into());
    }
    let path = s.store.path(&id)?.join(format!("{table}.parquet"));
    Ok(Json(
        tokio::task::spawn_blocking(move || read_table(&path, p)).await??,
    ))
}
fn read_table(path: &std::path::Path, p: Page) -> anyhow::Result<Value> {
    use arrow_array::{Array, Float64Array, Int64Array, StringArray, UInt64Array};
    use parquet::arrow::arrow_reader::ParquetRecordBatchReaderBuilder;
    let b = ParquetRecordBatchReaderBuilder::try_new(File::open(path)?)?;
    let total = b.metadata().file_metadata().num_rows() as usize;
    let limit = p.sample.unwrap_or(p.limit).clamp(1, 2000);
    let stride = if p.sample.is_some() {
        total.div_ceil(limit).max(1)
    } else {
        1
    };
    let mut rows = vec![];
    let mut at = 0;
    for batch in b.with_batch_size(1024).build()? {
        let batch = batch?;
        for i in 0..batch.num_rows() {
            let index = at;
            at += 1;
            if index < p.offset || !(index - p.offset).is_multiple_of(stride) {
                continue;
            }
            let mut row = serde_json::Map::new();
            for (f, a) in batch.schema().fields().iter().zip(batch.columns()) {
                let value = if a.is_null(i) {
                    Value::Null
                } else if let Some(a) = a.as_any().downcast_ref::<StringArray>() {
                    json!(a.value(i))
                } else if let Some(a) = a.as_any().downcast_ref::<Int64Array>() {
                    json!(a.value(i))
                } else if let Some(a) = a.as_any().downcast_ref::<UInt64Array>() {
                    json!(a.value(i))
                } else if let Some(a) = a.as_any().downcast_ref::<Float64Array>() {
                    json!(a.value(i))
                } else {
                    Value::Null
                };
                row.insert(f.name().clone(), value);
            }
            rows.push(Value::Object(row));
            if rows.len() == limit {
                return Ok(
                    json!({"total":total,"offset":p.offset,"sample_stride":stride,"rows":rows}),
                );
            }
        }
    }
    Ok(json!({"total":total,"offset":p.offset,"sample_stride":stride,"rows":rows}))
}
