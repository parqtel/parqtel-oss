//! Pipeline query endpoint (`fetch ... | filter ... | stats ...`).

use super::common::map_error;
use crate::state::AppState;
use axum::{
    extract::State,
    http::StatusCode,
    response::{IntoResponse, Response},
    Json,
};
use serde::Deserialize;
use serde_json::json;

/// POST /v1/search — executes a ParqtelQL pipeline:
/// `fetch logs | filter <pql> | parse ".." as f | stats ... | correlate ...`
#[derive(Deserialize)]
pub struct PipelineRequest {
    pub query: String,
    pub start: f64,
    pub end: f64,
}

pub async fn run_pipeline_search(
    State(state): State<AppState>,
    axum::Json(req): axum::Json<PipelineRequest>,
) -> Response {
    let pipeline = match parqtel_query::pipeline::parse_pipeline(&req.query) {
        Ok(p) => p,
        Err(e) => {
            return (
                StatusCode::BAD_REQUEST,
                Json(json!({"status": "error", "error": e.to_string()})),
            )
                .into_response()
        }
    };
    let start_ns = (req.start * 1_000_000_000.0) as i64;
    let end_ns = (req.end * 1_000_000_000.0) as i64;

    match state
        .inner
        .query_executor
        .execute_pipeline(&pipeline, start_ns, end_ns)
        .await
    {
        Ok(result) => {
            let body = match result {
                parqtel_query::pipeline_exec::PipelineResult::Rows(rows) => json!({
                    "type": "rows",
                    "rows": rows
                        .iter()
                        .map(|r| serde_json::Value::Object(
                            r.fields.iter()
                                .map(|(k, v)| (k.clone(), v.clone()))
                                .collect()
                        ))
                        .collect::<Vec<_>>(),
                }),
                parqtel_query::pipeline_exec::PipelineResult::Table {
                    columns,
                    rows,
                    timeseries,
                } => {
                    let mut obj = json!({
                        "type": "table",
                        "columns": columns,
                        "rows": rows,
                    });
                    if let Some(ts) = timeseries {
                        if let Some(o) = obj.as_object_mut() {
                            o.insert(
                                "timeseries".into(),
                                json!({
                                    "timestamps": ts.timestamps,
                                    "series": ts.series.iter().map(|(fields, values)| json!({
                                        "fields": fields,
                                        "values": values,
                                    })).collect::<Vec<_>>(),
                                }),
                            );
                        }
                    }
                    obj
                }
            };
            (
                StatusCode::OK,
                Json(json!({ "status": "success", "data": body })),
            )
                .into_response()
        }
        Err(e) => map_error(e),
    }
}
