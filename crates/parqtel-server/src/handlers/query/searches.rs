//! Saved-search CRUD endpoints.

use super::common::map_error;
use crate::state::AppState;
use axum::{
    extract::State,
    http::StatusCode,
    response::{IntoResponse, Response},
    Json,
};
use serde_json::json;

pub async fn list_saved_searches(State(state): State<AppState>) -> Response {
    let searches = state.inner.saved_searches.list().await;
    (
        StatusCode::OK,
        Json(json!({ "status": "success", "data": searches })),
    )
        .into_response()
}

pub async fn create_saved_search(
    State(state): State<AppState>,
    axum::Json(search): axum::Json<serde_json::Value>,
) -> Response {
    let parsed: Result<crate::saved_searches::SavedSearch, _> = serde_json::from_value(search);
    match parsed {
        Ok(s) => match state.inner.saved_searches.create(s).await {
            Ok(created) => (
                StatusCode::OK,
                Json(json!({ "status": "success", "data": created })),
            )
                .into_response(),
            Err(e) => map_error(e),
        },
        Err(e) => (
            StatusCode::BAD_REQUEST,
            Json(json!({ "status": "error", "error": e.to_string() })),
        )
            .into_response(),
    }
}

pub async fn delete_saved_search(
    State(state): State<AppState>,
    axum::extract::Path(id): axum::extract::Path<String>,
) -> Response {
    if state.inner.saved_searches.delete(&id).await {
        (StatusCode::OK, Json(json!({ "status": "success" }))).into_response()
    } else {
        (
            StatusCode::NOT_FOUND,
            Json(json!({ "status": "error", "error": "saved search not found" })),
        )
            .into_response()
    }
}
