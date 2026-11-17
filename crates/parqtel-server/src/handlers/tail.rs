//! Server-sent-events live log tail over the in-memory buffer.
//!
//! Polls the buffer rather than introducing a second publication path; the
//! buffer already holds exactly the rows that arrived since the last flush.

use crate::state::AppState;
use async_stream::stream;
use axum::{
    extract::{Query, State},
    response::sse::{Event, KeepAlive, Sse},
};
use futures::Stream;
use serde::Deserialize;
use std::convert::Infallible;
use std::time::Duration;

#[derive(Debug, Deserialize)]
pub struct TailParams {
    pub service_name: Option<String>,
    pub body_contains: Option<String>,
    pub limit: Option<usize>,
}

pub async fn tail_logs(
    State(state): State<AppState>,
    Query(params): Query<TailParams>,
) -> Sse<impl Stream<Item = Result<Event, Infallible>>> {
    let limit = params.limit.unwrap_or(100).clamp(1, 1000);
    let stream = stream! {
        let mut last_ns = chrono::Utc::now().timestamp_nanos_opt().unwrap_or(0);
        loop {
            tokio::time::sleep(Duration::from_millis(250)).await;
            let now_ns = chrono::Utc::now().timestamp_nanos_opt().unwrap_or(last_ns);
            let logs = state
                .inner
                .query_executor
                .memory_buffer()
                .scan_logs(last_ns.saturating_add(1), now_ns)
                .await;
            last_ns = now_ns;
            let mut sent = 0usize;
            for log in logs {
                if sent >= limit {
                    break;
                }
                if let Some(service) = &params.service_name {
                    let actual = log
                        .resource_attributes
                        .get("service.name")
                        .or_else(|| log.attributes.get("service.name"))
                        .unwrap_or("");
                    if actual != service {
                        continue;
                    }
                }
                if let Some(needle) = &params.body_contains {
                    if !log.body.contains(needle) {
                        continue;
                    }
                }
                if let Ok(json) = serde_json::to_string(&log) {
                    sent += 1;
                    yield Ok(Event::default().event("log").data(json));
                }
            }
        }
    };
    Sse::new(stream).keep_alive(KeepAlive::default())
}
