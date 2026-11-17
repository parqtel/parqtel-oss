//! Prometheus remote-write receiver.
//!
//! One route (`POST /api/v1/write`) accepts the standard snappy-compressed
//! protobuf `WriteRequest`. Only samples are consumed; metadata is ignored.

use crate::state::AppState;
use axum::{
    body::Bytes,
    extract::State,
    http::{header, StatusCode},
    response::{IntoResponse, Response},
    Json,
};
use parqtel_core::{DataPoint, LabelSet, Metric, MetricKind, MetricValue};
use prost::Message;
use serde_json::json;

#[derive(Clone, PartialEq, Message)]
pub struct WriteRequest {
    #[prost(message, repeated, tag = "1")]
    pub timeseries: Vec<TimeSeries>,
}

#[derive(Clone, PartialEq, Message)]
pub struct TimeSeries {
    #[prost(message, repeated, tag = "1")]
    pub labels: Vec<Label>,
    #[prost(message, repeated, tag = "2")]
    pub samples: Vec<Sample>,
}

#[derive(Clone, PartialEq, Message)]
pub struct Label {
    #[prost(string, tag = "1")]
    pub name: String,
    #[prost(string, tag = "2")]
    pub value: String,
}

#[derive(Clone, PartialEq, Message)]
pub struct Sample {
    #[prost(double, tag = "1")]
    pub value: f64,
    #[prost(int64, tag = "2")]
    pub timestamp: i64,
}

pub async fn remote_write(
    State(state): State<AppState>,
    headers: axum::http::HeaderMap,
    body: Bytes,
) -> Response {
    let encoding = headers
        .get(header::CONTENT_ENCODING)
        .and_then(|value| value.to_str().ok())
        .unwrap_or("snappy");
    if !encoding.eq_ignore_ascii_case("snappy") {
        return (
            StatusCode::UNSUPPORTED_MEDIA_TYPE,
            Json(json!({"status": "error", "error": "Content-Encoding must be snappy"})),
        )
            .into_response();
    }
    let decoded = match snap::raw::Decoder::new().decompress_vec(&body) {
        Ok(decoded) => decoded,
        Err(e) => {
            return (
                StatusCode::BAD_REQUEST,
                Json(json!({"status": "error", "error": format!("snappy decode failed: {e}")})),
            )
                .into_response()
        }
    };
    let request = match WriteRequest::decode(decoded.as_slice()) {
        Ok(request) => request,
        Err(e) => {
            return (
                StatusCode::BAD_REQUEST,
                Json(json!({"status": "error", "error": format!("protobuf decode failed: {e}")})),
            )
                .into_response()
        }
    };

    let mut metrics = Vec::with_capacity(request.timeseries.len());
    let mut points = 0u64;
    for series in request.timeseries {
        let mut name = None;
        let mut labels = Vec::with_capacity(series.labels.len());
        for label in series.labels {
            if label.name == "__name__" {
                name = Some(label.value);
            } else {
                labels.push((label.name, label.value));
            }
        }
        let Some(name) = name else {
            continue;
        };
        let labels = match LabelSet::try_from_iter(labels) {
            Ok(labels) => labels,
            Err(e) => {
                return (
                    StatusCode::BAD_REQUEST,
                    Json(json!({"status": "error", "error": e.to_string()})),
                )
                    .into_response()
            }
        };
        let data_points: Vec<DataPoint> = series
            .samples
            .into_iter()
            .map(|sample| DataPoint {
                timestamp_ns: sample.timestamp.saturating_mul(1_000_000),
                value: MetricValue::Double(sample.value),
                labels: labels.clone(),
            })
            .collect();
        points += data_points.len() as u64;
        if !data_points.is_empty() {
            metrics.push(Metric {
                name,
                description: String::new(),
                unit: String::new(),
                kind: MetricKind::Gauge,
                resource_attributes: LabelSet::default(),
                data_points,
            });
        }
    }

    match state.inner.ingestion_service.ingest_metrics(metrics).await {
        Ok(_) => (StatusCode::OK, Json(json!({"ingested": points}))).into_response(),
        Err(e) => (
            StatusCode::BAD_REQUEST,
            Json(json!({"status": "error", "error": e.to_string()})),
        )
            .into_response(),
    }
}

#[cfg(test)]
mod tests {
    #![allow(clippy::unwrap_used, clippy::expect_used, clippy::panic)]
    use super::*;

    #[test]
    fn write_request_round_trips_through_snappy() {
        let request = WriteRequest {
            timeseries: vec![TimeSeries {
                labels: vec![
                    Label {
                        name: "__name__".into(),
                        value: "http_requests_total".into(),
                    },
                    Label {
                        name: "job".into(),
                        value: "api".into(),
                    },
                ],
                samples: vec![Sample {
                    value: 7.0,
                    timestamp: 1_700_000_000_000,
                }],
            }],
        };
        let encoded = request.encode_to_vec();
        let compressed = snap::raw::Encoder::new().compress_vec(&encoded).unwrap();
        let decoded = WriteRequest::decode(
            snap::raw::Decoder::new()
                .decompress_vec(&compressed)
                .unwrap()
                .as_slice(),
        )
        .unwrap();
        assert_eq!(decoded.timeseries[0].labels[0].value, "http_requests_total");
        assert_eq!(decoded.timeseries[0].samples[0].value, 7.0);
    }
}
