//! Prometheus response envelope and shared query-parameter parsing.

use axum::{
    http::StatusCode,
    response::{IntoResponse, Response},
    Json,
};
use serde::Serialize;
use serde_json::json;

#[derive(Serialize)]
pub struct PrometheusResponse<T> {
    pub status: String,
    pub data: T,
}

#[derive(Serialize)]
#[serde(rename_all = "camelCase")]
pub struct PrometheusData {
    pub result_type: String,
    pub result: Vec<PrometheusResult>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub total_series_count: Option<usize>,
    pub volume_summary: Vec<u64>,
}

#[derive(Serialize)]
pub struct PrometheusResult {
    pub metric: serde_json::Value,
    /// Instant-vector sample: `[timestamp, "value"]` — the Prometheus API
    /// shape for resultType=vector. Present when the series carries exactly
    /// one sample (instant queries).
    #[serde(skip_serializing_if = "Option::is_none")]
    pub value: Option<(f64, String)>,
    /// Matrix sample list — the Prometheus API shape for resultType=matrix
    /// (range queries). Also populated for instant results so the embedded
    /// UI (which charts `values` from either query kind) keeps working.
    pub values: Vec<(f64, String)>,
}

pub(super) fn map_error(e: parqtel_core::Error) -> Response {
    let status = match &e {
        parqtel_core::Error::Validation(_) => StatusCode::BAD_REQUEST,
        parqtel_core::Error::InvalidOtlp(_) => StatusCode::BAD_REQUEST,
        _ => StatusCode::INTERNAL_SERVER_ERROR,
    };

    (
        status,
        Json(json!({
            "status": "error",
            "errorType": "bad_data",
            "error": e.to_string()
        })),
    )
        .into_response()
}

/// Parse a `step` query parameter per the Prometheus HTTP API: a duration
/// string (e.g. `1m`, `30s`, `1h30m`) or a float number of seconds
/// (e.g. `15`, `0.5`). Grafana's Prometheus datasource sends plain float
/// seconds for range panels, so rejecting them breaks every Grafana range
/// query. Rejects non-positive values: the range evaluator advances sample
/// timestamps with `ts += step_ns`, so `0` would loop forever.
pub(super) fn parse_step_duration(s: &str) -> Result<i64, ()> {
    let s = s.trim();
    if s.is_empty() {
        return Err(());
    }
    // Float seconds first (spec: "duration format or float seconds").
    if let Ok(secs) = s.parse::<f64>() {
        let ns = (secs * 1_000_000_000.0) as i64;
        if secs.is_finite() && ns > 0 {
            return Ok(ns);
        }
        return Err(());
    }
    // Duration units — parse_duration itself accepts negative values
    // (e.g. "-1m"), which would loop the range evaluator forever.
    match parse_duration(s) {
        Ok(ns) if ns > 0 => Ok(ns),
        _ => Err(()),
    }
}

fn parse_duration(s: &str) -> Result<i64, ()> {
    if s.is_empty() {
        return Err(());
    }
    let val: f64 = s[..s.len() - 1].parse().map_err(|_| ())?;
    let unit = &s[s.len() - 1..];
    let multiplier: i64 = match unit {
        "s" => 1_000_000_000,
        "m" => 60 * 1_000_000_000,
        "h" => 3600 * 1_000_000_000,
        "d" => 24 * 3600 * 1_000_000_000,
        _ => return Err(()),
    };
    Ok((val * multiplier as f64) as i64)
}

#[cfg(test)]
mod step_tests {
    #![allow(clippy::unwrap_used, clippy::expect_used, clippy::panic)]
    use super::parse_step_duration;

    #[test]
    fn accepts_float_seconds() {
        assert_eq!(parse_step_duration("60").unwrap(), 60_000_000_000);
        assert_eq!(parse_step_duration("15").unwrap(), 15_000_000_000);
        assert_eq!(parse_step_duration("0.5").unwrap(), 500_000_000);
    }

    #[test]
    fn accepts_duration_units() {
        assert_eq!(parse_step_duration("60s").unwrap(), 60_000_000_000);
        assert_eq!(parse_step_duration("5m").unwrap(), 300_000_000_000);
        assert_eq!(parse_step_duration("1h").unwrap(), 3_600_000_000_000);
        assert_eq!(parse_step_duration("1d").unwrap(), 86_400_000_000_000);
        assert_eq!(parse_step_duration("30m").unwrap(), 1_800_000_000_000);
    }

    #[test]
    fn rejects_invalid_and_dangerous_values() {
        // Zero / negative would make the range evaluator loop forever (ts += step_ns).
        assert!(parse_step_duration("0").is_err());
        assert!(parse_step_duration("-15").is_err());
        assert!(parse_step_duration("-1m").is_err());
        // Garbage and empty.
        assert!(parse_step_duration("").is_err());
        assert!(parse_step_duration("abc").is_err());
        assert!(parse_step_duration("1x").is_err());
        // NaN / Infinity must not reach the evaluator either.
        assert!(parse_step_duration("NaN").is_err());
        assert!(parse_step_duration("inf").is_err());
        // Sub-nanosecond steps that truncate to 0 ns are rejected too.
        assert!(parse_step_duration("1e-12").is_err());
    }

    #[test]
    fn trims_whitespace() {
        assert_eq!(parse_step_duration(" 60s ").unwrap(), 60_000_000_000);
        assert_eq!(parse_step_duration(" 60 ").unwrap(), 60_000_000_000);
    }
}
