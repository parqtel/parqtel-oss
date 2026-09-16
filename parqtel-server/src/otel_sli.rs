//! SLI instruments + self-profiling for Parqtel.
//!
//! Metrics are recorded through the global OTel meter (initialised in
//! [`crate::telemetry`]) and pushed over OTLP/gRPC by the periodic reader:
//!
//! * Golden signals — `parqtel_http_requests_total`,
//!   `parqtel_http_request_duration_seconds` (latency/error SLIs),
//! * Throughput — `parqtel_ingest_points_total`, `parqtel_ingest_batch_points`,
//! * Saturation — `parqtel_buffer_points{signal}`, `parqtel_process_rss_bytes`,
//!   `parqtel_process_rss_hwm_bytes` (recorded on every flush tick),
//! * Internals — `parqtel_flush_duration_seconds`, `parqtel_flush_total`,
//!   `parqtel_query_duration_seconds`.
//!
//! CPU profiling uses the `pprof` crate: a sampling guard runs for the
//! process lifetime (99 Hz, negligible overhead) and `/debug/pprof/*`
//! endpoints render on-demand reports.

use std::collections::HashMap;
use std::sync::OnceLock;
use std::time::{Duration, Instant};

use axum::{
    extract::{MatchedPath, Query, Request},
    http::{header, StatusCode},
    middleware::Next,
    response::{IntoResponse, Response},
    Json,
};
use opentelemetry::{
    global,
    metrics::{Counter, Gauge, Histogram},
    KeyValue,
};
use serde::Deserialize;

// ── Instruments ───────────────────────────────────────────────────────────────

pub struct Instruments {
    pub http_requests: Counter<u64>,
    pub http_duration: Histogram<f64>,
    pub ingest_points: Counter<u64>,
    pub ingest_batch_points: Histogram<u64>,
    pub flush_duration: Histogram<f64>,
    pub flush_total: Counter<u64>,
    pub flush_rows: Histogram<u64>,
    pub query_duration: Histogram<f64>,
    pub buffer_points: Gauge<u64>,
    pub buffer_logs: Gauge<u64>,
    pub buffer_spans: Gauge<u64>,
    pub process_rss: Gauge<u64>,
    pub process_rss_hwm: Gauge<u64>,
}

const LATENCY_BUCKETS: [f64; 13] = [
    0.001, 0.005, 0.010, 0.025, 0.050, 0.100, 0.250, 0.500, 1.0, 2.5, 5.0, 10.0, 30.0,
];

const BATCH_BUCKETS: [f64; 10] = [
    10.0, 50.0, 100.0, 250.0, 500.0, 1_000.0, 2_500.0, 5_000.0, 10_000.0, 50_000.0,
];

fn instruments() -> &'static Instruments {
    static INSTRUMENTS: OnceLock<Instruments> = OnceLock::new();
    INSTRUMENTS.get_or_init(|| {
        let meter = global::meter("parqtel");
        Instruments {
            http_requests: meter
                .u64_counter("parqtel_http_requests_total")
                .with_description("HTTP requests handled, by method/route/status")
                .with_unit("{request}")
                .build(),
            http_duration: meter
                .f64_histogram("parqtel_http_request_duration_seconds")
                .with_description("HTTP request latency (SLI: request latency)")
                .with_unit("s")
                .with_boundaries(LATENCY_BUCKETS.to_vec())
                .build(),
            ingest_points: meter
                .u64_counter("parqtel_ingest_points_total")
                .with_description("Telemetry points/records/spans accepted, by signal")
                .with_unit("{point}")
                .build(),
            ingest_batch_points: meter
                .u64_histogram("parqtel_ingest_batch_points")
                .with_description("Points per accepted ingest batch")
                .with_unit("{point}")
                .with_boundaries(BATCH_BUCKETS.to_vec())
                .build(),
            flush_duration: meter
                .f64_histogram("parqtel_flush_duration_seconds")
                .with_description("In-memory buffer flush (Parquet block write) duration")
                .with_unit("s")
                .with_boundaries(vec![0.01, 0.05, 0.1, 0.25, 0.5, 1.0, 2.5, 5.0, 10.0, 30.0])
                .build(),
            flush_total: meter
                .u64_counter("parqtel_flush_total")
                .with_description("Buffer flush attempts, by signal and result")
                .build(),
            flush_rows: meter
                .u64_histogram("parqtel_flush_rows")
                .with_description("Rows drained from the buffer into a Parquet block per flush")
                .with_unit("{row}")
                .with_boundaries(BATCH_BUCKETS.to_vec())
                .build(),
            query_duration: meter
                .f64_histogram("parqtel_query_duration_seconds")
                .with_description("Query execution latency (SLI: query latency)")
                .with_unit("s")
                .with_boundaries(LATENCY_BUCKETS.to_vec())
                .build(),
            buffer_points: meter
                .u64_gauge("parqtel_buffer_points")
                .with_description("Metric points currently held in the in-memory buffer")
                .with_unit("{point}")
                .build(),
            buffer_logs: meter
                .u64_gauge("parqtel_buffer_logs")
                .with_description("Log records currently held in the in-memory buffer")
                .with_unit("{record}")
                .build(),
            buffer_spans: meter
                .u64_gauge("parqtel_buffer_spans")
                .with_description("Spans currently held in the in-memory buffer")
                .with_unit("{span}")
                .build(),
            process_rss: meter
                .u64_gauge("parqtel_process_rss_bytes")
                .with_description("Process resident set size")
                .with_unit("By")
                .build(),
            process_rss_hwm: meter
                .u64_gauge("parqtel_process_rss_hwm_bytes")
                .with_description("Process resident set size high-water mark")
                .with_unit("By")
                .build(),
        }
    })
}

// ── Recording helpers ─────────────────────────────────────────────────────────

/// Record one handled HTTP request (called from the SLI middleware).
pub fn record_http(method: &str, route: &str, status: StatusCode, duration_secs: f64) {
    let i = instruments();
    let method_kv = KeyValue::new("method", method.to_string());
    let route_kv = KeyValue::new("route", route.to_string());
    let status_kv = KeyValue::new("status", status.as_u16().to_string());
    i.http_requests
        .add(1, &[method_kv.clone(), route_kv.clone(), status_kv.clone()]);
    i.http_duration
        .record(duration_secs, &[method_kv, route_kv, status_kv]);
}

/// Record an accepted ingest batch.
pub fn record_ingest(signal: &str, points: u64) {
    let i = instruments();
    let kv = KeyValue::new("signal", signal.to_string());
    i.ingest_points.add(points, std::slice::from_ref(&kv));
    i.ingest_batch_points
        .record(points, std::slice::from_ref(&kv));
}

/// Record a completed flush or a failed check. `Ok(false)` is an idle check,
/// not a successful flush, and must not dilute the flush latency histogram.
pub fn record_flush(signal: &str, duration_secs: f64, outcome: Result<bool, ()>) {
    let Some(ok) = flush_result(outcome) else {
        return;
    };
    let i = instruments();
    let kv = KeyValue::new("signal", signal.to_string());
    let result_kv = KeyValue::new("result", if ok { "ok" } else { "error" });
    i.flush_total.add(1, &[kv.clone(), result_kv]);
    if ok {
        i.flush_duration.record(duration_secs, &[kv]);
    }
}

fn flush_result(outcome: Result<bool, ()>) -> Option<bool> {
    match outcome {
        Ok(false) => None,
        Ok(true) => Some(true),
        Err(()) => Some(false),
    }
}

/// Record how many buffered items a flush drained into a Parquet block.
pub fn record_flush_rows(signal: &str, rows: u64) {
    if rows == 0 {
        return;
    }
    instruments()
        .flush_rows
        .record(rows, &[KeyValue::new("signal", signal.to_string())]);
}

/// Record a query execution.
pub fn record_query(route: &str, duration_secs: f64) {
    let i = instruments();
    i.query_duration
        .record(duration_secs, &[KeyValue::new("route", route.to_string())]);
}

/// Push saturation gauges (called from the 5 s flush tick).
pub fn record_gauges(metrics_points: u64, logs: u64, spans: u64) {
    let i = instruments();
    let (rss, hwm) = read_vm_rss_hwm();
    i.buffer_points.record(metrics_points, &[]);
    i.buffer_logs.record(logs, &[]);
    i.buffer_spans.record(spans, &[]);
    if let Some(rss) = rss {
        i.process_rss.record(rss, &[]);
    }
    if let Some(hwm) = hwm {
        i.process_rss_hwm.record(hwm, &[]);
    }
}

/// Parse VmRSS / VmHWM (bytes) from /proc/self/status.
pub fn read_vm_rss_hwm() -> (Option<u64>, Option<u64>) {
    let mut rss = None;
    let mut hwm = None;
    if let Ok(status) = std::fs::read_to_string("/proc/self/status") {
        for line in status.lines() {
            if let Some(v) = line.strip_prefix("VmRSS:") {
                rss = parse_kb_to_bytes(v);
            } else if let Some(v) = line.strip_prefix("VmHWM:") {
                hwm = parse_kb_to_bytes(v);
            }
        }
    }
    (rss, hwm)
}

fn parse_kb_to_bytes(field: &str) -> Option<u64> {
    field
        .split_whitespace()
        .next()?
        .parse::<u64>()
        .ok()
        .map(|kb| kb * 1024)
}

// ── HTTP SLI middleware ───────────────────────────────────────────────────────

/// Records the golden signals for every HTTP request: count by
/// method/route/status and a latency histogram. The route label uses axum's
/// *matched* path (`/api/v1/alerts/:id/resolve`) rather than the raw URI, so
/// cardinality stays bounded no matter how many distinct IDs are queried.
///
/// Read-path routes additionally feed the query-latency SLI so query
/// performance can be alerted on independently of ingest health.
pub async fn http_sli_middleware(request: Request, next: Next) -> Response {
    let method = request.method().clone();
    let route = request
        .extensions()
        .get::<MatchedPath>()
        .map(|p| p.as_str().to_string())
        .unwrap_or_else(|| "unmatched".to_string());
    let start = Instant::now();

    let response = next.run(request).await;

    let elapsed = start.elapsed().as_secs_f64();
    record_http(method.as_str(), &route, response.status(), elapsed);
    if is_query_route(&route) {
        record_query(&route, elapsed);
    }
    response
}

/// Whether a matched route is part of the read/query path, so its latency is
/// attributable to query SLIs rather than general request handling.
fn is_query_route(route: &str) -> bool {
    const QUERY_ROUTES: [&str; 9] = [
        "/api/v1/query",
        "/api/v1/query_range",
        "/api/v1/labels",
        "/api/v1/label/:name/values",
        "/api/v1/logs",
        "/v1/traces/search",
        "/v1/correlate",
        "/search",
        "/query",
    ];
    QUERY_ROUTES.contains(&route)
}

// ── CPU profiling ─────────────────────────────────────────────────────────────

/// Profiling settings, installed once at startup from `config.telemetry`.
static PROFILING: OnceLock<ProfilingConfig> = OnceLock::new();

#[derive(Debug, Clone, Copy)]
struct ProfilingConfig {
    enabled: bool,
    frequency: i32,
}

/// Install the profiling configuration (first call wins; safe to call once at
/// startup).
pub fn configure_profiling(enabled: bool, frequency: i32) {
    let _ = PROFILING.set(ProfilingConfig {
        enabled,
        frequency: clamp_frequency(frequency),
    });
}

/// Clamp a requested sampling frequency into the range `pprof` is useful at:
/// too low misses short hot paths, too high costs measurable CPU itself.
fn clamp_frequency(frequency: i32) -> i32 {
    frequency.clamp(10, 999)
}

/// Clamp a requested capture window into the supported range, so an oversized
/// `?seconds=` cannot pin a request (and the profiler) indefinitely.
fn clamp_window(seconds: u64) -> u64 {
    seconds.clamp(1, MAX_PROFILE_SECS)
}

fn profiling() -> ProfilingConfig {
    PROFILING.get().copied().unwrap_or(ProfilingConfig {
        enabled: false,
        frequency: 99,
    })
}

/// `pprof` drives a single process-global profiler (and a process-wide
/// `ITIMER_PROF`), so windowed captures must not overlap. Every report handler
/// serialises on this lock.
static PROFILE_LOCK: tokio::sync::Mutex<()> = tokio::sync::Mutex::const_new(());

/// Longest capture window honoured, to bound request/goroutine occupancy.
const MAX_PROFILE_SECS: u64 = 120;
const DEFAULT_PROFILE_SECS: u64 = 30;

#[derive(Debug, Deserialize)]
pub struct ProfileParams {
    /// Sampling window in seconds (default 30, clamped to 1..=120).
    seconds: Option<u64>,
    /// Max functions returned by the summary endpoint (default 25).
    top: Option<usize>,
}

/// Samples CPU for `seconds`, returning a report owned by the caller (the
/// sampling guard is dropped, and the profiler stopped, on return).
async fn capture(seconds: u64) -> Result<pprof::Report, (StatusCode, String)> {
    let cfg = profiling();
    if !cfg.enabled {
        return Err((
            StatusCode::NOT_FOUND,
            "profiling is disabled (telemetry.profiling_enabled = false)".to_string(),
        ));
    }
    let seconds = clamp_window(seconds);
    let permit = PROFILE_LOCK.try_lock().map_err(|_| {
        (
            StatusCode::TOO_MANY_REQUESTS,
            "a CPU capture is already running".to_string(),
        )
    })?;

    // Symbol resolution and sampling setup are blocking work. Move the permit
    // into the worker: cancellation of an HTTP request must not release it
    // while this process-global profiler is still running.
    tokio::task::spawn_blocking(move || {
        let _permit = permit;
        let profiler = pprof::ProfilerGuardBuilder::default()
            .frequency(cfg.frequency)
            .blocklist(&["libc", "libgcc", "pthread", "vdso"])
            .build()
            .map_err(|e| {
                (
                    StatusCode::INTERNAL_SERVER_ERROR,
                    format!("failed to start profiler: {e}"),
                )
            })?;
        std::thread::sleep(Duration::from_secs(seconds));
        profiler.report().build().map_err(|e| {
            (
                StatusCode::INTERNAL_SERVER_ERROR,
                format!("failed to build profile report: {e}"),
            )
        })
    })
    .await
    .map_err(|e| {
        (
            StatusCode::INTERNAL_SERVER_ERROR,
            format!("profile worker failed: {e}"),
        )
    })?
}

/// GET /debug/pprof/profile — Google pprof protobuf, consumable directly by
/// `go tool pprof` (e.g. `go tool pprof -http=:8081 'http://host/debug/pprof/profile?seconds=30'`).
pub async fn pprof_profile(Query(params): Query<ProfileParams>) -> Response {
    use pprof::protos::Message as _;

    let report = match capture(params.seconds.unwrap_or(DEFAULT_PROFILE_SECS)).await {
        Ok(r) => r,
        Err((status, msg)) => return (status, msg).into_response(),
    };
    let profile = match report.pprof() {
        Ok(p) => p,
        Err(e) => {
            return (
                StatusCode::INTERNAL_SERVER_ERROR,
                format!("failed to encode pprof profile: {e}"),
            )
                .into_response()
        }
    };
    match profile.write_to_bytes() {
        Ok(bytes) => (
            [
                (header::CONTENT_TYPE, "application/octet-stream"),
                (
                    header::CONTENT_DISPOSITION,
                    "attachment; filename=\"parqtel-cpu.pb\"",
                ),
            ],
            bytes,
        )
            .into_response(),
        Err(e) => (
            StatusCode::INTERNAL_SERVER_ERROR,
            format!("failed to serialise pprof profile: {e}"),
        )
            .into_response(),
    }
}

/// GET /debug/pprof/flamegraph — folded-stack SVG flamegraph of the window.
pub async fn pprof_flamegraph(Query(params): Query<ProfileParams>) -> Response {
    let report = match capture(params.seconds.unwrap_or(DEFAULT_PROFILE_SECS)).await {
        Ok(r) => r,
        Err((status, msg)) => return (status, msg).into_response(),
    };
    let mut svg: Vec<u8> = Vec::new();
    if let Err(e) = report.flamegraph(&mut svg) {
        return (
            StatusCode::INTERNAL_SERVER_ERROR,
            format!("failed to render flamegraph: {e}"),
        )
            .into_response();
    }
    (
        [
            (header::CONTENT_TYPE, "image/svg+xml; charset=utf-8"),
            (
                header::CONTENT_DISPOSITION,
                "inline; filename=\"parqtel-flamegraph.svg\"",
            ),
        ],
        svg,
    )
        .into_response()
}

/// One aggregated CPU consumer from a profile window.
#[derive(Debug, serde::Serialize)]
pub struct FnSummary {
    /// Demangled function name.
    pub function: String,
    /// Samples where this function owned the CPU (innermost frame).
    pub self_samples: i64,
    /// Samples where this function appeared anywhere on the stack (inclusive).
    pub cumulative_samples: i64,
    /// `self_samples` converted to CPU seconds via the sampling frequency.
    pub self_cpu_seconds: f64,
    /// Share of the window's total sampled CPU time.
    pub self_percent: f64,
}

/// GET /debug/pprof/summary — the top CPU consumers in the window, as JSON.
/// This is the fastest way to answer "what is burning CPU right now" without a
/// pprof toolchain.
pub async fn pprof_summary(Query(params): Query<ProfileParams>) -> Response {
    let seconds = params.seconds.unwrap_or(DEFAULT_PROFILE_SECS);
    let report = match capture(seconds).await {
        Ok(r) => r,
        Err((status, msg)) => return (status, msg).into_response(),
    };
    let top = params.top.unwrap_or(25).clamp(1, 500);
    let frequency = f64::from(report.timing.frequency.max(1));

    let mut self_by_fn: HashMap<String, i64> = HashMap::new();
    let mut cumulative_by_fn: HashMap<String, i64> = HashMap::new();
    let mut total: i64 = 0;

    for (frames, count) in &report.data {
        // `count` can theoretically be negative in a folded report; clamp so
        // percentage maths stays meaningful.
        let count = (*count).max(0) as i64;
        total += count;

        for frame in &frames.frames {
            if let Some(symbol) = frame.first() {
                *cumulative_by_fn.entry(symbol.name()).or_default() += count;
            }
        }
        // The first frame is the innermost (CPU-owning) one.
        if let Some(symbol) = frames.frames.first().and_then(|f| f.first()) {
            *self_by_fn.entry(symbol.name()).or_default() += count;
        }
    }

    let mut functions: Vec<FnSummary> = self_by_fn
        .into_iter()
        .map(|(function, self_samples)| {
            let self_cpu_seconds = self_samples as f64 / frequency;
            FnSummary {
                cumulative_samples: cumulative_by_fn.get(&function).copied().unwrap_or(0),
                function,
                self_samples,
                self_cpu_seconds,
                self_percent: if total > 0 {
                    (self_samples as f64 / total as f64) * 100.0
                } else {
                    0.0
                },
            }
        })
        .collect();
    // Hottest functions first (descending self-CPU samples).
    functions.sort_by_key(|f| std::cmp::Reverse(f.self_samples));
    functions.truncate(top);

    let top_self_percent: f64 = functions.iter().map(|f| f.self_percent).sum();

    Json(serde_json::json!({
        "window_secs": seconds.clamp(1, MAX_PROFILE_SECS),
        "sampling_frequency_hz": report.timing.frequency,
        "threads_sampled": report.data.len(),
        "total_samples": total,
        "sampled_cpu_seconds": total as f64 / frequency,
        "top_self_percent": top_self_percent,
        "functions": functions,
    }))
    .into_response()
}

/// GET /debug/pprof/memory — memory-bottleneck snapshot. `pprof` is a CPU
/// sampler, so this reports what is actually measurable about RSS rather than
/// pretending to be a heap profiler.
pub async fn pprof_memory(
    axum::extract::State(state): axum::extract::State<crate::state::AppState>,
) -> Response {
    let (rss, hwm) = read_vm_rss_hwm();
    let (buf_metrics, buf_logs, buf_spans) =
        state.inner.query_executor.memory_buffer().stats().await;
    Json(serde_json::json!({
        "rss_bytes": rss,
        "rss_hwm_bytes": hwm,
        "buffer": {
            "metrics_points": buf_metrics,
            "log_records": buf_logs,
            "spans": buf_spans,
            "total": buf_metrics + buf_logs + buf_spans,
        },
        "note": "pprof samples CPU only; buffer occupancy is the dominant Parqtel heap driver \
                 (points are held in RAM until the block flush). Tune \
                 storage.block_duration_secs down or raise the memory limit if RSS \
                 approaches the limit between flushes.",
    }))
    .into_response()
}
#[cfg(test)]
mod tests {
    #![allow(clippy::unwrap_used, clippy::expect_used, clippy::panic)]
    use super::*;

    #[test]
    fn parse_kb_to_bytes_converts_kib_to_bytes() {
        assert_eq!(parse_kb_to_bytes("   12345 kB"), Some(12_641_280));
        assert_eq!(parse_kb_to_bytes("0 kB"), Some(0));
        // Trailing unit is optional — only the leading number is parsed.
        assert_eq!(parse_kb_to_bytes("7"), Some(7 * 1024));
    }

    #[test]
    fn parse_kb_to_bytes_rejects_malformed_input() {
        assert_eq!(parse_kb_to_bytes(""), None);
        assert_eq!(parse_kb_to_bytes("kB"), None);
        assert_eq!(parse_kb_to_bytes("not-a-number kB"), None);
    }

    #[test]
    fn read_vm_rss_hwm_reads_proc_self_status_on_linux() {
        if !cfg!(target_os = "linux") {
            return;
        }
        let (rss, hwm) = read_vm_rss_hwm();
        let rss = rss.expect("VmRSS must be readable from /proc/self/status");
        let hwm = hwm.expect("VmHWM must be readable from /proc/self/status");
        assert!(rss > 0, "a running process must have non-zero RSS");
        assert!(
            hwm >= rss,
            "high-water mark ({hwm}) must be >= current RSS ({rss})"
        );
    }

    #[test]
    fn clamp_frequency_bounds_sampling_rate() {
        assert_eq!(clamp_frequency(0), 10, "0 Hz would never sample");
        assert_eq!(clamp_frequency(-5), 10);
        assert_eq!(
            clamp_frequency(99),
            99,
            "the documented default is honoured"
        );
        assert_eq!(clamp_frequency(100_000), 999);
    }

    #[test]
    fn clamp_window_bounds_capture_duration() {
        assert_eq!(clamp_window(0), 1, "zero seconds would capture nothing");
        assert_eq!(clamp_window(DEFAULT_PROFILE_SECS), 30);
        assert_eq!(clamp_window(u64::MAX), MAX_PROFILE_SECS);
    }

    #[test]
    fn profile_params_treat_absent_fields_as_defaults() {
        let p: ProfileParams =
            serde_json::from_str("{}").expect("empty query params must deserialize");
        assert_eq!(p.seconds, None);
        assert_eq!(p.top, None);
    }

    #[test]
    fn profile_params_parse_explicit_values() {
        let p: ProfileParams =
            serde_json::from_str(r#"{"seconds":15,"top":5}"#).expect("params must deserialize");
        assert_eq!(p.seconds, Some(15));
        assert_eq!(p.top, Some(5));
    }
}
