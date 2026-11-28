//! Fan-out to the writers' in-memory buffers.
//!
//! A sharded deployment gives the reader tier no ingest path: writers hold the
//! newest rows in memory until the next flush, and the reader sees only what
//! has landed in object storage. The reader therefore asks every writer for the
//! buffered rows *after* the newest block it can already see (the watermark),
//! so merged results neither miss the freshest seconds nor count a row twice
//! once its block becomes visible.
//!
//! The transport lives in `parqtel-server`; this module only defines the
//! request/response shape and the one hook the executor needs.

use std::future::Future;
use std::pin::Pin;

use parqtel_core::{DataPoint, Error, LogRecord, Span};
use serde::{Deserialize, Serialize};

/// Which signal a buffer query asks for.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "lowercase")]
pub enum RemoteSignal {
    Metrics,
    Logs,
    Traces,
}

/// Rows a reader asks each writer for.
///
/// `since_ns` is the reader's watermark: the newest block timestamp it can
/// already see for that signal. Writers return only rows strictly newer, which
/// is what makes the merge exactly-once while a writer's buffer drains.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct RemoteBufferRequest {
    pub signal: RemoteSignal,
    /// Metric name for `Metrics`; ignored by the other signals.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub metric: Option<String>,
    pub since_ns: i64,
    pub start_ns: i64,
    pub end_ns: i64,
    /// Upper bound on returned rows, so one writer cannot flood a reader.
    pub limit: usize,
}

/// Buffered rows returned by one writer.
#[derive(Debug, Default, Clone, Serialize, Deserialize)]
pub struct RemoteBufferRows {
    /// Newest buffered timestamp this writer saw; the reader tracks the
    /// maximum across writers.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub watermark_ns: Option<i64>,
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub points: Vec<(String, DataPoint)>,
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub logs: Vec<LogRecord>,
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub spans: Vec<Span>,
}

/// One writer's buffer query, boxed so the trait stays object-safe without an
/// async-trait dependency.
pub type RemoteFuture<'a> =
    Pin<Box<dyn Future<Output = Result<RemoteBufferRows, Error>> + Send + 'a>>;

/// Fetch hook the reader installs into the executor.
pub trait RemoteBuffer: Send + Sync {
    fn fetch(&self, request: RemoteBufferRequest) -> RemoteFuture<'_>;
}

/// Cap on rows one writer may return per request. Matches the per-series
/// sample cap: more than this in the unflushed window is already pathological.
pub const MAX_BUFFER_ROWS: usize = 10_000;
