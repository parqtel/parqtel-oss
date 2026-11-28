//! HTTP fan-out to the writers' in-memory buffers.
//!
//! A reader node has no ingest path, so the newest seconds of every signal sit
//! in the writers' buffers until their next flush. The reader asks each writer
//! for the rows newer than its own watermark and merges the replies; a writer
//! that is down or slow degrades the freshness of the answer, never its
//! correctness, so a failed request only logs a warning.

use futures::future::join_all;
use parqtel_core::{Error, Result};
use parqtel_query::remote::{RemoteBuffer, RemoteBufferRequest, RemoteBufferRows, RemoteFuture};
use std::collections::HashSet;
use std::time::Duration;

/// Fans a buffer query out to every configured writer over HTTP.
pub(crate) struct HttpWriterFanout {
    client: reqwest::Client,
    endpoints: Vec<String>,
    token: Option<String>,
}

impl HttpWriterFanout {
    pub(crate) fn new(
        endpoints: Vec<String>,
        token: Option<String>,
        timeout: Duration,
    ) -> Result<Self> {
        for endpoint in &endpoints {
            let parsed = reqwest::Url::parse(endpoint).map_err(|error| {
                Error::Validation(format!(
                    "query.writer_endpoints entry '{endpoint}': {error}"
                ))
            })?;
            if !matches!(parsed.scheme(), "http" | "https") {
                return Err(Error::Validation(format!(
                    "query.writer_endpoints entry '{endpoint}' must be an http(s) URL"
                )));
            }
        }
        let client = reqwest::Client::builder()
            .timeout(timeout)
            .build()
            .map_err(|error| Error::Validation(format!("query writer fan-out client: {error}")))?;
        Ok(Self {
            client,
            endpoints,
            token,
        })
    }

    /// Number of writers this reader asks. Zero means no fan-out is installed.
    pub(crate) fn writer_count(&self) -> usize {
        self.endpoints.len()
    }

    async fn fetch_one(
        &self,
        endpoint: String,
        request: &RemoteBufferRequest,
    ) -> Result<RemoteBufferRows> {
        let url = format!("{}/internal/v1/buffer", endpoint.trim_end_matches('/'));
        let mut call = self.client.post(&url).json(request);
        if let Some(token) = &self.token {
            call = call.bearer_auth(token);
        }
        let response = call
            .send()
            .await
            .map_err(|error| Error::Internal(format!("buffer query to {url}: {error}")))?;
        let status = response.status();
        if !status.is_success() {
            return Err(Error::Internal(format!(
                "buffer query to {url} returned HTTP {status}"
            )));
        }
        response
            .json()
            .await
            .map_err(|error| Error::Internal(format!("buffer reply from {url}: {error}")))
    }
}

impl RemoteBuffer for HttpWriterFanout {
    fn fetch(&self, request: RemoteBufferRequest) -> RemoteFuture<'_> {
        Box::pin(async move {
            let replies = join_all(
                self.endpoints
                    .iter()
                    .cloned()
                    .map(|endpoint| self.fetch_one(endpoint, &request)),
            )
            .await;

            // Writers own disjoint rows: a row exists in exactly one buffer.
            // A retried OTLP export can still land the same point twice, so
            // duplicate identities collapse here.
            let mut rows = RemoteBufferRows::default();
            let mut seen_points = HashSet::new();
            let mut seen_logs = HashSet::new();
            let mut seen_spans = HashSet::new();
            for reply in replies {
                let reply = match reply {
                    Ok(reply) => reply,
                    Err(error) => {
                        tracing::warn!(%error, "writer buffer query failed");
                        continue;
                    }
                };
                rows.watermark_ns = match (rows.watermark_ns, reply.watermark_ns) {
                    (Some(left), Some(right)) => Some(left.max(right)),
                    (left, right) => left.or(right),
                };
                for (metric, point) in reply.points {
                    let identity = (
                        metric.clone(),
                        point.labels.fingerprint(),
                        point.timestamp_ns,
                    );
                    if seen_points.insert(identity) {
                        rows.points.push((metric, point));
                    }
                }
                for log in reply.logs {
                    let identity = (
                        log.timestamp_ns,
                        log.severity_number,
                        log.trace_id,
                        log.body.clone(),
                    );
                    if seen_logs.insert(identity) {
                        rows.logs.push(log);
                    }
                }
                for span in reply.spans {
                    if seen_spans.insert((span.trace_id, span.span_id)) {
                        rows.spans.push(span);
                    }
                }
            }
            Ok(rows)
        })
    }
}
