//! TODO

use super::common::{decode_offloading, lock_rotator, IngestionStats};
use super::serial::TraceRotator;
use crate::decode::OtlpDecoder;
use crate::otel::collector::trace::v1::ExportTraceServiceRequest;
use bytes::Bytes;
use parqtel_core::storage::PendingIndex;
use parqtel_core::wal::WalWriter;
use parqtel_core::MemoryBuffer;
use parqtel_core::{
    BlockConfig, ContentionMetrics, Error, Metric, Result, SignalType, Span, TailSamplingConfig,
};
use prost::Message;
use std::sync::atomic::Ordering;
use std::sync::Arc;
use tokio::sync::{mpsc, Mutex};

/// Public service for ingesting OTLP traces.
pub struct TraceIngestionService {
    pub(super) rotator: Arc<Mutex<TraceRotator>>,
    wal_enabled: bool,
    stats: Arc<IngestionStats>,
    memory_buffer: Option<MemoryBuffer>,
    /// When set, span-metrics RED derivation sends metric batches here
    /// (consumed by a task that feeds the metrics ingestion service).
    span_metrics_tx: Option<mpsc::UnboundedSender<Vec<Metric>>>,
    /// Tail-sampling policy; default (keep-all) short-circuits to zero cost.
    tail_sampling: TailSamplingConfig,
    contention: Option<Arc<ContentionMetrics>>,
}

impl TraceIngestionService {
    pub fn new(config: BlockConfig, metadata_tx: mpsc::UnboundedSender<PendingIndex>) -> Self {
        Self::with_wal(config, metadata_tx, None)
    }

    /// Creates the service with an optional write-ahead log.
    pub fn with_wal(
        config: BlockConfig,
        metadata_tx: mpsc::UnboundedSender<PendingIndex>,
        wal: Option<WalWriter>,
    ) -> Self {
        let mut rotator = TraceRotator::new(config, metadata_tx);
        let wal_enabled = wal.is_some();
        if let Some(w) = wal {
            rotator = rotator.with_wal(w);
        }
        Self {
            rotator: Arc::new(Mutex::new(rotator)),
            wal_enabled,
            stats: Arc::new(IngestionStats::default()),
            memory_buffer: None,
            span_metrics_tx: None,
            tail_sampling: TailSamplingConfig::default(),
            contention: None,
        }
    }

    /// Set the shared memory buffer for stream-queryable spans.
    ///
    /// The rotator gets a handle too: it feeds the buffer next to its writer and
    /// removes a block's spans once that block is durable.
    pub fn with_memory_buffer(mut self, buffer: MemoryBuffer) -> Self {
        // Builder-time, before the service is shared: the lock is uncontended.
        if let Ok(mut rotator) = self.rotator.try_lock() {
            rotator.set_buffer(Some(buffer.clone()));
        }
        self.memory_buffer = Some(buffer);
        self
    }

    /// Share lock-wait and flush counters with the `/metrics` renderer.
    pub fn with_contention(mut self, contention: Arc<ContentionMetrics>) -> Self {
        self.contention = Some(contention);
        self
    }

    /// Enable the span-metrics RED bridge: derived metrics
    /// (`traces_service_{requests,errors,duration_ms}_total`) are sent to
    /// this channel for ingestion as normal metrics.
    pub fn with_span_metrics(mut self, tx: mpsc::UnboundedSender<Vec<Metric>>) -> Self {
        self.span_metrics_tx = Some(tx);
        self
    }

    /// Set the tail-sampling policy for traces.
    pub fn with_tail_sampling(mut self, policy: TailSamplingConfig) -> Self {
        self.tail_sampling = policy;
        self
    }

    pub async fn ingest_proto(&self, body: Bytes) -> Result<u64> {
        self.stats.total_batches.fetch_add(1, Ordering::Relaxed);
        let spans = decode_offloading(move || {
            let req = ExportTraceServiceRequest::decode(body)
                .map_err(|e| Error::Validation(format!("Protobuf decode error: {e}")))?;
            OtlpDecoder::decode_traces(req)
        })
        .await
        .inspect_err(|_| {
            self.stats.failed_batches.fetch_add(1, Ordering::Relaxed);
        })?;
        self.ingest_spans(spans).await
    }

    /// Ingests a request a transport has already decoded (gRPC).
    pub async fn ingest_request(&self, request: ExportTraceServiceRequest) -> Result<u64> {
        self.stats.total_batches.fetch_add(1, Ordering::Relaxed);
        let spans = decode_offloading(move || OtlpDecoder::decode_traces(request))
            .await
            .inspect_err(|_| {
                self.stats.failed_batches.fetch_add(1, Ordering::Relaxed);
            })?;
        self.ingest_spans(spans).await
    }

    pub async fn ingest_json(&self, body: Bytes) -> Result<u64> {
        self.stats.total_batches.fetch_add(1, Ordering::Relaxed);
        let spans = decode_offloading(move || {
            let json: serde_json::Value = serde_json::from_slice(&body)
                .map_err(|e| Error::Validation(format!("JSON parse error: {e}")))?;
            OtlpDecoder::decode_traces_json(json)
        })
        .await
        .inspect_err(|_| {
            self.stats.failed_batches.fetch_add(1, Ordering::Relaxed);
        })?;
        self.ingest_spans(spans).await
    }

    /// Ingests already-decoded spans.
    ///
    /// Public because WAL replay feeds recovered records back through exactly
    /// this path, so recovered telemetry is treated identically to live data.
    pub async fn ingest_spans(&self, spans: Vec<Span>) -> Result<u64> {
        let count = spans.len() as u64;
        // Span-metrics RED bridge: derive metrics from the FULL span set
        // (BEFORE tail sampling) so RED rates stay accurate while trace
        // storage is sampled.
        if let Some(ref tx) = self.span_metrics_tx {
            let derived = crate::span_metrics::derive_span_metrics(&spans);
            if !derived.is_empty() {
                let _ = tx.send(derived);
            }
        }
        // Tail sampling: decide which traces to persist.
        let (spans, dropped) = crate::tail_sampling::sample_spans(&self.tail_sampling, spans);
        if dropped > 0 {
            self.stats
                .dropped_spans
                .fetch_add(dropped, Ordering::Relaxed);
        }
        // Framing the WAL records is the CPU-heavy half of logging them and
        // needs no lock; the rotator then writes the whole request in one go.
        let wal_records = if self.wal_enabled {
            let mut bytes = Vec::with_capacity(spans.len() * 512);
            for span in &spans {
                parqtel_core::wal::encode_record_into(span, &mut bytes)?;
            }
            Some(bytes)
        } else {
            None
        };
        let contention = self.contention.clone();
        let mut rotator =
            lock_rotator(&self.rotator, SignalType::Traces, contention.as_deref()).await;
        // The rotator also makes the spans queryable (memory buffer) as it
        // accepts them.
        let mut flushed = rotator
            .push_batch(spans, wal_records, contention.as_ref())
            .await?;
        if rotator.check_and_flush(contention.as_ref()).await? {
            flushed = true;
        }
        drop(rotator);
        self.stats
            .ingested_points
            .fetch_add(count, Ordering::Relaxed);
        tracing::debug!(
            signal = "traces",
            ingested = count,
            flushed = flushed,
            "traces ingestion complete"
        );
        Ok(count)
    }

    /// Checks for duration-based flush; returns `true` when a flush started.
    /// The spans leave the memory buffer when their block is durable.
    pub async fn check_and_flush(&self) -> Result<bool> {
        let contention = self.contention.clone();
        lock_rotator(&self.rotator, SignalType::Traces, contention.as_deref())
            .await
            .check_and_flush(contention.as_ref())
            .await
    }

    pub async fn shutdown(&self) -> Result<()> {
        let contention = self.contention.clone();
        let mut rotator =
            lock_rotator(&self.rotator, SignalType::Traces, contention.as_deref()).await;
        let _ = rotator.flush(contention.as_ref()).await;
        Ok(())
    }

    pub fn stats(&self) -> (u64, u64, u64) {
        (
            self.stats.total_batches.load(Ordering::Relaxed),
            self.stats.failed_batches.load(Ordering::Relaxed),
            self.stats.ingested_points.load(Ordering::Relaxed),
        )
    }
}
