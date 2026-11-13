//! TODO

use super::common::{decode_offloading, IngestionStats};
use super::rotator::BlockRotator;
use crate::decode::OtlpDecoder;
use crate::otel::collector::metrics::v1::ExportMetricsServiceRequest;
use bytes::Bytes;
use parqtel_core::storage::PendingIndex;
use parqtel_core::wal::WalWriter;
use parqtel_core::MemoryBuffer;
use parqtel_core::{BlockConfig, ContentionMetrics, DataPoint, Error, Metric, Result};
use prost::Message;
use std::sync::atomic::Ordering;
use std::sync::Arc;
use tokio::sync::mpsc;

/// Public service for ingesting OTLP metrics.
pub struct IngestionService {
    rotator: Arc<BlockRotator>,
    stats: Arc<IngestionStats>,
    memory_buffer: Option<MemoryBuffer>,
    contention: Option<Arc<ContentionMetrics>>,
}

/// The points of `metric` as the memory buffer should hold them: each carrying
/// the resource's `service.name` unless the point has its own.
///
/// Label sets are shared by many points, so the labels with `service.name`
/// added are built once per distinct set rather than once per point.
fn buffer_points(metric: &Metric) -> Vec<DataPoint> {
    let Some(svc) = metric.resource_attributes.get("service.name") else {
        return metric.data_points.clone();
    };
    let mut resolved: std::collections::HashMap<parqtel_core::LabelSet, parqtel_core::LabelSet> =
        std::collections::HashMap::new();
    metric
        .data_points
        .iter()
        .map(|dp| {
            let labels = match resolved.get(&dp.labels) {
                Some(l) => l.clone(),
                None => {
                    let l = if dp.labels.get("service.name").is_some() {
                        dp.labels.clone()
                    } else {
                        parqtel_core::LabelInterner::global().intern_with(
                            &dp.labels,
                            "service.name",
                            svc,
                        )
                    };
                    resolved.insert(dp.labels.clone(), l.clone());
                    l
                }
            };
            DataPoint {
                timestamp_ns: dp.timestamp_ns,
                value: dp.value.clone(),
                labels,
            }
        })
        .collect()
}

impl IngestionService {
    pub fn new(config: BlockConfig, metadata_tx: mpsc::UnboundedSender<PendingIndex>) -> Self {
        Self::with_shards(config, metadata_tx, 1, None)
    }

    /// Creates a service whose rotator uses `shards` writer shards, with an
    /// optional write-ahead log.
    ///
    /// See [`BlockRotator`] for why the shards are merged into one block
    /// rather than each producing their own.
    pub fn with_shards(
        config: BlockConfig,
        metadata_tx: mpsc::UnboundedSender<PendingIndex>,
        shards: usize,
        wal: Option<WalWriter>,
    ) -> Self {
        let mut rotator = BlockRotator::with_shards(config, metadata_tx, shards);
        if let Some(wal) = wal {
            rotator = rotator.with_wal(wal);
        }
        Self {
            rotator: Arc::new(rotator),
            stats: Arc::new(IngestionStats::default()),
            memory_buffer: None,
            contention: None,
        }
    }

    /// Number of writer shards in use.
    pub fn rotator_shards(&self) -> usize {
        self.rotator.shard_count()
    }

    /// Set the shared memory buffer for stream-queryable data.
    ///
    /// The rotator gets a handle too: with a background flush worker it must
    /// drain the buffer when a block becomes *durable*, which is long after
    /// this request returns.
    pub fn with_memory_buffer(mut self, buffer: MemoryBuffer) -> Self {
        self.rotator.set_buffer(Some(buffer.clone()));
        self.memory_buffer = Some(buffer);
        self
    }

    /// Share lock-wait and flush counters with the `/metrics` renderer.
    pub fn with_contention(mut self, contention: Arc<ContentionMetrics>) -> Self {
        self.contention = Some(contention);
        self
    }

    /// Starts the background flush worker.
    ///
    /// `max_in_flight` bounds concurrent encodes; when the queue is full a flush
    /// encodes inline instead, so backpressure degrades to the previous
    /// synchronous behaviour rather than growing a backlog.
    pub fn start_flush_worker(&self, max_in_flight: usize) {
        self.rotator.start_flush_worker(max_in_flight);
    }

    /// Schedules a flush without stopping the worker, so a test can observe the
    /// window between a flush being scheduled and its block becoming durable.
    pub async fn schedule_flush_for_test(&self) -> Result<bool> {
        let contention = self.contention.clone();
        self.rotator.flush(contention).await?;
        Ok(true)
    }

    pub async fn ingest_proto(&self, body: Bytes) -> Result<u64> {
        self.stats.total_batches.fetch_add(1, Ordering::Relaxed);
        let metrics = decode_offloading(move || {
            let req = ExportMetricsServiceRequest::decode(body)
                .map_err(|e| Error::Validation(format!("Protobuf decode error: {e}")))?;
            OtlpDecoder::decode_metrics(req)
        })
        .await
        .inspect_err(|_| {
            self.stats.failed_batches.fetch_add(1, Ordering::Relaxed);
        })?;
        self.process_metrics(metrics).await
    }

    /// Ingests a request a transport has already decoded (gRPC), skipping the
    /// encode and decode a byte-oriented entry point would need.
    pub async fn ingest_request(&self, request: ExportMetricsServiceRequest) -> Result<u64> {
        self.stats.total_batches.fetch_add(1, Ordering::Relaxed);
        let metrics = decode_offloading(move || OtlpDecoder::decode_metrics(request))
            .await
            .inspect_err(|_| {
                self.stats.failed_batches.fetch_add(1, Ordering::Relaxed);
            })?;
        self.process_metrics(metrics).await
    }

    /// Ingest already-decoded metrics directly (used by the span-metrics
    /// RED bridge, which derives metrics from trace spans).
    pub async fn ingest_metrics(&self, metrics: Vec<Metric>) -> Result<u64> {
        self.stats.total_batches.fetch_add(1, Ordering::Relaxed);
        self.process_metrics(metrics).await
    }

    pub async fn ingest_json(&self, body: Bytes) -> Result<u64> {
        self.stats.total_batches.fetch_add(1, Ordering::Relaxed);
        let metrics = decode_offloading(move || {
            let json: serde_json::Value = serde_json::from_slice(&body)
                .map_err(|e| Error::Validation(format!("JSON parse error: {e}")))?;
            OtlpDecoder::decode_metrics_json(json)
        })
        .await
        .inspect_err(|_| {
            self.stats.failed_batches.fetch_add(1, Ordering::Relaxed);
        })?;
        self.process_metrics(metrics).await
    }

    async fn process_metrics(&self, metrics: Vec<Metric>) -> Result<u64> {
        let mut count = 0;
        let contention = self.contention.clone();
        for m in metrics {
            count += m.data_points.len() as u64;
            // The memory buffer copy carries `service.name` from the resource
            // attributes, so label matchers behave the same before and after a
            // flush. It is placed by the rotator, inside the shard lock, to keep
            // buffer order identical to writer order. The rotator measures its
            // own per-shard lock wait.
            let buffered = self.memory_buffer.as_ref().map(|_| buffer_points(&m));
            self.rotator
                .push_buffered(m, buffered, contention.clone())
                .await?;
        }
        // Duration-based rotation. Whatever a flush covers leaves the buffer
        // when its block is durable, not here.
        self.rotator.check_and_flush(contention.clone()).await?;
        self.stats
            .ingested_points
            .fetch_add(count, Ordering::Relaxed);
        tracing::debug!(
            signal = "metrics",
            ingested = count,
            "metrics ingestion complete"
        );
        Ok(count)
    }

    /// Checks for duration-based flush; returns `true` when a flush happened
    /// (the shared memory buffer is drained so flushed rows aren't double-read).
    pub async fn check_and_flush(&self) -> Result<bool> {
        let contention = self.contention.clone();
        self.rotator.check_and_flush(contention).await
    }

    pub async fn shutdown(&self) -> Result<()> {
        let contention = self.contention.clone();
        let _ = self.rotator.flush(contention.clone()).await;
        // Stops the worker after it drains, so every scheduled block is written
        // and its WAL position committed before we exit.
        self.rotator.stop_flush_worker().await;
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
