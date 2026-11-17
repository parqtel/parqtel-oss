//! Log ingest: decode an OTLP log request, buffer it, WAL it and rotate it into
//! blocks through the shared serial rotator.

use super::common::{decode_offloading, lock_rotator, IngestionStats};
use super::serial::LogRotator;
use crate::decode::OtlpDecoder;
use crate::otel::collector::logs::v1::ExportLogsServiceRequest;
use bytes::Bytes;
use parqtel_core::storage::PendingIndex;
use parqtel_core::wal::WalWriter;
use parqtel_core::{
    ContentionMetrics, Error, LogBlockConfig, LogRecord, MemoryBuffer, Metric, Result, SignalType,
};
use parqtel_pipeline::pipeline::PipelineSet;
use parqtel_pipeline::rule::registry::RuleRegistry as PipelineRegistry;
use prost::Message;
use std::sync::atomic::Ordering;
use std::sync::Arc;
use tokio::sync::{mpsc, Mutex, RwLock};

type CompiledPipelineCache = Arc<RwLock<Option<(u64, Arc<PipelineSet>)>>>;

/// Public service for ingesting OTLP logs.
pub struct LogIngestionService {
    pub(super) rotator: Arc<Mutex<LogRotator>>,
    wal_enabled: bool,
    stats: Arc<IngestionStats>,
    memory_buffer: Option<MemoryBuffer>,
    contention: Option<Arc<ContentionMetrics>>,
    /// Definitions and their compiled form. The ingest path recompiles only
    /// when the registry generation changes, so API edits take effect without
    /// a restart and without compiling on every batch.
    pipeline_registry: Option<PipelineRegistry>,
    pipeline_cache: CompiledPipelineCache,
    /// Extracted metrics are forwarded through the metrics ingestion service,
    /// which owns the WAL and buffer for that signal.
    metric_tx: Option<mpsc::Sender<Vec<Metric>>>,
}

impl LogIngestionService {
    /// Bytes the log WAL currently occupies.
    pub async fn wal_bytes(&self) -> u64 {
        self.rotator.lock().await.wal_bytes().await
    }

    pub fn new(config: LogBlockConfig, metadata_tx: mpsc::UnboundedSender<PendingIndex>) -> Self {
        Self::with_wal(config, metadata_tx, None)
    }

    /// Creates the service with an optional write-ahead log.
    pub fn with_wal(
        config: LogBlockConfig,
        metadata_tx: mpsc::UnboundedSender<PendingIndex>,
        wal: Option<WalWriter>,
    ) -> Self {
        let mut rotator = LogRotator::new(config, metadata_tx);
        let wal_enabled = wal.is_some();
        if let Some(w) = wal {
            rotator = rotator.with_wal(w);
        }
        Self {
            rotator: Arc::new(Mutex::new(rotator)),
            wal_enabled,
            stats: Arc::new(IngestionStats::default()),
            memory_buffer: None,
            contention: None,
            pipeline_registry: None,
            pipeline_cache: Arc::new(RwLock::new(None)),
            metric_tx: None,
        }
    }

    /// Attach compiled pipelines and the sink for metrics they extract.
    ///
    /// The registry is shared with the API so a definition change is picked up
    /// by the next live batch (WAL replay deliberately does not re-run
    /// pipelines, otherwise a crash would duplicate derived metrics).
    pub fn with_pipelines(
        mut self,
        registry: PipelineRegistry,
        metric_tx: mpsc::Sender<Vec<Metric>>,
    ) -> Self {
        self.pipeline_registry = Some(registry);
        self.metric_tx = Some(metric_tx);
        self
    }

    /// Set the shared memory buffer for stream-queryable data.
    ///
    /// The rotator gets a handle too: it feeds the buffer next to its writer and
    /// removes a block's rows once that block is durable.
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

    pub async fn ingest_proto(&self, body: Bytes) -> Result<u64> {
        self.stats.total_batches.fetch_add(1, Ordering::Relaxed);
        let logs = decode_offloading(move || {
            let req = ExportLogsServiceRequest::decode(body)
                .map_err(|e| Error::Validation(format!("Protobuf decode error: {e}")))?;
            OtlpDecoder::decode_logs(req)
        })
        .await
        .inspect_err(|_| {
            self.stats.failed_batches.fetch_add(1, Ordering::Relaxed);
        })?;
        self.ingest_logs_live(logs).await
    }

    /// Ingests a request a transport has already decoded (gRPC).
    pub async fn ingest_request(&self, request: ExportLogsServiceRequest) -> Result<u64> {
        self.stats.total_batches.fetch_add(1, Ordering::Relaxed);
        let logs = decode_offloading(move || OtlpDecoder::decode_logs(request))
            .await
            .inspect_err(|_| {
                self.stats.failed_batches.fetch_add(1, Ordering::Relaxed);
            })?;
        self.ingest_logs_live(logs).await
    }

    pub async fn ingest_json(&self, body: Bytes) -> Result<u64> {
        self.stats.total_batches.fetch_add(1, Ordering::Relaxed);
        let logs = decode_offloading(move || {
            let json: serde_json::Value = serde_json::from_slice(&body)
                .map_err(|e| Error::Validation(format!("JSON parse error: {e}")))?;
            OtlpDecoder::decode_logs_json(json)
        })
        .await
        .inspect_err(|_| {
            self.stats.failed_batches.fetch_add(1, Ordering::Relaxed);
        })?;
        self.ingest_logs_live(logs).await
    }

    /// Live ingest: run the configured pipelines, forward extracted metrics,
    /// then write the surviving logs through the normal WAL/buffer path.
    ///
    /// WAL replay calls [`Self::ingest_logs`] directly instead, so a crash
    /// between the log WAL append and the metric WAL append does not derive
    /// the same metrics twice.
    pub async fn ingest_logs_live(&self, logs: Vec<LogRecord>) -> Result<u64> {
        let (logs, metrics) = self.apply_pipelines(logs).await?;
        if !metrics.is_empty() {
            if let Some(tx) = &self.metric_tx {
                if let Err(e) = tx.try_send(metrics) {
                    tracing::warn!(
                        error = %e,
                        "pipeline metric queue is full; dropping extracted metrics"
                    );
                }
            }
        }
        self.ingest_logs(logs).await
    }

    async fn apply_pipelines(&self, logs: Vec<LogRecord>) -> Result<(Vec<LogRecord>, Vec<Metric>)> {
        let Some(registry) = &self.pipeline_registry else {
            return Ok((logs, Vec::new()));
        };
        let generation = registry.generation();
        let cached = self.pipeline_cache.read().await.clone();
        let set = match cached {
            Some((cached_generation, set)) if cached_generation == generation => set,
            _ => {
                let definitions = registry.get_pipelines();
                match PipelineSet::from_definitions(&definitions) {
                    Ok(compiled) => {
                        let compiled = Arc::new(compiled);
                        *self.pipeline_cache.write().await = Some((generation, compiled.clone()));
                        compiled
                    }
                    Err(e) => {
                        tracing::error!(
                            error = %e,
                            "pipeline compilation failed; passing logs through unchanged"
                        );
                        return Ok((logs, Vec::new()));
                    }
                }
            }
        };
        if set.is_empty() {
            return Ok((logs, Vec::new()));
        }
        tokio::task::spawn_blocking(move || set.execute(logs))
            .await
            .map_err(|e| Error::Internal(format!("pipeline task failed: {e}")))
    }

    /// Ingests already-decoded log records.
    ///
    /// Public because WAL replay feeds recovered records back through exactly
    /// this path, so recovered telemetry is treated identically to live data.
    pub async fn ingest_logs(&self, logs: Vec<LogRecord>) -> Result<u64> {
        let count = logs.len() as u64;
        // Framing the WAL records (JSON + checksum) is the CPU-heavy half of
        // logging them and needs no lock; the rotator then writes the whole
        // request in one go.
        let wal_records = if self.wal_enabled {
            let mut bytes = Vec::with_capacity(logs.len() * 320);
            for log in &logs {
                parqtel_core::wal::encode_record_into(log, &mut bytes)?;
            }
            Some(bytes)
        } else {
            None
        };
        let contention = self.contention.clone();
        let mut rotator =
            lock_rotator(&self.rotator, SignalType::Logs, contention.as_deref()).await;
        let mut flushed = rotator
            .push_batch(logs, wal_records, contention.as_ref())
            .await?;
        if rotator.check_and_flush(contention.as_ref()).await? {
            flushed = true;
        }
        drop(rotator);
        self.stats
            .ingested_points
            .fetch_add(count, Ordering::Relaxed);
        tracing::debug!(
            signal = "logs",
            ingested = count,
            flushed = flushed,
            "logs ingestion complete"
        );
        Ok(count)
    }

    /// Checks for duration-based flush; returns `true` when a flush started.
    /// The rows leave the memory buffer when their block is durable.
    pub async fn check_and_flush(&self) -> Result<bool> {
        let contention = self.contention.clone();
        lock_rotator(&self.rotator, SignalType::Logs, contention.as_deref())
            .await
            .check_and_flush(contention.as_ref())
            .await
    }

    pub async fn shutdown(&self) -> Result<()> {
        let contention = self.contention.clone();
        let mut rotator =
            lock_rotator(&self.rotator, SignalType::Logs, contention.as_deref()).await;
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
