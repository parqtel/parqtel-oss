//! TODO

use super::common::{decode_offloading, lock_rotator, IngestionStats};
use super::serial::LogRotator;
use crate::decode::OtlpDecoder;
use crate::otel::collector::logs::v1::ExportLogsServiceRequest;
use bytes::Bytes;
use parqtel_core::storage::PendingIndex;
use parqtel_core::wal::WalWriter;
use parqtel_core::MemoryBuffer;
use parqtel_core::{ContentionMetrics, Error, LogBlockConfig, LogRecord, Result, SignalType};
use prost::Message;
use std::sync::atomic::Ordering;
use std::sync::Arc;
use tokio::sync::{mpsc, Mutex};

/// Public service for ingesting OTLP logs.
pub struct LogIngestionService {
    pub(super) rotator: Arc<Mutex<LogRotator>>,
    wal_enabled: bool,
    stats: Arc<IngestionStats>,
    memory_buffer: Option<MemoryBuffer>,
    contention: Option<Arc<ContentionMetrics>>,
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
        }
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
        self.ingest_logs(logs).await
    }

    /// Ingests a request a transport has already decoded (gRPC).
    pub async fn ingest_request(&self, request: ExportLogsServiceRequest) -> Result<u64> {
        self.stats.total_batches.fetch_add(1, Ordering::Relaxed);
        let logs = decode_offloading(move || OtlpDecoder::decode_logs(request))
            .await
            .inspect_err(|_| {
                self.stats.failed_batches.fetch_add(1, Ordering::Relaxed);
            })?;
        self.ingest_logs(logs).await
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
        self.ingest_logs(logs).await
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
