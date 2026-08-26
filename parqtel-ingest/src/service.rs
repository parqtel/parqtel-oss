use crate::decode::OtlpDecoder;
use crate::otel::collector::logs::v1::ExportLogsServiceRequest;
use crate::otel::collector::metrics::v1::ExportMetricsServiceRequest;
use crate::otel::collector::trace::v1::ExportTraceServiceRequest;
use crate::writer::{BlockMetadata, BlockWriter, LogWriter, TraceWriter};
use bytes::Bytes;
use parqtel_core::MemoryBuffer;
use parqtel_core::{BlockConfig, Error, LogBlockConfig, LogRecord, Metric, Result, Span};
use prost::Message;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;
use std::time::{Duration, Instant};
use tokio::sync::{mpsc, Mutex};

/// Handles automatic rotation and flushing of metric blocks.
pub struct BlockRotator {
    writer: BlockWriter,
    config: BlockConfig,
    last_flush: Instant,
    max_duration: Duration,
    metadata_tx: mpsc::UnboundedSender<BlockMetadata>,
}

impl BlockRotator {
    pub fn new(config: BlockConfig, metadata_tx: mpsc::UnboundedSender<BlockMetadata>) -> Self {
        let max_duration = Duration::from_secs(config.block_duration_secs);
        Self {
            writer: BlockWriter::new(config.clone()),
            config,
            last_flush: Instant::now(),
            max_duration,
            metadata_tx,
        }
    }

    /// Flushes first if the batch would exceed block capacity, then pushes.
    /// Preserves every data point; the original dropped points past capacity.
    /// Returns `true` if a flush happened (caller drains the memory buffer).
    /// ponytail: a single batch larger than a whole block still overflows one
    /// block boundary — split batches if that ever matters.
    pub async fn push(&mut self, metric: Metric) -> Result<bool> {
        let mut flushed = false;
        if self.writer.len() + metric.data_points.len() > self.config.max_rows_per_block {
            self.flush().await?;
            flushed = true;
        }
        self.writer.push(metric).map(|_| flushed)
    }

    pub async fn check_and_flush(&mut self) -> Result<bool> {
        if Instant::now().duration_since(self.last_flush) >= self.max_duration {
            self.flush().await?;
            return Ok(true);
        }
        Ok(false)
    }

    /// Writes buffered rows to Parquet on the blocking thread pool so Parquet
    /// encoding/compression/disk I/O never stalls a tokio worker while the
    /// ingest mutex is held. Idempotent on an empty buffer (the old code
    /// returned a spurious "Cannot flush empty buffer" error).
    pub async fn flush(&mut self) -> Result<()> {
        if self.writer.is_empty() {
            return Ok(());
        }
        let mut writer = std::mem::replace(&mut self.writer, BlockWriter::new(self.config.clone()));
        let metadata = tokio::task::spawn_blocking(move || writer.flush())
            .await
            .map_err(|e| Error::Internal(format!("flush task panicked: {}", e)))??;
        self.last_flush = Instant::now();
        let _ = self.metadata_tx.send(metadata);
        Ok(())
    }
}

/// Handles automatic rotation and flushing of log blocks.
pub struct LogRotator {
    writer: LogWriter,
    config: LogBlockConfig,
    last_flush: Instant,
    max_duration: Duration,
    metadata_tx: mpsc::UnboundedSender<BlockMetadata>,
}

impl LogRotator {
    pub fn new(config: LogBlockConfig, metadata_tx: mpsc::UnboundedSender<BlockMetadata>) -> Self {
        let max_duration = Duration::from_secs(config.block_duration_secs);
        Self {
            writer: LogWriter::new(config.clone()),
            config,
            last_flush: Instant::now(),
            max_duration,
            metadata_tx,
        }
    }

    pub async fn push(&mut self, log: LogRecord) -> Result<bool> {
        if self.writer.len() + 1 > self.config.max_rows_per_block {
            self.flush().await?;
            return self.writer.push(log).map(|_| true);
        }
        self.writer.push(log).map(|_| false)
    }

    pub async fn check_and_flush(&mut self) -> Result<bool> {
        if Instant::now().duration_since(self.last_flush) >= self.max_duration {
            self.flush().await?;
            return Ok(true);
        }
        Ok(false)
    }

    /// See [BlockRotator::flush] for the blocking-pool rationale.
    pub async fn flush(&mut self) -> Result<()> {
        if self.writer.is_empty() {
            return Ok(());
        }
        let mut writer = std::mem::replace(&mut self.writer, LogWriter::new(self.config.clone()));
        let metadata = tokio::task::spawn_blocking(move || writer.flush())
            .await
            .map_err(|e| Error::Internal(format!("flush task panicked: {}", e)))??;
        self.last_flush = Instant::now();
        let _ = self.metadata_tx.send(metadata);
        Ok(())
    }
}

/// Stats for an ingestion service.
#[derive(Default)]
pub struct IngestionStats {
    pub total_batches: AtomicU64,
    pub failed_batches: AtomicU64,
    pub ingested_points: AtomicU64,
}

/// Public service for ingesting OTLP metrics.
pub struct IngestionService {
    rotator: Arc<Mutex<BlockRotator>>,
    stats: Arc<IngestionStats>,
    memory_buffer: Option<MemoryBuffer>,
}

impl IngestionService {
    pub fn new(config: BlockConfig, metadata_tx: mpsc::UnboundedSender<BlockMetadata>) -> Self {
        Self {
            rotator: Arc::new(Mutex::new(BlockRotator::new(config, metadata_tx))),
            stats: Arc::new(IngestionStats::default()),
            memory_buffer: None,
        }
    }

    /// Set the shared memory buffer for stream-queryable data.
    pub fn with_memory_buffer(mut self, buffer: MemoryBuffer) -> Self {
        self.memory_buffer = Some(buffer);
        self
    }

    pub async fn ingest_proto(&self, body: Bytes) -> Result<u64> {
        self.stats.total_batches.fetch_add(1, Ordering::Relaxed);
        let req = ExportMetricsServiceRequest::decode(body)
            .map_err(|e| Error::Validation(format!("Protobuf decode error: {}", e)))?;
        let metrics = OtlpDecoder::decode_metrics(req).inspect_err(|_| {
            self.stats.failed_batches.fetch_add(1, Ordering::Relaxed);
        })?;
        self.process_metrics(metrics).await
    }

    pub async fn ingest_json(&self, body: Bytes) -> Result<u64> {
        self.stats.total_batches.fetch_add(1, Ordering::Relaxed);
        let json: serde_json::Value = serde_json::from_slice(&body)
            .map_err(|e| Error::Validation(format!("JSON parse error: {}", e)))?;
        let metrics = OtlpDecoder::decode_metrics_json(json).inspect_err(|_| {
            self.stats.failed_batches.fetch_add(1, Ordering::Relaxed);
        })?;
        self.process_metrics(metrics).await
    }

    async fn process_metrics(&self, metrics: Vec<Metric>) -> Result<u64> {
        let mut count = 0;
        let mut flushed = false;
        // Write to in-memory buffer first (before rotator takes ownership)
        if let Some(ref buf) = self.memory_buffer {
            for m in &metrics {
                buf.push_metrics(&m.name, &m.data_points).await;
            }
        }
        let mut rotator = self.rotator.lock().await;
        for m in metrics {
            count += m.data_points.len() as u64;
            if rotator.push(m).await? {
                flushed = true;
            }
        }
        if rotator.check_and_flush().await? {
            flushed = true;
        }
        drop(rotator);
        if flushed {
            if let Some(ref buf) = self.memory_buffer {
                buf.drain_metrics().await;
            }
        }
        self.stats
            .ingested_points
            .fetch_add(count, Ordering::Relaxed);
        Ok(count)
    }

    pub async fn check_and_flush(&self) -> Result<()> {
        let _ = self.rotator.lock().await.check_and_flush().await?;
        Ok(())
    }

    pub async fn shutdown(&self) -> Result<()> {
        let mut rotator = self.rotator.lock().await;
        let _ = rotator.flush().await;
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

/// Public service for ingesting OTLP logs.
pub struct LogIngestionService {
    rotator: Arc<Mutex<LogRotator>>,
    stats: Arc<IngestionStats>,
    memory_buffer: Option<MemoryBuffer>,
}

impl LogIngestionService {
    pub fn new(config: LogBlockConfig, metadata_tx: mpsc::UnboundedSender<BlockMetadata>) -> Self {
        Self {
            rotator: Arc::new(Mutex::new(LogRotator::new(config, metadata_tx))),
            stats: Arc::new(IngestionStats::default()),
            memory_buffer: None,
        }
    }

    /// Set the shared memory buffer for stream-queryable data.
    pub fn with_memory_buffer(mut self, buffer: MemoryBuffer) -> Self {
        self.memory_buffer = Some(buffer);
        self
    }

    pub async fn ingest_proto(&self, body: Bytes) -> Result<u64> {
        self.stats.total_batches.fetch_add(1, Ordering::Relaxed);
        let req = ExportLogsServiceRequest::decode(body)
            .map_err(|e| Error::Validation(format!("Protobuf decode error: {}", e)))?;
        let logs = OtlpDecoder::decode_logs(req).inspect_err(|_| {
            self.stats.failed_batches.fetch_add(1, Ordering::Relaxed);
        })?;
        self.process_logs(logs).await
    }

    pub async fn ingest_json(&self, body: Bytes) -> Result<u64> {
        self.stats.total_batches.fetch_add(1, Ordering::Relaxed);
        let json: serde_json::Value = serde_json::from_slice(&body)
            .map_err(|e| Error::Validation(format!("JSON parse error: {}", e)))?;
        let logs = OtlpDecoder::decode_logs_json(json).inspect_err(|_| {
            self.stats.failed_batches.fetch_add(1, Ordering::Relaxed);
        })?;
        self.process_logs(logs).await
    }

    async fn process_logs(&self, logs: Vec<LogRecord>) -> Result<u64> {
        let count = logs.len() as u64;
        let mut flushed = false;
        // Write to in-memory buffer first
        if let Some(ref buf) = self.memory_buffer {
            buf.push_logs(&logs).await;
        }
        let mut rotator = self.rotator.lock().await;
        for l in logs {
            if rotator.push(l).await? {
                flushed = true;
            }
        }
        if rotator.check_and_flush().await? {
            flushed = true;
        }
        drop(rotator);
        if flushed {
            if let Some(ref buf) = self.memory_buffer {
                buf.drain_logs().await;
            }
        }
        self.stats
            .ingested_points
            .fetch_add(count, Ordering::Relaxed);
        Ok(count)
    }

    pub async fn check_and_flush(&self) -> Result<()> {
        let _ = self.rotator.lock().await.check_and_flush().await?;
        Ok(())
    }

    pub async fn shutdown(&self) -> Result<()> {
        let mut rotator = self.rotator.lock().await;
        let _ = rotator.flush().await;
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

/// Handles automatic rotation and flushing of trace blocks.
pub struct TraceRotator {
    writer: TraceWriter,
    config: BlockConfig,
    last_flush: Instant,
    max_duration: Duration,
    metadata_tx: mpsc::UnboundedSender<BlockMetadata>,
}

impl TraceRotator {
    pub fn new(config: BlockConfig, metadata_tx: mpsc::UnboundedSender<BlockMetadata>) -> Self {
        let max_duration = Duration::from_secs(config.block_duration_secs);
        Self {
            writer: TraceWriter::new(config.clone()),
            config,
            last_flush: Instant::now(),
            max_duration,
            metadata_tx,
        }
    }

    pub async fn push(&mut self, span: Span) -> Result<bool> {
        if self.writer.len() + 1 > self.config.max_rows_per_block {
            self.flush().await?;
            return self.writer.push(span).map(|_| true);
        }
        self.writer.push(span).map(|_| false)
    }

    pub async fn check_and_flush(&mut self) -> Result<()> {
        if Instant::now().duration_since(self.last_flush) >= self.max_duration {
            self.flush().await?;
        }
        Ok(())
    }

    /// See [BlockRotator::flush] for the blocking-pool rationale.
    pub async fn flush(&mut self) -> Result<()> {
        if self.writer.is_empty() {
            return Ok(());
        }
        let mut writer = std::mem::replace(&mut self.writer, TraceWriter::new(self.config.clone()));
        let metadata = tokio::task::spawn_blocking(move || writer.flush())
            .await
            .map_err(|e| Error::Internal(format!("flush task panicked: {}", e)))??;
        self.last_flush = Instant::now();
        let _ = self.metadata_tx.send(metadata);
        Ok(())
    }
}

/// Public service for ingesting OTLP traces.
pub struct TraceIngestionService {
    rotator: Arc<Mutex<TraceRotator>>,
    stats: Arc<IngestionStats>,
}

impl TraceIngestionService {
    pub fn new(config: BlockConfig, metadata_tx: mpsc::UnboundedSender<BlockMetadata>) -> Self {
        Self {
            rotator: Arc::new(Mutex::new(TraceRotator::new(config, metadata_tx))),
            stats: Arc::new(IngestionStats::default()),
        }
    }

    pub async fn ingest_proto(&self, body: Bytes) -> Result<u64> {
        self.stats.total_batches.fetch_add(1, Ordering::Relaxed);
        let req = ExportTraceServiceRequest::decode(body)
            .map_err(|e| Error::Validation(format!("Protobuf decode error: {}", e)))?;
        let spans = OtlpDecoder::decode_traces(req).inspect_err(|_| {
            self.stats.failed_batches.fetch_add(1, Ordering::Relaxed);
        })?;
        self.process_traces(spans).await
    }

    pub async fn ingest_json(&self, body: Bytes) -> Result<u64> {
        self.stats.total_batches.fetch_add(1, Ordering::Relaxed);
        let json: serde_json::Value = serde_json::from_slice(&body)
            .map_err(|e| Error::Validation(format!("JSON parse error: {}", e)))?;
        let spans = OtlpDecoder::decode_traces_json(json).inspect_err(|_| {
            self.stats.failed_batches.fetch_add(1, Ordering::Relaxed);
        })?;
        self.process_traces(spans).await
    }

    async fn process_traces(&self, spans: Vec<Span>) -> Result<u64> {
        let count = spans.len() as u64;
        let mut flushed = false;
        let mut rotator = self.rotator.lock().await;
        for s in spans {
            if rotator.push(s).await? {
                flushed = true;
            }
        }
        rotator.check_and_flush().await?;
        self.stats
            .ingested_points
            .fetch_add(count, Ordering::Relaxed);
        Ok(count)
    }

    pub async fn check_and_flush(&self) -> Result<()> {
        let _ = self.rotator.lock().await.check_and_flush().await?;
        Ok(())
    }

    pub async fn shutdown(&self) -> Result<()> {
        let mut rotator = self.rotator.lock().await;
        let _ = rotator.flush().await;
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

#[cfg(test)]
mod tests {
    #![allow(clippy::unwrap_used, clippy::expect_used, clippy::panic)]
    use super::*;
    use serde_json::json;
    use tempfile::tempdir;

    #[tokio::test]
    async fn test_block_rotator_flush() {
        let dir = tempdir().unwrap();
        let config = BlockConfig {
            data_dir: dir.path().to_path_buf(),
            max_rows_per_block: 10,
            block_duration_secs: 1,
            ..Default::default()
        };
        let (tx, mut rx) = mpsc::unbounded_channel();
        let mut rotator = BlockRotator::new(config, tx);

        rotator
            .push(Metric {
                name: "m1".into(),
                kind: parqtel_core::MetricKind::Gauge,
                data_points: vec![parqtel_core::DataPoint::new(
                    100,
                    parqtel_core::MetricValue::Double(1.0),
                    parqtel_core::LabelSet::default(),
                )
                .unwrap()],
                ..Default::default()
            })
            .await
            .unwrap();

        rotator.flush().await.unwrap();
        let meta = rx.recv().await.unwrap();
        assert_eq!(meta.row_count, 1);
        assert!(meta.path.exists());
    }

    #[tokio::test]
    async fn test_ingestion_service_json() {
        let dir = tempdir().unwrap();
        let config = BlockConfig {
            data_dir: dir.path().to_path_buf(),
            max_rows_per_block: 10,
            block_duration_secs: 1,
            ..Default::default()
        };
        let (tx, _rx) = mpsc::unbounded_channel();
        let service = IngestionService::new(config, tx);

        let payload = json!({
            "resourceMetrics": [{
                "scopeMetrics": [{
                    "metrics": [{
                        "name": "test_m",
                        "gauge": {"dataPoints": [{"timeUnixNano": 1000, "asDouble": 1.0}]}
                    }]
                }]
            }]
        });

        let count = service
            .ingest_json(Bytes::from(payload.to_string()))
            .await
            .unwrap();
        assert_eq!(count, 1);
        let (total, failed, ingested) = service.stats();
        assert_eq!(total, 1);
        assert_eq!(failed, 0);
        assert_eq!(ingested, 1);
    }

    #[tokio::test]
    async fn test_log_ingestion_service_json() {
        let dir = tempdir().unwrap();
        let config = LogBlockConfig {
            data_dir: dir.path().to_path_buf(),
            max_rows_per_block: 10,
            block_duration_secs: 1,
            ..Default::default()
        };
        let (tx, _rx) = mpsc::unbounded_channel();
        let service = LogIngestionService::new(config, tx);

        let payload = json!({
            "resourceLogs": [{
                "scopeLogs": [{
                    "logRecords": [{
                        "timeUnixNano": 1000,
                        "body": "test log"
                    }]
                }]
            }]
        });

        let count = service
            .ingest_json(Bytes::from(payload.to_string()))
            .await
            .unwrap();
        assert_eq!(count, 1);
        let (total, failed, ingested) = service.stats();
        assert_eq!(total, 1);
        assert_eq!(failed, 0);
        assert_eq!(ingested, 1);
    }

    #[tokio::test]
    async fn test_trace_ingestion_service_json() {
        let dir = tempdir().unwrap();
        let config = BlockConfig {
            data_dir: dir.path().to_path_buf(),
            max_rows_per_block: 10,
            block_duration_secs: 1,
            ..Default::default()
        };
        let (tx, _rx) = mpsc::unbounded_channel();
        let service = TraceIngestionService::new(config, tx);

        let payload = json!({
            "resource_spans": [{
                "scope_spans": [{
                    "spans": [{
                        "trace_id": "0102030405060708090a0b0c0d0e0f10",
                        "span_id": "0102030405060708",
                        "name": "test-span",
                        "kind": 1,
                        "start_time_unix_nano": "1000",
                        "end_time_unix_nano": "2000"
                    }]
                }]
            }]
        });

        let count = service
            .ingest_json(Bytes::from(payload.to_string()))
            .await
            .unwrap();
        assert_eq!(count, 1);
        let (total, failed, ingested) = service.stats();
        assert_eq!(total, 1);
        assert_eq!(failed, 0);
        assert_eq!(ingested, 1);
    }

    #[tokio::test]
    async fn test_trace_ingestion_service_error_paths() {
        let dir = tempdir().unwrap();
        let config = BlockConfig {
            data_dir: dir.path().to_path_buf(),
            max_rows_per_block: 2,
            block_duration_secs: 1,
            ..Default::default()
        };
        let (tx, _rx) = mpsc::unbounded_channel();
        let service = TraceIngestionService::new(config, tx);

        // Test empty body
        let res = service.ingest_json(Bytes::from("")).await;
        assert!(res.is_err());

        // Test invalid JSON
        let res = service.ingest_json(Bytes::from("{invalid json}")).await;
        assert!(res.is_err());

        // Test buffer full (3 spans > capacity of 2)
        let payload = json!({
            "resource_spans": [{
                "scope_spans": [{
                    "spans": [
                        {"trace_id": "0102030405060708090a0b0c0d0e0f10", "span_id": "0102030405060708", "name": "s1", "kind": 1, "start_time_unix_nano": "1000", "end_time_unix_nano": "2000"},
                        {"trace_id": "0102030405060708090a0b0c0d0e0f10", "span_id": "0102030405060708", "name": "s2", "kind": 1, "start_time_unix_nano": "1000", "end_time_unix_nano": "2000"},
                        {"trace_id": "0102030405060708090a0b0c0d0e0f10", "span_id": "0102030405060708", "name": "s3", "kind": 1, "start_time_unix_nano": "1000", "end_time_unix_nano": "2000"}
                    ]
                }]
            }]
        });

        let count = service
            .ingest_json(Bytes::from(payload.to_string()))
            .await
            .unwrap();
        assert_eq!(count, 3);
    }

    #[tokio::test]
    async fn test_ingestion_service_shutdown() {
        let dir = tempdir().unwrap();
        let config = BlockConfig {
            data_dir: dir.path().to_path_buf(),
            max_rows_per_block: 10,
            block_duration_secs: 60,
            ..Default::default()
        };
        let (tx, _rx) = mpsc::unbounded_channel();
        let service = IngestionService::new(config, tx);

        let payload = json!({ "resourceMetrics": [{ "scopeMetrics": [{ "metrics": [{ "name": "m", "gauge": {"dataPoints": [{"timeUnixNano": 1000, "asDouble": 1.0}]} }] }] }] });
        service
            .ingest_json(Bytes::from(payload.to_string()))
            .await
            .unwrap();
        service.shutdown().await.unwrap();
    }

    #[tokio::test]
    async fn test_ingestion_service_invalid_json() {
        let dir = tempdir().unwrap();
        let config = BlockConfig {
            data_dir: dir.path().to_path_buf(),
            max_rows_per_block: 10,
            block_duration_secs: 1,
            ..Default::default()
        };
        let (tx, _rx) = mpsc::unbounded_channel();
        let service = IngestionService::new(config, tx);
        let res = service.ingest_json(Bytes::from("not json")).await;
        assert!(res.is_err());
        let (total, _, _) = service.stats();
        assert_eq!(total, 1);
    }

    #[tokio::test]
    async fn test_ingestion_service_invalid_proto() {
        let dir = tempdir().unwrap();
        let config = BlockConfig {
            data_dir: dir.path().to_path_buf(),
            max_rows_per_block: 10,
            block_duration_secs: 1,
            ..Default::default()
        };
        let (tx, _rx) = mpsc::unbounded_channel();
        let service = IngestionService::new(config, tx);
        let res = service.ingest_proto(Bytes::from_static(b"\xff\xff")).await;
        assert!(res.is_err());
    }

    #[tokio::test]
    async fn test_log_ingestion_service_invalid_proto() {
        let dir = tempdir().unwrap();
        let config = LogBlockConfig {
            data_dir: dir.path().to_path_buf(),
            max_rows_per_block: 10,
            block_duration_secs: 1,
            ..Default::default()
        };
        let (tx, _rx) = mpsc::unbounded_channel();
        let service = LogIngestionService::new(config, tx);
        let res = service.ingest_proto(Bytes::from_static(b"\xff\xff")).await;
        assert!(res.is_err());
    }

    #[tokio::test]
    async fn test_log_ingestion_service_shutdown() {
        let dir = tempdir().unwrap();
        let config = LogBlockConfig {
            data_dir: dir.path().to_path_buf(),
            max_rows_per_block: 10,
            block_duration_secs: 60,
            ..Default::default()
        };
        let (tx, _rx) = mpsc::unbounded_channel();
        let service = LogIngestionService::new(config, tx);
        service.shutdown().await.unwrap();
    }

    #[tokio::test]
    async fn test_trace_ingestion_service_invalid_proto() {
        let dir = tempdir().unwrap();
        let config = BlockConfig {
            data_dir: dir.path().to_path_buf(),
            max_rows_per_block: 10,
            block_duration_secs: 1,
            ..Default::default()
        };
        let (tx, _rx) = mpsc::unbounded_channel();
        let service = TraceIngestionService::new(config, tx);
        let res = service.ingest_proto(Bytes::from_static(b"\xff\xff")).await;
        assert!(res.is_err());
    }

    #[tokio::test]
    async fn test_trace_ingestion_service_shutdown() {
        let dir = tempdir().unwrap();
        let config = BlockConfig {
            data_dir: dir.path().to_path_buf(),
            max_rows_per_block: 10,
            block_duration_secs: 60,
            ..Default::default()
        };
        let (tx, _rx) = mpsc::unbounded_channel();
        let service = TraceIngestionService::new(config, tx);
        service.shutdown().await.unwrap();
    }

    #[tokio::test]
    async fn test_log_rotator_flush() {
        let dir = tempdir().unwrap();
        let config = LogBlockConfig {
            data_dir: dir.path().to_path_buf(),
            max_rows_per_block: 10,
            block_duration_secs: 1,
            ..Default::default()
        };
        let (tx, mut rx) = mpsc::unbounded_channel();
        let mut rotator = LogRotator::new(config, tx);

        let log = parqtel_core::LogRecord::new(
            100,
            100,
            9,
            "INFO".into(),
            "test".into(),
            parqtel_core::LabelSet::default(),
            parqtel_core::LabelSet::default(),
            [0u8; 16],
            [0u8; 8],
            0,
            "".into(),
            "".into(),
        );
        rotator.push(log).await.unwrap();

        rotator.flush().await.unwrap();
        let meta = rx.recv().await.unwrap();
        assert_eq!(meta.row_count, 1);
    }

    #[tokio::test]
    async fn test_check_and_flush_within_duration() {
        let dir = tempdir().unwrap();
        let config = BlockConfig {
            data_dir: dir.path().to_path_buf(),
            max_rows_per_block: 10,
            block_duration_secs: 3600,
            ..Default::default()
        };
        let (tx, _rx) = mpsc::unbounded_channel();
        let mut rotator = BlockRotator::new(config, tx);

        rotator
            .push(parqtel_core::Metric {
                name: "m".into(),
                kind: parqtel_core::MetricKind::Gauge,
                data_points: vec![parqtel_core::DataPoint::new(
                    100,
                    parqtel_core::MetricValue::Double(1.0),
                    parqtel_core::LabelSet::default(),
                )
                .unwrap()],
                ..Default::default()
            })
            .await
            .unwrap();

        // Should not flush since duration hasn't elapsed
        rotator.check_and_flush().await.unwrap();
    }
}
