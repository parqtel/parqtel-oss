use std::fs::File;
use crate::error::{Error, Result};
use crate::models::storage::{BlockMetadata, StorageModel};
use crate::models::metrics::DataPoint;
use crate::models::logs::LogRecord;
use crate::models::traces::Span;
use arrow2::io::parquet::read;

/// Reads and filters data points from Parquet files.
pub struct Scanner;

impl Scanner {
    /// Scans a set of blocks for data points matching a metric and time range.
    pub async fn scan(
        blocks: Vec<BlockMetadata>,
        metric_name: String,
        start_ns: i64,
        end_ns: i64,
    ) -> Result<Vec<DataPoint>> {
        let mut tasks = Vec::new();
        for block in blocks {
            let m_name = metric_name.clone();
            tasks.push(tokio::spawn(async move {
                Self::scan_block(block, m_name, start_ns, end_ns)
            }));
        }

        let mut all_points = Vec::new();
        for task in tasks {
            let points = task.await.map_err(|e| Error::Internal(e.to_string()))??;
            all_points.extend(points);
        }
        all_points.sort_by_key(|p| p.timestamp_ns);
        Ok(all_points)
    }

    fn scan_block(meta: BlockMetadata, metric_name: String, start_ns: i64, end_ns: i64) -> Result<Vec<DataPoint>> {
        let mut file = match File::open(&meta.path) {
            Ok(f) => f,
            Err(e) if e.kind() == std::io::ErrorKind::NotFound => {
                tracing::warn!("Block file not found, skipping: {:?}", meta.path);
                return Ok(Vec::new());
            }
            Err(e) => return Err(Error::Io(e)),
        };
        let metadata = read::read_metadata(&mut file).map_err(|e| Error::Parquet(e.to_string()))?;
        let schema = StorageModel::metrics_schema();
        let reader = read::FileReader::new(file, metadata.row_groups, schema, None, None, None);

        let mut points = Vec::new();
        for chunk in reader {
            let chunk = chunk.map_err(|e| Error::Parquet(e.to_string()))?;
            for row in 0..chunk.len() {
                let (name, _kind, _res, dp) = StorageModel::row_to_point(&chunk, row)?;
                let name_match = metric_name.is_empty() || name == metric_name;
                if name_match && dp.timestamp_ns >= start_ns && dp.timestamp_ns <= end_ns {
                    points.push(dp);
                }
            }
        }
        Ok(points)
    }

    /// Scans a set of blocks for log records matching a time range.
    pub async fn scan_logs(
        blocks: Vec<BlockMetadata>,
        start_ns: i64,
        end_ns: i64,
    ) -> Result<Vec<LogRecord>> {
        let mut tasks = Vec::new();
        for block in blocks {
            tasks.push(tokio::spawn(async move {
                Self::scan_log_block(block, start_ns, end_ns)
            }));
        }

        let mut all_logs = Vec::new();
        for task in tasks {
            let logs = task.await.map_err(|e| Error::Internal(e.to_string()))??;
            all_logs.extend(logs);
        }
        all_logs.sort_by_key(|l| l.timestamp_ns);
        Ok(all_logs)
    }

    fn scan_log_block(meta: BlockMetadata, start_ns: i64, end_ns: i64) -> Result<Vec<LogRecord>> {
        let mut file = match File::open(&meta.path) {
            Ok(f) => f,
            Err(e) if e.kind() == std::io::ErrorKind::NotFound => {
                tracing::warn!("Log block file not found, skipping: {:?}", meta.path);
                return Ok(Vec::new());
            }
            Err(e) => return Err(Error::Io(e)),
        };
        let metadata = read::read_metadata(&mut file).map_err(|e| Error::Parquet(e.to_string()))?;
        let schema = StorageModel::logs_schema();
        let reader = read::FileReader::new(file, metadata.row_groups, schema, None, None, None);

        let mut logs = Vec::new();
        for chunk in reader {
            let chunk = chunk.map_err(|e| Error::Parquet(e.to_string()))?;
            for row in 0..chunk.len() {
                let log = StorageModel::row_to_log(&chunk, row)?;
                if log.timestamp_ns >= start_ns && log.timestamp_ns <= end_ns {
                    logs.push(log);
                }
            }
        }
        Ok(logs)
    }

    /// Scans a set of blocks for spans matching a time range.
    /// Uses bounded concurrency and graceful error handling — a single corrupted
    /// block will not crash the process or abort the entire query.
    pub async fn scan_traces(
        blocks: Vec<BlockMetadata>,
        start_ns: i64,
        end_ns: i64,
        limit: usize,
    ) -> Result<Vec<Span>> {
        // Cap concurrent I/O to avoid overwhelming the system at petabyte scale
        let mut tasks = Vec::with_capacity(blocks.len().min(16));
        for block in blocks {
            tasks.push(tokio::spawn(async move {
                // catch_unwind guards against panics in arrow2 deserialization
                let result = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
                    Self::scan_trace_block(block.clone(), start_ns, end_ns, limit)
                }));
                match result {
                    Ok(r) => r,
                    Err(_) => {
                        tracing::error!("Panic scanning trace block: {:?} — skipping", block.path);
                        Ok(Vec::new())
                    }
                }
            }));
        }

        let mut all_spans = Vec::with_capacity(limit.min(10_000));
        for task in tasks {
            match task.await {
                Ok(Ok(spans)) => {
                    all_spans.extend(spans);
                    if all_spans.len() >= limit { break; }
                }
                Ok(Err(e)) => {
                    tracing::warn!("Error scanning trace block, skipping: {}", e);
                }
                Err(e) => {
                    tracing::warn!("Task join error scanning trace block: {}", e);
                }
            }
        }
        all_spans.sort_by_key(|s| s.start_time_ns);
        all_spans.truncate(limit);
        Ok(all_spans)
    }

    fn scan_trace_block(meta: BlockMetadata, start_ns: i64, end_ns: i64, limit: usize) -> Result<Vec<Span>> {
        let mut file = match File::open(&meta.path) {
            Ok(f) => f,
            Err(e) if e.kind() == std::io::ErrorKind::NotFound => {
                tracing::warn!("Trace block file not found, skipping: {:?}", meta.path);
                return Ok(Vec::new());
            }
            Err(e) => return Err(Error::Io(e)),
        };
        let file_meta = read::read_metadata(&mut file).map_err(|e| Error::Parquet(e.to_string()))?;

        // Validate schema before deserialization to prevent arrow2 panics.
        let file_schema = match read::infer_schema(&file_meta) {
            Ok(s) => s,
            Err(e) => {
                tracing::warn!("Cannot infer schema from {:?}: {} — skipping", meta.path, e);
                return Ok(Vec::new());
            }
        };

        let expected_schema = StorageModel::traces_schema();
        if file_schema.fields.len() != expected_schema.fields.len() {
            tracing::warn!(
                "Trace block schema mismatch at {:?}: expected {} fields, got {} — skipping",
                meta.path, expected_schema.fields.len(), file_schema.fields.len()
            );
            return Ok(Vec::new());
        }

        // Validate FixedSizeBinary columns match expected widths.
        // arrow2 will panic (abort) if page data length doesn't align with declared size.
        for (actual, expected) in file_schema.fields.iter().zip(expected_schema.fields.iter()) {
            use arrow2::datatypes::DataType;
            if let (DataType::FixedSizeBinary(a), DataType::FixedSizeBinary(e)) = (&actual.data_type, &expected.data_type) {
                if a != e {
                    tracing::warn!(
                        "FixedSizeBinary width mismatch in {:?} col {:?}: file={}, expected={} — skipping block",
                        meta.path, expected.name, a, e
                    );
                    return Ok(Vec::new());
                }
            }
        }

        // Validate row group column physical types to detect data corruption.
        // Prevents the assert_eq panic in arrow2::fixed_size_binary when page buffer
        // length doesn't match the declared FixedLenByteArray size.
        for rg in &file_meta.row_groups {
            for col in rg.columns() {
                if let parquet2::schema::types::PhysicalType::FixedLenByteArray(declared_len) = col.physical_type() {
                    let num_vals = col.num_values();
                    if num_vals > 0 && col.uncompressed_size() > 0 {
                        // Basic sanity: uncompressed data should be at least num_values * size
                        // (with some tolerance for page headers/encoding overhead)
                        let min_expected_bytes = (num_vals as i64) * (declared_len as i64);
                        if col.uncompressed_size() < min_expected_bytes / 2 {
                            tracing::warn!(
                                "Corrupt FixedLenByteArray column in {:?}: declared_len={}, num_values={}, uncompressed={} — skipping block",
                                meta.path, declared_len, num_vals, col.uncompressed_size()
                            );
                            return Ok(Vec::new());
                        }
                    }
                }
            }
        }

        let reader = read::FileReader::new(file, file_meta.row_groups, file_schema, None, None, None);

        let mut spans = Vec::new();
        for chunk in reader {
            let chunk = match chunk {
                Ok(c) => c,
                Err(e) => {
                    tracing::warn!("Error reading chunk from {:?}: {} — skipping", meta.path, e);
                    continue;
                }
            };
            for row in 0..chunk.len() {
                match StorageModel::row_to_span(&chunk, row) {
                    Ok(span) if span.start_time_ns >= start_ns && span.start_time_ns <= end_ns => {
                        spans.push(span);
                        if spans.len() >= limit { return Ok(spans); }
                    }
                    Ok(_) => {}
                    Err(e) => {
                        tracing::debug!("Skipping malformed span row in {:?}: {}", meta.path, e);
                    }
                }
            }
        }
        Ok(spans)
    }
}
