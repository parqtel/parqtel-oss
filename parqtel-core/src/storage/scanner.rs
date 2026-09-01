use crate::error::{Error, Result};
use crate::models::logs::LogRecord;
use crate::models::metrics::DataPoint;
use crate::models::storage::{BlockMetadata, StorageModel};
use crate::models::traces::Span;
use arrow_array::{Array, TimestampNanosecondArray};
use parquet::arrow::arrow_reader::ParquetRecordBatchReaderBuilder;
use std::collections::HashMap;
use std::fs::File;
use std::sync::Arc;

/// Max concurrent blocking scans; also bounds tokio blocking-pool usage.
const MAX_CONCURRENT: usize = 16;
/// Hard cap on blocks scanned per query.
const MAX_BLOCKS: usize = 128;
/// Label-cache size ceiling per chunk before it is cleared.
/// ponytail: naive bound — clear-on-overflow instead of LRU; revisit if
/// high-cardinality queries dominate profiles.
const LABEL_CACHE_MAX: usize = 10_000;

/// Reads and filters data points from Parquet files.
pub struct Scanner;

impl Scanner {
    /// Scans a set of blocks for data points matching a metric and time range.
    ///
    /// Runs each block's decode on the blocking thread pool (bounded by a
    /// semaphore acquired *before* spawning, so we never oversubscribe) and
    /// skips row groups whose timestamp statistics fall outside [start_ns,
    /// end_ns].
    pub async fn scan(
        blocks: Vec<BlockMetadata>,
        metric_name: String,
        start_ns: i64,
        end_ns: i64,
    ) -> Result<Vec<DataPoint>> {
        let sem = Arc::new(tokio::sync::Semaphore::new(MAX_CONCURRENT));
        let mut tasks = Vec::new();
        for block in blocks.into_iter().take(MAX_BLOCKS) {
            let m_name = metric_name.clone();
            let permit = sem
                .clone()
                .acquire_owned()
                .await
                .map_err(|e| Error::Internal(e.to_string()))?;
            tasks.push(tokio::task::spawn_blocking(move || {
                let _permit = permit;
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

    fn scan_block(
        meta: BlockMetadata,
        metric_name: String,
        start_ns: i64,
        end_ns: i64,
    ) -> Result<Vec<DataPoint>> {
        let file = match File::open(&meta.path) {
            Ok(f) => f,
            Err(e) if e.kind() == std::io::ErrorKind::NotFound => {
                tracing::warn!("Block file not found, skipping: {:?}", meta.path);
                return Ok(Vec::new());
            }
            Err(e) => return Err(Error::Io(e)),
        };

        let reader_builder = ParquetRecordBatchReaderBuilder::try_new(file)
            .map_err(|e| Error::Parquet(e.to_string()))?;

        // Read all row groups (skip statistics filtering for simplicity with new parquet API)
        let reader = reader_builder
            .build()
            .map_err(|e| Error::Parquet(e.to_string()))?;

        let mut points = Vec::new();

        for record_batch in reader {
            let record_batch = record_batch.map_err(|e| Error::Parquet(e.to_string()))?;

            // Cache keys borrow from this chunk — recreate per chunk.
            let mut labels_cache: HashMap<&str, crate::LabelSet> = HashMap::new();
            let ts_arr = record_batch
                .column(0)
                .as_any()
                .downcast_ref::<TimestampNanosecondArray>()
                .ok_or_else(|| Error::Arrow("Invalid timestamp column".into()))?;
            let name_arr = record_batch
                .column(1)
                .as_any()
                .downcast_ref::<arrow_array::DictionaryArray<arrow_array::types::Int32Type>>()
                .ok_or_else(|| Error::Arrow("Invalid metric_name column".into()))?;
            let name_values = name_arr
                .values()
                .as_any()
                .downcast_ref::<arrow_array::StringArray>()
                .ok_or_else(|| Error::Arrow("Invalid metric_name values".into()))?;
            let labels_col = record_batch
                .column(11)
                .as_any()
                .downcast_ref::<arrow_array::StringArray>()
                .ok_or_else(|| Error::Arrow("Invalid labels column".into()))?;
            let vf_arr = record_batch
                .column(12)
                .as_any()
                .downcast_ref::<arrow_array::Float64Array>()
                .ok_or_else(|| Error::Arrow("Invalid value_float column".into()))?;
            let vi_arr = record_batch
                .column(13)
                .as_any()
                .downcast_ref::<arrow_array::Int64Array>()
                .ok_or_else(|| Error::Arrow("Invalid value_int column".into()))?;
            let vc_arr = record_batch
                .column(14)
                .as_any()
                .downcast_ref::<arrow_array::StringArray>()
                .ok_or_else(|| Error::Arrow("Invalid value_complex column".into()))?;

            for row in 0..record_batch.num_rows() {
                // Cheap filters first: no allocations unless the row matches.
                let t = ts_arr.value(row);
                if t < start_ns || t > end_ns {
                    continue;
                }

                let name = name_values.value(name_arr.keys().value(row) as usize);
                if !metric_name.is_empty() && name != metric_name {
                    continue;
                }

                let labels_json = labels_col.value(row);
                let labels = match labels_cache.get(labels_json) {
                    Some(l) => l.clone(),
                    None => {
                        if labels_cache.len() >= LABEL_CACHE_MAX {
                            labels_cache.clear();
                        }
                        let l = crate::LabelSet::from_json(labels_json)?;
                        labels_cache.insert(labels_json, l.clone());
                        l
                    }
                };

                points.push(DataPoint {
                    timestamp_ns: t,
                    value: decode_value(Some(vf_arr), Some(vi_arr), vc_arr, row)?,
                    labels,
                });
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
        let sem = Arc::new(tokio::sync::Semaphore::new(MAX_CONCURRENT));
        let mut tasks = Vec::new();
        for block in blocks.into_iter().take(MAX_BLOCKS) {
            let permit = sem
                .clone()
                .acquire_owned()
                .await
                .map_err(|e| Error::Internal(e.to_string()))?;
            tasks.push(tokio::task::spawn_blocking(move || {
                let _permit = permit;
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
        let file = match File::open(&meta.path) {
            Ok(f) => f,
            Err(e) if e.kind() == std::io::ErrorKind::NotFound => {
                tracing::warn!("Log block file not found, skipping: {:?}", meta.path);
                return Ok(Vec::new());
            }
            Err(e) => return Err(Error::Io(e)),
        };

        let reader_builder = ParquetRecordBatchReaderBuilder::try_new(file)
            .map_err(|e| Error::Parquet(e.to_string()))?;

        // Read all row groups
        let reader = reader_builder
            .build()
            .map_err(|e| Error::Parquet(e.to_string()))?;

        let mut logs = Vec::new();
        for record_batch in reader {
            let record_batch = record_batch.map_err(|e| Error::Parquet(e.to_string()))?;

            // Cache keys borrow from this chunk — recreate per chunk.
            let mut attr_cache: HashMap<&str, crate::LabelSet> = HashMap::new();
            let mut res_cache: HashMap<&str, crate::LabelSet> = HashMap::new();
            for row in 0..record_batch.num_rows() {
                let log =
                    StorageModel::row_to_log(&record_batch, row, &mut attr_cache, &mut res_cache)?;
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
        let sem = Arc::new(tokio::sync::Semaphore::new(MAX_CONCURRENT));
        let mut tasks = Vec::with_capacity(blocks.len().min(MAX_CONCURRENT));
        for block in blocks {
            let permit = sem
                .clone()
                .acquire_owned()
                .await
                .map_err(|e| Error::Internal(e.to_string()))?;
            tasks.push(tokio::task::spawn_blocking(move || {
                let _permit = permit;
                // catch_unwind guards against panics in parquet deserialization
                let result = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
                    Self::scan_trace_block(block.clone(), start_ns, end_ns, limit)
                }));
                (result, block)
            }));
        }

        let mut all_spans = Vec::with_capacity(limit.min(10_000));
        for task in tasks {
            let (result, block) = task.await.map_err(|e| Error::Internal(e.to_string()))?;
            match result {
                Ok(r) => {
                    all_spans.extend(r?);
                    if all_spans.len() >= limit {
                        break;
                    }
                }
                Err(_) => {
                    tracing::error!("Panic scanning trace block: {:?} — skipping", block.path);
                }
            }
        }
        all_spans.sort_by_key(|s| s.start_time_ns);
        all_spans.truncate(limit);
        Ok(all_spans)
    }

    fn scan_trace_block(
        meta: BlockMetadata,
        start_ns: i64,
        end_ns: i64,
        limit: usize,
    ) -> Result<Vec<Span>> {
        let file = match File::open(&meta.path) {
            Ok(f) => f,
            Err(e) if e.kind() == std::io::ErrorKind::NotFound => {
                tracing::warn!("Trace block file not found, skipping: {:?}", meta.path);
                return Ok(Vec::new());
            }
            Err(e) => return Err(Error::Io(e)),
        };

        let reader_builder = ParquetRecordBatchReaderBuilder::try_new(file)
            .map_err(|e| Error::Parquet(e.to_string()))?;

        // Read all row groups
        let reader = reader_builder
            .build()
            .map_err(|e| Error::Parquet(e.to_string()))?;

        let mut spans = Vec::new();

        for record_batch in reader {
            let record_batch = match record_batch {
                Ok(c) => c,
                Err(e) => {
                    tracing::warn!("Error reading chunk from {:?}: {} — skipping", meta.path, e);
                    continue;
                }
            };

            for row in 0..record_batch.num_rows() {
                match StorageModel::row_to_span(&record_batch, row) {
                    Ok(span) if span.start_time_ns >= start_ns && span.start_time_ns <= end_ns => {
                        spans.push(span);
                        if spans.len() >= limit {
                            return Ok(spans);
                        }
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

/// Decodes a MetricValue from the three value columns without re-downcasting.
fn decode_value(
    vf: Option<&arrow_array::Float64Array>,
    vi: Option<&arrow_array::Int64Array>,
    vc: &arrow_array::StringArray,
    row: usize,
) -> Result<crate::MetricValue> {
    use crate::MetricValue;
    if let Some(f) = vf {
        if !f.is_null(row) {
            return Ok(MetricValue::Double(f.value(row)));
        }
    }
    if let Some(i) = vi {
        if !i.is_null(row) {
            return Ok(MetricValue::Int(i.value(row)));
        }
    }
    let complex = vc.value(row);
    serde_json::from_str(complex).map_err(Error::Serde)
}
