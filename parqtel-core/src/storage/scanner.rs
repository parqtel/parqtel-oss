use crate::error::{Error, Result};
use crate::models::logs::LogRecord;
use crate::models::metrics::DataPoint;
use crate::models::storage::{BlockMetadata, StorageModel};
use crate::models::traces::Span;
use arrow2::array::{Array, DictionaryArray, Float64Array, Int64Array, Utf8Array};
use arrow2::io::parquet::read;
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
        let mut file = match File::open(&meta.path) {
            Ok(f) => f,
            Err(e) if e.kind() == std::io::ErrorKind::NotFound => {
                tracing::warn!("Block file not found, skipping: {:?}", meta.path);
                return Ok(Vec::new());
            }
            Err(e) => return Err(Error::Io(e)),
        };
        let metadata = read::read_metadata(&mut file).map_err(|e| Error::Parquet(e.to_string()))?;

        // Skip row groups whose timestamp range cannot overlap the query.
        // Timestamp is column 0 with physical type Int64.
        let row_groups = filter_row_groups_by_time(&metadata.row_groups, start_ns, end_ns);
        if row_groups.is_empty() {
            return Ok(Vec::new());
        }

        let schema = StorageModel::metrics_schema();
        let reader = read::FileReader::new(file, row_groups, schema, None, None, None);

        let mut points = Vec::new();
        for chunk in reader {
            let chunk = chunk.map_err(|e| Error::Parquet(e.to_string()))?;
            // Cache keys borrow from this chunk — recreate per chunk.
            let mut labels_cache: HashMap<&str, crate::LabelSet> = HashMap::new();
            let ts = downcast::<Int64Array>(chunk.arrays()[0].as_ref(), "timestamp")?;
            let name_arr = downcast_dict(chunk.arrays()[1].as_ref(), "metric_name")?;
            let name_values = downcast_utf8(name_arr.values().as_ref(), "metric_name values")?;
            let labels_col = downcast_utf8(chunk.arrays()[11].as_ref(), "labels")?;
            let vf = downcast::<Float64Array>(chunk.arrays()[12].as_ref(), "value_float")?;
            let vi = downcast::<Int64Array>(chunk.arrays()[13].as_ref(), "value_int")?;

            for row in 0..chunk.len() {
                // Cheap filters first: no allocations unless the row matches.
                let t = ts.value(row);
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
                    value: decode_value(Some(vf), Some(vi), chunk.arrays()[14].as_ref(), row)?,
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
        let mut file = match File::open(&meta.path) {
            Ok(f) => f,
            Err(e) if e.kind() == std::io::ErrorKind::NotFound => {
                tracing::warn!("Log block file not found, skipping: {:?}", meta.path);
                return Ok(Vec::new());
            }
            Err(e) => return Err(Error::Io(e)),
        };
        let metadata = read::read_metadata(&mut file).map_err(|e| Error::Parquet(e.to_string()))?;
        let row_groups = filter_row_groups_by_time(&metadata.row_groups, start_ns, end_ns);
        if row_groups.is_empty() {
            return Ok(Vec::new());
        }

        let schema = StorageModel::logs_schema();
        let reader = read::FileReader::new(file, row_groups, schema, None, None, None);

        let mut logs = Vec::new();
        for chunk in reader {
            let chunk = chunk.map_err(|e| Error::Parquet(e.to_string()))?;
            // Cache keys borrow from this chunk — recreate per chunk.
            let mut attr_cache: HashMap<&str, crate::LabelSet> = HashMap::new();
            let mut res_cache: HashMap<&str, crate::LabelSet> = HashMap::new();
            for row in 0..chunk.len() {
                let log = StorageModel::row_to_log(&chunk, row, &mut attr_cache, &mut res_cache)?;
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
                // catch_unwind guards against panics in arrow2 deserialization
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
        let mut file = match File::open(&meta.path) {
            Ok(f) => f,
            Err(e) if e.kind() == std::io::ErrorKind::NotFound => {
                tracing::warn!("Trace block file not found, skipping: {:?}", meta.path);
                return Ok(Vec::new());
            }
            Err(e) => return Err(Error::Io(e)),
        };
        let file_meta =
            read::read_metadata(&mut file).map_err(|e| Error::Parquet(e.to_string()))?;
        let row_groups = filter_row_groups_by_time(&file_meta.row_groups, start_ns, end_ns);
        if row_groups.is_empty() {
            return Ok(Vec::new());
        }

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
                meta.path,
                expected_schema.fields.len(),
                file_schema.fields.len()
            );
            return Ok(Vec::new());
        }

        // Validate FixedSizeBinary columns match expected widths.
        // arrow2 will panic (abort) if page data length doesn't align with declared size.
        for (actual, expected) in file_schema.fields.iter().zip(expected_schema.fields.iter()) {
            use arrow2::datatypes::DataType;
            if let (DataType::FixedSizeBinary(a), DataType::FixedSizeBinary(e)) =
                (&actual.data_type, &expected.data_type)
            {
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
                if let parquet2::schema::types::PhysicalType::FixedLenByteArray(declared_len) =
                    col.physical_type()
                {
                    let num_vals = col.num_values();
                    if num_vals > 0 && col.uncompressed_size() > 0 {
                        // Basic sanity: uncompressed data should be at least num_values * size
                        // (with some tolerance for page headers/encoding overhead)
                        let min_expected_bytes = num_vals * (declared_len as i64);
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

        let reader = read::FileReader::new(file, row_groups, file_schema, None, None, None);

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

fn downcast<'a, T: Array + 'static>(arr: &'a dyn Array, what: &str) -> Result<&'a T> {
    arr.as_any()
        .downcast_ref::<T>()
        .ok_or_else(|| Error::Arrow(format!("Invalid {} column", what)))
}

fn downcast_dict<'a>(arr: &'a dyn Array, what: &str) -> Result<&'a DictionaryArray<i32>> {
    arr.as_any()
        .downcast_ref::<DictionaryArray<i32>>()
        .ok_or_else(|| Error::Arrow(format!("Invalid {} column", what)))
}

fn downcast_utf8<'a>(arr: &'a dyn Array, what: &str) -> Result<&'a Utf8Array<i32>> {
    arr.as_any()
        .downcast_ref::<Utf8Array<i32>>()
        .ok_or_else(|| Error::Arrow(format!("Invalid {} column", what)))
}

/// Decodes a MetricValue from the three value columns without re-downcasting.
fn decode_value(
    vf: Option<&arrow2::array::Float64Array>,
    vi: Option<&arrow2::array::Int64Array>,
    vc: &dyn Array,
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
    let complex = downcast_utf8(vc, "value_complex")?.value(row);
    serde_json::from_str(complex).map_err(Error::Serde)
}

/// Returns the subset of row groups whose column-0 timestamp statistics can
/// overlap [start_ns, end_ns]. Falls back to keeping the group when statistics
/// are absent/unreadable — correctness over pruning.
fn filter_row_groups_by_time(
    row_groups: &[parquet2::metadata::RowGroupMetaData],
    start_ns: i64,
    end_ns: i64,
) -> Vec<parquet2::metadata::RowGroupMetaData> {
    row_groups
        .iter()
        .filter(|rg| {
            rg.columns()
                .first()
                .and_then(|c| c.statistics())
                .and_then(|s| s.ok())
                .and_then(|s| {
                    s.as_any()
                        .downcast_ref::<parquet2::statistics::PrimitiveStatistics<i64>>()
                        .map(|st| st.min_value.zip(st.max_value))
                })
                .map(|stats| match stats {
                    Some((min, max)) => max >= start_ns && min <= end_ns,
                    _ => true,
                })
                .unwrap_or(true)
        })
        .cloned()
        .collect()
}
