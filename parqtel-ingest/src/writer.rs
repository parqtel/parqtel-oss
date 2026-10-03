pub use parqtel_core::BlockMetadata;
use parqtel_core::{
    compression_from_name, BlockConfig, DataPoint, Error, LabelSet, LogBlockConfig, LogRecord,
    Metric, MetricKind, Result, Span, StorageModel,
};
use parquet::arrow::ArrowWriter;
use parquet::file::properties::{EnabledStatistics, WriterProperties, WriterVersion};
use parquet::schema::types::ColumnPath;
use std::collections::{BTreeMap, HashSet};
use std::fs::{self, File};
use std::path::Path;
use std::sync::Arc;
use uuid::Uuid;

/// Buffers metrics in memory and flushes them to Parquet blocks.
pub struct BlockWriter {
    config: BlockConfig,
    buffer: Vec<DataPointContext>,
    capacity: usize,
}

struct DataPointContext {
    name: Arc<String>,
    kind: MetricKind,
    resource: Arc<LabelSet>,
    dp: DataPoint,
}

impl BlockWriter {
    pub fn new(config: BlockConfig) -> Self {
        let capacity = config.max_rows_per_block;
        // Grow on demand instead of pre-allocating `max_rows_per_block`
        // slots: at 1M rows × ~100 bytes/ctx this parks hundreds of MB per
        // writer in the allocator, and every flush swaps in a fresh writer.
        Self {
            config,
            buffer: Vec::new(),
            capacity,
        }
    }

    /// Appends a metric, splitting it across blocks when it does not fit.
    ///
    /// A single metric can carry more points than a whole block (a high-card
    /// burst), which the previous code handled by pushing points until the
    /// buffer filled and then returning an error — leaving the request
    /// *partially* accepted and telling the client to retry a batch that was
    /// already half-ingested. Splitting keeps the invariant the caller relies
    /// on: every point handed to `push` is either fully accepted into a block
    /// or reported as an error, never both.
    ///
    /// Returns the block metadata of each block written along the way.
    pub fn push(&mut self, metric: Metric) -> Result<Vec<BlockMetadata>> {
        let name = Arc::new(metric.name);
        let resource = Arc::new(metric.resource_attributes);
        let kind = metric.kind;

        let mut flushed = Vec::new();
        let mut points = metric.data_points.into_iter().peekable();
        while points.peek().is_some() {
            let room = self.capacity.saturating_sub(self.buffer.len());
            if room == 0 {
                flushed.push(self.flush()?);
                continue;
            }
            let take = room.min(points.len());
            for dp in points.by_ref().take(take) {
                self.buffer.push(DataPointContext {
                    name: name.clone(),
                    kind,
                    resource: resource.clone(),
                    dp,
                });
            }
        }
        Ok(flushed)
    }

    /// Combines several shard writers into one, so a sharded rotator still
    /// produces a **single** Parquet block per flush.
    ///
    /// Sharding the ingest lock without merging here would multiply the block
    /// count — and therefore query fan-out — by the shard count, trading one
    /// problem for a worse one. Merging keeps the on-disk shape identical to
    /// the unsharded rotator: same file count, same size, same row-group
    /// layout.
    ///
    /// Shard writers hold disjoint metric sets by construction (a metric is
    /// always routed to the same shard), so no deduplication is needed; the
    /// merged writer simply re-groups on flush as it always did.
    pub fn merge(writers: Vec<BlockWriter>) -> Result<BlockWriter> {
        let mut config: Option<BlockConfig> = None;
        let mut buffer = Vec::new();
        for mut w in writers {
            config = Some(w.config.clone());
            buffer.append(&mut w.buffer);
        }
        let config =
            config.ok_or_else(|| Error::Internal("Cannot merge an empty writer set".into()))?;
        let capacity = config.max_rows_per_block;
        Ok(BlockWriter {
            config,
            buffer,
            capacity,
        })
    }

    pub fn len(&self) -> usize {
        self.buffer.len()
    }
    pub fn is_empty(&self) -> bool {
        self.buffer.is_empty()
    }

    pub fn flush(&mut self) -> Result<BlockMetadata> {
        if self.buffer.is_empty() {
            return Err(Error::Internal("Cannot flush empty buffer".into()));
        }
        self.buffer.sort_by_key(|ctx| ctx.dp.timestamp_ns);

        let start_ts = self.buffer[0].dp.timestamp_ns;
        let end_ts = self
            .buffer
            .last()
            .ok_or_else(|| Error::Internal("Buffer empty".into()))?
            .dp
            .timestamp_ns;
        let row_count = self.buffer.len();

        let mut metric_names = HashSet::new();
        let mut label_names = HashSet::new();
        // G5 label-value index for the metrics signal as well.
        const MAX_VALUES_PER_FIELD: usize = 10_000;
        let mut label_values: std::collections::BTreeMap<
            String,
            std::collections::BTreeSet<String>,
        > = std::collections::BTreeMap::new();
        for ctx in &self.buffer {
            metric_names.insert((*ctx.name).clone());
            for label in ctx.resource.keys() {
                label_names.insert(label.clone());
                let entry = label_values.entry(label.clone()).or_default();
                if entry.len() < MAX_VALUES_PER_FIELD {
                    entry.insert(
                        ctx.resource
                            .get(label)
                            .map(|s| s.to_string())
                            .unwrap_or_default(),
                    );
                }
            }
            for label in ctx.dp.labels.keys() {
                label_names.insert(label.clone());
                let entry = label_values.entry(label.clone()).or_default();
                if entry.len() < MAX_VALUES_PER_FIELD {
                    entry.insert(
                        ctx.dp
                            .labels
                            .get(label)
                            .map(|s| s.to_string())
                            .unwrap_or_default(),
                    );
                }
            }
        }

        let metrics = self.reconstruct_metrics()?;
        let chunk = StorageModel::metrics_to_chunk(&metrics)?;
        let filename = format!(
            "{}_{}_{}.parquet",
            start_ts,
            end_ts,
            Uuid::new_v4().simple()
        );
        let final_path = self.config.data_dir.join(&filename);
        let tmp_path = self.config.data_dir.join(format!(".tmp_{}", filename));

        fs::create_dir_all(&self.config.data_dir)?;
        write_parquet_file(
            &tmp_path,
            chunk,
            &self.config.compression,
            self.config.compression_level,
            self.config.row_group_size,
        )?;
        fs::rename(&tmp_path, &final_path)?;
        let size_bytes = fs::metadata(&final_path)?.len();
        self.buffer.clear();

        Ok(BlockMetadata {
            path: final_path,
            start_timestamp_ns: start_ts,
            end_timestamp_ns: end_ts,
            row_count,
            size_bytes,
            metric_names,
            label_names,
            label_values,
            signal_type: parqtel_core::models::storage::SignalType::Metrics,
        })
    }

    fn reconstruct_metrics(&self) -> Result<Vec<Metric>> {
        let mut groups: BTreeMap<(String, MetricKind, LabelSet), Vec<DataPoint>> = BTreeMap::new();
        for ctx in &self.buffer {
            let key = ((*ctx.name).clone(), ctx.kind, (*ctx.resource).clone());
            groups.entry(key).or_default().push(ctx.dp.clone());
        }
        Ok(groups
            .into_iter()
            .map(|((name, kind, resource), dps)| Metric {
                name,
                description: String::new(),
                unit: String::new(),
                kind,
                resource_attributes: resource,
                data_points: dps,
            })
            .collect())
    }
}

/// Buffers logs in memory and flushes them to Parquet blocks.
pub struct LogWriter {
    config: LogBlockConfig,
    buffer: Vec<LogRecord>,
    capacity: usize,
}

impl LogWriter {
    pub fn new(config: LogBlockConfig) -> Self {
        let capacity = config.max_rows_per_block;
        // Grown on demand — a 200K-row pre-allocation is tens of MB parked
        // per writer and re-allocated on every rotator flush swap.
        Self {
            config,
            buffer: Vec::new(),
            capacity,
        }
    }

    /// Appends a log record, flushing first when the buffer is full.
    ///
    /// Mirrors [`BlockWriter::push`]: rather than rejecting the record the
    /// caller already pushed, the block is closed and the record starts the
    /// next one, so a full buffer can never fail a request.
    pub fn push(&mut self, log: LogRecord) -> Result<Option<BlockMetadata>> {
        let mut flushed = None;
        if self.buffer.len() >= self.capacity {
            flushed = Some(self.flush()?);
        }
        self.buffer.push(log);
        Ok(flushed)
    }

    pub fn len(&self) -> usize {
        self.buffer.len()
    }
    pub fn is_empty(&self) -> bool {
        self.buffer.is_empty()
    }

    pub fn flush(&mut self) -> Result<BlockMetadata> {
        if self.buffer.is_empty() {
            return Err(Error::Internal("Cannot flush empty buffer".into()));
        }
        self.buffer.sort_by_key(|log| log.timestamp_ns);

        let start_ts = self.buffer[0].timestamp_ns;
        let end_ts = self
            .buffer
            .last()
            .ok_or_else(|| Error::Internal("Buffer empty".into()))?
            .timestamp_ns;
        let row_count = self.buffer.len();

        let mut label_names = HashSet::new();
        // G5 label-value index: distinct values per field collected at
        // flush time so label-value queries stop decoding whole blocks.
        // Capped per field to bound metadata size.
        const MAX_VALUES_PER_FIELD: usize = 10_000;
        let mut label_values: std::collections::BTreeMap<
            String,
            std::collections::BTreeSet<String>,
        > = std::collections::BTreeMap::new();
        for log in &self.buffer {
            for label in log.attributes.keys() {
                label_names.insert(label.clone());
                let entry = label_values.entry(label.clone()).or_default();
                if entry.len() < MAX_VALUES_PER_FIELD {
                    entry.insert(
                        log.attributes
                            .get(label)
                            .map(|s| s.to_string())
                            .unwrap_or_default(),
                    );
                }
            }
            for label in log.resource_attributes.keys() {
                label_names.insert(label.clone());
                let entry = label_values.entry(label.clone()).or_default();
                if entry.len() < MAX_VALUES_PER_FIELD {
                    entry.insert(
                        log.resource_attributes
                            .get(label)
                            .map(|s| s.to_string())
                            .unwrap_or_default(),
                    );
                }
            }
        }

        let chunk = StorageModel::logs_to_chunk(&self.buffer)?;
        let filename = format!(
            "logs_{}_{}_{}.parquet",
            start_ts,
            end_ts,
            Uuid::new_v4().simple()
        );
        let final_path = self.config.data_dir.join(&filename);
        let tmp_path = self.config.data_dir.join(format!(".tmp_{}", filename));

        fs::create_dir_all(&self.config.data_dir)?;
        write_parquet_file(
            &tmp_path,
            chunk,
            &self.config.compression,
            self.config.compression_level,
            self.config.row_group_size,
        )?;
        fs::rename(&tmp_path, &final_path)?;
        let size_bytes = fs::metadata(&final_path)?.len();
        self.buffer.clear();

        Ok(BlockMetadata {
            path: final_path,
            start_timestamp_ns: start_ts,
            end_timestamp_ns: end_ts,
            row_count,
            size_bytes,
            metric_names: HashSet::new(),
            label_names,
            label_values,
            signal_type: parqtel_core::models::storage::SignalType::Logs,
        })
    }
}

/// Buffers traces in memory and flushes them to Parquet blocks.
pub struct TraceWriter {
    config: BlockConfig,
    buffer: Vec<Span>,
    capacity: usize,
}

impl TraceWriter {
    pub fn new(config: BlockConfig) -> Self {
        let capacity = config.max_rows_per_block;
        // Span is a large struct (inline arrays + Strings + LabelSet + Vec
        // events/links); at 1M rows a pre-allocation here reserves a
        // multi-hundred-MB chunk every flush cycle. Grow on demand.
        Self {
            config,
            buffer: Vec::new(),
            capacity,
        }
    }

    /// Appends a span, flushing first when the buffer is full.
    ///
    /// Mirrors [`LogWriter::push`].
    pub fn push(&mut self, span: Span) -> Result<Option<BlockMetadata>> {
        let mut flushed = None;
        if self.buffer.len() >= self.capacity {
            flushed = Some(self.flush()?);
        }
        self.buffer.push(span);
        Ok(flushed)
    }

    pub fn len(&self) -> usize {
        self.buffer.len()
    }
    pub fn is_empty(&self) -> bool {
        self.buffer.is_empty()
    }

    pub fn flush(&mut self) -> Result<BlockMetadata> {
        if self.buffer.is_empty() {
            return Err(Error::Internal("Cannot flush empty buffer".into()));
        }
        self.buffer.sort_by_key(|span| span.start_time_ns);

        let start_ts = self.buffer[0].start_time_ns;
        let end_ts = self
            .buffer
            .last()
            .ok_or_else(|| Error::Internal("Buffer empty".into()))?
            .end_time_ns;
        let row_count = self.buffer.len();

        let mut label_names = HashSet::new();
        for span in &self.buffer {
            for label in span.attributes.keys() {
                label_names.insert(label.clone());
            }
        }

        let chunk = StorageModel::traces_to_chunk(&self.buffer)?;
        let filename = format!(
            "traces_{}_{}_{}.parquet",
            start_ts,
            end_ts,
            Uuid::new_v4().simple()
        );
        let final_path = self.config.data_dir.join(&filename);
        let tmp_path = self.config.data_dir.join(format!(".tmp_{}", filename));

        fs::create_dir_all(&self.config.data_dir)?;
        write_parquet_file(
            &tmp_path,
            chunk,
            &self.config.compression,
            self.config.compression_level,
            self.config.row_group_size,
        )?;
        fs::rename(&tmp_path, &final_path)?;
        let size_bytes = fs::metadata(&final_path)?.len();
        self.buffer.clear();

        Ok(BlockMetadata {
            path: final_path,
            start_timestamp_ns: start_ts,
            end_timestamp_ns: end_ts,
            row_count,
            size_bytes,
            metric_names: HashSet::new(),
            label_names,
            label_values: Default::default(),
            signal_type: parqtel_core::models::storage::SignalType::Traces,
        })
    }
}

/// Columns that carry a bloom filter.
///
/// Chosen because they are the columns a query filters on *before* it can skip
/// anything: the metric name selects which blocks are relevant at all, and the
/// service name is the second-level discriminator. `timestamp_ns` is already
/// covered exactly by row-group statistics, which are cheaper and exact, so a
/// bloom filter there would add size for no pruning power.
const BLOOM_METRIC_NAME: &str = "metric_name";
const BLOOM_SERVICE_NAME: &str = "service_name";

/// Bloom filter false-positive probability.
///
/// The reader treats a positive as "the value might be present" and decodes the
/// row group anyway, so this only affects how often that happens — never
/// correctness.
const BLOOM_FPP: f64 = 0.01;

/// Truncation length for the column index, in bytes.
///
/// Long enough to keep a full path, short enough that a pathological label
/// value cannot bloat the footer.
const COLUMN_INDEX_TRUNCATE_LENGTH: usize = 64;

/// Target data page size, in bytes.
///
/// Smaller than the Parquet default so the column index has pages finer than
/// `row_group_size` to point at.
const DATA_PAGE_SIZE_LIMIT: usize = 512 * 1024;

/// Writes one block to Parquet, splitting it into `row_group_size`-row groups.
///
/// `row_group_size` is not cosmetic: the scanner skips whole row groups whose
/// timestamp statistics fall outside a query window, so this is what decides
/// whether a narrow query decodes a slice of the block or all of it. It comes
/// from `BlockConfig::row_group_size` (100k for metrics, 20k for logs by
/// default). A zero would be rejected by Parquet itself, so it is clamped.
fn write_parquet_file(
    path: &Path,
    record_batch: arrow::record_batch::RecordBatch,
    compression: &str,
    compression_level: Option<i32>,
    row_group_size: usize,
) -> Result<()> {
    let file = File::create(path)?;

    let writer_props = WriterProperties::builder()
        .set_compression(compression_from_name(compression, compression_level))
        .set_writer_version(WriterVersion::PARQUET_2_0)
        .set_max_row_group_row_count(Some(row_group_size.max(1)))
        // A bloom filter per row group lets the reader reject a row group for a
        // metric it does not contain without decoding any page. A metrics block
        // holds many metric names, so time-range statistics alone cannot narrow
        // a single-metric query: every row group overlaps in time and all of
        // them were being decoded.
        // Per-column, so only these two pay the size cost. Enabling it for
        // every column would bloat blocks with filters nothing reads.
        .set_column_bloom_filter_enabled(ColumnPath::from(BLOOM_METRIC_NAME), true)
        .set_column_bloom_filter_enabled(ColumnPath::from(BLOOM_SERVICE_NAME), true)
        // Default false-positive probability. Named so the trade is visible:
        // a filter this good costs roughly 10 bits per distinct value.
        .set_bloom_filter_fpp(BLOOM_FPP)
        // Page-level statistics plus a bounded column index. Row-group
        // statistics are all-or-nothing at `row_group_size` granularity; the
        // column index lets a reader skip a *page* inside a group.
        .set_statistics_enabled(EnabledStatistics::Page)
        .set_column_index_truncate_length(Some(COLUMN_INDEX_TRUNCATE_LENGTH))
        // Bound the page size so the column index has finer granularity than a
        // whole row group would otherwise give.
        .set_data_page_size_limit(DATA_PAGE_SIZE_LIMIT)
        .build();

    let mut writer = ArrowWriter::try_new(file, record_batch.schema(), Some(writer_props))
        .map_err(|e| Error::Parquet(e.to_string()))?;

    writer
        .write(&record_batch)
        .map_err(|e| Error::Parquet(e.to_string()))?;
    writer.close().map_err(|e| Error::Parquet(e.to_string()))?;
    Ok(())
}

#[cfg(test)]
mod tests {
    #![allow(clippy::unwrap_used, clippy::expect_used, clippy::panic)]
    use super::*;
    use parqtel_core::{DataPoint, LabelSet, Metric, MetricKind, MetricValue};
    use tempfile::tempdir;

    #[test]
    fn test_block_writer_flush() {
        let dir = tempdir().unwrap();
        let config = BlockConfig {
            data_dir: dir.path().to_path_buf(),
            ..Default::default()
        };
        let mut writer = BlockWriter::new(config);
        let metric = Metric {
            name: "test_metric".into(),
            kind: MetricKind::Gauge,
            resource_attributes: LabelSet::try_from_iter(vec![
                ("host", "localhost"),
                ("service.name", "test-svc"),
            ])
            .unwrap_or_default(),
            data_points: vec![DataPoint::new(
                100,
                MetricValue::Double(42.0),
                LabelSet::try_from_iter(vec![("env", "prod")]).unwrap_or_default(),
            )
            .unwrap()],
            ..Default::default()
        };
        writer.push(metric).unwrap();
        let meta = writer.flush().unwrap();
        assert!(meta.path.exists());
        assert_eq!(meta.row_count, 1);
    }

    /// `BlockConfig::row_group_size` has to reach the Parquet writer, because
    /// the scanner prunes whole row groups by timestamp statistics: a block
    /// written as a single row group can never be narrowed down, and readers
    /// silently fall back to decoding it in full.
    #[test]
    fn test_flush_honours_configured_row_group_size() {
        let dir = tempdir().unwrap();
        let config = BlockConfig {
            data_dir: dir.path().to_path_buf(),
            row_group_size: 100,
            ..Default::default()
        };
        let mut writer = BlockWriter::new(config);
        writer
            .push(Metric {
                name: "rg.cpu".into(),
                kind: MetricKind::Gauge,
                data_points: (0..250)
                    .map(|i| {
                        DataPoint::new(i + 1, MetricValue::Double(i as f64), LabelSet::default())
                            .unwrap()
                    })
                    .collect(),
                ..Default::default()
            })
            .unwrap();
        let meta = writer.flush().unwrap();
        assert_eq!(meta.row_count, 250);

        let file = std::fs::File::open(&meta.path).unwrap();
        let builder =
            parquet::arrow::arrow_reader::ParquetRecordBatchReaderBuilder::try_new(file).unwrap();
        assert_eq!(
            builder.metadata().num_row_groups(),
            3,
            "250 rows at 100 rows/group must produce 3 row groups"
        );
    }
}
