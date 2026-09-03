pub use parqtel_core::BlockMetadata;
use parqtel_core::{
    BlockConfig, DataPoint, Error, LabelSet, LogBlockConfig, LogRecord, Metric, MetricKind, Result,
    Span, StorageModel,
};
use parquet::arrow::ArrowWriter;
use parquet::basic::Compression;
use parquet::file::properties::{WriterProperties, WriterVersion};
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
        Self {
            config,
            buffer: Vec::with_capacity(capacity),
            capacity,
        }
    }

    pub fn push(&mut self, metric: Metric) -> Result<()> {
        let name = Arc::new(metric.name);
        let resource = Arc::new(metric.resource_attributes);
        let kind = metric.kind;

        for dp in metric.data_points {
            if self.buffer.len() >= self.capacity {
                return Err(Error::Validation("Block writer buffer is full".into()));
            }
            self.buffer.push(DataPointContext {
                name: name.clone(),
                kind,
                resource: resource.clone(),
                dp,
            });
        }
        Ok(())
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
        write_parquet_file(&tmp_path, chunk, &self.config.compression)?;
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
        Self {
            config,
            buffer: Vec::with_capacity(capacity),
            capacity,
        }
    }

    pub fn push(&mut self, log: LogRecord) -> Result<()> {
        if self.buffer.len() >= self.capacity {
            return Err(Error::Validation("Log writer buffer is full".into()));
        }
        self.buffer.push(log);
        Ok(())
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
        write_parquet_file(&tmp_path, chunk, &self.config.compression)?;
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
        Self {
            config,
            buffer: Vec::with_capacity(capacity),
            capacity,
        }
    }

    pub fn push(&mut self, span: Span) -> Result<()> {
        if self.buffer.len() >= self.capacity {
            return Err(Error::Validation("Trace writer buffer is full".into()));
        }
        self.buffer.push(span);
        Ok(())
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
        write_parquet_file(&tmp_path, chunk, &self.config.compression)?;
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

/// Rows per Parquet row group when flushing blocks.
/// Multiple row groups let readers skip groups via timestamp statistics on
/// narrow-range queries. ponytail: fixed size — expose in BlockConfig if
/// workloads need different pruning/compression trade-offs.
#[allow(dead_code)]
const ROW_GROUP_ROWS: usize = 25_000;

fn write_parquet_file(
    path: &Path,
    record_batch: arrow::record_batch::RecordBatch,
    compression: &str,
) -> Result<()> {
    let file = File::create(path)?;

    let writer_props = WriterProperties::builder()
        .set_compression(match compression {
            "zstd" => Compression::ZSTD(Default::default()),
            "snappy" => Compression::SNAPPY,
            "lz4" => Compression::LZ4_RAW,
            _ => Compression::UNCOMPRESSED,
        })
        .set_writer_version(WriterVersion::PARQUET_2_0)
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
    use parqtel_core::{DataPoint, LabelSet, Metric, MetricValue};
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
}
