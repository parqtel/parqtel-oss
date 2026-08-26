use arrow2::chunk::Chunk;
use arrow2::io::parquet::write::{
    CompressionOptions, Encoding, FileWriter, RowGroupIterator, Version, WriteOptions,
};
pub use parqtel_core::BlockMetadata;
use parqtel_core::{
    BlockConfig, DataPoint, Error, LabelSet, LogBlockConfig, LogRecord, Metric, MetricKind, Result,
    Span, StorageModel,
};
use std::collections::HashSet;
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
        for ctx in &self.buffer {
            metric_names.insert((*ctx.name).clone());
            for label in ctx.resource.keys() {
                label_names.insert(label.clone());
            }
            for label in ctx.dp.labels.keys() {
                label_names.insert(label.clone());
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
            StorageModel::metrics_schema(),
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
            signal_type: parqtel_core::models::storage::SignalType::Metrics,
        })
    }

    fn reconstruct_metrics(&self) -> Result<Vec<Metric>> {
        use std::collections::BTreeMap;
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
        for log in &self.buffer {
            for label in log.attributes.keys() {
                label_names.insert(label.clone());
            }
            for label in log.resource_attributes.keys() {
                label_names.insert(label.clone());
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
            StorageModel::logs_schema(),
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
        write_parquet_file(
            &tmp_path,
            chunk,
            &self.config.compression,
            StorageModel::traces_schema(),
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
            signal_type: parqtel_core::models::storage::SignalType::Traces,
        })
    }
}

/// Rows per Parquet row group when flushing blocks.
/// Multiple row groups let readers skip groups via timestamp statistics on
/// narrow-range queries. ponytail: fixed size — expose in BlockConfig if
/// workloads need different pruning/compression trade-offs.
const ROW_GROUP_ROWS: usize = 25_000;

/// Splits one Arrow chunk into row-group-sized chunks by slicing columns.
fn split_chunk(
    chunk: &Chunk<Arc<dyn arrow2::array::Array>>,
    rows_per_group: usize,
) -> Vec<Chunk<Arc<dyn arrow2::array::Array>>> {
    let total = chunk.len();
    if total <= rows_per_group {
        return vec![reslice_chunk(chunk, 0, total)];
    }
    let mut out = Vec::with_capacity(total.div_ceil(rows_per_group));
    let mut offset = 0;
    while offset < total {
        let len = rows_per_group.min(total - offset);
        out.push(reslice_chunk(chunk, offset, len));
        offset += len;
    }
    out
}

fn reslice_chunk(
    chunk: &Chunk<Arc<dyn arrow2::array::Array>>,
    offset: usize,
    len: usize,
) -> Chunk<Arc<dyn arrow2::array::Array>> {
    let cols: Vec<Arc<dyn arrow2::array::Array>> = chunk
        .arrays()
        .iter()
        .map(|c| Arc::from(c.as_ref().sliced(offset, len)))
        .collect();
    Chunk::new(cols)
}

fn write_parquet_file(
    path: &Path,
    chunk: Chunk<Arc<dyn arrow2::array::Array>>,
    compression: &str,
    schema: arrow2::datatypes::Schema,
) -> Result<()> {
    let file = File::create(path)?;
    let options = WriteOptions {
        write_statistics: true,
        compression: match compression {
            "zstd" => CompressionOptions::Zstd(None),
            "snappy" => CompressionOptions::Snappy,
            "lz4" => CompressionOptions::Lz4Raw,
            _ => CompressionOptions::Uncompressed,
        },
        version: Version::V2,
        data_pagesize_limit: None,
    };

    let encodings: Vec<Vec<Encoding>> = schema
        .fields
        .iter()
        .map(|f| match f.data_type() {
            arrow2::datatypes::DataType::Dictionary(_, _, _) => vec![Encoding::RleDictionary],
            _ => vec![Encoding::Plain],
        })
        .collect();

    let row_groups = RowGroupIterator::try_new(
        split_chunk(&chunk, ROW_GROUP_ROWS).into_iter().map(Ok),
        &schema,
        options,
        encodings,
    )
    .map_err(|e| Error::Parquet(e.to_string()))?;

    let mut writer =
        FileWriter::try_new(file, schema, options).map_err(|e| Error::Parquet(e.to_string()))?;

    for group in row_groups {
        writer
            .write(group.map_err(|e| Error::Parquet(e.to_string()))?)
            .map_err(|e| Error::Parquet(e.to_string()))?;
    }

    writer
        .end(None)
        .map_err(|e| Error::Parquet(e.to_string()))?;
    Ok(())
}

#[cfg(test)]
mod tests {
    #![allow(clippy::unwrap_used, clippy::expect_used, clippy::panic)]
    use super::*;
    use parqtel_core::{DataPoint, LabelSet, LogRecord, Metric, MetricValue};
    use tempfile::tempdir;

    #[test]
    fn test_block_writer_metrics() {
        let dir = tempdir().unwrap();
        let config = BlockConfig {
            data_dir: dir.path().to_path_buf(),
            max_rows_per_block: 100,
            ..Default::default()
        };
        let mut writer = BlockWriter::new(config);

        let m1 = Metric {
            name: "test_m".into(),
            kind: parqtel_core::MetricKind::Gauge,
            data_points: vec![
                DataPoint::new(1000, MetricValue::Double(1.0), LabelSet::default()).unwrap(),
            ],
            ..Default::default()
        };
        writer.push(m1).unwrap();

        let meta = writer.flush().unwrap();
        assert_eq!(meta.row_count, 1);
        assert!(meta.path.exists());
    }

    #[test]
    fn test_log_writer() {
        let dir = tempdir().unwrap();
        let config = LogBlockConfig {
            data_dir: dir.path().to_path_buf(),
            max_rows_per_block: 100,
            ..Default::default()
        };
        let mut writer = LogWriter::new(config);

        let log = LogRecord::new(
            1000,
            1001,
            9,
            "INFO".into(),
            "test msg".into(),
            LabelSet::default(),
            LabelSet::default(),
            [0; 16],
            [0; 8],
            0,
            "test".into(),
            "1.0".into(),
        );
        writer.push(log).unwrap();

        let meta = writer.flush().unwrap();
        assert_eq!(meta.row_count, 1);
        assert!(meta.path.exists());
    }

    #[test]
    fn test_trace_writer() {
        let dir = tempdir().unwrap();
        let config = BlockConfig {
            data_dir: dir.path().to_path_buf(),
            max_rows_per_block: 100,
            ..Default::default()
        };
        let mut writer = TraceWriter::new(config);

        let span = Span::new(
            [0; 16],
            [0; 8],
            "".into(),
            "test-span".into(),
            1,
            1000,
            2000,
            LabelSet::default(),
            vec![],
            vec![],
            parqtel_core::SpanStatus {
                code: 0,
                message: "".into(),
            },
            [0; 8],
            0,
        );
        writer.push(span).unwrap();

        let meta = writer.flush().unwrap();
        assert_eq!(meta.row_count, 1);
        assert!(meta.path.exists());
        assert_eq!(
            meta.signal_type,
            parqtel_core::models::storage::SignalType::Traces
        );
    }
}
