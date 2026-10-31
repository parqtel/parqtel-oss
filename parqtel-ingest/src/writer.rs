use parqtel_core::models::storage::MetricRun;
use parqtel_core::storage::{write_block_file, FooterIndex};
pub use parqtel_core::BlockMetadata;
use parqtel_core::{
    BlockConfig, DataPoint, Error, LabelSet, LogBlockConfig, LogRecord, Metric, MetricKind, Result,
    Span, StorageModel,
};
use std::collections::{BTreeMap, HashSet};
use std::fs;
use std::sync::Arc;
use uuid::Uuid;

/// Upper bound on distinct values remembered per label in the block index.
const MAX_VALUES_PER_FIELD: usize = 10_000;

/// Distinct label names and (bounded) values seen while writing a block.
///
/// Each distinct [`LabelSet`] is observed once, however many rows share it, and
/// nothing is allocated for a name or value that is already recorded.
#[derive(Default)]
struct LabelIndex {
    names: HashSet<String>,
    values: BTreeMap<String, std::collections::BTreeSet<String>>,
}

impl LabelIndex {
    fn observe(&mut self, labels: &LabelSet) {
        for (k, v) in labels.iter() {
            if !self.names.contains(k) {
                self.names.insert(k.to_string());
            }
            match self.values.get_mut(k) {
                Some(set) => {
                    if set.len() < MAX_VALUES_PER_FIELD && !set.contains(v) {
                        set.insert(v.to_string());
                    }
                }
                None => {
                    let mut set = std::collections::BTreeSet::new();
                    set.insert(v.to_string());
                    self.values.insert(k.to_string(), set);
                }
            }
        }
    }
}

/// Buffers metrics in memory and flushes them to Parquet blocks.
pub struct BlockWriter {
    config: BlockConfig,
    /// Points exactly as they were pushed, one entry per push (or per capacity
    /// split of a push). Nothing is copied or re-encoded until the flush.
    batches: Vec<PushedBatch>,
    rows: usize,
    capacity: usize,
    /// WAL span the block written by the next flush covers; see
    /// [`Self::set_wal_range`].
    wal_range: Option<(u64, u64)>,
    /// Which generation of this shard's writer this is. Starts at 1 and moves on
    /// every time the writer's rows are written out, so the memory buffer can tell
    /// the points a block holds from the ones that arrived afterwards.
    epoch: u64,
}

struct PushedBatch {
    name: Arc<str>,
    kind: MetricKind,
    resource: LabelSet,
    points: Vec<DataPoint>,
}

impl BlockWriter {
    pub fn new(config: BlockConfig) -> Self {
        Self::with_epoch(config, 1)
    }

    /// A writer for generation `epoch` of its shard.
    pub fn with_epoch(config: BlockConfig, epoch: u64) -> Self {
        let capacity = config.max_rows_per_block;
        // Grows on demand: pre-allocating `max_rows_per_block` slots would park
        // hundreds of MB per writer, and every flush swaps in a fresh writer.
        Self {
            config,
            batches: Vec::new(),
            rows: 0,
            capacity,
            wal_range: None,
            epoch,
        }
    }

    /// Records the WAL span `(from, to]` that the next flush makes durable.
    pub fn set_wal_range(&mut self, range: Option<(u64, u64)>) {
        self.wal_range = range;
    }

    /// This writer's generation; see the field docs.
    pub fn epoch(&self) -> u64 {
        self.epoch
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
        let name: Arc<str> = Arc::from(metric.name);
        let resource = metric.resource_attributes;
        let kind = metric.kind;

        let mut flushed = Vec::new();
        let mut remaining = metric.data_points;
        while !remaining.is_empty() {
            let room = self.capacity.saturating_sub(self.rows);
            if room == 0 {
                flushed.push(self.flush()?);
                continue;
            }
            // The common case moves the whole vector in without copying a point.
            let rest = if remaining.len() > room {
                remaining.split_off(room)
            } else {
                Vec::new()
            };
            self.rows += remaining.len();
            self.batches.push(PushedBatch {
                name: Arc::clone(&name),
                kind,
                resource: resource.clone(),
                points: remaining,
            });
            remaining = rest;
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
        let mut batches = Vec::new();
        let mut rows = 0;
        for mut w in writers {
            config = Some(w.config.clone());
            rows += w.rows;
            batches.append(&mut w.batches);
        }
        let config =
            config.ok_or_else(|| Error::Internal("Cannot merge an empty writer set".into()))?;
        let capacity = config.max_rows_per_block;
        Ok(BlockWriter {
            config,
            batches,
            rows,
            capacity,
            wal_range: None,
            epoch: 1,
        })
    }

    pub fn len(&self) -> usize {
        self.rows
    }
    pub fn is_empty(&self) -> bool {
        self.rows == 0
    }

    pub fn flush(&mut self) -> Result<BlockMetadata> {
        if self.rows == 0 {
            return Err(Error::Internal("Cannot flush empty buffer".into()));
        }
        let row_count = self.rows;

        // One pass over the pushed batches: time bounds, and the label index.
        // Label sets are shared between rows of a series, so each distinct set
        // is walked once instead of once per row.
        let mut start_ts = i64::MAX;
        let mut end_ts = i64::MIN;
        let mut metric_names: HashSet<String> = HashSet::new();
        let mut index = LabelIndex::default();
        {
            let mut seen: HashSet<&LabelSet> = HashSet::new();
            for batch in &self.batches {
                if !metric_names.contains(&*batch.name) {
                    metric_names.insert(batch.name.to_string());
                }
                if seen.insert(&batch.resource) {
                    index.observe(&batch.resource);
                }
                for dp in &batch.points {
                    start_ts = start_ts.min(dp.timestamp_ns);
                    end_ts = end_ts.max(dp.timestamp_ns);
                    if seen.insert(&dp.labels) {
                        index.observe(&dp.labels);
                    }
                }
            }
        }

        // Rows are written grouped by metric identity and, inside a group, in
        // time order. That layout is what lets the reader prune row groups by
        // `metric_name`/`service_name` statistics. Groups are ordered by
        // (name, kind, resource); each group sorts a compact
        // (timestamp, batch, index) key rather than moving points around, and
        // the key's tail keeps equal timestamps in arrival order.
        type GroupKey<'a> = (&'a str, MetricKind, &'a LabelSet);
        let mut groups: BTreeMap<GroupKey<'_>, Vec<(u64, i64, u32, u32)>> = BTreeMap::new();
        for (bi, batch) in self.batches.iter().enumerate() {
            let keys = groups
                .entry((&batch.name, batch.kind, &batch.resource))
                .or_default();
            keys.reserve(batch.points.len());
            keys.extend(batch.points.iter().enumerate().map(|(pi, dp)| {
                (
                    dp.labels.fingerprint(),
                    dp.timestamp_ns,
                    bi as u32,
                    pi as u32,
                )
            }));
        }
        let runs: Vec<MetricRun<'_>> = groups
            .into_iter()
            .map(|((name, kind, resource), mut keys)| {
                keys.sort_unstable();
                MetricRun {
                    name,
                    kind,
                    resource,
                    points: keys
                        .iter()
                        .map(|&(_, _, bi, pi)| &self.batches[bi as usize].points[pi as usize])
                        .collect(),
                }
            })
            .collect();
        let (chunk, series) = StorageModel::metric_runs_to_block(&runs)?;
        drop(runs);

        let filename = format!(
            "{}_{}_{}.parquet",
            start_ts,
            end_ts,
            Uuid::new_v4().simple()
        );
        let final_path = self.config.data_dir.join(&filename);
        let tmp_path = self.config.data_dir.join(format!(".tmp_{}", filename));

        fs::create_dir_all(&self.config.data_dir)?;
        write_block_file(
            &tmp_path,
            chunk,
            &self.config.compression,
            self.config.compression_level,
            self.config.row_group_size,
            self.wal_range,
            Some(FooterIndex::Series(&series)),
        )?;
        fs::rename(&tmp_path, &final_path)?;
        let size_bytes = fs::metadata(&final_path)?.len();
        self.batches.clear();
        self.rows = 0;
        self.epoch += 1;

        Ok(BlockMetadata {
            path: final_path,
            start_timestamp_ns: start_ts,
            end_timestamp_ns: end_ts,
            row_count,
            size_bytes,
            metric_names,
            label_names: index.names,
            label_values: index.values,
            signal_type: parqtel_core::models::storage::SignalType::Metrics,
            wal_range: self.wal_range,
        })
    }
}

/// Buffers logs in memory and flushes them to Parquet blocks.
pub struct LogWriter {
    config: LogBlockConfig,
    buffer: Vec<LogRecord>,
    capacity: usize,
    wal_range: Option<(u64, u64)>,
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
            wal_range: None,
        }
    }

    /// Records the WAL span `(from, to]` that the next flush makes durable.
    pub fn set_wal_range(&mut self, range: Option<(u64, u64)>) {
        self.wal_range = range;
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

        let (chunk, skip_index) = StorageModel::logs_to_block(&self.buffer)?;
        let filename = format!(
            "logs_{}_{}_{}.parquet",
            start_ts,
            end_ts,
            Uuid::new_v4().simple()
        );
        let final_path = self.config.data_dir.join(&filename);
        let tmp_path = self.config.data_dir.join(format!(".tmp_{}", filename));

        fs::create_dir_all(&self.config.data_dir)?;
        write_block_file(
            &tmp_path,
            chunk,
            &self.config.compression,
            self.config.compression_level,
            self.config.row_group_size,
            self.wal_range,
            Some(FooterIndex::Logs(&skip_index)),
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
            wal_range: self.wal_range,
        })
    }
}

/// Buffers traces in memory and flushes them to Parquet blocks.
pub struct TraceWriter {
    config: BlockConfig,
    buffer: Vec<Span>,
    capacity: usize,
    wal_range: Option<(u64, u64)>,
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
            wal_range: None,
        }
    }

    /// Records the WAL span `(from, to]` that the next flush makes durable.
    pub fn set_wal_range(&mut self, range: Option<(u64, u64)>) {
        self.wal_range = range;
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
        write_block_file(
            &tmp_path,
            chunk,
            &self.config.compression,
            self.config.compression_level,
            self.config.row_group_size,
            self.wal_range,
            None,
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
            wal_range: self.wal_range,
        })
    }
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

    /// The layout the reader's row-group pruning relies on: rows grouped by
    /// (metric, kind, resource) in key order, time-ordered inside a group, ties
    /// kept in arrival order - however the pushes interleaved.
    #[test]
    fn test_flush_groups_rows_by_series_identity_in_time_order() {
        let dir = tempdir().unwrap();
        let mut writer = BlockWriter::new(BlockConfig {
            data_dir: dir.path().to_path_buf(),
            ..Default::default()
        });
        let res = |svc: &str| LabelSet::try_from_iter(vec![("service.name", svc)]).unwrap();
        let lab = |h: &str| LabelSet::try_from_iter(vec![("host", h)]).unwrap();
        let pt =
            |ts: i64, v: f64, h: &str| DataPoint::new(ts, MetricValue::Double(v), lab(h)).unwrap();
        let mk = |name: &str, svc: &str, dps: Vec<DataPoint>| Metric {
            name: name.into(),
            kind: MetricKind::Gauge,
            resource_attributes: res(svc),
            data_points: dps,
            ..Default::default()
        };
        // "b" is pushed before "a"; "a" is pushed twice with out-of-order and
        // tied timestamps.
        writer
            .push(mk("b", "s1", vec![pt(50, 1.0, "x"), pt(10, 2.0, "x")]))
            .unwrap();
        writer
            .push(mk("a", "s1", vec![pt(30, 3.0, "y"), pt(20, 4.0, "y")]))
            .unwrap();
        writer
            .push(mk("a", "s1", vec![pt(20, 5.0, "z"), pt(40, 6.0, "z")]))
            .unwrap();
        writer.push(mk("a", "s0", vec![pt(25, 7.0, "w")])).unwrap();
        let meta = writer.flush().unwrap();

        assert_eq!(meta.row_count, 7);
        assert_eq!(meta.start_timestamp_ns, 10);
        assert_eq!(meta.end_timestamp_ns, 50);
        assert_eq!(meta.metric_names.len(), 2);
        let mut names: Vec<_> = meta.label_names.iter().cloned().collect();
        names.sort();
        assert_eq!(names, vec!["host", "service.name"]);
        let hosts: Vec<_> = meta.label_values["host"].iter().cloned().collect();
        assert_eq!(hosts, vec!["w", "x", "y", "z"]);

        let file = std::fs::File::open(&meta.path).unwrap();
        let builder =
            parquet::arrow::arrow_reader::ParquetRecordBatchReaderBuilder::try_new(file).unwrap();
        let footer = builder
            .metadata()
            .file_metadata()
            .key_value_metadata()
            .and_then(|kv| {
                kv.iter()
                    .find(|k| k.key == parqtel_core::storage::SERIES_INDEX_FOOTER_KEY)
            })
            .and_then(|k| k.value.clone())
            .unwrap();
        let reader = builder.build().unwrap();
        let mut rows = Vec::new();
        for batch in reader {
            let batch = batch.unwrap();
            for r in 0..batch.num_rows() {
                let (name, _, _, dp) = StorageModel::row_to_point(&batch, r).unwrap();
                let MetricValue::Double(v) = dp.value else {
                    panic!("not a double")
                };
                let host = dp.labels.get("host").unwrap().to_string();
                rows.push((name, host, dp.timestamp_ns, v));
            }
        }
        let row = |n: &str, h: &str, t: i64, v: f64| (n.to_string(), h.to_string(), t, v);
        // Groups by (metric, resource): a/s0, a/s1, b/s1. Inside a group each
        // series is one contiguous, time-ordered run; the order of the series
        // themselves is by fingerprint, so only the pair is pinned.
        assert_eq!(rows[0], row("a", "w", 25, 7.0));
        let mut pair = [rows[1..3].to_vec(), rows[3..5].to_vec()];
        pair.sort_by(|x, y| x[0].1.cmp(&y[0].1));
        assert_eq!(
            pair,
            [
                vec![row("a", "y", 20, 4.0), row("a", "y", 30, 3.0)],
                vec![row("a", "z", 20, 5.0), row("a", "z", 40, 6.0)],
            ]
        );
        assert_eq!(rows[5..], [row("b", "x", 10, 2.0), row("b", "x", 50, 1.0)]);

        // The table in the footer describes exactly that layout: one entry per
        // series, covering the rows it occupies, with service.name merged in.
        let table = parqtel_core::storage::SeriesTable::from_footer_value(&footer).unwrap();
        let index = parqtel_core::storage::SeriesIndex::from_table(&table).unwrap();
        assert_eq!(index.series.len(), 4);
        assert_eq!(index.total_rows(), 7);
        for entry in &index.series {
            let (a, b) = (
                entry.start as usize,
                entry.start as usize + entry.rows as usize,
            );
            for r in &rows[a..b] {
                assert_eq!(entry.labels.get("host"), Some(r.1.as_str()));
                assert!(entry.min_ts <= r.2 && r.2 <= entry.max_ts);
            }
            assert!(entry.labels.get("service.name").is_some());
        }
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
