//! Bounded metrics compaction over series runs.

use super::block_file::{read_compacted_sources, sync_parent_dir, BlockFileWriter, FooterIndex};
use super::index::{BlockIndex, BlockIndexStore};
use crate::config::BlockConfig;
use crate::error::{Error, Result};
use crate::models::labels::LabelSet;
use crate::models::metrics::{DataPoint, MetricKind, MetricValue};
use crate::models::storage::{BlockMetadata, MetricRun, SignalType, StorageModel};
use crate::storage::scanner::Scanner;
use crate::storage::{SeriesTable, SeriesTableBuilder};
use arrow_array::{Array, Float64Array, Int64Array, StringArray, TimestampNanosecondArray};
use parquet::arrow::arrow_reader::{
    ParquetRecordBatchReader, ParquetRecordBatchReaderBuilder, RowSelection,
};
use parquet::arrow::ProjectionMask;
use std::cmp::{Ordering, Reverse};
use std::collections::{BTreeMap, BTreeSet, BinaryHeap, HashSet};
use std::fs::{self, File};
use std::path::{Path, PathBuf};
use std::sync::Arc;
use std::time::Duration;
use tokio::sync::RwLock;
use uuid::Uuid;

const MAX_MERGE_FAN_IN: usize = 8;
const LEVEL_ONE_WINDOW_NS: i64 = 5 * 60 * 1_000_000_000;
const SOURCE_DELETE_GRACE: Duration = Duration::from_secs(5 * 60);
const MERGE_BATCH_ROWS: usize = 8192;
const MAX_NORMALIZE_ROWS: usize = 500_000;
const MAX_NORMALIZATIONS_PER_PASS: usize = 1;
const MAX_LEVEL_MERGES_PER_PASS: usize = 2;
const MAX_COMPACTION_ROWS_PER_PASS: u64 = 16_000_000;
const METRIC_GROUP_COLUMNS: [&str; 9] = [
    "metric_kind",
    "service_name",
    "service_version",
    "k8s_namespace",
    "k8s_pod_name",
    "k8s_pod_uid",
    "k8s_container_name",
    "k8s_node_name",
    "resource_attributes",
];
const METRIC_VALUE_COLUMNS: [&str; 4] =
    ["timestamp_ns", "value_float", "value_int", "value_complex"];

#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord)]
struct MetricGroupKey {
    name: String,
    kind: MetricKind,
    resource: LabelSet,
}

struct SourceGroup {
    key: MetricGroupKey,
    first_series: usize,
    end_series: usize,
}

struct SourceTable {
    meta: BlockMetadata,
    table: SeriesTable,
    groups: Vec<SourceGroup>,
}

#[derive(Clone)]
struct SourceSeries {
    id: usize,
    group: usize,
    labels: LabelSet,
    fingerprint: u64,
    rows: u32,
    min_ts: i64,
    max_ts: i64,
}

struct SeriesCursor<'a> {
    source: &'a SourceTable,
    next_id: usize,
    group: usize,
    current: Option<SourceSeries>,
    previous_group: Option<usize>,
    previous_fingerprint: u64,
}

struct OutputSeries {
    group: usize,
    labels: LabelSet,
    sources: [Option<u32>; MAX_MERGE_FAN_IN],
    row_start: u64,
    rows: u32,
    min_ts: i64,
    max_ts: i64,
}

struct OutputPlan {
    table: SeriesTable,
    groups: Vec<MetricGroupKey>,
    series: Vec<OutputSeries>,
    total_rows: u64,
    start_ts: i64,
    end_ts: i64,
    metric_names: HashSet<String>,
    label_names: HashSet<String>,
    label_values: BTreeMap<String, BTreeSet<String>>,
}

#[derive(Default)]
struct LabelCatalog {
    names: HashSet<String>,
    values: BTreeMap<String, BTreeSet<String>>,
}

impl LabelCatalog {
    fn observe(&mut self, labels: &LabelSet) {
        for (name, value) in labels.iter() {
            self.names.insert(name.to_string());
            let values = self.values.entry(name.to_string()).or_default();
            if values.len() < 10_000 {
                values.insert(value.to_string());
            }
        }
    }

    fn merge_metadata(&mut self, metadata: &BlockMetadata) {
        self.names.extend(metadata.label_names.iter().cloned());
        for (name, values) in &metadata.label_values {
            let target = self.values.entry(name.clone()).or_default();
            for value in values {
                if target.len() >= 10_000 {
                    break;
                }
                target.insert(value.clone());
            }
        }
    }
}

impl SourceTable {
    fn load(meta: BlockMetadata) -> Result<Self> {
        let file = File::open(&meta.path)?;
        let builder = ParquetRecordBatchReaderBuilder::try_new(file)
            .map_err(|e| Error::Parquet(e.to_string()))?;
        let row_count = builder.metadata().file_metadata().num_rows().max(0) as u64;
        let footer = builder
            .metadata()
            .file_metadata()
            .key_value_metadata()
            .and_then(|items| {
                items
                    .iter()
                    .find(|item| item.key == crate::storage::SERIES_INDEX_FOOTER_KEY)
            })
            .and_then(|item| item.value.as_deref())
            .ok_or_else(|| {
                Error::Internal(format!(
                    "metrics block {:?} has no series index; leaving it unmodified",
                    meta.path
                ))
            })?;
        let table = SeriesTable::from_footer_value(footer)?;
        if table.total_rows() != row_count {
            return Err(Error::Internal(format!(
                "series index row count disagrees with compaction source {:?}",
                meta.path
            )));
        }

        let mut next_row = 0u64;
        for series_id in 0..table.series_count() {
            let (start, rows, _, _) = table.series_bounds(series_id).ok_or_else(|| {
                Error::Internal("corrupt series index: series range missing".into())
            })?;
            if start != next_row || rows == 0 {
                return Err(Error::Internal(format!(
                    "series index row ranges are not contiguous in {:?}",
                    meta.path
                )));
            }
            next_row = next_row
                .checked_add(u64::from(rows))
                .ok_or_else(|| Error::Internal("series index row range overflow".into()))?;
        }
        if next_row != row_count {
            return Err(Error::Internal(format!(
                "series index does not cover compaction source {:?}",
                meta.path
            )));
        }

        let groups = read_metric_groups(&meta.path, &table, row_count)?;
        Ok(Self {
            meta,
            table,
            groups,
        })
    }

    fn group_for_series(&self, series_id: usize) -> Option<usize> {
        let group = self
            .groups
            .partition_point(|group| group.end_series <= series_id);
        self.groups
            .get(group)
            .filter(|item| item.first_series <= series_id)
            .map(|_| group)
    }
}

fn read_metric_groups(
    path: &Path,
    table: &SeriesTable,
    row_count: u64,
) -> Result<Vec<SourceGroup>> {
    let file = File::open(path)?;
    let builder = ParquetRecordBatchReaderBuilder::try_new(file)
        .map_err(|e| Error::Parquet(e.to_string()))?;
    let runs: Vec<_> = (0..table.metric_run_count())
        .map(|index| {
            table
                .metric_run(index)
                .ok_or_else(|| Error::Internal("corrupt series index: invalid metric run".into()))
        })
        .collect::<Result<_>>()?;
    let mut expected_series = 0usize;
    let mut run_starts = Vec::with_capacity(runs.len());
    for run in &runs {
        if run.first_series != expected_series || run.series_count == 0 {
            return Err(Error::Internal(format!(
                "series index metric runs are not contiguous in {path:?}"
            )));
        }
        let end = run
            .first_series
            .checked_add(run.series_count)
            .ok_or_else(|| Error::Internal("series id range overflow".into()))?;
        run_starts.push(run.first_row);
        expected_series = end;
    }
    if expected_series != table.series_count() {
        return Err(Error::Internal(format!(
            "series index metric runs do not cover {path:?}"
        )));
    }

    let row_ranges = run_starts
        .iter()
        .map(|start| {
            let start = usize::try_from(*start)
                .map_err(|_| Error::Internal("compaction row offset overflow".into()))?;
            Ok(start..start + 1)
        })
        .collect::<Result<Vec<_>>>()?;
    let row_count = usize::try_from(row_count)
        .map_err(|_| Error::Internal("compaction row count overflow".into()))?;
    let selection = RowSelection::from_consecutive_ranges(row_ranges.into_iter(), row_count);
    let schema = builder.parquet_schema();
    let columns = crate::storage::scanner::project_present(schema, &METRIC_GROUP_COLUMNS);
    let mask = ProjectionMask::leaves(schema, columns);
    let reader = builder
        .with_row_selection(selection)
        .with_projection(mask)
        .with_batch_size(MERGE_BATCH_ROWS)
        .build()
        .map_err(|e| Error::Parquet(e.to_string()))?;

    let mut groups = Vec::with_capacity(runs.len());
    for batch in reader {
        let batch = batch.map_err(|e| Error::Parquet(e.to_string()))?;
        for row in 0..batch.num_rows() {
            let index = groups.len();
            let run = runs.get(index).ok_or_else(|| {
                Error::Internal("series metadata scan returned extra rows".into())
            })?;
            let (kind, resource) = StorageModel::metric_group_from_row(&batch, row)?;
            groups.push(SourceGroup {
                key: MetricGroupKey {
                    name: run.metric_name.to_string(),
                    kind,
                    resource,
                },
                first_series: run.first_series,
                end_series: run.first_series + run.series_count,
            });
        }
    }
    if groups.len() != runs.len() {
        return Err(Error::Internal(format!(
            "series metadata scan returned {} of {} runs in {path:?}",
            groups.len(),
            runs.len()
        )));
    }
    Ok(groups)
}

impl<'a> SeriesCursor<'a> {
    fn new(source: &'a SourceTable) -> Result<Self> {
        let mut cursor = Self {
            source,
            next_id: 0,
            group: 0,
            current: None,
            previous_group: None,
            previous_fingerprint: 0,
        };
        cursor.advance()?;
        Ok(cursor)
    }

    fn advance(&mut self) -> Result<()> {
        let series_id = self.next_id;
        if series_id >= self.source.table.series_count() {
            self.current = None;
            return Ok(());
        }
        while self
            .source
            .groups
            .get(self.group)
            .is_some_and(|group| series_id >= group.end_series)
        {
            self.group += 1;
        }
        let group = self
            .source
            .groups
            .get(self.group)
            .ok_or_else(|| Error::Internal("series index is missing a metric group".into()))?;
        if series_id < group.first_series {
            return Err(Error::Internal(
                "series index has a gap between metric groups".into(),
            ));
        }
        let entry = self.source.table.series_entry(series_id)?;
        let fingerprint = entry.labels.fingerprint();
        if let Some(previous_group) = self.previous_group {
            let order = self.source.groups[previous_group]
                .key
                .cmp(&group.key)
                .then(self.previous_fingerprint.cmp(&fingerprint));
            if order == Ordering::Greater {
                return Err(Error::Internal(format!(
                    "series runs are not sorted in compaction source {:?}",
                    self.source.meta.path
                )));
            }
        }
        self.current = Some(SourceSeries {
            id: series_id,
            group: self.group,
            fingerprint,
            labels: entry.labels,
            rows: entry.rows,
            min_ts: entry.min_ts,
            max_ts: entry.max_ts,
        });
        self.previous_group = Some(self.group);
        self.previous_fingerprint = fingerprint;
        self.next_id += 1;
        Ok(())
    }
}

fn compare_series_keys(
    source_a: &SourceTable,
    series_a: &SourceSeries,
    source_b: &SourceTable,
    series_b: &SourceSeries,
) -> Ordering {
    source_a.groups[series_a.group]
        .key
        .cmp(&source_b.groups[series_b.group].key)
        .then(series_a.fingerprint.cmp(&series_b.fingerprint))
}

fn same_series_key(
    source_a: &SourceTable,
    series_a: &SourceSeries,
    source_b: &SourceTable,
    series_b: &SourceSeries,
) -> bool {
    source_a.groups[series_a.group].key == source_b.groups[series_b.group].key
        && series_a.fingerprint == series_b.fingerprint
        && series_a.labels == series_b.labels
}

fn build_output_plan(sources: &[SourceTable]) -> Result<OutputPlan> {
    if sources.is_empty() || sources.len() > MAX_MERGE_FAN_IN {
        return Err(Error::Internal("invalid metrics compaction fan-in".into()));
    }
    let mut cursors = sources
        .iter()
        .map(SeriesCursor::new)
        .collect::<Result<Vec<_>>>()?;
    let mut table_builder = SeriesTableBuilder::new();
    let mut groups: Vec<MetricGroupKey> = Vec::new();
    let mut series = Vec::new();
    let mut metric_names = HashSet::new();
    let mut catalog = LabelCatalog::default();
    for source in sources {
        metric_names.extend(source.meta.metric_names.iter().cloned());
        catalog.merge_metadata(&source.meta);
    }

    let mut total_rows = 0u64;
    let mut start_ts = i64::MAX;
    let mut end_ts = i64::MIN;
    loop {
        let mut first: Option<usize> = None;
        for index in 0..cursors.len() {
            let Some(candidate) = cursors[index].current.as_ref() else {
                continue;
            };
            let Some(best_index) = first else {
                first = Some(index);
                continue;
            };
            let best = cursors[best_index]
                .current
                .as_ref()
                .ok_or_else(|| Error::Internal("series merge cursor disappeared".into()))?;
            if compare_series_keys(
                cursors[index].source,
                candidate,
                cursors[best_index].source,
                best,
            ) == Ordering::Less
            {
                first = Some(index);
            }
        }
        let Some(first) = first else {
            break;
        };
        let first_series = cursors[first]
            .current
            .as_ref()
            .ok_or_else(|| Error::Internal("series merge cursor disappeared".into()))?
            .clone();
        let matching: Vec<usize> = (0..cursors.len())
            .filter(|&index| {
                cursors[index].current.as_ref().is_some_and(|candidate| {
                    same_series_key(
                        cursors[index].source,
                        candidate,
                        cursors[first].source,
                        &first_series,
                    )
                })
            })
            .collect();

        let group_key = cursors[first].source.groups[first_series.group].key.clone();
        let group_index = match groups.last() {
            Some(previous) if previous == &group_key => groups.len() - 1,
            _ => {
                table_builder.begin_metric(&group_key.name);
                groups.push(group_key.clone());
                groups.len() - 1
            }
        };
        metric_names.insert(group_key.name.clone());
        catalog.observe(&group_key.resource);
        catalog.observe(&first_series.labels);

        let mut source_series = [None; MAX_MERGE_FAN_IN];
        let mut rows = 0u64;
        let mut min_ts = i64::MAX;
        let mut max_ts = i64::MIN;
        for &index in &matching {
            let candidate = cursors[index]
                .current
                .as_ref()
                .ok_or_else(|| Error::Internal("series merge cursor disappeared".into()))?;
            source_series[index] = Some(
                u32::try_from(candidate.id)
                    .map_err(|_| Error::Internal("too many series in compaction source".into()))?,
            );
            rows = rows
                .checked_add(u64::from(candidate.rows))
                .ok_or_else(|| Error::Internal("compacted series row count overflow".into()))?;
            min_ts = min_ts.min(candidate.min_ts);
            max_ts = max_ts.max(candidate.max_ts);
        }
        let rows_u32 = u32::try_from(rows)
            .map_err(|_| Error::Internal("compacted series exceeds u32 row count".into()))?;
        table_builder.push_series(&first_series.labels, total_rows, rows_u32, min_ts, max_ts);
        series.push(OutputSeries {
            group: group_index,
            labels: first_series.labels,
            sources: source_series,
            row_start: total_rows,
            rows: rows_u32,
            min_ts,
            max_ts,
        });
        total_rows = total_rows
            .checked_add(rows)
            .ok_or_else(|| Error::Internal("compacted row count overflow".into()))?;
        start_ts = start_ts.min(min_ts);
        end_ts = end_ts.max(max_ts);
        for index in matching {
            cursors[index].advance()?;
        }
    }

    let table = table_builder.finish();
    if table.total_rows() != total_rows {
        return Err(Error::Internal(
            "compacted series table row count mismatch".into(),
        ));
    }
    if total_rows == 0 {
        return Err(Error::Internal(
            "cannot compact empty metrics blocks".into(),
        ));
    }
    Ok(OutputPlan {
        table,
        groups,
        series,
        total_rows,
        start_ts,
        end_ts,
        metric_names,
        label_names: catalog.names,
        label_values: catalog.values,
    })
}

struct ActiveSeries {
    id: usize,
    remaining: u32,
}

struct MetricDataCursor<'a> {
    source: &'a SourceTable,
    reader: ParquetRecordBatchReader,
    batch: Option<arrow::record_batch::RecordBatch>,
    batch_row: usize,
    columns: Option<[usize; 4]>,
    row_position: u64,
    next_series: usize,
    active: Option<ActiveSeries>,
}

impl<'a> MetricDataCursor<'a> {
    fn new(source: &'a SourceTable) -> Result<Self> {
        let file = File::open(&source.meta.path)?;
        let builder = ParquetRecordBatchReaderBuilder::try_new(file)
            .map_err(|e| Error::Parquet(e.to_string()))?;
        let schema = builder.parquet_schema();
        let columns = crate::storage::scanner::project_present(schema, &METRIC_VALUE_COLUMNS);
        let mask = ProjectionMask::leaves(schema, columns);
        let reader = builder
            .with_projection(mask)
            .with_batch_size(MERGE_BATCH_ROWS)
            .build()
            .map_err(|e| Error::Parquet(e.to_string()))?;
        Ok(Self {
            source,
            reader,
            batch: None,
            batch_row: 0,
            columns: None,
            row_position: 0,
            next_series: 0,
            active: None,
        })
    }

    fn begin_series(&mut self, series_id: usize) -> Result<()> {
        if self.active.is_some() || series_id != self.next_series {
            return Err(Error::Internal(
                "metrics compaction series cursor is out of order".into(),
            ));
        }
        let (start, rows, _, _) = self
            .source
            .table
            .series_bounds(series_id)
            .ok_or_else(|| Error::Internal("compaction source series is missing".into()))?;
        if start != self.row_position {
            return Err(Error::Internal(
                "metrics compaction row cursor disagrees with series index".into(),
            ));
        }
        self.active = Some(ActiveSeries {
            id: series_id,
            remaining: rows,
        });
        Ok(())
    }

    fn next_sample(&mut self) -> Result<Option<(i64, MetricValue)>> {
        let remaining = self
            .active
            .as_ref()
            .ok_or_else(|| Error::Internal("metrics compaction has no active series".into()))?
            .remaining;
        if remaining == 0 {
            return Ok(None);
        }
        if !self.ensure_batch()? {
            return Err(Error::Internal(
                "metrics compaction source ended before its series index".into(),
            ));
        }
        let batch = self
            .batch
            .as_ref()
            .ok_or_else(|| Error::Internal("metrics compaction batch missing".into()))?;
        let [ts_idx, float_idx, int_idx, complex_idx] = self
            .columns
            .ok_or_else(|| Error::Internal("metrics compaction columns missing".into()))?;
        let row = self.batch_row;
        let timestamp = batch
            .column(ts_idx)
            .as_any()
            .downcast_ref::<TimestampNanosecondArray>()
            .ok_or_else(|| Error::Arrow("Invalid timestamp_ns column".into()))?
            .value(row);
        let float = batch
            .column(float_idx)
            .as_any()
            .downcast_ref::<Float64Array>()
            .ok_or_else(|| Error::Arrow("Invalid value_float column".into()))?;
        let int = batch
            .column(int_idx)
            .as_any()
            .downcast_ref::<Int64Array>()
            .ok_or_else(|| Error::Arrow("Invalid value_int column".into()))?;
        let complex = batch
            .column(complex_idx)
            .as_any()
            .downcast_ref::<StringArray>()
            .ok_or_else(|| Error::Arrow("Invalid value_complex column".into()))?;
        let value = if !float.is_null(row) {
            MetricValue::Double(float.value(row))
        } else if !int.is_null(row) {
            MetricValue::Int(int.value(row))
        } else {
            serde_json::from_str(complex.value(row)).map_err(Error::Serde)?
        };

        self.batch_row += 1;
        self.row_position += 1;
        if let Some(active) = &mut self.active {
            active.remaining -= 1;
        }
        Ok(Some((timestamp, value)))
    }

    fn finish_series(&mut self, series_id: usize) -> Result<()> {
        let active = self
            .active
            .take()
            .ok_or_else(|| Error::Internal("metrics compaction has no active series".into()))?;
        if active.id != series_id || active.remaining != 0 {
            return Err(Error::Internal(
                "metrics compaction did not consume a complete series".into(),
            ));
        }
        self.next_series += 1;
        Ok(())
    }

    fn finish(&mut self) -> Result<()> {
        if self.active.is_some()
            || self.next_series != self.source.table.series_count()
            || self.row_position != self.source.table.total_rows()
        {
            return Err(Error::Internal(
                "metrics compaction did not consume every source row".into(),
            ));
        }
        Ok(())
    }

    fn ensure_batch(&mut self) -> Result<bool> {
        if self
            .batch
            .as_ref()
            .is_some_and(|batch| self.batch_row < batch.num_rows())
        {
            return Ok(true);
        }
        let Some(batch) = self.reader.next() else {
            self.batch = None;
            return Ok(false);
        };
        let batch = batch.map_err(|e| Error::Parquet(e.to_string()))?;
        let schema = batch.schema();
        let mut columns = [0usize; 4];
        for (index, name) in columns.iter_mut().zip(METRIC_VALUE_COLUMNS) {
            *index = schema
                .index_of(name)
                .map_err(|_| Error::Arrow(format!("Missing {name} column")))?;
        }
        self.columns = Some(columns);
        self.batch_row = 0;
        self.batch = Some(batch);
        Ok(self
            .batch
            .as_ref()
            .is_some_and(|batch| self.batch_row < batch.num_rows()))
    }
}

struct BatchRun {
    group: usize,
    start: usize,
    end: usize,
}

struct MetricBatchAccumulator {
    points: Vec<DataPoint>,
    runs: Vec<BatchRun>,
    max_rows: usize,
}

impl MetricBatchAccumulator {
    fn new(max_rows: usize) -> Self {
        Self {
            points: Vec::new(),
            runs: Vec::new(),
            max_rows: max_rows.max(1),
        }
    }

    fn push(
        &mut self,
        group: usize,
        groups: &[MetricGroupKey],
        labels: &LabelSet,
        timestamp_ns: i64,
        value: MetricValue,
    ) -> Result<Option<arrow::record_batch::RecordBatch>> {
        if let Some(run) = self.runs.last_mut().filter(|run| run.group == group) {
            run.end += 1;
        } else {
            let start = self.points.len();
            self.runs.push(BatchRun {
                group,
                start,
                end: start + 1,
            });
        }
        self.points
            .push(DataPoint::new(timestamp_ns, value, labels.clone())?);
        if self.points.len() >= self.max_rows {
            self.flush(groups)
        } else {
            Ok(None)
        }
    }

    fn finish(
        &mut self,
        groups: &[MetricGroupKey],
    ) -> Result<Option<arrow::record_batch::RecordBatch>> {
        self.flush(groups)
    }

    fn flush(
        &mut self,
        groups: &[MetricGroupKey],
    ) -> Result<Option<arrow::record_batch::RecordBatch>> {
        if self.points.is_empty() {
            return Ok(None);
        }
        let runs: Vec<MetricRun<'_>> = self
            .runs
            .iter()
            .map(|run| {
                let group = &groups[run.group];
                MetricRun {
                    name: &group.name,
                    kind: group.kind,
                    resource: &group.resource,
                    points: self.points[run.start..run.end].iter().collect(),
                }
            })
            .collect();
        let (batch, _) = StorageModel::metric_runs_to_block(&runs)?;
        drop(runs);
        self.points.clear();
        self.runs.clear();
        Ok(Some(batch))
    }
}

fn write_batch(
    writer: &mut Option<BlockFileWriter>,
    batch: arrow::record_batch::RecordBatch,
    temp_path: &Path,
    config: &BlockConfig,
    table: &SeriesTable,
    wal_range: Option<(u64, u64)>,
    compacted_sources: &[PathBuf],
) -> Result<()> {
    if writer.is_none() {
        *writer = Some(BlockFileWriter::create(
            temp_path,
            batch.schema(),
            &config.compression,
            config.compression_level,
            config.row_group_size,
            wal_range,
            Some(FooterIndex::Series(table)),
            compacted_sources,
        )?);
    }
    writer
        .as_mut()
        .ok_or_else(|| Error::Internal("streaming block writer missing".into()))?
        .write(&batch)
}

fn write_metric_compaction(
    sources: &[SourceTable],
    plan: &OutputPlan,
    config: &BlockConfig,
    temp_path: &Path,
    wal_range: Option<(u64, u64)>,
    compacted_sources: &[PathBuf],
) -> Result<()> {
    let mut cursors = sources
        .iter()
        .map(MetricDataCursor::new)
        .collect::<Result<Vec<_>>>()?;
    let mut accumulator = MetricBatchAccumulator::new(config.row_group_size);
    let mut writer = None;
    let mut output_row = 0u64;
    for series in &plan.series {
        if series.row_start != output_row {
            return Err(Error::Internal(
                "compaction output series row ranges are not contiguous".into(),
            ));
        }
        let group = plan
            .groups
            .get(series.group)
            .ok_or_else(|| Error::Internal("compaction metric group missing".into()))?;
        let mut heads: Vec<Option<MetricValue>> = std::iter::repeat_with(|| None)
            .take(sources.len())
            .collect();
        let mut queue = BinaryHeap::new();
        for (source_index, source_series) in series.sources.iter().enumerate() {
            let Some(source_series) = source_series else {
                continue;
            };
            let source_table = sources
                .get(source_index)
                .ok_or_else(|| Error::Internal("compaction source index out of range".into()))?;
            let source_group = source_table
                .group_for_series(*source_series as usize)
                .and_then(|index| source_table.groups.get(index))
                .ok_or_else(|| Error::Internal("compaction source group missing".into()))?;
            if source_group.key != *group {
                return Err(Error::Internal(
                    "compaction output series disagrees with source index".into(),
                ));
            }
            let cursor = cursors
                .get_mut(source_index)
                .ok_or_else(|| Error::Internal("compaction data cursor out of range".into()))?;
            cursor.begin_series(*source_series as usize)?;
            let (timestamp_ns, value) = cursor.next_sample()?.ok_or_else(|| {
                Error::Internal("compaction source series contains no rows".into())
            })?;
            heads[source_index] = Some(value);
            queue.push(Reverse((timestamp_ns, source_index)));
        }

        let mut written = 0u64;
        let mut min_ts = i64::MAX;
        let mut max_ts = i64::MIN;
        while let Some(Reverse((timestamp_ns, source_index))) = queue.pop() {
            let value = heads[source_index]
                .take()
                .ok_or_else(|| Error::Internal("compaction sample missing".into()))?;
            if let Some(batch) = accumulator.push(
                series.group,
                &plan.groups,
                &series.labels,
                timestamp_ns,
                value,
            )? {
                write_batch(
                    &mut writer,
                    batch,
                    temp_path,
                    config,
                    &plan.table,
                    wal_range,
                    compacted_sources,
                )?;
            }
            written += 1;
            min_ts = min_ts.min(timestamp_ns);
            max_ts = max_ts.max(timestamp_ns);
            if let Some((next_ts, next_value)) = cursors[source_index].next_sample()? {
                heads[source_index] = Some(next_value);
                queue.push(Reverse((next_ts, source_index)));
            }
        }
        if written != u64::from(series.rows) {
            return Err(Error::Internal(
                "streamed series row count disagrees with compaction plan".into(),
            ));
        }
        if min_ts != series.min_ts || max_ts != series.max_ts {
            return Err(Error::Internal(
                "streamed series timestamps disagree with compaction index".into(),
            ));
        }
        output_row = output_row
            .checked_add(written)
            .ok_or_else(|| Error::Internal("compaction output row count overflow".into()))?;
        for (source_index, source_series) in series.sources.iter().enumerate() {
            if let Some(source_series) = source_series {
                cursors[source_index].finish_series(*source_series as usize)?;
            }
        }
    }

    if let Some(batch) = accumulator.finish(&plan.groups)? {
        write_batch(
            &mut writer,
            batch,
            temp_path,
            config,
            &plan.table,
            wal_range,
            compacted_sources,
        )?;
    }
    for cursor in &mut cursors {
        cursor.finish()?;
    }
    if output_row != plan.total_rows {
        return Err(Error::Internal(
            "streamed block row count disagrees with compaction plan".into(),
        ));
    }
    writer
        .ok_or_else(|| Error::Internal("compaction produced no Parquet batches".into()))?
        .close_and_sync()
}

fn compact_metric_blocks(
    blocks: &[BlockMetadata],
    config: &BlockConfig,
    target_level: u8,
) -> Result<BlockMetadata> {
    let committed = crate::wal::read_commit(&config.data_dir, "metrics");
    let wal_range = merged_wal_range(blocks, committed)?;
    let sources = blocks
        .iter()
        .cloned()
        .map(SourceTable::load)
        .collect::<Result<Vec<_>>>()?;
    let plan = build_output_plan(&sources)?;
    let superseded = collect_superseded_sources(&sources)?;
    let filename = format!(
        "m_l{target_level}_{}_{}_{}.parquet",
        plan.start_ts,
        plan.end_ts,
        Uuid::new_v4().simple()
    );
    let final_path = config.data_dir.join(&filename);
    let temp_path = config.data_dir.join(format!(".tmp_{filename}"));
    fs::create_dir_all(&config.data_dir)?;
    let _temp = TempBlock(temp_path.clone());

    write_metric_compaction(&sources, &plan, config, &temp_path, wal_range, &superseded)?;
    validate_compacted_file(&temp_path, &plan)?;
    fs::rename(&temp_path, &final_path)?;
    sync_parent_dir(&final_path)?;
    let size_bytes = fs::metadata(&final_path)?.len();
    let row_count = usize::try_from(plan.total_rows)
        .map_err(|_| Error::Internal("compacted row count exceeds usize".into()))?;
    Ok(BlockMetadata {
        path: final_path,
        start_timestamp_ns: plan.start_ts,
        end_timestamp_ns: plan.end_ts,
        row_count,
        size_bytes,
        metric_names: plan.metric_names,
        label_names: plan.label_names,
        label_values: plan.label_values,
        signal_type: SignalType::Metrics,
        wal_range,
    })
}

struct TempBlock(PathBuf);

impl Drop for TempBlock {
    fn drop(&mut self) {
        let _ = fs::remove_file(&self.0);
    }
}

fn validate_compacted_file(path: &Path, plan: &OutputPlan) -> Result<()> {
    let file = File::open(path)?;
    let builder = ParquetRecordBatchReaderBuilder::try_new(file)
        .map_err(|e| Error::Parquet(e.to_string()))?;
    let rows = builder.metadata().file_metadata().num_rows().max(0) as u64;
    if rows != plan.total_rows {
        return Err(Error::Internal(
            "compacted Parquet row count disagrees with output plan".into(),
        ));
    }
    let value = builder
        .metadata()
        .file_metadata()
        .key_value_metadata()
        .and_then(|items| {
            items
                .iter()
                .find(|item| item.key == crate::storage::SERIES_INDEX_FOOTER_KEY)
        })
        .and_then(|item| item.value.as_deref())
        .ok_or_else(|| Error::Internal("compacted block has no series index".into()))?;
    let table = SeriesTable::from_footer_value(value)?;
    if table != plan.table || table.total_rows() != rows {
        return Err(Error::Internal(
            "compacted series index disagrees with output plan".into(),
        ));
    }
    Ok(())
}

/// The WAL span the merged block makes durable.
///
/// Contiguous ranges merge into one span. Mixed or gapped ranges cannot be
/// described by one span, but if every range is already at or below the WAL
/// commit point none of those records can be replayed again, so the output
/// needs no span at all. Otherwise the group is rejected.
fn merged_wal_range(
    blocks: &[BlockMetadata],
    committed: crate::wal::WalPosition,
) -> Result<Option<(u64, u64)>> {
    let ranges: Vec<_> = blocks.iter().filter_map(|block| block.wal_range).collect();
    if ranges.is_empty() {
        return Ok(None);
    }
    let all_committed = ranges
        .iter()
        .all(|&(_, to)| crate::wal::WalPosition(to).is_covered_by(committed));
    if ranges.len() != blocks.len() {
        if all_committed {
            return Ok(None);
        }
        return Err(Error::Internal(
            "cannot compact blocks with mixed WAL metadata".into(),
        ));
    }
    let mut ranges = ranges;
    ranges.sort_unstable_by_key(|range| range.0);
    if ranges.windows(2).any(|pair| pair[0].1 != pair[1].0) {
        if all_committed {
            return Ok(None);
        }
        return Err(Error::Internal(
            "cannot compact non-contiguous WAL ranges".into(),
        ));
    }
    Ok(Some((ranges[0].0, ranges[ranges.len() - 1].1)))
}

fn collect_superseded_sources(sources: &[SourceTable]) -> Result<Vec<PathBuf>> {
    let blocks: Vec<_> = sources.iter().map(|source| source.meta.clone()).collect();
    collect_superseded_paths(&blocks)
}

fn collect_superseded_paths(blocks: &[BlockMetadata]) -> Result<Vec<PathBuf>> {
    let mut paths = BTreeSet::new();
    for block in blocks {
        let name = block
            .path
            .file_name()
            .ok_or_else(|| Error::Internal("compaction source has no filename".into()))?;
        paths.insert(PathBuf::from(name));
        for path in read_compacted_sources(&block.path)? {
            paths.insert(path);
        }
    }
    Ok(paths.into_iter().collect())
}

fn metric_compaction_level(path: &Path) -> u8 {
    let name = path
        .file_name()
        .and_then(|name| name.to_str())
        .unwrap_or_default();
    if name.starts_with("m_l2_") {
        2
    } else if name.starts_with("m_l1_") {
        1
    } else {
        0
    }
}

fn completed_bucket(timestamp_ns: i64, window_ns: i64, now_ns: i64) -> Option<i64> {
    let start = timestamp_ns.div_euclid(window_ns) * window_ns;
    let end = start.checked_add(window_ns)?;
    (end <= now_ns).then_some(start)
}

#[cfg(test)]
mod tests {
    #![allow(clippy::unwrap_used, clippy::expect_used, clippy::panic)]
    use super::*;
    use crate::storage::block_file::write_block_file;
    use crate::storage::{SeriesIndex, SeriesSelector, SeriesTable, SERIES_INDEX_FOOTER_KEY};
    use std::collections::BTreeSet;

    fn write_metrics_block(
        dir: &Path,
        filename: &str,
        start: i64,
        wal_range: (u64, u64),
        canonical_order: bool,
    ) -> BlockMetadata {
        let resource =
            LabelSet::try_from_iter(vec![("service.name", "api"), ("zone", "east")]).unwrap();
        let mut points: Vec<_> = ["h0", "h1"]
            .into_iter()
            .flat_map(|host| {
                [1i64, 2].into_iter().map(move |offset| {
                    let value = match (host, offset) {
                        ("h0", 2) => MetricValue::Int(start + offset),
                        ("h1", 1) => MetricValue::histogram(
                            3,
                            12.0,
                            Some(1.0),
                            Some(8.0),
                            vec![2.0],
                            vec![1, 2],
                        ),
                        ("h1", 2) => MetricValue::summary(3, 12.0, vec![(0.5, 3.0)]),
                        _ => MetricValue::Double((start + offset) as f64),
                    };
                    DataPoint::new(
                        start + offset,
                        value,
                        LabelSet::try_from_iter(vec![("host", host)]).unwrap(),
                    )
                    .unwrap()
                })
            })
            .collect();
        points.sort_by(|a, b| {
            let fingerprint = |point: &DataPoint| {
                if canonical_order {
                    resource
                        .get("service.name")
                        .filter(|value| !value.is_empty())
                        .map_or_else(
                            || point.labels.fingerprint(),
                            |service| point.labels.with("service.name", service).fingerprint(),
                        )
                } else {
                    point.labels.fingerprint()
                }
            };
            fingerprint(a)
                .cmp(&fingerprint(b))
                .then(a.timestamp_ns.cmp(&b.timestamp_ns))
        });
        let refs = points.iter().collect();
        let run = MetricRun {
            name: "cpu",
            kind: MetricKind::Gauge,
            resource: &resource,
            points: refs,
        };
        let (batch, table) = StorageModel::metric_runs_to_block(&[run]).unwrap();
        let path = dir.join(filename);
        write_block_file(
            &path,
            batch,
            "uncompressed",
            None,
            2,
            Some(wal_range),
            Some(FooterIndex::Series(&table)),
        )
        .unwrap();
        BlockMetadata {
            path: path.clone(),
            start_timestamp_ns: start + 1,
            end_timestamp_ns: start + 2,
            row_count: points.len(),
            size_bytes: fs::metadata(&path).unwrap().len(),
            metric_names: HashSet::from(["cpu".into()]),
            label_names: HashSet::from(["host".into(), "service.name".into(), "zone".into()]),
            label_values: BTreeMap::from([
                ("host".into(), BTreeSet::from(["h0".into(), "h1".into()])),
                ("service.name".into(), BTreeSet::from(["api".into()])),
                ("zone".into(), BTreeSet::from(["east".into()])),
            ]),
            signal_type: SignalType::Metrics,
            wal_range: Some(wal_range),
        }
    }

    #[test]
    fn streaming_merge_coalesces_series_and_preserves_wal_provenance() {
        let dir = tempfile::tempdir().unwrap();
        let config = BlockConfig {
            data_dir: dir.path().to_path_buf(),
            compression: "uncompressed".into(),
            row_group_size: 2,
            ..Default::default()
        };
        let first = write_metrics_block(dir.path(), "first.parquet", 100, (0, 4), true);
        let second = write_metrics_block(dir.path(), "second.parquet", 200, (4, 8), true);

        let merged = compact_metric_blocks(&[first.clone(), second.clone()], &config, 1).unwrap();
        assert_eq!(merged.row_count, 8);
        assert_eq!(merged.wal_range, Some((0, 8)));
        assert!(merged
            .path
            .file_name()
            .unwrap()
            .to_string_lossy()
            .starts_with("m_l1_"));
        assert!(
            first.path.exists(),
            "sources stay readable during query grace"
        );
        assert!(
            second.path.exists(),
            "sources stay readable during query grace"
        );

        let file = File::open(&merged.path).unwrap();
        let builder = ParquetRecordBatchReaderBuilder::try_new(file).unwrap();
        let footer = builder
            .metadata()
            .file_metadata()
            .key_value_metadata()
            .unwrap()
            .iter()
            .find(|item| item.key == SERIES_INDEX_FOOTER_KEY)
            .and_then(|item| item.value.as_deref())
            .unwrap();
        let index =
            SeriesIndex::from_table(&SeriesTable::from_footer_value(footer).unwrap()).unwrap();
        assert_eq!(index.series.len(), 2, "one entry per logical series");
        assert!(index.series.iter().all(|series| series.rows == 4));
        assert_eq!(
            read_compacted_sources(&merged.path).unwrap(),
            vec![
                PathBuf::from("first.parquet"),
                PathBuf::from("second.parquet")
            ]
        );

        let scan = Scanner::scan_block_filtered(merged, "cpu", 0, i64::MAX, &SeriesSelector::all())
            .unwrap();
        assert_eq!(scan.points.len(), 8);
        for host in ["h0", "h1"] {
            let timestamps: Vec<_> = scan
                .points
                .iter()
                .filter(|point| point.labels.get("host") == Some(host))
                .map(|point| point.timestamp_ns)
                .collect();
            assert_eq!(timestamps, vec![101, 102, 201, 202]);
        }
        let values_for = |host| {
            scan.points
                .iter()
                .filter(|point| point.labels.get("host") == Some(host))
                .map(|point| point.value.clone())
                .collect::<Vec<_>>()
        };
        assert_eq!(
            values_for("h0"),
            vec![
                MetricValue::Double(101.0),
                MetricValue::Int(102),
                MetricValue::Double(201.0),
                MetricValue::Int(202),
            ]
        );
        assert_eq!(
            values_for("h1"),
            vec![
                MetricValue::histogram(3, 12.0, Some(1.0), Some(8.0), vec![2.0], vec![1, 2]),
                MetricValue::summary(3, 12.0, vec![(0.5, 3.0)]),
                MetricValue::histogram(3, 12.0, Some(1.0), Some(8.0), vec![2.0], vec![1, 2]),
                MetricValue::summary(3, 12.0, vec![(0.5, 3.0)]),
            ]
        );

        let mut recovered = BlockIndex::new(dir.path());
        recovered.add(first);
        recovered.add(second);
        let stats = super::super::index::reconcile(&mut recovered, SignalType::Metrics).unwrap();
        assert_eq!(stats.dropped, 2);
        assert_eq!(stats.adopted, 1);
        assert_eq!(recovered.total_blocks(), 1);
        assert_eq!(recovered.blocks[0].row_count, 8);
    }

    #[test]
    fn streaming_merge_rejects_noncontiguous_wal_without_touching_sources() {
        let dir = tempfile::tempdir().unwrap();
        let config = BlockConfig {
            data_dir: dir.path().to_path_buf(),
            compression: "uncompressed".into(),
            ..Default::default()
        };
        let first = write_metrics_block(dir.path(), "first.parquet", 100, (0, 4), true);
        let second = write_metrics_block(dir.path(), "second.parquet", 200, (5, 8), true);

        assert!(compact_metric_blocks(&[first.clone(), second.clone()], &config, 1).is_err());
        assert!(first.path.exists());
        assert!(second.path.exists());
        assert_eq!(
            fs::read_dir(dir.path())
                .unwrap()
                .filter_map(std::result::Result::ok)
                .filter(|entry| entry.path().extension().is_some_and(|ext| ext == "parquet"))
                .count(),
            2
        );
    }

    #[test]
    fn mixed_wal_metadata_is_rejected_before_source_io() {
        let dir = tempfile::tempdir().unwrap();
        let config = BlockConfig {
            data_dir: dir.path().to_path_buf(),
            compression: "uncompressed".into(),
            ..Default::default()
        };
        let block = |name: &str, wal_range| BlockMetadata {
            path: dir.path().join(name),
            start_timestamp_ns: 1,
            end_timestamp_ns: 1,
            row_count: 1,
            size_bytes: 0,
            metric_names: HashSet::new(),
            label_names: HashSet::new(),
            label_values: BTreeMap::new(),
            signal_type: SignalType::Metrics,
            wal_range,
        };

        let error = compact_metric_blocks(
            &[
                block("missing-a.parquet", Some((0, 1))),
                block("missing-b.parquet", None),
            ],
            &config,
            1,
        )
        .unwrap_err();
        assert!(error
            .to_string()
            .contains("cannot compact blocks with mixed WAL metadata"));
    }

    #[test]
    fn committed_mixed_or_gapped_wal_ranges_merge_without_a_span() {
        let dir = tempfile::tempdir().unwrap();
        let config = BlockConfig {
            data_dir: dir.path().to_path_buf(),
            compression: "uncompressed".into(),
            ..Default::default()
        };
        let with_range = write_metrics_block(dir.path(), "a.parquet", 100, (0, 4), true);
        let mut without_range = write_metrics_block(dir.path(), "b.parquet", 200, (4, 8), true);
        without_range.wal_range = None;
        let gapped = write_metrics_block(dir.path(), "c.parquet", 300, (9, 12), true);

        // Commit below the ranges: still unsafe, still rejected.
        crate::wal::commit_position(dir.path(), "metrics", crate::wal::WalPosition(3)).unwrap();
        let mixed = [with_range.clone(), without_range.clone()];
        assert!(compact_metric_blocks(&mixed, &config, 1).is_err());

        // Every range committed: nothing can be replayed, so merge with no span.
        crate::wal::commit_position(dir.path(), "metrics", crate::wal::WalPosition(12)).unwrap();
        let merged = compact_metric_blocks(&mixed, &config, 1).unwrap();
        assert_eq!(merged.row_count, 8);
        assert_eq!(merged.wal_range, None);

        let merged = compact_metric_blocks(&[with_range, gapped], &config, 1).unwrap();
        assert_eq!(merged.row_count, 8);
        assert_eq!(merged.wal_range, None);
    }

    #[test]
    fn legacy_normalization_skips_blocks_above_memory_bound() {
        let dir = tempfile::tempdir().unwrap();
        let config = BlockConfig {
            data_dir: dir.path().to_path_buf(),
            ..Default::default()
        };
        let block = BlockMetadata {
            path: dir.path().join("missing.parquet"),
            start_timestamp_ns: 1,
            end_timestamp_ns: 1,
            row_count: MAX_NORMALIZE_ROWS + 1,
            size_bytes: 0,
            metric_names: HashSet::new(),
            label_names: HashSet::new(),
            label_values: BTreeMap::new(),
            signal_type: SignalType::Metrics,
            wal_range: None,
        };

        let error = normalize_legacy_metric_block(&block, &config).unwrap_err();
        assert!(error
            .to_string()
            .contains("legacy block exceeds normalization bound of 500000 rows"));
        assert!(!block.path.exists());
    }

    #[test]
    fn legacy_normalization_orders_series_by_query_visible_labels() {
        let dir = tempfile::tempdir().unwrap();
        let config = BlockConfig {
            data_dir: dir.path().to_path_buf(),
            compression: "uncompressed".into(),
            row_group_size: 2,
            ..Default::default()
        };
        let legacy = write_metrics_block(dir.path(), "legacy.parquet", 100, (0, 4), false);
        let file = File::open(&legacy.path).unwrap();
        let builder = ParquetRecordBatchReaderBuilder::try_new(file).unwrap();
        let footer = builder
            .metadata()
            .file_metadata()
            .key_value_metadata()
            .unwrap()
            .iter()
            .find(|item| item.key == SERIES_INDEX_FOOTER_KEY)
            .and_then(|item| item.value.as_deref())
            .unwrap();
        let legacy_table = SeriesTable::from_footer_value(footer).unwrap();
        let legacy_fingerprints: Vec<_> = (0..legacy_table.series_count())
            .map(|id| legacy_table.series_entry(id).unwrap().labels.fingerprint())
            .collect();
        let mut sorted = legacy_fingerprints.clone();
        sorted.sort_unstable();
        assert_ne!(legacy_fingerprints, sorted, "fixture must use legacy order");

        let normalized = normalize_legacy_metric_block(&legacy, &config).unwrap();
        assert!(normalized
            .path
            .file_name()
            .unwrap()
            .to_string_lossy()
            .starts_with("m_n_"));
        assert!(
            legacy.path.exists(),
            "legacy source stays readable during grace"
        );
        assert_eq!(
            read_compacted_sources(&normalized.path).unwrap(),
            vec![PathBuf::from("legacy.parquet")]
        );

        let file = File::open(&normalized.path).unwrap();
        let builder = ParquetRecordBatchReaderBuilder::try_new(file).unwrap();
        let footer = builder
            .metadata()
            .file_metadata()
            .key_value_metadata()
            .unwrap()
            .iter()
            .find(|item| item.key == SERIES_INDEX_FOOTER_KEY)
            .and_then(|item| item.value.as_deref())
            .unwrap();
        let table = SeriesTable::from_footer_value(footer).unwrap();
        let fingerprints: Vec<_> = (0..table.series_count())
            .map(|id| table.series_entry(id).unwrap().labels.fingerprint())
            .collect();
        let mut ordered = fingerprints.clone();
        ordered.sort_unstable();
        assert_eq!(fingerprints, ordered);
        assert_eq!(table.series_count(), 2);
        let normalized_source = SourceTable::load(normalized).unwrap();
        assert_eq!(
            build_output_plan(&[normalized_source]).unwrap().total_rows,
            4
        );

        let mut recovered = BlockIndex::new(dir.path());
        recovered.add(legacy);
        let stats = super::super::index::reconcile(&mut recovered, SignalType::Metrics).unwrap();
        assert_eq!(stats.dropped, 1);
        assert_eq!(stats.adopted, 1);
        assert_eq!(recovered.total_blocks(), 1);
    }
}

fn candidate_groups(
    index: &BlockIndex,
    now_ns: i64,
    window_ns: i64,
    target_level: u8,
    fan_in: usize,
    skipped: &BTreeSet<i64>,
) -> Vec<(i64, Vec<BlockMetadata>)> {
    let mut by_bucket: BTreeMap<i64, Vec<BlockMetadata>> = BTreeMap::new();
    for block in &index.blocks {
        if block.signal_type != SignalType::Metrics
            || !is_normalized_metric_path(&block.path)
            || metric_compaction_level(&block.path) > target_level
            || block.end_timestamp_ns > now_ns
        {
            continue;
        }
        let Some(bucket) = completed_bucket(block.start_timestamp_ns, window_ns, now_ns) else {
            continue;
        };
        if !skipped.contains(&bucket) {
            by_bucket.entry(bucket).or_default().push(block.clone());
        }
    }
    by_bucket
        .into_iter()
        .filter_map(|(bucket, mut blocks)| {
            if blocks.len() < 2 {
                return None;
            }
            blocks.sort_by_key(|block| block.start_timestamp_ns);
            blocks.truncate(fan_in);
            Some((bucket, blocks))
        })
        .collect()
}

/// Runs a bounded L0-to-L1 metrics compaction pass.
pub(crate) async fn compact_metrics_levels_once(
    index: &Arc<RwLock<BlockIndex>>,
    store: &BlockIndexStore,
    config: &BlockConfig,
) -> Result<usize> {
    let has_metrics = index
        .read()
        .await
        .blocks
        .iter()
        .any(|block| block.signal_type == SignalType::Metrics);
    if !has_metrics {
        return Ok(0);
    }
    let Some(_scan_slots) = Scanner::try_reserve_scan_slots(2) else {
        tracing::debug!("metrics compaction deferred while scan budget is busy");
        return Ok(0);
    };
    let now_ns = chrono::Utc::now().timestamp_nanos_opt().unwrap_or(0);
    let fan_in = config
        .compaction_max_merge_blocks
        .clamp(2, MAX_MERGE_FAN_IN);
    let normalization_limit = config
        .compaction_max_merges_per_pass
        .clamp(1, MAX_NORMALIZATIONS_PER_PASS);
    let (normalized, mut rows_processed) = normalize_legacy_metrics_once(
        index,
        store,
        config,
        normalization_limit,
        MAX_COMPACTION_ROWS_PER_PASS,
    )
    .await?;
    let max_merges = config
        .compaction_max_merges_per_pass
        .clamp(1, MAX_LEVEL_MERGES_PER_PASS);
    let mut completed = 0usize;

    let mut skipped_buckets = BTreeSet::new();
    let mut attempted_merges = 0usize;
    while attempted_merges < max_merges {
        let candidates = {
            let idx = index.read().await;
            candidate_groups(
                &idx,
                now_ns,
                LEVEL_ONE_WINDOW_NS,
                1,
                fan_in,
                &skipped_buckets,
            )
        };
        let Some((bucket, blocks)) = candidates.into_iter().next() else {
            break;
        };
        attempted_merges += 1;
        let group_rows = blocks
            .iter()
            .try_fold(0u64, |sum, block| sum.checked_add(block.row_count as u64))
            .ok_or_else(|| Error::Internal("compaction row count overflow".into()))?;
        if rows_processed.saturating_add(group_rows) > MAX_COMPACTION_ROWS_PER_PASS {
            skipped_buckets.insert(bucket);
            continue;
        }
        let paths: Vec<_> = blocks.iter().map(|block| block.path.clone()).collect();
        let config = config.clone();
        let merged =
            tokio::task::spawn_blocking(move || compact_metric_blocks(&blocks, &config, 1))
                .await
                .map_err(|e| Error::Internal(format!("metrics compaction task panicked: {e}")))?;
        let new_meta = match merged {
            Ok(meta) => meta,
            Err(error) => {
                tracing::warn!(
                    target_level = 1,
                    bucket_start_ns = bucket,
                    error = %error,
                    "metrics compaction group skipped"
                );
                skipped_buckets.insert(bucket);
                continue;
            }
        };
        {
            let mut idx = index.write().await;
            idx.replace(&paths, Some(new_meta.clone()));
            store.mark_dirty();
        }
        completed += 1;
        rows_processed += group_rows;
        tracing::info!(
            target_level = 1,
            source_blocks = paths.len(),
            rows = new_meta.row_count,
            bytes = new_meta.size_bytes,
            "metrics blocks compacted"
        );
    }

    let data_dir = config.data_dir.clone();
    tokio::task::spawn_blocking(move || prune_compacted_sources(&data_dir))
        .await
        .map_err(|e| Error::Internal(format!("compaction cleanup task panicked: {e}")))??;
    Ok(normalized + completed)
}

async fn normalize_legacy_metrics_once(
    index: &Arc<RwLock<BlockIndex>>,
    store: &BlockIndexStore,
    config: &BlockConfig,
    max_blocks: usize,
    row_budget: u64,
) -> Result<(usize, u64)> {
    let mut candidates = {
        let idx = index.read().await;
        idx.blocks
            .iter()
            .filter(|block| {
                block.signal_type == SignalType::Metrics
                    && !is_normalized_metric_path(&block.path)
                    && block.row_count > 0
                    && block.row_count <= MAX_NORMALIZE_ROWS
            })
            .cloned()
            .collect::<Vec<_>>()
    };
    candidates.sort_by_key(|block| block.start_timestamp_ns);

    let mut normalized = 0usize;
    let mut rows_processed = 0u64;
    for (attempted, source) in candidates.into_iter().enumerate() {
        if attempted >= max_blocks
            || rows_processed.saturating_add(source.row_count as u64) > row_budget
        {
            break;
        }
        rows_processed += source.row_count as u64;
        let config = config.clone();
        let source_for_rewrite = source.clone();
        let rewritten = tokio::task::spawn_blocking(move || {
            normalize_legacy_metric_block(&source_for_rewrite, &config)
        })
        .await
        .map_err(|e| Error::Internal(format!("metric normalization task panicked: {e}")))?;
        let rewritten = match rewritten {
            Ok(meta) => meta,
            Err(error) => {
                tracing::warn!(
                    path = ?source.path,
                    rows = source.row_count,
                    error = %error,
                    "legacy metrics block normalization skipped"
                );
                continue;
            }
        };
        {
            let mut idx = index.write().await;
            idx.replace(std::slice::from_ref(&source.path), Some(rewritten.clone()));
            store.mark_dirty();
        }
        normalized += 1;
        tracing::info!(
            rows = source.row_count,
            bytes = rewritten.size_bytes,
            path = ?source.path,
            "legacy metrics block normalized"
        );
    }
    Ok((normalized, rows_processed))
}

fn normalize_legacy_metric_block(
    source: &BlockMetadata,
    config: &BlockConfig,
) -> Result<BlockMetadata> {
    if source.row_count > MAX_NORMALIZE_ROWS {
        return Err(Error::Internal(format!(
            "legacy block exceeds normalization bound of {MAX_NORMALIZE_ROWS} rows"
        )));
    }
    // shortcut: buffers one <=500K-row legacy block; leave larger blocks untouched until normalization streams.
    let points = read_normalization_points(source)?;
    if points.len() != source.row_count {
        return Err(Error::Internal(
            "legacy metrics normalization row count mismatch".into(),
        ));
    }
    let sources = collect_superseded_paths(std::slice::from_ref(source))?;
    let mut catalog = LabelCatalog::default();
    catalog.merge_metadata(source);
    for (_, _, resource, point) in &points {
        catalog.observe(resource);
        catalog.observe(&point.labels);
    }
    super::Compactor::write_merged_with_provenance(
        config,
        SignalType::Metrics,
        points,
        Vec::new(),
        Vec::new(),
        catalog.values,
        source.wal_range,
        Some("m_n"),
        &sources,
    )
}

fn read_normalization_points(
    source: &BlockMetadata,
) -> Result<Vec<(String, MetricKind, LabelSet, DataPoint)>> {
    let source_table = match SourceTable::load(source.clone()) {
        Ok(table) => table,
        Err(index_error) => {
            let file = File::open(&source.path)?;
            let builder = ParquetRecordBatchReaderBuilder::try_new(file)
                .map_err(|e| Error::Parquet(e.to_string()))?;
            if builder.schema().index_of("labels").is_err() {
                return Err(index_error);
            }
            let (points, logs, spans) = super::Compactor::read_source_blocks(
                std::slice::from_ref(source),
                SignalType::Metrics,
            )?;
            if points.len() != source.row_count || !logs.is_empty() || !spans.is_empty() {
                return Err(Error::Internal(
                    "legacy metrics normalization row count mismatch".into(),
                ));
            }
            return Ok(points);
        }
    };
    let mut cursor = MetricDataCursor::new(&source_table)?;
    let mut points = Vec::new();
    for series_id in 0..source_table.table.series_count() {
        let group_index = source_table
            .group_for_series(series_id)
            .ok_or_else(|| Error::Internal("normalization source group missing".into()))?;
        let group = &source_table.groups[group_index].key;
        let entry = source_table.table.series_entry(series_id)?;
        cursor.begin_series(series_id)?;
        for _ in 0..entry.rows {
            let (timestamp_ns, value) = cursor.next_sample()?.ok_or_else(|| {
                Error::Internal("normalization source ended before its series index".into())
            })?;
            points.push((
                group.name.clone(),
                group.kind,
                group.resource.clone(),
                DataPoint::new(timestamp_ns, value, entry.labels.clone())?,
            ));
        }
        cursor.finish_series(series_id)?;
    }
    cursor.finish()?;
    Ok(points)
}

fn prune_compacted_sources(data_dir: &Path) -> Result<usize> {
    let now = std::time::SystemTime::now();
    let mut deleted = 0usize;
    for entry in fs::read_dir(data_dir)? {
        let path = entry?.path();
        if !is_levelled_metric_path(&path) {
            continue;
        }
        let modified = match fs::metadata(&path).and_then(|metadata| metadata.modified()) {
            Ok(modified) => modified,
            Err(error) => {
                tracing::debug!(path = ?path, error = %error, "cannot age compacted block");
                continue;
            }
        };
        if now.duration_since(modified).unwrap_or_default() < SOURCE_DELETE_GRACE {
            continue;
        }
        for source in read_compacted_sources(&path)? {
            match fs::remove_file(data_dir.join(source)) {
                Ok(()) => deleted += 1,
                Err(error) if error.kind() == std::io::ErrorKind::NotFound => {}
                Err(error) => {
                    tracing::warn!(path = ?path, error = %error, "cannot remove superseded source block")
                }
            }
        }
    }
    if deleted > 0 {
        tracing::info!(
            deleted_sources = deleted,
            "removed superseded metrics blocks"
        );
    }
    Ok(deleted)
}

fn is_levelled_metric_path(path: &Path) -> bool {
    let name = path
        .file_name()
        .and_then(|name| name.to_str())
        .unwrap_or_default();
    name.starts_with("m_n_") || matches!(metric_compaction_level(path), 1 | 2)
}

fn is_normalized_metric_path(path: &Path) -> bool {
    let name = path
        .file_name()
        .and_then(|name| name.to_str())
        .unwrap_or_default();
    name.starts_with("m_l0_") || is_levelled_metric_path(path)
}
