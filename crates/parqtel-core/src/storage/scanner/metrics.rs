//! Metric scans: the series-table path and the full-scan fallback.

use super::block::{open_block, OpenedBlock};
use super::pruning::{
    plan_row_selection, project_present, row_groups_in_range, row_groups_matching_value,
};
use super::Scanner;
use super::{MAX_BLOCKS, MAX_CONCURRENT, SCAN_BATCH_ROWS};
use crate::error::{Error, Result};
use crate::models::metrics::DataPoint;
use crate::models::storage::BlockMetadata;
use crate::storage::series_index::{row_ranges, SeriesIndex, SeriesSelector};
use arrow_array::{Array, TimestampNanosecondArray};
use parquet::arrow::arrow_reader::ParquetRecordBatchReaderBuilder;
use parquet::arrow::ProjectionMask;
use std::collections::HashMap;
use std::sync::Arc;

/// Columns the metrics scan decodes, in schema order.
///
/// A metrics block has 15 columns and this path reads 7. The other 8 -
/// `metric_kind`, the six k8s dictionaries and `resource_attributes` - are
/// per-row data that a range query never touches, and decoding them costs
/// bandwidth and CPU on every block the query opens.
///
/// Projection is intersected with the file's own schema before being applied,
/// so a block written by an older build that lacks one of these columns still
/// reads (see [`project_present`]).
pub const METRIC_SCAN_COLUMNS: [&str; 7] = [
    "timestamp_ns",
    "metric_name",
    "service_name",
    "labels",
    "value_float",
    "value_int",
    "value_complex",
];

/// Positional indices of the projected columns within a projected batch.
///
/// The decoders index columns positionally, and a projection renumbers them,
/// so every index is resolved **by name** from the batch's own schema. Looking
/// an index up once per batch is free; hard-coding positions against a
/// projection is how a projection silently starts returning the wrong column.
pub fn metric_scan_indices(schema: &std::sync::Arc<arrow_schema::Schema>) -> Result<[usize; 7]> {
    let mut idx = [usize::MAX; 7];
    for (slot, name) in idx.iter_mut().zip(METRIC_SCAN_COLUMNS) {
        *slot = schema
            .index_of(name)
            .map_err(|_| Error::Arrow(format!("metrics scan is missing the {name} column")))?;
    }
    Ok(idx)
}

/// Predicate over a series' label set (after the `service.name` label has been
/// merged back in). It is evaluated once per distinct series per block - not
/// per row - so it may be moderately expensive (regex matchers).
pub type SeriesFilter = Arc<dyn Fn(&crate::LabelSet) -> bool + Send + Sync>;

/// Result of a filtered metrics scan.
#[derive(Debug, Default)]
pub struct MetricScan {
    /// Points of the series accepted by the filter, in block order (not sorted
    /// across blocks).
    pub points: Vec<DataPoint>,
    /// Rows inside the time window that the scan actually read. A block with a
    /// series table reads only the rows of the series it selected; a block
    /// without one reads every row of the metric. Reported as "points scanned"
    /// by the query layer.
    pub rows_considered: u64,
}

/// How a row's `labels` text is reached: through the reader's dictionary, or as
/// a plain string column for a file whose schema could not be overridden.
enum LabelsColumn<'a> {
    Dict {
        keys: &'a arrow_array::Int32Array,
        values: &'a arrow_array::StringArray,
        values_ref: &'a arrow_array::ArrayRef,
    },
    Plain(&'a arrow_array::StringArray),
}

/// Series resolution state that survives across the batches of one block scan.
///
/// A row's series is `(labels dictionary key, service dictionary key)`. While
/// the reader keeps handing out the same dictionaries, a series' label set is
/// parsed - and the filter evaluated - once for the whole block instead of once
/// per batch (let alone once per row).
struct SeriesResolver {
    /// Dictionaries the cached slots were computed against. Holding the arrays
    /// keeps them alive, so a pointer match really is the same dictionary and
    /// not a freed one whose address was reused.
    labels_dict: Option<arrow_array::ArrayRef>,
    svc_dict: Option<arrow_array::ArrayRef>,
    slots: ahash::AHashMap<u64, Option<crate::LabelSet>>,
    filter: Option<SeriesFilter>,
}

impl SeriesResolver {
    pub(super) fn new(filter: Option<SeriesFilter>) -> Self {
        Self {
            labels_dict: None,
            svc_dict: None,
            slots: ahash::AHashMap::new(),
            filter,
        }
    }

    fn rebind(&mut self, labels: &arrow_array::ArrayRef, svc: &arrow_array::ArrayRef) {
        let same = matches!(
            (&self.labels_dict, &self.svc_dict),
            (Some(l), Some(s)) if Arc::ptr_eq(l, labels) && Arc::ptr_eq(s, svc)
        );
        if !same {
            self.slots.clear();
            self.labels_dict = Some(Arc::clone(labels));
            self.svc_dict = Some(Arc::clone(svc));
        }
    }

    /// Builds the series' label set (merging in `service.name`) and applies the
    /// filter; `None` means the series is rejected.
    pub(super) fn build(&self, labels_json: &str, svc: &str) -> Result<Option<crate::LabelSet>> {
        let mut l = crate::LabelSet::from_json(labels_json)?;
        if !svc.is_empty() {
            l = l.with("service.name", svc);
        }
        Ok(match &self.filter {
            Some(f) if !f(&l) => None,
            _ => Some(l),
        })
    }
}

/// Columns an indexed scan decodes: labels come from the series table.
const INDEXED_SCAN_COLUMNS: [&str; 4] =
    ["timestamp_ns", "value_float", "value_int", "value_complex"];

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

impl Scanner {
    /// Scans a set of blocks for data points matching a metric and time range.
    ///
    /// Runs each block's decode on the blocking thread pool (bounded by a
    /// semaphore acquired *before* spawning, so we never oversubscribe) and
    /// skips row groups whose timestamp statistics fall outside [start_ns,
    /// end_ns].
    #[allow(clippy::too_many_arguments)]
    pub async fn scan(
        blocks: Vec<BlockMetadata>,
        metric_name: String,
        start_ns: i64,
        end_ns: i64,
        service_name: Option<&str>,
    ) -> Result<Vec<DataPoint>> {
        let selector = SeriesSelector::all().with_service(service_name.map(|s| s.to_string()));
        let mut scan = Self::scan_filtered(blocks, metric_name, start_ns, end_ns, selector).await?;
        scan.points.sort_by_key(|p| p.timestamp_ns);
        Ok(scan.points)
    }

    /// As [`Self::scan`], but only the series `selector` accepts are returned.
    ///
    /// A block with a series table is asked which of its series qualify and reads
    /// only those rows; a block without one is scanned in full and its series
    /// are filtered as they are met. Either way the selector's `filter` decides
    /// every series that is returned, so the result does not depend on whether a
    /// block carries a table.
    ///
    /// The returned points are **not** sorted across blocks; callers that need
    /// order sort per series, which is far cheaper than sorting everything.
    pub async fn scan_filtered(
        blocks: Vec<BlockMetadata>,
        metric_name: String,
        start_ns: i64,
        end_ns: i64,
        selector: SeriesSelector,
    ) -> Result<MetricScan> {
        let selector = Arc::new(selector);
        let start_time = std::time::Instant::now();
        let block_count = blocks.len();
        tracing::debug!(
            metric = %metric_name,
            blocks = block_count,
            start_ns,
            end_ns,
            "scanning blocks"
        );
        let sem = Arc::new(tokio::sync::Semaphore::new(MAX_CONCURRENT));
        let mut tasks = Vec::new();
        for block in blocks.into_iter().take(MAX_BLOCKS) {
            let m_name = metric_name.clone();
            let selector = Arc::clone(&selector);
            let permit = sem
                .clone()
                .acquire_owned()
                .await
                .map_err(|e| Error::Internal(e.to_string()))?;
            tasks.push(tokio::task::spawn_blocking(move || {
                let _permit = permit;
                Self::scan_block_filtered(block, &m_name, start_ns, end_ns, &selector)
            }));
        }

        let mut out = MetricScan::default();
        for task in tasks {
            let part = task.await.map_err(|e| Error::Internal(e.to_string()))??;
            out.rows_considered += part.rows_considered;
            if out.points.is_empty() {
                out.points = part.points;
            } else {
                out.points.extend(part.points);
            }
        }
        tracing::debug!(
            metric = %metric_name,
            blocks = block_count,
            points = out.points.len(),
            duration_ms = start_time.elapsed().as_millis(),
            "scan complete"
        );
        Ok(out)
    }

    pub fn scan_block(
        meta: BlockMetadata,
        metric_name: String,
        start_ns: i64,
        end_ns: i64,
        service_name: Option<&str>,
    ) -> Result<Vec<DataPoint>> {
        let selector = SeriesSelector::all().with_service(service_name.map(|s| s.to_string()));
        Ok(Self::scan_block_filtered(meta, &metric_name, start_ns, end_ns, &selector)?.points)
    }

    /// Scans one block. See [`Self::scan_filtered`].
    pub fn scan_block_filtered(
        meta: BlockMetadata,
        metric_name: &str,
        start_ns: i64,
        end_ns: i64,
        selector: &SeriesSelector,
    ) -> Result<MetricScan> {
        let Some(opened) = open_block(&meta.path)? else {
            return Ok(MetricScan::default());
        };
        if let Some(index) = &opened.series {
            match Self::scan_block_indexed(&opened, index, metric_name, start_ns, end_ns, selector)
            {
                Ok(scan) => return Ok(scan),
                // A table that disagrees with its file costs speed, not
                // correctness: the rows are still there to be scanned.
                Err(e) => tracing::warn!(
                    path = ?meta.path,
                    "indexed block scan failed, scanning without the series table: {e}"
                ),
            }
        }
        let OpenedBlock { file, meta, .. } = opened;
        let service_name = selector.service.as_deref();
        let filter = selector.filter.clone();
        let reader_builder = ParquetRecordBatchReaderBuilder::new_with_metadata(file, meta);

        // Skip row groups whose timestamp statistics cannot overlap the query
        // window: on a multi-row-group block this turns "decode the block" into
        // "decode the slice in range".
        let mut groups =
            row_groups_in_range(reader_builder.metadata(), "timestamp_ns", start_ns, end_ns)
                .unwrap_or_else(|| (0..reader_builder.metadata().num_row_groups()).collect());

        // Then narrow by value. Time statistics alone cannot help here: a block
        // interleaves every metric it holds, so every row group in the window
        // matches and all of them get decoded.
        groups = row_groups_matching_value(&reader_builder, &groups, "metric_name", metric_name);
        // Rows are written grouped by (metric, resource), so `service_name`
        // statistics are exact per row group too. Pruning on service is what a
        // `{service.name="api"}` selector needs once the metric has narrowed
        // things down - and on a multi-tenant cluster it is usually the
        // selective dimension, not the metric.
        if let Some(service) = service_name {
            groups = row_groups_matching_value(&reader_builder, &groups, "service_name", service);
        }
        if groups.is_empty() {
            return Ok(MetricScan::default());
        }
        // Read only the seven columns this path decodes; the other eight are
        // per-row data a range query never touches.
        let mask = ProjectionMask::leaves(
            reader_builder.parquet_schema(),
            project_present(reader_builder.parquet_schema(), &METRIC_SCAN_COLUMNS),
        );
        let reader = reader_builder
            .with_row_groups(groups)
            .with_projection(mask)
            .with_batch_size(SCAN_BATCH_ROWS)
            .build()
            .map_err(|e| Error::Parquet(e.to_string()))?;

        let mut out = MetricScan::default();
        let mut resolver = SeriesResolver::new(filter);

        for record_batch in reader {
            let record_batch = record_batch.map_err(|e| Error::Parquet(e.to_string()))?;

            // Resolved by name because the projection renumbers positions.
            let [c_ts, c_name, c_svc, c_labels, c_vf, c_vi, c_vc] =
                metric_scan_indices(&record_batch.schema())?;
            let ts_arr = record_batch
                .column(c_ts)
                .as_any()
                .downcast_ref::<TimestampNanosecondArray>()
                .ok_or_else(|| Error::Arrow("Invalid timestamp column".into()))?;
            let name_arr = record_batch
                .column(c_name)
                .as_any()
                .downcast_ref::<arrow_array::DictionaryArray<arrow_array::types::Int32Type>>()
                .ok_or_else(|| Error::Arrow("Invalid metric_name column".into()))?;
            let name_values = name_arr
                .values()
                .as_any()
                .downcast_ref::<arrow_array::StringArray>()
                .ok_or_else(|| Error::Arrow("Invalid metric_name values".into()))?;
            let svc_arr = record_batch
                .column(c_svc)
                .as_any()
                .downcast_ref::<arrow_array::DictionaryArray<arrow_array::types::Int32Type>>()
                .ok_or_else(|| Error::Arrow("Invalid service_name column".into()))?;
            let svc_values = svc_arr
                .values()
                .as_any()
                .downcast_ref::<arrow_array::StringArray>()
                .ok_or_else(|| Error::Arrow("Invalid service_name values".into()))?;
            let labels_col = {
                let col = record_batch.column(c_labels);
                match col
                    .as_any()
                    .downcast_ref::<arrow_array::DictionaryArray<arrow_array::types::Int32Type>>()
                {
                    Some(d) => LabelsColumn::Dict {
                        keys: d.keys(),
                        values: d
                            .values()
                            .as_any()
                            .downcast_ref::<arrow_array::StringArray>()
                            .ok_or_else(|| Error::Arrow("Invalid labels values".into()))?,
                        values_ref: d.values(),
                    },
                    None => LabelsColumn::Plain(
                        col.as_any()
                            .downcast_ref::<arrow_array::StringArray>()
                            .ok_or_else(|| Error::Arrow("Invalid labels column".into()))?,
                    ),
                }
            };
            let vf_arr = record_batch
                .column(c_vf)
                .as_any()
                .downcast_ref::<arrow_array::Float64Array>()
                .ok_or_else(|| Error::Arrow("Invalid value_float column".into()))?;
            let vi_arr = record_batch
                .column(c_vi)
                .as_any()
                .downcast_ref::<arrow_array::Int64Array>()
                .ok_or_else(|| Error::Arrow("Invalid value_int column".into()))?;
            let vc_arr = record_batch
                .column(c_vc)
                .as_any()
                .downcast_ref::<arrow_array::StringArray>()
                .ok_or_else(|| Error::Arrow("Invalid value_complex column".into()))?;

            // Which dictionary entries carry the requested metric: one compare
            // per distinct name, then a table lookup per row.
            let name_ok: Vec<bool> = (0..name_values.len())
                .map(|i| metric_name.is_empty() || name_values.value(i) == metric_name)
                .collect();

            match &labels_col {
                LabelsColumn::Dict { values_ref, .. } => {
                    resolver.rebind(values_ref, svc_arr.values());
                }
                LabelsColumn::Plain(_) => {
                    resolver.slots.clear();
                    resolver.labels_dict = None;
                    resolver.svc_dict = None;
                }
            }
            let mut plain_cache: HashMap<(&str, &str), Option<crate::LabelSet>> = HashMap::new();

            for row in 0..record_batch.num_rows() {
                // Cheap filters first: no allocations unless the row matches.
                let t = ts_arr.value(row);
                if t < start_ns || t > end_ns {
                    continue;
                }
                if !name_ok[name_arr.keys().value(row) as usize] {
                    continue;
                }
                out.rows_considered += 1;

                let svc_key = if svc_arr.is_null(row) {
                    None
                } else {
                    Some(svc_arr.keys().value(row))
                };
                let slot: &Option<crate::LabelSet> = match &labels_col {
                    LabelsColumn::Dict { keys, values, .. } => {
                        let lk = keys.value(row);
                        let id = ((lk as u32 as u64) << 32)
                            | svc_key.map_or(u32::MAX, |k| k as u32) as u64;
                        match resolver.slots.get(&id) {
                            Some(s) => s,
                            None => {
                                let svc = svc_key.map_or("", |k| svc_values.value(k as usize));
                                let built = resolver.build(values.value(lk as usize), svc)?;
                                resolver.slots.entry(id).or_insert(built)
                            }
                        }
                    }
                    LabelsColumn::Plain(col) => {
                        let labels_json = col.value(row);
                        let svc = svc_key.map_or("", |k| svc_values.value(k as usize));
                        match plain_cache.entry((labels_json, svc)) {
                            std::collections::hash_map::Entry::Occupied(o) => &*o.into_mut(),
                            std::collections::hash_map::Entry::Vacant(v) => {
                                &*v.insert(resolver.build(labels_json, svc)?)
                            }
                        }
                    }
                };
                let Some(labels) = slot else { continue };
                out.points.push(DataPoint {
                    timestamp_ns: t,
                    value: decode_value(Some(vf_arr), Some(vi_arr), vc_arr, row)?,
                    labels: labels.clone(),
                });
            }
        }
        Ok(out)
    }

    /// Reads one block through its series table.
    ///
    /// The table names the series that qualify and the rows each occupies, so
    /// only those rows are requested from Parquet - and of those only the
    /// timestamp and value columns. Labels come from the table; the label, metric
    /// name and service columns are not read at all.
    fn scan_block_indexed(
        opened: &OpenedBlock,
        index: &SeriesIndex,
        metric_name: &str,
        start_ns: i64,
        end_ns: i64,
        selector: &SeriesSelector,
    ) -> Result<MetricScan> {
        let ids = index.select(metric_name, selector, start_ns, end_ns);
        if ids.is_empty() {
            return Ok(MetricScan::default());
        }
        let file = opened.file.try_clone().map_err(Error::Io)?;
        let builder = ParquetRecordBatchReaderBuilder::new_with_metadata(file, opened.meta.clone());
        let (groups, selection) = plan_row_selection(builder.metadata(), &row_ranges(index, &ids));
        let mask = ProjectionMask::leaves(
            builder.parquet_schema(),
            project_present(builder.parquet_schema(), &INDEXED_SCAN_COLUMNS),
        );
        let reader = builder
            .with_row_groups(groups)
            .with_row_selection(selection)
            .with_projection(mask)
            .with_batch_size(SCAN_BATCH_ROWS)
            .build()
            .map_err(|e| Error::Parquet(e.to_string()))?;

        let mut out = MetricScan::default();
        // Rows arrive in series order, `rows` of them per selected series.
        let mut series = ids.iter().map(|id| &index.series[*id as usize]);
        let mut current = series.next();
        let mut left = current.map_or(0, |s| s.rows as usize);
        for batch in reader {
            let batch = batch.map_err(|e| Error::Parquet(e.to_string()))?;
            let schema = batch.schema();
            let col = |name: &str| {
                schema
                    .index_of(name)
                    .map_err(|_| Error::Arrow(format!("metrics scan is missing the {name} column")))
            };
            let ts_arr = batch
                .column(col("timestamp_ns")?)
                .as_any()
                .downcast_ref::<TimestampNanosecondArray>()
                .ok_or_else(|| Error::Arrow("Invalid timestamp column".into()))?;
            let vf_arr = batch
                .column(col("value_float")?)
                .as_any()
                .downcast_ref::<arrow_array::Float64Array>()
                .ok_or_else(|| Error::Arrow("Invalid value_float column".into()))?;
            let vi_arr = batch
                .column(col("value_int")?)
                .as_any()
                .downcast_ref::<arrow_array::Int64Array>()
                .ok_or_else(|| Error::Arrow("Invalid value_int column".into()))?;
            let vc_arr = batch
                .column(col("value_complex")?)
                .as_any()
                .downcast_ref::<arrow_array::StringArray>()
                .ok_or_else(|| Error::Arrow("Invalid value_complex column".into()))?;

            let mut row = 0;
            while row < batch.num_rows() {
                let Some(entry) = current else {
                    return Err(Error::Internal(
                        "block holds more selected rows than its series table lists".into(),
                    ));
                };
                let take = left.min(batch.num_rows() - row);
                // A series wholly inside the window needs no per-row time check.
                let inside = entry.min_ts >= start_ns && entry.max_ts <= end_ns;
                for r in row..row + take {
                    let t = ts_arr.value(r);
                    if !inside && (t < start_ns || t > end_ns) {
                        continue;
                    }
                    out.rows_considered += 1;
                    out.points.push(DataPoint {
                        timestamp_ns: t,
                        value: decode_value(Some(vf_arr), Some(vi_arr), vc_arr, r)?,
                        labels: entry.labels.clone(),
                    });
                }
                row += take;
                left -= take;
                if left == 0 {
                    current = series.next();
                    left = current.map_or(0, |s| s.rows as usize);
                }
            }
        }
        if current.is_some() {
            return Err(Error::Internal(
                "block holds fewer selected rows than its series table lists".into(),
            ));
        }
        Ok(out)
    }
}
