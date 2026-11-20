//! Metric scans: the series-table path and the full-scan fallback.

use super::block::{open_block, OpenedBlock};
use super::pruning::{
    page_ranges_in_time, plan_row_selection, project_present, restrict_to_ranges,
    row_groups_in_range, row_groups_matching_value,
};
use super::Scanner;
use super::{
    acquire_scan_slot, record_scan_block, record_scan_output, MAX_BLOCKS, SCAN_BATCH_ROWS,
};
use crate::error::{Error, Result};
use crate::models::metrics::DataPoint;
use crate::models::storage::BlockMetadata;
use crate::storage::series_index::{row_ranges, SeriesIndex, SeriesSelector};
use arrow_array::{Array, TimestampNanosecondArray};
use parquet::arrow::arrow_reader::ParquetRecordBatchReaderBuilder;
use parquet::arrow::ProjectionMask;
use std::collections::{HashMap, HashSet};
use std::sync::Arc;

/// Columns the metrics scan decodes, in schema order.
///
/// Legacy metrics blocks have 15 columns and this fallback reads 7. New indexed
/// blocks omit `labels` and use `INDEXED_SCAN_COLUMNS` instead.
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
const INDEXED_TIMESTAMP_COLUMN: [&str; 1] = ["timestamp_ns"];

/// A metrics scan that has applied the per-query series limit before decoding values.
#[derive(Debug)]
pub struct LimitedMetricScan {
    /// Points from the smallest matching fingerprints, in block order.
    pub points: Vec<DataPoint>,
    /// Rows considered by the disk scan and memory buffer.
    pub rows_considered: u64,
    /// Distinct matching fingerprints before the series limit.
    pub total_series: usize,
    /// Counts every matched point in the visible timeline.
    pub volume: [u64; 60],
}

impl Default for LimitedMetricScan {
    fn default() -> Self {
        Self {
            points: Vec::new(),
            rows_considered: 0,
            total_series: 0,
            volume: [0; 60],
        }
    }
}

struct IndexedBlockPlan {
    opened: OpenedBlock,
    matched: Vec<(u32, u64)>,
    rows_considered: u64,
    volume: [u64; 60],
}

enum LimitedBlockPlan {
    Indexed(Box<IndexedBlockPlan>),
    Fallback(MetricScan),
}

struct TimestampScan {
    matched_ids: Vec<u32>,
    rows_considered: u64,
    volume: [u64; 60],
}

impl Default for TimestampScan {
    fn default() -> Self {
        Self {
            matched_ids: Vec::new(),
            rows_considered: 0,
            volume: [0; 60],
        }
    }
}

fn add_volume(volume: &mut [u64; 60], timestamp_ns: i64, start_ns: i64, window_ns: i64) {
    if window_ns > 0 && timestamp_ns >= start_ns {
        let bucket = ((timestamp_ns - start_ns) / window_ns).clamp(0, 59) as usize;
        volume[bucket] += 1;
    }
}

fn limit_full_scan(
    scan: MetricScan,
    buffered_points: Vec<DataPoint>,
    buffered_rows: u64,
    max_series: usize,
    volume_start_ns: i64,
    volume_window_ns: i64,
) -> LimitedMetricScan {
    let mut fingerprints: HashSet<_> = scan
        .points
        .iter()
        .chain(&buffered_points)
        .map(|point| point.labels.fingerprint())
        .collect();
    let total_series = fingerprints.len();
    let mut ordered: Vec<_> = fingerprints.drain().collect();
    ordered.sort_unstable();
    ordered.truncate(max_series);
    let kept: HashSet<_> = ordered.into_iter().collect();
    let mut volume = [0; 60];
    let points = scan
        .points
        .into_iter()
        .chain(buffered_points)
        .filter(|point| {
            add_volume(
                &mut volume,
                point.timestamp_ns,
                volume_start_ns,
                volume_window_ns,
            );
            kept.contains(&point.labels.fingerprint())
        })
        .collect();
    LimitedMetricScan {
        points,
        rows_considered: scan.rows_considered + buffered_rows,
        total_series,
        volume,
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
        let mut tasks = Vec::new();
        let mut admission_wait = std::time::Duration::ZERO;
        for block in blocks.into_iter().take(MAX_BLOCKS) {
            let m_name = metric_name.clone();
            let selector = Arc::clone(&selector);
            let waiting = std::time::Instant::now();
            let permit = acquire_scan_slot().await?;
            admission_wait += waiting.elapsed();
            tasks.push(tokio::task::spawn_blocking(move || {
                let _permit = permit;
                record_scan_block(&block.path);
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
        record_scan_output(out.rows_considered, out.points.len());
        tracing::debug!(
            metric = %metric_name,
            blocks = block_count,
            rows_considered = out.rows_considered,
            points = out.points.len(),
            admission_wait_ms = admission_wait.as_millis(),
            duration_ms = start_time.elapsed().as_millis(),
            "scan complete"
        );
        Ok(out)
    }

    /// Scans matched series while decoding values only for the smallest
    /// `max_series` fingerprints. The buffer points participate in that limit.
    #[allow(clippy::too_many_arguments)]
    pub async fn scan_filtered_limited(
        blocks: Vec<BlockMetadata>,
        metric_name: String,
        start_ns: i64,
        end_ns: i64,
        selector: SeriesSelector,
        max_series: usize,
        volume_start_ns: i64,
        volume_window_ns: i64,
        buffered_points: Vec<DataPoint>,
        buffered_rows: u64,
    ) -> Result<LimitedMetricScan> {
        let scan_started = std::time::Instant::now();
        let blocks: Vec<_> = blocks.into_iter().take(MAX_BLOCKS).collect();
        let fallback_blocks = blocks.clone();
        let selector = Arc::new(selector);
        let mut plans = Vec::with_capacity(blocks.len());
        let mut tasks = Vec::with_capacity(blocks.len());
        let mut admission_wait = std::time::Duration::ZERO;

        for block in blocks {
            let metric = metric_name.clone();
            let selector = Arc::clone(&selector);
            let waiting = std::time::Instant::now();
            let permit = acquire_scan_slot().await?;
            admission_wait += waiting.elapsed();
            tasks.push(tokio::task::spawn_blocking(move || {
                let _permit = permit;
                record_scan_block(&block.path);
                Self::plan_limited_block(
                    block,
                    &metric,
                    start_ns,
                    end_ns,
                    &selector,
                    volume_start_ns,
                    volume_window_ns,
                )
            }));
        }

        for task in tasks {
            if let Some(plan) = task.await.map_err(|e| Error::Internal(e.to_string()))?? {
                plans.push(plan);
            }
        }

        let mut fingerprints = HashSet::new();
        for plan in &plans {
            match plan {
                LimitedBlockPlan::Indexed(plan) => {
                    fingerprints.extend(plan.matched.iter().map(|(_, fingerprint)| *fingerprint));
                }
                LimitedBlockPlan::Fallback(scan) => {
                    fingerprints.extend(scan.points.iter().map(|point| point.labels.fingerprint()));
                }
            }
        }
        fingerprints.extend(
            buffered_points
                .iter()
                .map(|point| point.labels.fingerprint()),
        );
        let total_series = fingerprints.len();
        let mut kept: Vec<_> = fingerprints.into_iter().collect();
        kept.sort_unstable();
        kept.truncate(max_series);
        let kept: HashSet<_> = kept.into_iter().collect();

        let mut block_points: Vec<Vec<DataPoint>> = (0..plans.len()).map(|_| Vec::new()).collect();
        let mut rows_considered = buffered_rows;
        let mut disk_rows_considered = 0;
        let mut disk_points_returned = 0;
        let mut volume = [0; 60];

        for point in &buffered_points {
            add_volume(
                &mut volume,
                point.timestamp_ns,
                volume_start_ns,
                volume_window_ns,
            );
        }
        for (block_index, plan) in plans.iter_mut().enumerate() {
            match plan {
                LimitedBlockPlan::Fallback(scan) => {
                    rows_considered += scan.rows_considered;
                    disk_rows_considered += scan.rows_considered;
                    for point in scan.points.drain(..) {
                        add_volume(
                            &mut volume,
                            point.timestamp_ns,
                            volume_start_ns,
                            volume_window_ns,
                        );
                        if kept.contains(&point.labels.fingerprint()) {
                            disk_points_returned += 1;
                            block_points[block_index].push(point);
                        }
                    }
                }
                LimitedBlockPlan::Indexed(plan) => {
                    rows_considered += plan.rows_considered;
                    disk_rows_considered += plan.rows_considered;
                    for (bucket, count) in volume.iter_mut().zip(plan.volume) {
                        *bucket += count;
                    }
                }
            }
        }

        let mut read_tasks = Vec::new();
        for (block_index, plan) in plans.into_iter().enumerate() {
            let LimitedBlockPlan::Indexed(plan) = plan else {
                continue;
            };
            let kept_ids: Vec<_> = plan
                .matched
                .iter()
                .filter_map(|(id, fingerprint)| kept.contains(fingerprint).then_some(*id))
                .collect();
            let waiting = std::time::Instant::now();
            let permit = acquire_scan_slot().await?;
            admission_wait += waiting.elapsed();
            read_tasks.push((
                block_index,
                tokio::task::spawn_blocking(move || {
                    let _permit = permit;
                    if kept_ids.is_empty() {
                        Ok(MetricScan::default())
                    } else {
                        Self::scan_block_indexed(
                            &plan.opened,
                            plan.opened.series.as_deref().ok_or_else(|| {
                                Error::Internal(
                                    "planned indexed block lost its series table".into(),
                                )
                            })?,
                            &kept_ids,
                            start_ns,
                            end_ns,
                        )
                    }
                }),
            ));
        }

        let mut indexed_scans = Vec::with_capacity(read_tasks.len());
        let mut read_error = None;
        for (block_index, task) in read_tasks {
            match task.await {
                Ok(Ok(scan)) => indexed_scans.push((block_index, scan)),
                Ok(Err(error)) => read_error = Some(error),
                Err(error) => read_error = Some(Error::Internal(error.to_string())),
            }
        }
        if let Some(error) = read_error {
            tracing::warn!("limited indexed scan failed; retrying with the full scan: {error}");
            let scan = Self::scan_filtered(
                fallback_blocks,
                metric_name,
                start_ns,
                end_ns,
                (*selector).clone(),
            )
            .await?;
            return Ok(limit_full_scan(
                scan,
                buffered_points.clone(),
                buffered_rows,
                max_series,
                volume_start_ns,
                volume_window_ns,
            ));
        }

        for (block_index, kept_scan) in indexed_scans {
            disk_points_returned += kept_scan.points.len();
            block_points[block_index] = kept_scan.points;
        }

        let points = block_points
            .into_iter()
            .flatten()
            .chain(
                buffered_points
                    .into_iter()
                    .filter(|point| kept.contains(&point.labels.fingerprint())),
            )
            .collect::<Vec<_>>();

        record_scan_output(disk_rows_considered, disk_points_returned);
        tracing::debug!(
            metric = %metric_name,
            rows_considered,
            points = points.len(),
            series = total_series,
            admission_wait_ms = admission_wait.as_millis(),
            duration_ms = scan_started.elapsed().as_millis(),
            "limited metrics scan complete"
        );
        Ok(LimitedMetricScan {
            points,
            rows_considered,
            total_series,
            volume,
        })
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
        let has_labels_column = opened.meta.schema().index_of("labels").is_ok();
        if let Some(index) = &opened.series {
            let ids = index.select(metric_name, selector, start_ns, end_ns);
            match Self::scan_block_indexed(&opened, index, &ids, start_ns, end_ns) {
                Ok(scan) => return Ok(scan),
                // A table that disagrees with its file costs speed, not
                // correctness: the rows are still there to be scanned.
                Err(e) if has_labels_column => tracing::warn!(
                    path = ?meta.path,
                    "indexed block scan failed, scanning without the series table: {e}"
                ),
                Err(e) => return Err(e),
            }
        }
        if !has_labels_column {
            return Err(Error::Arrow(
                "metrics block without a labels column requires a valid series index".into(),
            ));
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
        let reader_builder = match page_ranges_in_time(
            reader_builder.metadata(),
            "timestamp_ns",
            start_ns,
            end_ns,
            &groups,
        ) {
            Some(ranges) if ranges.is_empty() => return Ok(MetricScan::default()),
            Some(ranges) => restrict_to_ranges(reader_builder, &ranges),
            None => reader_builder.with_row_groups(groups),
        };
        let reader = reader_builder
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

    #[allow(clippy::too_many_arguments)]
    fn plan_limited_block(
        meta: BlockMetadata,
        metric_name: &str,
        start_ns: i64,
        end_ns: i64,
        selector: &SeriesSelector,
        volume_start_ns: i64,
        volume_window_ns: i64,
    ) -> Result<Option<LimitedBlockPlan>> {
        let opened = match open_block(&meta.path) {
            Ok(Some(opened)) => opened,
            Ok(None) => return Ok(None),
            Err(error) => {
                tracing::warn!(path = ?meta.path, "limited scan planning failed; retrying block normally: {error}");
                return Self::scan_block_filtered(meta, metric_name, start_ns, end_ns, selector)
                    .map(|scan| Some(LimitedBlockPlan::Fallback(scan)));
            }
        };
        let Some(index) = &opened.series else {
            return Self::scan_block_filtered(meta, metric_name, start_ns, end_ns, selector)
                .map(|scan| Some(LimitedBlockPlan::Fallback(scan)));
        };

        let planned = (|| {
            let candidates = index.select(metric_name, selector, start_ns, end_ns);
            let candidates: Vec<_> = candidates
                .into_iter()
                .map(|id| Ok((id, index.labels(id)?.fingerprint())))
                .collect::<Result<_>>()?;
            let timestamps = Self::scan_block_indexed_timestamps(
                &opened,
                index,
                &candidates.iter().map(|(id, _)| *id).collect::<Vec<_>>(),
                start_ns,
                end_ns,
                volume_start_ns,
                volume_window_ns,
            )?;
            let matched_ids: HashSet<_> = timestamps.matched_ids.iter().copied().collect();
            let matched = candidates
                .into_iter()
                .filter(|(id, _)| matched_ids.contains(id))
                .collect();
            Ok::<_, Error>((matched, timestamps.rows_considered, timestamps.volume))
        })();

        match planned {
            Ok((matched, rows_considered, volume)) => Ok(Some(LimitedBlockPlan::Indexed(
                Box::new(IndexedBlockPlan {
                    opened,
                    matched,
                    rows_considered,
                    volume,
                }),
            ))),
            Err(error) => {
                tracing::warn!(path = ?meta.path, "limited scan planning failed; retrying block normally: {error}");
                Self::scan_block_filtered(meta, metric_name, start_ns, end_ns, selector)
                    .map(|scan| Some(LimitedBlockPlan::Fallback(scan)))
            }
        }
    }

    fn scan_block_indexed_timestamps(
        opened: &OpenedBlock,
        index: &SeriesIndex,
        ids: &[u32],
        start_ns: i64,
        end_ns: i64,
        volume_start_ns: i64,
        volume_window_ns: i64,
    ) -> Result<TimestampScan> {
        if ids.is_empty() {
            return Ok(TimestampScan::default());
        }
        let file = opened.file.try_clone().map_err(Error::Io)?;
        let builder = ParquetRecordBatchReaderBuilder::new_with_metadata(file, opened.meta.clone());
        let (groups, selection) = plan_row_selection(builder.metadata(), &row_ranges(index, ids));
        let mask = ProjectionMask::leaves(
            builder.parquet_schema(),
            project_present(builder.parquet_schema(), &INDEXED_TIMESTAMP_COLUMN),
        );
        let reader = builder
            .with_row_groups(groups)
            .with_row_selection(selection)
            .with_projection(mask)
            .with_batch_size(SCAN_BATCH_ROWS)
            .build()
            .map_err(|e| Error::Parquet(e.to_string()))?;

        let mut out = TimestampScan::default();
        let mut series = ids.iter().map(|id| -> Result<_> {
            let (_, rows, min_ts, max_ts) = index.bounds(*id).ok_or_else(|| {
                Error::Internal("selected series id is missing from its table".into())
            })?;
            Ok((*id, rows as usize, min_ts, max_ts))
        });
        let mut current = series.next().transpose()?;
        let mut left = current.as_ref().map_or(0, |(_, rows, _, _)| *rows);
        let mut current_matched = false;
        for batch in reader {
            let batch = batch.map_err(|e| Error::Parquet(e.to_string()))?;
            let ts_idx = batch.schema().index_of("timestamp_ns").map_err(|_| {
                Error::Arrow("metrics scan is missing the timestamp_ns column".into())
            })?;
            let ts_arr = batch
                .column(ts_idx)
                .as_any()
                .downcast_ref::<TimestampNanosecondArray>()
                .ok_or_else(|| Error::Arrow("Invalid timestamp column".into()))?;
            let mut row = 0;
            while row < batch.num_rows() {
                let Some((_, _, min_ts, max_ts)) = current else {
                    return Err(Error::Internal(
                        "block holds more selected rows than its series table lists".into(),
                    ));
                };
                let take = left.min(batch.num_rows() - row);
                let inside = min_ts >= start_ns && max_ts <= end_ns;
                for r in row..row + take {
                    let timestamp = ts_arr.value(r);
                    if inside || (timestamp >= start_ns && timestamp <= end_ns) {
                        out.rows_considered += 1;
                        current_matched = true;
                        add_volume(
                            &mut out.volume,
                            timestamp,
                            volume_start_ns,
                            volume_window_ns,
                        );
                    }
                }
                row += take;
                left -= take;
                if left == 0 {
                    if current_matched {
                        if let Some((id, _, _, _)) = current {
                            out.matched_ids.push(id);
                        }
                    }
                    current = series.next().transpose()?;
                    left = current.as_ref().map_or(0, |(_, rows, _, _)| *rows);
                    current_matched = false;
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

    /// Reads one block through its series table.
    ///
    /// The table names the series that qualify and the rows each occupies, so
    /// only those rows are requested from Parquet - and of those only the
    /// timestamp and value columns. Labels come from the table; the label, metric
    /// name and service columns are not read at all.
    fn scan_block_indexed(
        opened: &OpenedBlock,
        index: &SeriesIndex,
        ids: &[u32],
        start_ns: i64,
        end_ns: i64,
    ) -> Result<MetricScan> {
        if ids.is_empty() {
            return Ok(MetricScan::default());
        }
        let file = opened.file.try_clone().map_err(Error::Io)?;
        let builder = ParquetRecordBatchReaderBuilder::new_with_metadata(file, opened.meta.clone());
        let (groups, selection) = plan_row_selection(builder.metadata(), &row_ranges(index, ids));
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
        let mut series = ids.iter().map(|id| -> Result<_> {
            let (_, rows, min_ts, max_ts) = index.bounds(*id).ok_or_else(|| {
                Error::Internal("selected series id is missing from its table".into())
            })?;
            Ok((rows as usize, min_ts, max_ts, index.labels(*id)?))
        });
        let mut current = series.next().transpose()?;
        let mut left = current.as_ref().map_or(0, |(rows, _, _, _)| *rows);
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
                let Some((_, _, _, labels)) = current.as_ref() else {
                    return Err(Error::Internal(
                        "block holds more selected rows than its series table lists".into(),
                    ));
                };
                let take = left.min(batch.num_rows() - row);
                // A series wholly inside the window needs no per-row time check.
                let inside = current
                    .as_ref()
                    .is_some_and(|(_, min_ts, max_ts, _)| *min_ts >= start_ns && *max_ts <= end_ns);
                for r in row..row + take {
                    let t = ts_arr.value(r);
                    if !inside && (t < start_ns || t > end_ns) {
                        continue;
                    }
                    out.rows_considered += 1;
                    out.points.push(DataPoint {
                        timestamp_ns: t,
                        value: decode_value(Some(vf_arr), Some(vi_arr), vc_arr, r)?,
                        labels: labels.clone(),
                    });
                }
                row += take;
                left -= take;
                if left == 0 {
                    current = series.next().transpose()?;
                    left = current.as_ref().map_or(0, |(rows, _, _, _)| *rows);
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
