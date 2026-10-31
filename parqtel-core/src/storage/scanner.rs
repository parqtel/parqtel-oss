use crate::error::{Error, Result};
use crate::models::logs::LogRecord;
use crate::models::metrics::DataPoint;
use crate::models::storage::{BlockMetadata, StorageModel};
use crate::models::traces::Span;
use crate::storage::log_index::{LogIndexTable, LogSkipIndex, LOG_INDEX_FOOTER_KEY};
use crate::storage::series_index::{
    row_ranges, SeriesIndex, SeriesSelector, SeriesTable, SERIES_INDEX_FOOTER_KEY,
};
use arrow_array::{Array, TimestampNanosecondArray};
use parquet::arrow::arrow_reader::{
    ArrowPredicateFn, ArrowReaderMetadata, ArrowReaderOptions, ParquetRecordBatchReaderBuilder,
    RowFilter,
};
use parquet::arrow::ProjectionMask;
use parquet::file::metadata::PageIndexPolicy;
use parquet::file::metadata::ParquetMetaData;
use parquet::file::statistics::Statistics;
use std::collections::HashMap;
use std::fs::File;
use std::sync::Arc;

/// Max concurrent blocking scans; also bounds tokio blocking-pool usage.
const MAX_CONCURRENT: usize = 16;
/// Hard cap on blocks scanned per query.
const MAX_BLOCKS: usize = 128;

/// Row predicate pushed down into blocking scan tasks.
pub type LogRowFilter = Arc<dyn Fn(&LogRecord) -> bool + Send + Sync>;

/// Conditions on log columns that every matching row must satisfy, so a block
/// scan can reject rows from three columns before decoding the other sixteen.
///
/// This is a *necessary* condition only: the row predicate still decides every
/// row that passes. Whoever builds one must derive it from conjuncts of that
/// predicate, never from an `OR` or a negation.
#[derive(Debug, Clone, Default)]
pub struct LogColumnPrefilter {
    /// `severity_number >= n`.
    pub min_severity: Option<i32>,
    /// Lowercase needles; the body must contain every one, ASCII-case-insensitively.
    pub body_contains_lower: Vec<String>,
    /// `name = value` pairs that the row's attributes or resource attributes must
    /// contain. Used only to skip granules; they never decide a row.
    pub attr_equals: Vec<(String, String)>,
    /// The row predicate is *equivalent* to the conditions above, not merely
    /// implied by them. The scan may then count, bucket and rank matching rows
    /// from the timestamp, severity and body columns alone, and decode the other
    /// columns only for the rows it returns. Set it only when nothing else
    /// constrains a row.
    pub exact: bool,
}

impl LogColumnPrefilter {
    /// Whether this rules nothing out.
    pub fn is_empty(&self) -> bool {
        self.min_severity.is_none()
            && self.body_contains_lower.is_empty()
            && self.attr_equals.is_empty()
    }
}

/// Aggregate statistics for a counted log scan: the true match count
/// (never capped by `keep`) and 60 ingestion-volume buckets spread over
/// `[start_ns, end_ns]`.
#[derive(Debug, Default)]
pub struct LogScanStats {
    pub total_matched: usize,
    pub volume_buckets: Vec<u64>,
}
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

/// Leaf indices of `columns` that the file's schema actually has.
///
/// Blocks written by older builds may lack a column, and a projection naming an
/// absent column errors, so the projection is intersected with the file rather
/// than applied blindly. Leaf indices are correct here because every parqtel
/// schema is flat - no nested groups - so a leaf index equals its root index.
pub fn project_present(
    schema: &parquet::schema::types::SchemaDescriptor,
    columns: &[&str],
) -> Vec<usize> {
    columns
        .iter()
        .filter_map(|name| schema.columns().iter().position(|c| c.name() == *name))
        .collect()
}

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

/// Row mask for a [`LogColumnPrefilter`] over a batch holding exactly
/// `timestamp_ns`, `severity_number` and `body` (in file order).
fn log_prefilter_mask(
    batch: &arrow_array::RecordBatch,
    pre: &LogColumnPrefilter,
    start_ns: i64,
    end_ns: i64,
) -> std::result::Result<arrow_array::BooleanArray, arrow_schema::ArrowError> {
    let bad = |what: &str| arrow_schema::ArrowError::SchemaError(format!("log prefilter: {what}"));
    let schema = batch.schema();
    let col = |name: &str| {
        schema
            .index_of(name)
            .map(|i| batch.column(i))
            .map_err(|_| bad(name))
    };
    let ts = col("timestamp_ns")?
        .as_any()
        .downcast_ref::<TimestampNanosecondArray>()
        .ok_or_else(|| bad("timestamp_ns type"))?;
    let sev = col("severity_number")?
        .as_any()
        .downcast_ref::<arrow_array::Int32Array>()
        .ok_or_else(|| bad("severity_number type"))?;
    let body = col("body")?
        .as_any()
        .downcast_ref::<arrow_array::StringArray>()
        .ok_or_else(|| bad("body type"))?;
    // Prepared once per batch, not once per row.
    let needles: Vec<crate::text::CiNeedle> = pre
        .body_contains_lower
        .iter()
        .map(|n| crate::text::CiNeedle::new(n))
        .collect();
    Ok((0..batch.num_rows())
        .map(|i| {
            let t = ts.value(i);
            t >= start_ns
                && t <= end_ns
                && pre.min_severity.is_none_or(|m| sev.value(i) >= m)
                && needles.iter().all(|n| n.is_in(body.value(i)))
        })
        .collect())
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

/// What is kept per block file: its parsed footer (with page indexes) and, for
/// metric blocks, the queryable series table that footer carried.
///
/// Block files are immutable once renamed into place, so a footer never goes
/// stale for a given (path, length, mtime). Re-reading and re-decoding it for
/// every block of every query was a fixed cost paid before any data moved - and
/// the series table is costlier still to rebuild (JSON parse, label interning,
/// postings), so it is parsed once and shared.
struct CachedBlock {
    meta: Arc<ParquetMetaData>,
    series: Option<Arc<SeriesIndex>>,
    log_index: Option<Arc<LogSkipIndex>>,
    bytes: usize,
    used: u64,
}

type BlockKey = (std::path::PathBuf, u64, i128);

struct FooterCache {
    entries: HashMap<BlockKey, CachedBlock>,
    bytes: usize,
    tick: u64,
}

/// Memory the footer cache may hold, counted in bytes rather than entries: a
/// footer with page indexes and a series table for a hundred thousand series is
/// orders of magnitude larger than one for a small log block.
const FOOTER_CACHE_BUDGET: usize = 192 * 1024 * 1024;

fn footer_cache() -> &'static std::sync::Mutex<FooterCache> {
    static CACHE: std::sync::OnceLock<std::sync::Mutex<FooterCache>> = std::sync::OnceLock::new();
    CACHE.get_or_init(|| {
        std::sync::Mutex::new(FooterCache {
            entries: HashMap::new(),
            bytes: 0,
            tick: 0,
        })
    })
}

impl FooterCache {
    /// Inserts `block`, evicting the least recently used entries until the cache
    /// is back under budget. An entry larger than the whole budget is not kept.
    fn insert(&mut self, key: BlockKey, mut block: CachedBlock) {
        if block.bytes > FOOTER_CACHE_BUDGET {
            return;
        }
        self.tick += 1;
        block.used = self.tick;
        let added = block.bytes;
        if let Some(old) = self.entries.insert(key, block) {
            self.bytes -= old.bytes;
        }
        self.bytes += added;
        if self.bytes <= FOOTER_CACHE_BUDGET {
            return;
        }
        // Drop the least recently used until a quarter of the budget is free, so
        // eviction is not triggered again by the very next insert.
        let mut by_age: Vec<(u64, BlockKey)> = self
            .entries
            .iter()
            .map(|(k, e)| (e.used, k.clone()))
            .collect();
        by_age.sort_unstable_by_key(|(used, _)| *used);
        let target = FOOTER_CACHE_BUDGET / 4 * 3;
        for (_, k) in by_age {
            if self.bytes <= target {
                break;
            }
            if let Some(e) = self.entries.remove(&k) {
                self.bytes -= e.bytes;
            }
        }
    }
}

/// A block opened for reading.
struct OpenedBlock {
    file: File,
    meta: ArrowReaderMetadata,
    /// The block's series table, when it has a valid one.
    series: Option<Arc<SeriesIndex>>,
    /// The block's log skip index, when it has a valid one.
    log_index: Option<Arc<LogSkipIndex>>,
}

/// A block's footer and the indexes lifted out of it.
struct LoadedBlock {
    meta: Arc<ParquetMetaData>,
    series: Option<Arc<SeriesIndex>>,
    log_index: Option<Arc<LogSkipIndex>>,
}

/// Parses the footer of `file`, lifting its series table or log index out.
///
/// Either can be megabytes of text. It is parsed into its queryable form and then
/// **removed from the cached footer**, so the text is not held a second time next
/// to the structure built from it. An index that does not parse, or that does not
/// account for exactly the file's rows, is dropped (and logged): the block is then
/// scanned without it.
fn load_block(file: &File) -> Result<LoadedBlock> {
    let md = parquet::file::metadata::ParquetMetaDataReader::new()
        .with_page_index_policy(PageIndexPolicy::Optional)
        .parse_and_finish(file)
        .map_err(|e| Error::Parquet(e.to_string()))?;

    let find = |key: &str| {
        md.file_metadata()
            .key_value_metadata()
            .and_then(|kv| kv.iter().find(|k| k.key == key))
            .and_then(|k| k.value.clone())
    };
    let (series_text, log_text) = (find(SERIES_INDEX_FOOTER_KEY), find(LOG_INDEX_FOOTER_KEY));
    if series_text.is_none() && log_text.is_none() {
        return Ok(LoadedBlock {
            meta: Arc::new(md),
            series: None,
            log_index: None,
        });
    }
    let rows = md.file_metadata().num_rows().max(0) as u64;

    let series = series_text.and_then(|text| {
        match SeriesTable::from_footer_value(&text).and_then(|t| SeriesIndex::from_table(&t)) {
            // The table must account for every row, or it describes some other file.
            Ok(idx) if idx.total_rows() == rows => Some(Arc::new(idx)),
            Ok(_) => {
                tracing::warn!("series table disagrees with the block's row count; ignoring it");
                None
            }
            Err(e) => {
                tracing::warn!("unreadable series table, scanning without it: {e}");
                None
            }
        }
    });
    let log_index = log_text.and_then(|text| {
        match LogIndexTable::from_footer_value(&text).and_then(|t| LogSkipIndex::from_table(&t)) {
            Ok(idx) if idx.rows() == rows => Some(Arc::new(idx)),
            Ok(_) => {
                tracing::warn!("log index disagrees with the block's row count; ignoring it");
                None
            }
            Err(e) => {
                tracing::warn!("unreadable log index, scanning without it: {e}");
                None
            }
        }
    });

    // Rebuild the footer without the index text.
    let fm = md.file_metadata();
    let kept: Vec<_> = fm
        .key_value_metadata()
        .map(|kv| {
            kv.iter()
                .filter(|k| k.key != SERIES_INDEX_FOOTER_KEY && k.key != LOG_INDEX_FOOTER_KEY)
                .cloned()
                .collect()
        })
        .unwrap_or_default();
    let stripped = parquet::file::metadata::FileMetaData::new(
        fm.version(),
        fm.num_rows(),
        fm.created_by().map(str::to_string),
        Some(kept),
        fm.schema_descr_ptr(),
        fm.column_orders().cloned(),
    );
    let mut builder = md.into_builder();
    let (row_groups, column_index, offset_index) = (
        builder.take_row_groups(),
        builder.take_column_index(),
        builder.take_offset_index(),
    );
    let md = parquet::file::metadata::ParquetMetaDataBuilder::new(stripped)
        .set_row_groups(row_groups)
        .set_column_index(column_index)
        .set_offset_index(offset_index)
        .build();
    Ok(LoadedBlock {
        meta: Arc::new(md),
        series,
        log_index,
    })
}

/// Opens a block and returns its reader metadata, or `None` if the file is gone.
///
/// The `labels` column is requested as a dictionary: every row of a series
/// carries the same JSON text, and Parquet already stores it dictionary-encoded,
/// so this hands the reader's dictionary straight through instead of expanding
/// one string per row. Falls back to the file's own schema if the override is
/// not accepted.
fn open_block(path: &std::path::Path) -> Result<Option<OpenedBlock>> {
    let file = match File::open(path) {
        Ok(f) => f,
        Err(e) if e.kind() == std::io::ErrorKind::NotFound => {
            tracing::warn!("Block file not found, skipping: {:?}", path);
            return Ok(None);
        }
        Err(e) => return Err(Error::Io(e)),
    };
    // Metadata is kept, not just the builder: bloom filters and the column
    // index live on it, and both prune row groups before any page is decoded.
    //
    // The page index is what makes column- and page-level pruning possible;
    // without it only row-group statistics exist. `Optional` rather than
    // `Required`, so a block without one (an older build's) is still readable.
    let options = ArrowReaderOptions::new().with_page_index_policy(PageIndexPolicy::Optional);
    let stamp = file.metadata().ok().map(|m| {
        let mtime = m
            .modified()
            .ok()
            .and_then(|t| t.duration_since(std::time::UNIX_EPOCH).ok())
            .map_or(0, |d| d.as_nanos() as i128);
        (m.len(), mtime)
    });
    let key = stamp.map(|(len, mtime)| (path.to_path_buf(), len, mtime));

    let cached = key.as_ref().and_then(|k| {
        let mut cache = footer_cache().lock().unwrap_or_else(|e| e.into_inner());
        cache.tick += 1;
        let tick = cache.tick;
        cache.entries.get_mut(k).map(|e| {
            e.used = tick;
            (Arc::clone(&e.meta), e.series.clone(), e.log_index.clone())
        })
    });
    let (parsed, series, log_index) = match cached {
        Some(hit) => hit,
        None => {
            let LoadedBlock {
                meta,
                series,
                log_index,
            } = load_block(&file)?;
            if let Some(k) = key {
                let bytes = meta.memory_size()
                    + series.as_ref().map_or(0, |s| s.approx_bytes())
                    + log_index.as_ref().map_or(0, |s| s.approx_bytes());
                let mut cache = footer_cache().lock().unwrap_or_else(|e| e.into_inner());
                cache.insert(
                    k,
                    CachedBlock {
                        meta: Arc::clone(&meta),
                        series: series.clone(),
                        log_index: log_index.clone(),
                        bytes,
                        used: 0,
                    },
                );
            }
            (meta, series, log_index)
        }
    };
    let base = ArrowReaderMetadata::try_new(parsed, options.clone())
        .map_err(|e| Error::Parquet(e.to_string()))?;

    let meta = match dictionary_labels_schema(base.schema()) {
        Some(schema) => {
            ArrowReaderMetadata::try_new(Arc::clone(base.metadata()), options.with_schema(schema))
                .unwrap_or(base)
        }
        None => base,
    };
    Ok(Some(OpenedBlock {
        file,
        meta,
        series,
        log_index,
    }))
}

/// `schema` with the `labels` column typed as `Dictionary(Int32, Utf8)`.
fn dictionary_labels_schema(schema: &arrow_schema::SchemaRef) -> Option<arrow_schema::SchemaRef> {
    use arrow_schema::DataType;
    let idx = schema.index_of("labels").ok()?;
    let field = schema.field(idx);
    if field.data_type() != &DataType::Utf8 {
        return None;
    }
    let mut fields: Vec<arrow_schema::FieldRef> = schema.fields().iter().cloned().collect();
    fields[idx] = Arc::new(field.as_ref().clone().with_data_type(DataType::Dictionary(
        Box::new(DataType::Int32),
        Box::new(DataType::Utf8),
    )));
    Some(Arc::new(arrow_schema::Schema::new_with_metadata(
        fields,
        schema.metadata().clone(),
    )))
}

/// Rows read per Arrow batch. The default (1024) makes per-batch setup - and,
/// for dictionary columns, per-batch resolution - a visible share of the work.
const SCAN_BATCH_ROWS: usize = 8192;

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
    fn new(filter: Option<SeriesFilter>) -> Self {
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
    fn build(&self, labels_json: &str, svc: &str) -> Result<Option<crate::LabelSet>> {
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

/// Reads and filters data points from Parquet files.
pub struct Scanner;

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

    /// Scans a set of blocks for log records matching a time range.
    pub async fn scan_logs(
        blocks: Vec<BlockMetadata>,
        start_ns: i64,
        end_ns: i64,
    ) -> Result<Vec<LogRecord>> {
        // Full-materialization path (pipelines/correlate need every row).
        let (logs, _) =
            Self::scan_logs_counted_filtered(blocks, start_ns, end_ns, usize::MAX, true, None)
                .await?;
        Ok(logs)
    }

    /// Scans blocks for logs with a pre-filter pushed down into the scan so
    /// non-matching rows never materialize, while still counting every
    /// matching row and bucketing it for volume summaries. `keep` bounds
    /// how many records are retained in memory: with `keep_newest` the
    /// NEWEST `keep` matches are kept (Loki-style "last N logs"), otherwise
    /// the OLDEST `keep` — rows inside a flushed block are stored in
    /// timestamp order (both flush and compaction sort before writing).
    ///
    /// The filter runs inside each blocking task; it must be cheap
    /// (label-equality checks over already-decoded rows).
    #[allow(clippy::too_many_arguments)]
    pub async fn scan_logs_counted_filtered(
        blocks: Vec<BlockMetadata>,
        start_ns: i64,
        end_ns: i64,
        keep: usize,
        keep_newest: bool,
        filter: Option<LogRowFilter>,
    ) -> Result<(Vec<LogRecord>, LogScanStats)> {
        Self::scan_logs_counted_prefiltered(
            blocks,
            start_ns,
            end_ns,
            keep,
            keep_newest,
            filter,
            None,
        )
        .await
    }

    /// As [`Self::scan_logs_counted_filtered`], also rejecting rows on
    /// `prefilter` from the time, severity and body columns alone, so only rows
    /// that can match are fully decoded. `filter` still has the last word.
    #[allow(clippy::too_many_arguments)]
    pub async fn scan_logs_counted_prefiltered(
        blocks: Vec<BlockMetadata>,
        start_ns: i64,
        end_ns: i64,
        keep: usize,
        keep_newest: bool,
        filter: Option<LogRowFilter>,
        prefilter: Option<LogColumnPrefilter>,
    ) -> Result<(Vec<LogRecord>, LogScanStats)> {
        let sem = Arc::new(tokio::sync::Semaphore::new(MAX_CONCURRENT));
        let window_ns = (end_ns - start_ns) / 60;
        let mut tasks = Vec::new();
        for block in blocks.into_iter().take(MAX_BLOCKS) {
            let permit = sem
                .clone()
                .acquire_owned()
                .await
                .map_err(|e| Error::Internal(e.to_string()))?;
            let filter = filter.clone();
            let prefilter = prefilter.clone();
            tasks.push(tokio::task::spawn_blocking(move || {
                let _permit = permit;
                Self::scan_log_block_filtered(
                    block,
                    start_ns,
                    end_ns,
                    keep,
                    keep_newest,
                    filter,
                    prefilter,
                    window_ns,
                )
            }));
        }

        let mut all_logs = Vec::new();
        let mut stats = LogScanStats {
            total_matched: 0,
            volume_buckets: vec![0; 60],
        };
        for task in tasks {
            let (logs, matched, buckets) =
                task.await.map_err(|e| Error::Internal(e.to_string()))??;
            stats.total_matched += matched;
            for (acc, b) in stats.volume_buckets.iter_mut().zip(buckets) {
                *acc += b;
            }
            all_logs.extend(logs);
            // Incremental trim keeps the working set at ~`keep` records
            // even when many blocks are in range.
            all_logs.sort_by_key(|l| l.timestamp_ns);
            if all_logs.len() > keep {
                if keep_newest {
                    let excess = all_logs.len() - keep;
                    all_logs.drain(..excess);
                } else {
                    all_logs.truncate(keep);
                }
            }
        }
        Ok((all_logs, stats))
    }

    #[allow(clippy::too_many_arguments)]
    fn scan_log_block_filtered(
        meta: BlockMetadata,
        start_ns: i64,
        end_ns: i64,
        keep: usize,
        keep_newest: bool,
        filter: Option<LogRowFilter>,
        prefilter: Option<LogColumnPrefilter>,
        window_ns: i64,
    ) -> Result<(Vec<LogRecord>, usize, Vec<u64>)> {
        // The cached footer carries the page index, which is what lets the row
        // filter below skip pages rather than decode them.
        let Some(opened) = open_block(&meta.path)? else {
            return Ok((Vec::new(), 0, vec![0; 60]));
        };
        if let Some(pre) = prefilter.as_ref().filter(|p| p.exact && !p.is_empty()) {
            if let Some(found) = Self::scan_log_block_exact(
                &opened,
                pre,
                start_ns,
                end_ns,
                keep,
                keep_newest,
                window_ns,
            )? {
                return Ok(found);
            }
        }
        let ranges = log_row_ranges(&opened, start_ns, end_ns, prefilter.as_ref());
        if ranges.is_empty() {
            return Ok((Vec::new(), 0, vec![0; 60]));
        }
        let reader_builder =
            ParquetRecordBatchReaderBuilder::new_with_metadata(opened.file, opened.meta);
        let reader_builder = restrict_to_ranges(reader_builder, &ranges);

        // Late materialisation: decide from timestamp, severity and body alone,
        // and let the reader decode the other columns only for the rows (and
        // pages) that pass.
        let reader_builder = match prefilter.filter(|p| !p.is_empty()) {
            Some(pre) => {
                let schema = reader_builder.parquet_schema();
                let cols = project_present(schema, &["timestamp_ns", "severity_number", "body"]);
                if cols.len() == 3 {
                    let mask = ProjectionMask::leaves(schema, cols);
                    let predicate = ArrowPredicateFn::new(mask, move |batch| {
                        log_prefilter_mask(&batch, &pre, start_ns, end_ns)
                    });
                    reader_builder.with_row_filter(RowFilter::new(vec![Box::new(predicate)]))
                } else {
                    reader_builder
                }
            }
            None => reader_builder,
        };

        let reader = reader_builder
            .with_batch_size(SCAN_BATCH_ROWS)
            .build()
            .map_err(|e| Error::Parquet(e.to_string()))?;

        let mut kept: std::collections::VecDeque<LogRecord> = std::collections::VecDeque::new();
        let mut matched = 0usize;
        let mut buckets = vec![0u64; 60];
        for record_batch in reader {
            let record_batch = record_batch.map_err(|e| Error::Parquet(e.to_string()))?;

            // Cache keys borrow from this chunk — recreate per chunk.
            let mut attr_cache: HashMap<&str, crate::LabelSet> = HashMap::new();
            let mut res_cache: HashMap<&str, crate::LabelSet> = HashMap::new();
            // Cheap reject first: pulling a row out of the batch costs a JSON
            // attribute parse, so rows outside the window must never get there.
            let ts_arr = record_batch
                .column(0)
                .as_any()
                .downcast_ref::<TimestampNanosecondArray>()
                .ok_or_else(|| Error::Arrow("Invalid timestamp column".into()))?;
            for row in 0..record_batch.num_rows() {
                let timestamp_ns = ts_arr.value(row);
                if timestamp_ns < start_ns || timestamp_ns > end_ns {
                    continue;
                }
                let log =
                    StorageModel::row_to_log(&record_batch, row, &mut attr_cache, &mut res_cache)?;
                if let Some(f) = &filter {
                    if !f(&log) {
                        continue;
                    }
                }
                matched += 1;
                if window_ns > 0 {
                    let bucket = ((log.timestamp_ns - start_ns) / window_ns).clamp(0, 59) as usize;
                    buckets[bucket] += 1;
                }
                if keep > 0 {
                    if keep_newest && kept.len() >= keep {
                        kept.pop_front();
                    }
                    if kept.len() < keep {
                        kept.push_back(log);
                    }
                }
            }
        }
        Ok((kept.into_iter().collect(), matched, buckets))
    }

    /// Two-phase scan for a predicate that the column prefilter fully decides.
    ///
    /// Phase one reads only timestamp, severity and body, and from them counts
    /// every match, buckets it for the volume histogram and remembers the rows
    /// that will be returned. Phase two reads all columns for just those rows.
    /// A search that matches a fifth of a block used to materialise a fifth of
    /// it, to keep a hundred rows.
    ///
    /// Returns `None` when the block lacks the columns this needs, so the caller
    /// scans it the ordinary way.
    #[allow(clippy::type_complexity)]
    fn scan_log_block_exact(
        opened: &OpenedBlock,
        pre: &LogColumnPrefilter,
        start_ns: i64,
        end_ns: i64,
        keep: usize,
        keep_newest: bool,
        window_ns: i64,
    ) -> Result<Option<(Vec<LogRecord>, usize, Vec<u64>)>> {
        let ranges = log_row_ranges(opened, start_ns, end_ns, Some(pre));
        if ranges.is_empty() {
            return Ok(Some((Vec::new(), 0, vec![0; 60])));
        }
        let builder = ParquetRecordBatchReaderBuilder::new_with_metadata(
            opened.file.try_clone().map_err(Error::Io)?,
            opened.meta.clone(),
        );
        let cols = project_present(
            builder.parquet_schema(),
            &["timestamp_ns", "severity_number", "body"],
        );
        if cols.len() != 3 {
            return Ok(None);
        }
        let mask = ProjectionMask::leaves(builder.parquet_schema(), cols);

        // Where each range starts in the stream the reader will produce, to turn
        // a position in that stream back into a row of the file.
        let mut stream_ranges: Vec<(u64, u64)> = Vec::with_capacity(ranges.len());
        let mut stream_pos = 0u64;
        for (a, b) in &ranges {
            stream_ranges.push((stream_pos, *a));
            stream_pos += b - a;
        }
        let builder = restrict_to_ranges(builder, &ranges);

        let reader = builder
            .with_projection(mask)
            .with_batch_size(SCAN_BATCH_ROWS)
            .build()
            .map_err(|e| Error::Parquet(e.to_string()))?;

        let mut matched = 0usize;
        let mut buckets = vec![0u64; 60];
        // The `keep` best rows by (timestamp, row), as a bounded heap whose top is
        // the first to be displaced.
        let mut best: std::collections::BinaryHeap<std::cmp::Reverse<(i64, u64)>> =
            std::collections::BinaryHeap::new();
        let mut best_oldest: std::collections::BinaryHeap<(i64, u64)> =
            std::collections::BinaryHeap::new();
        let mut pos = 0u64;
        let mut group = 0usize;
        for batch in reader {
            let batch = batch.map_err(|e| Error::Parquet(e.to_string()))?;
            let hits = log_prefilter_mask(&batch, pre, start_ns, end_ns)
                .map_err(|e| Error::Arrow(e.to_string()))?;
            let ts = batch
                .column(
                    batch
                        .schema()
                        .index_of("timestamp_ns")
                        .map_err(|e| Error::Arrow(e.to_string()))?,
                )
                .as_any()
                .downcast_ref::<TimestampNanosecondArray>()
                .ok_or_else(|| Error::Arrow("Invalid timestamp column".into()))?;
            for row in 0..batch.num_rows() {
                let here = pos + row as u64;
                if !hits.value(row) {
                    continue;
                }
                let t = ts.value(row);
                matched += 1;
                if window_ns > 0 {
                    buckets[((t - start_ns) / window_ns).clamp(0, 59) as usize] += 1;
                }
                if keep == 0 {
                    continue;
                }
                while group + 1 < stream_ranges.len() && stream_ranges[group + 1].0 <= here {
                    group += 1;
                }
                let file_row = stream_ranges[group].1 + (here - stream_ranges[group].0);
                if keep_newest {
                    best.push(std::cmp::Reverse((t, file_row)));
                    if best.len() > keep {
                        best.pop();
                    }
                } else {
                    best_oldest.push((t, file_row));
                    if best_oldest.len() > keep {
                        best_oldest.pop();
                    }
                }
            }
            pos += batch.num_rows() as u64;
        }

        let mut rows: Vec<u64> = if keep_newest {
            best.into_iter()
                .map(|std::cmp::Reverse((_, r))| r)
                .collect()
        } else {
            best_oldest.into_iter().map(|(_, r)| r).collect()
        };
        if rows.is_empty() {
            return Ok(Some((Vec::new(), matched, buckets)));
        }
        rows.sort_unstable();

        // Phase two: every column, for the rows being returned.
        let mut ranges: Vec<(u64, u64)> = Vec::new();
        for r in &rows {
            match ranges.last_mut() {
                Some(last) if last.1 == *r => last.1 += 1,
                _ => ranges.push((*r, *r + 1)),
            }
        }
        let builder = ParquetRecordBatchReaderBuilder::new_with_metadata(
            opened.file.try_clone().map_err(Error::Io)?,
            opened.meta.clone(),
        );
        let (groups, selection) = plan_row_selection(builder.metadata(), &ranges);
        let reader = builder
            .with_row_groups(groups)
            .with_row_selection(selection)
            .with_batch_size(SCAN_BATCH_ROWS)
            .build()
            .map_err(|e| Error::Parquet(e.to_string()))?;
        let mut logs = Vec::with_capacity(rows.len());
        for batch in reader {
            let batch = batch.map_err(|e| Error::Parquet(e.to_string()))?;
            let mut attr_cache: HashMap<&str, crate::LabelSet> = HashMap::new();
            let mut res_cache: HashMap<&str, crate::LabelSet> = HashMap::new();
            for row in 0..batch.num_rows() {
                logs.push(StorageModel::row_to_log(
                    &batch,
                    row,
                    &mut attr_cache,
                    &mut res_cache,
                )?);
            }
        }
        Ok(Some((logs, matched, buckets)))
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

        // Prune by the column the caller filters on (`start_time_ns`, index 4 of
        // traces_schema) rather than the block's own timestamp column.
        let reader_builder =
            match row_groups_in_range(reader_builder.metadata(), "start_time_ns", start_ns, end_ns)
            {
                Some(groups) if groups.is_empty() => return Ok(Vec::new()),
                Some(groups) => reader_builder.with_row_groups(groups),
                None => reader_builder,
            };

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

            // Cheap reject first: `row_to_span` parses JSON attributes, events
            // and links, so rows outside the window must never reach it.
            let ts_arr = record_batch
                .column(4)
                .as_any()
                .downcast_ref::<TimestampNanosecondArray>()
                .ok_or_else(|| Error::Arrow("Invalid start_time_ns column".into()))?;

            for row in 0..record_batch.num_rows() {
                let start_time_ns = ts_arr.value(row);
                if start_time_ns < start_ns || start_time_ns > end_ns {
                    continue;
                }
                match StorageModel::row_to_span(&record_batch, row) {
                    Ok(span) => {
                        spans.push(span);
                        if spans.len() >= limit {
                            return Ok(spans);
                        }
                    }
                    Err(e) => {
                        tracing::debug!("Skipping malformed span row in {:?}: {}", meta.path, e);
                    }
                }
            }
        }
        Ok(spans)
    }
}

/// Row-group indices whose `ts_column` statistics can overlap `[start_ns, end_ns]`.
///
/// Skipping a row group is always *sound*: a row group's min/max bound every
/// row it contains, so a group entirely outside the window cannot hold a
/// matching row — and that holds whether or not the rows are sorted. It is
/// only *useful* when rows are roughly time-ordered, which they are here:
/// both flush and compaction sort before writing, so a block's row groups
/// cover disjoint time slices.
///
/// Returns `None` when the timestamp column cannot be located, in which case
/// the caller must read every row group. Row groups with unusable statistics
/// are kept rather than skipped, so the worst case is reading more than
/// needed, never returning fewer rows.
/// Drops row groups that cannot contain `metric_name`, using the bloom filters
/// the writer emits on `metric_name` and `service_name`.
///
/// Row-group statistics only narrow on **time**. A metrics block interleaves
/// every metric it holds, so a query for one metric finds every row group
/// overlapping the window and decodes all of them. A bloom filter answers "could
/// this row group contain this value?" without decoding a page, which is the
/// difference between reading one row group per query and reading all of them.
///
/// Two mechanisms, cheapest first:
///
/// 1. **Row-group statistics.** Rows are written grouped by metric name, so a
///    row group's `metric_name` min and max are usually the same value. Testing
///    the needle against that range is exact and free — no filter read, no
///    probabilistic answer. This is what prunes in practice.
/// 2. **Bloom filters**, consulted only when the statistics cannot decide
///    (column missing, no min/max, or an unexpected physical type).
///
/// Either way this can only skip groups that genuinely lack the value: a
/// statistic range or a bloom filter may report a false *positive*, never a
/// false negative. Any condition that cannot be evaluated keeps the group, so
/// the worst case is the behaviour that existed before any of this.
///
/// An empty `metric_name` means "no metric filter" and keeps every candidate.
///
/// Only `metric_name` is filtered. A service-name conjunct would need the
/// service plumbed into the scan, and once `BL-03-07` orders rows by
/// `(metric_name, service_name, timestamp_ns)` the *row-group statistics*
/// prune on service exactly and for free — strictly better than a bloom filter,
/// which can only be probabilistic.
pub fn row_groups_matching_metric(
    reader: &ParquetRecordBatchReaderBuilder<File>,
    candidates: &[usize],
    metric_column: &str,
    metric_name: &str,
) -> Vec<usize> {
    row_groups_matching_value(reader, candidates, metric_column, metric_name)
}

/// Narrows `candidates` to the row groups that may contain `value` in
/// `column`. See [`row_groups_matching_metric`] for why statistics come first
/// and what the guarantees are.
pub fn row_groups_matching_value(
    reader: &ParquetRecordBatchReaderBuilder<File>,
    candidates: &[usize],
    column: &str,
    value: &str,
) -> Vec<usize> {
    let columns = reader.metadata().file_metadata().schema_descr().columns();
    let Some(col_idx) = columns.iter().position(|c| c.name() == column) else {
        // No such column: cannot prune, keep everything.
        return candidates.to_vec();
    };
    // An empty metric name means "no metric filter", not "the metric whose name
    // is the empty string". Callers use it to scan a block for every series -
    // `list_label_values` falls back to a full scan when a block predates the
    // flush-time label index. Treating it as a value to match would prune the
    // whole block, since the empty string sorts before every real name.
    if value.is_empty() {
        return candidates.to_vec();
    }
    let needle = value.as_bytes();
    let mut kept = Vec::with_capacity(candidates.len());
    for &rg in candidates {
        let col_md = reader.metadata().row_group(rg).column(col_idx);

        // Statistics first: they are free and, because rows are written grouped
        // by metric name, a row group's min and max for `metric_name` are
        // usually the same value — exact, not probabilistic. This is the check
        // that actually does the work on well-written blocks.
        //
        // This is a one-sided test, so unlike the time prune it does **not**
        // need a null check: min/max cover non-null values only, and a null row
        // cannot contain the value being searched for. If the value lies
        // outside [min, max] then no row in this group has it.
        //
        // Byte comparison is sound: UTF-8 byte order is code-point order, so
        // comparing raw bytes against the encoded needle matches string order.
        let decided_by_stats = match col_md.statistics() {
            Some(Statistics::ByteArray(s)) => match (s.min_opt(), s.max_opt()) {
                (Some(min), Some(max)) => {
                    if min.data() > needle || max.data() < needle {
                        continue;
                    }
                    true
                }
                // All-null column, or no min/max: says nothing either way.
                _ => false,
            },
            // A different physical type (INT32 indices for an older writer,
            // say): not comparable here.
            _ => false,
        };
        if decided_by_stats {
            kept.push(rg);
            continue;
        }

        // Statistics could not decide, so fall back to the bloom filter. It can
        // report a false positive but never a false negative, so this is also
        // safe to use for absence.
        match reader.get_row_group_column_bloom_filter(rg, col_idx) {
            Ok(Some(filter)) => {
                if !filter.check(needle) {
                    continue;
                }
            }
            // No filter on this column for this group: cannot rule it out.
            Ok(None) => {}
            Err(e) => {
                tracing::trace!(
                    row_group = rg,
                    error = %e,
                    "bloom filter unreadable; keeping row group"
                );
            }
        }
        kept.push(rg);
    }
    kept
}

pub fn row_groups_in_range(
    metadata: &ParquetMetaData,
    ts_column: &str,
    start_ns: i64,
    end_ns: i64,
) -> Option<Vec<usize>> {
    let col_idx = metadata
        .file_metadata()
        .schema_descr()
        .columns()
        .iter()
        .position(|c| c.name() == ts_column)?;

    let mut keep = Vec::with_capacity(metadata.num_row_groups());
    for rg_idx in 0..metadata.num_row_groups() {
        let statistics = metadata
            .row_group(rg_idx)
            .columns()
            .get(col_idx)
            .and_then(|c| c.statistics());
        match statistics {
            // Timestamps are stored as INT64 nanoseconds, so their statistics
            // arrive as `Int64` regardless of the logical timestamp type.
            Some(Statistics::Int64(s)) => {
                // min/max cover non-null values only: with nulls present we
                // cannot prove an out-of-range group holds no matching row.
                let nulls = s.null_count_opt().unwrap_or(0);
                match (s.min_opt(), s.max_opt()) {
                    (Some(min), Some(max)) if nulls == 0 => {
                        if *max >= start_ns && *min <= end_ns {
                            keep.push(rg_idx);
                        }
                    }
                    _ => keep.push(rg_idx),
                }
            }
            // Statistics disabled, or a non-INT64 column: keep the group.
            _ => keep.push(rg_idx),
        }
    }
    Some(keep)
}

/// File row ranges a log scan has to read for `[start_ns, end_ns]`.
///
/// With the block's skip index these are the granules that might hold a match
/// (see [`LogSkipIndex::candidate_ranges`]); without one, the row groups whose
/// timestamp statistics overlap the window.
fn log_row_ranges(
    opened: &OpenedBlock,
    start_ns: i64,
    end_ns: i64,
    pre: Option<&LogColumnPrefilter>,
) -> Vec<(u64, u64)> {
    if let Some(index) = &opened.log_index {
        return index.candidate_ranges(start_ns, end_ns, pre);
    }
    let md = opened.meta.metadata();
    let groups = row_groups_in_range(md, "timestamp_ns", start_ns, end_ns)
        .unwrap_or_else(|| (0..md.num_row_groups()).collect());
    let mut out: Vec<(u64, u64)> = Vec::new();
    let mut starts = Vec::with_capacity(md.num_row_groups());
    let mut at = 0u64;
    for rg in md.row_groups() {
        starts.push(at);
        at += rg.num_rows().max(0) as u64;
    }
    for g in groups {
        let (a, b) = (
            starts[g],
            starts[g] + md.row_group(g).num_rows().max(0) as u64,
        );
        match out.last_mut() {
            Some(last) if last.1 == a => last.1 = b,
            _ => out.push((a, b)),
        }
    }
    out
}

/// Restricts a reader to the given file row ranges: only the row groups that
/// hold them are visited, and a selection skips the rest inside those groups.
/// When the ranges are whole groups no selection is needed.
fn restrict_to_ranges<T>(
    builder: ParquetRecordBatchReaderBuilder<T>,
    ranges: &[(u64, u64)],
) -> ParquetRecordBatchReaderBuilder<T>
where
    T: parquet::file::reader::ChunkReader + 'static,
{
    let (groups, selection) = plan_row_selection(builder.metadata(), ranges);
    let whole: u64 = groups
        .iter()
        .map(|g| builder.metadata().row_group(*g).num_rows().max(0) as u64)
        .sum();
    let wanted: u64 = ranges.iter().map(|(a, b)| b - a).sum();
    let builder = builder.with_row_groups(groups);
    if whole == wanted {
        builder
    } else {
        builder.with_row_selection(selection)
    }
}

/// Columns an indexed scan decodes: labels come from the series table.
const INDEXED_SCAN_COLUMNS: [&str; 4] =
    ["timestamp_ns", "value_float", "value_int", "value_complex"];

/// Turns row ranges of the whole file into the row groups that hold them and a
/// selection over **those groups' rows**, which is what the reader expects once
/// the groups are restricted. Groups with no selected row are not read.
///
/// `ranges` are ascending and non-overlapping.
fn plan_row_selection(
    metadata: &ParquetMetaData,
    ranges: &[(u64, u64)],
) -> (Vec<usize>, parquet::arrow::arrow_reader::RowSelection) {
    let mut groups = Vec::new();
    let mut local: Vec<std::ops::Range<usize>> = Vec::new();
    let mut file_start = 0u64;
    let mut selected_rows = 0usize;
    let mut first = 0usize;
    for (g, rg) in metadata.row_groups().iter().enumerate() {
        let rows = rg.num_rows().max(0) as u64;
        let (lo, hi) = (file_start, file_start + rows);
        // Ranges ending at or before this group cannot touch a later one.
        while first < ranges.len() && ranges[first].1 <= lo {
            first += 1;
        }
        let mut any = false;
        for &(a, b) in &ranges[first..] {
            if a >= hi {
                break;
            }
            let (a, b) = (a.max(lo), b.min(hi));
            if a < b {
                local.push(selected_rows + (a - lo) as usize..selected_rows + (b - lo) as usize);
                any = true;
            }
        }
        if any {
            groups.push(g);
            selected_rows += rows as usize;
        }
        file_start = hi;
    }
    (
        groups,
        parquet::arrow::arrow_reader::RowSelection::from_consecutive_ranges(
            local.into_iter(),
            selected_rows,
        ),
    )
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

#[cfg(test)]
mod dictionary_tests {
    #![allow(clippy::unwrap_used, clippy::expect_used, clippy::panic)]
    use super::*;
    use crate::{DataPoint, LabelSet, Metric, MetricKind, MetricValue, StorageModel};
    use parquet::arrow::ArrowWriter;

    /// The `labels` column is requested as a dictionary; if the override were
    /// ever silently rejected the scan would still be correct but lose its fast
    /// path, so assert it is the type the reader hands back.
    #[test]
    fn labels_are_read_as_a_dictionary() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("b.parquet");
        let metric = Metric {
            name: "m".into(),
            kind: MetricKind::Gauge,
            data_points: (1..=50)
                .map(|i| {
                    DataPoint::new(
                        i,
                        MetricValue::Double(i as f64),
                        LabelSet::try_from_iter(vec![("host", format!("h{}", i % 3))]).unwrap(),
                    )
                    .unwrap()
                })
                .collect(),
            ..Default::default()
        };
        let chunk = StorageModel::metrics_to_chunk(&[metric]).unwrap();
        let mut w =
            ArrowWriter::try_new(File::create(&path).unwrap(), chunk.schema(), None).unwrap();
        w.write(&chunk).unwrap();
        w.close().unwrap();

        let opened = open_block(&path).unwrap().unwrap();
        let (file, meta) = (opened.file, opened.meta);
        let idx = meta.schema().index_of("labels").unwrap();
        assert!(
            matches!(
                meta.schema().field(idx).data_type(),
                arrow_schema::DataType::Dictionary(_, _)
            ),
            "labels should be requested as a dictionary, got {:?}",
            meta.schema().field(idx).data_type()
        );
        let mut reader = ParquetRecordBatchReaderBuilder::new_with_metadata(file, meta)
            .build()
            .unwrap();
        let batch = reader.next().unwrap().unwrap();
        assert!(matches!(
            batch
                .column(batch.schema().index_of("labels").unwrap())
                .data_type(),
            arrow_schema::DataType::Dictionary(_, _)
        ));

        // A second open is served from the footer cache and behaves the same.
        let again = open_block(&path).unwrap().unwrap().meta;
        assert_eq!(again.schema().as_ref(), batch.schema_ref().as_ref());
    }
}

#[cfg(test)]
mod indexed_scan_tests {
    #![allow(clippy::unwrap_used, clippy::expect_used, clippy::panic)]
    use super::*;
    use crate::models::storage::{MetricRun, SignalType};
    use crate::storage::block_file::write_block_file;
    use crate::{LabelSet, Metric, MetricKind, MetricValue};
    use parquet::arrow::ArrowWriter;
    use parquet::file::properties::WriterProperties;
    use std::collections::HashSet;

    const ROWS: usize = 12_000;
    const RG: usize = 1_000;

    fn metrics() -> Vec<Metric> {
        let svc = |name: &str| LabelSet::try_from_iter(vec![("service.name", name)]).unwrap();
        let mk = |service: &str, parity: usize| Metric {
            name: "scan.cpu".into(),
            kind: MetricKind::Gauge,
            resource_attributes: svc(service),
            // Deliberately not in time order: the writer has to sort.
            data_points: (0..ROWS)
                .rev()
                .filter(|i| i % 2 == parity)
                .map(|i| {
                    DataPoint::new(
                        i as i64 + 1,
                        if i % 5 == 0 {
                            MetricValue::Int(i as i64)
                        } else {
                            MetricValue::Double(i as f64)
                        },
                        LabelSet::try_from_iter(vec![
                            ("host", format!("h{}", i % 37)),
                            ("env", if i % 3 == 0 { "prod" } else { "stage" }.to_string()),
                        ])
                        .unwrap(),
                    )
                    .unwrap()
                })
                .collect(),
            ..Default::default()
        };
        let other = Metric {
            name: "scan.other".into(),
            kind: MetricKind::Gauge,
            data_points: vec![
                DataPoint::new(5, MetricValue::Double(1.0), LabelSet::default()).unwrap(),
            ],
            ..Default::default()
        };
        vec![mk("a", 0), mk("b", 1), other]
    }

    fn block_meta(path: std::path::PathBuf) -> BlockMetadata {
        BlockMetadata {
            path,
            start_timestamp_ns: 1,
            end_timestamp_ns: ROWS as i64,
            row_count: ROWS + 1,
            size_bytes: 0,
            metric_names: HashSet::from(["scan.cpu".into(), "scan.other".into()]),
            label_names: HashSet::new(),
            label_values: Default::default(),
            signal_type: SignalType::Metrics,
            wal_range: None,
        }
    }

    /// The same data twice: once with no series table (rows in arrival order),
    /// once laid out series by series with a table, over many row groups.
    fn pair(dir: &std::path::Path) -> (BlockMetadata, BlockMetadata) {
        let ms = metrics();

        let plain = dir.join("plain.parquet");
        let chunk = StorageModel::metrics_to_chunk(&ms).unwrap();
        let props = WriterProperties::builder()
            .set_max_row_group_row_count(Some(RG))
            .build();
        let mut w =
            ArrowWriter::try_new(File::create(&plain).unwrap(), chunk.schema(), Some(props))
                .unwrap();
        w.write(&chunk).unwrap();
        w.close().unwrap();

        let indexed = dir.join("indexed.parquet");
        let runs: Vec<MetricRun<'_>> = ms
            .iter()
            .map(|m| {
                let mut points: Vec<&DataPoint> = m.data_points.iter().collect();
                points.sort_by(|a, b| {
                    a.labels
                        .fingerprint()
                        .cmp(&b.labels.fingerprint())
                        .then(a.timestamp_ns.cmp(&b.timestamp_ns))
                });
                MetricRun {
                    name: &m.name,
                    kind: m.kind,
                    resource: &m.resource_attributes,
                    points,
                }
            })
            .collect();
        let (batch, table) = StorageModel::metric_runs_to_block(&runs).unwrap();
        write_block_file(
            &indexed,
            batch,
            "zstd",
            None,
            RG,
            None,
            Some(crate::storage::FooterIndex::Series(&table)),
        )
        .unwrap();
        (block_meta(plain), block_meta(indexed))
    }

    fn canonical(mut points: Vec<DataPoint>) -> Vec<(i64, String, String)> {
        points.sort_by_key(|p| (p.timestamp_ns, p.labels.json_str().to_string()));
        points
            .iter()
            .map(|p| {
                (
                    p.timestamp_ns,
                    p.labels.json_str().to_string(),
                    format!("{:?}", p.value),
                )
            })
            .collect()
    }

    fn selectors() -> Vec<(&'static str, SeriesSelector)> {
        let host5: SeriesFilter = Arc::new(|l: &LabelSet| l.get("host") == Some("h5"));
        let host5_prod_b: SeriesFilter = Arc::new(|l: &LabelSet| {
            l.get("host") == Some("h5")
                && l.get("env") == Some("prod")
                && l.get("service.name") == Some("b")
        });
        let h1x: SeriesFilter =
            Arc::new(|l: &LabelSet| l.get("host").is_some_and(|h| h.starts_with("h1")));
        let not_prod: SeriesFilter = Arc::new(|l: &LabelSet| l.get("env") != Some("prod"));
        let mut equals_two = SeriesSelector::all().with_filter(Some(host5_prod_b));
        equals_two.equals = vec![
            ("host".into(), "h5".into()),
            ("env".into(), "prod".into()),
            ("service.name".into(), "b".into()),
        ];
        let mut host_eq = SeriesSelector::all().with_filter(Some(host5.clone()));
        host_eq.equals = vec![("host".into(), "h5".into())];
        let mut regex = SeriesSelector::all().with_filter(Some(h1x));
        regex.value_tests = vec![(
            "host".into(),
            Arc::new(|v: &str| v.starts_with("h1")) as ValueTest,
        )];
        let mut missing = SeriesSelector::all()
            .with_filter(Some(Arc::new(|l: &LabelSet| l.get("host") == Some("nope"))));
        missing.equals = vec![("host".into(), "nope".into())];
        vec![
            ("everything", SeriesSelector::all()),
            (
                "service",
                // As the query layer builds it: the matcher has the last word.
                SeriesSelector::all()
                    .with_service(Some("a".into()))
                    .with_filter(Some(Arc::new(|l: &LabelSet| {
                        l.get("service.name") == Some("a")
                    }))),
            ),
            ("host equality", host_eq),
            ("three equalities", equals_two),
            ("regex narrowing", regex),
            (
                "filter only (not-equal)",
                SeriesSelector::all().with_filter(Some(not_prod)),
            ),
            ("absent value", missing),
        ]
    }

    use super::super::series_index::ValueTest;

    /// Whatever the selector and window, a block read through its series table
    /// returns exactly the points the same data returns without one.
    #[test]
    fn indexed_scan_equals_unindexed_scan() {
        let dir = tempfile::tempdir().unwrap();
        let (plain, indexed) = pair(dir.path());
        assert!(open_block(&indexed.path).unwrap().unwrap().series.is_some());
        assert!(open_block(&plain.path).unwrap().unwrap().series.is_none());

        for (label, selector) in selectors() {
            for (start, end) in [
                (0, i64::MAX),
                (3_001, 4_000),
                (11_990, 20_000),
                (50_000, 60_000),
            ] {
                for metric in ["scan.cpu", "scan.other", "scan.absent", ""] {
                    let want =
                        Scanner::scan_block_filtered(plain.clone(), metric, start, end, &selector)
                            .unwrap();
                    let got = Scanner::scan_block_filtered(
                        indexed.clone(),
                        metric,
                        start,
                        end,
                        &selector,
                    )
                    .unwrap();
                    assert_eq!(
                        canonical(got.points),
                        canonical(want.points),
                        "{label}: metric {metric:?}, window {start}..{end}"
                    );
                    // Reading less never means counting more.
                    assert!(got.rows_considered <= want.rows_considered, "{label}");
                }
            }
        }
    }

    /// The point of the table: a selective query reads only the rows of the
    /// series it selects.
    #[test]
    fn selective_scan_reads_only_the_selected_series() {
        let dir = tempfile::tempdir().unwrap();
        let (plain, indexed) = pair(dir.path());
        let (_, host_eq) = selectors().into_iter().nth(2).unwrap();
        let unindexed =
            Scanner::scan_block_filtered(plain, "scan.cpu", 0, i64::MAX, &host_eq).unwrap();
        let got = Scanner::scan_block_filtered(indexed, "scan.cpu", 0, i64::MAX, &host_eq).unwrap();
        assert_eq!(unindexed.rows_considered, ROWS as u64);
        assert_eq!(got.rows_considered, got.points.len() as u64);
        assert!(!got.points.is_empty() && got.points.len() < ROWS / 20);
    }

    /// A table the file does not agree with is ignored, not trusted.
    #[test]
    fn a_damaged_series_table_falls_back_to_a_full_scan() {
        let dir = tempfile::tempdir().unwrap();
        let (plain, _) = pair(dir.path());
        let ms = metrics();

        let write_with = |name: &str, value: String| {
            let path = dir.path().join(name);
            let chunk = StorageModel::metrics_to_chunk(&ms).unwrap();
            let props = WriterProperties::builder()
                .set_max_row_group_row_count(Some(RG))
                .set_key_value_metadata(Some(vec![parquet::file::metadata::KeyValue::new(
                    SERIES_INDEX_FOOTER_KEY.to_string(),
                    value,
                )]))
                .build();
            let mut w =
                ArrowWriter::try_new(File::create(&path).unwrap(), chunk.schema(), Some(props))
                    .unwrap();
            w.write(&chunk).unwrap();
            w.close().unwrap();
            block_meta(path)
        };

        // Not JSON at all.
        let garbage = write_with("garbage.parquet", "{not json".to_string());
        // A well-formed table that describes a different, smaller file.
        let mut b = crate::storage::SeriesTableBuilder::new();
        b.begin_metric("scan.cpu");
        b.push_series(&LabelSet::default(), 0, 3, 1, 3);
        let wrong = write_with("wrong.parquet", b.finish().to_footer_value().unwrap());

        let selector = SeriesSelector::all();
        let want = canonical(
            Scanner::scan_block_filtered(plain, "scan.cpu", 0, i64::MAX, &selector)
                .unwrap()
                .points,
        );
        for bad in [garbage, wrong] {
            assert!(open_block(&bad.path).unwrap().unwrap().series.is_none());
            let got =
                Scanner::scan_block_filtered(bad, "scan.cpu", 0, i64::MAX, &selector).unwrap();
            assert_eq!(canonical(got.points), want);
        }
    }

    /// A log index that is not JSON, or that counts the wrong number of rows, is
    /// ignored and the block still scans.
    #[test]
    fn a_damaged_log_index_is_ignored() {
        use crate::storage::log_index::{LogIndexBuilder, LOG_INDEX_FOOTER_KEY};
        let dir = tempfile::tempdir().unwrap();
        let logs: Vec<crate::LogRecord> = (0..10i64)
            .map(|i| {
                crate::LogRecord::new(
                    i + 1,
                    i + 1,
                    9,
                    "INFO".into(),
                    format!("hello {i}"),
                    LabelSet::default(),
                    LabelSet::default(),
                    [0u8; 16],
                    [0u8; 8],
                    0,
                    "".into(),
                    "".into(),
                )
            })
            .collect();
        let mut short = LogIndexBuilder::new();
        for l in &logs[..4] {
            short.push_row(
                l.timestamp_ns,
                l.severity_number,
                &l.body,
                &l.attributes,
                &l.resource_attributes,
            );
        }
        let wrong_rows = short.finish().to_footer_value().unwrap();

        for (name, value) in [("garbage", "{nope".to_string()), ("wrong", wrong_rows)] {
            let path = dir.path().join(format!("{name}.parquet"));
            let chunk = StorageModel::logs_to_chunk(&logs).unwrap();
            let props = WriterProperties::builder()
                .set_key_value_metadata(Some(vec![parquet::file::metadata::KeyValue::new(
                    LOG_INDEX_FOOTER_KEY.to_string(),
                    value,
                )]))
                .build();
            let mut w =
                ArrowWriter::try_new(File::create(&path).unwrap(), chunk.schema(), Some(props))
                    .unwrap();
            w.write(&chunk).unwrap();
            w.close().unwrap();
            let opened = open_block(&path).unwrap().unwrap();
            assert!(opened.log_index.is_none(), "{name}");
            let meta = BlockMetadata {
                path,
                start_timestamp_ns: 1,
                end_timestamp_ns: 10,
                row_count: 10,
                size_bytes: 0,
                metric_names: HashSet::new(),
                label_names: HashSet::new(),
                label_values: Default::default(),
                signal_type: SignalType::Logs,
                wal_range: None,
            };
            let (got, stats, _) = Scanner::scan_log_block_filtered(
                meta,
                0,
                i64::MAX,
                usize::MAX,
                false,
                None,
                None,
                0,
            )
            .unwrap();
            assert_eq!(got.len(), 10, "{name}");
            assert_eq!(stats, 10, "{name}");
        }
    }

    #[test]
    fn row_selection_maps_file_rows_onto_the_groups_it_touches() {
        let dir = tempfile::tempdir().unwrap();
        let (_, indexed) = pair(dir.path());
        let opened = open_block(&indexed.path).unwrap().unwrap();
        let md = opened.meta.metadata();
        let groups = md.num_row_groups();
        assert!(groups > 5);
        // A range inside group 2 and one spanning groups 4 and 5.
        let ranges = [(2_100u64, 2_200u64), (4_900, 5_100)];
        let (picked, selection) = plan_row_selection(md, &ranges);
        assert_eq!(picked, vec![2, 4, 5]);
        let selected: usize = selection
            .iter()
            .filter(|s| !s.skip)
            .map(|s| s.row_count)
            .sum();
        assert_eq!(selected, 100 + 200);
        // 3 groups x 1000 rows, addressed relative to those groups.
        let total: usize = selection.iter().map(|s| s.row_count).sum();
        assert_eq!(total, 3 * RG);
    }
}
