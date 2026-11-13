//! Log scans: column prefilter, skip-index ranges and the two-phase scan.

use super::block::{open_block, OpenedBlock};
use super::pruning::{
    plan_row_selection, project_present, restrict_to_ranges, row_groups_in_range,
};
use super::Scanner;
use super::{MAX_BLOCKS, MAX_CONCURRENT, SCAN_BATCH_ROWS};
use crate::error::{Error, Result};
use crate::models::logs::LogRecord;
use crate::models::storage::{BlockMetadata, StorageModel};
use arrow_array::{Array, TimestampNanosecondArray};
use parquet::arrow::arrow_reader::{ArrowPredicateFn, ParquetRecordBatchReaderBuilder, RowFilter};
use parquet::arrow::ProjectionMask;
use std::collections::HashMap;
use std::sync::Arc;

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

impl Scanner {
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
    pub(super) fn scan_log_block_filtered(
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
}
