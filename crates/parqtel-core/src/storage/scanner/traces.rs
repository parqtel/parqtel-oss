//! Trace scans.

use super::pruning::{
    page_ranges_in_time, project_present, restrict_to_ranges, row_groups_in_range,
};
use super::Scanner;
use super::{acquire_scan_slot, record_scan_block, record_scan_output, MAX_CONCURRENT_BLOCK_SCANS};
use crate::error::{Error, Result};
use crate::models::storage::{BlockMetadata, StorageModel};
use crate::models::traces::Span;
use arrow_array::{Array, BinaryArray, FixedSizeBinaryArray, TimestampNanosecondArray};
use parquet::arrow::arrow_reader::{
    ArrowReaderMetadata, ArrowReaderOptions, ParquetRecordBatchReaderBuilder,
};
use parquet::file::metadata::PageIndexPolicy;

/// Coalesces sorted file-row indices into ascending, non-overlapping ranges.
fn row_ranges(rows: &[(i64, u64)]) -> Vec<(u64, u64)> {
    let mut out: Vec<(u64, u64)> = Vec::new();
    for (_, row) in rows {
        match out.last_mut() {
            Some(last) if last.1 == *row => last.1 = *row + 1,
            _ => out.push((*row, *row + 1)),
        }
    }
    out
}

impl Scanner {
    /// Scans a set of blocks for spans matching a time range.
    /// Uses bounded concurrency and graceful error handling — a single corrupted
    /// block will not crash the process or abort the entire query.
    pub async fn scan_traces(
        blocks: Vec<BlockMetadata>,
        start_ns: i64,
        end_ns: i64,
        limit: usize,
    ) -> Result<Vec<Span>> {
        Self::scan_traces_filtered(blocks, start_ns, end_ns, limit, None).await
    }

    /// Scans spans, applying an optional trace-ID predicate before the result cap.
    pub async fn scan_traces_filtered(
        blocks: Vec<BlockMetadata>,
        start_ns: i64,
        end_ns: i64,
        limit: usize,
        trace_id_filter: Option<[u8; 16]>,
    ) -> Result<Vec<Span>> {
        if limit == 0 {
            return Ok(Vec::new());
        }
        let block_count = blocks.len();
        let started = std::time::Instant::now();
        let mut admission_wait = std::time::Duration::ZERO;
        let mut tasks = Vec::with_capacity(block_count.min(MAX_CONCURRENT_BLOCK_SCANS));
        for block in blocks {
            let waiting = std::time::Instant::now();
            let permit = acquire_scan_slot().await?;
            admission_wait += waiting.elapsed();
            tasks.push(tokio::task::spawn_blocking(move || {
                let _permit = permit;
                record_scan_block();
                // catch_unwind guards against panics in parquet deserialization
                let result = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
                    Self::scan_trace_block(block.clone(), start_ns, end_ns, limit, trace_id_filter)
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
        record_scan_output(0, all_spans.len());
        tracing::debug!(
            blocks = block_count,
            spans = all_spans.len(),
            trace_id_filtered = trace_id_filter.is_some(),
            admission_wait_ms = admission_wait.as_millis(),
            duration_ms = started.elapsed().as_millis(),
            "trace scan complete"
        );
        Ok(all_spans)
    }

    fn scan_trace_block(
        meta: BlockMetadata,
        start_ns: i64,
        end_ns: i64,
        limit: usize,
        trace_id_filter: Option<[u8; 16]>,
    ) -> Result<Vec<Span>> {
        let file = match crate::storage::block_store::open(&meta.path) {
            Ok(f) => f,
            Err(Error::Io(e)) if e.kind() == std::io::ErrorKind::NotFound => {
                tracing::warn!("Trace block file not found, skipping: {:?}", meta.path);
                return Ok(Vec::new());
            }
            Err(e) => return Err(e),
        };

        let options = ArrowReaderOptions::new().with_page_index_policy(PageIndexPolicy::Optional);
        let base = ParquetRecordBatchReaderBuilder::try_new_with_options(
            file.try_clone().map_err(Error::Io)?,
            options.clone(),
        )
        .map_err(|e| Error::Parquet(e.to_string()))?;
        let metadata = base.metadata().clone();
        let parquet_schema = base.parquet_schema().clone();

        // Row groups and pages that can hold a row in the window.
        let groups = match row_groups_in_range(&metadata, "start_time_ns", start_ns, end_ns) {
            Some(groups) if groups.is_empty() => return Ok(Vec::new()),
            Some(groups) => Some(groups),
            None => None,
        };
        let page_ranges = groups.as_ref().and_then(|groups| {
            page_ranges_in_time(&metadata, "start_time_ns", start_ns, end_ns, groups)
        });
        if matches!(&page_ranges, Some(ranges) if ranges.is_empty()) {
            return Ok(Vec::new());
        }

        // File-row ranges the reader will yield, used to turn a batch row back
        // into a file row for the second pass.
        let source_ranges: Vec<(u64, u64)> = if let Some(ranges) = &page_ranges {
            ranges.clone()
        } else if let Some(groups) = &groups {
            let mut starts = Vec::with_capacity(metadata.num_row_groups());
            let mut at = 0u64;
            for rg in metadata.row_groups() {
                starts.push(at);
                at += rg.num_rows().max(0) as u64;
            }
            groups
                .iter()
                .map(|g| {
                    let start = starts[*g];
                    (
                        start,
                        start + metadata.row_group(*g).num_rows().max(0) as u64,
                    )
                })
                .collect()
        } else {
            let rows = metadata.file_metadata().num_rows().max(0) as u64;
            vec![(0, rows)]
        };

        // Phase one: timestamp and trace id only. Attributes, events and links
        // stay encoded for rows that cannot match.
        let columns = project_present(&parquet_schema, &["start_time_ns", "trace_id"]);
        if columns.len() != 2 {
            return Err(Error::Arrow(
                "trace block is missing start_time_ns or trace_id".into(),
            ));
        }
        let mask = parquet::arrow::ProjectionMask::leaves(&parquet_schema, columns);
        let phase_one = if let Some(ranges) = &page_ranges {
            restrict_to_ranges(base, ranges)
        } else if let Some(groups) = &groups {
            base.with_row_groups(groups.clone())
        } else {
            base
        };
        let reader = phase_one
            .with_projection(mask)
            .build()
            .map_err(|e| Error::Parquet(e.to_string()))?;

        let mut selected: Vec<(i64, u64)> = Vec::new();
        let mut source_idx = 0usize;
        let mut next_row = source_ranges.first().map_or(0, |r| r.0);
        for record_batch in reader {
            let record_batch = match record_batch {
                Ok(c) => c,
                Err(e) => {
                    tracing::warn!("Error reading chunk from {:?}: {} — skipping", meta.path, e);
                    continue;
                }
            };
            let ts_idx = record_batch
                .schema()
                .index_of("start_time_ns")
                .map_err(|_| Error::Arrow("Missing start_time_ns column".into()))?;
            let ts_arr = record_batch
                .column(ts_idx)
                .as_any()
                .downcast_ref::<TimestampNanosecondArray>()
                .ok_or_else(|| Error::Arrow("Invalid start_time_ns column".into()))?;
            let trace_ids = match trace_id_filter {
                Some(_) => {
                    let index = record_batch
                        .schema()
                        .index_of("trace_id")
                        .map_err(|_| Error::Arrow("Missing trace_id column".into()))?;
                    Some(record_batch.column(index))
                }
                None => None,
            };
            for row in 0..record_batch.num_rows() {
                while source_idx + 1 < source_ranges.len()
                    && next_row >= source_ranges[source_idx].1
                {
                    source_idx += 1;
                    next_row = source_ranges[source_idx].0;
                }
                let file_row = next_row;
                next_row += 1;
                let start_time_ns = ts_arr.value(row);
                if start_time_ns < start_ns || start_time_ns > end_ns {
                    continue;
                }
                if let (Some(target), Some(column)) = (trace_id_filter, trace_ids) {
                    let matches =
                        if let Some(ids) = column.as_any().downcast_ref::<FixedSizeBinaryArray>() {
                            !ids.is_null(row) && ids.value(row) == target.as_slice()
                        } else if let Some(ids) = column.as_any().downcast_ref::<BinaryArray>() {
                            !ids.is_null(row) && ids.value(row) == target.as_slice()
                        } else {
                            return Err(Error::Arrow("Invalid trace_id column".into()));
                        };
                    if !matches {
                        continue;
                    }
                }
                selected.push((start_time_ns, file_row));
                if trace_id_filter.is_none() && selected.len() >= limit {
                    break;
                }
            }
            if trace_id_filter.is_none() && selected.len() >= limit {
                break;
            }
        }
        if selected.is_empty() {
            return Ok(Vec::new());
        }
        if trace_id_filter.is_some() && selected.len() > limit {
            selected.sort_by_key(|(ts, row)| (std::cmp::Reverse(*ts), *row));
            selected.truncate(limit);
        }
        selected.sort_by_key(|(_, row)| *row);
        let selected_ranges = row_ranges(&selected);

        // Phase two: all columns, but only the rows phase one selected.
        let arrow_meta = ArrowReaderMetadata::try_new(metadata, options)
            .map_err(|e| Error::Parquet(e.to_string()))?;
        let builder = ParquetRecordBatchReaderBuilder::new_with_metadata(file, arrow_meta);
        let builder = if !selected_ranges.is_empty() {
            restrict_to_ranges(builder, &selected_ranges)
        } else {
            builder
        };
        let reader = builder.build().map_err(|e| Error::Parquet(e.to_string()))?;

        let mut spans = Vec::new();
        for record_batch in reader {
            let record_batch = match record_batch {
                Ok(c) => c,
                Err(e) => {
                    tracing::warn!("Error reading chunk from {:?}: {} — skipping", meta.path, e);
                    continue;
                }
            };
            for row in 0..record_batch.num_rows() {
                match StorageModel::row_to_span(&record_batch, row) {
                    Ok(span) => spans.push(span),
                    Err(e) => {
                        tracing::debug!("Skipping malformed span row in {:?}: {}", meta.path, e);
                    }
                }
            }
        }
        spans.sort_by_key(|s| s.start_time_ns);
        if trace_id_filter.is_some() && spans.len() > limit {
            spans.truncate(limit);
        }
        Ok(spans)
    }
}
