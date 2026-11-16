//! Trace scans.

use super::pruning::row_groups_in_range;
use super::Scanner;
use super::{acquire_scan_slot, record_scan_block, record_scan_output, MAX_CONCURRENT_BLOCK_SCANS};
use crate::error::{Error, Result};
use crate::models::storage::{BlockMetadata, StorageModel};
use crate::models::traces::Span;
use arrow_array::{Array, BinaryArray, FixedSizeBinaryArray, TimestampNanosecondArray};
use parquet::arrow::arrow_reader::ParquetRecordBatchReaderBuilder;
use std::collections::BTreeMap;
use std::fs::File;

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
        let mut newest_matches = BTreeMap::new();
        let mut sequence = 0u64;

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
                match StorageModel::row_to_span(&record_batch, row) {
                    Ok(span) => {
                        if trace_id_filter.is_some() {
                            sequence += 1;
                            newest_matches.insert((span.start_time_ns, sequence), span);
                            if newest_matches.len() > limit {
                                let _ = newest_matches.pop_first();
                            }
                        } else {
                            spans.push(span);
                            if spans.len() >= limit {
                                return Ok(spans);
                            }
                        }
                    }
                    Err(e) => {
                        tracing::debug!("Skipping malformed span row in {:?}: {}", meta.path, e);
                    }
                }
            }
        }
        if trace_id_filter.is_some() {
            Ok(newest_matches.into_values().collect())
        } else {
            Ok(spans)
        }
    }
}
