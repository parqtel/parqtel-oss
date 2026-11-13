//! Trace scans.

use super::pruning::row_groups_in_range;
use super::Scanner;
use super::MAX_CONCURRENT;
use crate::error::{Error, Result};
use crate::models::storage::{BlockMetadata, StorageModel};
use crate::models::traces::Span;
use arrow_array::{Array, TimestampNanosecondArray};
use parquet::arrow::arrow_reader::ParquetRecordBatchReaderBuilder;
use std::fs::File;
use std::sync::Arc;

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
