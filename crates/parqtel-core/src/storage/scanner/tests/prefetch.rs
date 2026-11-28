//! Footer prefetch: cold block opens happen before a scan slot is acquired.

#![allow(clippy::unwrap_used, clippy::expect_used, clippy::panic)]
use super::super::block::footer_cached;
use super::*;
use crate::models::storage::{SignalType, StorageModel};
use crate::{LabelSet, LogRecord};
use parquet::arrow::ArrowWriter;
use std::collections::HashSet;
use std::fs::File;

fn write_logs(path: &std::path::Path, first_ts: i64, rows: i64) {
    let logs: Vec<LogRecord> = (0..rows)
        .map(|i| {
            let ts = first_ts + i;
            LogRecord::new(
                ts,
                ts,
                9,
                "INFO".into(),
                format!("hello {ts}"),
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
    let chunk = StorageModel::logs_to_chunk(&logs).unwrap();
    let mut writer =
        ArrowWriter::try_new(File::create(path).unwrap(), chunk.schema(), None).unwrap();
    writer.write(&chunk).unwrap();
    writer.close().unwrap();
}

fn log_block(path: std::path::PathBuf, start_ns: i64, end_ns: i64, rows: i64) -> BlockMetadata {
    BlockMetadata {
        path,
        start_timestamp_ns: start_ns,
        end_timestamp_ns: end_ns,
        row_count: rows as usize,
        size_bytes: 0,
        metric_names: HashSet::new(),
        label_names: HashSet::new(),
        label_values: Default::default(),
        signal_type: SignalType::Logs,
        wal_range: None,
    }
}

/// A cold log scan warms the footer cache outside the scan slots, and the warm
/// scan returns the same records.
#[tokio::test]
async fn log_scan_prefetches_cold_footers_without_changing_results() {
    let dir = tempfile::tempdir().unwrap();
    let a = dir.path().join("logs_a.parquet");
    let b = dir.path().join("logs_b.parquet");
    write_logs(&a, 1, 20);
    write_logs(&b, 1_000, 20);
    let blocks = vec![
        log_block(a.clone(), 1, 20, 20),
        log_block(b.clone(), 1_000, 1_019, 20),
    ];
    assert!(prefetch_needed(&blocks), "fresh temp files start cold");

    let before = Scanner::scan_metrics_snapshot();
    let cold = Scanner::scan_logs(blocks.clone(), 0, i64::MAX)
        .await
        .unwrap();
    let after_cold = Scanner::scan_metrics_snapshot();

    // The prefetch phase paid the misses, then each decode found its footer.
    assert!(after_cold.footer_cache_misses >= before.footer_cache_misses + 2);
    assert!(after_cold.footer_cache_hits >= before.footer_cache_hits + 2);
    assert!(footer_cached(&a) && footer_cached(&b));
    assert!(
        !prefetch_needed(&blocks),
        "a fully warm cache must skip the prefetch phase"
    );

    let warm = Scanner::scan_logs(blocks, 0, i64::MAX).await.unwrap();
    assert_eq!(cold.len(), 40);
    assert_eq!(cold, warm, "prefetching must not change the records");
}

/// One block has no parallelism to win, so it skips the prefetch phase.
#[test]
fn single_block_scan_skips_prefetch() {
    let block = log_block(
        std::path::PathBuf::from("/nonexistent/one.parquet"),
        0,
        0,
        0,
    );
    assert!(!prefetch_needed(std::slice::from_ref(&block)));
}
