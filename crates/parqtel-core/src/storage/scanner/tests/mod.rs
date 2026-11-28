//! Scanner tests.

#![allow(clippy::unwrap_used, clippy::expect_used, clippy::panic)]

use super::block::open_block;
use super::pruning::plan_row_selection;
use super::*;
use crate::models::metrics::DataPoint;
use crate::models::storage::{BlockMetadata, StorageModel};
use crate::storage::series_index::{SeriesSelector, ValueTest, SERIES_INDEX_FOOTER_KEY};
use parquet::arrow::arrow_reader::ParquetRecordBatchReaderBuilder;
use std::fs::File;
use std::sync::Arc;

mod dictionary;
mod indexed;
mod prefetch;

#[test]
fn scan_limits_lazily_default_to_query_config() {
    let limits = ScanLimits::defaults();
    assert_eq!(limits.max_scan_blocks, 1024);
    assert_eq!(limits.compaction_scan_slots, 2);
    assert_eq!(
        limits.max_concurrent_scans,
        crate::config::QueryConfig::default_max_concurrent_scans()
    );
    let snapshot = Scanner::scan_metrics_snapshot();
    assert_eq!(snapshot.slot_limit as usize, limits.max_concurrent_scans);
    assert_eq!(
        snapshot.compaction_slot_limit as usize,
        limits.compaction_scan_slots
    );
}

#[test]
fn configure_limits_rejects_zero_for_every_knob() {
    for (blocks, scans, compaction) in [(0, 1, 1), (1, 0, 1), (1, 1, 0)] {
        assert!(
            Scanner::configure_limits(blocks, scans, compaction).is_err(),
            "zero is never a valid scan limit"
        );
    }
}

#[test]
fn runtime_is_installed_once_and_lazily_defaulted() {
    static CELL: std::sync::OnceLock<ScanRuntime> = std::sync::OnceLock::new();
    assert!(CELL.get().is_none(), "nothing installs the runtime eagerly");
    let limits = ScanLimits {
        max_scan_blocks: 7,
        max_concurrent_scans: 3,
        compaction_scan_slots: 1,
    };
    assert!(install_runtime(&CELL, limits).is_ok());
    let installed = CELL.get().expect("installed by the call above");
    assert_eq!(installed.limits.max_scan_blocks, 7);
    assert_eq!(installed.limits.max_concurrent_scans, 3);
    assert_eq!(installed.limits.compaction_scan_slots, 1);
    assert_eq!(installed.query.available_permits(), 3);
    assert_eq!(installed.compaction.available_permits(), 1);
    assert!(
        install_runtime(&CELL, ScanLimits::defaults()).is_err(),
        "a second install must not resize live admission pools"
    );
}

#[tokio::test]
async fn over_limit_scan_reports_truncated_blocks() {
    let dir = tempfile::tempdir().expect("temp dir");
    let meta = |name: &str| BlockMetadata {
        path: dir.path().join(name),
        start_timestamp_ns: 0,
        end_timestamp_ns: 10,
        row_count: 0,
        size_bytes: 0,
        metric_names: std::collections::HashSet::from(["scan.cpu".to_string()]),
        label_names: Default::default(),
        label_values: Default::default(),
        signal_type: crate::models::storage::SignalType::Metrics,
        wal_range: None,
    };
    let blocks = vec![meta("a.parquet"), meta("b.parquet"), meta("c.parquet")];
    let scan = Scanner::scan_filtered_with_limit(
        blocks,
        "scan.cpu".into(),
        0,
        10,
        SeriesSelector::all(),
        2,
    )
    .await
    .expect("missing block files are skipped, not fatal");
    assert_eq!(scan.scanned_blocks, 2);
    assert_eq!(scan.truncated_blocks, 1);
    assert!(scan.points.is_empty());
}

#[test]
fn block_paths_are_classified_by_level_and_signal() {
    let cases = [
        ("/data/m_l0_1.parquet", "l0"),
        ("/data/m_l1_1.parquet", "l1"),
        ("/data/m_l2_1.parquet", "l2"),
        ("/data/m_n_1.parquet", "normalized"),
        ("/data/m_old_1.parquet", "other"),
        ("/data/logs_1.parquet", "logs"),
        ("/data/traces_1.parquet", "traces"),
        ("/data/logs/block.parquet", "logs"),
        ("/data/traces/block.parquet", "traces"),
    ];
    for (path, expected) in cases {
        assert_eq!(block_level(std::path::Path::new(path)), expected);
    }
}

#[test]
fn background_scan_slot_is_dedicated_and_reports_skips() {
    let permit = Scanner::try_reserve_background_scan_slot()
        .expect("the dedicated background slot starts free");
    let before = Scanner::scan_metrics_snapshot().background_skips;
    assert!(
        Scanner::try_reserve_background_scan_slot().is_none(),
        "only one alert-evaluation scan may run at a time"
    );
    assert_eq!(
        Scanner::scan_metrics_snapshot().background_skips,
        before + 1
    );
    drop(permit);
    assert!(
        Scanner::try_reserve_background_scan_slot().is_some(),
        "releasing the slot makes it available again"
    );
}
