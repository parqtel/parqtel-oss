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
