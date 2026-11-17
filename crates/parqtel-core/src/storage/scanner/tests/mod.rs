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
