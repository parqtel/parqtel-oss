//! Scanner tests.

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
