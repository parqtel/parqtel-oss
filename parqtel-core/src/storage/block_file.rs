//! Writing a block file: one Parquet file with the layout the scanner prunes on.

use crate::config::compression_from_name;
use crate::error::{Error, Result};
use crate::storage::series_index::{footer_metadata, SeriesTable};
use parquet::arrow::ArrowWriter;
use parquet::file::properties::{EnabledStatistics, WriterProperties, WriterVersion};
use parquet::schema::types::ColumnPath;
use std::fs::File;
use std::path::Path;

/// Columns that carry a bloom filter.
///
/// Chosen because they are the columns a query filters on *before* it can skip
/// anything: the metric name selects which blocks are relevant at all, and the
/// service name is the second-level discriminator. `timestamp_ns` is already
/// covered exactly by row-group statistics, which are cheaper and exact, so a
/// bloom filter there would add size for no pruning power.
const BLOOM_METRIC_NAME: &str = "metric_name";
const BLOOM_SERVICE_NAME: &str = "service_name";

/// Bloom filter false-positive probability.
///
/// The reader treats a positive as "the value might be present" and decodes the
/// row group anyway, so this only affects how often that happens — never
/// correctness.
const BLOOM_FPP: f64 = 0.01;

/// Truncation length for the column index, in bytes.
///
/// Long enough to keep a full path, short enough that a pathological label
/// value cannot bloat the footer.
const COLUMN_INDEX_TRUNCATE_LENGTH: usize = 64;

/// Target data page size, in bytes.
///
/// Smaller than the Parquet default so the column index has pages finer than
/// `row_group_size` to point at.
const DATA_PAGE_SIZE_LIMIT: usize = 512 * 1024;

/// Writes one block to Parquet, splitting it into `row_group_size`-row groups.
///
/// `row_group_size` is not cosmetic: the scanner skips whole row groups whose
/// timestamp statistics fall outside a query window, so this is what decides
/// whether a narrow query decodes a slice of the block or all of it. It comes
/// from `BlockConfig::row_group_size` (100k for metrics, 20k for logs by
/// default). A zero would be rejected by Parquet itself, so it is clamped.
pub fn write_block_file(
    path: &Path,
    record_batch: arrow::record_batch::RecordBatch,
    compression: &str,
    compression_level: Option<i32>,
    row_group_size: usize,
    wal_range: Option<(u64, u64)>,
    series: Option<&SeriesTable>,
) -> Result<()> {
    let file = File::create(path)?;

    // The WAL span this block makes durable, and (for metrics) the series it
    // holds, travel in the file itself: a block found on disk without an index
    // entry still says what it covers, and the series table cannot fall out of
    // step with the rows it describes.
    let footer = footer_metadata(wal_range, series)?;

    let writer_props = WriterProperties::builder()
        .set_key_value_metadata(footer)
        .set_compression(compression_from_name(compression, compression_level))
        .set_writer_version(WriterVersion::PARQUET_2_0)
        .set_max_row_group_row_count(Some(row_group_size.max(1)))
        // A bloom filter per row group lets the reader reject a row group for a
        // metric it does not contain without decoding any page. A metrics block
        // holds many metric names, so time-range statistics alone cannot narrow
        // a single-metric query: every row group overlaps in time and all of
        // them were being decoded.
        // Per-column, so only these two pay the size cost. Enabling it for
        // every column would bloat blocks with filters nothing reads.
        .set_column_bloom_filter_enabled(ColumnPath::from(BLOOM_METRIC_NAME), true)
        .set_column_bloom_filter_enabled(ColumnPath::from(BLOOM_SERVICE_NAME), true)
        // Default false-positive probability. Named so the trade is visible:
        // a filter this good costs roughly 10 bits per distinct value.
        .set_bloom_filter_fpp(BLOOM_FPP)
        // Page-level statistics plus a bounded column index. Row-group
        // statistics are all-or-nothing at `row_group_size` granularity; the
        // column index lets a reader skip a *page* inside a group.
        .set_statistics_enabled(EnabledStatistics::Page)
        .set_column_index_truncate_length(Some(COLUMN_INDEX_TRUNCATE_LENGTH))
        // Bound the page size so the column index has finer granularity than a
        // whole row group would otherwise give.
        .set_data_page_size_limit(DATA_PAGE_SIZE_LIMIT)
        .build();

    let mut writer = ArrowWriter::try_new(file, record_batch.schema(), Some(writer_props))
        .map_err(|e| Error::Parquet(e.to_string()))?;

    writer
        .write(&record_batch)
        .map_err(|e| Error::Parquet(e.to_string()))?;
    writer.close().map_err(|e| Error::Parquet(e.to_string()))?;
    Ok(())
}
