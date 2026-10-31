//! Deciding which row groups and rows of a block a scan has to read.

use parquet::arrow::arrow_reader::ParquetRecordBatchReaderBuilder;
use parquet::file::metadata::ParquetMetaData;
use parquet::file::statistics::Statistics;
use std::fs::File;

/// Leaf indices of `columns` that the file's schema actually has.
///
/// Blocks written by older builds may lack a column, and a projection naming an
/// absent column errors, so the projection is intersected with the file rather
/// than applied blindly. Leaf indices are correct here because every parqtel
/// schema is flat - no nested groups - so a leaf index equals its root index.
pub fn project_present(
    schema: &parquet::schema::types::SchemaDescriptor,
    columns: &[&str],
) -> Vec<usize> {
    columns
        .iter()
        .filter_map(|name| schema.columns().iter().position(|c| c.name() == *name))
        .collect()
}

/// Row-group indices whose `ts_column` statistics can overlap `[start_ns, end_ns]`.
///
/// Skipping a row group is always *sound*: a row group's min/max bound every
/// row it contains, so a group entirely outside the window cannot hold a
/// matching row — and that holds whether or not the rows are sorted. It is
/// only *useful* when rows are roughly time-ordered, which they are here:
/// both flush and compaction sort before writing, so a block's row groups
/// cover disjoint time slices.
///
/// Returns `None` when the timestamp column cannot be located, in which case
/// the caller must read every row group. Row groups with unusable statistics
/// are kept rather than skipped, so the worst case is reading more than
/// needed, never returning fewer rows.
/// Drops row groups that cannot contain `metric_name`, using the bloom filters
/// the writer emits on `metric_name` and `service_name`.
///
/// Row-group statistics only narrow on **time**. A metrics block interleaves
/// every metric it holds, so a query for one metric finds every row group
/// overlapping the window and decodes all of them. A bloom filter answers "could
/// this row group contain this value?" without decoding a page, which is the
/// difference between reading one row group per query and reading all of them.
///
/// Two mechanisms, cheapest first:
///
/// 1. **Row-group statistics.** Rows are written grouped by metric name, so a
///    row group's `metric_name` min and max are usually the same value. Testing
///    the needle against that range is exact and free — no filter read, no
///    probabilistic answer. This is what prunes in practice.
/// 2. **Bloom filters**, consulted only when the statistics cannot decide
///    (column missing, no min/max, or an unexpected physical type).
///
/// Either way this can only skip groups that genuinely lack the value: a
/// statistic range or a bloom filter may report a false *positive*, never a
/// false negative. Any condition that cannot be evaluated keeps the group, so
/// the worst case is the behaviour that existed before any of this.
///
/// An empty `metric_name` means "no metric filter" and keeps every candidate.
///
/// Only `metric_name` is filtered here. Rows are grouped by metric and resource,
/// so the *row-group statistics* on `service_name` prune on service exactly and
/// for free — strictly better than a bloom filter, which can only be
/// probabilistic.
pub fn row_groups_matching_metric(
    reader: &ParquetRecordBatchReaderBuilder<File>,
    candidates: &[usize],
    metric_column: &str,
    metric_name: &str,
) -> Vec<usize> {
    row_groups_matching_value(reader, candidates, metric_column, metric_name)
}

/// Narrows `candidates` to the row groups that may contain `value` in
/// `column`. See [`row_groups_matching_metric`] for why statistics come first
/// and what the guarantees are.
pub fn row_groups_matching_value(
    reader: &ParquetRecordBatchReaderBuilder<File>,
    candidates: &[usize],
    column: &str,
    value: &str,
) -> Vec<usize> {
    let columns = reader.metadata().file_metadata().schema_descr().columns();
    let Some(col_idx) = columns.iter().position(|c| c.name() == column) else {
        // No such column: cannot prune, keep everything.
        return candidates.to_vec();
    };
    // An empty metric name means "no metric filter", not "the metric whose name
    // is the empty string". Callers use it to scan a block for every series -
    // `list_label_values` falls back to a full scan when a block predates the
    // flush-time label index. Treating it as a value to match would prune the
    // whole block, since the empty string sorts before every real name.
    if value.is_empty() {
        return candidates.to_vec();
    }
    let needle = value.as_bytes();
    let mut kept = Vec::with_capacity(candidates.len());
    for &rg in candidates {
        let col_md = reader.metadata().row_group(rg).column(col_idx);

        // Statistics first: they are free and, because rows are written grouped
        // by metric name, a row group's min and max for `metric_name` are
        // usually the same value — exact, not probabilistic. This is the check
        // that actually does the work on well-written blocks.
        //
        // This is a one-sided test, so unlike the time prune it does **not**
        // need a null check: min/max cover non-null values only, and a null row
        // cannot contain the value being searched for. If the value lies
        // outside [min, max] then no row in this group has it.
        //
        // Byte comparison is sound: UTF-8 byte order is code-point order, so
        // comparing raw bytes against the encoded needle matches string order.
        let decided_by_stats = match col_md.statistics() {
            Some(Statistics::ByteArray(s)) => match (s.min_opt(), s.max_opt()) {
                (Some(min), Some(max)) => {
                    if min.data() > needle || max.data() < needle {
                        continue;
                    }
                    true
                }
                // All-null column, or no min/max: says nothing either way.
                _ => false,
            },
            // A different physical type (INT32 indices for an older writer,
            // say): not comparable here.
            _ => false,
        };
        if decided_by_stats {
            kept.push(rg);
            continue;
        }

        // Statistics could not decide, so fall back to the bloom filter. It can
        // report a false positive but never a false negative, so this is also
        // safe to use for absence.
        match reader.get_row_group_column_bloom_filter(rg, col_idx) {
            Ok(Some(filter)) => {
                if !filter.check(needle) {
                    continue;
                }
            }
            // No filter on this column for this group: cannot rule it out.
            Ok(None) => {}
            Err(e) => {
                tracing::trace!(
                    row_group = rg,
                    error = %e,
                    "bloom filter unreadable; keeping row group"
                );
            }
        }
        kept.push(rg);
    }
    kept
}

pub fn row_groups_in_range(
    metadata: &ParquetMetaData,
    ts_column: &str,
    start_ns: i64,
    end_ns: i64,
) -> Option<Vec<usize>> {
    let col_idx = metadata
        .file_metadata()
        .schema_descr()
        .columns()
        .iter()
        .position(|c| c.name() == ts_column)?;

    let mut keep = Vec::with_capacity(metadata.num_row_groups());
    for rg_idx in 0..metadata.num_row_groups() {
        let statistics = metadata
            .row_group(rg_idx)
            .columns()
            .get(col_idx)
            .and_then(|c| c.statistics());
        match statistics {
            // Timestamps are stored as INT64 nanoseconds, so their statistics
            // arrive as `Int64` regardless of the logical timestamp type.
            Some(Statistics::Int64(s)) => {
                // min/max cover non-null values only: with nulls present we
                // cannot prove an out-of-range group holds no matching row.
                let nulls = s.null_count_opt().unwrap_or(0);
                match (s.min_opt(), s.max_opt()) {
                    (Some(min), Some(max)) if nulls == 0 => {
                        if *max >= start_ns && *min <= end_ns {
                            keep.push(rg_idx);
                        }
                    }
                    _ => keep.push(rg_idx),
                }
            }
            // Statistics disabled, or a non-INT64 column: keep the group.
            _ => keep.push(rg_idx),
        }
    }
    Some(keep)
}

/// Restricts a reader to the given file row ranges: only the row groups that
/// hold them are visited, and a selection skips the rest inside those groups.
/// When the ranges are whole groups no selection is needed.
pub(super) fn restrict_to_ranges<T>(
    builder: ParquetRecordBatchReaderBuilder<T>,
    ranges: &[(u64, u64)],
) -> ParquetRecordBatchReaderBuilder<T>
where
    T: parquet::file::reader::ChunkReader + 'static,
{
    let (groups, selection) = plan_row_selection(builder.metadata(), ranges);
    let whole: u64 = groups
        .iter()
        .map(|g| builder.metadata().row_group(*g).num_rows().max(0) as u64)
        .sum();
    let wanted: u64 = ranges.iter().map(|(a, b)| b - a).sum();
    let builder = builder.with_row_groups(groups);
    if whole == wanted {
        builder
    } else {
        builder.with_row_selection(selection)
    }
}

/// Turns row ranges of the whole file into the row groups that hold them and a
/// selection over **those groups' rows**, which is what the reader expects once
/// the groups are restricted. Groups with no selected row are not read.
///
/// `ranges` are ascending and non-overlapping.
pub(super) fn plan_row_selection(
    metadata: &ParquetMetaData,
    ranges: &[(u64, u64)],
) -> (Vec<usize>, parquet::arrow::arrow_reader::RowSelection) {
    let mut groups = Vec::new();
    let mut local: Vec<std::ops::Range<usize>> = Vec::new();
    let mut file_start = 0u64;
    let mut selected_rows = 0usize;
    let mut first = 0usize;
    for (g, rg) in metadata.row_groups().iter().enumerate() {
        let rows = rg.num_rows().max(0) as u64;
        let (lo, hi) = (file_start, file_start + rows);
        // Ranges ending at or before this group cannot touch a later one.
        while first < ranges.len() && ranges[first].1 <= lo {
            first += 1;
        }
        let mut any = false;
        for &(a, b) in &ranges[first..] {
            if a >= hi {
                break;
            }
            let (a, b) = (a.max(lo), b.min(hi));
            if a < b {
                local.push(selected_rows + (a - lo) as usize..selected_rows + (b - lo) as usize);
                any = true;
            }
        }
        if any {
            groups.push(g);
            selected_rows += rows as usize;
        }
        file_start = hi;
    }
    (
        groups,
        parquet::arrow::arrow_reader::RowSelection::from_consecutive_ranges(
            local.into_iter(),
            selected_rows,
        ),
    )
}
