# Series table in every metric block

Label matchers were already evaluated once per distinct series, but a block still
had to decode every row of the metric to find the rows of the series that
matched: a one-series selector read 100% of its metric and kept 0.05% of it. A
metric block now describes its own series, so the work scales with what the query
selects.

## What a block carries

Rows are written series by series (each in time order), so a series is one
contiguous row range. The Parquet footer holds a table of the block's series:
labels (with `service.name` merged in, as a scan reports them), first row, row
count and time bounds, with strings stored once. Loading it builds postings (label
name → value → ascending series ids); a selector is answered by intersecting
postings, dropping series outside the time window, and letting the full matcher
judge what is left. The surviving series' row ranges become a Parquet
`RowSelection` over the four columns the values live in (`timestamp_ns` and the
three value columns), so `labels`, `metric_name` and `service_name` are not
decoded and pages with no selected row are skipped. The table adds about 1.7% to
block size (224 -> 228 MB for the benchmark dataset).

## Result

Apple M1, 8 cores, thin-LTO release builds of the commit before and the commit
after, same dataset (2.4 M metric points, 20 000 series across 10 metrics, 3
blocks), p50:

| | before, 1 client | after, 1 client | before, 8 clients | after, 8 clients |
|---|---|---|---|---|
| single-series selector | 15.4 ms | 0.88 ms | 40.7 ms | 1.8 ms |
| `sum by (env) (m)` | 28.6 ms | 7.1 ms | 77 ms | 20 ms |
| `sum by (region) (rate(m[1m]))` | 32.0 ms | 8.5 ms | 81 ms | 24 ms |
| `avg(m)` over 1 h | 30.1 ms | 6.5 ms | 75 ms | 20 ms |
| `topk(5, rate(m[1m]))` | 28.8 ms | 7.1 ms | 76 ms | 21 ms |
| instant `sum(m)` | 14.9 ms | 2.9 ms | 32 ms | 9.7 ms |
| peak RSS | 0.80 GB | 0.79 GB | 1.70 GB | 1.51 GB |

Ingest is unchanged (1.8-1.9 M points/s; the flush sorts by series instead of by
time and writes the table). Aggregations over all series fall too, although they
select everything: the table lets the scan read four columns instead of seven and
skip the per-row label lookup.

## Correctness

- `indexed_scan_equals_unindexed_scan` writes the same data with and without a
  table over many row groups and compares the points returned for seven selector
  shapes (service, equality, three equalities, regex narrowing, filter only, no
  such value), four time windows and four metric names (including none and an
  unknown one).
- A table that is not JSON, or whose row count differs from the file's, is ignored
  and the block is scanned in full (tested). If a table that passed that check
  still disagrees with the rows while scanning, the scan of that block is
  abandoned and redone without it (not covered by a test).
- The query results of the old and new builds were compared over the same blocks
  for selectors, regex matchers, `rate`, `topk`, `count` and `avg`: identical
  wherever the result is not cut by the series limit.
- `kill -9` under load, three runs: every acknowledged metric recovered, none
  twice.

## A bug found on the way

Comparing two builds of the same data showed aggregation results that differed
between **restarts of the same binary**. The label-set fingerprint was documented
as stable but used a randomly seeded hasher, and series are sorted by fingerprint
before the series limit (default 1 000) is applied, so any query matching more
series than the limit returned a different subset after each restart. The
fingerprint is now a fixed function of the labels.
