# Compaction

## Trace blocks now merge at all

`compact_tiered` carried an explicit skip:

```rust
if *signal_type == SignalType::Traces {
    // For traces, skip read_source_blocks (which only handles metrics/logs)
    continue;
}
```

so trace blocks never merged. They accumulated monotonically until retention
deleted them, and every trace query paid the per-block open/footer/decode cost
across all of them.

`read_source_blocks` now decodes spans as a third arm
(`StorageModel::row_to_span`), `write_merged` encodes them with
`StorageModel::traces_to_chunk` sorted by `start_time_ns` so the merged block
keeps the time-ordered layout row-group pruning depends on, and both passes
handle all three signals uniformly.

Verified by `test_compactor_merges_trace_blocks`: three small trace blocks
become one, all nine spans survive, and the merged block still decodes.

## Compaction no longer destroys the label-value index

`write_merged` emitted `label_values: Default::default()`. Compaction therefore
*deleted* the flush-time label-value dictionary, so
`/api/v1/label/:name/values` lost its metadata source on every merge and had to
fall back to decoding blocks — degrading progressively with every compaction
pass until the source blocks aged out.

The merged metadata now carries the **union** of its sources' dictionaries,
under the same per-field cap the flush path applies.
`test_compaction_preserves_label_values` asserts both hosts survive a merge,
and was verified to **fail against the old behaviour** (`no entry found for
key`).

## Merge limits are configuration, and a cycle can converge

- `compaction_max_merge_blocks` (default 12) replaces the literals 8 and 12.
- `compaction_max_merges_per_pass` (default 8) replaces `break`ing after the
  first merge per signal. One merge per signal per hour cannot keep up with a
  cluster whose small blocks arrive faster than that; the cycle is now bounded
  but does real work.

## Metrics now roll up into 5-minute L1 blocks

Metrics no longer use the classic small-block merge. A **streaming k-way
merge** coalesces canonical blocks into one 5-minute window:

- **Canonical L0 order.** New flushes sort by the final query-visible label
  fingerprint (`service.name` merged), so a merge by label fingerprint is
  well-defined. Files are named `m_l0_`.
- **Normalize legacy blocks.** Older flushes sorted by raw point labels, which
  can disagree with their footer index. They are rewritten one per pass (500 K
  row cap) into `m_n_` files.
- **Streaming merge.** Up to 8 ordered blocks are merged into one `m_l1_`
  block, one Arrow batch (8 192 rows) at a time, so memory does not grow with
  block size.
- **WAL preflight.** A group is merged only when every source's WAL range is
  at or below the metrics WAL commit point. Mixed or gapped ranges are skipped.
- **Provenance.** The output footer records its sources under
  `parqtel.compacted_sources`; reconcile drops superseded sources after a
  crash, and sources are deleted after a 5-minute grace period.

One pass is bounded: 8 sources, 2 groups, 1 normalisation, 16 M rows, and 2 of
the 4 process-wide scan slots. A pass that cannot reserve its slots is
deferred rather than competing with interactive queries.

`parqtel compact` runs the same bounded pass offline. It is an offline
maintenance command — stop the service first.

## The label-value dictionary moved to the index

The flush-time per-field label-value sets are no longer carried per block.
`BlockIndex` owns **one shared dictionary per signal** (10 K values per field),
and the sidecar became `{"blocks": [...], "label_values": {...}}`. On 97 real
blocks the sidecar shrank from 73.5 MiB to 12.6 MiB (17 %), because the
per-block copies were ~85 % of it.

## Verified by unit test rather than a long container run

Compaction runs every `storage.compaction_interval_secs` (5 minutes for
metrics, 1 hour for logs), so observing a trace merge end-to-end in a
short-lived container is not practical without lowering the interval and
waiting. The merge behaviour is therefore pinned by unit tests that run the
real `compact_once` against real Parquet blocks. Live container checks confirmed
no regression: spans ingest, flush (`parqtel_flush_rows_total{signal="traces"}`),
and trace search returns correct results.
