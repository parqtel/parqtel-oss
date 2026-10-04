# Changelog

All notable changes to Parqtel are recorded here.

The format follows [Keep a Changelog](https://keepachangelog.com/en/1.1.0/),
and this project adheres to [Semantic Versioning](https://semver.org/).

Releases are cut by pushing a `v*` tag. `.github/workflows/release.yml` re-runs
the full test suite on the tagged commit before publishing, so an untested
build cannot reach a registry or the release page.

## [Unreleased]

Nothing yet.

## [0.2.0]

Initial public beta.

### Performance

Query and storage work landed over 30-odd pull requests, each measured against
a running build. Full method and caveats in
[docs/benchmarks/PERFORMANCE.md](docs/benchmarks/PERFORMANCE.md).

- **Query evaluation.** Label sets are shared behind an `Arc` throughout the
  evaluator instead of being cloned per series per *step* — ~20 M string
  allocations per panel eliminated. Aggregation groups are keyed by fingerprint
  with the group's label set built once per distinct group. Step results are
  folded into the response as they are produced rather than all being retained:
  peak memory for a wide panel dropped from 168 MB to 110 MB.
- **Log and trace search.** Queries are *prepared* once per request — regexes
  and wildcards compiled, needles lowercased — rather than per row, with an
  allocation-free ASCII-case-folded substring scan. Measured 205× on a
  representative query.
- **Query parallelism.** Evaluation runs on the blocking pool, so a wide range
  query cannot starve an async worker. Four concurrent queries run ~3.5× faster
  than sequentially.
- **Block pruning.** Rows are written grouped by metric name, so a row group's
  `metric_name` and `service_name` statistics are exact. The reader uses them to
  skip row groups, plus bloom filters on the same columns. A single-metric query
  is ~12.9× faster; a service-scoped one ~4.8×.
- **Column projection.** The metrics scan reads 7 of 15 columns; a full-block
  scan is ~28 % cheaper.
- **Storage lifecycle.** Compaction covers trace blocks (previously skipped
  entirely, so they accumulated until retention deleted them), merge limits are
  configurable rather than literal, and a cycle performs a bounded number of
  merges instead of one per signal.

### Durability

- **Write-ahead log** (`parqtel-core/src/wal`), on by default. Bounds crash
  loss to `ingest.wal_sync_interval_ms` (1 s) rather than the whole block
  window (2 h by default). A torn tail from a crash mid-append is detected by
  per-record CRC and truncated on replay. Replays across metrics, logs and
  traces are verified against killed containers.
- **Durable index before WAL commit.** A flush now waits for the block index
  sidecar to be durable before committing the WAL, and fails *safe* on timeout
  by leaving the WAL uncommitted. Without this, a crash left acknowledged data
  on disk, absent from the index, and already marked recovered.
- **Self-healing block index.** At startup the index is reconciled against the
  data directory in both directions: unknown blocks are adopted from their
  Parquet footer, and entries whose file has gone are dropped.
- **Background flush.** A request that crosses the block cap is acknowledged
  once the WAL has its rows rather than after the encode. Requires a WAL; with
  `ingest.wal_enabled = false` the worker refuses to start and flushing stays
  synchronous.
- **Sharded metrics ingest.** Unrelated metrics ingest concurrently while a
  flush encodes, with the per-shard WAL order matching the writer order.

### Fixes

- **Oversized ingest batches lost half their data.** A batch larger than the
  block cap was rejected with `400 Block writer buffer is full` *after* part of
  it had been accepted. It is now split across blocks and ingested in full.
- **`max_body_size` was not honoured.** axum's 2 MB default silently beat the
  configured 10 MiB, so raising it changed nothing and large batches failed
  with an unexplained `413`.
- **Compaction discarded the label-value index.** Merged blocks were written
  with an empty dictionary, degrading `/api/v1/label/:name/values` on every
  compaction pass.
- **A query series selector could be routed incorrectly** when the selector
  matched more than one metric name.

### Configuration

New options: `ingest.rotator_shards`, `ingest.max_inflight_flushes`,
`ingest.wal_sync_mode`, `ingest.wal_sync_interval_ms`,
`ingest.wal_max_segment_bytes`, `storage.compaction_max_merge_blocks`,
`storage.compaction_max_merges_per_pass`, `server.flush_interval_secs`,
`server.alert_interval_secs`, `server.retention_interval_secs`,
`server.index_persist_interval_secs`, `server.grpc_concurrency_limit`. All
default to the previous hardcoded behaviour. See
[docs/CONFIGURATION.md](docs/CONFIGURATION.md).

### Observability

Added `parqtel_ingest_lock_wait_seconds`, `parqtel_flush_duration_seconds`,
`parqtel_flush_inflight`, `parqtel_flush_rows_total`,
`parqtel_index_lock_wait_seconds`, `parqtel_index_sidecar_bytes`,
`parqtel_index_pending_writes` and `parqtel_ingest_rotator_shards`. `/metrics`
no longer reads `/proc`.

### Known limitations

Recorded deliberately rather than left to be discovered. See
[docs/PERFORMANCE_SIZING.md](docs/PERFORMANCE_SIZING.md) and
[docs/PQL_GUIDE.md](docs/PQL_GUIDE.md).

- The in-memory buffer is unbounded — there is no max size, no byte accounting
  and no eviction. Resident memory is a function of ingest rate ×
  `storage.block_duration_secs` with no ceiling. Size the container from the
  formula in `docs/PERFORMANCE_SIZING.md` until this is bounded.
- A range query evaluates steps from `start` while `ts < end`, so samples newer
  than one `step` before `end` are not visible to `/api/v1/query_range` (an
  instant query at the same `end` still sees them through the 5-minute
  lookback). Use `step <= 15` for a window ending at "now".
- Log and trace scans still decode every column; only the metrics scan is
  projected.
- No authentication or authorization. Do not expose the HTTP or OTLP ports
  publicly without a reverse proxy that provides it.

## [0.1.0]

- Initial internal release.

[Unreleased]: https://github.com/parqtel/parqtel-oss/compare/v0.2.0...HEAD
[0.2.0]: https://github.com/parqtel/parqtel-oss/compare/v0.1.0...v0.2.0
[0.1.0]: https://github.com/parqtel/parqtel-oss/releases/tag/v0.1.0
