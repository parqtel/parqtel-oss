# Changelog

All notable changes to Parqtel are recorded here.

The format follows [Keep a Changelog](https://keepachangelog.com/en/1.1.0/),
and this project adheres to [Semantic Versioning](https://semver.org/).

Releases are cut by pushing a `v*` tag. `.github/workflows/release.yml` re-runs
the full test suite on the tagged commit before publishing, so an untested
build cannot reach a registry or the release page.

## [Unreleased]

Nothing yet.

## [0.3.1]

Chart and release-pipeline release. No binary behaviour changes.

### Fixed

- **Releases no longer fail on the Trivy step.** Every tag since `v0.1.0` produced
  a red release run. The failure was in Trivy's own installer, not the scan:
  `trivy-action` was unpinned, resolved v0.65.0, and its install step aborted
  before examining a single image layer. Because `github-release` needs
  `trivy-scan`, Docker Publish, Helm Publish and Sign Image all succeeded and
  published — and the GitHub Release and homebrew tap were skipped anyway.
  Trivy is now pinned to `v0.58.1`, the version the CI filesystem scan already
  runs green on every push. The SARIF upload is guarded, because
  `upload-sarif` hard-fails on an empty file and would turn "no vulnerabilities
  found" into a failed release. Scan semantics are unchanged: still report-only,
  since image findings are distroless base CVEs with no source-level remediation.
  The CI filesystem scan remains the hard gate.
- **Chart values that were silently ignored.** `wal_max_segment_bytes` and
  `wal_sync_interval_ms` were never rendered by the ConfigMap template, so
  there was no way to tune WAL segment size or fsync interval without
  hand-patching a ConfigMap that the next `helm upgrade` discards. Both are now
  rendered and settable.

### Changed

- **Probes are configurable.** `livenessProbe`, `readinessProbe` and
  `startupProbe` were hardcoded in the deployment template. On start, parqtel
  replays whatever the WAL did not flush, and that replay is proportional to
  unflushed WAL — so it is slow exactly when the pod is least healthy. The
  hardcoded startup budget was 150s; an install restarting with a large WAL
  replay exceeds it, is killed by the liveness probe mid-replay, and restarts
  with an even larger WAL. Defaults reproduce the previous values exactly, so
  upgrading changes nothing until edited, and each probe can be set to `{}` to
  disable.
- **The metrics write-ahead log is now on by default**, matching logs. It
  defaulted to off, which is the wrong default for a product that acknowledges
  writes over HTTP: without it, a crash loses up to a whole block window and the
  ingest path degrades to synchronous flushing, so an acknowledged write is not
  recoverable. Still overridable via `parqtel.ingest.walEnabled`.

## [0.3.0]

Second public beta.

### Features

- **Built-in alert preset packs.** Five ready-made packs — kubernetes-cluster,
  coredns, external-secrets, service-red and parqtel-self — are embedded in the
  binary at compile time and activated by `[alerts.presets]`
  (`mode = auto|all|off`, default `auto`; `include` wins over `exclude` with a
  warning). Activation is insert-if-absent, so user rules from `rules_dir` and
  API disables/deletes always win.
- **MCP live tools.** The `parqtel-mcp-parqtel` server now serves live data
  through real tools — PromQL (instant, absolute range, or trailing window),
  label discovery, logs, ingest rates, alerts, noise statistics and topology —
  instead of echoing parameters back. The MCP framework gains a spec-compliant
  Streamable HTTP transport (`POST/GET/DELETE /mcp`), with the legacy
  `/tools/*` routes kept as deprecated aliases.
- **Overview console.** The Overview pane is rebuilt around live signal
  rollups.
- **OpenTelemetry self-telemetry.** `parqtel-server` exports its own traces
  and SLI metrics (HTTP golden signals, ingest throughput, flush
  duration/rows, buffer saturation, RSS high-water mark) over OTLP, with
  CPU profiling endpoints under `/debug/pprof`. Configured by `telemetry.*`;
  the parqtel-self preset pack alerts on it.

### Fixes

- **The AST evaluator miscounted series and ignored result limits.**
  `execute_ast` counted one series per *point* and applied no limits, so
  `query.max_series` and `query.max_samples_per_series` were silently ignored
  for nested range selectors and most composed PromQL. It now groups points by
  fingerprint first and evaluates matchers once per distinct label set, so
  both query engines report the same `total_series_count` under the same
  configured memory bound.
- **`min()` folded the same value twice** — the fold was re-evaluated to
  produce the value it had just computed.
- **`/api/v1/label/:name/values` missed buffered series.** The in-memory
  buffer is now included.
- **gRPC and HTTP disagreed on batch size.** tonic's 4 MiB default beat
  `ingest.max_body_size` (10 MiB), so a batch that succeeded over HTTP failed
  over gRPC with an opaque `ResourceExhausted`. gRPC message limits are now
  derived from `ingest.max_body_size`.
- **Alerts evaluated over a hardcoded 300 s window.** They now use
  `query.lookback_delta_ns`, the same window an operator sees in the UI.
- **The documented `PARQTEL__SECTION__KEY` env spelling was silently
  ignored.** `Env::prefixed("PARQTEL_")` left an empty first key segment, so
  every env example in the docs had no effect; the double-underscore form now
  works (single-underscore still does).
- **MCP label matchers rejected dotted label names** via the percent-encoded
  path segment.
- **The metrics UI timeline** was corrected.
- **`compose/parqtel/Dockerfile.dev` never copied `rules/`**, so the dev image
  could not build once the preset packs were embedded.

### Configuration

New options: `storage.compression_level` and `logs.compression_level` (zstd
1–22, one shared resolver for the block writers and the compactor),
`[alerts.presets]` (`mode`, `include`, `exclude`), and `telemetry.*`
(OTLP export and profiling). Config validation now rejects a `row_group_size`
of 0 or above `max_rows_per_block` — a combination that silently disabled
row-group pruning. See
[docs/CONFIGURATION.md](docs/CONFIGURATION.md).

### Security

- Dropped vulnerable `protobuf` 2.x and `quick-xml` from the pprof feature
  set.

### Documentation

- [Argo Rollouts integration guide](docs/ARGO_ROLLOUTS.md) with runnable
  AnalysisTemplate and canary Rollout examples.
- [PQL guide](docs/PQL_GUIDE.md).
- Internal working documents pruned; user documentation refreshed against the
  current build.

### Community

- CODEOWNERS, dependabot, issue and PR templates, SECURITY.md, SUPPORT.md and
  an OSSF Scorecard workflow.

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

- **Credentials nested in an array reached the MCP audit log in the clear.**
  The parameter sanitiser recursed into arrays, but the function it called
  returns non-objects unchanged — so `[{ "token": … }]` was logged verbatim.
  Recursion now handles arrays and objects uniformly, and the sensitive-key
  list covers `authorization`, `auth`, `session`, `cookie` and `private`, which
  it previously did not.
- **The Kubernetes readiness probe did not check anything.** `/health` is a
  static `200` and was wired to liveness, readiness *and* startup, so a pod was
  marked ready while its storage was failing. Added `/ready`, which reads
  block-index metadata, and pointed readiness at it.

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

Collected in [docs/KNOWN_ISSUES.md](docs/KNOWN_ISSUES.md), with a workaround for
each. They are recorded deliberately rather than left to be discovered.

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

[Unreleased]: https://github.com/parqtel/parqtel-oss/compare/v0.3.0...HEAD
[0.3.0]: https://github.com/parqtel/parqtel-oss/compare/v0.2.0...v0.3.0
[0.2.0]: https://github.com/parqtel/parqtel-oss/compare/v0.1.0...v0.2.0
[0.1.0]: https://github.com/parqtel/parqtel-oss/releases/tag/v0.1.0
