# Changelog

All notable changes to Parqtel are recorded here.

The format follows [Keep a Changelog](https://keepachangelog.com/en/1.1.0/),
and this project adheres to [Semantic Versioning](https://semver.org/).

Releases are cut by pushing a `v*` tag. `.github/workflows/release.yml` re-runs
the full test suite on the tagged commit before publishing, so an untested
build cannot reach a registry or the release page.

## [Unreleased]

Ingest and query performance, and a set of durability and visibility fixes found
while measuring. Measurements and method are in
[docs/performance/README.md](docs/performance/README.md).

### Performance

- **Ingest**: metrics 0.62 → 2.0 M points/s, logs 0.10 → 1.8 M records/s, p99
  ingest latency 55 → 17 ms (metrics) and 538 → 10 ms (logs), on 8 cores.
- **Queries**: single-series selector 156 → 15 ms, aggregations 180 → 28 ms,
  instant queries 107 → 17 ms, log search 284 → 102 ms (p50, 2.4 M-point
  dataset).
- Peak RSS under the same load fell from 2.3 GB to 0.8 GB.
- **Memory at high series counts**: with 578K active series (2.3M points through
  the HTTP ingest path) peak RSS fell from 4.1 GB to 1.6 GB and steady RSS from
  3.6 GB to 1.0 GB. Two changes: `ingest.label_interner_capacity` (new, default
  1048576; set it to at least twice the active series count) and jemalloc as the
  global allocator on non-MSVC targets, which returns freed block-encode memory
  to the OS instead of keeping it resident. Startup WAL replay now replays one
  segment at a time and shares label sets, so replay memory no longer grows with
  the unflushed backlog.
- **Log search** 102 → 23 ms (a term matching a fifth of the records) and
  29 → 12 ms (a term in one record) at p50, one client; with 8 clients 311 → 64 ms
  and 113 → 32 ms.
- **Series table in every metric block** (see Added): a selective query now reads
  only the rows of the series it selects. Single-series selector 15 → 0.9 ms,
  aggregations 28 → 7 ms, instant queries 15 → 3 ms (p50, one client, same
  dataset and build mode); with 8 concurrent clients 41 → 1.8 ms, 77 → 20 ms and
  32 → 9.7 ms.

### Added

- **Kafka JSON log consumer.** `[ingest.kafka]` polls a topic, decodes each
  message with the generic-JSON parser, writes through the normal WAL/buffer
  path, and commits the offset only after the batch is durable.
- **OIDC login and role mapping.** Optional `[auth.oidc]` adds an
  authorization-code flow at `/auth/login` and `/auth/callback`, an HttpOnly
  session cookie, JWKS/RS256 bearer validation, and role-to-scope mapping
  (`admin_role` / `write_role`; every authenticated user reads).
- **Object-store compaction and retention.** Compaction opens sources and
  deletes merged sources through `BlockStore`; retention deletes through it too.
  Catalog entries are writer-owned, so a node retires only blocks it wrote and
  deliberately dropped (retention/compaction), never another node's live data.
- **Shared object-storage catalog.** Writers publish a versioned manifest with
  a CAS update; every node polls it, adopts blocks it has not seen, and marks
  its sidecar dirty. Concurrent writers merge by block key instead of
  overwriting each other's catalog.
- **S3-compatible block upload with a bounded read cache.** `[object_store]`
  enables upload on block close and materialises cache files for Parquet seeks;
  the cache is LRU-evicted under `cache_bytes`. Local disk remains the WAL
  staging area and the default backend.
- **`BlockStore` seam with a local backend.** Scanner reads, trace reads, and
  retention deletes now go through a process-wide store; absolute block paths
  keep the previous local behavior. The server installs `LocalBlockStore` at
  startup, giving object storage a place to cache and publish blocks later.
- **Compressed block-index sidecar.** `index.json` is now a `PQZ1` zstd frame
  instead of plain JSON; plain JSON sidecars still load. On the local 130-block
  corpus the sidecar fell from 42 KB to 2.8 KB, 0.4% of Parquet bytes.
- **Two-phase trace scans.** Trace searches now read only `start_time_ns`
  and `trace_id` first, then decode attributes, events, and links for selected
  rows. This is the trace half of P1-05; log scans already used the same
  late-materialisation pattern.
- **Persisted dashboards.** `GET/POST /api/v1/dashboards`,
  `PUT/DELETE /api/v1/dashboards/{id}`, and
  `GET /api/v1/dashboards/{id}/render?start=&end=` store named panels over
  metric or log queries in `dashboards.json` and execute every panel against
  the live engine. Panels carry query text, never HTML.
- **Page-index pruning for log, metric, and trace scans.** Blocks written with
  page statistics now skip data pages wholly outside the query window before
  decoding them; blocks without a page index fall back to row-group statistics.
- **Median+MAD baseline anomaly rules.** `LearnedAnomaly` rules now query their
  configured baseline window and fire when the latest sample deviates from the
  median by more than `sensitivity` MADs, instead of silently reusing the static
  threshold. Noise scoring remains reserved.
- **Three more MCP tools.** `query_traces`, `get_storage_stats`, and
  `get_pipelines` join the seven existing tools, so an agent can inspect spans,
  cost drivers, and rule/pipeline health without falling back to raw REST.
- **Chunked NDJSON log streaming.** `GET /api/v1/logs/stream` runs the normal
  log search in 5-minute slices and streams each record as a JSON line, so a
  large range is bounded by one slice instead of the full result set.
- **Per-service stats.** `GET /api/v1/stats?dataset=<service.name>` reports
  matching block count, rows, bytes, metric count, global sidecar bytes, and
  compaction lag; the global snapshot is unchanged.
- **Per-metric series cap.** `ingest.series_limit_per_metric` (default 1M)
  refuses new label sets past the cap while existing series keep ingesting,
  and exports `parqtel_series_rejected_total{metric}`. One label explosion no
  longer degrades every metric on the node.
- **Live tail over SSE.** `GET /api/v1/logs/tail` streams buffered log rows as
  server-sent events with optional `service_name` and `body_contains` filters.
  It polls the existing memory buffer (250 ms) and requires the read scope.
- **Prometheus `remote_write`.** `POST /api/v1/write` accepts the standard
  snappy-compressed protobuf WriteRequest, converts samples to gauge points
  (ms timestamps), and writes them through the metrics ingestion path. This
  unlocks Prometheus agents/collectors that speak remote_write.
- **Generic JSON log ingest.** `POST /api/v1/ingest` accepts an object or array,
  maps timestamp/body/severity via `ingest.json_*_field`, turns the remaining
  keys into attributes, and takes the service name from
  `X-Parqtel-Service`. This is the on-ramp for applications without an OTel
  shim. Rows go through live log pipelines and the normal WAL/buffer path.
- **Append-only audit trail.** Config mutations (alert rules/routes/silences,
  recording rules, pipelines) append a JSONL record with timestamp, token
  actor, action, target, and before/after values. Webhook URLs are redacted.
  `[audit]` controls the path (default `data/audit.jsonl`) and can disable it.
- **In-process TLS.** `[server.tls]` takes a PEM certificate/key pair and serves
  the HTTP API/console over HTTPS (no plaintext listener) and OTLP gRPC over
  TLS with the same identity. Verified with a self-signed certificate; plaintext
  requests to the TLS port are rejected during the handshake.
- **Scoped bearer tokens.** Optional `[auth]` config with hashed `read`,
  `write`, and `admin` tokens. Health/readiness and the static console shell
  stay public; data routes require a matching token, ingest requires `write`,
  and mutations require `admin`. Disabled by default. The OTLP gRPC listener
  still needs a proxy/mesh boundary.
- **Rule definitions are validated and observable.** API-created recording
  rules and pipelines are validated/compiled before storage (`400` on failure),
  watcher deletes now remove the definition, and `GET /api/v1/rules/status`
  reports the registry generation, definitions, load errors, and persisted
  ruler state.
- **Recording rules execute.** `RulerScheduler` now runs in `serve`, evaluates
  `rules/recording/` groups on their intervals against the live query executor,
  and writes results through the metrics ingestion service so they share its
  WAL, buffer, and index. State is persisted under `data/ruler/state.json`;
  `alerts.recording_rules_enabled = false` disables the loop.
- **Pipelines execute on live log ingest.** YAML pipeline definitions are now
  compiled into stages with validation at load time (unknown processors,
  invalid regex, missing fields, unsupported signals) and applied to live logs
  with first-match-wins. Extracted metrics are forwarded to the metrics
  ingestion service through a bounded channel; surviving logs continue through
  the normal WAL/buffer/writer. WAL replay skips pipeline derivation so a crash
  does not duplicate derived metrics.
- **PII masker stage.** The new `masker` stage rewrites the stored log body and
  matching attributes, not just the pipeline field map. The
  `nginx-access-logs.yaml` example compiles and produces its counter and
  latency metrics; the `/health` router drops the matching rows.
- **Dedicated scan slot for alert evaluation.** Alert evaluation no longer
  competes with queries for the four-slot scan budget; it reserves its own slot
  and reports `parqtel_alert_eval_skipped_total` when a previous cycle still
  holds it.
- **Query timeout scoped to scans, ingest load shedding.** `query.timeout_secs`
  now applies only to query/log/trace/Grafana scan endpoints; a slow ingest is
  no longer reported as a query timeout. OTLP/HTTP ingest is bounded by
  `ingest.max_concurrent_requests` (default 64) and returns `429` with
  `Retry-After` when saturated. `parqtel_ingest_rejected_total` counts it.
- **Bounded in-memory query buffer.** `ingest.buffer_max_points` (default 5M
  rows) and `ingest.buffer_max_bytes` (default 2 GiB estimated) cap the
  metrics/logs/traces buffer, with `drop_oldest` or `reject_new` overflow
  policy. The writer and WAL keep acknowledged rows, so eviction shortens the
  pre-flush query window rather than losing data. New counters:
  `parqtel_buffer_points{signal}`, `parqtel_buffer_bytes`,
  `parqtel_buffer_limit_points`, `parqtel_buffer_limit_bytes`, and
  `parqtel_buffer_dropped_total{signal,reason}`.
- **Runtime configuration is persisted and gated.** Alert routes written over
  the API are saved atomically to `data/alert_routes.json`, and recording-rule
  and pipeline definitions to `rules/recording/` and `rules/pipelines/`; all
  three are reloaded at startup. Because the OSS build has no authentication,
  the mutation endpoints now return `403` unless
  `alerts.allow_runtime_updates = true`.
- **Outbound egress policy for alert delivery.** Every webhook destination —
  legacy `[alerts.notifications.routes]` and Alertmanager receivers — is
  checked immediately before the POST. The default rejects loopback, private,
  link-local, CGNAT, ULA, multicast and reserved addresses, and disables HTTP
  redirects, closing the unauthenticated SSRF path through
  `POST /api/v1/alerts/routes`. Operators with a receiver on a private network
  opt in with `[alerts.notifications.egress]` / `[alerting.routing.egress]`
  (`allow_private`, `allow_hosts`). See
  [configuration](docs/operations/configuration.md).
- **Skip index in each log block.** The Parquet footer (`parqtel.log_index`)
  holds, per granule of 4 096 rows, the time bounds, the highest severity and a
  bloom filter over the body's three-byte windows and the records' `name=value`
  attribute and resource-attribute pairs. A search reads only the granules that
  can hold a row meeting all its conditions, so a needle that occurs in one
  record touches one or two granules. A granule is skipped only when it provably
  holds no match; counts and volume buckets are unchanged. Costs about a byte per
  row.
- Log blocks also index trace and span ids; `trace_id`, `span_id`, `service`, `attr.K`
  and `res.K` equalities of a LogQL query (and bare field names, when no
  pipeline-enriched fields are in play) narrow the granules read.
- Searches that are only severity and body conditions count, bucket and rank
  matches from three columns and decode the other sixteen only for the rows they
  return.
- **Series table in each metric block.** Metric blocks are now laid out series by
  series (each in time order) and carry, in the Parquet footer
  (`parqtel.series_index`), a table of their series: labels, the rows each one
  occupies and its time bounds. At load the table becomes an inverted index
  (label name → value → series) that narrows a selector to its candidate series
  without touching a row; the candidates' row ranges become a Parquet row
  selection over the timestamp and value columns only. Labels come from the
  table, so the label, metric-name and service columns are not read. The
  matcher still judges every candidate, so a block read through its table
  returns exactly what a scan of the same block without one returns. Flushed,
  compacted and engine-written blocks all carry it; a block without a valid
  table (or one that disagrees with its row count) is scanned as before.
  `points_scanned` now counts the rows actually read.
- The memory buffer judges a metric selector once per distinct series before
  reading any buffered point, so a selector that matches nothing in the buffer
  costs nothing.
- Footer cache is bounded by bytes (192 MiB) rather than entry count, and holds
  each block's parsed series table.
- **gzip-compressed OTLP**, which the OpenTelemetry Collector's exporters send by
  default: request bodies on HTTP (limits apply to the decompressed size) and
  gRPC.
- **Alertmanager-compatible alert routing** in a new `[alerting.routing]`
  section: a hierarchical `route` tree with `=`, `!=`, `=~`, `!~` matchers,
  `continue` fan-out, `group_by` (including `"..."`) and inherited
  `group_wait` / `group_interval` / `repeat_interval`, plus `inhibit_rules`
  (`source_matchers`, `target_matchers`, `equal`). Alerts are grouped by
  route and label set, held for `group_wait`, and re-notified on new members
  (`group_interval`) or on `repeat_interval`; resolved alerts are delivered to
  receivers with `send_resolved = true`. Active groups are bounded by
  `max_active_groups`.
- **Native receivers**: Slack incoming webhooks, PagerDuty Events API v2
  (`dedup_key` per group, `trigger`/`resolve`), Microsoft Teams Adaptive Cards,
  and a generic webhook. Transient failures (`429` honoring `Retry-After`,
  `5xx`, timeouts) retry with exponential backoff and jitter; `400`/`401`/
  `403`/`404` fail immediately.
- Secrets and webhook URLs may be written as `${ENV_VAR}` or `{env:ENV_VAR}`
  so tokens stay out of the config file.
- `POST /api/v1/alerts/test_route` dry-runs a label set through the routing
  tree and reports matched routes, receivers, grouping keys, and
  silence/inhibition status; `GET /api/v1/alerts/routes` now also returns the
  configured tree with `***REDACTED***` secrets.
- `/metrics` gains `parqtel_alert_notifications_total{receiver,channel,status}`,
  `parqtel_alert_notification_duration_seconds{receiver,channel}` and
  `parqtel_alert_active_groups`.

### Fixed

- **Smaller block-index sidecar.** The shared label-value dictionary moved to a
  sibling `label_values.json`, so `index.json` no longer rewrites it on every
  persist or carries it through every index load. Legacy inline dictionaries
  still load and migrate on first start; autocomplete reads the sibling.

- **Compaction selects a time-adjacent run.** Small blocks are sorted by start
  timestamp and filtered to one signal before the merge cap is applied, so
  compacted outputs are contiguous and row-group pruning stays effective.
- **Instant-query lookback is overridable.** `GET /api/v1/query` accepts
  `lookback_delta` (duration or float seconds, positive, max 30d); the default
  remains the Prometheus 5m. Documented in the OpenAPI spec.

- **Helm chart refuses a multi-writer deployment.** `replicaCount` is capped at
  1 in `values.schema.json` and the Deployment template fails if `replicaCount`
  or `autoscaling.maxReplicas` would create more than one pod. Each replica owns
  a disjoint local data directory; shared storage is not implemented. CI values
  and deployment docs now use a single replica.

- **One alert delivery path.** When `[alerting.routing]` is configured it now
  wins; the legacy `[alerts.notifications.routes]` table is a fallback only.
  Configuring both logs a warning and disables the legacy routes, so an event
  is never delivered twice. Removed the dead `EvaluationEngine::run` /
  `evaluate_all` / `evaluate_rule` stub; the live query loop remains in the
  server tasks.
- **A capped query could return different series after every restart.** The
  label-set fingerprint used a randomly seeded hasher although it is documented
  as stable, and series are ordered by fingerprint before the series limit is
  applied. It is now a fixed function of the labels (value pinned in a test).
- **OTLP/HTTP protobuf traces** were rejected with 400 on `/v1/traces`; only gRPC
  could ingest protobuf traces. The endpoint now negotiates on `Content-Type`
  like metrics and logs.
- **Crash recovery lost data.** Replay numbered WAL records by where they started
  while commit points are where they end, so the record after the committed one
  was skipped; and a sharded flush could commit a position past a record that
  belonged to the next block. After `kill -9` under load, every acknowledged
  metric, log and span is now recovered.
- **Concurrent flushes committed out of order**, which could move the WAL commit
  point past a block that was not yet on disk. Commits are released in flush
  order.
- **Recovery no longer duplicates rows.** A crash in the few milliseconds after a
  block became durable and before its WAL range was committed replayed that
  range into a second block. Each block now records the WAL span `(from, to]` it
  covers in its Parquet footer and in the index, and startup advances the commit
  point along the chain of blocks on disk before replaying. It follows the chain
  rather than taking the largest `to`, so a later block that finished first can
  never cause an earlier, unwritten one to be skipped. (A block closed early by
  the writer on capacity, rather than by a flush, carries no span.)
- **Fresh data vanished from queries after each flush.** The memory buffer was
  emptied completely when a block became durable, including rows that arrived
  while it was being written. Only the rows the block holds are removed now.
- **SIGTERM could leave the process running** after "Shutting down gracefully":
  a background task kept the ingestion services alive. Shutdown also bounds its
  waits on the index tasks.

### Documentation

- **Comparative benchmark harness.** `scripts/bench/compare_queries.py` runs a
  per-engine query set with warmup/runs and emits p50/min/max JSON plus host
  shape; `docs/performance/comparison.md` records the method and forbids
  comparing unlike runs.
- **Edge/single-node mode documented as a product shape.** Topology,
  recommended buffer/series settings, operating checks, and explicit non-goals
  (no replication, object storage, tenants, SQL, or dashboard builder).
- **Architecture decisions recorded.** ADR 0001 defines the `BlockStore` seam
  and versioned manifest needed for object storage (explicitly not
  `StorageEngine`); ADR 0002 makes the SQL choice a measured DataFusion-vs-
  hand-rolled spike with go/no-go criteria.
- **Agent on-ramps.** `examples/agents/` has tested-shape configurations for OTel
  Collector (gRPC/HTTP), Vector, Fluent Bit, and a Filebeat→Vector path, with
  the exact endpoint and encoding each one uses.
- **Anomaly and noise claims match the binary.** The README, MCP guide,
  configuration reference, OpenAPI schema, and preset tuning notes now state
  that anomaly detection/noise scoring are not implemented in 0.3.x;
  `LearnedAnomaly` is documented as source metadata over the threshold
  condition, and `noise_score` as reserved/always `0.0`. The alerting feature
  row now also lists the routing, inhibition, grouping, silence and receiver
  capabilities that do exist.
- **Architecture diagrams** (`docs/development/diagrams.md`): ten Mermaid
  diagrams covering the system context, the crate graph, the ingest and query
  paths, the block lifecycle, the on-disk layout, admission control, the alert
  path and the three query surfaces.
- The architecture, configuration, deployment, troubleshooting, sizing,
  glossary, FAQ and best-practices documents were brought up to date with the
  L0/L1 block levels, the process-wide scan budget, the shared label-value
  dictionary, the jemalloc allocator and the current MSRV (1.88).
- The OpenAPI description now covers every registered route, including the
  pipeline search endpoint, saved searches, alert routes and silences.
- `make docs-check` validates relative links and anchors, the repository paths
  quoted in the docs, the images the docs embed, and that the OpenAPI spec and
  the Axum router describe the same endpoints. It runs as a CI job.
- **Animated architecture diagrams.** The diagram page and the README now embed
  self-contained SVG with the motion system in `docs/assets/diagrams/`: dashes
  travel along every edge, nodes breathe in a wave, subgraph frames lift, and
  sequence lifelines pulse. Declarative only (no JavaScript, no external
  assets), it animates inside an `<img>` on GitHub and stops under
  `prefers-reduced-motion: reduce`. Mermaid sources stay in
  `docs/assets/diagrams/src/` and `scripts/diagrams/build.sh` regenerates the
  SVGs.

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
[docs/operations/configuration.md](docs/operations/configuration.md).

### Security

- Dropped vulnerable `protobuf` 2.x and `quick-xml` from the pprof feature
  set.

### Documentation

- [Argo Rollouts integration guide](docs/guides/argo-rollouts.md) with runnable
  AnalysisTemplate and canary Rollout examples.
- [PQL guide](docs/guides/pql-guide.md).
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
[docs/performance/README.md](docs/performance/README.md).

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
[docs/operations/configuration.md](docs/operations/configuration.md).

### Observability

Added `parqtel_ingest_lock_wait_seconds`, `parqtel_flush_duration_seconds`,
`parqtel_flush_inflight`, `parqtel_flush_rows_total`,
`parqtel_index_lock_wait_seconds`, `parqtel_index_sidecar_bytes`,
`parqtel_index_pending_writes` and `parqtel_ingest_rotator_shards`. `/metrics`
no longer reads `/proc`.

### Known limitations

Collected in [docs/operations/known-issues.md](docs/operations/known-issues.md), with a workaround for
each. They are recorded deliberately rather than left to be discovered.

- The in-memory buffer is unbounded — there is no max size, no byte accounting
  and no eviction. Resident memory is a function of ingest rate ×
  `storage.block_duration_secs` with no ceiling. Size the container from the
  formula in `docs/operations/performance-sizing.md` until this is bounded.
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
