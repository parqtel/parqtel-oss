# Configuration Reference

Parqtel uses [Figment](https://github.com/SergioBenitez/Figment) for layered configuration. Settings are resolved in this priority order (highest wins):

1. **CLI flags** — `--bind`, `--data-dir`, `--config`, `--log-level`
2. **Environment variables** — prefixed with `PARQTEL__`, using `__` as section separator
3. **TOML config file** — optional; specified via `--config` / `PARQTEL_CONFIG`, or auto-detected at `config/default.toml` if present (not shipped by default)
4. **Built-in defaults** — hardcoded in the source

## TOML Configuration File

```toml
[server]
bind_address = "0.0.0.0:8080"
grpc_bind_address = "0.0.0.0:4317"   # OTLP gRPC; "" disables
max_connections = 1024
shutdown_timeout_secs = 30
flush_interval_secs = 5
alert_interval_secs = 15
retention_interval_secs = 3600
grpc_concurrency_limit = 64
index_persist_interval_secs = 2

[storage]
backend = "parquet"
data_dir = "data"
block_duration_secs = 7200        # 2 hours
max_rows_per_block = 1000000
compression = "zstd"              # zstd | snappy | lz4 | none
compression_level = 1             # optional zstd level (1-22); unset = codec default
retention_days = 7
compaction_interval_secs = 300    # 5 minutes
row_group_size = 100000
compaction_max_merge_blocks = 12  # max source blocks merged in one pass
compaction_max_merges_per_pass = 8 # max merges one cycle performs

[logs]
data_dir = "data/logs"
block_duration_secs = 1800        # 30 minutes
max_rows_per_block = 200000
compression = "zstd"
compression_level = 1             # optional zstd level (1-22); unset = codec default
retention_days = 3
compaction_interval_secs = 3600
row_group_size = 20000

[ingest]
max_body_size = 10485760          # 10 MB
wal_enabled = true                # metrics WAL; also gates the background flush worker
log_wal_enabled = true
wal_sync_mode = "interval"        # none | interval | always
wal_sync_interval_ms = 1000       # fsync period in "interval" mode
wal_max_segment_bytes = 67108864  # WAL segment roll size
max_inflight_flushes = 4          # concurrent block encodes
rotator_shards = 4                # metrics writer shards (1-256)
label_interner_capacity = 1048576 # distinct label sets the interner holds
[ingest.tail_sampling]
keep_errors = true                   # always keep traces with ERROR spans
# slow_trace_ms = 1000               # keep traces slower than this (ms)
sampling_ratio = 1.0                 # fraction of remaining traces kept

# Per-service overrides fully replace the global policy for that service:
# [ingest.tail_sampling.per_service.noisy-service]
# keep_errors = false
# sampling_ratio = 0.05

[query]
max_series = 1000
max_samples_per_series = 10000
timeout_secs = 30
lookback_delta_ns = 300000000000   # 5 minutes (Prometheus default)

[ui]
enabled = true

[telemetry]
log_level = "info"
log_format = "text"               # text | json
# Self-observability: export Parqtel's own traces + SLI metrics over OTLP/gRPC.
otlp_enabled = false
otlp_endpoint = "http://127.0.0.1:4317"  # http:// = plaintext gRPC; https:// = TLS
otlp_trace_level = "info"          # span level exported; independent of log_level
export_interval_secs = 30          # SLI metric push interval
profiling_enabled = false          # /debug/pprof/* endpoints; 404 while disabled
profiling_frequency = 99           # CPU sampling frequency (Hz)

[alerts]
rules_dir = "rules"
noise_window_firings = 30
refinement_enabled = true
noise_suppression_threshold = 0.7
[alerts.presets]
# Built-in alert packs embedded in the binary (see rules/presets/README.md).
# auto = activate a pack when its canary metrics are detected (default)
# all  = activate every selected pack at startup, regardless of metrics
# off  = never load built-in packs
mode = "auto"
# Include-only pack keys (wins over exclude when both are set):
# include = ["kubernetes", "postgres"]
# Or exclude specific packs (applied when include is empty):
# exclude = ["service-red"]
include = []
exclude = []
[alerts.postmortem]
enabled = true
auto_draft = true
postmortem_delay_minutes = 5
template_path = ""
auto_publish_notion = false
auto_publish_gdocs = false
auto_publish_slack = true
auto_create_jira_actions = false
min_duration_minutes = 5
min_severity = "warning"
max_knowledge_entries = 1000

[alerts.notifications]
dedup_flush_interval_secs = 300
max_concurrent_sends = 10
send_timeout_secs = 30
self_monitoring_failure_threshold = 0.5

[alerts.notifications.egress]
allow_private = false
allow_hosts = ["*.internal.example"]
# [[alerts.notifications.routes]]
# name = "slack-critical"
# match_severity = "critical"
# match_labels = { team = "platform" }
# webhook_url = "https://hooks.slack.com/services/..."
# repeat_minutes = 240

# Alertmanager-compatible routing, grouping, inhibition and native receivers.
# Secrets can be injected as "${ENV_VAR}" or "{env:ENV_VAR}".
[alerting.routing.global]
resolve_timeout = "5m"
http_timeout = "10s"
retry_initial_backoff = "1s"
retry_max_backoff = "30s"
retry_max_attempts = 3

[alerting.routing]
max_active_groups = 10000

[alerting.routing.egress]
allow_private = false

[alerting.routing.route]
receiver = "slack-default"
group_by = ["service.name"]
group_wait = "30s"
group_interval = "5m"
repeat_interval = "4h"

[[alerting.routing.route.routes]]
matchers = ["severity = critical"]
receiver = "pagerduty-oncall"
continue = true

[[alerting.routing.inhibit_rules]]
source_matchers = ["severity = critical"]
target_matchers = ["severity = warning"]
equal = ["service.name"]

[[alerting.routing.receivers]]
name = "slack-default"
[[alerting.routing.receivers.slack_configs]]
webhook_url = "${SLACK_WEBHOOK_URL}"
send_resolved = true

[[alerting.routing.receivers]]
name = "pagerduty-oncall"
[[alerting.routing.receivers.pagerduty_configs]]
routing_key = "${PAGERDUTY_ROUTING_KEY}"
send_resolved = true

[k8s_provider]
enabled = false
bind_address = "0.0.0.0:6443"
cache_expiry_secs = 30
query_timeout_secs = 10
max_concurrent = 10
tls_secret_name = "parqtel-provider-tls"
```

## Section Reference

### `[server]`

| Key | Type | Default | Description |
|-----|------|---------|-------------|
| `bind_address` | String | `"0.0.0.0:8080"` | TCP address to listen on |
| `grpc_bind_address` | String | `"0.0.0.0:4317"` | TCP address for OTLP gRPC; empty string disables |
| `max_connections` | Integer | `1024` | Maximum simultaneous TCP connections |
| `shutdown_timeout_secs` | Integer | `30` | Seconds to wait for in-flight requests during graceful shutdown |
| `flush_interval_secs` | Integer | `5` | Background tick that checks whether a block has reached its duration. Bounds how *late* a duration-triggered flush can be, not how often blocks are written |
| `alert_interval_secs` | Integer | `15` | Seconds between alert-rule evaluation cycles |
| `retention_interval_secs` | Integer | `3600` | Seconds between retention sweeps. Lower it if the sweep visibly stalls endpoints; raise it on very large indexes |
| `grpc_concurrency_limit` | Integer | `64` | Maximum concurrent OTLP exports per gRPC connection. Without it a single client can monopolise the ingest mutex that HTTP exporters also queue on |
| `index_persist_interval_secs` | Integer | `2` | How often the block-index sidecar is written. Mutations mark the index dirty and are batched into one write per interval, so this bounds how long a block can sit in memory before the sidecar catches up. Lower it if you restart often; raise it to cut filesystem writes on write-heavy ingest |

### `[storage]`

| Key | Type | Default | Description |
|-----|------|---------|-------------|
| `backend` | String | `"parquet"` | Storage backend (currently only `"parquet"`) |
| `data_dir` | Path | `"data"` | Directory for metric block files |
| `block_duration_secs` | Integer | `7200` | Time span of a single block (seconds) |
| `max_rows_per_block` | Integer | `1000000` | Maximum rows before forced rotation |
| `compression` | String | `"zstd"` | Parquet compression: `zstd`, `snappy`, `lz4`, `none` |
| `compression_level` | Integer | unset | Compression level for `zstd` (`1`–`22`). Unset uses the codec default. Lower levels encode faster and compress less; use a low level for frequently rewritten blocks |
| `retention_days` | Integer | `7` | Days to retain metric data |
| `compaction_interval_secs` | Integer | `300` | Interval between compaction passes. Each pass merges small log blocks, rolls metric blocks up into 5-minute L1 windows, and runs the tiered log merge. Lower it to roll up sooner; raise it on a store where each pass is expensive |
| `row_group_size` | Integer | `100000` | Rows per Parquet row group. Must be ≤ `max_rows_per_block` — this is what row-group pruning depends on, so a too-large value silently costs query time |
| `compaction_max_merge_blocks` | Integer | `12` | Max source blocks merged into one compacted block. Too few and the small-block population grows faster than compaction removes it |
| `compaction_max_merges_per_pass` | Integer | `8` | Max merges one compaction cycle performs. Bounds the disk I/O a single cycle can take |

**What a compaction pass does.** Every `compaction_interval_secs` the background
loop runs three bounded passes: small log blocks are merged, metric blocks are
rolled up into 5-minute **L1** windows by a streaming merge, and old log blocks
are merged within a 6-hour tier. Metric block files are named by level, so the
layout is readable from the filesystem alone:

| Prefix | Meaning |
|--------|---------|
| `m_l0_` | One ingest flush, series-sorted by the final query-visible labels |
| `m_n_` | A legacy block normalised into the same order |
| `m_l1_` | A coalesced 5-minute window |

The L1 pass is bounded (8 sources, 2 groups, 16 M rows per pass) and reserves
2 of the 4 process-wide scan slots, so it never monopolises the disk. Run
`parqtel compact` to trigger the same pass offline.

### `[logs]`

| Key | Type | Default | Description |
|-----|------|---------|-------------|
| `data_dir` | Path | `"data/logs"` | Directory for log block files |
| `block_duration_secs` | Integer | `1800` | Time span of a single log block |
| `max_rows_per_block` | Integer | `200000` | Maximum rows before forced rotation |
| `compression` | String | `"zstd"` | Parquet compression codec |
| `compression_level` | Integer | unset | Compression level for `zstd` (`1`–`22`). Unset uses the codec default |
| `retention_days` | Integer | `3` | Days to retain log data |
| `compaction_interval_secs` | Integer | `3600` | Interval between compaction passes |
| `row_group_size` | Integer | `20000` | Rows per Parquet row group. Must be ≤ `max_rows_per_block` |
| `compaction_max_merge_blocks` | Integer | `12` | Max source blocks merged into one compacted block |
| `compaction_max_merges_per_pass` | Integer | `8` | Max merges one compaction cycle performs |

### `[ingest]`

| Key | Type | Default | Description |
|-----|------|---------|-------------|
| `max_body_size` | Integer | `10485760` | Maximum request body size in bytes (10 MB) |
| `wal_enabled` | Boolean | `true` | Write-ahead log for metrics. Bounds crash loss to the sync interval instead of the whole block window. A WAL that cannot be opened **fails startup** rather than silently accepting unrecoverable data. **Also gates the background flush worker**: the worker acknowledges a request once the WAL has its rows rather than once the block is on disk, which is only sound with a WAL. Setting this `false` disables the worker too and flushing falls back to synchronous — still acknowledged only once the block is durable |
| `max_inflight_flushes` | Integer | `4` | Max block encodes in flight for the background flush worker, which also bounds its queue. When the queue is full a flush encodes inline, so backpressure degrades to synchronous rather than growing a backlog. Requires `wal_enabled` |
| `wal_sync_mode` | String | `interval` | `none` (write only — survives a *process* crash, not a machine loss), `interval` (periodic `fsync`, the default), `always` (fsync per batch) |
| `wal_sync_interval_ms` | Integer | `1000` | How often the WAL is `fsync`ed in `interval` mode |
| `wal_max_segment_bytes` | Integer | `67108864` | Size at which the WAL rolls to a new segment. Segments are deleted once the commit point passes them, so the on-disk WAL stays bounded |
| `label_interner_capacity` | Integer | `1048576` | Distinct label sets the process-wide interner holds before it starts emptying shards. Interning lets every point of a series share one copy of its labels; past this capacity sharing is lost and memory grows with points instead of series. Set it to at least **twice the active series count**, because each series is interned twice (as received, and with `service.name` for the memory buffer). The table only holds sets that were actually seen, so a high value costs nothing on a small deployment |
| `rotator_shards` | Integer | `4` | Writer shards for the metrics ingest rotator. Every ingest request serialises on one lock per signal and a block flush runs while that lock is held, so one flush stalls every metric request for its encode duration. Sharding lets unrelated metrics be ingested concurrently during a flush. **The shards are not independent blocks** — a flush merges them into one file, so block count and query fan-out are unchanged, and durability is unchanged. Clamped to 1–256. Raise only where `parqtel_ingest_lock_wait_seconds` shows real contention; it costs one writer buffer per shard |
| `log_wal_enabled` | Boolean | `true` | Enable write-ahead log for logs |
| `buffer_max_points` | Integer | `5000000` | Max buffered rows across metrics, logs, and traces (`0` disables). The writer and WAL hold the same rows, so eviction shortens the pre-flush query window, not durability |
| `buffer_max_bytes` | Integer | `2147483648` | Approximate max buffered bytes (`0` disables). Counts record structure plus body and label bytes, not allocator overhead |
| `buffer_overflow_policy` | String | `drop_oldest` | `drop_oldest` evicts the oldest buffered rows; `reject_new` refuses the incoming batch. Both increment `parqtel_buffer_dropped_total{signal,reason}` |
| `max_concurrent_requests` | Integer | `64` | Simultaneous OTLP/HTTP ingest requests. When full, ingest returns `429` with `Retry-After` instead of queueing unbounded work. gRPC has its own `server.grpc_concurrency_limit` |
| `load_shed_retry_after_secs` | Integer | `1` | `Retry-After` value on an ingest `429` |

Buffer occupancy and drops are exported at `/metrics` as
`parqtel_buffer_points{signal}`, `parqtel_buffer_bytes`,
`parqtel_buffer_limit_points`, `parqtel_buffer_limit_bytes`, and
`parqtel_buffer_dropped_total{signal,reason}`. Rejected ingest requests are
counted by `parqtel_ingest_rejected_total`.

The query timeout (`query.timeout_secs`) applies only to scan endpoints
(`/api/v1/query*`, labels, log/trace search, correlate, pipeline search, and
Grafana `/query`/`/search`). Ingest is never wrapped in the query timeout.

### `[ingest.tail_sampling]`

| Key | Type | Default | Description |
|-----|------|---------|-------------|
| `keep_errors` | Boolean | `true` | Keep traces containing ERROR-status spans |
| `slow_trace_ms` | Integer (optional) | `null` | Keep traces whose server spans exceed this duration (ms) |
| `sampling_ratio` | Float | `1.0` | Fraction of remaining traces to keep (0.0–1.0) |

### `[ingest.tail_sampling.per_service]`

Per-service overrides; each entry fully replaces the global policy for that service.

### `[query]`

| Key | Type | Default | Description |
|-----|------|---------|-------------|
| `max_series` | Integer | `1000` | Maximum time series returned per query |
| `max_samples_per_series` | Integer | `10000` | Maximum samples per series |
| `timeout_secs` | Integer | `30` | Query execution timeout |
| `lookback_delta_ns` | Integer | `300000000000` | Instant-query lookback window in nanoseconds (5 minutes default) |

### `[ui]`

| Key | Type | Default | Description |
|-----|------|---------|-------------|
| `enabled` | Boolean | `true` | Enable the built-in web UI at `/ui` |

### `[telemetry]`

| Key | Type | Default | Description |
|-----|------|---------|-------------|
| `log_level` | String | `"info"` | Log level filter (trace, debug, info, warn, error) |
| `log_format` | String | `"text"` | Log output format: `text` or `json` |
| `otlp_enabled` | Boolean | `false` | Export Parqtel's own traces + SLI metrics over OTLP/gRPC |
| `otlp_endpoint` | String | `"http://127.0.0.1:4317"` | OTLP/gRPC collector endpoint (`http://` = plaintext, `https://` = TLS) |
| `otlp_trace_level` | String | `"info"` | Minimum span level exported over OTLP — independent of `log_level`, so trace export survives a raised console level |
| `export_interval_secs` | Integer | `30` | SLI metric push interval (seconds) |
| `profiling_enabled` | Boolean | `false` | Enable the `/debug/pprof/{profile,summary,memory}` endpoints (they 404 while disabled) |
| `profiling_frequency` | Integer | `99` | CPU sampling frequency in Hz (1–1000) |

> **Operational note**: when `otlp_enabled = true` but the OTLP SDK fails to initialise (e.g. an unreachable collector), the server logs a warning and continues with console logs only — ingestion is never blocked by self-telemetry.

### `[alerts]`

| Key | Type | Default | Description |
|-----|------|---------|-------------|
| `rules_dir` | String | `"rules"` | Directory containing alert rule YAML files |
| `noise_window_firings` | Integer | `30` | Number of firings in window to calculate noise score |
| `refinement_enabled` | Boolean | `true` | Enable automatic alert refinement |
| `noise_suppression_threshold` | Float | `0.7` | Threshold for noise suppression (0.0-1.0) |
| `allow_runtime_updates` | Boolean | `false` | Permit REST writes to alert routes, recording rules, and pipelines. Keep off unless an authenticating proxy fronts the API. |
| `recording_rules_enabled` | Boolean | `true` | Evaluate `rules/recording/` groups on their intervals and write results through the metrics ingest path |

### `[alerts.presets]`

Built-in alert packs embedded in the binary (`rules/presets/*.yaml`). Packs activate automatically when
their canary metrics appear in the metric index/buffer, or explicitly via the
selection lists below. Once activated, a pack never deactivates, and rules in
`rules_dir` (or created via the API) with the same id always override the
built-in.

| Key | Type | Default | Description |
|-----|------|---------|-------------|
| `mode` | String | `"auto"` | Activation mode: `auto` (activate on canary metric detection), `all` (activate all selected packs at startup), `off` (never load built-ins) |
| `include` | String list or comma-separated string | `[]` | Include-only list of pack keys; wins over `exclude` when both are set (warning logged). Unknown keys are logged and ignored |
| `exclude` | String list or comma-separated string | `[]` | Pack keys to skip; applied only when `include` is empty |

Pack keys: `kubernetes`, `coredns`, `external-secrets`, `service-red`,
`parqtel` (more packs land with the remaining plan phases).

Env override examples:

```bash
PARQTEL__ALERTS__PRESETS__MODE=auto
PARQTEL__ALERTS__PRESETS__INCLUDE=kubernetes,parqtel   # include-only
PARQTEL__ALERTS__PRESETS__EXCLUDE=service-red          # exclude
```

### `[alerts.postmortem]`

| Key | Type | Default | Description |
|-----|------|---------|-------------|
| `enabled` | Boolean | `true` | Enable the postmortem engine |
| `auto_draft` | Boolean | `true` | Auto-generate drafts when alerts resolve |
| `postmortem_delay_minutes` | Integer | `5` | Minutes to wait after resolution before generating draft |
| `template_path` | String (optional) | `""` | Path to custom template directory |
| `auto_publish_notion` | Boolean | `false` | Auto-publish to Notion |
| `auto_publish_gdocs` | Boolean | `false` | Auto-publish to Google Docs |
| `auto_publish_slack` | Boolean | `true` | Auto-publish to Slack |
| `auto_create_jira_actions` | Boolean | `false` | Auto-create Jira action items |
| `min_duration_minutes` | Integer | `5` | Minimum incident duration to trigger postmortem |
| `min_severity` | String | `"warning"` | Minimum severity level to trigger postmortem |
| `max_knowledge_entries` | Integer | `1000` | Maximum entries in knowledge base |

### `[alerts.notifications]`

| Key | Type | Default | Description |
|-----|------|---------|-------------|
| `dedup_flush_interval_secs` | Integer | `300` | Deduplication flush interval |
| `max_concurrent_sends` | Integer | `10` | Max concurrent webhook sends |
| `send_timeout_secs` | Integer | `30` | Webhook send timeout |
| `self_monitoring_failure_threshold` | Float | `0.5` | Self-monitoring failure threshold |
| `routes` | Array | `[]` | Alert routing rules (see RouteConfig) |
| `egress` | Table | fail-closed | Outbound destination policy (see below) |

### `[alerts.notifications.egress]`

Every webhook destination — legacy routes and the Alertmanager receivers — is
checked immediately before delivery. The default rejects loopback, private,
link-local, CGNAT, ULA, multicast, and reserved addresses, and disables HTTP
redirects. A receiver that needs a private address must opt in explicitly.

| Key | Type | Default | Description |
|-----|------|---------|-------------|
| `allow_private` | Boolean | `false` | Permit loopback/private/link-local/CGNAT/ULA destinations |
| `allow_hosts` | Array | `[]` | Exact hosts or `*.suffix` patterns that bypass the address check |

```toml
[alerts.notifications.egress]
allow_private = false
allow_hosts = ["*.internal.example"]
```

### `[alerts.notifications.routes]` (array of tables)

| Key | Type | Default | Description |
|-----|------|---------|-------------|
| `name` | String | required | Route name |
| `match_severity` | String | `"info"` | Minimum severity: critical, page, warning, info |
| `match_labels` | Map | `{}` | Exact label matches required |
| `webhook_url` | String | required | Webhook URL for delivery |
| `repeat_minutes` | Integer | `240` | Re-notification interval (0 = no repeat) |

### `[alerting.routing]`

Alertmanager-compatible routing. When set, firing **and** resolved alerts flow
through grouping, inhibition, silences, and the native receivers below. The
legacy `[alerts.notifications.routes]` webhook table keeps working alongside it.
Invalid matchers, bad durations, or a `receiver` that names no configured
receiver fail validation at startup.

| Key | Type | Default | Description |
|-----|------|---------|-------------|
| `max_active_groups` | Integer | `10000` | Bound on in-memory alert groups; oldest resolved/idle groups are evicted first |
| `global` | Table | — | `resolve_timeout` `5m`, `http_timeout` `10s`, `retry_initial_backoff` `1s`, `retry_max_backoff` `30s`, `retry_max_attempts` `3` |
| `route` | Table | none | Root route (see below) |
| `inhibit_rules` | Array | `[]` | `source_matchers`, `target_matchers`, `equal` |
| `receivers` | Array | `[]` | Named receivers with `slack_configs`, `pagerduty_configs`, `teams_configs`, `webhook_configs` |
| `egress` | Table | fail-closed | Same `allow_private` / `allow_hosts` policy as `[alerts.notifications.egress]` |

`[alerting.routing.egress]` applies to every receiver built by this dispatcher.
The legacy route table and the dispatcher each carry their own policy; set both
when both are configured.

Durations accept `ms`, `s`, `m`, `h`, `d`, `w` and compound forms (`1h30m`).
Matchers use `=`, `!=`, `=~` (anchored regex), `!~`; a missing label compares as
`""`. Any value may be `${ENV_VAR}` or `{env:ENV_VAR}`.

### `[alerting.routing.route]`

| Key | Type | Default | Description |
|-----|------|---------|-------------|
| `matchers` | Array | `[]` | Alertmanager matchers; empty matches everything |
| `continue` | Boolean | `false` | Keep evaluating sibling routes after a match (fan-out) |
| `group_by` | Array | inherits | Labels to group by; `["..."]` disables grouping |
| `group_wait` | Duration | `30s` | First-notification buffer for a new group |
| `group_interval` | Duration | `5m` | Minimum gap when new alerts join an active group |
| `repeat_interval` | Duration | `4h` | Re-notify an unchanged firing group |
| `receiver` | String | inherits | Receiver for matched alerts |
| `routes` | Array | `[]` | Child routes, evaluated depth-first |

`slack_configs` take `webhook_url`, `channel`, `username`, `icon_emoji`,
`send_resolved`; `pagerduty_configs` take `routing_key`, `url`, `severity`,
`send_resolved`; `teams_configs` take `webhook_url`, `title`, `send_resolved`;
`webhook_configs` take `url`, `headers`, `send_resolved`.

### `[k8s_provider]`

| Key | Type | Default | Description |
|-----|------|---------|-------------|
| `enabled` | Boolean | `false` | Enable Kubernetes custom metrics API server |
| `bind_address` | String | `"0.0.0.0:6443"` | Address for the K8s API service |
| `cache_expiry_secs` | Integer | `30` | Cache TTL for metric values |
| `query_timeout_secs` | Integer | `10` | Query timeout to parqtel storage |
| `max_concurrent` | Integer | `10` | Max concurrent queries |
| `tls_secret_name` | String | `"parqtel-provider-tls"` | TLS certificate secret name |

## Environment Variables

Environment variables use the `PARQTEL__` prefix with `__` as the section separator. Examples:

```bash
# Server
export PARQTEL__SERVER__BIND_ADDRESS="0.0.0.0:9090"
export PARQTEL__SERVER__MAX_CONNECTIONS=2048
export PARQTEL__SERVER__GRPC_BIND_ADDRESS="0.0.0.0:4317"
export PARQTEL__SERVER__FLUSH_INTERVAL_SECS=10
export PARQTEL__SERVER__RETENTION_INTERVAL_SECS=1800
export PARQTEL__SERVER__INDEX_PERSIST_INTERVAL_SECS=5

# Storage
export PARQTEL__STORAGE__DATA_DIR="/var/lib/parqtel/data"
export PARQTEL__STORAGE__COMPRESSION="zstd"
export PARQTEL__STORAGE__COMPRESSION_LEVEL=1
export PARQTEL__STORAGE__RETENTION_DAYS=14
export PARQTEL__STORAGE__BLOCK_DURATION_SECS=3600
# Row-group pruning only works when the row group fits inside a block.
export PARQTEL__STORAGE__ROW_GROUP_SIZE=50000
export PARQTEL__STORAGE__COMPACTION_INTERVAL_SECS=300

# Logs
export PARQTEL__LOGS__DATA_DIR="/var/lib/parqtel/logs"
export PARQTEL__LOGS__RETENTION_DAYS=7
export PARQTEL__LOGS__BLOCK_DURATION_SECS=1800

# Ingest
export PARQTEL__INGEST__MAX_BODY_SIZE=20971520
export PARQTEL__INGEST__ROTATOR_SHARDS=8
export PARQTEL__INGEST__WAL_ENABLED=true
export PARQTEL__INGEST__LOG_WAL_ENABLED=true
export PARQTEL__INGEST__LABEL_INTERNER_CAPACITY=2097152
export PARQTEL__INGEST__TAIL_SAMPLING__KEEP_ERRORS=true
export PARQTEL__INGEST__TAIL_SAMPLING__SAMPLING_RATIO=0.5

# Query
export PARQTEL__QUERY__TIMEOUT_SECS=60
export PARQTEL__QUERY__MAX_SERIES=5000
export PARQTEL__QUERY__LOOKBACK_DELTA_NS=300000000000

# Telemetry
export PARQTEL__TELEMETRY__LOG_LEVEL="debug"
export PARQTEL__TELEMETRY__LOG_FORMAT="json"

# Alerts
export PARQTEL__ALERTS__RULES_DIR="/etc/parqtel/rules"
export PARQTEL__ALERTS__NOISE_SUPPRESSION_THRESHOLD=0.8
export PARQTEL__ALERTS__POSTMORTEM__ENABLED=true
export PARQTEL__ALERTS__NOTIFICATIONS__MAX_CONCURRENT_SENDS=20

# K8s Provider
export PARQTEL__K8S_PROVIDER__ENABLED=true
export PARQTEL__K8S_PROVIDER__BIND_ADDRESS="0.0.0.0:6443"
```

## CLI Flags

```bash
parqtel [OPTIONS] [COMMAND]

Options:
  -c, --config <PATH>      Path to TOML configuration file [env: PARQTEL_CONFIG]
  -b, --bind <ADDRESS>     TCP bind address [env: PARQTEL_BIND]
  -d, --data-dir <PATH>    Data directory [env: PARQTEL_DATA_DIR]
      --log-level <LEVEL>  Log level override [env: RUST_LOG]
  -h, --help               Print help
  -V, --version            Print version

Commands:
  serve    Start the HTTP server (default)
  compact  Run one compaction pass and exit
  inspect  Print storage summary as JSON
  export   Export metric data to CSV
```

## Alert Rule YAML Schema

Alert rules are defined in YAML files under the `rules_dir` (one rule per
`---` document; files are loaded non-recursively at startup):

```yaml
id: http-error-rate-high          # unique, stable — used as the alert instance prefix
name: HTTP error rate high
signal: metrics                   # metrics | logs | traces (default: metrics)
query: "rate(http_errors_total[5m])"   # PromQL selector + optional aggregation
condition:
  type: threshold                 # only "threshold" is implemented
  operator: ">"                  # > >= < <= == !=
  value: 0.5                     # errors/sec — see tuning note below
  for_duration_secs: 300         # must hold this long before Pending → Firing
severity: critical               # critical | warning | info
labels:
  team: platform
annotations:
  summary: "HTTP error rate above 0.5/sec"
  description: "Tune to your traffic profile."
  runbook: "https://wiki.example.com/runbooks/http-errors"
enabled: true
noise_suppression_threshold: 0.7  # 0.0-1.0, higher = more suppression
```

The evaluation loop parses each enabled rule's `query` every 15s, executes it
over a trailing 5-minute window, and thresholds the **last sample of every
result series** — one alert instance per series, fingerprinted by `id` + the
series' labels. Two consequences for the queries you can write:

1. **No cross-metric arithmetic** — a rule query is a single selector with an
   optional aggregation (`rate`, `increase`, `avg`/`sum`/`min`/`max`/`count`
   with `by`/`without`, `topk`/`bottomk`, `irate`, `delta`, `round`,
   `abs`/`ceil`/`floor`, `clamp_min`/`clamp_max`, `label_replace`). Ratios
   such as `errors / requests` cannot be expressed — threshold absolute rates
   or window counts instead.
2. **`histogram_quantile` is unavailable in alert rules** — the alert loop
   uses the query plan path, not the AST engine that powers interactive
   queries. Use `_sum` rate proxies (e.g. seconds-of-flush per second) for
   latency-style rules.

**Presets.** `rules/presets/` ships ready-made packs for Kubernetes cluster
health, CoreDNS, external-secrets, service RED health, and Parqtel
self-monitoring — see [its README](../../rules/presets/README.md). The presets
directory is not scanned by default; copy the packs you want into `rules_dir`.

## Recording Rule YAML Schema

Files under `rules/recording/` contain a `groups:` list. Each group is
evaluated on its `interval`; every rule runs its PromQL expression over the
configured instant lookback and writes the resulting series under `record`
through the normal metrics ingest path (WAL, buffer, index). Set
`alerts.recording_rules_enabled = false` to disable the scheduler.

```yaml
groups:
  - name: http_service_aggregations
    interval: 1m
    rules:
      - record: service:http_requests:rate5m
        expr: sum by (service_name) (rate(http_requests_total[5m]))
        labels:
          aggregation: rate5m
        description: "Pre-aggregated request rate per service"
```

Invalid expressions are logged and skipped; a group's cadence is tracked in
`data/ruler/state.json` so a restart does not immediately re-run every group.

## Pipeline YAML Schema

Files under `rules/pipelines/` contain a `pipelines:` list. Each pipeline
matches log records (`match.signal: logs`) and runs stages in order. The
first matching pipeline wins; unmatched logs pass through unchanged.

```yaml
pipelines:
  - name: nginx-access-logs
    match:
      signal: logs
      conditions:
        - field: service_name
          op: "="
          value: nginx
    stages:
      - type: processor
        name: parse
        processor: regex_extract
        source_field: body
        pattern: '^(?P<ip>\S+) .* "(?P<method>\S+) (?P<path>\S+)'
        target_fields:
          http.method: method
          http.target: path
        on_parse_failure: keep_original
      - type: metric_extract
        name: requests
        metric_name: nginx_requests_total
        metric_type: counter
        value_field: "constant:1"
        dimensions: [http.method, http.target, service_name]
      - type: masker
        name: mask_ips
        rules:
          - field: body
            pattern: '\d{1,3}\.\d{1,3}\.\d{1,3}\.\d{1,3}'
            replacement: "[IP_MASKED]"
      - type: router
        name: drop_health
        conditions:
          - field: http.target
            op: "=~"
            value: '^/(health|ready|live)'
        action: drop
```

Stage types: `preprocessor`, `processor` (`regex_extract`, `add_fields`,
`cast`), `metric_extract` (`gauge`, `counter`, `histogram`), `masker`, and
`router` (`drop` or `route` + `destination`). Condition operators: `=`, `!=`,
`>`, `>=`, `<`, `<=`, `contains`, `=~`, `exists`, `in`, `between`. Regex
patterns are validated at load time; a trailing newline from a YAML block
scalar is stripped.

## Validation

Configuration is validated at startup. Invalid configurations produce clear error messages:

```
Error: server.bind_address cannot be empty; storage.compression must be one of: zstd, snappy, lz4, none
```