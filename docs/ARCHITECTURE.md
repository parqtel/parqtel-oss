# Architecture

This document describes the internal architecture of Parqtel, its crate boundaries, data flow, and key design decisions.

## High-Level Overview

Parqtel is a Rust workspace organized into focused crates that communicate through well-defined interfaces. The system follows a pipeline architecture: data enters through OTLP endpoints, flows through ingestion and optional pipeline processing, and lands in compressed Parquet blocks on the filesystem.

```
                    ┌─────────────────────────┐
                    │   OpenTelemetry SDKs    │
                    │  (metrics/logs/traces)  │
                    └───────────┬─────────────┘
                                │ OTLP/Proto or JSON
                                ▼
┌───────────────────────────────────────────────────────────────────┐
│                        parqtel-server                             │
│                                                                   │
│  ┌─────────────────────────────────────────────────────────────┐ │
│  │                    Axum Router                               │ │
│  │  /v1/metrics  /api/v1/query  /search  /api/v1/alerts  /ui  │ │
│  └──────┬──────────────┬──────────────┬──────────────┬─────────┘ │
│         │              │              │              │            │
│         ▼              ▼              ▼              ▼            │
│  ┌──────────┐   ┌──────────┐   ┌──────────┐   ┌──────────┐     │
│  │  Ingest  │   │  Query   │   │ Pipeline │   │  Alert   │     │
│  │ Handlers │   │ Handlers │   │ Handlers │   │ Handlers │     │
│  └──────────┘   └──────────┘   └──────────┘   └──────────┘     │
└───────────────────────────────────────────────────────────────────┘
         │              │              │              │
         ▼              ▼              ▼              ▼
┌──────────────┐ ┌──────────────┐ ┌──────────────┐ ┌──────────────┐
│parqtel-ingest│ │parqtel-query │ │parqtel-      │ │parqtel-alert │
│              │ │              │ │pipeline      │ │              │
│• OTLP decode│ │• PromQL parse│ │• Recording   │ │• Rule registry│
│• Block write │ │• Label match │ │  rules       │ │• Threshold   │
│• Rotation   │ │• Aggregation │ │• Stream proc │ │  evaluation  │
│• Crash-safe │ │• Plan exec   │ │• Metric      │ │• State machine│
│  flush      │ │              │ │  extraction  │ │• Alert store │
└──────┬───────┘ └──────┬───────┘ └──────┬───────┘ └──────┬───────┘
       │                │                │                │
       └────────────────┴────────────────┴────────────────┘
                                │
                                ▼
┌───────────────────────────────────────────────────────────────────┐
│                         parqtel-core                              │
│                                                                   │
│  ┌────────────┐  ┌────────────┐  ┌────────────┐  ┌────────────┐ │
│  │   Models   │  │  Storage   │  │   Engine   │  │   Config   │ │
│  │            │  │            │  │            │  │            │ │
│  │• Metric    │  │• BlockIndex│  │• Parquet   │  │• Figment   │ │
│  │• LogRecord │  │• Scanner   │  │  Engine    │  │• Layered   │ │
│  │• Span      │  │• Compactor │  │• Registry  │  │• Validated │ │
│  │• LabelSet  │  │• Retention │  │            │  │            │ │
│  └────────────┘  └─────┬──────┘  └────────────┘  └────────────┘ │
│                         │                                         │
└─────────────────────────┼─────────────────────────────────────────┘
                          ▼
                ┌───────────────────┐
                │   Filesystem      │
                │                   │
                │  data/            │
                │  ├── *.parquet    │  ← Metric blocks (Zstd)
                │  ├── index.json   │  ← Block index (JSON sidecar)
                │  ├── traces/      │
                │  │   ├── *.parquet│  ← Trace blocks
                │  │   └── index.json
                │  └── logs/        │
                │      ├── *.parquet│  ← Log blocks (Zstd)
                │      └── index.json
                └───────────────────┘
```

## Crate Dependency Graph

```
parqtel-server
├── parqtel-core
├── parqtel-ingest
│   └── parqtel-core
├── parqtel-query
│   └── parqtel-core
├── parqtel-alert
│   └── parqtel-core
└── parqtel-pipeline
    └── parqtel-core

parqtel-mcp-{slack,pagerduty,jira,notion,discord,gdocs,parqtel}
└── parqtel-mcp-core
```

## Data Flow

### Ingestion Path

1. **Transport** → OTLP payloads arrive via HTTP (`/v1/{metrics,logs,traces}` — content negotiation via `Content-Type` between protobuf and JSON) or via the dedicated **OTLP gRPC server on `:4317`** (tonic; implements all three collector services and routes through the identical `ingest_proto` decode path). gRPC is the OTel SDK default transport.
2. **Decode** → `parqtel-ingest::decode` converts OTLP payloads to internal `Metric`/`LogRecord`/`Span` models. JSON `attributes` must be OTLP arrays of `{key, value}` objects
3. **Buffer** → metrics, logs, and traces are all immediately queryable via `MemoryBuffer` (metrics indexed by name; spans matched by interval overlap); the buffer injects `service.name` from resource attributes into point labels so label matchers behave identically before and after flush, and drains on every flush so buffered + flushed rows never double-count
4. **Rotate** → `IngestionService` accumulates data points in memory via `BlockRotator`
4a. **Span-metrics RED bridge** → trace ingestion derives `traces_service_{requests,errors,duration_ms}_total` metrics from `SPAN_KIND_SERVER` spans (grouped by service/operation) and feeds them back through the metrics path — services get out-of-the-box RED metrics without hand-written rules. Trace decode merges resource attributes (service.name, k8s.*) into span attributes, span attrs winning on conflict.
4b. **Tail sampling** → after RED derivation (which sees the full span set, keeping metric rates accurate), traces are evaluated per trace against the `ingest.tail_sampling` policy: `keep_errors` (ERROR-status spans), `slow_trace_ms` (server spans above threshold), then a deterministic trace_id-hash probabilistic `sampling_ratio` (trace-coherent — whole traces live or die together). Per-service overrides replace the global policy. Dropped spans count in `dropped_spans` stats; default policy keeps everything at zero cost.
5. **Flush** → When block duration expires or row limit is reached, `BlockWriter` serializes to Parquet with configured compression. Blocking work (encode/compress/IO) runs on `spawn_blocking`; the rotator swaps out its writer so ingest continues during flushes
6. **Index** → `BlockMetadata` is sent via `mpsc::unbounded_channel` to the index task, which updates the in-memory `BlockIndex`
7. **Background** → A periodic flush task (5s interval) calls `check_and_flush()` on all three services

### Query Path

1. **HTTP Request** → Prometheus-compatible query parameters parsed
2. **Parse** → `parse_query()` extracts metric name, label matchers, aggregation, and time range
3. **Plan** → `QueryPlan` identifies which blocks to scan via `BlockIndex::query(start, end, metric_name)`
4. **Scan** → `Scanner::scan()` reads matching Parquet files on the blocking pool (bounded by a pre-acquired semaphore), applying time-range and metric-name filters at the columnar level with row-group statistics pruning. The scanner merges the dedicated `service_name` column back into point labels as `service.name`
5. **Merge** → in-memory buffer results are merged with block results
6. **Aggregate** → Aggregation functions (sum, avg, rate, histogram_quantile, etc.) are applied
7. **Response** → Results formatted as Prometheus JSON response

**Instant query semantics**: `/api/v1/query` evaluates over a 1-minute lookback window ending at `time`. Recent buffered data appears immediately; older data requires a `query_range` covering the flushed blocks.

### Alert Path

1. **Rule Loading** → `AlertRuleRegistry` loads YAML rules from `rules/` directory, watches for changes via `notify`
2. **Evaluation** → A background loop (15s interval) evaluates enabled rules against current data using the query executor
3. **State Machine** → Each alert instance transitions through: `Inactive → Pending → Firing → Resolved`
4. **Storage** → `AlertStore` persists alert instances with full transition history
5. **Notification** → Firing alerts can trigger MCP server actions (Slack, PagerDuty, etc.)

### Pipeline Path

1. **Rule Loading** → Recording rules and pipeline definitions loaded from YAML
2. **Evaluation** → `Ruler` evaluates PromQL/DQL expressions on a schedule with backfill support
3. **Processing** → Stream pipelines process incoming data through configurable stages (preprocessor → processor → metric extractor → router)
4. **Output** → Derived metrics are written back into the ingestion pipeline

## Storage Engine

### Block-Based Design

Parqtel stores data in time-bounded blocks. Each block is a self-contained Parquet file:

| Signal | Block Duration | Max Rows | Default Retention |
|--------|---------------|----------|-------------------|
| Metrics | 2 hours | 1,000,000 | 7 days |
| Logs | 30 minutes | 200,000 | 3 days |
| Traces | 2 hours (metrics config) | 1,000,000 | 7 days |

Trace blocks live in `data/traces/` with their own index sidecar, reusing the metrics `BlockConfig`.

### Parquet Schemas

Schemas are defined canonically in `parqtel-core/src/models/storage/schema.rs` and written with the `arrow`/`parquet` 59 stack (WriterProperties: PARQUET_2_0, configured codec).

**Metrics** (15 columns): `timestamp_ns` (Timestamp ns), `metric_name` (Dictionary<Int32, Utf8>), `metric_kind` (Utf8), dedicated correlation columns `service_name`, `service_version`, `k8s_namespace`, `k8s_pod_name`, `k8s_pod_uid`, `k8s_container_name`, `k8s_node_name` (all Dictionary, nullable), `resource_attributes` (Dictionary, JSON), `labels` (Utf8, JSON), and value columns `value_float` (F64), `value_int` (I64), `value_complex` (Utf8, JSON for histograms/summaries).

At write time `extract_correlation_labels` pulls `service.name`, `service.version`, and `k8s.*` out of resource attributes into their dedicated dictionary columns (fast equality filtering); everything else stays in the `resource_attributes` JSON. At read time the scanner injects `service_name` back as the `service.name` label so PromQL matchers work.

**Logs** (17 columns): timestamp, severity (`severity_number` Int32, `severity_text` Utf8), `body`, `trace_id` (FixedSizeBinary 16), `span_id` (FixedSizeBinary 8), flags, scope, `attributes` (Utf8 JSON), `resource_attributes`, and the same dedicated correlation columns as metrics.

**Traces**: `timestamp_ns`, `span_id` (FixedSizeBinary 8), `span_name` (Dictionary), `span_kind`, dedicated correlation columns, `trace_id` (FixedSizeBinary 16), `parent_span_id`, `status_code`/`status_message`, `start`/`end` timestamps, `duration_ns`, events, links, `attributes`, `resource_attributes`.

### Block Index

The `BlockIndex` is an in-memory structure that tracks all blocks per signal:

- **Persistence**: Serialized to `index.json` (JSON sidecar, atomic tmp+rename) in each signal's data directory — one index for metrics, one for logs, one for traces
- **Queries**: Supports time-range filtering and metric-name filtering
- **Statistics**: Tracks total blocks, rows, bytes, metric names, and label names

### Compaction

The `Compactor` runs on a configurable interval (default: 1 hour):

1. Identifies small adjacent blocks with overlapping time ranges
2. Reads all data points from candidate blocks
3. Writes a single merged Parquet file
4. Atomically updates the index and removes old files

### Retention

The `RetentionPolicy` runs alongside compaction:

1. Scans the index for blocks whose `end_timestamp_ns` is older than the retention window
2. Deletes expired Parquet files from disk
3. Removes entries from the index

## Storage Engine Registry

The `StorageEngineRegistry` provides a pluggable backend system (`parqtel-core/src/engine/`):

```rust
#[async_trait]
pub trait StorageEngine: Send + Sync {
    async fn write_metrics_batch(&self, metrics: Vec<Metric>) -> Result<WrittenBlockMeta>;
    async fn write_logs_batch(&self, logs: Vec<LogRecord>) -> Result<WrittenBlockMeta>;
    async fn scan_metrics(&self, request: MetricScanRequest) -> Result<Vec<DataPoint>>;
    async fn scan_logs(&self, request: LogScanRequest) -> Result<Vec<LogRecord>>;
    async fn compact_metrics(&self) -> Result<CompactionStats>;
    async fn compact_logs(&self) -> Result<CompactionStats>;
    // ... expiry, stats
}
```

Currently ships with the `"parquet"` backend (`ParquetStorageEngine`). The registry pattern allows future backends (e.g., object storage) without changing the core API.

## Configuration System

Parqtel uses [Figment](https://github.com/SergioBenitez/Figment) for layered configuration with this priority (highest wins):

1. CLI flags (`--bind`, `--data-dir`, `--log-level`)
2. Environment variables (`PARQTEL__STORAGE__COMPRESSION`)
3. TOML config file (`config/default.toml`)
4. Built-in defaults

All configuration is validated at startup via `Config::validate()`.

## Embedded Web UI

The console at `/ui` is a single-file vanilla-JS app (`parqtel-server/src/ui.html`) embedded via `include_str!`:

- **Serving**: pre-gzipped at startup with a content-hash ETag; `Cache-Control: public, max-age=3600` + 304 responses — zero per-request server cost
- **Zero external requests**: no CDNs, no fonts, no frameworks; system font stacks only — works air-gapped
- **Budget**: ≤42 KB gzipped (CI-checkable)
- **Features**: Overview landing pane with per-signal stat cards, hash-based deep-linkable URLs, guided metrics Builder⇄Code query toggle, log facets sidebar, trace-grouped browse list + waterfall, alert stream with Evidence tab (metric chart + correlated logs), form-based rule editor with YAML escape hatch, saved views (localStorage), keyboard shortcuts with `?` help modal, WCAG AA contrast, reduced-motion support

See [UI_UX_IMPROVEMENT_PLAN.md](UI_UX_IMPROVEMENT_PLAN.md) for the design audit and phased plan that produced the current console.

## Concurrency Model

- **Async runtime**: Tokio with `features = ["full"]`
- **Shared state**: `Arc<Inner>` wrapped in `AppState` (cheaply cloneable)
- **Ingestion services**: Protected by `tokio::sync::Mutex` (async-aware)
- **Block index**: Protected by `tokio::sync::RwLock` (readers don't block each other)
- **Background tasks**: Spawned via `tokio::spawn` for flush (5s), alert evaluation (15s), compaction, retention, and index updates
- **Channel communication**: `mpsc::unbounded_channel` for metadata propagation from writers to index
- **Blocking pool**: Parquet encode/compress/IO and block scans run on `spawn_blocking` with a semaphore acquired before spawning (bounded concurrency, never starving async workers)

## Error Handling

- **Library crates** (`parqtel-core`, `parqtel-ingest`, etc.): Use `thiserror` with typed error enums
- **Binary** (`parqtel-server`): Uses `anyhow` for top-level error propagation
- **No panics**: `unwrap`, `expect`, and `panic` are denied by clippy workspace lints
- **No unsafe**: `unsafe_code` is forbidden at the workspace level

## MCP Architecture

MCP servers are separate binaries built on `parqtel-mcp-core`:

```rust
pub struct McpServer {
    config: ServerConfig,
    tools: Vec<McpTool>,
    rate_limiter: RateLimiter,
}
```

Each MCP server:
- Exposes a JSON-RPC API over HTTP
- Registers tools with typed input schemas (JSON Schema)
- Implements rate limiting (token bucket, configurable per-minute)
- Runs independently — can be deployed alongside or separately from the main server

## Build & Release

| Setting | Value | Purpose |
|---------|-------|---------|
| `lto` | `"fat"` | Full link-time optimization |
| `codegen-units` | `1` | Maximum optimization (single compilation unit) |
| `strip` | `"symbols"` | Remove debug symbols from binary |
| `panic` | `"abort"` | No unwinding overhead |
| `opt-level` | `3` | Maximum runtime performance |

The Docker image uses a multi-stage build: Rust builder → distroless runtime (~15 MB final image). Requires Rust 1.87+ (`ARG RUST_VERSION=1.87`).

## Data Compatibility

The arrow2→arrow 59 migration (RUSTSEC-2025-0038) changed the on-disk encoding: **Parquet blocks written by arrow2-era builds are unreadable by the current reader**. When upgrading across that boundary, wipe the data directory (or accept that old blocks will be skipped with scan warnings).
