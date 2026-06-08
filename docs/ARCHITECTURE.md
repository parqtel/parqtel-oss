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
                │  ├── index.bin    │  ← Block index
                │  └── logs/        │
                │      ├── *.parquet│  ← Log blocks (Zstd)
                │      └── index.bin│
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

1. **HTTP Request** → Axum handler receives OTLP protobuf or JSON body
2. **Decode** → `parqtel-ingest::decode` converts OTLP proto to internal `Metric`/`LogRecord`/`Span` models
3. **Buffer** → `IngestionService` accumulates data points in memory via `BlockRotator`
4. **Flush** → When block duration expires or row limit is reached, `BlockWriter` serializes to Parquet with configured compression
5. **Index** → `BlockMetadata` is sent via `mpsc::unbounded_channel` to the index task, which updates the in-memory `BlockIndex`
6. **Background** → A periodic flush task (5s interval) calls `check_and_flush()` to ensure data is persisted even under low traffic

### Query Path

1. **HTTP Request** → Prometheus-compatible query parameters parsed
2. **Parse** → `parse_query()` extracts metric name, label matchers, aggregation, and time range
3. **Plan** → `QueryPlan` identifies which blocks to scan via `BlockIndex::query(start, end, metric_name)`
4. **Scan** → `Scanner::scan()` reads matching Parquet files, applies time-range and metric-name filters at the columnar level
5. **Aggregate** → Aggregation functions (sum, avg, rate, histogram_quantile, etc.) are applied
6. **Response** → Results formatted as Prometheus JSON response

### Alert Path

1. **Rule Loading** → `AlertRuleRegistry` loads YAML rules from `rules/` directory, watches for changes via `notify`
2. **Evaluation** → `AlertEngine` periodically evaluates rules against current data using the query executor
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

### Parquet Schema (Metrics)

Each metric data point is stored with these columns:

| Column | Type | Encoding |
|--------|------|----------|
| `metric_name` | Dictionary(UTF-8) | RLE Dictionary |
| `timestamp_ns` | Int64 | Plain |
| `value` | Float64 | Plain |
| `labels` | UTF-8 (JSON) | Plain |
| `kind` | Dictionary(UTF-8) | RLE Dictionary |

### Block Index

The `BlockIndex` is an in-memory structure that tracks all blocks:

- **Persistence**: Serialized to `index.bin` via bincode on shutdown
- **Queries**: Supports time-range filtering and metric-name filtering
- **Statistics**: Tracks total blocks, rows, bytes, metric names, and label names

### Compaction

The `Compactor` runs on a configurable interval (default: 1 hour) and implements a **Tiered Compaction** strategy:

1. **Small Block Merging**: Identifies small adjacent blocks (<10K rows) and merges up to 12 into a single block to reduce metadata overhead.
2. **Warm Tier (>6h)**: Blocks older than 6 hours are merged into larger blocks spanning approximately 6 hours of data.
3. **Cold Tier (>24h)**: Blocks older than 24 hours are merged into daily blocks (24 hours).
4. **Limits**: Merge groups are capped at 500,000 rows to ensure blocks remain manageable for rapid scanning.
5. **Efficiency**: Only one merge pass is performed per signal type (Metrics, Logs, Traces) per cycle to avoid long lock holds on the index.

### Retention

The `RetentionPolicy` runs alongside compaction:

1. Scans the index for blocks whose `end_timestamp_ns` is older than the retention window.
2. Deletes expired Parquet files from disk.
3. Removes entries from the index.

## Concurrency & Scaling

### Bounded Query Concurrency
To prevent I/O saturation and system-wide slowdowns during heavy query loads, Parqtel implements bounded concurrency in the `Scanner`:
- **Semaphore-based limiting**: Max 16 concurrent block read tasks per signal.
- **Block Scanning Caps**: Queries are capped to scan a maximum of 128 blocks (64 for traces) to ensure predictable response times even for massive time ranges.
- **Reverse Scan Order**: Most recent blocks are scanned first (LIFO) to prioritize SRE relevance.

### Data Resilience
Parqtel is designed to survive data corruption without crashing:
- **Unwinding Panics**: The release profile uses `panic = "unwind"` to allow `catch_unwind` at the block-scanning boundary.
- **Graceful Skipping**: If a Parquet block is corrupt or has schema mismatches (e.g., incorrect byte widths for IDs), the scanner logs the error and skips the block rather than aborting the process.
- **Validation**: Schema validation (field count and physical type checks) is performed during block initialization.

## Configuration System

The `StorageEngineRegistry` provides a pluggable backend system:

```rust
pub trait StorageEngine: Send + Sync {
    fn write(&self, ...) -> Result<()>;
    fn read(&self, ...) -> Result<Vec<DataPoint>>;
}
```

Currently ships with the `"parquet"` backend. The registry pattern allows future backends (e.g., object storage) without changing the core API.

## Configuration System

Parqtel uses [Figment](https://github.com/SergioBenitez/Figment) for layered configuration with this priority (highest wins):

1. CLI flags (`--bind`, `--data-dir`, `--log-level`)
2. Environment variables (`PARQTEL__STORAGE__COMPRESSION`)
3. TOML config file (`config/default.toml`)
4. Built-in defaults

All configuration is validated at startup via `Config::validate()`.

## Concurrency Model

- **Async runtime**: Tokio with `features = ["full"]`
- **Shared state**: `Arc<Inner>` wrapped in `AppState` (cheaply cloneable)
- **Ingestion services**: Protected by `tokio::sync::Mutex` (async-aware)
- **Block index**: Protected by `tokio::sync::RwLock` (readers don't block each other)
- **Background tasks**: Spawned via `tokio::spawn` for flush, compaction, retention, and index updates
- **Channel communication**: `mpsc::unbounded_channel` for metadata propagation from writers to index

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
| `panic` | `"unwind"` | Enables resilience via `catch_unwind` |
| `opt-level` | `3` | Maximum runtime performance |

The Docker image uses a multi-stage build: Rust builder → distroless runtime (~15 MB final image).

## Extension Points

The server provides a `ServerExtension` trait for plugging in additional functionality:

```rust
pub trait ServerExtension: Send + Sync {
    fn routes(&self, state: AppState) -> Router<AppState>;
}
```

This enables enterprise features (auth, clustering, AI endpoints) to be added without modifying the core server.
