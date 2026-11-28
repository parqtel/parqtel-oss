# Parqtel Glossary

This document defines key terms and concepts used within the Parqtel ecosystem.

### A
- **Alert Engine**: The subsystem responsible for evaluating YAML-based rules against ingested data and managing alert state transitions.
- **Axum**: The high-performance, asynchronous web framework used for Parqtel's HTTP server.

### B
- **Block**: A time-bounded collection of data (metrics, logs, or traces) stored as a single Parquet file. Each signal directory (`data/`, `data/logs/`, `data/traces/`) has its own blocks and `index.json`.
- **Block Index**: An in-memory structure (with disk persistence) that tracks the metadata and time ranges of all Parquet blocks for fast querying.
- **Block Rotator**: The component that decides when to close a current memory buffer and write it to a new Parquet block on disk.

### C
- **Columnar Storage**: A database storage format where data is stored by column rather than row, enabling high compression and fast analytical queries.
- **Compaction**: The background process that merges multiple small Parquet blocks into larger ones to optimize storage efficiency and query performance.

### D
- **DQL (Data Query Language)**: Parqtel's internal expression language used for advanced log filtering and metric extraction.

### G
- **Grafana SimpleJSON**: A protocol used by Parqtel to serve data to Grafana dashboards without needing a complex Prometheus setup.

### I
- **Ingestion Service**: The entry point for all OTLP data, responsible for decoding, validation, and initial buffering.

### M
- **MCP (Model Context Protocol)**: An open standard that allows Parqtel to expose its data and tools directly to Large Language Models (LLMs) like Claude or GPT.

### O
- **OTLP (OpenTelemetry Protocol)**: The industry-standard protocol for transmitting metrics, logs, and traces. Parqtel supports both Protobuf and JSON variants.

### P
- **Parquet**: An open-source, column-oriented data file format designed for efficient data storage and retrieval.
- **Pipeline**: A set of stages (parsing, filtering, extraction) that data passes through during ingestion.
- **PromQL**: The Prometheus Query Language, supported by Parqtel for metric retrieval.

### R
- **Recording Rule**: A pre-calculated query that saves the result as a new metric series, reducing query-time overhead for complex dashboards.
- **Retention Policy**: The logic that determines when old data blocks should be deleted from the filesystem based on age.

### W
- **WAL (Write-Ahead Log)**: A crash-recovery mechanism that records incoming data to a persistent log before it is flushed to a Parquet block. On by default for both metrics and logs (`ingest.wal_enabled`, `ingest.log_wal_enabled`); turning the metrics WAL off makes the background flush worker refuse to start and fall back to synchronous flush.
- **WAL Span**: The `(from, to]` range of WAL records a block covers. Stored in the block footer (`parqtel.wal_range`) so startup can advance the commit point and skip records already in a block.

### L
- **Lookback Delta** (`query.lookback_delta_ns`): The window an instant query (`/api/v1/query`) scans backward from the query time — 5 minutes by default (the Prometheus default). Points older than that are visible only via `/api/v1/query_range` once flushed to a block.

### M
- **MemoryBuffer**: The in-memory index holding freshly ingested metrics, logs, and traces so they are queryable immediately — before the flush to Parquet. The buffer drains on every flush, so buffered and flushed data never double-count.

### P
- **PQL / ParqtelQL**: The two query surfaces — PQL is the umbrella name for PromQL-style metric queries plus ParqtelQL (log/trace selector search) and cross-signal pipelines. See [PQL_GUIDE.md](../guides/pql-guide.md).

### S
- **Span-Metrics RED Bridge**: Server-kind spans automatically derive `traces_service_{requests,errors,duration_ms}_total` metrics (labels: `service`, `operation`, `service.name`) through the normal metrics path.

### T
- **Tail Sampling**: An `ingest.tail_sampling` policy (keep_errors, slow_trace_ms, sampling_ratio, per-service overrides) applied after RED derivation — RED metrics see all spans, while storage sees only the sampled traces. Decisions are trace-coherent via a deterministic trace_id hash; the default keeps everything.

### L (levels)
- **L0 Block**: A metric block written by one ingest flush (`m_l0_*`). Series are sorted by the final query-visible labels, so the footer series table agrees with the row order.
- **L1 Block**: A metric block produced by the streaming compactor (`m_l1_*`), covering one 5-minute window. L1 blocks are what keeps a short query's block fan-out bounded.
- **L2 Block**: A metric block produced by the second compaction level (`m_l2_*`), covering one 2-hour window built from L1 sources. L2 blocks bound the fan-out of long ranges.

### I (interning and admission)
- **Label Interner**: A process-wide table of distinct label sets (`ingest.label_interner_capacity`, default 1 048 576). Points of one series share a single copy of their labels; below the working set, memory grows with points instead of series.
- **Scan Admission / Scan Budget**: The process-wide budget of concurrent block scans (`query.max_concurrent_scans`, default `min(cpus, 8)`). Queries wait for a slot; compaction draws from its own `query.compaction_scan_slots` budget and alert evaluation keeps a single slot, so background work skips its cycle instead of competing with dashboards.
- **Scoped Ownership**: With an object store, the registry that limits compaction and retention to blocks this node produced, so a sharded node never retires a peer's block.

### P (provenance)
- **Provenance Footer**: The list of source blocks an output block was merged from (`parqtel.compacted_sources`). Reconcile drops superseded sources after a crash; sources are deleted after a 5-minute grace period.

### Z
- **Zstd**: A high-performance compression algorithm used by Parqtel to reduce disk usage by up to 90%.
