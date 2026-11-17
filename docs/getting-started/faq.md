# Frequently Asked Questions (FAQ)

### General

#### What makes Parqtel different from Prometheus?
Prometheus is a Time-Series Database (TSDB) optimized for high-frequency scraping and short-term storage. Parqtel is an **observability engine** that stores data in **Parquet blocks**. This allows for significantly better compression (10-20x), cheaper long-term storage, and native support for high-cardinality logs and traces in the same system.

#### Is Parqtel a replacement for ClickHouse?
For observability use cases, **yes**. While ClickHouse is a general-purpose OLAP database, Parqtel is purpose-built for OTLP data. It is easier to deploy (single binary vs. complex cluster) and requires zero schema management—just send your OTLP data and it works.

#### Why Rust?
Rust provides the memory safety and performance required for high-throughput ingestion while maintaining a tiny resource footprint. A full Parqtel instance starts in tens of megabytes; resident memory then follows the ingest rate, the number of active series and the buffered point count — see [sizing](../operations/performance-sizing.md).

### Storage

#### How do I back up my data?
Parquet blocks are immutable once written, so `rsync` or a filesystem snapshot of the `data/` directory is safe. Include each signal's `index.json` and `label_values.json` for a faster startup; reconciliation can rebuild a missing index from block footers. With object storage enabled, enable bucket versioning (or replicate the bucket) as well as backing up the local WAL and cache.

#### Can I store data in S3/GCS/Azure?
Yes for S3-compatible storage: set `[object_store]` and Parqtel uploads final Parquet blocks after the local rename, keeps a bounded LRU read cache, and shares a versioned catalog across nodes. GCS and Azure Blob are not implemented. See [Object storage](../operations/object-storage.md).

#### Is Parqtel multi-writer?
No. Run one writer per data root or object-store prefix. Additional nodes can read/adopt blocks from the shared catalog, but there is no writer fencing.

### Ingestion

#### Does Parqtel support scraping?
Parqtel does not run a Prometheus scrape loop. It is push-first: use the **OpenTelemetry Collector** to scrape and export OTLP, or point Prometheus `remote_write` at `POST /api/v1/write`.

#### Why doesn't my metric show up in instant queries?
`/api/v1/query` uses a 5-minute lookback window (`query.lookback_delta_ns`, default 300s — the Prometheus default). Send data stamped within the last 5 minutes, or use `/api/v1/query_range` with an explicit range covering your flushed blocks.

#### Can I filter metrics by service?
Yes — resource attributes like `service.name` are stored in dedicated Parquet columns and injected back as labels, so PromQL matchers like `http_requests_total{service.name="api"}` work for both freshly-buffered and flushed data.

#### What happens if the server crashes?
Both write-ahead logs are on by default (`ingest.wal_enabled` for metrics,
`ingest.log_wal_enabled` for logs), so Parqtel recovers any data that wasn't
yet written to a Parquet block upon restart. Turning the metrics WAL off also
disables the background flush worker and makes flushes synchronous.

### Performance

#### How many metrics can a single instance handle?
Measured end to end on 8 cores with a 2.4 M-point dataset: **1.85 M metric
points/s** (p99 16.8 ms), **1.59 M log records/s** (p99 10.8 ms) and 1.66 M
spans/s. The method and the raw outputs are in
[the performance notes](../performance/README.md). Scaling is primarily limited
by disk I/O and by how many blocks a query has to touch.

#### How does high cardinality affect performance?
Parquet stores labels in a per-block series table rather than on every row, so a high-cardinality series set costs less than a row-per-label TSDB. It is still a real cost: the series table, label interner, and query fan-out grow with active series. `ingest.series_limit_per_metric` (default 1M) caps new series per metric, and `ingest.label_interner_capacity` should be at least twice the active series count.

### Integration

#### How do I connect Grafana?
Use the **SimpleJSON** datasource plugin and point it to your Parqtel URL. The HTTP query API is already PromQL-compatible (`/api/v1/query`, `/api/v1/query_range`, `/api/v1/labels`, `/api/v1/label/:name/values`), so existing PromQL queries work unchanged.

#### How do I monitor Parqtel itself?
Three built-in mechanisms:

- **`/api/v1/ingest_rates`** — live per-signal ingestion health: current rate plus 60s/5m/15m averages, wire bytes/sec, and gap detection (`gap_secs` counts silence; sparkline history via the `history_secs` param — default 180s, max 900s). Alert on `gap_secs` to catch a stalled pipeline.
- **`/metrics`** — Prometheus exposition of internal counters (`parqtel_batches_received_total`, `parqtel_storage_blocks`, and friends) for dashboards and alerting.
- **OTLP self-telemetry** — set `telemetry.otlp_enabled = true` (plus `otlp_endpoint`, `export_interval_secs`, `otlp_trace_level`) and Parqtel exports its own traces and SLI metrics to a collector, exactly like any other service it ingests from.

For CPU/memory profiling during tuning, set `telemetry.profiling_enabled = true` (with `profiling_frequency`) and scrape `/debug/pprof/profile` or `/summary`. `/debug/pprof/memory` is always available. Restrict these endpoints to an internal network.

#### How does the MCP server work?
`parqtel-mcp-parqtel` is a lightweight Streamable HTTP (`/mcp`) server built on `parqtel-mcp-core` that translates LLM tool calls into Parqtel HTTP API queries (`/api/v1/query`, `/api/v1/logs`, `/api/v1/alerts`, `/api/v1/ingest_rates`, `/api/v1/stats`). It gives AI agents read access to your metrics, logs, alerts, and observed topology during an incident. See [MCP Integration](../guides/mcp.md).
