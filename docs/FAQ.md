# Frequently Asked Questions (FAQ)

### General

#### What makes Parqtel different from Prometheus?
Prometheus is a Time-Series Database (TSDB) optimized for high-frequency scraping and short-term storage. Parqtel is an **observability engine** that stores data in **Parquet blocks**. This allows for significantly better compression (10-20x), cheaper long-term storage, and native support for high-cardinality logs and traces in the same system.

#### Is Parqtel a replacement for ClickHouse?
For observability use cases, **yes**. While ClickHouse is a general-purpose OLAP database, Parqtel is purpose-built for OTLP data. It is easier to deploy (single binary vs. complex cluster) and requires zero schema management—just send your OTLP data and it works.

#### Why Rust?
Rust provides the memory safety and performance required for high-throughput ingestion while maintaining a tiny resource footprint. A full Parqtel instance typically uses < 100MB of RAM at idle.

### Storage

#### How do I back up my data?
Since Parquet blocks are immutable once written, you can simply use `rsync` or cloud snapshots to back up the `data/` directory. Each signal directory also holds its `index.json` sidecar (block index) — include it for faster startups after restoration.

#### Can I store data in S3/GCS?
Currently, Parqtel stores data on the local filesystem. Support for S3-compatible object storage as a "cold tier" is on the roadmap.

### Ingestion

#### Does Parqtel support scraping?
No. Parqtel is a **push-based** backend. We recommend using the **OpenTelemetry Collector** to scrape your targets and export them to Parqtel via OTLP.

#### Why doesn't my metric show up in instant queries?
`/api/v1/query` uses a 5-minute lookback window (`query.lookback_delta_ns`, default 300s — the Prometheus default). Send data stamped within the last 5 minutes, or use `/api/v1/query_range` with an explicit range covering your flushed blocks.

#### Can I filter metrics by service?
Yes — resource attributes like `service.name` are stored in dedicated Parquet columns and injected back as labels, so PromQL matchers like `http_requests_total{service.name="api"}` work for both freshly-buffered and flushed data.

#### What happens if the server crashes?
If WAL (Write-Ahead Log) is enabled (`ingest.wal_enabled` — off by default for metrics, on for logs via `ingest.log_wal_enabled`), Parqtel will recover any data that wasn't yet written to a Parquet block upon restart.

### Performance

#### How many metrics can a single instance handle?
A single Parqtel instance sustains ~1,000 samples/sec (metrics + logs + traces) on modest hardware — see [docs/benchmarks/PERFORMANCE.md](benchmarks/PERFORMANCE.md) for the sustained 15-minute numbers (ingest p99 63ms, zero errors). Scaling is primarily limited by Disk I/O.

#### How does high cardinality affect performance?
Unlike traditional TSDBs, Parqtel handles high cardinality (e.g., `user_id`, `container_id`) exceptionally well because of its columnar format. Adding more labels increases the file size slightly but does not "explode" memory usage in the same way it does in Prometheus.

### Integration

#### How do I connect Grafana?
Use the **SimpleJSON** datasource plugin and point it to your Parqtel URL. The HTTP query API is already PromQL-compatible (`/api/v1/query`, `/api/v1/query_range`, `/api/v1/labels`, `/api/v1/label/:name/values`), so existing PromQL queries work unchanged.

#### How do I monitor Parqtel itself?
Three built-in mechanisms:

- **`/api/v1/ingest_rates`** — live per-signal ingestion health: current rate plus 60s/5m/15m averages, wire bytes/sec, and gap detection (`gap_secs` counts silence; sparkline history via the `history_secs` param — default 180s, max 900s). Alert on `gap_secs` to catch a stalled pipeline.
- **`/metrics`** — Prometheus exposition of internal counters (`parqtel_batches_received_total`, `parqtel_storage_blocks`, and friends) for dashboards and alerting.
- **OTLP self-telemetry** — set `telemetry.otlp_enabled = true` (plus `otlp_endpoint`, `export_interval_secs`, `otlp_trace_level`) and Parqtel exports its own traces and SLI metrics to a collector, exactly like any other service it ingests from.

For CPU/memory profiling during tuning, set `telemetry.profiling_enabled = true` (with `profiling_frequency`) and scrape the `/debug/pprof/{profile,summary,memory}` endpoints — they 404 while profiling is disabled.

#### How do the MCP servers work?
MCP servers are lightweight proxies that translate LLM requests into Parqtel queries or actions (like posting to Slack). They allow your AI agents to "see" your metrics and logs during an incident.
