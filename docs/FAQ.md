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
`/api/v1/query` uses a 1-minute lookback window. Send data stamped within the last 60 seconds, or use `/api/v1/query_range` with an explicit range covering your flushed blocks.

#### Can I filter metrics by service?
Yes — resource attributes like `service.name` are stored in dedicated Parquet columns and injected back as labels, so PromQL matchers like `http_requests_total{service.name="api"}` work for both freshly-buffered and flushed data.

#### What happens if the server crashes?
If WAL (Write-Ahead Log) is enabled, Parqtel will recover any data that wasn't yet written to a Parquet block upon restart.

### Performance

#### How many metrics can a single instance handle?
A single Parqtel instance can handle 50,000+ samples per second on a modern single-core CPU. Scaling is primarily limited by Disk I/O.

#### How does high cardinality affect performance?
Unlike traditional TSDBs, Parqtel handles high cardinality (e.g., `user_id`, `container_id`) exceptionally well because of its columnar format. Adding more labels increases the file size slightly but does not "explode" memory usage in the same way it does in Prometheus.

### Integration

#### How do I connect Grafana?
Use the **SimpleJSON** datasource plugin and point it to your Parqtel URL. We are also working on a native PromQL-compatible datasource interface.

#### How do the MCP servers work?
MCP servers are lightweight proxies that translate LLM requests into Parqtel queries or actions (like posting to Slack). They allow your AI agents to "see" your metrics and logs during an incident.
