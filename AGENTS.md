# AGENTS.md - Parqtel OSS Agent Instructions

## Project Overview

Parqtel is an ultra-lightweight SRE observability engine written in Rust. It ingests OpenTelemetry (OTLP) metrics, logs, and traces and stores them as compressed Apache Parquet files. Single binary, ~15 MB distroless Docker image.

## Workspace Layout (14 crates)

| Crate | Role |
|-------|------|
| `parqtel-core` | Data models, storage engine, block index, scanner, compaction, retention, config |
| `parqtel-ingest` | OTLP protobuf/JSON decoding, block rotation, crash-safe Parquet writing |
| `parqtel-query` | PromQL-compatible query execution, label matching, aggregations |
| `parqtel-alert` | Alert rule registry, threshold evaluation, state machine |
| `parqtel-pipeline` | Recording rules, stream processing, expression evaluation |
| `parqtel-server` | Axum HTTP server, route handlers, embedded web UI (`src/ui.html`) |
| `parqtel-mcp-core` | Shared MCP framework (JSON-RPC, rate limiting, tool registry) |
| `parqtel-mcp-{slack,pagerduty,jira,notion,discord,gdocs,parqtel}` | MCP tool servers |

## Build & Test Commands

```bash
make build          # Debug build
make release        # Optimized release build (LTO, stripped)
make test           # cargo test --workspace
make lint           # cargo fmt --check && cargo clippy --workspace -- -D warnings
make run            # Start server locally (cargo run --bin parqtel -- serve)
make local-up       # Docker compose stack: Parqtel(9090) + Grafana(3000) + Prometheus(9091) + load-generator
make local-rebuild  # Rebuild images from current source and restart (REQUIRED after source changes)
make local-down    # Stop and remove the compose stack
make docker        # Build Docker image
```

## Key Facts (verify before documenting)

- **MSRV**: 1.87 (workspace `Cargo.toml`; comfy-table 7.2+ via arrow 59 uses let-chains). Dockerfile `ARG RUST_VERSION=1.87`. CI matrix pins 1.87 — update all three together if bumping.
- **gRPC stack**: tonic 0.13 / prost 0.13 (hyper 1.x + h2 0.4 — RUSTSEC-2026-0258 fixed by the upgrade from tonic 0.11)
- **Arrow stack**: `arrow`/`parquet` 59.x (migrated from arrow2/parquet2 — RUSTSEC-2025-0038). Old arrow2-era Parquet blocks are unreadable by the current reader; wipe the data dir when upgrading across that boundary.
- **Binary name**: `parqtel` (not `parqtel-server`) — the crate is `parqtel-server` but `[[bin]] name = "parqtel"`.
- **Block index**: persisted as `index.json` (JSON sidecar, atomic tmp+rename), not bincode/`index.bin`.
- **Instant queries** (`/api/v1/query`) use a **1-minute lookback window** — data older than ~60s won't appear until flushed to a block and queried via `query_range`.
- **In-memory buffer**: metrics, logs, and traces are all queryable immediately after ingest via `MemoryBuffer`; the buffer is drained on every flush so buffered + flushed data never double-counts.
- **service.name label**: the scanner and ingest buffer both inject the dedicated `service_name` Parquet column back as the `service.name` label, so PromQL matchers like `http_requests_total{service.name="api"}` work for both buffered and flushed data. Trace decode merges resource attributes into span attributes (span attrs win).
- **OTLP gRPC**: `:4317` by default (`ServerConfig.grpc_bind_address`; empty string disables). tonic server implements all three collector services and routes through the same `ingest_proto` path as HTTP protobuf.
- **Tail sampling**: `ingest.tail_sampling` policy (keep_errors, slow_trace_ms, sampling_ratio, per-service overrides) runs AFTER RED derivation — RED metrics see all spans; storage/buffer sees only sampled traces. Decisions are trace-coherent (deterministic trace_id hash); default keeps everything.
- **Span-metrics RED bridge**: server-kind spans automatically derive `traces_service_{requests,errors,duration_ms}_total` metrics (labels: `service`, `operation`, `service.name`) through the normal metrics path — wired in `main.rs` via `with_span_metrics` channel.

## Code Conventions

- **No unsafe**: `#[forbid(unsafe_code)]` workspace-wide
- **No panics**: `unwrap_used`, `expect_used`, `panic` denied by clippy
- **Errors**: `thiserror` for libraries, `anyhow` for binary
- **Async**: Tokio with full features; blocking I/O (Parquet encode, scans) on `spawn_blocking` with pre-acquired semaphore
- **Config**: Figment layered (defaults → TOML → env → CLI)
- **Logging**: tracing + tracing-subscriber
- **Embedded UI**: single-file `parqtel-server/src/ui.html`, no external requests, no frameworks, gzip+ETag served at `/ui`. Budget: ≤42 KB gzipped. Keep it dependency-free.

## Key Source Paths

- `parqtel-server/src/main.rs` — CLI entry point
- `parqtel-server/src/router.rs` — Axum routes
- `parqtel-server/src/handlers/` — HTTP handlers (ingest, prometheus, alerts, simplejson, pipeline, misc)
- `parqtel-server/src/ui.html` — embedded console (vanilla JS, embedded via `include_str!`)
- `parqtel-core/src/storage/` — scanner, block index, compaction
- `parqtel-core/src/models/storage/schema.rs` — canonical Parquet schemas (metrics/logs/traces)
- `parqtel-ingest/src/` — OTLP decoding, Parquet writing
- `parqtel-query/src/` — PromQL query execution
- `parqtel-alert/src/` — Alert rules, evaluation, state machine
- `rules/` — Example alert rules, recording rules, pipelines (YAML)

## Configuration

Layered via Figment (priority: CLI > env > TOML > defaults):
- `PARQTEL_BIND` — Server bind address (default `0.0.0.0:8080`)
- `PARQTEL_DATA_DIR` — Data directory (default `data`)
- `PARQTEL__STORAGE__COMPRESSION` — Compression codec (zstd/snappy/lz4/none)
- `PARQTEL__STORAGE__RETENTION_DAYS` — Retention period
- `RUST_LOG` — Log level

## API Endpoints

- **Ingestion**: gRPC `:4317` (OTLP default; all three collector services) + HTTP `/v1/metrics`, `/v1/logs`, `/v1/traces` (content negotiation: protobuf + JSON) + `/json` variants
- **Query**: `/api/v1/query`, `/api/v1/query_range`, `/api/v1/labels`, `/api/v1/label/:name/values`
- **Logs**: `/api/v1/logs`, `/v1/logs/count`, `/v1/logs/fields`, `/v1/logs/field_values`
- **Traces**: `/v1/traces/search` (per-span `trace_id` included for client-side grouping)
- **Alerts**: `/api/v1/alerts`, `/api/v1/rules`, `/api/v1/alerts/:id/{acknowledge,resolve,noise,signal}`
- **Correlation**: `/v1/correlate`
- **Pipelines**: `/api/v1/recording_rules`, `/api/v1/pipelines`
- **Grafana SimpleJSON**: `/search`, `/query`, `/annotations`, `/tag-keys`, `/tag-values`
- **Ops**: `/health`, `/metrics`, `/oas`, `/ui`

## CI Pipeline

- **Lint**: fmt + clippy with `-D warnings`
- **Test**: All workspace tests + doc tests
- **MSRV**: Checks minimum supported Rust version (1.87 matrix — see Key Facts)
- **Security**: rustsec/audit-check + Trivy filesystem scan
- **Helm**: Lint chart with multiple value files
- **Docker**: Build + smoke test (health check + metrics endpoint)

## Security Notes

- No authentication/authorization in open-source version (commercial feature)
- Ingestion rate limiting not implemented (recommend auth proxy)
- Export CLI validates output path within data directory
- Container runs as non-root (UID 65532), read-only rootfs, all capabilities dropped

## Known Pitfalls for Agents

- `rg` may be broken locally (missing `libpcre2-8.0.dylib`) — use `grep -n` via bash
- Node may be broken locally (missing `libllhttp`) — validate UI JS via headless Chrome instead
- Headless Chrome: `"/Applications/Google Chrome.app/Contents/MacOS/Google Chrome" --headless=new --disable-gpu --enable-logging=stderr --virtual-time-budget=15000 URL` to catch console errors
- Seeding demo data: OTLP JSON `attributes` must be **arrays of {key, value:{stringValue}}** (not plain objects); metric instant queries need points stamped within the last 60s
