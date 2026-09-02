# GEMINI.md - Parqtel OSS Context

## Project Overview
Parqtel is an ultra-lightweight SRE observability engine designed to ingest OpenTelemetry (OTLP) metrics, logs, and traces and store them as compressed Apache Parquet files. It is written in Rust and focuses on minimal resource footprint, columnar storage efficiency, and PromQL compatibility.

### Key Technologies
- **Language:** Rust 1.87+ (MSRV; CI also checks 1.86) — Tokio, Axum, arrow/parquet 59
- **Storage:** Apache Parquet with Zstd compression (block-based, JSON index sidecars)
- **APIs:** OTLP (Protobuf/JSON), Prometheus-compatible Query API, Grafana SimpleJSON
- **Integrations:** Model Context Protocol (MCP) for AI-driven incident response (Slack, PagerDuty, Jira, etc.)

## Architecture
- `parqtel-core`: Core storage engine, block indexing, and compaction.
- `parqtel-ingest`: OTLP decoding and Parquet writing.
- `parqtel-query`: PromQL-compatible execution engine.
- `parqtel-alert`: YAML-based alerting engine with state management.
- `parqtel-pipeline`: Recording rules and stream processing.
- `parqtel-server`: HTTP server, API handlers, and embedded web UI (`src/ui.html`, zero dependencies, ≤42 KB gzipped).
- `parqtel-mcp-*`: Specialized AI tool servers.

## Building and Running

### Prerequisites
- Rust 1.87+
- Docker & Docker Compose

### Key Commands
- `make build`: Build the project in debug mode.
- `make release`: Build optimized release binary.
- `make test`: Run unit and integration tests.
- `make lint`: Run `cargo fmt` and `clippy`.
- `make run`: Start the server locally.
- `make dev-setup`: First-time setup (copies `.env.example` → `.env`, starts the full stack — Parqtel, Grafana, Prometheus, load-generator). MCP servers are opt-in.
- `make local-rebuild`: Rebuild compose images from current source (required after source changes).

## Development Conventions
- **Safety:** No `unsafe` code allowed (`#[forbid(unsafe_code)]`).
- **Error Handling:** Use `thiserror` for internal errors and `anyhow` for top-level application errors.
- **Strictness:** `unwrap()`, `expect()`, and `panic!()` are forbidden in production code (enforced by Clippy).
- **Testing:** Unit tests in each crate, E2E tests in the `e2e/` directory using Go.

## Testing Infrastructure
- **E2E Tests:** Located in `e2e/tests/`, written in Go.
- **Load Testing:** Python scripts in `scripts/` (e.g., `load_gen.py`, `varying-load-test.py`).
- **Performance Audit:** `scripts/run_perf_audit.sh` for comprehensive benchmarking.
- **Resiliency:** E2E test `06_resilience_test.go` covers basic failure scenarios.

## Storage & Ingestion Notes
- OTLP ingestion over gRPC (:4317, OTel SDK default) and HTTP (protobuf/JSON).
- All three signals (metrics/logs/traces) are queryable immediately via the in-memory buffer; drained on flush so buffered + flushed data never double-counts.
- Span-metrics RED bridge: server-kind spans auto-derive `traces_service_{requests,errors,duration_ms}_total` metrics per service/operation.
- Instant queries (`/api/v1/query`) use a 1-minute lookback window.
- The `service.name` label is injected from a dedicated Parquet column, so `{service.name="x"}` matchers work on buffered and flushed data alike.
- Blocks written by arrow2-era builds are unreadable after the arrow 59 migration — wipe the data dir when upgrading across that boundary.
