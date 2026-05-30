# GEMINI.md - Parqtel OSS Context

## Project Overview
Parqtel is an ultra-lightweight SRE observability engine designed to ingest OpenTelemetry (OTLP) metrics, logs, and traces and store them as compressed Apache Parquet files. It is written in Rust and focuses on minimal resource footprint, columnar storage efficiency, and PromQL compatibility.

### Key Technologies
- **Language:** Rust (Tokio, Axum, Arrow2/Parquet2)
- **Storage:** Apache Parquet with Zstd compression
- **APIs:** OTLP (Protobuf/JSON), Prometheus-compatible Query API, Grafana SimpleJSON
- **Integrations:** Model Context Protocol (MCP) for AI-driven incident response (Slack, PagerDuty, Jira, etc.)

## Architecture
- `parqtel-core`: Core storage engine, block indexing, and compaction.
- `parqtel-ingest`: OTLP decoding and Parquet writing.
- `parqtel-query`: PromQL-compatible execution engine.
- `parqtel-alert`: YAML-based alerting engine with state management.
- `parqtel-pipeline`: Recording rules and stream processing.
- `parqtel-server`: HTTP server and API handlers.
- `parqtel-mcp-*`: Specialized AI tool servers.

## Building and Running

### Prerequisites
- Rust 1.85+
- Docker & Docker Compose

### Key Commands
- `make build`: Build the project in debug mode.
- `make release`: Build optimized release binary.
- `make test`: Run unit and integration tests.
- `make lint`: Run `cargo fmt` and `clippy`.
- `make run`: Start the server locally.
- `cd deploy/compose && docker-compose up -d`: Start the full stack (Parqtel, Grafana, Prometheus, MCP servers).

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
