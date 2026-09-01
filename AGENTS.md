# AGENTS.md - Parqtel OSS Agent Instructions

## Project Overview
Parqtel is an ultra-lightweight SRE observability engine written in Rust. It ingests OpenTelemetry (OTLP) metrics, logs, and traces and stores them as compressed Apache Parquet files.

## Recent Changes (fix/architecture-quality branch)

### Architecture & Quality Fixes Applied

#### 1. Fixed QueryExecutor Hardcoded Trace Path
- **Files**: `parqtel-query/src/executor.rs`, `parqtel-server/src/main.rs`, `parqtel-server/src/state.rs`, `parqtel-server/src/tests.rs`, `parqtel-server/examples/perf_bench.rs`
- **Issue**: Trace index path was hardcoded to `/tmp/parqtel-traces` in 4 constructors
- **Fix**: Added `trace_data_dir` parameter to all `QueryExecutor` constructors; now uses configurable path from `config.storage.data_dir.join("traces")`

#### 2. Removed Dead Code - Unused Trace Proto Handlers
- **File**: `parqtel-server/src/handlers/ingest.rs`
- **Removed**: `ingest_traces_proto()` and `ingest_otlp_traces()` functions
- **Reason**: Not registered in router; clippy dead_code warnings

#### 3. Removed Unused ServerExtension Trait
- **File**: `parqtel-server/src/router.rs`
- **Removed**: `ServerExtension` trait (never implemented)
- **Reason**: Clippy unused trait warning

#### 4. Removed Unused AppStateInner Fields
- **File**: `parqtel-server/src/state.rs`
- **Removed**: `storage_engine` and `memory_buffer` fields (never accessed)
- **Impact**: Reduced `AppState::new()` from 10 to 8 parameters

#### 5. Fixed Export Path Traversal Vulnerability
- **File**: `parqtel-server/src/main.rs` (`run_export` function)
- **Issue**: User-controlled output path without validation
- **Fix**: Validates output path is within configured data directory using canonicalize

## Build & Test Commands

```bash
# Build
make build          # Debug build
make release        # Optimized release build (LTO, stripped)

# Test
make test           # cargo test --workspace
make lint           # cargo fmt --check && cargo clippy --workspace -- -D warnings

# Run
make run            # Start server locally

# Docker
make docker         # Build Docker image
```

## CI Pipeline
- **Lint**: fmt + clippy with `-D warnings`
- **Test**: All workspace tests + doc tests
- **MSRV**: Checks minimum supported Rust version (1.86)
- **Security**: rustsec/audit-check + Trivy filesystem scan
- **Helm**: Lint chart with multiple value files
- **Docker**: Build + smoke test (health check + metrics endpoint)

## Code Conventions
- **No unsafe**: `#[forbid(unsafe_code)]` workspace-wide
- **No panics**: `unwrap_used`, `expect_used`, `panic` denied by clippy
- **Errors**: `thiserror` for libraries, `anyhow` for binary
- **Async**: Tokio with full features
- **Config**: Figment layered (defaults → TOML → env → CLI)
- **Logging**: tracing + tracing-subscriber

## Key Source Paths
- `parqtel-server/src/main.rs` — CLI entry point
- `parqtel-server/src/router.rs` — Axum routes
- `parqtel-server/src/handlers/` — HTTP handlers
- `parqtel-core/src/` — Storage engine, models, config
- `parqtel-ingest/src/` — OTLP decoding, Parquet writing
- `parqtel-query/src/` — PromQL query execution
- `parqtel-alert/src/` — Alert rules, evaluation, state machine

## Configuration
Layered via Figment (priority: CLI > env > TOML > defaults):
- `PARQTEL_BIND` — Server bind address
- `PARQTEL_DATA_DIR` — Data directory
- `PARQTEL__STORAGE__COMPRESSION` — Compression codec
- `PARQTEL__STORAGE__RETENTION_DAYS` — Retention period
- `RUST_LOG` — Log level

## API Endpoints
- **Ingestion**: `/v1/metrics`, `/v1/logs`, `/v1/traces` (protobuf + JSON)
- **Query**: `/api/v1/query`, `/api/v1/query_range`, `/api/v1/labels*`
- **Logs**: `/api/v1/logs`, `/v1/logs/count`, `/v1/logs/fields`
- **Alerts**: `/api/v1/alerts`, `/api/v1/rules`
- **Pipelines**: `/api/v1/recording_rules`, `/api/v1/pipelines`
- **Ops**: `/health`, `/metrics`, `/oas`, `/ui`

## Security Notes
- No authentication/authorization in open-source version (commercial feature)
- Ingestion rate limiting not implemented (recommend auth proxy)
- Export CLI validates output path within data directory
- Container runs as non-root (UID 65532), read-only rootfs, all capabilities dropped