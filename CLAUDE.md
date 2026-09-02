# CLAUDE.md - Parqtel OSS Project Context

## Project Overview

Parqtel is an ultra-lightweight SRE observability engine written in Rust. It ingests OpenTelemetry (OTLP) metrics, logs, and traces and stores them as compressed Apache Parquet files. Single binary, ~15 MB Docker image (distroless).

## Build & Development Commands

```bash
make build          # Debug build
make release        # Optimized release build (LTO, stripped)
make test           # Run all workspace tests (cargo test --workspace)
make lint           # cargo fmt --check && cargo clippy --workspace -- -D warnings
make run            # Start server locally (cargo run --bin parqtel -- serve)
make docker         # Build Docker image and report size
make clean          # Remove target/ and data/
make load           # Send 10k synthetic data points to localhost:9090
make load-test      # Full load test with configurable LOAD_RATE, LOAD_TIME, TARGET_URL
make perf-audit     # Release build + performance audit script
```

### Docker Compose (full stack)

```bash
# First-time setup (copies .env.example → .env and starts the stack)
make dev-setup

# Or manually
cp .env.example .env       # edit for MCP API keys (optional)
docker compose up -d       # Parqtel + Grafana + Prometheus + load-generator
docker compose down
```

### E2E Tests (Go)

```bash
cd e2e && go test -v ./...
```

### Helm Chart

```bash
helm lint deploy/charts/parqtel
helm template test deploy/charts/parqtel
helm install parqtel deploy/charts/parqtel -n parqtel --create-namespace
```

## Architecture

Rust workspace with 14 crates:

| Crate | Role |
|-------|------|
| `parqtel-core` | Data models, storage engine, block index, compaction, retention, config |
| `parqtel-ingest` | OTLP protobuf/JSON decoding, block rotation, Parquet writing |
| `parqtel-query` | PromQL-compatible query execution, label matching, aggregations |
| `parqtel-alert` | Alert rule registry, threshold evaluation, state machine |
| `parqtel-pipeline` | Recording rules, stream processing, expression evaluation |
| `parqtel-server` | Axum HTTP server, route handlers, middleware, built-in UI |
| `parqtel-mcp-core` | Shared MCP framework (JSON-RPC, rate limiting, tool registry) |
| `parqtel-mcp-{slack,pagerduty,jira,notion,discord,gdocs,parqtel}` | MCP tool servers |

### Key Source Paths

- `parqtel-server/src/main.rs` — CLI entry point (clap), server bootstrap
- `parqtel-server/src/router.rs` — Axum route definitions
- `parqtel-server/src/handlers/` — HTTP handlers (ingest, prometheus, alerts, simplejson, pipeline)
- `parqtel-core/src/` — Storage engine, block index, config structs
- `parqtel-ingest/proto/` — OTLP protobuf definitions (compiled via build.rs)
- `rules/` — Example alert rules, recording rules, pipelines (YAML)

## Embedded Web UI (`/ui`)

Single-file vanilla-JS console at `parqtel-server/src/ui.html`, embedded via `include_str!`, pre-gzipped with ETag. **Zero external requests** (no CDNs/fonts/frameworks) — works air-gapped. Budget: ≤42 KB gzipped; keep it dependency-free.

Features: Overview pane, deep-linkable hash URLs, Builder⇄Code metrics query builder, log facets, trace-grouped browse list, alert Evidence tab, rule editor (form + YAML), saved views, `?` shortcut help, WCAG AA contrast.

## Deployment Infrastructure

### Docker

- **Dockerfile** — 4-stage build: cargo-chef planner → dependency cook → binary build → distroless runtime
- **Image**: `gcr.io/distroless/cc-debian12:nonroot`, ~15MB, no shell
- **Layer caching**: cargo-chef ensures dependency changes don't rebuild source

### Docker Compose (`compose/`)

- Network isolation: `frontend` (user-facing), `backend` (internal, no external access)
- YAML anchors (`x-common`) for DRY service config
- Resource limits, healthchecks (wget-based for distroless compatibility)
- Configurable ports via `.env`

### Helm Chart (`deploy/charts/parqtel/`)

- **values.schema.json** — Full JSON Schema validation, passes `helm lint`
- **CI test values** — `ci/minimal-values.yaml`, `ci/default-values.yaml`, `ci/full-values.yaml`
- **Features**: HPA, PDB, NetworkPolicy, Ingress, ServiceMonitor, PrometheusRule
- **MCP servers** — Each deployable as separate pod via `mcp.<name>.enabled`
- **Provider** — Optional Kubernetes custom metrics API server
- **Integrations** — AWS CloudWatch, GCP Cloud Monitoring, Azure Monitor

### GitHub Actions (`.github/workflows/`)

**ci.yml** (push/PR to main):
- `lint` — fmt + clippy
- `test` — cargo test --workspace
- `build` — release binary, uploaded as artifact
- `helm-lint` — validates chart with all CI value files
- `docker-build` — buildx with GHA cache (no push)
- `security` — cargo-audit

**release.yml** (on `v*` tags):
- Multi-arch Docker image (amd64 + arm64) → GHCR
- Helm chart → OCI registry
- GitHub Release with changelog

## Code Conventions

- **No unsafe code** — `#[forbid(unsafe_code)]` workspace-wide
- **No panics** — `unwrap_used`, `expect_used`, `panic` are denied by clippy
- **Error handling** — `thiserror` for library crate errors, `anyhow` at binary level
- **Async runtime** — Tokio with full features
- **HTTP framework** — Axum 0.7
- **Serialization** — serde + serde_json/serde_yaml
- **Config** — Figment (layered: defaults → TOML file → env vars → CLI flags)
- **Logging** — tracing + tracing-subscriber with env-filter
- **Release profile** — LTO (fat), single codegen unit, symbols stripped, abort on panic

## Configuration

Layered via Figment:
1. Built-in defaults
2. TOML config file (default: `config/default.toml`, override with `PARQTEL_CONFIG`)
3. Environment variables (`PARQTEL__` prefix, `__` as nested separator)
4. CLI flags (highest priority)

Key env vars: `PARQTEL_BIND`, `PARQTEL_DATA_DIR`, `PARQTEL__STORAGE__COMPRESSION`, `PARQTEL__STORAGE__RETENTION_DAYS`, `RUST_LOG`

## API Surface

- **Ingestion**: `/v1/{metrics,logs,traces}` (protobuf), `/v1/{metrics,logs,traces}/json`
- **Query**: `/api/v1/query`, `/api/v1/query_range`, `/api/v1/labels`, `/api/v1/label/:name/values`
- **Logs**: `/api/v1/logs`, `/v1/logs/count`, `/v1/logs/fields`, `/v1/logs/field_values`
- **Alerts**: `/api/v1/alerts`, `/api/v1/rules`
- **Pipelines**: `/api/v1/recording_rules`, `/api/v1/pipelines`
- **Grafana**: `/search`, `/query`, `/annotations`, `/tag-keys`, `/tag-values`
- **Ops**: `/health`, `/metrics`, `/oas`, `/ui`

## Storage Model

- Metrics blocks: 2h duration, up to 1M rows, Zstd compressed
- Log blocks: 30min duration, up to 200K rows, Zstd compressed
- Trace blocks: stored under `data/traces/` with their own `index.json`
- Block index: persisted as `index.json` (JSON sidecar, atomic tmp+rename) per signal directory
- Automatic compaction (hourly), configurable retention (default: 7d metrics, 3d logs)
- Each block is a self-contained Parquet file (arrow/parquet 59; arrow2-era blocks unreadable — wipe data dir across that upgrade)
- **In-memory buffer**: metrics, logs, AND traces are immediately queryable via `MemoryBuffer` (metrics HashMap-indexed by name); buffer drains on every flush — no double-counting
- **Instant queries** (`/api/v1/query`) use a 1-minute lookback window
- **service.name**: scanner + ingest buffer inject the dedicated `service_name` column back as the `service.name` label, so `{service.name="x"}` matchers work on both buffered and flushed data
- **OTLP gRPC**: tonic server on `:4317` (`server.grpc_bind_address`, "" disables) — all three collector services via the same `ingest_proto` path
- **Span-metrics RED**: server spans auto-derive `traces_service_{requests,errors,duration_ms}_total` (labels service/operation/service.name) fed through the normal metrics path
- **Tail sampling**: `ingest.tail_sampling` (keep_errors, slow_trace_ms, sampling_ratio, per_service) — runs after RED derivation (metrics unsampled, trace storage sampled); trace-coherent via trace_id hash; default keep-all

## Performance Optimizations

- **Non-blocking flushes** — Parquet encode + compression + disk I/O run on `tokio::task::spawn_blocking`; the rotator swaps out its writer so ingest continues during flushes. Flush is idempotent on an empty buffer.
- **Capacity pre-check** — rotators flush before a batch would exceed block capacity (no error-string matching, no dropped overflow points); push reports whether it flushed so callers drain the memory buffer correctly.
- **Scanner on blocking pool** — block scans use bounded `spawn_blocking` tasks (semaphore acquired before spawn), never blocking async workers.
- **Row-group statistics pruning** — row groups are skipped via timestamp min/max statistics; blocks are written with ~25K-row groups to make pruning effective.
- **Per-chunk label caching** — parsed LabelSets cached by raw JSON text per chunk; metric scans skip resource-attribute parsing entirely and filter timestamp/metric name before any allocation.
- **Indexed MemoryBuffer** — `HashMap<String, Vec<DataPoint>>` for O(1) metric lookup (was O(n) linear scan)
- **Buffer drain on flush** — memory stays bounded; buffer cleared when data hits Parquet
- **OTLP content negotiation** — `/v1/{metrics,logs,traces}` accept both protobuf and JSON via content-type dispatch

### Benchmarks (sustained 15 min, 1000 samples/sec)

| Metric | Value |
|--------|-------|
| Ingest p50 | 4.4ms |
| Ingest p99 | 63ms |
| Query p50 (instant) | 12.7ms |
| Query p99 (range) | 160ms |
| Immediate queryability | 1.7ms |
| Total ingested | 875,100 samples |
| Errors | 0 |

Hot-path micro-benchmarks (ingest/flush/scan/query throughput, before/after): [docs/benchmarks/PERFORMANCE.md](docs/benchmarks/PERFORMANCE.md). Reproduce with `cargo run --release -p parqtel-server --example perf_bench`.

## Dependencies (key)

arrow/parquet 59 (arrow2/parquet2 removed — RUSTSEC-2025-0038), axum 0.7, tokio 1, prost 0.12, clap 4, figment 0.10, tracing 0.1, reqwest 0.12 (rustls), chrono 0.4. MSRV: Rust 1.87 (workspace Cargo.toml; CI matrix also checks 1.86).
