# AGENTS.md - Parqtel OSS Agent Instructions

## Project Overview

Parqtel is an ultra-lightweight SRE observability engine written in Rust. It ingests OpenTelemetry (OTLP) metrics, logs, and traces and stores them as compressed Apache Parquet files. Single binary, ~15 MB Docker image (scratch-based, no shell).

## Repository Layout

```text
.
├── crates/                  Rust workspace members (one crate per concern)
│   ├── parqtel-core/        data models, storage engine, block index, scanner, compaction, WAL, config
│   ├── parqtel-ingest/      OTLP decoding, block rotation, crash-safe Parquet writing
│   ├── parqtel-query/       PromQL / LogQL execution, label matching, aggregation
│   ├── parqtel-alert/       alert rules, evaluation, state machine, built-in preset packs
│   ├── parqtel-pipeline/    recording rules, stream processing, expression evaluation
│   ├── parqtel-server/      Axum HTTP + gRPC server, handlers, embedded web console (the `parqtel` binary)
│   ├── parqtel-mcp-core/    shared MCP framework (JSON-RPC, rate limiting, tool registry)
│   └── parqtel-mcp-*/       MCP tool servers (slack, pagerduty, jira, notion, discord, gdocs, parqtel)
├── deploy/
│   ├── compose/             Docker Compose stack (Parqtel, Grafana, Prometheus, load generator, MCP)
│   ├── helm/parqtel/        Helm chart
│   ├── k8s/                 Kubernetes overlays, k3d config and helper scripts
│   └── systemd/             systemd unit
├── docs/                    user, operator and developer documentation (index: docs/README.md)
│   └── api/openapi.yaml     HTTP API description, embedded into the binary
├── rules/                   example alert rules, recording rules, pipelines, preset packs
├── scripts/                 bench/, load/, demo/, k8s/, checks/, release/
├── tests/e2e/               Go end-to-end tests against a Kubernetes deployment
├── examples/                integration examples
├── Dockerfile               production image
└── Makefile                 developer entry points (`make help`)
```

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
- **Instant queries** (`/api/v1/query`) use a **5-minute lookback window** (`query.lookback_delta_ns`, default 300000000000 — the Prometheus default) — data older than that won't appear until flushed to a block and queried via `query_range`.
- **In-memory buffer**: metrics, logs, and traces are all queryable immediately after ingest via `MemoryBuffer`; the buffer is drained on every flush so buffered + flushed data never double-counts.
- **service.name label**: the scanner and ingest buffer both inject the dedicated `service_name` Parquet column back as the `service.name` label, so PromQL matchers like `http_requests_total{service.name="api"}` work for both buffered and flushed data. Trace decode merges resource attributes into span attributes (span attrs win).
- **OTLP gRPC**: `:4317` by default (`ServerConfig.grpc_bind_address`; empty string disables). tonic server implements all three collector services and routes through the same `ingest_proto` path as HTTP protobuf.
- **Label sets** (`parqtel_core::LabelSet`) are immutable and `Arc`-shared; the fingerprint and compact JSON are cached on the shared value. OTLP decode resolves attributes through `LabelInterner` (bounded, a pure cache). Build them with `try_from_iter`/`with`/`merge`; never assume two equal sets are the same allocation.
- **Memory buffer epochs**: every writer shard has an epoch, buffered metric points carry `(shard, epoch)`, and a finished block removes exactly the sealed epochs (`drop_sealed_metrics`); logs and spans drop the oldest `row_count` rows. The buffer must be fed under the same lock as the writer so both hold one order.
- **WAL ordering**: a block's commit position is the WAL's end taken while every shard is locked; commits and buffer drops are released in flush order (`FlushOrder`). Each block records the WAL span `(from, to]` it covers (Parquet footer key `parqtel.wal_range` + `BlockMetadata.wal_range`); startup runs `advance_wal_commit` after reconcile and before replay, following the chain of spans so rows already in a block are not replayed. Records are numbered by their **end** offset, the same number `append` returns. Logs and traces share one generic `SerialRotator` (one flush in flight, WAL records framed before the lock).
- **Series table**: metric blocks are written series-by-series (then time) and carry `parqtel.series_index` in the Parquet footer (`storage/series_index.rs`; built by `metric_runs_to_block`, written by `write_block_file` for ingest, compaction and the engine). `open_block` parses it once into a `SeriesIndex` (cached, byte-budgeted) and strips the text from the cached footer. `SeriesSelector.equals`/`value_tests` only narrow; `filter` is the full matcher and always decides. Anything that writes a metric block must go through `metric_runs_to_block`; anything that changes the layout must keep `SeriesTable::validate` and the equivalence test (`indexed_scan_equals_unindexed_scan`) passing. Label-set fingerprints must stay process-independent (series limits are applied in fingerprint order).
- **Log skip index**: log blocks carry `parqtel.log_index` (`storage/log_index.rs`, built by `logs_to_block`, per 4096-row granule: time bounds, max severity, bloom over lower-cased body trigrams and attribute/resource `name=value` pairs). `LogColumnPrefilter` conditions (`body_contains_lower`, `attr_equals`, `min_severity`) must be *necessary* conditions of the row predicate; set `exact` only when the predicate is nothing more than severity + body-contains, because the scan then counts and ranks from three columns without calling the row filter. Filters may only skip granules, never decide rows.
- **Scans** (`Scanner::scan_filtered`, `scan_logs_counted_prefiltered`) take pushed-down predicates: a series filter evaluated once per distinct series per block, and a log column prefilter (severity/body) derived only from AND conjuncts. Callers that add a pushdown must keep the full row predicate as the final judge.
- **Compressed OTLP**: HTTP request bodies are gunzipped outside the size limits; gRPC accepts gzip.
- **Tail sampling**: `ingest.tail_sampling` policy (keep_errors, slow_trace_ms, sampling_ratio, per-service overrides) runs AFTER RED derivation — RED metrics see all spans; storage/buffer sees only sampled traces. Decisions are trace-coherent (deterministic trace_id hash); default keeps everything.
- **Span-metrics RED bridge**: server-kind spans automatically derive `traces_service_{requests,errors,duration_ms}_total` metrics (labels: `service`, `operation`, `service.name`) through the normal metrics path — wired in `server.rs` via the `with_span_metrics` channel.

## Code Conventions

- **No unsafe**: `#[forbid(unsafe_code)]` workspace-wide
- **No panics**: `unwrap_used`, `expect_used`, `panic` denied by clippy
- **Errors**: `thiserror` for libraries, `anyhow` for binary
- **Async**: Tokio with full features; blocking I/O (Parquet encode, scans) on `spawn_blocking` with pre-acquired semaphore
- **Config**: Figment layered (defaults → TOML → env → CLI)
- **Logging**: tracing + tracing-subscriber
- **Embedded UI**: single-file `crates/parqtel-server/src/ui.html`, no external requests, no frameworks, gzip+ETag served at `/ui`. Budget: ≤1000 KB gzipped (soft limit — keep it lean; it is a single file embedded in the binary). Keep it dependency-free and render-fast (no heavy libraries, no layout thrash on poll updates).

## Key Source Paths

- `crates/parqtel-server/src/main.rs` — configuration and subcommand dispatch; `cli.rs`, `commands.rs`, `server.rs` (serve wiring), `tasks.rs` (background tasks), `recovery.rs` (startup reconcile and WAL replay)
- `crates/parqtel-server/src/router.rs` — Axum routes; `handlers/` — HTTP handlers (`handlers/query/` is the query API); `ui.html` — embedded console
- `crates/parqtel-core/src/storage/` — `scanner/` (block scans), block writer, block/series/log indexes, compaction, retention
- `crates/parqtel-core/src/models/storage/schema.rs` — canonical Parquet schemas (metrics/logs/traces)
- `crates/parqtel-core/src/wal.rs` — write-ahead log
- `crates/parqtel-ingest/src/` — `decode/` (OTLP decoding), `service/` (block rotation, flush ordering, per-signal services), `writer.rs` (Parquet writing); `proto/` — OTLP definitions (compiled by `build.rs`)
- `crates/parqtel-query/src/` — `parser.rs` and `eval/` (PromQL), `logql/` (ParqtelQL), `executor/` (execution per signal), `pipeline*.rs`
- `crates/parqtel-alert/src/` — alert rules, evaluation, state machine
- `rules/` — example rules; `rules/presets/` holds ready-made alert packs, embedded into the binary and inert until copied into the rules dir — validated by `make test-presets-static` and `make test-alert-presets`
- Large modules are directories whose `mod.rs` documents the split; larger unit-test suites sit in a sibling `tests.rs` or `tests/` (see `docs/development/developer-guide.md`)

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
- **Ops**: `/health`, `/metrics`, `/oas`, `/ui`, `/api/v1/stats` (storage/buffer/config snapshot), `/api/v1/ingest_rates` (live per-signal rates: current + 60s/5m/15m averages, gap_secs spike/gap detection, per-second sparkline history via `history_secs` param — default 180s, max 900s in-memory wheel)

## CI Pipeline

- **Lint**: fmt + clippy with `-D warnings`
- **Test**: All workspace tests + doc tests
- **MSRV**: Checks minimum supported Rust version (1.87 matrix — see Key Facts)
- **Security**: `cargo-audit` (installed via `taiki-e/install-action`, `--deny warnings`) + Trivy filesystem scan
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
- Seeding demo data: OTLP JSON `attributes` must be **arrays of {key, value:{stringValue}}** (not plain objects); metric instant queries need points stamped within the last 5 minutes (`query.lookback_delta_ns`)
