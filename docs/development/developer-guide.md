# Developer Guide

This guide provides deep technical insights into Parqtel's internals for developers who want to contribute to the core engine or build extensions.

## 1. Codebase Architecture

Parqtel is a Rust workspace composed of several specialized crates. This modularity ensures clear boundaries and allows for independent testing.

### Core Crates

Every crate is a directory under `crates/`, one per concern.

- **`parqtel-core`**: The heart of the system. Contains the `BlockStore`/`StorageEngine` seams, the `BlockIndex`, the write-ahead log, the shared data models (`Metric`, `LogRecord`, `Span`), the scanner/compactor and the layered configuration. Depends on no other workspace crate.
- **`parqtel-ingest`**: Handles the heavy lifting of OTLP decoding. It manages the `BlockRotator` and ensures crash-safe writes to Parquet.
- **`parqtel-query`**: Contains the query execution engine. It handles PromQL parsing and coordinates data retrieval from the storage layer.
- **`parqtel-alert`**: Alert rules, the evaluation loop, the state machine and the built-in preset packs.
- **`parqtel-pipeline`**: Recording rules and stream-processing pipelines.
- **`parqtel-server`**: The Axum-based HTTP entry point — routes, middleware, telemetry, and the embedded console. The binary is named `parqtel`, not `parqtel-server`.
- **`parqtel-mcp-core`** and **`parqtel-mcp-parqtel`**: the Model Context Protocol framework and the Parqtel query/observability MCP server.

See [Diagrams](diagrams.md) and [Architecture](architecture.md#2-crate-dependency-graph) for the dependency graph.

## 2. Extending block storage

There are two seams, and they solve different problems:

- **`StorageEngine`** (`crates/parqtel-core/src/engine/`) is the write/compaction
  registry. Only the `parquet` backend ships. Adding a new format means
  implementing the trait and registering it.
- **`BlockStore`** (`crates/parqtel-core/src/storage/block_store.rs`) is the I/O
  seam used by the scanner, compaction and retention. `LocalBlockStore` and
  `ObjectBlockStore` implement it. Object storage does **not** implement a
  second engine: it keeps the same Parquet blocks and materialises a cache file
  for `Seek`.

To add an object-storage provider:

1. Build an `object_store::ObjectStore` implementation.
2. Construct `ObjectBlockStore` with the store, prefix, cache directory, cache
   quota and Tokio handle.
3. Install it once with `block_store::install`.
4. Publish a `CatalogBlock` after the local block is durable; the shared
   catalog uses compare-and-swap and writer-owned retirement.

See [Object storage](../operations/object-storage.md) for the operational
contract and [ADR 0001](adr-0001-object-storage.md) for why the seam is
`BlockStore`, not `StorageEngine`.

## 3. Working on the Embedded UI

The web console (`crates/parqtel-server/src/ui.html`) is a single-file vanilla-JS app with strict constraints:

- **Zero external requests** — no CDNs, no web fonts, no frameworks, no icon packs. Use system font stacks and inline SVG.
- **Size budget: ≤1000 KB gzipped (soft)** — keep lean; check with `gzip -c crates/parqtel-server/src/ui.html | wc -c`.
- **No build step** — the file is embedded via `include_str!` and served pre-gzipped with an ETag.
- **Validating changes**: Node may be unavailable locally; verify JS with headless Chrome instead:
  ```bash
  "/Applications/Google Chrome.app/Contents/MacOS/Google Chrome" \
    --headless=new --disable-gpu --enable-logging=stderr \
    --virtual-time-budget=15000 http://localhost:8080/ui
  ```
  Any `Uncaught SyntaxError`/`ReferenceError` lines in stderr indicate a regression.
- **Design system**: semantic CSS tokens, inline SVG icon sprite, WCAG AA contrast, `prefers-reduced-motion` support. The console is a single embedded file (`crates/parqtel-server/src/ui.html`) with no external requests and no framework — see [Architecture](architecture.md#12-console-and-mcp).

## 4. Adding a New MCP Tool

Parqtel's AI-native capabilities come from its MCP servers. To add a new tool (e.g., a "GitHub" tool):

1. **Navigate to `parqtel-mcp-core`**: This crate provides the shared framework. An `McpTool` is a plain struct with `name`, `description`, and `input_schema` fields — not a trait.
2. **Define the Tool**: Construct an `McpTool` and pair it with a `ToolHandler` closure.
   ```rust
   use parqtel_mcp_core::{McpTool, ToolHandler};
   use serde_json::{json, Value};

   let github_tool = McpTool {
       name: "create_issue".to_string(),
       description: "Open a GitHub issue".to_string(),
       input_schema: json!({
           "type": "object",
           "properties": {"title": {"type": "string"}},
           "required": ["title"]
       }),
   };

   let handler: ToolHandler = Arc::new(|params: Value| {
       Box::pin(async move {
           // call the GitHub API, then return a JSON Value
           Ok(json!({"issue": 42}))
       })
   });
   ```
3. **Register it**: `server.register_tool_with_handler(github_tool, handler)` in the relevant MCP server binary. See `crates/parqtel-mcp-parqtel/src/lib.rs` (tool definitions) and `src/main.rs` (registration) for a complete example.

## 5. Development Workflow

### Prerequisites
- Rust 1.88 (MSRV — CI pins and checks exactly 1.88; the workspace `Cargo.toml`, `Dockerfile`, and `ci.yml` move together)
- `protoc` (for OTLP protobuf compilation; `make build`/`make test` auto-fetch it via the `tools` target, or install `protobuf-compiler`)

### Build Commands
- `cargo build`: Standard debug build.
- `cargo test --workspace`: Run the entire test suite.
- `cargo clippy --workspace -- -D warnings`: Ensure code follows project standards (no panics, no unsafe).
- `make local-rebuild`: Rebuild the Docker compose stack after source changes (required — a stale image silently runs old code).

## 6. Coding Standards

- **Lints**: Every crate opts into `[workspace.lints]` (`[lints] workspace = true`): `unsafe_code` is forbidden, and `unwrap`/`expect`/`panic` are denied outside tests. New crates inherit `version`, `edition`, `rust-version`, `license` and `authors` from `[workspace.package]`.
- **Error Handling**: Always use `Result` and the `thiserror` crate for library errors. Avoid `unwrap()` at all costs.
- **Concurrency**: Use `tokio` primitives. Prefer `mpsc` channels for communication between services. Run blocking I/O (Parquet encode, file scans) on `spawn_blocking` with a pre-acquired semaphore. Block scans additionally share a process-wide 4-slot admission budget — reserve a slot with `Scanner::try_reserve_scan_slot()` for background work and skip the cycle when none is free.
- **Documentation**: All public functions must have doc comments (`///`).
- **Module layout**: Split a file once it covers several concerns (roughly 600+ lines of production code). Make it a directory module, keep the public API re-exported from `mod.rs`, mark items shared between sibling files `pub(super)`, and describe the split in the `mod.rs` doc comment.

## 7. Testing Strategy

- **Unit Tests**: Small test modules sit inline at the bottom of the file they cover. Larger suites live next to the module as `<module>/tests.rs` (for a file module `foo.rs`, create `foo/tests.rs` and declare `#[cfg(test)] mod tests;`), or as a `tests/` directory when they split by topic. Test code may use `unwrap`/`expect`; production code may not.
- **Integration Tests**: Found in the `tests/` directory of each crate.
- **E2E Tests**: The Kubernetes suite in `tests/e2e/` requires a running cluster and is built with the `e2e` build tag (`cd tests/e2e && go test -tags e2e ./...`; Go + client-go). The PromQL functional suite needs no cluster — `make e2e-promql` runs it against the local compose stack.

## 8. Performance Profiling

We use the `perf_bench` example for hot-path micro-benchmarking (hand-rolled timing, no `criterion` dependency) and `run_perf_audit.sh` for system-level audits.
- To run micro-benchmarks: `cargo run --release -p parqtel-server --example perf_bench`
- To run a system audit: `make perf-audit` (release build + `scripts/bench/run_perf_audit.sh`)
- To drive synthetic load: `make load` (10k points) or `make load-test LOAD_RATE=... LOAD_TIME=...`
- To roll metric blocks up into their 5-minute L1 windows offline: `parqtel compact`

### Changing the storage layout

Metric blocks are written series-by-series and carry a series table in the
Parquet footer (`parqtel.series_index`). Anything that writes a metric block
must go through `metric_runs_to_block`, and `write_block_file` is the single
place the footer, bloom filters and page statistics are applied. If you change
the layout, keep `SeriesTable::validate` and the
`indexed_scan_equals_unindexed_scan` equivalence test passing.
