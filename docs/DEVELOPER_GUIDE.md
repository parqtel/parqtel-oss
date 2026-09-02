# Developer Guide

This guide provides deep technical insights into Parqtel's internals for developers who want to contribute to the core engine or build extensions.

## 1. Codebase Architecture

Parqtel is a Rust workspace composed of several specialized crates. This modularity ensures clear boundaries and allows for independent testing.

### Core Crates
- **`parqtel-core`**: The heart of the system. Contains the `StorageEngine` trait, the `BlockIndex`, and shared data models (`Metric`, `LogRecord`, `Span`).
- **`parqtel-ingest`**: Handles the heavy lifting of OTLP decoding. It manages the `BlockRotator` and ensures crash-safe writes to Parquet.
- **`parqtel-query`**: Contains the query execution engine. It handles PromQL parsing and coordinates data retrieval from the storage layer.
- **`parqtel-server`**: The Axum-based HTTP entry point. This is where routes, middleware, and telemetry are defined.

## 2. Extending the Storage Engine

Parqtel uses a registry pattern for storage backends. If you want to add a new backend (e.g., S3 or a different file format):

1. **Implement the Trait**: Define your backend by implementing the `StorageEngine` trait found in `parqtel-core/src/engine/mod.rs`.
   ```rust
   #[async_trait]
   pub trait StorageEngine: Send + Sync {
       async fn write_metrics_batch(&self, metrics: Vec<Metric>) -> Result<WrittenBlockMeta>;
       async fn write_logs_batch(&self, logs: Vec<LogRecord>) -> Result<WrittenBlockMeta>;
       async fn scan_metrics(&self, request: MetricScanRequest) -> Result<Vec<DataPoint>>;
       async fn scan_logs(&self, request: LogScanRequest) -> Result<Vec<LogRecord>>;
       async fn compact_metrics(&self) -> Result<CompactionStats>;
       async fn compact_logs(&self) -> Result<CompactionStats>;
       // ... expiry and stats methods
   }
   ```
2. **Register the Backend**: Add your implementation to the `StorageEngineRegistry`.

## 3. Working on the Embedded UI

The web console (`parqtel-server/src/ui.html`) is a single-file vanilla-JS app with strict constraints:

- **Zero external requests** — no CDNs, no web fonts, no frameworks, no icon packs. Use system font stacks and inline SVG.
- **Size budget: ≤42 KB gzipped** — check with `gzip -c parqtel-server/src/ui.html | wc -c`.
- **No build step** — the file is embedded via `include_str!` and served pre-gzipped with an ETag.
- **Validating changes**: Node may be unavailable locally; verify JS with headless Chrome instead:
  ```bash
  "/Applications/Google Chrome.app/Contents/MacOS/Google Chrome" \
    --headless=new --disable-gpu --enable-logging=stderr \
    --virtual-time-budget=15000 http://localhost:8080/ui
  ```
  Any `Uncaught SyntaxError`/`ReferenceError` lines in stderr indicate a regression.
- **Design system**: semantic CSS tokens, inline SVG icon sprite, WCAG AA contrast, `prefers-reduced-motion` support. See [UI_UX_IMPROVEMENT_PLAN.md](UI_UX_IMPROVEMENT_PLAN.md) for the conventions.

## 4. Adding a New MCP Tool

Parqtel's AI-native capabilities come from its MCP servers. To add a new tool (e.g., a "GitHub" tool):

1. **Navigate to `parqtel-mcp-core`**: This crate provides the shared framework.
2. **Define the Tool**: Create a struct that implements the `McpTool` trait.
   ```rust
   pub struct GitHubTool;
   impl McpTool for GitHubTool {
       fn name(&self) -> &str { "create_issue" }
       fn schema(&self) -> Value { /* JSON Schema for inputs */ }
       async fn call(&self, args: Value) -> Result<Value>;
   }
   ```
3. **Add to Registry**: Register the tool in the relevant MCP server binary.

## 5. Development Workflow

### Prerequisites
- Rust 1.87+ (MSRV; CI also checks 1.86)
- `protoc` (for OTLP protobuf compilation)

### Build Commands
- `cargo build`: Standard debug build.
- `cargo test --workspace`: Run the entire test suite.
- `cargo clippy --workspace -- -D warnings`: Ensure code follows project standards (no panics, no unsafe).
- `make local-rebuild`: Rebuild the Docker compose stack after source changes (required — a stale image silently runs old code).

## 6. Coding Standards

- **Error Handling**: Always use `Result` and the `thiserror` crate for library errors. Avoid `unwrap()` at all costs.
- **Concurrency**: Use `tokio` primitives. Prefer `mpsc` channels for communication between services. Run blocking I/O (Parquet encode, file scans) on `spawn_blocking` with a pre-acquired semaphore.
- **Documentation**: All public functions must have doc comments (`///`).

## 7. Testing Strategy

- **Unit Tests**: Found in `src/` of each crate.
- **Integration Tests**: Found in the `tests/` directory of each crate.
- **E2E Tests**: Found in the root `e2e/` directory. These require a running Kubernetes environment (Go + client-go).

## 8. Performance Profiling

We use `criterion` for micro-benchmarking and `run_perf_audit.sh` for system-level audits.
- To run benchmarks: `cargo bench`
- To run system audit: `./scripts/run_perf_audit.sh`
- Hot-path micro-benchmarks: `cargo run --release -p parqtel-server --example perf_bench`
