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

1. **Implement the Trait**: Define your backend by implementing the `StorageEngine` trait found in `parqtel-core/src/storage/mod.rs`.
   ```rust
   pub trait StorageEngine: Send + Sync {
       fn write_block(&self, block: &Block) -> Result<BlockMetadata>;
       fn read_block(&self, meta: &BlockMetadata) -> Result<Vec<DataPoint>>;
   }
   ```
2. **Register the Backend**: Add your implementation to the `StorageEngineRegistry`.

## 3. Adding a New MCP Tool

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

## 4. Development Workflow

### Prerequisites
- Rust 1.85+
- `protoc` (for OTLP protobuf compilation)

### Build Commands
- `cargo build`: Standard debug build.
- `cargo test --workspace`: Run the entire test suite.
- `cargo clippy --workspace`: Ensure code follows project standards (no panics, no unsafe).

## 5. Coding Standards

- **Error Handling**: Always use `Result` and the `thiserror` crate for library errors. Avoid `unwrap()` at all costs.
- **Concurrency**: Use `tokio` primitives. Prefer `mpsc` channels for communication between services.
- **Documentation**: All public functions must have doc comments (`///`).

## 6. Testing Strategy

- **Unit Tests**: Found in `src/` of each crate.
- **Integration Tests**: Found in the `tests/` directory of each crate.
- **E2E Tests**: Found in the root `e2e/` directory. These require a running Docker environment.

## 7. Performance Profiling

We use `criterion` for micro-benchmarking and `run_perf_audit.sh` for system-level audits.
- To run benchmarks: `cargo bench`
- To run system audit: `./scripts/run_perf_audit.sh`
