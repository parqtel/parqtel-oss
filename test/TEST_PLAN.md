# Parqtel OSS Test Plan

## Overview
This document outlines the comprehensive test strategy for Parqtel OSS, covering End-to-End (E2E) functional testing, edge cases, performance benchmarking, and resiliency/fault tolerance.

## 1. Test Environment Setup
We recommend using the `docker-compose` setup for local testing to ensure all components (Parqtel, Grafana, Prometheus, MCP servers) are running in a consistent environment.

### Prerequisites
- Docker and Docker Compose installed.
- Python 3.x (for load generation scripts).

### Launching the Test Environment
```bash
cd deploy/compose
cp .env.example .env
docker-compose up -d --build
```

---

## 2. E2E Test Scenarios

### 2.1 Metrics Ingestion & Querying
- **Scenario:** Ingest OTLP metrics (Protobuf/JSON) and query them via PromQL.
- **Verification:**
    - Send Gauge, Counter, and Histogram metrics.
    - Execute `api/v1/query` and `api/v1/query_range`.
    - Verify label matching and aggregations (`sum`, `avg`, `rate`).
- **Tools:** `curl`, `scripts/load_gen.py`.

### 2.2 Log Ingestion & Filtering
- **Scenario:** Ingest OTLP logs and query them with filters.
- **Verification:**
    - Send logs with various severity levels and attributes.
    - Query logs via `/api/v1/logs` with attribute filters.
    - Verify log count and field extraction.

### 2.3 Alerting Engine
- **Scenario:** Define an alert rule and trigger it with synthetic data.
- **Verification:**
    - Create a YAML rule (e.g., `http_error_rate > 5%`).
    - Send metrics that exceed the threshold.
    - Verify alert state transition: `Inactive` -> `Pending` -> `Firing`.
    - Verify alert resolution when metrics return to normal.

### 2.4 Pipelines & Recording Rules
- **Scenario:** Define a recording rule to pre-aggregate metrics.
- **Verification:**
    - Create a pipeline rule.
    - Verify that new series are generated based on the rule.
    - Verify that recording rules reduce query-time load.

### 2.5 MCP Server Integrations
- **Scenario:** Verify MCP servers can interact with external tools (Mocked).
- **Verification:**
    - Check `/health` endpoint of each MCP server.
    - (Optional) Use `parqtel-mcp-parqtel` to query data via MCP.

---

## 3. Edge Cases

- **Invalid Data Formats:** Send malformed JSON/Protobuf to ingestion endpoints. Verify 400 Bad Request response and system stability.
- **High Cardinality:** Ingest metrics with 10,000+ unique label combinations. Verify query performance and memory usage.
- **Clock Drift:** Send data points with timestamps in the future or distant past. Verify how the storage engine handles out-of-order or stale data.
- **Disk Full:** Simulate a full disk. Verify that Parqtel stops ingesting gracefully and does not corrupt existing Parquet files.
- **Huge Payload:** Send a single POST request with 50MB+ of OTLP data. Verify handling and potential timeouts.

---

## 4. Performance Benchmarking

### 4.1 Ingestion Throughput
- **Goal:** Measure max RPS (Requests Per Second) for metrics and logs.
- **Target:** 50,000+ samples/sec on a single core.

### 4.2 Query Latency
- **Goal:** Measure latency for range queries over 1h, 24h, and 7d.
- **Target:** < 500ms for 1h range queries with 100k series.

### 4.3 Storage Efficiency
- **Goal:** Measure Parquet compression ratio.
- **Target:** 10-20x reduction compared to raw JSON.

### 4.4 Resource Footprint
- **Goal:** Monitor CPU and RSS memory under constant load.
- **Target:** < 200MB RSS for idle, < 1GB RSS for high load.

### 4.5 Performance Audit Script
A wrapper script is provided in `scripts/run_perf_audit.sh`. To run a local audit:
```bash
./scripts/run_perf_audit.sh
```
This script will:
1. Start a local Parqtel instance.
2. Run `load_gen.py` to saturate ingestion.
3. Collect resource metrics (CPU/RSS).
4. Generate a `perf_report.md`.

---

## 5. Resiliency & Fault Tolerance

### 5.1 Process Crash & Recovery
- **Scenario:** Kill the `parqtel` process while it's writing a block.
- **Verification:** Restart the process and verify that the WAL (Write-Ahead Log) or partial Parquet files are recovered or cleaned up, and no data corruption occurs.

### 5.2 Network Partition
- **Scenario:** Block traffic between the load generator and Parqtel.
- **Verification:** Verify that the load generator handles retries and Parqtel resumes ingestion once the partition is resolved.

### 5.3 Graceful Shutdown
- **Scenario:** Send `SIGTERM` to the process during high load.
- **Verification:** Verify that all pending blocks are flushed to disk before the process exits.

### 5.4 Disk Corruption
- **Scenario:** Manually corrupt a Parquet block index.
- **Verification:** Verify that Parqtel identifies the corrupt block, logs an error, but continues to serve other blocks.

---

## 6. Execution & Results
The testing agent will execute the scenarios above.
Results will be recorded in `test/results/`.

### Commands for Testing Agent:
```bash
# Run unit tests
make test

# Run E2E tests (requires Go)
cd e2e && go test ./...

# Run performance audit
./scripts/run_perf_audit.sh
```
