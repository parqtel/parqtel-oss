# Testing & Validation

Parqtel is built with a "Safety First" mindset. This document describes the multi-layered testing strategy we use to ensure data integrity and system reliability.

## 1. Unit & Integration Testing

Every crate in the Parqtel workspace contains its own test suite.
- **Unit Tests**: Focus on individual functions and modules (e.g., OTLP decoding, label matching).
- **Integration Tests**: Focus on the interaction between modules (e.g., the `IngestionService` writing to the `StorageEngine`).

Run all crate tests:
```bash
make test
```

## 2. End-to-End (E2E) Testing

Our E2E tests are written in Go and reside in the `e2e/` directory. They validate the entire system from the outside:
1. Start the Parqtel container.
2. Send OTLP data via HTTP.
3. Query the data via the Prometheus API.
4. Verify results match the input.

Run E2E tests:
```bash
cd e2e && go test ./...
```

## 3. Performance & Load Testing

We use Python-based load generators to stress-test the system:
- **`scripts/load_gen.py`**: Generates high-volume metrics and traces.
- **`scripts/run_perf_audit.sh`**: A comprehensive script that runs a load test, monitors CPU/RSS memory, and generates a `perf_report.md`.

## 4. Resiliency Testing

We perform "Chaos" style testing to ensure Parqtel handles failures gracefully:
- **Crash Recovery**: We kill the process during high-load ingestion and verify that the WAL (Write-Ahead Log) restores all data.
- **Disk Full**: We simulate a full disk and verify that Parqtel stops ingestion without corrupting existing Parquet blocks.

## 5. Automated Validation (CI)

Our GitHub Actions workflows run on every PR to ensure:
- Code builds on Linux and macOS.
- All tests pass.
- `cargo clippy` and `cargo fmt` checks pass.
- No `unsafe` code has been introduced.

## 6. How to Add a Test

### Adding a Unit Test (Rust)
Add a `#[cfg(test)]` module at the bottom of your file.

### Adding an E2E Scenario (Go)
Add a new file or function in `e2e/tests/`. Use the helpers in `e2e/helpers/` for common tasks like sending metrics or waiting for readiness.

### Adding a Load Test (Python)
Extend `scripts/load_gen.py` or create a new script in `scripts/` if you need to simulate a specific traffic pattern.
