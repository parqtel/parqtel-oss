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

Run E2E tests (the Go suite carries a `//go:build e2e` tag, so it must be enabled or nothing compiles):
```bash
cd e2e && go test -tags e2e ./...
```
The PromQL functional suite needs no cluster — `make e2e-promql` runs it against the local compose stack. The Kubernetes suite (`-tags e2e`) requires a running cluster (Go + client-go).

## 3. Performance & Load Testing

We use Python-based load generators to stress-test the system:
- **`scripts/load_gen.py`**: Generates high-volume metrics and traces.
- **`scripts/run_perf_audit.sh`**: A comprehensive script that runs a load test, monitors CPU/RSS memory, and generates a `perf_report.md`.

Canonical entry points (see the Makefile):
- `make load` — send 10k synthetic points to the running instance
- `make load-test LOAD_RATE=1000 LOAD_TIME=60 TARGET_URL=http://localhost:9090 LOAD_TYPE=metrics` — full configurable load test
- `make perf-audit` — release build + full performance audit report
- Query conformance suites against a running instance: `make test-api`, `make test-aggregations`, `make test-functions`, `make test-builder`, `make test-builder-ui` (builder E2E via headless Chrome)
- Preset alert rules: `make test-presets-static` (schema + query plan validation, CI-safe) and `make test-alert-presets` (end-to-end — boots an instance, loads the packs, and proves rules fire on threshold crossings and recover when they clear; needs `curl`, `jq`, `python3`)

## 4. Resiliency Testing

We perform "Chaos" style testing to ensure Parqtel handles failures gracefully:
- **Crash Recovery**: We kill the process during high-load ingestion and verify that the WAL (Write-Ahead Log) restores unflushed data. Note the defaults: the metrics WAL is off (`ingest.wal_enabled = false`) and the logs WAL is on (`ingest.log_wal_enabled = true`), so coverage depends on which WAL was enabled for the run.
- **Disk Full**: We simulate a full disk and verify that Parqtel stops ingestion without corrupting existing Parquet blocks.

## 5. Automated Validation (CI)

Our GitHub Actions workflows gate every PR:
- **Detect Changes** — path filtering gates the expensive jobs (docs-only PRs finish in ~10 s)
- **Lint** — `cargo fmt --check` + `cargo clippy --workspace --all-targets --locked -- -D warnings` (also forbids `unsafe` code workspace-wide)
- **Test** — `cargo test --workspace` plus doc tests, all `--locked`
- **MSRV** — checks the pinned minimum Rust version (1.87)
- **Security Audit** — `cargo audit --deny warnings` + Trivy filesystem scan
- **Helm Lint** — validates `charts/parqtel` against `ci/minimal-values.yaml`, `ci/default-values.yaml`, and `ci/full-values.yaml`
- **Docker Build & Smoke Test** — builds the image and probes `/health` and `/metrics`

PR CI runs on Linux (`ubuntu-latest`) only; the multi-platform binaries (linux amd64/arm64, macOS) are built by `release.yml` on tags.

## 6. How to Add a Test

### Adding a Unit Test (Rust)
Add a `#[cfg(test)]` module at the bottom of your file.

### Adding an E2E Scenario (Go)
Add a new file or function in `e2e/tests/`. Use the helpers in `e2e/helpers/` for common tasks like sending metrics or waiting for readiness.

### Adding a Load Test (Python)
Extend `scripts/load_gen.py` or create a new script in `scripts/` if you need to simulate a specific traffic pattern.
