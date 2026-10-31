# parqtel E2E Test Suite

This directory contains the end-to-end test suite for parqtel, written in Go using `client-go`.

## Prerequisites

- Go 1.21+
- A Kubernetes cluster (kind, k3d, etc.)
- `helm` and `kubectl` installed and in PATH
- `KUBECONFIG` environment variable set

## Running Tests

To run the full suite:
```bash
go test -v -tags e2e ./...
```

To run a specific test:
```bash
go test -v -tags e2e -run TestInstallation ./tests/
```

To run only security tests:
```bash
go test -v -tags e2e,security ./tests/
```

## Build Tags

- `e2e`: Required for all tests in this suite.
- `slow`: Long-running tests (load tests, persistence checks).
- `security`: Security posture and RBAC tests.
- `resilience`: Failure injection and recovery tests.
