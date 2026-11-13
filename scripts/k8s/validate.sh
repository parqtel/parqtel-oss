#!/usr/bin/env bash
set -euo pipefail

# parqtel E2E Validation Script
# Runs the Go-based E2E test suite.

echo ">>> Starting E2E validation..."
echo ">>> Using Context: $(kubectl config current-context)"

cd e2e
# We use parqtel.localhost which is mapped to the cluster ingress
go test -v -tags e2e ./... -namespace parqtel -parqtel-url http://parqtel.localhost

echo ">>> Validation successful."
