#!/usr/bin/env bash
set -euo pipefail

# parqtel SRE Validation Script
# Comprehensive validation of metrics, logs, and HPA without Go dependency.

NAMESPACE=${NAMESPACE:-parqtel}
# Use port-forwarding for reliable access during validation
PORT=9090
PARQTEL_URL="http://localhost:$PORT"

echo "===================================================="
echo ">>> Starting parqtel SRE Validation"
echo ">>> Namespace: $NAMESPACE"
echo ">>> URL:       $PARQTEL_URL (via port-forward)"
echo ">>> Context:   $(kubectl config current-context)"
echo "===================================================="

# Start port-forward in background
echo ">>> Starting port-forward to deploy/parqtel..."
kubectl port-forward -n "$NAMESPACE" deploy/parqtel "$PORT:8080" > /dev/null 2>&1 &
PF_PID=$!

# Ensure cleanup on exit
trap "kill $PF_PID || true" EXIT

echo ">>> Waiting for port-forward to be ready..."
RETRY=0
while ! curl -s "$PARQTEL_URL/api/v1/label/__name__/values" > /dev/null; do
    sleep 2
    RETRY=$((RETRY+1))
    if [ $RETRY -gt 15 ]; then
        echo "ERROR: Port-forward failed to become ready."
        exit 1
    fi
done
echo ">>> Port-forward ready."

# 1. Metric Ingestion Validation
echo ">>> [1/3] Validating Metric Ingestion..."
METRIC_NAME="sre_validation_metric_$(date +%s)"
TIMESTAMP=$(date +%s%N)
PAYLOAD=$(cat <<EOF
{
  "resource_metrics": [{
    "resource": { "attributes": [{ "key": "service.name", "value": { "string_value": "sre-validator" } }] },
    "scope_metrics": [{
      "metrics": [{
        "name": "$METRIC_NAME",
        "gauge": {
          "data_points": [{
            "time_unix_nano": "$TIMESTAMP",
            "as_double": 42.0,
            "attributes": [{ "key": "env", "value": { "string_value": "lab" } }]
          }]
        }
      }]
    }]
  }]
}
EOF
)

curl -s -f -X POST -H "Content-Type: application/json" -d "$PAYLOAD" "$PARQTEL_URL/v1/metrics/json"
echo "Metric $METRIC_NAME ingested."

# Verify via API
echo "Verifying metric availability..."
RETRY=0
while [ $RETRY -lt 12 ]; do
    RESPONSE=$(curl -s -f "$PARQTEL_URL/api/v1/label/__name__/values")
    if echo "$RESPONSE" | grep -q "$METRIC_NAME"; then
        echo "SUCCESS: Metric ingestion verified."
        break
    fi
    echo "...waiting for metric indexing ($RETRY/12)..."
    sleep 5
    RETRY=$((RETRY+1))
done

if [ $RETRY -eq 12 ]; then
    echo "ERROR: Metric $METRIC_NAME not found in metadata API after 60s."
    echo "Response: $RESPONSE"
    exit 1
fi

# 2. Log Ingestion Validation
echo ">>> [2/3] Validating Log Ingestion..."
LOG_MESSAGE="SRE validation log at $(date)"
PAYLOAD=$(cat <<EOF
{
  "resource_logs": [{
    "resource": { "attributes": [{ "key": "service.name", "value": { "string_value": "sre-validator" } }] },
    "scope_logs": [{
      "log_records": [{
        "time_unix_nano": "$TIMESTAMP",
        "body": { "string_value": "$LOG_MESSAGE" },
        "attributes": [{ "key": "level", "value": { "string_value": "info" } }]
      }]
    }]
  }]
}
EOF
)

curl -s -f -X POST -H "Content-Type: application/json" -d "$PAYLOAD" "$PARQTEL_URL/v1/logs/json"
echo "Log message ingested."
# Note: Currently logs are only verifiable by querying Parquet directly or via future logs API
# For now, we verify the 200 OK from the ingestion endpoint.
echo "SUCCESS: Log ingestion endpoint responded with 200 OK."

# 3. HPA Custom Metrics Validation
echo ">>> [3/3] Validating HPA Custom Metrics Scaling..."
PARQTEL_URL="$PARQTEL_URL" bash scripts/validate-hpa.sh

echo "===================================================="
echo ">>> ALL VALIDATIONS PASSED SUCCESSFULLY"
echo "===================================================="
