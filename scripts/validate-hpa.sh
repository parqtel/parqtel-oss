#!/usr/bin/env bash
set -euo pipefail

# parqtel HPA Validation Script
# Validates the Kubernetes Custom Metrics Provider integration.

NAMESPACE=${NAMESPACE:-parqtel}
PARQTEL_URL=${PARQTEL_URL:-http://parqtel.localhost}
METRIC_NAME="http-requests-qps"
INTERNAL_METRIC_NAME="http_requests_qps"

echo ">>> Validating HPA Custom Metrics Provider..."

# 1. Check APIService
echo ">>> Checking APIService v1beta2.custom.metrics.k8s.io..."
STATUS=$(kubectl get apiservice v1beta2.custom.metrics.k8s.io -o jsonpath='{.status.conditions[?(@.type=="Available")].status}') || STATUS="False"
if [ "$STATUS" != "True" ]; then
    echo "ERROR: APIService v1beta2.custom.metrics.k8s.io is not Available (Status: $STATUS)"
    kubectl describe apiservice v1beta2.custom.metrics.k8s.io || true
    exit 1
fi
echo "SUCCESS: APIService is Available."

# 2. Create Metric Mapping
echo ">>> Creating ParqtelMetricMapping for $METRIC_NAME..."
cat <<EOF | kubectl apply -f -
apiVersion: parqtel.io/v1alpha1
kind: ParqtelMetricMapping
metadata:
  name: $METRIC_NAME
  namespace: $NAMESPACE
spec:
  metricName: $INTERNAL_METRIC_NAME
  sourceQuery: "sum(rate(http_requests_total[1m])) by (k8s_pod_name)"
  objectType: pods
  aggregation: avg
  windowSeconds: 60
EOF

# 3. Ingest some sample data
echo ">>> Ingesting sample metrics for http_requests_total..."
# We use the pod name from the deployment
POD_NAME=$(kubectl get pods -n $NAMESPACE -l app.kubernetes.io/name=parqtel -o jsonpath='{.items[0].metadata.name}')

# Build a simple OTLP JSON payload with http_requests_total
TIMESTAMP=$(date +%s%N)
PAYLOAD=$(cat <<EOF
{
  "resource_metrics": [{
    "resource": { "attributes": [{ "key": "service.name", "value": { "string_value": "parqtel" } }] },
    "scope_metrics": [{
      "metrics": [{
        "name": "http_requests_total",
        "sum": {
          "data_points": [{
            "time_unix_nano": "$TIMESTAMP",
            "as_int": "100",
            "attributes": [
              { "key": "k8s_pod_name", "value": { "string_value": "$POD_NAME" } },
              { "key": "k8s_namespace_name", "value": { "string_value": "$NAMESPACE" } }
            ]
          }],
          "is_monotonic": true,
          "aggregation_temporality": 1
        }
      }]
    }]
  }]
}
EOF
)

# Send the payload via port-forward or ingress
curl -s -f -X POST -H "Content-Type: application/json" \
    -d "$PAYLOAD" \
    "$PARQTEL_URL/v1/metrics/json"

echo ">>> Waiting for metric to be indexed (30s)..."
sleep 30

# 4. Query Custom Metrics API
echo ">>> Querying Custom Metrics API: /apis/custom.metrics.k8s.io/v1beta2/namespaces/$NAMESPACE/pods/*/$INTERNAL_METRIC_NAME"
# Retry a few times as the query engine might need time to process the first block
RETRY=0
while [ $RETRY -lt 12 ]; do
    RESPONSE=$(kubectl get --raw "/apis/custom.metrics.k8s.io/v1beta2/namespaces/$NAMESPACE/pods/*/$INTERNAL_METRIC_NAME" 2>/dev/null || echo "FAILED")
    if [[ "$RESPONSE" != "FAILED" && "$RESPONSE" != '{"kind":"MetricValueList","apiVersion":"custom.metrics.k8s.io/v1beta2","metadata":{},"items":[]}' ]]; then
        echo "SUCCESS: Received valid response from Custom Metrics API:"
        echo "$RESPONSE" | jq .
        break
    fi
    echo "...waiting for metric data ($RETRY/12)..."
    sleep 10
    RETRY=$((RETRY+1))
done

if [ $RETRY -eq 12 ]; then
    echo "ERROR: Failed to get custom metric data after 120s."
    echo "Last Response: $RESPONSE"
    exit 1
fi

echo ">>> HPA Validation Successful."
