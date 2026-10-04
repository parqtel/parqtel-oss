#!/usr/bin/env bash
# Seeds demo data into a running Parqtel instance for Argo Rollouts validation.
# Usage: PARQTEL_URL=http://localhost:8080 ./seed-data.sh
set -euo pipefail

PARQTEL_URL="${PARQTEL_URL:-http://localhost:8080}"
SERVICE="${SERVICE:-demo}"
NOW_NS="$(date +%s)000000000"

# ── Tier 1: a gauge metric for the smoke test ────────────────────────────────
curl -sS -X POST "$PARQTEL_URL/v1/metrics/json" \
  -H "Content-Type: application/json" \
  -d '{
    "resourceMetrics": [{
      "resource": {"attributes": [{"key":"service.name","value":{"stringValue":"'"$SERVICE"'"}}]},
      "scopeMetrics": [{
        "metrics": [{
          "name": "canary_success_ratio",
          "gauge": {"dataPoints": [{"asDouble": 0.99, "timeUnixNano": "'"$NOW_NS"'"}]}
        }]
      }]
    }]
  }'
echo "Seeded gauge canary_success_ratio=0.99 (service=$SERVICE)"

# ── Tier 2: a burst of server spans so RED span-metrics derive ───────────────
# OTel: kind=2 (SERVER), status.code=2 (ERROR), 1/0 (OK/UNSET)
OK_SPANS="${OK_SPANS:-100}"
ERR_SPANS="${ERR_SPANS:-5}"

seed_span() {
  local status_code="$1"
  local trace_id span_id
  trace_id="$(openssl rand -hex 16)"
  span_id="$(openssl rand -hex 8)"
  curl -sS -X POST "$PARQTEL_URL/v1/traces/json" \
    -H "Content-Type: application/json" \
    -d '{
      "resourceSpans": [{
        "resource": {"attributes": [{"key":"service.name","value":{"stringValue":"'"$SERVICE"'"}}]},
        "scopeSpans": [{
          "spans": [{
            "traceId": "'"$trace_id"'",
            "spanId": "'"$span_id"'",
            "name": "GET /",
            "kind": 2,
            "startTimeUnixNano": "'"$NOW_NS"'",
            "endTimeUnixNano": "'"$NOW_NS"'",
            "status": {"code": '"$status_code"'}
          }]
        }]
      }]
    }'
}

for _ in $(seq 1 "$OK_SPANS");  do seed_span 1; done
for _ in $(seq 1 "$ERR_SPANS"); do seed_span 2; done
echo "Seeded $OK_SPANS OK + $ERR_SPANS ERROR server spans (service=$SERVICE)"
echo "RED metrics available: traces_service_{requests,errors,duration_ms}_total{service.name=\""$SERVICE"\"}"
