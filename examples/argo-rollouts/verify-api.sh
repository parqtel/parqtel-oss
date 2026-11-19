#!/usr/bin/env bash
# Verify the exact Prometheus-provider queries used by the Argo examples.
#
# Usage:
#   PARQTEL_URL=http://localhost:8080 ./verify-api.sh
#
# Seeds the demo service, then checks the smoke gauge, the RED ratio and the
# range-query shape Argo Rollouts reads. Requires curl and jq.
set -euo pipefail

ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
PARQTEL_URL="${PARQTEL_URL:-http://localhost:8080}"
SERVICE="${SERVICE:-demo}"

for command in curl jq; do
  command -v "$command" >/dev/null || {
    echo "missing required command: $command" >&2
    exit 1
  }
done

PARQTEL_URL="$PARQTEL_URL" SERVICE="$SERVICE" "$ROOT/seed-data.sh" >/dev/null

query() {
  curl -fsS -G "$PARQTEL_URL/api/v1/query" \
    --data-urlencode "query=$1" |
    jq -er '.data.result'
}

echo "smoke query"
smoke="$(query "canary_success_ratio{service.name=\"$SERVICE\"}")"
jq -e 'length > 0 and (.[0].value[1] | tonumber) >= 0.95' <<<"$smoke" >/dev/null

echo "red ratio"
red="$(query "sum(sum_over_time(traces_service_errors_total{service.name=\"$SERVICE\"}[5m])) / sum(sum_over_time(traces_service_requests_total{service.name=\"$SERVICE\"}[5m]))")"
jq -e 'length > 0 and (.[0].value[1] | tonumber) >= 0 and (.[0].value[1] | tonumber) <= 1' <<<"$red" >/dev/null

echo "range query shape"
now="$(date +%s)"
curl -fsS -G "$PARQTEL_URL/api/v1/query_range" \
  --data-urlencode "query=canary_success_ratio{service.name=\"$SERVICE\"}" \
  --data-urlencode "start=$((now - 60))" \
  --data-urlencode "end=$now" \
  --data-urlencode "step=10" |
  jq -e '(.data.result | length) > 0 and ((.data.result[0].values | length) > 0)' >/dev/null

echo "argo api checks: pass"
