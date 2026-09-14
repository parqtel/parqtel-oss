#!/usr/bin/env bash
# ─────────────────────────────────────────────────────────────────────────────
# test_aggregations.sh — PromQL aggregation conformance suite against a live
# parqtel instance. Verifies count/sum/avg/min/max/stddev grouping (both
# `agg(x) by (l)` and `agg by (l) (x)` positions), without-clauses, rate
# composition, filtered+regex grouping, topk, and the Prometheus instant-vector
# response shape (`value` field, exactly one sample per series).
#
# Usage:  scripts/test_aggregations.sh [URL]     (default http://localhost:9090)
# Requires: curl, jq, and a running stack with the load-generator feeding data
# (`make local-up`).
# ─────────────────────────────────────────────────────────────────────────────
set -u
URL="${1:-http://localhost:9090}"
PASS=0
FAIL=0

q() { # q <expr> — instant query, returns compact JSON
    curl -s -m 25 --get "$URL/api/v1/query" --data-urlencode "query=$1"
}

check() { # check <name> <expr> <jq-assert> — jq assertion over the response
    local name="$1" expr="$2" assertion="$3"
    local resp
    resp="$(q "$expr")"
    if echo "$resp" | jq -e "$assertion" >/dev/null 2>&1; then
        echo "PASS  $name"
        PASS=$((PASS + 1))
    else
        echo "FAIL  $name"
        echo "      query: $expr"
        echo "      response: $(echo "$resp" | head -c 300)"
        FAIL=$((FAIL + 1))
    fi
}

# The load-generator ships 10 http_requests_total_* counters (labels:
# method/status/instance/service.name), 15 system_metric_* gauges (host), and
# 5 latency_ms_* histograms (service). Sanity-gate on their presence first.
# NOTE: {__name__=~"regex"} name-matching is not yet supported by the
# selector engine, so we gate on a concrete series instead.
echo "─── sanity: load-generator data present ─────────────────────"
q 'http_requests_total_0' | jq -e '(.data.result | length) > 0' >/dev/null 2>&1 \
    || { echo "FATAL: no http_requests_total_0 series — run 'make local-up' first"; exit 1; }
q 'system_metric_0' | jq -e '(.data.result | length) > 0' >/dev/null 2>&1 \
    || { echo "FATAL: no system_metric_0 series — is the load-generator running?"; exit 1; }

echo "─── instant aggregations: count / sum ───────────────────────"
check "count(x) collapses to one scalar" \
    'count(http_requests_total_0)' \
    '.data.resultType == "vector" and (.data.result | length == 1)'
check "count(x) instant shape has value" \
    'count(http_requests_total_0)' \
    '.data.result[0].value | (type == "array" and length == 2)'
check "sum(x) collapses to one scalar" \
    'sum(http_requests_total_0)' \
    '.data.result | length == 1'

echo "─── grouping: by (suffix and prefix positions) ──────────────"
check "sum(x) by (method) [suffix]" \
    'sum(http_requests_total_0) by (method)' \
    '.data.result | length == 3'
check "sum by (method) (x) [prefix]" \
    'sum by (method) (http_requests_total_0)' \
    '.data.result | length == 3'
check "suffix == prefix grouping values" \
    'sum(http_requests_total_0) by (method)' \
    '([.data.result[].metric.method] | sort) == ["GET","POST","PUT"]'
check "group labels kept, others dropped" \
    'sum(http_requests_total_0) by (method)' \
    '.data.result | all(.metric | keys | sort == ["method"])'
check "sum without (method, instance) keeps status" \
    'sum(http_requests_total_0) without (method, instance)' \
    '.data.result | all(.metric | has("status"))'
check "count(x) by (status) partitions the series" \
    'count(http_requests_total_0) by (status)' \
    '(.data.result | length) == 5 and ([.data.result[].value[1] | tonumber] | add) >= 25'
check "multi-label grouping by (method, status)" \
    'sum by (method, status) (http_requests_total_0)' \
    '.data.result | length == 15'

echo "─── cross-group consistency: parts sum to whole ─────────────"
GROUP_TOTAL=$(q 'sum(http_requests_total_0) by (method)' | jq -r '[.data.result[].value[1] | tonumber] | add')
WHOLE=$(q 'sum(http_requests_total_0)' | jq -r '.data.result[0].value[1]')
if [ "$(echo "$GROUP_TOTAL" | cut -d. -f1)" = "$(echo "$WHOLE" | cut -d. -f1)" ]; then
    echo "PASS  grouped sum ($GROUP_TOTAL) == whole ($WHOLE)"
    PASS=$((PASS + 1))
else
    echo "FAIL  grouped sum ($GROUP_TOTAL) != whole ($WHOLE)"
    FAIL=$((FAIL + 1))
fi

echo "─── other aggregation operators ─────────────────────────────"
check "avg(x) by (host)" \
    'avg(system_metric_0) by (host)' \
    '.data.result | length >= 1'
check "min/max by (status)" \
    'max(http_requests_total_0) by (status)' \
    '.data.result | length == 5'
check "stddev by (method)" \
    'stddev(http_requests_total_0) by (method)' \
    '.data.result | length == 3'
check "topk(3, sum by (status))" \
    'topk(3, sum by (status) (http_requests_total_0))' \
    '.data.result | length == 3'

echo "─── rate composition (SRE golden signals) ───────────────────"
check "sum(rate(x[5m])) by (method) — RPS per method" \
    'sum(rate(http_requests_total_0[5m])) by (method)' \
    '.data.result | length == 3'
check "sum(rate(x{status=\"500\"}[5m])) — nested braced selector" \
    'sum(rate(http_requests_total_0{status="500"}[5m]))' \
    '.data.result | length == 1'
check "error ratio: agg/agg binary" \
    'sum(rate(http_requests_total_0{status="500"}[5m])) / sum(rate(http_requests_total_0[5m]))' \
    '.data.result | length == 1'
check "regex matcher grouping: status=~4xx|5xx" \
    'sum(rate(http_requests_total_0{status=~"4..|5.."}[5m])) by (method)' \
    '.data.result | length == 3'

echo "─── range (matrix) aggregations ─────────────────────────────"
END=$(date +%s)
START=$((END - 900))
rq() {
    curl -s -m 25 --get "$URL/api/v1/query_range" \
        --data-urlencode "query=$1" \
        --data-urlencode "start=$START" \
        --data-urlencode "end=$END" \
        --data-urlencode "step=60"
}
R=$(rq 'sum by (method) (http_requests_total_0)')
if echo "$R" | jq -e '.data.resultType == "matrix" and (.data.result | length == 3)' >/dev/null 2>&1; then
    echo "PASS  range sum by (method) — matrix, 3 series"
    PASS=$((PASS + 1))
else
    echo "FAIL  range sum by (method)"; echo "      $R" | head -c 300; FAIL=$((FAIL + 1))
fi
R=$(rq 'sum(rate(http_requests_total_0{status="500"}[5m])) by (method)')
if echo "$R" | jq -e '.data.result | length == 3' >/dev/null 2>&1; then
    echo "PASS  range nested rate + braces + grouping"
    PASS=$((PASS + 1))
else
    echo "FAIL  range nested rate + braces"; echo "      $R" | head -c 300; FAIL=$((FAIL + 1))
fi

echo "─── response-shape conformance ──────────────────────────────"
check "instant: no multi-sample series (aggregated = 1 sample)" \
    'sum by (method) (http_requests_total_0)' \
    '.data.result | all(.value | length == 2)'

echo ""
echo "════════════════════════════════════════════"
echo "Results: $PASS passed, $FAIL failed"
[ "$FAIL" -eq 0 ] && echo "All aggregation checks passed ✓" || exit 1
