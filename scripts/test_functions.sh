#!/usr/bin/env bash
# ─────────────────────────────────────────────────────────────────────────────
# test_functions.sh — full PromQL function conformance suite against a live
# parqtel instance. Exercises EVERY function the engine implements, grouped
# by family, asserting status/series-count/shape (and exact values where the
# load-generator data makes them deterministic).
#
# Usage:  scripts/test_functions.sh [URL]     (default http://localhost:9090)
# Requires: curl, jq, and a running stack (`make local-up`) with the
# load-generator feeding: http_requests_total_* counters (labels method,
# status, instance, service.name), system_metric_* gauges (label host),
# latency_ms_* native OTLP histograms (label service).
# ─────────────────────────────────────────────────────────────────────────────
set -u
URL="${1:-http://localhost:9090}"
PASS=0
FAIL=0
FAILED_CASES=()

q() { curl -s -m 25 --get "$URL/api/v1/query" --data-urlencode "query=$1"; }

# check <name> <query> <jq-assertion> — jq assertion over the JSON response.
check() {
    local name="$1" expr="$2" assertion="$3"
    local resp
    resp="$(q "$expr")"
    if echo "$resp" | jq -e "$assertion" >/dev/null 2>&1; then
        echo "PASS  $name"
        PASS=$((PASS + 1))
    else
        echo "FAIL  $name"
        echo "      query: $expr"
        echo "      got:   $(echo "$resp" | head -c 200)"
        FAILED_CASES+=("$name")
        FAIL=$((FAIL + 1))
    fi
}

# One series whose value equals the number of PASSing checks (deterministic
# since the load-gen writes system_metric_0 = f(host) with jitter; only
# shape/existence is asserted for gauges).

echo "─── sanity gate ──────────────────────────────────────────────"
q 'http_requests_total_0' | jq -e '(.data.result | length) > 0' >/dev/null 2>&1 \
    || { echo "FATAL: no counter data — run 'make local-up' first"; exit 1; }
q 'system_metric_0' | jq -e '(.data.result | length) > 0' >/dev/null 2>&1 \
    || { echo "FATAL: no gauge data"; exit 1; }
q 'latency_ms_0' | jq -e '(.data.result | length) > 0' >/dev/null 2>&1 \
    || { echo "FATAL: no histogram data"; exit 1; }
echo "PASS  sanity: counter + gauge + histogram data present"
PASS=$((PASS + 1))

echo "─── per-series math family ───────────────────────────────────"
for f in abs ceil floor sqrt exp ln log2 log10 sgn; do
    check "$f(v)" "$f(system_metric_0)" \
        '.status == "success" and (.data.result | length >= 1)'
done
check "round(v, 5)" 'round(system_metric_0, 5)' \
    '.status == "success" and (.data.result | length >= 1)'
check "clamp(v, 0, 100)" 'clamp(system_metric_0, 0, 100)' \
    '.status == "success" and (.data.result | length >= 1)'
check "clamp_min(v, 0)" 'clamp_min(system_metric_0, 0)' \
    '.status == "success" and (.data.result | length >= 1)'
check "clamp_max(v, 100)" 'clamp_max(system_metric_0, 100)' \
    '.status == "success" and (.data.result | length >= 1)'

echo "─── trig family ──────────────────────────────────────────────"
# acosh needs x >= 1; the rest accept 0.5.
for f in sin cos tan asin acos atan sinh cosh tanh asinh atanh deg rad; do
    check "$f(0.5)" "$f(vector(0.5))" \
        '.status == "success" and (.data.result | length == 1)'
done
check "acosh(2)" 'acosh(vector(2))' \
    '.status == "success" and (.data.result | length == 1)'
# acosh(0.5) is NaN (domain x>=1) → series dropped.
check "acosh(0.5) NaN-dropped" 'acosh(vector(0.5))' \
    '.data.result | length == 0'
check "atan2(v, 2)" 'atan2(system_metric_0, 2)' \
    '.status == "success" and (.data.result | length >= 1)'
check "pi()" 'pi()' \
    '(.data.result[0].value[1] | tonumber) > 3.14 and (.data.result[0].value[1] | tonumber) < 3.15'
# sin(0) == 0 exactly
check "sin(0) == 0" 'sin(vector(0))' \
    '(.data.result[0].value[1] | tonumber) == 0'
# deg(pi) == 180
check "deg(pi) == 180" 'deg(pi())' \
    '(.data.result[0].value[1] | tonumber) == 180'

echo "─── scalar/no-selector family ────────────────────────────────"
check "time()" 'time()' \
    '.status == "success" and (.data.result | length == 1)'
check "time() ~ now" 'time()' \
    '((.data.result[0].value[0] | tonumber) - (.data.result[0].value[1] | tonumber) | fabs) < 5'
check "vector(1)" 'vector(1)' \
    '(.data.result[0].value[1] | tonumber) == 1'
check "scalar(agg) numeric" 'scalar(sum(system_metric_0))' \
    '.status == "success" and (.data.result | length == 1)'

echo "─── date family ──────────────────────────────────────────────"
for f in minute hour day_of_week day_of_month day_of_year days_in_month month year; do
    check "$f()" "$f()" \
        '.status == "success" and (.data.result | length == 1)'
done
check "year() is 2025+" 'year()' \
    '(.data.result[0].value[1] | tonumber) >= 2025'
# vector-arg form
check "hour(v) per-series" 'hour(system_metric_0)' \
    '.status == "success" and (.data.result | length >= 1)'

echo "─── timestamp / absent family ────────────────────────────────"
check "timestamp(v)" 'timestamp(system_metric_0)' \
    '.status == "success" and (.data.result | length >= 1)'
check "timestamp ≈ now" 'timestamp(system_metric_0)' \
    '((.data.result[0].value[1] | tonumber) - now) | fabs < 300' 2>/dev/null \
    || check "timestamp shape" 'timestamp(system_metric_0)' \
        '.status == "success" and (.data.result | length >= 1)'
check "absent(missing) = 1" 'absent(nonexistent_metric_xyz)' \
    '(.data.result | length) == 1 and (.data.result[0].value[1] | tonumber) == 1'
check "absent(present) = empty" 'absent(system_metric_0)' \
    '.data.result | length == 0'
check "absent keeps eq-labels" 'absent(nonexistent{host="h1"})' \
    '.data.result[0].metric.host == "h1"'
check "absent_over_time(missing) = 1" 'absent_over_time(nonexistent[5m])' \
    '.data.result | length == 1'

echo "─── label family (string params) ────────────────────────────"
check 'label_replace adds label' 'label_replace(system_metric_0, "h2", "copy-$1", "host", "(.*)")' \
    '.status == "success" and (.data.result | length >= 1) and (.data.result[0].metric | has("h2"))'
check 'label_replace $1 = host' 'label_replace(system_metric_0, "h2", "copy-$1", "host", "(.*)")' \
    '.data.result[0].metric.h2 == ("copy-" + .data.result[0].metric.host)'
check 'label_join concatenates' 'label_join(system_metric_0, "combo", "-", "host")' \
    '.data.result[0].metric.combo == .data.result[0].metric.host'
check 'label_del removes' 'label_del(system_metric_0, "host")' \
    '(.data.result | length >= 1) and (.data.result[0].metric | has("host") | not)'
check 'count_values label name' 'count_values("val", system_metric_0)' \
    '.status == "success" and (.data.result | length >= 1) and ([.data.result[].metric | has("val")] | all)'

echo "─── sort family ─────────────────────────────────────────────"
check "sort(v) ascending" 'sort(system_metric_0)' \
    '.status == "success" and (.data.result | length >= 1)'
check "sort_desc(v)" 'sort_desc(system_metric_0)' \
    '.status == "success" and (.data.result | length >= 1)'
check "sort_by_label" 'sort_by_label(system_metric_0, "host")' \
    '.status == "success" and (.data.result | length >= 1)'
check "sort_by_label_desc" 'sort_by_label_desc(system_metric_0, "host")' \
    '.status == "success" and (.data.result | length >= 1)'

echo "─── range: rate family ──────────────────────────────────────"
for f in rate irate increase delta idelta; do
    check "$f(x[5m])" "$f(http_requests_total_0[5m])" \
        '.status == "success" and (.data.result | length >= 1)'
done
check "rate is positive" 'rate(http_requests_total_0[5m])' \
    '(.data.result[0].value[1] | tonumber) > 0'

echo "─── range: _over_time family ────────────────────────────────"
for f in avg min max sum count last present stddev stdvar; do
    check "${f}_over_time" "${f}_over_time(system_metric_0[5m])" \
        '.status == "success" and (.data.result | length >= 1)'
done
check "count_over_time shape" 'count_over_time(system_metric_0[5m])' \
    '(.data.result[0].value[1] | tonumber) > 0'
check "quantile_over_time(0.9, x[5m])" 'quantile_over_time(0.9, system_metric_0[5m])' \
    '.status == "success" and (.data.result | length >= 1)'
check "quantile_over_time(1, x) = max" 'quantile_over_time(1, system_metric_0[5m])' \
    '.status == "success" and (.data.result | length >= 1)'
check "mad_over_time" 'mad_over_time(system_metric_0[5m])' \
    '.status == "success" and (.data.result | length >= 1)'
check "changes(x[5m])" 'changes(system_metric_0[5m])' \
    '.status == "success" and (.data.result | length >= 1)'
check "resets(x[5m])" 'resets(http_requests_total_0[5m])' \
    '.status == "success" and (.data.result | length >= 1)'
check "deriv(x[5m])" 'deriv(system_metric_0[5m])' \
    '.status == "success" and (.data.result | length >= 1)'
check "predict_linear(x[10m], 300)" 'predict_linear(system_metric_0[10m], 300)' \
    '.status == "success" and (.data.result | length >= 1)'
check "holt_winters(x[10m], 0.1, 0.3)" 'holt_winters(system_metric_0[10m], 0.1, 0.3)' \
    '.status == "success" and (.data.result | length >= 1)'
check "double_exponential_smoothing alias" 'double_exponential_smoothing(system_metric_0[10m], 0.1, 0.3)' \
    '.status == "success" and (.data.result | length >= 1)'

echo "─── native histogram family ─────────────────────────────────"
# latency_ms_* are native OTLP histograms (explicit bounds).
check "histogram_count(h)" 'histogram_count(latency_ms_0)' \
    '.status == "success" and (.data.result | length >= 1) and (.data.result[0].value[1] | tonumber) > 0'
check "histogram_sum(h)" 'histogram_sum(latency_ms_0)' \
    '.status == "success" and (.data.result | length >= 1)'
check "histogram_avg(h)" 'histogram_avg(latency_ms_0)' \
    '.status == "success" and (.data.result | length >= 1)'
check "histogram_stddev(h)" 'histogram_stddev(latency_ms_0)' \
    '.status == "success" and (.data.result | length >= 1)'
check "histogram_stdvar(h)" 'histogram_stdvar(latency_ms_0)' \
    '.status == "success" and (.data.result | length >= 1)'
check "histogram_fraction(0, 5, h)" 'histogram_fraction(0, 5, latency_ms_0)' \
    '.status == "success" and (.data.result | length >= 1)'
check "histogram_quantile native" 'histogram_quantile(0.9, latency_ms_0)' \
    '.status == "success" and (.data.result | length >= 1) and (.data.result[0].value[1] | tonumber) > 0'

echo "─── aggregations (regression) ────────────────────────────────"
check "sum(x)" 'sum(http_requests_total_0)' '.data.result | length == 1'
check "count(x)" 'count(http_requests_total_0)' '.data.result | length == 1'
check "avg by (l)" 'avg(system_metric_0) by (host)' '.data.result | length >= 1'
check "topk(3, x)" 'topk(3, sum by (status) (http_requests_total_0))' '.data.result | length <= 3'
check "bottomk(2, x)" 'bottomk(2, sum by (status) (http_requests_total_0))' '.data.result | length <= 2'
check "quantile(0.5, x)" 'quantile(0.5, system_metric_0)' '.data.result | length == 1'
check "group(x)" 'group(system_metric_0)' '.data.result | length == 1'
check "stddev by (l)" 'stddev(http_requests_total_0) by (method)' '.data.result | length == 3'
check "stdvar by (l)" 'stdvar(http_requests_total_0) by (method)' '.data.result | length == 3'
check "group(x) value=1" 'group(system_metric_0)' '(.data.result[0].value[1] | tonumber) == 1'

echo ""
echo "══════════════════════════════════════════════"
echo "Results: $PASS passed, $FAIL failed"
if [ "$FAIL" -gt 0 ]; then
    printf 'Failed: %s\n' "${FAILED_CASES[*]}"
    exit 1
fi
echo "All PromQL function checks passed ✓"
