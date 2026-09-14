#!/usr/bin/env bash
# ─────────────────────────────────────────────────────────────────────────────
# test_builder.sh — E2E validation of every PromQL query shape the UI query
# builder generates, against a live parqtel instance. Mirrors the builder's
# function catalog (aggregations with grouping/window fns, rate family,
# _over_time family, instant transforms, label fns, scalar fns, histogram
# fns, high-cardinality filters).
#
# Usage:  scripts/test_builder.sh [URL]     (default http://localhost:9090)
# Requires: make local-up (load-generator feeding all metric families,
# including the high-cardinality user_sessions_active_*)
# ─────────────────────────────────────────────────────────────────────────────
set -u
URL="${1:-http://localhost:9090}"
PASS=0
FAIL=0
FAILED_CASES=()

q() { curl -s -m 25 --get "$URL/api/v1/query" --data-urlencode "query=$1"; }

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

echo "─── sanity gate ──────────────────────────────────────────────"
q 'user_sessions_active_0' | jq -e '(.data.result | length) > 0' >/dev/null 2>&1 \
    || { echo "FATAL: no high-cardinality data — run 'make local-up' first"; exit 1; }
echo "PASS  sanity: high-cardinality metric present"
PASS=$((PASS + 1))

echo "─── raw selector + label filters (incl. high-card) ───────────"
check "raw selector" 'user_sessions_active_0' '.status=="success" and (.data.result|length)>0'
check "filter eq (high-card)" 'user_sessions_active_0{user_id=~".+"}' '.status=="success" and (.data.result|length)>0'
check "filter regex" 'http_requests_total_0{method=~"GET|POST"}' '.status=="success" and (.data.result|length)>0'
check "filter not-eq" 'http_requests_total_0{method!="GET"}' '.status=="success" and (.data.result|length)>0'
check "filter not-regex" 'http_requests_total_0{method!~"PUT"}' '.status=="success" and (.data.result|length)>0'
check "multi-filter" 'http_requests_total_0{method="GET",status="200"}' '.status=="success"'

echo "─── aggregations + grouping ──────────────────────────────────"
check "sum plain" 'sum(http_requests_total_0)' '.status=="success" and (.data.result|length)==1'
check "sum by (method)" 'sum by (method) (http_requests_total_0)' '.status=="success" and (.data.result|length)==3'
check "sum by service.name" 'sum by (service.name) (http_requests_total_0)' '.status=="success"'
check "sum without ()" 'sum without () (http_requests_total_0)' '.status=="success"'
check "avg by (host)" 'avg by (host) (system_metric_0)' '.status=="success"'
check "max" 'max(system_metric_0)' '.status=="success" and (.data.result|length)==1'
check "group" 'group(system_metric_0)' '(.data.result[0].value[1]|tonumber)==1'
check "stddev by" 'stddev by (method) (http_requests_total_0)' '.status=="success"'
check "stdvar by" 'stdvar by (method) (http_requests_total_0)' '.status=="success"'
check "topk k=3" 'topk(3, sum by (status) (http_requests_total_0))' '.data.result|length<=3'
check "bottomk k=2" 'bottomk(2, sum by (status) (http_requests_total_0))' '.data.result|length<=2'
check "quantile φ=0.9" 'quantile(0.9, system_metric_0)' '.status=="success" and (.data.result|length)==1'
check "count_values" 'count_values("val", system_metric_0)' '[.data.result[].metric.val] | length >= 1'
check "sum(rate(m[5m])) grouped" 'sum by (method) (rate(http_requests_total_0[5m]))' '.status=="success" and (.data.result|length)==3'
check "sum(increase(m[1h]))" 'sum by (status) (increase(http_requests_total_0[1h]))' '.status=="success"'

echo "─── rate family (windowed) ───────────────────────────────────"
for f in rate irate increase delta idelta; do
    m='http_requests_total_0'; [ "$f" = delta ] && m='system_metric_0'
    check "$f(m[5m])" "$f($m[5m])" '.status=="success" and (.data.result|length)>0'
done

echo "─── _over_time family ───────────────────────────────────────"
for f in avg min max sum count last present stddev stdvar; do
    check "${f}_over_time" "${f}_over_time(system_metric_0[5m])" '.status=="success" and (.data.result|length)>0'
done
check "quantile_over_time(0.9, m[5m])" 'quantile_over_time(0.9, system_metric_0[5m])' '.status=="success"'
check "mad_over_time" 'mad_over_time(system_metric_0[5m])' '.status=="success"'
check "predict_linear(m[10m], 300)" 'predict_linear(system_metric_0[10m], 300)' '.status=="success"'
check "holt_winters(m[10m], 0.1, 0.3)" 'holt_winters(system_metric_0[10m], 0.1, 0.3)' '.status=="success"'
check "deriv" 'deriv(system_metric_0[5m])' '.status=="success"'
check "changes" 'changes(system_metric_0[5m])' '.status=="success"'
check "resets" 'resets(http_requests_total_0[5m])' '.status=="success"'

echo "─── instant transforms ──────────────────────────────────────"
for f in abs ceil floor sqrt exp ln log2 log10 sgn; do
    check "$f(m)" "$f(system_metric_0)" '.status=="success"'
done
check "round(m, 5)" 'round(system_metric_0, 5)' '.status=="success"'
check "clamp(m, 0, 100)" 'clamp(system_metric_0, 0, 100)' '[.data.result[].value[1]|tonumber] | (length==0) or (all(.[]; . <= 100 and . >= 0))'
check "clamp_min(m, 0)" 'clamp_min(system_metric_0, 0)' '.status=="success"'
check "clamp_max(m, 100)" 'clamp_max(system_metric_0, 100)' '[.data.result[].value[1]|tonumber] | (length==0) or all(.[]; . <= 100)'
check "atan2(m, 2)" 'atan2(system_metric_0, 2)' '.status=="success"'
for f in sin cos tan asin acos atan sinh cosh tanh asinh deg rad; do
    check "$f(vector(0.5))" "$f(vector(0.5))" '.status=="success" and (.data.result|length)==1'
done

echo "─── label functions ─────────────────────────────────────────"
check 'label_replace' 'label_replace(system_metric_0, "h2", "copy-$1", "host", "(.*)")' '.data.result[0].metric.h2=="copy-host-0"'
check 'label_join (multi src)' 'label_join(http_requests_total_0, "combo", "-", "method", "status")' '.data.result[0].metric.combo|test("-")'
check 'label_del' 'label_del(system_metric_0, "host")' '(.data.result[0].metric|has("host"))|not'
check 'sort_by_label' 'sort_by_label(system_metric_0, "host")' '.status=="success"'
check 'sort_by_label_desc' 'sort_by_label_desc(system_metric_0, "host")' '.status=="success"'
check 'sort / sort_desc' 'sort(system_metric_0)' '.status=="success"'

echo "─── scalar / time functions ─────────────────────────────────"
check "scalar(sum(m))" 'scalar(sum(system_metric_0))' '.status=="success"'
check "timestamp(m)" 'timestamp(system_metric_0)' '.status=="success"'
check "vector(1)" 'vector(1)' '(.data.result[0].value[1]|tonumber)==1'
check "time()" 'time()' '.status=="success" and (.data.result|length)==1'
check "pi()" 'pi()' '(.data.result[0].value[1]|tonumber)>3.14'
for f in minute hour day_of_week day_of_month day_of_year days_in_month month year; do
    check "$f()" "$f()" '.status=="success" and (.data.result|length)==1'
done

echo "─── histogram functions (native OTLP) ────────────────────────"
check "histogram_quantile" 'histogram_quantile(0.9, latency_ms_0)' '.status=="success" and (.data.result|length)>0'
check "histogram_fraction(0, 1, m)" 'histogram_fraction(0, 1, latency_ms_0)' '.status=="success"'
check "histogram_count" 'histogram_count(latency_ms_0)' '.status=="success"'
check "histogram_sum" 'histogram_sum(latency_ms_0)' '.status=="success"'
check "histogram_avg" 'histogram_avg(latency_ms_0)' '.status=="success"'
check "histogram_stddev" 'histogram_stddev(latency_ms_0)' '.status=="success"'
check "histogram_stdvar" 'histogram_stdvar(latency_ms_0)' '.status=="success"'

echo "─── high-cardinality paths ───────────────────────────────────"
check "avg by (tier) high-card" 'avg by (tier) (user_sessions_active_0)' '.data.result|length==3'
check "topk on high-card" 'topk(3, user_sessions_active_0)' '.data.result|length<=3'
check "windowed on high-card" 'avg(avg_over_time(user_sessions_active_1[5m]))' '.data.result|length==1'
check "exact high-card filter" 'user_sessions_active_0{user_id=~"user-00042.*"}' '.status=="success"'

echo ""
echo "══════════════════════════════════════════════"
echo "Results: $PASS passed, $FAIL failed"
if [ "$FAIL" -gt 0 ]; then
    printf 'Failed: %s\n' "${FAILED_CASES[*]}"
    exit 1
fi
echo "All builder query shapes valid ✓"
