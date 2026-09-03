#!/usr/bin/env python3
"""parqtel query/ingestion benchmark runner.

Runs a fixed query suite against a seeded server and records latency
percentiles. Designed to be run at the end of every query-language phase;
results are appended to scripts/bench_results/ for regression diffing.

Query suite covers: selector scans, label-matched scans, aggregations
with by-grouping, windowed rate (the Phase-0 fix), histogram_quantile,
topk, instant vs range, logs search + severity, trace search.

Usage:
  python3 bench_query.py [http_addr] [--label phase0]
"""
import json
import os
import statistics
import sys
import time
import urllib.request
import urllib.parse
from concurrent.futures import ThreadPoolExecutor

ADDR = sys.argv[1] if len(sys.argv) > 1 and not sys.argv[1].startswith("--") else "http://127.0.0.1:14318"
LABEL = "phase0"
for a in sys.argv[2:]:
    if a.startswith("--label"):
        LABEL = a.split("=", 1)[1] if "=" in a else sys.argv[-1]
RESULTS_DIR = os.path.join(os.path.dirname(__file__), "bench_results")
RUNS_PER_QUERY = 5
RANGE_SPAN_S = 3000  # ~50min of data


def q(path, params=None):
    url = ADDR + path
    if params:
        url += "?" + urllib.parse.urlencode(params)
    t0 = time.perf_counter()
    try:
        with urllib.request.urlopen(url, timeout=120) as r:
            body = r.read()
            dt = time.perf_counter() - t0
            try:
                d = json.loads(body).get("data", {})
                if "result" in d:          # prom-style metrics
                    n_series = len(d["result"])
                elif "logs" in d:          # logs
                    n_series = len(d["logs"])
                elif "spans" in d:         # traces
                    n_series = len(d["spans"])
                else:
                    n_series = len(d) if isinstance(d, list) else -1
            except Exception:
                n_series = -1
            return dt, n_series, None
    except Exception as e:
        return time.perf_counter() - t0, -1, str(e)


def pct(lat, p):
    ls = sorted(lat)
    idx = min(int(p / 100 * len(ls)), len(ls) - 1)
    return ls[idx]


def run_suite():
    now = int(time.time())
    start = now - RANGE_SPAN_S
    suite = [
        # name, path, params
        ("selector_all", "/api/v1/query_range", {"query": "http_requests_total", "start": start, "end": now, "step": "120s"}),
        ("selector_match", "/api/v1/query_range", {"query": 'http_requests_total{service.name="api-gateway"}', "start": start, "end": now, "step": "120s"}),
        ("selector_regex", "/api/v1/query_range", {"query": 'http_requests_total{service.name=~"api-.*"}', "start": start, "end": now, "step": "120s"}),
        ("sum_by_service", "/api/v1/query_range", {"query": 'sum by (service.name) (http_requests_total)', "start": start, "end": now, "step": "120s"}),
        ("avg_gauge", "/api/v1/query_range", {"query": "avg(cpu_usage)", "start": start, "end": now, "step": "120s"}),
        ("rate_5m", "/api/v1/query_range", {"query": "rate(http_requests_total[5m])", "start": start, "end": now, "step": "120s"}),
        ("rate_1m_vs_1h", "/api/v1/query_range", {"query": "rate(http_requests_total[1h])", "start": start, "end": now, "step": "120s"}),
        ("increase_5m", "/api/v1/query_range", {"query": "increase(http_requests_total[5m])", "start": start, "end": now, "step": "120s"}),
        ("topk", "/api/v1/query", {"query": "topk(10, http_requests_total)", "time": now}),
        ("instant_selector", "/api/v1/query", {"query": 'http_request_duration_seconds{service.name="api-gateway"}', "time": now}),
        ("labels_api", "/api/v1/labels", None),
        ("label_values", "/api/v1/label/service.name/values", None),
        ("logs_plain", "/api/v1/logs", {"query": "{}", "start": start, "end": now, "limit": 500}),
        ("logs_sev", "/api/v1/logs", {"query": "{}", "start": start, "end": now, "limit": 500, "severity_min": 13}),
        ("logs_search", "/api/v1/logs", {"query": "{}", "start": start, "end": now, "limit": 500, "search": "timeout"}),
        ("traces_search", "/v1/traces/search", {"start": start, "end": now}),
        # Phase 1A composed queries
        ("ast_sum_rate", "/api/v1/query_range", {"query": "sum(rate(http_requests_total[5m]))", "start": start, "end": now, "step": "120s"}),
        ("ast_sum_rate_by_svc", "/api/v1/query_range", {"query": "sum by (service.name) (rate(http_requests_total[5m]))", "start": start, "end": now, "step": "120s"}),
        ("ast_ratio", "/api/v1/query_range", {"query": "sum(rate(traces_service_errors_total[5m])) / sum(rate(traces_service_requests_total[5m]))", "start": start, "end": now, "step": "120s"}),
        ("ast_avg_over_time", "/api/v1/query_range", {"query": "avg_over_time(cpu_usage[5m])", "start": start, "end": now, "step": "120s"}),
        ("ast_binary_scalar", "/api/v1/query_range", {"query": "avg(cpu_usage) * 100 > 40", "start": start, "end": now, "step": "120s"}),
        ("ast_topk_rate", "/api/v1/query_range", {"query": "topk(5, rate(http_requests_total[5m]))", "start": start, "end": now, "step": "120s"}),
        # Phase 1B ParqtelQL
        ("pql_terms", "/api/v1/logs", {"query": "timeout", "start": start, "end": now, "limit": 500}),
        ("pql_exclude", "/api/v1/logs", {"query": "timeout -refused", "start": start, "end": now, "limit": 500}),
        ("pql_field", "/api/v1/logs", {"query": "service=api-gateway", "start": start, "end": now, "limit": 500}),
        ("pql_severity", "/api/v1/logs", {"query": "severity>=ERROR", "start": start, "end": now, "limit": 500}),
        ("pql_combined", "/api/v1/logs", {"query": "service=api-gateway severity>=ERROR timeout", "start": start, "end": now, "limit": 500}),
        ("pql_trace_q", "/v1/traces/search", {"start": start, "end": now, "q": "status=ERROR"}),
        ("pql_trace_dur", "/v1/traces/search", {"start": start, "end": now, "q": "duration>100"}),
    ]

    results = {}

    # Pipeline queries run over POST /v1/search (different shape — runner).
    def pipeline_query(name, query):
        import urllib.request as _r
        body = json.dumps({"query": query, "start": start, "end": now}).encode()
        req = _r.Request(ADDR + "/v1/search", data=body, headers={"Content-Type": "application/json"}, method="POST")
        lats, err = [], None
        for _ in range(RUNS_PER_QUERY):
            t0 = time.perf_counter()
            try:
                with _r.urlopen(req, timeout=120) as resp:
                    data = json.loads(resp.read())
                    n = (len(data.get("data", {}).get("rows", []))
                         or len(data.get("data", {}).get("timeseries", {}).get("series", []))
                         or -1)
            except Exception as e:
                n, err = -1, str(e)
            lats.append((time.perf_counter() - t0) * 1000.0)
        results[name] = {
            "p50_ms": round(pct(lats, 50), 1),
            "p95_ms": round(pct(lats, 95), 1),
            "p99_ms": round(pct(lats, 99), 1),
            "mean_ms": round(statistics.mean(lats), 1),
            "series_or_rows": n,
            "runs": len(lats),
            "error": err,
        }
        status = "ERR " + str(err)[:40] if err else f"p50={results[name]['p50_ms']}ms"
        print(f"  {name:22s} {status} rows={n}")

    pipeline_query("pipe_count_by_service", "fetch logs | stats count() by service")
    pipeline_query("pipe_filter_or_stats", "fetch logs | filter service=api-gateway OR severity>=ERROR | stats count()")
    pipeline_query("pipe_parse_p95", r'fetch logs | parse "duration_ms=(\d+)" as dur | stats p95(dur) by service')
    pipeline_query("pipe_fetch_metrics", "fetch metrics | stats sum(value) by service")
    pipeline_query("pipe_fetch_traces", "fetch traces | stats p95(duration_ms) by service")

    for name, path, params in suite:
        lats, series, err = [], None, None
        for _ in range(RUNS_PER_QUERY):
            dt, n, e = q(path, params)
            if e and err is None:
                err = e
            lats.append(dt * 1000.0)
            series = n
        results[name] = {
            "p50_ms": round(pct(lats, 50), 1),
            "p95_ms": round(pct(lats, 95), 1),
            "p99_ms": round(pct(lats, 99), 1),
            "mean_ms": round(statistics.mean(lats), 1),
            "series_or_rows": series,
            "runs": len(lats),
            "error": err,
        }
        status = "ERR" if err else f"p50={results[name]['p50_ms']}ms p95={results[name]['p95_ms']}ms"
        print(f"  {name:22s} {status} rows={series}")
    return results


def server_stats():
    out = {}
    try:
        with urllib.request.urlopen(ADDR + "/metrics", timeout=30) as r:
            for line in r.read().decode().splitlines():
                if line.startswith(("parqtel_queries_executed_total", "parqtel_query_duration_ms_count",
                                    "parqtel_ingested_points_total", "parqtel_batches_received_total")):
                    k, v = line.rsplit(" ", 1)
                    out[k.split("{")[0]] = v
    except Exception as e:
        out["error"] = str(e)
    return out


def main():
    print(f"[bench] target {ADDR} label={LABEL}")
    t0 = time.time()
    results = run_suite()
    report = {
        "label": LABEL,
        "timestamp": time.strftime("%Y-%m-%dT%H:%M:%S"),
        "target": ADDR,
        "runs_per_query": RUNS_PER_QUERY,
        "results": results,
        "server_stats": server_stats(),
        "total_wall_s": round(time.time() - t0, 1),
    }
    os.makedirs(RESULTS_DIR, exist_ok=True)
    path = os.path.join(RESULTS_DIR, f"{LABEL}.json")
    with open(path, "w") as f:
        json.dump(report, f, indent=2)
    print(f"[bench] saved {path} (wall {report['total_wall_s']}s)")


if __name__ == "__main__":
    main()
