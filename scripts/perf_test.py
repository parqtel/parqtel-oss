#!/usr/bin/env python3
"""
Parqtel Performance Test — 1000 samples/sec for 15 minutes
Measures: ingestion p99 latency, query p99 latency, throughput
"""
import time
import json
import random
import string
import statistics
import threading
import urllib.request
import urllib.error
from concurrent.futures import ThreadPoolExecutor
from collections import defaultdict

BASE_URL = "http://localhost:9095"
RATE = 1000          # samples/second
DURATION_SEC = 900   # 15 minutes
BATCH_SIZE = 50      # samples per HTTP request
QUERY_INTERVAL = 5   # query every 5 seconds
SERVICES = ["api-gateway", "order-service", "payment-service", "user-service", "inventory-service"]
ENDPOINTS = ["/api/v1/users", "/api/v1/orders", "/api/v1/payments", "/api/v1/products", "/health"]

# Results storage
ingest_latencies = {"metrics": [], "logs": [], "traces": []}
query_latencies = {"metrics": [], "logs": [], "instant": []}
errors = defaultdict(int)
total_ingested = {"metrics": 0, "logs": 0, "traces": 0}
lock = threading.Lock()


def post_json(url, data):
    """POST JSON and return latency in ms."""
    body = json.dumps(data).encode()
    req = urllib.request.Request(url, data=body, headers={"Content-Type": "application/json"})
    start = time.perf_counter()
    try:
        with urllib.request.urlopen(req, timeout=10) as resp:
            resp.read()
        return (time.perf_counter() - start) * 1000
    except Exception as e:
        with lock:
            errors[str(type(e).__name__)] += 1
        return None


def get_json(url):
    """GET and return (latency_ms, response_json)."""
    req = urllib.request.Request(url)
    start = time.perf_counter()
    try:
        with urllib.request.urlopen(req, timeout=10) as resp:
            data = json.loads(resp.read())
        return (time.perf_counter() - start) * 1000, data
    except Exception as e:
        with lock:
            errors[f"query_{type(e).__name__}"] += 1
        return None, None


def gen_metrics_batch(batch_size):
    """Generate a batch of OTLP metrics JSON."""
    now_ns = int(time.time() * 1e9)
    svc = random.choice(SERVICES)
    metrics = []
    for _ in range(batch_size):
        metrics.append({
            "name": random.choice(["http_requests_total", "http_request_duration_seconds",
                                   "cpu_usage_percent", "memory_usage_mb", "active_connections",
                                   "error_rate", "queue_depth", "gc_pause_ms"]),
            "gauge": {"dataPoints": [{
                "asDouble": random.uniform(0, 500),
                "timeUnixNano": str(now_ns + random.randint(-1000000000, 0)),
                "attributes": [
                    {"key": "method", "value": {"stringValue": random.choice(["GET", "POST", "PUT"])}},
                    {"key": "status", "value": {"stringValue": random.choice(["200", "201", "400", "500"])}},
                    {"key": "endpoint", "value": {"stringValue": random.choice(ENDPOINTS)}},
                ]
            }]}
        })
    return {"resourceMetrics": [{"resource": {"attributes": [
        {"key": "service.name", "value": {"stringValue": svc}},
        {"key": "host.name", "value": {"stringValue": f"node-{random.randint(1,10)}"}},
    ]}, "scopeMetrics": [{"metrics": metrics}]}]}


def gen_logs_batch(batch_size):
    """Generate a batch of OTLP logs JSON."""
    now_ns = int(time.time() * 1e9)
    svc = random.choice(SERVICES)
    records = []
    for _ in range(batch_size):
        sev = random.choice([9, 9, 9, 9, 13, 13, 17])
        sev_text = {9: "INFO", 13: "WARN", 17: "ERROR"}[sev]
        records.append({
            "timeUnixNano": str(now_ns + random.randint(-1000000000, 0)),
            "severityNumber": sev,
            "severityText": sev_text,
            "body": {"stringValue": f"{sev_text} {random.choice(ENDPOINTS)} completed in {random.randint(1,500)}ms"},
            "attributes": [
                {"key": "trace_id", "value": {"stringValue": ''.join(random.choices(string.hexdigits[:16], k=16))}},
                {"key": "method", "value": {"stringValue": random.choice(["GET", "POST"])}},
                {"key": "endpoint", "value": {"stringValue": random.choice(ENDPOINTS)}},
            ]
        })
    return {"resourceLogs": [{"resource": {"attributes": [
        {"key": "service.name", "value": {"stringValue": svc}},
    ]}, "scopeLogs": [{"logRecords": records}]}]}


def gen_traces_batch(batch_size):
    """Generate a batch of OTLP traces JSON."""
    now_ns = int(time.time() * 1e9)
    svc = random.choice(SERVICES)
    spans = []
    for _ in range(batch_size):
        trace_id = ''.join(random.choices(string.hexdigits[:16], k=32))
        span_id = ''.join(random.choices(string.hexdigits[:16], k=16))
        spans.append({
            "traceId": trace_id,
            "spanId": span_id,
            "name": random.choice(ENDPOINTS),
            "kind": 2,
            "startTimeUnixNano": str(now_ns - random.randint(1000000, 500000000)),
            "endTimeUnixNano": str(now_ns),
            "attributes": [
                {"key": "http.method", "value": {"stringValue": "GET"}},
                {"key": "http.status_code", "value": {"intValue": "200"}},
            ]
        })
    return {"resourceSpans": [{"resource": {"attributes": [
        {"key": "service.name", "value": {"stringValue": svc}},
    ]}, "scopeSpans": [{"spans": spans}]}]}


def ingest_worker(signal_type, duration_sec, rate_per_sec):
    """Worker that ingests data at the specified rate."""
    batch_interval = BATCH_SIZE / rate_per_sec
    end_time = time.time() + duration_sec
    count = 0

    while time.time() < end_time:
        batch_start = time.time()

        if signal_type == "metrics":
            data = gen_metrics_batch(BATCH_SIZE)
            url = f"{BASE_URL}/v1/metrics/json"
        elif signal_type == "logs":
            data = gen_logs_batch(BATCH_SIZE)
            url = f"{BASE_URL}/v1/logs/json"
        else:
            data = gen_traces_batch(BATCH_SIZE)
            url = f"{BASE_URL}/v1/traces/json"

        latency = post_json(url, data)
        if latency is not None:
            with lock:
                ingest_latencies[signal_type].append(latency)
                total_ingested[signal_type] += BATCH_SIZE
            count += BATCH_SIZE

        # Rate limiting
        elapsed = time.time() - batch_start
        sleep_time = batch_interval - elapsed
        if sleep_time > 0:
            time.sleep(sleep_time)

    return count


def query_worker(duration_sec):
    """Worker that periodically queries to measure read latency."""
    end_time = time.time() + duration_sec
    now = int(time.time())

    while time.time() < end_time:
        # Range query
        start = now - 300
        end = int(time.time())
        lat, _ = get_json(f"{BASE_URL}/api/v1/query_range?query=http_requests_total&start={start}&end={end}&step=30s")
        if lat:
            with lock:
                query_latencies["metrics"].append(lat)

        # Instant query
        lat, _ = get_json(f"{BASE_URL}/api/v1/query?query=cpu_usage_percent")
        if lat:
            with lock:
                query_latencies["instant"].append(lat)

        # Log query
        lat, _ = get_json(f"{BASE_URL}/api/v1/logs?query=%7B%7D&start={start}.0&end={end}.0&limit=100")
        if lat:
            with lock:
                query_latencies["logs"].append(lat)

        time.sleep(QUERY_INTERVAL)


def percentile(data, p):
    if not data:
        return 0
    sorted_data = sorted(data)
    idx = int(len(sorted_data) * p / 100)
    return sorted_data[min(idx, len(sorted_data) - 1)]


def main():
    print("=" * 70)
    print("  PARQTEL PERFORMANCE TEST")
    print(f"  Rate: {RATE} samples/sec | Duration: {DURATION_SEC}s ({DURATION_SEC//60}min)")
    print(f"  Target: ~{RATE * DURATION_SEC:,} total samples")
    print(f"  Split: ~333 metrics/s + ~333 logs/s + ~333 traces/s")
    print("=" * 70)
    print()

    # Distribute rate across 3 signal types
    rate_per_signal = RATE // 3

    start_time = time.time()

    with ThreadPoolExecutor(max_workers=6) as pool:
        # Start ingest workers
        f_metrics = pool.submit(ingest_worker, "metrics", DURATION_SEC, rate_per_signal)
        f_logs = pool.submit(ingest_worker, "logs", DURATION_SEC, rate_per_signal)
        f_traces = pool.submit(ingest_worker, "traces", DURATION_SEC, rate_per_signal)
        # Start query worker
        f_query = pool.submit(query_worker, DURATION_SEC)

        # Progress reporting
        last_report = time.time()
        while not all(f.done() for f in [f_metrics, f_logs, f_traces]):
            time.sleep(10)
            elapsed = time.time() - start_time
            with lock:
                total = sum(total_ingested.values())
                rate_actual = total / elapsed if elapsed > 0 else 0
                m_p99 = percentile(ingest_latencies["metrics"][-100:], 99) if ingest_latencies["metrics"] else 0
                q_p99 = percentile(query_latencies["metrics"][-20:], 99) if query_latencies["metrics"] else 0
            print(f"  [{int(elapsed):>4}s] ingested={total:>8,} | rate={rate_actual:>6.0f}/s | "
                  f"ingest_p99={m_p99:>6.1f}ms | query_p99={q_p99:>6.1f}ms | "
                  f"errors={sum(errors.values())}")

    wall_time = time.time() - start_time

    # Final report
    print()
    print("=" * 70)
    print("  RESULTS")
    print("=" * 70)
    print()
    print(f"  Duration: {wall_time:.1f}s")
    print(f"  Total ingested: {sum(total_ingested.values()):,}")
    print(f"    Metrics: {total_ingested['metrics']:,}")
    print(f"    Logs:    {total_ingested['logs']:,}")
    print(f"    Traces:  {total_ingested['traces']:,}")
    print(f"  Effective rate: {sum(total_ingested.values()) / wall_time:.0f} samples/sec")
    print(f"  Errors: {dict(errors)}")
    print()
    print("  ┌─────────────────────────────────────────────────────────────┐")
    print("  │ INGESTION LATENCY (per batch of 50 samples)                 │")
    print("  ├─────────────┬──────────┬──────────┬──────────┬─────────────┤")
    print("  │ Signal      │ p50 (ms) │ p95 (ms) │ p99 (ms) │ max (ms)    │")
    print("  ├─────────────┼──────────┼──────────┼──────────┼─────────────┤")
    for sig in ["metrics", "logs", "traces"]:
        data = ingest_latencies[sig]
        if data:
            print(f"  │ {sig:<11} │ {percentile(data,50):>8.2f} │ {percentile(data,95):>8.2f} │ "
                  f"{percentile(data,99):>8.2f} │ {max(data):>11.2f} │")
    print("  └─────────────┴──────────┴──────────┴──────────┴─────────────┘")
    print()
    print("  ┌─────────────────────────────────────────────────────────────┐")
    print("  │ QUERY LATENCY                                               │")
    print("  ├─────────────┬──────────┬──────────┬──────────┬─────────────┤")
    print("  │ Type        │ p50 (ms) │ p95 (ms) │ p99 (ms) │ max (ms)    │")
    print("  ├─────────────┼──────────┼──────────┼──────────┼─────────────┤")
    for qtype in ["metrics", "instant", "logs"]:
        data = query_latencies[qtype]
        if data:
            print(f"  │ {qtype:<11} │ {percentile(data,50):>8.2f} │ {percentile(data,95):>8.2f} │ "
                  f"{percentile(data,99):>8.2f} │ {max(data):>11.2f} │")
    print("  └─────────────┴──────────┴──────────┴──────────┴─────────────┘")
    print()

    # Memory buffer validation
    print("  ┌─────────────────────────────────────────────────────────────┐")
    print("  │ IMMEDIATE QUERYABILITY TEST (memory buffer)                  │")
    print("  ├─────────────────────────────────────────────────────────────┤")
    # Ingest fresh data and query immediately
    now_ns = int(time.time() * 1e9)
    data = {"resourceMetrics": [{"resource": {"attributes": [
        {"key": "service.name", "value": {"stringValue": "perf-test"}}
    ]}, "scopeMetrics": [{"metrics": [{
        "name": "perf_test_immediate",
        "gauge": {"dataPoints": [{"asDouble": 123.456, "timeUnixNano": str(now_ns),
                                   "attributes": [{"key": "test", "value": {"stringValue": "buffer"}}]}]}
    }]}]}]}
    post_json(f"{BASE_URL}/v1/metrics/json", data)
    lat, resp = get_json(f"{BASE_URL}/api/v1/label/__name__/values")
    names = resp.get("data", []) if resp else []
    if "perf_test_immediate" in names:
        print(f"  │ ✅ Metric visible immediately after ingest ({lat:.1f}ms)        │")
    else:
        print(f"  │ ❌ Metric NOT visible immediately                            │")
    print("  └─────────────────────────────────────────────────────────────┘")
    print()


if __name__ == "__main__":
    main()
