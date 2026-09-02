#!/usr/bin/env python3
"""parqtel high-fidelity benchmark data generator.

Seeds via OTLP gRPC (the production ingestion path) with realistic
distributions. Target profile (defaults, tunable via env):

  METRICS: 10,000 series x 120 samples/series = 1,200,000 samples
           40 services; per-series: counter/gauge/histogram mix,
           diurnal sine patterns, bursts, error injection
  LOGS:    200,000 records across services/severities with
           structured JSON bodies, trace_id correlation, attributes
  TRACES:  5,000 traces x 6 spans = 30,000 spans; multi-service
           call trees, error spans, latency distributions

Payload size lands well above 1 GB decoded on the server once
Parquet zstd blocks are written (verify via benchmark report).

Usage: python3 gen_bench_data.py [grpc_addr] [--quick]
"""
import math
import os
import random
import sys
import time
import threading
from concurrent.futures import ThreadPoolExecutor

sys.path.insert(0, os.path.join(os.path.dirname(__file__), "bench_stubs"))

import grpc
from opentelemetry.proto.collector.trace.v1 import trace_service_pb2, trace_service_pb2_grpc
from opentelemetry.proto.collector.metrics.v1 import metrics_service_pb2, metrics_service_pb2_grpc
from opentelemetry.proto.collector.logs.v1 import logs_service_pb2, logs_service_pb2_grpc
from opentelemetry.proto.common.v1 import common_pb2
from opentelemetry.proto.resource.v1 import resource_pb2

ADDR = sys.argv[1] if len(sys.argv) > 1 and not sys.argv[1].startswith("--") else "127.0.0.1:14317"
QUICK = "--quick" in sys.argv

# ── Profile knobs ────────────────────────────────────────────────────────────
SERVICES = [
    "api-gateway", "auth-service", "user-service", "order-service", "payment-service",
    "inventory-service", "search-service", "notification-service", "email-service",
    "media-service", "cache-layer", "message-queue", "scheduler", "aggregator",
    "reporting", "billing", "shipping", "reviews", "catalog", "recommendation",
    "session-store", "config-service", "discovery", "proxy-edge", "rate-limiter",
    "feature-flags", "webhooks", "audit-log", "data-pipeline", "stream-processor",
    "geo-service", "locale-service", "doc-service", "pdf-renderer", "image-resizer",
    "video-transcoder", "ml-inference", "model-serving", "batch-jobs", "health-check",
]
REGIONS = ["us-east-1", "us-west-2", "eu-west-1", "ap-south-1"]
NODES = [f"node-{i:03d}" for i in range(60)]
ROUTES = ["/api/v1/users", "/api/v1/orders", "/api/v1/cart", "/api/v1/checkout",
          "/api/v1/search", "/api/v1/recommend", "/api/v1/pay", "/api/v1/ship",
          "/api/v1/media", "/api/v1/report", "/api/v1/login", "/api/v1/logout"]
METHODS = ["GET", "POST", "PUT", "DELETE"]

if QUICK:
    N_SERIES = 500
    SAMPLES_PER_SERIES = 60
    N_LOGS = 10_000
    N_TRACES = 500
else:
    N_SERIES = 10_000
    SAMPLES_PER_SERIES = 120
    N_LOGS = 200_000
    N_TRACES = 5_000

SPAN_PER_TRACE = 6
INTERVAL_S = 30  # scrape interval for the metric timeline

rng = random.Random(42)  # deterministic across runs


def kv(k, v):
    return common_pb2.KeyValue(key=k, value=common_pb2.AnyValue(string_value=v))


def kvi(k, v):
    return common_pb2.KeyValue(key=k, value=common_pb2.AnyValue(int_value=v))


# ── METRICS ───────────────────────────────────────────────────────────────────

def series_inventory():
    """10K unique series across metric shapes and label dimensions."""
    inv = []
    shapes = ["counter", "gauge", "histogram"]
    for i in range(N_SERIES):
        svc = SERVICES[i % len(SERVICES)]
        shape = shapes[i % len(shapes)]
        base = {
            "service.name": svc,
            "region": REGIONS[(i // 7) % len(REGIONS)],
            "node": NODES[i % len(NODES)],
            "instance": f"{svc}-{i % 40}",
        }
        if shape == "counter":
            inv.append(("http_requests_total", base | {"http.method": METHODS[i % 4], "http.route": ROUTES[i % len(ROUTES)]}, "counter", i))
        elif shape == "gauge":
            kind = ["cpu_usage", "memory_usage", "active_connections", "queue_depth"][i % 4]
            inv.append((kind, base, "gauge", i))
        else:
            inv.append(("http_request_duration_seconds", base | {"http.route": ROUTES[i % len(ROUTES)]}, "histogram", i))
    return inv


def gen_metrics_batch(inv_slice, now_ns):
    """One ExportMetricsServiceRequest per series covering the full timeline."""
    requests = []
    span_s = SAMPLES_PER_SERIES * INTERVAL_S
    for (name, labels, shape, seed) in inv_slice:
        r = random.Random(seed)
        phase = r.uniform(0, 2 * math.pi)
        amp = r.uniform(0.5, 1.5)
        base = r.uniform(10, 500)
        dps = []
        counter_val = 0.0
        for s in range(SAMPLES_PER_SERIES):
            ts = now_ns - (SAMPLES_PER_SERIES - s) * INTERVAL_S * 1_000_000_000
            t = s * INTERVAL_S
            diurnal = math.sin(t / 3600.0 + phase) * 0.3 + 1.0
            burst = r.uniform(2.0, 6.0) if r.random() < 0.02 else 1.0
            if shape == "counter":
                counter_val += base * 0.05 * diurnal * burst
                dps.append(dict(time_unix_nano=ts, as_double=counter_val))
            elif shape == "gauge":
                val = base * diurnal * burst * r.uniform(0.8, 1.2)
                dps.append(dict(time_unix_nano=ts, as_double=max(0.0, val)))
            else:  # histogram: emit the bucket-count form
                n = int(base * diurnal * burst)
                dps.append(dict(time_unix_nano=ts, as_double=float(n)))
        attrs = [kv(k, v) for k, v in labels.items()]
        for d in dps:
            d["attributes"] = attrs
        metric = dict(name=name)
        if shape == "counter":
            metric["sum"] = dict(data_points=dps, aggregation_temporality=2, is_monotonic=True)
        else:
            metric["gauge"] = dict(data_points=dps)
        requests.append(metrics_service_pb2.ExportMetricsServiceRequest(
            resource_metrics=[dict(
                resource=resource_pb2.Resource(attributes=[kv("service.name", labels["service.name"]), kv("service.version", "2.4.1")]),
                scope_metrics=[dict(scope=dict(name="bench-gen", version="1.0"), metrics=[metric])],
            )]))
    return requests


# ── LOGS ──────────────────────────────────────────────────────────────────────

LOG_BODIES = {
    "INFO": [
        "Request completed status=200 route={route} duration_ms={dur} user={user}",
        "Cache hit key=session:{user} ttl=300",
        "Scheduled job {job} completed in {dur}ms",
        "Connection pool stats active={dur} idle=12",
    ],
    "WARN": [
        "Slow query detected duration_ms={dur} threshold=500 route={route}",
        "Retry attempt 2/5 for upstream {route}",
        "Cache miss ratio high: 0.{dur}",
        "Rate limit near 90% for tenant {user}",
    ],
    "ERROR": [
        '{{"error":"upstream connect failure","route":"{route}","attempt":3,"duration_ms":{dur}}}',
        "Database connection refused host=db-{dur} retries=3",
        "Failed to process order order_id={user} reason=inventory_unavailable",
        "Timeout waiting for payment confirmation payment_id={user} after {dur}ms",
    ],
    "DEBUG": [
        "Entering handler route={route} trace_id={user}",
        "Serialized payload size={dur}b",
        "Feature flag check flag={user} value=on",
    ],
}


def gen_logs_batch(offset, count, now_ns):
    requests = []
    for i in range(offset, offset + count):
        r = random.Random(i)
        svc = SERVICES[i % len(SERVICES)]
        sev = r.choices(["INFO", "WARN", "ERROR", "DEBUG"], weights=[70, 15, 10, 5])[0]
        route = ROUTES[r.randrange(len(ROUTES))]
        dur = r.randrange(1, 5000)
        user = f"u{r.randrange(100000):05d}"
        tmpl = r.choice(LOG_BODIES[sev])
        class SafeDict(dict):
            def __missing__(self, k):
                return "{" + k + "}"
        body = tmpl.format_map(SafeDict(route=route, dur=dur, user=user, job=f"job-{i%20}"))
        attrs = [kv("http.route", route), kv("http.method", METHODS[r.randrange(4)])]
        if i % 20 == 0:  # 5% trace-linked logs
            attrs.append(kv("trace_id", f"{i:032x}"))
        requests.append(dict(
            time_unix_nano=now_ns - r.randrange(0, SAMPLES_PER_SERIES * INTERVAL_S) * 1_000_000_000,
            severity_text=sev,
            severity_number={"DEBUG": 5, "INFO": 9, "WARN": 13, "ERROR": 17}[sev],
            body=common_pb2.AnyValue(string_value=body),
            attributes=attrs,
        ))
    return logs_service_pb2.ExportLogsServiceRequest(
        resource_logs=[dict(
            resource=resource_pb2.Resource(attributes=[kv("service.name", svc)]),
            scope_logs=[dict(scope=dict(name="bench-gen"), log_records=requests)],
        )])


# ── TRACES ───────────────────────────────────────────────────────────────────

def gen_trace(idx, now_ns):
    """Multi-service call tree with error/latency realism."""
    r = random.Random(9000 + idx)
    tid = f"{idx:032x}"
    tid_bytes = bytes.fromhex(tid)
    t0 = now_ns - r.randrange(0, SAMPLES_PER_SERIES * INTERVAL_S) * 1_000_000_000
    root_svc = SERVICES[r.randrange(len(SERVICES))]
    route = ROUTES[r.randrange(len(ROUTES))]
    downstream = r.sample([s for s in SERVICES if s != root_svc], SPAN_PER_TRACE - 1)
    is_error = r.random() < 0.08
    total_ms = r.uniform(50, 2500)

    spans = []
    # Root server span
    root_dur = int(total_ms * 1e6)
    spans.append(dict(
        trace_id=tid_bytes, span_id=bytes.fromhex(f"{idx:016x}")[:8],
        name=f"{METHODS[r.randrange(4)]} {route}", kind=2,
        start_time_unix_nano=t0, end_time_unix_nano=t0 + root_dur,
        status=dict(code=2 if is_error else 1, message="upstream timeout" if is_error else ""),
        attributes=[kv("http.route", route), kv("http.method", "GET")],
    ))
    cursor = t0
    for j, svc in enumerate(downstream):
        parent = spans[-1]
        start = cursor + int(r.uniform(1, 20) * 1e6)
        dur = int(root_dur * r.uniform(0.05, 0.4))
        err = is_error and j == len(downstream) - 1
        spans.append(dict(
            trace_id=tid_bytes,
            span_id=bytes.fromhex(f"{idx:016x}{j + 1:04x}")[:8],
            parent_span_id=parent["span_id"],
            name=f"{svc}.Process", kind=3,
            start_time_unix_nano=start,
            end_time_unix_nano=start + dur,
            status=dict(code=2 if err else 1, message="timeout" if err else ""),
            attributes=[kv("db.system", "postgresql" if j % 2 else "redis")],
        ))
        cursor = start
    resource_spans = []
    for svc, spans_of_svc in [(root_svc, spans[:1])]:
        resource_spans.append(dict(
            resource=resource_pb2.Resource(attributes=[kv("service.name", svc)]),
            scope_spans=[dict(scope=dict(name="bench-gen"), spans=spans_of_svc)],
        ))
    for svc, sp in zip(downstream, spans[1:]):
        resource_spans.append(dict(
            resource=resource_pb2.Resource(attributes=[kv("service.name", svc)]),
            scope_spans=[dict(scope=dict(name="bench-gen"), spans=[sp])],
        ))
    return trace_service_pb2.ExportTraceServiceRequest(resource_spans=resource_spans)


# ── Main ─────────────────────────────────────────────────────────────────────

def main():
    now_ns = time.time_ns()
    ch = grpc.insecure_channel(ADDR)
    grpc.channel_ready_future(ch).result(timeout=15)
    mstub = metrics_service_pb2_grpc.MetricsServiceStub(ch)
    lstub = logs_service_pb2_grpc.LogsServiceStub(ch)
    tstub = trace_service_pb2_grpc.TraceServiceStub(ch)

    print(f"[gen] target {ADDR} | series={N_SERIES} samples/series={SAMPLES_PER_SERIES} "
          f"logs={N_LOGS} traces={N_TRACES}x{SPAN_PER_TRACE} spans")

    t0 = time.time()

    # Metrics: 10K requests (one per series), 8 parallel workers
    inv = series_inventory()
    print(f"[gen] metrics: {len(inv)} series -> {len(inv) * SAMPLES_PER_SERIES:,} samples")
    batches_per_worker = 16

    def send_metric_slice(args):
        lo, hi = args
        for req in gen_metrics_batch(inv[lo:hi], now_ns):
            mstub.Export(req)

    slices = [(i, min(i + batches_per_worker, len(inv)))
              for i in range(0, len(inv), batches_per_worker)]
    with ThreadPoolExecutor(max_workers=8) as ex:
        list(ex.map(send_metric_slice, slices))
    print(f"[gen] metrics done in {time.time()-t0:.1f}s")

    # Logs: 4 x 50K
    t1 = time.time()
    log_chunk = 2_500 if QUICK else 25_000  # <4MB gRPC message limit

    def send_logs(worker):
        # Each worker owns a strided partition: [worker::8]
        offset = worker
        while offset < N_LOGS:
            n = min(log_chunk, N_LOGS - offset)
            lstub.Export(gen_logs_batch(offset, n, now_ns))
            offset += 8 * log_chunk

    with ThreadPoolExecutor(max_workers=8) as ex:
        list(ex.map(send_logs, range(8)))
    print(f"[gen] logs done in {time.time()-t1:.1f}s")

    # Traces: 8 workers
    t2 = time.time()

    def send_trace_slice(lo):
        for i in range(lo, min(lo + N_TRACES // 8, N_TRACES)):
            tstub.Export(gen_trace(i, now_ns))

    with ThreadPoolExecutor(max_workers=8) as ex:
        list(ex.map(send_trace_slice, range(0, N_TRACES, N_TRACES // 8)))
    print(f"[gen] traces done in {time.time()-t2:.1f}s")

    total_samples = len(inv) * SAMPLES_PER_SERIES
    print(f"[gen] COMPLETE in {time.time()-t0:.1f}s: "
          f"{total_samples:,} metric samples, {N_LOGS:,} logs, {N_TRACES * SPAN_PER_TRACE:,} spans")


if __name__ == "__main__":
    main()
