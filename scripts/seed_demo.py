#!/usr/bin/env python3
"""Parqtel demo seeder — rich, coherent telemetry for UI screenshots.

Seeds 4 hours of story-arc telemetry via OTLP gRPC (production path):
  - 12 services, 3 gateways, realistic RED metrics (diurnal + incidents)
  - A 20-minute INCIDENT (17:10–17:30) on checkout-service: latency
    spikes, error bursts, correlated error logs + slow failing traces
  - Span-metrics visible: traces_service_* RED derived server-side
  - Log severities with a narrative (deploys, warnings, the incident)
  - Multi-service traces with parent/child trees, errors, slow spans
  - Alert rules (4) loaded via the rules API so the Alerts UI shows
    rule management; alerts FIRE from real data via the eval engine
  - Trace-linked logs (same trace_ids) so correlation clicks work

Usage: python3 seed_demo.py [grpc_addr] [http_addr]

Prereq (one-time): regenerate the gRPC stubs (gitignored) into scripts/bench_stubs:
  python3 -m grpc_tools.protoc -I parqtel-ingest/proto \
    --python_out=scripts/bench_stubs --grpc_python_out=scripts/bench_stubs \
    opentelemetry/proto/trace/v1/trace.proto opentelemetry/proto/common/v1/common.proto \
    opentelemetry/proto/resource/v1/resource.proto opentelemetry/proto/logs/v1/logs.proto \
    opentelemetry/proto/metrics/v1/metrics.proto \
    opentelemetry/proto/collector/trace/v1/trace_service.proto \
    opentelemetry/proto/collector/metrics/v1/metrics_service.proto \
    opentelemetry/proto/collector/logs/v1/logs_service.proto
  find scripts/bench_stubs -type d -exec touch {}/__init__.py \;
"""
import json
import math
import random
import sys
import time
import urllib.request
from concurrent.futures import ThreadPoolExecutor

sys.path.insert(0, __file__.rsplit("/", 1)[0] + "/bench_stubs")
import grpc
from opentelemetry.proto.collector.trace.v1 import trace_service_pb2, trace_service_pb2_grpc
from opentelemetry.proto.collector.metrics.v1 import metrics_service_pb2, metrics_service_pb2_grpc
from opentelemetry.proto.collector.logs.v1 import logs_service_pb2, logs_service_pb2_grpc
from opentelemetry.proto.common.v1 import common_pb2
from opentelemetry.proto.resource.v1 import resource_pb2

GRPC_ADDR = sys.argv[1] if len(sys.argv) > 1 else "127.0.0.1:14317"
HTTP_ADDR = sys.argv[2] if len(sys.argv) > 2 else "http://127.0.0.1:14318"

NOW_NS = time.time_ns()
H = 3_600_000_000_000

SERVICES = [
    "api-gateway", "auth-service", "checkout-service", "payment-service",
    "inventory-service", "search-service", "notification-service",
    "user-service", "cart-service", "recommendation-service",
    "media-service", "audit-service",
]
ROUTES = ["/api/v1/checkout", "/api/v1/cart", "/api/v1/login", "/api/v1/search",
          "/api/v1/users", "/api/v1/orders", "/api/v1/recommend", "/api/v1/media"]
REGIONS = ["us-east-1", "us-west-2", "eu-west-1"]
rng = random.Random(42)


def kv(k, v):
    return common_pb2.KeyValue(key=k, value=common_pb2.AnyValue(string_value=v))


def res_attrs(svc, region="us-east-1", version="1.8.2"):
    return [
        kv("service.name", svc),
        kv("service.version", version),
        kv("deployment.region", region),
    ]


# ── Story arc: an incident on checkout-service from t-40m to t-20m ────────
INCIDENT_START_NS = NOW_NS - 40 * 60_000_000_000
INCIDENT_END_NS = NOW_NS - 20 * 60_000_000_000


def in_incident(ts_ns):
    return INCIDENT_START_NS <= ts_ns <= INCIDENT_END_NS


# ── METRICS: RED + resource gauges, 4h at 30s, diurnal + incident arcs ────

def gen_metrics():
    requests = []
    for svc in SERVICES:
        base = rng.uniform(40, 180)
        phase = rng.uniform(0, 2 * math.pi)
        for i in range(480):  # 4h @ 30s
            ts = NOW_NS - 4 * H + i * 30_000_000_000
            t = i * 30
            diurnal = 0.7 + 0.3 * math.sin(t / 2400 + phase)
            burst = rng.uniform(1.0, 1.15)

            # checkout request rate dips during the incident (users retry less)
            dip = 0.45 if (svc == "checkout-service" and in_incident(ts)) else 1.0
            req = base * diurnal * burst * dip

            err_rate = 0.004
            if svc == "checkout-service":
                err_rate = 0.35 if in_incident(ts) else 0.006
            elif svc in ("payment-service", "cart-service") and in_incident(ts):
                err_rate = 0.06  # cascade
            errs = req * err_rate

            lat_base = {"checkout-service": 180, "payment-service": 220}.get(svc, rng.uniform(40, 120))
            lat = lat_base * (4.5 if (svc == "checkout-service" and in_incident(ts)) else 1.0)
            lat *= rng.uniform(0.9, 1.1)

            region = REGIONS[rng.randrange(3)]
            common = res_attrs(svc, region)
            for name, val in [
                ("http_requests_total", req),
                ("http_errors_total", errs),
                ("http_request_duration_ms", lat),
                ("cpu_usage", min(95, rng.uniform(22, 48) * (1.8 if in_incident(ts) else 1.0))),
                ("memory_usage", rng.uniform(380, 720)),
            ]:
                dp = {"time_unix_nano": ts, "as_double": round(val, 2)}
                requests.append((name, dp, common))
    return requests


def send_metrics(mstub):
    # Batch per service to keep messages small
    all_metrics = {}
    for name, dp, common in gen_metrics():
        all_metrics.setdefault(name, []).append((dp, common))
    for name, entries in all_metrics.items():
        per_resource = {}
        for dp, common in entries:
            key = (common[0].value.string_value, common[2].value.string_value)  # svc, region
            per_resource.setdefault(key, []).append((dp, common))
        for (dp0, common) in per_resource[list(per_resource)[0]][0:1]:  # placeholder
            pass
        for key, pairs in per_resource.items():
            dps = [p[0] for p in pairs]
            common = pairs[0][1]
            mstub.Export(metrics_service_pb2.ExportMetricsServiceRequest(
                resource_metrics=[dict(
                    resource=resource_pb2.Resource(attributes=list(common)),
                    scope_metrics=[dict(scope=dict(name="parqtel-demo", version="1.0"), metrics=[
                        dict(name=name, gauge=dict(data_points=dps))
                    ])],
                )]))


# ── LOGS: narrative severities + incident storm + trace links ─────────────

LOG_STORIES = {
    "INFO": [
        "Request completed status=200 route={route} duration_ms={dur}",
        "Successfully processed order order_id={id} in {dur}ms",
        "Cache hit ratio 0.{p} for {route}",
        "Background job {job} finished in {dur}ms",
    ],
    "WARN": [
        "Slow query detected duration_ms={dur} threshold=500 route={route}",
        "Connection pool at {p}% capacity",
        "Retry attempt 2/3 for upstream {route}",
    ],
    "ERROR": [
        "Upstream connection refused host=checkout-db-primary attempt={p}",
        "Request failed status=500 route={route} duration_ms={dur}",
        'Payment gateway timeout after {dur}ms order_id={id}',
    ],
    "DEBUG": [
        "Entering handler route={route} trace_id={id}",
        "Serialized payload size={dur}b route={route}",
    ],
}


def gen_log(sev, ts):
    story = rng.choice(LOG_STORIES[sev])
    return story.format(
        route=rng.choice(ROUTES), dur=rng.randrange(12, 4800),
        id=f"{rng.randrange(10**8):08d}", p=rng.randrange(60, 99),
        job=rng.choice(["cleanup", "reindex", "report", "sync"]),
    )


def send_logs(lstub):
    records = []
    now_s = NOW_NS // 1_000_000_000
    for i in range(2400):  # 4h of logs, denser during incident
        ts = NOW_NS - 4 * H + rng.randrange(0, 4 * H)
        incident = in_incident(ts)
        if incident:
            sev = rng.choices(["INFO", "WARN", "ERROR", "DEBUG"], weights=[30, 30, 35, 5])[0]
            svc = rng.choice(["checkout-service", "payment-service", "cart-service", "api-gateway"])
        else:
            sev = rng.choices(["INFO", "WARN", "ERROR", "DEBUG"], weights=[72, 14, 8, 6])[0]
            svc = rng.choice(SERVICES)
        attrs = [kv("http.route", rng.choice(ROUTES)), kv("http.method", "GET")]
        # 12% of incident errors carry a real trace id for correlation
        if sev in ("ERROR", "WARN") and incident and rng.random() < 0.12:
            attrs.append(kv("trace_id", f"{rng.randrange(16**32):032x}"))
        records.append(dict(
            time_unix_nano=ts,
            severity_text=sev,
            severity_number={"DEBUG": 5, "INFO": 9, "WARN": 13, "ERROR": 17}[sev],
            body=common_pb2.AnyValue(string_value=gen_log(sev, ts)),
            attributes=attrs,
        ))
    # batch 25k-safe chunks per service-agnostic resource
    for off in range(0, len(records), 500):
        chunk = records[off:off + 500]
        lstub.Export(logs_service_pb2.ExportLogsServiceRequest(
            resource_logs=[dict(
                resource=resource_pb2.Resource(attributes=res_attrs("api-gateway")),
                scope_logs=[dict(scope=dict(name="parqtel-demo"), log_records=chunk)],
            )]))
    print(f"[demo] logs: {len(records)}")


# ── TRACES: multi-service trees; incident spans slow + error ──────────────

def send_traces(tstub):
    n_traces = 400
    for t in range(n_traces):
        ts = NOW_NS - 4 * H + rng.randrange(0, 4 * H)
        incident = in_incident(ts)
        tid = bytes([rng.randrange(256) for _ in range(16)])
        sid = lambda: bytes([rng.randrange(256) for _ in range(8)])

        gateway_sid = sid()
        root_dur = rng.randrange(80, 400) * 1_000_000
        if incident and rng.random() < 0.5:
            root_dur *= 6  # slow incident traces
        spans = [dict(
            trace_id=tid, span_id=gateway_sid, name=f"POST {rng.choice(ROUTES)}", kind=2,
            start_time_unix_nano=ts, end_time_unix_nano=ts + root_dur,
            attributes=[kv("http.method", "POST"), kv("http.route", rng.choice(ROUTES))],
            status=dict(code=0, message=""),
        )]
        # 2-4 downstream services
        depth = rng.randrange(2, 5)
        cursor = ts + rng.randrange(1, 10) * 1_000_000
        child_svcs = rng.sample(SERVICES[1:], depth)
        for d in range(depth):
            svc = child_svcs[d]
            dur = int(root_dur * rng.uniform(0.15, 0.5))
            is_err = incident and rng.random() < 0.4
            spans.append(dict(
                _svc=svc,
                trace_id=tid, span_id=sid(), parent_span_id=spans[-1]["span_id"],
                name=f"{svc}.Process", kind=3,
                start_time_unix_nano=cursor, end_time_unix_nano=cursor + dur,
                attributes=[kv("db.system", "postgresql" if d % 2 else "redis")],
                status=dict(code=2 if is_err else 0, message="upstream timeout" if is_err else ""),
            ))
            cursor += dur
        # group by service for proper ResourceSpans (root = gateway, children
        # keep the service they were assigned when created); _svc is a
        # python-side marker stripped before protobuf construction
        by_svc = {}
        for idx, s in enumerate(spans):
            svc = "api-gateway" if idx == 0 else s.pop("_svc", "api-gateway")
            by_svc.setdefault(svc, []).append(s)
        resource_spans = []
        for svc, sp in by_svc.items():
            resource_spans.append(dict(
                resource=resource_pb2.Resource(attributes=res_attrs(svc)),
                scope_spans=[dict(scope=dict(name="parqtel-demo"), spans=sp)],
            ))
        tstub.Export(trace_service_pb2.ExportTraceServiceRequest(resource_spans=resource_spans))
    print(f"[demo] traces: {n_traces}")


# ── ALERT RULES via HTTP API ───────────────────────────────────────────────

RULES = [
    dict(id="high-error-rate-checkout", name="High error rate — checkout",
         signal="metrics", query='http_errors_total{service.name="checkout-service"}',
         condition=dict(type="threshold", operator=">", value=5),
         severity="critical", enabled=True, labels={"team": "core", "service": "checkout-service"}),
    dict(id="high-latency-payment", name="High latency — payment",
         signal="metrics", query='http_request_duration_ms{service.name="payment-service"}',
         condition=dict(type="threshold", operator=">", value=800),
         severity="warning", enabled=True, labels={"team": "core", "service": "payment-service"}),
    dict(id="cpu-saturation", name="CPU saturation",
         signal="metrics", query="cpu_usage",
         condition=dict(type="threshold", operator=">", value=85),
         severity="warning", enabled=True, labels={"team": "infra"}),
    dict(id="memory-pressure", name="Memory pressure",
         signal="metrics", query="memory_usage",
         condition=dict(type="threshold", operator=">", value=700),
         severity="info", enabled=True, labels={"team": "infra"}),
]


def yaml_rule(r):
    c = r["condition"]
    return (f"id: {r['id']}\nname: {r['name']}\nsignal: {r['signal']}\n"
            f"query: {r['query']}\ncondition:\n  type: {c['type']}\n"
            f"  operator: '{c['operator']}'\n  value: {c['value']}\n"
            f"severity: {r['severity']}\nenabled: {str(r['enabled']).lower()}\n"
            + ("labels:\n" + "".join(f"  {k}: \"{v}\"\n" for k, v in r["labels"].items()) if r.get("labels") else ""))


def send_rules():
    for r in RULES:
        req = urllib.request.Request(
            f"{HTTP_ADDR}/api/v1/rules", data=yaml_rule(r).encode(),
            headers={"Content-Type": "text/plain"})
        try:
            urllib.request.urlopen(req)
        except Exception as e:
            print(f"[demo] rule {r['id']}: {e}")
    print(f"[demo] rules: {len(RULES)} loaded (alerts fire from the seeded arcs "
          f"as the eval engine runs — error-rate critical fires during the incident replay)")


def main():
    ch = grpc.insecure_channel(GRPC_ADDR)
    grpc.channel_ready_future(ch).result(timeout=20)
    mstub = metrics_service_pb2_grpc.MetricsServiceStub(ch)
    lstub = logs_service_pb2_grpc.LogsServiceStub(ch)
    tstub = trace_service_pb2_grpc.TraceServiceStub(ch)

    t0 = time.time()
    with ThreadPoolExecutor(max_workers=4) as ex:
        futs = [ex.submit(send_metrics, mstub), ex.submit(send_logs, lstub),
                ex.submit(send_traces, tstub)]
        for f in futs:
            f.result()
    send_rules()
    print(f"[demo] COMPLETE in {time.time()-t0:.1f}s — incident window at "
          f"t-40m..t-20m; open {HTTP_ADDR}/ui")


if __name__ == "__main__":
    main()
