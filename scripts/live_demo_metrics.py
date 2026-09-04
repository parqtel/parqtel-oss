#!/usr/bin/env python3
"""Keeps the demo UI 'live' for screenshot capture: posts fresh metric
points every 20s (hot tail) so instant queries and the alert engine see
current data while screenshots are taken. Ctrl-C to stop."""
import sys, time, random
sys.path.insert(0, __file__.rsplit("/", 1)[0] + "/bench_stubs")
import grpc
from opentelemetry.proto.collector.metrics.v1 import metrics_service_pb2, metrics_service_pb2_grpc
from opentelemetry.proto.common.v1 import common_pb2
from opentelemetry.proto.resource.v1 import resource_pb2

GRPC_ADDR = sys.argv[1] if len(sys.argv) > 1 else "127.0.0.1:14317"
SVCS = ["api-gateway", "auth-service", "checkout-service", "payment-service",
        "inventory-service", "search-service", "notification-service",
        "user-service", "cart-service", "recommendation-service", "media-service", "audit-service"]

def kv(k, v):
    return common_pb2.KeyValue(key=k, value=common_pb2.AnyValue(string_value=v))

ch = grpc.insecure_channel(GRPC_ADDR)
grpc.channel_ready_future(ch).result(timeout=20)
stub = metrics_service_pb2_grpc.MetricsServiceStub(ch)
rng = random.Random(99)
print("[live] rebroadcasting hot metrics every 20s", flush=True)
try:
    while True:
        ts = time.time_ns()
        for svc in SVCS:
            hot = svc == "checkout-service"
            attrs = [kv("service.name", svc), kv("service.version", "1.8.2"),
                     kv("deployment.region", "us-east-1")]
            metrics = [
                dict(name="http_requests_total", gauge=dict(data_points=[dict(
                    time_unix_nano=ts, as_double=round(rng.uniform(40, 180), 2))])),
                dict(name="http_errors_total", gauge=dict(data_points=[dict(
                    time_unix_nano=ts, as_double=round(rng.uniform(8, 14), 2) if hot else round(rng.uniform(0, 0.9), 2))])),
                dict(name="http_request_duration_ms", gauge=dict(data_points=[dict(
                    time_unix_nano=ts, as_double=round(rng.uniform(700, 950), 2) if hot else round(rng.uniform(40, 160), 2))])),
                dict(name="cpu_usage", gauge=dict(data_points=[dict(
                    time_unix_nano=ts, as_double=round(rng.uniform(30, 55), 2))])),
                dict(name="memory_usage", gauge=dict(data_points=[dict(
                    time_unix_nano=ts, as_double=round(rng.uniform(560, 760), 2))])),
            ]
            stub.Export(metrics_service_pb2.ExportMetricsServiceRequest(
                resource_metrics=[dict(
                    resource=resource_pb2.Resource(attributes=attrs),
                    scope_metrics=[dict(scope=dict(name="parqtel-demo"), metrics=metrics)],
                )]))
        time.sleep(20)
except KeyboardInterrupt:
    print("[live] stopped")
