# Agent on-ramps

Copy-paste configurations for common collectors. Replace `parqtel:8080` /
`parqtel:4317` with your server and add an `Authorization: Bearer ...` header
when `[auth]` is enabled.

| Agent | Transport | Endpoint | Encoding |
|---|---|---|---|
| OTel Collector | OTLP/gRPC | `parqtel:4317` | protobuf (default) |
| OTel Collector | OTLP/HTTP | `http://parqtel:8080` | protobuf (default) or JSON |
| Vector | HTTP | `http://parqtel:8080/api/v1/ingest` | JSON array |
| Fluent Bit | HTTP | `http://parqtel:8080/api/v1/ingest` | JSON array |
| Filebeat | Logstash/Vector | local pipeline | JSON |

## Two-replica OTel Collector

Use [`otel-collector-ha.yaml`](otel-collector-ha.yaml) with two Parqtel replicas.
It sends metrics, logs, and traces to each pod directly, so do not replace the
per-pod endpoints with a load-balancing Service. Adjust DNS names for your
headless Service and namespace. Mount durable storage at
`/var/lib/otelcol/ha-queue`; each replica has a persistent sending queue, with
unlimited retries while it is unavailable. The 6,000-batch queue estimates
about 10 minutes at 9.5K metric points/s, assuming batches of up to 1,000 points.

Validate with `otelcol validate --config examples/agents/otel-collector-ha.yaml`
using the same Collector distribution that will run this config.

## Sharded writers

Use [`otel-collector-sharded.yaml`](otel-collector-sharded.yaml) when the chart
runs with `mode: sharded`. It fans OTLP/gRPC out to every writer pod
(`parqtel-writer-<i>.parqtel-writer:4317`); any writer may take any batch
because the readers union every writer's unflushed buffer with the shared
bucket. Never point it at the reader Service: readers do not ingest.

Parqtel accepts OTLP protobuf over gRPC/HTTP and OTLP JSON. A collector using
the SDK defaults needs no encoding override. Generic applications can post a
JSON object/array to `/api/v1/ingest`; see
[configuration](../../docs/operations/configuration.md#ingest).
