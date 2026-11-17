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

Parqtel accepts OTLP protobuf over gRPC/HTTP and OTLP JSON. A collector using
the SDK defaults needs no encoding override. Generic applications can post a
JSON object/array to `/api/v1/ingest`; see
[configuration](../../docs/operations/configuration.md#ingest).
