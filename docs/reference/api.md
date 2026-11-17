# HTTP API Reference

Complete endpoint list, verified against `crates/parqtel-server/src/router.rs`.
The machine-readable description is [`docs/api/openapi.yaml`](../api/openapi.yaml),
also served by a running instance at **`/oas`** (OpenAPI 3.0).

Base URL in the examples: `http://localhost:8080` (the single-container and binary
default). The Docker Compose stack publishes Parqtel on `9090` — see
[the port note](#ports).

## Ports

| Port | Protocol | Default | Note |
|---|---|---|---|
| `8080` | HTTP | `server.bind_address` = `0.0.0.0:8080` | API, embedded UI, Prometheus scrape |
| `4317` | gRPC | `server.grpc_bind_address` = `0.0.0.0:4317` | OTLP gRPC, the OTel SDK default. Empty string disables it |

The Compose stack overrides the HTTP port to **`9090`**
(`deploy/compose/parqtel/config/dev.toml`) so it does not collide with
Prometheus, which is published on `9091` there.

## Ingestion (OTLP)

OTel SDKs export directly over **gRPC at `:4317`** (all three collector
services — no collector sidecar needed) or over HTTP with protobuf or JSON.

| Method | Endpoint | Description |
|---|---|---|
| POST | `/v1/metrics` | Ingest metrics (protobuf) |
| POST | `/v1/metrics/json` | Ingest metrics (JSON) |
| POST | `/v1/logs` | Ingest logs (protobuf) |
| POST | `/v1/logs/json` | Ingest logs (JSON) |
| POST | `/v1/traces` | Ingest traces (protobuf) |
| POST | `/v1/traces/json` | Ingest traces (JSON) |

HTTP request bodies may be gzip-compressed; the gRPC server accepts gzip too.

## Metrics query (Prometheus-compatible)

| Method | Endpoint | Description |
|---|---|---|
| GET | `/api/v1/query` | Instant query; optional `lookback_delta` overrides the 5m default (max 30d) |
| GET | `/api/v1/query_range` | Range query |
| GET | `/api/v1/labels` | List all label names |
| GET | `/api/v1/label/__name__/values` | List metric names |
| GET | `/api/v1/label/:name/values` | List values for a label |

Instant queries use a 5-minute lookback (`query.lookback_delta_ns`, default
`300000000000`). Range steps are evaluated from `start` while `ts < end`, so the
final partial step is not evaluated — the same as Prometheus.

## Logs and traces

| Method | Endpoint | Description |
|---|---|---|
| GET | `/api/v1/logs` | Query logs with filters |
| GET | `/v1/logs/count` | Count matching logs |
| GET | `/v1/logs/fields` | List available log fields |
| GET | `/v1/logs/field_values` | List values for a log field |
| GET | `/v1/traces/search` | Search spans (per-span `trace_id` included for client-side grouping) |
| GET | `/v1/correlate` | Cross-signal correlation for a time window |
| GET | `/api/v1/logs/tail` | SSE live tail over buffered logs (`service_name`, `body_contains`, `limit`) |
| POST | `/v1/search` | Run a pipeline search |
| POST | `/api/v1/ingest` | Generic JSON log ingest; service from `X-Parqtel-Service` |
| POST | `/api/v1/write` | Prometheus remote_write (snappy + protobuf) |

## Alerts and rules

| Method | Endpoint | Description |
|---|---|---|
| GET | `/api/v1/alerts` | List all alerts |
| GET | `/api/v1/alerts/:id` | Get alert details |
| POST | `/api/v1/alerts/:id/acknowledge` | Acknowledge an alert |
| POST | `/api/v1/alerts/:id/resolve` | Resolve an alert |
| GET | `/api/v1/alerts/routes` | List notification routes (plus the redacted `[alerting.routing]` tree) |
| POST | `/api/v1/alerts/routes` | Replace notification routes |
| POST | `/api/v1/alerts/test_route` | Dry-run route/group/silence/inhibition evaluation for a label set |
| GET | `/api/v1/alerts/silences` | List silences |
| POST | `/api/v1/alerts/silences` | Create a silence |
| DELETE | `/api/v1/alerts/silences/:name` | Delete a silence |
| GET | `/api/v1/rules` | List alert rules |
| POST | `/api/v1/rules` | Create an alert rule |
| PUT | `/api/v1/rules/:id` | Update a rule |
| DELETE | `/api/v1/rules/:id` | Delete a rule |
| GET | `/api/v1/saved_searches` | List saved searches |
| POST | `/api/v1/saved_searches` | Save a search |
| DELETE | `/api/v1/saved_searches/:id` | Delete a saved search |

## Pipelines and recording rules

| Method | Endpoint | Description |
|---|---|---|
| GET | `/api/v1/pipelines` | List pipeline definitions |
| POST | `/api/v1/pipelines` | Create a pipeline |
| DELETE | `/api/v1/pipelines/:name` | Delete a pipeline |
| GET | `/api/v1/recording_rules` | List recording rule groups |
| POST | `/api/v1/recording_rules` | Create a recording rule group |
| DELETE | `/api/v1/recording_rules/:name` | Delete a recording rule group |
| GET | `/api/v1/rules/status` | Registry generation, definitions, load errors, and persisted ruler state |

> **Mutation gate.** Route, recording-rule, and pipeline `POST`/`PUT`/`DELETE`
> endpoints return `403` unless `alerts.allow_runtime_updates = true`. The OSS
> build has no authentication; enable this only behind an authenticating proxy.
> Mutations are persisted: routes to `data/alert_routes.json`, definitions to
> `rules/recording/` and `rules/pipelines/`, and both are reloaded at startup.
> Creating a definition validates its expressions and compiles pipelines first;
> a bad definition returns `400` with the stage or rule name and is not stored.

## Grafana SimpleJSON

| Method | Endpoint | Description |
|---|---|---|
| POST | `/search` | List available metrics |
| POST | `/query` | Execute a time-series query |
| POST | `/annotations` | Query annotations |
| POST | `/tag-keys` | List tag keys |
| POST | `/tag-values` | List tag values |

## Operations and self-monitoring

| Method | Endpoint | Description |
|---|---|---|
| GET | `/health` | Liveness check (static) |
| GET | `/ready` | Readiness check — reads the block index, `503` when storage is unusable |
| GET | `/metrics` | Prometheus metrics (self-monitoring) |
| GET | `/api/v1/stats` | Storage, buffer and config snapshot |
| GET | `/api/v1/ingest_rates` | Live per-signal rates: current plus 60s/5m/15m averages, `gap_secs`, sparkline via `history_secs` (default 180s, max 900s) |
| GET | `/debug/pprof/profile` | CPU profile (404 unless `telemetry.profiling_enabled`) |
| GET | `/debug/pprof/summary` | Profile summary (404 unless enabled) |
| GET | `/debug/pprof/memory` | Memory profile (404 unless enabled) |
| GET | `/oas` | OpenAPI 3.0 specification |
| GET | `/ui` | Built-in web console |

`/` redirects to `/ui`.

## Response envelope

Metrics queries answer in the Prometheus envelope. An instant query that matches
one series looks like this:

```json
{
  "status": "success",
  "data": {
    "resultType": "vector",
    "result": [
      {
        "metric": { "__name__": "http_requests_total", "method": "GET", "service.name": "demo" },
        "value": [1731000123.456, "42"],
        "values": [[1731000123.456, "42"]]
      }
    ],
    "totalSeriesCount": 1,
    "volumeSummary": []
  }
}
```

`values` is always populated (the embedded UI charts it for both query kinds);
`value` is present for instant vector results. An empty match returns
`"result": []` with `"status": "success"`.

## Errors

```json
{ "status": "error", "errorType": "bad_data", "error": "..." }
```

`400` for validation and OTLP decoding failures, `500` otherwise.
