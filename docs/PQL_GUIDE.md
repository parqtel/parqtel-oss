# Parqtel Query Language (PQL) — End-User Guide

**Version:** 0.1.0 · **Applies to:** Parqtel 0.1.0

PQL is the umbrella name for Parqtel's three query surfaces:

| Surface | Where | What it's for |
|---|---|---|
| **ParQL (PromQL-compatible)** | Metrics queries — `/api/v1/query`, `/api/v1/query_range`, Grafana, alert rules | Time-series selectors, functions, aggregations, binary math |
| **ParqtelQL (search)** | Logs (`/api/v1/logs?query=`) and traces (`/v1/traces/search?q=`) | Lenient search-box queries: terms, field filters, ranges |
| **PQL Pipeline** | `POST /v1/search` | Multi-stage analytics: `fetch … \| filter … \| stats …` across all three signals |

All three share one engine family, one config, and Prometheus-compatible
response shapes (Grafana works out of the box).

---

## 1. ParQL — Metrics (PromQL-compatible)

Parqtel implements the PromQL grammar your dashboards already speak:
selectors, the counter family with true `[range]` windows, the
`_over_time` family, binary ops with vector matching, and aggregations.

### 1.1 Selectors

```promql
cpu_usage
http_requests_total{service.name="api-gateway"}
http_requests_total{service.name=~"api-.*", http.method!="POST"}
```

- Label matchers: `=` `!=` `=~` (regex) `!~`
- `service.name` (resource attributes) are first-class labels
- Instant selectors use a **5-minute lookback** (Prometheus default,
  configurable via `query.lookback_delta_ns`)

### 1.2 Range functions — `[range]` windows are honored

```promql
rate(http_requests_total[5m])            # per-second rate over 5m
increase(http_requests_total[1h])        # absolute growth over 1h
irate(cpu_usage[1m])                     # instant rate (last 2 samples)
delta(memory_usage[10m])                 # raw difference (gauges)
```

Ranges: `5m`, `1h30m`, `2d`, `250ms`, or bare seconds (`[300]`).
Counter resets inside a window are handled **per-segment** (multi-reset
safe), with Prometheus-style edge extrapolation.

> `rate(x[1m])` and `rate(x[1h])` return different numbers — the window
> is the query's contract.

### 1.3 The `_over_time` family

```promql
avg_over_time(cpu_usage[10m])
max_over_time(http_request_duration[1h])
sum_over_time(disk_bytes_written[1d])
count_over_time(http_requests_total[5m])
last_over_time(cache_hits[5m])
stddev_over_time(latency_ms[30m])
changes(config_reloads[1h])              # value-change count
resets(counter_with_crashes[1d])         # counter-reset count
deriv(trend_gauge[6h])                   # per-second slope
predict_linear(disk_free[1h], 4*3600)    # value 4h from now
double_exponential_smoothing(trendy[1h], 0.1, 0.3)
absent_over_time(job_missing{team="core"}[10m])  # 1 if no data
```

### 1.4 Aggregations with `by` / `without`

```promql
sum by (service.name) (http_requests_total)
avg without (instance) (cpu_usage)
count by (region) (up)
topk(5, sum by (service.name) (rate(http_requests_total[5m])))
bottomk(3, memory_usage)
quantile(0.99, http_request_duration_ms)
count_values("value", up)                # series count per distinct value
```

### 1.5 Composition — the reason the AST engine exists

```promql
# THE canonical RED query (was impossible before Parqtel 0.1.0):
sum by (service.name) (rate(http_requests_total[5m]))

# Error ratio:
sum by (service.name) (rate(http_requests_errors_total[5m]))
  / sum by (service.name) (rate(http_requests_total[5m]))

# p90 latency from classic histogram buckets:
histogram_quantile(0.9,
  sum by (le, service.name) (rate(http_request_duration_bucket[5m])))

# Join two metrics on shared labels (keep only service in the result):
errors_total * on(service.name) group_left(team) team_info
```

Binary operators: `+ - * / % ^`, comparisons (`> >= < <= == !=`, with
optional `bool` modifier returning 0/1), set ops (`and or unless`),
and vector matching (`on(labels)`, `ignoring(labels)`,
`group_left/right`).

### 1.6 Instant transforms

```promql
abs(-x)  ceil(x)  floor(x)  round(x, 0.5)
clamp(x, 0, 100)  clamp_min(x, 0)  clamp_max(x, 10)
sqrt(x)  exp(x)  ln(x)  log2(x)  log10(x)  sgn(x)
label_replace(x, "team", "$1-tls", "service", "(.*)-gateway")
label_join(x, "endpoint", "/", "service", "route")
sort(x)  sort_desc(x)
scalar(x)  vector(42)  time()
absent(nonexistent{job="x"})     # 1 with {job="x"} when no series match
```

Date helpers over the evaluation timestamp:
`hour() day_of_week() day_of_month() day_of_year() days_in_month() month() year()`

---

## 2. ParqtelQL — Log & Trace Search

A lenient, search-box-first grammar (ClickStack-inspired). **It never
errors on input** — unknown tokens become body search terms.

### 2.1 Logs

```
GET /api/v1/logs?query=<ParqtelQL>&start=<unix_s>&end=<unix_s>&limit=500
```

**Terms** (case-insensitive body search):

```
error
"connection refused by upstream"      # exact phrase
*timeout*                             # wildcard
error timeout -retry                  # exclude 'retry'
timeout OR latency                    # boolean (OR of ANDs)
(service=api AND error) OR (service=web AND timeout)
NOT service=api
```

**Field filters**:

```
service=api-gateway                   # or service:api-gateway
severity>=ERROR                       # thresholds: TRACE DEBUG INFO WARN ERROR
severity>=WARN
attr.http.status_code=500            # span/log attributes
res.k8s.pod_name=api-7f9-xyz          # resource attributes
url:https://api.example.com:8443/health   # colons inside values work
duration_ms:>500
duration_ms:200-500                   # numeric range
trace_id:*                            # field exists
body:timeout                          # explicit body-contains search
```

Fields resolve in order: dedicated columns (`body`, `severity`,
`service`, `trace_id`, `span_id`) → `attr.KEY` (attributes) → `res.KEY`
(resource attributes) → bare attribute fallback.

**Precedence:** `NOT` > implicit-AND > `OR`. Parentheses group.

### 2.2 Traces

```
GET /v1/traces/search?start=…&end=…&q=<ParqtelQL>
```

Same grammar, span fields:

```
service=checkout-service status=ERROR
duration>500                          # milliseconds
duration:100-2000 kind=server
operation="GET /orders/{id}"
attr.db.system=postgresql
```

Predicates are pushed **into the scan** — filters don't lose matches to
result caps.

### 2.3 Saved searches

```
POST /api/v1/saved_searches   {"name": "api errors", "signal": "logs", "query": "service=api severity>=ERROR", "range_minutes": 60}
GET  /api/v1/saved_searches
DELETE /api/v1/saved_searches/{id}
```

Persisted server-side; shared across browsers and restarts.

---

## 3. PQL Pipeline — Cross-Signal Analytics

The Grail-shaped command language, run via `POST /v1/search`:

```json
POST /v1/search
{"query": "<pipeline>", "start": <unix_s>, "end": <unix_s>}
```

### 3.1 Stages

```
fetch <logs|metrics|traces>
  | filter <ParqtelQL predicate>
  | parse "<regex>" as <field>
  | stats <aggs> [by <fields>] [interval=<dur>]
  | limit <n>
  | correlate <traces|logs> [window=<dur>]
```

### 3.2 Examples

**Log error rate by service:**

```
fetch logs
  | filter severity>=ERROR
  | stats count() by service interval=5m
```

**Extract latency from raw bodies, p95 per service:**

```
fetch logs
  | filter service=api-gateway
  | parse "duration_ms=(\d+)" as duration_ms
  | stats p95(duration_ms) by service
```

**Error ratio + trace context (the cross-signal headline):**

```
fetch metrics
  | filter service=api
  | correlate traces window=10m
  | stats max(correlated.span_count), max(correlated.trace_duration_ms) by service
```

**Slow failing traces:**

```
fetch traces
  | filter status=ERROR duration>500
  | stats count() by service, operation interval=1h
```

**Join logs onto metrics for context enrichment:**

```
fetch metrics
  | filter __name__=http_requests_total
  | correlate logs window=5m
  | stats max(correlated.max_severity_number) by service
```

### 3.3 Stats functions

`count()`, `avg(f)`, `min(f)`, `max(f)`, `sum(f)`, `p50(f)`, `p95(f)`,
`p99(f)` — with `as` aliases (`count() as total`) and `interval=` for
time-bucketed (timeseries) output.

### 3.4 Rules

- `fetch` first, once; `stats` is terminal for row filters
- `filter` accepts the full ParqtelQL boolean tree (OR/NOT/parens)
- `correlate` joins by `trace_id` first, service+window fallback
- Buckets are wall-clock snapped to `interval` boundaries

---

## 4. Configuration

```toml
[query]
max_series = 1000
max_samples_per_series = 10000
timeout_secs = 30
lookback_delta_ns = 300000000000     # instant lookback (5m, Prometheus default)
```

## 5. What's intentionally different

| Behavior | Parqtel | Prometheus |
|---|---|---|
| Instant lookback | 5m (configurable) | 5m |
| Bare-word log queries | Body search (ClickStack parity) | n/a |
| `timestamp()` | Not yet (needs per-sample time retention) | Yes |
| `@` modifier | Rejected with clear error | Yes |

See `docs/QUERY_PHASE_LIMITATIONS.md` for the full fidelity log and
`docs/LEGACY_ENGINE_RETIREMENT.md` for the engine-unification plan.
