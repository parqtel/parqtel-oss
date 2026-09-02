# Parqtel Query Functions Reference

Parqtel exposes a Prometheus-compatible query API at `/api/v1/query` (instant) and `/api/v1/query_range` (range). All examples below use `localhost:9090` — the default compose port.

---

## Label Matchers

Supported in every selector inside `{}`.

| Operator | Example | Meaning |
|----------|---------|---------|
| `=`  | `{method="GET"}` | Exact equality |
| `!=` | `{status!="500"}` | Not equal |
| `=~` | `{host=~"api-.*"}` | Regex match (RE2) |
| `!~` | `{host!~"api-.*"}` | Regex not match |

Special label `__name__` matches the metric name: `{__name__="cpu_usage"}`.

**Resource-attribute labels**: `service.name`, `service.version`, and `k8s.*` resource attributes are stored in dedicated Parquet columns and injected back as labels, so matchers like `http_requests_total{service.name="api"}` work on both freshly-buffered and flushed data.

**Instant-query lookback**: `/api/v1/query` evaluates a 1-minute window ending at `time`. Use `/api/v1/query_range` for older data.

---

## Range Aggregations

Applied per time-step window. Require a `step` parameter on range queries.

### avg

Mean of all sample values in the window.

```
avg(metric_name{labels})
avg(http_requests_total{method="GET"})
sum(http_requests_total by (service))   # leading by() form
```

```bash
curl "http://localhost:9090/api/v1/query_range?query=avg(http_requests_total{method=%22GET%22})&start=1700000000&end=1700003600&step=60s"
```

### sum

Sum of all values in the window, with optional `by`/`without` grouping.

```
sum(metric_name{labels})
sum(http_requests_total{} by (service, method))
sum without (instance) (http_requests_total)
```

```bash
curl "http://localhost:9090/api/v1/query_range?query=sum(http_requests_total%7B%7D%20by%20(service))&start=1700000000&end=1700003600&step=60s"
```

### min / max

Minimum or maximum value across the window.

```
min(cpu_usage{host=~"prod-.*"})
max(memory_bytes{service="api"})
```

```bash
curl "http://localhost:9090/api/v1/query?query=max(cpu_usage{host%3D~%22prod-.*%22})"
```

### count

Number of data points in the window.

```
count(http_requests_total{status="500"})
```

```bash
curl "http://localhost:9090/api/v1/query?query=count(http_requests_total{status=%22500%22})"
```

### stddev / stdvar

Population standard deviation and variance.

```
stddev(response_time_ms{service="checkout"})
stdvar(cpu_usage{})
```

```bash
curl "http://localhost:9090/api/v1/query_range?query=stddev(response_time_ms)&start=1700000000&end=1700003600&step=60s"
```

---

## Range Functions

Operate on the raw counter/gauge samples within the window.

### rate

Per-second average rate of increase of a counter. Requires ≥2 samples.

```
rate(metric_name[range])
rate(http_requests_total{service="api"}[5m])
```

> The range selector `[5m]` is parsed but the actual window is controlled by the query `step`. It is included for Prometheus syntax compatibility.

```bash
curl "http://localhost:9090/api/v1/query_range?query=rate(http_requests_total%5B5m%5D)&start=1700000000&end=1700003600&step=60s"
```

### irate

Instantaneous rate using only the **last two samples** in the window. More responsive to spikes than `rate`; handles counter resets.

```
irate(metric_name[range])
irate(http_requests_total{service="api"}[1m])
```

```bash
curl "http://localhost:9090/api/v1/query_range?query=irate(http_requests_total%5B1m%5D)&start=1700000000&end=1700003600&step=15s"
```

### increase

Total increase of a counter over the window (`last - first`, reset-safe).

```
increase(metric_name[range])
increase(http_requests_total{status="200"}[5m])
```

```bash
curl "http://localhost:9090/api/v1/query_range?query=increase(http_requests_total%5B5m%5D)&start=1700000000&end=1700003600&step=60s"
```

### delta

Difference between the last and first sample (for **gauges**, not counters; can be negative).

```
delta(metric_name[range])
delta(temperature_celsius{sensor="room1"}[10m])
```

```bash
curl "http://localhost:9090/api/v1/query_range?query=delta(temperature_celsius%5B10m%5D)&start=1700000000&end=1700003600&step=60s"
```

---

## Histogram Function

### histogram_quantile

Estimates the φ-quantile from OTel cumulative histogram buckets. φ must be in the open interval `(0, 1)`.

```
histogram_quantile(φ, metric_name{labels})
histogram_quantile(0.95, http_request_duration_ms{service="api"})
histogram_quantile(0.5, latency_ms)
```

```bash
# p95 latency
curl "http://localhost:9090/api/v1/query?query=histogram_quantile(0.95,%20http_request_duration_ms)"
```

---

## Instant Transforms

Applied to the last value in the window. No `step` required.

### abs

Absolute value.

```
abs(metric_name{labels})
abs(net_bytes_delta{interface="eth0"})
```

```bash
curl "http://localhost:9090/api/v1/query?query=abs(net_bytes_delta)"
```

### ceil / floor

Round up or down to the nearest integer.

```
ceil(cpu_fraction{host="web-01"})
floor(memory_utilization{})
```

```bash
curl "http://localhost:9090/api/v1/query?query=ceil(cpu_fraction)"
```

### round

Round to the nearest integer, or to the nearest `to_nearest` multiple.

```
round(metric_name)
round(metric_name, to_nearest)
round(latency_ms, 5)       # rounds to nearest 5ms
round(cpu_fraction, 0.01)  # rounds to 2 decimal places
```

```bash
curl "http://localhost:9090/api/v1/query?query=round(latency_ms,%200.5)"
```

### clamp_min

Clamps all values to a minimum floor.

```
clamp_min(metric_name{labels}, min)
clamp_min(cpu_usage{}, 0)
```

```bash
curl "http://localhost:9090/api/v1/query?query=clamp_min(cpu_usage,%200.0)"
```

### clamp_max

Clamps all values to a maximum ceiling.

```
clamp_max(metric_name{labels}, max)
clamp_max(cpu_usage{}, 100)
```

```bash
curl "http://localhost:9090/api/v1/query?query=clamp_max(cpu_usage,%20100.0)"
```

---

## Ranking Functions

### topk

Returns the top N series ranked by their **last sample value** (highest first).

```
topk(N, metric_name{labels})
topk(5, http_requests_total{status="200"})
```

```bash
curl "http://localhost:9090/api/v1/query?query=topk(5,%20http_requests_total)"
```

### bottomk

Returns the bottom N series ranked by their **last sample value** (lowest first).

```
bottomk(N, metric_name{labels})
bottomk(3, cpu_usage{env="prod"})
```

```bash
curl "http://localhost:9090/api/v1/query?query=bottomk(3,%20cpu_usage{env=%22prod%22})"
```

---

## Grouping Clauses

`by` and `without` collapse the label space before aggregating. Compatible with `sum`, `avg`, `min`, `max`, `count`, `stddev`, `stdvar`.

### by

Keep only the listed labels; all other labels are dropped. Series with the same resulting label set are merged.

```
sum(http_requests_total{} by (service, status))
avg by (host) (cpu_usage{env="prod"})
```

```bash
curl "http://localhost:9090/api/v1/query_range?query=sum(http_requests_total%7B%7D%20by%20(service))&start=1700000000&end=1700003600&step=60s"
```

### without

Drop the listed labels; keep all others.

```
sum without (instance, pod) (http_requests_total)
avg without (host) (cpu_usage{env="prod"})
```

```bash
curl "http://localhost:9090/api/v1/query_range?query=sum%20without%20(instance)%20(http_requests_total)&start=1700000000&end=1700003600&step=60s"
```

---

## Label Manipulation

### label_replace

Applies a regex substitution to a source label and writes the result into a destination label.

```
label_replace(metric, "dst_label", "replacement", "src_label", "regex")
```

- `dst_label` — label to write to (created if absent)
- `replacement` — replacement string; `$1`, `$2` refer to capture groups
- `src_label` — label to read from
- `regex` — RE2 regular expression applied to the source value

```
label_replace(cpu_usage, "short_host", "$1", "host", "([^.]+).*")
label_replace(http_requests_total, "path_prefix", "$1", "path", "(/[^/]+).*")
```

```bash
curl "http://localhost:9090/api/v1/query?query=label_replace(cpu_usage,%20%22short_host%22,%20%22\$1%22,%20%22host%22,%20%22(%5B%5E.%5D%2B).*%22)"
```

---

## Constraints

| Constraint | Value |
|-----------|-------|
| Max time range | 30 days |
| Max series returned | 500 (dev) — configurable via `query.max_series` |
| Max samples per series | 5000 (dev) — configurable via `query.max_samples_per_series` |
| Quantile range | Open interval `(0.0, 1.0)` |
| `rate` / `irate` / `increase` minimum data | ≥ 2 samples in window |
| Step durations | `s` (seconds), `m` (minutes), `h` (hours), `d` (days) |

---

## Not Yet Supported

The following standard PromQL features are not currently implemented:

- Binary operators between two metrics (`metric_a / metric_b`)
- `predict_linear`, `deriv`, `idelta`, `absent`, `changes`, `resets`
- Time functions: `time()`, `timestamp()`, `year()`, `month()`, etc.
- Subqueries (`metric[5m:1m]`)
- `offset` modifier (`metric offset 5m`)
- `label_join`
