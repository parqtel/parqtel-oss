# Parqtel Query Language Build-Out — Phase Limitations & Findings Log

Running log of limitations, correctness findings, and performance
observations discovered per phase. Review checkpoint after all phases.

Benchmark methodology: `scripts/gen_bench_data.py` (1.2M metric samples /
10K series / 200K logs / 30K spans via OTLP gRPC) + `scripts/bench_query.py`
(p50/p95/p99 per query). Baselines in `scripts/bench_results/`.

---

## Phase 0 — Range-selector correctness + windowed semantics (baseline: phase0.json)

### Correctness findings (fixed this phase)

| # | Finding | Severity | Fix |
|---|---|---|---|
| P0-F1 | `[range]` selectors parsed then **discarded** — `rate(x[1m])` ≡ `rate(x[1h])`; every windowed alert/dashboard computed over the wrong window | Critical | `split_range`/`parse_duration_ns` + `QueryPlan.range_ns` + `downsample_windowed` per-step lookback with reset correction and Prometheus-style extrapolation |
| P0-F2 | **SIGTERM caused total data loss**: only SIGINT (ctrl_c) triggered graceful shutdown; `docker stop`/k8s termination SIGTERM-killed the process, losing ALL buffered telemetry (verified: 1.43M items → 0 bytes on disk) | Critical (production) | shutdown now selects on SIGINT ∥ SIGTERM; verified 1.43M items flush to 3 Parquet blocks in 7.4s |
| P0-F3 | `parqtel_queries_executed_total`, `parqtel_query_errors_total`, `parqtel_query_duration_ms` existed but were **never recorded** (dead wiring) | High | recorded in instant + range handlers; `Histogram::record` implemented |
| P0-F4 | `parqtel_ingested_points_total`/`batches_received_total` never recorded on the **gRPC** path (HTTP only) | Medium | recorded in all three collector services |
| P0-F5 | `Histogram` had no `record()` method at all — the query-duration histogram could never observe anything | High | bucketed record with +Inf overflow |

### Known limitations carried forward (to review after all phases)

1. **Windowed evaluation is per-series flat-scan**: `downsample_windowed` binary-searches each step window but re-scans points per step — O(steps × log n) per series. Acceptable at 10K series × 120 samples; revisit if step counts explode.
2. **Single counter-reset approximation**: a window with a reset uses `dv = last_value` (one reset). Multiple resets inside one window under-count (Prometheus sums per-segment). Same behaviour as legacy; documented deviation.
3. **Extrapolation simplification**: Prometheus extrapolates to window edges capped at 10% of observed span each side; we cap the total slack at 1.1× span — equivalent for regular scrape intervals, slightly different for very sparse series.
4. **Instant lookback remains fixed 1 minute** (not Prometheus's 5m lookback-delta); benchmark datasets older than 60s return empty on instant queries by design (`topk`, `instant_selector` show 0 rows in the baseline for this reason — data is ~35–60 min old). Will revisit in the AST phase (staleness handling).
5. **rate over gauge semantics**: we don't distinguish counter vs gauge at storage level; callers must use the right function (PromQL has the same contract but alerts on hidden/unknown types; fine).
6. **Histogram metrics are stored as plain gauges** in the benchmark dataset (bucket-merging isn't implemented yet) — `histogram_quantile` over classic `_bucket` series still works via `le` labels when present; native-histogram merge is future work (per analysis non-goals).
7. **Non-counter ops with `[range]`** (e.g. `avg(x[5m])`) fall back to lookback-window aggregation rather than erroring — pragmatic choice, deviates from PromQL which requires `_over_time` for that shape; will align in the AST phase.
8. **Trace search limit**: `MAX_BLOCKS=64` scan cap per query — at benchmark volume (1 block) irrelevant; at scale, old traces may be unqueryable until compaction merges blocks. Revisit with block-tiering.

### Performance baseline (block-backed, release build, M-series, 8.6MB zstd data)

| Query | p50 | p95 | Notes |
|---|---|---|---|
| selector_all (160 series) | 242ms | 273ms | full-scan decode dominates |
| selector_match (4 series) | 211ms | 214ms | same blocks scanned — matcher is post-scan |
| sum by service.name | 245ms | 247ms | grouping cheap after scan |
| rate(…[5m]) | 242ms | 245ms | windowing adds no measurable cost vs selector |
| rate(…[1h]) | 245ms | 253ms | [1m]≠[1h] verified semantically; cost identical |
| label_values (service.name) | 863ms | **1517ms** | **hot spot**: scans ALL log blocks (see below) |
| logs_plain (500 rows) | 182ms | 183ms | |
| logs_sev / logs_search | 159ms | 164ms | |
| traces_search (200 spans) | 8ms | 11ms | trace block is small |

**Flagged for the review**: `label_values` is the worst query (p95 1.5s) —
it enumerates label values by scanning log blocks (`get_log_field_values`)
plus block metadata; needs a label-value index or metadata cache in a later
phase. Metric series queries all cluster at ~210-270ms dominated by Parquet
decode of the 5.5MB metrics block; predicate pushdown (column pruning) is
the obvious next lever and is scheduled with the AST phase work.

### Benchmark infra gaps (accepted for now)

- Instant-query rows are 0 for aged datasets (expected; see limitation 4) — the suite keeps them to catch lookback regressions.
- Dataset is single-block per signal; multi-block scan fan-out (and the 64-block cap) isn't exercised yet. Phase 1 benchmark should seed across ≥3 flush windows.
- Ingestion throughput measured at seed time (metrics 272K samples/s via gRPC JSON-encode path is the generator bottleneck, not the server; server-side counters are the source of truth: 1.43M points/9.8s wall including logs+traces).
