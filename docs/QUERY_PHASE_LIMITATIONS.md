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

---

## Phase 1A — Composable AST engine (baseline: phase1a.json)

### Shipped

- Pratt parser (lexer with duration tokens, precedence table, vector
  matching, by/without prefix+postfix, subquery `[r:step]`, offset, bool)
- Per-step AST evaluator: binary ops (scalar/scalar, scalar/vector,
  vector/vector with on/ignoring + group_left/right), and/or/unless,
  range family (rate/increase/irate/delta + 12 `_over_time` fns,
  changes, resets, deriv), instant fns (math, round/clamp,
  label_replace/label_join, absent, sort, histogram_quantile, scalar/
  vector/time), aggregations with by/without, topk/bottomk/quantile
- Handler dispatch: composed/AST-only queries → execute_ast; legacy
  shapes unchanged (zero regression for existing dashboards)

### Correctness findings (fixed this phase)

| # | Finding | Fix |
|---|---|---|
| P1A-F1 | `needs_ast` classified `avg_over_time(x[5m])` as legacy-compatible (Range-of-Selector arg shape), sending an AST-only function to the legacy parser — silent 0-row results instead of either executing it or erroring | dispatch now checks `is_legacy_function` first; regression-tested |
| P1A-F2 | AST instant queries evaluated at `end-60s` instead of AT `time` — aged-data instants returned stale-window rows | instant path evaluates at `end_ns` with lookback |
| P1A-F3 | Test harness (`default_for_tests`) dropped the block-metadata receiver — flushed blocks were invisible to block-backed queries in tests (mirrors the F10 alert-channel bug class) | index-update task spawned in test state, mirroring main.rs |

### Known limitations carried forward

1. **Single binary-expression evaluation per step** — the evaluator re-walks
   the tree per step; range windows are re-partitioned per step per series.
   Fine at benchmark scale (AST queries ≈ legacy cost); revisit with
   result caching if step counts grow (subquery matrices).
2. **Subquery inner evaluation is uncached** — `x[1h:30s]` re-evaluates the
   inner expression per sub-step per outer step. Prometheus caches per
   (expr, range). Left until a consumer needs it.
3. **`@` modifier rejected** (clear error) — parsed, not evaluated.
4. **`count_values`, `timestamp`, `absent_over_time`, `predict_linear`,
   `double_exponential_smoothing`, date fns** — not yet in the evaluator
   (parser accepts the shapes; eval errors clearly). Next tranche.
5. **Vector matching `result_labels` simplification**: result takes LHS
   labels minus `__name__`; Prometheus keeps only on()-labels for on()
   matches. Verified per-op semantics still need conformance-corpus runs.
6. **histogram_quantile** operates on pre-evaluated instant vectors
   (le-label buckets); native histograms unsupported (unchanged non-goal).
7. **Legacy dispatch retained intentionally** — two engines in flight;
   the conformance corpus (planned) will gate a future legacy removal.
8. **Instant-selector lookback is 60s (AST) vs Prometheus 5m** —
   unchanged from Phase 0's documented deviation.

### Performance (phase1a.json vs phase0.json, same dataset, block-backed)

- All legacy queries within noise of baseline (139–160ms p50; slightly
  faster than phase0's 210–250ms — block-cache warmth, not code).
- AST composed queries: sum(rate) 172ms, ratio 136ms, topk(rate) 172ms,
  avg_over_time 85ms — no measurable AST overhead vs legacy equivalents
  at this scale (single scan dominates).
- `label_values` remains the flagged hot spot (p95 ~1.3s) — unchanged;
  label-value index still scheduled.
