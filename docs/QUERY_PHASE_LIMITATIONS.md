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

---

## Phase 1B — ParqtelQL log/trace search + saved searches (baseline: phase1b.json)

### Shipped

- Lenient search grammar (terms/phrases/wildcards/exclusion, field ops,
  ranges, exists, regex, severity thresholds) shared by logs and traces
- /api/v1/logs ParqtelQL dispatch (selector shapes stay legacy); the
  `severity_min`/`search` HTTP params fold into clauses — with a
  legacy-`{}`-selector adapter so combined forms keep working
- /v1/traces/search?q= span predicates (service/status/kind/duration/
  operation/attr.*) applied post-scan
- Server-side saved searches (API + data-dir persistence)

### Correctness findings (fixed)

| # | Finding | Fix |
|---|---|---|
| P1B-F1 | `{}` + severity_min param took the ParqtelQL path where `{}` parsed as a body term "{}" — 0 rows for the most common param combo | empty/`{}` parses as no-constraint; `{a="b"}` selectors convert to clauses |
| P1B-F2 | `severity>=WARN` in comparison position tried numeric parse first — word severities errored into the lenient fallback | severity-word check precedes numeric parse for comparison ops |

### Known limitations carried forward

1. **AND-only semantics**: `OR`/parenthesized grouping tokenize but every
   clause remains conjunctive (documented inline); a boolean expression
   tree is the Phase-2 pipeline's first item.
2. **Bare-word dispatch**: a single identifier like `error` is ParqtelQL
   (body search) — intentional ClickStack parity, but means a metric-name
   log-matcher query must be written `{...}` or `key="value"` (no spaces).
   The space-free heuristic (`contains("=\"")`) guides dispatch.
3. **Trace predicates are post-scan**: 200-span cap applies BEFORE
   filtering, so `q` can filter to zero even when more matches exist in
   older blocks. Push-down into query_traces is the fix (scheduled with
   the trace search limit raise).
4. **`field:value` with `:` inside values** (URLs) needs quoting.
5. **Negation on non-Eq clauses** downgrades to positive matching
   (lenient); only `!=`/`-term` negate cleanly.
6. **Saved searches store, no share/namespace model** (single tenant,
   OSS scope).

### Performance (phase1b.json)

- ParqtelQL log queries: 156-158ms p50 — indistinguishable from the
  plain path (post-scan predicate cost is noise vs block decode).
- Combined clause+term query: 158ms, 449/500 precise rows.
- Trace predicates: 8-9ms (2× over the unfiltered 4-8ms; post-scan on
  200 spans).
- Legacy paths unchanged (no regression vs phase1a).

---

## Phase 2 — Pipeline engine + boolean search + trace push-down (baseline: phase2.json)

### Shipped

- Pipeline: `fetch logs | filter | parse ".. as f" | stats [by] [interval=]
  | limit | correlate traces window=` via POST /v1/search
- Boolean predicate trees (OR/AND/NOT/parens) for search; AND-only
  queries stay flat-shape compatible
- Trace predicates pushed down INTO the scan (before the result cap;
  filtered scan cap raised to 10k spans)

### Correctness findings (fixed)

| # | Finding | Fix |
|---|---|---|
| P2-F1 | Tree-parser field-clause index arithmetic consumed Term+3 (op+value+1 extra), cascading into misparse → lenient fallback for AND queries with field clauses | normalized the Term/Op/Value advance to match the flat parser |
| P2-F2 | `by service` grouped nothing — row fields carry `service.name`, no alias | rows expose a `service` alias for grouping/filtering |
| P2-F3 | stats-then-filter ran (stats returns early) instead of erroring | stage-order validated at parse time |

### Known limitations carried forward

1. **Pipeline fetch targets**: metrics/traces parse but materialize empty
   rows — logs-first scope this phase; metrics rows need series-to-row
   mapping and traces need span-to-row mapping (both mechanical).
2. **Pipeline row cost**: full-scan materialization (200K logs → rows →
   stages) runs 2× a limited log query (341-423ms p50) — push-down of
   limit/filter into the scan (like query_traces_filtered) is the next
   optimization.
3. **correlate is trace_id-only** — no service+window fallback join yet;
   window= parses but is unused in the current join.
4. **`interval` buckets are aligned to ts/interval** — no
   timestamp-snapped buckets; series values include Null gaps.
5. **Pipeline filter reuses row-level predicate evaluation** — SeverityMin
   needs a severity_number field on rows (present for logs fetch).
6. **Saved searches are not yet pipeline-aware in the UI** — API accepts
   any query text; UI surfacing is commercial-scope.

### Performance (phase2.json)

- Pipelines: 341-423ms p50 (count-by-service 362ms, filter+OR 341ms,
  parse+p95 423ms) at 200K logs — dominated by row materialization
- Trace predicates: 200 rows at 16ms (previously 8 rows post-scan —
  push-down fixed both coverage AND kept latency ~2× the unfiltered scan)
- Log/traces/metrics suites: no regression vs phase1b

---

## Wave 1 — Correctness (baseline: wave1.json; corpus: 88/88)

Review plan (docs/QUERY_LIMITATIONS_REVIEW.md) Wave 1, all approved
recommendations applied:

| Item | Resolution |
|---|---|
| G0 conformance corpus | 88-case corpus (28 hand-derived core + 60 parameterized variants: windows/over_time-family/transforms/agg-matrix/matcher-matrix/binary-matrix/composition), deterministic fixtures, existence-mode + exact-value checks with tolerance; runs in cargo test — the reference gate for every future semantic change and the legacy-retirement decision |
| G1 5m lookback | `query.lookback_delta_ns` config (default 5m, Prometheus semantics) threaded AST evaluator → executor → both handlers; benchmark proof: instant queries now return rows on the 35-60min-old dataset (0 rows in all prior baselines) |
| G2 avg(x[5m]) fallback | legacy parser rejects `[range]` on non-counter fns with the `_over_time` suggestion; windowed path yields no samples; regression-tested |
| G3 on() label projection | result keeps ONLY on-labels (+group_left extras); ignoring()/default keep LHS minus __name__/ignored; projection test verifies pod dropped |
| G4 multi-reset rate | per-segment increase accumulation in BOTH engines (eval.rs + aggregation.rs); two-reset test: 110/60 ≈ 1.833/s where the old code returned 20/60 |
| G12 NOT on clauses | Clause::Not flattens single-clause negation exactly (ranges/comparisons/exists invert); tree path unchanged for compound NOT |

Corpus findings while calibrating (all engine-correct, expectations fixed):
fixture counter slope is per-interval not per-second; stddev/stdvar drop
single-sample groups (Prometheus parity); constant counters rate to 0
(histogram_quantile fixture switched to instant form); on() join with
missing RHS side correctly drops the unmatched group.

Benchmark (wave1.json): zero regressions; instant queries return rows
(G1); all 29 queries green.

---

## Wave 2 — Latency (baseline: wave2.json)

Review plan Wave 2, approved recommendations applied:

| Item | Resolution |
|---|---|
| G7 multi-block benchmark | `--blocks=N` generator option (disjoint time windows per pass; short block-duration config cuts one block per pass); validated 3 log blocks via the label-value index under fan-out |
| G5 label-value hot spot | **830ms p50 / 1.3-5.3s p95 → 0.3ms p50 / 0.4ms p95 (~3000×)**: flush-time label-value dictionaries (BTreeMap per field, 10K value cap) in BlockMetadata for BOTH metrics and logs; both `list_label_values` and `get_log_field_values` merge metadata first and scan only pre-index blocks |
| G8 trace scan cap | 64 → 256 blocks (covers a week of hourly blocks / a day of 5-min trace flushes); verified against the multi-block dataset |
| G6 pipeline push-down | first `filter` stage applies during row materialization (non-matching logs never allocate rows); measured −7% on filter+stats pipelines |

### Correctness findings (fixed)

| # | Finding | Fix |
|---|---|---|
| W2-F1 | **Benchmark generator collapsed log resources**: a 25K-record batch carried ONE ResourceLogs (the last record's service) — all prior baselines seeded logs with only 8 of 40 services; block service counts were wrong since Phase 0 | one ResourceLogs per service per batch |
| W2-F2 | Multi-block pass shifting moved seeded data outside the suite's 50-min query window (benchmark-protocol artifact, not engine) | documented; multi-block mode is for index/fan-out validation, suite baselines stay single-pass |

### Carried forward (updated)

- `label_values` falls back to full scan for pre-index blocks; after one
  retention cycle the fallback disappears naturally. A compaction merge of
  label_values (currently Default::default() on compacted blocks — G2 of
  the next review) is scheduled with compaction work.
- Trace predicate scan cap (10k spans) unchanged.

---

## Wave 3 — Coverage (baseline: wave3.json; corpus: 98/98)

| Item | Resolution |
|---|---|
| G9a pipeline fetch metrics | series→rows: one row per data point (`__name__`, `value`, timestamp, labels incl. service alias); metric names from index + buffer; stats/filter/pipeline stages compose over metric rows (sum(value) by service verified e2e) |
| G9b pipeline fetch traces | span→rows: duration_ms, status, kind, name, trace_id, attributes; p95(duration) + status filters verified e2e |
| G10 PromQL functions | absent_over_time (with equality-label projection + empty-window semantics), predict_linear (OLS regression extrapolation), double_exponential_smoothing/holt_winters (trend-corrected), count_values (per-value series counts), date fns (hour/day_of_week/day_of_month/day_of_year/days_in_month/month/year); changes/resets/deriv were already present; timestamp() remains blocked on per-sample time retention (documented) |
| G11 correlate fallback | trace_id join first; rows without trace_id join via service + window (±window_ns of the row timestamp); `correlate logs` onto metric/trace rows with log_count + max_severity_number enrichment |

### Correctness findings (fixed)

| # | Finding | Fix |
|---|---|---|
| W3-F1 | fetch metrics saw only INDEX metric names — buffered metrics (unflushed) were invisible in tests | names merged from index + buffer |
| W3-F2 | `count_values("label", x)` failed to parse — string literals unsupported in expression position | parse_primary accepts string literals as NaN scalars (param-position only) |

### Carried forward

1. **fetch metrics row cost**: 1.2M points → 1.2M rows = 1.8s p50 for
   `sum(value) by service`. The per-metric scan loop is the obvious
   next optimization (parallelize + push stats into the scan); the G6
   filter push-down applies but stats materialization dominates. Next
   review.
2. **correlate fallback is O(rows × spans)** linear scan per unjoined
   row — fine at 10K spans; a service+time index would be needed at
   larger scale.
3. **timestamp()** still requires per-sample time retention in
   InstantVector (architectural, queued with subquery caching).
4. **fetch targets ignore their own limit before stats** — a `limit`
   stage before `stats` truncates rows (correct), but the fetch itself
   is unbounded; a fetch-level cap belongs with the row-cost fix.

---

## Wave 4 — Polish + retirement-decision data (baseline: wave4.json; corpus: 98/98)

| Item | Resolution |
|---|---|
| G13 body prefix | `body:value` / `body=value` use contains semantics (case-insensitive) — the unambiguous form of bare-term search; dispatch heuristic documented in-query via the explicit prefix |
| G14 colon-in-values | `:` operator now scans the value term to whitespace without breaking on colons — `url:https://x:8443/path` parses as url Eq "https://x:8443/path" (verified exact); plain `field:value` unchanged |
| G15 interval buckets | wall-clock-snapped (div_euclid) bucket timestamps; per-bucket gaps explicit |
| G16 | scoped down per review: subquery/pipeline caching deferred until usage exists; fetch-metrics row cost carried with its optimization path |
| Retirement analysis | docs/LEGACY_ENGINE_RETIREMENT.md: corpus routing audit (57 AST / 41 legacy, all legacy-routed cases parse clean, both engines aligned by Wave 1), recommendation = flag-gated default flip now + deletion after one release; execution checklist included |

### Benchmark (wave4.json)
All 31 queries green; every measurement within noise of wave3 (no
regression gate tripped). Corpus 98/98; routing audit tests added as
permanent CI guards.
