# Parqtel Query Engine — Deep Capability Analysis vs Market Leaders

**Author:** Observability Specialist
**Status:** Analysis + Gap-Closure Plan
**Date:** September 2026
**Scope:** Metrics (PromQL), logs, and traces search across `parqtel-query` (~2,750 LOC), benchmarked against Prometheus (PromQL), ClickHouse (ClickStack), Elasticsearch (Lucene/KQL/DSL), and Dynatrace Grail (DQL).

---

## 1. Executive Summary

Parqtel's query engine is a **credible PromQL subset** — the 20-operator core covers the queries that appear on 90% of real-world dashboards (`rate`, `sum by`, `histogram_quantile`, `topk`). But the analysis surfaced one **correctness bug** (range-selector durations like `[5m]` are parsed and then silently discarded, so `rate(x[5m])` does not actually evaluate a 5-minute window), and three **structural gaps** that separate it from every market leader:

1. **No query composition** — no nesting (`sum(rate(x[5m]))` fails), no binary operators (`a / b`), no vector matching (`on()`, `group_left`), no subqueries. Every market leader composes; parqtel dispatches one function.
2. **Logs have no real query language** — label matchers plus a single case-insensitive substring search. ClickStack/Elasticsearch/DQL all offer boolean term search, wildcards, `field:value` matching, and range/existence operators.
3. **Traces are browsable but not queryable** — time-range + trace_id only; no service/operation/duration/status/attribute predicates, which even Jaeger's basic UI offers.

The plan (§6) closes these in three phases: **fix the windowing semantics first** (P0 — it's a correctness issue), then a **unified pipeline query engine** (P1 — composition + a `ParqtelQL` log language), then **trace search + analytics** (P2). Full Prometheus parity is the wrong goal; **"PromQL-compatible metrics + Lucene-class log search + span-search + Grail-style cross-signal joins"** is the defensible position that matches Parqtel's single-binary story.

---

## 2. Parqtel Current State (verified in code)

### 2.1 Metrics — PromQL subset

**Parser** (`matcher.rs`, 712 LOC): hand-rolled single-function dispatcher. `parse_query` strips one function name (`strip_fn`), parses one selector with label matchers, optional `by()/without()` grouping, and at most a few special forms (`histogram_quantile(φ, sel)`, `label_replace(...)`, `clamp_min/max(v, n)`, `round(v, n?)`).

**Supported (20 ops, verified against `AggregationOp` dispatch):**

| Category | Operators |
|---|---|
| Range aggregation | `sum`, `avg`, `min`, `max`, `count`, `stddev`, `stdvar` — with `by`/`without` |
| Counter/range fns | `rate`, `irate`, `increase`, `delta` |
| Histogram | `histogram_quantile(φ, …)` |
| Instant transforms | `abs`, `ceil`, `floor`, `round`, `clamp_min`, `clamp_max` |
| Ranking | `topk(n, …)`, `bottomk(n, …)` |
| Label manipulation | `label_replace` |
| Matchers | `=`, `!=`, `=~` (regex crate) |

**Structural limits (all verified):**

1. **BUG — `[5m]` duration discarded.** `strip_range()` (matcher.rs:455) removes the range selector and returns only the inner selector; the parsed duration is never propagated. The executor instead derives a window as `(end−start)/60` (executor.rs:154). Consequence: `rate(x[1m])` and `rate(x[1h])` are **identical queries** in parqtel. In Prometheus the range selector *is* the window; discarding it changes alert and dashboard semantics silently. This is the single most important fix in this report.
2. **No nesting.** `sum(rate(http_requests_total[5m]))` — the canonical RED query pattern — cannot be parsed (second function has no dispatch path). Grouping exists only as a direct modifier of the single aggregation.
3. **No binary operators.** No `+ - * / % ^`, no comparisons, no `bool` modifier, no vector matching (`on(labels)`, `ignoring()`, `group_left/right`). Ratio queries (`errors / requests`) are impossible.
4. **No `offset`, `@` modifier, subqueries (`[1h:5m]`), `absent()`, `changes()`, `deriv()`, `predict_linear()`, `holt_winters()`, `sort()`/`sort_desc()`, `scalar()/vector()/time()`, `ln/exp/log2/log10/sqrt/sgn`, date functions (`hour()`, `day_of_week()`), or the 16 `_over_time` family functions** (`avg_over_time`, `count_over_time`, `last_over_time`, `quantile_over_time`, `present_over_time`, `absent_over_time`, …).
5. **Instant-query semantics:** fixed 1-minute lookback (documented); Prometheus uses a configurable 5-minute lookback with staleness handling.

### 2.2 Logs

**Current surface:** selector-style label matchers (`service="api"`, `severity_text="ERROR"`) + two HTTP params — `severity_min` (numeric threshold) and `search` (single **case-insensitive substring contains** on `body`, executor.rs:378).

**No term operators** (AND/OR/NOT), **no wildcards**, **no quoting/phrase**, **no field-scoped matching beyond label equality**, **no regex on body**, **no `exists`**, **no numeric ranges** (e.g. `duration:>100`), **no relevance ordering**, and **no full-text index** — every query is a scan-and-filter over decoded rows (fine at Parqtel's scale, but the *language* should not be the bottleneck).

### 2.3 Traces

**Current surface:** time-range + optional single `trace_id` (handler-level); results sorted newest-first, capped at 64 blocks / limit rows. The UI groups client-side by trace.

**No predicates**: cannot search "traces where `service=checkout AND status=ERROR AND duration>500ms`", cannot filter by span name, kind, or attributes — queries every market leader supports as table stakes.

### 2.4 Cross-signal

`/v1/correlate` performs dimension-priority joins (trace_id → k8s_pod_uid → service.name) over a window — **genuinely ahead of most OSS backends**; this is a Parqtel differentiator to build on, not a gap.

---

## 3. Market Leader Benchmarks (2026 research)

### 3.1 Prometheus / PromQL — the metrics grammar baseline

Prometheus 3.x PromQL remains the lingua franca and the skill-base Parqtel inherits. Current function surface (from docs): **~70+ functions** including the full `_over_time` family (16), trigonometric/hyperbolic (`sin/cos/atan/…`), `histogram_*` suite for native histograms (`histogram_avg/count/sum/fraction/stddev/stdvar/quantiles`), `double_exponential_smoothing` (renamed from `holt_winters`), experimental `info()` for label enrichment joins, `sort_by_label`, `start()/end()/step()`. Structural features parqtel lacks: **nested expressions via a real grammar, binary operators with set/binary vector matching, subqueries, offset, `@`, absent-detection, native histograms, and staleness marking**. PromQL's operator-count advantage is less important than its *composition* advantage — real dashboards are pipelines like `sum by (job) (rate(...[5m])) / clamp_min(sum by (job) (rate(...[5m])), 1)`.

### 3.2 ClickHouse / ClickStack — the search-experience benchmark

ClickStack's search bar (2026 docs) is the reference UX for log search: **natural-language syntax** (case-insensitive whole-word terms, `*Error*` wildcards for partials), **boolean combination** (`AND`/`OR`/`NOT`/`-` exclusion), **quoted exact phrases**, **`column:value` matching including JSON/map properties**, **numeric comparisons** (`Duration:>1000`), **existence** (`property:*`), natural-language time input, **SQL WHERE escape hatch** for complex cases, **saved searches with alerting**, and tagging. ClickHouse's engine advantage (skip indexes, bloom filters, materialized columns) is out of scope for a single-binary engine, but the *language* is exactly what parqtel logs need.

### 3.3 Elasticsearch — the full-text query benchmark

Lucene `query_string` syntax: fields (`status:active`), grouped field queries (`title:(quick OR brown)`), phrases, **wildcards `?`/`*`**, **embedded regex `/joh?n(ath[oa]n)/`**, **fuzzy `quikc~` (Damerau-Levenshtein)**, **proximity `"fox quick"~5`**, **inclusive/exclusive ranges `[1 TO 5]` + `>=/<=` shortcuts**, **boosting `quick^2`**, **`_exists_:field`**, multi-field search with per-field boosts, `minimum_should_match`, and the full JSON DSL behind it. Elastic's own guidance is notable: **strict syntax errors are bad UX for search boxes** — forgiving parsing (`simple_query_string`) is their recommendation. Lesson for parqtel: adopt the *operators* but keep the parser lenient.

### 3.4 Dynatrace Grail / DQL — the unified-query benchmark

Grail is the strategic blueprint for cross-signal query: **pipeline model** (`fetch logs | filter … | fields … | summarize … | sort … | limit`), schema-on-read, indexless scans, `makeTimeseries` for chartable aggregates, `fieldsAdd`/`parse` for on-read transformation, business-hours filters in one expression. DQL demonstrates that a unified pipeline over heterogeneous records (logs/traces/metrics/events/bizevents) is the end-state users expect. Grail's scale (exabyte, MPP, datawarping) is proprietary — but the **language shape is reproducible** and fits Parqtel's correlate primitive.

### 3.5 What users say (2026 feedback themes)

- ClickStack praise centers on **search-bar simplicity with SQL escape hatch**; complaints historically about ClickHouse ops burden (Parqtel's niche).
- Elastic users: KQL/Lucene **strictness and escaping** are chronic pain (`+ - = && || > < ! ( ) { } [ ] ^ " ~ * ? : \` must be escaped) — validates a forgiving grammar.
- DQL adoption is strong but **vendor-locked**; the community keeps asking for open equivalents (SigNoz/ClickHouse comparisons) — an open, pipeline-capable query layer over open Parquet is a differentiated answer.
- PromQL criticism (long-standing): no logs/traces story; **columnar/SQL people want `SELECT`** — the OTel community's battle between PromQL and SQL persists. Parqtel should answer "both": PromQL front, SQL-ish core.

---

## 4. Comparison Matrix

Legend: ✅ full · 🟡 partial/simplified · ❌ absent

| Capability | Parqtel today | Prometheus | ClickStack | Elasticsearch | Grail/DQL |
|---|---|---|---|---|---|
| **METRICS** | | | | | |
| Core aggregation (sum/avg/min/max/count) + by/without | ✅ | ✅ | ✅ (SQL) | ✅ (DSL) | ✅ |
| Counter functions (rate/increase/irate/delta) | ✅* | ✅ | ✅ (SQL windows) | ✅ (ES|QL) | ✅ |
| **Range-selector windows (`[5m]`)** | ❌ **BUG — parsed, discarded** | ✅ | n/a (SQL) | n/a | ✅ (`interval:`) |
| Nested function pipelines | ❌ | ✅ | ✅ | ✅ | ✅ |
| Binary ops + vector matching (on/group_left) | ❌ | ✅ | ✅ | ✅ | ✅ |
| Subqueries / offset / @ | ❌ | ✅ | 🟡 | 🟡 | 🟡 |
| histogram_quantile | ✅ | ✅ | ✅ | ✅ | ✅ |
| `_over_time` family | ❌ (0/16) | ✅ (16) | ✅ | 🟡 | ✅ |
| absent/changes/deriv/predict_linear | ❌ | ✅ | ✅ | ❌ | ✅ |
| Sort/scalar/date/time helpers | ❌ | ✅ (20+) | ✅ | 🟡 | ✅ |
| **LOGS** | | | | | |
| Label/field equality matchers | ✅ | n/a | ✅ | ✅ | ✅ |
| Severity threshold filter | ✅ (`severity_min`) | n/a | ✅ | ✅ | ✅ |
| Body substring search | 🟡 (single term, contains) | n/a | ✅ | ✅ | ✅ |
| Boolean term operators (AND/OR/NOT/-) | ❌ | n/a | ✅ | ✅ | ✅ |
| Wildcards / regex on text | ❌ | n/a | ✅ | ✅ | ✅ |
| Field:value + numeric range (`dur:>100`) | ❌ | n/a | ✅ | ✅ | ✅ |
| Exists (`field:*` / `_exists_`) | ❌ | n/a | ✅ | ✅ | ✅ |
| Phrase + proximity | ❌ | n/a | ❌ | ✅ | 🟡 |
| Saved searches (server-side) | ❌ (UI localStorage only) | n/a | ✅ | ✅ | ✅ |
| **TRACES** | | | | | |
| Time-range browse + trace_id lookup | ✅ | n/a | ✅ | ✅ | ✅ |
| Service/operation filters | ❌ (UI-only client-side) | n/a | ✅ | ✅ | ✅ |
| Duration/status/span-kind predicates | ❌ | n/a | ✅ | ✅ | ✅ |
| Attribute search on spans | ❌ | n/a | ✅ | ✅ | ✅ |
| **CROSS-SIGNAL** | | | | | |
| Metric↔log dimension join | ✅ (`/v1/correlate`) | ❌ | 🟡 (manual SQL) | 🟡 | ✅ |
| Unified pipeline over all signals | ❌ | ❌ | 🟡 | 🟡 | ✅ |
| **INFRA** | | | | | |
| Open storage (external tools) | ✅ Parquet | ❌ TSDB chunks | ✅ | ❌ | ❌ |
| Query metrics/logging (per-query stats) | ❌ | ✅ | ✅ | ✅ | ✅ |

\* rate/increase work, but the window is the whole query range / 60 — see §2.1.

---

## 5. Gap Analysis — Prioritized

| # | Gap | Why it matters | Class |
|---|---|---|---|
| G1 | **Range-selector windowing discarded** | Wrong numbers, silently. `rate(x[5m])` must mean 5 minutes. Blocks correct alerting on counters. | **Correctness — P0** |
| G2 | No nested/composed expressions | The most common real PromQL (`sum(rate(...))`, ratios) cannot run. Grafana parity impossible. | P0 |
| G3 | No binary ops + vector matching | SLO/error-ratio/multi-metric math impossible. | P1 |
| G4 | No real log query language | Logs are the #1 search surface for on-call; current capability is below 2015-era grep UIs of every leader. | P1 |
| G5 | No trace predicates (service/duration/status/attrs) | Basic "find slow failing traces" impossible server-side. | P1 |
| G6 | Missing `_over_time` + helpers (absent, changes, sort, time/date, math) | Long-tail of dashboard/alert patterns; cheap wins once the engine composes. | P1 |
| G7 | No subqueries/offset/@ | Power-user patterns; Prometheus parity checkbox. | P2 |
| G8 | No unified pipeline / cross-signal language | Strategic (Grail parity) — builds on existing `/v1/correlate`. | P2 |
| G9 | No server-side saved searches; no query stats | UX + ops parity; trivial once API exists. | P2 |
| G10 | Single-arg functions only for label_replace/round/clamp | Minor parser gaps within existing operators. | P2 |

---

## 6. Gap-Closure Plan

### Phase 0 — Correctness (do first, ~2-3 days)

| Item | Work |
|---|---|
| **F0.1 Fix `[range]` semantics** | Parse duration (`5m`, `1h30m`, `2d`, plain seconds) into the plan; executor windows `rate/increase/delta/irate` and future `_over_time` on **per-step windows ending at each evaluation timestamp** (Prometheus model), not query-range/60. Unit tests: `rate(x[1m])` ≠ `rate(x[1h])` over the same data. |
| **F0.2 Window evaluation model** | For range queries, evaluate windowed functions at each `step` boundary looking back `range`; for instant queries, look back `range` from `time`. This unblocks everything in Phase 1. |
| **F0.3 Query metrics** | Per-query `parqtel_query_duration_seconds`, series-scanned counters, and `status` in API responses (already partially present). Baseline before engine rewrite. |

### Phase 1 — Composable engine + log language (the market-parity phase, ~2-4 weeks)

**1A. Expression engine for metrics (replaces single-function dispatch):**
- Pratt/recursive-descent grammar producing an AST: `Expr := Unary | Binary | Call | Selector | Subquery | NumberLiteral`
- **Binary operators** with **vector matching**: `on()/ignoring()`, `group_left/group_right`, `bool` modifier, set ops for matcher-only selectors; arithmetic/comparison precedence per PromQL spec.
- **Nesting for free**: `sum by (job) (rate(http[5m]))`, `histogram_quantile(0.9, sum by (le) (rate(x_bucket[5m])))`.
- **New functions** (all window-aware post-F0.2): the 16 `_over_time` family, `absent/absent_over_time`, `changes`, `resets`, `deriv`, `predict_linear`, `double_exponential_smoothing`, `sort/sort_desc/sort_by_label`, `scalar/vector/time/timestamp`, `abs/exp/ln/log2/log10/sqrt/sgn`, date helpers (`hour/day_of_week/...`), `label_join`, `clamp()` 3-arg, `round()` 2-arg.
- **Subqueries** (`expr[1h:5m]`), `offset`, `@` modifier.
- Keep the 20 existing operators' semantics; the AST lowers to the current scan/aggregate paths (no storage rewrite needed).

**1B. `ParqtelQL` — unified log/trace search language (ClickStack-inspired, Lucene-class):**
- Grammar (lenient parser — never error on user typing; unmatched terms become body terms):
  - terms: `error`, `"exact phrase"`, `*partial*`, `-exclude`
  - boolean: `AND OR NOT` (case-insensitive), parentheses
  - field ops: `service=api`, `severity>=WARN` (maps to severity_min), `duration:>100`, `trace_id:"a1b2…"`, `attr.http.status_code=500`
  - existence: `field:*`
  - regex: `body:/error \d{3}/` (bounded by the regex crate)
- **Same grammar drives traces**: span predicates `service=checkout status=ERROR duration>500ms kind=server` with server-side filtering in `query_traces` (new params; UI builder/facets compose queries into it).
- Server-side **saved searches** API (per-signal, stored in data dir like silences) — supersedes localStorage-only UI views.
- Ship as both the `/api/v1/logs?query=` grammar **and** a `/v1/search` unified endpoint accepting `{signal, query, range}` — the seed of Phase 2's pipeline.

**1C. Builder/UI wiring:** metrics Builder emits nested AST queries; logs facets emit ParqtelQL; trace list gains service/duration/status filter chips.

### Phase 2 — Pipeline + analytics (differentiation, ~3-4 weeks)

- **PQL pipeline** over signals (Grail-shaped, pipe syntax): `fetch logs | filter service="api" AND body~"timeout" | stats count by service | correlate traces window=5m` — implemented as AST stages over the 1B grammar; correlates via the existing dimension-priority join.
- **makeTimeseries / log-to-metric** derived queries: `count() by interval` over log results (the missing log volume chart in ad-hoc queries).
- `parse` / `fieldsAdd`-style on-read extraction (regex capture groups to ephemeral fields) — the highest-value DQL feature for log analytics without ingest-time pipelines.
- Query budget/limits surfaced per-stage (`limit`, `timeout` stage params).

### Sequencing & gates

```
P0 (correctness)        P1A (AST + binary ops)     P1B (ParqtelQL)          P2 (pipeline)
F0.1 windowing  ──►     grammar replace     ──►    log grammar + saved ──►  fetch|filter|stats
F0.2 eval model         16 over_time fns          trace predicates          parse/correlate stages
F0.3 query metrics      subquery/offset/@         UI builders               makeTimeseries
```
- Every phase keeps the existing API shapes (Prom-compatible JSON) — Grafana and current UI must not break.
- Golden-file conformance suite: run a corpus of real-world PromQL queries against both parqtel and a reference (promtool/prometheus where feasible); diff. Same for log grammar with an expectation corpus.
- Performance gate: scanner stays untouched except pushing predicates into `Scanner::scan_*` where cheap (labels/duration/status push down; body regex stays post-scan).

### Explicit non-goals

- Full-text inverted indexes (scale story differs; scan+predicate is fine at OSS scale — revisit with Parquet bloom/prefix indexes later)
- Trigonometric PromQL functions, native histogram storage format (classic-histogram `le` stays)
- SQL front-end (ParqtelQL + PromQL suffice; DuckDB covers exit-ramp analytics over the open Parquet)

---

## 7. Success Metrics

| Dimension | Target after P1 |
|---|---|
| PromQL conformance corpus | ≥85% of a 200-query real-world corpus parses & evaluates identically to reference semantics |
| Windowing | `rate(x[5m])` correct at every step boundary; regression test locks it |
| Log search | Boolean+field+range+exists queries <100ms p95 over 1M buffered+scanned logs |
| Trace search | "slow failing traces of service X" answerable in one server-side query |
| Composition | `sum(rate(x[5m])) / sum(rate(y[5m]))` renders in Grafana unchanged |
| Zero regressions | Existing UI, Grafana SimpleJSON, MCP query tools all green per phase |

## 8. Sources

- Prometheus 3.x query functions documentation (full 70+ function surface, native histograms, experimental `info()`/`sort_by_label`)
- ClickHouse ClickStack Search docs (natural-language syntax, column:value, ranges, exists, saved searches; SQL WHERE mode)
- Elasticsearch `query_string` reference (fields/wildcards/regex/fuzzy/proximity/ranges/boosting/exists; strictness guidance recommending lenient parsers)
- Dynatrace Grail/DQL docs (pipeline model, fetch/filter/fields/summarize/makeTimeseries, schema-on-read, indexless MPP positioning) and platform pages (2026)
- Parqtel code: `parqtel-query/src/{matcher,plan,executor,aggregation}.rs`, handlers (verified operator lists, `strip_range` discard bug at matcher.rs:455, executor.rs:154 window derivation, logs substring filter at executor.rs:378)
