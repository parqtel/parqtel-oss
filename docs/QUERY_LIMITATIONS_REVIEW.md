# Query Language — Limitations Review & Prioritized Remediation Plan

**Status: PROPOSAL — awaiting review before implementation**
**Scope:** All carried-forward findings from Phases 0/1A/1B/2 (`QUERY_PHASE_LIMITATIONS.md`), re-scored post-buildout.

Scoring dimensions:
- **Trust** = silent wrong/missing results vs user expectation (PromQL/ClickStack semantics)
- **Latency** = user-facing query cost
- **Coverage** = capability gap blocking real workflows
- Effort: S (< 1 day) · M (1–3 days) · L (1–2 weeks)

---

## Resolved during the buildout (verified, no action)

| Was | Resolved by |
|---|---|
| AND-only search semantics (P1B-1) | Phase 2 boolean predicate trees |
| Trace predicates post-scan/200-cap loss (P1B-3) | Phase 2 push-down (200→10k scan cap) |
| 12 in-flight correctness bugs (P0-F1..F5, P1A-F1..3, P1B-F1..2, P2-F1..3) | Fixed in their phases |

---

## P0 — Correctness deviations that silently produce wrong results

The highest class: queries that *return data a user acts on* with semantics
that differ from what the PromQL skill base expects. Fix before any
capability work — each fix is verified against a conformance corpus (see
G0, which gates this entire wave).

### G0 — PromQL conformance corpus (the missing safety net) · Effort M
**Source:** original plan success metric ("≥85% of 200-query real-world
corpus"), never built. **Why first:** every P0 item below changes
evaluation semantics; without a reference-diff corpus we're fixing
deviations blind, and the "legacy engine retirement" decision (see
Non-goals) has no gate. **Plan:** 200-query corpus of real-world PromQL
shapes with expected outputs derived from Prometheus semantics;
run against both engines per PR; track pass-rate per shape-class.
**Also answers:** whether the two-engine dispatch (legacy + AST) can be
retired — the buildout's largest architectural debt.

### G1 — Instant-selector lookback: 60s vs Prometheus 5m · Effort S
**Sources:** P0-4, P1A-8 (flagged twice, deferred twice). **Impact:** any
series with a scrape gap of >60s returns "no data" on instant queries
where Grafana+Prometheus would show the last sample — on-call sees
"empty" during exactly the irregular-scrape moments that matter.
**Plan:** configurable `query.lookback_delta` defaulting to 5m
(Prometheus semantics); keep per-call `@` as the eventual override.

### G2 — `avg(x[5m])` style lenient fallback on the legacy path · Effort S
**Source:** P0-7 ("will align in the AST phase" — the AST path errors
correctly, but `needs_ast` routes single-fn+range shapes to legacy where
the lookback-aggregation fallback silently applies). **Impact:** PromQL
rejects this shape; we return a *plausible-looking but different*
aggregation. **Plan:** legacy `downsample_plan` returns a validation
error suggesting the `_over_time` form (AST path already does).

### G3 — Vector-matching result labels for `on()` · Effort M
**Source:** P1A-5. **Impact:** `a * on(x) b` should project result
labels to only `x` (plus `group_left` extras); we keep the full LHS
label set. Downstream `by()`/dashboards group on labels that PromQL
would have dropped — silent cardinality and grouping surprises.
**Plan:** per-mode label projection (All/On/Ignoring) in
`eval_vector_binary` + corpus cases per mode.

### G4 — Multi-reset counter windows under-count · Effort M
**Source:** P0-2. **Impact:** `rate`/`increase` over a window containing
≥2 counter resets under-count (single-reset approximation
`dv = last_value`). Alert thresholds on churning counters read low.
**Plan:** per-segment summing (Prometheus algorithm): accumulate deltas
across reset boundaries instead of collapsing to one segment.

## P1 — Latency hot spots (user-visible, flagged in baselines)

### G5 — `label_values` p95 1.3–1.5s · Effort M
**Source:** flagged in phase0/1a/1b baselines — *three consecutive
phases, never scheduled*. The single worst query in every benchmark.
**Impact:** service/name dropdowns, Grafana variable queries, facets —
high-traffic paths. **Plan:** label-value index maintained from block
metadata on flush/compaction (dictionary per block, merged on read),
plus a TTL cache; target p95 < 100ms on the benchmark dataset.

### G6 — Pipeline filter/limit push-down (2× row cost) · Effort M
**Source:** P2-2 (341–423ms p50, row materialization dominated).
**Plan:** mirror the trace push-down — apply `filter` predicates during
scan, `limit` before full materialization; keeps stage API unchanged.

### G7 — Multi-block benchmark dataset (enabler) · Effort S
**Source:** P0 benchmark-infra gap. **Why here:** gates G8 verification
and exercises scan fan-out + the 64-block cap that production hits but
the suite never has. **Plan:** seed across ≥3 flush windows
(generator profile option), re-baseline, keep in the per-phase protocol.

### G8 — Trace 64-block scan cap · Effort S (after G7)
**Source:** P0-8. With G7's multi-block baseline, tune the cap or add
tier-aware selection (recent blocks + compacted tails).

## P2 — Coverage gaps blocking real workflows

### G9 — Pipeline `fetch metrics` / `fetch traces` · Effort M each
**Source:** P2-1. The pipeline's headline query — "error rate by service
joined with traces" — needs metrics/traces as fetch targets.
**Plan:** series→row mapping (labels + value + ts) and span→row mapping
(name/service/duration/status/attrs), both mechanical against the
existing scan APIs.

### G10 — Missing PromQL functions · Effort S–M total
**Source:** P1A-4, P1A-3. `count_values`, `timestamp`,
`absent_over_time`, `predict_linear`, `double_exponential_smoothing`,
date fns (`hour`, `day_of_week`, …), `@` modifier. All have clear
Prometheus algorithms; batch-implement against the corpus.

### G11 — `correlate` service+window fallback · Effort M
**Source:** P2-3. trace_id-only join today; `window=` parses but is
unused. **Plan:** when a row lacks trace_id, join on
service.name ± window to the nearest matching signal rows — the same
dimension-priority logic `/v1/correlate` already implements.

### G12 — Negation on non-Eq clauses (NOT on ranges/comparisons) · Effort S
**Source:** P1B-5. `NOT duration>500` currently downgrades to positive
matching. Predicates already support `Not` nodes — wire range/cmp atoms
through inverted evaluation.

## P3 — Polish (schedule after waves land)

| # | Item | Source | Effort |
|---|---|---|---|
| G13 | Bare-word dispatch heuristic — document + optional explicit `body:` prefix | P1B-2 | S |
| G14 | `field:value` containing `:` requires quoting — relax tokenizer for URL-ish values | P1B-4 | S |
| G15 | `interval=` buckets timestamp-snapped (currently ts/interval) | P2-4 | S |
| G16 | Evaluator per-step re-walk / subquery caching — defer until subquery usage exists | P1A-1/2 | L, on-demand |

## Explicit non-goals — confirm these stay OUT

| Item | Rationale |
|---|---|
| Native histogram storage format | Classic `le` buckets suffice; per analysis non-goals |
| Counter-vs-gauge type enforcement | Same contract as PromQL |
| Saved-search multi-tenancy | Commercial scope |
| Pipeline UI surfacing | Commercial scope |
| Legacy-engine removal | **Gated on G0 corpus results** — not a non-goal, but not schedulable before G0 |

---

## Proposed sequencing

```
Wave 1 (correctness, corpus-gated):  G0 → G1, G2, G3, G4, G12   (~2 wks)
Wave 2 (latency):                    G5, G7 → G8, G6            (~1.5 wks)
Wave 3 (coverage):                   G9, G10, G11               (~2 wks)
Wave 4 (polish, on demand):          G13–G16; legacy-retirement
                                     decision from G0 data
```

Each wave ends with the benchmark protocol re-run; Wave 1 additionally
reports conformance-corpus pass-rate. No wave starts until the previous
baseline is green and committed.

## Open questions for review

1. **G1 default:** align to Prometheus 5m (conformant, changes what
   "no data" means for gap-prone series) or keep 60s configurable? I
   recommend 5m + config override.
2. **G0 corpus reference:** derive expectations from Prometheus docs
   semantics by hand (no external dep) vs. running actual Prometheus as
   a CI oracle (heavier, exact). I recommend hand-derived first,
   oracle later if drift appears.
3. **G9 order:** metrics fetch target first (error-rate pipelines) or
   traces first (waterfall-in-pipeline)? I recommend metrics.
4. **Scope check:** Wave 1 is pure semantics — confirm we want the
   behavioral changes (5m lookback especially) in a minor release, or
   gated behind config defaults.
