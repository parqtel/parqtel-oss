# Legacy Query Engine — Retirement Decision (G0 Gate)

**Status: DECISION PROPOSAL**
**Data: corpus 98/98 (Wave 3), routing audit (Wave 4): 57 AST / 41 legacy of 98**

## The question

The buildout runs two engines side by side: the legacy single-function
plan path (Phase 0 semantics: `QueryPlan` + `downsample_plan`) and the
AST evaluator (Phases 1A+). `needs_ast` routes per query. Retiring the
legacy engine would delete ~700 LOC of dispatch + planning and remove
the dual-maintenance burden; keeping it preserves the zero-regression
guarantee for the original simple-shape dashboards.

## Data from the G0 corpus

| Signal | Finding |
|---|---|
| Corpus conformance | 98/98 on the AST evaluator (default config) |
| Routing split | 57% of corpus cases route AST; 41 legacy-routed cases are all single-function/simple-aggregation shapes |
| Legacy parse health | Every legacy-routed corpus case parses via `parse_query` (no fall-through gaps) |
| Semantics divergence fixed | Wave 1 aligned the load-bearing divergences (5m lookback on both paths, multi-reset rate in BOTH engines, G2 rejection on both) — the two engines now agree on every corpus-verifiable behavior |

## Engine-parity gaps that remain

1. **Range-selector windows**: legacy `downsample_windowed` implements
   per-step lookback (aligned in Phase 0 + Wave 1 G4); the AST
   `rate_over_samples` implements the same segment math. Verified equal
   by the corpus rate/increase cases passing through both paths.
2. **Aggregation grouping**: legacy `by/without` vs AST `Grouping` —
   both corpus-green.
3. **Legacy-only functions**: none — every legacy function is either
   in the AST dispatch or the corpus proves the AST path handles the
   shape (`is_legacy_function` mirrors the AST's coverage).

## Recommendation

**Retire the legacy engine — but not in one step.**

- **Now (Wave 4)**: flip the default — route ALL queries through the
  AST engine; keep the legacy path behind a config flag
  (`query.legacy_engine = false` default) as the escape hatch for one
  release cycle.
- **After one release + corpus + benchmark cycle**: delete the legacy
  plan path, `needs_ast`, and the `is_legacy_function` table; the AST
  parser's errors replace the legacy messages (the parser now produces
  equal-or-better messages with G2-style suggestions).

**Rationale**: the corpus proves behavioral parity on every checkable
case; the dual-engine cost (two windowing implementations of the SAME
math, a routing predicate that has already caused two silent-wrong-path
bugs — P1A-F1, P1B-F1 — and growing maintenance surface) exceeds the
zero-regression value now that parity is verified. The one-release flag
window preserves the rollback story.

## Risks & mitigations

| Risk | Mitigation |
|---|---|
| Unseen legacy-only consumers (alert rules, pipelines written against legacy shapes) | Alert eval + pipeline ruler go through `parse_query`; a Wave-4 pass converts their queries through the AST with the flag ON in CI before the default flips |
| Legacy error-message compatibility (API consumers keying on message text) | Release-note the message changes; messages now include suggestions (better UX, breaking only for brittle parsers) |
| Perf regression on simple queries (AST tree walk vs single dispatch) | Benchmark gate: wave4.json must show simple-shape queries within noise; the AST path already measures equal in wave1-3 baselines |

## Execution checklist (for the flip PR)

- [ ] `needs_ast` returns true unconditionally behind
      `query.legacy_engine = false`; flag overrides
- [ ] Alert-eval query path runs the AST evaluator (executor.execute_ast)
- [ ] Pipeline ruler (recording rules) same
- [ ] Corpus: add the flag-off corpus run (legacy) as a CI job for one
      release, then remove
- [ ] Bench: full suite on the flipped default; compare wave4.json vs
      wave3.json (within-noise gate)
