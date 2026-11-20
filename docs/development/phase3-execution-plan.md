# Phase 3 execution plan: bounded query cost

Branch: `perf/phase3-query-streaming`, based on `origin/main` `f34f48a`.
Parent plan: [efficiency-plan.md](efficiency-plan.md), Phase 3.

Executor: subagents on `github-copilot/gpt-6-luna` (`#medium` default, `#high` for
Q4). One task per run, in order. The orchestrator reviews each diff, runs the gate
and commits as `sonu202604 <sonu202604@gmail.com>` before the next task.

## Why this order

Phase 0 passed its six-hour UAT soak on the Phase 2a image (`14c29d2a…`): zero
restarts, peak 2,736 MiB, 1,444 panel queries with zero failures. The same soak shows
query latency growing with data, not memory:

| 15-minute panel (`erlang_vm_allocators`, 1,000 of ~40K series, 30 s step) | p50 | p95 |
|---|---:|---:|
| Hour 1 | 3.9 s | 7.5 s |
| Hour 2 | 4.2 s | 6.7 s |
| Hours 4–6 | 11.2–11.8 s | 13.4–14.3 s |

UAT scan counters at the end of the soak:

| Counter | Value | Meaning |
|---|---:|---|
| Block opens | 24,600 | |
| Footer cache misses | 8,169 (33%) | a third of block opens re-read and re-parse the footer |
| Footer load time | 7,564 s | about **0.93 s per miss** |
| Footer cache bytes | 148 MB | hard-coded budget is 192 MiB, about two large L1 tables |
| Rows considered / returned | 534.8M / 534.8M | every scanned row becomes a `DataPoint` |
| Scan admission wait | 3,537 s | queries queue on the 4-slot budget |

Two causes, in order of cost:

1. **Footer cache churn.** `FOOTER_CACHE_BUDGET` is fixed at 192 MiB
   (`crates/parqtel-core/src/storage/scanner/block.rs`). After Phase 2a a 283K-series
   L1 table is about 84 MB in memory, so the cache holds about two of them. A 15-minute
   query spans three or more L1 blocks plus L0 tails, so concurrent panels evict each
   other and pay about 0.93 s per re-open.
2. **Full materialization before limits.** `Scanner::scan_filtered` turns every row
   of every matched series into a `DataPoint`; `build_series_data`
   (`crates/parqtel-query/src/executor/series.rs`) then groups, sorts by fingerprint and
   keeps `max_series`. For this panel that is ~40K series read to return 1,000.

Phase 3 fixes (1) cheaply first, then (2). Range-function pushdown and log/trace work
follow only if measurement says they still matter.

## Rules for every task

1. Results must not change: series set, samples, `total_series_count` and
   `volume_summary` stay identical to today for the same inputs.
2. Series limits keep their current definition: matched series sorted by label-set
   fingerprint, the first `max_series` kept, each keeping its oldest
   `max_samples_per_series` samples.
3. The full matcher (`SeriesSelector.filter`) still decides every series.
4. No stored-format change. Blocks without a series table keep the current path.
5. Workspace lints apply (no `unsafe`, no `unwrap`/`expect`/`panic` outside tests).
   CI runs a newer clippy than local; the gate command is
   `cargo clippy --workspace --all-targets --locked -- -D warnings`.

## Tasks

### Q0 — Record Phase 0 completion (docs only)

Files: `docs/development/efficiency-plan.md` (Phase 0 section only).

Add the six-hour soak result as the Phase 0 exit record: image digest, window, peak
memory, restarts, panel success count, ingest rate and gap, alert skip count. Mark the
gate passed under the operator's six-hour revision.

Gate: `make docs-check`.

### Q1 — Query harness and baseline (new example)

Files: new example `bench_query_scan.rs` under `crates/parqtel-query/examples/`
(register in `crates/parqtel-query/Cargo.toml` only if needed).

Build on-disk metric blocks through `metric_runs_to_block` and `write_block_file`:
four blocks, each with one target metric of 40,000 series × 10 rows and 3,000 filler
metrics up to ~250K series per block (UAT shape). Index them in a `BlockIndex`.

Modes, each run in a fresh process: `fixture <dir>` writes the blocks once;
`measure <query> <dir>` opens the index and runs one `execute_ast` range query
(15-minute window, 30 s step, `max_series = 1000`) and prints:
`query=<q> ms=<n> hwm_mib=<n> rows_considered=<n> points=<n> footer_misses=<n> series=<n> total_series=<n>`.
Queries: `target` (the 40K-series metric), `selective` (`target{pod="pod-7"}`),
`two-panels` (two concurrent `target` queries over different windows, to show cache
eviction with the default budget).

Gate: fixture plus all three measurements run; record three-run medians in the task
report as the baseline.

### Q2 — Configurable footer cache budget

Files: `crates/parqtel-core/src/config/query.rs`,
`crates/parqtel-core/src/storage/scanner/block.rs`, `crates/parqtel-core/src/storage/scanner/mod.rs`,
`crates/parqtel-server/src/server.rs` (install at startup), `crates/parqtel-server/src/handlers/misc.rs`
(show in `/api/v1/stats`), `docs/operations/configuration.md`, tests.

Add `query.footer_cache_bytes` (default 192 MiB, unchanged). Install it once at startup
into the footer cache (process-wide, set before the first scan). Keep the existing LRU
and the rule that an entry larger than the whole budget is not cached. Export
`parqtel_scan_footer_cache_budget_bytes`.

Tests: budget honored on insert and eviction; oversized entry not cached; default
unchanged.

Gate: `cargo test -p parqtel-core scanner`, `cargo test -p parqtel-server`, CI clippy
command. Run Q1 `two-panels` with 192 MiB and 512 MiB; report miss counts.

### Q3 — Compact in-memory series table

Files: `crates/parqtel-core/src/storage/series_index.rs`, its tests.

Keep the serde shape of `SeriesTable` (footer format unchanged). After parse, store
label ids as one flat `Vec<u32>` plus per-series `u32` offsets instead of
`Vec<Vec<u32>>`, and strings in one arena (`String` buffer plus offsets) instead of
`Vec<String>`. Update accessors (`label_ids`, `string`, `series_entry`,
`approx_heap_bytes`) to read the compact form; callers do not change.

Tests: round-trip footer text unchanged; accessor results unchanged on `sample_table()`;
`approx_heap_bytes` lower than before for the T0 Phase 2 fixture.

Gate: `cargo test -p parqtel-core storage::series_index`,
`cargo test -p parqtel-core indexed_scan_equals_unindexed_scan`, CI clippy. Run the
Phase 2 `bench_series_index` `open` step; report HWM and `approx_bytes` against
Phase 2a (106.7 MiB / 83,895,996 bytes).

### Q4 — Series-limited indexed scan (use `#high`)

Files: `crates/parqtel-core/src/storage/scanner/metrics.rs`,
`crates/parqtel-core/src/storage/scanner/mod.rs`, `crates/parqtel-query/src/executor/metrics.rs`,
`crates/parqtel-query/src/executor/series.rs`, tests in both crates.

Two passes over indexed blocks, so rows of series that will be dropped are never
turned into points:

1. **Plan.** For each block, `select()` gives candidate series ids; get each one's
   fingerprint from `labels(id)` (already cached per label set). Merge with buffered
   series fingerprints. Matched series = union of fingerprints → exact
   `total_series_count`. Kept = the `max_series` smallest fingerprints.
2. **Read.** For kept series, read timestamp and value columns as today. For matched
   but not kept series, read the **timestamp column only** and count it into the
   60 volume buckets, so `volume_summary` stays exact. Never build `DataPoint`s for
   them.

Blocks without a series table, and any error in the plan pass, fall back to today's
path for that block. Return the kept points plus precomputed `total_series` and
`volume` so `build_series_data` does not recount them; it still applies
`max_samples_per_series` per kept series after sorting by time.

Wire both `execute_ast` and the plan path `execute` to it.

Tests (all must compare against the current implementation on the same data):
- more matched than `max_series`, across 3 blocks and the buffer, with the same series
  in several blocks: identical series, samples, `total_series_count`, `volume_summary`;
- fewer matched than `max_series`;
- histogram-valued series;
- a block without a series table mixed with indexed blocks;
- `max_samples_per_series` truncation across blocks.

Gate: `cargo test --workspace -- --test-threads=1`,
`cargo test -p parqtel-core indexed_scan_equals_unindexed_scan`, CI clippy. Run Q1
`target` and `selective`; report `points` and `rows_considered`.

### Q5 — Bounded log result merge

Files: `crates/parqtel-core/src/storage/scanner/logs.rs`, its tests.

Replace the per-block `extend` + full `sort_by_key` + trim with a bounded heap of size
`keep` (newest or oldest per `keep_newest`), then one final sort of at most `keep`
records. Counts and volume buckets unchanged.

Tests: same records, order and counts as the current implementation for newest/oldest,
ties on timestamp, `keep = 0`, and more blocks than `keep`.

Gate: `cargo test -p parqtel-core scanner`, CI clippy.

### Q6 — Measure and decide (docs only)

Files: `docs/development/efficiency-plan.md` (Phase 3 section only).

Run Q1 on the Q5 build; record a before/after table (three-run medians). Exit check:

- `target`: `points` ≤ 1.1 × (`max_series` × rows per series in window) — memory scales
  with returned series;
- `target`: latency ≤ 50% of the Q1 baseline; HWM growth ≤ 50% of baseline;
- `two-panels` with the configured budget: footer misses ≤ 10% of block opens;
- all equivalence tests pass.

If `target` still spends most time in step evaluation, propose range-function
pushdown (`rate`, `increase`, `*_over_time`, `sum by`) as Phase 3b for approval. If
footer misses stay high at a reasonable budget, propose Phase 2b (lazy binary footer)
for approval.

Gate: `make docs-check`.

### Q7 — UAT validation (manual approval)

Not delegated. Build and push an image; roll out with `query.footer_cache_bytes =
512 MiB` (or the Q6 recommendation). Re-run the six-hour four-panel soak with the same
script. Pass when, in hours 4–6:

- 15-minute panel p50 ≤ 4 s and p95 ≤ 7 s (baseline 11.2–11.8 s / 13.4–14.3 s);
- footer miss rate ≤ 10% (baseline 33%);
- peak memory < 4 GiB, zero restarts, zero ingest errors.

## Deferred

- Range-function and aggregation pushdown into per-series accumulators (Phase 3b).
- Trace column projection beyond `12b4497` (Phase 4).
- Lazy binary footer (Phase 2b), only if Q2/Q3 cannot bring misses down.

## Goal text for the coding agent

> Complete Phase 3 tasks Q0–Q6 of `docs/development/phase3-execution-plan.md` on branch
> `perf/phase3-query-streaming`: one gpt-6-luna subagent per task, in order, each
> reviewed, gated and committed. Query results must stay identical to the current
> implementation. Q6 records before/after measurements and the exit decision. Do not
> roll out to UAT (Q7) or start Phase 3b/2b without approval.
