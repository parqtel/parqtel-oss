# Phase 2 execution plan: lazy series index

Branch: `perf/phase2-lazy-series-index` (worktree `/tmp/opencode/wt-phase2`), based on
`origin/main` `63a9cf1`. Parent plan: [efficiency-plan.md](efficiency-plan.md), Phase 2.

Executor: subagents on `github-copilot/gpt-6-luna` (`#medium` by default, `#high` for
T3). One task per subagent run, strictly in order. The orchestrator reviews each diff,
runs the gate, and commits as `sonu202604 <sonu202604@gmail.com>` before the next task
starts.

## Why split Phase 2

The parent plan bundles three changes. Only one of them changes the stored format:

| Part | Change | Door | When |
|---|---|---|---|
| **2a** | Lazy in-memory index: no eager label interning, postings built per label on first use, streaming intersections | two-way (memory only, same footer) | now |
| **2b** | Binary, sectioned footer read lazily from the file | one-way (new footer format) | only if 2a misses its exit target |

2a removes the dominant cost first. Today `SeriesIndex::from_table`
(`crates/parqtel-core/src/storage/series_index.rs`) does, for **every** series of a block:

- interns a full `LabelSet` (strings plus cached JSON) through `LabelInterner::global()`;
- clones every `(label name, value)` pair into a `HashMap<String, HashMap<String, Vec<u32>>>`;
- copies row bounds into a `SeriesEntry`.

A UAT block holds ~283K series with ~14 labels each. A selective query touches a few
hundred of them and one or two label names.

## Target design (2a)

`SeriesIndex` keeps the parsed `SeriesTable`, a small metric/name directory and
per-series lazy label cells. It does **not** cache a full postings map, even lazily
per label name: `pod` is high-cardinality, so the first `pod="one-value"` query
would otherwise build hundreds of thousands of map entries. Instead, selectors
build query-local candidate-id lists by scanning only the chosen metric's compact
label-id rows. `intersect_all` combines those lists; they die with the query.

```text
SeriesIndex
  table: SeriesTable                       # ids + strings, as parsed (already compact)
  metric_ranges: HashMap<String, Vec<(u32, u32)>>   # ~3.4K metrics: kept eager
  label_name_ids: HashMap<String, u32>     # label names only (~560): eager, small
  labels: Vec<OnceLock<LabelSet>>          # interned on first use, per series
  total_rows, approx_bytes
```

Rules every task must keep:

1. **Candidate ids only narrow.** Each equality or value-test scan produces an
   ascending query-local id list. `selector.filter` stays the final judge of every
   series; a candidate-list bug must not hide a possible match.
2. `select()` keeps its signature and returns ascending ids in row order.
3. Any metric block writer still goes through `metric_runs_to_block`; the footer
   format, `FORMAT_VERSION` and `SERIES_INDEX_FOOTER_KEY` do not change in 2a.
4. `approx_bytes()` accounts for the long-lived table, metric/name directory and
   `OnceLock<LabelSet>` slots. Candidate-id vectors are query-local and are not
   retained in the footer cache. Use saturating arithmetic.
5. Label sets stay shared across blocks: the same series in two blocks yields
   `ptr_eq` label sets once both are materialised.
6. Workspace lints apply: no `unsafe`, no `unwrap`/`expect`/`panic` outside tests,
   `thiserror` errors. Keep existing comments unless the task changes their subject.

## Tasks

Each task lists the only files it may change, the steps, and the gate it must pass.
A subagent stops and reports if the gate fails twice or a needed change falls outside
its files.

### T0 — Baseline benchmark (new file)

Files: `crates/parqtel-core/examples/bench_series_index.rs` (new),
`crates/parqtel-core/Cargo.toml` (only if examples need registering — check how
`bench_bloom.rs` is wired first).

Build a synthetic `SeriesTable` with `SeriesTableBuilder`: 283,000 series across 3,400
metric names, 14 labels each (`pod` unique per series, `namespace` 50 values,
`service.name` 200 values, the rest low-cardinality), 4 rows per series. Encode it
with `to_footer_value()`.

The example has two modes. `fixture <path>` generates and writes the table once;
`measure <step> <path>` reads that same footer in a fresh process, so table generation
does not contaminate the memory measurement. Measure modes report `VmHWM` / `VmRSS`
from `/proc/self/status` (Linux only; no allocator hooks, `unsafe` is forbidden) plus
wall time for the step:

- `parse` — `SeriesTable::from_footer_value`
- `open` — parse + `SeriesIndex::from_table`
- `select` — open + `select("metric_0", equals pod=<one value>)` + labels of the result
- `select-broad` — open + `select` with only a `filter` closure (every series of one metric)

Run all four measure steps as separate invocations against the fixture, and print one
line each: `step=<s> hwm_mib=<n> rss_mib=<n> ms=<n> approx_bytes=<n> selected=<n>`.

Gate: fixture generation and all four measure invocations run;
`cargo clippy -p parqtel-core --examples -- -D warnings` clean. Record all four lines in
the task report — they are the baseline.

### T1 — Table accessors (series_index.rs only)

Files: `crates/parqtel-core/src/storage/series_index.rs`,
`crates/parqtel-core/src/storage/series_index/tests.rs`.

Add to `impl SeriesTable`, no behaviour change elsewhere:

- `fn string(&self, id: u32) -> Option<&str>`
- `fn label_ids(&self, series: usize) -> Option<&[u32]>` — the flat `[name, value, ...]`
- `fn label_pair_count(&self) -> usize` — total pairs over all series
- `fn approx_heap_bytes(&self) -> usize` — strings + all `Vec` payloads + per-`Vec` headers

Tests: each accessor on the existing `sample()` table, including out-of-range ids.

Gate: `cargo test -p parqtel-core storage::series_index` and
`cargo clippy -p parqtel-core -- -D warnings`.

### T2 — Streaming intersection helper (series_index.rs only)

Files: `series_index.rs`, `series_index/tests.rs`.

Replace pairwise `intersect` materialisation with
`fn intersect_all(lists: &mut [&[u32]]) -> Vec<u32>`: sort by length, walk the shortest,
and for each id `partition_point`-seek forward in every other list (galloping or
binary seek; keep a cursor per list). Keep `restrict`. Update `select()` to collect
borrowed slices where it can.

Tests: empty input, one list, disjoint lists, identical lists, and a property-style
test comparing `intersect_all` with a `BTreeSet` intersection over 200 seeded random
cases (use a fixed-seed LCG; no new dependency).

Gate: as T1.

### T3 — Lazy `SeriesIndex` plus caller migration — use `#high`

This must be one buildable commit. `SeriesIndex.series` is a public field used by
the scanner, compactor and tests, so removing it without migrating every caller
breaks `parqtel-core` itself. Do not leave a broken intermediate commit.

Files: `crates/parqtel-core/src/storage/series_index.rs`,
`crates/parqtel-core/src/storage/series_index/tests.rs`,
`crates/parqtel-core/examples/bench_series_index.rs`,
`crates/parqtel-core/src/storage/scanner/block.rs`,
`crates/parqtel-core/src/storage/scanner/metrics.rs`,
`crates/parqtel-core/src/storage/compactor.rs`,
`crates/parqtel-core/src/storage/scanner/tests/indexed.rs`,
`crates/parqtel-core/src/storage/stream_compactor.rs` (tests only),
`crates/parqtel-core/src/storage/tests/compaction.rs`, and
`crates/parqtel-ingest/src/writer.rs` (tests only).

Implement the target design above. Public API after the task:

- keep: `from_table(&SeriesTable) -> Result<Self>` (may take the table by value via a
  new `from_owned_table(SeriesTable)`; keep `from_table` cloning for existing callers),
  `approx_bytes`, `total_rows`, `has_metric`, `select`, `row_ranges`;
- add: `series_count() -> usize`, `bounds(id: u32) -> Option<(u64, u32, i64, i64)>`,
  `labels(id: u32) -> Result<LabelSet>` (interns via `LabelInterner::global()` on first
  call, then returns the cached clone);
- remove: the `pub series: Vec<SeriesEntry>` field.

`scanner/block.rs` must pass the parsed table to `from_owned_table` so opening a
block does not clone the full table. Keep `from_table(&SeriesTable)` for callers
and tests that need a borrowed input. Migrate every direct `index.series` access
in the files above to `series_count`, `bounds`, or `labels`; preserve row-count
and contiguity checks. For this buildable transition, the legacy tiered compactor
may iterate via `bounds` and `labels`; T4 removes that unnecessary query index.
The existing streaming compactor already reads `SeriesTable::series_entry` and
should not be refactored in this task. Update T0's `open`, `select`, and
`select-broad` modes to consume the parsed table via `from_owned_table`; `parse`
continues to retain the parsed table.

`select()`:

1. resolve metric ranges as today;
2. for each `equals` / `value_tests` matcher: look up the label-name id; if absent,
   return empty. Resolve equality values with a linear lookup in the compact symbol
   table; do not build a global value-to-id map. Scan the selected metric ranges' flat
   label-id pairs, compare the value id or apply the value test to its string, and
   collect matching series ids in row order. Keep these lists local to this query;
3. intersect with `intersect_all`;
4. time-window check from the table arrays, then `filter` on `labels(id)` only for
   survivors.

A label that fails to intern is a corrupt-index error (`Error::Internal`), not a
panic. The query-local candidate vectors are dropped when `select()` returns.

Tests (add, keep all existing ones passing after mechanical updates from `idx.series[i]`
to `idx.labels(i)` / `idx.bounds(i)`):

- `select` with multiple equality/value-test matchers returns the same ids as the full
  matcher across all labels; unknown labels and empty values return no ids;
- `labels_are_materialised_lazily` — a `#[cfg(test)]` counter of initialized
  `OnceLock`s is 0 after construction and after a selector-only `select`; a subsequent
  full-filter select materializes only its narrowed candidates;
- `the_same_series_in_two_blocks_shares_one_label_set` — rewritten via `labels(0)`;
- `approx_bytes_is_stable` — `approx_bytes()` covers the table, name directory and
  lazy label slots, and stays unchanged after candidate scans and label materialization.

Gate: `cargo test -p parqtel-core storage::series_index -- --test-threads=1`,
`cargo test -p parqtel-core indexed_scan_equals_unindexed_scan`,
`cargo check -p parqtel-ingest --tests`, and
`cargo clippy -p parqtel-core --all-targets -- -D warnings`. Every call site and the
T0 example must compile at the end of T3.

### T4 — Keep compaction out of the query index

Files: `crates/parqtel-core/src/storage/compactor.rs` and its focused tests.

The legacy tiered compactor currently builds `SeriesIndex` and postings only to
walk every source row. Parse its `SeriesTable` and use `series_count` /
`series_entry` directly, as `stream_compactor.rs` already does. Preserve every
row-range, contiguity and total-row check. This prevents compaction from eagerly
interning all source labels or building query postings.

Gate: `cargo test -p parqtel-core compaction -- --test-threads=1`,
`cargo test -p parqtel-core indexed_scan_equals_unindexed_scan`, and
`cargo clippy -p parqtel-core -- -D warnings`.

### T5 — Measure and decide (no product code)

Files: `docs/development/efficiency-plan.md` (Phase 2 section only).

Run the four T0 steps in three fresh processes each, using the same fixture and
release build for baseline and T4. Record medians for HWM, RSS and time in a table
under Phase 2. Define `open_growth = open HWM - parse HWM` and
`select_growth = select HWM - open HWM`. Then apply the exit check:

- **2a passes** if T4 `open_growth` is ≤ 25% of baseline `open_growth`, `select_growth`
  is < 10 MiB, and `select-broad` median time is no more than 20% slower than baseline.
- Otherwise, stop: 2b needs approval before any work.

Gate: `make docs-check`.

### T6 — Orchestrator only (not delegated)

- Review the full branch diff (`ponytail-review`), then one PR.
- UAT rollout only after the Phase 0 24-hour gate and the Phase 1 UAT checks pass
  (see efficiency-plan.md). Rollout is two-way: the footer format is unchanged, so the
  previous image reads every block this branch writes.

## 2b outline (gated, do not start)

New footer key `parqtel.series_index.v2`: a section table (offsets/lengths) followed
by a symbol table, a metric directory, per-label postings sections and fixed-width
series records. Read with ranged file reads, not from the Parquet key-value footer
(which `ParquetMetaDataReader` loads whole). Writers emit v2 only; readers keep v1.
Older binaries cannot use v2 indexes and fall back to unindexed scans — measure that
cost before approving.

## Subagent prompt template

```text
You are implementing task <Tn> of docs/development/phase2-execution-plan.md in the
worktree /tmp/opencode/wt-phase2 (Rust workspace, branch perf/phase2-lazy-series-index).
Read the whole plan file and AGENTS.md first.

Only change these files: <list from the task>.
Do the steps in the task exactly. Do not refactor, rename or reformat anything else.
Do not commit, push, or touch git state.

When done, run the task's gate commands and report:
1. files changed with a one-line summary each;
2. the exact gate command output tail (test result lines, clippy result);
3. anything you could not do and why.
If a gate fails twice, stop and report the failure instead of widening scope.
```

## Suggested goal text

> Complete Phase 2a of docs/development/phase2-execution-plan.md on branch
> perf/phase2-lazy-series-index: tasks T0–T5 executed by gpt-6-luna subagents in order,
> each reviewed, gated and committed; workspace tests and lint pass; T5 records
> before/after measurements and the 2a exit decision. Do not start 2b or roll out to
> UAT without approval.
