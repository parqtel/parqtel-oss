# Query evaluation

How a query is turned into series, steps and results, and what each stage costs.

## High-cardinality label-value autocomplete

Benchmark harness: `parqtel-query/examples/bench_label_values.rs` (run with
`cargo run --release -p parqtel-query --example bench_label_values`).

The UI query builder's label-value autocomplete must never enumerate a
high-cardinality label (tens of thousands of distinct values). It asks the
server for a bounded top-N of *recent* values, and narrows server-side as
the user types (`/api/v1/label/<name>/values?limit=10&match=<prefix>`).

### Worst case: 100,000 distinct `user_id` values (300K buffered points)

| Path | Latency (best of 20) | Payload per keystroke |
|------|----------------------|----------------------|
| **Bounded top-10 (no prefix)** — buffer walk, newest-first, stops at 10 | **~1.2 µs** | 10 values |
| **Bounded top-10 (prefix `user-099`)** — server-side `starts_with` filter | **~1.2 µs** | 10 values |
| Full enumeration (unbounded dropdown) | ~15.6 ms | 100,000 values |

- Bounded autocomplete is **~13,000× faster** than full enumeration and sends
  **10,000× less data** per keystroke.
- Implementation: `MemoryBuffer::recent_label_values` walks each series'
  points newest-first (points append in ingest order) and stops at the
  limit; the executor's `recent_label_values` layers the in-memory buffer
  (freshest) over the newest blocks' flush-time `label_values` index,
  bounded to the last 5 blocks. No full-history scan ever happens.
- The flush-time per-field index is capped at 10K values per label at write
  time (`MAX_VALUES_PER_FIELD`), so even cold (buffer-drained) lookups are
  bounded; the API clamps `limit` to 100.
- Unit tests: `parqtel-core/src/buffer.rs` (`recent_label_values_*`) and
  `parqtel-query/src/executor.rs` (`test_recent_label_values_*`) cover
  bounded-count, newest-first ordering, prefix filtering, and graceful
  degradation when blocks lack the index.
- Live validation: `make test-builder` (99 query-shape checks) and
  `make test-builder-ui` (headless-browser builder E2E, includes asserting
  the `user_id` dropdown shows exactly ≤ 10 of ~3,400 live values and that
  typing `user-04` narrows server-side).

### Reproducing

```bash
cargo run --release -p parqtel-query --example bench_label_values
```

## Prepared log queries

Log and trace search evaluated the *query* once per row: a `Regex::new` per
`=~` clause per row, a `to_lowercase()` of the whole body per search term per
row, a cloned `SearchQuery` per predicate atom, and a `String` clone of every
resolved field value. Compiling a pattern costs ~1–10 µs against ~10 ns for
the match, so the setup work dominated the search by three orders of
magnitude.

A *prepared* query now moves all of it to a one-time `new()`:
`PreparedLogQuery`, `PreparedPredicate`, `PreparedSpanQuery` and
`PreparedRowPredicate` hold compiled regexes, lowercased needles and resolved
severity thresholds, then match rows through `&self`. Substring tests use an
allocation-free ASCII-case-folded `contains_ci`, so no per-row lowercase is
needed at all (`(?i)` wildcard regexes already match the raw body).

The old `log_matches` / `log_matches_predicate` / `span_matches` /
`row_matches_pred` entry points remain as thin wrappers that prepare and match
in one call, so embedders and existing tests are unaffected; the row loops in
`executor.rs` prepare once.

### Result

20 000 rows, 3 clauses + 1 wildcard term, 50 % of rows matching, identical
results on both paths:

```
per-row prepare : 1.000s
prepared once   : 4.881ms
speedup         : 204.9x
```

### Semantics

The refactor is behaviour-preserving, and that is pinned by tests rather than
asserted in prose:

- `clause_and_term_semantics_are_pinned` — an explicit expectation table for
  every clause and term form, derived from the pre-refactor implementation
  (case-sensitive equality for non-body fields, case-insensitive body
  substring, `service` resolving through `resource_attributes` so it shadows
  the same-named attribute, only `*` setting the wildcard flag, severity
  mapping to `severity_number`).
- `predicate_composition_semantics_are_pinned` — AND/OR/NOT trees through the
  predicate API. These are separate because `parse_search` deliberately does
  **not** flatten a top-level `OR`: it falls back to an unconstrained query, so
  a search string containing `OR` matches everything. That is pre-existing
  behaviour, which is why composition is pinned where the handlers actually
  evaluate it.
- `regex_patterns_are_compiled_once_per_query` — the compiled-pattern count is
  a property of the query, not of the row count.
- `contains_ci_matches_lowercase_contains` — the allocation-free replacement
  agrees with the `to_lowercase().contains()` form it replaces, including
  empty needles and needles longer than the haystack.

A deliberate non-test: comparing the prepared path against the `log_matches`
wrapper cannot detect a semantic regression, because the wrapper is
implemented on top of the prepared path. An earlier draft of this work had
exactly that test and it was worthless; the expectation tables above replace it.

### Reproducing

```bash
cargo run --release -p parqtel-query --example bench_logql
```

## Query CPU off the async runtime

`execute_ast` did its per-step evaluation inline. A wide range query is
hundreds of milliseconds of aggregation with no `await` in it, so it pinned
the tokio worker for its whole duration — on a single-threaded runtime that
means every other request waits, and on a multi-worker one it means every
request multiplexed onto that worker waits.

The CPU half (`eval_steps` plus the conversion to `TimeSeries`) is now the free
function `evaluate_steps_to_series`, called through one `spawn_blocking`.
Everything above it — block scan, buffer scan — is I/O and stays on the worker.
The AST is cloned so the closure can be `'static`; that is a few dozen nodes
against a CPU cost orders of magnitude larger.

### Result

Single-threaded runtime, 300 series × 400 points, 20 000 evaluation steps:

| build | single query | 4 sequential | 4 concurrent | speedup from concurrency |
|---|---|---|---|---|
| evaluation inline | 2.92 s | 11.71 s | 11.48 s | **1.02×** (no overlap) |
| evaluation offloaded | 2.87 s | 11.41 s | 3.26 s | **3.50×** |

Single-query latency is unchanged (2.92 s → 2.87 s): the offload costs nothing,
it just stops the worker being blocked. All concurrent results are asserted
identical to the single-query result.

A single-threaded runtime is deliberate — it is the configuration where inline
CPU does the most damage, and it makes the effect measurable without needing
several cores.

### The guard

`test_query_evaluation_does_not_block_the_async_worker` runs a 1 ms ticker
against a heavy query on a current-thread runtime. Verified to **fail with 0
ticks** when the evaluation is inlined, so it discriminates rather than merely
passing. Asserting `>= 3` rather than an exact count keeps it off the timing
floor.

### Reproducing

```bash
cargo run --release -p parqtel-query --example bench_query_concurrency
```

## Shared label sets and hashed group keys

The single largest cost in query evaluation was `LabelSet` — a
`BTreeMap<String, String>` — being cloned at every stage boundary.

**Per-step label clones.** `eval_selector` cloned each series' labels once per
*step*. Every step sees the same series, so a 1 000-series × 1 000-step panel
spent ~20M string allocations producing identical results.
`SeriesData`, `HistData`, `InstantVector` and `RangeVector` now carry
`Arc<LabelSet>`; cloning is a refcount bump. Operators that genuinely change
labels build a new set; binary-op results reuse the input `Arc` outright when
nothing changes.

**Aggregation group keys.** The grouping key was a
`Vec<(String, String)>` built per series per step (two allocations per label)
and cloned twice more, with the group's `LabelSet` rebuilt by merging one label
at a time — O(L² log L). Now:

- the key is an allocation-free `u64` fingerprint of the projected pairs;
- `Grouping::By` lists are sorted and deduplicated **once per aggregation**, so
  `by(a,b)` and `by(b,a)` hash identically without a per-series sort;
- the group's label set is built once per distinct fingerprint and cached on
  the `Evaluator` for the whole query — a group's values change every step, its
  labels never do;
- `LabelSet::filtered` / `LabelSet::with` replace every remaining
  merge-per-label loop in the evaluator.

### Result

300 series × 400 points, 20 000 evaluation steps, identical results:

```
before:  3.01s
after:   0.438s
         6.9x
```

Concurrency behaviour is unchanged: four concurrent queries still run 3.5×
faster than sequentially, because each one's evaluation is its own blocking
job.

### Correctness

Hashing the group key means a collision would silently merge two groups. That is
pinned by `grouping_fingerprint_separates_distinct_projections`, which covers
different values, an absent label vs a present one, source label ordering, `by`
argument ordering, and labels excluded by `without`.
`group_labels_are_cached_across_steps_but_values_are_not` pins the other half:
group labels are identical across steps while the aggregated values are
recomputed.

Verified against the live stack: `by`/`without`/ungrouped/`on`/`ignoring` all
return the expected label shapes, and `test-aggregations` (22/22),
`test-functions` (104/104) and `test-builder` (99/99) pass.

### Reproducing

```bash
cargo run --release -p parqtel-query --example bench_query_concurrency
```

## Streaming step evaluation

`eval_steps` returned `Vec<(i64, InstantVector)>` — every step of the range held
live until the last one finished. The executor then folded those into the
result. Peak memory therefore scaled with **steps × output cardinality** on top
of the output itself.

`eval_steps_into` hands each step's vector to a consumer and drops it before the
next step, so peak memory is one step's vector. `eval_steps` remains as a thin
collecting wrapper, since the operator tests assert against that shape.

### Result

1 000 output series × 3 599 steps (a one-hour panel at a 1 s step), output
56.6 MB, measured in a fresh process:

| build | resident growth for the query | wall |
|---|---|---|
| collecting every step | 168.0 MB | 408 ms |
| streaming into the result | **109.9 MB** | **368 ms** |

58 MB less on a query whose own result is 56.6 MB. The saving is the retained
per-step vectors — `steps × series × ~24 B`, i.e. ~86 MB at this size — and it
scales with step count and output cardinality.

### The size of this win depended on shared label sets

The original estimate of ~600 MB for a 1 000-series × 1 000-step panel was
computed against **pre-`Arc`** label sets, at roughly 600 B each. With labels
shared behind an `Arc`, a retained series costs ~24 B, so that workload's
retained-step cost was already down to ~24 MB before this change.

Streaming removed what remained. It did not remove 600 MB, because shared label
sets had already taken most of it — worth knowing before quoting the original figure.

### A tempting optimisation that was unsound

Caching series fingerprints keyed on `std::sync::Arc::as_ptr` looks free: the
same series recur at every step, so it avoids re-hashing the label set. It is
wrong. Each step's vector is dropped at the end of its callback, so a freed
`Arc`'s address can be reused by the next step's allocation — a stale cache hit
returns another series' fingerprint and merges two distinct series into one.
Fingerprints are computed from label contents every time.

### Measurement notes

The package inherits `unsafe_code = "forbid"` from the workspace lint table, so
a counting `GlobalAlloc` is not available in an example here; `VmHWM` is used
instead. It is monotonic, so it must be sampled as a before/after delta in a
**fresh process**. An earlier version warmed up first, which defeated the
measurement entirely: the warm-up touched exactly the pages the measured query
wanted, the allocator kept them, and the reported growth collapsed from 168 MB
to 16 MB — a 10× under-report caused entirely by the warm-up.

### Reproducing

```bash
cargo run --release -p parqtel-query --example bench_query_memory
```
