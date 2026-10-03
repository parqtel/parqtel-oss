# Hot-Path Performance Optimizations

Branch: `perf/hot-path-optimizations` (merged; optimizations are part of current `main`)
Benchmark harness: `parqtel-server/examples/perf_bench.rs` (untracked-by-default; run with `cargo run --release -p parqtel-server --example perf_bench`)
Raw results: `baseline_before.txt`, `results_after.txt` (3 runs each, median-of-5 per metric)

## What changed

| # | Change | File(s) | Rationale |
|---|--------|---------|-----------|
| 1 | **Non-blocking flushes** — Parquet encode + zstd compress + disk I/O moved to `tokio::task::spawn_blocking`. The writer is swapped out (`mem::replace`) so the rotator can accept new pushes immediately. Flush is now idempotent on an empty buffer (fixes spurious `"Cannot flush empty buffer"` errors on idle systems). | `parqtel-ingest/src/service.rs` | Previously the entire compression + `fs::write` ran synchronously on a tokio worker thread while holding the ingest mutex — stalling all concurrent ingest batches and blocking a runtime worker. |
| 2 | **Capacity pre-check instead of error-string control flow** — rotator checks `writer.len() + incoming > capacity` and flushes *before* pushing, instead of matching `e.to_string().contains("buffer is full")` per point. Also fixes silent data loss: the old code discarded points of the batch that tripped capacity mid-push. Push returns whether it flushed so callers drain the memory buffer at the right time. | `service.rs` | Typed, lossless, one check per batch instead of string formatting per failed push. |
| 3 | **Scanner runs on blocking pool with pre-acquired semaphore** — `tokio::spawn` (async worker) replaced by `spawn_blocking`; semaphore permit acquired *before* spawning so concurrency is bounded without oversubscribing. | `parqtel-core/src/storage/scanner.rs` | Blocking file I/O inside `tokio::spawn` starves async workers under load. |
| 4 | **Row-group statistics pruning** — before decoding a block, row groups whose column-0 timestamp min/max cannot overlap `[start_ns, end_ns]` are skipped using parquet2 statistics (which were already written but never read). Falls back to scanning when stats are absent. | `scanner.rs` | Enables time-range pruning *within* block files. |
| 5 | **Multi-row-group block files** — flushes now split each block into ~25K-row Parquet row groups instead of one giant group. | `parqtel-ingest/src/writer.rs` | Prerequisite for #4: a single-row-group file has nothing to prune. Also bounds decode memory spikes. |
| 6 | **Per-chunk label caching in scanner** — parsed `LabelSet`s are cached by their raw JSON text within each chunk (keys borrow from the chunk arrays). Metric scan additionally skips resource-attribute parsing entirely (the scanner never used it), skips kind/correlation extraction, filters timestamp + metric name *before* any allocation, and compares metric names by borrowed `&str` instead of `.to_string()` per row. Log rows get cached attribute/resource label parsing via `row_to_log` (signature now takes caller-scoped caches). | `scanner.rs`, `models/storage/reader.rs` | Label JSON repeats once per series across thousands of rows; parsing it once per unique string removes ~1 serde round-trip per point. |
| 7 | **Log query: filter before sort, allocation-free search** — severity/search/matcher filtering now happens before the timestamp sort, and case-insensitive search uses byte-window ASCII folding instead of allocating `body.to_lowercase()` per record per query. ponytail: non-ASCII case folding is not handled. | `parqtel-query/src/executor.rs` | Fewer allocations and a smaller sort input. |
| 8 | **OTLP protobuf ingest fixed** — `/v1/{metrics,logs,traces}` were routed to the *JSON* handlers, so every protobuf request failed with 400. Added content-type dispatch (`application/x-protobuf` → proto handler, else JSON) per the OTLP spec. | `parqtel-server/src/router.rs`, `handlers/ingest.rs` | Pre-existing bug surfaced by the existing failing test; proto handlers existed but were unrouted. |
| 9 | Minor: compact index sidecar serialization (no pretty-print); clippy fix in prometheus handler. | `storage/index.rs`, `handlers/prometheus.rs` | |

## Benchmark methodology

Single machine, release build (`cargo run --release -p parqtel-server --example perf_bench`).

Dataset: 12 seeded Parquet blocks × 50K points × 100 series (2 row groups/block after change #5), plus live batches through the real OTLP JSON decode path.

Each number = median of 5 iterations after warmup; tables below show medians of 3 process runs.

## Results

Throughput in points/sec, higher is better:

| Benchmark | Before (main) | After (branch) | Δ |
|---|---|---|---|
| ingest (decode + buffer push) | ~570 K pts/s | ~550 K pts/s | ≈ flat (±3% noise) |
| flush (50K pts → Parquet) | ~840 K pts/s | ~830 K pts/s | ≈ flat (compression-bound) |
| **scan (full range, 600K pts)** | ~4.58 M pts/s | **~6.40 M pts/s** | **+39%** |
| query (full range, executor) | ~1.99 M pts/s | ~1.95 M pts/s | ≈ flat |
| **scan narrow** (pruned to 25K pts) | n/a* | 15–16 ms/op | new capability |
| **query narrow** (pruned) | n/a* | ~26 ms/op (~11.9 M pts/s effective) | new capability |
| svc-ingest (8 concurrent workers, flush contention) | ~2110 ms/op | ~2125 ms/op | parity, see notes |

\* Narrow benchmarks were added mid-work; they only become meaningful together with changes #4+#5, which is exactly their point: a query touching half of one block out of twelve now decodes ~half of ~one file instead of everything.

### Notes on the numbers

- **The full-range query is unchanged** because executor-side grouping/downsampling dominates once scanning is fast. Scanning itself improved 39%.
- **svc-ingest is at parity by design.** The old code held the ingest mutex across the whole batch loop including synchronous Parquet writes — terrible for latency (workers fully stall behind each other's flushes) but not throughput, since work was serialized anyway. The win from change #1/#3 is *isolation*: no tokio worker thread ever blocks on disk I/O, so event-loop tasks (health checks, queries, other signals) don't stall behind flushes. Under co-located load this prevents p99 spikes rather than raising mean throughput.
- The svc-ingest bench also validated a regression we caught and fixed during development: without propagating the flush signal back (#2), the memory buffer never drained and alloc pressure cost ~7%.

### Reproducing

```bash
cargo run --release -p parqtel-server --example perf_bench
```

Baseline comparison requires checking out `main` (the harness only uses APIs present on both branches).

## Known follow-ups

- Shard rotators per metric-name hash if single-mutex ingest contention ever shows up in profiles.
- Expose `ROW_GROUP_ROWS` (currently 25K, hardcoded) via `BlockConfig`.
- Non-ASCII case folding for log search (currently ASCII-only).
- `get_log_field_values` / `list_label_values` re-scan blocks per call; add a short-TTL cache if UI traffic makes them hot.

## High-Cardinality Label-Value Autocomplete (query builder)

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

## LogQL prepared-query path (`BL-02-06`, `BL-02-07`, `BL-02-08`)

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

## Query CPU off the async runtime (`BL-02-04a`)

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

### Not done: sharding the evaluator

The original item also proposed partitioning `SeriesData` across the blocking
pool to raise parallelism *within* one query. That is split out as
`BL-02-04b` and deliberately left open: with `04a` in place, concurrent requests
already overlap, and a single very wide query is dominated by the per-step
`LabelSet` clone (`BL-02-01`) rather than by scheduling. Fixing the allocation
count is worth more than splitting the loop.

### Reproducing

```bash
cargo run --release -p parqtel-query --example bench_query_concurrency
```

## Shared label sets and hashed group keys (`BL-02-01`, `BL-02-02`)

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
