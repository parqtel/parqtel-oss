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

## Metric-name pruning uses statistics first (`BL-03-03`, follow-up)

#51 added bloom filters on `metric_name` because the premise was that
row-group statistics cannot discriminate on a value column. **They can.** Rows
are written grouped by metric name, so a row group's `metric_name` min and max
are usually the *same value* — an exact, free answer requiring nothing to be
read from the filter region.

Probing the writer output confirms it: for a five-metric block with one row
group per metric, `metric_name` statistics are `min == max == "metric_N"` in
every group. (`service_name` statistics came back all-null in that probe only
because the fixture put `service.name` in point labels rather than resource
attributes, so the column was never populated.)

### The real comparison is three-way

200 metrics × 500 points in one block, querying one metric of 200, through the
production `Scanner::scan` path:

| configuration | size | write | query |
|---|---|---|---|
| neither (old blocks) | 1.29 MB | 38 ms | 30.33 ms |
| **statistics only** | 1.47 MB | 55 ms | **2.34 ms** |
| statistics + bloom | 1.49 MB | 42 ms | 2.49 ms |

Against "neither": **12.1× for 14 % size with statistics alone**; adding bloom
filters on top moves the query by nothing measurable (2.34 → 2.49 ms is noise)
and costs a further ~2 % size.

So the honest conclusion is that **statistics do the work and bloom filters are
a fallback**, not the primary mechanism. Bloom filters are retained only because
they still answer correctly when statistics are absent — a block written with
statistics disabled, or one where a long value was truncated out of the index —
and because they cost ~2 % of size for that.

An absent metric now costs **0.02 ms** instead of 30 ms: nothing is decoded at
all.

### A bug the change exposed

`list_label_values` calls `Scanner::scan` with an **empty** metric name to mean
"no metric filter" — it falls back to a full block scan for blocks that predate
the flush-time label index. The empty string sorts before every real metric
name, so treating it as a value to match pruned the whole block and silently
returned no label values. `test_list_label_values` caught it.

An empty metric name now means "no filter" and keeps every candidate, pinned by
`metric_pruning_treats_an_empty_name_as_no_filter` across all four
statistics/filter combinations.

### Why the statistics test is one-sided

Testing "the needle is outside `[min, max]`" proves **absence**, which is all
that is needed to skip a group. Unlike the time prune, it needs no null check:
min/max cover non-null values only, and a null row cannot contain the value
being searched for. Byte comparison is sound because UTF-8 byte order is
code-point order.

### Reproducing

```bash
cargo run --release -p parqtel-core --example bench_bloom
```

## Streaming step evaluation (`BL-02-03`)

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

### The size of this win depended on `BL-02-01`

The original estimate of ~600 MB for a 1 000-series × 1 000-step panel was
computed against **pre-`Arc`** label sets, at roughly 600 B each. With labels
shared behind an `Arc`, a retained series costs ~24 B, so that workload's
retained-step cost was already down to ~24 MB before this change.

Streaming removed what remained. It did not remove 600 MB, because `BL-02-01`
had already taken most of it — worth knowing before quoting the original figure.

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

## Bloom filters on the metric-name column (`BL-03-03`)

Row-group statistics only narrow on **time**. A metrics block interleaves every
metric it holds, so a query for one metric found every row group overlapping the
window and decoded all of them — pruning could only ever help a *range* query,
never a *selectivity* query, which is the common case in an SRE tool.

Bloom filters on `metric_name` and `service_name` answer "could this row group
contain this value?" without decoding a page.

### Result

200 metrics × 500 points in one block, queried for one metric of 200, measured
through the production `Scanner::scan` path:

| | without bloom | with bloom | change |
|---|---|---|---|
| block size | 1.47 MB | 1.49 MB | **+1.5 %** |
| block write | 44 ms | 59 ms | **+33 %** |
| single-metric query | 31.98 ms | **2.83 ms** | **11.3×** |

Both paths return the same 500 points, asserted by the benchmark. A query for a
metric that is not in the block decodes nothing at all and returns empty.

### Why the write cost is worth paying

+33 % on a 100 000-row block is ~15 ms. At the default 1 M-row block size that is
roughly 150 ms extra per block, and blocks are written every few minutes. Against
an 11× read improvement on the query that an SRE dashboard actually issues, that
is not a close call — but it is a real cost and is stated rather than omitted.

Filters are enabled **per column**, not globally, so `labels` (already the
largest column) does not pay for a filter nothing reads.

### Soundness, and the fail-open rule

A bloom filter may report a false positive but must never report a **false
negative** — a false negative silently drops data from a query result. Every
condition that cannot be evaluated keeps the row group:

- no filter on that column for that group,
- an unreadable filter,
- the column absent from the schema.

So the worst case is exactly the behaviour that existed before bloom filters.
Pinned by `bloom_pruning_never_drops_a_row_group_that_contains_the_metric`,
`bloom_pruning_reduces_the_row_groups_read`,
`bloom_pruning_drops_everything_for_an_absent_metric`,
`bloom_pruning_fails_open_when_no_filter_exists` and
`bloom_pruning_fails_open_for_an_unknown_column`.

### Backwards compatibility

The page index is loaded with `PageIndexPolicy::Optional`, not `Required`:
blocks written before this change have no page index and must still be
readable. `bloom_pruning_fails_open_when_no_filter_exists` writes a block with
the *old* writer properties and asserts every row group is kept.

### Still open

Page/column-index-based pruning *within* a row group. The index is now written
and loaded, but nothing reads it yet; it is left for a follow-up because it must
stay sound under the same fail-open rule.

`service_name` is not used as a bloom conjunct yet, and deliberately so: once
rows are ordered by `(metric_name, service_name, timestamp_ns)` (`BL-03-07`), the
row-group statistics prune on service *exactly* and for free — strictly better
than a filter that can only be probabilistic.

### Reproducing

```bash
cargo run --release -p parqtel-core --example bench_bloom
```

## Service-level row-group pruning (`BL-03-07`, revised)

#52 established that `metric_name` row-group statistics are exact and free,
which left `BL-03-07` worth only the **service** dimension. That turns out to
be available without re-ordering any rows either: rows are already written
grouped by `(metric, resource)`, so `service_name` statistics are exact per row
group too.

Probing a production-shaped block (one metric, eight services, one row group
each, service in the dotted OTLP resource key `service.name`) gives:

```
metric_name:   rg0..rg7  min == max == "http_requests"
service_name:  rg0..rg7  min == max == "svc-0" .. "svc-7"
```

Both exact. So `Scanner::scan` now takes an optional service, taken from the
query's `service.name` equality matcher, and prunes on it after pruning on the
metric.

### Result

Multi-tenant shape — one metric, eight services, querying `service.name="svc-3"`:

| | time | rows decoded |
|---|---|---|
| without the service predicate | 2.05 ms | 4 000 (whole block) |
| **with service pruning** | **0.43 ms** | 500 |

**4.8×**, and metric-only pruning cannot help here at all: all eight row groups
are the same metric, so this is entirely the service dimension.

### Only equality matchers, and only the real label

- `!=` and `=~` are ignored. They exclude some rows rather than identifying one
  group, so there is nothing safe to prune from.
- The underscored `service_name` spelling is **not** treated as an alias. No
  series carries that label — the resource attribute is stored under the
  dotted OTLP key — so such a matcher matches nothing at row level, and
  honouring it here would prune the block on a value no row can hold. Caught
  by live probing: the alias returned 0 series, which is correct only because
  the matcher matches nothing, not because pruning worked.

### Not over-pruning

The failure mode that matters is dropping a row group that *does* hold the
requested service, which is silent data loss. `test_service_selector_matches_the_unfiltered_query`
builds a two-service × two-host fixture with the dotted key and asserts
`cpu{service.name="web"}` returns exactly the web series, that an absent
service returns none, and that the fixture itself is production-shaped — the
existing `setup_with_data` fixture uses the underscored key, so it would have
made the test pass for the wrong reason. `service_pruning_is_exact_in_both_directions`
does the same at the row-group level.

Verified live: `http_requests_total_0{service.name="load-generator"}` returns
the same 30 series as the unfiltered query, and a nonexistent service returns
none.

### Row-group sizing, still open

`BL-03-07` also recommended retuning the default `row_group_size`. That is not
addressed here and remains open: with metric and service pruning both exact, the
remaining lever is how many *series* a row group spans when several share a
metric and service — which is the common case, since row-group boundaries
follow row counts, not series boundaries.

### Reproducing

```bash
cargo run --release -p parqtel-core --example bench_bloom
```

## Column projection on the metrics scan (`BL-03-08`, partial)

A metrics block has 15 columns; the scan decoded all 15 to use 7. The eight
skipped — `metric_kind`, the six k8s dictionaries and `resource_attributes` —
are per-row data a range query never touches.

The scan now projects to `timestamp_ns`, `metric_name`, `service_name`,
`labels`, `value_float`, `value_int`, `value_complex`.

### Result

200 metrics × 500 points in one block:

| query shape | no projection | projected | change |
|---|---|---|---|
| single metric of 200 (1 row group) | 2.47 ms | 2.33 ms | −6 % |
| one service of 8 (1 row group) | 0.40 ms | 0.34 ms | −15 % |
| **full-block scan, 100 000 rows** | **53.10 ms** | **38.13 ms** | **−28 %** |

The backlog target was "≥ 30 % for all three signals". **It is not met, and the
reason is that the target was written before #52 and #53.** Metric and service
pruning now means a typical query decodes a *single* row group, so the skipped
columns were never where the time went. Projection pays in proportion to how
many row groups a query must read — a wide panel or an old block — which is
the −28 % row.

### Two things that made it safe

**Indices resolved by name.** The decoder indexes columns positionally, and a
projection renumbers them. Every index is now looked up from the batch's own
schema once per batch. Hard-coded positions against a projection is exactly how
a projection silently starts reading the wrong column.

**The projection is intersected with the file's schema.** `with_projection`
errors on a column the block does not have, and blocks written by older builds
may lack one — so intersecting keeps them readable.

### The failure mode is silent, so the test is adversarial

A wrong mask does not crash: `labels` and `value_complex` are both `Utf8`, so
swapping them type-checks and returns wrong data. `projection_does_not_change_decoded_points`
compares timestamps and values against a full unprojected decode and asserts
each point's `host` label matches the series it came from.

Verified to **fail** for both a plain type mismatch and the same-typed
`labels`/`value_complex` swap.

Labels are compared by content rather than by fingerprint against
`StorageModel::row_to_point`, because the two decoders legitimately differ
there: the scan merges the dedicated `service_name` column back into the series
labels, while `row_to_point` leaves it in the resource attributes. Comparing
them would have asserted a difference that was already there.

### Not done

Logs and traces. Their decoders (`row_to_log`, `row_to_span`) index positionally
and would need a per-batch index struct passed in rather than resolved per row.
Worth doing for the log count and trace filter paths, where `events`, `links`
and the k8s dictionaries are likewise unused.

### Reproducing

```bash
cargo run --release -p parqtel-core --example bench_bloom
```

## Trace compaction and the label-value index (`BL-03-09`, `BL-03-10`)

### Trace blocks now merge at all

`compact_tiered` carried an explicit skip:

```rust
if *signal_type == SignalType::Traces {
    // For traces, skip read_source_blocks (which only handles metrics/logs)
    continue;
}
```

so trace blocks never merged. They accumulated monotonically until retention
deleted them, and every trace query paid the per-block open/footer/decode cost
across all of them.

`read_source_blocks` now decodes spans as a third arm
(`StorageModel::row_to_span`), `write_merged` encodes them with
`StorageModel::traces_to_chunk` sorted by `start_time_ns` so the merged block
keeps the time-ordered layout row-group pruning depends on, and both passes
handle all three signals uniformly.

Verified by `test_compactor_merges_trace_blocks`: three small trace blocks
become one, all nine spans survive, and the merged block still decodes.

### Compaction no longer destroys the label-value index

`write_merged` emitted `label_values: Default::default()`. Compaction therefore
*deleted* the flush-time label-value dictionary, so
`/api/v1/label/:name/values` lost its metadata source on every merge and had to
fall back to decoding blocks — degrading progressively with every compaction
pass until the source blocks aged out.

The merged metadata now carries the **union** of its sources' dictionaries,
under the same per-field cap the flush path applies.
`test_compaction_preserves_label_values` asserts both hosts survive a merge,
and was verified to **fail against the old behaviour** (`no entry found for
key`).

### Merge limits are configuration, and a cycle can converge

- `compaction_max_merge_blocks` (default 12) replaces the literals 8 and 12.
- `compaction_max_merges_per_pass` (default 8) replaces `break`ing after the
  first merge per signal. One merge per signal per hour cannot keep up with a
  cluster whose small blocks arrive faster than that; the cycle is now bounded
  but does real work.

### Verified by unit test rather than a long container run

Compaction runs hourly by default, so observing a trace merge end-to-end in a
short-lived container is not practical without lowering the interval and
waiting. The merge behaviour is therefore pinned by unit tests that run the
real `compact_once` against real Parquet blocks. Live container checks confirmed
no regression: spans ingest, flush (`parqtel_flush_rows_total{signal="traces"}`),
and trace search returns correct results.

## Series dictionary for the `labels` column: measured, not worth doing

`BL-03-04` proposed replacing the per-row JSON `labels` column with a
series-ID dictionary, on the reasoning that *"zstd must re-compress the same
text per row"*. That was the largest storage item in the backlog, an XL item
with an on-disk format change behind it.

**It was measured instead of built, and the premise is false.**

100 000 rows, 200 series, 5 000 rows/row-group:

| configuration | `Utf8` (current) | `Dictionary(Int32, Utf8)` | change |
|---|---|---|---|
| zstd (production) | 810 614 B | 810 678 B | **0.0 %** |
| uncompressed | 1 784 431 B | 1 784 495 B | **0.0 %** |
| write time | 0.034 s | 0.035 s | wash |

Reading the encodings back out of a block written with the **plain `Utf8`
column**:

```
"labels"  ["PLAIN", "RLE", "RLE_DICTIONARY"]
```

**Parquet dictionary-encodes string columns by default.** The Arrow-level field
type only decides whether *Arrow* does the encoding; Parquet applies the same
`RLE_DICTIONARY` either way. A series dictionary would therefore add an encoding
that is already present, and zstd compresses what is left — 9 MB of raw labels
JSON becomes 810 KB.

So: no schema version bump, no migration, no v1-read/v2-write reader. The
expensive, risky work is avoided rather than done for nothing.

The same conclusion applies to `metric_kind`, the other plain-`Utf8` per-row
column.

### What does survive

The per-row `labels.to_json()` on the write path and the per-series
`from_json()` on the read path are real **CPU** costs. The read side is already
covered by the per-chunk label cache from `perf(storage)`. The write side was
not measured here; `parqtel_flush_duration_seconds` is the metric to watch if
anyone wants to quantify it.

### Evidence kept

`probe_labels_size` is retained rather than deleted, both as the record of why
this was closed and as a guard for anyone who revisits the assumption.

```bash
cargo run --release -p parqtel-core --example probe_labels_size
```

## Write-ahead log wired for metrics (`BL-03-12`)

Bounded crash loss, verified by killing a real container.

### The ordering contract

A flush and the WAL must agree, or a crash duplicates or loses data:

1. rows appended to the WAL, request acknowledged
2. a flush takes rows, **snapshotting the WAL position it will cover**
3. the block is written and renamed
4. the **commit file** is advanced to that snapshot
5. segments below the commit are deleted

The commit file is the single source of truth and advances **after** the rename:

| crash after | result |
|---|---|
| (1)/(2) | replayed, nothing on disk — correct |
| (3) rename, before (4) | block exists but is not committed, so it is not indexed; the WAL still holds the rows and replay rewrites them. The orphan is invisible → one copy |
| (4) commit, before (5) | rows covered; replay skips at or below the commit → discarded, not duplicated |

Advancing the commit *before* the rename could lose a block — the one failure a
WAL exists to prevent.

The WAL is appended **while the shard lock is held**, so WAL order and writer
order are the same total order. Each shard records the highest WAL position
among the rows in its writer, taken and reset under that shard's own lock as
its writer is swapped — so a block can only ever commit positions for rows it
actually contains.

### A bug the crash test caught and the unit tests did not

The first version buffered appends in a 64 KB `BufWriter` and flushed only on
the fsync interval. A `SIGKILL` lost everything still in **userspace** — 4 of 5
points on the first crash run.

The module docs had claimed *"the page cache survives a process crash"*, which
is true of `write` and **false of an unflushed userspace buffer**. Every append
is now flushed to the OS; `fsync` remains governed by `WalSyncMode`.

The same run also showed the record type had to be a single `Metric`, not the
batch `Vec<Metric>` the rotator receives. The unit test and the replay helper
were wrong in the same way and would have recovered nothing.

### Verified against a real container

| scenario | outcome |
|---|---|
| ingest 5 points, nothing flushed, `SIGKILL`, restart | replay `records=5`; **all 5 queryable again** |
| ingest 3 points, graceful stop (flushes + commits), restart | replay `records=0 skipped=3`; 1 block / 3 rows; all 3 queryable, **not duplicated** |

### Cost

The flagged trade-off — a file write under the ingest lock — measured as
**54 ns average lock wait over 9 695 acquisitions** (0.52 ms total) under the
load generator:

```
parqtel_ingest_lock_wait_seconds{signal="metrics"}_sum   0.000522
parqtel_ingest_lock_wait_seconds{signal="metrics"}_count 9695
```

So it is not material, which is worth having measured rather than assumed.

`WalSyncMode::default()` is `Interval`, not the fastest option: a default that
quietly means "do not sync" is a footgun in a durability feature.

### Not yet

Logs and traces have no WAL (`log_wal_enabled` exists but is unused). The
mechanism is per-signal, so they follow the same shape.

### Configuration

`wal_enabled` (default **true**), `wal_sync_mode` (`interval`),
`wal_sync_interval_ms` (1000), `wal_max_segment_bytes` (64 MB). A WAL that
cannot be opened **fails startup** rather than silently accepting unrecoverable
data.
