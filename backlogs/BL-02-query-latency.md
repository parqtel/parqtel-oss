# BL-02 — Query Latency

**Domain:** `parqtel-query`, response path in `parqtel-server/handlers/prometheus.rs`
**Goal:** instant-query p99 < 1 s at 1 000 series × 1 000 steps; log/trace search p99 < 300 ms; query peak RSS < 150 MB.

**Reference workload used for all estimates:** 1 000 series × 10 labels × 1 000 steps (15 s step over ~4 h), 15 s scrape interval → ~15 samples/series/step. "Points" = raw `DataPoint`s scanned.

Severity: **C** Critical, **H** High, **M** Medium, **L** Low. Effort: S ≤ 2d, M ≤ 1w, L > 1w.

---

## BL-02-01 (C) — `LabelSet` deep-cloned per series **per step** in every selector evaluation

**Evidence** — `parqtel-query/src/eval.rs:160` `out.push((labels.clone(), v));` inside `eval_selector`, which runs **once per step** (`eval.rs:102-112`). Same pattern at `eval.rs:87` (hist selector), `:193` (range windows), `:524`, `:588`. `LabelSet` is a `BTreeMap<String,String>` (`parqtel-core/src/models/labels.rs:11-15`), so a clone is `L` heap allocations plus a fresh B-tree — not a refcount bump.

**Gap.** At the reference workload: 1 000 steps × 1 000 series × 10 labels × 2 `String`s = **~20 M `String` allocations + 1 M BTreeMap builds per query**, purely to hand a label set to the next operator. This alone is multiple seconds and is the largest allocator load in the crate.

**Resolution.** Make the evaluator's internal value type carry `Arc<LabelSet>`: `InstantVector`/`RangeVector` in `parqtel-query/src/ast.rs:156-167` hold `Vec<(Arc<LabelSet>, f64)>`; cloning is a refcount bump. Operators that *mutate* labels (`label_replace`, `label_join`, aggregations that project) build a new set once; pure readers (`map_values`, binary ops, most range functions) reuse the `Arc`. Provide `LabelSet::with_capacity`/single-pass builders so the mutating sites are O(L log L), not O(L² log L).

**Acceptance.** 20 M → ~1 M refcount bumps. Wall-clock for `eval_steps` on the reference workload improves ≥ 10×; allocation profile shows `BTreeMap` construction confined to mutating operators.

**Effort** XL · **Risk** High (touches every operator; conformance suite `parqtel-query/src/conformance.rs` must gate each step)

---

## BL-02-02 (C) — Aggregation group keys allocated and cloned 3× per series per step; label sets rebuilt via `merge`-per-label

**Evidence** — `parqtel-query/src/eval.rs:682-700`: the key `Vec<(String, String)>` is built fresh per series — `labels.get(l).map(|x| (l.clone(), x.to_string()))` (`:686`), and `Grouping::Without` also sorts (`:698`). Then `groups.entry(key.clone())` (`:701`) and `group_labels.entry(key.clone())` (`:702`) deep-clone the key **twice more**. Finally `:708-714` rebuilds a `LabelSet` one label at a time via `merge`, which clones the entire map per `merge` (`labels.rs:72-78`) → **O(L² log L)**.

**Gap.** At the reference workload: 1 000 × 1 000 × 3 copies × 10 pairs × 2 `String`s = **~60 M allocations**, plus an O(L² log L) label-set rebuild per group. `group_labels` also duplicates data already recoverable from the key.

**Resolution.**
- Key groups by the `u64` fingerprint the executor **already computes** at `parqtel-query/src/executor.rs:261`; carry it in the series handle rather than recomputing.
- Build the group `LabelSet` with a single `LabelSet::try_from_iter(key)` (`labels.rs:21-39` builds the map in one pass) instead of N `merge` calls.
- Store labels in the group entry; drop the second map.
- Same single-pass builder replaces the `merge`-per-label pattern in `executor.rs:1827-1839` (`strip_metric_name`) and `eval.rs:1068-1087` (`result_labels`).

**Acceptance.** ~60 M allocations → ~0 extra. `sum by(...)` on the reference workload: ≥ 8× faster; no `merge` call remains inside a per-series or per-step loop (enforceable by a clippy lint or a source assertion test).

**Effort** L · **Risk** Medium

---

## BL-02-03 (C) — Every step's result is retained in memory simultaneously

**Evidence** — `eval.rs:93-114` builds `Vec<(i64, InstantVector)>` for **all** steps, each `InstantVector` owning a `LabelSet` per series (`ast.rs:156-159`). `executor.rs:308` holds that whole vector, then `executor.rs:312-326` converts it to `BTreeMap<u64, TimeSeries>` — cloning labels **again** per series (`strip_metric_name`, itself O(L²) per BL-02-02).

**Gap.** At the reference workload: 1 000 steps × 1 000 series × ~600 B per label set ≈ **~600 MB live** for a single 4-hour panel, plus the conversion copy. This — not raw CPU — is what makes "sub-second at scale" impossible: it is a page-fault and allocator-pressure problem.

**Resolution.** Invert the loops: `eval_steps` should take a callback (`FnMut(&mut ResultBuilder)`) and fold each step's vector into `out_series` before dropping it. `executor.rs:313-325` already does the fold — just make it the inner loop. Peak memory becomes O(series) instead of O(steps × series), and labels are cloned once per **output** series rather than once per (step, series).

**Acceptance.** Peak RSS for a 4-hour, 1 000-series panel < 150 MB. Guard with a test that asserts peak allocation does not scale with `step_ns`.

**Effort** M · **Risk** Medium

---

## BL-02-04a (C, landed) — All query CPU runs on the async runtime thread; the evaluator has no `spawn_blocking`

**Evidence**
- `execute_ast` (`executor.rs:193-342`) does the block scan (which *does* use `spawn_blocking`, `parqtel-core/src/storage/scanner.rs:69`) and then the **entire** group/sort/evaluate/serialize pipeline inline in an `async fn` on the axum worker.
- `parqtel-query/src/eval.rs` has no parallelism at all: `eval_steps` is a single `while` loop (`:102`); `eval_aggregation`, `eval_binary`, `eval_range_windows` are single-threaded. No `rayon`, no partitioning.

**Gap.** One query saturates one core and — because it never yields — blocks the runtime worker, and every other request multiplexed onto it, for its full duration. Two concurrent dashboards cost 2× latency; extra cores are idle. This is simultaneously a latency problem and a fairness problem.

**Resolution taken (04a).** The CPU half of `execute_ast` — `eval_steps` plus
the conversion to `TimeSeries` — is now the free function
`evaluate_steps_to_series`, called through one `spawn_blocking`. Everything
above it is I/O (block scan, buffer scan) and stays on the worker. The AST is
cloned so the closure is `'static`, which costs a few dozen nodes against a CPU
cost orders of magnitude larger. Single-query latency is unchanged; what changes
is that the worker is no longer pinned for the duration.

Measured on a **single-threaded** runtime (where inline CPU does the most
damage), 300 series × 400 points, 20 000 evaluation steps, 4 concurrent queries:

| build | single | 4 sequential | 4 concurrent | concurrency speedup |
|---|---|---|---|---|
| eval inline | 2.92s | 11.71s | 11.48s | **1.02×** (no overlap) |
| eval offloaded | 2.87s | 11.41s | 3.26s | **3.50×** |

Reproduce: `cargo run --release -p parqtel-query --example bench_query_concurrency`.

The guard is `test_query_evaluation_does_not_block_the_async_worker`, which runs
a 1 ms ticker against a heavy query on a current-thread runtime. Verified it
**fails with 0 ticks** when the evaluation is inlined, so it discriminates
rather than merely passing.

**Acceptance.** Four concurrent 20 000-step queries complete in ≤ 1.5× the
single-query time; the worker-starvation test passes. Met.

**Effort** M · **Risk** Low (no behaviour change; the extraction is mechanical)

## BL-02-04b (M, open) — Shard the evaluator across series

The second half of the original BL-02-04: partition `SeriesData` into N chunks
by `fingerprint % N`, evaluate each shard on the blocking pool, and merge group
results by fingerprint.

**Why it is still open.** `04a` already lets concurrent *requests* overlap,
because each one's evaluation is its own `spawn_blocking` job and the blocking
pool is large. Sharding would raise the parallelism *within* one query, which
matters only for a single very wide query — and that case is dominated by
`BL-02-01` (a `LabelSet` clone per series per step), not by scheduling. Fixing
the allocation count first is worth far more than splitting the loop.

**Acceptance when taken.** A single 1 000-series × 20 000-step query uses more
than one core, and results are identical to the unpartitioned path.

**Effort** L · **Risk** Medium (per-series concat order can change float
summation order; assert against the existing conformance fixtures)

---

## BL-02-05 (C) — Matchers and fingerprints evaluated **per data point**, not per series

**Evidence** — `executor.rs:249` runs `evaluate_matchers(matchers, &dp.labels, name)` for **every** `DataPoint`; `:261` computes `dp.labels.fingerprint()` likewise. Legacy path: `executor.rs:377`, `:383`, `:466`, `:474`. `executor.rs:384`/`:475` also insert the fingerprint into a `HashSet<u64>` per point only to take `.len()` at the end.

**Gap.** All points of a series share one `LabelSet`, so this is `points × matchers` B-tree string lookups and `points × L` hashing where `series × matchers` would do. At 15 samples/series/step this is a 15× multiplier on both matching and hashing. The redundant `HashSet` insert is pure waste per point.

**Resolution.** Group by fingerprint first (`HashMap<u64, (LabelSet, Vec<…>)>`, as the AST path partly does at `executor.rs:244`), then run matchers once per distinct label set; drop `matched_series_fps` in favour of the map's length. Bonus: `matcher.rs:472-479 needs_ast()` parses the query and the handler parses it again (`prometheus.rs:201`, `:367`) — two parses plus two `Regex::new` passes per request; return the parsed `Expr` or add a small parse cache.

**Acceptance.** Matcher evaluation count drops from O(points) to O(series) per query; request path parses the PromQL expression exactly once.

**Effort** M · **Risk** Low

---

## BL-02-06 (C, landed) — `Regex::new` compiled **per row** in LogQL and pipeline predicates

**Evidence**
- `parqtel-query/src/logql.rs:681` — `let re = regex::Regex::new(regex).ok();` inside the per-clause loop of `log_matches`, which is the closure pushed into the block scan (`executor.rs:796-798`, `:709-711`). Runs **once per candidate row**.
- `logql.rs:839` — same, in `span_matches`, per span per clause.
- `logql.rs:741` — `wildcard_to_regex_ci` builds and compiles a regex per term **per row** (`:716`).
- `parqtel-query/src/pipeline_exec.rs:130` — `Regex::new` inside `row_matches_pred`, i.e. per row.
- By contrast `matcher.rs:41` compiles selector regexes **once at parse time** and `Regex` clones are `Arc` bumps. The defect is confined to the LogQL/pipeline predicates.

**Gap.** Compiling a regex costs ~1–10 µs versus ~10 ns for the match — three orders of magnitude. A 5 000-row block with a `=~` clause spends ~50 ms on compilation alone.

**Resolution.** Change `Clause::Re { field, regex: String }` (clause enum at `logql.rs:342-429`) to hold `regex: Arc<Regex>`, populated in the parser (which already validates at `logql.rs:471`). Same for `Term.wildcard`. Keep the compile-once behaviour the selector path already has as the model.

**Resolution taken.** Rather than change the parsed `Clause` shape, a
*prepared* query was added alongside it: `PreparedLogQuery`,
`PreparedPredicate`, `PreparedSpanQuery` and `PreparedRowPredicate` move every
per-query computation — regex compilation, wildcard compilation, needle
lowercasing, severity-rank lookup — to a one-time `new()`, then match rows
through `&self`. The existing `log_matches` / `log_matches_predicate` /
`span_matches` / `row_matches_pred` remain as thin wrappers that prepare and
match in one call, so embedders and tests are unaffected; the row loops in
`executor.rs` now prepare once. `PreparedLogQuery::compiled_patterns()` makes
"compiled once per query, not per row" directly assertable.

**Acceptance.** Regex compilation count per log query equals the number of
`=~` clauses, not the number of rows — covered by
`regex_patterns_are_compiled_once_per_query`. Semantics pinned by
`clause_and_term_semantics_are_pinned` and
`predicate_composition_semantics_are_pinned`. Log query p99 < 300 ms on 50k
rows: measured **205× faster** on the 3-clause/1-term benchmark over 20 000
rows (`cargo run --release -p parqtel-query --example bench_logql`).

**Effort** M · **Risk** Low (mechanical, parser-local)

---

## BL-02-07 (M, landed) — Per-row `to_lowercase()` allocations, repeated once **per term**

**Evidence**
- `logql.rs:714` — `let body = log.body.to_lowercase();` sits **inside** `for term in &q.terms`: a 2 KB body is lowercased and allocated 3× per row for 3 terms.
- `logql.rs:669`, `:672` — the *pattern* is lowered per row (should be once at parse).
- `logql.rs:884`, `:888` — `s.name.to_lowercase()` / `v.to_lowercase()` per span attribute per term in `span_matches`.
- `pipeline_exec.rs:107` — `hay.to_lowercase().contains(&term.text)`, same.
- `executor.rs:1665-1674 contains_ci` is the *correct* allocation-free pattern and already exists in the crate — the LogQL path simply does not use it.

**Gap.** `terms × body_len` bytes of allocation per row where `body_len` once suffices.

**Resolution.** Hoist the lowercase out of the term loop; store terms pre-lowercased at parse (they already are, `logql.rs:305`); replace the substring scans with the existing `contains_ci`. Optional follow-up: `contains_ci`'s byte-window scan (`executor.rs:1665-1674`) has no `memchr` fast path — a precompiled case-insensitive literal regex would use SIMD; only worth it once `contains_ci` becomes the shared implementation.

**Resolution taken.** Superseded by the prepared-query work above: with the
needle pre-lowercased and the haystack left raw, no per-row lowercase is needed
at all. `contains_ci` compares ASCII case-folded over byte windows, so
`hay.to_lowercase().contains(&needle.to_lowercase())` becomes allocation-free;
`(?i)`-prefixed wildcard regexes already matched case-insensitively, so they
also run against the raw body. Equivalence with the old form is asserted by
`contains_ci_matches_lowercase_contains`.

**Acceptance.** Zero per-row lowercase allocations; `contains_ci` verified
equivalent to the lowercase form across case, empty-needle and
needle-longer-than-haystack cases. Included in the 205× benchmark result.

**Effort** S · **Risk** Low

---

## BL-02-08 (M, landed) — Per-row `SearchQuery` construction with cloned `Clause`s in the predicate evaluator

**Evidence** — `logql.rs:636-650`: `log_matches_predicate` handles **every** atom by building a fresh `SearchQuery { clauses: vec![clause.clone()], terms: vec![] }` and recursing. `Clause` owns `String` field names (`logql.rs:342-429`), so that is 1–3 `String` clones + 2 `Vec` allocations **per atom per row**. Same at `:701-707` (`Clause::Not`) and `:871-877` (span side).

**Gap.** With a 5-atom AND predicate over 640 k rows: ~10 M transient allocations used purely for control flow.

**Resolution.** Split `log_matches` into `clause_matches(&Clause, log, extra)` and `term_matches(&Term, log)`; the `And`/`Or`/`Not` tree then recurses directly with no wrapper `SearchQuery`. Pure refactor, no semantic change.

**Resolution taken.** The predicate walkers now recurse over
`PreparedPredicate`/`PreparedRowPredicate` trees directly — the `SearchQuery`
wrapper per atom is gone, so no `Clause` (and therefore no `String` field
name) is cloned per row.

**Acceptance.** No allocation attributable to predicate-tree traversal in the
allocation profile of a log query. Covered by
`predicate_composition_semantics_are_pinned`, which exercises AND/OR/NOT
through both the wrapper and the prepared tree.

**Effort** S · **Risk** Low

---

## BL-02-09 (M) — Series grouping in `apply_post_processing` is O(samples²) per group

**Evidence** — `executor.rs:1715-1725`: for every incoming sample, `entry.samples.iter_mut().find(|e| e.timestamp_ns == s.timestamp_ns)` — a **linear scan** of the group's sample vector. Merging `m` series of `k` samples costs **O(m · S · k)**; at `S=1000`, `k=1000` that is ~10⁹ comparisons. Then `executor.rs:1729` sorts each group again and `:1708` `key.clone()` deep-clones the group key per series.

**Resolution.** Collect samples per group, sort once by timestamp, then merge adjacent equal timestamps in a single linear pass — or accumulate into a `BTreeMap<i64, f64>` / step-indexed buffer. This is the legacy engine's `by(...)`/`without(...)` path, which `matcher.rs:501-510` routes aggregations away from, but plain `foo by (l)` still reaches it.

**Acceptance.** Grouped legacy queries are O(S·k log S); no `find` over a sample vector remains in the merge loop.

**Effort** S · **Risk** Low

---

## BL-02-10 (M) — Response path allocates a `String` per sample and a `serde_json::Value` map per series, then buffers the whole payload

**Evidence** — `parqtel-server/src/handlers/prometheus.rs:144-146` and `:109-111`: `values.push((s.timestamp_ns as f64 / 1e9, s.value.to_string()))` — one `String` per sample (1 M allocations at the reference workload). `:147`/`:118`: `serde_json::to_value(&ts.labels)` builds a `serde_json::Map` per series, then `Json(PrometheusResponse{…})` serializes the entire nested structure into an in-memory `Vec<u8>` before writing. `QueryResult` also derives `Serialize` (`parqtel-query/src/models.rs:31`) although handlers never serialize it directly.

**Gap.** Two full copies of the payload (Value tree → `Vec<u8>`), 1 M `String` allocations, and — a correctness-pressure point — `f64::to_string()` emits `NaN`/`inf`, which `serde_json::Number` would reject; the string form papers over a real edge case.

**Resolution.** Serialize straight to the response body with a streaming writer (`serde_json::to_writer` over the axum body) and emit sample values as raw JSON numbers. Explicitly handle non-finite values (omit the sample, per Prometheus convention).

**Acceptance.** Zero `String` allocations per sample; response payload materialised once; wide-panel p99 improved by ≥ 20 % with no change in output bytes for finite values.

**Effort** M · **Risk** Low

---

## BL-02-11 (M) — Buffer merge holds the buffer read lock while cloning; index lock taken once per metric name

**Evidence**
- `executor.rs:239` / `:455` — `raw.extend(self.buffer.scan_metrics(...).await)`; `MemoryBuffer::scan_metrics` (`parqtel-core/src/buffer.rs:67-82`) **clones every matching `DataPoint`** (including its `LabelSet`) into a fresh `Vec` **while holding the buffer's `tokio::sync::RwLock` read guard**. Same for `scan_logs` (`:85-91`) and `scan_spans` (`:58-64`), which clone entire filtered buffers.
- `executor.rs:1081-1085` — `for name in names { let idx = self.index.read().await; idx.query(…) }`: one async lock acquisition (with `idx.query` under it) **per metric name**.

**Gap.** (a) The buffer read lock is held across a large allocation-heavy loop, blocking ingest writers (`buffer.rs:34`, `:45`) for its duration — this is the one place where the **query path stalls ingestion**, and with a 2-hour buffer (BL-01-04) it is O(hundreds of thousands) clones per log query. Tokio's write-preferring `RwLock` stalls all pushes for that duration. (b) Thousands of distinct metrics means thousands of lock round-trips that interleave with ingest writers on the same lock.

**Resolution.** (a) Clone the `Vec` under the lock and filter *outside* it, or store buffer rows as `Arc`-shared columnar batches so a "clone" is a refcount bump; hold the lock for at most one `Vec` clone. (b) Acquire the index read lock **once**, compute all block sets, drop the guard. (c) Since both sources are individually sorted, use a k-way merge on timestamp instead of pushing everything through one sort.

**Acceptance.** Measured lock-wait time for ingest writers during a concurrent log query < 5 ms; index acquisitions per request = 1.

**Effort** M · **Risk** Low

---

## BL-02-12 (M) — Logs and traces: double sorting, `hex::encode` per span, filter not pushed into the trace scan

**Evidence**
- `scanner.rs:302` sorts `all_logs` after **every** block extend, then trims: with 128 blocks and `keep = 10 000` that is 128 sorts of ~10 000 elements (~1.7 × 10⁷ comparisons plus 128 full re-sorts). `executor.rs:666` (and `:753`, `:842`) sorts the merged set **again**.
- `scanner.rs:442` sorts spans; `executor.rs:1248` sorts **again**.
- `executor.rs:1253` — `spans.retain(|s| hex::encode(s.trace_id) == tid_lower)` allocates a 32-char `String` **per span**.
- `executor.rs:1231` — `scan_cap = 10_000` when a filter is present vs `200` otherwise, with the filter applied **after** the scan (`:1257`); the log path already pushes its filter into the block scan (`executor.rs:646`), traces do not.

**Resolution.** (a) K-way merge with a `BinaryHeap` of size `nblocks` in the scanner — O(N log B) instead of O(N log N) per block — and delete the final sorts in the executor (the merged set is already ordered). (b) Compare `s.trace_id` bytes directly against the decoded filter (validated as 32 hex chars at the boundary) instead of `hex::encode`. (c) Push the trace filter into `scan_traces` the way logs already do, and set the cap from the post-filter expectation rather than a blanket 10 000.

**Acceptance.** Log search over 128 blocks is ≈128× cheaper in sort work; no allocation per span in the trace-id filter; filtered trace search stops decoding once `limit` matches are found.

**Effort** M · **Risk** Low

---

## BL-02-13 (M) — `list_label_values` / `get_log_field_values` re-scan blocks and clone block metadata per call

**Evidence** — `executor.rs:1423`, `:1435` clone `BlockMetadata` per block; `executor.rs:1293`, `:1317`, `:1357`, `:1509` do `idx.blocks[start..].to_vec()`. `BlockMetadata` carries `label_names` and `label_values` collections (`executor.rs:1297`, `:1324`, `:1513`), so these clones copy whole per-block dictionaries — while the read lock is held (`:1313-1343`). These endpoints back the UI query-builder autocomplete and are polled frequently.

**Resolution.** Iterate by slice reference under the lock and copy out only the strings needed; move the per-block dictionaries behind a shared `Arc` side table (pairs with BL-03-06). Add a short-TTL cache if UI traffic still makes them hot — this was already identified as a follow-up in `docs/benchmarks/PERFORMANCE.md`.

**Acceptance.** `/api/v1/label/:name/values` p99 < 50 ms with 100k distinct values; no `BlockMetadata` clone on the path.

**Effort** S · **Risk** Low

---

## BL-02-14 (M) — `correlate`: O(rows × spans) and O(rows × logs) linear scans

**Evidence** — `executor.rs:930-937`: for every row without a `trace_id`, a full linear scan of `by_service_time` (`Vec<&Span>` built at `:912`) with a per-span attribute lookup — **O(rows × spans)**, with the span count hard-capped at 10 000 (`:902`). `executor.rs:976-983`: the same shape for `corr_signal == "logs"`, unbounded.

**Gap.** 10 000 spans × 10 000 rows ≈ 10⁸ span comparisons, on the async worker.

**Resolution.** Build `HashMap<&str /*service*/, Vec<(i64 ts, &Span)>>` sorted by timestamp once, then binary-search the window: O(rows log spans).

**Acceptance.** `/v1/correlate` over 10k spans completes < 500 ms (currently seconds).

**Effort** S · **Risk** Low

---

## BL-02-15 (M) — Binary-op match keys built and sorted per series **per step**; full `matching` clone per step

**Evidence** — `parqtel-query/src/eval.rs:924-947`: `match_key` allocates a `Vec<(String, String)>` per series and `k.sort()`s it, for **both sides of every binary op, at every step**; the `BTreeMap<Vec<(String,String)>, …>` index (`:952`) is rebuilt from scratch per step; `:917-920` `b.matching.clone()` deep-clones the `on(...)`/`ignoring(...)` label vectors per step.

**Resolution.** Precompute the match key (or the fingerprint) once per series **per query**, outside the step loop; index the right-hand vector with a `HashMap` (see BL-02-16).

**Acceptance.** Binary-op allocation count is independent of `steps`.

**Effort** M · **Risk** Low

---

## BL-02-16 (L) — `InstantVector::get` is a linear scan comparing full `LabelSet`s; `or` deep-clones the left vector

**Evidence** — `ast.rs:162-167` `get` scans linearly comparing `LabelSet`s (B-tree equality). Used by `And` (`eval.rs:889`), `Unless` (`:908`) and `Or` (`:899`) → **O(n·m)** label-set comparisons on 1 000-series vectors. `eval.rs:897` `let mut out = lhs.series.clone();` deep-clones every `LabelSet` in the left vector even when `rhs` is empty.

**Resolution.** Build a `HashMap<u64 /*fingerprint*/, f64>` index of the right vector once (O(m)), then O(1) lookups; drop the clone in `Or` when `rhs` is empty.

**Acceptance.** Set operators are O(n+m) on label lookups.

**Effort** S · **Risk** Low

---

## BL-02-17 (L) — Pipeline `stats` stage: JSON string key per row, O(groups × buckets) with per-iteration allocation

**Evidence** — `parqtel-query/src/pipeline_exec.rs:194-203`: `serde_json::to_string(&key)` per row purely to build a group key. `:231-239`: for **every group × every bucket** in the global timestamp list, `compute_agg` runs even when the bucket is absent, substituting `let empty: Vec<&Row> = vec![];` allocated **inside** the loop (`:232`). `:319` `let mut sorted = nums.clone();` clones the whole numeric vector for p50/p95/p99.

**Resolution.** Key groups by a hash of the `Vec<Json>` values (or the joined field values — no serde); hoist `empty` out of the loop and return `Json::Null` without allocating; `sort_unstable_by` in place instead of `clone()`.

**Acceptance.** No `serde_json::to_string` per row; `stats` stage allocation independent of `groups × buckets` for sparse windows.

**Effort** S · **Risk** Low

---

## BL-02-18 (L) — `MetricValue` (with `Vec`s) deep-cloned per point per step in legacy downsampling; window vector re-created per step

**Evidence** — `parqtel-query/src/aggregation.rs:336` `window.push(points[idx].clone());` — `MetricValue::Histogram`/`Summary` carry `Vec<f64>`/`Vec<u64>` (`aggregation.rs:120-133`), so this is a deep copy per point per step bucket; the `window` vector is re-created for every step (`aggregation.rs:333`).

**Resolution.** Take `&[(i64, MetricValue)]` for the window (`aggregate` already does) and avoid the clone; reuse one `Vec` across steps via `window.clear()`.

**Acceptance.** No `MetricValue::clone` in the downsample loop.

**Effort** S · **Risk** Low

---

## BL-02-19 (L) — Remaining query-path inefficiencies


| # | Gap | Evidence | Resolution |
|---|-----|----------|------------|
| a | `Regex::new` per **step** for `label_replace` in the AST engine (legacy path compiles once) | `eval.rs:512` inside `eval_label_replace`, called per step from `eval_call` (`:293`) | Hoist into parse phase — store `Arc<Regex>` in `CallExpr` |
| b | `format!("${i}")` per capture group per series per step | `eval.rs:516-522` | Pre-render the replacement template as literal/capture segments once |
| c | `label_join` builds a `Vec<String>` of all source values per series per step | `eval.rs:546-551` | Reuse the series' `Arc<LabelSet>`; single-pass join |
| d | `AggregationOp::Min` folds twice (`is_finite().then(|| identical fold)`) | `eval.rs:726-732` | Compute once into a local |
| e | `Vec<(i64, f64)>` re-materialised per series per step in the range path, and samples re-copied `window_steps` times | `eval.rs:186-192` copies `points[lo..hi]` into `Vec<Sample>`, then `eval.rs:665` copies again into `Vec<(i64,f64)>` for `apply_range_fn` | Pass `&[(i64, f64)]` (the source slice) directly; drop the `Sample` intermediate |
| f | Range functions re-walk the window every step (quadratic in window length) | `eval.rs` range path; a 5 m window at 15 s = 20 steps copies each sample 20× | Precompute per-series prefix sums of `segment_increase` once per query so `rate`/`increase` are O(1) per step; keep the linear walk for `changes`/`resets` |
| g | `parse_query` allocates a prefix `String` per function-name probe (~20 probes) | `matcher.rs:549-556` `strip_fn`, `:201`, `:331`, `:355`, `:384`; `:421-422` | Byte-slice `starts_with` checks |
| h | Query parsed twice per request | `matcher.rs:472-479 needs_ast()` → `parse_expr`, then `prometheus.rs:201`/`:367` parse again | Return the parsed `Expr`, or add a small parse cache (see BL-04-08) |
| i | `BTreeMap` used where the access pattern is hash | `executor.rs:244`, `:371`, `:460` (`series_map`), `:312` (`out_series`); also `contains_key` then `entry` at `:385-388`, `:478-482` (two descents per point) | `HashMap` + `with_capacity(series_hint)`; keep one `BTreeMap` only where deterministic output ordering is required and sort once at the end; single `entry()` call |
| j | `QueryResult`/`TimeSeries` derive `Clone`, `PartialEq`, `Deserialize` although only consumed once | `models.rs:6-11`, `:22-28`, `:31-43` | Drop unused derives — especially `Clone`, which given BL-02-03 makes accidental deep clones of entire result sets look free |

## BL-02-20 (L) — `query_range` never evaluates the final partial step

**Evidence** — `eval.rs:102` steps with `while ts < end_ns`, so for `start`, `end`, `step` the last evaluated instant is `start + k*step` with `start + k*step < end` — i.e. up to one `step` before `end`. A `query_range` over `[now-900, now]` with `step=60` therefore cannot see samples in the newest 60 s, even though `query` (instant) at the same `end` sees them through the 5-minute lookback.

**Gap.** This matches Prometheus's step semantics, so it is **not** a bug to fix — but it is silent and surprising. Observed while validating the container suites: `sum by (method) (x)` over a 15-minute window returned zero series while the same query returned three a minute later, purely because the newest samples fell past the last step. An integration suite that rebuilds the stack and immediately asserts on a wide range window will fail intermittently for this reason and be blamed on the change under test.

**Resolution.** Document it in `docs/QUERY_LIMITATIONS_REVIEW.md` and in the range-query handler docs, and make the integration scripts step-aware: either use `step <= 15` for windows that end at "now", or end the window at `now - step`. Consider whether the UI's own range queries should extend `end` by one step so the newest interval is visible.

**Acceptance.** Documented limitation; `make test-aggregations` and `make test-functions` pass immediately after `make local-rebuild` without waiting for data to accumulate.

**Effort** S · **Risk** Low

---

## Sequencing within BL-02

1. **Quick, low risk, immediately visible:** BL-02-05, BL-02-06, BL-02-07, BL-02-08, BL-02-09, BL-02-11, BL-02-12, BL-02-14, BL-02-16, BL-02-18, BL-02-19d/i
2. **Label-set spine (one change, many wins):** BL-02-01 → BL-02-02 → BL-02-15
3. **Memory + parallelism:** BL-02-03 → BL-02-04
4. **Response path & long tail:** BL-02-10, BL-02-13, BL-02-17, BL-02-19 remainder

## Verification commands

```bash
# full-range and narrow scan/query baselines
cargo run --release -p parqtel-server --example perf_bench

# label-value autocomplete worst case (high cardinality)
cargo run --release -p parqtel-query --example bench_label_values

# live query latency distribution
python3 scripts/bench_query.py --help
python3 scripts/test_aggregations.sh
```
