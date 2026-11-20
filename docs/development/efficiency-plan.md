# Parqtel efficiency plan: query speed, memory, CPU and storage

Date: 2026-11-15. Scope: metrics, logs and traces. Inputs: uat-oman measurements,
the code paths cited below, and [the index research note](../research/parqtel-query-index-research.md).

## Goal and budget

One pod, 2 CPU / 4 GiB, ~9.4K OTel points/s (~26.6K Prometheus samples/s
equivalent), 7-day retention. Targets:

| Dimension | Target | Today (uat-oman) |
|---|---|---|
| Disk, metrics | <= Prometheus per sample (~1.6 B/sample), so <= ~26 GB / 7 d | 4.8 B/row new blocks (~27.5 GB / 7 d), 48.7 B/row old blocks |
| Memory | Query peaks bounded by a budget; no OOMKill at 4 GiB | Two OOMKills; 2.1 -> 4.06 GiB in 10 s |
| Query latency | 5-15 min UI panel p95 < 1 s | 5-7.6 s per query |
| CPU | Ingest + queries within 2 cores, no sustained throttling | 1.6-1.8 cores, up to 37% throttled |

## The one root cause behind most of it: block shape

A metric block is cut at 1M rows. At ~283K active series that is ~2 minutes of
data and ~3.5 rows per series. Every per-block cost scales with series count,
not with rows:

- the series index (43 MB raw JSON, ~100 MiB parsed) is rebuilt per block;
- a 15-minute query touches ~8 blocks, so it parses ~8 indexes;
- storage pays the index once per 3.5 samples instead of once per hundreds;
- compaction cannot fix it: 1M-row blocks are excluded (`row_count < 500_000`),
  and merging decodes every row into memory.

Prometheus avoids this by keeping each series in one 2-hour block, with an
mmapped index read lazily. Parqtel must get the same property: **few series
indexes per query, each read lazily, each covering many samples per series.**

## Pre-Phase 0 findings per signal

### Metrics

| Finding | Effect | Evidence |
|---|---|---|
| Blocks cut by row count, ~2 min each | Index cost x block count on disk, CPU and memory | `max_rows_per_block` 1M; UAT block 283,589 series / 1,000,300 rows |
| `SeriesIndex::from_table` builds every posting and interns every label set on load | ~100 MiB per block, rebuilt on each cache miss; two do not fit the 192 MiB cache | `series_index.rs` `from_table` |
| Per-call semaphore (16) and up to 128 blocks per call | Concurrency was unbounded across UI panels and alert rules | `scanner/metrics.rs` `scan_filtered` |
| Points accumulate before `max_series` / `max_samples_per_series` | Limits do not bound scan memory | `executor/metrics.rs`, `executor/series.rs` |
| Alert rules run serially every 15 s through the same executor | Background query load competes with UI | `tasks.rs` `spawn_alert_eval_task` |
| Interner (2M sets) retains labels after index eviction | Memory not released with cache | `labels.rs` `LabelInterner` |
| Compaction excludes >=500K-row blocks and decodes whole groups | Cannot densify; unsafe to force | `compactor.rs` |

### Logs (already the healthiest path)

| Finding | Effect |
|---|---|
| Per-granule skip index, late materialisation, exact count path | Good: rows decoded only when needed |
| Result trimmed to `keep` after each block | Memory bounded by limit |
| `all_logs.sort_by_key` after every block | O(blocks x keep log keep) CPU; a heap or merge is cheaper |
| Same per-call semaphore as metrics | Same cross-request concurrency gap |
| Attribute/resource JSON parsed per matched row (cache per batch) | CPU on wide results |

### Traces (weakest path)

| Finding | Effect |
|---|---|
| **`trace_id` filter runs after a 200-span scan cap** | **Correctness bug**: a trace lookup can return incomplete or no spans |
| No projection: every column decoded, `row_to_span` parses attributes, events and links JSON per row | High CPU and memory per span |
| No footer cache, no bloom on `trace_id`/`service_name`, no trace-level index | Every lookup reads every overlapping block |
| Tiered compaction skips traces (`continue`) | Trace blocks never merge past the small-block pass |
| Tasks spawned for all blocks; early `break` on limit does not cancel remaining tasks | Wasted CPU after the limit is met |

## Plan

Ordered by risk removed per unit of work. Each phase has an exit check; do not
start the next phase until it passes on uat-oman.

### Phase 0: stop the bleeding (small, low risk)

1. **Process-wide scan admission.** One shared semaphore for block scans and
   cold index builds across all queries and signals. Start at 4 concurrent
   blocks for the 4 GiB pod. Replaces the per-call semaphores.
2. **Single-flight index loads.** Concurrent misses on the same block wait for
   one build instead of building duplicates.
3. **Trace-ID correctness.** Compare binary trace IDs before materializing spans
   or applying the result cap. Parquet still decodes the batch's columns; column
   projection remains Phase 3 work.
4. **Phase metrics.** Export cumulative admission, cache, index-load, row, and
   result counters at `/metrics`. Add per-query debug events for block counts,
   scan wait, selected series, retained points, and grouping time.
5. **Alert scheduling.** Alerts try to reserve one scan slot without waiting.
   Skip a rule evaluation attempt when no slot is free, so alerts do not queue
   ahead of UI queries. This can delay alert evaluation under sustained query
   saturation; monitor `parqtel_scan_low_priority_skips_total`.

Exit (operator decision, 2026-11-19): no OOMKill or container restart under the
full stream with 4 concurrent UI panels plus alerts for 6 continuous hours. Track
memory for the same 6-hour window; peak working set must stay below the existing
4 GiB limit. Trace-ID filtering must pass a regression test and honor the requested
result limit. Query results can still retain many points before Phase 3 limits.

### Phase 0 rollout record — uat-oman

- Workspace tests, lint, image build, and container smoke test passed.
- Pushed `latest` as `sha256:76fcafd6c18eb7900efe8fec0d6f15b5540bb7dfed6aea76c04a516b1fb5d594`.
  StatefulSet rollout completed; pod is Ready with zero restarts on that digest.
- The 5-minute high-cardinality query completed in 9.6 s and returned 1,000
  series / 10,000 samples. The 15-minute query completed in 20.4 s and returned
  1,000 series / 30,000 samples. Query latency still needs Phase 1 and 2 work.
- At the last sample, pod memory was 1,997 MiB, scan slots were 4/4, the footer
  cache held about 101 MiB, and estimated index bytes loaded totaled 11.8 GB.
- There were 20 enabled alert rules and 152 skipped rule evaluations in about
  four minutes. The operator accepted this temporary trade-off; keep monitoring
  skipped evaluations and alert delay.
- OTel continued at about 9.6K metric points/s; queue was 2/2,000 with zero
  export failures. Query and ingest error counters were zero.
- `parqtel_scan_low_priority_skips_total` reached 152 during short validation.
  Alert rules skipped work while all scan slots were busy. This needs operator
  review before treating alert evaluation as stable.
- UAT has no stored logs or traces, so trace-ID behavior was validated by the
  regression test, not a live trace query.

At this rollout, the original 24-hour stability gate had not passed. The operator
revised the remaining Phase 0 soak to 6 hours on 2026-11-19; this rollout record is
historical.

Final six-hour exit record: the operator-approved 6-hour revision passed on image
`sha256:14c29d2a…`. The four-panel soak recorded 361 one-minute samples and 1,444
concurrent panel queries with zero failures. Peak RSS was 2,736 MiB, with zero
restarts. Metrics stayed near 9.1–9.3K points/s with a maximum 1-second gap and
zero ingest errors. All 73 tagged log batches and 73 tagged span batches were
accepted. Low-priority alerts skipped 44 evaluations over six hours. Phase 0 gate
passed under the revised soak duration.

### Phase 1a: canonical order and 5-minute L1 rollups

Phase 0 showed index churn, not a missing index. A 15-minute query still loaded
11.8 GB of estimated index bytes during short validation. It returned 1,000
series in 20.4 s, so Phase 1 targets block fan-out.

Immediately before Phase 1 rollout, UAT held 301 metric blocks, 298,998,458
rows, and 7,196,925,784 bytes. The pod used 1,468m CPU and 2,879 MiB memory;
ingest averaged 9,470 metric points/s with 3 of 4 scan slots occupied.

One source-layout correction is required first: old flushes sorted by raw point
labels, but their footer indexes record `service.name` after resource-label
merging. Those orders can differ. A regression test caught this; a naive k-way
merge by indexed-label fingerprint would misorder runs.

1. **Canonical L0 order.** New flushes sort by final query-visible label
   fingerprint. Mark new L0 files `m_l0_` so compaction can distinguish them.
2. **Normalize legacy L0 blocks.** Rewrite at most one block per pass, with a
   500K-row cap. Leave larger blocks untouched until normalization streams.
3. **Streaming L0-to-L1 merge.** Merge up to 8 ordered blocks into one 5-minute
   L1 block. Coalesce each series across sources. Read one Arrow batch at a time
   and write row groups at configured size.
4. **Crash recovery.** Store source filenames in output footers. Reconcile ignores
   superseded blocks after restart. Keep sources for 5 minutes so active queries
   can finish, then reclaim them.
   Previous binaries can read L1 blocks but do not understand provenance. Roll
   back only after superseded files are removed; an earlier rollback can
   re-adopt those files and duplicate query results.
5. **Bound work.** Run metrics compaction every 5 minutes by default. Limit each
   pass to one legacy normalization, 16M total rows, 2 L1 merges and 2 reserved
   scan slots. Defer compaction when scan budget is busy.
6. `parqtel compact` runs repeated bounded L1 passes. Stop service first; command
   is offline maintenance.

Expected: closed 5-minute queries touch one L1 block, plus unclosed tail blocks.
A 15-minute query touches up to three L1 blocks plus its current tail. At UAT
rate, each L1 has about 2.85M rows and 283K series, or ~10 rows per series.
That yields roughly 3x fewer index entries per sample than L0. Measure
bytes/row, query latency, RSS, CPU, and block fan-out before claiming a win.

Exit: tests prove indexed/unindexed equivalence, row counts, WAL provenance and
crash reconciliation. On UAT, verify block count falls, query results match,
RSS remains bounded, and ingest stays healthy during rollups.

### Phase 1 rollout — uat-oman (validation incomplete)

- Built and smoke-tested locally; workspace tests and lint passed. Pushed
  `latest` as `sha256:fc8274d2ea5e093ed83a94603c1967b43e7b29ddef89a23281fe469497277803`.
  StatefulSet rollout completed; pod is Ready with zero restarts on that digest.
- After rollout, ingest averaged about 9.5K metric points/s, gap stayed at 0–1 s,
  Parqtel ingest errors stayed at zero, and OTel exporter queue drained to 0/2,000.
  Collector logged one transient unavailable error during restart and retried it.
- Observed CPU ranged from 1.47 to 1.91 cores; RSS ranged from 1,971 to
  2,166 MiB. Before rollout, UAT was already at 1.47 cores and 2,879 MiB.
- Initial compaction passes deferred at 3 of 4 scan slots. Later passes
  normalized legacy blocks, but L1 groups repeatedly skipped mixed WAL ranges.
  The operator chose to keep 2-slot admission near the 2-core CPU limit.
- Post-rollout storage reached 326 metric blocks, 323,123,097 rows, and
  7,313,884,080 bytes while ingestion continued. These counts include new data;
  L1 compaction and query latency remain unvalidated.
- The initial image was OOMKilled at 18:10:48. cAdvisor reported working-set
  memory of 4,262,019,072 bytes and usage of 4,293,988,352 bytes, near the 4 GiB
  limit. Startup replayed 99,377 WAL records and reconciled one output block
  against one source block.
- A mixed-WAL preflight fix shipped as `sha256:e8e2c474407db1f35905473faebef9348381e490738fa5f40cb23943ecb96428`.
  The container OOMKilled twice more, at 18:57:48 and 19:16:21, after repeated
  1M-row legacy normalizations. WAL preflight avoided rejected-plan allocation,
  but did not bound normalization memory.
- The operator approved limiting normalization to one block of at most 500K rows
  per pass. Larger legacy blocks stay untouched. The capped image was pushed as
  `sha256:074b2f8c276cba8bc192ca66649af5ef160b50d8a63ea0c7c90f0d1c2afcf292`;
  pod was Ready with zero restarts at the last check.
- After the capped rollout, ingest averaged about 9.5K points/s, gap stayed at
  0–1 s, ingest errors stayed at zero, and OTel queue was 0/2,000. CPU was about
  1.6–1.8 cores and RSS about 2.4–2.6 GiB. No further OOM occurred during
  observed validation. Two L1 merges each coalesced 4 source blocks and about
  4.0M rows. Mixed-WAL groups still skip before source planning.
- `erlang_vm_allocators` range queries at 30 s step returned 1,000 series and
  10,000 samples in 4.72 s for 5 minutes, and 1,000 series and 30,000 samples
  in 16.20 s for 15 minutes. Phase 0 query name/step was not recorded, so these
  timings are indicative, not a strict A/B comparison. Query fan-out still needs
  measurement during a lower-load window.
- Admission warned that the pod lacks RuntimeDefault seccomp and has no
  AppArmor/SELinux profile. This pre-existing pod security configuration was not
  changed.
- The metrics PVC held 7.6 of 10.5 GB. At ~4.8 B/row, 7-day retention needs
  ~27 GB, so `data-odin-parqtel-0` was expanded from 10Gi to 30Gi (the volume
  reports 31.6 GB capacity). Expansion cannot be reverted. The StatefulSet
  `volumeClaimTemplates` and Helm values still say 10Gi.
- To cut background scan load, alert evaluation moved from every 15 s to every
  60 s (`PARQTEL__SERVER__ALERT_INTERVAL_SECS=60`). All 20 preset rules stay
  enabled. This env var was set with `kubectl` and is not in the Helm release;
  a Helm upgrade would revert it.
- Mixed or gapped WAL ranges no longer block L1 forever. If every source range
  is at or below the metrics WAL commit point, the merge proceeds and the output
  carries no WAL span: those records can never be replayed, so no span is
  needed. Ranges above the commit point are still rejected. Not yet deployed;
  held until the 24-hour soak of the current image finishes.

### Phase 1b: L1-to-L2 rollups — deferred

A 2-hour block at UAT rate holds about 68M rows. One merge can monopolize the
2-core pod for several minutes. Defer L2 until L1 CPU, bytes, memory, and query
results show a safe budget. Do not force current full-materialization compactor
on these blocks.

### Phase 2: lazy, compact series index (memory and cold-query CPU)

Phase 2a keeps the existing footer format and makes the in-memory query index
lazy. Opening retains the parsed table and small metric/name directories; label
sets materialize on demand, while selectors scan compact label IDs and build
query-local candidate lists. The legacy tiered compactor reads its `SeriesTable`
directly instead of building a query index.

Phase 2b would introduce a binary, sectioned footer read lazily from disk. It
changes the stored format and needs separate approval; do not start it unless
Phase 2a misses its measured exit target.

#### Phase 2a measurement — T0 baseline vs T4

Same fixture (283,000 series, 3,400 metrics, 14 labels, 4 rows per series) and
release build; each step ran in three fresh processes. Values are medians; memory
is MiB and time is ms. T0 baseline uses product source `63a9cf1`; T4 uses `5c9f523`.
These T5 measurements use a local synthetic fixture and do not constitute UAT
validation or rollout.

| Step | Baseline HWM | Baseline RSS | Baseline ms | T4 HWM | T4 RSS | T4 ms |
|---|---:|---:|---:|---:|---:|---:|
| `parse` | 106.8 | 71.9 | 141.7 | 106.7 | 71.8 | 139.8 |
| `open` | 626.9 | 605.9 | 774.2 | 106.7 | 76.6 | 188.8 |
| `select` | 626.9 | 605.8 | 786.9 | 106.7 | 76.7 | 182.5 |
| `select-broad` | 626.7 | 605.7 | 782.5 | 106.7 | 76.8 | 196.2 |

- Baseline open growth: 626.9 - 106.8 = 520.1 MiB.
- T4 open growth: 106.7 - 106.7 = 0.0 MiB; 25% of baseline growth = 130.025 MiB.
- T4 select growth: 106.7 - 106.7 = 0.0 MiB.
- T4 `select-broad` time: 196.2 / 782.5 = 25.1% of baseline (limit: 120%, or 939.0 ms).
- **2a passes:** open growth is within 25% of baseline, select growth is below
  10 MiB, and broad-select time is within 120% of baseline.
- **2b not started:** its format change still needs approval.

### Phase 3: streaming query execution (memory bounded by result, not scan)

1. Scan batches straight into per-series accumulators; apply `max_series` /
   `max_samples_per_series` while scanning, keep counts and volume summary
   exact.
2. Push simple range functions (`rate`, `increase`, `*_over_time`) and
   `sum by` into the per-series accumulator so raw points are not retained.
3. Logs: replace per-block full sort with a bounded heap. Traces: project only
   the columns a query reads; parse attributes/events/links lazily for returned
   spans.

Exit: memory per query scales with returned series, not scanned rows.

Execution: [phase3-execution-plan.md](phase3-execution-plan.md). The Phase 0 soak
showed footer-cache churn (33% misses, ~0.93 s each) as the larger latency cost, so
the plan adds a configurable cache budget and a compact in-memory table before the
series-limited scan, and defers range-function pushdown to Phase 3b.

#### Phase 3 Q6 measurements — synthetic fixture

Q1 baseline and post-Q5 values are three-run medians from fresh release-build
processes. Deltas compare post-Q5 with baseline; negative values mean lower cost.
The two-panels post-Q5 latency and HWM were not recorded. Q2 cache-budget results
are listed separately and are not represented as post-Q5 query timings.

| Query / measure | Q1 baseline | Post-Q5 / Q2 | Delta |
|---|---:|---:|---:|
| `target` latency | 201 ms | 176 ms | -25 ms (-12.4%) |
| `target` HWM | 260.9 MiB | 173.2 MiB | -87.7 MiB (-33.6%) |
| `target` rows considered | 1,240,000 | 1,240,000 | unchanged |
| `target` points returned | 30,000 | 30,000 | unchanged |
| `selective` latency | 131 ms | 132 ms | +1 ms (+0.8%) |
| `selective` HWM | 229.8 MiB | 173.3 MiB | -56.5 MiB (-24.6%) |
| `selective` rows considered | 12,400 | 12,400 | unchanged |
| `selective` points returned | 12,000 | 12,000 | unchanged |
| `two-panels` latency / HWM | 224 ms / 328.1 MiB | not recorded | not available |
| `two-panels` footer misses | 4 | 4 at both 192 MiB and 512 MiB budgets | no reduction |

The `target` result remains 30,000 points: `max_series` 1,000 × 30 rows per
series. Its 1,240,000 scanned rows did not change. HWM fell 33.6%, and latency
improved 12.4%; neither reaches the 50% target.

Q3's separate series-index `open` benchmark changed from 106.7 to 81.9 MiB HWM
(-24.8 MiB, -23.2%) and from 83,895,996 to 56,481,268 `approx_bytes`
(-27,414,728 bytes, -32.7%).

| Q6 exit check | Target | Result | Status |
|---|---|---|---|
| `target` points | ≤33,000 (1.1 × 1,000 × 30) | 30,000 | Pass |
| `target` latency | ≤100.5 ms (50% of 201 ms) | 176 ms; 12.4% lower than baseline | Fail |
| `target` HWM | ≤130.45 MiB (50% of 260.9 MiB) | 173.2 MiB; 33.6% lower than baseline | Fail |
| `two-panels` footer misses | ≤10% of block opens | 4 misses / 4 blocks (100%, cold cache) at both budgets | Fail; no reduction |
| Equivalence tests | All pass | Q4: 864 workspace tests passed, including `indexed_scan_equals_unindexed_scan`; Q5: 21 scanner tests passed, including the same equivalence test | Pass |

**Phase 3 exit did not pass.** The point-count bound and equivalence gates passed,
but latency and HWM missed their targets, and the synthetic fixture's cold-cache
miss rate did not improve with a larger budget. The four misses are the four
fixture blocks opened by a fresh process; the cache has no prior warm entries.

**Follow-ups:** Q3's compact table reduced measured open memory. Q2's four footer
misses persisted at both 192 MiB and 512 MiB, but its four-block fixture could not
reproduce UAT cache churn; that result is not evidence against a cache-size fix.
Validate miss rate on UAT before claiming a production benefit.
The query missed its latency target, but no step-evaluation breakdown was recorded.
Profile `target` first; propose Phase 3b range-function pushdown for approval if
step evaluation is the dominant cost.

#### Phase 3 Q7 UAT result — six-hour soak

Image `ofticr.azurecr.io/infra/maria/parqtel:q7-ae9112e` (`sha256:957a5d7f59756a54e933b8ee043dff92bb35bdaaeb255b713664c3b07d7dc189`)
completed rollout on uat-oman; rollback target was the old digest
`sha256:14c29d2a…`. UAT overrides remained: `ALERT_INTERVAL_SECS=60`,
`MAX_INFLIGHT_FLUSHES=1`, `LABEL_INTERNER_CAPACITY=2097152`,
startupProbe `failureThreshold=120`, and PVC `30Gi`. The dedicated HTTP runtime
from PR #85 stayed healthy: every sample returned `health=200`, with zero restarts.

The six-hour, four-panel soak recorded 361 one-minute samples and 1,444 panel
queries with zero failures. It accepted 73 log batches and 73 trace batches; peak
memory was 3,786 MiB, with zero restarts, zero ingest errors, and a maximum ingest
gap of 0s. Metrics ingestion stayed near 9.4K points/s; 16 low-priority scans were
skipped.

| Panel (hours 4–6) | p50 ms | p95 ms |
|---|---:|---:|
| `allocators-15m` | 3,706 | 17,315 |
| `allocators-5m` | 3,426 | 10,265 |
| `up-5m` | 3,137 | 9,461 |
| `running-pods-15m` | 3,533 | 16,611 |

Phase 0 `allocators-15m` baseline for hours 4–6 was approximately 11.2–11.8s
p50 and 13.4–14.3s p95.

| Final UAT measure | Result |
|---|---:|
| Blocks scanned | 26,371 |
| Footer cache hits / misses | 16,862 / 9,293 (35.5% of opens missed) |
| Footer cache bytes | 102,438,874 |
| Footer load time | 12,985s |
| Scan admission wait | 8,684.5s |
| Metric storage | 313 blocks; 872,327,128 rows; 2,425,619,718 bytes |

| Q7 gate | Target | Result | Status |
|---|---|---|---|
| Panel p50 | ≤4s | All four panels: 3.1–3.7s | Pass |
| Panel p95 | ≤7s | All four panels: 9.461–17.315s | Fail |
| Peak memory | <4 GiB | 3,786 MiB | Pass |
| Restarts / ingest errors | 0 / 0 | 0 / 0 | Pass |
| Footer misses | ≤10% | 35.5% | Fail |

**Q7 failed** on p95 latency and footer misses. The compact table and series-limited
scan cut p50 substantially, but footer-cache churn still dominates tail latency.
Recommend Phase 2b (binary lazy footer) for approval; do not start it without
approval. Phase 3b remains ruled out by N2: step evaluation was 5.45%.

#### Phase 3b decision

Each query ran in three fresh release processes against `/tmp/opencode/phase3-q1-fixture`.
No CPU sampler (`perf` or equivalent) was installed, so phase timings use existing
DEBUG events. Step-evaluation time is `total_ms - scan_ms - grouping_ms` per run;
shares are calculated per run before taking medians. Event timings are integer
milliseconds.

| Query | Total ms (median) | Scan ms (median) | Grouping ms (median) | Step-eval ms (median) | Step-eval share (median) |
|---|---:|---:|---:|---:|---:|
| `rate-target` | 189 | 179 | 1 | 6 | 3.51% |
| `sum-rate` | 257 | 242 | 1 | 14 | 5.45% |
| `over-time` | 264 | 252 | 1 | 9 | 3.41% |

Decision rule: propose Phase 3b only if step evaluation is at least 40% of
`sum by (pod) (rate(...))` time. **No proposal:** `sum-rate` step evaluation is
5.45% of total time, below the 40% threshold.

#### Revised diagnosis

Q7 observed 16.1 blocks per query and 35.5% footer misses. At about 46.7 MB of
index bytes per miss, that implies a roughly 750 MB working set against the
192 MiB cache. Phase 3 returned only 3.4% of rows read. Q2's 192-vs-512 MiB
synthetic test used only four blocks, so it could not reproduce UAT churn; its
"no difference" result is not evidence against a cache-size fix.

### Phase 4: traces and logs storage

1. **Traces**: sort blocks by (trace_id) or (service, start_time); add bloom
   filters on `trace_id` and `service_name`; use the footer cache; enable tiered
   compaction for traces with the streaming merger.
2. **Logs**: keep the granule skip index; extend it to service/severity
   statistics per granule; compact with the streaming merger.
3. **Self-describing per-signal sizing**: per-signal `max_rows_per_block`,
   `row_group_size`, and zstd level, chosen from measured bytes/row and query
   latency on UAT rather than one default.

## Considered: one file per metric name per 2 hours (append/rewrite)

Measured on a UAT block (127 s, 1,000,300 rows, 283,589 series):

| Fact | Value |
|---|---|
| Distinct metric names | 3,392 |
| Rows per name per 2 h | median 1,361; p90 31,635; p99 191,112; max 7.3M (`erlang_vm_allocators`, 40,176 series) |
| Names with < 10K rows per 2 h | 2,757 (81%) |
| Share of rows in top 100 / top 1,000 names | 55.8% / 95.3% |

Verdict: the *idea* (a query for one metric reads only that metric's data in
one 2-hour unit) is right; *separate files with append/rewrite* is the wrong
way to get it.

- **Parquet files are immutable.** "Append" means rewriting, or truncating the
  footer and writing new row groups. Rewriting the whole file on every ~2 min
  flush gives ~30x write amplification over 2 h (sum of 1/60..60/60). At
  ~6 s CPU per 1M rows to encode (UAT flush timings) that is ~68M rows x 30 x
  6 s / 7,200 s = **~1.7 cores**, against a 2-core pod.
- **Footer-append without rewrite** avoids the CPU, but leaves one row group per
  flush: median metric gets ~23 rows per row group, which compresses poorly and
  bloats footers; it still needs a final rewrite and is not crash-safe without
  extra WAL work. arrow-rs has no support for it.
- **File count.** 3,392 names x 12 blocks/day x 7 days = ~285K files, most a few
  KB where the Parquet footer and schema dominate. Opening many tiny files costs
  more than one selective read of a big file.
- **Disk throughput is not the problem.** New blocks run ~4.8 B/row x 9.4K rows/s
  = ~45 KB/s. Even 3x write amplification is ~135 KB/s. The cost is CPU
  re-encoding and memory, not IOPS.

What gets the same query benefit cheaply:

1. **Leveled, time-bounded compaction instead of per-metric files.** Keep small
   L0 blocks (durable, as today), merge them by streaming into 5-minute L1
   blocks. Defer L2 (2 h) until L1 CPU and memory are measured. Each row stays
   stream-bounded and is not held with a full level in memory.
2. **Sort each L1 block by (metric, series, time)**, so one
   metric is one contiguous row range. Add a small **metric directory** to the
   index (3,392 entries: name -> row range and postings offset).
3. **Lazy index (Phase 2)**: a query for one metric reads the directory, that
   metric's postings and its row range. For the median metric that is a few
   pages of one file, which is the "few identified blocks" outcome, without
   per-metric files.
4. Optional: shard future L2 blocks into K files by metric-name hash (e.g.
   K = 16) if one file grows too large to merge or cache comfortably.

## What not to do

- Do not add a second, global inverted index over all blocks. It duplicates the
  per-block index and grows with total series history. Fix block shape and lazy
  loading first.
- Do not force the current compactor on 1M-row blocks; it decodes every row.
- Do not raise the 4 GiB limit to hide the problem; bound concurrency instead.

## Validation method (all phases)

- UAT A/B against the same collector stream; record bytes/sample per signal,
  RSS peak and p95 query latency for 5-, 15- and 60-minute panels, cold and warm.
- Keep the equivalence tests (indexed vs unindexed scans, log prefilter vs row
  filter) as correctness gates, and add a trace-ID completeness test.
- Compare against Prometheus on the same targets using samples, not OTel points.

## Open questions

- Exact allocator split of the OOM peak: needs a heap profile (jemalloc
  `prof`) under the UI workload.
- Whether OTel histogram points should be stored as one row (current) or
  per-bucket columns for cheaper `histogram_quantile`.
- Whether the in-memory head should index series (Prometheus `head`) so recent
  queries skip block scans entirely.
