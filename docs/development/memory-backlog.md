# Parqtel memory backlog (from uat-oman analysis, 2026-11-15)

Context: idle RSS is ~340 MiB, but one query can spike the pod to 0.65-3.5 GiB. Cause: blocks cut by
row count at high series counts have huge series tables (local: 82 MiB block, 72 MiB footer, ~1.7 rows
per series). `scanner/block.rs` re-parses a table on every query when it exceeds `FOOTER_CACHE_BUDGET`
(192 MiB), across several blocks at once.

| # | Item | Expected effect | Status |
|---|------|-----------------|--------|
| 1 | Bound concurrent block opens per query; make `FOOTER_CACHE_BUDGET` configurable | Cuts the query peak | done; configurable in Phase 3, PR #82 (`16c068a` on main) |
| 2 | Build the inverted index only for labels a query uses | Less parse memory and time | done; lazy series index, Phase 2a, PR #80 |
| 3 | Compact row-cut blocks so a series table covers many samples per series | Disk density (55-82 B/row now) and query memory | implemented; 5-minute L1 rollup, UAT verification pending |
| 4 | Replay OOM: previous pod OOMKilled 5 min after start at 4 GiB. Lower `max_inflight_flushes`, measure replay with big tables | No OOM loop on restart | backlog, undiagnosed |
| 5 | Find the client querying parqtel about once a minute | Explains periodic spikes | backlog |
| 6 | Replay interning for logs and traces (only metrics done) | Smaller replay heap | backlog |

| 7 | Ingest CPU: 2026-11-20 release profile pinned to one core under synthetic multi-label OTLP JSON load (10,000 series, 8 datapoint labels): 11,626 points/s/core over 10s (53 samples). Top self-CPU: `<serde_json::value::Value as serde_core::de::Deserialize>::deserialize::<&mut serde_json::de::Deserializer<serde_json::read::SliceRead>>` 15.09%; `parqtel_ingest::decode::common::json_attrs_to_labels` 15.09%; `core::ptr::drop_glue::<serde_json::value::Value>` 11.32%; `<alloc::collections::btree::map::BTreeMap<alloc::string::String, serde_json::value::Value>>::insert` 9.43%; `sz_index2size_lookup_impl` 5.66%. Conclusion: no small clear fix; keep backlog. | More points per core | backlog |
| 8 | Liveness: `/health` timed out under CPU starvation and the probe killed the pod (09:14Z). Serve `/health` off the ingest runtime, or widen probe timeout; do not let a busy pod be restarted into a bigger replay | No restart loop under load | implemented; dedicated HTTP runtime, PR #85, UAT verification pending |
| 9 | Flush stalls ingest: ingest rate drops to ~1.5K pts/s while a 1M-row block encodes (~15 s each at 1 core) | Smooth ingest | reproduced locally at lower magnitude (2026-11-20; 1 core; 590K synthetic series): 77M points/300 s (~257K/s); 300 rate polls returned 227 distinct wall seconds, with current rate 0–570K/s and up to 4.53 s reply gaps. Wrote 77 × 1M-row blocks (23,278,438–23,366,683 bytes; encode 1.23–4.37 s, median 3.97 s). Pprof during flush: LabelInterner::intern 7.75%, build_metric_batch 7.04%, process_metrics 6.34%, JSON deserialize 5.63%. No small clear fix; keep backlog. | backlog |
| 10 | Sizing: uat-oman workers cannot place a 4 Gi / 1 CPU request; pick limits from measured load | Schedulable pod | open |
| 11 | Storage: `parqtel.series_index` footer is uncompressed JSON. uat-oman block: 43.2 MB of 51.4 MB (84%) for 1M rows / 283,566 series; zstd-3 shrinks it to 1.7 MB, ~2.3 MB after base64 footer encoding | ~5x smaller blocks (51.4 -> ~9.9 B/row) | implemented; UAT verification pending |
| 12 | Compaction never runs: summaries with NaN quantiles serialize as `null` in `value_complex`, then `serde_json` fails on read (`invalid type: null, expected f64`). Same error breaks queries on those metrics (`certmanager_http_acme_client_request_duration_seconds`) | Compaction amortizes the series table over 6h/24h blocks; summary queries work | implemented; UAT verification pending |
| 13 | Metrics blocks store labels twice: `labels` column (5.4 B/row, 71% of column bytes) plus the series table | Up to ~5 B/row more | implemented; UAT verification pending |

Phase 0 six-hour soak passed on the Phase 2a image; see [efficiency-plan.md](efficiency-plan.md).

Done and committed: segment-wise replay, no duplicate replay, replay label sharing,
`ingest.label_interner_capacity`, jemalloc, and the process-wide 4-slot scan budget
(item 1). The 5-minute L1 block rollup (item 3) is implemented; UAT verification
remains pending.
