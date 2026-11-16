# Parqtel memory backlog (from uat-oman analysis, 2026-11-15)

Context: idle RSS is ~340 MiB, but one query can spike the pod to 0.65-3.5 GiB. Cause: blocks cut by
row count at high series counts have huge series tables (local: 82 MiB block, 72 MiB footer, ~1.7 rows
per series). `scanner/block.rs` re-parses a table on every query when it exceeds `FOOTER_CACHE_BUDGET`
(192 MiB), across several blocks at once.

| # | Item | Expected effect | Status |
|---|------|-----------------|--------|
| 1 | Bound concurrent block opens per query; make `FOOTER_CACHE_BUDGET` configurable | Cuts the query peak | backlog |
| 2 | Build the inverted index only for labels a query uses | Less parse memory and time | backlog |
| 3 | Compact row-cut blocks so a series table covers many samples per series | Disk density (55-82 B/row now) and query memory | backlog, untested |
| 4 | Replay OOM: previous pod OOMKilled 5 min after start at 4 GiB. Lower `max_inflight_flushes`, measure replay with big tables | No OOM loop on restart | backlog, undiagnosed |
| 5 | Find the client querying parqtel about once a minute | Explains periodic spikes | backlog |
| 6 | Replay interning for logs and traces (only metrics done) | Smaller replay heap | backlog |

| 7 | Ingest CPU: parqtel saturates 1 core at ~11.7K pts/s on the mirror stream (93% of CFS periods throttled). Profile of a 1-CPU copy: label-interner lookup ~14% (`LabelSet::get` binary search + hashing), series-table JSON parse ~28% (`serde_json` u32 arrays), series-table build at flush ~4%. Cache parsed series tables or stop re-parsing them; cheaper interner lookup (hash once, compare by pointer/fingerprint) | More points per core | backlog |
| 8 | Liveness: `/health` timed out under CPU starvation and the probe killed the pod (09:14Z). Serve `/health` off the ingest runtime, or widen probe timeout; do not let a busy pod be restarted into a bigger replay | No restart loop under load | backlog |
| 9 | Flush stalls ingest: ingest rate drops to ~1.5K pts/s while a 1M-row block encodes (~15 s each at 1 core) | Smooth ingest | backlog |
| 10 | Sizing: uat-oman workers cannot place a 4 Gi / 1 CPU request; pick limits from measured load | Schedulable pod | open |
| 11 | Storage: `parqtel.series_index` footer is uncompressed JSON. uat-oman block: 43.2 MB of 51.4 MB (84%) for 1M rows / 283,566 series; zstd-3 shrinks it to 1.7 MB, ~2.3 MB after base64 footer encoding | ~5x smaller blocks (51.4 -> ~9.9 B/row) | implemented; UAT verification pending |
| 12 | Compaction never runs: summaries with NaN quantiles serialize as `null` in `value_complex`, then `serde_json` fails on read (`invalid type: null, expected f64`). Same error breaks queries on those metrics (`certmanager_http_acme_client_request_duration_seconds`) | Compaction amortizes the series table over 6h/24h blocks; summary queries work | implemented; UAT verification pending |
| 13 | Metrics blocks store labels twice: `labels` column (5.4 B/row, 71% of column bytes) plus the series table | Up to ~5 B/row more | implemented; UAT verification pending |

Done and committed: segment-wise replay, no duplicate replay, replay label sharing,
`ingest.label_interner_capacity`, jemalloc, the process-wide 4-slot scan budget
(item 1), and the 5-minute L1 block rollup (item 3).
