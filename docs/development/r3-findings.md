# R3 findings and Phase 2b design

## R3-1 probe

The probe used image `sha256:54f4a7be…`, based on main `2b2ce43` with R1
per-level counters. UAT configuration stayed unchanged at 4Gi / 192MiB. The
read-only probe ran 60 minutes, 07:43–08:45 UTC, with zero restarts. Counter
samples were taken from the service `/metrics` endpoint.

Passive counters over the hour:

| Measure | Result |
|---|---:|
| Blocks | +1,771 (1,804 − 33) |
| L0 blocks | +1,452 (82%) |
| L1 blocks | +319 (18%) |
| L2 / normalized / other | 0 |
| Footer misses | +72 (75 − 3), 4.1% miss rate |

The miss rate is low because only alert-evaluation queries ran. Stream-compactor
logs show L0→L1 passes about every five minutes, merging 2–3 source blocks and
deleting superseded sources. Compaction is keeping up; no L0 backlog appeared.

Controlled query results:

- One `erlang_vm_allocators` 15-minute range query, repeated six times: 18 L0
  and 18 L1 opens, or 3 of each per query.
- Four concurrent 15-minute panels, repeated for four rounds: 28 L0 and 44 L1
  opens; L1 was 61%. There were 8 L0 and 12 L1 misses.

## Memory conclusion

| Run | Peak | Cache | Non-cache baseline |
|---|---:|---:|---:|
| Q7 | 3,786 MiB | 192 MiB | ~3,594 MiB |
| R2 | 4,658 MiB | 971 MiB | ~3,687 MiB |

Non-cache use is about 3.6–3.7 GiB. These baselines are derived by subtracting
the observed cache size from peak RSS, not measured in isolation. At a 4Gi limit,
only about 320–450 MiB remains for cache. A 384–512 MiB cache, the R3-3 target, would put peak near or
above 4Gi with no safety margin. R2 already hit the 3,800 MiB soak safety stop.

Reducing block opens from six to about four per query cuts the working set from
about 280 MB to 190 MB. That remains far above a safe cache size. R3-2 alone
cannot meet the ≤10% miss target at 4Gi.

## Decision

R3-2's L0/L1-skew premise is not a strong enough lever. L1 slightly dominates
under load, but neither L2 windows nor faster L0 compaction closes a roughly
4× working-set gap.

Recommend Phase 2b: a binary, lazy footer index. Store series tables as a
compact binary payload and read data lazily per matched series, instead of
loading a roughly 47 MB JSON-derived table per block. This targets the largest
footer-memory and load-time costs without changing query results.

Phase 2b changes the stored format. Existing blocks must remain readable through
fallback to the current JSON footer. Gate the new format with a migration or
compatibility gate. This document proposes design only; every task needs explicit
user approval before code work.

Phase 3b remains ruled out: step evaluation measured 5.45%. HA H8 stays separate
and approval-gated.

## Phase 2b design and gates

| Task | Design | Required tests and gate |
|---|---|---|
| **D0 — Payload design** | Define a compact binary footer payload with per-series label-ID ranges, a string arena, length prefixes, and a checksum. Keep the JSON field readable by old binaries, or place binary data under a footer key old readers ignore. Measure expected payload bytes against the current 47 MB on the Phase 2 fixture. | Record format, compatibility behavior, and measured size. Approval required before implementation. |
| **D1 — Writer compatibility** | Emit binary plus JSON for one compatibility release (dual-write), or use a single opt-in configuration. | Test that an old reader can read new blocks. Approval required before implementation. |
| **D2 — Reader** | Prefer binary, fall back to JSON, and access series lazily. | Pass `indexed_scan_equals_unindexed_scan`; prove byte-identical query results; record open HWM and approximate bytes on the 283K-series fixture. Approval required before implementation. |
| **D3 — UAT measurement** | Measure footer-miss cost and memory. | If UAT shows at least 70% reduction in index bytes and load time, propose removing JSON after one release. Approval required before implementation. |

This PR documents the design only. Gate: `make docs-check`.
