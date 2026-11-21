# R2b UAT result — six-hour 1 GiB cache soak

## Configuration and run

The UAT image used digest `sha256:54f4a7be…`, based on main `2b2ce43` with
per-level scan counters. The pod memory limit was `6Gi`; the request stayed at
`3Gi`. The footer cache was set with
`PARQTEL__QUERY__FOOTER_CACHE_BYTES=1073741824`. All other UAT overrides stayed
unchanged. The run had zero restarts.

R2b differed from the earlier R2 attempt only by raising the soak script's safety
guard from 3,800 MiB to 5,632 MiB. That allowed the full six hours to run. The
earlier run stopped at the script guard, not because of an OOM or pod-limit
breach.

The soak recorded 361 one-minute samples and 1,444 panel queries, with zero
failures. It accepted 73 log batches and 73 trace batches. Peak memory was
5,237 MiB, below both the 6Gi pod limit and the 5,632 MiB guard. There were zero
restarts and zero ingest errors; maximum ingest gap was 1s. Ingest stayed near
9.4K points/s. One low-priority scan was skipped.

## Results

| Panel (hours 4–6) | p50 ms | p95 ms |
|---|---:|---:|
| `allocators-15m` | 1,182 | 2,448 |
| `allocators-5m` | 1,077 | 2,330 |
| `up-5m` | 1,035 | 2,131 |
| `running-pods-15m` | 1,047 | 2,337 |

| Final scan counter | Result |
|---|---:|
| Blocks | 22,042 |
| Footer hits / misses | 18,584 / 331 (1.8% misses) |
| Footer cache bytes / budget | 1,023,745,964 / 1,073,741,824 |
| Footer load | 309.8 s |
| Admission wait | 180.4 s |

| Storage | Result |
|---|---:|
| Blocks | 542 |
| Rows | 1,519,378,537 |
| Bytes | 4,202,680,057 |

| Gate | Target | Result | Status |
|---|---|---:|---|
| Panel p50 | ≤4s | All four panels: 1.035–1.182s | PASS |
| Panel p95 | ≤7s | All four panels: 2.131–2.448s | PASS |
| Footer misses | ≤10% | 1.8% | PASS |
| Peak memory | <6Gi | 5,237 MiB | PASS |
| Restarts / ingest errors | 0 / 0 | 0 / 0 | PASS |

**Overall: PASS.**

## Comparison and decision

Q7 used a 192 MiB cache and a 4Gi pod limit. Its `allocators-15m` result was
p50 3,706 ms and p95 17,315 ms, with 35.5% footer misses, 12,988 s of footer
load, and 8,684 s of admission wait. R2b's 1 GiB cache reduced the miss rate to
1.8%, footer load to 309.8 s, and admission wait to 180.4 s. The footer-cache
working set was the dominant cause of the earlier query tail latency. A 1 GiB
cache at a 6Gi pod limit resolves it; keep this configuration on UAT.

Peak memory was 5,237 MiB, leaving about 800 MiB of headroom under the 6Gi pod
limit. Nodes have about 20 GB allocatable.

Phase 2b code was attempted and discarded. The binary footer cut memory by only
12% and grew footers by 2.8x; the raw-string cache regressed warm queries by 81%.
No stored-format change ships.

HA H8 and memory-backlog items 4, 5, 9, and 10 remain open. Open questions remain
unchanged; see the [efficiency plan](efficiency-plan.md#open-questions),
[memory backlog](memory-backlog.md), and [HA pair plan](ha-pair-plan.md#h8--uat-rollout-manual-approval).
