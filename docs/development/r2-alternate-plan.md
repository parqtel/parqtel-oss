# R2 alternate plan: fewer blocks per query, then a smaller cache

## R2 result

R2 used the same Q7 image and digest, `sha256:957a5d7f…`. The only changes were
`limits.memory=6Gi` and
`PARQTEL__QUERY__FOOTER_CACHE_BYTES=1073741824`. UAT was restored afterward to
4Gi and 192MiB, with zero restarts.

The six-hour soak log is `/tmp/opencode/r2-soak-6h.log`. It recorded 361 samples.
The script stopped panel load at its 3,800 MiB safety threshold after sample 37.
It reported `panel_ok=148`, zero failures, peak memory of 4,658 MiB, zero
restarts, zero ingest errors, a maximum ingest gap of 1s, and 46 log and 46 trace
batches.

Only 37 panel samples preceded the safety stop. For hours 1–2,
`allocators-15m` latency was p50 1,139 ms and p95 4,065 ms, far below Q7's
7,650 ms and 15,827 ms. R2 has no valid hours 4–6 latency data.

| Final scan counter | R2 result |
|---|---:|
| Blocks | 12,185 |
| Footer hits / misses | 11,829 / 314 (2.6% misses) |
| Footer cache bytes / budget | 971,209,085 / 1,073,741,824 |
| Footer load | 293.7 s |
| Index bytes loaded | 15,408,751,057 |
| Admission wait | 23.3 s |

Q7 had 35.5% footer misses, 12,988 s footer load, and 8,684 s admission wait.
R2 confirms the cache-size diagnosis: 1GiB removes most footer tail cost, but
violates the memory target. Peak memory of 4,658 MiB exceeds both the former 4Gi
pod limit and the soak's 3,800 MiB safety threshold. Do not keep 1GiB on UAT.

## Why not just keep the larger cache

It works, but memory safety wins. The 4,658 MiB peak exceeds both the pod's
former 4Gi limit and the soak's safety threshold.

## Ordered plan and gates

### R3-0 — record R2 locally

**Approval:** Not needed; local documentation only. **UAT:** None.

In a future task, document the R2 result in `efficiency-plan.md`. This work is
not part of this PR.

**Gate:** Record measured result and limits without changing UAT.

### R3-1 — identify block-level query fan-out

**Approval:** Required before UAT probe. **UAT:** Read-only; no soak.

Build an image from main `2b2ce43` that includes the R1 per-level counters.
Deploy with original UAT settings: 4Gi memory and a 192MiB footer cache. Run a
60-minute read-only probe and capture `parqtel_scan_*_by_level_total` to determine
whether block opens are dominated by L0 backlog or L1/L2.

Use no load generator beyond the existing collector. Do not run a soak.

**Gate:** Counters identify whether L0 or L1/L2 dominates block opens. Stop if
the probe changes data, adds load, or requires config changes beyond the stated
deployment.

### R3-2 — reduce blocks per query locally

**Approval:** Not needed for local code and tests. **UAT:** None.

Choose exactly one path from R3-1 evidence:

- If L0 dominates, make L0→L1 compaction keep up.
- If L1 dominates, add 30–60 minute L2 windows through Phase 1b.

Add tests that compare query results and block counts. Do not implement both
paths without evidence.

**Gate:** Tests prove query results remain equivalent and block counts improve
for the selected path.

### R3-3 — validate smaller cache on UAT

**Approval:** Required before rollout and soak. **UAT:** Six-hour soak.

Roll out the R3-2 build with a moderate 384–512 MiB cache and a 4Gi memory
limit. Run the six-hour soak.

**Pass targets:** During hours 4–6, p50 ≤4s and p95 ≤7s; footer misses ≤10%;
peak memory <3.8 GiB; zero restarts and zero ingest errors.

**Gate:** Pass every target before keeping the rollout. On any failure, stop and
reassess; do not raise the memory limit or retain a 1GiB cache as a workaround.

### R4 — keep deferred work gated

Phase 2b binary lazy footer stays parked unless R3-3 fails. Phase 3b stays ruled
out: step evaluation measured 5.45%. HA H8 stays separate and approval-gated.
