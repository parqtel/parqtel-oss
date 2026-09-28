# Built-in Alert Presets — Design & Backlog Plan

Status: **DRAFT for review** (Phase 0 — no implementation started)

## 1. Problem / Goal

A greenfield Parqtel install alerts on nothing: the engine ships with threshold
evaluation, state machine, noise suppression and routing, but zero rules. The
existing `rules/presets/` packs (Kubernetes, CoreDNS, external-secrets,
service-RED, Parqtel self) help, but they are **inert** — a user must discover
them, copy them into `alerts.rules_dir`, and restart. Users also asked for
coverage beyond the Kubernetes day-1 set.

**Goal:** ship built-in ("batteries-included") alert packs **inside the
binary** for:

| Domain | Pack key | Existing pack? |
|---|---|---|
| Kubernetes | `kubernetes` | yes (`kubernetes-cluster.yaml`, 11 rules) |
| Kafka | `kafka` | no |
| Redis | `redis` | no |
| PostgreSQL | `postgresql` | no |
| Java (JVM) | `java` | no |
| Golang (runtime) | `golang` | no |
| Parqtel | `parqtel` | yes (`parqtel-self.yaml`, 6 rules) |
| ArgoCD | `argocd` | no |
| etcd | `etcd` | no |
| OpenTelemetry (Collector) | `opentelemetry` | no |
| Kyverno | `kyverno` | no |
| CoreDNS | `coredns` | yes (5 rules) |
| External Secrets | `external-secrets` | yes (4 rules) |
| Service RED | `service-red` | yes (3 rules) |

…with **two enablement mechanisms**:

1. **Auto mode** — a pack activates automatically when its canary metrics are
   detected in the metric index/buffer.
2. **Explicit mode** — a config flag with an **include-only list** or an
   **exclude list** selecting which packs are active.

## 2. Current state (facts the design builds on)

- Packs live in `rules/presets/*.yaml` (29 rules total today) and are validated
  in CI by `make test-presets-static` (parse → `AlertRule` → `parse_query` →
  `QueryPlan`) and `make test-alert-presets` (e2e fire/recover over HTTP).
- `load_rules_dir()` scans `alerts.rules_dir` **non-recursively** at startup and
  inserts rules into an in-memory `AlertRuleRegistry` keyed by `rule.id`;
  `list_enabled()` drives the 15 s evaluation loop.
- `AlertRule` has `enabled: bool` and a free-form `labels: BTreeMap` — enough to
  tag provenance (`labels.pack = "kafka"`) with **no schema change**.
- `QueryExecutor::list_metrics()` already returns every known metric name from
  **both** the block index and the `MemoryBuffer` — a ready-made detection source.
- Config is Figment-layered (`AlertConfig` in `parqtel-core/src/config/alert.rs`,
  defaults → TOML → env `PARQTEL__…` → CLI) and mirrored by the Helm chart
  (`charts/parqtel/values.yaml` → `templates/configmap.yaml`).
- Alert queries are single selectors with optional aggregation: **no cross-metric
  arithmetic and no `histogram_quantile`** in alert rules. Thresholds are
  absolute, or window `increase()`/`rate()` proxies; ratios only when the
  exporter emits a pre-computed gauge. Every pack's README documents this.


## 3. Design decisions

### D1 — Packs are embedded in the binary; YAML stays the source of truth

`rules/presets/*.yaml` remains the authoring format (and stays covered by the
existing static test, byte-for-byte). A new `parqtel-alert/src/builtin.rs`
embeds each pack with `include_str!`:

```rust
pub struct BuiltinPack {
    pub name: &'static str,                // "kafka"
    pub description: &'static str,         // one-liner for UI/status endpoint
    pub canaries: &'static [&'static str], // ANY match => pack detected
    pub rules: &'static str, // include_str!("../../rules/presets/kafka.yaml")
}

pub const BUILTIN_PACKS: &[BuiltinPack] = { /* … */ };
```

Rationale: Parqtel's single-binary / scratch-container philosophy (same pattern
as `ui.html` via `include_str!`); disk-loaded presets would require volume
mounts in Kubernetes for something that should just work.

### D2 — Canary metrics live in the Rust manifest, not in YAML

The preset format deserializes every `---` document as an `AlertRule`; adding
pack front-matter would break `load_rules_dir` and the static test. Keeping
canaries in the manifest preserves the format. A unit test enforces: unique pack
names, non-empty canary lists, and (via `include_str!`) that files exist.

Canaries are **ANY-of** lists to tolerate exporter variance (e.g. Kafka via JMX
exporter vs `kafka_exporter` vs Redpanda emit different metric names).

### D3 — Config surface

```toml
[alerts.presets]
mode = "off"        # "off" | "auto" | "all"
include = []        # include-only list of pack keys (wins over exclude)
exclude = []        # skip these pack keys
```

- `off` (default) — nothing built-in loads; **zero behavior change** for
  existing deployments.
- `auto` — candidate packs activate when any canary metric is present.
- `all` — candidate packs activate at startup regardless of metrics (a rule
  whose metric is absent simply never evaluates — existing, documented
  behavior).
- Selection: non-empty `include` ⇒ include-only (if `exclude` is also set,
  `include` wins and a warning is logged). Otherwise `exclude` is subtracted.
  Unknown pack keys are logged as warnings and ignored (lenient under layered
  config).
- Env overrides: `PARQTEL__ALERTS__PRESETS__MODE=auto`; `include`/`exclude`
  accept a comma-separated string **or** a YAML array (custom
  `deserialize_with` helper — Figment env values are plain strings).
- Helm: `parqtel.alerts.presets.{mode,include,exclude}` → configmap.

### D4 — Activation semantics

```
candidates = BUILTIN_PACKS filtered by include/exclude
mode == "off"  → done (nothing)
mode == "all"  → insert every candidate's rules at startup
mode == "auto" → startup check of list_metrics();
                 packs with >=1 canary present activate immediately;
                 remaining packs re-checked every 5 min until activated;
                 log info!(pack, rules) once per activation; then stop.
```

Invariants:

1. **Never deactivate.** A pack activated on canary detection stays on even if
   metrics later disappear (no flap); hysteresis by design.
2. **One-shot per pack per process.** Activation runs at most once per pack, so
   a user's `DELETE /api/v1/rules/:id` or `enabled=false` via the API is never
   silently reverted.
3. **User rules always win.** Activation inserts only rule ids absent from the
   registry, and `main.rs` loads built-ins **before** `rules_dir` files, so a
   file with the same id overrides the built-in. (Power-user flow: copy a
   built-in id into `rules_dir` to customize it.)
4. **Inactive packs stay invisible.** Candidates are not staged into the
   registry (no `enabled=false` clutter in `/api/v1/rules`); pending state is
   exposed via the status endpoint (D6).
5. Every injected rule carries `labels.pack = <key>` for grouping/filtering.

### D5 — Rule content constraints (house style)

Per pack: 4–7 rules, ids prefixed by domain (`kafka-under-replicated-partitions`,
`redis-evicted-keys`, …), each with `summary`, tuning-guided `description`
including metric-name fallbacks, and a `runbook` link. Query shapes limited to
what the alert loop can execute: selector + `rate`/`increase`/aggregation,
absolute thresholds; `histogram_quantile` avoided (use `_sum` proxies); ratios
only from pre-computed gauges. A new static assertion checks **global id
uniqueness across all pack files**.

### D6 — Visibility (status endpoint)

`GET /api/v1/alerts/presets` → per pack: `{key, description, rule_count,
selected, active, canaries, canaries_detected}`. Enables the Rules page to show
packs as *available / active / not selected* badges (UI work optional, see
phases). No UI change is required for v1 of the engine feature.


## 4. Pack catalog (proposed rules)

Existing packs (already written; gain manifest entries + canaries only):

| Pack | Canary (ANY-of) | Rules |
|---|---|---|
| `kubernetes` | `kube_node_status_condition`, `k8s.node.cpu.utilization` | 11 |
| `parqtel` | `parqtel_ingested_points_total`, `parqtel_ingest_gap_secs` | 6 |
| `coredns` | `coredns_dns_requests_total` | 5 |
| `external-secrets` | `externalsecret_status_condition` (KSM CR metric) | 4 |
| `service-red` | `traces_service_requests_total_total` | 3 |

New packs (~50 rules total):

| Pack | Canary (ANY-of) | Proposed rules (ids → query shape) |
|---|---|---|
| `kafka` | `kafka_server_brokertopicmetrics_messagesin_total`, `kafka_broker_topic_metrics_messages_in_total`, `kafka_consumergroup_lag` | under-replicated partitions (`…underreplicatedpartitions > 0`, critical); active-controller count `!= 1`; offline partitions `> 0`; consumer lag `> 10k` (warning); failed produce `increase(…[5m]) > 0`; ISR shrink `increase(…[15m]) > 0`; request-handler idle `< 0.2` |
| `redis` | `redis_up`, `redis_memory_used_bytes` | `redis_up == 0` (critical); fragmentation ratio `> 1.5`; connected clients `> 5k`; evictions `increase(redis_evicted_keys_total[5m]) > 0` (critical — silent data loss); keyspace misses `increase(…[5m]) > 10k`; master link down `redis_master_link_status == 0`; slow fork `redis_latest_fork_usec > 100k` |
| `postgresql` | `pg_up`, `pg_stat_database_numbackends` | `pg_up == 0` (critical); connections near limit `pg_stat_activity_count > 100`; deadlocks `increase(pg_stat_database_deadlocks[5m]) > 0` (critical); long tx `pg_stat_activity_max_tx_duration > 3600`; replication lag `pg_replication_lag > 30s` (critical); rollbacks `increase(pg_stat_database_xact_rollback[5m]) > 10` |
| `java` | `jvm_memory_used_bytes`, `jvm_gc_pause_seconds_sum` | heap used `jvm_memory_used_bytes{area="heap"} > N` (bytes; ≈80% of `-Xmx`); GC overhead `increase(jvm_gc_pause_seconds_sum[10m]) > 60s`; live threads `> 400`; metaspace `jvm_memory_used_bytes{area="metaspace"} > 3e8` (fallback `area="nonheap"`); class-unloading stall rate (`gc _count` proxy) |

| `golang` | `go_goroutines`, `go_memstats_alloc_bytes` | goroutines `> 5k`; heap alloc `> 1e9`; GC pause `increase(go_gc_duration_seconds_sum[10m]) > 60s`; threads `go_threads > 100`; GC stall rate (`go_gc_duration_seconds_count` rate proxy) |
| `argocd` | `argocd_app_info` | app Degraded `argocd_app_info{health_status="Degraded"} > 0` (critical, 15m); OutOfSync `argocd_app_info{sync_status="OutOfSync"} > 0` (30m); sync failures `increase(argocd_app_sync_total{phase!~"Succeeded|Running"}[15m]) > 0`; cluster unreachable (cluster-info gauge) |
| `etcd` | `etcd_server_has_leader` | no leader `== 0` (critical); leader churn `increase(etcd_server_leader_changes_seen_total[15m]) > 5`; failed proposals `increase(etcd_server_proposals_failed_total[5m]) > 0` (critical); DB size `> 8GiB` (`etcd_mvcc_db_total_size_in_bytes`, fallback `etcd_mvcc_db_total_size`); slow apply `increase(etcd_server_slow_apply_total[5m]) > 0`; cumulative fsync `increase(etcd_disk_wal_fsync_duration_seconds_sum[5m]) > 15s` |
| `opentelemetry` | `otelcol_exporter_sent_spans`, `otelcol_exporter_sent_metrics` | exporter enqueue failed `{spans,metrics,logs}` `increase(otelcol_exporter_enqueue_failed_*[5m]) > 0` (critical); receiver refused `increase(otelcol_receiver_refused_spans[5m]) > 0`; processor dropped `increase(otelcol_processor_dropped_*[5m]) > 0`; queue saturation `otelcol_processor_queue_size > 10k`; send latency `increase(otelcol_exporter_send_duration_seconds_sum[5m]) > 30` |
| `kyverno` | `kyverno_policy_rule_results_total` | policy rule failures `increase(…{status="fail"}[5m]) > 0`; admission rejections spike `increase(kyverno_admission_requests_total{admitted="false"}[15m]) > 50`; background scan failures `increase(kyverno_background_scan_results{status="fail"}[15m]) > 0`; policy churn `increase(kyverno_policy_changes_total[15m]) > 10` (warning) |

> Rule sketches are validated against Parqtel's query constraints at authoring
> time by `make test-presets-static`; metric/label names are verified against
> the respective exporters during implementation (exporter names vary by
> version — that is exactly what the canary ANY-of lists and per-rule
> description fallbacks are for).


## 5. Phased execution plan (each phase = reviewable PR)

### Phase 1 — Engine & config (PR 1)
- `AlertConfig` gains `presets: PresetConfig { mode, include, exclude }`
  (defaults `off/[]/[]`), env comma-string/array helper, config tests.
- `parqtel-alert/src/builtin.rs`: `BuiltinPack` manifest embedding the **5
  existing** packs; manifest unit tests (unique names, canaries, global rule-id
  uniqueness across pack files).
- Activation module: pure `activate(mode, selection, &metric_names, &registry)`
  + auto-detector task (startup + 5 min retry until the pending set is empty),
  wired in `main.rs` **before** the `rules_dir` load; invariants 1–5 from D4.
- Helm values/configmap for `parqtel.alerts.presets.*`.
- `docs/CONFIGURATION.md` section.
- Tests: selection precedence (include wins + warn), unknown-key warning,
  `mode=off` noop, `mode=all` insertion + rules_dir override, auto activation on
  fake metric set, no re-activation after user disable, detector stops when done.

### Phase 2 — Pack batch A: datastores (PR 2)
`kafka`, `redis`, `postgresql` (~20 rules) + manifest entries + README row.

### Phase 3 — Pack batch B: runtimes (PR 3)
`java`, `golang` (~11 rules) + manifest entries (+ decision on a `process`
pack, see open questions).

### Phase 4 — Pack batch C: platform (PR 4)
`argocd`, `etcd`, `opentelemetry`, `kyverno` (~19 rules) + manifest entries.

### Phase 5 — Visibility & docs (PR 5)
- `GET /api/v1/alerts/presets` status endpoint (+ `openapi.yaml` entry).
- Optional: Rules-page pack badges in `ui.html` (headless-Chrome console check,
  keep gzip budget ≤ 1000 KB).
- `rules/presets/README.md` rewrite (built-ins, both enablement modes,
  include/exclude/auto examples), `AGENTS.md` key-facts line, FAQ entry.
- Optional e2e extension: boot with `mode=auto`, ingest a canary metric, assert
  the pack's rules appear in `/api/v1/rules`.

## 6. Validation gates (every PR)

- `cargo fmt --check` · `cargo clippy --workspace --all-targets -- -D warnings`
- `cargo test --workspace`
- `make test-presets-static` (every pack parses + is executable by the alert loop)
- `make test-alert-presets` (e2e fire/recover)
- New unit tests listed per phase; Helm lint via CI.


## 7. Risks & mitigations

| Risk | Mitigation |
|---|---|
| Canary false positives (`go_goroutines` exists wherever any Go app is scraped) | default `mode = "off"`; include/exclude control; docs call it out |
| Exporter metric-name variance across versions | canary ANY-of lists; per-rule description fallbacks (existing house style) |
| Rule id collisions across packs | new global-uniqueness static assertion |
| Auto-activation overriding a user's API disable | one-shot activation + insert-if-absent (D4 invariants 1–3) |
| Figment env array parsing | `deserialize_with` accepting comma-string or array + tests |
| Alert engine query limits (no ratios / `histogram_quantile`) | rule sketches use allowed shapes only; static test is the gate |

## 8. Open questions for review

1. **Default mode:** `off` (proposed — zero behavior change) vs `auto`
   (opinionated greenfield, but surprising for users who already scrape
   `go_*`/`jvm_*` metrics)?
2. **`include` + `exclude` both set:** include wins + warn (proposed) vs hard
   config error?
3. **New `process` pack** (canary `process_cpu_seconds_total`): CPU / RSS /
   FD saturation rules are cross-runtime — give them their own pack (proposed)
   rather than duplicating ids/queries across `java` and `golang`?
4. **Auto-detector interval:** startup + every 5 min until all pending packs
   activate; never deactivate — OK?
5. **Pack keys & naming** (table in §1) — e.g. `postgresql` vs `postgres`?
6. **Rule budget** of 4–7 per pack OK for v1 (extensible later)?
7. `service-red` auto-activates for anyone tracing services when `mode=auto` —
   intended? (proposed: yes, that is what `auto` means; use `exclude` to opt
   out.)

## 9. Effort estimate

| Phase | Size |
|---|---|
| 1 Engine & config | ~1–2 days |
| 2–4 Pack batches | ~0.5 day each |
| 5 Visibility & docs | ~1 day |
| **Total** | **~4–5 days** |
