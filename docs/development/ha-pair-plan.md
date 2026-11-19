# HA pair plan: two independent replicas

Status: design only. No product code in this branch. Based on `origin/main` `63a9cf1`.
Decision (2026-11-19): the operator chose an independent replica pair over
leader/follower buddy peering. The earlier leader/follower plan is in this file's git
history (`9dca08c`).

## Design

Run two identical Parqtel replicas. Each has its own PVC, WAL, buffer, blocks, rules
and alert state. The OTel collector sends every batch to **both**. Either replica can
answer any query. Duplicate notifications are removed downstream.

```text
                 OTel collector
        (one pipeline, two exporters, persistent queues)
              |                          |
              v                          v
   parqtel-0.<headless svc>     parqtel-1.<headless svc>
   +--------------------+       +--------------------+
   | WAL, buffer, blocks|       | WAL, buffer, blocks|
   | rules, alerts      |       | rules, alerts      |
   +---------+----------+       +---------+----------+
             |  identical alert labels    |
             +-------------+--------------+
                           v
       Alertmanager (dedupe)  /  PagerDuty (dedup_key)
                           |
                  Slack, Teams, email, ...

   Queries: one Service over both pods (either replica answers)
```

No Parqtel-to-Parqtel traffic, no leases, no shared catalog, no object store needed.

## What this gives and costs

| | |
|---|---|
| Availability | Ingest, queries and alerting continue when one replica is down. |
| Storage, CPU, memory | Doubled: two PVCs, two 4 GiB pods, collector egress ×2. |
| Consistency | Replicas hold the same data except gaps when one was down longer than the collector queue covers. No backfill between replicas. |
| Query results | Can differ slightly by replica after an outage (a gap on one side). |
| Ingest scaling | None. Each replica ingests everything. |

## Facts this plan depends on (checked in the tree and on UAT)

- Alert identity is **not** stable today. `AlertInstance::compute_fingerprint`
  (`crates/parqtel-alert/src/lib.rs`) hashes with `ahash::AHasher::default()`. Its
  output is not guaranteed across builds or versions, so two replicas on different
  builds produce different fingerprints and group keys. PagerDuty `dedup_key` is the
  group key (`crates/parqtel-alert/src/notifier/pagerduty.rs`), so dedupe there also
  depends on this. Fix in H1.
- Group keys already use a fixed FNV-1a hash over fingerprints
  (`crates/parqtel-alert/src/dispatcher/group.rs`), so stable fingerprints make group
  keys stable.
- There is no Alertmanager push receiver. `notifier/webhook.rs` sends the
  Alertmanager *webhook notification* format, which Alertmanager does not ingest.
  Add one in H2.
- The outbound egress policy blocks private ranges by default
  (`crates/parqtel-alert/src/egress.rs`); an in-cluster Alertmanager needs an explicit
  host allowlist entry.
- The Helm chart deploys a single-PVC `Deployment` and fails for `replicaCount > 1`
  (`deploy/helm/parqtel/templates/deployment.yaml`, `values.schema.json`).
- UAT runs its own `StatefulSet` `odin-parqtel` with `volumeClaimTemplates` and a
  **headless** Service `odin-parqtel` (ClusterIP `None`). Pod DNS names such as
  `odin-parqtel-0.odin-parqtel.monitoring.svc` already resolve.
- The UAT collector exports to `odin-parqtel.monitoring.svc.cluster.local:4317`. With a
  headless Service and two pods, that name returns both IPs and a gRPC client usually
  sticks to one, so each replica would get part of the data. The collector must use one
  exporter **per pod DNS name**.

## Rules

1. Ingest must target each replica directly. Never point the collector at a name that
   load-balances across replicas.
2. Alert labels must be identical on both replicas: no replica-specific label on
   alerts or on stored data.
3. A replica never talks to the other. Each must work alone.
4. Single-replica deployments behave exactly as today.

## Alert dedupe paths

| Destination | Dedupe |
|---|---|
| PagerDuty | Native: same `dedup_key` from both replicas once H1 lands. |
| Alertmanager | Native: identical label sets from two senders are one alert (H2). |
| Slack, Teams, generic webhook direct from Parqtel | **Duplicates.** Route these through Alertmanager in HA mode, or accept two messages. |

Recommended HA routing: Parqtel → Alertmanager (H2) for every channel; Alertmanager
owns grouping, silences and fan-out. Keep Parqtel's native receivers for single-node.

## Tasks

Each task is one PR, delegated to the lowest-cost model that passes its gate,
reviewed, gated and committed as `sonu202604 <sonu202604@gmail.com>`.

### H0 — Design doc
This document. Gate: `make docs-check`.

### H1 — Stable alert fingerprints
- Files: `crates/parqtel-alert/src/lib.rs`, `crates/parqtel-alert/src/store/alert_store.rs`, tests.
- Work: replace `AHasher::default()` with a fixed 64-bit FNV-1a over
  `rule_id` and sorted `(name, value)` pairs, with a separator byte so `("ab","c")` and
  `("a","bc")` differ. On `AlertStore` load, recompute each stored alert's fingerprint
  from its rule id and labels so persisted alerts keep matching.
- Tests: golden value for a fixed input (pins the algorithm across builds); separator
  case; reload keeps an existing firing alert matched (no duplicate instance).
- Gate: `cargo test -p parqtel-alert`, `make lint`.

### H2 — Alertmanager push receiver
- Files: `crates/parqtel-alert/src/notifier/alertmanager.rs` (new), `notifier/mod.rs`,
  `crates/parqtel-core/src/config/routing.rs`, `docs/operations/alerting.md`.
- Work: receiver type `alertmanager` with `url` (base URL). POST
  `/api/v2/alerts` with `[{labels, annotations, startsAt, endsAt, generatorURL}]`.
  Labels: `alertname` (rule name), `rule_id`, `severity`, plus series labels.
  Firing: `endsAt` = now + `resend_ttl` (default 4 × evaluation interval); resolved:
  `endsAt` = resolve time. Resend firing alerts every evaluation so Alertmanager keeps
  them active. Reuse the existing retry and egress checks.
- Tests: mock server receives the expected body for firing and resolved; two
  "replicas" (two notifier instances) send byte-identical label sets.
- Gate: `cargo test -p parqtel-alert notifier`, `make lint`.

### H3 — Replica identity for operators
- Files: `crates/parqtel-core/src/config/server.rs`, `crates/parqtel-server/src/metrics.rs`,
  `crates/parqtel-server/src/handlers/misc.rs`.
- Work: `server.replica_id` (default `HOSTNAME`). Expose it in `/api/v1/stats` and as
  `parqtel_build_info{replica=...}` only. Never add it to stored data or alert labels.
- Gate: `cargo test -p parqtel-server`, `make lint`.

### H4 — Helm HA mode
- Files: `deploy/helm/parqtel/values.yaml`, `values.schema.json`, `templates/deployment.yaml`,
  new StatefulSet and headless Service templates,
  `templates/poddisruptionbudget.yaml`, `deploy/helm/parqtel/README.md`.
- Work: `ha.enabled` (default false). When true: render a `StatefulSet` with
  `replicaCount: 2` (allow 2..N), `volumeClaimTemplates` (default 30Gi), headless
  Service for per-pod DNS, required pod anti-affinity across nodes, PDB
  `minAvailable: 1`, startup probe `failureThreshold: 120`. Keep the single-replica
  `Deployment` and the `replicaCount > 1` guard when `ha.enabled` is false.
- Gate: `helm lint` and `helm template` with default and HA values; schema rejects
  `replicaCount: 2` without `ha.enabled`.

### H5 — Collector fan-out configuration
- Files: `examples/agents/otel-collector-ha.yaml` (new), `examples/agents/README.md`.
- Work: one exporter per replica (`otlp_grpc/parqtel-0`, `otlp_grpc/parqtel-1`) on pod
  DNS names; both in every pipeline; `sending_queue` with the `file_storage` extension
  so a restarted replica receives what it missed up to the queue size; unlimited retry.
  Note the queue size needed for a target outage window at the UAT rate.
- Gate: `otelcol validate --config examples/agents/otel-collector-ha.yaml` when the
  binary is available; otherwise YAML lint plus review.

### H6 — Operations guide
- Files: `docs/operations/high-availability.md` (new), `docs/README.md`,
  `docs/operations/deployment.md`.
- Work: when to use HA, the cost table, ingest-per-pod rule, alert routing through
  Alertmanager, egress allowlist for an in-cluster Alertmanager, divergence after
  outages, rollback (scale to 1).
- Gate: `make docs-check`.

### H7 — Failover check script
- Files: `scripts/checks/ha_pair.sh` (new).
- Work: against a two-replica deployment: send tagged points through the collector,
  confirm both replicas have them; delete one pod; confirm ingest and queries continue
  on the other and a test alert produces one Alertmanager alert; restore and confirm
  the restarted pod catches up from the collector queue.
- Gate: script passes on local k3d (`make local-k3d-up`) with an Alertmanager.

### H8 — UAT rollout (manual approval)
Not delegated. Steps, each needing approval:
1. Ship an image with H1 (and H2 if Alertmanager is used).
2. Scale `odin-parqtel` to 2. The new pod's PVC `data-odin-parqtel-1` comes from the
   template at 10Gi; expand it to 30Gi right after creation.
3. Change the UAT collector (`otel-collector-ksm`) to the two per-pod exporters.
4. Point alert routing at UAT Alertmanager (`kube-prometheus-stack-alertmanager`) and
   allowlist it in egress policy.
5. Validate for 6 h: both replicas ingest ~9.5K points/s, zero restarts, memory below
   4 GiB each, one notification per alert, queries succeed from both.

Rollback: scale to 1 and restore the single collector exporter; replica 0 keeps its data.

## Open decisions

- Alertmanager for all channels (recommended) or keep direct Slack/Teams and accept
  duplicates.
- Collector persistent queue size, which sets how long a replica can be down without a
  permanent gap.
- Whether queries should prefer replica 0 (fewer gaps after replica 1 restarts) via a
  second Service, or use both.

## Goal text for the coding agent

> Implement HA pair tasks H1–H7 from `docs/development/ha-pair-plan.md` on branch
> `feat/ha-pair`, in order, one task per subagent. Review each diff, run its gate,
> commit as `sonu202604 <sonu202604@gmail.com>`. Single-replica behavior must stay
> unchanged. Do not change UAT (H8) without approval.
