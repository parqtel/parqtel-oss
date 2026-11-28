# High availability: independent replica pair

Use this mode when ingest, queries, and alert evaluation must continue through
one Parqtel pod failure. It runs two independent replicas, not a clustered
store: each owns a PVC, WAL, buffer, blocks, rules, and alert state. No data is
shared or replicated between pods. Use the normal single-replica deployment if
you do not need this availability and can accept one pod's resource cost.

## Cost and trade-offs

| | Two independent replicas |
|---|---|
| Availability | Ingest, queries, and alerting continue when one replica is down. |
| Storage, CPU, memory | Doubled: two PVCs, two 4 GiB pods, and collector egress ×2. |
| Consistency | Replicas hold the same data except gaps when one was down longer than the collector queue covers. No backfill between replicas. |
| Query results | Can differ slightly by replica after an outage, because one side may have a gap. |
| Ingest scaling | None. Each replica ingests everything. |

## Configure ingestion

Send every batch to both replicas using one collector pipeline and one OTLP
exporter per pod DNS name. Never send ingest to a regular or headless Service
that resolves to multiple pods: connection-level load balancing can split data
between replicas instead of delivering every batch to each one.

Start from the [OTel Collector HA example](../../examples/agents/otel-collector-ha.yaml).
It uses persistent per-replica queues and unlimited retries. Its default queue
covers about 10 minutes at 9.5K metric points/s with batches up to 1,000 points;
size durable queue storage for your ingest rate and target outage window. If a
replica returns before its queue fills, the collector replays its missed data.
An outage longer than the queue coverage leaves a permanent gap on that replica;
there is no Parqtel-to-Parqtel backfill.

Enable the Helm chart's [`ha.enabled`](../../deploy/helm/parqtel/README.md)
mode with `replicaCount: 2` or more. The chart provisions per-pod storage,
headless per-pod DNS, anti-affinity, and disruption protection. Adjust the
collector endpoints for the chart's release, namespace, and headless Service.

## Queries and monitoring

The regular query Service selects both pods. Each request is answered by one
replica; Parqtel does not merge or reconcile results across replicas. Results
may differ after an outage until the collector queue catches up, or permanently
if the queue overflowed. Query a specific pod when diagnosing that difference.

Scrape `/metrics` from both pods and distinguish them with
`parqtel_build_info{replica="..."}`. Set a unique `server.replica_id` for each
pod, or use the default pod `HOSTNAME`; `/api/v1/stats` also reports
`replica_id`. Do not add replica identity to stored telemetry or alert labels.
Compare per-replica ingest rates and alert that either replica stops receiving
data.

## Alert routing and deduplication

Route alerts from both replicas through Alertmanager for grouping, silences,
and fan-out. Both replicas must use identical alert labels; do not include
replica identity. Alertmanager deduplicates identical label sets. Direct
Slack, Teams, or generic webhook delivery from each Parqtel pod produces
duplicate notifications. PagerDuty can deduplicate by its shared `dedup_key`.

Configure the Alertmanager receiver as described in
[Alerting](alerting.md#receivers). The default egress policy blocks private
addresses. For an in-cluster Alertmanager, allow its exact service DNS hostname
in Parqtel's egress policy, keeping `allow_private = false`:

```toml
[alerting.routing.egress]
allow_private = false
allow_hosts = ["alertmanager.monitoring.svc"]
```

Use the same hostname in the receiver URL, and replace it with your
Alertmanager Service DNS name and namespace. `allow_hosts` bypasses private-IP
blocking for that hostname, so avoid broad wildcards.

## Rollback

Change the collector back to its single exporter targeting replica 0, then
scale the StatefulSet to one pod. Replica 0 keeps its PVC and data; replica 1's
data remains separate and is not merged.

## Scaling writes and reads separately

The HA StatefulSet replicates one shard: every replica keeps its own PVC and
the common Service sends every read to one replica. When ingestion outgrows a
single shard's PVC or reads outgrow one pod, use `mode: sharded` instead: the
writer StatefulSet ingests into a shared object-store bucket, a reader
Deployment (with an HPA) serves queries from that bucket, and one compactor
merges and retires blocks. Writers keep no shared state beyond the bucket and
their own WAL staging, so readers scale independently of ingest.

See [Sharded writers and stateless readers](deployment.md#sharded-writers-and-stateless-readers)
for the values, the internal buffer token and the collector fan-out example.
