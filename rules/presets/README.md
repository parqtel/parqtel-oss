# Parqtel preset alert rules

Battle-tested starting-point alert rules for Kubernetes platforms and the
core components that every cluster depends on. Each preset is a single
YAML file (multi-document) that drops straight into Parqtel's rules
directory.

These presets exist because a greenfield Parqtel install alerts on nothing
by default: you get the engine (threshold evaluation, state machine,
noise suppression, notification routing) but no rules. The packs below are
the rules an SRE team would write in the first week — already shaped to
fit Parqtel's evaluation model, with per-rule runbooks and tuning notes.

## Packs

| File | Scope | Metrics required |
|------|-------|------------------|
| [`kubernetes-cluster.yaml`](kubernetes-cluster.yaml) | Node CPU/memory saturation, node NotReady, CrashLoopBackOff, restart loops, stuck Pending pods, deployment rollout stalls, volume pressure | kube-state-metrics + kubelet + OTel `k8sclusterreceiver` |
| [`coredns.yaml`](coredns.yaml) | DNS SERVFAIL/REFUSED, panics, cache misses, traffic drop | CoreDNS prometheus exporter (`:9153`) |
| [`external-secrets.yaml`](external-secrets.yaml) | Stale ExternalSecrets, sync failures, `Available=False`, never-synced | external-secrets controller + kube-state-metrics (with `externalsecrets`) |
| [`service-red.yaml`](service-red.yaml) | Rate/error/burst/traffic-drop for any traced service | Parqtel span-metrics RED bridge — **no extra instrumentation** |
| [`parqtel-self.yaml`](parqtel-self.yaml) | Ingest stall, query errors, memory, buffer backlog, flush time, compaction lag | Parqtel self-telemetry (`telemetry.otlp_enabled`) |

## Enabling a pack

Parqtel loads every `*.yaml` file in `alerts.rules_dir` (default `rules/`)
at startup, non-recursively. This `presets/` subdirectory is intentionally
**not** scanned, so the presets ship inert — enable only what you want.

Copy the pack into the live rules directory:

```bash
cp rules/presets/coredns.yaml rules/          # or your mounted rules volume
docker restart parqtel                         # or: helm upgrade … --reuse-values
```

Or point the whole deployment at a presets-only directory:

```bash
parqtel serve --rules-dir rules/presets
```

```yaml
# config.toml — or PARQTEL__ALERTS__RULES_DIR
[alerts]
rules_dir = "/etc/parqtel/rules/presets"
```

In Kubernetes, mount the pack as a ConfigMap and set `alerts.rulesDir` to
the mount path (the Helm chart supports this via `extraVolumes` /
`extraVolumeMounts`).

## Feeding the metrics

Parqtel alerts evaluate **only** against data Parqtel has ingested. The
Kubernetes/CoreDNS/external-secrets metrics come from the standard
pipeline: an OpenTelemetry Collector running the `prometheus` receiver
(scraping kube-state-metrics, kubelet, coredns, the external-secrets
controller) and the `k8s_cluster` receiver, exporting OTLP to Parqtel's
`:4317`. The `service-red` and `parqtel-self` packs need no collector —
span metrics are derived inside Parqtel, and self-telemetry is exported by
Parqtel itself (point `telemetry.otlp_endpoint` at a collector or back at
the instance's own `:4317`).

If a metric is absent, its rules simply do not evaluate — no false
positives, but no coverage either. Verify each pack's metrics are arriving
before relying on it:

```bash
# Does CoreDNS data reach Parqtel?
curl -s 'http://localhost:9090/api/v1/query?query=coredns_dns_requests_total' | jq '.data.result | length'
```

## How evaluation works (and why the queries look like this)

Every 15s the evaluation loop parses each enabled rule's `query`, executes
it over a trailing 5-minute window, and thresholds the **last sample of
every result series** — one alert instance per series (fingerprinted by
the rule id + the series' labels). A condition that stays true for
`for_duration_secs` transitions Pending → Firing and emits a notification
event; the first cycle below the threshold transitions it to Resolved.

Two consequences shape every rule here:

1. **No cross-metric arithmetic.** A rule query is a single selector with
   optional aggregation, so ratios like `errors / requests` are not
   expressible. Error rules threshold absolute rates (`rate(...) > N/s`)
   or window counts (`increase(...[5m]) > N`) instead — tune the numbers
   to your traffic.
2. **`histogram_quantile` is unavailable in alert rules** (the alert loop
   uses the plan path, not the AST engine). Latency-style rules therefore
   use `_sum` rate proxies (see `parqtel-flush-oversubscribed`) rather
   than p95/p99.

Each rule carries a `summary`, a `description` with tuning guidance and
the metric-name fallbacks, and a `runbook` link.

## Validating

```bash
# Static: every preset parses, matches the AlertRule schema, and its query
# is executable by the alert loop (runs in CI via `make test`).
cargo test -p parqtel-alert --test preset_rules

# End-to-end: boots Parqtel, loads the presets, proves rules fire on
# threshold crossings and recover when they clear.
scripts/test_alert_presets.sh
```

The end-to-end script exercises one rule per distinct query shape — plain
gauge threshold, matcher-restricted gauge, `increase()` over a counter, and
the `<` operator — plus the Pending → Firing → Resolved state transitions.
It needs `curl`, `jq`, and `python3`.

## Tuning checklist

- **Severity** is `warning` (investigate soon) or `critical` (page now).
  Route them differently with notification rules in `rules/notifications/`.
- **for_duration_secs** controls flap resistance: short for symptoms that
  self-heal, longer for noisy signals.
- **Thresholds** are conservative defaults, not universal truths. The
  CPU/memory utilization rules assume 0–1 ratios from the OTel
  `k8sclusterreceiver`; if you collect byte gauges instead, swap the query
  to the byte metric and set the value to the matching fraction of capacity.
- **noise_suppression_threshold** (default 0.7) can be raised for packs
  that see bursty traffic.
