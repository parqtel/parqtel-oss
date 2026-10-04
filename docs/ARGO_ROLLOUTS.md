# Argo Rollouts Integration

Parqtel is a drop-in **metrics backend** for [Argo Rollouts](https://argo-rollouts.readthedocs.io/) analysis. Because Parqtel exposes a **Prometheus-compatible query API** (`/api/v1/query`, `/api/v1/query_range`), Argo Rollouts' built-in **Prometheus metric provider** queries Parqtel with **no custom code** — you only point the provider's `address` at Parqtel.

This page shows how to:
1. Deploy Parqtel for rollout analysis
2. Gate canary/blue-green rollouts on **RED metrics** (Rate / Errors / Duration) derived from your OpenTelemetry traces
3. Gate on any custom Parqtel metric
4. What's coming next (a dedicated provider / plugin for logs + traces + cross-signal gating)

## How it fits

Argo Rollouts gates each progressive-delivery step on **AnalysisRuns**. Each AnalysisRun evaluates one or more *metrics*; a *provider* fetches a measurement, and `successCondition` / `failureCondition` expressions decide pass/fail. Parqtel is the data source behind the provider:

```
Rollout ──(canary step)──▶ AnalysisRun ──▶ Prometheus provider ──HTTP──▶ Parqtel /api/v1/query
                                                                          │
                                                                      ParQL (PromQL) over
                                                                      OTel metrics + derived
                                                                      RED span-metrics
```

The provider is stock Argo Rollouts; only `provider.prometheus.address` points at Parqtel.

## Prerequisites
- A Kubernetes cluster (k3d / kind / any k8s)
- [Argo Rollouts](https://argo-rollouts.readthedocs.io/en/stable/) installed
- Parqtel deployed (Helm chart in this repo: `charts/parqtel`)
- Your workloads exporting OTel **traces** (and/or metrics) to Parqtel via OTLP gRPC `:4317` or HTTP `:8080`

> **Zero-config RED:** If your app exports OTel **traces**, Parqtel auto-derives `traces_service_{requests,errors,duration_ms}_total` per `service.name` / operation from **server** spans — no hand-written instrumentation or recording rules needed.

## 1. Deploy Parqtel

```bash
helm install parqtel ./charts/parqtel -n parqtel --create-namespace
# HTTP API : http://parqtel.parqtel.svc.cluster.local:8080
# OTLP gRPC: parqtel.parqtel.svc.cluster.local:4317
```

Verify:
```bash
kubectl -n parqtel port-forward svc/parqtel 8080:8080 &
curl -s http://localhost:8080/health
```

## 2. Gate a canary on error rate (RED)

See `examples/argo-rollouts/analysis-template-red.yaml`:

```yaml
apiVersion: argoproj.io/v1alpha1
kind: AnalysisTemplate
metadata:
  name: parqtel-red
spec:
  args:
  - name: service          # OTel service.name of the workload under test
  metrics:
  - name: error-rate
    initialDelay: 1m       # let RED metrics accumulate before the first check
    interval: 30s
    count: 10              # ~5 minutes of sampling
    failureLimit: 1
    successCondition: "result[0] < 0.01"      # error rate under 1%
    failureCondition: "result[0] > 0.05"      # error rate over 5% -> abort
    provider:
      prometheus:
        address: http://parqtel.parqtel.svc.cluster.local:8080
        query: |
          sum(sum_over_time(traces_service_errors_total{service.name="{{args.service}}"}[5m]))
          /
          sum(sum_over_time(traces_service_requests_total{service.name="{{args.service}}"}[5m]))
```

Wire it into a canary (`examples/argo-rollouts/rollout-canary.yaml`):

```yaml
strategy:
  canary:
    canaryService: parqtel-canary-canary
    stableService: parqtel-canary-stable
    steps:
    - analysis:
        templates: [parqtel-red]
        args:
        - name: service
          value: my-service
    - setWeight: 100
```

If the canary's error rate exceeds 5% for more than `failureLimit` samples, the AnalysisRun fails and the rollout **aborts** (canary scaled down, stable kept).

### Canary vs. stable
RED metrics are grouped by `service.name` / `operation`. To gate **only the canary**, make the canary revision export a distinguishing label (set the OTel `service.name` to include the revision, or filter on a `revision`/`deployment.environment` resource attribute) and put it in `{{args.service}}`. Argo Rollouts can pass the pod hash via `valueFrom.podTemplateHashValue` if you key on the rollout's unique label.

## 3. Gate on any custom metric

Any Parqtel/ParQL query works. Example — fail if p95 latency exceeds 300ms:

```yaml
metrics:
- name: p95-latency
  interval: 30s
  count: 5
  failureLimit: 2
  successCondition: "result[0] < 300"
  provider:
    prometheus:
      address: http://parqtel.parqtel.svc.cluster.local:8080
      query: "histogram_quantile(0.95, sum by (le, service.name) (rate(http_request_duration_bucket{service.name=\"{{args.service}}\"}[5m])))"
```

## Notes & limitations
- **Instant-query lookback:** `/api/v1/query` looks back 5 minutes (`query.lookback_delta_ns`). Use `initialDelay` so the first measurement has data, or use `provider.prometheus.rangeQuery` for an explicit window.
- **Empty results:** if a query returns no series (no traffic yet), `result[0]` is undefined and the measurement errors. Use `initialDelay` + a realistic `interval`.
- **Metrics only (Phase 0):** this integration uses the Prometheus provider, so it gates on **metrics**. Parqtel's **log** and **trace** search and **cross-signal** pipelines are not reachable through the stock Prometheus provider — that's the dedicated provider (Phase 2) / go-plugin (Phase 1) work in the integration roadmap.

## What's next
- **Phase 1 — go-plugin** (`rollouts-plugin-metric-parqtel`): out-of-tree plugin exposing Parqtel's log, trace, and PQL-pipeline queries as first-class analysis metrics. No Argo Rollouts core changes.
- **Phase 2 — native `parqtel` provider**: a first-class `provider: parqtel: {...}` in the Argo Rollouts CRD (like `skywalking`), with schema validation and dedicated docs.
