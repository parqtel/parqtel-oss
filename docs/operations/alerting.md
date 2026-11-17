# Alerting

Parqtel evaluates YAML alert rules against the live query engine, stores alert
instances with transition history, and delivers firing/resolved notifications
through an Alertmanager-compatible route tree.

![Alert delivery: rules and presets are evaluated with a dedicated scan slot; the state machine records transitions and noise score; the route tree applies matchers, grouping, silences and inhibition; the dispatcher retries to Slack, PagerDuty, Teams or a webhook through the egress policy](../assets/diagrams/alert-delivery.svg)

## Evaluation

- Rules load from `alerts.rules_dir` at startup and on change.
- The loop runs every `server.alert_interval_secs` (default 15 s).
- Alert evaluation reserves its **own** scan slot. It never waits behind
  dashboard queries; if the previous cycle still holds the slot, the cycle is
  skipped and `parqtel_alert_eval_skipped_total` increments.
- Threshold rules threshold the last sample of every result series. One alert
  instance exists per series, fingerprinted by rule id plus labels.
- Learned anomaly rules query a baseline window, require at least five samples,
  and fire when the latest sample deviates from the median by more than
  `sensitivity` MADs.
- State transitions: `Inactive → Pending → Firing → Resolved`.
  `for_duration_secs` is the sustain window before Pending → Firing.
- `noise_score` is derived from transition history: unacknowledged firings
  that later resolve raise it; acknowledged or still-firing alerts keep it low.
  It is an observability signal, not a delivery suppression input in this
  build.

Rule schema details are in
[Configuration](configuration.md#alert-rule-yaml). Built-in packs are embedded
and selected with `[alerts.presets]`; a rule with the same id in `rules_dir` or
via the API always wins.

## Delivery paths

There are two configuration shapes. Do not use both for the same event.

1. **`[alerting.routing]`** — preferred. Hierarchical routes, matchers,
   grouping, inhibition, silences, native receivers and retries.
2. **`[alerts.notifications.routes]`** — legacy severity/label webhook table.
   Used only when `[alerting.routing]` is absent.

If both are present, Parqtel logs a warning and disables the legacy routes.

## Route tree

```toml
[alerting.routing.route]
receiver = "slack-default"
group_by = ["service.name"]
group_wait = "30s"
group_interval = "5m"
repeat_interval = "4h"

[[alerting.routing.route.routes]]
matchers = ["severity = critical"]
receiver = "pagerduty-oncall"
continue = true

[[alerting.routing.route.routes]]
matchers = ["service.name =~ api-.*", "severity != info"]
receiver = "slack-api"
```

- Matchers use `=`, `!=`, `=~` (anchored) and `!~`; a missing label compares as
  `""`.
- Routes are evaluated depth-first. `continue = true` keeps evaluating sibling
  routes after a match.
- A child inherits `receiver`, `group_by` and intervals unless it overrides
  them.
- `group_by = ["..."]` disables grouping.
- Durations accept `ms`, `s`, `m`, `h`, `d`, `w` and compound forms.

## Grouping and retries

- A new group waits `group_wait` before its first notification.
- New alerts joining an active group wait at least `group_interval`.
- An unchanged firing group repeats every `repeat_interval`.
- Resolved alerts are delivered to receivers with `send_resolved = true`.
- Active groups are bounded by `max_active_groups`; oldest resolved/idle
  groups are evicted first.
- Transient failures (`429` honoring `Retry-After`, `5xx`, timeouts) retry with
  exponential backoff and jitter.
- `400`, `401`, `403` and `404` fail immediately.

## Inhibition and silences

Inhibition suppresses a target when an active source matches and every `equal`
label agrees:

```toml
[[alerting.routing.inhibit_rules]]
source_matchers = ["severity = critical"]
target_matchers = ["severity = warning"]
equal = ["service.name"]
```

Silences mute delivery only; alerts still fire and remain queryable. Create one
through the API:

```bash
curl -X POST http://localhost:8080/api/v1/alerts/silences \
  -H 'Content-Type: application/json' \
  -d '{"name":"maintenance","created_by":"oncall","match_labels":{"service.name":"api"},"starts_at":1760000000,"ends_at":1760003600}'
```

List with `GET /api/v1/alerts/silences`; delete with
`DELETE /api/v1/alerts/silences/{name}`.

## Receivers

```toml
[[alerting.routing.receivers]]
name = "slack-default"
[[alerting.routing.receivers.slack_configs]]
webhook_url = "${SLACK_WEBHOOK_URL}"
channel = "#alerts"
send_resolved = true

[[alerting.routing.receivers]]
name = "pagerduty-oncall"
[[alerting.routing.receivers.pagerduty_configs]]
routing_key = "${PAGERDUTY_ROUTING_KEY}"
severity = "critical"
send_resolved = true
```

| Channel | Fields |
|---|---|
| Slack | `webhook_url`, `channel`, `username`, `icon_emoji`, `send_resolved` |
| PagerDuty | `routing_key`, `url`, `severity`, `send_resolved` |
| Teams | `webhook_url`, `title`, `send_resolved` |
| Webhook | `url`, `headers`, `send_resolved` |

PagerDuty uses Events API v2 with a `dedup_key` per group: the first delivery
triggers, a resolved group sends `resolve`.

Secrets may be `${ENV_VAR}` or `{env:ENV_VAR}`. `GET /api/v1/alerts/routes`
returns the configured tree with secrets redacted.

## Egress policy

Every receiver is checked immediately before delivery:

```toml
[alerting.routing.egress]
allow_private = false
allow_hosts = ["*.internal.example"]
```

The default rejects loopback, private, link-local, CGNAT, ULA, multicast and
reserved addresses, and disables redirects. A receiver on an internal network
must opt in. The legacy router has its own
`[alerts.notifications.egress]` policy; set both when both paths are used.

## Testing a route

`POST /api/v1/alerts/test_route` dry-runs a label set through the tree and
reports the matched route, receiver, grouping key, silence and inhibition
state. It does not send a notification.

```bash
curl -X POST http://localhost:8080/api/v1/alerts/test_route \
  -H 'Content-Type: application/json' \
  -d '{"labels":{"severity":"critical","service.name":"api"}}'
```

Mutation endpoints that write notification routes, recording rules or
pipelines require:

```toml
[alerts]
allow_runtime_updates = true
```

Alert-rule and silence endpoints are not gated by this key; protect them with
`admin` auth or an authenticating proxy. Enable the gate only in the same
trusted context.

## Metrics

| Metric | Meaning |
|---|---|
| `parqtel_alert_notifications_total{receiver,channel,status}` | Delivery attempts |
| `parqtel_alert_notification_duration_seconds{receiver,channel}` | Delivery latency |
| `parqtel_alert_active_groups` | Current grouped alerts |
| `parqtel_alert_eval_skipped_total` | Evaluation cycles skipped because the dedicated slot was busy |
| `parqtel_alert_eval_duration_seconds` | Evaluation cycle duration |

Alert instances and transition history are available at `/api/v1/alerts` and
through the console. `parqtel-mcp-parqtel` exposes `get_alert_history` and
topology tools for agent workflows.

## Not implemented

- Automatic postmortem generation (`[alerts.postmortem]` is inert).
- Global noise suppression thresholds (`alerts.noise_suppression_threshold` is
  parsed but not read).
- A Kubernetes custom-metrics provider (`[k8s_provider]` is inert).
- Email and SMS receivers. Use a webhook bridge if required.
