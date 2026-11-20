#!/usr/bin/env bash
set -euo pipefail

usage() {
  cat <<'EOF'
Usage: scripts/checks/ha_pair.sh [--help]

Check two StatefulSet replicas by sending tagged OTLP metrics through
pod-specific kubectl port-forwards, then deleting each pod in turn.

Environment:
  NAMESPACE          Kubernetes namespace (default: default)
  RELEASE            Helm release label (default: parqtel)
  STATEFULSET        StatefulSet name (default: value of RELEASE)
  KUBE_CONTEXT       kubectl context (default: current context)
  HTTP_PORT          Parqtel HTTP container port (default: 8080)
  ALERTMANAGER_URL   Optional Alertmanager base URL, e.g. http://localhost:9093
  WAIT_TIMEOUT       Pod readiness timeout in seconds (default: 120)
  FORWARD_TIMEOUT    Port-forward startup timeout in seconds (default: 30)

Example:
  NAMESPACE=monitoring RELEASE=odin STATEFULSET=odin-parqtel \
    KUBE_CONTEXT=k3d-uat ALERTMANAGER_URL=http://localhost:9093 \
    scripts/checks/ha_pair.sh

The script deletes both StatefulSet pods once. Do not run against production.
EOF
}

if [[ ${1:-} == --help || ${1:-} == -h ]]; then
  usage
  exit 0
fi
if (( $# > 0 )); then
  usage >&2
  exit 2
fi

NAMESPACE=${NAMESPACE:-default}
RELEASE=${RELEASE:-parqtel}
STATEFULSET=${STATEFULSET:-$RELEASE}
KUBE_CONTEXT=${KUBE_CONTEXT:-}
HTTP_PORT=${HTTP_PORT:-8080}
ALERTMANAGER_URL=${ALERTMANAGER_URL:-}
WAIT_TIMEOUT=${WAIT_TIMEOUT:-120}
FORWARD_TIMEOUT=${FORWARD_TIMEOUT:-30}
LOCAL_PORT_BASE=${LOCAL_PORT_BASE:-18080}
RUN_ID="$$-$RANDOM"
METRIC_NAME=ha_pair_probe
FORWARD_PIDS=()

die() {
  printf 'ERROR: %s\n' "$*" >&2
  exit 1
}

k() {
  if [[ -n $KUBE_CONTEXT ]]; then
    kubectl --context "$KUBE_CONTEXT" -n "$NAMESPACE" "$@"
  else
    kubectl -n "$NAMESPACE" "$@"
  fi
}

cleanup() {
  local pid
  for pid in "${FORWARD_PIDS[@]}"; do
    kill "$pid" 2>/dev/null || true
    wait "$pid" 2>/dev/null || true
  done
}
trap cleanup EXIT

check_alertmanager() {
  [[ -n $ALERTMANAGER_URL ]] || return 0
  local body remaining fingerprint
  local -A seen=()
  printf '  Check Alertmanager alerts for duplicate fingerprints\n'
  body=$(curl -fsS --connect-timeout 3 --max-time 10 \
    "${ALERTMANAGER_URL%/}/api/v2/alerts") || die 'Alertmanager query failed'
  remaining=$body
  while [[ $remaining =~ "fingerprint"[[:space:]]*:[[:space:]]*"([[:xdigit:]]+)" ]]; do
    fingerprint=${BASH_REMATCH[1]}
    [[ -z ${seen[$fingerprint]+x} ]] || die "duplicate Alertmanager fingerprint: $fingerprint"
    seen[$fingerprint]=1
    remaining=${remaining#*"fingerprint":"$fingerprint"}
  done
}

start_forward() {
  local slot=$1 pod=$2
  local local_port=$((LOCAL_PORT_BASE + slot))
  local deadline=$((SECONDS + FORWARD_TIMEOUT))
  k port-forward --address 127.0.0.1 "pod/$pod" "$local_port:$HTTP_PORT" \
    >/dev/null 2>&1 &
  FORWARD_PIDS[slot]=$!
  while (( SECONDS < deadline )); do
    if ! kill -0 "${FORWARD_PIDS[$slot]}" 2>/dev/null; then
      die "port-forward for $pod exited before becoming ready"
    fi
    if curl -fsS --connect-timeout 1 --max-time 2 \
      "http://127.0.0.1:$local_port/health" >/dev/null 2>&1; then
      return 0
    fi
    sleep 1
  done
  die "timed out starting port-forward for $pod"
}

stop_forward() {
  local slot=$1
  local pid=${FORWARD_PIDS[slot]:-}
  [[ -n $pid ]] || return 0
  kill "$pid" 2>/dev/null || true
  wait "$pid" 2>/dev/null || true
  FORWARD_PIDS[slot]=
}

wait_ready() {
  local pod=$1
  k wait --for=condition=Ready "pod/$pod" --timeout="${WAIT_TIMEOUT}s" \
    >/dev/null || die "pod $pod did not become Ready within ${WAIT_TIMEOUT}s"
}

assert_pair_ready() {
  local replicas ready
  replicas=$(k get statefulset "$STATEFULSET" -o jsonpath='{.spec.replicas}')
  ready=$(k get statefulset "$STATEFULSET" -o jsonpath='{.status.readyReplicas}')
  [[ $replicas == 2 && $ready == 2 ]] ||
    die "expected 2 ready replicas for StatefulSet $STATEFULSET; found replicas=${replicas:-0}, ready=${ready:-0}"
  local pod index
  for index in 0 1; do
    pod="$STATEFULSET-$index"
    [[ $(k get pod "$pod" -o jsonpath='{.status.conditions[?(@.type=="Ready")].status}') == True ]] ||
      die "pod $pod is not Ready"
  done
}

send_metric() {
  local slot=$1 pod=$2 phase=$3
  local local_port=$((LOCAL_PORT_BASE + slot))
  local timestamp payload
  timestamp="$(date +%s)000000000"
  payload=$(cat <<EOF
{"resourceMetrics":[{"scopeMetrics":[{"metrics":[{"name":"$METRIC_NAME","gauge":{"dataPoints":[{"timeUnixNano":"$timestamp","asDouble":1,"attributes":[{"key":"ha_pair_id","value":{"stringValue":"$RUN_ID"}},{"key":"ha_pair_phase","value":{"stringValue":"$phase"}},{"key":"ha_pair_replica","value":{"stringValue":"$pod"}}]}]}}]}]}]}
EOF
)
  curl -fsS --connect-timeout 3 --max-time 10 \
    -H 'Content-Type: application/json' --data-binary "$payload" \
    "http://127.0.0.1:$local_port/v1/metrics/json" >/dev/null ||
    die "metric ingest failed for $pod ($phase)"
}

assert_metric_visible() {
  local slot=$1 pod=$2 phase=$3
  local local_port=$((LOCAL_PORT_BASE + slot))
  local rates query_response query
  rates=$(curl -fsS --connect-timeout 3 --max-time 10 \
    "http://127.0.0.1:$local_port/api/v1/ingest_rates") ||
    die "ingest_rates query failed for $pod"
  [[ $rates =~ "metrics"[[:space:]]*:[[:space:]]*\{[^}]*"total"[[:space:]]*:[[:space:]]*([0-9]+) ]] ||
    die "ingest_rates response for $pod has no metrics signal"
  (( BASH_REMATCH[1] > 0 )) || die "metrics signal has no ingested points on $pod"

  query="$METRIC_NAME{ha_pair_id=\"$RUN_ID\",ha_pair_phase=\"$phase\",ha_pair_replica=\"$pod\"}"
  query_response=$(curl -fsS --connect-timeout 3 --max-time 10 --get \
    --data-urlencode "query=$query" \
    "http://127.0.0.1:$local_port/api/v1/query") ||
    die "PromQL query failed for $pod ($phase)"
  [[ $query_response =~ "status"[[:space:]]*:[[:space:]]*"success" &&
     $query_response == *"\"ha_pair_id\":\"$RUN_ID\""* &&
     $query_response == *"\"ha_pair_phase\":\"$phase\""* &&
     $query_response == *"\"ha_pair_replica\":\"$pod\""* ]] ||
    die "tagged metric not queryable on $pod ($phase)"
}

printf '[1/5] Assert exactly two ready StatefulSet replicas\n'
assert_pair_ready
POD_0="$STATEFULSET-0"
POD_1="$STATEFULSET-1"

printf '[2/5] Ingest tagged OTLP metrics directly to both pods\n'
start_forward 0 "$POD_0"
start_forward 1 "$POD_1"
send_metric 0 "$POD_0" initial
send_metric 1 "$POD_1" initial

printf '[3/5] Confirm both replicas report metrics and return tagged queries\n'
assert_metric_visible 0 "$POD_0" initial
assert_metric_visible 1 "$POD_1" initial

printf '[4/5] Delete replica 0; verify replica 1, Alertmanager, and replacement\n'
stop_forward 0
k delete pod "$POD_0" --wait=false >/dev/null
wait_ready "$POD_0"
send_metric 1 "$POD_1" failover
assert_metric_visible 1 "$POD_1" failover
check_alertmanager
start_forward 0 "$POD_0"
send_metric 0 "$POD_0" recovered
assert_metric_visible 0 "$POD_0" recovered

printf '[5/5] Delete replica 1; verify replica 0 and replica 1 recovery\n'
stop_forward 1
k delete pod "$POD_1" --wait=false >/dev/null
wait_ready "$POD_1"
send_metric 0 "$POD_0" second_failover
assert_metric_visible 0 "$POD_0" second_failover
start_forward 1 "$POD_1"
send_metric 1 "$POD_1" recovered
assert_metric_visible 1 "$POD_1" recovered
check_alertmanager

printf 'HA pair check passed. Run ID: %s\n' "$RUN_ID"
