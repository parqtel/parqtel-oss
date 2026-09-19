#!/usr/bin/env bash
# End-to-end trigger test for the preset alert rules (rules/presets/).
#
# Boots a throwaway Parqtel, loads the shipped preset rules through the
# /api/v1/rules API, ingests metric data engineered to cross each rule's
# threshold, and asserts that the rules actually transition to Firing —
# then ingests clearing data and asserts they recover.
#
# The evaluation loop runs every 15s and scans a trailing 5-minute window,
# so the fire phase takes ~2.5 min (the gauge rules hold for 120s) and the
# recovery phase up to ~6.5 min (counter deltas must age out of the window).
# Total runtime is roughly 10 minutes.
#
# Usage: scripts/test_alert_presets.sh [parqtel-binary]
# Requires: curl, jq, python3 (the parqtel binary is built via `make build`).

set -euo pipefail

BIN="${1:-target/debug/parqtel}"
PORT="${PARQTEL_PORT:-9199}"
URL="http://127.0.0.1:${PORT}"
DATA_DIR="$(mktemp -d)"
PID=""

cleanup() {
  [[ -n "${PID}" ]] && kill "${PID}" 2>/dev/null || true
  rm -rf "${DATA_DIR}"
}
trap cleanup EXIT

log() { echo "[presets] $*"; }
fail() { echo "[presets] FAIL: $*" >&2; exit 1; }

# ── helpers ──────────────────────────────────────────────────────────────
wait_for_health() {
  for _ in $(seq 1 60); do
    curl -sf "${URL}/health" >/dev/null 2>&1 && return 0
    sleep 1
  done
  return 1
}

# POST one OTLP-JSON metrics payload.
ingest() { # payload_file
  curl -sf -X POST "${URL}/v1/metrics/json" -H 'Content-Type: application/json' \
    --data-binary "@$1" >/dev/null
}

# POST every preset rule file into the registry.
load_presets() {
  local n
  n="$(python3 - "${URL}" rules/presets/*.yaml <<'PY'
import sys, urllib.request, yaml
url = sys.argv[1] + "/api/v1/rules"
n = 0
for f in sys.argv[2:]:
    for d in yaml.safe_load_all(open(f)):
        if not d:
            continue
        body = yaml.safe_dump(d, sort_keys=False).encode()
        req = urllib.request.Request(url, data=body,
                                     headers={"Content-Type": "application/yaml"})
        with urllib.request.urlopen(req) as r:
            r.read()
        n += 1
print(n)
PY
)"
  log "loaded ${n} preset rules"
}

# Echo the state of a rule's instances, or "absent".
rule_state() { # rule_id
  curl -sf "${URL}/api/v1/alerts" \
    | jq -r --arg id "$1" '.data[]? | select(.rule_id==$id) | .state' \
    | sort -u | paste -sd, -
}

expect_firing() { # rule_id
  local s; s="$(rule_state "$1" | tr '[:upper:]' '[:lower:]')"
  case "$s" in *firing*) log "  ✓ $1 firing"; return 0;; esac
  log "  ✗ $1 state='${s:-absent}' (wanted firing)"
  return 1
}

expect_cleared() { # rule_id
  local s; s="$(rule_state "$1" | tr '[:upper:]' '[:lower:]')"
  case "$s" in ""|*resolved*) log "  ✓ $1 cleared"; return 0;; esac
  log "  ✗ $1 state='${s}' (wanted cleared/resolved)"
  return 1
}

now_ns() { python3 -c 'import time; print(int(time.time()*1_000_000_000))'; }

# Write a payload with one gauge point (attributes optional) to stdout.
gauge() { # metric value [key:value ...]
  local metric="$1" val="$2"; shift 2
  local attrs="[]"
  if (( $# )); then
    attrs=$(python3 - "$@" <<'PY'
import sys, json
attrs=[]
for kv in sys.argv[1:]:
    k,v = kv.split(":",1)
    attrs.append({"key":k,"value":{"string_value":v}})
print(json.dumps(attrs))
PY
)
  fi
  python3 - "$metric" "$val" "$attrs" "$(now_ns)" <<'PY'
import sys, json
name, val, attrs, ts = sys.argv[1:5]
print(json.dumps({"resource_metrics":[{"resource":{"attributes":[{"key":"service.name","value":{"string_value":"preset-test"}}],"dropped_attributes_count":0},"scope_metrics":[{"scope":{"name":"s","version":"1.0","attributes":[],"dropped_attributes_count":0},"metrics":[{"name":name,"description":"t","unit":"1","gauge":{"data_points":[{"time_unix_nano":int(ts),"value":{"as_double":float(val)},"attributes":json.loads(attrs),"dropped_attributes_count":0,"flags":0}]}}],"schema_url":""}],"schema_url":""}]}))
PY
}

# Write a payload appending one counter point (monotonic sum).
counter() { # metric value [key:value ...]
  local metric="$1" val="$2"; shift 2
  local attrs="[]"
  if (( $# )); then
    attrs=$(python3 - "$@" <<'PY'
import sys, json
attrs=[]
for kv in sys.argv[1:]:
    k,v = kv.split(":",1)
    attrs.append({"key":k,"value":{"string_value":v}})
print(json.dumps(attrs))
PY
)
  fi
  python3 - "$metric" "$val" "$attrs" "$(now_ns)" <<'PY'
import sys, json
name, val, attrs, ts = sys.argv[1:5]
print(json.dumps({"resource_metrics":[{"resource":{"attributes":[{"key":"service.name","value":{"string_value":"preset-test"}}],"dropped_attributes_count":0},"scope_metrics":[{"scope":{"name":"s","version":"1.0","attributes":[],"dropped_attributes_count":0},"metrics":[{"name":name,"description":"t","unit":"1","sum":{"data_points":[{"start_time_unix_nano":int(ts)-60_000_000_000,"time_unix_nano":int(ts),"value":{"as_double":float(val)},"attributes":json.loads(attrs),"exemplars":[],"flags":0,"aggregation_temporality":2,"is_monotonic":True}]}}],"schema_url":""}],"schema_url":""}]}))
PY
}

# ── boot ─────────────────────────────────────────────────────────────────
command -v jq  >/dev/null || fail "jq is required"
command -v python3 >/dev/null || fail "python3 is required"
[[ -x "${BIN}" ]] || { cargo build --bin parqtel 2>/dev/null || fail "cannot build ${BIN}"; }

log "starting parqtel (data ${DATA_DIR})"
PARQTEL_BIND="127.0.0.1:${PORT}" \
PARQTEL_DATA_DIR="${DATA_DIR}/data" \
PARQTEL_LOGS_DATA_DIR="${DATA_DIR}/logs" \
RUST_LOG=info \
  "${BIN}" serve >"${DATA_DIR}/parqtel.log" 2>&1 &
PID=$!
wait_for_health || { tail -20 "${DATA_DIR}/parqtel.log"; fail "parqtel did not become healthy"; }
log "healthy"

load_presets
sleep 2
RULE_COUNT=$(curl -sf "${URL}/api/v1/rules" | jq '.data | length')
log "registry holds ${RULE_COUNT} rules"
[[ "${RULE_COUNT}" -ge 30 ]] || fail "expected >=30 preset rules, got ${RULE_COUNT}"

# ── phase 1: fire ────────────────────────────────────────────────────────
log "phase 1 — driving thresholds to fire"
TMP="${DATA_DIR}/p.json"

# gauge > 0.92 (critical, for 120s) — plain selector
gauge k8s.node.cpu.utilization 0.97        >"${TMP}"; ingest "${TMP}"
# gauge with matcher > 0.95 (critical, for 120s)
gauge kubelet_volume_stats_utilization 0.98 persistentvolumeclaim:pv-1 >"${TMP}"; ingest "${TMP}"
# increase(counter[15m]) > 3 (for 0) — two rising samples
counter kube_pod_container_status_restarts_total 0 pod:web-0 >"${TMP}"; ingest "${TMP}"; sleep 1
counter kube_pod_container_status_restarts_total 7 pod:web-0 >"${TMP}"; ingest "${TMP}"
# increase(counter[5m]) > 0 (for 0) — CoreDNS panic
counter coredns_panics_total 0 >"${TMP}"; ingest "${TMP}"; sleep 1
counter coredns_panics_total 1 >"${TMP}"; ingest "${TMP}"
# increase < 1 (never-synced shape, for 0) — zero delta over the window
counter externalsecret_sync_calls_total 0 name:db-secret >"${TMP}"; ingest "${TMP}"; sleep 1
counter externalsecret_sync_calls_total 0 name:db-secret >"${TMP}"; ingest "${TMP}"

# the for_duration:0 rules need two evaluation cycles (Pending → Firing)
log "waiting for evaluation cycles (15s cadence)…"
FAILED=0
for _ in $(seq 1 12); do   # up to ~2.5 min
  sleep 15
  FAILED=0
  expect_firing k8s-node-cpu-saturation-critical   || FAILED=1
  expect_firing k8s-pvc-usage-critical             || FAILED=1
  expect_firing k8s-pod-high-restart-rate          || FAILED=1
  expect_firing coredns-panic                      || FAILED=1
  expect_firing external-secret-never-synced       || FAILED=1
  [[ ${FAILED} -eq 0 ]] && break
done
[[ ${FAILED} -eq 0 ]] || { tail -30 "${DATA_DIR}/parqtel.log"; fail "not all rules fired"; }

# ── phase 2: recover ────────────────────────────────────────────────────
log "phase 2 — clearing thresholds to recover"
gauge k8s.node.cpu.utilization 0.30 >"${TMP}"; ingest "${TMP}"
gauge kubelet_volume_stats_utilization 0.40 persistentvolumeclaim:pv-1 >"${TMP}"; ingest "${TMP}"
# Zero deltas: "no new restarts / no new panics". The phase-1 non-zero deltas
# remain in the 5-minute scan window, so these rules only clear once the old
# points age out — the loop below allows for that.
counter kube_pod_container_status_restarts_total 0 pod:web-0 >"${TMP}"; ingest "${TMP}"
counter coredns_panics_total 0 >"${TMP}"; ingest "${TMP}"
counter externalsecret_sync_calls_total 9 name:db-secret >"${TMP}"; ingest "${TMP}"

CLEARED=0
for _ in $(seq 1 26); do   # up to ~6.5 min; counters need the 5-min scan window to roll
  sleep 15
  CLEARED=0
  expect_cleared k8s-node-cpu-saturation-critical   || CLEARED=1
  expect_cleared k8s-pvc-usage-critical             || CLEARED=1
  expect_cleared k8s-pod-high-restart-rate          || CLEARED=1
  expect_cleared coredns-panic                      || CLEARED=1
  expect_cleared external-secret-never-synced       || CLEARED=1
  [[ ${CLEARED} -eq 0 ]] && break
done
[[ ${CLEARED} -eq 0 ]] || fail "alerts did not clear"

log ""
log "══════════════════════════════════════════════"
log "  PASS — preset rules fire and recover end-to-end"
log "══════════════════════════════════════════════"
