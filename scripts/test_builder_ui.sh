#!/usr/bin/env bash
# ─────────────────────────────────────────────────────────────────────────────
# test_builder_ui.sh — headless-browser E2E of the metrics UI query builder.
#
# Drives the real UI (Chrome DevTools Protocol, no synthetic page state) and
# verifies: builder toggle, 92-function catalog, metric dropdown, live
# PromQL preview for every arg shape (range/scalar/grouping/window-fn),
# label-filter rows, and the bounded high-cardinality autocomplete
# (user_id: top-10 recent values; typing narrows server-side via match=).
#
# Usage:  scripts/test_builder_ui.sh [URL]     (default http://localhost:9090)
# Requires: make local-up, node ≥ 22 (global WebSocket), google-chrome.
# ─────────────────────────────────────────────────────────────────────────────
set -eu
URL="${1:-http://localhost:9090}"
HOST=$(echo "$URL" | sed -E 's#https?://##; s#:.*##')
PORT=$(echo "$URL" | sed -E 's#.*:##; s#/.*##')
PROXY_PORT=9099
DIR="$(cd "$(dirname "$0")/.." && pwd)"

command -v google-chrome >/dev/null || { echo "FATAL: google-chrome not found"; exit 1; }
command -v node >/dev/null || { echo "FATAL: node not found"; exit 2; }

curl -sf -o /dev/null "$URL/api/v1/label/__name__/values" || { echo "FATAL: parqtel not reachable at $URL"; exit 3; }

# Start the API-proxying page server (serves the real /ui while piping /api
# calls to parqtel so the page's own fetches work under test).
node "$DIR/scripts/lib/probe_server.js" "$URL" "$PROXY_PORT" &
PROXY_PID=$!
trap 'kill $PROXY_PID 2>/dev/null || true' EXIT
for i in $(seq 1 20); do curl -sf -o /dev/null "http://localhost:$PROXY_PORT/ui" && break; sleep 0.3; done
curl -sf -o /dev/null "http://localhost:$PROXY_PORT/ui" || { echo "FATAL: probe server failed to start"; exit 4; }

node "$DIR/scripts/test_builder_ui.js" "$PROXY_PORT"
echo "── round-trip regression (typed query ⇄ builder) ──"
node "$DIR/scripts/test_builder_ui_roundtrip.js" "$PROXY_PORT"
