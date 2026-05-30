#!/usr/bin/env bash
set -euo pipefail

# parqtel Development Environment Smoke Test
# This script verifies that the docker-compose environment starts correctly
# and that data is flowing through the system.

echo ">>> Starting parqtel development environment..."
docker compose -f infra/docker-compose.yml up -d --build

# Cleanup on exit
trap 'echo ">>> Tearing down environment..."; docker compose -f infra/docker-compose.yml down' EXIT

echo ">>> Waiting for parqtel to become healthy (up to 120s)..."
MAX_RETRIES=60
RETRY_COUNT=0
HEALTHY=false

until [ $RETRY_COUNT -ge $MAX_RETRIES ]; do
    if curl -s -f http://localhost:9090/api/v1/label/__name__/values > /dev/null; then
        echo ">>> parqtel is healthy!"
        HEALTHY=true
        break
    fi
    echo "...waiting..."
    sleep 2
    RETRY_COUNT=$((RETRY_COUNT+1))
done

if [ "$HEALTHY" = false ]; then
    echo "FAIL: parqtel failed to become healthy within 120s"
    docker compose -f infra/docker-compose.yml logs parqtel
    exit 1
fi

echo ">>> Waiting for load generator to send data (up to 60s)..."
RETRY_COUNT=0
DATA_FLOWING=false

until [ $RETRY_COUNT -ge 30 ]; do
    METRICS=$(curl -s http://localhost:9090/api/v1/label/__name__/values | jq -r '.data | length')
    if [ "$METRICS" -gt 0 ]; then
        echo ">>> Data is flowing! Found $METRICS unique metrics."
        DATA_FLOWING=true
        break
    fi
    echo "...waiting for data ($RETRY_COUNT/30)..."
    # Show logs of load-generator if data is not flowing
    if [ $((RETRY_COUNT % 5)) -eq 0 ] && [ $RETRY_COUNT -gt 0 ]; then
        echo ">>> Current load-generator logs:"
        docker compose -f infra/docker-compose.yml logs --tail 5 load-generator
    fi
    sleep 2
    RETRY_COUNT=$((RETRY_COUNT+1))
done

if [ "$DATA_FLOWING" = false ]; then
    echo "FAIL: No metrics found in parqtel after startup"
    echo ">>> parqtel logs:"
    docker compose -f infra/docker-compose.yml logs parqtel
    echo ">>> load-generator logs:"
    docker compose -f infra/docker-compose.yml logs load-generator
    exit 1
fi

echo ">>> Running sample range queries..."
NOW=$(date +%s)
START=$((NOW - 300)) # Last 5 minutes
for KIND in "http_requests_total_0" "system_metric_0" "latency_ms_0"; do
    echo "Checking metric: $KIND"
    # Query for the metric name
    RESPONSE=$(curl -s "http://localhost:9090/api/v1/query_range?query=$KIND&start=$START&end=$NOW&step=15s")
    STATUS=$(echo "$RESPONSE" | jq -r '.status')
    if [ "$STATUS" != "success" ]; then
        echo "FAIL: Query for $KIND failed. Response: $RESPONSE"
        exit 1
    fi
    RESULT_COUNT=$(echo "$RESPONSE" | jq -r '.data.result | length')
    echo "  OK (found $RESULT_COUNT series)"
done

echo ">>> Checking Grafana health..."
GRAFANA_STATUS=$(curl -s http://localhost:3000/api/health | jq -r '.database')
if [ "$GRAFANA_STATUS" != "ok" ]; then
    echo "FAIL: Grafana is not healthy"
    exit 1
fi
echo ">>> Grafana is healthy!"

echo "========================================"
echo "PASS: Development environment is working"
echo "========================================"
exit 0
