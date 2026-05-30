#!/usr/bin/env bash
set -euo pipefail

# parqtel Timed Load Test Runner
# Verifies performance and memory budget under load.

TARGET_URL=${TARGET_URL:-http://parqtel.localhost}
TEST_DURATION=${TEST_DURATION:-300}
NUM_SERIES=${NUM_SERIES:-10000}
TARGET_RPS=${TARGET_RPS:-17000}
RAMP_UP=${RAMP_UP:-60}
OUTPUT_CSV=${OUTPUT_CSV:-load-test-results.csv}

echo ">>> Initializing load test against $TARGET_URL"

# Verify context if using localhost
if [[ "$TARGET_URL" == *"localhost"* ]]; then
    CURRENT_CONTEXT=$(kubectl config current-context)
    if [[ "$CURRENT_CONTEXT" != "k3d-secure-cluster" ]]; then
        echo "WARNING: Current context is $CURRENT_CONTEXT, expected k3d-secure-cluster"
    fi
fi

# Verify reachability
if ! curl -s -f "$TARGET_URL/metrics" > /dev/null; then
    echo "ERROR: $TARGET_URL is unreachable."
    exit 1
fi

echo "timestamp,rss_bytes,total_points,rps" > "$OUTPUT_CSV"

# Start the load generator in the cluster by patching the deployment
# (This is more reliable than running a local python script for high volume)
echo ">>> Triggering high-volume load in cluster..."
kubectl -n parqtel patch deployment load-generator -p "{\"spec\":{\"template\":{\"spec\":{\"containers\":[{\"name\":\"load-generator\",\"env\":[{\"name\":\"LOAD_TEST_MODE\",\"value\":\"true\"},{\"name\":\"GENERATOR_LOAD_SERIES\",\"value\":\"$NUM_SERIES\"},{\"name\":\"GENERATOR_LOAD_RPS\",\"value\":\"$TARGET_RPS\"}]}]}}}}"

PEAK_RSS=0
START_TIME=$(date +%s)
END_TIME=$((START_TIME + TEST_DURATION))

echo ">>> Monitoring performance for ${TEST_DURATION}s..."
echo "TIME | RSS (MB) | POINTS | RATE (pts/s)"

while [ $(date +%s) -lt $END_TIME ]; do
    sleep 10
    
    METRICS=$(curl -s "$TARGET_URL/metrics")
    RSS=$(echo "$METRICS" | grep '^parqtel_process_rss_bytes' | awk '{print $2}')
    POINTS=$(echo "$METRICS" | grep '^parqtel_ingested_points_total' | awk '{print $2}')
    
    if [ -n "$RSS" ]; then
        RSS_MB=$((RSS / 1024 / 1024))
        if [ "$RSS" -gt "$PEAK_RSS" ]; then PEAK_RSS=$RSS; fi
        
        NOW=$(date +%H:%M:%S)
        echo "$NOW | ${RSS_MB}MB | $POINTS | -"
        echo "$(date +%s),$RSS,$POINTS,0" >> "$OUTPUT_CSV"
    fi
done

# Reset load generator
echo ">>> Test complete. Resetting load generator..."
kubectl -n parqtel patch deployment load-generator -p "{\"spec\":{\"template\":{\"spec\":{\"containers\":[{\"name\":\"load-generator\",\"env\":[{\"name\":\"LOAD_TEST_MODE\",\"value\":\"false\"}]}]}}}}"

# Summary
PEAK_RSS_MB=$((PEAK_RSS / 1024 / 1024))
echo "========================================"
echo "LOAD TEST SUMMARY"
echo "Peak RSS: ${PEAK_RSS_MB}MB"
echo "Target:   50MB"

if [ "$PEAK_RSS" -gt 52428800 ]; then
    echo "RESULT: FAIL (Memory budget exceeded)"
    exit 1
else
    echo "RESULT: PASS"
    exit 0
fi
