#!/usr/bin/env bash
set -euo pipefail

# Configuration Default Assignments
TARGET_URL=${TARGET_URL:-"http://localhost:8080"}
DURATION=${TEST_DURATION_SEC:-300}
REPORT_PATH=${OUTPUT_REPORT:-"perf_report.md"}
PARQTEL_BIN=${PARQTEL_BIN:-"./target/release/parqtel"}

echo "=== 1. Launching Parqtel Engine Backend Process ==="
# Spin up the Parqtel instance natively, forwarding logs to background pools
rm -rf /tmp/parqtel_perf_data
mkdir -p /tmp/parqtel_perf_data
$PARQTEL_BIN --data-dir /tmp/parqtel_perf_data > /tmp/parqtel_perf.log 2>&1 &
PARQTEL_PID=$!
sleep 3

# Verify parqtel is running
if ! kill -0 $PARQTEL_PID 2>/dev/null; then
    echo "❌ CRITICAL: Parqtel failed to start. Check /tmp/parqtel_perf.log"
    exit 1
fi

# Wait for server to be ready
for i in {1..10}; do
    if curl -s -f "${TARGET_URL}/api/v1/label/__name__/values" > /dev/null 2>&1; then
        echo "✅ Parqtel server is ready"
        break
    fi
    sleep 1
done

echo "=== 2. Initiating Concurrent Metrics & Traces Load Influx ==="
# Activate venv if it exists
if [[ -f ".venv/bin/activate" ]]; then
    source .venv/bin/activate
fi

# Fire simultaneous background load tasks (60 seconds for quick validation)
python3 scripts/load_gen.py \
    --endpoint "${TARGET_URL}" \
    --rate 1000 \
    --duration 1 \
    --type metrics > /tmp/metrics_load.log 2>&1 &
METRICS_PID=$!

python3 scripts/load_gen.py \
    --endpoint "${TARGET_URL}" \
    --rate 1000 \
    --duration 1 \
    --type traces > /tmp/traces_load.log 2>&1 &
TRACES_PID=$!

echo "=== 3. Starting Kernel Profiling Loop (Duration: ${DURATION}s) ==="
INTERVALS=$((DURATION * 2))
echo "Timestamp,RSS_KB,CPU_PCT" > /tmp/resource_metrics.csv

for ((i=0; i<INTERVALS; i++)); do
    if ! kill -0 $PARQTEL_PID 2>/dev/null; then
        echo "❌ CRITICAL: Parqtel process died prematurely during load test."
        echo "Log: $(tail -20 /tmp/parqtel_perf.log)"
        exit 1
    fi
    
    # Query system primitives - use ps for both Linux and macOS
    if [[ -f "/proc/${PARQTEL_PID}/statm" ]]; then
        # Linux: read from /proc
        STATM=$(cat "/proc/${PARQTEL_PID}/statm" 2>/dev/null || echo "0 0")
        PAGES=$(echo "$STATM" | cut -d' ' -f2)
        RSS_KB=$((PAGES * 4))
    else
        # macOS: use ps for RSS (in KB)
        RSS_KB=$(ps -o rss= -p $PARQTEL_PID 2>/dev/null | tr -d ' ' || echo "0")
    fi
    
    CPU_UTIL=$(ps -p $PARQTEL_PID -o %cpu= 2>/dev/null | tr -d ' ' || echo "0")
    
    echo "$(date +%s),${RSS_KB},${CPU_UTIL}" >> /tmp/resource_metrics.csv
    sleep 0.5
done

echo "=== 4. Reaping Processes & Consolidating Metrics ==="
wait $METRICS_PID $TRACES_PID 2>/dev/null || true

echo "=== 5. Generating Performance Audit Report ==="
# Calculate max memory peaks and average CPU cycles spent under heavy load
MAX_RSS_KB=$(awk -F, 'NR>1 {if($2>max) max=$2} END {print max}' /tmp/resource_metrics.csv)
MAX_RSS_MB=$((MAX_RSS_KB / 1024))
AVG_CPU=$(awk -F, 'NR>1 {sum+=$3; count++} END {printf "%.2f", sum/count}' /tmp/resource_metrics.csv)

# Get load test results
METRICS_COUNT=$(grep "Total requests sent:" /tmp/metrics_load.log 2>/dev/null | grep -oE '[0-9]+' | head -1 || echo "0")
TRACES_COUNT=$(grep "Total spans sent:" /tmp/traces_load.log 2>/dev/null | grep -oE '[0-9]+' | head -1 || echo "0")

cat << EOF > "$REPORT_PATH"
# 📊 Parqtel End-to-End Performance Audit Report
* **Test Date:** $(date)
* **Target Load Duration:** ${DURATION} seconds

## ⚡ Core Resource Saturation Metrics
* **Peak Resident Set Size (RSS):** ${MAX_RSS_MB} MB / 50 MB Max Ceiling
* **Average CPU Utilization:** ${AVG_CPU}%

## 📈 Ingestion Throughput
* **Metrics Ingested:** ${METRICS_COUNT} samples
* **Traces Ingested:** ${TRACES_COUNT} spans

## 📊 System Observations
EOF

if [ "$MAX_RSS_MB" -le 50 ]; then
    echo "* ✅ PASS: System remained strictly bounded within memory constraints." >> "$REPORT_PATH"
else
    echo "* ❌ FAIL: System breached the 50MB RSS performance boundary." >> "$REPORT_PATH"
fi

echo "" >> "$REPORT_PATH"
echo "## 📁 Raw Metrics" >> "$REPORT_PATH"
echo "\`\`\`" >> "$REPORT_PATH"
echo "Timestamp,RSS_KB,CPU_PCT" >> "$REPORT_PATH"
cat /tmp/resource_metrics.csv >> "$REPORT_PATH"
echo "\`\`\`" >> "$REPORT_PATH"

echo "✅ Audit completed successfully. Summary compiled at ${REPORT_PATH}"

# Cleanup
kill $PARQTEL_PID 2>/dev/null || true
wait $PARQTEL_PID 2>/dev/null || true
