#!/usr/bin/env bash
set -euo pipefail

TARGET_ENDPOINT="http://localhost:8080"
EXPECTED_METRIC="webservice.http.requests_total"
LOAD_RATE=1000
LOAD_TIME=2
EXPECTED_SAMPLES=$((LOAD_RATE * LOAD_TIME))

echo "=== 1. Launching Telemetry Ingestion Loop ==="
echo "Target: ${TARGET_ENDPOINT}"
echo "Rate: ${LOAD_RATE} samples/min"
echo "Duration: ${LOAD_TIME} minutes"
echo "Expected samples: ${EXPECTED_SAMPLES}"

# Start load test in background
make load-test LOAD_RATE="${LOAD_RATE}" LOAD_TIME="${LOAD_TIME}" > /tmp/load_test_output.log 2>&1 &
LOAD_PID=$!

echo "=== 2. Monitoring Process Execution Bounds (PID: ${LOAD_PID}) ==="
if ! wait "${LOAD_PID}"; then
    echo "❌ ERROR: Load test process failed with non-zero exit code"
    echo "📄 Load test output:"
    cat /tmp/load_test_output.log
    exit 1
fi
echo "✅ Load test process completed successfully (exit 0)"

# Check for network errors in output
if grep -qi "error\|timeout\|drop" /tmp/load_test_output.log; then
    echo "❌ ERROR: Network errors detected in load test output"
    grep -i "error\|timeout\|drop" /tmp/load_test_output.log || true
    exit 1
fi
echo "✅ No network errors detected in load test output"

echo "=== 3. Pipeline Ingestion Buffering (10s Settle Time) ==="
sleep 10

echo "=== 4. Executing Database Ingestion Verification Query ==="
RESPONSE=$(curl -s -X POST "${TARGET_ENDPOINT}/api/v1/query_range" \
  -H "Content-Type: application/json" \
  -d "{\"query\": \"${EXPECTED_METRIC}\", \"start\": $(date -d '2 minutes ago' +%s), \"end\": $(date +%s), \"step\": \"15s\"}" 2>/dev/null || echo '{"status":"error"}')

echo "=== 5. Running Automated Data Validation Checks ==="

# Check 1: Valid JSON response
if ! echo "$RESPONSE" | python3 -c "import sys,json; json.load(sys.stdin)" 2>/dev/null; then
    echo "❌ ERROR: Invalid JSON response from query endpoint"
    echo "📄 Server Output:"
    echo "$RESPONSE"
    exit 1
fi

# Check 2: Status is success
if ! echo "$RESPONSE" | grep -q '"status":"success"'; then
    echo "❌ ERROR: Query did not return success status"
    echo "📄 Server Output:"
    echo "$RESPONSE"
    exit 1
fi
echo "✅ Query returned success status"

# Check 3: Matrix data type
if ! echo "$RESPONSE" | grep -q '"resultType":"matrix"'; then
    echo "❌ ERROR: Expected matrix result type, got different type"
    echo "📄 Server Output:"
    echo "$RESPONSE"
    exit 1
fi
echo "✅ Query returned matrix result type"

# Check 4: Result is not empty
RESULT_COUNT=$(echo "$RESPONSE" | python3 -c "import sys,json; data=json.load(sys.stdin); print(len(data.get('data',{}).get('result',[])))" 2>/dev/null || echo "0")
if [ "$RESULT_COUNT" -eq 0 ]; then
    echo "❌ ERROR: No telemetry records found in query result"
    echo "📄 Server Output:"
    echo "$RESPONSE"
    exit 1
fi
echo "✅ Found ${RESULT_COUNT} metric series in result"

# Check 5: Schema validation - verify required labels exist
HAS_METHOD=$(echo "$RESPONSE" | python3 -c "
import sys, json
data = json.load(sys.stdin)
for r in data.get('data', {}).get('result', []):
    if 'method' in r.get('metric', {}):
        print('yes')
        sys.exit(0)
print('no')
" 2>/dev/null || echo "no")

HAS_STATUS=$(echo "$RESPONSE" | python3 -c "
import sys, json
data = json.load(sys.stdin)
for r in data.get('data', {}).get('result', []):
    if 'status_code' in r.get('metric', {}):
        print('yes')
        sys.exit(0)
print('no')
" 2>/dev/null || echo "no")

HAS_ENV=$(echo "$RESPONSE" | python3 -c "
import sys, json
data = json.load(sys.stdin)
for r in data.get('data', {}).get('result', []):
    if 'environment' in r.get('metric', {}):
        print('yes')
        sys.exit(0)
print('no')
" 2>/dev/null || echo "no")

if [ "$HAS_METHOD" != "yes" ]; then
    echo "❌ ERROR: Missing 'method' label in metric series"
    exit 1
fi
echo "✅ Schema validation: 'method' label present"

if [ "$HAS_STATUS" != "yes" ]; then
    echo "❌ ERROR: Missing 'status_code' label in metric series"
    exit 1
fi
echo "✅ Schema validation: 'status_code' label present"

if [ "$HAS_ENV" != "yes" ]; then
    echo "❌ ERROR: Missing 'environment' label in metric series"
    exit 1
fi
echo "✅ Schema validation: 'environment' label present"

# Check 6: Volume validation (±2% tolerance)
TOTAL_COUNT=$(echo "$RESPONSE" | python3 -c "
import sys, json
data = json.load(sys.stdin)
total = 0
for r in data.get('data', {}).get('result', []):
    for _, v in r.get('values', []):
        total += float(v)
print(int(total))
" 2>/dev/null || echo "0")

TOLERANCE=$((EXPECTED_SAMPLES * 2 / 100))
MIN_COUNT=$((EXPECTED_SAMPLES - TOLERANCE))
MAX_COUNT=$((EXPECTED_SAMPLES + TOLERANCE))

echo "Expected samples: ${EXPECTED_SAMPLES}"
echo "Actual samples: ${TOTAL_COUNT}"
echo "Acceptable range: ${MIN_COUNT} - ${MAX_COUNT} (±2%)"

if [ "$TOTAL_COUNT" -lt "$MIN_COUNT" ] || [ "$TOTAL_COUNT" -gt "$MAX_COUNT" ]; then
    echo "❌ ERROR: Sample count ${TOTAL_COUNT} outside acceptable range"
    exit 1
fi
echo "✅ Volume validation: ${TOTAL_COUNT} samples within ±2% tolerance"

echo ""
echo "=== 6. Sample Return Matrix (first 500 chars) ==="
echo "$RESPONSE" | cut -c1-500
echo ""

echo "=========================================="
echo "✅ ALL VALIDATION CHECKS PASSED"
echo "=========================================="
echo "Total samples ingested: ${TOTAL_COUNT}"
echo "Schema validation: PASSED"
echo "Volume validation: PASSED (±2% tolerance)"
echo "=========================================="

exit 0
