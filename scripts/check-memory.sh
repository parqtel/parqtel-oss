#!/usr/bin/env bash
set -euo pipefail

# This script checks the memory usage of a running parqtel server
# against a specified limit using its /metrics endpoint.

if [ "$#" -ne 2 ]; then
    echo "Usage: $0 <metrics_url> <limit_bytes>"
    exit 1
fi

METRICS_URL="$1"
LIMIT_BYTES="$2"

echo "Checking memory usage at $METRICS_URL against limit $LIMIT_BYTES bytes..."

METRICS=$(curl -s "$METRICS_URL")
if [ -z "$METRICS" ]; then
    echo "Error: Could not fetch metrics from $METRICS_URL"
    exit 1
fi

# Extract the value using grep and awk
RSS_BYTES=$(echo "$METRICS" | grep '^parqtel_process_rss_bytes' | awk '{print $2}')

if [ -z "$RSS_BYTES" ]; then
    echo "Error: Metric parqtel_process_rss_bytes not found in response."
    exit 1
fi

echo "Current RSS: $RSS_BYTES bytes"
echo "Limit:       $LIMIT_BYTES bytes"

if [ "$RSS_BYTES" -gt "$LIMIT_BYTES" ]; then
    echo "FAIL: Memory usage exceeds limit!"
    exit 1
else
    echo "PASS: Memory usage is within limits."
    exit 0
fi
