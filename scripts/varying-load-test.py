#!/usr/bin/env python3
import sys
import json
import time
import uuid
import math
import urllib.request
import urllib.error

def generate_metric_payload(start_ts, count):
    points = []
    for i in range(count):
        user_id = f"user_{uuid.uuid4().hex[:8]}"
        points.append({
            "timeUnixNano": str(start_ts + (i * 1_000_000)),
            "asDouble": float(i % 100),
            "attributes": [
                {"key": "user_id", "value": {"stringValue": user_id}},
                {"key": "region", "value": {"stringValue": f"region_{i % 5}"}}
            ]
        })
    return {
        "resourceMetrics": [{
            "resource": {"attributes": [{"key": "service.name", "value": {"stringValue": "trend-tester"}}]},
            "scopeMetrics": [{
                "metrics": [{
                    "name": "varying_metric",
                    "gauge": {"dataPoints": points}
                }]
            }]
        }]
    }

def generate_log_payload(start_ts, count):
    records = []
    for i in range(count):
        records.append({
            "timeUnixNano": str(start_ts + (i * 1_000_000)),
            "severityNumber": 9 if i % 10 != 0 else 17,
            "severityText": "INFO" if i % 10 != 0 else "ERROR",
            "body": {"stringValue": f"Dynamic event {uuid.uuid4().hex[:4]}"},
            "attributes": [{"key": "component", "value": {"stringValue": "worker"}}]
        })
    return {
        "resourceLogs": [{
            "resource": {"attributes": [{"key": "service.name", "value": {"stringValue": "trend-tester"}}]},
            "scopeLogs": [{"logRecords": records}]
        }]
    }

def send_request(url, payload):
    data = json.dumps(payload).encode('utf-8')
    req = urllib.request.Request(url, data=data, headers={'Content-Type': 'application/json'}, method='POST')
    try:
        with urllib.request.urlopen(req) as response:
            return response.status == 200
    except Exception as e:
        print(f"Request failed: {e}")
        return False

def main():
    base_url = "http://localhost:8080"
    duration_secs = 300  # 5 minutes
    start_time = time.time()
    
    print(f"🚀 Starting 5-minute varying load test to {base_url}...")
    
    while (time.time() - start_time) < duration_secs:
        elapsed = time.time() - start_time
        # Vary load using a sine wave + baseline
        # Amplitude of 800, baseline of 200 -> Range 200 to 1800 points per batch
        intensity = 1000 + 800 * math.sin(2 * math.pi * elapsed / 60) # 60s period
        batch_size = int(intensity)
        
        ts = time.time_ns()
        
        # Send Metrics
        send_request(f"{base_url}/v1/metrics/json", generate_metric_payload(ts, batch_size))
        # Send Logs
        send_request(f"{base_url}/v1/logs/json", generate_log_payload(ts, batch_size))
        
        print(f"📦 [{int(elapsed)}s] Ingesting {batch_size} signals (sine intensity: {intensity:.1f})")
        time.sleep(1) # Small pause to allow server to breathe and vary over time

    print("\n✅ Load test complete.")

if __name__ == "__main__":
    main()
