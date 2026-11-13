#!/usr/bin/env python3
import sys
import json
import time
import uuid
import urllib.request
import urllib.error

def generate_log_payload(start_ts, count):
    records = []
    for i in range(count):
        ts = start_ts + (i * 1_000_000) # 1ms apart
        records.append({
            "timeUnixNano": str(ts),
            "severityNumber": 9 if i % 10 != 0 else 17, # INFO or ERROR
            "severityText": "INFO" if i % 10 != 0 else "ERROR",
            "body": {"stringValue": f"Operation successful for request_{uuid.uuid4().hex[:6]}" if i % 10 != 0 else "DB Connection Timeout"},
            "attributes": [
                {"key": "app.version", "value": {"stringValue": "1.2.3"}},
                {"key": "http.method", "value": {"stringValue": "POST" if i % 2 == 0 else "GET"}},
                {"key": "http.status_code", "value": {"stringValue": "200" if i % 10 != 0 else "500"}}
            ]
        })
        
    return {
        "resourceLogs": [{
            "resource": {
                "attributes": [
                    {"key": "service.name", "value": {"stringValue": "benchmark-service"}},
                    {"key": "k8s.namespace.name", "value": {"stringValue": "bench-ns"}},
                    {"key": "k8s.pod.name", "value": {"stringValue": f"pod-{uuid.uuid4().hex[:4]}"}}
                ]
            },
            "scopeLogs": [{
                "scope": {"name": "bench-scope", "version": "v1.0"},
                "logRecords": records
            }]
        }]
    }

def main():
    if len(sys.argv) < 3:
        print(f"Usage: {sys.argv[0]} <base_url> <total_logs> [batch_size]")
        sys.exit(1)

    base_url = sys.argv[1].rstrip('/')
    total_logs = int(sys.argv[2])
    batch_size = int(sys.argv[3]) if len(sys.argv) > 3 else 1000
    
    url = f"{base_url}/v1/logs/json"
    headers = {'Content-Type': 'application/json'}
    
    print(f"🚀 Starting log benchmark: {total_logs} logs to {url}...")
    
    logs_sent = 0
    start_time = time.time()
    
    while logs_sent < total_logs:
        current_batch = min(batch_size, total_logs - logs_sent)
        payload = generate_log_payload(time.time_ns(), current_batch)
        data = json.dumps(payload).encode('utf-8')
        
        req = urllib.request.Request(url, data=data, headers=headers, method='POST')
        try:
            with urllib.request.urlopen(req) as response:
                if response.status != 200:
                    print(f"❌ Error: {response.status}")
                    sys.exit(1)
        except urllib.error.URLError as e:
            print(f"❌ Request failed: {e}")
            sys.exit(1)
            
        logs_sent += current_batch
        if logs_sent % 5000 == 0 or logs_sent == total_logs:
            elapsed = time.time() - start_time
            rate = logs_sent / elapsed if elapsed > 0 else 0
            print(f"📦 Progress: {logs_sent}/{total_logs} logs sent ({rate:.0f} logs/s)")

    duration = time.time() - start_time
    print(f"\n✅ Benchmark complete!")
    print(f"⏱️  Total Duration: {duration:.2f}s")
    print(f"📊 Average Rate: {total_logs/duration:.0f} logs/s")

if __name__ == "__main__":
    main()
