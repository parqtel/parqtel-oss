#!/usr/bin/env python3
import sys
import json
import time
import urllib.request
import urllib.error

def generate_payload(start_ts, count):
    points = []
    # Generate monotonic increasing gauge
    for i in range(count):
        points.append({
            "timeUnixNano": str(start_ts + (i * 1_000_000_000)),
            "asDouble": float(i),
            "attributes": [{"key": "test_label", "value": {"stringValue": "load_test"}}]
        })
        
    return {
        "resourceMetrics": [{
            "resource": {"attributes": [{"key": "service.name", "value": {"stringValue": "load_tester"}}]},
            "scopeMetrics": [{
                "metrics": [{
                    "name": "load_test_metric",
                    "description": "Synthetic load test metric",
                    "unit": "1",
                    "data": {
                        "gauge": {
                            "dataPoints": points
                        }
                    }
                }]
            }]
        }]
    }

def main():
    if len(sys.argv) != 3:
        print(f"Usage: {sys.argv[0]} <base_url> <point_count>")
        sys.exit(1)

    base_url = sys.argv[1].rstrip('/')
    total_points = int(sys.argv[2])
    batch_size = 1000
    
    url = f"{base_url}/v1/metrics/json"
    headers = {'Content-Type': 'application/json'}
    
    print(f"Sending {total_points} points to {url} in batches of {batch_size}...")
    
    start_time = time.time_ns()
    points_sent = 0
    
    while points_sent < total_points:
        current_batch = min(batch_size, total_points - points_sent)
        payload = generate_payload(start_time + (points_sent * 1_000_000_000), current_batch)
        data = json.dumps(payload).encode('utf-8')
        
        req = urllib.request.Request(url, data=data, headers=headers, method='POST')
        try:
            with urllib.request.urlopen(req) as response:
                if response.status != 200:
                    print(f"Error: Server returned {response.status}")
                    sys.exit(1)
        except urllib.error.URLError as e:
            print(f"Request failed: {e}")
            sys.exit(1)
            
        points_sent += current_batch
        if points_sent % 10000 == 0 or points_sent == total_points:
            print(f"Progress: {points_sent}/{total_points} points sent.")

    print("Load test complete.")

if __name__ == "__main__":
    main()
