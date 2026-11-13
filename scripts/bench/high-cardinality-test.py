#!/usr/bin/env python3
import sys
import json
import time
import uuid
import urllib.request
import urllib.error

def generate_payload(start_ts, point_count, cardinality):
    metrics = []
    
    # Generate points with many unique 'user_id' values
    points = []
    for i in range(point_count):
        user_id = f"user_{uuid.uuid4().hex[:8]}"
        points.append({
            "timeUnixNano": str(start_ts + (i * 100_000_000)), # 100ms intervals
            "asDouble": float(i % 100),
            "attributes": [
                {"key": "user_id", "value": {"stringValue": user_id}},
                {"key": "region", "value": {"stringValue": f"region_{i % 5}"}},
                {"key": "status", "value": {"stringValue": "active" if i % 10 != 0 else "error"}}
            ]
        })
        
    return {
        "resourceMetrics": [{
            "resource": {"attributes": [{"key": "service.name", "value": {"stringValue": "high_cardinality_tester"}}]},
            "scopeMetrics": [{
                "metrics": [{
                    "name": "api_requests_total",
                    "description": "High cardinality test metric",
                    "unit": "1",
                    "gauge": {
                        "dataPoints": points
                    }
                }]
            }]
        }]
    }

def main():
    if len(sys.argv) < 3:
        print(f"Usage: {sys.argv[0]} <base_url> <total_points> [cardinality]")
        sys.exit(1)

    base_url = sys.argv[1].rstrip('/')
    total_points = int(sys.argv[2])
    cardinality = int(sys.argv[3]) if len(sys.argv) > 3 else total_points
    
    url = f"{base_url}/v1/metrics/json"
    headers = {'Content-Type': 'application/json'}
    
    print(f"Sending {total_points} points with high cardinality to {url}...")
    
    batch_size = 500
    points_sent = 0
    start_time_ns = time.time_ns()
    
    while points_sent < total_points:
        current_batch = min(batch_size, total_points - points_sent)
        payload = generate_payload(start_time_ns + (points_sent * 100_000_000), current_batch, cardinality)
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
        print(f"Progress: {points_sent}/{total_points} points sent.")

    print("High cardinality test complete.")

if __name__ == "__main__":
    main()
