import time
import random
import json
import os
import sys
import urllib.request
import urllib.error
import math
from datetime import datetime

# Configuration
PARQTEL_URL = os.getenv("PARQTEL_URL", "http://parqtel:9090")
LOAD_TEST_MODE = os.getenv("LOAD_TEST_MODE", "false").lower() == "true"

# Scaling
NORMAL_SERIES = int(os.getenv("GENERATOR_NORMAL_SERIES", 1000))
NORMAL_RPS = int(os.getenv("GENERATOR_NORMAL_RPS", 167))
LOAD_SERIES = int(os.getenv("GENERATOR_LOAD_SERIES", 10000))
LOAD_RPS = int(os.getenv("GENERATOR_LOAD_RPS", 17000))

RESULTS_DIR = "/results"
CSV_FILE = os.path.join(RESULTS_DIR, "load_test_results.csv")

# State
counters = {}
histograms = {}

def get_config():
    if LOAD_TEST_MODE:
        return LOAD_SERIES, LOAD_RPS
    return NORMAL_SERIES, NORMAL_RPS

def generate_otel_json(series_count, timestamp_ns):
    # 1. Counters
    counter_metrics = []
    for m_idx in range(10):
        name = f"http_requests_total_{m_idx}"
        data_points = []
        series_per_metric = series_count // 40
        for s_idx in range(max(1, series_per_metric // 10)):
            labels = [
                {"key": "method", "value": {"string_value": random.choice(["GET", "POST", "PUT"])}},
                {"key": "status", "value": {"string_value": str(random.choice([200, 201, 400, 404, 500]))}},
                {"key": "instance", "value": {"string_value": f"inst-{s_idx}"}}
            ]
            fp = hash(tuple(sorted((l["key"], l["value"]["string_value"]) for l in labels)))
            count = counters.get(fp, 0) + random.randint(1, 10)
            counters[fp] = count
            
            data_points.append({
                "time_unix_nano": timestamp_ns,
                "value": {"as_int": count},
                "attributes": labels,
                "dropped_attributes_count": 0,
                "exemplars": [],
                "flags": 0,
                "start_time_unix_nano": 0
            })
        counter_metrics.append({
            "name": name,
            "description": "Counter description",
            "unit": "1",
            "sum": {
                "data_points": data_points,
                "aggregation_temporality": 1,
                "is_monotonic": True
            }
        })

    # 2. Gauges
    gauge_metrics = []
    for m_idx in range(15):
        name = f"system_metric_{m_idx}"
        data_points = []
        series_per_metric = series_count // 40
        for s_idx in range(max(1, series_per_metric // 15)):
            val = 50.0 + 20.0 * math.sin(time.time() / 60.0) + random.uniform(-5.0, 5.0)
            data_points.append({
                "time_unix_nano": timestamp_ns,
                "value": {"as_double": val},
                "attributes": [{"key": "host", "value": {"string_value": f"host-{s_idx}"}}],
                "dropped_attributes_count": 0,
                "exemplars": [],
                "flags": 0,
                "start_time_unix_nano": 0
            })
        gauge_metrics.append({
            "name": name,
            "description": "Gauge description",
            "unit": "1",
            "gauge": {"data_points": data_points}
        })

    # 3. Histograms
    hist_metrics = []
    buckets = [0.001, 0.005, 0.01, 0.025, 0.05, 0.1, 0.25, 0.5, 1, 2.5, 5, 10]
    for m_idx in range(5):
        name = f"latency_ms_{m_idx}"
        data_points = []
        series_per_metric = series_count // 40
        for s_idx in range(max(1, series_per_metric // 5)):
            fp = hash((name, s_idx))
            state = histograms.get(fp, {"count": 0, "sum": 0.0, "buckets": [0] * (len(buckets) + 1)})
            
            for _ in range(10):
                obs = random.expovariate(1.0 / 0.1)
                state["count"] += 1
                state["sum"] += obs
                for i, b in enumerate(buckets):
                    if obs <= b:
                        state["buckets"][i] += 1
                        break
                else:
                    state["buckets"][-1] += 1
            
            histograms[fp] = state
            data_points.append({
                "time_unix_nano": timestamp_ns,
                "count": state["count"],
                "sum": state["sum"],
                "bucket_counts": state["buckets"],
                "explicit_bounds": buckets,
                "attributes": [{"key": "service", "value": {"string_value": f"svc-{s_idx}"}}],
                "dropped_attributes_count": 0,
                "exemplars": [],
                "flags": 0,
                "start_time_unix_nano": 0,
                "min": 0.0,
                "max": 10.0
            })
        hist_metrics.append({
            "name": name,
            "description": "Histogram description",
            "unit": "ms",
            "histogram": {
                "data_points": data_points,
                "aggregation_temporality": 1
            }
        })

    # 4. Summaries
    sum_metrics = []
    for m_idx in range(3):
        name = f"request_size_bytes_{m_idx}"
        data_points = []
        series_per_metric = series_count // 40
        for s_idx in range(max(1, series_per_metric // 3)):
            data_points.append({
                "time_unix_nano": timestamp_ns,
                "count": 100,
                "sum": 50000.0,
                "quantile_values": [
                    {"quantile": 0.5, "value": 450.0},
                    {"quantile": 0.9, "value": 850.0},
                    {"quantile": 0.99, "value": 1200.0}
                ],
                "attributes": [{"key": "region", "value": {"string_value": random.choice(["us-east", "eu-west"])}}],
                "flags": 0,
                "dropped_attributes_count": 0,
                "start_time_unix_nano": 0
            })
        sum_metrics.append({
            "name": name,
            "description": "Summary description",
            "unit": "bytes",
            "summary": {"data_points": data_points}
        })

    return {
        "resource_metrics": [{
            "resource": {
                "attributes": [{"key": "service.name", "value": {"string_value": "load-generator"}}],
                "dropped_attributes_count": 0
            },
            "scope_metrics": [{
                "scope": {"name": "load-gen-scope", "version": "1.0", "attributes": [], "dropped_attributes_count": 0},
                "metrics": counter_metrics + gauge_metrics + hist_metrics + sum_metrics,
                "schema_url": ""
            }],
            "schema_url": ""
        }]
    }

def main():
    # Handle URL argument if provided
    global PARQTEL_URL
    if len(sys.argv) > 1 and sys.argv[1].startswith("http"):
        PARQTEL_URL = sys.argv[1].rstrip('/')

    series_count, rps = get_config()
    print(f"Starting load generator. Mode: {'LOAD TEST' if LOAD_TEST_MODE else 'NORMAL'}")
    print(f"Target: {series_count} series, {rps} samples/sec")
    print(f"Server URL: {PARQTEL_URL}")

    if LOAD_TEST_MODE:
        os.makedirs(RESULTS_DIR, exist_ok=True)
        with open(CSV_FILE, "w") as f:
            f.write("timestamp,samples_sent,rps,errors\n")

    total_sent = 0
    total_errors = 0
    start_time = time.time()
    last_stat_time = start_time
    last_csv_time = start_time
    points_since_stat = 0
    errors_since_stat = 0

    batch_interval = 1.0 # 1 second batches
    ramp_start = time.time()
    ramp_duration = 60.0

    try:
        while True:
            loop_start = time.time()
            
            current_rps = rps
            current_series = series_count
            if LOAD_TEST_MODE:
                elapsed = loop_start - ramp_start
                if elapsed < ramp_duration:
                    factor = elapsed / ramp_duration
                    current_rps = int(rps * factor)
                    current_series = int(series_count * factor)

            payload = generate_otel_json(current_series, time.time_ns())
            data = json.dumps(payload).encode('utf-8')
            
            req = urllib.request.Request(
                f"{PARQTEL_URL}/v1/metrics/json",
                data=data,
                headers={'Content-Type': 'application/json'},
                method='POST'
            )
            
            try:
                with urllib.request.urlopen(req) as res:
                    if res.status == 200:
                        batch_points = sum(len(m.get("sum", {}).get("data_points", []) or 
                                            m.get("gauge", {}).get("data_points", []) or
                                            m.get("histogram", {}).get("data_points", []) or
                                            m.get("summary", {}).get("data_points", []))
                                         for m in payload["resource_metrics"][0]["scope_metrics"][0]["metrics"])
                        total_sent += batch_points
                        points_since_stat += batch_points
                    else:
                        print(f"Server returned {res.status}: {res.read().decode()}", file=sys.stderr)
                        total_errors += 1
                        errors_since_stat += 1
            except urllib.error.HTTPError as e:
                # Read the response body for debugging
                body = e.read().decode()
                print(f"HTTP Error {e.code}: {body}", file=sys.stderr)
                total_errors += 1
                errors_since_stat += 1
            except Exception as e:
                print(f"Error sending batch: {e}", file=sys.stderr)
                total_errors += 1
                errors_since_stat += 1

            # Stats logging
            now = time.time()
            if now - last_stat_time >= 30:
                duration = now - last_stat_time
                actual_rps = points_since_stat / duration
                print(f"[{datetime.now().isoformat()}] Sent: {points_since_stat} | Rate: {actual_rps:.2f} pts/s | Errors: {errors_since_stat}")
                points_since_stat = 0
                errors_since_stat = 0
                last_stat_time = now

            if LOAD_TEST_MODE and now - last_csv_time >= 10:
                with open(CSV_FILE, "a") as f:
                    f.write(f"{now},{total_sent},{points_since_stat/10.0},{total_errors}\n")
                last_csv_time = now

            elapsed = time.time() - loop_start
            sleep_time = max(0, batch_interval - elapsed)
            time.sleep(sleep_time)

    except KeyboardInterrupt:
        print("Stopping load generator...")

if __name__ == "__main__":
    main()
