#!/usr/bin/env python3
"""
Parqtel Unified Load Generator
Generates realistic web service metrics, logs, and traces via OTLP/HTTP for ingestion testing.

Usage:
  # Metrics only (default)
  python3 scripts/load_gen.py --endpoint http://localhost:8080/v1/metrics --rate 1000 --duration 10 --type metrics

  # Traces only
  python3 scripts/load_gen.py --endpoint http://localhost:8080/v1/traces --rate 1000 --duration 5 --type traces

  # Combined (metrics + traces)
  python3 scripts/load_gen.py --endpoint http://localhost:8080 --rate 1000 --duration 5 --type all

Notes:
  - On macOS with LibreSSL, urllib3 warnings are suppressed automatically
  - For metrics: requires opentelemetry-sdk, opentelemetry-exporter-otlp-proto-http
  - For traces: uses direct HTTP POST with JSON
"""

import argparse
import json
import math
import random
import sys
import time
import tracemalloc
import uuid
import warnings
from typing import Any, Dict, List, Tuple

# Suppress urllib3 LibreSSL warning on macOS
warnings.filterwarnings("ignore", message=".*LibreSSL.*", category=Warning)


def generate_trace_id() -> str:
    """Generate a random trace ID (16 bytes hex)."""
    return uuid.uuid4().hex


def generate_span_id() -> str:
    """Generate a random span ID (8 bytes hex)."""
    return uuid.uuid4().hex[:16]


def generate_span(
    trace_id: str,
    span_id: str,
    parent_span_id: str,
    span_name: str,
    span_kind: int,
    start_time_ns: int,
    duration_ns: int,
    attributes: Dict[str, str],
    status_code: int = 1,
    status_message: str = "",
) -> Dict[str, Any]:
    """Generate a single span."""
    end_time_ns = start_time_ns + duration_ns
    
    span = {
        "trace_id": trace_id,
        "span_id": span_id,
        "name": span_name,
        "kind": span_kind,
        "start_time_unix_nano": str(start_time_ns),
        "end_time_unix_nano": str(end_time_ns),
        "attributes": [
            {"key": k, "value": {"stringValue": v}} for k, v in attributes.items()
        ],
        "status": {
            "code": status_code,
            "message": status_message
        }
    }
    
    if parent_span_id:
        span["parent_span_id"] = parent_span_id
    
    return span


def generate_resource_spans(service_name: str, spans: List[Dict[str, Any]]) -> Dict[str, Any]:
    """Generate resource_spans structure."""
    return {
        "resource": {
            "attributes": [
                {"key": "service.name", "value": {"stringValue": service_name}},
                {"key": "deployment.environment", "value": {"stringValue": "production"}},
            ]
        },
        "scope_spans": [{
            "spans": spans
        }]
    }


def generate_resource_metrics(service_name: str, metrics: List[Dict[str, Any]]) -> Dict[str, Any]:
    """Generate resource_metrics structure."""
    return {
        "resource": {
            "attributes": [
                {"key": "service.name", "value": {"stringValue": service_name}},
                {"key": "deployment.environment", "value": {"stringValue": "production"}},
            ]
        },
        "scope_metrics": [{
            "metrics": metrics
        }]
    }


def generate_metric_payload(metrics: List[Dict[str, Any]]) -> Dict[str, Any]:
    """Generate full OTLP metrics JSON payload."""
    return {
        "resourceMetrics": [generate_resource_metrics("webservice-load-gen", metrics)]
    }


def generate_trace_payload(spans: List[Dict[str, Any]]) -> Dict[str, Any]:
    """Generate full OTLP traces JSON payload."""
    return {
        "resource_spans": [generate_resource_spans("webservice-load-gen", spans)]
    }


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Generate continuous OTLP telemetry loads for Parqtel ingestion testing"
    )
    parser.add_argument(
        "--endpoint",
        type=str,
        default="http://localhost:8080",
        help="Target OTLP endpoint URL (default: http://localhost:8080)",
    )
    parser.add_argument(
        "--rate",
        type=int,
        default=1000,
        help="Total telemetry samples per minute (metrics) or per second (traces) to emit (default: 1000)",
    )
    parser.add_argument(
        "--duration",
        type=int,
        default=10,
        help="Continuous load duration in minutes (default: 10)",
    )
    parser.add_argument(
        "--type",
        type=str,
        choices=["metrics", "traces", "all"],
        default="metrics",
        help="Type of telemetry to generate: metrics, traces, or all (default: metrics)",
    )
    return parser.parse_args()


class MetricsGenerator:
    """Generates realistic web service metrics with controlled distributions."""

    def __init__(self, endpoint: str, samples_per_minute: int):
        self.endpoint = endpoint
        self.samples_per_minute = samples_per_minute
        self.samples_per_second = samples_per_minute / 60.0
        self.interval_seconds = 60.0 / samples_per_minute

        # Start memory tracking
        tracemalloc.start()

        # Initialize OTLP exporter
        from opentelemetry.exporter.otlp.proto.http.metric_exporter import OTLPMetricExporter
        from opentelemetry.metrics import get_meter_provider, set_meter_provider
        from opentelemetry.sdk.metrics import MeterProvider
        from opentelemetry.sdk.metrics.export import PeriodicExportingMetricReader
        from opentelemetry.sdk.resources import Resource

        self.exporter = OTLPMetricExporter(endpoint=endpoint, timeout=30)
        self.reader = PeriodicExportingMetricReader(
            self.exporter,
            export_interval_millis=int(self.interval_seconds * 1000),
            export_timeout_millis=10000,
        )

        self.resource = Resource.create({
            "service.name": "webservice-load-generator",
            "deployment.environment": "production",
        })

        self.provider = MeterProvider(resource=self.resource, metric_readers=[self.reader])
        set_meter_provider(self.provider)
        self.meter = self.provider.get_meter("webservice", "1.0.0")

        # Initialize metrics
        self._init_metrics()

        # State tracking
        self.start_time = time.time()
        self.total_requests = 0
        self.error_count = 0
        self.last_report_time = self.start_time
        self.last_request_count = 0
        self.base_minute = 0

    def _init_metrics(self):
        """Initialize all metric instruments."""
        self.http_requests_counter = self.meter.create_counter(
            name="webservice.http.requests_total", description="Total number of HTTP requests", unit="1",
        )
        self.memory_gauge = self.meter.create_gauge(
            name="webservice.system.memory_utilization_bytes", description="Active memory allocation", unit="1",
        )
        self.request_duration_histogram = self.meter.create_histogram(
            name="webservice.http.request_duration_seconds", description="Server response latencies", unit="s",
        )
        self.active_sessions_histogram = self.meter.create_histogram(
            name="webservice.db.connection_pool.active_sessions", description="Active database sessions distribution", unit="1",
        )

    def _get_traffic_multiplier(self, current_minute: int) -> float:
        """Calculate traffic multiplier based on time-of-day pattern."""
        hour_of_day = (current_minute // 60) % 24
        if 9 <= hour_of_day <= 17:
            return 1.5 + 0.3 * math.sin(current_minute * math.pi / 120)
        return 1.0

    def _generate_http_request(self, current_minute: int) -> Tuple[str, int, float, bool]:
        """Generate a single HTTP request event with attributes."""
        traffic_mult = self._get_traffic_multiplier(current_minute)
        rand = random.random()

        method = "GET" if rand < 0.7 else "POST"

        if method == "POST" and rand < 0.025:
            status_code = 500
        elif rand < 0.02:
            status_code = 500
        elif rand < 0.05:
            status_code = 400
        else:
            status_code = 200

        if method == "POST" and status_code == 500:
            duration = random.lognormvariate(math.log(1.5), 0.8)
        else:
            duration = random.lognormvariate(math.log(0.08), 0.5)

        return method, status_code, duration, method == "POST" and status_code == 500

    def _generate_memory_value(self, elapsed_seconds: float) -> float:
        """Generate memory utilization with sine wave + random walk + slow leak."""
        sine_component = 100 * math.sin(elapsed_seconds * 2 * math.pi / 3600)
        random_walk = 20 * math.sin(elapsed_seconds * 0.01 + random.uniform(0, 1))
        leak = elapsed_seconds * 0.001
        base_memory = 500 * 1024 * 1024
        variation = sine_component + random_walk + leak
        return base_memory + variation

    def _generate_active_sessions(self) -> float:
        """Generate active database sessions with quantile distribution."""
        base = random.gauss(25, 10)
        return max(1, min(base, 150))

    def _export_metrics(self, method: str, status_code: int, duration: float, is_slow_db_block: bool, memory_bytes: float, active_sessions: float):
        """Export all metrics for a single request cycle."""
        self.http_requests_counter.add(1, attributes={"method": method, "status_code": str(status_code), "environment": "production"})
        self.memory_gauge.set(memory_bytes, attributes={"environment": "production"})
        self.request_duration_histogram.record(duration, attributes={"method": method, "status_code": str(status_code), "environment": "production"})
        self.active_sessions_histogram.record(active_sessions, attributes={"environment": "production"})

    def run(self, duration_minutes: int):
        """Run the load generator for the specified duration."""
        print(f"🚀 Starting metrics load generator")
        print(f"  Endpoint: {self.endpoint}")
        print(f"  Target rate: {self.samples_per_minute} samples/minute")
        print(f"  Duration: {duration_minutes} minutes")
        print()

        end_time = self.start_time + (duration_minutes * 60)
        sample_count = 0
        slow_db_blocks = 0

        try:
            while time.time() < end_time:
                current_minute = int((time.time() - self.start_time) // 60)
                elapsed = time.time() - self.start_time

                method, status_code, duration, is_slow_db_block = self._generate_http_request(current_minute)
                memory_bytes = self._generate_memory_value(elapsed)
                active_sessions = self._generate_active_sessions()

                self._export_metrics(method, status_code, duration, is_slow_db_block, memory_bytes, active_sessions)

                self.total_requests += 1
                sample_count += 1
                if is_slow_db_block:
                    slow_db_blocks += 1

                if time.time() - self.last_report_time >= 60:
                    current_time = time.time()
                    elapsed_since_report = current_time - self.last_report_time
                    samples_in_interval = sample_count - self.last_request_count
                    throughput = samples_in_interval / elapsed_since_report * 60
                    current_mem, peak_mem = tracemalloc.get_traced_memory()

                    print(f"[{time.strftime('%H:%M:%S')}] ✅ Requests: {self.total_requests:,} | Throughput: {throughput:.1f} samples/min | Errors: {self.error_count} | Slow DB blocks: {slow_db_blocks} | Memory: {current_mem / 1024 / 1024:.1f}MB (peak: {peak_mem / 1024 / 1024:.1f}MB)")

                    self.last_report_time = current_time
                    self.last_request_count = sample_count

                time.sleep(self.interval_seconds)

        except KeyboardInterrupt:
            print("\n🛑 Load generator interrupted by user")
        finally:
            current_mem, peak_mem = tracemalloc.get_traced_memory()
            print()
            print("=" * 60)
            print("📊 Load Test Summary")
            print("=" * 60)
            print(f"Total requests sent: {self.total_requests:,}")
            print(f"Slow DB blocks triggered: {slow_db_blocks}")
            print(f"Final memory usage: {current_mem / 1024 / 1024:.1f}MB")
            print(f"Peak memory usage: {peak_mem / 1024 / 1024:.1f}MB")
            print(f"Average throughput: {self.total_requests / (time.time() - self.start_time) * 60:.1f} samples/min")
            print("=" * 60)
            self.provider.shutdown()


class TracesGenerator:
    """Generates realistic distributed trace spans with controlled distributions."""

    def __init__(self, endpoint: str, spans_per_second: int):
        self.endpoint = endpoint
        self.spans_per_second = spans_per_second
        self.interval_seconds = 1.0 / spans_per_second

        tracemalloc.start()

        self.start_time = time.time()
        self.total_spans = 0
        self.error_count = 0
        self.last_report_time = self.start_time
        self.last_span_count = 0

    def _generate_span_batch(self, batch_size: int, base_time_ns: int) -> List[Dict[str, Any]]:
        """Generate a batch of spans."""
        spans = []
        for i in range(batch_size):
            trace_id = generate_trace_id()
            span_id = generate_span_id()
            parent_span_id = generate_span_id() if random.random() < 0.7 else ""

            rand = random.random()
            if rand < 0.4:
                span_kind, span_name = 2, "HTTP/GET"
            elif rand < 0.7:
                span_kind, span_name = 3, "HTTP/POST"
            elif rand < 0.9:
                span_kind, span_name = 1, "database.query"
            else:
                span_kind, span_name = 4, "message.publish"

            duration_ns = int(random.lognormvariate(-4.5, 0.8) * 1_000_000_000)
            duration_ns = max(100_000, min(duration_ns, 5_000_000_000))

            status_code = 2 if random.random() < 0.02 else 1
            status_message = "Internal Server Error" if status_code == 2 else ""

            attributes = {
                "http.method": "GET" if span_kind == 2 else "POST",
                "http.status_code": str(200 if status_code == 1 else 500),
                "db.system": "postgresql" if "database" in span_name else "http",
            }

            start_time_ns = base_time_ns + int(i * self.interval_seconds * 1_000_000_000)

            span = generate_span(
                trace_id=trace_id, span_id=span_id, parent_span_id=parent_span_id,
                span_name=span_name, span_kind=span_kind, start_time_ns=start_time_ns,
                duration_ns=duration_ns, attributes=attributes, status_code=status_code,
                status_message=status_message,
            )
            spans.append(span)
        return spans

    def _send_batch(self, spans: List[Dict[str, Any]]) -> bool:
        """Send a batch of spans to the endpoint."""
        import urllib.request
        import urllib.error

        payload = generate_trace_payload(spans)

        try:
            req = urllib.request.Request(
                self.endpoint, data=json.dumps(payload).encode('utf-8'),
                headers={'Content-Type': 'application/json'}, method='POST'
            )
            with urllib.request.urlopen(req, timeout=30) as response:
                return response.status == 200
        except urllib.error.URLError as e:
            self.error_count += 1
            return False

    def run(self, duration_minutes: int):
        """Run the load generator for the specified duration."""
        print(f"🚀 Starting traces load generator")
        print(f"  Endpoint: {self.endpoint}")
        print(f"  Target rate: {self.spans_per_second} spans/sec")
        print(f"  Duration: {duration_minutes} minutes")
        print()

        end_time = self.start_time + (duration_minutes * 60)
        span_count = 0
        batch_size = min(100, self.spans_per_second // 10)

        try:
            while time.time() < end_time:
                current_time = time.time()
                elapsed = current_time - self.start_time

                base_time_ns = int(current_time * 1_000_000_000)
                spans = self._generate_span_batch(batch_size, base_time_ns)

                if self._send_batch(spans):
                    self.total_spans += len(spans)
                    span_count += len(spans)
                else:
                    self.error_count += len(spans)

                if current_time - self.last_report_time >= 60:
                    elapsed_since_report = current_time - self.last_report_time
                    spans_in_interval = span_count - self.last_span_count
                    throughput = spans_in_interval / elapsed_since_report
                    current_mem, peak_mem = tracemalloc.get_traced_memory()

                    print(f"[{time.strftime('%H:%M:%S')}] ✅ Spans: {self.total_spans:,} | Throughput: {throughput:.1f} spans/sec | Errors: {self.error_count} | Memory: {current_mem / 1024 / 1024:.1f}MB (peak: {peak_mem / 1024 / 1024:.1f}MB)")

                    self.last_report_time = current_time
                    self.last_span_count = span_count

                time.sleep(self.interval_seconds)

        except KeyboardInterrupt:
            print("\n🛑 Load generator interrupted by user")
        finally:
            current_mem, peak_mem = tracemalloc.get_traced_memory()
            print()
            print("=" * 60)
            print("📊 Load Test Summary")
            print("=" * 60)
            print(f"Total spans sent: {self.total_spans:,}")
            print(f"Total errors: {self.error_count}")
            print(f"Final memory usage: {current_mem / 1024 / 1024:.1f}MB")
            print(f"Peak memory usage: {peak_mem / 1024 / 1024:.1f}MB")
            print(f"Average throughput: {self.total_spans / (time.time() - self.start_time):.1f} spans/sec")
            print("=" * 60)


def main():
    args = parse_args()

    if args.rate < 1:
        print("Error: Rate must be at least 1")
        sys.exit(1)

    if args.duration < 1:
        print("Error: Duration must be at least 1 minute")
        sys.exit(1)

    if args.type == "metrics":
        generator = MetricsGenerator(f"{args.endpoint}/v1/metrics", args.rate)
        generator.run(args.duration)
    elif args.type == "traces":
        # Convert rate from per-minute to per-second for traces
        traces_rate = args.rate // 60 if args.rate > 60 else 1
        generator = TracesGenerator(f"{args.endpoint}/v1/traces/json", traces_rate)
        generator.run(args.duration)
    elif args.type == "all":
        # Run both metrics and traces
        print("🚀 Starting unified load generator (metrics + traces)")
        print(f"  Metrics endpoint: {args.endpoint}/v1/metrics")
        print(f"  Traces endpoint: {args.endpoint}/v1/traces/json")
        print(f"  Target rate: {args.rate} samples/minute (metrics) + {args.rate // 60} spans/sec (traces)")
        print(f"  Duration: {args.duration} minutes")
        print()

        # Run traces in background, metrics in foreground
        import threading

        traces_rate = args.rate // 60 if args.rate > 60 else 1
        traces_gen = TracesGenerator(f"{args.endpoint}/v1/traces/json", traces_rate)
        traces_thread = threading.Thread(target=traces_gen.run, args=(args.duration,), daemon=True)
        traces_thread.start()

        metrics_gen = MetricsGenerator(f"{args.endpoint}/v1/metrics", args.rate)
        metrics_gen.run(args.duration)

        traces_thread.join(timeout=args.duration + 10)


if __name__ == "__main__":
    main()
