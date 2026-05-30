#!/usr/bin/env python3
"""
Parqtel E-Commerce Observability Test Data Generator

Generates realistic multi-signal telemetry (metrics, logs, traces, alerts) simulating
an e-commerce platform across 1 hour with sine-wave volume variation.

Usage:
  python3 scripts/ecommerce-load-gen.py --endpoint http://localhost:8080 --duration 60
"""

import argparse
import json
import math
import random
import sys
import time
import uuid
import urllib.request
import urllib.error
from datetime import datetime

# --- E-Commerce Service Topology ---
SERVICES = [
    "frontend-web", "api-gateway", "order-service", "payment-gateway-svc",
    "inventory-service", "cart-service", "user-service", "notification-svc",
    "shipping-service", "fraud-detection-svc",
]

REGIONS = ["us-east-1", "eu-west-1", "ap-southeast-1"]
PAYMENT_METHODS = ["stripe", "paypal", "apple_pay", "google_pay"]
PRODUCT_CATEGORIES = ["electronics", "clothing", "groceries", "home", "sports"]
WAREHOUSES = ["warehouse-east", "warehouse-west", "warehouse-eu"]
USER_TIERS = ["free", "premium", "enterprise"]
PLATFORMS = ["web", "ios", "android"]

LOG_BODIES_INFO = [
    "Order {order_id} placed successfully by user {user_id}",
    "Payment of ${amount} processed via {method} for order {order_id}",
    "Item added to cart: {product} (qty: {qty}) by user {user_id}",
    "User {user_id} logged in from {region}",
    "Inventory check passed for {product} in {warehouse}",
    "Shipping label generated for order {order_id}",
    "Email confirmation sent to user {user_id} for order {order_id}",
    "Session heartbeat: user {user_id} active on {platform}",
    "Cart updated: {qty} items, total ${amount}",
    "Stock replenishment scheduled for {product} at {warehouse}",
]

LOG_BODIES_WARN = [
    "Slow query detected: inventory lookup took {duration}ms for {product}",
    "Rate limit approaching for api-gateway: {rate}/1000 requests",
    "Retry attempt {attempt}/3 for payment processing order {order_id}",
    "Cart session timeout after 30s inactivity for user {user_id}",
    "High memory usage on {service}: {mem}% utilized",
    "Connection pool near capacity: {pool}/100 active connections",
]

LOG_BODIES_ERROR = [
    "Payment processing failed: timeout connecting to Stripe API for order {order_id}",
    "Inventory stockout: {product} unavailable in {warehouse}",
    "Authentication token expired for user {user_id}, session terminated",
    "Database connection refused: max retries exceeded on {service}",
    "Order {order_id} failed: insufficient funds for user {user_id}",
    "Fraud detection timeout: transaction {trace_id} not scored within SLA",
]

# --- Volume Profile (sine-wave with phases) ---
def get_volume_multiplier(minute: int) -> float:
    """Returns volume multiplier based on the 1-hour test profile."""
    if minute < 10:      # Warm-up
        return 0.5 + 0.3 * (minute / 10)
    elif minute < 15:    # Ramp
        return 0.8 + 1.2 * ((minute - 10) / 5)
    elif minute < 30:    # Flash sale peak
        return 2.0 + math.sin((minute - 15) * math.pi / 15)
    elif minute < 40:    # Cool-down
        return 2.0 - 1.0 * ((minute - 30) / 10)
    elif minute < 50:    # Second wave
        return 1.0 + 1.5 * math.sin((minute - 40) * math.pi / 10)
    else:                # Tail-off
        return 0.5 - 0.3 * ((minute - 50) / 10)


def send_json(url: str, payload: dict) -> bool:
    """Send JSON payload to endpoint."""
    try:
        data = json.dumps(payload).encode("utf-8")
        req = urllib.request.Request(url, data=data, headers={"Content-Type": "application/json"}, method="POST")
        with urllib.request.urlopen(req, timeout=30) as resp:
            return resp.status == 200
    except Exception as e:
        print(f"  ⚠️  Send failed: {e}")
        return False


def rand_order_id():
    return f"ORD-2024-{random.randint(10000, 99999):05d}"

def rand_user_id():
    return f"user_{uuid.uuid4().hex[:8]}"

def rand_trace_id():
    return uuid.uuid4().hex

def rand_span_id():
    return uuid.uuid4().hex[:16]


# --- Metrics Generation ---
def generate_metrics_batch(base_ts_ns: int, minute: int, multiplier: float) -> dict:
    """Generate a batch of e-commerce metrics for one time slice."""
    batch_size = int(20 * multiplier)
    metrics = []

    # Order placement rate
    order_rate = 50 * multiplier + random.gauss(0, 5)
    metrics.append(_gauge("order.placement.rate", max(0, order_rate), base_ts_ns, {
        "region": random.choice(REGIONS), "payment_method": random.choice(PAYMENT_METHODS),
    }))

    # Cart item count
    metrics.append(_gauge("cart.item.count", random.gauss(3.5, 1.5) * multiplier, base_ts_ns, {
        "user_tier": random.choice(USER_TIERS), "platform": random.choice(PLATFORMS),
    }))

    # Payment processing duration
    pay_dur = 200 if minute < 25 or minute > 35 else 200 + (minute - 25) * 1400 / 10
    metrics.append(_gauge("payment.processing.duration_ms", pay_dur + random.gauss(0, 20), base_ts_ns, {
        "provider": random.choice(PAYMENT_METHODS), "status": "success" if minute < 25 else "timeout",
    }))

    # Payment failure rate (spikes during TS-02 window)
    fail_rate = 0.02 if minute < 25 or minute > 35 else 0.15 + random.uniform(0, 0.1)
    metrics.append(_gauge("payment.failure.rate", fail_rate, base_ts_ns, {
        "provider": random.choice(PAYMENT_METHODS),
    }))

    # Inventory stock levels
    for cat in PRODUCT_CATEGORIES:
        stock = max(0, 500 - (minute * 8 if cat == "electronics" else minute * 2))
        metrics.append(_gauge("inventory.stock.level", stock + random.randint(-10, 10), base_ts_ns, {
            "product_category": cat, "warehouse": random.choice(WAREHOUSES),
        }))

    # API request duration per region
    for region in REGIONS:
        base_latency = {"us-east-1": 45, "eu-west-1": 120, "ap-southeast-1": 200}[region]
        latency = base_latency * (1 + 0.5 * (multiplier - 1)) + random.gauss(0, 10)
        metrics.append(_gauge("api.request.duration_ms", max(1, latency), base_ts_ns, {
            "region": region, "method": random.choice(["GET", "POST"]), "status_code": "200",
        }))

    # HTTP error rate
    error_rate = 0.01 * multiplier if minute < 25 or minute > 35 else 0.08
    metrics.append(_gauge("http_error_rate", error_rate, base_ts_ns, {
        "service": "api-gateway",
    }))

    # Cart abandonment rate (TS-11)
    abandon = 0.15 if minute < 35 else min(0.65, 0.15 + (minute - 35) * 0.05)
    metrics.append(_gauge("cart.abandonment.rate", abandon, base_ts_ns, {
        "platform": random.choice(PLATFORMS),
    }))

    # Order processing duration
    order_dur = 800 if minute < 15 else 800 + (multiplier - 1) * 2000
    metrics.append(_gauge("order.processing.duration_ms", order_dur + random.gauss(0, 50), base_ts_ns, {
        "status": "completed",
    }))

    # System metrics per service
    for svc in random.sample(SERVICES, min(5, len(SERVICES))):
        metrics.append(_gauge("service.cpu.utilization", random.uniform(10, 80) * multiplier / 2, base_ts_ns, {
            "service": svc,
        }))
        metrics.append(_gauge("service.memory.utilization_mb", random.uniform(128, 512), base_ts_ns, {
            "service": svc,
        }))

    # HTTP requests total
    metrics.append(_gauge("http_requests_total", int(1000 * multiplier + random.gauss(0, 50)), base_ts_ns, {
        "method": random.choice(["GET", "POST"]), "status": random.choice(["200", "200", "200", "500"]),
    }))

    return {
        "resourceMetrics": [{
            "resource": {"attributes": [
                {"key": "service.name", "value": {"stringValue": random.choice(SERVICES)}},
                {"key": "deployment.environment", "value": {"stringValue": "production"}},
                {"key": "k8s.namespace.name", "value": {"stringValue": "ecommerce-prod"}},
            ]},
            "scopeMetrics": [{"metrics": metrics}],
        }]
    }


def _gauge(name: str, value: float, ts_ns: int, labels: dict) -> dict:
    attrs = [{"key": k, "value": {"stringValue": str(v)}} for k, v in labels.items()]
    return {
        "name": name,
        "gauge": {"dataPoints": [{"timeUnixNano": str(ts_ns), "asDouble": value, "attributes": attrs}]},
    }


# --- Logs Generation ---
def generate_logs_batch(base_ts_ns: int, minute: int, multiplier: float) -> dict:
    """Generate a batch of e-commerce logs with severity distribution."""
    count = int(150 * multiplier)
    records = []

    for i in range(count):
        ts = base_ts_ns + i * 1_000_000  # 1ms apart
        rand_val = random.random()

        # Severity distribution: 90% INFO, 8% WARN, 2% ERROR (skewed during failure window)
        if minute >= 25 and minute <= 35:
            # During payment failure window, more errors
            if rand_val < 0.70:
                sev_num, sev_text = 9, "INFO"
            elif rand_val < 0.85:
                sev_num, sev_text = 13, "WARN"
            else:
                sev_num, sev_text = 17, "ERROR"
        else:
            if rand_val < 0.90:
                sev_num, sev_text = 9, "INFO"
            elif rand_val < 0.98:
                sev_num, sev_text = 13, "WARN"
            else:
                sev_num, sev_text = 17, "ERROR"

        # Pick body template based on severity
        ctx = {
            "order_id": rand_order_id(), "user_id": rand_user_id(),
            "amount": f"{random.uniform(9.99, 499.99):.2f}",
            "method": random.choice(PAYMENT_METHODS), "product": random.choice(PRODUCT_CATEGORIES),
            "qty": random.randint(1, 5), "region": random.choice(REGIONS),
            "warehouse": random.choice(WAREHOUSES), "platform": random.choice(PLATFORMS),
            "duration": random.randint(100, 5000), "rate": random.randint(500, 950),
            "attempt": random.randint(1, 3), "service": random.choice(SERVICES),
            "mem": random.randint(60, 95), "pool": random.randint(70, 99),
            "trace_id": rand_trace_id(),
        }

        if sev_text == "INFO":
            body = random.choice(LOG_BODIES_INFO).format(**ctx)
        elif sev_text == "WARN":
            body = random.choice(LOG_BODIES_WARN).format(**ctx)
        else:
            body = random.choice(LOG_BODIES_ERROR).format(**ctx)

        svc = random.choice(SERVICES)
        records.append({
            "timeUnixNano": str(ts),
            "severityNumber": sev_num,
            "severityText": sev_text,
            "body": {"stringValue": body},
            "attributes": [
                {"key": "service.name", "value": {"stringValue": svc}},
                {"key": "order.id", "value": {"stringValue": ctx["order_id"]}},
                {"key": "user.id", "value": {"stringValue": ctx["user_id"]}},
                {"key": "region", "value": {"stringValue": ctx["region"]}},
            ],
        })

    return {
        "resourceLogs": [{
            "resource": {"attributes": [
                {"key": "service.name", "value": {"stringValue": random.choice(SERVICES)}},
                {"key": "k8s.namespace.name", "value": {"stringValue": "ecommerce-prod"}},
                {"key": "k8s.pod.name", "value": {"stringValue": f"pod-{uuid.uuid4().hex[:6]}"}},
            ]},
            "scopeLogs": [{"logRecords": records}],
        }]
    }


# --- Traces Generation ---
def generate_traces_batch(base_ts_ns: int, minute: int, multiplier: float) -> dict:
    """Generate distributed traces simulating order placement flows."""
    trace_count = int(10 * multiplier)
    all_spans = []

    for _ in range(trace_count):
        trace_id = rand_trace_id()
        order_id = rand_order_id()
        user_id = rand_user_id()
        is_error = (minute >= 25 and minute <= 35 and random.random() < 0.3)

        # Root span: frontend-web POST /checkout
        root_span_id = rand_span_id()
        root_dur = int((2400 if not is_error else 15000) * 1_000_000)  # ns
        root_start = base_ts_ns + random.randint(0, 30_000_000_000)

        spans = [_span(trace_id, root_span_id, "", "POST /checkout", 2, root_start, root_dur,
                       "frontend-web", {"http.method": "POST", "http.url": "/checkout", "user.id": user_id, "order.id": order_id},
                       2 if is_error else 1)]

        # api-gateway span
        gw_span_id = rand_span_id()
        gw_start = root_start + 10_000_000
        gw_dur = root_dur - 50_000_000
        spans.append(_span(trace_id, gw_span_id, root_span_id, "route.checkout", 1, gw_start, gw_dur,
                           "api-gateway", {"http.route": "/checkout"}, 2 if is_error else 1))

        # order-service span
        order_span_id = rand_span_id()
        order_start = gw_start + 20_000_000
        order_dur = int(1800_000_000 * (1 if not is_error else 3))
        spans.append(_span(trace_id, order_span_id, gw_span_id, "create_order", 1, order_start, order_dur,
                           "order-service", {"order.id": order_id}, 2 if is_error else 1))

        # inventory-service span
        inv_span_id = rand_span_id()
        inv_start = order_start + 10_000_000
        inv_dur = 200_000_000
        spans.append(_span(trace_id, inv_span_id, order_span_id, "reserve_stock", 1, inv_start, inv_dur,
                           "inventory-service", {"product.category": random.choice(PRODUCT_CATEGORIES)}, 1))

        # payment-gateway-svc span
        pay_span_id = rand_span_id()
        pay_start = inv_start + inv_dur + 5_000_000
        pay_dur = int((1200 if not is_error else 14000) * 1_000_000)
        pay_status = 2 if is_error else 1
        pay_msg = "timeout connecting to Stripe API" if is_error else ""
        spans.append(_span(trace_id, pay_span_id, order_span_id, "charge_card", 3, pay_start, pay_dur,
                           "payment-gateway-svc", {"payment.method": random.choice(PAYMENT_METHODS), "payment.amount": f"{random.uniform(10,500):.2f}"},
                           pay_status, pay_msg))

        # fraud-detection-svc span (child of payment)
        fraud_span_id = rand_span_id()
        fraud_start = pay_start + 10_000_000
        fraud_dur = 150_000_000
        spans.append(_span(trace_id, fraud_span_id, pay_span_id, "score_transaction", 1, fraud_start, fraud_dur,
                           "fraud-detection-svc", {"fraud.score": str(random.uniform(0, 1))}, 1))

        # notification-svc span
        notif_span_id = rand_span_id()
        notif_start = order_start + order_dur - 150_000_000
        notif_dur = 100_000_000
        spans.append(_span(trace_id, notif_span_id, order_span_id, "send_confirmation", 1, notif_start, notif_dur,
                           "notification-svc", {"notification.type": "email"}, 1 if not is_error else 2))

        # cart-service span
        cart_span_id = rand_span_id()
        cart_start = gw_start + gw_dur - 60_000_000
        cart_dur = 50_000_000
        spans.append(_span(trace_id, cart_span_id, gw_span_id, "clear_cart", 1, cart_start, cart_dur,
                           "cart-service", {"user.id": user_id}, 1))

        all_spans.extend(spans)

    # Group spans by service for proper resource_spans structure
    by_service = {}
    for s in all_spans:
        svc = s.pop("_service")
        by_service.setdefault(svc, []).append(s)

    resource_spans = []
    for svc, spans in by_service.items():
        resource_spans.append({
            "resource": {"attributes": [
                {"key": "service.name", "value": {"stringValue": svc}},
                {"key": "deployment.environment", "value": {"stringValue": "production"}},
                {"key": "k8s.namespace.name", "value": {"stringValue": "ecommerce-prod"}},
            ]},
            "scope_spans": [{"spans": spans}],
        })

    return {"resource_spans": resource_spans}


def _span(trace_id, span_id, parent_span_id, name, kind, start_ns, dur_ns, service, attrs, status_code, status_msg=""):
    span = {
        "_service": service,
        "trace_id": trace_id, "span_id": span_id, "name": name, "kind": kind,
        "start_time_unix_nano": str(start_ns), "end_time_unix_nano": str(start_ns + dur_ns),
        "attributes": [{"key": k, "value": {"stringValue": str(v)}} for k, v in attrs.items()],
        "status": {"code": status_code, "message": status_msg},
    }
    if parent_span_id:
        span["parent_span_id"] = parent_span_id
    return span


# --- Main Execution ---
def main():
    parser = argparse.ArgumentParser(description="Parqtel E-Commerce Test Data Generator")
    parser.add_argument("--endpoint", default="http://localhost:8080", help="Parqtel base URL")
    parser.add_argument("--duration", type=int, default=60, help="Duration in minutes (default: 60)")
    args = parser.parse_args()

    base_url = args.endpoint.rstrip("/")
    metrics_url = f"{base_url}/v1/metrics/json"
    logs_url = f"{base_url}/v1/logs/json"
    traces_url = f"{base_url}/v1/traces/json"

    print("=" * 70)
    print("🛒 Parqtel E-Commerce Observability Test Data Generator")
    print("=" * 70)
    print(f"  Target:   {base_url}")
    print(f"  Duration: {args.duration} minutes")
    print(f"  Metrics:  {metrics_url}")
    print(f"  Logs:     {logs_url}")
    print(f"  Traces:   {traces_url}")
    print("=" * 70)
    print()

    start_time = time.time()
    total_metrics = 0
    total_logs = 0
    total_traces = 0
    errors = 0

    # Generate data spread across the full duration with 1-second ticks
    total_seconds = args.duration * 60
    for sec in range(total_seconds):
        minute = sec // 60
        multiplier = max(0.1, get_volume_multiplier(minute))
        ts_ns = int((start_time + sec) * 1_000_000_000)

        # Send metrics every 5 seconds
        if sec % 5 == 0:
            payload = generate_metrics_batch(ts_ns, minute, multiplier)
            if send_json(metrics_url, payload):
                total_metrics += len(payload["resourceMetrics"][0]["scopeMetrics"][0]["metrics"])
            else:
                errors += 1

        # Send logs every 2 seconds
        if sec % 2 == 0:
            payload = generate_logs_batch(ts_ns, minute, multiplier)
            if send_json(logs_url, payload):
                total_logs += len(payload["resourceLogs"][0]["scopeLogs"][0]["logRecords"])
            else:
                errors += 1

        # Send traces every 10 seconds
        if sec % 10 == 0:
            payload = generate_traces_batch(ts_ns, minute, multiplier)
            span_count = sum(len(rs["scope_spans"][0]["spans"]) for rs in payload["resource_spans"])
            if send_json(traces_url, payload):
                total_traces += span_count
            else:
                errors += 1

        # Progress report every 60 seconds
        if sec > 0 and sec % 60 == 0:
            elapsed = time.time() - start_time
            print(f"  [{minute:02d}m] ✅ Metrics: {total_metrics:,} | Logs: {total_logs:,} | Traces: {total_traces:,} | Errors: {errors} | Multiplier: {multiplier:.1f}x")

        # Small sleep to avoid overwhelming the server (but fast enough for backfill)
        time.sleep(0.05)

    elapsed = time.time() - start_time
    print()
    print("=" * 70)
    print("📊 Ingestion Complete")
    print("=" * 70)
    print(f"  Duration:      {elapsed:.1f}s")
    print(f"  Total Metrics: {total_metrics:,}")
    print(f"  Total Logs:    {total_logs:,}")
    print(f"  Total Traces:  {total_traces:,} spans")
    print(f"  Errors:        {errors}")
    print(f"  Avg Rate:      {(total_metrics + total_logs + total_traces) / elapsed:.0f} signals/sec")
    print("=" * 70)


if __name__ == "__main__":
    main()
