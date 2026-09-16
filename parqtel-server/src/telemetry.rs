//! Self-observability for Parqtel: leveled console logs, OTLP traces, and
//! OTLP SLI metrics — plus a provider pair that is flushed on shutdown.
//!
//! Everything here is best-effort: if the OTLP endpoint is unreachable the
//! server still starts (with console logs only) rather than failing to boot.
//! Losing observability must never take down the observability engine.

use std::time::Duration;

use opentelemetry::global;
use opentelemetry::trace::TracerProvider as _;
use opentelemetry::KeyValue;
use opentelemetry_otlp::{MetricExporter, SpanExporter, WithExportConfig};
use opentelemetry_sdk::{
    metrics::{PeriodicReader, SdkMeterProvider, Temporality},
    resource::Resource,
    trace::{RandomIdGenerator, Sampler, SdkTracer, SdkTracerProvider},
};
use tracing_subscriber::{
    filter::LevelFilter, fmt, layer::SubscriberExt, util::SubscriberInitExt, EnvFilter, Layer,
};

use parqtel_core::config::TelemetryConfig;

/// Initializes the global tracing subscriber (console logs) and, when
/// `telemetry.otlp_enabled`, the OpenTelemetry SDK:
///
/// * **Logs** — `tracing` events at the configured level (`RUST_LOG` wins over
///   `telemetry.log_level`), rendered as text or JSON. Events emitted inside a
///   span automatically carry the active `trace_id`/`span_id`, so a log line
///   can be correlated with the exported trace.
/// * **Traces** — every `tracing` span is bridged to an OTel span and exported
///   in batches over OTLP/gRPC (`telemetry.otlp_endpoint`).
/// * **Metrics** — a `PeriodicReader` pushes SLI metrics (see [`crate::otel_sli`])
///   on the configured interval.
///
/// Returns a guard; call [`TelemetryGuard::shutdown`] on graceful shutdown to
/// flush in-flight telemetry before the process exits.
pub fn init(config: &TelemetryConfig) -> TelemetryGuard {
    // RUST_LOG (container/debug override) > telemetry.log_level (config).
    let filter =
        EnvFilter::try_from_default_env().unwrap_or_else(|_| EnvFilter::new(&config.log_level));
    let json = config.log_format.eq_ignore_ascii_case("json");

    let (guard, tracer) = if config.otlp_enabled {
        match init_otel(config) {
            Ok((guard, tracer)) => (guard, Some(tracer)),
            Err(e) => {
                // Console logging is not up yet, so report on stderr.
                eprintln!("parqtel: OpenTelemetry self-telemetry disabled: {e}");
                (TelemetryGuard::disabled(), None)
            }
        }
    } else {
        (TelemetryGuard::disabled(), None)
    };

    // The console filter honours RUST_LOG / `telemetry.log_level`; the OTLP span
    // filter is independent so raising the console level to `warn` does not
    // silently stop trace export.
    let otel_filter = config
        .otlp_trace_level
        .parse::<LevelFilter>()
        .unwrap_or(LevelFilter::INFO);

    match (json, tracer) {
        (true, Some(tracer)) => tracing_subscriber::registry()
            .with(
                fmt::layer()
                    .json()
                    .with_timer(fmt::time::ChronoUtc::rfc_3339())
                    .with_filter(filter),
            )
            .with(
                tracing_opentelemetry::layer()
                    .with_tracer(tracer)
                    .with_filter(otel_filter),
            )
            .init(),
        (false, Some(tracer)) => tracing_subscriber::registry()
            .with(
                fmt::layer()
                    .with_timer(fmt::time::ChronoUtc::rfc_3339())
                    .with_filter(filter),
            )
            .with(
                tracing_opentelemetry::layer()
                    .with_tracer(tracer)
                    .with_filter(otel_filter),
            )
            .init(),
        (true, None) => tracing_subscriber::registry()
            .with(
                fmt::layer()
                    .json()
                    .with_timer(fmt::time::ChronoUtc::rfc_3339())
                    .with_filter(filter),
            )
            .init(),
        (false, None) => tracing_subscriber::registry()
            .with(
                fmt::layer()
                    .with_timer(fmt::time::ChronoUtc::rfc_3339())
                    .with_filter(filter),
            )
            .init(),
    }

    guard
}

/// Builds the OTel resource describing this Parqtel instance. The `k8s.*`
/// attributes come from the downward-API env vars the Helm chart injects, so
/// exported telemetry is attributable to a specific pod/node.
fn build_resource() -> Resource {
    let mut builder = Resource::builder()
        .with_service_name("parqtel")
        .with_attribute(KeyValue::new("service.version", env!("CARGO_PKG_VERSION")))
        .with_attribute(KeyValue::new(
            "deployment.environment",
            std::env::var("PARQTEL_DEPLOYMENT_ENV").unwrap_or_else(|_| "k8s".into()),
        ));

    // Stable instance identity: explicit override, else the pod name (which is
    // what a container sees as HOSTNAME).
    if let Ok(instance) =
        std::env::var("PARQTEL_SERVICE_INSTANCE_ID").or_else(|_| std::env::var("HOSTNAME"))
    {
        builder = builder.with_attribute(KeyValue::new("service.instance.id", instance));
    }

    for (env_key, attr) in [
        ("K8S_POD_NAME", "k8s.pod.name"),
        ("K8S_NAMESPACE", "k8s.namespace.name"),
        ("K8S_NODE_NAME", "k8s.node.name"),
    ] {
        if let Ok(v) = std::env::var(env_key) {
            builder = builder.with_attribute(KeyValue::new(attr, v));
        }
    }

    builder.build()
}

fn init_otel(
    config: &TelemetryConfig,
) -> Result<(TelemetryGuard, SdkTracer), Box<dyn std::error::Error>> {
    let resource = build_resource();
    // A hung collector must not wedge a batch flush indefinitely.
    let timeout = Duration::from_secs(10);

    // ── Traces ─────────────────────────────────────────────────────────────
    let span_exporter = SpanExporter::builder()
        .with_tonic()
        .with_endpoint(config.otlp_endpoint.clone())
        .with_timeout(timeout)
        .build()?;
    let tracer_provider = SdkTracerProvider::builder()
        .with_batch_exporter(span_exporter)
        .with_resource(resource.clone())
        .with_sampler(Sampler::ParentBased(Box::new(Sampler::TraceIdRatioBased(
            1.0,
        ))))
        .with_id_generator(RandomIdGenerator::default())
        .build();
    let tracer = tracer_provider.tracer("parqtel");
    global::set_tracer_provider(tracer_provider.clone());

    // ── Metrics ────────────────────────────────────────────────────────────
    // Cumulative temporality: Parqtel's own storage is a Prometheus-style
    // time-series store, so counters must stay monotonic across exports.
    let metric_exporter = MetricExporter::builder()
        .with_tonic()
        .with_endpoint(config.otlp_endpoint.clone())
        .with_timeout(timeout)
        .with_temporality(Temporality::Cumulative)
        .build()?;
    let meter_provider = SdkMeterProvider::builder()
        .with_reader(
            PeriodicReader::builder(metric_exporter)
                .with_interval(Duration::from_secs(
                    config.export_interval_secs.clamp(5, 300),
                ))
                .build(),
        )
        .with_resource(resource)
        .build();
    global::set_meter_provider(meter_provider.clone());

    Ok((
        TelemetryGuard {
            tracer_provider: Some(tracer_provider),
            meter_provider: Some(meter_provider),
        },
        tracer,
    ))
}

/// Holds the live OTel providers so they can be flushed on shutdown.
pub struct TelemetryGuard {
    tracer_provider: Option<SdkTracerProvider>,
    meter_provider: Option<SdkMeterProvider>,
}

impl TelemetryGuard {
    fn disabled() -> Self {
        Self {
            tracer_provider: None,
            meter_provider: None,
        }
    }

    /// Whether OTLP self-telemetry is active.
    pub fn is_enabled(&self) -> bool {
        self.tracer_provider.is_some() || self.meter_provider.is_some()
    }

    /// Flush + shutdown both providers (idempotent, safe when disabled).
    /// Called on graceful shutdown so the final batch is not dropped.
    pub fn shutdown(&self) {
        if let Some(tp) = &self.tracer_provider {
            if let Err(e) = tp.shutdown() {
                tracing::warn!(error = %e, "tracer provider shutdown failed");
            }
        }
        if let Some(mp) = &self.meter_provider {
            if let Err(e) = mp.shutdown() {
                tracing::warn!(error = %e, "meter provider shutdown failed");
            }
        }
    }
}
