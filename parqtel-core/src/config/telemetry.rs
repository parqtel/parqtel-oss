use serde::{Deserialize, Serialize};

/// Configuration for logging and metrics.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct TelemetryConfig {
    /// Log level (trace, debug, info, warn, error).
    pub log_level: String,
    /// Log format (text or json).
    pub log_format: String,
    /// Export Parqtel's own traces + SLI metrics over OTLP/gRPC. Opt-in: a
    /// bare local run has no collector, so the default is off. The Helm chart
    /// enables it and points the endpoint at the in-cluster collector.
    #[serde(default)]
    pub otlp_enabled: bool,
    /// OTLP/gRPC endpoint for self-telemetry export.
    #[serde(default = "default_otlp_endpoint")]
    pub otlp_endpoint: String,
    /// Metric export interval in seconds (PeriodicReader).
    #[serde(default = "default_export_interval")]
    pub export_interval_secs: u64,
    /// Minimum span level exported over OTLP independently of the console log
    /// level. Keeps trace export working when the operator raises `log_level`
    /// to `warn` to quiet stdout.
    #[serde(default = "default_otlp_trace_level")]
    pub otlp_trace_level: String,
    /// Enable on-demand CPU profiling endpoints (/debug/pprof/*).
    ///
    /// Defaults to **off**: Parqtel ships without authentication, so a
    /// sampling profiler that walks process memory is an information-disclosure
    /// surface. Enable deliberately (per-environment) behind a network policy.
    #[serde(default = "default_false")]
    pub profiling_enabled: bool,
    /// CPU sampling frequency in Hz for the process-lifetime profiler.
    #[serde(default = "default_profiling_frequency")]
    pub profiling_frequency: i32,
}

fn default_false() -> bool {
    false
}

fn default_otlp_endpoint() -> String {
    "http://127.0.0.1:4317".into()
}

fn default_export_interval() -> u64 {
    30
}

fn default_otlp_trace_level() -> String {
    "info".into()
}

fn default_profiling_frequency() -> i32 {
    99
}

impl Default for TelemetryConfig {
    fn default() -> Self {
        Self {
            log_level: "info".into(),
            log_format: "text".into(),
            otlp_enabled: false,
            otlp_endpoint: default_otlp_endpoint(),
            export_interval_secs: default_export_interval(),
            otlp_trace_level: default_otlp_trace_level(),
            profiling_enabled: default_false(),
            profiling_frequency: default_profiling_frequency(),
        }
    }
}

/// Configuration for the Kubernetes Custom Metrics Provider.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct K8sProviderConfig {
    /// Whether the provider is enabled.
    pub enabled: bool,
    /// Address to bind the provider HTTPS listener.
    pub bind_address: String,
    /// Lookback window for metric queries (seconds).
    pub cache_expiry_secs: u64,
    /// Timeout for individual queries to parqtel storage (seconds).
    pub query_timeout_secs: u64,
    /// Maximum number of concurrent queries.
    pub max_concurrent: usize,
    /// Name of the Secret to store/load provider TLS certificates.
    pub tls_secret_name: String,
}

impl Default for K8sProviderConfig {
    fn default() -> Self {
        Self {
            enabled: false,
            bind_address: "0.0.0.0:6443".into(),
            cache_expiry_secs: 30,
            query_timeout_secs: 10,
            max_concurrent: 10,
            tls_secret_name: "parqtel-provider-tls".into(),
        }
    }
}

#[cfg(test)]
mod tests {
    #![allow(clippy::unwrap_used, clippy::expect_used, clippy::panic)]
    use super::*;

    #[test]
    fn telemetry_defaults_are_self_hosted_and_quiet() {
        let c = TelemetryConfig::default();
        assert_eq!(c.log_level, "info");
        assert_eq!(c.log_format, "text");
        // A bare local run has no collector, so OTLP export must be opt-in.
        assert!(!c.otlp_enabled);
        assert_eq!(c.otlp_endpoint, "http://127.0.0.1:4317");
        assert_eq!(c.export_interval_secs, 30);
        assert!(!c.profiling_enabled);
        assert_eq!(c.profiling_frequency, 99);
    }

    #[test]
    fn telemetry_serde_defaults_fill_absent_otlp_and_profiling_fields() {
        // Only the two required fields are supplied; the rest must fall back.
        let c: TelemetryConfig =
            serde_json::from_str(r#"{"log_level":"debug","log_format":"json"}"#)
                .expect("config must deserialize with only required fields");
        assert_eq!(c.log_level, "debug");
        assert_eq!(c.log_format, "json");
        assert!(!c.otlp_enabled);
        assert_eq!(c.otlp_endpoint, "http://127.0.0.1:4317");
        assert_eq!(c.export_interval_secs, 30);
        assert_eq!(c.otlp_trace_level, "info");
        assert!(!c.profiling_enabled);
        assert_eq!(c.profiling_frequency, 99);
    }

    #[test]
    fn telemetry_serde_honours_explicit_otlp_configuration() {
        let c: TelemetryConfig = serde_json::from_str(
            r#"{
                "log_level": "warn",
                "log_format": "json",
                "otlp_enabled": true,
                "otlp_endpoint": "http://collector:4317",
                "export_interval_secs": 15,
                "profiling_enabled": false,
                "profiling_frequency": 500
            }"#,
        )
        .expect("explicit config must deserialize");
        assert!(c.otlp_enabled);
        assert_eq!(c.otlp_endpoint, "http://collector:4317");
        assert_eq!(c.export_interval_secs, 15);
        assert!(!c.profiling_enabled);
        assert_eq!(c.profiling_frequency, 500);
    }
}
