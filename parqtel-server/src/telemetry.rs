use tracing_subscriber::{fmt, prelude::*, EnvFilter};

/// Initializes the global tracing subscriber based on the provided configuration.
pub fn init(log_level: &str, log_format: &str) {
    let filter = EnvFilter::try_from_default_env()
        .unwrap_or_else(|_| EnvFilter::new(log_level));

    let registry = tracing_subscriber::registry().with(filter);

    match log_format {
        "json" => {
            let layer = fmt::layer()
                .json()
                .with_timer(fmt::time::ChronoUtc::rfc_3339());
            registry.with(layer).init();
        }
        _ => {
            let layer = fmt::layer()
                .with_timer(fmt::time::ChronoUtc::rfc_3339());
            registry.with(layer).init();
        }
    }
}
