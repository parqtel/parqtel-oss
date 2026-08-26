use std::io::Result;
use std::path::Path;

fn main() -> Result<()> {
    let proto_dir = "proto";

    let metrics_proto = Path::new(proto_dir).join("opentelemetry/proto/metrics/v1/metrics.proto");
    let common_proto = Path::new(proto_dir).join("opentelemetry/proto/common/v1/common.proto");
    let resource_proto =
        Path::new(proto_dir).join("opentelemetry/proto/resource/v1/resource.proto");
    let collector_metrics_proto =
        Path::new(proto_dir).join("opentelemetry/proto/collector/metrics/v1/metrics_service.proto");

    // New log protos
    let logs_proto = Path::new(proto_dir).join("opentelemetry/proto/logs/v1/logs.proto");
    let collector_logs_proto =
        Path::new(proto_dir).join("opentelemetry/proto/collector/logs/v1/logs_service.proto");

    // Trace protos
    let traces_proto = Path::new(proto_dir).join("opentelemetry/proto/trace/v1/trace.proto");
    let collector_traces_proto =
        Path::new(proto_dir).join("opentelemetry/proto/collector/trace/v1/trace_service.proto");

    if metrics_proto.exists()
        && common_proto.exists()
        && resource_proto.exists()
        && collector_metrics_proto.exists()
        && logs_proto.exists()
        && collector_logs_proto.exists()
        && traces_proto.exists()
        && collector_traces_proto.exists()
    {
        let mut config = prost_build::Config::new();
        config.disable_comments(["."]); // Prevent proto comments from becoming invalid doc-tests

        config.compile_protos(
            &[
                metrics_proto,
                common_proto,
                resource_proto,
                collector_metrics_proto,
                logs_proto,
                collector_logs_proto,
                traces_proto,
                collector_traces_proto,
            ],
            &[proto_dir],
        )?;
    }

    Ok(())
}
