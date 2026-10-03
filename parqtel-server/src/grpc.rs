//! OTLP gRPC ingestion server (tonic).
//!
//! Implements the three OpenTelemetry collector services —
//! `opentelemetry.proto.collector.{metrics,logs,trace}.v1` — so OTel SDKs and
//! collectors can export directly via the default gRPC endpoint (:4317)
//! without an HTTP bridge. Each handler reuses the same ingest path as the
//! protobuf HTTP endpoints (`ingest_proto`).

use crate::state::AppState;
use parqtel_ingest::otel::collector::logs::v1::logs_service_server::LogsService;
use parqtel_ingest::otel::collector::logs::v1::{
    ExportLogsServiceRequest, ExportLogsServiceResponse,
};
use parqtel_ingest::otel::collector::metrics::v1::metrics_service_server::MetricsService;
use parqtel_ingest::otel::collector::metrics::v1::{
    ExportMetricsServiceRequest, ExportMetricsServiceResponse,
};
use parqtel_ingest::otel::collector::trace::v1::trace_service_server::TraceService;
use parqtel_ingest::otel::collector::trace::v1::{
    ExportTraceServiceRequest, ExportTraceServiceResponse,
};
use tonic::{Request, Response, Status};
use tracing::warn;

/// Serves all three OTLP collector services over gRPC.
#[derive(Clone)]
pub struct OtlpGrpcService {
    state: AppState,
}

impl OtlpGrpcService {
    pub fn new(state: AppState) -> Self {
        Self { state }
    }

    /// Serves all three collector services on the given address.
    ///
    /// Message-size and concurrency limits are taken from the same config the
    /// HTTP path uses, so a batch that is accepted over HTTP is not rejected
    /// over gRPC with an opaque `ResourceExhausted` — tonic's own defaults
    /// (4 MiB decode) previously disagreed with `ingest.max_body_size`.
    pub async fn serve(
        state: AppState,
        addr: std::net::SocketAddr,
    ) -> Result<(), tonic::transport::Error> {
        use parqtel_ingest::otel::collector::logs::v1::logs_service_server::LogsServiceServer;
        use parqtel_ingest::otel::collector::metrics::v1::metrics_service_server::MetricsServiceServer;
        use parqtel_ingest::otel::collector::trace::v1::trace_service_server::TraceServiceServer;

        let limits = GrpcLimits::from_config(&state.inner.config);
        let svc = Self::new(state);
        tonic::transport::Server::builder()
            .concurrency_limit_per_connection(limits.concurrency_limit)
            // tonic 0.13 carries the message-size limits on the generated
            // service rather than the transport builder.
            .add_service(
                MetricsServiceServer::new(svc.clone())
                    .max_decoding_message_size(limits.max_message_bytes)
                    .max_encoding_message_size(limits.max_message_bytes),
            )
            .add_service(
                LogsServiceServer::new(svc.clone())
                    .max_decoding_message_size(limits.max_message_bytes)
                    .max_encoding_message_size(limits.max_message_bytes),
            )
            .add_service(
                TraceServiceServer::new(svc)
                    .max_decoding_message_size(limits.max_message_bytes)
                    .max_encoding_message_size(limits.max_message_bytes),
            )
            .serve(addr)
            .await
    }
}

/// Server limits applied to the OTLP gRPC listener.
#[derive(Debug, Clone, Copy)]
pub struct GrpcLimits {
    /// Maximum accepted request (and response) message size, in bytes.
    pub max_message_bytes: usize,
    /// Maximum concurrent in-flight requests per connection.
    pub concurrency_limit: usize,
}

impl GrpcLimits {
    /// Derives the limits from `ingest.max_body_size` and
    /// `server.grpc_concurrency_limit`.
    pub fn from_config(config: &parqtel_core::Config) -> Self {
        Self {
            max_message_bytes: config.ingest.max_body_size.max(1024),
            // Clamped to at least 1: a zero limit would reject every request.
            concurrency_limit: config.server.grpc_concurrency_limit.max(1),
        }
    }
}

#[tonic::async_trait]
impl MetricsService for OtlpGrpcService {
    async fn export(
        &self,
        request: Request<ExportMetricsServiceRequest>,
    ) -> Result<Response<ExportMetricsServiceResponse>, Status> {
        let body = prost::Message::encode_to_vec(&request.into_inner());
        let wire_bytes = body.len() as u64;
        match self
            .state
            .inner
            .ingestion_service
            .ingest_proto(bytes::Bytes::from(body))
            .await
        {
            Ok(count) => {
                use std::sync::atomic::Ordering;
                self.state
                    .inner
                    .metrics
                    .batches_received
                    .fetch_add(1, Ordering::Relaxed);
                self.state
                    .inner
                    .metrics
                    .ingested_points
                    .fetch_add(count, Ordering::Relaxed);
                self.state
                    .inner
                    .metrics
                    .rates
                    .record_metrics(count, wire_bytes);
                crate::otel_sli::record_ingest("metrics", count);
                tracing::debug!(
                    signal = "metrics",
                    ingested = count,
                    "gRPC metrics export accepted"
                );
                Ok(Response::new(ExportMetricsServiceResponse {
                    partial_success: None,
                }))
            }
            Err(e) => {
                warn!("gRPC metrics export rejected: {e}");
                Err(Status::invalid_argument(e.to_string()))
            }
        }
    }
}

#[tonic::async_trait]
impl LogsService for OtlpGrpcService {
    async fn export(
        &self,
        request: Request<ExportLogsServiceRequest>,
    ) -> Result<Response<ExportLogsServiceResponse>, Status> {
        let body = prost::Message::encode_to_vec(&request.into_inner());
        let wire_bytes = body.len() as u64;
        match self
            .state
            .inner
            .log_ingestion_service
            .ingest_proto(bytes::Bytes::from(body))
            .await
        {
            Ok(count) => {
                use std::sync::atomic::Ordering;
                self.state
                    .inner
                    .metrics
                    .batches_received
                    .fetch_add(1, Ordering::Relaxed);
                self.state
                    .inner
                    .metrics
                    .ingested_points
                    .fetch_add(count, Ordering::Relaxed);
                self.state
                    .inner
                    .metrics
                    .rates
                    .record_logs(count, wire_bytes);
                crate::otel_sli::record_ingest("logs", count);
                tracing::debug!(
                    signal = "logs",
                    ingested = count,
                    "gRPC logs export accepted"
                );
                Ok(Response::new(ExportLogsServiceResponse {
                    partial_success: None,
                }))
            }
            Err(e) => {
                warn!("gRPC logs export rejected: {e}");
                Err(Status::invalid_argument(e.to_string()))
            }
        }
    }
}

#[tonic::async_trait]
impl TraceService for OtlpGrpcService {
    async fn export(
        &self,
        request: Request<ExportTraceServiceRequest>,
    ) -> Result<Response<ExportTraceServiceResponse>, Status> {
        let body = prost::Message::encode_to_vec(&request.into_inner());
        let wire_bytes = body.len() as u64;
        match self
            .state
            .inner
            .trace_ingestion_service
            .ingest_proto(bytes::Bytes::from(body))
            .await
        {
            Ok(count) => {
                use std::sync::atomic::Ordering;
                self.state
                    .inner
                    .metrics
                    .batches_received
                    .fetch_add(1, Ordering::Relaxed);
                self.state
                    .inner
                    .metrics
                    .ingested_points
                    .fetch_add(count, Ordering::Relaxed);
                self.state
                    .inner
                    .metrics
                    .rates
                    .record_spans(count, wire_bytes);
                crate::otel_sli::record_ingest("traces", count);
                tracing::debug!(
                    signal = "traces",
                    ingested = count,
                    "gRPC traces export accepted"
                );
                Ok(Response::new(ExportTraceServiceResponse {
                    partial_success: None,
                }))
            }
            Err(e) => {
                warn!("gRPC trace export rejected: {e}");
                Err(Status::invalid_argument(e.to_string()))
            }
        }
    }
}

/// Spawns the OTLP gRPC server. Returns `Ok(None)` when gRPC is disabled
/// (empty `grpc_bind_address`), otherwise joins on the spawned task.
pub async fn serve_grpc(state: AppState, bind_address: &str) -> anyhow::Result<Option<()>> {
    if bind_address.is_empty() {
        tracing::info!("OTLP gRPC ingestion disabled (grpc_bind_address is empty)");
        return Ok(None);
    }
    let addr: std::net::SocketAddr = bind_address
        .parse()
        .map_err(|e| anyhow::anyhow!("invalid grpc_bind_address {bind_address:?}: {e}"))?;
    tracing::info!("OTLP gRPC ingestion listening on {addr}");
    OtlpGrpcService::serve(state, addr)
        .await
        .map_err(|e| anyhow::anyhow!("gRPC server error: {e}"))?;
    Ok(Some(()))
}

#[cfg(test)]
mod tests {
    #![allow(clippy::unwrap_used, clippy::expect_used, clippy::panic)]
    use super::*;

    #[test]
    fn limits_track_ingest_body_limit() {
        // The point of the change: a batch accepted over HTTP must also be
        // accepted over gRPC. tonic's own default was 4 MiB while
        // `ingest.max_body_size` defaulted to 10 MiB.
        let mut config = parqtel_core::Config::default();
        config.ingest.max_body_size = 10 * 1024 * 1024;
        let limits = GrpcLimits::from_config(&config);
        assert_eq!(limits.max_message_bytes, 10 * 1024 * 1024);
        assert_eq!(limits.concurrency_limit, 64);
    }

    #[test]
    fn degenerate_config_values_are_clamped() {
        // A zero concurrency limit would reject every request outright; a
        // zero body limit would make the listener unusable. Both must floor.
        let mut config = parqtel_core::Config::default();
        config.ingest.max_body_size = 0;
        config.server.grpc_concurrency_limit = 0;
        let limits = GrpcLimits::from_config(&config);
        assert_eq!(limits.max_message_bytes, 1024);
        assert_eq!(limits.concurrency_limit, 1);
    }
}
