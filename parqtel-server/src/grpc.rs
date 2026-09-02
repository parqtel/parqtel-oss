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
    pub async fn serve(
        state: AppState,
        addr: std::net::SocketAddr,
    ) -> Result<(), tonic::transport::Error> {
        use parqtel_ingest::otel::collector::logs::v1::logs_service_server::LogsServiceServer;
        use parqtel_ingest::otel::collector::metrics::v1::metrics_service_server::MetricsServiceServer;
        use parqtel_ingest::otel::collector::trace::v1::trace_service_server::TraceServiceServer;

        let svc = Self::new(state);
        tonic::transport::Server::builder()
            .add_service(MetricsServiceServer::new(svc.clone()))
            .add_service(LogsServiceServer::new(svc.clone()))
            .add_service(TraceServiceServer::new(svc))
            .serve(addr)
            .await
    }
}

#[tonic::async_trait]
impl MetricsService for OtlpGrpcService {
    async fn export(
        &self,
        request: Request<ExportMetricsServiceRequest>,
    ) -> Result<Response<ExportMetricsServiceResponse>, Status> {
        let body = prost::Message::encode_to_vec(&request.into_inner());
        match self
            .state
            .inner
            .ingestion_service
            .ingest_proto(bytes::Bytes::from(body))
            .await
        {
            Ok(_count) => Ok(Response::new(ExportMetricsServiceResponse {
                partial_success: None,
            })),
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
        match self
            .state
            .inner
            .log_ingestion_service
            .ingest_proto(bytes::Bytes::from(body))
            .await
        {
            Ok(_count) => Ok(Response::new(ExportLogsServiceResponse {
                partial_success: None,
            })),
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
        match self
            .state
            .inner
            .trace_ingestion_service
            .ingest_proto(bytes::Bytes::from(body))
            .await
        {
            Ok(_count) => Ok(Response::new(ExportTraceServiceResponse {
                partial_success: None,
            })),
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
