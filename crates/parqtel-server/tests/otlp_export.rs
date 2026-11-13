//! End-to-end SDK export check, isolated from unit-test global subscribers.
#![allow(clippy::unwrap_used, clippy::expect_used, clippy::panic)]

use parqtel_ingest::otel::collector::trace::v1::{
    trace_service_server::{TraceService, TraceServiceServer},
    ExportTraceServiceRequest, ExportTraceServiceResponse,
};
use std::{
    process::{Child, Command},
    time::Duration,
};
use tokio::{
    io::{AsyncReadExt, AsyncWriteExt},
    net::{TcpListener, TcpStream},
    sync::mpsc,
};
use tonic::{Request, Response, Status};

struct Receiver(mpsc::UnboundedSender<ExportTraceServiceRequest>);

#[tonic::async_trait]
impl TraceService for Receiver {
    async fn export(
        &self,
        request: Request<ExportTraceServiceRequest>,
    ) -> Result<Response<ExportTraceServiceResponse>, Status> {
        let _ = self.0.send(request.into_inner());
        Ok(Response::new(ExportTraceServiceResponse::default()))
    }
}

struct Process(Child);
impl Drop for Process {
    fn drop(&mut self) {
        let _ = self.0.kill();
        let _ = self.0.wait();
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn http_request_exports_span_over_otlp_with_trace_filter() {
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let collector_addr = listener.local_addr().unwrap();
    let incoming = futures::stream::unfold(listener, |listener| async {
        let accepted = listener.accept().await.map(|(socket, _)| socket);
        Some((accepted, listener))
    });
    let (tx, mut rx) = mpsc::unbounded_channel();
    let (stop_tx, stop_rx) = tokio::sync::oneshot::channel();
    let collector = tokio::spawn(async move {
        tonic::transport::Server::builder()
            .add_service(TraceServiceServer::new(Receiver(tx)))
            .serve_with_incoming_shutdown(incoming, async {
                let _ = stop_rx.await;
            })
            .await
            .unwrap();
    });

    let port_reservation = std::net::TcpListener::bind("127.0.0.1:0").unwrap();
    let http_addr = port_reservation.local_addr().unwrap();
    let temp = tempfile::tempdir().unwrap();
    let config = temp.path().join("test.toml");
    std::fs::write(
        &config,
        format!(
            r#"
[server]
bind_address = "{http_addr}"
grpc_bind_address = ""
[storage]
data_dir = "{}/metrics"
[logs]
data_dir = "{}/logs"
[telemetry]
log_level = "warn"
log_format = "json"
otlp_enabled = true
otlp_endpoint = "http://{collector_addr}"
otlp_trace_level = "trace"
export_interval_secs = 300
profiling_enabled = false
"#,
            temp.path().display(),
            temp.path().display()
        ),
    )
    .unwrap();
    let output = std::fs::File::create(temp.path().join("server.log")).unwrap();
    drop(port_reservation);
    let mut command = Command::new(env!("CARGO_BIN_EXE_parqtel"));
    // Developer shell settings must not override the test's TOML config.
    for (key, _) in std::env::vars().filter(|(k, _)| k.starts_with("PARQTEL_") || k == "RUST_LOG") {
        command.env_remove(key);
    }
    let mut server = Process(
        command
            .arg("--config")
            .arg(&config)
            .arg("serve")
            .current_dir(temp.path())
            .stdout(output.try_clone().unwrap())
            .stderr(output)
            .spawn()
            .unwrap(),
    );
    tokio::time::timeout(Duration::from_secs(20), async {
        loop {
            assert!(
                server.0.try_wait().unwrap().is_none(),
                "server exited: {}",
                std::fs::read_to_string(temp.path().join("server.log")).unwrap()
            );
            if let Ok(mut stream) = TcpStream::connect(http_addr).await {
                stream
                    .write_all(
                        b"GET /health HTTP/1.1\r\nHost: localhost\r\nConnection: close\r\n\r\n",
                    )
                    .await
                    .unwrap();
                let mut response = String::new();
                stream.read_to_string(&mut response).await.unwrap();
                assert!(response.starts_with("HTTP/1.1 200"), "{response}");
                break;
            }
            tokio::time::sleep(Duration::from_millis(50)).await;
        }
    })
    .await
    .expect("HTTP server must start");

    tokio::time::timeout(Duration::from_secs(20), async {
        while let Some(request) = rx.recv().await {
            for resource in request.resource_spans {
                for scope in resource.scope_spans {
                    for span in scope.spans {
                        if span.name == "request" {
                            assert_eq!(span.trace_id.len(), 16);
                            assert!(span.trace_id.iter().any(|byte| *byte != 0));
                            assert!(span.end_time_unix_nano >= span.start_time_unix_nano);
                            return;
                        }
                    }
                }
            }
        }
        panic!("receiver closed before HTTP span arrived");
    })
    .await
    .expect("HTTP span must arrive over OTLP/gRPC");
    drop(server);
    let _ = stop_tx.send(());
    collector.await.unwrap();
}
