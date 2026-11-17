//! In-process TLS for the HTTP console/API.
//!
//! The OTLP gRPC listener builds its own tonic TLS config from the same PEM
//! pair; keeping one cert/key config avoids two ways to get it wrong.

use axum::Router;
use hyper_util::rt::{TokioExecutor, TokioIo};
use hyper_util::server::conn::auto::Builder as HyperBuilder;
use hyper_util::service::TowerToHyperService;
use parqtel_core::config::TlsConfig;
use rustls_pemfile::{certs, private_key};
use std::fs::File;
use std::io::BufReader;
use std::sync::Arc;
use tokio::net::TcpListener;
use tokio_rustls::rustls::ServerConfig;
use tokio_rustls::TlsAcceptor;

/// Read a PEM certificate/key pair into a rustls server config.
pub fn load(config: &TlsConfig) -> anyhow::Result<ServerConfig> {
    let certs = certs(&mut BufReader::new(File::open(&config.cert_path)?))
        .collect::<Result<Vec<_>, _>>()
        .map_err(|e| anyhow::anyhow!("failed to read {}: {e}", config.cert_path))?;
    let key = private_key(&mut BufReader::new(File::open(&config.key_path)?))
        .map_err(|e| anyhow::anyhow!("failed to read {}: {e}", config.key_path))?
        .ok_or_else(|| anyhow::anyhow!("{} contains no private key", config.key_path))?;
    ServerConfig::builder()
        .with_no_client_auth()
        .with_single_cert(certs, key)
        .map_err(|e| anyhow::anyhow!("invalid TLS certificate/key: {e}"))
}

/// Serve the axum router over TLS until `shutdown` resolves.
pub async fn serve(
    listener: TcpListener,
    router: Router,
    config: ServerConfig,
    shutdown: impl std::future::Future<Output = ()> + Send,
) -> anyhow::Result<()> {
    let acceptor = TlsAcceptor::from(Arc::new(config));
    let builder = HyperBuilder::new(TokioExecutor::new());
    let graceful = hyper_util::server::graceful::GracefulShutdown::new();
    tokio::pin!(shutdown);

    loop {
        tokio::select! {
            _ = &mut shutdown => break,
            accepted = listener.accept() => {
                let (stream, peer) = accepted?;
                let acceptor = acceptor.clone();
                let service = TowerToHyperService::new(router.clone());
                let builder = builder.clone();
                let watcher = graceful.watcher();
                tokio::spawn(async move {
                    let tls = match acceptor.accept(stream).await {
                        Ok(tls) => tls,
                        Err(e) => {
                            tracing::warn!(%peer, error = %e, "TLS handshake failed");
                            return;
                        }
                    };
                    let connection = builder
                        .serve_connection_with_upgrades(TokioIo::new(tls), service);
                    if let Err(e) = watcher.watch(connection).await {
                        tracing::debug!(%peer, error = %e, "TLS connection ended");
                    }
                });
            }
        }
    }
    graceful.shutdown().await;
    Ok(())
}
