use parqtel_alert::dispatcher::metrics::RoutingMetrics;
use parqtel_alert::dispatcher::AlertDispatcher;
use parqtel_alert::evaluator::engine::EvaluationEngine;
use parqtel_alert::router::AlertRouter;
use parqtel_alert::AlertRuleRegistry;
use parqtel_alert::AlertStore;
use parqtel_core::{BlockIndex, Config};
use parqtel_ingest::{IngestionService, LogIngestionService, TraceIngestionService};
use parqtel_pipeline::rule::registry::RuleRegistry as PipelineRegistry;
use parqtel_query::QueryExecutor;
use std::sync::Arc;
use tokio::sync::RwLock;

/// Shared application state for the HTTP server.
#[derive(Clone)]
pub struct AppState {
    pub inner: Arc<AppStateInner>,
}

pub struct AppStateInner {
    pub ingestion_service: IngestionService,
    pub log_ingestion_service: LogIngestionService,
    pub trace_ingestion_service: TraceIngestionService,
    pub query_executor: QueryExecutor,
    pub index: Arc<RwLock<BlockIndex>>,
    pub config: Config,
    pub ui_content: Vec<u8>,
    pub ui_etag: String,
    pub metrics: crate::metrics::ServerMetrics,
    pub alert_registry: AlertRuleRegistry,
    pub alert_store: AlertStore,
    pub alert_engine: Arc<EvaluationEngine>,
    pub alert_router: Arc<AlertRouter>,
    pub alert_dispatcher: Option<Arc<AlertDispatcher>>,
    /// Receiver for alert firing events (consumed once by the router task).
    pub alert_event_rx: tokio::sync::Mutex<
        Option<tokio::sync::mpsc::UnboundedReceiver<parqtel_alert::AlertFiringEvent>>,
    >,
    pub pipeline_registry: PipelineRegistry,
    pub saved_searches: crate::saved_searches::SavedSearchStore,
    pub dashboards: crate::dashboards::DashboardStore,
    pub object_catalog: Option<Arc<parqtel_core::storage::ObjectCatalog>>,
    pub auth: Arc<crate::auth::AuthState>,
    pub audit: Arc<crate::audit::AuditLog>,
}

impl AppState {
    #[allow(clippy::too_many_arguments)]
    pub async fn new(
        ingestion_service: IngestionService,
        log_ingestion_service: LogIngestionService,
        trace_ingestion_service: TraceIngestionService,
        pipeline_registry: PipelineRegistry,
        query_executor: QueryExecutor,
        index: Arc<RwLock<BlockIndex>>,
        config: Config,
        ui_content: Vec<u8>,
        ui_etag: String,
        contention: Arc<parqtel_core::ContentionMetrics>,
        index_stores: [Arc<parqtel_core::BlockIndexStore>; 3],
    ) -> Self {
        let data_dir = config.storage.data_dir.clone();
        // The server owns block I/O for the process. Absolute block paths are
        // unchanged; the store is the seam an object-store backend replaces.
        let mut object_catalog = None;
        let object_store: std::sync::Arc<dyn parqtel_core::storage::BlockStore> = if config
            .object_store
            .enabled
        {
            let cache_dir = config
                .object_store
                .cache_dir
                .clone()
                .unwrap_or_else(|| data_dir.join("cache"));
            match parqtel_core::storage::block_store::ObjectBlockStore::s3(
                &config.object_store.bucket,
                &config.object_store.region,
                config.object_store.endpoint.as_deref(),
                config.object_store.access_key.as_deref(),
                config.object_store.secret_key.as_deref(),
                &config.object_store.prefix,
                cache_dir,
                config.object_store.cache_bytes,
                tokio::runtime::Handle::current(),
            ) {
                Ok(store) => {
                    object_catalog = Some(Arc::new(store.catalog(&config.object_store.prefix)));
                    std::sync::Arc::new(store)
                }
                Err(e) => {
                    tracing::error!(error = %e, "failed to build object store; using local disk");
                    std::sync::Arc::new(parqtel_core::storage::LocalBlockStore::new(
                        data_dir.clone(),
                    ))
                }
            }
        } else {
            std::sync::Arc::new(parqtel_core::storage::LocalBlockStore::new(
                data_dir.clone(),
            ))
        };
        let _ = parqtel_core::storage::block_store::install(object_store);
        let saved_search_dir = config.storage.data_dir.clone();
        let dashboard_dir = config.storage.data_dir.clone();
        let auth = Arc::new(
            crate::auth::AuthState::from_config(&config.auth, config.server.dataset.clone()).unwrap_or_else(|e| {
                tracing::error!(error = %e, "invalid auth configuration; rejecting all requests");
                crate::auth::AuthState::deny_all()
            }),
        );
        let audit = Arc::new(crate::audit::AuditLog::new(
            &config.audit,
            &config.storage.data_dir,
        ));
        let alert_registry = AlertRuleRegistry::new();
        let alert_store = AlertStore::new(Some(data_dir)).await;
        let (alert_tx, alert_event_rx) = tokio::sync::mpsc::unbounded_channel();
        let alert_engine = Arc::new(
            EvaluationEngine::new(alert_store.clone(), alert_tx)
                .with_noise_threshold(config.alerts.noise_suppression_threshold)
                .with_noise_window(config.alerts.noise_window_firings),
        );
        let alert_router = Arc::new(
            AlertRouter::new(config.alerts.notifications.clone().unwrap_or_default())
                // Persist silences alongside alerts.json (same data dir).
                .with_silence_dir(config.storage.data_dir.clone()),
        );
        let routing_metrics = Arc::new(RoutingMetrics::new());
        let alert_dispatcher = config.alerting.routing.as_ref().and_then(|routing_cfg| {
            match AlertDispatcher::from_config(routing_cfg, routing_metrics.clone()) {
                Ok(d) => Some(Arc::new(d)),
                Err(e) => {
                    tracing::error!(error = %e, "failed to initialize Alertmanager dispatcher");
                    None
                }
            }
        });

        Self {
            inner: Arc::new(AppStateInner {
                ingestion_service,
                log_ingestion_service,
                trace_ingestion_service,
                query_executor,
                index,
                config,
                ui_content,
                ui_etag,
                metrics: crate::metrics::ServerMetrics::with_contention(contention)
                    .with_index_stores(index_stores)
                    .with_routing_metrics(routing_metrics.clone()),
                alert_registry,
                alert_store,
                alert_engine,
                alert_router,
                alert_dispatcher,
                alert_event_rx: tokio::sync::Mutex::new(Some(alert_event_rx)),
                pipeline_registry,
                saved_searches: crate::saved_searches::SavedSearchStore::new(saved_search_dir),
                dashboards: crate::dashboards::DashboardStore::new(dashboard_dir),
                object_catalog,
                auth: auth.clone(),
                audit,
            }),
        }
    }

    #[cfg(test)]
    #[allow(clippy::unwrap_used)]
    pub async fn default_for_tests() -> Self {
        Self::default_for_tests_with_config(Config::default()).await
    }

    #[cfg(test)]
    #[allow(clippy::unwrap_used)]
    pub async fn default_for_tests_with_config(mut config: Config) -> Self {
        use tokio::sync::mpsc;
        // Kept for the life of the process: the services below flush from
        // background tasks and must not write into the working directory.
        let dir = tempfile::tempdir().unwrap().keep();
        config.storage.data_dir = dir.clone();
        config.logs.data_dir = dir.join("logs");
        // Blocks now carry a durability handshake, so the index task can
        // confirm the sidecar write before the flush commits its WAL.
        let (tx, mut metadata_rx) =
            mpsc::unbounded_channel::<parqtel_core::storage::PendingIndex>();
        let (ltx, _) = mpsc::unbounded_channel::<parqtel_core::storage::PendingIndex>();
        let (ttx, _) = mpsc::unbounded_channel::<parqtel_core::storage::PendingIndex>();
        let index = Arc::new(RwLock::new(BlockIndex::new(&dir)));
        let log_index = Arc::new(RwLock::new(BlockIndex::new(&dir.join("logs"))));
        let trace_dir = config.storage.data_dir.join("traces");
        let trace_index = Arc::new(RwLock::new(BlockIndex::new(&trace_dir)));
        let index_stores: [Arc<parqtel_core::BlockIndexStore>; 3] = [
            Arc::new(parqtel_core::BlockIndexStore::at_path(
                dir.join("index.json"),
            )),
            Arc::new(parqtel_core::BlockIndexStore::at_path(
                dir.join("logs/index.json"),
            )),
            Arc::new(parqtel_core::BlockIndexStore::at_path(
                dir.join("traces/index.json"),
            )),
        ];
        // Index-update task (mirrors main.rs wiring): flushed-block metadata
        // flows into the shared BlockIndex so block-backed queries work in
        // tests exactly as in production.
        {
            let idx = index.clone();
            let store = index_stores[0].clone();
            tokio::spawn(async move {
                while let Some(pending) = metadata_rx.recv().await {
                    idx.write().await.add(pending.meta);
                    store.mark_dirty();
                    if let Some(ack) = pending.durable {
                        let _ = ack.send(());
                    }
                }
            });
        }
        let executor = QueryExecutor::with_trace_index(
            index.clone(),
            log_index.clone(),
            trace_index,
            parqtel_core::MemoryBuffer::new(),
            config.storage.data_dir.join("traces"),
        );
        let memory_buffer = executor.memory_buffer();
        let contention = Arc::new(parqtel_core::ContentionMetrics::new());
        let dir = dir.to_path_buf();
        let index_stores: [Arc<parqtel_core::BlockIndexStore>; 3] = [
            Arc::new(parqtel_core::BlockIndexStore::at_path(
                dir.join("index.json"),
            )),
            Arc::new(parqtel_core::BlockIndexStore::at_path(
                dir.join("logs/index.json"),
            )),
            Arc::new(parqtel_core::BlockIndexStore::at_path(
                dir.join("traces/index.json"),
            )),
        ];

        Self::new(
            IngestionService::new(config.storage.clone(), tx)
                .with_memory_buffer(memory_buffer.clone())
                .with_contention(contention.clone()),
            LogIngestionService::new(config.logs.clone(), ltx)
                .with_memory_buffer(memory_buffer.clone())
                .with_contention(contention.clone()),
            TraceIngestionService::new(config.storage.clone(), ttx)
                .with_memory_buffer(memory_buffer)
                .with_contention(contention.clone()),
            PipelineRegistry::new(),
            executor,
            index,
            config,
            vec![],
            "".into(),
            contention,
            index_stores,
        )
        .await
    }
}
