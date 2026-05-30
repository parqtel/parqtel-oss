use std::sync::Arc;
use tokio::sync::{RwLock, Mutex};
use parqtel_core::{BlockIndex, Config, StorageEngine};
use parqtel_ingest::{IngestionService, LogIngestionService, TraceIngestionService};
use parqtel_query::QueryExecutor;
use parqtel_alert::AlertRuleRegistry;
use parqtel_alert::AlertStore;

/// Shared application state for the HTTP server.
#[derive(Clone)]
pub struct AppState {
    pub inner: Arc<AppStateInner>,
}

pub struct AppStateInner {
    pub storage_engine: Arc<dyn StorageEngine>,
    pub ingestion_service: Mutex<IngestionService>,
    pub log_ingestion_service: Mutex<LogIngestionService>,
    pub trace_ingestion_service: Mutex<TraceIngestionService>,
    pub query_executor: QueryExecutor,
    pub index: Arc<RwLock<BlockIndex>>,
    pub config: Config,
    pub ui_content: Vec<u8>,
    pub ui_etag: String,
    pub metrics: crate::metrics::ServerMetrics,
    pub alert_registry: AlertRuleRegistry,
    pub alert_store: AlertStore,
}

impl AppState {
    pub async fn new(
        storage_engine: Arc<dyn StorageEngine>,
        ingestion_service: IngestionService,
        log_ingestion_service: LogIngestionService,
        trace_ingestion_service: TraceIngestionService,
        query_executor: QueryExecutor,
        index: Arc<RwLock<BlockIndex>>,
        config: Config,
        ui_content: Vec<u8>,
        ui_etag: String,
    ) -> Self {
        let data_dir = config.storage.data_dir.clone();

        Self {
            inner: Arc::new(AppStateInner {
                storage_engine,
                ingestion_service: Mutex::new(ingestion_service),
                log_ingestion_service: Mutex::new(log_ingestion_service),
                trace_ingestion_service: Mutex::new(trace_ingestion_service),
                query_executor,
                index,
                config,
                ui_content,
                ui_etag,
                metrics: crate::metrics::ServerMetrics::default(),
                alert_registry: AlertRuleRegistry::new(),
                alert_store: AlertStore::new(Some(data_dir)).await,
            }),
        }
    }

    #[cfg(test)]
    pub async fn default_for_tests() -> Self {
        use tokio::sync::mpsc;
        let dir = tempfile::tempdir().unwrap();
        let config = Config::default();
        let (tx, _) = mpsc::unbounded_channel();
        let (ltx, _) = mpsc::unbounded_channel();
        let (ttx, _) = mpsc::unbounded_channel();
        let index = Arc::new(RwLock::new(BlockIndex::new(dir.path())));
        let log_index = Arc::new(RwLock::new(BlockIndex::new(&dir.path().join("logs"))));

        let storage_engine: Arc<dyn StorageEngine> = Arc::new(
            parqtel_core::engine::parquet::ParquetStorageEngine::new(config.storage.clone())
        );

        Self::new(
            storage_engine,
            IngestionService::new(config.storage.clone(), tx),
            LogIngestionService::new(config.logs.clone(), ltx),
            TraceIngestionService::new(config.storage.clone(), ttx),
            QueryExecutor::new(index.clone(), log_index),
            index,
            config,
            vec![],
            "".into(),
        ).await
    }
}
