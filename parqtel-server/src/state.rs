use parqtel_alert::evaluator::engine::{EvalConfig, EvaluationEngine};
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
    pub pipeline_registry: PipelineRegistry,
}

impl AppState {
    #[allow(clippy::too_many_arguments)]
    pub async fn new(
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
        let alert_registry = AlertRuleRegistry::new();
        let alert_store = AlertStore::new(Some(data_dir)).await;
        let (alert_tx, _) = tokio::sync::mpsc::unbounded_channel();
        let alert_engine = Arc::new(EvaluationEngine::new(
            EvalConfig {
                evaluation_interval_secs: 15,
                evaluation_timeout_secs: 10,
            },
            alert_registry.clone(),
            alert_store.clone(),
            alert_tx,
        ));

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
                metrics: crate::metrics::ServerMetrics::default(),
                alert_registry,
                alert_store,
                alert_engine,
                pipeline_registry: PipelineRegistry::new(),
            }),
        }
    }

    #[cfg(test)]
    #[allow(clippy::unwrap_used)]
    pub async fn default_for_tests() -> Self {
        use tokio::sync::mpsc;
        let dir = tempfile::tempdir().unwrap();
        let config = Config::default();
        let (tx, _) = mpsc::unbounded_channel();
        let (ltx, _) = mpsc::unbounded_channel();
        let (ttx, _) = mpsc::unbounded_channel();
        let index = Arc::new(RwLock::new(BlockIndex::new(dir.path())));
        let log_index = Arc::new(RwLock::new(BlockIndex::new(&dir.path().join("logs"))));
        let trace_index = Arc::new(RwLock::new(BlockIndex::new(
            config.storage.data_dir.join("traces"),
        )));
        let executor = QueryExecutor::with_trace_index(
            index.clone(),
            log_index.clone(),
            trace_index,
            parqtel_core::MemoryBuffer::new(),
            config.storage.data_dir.join("traces"),
        );
        let memory_buffer = executor.memory_buffer();

        Self::new(
            IngestionService::new(config.storage.clone(), tx)
                .with_memory_buffer(memory_buffer.clone()),
            LogIngestionService::new(config.logs.clone(), ltx)
                .with_memory_buffer(memory_buffer.clone()),
            TraceIngestionService::new(config.storage.clone(), ttx)
                .with_memory_buffer(memory_buffer),
            executor,
            index,
            config,
            vec![],
            "".into(),
        )
        .await
    }
}
