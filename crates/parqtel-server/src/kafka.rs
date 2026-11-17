//! Optional Kafka consumer for JSON logs.
//!
//! Messages are decoded with the same generic-JSON parser as
//! `POST /api/v1/ingest`, ingested through the normal WAL/buffer path, and
//! committed only after `ingest_logs` returns. A failed ingest leaves the
//! offset uncommitted, so a restart redelivers rather than loses the batch.

use crate::state::AppState;
use rdkafka::consumer::{BaseConsumer, CommitMode, Consumer};
use rdkafka::{ClientConfig, Message};
use std::time::Duration;

/// Starts a blocking Kafka consumer. Returns `None` when disabled.
pub fn spawn_kafka_consumer(state: AppState) -> Option<tokio::task::JoinHandle<()>> {
    let config = state.inner.config.ingest.kafka.clone();
    if !config.enabled {
        return None;
    }
    let handle = tokio::runtime::Handle::current();
    Some(tokio::task::spawn_blocking(move || {
        let consumer: BaseConsumer = match ClientConfig::new()
            .set("bootstrap.servers", &config.brokers)
            .set("group.id", &config.group_id)
            .set("enable.auto.commit", "false")
            .set("auto.offset.reset", "earliest")
            .create()
        {
            Ok(consumer) => consumer,
            Err(e) => {
                tracing::error!(error = %e, "failed to create Kafka consumer");
                return;
            }
        };
        if let Err(e) = consumer.subscribe(&[&config.topic]) {
            tracing::error!(error = %e, topic = %config.topic, "failed to subscribe");
            return;
        }
        tracing::info!(
            brokers = %config.brokers,
            topic = %config.topic,
            group = %config.group_id,
            "Kafka log consumer started"
        );
        loop {
            let Some(result) = consumer.poll(Duration::from_millis(500)) else {
                continue;
            };
            let message = match result {
                Ok(message) => message,
                Err(e) => {
                    tracing::warn!(error = %e, "Kafka poll failed");
                    continue;
                }
            };
            let Some(payload) = message.payload() else {
                continue;
            };
            let logs = match crate::handlers::ingest::parse_generic_json(
                payload,
                &config.service,
                &state.inner.config.ingest,
            ) {
                Ok(logs) => logs,
                Err(e) => {
                    tracing::warn!(error = %e, "Kafka message is not valid generic JSON; skipping");
                    let _ = consumer.commit_message(&message, CommitMode::Async);
                    continue;
                }
            };
            match handle.block_on(state.inner.log_ingestion_service.ingest_logs(logs)) {
                Ok(_) => {
                    if let Err(e) = consumer.commit_message(&message, CommitMode::Sync) {
                        tracing::warn!(error = %e, "Kafka offset commit failed");
                    }
                }
                Err(e) => {
                    tracing::warn!(error = %e, "Kafka batch ingest failed; offset not committed");
                }
            }
        }
    }))
}
