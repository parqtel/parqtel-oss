//! Optional Kafka consumers for logs, metrics, and traces.
//!
//! Each configured topic gets one blocking consumer. Messages are decoded with
//! the same OTLP decoders used by the HTTP/gRPC ingest paths (or the
//! generic-JSON parser for `format = "json"` logs) and handed to the existing
//! ingest services. An offset is committed only after the ingest call returns
//! `Ok(())`; a decode or ingest failure leaves it uncommitted, so a restart
//! redelivers rather than drops the batch.

use crate::state::AppState;
use parqtel_core::config::IngestConfig;
use parqtel_core::{LogRecord, Metric, Span};
use parqtel_ingest::decode::OtlpDecoder;
use parqtel_ingest::otel::collector::logs::v1::ExportLogsServiceRequest;
use parqtel_ingest::otel::collector::metrics::v1::ExportMetricsServiceRequest;
use parqtel_ingest::otel::collector::trace::v1::ExportTraceServiceRequest;
use prost::Message;
use rdkafka::consumer::{BaseConsumer, CommitMode, Consumer};
use rdkafka::{ClientConfig, Message as KafkaMessage};
use std::collections::HashSet;
use std::time::Duration;
use tokio::runtime::Handle;

/// One decoded Kafka message, tagged with the signal its topic carries.
#[derive(Debug)]
enum KafkaBatch {
    Metrics(Vec<Metric>),
    Logs(Vec<LogRecord>),
    Traces(Vec<Span>),
}

/// A decoded message's destination. Production routes into `AppState`'s ingest
/// services; tests use a recording sink without a broker.
trait BatchSink {
    fn ingest(&self, batch: KafkaBatch) -> Result<(), String>;
}

type Decoder = Box<dyn FnMut(&[u8]) -> Result<KafkaBatch, String> + Send>;

/// Starts one blocking consumer per configured topic. Returns `None` when the
/// consumer is disabled or misconfigured.
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
                tracing::warn!(error = %e, topic, "Kafka poll failed");
                continue;
            }
        };
        let Some(payload) = KafkaMessage::payload(&message) else {
            continue;
        };
        let commit = || {
            consumer
                .commit_message(&message, CommitMode::Sync)
                .map_err(|e| e.to_string())
        };
        if let Err(e) = consume_payload(payload, &mut *decode, &sink, commit) {
            tracing::warn!(
                topic,
                error = %e,
                "Kafka message failed; offset not committed"
            );
        }
        tracing::info!(
            brokers = %config.brokers,
            topic = %config.topic,
            group = %config.group_id,
            "Kafka log consumer started"
        );
        assert!(result.is_err());
        assert_eq!(committed.get(), 0);
    }

    #[test]
    fn decode_error_leaves_offset_uncommitted() {
        let sink = RecordingSink::default();
        let committed = Cell::new(0);
        let result = consume_payload(
            b"not a protobuf payload",
            decode_metrics_payload,
            &sink,
            commit_counter(&committed),
        );
        assert!(result.is_err());
        assert_eq!(committed.get(), 0);
        assert!(sink.batches.lock().unwrap().is_empty());
    }

    #[test]
    fn consumer_specs_start_only_configured_signals() {
        let mut ingest = IngestConfig::default();
        ingest.kafka.enabled = true;
        ingest.kafka.brokers = "localhost:9092".into();
        ingest.kafka.topic = "logs".into();
        ingest.kafka.metrics_topic = Some("metrics".into());
        ingest.kafka.format = "otlp_proto".into();
        let specs = consumer_specs(&ingest).unwrap();
        let topics: Vec<&str> = specs.iter().map(|(topic, _)| topic.as_str()).collect();
        assert_eq!(topics, vec!["logs", "metrics"]);
    }

    #[test]
    fn json_format_uses_generic_log_parser() {
        let mut ingest = IngestConfig::default();
        ingest.kafka.enabled = true;
        ingest.kafka.brokers = "localhost:9092".into();
        ingest.kafka.topic = "logs".into();
        let mut specs = consumer_specs(&ingest).unwrap();
        let (_, mut decode) = specs.pop().unwrap();
        let sink = RecordingSink::default();
        let committed = Cell::new(0);
        consume_payload(
            br#"{"message":"hello","level":"INFO","timestamp":5000}"#,
            &mut *decode,
            &sink,
            commit_counter(&committed),
        )
        .unwrap();
        assert_eq!(committed.get(), 1);
        let batches = sink.batches.lock().unwrap();
        match &batches[0] {
            KafkaBatch::Logs(logs) => {
                assert_eq!(logs.len(), 1);
                assert_eq!(logs[0].body, "hello");
                assert_eq!(logs[0].timestamp_ns, 5_000_000_000_000);
            }
            other => panic!("expected logs batch, got {other:?}"),
        }
    }
}
