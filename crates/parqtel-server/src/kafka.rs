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
    let ingest_config = state.inner.config.ingest.clone();
    let specs = match consumer_specs(&ingest_config) {
        Ok(specs) => specs,
        Err(e) => {
            tracing::error!(error = %e, "Kafka consumer is misconfigured");
            return None;
        }
    };
    let brokers = config.brokers;
    let group_id = config.group_id;
    let runtime = Handle::current();
    Some(tokio::spawn(async move {
        let mut tasks = Vec::with_capacity(specs.len());
        for (topic, decode) in specs {
            let state = state.clone();
            let brokers = brokers.clone();
            let group_id = group_id.clone();
            let runtime = runtime.clone();
            tasks.push(tokio::task::spawn_blocking(move || {
                run_consumer(&state, &runtime, &brokers, &group_id, &topic, decode);
            }));
        }
        for task in tasks {
            if let Err(e) = task.await {
                tracing::error!(error = %e, "Kafka consumer task stopped");
            }
        }
    }))
}

/// Builds `(topic, decoder)` pairs. The log consumer always runs (legacy
/// behaviour); metrics and traces consumers run only when their topic is set.
fn consumer_specs(ingest: &IngestConfig) -> Result<Vec<(String, Decoder)>, String> {
    let config = &ingest.kafka;
    let log_topic = config.resolved_logs_topic().trim().to_string();
    if log_topic.is_empty() {
        return Err("ingest.kafka log topic is empty".into());
    }
    let log_decoder: Decoder = match config.format.as_str() {
        "json" => {
            let service = config.service.clone();
            let ingest = ingest.clone();
            Box::new(move |payload| {
                crate::handlers::ingest::parse_generic_json(payload, &service, &ingest)
                    .map(KafkaBatch::Logs)
            })
        }
        "otlp_proto" => Box::new(decode_logs_payload),
        other => {
            return Err(format!(
                "unsupported ingest.kafka.format '{other}'; expected 'json' or 'otlp_proto'"
            ))
        }
    };
    let mut specs: Vec<(String, Decoder)> = vec![(log_topic, log_decoder)];
    if let Some(topic) = config.resolved_metrics_topic() {
        specs.push((topic.trim().to_string(), Box::new(decode_metrics_payload)));
    }
    if let Some(topic) = config.resolved_traces_topic() {
        specs.push((topic.trim().to_string(), Box::new(decode_traces_payload)));
    }
    // Validation rejects duplicates, but a programmatic config could bypass
    // it. Two consumers in one group on one topic would split its partitions
    // between signal decoders.
    let mut seen = HashSet::new();
    for (topic, _) in &specs {
        if !seen.insert(topic.as_str()) {
            return Err(format!(
                "Kafka topic '{topic}' is configured for more than one signal"
            ));
        }
    }
    Ok(specs)
}

fn run_consumer(
    state: &AppState,
    runtime: &Handle,
    brokers: &str,
    group_id: &str,
    topic: &str,
    mut decode: Decoder,
) {
    let consumer: BaseConsumer = match ClientConfig::new()
        .set("bootstrap.servers", brokers)
        .set("group.id", group_id)
        .set("enable.auto.commit", "false")
        .set("auto.offset.reset", "earliest")
        .create()
    {
        Ok(consumer) => consumer,
        Err(e) => {
            tracing::error!(error = %e, topic, "failed to create Kafka consumer");
            return;
        }
    };
    if let Err(e) = consumer.subscribe(&[topic]) {
        tracing::error!(error = %e, topic, "failed to subscribe");
        return;
    }
    tracing::info!(brokers, topic, group = group_id, "Kafka consumer started");
    let sink = ServiceSink { state, runtime };
    loop {
        let Some(result) = consumer.poll(Duration::from_millis(500)) else {
            continue;
        };
        let message = match result {
            Ok(message) => message,
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
    }
}

/// Decode, ingest, then commit. The commit callback runs only when both the
/// decoder and the ingest sink return success.
fn consume_payload<S, D, C>(
    payload: &[u8],
    mut decode: D,
    sink: &S,
    commit: C,
) -> Result<(), String>
where
    S: BatchSink,
    D: FnMut(&[u8]) -> Result<KafkaBatch, String>,
    C: FnOnce() -> Result<(), String>,
{
    let batch = decode(payload)?;
    sink.ingest(batch)?;
    commit()
}

/// Routes decoded batches into the same ingest services HTTP and gRPC use.
struct ServiceSink<'a> {
    state: &'a AppState,
    runtime: &'a Handle,
}

impl BatchSink for ServiceSink<'_> {
    fn ingest(&self, batch: KafkaBatch) -> Result<(), String> {
        let result = match batch {
            KafkaBatch::Metrics(metrics) => self
                .runtime
                .block_on(self.state.inner.ingestion_service.ingest_metrics(metrics)),
            KafkaBatch::Logs(logs) => self
                .runtime
                .block_on(self.state.inner.log_ingestion_service.ingest_logs(logs)),
            KafkaBatch::Traces(spans) => self
                .runtime
                .block_on(self.state.inner.trace_ingestion_service.ingest_spans(spans)),
        };
        result.map(|_| ()).map_err(|e| e.to_string())
    }
}

fn decode_metrics_payload(payload: &[u8]) -> Result<KafkaBatch, String> {
    let request = ExportMetricsServiceRequest::decode(payload)
        .map_err(|e| format!("Protobuf decode error: {e}"))?;
    OtlpDecoder::decode_metrics(request)
        .map(KafkaBatch::Metrics)
        .map_err(|e| e.to_string())
}

fn decode_logs_payload(payload: &[u8]) -> Result<KafkaBatch, String> {
    let request = ExportLogsServiceRequest::decode(payload)
        .map_err(|e| format!("Protobuf decode error: {e}"))?;
    OtlpDecoder::decode_logs(request)
        .map(KafkaBatch::Logs)
        .map_err(|e| e.to_string())
}

fn decode_traces_payload(payload: &[u8]) -> Result<KafkaBatch, String> {
    let request = ExportTraceServiceRequest::decode(payload)
        .map_err(|e| format!("Protobuf decode error: {e}"))?;
    OtlpDecoder::decode_traces(request)
        .map(KafkaBatch::Traces)
        .map_err(|e| e.to_string())
}

#[cfg(test)]
mod tests {
    #![allow(clippy::unwrap_used, clippy::expect_used, clippy::panic)]
    use super::*;
    use parqtel_ingest::otel::logs::v1::{LogRecord as ProtoLogRecord, ResourceLogs, ScopeLogs};
    use parqtel_ingest::otel::metrics::v1::{
        number_data_point, Gauge, Metric as ProtoMetric, NumberDataPoint, ResourceMetrics,
        ScopeMetrics,
    };
    use parqtel_ingest::otel::trace::v1::{ResourceSpans, ScopeSpans, Span as ProtoSpan};
    use std::cell::Cell;
    use std::sync::Mutex;

    #[derive(Default)]
    struct RecordingSink {
        batches: Mutex<Vec<KafkaBatch>>,
        fail: bool,
    }

    impl BatchSink for RecordingSink {
        fn ingest(&self, batch: KafkaBatch) -> Result<(), String> {
            if self.fail {
                return Err("simulated ingest failure".into());
            }
            self.batches.lock().unwrap().push(batch);
            Ok(())
        }
    }

    fn metrics_payload() -> Vec<u8> {
        ExportMetricsServiceRequest {
            resource_metrics: vec![ResourceMetrics {
                resource: None,
                scope_metrics: vec![ScopeMetrics {
                    scope: None,
                    metrics: vec![ProtoMetric {
                        name: "cpu".into(),
                        description: String::new(),
                        unit: String::new(),
                        data: Some(parqtel_ingest::otel::metrics::v1::metric::Data::Gauge(
                            Gauge {
                                data_points: vec![NumberDataPoint {
                                    time_unix_nano: 1000,
                                    value: Some(number_data_point::Value::AsDouble(42.5)),
                                    attributes: vec![],
                                    start_time_unix_nano: 0,
                                    exemplars: vec![],
                                    flags: 0,
                                }],
                            },
                        )),
                    }],
                    schema_url: String::new(),
                }],
                schema_url: String::new(),
            }],
        }
        .encode_to_vec()
    }

    fn logs_payload() -> Vec<u8> {
        ExportLogsServiceRequest {
            resource_logs: vec![ResourceLogs {
                resource: None,
                scope_logs: vec![ScopeLogs {
                    scope: None,
                    log_records: vec![ProtoLogRecord {
                        time_unix_nano: 5000,
                        observed_time_unix_nano: 5001,
                        severity_number: 9,
                        severity_text: "INFO".into(),
                        body: Some(parqtel_ingest::otel::common::v1::AnyValue {
                            value: Some(
                                parqtel_ingest::otel::common::v1::any_value::Value::StringValue(
                                    "hello".into(),
                                ),
                            ),
                        }),
                        attributes: vec![],
                        flags: 0,
                        trace_id: vec![0; 16],
                        span_id: vec![0; 8],
                        dropped_attributes_count: 0,
                    }],
                    schema_url: String::new(),
                }],
                schema_url: String::new(),
            }],
        }
        .encode_to_vec()
    }

    fn traces_payload() -> Vec<u8> {
        ExportTraceServiceRequest {
            resource_spans: vec![ResourceSpans {
                resource: None,
                scope_spans: vec![ScopeSpans {
                    scope: None,
                    spans: vec![ProtoSpan {
                        trace_id: vec![1; 16],
                        span_id: vec![2; 8],
                        parent_span_id: vec![],
                        name: "test-span".into(),
                        kind: 2,
                        start_time_unix_nano: 1000,
                        end_time_unix_nano: 2000,
                        attributes: vec![],
                        events: vec![],
                        links: vec![],
                        status: None,
                        trace_state: String::new(),
                        flags: 0,
                        dropped_attributes_count: 0,
                        dropped_events_count: 0,
                        dropped_links_count: 0,
                    }],
                    schema_url: String::new(),
                }],
                schema_url: String::new(),
            }],
        }
        .encode_to_vec()
    }

    fn commit_counter(committed: &Cell<u32>) -> impl FnOnce() -> Result<(), String> + '_ {
        move || {
            committed.set(committed.get() + 1);
            Ok(())
        }
    }

    #[test]
    fn dispatch_decodes_metrics_otlp_proto() {
        let sink = RecordingSink::default();
        let committed = Cell::new(0);
        consume_payload(
            &metrics_payload(),
            decode_metrics_payload,
            &sink,
            commit_counter(&committed),
        )
        .unwrap();
        assert_eq!(committed.get(), 1);
        let batches = sink.batches.lock().unwrap();
        match &batches[0] {
            KafkaBatch::Metrics(metrics) => {
                assert_eq!(metrics.len(), 1);
                assert_eq!(metrics[0].name, "cpu");
            }
            other => panic!("expected metrics batch, got {other:?}"),
        }
    }

    #[test]
    fn dispatch_decodes_logs_otlp_proto() {
        let sink = RecordingSink::default();
        let committed = Cell::new(0);
        consume_payload(
            &logs_payload(),
            decode_logs_payload,
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
            }
            other => panic!("expected logs batch, got {other:?}"),
        }
    }

    #[test]
    fn dispatch_decodes_traces_otlp_proto() {
        let sink = RecordingSink::default();
        let committed = Cell::new(0);
        consume_payload(
            &traces_payload(),
            decode_traces_payload,
            &sink,
            commit_counter(&committed),
        )
        .unwrap();
        assert_eq!(committed.get(), 1);
        let batches = sink.batches.lock().unwrap();
        match &batches[0] {
            KafkaBatch::Traces(spans) => {
                assert_eq!(spans.len(), 1);
                assert_eq!(spans[0].name, "test-span");
            }
            other => panic!("expected traces batch, got {other:?}"),
        }
    }

    #[test]
    fn failed_ingest_leaves_offset_uncommitted() {
        let sink = RecordingSink {
            fail: true,
            ..Default::default()
        };
        let committed = Cell::new(0);
        let result = consume_payload(
            &metrics_payload(),
            decode_metrics_payload,
            &sink,
            commit_counter(&committed),
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
