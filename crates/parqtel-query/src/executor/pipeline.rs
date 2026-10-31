//! Row sources for pipeline queries.

use super::QueryExecutor;
use crate::pipeline_exec::Row;
use parqtel_core::{Result, Scanner};

impl QueryExecutor {
    /// Executes a parsed ParqtelQL pipeline. Fetch materializes rows from
    /// the log scan; correlate enriches rows with trace context when
    /// requested (dimension-priority join on trace_id/service).
    pub async fn execute_pipeline(
        &self,
        pipeline: &crate::pipeline::Pipeline,
        start_ns: i64,
        end_ns: i64,
    ) -> Result<crate::pipeline_exec::PipelineResult> {
        use crate::pipeline::Stage;
        use crate::pipeline_exec::run_stages;
        use serde_json::json;

        let signal = pipeline.fetch_signal();
        // G6: push the first filter stage into row materialization.
        let first_filter = pipeline.stages.iter().find_map(|s| match s {
            Stage::Filter { pred } => Some(pred.clone()),
            _ => None,
        });
        let rows: Vec<Row> = match signal.as_str() {
            // G9a: metrics rows — one row per data point.
            "metrics" => {
                self.fetch_metric_rows(start_ns, end_ns, &first_filter)
                    .await?
            }
            // G9b: trace rows — one row per span.
            "traces" => {
                self.fetch_trace_rows(start_ns, end_ns, &first_filter)
                    .await?
            }
            _ => self.fetch_log_rows(start_ns, end_ns, &first_filter).await?,
        };

        // Correlate enrichment: join related signal data per row —
        // trace_id first, service+window fallback (G11) when absent.
        if let Some(Stage::Correlate {
            signal: corr_signal,
            window_ns,
        }) = pipeline.stages.iter().find_map(|s| match s {
            Stage::Correlate { signal, window_ns } => Some(Stage::Correlate {
                signal: signal.clone(),
                window_ns: *window_ns,
            }),
            _ => None,
        }) {
            if corr_signal == "traces" {
                let spans = self.query_traces(start_ns, end_ns, None, 10_000).await?;
                let by_trace: std::collections::HashMap<String, Vec<&parqtel_core::Span>> = {
                    let mut m: std::collections::HashMap<String, Vec<&parqtel_core::Span>> =
                        std::collections::HashMap::new();
                    for s in &spans {
                        m.entry(hex::encode(s.trace_id)).or_default().push(s);
                    }
                    m
                };
                // G11: service+window index for rows lacking trace_id.
                let by_service_time: Vec<&parqtel_core::Span> = spans.iter().collect();
                let mut enriched: Vec<Row> = Vec::with_capacity(rows.len());
                for mut row in rows {
                    let tid = row.fields.get("trace_id").and_then(|v| v.as_str());
                    let trace_spans = tid.and_then(|t| by_trace.get(t).map(|v| v.as_slice()));
                    let matched: Vec<&parqtel_core::Span> = match trace_spans {
                        Some(ts) => ts.to_vec(),
                        None => {
                            // Fallback: same service, within the window of
                            // the row's timestamp.
                            if let Some(svc) = row
                                .fields
                                .get("service")
                                .and_then(|v| v.as_str())
                                .map(str::to_string)
                            {
                                let ts = row.ts_ns();
                                let w = window_ns.max(1);
                                by_service_time
                                    .iter()
                                    .filter(|s| {
                                        s.attributes.get("service.name") == Some(svc.as_str())
                                            && (s.start_time_ns - ts).abs() <= w
                                    })
                                    .copied()
                                    .collect()
                            } else {
                                Vec::new()
                            }
                        }
                    };
                    if !matched.is_empty() {
                        let svc_names: Vec<String> = matched
                            .iter()
                            .filter_map(|s| s.attributes.get("service.name").map(|v| v.to_string()))
                            .collect();
                        row.fields
                            .insert("correlated.trace_services".to_string(), json!(svc_names));
                        row.fields
                            .insert("correlated.span_count".to_string(), json!(matched.len()));
                        if let Some(dur) = matched.iter().map(|s| s.duration_ns()).max() {
                            row.fields.insert(
                                "correlated.trace_duration_ms".to_string(),
                                json!(dur as f64 / 1_000_000.0),
                            );
                        }
                    }
                    enriched.push(row);
                }
                return run_stages(pipeline, || Ok(enriched));
            }
            if corr_signal == "logs" {
                // Correlate logs onto metric/trace rows via service+window.
                let logs = self.fetch_log_rows(start_ns, end_ns, &None).await?;
                let mut enriched: Vec<Row> = Vec::with_capacity(rows.len());
                for mut row in rows {
                    let svc = row
                        .fields
                        .get("service")
                        .and_then(|v| v.as_str())
                        .map(str::to_string);
                    let ts = row.ts_ns();
                    if let Some(svc) = svc {
                        let w = window_ns.max(1);
                        let hits: Vec<&Row> = logs
                            .iter()
                            .filter(|l| {
                                l.fields.get("service").and_then(|v| v.as_str())
                                    == Some(svc.as_str())
                                    && (l.ts_ns() - ts).abs() <= w
                            })
                            .collect();
                        if !hits.is_empty() {
                            row.fields
                                .insert("correlated.log_count".to_string(), json!(hits.len()));
                            if let Some(sev) = hits
                                .iter()
                                .filter_map(|l| l.get_num("severity_number"))
                                .fold(None::<f64>, |acc, x| {
                                    Some(match acc {
                                        Some(a) if a >= x => a,
                                        _ => x,
                                    })
                                })
                            {
                                row.fields.insert(
                                    "correlated.max_severity_number".to_string(),
                                    json!(sev),
                                );
                            }
                        }
                    }
                    enriched.push(row);
                }
                return run_stages(pipeline, || Ok(enriched));
            }
        }

        run_stages(pipeline, || Ok(rows))
    }

    /// Logs → rows (G6: filter pushed into materialization).
    async fn fetch_log_rows(
        &self,
        start_ns: i64,
        end_ns: i64,
        first_filter: &Option<crate::logql::Predicate>,
    ) -> Result<Vec<Row>> {
        use serde_json::json;
        let blocks = {
            let idx = self.log_index.read().await;
            idx.query(start_ns, end_ns, None)
        };
        let buffered = self.buffer.scan_logs(start_ns, end_ns).await;
        let logs = if blocks.is_empty() {
            buffered
        } else {
            let disk = Scanner::scan_logs(blocks, start_ns, end_ns).await?;
            let mut all = disk;
            all.extend(buffered);
            all
        };

        Ok(logs
            .into_iter()
            .filter_map(|l| {
                let mut fields = std::collections::BTreeMap::new();
                fields.insert("timestamp_ns".to_string(), json!(l.timestamp_ns));
                fields.insert("body".to_string(), json!(l.body));
                fields.insert("severity".to_string(), json!(l.severity_text));
                fields.insert("severity_number".to_string(), json!(l.severity_number));
                fields.insert("trace_id".to_string(), json!(hex::encode(l.trace_id)));
                for (k, v) in l.attributes.iter() {
                    fields.insert(format!("attr.{k}"), json!(v));
                    fields.insert(k.to_string(), json!(v));
                }
                for (k, v) in l.resource_attributes.iter() {
                    fields.insert(k.to_string(), json!(v));
                }
                if let Some(svc) = l.resource_attributes.get("service.name") {
                    fields.insert("service".to_string(), json!(svc));
                }
                let row = Row { fields };
                match first_filter {
                    Some(pred) => crate::pipeline_exec::row_matches_pred(&row, pred).then_some(row),
                    None => Some(row),
                }
            })
            .collect())
    }

    /// Metrics → rows: one row per data point (G9a).
    async fn fetch_metric_rows(
        &self,
        start_ns: i64,
        end_ns: i64,
        first_filter: &Option<crate::logql::Predicate>,
    ) -> Result<Vec<Row>> {
        use serde_json::json;
        // Every metric name from the index AND the buffer (buffered
        // metrics aren't in the index until flush).
        let mut names: Vec<String> = {
            let idx = self.index.read().await;
            idx.all_metrics().into_iter().collect()
        };
        names.extend(self.buffer.metric_names().await);
        names.sort();
        names.dedup();
        let mut rows = Vec::new();
        for name in names {
            let blocks = {
                let idx = self.index.read().await;
                idx.query(start_ns, end_ns, Some(&name))
            };
            let mut points: Vec<parqtel_core::DataPoint> = if blocks.is_empty() {
                Vec::new()
            } else {
                // This loop enriches a correlate result across every metric
                // name, so there is no single selector and nothing to narrow
                // the service on.
                Scanner::scan(blocks, name.clone(), start_ns, end_ns, None).await?
            };
            points.extend(self.buffer.scan_metrics(&name, start_ns, end_ns).await);
            for p in points {
                let mut fields = std::collections::BTreeMap::new();
                fields.insert("timestamp_ns".to_string(), json!(p.timestamp_ns));
                fields.insert("__name__".to_string(), json!(name));
                let value = match &p.value {
                    parqtel_core::MetricValue::Double(v) => *v,
                    parqtel_core::MetricValue::Int(v) => *v as f64,
                    parqtel_core::MetricValue::Histogram(_)
                    | parqtel_core::MetricValue::Summary(_) => f64::NAN,
                };
                fields.insert("value".to_string(), json!(value));
                for (k, v) in p.labels.iter() {
                    fields.insert(k.to_string(), json!(v));
                }
                if let Some(svc) = p.labels.get("service.name") {
                    fields.insert("service".to_string(), json!(svc));
                }
                let row = Row { fields };
                if let Some(pred) = first_filter {
                    if !crate::pipeline_exec::row_matches_pred(&row, pred) {
                        continue;
                    }
                }
                rows.push(row);
            }
        }
        Ok(rows)
    }
}
