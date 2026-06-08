use std::sync::Arc;
use std::time::Instant;
use tokio::sync::RwLock;
use std::collections::{BTreeMap, HashSet};
use serde_json::json;
use parqtel_core::{BlockIndex, Scanner, Result, LabelSet, MetricValue, StorageEngine, MemoryBuffer};
use crate::models::{QueryResult, TimeSeries, Sample, LogQueryResult, CorrelatedEvent, CorrelationResult};
use crate::plan::QueryPlan;
use crate::matcher::{evaluate_matchers, LabelMatcher};

/// Executes queries against the storage engine.
pub struct QueryExecutor {
    #[allow(dead_code)]
    storage: Arc<dyn StorageEngine>,
    index: Arc<RwLock<BlockIndex>>,
    log_index: Arc<RwLock<BlockIndex>>,
    trace_index: Arc<RwLock<BlockIndex>>,
    buffer: MemoryBuffer,
}

impl QueryExecutor {
    /// Creates a new [QueryExecutor] with shared block indexes.
    pub fn new(index: Arc<RwLock<BlockIndex>>, log_index: Arc<RwLock<BlockIndex>>) -> Self {
        let config = parqtel_core::BlockConfig::default();
        let storage: Arc<dyn StorageEngine> = Arc::new(
            parqtel_core::engine::parquet::ParquetStorageEngine::new(config)
        );
        let trace_index = Arc::new(RwLock::new(BlockIndex::new(std::path::Path::new("/tmp/parqtel-traces"))));
        Self { storage, index, log_index, trace_index, buffer: MemoryBuffer::new() }
    }

    /// Creates a new [QueryExecutor] with a memory buffer for stream-queryable data.
    pub fn with_buffer(
        index: Arc<RwLock<BlockIndex>>,
        log_index: Arc<RwLock<BlockIndex>>,
        buffer: MemoryBuffer,
    ) -> Self {
        let config = parqtel_core::BlockConfig::default();
        let storage: Arc<dyn StorageEngine> = Arc::new(
            parqtel_core::engine::parquet::ParquetStorageEngine::new(config)
        );
        let trace_index = Arc::new(RwLock::new(BlockIndex::new(std::path::Path::new("/tmp/parqtel-traces"))));
        Self { storage, index, log_index, trace_index, buffer }
    }

    /// Creates a new [QueryExecutor] with a trace index and memory buffer.
    pub fn with_trace_index(
        index: Arc<RwLock<BlockIndex>>,
        log_index: Arc<RwLock<BlockIndex>>,
        trace_index: Arc<RwLock<BlockIndex>>,
        buffer: MemoryBuffer,
    ) -> Self {
        let config = parqtel_core::BlockConfig::default();
        let storage: Arc<dyn StorageEngine> = Arc::new(
            parqtel_core::engine::parquet::ParquetStorageEngine::new(config)
        );
        Self { storage, index, log_index, trace_index, buffer }
    }

    /// Creates a new [QueryExecutor] with a storage engine and block indexes.
    pub fn with_engine(
        storage: Arc<dyn StorageEngine>,
        index: Arc<RwLock<BlockIndex>>,
        log_index: Arc<RwLock<BlockIndex>>,
    ) -> Self {
        let trace_index = Arc::new(RwLock::new(BlockIndex::new(std::path::Path::new("/tmp/parqtel-traces"))));
        Self { storage, index, log_index, trace_index, buffer: MemoryBuffer::new() }
    }

    /// Returns a clone of the memory buffer for use by ingestion services.
    pub fn memory_buffer(&self) -> MemoryBuffer {
        self.buffer.clone()
    }

    /// Executes a [QueryPlan] for metrics and returns a [QueryResult].
    pub async fn execute(&self, plan: QueryPlan) -> Result<QueryResult> {
        let start_time = Instant::now();

        // 1. Find candidate blocks
        let blocks = {
            let idx = self.index.read().await;
            idx.query(plan.start_ns, plan.end_ns, Some(&plan.metric_name))
        };

        if blocks.is_empty() {
            // Even with no disk blocks, check the in-memory buffer
            let buffered = self.buffer.scan_metrics(&plan.metric_name, plan.start_ns, plan.end_ns).await;
            if buffered.is_empty() {
                return Ok(QueryResult {
                    series: Vec::new(),
                    execution_time: start_time.elapsed(),
                    points_scanned: 0,
                    total_series_count: 0,
                    volume_summary: vec![0; 60],
                });
            }
            // Process buffered data through the same pipeline below
            let raw_points = buffered;
            let points_scanned = raw_points.len() as u64;
            let mut series_map: BTreeMap<u64, (LabelSet, Vec<(i64, MetricValue)>)> = BTreeMap::new();
            let mut matched_series_fps = HashSet::new();
            let mut volume_summary = vec![0u64; 60];
            let window_ns = (plan.end_ns - plan.start_ns) / 60;
            for dp in raw_points {
                if evaluate_matchers(&plan.matchers, &dp.labels, &plan.metric_name) {
                    if window_ns > 0 {
                        let bucket = ((dp.timestamp_ns - plan.start_ns) / window_ns).clamp(0, 59) as usize;
                        volume_summary[bucket] += 1;
                    }
                    let fp = dp.labels.fingerprint();
                    matched_series_fps.insert(fp);
                    if series_map.contains_key(&fp) || series_map.len() < plan.max_series {
                        let entry = series_map.entry(fp).or_insert_with(|| (dp.labels.clone(), Vec::new()));
                        if entry.1.len() < plan.max_samples_per_series {
                            entry.1.push((dp.timestamp_ns, dp.value));
                        }
                    }
                }
            }
            let total_series_count = matched_series_fps.len();
            let mut results = Vec::new();
            for (_, (mut labels, points)) in series_map {
                labels = labels.merge(&LabelSet::try_from_iter(vec![("__name__", plan.metric_name.clone())])?);
                let samples = if let Some(step) = plan.step_ns {
                    if let Some(op) = plan.aggregation {
                        crate::aggregation::downsample(points, plan.start_ns, plan.end_ns, step, op, plan.quantile, plan.scalar_param, plan.clamp)
                    } else {
                        points.into_iter().map(|(t, v)| Sample { timestamp_ns: t, value: v_to_f64(&v) }).collect()
                    }
                } else {
                    points.into_iter().map(|(t, v)| Sample { timestamp_ns: t, value: v_to_f64(&v) }).collect()
                };
                results.push(TimeSeries { labels, samples });
            }
            results = apply_post_processing(results, &plan);
            return Ok(QueryResult { series: results, execution_time: start_time.elapsed(), points_scanned, total_series_count, volume_summary });
        }

        // 2. Scan blocks concurrently
        let mut raw_points = Scanner::scan(
            blocks,
            plan.metric_name.clone(),
            plan.start_ns,
            plan.end_ns,
        ).await?;

        // 2b. Merge in-memory buffer data (not yet flushed to disk)
        let buffered = self.buffer.scan_metrics(&plan.metric_name, plan.start_ns, plan.end_ns).await;
        raw_points.extend(buffered);

        let points_scanned = raw_points.len() as u64;

        // 3. Filter and group by series + Calculate Volume
        let mut series_map: BTreeMap<u64, (LabelSet, Vec<(i64, MetricValue)>)> = BTreeMap::new();
        let mut matched_series_fps = HashSet::new();
        let mut volume_summary = vec![0u64; 60];
        let window_ns = (plan.end_ns - plan.start_ns) / 60;

        for dp in raw_points {
            if evaluate_matchers(&plan.matchers, &dp.labels, &plan.metric_name) {
                // Update volume histogram regardless of truncation
                if window_ns > 0 {
                    let bucket = ((dp.timestamp_ns - plan.start_ns) / window_ns).clamp(0, 59) as usize;
                    volume_summary[bucket] += 1;
                }

                let fp = dp.labels.fingerprint();
                matched_series_fps.insert(fp);

                // Check if we already have this series or if we can add a new one
                if series_map.contains_key(&fp) || series_map.len() < plan.max_series {
                    let entry = series_map.entry(fp).or_insert_with(|| (dp.labels.clone(), Vec::new()));
                    
                    if entry.1.len() < plan.max_samples_per_series {
                        entry.1.push((dp.timestamp_ns, dp.value));
                    }
                }
            }
        }

        let total_series_count = matched_series_fps.len();

        // 4. Downsample and format results
        let mut results = Vec::new();
        for (_, (mut labels, points)) in series_map {
            labels = labels.merge(&LabelSet::try_from_iter(vec![("__name__", plan.metric_name.clone())])?);
            
            let samples = if let Some(step) = plan.step_ns {
                if let Some(op) = plan.aggregation {
                    crate::aggregation::downsample(
                        points,
                        plan.start_ns,
                        plan.end_ns,
                        step,
                        op,
                        plan.quantile,
                        plan.scalar_param,
                        plan.clamp,
                    )
                } else {
                    points.into_iter().map(|(t, v)| Sample { timestamp_ns: t, value: v_to_f64(&v) }).collect()
                }
            } else {
                points.into_iter().map(|(t, v)| Sample { timestamp_ns: t, value: v_to_f64(&v) }).collect()
            };

            results.push(TimeSeries {
                labels,
                samples,
            });
        }
        results = apply_post_processing(results, &plan);

        Ok(QueryResult {
            series: results,
            execution_time: start_time.elapsed(),
            points_scanned,
            total_series_count,
            volume_summary,
        })
    }

    /// Executes a log query and returns matching [LogRecord]s.
    #[allow(clippy::too_many_arguments)]
    pub async fn query_logs(
        &self,
        start_ns: i64,
        end_ns: i64,
        matchers: Vec<crate::matcher::LabelMatcher>,
        limit: usize,
        order_desc: bool,
        severity_min: Option<i32>,
        search: Option<String>,
    ) -> Result<LogQueryResult> {
        let start_time = Instant::now();

        // Normalize matcher names: map user-friendly underscore names to OTel dotted names
        let matchers: Vec<crate::matcher::LabelMatcher> = matchers.into_iter().map(|mut m| {
            m.name = normalize_log_field_name(&m.name).to_string();
            m
        }).collect();

        let blocks = {
            let idx = self.log_index.read().await;
            idx.query(start_ns, end_ns, None)
        };

        // Scan in-memory buffer for unflushed logs
        let buffered_logs = self.buffer.scan_logs(start_ns, end_ns).await;

        if blocks.is_empty() && buffered_logs.is_empty() {
            return Ok(LogQueryResult {
                logs: Vec::new(),
                execution_time: start_time.elapsed(),
                total_logs_count: 0,
                volume_summary: vec![0; 60],
            }); 
        }

        let mut raw_logs = if blocks.is_empty() {
            buffered_logs
        } else {
            let mut disk_logs = Scanner::scan_logs(blocks, start_ns, end_ns).await?;
            disk_logs.extend(buffered_logs);
            disk_logs
        };
        
        // Apply ordering
        if order_desc {
            raw_logs.sort_by_key(|b| std::cmp::Reverse(b.timestamp_ns));
        } else {
            raw_logs.sort_by_key(|a| a.timestamp_ns);
        }

        let mut filtered = Vec::new();
        let mut volume_summary = vec![0u64; 60];
        let window_ns = (end_ns - start_ns) / 60;
        let mut total_logs_count = 0;
        let search_pattern = search.map(|s| s.to_lowercase());

        for log in raw_logs {
            // 1. Severity filter
            if let Some(min) = severity_min {
                if log.severity_number < min { continue; }
            }

            // 2. Search filter
            if let Some(ref pattern) = search_pattern {
                if !log.body.to_lowercase().contains(pattern) { continue; }
            }

            // 3. Label matchers — merge attributes with resource and include dedicated fields
            let mut all_labels = log.attributes.merge(&log.resource_attributes);
            all_labels = inject_dedicated_log_fields(all_labels, &log);
            if evaluate_log_matchers(&matchers, &all_labels) {
                total_logs_count += 1;
                
                if window_ns > 0 {
                    let bucket = ((log.timestamp_ns - start_ns) / window_ns).clamp(0, 59) as usize;
                    volume_summary[bucket] += 1;
                }

                if filtered.len() < limit {
                    filtered.push(log);
                }
            }
        }

        Ok(LogQueryResult {
            logs: filtered,
            execution_time: start_time.elapsed(),
            total_logs_count,
            volume_summary,
        })
    }

    /// Executes a trace query and returns matching spans.
    pub async fn query_traces(
        &self,
        start_ns: i64,
        end_ns: i64,
        trace_id_filter: Option<&str>,
        limit: usize,
    ) -> Result<Vec<parqtel_core::Span>> {
        let blocks = {
            let idx = self.trace_index.read().await;
            idx.query(start_ns, end_ns, None)
        };

        if blocks.is_empty() {
            return Ok(Vec::new());
        }

        let mut spans = Scanner::scan_traces(blocks, start_ns, end_ns).await?;

        // Filter by trace_id if provided
        if let Some(tid) = trace_id_filter {
            let tid_lower = tid.to_lowercase();
            spans.retain(|s| hex::encode(s.trace_id) == tid_lower);
        }

        spans.truncate(limit);
        Ok(spans)
    }

    /// Returns all known field names across the log schema.
    pub async fn get_log_fields(&self) -> (Vec<String>, Vec<String>) {
        let dedicated = vec![
            "service_name".into(), "service_version".into(),
            "k8s_namespace".into(), "k8s_pod_name".into(),
            "k8s_container_name".into(), "k8s_node_name".into(),
            "severity_text".into(), "body".into()
        ];
        
        // Sample common attributes from recent blocks
        let mut common = HashSet::new();
        let blocks = {
            let idx = self.log_index.read().await;
            let len = idx.blocks.len();
            let start = len.saturating_sub(3);
            idx.blocks[start..].to_vec()
        };

        for b in blocks {
            for name in b.label_names {
                if !dedicated.contains(&name) { common.insert(name); }
            }
        }

        (dedicated, common.into_iter().collect())
    }

    /// Returns distinct values for a given log field, sorted by frequency.
    pub async fn get_log_field_values(&self, field: &str, limit: usize) -> Vec<String> {
        let normalized = normalize_log_field_name(field);
        let mut freq = BTreeMap::new();
        let blocks = {
            let idx = self.log_index.read().await;
            let len = idx.blocks.len();
            let start = len.saturating_sub(5);
            idx.blocks[start..].to_vec()
        };

        for block in blocks {
            if let Ok(logs) = Scanner::scan_logs(vec![block], 0, i64::MAX).await {
                for l in logs {
                    let val = match normalized {
                        "severity_text" => Some(l.severity_text.as_str().to_string()),
                        "body" => Some(l.body.clone()),
                        _ => l.attributes.get(normalized).map(|v| v.to_string())
                                .or_else(|| l.resource_attributes.get(normalized).map(|v| v.to_string()))
                                .or_else(|| l.attributes.get(field).map(|v| v.to_string()))
                                .or_else(|| l.resource_attributes.get(field).map(|v| v.to_string())),
                    };
                    
                    if let Some(v) = val {
                        *freq.entry(v).or_insert(0u64) += 1;
                    }
                }
            }
        }

        let mut entries: Vec<_> = freq.into_iter().collect();
        entries.sort_by_key(|b| std::cmp::Reverse(b.1));
        entries.into_iter().take(limit).map(|(v, _)| v).collect()
    }

    /// Returns a list of all known metric names.
    pub async fn list_metrics(&self) -> HashSet<String> {
        let mut names = self.index.read().await.all_metrics();
        // Include metrics from in-memory buffer
        for n in self.buffer.metric_names().await {
            names.insert(n);
        }
        names
    }

    /// Returns all label names across both metrics and logs.
    pub async fn list_labels(&self, _metric: Option<&str>) -> HashSet<String> {
        let mut names = self.index.read().await.all_labels();
        names.extend(self.log_index.read().await.all_labels());
        // Include labels from in-memory buffer
        for n in self.buffer.label_names().await {
            names.insert(n);
        }
        names
    }

    /// Returns all values for a given label name across both metrics and logs.
    pub async fn list_label_values(&self, label: &str) -> HashSet<String> {
        let mut values = HashSet::new();

        let metric_blocks = {
            let idx = self.index.read().await;
            let len = idx.blocks.len();
            let start = len.saturating_sub(5);
            idx.blocks[start..].to_vec()
        };

        for block in metric_blocks {
            if !block.label_names.contains(label) { continue; }
            if let Ok(points) = Scanner::scan(vec![block], "".into(), 0, i64::MAX).await {
                for p in points { if let Some(v) = p.labels.get(label) { values.insert(v.to_string()); } }
            }
        }

        let log_blocks = {
            let idx = self.log_index.read().await;
            let len = idx.blocks.len();
            let start = len.saturating_sub(5);
            idx.blocks[start..].to_vec()
        };

        for block in log_blocks {
            if !block.label_names.contains(label) { continue; }
            if let Ok(logs) = Scanner::scan_logs(vec![block], 0, i64::MAX).await {
                for l in logs {
                    if let Some(v) = l.attributes.get(label) { values.insert(v.to_string()); }
                    if let Some(v) = l.resource_attributes.get(label) { values.insert(v.to_string()); }
                }
            }
        }

        values
    }

    /// Performs a cross-signal correlation query.
    pub async fn correlate(
        &self,
        _anchor_signal: &str,
        anchor_timestamp_ns: i64,
        anchor_labels: LabelSet,
        target_signal: &str,
        window_ns: i64,
        limit: usize,
    ) -> Result<CorrelationResult> {
        let start_time = Instant::now();

        // 1. Identify strongest dimension
        let (dimension, weight, matchers) = if let Some(trace_id) = anchor_labels.get("trace_id") {
            ("trace_id", 100, vec![LabelMatcher::equal("trace_id", trace_id)])
        } else if let Some(pod_uid) = anchor_labels.get("k8s_pod_uid") {
            ("k8s_pod_uid", 80, vec![LabelMatcher::equal("k8s_pod_uid", pod_uid)])
        } else if let (Some(pod), Some(ns)) = (anchor_labels.get("k8s_pod_name"), anchor_labels.get("k8s_namespace")) {
            ("k8s_pod_name+namespace", 60, vec![
                LabelMatcher::equal("k8s_pod_name", pod),
                LabelMatcher::equal("k8s_namespace", ns),
            ])
        } else if let Some(svc) = anchor_labels.get("service_name") {
            ("service_name", 40, vec![LabelMatcher::equal("service_name", svc)])
        } else if let Some(ns) = anchor_labels.get("k8s_namespace") {
            ("k8s_namespace", 20, vec![LabelMatcher::equal("k8s_namespace", ns)])
        } else {
            return Ok(CorrelationResult {
                correlation_dimension_used: "none".into(),
                correlated: Vec::new(),
                execution_time: start_time.elapsed(),
            });
        };

        let start_ns = anchor_timestamp_ns - window_ns;
        let end_ns = anchor_timestamp_ns + window_ns;

        let mut results = Vec::new();

        if target_signal == "log" {
            let res = self.query_logs(start_ns, end_ns, matchers, limit, true, None, None).await?;
            for log in res.logs {
                let time_delta = (log.timestamp_ns - anchor_timestamp_ns).abs();
                let time_score = (window_ns - time_delta).max(0) as f64 / window_ns as f64 * 100.0;
                results.push(CorrelatedEvent {
                    signal: "log".into(),
                    record: serde_json::to_value(&log).unwrap_or_default(),
                    score: weight as f64 * 1000.0 + time_score,
                    time_delta_ns: log.timestamp_ns - anchor_timestamp_ns,
                });
            }
        } else if target_signal == "metric" {
            // Find metrics matching the dimension
            let metric_names = {
                let idx = self.index.read().await;
                idx.query(start_ns, end_ns, None)
                    .into_iter()
                    .flat_map(|b| b.metric_names.clone())
                    .collect::<HashSet<_>>()
            };

            for metric_name in metric_names {
                let plan = QueryPlan::new(
                    metric_name,
                    matchers.clone(),
                    start_ns,
                    end_ns,
                    None,
                    limit,
                    100,
                    None,
                    None,
                )?;
                let res = self.execute(plan).await?;
                for series in res.series {
                    for sample in series.samples {
                        let time_delta = (sample.timestamp_ns - anchor_timestamp_ns).abs();
                        let time_score = (window_ns - time_delta).max(0) as f64 / window_ns as f64 * 100.0;
                        results.push(CorrelatedEvent {
                            signal: "metric".into(),
                            record: json!({
                                "metric": series.labels,
                                "value": sample.value,
                                "timestamp_ns": sample.timestamp_ns,
                            }),
                            score: weight as f64 * 1000.0 + time_score,
                            time_delta_ns: sample.timestamp_ns - anchor_timestamp_ns,
                        });
                    }
                }
            }
        }

        results.sort_by(|a, b| b.score.partial_cmp(&a.score).unwrap_or(std::cmp::Ordering::Equal));
        results.truncate(limit);

        Ok(CorrelationResult {
            correlation_dimension_used: dimension.into(),
            correlated: results,
            execution_time: start_time.elapsed(),
        })
    }
}

fn v_to_f64(v: &MetricValue) -> f64 {
    match v {
        MetricValue::Double(f) => *f,
        MetricValue::Int(i) => *i as f64,
        MetricValue::Histogram { sum, .. } => *sum,
        MetricValue::Summary { sum, .. } => *sum,
    }
}

/// Applies grouping (by/without), topk/bottomk ranking, and label_replace
/// to the per-series result set produced by the executor.
fn apply_post_processing(
    mut series: Vec<crate::models::TimeSeries>,
    plan: &crate::plan::QueryPlan,
) -> Vec<crate::models::TimeSeries> {
    // ── by / without grouping ──────────────────────────────────────────────
    if !plan.group_by.is_empty() || !plan.group_without.is_empty() {
        use std::collections::HashMap;
        let mut grouped: HashMap<Vec<(String, String)>, crate::models::TimeSeries> = HashMap::new();
        for ts in series {
            let key: Vec<(String, String)> = if !plan.group_by.is_empty() {
                plan.group_by.iter()
                    .filter_map(|l| ts.labels.get(l).map(|v| (l.clone(), v.to_string())))
                    .collect()
            } else {
                ts.labels.iter()
                    .filter(|(k, _)| !plan.group_without.contains(&k.to_string()))
                    .map(|(k, v)| (k.to_string(), v.to_string()))
                    .collect()
            };
            let entry = grouped.entry(key.clone()).or_insert_with(|| {
                let new_labels = LabelSet::try_from_iter(key).unwrap_or_default();
                crate::models::TimeSeries { labels: new_labels, samples: Vec::new() }
            });
            for s in ts.samples {
                if let Some(existing) = entry.samples.iter_mut().find(|e| e.timestamp_ns == s.timestamp_ns) {
                    existing.value += s.value;
                } else {
                    entry.samples.push(s);
                }
            }
        }
        series = grouped.into_values().collect();
        for ts in &mut series {
            ts.samples.sort_by_key(|s| s.timestamp_ns);
        }
    }

    // ── label_replace ──────────────────────────────────────────────────────
    if let Some((dst, repl, src, regex)) = &plan.label_replace {
        if let Ok(re) = regex::Regex::new(regex) {
            for ts in &mut series {
                let src_val = ts.labels.get(src).unwrap_or("").to_string();
                let new_val = re.replace(&src_val, repl.as_str()).to_string();
                let patch = LabelSet::try_from_iter(std::iter::once((dst.as_str(), new_val.as_str())));
                if let Ok(patch) = patch {
                    ts.labels = ts.labels.merge(&patch);
                }
            }
        }
    }

    // ── topk / bottomk ────────────────────────────────────────────────────
    if let Some(n) = plan.topk_n {
        let last_val = |ts: &crate::models::TimeSeries| ts.samples.last().map(|s| s.value).unwrap_or(0.0);
        match plan.aggregation {
            Some(crate::plan::AggregationOp::TopK) => {
                series.sort_by(|a, b| last_val(b).partial_cmp(&last_val(a)).unwrap_or(std::cmp::Ordering::Equal));
                series.truncate(n);
            }
            Some(crate::plan::AggregationOp::BottomK) => {
                series.sort_by(|a, b| last_val(a).partial_cmp(&last_val(b)).unwrap_or(std::cmp::Ordering::Equal));
                series.truncate(n);
            }
            _ => {}
        }
    }

    series
}


/// Maps user-friendly underscore field names to OTel dotted key names.
/// Passes through names that don't need mapping (including already-dotted names).
fn normalize_log_field_name(name: &str) -> &str {
    match name {
        "service_name" => "service.name",
        "service_version" => "service.version",
        "k8s_namespace" => "k8s.namespace.name",
        "k8s_pod_name" => "k8s.pod.name",
        "k8s_pod_uid" => "k8s.pod.uid",
        "k8s_container_name" => "k8s.container.name",
        "k8s_node_name" => "k8s.node.name",
        other => other,
    }
}

/// Evaluates log matchers trying both the normalized (dotted) name and the original name.
fn evaluate_log_matchers(matchers: &[LabelMatcher], labels: &LabelSet) -> bool {
    for m in matchers {
        let val = labels.get(&m.name)
            .or_else(|| labels.get(normalize_log_field_name(&m.name)));
        if !m.matches(val) {
            return false;
        }
    }
    true
}

/// Injects dedicated LogRecord struct fields into a LabelSet for matching.
fn inject_dedicated_log_fields(labels: LabelSet, log: &parqtel_core::LogRecord) -> LabelSet {
    let mut owned_pairs: Vec<(String, String)> = Vec::new();
    if !log.severity_text.is_empty() {
        owned_pairs.push(("severity_text".into(), log.severity_text.clone()));
    }
    if !log.body.is_empty() {
        owned_pairs.push(("body".into(), log.body.clone()));
    }
    if log.trace_id != [0u8; 16] {
        owned_pairs.push(("trace_id".into(), hex::encode(log.trace_id)));
    }
    if owned_pairs.is_empty() {
        return labels;
    }
    let extra = LabelSet::try_from_iter(owned_pairs).unwrap_or_default();
    labels.merge(&extra)
}


#[cfg(test)]
mod tests {
    #![allow(clippy::unwrap_used, clippy::expect_used, clippy::panic)]
    use super::*;
    use std::path::Path;
    use parqtel_core::BlockConfig;
    use parqtel_core::engine::parquet::ParquetStorageEngine;
    use tempfile::tempdir;

    /// Helper: create a ParquetStorageEngine-backed executor with real parquet files.
    async fn setup_with_data() -> (QueryExecutor, tempfile::TempDir) {
        let dir = tempdir().unwrap();
        let config = BlockConfig {
            data_dir: dir.path().to_path_buf(),
            ..Default::default()
        };
        let engine = ParquetStorageEngine::new(config);
        let storage: Arc<dyn StorageEngine> = Arc::new(engine);

        // Write metrics
        let m1 = parqtel_core::Metric {
            name: "cpu".into(),
            kind: parqtel_core::MetricKind::Gauge,
            resource_attributes: LabelSet::try_from_iter(vec![("service_name", "web")]).unwrap(),
            data_points: vec![
                parqtel_core::DataPoint::new(1000, parqtel_core::MetricValue::Double(10.0),
                    LabelSet::try_from_iter(vec![("host", "h1")]).unwrap()).unwrap(),
                parqtel_core::DataPoint::new(2000, parqtel_core::MetricValue::Double(20.0),
                    LabelSet::try_from_iter(vec![("host", "h1")]).unwrap()).unwrap(),
                parqtel_core::DataPoint::new(3000, parqtel_core::MetricValue::Double(30.0),
                    LabelSet::try_from_iter(vec![("host", "h2")]).unwrap()).unwrap(),
            ],
            ..Default::default()
        };
        let m2 = parqtel_core::Metric {
            name: "mem".into(),
            kind: parqtel_core::MetricKind::Gauge,
            data_points: vec![
                parqtel_core::DataPoint::new(1500, parqtel_core::MetricValue::Double(512.0),
                    LabelSet::try_from_iter(vec![("host", "h1")]).unwrap()).unwrap(),
            ],
            ..Default::default()
        };
        storage.write_metrics_batch(vec![m1, m2]).await.unwrap();

        // Write logs
        let log = parqtel_core::LogRecord::new(
            2000, 2000, 9, "INFO".into(), "request handled".into(),
            LabelSet::try_from_iter(vec![("service_name", "web")]).unwrap(),
            LabelSet::try_from_iter(vec![("k8s_namespace", "prod")]).unwrap(),
            [0u8; 16], [0u8; 8], 0, "".into(), "".into(),
        );
        storage.write_logs_batch(vec![log]).await.unwrap();

        // Load indexes from what the engine wrote
        let _snapshot = storage.metric_index_snapshot().await.unwrap();
        let mut index = BlockIndex::new(dir.path());
        index.load().unwrap();

        let log_dir = dir.path().join("logs");
        let mut log_index = BlockIndex::new(&log_dir);
        log_index.load().unwrap();

        let index = Arc::new(RwLock::new(index));
        let log_index = Arc::new(RwLock::new(log_index));
        let exec = QueryExecutor::with_engine(storage, index, log_index);
        (exec, dir)
    }

    #[tokio::test]
    async fn test_query_executor_empty() {
        let index = Arc::new(RwLock::new(BlockIndex::new(Path::new("/tmp/ne"))));
        let log_index = Arc::new(RwLock::new(BlockIndex::new(Path::new("/tmp/ne"))));
        let exec = QueryExecutor::new(index, log_index);
        let plan = QueryPlan::new("cpu".into(), vec![], 0, 100, None, 10, 100, None, None).unwrap();
        let res = exec.execute(plan).await.unwrap();
        assert_eq!(res.series.len(), 0);
        assert_eq!(res.points_scanned, 0);
        assert_eq!(res.volume_summary.len(), 60);
    }

    #[tokio::test]
    async fn test_execute_with_real_data() {
        let (exec, _dir) = setup_with_data().await;
        let plan = QueryPlan::new("cpu".into(), vec![], 0, 5000, None, 10, 100, None, None).unwrap();
        let res = exec.execute(plan).await.unwrap();
        assert_eq!(res.points_scanned, 3);
        assert!(!res.series.is_empty());
        assert_eq!(res.total_series_count, 2); // h1 and h2
    }

    #[tokio::test]
    async fn test_execute_with_label_matcher() {
        let (exec, _dir) = setup_with_data().await;
        let matchers = vec![LabelMatcher::equal("host", "h1")];
        let plan = QueryPlan::new("cpu".into(), matchers, 0, 5000, None, 10, 100, None, None).unwrap();
        let res = exec.execute(plan).await.unwrap();
        assert_eq!(res.total_series_count, 1);
        assert_eq!(res.series[0].samples.len(), 2);
    }

    #[tokio::test]
    async fn test_execute_max_series_truncation() {
        let (exec, _dir) = setup_with_data().await;
        let plan = QueryPlan::new("cpu".into(), vec![], 0, 5000, None, 1, 100, None, None).unwrap();
        let res = exec.execute(plan).await.unwrap();
        // max_series=1 so only 1 series returned, but total_series_count reflects all matched
        assert_eq!(res.series.len(), 1);
        assert_eq!(res.total_series_count, 2);
    }

    #[tokio::test]
    async fn test_execute_with_step_aggregation() {
        let (exec, _dir) = setup_with_data().await;
        let plan = QueryPlan::new(
            "cpu".into(), vec![], 0, 5000, Some(5000), 10, 100,
            Some(crate::plan::AggregationOp::Avg), None,
        ).unwrap();
        let res = exec.execute(plan).await.unwrap();
        assert!(!res.series.is_empty());
    }

    #[tokio::test]
    async fn test_query_logs_empty() {
        let index = Arc::new(RwLock::new(BlockIndex::new(Path::new("/tmp/ne"))));
        let log_index = Arc::new(RwLock::new(BlockIndex::new(Path::new("/tmp/ne"))));
        let exec = QueryExecutor::new(index, log_index);
        let res = exec.query_logs(0, 100, vec![], 10, false, None, None).await.unwrap();
        assert!(res.logs.is_empty());
        assert_eq!(res.total_logs_count, 0);
    }

    #[tokio::test]
    async fn test_query_logs_with_data() {
        let (exec, _dir) = setup_with_data().await;
        let res = exec.query_logs(0, 5000, vec![], 10, false, None, None).await.unwrap();
        assert_eq!(res.logs.len(), 1);
        assert_eq!(res.total_logs_count, 1);
    }

    #[tokio::test]
    async fn test_query_logs_severity_filter() {
        let (exec, _dir) = setup_with_data().await;
        // severity_min=13 (WARN) should filter out our INFO (9) log
        let res = exec.query_logs(0, 5000, vec![], 10, false, Some(13), None).await.unwrap();
        assert!(res.logs.is_empty());
    }

    #[tokio::test]
    async fn test_query_logs_search_filter() {
        let (exec, _dir) = setup_with_data().await;
        let res = exec.query_logs(0, 5000, vec![], 10, false, None, Some("request".into())).await.unwrap();
        assert_eq!(res.logs.len(), 1);
        let res2 = exec.query_logs(0, 5000, vec![], 10, false, None, Some("nonexistent".into())).await.unwrap();
        assert!(res2.logs.is_empty());
    }

    #[tokio::test]
    async fn test_query_logs_ordering() {
        let (exec, _dir) = setup_with_data().await;
        let res = exec.query_logs(0, 5000, vec![], 10, true, None, None).await.unwrap();
        assert_eq!(res.logs.len(), 1); // only 1 log, ordering still works
    }

    #[tokio::test]
    async fn test_list_metrics() {
        let (exec, _dir) = setup_with_data().await;
        let metrics = exec.list_metrics().await;
        assert!(metrics.contains("cpu"));
        assert!(metrics.contains("mem"));
    }

    #[tokio::test]
    async fn test_list_labels() {
        let (exec, _dir) = setup_with_data().await;
        let labels = exec.list_labels(None).await;
        assert!(labels.contains("host"));
    }

    #[tokio::test]
    async fn test_list_label_values() {
        let (exec, _dir) = setup_with_data().await;
        let values = exec.list_label_values("host").await;
        assert!(values.contains("h1"));
        assert!(values.contains("h2"));
    }

    #[tokio::test]
    async fn test_get_log_fields_with_data() {
        let (exec, _dir) = setup_with_data().await;
        let (dedicated, _common) = exec.get_log_fields().await;
        assert!(dedicated.contains(&"body".to_string()));
        assert!(dedicated.contains(&"service_name".to_string()));
    }

    #[tokio::test]
    async fn test_get_log_field_values() {
        let (exec, _dir) = setup_with_data().await;
        let values = exec.get_log_field_values("service_name", 10).await;
        assert!(values.contains(&"web".to_string()));
    }

    #[tokio::test]
    async fn test_correlation_with_service_name() {
        let (exec, _dir) = setup_with_data().await;
        let labels = LabelSet::try_from_iter(vec![("service_name", "web")]).unwrap();
        let res = exec.correlate("metric", 2000, labels, "log", 5000, 10).await.unwrap();
        assert_eq!(res.correlation_dimension_used, "service_name");
    }

    #[tokio::test]
    async fn test_correlation_pod_uid_dimension() {
        let (exec, _dir) = setup_with_data().await;
        let labels = LabelSet::try_from_iter(vec![("k8s_pod_uid", "uid-123")]).unwrap();
        let res = exec.correlate("metric", 2000, labels, "log", 5000, 10).await.unwrap();
        assert_eq!(res.correlation_dimension_used, "k8s_pod_uid");
    }

    #[tokio::test]
    async fn test_correlation_pod_namespace_dimension() {
        let (exec, _dir) = setup_with_data().await;
        let labels = LabelSet::try_from_iter(vec![
            ("k8s_pod_name", "pod-1"), ("k8s_namespace", "prod"),
        ]).unwrap();
        let res = exec.correlate("metric", 2000, labels, "log", 5000, 10).await.unwrap();
        assert_eq!(res.correlation_dimension_used, "k8s_pod_name+namespace");
    }

    #[tokio::test]
    async fn test_correlation_namespace_only() {
        let (exec, _dir) = setup_with_data().await;
        let labels = LabelSet::try_from_iter(vec![("k8s_namespace", "prod")]).unwrap();
        let res = exec.correlate("metric", 2000, labels, "log", 5000, 10).await.unwrap();
        assert_eq!(res.correlation_dimension_used, "k8s_namespace");
    }

    #[tokio::test]
    async fn test_correlation_metric_target() {
        let (exec, _dir) = setup_with_data().await;
        let labels = LabelSet::try_from_iter(vec![("service_name", "web")]).unwrap();
        let res = exec.correlate("log", 2000, labels, "metric", 5000, 10).await.unwrap();
        assert_eq!(res.correlation_dimension_used, "service_name");
    }

    #[tokio::test]
    async fn test_empty_correlation_no_dimension() {
        let (exec, _dir) = setup_with_data().await;
        let labels = LabelSet::try_from_iter(vec![("random_key", "val")]).unwrap();
        let res = exec.correlate("metric", 2000, labels, "log", 5000, 10).await.unwrap();
        assert_eq!(res.correlation_dimension_used, "none");
        assert!(res.correlated.is_empty());
    }

    #[test]
    fn test_v_to_f64() {
        assert_eq!(v_to_f64(&parqtel_core::MetricValue::Double(3.15)), 3.15);
        assert_eq!(v_to_f64(&parqtel_core::MetricValue::Int(42)), 42.0);
        assert_eq!(v_to_f64(&parqtel_core::MetricValue::Histogram {
            count: 1, sum: 99.0, min: None, max: None, boundaries: vec![], counts: vec![],
        }), 99.0);
        assert_eq!(v_to_f64(&parqtel_core::MetricValue::Summary {
            count: 1, sum: 55.0, quantiles: vec![],
        }), 55.0);
    }

    #[tokio::test]
    async fn test_execute_volume_summary() {
        let (exec, _dir) = setup_with_data().await;
        let plan = QueryPlan::new("cpu".into(), vec![], 0, 5000, None, 10, 100, None, None).unwrap();
        let res = exec.execute(plan).await.unwrap();
        assert_eq!(res.volume_summary.len(), 60);
        let total: u64 = res.volume_summary.iter().sum();
        assert_eq!(total, 3); // 3 data points
    }

    #[tokio::test]
    async fn test_execute_max_samples_per_series() {
        let (exec, _dir) = setup_with_data().await;
        let plan = QueryPlan::new("cpu".into(), vec![], 0, 5000, None, 10, 1, None, None).unwrap();
        let res = exec.execute(plan).await.unwrap();
        // Each series should have at most 1 sample
        for s in &res.series {
            assert!(s.samples.len() <= 1);
        }
    }
}
