//! Tail sampling for traces: decide per trace which to persist.
//!
//! Semantics:
//! - Decisions are **trace-coherent**: the probabilistic rule hashes the
//!   trace_id so an entire trace is kept or dropped together — fragments
//!   of a dropped trace are never written.
//! - The span-metrics RED bridge runs on the FULL span set regardless of
//!   sampling, so derived RED metrics keep accurate rates. Only the stored
//!   (and buffered) span set is sampled.
//! - `keep_errors` and `slow_trace_ms` run BEFORE the probabilistic rule,
//!   so errors and slow traces are always retained even at low ratios.

use parqtel_core::config::TailSamplingConfig;
use parqtel_core::models::traces::Span;
use std::collections::hash_map::DefaultHasher;
use std::hash::{Hash, Hasher};

/// Span kinds that can serve as a trace's "root" for latency evaluation.
const SERVER_KIND: i32 = 2;

/// Decision outcome for a single trace.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum SampleDecision {
    Keep,
    Drop,
}

/// Decide whether a trace (given its spans) should be persisted.
pub fn decide(policy: &TailSamplingConfig, trace_id: &[u8; 16], spans: &[Span]) -> SampleDecision {
    if policy.keep_errors && spans.iter().any(|s| s.status.code == 2) {
        return SampleDecision::Keep;
    }
    if let Some(threshold_ms) = policy.slow_trace_ms {
        let threshold_ns = (threshold_ms as i128) * 1_000_000;
        let has_slow = spans
            .iter()
            .filter(|s| s.kind == SERVER_KIND)
            .any(|s| (s.duration_ns() as i128) >= threshold_ns);
        if has_slow {
            return SampleDecision::Keep;
        }
    }
    // Probabilistic rule — deterministic on trace_id.
    let ratio = policy.sampling_ratio.clamp(0.0, 1.0);
    if ratio >= 1.0 {
        return SampleDecision::Keep;
    }
    if ratio <= 0.0 {
        return SampleDecision::Drop;
    }
    let mut hasher = DefaultHasher::new();
    trace_id.hash(&mut hasher);
    let bucket = hasher.finish() % 10_000;
    if (bucket as f64) < ratio * 10_000.0 {
        SampleDecision::Keep
    } else {
        SampleDecision::Drop
    }
}

/// Partition a span batch into (kept spans, dropped span count).
///
/// Spans are grouped by trace_id; the per-service policy is selected by the
/// service.name attribute of the trace's spans (a trace normally belongs to
/// one service's export stream; mixed-service traces use the first span's
/// service for policy selection).
pub fn sample_spans(policy: &TailSamplingConfig, spans: Vec<Span>) -> (Vec<Span>, u64) {
    use std::collections::HashMap;

    if policy_is_keep_all(policy) {
        return (spans, 0);
    }

    let total = spans.len();
    let mut by_trace: HashMap<[u8; 16], Vec<Span>> = HashMap::new();
    for span in spans {
        by_trace.entry(span.trace_id).or_default().push(span);
    }

    let mut kept = Vec::with_capacity(total);
    let mut dropped: u64 = 0;
    for (_tid, group) in by_trace {
        let service = group
            .first()
            .and_then(|s| s.attributes.get("service.name"))
            .map(|v| v.to_string())
            .unwrap_or_default();
        let effective = policy_for_service(policy, &service);
        let tid = group.first().map(|s| s.trace_id).unwrap_or([0; 16]);
        match decide(effective, &tid, &group) {
            SampleDecision::Keep => kept.extend(group),
            SampleDecision::Drop => dropped += group.len() as u64,
        }
    }
    (kept, dropped)
}

/// Fast path: no overrides and a trivially keep-all global policy.
fn policy_is_keep_all(policy: &TailSamplingConfig) -> bool {
    policy.per_service.is_empty() && policy.sampling_ratio >= 1.0
}

fn policy_for_service<'a>(policy: &'a TailSamplingConfig, service: &str) -> &'a TailSamplingConfig {
    policy.per_service.get(service).unwrap_or(policy)
}

#[cfg(test)]
mod tests {
    #![allow(clippy::unwrap_used, clippy::expect_used)]
    use super::*;
    use parqtel_core::models::traces::SpanStatus;
    use parqtel_core::LabelSet;
    use std::collections::HashMap;

    fn span(trace: [u8; 16], kind: i32, status: i32, dur_ns: i64, service: &str) -> Span {
        Span {
            trace_id: trace,
            span_id: [1; 8],
            trace_state: String::new(),
            parent_span_id: [0; 8],
            name: "op".into(),
            kind,
            start_time_ns: 1_000,
            end_time_ns: 1_000 + dur_ns,
            attributes: LabelSet::try_from_iter(vec![("service.name", service.to_string())])
                .unwrap_or_default(),
            events: Vec::new(),
            links: Vec::new(),
            status: SpanStatus {
                code: status,
                message: String::new(),
            },
            flags: 0,
        }
    }

    fn policy(ratio: f64) -> TailSamplingConfig {
        TailSamplingConfig {
            keep_errors: false,
            slow_trace_ms: None,
            sampling_ratio: ratio,
            per_service: HashMap::new(),
        }
    }

    #[test]
    fn errors_always_kept_when_enabled() {
        let p = TailSamplingConfig {
            keep_errors: true,
            sampling_ratio: 0.0, // drop everything else
            ..Default::default()
        };
        let tid = [7; 16];
        let spans = vec![span(tid, SERVER_KIND, 2, 1_000, "api")];
        assert_eq!(decide(&p, &tid, &spans), SampleDecision::Keep);
    }

    #[test]
    fn slow_traces_kept() {
        let p = TailSamplingConfig {
            keep_errors: false,
            slow_trace_ms: Some(500),
            sampling_ratio: 0.0,
            ..Default::default()
        };
        let tid = [8; 16];
        // 600ms server span — kept; internal span would not count.
        let spans = vec![span(tid, SERVER_KIND, 0, 600_000_000, "api")];
        assert_eq!(decide(&p, &tid, &spans), SampleDecision::Keep);
        // 100ms — dropped (ratio 0).
        let fast = vec![span(tid, SERVER_KIND, 0, 100_000_000, "api")];
        assert_eq!(decide(&p, &tid, &fast), SampleDecision::Drop);
    }

    #[test]
    fn probabilistic_is_deterministic_per_trace() {
        let p = policy(0.5);
        let tid = [42; 16];
        let spans = vec![span(tid, SERVER_KIND, 0, 1_000, "api")];
        let first = decide(&p, &tid, &spans);
        for _ in 0..20 {
            assert_eq!(decide(&p, &tid, &spans), first);
        }
    }

    #[test]
    fn probabilistic_ratio_bounds() {
        let keep_all = policy(1.0);
        let tid = [1; 16];
        let spans = vec![span(tid, SERVER_KIND, 0, 1_000, "api")];
        assert_eq!(decide(&keep_all, &tid, &spans), SampleDecision::Keep);

        let drop_all = policy(0.0);
        assert_eq!(decide(&drop_all, &tid, &spans), SampleDecision::Drop);
    }

    #[test]
    fn sample_spans_groups_by_trace_coherently() {
        use std::collections::HashMap;
        let mut per_service = HashMap::new();
        // Drop "noisy" service entirely; keep everything else.
        per_service.insert(
            "noisy".to_string(),
            TailSamplingConfig {
                keep_errors: false,
                slow_trace_ms: None,
                sampling_ratio: 0.0,
                per_service: HashMap::new(),
            },
        );
        let p = TailSamplingConfig {
            keep_errors: false,
            slow_trace_ms: None,
            sampling_ratio: 1.0,
            per_service,
        };

        let t1 = [1; 16];
        let t2 = [2; 16];
        let spans = vec![
            span(t1, SERVER_KIND, 0, 1_000, "api"),
            span(t1, 1, 0, 500, "api"), // same trace, second span
            span(t2, SERVER_KIND, 0, 1_000, "noisy"),
            span(t2, 1, 0, 500, "noisy"),
        ];
        let (kept, dropped) = sample_spans(&p, spans);
        assert_eq!(dropped, 2);
        assert_eq!(kept.len(), 2);
        assert!(kept.iter().all(|s| s.trace_id == t1));
    }

    #[test]
    fn keep_all_policy_short_circuits() {
        let p = TailSamplingConfig::default();
        let spans = vec![span([9; 16], SERVER_KIND, 0, 1_000, "api")];
        let (kept, dropped) = sample_spans(&p, spans.clone());
        assert_eq!(kept.len(), spans.len());
        assert_eq!(dropped, 0);
    }

    #[test]
    fn approx_half_kept_at_half_ratio() {
        let p = policy(0.5);
        let mut kept = 0;
        let mut total = 0;
        for i in 0..200u8 {
            let tid = [i; 16];
            let spans = vec![span(tid, SERVER_KIND, 0, 1_000, "api")];
            total += 1;
            if decide(&p, &tid, &spans) == SampleDecision::Keep {
                kept += 1;
            }
        }
        // Statistical: 200 traces at 50% → expect 60–140.
        assert!(
            kept > 60 && kept < 140,
            "expected roughly half kept, got {kept}/{total}"
        );
    }
}
