use std::collections::BTreeMap;
use std::sync::Arc;

use tracing::info;

use crate::rule::schema::RecordingRuleGroup;
use crate::rule::validator::parse_duration;

use super::evaluator::RulerEvaluator;

/// Backfills missed evaluation intervals after a restart.
pub struct Backfiller {
    evaluator: Arc<RulerEvaluator>,
    max_backfill_intervals: u64,
}

impl Backfiller {
    pub fn new(evaluator: Arc<RulerEvaluator>, max_backfill_intervals: u64) -> Self {
        Self {
            evaluator,
            max_backfill_intervals,
        }
    }

    /// Check and backfill missed intervals for all groups.
    pub async fn backfill(
        &self,
        groups: &[RecordingRuleGroup],
        last_eval_state: &BTreeMap<String, i64>,
    ) -> crate::Result<BTreeMap<String, i64>> {
        let now_ns = chrono::Utc::now().timestamp_nanos_opt().unwrap_or(0);
        let mut updated_state = last_eval_state.clone();

        for group in groups {
            let interval_secs = parse_duration(&group.interval).unwrap_or(60);
            let interval_ns = interval_secs as i64 * 1_000_000_000;
            let last = last_eval_state.get(&group.name).copied().unwrap_or(0);

            if last == 0 {
                // No previous state, start from now
                updated_state.insert(group.name.clone(), now_ns);
                continue;
            }

            let missed = ((now_ns - last) / interval_ns).max(0) as u64;
            if missed <= 1 {
                continue;
            }

            let to_backfill = missed.min(self.max_backfill_intervals);
            info!(
                "Backfilling rule group '{}': evaluating {} missed intervals from {} to {}",
                group.name, to_backfill, last, now_ns
            );

            for i in 1..=to_backfill {
                let eval_ts = last + (i as i64 * interval_ns);
                if let Err(e) = self.evaluator.evaluate_group(group, eval_ts).await {
                    tracing::error!(
                        "Backfill failed for group '{}' at ts={}: {}",
                        group.name,
                        eval_ts,
                        e
                    );
                }
            }

            let final_ts = last + (to_backfill as i64 * interval_ns);
            updated_state.insert(group.name.clone(), final_ts);
        }

        Ok(updated_state)
    }
}

#[cfg(test)]
mod tests {
    #![allow(clippy::unwrap_used, clippy::expect_used, clippy::panic)]
    use super::*;
    use crate::ruler::evaluator::{QueryExecutor, RulerEvaluator};
    use async_trait::async_trait;
    use parqtel_core::config::BlockConfig;
    use parqtel_core::engine::parquet::ParquetStorageEngine;
    use parqtel_core::LabelSet;
    use tempfile::tempdir;

    struct CountingExecutor {
        call_count: Arc<std::sync::atomic::AtomicU64>,
    }

    #[async_trait]
    impl QueryExecutor for CountingExecutor {
        async fn instant_query(
            &self,
            _expr: &str,
            _timestamp_ns: i64,
        ) -> crate::Result<Vec<(LabelSet, f64)>> {
            self.call_count
                .fetch_add(1, std::sync::atomic::Ordering::SeqCst);
            let labels = LabelSet::try_from_iter(vec![("k", "v")]).unwrap();
            Ok(vec![(labels, 1.0)])
        }
    }

    #[tokio::test]
    async fn test_backfill_catches_up_missed_intervals() {
        let dir = tempdir().unwrap();
        let mut config = BlockConfig::default();
        config.data_dir = dir.path().to_path_buf();
        let storage = Arc::new(ParquetStorageEngine::new(config));

        let call_count = Arc::new(std::sync::atomic::AtomicU64::new(0));
        let executor = Arc::new(CountingExecutor {
            call_count: call_count.clone(),
        });
        let evaluator = Arc::new(RulerEvaluator::new(executor, storage));

        let backfiller = Backfiller::new(evaluator, 10);

        let group = RecordingRuleGroup {
            name: "backfill_test".into(),
            interval: "1m".into(),
            rules: vec![crate::rule::schema::RecordingRule {
                record: "test:backfill".into(),
                expr: "sum(x)".into(),
                labels: BTreeMap::new(),
                description: None,
                retention_override_days: None,
                for_duration: None,
            }],
        };

        // Simulate 5 missed intervals (5 minutes ago)
        let now_ns = chrono::Utc::now().timestamp_nanos_opt().unwrap_or(0);
        let five_min_ago = now_ns - (5 * 60 * 1_000_000_000);
        let mut state = BTreeMap::new();
        state.insert("backfill_test".into(), five_min_ago);

        backfiller.backfill(&[group], &state).await.unwrap();

        let calls = call_count.load(std::sync::atomic::Ordering::SeqCst);
        assert_eq!(calls, 5);
    }
}
