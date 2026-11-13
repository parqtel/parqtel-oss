use crate::rule::types::Condition;

/// Evaluate a threshold condition against a metric value.
pub fn evaluate_threshold(condition: &Condition, value: f64) -> bool {
    condition.evaluate(value)
}
