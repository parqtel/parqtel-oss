use parqtel_core::Metric;

use crate::expr::CompiledCondition;
use crate::rule::schema::StageType;

use super::stage::{SignalRecord, Stage, StageResult};

/// Routes records to different destinations or drops them.
pub struct Router {
    pub name: String,
    pub condition: CompiledCondition,
    pub action: RouterAction,
}

#[derive(Debug, Clone)]
pub enum RouterAction {
    Drop,
    RouteTo(String),
}

impl Stage for Router {
    fn stage_name(&self) -> &str {
        &self.name
    }

    fn stage_type(&self) -> StageType {
        StageType::Router
    }

    fn process(&self, record: SignalRecord, _extracted: &mut Vec<Metric>) -> StageResult {
        if self.condition.evaluate(&record.fields) {
            match &self.action {
                RouterAction::Drop => StageResult::Drop,
                RouterAction::RouteTo(dest) => StageResult::RouteTo {
                    destination: dest.clone(),
                    record,
                },
            }
        } else {
            StageResult::Continue(record)
        }
    }
}
