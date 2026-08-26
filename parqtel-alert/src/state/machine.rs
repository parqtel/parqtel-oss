use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};

/// Alert states.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum AlertState {
    Pending,
    Firing,
    Acknowledged,
    Resolved,
    Suppressed,
    NoiseFlagged,
}

/// A recorded state transition.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct StateTransition {
    pub from: AlertState,
    pub to: AlertState,
    pub at: DateTime<Utc>,
    pub reason: String,
}

/// Drives alert state transitions.
pub struct AlertStateMachine;

impl AlertStateMachine {
    /// Attempt a state transition. Returns the new state and a transition record,
    /// or None if the transition is invalid.
    pub fn transition(
        current: AlertState,
        event: TransitionEvent,
    ) -> Option<(AlertState, StateTransition)> {
        let (new_state, reason) = match (current, &event) {
            (AlertState::Pending, TransitionEvent::DurationElapsed) => (
                AlertState::Firing,
                "for_duration elapsed while condition met",
            ),
            (AlertState::Pending, TransitionEvent::ConditionCleared) => (
                AlertState::Resolved,
                "condition no longer met before duration elapsed",
            ),
            (AlertState::Firing, TransitionEvent::Acknowledged { .. }) => {
                (AlertState::Acknowledged, "alert acknowledged")
            }
            (AlertState::Firing, TransitionEvent::ConditionCleared) => {
                (AlertState::Resolved, "condition no longer met")
            }
            (AlertState::Firing, TransitionEvent::NoiseSuppressed) => (
                AlertState::Suppressed,
                "noise score exceeded suppression threshold",
            ),
            (AlertState::Firing, TransitionEvent::MarkedNoise) => {
                (AlertState::NoiseFlagged, "manually classified as noise")
            }
            (AlertState::Resolved, TransitionEvent::ConditionMet) => {
                (AlertState::Pending, "condition met again (re-firing)")
            }
            (AlertState::Suppressed, TransitionEvent::NoiseScoreDropped) => (
                AlertState::Firing,
                "noise score dropped below threshold (manual reset)",
            ),
            (AlertState::NoiseFlagged, TransitionEvent::AutoSuppressed) => (
                AlertState::Suppressed,
                "auto-suppressed after repeated noise flags",
            ),
            (AlertState::Acknowledged, TransitionEvent::ConditionCleared) => (
                AlertState::Resolved,
                "condition no longer met after acknowledgement",
            ),
            (AlertState::Acknowledged, TransitionEvent::AckWindowExpired) => (
                AlertState::Firing,
                "condition continues after acknowledgement window",
            ),
            _ => return None,
        };

        Some((
            new_state,
            StateTransition {
                from: current,
                to: new_state,
                at: Utc::now(),
                reason: reason.into(),
            },
        ))
    }
}

/// Events that can trigger state transitions.
#[derive(Debug, Clone)]
pub enum TransitionEvent {
    /// The for_duration has elapsed while condition remains met.
    DurationElapsed,
    /// The alert condition is no longer met.
    ConditionCleared,
    /// The alert condition is met (used for Resolved → Pending).
    ConditionMet,
    /// A human or AI acknowledged the alert.
    Acknowledged { by: String },
    /// Noise score exceeded the suppression threshold.
    NoiseSuppressed,
    /// Noise score dropped below threshold.
    NoiseScoreDropped,
    /// Manually or AI classified as noise.
    MarkedNoise,
    /// Auto-suppressed after 3 NoiseFlagged events.
    AutoSuppressed,
    /// Acknowledgement window expired while condition persists.
    AckWindowExpired,
}

#[cfg(test)]
mod tests {
    #![allow(clippy::unwrap_used, clippy::expect_used, clippy::panic)]
    use super::*;

    #[test]
    fn test_pending_to_firing() {
        let result =
            AlertStateMachine::transition(AlertState::Pending, TransitionEvent::DurationElapsed);
        let (state, transition) = result.unwrap();
        assert_eq!(state, AlertState::Firing);
        assert_eq!(transition.from, AlertState::Pending);
        assert_eq!(transition.to, AlertState::Firing);
    }

    #[test]
    fn test_pending_to_resolved() {
        let result =
            AlertStateMachine::transition(AlertState::Pending, TransitionEvent::ConditionCleared);
        let (state, _) = result.unwrap();
        assert_eq!(state, AlertState::Resolved);
    }

    #[test]
    fn test_firing_to_acknowledged() {
        let result = AlertStateMachine::transition(
            AlertState::Firing,
            TransitionEvent::Acknowledged { by: "user".into() },
        );
        let (state, _) = result.unwrap();
        assert_eq!(state, AlertState::Acknowledged);
    }

    #[test]
    fn test_firing_to_resolved() {
        let result =
            AlertStateMachine::transition(AlertState::Firing, TransitionEvent::ConditionCleared);
        let (state, _) = result.unwrap();
        assert_eq!(state, AlertState::Resolved);
    }

    #[test]
    fn test_firing_to_suppressed() {
        let result =
            AlertStateMachine::transition(AlertState::Firing, TransitionEvent::NoiseSuppressed);
        let (state, _) = result.unwrap();
        assert_eq!(state, AlertState::Suppressed);
    }

    #[test]
    fn test_firing_to_noise_flagged() {
        let result =
            AlertStateMachine::transition(AlertState::Firing, TransitionEvent::MarkedNoise);
        let (state, _) = result.unwrap();
        assert_eq!(state, AlertState::NoiseFlagged);
    }

    #[test]
    fn test_resolved_to_pending() {
        let result =
            AlertStateMachine::transition(AlertState::Resolved, TransitionEvent::ConditionMet);
        let (state, _) = result.unwrap();
        assert_eq!(state, AlertState::Pending);
    }

    #[test]
    fn test_suppressed_to_firing() {
        let result = AlertStateMachine::transition(
            AlertState::Suppressed,
            TransitionEvent::NoiseScoreDropped,
        );
        let (state, _) = result.unwrap();
        assert_eq!(state, AlertState::Firing);
    }

    #[test]
    fn test_invalid_transition_returns_none() {
        let result =
            AlertStateMachine::transition(AlertState::Resolved, TransitionEvent::DurationElapsed);
        assert!(result.is_none());
    }

    #[test]
    fn test_acknowledged_to_resolved() {
        let result = AlertStateMachine::transition(
            AlertState::Acknowledged,
            TransitionEvent::ConditionCleared,
        );
        let (state, _) = result.unwrap();
        assert_eq!(state, AlertState::Resolved);
    }

    #[test]
    fn test_acknowledged_window_expired() {
        let result = AlertStateMachine::transition(
            AlertState::Acknowledged,
            TransitionEvent::AckWindowExpired,
        );
        let (state, _) = result.unwrap();
        assert_eq!(state, AlertState::Firing);
    }

    #[test]
    fn test_alert_state_serialization() {
        let state = AlertState::Firing;
        let json = serde_json::to_string(&state).unwrap();
        let decoded: AlertState = serde_json::from_str(&json).unwrap();
        assert_eq!(decoded, AlertState::Firing);
    }
}
