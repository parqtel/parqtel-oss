use std::time::Duration;
use thiserror::Error;

/// Errors returned by the alert routing, notifier, and configuration layers.
#[derive(Debug, Error)]
pub enum NotificationError {
    /// Permanent 4xx client failure (400, 401, 403, 404) — must NOT be retried.
    #[error("permanent delivery error (HTTP {status}): {message}")]
    Permanent { status: u16, message: String },

    /// Transient delivery error (429, 5xx, timeout, network) — eligible for retry.
    #[error("transient delivery error (status={status:?}): {message}")]
    Transient {
        status: Option<u16>,
        retry_after: Option<Duration>,
        message: String,
    },

    /// Invalid routing or receiver configuration.
    #[error("routing configuration error: {0}")]
    Config(String),

    /// Payload serialization error.
    #[error("notification serialization error: {0}")]
    Serialization(String),
}

impl NotificationError {
    /// Whether this error should be retried by the delivery worker.
    pub fn is_retryable(&self) -> bool {
        matches!(self, Self::Transient { .. })
    }

    /// Optional `Retry-After` hint from a `429 Too Many Requests` response.
    pub fn retry_after(&self) -> Option<Duration> {
        match self {
            Self::Transient { retry_after, .. } => *retry_after,
            _ => None,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_notification_error_retryability() {
        let perm = NotificationError::Permanent {
            status: 400,
            message: "bad payload".into(),
        };
        assert!(!perm.is_retryable());
        assert_eq!(perm.retry_after(), None);
        assert!(perm.to_string().contains("400"));

        let trans = NotificationError::Transient {
            status: Some(429),
            retry_after: Some(Duration::from_secs(5)),
            message: "rate limited".into(),
        };
        assert!(trans.is_retryable());
        assert_eq!(trans.retry_after(), Some(Duration::from_secs(5)));
        assert!(trans.to_string().contains("429"));

        let cfg = NotificationError::Config("dangling".into());
        assert!(!cfg.is_retryable());

        let ser = NotificationError::Serialization("bad json".into());
        assert!(!ser.is_retryable());
    }
}
