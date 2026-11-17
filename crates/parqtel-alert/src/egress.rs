//! Outbound URL policy for alert delivery.
//!
//! Alert receiver URLs are configuration, and in the legacy router they can be
//! replaced at runtime over the API. A URL is therefore attacker-controlled in
//! the realistic deployment (an authenticated proxy user, or anyone who can
//! reach the unauthenticated endpoint). This module rejects destinations that
//! can reach the host itself or a private network before a request is made.
//!
//! The policy is checked on every send, not only when a notifier is built:
//! DNS answers can change after startup, and runtime route updates can add a
//! URL later. It is deliberately fail-closed: an unparseable URL, a missing
//! host, or a failed DNS lookup is rejected rather than attempted.
//!
//! Residual risk: the check resolves the name, then `reqwest` resolves it
//! again to connect. A DNS-rebinding attacker could answer differently between
//! the two lookups. Redirects are disabled, which removes the other common
//! bypass. Pinning the resolved address into the connection would require a
//! custom connector; the allowlist and `allow_private` are the escape hatches
//! for legitimate internal receivers.

use parqtel_core::config::EgressConfig;
use std::net::IpAddr;

/// Why an outbound URL was rejected.
#[derive(Debug, Clone, PartialEq, Eq, thiserror::Error)]
pub enum EgressError {
    #[error("invalid outbound URL: {0}")]
    InvalidUrl(String),
    #[error("outbound URL is not permitted: {0}")]
    Blocked(String),
    #[error("failed to resolve outbound host: {0}")]
    Resolve(String),
}

/// A validated destination policy shared by every notifier.
#[derive(Debug, Clone, Default)]
pub struct EgressPolicy {
    allow_private: bool,
    allow_hosts: Vec<String>,
}

impl EgressPolicy {
    pub fn new(allow_private: bool, allow_hosts: Vec<String>) -> Self {
        Self {
            allow_private,
            allow_hosts,
        }
    }

    pub fn from_config(config: &EgressConfig) -> Self {
        Self::new(config.allow_private, config.allow_hosts.clone())
    }

    /// Policy for tests and explicit operator opt-in: every destination is
    /// allowed, including loopback mock servers.
    pub fn permissive() -> Self {
        Self::new(true, Vec::new())
    }

    pub fn allow_private(&self) -> bool {
        self.allow_private
    }

    /// Validate a destination immediately before use.
    pub async fn check_url(&self, raw: &str) -> Result<(), EgressError> {
        let url = reqwest::Url::parse(raw)
            .map_err(|e| EgressError::InvalidUrl(format!("{raw:?}: {e}")))?;

        match url.scheme() {
            "http" | "https" => {}
            other => {
                return Err(EgressError::Blocked(format!(
                    "scheme {other:?} is not http or https"
                )))
            }
        }
        if !url.username().is_empty() || url.password().is_some() {
            return Err(EgressError::Blocked(
                "URLs with embedded credentials are not allowed".into(),
            ));
        }

        let host = url
            .host_str()
            .ok_or_else(|| EgressError::InvalidUrl("URL has no host".into()))?;
        let host = host.trim_start_matches('[').trim_end_matches(']');

        if self.host_allowed(host) {
            return Ok(());
        }

        let port = url.port_or_known_default().ok_or_else(|| {
            EgressError::InvalidUrl("URL has no known port for its scheme".into())
        })?;

        if let Ok(ip) = host.parse::<IpAddr>() {
            return self.check_ip(ip);
        }

        let addrs = tokio::net::lookup_host((host, port))
            .await
            .map_err(|e| EgressError::Resolve(format!("{host}:{port}: {e}")))?;

        let mut seen = false;
        for addr in addrs {
            seen = true;
            self.check_ip(addr.ip())?;
        }
        if !seen {
            return Err(EgressError::Resolve(format!(
                "{host}:{port} returned no addresses"
            )));
        }
        Ok(())
    }

    fn host_allowed(&self, host: &str) -> bool {
        self.allow_hosts.iter().any(|pattern| {
            let pattern = pattern.trim();
            if let Some(suffix) = pattern.strip_prefix("*.") {
                !suffix.is_empty()
                    && (host.eq_ignore_ascii_case(suffix)
                        || host
                            .to_ascii_lowercase()
                            .ends_with(&format!(".{}", suffix.to_ascii_lowercase())))
            } else {
                host.eq_ignore_ascii_case(pattern)
            }
        })
    }

    fn check_ip(&self, ip: IpAddr) -> Result<(), EgressError> {
        if self.allow_private {
            return Ok(());
        }
        if is_blocked_ip(ip) {
            return Err(EgressError::Blocked(format!(
                "{ip} is a loopback, private, link-local, CGNAT, ULA, multicast, or reserved address"
            )));
        }
        Ok(())
    }
}

/// Whether an address must not be reached by an alert receiver.
///
/// This covers the ranges that can reach the host's metadata service, the
/// host's own APIs, or another service on a private network. It is
/// intentionally broader than "private" so documentation and benchmark ranges
/// cannot be used as a blind spot.
fn is_blocked_ip(ip: IpAddr) -> bool {
    match ip {
        IpAddr::V4(v4) => {
            let octets = v4.octets();
            v4.is_unspecified()
                || v4.is_loopback()
                || v4.is_private()
                || v4.is_link_local()
                || v4.is_multicast()
                || v4.is_broadcast()
                // 100.64.0.0/10 — CGNAT, used by some cloud metadata paths.
                || (octets[0] == 100 && (64..=127).contains(&octets[1]))
                // 198.18.0.0/15 — benchmarking.
                || (octets[0] == 198 && (18..=19).contains(&octets[1]))
                // 192.0.0.0/24 — IETF protocol assignments.
                || (octets[0] == 192 && octets[1] == 0 && octets[2] == 0)
                // 240.0.0.0/4 — reserved, including 255.255.255.255.
                || octets[0] >= 240
        }
        IpAddr::V6(v6) => {
            if let Some(mapped) = v6.to_ipv4_mapped() {
                return is_blocked_ip(IpAddr::V4(mapped));
            }
            let segments = v6.segments();
            v6.is_unspecified()
                || v6.is_loopback()
                || v6.is_multicast()
                // fc00::/7 — unique local addresses.
                || (segments[0] & 0xfe00) == 0xfc00
                // fe80::/10 — link-local.
                || (segments[0] & 0xffc0) == 0xfe80
                // 2001:db8::/32 — documentation.
                || (segments[0] == 0x2001 && segments[1] == 0x0db8)
        }
    }
}

/// Wraps a notifier so its destination is checked against the policy on every
/// send. `build_notifiers` uses this for all configured receivers.
pub struct EgressCheckedNotifier {
    inner: std::sync::Arc<dyn crate::notifier::Notifier>,
    policy: EgressPolicy,
}

impl EgressCheckedNotifier {
    pub fn new(inner: std::sync::Arc<dyn crate::notifier::Notifier>, policy: EgressPolicy) -> Self {
        Self { inner, policy }
    }
}

#[async_trait::async_trait]
impl crate::notifier::Notifier for EgressCheckedNotifier {
    fn name(&self) -> &str {
        self.inner.name()
    }

    fn channel_type(&self) -> &'static str {
        self.inner.channel_type()
    }

    fn send_resolved(&self) -> bool {
        self.inner.send_resolved()
    }

    fn target_url(&self) -> Option<&str> {
        self.inner.target_url()
    }

    async fn send(
        &self,
        batch: &crate::notifier::NotificationBatch,
    ) -> Result<(), crate::error::NotificationError> {
        let Some(url) = self.inner.target_url() else {
            return Err(crate::error::NotificationError::Config(format!(
                "notifier {:?} has no destination URL to validate",
                self.inner.name()
            )));
        };
        self.policy
            .check_url(url)
            .await
            .map_err(|e| crate::error::NotificationError::Egress(e.to_string()))?;
        self.inner.send(batch).await
    }
}

#[cfg(test)]
mod tests {
    #![allow(clippy::unwrap_used, clippy::expect_used, clippy::panic)]
    use super::*;
    use std::net::{Ipv4Addr, Ipv6Addr};

    #[test]
    fn blocks_metadata_and_private_ranges() {
        for ip in [
            "127.0.0.1",
            "127.255.255.254",
            "10.1.2.3",
            "172.16.0.1",
            "172.31.255.255",
            "192.168.1.1",
            "169.254.169.254",
            "100.64.0.1",
            "100.127.255.255",
            "0.0.0.0",
            "224.0.0.1",
            "255.255.255.255",
            "198.18.0.1",
            "192.0.0.1",
            "240.0.0.1",
        ] {
            assert!(
                is_blocked_ip(ip.parse().unwrap()),
                "expected {ip} to be blocked"
            );
        }
    }

    #[test]
    fn allows_public_ranges() {
        for ip in [
            "1.1.1.1",
            "8.8.8.8",
            "93.184.216.34",
            "2606:4700:4700::1111",
        ] {
            assert!(
                !is_blocked_ip(ip.parse().unwrap()),
                "expected {ip} to be allowed"
            );
        }
    }

    #[test]
    fn blocks_ipv6_private_ranges() {
        for ip in ["::1", "::", "fc00::1", "fd12:3456::1", "fe80::1", "ff02::1"] {
            assert!(
                is_blocked_ip(ip.parse().unwrap()),
                "expected {ip} to be blocked"
            );
        }
        assert!(is_blocked_ip(IpAddr::V6(Ipv6Addr::new(
            0, 0, 0, 0, 0, 0xffff, 0x7f00, 0x0001
        ))));
        assert!(!is_blocked_ip(IpAddr::V6(Ipv6Addr::new(
            0x2606, 0x4700, 0x4700, 0, 0, 0, 0, 0x1111
        ))));
    }

    #[tokio::test]
    async fn rejects_loopback_and_private_literals_before_connecting() {
        let policy = EgressPolicy::default();
        for url in [
            "http://127.0.0.1:8080/hook",
            "http://169.254.169.254/latest/meta-data/",
            "http://10.0.0.1/hook",
            "http://100.64.0.1/",
            "http://[::1]:8080/",
            "http://[fc00::1]/",
        ] {
            let err = policy.check_url(url).await.unwrap_err();
            assert!(
                matches!(err, EgressError::Blocked(_)),
                "{url} should be blocked, got {err:?}"
            );
        }
    }

    #[tokio::test]
    async fn rejects_bad_schemes_credentials_and_missing_hosts() {
        let policy = EgressPolicy::default();
        for url in [
            "file:///etc/passwd",
            "ftp://example.com/x",
            "http://user:pass@example.com/x",
            "http:///no-host",
        ] {
            assert!(policy.check_url(url).await.is_err(), "{url} should fail");
        }
    }

    #[tokio::test]
    async fn allow_private_and_host_allowlist_are_explicit_escape_hatches() {
        let permissive = EgressPolicy::permissive();
        permissive
            .check_url("http://127.0.0.1:9/hook")
            .await
            .unwrap();

        let allowlisted = EgressPolicy::new(false, vec!["127.0.0.1".into()]);
        allowlisted
            .check_url("http://127.0.0.1:9/hook")
            .await
            .unwrap();

        let suffix = EgressPolicy::new(false, vec!["*.internal.example".into()]);
        assert!(suffix.host_allowed("api.internal.example"));
        assert!(suffix.host_allowed("internal.example"));
        assert!(!suffix.host_allowed("evilinternal.example"));
        assert!(!suffix.host_allowed("internal.example.evil"));
    }

    #[tokio::test]
    async fn public_hostnames_resolve_and_pass() {
        let policy = EgressPolicy::default();
        // `localhost` resolves to loopback on every supported platform.
        assert!(policy.check_url("http://localhost:9/").await.is_err());
    }

    #[test]
    fn config_defaults_are_fail_closed() {
        let cfg = EgressConfig::default();
        assert!(!cfg.allow_private);
        assert!(cfg.allow_hosts.is_empty());
        let policy = EgressPolicy::from_config(&cfg);
        assert!(!policy.allow_private());
        let ip: IpAddr = Ipv4Addr::LOCALHOST.into();
        assert!(is_blocked_ip(ip));
    }
}
