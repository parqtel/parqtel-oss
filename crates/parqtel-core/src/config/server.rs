use serde::{Deserialize, Serialize};
use std::sync::LazyLock;

/// Which subsystems this process runs.
///
/// The default, [`NodeRole::All`], reproduces the single-node behaviour: every
/// subsystem runs in one process. The other roles split ingest, query and
/// compaction across independently scaled deployments. Capability decisions
/// live here and nowhere else, so `server.rs`, `router.rs` and `tasks.rs` never
/// scatter `match role` branches.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default, Serialize, Deserialize)]
#[serde(rename_all = "lowercase")]
pub enum NodeRole {
    /// Every subsystem in one process (the default, single-node deployment).
    #[default]
    All,
    /// Accepts OTLP/HTTP/gRPC/Kafka writes and publishes blocks to the catalog.
    Writer,
    /// Serves the query API from the shared catalog; never ingests or publishes.
    Reader,
    /// Merges and retires shared blocks; serves no public ingest or query API.
    Compactor,
}

impl NodeRole {
    /// Accepts telemetry writes (OTLP over gRPC and HTTP, Kafka, remote write).
    pub fn runs_ingest(self) -> bool {
        matches!(self, Self::All | Self::Writer)
    }

    /// Serves the public query API and the embedded UI.
    pub fn runs_query(self) -> bool {
        matches!(self, Self::All | Self::Reader)
    }

    /// Runs L0→L1 (and higher) block merges.
    pub fn runs_compaction(self) -> bool {
        matches!(self, Self::All | Self::Compactor)
    }

    /// Evaluates alert rules and dispatches notifications.
    pub fn runs_alerts(self) -> bool {
        matches!(self, Self::All)
    }

    /// Alert evaluation, including the dedicated evaluator a sharded reader
    /// deployment may select. Writers and compactors never evaluate: alerts
    /// follow the query tier so each rule fires once.
    pub fn runs_alert_evaluator(self, alert_evaluator: bool) -> bool {
        self.runs_alerts() || (self == Self::Reader && alert_evaluator)
    }

    /// Deletes expired blocks from the local index and storage.
    pub fn runs_retention(self) -> bool {
        matches!(self, Self::All | Self::Compactor)
    }

    /// Writes this node's blocks into the shared object catalog. Readers only
    /// read the catalog and adopt remote blocks.
    pub fn runs_catalog_publish(self) -> bool {
        matches!(self, Self::All | Self::Writer | Self::Compactor)
    }

    /// Short lowercase name used in logs and CLI parsing errors.
    pub fn as_str(self) -> &'static str {
        match self {
            Self::All => "all",
            Self::Writer => "writer",
            Self::Reader => "reader",
            Self::Compactor => "compactor",
        }
    }
}

impl std::fmt::Display for NodeRole {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str(self.as_str())
    }
}

impl std::str::FromStr for NodeRole {
    type Err = String;

    fn from_str(value: &str) -> std::result::Result<Self, Self::Err> {
        match value.trim().to_ascii_lowercase().as_str() {
            "all" => Ok(Self::All),
            "writer" => Ok(Self::Writer),
            "reader" => Ok(Self::Reader),
            "compactor" => Ok(Self::Compactor),
            other => Err(format!(
                "invalid node role {other:?}; expected one of all, writer, reader, compactor"
            )),
        }
    }
}

/// Process-local node identity used as the catalog `writer` lease.
///
/// The config value wins; otherwise `HOSTNAME` (the Kubernetes pod name) is
/// used. When neither exists, a stable process-local id is generated once and
/// reused for the life of the process. Never panics.
fn fallback_node_id() -> &'static str {
    static FALLBACK: LazyLock<String> = LazyLock::new(|| {
        let nanos = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .map(|elapsed| elapsed.as_nanos())
            .unwrap_or(0);
        format!("parqtel-{}-{}", std::process::id(), nanos)
    });
    &FALLBACK
}

/// Configuration for the HTTP server.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ServerConfig {
    /// Subsystems this process runs. Defaults to `all` so an existing
    /// single-node deployment is unchanged.
    #[serde(default)]
    pub role: NodeRole,
    /// Cross-node identity written into the shared block catalog. Defaults to
    /// `HOSTNAME`, then a process-local fallback.
    #[serde(default)]
    pub node_id: Option<String>,
    /// Marks one reader pod as the alert evaluator in a sharded deployment.
    /// Ignored by writers and compactors, and unnecessary for `role = "all"`.
    #[serde(default)]
    pub alert_evaluator: bool,
    /// Logical dataset this process serves. Non-default values prefix the
    /// metrics/logs data directories and are enforced by token scopes.
    #[serde(default = "default_dataset")]
    pub dataset: String,
    /// Operator-visible identity for this server replica.
    #[serde(default = "default_replica_id")]
    pub replica_id: String,
    /// TCP address to bind to (e.g. "0.0.0.0:8080").
    pub bind_address: String,
    /// TCP address for the OTLP gRPC server (e.g. "0.0.0.0:4317").
    /// Set to an empty string to disable gRPC ingestion.
    #[serde(default = "default_grpc_bind_address")]
    pub grpc_bind_address: String,
    /// Maximum simultaneous TCP connections.
    pub max_connections: usize,
    /// Seconds to wait for in-flight requests during shutdown.
    pub shutdown_timeout_secs: u64,
    /// Seconds between background flush checks. Each tick asks each rotator
    /// whether its block duration has elapsed, so this bounds how late a
    /// duration-triggered flush can be.
    #[serde(default = "default_flush_interval_secs")]
    pub flush_interval_secs: u64,
    /// Seconds between alert-rule evaluation cycles.
    #[serde(default = "default_alert_interval_secs")]
    pub alert_interval_secs: u64,
    /// Seconds between retention sweeps (time-based expiry only).
    #[serde(default = "default_retention_interval_secs")]
    pub retention_interval_secs: u64,
    /// Maximum concurrent in-flight OTLP export requests per gRPC
    /// connection. Without it a single client can monopolise the shared
    /// ingest mutex that HTTP exporters also queue on.
    #[serde(default = "default_grpc_concurrency_limit")]
    pub grpc_concurrency_limit: usize,
    /// Seconds between block-index sidecar writes. Mutations mark the index
    /// dirty; this bounds how long a block can sit in memory before the
    /// sidecar catches up, and amortises many flushes into one write.
    /// Lower it if you restart often and want the index written sooner; raise
    /// it to reduce filesystem writes on a write-heavy ingest.
    #[serde(default = "default_index_persist_interval_secs")]
    pub index_persist_interval_secs: u64,
    /// Serve HTTPS and OTLP gRPC over TLS. Both listeners use the same pair.
    #[serde(default)]
    pub tls: Option<TlsConfig>,
}

impl ServerConfig {
    /// Stable identity used as this node's catalog writer lease.
    ///
    /// Precedence: configured `node_id`, then `HOSTNAME`, then a process-local
    /// fallback generated once. Always non-empty; never panics.
    pub fn resolved_node_id(&self) -> String {
        if let Some(node_id) = self
            .node_id
            .as_deref()
            .map(str::trim)
            .filter(|id| !id.is_empty())
        {
            return node_id.to_string();
        }
        match std::env::var("HOSTNAME") {
            Ok(hostname) if !hostname.trim().is_empty() => hostname,
            _ => fallback_node_id().to_string(),
        }
    }
}

/// PEM certificate and private key paths for in-process TLS.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct TlsConfig {
    pub cert_path: String,
    pub key_path: String,
}

fn default_grpc_bind_address() -> String {
    "0.0.0.0:4317".into()
}

fn default_flush_interval_secs() -> u64 {
    5
}

fn default_alert_interval_secs() -> u64 {
    15
}

fn default_retention_interval_secs() -> u64 {
    3600
}

fn default_grpc_concurrency_limit() -> usize {
    64
}

fn default_index_persist_interval_secs() -> u64 {
    2
}

fn default_dataset() -> String {
    "default".into()
}

fn default_replica_id() -> String {
    std::env::var("HOSTNAME").unwrap_or_default()
}

impl Default for ServerConfig {
    fn default() -> Self {
        Self {
            role: NodeRole::All,
            node_id: None,
            alert_evaluator: false,
            dataset: default_dataset(),
            replica_id: default_replica_id(),
            bind_address: "0.0.0.0:8080".into(),
            grpc_bind_address: default_grpc_bind_address(),
            max_connections: 1024,
            shutdown_timeout_secs: 30,
            flush_interval_secs: default_flush_interval_secs(),
            alert_interval_secs: default_alert_interval_secs(),
            retention_interval_secs: default_retention_interval_secs(),
            grpc_concurrency_limit: default_grpc_concurrency_limit(),
            index_persist_interval_secs: default_index_persist_interval_secs(),
            tls: None,
        }
    }
}

#[cfg(test)]
mod tests {
    #![allow(clippy::unwrap_used, clippy::expect_used, clippy::panic)]
    use super::*;

    #[test]
    fn defaults_preserve_previous_hardcoded_intervals() {
        // These were literals in main.rs and retention.rs; the defaults must
        // reproduce them exactly so upgrading is a no-op.
        let c = ServerConfig::default();
        assert_eq!(c.flush_interval_secs, 5);
        assert_eq!(c.alert_interval_secs, 15);
        assert_eq!(c.retention_interval_secs, 3600);
        assert_eq!(c.grpc_concurrency_limit, 64);
        assert_eq!(c.index_persist_interval_secs, 2);
    }

    #[test]
    fn serde_round_trips_new_knobs() {
        let c = ServerConfig {
            replica_id: "parqtel-0".into(),
            flush_interval_secs: 30,
            alert_interval_secs: 60,
            retention_interval_secs: 900,
            grpc_concurrency_limit: 8,
            index_persist_interval_secs: 30,
            ..Default::default()
        };
        let json = serde_json::to_string(&c).unwrap();
        let back: ServerConfig = serde_json::from_str(&json).unwrap();
        assert_eq!(back.replica_id, "parqtel-0");
        assert_eq!(back.flush_interval_secs, 30);
        assert_eq!(back.alert_interval_secs, 60);
        assert_eq!(back.retention_interval_secs, 900);
        assert_eq!(back.grpc_concurrency_limit, 8);
        assert_eq!(back.index_persist_interval_secs, 30);
    }

    #[test]
    fn missing_new_knobs_fall_back_to_defaults() {
        // A config file written by an older build must still load.
        let json =
            r#"{"bind_address":"0.0.0.0:8080","max_connections":1024,"shutdown_timeout_secs":30}"#;
        let c: ServerConfig = serde_json::from_str(json).unwrap();
        assert_eq!(c.flush_interval_secs, 5);
        assert_eq!(c.alert_interval_secs, 15);
        assert_eq!(c.retention_interval_secs, 3600);
        assert_eq!(c.grpc_concurrency_limit, 64);
        assert_eq!(c.index_persist_interval_secs, 2);
        assert_eq!(c.replica_id, default_replica_id());
    }

    #[test]
    fn role_defaults_to_all_and_parses_lowercase() {
        let c = ServerConfig::default();
        assert_eq!(c.role, NodeRole::All);
        assert!(c.node_id.is_none());

        let json = r#"{"role":"reader","bind_address":"0.0.0.0:8080","max_connections":1,"shutdown_timeout_secs":1}"#;
        let c: ServerConfig = serde_json::from_str(json).unwrap();
        assert_eq!(c.role, NodeRole::Reader);

        for (text, expected) in [
            ("all", NodeRole::All),
            ("writer", NodeRole::Writer),
            ("reader", NodeRole::Reader),
            ("compactor", NodeRole::Compactor),
            (" Reader ", NodeRole::Reader),
        ] {
            assert_eq!(text.parse::<NodeRole>().unwrap(), expected);
        }
        assert!("ingest".parse::<NodeRole>().is_err());
    }

    #[test]
    fn alert_evaluator_follows_the_reader_tier_only() {
        assert!(NodeRole::All.runs_alert_evaluator(false));
        assert!(NodeRole::Reader.runs_alert_evaluator(true));
        assert!(!NodeRole::Reader.runs_alert_evaluator(false));
        assert!(!NodeRole::Writer.runs_alert_evaluator(true));
        assert!(!NodeRole::Compactor.runs_alert_evaluator(true));
        assert!(!ServerConfig::default().alert_evaluator);
    }

    #[test]
    fn capability_matrix_matches_the_sharding_contract() {
        let cases = [
            // role, ingest, query, compaction, alerts, retention, publish
            (NodeRole::All, true, true, true, true, true, true),
            (NodeRole::Writer, true, false, false, false, false, true),
            (NodeRole::Reader, false, true, false, false, false, false),
            (NodeRole::Compactor, false, false, true, false, true, true),
        ];
        for (role, ingest, query, compaction, alerts, retention, publish) in cases {
            assert_eq!(role.runs_ingest(), ingest, "{role:?} runs_ingest");
            assert_eq!(role.runs_query(), query, "{role:?} runs_query");
            assert_eq!(
                role.runs_compaction(),
                compaction,
                "{role:?} runs_compaction"
            );
            assert_eq!(role.runs_alerts(), alerts, "{role:?} runs_alerts");
            assert_eq!(role.runs_retention(), retention, "{role:?} runs_retention");
            assert_eq!(
                role.runs_catalog_publish(),
                publish,
                "{role:?} runs_catalog_publish"
            );
        }
    }

    #[test]
    fn resolved_node_id_prefers_config_then_hostname() {
        let mut c = ServerConfig {
            node_id: Some("writer-2".into()),
            ..ServerConfig::default()
        };
        assert_eq!(c.resolved_node_id(), "writer-2");

        c.node_id = Some("   ".into());
        let resolved = c.resolved_node_id();
        match std::env::var("HOSTNAME") {
            Ok(hostname) if !hostname.trim().is_empty() => assert_eq!(resolved, hostname),
            _ => {
                assert!(!resolved.is_empty());
                assert_eq!(resolved, c.resolved_node_id());
            }
        }

        c.node_id = None;
        assert!(!c.resolved_node_id().is_empty());
        assert_eq!(c.resolved_node_id(), c.resolved_node_id());
    }
}
