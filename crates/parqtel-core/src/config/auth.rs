//! Scoped bearer-token authentication.
//!
//! Disabled by default: the single-user/edge deployment keeps working without
//! a credential store. When enabled, every route except health/readiness needs
//! a token whose actions cover the request.

use serde::{Deserialize, Serialize};

/// Authentication configuration (`[auth]`).
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct AuthConfig {
    /// Require a bearer token on non-public routes.
    #[serde(default)]
    pub enabled: bool,
    /// Hashed bearer tokens. Configure either `token` (hashed at startup) or
    /// `token_sha256` (lowercase hex).
    #[serde(default)]
    pub tokens: Vec<AuthTokenConfig>,
    /// Header carrying the token. Defaults to `Authorization: Bearer <token>`.
    #[serde(default = "default_header")]
    pub header: String,
}

impl Default for AuthConfig {
    fn default() -> Self {
        Self {
            enabled: false,
            tokens: Vec::new(),
            header: default_header(),
        }
    }
}

fn default_header() -> String {
    "authorization".to_string()
}

/// One API token and the actions it may perform.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct AuthTokenConfig {
    /// Human-readable name used in logs/audit records.
    pub name: String,
    /// Plaintext token. Prefer `${ENV_VAR}` so the secret stays out of the file;
    /// it is hashed at startup and never stored.
    #[serde(default)]
    pub token: Option<String>,
    /// Lowercase hex SHA-256 of the token, if a plaintext value is not supplied.
    #[serde(default)]
    pub token_sha256: Option<String>,
    /// Permitted actions. Empty defaults to `read`.
    #[serde(default)]
    pub actions: Vec<AuthScope>,
}

/// Coarse capability granted to a token.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum AuthScope {
    /// Query and read metadata.
    Read,
    /// Read plus ingest (OTLP/HTTP, generic JSON, remote write).
    Write,
    /// Every route, including rule/route/pipeline mutations.
    Admin,
}

impl AuthConfig {
    /// Validate the credential configuration before the server starts.
    pub fn validate(&self) -> Result<(), String> {
        if !self.enabled {
            return Ok(());
        }
        if self.tokens.is_empty() {
            return Err("auth.enabled is true but auth.tokens is empty".into());
        }
        let mut names = std::collections::HashSet::new();
        for token in &self.tokens {
            if token.name.trim().is_empty() {
                return Err("auth token name cannot be empty".into());
            }
            if !names.insert(token.name.clone()) {
                return Err(format!("duplicate auth token name '{}'", token.name));
            }
            if token.token.is_none() && token.token_sha256.is_none() {
                return Err(format!(
                    "auth token '{}' needs token or token_sha256",
                    token.name
                ));
            }
        }
        Ok(())
    }
}
