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
    /// Optional OpenID Connect login and bearer-token validation.
    #[serde(default)]
    pub oidc: Option<OidcConfig>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct OidcConfig {
    /// Issuer URL; discovery is read from `/.well-known/openid-configuration`.
    pub issuer: String,
    pub client_id: String,
    /// Required for the authorization-code flow; bearer validation alone does
    /// not need it.
    #[serde(default)]
    pub client_secret: Option<String>,
    /// Must match the IdP registration, e.g. `https://parqtel.example/auth/callback`.
    pub redirect_url: String,
    #[serde(default = "default_oidc_scopes")]
    pub scopes: Vec<String>,
    /// Claim carrying roles. Defaults to `roles`.
    #[serde(default = "default_roles_claim")]
    pub roles_claim: String,
    /// Role mapped to admin scope; everything else authenticated is read.
    #[serde(default = "default_admin_role")]
    pub admin_role: String,
    /// Optional role mapped to write scope.
    #[serde(default)]
    pub write_role: Option<String>,
}

fn default_oidc_scopes() -> Vec<String> {
    vec!["openid".into(), "profile".into(), "email".into()]
}

fn default_roles_claim() -> String {
    "roles".into()
}

fn default_admin_role() -> String {
    "parqtel-admin".into()
}

impl Default for AuthConfig {
    fn default() -> Self {
        Self {
            enabled: false,
            tokens: Vec::new(),
            header: default_header(),
            oidc: None,
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
        if self.tokens.is_empty() && self.oidc.is_none() {
            return Err("auth.enabled is true but no tokens or OIDC are configured".into());
        }
        if let Some(oidc) = &self.oidc {
            if oidc.issuer.trim().is_empty()
                || oidc.client_id.trim().is_empty()
                || oidc.redirect_url.trim().is_empty()
            {
                return Err("auth.oidc issuer, client_id and redirect_url are required".into());
            }
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
