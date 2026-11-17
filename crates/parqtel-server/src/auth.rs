//! Scoped bearer-token authentication for the HTTP API.
//!
//! Disabled by default. When enabled, health/readiness and the static console
//! shell stay public; every data or mutation route needs a matching token.
//! Scopes are coarse (`read` < `write` < `admin`) until a dataset/tenant
//! concept exists; the token name is returned as the actor for audit logs.

use axum::{
    extract::State,
    http::{header, HeaderMap, HeaderName, HeaderValue, Request, StatusCode},
    middleware::Next,
    response::{IntoResponse, Response},
    Json,
};
use parqtel_core::config::{resolve_secret, AuthConfig, AuthScope};
use sha2::{Digest, Sha256};
use std::sync::Arc;

#[derive(Debug, Clone)]
struct HashedToken {
    name: String,
    hash: [u8; 32],
    scopes: Vec<AuthScope>,
}

/// Immutable credential set used by the middleware.
#[derive(Debug, Clone)]
pub struct AuthState {
    enabled: bool,
    header: HeaderName,
    tokens: Vec<HashedToken>,
    /// Credentials could not be parsed; fail closed rather than serve open.
    broken: bool,
}

/// Authenticated identity inserted into request extensions for audit records.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Actor(pub String);

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum AuthError {
    Missing,
    Invalid,
    Forbidden,
}

impl AuthState {
    pub fn from_config(config: &AuthConfig) -> Result<Self, String> {
        let header = HeaderName::from_bytes(config.header.as_bytes())
            .map_err(|e| format!("invalid auth header '{}': {e}", config.header))?;
        let mut tokens = Vec::with_capacity(config.tokens.len());
        for token in &config.tokens {
            let secret = match (&token.token, &token.token_sha256) {
                (Some(secret), _) => resolve_secret(secret).map_err(|e| e.to_string())?,
                (None, Some(hash)) => {
                    let bytes = hex::decode(hash.trim())
                        .map_err(|e| format!("token '{}' has invalid sha256: {e}", token.name))?;
                    let hash: [u8; 32] = bytes
                        .try_into()
                        .map_err(|_| format!("token '{}' sha256 must be 32 bytes", token.name))?;
                    tokens.push(HashedToken {
                        name: token.name.clone(),
                        hash,
                        scopes: if token.actions.is_empty() {
                            vec![AuthScope::Read]
                        } else {
                            token.actions.clone()
                        },
                    });
                    continue;
                }
                (None, None) => {
                    return Err(format!("token '{}' has no secret", token.name));
                }
            };
            let hash: [u8; 32] = Sha256::digest(secret.as_bytes()).into();
            tokens.push(HashedToken {
                name: token.name.clone(),
                hash,
                scopes: if token.actions.is_empty() {
                    vec![AuthScope::Read]
                } else {
                    token.actions.clone()
                },
            });
        }
        Ok(Self {
            enabled: config.enabled,
            header,
            tokens,
            broken: false,
        })
    }

    /// A state that rejects every request. Used only when the configured
    /// credentials cannot be parsed; serving open would be worse.
    pub fn deny_all() -> Self {
        Self {
            enabled: true,
            header: header::AUTHORIZATION,
            tokens: Vec::new(),
            broken: true,
        }
    }

    fn required_scope(method: &axum::http::Method, path: &str) -> AuthScope {
        if crate::router::is_ingest_path(path) {
            AuthScope::Write
        } else if method == axum::http::Method::GET
            || method == axum::http::Method::HEAD
            || crate::router::is_query_path(path)
        {
            AuthScope::Read
        } else {
            AuthScope::Admin
        }
    }

    fn public(path: &str) -> bool {
        matches!(path, "/health" | "/ready" | "/ui" | "/oas" | "/")
    }

    pub fn authorize(
        &self,
        headers: &HeaderMap,
        method: &axum::http::Method,
        path: &str,
    ) -> Result<Option<String>, AuthError> {
        if !self.enabled || Self::public(path) || method == axum::http::Method::OPTIONS {
            return Ok(None);
        }
        if self.broken {
            return Err(AuthError::Invalid);
        }
        let raw = headers
            .get(&self.header)
            .and_then(|value| value.to_str().ok())
            .ok_or(AuthError::Missing)?;
        let raw = raw.strip_prefix("Bearer ").unwrap_or(raw).trim();
        if raw.is_empty() {
            return Err(AuthError::Missing);
        }
        let digest: [u8; 32] = Sha256::digest(raw.as_bytes()).into();
        let required = Self::required_scope(method, path);
        let token = self
            .tokens
            .iter()
            .find(|token| token.hash == digest)
            .ok_or(AuthError::Invalid)?;
        let granted = |scope: AuthScope| token.scopes.contains(&scope);
        let allowed = match required {
            AuthScope::Read => {
                granted(AuthScope::Read) || granted(AuthScope::Write) || granted(AuthScope::Admin)
            }
            AuthScope::Write => granted(AuthScope::Write) || granted(AuthScope::Admin),
            AuthScope::Admin => granted(AuthScope::Admin),
        };
        if allowed {
            Ok(Some(token.name.clone()))
        } else {
            Err(AuthError::Forbidden)
        }
    }
}

/// Axum middleware wrapper.
pub async fn require_auth(
    State(auth): State<Arc<AuthState>>,
    mut req: Request<axum::body::Body>,
    next: Next,
) -> Response {
    match auth.authorize(req.headers(), req.method(), req.uri().path()) {
        Ok(actor) => {
            req.extensions_mut()
                .insert(Actor(actor.unwrap_or_else(|| "anonymous".into())));
            next.run(req).await
        }
        Err(error) => {
            let (status, message) = match error {
                AuthError::Missing => (StatusCode::UNAUTHORIZED, "missing bearer token"),
                AuthError::Invalid => (StatusCode::UNAUTHORIZED, "invalid bearer token"),
                AuthError::Forbidden => (StatusCode::FORBIDDEN, "token lacks the required scope"),
            };
            let mut response = (
                status,
                Json(serde_json::json!({
                    "status": "error",
                    "error": message,
                })),
            )
                .into_response();
            if status == StatusCode::UNAUTHORIZED {
                response
                    .headers_mut()
                    .insert(header::WWW_AUTHENTICATE, HeaderValue::from_static("Bearer"));
            }
            response
        }
    }
}

#[cfg(test)]
mod tests {
    #![allow(clippy::unwrap_used, clippy::expect_used, clippy::panic)]
    use super::*;
    use parqtel_core::config::{AuthScope, AuthTokenConfig};

    fn state() -> AuthState {
        AuthState::from_config(&AuthConfig {
            enabled: true,
            tokens: vec![
                AuthTokenConfig {
                    name: "reader".into(),
                    token: Some("read-token".into()),
                    token_sha256: None,
                    actions: vec![AuthScope::Read],
                },
                AuthTokenConfig {
                    name: "writer".into(),
                    token: Some("write-token".into()),
                    token_sha256: None,
                    actions: vec![AuthScope::Write],
                },
                AuthTokenConfig {
                    name: "admin".into(),
                    token: Some("admin-token".into()),
                    token_sha256: None,
                    actions: vec![AuthScope::Admin],
                },
            ],
            header: "authorization".into(),
        })
        .unwrap()
    }

    fn headers(token: &str) -> HeaderMap {
        let mut headers = HeaderMap::new();
        headers.insert(
            header::AUTHORIZATION,
            HeaderValue::from_str(&format!("Bearer {token}")).unwrap(),
        );
        headers
    }

    #[test]
    fn missing_and_invalid_tokens_are_rejected() {
        let auth = state();
        assert_eq!(
            auth.authorize(&HeaderMap::new(), &axum::http::Method::GET, "/api/v1/query"),
            Err(AuthError::Missing)
        );
        assert_eq!(
            auth.authorize(&headers("nope"), &axum::http::Method::GET, "/api/v1/query"),
            Err(AuthError::Invalid)
        );
    }

    #[test]
    fn scopes_are_enforced() {
        let auth = state();
        assert_eq!(
            auth.authorize(
                &headers("read-token"),
                &axum::http::Method::GET,
                "/api/v1/query"
            )
            .unwrap(),
            Some("reader".into())
        );
        assert_eq!(
            auth.authorize(
                &headers("read-token"),
                &axum::http::Method::POST,
                "/v1/logs"
            ),
            Err(AuthError::Forbidden)
        );
        assert_eq!(
            auth.authorize(
                &headers("write-token"),
                &axum::http::Method::POST,
                "/v1/logs"
            )
            .unwrap(),
            Some("writer".into())
        );
        assert_eq!(
            auth.authorize(
                &headers("write-token"),
                &axum::http::Method::DELETE,
                "/api/v1/rules/x"
            ),
            Err(AuthError::Forbidden)
        );
        assert_eq!(
            auth.authorize(
                &headers("admin-token"),
                &axum::http::Method::DELETE,
                "/api/v1/rules/x"
            )
            .unwrap(),
            Some("admin".into())
        );
    }

    #[test]
    fn health_and_ui_stay_public() {
        let auth = state();
        for path in ["/health", "/ready", "/ui", "/oas", "/"] {
            assert_eq!(
                auth.authorize(&HeaderMap::new(), &axum::http::Method::GET, path),
                Ok(None)
            );
        }
    }
}
