//! Scoped bearer-token authentication for the HTTP API.
//!
//! Disabled by default. When enabled, health/readiness and the static console
//! shell stay public; every data or mutation route needs a matching token.
//! Scopes are coarse (`read` < `write` < `admin`) until a dataset/tenant
//! concept exists; the token name is returned as the actor for audit logs.

use crate::state::AppState;
use axum::extract::Query;
use axum::response::Redirect;
use axum::{
    extract::State,
    http::{header, HeaderMap, HeaderName, HeaderValue, Request, StatusCode},
    middleware::Next,
    response::{IntoResponse, Response},
    Json,
};
use jsonwebtoken::{decode, decode_header, DecodingKey, Validation};
use parqtel_core::config::{resolve_secret, AuthConfig, AuthScope, OidcConfig};
use serde::Deserialize;
use sha2::{Digest, Sha256};
use std::sync::Arc;
use tokio::sync::Mutex;

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
    oidc: Option<Arc<OidcRuntime>>,
}

#[derive(Debug)]
struct OidcRuntime {
    config: OidcConfig,
    client: reqwest::Client,
    discovery: Mutex<Option<OidcDiscovery>>,
    jwks: Mutex<Option<jsonwebtoken::jwk::JwkSet>>,
}

#[derive(Debug, Clone)]
struct OidcDiscovery {
    authorization_endpoint: String,
    token_endpoint: String,
    jwks_uri: String,
}

#[derive(Debug, Deserialize)]
struct DiscoveryDocument {
    authorization_endpoint: String,
    token_endpoint: String,
    jwks_uri: String,
}

#[derive(Debug, Deserialize)]
struct TokenResponse {
    id_token: String,
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
            oidc: config.oidc.clone().map(|oidc| {
                Arc::new(OidcRuntime {
                    config: oidc,
                    client: reqwest::Client::new(),
                    discovery: Mutex::new(None),
                    jwks: Mutex::new(None),
                })
            }),
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
            oidc: None,
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
        matches!(
            path,
            "/health" | "/ready" | "/ui" | "/oas" | "/" | "/auth/login" | "/auth/callback"
        )
    }

    pub async fn authorize(
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
        let required = Self::required_scope(method, path);
        let bearer = headers
            .get(&self.header)
            .and_then(|value| value.to_str().ok())
            .map(|raw| {
                raw.strip_prefix("Bearer ")
                    .unwrap_or(raw)
                    .trim()
                    .to_string()
            })
            .filter(|raw| !raw.is_empty());
        let cookie = headers
            .get(header::COOKIE)
            .and_then(|value| value.to_str().ok())
            .and_then(|cookies| {
                cookies.split(';').find_map(|cookie| {
                    cookie
                        .trim()
                        .strip_prefix("parqtel_session=")
                        .map(str::to_string)
                })
            });
        let raw = bearer.or(cookie).ok_or(AuthError::Missing)?;
        let digest: [u8; 32] = Sha256::digest(raw.as_bytes()).into();
        let token = self.tokens.iter().find(|token| token.hash == digest);
        let mut oidc_actor = None;
        let scopes = if let Some(token) = token {
            token.scopes.clone()
        } else if let Some(oidc) = &self.oidc {
            let (subject, scopes) = oidc.verify(&raw).await?;
            oidc_actor = Some(subject);
            scopes
        } else {
            return Err(AuthError::Invalid);
        };
        let granted = |scope: AuthScope| scopes.contains(&scope);
        let allowed = match required {
            AuthScope::Read => {
                granted(AuthScope::Read) || granted(AuthScope::Write) || granted(AuthScope::Admin)
            }
            AuthScope::Write => granted(AuthScope::Write) || granted(AuthScope::Admin),
            AuthScope::Admin => granted(AuthScope::Admin),
        };
        if allowed {
            let actor = token
                .map(|token| token.name.clone())
                .or(oidc_actor)
                .unwrap_or_else(|| "oidc".into());
            Ok(Some(actor))
        } else {
            Err(AuthError::Forbidden)
        }
    }
}

impl OidcRuntime {
    async fn discovery(&self) -> Result<OidcDiscovery, AuthError> {
        if let Some(discovery) = self.discovery.lock().await.clone() {
            return Ok(discovery);
        }
        let issuer = self.config.issuer.trim_end_matches('/');
        let document: DiscoveryDocument = self
            .client
            .get(format!("{issuer}/.well-known/openid-configuration"))
            .send()
            .await
            .map_err(|_| AuthError::Invalid)?
            .error_for_status()
            .map_err(|_| AuthError::Invalid)?
            .json()
            .await
            .map_err(|_| AuthError::Invalid)?;
        let discovery = OidcDiscovery {
            authorization_endpoint: document.authorization_endpoint,
            token_endpoint: document.token_endpoint,
            jwks_uri: document.jwks_uri,
        };
        *self.discovery.lock().await = Some(discovery.clone());
        Ok(discovery)
    }

    async fn keys(&self) -> Result<jsonwebtoken::jwk::JwkSet, AuthError> {
        if let Some(keys) = self.jwks.lock().await.clone() {
            return Ok(keys);
        }
        let discovery = self.discovery().await?;
        let keys: jsonwebtoken::jwk::JwkSet = self
            .client
            .get(&discovery.jwks_uri)
            .send()
            .await
            .map_err(|_| AuthError::Invalid)?
            .error_for_status()
            .map_err(|_| AuthError::Invalid)?
            .json()
            .await
            .map_err(|_| AuthError::Invalid)?;
        *self.jwks.lock().await = Some(keys.clone());
        Ok(keys)
    }

    async fn verify(&self, token: &str) -> Result<(String, Vec<AuthScope>), AuthError> {
        let header = decode_header(token).map_err(|_| AuthError::Invalid)?;
        let kid = header.kid.ok_or(AuthError::Invalid)?;
        let mut keys = self.keys().await?;
        let jwk = match keys.find(&kid) {
            Some(jwk) => jwk,
            None => {
                // Key rotation: refresh once before rejecting.
                *self.jwks.lock().await = None;
                keys = self.keys().await?;
                keys.find(&kid).ok_or(AuthError::Invalid)?
            }
        };
        let key = DecodingKey::from_jwk(jwk).map_err(|_| AuthError::Invalid)?;
        let mut validation = Validation::new(header.alg);
        validation.set_audience(&[self.config.client_id.as_str()]);
        validation.set_issuer(&[self.config.issuer.trim_end_matches('/')]);
        validation.set_required_spec_claims(&["exp", "iss", "aud"]);
        let claims = decode::<serde_json::Value>(token, &key, &validation)
            .map_err(|_| AuthError::Invalid)?
            .claims;
        let subject = claims
            .get("sub")
            .and_then(|value| value.as_str())
            .ok_or(AuthError::Invalid)?
            .to_string();
        let roles = claims
            .get(&self.config.roles_claim)
            .map(|value| match value {
                serde_json::Value::Array(items) => items
                    .iter()
                    .filter_map(|item| item.as_str().map(str::to_string))
                    .collect(),
                serde_json::Value::String(role) => vec![role.clone()],
                _ => Vec::new(),
            })
            .unwrap_or_default();
        let mut scopes = vec![AuthScope::Read];
        if roles.iter().any(|role| role == &self.config.admin_role) {
            scopes.push(AuthScope::Admin);
        }
        if self
            .config
            .write_role
            .as_ref()
            .is_some_and(|candidate| roles.iter().any(|role| role == candidate))
        {
            scopes.push(AuthScope::Write);
        }
        Ok((subject, scopes))
    }

    async fn exchange(&self, code: &str) -> Result<String, AuthError> {
        let discovery = self.discovery().await?;
        let mut form = vec![
            ("grant_type", "authorization_code".to_string()),
            ("code", code.to_string()),
            ("redirect_uri", self.config.redirect_url.clone()),
            ("client_id", self.config.client_id.clone()),
        ];
        if let Some(secret) = &self.config.client_secret {
            form.push(("client_secret", secret.clone()));
        }
        let response: TokenResponse = self
            .client
            .post(&discovery.token_endpoint)
            .form(&form)
            .send()
            .await
            .map_err(|_| AuthError::Invalid)?
            .error_for_status()
            .map_err(|_| AuthError::Invalid)?
            .json()
            .await
            .map_err(|_| AuthError::Invalid)?;
        Ok(response.id_token)
    }

    fn authorize_url(&self, discovery: &OidcDiscovery, state: &str) -> String {
        format!(
            "{}?response_type=code&client_id={}&redirect_uri={}&scope={}&state={}",
            discovery.authorization_endpoint,
            urlencoding::encode(&self.config.client_id),
            urlencoding::encode(&self.config.redirect_url),
            urlencoding::encode(&self.config.scopes.join(" ")),
            urlencoding::encode(state),
        )
    }
}

#[derive(Deserialize)]
pub struct LoginParams {
    pub redirect: Option<String>,
}

#[derive(Deserialize)]
pub struct CallbackParams {
    pub code: String,
    pub state: String,
}

fn cookie_value(headers: &HeaderMap, name: &str) -> Option<String> {
    headers
        .get(header::COOKIE)
        .and_then(|value| value.to_str().ok())
        .and_then(|cookies| {
            cookies.split(';').find_map(|cookie| {
                cookie
                    .trim()
                    .strip_prefix(&format!("{name}="))
                    .map(str::to_string)
            })
        })
}

fn session_cookie(token: &str, secure: bool) -> String {
    format!(
        "parqtel_session={token}; HttpOnly; SameSite=Lax; Path=/; Max-Age=3600{}",
        if secure { "; Secure" } else { "" }
    )
}

/// Axum middleware wrapper.
pub async fn require_auth(
    State(auth): State<Arc<AuthState>>,
    mut req: Request<axum::body::Body>,
    next: Next,
) -> Response {
    match auth
        .authorize(req.headers(), req.method(), req.uri().path())
        .await
    {
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

/// `GET /auth/login`: start the authorization-code flow.
pub async fn login(State(state): State<AppState>, Query(params): Query<LoginParams>) -> Response {
    let Some(auth) = &state.inner.auth.oidc else {
        return (StatusCode::NOT_FOUND, "OIDC is not configured").into_response();
    };
    let discovery = match auth.discovery().await {
        Ok(discovery) => discovery,
        Err(_) => {
            return (StatusCode::BAD_GATEWAY, "OIDC discovery failed").into_response();
        }
    };
    let nonce = ulid::Ulid::new().to_string();
    let url = auth.authorize_url(&discovery, &nonce);
    let mut response = Redirect::temporary(&url).into_response();
    let secure = auth.config.redirect_url.starts_with("https://");
    let state_cookie = format!(
        "parqtel_oidc_state={nonce}; HttpOnly; SameSite=Lax; Path=/; Max-Age=600{}",
        if secure { "; Secure" } else { "" }
    );
    let redirect = params
        .redirect
        .as_deref()
        .filter(|target| target.starts_with('/'));
    let redirect_cookie = format!(
        "parqtel_oidc_redirect={}; HttpOnly; SameSite=Lax; Path=/; Max-Age=600{}",
        urlencoding::encode(redirect.unwrap_or("/ui")),
        if secure { "; Secure" } else { "" }
    );
    for cookie in [state_cookie, redirect_cookie] {
        if let Ok(value) = HeaderValue::from_str(&cookie) {
            response.headers_mut().append(header::SET_COOKIE, value);
        }
    }
    response
}

/// `GET /auth/callback`: exchange the code and set an HttpOnly session cookie.
pub async fn callback(
    State(state): State<AppState>,
    Query(params): Query<CallbackParams>,
    headers: HeaderMap,
) -> Response {
    let Some(auth) = &state.inner.auth.oidc else {
        return (StatusCode::NOT_FOUND, "OIDC is not configured").into_response();
    };
    let expected = cookie_value(&headers, "parqtel_oidc_state");
    if expected.as_deref() != Some(params.state.as_str()) {
        return (StatusCode::BAD_REQUEST, "invalid OIDC state").into_response();
    }
    let token = match auth.exchange(&params.code).await {
        Ok(token) => token,
        Err(_) => return (StatusCode::BAD_GATEWAY, "OIDC token exchange failed").into_response(),
    };
    let target = cookie_value(&headers, "parqtel_oidc_redirect")
        .and_then(|value| urlencoding::decode(&value).ok().map(|v| v.into_owned()))
        .filter(|value| value.starts_with('/'))
        .unwrap_or_else(|| "/ui".into());
    let mut response = Redirect::temporary(&target).into_response();
    let secure = auth.config.redirect_url.starts_with("https://");
    for cookie in [
        session_cookie(&token, secure),
        "parqtel_oidc_state=; HttpOnly; SameSite=Lax; Path=/; Max-Age=0".to_string(),
        "parqtel_oidc_redirect=; HttpOnly; SameSite=Lax; Path=/; Max-Age=0".to_string(),
    ] {
        if let Ok(value) = HeaderValue::from_str(&cookie) {
            response.headers_mut().append(header::SET_COOKIE, value);
        }
    }
    response
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
            oidc: None,
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

    #[tokio::test]
    async fn missing_and_invalid_tokens_are_rejected() {
        let auth = state();
        assert_eq!(
            auth.authorize(&HeaderMap::new(), &axum::http::Method::GET, "/api/v1/query")
                .await,
            Err(AuthError::Missing)
        );
        assert_eq!(
            auth.authorize(&headers("nope"), &axum::http::Method::GET, "/api/v1/query")
                .await,
            Err(AuthError::Invalid)
        );
    }

    #[tokio::test]
    async fn scopes_are_enforced() {
        let auth = state();
        assert_eq!(
            auth.authorize(
                &headers("read-token"),
                &axum::http::Method::GET,
                "/api/v1/query"
            )
            .await
            .unwrap(),
            Some("reader".into())
        );
        assert_eq!(
            auth.authorize(
                &headers("read-token"),
                &axum::http::Method::POST,
                "/v1/logs"
            )
            .await,
            Err(AuthError::Forbidden)
        );
        assert_eq!(
            auth.authorize(
                &headers("write-token"),
                &axum::http::Method::POST,
                "/v1/logs"
            )
            .await
            .unwrap(),
            Some("writer".into())
        );
        assert_eq!(
            auth.authorize(
                &headers("write-token"),
                &axum::http::Method::DELETE,
                "/api/v1/rules/x"
            )
            .await,
            Err(AuthError::Forbidden)
        );
        assert_eq!(
            auth.authorize(
                &headers("admin-token"),
                &axum::http::Method::DELETE,
                "/api/v1/rules/x"
            )
            .await
            .unwrap(),
            Some("admin".into())
        );
    }

    #[tokio::test]
    async fn oidc_roles_map_to_scopes() {
        use base64::Engine;
        use jsonwebtoken::{encode, Algorithm, EncodingKey, Header};
        let secret = b"oidc-test-secret";
        let runtime = OidcRuntime {
            config: OidcConfig {
                issuer: "https://issuer.example".into(),
                client_id: "parqtel".into(),
                client_secret: None,
                redirect_url: "https://parqtel.example/auth/callback".into(),
                scopes: vec!["openid".into()],
                roles_claim: "roles".into(),
                admin_role: "parqtel-admin".into(),
                write_role: Some("parqtel-write".into()),
            },
            client: reqwest::Client::new(),
            discovery: Mutex::new(None),
            jwks: Mutex::new(Some(jsonwebtoken::jwk::JwkSet {
                keys: vec![serde_json::from_value(serde_json::json!({
                    "kty": "oct",
                    "kid": "test-key",
                    "k": base64::engine::general_purpose::URL_SAFE_NO_PAD.encode(secret),
                }))
                .unwrap()],
            })),
        };
        let now = chrono::Utc::now().timestamp() as u64;
        let token = encode(
            &Header {
                alg: Algorithm::HS256,
                kid: Some("test-key".into()),
                ..Default::default()
            },
            &serde_json::json!({
                "sub": "user@example",
                "iss": "https://issuer.example",
                "aud": "parqtel",
                "exp": now + 600,
                "roles": ["parqtel-admin"],
            }),
            &EncodingKey::from_secret(secret),
        )
        .unwrap();
        let (subject, scopes) = runtime.verify(&token).await.unwrap();
        assert_eq!(subject, "user@example");
        assert!(scopes.contains(&AuthScope::Admin));
        assert!(scopes.contains(&AuthScope::Read));
    }

    #[tokio::test]
    async fn health_and_ui_stay_public() {
        let auth = state();
        for path in ["/health", "/ready", "/ui", "/oas", "/"] {
            assert_eq!(
                auth.authorize(&HeaderMap::new(), &axum::http::Method::GET, path)
                    .await,
                Ok(None)
            );
        }
    }
}
