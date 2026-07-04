//! OIDC-native authentication.
//!
//! Meridian validates bearer tokens issued by configured external identity
//! providers; it never issues tokens of its own (the catalog-hosted
//! `oauth/tokens` endpoint is deprecated in the IRC spec and deliberately
//! not implemented). The middleware here establishes a
//! [`Principal`] for every request and stores it in the request extensions;
//! everything downstream (audit, future authorization) consumes that
//! contract and nothing else.
//!
//! Modes:
//!
//! - `auth.mode = "disabled"` (default): every request runs as
//!   [`Principal::anonymous()`]. A loud warning is logged at startup.
//! - `auth.mode = "oidc"`: a valid `Authorization: Bearer` token from a
//!   configured issuer is required on every route except the health probes
//!   (`/healthz`, `/readyz` — liveness must not depend on IdP
//!   availability). Failures are 401 `NotAuthorizedException` in the IRC
//!   error envelope; an unreachable IdP (JWKS fetch failure) is a 503, not
//!   a token problem.
//!
//! On the first authenticated request of an identity, a `principals` row is
//! provisioned just-in-time (race-safe) so audit history and future grants
//! reference a stable local identity.

mod jwks;

use std::collections::HashSet;
use std::sync::Arc;
use std::time::Duration;

use axum::extract::{Request, State};
use axum::http::{HeaderValue, StatusCode, header};
use axum::middleware::Next;
use axum::response::{IntoResponse, Response};
use jsonwebtoken::jwk::Jwk;
use jsonwebtoken::{Algorithm, DecodingKey, Validation, decode, decode_header};
use meridian_common::config::{AppConfig, AuthMode, OidcConfig};
use meridian_common::principal::{Principal, PrincipalKind};
use meridian_common::{MeridianError, Result};
use meridian_store::{principal, tenancy};
use serde::Deserialize;
use sqlx::PgPool;

use crate::error::ApiError;

/// Signature algorithms Meridian accepts. Asymmetric only — a symmetric
/// algorithm would mean sharing the IdP's signing secret, which is exactly
/// what an OIDC-native catalog must never do.
const ALLOWED_ALGORITHMS: &[Algorithm] = &[
    Algorithm::RS256,
    Algorithm::RS384,
    Algorithm::RS512,
    Algorithm::ES256,
    Algorithm::ES384,
];

/// Paths that never require authentication: orchestrator probes must work
/// while the IdP is down or unconfigured.
const OPEN_PATHS: &[&str] = &["/healthz", "/readyz"];

/// Path prefix for the recipient-facing data-sharing endpoint (Pillar J,
/// J-F1). These requests are authenticated by the share **token** in the URL
/// (an external recipient org holds no Meridian OIDC identity), not by the
/// bearer-JWT middleware — so the middleware passes them through and
/// `routes::shares` resolves the share by its token and constructs a synthetic
/// recipient principal itself. The token is a high-entropy bearer secret; a
/// bad or revoked token gets a clean 401/403 from the handler.
const SHARE_PREFIX: &str = "/share/";

/// Timeout for JWKS/discovery requests to the IdP.
const IDP_HTTP_TIMEOUT: Duration = Duration::from_secs(10);

/// Shared state of the authentication middleware.
#[derive(Debug, Clone)]
pub(crate) struct AuthState {
    inner: Arc<AuthInner>,
}

#[derive(Debug)]
struct AuthInner {
    mode: Mode,
    pool: PgPool,
    /// `(issuer, subject)` pairs already provisioned by this process, so
    /// steady-state requests skip the JIT upsert round-trip.
    provisioned: std::sync::RwLock<HashSet<(String, String)>>,
}

#[derive(Debug)]
enum Mode {
    Disabled,
    Oidc(Authenticator),
}

impl AuthState {
    /// Builds the middleware state from configuration.
    ///
    /// Never fails: an OIDC setup that cannot be initialized fails
    /// *closed* (every request is rejected) with an error log, never open.
    pub(crate) fn from_app_config(config: &AppConfig, pool: PgPool) -> Self {
        let mode = match config.auth.mode {
            AuthMode::Disabled => {
                tracing::warn!(
                    "AUTHENTICATION IS DISABLED (auth.mode = \"disabled\"): every request runs \
                     as the anonymous principal, and anyone who can reach this server owns the \
                     catalog. Do not expose it to a network you do not fully trust."
                );
                Mode::Disabled
            }
            AuthMode::Oidc => match Authenticator::from_config(&config.auth.oidc) {
                Ok(authenticator) => {
                    authenticator.spawn_prefetch();
                    Mode::Oidc(authenticator)
                }
                Err(error) => {
                    tracing::error!(
                        %error,
                        "failed to initialize OIDC authentication; failing closed \
                         (every request will be rejected until this is fixed)"
                    );
                    Mode::Oidc(Authenticator::fail_closed())
                }
            },
        };
        Self {
            inner: Arc::new(AuthInner {
                mode,
                pool,
                provisioned: std::sync::RwLock::new(HashSet::new()),
            }),
        }
    }

    /// JIT-provisions the caller's `principals` row on first sight
    /// (per-process cache avoids re-checking on every request).
    async fn provision(&self, principal: &Principal) -> Result<()> {
        let Some(issuer) = principal.issuer.clone() else {
            // Only OIDC principals reach here; defensive rather than panicky.
            return Ok(());
        };
        let key = (issuer, principal.subject.clone());
        // Guard scope kept away from the awaits below (a std lock guard
        // held across an await would make this future !Send).
        let already_provisioned = self
            .inner
            .provisioned
            .read()
            .is_ok_and(|seen| seen.contains(&key));
        if already_provisioned {
            return Ok(());
        }
        principal::ensure(&self.inner.pool, tenancy::default_workspace_id(), principal).await?;
        if let Ok(mut seen) = self.inner.provisioned.write() {
            seen.insert(key);
        }
        Ok(())
    }
}

/// The authentication middleware: establishes the request's [`Principal`].
///
/// Installed via `axum::middleware::from_fn_with_state` on every route.
/// Health probes pass through untouched; everything else either gets a
/// principal in its request extensions or an error response.
pub(crate) async fn authenticate(
    State(auth): State<AuthState>,
    mut request: Request,
    next: Next,
) -> Response {
    let path = request.uri().path();
    if OPEN_PATHS.contains(&path) || path.starts_with(SHARE_PREFIX) {
        // Health probes and the token-authenticated recipient-sharing endpoint
        // bypass the OIDC middleware. The share endpoint does its own
        // token-based authentication in `routes::shares`.
        return next.run(request).await;
    }

    let principal = match &auth.inner.mode {
        Mode::Disabled => Principal::anonymous(),
        Mode::Oidc(authenticator) => {
            // Owned copy: borrowing the token from the request across the
            // awaits below would require the (deliberately !Sync) request
            // body to be shareable.
            let token = match bearer_token(&request) {
                Ok(token) => token.to_owned(),
                Err(error) => return with_www_authenticate(error.into_response()),
            };
            let outcome = async {
                let principal = authenticator.validate(&token).await?;
                auth.provision(&principal).await.map_err(ApiError::from)?;
                Ok::<_, ApiError>(principal)
            }
            .await;
            match outcome {
                Ok(principal) => principal,
                Err(error) => return with_www_authenticate(error.into_response()),
            }
        }
    };

    request.extensions_mut().insert(principal);
    next.run(request).await
}

/// Adds the RFC 6750 challenge header to 401 responses.
fn with_www_authenticate(mut response: Response) -> Response {
    if response.status() == StatusCode::UNAUTHORIZED {
        response.headers_mut().insert(
            header::WWW_AUTHENTICATE,
            HeaderValue::from_static("Bearer realm=\"meridian\""),
        );
    }
    response
}

/// 401 in the IRC envelope. The spec's exception type for bad/missing
/// credentials is `NotAuthorizedException`.
fn unauthorized(message: impl Into<String>) -> ApiError {
    ApiError::new(StatusCode::UNAUTHORIZED, "NotAuthorizedException", message)
}

/// Extracts the bearer token from the `Authorization` header.
fn bearer_token(request: &Request) -> std::result::Result<&str, ApiError> {
    let Some(value) = request.headers().get(header::AUTHORIZATION) else {
        return Err(unauthorized("missing bearer token"));
    };
    let value = value
        .to_str()
        .map_err(|_| unauthorized("malformed Authorization header"))?;
    let token = value
        .strip_prefix("Bearer ")
        .or_else(|| value.strip_prefix("bearer "))
        .map(str::trim)
        .filter(|t| !t.is_empty())
        .ok_or_else(|| unauthorized("Authorization header must carry a Bearer token"))?;
    Ok(token)
}

/// Validates tokens against the configured issuers.
#[derive(Debug)]
struct Authenticator {
    issuers: Vec<IssuerValidator>,
    clock_skew_secs: u64,
    service_claim: Option<String>,
}

/// One trusted issuer: its audience and JWKS cache.
#[derive(Debug)]
struct IssuerValidator {
    issuer_url: String,
    audience: String,
    jwks: Arc<jwks::JwksCache>,
}

/// The claims Meridian consumes. Registered claims (`exp`/`nbf`/`iss`/
/// `aud`) are checked by `jsonwebtoken` and not re-read here; the rest
/// feed principal construction.
#[derive(Debug, Deserialize)]
struct TokenClaims {
    /// Stable subject identifier; becomes `Principal::subject` verbatim.
    sub: String,
    #[serde(default)]
    email: Option<String>,
    #[serde(default)]
    preferred_username: Option<String>,
    #[serde(default)]
    client_id: Option<String>,
    /// Auth0-style grant-type claim; `client-credentials` marks a workload.
    #[serde(default)]
    gty: Option<String>,
    /// Everything else, consulted for the configurable service claim.
    #[serde(flatten)]
    extra: serde_json::Map<String, serde_json::Value>,
}

impl Authenticator {
    /// Builds validators from config. Non-https issuers are accepted here
    /// only because [`meridian_common::config::AuthConfig::validate`]
    /// already gated them behind `require_https_issuers = false`; they
    /// still get a warning.
    fn from_config(config: &OidcConfig) -> Result<Self> {
        let http = reqwest::Client::builder()
            .timeout(IDP_HTTP_TIMEOUT)
            .build()
            .map_err(|e| MeridianError::internal("failed to build the JWKS HTTP client", e))?;

        let mut issuers = Vec::with_capacity(config.issuers.len());
        for issuer in &config.issuers {
            if !issuer.issuer_url.starts_with("https://") {
                tracing::warn!(
                    issuer = %issuer.issuer_url,
                    "OIDC issuer is not https (require_https_issuers is off) — \
                     acceptable for tests only, never for production"
                );
            }
            issuers.push(IssuerValidator {
                issuer_url: issuer.issuer_url.clone(),
                audience: issuer.audience.clone(),
                jwks: Arc::new(jwks::JwksCache::new(
                    issuer.issuer_url.clone(),
                    issuer.jwks_uri.clone(),
                    http.clone(),
                )),
            });
        }

        Ok(Self {
            issuers,
            clock_skew_secs: config.clock_skew_secs,
            service_claim: config.service_claim.clone(),
        })
    }

    /// An authenticator that rejects everything (used when OIDC
    /// initialization fails: fail closed, never open).
    fn fail_closed() -> Self {
        Self {
            issuers: Vec::new(),
            clock_skew_secs: 0,
            service_claim: None,
        }
    }

    /// Kicks off boot-time JWKS fetches when a runtime is available.
    /// Best-effort by design: startup and liveness must not block on the
    /// IdP.
    fn spawn_prefetch(&self) {
        let Ok(handle) = tokio::runtime::Handle::try_current() else {
            tracing::debug!("no tokio runtime at auth setup; JWKS is fetched on first use");
            return;
        };
        for issuer in &self.issuers {
            let jwks = Arc::clone(&issuer.jwks);
            handle.spawn(async move { jwks.prefetch().await });
        }
    }

    /// Validates a bearer token and builds the caller's [`Principal`].
    async fn validate(&self, token: &str) -> std::result::Result<Principal, ApiError> {
        let header = decode_header(token).map_err(|error| {
            tracing::debug!(%error, "rejected token with unparsable header");
            unauthorized("invalid bearer token: malformed")
        })?;
        if !ALLOWED_ALGORITHMS.contains(&header.alg) {
            return Err(unauthorized(
                "invalid bearer token: unsupported signature algorithm",
            ));
        }

        let iss = peek_issuer(token, header.alg)
            .ok_or_else(|| unauthorized("invalid bearer token: missing issuer claim"))?;
        let issuer = self
            .issuers
            .iter()
            .find(|candidate| candidate.issuer_url == iss)
            .ok_or_else(|| {
                tracing::debug!(issuer = %iss, "rejected token from unconfigured issuer");
                unauthorized("invalid bearer token: unknown issuer")
            })?;

        let jwk = self.resolve_key(issuer, header.kid.as_deref()).await?;
        let key = DecodingKey::from_jwk(&jwk).map_err(|error| {
            tracing::warn!(issuer = %iss, %error, "JWKS key is unusable");
            unauthorized("invalid bearer token: unusable signing key")
        })?;

        let mut validation = Validation::new(header.alg);
        validation.set_issuer(&[&issuer.issuer_url]);
        validation.set_audience(&[&issuer.audience]);
        validation.set_required_spec_claims(&["exp", "iss", "aud"]);
        validation.leeway = self.clock_skew_secs;
        validation.validate_nbf = true;

        let data = decode::<TokenClaims>(token, &key, &validation).map_err(|error| {
            tracing::debug!(issuer = %iss, %error, "rejected invalid token");
            unauthorized(format!(
                "invalid bearer token: {}",
                rejection_reason(&error)
            ))
        })?;

        Ok(self.principal_from_claims(data.claims, &issuer.issuer_url))
    }

    /// Finds the signing key for a `kid`, refreshing the JWKS once when it
    /// is unknown (key rotation).
    async fn resolve_key(
        &self,
        issuer: &IssuerValidator,
        kid: Option<&str>,
    ) -> std::result::Result<Jwk, ApiError> {
        if let Some(jwk) = issuer.jwks.key_for(kid).await {
            return Ok(jwk);
        }
        issuer
            .jwks
            .refresh_for_unknown_key()
            .await
            .map_err(ApiError::from)?;
        issuer
            .jwks
            .key_for(kid)
            .await
            .ok_or_else(|| unauthorized("invalid bearer token: unknown signing key"))
    }

    /// Builds the [`Principal`] per the contract in
    /// `meridian_common::principal`:
    ///
    /// - `kind`: [`PrincipalKind::Service`] when the token carries
    ///   client-credentials-style identity — `gty = "client-credentials"`,
    ///   the configured `auth.oidc.service_claim` is present (and not
    ///   `false`/`null`), or the token has neither `email` nor
    ///   `preferred_username`. Otherwise [`PrincipalKind::User`].
    /// - `subject`: the raw `sub` claim; the issuer travels separately.
    /// - `display_name`: `preferred_username`, then `email`, then
    ///   `client_id`.
    fn principal_from_claims(&self, claims: TokenClaims, issuer_url: &str) -> Principal {
        let service_marked = claims.gty.as_deref() == Some("client-credentials")
            || self.service_claim.as_deref().is_some_and(|name| {
                claims.extra.get(name).is_some_and(|value| {
                    !matches!(
                        value,
                        serde_json::Value::Null | serde_json::Value::Bool(false)
                    )
                })
            });
        let has_user_identity = claims.email.is_some() || claims.preferred_username.is_some();
        let kind = if service_marked || !has_user_identity {
            PrincipalKind::Service
        } else {
            PrincipalKind::User
        };
        let display_name = claims
            .preferred_username
            .or(claims.email)
            .or(claims.client_id);
        Principal {
            kind,
            subject: claims.sub,
            issuer: Some(issuer_url.to_owned()),
            display_name,
        }
    }
}

/// Reads the unverified `iss` claim so the right issuer's keys and rules
/// can be selected. Safe by construction: the value is only used to *pick*
/// a validator, and the subsequent full validation independently enforces
/// signature, `iss`, `aud`, `exp`, and `nbf`.
fn peek_issuer(token: &str, alg: Algorithm) -> Option<String> {
    #[derive(Deserialize)]
    struct IssOnly {
        iss: String,
    }
    let mut validation = Validation::new(alg);
    validation.insecure_disable_signature_validation();
    validation.validate_exp = false;
    validation.validate_nbf = false;
    validation.validate_aud = false;
    validation.required_spec_claims = HashSet::new();
    decode::<IssOnly>(token, &DecodingKey::from_secret(&[]), &validation)
        .ok()
        .map(|data| data.claims.iss)
}

/// A coarse, client-safe reason string for a rejected token. Never echoes
/// claim values.
fn rejection_reason(error: &jsonwebtoken::errors::Error) -> &'static str {
    use jsonwebtoken::errors::ErrorKind;
    match error.kind() {
        ErrorKind::ExpiredSignature => "expired",
        ErrorKind::ImmatureSignature => "not yet valid (nbf)",
        ErrorKind::InvalidSignature => "signature verification failed",
        ErrorKind::InvalidAudience => "audience mismatch",
        ErrorKind::InvalidIssuer => "issuer mismatch",
        ErrorKind::MissingRequiredClaim(_) => "missing required claim",
        _ => "rejected",
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn authenticator(service_claim: Option<&str>) -> Authenticator {
        Authenticator {
            issuers: Vec::new(),
            clock_skew_secs: 60,
            service_claim: service_claim.map(str::to_owned),
        }
    }

    fn claims(json: serde_json::Value) -> TokenClaims {
        serde_json::from_value(json).expect("valid claims")
    }

    #[test]
    fn user_tokens_become_user_principals() {
        let principal = authenticator(None).principal_from_claims(
            claims(serde_json::json!({
                "sub": "auth0|abc",
                "email": "alice@example.com",
                "preferred_username": "alice",
            })),
            "https://idp.example.com",
        );
        assert_eq!(principal.kind, PrincipalKind::User);
        assert_eq!(principal.subject, "auth0|abc");
        assert_eq!(principal.issuer.as_deref(), Some("https://idp.example.com"));
        // preferred_username wins over email.
        assert_eq!(principal.display_name.as_deref(), Some("alice"));
        assert_eq!(principal.audit_string(), "user:auth0|abc");
    }

    #[test]
    fn client_credentials_tokens_become_service_principals() {
        // No user identity claims at all.
        let bare = authenticator(None).principal_from_claims(
            claims(serde_json::json!({ "sub": "svc-1", "client_id": "spark-etl" })),
            "https://idp.example.com",
        );
        assert_eq!(bare.kind, PrincipalKind::Service);
        assert_eq!(bare.display_name.as_deref(), Some("spark-etl"));

        // gty=client-credentials wins even when an email is present.
        let gty = authenticator(None).principal_from_claims(
            claims(serde_json::json!({
                "sub": "svc-2",
                "email": "robot@example.com",
                "gty": "client-credentials",
            })),
            "https://idp.example.com",
        );
        assert_eq!(gty.kind, PrincipalKind::Service);

        // The configured service claim wins too, unless false/null.
        let auth = authenticator(Some("is_service"));
        let marked = auth.principal_from_claims(
            claims(serde_json::json!({
                "sub": "svc-3",
                "email": "robot@example.com",
                "is_service": true,
            })),
            "https://idp.example.com",
        );
        assert_eq!(marked.kind, PrincipalKind::Service);
        let unmarked = auth.principal_from_claims(
            claims(serde_json::json!({
                "sub": "u-1",
                "email": "human@example.com",
                "is_service": false,
            })),
            "https://idp.example.com",
        );
        assert_eq!(unmarked.kind, PrincipalKind::User);
    }
}
