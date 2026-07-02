//! Per-issuer JWKS caching.
//!
//! Keys are fetched once at boot (best-effort — server liveness must not
//! depend on IdP availability) and refreshed on demand when a token carries
//! an unknown `kid` (the standard signal for key rotation). On-demand
//! refreshes are single-flight (concurrent unknown-`kid` requests share one
//! fetch) and rate-limited (a minimum interval between fetches), so a flood
//! of bogus `kid`s cannot be used to hammer the IdP through us.

use std::time::{Duration, Instant};

use jsonwebtoken::jwk::{Jwk, JwkSet};
use meridian_common::MeridianError;
use serde::Deserialize;

/// Minimum time between unknown-`kid` refreshes of one issuer's JWKS.
/// Genuine rotations are rare; anything more frequent is noise or abuse.
const MIN_REFRESH_INTERVAL: Duration = Duration::from_secs(10);

/// OIDC discovery document — only the field we need.
#[derive(Debug, Deserialize)]
struct DiscoveryDocument {
    jwks_uri: String,
}

/// Cached signing keys for one issuer.
#[derive(Debug)]
pub(crate) struct JwksCache {
    /// Issuer URL (for discovery and log context).
    issuer_url: String,
    /// Explicitly configured JWKS endpoint, if any.
    configured_uri: Option<String>,
    /// Shared HTTP client (bounded timeouts; rustls).
    http: reqwest::Client,
    /// JWKS endpoint after discovery; initialized at most once.
    resolved_uri: tokio::sync::OnceCell<String>,
    /// The current key set.
    keys: tokio::sync::RwLock<Vec<Jwk>>,
    /// Single-flight guard and timestamp of the last on-demand refresh.
    refresh: tokio::sync::Mutex<Option<Instant>>,
}

impl JwksCache {
    /// Creates an empty cache; keys arrive via [`prefetch`](Self::prefetch)
    /// or the first on-demand refresh.
    pub(crate) fn new(
        issuer_url: String,
        configured_uri: Option<String>,
        http: reqwest::Client,
    ) -> Self {
        Self {
            issuer_url,
            configured_uri,
            http,
            resolved_uri: tokio::sync::OnceCell::new(),
            keys: tokio::sync::RwLock::new(Vec::new()),
            refresh: tokio::sync::Mutex::new(None),
        }
    }

    /// The JWKS endpoint: the configured one, or the one discovered from
    /// `<issuer>/.well-known/openid-configuration`.
    async fn endpoint(&self) -> Result<&String, MeridianError> {
        self.resolved_uri
            .get_or_try_init(|| async {
                if let Some(uri) = &self.configured_uri {
                    return Ok(uri.clone());
                }
                let url = format!(
                    "{}/.well-known/openid-configuration",
                    self.issuer_url.trim_end_matches('/')
                );
                let doc: DiscoveryDocument = self
                    .http
                    .get(&url)
                    .send()
                    .await
                    .and_then(reqwest::Response::error_for_status)
                    .map_err(|e| discovery_error(&url, &e))?
                    .json()
                    .await
                    .map_err(|e| discovery_error(&url, &e))?;
                Ok(doc.jwks_uri)
            })
            .await
    }

    /// Fetches the key set and replaces the cache contents.
    async fn fetch(&self) -> Result<(), MeridianError> {
        let endpoint = self.endpoint().await?.clone();
        let set: JwkSet = self
            .http
            .get(&endpoint)
            .send()
            .await
            .and_then(reqwest::Response::error_for_status)
            .map_err(|e| jwks_error(&endpoint, &e))?
            .json()
            .await
            .map_err(|e| jwks_error(&endpoint, &e))?;
        tracing::debug!(
            issuer = %self.issuer_url,
            key_count = set.keys.len(),
            "refreshed JWKS"
        );
        *self.keys.write().await = set.keys;
        Ok(())
    }

    /// Boot-time fetch. Failures are logged, never fatal: the server must
    /// come up (and its health endpoints must answer) even when the IdP is
    /// unreachable; keys are then fetched on first use.
    pub(crate) async fn prefetch(&self) {
        if let Err(error) = self.fetch().await {
            tracing::warn!(
                issuer = %self.issuer_url,
                %error,
                "initial JWKS fetch failed; keys will be fetched on demand"
            );
        }
    }

    /// Looks up a key. With a `kid`, matches by key id; without one, a
    /// single-key set is unambiguous and that key is returned.
    pub(crate) async fn key_for(&self, kid: Option<&str>) -> Option<Jwk> {
        let keys = self.keys.read().await;
        match kid {
            Some(kid) => keys
                .iter()
                .find(|k| k.common.key_id.as_deref() == Some(kid))
                .cloned(),
            None if keys.len() == 1 => keys.first().cloned(),
            None => None,
        }
    }

    /// Refreshes the key set in response to an unknown `kid`.
    ///
    /// Single-flight: concurrent callers serialize on the guard, and a
    /// caller that finds a refresh happened within [`MIN_REFRESH_INTERVAL`]
    /// returns without fetching (the caller re-checks the cache either
    /// way). A fetch failure is the IdP being unreachable — surfaced as
    /// `Unavailable`, not as a token problem.
    pub(crate) async fn refresh_for_unknown_key(&self) -> Result<(), MeridianError> {
        let mut last = self.refresh.lock().await;
        if let Some(at) = *last
            && at.elapsed() < MIN_REFRESH_INTERVAL
        {
            return Ok(());
        }
        *last = Some(Instant::now());
        self.fetch().await
    }
}

fn discovery_error(url: &str, error: &reqwest::Error) -> MeridianError {
    MeridianError::Unavailable(format!("OIDC discovery at {url} failed: {error}"))
}

fn jwks_error(endpoint: &str, error: &reqwest::Error) -> MeridianError {
    MeridianError::Unavailable(format!("JWKS fetch from {endpoint} failed: {error}"))
}
