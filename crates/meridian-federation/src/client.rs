//! A minimal, read-only HTTP client for the Iceberg REST Catalog (IRC)
//! protocol — the read subset an inbound mirror needs:
//!
//! - `GET {base}/v1/config` — confirm reachability and pick up server
//!   `defaults`/`overrides` (informational).
//! - `GET {base}/v1/{prefix}/namespaces[?parent=…]` — one level of namespaces.
//! - `GET {base}/v1/{prefix}/namespaces/{ns}/tables` — table identifiers.
//! - `GET {base}/v1/{prefix}/namespaces/{ns}/tables/{table}` — `loadTable`.
//!
//! Deliberately dependency-light: `reqwest` (rustls) + `serde_json`, no
//! generated SDK — matching the CLI's client. It speaks only GETs; a mirror is
//! read-only by construction. Namespace levels are joined with the `0x1F` unit
//! separator, percent-encoded (`%1F`) in URL paths, exactly as the spec's wire
//! format requires.
//!
//! Auth: [`MirrorAuth`] covers `none`, a static bearer token, and OAuth2
//! client-credentials (a token is fetched from the source's token endpoint and
//! cached until shortly before it expires). Meridian *is* an IRC, so this
//! client talks to another Meridian instance unchanged (the Meridian-to-Meridian
//! mirror path).

use std::sync::Mutex;
use std::time::{Duration, Instant};

use serde::Deserialize;
use serde_json::Value;

/// The `0x1F` unit separator used to join namespace levels on the wire.
const UNIT_SEPARATOR: char = '\u{1f}';

/// A failure talking to a source catalog. All variants are retryable at the
/// *run* level (the sync worker re-tries the mirror later); none panics.
#[derive(Debug, thiserror::Error)]
pub enum IrcClientError {
    /// The HTTP request could not be built or sent (DNS, TLS, connection).
    #[error("request to source catalog failed: {0}")]
    Transport(String),
    /// The source returned a non-success status. Carries the status and the
    /// server's error message when it spoke the IRC error envelope.
    #[error("source catalog returned {status}: {message}")]
    Status {
        /// HTTP status code.
        status: u16,
        /// Server-provided (or synthesized) message.
        message: String,
    },
    /// A response body was not the JSON shape the protocol requires.
    #[error("malformed response from source catalog: {0}")]
    Malformed(String),
    /// Acquiring an OAuth2 access token failed.
    #[error("failed to obtain access token from source: {0}")]
    Auth(String),
}

/// How to authenticate to the source catalog.
#[derive(Debug, Clone)]
pub enum MirrorAuth {
    /// No authentication (dev/local catalogs with auth disabled).
    None,
    /// A static bearer token, sent as `Authorization: Bearer <token>`.
    Bearer(String),
    /// OAuth2 client-credentials: exchange client id/secret at `token_url` for
    /// a bearer token, then use it until it nears expiry.
    OAuth2 {
        /// Token endpoint URL.
        token_url: String,
        /// OAuth2 client id.
        client_id: String,
        /// OAuth2 client secret.
        client_secret: String,
        /// Optional space-delimited scope.
        scope: Option<String>,
    },
}

/// A table identifier as the source reports it.
#[derive(Debug, Clone)]
pub struct RemoteTable {
    /// Namespace levels.
    pub namespace: Vec<String>,
    /// Table name.
    pub name: String,
}

/// A loaded table: its current metadata location and the raw `metadata` object
/// (parsed by the sync engine via [`meridian_iceberg`], which handles v1/v2/v3
/// normalization).
#[derive(Debug, Clone)]
pub struct LoadedTable {
    /// The source's current `metadata.json` location (the incremental key).
    pub metadata_location: Option<String>,
    /// The raw `metadata` JSON object from the `loadTable` response.
    pub metadata: Value,
}

/// A cached OAuth2 access token and the instant it should be refreshed by.
#[derive(Debug)]
struct CachedToken {
    token: String,
    refresh_at: Instant,
}

/// A read-only IRC client bound to one source catalog + warehouse prefix.
#[derive(Debug)]
pub struct IrcClient {
    http: reqwest::Client,
    /// Base URL with no trailing slash and a scheme (e.g.
    /// `http://host:8181/iceberg`).
    base: String,
    /// The `{prefix}` (warehouse) segment used in every path.
    prefix: String,
    auth: MirrorAuth,
    /// Cached OAuth2 token (only used for [`MirrorAuth::OAuth2`]).
    token_cache: Mutex<Option<CachedToken>>,
}

impl IrcClient {
    /// Builds a client for `base` (the IRC REST base carrying `/v1/config`) and
    /// `prefix` (the source warehouse to address). `timeout` bounds every HTTP
    /// call. Returns an error only if the underlying HTTP client cannot be
    /// constructed.
    pub fn new(
        base: &str,
        prefix: &str,
        auth: MirrorAuth,
        timeout: Duration,
    ) -> Result<Self, IrcClientError> {
        let http = reqwest::Client::builder()
            .timeout(timeout)
            .build()
            .map_err(|e| IrcClientError::Transport(format!("failed to build HTTP client: {e}")))?;
        Ok(Self {
            http,
            base: normalize_base(base),
            prefix: prefix.trim_matches('/').to_owned(),
            auth,
            token_cache: Mutex::new(None),
        })
    }

    /// `GET /v1/config` — confirms reachability. The response is parsed but its
    /// contents are only informational for a mirror; a non-success status here
    /// is how "the source is unreachable / misconfigured" surfaces early.
    pub async fn get_config(&self) -> Result<Value, IrcClientError> {
        // The warehouse selector belongs on config per the spec; harmless when
        // the source ignores it.
        let url = format!("{}/v1/config?warehouse={}", self.base, encode(&self.prefix));
        self.get_json(&url).await
    }

    /// Lists the namespaces exactly one level below `parent` (top-level when
    /// `parent` is empty), following `next-page-token` pagination to
    /// completion. This is one level only — [`Self::list_all_namespaces`] walks
    /// the tree.
    pub async fn list_namespaces_under(
        &self,
        parent: &[String],
    ) -> Result<Vec<Vec<String>>, IrcClientError> {
        let mut out = Vec::new();
        let mut page_token: Option<String> = None;
        loop {
            let mut url = format!("{}/v1/{}/namespaces", self.base, encode(&self.prefix));
            let mut sep = '?';
            if !parent.is_empty() {
                url.push(sep);
                url.push_str("parent=");
                url.push_str(&encode(&join_levels(parent)));
                sep = '&';
            }
            if let Some(token) = &page_token {
                url.push(sep);
                url.push_str("pageToken=");
                url.push_str(&encode(token));
            }
            let body = self.get_json(&url).await?;
            let page: ListNamespacesBody = parse(body)?;
            out.extend(page.namespaces);
            match page.next_page_token {
                Some(token) if !token.is_empty() => page_token = Some(token),
                _ => break,
            }
        }
        Ok(out)
    }

    /// Recursively enumerates **every** namespace in the source warehouse by
    /// walking one level at a time (the spec's list is single-level). Bounded
    /// by `max_depth` so a pathological/looping source cannot spin forever.
    pub async fn list_all_namespaces(
        &self,
        max_depth: usize,
    ) -> Result<Vec<Vec<String>>, IrcClientError> {
        let mut all: Vec<Vec<String>> = Vec::new();
        // BFS over namespace levels. `frontier` holds parents whose children we
        // still need to fetch; start from the root (empty parent).
        let mut frontier: Vec<Vec<String>> = vec![Vec::new()];
        let mut depth = 0usize;
        while !frontier.is_empty() {
            if depth > max_depth {
                tracing::warn!(
                    max_depth,
                    "namespace tree deeper than max_depth; truncating recursion"
                );
                break;
            }
            let mut next_frontier = Vec::new();
            for parent in &frontier {
                let children = self.list_namespaces_under(parent).await?;
                for child in children {
                    // Only descend into genuinely deeper namespaces (a child
                    // must extend its parent by one level); guards against a
                    // source echoing the parent back and looping.
                    if child.len() > parent.len() {
                        next_frontier.push(child.clone());
                    }
                    all.push(child);
                }
            }
            frontier = next_frontier;
            depth += 1;
        }
        // De-duplicate: a well-behaved source will not repeat, but a listing
        // that returns full paths at every level could. Stable, cheap.
        all.sort();
        all.dedup();
        Ok(all)
    }

    /// Lists the table identifiers in one namespace, following pagination.
    pub async fn list_tables(
        &self,
        namespace: &[String],
    ) -> Result<Vec<RemoteTable>, IrcClientError> {
        let mut out = Vec::new();
        let mut page_token: Option<String> = None;
        loop {
            let mut url = format!(
                "{}/v1/{}/namespaces/{}/tables",
                self.base,
                encode(&self.prefix),
                encode(&join_levels(namespace))
            );
            if let Some(token) = &page_token {
                url.push_str("?pageToken=");
                url.push_str(&encode(token));
            }
            let body = self.get_json(&url).await?;
            let page: ListTablesBody = parse(body)?;
            for ident in page.identifiers {
                out.push(RemoteTable {
                    namespace: ident.namespace,
                    name: ident.name,
                });
            }
            match page.next_page_token {
                Some(token) if !token.is_empty() => page_token = Some(token),
                _ => break,
            }
        }
        Ok(out)
    }

    /// `loadTable` — loads a table's current metadata location and raw metadata.
    pub async fn load_table(
        &self,
        namespace: &[String],
        name: &str,
    ) -> Result<LoadedTable, IrcClientError> {
        let url = format!(
            "{}/v1/{}/namespaces/{}/tables/{}",
            self.base,
            encode(&self.prefix),
            encode(&join_levels(namespace)),
            encode(name)
        );
        let body = self.get_json(&url).await?;
        let metadata = body.get("metadata").cloned().ok_or_else(|| {
            IrcClientError::Malformed("loadTable response has no metadata".into())
        })?;
        let metadata_location = body
            .get("metadata-location")
            .and_then(Value::as_str)
            .map(str::to_owned);
        Ok(LoadedTable {
            metadata_location,
            metadata,
        })
    }

    /// Issues an authenticated GET and parses a JSON body, mapping non-success
    /// statuses to [`IrcClientError::Status`] (using the IRC error envelope's
    /// message when present).
    async fn get_json(&self, url: &str) -> Result<Value, IrcClientError> {
        let mut request = self.http.get(url);
        if let Some(token) = self.bearer_token().await? {
            request = request.bearer_auth(token);
        }
        let response = request
            .send()
            .await
            .map_err(|e| IrcClientError::Transport(e.to_string()))?;
        let status = response.status();
        if !status.is_success() {
            let body: Value = response.json().await.unwrap_or(Value::Null);
            let message = body
                .pointer("/error/message")
                .and_then(Value::as_str)
                .unwrap_or("no error message")
                .to_owned();
            return Err(IrcClientError::Status {
                status: status.as_u16(),
                message,
            });
        }
        response
            .json()
            .await
            .map_err(|e| IrcClientError::Malformed(e.to_string()))
    }

    /// Resolves the bearer token to send, if any: `None` for
    /// [`MirrorAuth::None`], the static token for [`MirrorAuth::Bearer`], and a
    /// cached-or-freshly-fetched token for [`MirrorAuth::OAuth2`].
    async fn bearer_token(&self) -> Result<Option<String>, IrcClientError> {
        match &self.auth {
            MirrorAuth::None => Ok(None),
            MirrorAuth::Bearer(token) => Ok(Some(token.clone())),
            MirrorAuth::OAuth2 {
                token_url,
                client_id,
                client_secret,
                scope,
            } => {
                // Serve a cached token if it is still comfortably valid.
                if let Ok(guard) = self.token_cache.lock()
                    && let Some(cached) = guard.as_ref()
                    && cached.refresh_at > Instant::now()
                {
                    return Ok(Some(cached.token.clone()));
                }
                let (token, ttl) = self
                    .fetch_oauth_token(token_url, client_id, client_secret, scope.as_deref())
                    .await?;
                // Refresh a minute before expiry (or halfway through very short
                // TTLs), so an in-flight sync never uses an expired token.
                let lead = ttl.min(Duration::from_secs(60)).max(Duration::from_secs(1));
                let refresh_at =
                    Instant::now() + ttl.saturating_sub(lead).max(Duration::from_secs(1));
                if let Ok(mut guard) = self.token_cache.lock() {
                    *guard = Some(CachedToken {
                        token: token.clone(),
                        refresh_at,
                    });
                }
                Ok(Some(token))
            }
        }
    }

    /// Performs the OAuth2 client-credentials token exchange. Returns the access
    /// token and its lifetime (defaulting to 1 hour when the server omits
    /// `expires_in`).
    async fn fetch_oauth_token(
        &self,
        token_url: &str,
        client_id: &str,
        client_secret: &str,
        scope: Option<&str>,
    ) -> Result<(String, Duration), IrcClientError> {
        let mut form = vec![
            ("grant_type", "client_credentials".to_owned()),
            ("client_id", client_id.to_owned()),
            ("client_secret", client_secret.to_owned()),
        ];
        if let Some(scope) = scope {
            form.push(("scope", scope.to_owned()));
        }
        let response = self
            .http
            .post(token_url)
            .form(&form)
            .send()
            .await
            .map_err(|e| IrcClientError::Auth(e.to_string()))?;
        if !response.status().is_success() {
            return Err(IrcClientError::Auth(format!(
                "token endpoint returned {}",
                response.status()
            )));
        }
        let body: TokenResponse = response
            .json()
            .await
            .map_err(|e| IrcClientError::Auth(format!("token response not JSON: {e}")))?;
        let ttl = Duration::from_secs(body.expires_in.unwrap_or(3600).max(1));
        Ok((body.access_token, ttl))
    }
}

/// OAuth2 token-endpoint response (the fields we use).
#[derive(Debug, Deserialize)]
struct TokenResponse {
    access_token: String,
    #[serde(default)]
    expires_in: Option<u64>,
}

/// `ListNamespacesResponse` (the fields we read).
#[derive(Debug, Deserialize)]
struct ListNamespacesBody {
    #[serde(default)]
    namespaces: Vec<Vec<String>>,
    #[serde(rename = "next-page-token", default)]
    next_page_token: Option<String>,
}

/// `ListTablesResponse` (the fields we read).
#[derive(Debug, Deserialize)]
struct ListTablesBody {
    #[serde(default)]
    identifiers: Vec<TableIdentBody>,
    #[serde(rename = "next-page-token", default)]
    next_page_token: Option<String>,
}

/// A `TableIdentifier` in a list response.
#[derive(Debug, Deserialize)]
struct TableIdentBody {
    #[serde(default)]
    namespace: Vec<String>,
    name: String,
}

/// Deserializes a `Value` into a typed body, mapping failure to
/// [`IrcClientError::Malformed`].
fn parse<T: for<'de> Deserialize<'de>>(value: Value) -> Result<T, IrcClientError> {
    serde_json::from_value(value).map_err(|e| IrcClientError::Malformed(e.to_string()))
}

/// Joins namespace levels with the unit separator (the pre-encoding wire form).
fn join_levels(levels: &[String]) -> String {
    levels.join(&UNIT_SEPARATOR.to_string())
}

/// Percent-encodes a single path/query segment. Encodes the unit separator to
/// `%1F` and other reserved characters so multi-level namespaces and
/// awkward names travel safely.
fn encode(segment: &str) -> String {
    let mut out = String::with_capacity(segment.len());
    for byte in segment.bytes() {
        match byte {
            // RFC 3986 unreserved set — safe to leave as-is.
            b'A'..=b'Z' | b'a'..=b'z' | b'0'..=b'9' | b'-' | b'_' | b'.' | b'~' => {
                out.push(byte as char);
            }
            other => {
                out.push('%');
                out.push(hex_digit(other >> 4));
                out.push(hex_digit(other & 0x0f));
            }
        }
    }
    out
}

/// Uppercase hex digit for a nibble (`0..=15`).
fn hex_digit(nibble: u8) -> char {
    match nibble {
        0..=9 => (b'0' + nibble) as char,
        _ => (b'A' + (nibble - 10)) as char,
    }
}

/// Normalizes a base URL: strips a trailing slash and defaults the scheme to
/// `http://` when none is present (so `localhost:8181/iceberg` works), matching
/// the CLI client's convenience.
fn normalize_base(base: &str) -> String {
    let trimmed = base.trim_end_matches('/');
    if trimmed.contains("://") {
        trimmed.to_owned()
    } else {
        format!("http://{trimmed}")
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn encode_handles_unit_separator_and_reserved() {
        assert_eq!(encode("plain"), "plain");
        assert_eq!(encode("a.b-c_d~e"), "a.b-c_d~e");
        // Unit separator -> %1F; slash and space encoded.
        assert_eq!(encode("a\u{1f}b"), "a%1Fb");
        assert_eq!(encode("a/b c"), "a%2Fb%20c");
    }

    #[test]
    fn join_levels_uses_unit_separator() {
        assert_eq!(join_levels(&["a".into(), "b".into()]), "a\u{1f}b");
        assert_eq!(
            encode(&join_levels(&["a".into(), "b".into()])),
            "a%1Fb",
            "joined levels encode to the %1F wire form"
        );
    }

    #[test]
    fn normalize_base_adds_scheme_and_trims_slash() {
        assert_eq!(
            normalize_base("localhost:8181/iceberg/"),
            "http://localhost:8181/iceberg"
        );
        assert_eq!(
            normalize_base("https://x/api/catalog"),
            "https://x/api/catalog"
        );
    }
}
