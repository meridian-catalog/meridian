//! Typed HTTP client for the transpilation sidecar (§8.5).
//!
//! The sidecar is a separate, stateless, localhost-scoped Python process
//! (`sidecar/`) that runs `SQLGlot`. This module is the Rust half of its HTTP
//! contract (`docs/design/transpilation.md`): the universal-view path (G-F1)
//! and metric compilation (G-F2) call it here rather than reading the Python.
//!
//! # Graceful degradation
//!
//! The client distinguishes a *transpilation outcome* (a well-formed response
//! whose `status` may be `unsupported`) from a *transport failure* (the sidecar
//! is unreachable or errored). Callers on the `LoadView` path treat a transport
//! failure as "serve the canonical representation with a note" — never a 500 —
//! so a sidecar outage degrades a nicety, it does not take the catalog down.
//!
//! # No LLM here
//!
//! The optional BYO-key LLM-assist fallback lives entirely *inside* the sidecar
//! and is off unless the operator configures it there. This client never selects
//! or contacts an LLM; it forwards a translation request and reads back a
//! labelled result. A `best_effort` status that carries an `llm_assist_used`
//! diagnostic is the only way LLM output ever reaches Rust, and it is always
//! labelled, never trusted as `verified`.

use std::time::Duration;

use meridian_common::config::TranspilationConfig;
use serde::{Deserialize, Serialize};

/// The transpile/parse/compile status machine (mirrors the sidecar's
/// `schemas.Status`). Serialized as the lowercase `snake_case` wire strings.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum TranspileStatus {
    /// `SQLGlot` translated the statement *and* the output re-parses cleanly in
    /// the target dialect. Safe to serve to an engine.
    Verified,
    /// Output was produced but a construct was approximated, parse-back
    /// surfaced a difference, or the LLM-assist fallback produced it. Usable,
    /// but the diagnostics must be surfaced; never guaranteed-correct.
    BestEffort,
    /// `SQLGlot` raised and no fallback produced a valid result. No SQL is served
    /// as correct (`sql` is `None`).
    Unsupported,
}

impl TranspileStatus {
    /// The wire/DB string (`verified` | `best_effort` | `unsupported`).
    #[must_use]
    pub fn as_str(self) -> &'static str {
        match self {
            Self::Verified => "verified",
            Self::BestEffort => "best_effort",
            Self::Unsupported => "unsupported",
        }
    }
}

/// One machine-readable note about a transpile/compile outcome.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Diagnostic {
    /// `info` | `warning` | `error`.
    pub severity: String,
    /// Stable short code (e.g. `parse_error`, `parse_back_diff`).
    pub code: String,
    /// Human-readable message.
    pub message: String,
}

/// `POST /v1/transpile` request body.
#[derive(Debug, Clone, Serialize)]
pub struct TranspileRequest {
    /// The statement to translate.
    pub sql: String,
    /// Source SQL dialect (`SQLGlot` dialect name).
    pub from_dialect: String,
    /// Target SQL dialect (`SQLGlot` dialect name).
    pub to_dialect: String,
}

/// `POST /v1/transpile` response body.
#[derive(Debug, Clone, Deserialize)]
pub struct TranspileResponse {
    /// Translated statement, or `None` when `status` is `unsupported`.
    pub sql: Option<String>,
    /// The outcome status.
    pub status: TranspileStatus,
    /// Source dialect (echoed).
    pub from_dialect: String,
    /// Target dialect (echoed).
    pub to_dialect: String,
    /// Zero or more notes.
    #[serde(default)]
    pub diagnostics: Vec<Diagnostic>,
}

/// A metric definition sent to `POST /v1/compile_metric`.
#[derive(Debug, Clone, Serialize)]
pub struct MetricInput {
    /// Metric machine name (the output measure column alias).
    pub name: String,
    /// Measure aggregation expression (e.g. `SUM(amount)`).
    pub expression: String,
    /// Source table/view identifier.
    pub source: String,
    /// Group-by dimensions.
    #[serde(default)]
    pub dimensions: Vec<String>,
    /// Boolean filter fragments (`AND`-ed).
    #[serde(default)]
    pub filters: Vec<String>,
    /// Canonical dialect the fragments are authored in.
    pub dialect: String,
}

/// `POST /v1/compile_metric` request body.
#[derive(Debug, Clone, Serialize)]
pub struct CompileMetricRequest {
    /// The metric to compile.
    pub metric: MetricInput,
    /// The engine dialect to render SQL for.
    pub to_dialect: String,
}

/// `POST /v1/compile_metric` response body.
#[derive(Debug, Clone, Deserialize)]
pub struct CompileMetricResponse {
    /// Compiled SQL, or `None` when `status` is `unsupported`.
    pub sql: Option<String>,
    /// The outcome status.
    pub status: TranspileStatus,
    /// Zero or more notes.
    #[serde(default)]
    pub diagnostics: Vec<Diagnostic>,
}

/// `GET /healthz` response body.
#[derive(Debug, Clone, Deserialize)]
pub struct HealthResponse {
    /// `ok` when live.
    pub status: String,
    /// The `SQLGlot` version the sidecar runs.
    pub sqlglot_version: String,
    /// True only when a BYO-key LLM-assist provider is configured on the
    /// sidecar.
    pub llm_assist: bool,
}

/// A transport-level failure talking to the sidecar (not a transpile outcome).
///
/// The universal-view path maps this to graceful degradation, never a 500.
#[derive(Debug, thiserror::Error)]
pub enum SidecarError {
    /// The HTTP client could not be built.
    #[error("failed to build sidecar HTTP client: {0}")]
    Build(String),
    /// The request did not complete (connection refused, timeout, DNS, ...).
    #[error("sidecar request failed: {0}")]
    Transport(String),
    /// The sidecar answered with a non-success status.
    #[error("sidecar returned HTTP {status}: {body}")]
    Http {
        /// The HTTP status code.
        status: u16,
        /// The (truncated) response body.
        body: String,
    },
    /// The response body did not match the expected schema.
    #[error("sidecar response was not understood: {0}")]
    Decode(String),
}

/// A thin, cloneable HTTP client for the transpilation sidecar.
///
/// Held as a request extension (like the scan-planning runtime) so handlers on
/// the view and semantics paths call the sidecar without the crate depending on
/// its process lifecycle.
#[derive(Debug, Clone)]
pub struct SidecarClient {
    http: reqwest::Client,
    /// Base URL, trailing slash trimmed (e.g. `http://127.0.0.1:8200`).
    base: String,
}

impl SidecarClient {
    /// Builds a client from the transpilation config. Returns an error only if
    /// the underlying HTTP client cannot be constructed (a bad TLS/config
    /// state), which the server surfaces at startup; a merely-unreachable
    /// sidecar is a per-call [`SidecarError::Transport`], not a build failure.
    pub fn from_config(config: &TranspilationConfig) -> Result<Self, SidecarError> {
        let http = reqwest::Client::builder()
            .timeout(Duration::from_secs(config.request_timeout_secs))
            .build()
            .map_err(|e| SidecarError::Build(e.to_string()))?;
        Ok(Self {
            http,
            base: config.sidecar_url.trim_end_matches('/').to_owned(),
        })
    }

    /// `POST /v1/transpile` — translate one statement between dialects.
    pub async fn transpile(
        &self,
        sql: &str,
        from_dialect: &str,
        to_dialect: &str,
    ) -> Result<TranspileResponse, SidecarError> {
        let request = TranspileRequest {
            sql: sql.to_owned(),
            from_dialect: from_dialect.to_owned(),
            to_dialect: to_dialect.to_owned(),
        };
        self.post_json("/v1/transpile", &request).await
    }

    /// `POST /v1/compile_metric` — compile a metric to a chosen engine's SQL.
    pub async fn compile_metric(
        &self,
        request: &CompileMetricRequest,
    ) -> Result<CompileMetricResponse, SidecarError> {
        self.post_json("/v1/compile_metric", request).await
    }

    /// `GET /healthz` — liveness + LLM-assist posture.
    pub async fn health(&self) -> Result<HealthResponse, SidecarError> {
        let url = format!("{}/healthz", self.base);
        let response = self
            .http
            .get(&url)
            .send()
            .await
            .map_err(|e| SidecarError::Transport(e.to_string()))?;
        Self::decode(response).await
    }

    /// POSTs `body` as JSON to `path` and decodes the JSON response.
    async fn post_json<B: Serialize, R: for<'de> Deserialize<'de>>(
        &self,
        path: &str,
        body: &B,
    ) -> Result<R, SidecarError> {
        let url = format!("{}{path}", self.base);
        let response = self
            .http
            .post(&url)
            .json(body)
            .send()
            .await
            .map_err(|e| SidecarError::Transport(e.to_string()))?;
        Self::decode(response).await
    }

    /// Turns a response into the typed body, mapping non-success status and
    /// decode failures onto [`SidecarError`].
    async fn decode<R: for<'de> Deserialize<'de>>(
        response: reqwest::Response,
    ) -> Result<R, SidecarError> {
        let status = response.status();
        if !status.is_success() {
            let body = response.text().await.unwrap_or_default();
            let truncated: String = body.chars().take(500).collect();
            return Err(SidecarError::Http {
                status: status.as_u16(),
                body: truncated,
            });
        }
        response
            .json::<R>()
            .await
            .map_err(|e| SidecarError::Decode(e.to_string()))
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn transpile_status_wire_strings() {
        assert_eq!(TranspileStatus::Verified.as_str(), "verified");
        assert_eq!(TranspileStatus::BestEffort.as_str(), "best_effort");
        assert_eq!(TranspileStatus::Unsupported.as_str(), "unsupported");
    }

    #[test]
    fn transpile_status_deserializes_from_wire() {
        let v: TranspileStatus = serde_json::from_str("\"best_effort\"").expect("parses");
        assert_eq!(v, TranspileStatus::BestEffort);
    }

    #[test]
    fn transpile_response_parses_full_shape() {
        let body = serde_json::json!({
            "sql": "SELECT 1",
            "status": "verified",
            "from_dialect": "spark",
            "to_dialect": "trino",
            "diagnostics": [],
        });
        let parsed: TranspileResponse = serde_json::from_value(body).expect("parses");
        assert_eq!(parsed.status, TranspileStatus::Verified);
        assert_eq!(parsed.sql.as_deref(), Some("SELECT 1"));
    }

    #[test]
    fn from_config_trims_trailing_slash() {
        let config = TranspilationConfig {
            sidecar_url: "http://127.0.0.1:8200/".to_owned(),
            request_timeout_secs: 5,
        };
        let client = SidecarClient::from_config(&config).expect("builds");
        assert_eq!(client.base, "http://127.0.0.1:8200");
    }
}
