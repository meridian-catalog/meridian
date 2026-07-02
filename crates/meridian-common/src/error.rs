//! The Meridian error model and its HTTP mapping.
//!
//! All fallible public APIs in the workspace return [`MeridianError`]. When an
//! error reaches the HTTP boundary it is rendered as the Iceberg REST catalog
//! error envelope:
//!
//! ```json
//! { "error": { "message": "...", "type": "...", "code": 404 } }
//! ```

use axum::Json;
use axum::http::StatusCode;
use axum::response::{IntoResponse, Response};
use serde::{Deserialize, Serialize};

/// Convenience alias used across the workspace.
pub type Result<T, E = MeridianError> = std::result::Result<T, E>;

/// Boxed error source for wrapping arbitrary underlying failures.
type BoxError = Box<dyn std::error::Error + Send + Sync + 'static>;

/// The unified error type for Meridian services.
///
/// Variants are deliberately coarse: they map one-to-one onto HTTP status
/// codes and the Iceberg REST error envelope's `type` field. Richer,
/// operation-specific error types (e.g. `NoSuchTableException`) will be
/// layered on in M1 when the IRC endpoints that need them exist.
#[derive(Debug, thiserror::Error)]
pub enum MeridianError {
    /// The requested resource does not exist.
    #[error("not found: {0}")]
    NotFound(String),

    /// The request conflicts with current state (e.g. duplicate name,
    /// optimistic-concurrency failure).
    #[error("conflict: {0}")]
    Conflict(String),

    /// The request is syntactically or semantically invalid.
    #[error("validation failed: {0}")]
    Validation(String),

    /// The caller is not authenticated or not permitted.
    #[error("unauthorized: {0}")]
    Unauthorized(String),

    /// The route exists but does not support the request method.
    #[error("method not allowed: {0}")]
    MethodNotAllowed(String),

    /// An unexpected internal failure. The source is logged, never sent to
    /// the client.
    #[error("internal error: {message}")]
    Internal {
        /// Operator-facing description of what failed.
        message: String,
        /// Underlying cause, if any.
        #[source]
        source: Option<BoxError>,
    },

    /// A required dependency (e.g. Postgres) is temporarily unavailable.
    #[error("unavailable: {0}")]
    Unavailable(String),
}

impl MeridianError {
    /// Builds an [`MeridianError::Internal`] from a message and a source error.
    pub fn internal(message: impl Into<String>, source: impl Into<BoxError>) -> Self {
        Self::Internal {
            message: message.into(),
            source: Some(source.into()),
        }
    }

    /// Builds an [`MeridianError::Internal`] with no underlying source.
    pub fn internal_msg(message: impl Into<String>) -> Self {
        Self::Internal {
            message: message.into(),
            source: None,
        }
    }

    /// The HTTP status code this error maps to.
    #[must_use]
    pub fn status_code(&self) -> StatusCode {
        match self {
            Self::NotFound(_) => StatusCode::NOT_FOUND,
            Self::Conflict(_) => StatusCode::CONFLICT,
            Self::Validation(_) => StatusCode::BAD_REQUEST,
            Self::Unauthorized(_) => StatusCode::UNAUTHORIZED,
            Self::MethodNotAllowed(_) => StatusCode::METHOD_NOT_ALLOWED,
            Self::Internal { .. } => StatusCode::INTERNAL_SERVER_ERROR,
            Self::Unavailable(_) => StatusCode::SERVICE_UNAVAILABLE,
        }
    }

    /// The `type` string used in the Iceberg REST error envelope.
    ///
    /// TODO(M1): return operation-specific exception types
    /// (`NoSuchTableException`, `AlreadyExistsException`, ...) once the IRC
    /// table/namespace endpoints exist.
    #[must_use]
    pub fn error_type(&self) -> &'static str {
        match self {
            Self::NotFound(_) => "NotFoundException",
            Self::Conflict(_) => "CommitFailedException",
            Self::Validation(_) => "BadRequestException",
            Self::Unauthorized(_) => "NotAuthorizedException",
            Self::MethodNotAllowed(_) => "MethodNotAllowedException",
            Self::Internal { .. } => "InternalServerError",
            Self::Unavailable(_) => "ServiceUnavailableException",
        }
    }

    /// The message that is safe to expose to API clients.
    ///
    /// Internal errors are masked: the detailed message and source chain are
    /// logged server-side only.
    #[must_use]
    pub fn public_message(&self) -> String {
        match self {
            Self::Internal { .. } => "an internal error occurred".to_owned(),
            other => other.to_string(),
        }
    }
}

/// The Iceberg REST catalog error envelope.
#[derive(Debug, Serialize, Deserialize)]
pub struct ErrorEnvelope {
    /// The single error carried by the envelope.
    pub error: ErrorBody,
}

/// Body of the Iceberg REST error envelope.
#[derive(Debug, Serialize, Deserialize)]
pub struct ErrorBody {
    /// Human-readable description of the failure.
    pub message: String,
    /// Machine-readable error type (exception-style name).
    #[serde(rename = "type")]
    pub error_type: String,
    /// HTTP status code, duplicated in the body per the IRC spec.
    pub code: u16,
}

impl IntoResponse for MeridianError {
    fn into_response(self) -> Response {
        let status = self.status_code();

        if status.is_server_error() {
            // Full detail (including masked internal messages and source
            // chains) goes to the logs, never to the client.
            tracing::error!(error = ?self, "request failed with server error");
        } else {
            tracing::debug!(error = %self, "request failed with client error");
        }

        let envelope = ErrorEnvelope {
            error: ErrorBody {
                message: self.public_message(),
                error_type: self.error_type().to_owned(),
                code: status.as_u16(),
            },
        };

        (status, Json(envelope)).into_response()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn status_codes_match_variants() {
        assert_eq!(
            MeridianError::NotFound("t".into()).status_code(),
            StatusCode::NOT_FOUND
        );
        assert_eq!(
            MeridianError::Conflict("t".into()).status_code(),
            StatusCode::CONFLICT
        );
        assert_eq!(
            MeridianError::Validation("t".into()).status_code(),
            StatusCode::BAD_REQUEST
        );
        assert_eq!(
            MeridianError::Unauthorized("t".into()).status_code(),
            StatusCode::UNAUTHORIZED
        );
        assert_eq!(
            MeridianError::internal_msg("t").status_code(),
            StatusCode::INTERNAL_SERVER_ERROR
        );
        assert_eq!(
            MeridianError::Unavailable("t".into()).status_code(),
            StatusCode::SERVICE_UNAVAILABLE
        );
    }

    #[test]
    fn internal_errors_are_masked() {
        let err = MeridianError::internal("db exploded: password=hunter2", std::fmt::Error);
        assert_eq!(err.public_message(), "an internal error occurred");
        // But the operator-facing Display keeps the detail.
        assert!(err.to_string().contains("db exploded"));
    }

    #[test]
    fn envelope_shape_matches_irc_spec() {
        let err = MeridianError::NotFound("namespace ns1 does not exist".into());
        let envelope = ErrorEnvelope {
            error: ErrorBody {
                message: err.public_message(),
                error_type: err.error_type().to_owned(),
                code: err.status_code().as_u16(),
            },
        };
        let value = serde_json::to_value(&envelope).expect("serialize");
        assert_eq!(
            value,
            serde_json::json!({
                "error": {
                    "message": "not found: namespace ns1 does not exist",
                    "type": "NotFoundException",
                    "code": 404,
                }
            })
        );
    }
}
