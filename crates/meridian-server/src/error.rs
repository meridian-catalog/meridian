//! HTTP-boundary error type with exact Iceberg REST exception names.
//!
//! [`meridian_common::MeridianError`] is deliberately coarse (one variant per
//! status code). The IRC spec, however, prescribes operation-specific
//! exception `type` strings (`NoSuchNamespaceException`,
//! `AlreadyExistsException`, ...). [`ApiError`] carries an explicit
//! status + type + message triple; handlers translate store-layer errors
//! into the exception the operation requires and everything renders as the
//! spec's error envelope.

use axum::Json;
use axum::http::StatusCode;
use axum::response::{IntoResponse, Response};
use meridian_common::MeridianError;
use meridian_common::error::{ErrorBody, ErrorEnvelope};

/// An error ready to render as the IRC error envelope.
#[derive(Debug)]
pub struct ApiError {
    /// HTTP status code.
    pub status: StatusCode,
    /// Exception-style `type` string from the IRC spec.
    pub error_type: &'static str,
    /// Client-safe message.
    pub message: String,
}

impl ApiError {
    /// Builds an error from its three envelope fields.
    #[must_use]
    pub fn new(status: StatusCode, error_type: &'static str, message: impl Into<String>) -> Self {
        Self {
            status,
            error_type,
            message: message.into(),
        }
    }

    /// 404 `NoSuchWarehouseException`: the `{prefix}` (or `warehouse` query
    /// parameter) does not name a registered warehouse.
    #[must_use]
    pub fn no_such_warehouse(name: &str) -> Self {
        Self::new(
            StatusCode::NOT_FOUND,
            "NoSuchWarehouseException",
            format!("warehouse {name:?} does not exist"),
        )
    }

    /// 404 `NoSuchNamespaceException`.
    #[must_use]
    pub fn no_such_namespace(message: impl Into<String>) -> Self {
        Self::new(StatusCode::NOT_FOUND, "NoSuchNamespaceException", message)
    }

    /// 404 `NoSuchTableException`.
    #[must_use]
    pub fn no_such_table(message: impl Into<String>) -> Self {
        Self::new(StatusCode::NOT_FOUND, "NoSuchTableException", message)
    }

    /// 409 `CommitFailedException`: a commit requirement failed or the
    /// retry budget for the compare-and-set race is exhausted. The client
    /// must refresh table state and rebuild its commit.
    #[must_use]
    pub fn commit_failed(message: impl Into<String>) -> Self {
        Self::new(StatusCode::CONFLICT, "CommitFailedException", message)
    }

    /// 500 `CommitStateUnknownException`: the commit reached the point of no
    /// return and its outcome could not be determined (design doc F3). The
    /// client must not assume failure; retrying with the same idempotency
    /// key resolves the ambiguity.
    #[must_use]
    pub fn commit_state_unknown(message: impl Into<String>) -> Self {
        Self::new(
            StatusCode::INTERNAL_SERVER_ERROR,
            "CommitStateUnknownException",
            message,
        )
    }

    /// 409 `AlreadyExistsException` (the YAML's `NamespaceAlreadyExistsError`
    /// example uses this type string).
    #[must_use]
    pub fn already_exists(message: impl Into<String>) -> Self {
        Self::new(StatusCode::CONFLICT, "AlreadyExistsException", message)
    }

    /// 409 `CommitFailedException` for a write against a **foreign** asset
    /// (Pillar B, B-F1): a table/namespace synced read-only from an external
    /// catalog. Unlike an ordinary commit conflict this is a *permanent*
    /// property — the external catalog is the write authority — so the message
    /// says so and points the writer at the source. The type is
    /// `CommitFailedException` so engines treat it as a rejected commit rather
    /// than a transient error to retry.
    #[must_use]
    pub fn foreign_read_only(message: impl Into<String>) -> Self {
        Self::new(StatusCode::CONFLICT, "CommitFailedException", message)
    }

    /// 409 `NamespaceNotEmptyException`.
    #[must_use]
    pub fn namespace_not_empty(message: impl Into<String>) -> Self {
        Self::new(StatusCode::CONFLICT, "NamespaceNotEmptyException", message)
    }

    /// 400 `BadRequestException`.
    #[must_use]
    pub fn bad_request(message: impl Into<String>) -> Self {
        Self::new(StatusCode::BAD_REQUEST, "BadRequestException", message)
    }

    /// 422 `UnprocessableEntityException`: a key appears in both `updates`
    /// and `removals`.
    #[must_use]
    pub fn unprocessable(message: impl Into<String>) -> Self {
        Self::new(
            StatusCode::UNPROCESSABLE_ENTITY,
            "UnprocessableEntityException",
            message,
        )
    }
}

impl From<MeridianError> for ApiError {
    /// Fallback translation for errors a handler does not map explicitly
    /// (internal failures, unavailability, generic validation). Uses the
    /// coarse per-status types from `meridian-common`.
    fn from(error: MeridianError) -> Self {
        Self {
            status: error.status_code(),
            error_type: error.error_type(),
            // public_message() masks internal detail; log the full error
            // here since we bypass MeridianError's own IntoResponse.
            message: {
                if error.status_code().is_server_error() {
                    tracing::error!(error = ?error, "request failed with server error");
                } else {
                    tracing::debug!(error = %error, "request failed with client error");
                }
                error.public_message()
            },
        }
    }
}

impl IntoResponse for ApiError {
    fn into_response(self) -> Response {
        let envelope = ErrorEnvelope {
            error: ErrorBody {
                message: self.message,
                error_type: self.error_type.to_owned(),
                code: self.status.as_u16(),
            },
        };
        (self.status, Json(envelope)).into_response()
    }
}
