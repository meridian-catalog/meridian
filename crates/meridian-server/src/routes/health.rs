//! Liveness and readiness probes.

use axum::Json;
use axum::extract::State;
use axum::http::StatusCode;
use serde::{Deserialize, Serialize};

use crate::AppState;

/// Body returned by `/healthz` and `/readyz`.
#[derive(Debug, Serialize, Deserialize)]
pub struct HealthResponse {
    /// Overall status: `ok` or `unavailable`.
    pub status: String,
    /// Per-dependency status.
    pub checks: HealthChecks,
}

/// Per-dependency health details.
#[derive(Debug, Serialize, Deserialize)]
pub struct HealthChecks {
    /// Postgres reachability: `ok` or `error`.
    pub database: String,
}

async fn database_health(state: &AppState) -> (StatusCode, Json<HealthResponse>) {
    match meridian_store::health_check(&state.pool).await {
        Ok(()) => (
            StatusCode::OK,
            Json(HealthResponse {
                status: "ok".to_owned(),
                checks: HealthChecks {
                    database: "ok".to_owned(),
                },
            }),
        ),
        Err(error) => {
            tracing::warn!(%error, "health check failed");
            (
                StatusCode::SERVICE_UNAVAILABLE,
                Json(HealthResponse {
                    status: "unavailable".to_owned(),
                    checks: HealthChecks {
                        database: "error".to_owned(),
                    },
                }),
            )
        }
    }
}

/// `GET /healthz` — process is up and its database is reachable.
pub async fn healthz(State(state): State<AppState>) -> (StatusCode, Json<HealthResponse>) {
    database_health(&state).await
}

/// `GET /readyz` — ready to accept traffic.
///
/// Currently identical to `/healthz` (the only readiness dependency is
/// Postgres). Kept as a separate endpoint so orchestrators can wire distinct
/// probes now and we can tighten readiness semantics later without breaking
/// deployments.
pub async fn readyz(State(state): State<AppState>) -> (StatusCode, Json<HealthResponse>) {
    database_health(&state).await
}
