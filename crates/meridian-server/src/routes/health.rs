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

/// `GET /healthz` — **liveness**: the process is up and serving requests.
///
/// Always `200` while the server can answer. It reports database reachability
/// in the body (so the happy path is `{"status":"ok","checks":{"database":"ok"}}`
/// and a human can still see a DB problem), but it deliberately does **not**
/// gate the status code on the database: a Postgres outage is not fixed by an
/// orchestrator killing and restarting the pod — that just crashloops every
/// replica during a database blip. Shedding traffic during a dependency outage
/// is **readiness's** job (`/readyz`), not liveness's.
pub async fn healthz(State(state): State<AppState>) -> (StatusCode, Json<HealthResponse>) {
    let database = if meridian_store::health_check(&state.pool).await.is_ok() {
        "ok"
    } else {
        "error"
    };
    (
        StatusCode::OK,
        Json(HealthResponse {
            status: "ok".to_owned(),
            checks: HealthChecks {
                database: database.to_owned(),
            },
        }),
    )
}

/// `GET /readyz` — **readiness**: ready to accept traffic. Returns `503` when
/// Postgres is unreachable so the orchestrator removes this replica from the
/// load balancer until the dependency recovers (without restarting it).
pub async fn readyz(State(state): State<AppState>) -> (StatusCode, Json<HealthResponse>) {
    database_health(&state).await
}
