//! The Meridian HTTP server: axum application wiring, middleware stack, and
//! route handlers for the Iceberg REST catalog and management APIs.

use std::sync::Arc;
use std::time::Duration;

use axum::Router;
use axum::extract::Request;
use axum::http::{HeaderValue, StatusCode};
use axum::routing::get;
use meridian_common::{AppConfig, MeridianError, Result};
use sqlx::PgPool;
use tokio::net::TcpListener;
use tower::ServiceBuilder;
use tower_http::limit::RequestBodyLimitLayer;
use tower_http::request_id::{
    MakeRequestId, PropagateRequestIdLayer, RequestId, SetRequestIdLayer,
};
use tower_http::timeout::TimeoutLayer;
use tower_http::trace::TraceLayer;
use ulid::Ulid;

pub mod routes;

/// Shared state available to all handlers.
#[derive(Debug, Clone)]
pub struct AppState {
    /// Postgres connection pool.
    pub pool: PgPool,
    /// Application configuration.
    pub config: Arc<AppConfig>,
}

/// Generates ULID request IDs (time-ordered, matching Meridian's ID scheme).
#[derive(Debug, Clone, Copy, Default)]
struct MakeUlidRequestId;

impl MakeRequestId for MakeUlidRequestId {
    fn make_request_id<B>(&mut self, _request: &Request<B>) -> Option<RequestId> {
        HeaderValue::from_str(&Ulid::new().to_string())
            .ok()
            .map(RequestId::new)
    }
}

/// Builds the complete application router with the middleware stack applied.
pub fn build_router(state: AppState) -> Router {
    let server = &state.config.server;
    let request_timeout = Duration::from_secs(server.request_timeout_secs);
    let max_body_bytes = server.max_body_bytes;

    let routes = Router::new()
        .route("/healthz", get(routes::health::healthz))
        .route("/readyz", get(routes::health::readyz))
        // The Iceberg REST config endpoint, both at the spec path prefix and
        // at the bare /v1 alias many clients default to.
        .route("/iceberg/v1/config", get(routes::iceberg::get_config))
        .route("/v1/config", get(routes::iceberg::get_config))
        // Unmatched routes and wrong methods must still speak the IRC error
        // envelope — engines parse error bodies, not just status codes.
        .fallback(route_not_found)
        .method_not_allowed_fallback(method_not_allowed)
        .with_state(state);

    routes.layer(
        ServiceBuilder::new()
            .layer(SetRequestIdLayer::x_request_id(MakeUlidRequestId))
            .layer(
                TraceLayer::new_for_http().make_span_with(|request: &Request| {
                    let request_id = request
                        .headers()
                        .get("x-request-id")
                        .and_then(|v| v.to_str().ok())
                        .unwrap_or("unknown");
                    tracing::info_span!(
                        "http_request",
                        method = %request.method(),
                        uri = %request.uri(),
                        request_id,
                    )
                }),
            )
            .layer(PropagateRequestIdLayer::x_request_id())
            // Body limit sits outside the timeout so the timeout is the
            // innermost wrapper around routes (its synthesized timeout
            // response needs the plain axum body type).
            .layer(RequestBodyLimitLayer::new(max_body_bytes))
            .layer(TimeoutLayer::with_status_code(
                StatusCode::REQUEST_TIMEOUT,
                request_timeout,
            )),
    )
}

/// Binds the configured address and serves until SIGTERM/ctrl-c.
/// Fallback for unmatched paths: a 404 rendered as the IRC error envelope.
async fn route_not_found(request: Request) -> MeridianError {
    MeridianError::NotFound(format!("no route for {}", request.uri().path()))
}

/// Fallback for known paths hit with an unsupported method: a 405 envelope.
async fn method_not_allowed() -> MeridianError {
    MeridianError::MethodNotAllowed("method not allowed for this route".to_owned())
}

pub async fn serve(config: AppConfig, pool: PgPool) -> Result<()> {
    let addr = config.bind_addr();
    let listener = TcpListener::bind(&addr)
        .await
        .map_err(|e| MeridianError::internal(format!("failed to bind {addr}"), e))?;
    let local_addr = listener
        .local_addr()
        .map_err(|e| MeridianError::internal("failed to read bound address", e))?;

    let state = AppState {
        pool,
        config: Arc::new(config),
    };
    let app = build_router(state);

    tracing::info!(%local_addr, "meridian server listening");

    axum::serve(listener, app)
        .with_graceful_shutdown(shutdown_signal())
        .await
        .map_err(|e| MeridianError::internal("server error", e))?;

    tracing::info!("meridian server shut down cleanly");
    Ok(())
}

/// Resolves when the process receives ctrl-c or (on unix) SIGTERM.
async fn shutdown_signal() {
    let ctrl_c = async {
        if let Err(error) = tokio::signal::ctrl_c().await {
            // If signal handlers cannot be installed we log and serve on;
            // the alternative (panicking) would take the server down for a
            // shutdown-ergonomics feature.
            tracing::error!(%error, "failed to install ctrl-c handler");
            std::future::pending::<()>().await;
        }
    };

    #[cfg(unix)]
    let terminate = async {
        match tokio::signal::unix::signal(tokio::signal::unix::SignalKind::terminate()) {
            Ok(mut signal) => {
                signal.recv().await;
            }
            Err(error) => {
                tracing::error!(%error, "failed to install SIGTERM handler");
                std::future::pending::<()>().await;
            }
        }
    };

    #[cfg(not(unix))]
    let terminate = std::future::pending::<()>();

    tokio::select! {
        () = ctrl_c => tracing::info!("received ctrl-c, shutting down"),
        () = terminate => tracing::info!("received SIGTERM, shutting down"),
    }
}
