//! The Meridian HTTP server: axum application wiring, middleware stack, and
//! route handlers for the Iceberg REST catalog and management APIs.

use std::sync::Arc;
use std::time::Duration;

use axum::Router;
use axum::extract::Request;
use axum::http::{HeaderValue, StatusCode};
use axum::routing::{delete, get, post};
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

mod auth;
pub mod error;
pub mod events;
pub mod planning;
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
// One route table, one function: splitting the mounts apart would hide the
// surface the config endpoint advertises.
#[allow(clippy::too_many_lines)]
pub fn build_router(state: AppState) -> Router {
    let server = &state.config.server;
    let request_timeout = Duration::from_secs(server.request_timeout_secs);
    let max_body_bytes = server.max_body_bytes;
    // Authentication middleware state (JWKS caches, JIT-provisioning
    // cache). Constructed once; logs the loud warning when auth is
    // disabled and fails closed when OIDC setup is broken.
    let auth_state = auth::AuthState::from_app_config(&state.config, state.pool.clone());
    // Scan-planning runtime (manifest LRU, bounded async plan pool),
    // shared by the planning handlers via a request extension.
    let planning_runtime = planning::PlanningRuntime::from_config(&state.config.planning);

    // The Iceberg REST surface, mounted both at the spec path prefix
    // (/iceberg/v1) and at the bare /v1 alias many clients default to.
    // {prefix} is a warehouse name; static segments (/v1/config) win over
    // the {prefix} capture in axum's router, so "config" is not a usable
    // warehouse prefix at the /config path itself.
    let iceberg = Router::new()
        .route("/config", get(routes::iceberg::get_config))
        .route(
            "/{prefix}/namespaces",
            get(routes::namespaces::list_namespaces).post(routes::namespaces::create_namespace),
        )
        .route(
            "/{prefix}/namespaces/{namespace}",
            get(routes::namespaces::load_namespace)
                .head(routes::namespaces::namespace_exists)
                .delete(routes::namespaces::drop_namespace),
        )
        .route(
            "/{prefix}/namespaces/{namespace}/properties",
            post(routes::namespaces::update_namespace_properties),
        )
        .route(
            "/{prefix}/namespaces/{namespace}/tables",
            get(routes::tables::list_tables).post(routes::tables::create_table),
        )
        .route(
            "/{prefix}/namespaces/{namespace}/tables/{table}",
            get(routes::tables::load_table)
                .head(routes::tables::table_exists)
                .post(routes::tables::commit_table)
                .delete(routes::tables::drop_table),
        )
        .route(
            "/{prefix}/namespaces/{namespace}/register",
            post(routes::tables::register_table),
        )
        .route(
            "/{prefix}/namespaces/{namespace}/tables/{table}/metrics",
            post(routes::tables::report_metrics),
        )
        .route(
            "/{prefix}/namespaces/{namespace}/tables/{table}/credentials",
            get(routes::vending::load_credentials),
        )
        // Remote signing (ADR 005): SigV4-signs client-built S3 requests
        // after proving they stay inside this table's location prefix.
        .route(
            "/{prefix}/namespaces/{namespace}/tables/{table}/sign",
            post(routes::signing::sign_request),
        )
        // Server-side scan planning (design doc: docs/design/scan-planning.md):
        // planTableScan, fetchPlanningResult, cancelPlanning, fetchScanTasks.
        .route(
            "/{prefix}/namespaces/{namespace}/tables/{table}/plan",
            post(routes::planning::plan_table_scan),
        )
        .route(
            "/{prefix}/namespaces/{namespace}/tables/{table}/plan/{plan_id}",
            get(routes::planning::fetch_planning_result).delete(routes::planning::cancel_planning),
        )
        .route(
            "/{prefix}/namespaces/{namespace}/tables/{table}/tasks",
            post(routes::planning::fetch_scan_tasks),
        )
        .route(
            "/{prefix}/tables/rename",
            post(routes::tables::rename_table),
        )
        .route(
            "/{prefix}/namespaces/{namespace}/views",
            get(routes::views::list_views).post(routes::views::create_view),
        )
        .route(
            "/{prefix}/namespaces/{namespace}/views/{view}",
            get(routes::views::load_view)
                .head(routes::views::view_exists)
                .post(routes::views::replace_view)
                .delete(routes::views::drop_view),
        )
        .route("/{prefix}/views/rename", post(routes::views::rename_view))
        .route(
            "/{prefix}/transactions/commit",
            post(routes::tables::commit_transaction),
        )
        // Nested routers do not inherit the outer fallbacks, so the IRC
        // error envelope for wrong methods must be installed here as well.
        .method_not_allowed_fallback(method_not_allowed);

    let routes = Router::new()
        .route("/healthz", get(routes::health::healthz))
        .route("/readyz", get(routes::health::readyz))
        .nest("/iceberg/v1", iceberg.clone())
        .nest("/v1", iceberg)
        // Management API v0 (authenticated like everything else; RBAC
        // enforcement per the routes::grants module docs).
        .route(
            "/api/v2/warehouses",
            get(routes::warehouses::list_warehouses).post(routes::warehouses::create_warehouse),
        )
        .route(
            "/api/v2/warehouses/{name}",
            delete(routes::warehouses::delete_warehouse),
        )
        .route(
            "/api/v2/principals",
            get(routes::principals::list_principals),
        )
        .route(
            "/api/v2/roles",
            get(routes::grants::list_roles).post(routes::grants::create_role),
        )
        .route("/api/v2/roles/{name}", delete(routes::grants::delete_role))
        .route(
            "/api/v2/roles/{name}/bindings",
            post(routes::grants::create_role_binding),
        )
        .route(
            "/api/v2/roles/{name}/bindings/{principal_id}",
            delete(routes::grants::delete_role_binding),
        )
        .route(
            "/api/v2/grants",
            get(routes::grants::list_grants).post(routes::grants::create_grant),
        )
        .route("/api/v2/grants/{id}", delete(routes::grants::delete_grant))
        .route("/api/v2/permissions", get(routes::grants::get_permissions))
        // Asset search (Pillar A search v1): results are filtered to the
        // caller's visibility inside the query — see routes::search.
        .route("/api/v2/search", get(routes::search::search))
        // Events surface (webhooks, feed, durable consumers) — see
        // routes::events for the authorization policy.
        .route(
            "/api/v2/webhooks",
            get(routes::events::list_webhooks).post(routes::events::create_webhook),
        )
        .route(
            "/api/v2/webhooks/{id}",
            get(routes::events::get_webhook).delete(routes::events::delete_webhook),
        )
        .route(
            "/api/v2/webhooks/{id}/deliveries",
            get(routes::events::list_webhook_deliveries),
        )
        // Audit surface (log query + chain verification) — management-
        // gated; see routes::audit for the pagination contract.
        .route("/api/v2/audit", get(routes::audit::query_audit))
        .route(
            "/api/v2/audit/verify",
            get(routes::audit::verify_audit_chain),
        )
        .route("/api/v2/events", get(routes::events::list_events))
        .route(
            "/api/v2/events/consumers",
            get(routes::events::list_consumers).post(routes::events::create_consumer),
        )
        .route(
            "/api/v2/events/consumers/{name}",
            delete(routes::events::delete_consumer),
        )
        .route(
            "/api/v2/events/consumers/{name}/next",
            get(routes::events::consumer_next),
        )
        .route(
            "/api/v2/events/consumers/{name}/commit",
            post(routes::events::consumer_commit),
        )
        // Unmatched routes and wrong methods must still speak the IRC error
        // envelope — engines parse error bodies, not just status codes.
        .fallback(route_not_found)
        .method_not_allowed_fallback(method_not_allowed)
        .with_state(state)
        // Authentication wraps every route — fallbacks included, health
        // probes exempt themselves inside the middleware — as the
        // innermost layer, so token validation (including any on-demand
        // JWKS refresh) counts against the request timeout and each
        // handler sees a Principal in its request extensions.
        .layer(axum::middleware::from_fn_with_state(
            auth_state,
            auth::authenticate,
        ))
        .layer(axum::Extension(planning_runtime));

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

    // Background event workers (A-F6): the outbox relay publishes
    // committed catalog events (draining any backlog in bounded batches on
    // first boot) and the webhook dispatcher delivers them. Both are
    // crash-safe by construction (durable outbox + durable deliveries), so
    // aborting them at shutdown is fine — anything in flight is redone.
    let relay = tokio::spawn(events::run_relay(pool.clone(), config.events.clone()));
    let dispatcher = tokio::spawn(events::run_dispatcher(pool.clone(), config.events.clone()));
    // Planning sweep: expires plan rows (crash orphans included) and
    // enforces the manifest byte-cache budget. Idempotent; safe to abort.
    let plan_sweeper = tokio::spawn(planning::run_sweeper(pool.clone(), config.planning.clone()));

    let state = AppState {
        pool,
        config: Arc::new(config),
    };
    let app = build_router(state);

    tracing::info!(%local_addr, "meridian server listening");

    let served = axum::serve(listener, app)
        .with_graceful_shutdown(shutdown_signal())
        .await
        .map_err(|e| MeridianError::internal("server error", e));

    relay.abort();
    dispatcher.abort();
    plan_sweeper.abort();
    served?;

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
