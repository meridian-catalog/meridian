//! The Meridian HTTP server: axum application wiring, middleware stack, and
//! route handlers for the Iceberg REST catalog and management APIs.

use std::sync::Arc;
use std::time::Duration;

use axum::Router;
use axum::extract::Request;
use axum::http::{HeaderName, HeaderValue, StatusCode};
use axum::routing::{delete, get, post};
use meridian_common::{AppConfig, MeridianError, Result};
use sqlx::PgPool;
use tokio::net::TcpListener;
use tower::ServiceBuilder;
use tower_http::cors::{AllowOrigin, CorsLayer};
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
pub mod governance;
pub mod maintenance;
pub mod planning;
pub mod quality_monitor;
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
    let cors_origins = server.cors_allowed_origins.clone();
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
        // Autonomous maintenance (Pillar C): policies CRUD, per-table health,
        // the job queue, the savings ledger, and fleet health. Authorization
        // per the routes::maintenance module docs (MANAGE_NAMESPACE for
        // mutations, READ / management for reads).
        .route(
            "/api/v2/maintenance/policies",
            get(routes::maintenance::list_policies)
                .post(routes::maintenance::create_policy)
                .put(routes::maintenance::update_policy)
                .delete(routes::maintenance::delete_policy),
        )
        .route(
            "/api/v2/maintenance/jobs",
            get(routes::maintenance::list_jobs).post(routes::maintenance::trigger_job),
        )
        .route(
            "/api/v2/maintenance/jobs/{id}",
            get(routes::maintenance::get_job),
        )
        .route(
            "/api/v2/maintenance/jobs/{id}/cancel",
            post(routes::maintenance::cancel_job),
        )
        .route(
            "/api/v2/maintenance/savings",
            get(routes::maintenance::list_savings),
        )
        .route(
            "/api/v2/maintenance/savings/rollup",
            get(routes::maintenance::savings_rollup),
        )
        .route(
            "/api/v2/warehouses/{name}/health-summary",
            get(routes::maintenance::warehouse_health_summary),
        )
        .route(
            "/api/v2/warehouses/{warehouse}/namespaces/{namespace}/tables/{table}/health",
            get(routes::maintenance::get_table_health),
        )
        .route(
            "/api/v2/warehouses/{warehouse}/namespaces/{namespace}/tables/{table}/health/history",
            get(routes::maintenance::get_table_health_history),
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
        // Catalog federation (Pillar B): mirror CRUD, per-mirror sync status
        // + sync-now, and the cross-catalog sprawl summary. Every route is
        // management-gated (see routes::federation). The federation sync
        // worker (concurrent crate) writes the mirror_assets + sync-run state
        // these read.
        .route(
            "/api/v2/mirrors",
            get(routes::federation::list_mirrors).post(routes::federation::create_mirror),
        )
        .route(
            "/api/v2/mirrors/{name}",
            get(routes::federation::get_mirror)
                .patch(routes::federation::update_mirror)
                .delete(routes::federation::delete_mirror),
        )
        .route(
            "/api/v2/mirrors/{name}/sync",
            get(routes::federation::get_sync_status).post(routes::federation::sync_now),
        )
        .route(
            "/api/v2/federation/sprawl",
            get(routes::federation::get_sprawl),
        )
        // Cross-engine access governance (Pillar D): tags + assignments +
        // coverage (D-F3), versioned policies + bindings + dry-run (D-F1), the
        // effective-policy / who-can-see-what / drift / evidence analytics
        // (D-F5). Every route is management-gated (see routes::governance).
        // The enforcement these configure is applied in the scan-plan path
        // (crate::governance + routes::planning, D-F2.1).
        .route(
            "/api/v2/governance/tags",
            get(routes::governance::list_tags).post(routes::governance::create_tag),
        )
        .route(
            "/api/v2/governance/tags/{id}",
            delete(routes::governance::delete_tag),
        )
        .route(
            "/api/v2/governance/tags/coverage",
            get(routes::governance::classification_coverage),
        )
        .route(
            "/api/v2/governance/tags/assignments",
            post(routes::governance::assign_tag),
        )
        .route(
            "/api/v2/governance/tags/assignments/{id}",
            delete(routes::governance::unassign_tag),
        )
        .route(
            "/api/v2/governance/tags/assignments/{id}/approve",
            post(routes::governance::approve_assignment),
        )
        .route(
            "/api/v2/governance/policies",
            get(routes::governance::list_policies).post(routes::governance::create_policy),
        )
        .route(
            "/api/v2/governance/policies/dry-run",
            post(routes::governance::dry_run_policy),
        )
        .route(
            "/api/v2/governance/policies/bindings/{binding_id}",
            delete(routes::governance::unbind_policy),
        )
        .route(
            "/api/v2/governance/policies/{id}",
            get(routes::governance::get_policy)
                .patch(routes::governance::update_policy)
                .delete(routes::governance::delete_policy),
        )
        .route(
            "/api/v2/governance/policies/{id}/versions",
            get(routes::governance::list_policy_versions),
        )
        .route(
            "/api/v2/governance/policies/{id}/rollback",
            post(routes::governance::rollback_policy),
        )
        .route(
            "/api/v2/governance/policies/{id}/bindings",
            get(routes::governance::list_bindings).post(routes::governance::bind_policy),
        )
        .route(
            "/api/v2/governance/effective-policy",
            get(routes::governance::effective_policy),
        )
        .route(
            "/api/v2/governance/who-can-see",
            get(routes::governance::who_can_see),
        )
        .route("/api/v2/governance/drift", get(routes::governance::drift))
        .route(
            "/api/v2/governance/evidence",
            get(routes::governance::evidence),
        )
        // Lineage & impact (Pillar F): the up/downstream graph (F-F5), the
        // impact/blast-radius query (F-F5), and the OpenLineage sink (F-F2).
        // All management-gated (see routes::lineage). Commit-native edges
        // (F-F1) are produced by the post-commit lineage worker spawned in
        // `serve`, off the sacred commit path.
        .route("/api/v2/lineage", get(routes::lineage::get_lineage))
        .route("/api/v2/lineage/impact", get(routes::lineage::get_impact))
        .route(
            "/api/v2/lineage/openlineage",
            post(routes::lineage::ingest_openlineage),
        )
        // Data contracts & the circuit breaker (Pillar E, E-F3/E-F4): versioned
        // contract objects, per-table status, the violation ledger, and
        // quarantine publish/discard. All management-gated (see routes::quality).
        // The circuit breaker itself runs as the pre-commit hook in the commit
        // driver (routes::tables); these endpoints are its control plane.
        .route(
            "/api/v2/quality/contracts",
            get(routes::quality::list_contracts).post(routes::quality::create_contract),
        )
        .route(
            "/api/v2/quality/contracts/{id}",
            get(routes::quality::get_contract)
                .patch(routes::quality::update_contract)
                .delete(routes::quality::delete_contract),
        )
        .route(
            "/api/v2/quality/contracts/{id}/versions",
            get(routes::quality::list_contract_versions),
        )
        .route(
            "/api/v2/quality/tables/{warehouse}/{namespace}/{table}/contracts",
            get(routes::quality::table_contracts),
        )
        .route(
            "/api/v2/quality/violations",
            get(routes::quality::list_violations),
        )
        .route(
            "/api/v2/quality/tables/{warehouse}/{namespace}/{table}/quarantine/{snapshot}/publish",
            post(routes::quality::publish_quarantine),
        )
        .route(
            "/api/v2/quality/tables/{warehouse}/{namespace}/{table}/quarantine/{snapshot}/discard",
            post(routes::quality::discard_quarantine),
        )
        // Zero-scan data-quality monitors (E-F1), incidents (E-F5), per-table
        // status + quality score (E-F5/E-F6). Monitors are evaluated off the
        // sacred commit path by the quality-monitor worker spawned in `serve`;
        // these endpoints are the control + read plane. All management-gated.
        .route(
            "/api/v2/quality/monitors",
            get(routes::quality::list_monitors).post(routes::quality::create_monitor),
        )
        // `results` is a static segment, so it must be registered before the
        // `{id}` capture (axum matches statics first, but keeping them adjacent
        // documents the intent).
        .route(
            "/api/v2/quality/monitors/results",
            get(routes::quality::list_monitor_results),
        )
        .route(
            "/api/v2/quality/monitors/{id}",
            get(routes::quality::get_monitor)
                .patch(routes::quality::update_monitor)
                .delete(routes::quality::delete_monitor),
        )
        .route(
            "/api/v2/quality/incidents",
            get(routes::quality::list_incidents),
        )
        .route(
            "/api/v2/quality/incidents/{id}",
            get(routes::quality::get_incident),
        )
        .route(
            "/api/v2/quality/incidents/{id}/ack",
            post(routes::quality::acknowledge_incident),
        )
        .route(
            "/api/v2/quality/incidents/{id}/resolve",
            post(routes::quality::resolve_incident),
        )
        .route(
            "/api/v2/quality/tables/{warehouse}/{namespace}/{table}/status",
            get(routes::quality::table_status),
        )
        .route(
            "/api/v2/quality/tables/{warehouse}/{namespace}/{table}/status/history",
            get(routes::quality::table_status_history),
        )
        .route(
            "/api/v2/quality/tables/{warehouse}/{namespace}/{table}/score",
            get(routes::quality::table_quality_score),
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

    routes
        .layer(
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
        // CORS is the outermost layer so a browser preflight (OPTIONS) is
        // answered by the CORS layer itself — before auth, the body limit,
        // or route matching (which would otherwise 405 the OPTIONS). Only
        // browsers send `Origin`; engines are unaffected.
        .layer(cors_layer(&cors_origins))
}

/// Builds the CORS layer from the configured browser origins.
///
/// Empty list → no CORS headers (browsers blocked, engines unaffected).
/// `["*"]` → any origin, but without credentials (the CORS spec forbids
/// `Allow-Credentials: true` alongside a wildcard). Otherwise an explicit
/// allow-list that permits credentialed requests (bearer tokens).
fn cors_layer(origins: &[String]) -> CorsLayer {
    use axum::http::Method;
    use axum::http::header::{AUTHORIZATION, CONTENT_TYPE};

    let base = CorsLayer::new()
        .allow_methods([
            Method::GET,
            Method::POST,
            Method::PUT,
            Method::DELETE,
            Method::HEAD,
            Method::OPTIONS,
        ])
        .allow_headers([
            AUTHORIZATION,
            CONTENT_TYPE,
            // Iceberg clients set these; harmless for the console.
            HeaderName::from_static("x-iceberg-access-delegation"),
            HeaderName::from_static("idempotency-key"),
        ])
        .expose_headers([HeaderName::from_static("etag")]);

    if origins.iter().any(|o| o == "*") {
        base.allow_origin(AllowOrigin::any())
    } else {
        let parsed: Vec<HeaderValue> = origins
            .iter()
            .filter_map(|o| o.parse::<HeaderValue>().ok())
            .collect();
        base.allow_origin(parsed).allow_credentials(true)
    }
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

    // Autonomous table maintenance (Pillar C): the job worker drains the
    // maintenance queue (running compaction/expiry as normal audited
    // commits), and the reconciler enqueues policy-violating tables. Both are
    // crash-safe by construction (the queue and jobs are durable), so
    // aborting them at shutdown is fine — an in-flight job re-queues.
    let maintenance_tasks = if config.maintenance.enabled {
        let worker = tokio::spawn(maintenance::run_worker(
            pool.clone(),
            config.maintenance.clone(),
        ));
        let reconciler = tokio::spawn(maintenance::run_reconciler(
            pool.clone(),
            config.maintenance.clone(),
        ));
        Some((worker, reconciler))
    } else {
        tracing::info!("autonomous maintenance disabled by configuration");
        None
    };

    // Catalog federation (Pillar B, B-F1): the inbound-mirror sync worker pulls
    // due mirrors and materializes their tables as foreign (read-only) assets.
    // Crash-safe: each run is recorded on the mirror and a `running` flag is
    // reclaimed on the next pass, so aborting at shutdown is fine.
    let federation_worker = if config.federation.enabled {
        Some(tokio::spawn(meridian_federation::run_worker(
            pool.clone(),
            config.federation.clone(),
        )))
    } else {
        tracing::info!("catalog federation sync worker disabled by configuration");
        None
    };

    // Lineage (Pillar F, F-F1): the post-commit lineage worker consumes the
    // durable `table.committed` event stream *after* the commit — never on the
    // sacred commit path — and records commit-native edges from each new
    // snapshot's summary. Crash-safe by construction (a durable consumer
    // cursor + idempotent edge upserts), so aborting at shutdown is fine.
    let lineage_worker = if config.lineage.enabled {
        Some(tokio::spawn(meridian_lineage::run_worker(
            pool.clone(),
            meridian_store::tenancy::default_workspace_id(),
            Duration::from_secs(config.lineage.poll_interval_secs),
        )))
    } else {
        tracing::info!("lineage post-commit worker disabled by configuration");
        None
    };

    // Data quality (Pillar E, E-F1/E-F5): the post-commit monitor evaluation
    // worker consumes the same durable `table.committed` stream *after* the
    // commit — never on the sacred commit path — computes zero-scan monitor
    // results per table (from the snapshot index, no data scan), and opens
    // incidents on breaches with a lineage-derived blast radius. Crash-safe by
    // construction (a durable consumer cursor + incident de-duplication), so
    // aborting at shutdown is fine.
    let quality_worker = if config.quality.enabled {
        Some(tokio::spawn(quality_monitor::run_worker(
            pool.clone(),
            meridian_store::tenancy::default_workspace_id(),
            config.quality.clone(),
        )))
    } else {
        tracing::info!("quality monitor evaluation worker disabled by configuration");
        None
    };

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
    if let Some((worker, reconciler)) = maintenance_tasks {
        worker.abort();
        reconciler.abort();
    }
    if let Some(federation_worker) = federation_worker {
        federation_worker.abort();
    }
    if let Some(lineage_worker) = lineage_worker {
        lineage_worker.abort();
    }
    if let Some(quality_worker) = quality_worker {
        quality_worker.abort();
    }
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
