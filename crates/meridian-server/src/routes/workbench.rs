//! The SQL workbench management API (Pillar L, L-F1/L-F3), mounted under
//! `/api/v2/workbench`.
//!
//! An in-console SQL editor over governed assets: small queries run on the
//! built-in `DataFusion` executor (`meridian-query`, ADR 010) with the **same
//! Pillar-D policies** the agent gateway and scan planner enforce, so a human at
//! the workbench sees exactly the rows and columns their grants permit. Big
//! queries route to a registered engine (or are refused with guidance) — no
//! BI-suite ambitions, a small-scan adoption wedge. Time-to-first-query is the
//! north star: vended credentials + the built-in executor mean zero engine
//! setup for the first taste.
//!
//! # Surfaces
//!
//! - `POST /query` — run a governed SELECT. Row/column policy applies (masks are
//!   value-preserving here — a human sees `hash(email)`, not an absent column,
//!   unlike the agent path which drops); results are size-capped and
//!   cost-estimated before execution; the run is recorded in the caller's
//!   history. Big scans are refused with a route-to-an-engine message.
//! - `GET /history` — the caller's recent queries.
//! - `GET|POST /saved`, `GET|DELETE /saved/{id}` — saved queries (L-F1).
//! - `POST /snippet` — the notebook handoff (L-F3): a one-click
//!   "open in PyIceberg/Daft/Pandas" snippet pointing at Meridian's IRC endpoint,
//!   so the client obtains **scoped, vended credentials** at connect time via the
//!   standard IRC flow (no secret is ever embedded in the snippet).
//!
//! # Governance & authorization
//!
//! Running a query and generating a snippet enforce **per-table RBAC READ** for
//! the caller inside `mcp::engine` (a table the caller cannot read fails the
//! whole query / the snippet) plus the ABAC row/column decision. Saved-query and
//! history management are per-workspace and attributed to the caller's audit
//! string; a caller sees only their own history.

use axum::Json;
use axum::extract::{Path, Query, State};
use axum::http::StatusCode;
use axum::response::IntoResponse;
use axum::{Extension, response::Response};
use meridian_common::principal::Principal;
use meridian_query::{Caps, QueryError};
use meridian_store::tenancy;
use meridian_store::workbench::{self, HistoryStatus, NewHistory, NewSavedQuery};
use serde::{Deserialize, Serialize};
use serde_json::{Value, json};

use crate::AppState;
use crate::error::ApiError;
use crate::mcp::engine::{self, MaskMode, PlanError, PlanOutcome, QueryScope};
use crate::routes::namespaces::decode_namespace_param;

/// The default cap on rows a single workbench query returns to the browser.
const WORKBENCH_RESULT_ROW_CAP: usize = 1_000;

// ---------------------------------------------------------------------------
// POST /query — run a governed SELECT
// ---------------------------------------------------------------------------

/// The workbench query request.
#[derive(Debug, Deserialize)]
pub struct QueryRequest {
    /// The SQL to run (a single read-only SELECT / CTE; enforced by the executor).
    pub sql: String,
    /// The warehouse the query targets. Optional for a table-free query.
    #[serde(default)]
    pub warehouse: Option<String>,
    /// The default namespace for resolving bare table names, as a dotted path
    /// (e.g. `sales.eu`). A qualified `ns.table` in the SQL uses `ns`.
    #[serde(default)]
    pub namespace: Option<String>,
}

/// The workbench query response.
#[derive(Debug, Serialize)]
pub struct QueryResponse {
    /// The result columns (name + type label).
    pub columns: Value,
    /// The result rows (JSON objects keyed by column).
    pub rows: Vec<Value>,
    /// Number of rows returned.
    pub row_count: usize,
    /// Whether the result was truncated by the row cap.
    pub truncated: bool,
    /// Provenance: tables + snapshot ids read and policies applied.
    pub provenance: Value,
    /// On-disk bytes the scan read.
    pub bytes_scanned: u64,
    /// Wall-clock duration in milliseconds.
    pub duration_ms: u64,
}

/// `POST /api/v2/workbench/query`: run a governed small-scan SELECT.
// One coherent flow (validate params → plan → execute → record history →
// respond); splitting it would scatter the single request's history recording
// across helpers.
#[allow(clippy::too_many_lines)]
pub async fn run_query(
    State(state): State<AppState>,
    Extension(principal): Extension<Principal>,
    Json(request): Json<QueryRequest>,
) -> Response {
    let started = std::time::Instant::now();
    let warehouse = request.warehouse.clone().unwrap_or_default();
    let default_namespace = match request.namespace.as_deref() {
        Some(ns) => match decode_namespace_param(ns) {
            Ok(levels) => Some(levels),
            Err(e) => return e.into_response(),
        },
        None => None,
    };

    let scope = QueryScope {
        warehouse: &warehouse,
        default_namespace: default_namespace.as_deref(),
    };

    // Plan → (workbench has no per-agent budget; a human's query is governed by
    // RBAC/ABAC + the small-scan cap) → execute. Masks are value-preserving here.
    let plan = engine::plan(
        &state,
        &principal,
        &request.sql,
        &scope,
        None,
        MaskMode::Preserve,
    )
    .await;

    let outcome = match plan {
        Ok(PlanOutcome::Planned(planned)) => {
            let caps = Caps {
                max_scan_bytes: meridian_query::DEFAULT_MAX_SCAN_BYTES,
                max_scan_rows: meridian_query::DEFAULT_MAX_SCAN_ROWS,
                max_result_rows: WORKBENCH_RESULT_ROW_CAP,
            };
            planned.execute(caps).await
        }
        Ok(PlanOutcome::Denied {
            table,
            reason,
            applied_policies: _,
        }) => {
            record_history(
                &state,
                &principal,
                &request,
                HistoryStatus::Denied,
                None,
                None,
                started.elapsed(),
                Some(&format!("access to {table} is denied: {reason}")),
            )
            .await;
            return ApiError::new(
                StatusCode::FORBIDDEN,
                "NotAuthorizedException",
                format!("access to {table} is denied by policy: {reason}"),
            )
            .into_response();
        }
        Err(PlanError::Executor(err)) => {
            return query_error_response(&state, &principal, &request, started, &err).await;
        }
        Err(PlanError::Resolve(api_error)) => {
            record_history(
                &state,
                &principal,
                &request,
                history_status_for(api_error.status),
                None,
                None,
                started.elapsed(),
                Some(&api_error.message),
            )
            .await;
            return (*api_error).into_response();
        }
    };

    match outcome {
        Ok((output, table_ids)) => {
            let duration = started.elapsed();
            let row_count = output.rows.len();
            record_history(
                &state,
                &principal,
                &request,
                HistoryStatus::Ok,
                Some(i64::try_from(row_count).unwrap_or(i64::MAX)),
                Some(i64::try_from(output.bytes_scanned).unwrap_or(i64::MAX)),
                duration,
                None,
            )
            .await;
            let provenance = engine::provenance_json(&output, &table_ids);
            Json(QueryResponse {
                columns: serde_json::to_value(&output.columns).unwrap_or(Value::Null),
                rows: output.rows,
                row_count,
                truncated: output.truncated,
                provenance,
                bytes_scanned: output.bytes_scanned,
                duration_ms: u64::try_from(duration.as_millis()).unwrap_or(u64::MAX),
            })
            .into_response()
        }
        Err(err) => query_error_response(&state, &principal, &request, started, &err).await,
    }
}

/// Renders a [`QueryError`] as the workbench error response and records it in
/// history. A caller-facing refusal (bad/oversized SQL) is a `400`; an
/// operational fault is a `500`.
async fn query_error_response(
    state: &AppState,
    principal: &Principal,
    request: &QueryRequest,
    started: std::time::Instant,
    err: &QueryError,
) -> Response {
    record_history(
        state,
        principal,
        request,
        HistoryStatus::Error,
        None,
        None,
        started.elapsed(),
        Some(&err.to_string()),
    )
    .await;
    let status = if err.is_caller_refusal() {
        StatusCode::BAD_REQUEST
    } else {
        StatusCode::INTERNAL_SERVER_ERROR
    };
    ApiError::new(status, "WorkbenchQueryError", err.to_string()).into_response()
}

/// Records one query run in the caller's history (best-effort: a history-write
/// failure is logged, never surfaced — the query already ran).
#[allow(clippy::too_many_arguments)]
async fn record_history(
    state: &AppState,
    principal: &Principal,
    request: &QueryRequest,
    status: HistoryStatus,
    row_count: Option<i64>,
    bytes_scanned: Option<i64>,
    duration: std::time::Duration,
    message: Option<&str>,
) {
    let entry = NewHistory {
        sql: &request.sql,
        warehouse: request.warehouse.as_deref(),
        status,
        row_count,
        bytes_scanned,
        duration_ms: Some(i64::try_from(duration.as_millis()).unwrap_or(i64::MAX)),
        message,
    };
    if let Err(error) = workbench::record_history(
        &state.pool,
        tenancy::default_workspace_id(),
        &principal.audit_string(),
        &entry,
    )
    .await
    {
        tracing::warn!(%error, "failed to record workbench query history");
    }
}

/// Maps an HTTP status to a history outcome (a 403 is a denial, else an error).
fn history_status_for(status: StatusCode) -> HistoryStatus {
    if status == StatusCode::FORBIDDEN {
        HistoryStatus::Denied
    } else {
        HistoryStatus::Error
    }
}

// ---------------------------------------------------------------------------
// GET /history — the caller's recent queries
// ---------------------------------------------------------------------------

/// Query params for history pagination.
#[derive(Debug, Deserialize)]
pub struct HistoryParams {
    /// Page size (1-200, default 50).
    #[serde(default)]
    pub limit: Option<i64>,
    /// Keyset cursor: return rows older than this history id.
    #[serde(default)]
    pub before: Option<String>,
}

/// `GET /api/v2/workbench/history`: the caller's own recent queries, newest
/// first.
pub async fn list_history(
    State(state): State<AppState>,
    Extension(principal): Extension<Principal>,
    Query(params): Query<HistoryParams>,
) -> Result<Json<Value>, ApiError> {
    let limit = params.limit.unwrap_or(50).clamp(1, 200);
    let entries = workbench::list_history(
        &state.pool,
        tenancy::default_workspace_id(),
        &principal.audit_string(),
        params.before.as_deref(),
        limit,
    )
    .await?;
    let items: Vec<Value> = entries.iter().map(history_json).collect();
    let next = entries.last().map(|e| e.id.clone());
    Ok(Json(json!({ "history": items, "next": next })))
}

fn history_json(e: &workbench::HistoryEntry) -> Value {
    json!({
        "id": e.id,
        "sql": e.sql,
        "warehouse": e.warehouse,
        "status": e.status,
        "row_count": e.row_count,
        "bytes_scanned": e.bytes_scanned,
        "duration_ms": e.duration_ms,
        "message": e.message,
        "created_at": e.created_at,
    })
}

// ---------------------------------------------------------------------------
// Saved queries
// ---------------------------------------------------------------------------

/// Create/save a query.
#[derive(Debug, Deserialize)]
pub struct SaveQueryRequest {
    /// Query name (unique per workspace, case-insensitively).
    pub name: String,
    /// The SQL.
    pub sql: String,
    /// Target warehouse, if any.
    #[serde(default)]
    pub warehouse: Option<String>,
    /// Default namespace levels for bare table names.
    #[serde(default)]
    pub namespace: Option<String>,
    /// Free-text description.
    #[serde(default)]
    pub description: Option<String>,
}

/// `POST /api/v2/workbench/saved`: save a reusable query.
pub async fn create_saved_query(
    State(state): State<AppState>,
    Extension(principal): Extension<Principal>,
    Json(request): Json<SaveQueryRequest>,
) -> Result<(StatusCode, Json<Value>), ApiError> {
    if request.name.trim().is_empty() {
        return Err(ApiError::bad_request("a saved query requires a name"));
    }
    if request.sql.trim().is_empty() {
        return Err(ApiError::bad_request("a saved query requires SQL"));
    }
    let levels = match request.namespace.as_deref() {
        Some(ns) => decode_namespace_param(ns)?,
        None => Vec::new(),
    };
    let record = workbench::create_saved_query(
        &state.pool,
        tenancy::default_workspace_id(),
        &NewSavedQuery {
            name: request.name.trim(),
            sql: &request.sql,
            warehouse: request.warehouse.as_deref(),
            default_namespace: &levels,
            description: request.description.as_deref(),
        },
        &principal.audit_string(),
    )
    .await?;
    Ok((StatusCode::CREATED, Json(saved_json(&record))))
}

/// `GET /api/v2/workbench/saved`: list the **caller's own** saved queries.
pub async fn list_saved_queries(
    State(state): State<AppState>,
    Extension(principal): Extension<Principal>,
) -> Result<Json<Value>, ApiError> {
    let records = workbench::list_saved_queries(
        &state.pool,
        tenancy::default_workspace_id(),
        &principal.audit_string(),
    )
    .await?;
    let items: Vec<Value> = records.iter().map(saved_json).collect();
    Ok(Json(json!({ "saved_queries": items })))
}

/// `GET /api/v2/workbench/saved/{id}`: load one of the caller's own saved
/// queries (another principal's query reads as 404, not their SQL).
pub async fn get_saved_query(
    State(state): State<AppState>,
    Extension(principal): Extension<Principal>,
    Path(id): Path<String>,
) -> Result<Json<Value>, ApiError> {
    let record = workbench::get_saved_query(
        &state.pool,
        tenancy::default_workspace_id(),
        &id,
        &principal.audit_string(),
    )
    .await?
    .ok_or_else(|| {
        ApiError::new(
            StatusCode::NOT_FOUND,
            "NotFound",
            format!("no saved query {id}"),
        )
    })?;
    Ok(Json(saved_json(&record)))
}

/// `DELETE /api/v2/workbench/saved/{id}`: delete a saved query.
pub async fn delete_saved_query(
    State(state): State<AppState>,
    Extension(principal): Extension<Principal>,
    Path(id): Path<String>,
) -> Result<StatusCode, ApiError> {
    let deleted = workbench::delete_saved_query(
        &state.pool,
        tenancy::default_workspace_id(),
        &id,
        &principal.audit_string(),
    )
    .await?;
    if deleted {
        Ok(StatusCode::NO_CONTENT)
    } else {
        Err(ApiError::new(
            StatusCode::NOT_FOUND,
            "NotFound",
            format!("no saved query {id}"),
        ))
    }
}

fn saved_json(r: &workbench::SavedQuery) -> Value {
    json!({
        "id": r.id,
        "name": r.name,
        "sql": r.sql,
        "warehouse": r.warehouse,
        "namespace": r.default_namespace.0,
        "description": r.description,
        "owner": r.owner,
        "created_at": r.created_at,
        "updated_at": r.updated_at,
    })
}

// ---------------------------------------------------------------------------
// POST /snippet — notebook handoff (L-F3)
// ---------------------------------------------------------------------------

/// A snippet request: which table, for which client.
#[derive(Debug, Deserialize)]
pub struct SnippetRequest {
    /// The warehouse (catalog prefix).
    pub warehouse: String,
    /// The dotted namespace path.
    pub namespace: String,
    /// The table name.
    pub table: String,
}

/// `POST /api/v2/workbench/snippet`: generate "open in PyIceberg/Daft/Pandas"
/// snippets for a governed table (L-F3).
///
/// RBAC READ on the table is required (a caller only gets a snippet for a table
/// they can read). The snippets point at Meridian's IRC endpoint and the table
/// identifier; the client obtains **scoped, vended credentials** at connect time
/// via the standard IRC credential flow — no secret is embedded here.
pub async fn generate_snippet(
    State(state): State<AppState>,
    Extension(principal): Extension<Principal>,
    Json(request): Json<SnippetRequest>,
) -> Result<Json<Value>, ApiError> {
    use crate::routes::grants::{namespace_scope_chain, require};
    use crate::routes::namespaces::resolve_warehouse;
    use meridian_store::rbac::{Privilege, SecurableScope};
    use meridian_store::table;

    let levels = decode_namespace_param(&request.namespace)?;
    let wh = resolve_warehouse(&state.pool, &request.warehouse).await?;
    let record = table::get_by_name(&state.pool, &wh.id, &levels, &request.table)
        .await?
        .ok_or_else(|| {
            ApiError::new(
                StatusCode::NOT_FOUND,
                "NoSuchTableException",
                format!(
                    "table {}/{}/{} does not exist",
                    request.warehouse, request.namespace, request.table
                ),
            )
        })?;
    let chain = namespace_scope_chain(&state.pool, &wh.id, &levels).await?;
    // RBAC READ: only for a table the caller can read.
    require(
        &state.pool,
        &principal,
        Privilege::Read,
        &SecurableScope::table(&wh.id, chain, Some(&record.id)),
    )
    .await?;

    // The IRC endpoint the client points at. Meridian does not know its own
    // external URL (it sits behind the operator's ingress), so the snippet uses
    // a documented placeholder host the user replaces — we never fabricate a
    // real hostname (honest-docs rule).
    let uri = "https://YOUR-MERIDIAN-HOST/iceberg".to_owned();
    let full_name = format!("{}.{}", levels.join("."), request.table);

    let pyiceberg = format!(
        "from pyiceberg.catalog.rest import RestCatalog\n\n\
         catalog = RestCatalog(\n\
         \x20   \"meridian\",\n\
         \x20   uri=\"{uri}\",\n\
         \x20   warehouse=\"{warehouse}\",\n\
         \x20   token=\"<YOUR_OIDC_TOKEN>\",  # Meridian validates this and vends scoped storage creds\n\
         )\n\
         table = catalog.load_table(\"{full_name}\")\n\
         df = table.scan().to_arrow()  # governed: only rows/columns your grants permit\n",
        warehouse = request.warehouse,
    );
    let daft = format!(
        "import daft\nfrom daft.io import IcebergCatalog\n\n\
         catalog = IcebergCatalog.from_rest(\n\
         \x20   uri=\"{uri}\",\n\
         \x20   warehouse=\"{warehouse}\",\n\
         \x20   token=\"<YOUR_OIDC_TOKEN>\",\n\
         )\n\
         df = daft.read_iceberg(catalog.load_table(\"{full_name}\"))\n",
        warehouse = request.warehouse,
    );
    let pandas = format!(
        "# Pandas via PyIceberg (small scans; for large tables prefer Daft/Spark)\n\
         from pyiceberg.catalog.rest import RestCatalog\n\n\
         catalog = RestCatalog(\"meridian\", uri=\"{uri}\", warehouse=\"{warehouse}\", token=\"<YOUR_OIDC_TOKEN>\")\n\
         pdf = catalog.load_table(\"{full_name}\").scan().to_pandas()\n",
        warehouse = request.warehouse,
    );

    Ok(Json(json!({
        "table": full_name,
        "catalog_uri": uri,
        "warehouse": request.warehouse,
        "note": "The snippet authenticates with your OIDC token; Meridian validates it and vends \
                 short-lived, table-scoped storage credentials at connect time — no secret is \
                 embedded here. Replace <YOUR_OIDC_TOKEN> with a token from your IdP.",
        "snippets": {
            "pyiceberg": pyiceberg,
            "daft": daft,
            "pandas": pandas,
        }
    })))
}
