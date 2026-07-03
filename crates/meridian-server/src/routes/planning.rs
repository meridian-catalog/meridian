//! Scan-planning endpoints (IRC 1.11+):
//!
//! - `POST   .../tables/{table}/plan` — planTableScan
//! - `GET    .../tables/{table}/plan/{plan-id}` — fetchPlanningResult
//! - `DELETE .../tables/{table}/plan/{plan-id}` — cancelPlanning
//! - `POST   .../tables/{table}/tasks` — fetchScanTasks
//!
//! Authorization: `READ` on the table for every endpoint — the same rule
//! as loadTable, re-checked on every fetch (a revoked grant immediately
//! cuts off plan results; possession of a plan-id or plan-task token is
//! never authorization by itself). Plans are not bound to the submitting
//! principal: any principal with `READ` on the table may fetch or cancel,
//! which is equivalent power to planning the same scan themselves.
//!
//! Sync-vs-async: tables whose snapshot tracks at most
//! `planning.sync_max_data_files` live data files (counted from the
//! manifest list) are planned inline and answered `completed` with every
//! file scan task in the response; larger tables are answered
//! `submitted` and planned on a bounded worker pool. Both paths persist
//! the plan (the spec's completed planTableScan response carries a
//! required plan-id, and cancelPlanning must resolve it).
//!
//! Divergences and gaps are documented in `docs/api-status.md` (scan
//! planning section): incremental scans are 406, `min-rows-requested` is
//! accepted and ignored, `select`/`stats-fields` are validated but only
//! `stats-fields` changes the payload, and plan results carry no
//! `storage-credentials` (clients use loadTable delegation).

use std::collections::BTreeSet;
use std::sync::Arc;

use axum::extract::{Path, State};
use axum::http::StatusCode;
use axum::response::{IntoResponse, Response};
use axum::{Extension, Json};
use meridian_common::principal::Principal;
use meridian_iceberg::expr::{BoundPredicate, Expression};
use meridian_iceberg::manifest::ManifestContentType;
use meridian_iceberg::spec::{Schema, Snapshot, TableMetadata};
use meridian_storage::{Storage, read_table_metadata};
use meridian_store::planning as plan_store;
use meridian_store::rbac::{Privilege, SecurableScope};
use meridian_store::table::TableRecord;
use meridian_store::{tenancy, warehouse::WarehouseRecord};
use serde_json::{Value, json};
use sqlx::PgPool;
use ulid::Ulid;

use crate::AppState;
use crate::error::ApiError;
use crate::governance::{self, ScanPolicy, TableContext};
use crate::planning::engine::{
    self, ManifestSource, PlanError, PlanOutcome, SerializeContext, plan_scan,
};
use crate::planning::rest::{FetchScanTasksRequest, PlanTableScanRequest};
use crate::planning::{ManifestIo, PlanningRuntime};
use crate::routes::grants::{forbidden, namespace_scope_chain, require};
use crate::routes::tables::{connect_storage, no_such_table, resolve_table};

/// 406 `UnsupportedOperationException` (the spec's response for
/// operations the server does not support).
fn unsupported(message: impl Into<String>) -> ApiError {
    ApiError::new(
        StatusCode::NOT_ACCEPTABLE,
        "UnsupportedOperationException",
        message,
    )
}

/// 404 `NoSuchPlanIdException`.
fn no_such_plan(plan_id: &str) -> ApiError {
    ApiError::new(
        StatusCode::NOT_FOUND,
        "NoSuchPlanIdException",
        format!("plan {plan_id:?} does not exist"),
    )
}

/// 404 `NoSuchPlanTaskException`.
fn no_such_plan_task() -> ApiError {
    ApiError::new(
        StatusCode::NOT_FOUND,
        "NoSuchPlanTaskException",
        "the plan task does not exist",
    )
}

/// Resolves table + RBAC (`READ`, like loadTable) for every planning
/// endpoint.
async fn authorize_read(
    state: &AppState,
    principal: &Principal,
    prefix: &str,
    raw_namespace: &str,
    name: &str,
) -> Result<(WarehouseRecord, Vec<String>, TableRecord), ApiError> {
    let (warehouse, levels, record) = resolve_table(state, prefix, raw_namespace, name).await?;
    let chain = namespace_scope_chain(&state.pool, &warehouse.id, &levels).await?;
    require(
        &state.pool,
        principal,
        Privilege::Read,
        &SecurableScope::table(&warehouse.id, chain, Some(&record.id)),
    )
    .await?;
    Ok((warehouse, levels, record))
}

/// The Meridian purpose-declaration header (purpose-based access, D-F1). A
/// scan client declares its purpose here (e.g. `fraud_investigation`); a
/// `pii:high` deny-unless-purpose policy consults it. Namespaced as a Meridian
/// extension — the IRC plan request carries no purpose field.
const PURPOSE_HEADER: &str = "x-meridian-purpose";

/// Extracts the declared purpose from the request headers, if present and
/// non-empty.
fn purpose_from_headers(headers: &axum::http::HeaderMap) -> Option<String> {
    headers
        .get(PURPOSE_HEADER)
        .and_then(|v| v.to_str().ok())
        .map(str::trim)
        .filter(|s| !s.is_empty())
        .map(str::to_owned)
}

/// Resolves the ABAC decision for this plan and attaches the enforcement to
/// `scan`. A full deny aborts with 403 (audited); an effective allow-with-
/// restrictions is audited and its row filter + masked-column field ids are
/// attached. A no-op decision (nothing applies) leaves `scan` untouched and
/// writes no audit row (a plan that changes nothing needs no governance
/// record — plan creation is audited separately by the store).
async fn apply_enforcement(
    state: &AppState,
    principal: &Principal,
    record: &TableRecord,
    namespace_ids: &[String],
    schema: &Schema,
    purpose: Option<&str>,
    scan: &mut ResolvedScan,
) -> Result<(), ApiError> {
    let table_ctx = TableContext {
        table_id: &record.id,
        namespace_ids,
        schema,
        owner: None,
    };
    let policy: ScanPolicy =
        governance::resolve_scan_policy(&state.pool, principal, &table_ctx, purpose).await?;

    if !policy.is_effective() {
        return Ok(());
    }

    // Audit every governance decision that changes the result (deny, filter,
    // or mask) — the audit trail is the product (D-F2). Its own transaction:
    // the decision is recorded whether or not the plan later persists.
    audit_enforcement(state, principal, record, &policy).await?;

    if policy.denied {
        return Err(forbidden(format!(
            "access denied by policy: {}",
            policy.reason
        )));
    }

    // Map masked column *names* to scan-schema field ids; a mask on a column
    // absent from the scan schema (dropped/renamed) is simply inert.
    let mut strip_fields = BTreeSet::new();
    for col in &policy.removed_columns {
        if let Some(id) = engine::resolve_field_name(schema, col, true) {
            strip_fields.insert(id);
        }
    }

    scan.enforcement = ScanEnforcement {
        row_policy: policy.row_filter,
        strip_fields,
    };
    Ok(())
}

/// Writes one governance-decision audit row (append-only chain), recording the
/// principal, table, applied policies, removed columns, and reason.
async fn audit_enforcement(
    state: &AppState,
    principal: &Principal,
    record: &TableRecord,
    policy: &ScanPolicy,
) -> Result<(), ApiError> {
    meridian_store::audit::append(
        &state.pool,
        meridian_store::audit::NewAuditEntry {
            workspace_id: Some(tenancy::default_workspace_id()),
            principal: principal.audit_string(),
            action: "governance.scan.enforced".to_owned(),
            resource: format!("table:{}", record.id),
            details: policy.audit_details(&record.id),
        },
    )
    .await?;
    Ok(())
}

/// Parses an optional JSON request body with a real 400 on malformed
/// JSON (an absent/empty body is a valid default request per the spec).
fn parse_plan_request(body: &axum::body::Bytes) -> Result<PlanTableScanRequest, ApiError> {
    if body.is_empty() {
        return Ok(PlanTableScanRequest::default());
    }
    serde_json::from_slice(body)
        .map_err(|e| ApiError::bad_request(format!("invalid planTableScan request body: {e}")))
}

fn plan_error_to_api(error: &PlanError) -> ApiError {
    // Unreadable manifests are catalog-side corruption or storage
    // trouble, not client mistakes; surface them loudly.
    ApiError::new(
        StatusCode::INTERNAL_SERVER_ERROR,
        "InternalServerError",
        error.to_string(),
    )
}

/// The resolved inputs of a plan, shared by the sync and async paths.
struct ResolvedScan {
    metadata: TableMetadata,
    schema: Schema,
    snapshot_id: i64,
    manifest_list_location: String,
    bound: Option<BoundPredicate>,
    stats_keep: Option<BTreeSet<i32>>,
    /// The resolved governance enforcement for this `(principal, table)`
    /// (D-F2.1): a row-filter expression to inject and the field ids of masked
    /// columns to strip. Attached by the handler after RBAC + ABAC resolution;
    /// [`ScanEnforcement::none`] when no policy applies (or the recompute path,
    /// where the stored plan already reflects the caller — see below).
    enforcement: ScanEnforcement,
}

/// The governance enforcement to fold into a plan's serialized tasks: the
/// row-filter predicate and the masked-column field ids to strip. Resolved
/// once per plan by [`crate::governance`] and carried on [`ResolvedScan`] so
/// the sync, async, and recompute serialization paths all apply it uniformly.
#[derive(Debug, Clone, Default)]
struct ScanEnforcement {
    /// Row-filter predicate to AND into every task residual, or `None`.
    row_policy: Option<Expression>,
    /// Field ids of masked columns to strip from returned data-file stats.
    strip_fields: BTreeSet<i32>,
}

impl ScanEnforcement {
    /// No enforcement (nothing to inject or strip).
    fn none() -> Self {
        Self::default()
    }
}

/// Rejects request shapes the planner does not (yet) support.
fn validate_scan_mode(request: &PlanTableScanRequest) -> Result<(), ApiError> {
    // Incremental scans: not yet implemented, honestly refused.
    if request.start_snapshot_id.is_none() && request.end_snapshot_id.is_none() {
        return Ok(());
    }
    if request.snapshot_id.is_some() {
        return Err(ApiError::bad_request(
            "a scan is either point-in-time (snapshot-id) or incremental \
             (start-snapshot-id/end-snapshot-id), not both",
        ));
    }
    if request.start_snapshot_id.is_some() && request.end_snapshot_id.is_none() {
        return Err(ApiError::bad_request(
            "end-snapshot-id is required when start-snapshot-id is set",
        ));
    }
    Err(unsupported(
        "incremental scan planning is not yet implemented",
    ))
}

/// The snapshot to plan: the requested one (400 when unknown) or the
/// current one (`None` for an empty table).
fn resolve_snapshot<'a>(
    metadata: &'a TableMetadata,
    request: &PlanTableScanRequest,
) -> Result<Option<&'a Snapshot>, ApiError> {
    match request.snapshot_id {
        Some(requested) => metadata
            .snapshots
            .iter()
            .flatten()
            .find(|s| s.snapshot_id == requested)
            .map(Some)
            .ok_or_else(|| ApiError::bad_request(format!("snapshot {requested} does not exist"))),
        None => match metadata.current_snapshot_id.filter(|id| *id >= 0) {
            Some(current) => metadata
                .snapshots
                .iter()
                .flatten()
                .find(|s| s.snapshot_id == current)
                .map(Some)
                .ok_or_else(|| {
                    ApiError::new(
                        StatusCode::INTERNAL_SERVER_ERROR,
                        "InternalServerError",
                        format!("current snapshot {current} is missing from table metadata"),
                    )
                }),
            None => Ok(None),
        },
    }
}

/// Validates the request against the loaded metadata and binds the
/// filter. Every early exit is an `ApiError`; an empty table resolves to
/// `None`.
fn resolve_scan(
    metadata: TableMetadata,
    request: &PlanTableScanRequest,
) -> Result<Option<ResolvedScan>, ApiError> {
    validate_scan_mode(request)?;
    let Some(snapshot) = resolve_snapshot(&metadata, request)? else {
        return Ok(None); // Empty table: a completed plan with no tasks.
    };

    let Some(manifest_list_location) = snapshot.manifest_list.clone() else {
        // v1 snapshots may carry a direct `manifests` list instead; those
        // ancient files are not supported by the planner.
        return Err(unsupported(format!(
            "snapshot {} has no manifest-list (legacy v1 layout); server-side planning \
             requires a manifest list",
            snapshot.snapshot_id
        )));
    };

    // Effective scan schema: the snapshot's schema when asked (time
    // travel), the current schema otherwise. Snapshots without a
    // schema-id (pre-v2 history) fall back to the current schema.
    let schema_id = if request.use_snapshot_schema.unwrap_or(false) {
        snapshot.schema_id.unwrap_or(metadata.current_schema_id)
    } else {
        metadata.current_schema_id
    };
    let schema = metadata
        .schemas
        .iter()
        .find(|s| s.schema_id == Some(schema_id))
        .cloned()
        .ok_or_else(|| {
            ApiError::new(
                StatusCode::INTERNAL_SERVER_ERROR,
                "InternalServerError",
                format!("schema {schema_id} is missing from table metadata"),
            )
        })?;

    let case_sensitive = request.case_sensitive.unwrap_or(true);

    // `select` is validated (unknown columns are client errors), and is
    // the seam where column masking will hook; it does not change the
    // response today.
    for field in request.select.iter().flatten() {
        if engine::resolve_field_name(&schema, field, case_sensitive).is_none() {
            return Err(ApiError::bad_request(format!(
                "select field {field:?} does not exist in the scan schema"
            )));
        }
    }
    let stats_keep = match &request.stats_fields {
        None => None,
        Some(fields) => {
            let mut keep = BTreeSet::new();
            for field in fields {
                let id = engine::resolve_field_name(&schema, field, case_sensitive).ok_or_else(
                    || {
                        ApiError::bad_request(format!(
                            "stats field {field:?} does not exist in the scan schema"
                        ))
                    },
                )?;
                keep.insert(id);
            }
            Some(keep)
        }
    };

    let bound = request
        .filter
        .clone()
        .map(|f| f.bind(&schema, case_sensitive))
        .transpose()
        .map_err(|e| ApiError::bad_request(format!("invalid filter: {e}")))?;

    let snapshot_id = snapshot.snapshot_id;
    Ok(Some(ResolvedScan {
        metadata,
        schema,
        snapshot_id,
        manifest_list_location,
        bound,
        stats_keep,
        enforcement: ScanEnforcement::none(),
    }))
}

/// Serializes an outcome into pages of `page_size` tasks and the pages'
/// store rows.
fn pages_for(
    outcome: &PlanOutcome,
    scan: &ResolvedScan,
    page_size: usize,
) -> Vec<plan_store::NewPlanPage> {
    let column_types = engine::schema_primitive_types(&scan.schema);
    let strip = &scan.enforcement.strip_fields;
    let ctx = SerializeContext {
        column_types: &column_types,
        stats_keep: scan.stats_keep.as_ref(),
        row_policy: scan.enforcement.row_policy.as_ref(),
        strip_fields: (!strip.is_empty()).then_some(strip),
    };
    engine::build_pages(outcome, &ctx, page_size)
        .into_iter()
        .enumerate()
        .map(|(i, payload)| plan_store::NewPlanPage {
            page_index: i32::try_from(i).unwrap_or(i32::MAX),
            page_token: Ulid::new().to_string(),
            payload,
        })
        .collect()
}

fn plan_ttl(state: &AppState) -> chrono::Duration {
    chrono::Duration::seconds(i64::try_from(state.config.planning.plan_ttl_secs).unwrap_or(3_600))
}

/// Merges `extra` fields into a `ScanTasks` payload object.
fn with_fields(payload: Value, extra: &[(&str, Value)]) -> Value {
    let mut obj = match payload {
        Value::Object(obj) => obj,
        other => {
            // Page payloads are objects by construction; tolerate anything
            // else by wrapping (never panic on stored data).
            let mut map = serde_json::Map::new();
            map.insert("file-scan-tasks".to_owned(), other);
            map
        }
    };
    for (key, value) in extra {
        obj.insert((*key).to_owned(), value.clone());
    }
    Value::Object(obj)
}

/// `POST /{prefix}/namespaces/{namespace}/tables/{table}/plan` —
/// planTableScan.
// A linear orchestration: RBAC → metadata → scan resolve → ABAC enforcement →
// sync/async dispatch. Splitting it apart would scatter the request lifecycle
// across helpers without making any single step clearer.
#[allow(clippy::too_many_lines)]
pub async fn plan_table_scan(
    State(state): State<AppState>,
    Extension(principal): Extension<Principal>,
    Extension(runtime): Extension<Arc<PlanningRuntime>>,
    Path((prefix, raw_namespace, name)): Path<(String, String, String)>,
    headers: axum::http::HeaderMap,
    body: axum::body::Bytes,
) -> Result<Response, ApiError> {
    if !state.config.planning.enabled {
        return Err(unsupported("server-side scan planning is disabled"));
    }
    let (warehouse, levels, record) =
        authorize_read(&state, &principal, &prefix, &raw_namespace, &name).await?;
    let request = parse_plan_request(&body)?;
    let request_json = if body.is_empty() {
        json!({})
    } else {
        serde_json::from_slice(&body).unwrap_or_else(|_| json!({}))
    };

    let Some(metadata_location) = record.metadata_location.clone() else {
        return Err(no_such_table(&levels, &name));
    };
    let storage = connect_storage(&warehouse)?;
    let metadata = read_table_metadata(storage.as_ref(), &metadata_location)
        .await
        .map_err(|e| {
            ApiError::new(
                StatusCode::INTERNAL_SERVER_ERROR,
                "InternalServerError",
                format!("current metadata at {metadata_location:?} is unreadable: {e}"),
            )
        })?;

    let created_by = principal.audit_string();

    let Some(mut scan) = resolve_scan(metadata, &request)? else {
        // Empty table: completed inline, zero tasks — still a real plan
        // row (plan-id is required in the response, and cancel must work).
        return respond_completed_inline(
            &state,
            CompletedInlinePlan {
                warehouse_id: &warehouse.id,
                table_id: &record.id,
                snapshot_id: -1,
                created_by: &created_by,
                request_json,
                summary: engine::PlanCounters::default().summary(0, 0),
                payload: json!({"file-scan-tasks": []}),
            },
        )
        .await;
    };

    // Layer-1 enforcement (Pillar D, D-F2.1): resolve the (principal, table,
    // purpose) ABAC decision now that RBAC READ has passed and the scan schema
    // is known. A full deny aborts the plan (403, audited); otherwise the
    // row-filter expression and masked-column field ids are attached to the
    // scan so every serialized task carries them.
    let chain = namespace_scope_chain(&state.pool, &warehouse.id, &levels).await?;
    let purpose = purpose_from_headers(&headers);
    apply_enforcement(
        &state,
        &principal,
        &record,
        &chain,
        &scan.schema.clone(),
        purpose.as_deref(),
        &mut scan,
    )
    .await?;

    let io = ManifestIo {
        runtime: &runtime,
        pool: &state.pool,
        storage: storage.as_ref(),
        warehouse_id: &warehouse.id,
        pg_cache: state.config.planning.pg_cache_max_bytes > 0,
    };

    // Sync-vs-async decision from the manifest list's live data-file
    // counts (one small cached read). Manifests without counts (optional
    // in v1) make the total unknowable cheaply: plan asynchronously.
    let list = io
        .manifest_list(&scan.manifest_list_location)
        .await
        .map_err(|e| plan_error_to_api(&e))?;
    let live_data_files: Option<i64> = list
        .manifests
        .iter()
        .filter(|m| m.content == ManifestContentType::Data)
        .try_fold(0_i64, |total, m| {
            match (m.added_files_count, m.existing_files_count) {
                (Some(added), Some(existing)) => {
                    Some(total + i64::from(added) + i64::from(existing))
                }
                _ => None,
            }
        });
    let sync =
        live_data_files.is_some_and(|count| count <= state.config.planning.sync_max_data_files);

    if sync {
        return respond_sync_plan(
            &state,
            &runtime,
            &io,
            &warehouse,
            &record,
            &created_by,
            scan,
            request_json,
        )
        .await;
    }

    submit_async_plan(
        &state,
        &runtime,
        storage,
        &warehouse,
        &record,
        &created_by,
        scan,
        request_json,
    )
    .await
}

/// The asynchronous path: persist a `submitted` plan, spawn the bounded
/// worker, answer the spec's `AsyncPlanningResult`. Rejects with 503
/// (rather than queueing without bound) when the pool is saturated.
#[allow(clippy::too_many_arguments)] // a plain fan-out of handler locals
async fn submit_async_plan(
    state: &AppState,
    runtime: &Arc<PlanningRuntime>,
    storage: Arc<dyn Storage>,
    warehouse: &WarehouseRecord,
    record: &TableRecord,
    created_by: &str,
    scan: ResolvedScan,
    request_json: Value,
) -> Result<Response, ApiError> {
    let Ok(permit) = runtime.semaphore.clone().try_acquire_owned() else {
        return Err(ApiError::new(
            StatusCode::SERVICE_UNAVAILABLE,
            "ServiceUnavailableException",
            "scan-planning capacity is exhausted; retry shortly",
        ));
    };

    let plan_id = plan_store::create(
        &state.pool,
        plan_store::NewScanPlan {
            workspace_id: tenancy::default_workspace_id(),
            warehouse_id: &warehouse.id,
            table_id: &record.id,
            snapshot_id: scan.snapshot_id,
            status: plan_store::PlanStatus::Submitted,
            result_mode: plan_store::ResultMode::Paged,
            created_by,
            request: request_json,
            summary: None,
            ttl: plan_ttl(state),
            pages: Vec::new(),
        },
    )
    .await?;

    let job = AsyncPlanJob {
        pool: state.pool.clone(),
        runtime: Arc::clone(runtime),
        storage,
        warehouse_id: warehouse.id.clone(),
        pg_cache: state.config.planning.pg_cache_max_bytes > 0,
        page_size: state.config.planning.page_size_files,
        plan_id: plan_id.clone(),
        table_id: record.id.clone(),
        scan,
    };
    tokio::spawn(async move {
        let _permit = permit; // released when the job finishes
        job.run().await;
    });

    Ok((
        StatusCode::OK,
        Json(json!({"status": "submitted", "plan-id": plan_id})),
    )
        .into_response())
}

/// Inputs for one completed-inline plan row + response.
struct CompletedInlinePlan<'a> {
    warehouse_id: &'a str,
    table_id: &'a str,
    snapshot_id: i64,
    created_by: &'a str,
    request_json: Value,
    summary: Value,
    payload: Value,
}

/// Persists a completed inline plan and renders the spec's
/// `CompletedPlanningWithIDResult`. Inline plans store no result pages —
/// a later fetchPlanningResult re-plans from the stored request, pinned
/// to the stored snapshot (deterministic on immutable metadata, warm in
/// the manifest cache, and the seam where per-caller policy residuals
/// will belong). See docs/design/scan-planning.md.
async fn respond_completed_inline(
    state: &AppState,
    plan: CompletedInlinePlan<'_>,
) -> Result<Response, ApiError> {
    let plan_id = plan_store::create(
        &state.pool,
        plan_store::NewScanPlan {
            workspace_id: tenancy::default_workspace_id(),
            warehouse_id: plan.warehouse_id,
            table_id: plan.table_id,
            snapshot_id: plan.snapshot_id,
            status: plan_store::PlanStatus::Completed,
            result_mode: plan_store::ResultMode::Inline,
            created_by: plan.created_by,
            request: plan.request_json,
            summary: Some(plan.summary),
            ttl: plan_ttl(state),
            pages: Vec::new(),
        },
    )
    .await?;
    let body = with_fields(
        plan.payload,
        &[("status", json!("completed")), ("plan-id", json!(plan_id))],
    );
    Ok((StatusCode::OK, Json(body)).into_response())
}

/// Recomputes an inline plan's payload for fetchPlanningResult: the
/// stored request re-planned against the stored (pinned) snapshot.
///
/// Enforcement (D-F2.1) is re-resolved and re-applied here for the *fetching*
/// principal — an inline plan stores no pages, so the filter/mask must be
/// recomputed on every fetch (RBAC READ is likewise re-checked upstream, so a
/// revoked grant or a policy that now fully denies is caught at fetch time,
/// not just at plan time). Re-resolution does not re-audit on every poll (the
/// decision was audited at plan creation); a *new* full deny does surface as a
/// 403.
#[allow(clippy::too_many_arguments)] // a plain fan-out of handler locals
async fn recompute_inline_payload(
    state: &AppState,
    runtime: &PlanningRuntime,
    principal: &Principal,
    namespace_ids: &[String],
    purpose: Option<&str>,
    warehouse: &WarehouseRecord,
    record: &TableRecord,
    plan: &plan_store::ScanPlanRecord,
) -> Result<Value, ApiError> {
    if plan.snapshot_id < 0 {
        return Ok(json!({"file-scan-tasks": []}));
    }
    let mut request: PlanTableScanRequest =
        serde_json::from_value(plan.request.clone()).map_err(|e| {
            ApiError::new(
                StatusCode::INTERNAL_SERVER_ERROR,
                "InternalServerError",
                format!("stored plan request is unreadable: {e}"),
            )
        })?;
    request.snapshot_id = Some(plan.snapshot_id);

    let Some(metadata_location) = record.metadata_location.clone() else {
        return Err(no_such_plan(&plan.id));
    };
    let storage = connect_storage(warehouse)?;
    let metadata = read_table_metadata(storage.as_ref(), &metadata_location)
        .await
        .map_err(|e| {
            ApiError::new(
                StatusCode::INTERNAL_SERVER_ERROR,
                "InternalServerError",
                format!("current metadata at {metadata_location:?} is unreadable: {e}"),
            )
        })?;
    let mut scan = resolve_scan(metadata, &request)?.ok_or_else(|| {
        // The pinned snapshot has been expired/removed from metadata since
        // the plan was created; the plan result is genuinely gone.
        no_such_plan(&plan.id)
    })?;

    // Re-apply Layer-1 enforcement for the fetching principal.
    let table_ctx = TableContext {
        table_id: &record.id,
        namespace_ids,
        schema: &scan.schema.clone(),
        owner: None,
    };
    let policy =
        governance::resolve_scan_policy(&state.pool, principal, &table_ctx, purpose).await?;
    if policy.denied {
        return Err(forbidden(format!(
            "access denied by policy: {}",
            policy.reason
        )));
    }
    let mut strip_fields = BTreeSet::new();
    for col in &policy.removed_columns {
        if let Some(id) = engine::resolve_field_name(&scan.schema, col, true) {
            strip_fields.insert(id);
        }
    }
    scan.enforcement = ScanEnforcement {
        row_policy: policy.row_filter,
        strip_fields,
    };

    let io = ManifestIo {
        runtime,
        pool: &state.pool,
        storage: storage.as_ref(),
        warehouse_id: &warehouse.id,
        pg_cache: state.config.planning.pg_cache_max_bytes > 0,
    };
    let outcome = plan_scan(
        &io,
        &scan.metadata,
        &scan.schema,
        &scan.manifest_list_location,
        scan.bound.as_ref(),
    )
    .await
    .map_err(|e| plan_error_to_api(&e))?;
    let mut pages = pages_for(&outcome, &scan, usize::MAX);
    Ok(if pages.is_empty() {
        json!({"file-scan-tasks": []})
    } else {
        pages.swap_remove(0).payload
    })
}

/// The synchronous path: plan inline, persist the completed plan, answer
/// with every file scan task in the body.
#[allow(clippy::too_many_arguments)] // a plain fan-out of handler locals
async fn respond_sync_plan(
    state: &AppState,
    runtime: &PlanningRuntime,
    io: &ManifestIo<'_>,
    warehouse: &WarehouseRecord,
    record: &TableRecord,
    created_by: &str,
    scan: ResolvedScan,
    request_json: Value,
) -> Result<Response, ApiError> {
    let started = std::time::Instant::now();
    let outcome = plan_scan(
        io,
        &scan.metadata,
        &scan.schema,
        &scan.manifest_list_location,
        scan.bound.as_ref(),
    )
    .await
    .map_err(|e| plan_error_to_api(&e))?;
    // The inline result is a single page holding every task, reused
    // verbatim by fetchPlanningResult.
    let mut pages = pages_for(&outcome, &scan, usize::MAX);
    let summary = enriched_summary(&outcome, "sync", started, runtime, None);
    tracing::debug!(
        table_id = %record.id,
        snapshot_id = scan.snapshot_id,
        matched = outcome.files.len(),
        %summary,
        "synchronous scan plan completed"
    );
    let payload = if pages.is_empty() {
        json!({"file-scan-tasks": []})
    } else {
        pages.swap_remove(0).payload
    };
    respond_completed_inline(
        state,
        CompletedInlinePlan {
            warehouse_id: &warehouse.id,
            table_id: &record.id,
            snapshot_id: scan.snapshot_id,
            created_by,
            request_json,
            summary,
            payload,
        },
    )
    .await
}

/// The plan summary with mode, duration, page count, and cache counters
/// attached.
fn enriched_summary(
    outcome: &PlanOutcome,
    mode: &str,
    started: std::time::Instant,
    runtime: &PlanningRuntime,
    pages: Option<usize>,
) -> Value {
    let referenced: BTreeSet<usize> = outcome
        .files
        .iter()
        .flat_map(|f| f.delete_indices.iter().copied())
        .collect();
    let mut summary = outcome
        .counters
        .summary(outcome.files.len(), referenced.len());
    if let Value::Object(map) = &mut summary {
        map.insert("mode".to_owned(), json!(mode));
        map.insert(
            "duration_ms".to_owned(),
            json!(u64::try_from(started.elapsed().as_millis()).unwrap_or(u64::MAX)),
        );
        if let Some(pages) = pages {
            map.insert("pages".to_owned(), json!(pages));
        }
        map.insert("cache".to_owned(), runtime.counters.snapshot());
    }
    summary
}

/// Everything an asynchronous plan needs, owned.
struct AsyncPlanJob {
    pool: PgPool,
    runtime: Arc<PlanningRuntime>,
    storage: Arc<dyn Storage>,
    warehouse_id: String,
    pg_cache: bool,
    page_size: usize,
    plan_id: String,
    table_id: String,
    scan: ResolvedScan,
}

impl AsyncPlanJob {
    async fn run(self) {
        let started = std::time::Instant::now();
        let io = ManifestIo {
            runtime: &self.runtime,
            pool: &self.pool,
            storage: self.storage.as_ref(),
            warehouse_id: &self.warehouse_id,
            pg_cache: self.pg_cache,
        };
        let result = plan_scan(
            &io,
            &self.scan.metadata,
            &self.scan.schema,
            &self.scan.manifest_list_location,
            self.scan.bound.as_ref(),
        )
        .await;

        match result {
            Ok(outcome) => {
                let pages = pages_for(&outcome, &self.scan, self.page_size);
                let summary =
                    enriched_summary(&outcome, "async", started, &self.runtime, Some(pages.len()));
                tracing::info!(
                    plan_id = %self.plan_id,
                    table_id = %self.table_id,
                    matched = outcome.files.len(),
                    %summary,
                    "asynchronous scan plan completed"
                );
                match plan_store::complete(&self.pool, &self.plan_id, &pages, &summary).await {
                    Ok(true) => {}
                    Ok(false) => tracing::debug!(
                        plan_id = %self.plan_id,
                        "plan left submitted state before completion; result discarded"
                    ),
                    Err(error) => tracing::error!(
                        plan_id = %self.plan_id, %error,
                        "failed to persist completed scan plan"
                    ),
                }
            }
            Err(error) => {
                tracing::error!(plan_id = %self.plan_id, %error, "asynchronous scan plan failed");
                let payload = json!({
                    "error": {
                        "message": error.to_string(),
                        "type": "InternalServerError",
                        "code": 500,
                    }
                });
                if let Err(store_error) =
                    plan_store::fail(&self.pool, &self.plan_id, &payload).await
                {
                    tracing::error!(
                        plan_id = %self.plan_id, %store_error,
                        "failed to persist scan-plan failure"
                    );
                }
            }
        }
    }
}

/// Loads a plan and verifies it belongs to the resolved table (a plan id
/// under the wrong table path is a 404, not a leak).
async fn plan_for_table(
    pool: &PgPool,
    record: &TableRecord,
    plan_id: &str,
) -> Result<plan_store::ScanPlanRecord, ApiError> {
    let plan = plan_store::get(pool, plan_id)
        .await?
        .ok_or_else(|| no_such_plan(plan_id))?;
    if plan.table_id != record.id {
        return Err(no_such_plan(plan_id));
    }
    Ok(plan)
}

/// `GET /{prefix}/namespaces/{namespace}/tables/{table}/plan/{plan-id}`
/// — fetchPlanningResult.
pub async fn fetch_planning_result(
    State(state): State<AppState>,
    Extension(principal): Extension<Principal>,
    Extension(runtime): Extension<Arc<PlanningRuntime>>,
    Path((prefix, raw_namespace, name, plan_id)): Path<(String, String, String, String)>,
    headers: axum::http::HeaderMap,
) -> Result<Response, ApiError> {
    if !state.config.planning.enabled {
        return Err(unsupported("server-side scan planning is disabled"));
    }
    let (warehouse, levels, record) =
        authorize_read(&state, &principal, &prefix, &raw_namespace, &name).await?;
    let plan = plan_for_table(&state.pool, &record, &plan_id).await?;

    let body = match plan.status {
        plan_store::PlanStatus::Submitted => json!({"status": "submitted"}),
        plan_store::PlanStatus::Cancelled => json!({"status": "cancelled"}),
        plan_store::PlanStatus::Failed => {
            let error = plan.error.clone().unwrap_or_else(|| {
                json!({"error": {"message": "planning failed", "type": "InternalServerError", "code": 500}})
            });
            with_fields(error, &[("status", json!("failed"))])
        }
        plan_store::PlanStatus::Completed => match plan.result_mode {
            plan_store::ResultMode::Inline => {
                let chain = namespace_scope_chain(&state.pool, &warehouse.id, &levels).await?;
                let purpose = purpose_from_headers(&headers);
                let payload = recompute_inline_payload(
                    &state,
                    &runtime,
                    &principal,
                    &chain,
                    purpose.as_deref(),
                    &warehouse,
                    &record,
                    &plan,
                )
                .await?;
                with_fields(payload, &[("status", json!("completed"))])
            }
            plan_store::ResultMode::Paged => {
                let tokens = plan_store::page_tokens(&state.pool, &plan.id).await?;
                json!({"status": "completed", "plan-tasks": tokens})
            }
        },
    };
    Ok((StatusCode::OK, Json(body)).into_response())
}

/// `DELETE /{prefix}/namespaces/{namespace}/tables/{table}/plan/{plan-id}`
/// — cancelPlanning. Idempotent on terminal states; 204 on success.
pub async fn cancel_planning(
    State(state): State<AppState>,
    Extension(principal): Extension<Principal>,
    Path((prefix, raw_namespace, name, plan_id)): Path<(String, String, String, String)>,
) -> Result<Response, ApiError> {
    if !state.config.planning.enabled {
        return Err(unsupported("server-side scan planning is disabled"));
    }
    let (_warehouse, _levels, record) =
        authorize_read(&state, &principal, &prefix, &raw_namespace, &name).await?;
    // Existence + table-scoping check first, then the (audited)
    // compare-and-set cancel. A concurrent expiry between the two reads
    // as a 404, which is also what a later retry would see.
    plan_for_table(&state.pool, &record, &plan_id).await?;
    let cancelled = plan_store::cancel(
        &state.pool,
        tenancy::default_workspace_id(),
        &plan_id,
        &principal.audit_string(),
    )
    .await?;
    if !cancelled {
        return Err(no_such_plan(&plan_id));
    }
    Ok(StatusCode::NO_CONTENT.into_response())
}

/// `POST /{prefix}/namespaces/{namespace}/tables/{table}/tasks` —
/// fetchScanTasks.
pub async fn fetch_scan_tasks(
    State(state): State<AppState>,
    Extension(principal): Extension<Principal>,
    Path((prefix, raw_namespace, name)): Path<(String, String, String)>,
    body: axum::body::Bytes,
) -> Result<Response, ApiError> {
    if !state.config.planning.enabled {
        return Err(unsupported("server-side scan planning is disabled"));
    }
    let (_warehouse, _levels, record) =
        authorize_read(&state, &principal, &prefix, &raw_namespace, &name).await?;
    let request: FetchScanTasksRequest = serde_json::from_slice(&body)
        .map_err(|e| ApiError::bad_request(format!("invalid fetchScanTasks request body: {e}")))?;

    let Some((plan, payload)) = plan_store::page_by_token(&state.pool, &request.plan_task).await?
    else {
        return Err(no_such_plan_task());
    };
    if plan.table_id != record.id || plan.status != plan_store::PlanStatus::Completed {
        return Err(no_such_plan_task());
    }
    Ok((StatusCode::OK, Json(payload)).into_response())
}
