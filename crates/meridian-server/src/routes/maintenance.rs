//! Management API for autonomous table maintenance (Pillar C), mounted under
//! `/api/v2`. Five surfaces:
//!
//! - **Policies** (`/api/v2/maintenance/policies`): declarative per-scope
//!   maintenance configuration (C-F3). CRUD; a policy attaches to a
//!   warehouse, a namespace, or one table, addressed by name (resolved to the
//!   internal scope id here).
//! - **Health** (`/api/v2/warehouses/{w}/namespaces/{ns}/tables/{t}/health`):
//!   the C-F1 health score, metric breakdown, file histogram, and top-3
//!   recommendations, computed on demand from metadata (zero data scan), plus
//!   `.../health/history` for the trend.
//! - **Jobs** (`/api/v2/maintenance/jobs`): list/get/cancel the maintenance
//!   queue and POST a manual compaction/expiry trigger on a table.
//! - **Savings ledger** (`/api/v2/maintenance/savings`): the per-job receipts
//!   and the monthly roll-up ("Meridian saved X bytes / Y files").
//! - **Fleet health** (`/api/v2/warehouses/{w}/health-summary`): the
//!   per-warehouse overview.
//!
//! # Authorization
//!
//! Reads (health, job list/get, ledger, fleet summary) require `READ` on the
//! scope (the table for a table read; management access for the
//! workspace-wide job/ledger lists, which span resources). Mutations —
//! creating/updating/deleting a policy, triggering a job, cancelling a job —
//! require **`MANAGE_NAMESPACE`** on the target's namespace (inherited from a
//! warehouse grant). `MANAGE_NAMESPACE` is the existing "operator of the
//! tables under this namespace" privilege; maintenance acts on those tables,
//! so it is the honest fit without inventing a new privilege. This is the
//! documented choice from the milestone brief.

use axum::extract::{Path, Query, State};
use axum::http::StatusCode;
use axum::{Extension, Json};
use chrono::{DateTime, NaiveDate, Utc};
use meridian_common::MeridianError;
use meridian_common::principal::Principal;
use meridian_store::maintenance::{
    self, JobRecord, JobState, JobType, PolicyRecord, PolicySpec, Scope,
};
use meridian_store::rbac::{Privilege, SecurableScope};
use meridian_store::warehouse::WarehouseRecord;
use meridian_store::{health, namespace, table, tenancy};
use serde::{Deserialize, Serialize};
use serde_json::{Value, json};

use crate::AppState;
use crate::error::ApiError;
use crate::routes::grants::{namespace_scope_chain, require, require_management};
use crate::routes::namespaces::{decode_namespace_param, resolve_warehouse};
use crate::routes::tables::connect_storage;

/// Default and max page sizes for the job and ledger lists.
const DEFAULT_LIMIT: i64 = 50;
const MAX_LIMIT: i64 = 500;
/// Default number of health-history points returned.
const DEFAULT_HISTORY_LIMIT: i64 = 50;
/// Default and max months in a savings roll-up.
const DEFAULT_ROLLUP_MONTHS: i64 = 12;
const MAX_ROLLUP_MONTHS: i64 = 120;

// ===========================================================================
// Policies
// ===========================================================================

/// The scope selector in a policy request, by name. Exactly one of the three
/// shapes: warehouse only (warehouse scope), warehouse + namespace (namespace
/// scope), or warehouse + namespace + table (table scope).
#[derive(Debug, Deserialize)]
pub struct PolicyScopeSelector {
    /// Warehouse name (always required — it is the addressing root).
    pub warehouse: String,
    /// Dotted namespace, e.g. `analytics.reporting`. Required for namespace
    /// and table scopes.
    #[serde(default)]
    pub namespace: Option<String>,
    /// Table name. Present for a table-scoped policy.
    #[serde(default)]
    pub table: Option<String>,
}

/// Request body for creating/updating a policy: the scope plus the spec
/// fields. Absent spec fields fall back to the C-F3 defaults.
#[derive(Debug, Deserialize)]
pub struct PolicyRequest {
    /// The scope this policy attaches to.
    pub scope: PolicyScopeSelector,
    /// Target compacted file size (bytes); default 512 MiB.
    #[serde(default)]
    pub target_file_size_bytes: Option<i64>,
    /// Minimum input files a compaction must combine; default 5.
    #[serde(default)]
    pub min_input_files: Option<i32>,
    /// Keep at least this many snapshots; default 100.
    #[serde(default)]
    pub snapshot_retention_count: Option<i32>,
    /// Keep any snapshot younger than this many millis; default 5 days.
    #[serde(default)]
    pub snapshot_retention_age_ms: Option<i64>,
    /// Freshness SLA (millis); `null`/absent = no SLA.
    #[serde(default)]
    pub max_staleness_ms: Option<i64>,
    /// Cron-ish schedule; absent = reconcile-driven only.
    #[serde(default)]
    pub schedule: Option<String>,
    /// Execution-window start (e.g. `"02:00"`).
    #[serde(default)]
    pub window_start: Option<String>,
    /// Execution-window end.
    #[serde(default)]
    pub window_end: Option<String>,
    /// Monthly spend cap (USD); absent = uncapped.
    #[serde(default)]
    pub cost_cap_usd_month: Option<f64>,
    /// Exclusion rules (opaque; shape owned by the worker).
    #[serde(default)]
    pub exclusions: Option<Value>,
    /// Whether the policy is active; default true.
    #[serde(default)]
    pub enabled: Option<bool>,
}

impl PolicyRequest {
    /// Builds the store [`PolicySpec`] from the request, defaulting absent
    /// fields to the C-F3 defaults.
    fn to_spec(&self) -> PolicySpec {
        let d = PolicySpec::default();
        PolicySpec {
            target_file_size_bytes: self
                .target_file_size_bytes
                .unwrap_or(d.target_file_size_bytes),
            min_input_files: self.min_input_files.unwrap_or(d.min_input_files),
            snapshot_retention_count: self
                .snapshot_retention_count
                .unwrap_or(d.snapshot_retention_count),
            snapshot_retention_age_ms: self
                .snapshot_retention_age_ms
                .unwrap_or(d.snapshot_retention_age_ms),
            max_staleness_ms: self.max_staleness_ms,
            schedule: self.schedule.clone(),
            window_start: self.window_start.clone(),
            window_end: self.window_end.clone(),
            cost_cap_usd_month: self.cost_cap_usd_month,
            exclusions: self.exclusions.clone().unwrap_or_else(|| json!({})),
            enabled: self.enabled.unwrap_or(d.enabled),
        }
    }
}

/// A policy as rendered by the API.
#[derive(Debug, Serialize)]
pub struct PolicyResponse {
    /// ULID of the policy.
    pub id: String,
    /// Scope kind: `warehouse` | `namespace` | `table`.
    pub scope: String,
    /// Internal id of the scoped object.
    pub scope_id: String,
    /// Human-readable scope, e.g. `warehouse:wh / analytics.reporting.orders`.
    pub scope_label: String,
    /// Target compacted file size (bytes).
    pub target_file_size_bytes: i64,
    /// Minimum input files a compaction must combine.
    pub min_input_files: i32,
    /// Snapshot retention count.
    pub snapshot_retention_count: i32,
    /// Snapshot retention age (millis).
    pub snapshot_retention_age_ms: i64,
    /// Freshness SLA (millis), if any.
    pub max_staleness_ms: Option<i64>,
    /// Cron-ish schedule, if any.
    pub schedule: Option<String>,
    /// Execution-window start.
    pub window_start: Option<String>,
    /// Execution-window end.
    pub window_end: Option<String>,
    /// Monthly spend cap (USD), if any.
    pub cost_cap_usd_month: Option<f64>,
    /// Exclusion rules.
    pub exclusions: Value,
    /// Whether the policy is active.
    pub enabled: bool,
    /// Creation time.
    pub created_at: DateTime<Utc>,
    /// Last-update time.
    pub updated_at: DateTime<Utc>,
}

impl PolicyResponse {
    fn from_record(record: PolicyRecord, scope_label: String) -> Self {
        Self {
            id: record.id,
            scope: record.scope.as_str().to_owned(),
            scope_id: record.scope_id,
            scope_label,
            target_file_size_bytes: record.spec.target_file_size_bytes,
            min_input_files: record.spec.min_input_files,
            snapshot_retention_count: record.spec.snapshot_retention_count,
            snapshot_retention_age_ms: record.spec.snapshot_retention_age_ms,
            max_staleness_ms: record.spec.max_staleness_ms,
            schedule: record.spec.schedule,
            window_start: record.spec.window_start,
            window_end: record.spec.window_end,
            cost_cap_usd_month: record.spec.cost_cap_usd_month,
            exclusions: record.spec.exclusions,
            enabled: record.spec.enabled,
            created_at: record.created_at,
            updated_at: record.updated_at,
        }
    }
}

/// A resolved policy scope: the store `(Scope, scope_id)` plus the namespace
/// scope chain used for the authorization check and a display label.
struct ResolvedScope {
    scope: Scope,
    scope_id: String,
    warehouse: WarehouseRecord,
    namespace_chain: Vec<String>,
    label: String,
}

/// Resolves a name-based [`PolicyScopeSelector`] to internal ids, and loads
/// the namespace scope chain for authorization. Enforces that the referenced
/// warehouse/namespace/table actually exist (the store does not, since
/// `scope_id` is polymorphic).
async fn resolve_scope(
    state: &AppState,
    selector: &PolicyScopeSelector,
) -> Result<ResolvedScope, ApiError> {
    let warehouse = resolve_warehouse(&state.pool, &selector.warehouse).await?;

    match (&selector.namespace, &selector.table) {
        (None, None) => Ok(ResolvedScope {
            scope: Scope::Warehouse,
            scope_id: warehouse.id.clone(),
            namespace_chain: Vec::new(),
            label: format!("warehouse:{}", warehouse.name),
            warehouse,
        }),
        (Some(ns), table) => {
            let levels = parse_dotted_namespace(ns)?;
            let namespace = namespace::get(&state.pool, &warehouse.id, &levels)
                .await?
                .ok_or_else(|| {
                    ApiError::no_such_namespace(format!("namespace {ns:?} does not exist"))
                })?;
            let chain = namespace_scope_chain(&state.pool, &warehouse.id, &levels).await?;
            match table {
                None => Ok(ResolvedScope {
                    scope: Scope::Namespace,
                    scope_id: namespace.id,
                    namespace_chain: chain,
                    label: format!("{}/{}", warehouse.name, ns),
                    warehouse,
                }),
                Some(table_name) => {
                    let record = table::get(&state.pool, &namespace.id, table_name)
                        .await?
                        .ok_or_else(|| {
                            ApiError::no_such_table(format!(
                                "table {:?} does not exist",
                                format!("{ns}.{table_name}")
                            ))
                        })?;
                    Ok(ResolvedScope {
                        scope: Scope::Table,
                        scope_id: record.id,
                        namespace_chain: chain,
                        label: format!("{}/{}.{}", warehouse.name, ns, table_name),
                        warehouse,
                    })
                }
            }
        }
        (None, Some(_)) => Err(ApiError::bad_request(
            "a table-scoped policy must also name its namespace",
        )),
    }
}

/// Requires `MANAGE_NAMESPACE` for a policy mutation on a resolved scope.
/// Warehouse-scoped policies check at the warehouse; namespace/table policies
/// check on the namespace chain (a warehouse grant inherits down).
async fn require_manage_scope(
    state: &AppState,
    principal: &Principal,
    resolved: &ResolvedScope,
) -> Result<(), ApiError> {
    let scope = if resolved.namespace_chain.is_empty() {
        SecurableScope::warehouse(&resolved.warehouse.id)
    } else {
        SecurableScope::namespace(&resolved.warehouse.id, resolved.namespace_chain.clone())
    };
    require(&state.pool, principal, Privilege::ManageNamespace, &scope).await
}

/// `GET /api/v2/maintenance/policies` — list every policy in the workspace.
pub async fn list_policies(
    State(state): State<AppState>,
    Extension(principal): Extension<Principal>,
) -> Result<Json<Value>, ApiError> {
    require_management(&state.pool, &principal).await?;
    let records = query_policies(&state.pool).await?;
    let mut policies = Vec::with_capacity(records.len());
    for record in records {
        let label = policy_scope_label(&state.pool, &record).await;
        policies.push(PolicyResponse::from_record(record, label));
    }
    Ok(Json(json!({ "policies": policies })))
}

/// `POST /api/v2/maintenance/policies` — create a policy for a scope.
pub async fn create_policy(
    State(state): State<AppState>,
    Extension(principal): Extension<Principal>,
    Json(request): Json<PolicyRequest>,
) -> Result<(StatusCode, Json<PolicyResponse>), ApiError> {
    let resolved = resolve_scope(&state, &request.scope).await?;
    require_manage_scope(&state, &principal, &resolved).await?;

    let record = maintenance::create_policy(
        &state.pool,
        tenancy::default_workspace_id(),
        resolved.scope,
        &resolved.scope_id,
        &request.to_spec(),
        &principal.audit_string(),
    )
    .await
    .map_err(|e| match e {
        MeridianError::Conflict(message) => ApiError::already_exists(message),
        MeridianError::Validation(message) => ApiError::bad_request(message),
        other => ApiError::from(other),
    })?;

    Ok((
        StatusCode::CREATED,
        Json(PolicyResponse::from_record(record, resolved.label)),
    ))
}

/// `PUT /api/v2/maintenance/policies` — update the policy for a scope in
/// place. (Policies are singular per scope; the body carries the scope.)
pub async fn update_policy(
    State(state): State<AppState>,
    Extension(principal): Extension<Principal>,
    Json(request): Json<PolicyRequest>,
) -> Result<Json<PolicyResponse>, ApiError> {
    let resolved = resolve_scope(&state, &request.scope).await?;
    require_manage_scope(&state, &principal, &resolved).await?;

    let record = maintenance::update_policy(
        &state.pool,
        tenancy::default_workspace_id(),
        resolved.scope,
        &resolved.scope_id,
        &request.to_spec(),
        &principal.audit_string(),
    )
    .await
    .map_err(|e| match e {
        MeridianError::NotFound(message) => {
            ApiError::new(StatusCode::NOT_FOUND, "NotFoundException", message)
        }
        MeridianError::Validation(message) => ApiError::bad_request(message),
        other => ApiError::from(other),
    })?;

    Ok(Json(PolicyResponse::from_record(record, resolved.label)))
}

/// `DELETE /api/v2/maintenance/policies` — delete the policy for a scope.
/// The scope is carried in the request body (a policy is addressed by scope,
/// not by an opaque id, so callers do not need to fetch the id first).
pub async fn delete_policy(
    State(state): State<AppState>,
    Extension(principal): Extension<Principal>,
    Json(selector): Json<PolicyScopeSelector>,
) -> Result<StatusCode, ApiError> {
    let resolved = resolve_scope(&state, &selector).await?;
    require_manage_scope(&state, &principal, &resolved).await?;

    let deleted = maintenance::delete_policy(
        &state.pool,
        tenancy::default_workspace_id(),
        resolved.scope,
        &resolved.scope_id,
        &principal.audit_string(),
    )
    .await?;
    if deleted {
        Ok(StatusCode::NO_CONTENT)
    } else {
        Err(ApiError::new(
            StatusCode::NOT_FOUND,
            "NotFoundException",
            "no policy exists for that scope",
        ))
    }
}

// ===========================================================================
// Health
// ===========================================================================

/// The rendered health of a table.
#[derive(Debug, Serialize)]
pub struct HealthResponse {
    /// Table id.
    pub table_id: String,
    /// Human-readable identity, e.g. `analytics.orders`.
    pub table_ident: String,
    /// The table snapshot the health was computed against.
    pub snapshot_id: Option<i64>,
    /// Composite 0..=100 health score.
    pub score: u8,
    /// The derived metric summary.
    pub metrics: Value,
    /// Top-3 recommended actions.
    pub recommendations: Vec<Value>,
    /// When it was computed.
    pub computed_at: DateTime<Utc>,
}

/// Resolves a warehouse/namespace/table path to the storage + table record +
/// effective policy needed to compute health, checking `READ`.
async fn resolve_table_for_read(
    state: &AppState,
    principal: &Principal,
    warehouse_name: &str,
    raw_namespace: &str,
    table_name: &str,
) -> Result<(WarehouseRecord, table::TableRecord, Vec<String>, PolicySpec), ApiError> {
    let warehouse = resolve_warehouse(&state.pool, warehouse_name).await?;
    let levels = decode_namespace_param(raw_namespace)?;
    let namespace = namespace::get(&state.pool, &warehouse.id, &levels)
        .await?
        .ok_or_else(|| {
            ApiError::no_such_namespace(format!("namespace {:?} does not exist", levels.join(".")))
        })?;
    let record = table::get(&state.pool, &namespace.id, table_name)
        .await?
        .ok_or_else(|| {
            ApiError::no_such_table(format!(
                "table {:?} does not exist",
                crate::routes::maintenance::display_ident(&levels, table_name)
            ))
        })?;
    let chain = namespace_scope_chain(&state.pool, &warehouse.id, &levels).await?;
    require(
        &state.pool,
        principal,
        Privilege::Read,
        &SecurableScope::table(&warehouse.id, chain, Some(&record.id)),
    )
    .await?;
    let policy = maintenance::resolve_effective(
        &state.pool,
        tenancy::default_workspace_id(),
        &record.id,
        &namespace.id,
        &warehouse.id,
    )
    .await?
    .map_or_else(PolicySpec::default, |r| r.spec);
    Ok((warehouse, record, levels, policy))
}

/// `GET /api/v2/warehouses/{w}/namespaces/{ns}/tables/{t}/health` — compute
/// and persist the table's current health, returning the score + breakdown.
pub async fn get_table_health(
    State(state): State<AppState>,
    Extension(principal): Extension<Principal>,
    Path((warehouse_name, raw_namespace, table_name)): Path<(String, String, String)>,
) -> Result<Json<HealthResponse>, ApiError> {
    let (warehouse, record, levels, policy) = resolve_table_for_read(
        &state,
        &principal,
        &warehouse_name,
        &raw_namespace,
        &table_name,
    )
    .await?;
    let Some(metadata_location) = record.metadata_location.clone() else {
        return Err(ApiError::no_such_table(format!(
            "table {:?} has no metadata",
            display_ident(&levels, &table_name)
        )));
    };
    let storage = connect_storage(&warehouse)?;
    let target = health::HealthTarget {
        table_id: record.id.clone(),
        table_ident: display_ident(&levels, &table_name),
        metadata_location,
        target_file_size_bytes: policy.target_file_size_bytes,
        max_staleness_ms: policy.max_staleness_ms,
    };
    let computed = health::compute_health(
        &state.pool,
        storage.as_ref(),
        tenancy::default_workspace_id(),
        &target,
    )
    .await?;
    Ok(Json(render_health(&target.table_ident, &computed)))
}

/// `GET /api/v2/warehouses/{w}/namespaces/{ns}/tables/{t}/health/history` —
/// the persisted health trend, newest first.
pub async fn get_table_health_history(
    State(state): State<AppState>,
    Extension(principal): Extension<Principal>,
    Path((warehouse_name, raw_namespace, table_name)): Path<(String, String, String)>,
    Query(query): Query<HistoryQuery>,
) -> Result<Json<Value>, ApiError> {
    let (_, record, levels, _) = resolve_table_for_read(
        &state,
        &principal,
        &warehouse_name,
        &raw_namespace,
        &table_name,
    )
    .await?;
    let limit = query
        .limit
        .unwrap_or(DEFAULT_HISTORY_LIMIT)
        .clamp(1, MAX_LIMIT);
    let history = health::history(&state.pool, &record.id, limit).await?;
    let ident = display_ident(&levels, &table_name);
    let points: Vec<HealthResponse> = history
        .into_iter()
        .map(|record| render_health(&ident, &record))
        .collect();
    Ok(Json(json!({ "history": points })))
}

/// Query params for the health-history endpoint.
#[derive(Debug, Deserialize)]
pub struct HistoryQuery {
    /// Max points to return (default 50, max 500).
    #[serde(default)]
    pub limit: Option<i64>,
}

/// Renders a persisted health record as the API response.
fn render_health(ident: &str, record: &health::HealthSnapshotRecord) -> HealthResponse {
    let metrics = json!({
        "total_bytes": record.metrics.total_bytes,
        "data_file_count": record.metrics.data_file_count,
        "small_file_ratio": record.metrics.small_file_ratio,
        "avg_file_bytes": record.metrics.avg_file_bytes,
        "median_file_bytes": record.metrics.median_file_bytes,
        "delete_debt_ratio": record.metrics.delete_debt_ratio,
        "delete_file_count": record.metrics.delete_file_count,
        "manifest_count": record.metrics.manifest_count,
        "avg_manifest_entries": record.metrics.avg_manifest_entries,
        "partition_skew": record.metrics.partition_skew,
        "snapshot_count": record.metrics.snapshot_count,
        "oldest_snapshot_ms": record.metrics.oldest_snapshot_ms,
        "metadata_json_bytes": record.metrics.metadata_json_bytes,
        "file_size_histogram": record.metrics.file_size_histogram,
    });
    let recommendations = record
        .recommendations
        .iter()
        .map(|r| json!({ "action": r.action, "reason": r.reason, "impact": r.impact }))
        .collect();
    HealthResponse {
        table_id: record.table_id.clone(),
        table_ident: ident.to_owned(),
        snapshot_id: record.snapshot_id,
        score: record.score,
        metrics,
        recommendations,
        computed_at: record.computed_at,
    }
}

// ===========================================================================
// Jobs
// ===========================================================================

/// A maintenance job as rendered by the API.
#[derive(Debug, Serialize)]
pub struct JobResponse {
    /// ULID of the job.
    pub id: String,
    /// Target table id.
    pub table_id: String,
    /// Job type: `compaction` | `expire_snapshots` | ...
    pub job_type: String,
    /// Lifecycle state.
    pub state: String,
    /// Scheduling policy id, if policy-driven.
    pub policy_id: Option<String>,
    /// Job parameters.
    pub spec: Value,
    /// Who created it (`reconciler`, a user audit string, ...).
    pub created_by: String,
    /// Worker holding it, if running.
    pub claimed_by: Option<String>,
    /// Claim-cycle count.
    pub attempts: i32,
    /// Failure payload, for failed jobs.
    pub error: Option<Value>,
    /// Success result (before/after metrics).
    pub result: Option<Value>,
    /// Creation time.
    pub created_at: DateTime<Utc>,
    /// When it started running.
    pub started_at: Option<DateTime<Utc>>,
    /// When it reached a terminal state.
    pub finished_at: Option<DateTime<Utc>>,
}

impl From<JobRecord> for JobResponse {
    fn from(record: JobRecord) -> Self {
        Self {
            id: record.id,
            table_id: record.table_id,
            job_type: record.job_type.as_str().to_owned(),
            state: record.state.as_str().to_owned(),
            policy_id: record.policy_id,
            spec: record.spec,
            created_by: record.created_by,
            claimed_by: record.claimed_by,
            attempts: record.attempts,
            error: record.error,
            result: record.result,
            created_at: record.created_at,
            started_at: record.started_at,
            finished_at: record.finished_at,
        }
    }
}

/// Query params for the job list.
#[derive(Debug, Deserialize)]
pub struct JobListQuery {
    /// Filter by state: `queued` | `running` | `succeeded` | `failed` |
    /// `cancelled`.
    #[serde(default)]
    pub state: Option<String>,
    /// Filter by table id.
    #[serde(default)]
    pub table_id: Option<String>,
    /// Page size (default 50, max 500).
    #[serde(default)]
    pub limit: Option<i64>,
}

/// `GET /api/v2/maintenance/jobs` — list maintenance jobs (newest first).
/// Management-gated: the job queue spans every table in the workspace.
pub async fn list_jobs(
    State(state): State<AppState>,
    Extension(principal): Extension<Principal>,
    Query(query): Query<JobListQuery>,
) -> Result<Json<Value>, ApiError> {
    require_management(&state.pool, &principal).await?;
    // Validate the state filter if present (an unknown state is a 400, not an
    // empty list, so a typo is caught).
    if let Some(state_str) = &query.state {
        parse_job_state(state_str)
            .ok_or_else(|| ApiError::bad_request(format!("unknown job state {state_str:?}")))?;
    }
    let limit = query.limit.unwrap_or(DEFAULT_LIMIT).clamp(1, MAX_LIMIT);
    let jobs = query_jobs(
        &state.pool,
        query.state.as_deref(),
        query.table_id.as_deref(),
        limit,
    )
    .await?;
    let jobs: Vec<JobResponse> = jobs.into_iter().map(JobResponse::from).collect();
    Ok(Json(json!({ "jobs": jobs })))
}

/// `GET /api/v2/maintenance/jobs/{id}` — one job.
pub async fn get_job(
    State(state): State<AppState>,
    Extension(principal): Extension<Principal>,
    Path(job_id): Path<String>,
) -> Result<Json<JobResponse>, ApiError> {
    require_management(&state.pool, &principal).await?;
    let job = maintenance::get_job(&state.pool, tenancy::default_workspace_id(), &job_id)
        .await?
        .ok_or_else(|| {
            ApiError::new(StatusCode::NOT_FOUND, "NotFoundException", "job not found")
        })?;
    Ok(Json(job.into()))
}

/// `POST /api/v2/maintenance/jobs/{id}/cancel` — cancel a queued/running job.
pub async fn cancel_job(
    State(state): State<AppState>,
    Extension(principal): Extension<Principal>,
    Path(job_id): Path<String>,
) -> Result<Json<JobResponse>, ApiError> {
    // Cancelling a job is a maintenance mutation. Resolve the job's table to
    // check MANAGE_NAMESPACE on it (job ids are workspace-scoped; a caller who
    // can manage the namespace can cancel its jobs).
    let job = maintenance::get_job(&state.pool, tenancy::default_workspace_id(), &job_id)
        .await?
        .ok_or_else(|| {
            ApiError::new(StatusCode::NOT_FOUND, "NotFoundException", "job not found")
        })?;
    require_manage_for_table(&state, &principal, &job.table_id).await?;

    let cancelled = maintenance::cancel_job(
        &state.pool,
        tenancy::default_workspace_id(),
        &job_id,
        &principal.audit_string(),
    )
    .await
    .map_err(|e| match e {
        MeridianError::Conflict(message) => {
            ApiError::new(StatusCode::CONFLICT, "ConflictException", message)
        }
        MeridianError::NotFound(message) => {
            ApiError::new(StatusCode::NOT_FOUND, "NotFoundException", message)
        }
        other => ApiError::from(other),
    })?;
    Ok(Json(cancelled.into()))
}

/// Request body for a manual job trigger.
#[derive(Debug, Deserialize)]
pub struct TriggerRequest {
    /// The table to act on, by name.
    pub warehouse: String,
    /// Dotted namespace.
    pub namespace: String,
    /// Table name.
    pub table: String,
    /// Job type: `compaction` (default) or `expire_snapshots`.
    #[serde(default)]
    pub job_type: Option<String>,
    /// Plan-only: run the executor's dry-run (compaction only), staging
    /// nothing. Recorded in the job spec; the worker honors it.
    #[serde(default)]
    pub dry_run: bool,
}

/// `POST /api/v2/maintenance/jobs` — manually enqueue a maintenance job on a
/// table. Requires `MANAGE_NAMESPACE` on the table's namespace.
pub async fn trigger_job(
    State(state): State<AppState>,
    Extension(principal): Extension<Principal>,
    Json(request): Json<TriggerRequest>,
) -> Result<(StatusCode, Json<JobResponse>), ApiError> {
    let warehouse = resolve_warehouse(&state.pool, &request.warehouse).await?;
    let levels = parse_dotted_namespace(&request.namespace)?;
    let namespace = namespace::get(&state.pool, &warehouse.id, &levels)
        .await?
        .ok_or_else(|| {
            ApiError::no_such_namespace(format!("namespace {:?} does not exist", request.namespace))
        })?;
    let record = table::get(&state.pool, &namespace.id, &request.table)
        .await?
        .ok_or_else(|| {
            ApiError::no_such_table(format!(
                "table {:?} does not exist",
                display_ident(&levels, &request.table)
            ))
        })?;

    let chain = namespace_scope_chain(&state.pool, &warehouse.id, &levels).await?;
    require(
        &state.pool,
        &principal,
        Privilege::ManageNamespace,
        &SecurableScope::namespace(&warehouse.id, chain),
    )
    .await?;

    let job_type = match request.job_type.as_deref() {
        None | Some("compaction") => JobType::Compaction,
        Some("expire_snapshots") => JobType::ExpireSnapshots,
        Some(other) => {
            return Err(ApiError::bad_request(format!(
                "unsupported job_type {other:?}: expected \"compaction\" or \"expire_snapshots\""
            )));
        }
    };
    // dry_run only applies to compaction (expiry is metadata-only, no staging).
    let spec = json!({
        "reason": "manual",
        "dry_run": request.dry_run && matches!(job_type, JobType::Compaction),
    });

    let job = maintenance::enqueue_job(
        &state.pool,
        tenancy::default_workspace_id(),
        &record.id,
        job_type,
        None,
        &spec,
        &principal.audit_string(),
    )
    .await?;
    Ok((StatusCode::ACCEPTED, Json(job.into())))
}

// ===========================================================================
// Savings ledger
// ===========================================================================

/// A savings-ledger row as rendered by the API.
#[derive(Debug, Serialize, sqlx::FromRow)]
pub struct SavingsRowResponse {
    /// ULID of the ledger row.
    pub id: String,
    /// The job that produced the savings.
    pub job_id: String,
    /// Table id (denormalized; survives the table's drop).
    pub table_id: String,
    /// Human-readable table identity at the time of the job.
    pub table_ident: String,
    /// Accounting period (first-of-month, UTC).
    pub period: NaiveDate,
    /// Data bytes before/after.
    pub bytes_before: i64,
    /// Data bytes after.
    pub bytes_after: i64,
    /// Data files before.
    pub files_before: i64,
    /// Data files after.
    pub files_after: i64,
    /// Bytes saved (before - after; may be negative honestly).
    pub bytes_saved: i64,
    /// Files removed.
    pub files_removed: i64,
    /// Projected GET requests avoided.
    pub est_get_requests_saved: i64,
    /// How the numbers were derived.
    pub methodology: String,
    /// When the row was written.
    pub created_at: DateTime<Utc>,
}

/// Query params for the ledger list.
#[derive(Debug, Deserialize)]
pub struct SavingsQuery {
    /// Filter by table id.
    #[serde(default)]
    pub table_id: Option<String>,
    /// Page size (default 50, max 500).
    #[serde(default)]
    pub limit: Option<i64>,
}

/// `GET /api/v2/maintenance/savings` — the per-job savings ledger, newest
/// first. Management-gated (spans the workspace).
pub async fn list_savings(
    State(state): State<AppState>,
    Extension(principal): Extension<Principal>,
    Query(query): Query<SavingsQuery>,
) -> Result<Json<Value>, ApiError> {
    require_management(&state.pool, &principal).await?;
    let limit = query.limit.unwrap_or(DEFAULT_LIMIT).clamp(1, MAX_LIMIT);
    let rows = query_savings(&state.pool, query.table_id.as_deref(), limit).await?;
    Ok(Json(json!({ "savings": rows })))
}

/// `GET /api/v2/maintenance/savings/rollup` — the monthly savings roll-up.
pub async fn savings_rollup(
    State(state): State<AppState>,
    Extension(principal): Extension<Principal>,
    Query(query): Query<RollupQuery>,
) -> Result<Json<Value>, ApiError> {
    require_management(&state.pool, &principal).await?;
    let months = query
        .months
        .unwrap_or(DEFAULT_ROLLUP_MONTHS)
        .clamp(1, MAX_ROLLUP_MONTHS);
    let rollup =
        maintenance::monthly_rollup(&state.pool, tenancy::default_workspace_id(), months).await?;
    let periods: Vec<Value> = rollup
        .into_iter()
        .map(|r| {
            json!({
                "period": r.period,
                "job_count": r.job_count,
                "bytes_saved": r.bytes_saved,
                "files_removed": r.files_removed,
                "est_get_requests_saved": r.est_get_requests_saved,
            })
        })
        .collect();
    Ok(Json(json!({ "rollup": periods })))
}

/// Query params for the rollup endpoint.
#[derive(Debug, Deserialize)]
pub struct RollupQuery {
    /// Number of months to return (default 12, max 120).
    #[serde(default)]
    pub months: Option<i64>,
}

// ===========================================================================
// Fleet health summary
// ===========================================================================

/// `GET /api/v2/warehouses/{w}/health-summary` — the per-warehouse fleet
/// overview: table count, score distribution, and the worst tables by score.
/// Requires management access (it aggregates every table under the
/// warehouse).
pub async fn warehouse_health_summary(
    State(state): State<AppState>,
    Extension(principal): Extension<Principal>,
    Path(warehouse_name): Path<String>,
) -> Result<Json<Value>, ApiError> {
    require_management(&state.pool, &principal).await?;
    let warehouse = resolve_warehouse(&state.pool, &warehouse_name).await?;

    // Aggregate over the newest health snapshot per table under the warehouse.
    let summary: Option<HealthSummaryRow> = sqlx::query_as(
        "WITH latest AS (
             SELECT DISTINCT ON (h.table_id) h.table_id, h.score, h.total_bytes,
                    h.data_file_count, h.small_file_ratio
             FROM health_snapshots h
             JOIN tables t ON t.id = h.table_id
             JOIN namespaces n ON n.id = t.namespace_id
             WHERE n.warehouse_id = $1
             ORDER BY h.table_id, h.computed_at DESC
         )
         SELECT
             COUNT(*)::bigint AS tables_scored,
             COALESCE(AVG(score), 0)::double precision AS avg_score,
             COALESCE(MIN(score), 0)::int AS min_score,
             COUNT(*) FILTER (WHERE score < 50)::bigint AS unhealthy_count,
             COUNT(*) FILTER (WHERE score >= 50 AND score < 80)::bigint AS degraded_count,
             COUNT(*) FILTER (WHERE score >= 80)::bigint AS healthy_count,
             COALESCE(SUM(total_bytes), 0)::bigint AS total_bytes,
             COALESCE(SUM(data_file_count), 0)::bigint AS total_data_files
         FROM latest",
    )
    .bind(&warehouse.id)
    .fetch_optional(&state.pool)
    .await
    .map_err(|e| MeridianError::internal("failed to summarize warehouse health", e))?;

    // The worst-scoring tables (up to 10), with their identity for the UI.
    let worst: Vec<WorstTableRow> = sqlx::query_as(
        "WITH latest AS (
             SELECT DISTINCT ON (h.table_id) h.table_id, h.score, h.small_file_ratio,
                    h.snapshot_count, h.data_file_count
             FROM health_snapshots h
             JOIN tables t ON t.id = h.table_id
             JOIN namespaces n ON n.id = t.namespace_id
             WHERE n.warehouse_id = $1
             ORDER BY h.table_id, h.computed_at DESC
         )
         SELECT l.table_id, l.score, l.small_file_ratio, l.snapshot_count, l.data_file_count,
                n.levels, t.name
         FROM latest l
         JOIN tables t ON t.id = l.table_id
         JOIN namespaces n ON n.id = t.namespace_id
         ORDER BY l.score ASC
         LIMIT 10",
    )
    .bind(&warehouse.id)
    .fetch_all(&state.pool)
    .await
    .map_err(|e| MeridianError::internal("failed to list worst tables", e))?;

    let s = summary.unwrap_or_default();
    let worst_tables: Vec<Value> = worst
        .into_iter()
        .map(|w| {
            let ident = if w.levels.is_empty() {
                w.name.clone()
            } else {
                format!("{}.{}", w.levels.join("."), w.name)
            };
            json!({
                "table_id": w.table_id,
                "table_ident": ident,
                "namespace": w.levels,
                "name": w.name,
                "score": w.score,
                "small_file_ratio": w.small_file_ratio,
                "snapshot_count": w.snapshot_count,
                "data_file_count": w.data_file_count,
            })
        })
        .collect();

    Ok(Json(json!({
        "warehouse": warehouse.name,
        "tables_scored": s.tables_scored,
        "avg_score": s.avg_score,
        "min_score": s.min_score,
        "healthy_count": s.healthy_count,
        "degraded_count": s.degraded_count,
        "unhealthy_count": s.unhealthy_count,
        "total_bytes": s.total_bytes,
        "total_data_files": s.total_data_files,
        "worst_tables": worst_tables,
    })))
}

#[derive(Debug, Default, sqlx::FromRow)]
struct HealthSummaryRow {
    tables_scored: i64,
    avg_score: f64,
    min_score: i32,
    unhealthy_count: i64,
    degraded_count: i64,
    healthy_count: i64,
    total_bytes: i64,
    total_data_files: i64,
}

#[derive(Debug, sqlx::FromRow)]
struct WorstTableRow {
    table_id: String,
    score: i16,
    small_file_ratio: f64,
    snapshot_count: i32,
    data_file_count: i64,
    levels: Vec<String>,
    name: String,
}

// ===========================================================================
// Store queries (no typed store accessor exists for these list shapes)
// ===========================================================================

/// Lists every maintenance policy in the default workspace, newest first.
async fn query_policies(pool: &sqlx::PgPool) -> Result<Vec<PolicyRecord>, ApiError> {
    let rows: Vec<PolicyQueryRow> = sqlx::query_as(
        "SELECT id, workspace_id, scope, scope_id, target_file_size_bytes, min_input_files,
                snapshot_retention_count, snapshot_retention_age_ms, max_staleness_ms, schedule,
                window_start, window_end, cost_cap_usd_month, exclusions, enabled, created_by,
                created_at, updated_at
         FROM maintenance_policies
         WHERE workspace_id = $1
         ORDER BY created_at DESC",
    )
    .bind(tenancy::default_workspace_id().to_string())
    .fetch_all(pool)
    .await
    .map_err(|e| MeridianError::internal("failed to list policies", e))?;
    Ok(rows.into_iter().map(PolicyQueryRow::into_record).collect())
}

/// Lists maintenance jobs with optional state/table filters, newest first.
async fn query_jobs(
    pool: &sqlx::PgPool,
    state: Option<&str>,
    table_id: Option<&str>,
    limit: i64,
) -> Result<Vec<JobRecord>, ApiError> {
    let rows: Vec<JobQueryRow> = sqlx::query_as(
        "SELECT id, workspace_id, table_id, job_type, state, policy_id, spec, created_by,
                claimed_by, attempts, error, result, created_at, started_at, finished_at
         FROM maintenance_jobs
         WHERE workspace_id = $1
           AND ($2::text IS NULL OR state = $2)
           AND ($3::text IS NULL OR table_id = $3)
         ORDER BY created_at DESC
         LIMIT $4",
    )
    .bind(tenancy::default_workspace_id().to_string())
    .bind(state)
    .bind(table_id)
    .bind(limit)
    .fetch_all(pool)
    .await
    .map_err(|e| MeridianError::internal("failed to list jobs", e))?;
    rows.into_iter()
        .map(JobQueryRow::into_record)
        .collect::<Result<_, _>>()
        .map_err(ApiError::from)
}

/// Lists savings-ledger rows with an optional table filter, newest first.
async fn query_savings(
    pool: &sqlx::PgPool,
    table_id: Option<&str>,
    limit: i64,
) -> Result<Vec<SavingsRowResponse>, ApiError> {
    sqlx::query_as::<_, SavingsRowResponse>(
        "SELECT id, job_id, table_id, table_ident, period, bytes_before, bytes_after,
                files_before, files_after, bytes_saved, files_removed, est_get_requests_saved,
                methodology, created_at
         FROM savings_ledger
         WHERE workspace_id = $1 AND ($2::text IS NULL OR table_id = $2)
         ORDER BY created_at DESC
         LIMIT $3",
    )
    .bind(tenancy::default_workspace_id().to_string())
    .bind(table_id)
    .bind(limit)
    .fetch_all(pool)
    .await
    .map_err(|e| MeridianError::internal("failed to list savings", e).into())
}

/// Resolves a table id to its namespace scope chain and enforces
/// `MANAGE_NAMESPACE` on it (for job cancel).
async fn require_manage_for_table(
    state: &AppState,
    principal: &Principal,
    table_id: &str,
) -> Result<(), ApiError> {
    let row: Option<(String, Vec<String>)> = sqlx::query_as(
        "SELECT n.warehouse_id, n.levels
         FROM tables t JOIN namespaces n ON n.id = t.namespace_id
         WHERE t.id = $1",
    )
    .bind(table_id)
    .fetch_optional(&state.pool)
    .await
    .map_err(|e| MeridianError::internal("failed to resolve table for authorization", e))?;
    let Some((warehouse_id, levels)) = row else {
        return Err(ApiError::new(
            StatusCode::NOT_FOUND,
            "NotFoundException",
            "the job's table no longer exists",
        ));
    };
    let chain = namespace_scope_chain(&state.pool, &warehouse_id, &levels).await?;
    require(
        &state.pool,
        principal,
        Privilege::ManageNamespace,
        &SecurableScope::namespace(&warehouse_id, chain),
    )
    .await
}

/// Builds a human-readable scope label for a policy record (best-effort:
/// falls back to the raw scope id if the referenced object was removed).
async fn policy_scope_label(pool: &sqlx::PgPool, record: &PolicyRecord) -> String {
    match record.scope {
        Scope::Warehouse => {
            let name: Option<String> =
                sqlx::query_scalar("SELECT name FROM warehouses WHERE id = $1")
                    .bind(&record.scope_id)
                    .fetch_optional(pool)
                    .await
                    .ok()
                    .flatten();
            name.map_or_else(
                || format!("warehouse:{}", record.scope_id),
                |n| format!("warehouse:{n}"),
            )
        }
        Scope::Namespace => {
            let row: Option<(Vec<String>, String)> = sqlx::query_as(
                "SELECT n.levels, w.name FROM namespaces n JOIN warehouses w ON w.id = n.warehouse_id
                 WHERE n.id = $1",
            )
            .bind(&record.scope_id)
            .fetch_optional(pool)
            .await
            .ok()
            .flatten();
            row.map_or_else(
                || format!("namespace:{}", record.scope_id),
                |(levels, wh)| format!("{wh}/{}", levels.join(".")),
            )
        }
        Scope::Table => {
            let row: Option<(String, Vec<String>, String)> = sqlx::query_as(
                "SELECT t.name, n.levels, w.name
                 FROM tables t JOIN namespaces n ON n.id = t.namespace_id
                 JOIN warehouses w ON w.id = n.warehouse_id
                 WHERE t.id = $1",
            )
            .bind(&record.scope_id)
            .fetch_optional(pool)
            .await
            .ok()
            .flatten();
            row.map_or_else(
                || format!("table:{}", record.scope_id),
                |(name, levels, wh)| format!("{wh}/{}.{name}", levels.join(".")),
            )
        }
    }
}

// ---- row → record mappers (mirror the store's private FromRow shapes) -----

#[derive(sqlx::FromRow)]
struct PolicyQueryRow {
    id: String,
    workspace_id: String,
    scope: String,
    scope_id: String,
    target_file_size_bytes: i64,
    min_input_files: i32,
    snapshot_retention_count: i32,
    snapshot_retention_age_ms: i64,
    max_staleness_ms: Option<i64>,
    schedule: Option<String>,
    window_start: Option<String>,
    window_end: Option<String>,
    cost_cap_usd_month: Option<f64>,
    exclusions: Value,
    enabled: bool,
    created_by: String,
    created_at: DateTime<Utc>,
    updated_at: DateTime<Utc>,
}

impl PolicyQueryRow {
    fn into_record(self) -> PolicyRecord {
        PolicyRecord {
            id: self.id,
            workspace_id: self.workspace_id,
            // scope was written by the store from a validated CHECK; an
            // unexpected value defaults to warehouse rather than erroring a
            // whole list. (The store's own parse is stricter on the hot path.)
            scope: match self.scope.as_str() {
                "table" => Scope::Table,
                "namespace" => Scope::Namespace,
                _ => Scope::Warehouse,
            },
            scope_id: self.scope_id,
            spec: PolicySpec {
                target_file_size_bytes: self.target_file_size_bytes,
                min_input_files: self.min_input_files,
                snapshot_retention_count: self.snapshot_retention_count,
                snapshot_retention_age_ms: self.snapshot_retention_age_ms,
                max_staleness_ms: self.max_staleness_ms,
                schedule: self.schedule,
                window_start: self.window_start,
                window_end: self.window_end,
                cost_cap_usd_month: self.cost_cap_usd_month,
                exclusions: self.exclusions,
                enabled: self.enabled,
            },
            created_by: self.created_by,
            created_at: self.created_at,
            updated_at: self.updated_at,
        }
    }
}

#[derive(sqlx::FromRow)]
struct JobQueryRow {
    id: String,
    workspace_id: String,
    table_id: String,
    job_type: String,
    state: String,
    policy_id: Option<String>,
    spec: Value,
    created_by: String,
    claimed_by: Option<String>,
    attempts: i32,
    error: Option<Value>,
    result: Option<Value>,
    created_at: DateTime<Utc>,
    started_at: Option<DateTime<Utc>>,
    finished_at: Option<DateTime<Utc>>,
}

impl JobQueryRow {
    fn into_record(self) -> Result<JobRecord, MeridianError> {
        Ok(JobRecord {
            id: self.id,
            workspace_id: self.workspace_id,
            table_id: self.table_id,
            job_type: parse_job_type(&self.job_type)
                .ok_or_else(|| MeridianError::internal_msg("job has an unknown type"))?,
            state: parse_job_state(&self.state)
                .ok_or_else(|| MeridianError::internal_msg("job has an unknown state"))?,
            policy_id: self.policy_id,
            spec: self.spec,
            created_by: self.created_by,
            claimed_by: self.claimed_by,
            attempts: self.attempts,
            error: self.error,
            result: self.result,
            created_at: self.created_at,
            started_at: self.started_at,
            finished_at: self.finished_at,
        })
    }
}

// ---- small helpers --------------------------------------------------------

/// Human-readable `ns.ns.table` identifier.
pub(crate) fn display_ident(levels: &[String], name: &str) -> String {
    if levels.is_empty() {
        name.to_owned()
    } else {
        format!("{}.{name}", levels.join("."))
    }
}

/// Parses a stored `job_type` string into the enum (the store's own parser is
/// private; this route needs the same mapping for its list/filter shapes).
fn parse_job_type(raw: &str) -> Option<JobType> {
    match raw {
        "compaction" => Some(JobType::Compaction),
        "expire_snapshots" => Some(JobType::ExpireSnapshots),
        "remove_orphans" => Some(JobType::RemoveOrphans),
        "rewrite_manifests" => Some(JobType::RewriteManifests),
        _ => None,
    }
}

/// Parses a stored/queried `state` string into [`JobState`].
fn parse_job_state(raw: &str) -> Option<JobState> {
    match raw {
        "queued" => Some(JobState::Queued),
        "running" => Some(JobState::Running),
        "succeeded" => Some(JobState::Succeeded),
        "failed" => Some(JobState::Failed),
        "cancelled" => Some(JobState::Cancelled),
        _ => None,
    }
}

/// Parses a dotted namespace string into levels, rejecting empty input.
fn parse_dotted_namespace(dotted: &str) -> Result<Vec<String>, ApiError> {
    let levels: Vec<String> = dotted
        .split('.')
        .map(str::trim)
        .filter(|s| !s.is_empty())
        .map(str::to_owned)
        .collect();
    if levels.is_empty() {
        return Err(ApiError::bad_request("namespace must not be empty"));
    }
    Ok(levels)
}
