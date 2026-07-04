//! Data-contracts management API (Pillar E / E-F3, E-F4), mounted under
//! `/api/v2/quality`. The control plane for the circuit breaker the commit
//! driver runs (`crate::routes::tables`, the pre-commit hook):
//!
//! - **Contracts** (`/contracts`): versioned contract objects bound to a table
//!   or a namespace, with a `mode` (warn | quarantine | block) and a typed
//!   spec (schema-evolution rules + cheap synchronous predicates). CRUD +
//!   version history.
//! - **Per-table status** (`/tables/{warehouse}/{ns}/{table}/contracts`): the
//!   contracts in force on a table — what a producer sees (E-F3).
//! - **Violations** (`/violations`): the ledger the circuit breaker writes,
//!   filterable by contract and table.
//! - **Quarantine** (`/tables/.../quarantine/{snapshot}/{publish|discard}`):
//!   publish (fast-forward `main` to the quarantined snapshot) or discard (drop
//!   the quarantine branch). Both go through the commit path — publishing a
//!   quarantined snapshot is itself a fully-audited, invariant-preserving
//!   commit.
//!
//! # Authorization
//!
//! Every route is **management-gated** (`require_management`: `admin` role or
//! any `MANAGE_WAREHOUSE` grant) — the same gate governance and maintenance
//! policy mutations use. This matches the "management or a govern/quality
//! privilege" requirement; `require_management` is the govern/quality gate
//! today (a dedicated RBAC privilege would need a migration to the 0005
//! privilege CHECK, and contracts are a cross-resource surface — a namespace
//! contract spans many tables).
//!
//! # Validation
//!
//! A contract's spec is a `meridian_store::contracts::ContractSpec`. It is
//! parsed and structurally validated at write time (unknown modes / bindings
//! are 400s), so a malformed contract is rejected at the API, not discovered as
//! a silent no-op in the commit hook.

use axum::extract::{Path, Query, State};
use axum::http::StatusCode;
use axum::{Extension, Json};
use chrono::{DateTime, Utc};
use meridian_common::principal::Principal;
use meridian_iceberg::commit::{CommitBackend, PointerCas};
use meridian_iceberg::spec::{RefType, SnapshotRef, TableMetadata};
use meridian_storage::{new_metadata_location, read_table_metadata};
use meridian_store::commit::{
    CommitTableOp, DerivedTableState, PostgresCommitBackend, SnapshotIndexRow,
};
use meridian_store::contracts::{
    self, BoundTo, ContractSpec, ContractUpdate, EnforcementMode, NewContract,
};
use meridian_store::incidents::{self, IncidentQuery, IncidentStatus};
use meridian_store::monitors::{
    self, MonitorConfig, MonitorKind, MonitorUpdate, NewMonitor, Severity,
};
use meridian_store::quality_score;
use meridian_store::{namespace, table, tenancy, warehouse};
use serde::{Deserialize, Serialize};
use serde_json::{Value, json};
use uuid::Uuid;

use crate::AppState;
use crate::error::ApiError;
use crate::routes::grants::require_management;
use crate::routes::namespaces::{decode_namespace_param, resolve_warehouse};

// ===========================================================================
// Shared helpers
// ===========================================================================

/// Maps store not-found / conflict / validation onto generic (non-IRC) types.
fn store_error(error: meridian_common::MeridianError) -> ApiError {
    match error {
        meridian_common::MeridianError::NotFound(m) => {
            ApiError::new(StatusCode::NOT_FOUND, "NotFoundException", m)
        }
        meridian_common::MeridianError::Conflict(m) => ApiError::already_exists(m),
        meridian_common::MeridianError::Validation(m) => ApiError::bad_request(m),
        other => ApiError::from(other),
    }
}

/// Resolves a table by name to `(warehouse record, table record)`. 404 on any
/// missing part.
async fn resolve_table(
    state: &AppState,
    warehouse_name: &str,
    dotted_namespace: &str,
    table_name: &str,
) -> Result<(warehouse::WarehouseRecord, table::TableRecord), ApiError> {
    let wh = resolve_warehouse(&state.pool, warehouse_name).await?;
    let levels = decode_namespace_param(dotted_namespace)?;
    let record = table::get_by_name(&state.pool, &wh.id, &levels, table_name)
        .await?
        .ok_or_else(|| {
            ApiError::no_such_table(format!(
                "table {warehouse_name}/{dotted_namespace}/{table_name} does not exist"
            ))
        })?;
    Ok((wh, record))
}

// ===========================================================================
// Contract wire shapes
// ===========================================================================

/// A contract as rendered by the API.
#[derive(Debug, Serialize)]
pub struct ContractResponse {
    /// ULID of the contract.
    pub id: String,
    /// Human name.
    pub name: String,
    /// What it binds to: `table` | `namespace`.
    pub bound_to: String,
    /// The bound securable's id.
    pub securable_id: String,
    /// Current version.
    pub version: i32,
    /// Whether in force.
    pub enabled: bool,
    /// Mode: `warn` | `quarantine` | `block`.
    pub mode: String,
    /// The typed spec.
    pub spec: ContractSpec,
    /// The Iceberg branch a quarantined commit is retargeted onto.
    pub quarantine_branch: String,
    /// Creating principal.
    pub created_by: String,
    /// Creation time.
    pub created_at: DateTime<Utc>,
    /// Last-update time.
    pub updated_at: DateTime<Utc>,
}

impl From<contracts::Contract> for ContractResponse {
    fn from(c: contracts::Contract) -> Self {
        Self {
            id: c.id,
            name: c.name,
            bound_to: c.bound_to.as_str().to_owned(),
            securable_id: c.securable_id,
            version: c.version,
            enabled: c.enabled,
            mode: c.mode.as_str().to_owned(),
            spec: c.spec,
            quarantine_branch: c.quarantine_branch,
            created_by: c.created_by,
            created_at: c.created_at,
            updated_at: c.updated_at,
        }
    }
}

/// Request body to create a contract. The binding is given by name (table or
/// namespace under a warehouse) and resolved to a securable id server-side.
#[derive(Debug, Deserialize)]
pub struct CreateContractRequest {
    /// Human name, unique per workspace.
    pub name: String,
    /// The warehouse the bound securable lives in.
    pub warehouse: String,
    /// What to bind to: `table` | `namespace`.
    pub bound_to: String,
    /// The dotted namespace (the namespace itself for a namespace binding, or
    /// the table's namespace for a table binding).
    pub namespace: String,
    /// The table name — required for a `table` binding, ignored for
    /// `namespace`.
    #[serde(default)]
    pub table: Option<String>,
    /// Mode: `warn` | `quarantine` | `block`. Defaults to `warn`.
    #[serde(default)]
    pub mode: Option<String>,
    /// The typed spec.
    pub spec: ContractSpec,
    /// The quarantine branch (defaults to `meridian_quarantine`).
    #[serde(default)]
    pub quarantine_branch: Option<String>,
}

/// Parses a mode string (defaulting to warn), 400 on an unknown value.
fn parse_mode(raw: Option<&str>) -> Result<EnforcementMode, ApiError> {
    match raw {
        None => Ok(EnforcementMode::Warn),
        Some(s) => EnforcementMode::parse(s).ok_or_else(|| {
            ApiError::bad_request(format!(
                "invalid contract mode {s:?}: expected warn, quarantine, or block"
            ))
        }),
    }
}

/// Resolves the `(bound_to, securable_id)` for a create request.
async fn resolve_binding(
    state: &AppState,
    req: &CreateContractRequest,
) -> Result<(BoundTo, String), ApiError> {
    let bound_to = BoundTo::parse(&req.bound_to).ok_or_else(|| {
        ApiError::bad_request(format!(
            "invalid bound_to {:?}: expected table or namespace",
            req.bound_to
        ))
    })?;
    let wh = resolve_warehouse(&state.pool, &req.warehouse).await?;
    let levels = decode_namespace_param(&req.namespace)?;
    let securable_id = match bound_to {
        BoundTo::Namespace => {
            namespace::get(&state.pool, &wh.id, &levels)
                .await?
                .ok_or_else(|| {
                    ApiError::no_such_namespace(format!(
                        "namespace {:?} does not exist in {}",
                        req.namespace, req.warehouse
                    ))
                })?
                .id
        }
        BoundTo::Table => {
            let name = req
                .table
                .as_deref()
                .ok_or_else(|| ApiError::bad_request("a table binding requires a 'table' name"))?;
            table::get_by_name(&state.pool, &wh.id, &levels, name)
                .await?
                .ok_or_else(|| {
                    ApiError::no_such_table(format!(
                        "table {}/{}/{name} does not exist",
                        req.warehouse, req.namespace
                    ))
                })?
                .id
        }
    };
    Ok((bound_to, securable_id))
}

// ===========================================================================
// Contract CRUD
// ===========================================================================

/// `GET /api/v2/quality/contracts` — list all contracts.
pub async fn list_contracts(
    State(state): State<AppState>,
    Extension(principal): Extension<Principal>,
) -> Result<Json<Value>, ApiError> {
    require_management(&state.pool, &principal).await?;
    let items = contracts::list(&state.pool, tenancy::default_workspace_id())
        .await
        .map_err(store_error)?;
    let out: Vec<ContractResponse> = items.into_iter().map(ContractResponse::from).collect();
    Ok(Json(json!({ "contracts": out })))
}

/// `POST /api/v2/quality/contracts` — create a contract.
pub async fn create_contract(
    State(state): State<AppState>,
    Extension(principal): Extension<Principal>,
    Json(req): Json<CreateContractRequest>,
) -> Result<(StatusCode, Json<ContractResponse>), ApiError> {
    require_management(&state.pool, &principal).await?;
    let mode = parse_mode(req.mode.as_deref())?;
    let (bound_to, securable_id) = resolve_binding(&state, &req).await?;
    let branch = req
        .quarantine_branch
        .as_deref()
        .filter(|b| !b.trim().is_empty())
        .unwrap_or(contracts::DEFAULT_QUARANTINE_BRANCH);

    let contract = contracts::create(
        &state.pool,
        tenancy::default_workspace_id(),
        NewContract {
            name: &req.name,
            bound_to,
            securable_id: &securable_id,
            mode,
            spec: &req.spec,
            quarantine_branch: branch,
        },
        &principal.audit_string(),
    )
    .await
    .map_err(store_error)?;
    Ok((StatusCode::CREATED, Json(contract.into())))
}

/// `GET /api/v2/quality/contracts/{id}` — one contract.
pub async fn get_contract(
    State(state): State<AppState>,
    Extension(principal): Extension<Principal>,
    Path(id): Path<String>,
) -> Result<Json<ContractResponse>, ApiError> {
    require_management(&state.pool, &principal).await?;
    let contract = contracts::get(&state.pool, tenancy::default_workspace_id(), &id)
        .await
        .map_err(store_error)?
        .ok_or_else(|| {
            ApiError::new(
                StatusCode::NOT_FOUND,
                "NotFoundException",
                format!("contract {id:?} does not exist"),
            )
        })?;
    Ok(Json(contract.into()))
}

/// Request body to update a contract. `None` fields are unchanged. The binding
/// is fixed at creation (a different binding is a different contract).
#[derive(Debug, Deserialize)]
pub struct UpdateContractRequest {
    /// New spec, if changing.
    #[serde(default)]
    pub spec: Option<ContractSpec>,
    /// New mode, if changing.
    #[serde(default)]
    pub mode: Option<String>,
    /// New enabled flag, if changing.
    #[serde(default)]
    pub enabled: Option<bool>,
    /// New quarantine branch, if changing.
    #[serde(default)]
    pub quarantine_branch: Option<String>,
}

/// `PATCH /api/v2/quality/contracts/{id}` — update a contract (new version).
pub async fn update_contract(
    State(state): State<AppState>,
    Extension(principal): Extension<Principal>,
    Path(id): Path<String>,
    Json(req): Json<UpdateContractRequest>,
) -> Result<Json<ContractResponse>, ApiError> {
    require_management(&state.pool, &principal).await?;
    let mode = match req.mode.as_deref() {
        None => None,
        Some(s) => Some(EnforcementMode::parse(s).ok_or_else(|| {
            ApiError::bad_request(format!(
                "invalid contract mode {s:?}: expected warn, quarantine, or block"
            ))
        })?),
    };
    let contract = contracts::update(
        &state.pool,
        tenancy::default_workspace_id(),
        &id,
        ContractUpdate {
            spec: req.spec,
            mode,
            enabled: req.enabled,
            quarantine_branch: req.quarantine_branch,
        },
        &principal.audit_string(),
    )
    .await
    .map_err(store_error)?;
    Ok(Json(contract.into()))
}

/// `DELETE /api/v2/quality/contracts/{id}` — delete a contract.
pub async fn delete_contract(
    State(state): State<AppState>,
    Extension(principal): Extension<Principal>,
    Path(id): Path<String>,
) -> Result<StatusCode, ApiError> {
    require_management(&state.pool, &principal).await?;
    contracts::delete(
        &state.pool,
        tenancy::default_workspace_id(),
        &id,
        &principal.audit_string(),
    )
    .await
    .map_err(store_error)?;
    Ok(StatusCode::NO_CONTENT)
}

/// `GET /api/v2/quality/contracts/{id}/versions` — version history.
pub async fn list_contract_versions(
    State(state): State<AppState>,
    Extension(principal): Extension<Principal>,
    Path(id): Path<String>,
) -> Result<Json<Value>, ApiError> {
    require_management(&state.pool, &principal).await?;
    let versions = contracts::versions(&state.pool, tenancy::default_workspace_id(), &id)
        .await
        .map_err(store_error)?;
    let out: Vec<Value> = versions
        .into_iter()
        .map(|v| {
            json!({
                "version": v.version,
                "mode": v.mode.as_str(),
                "enabled": v.enabled,
                "spec": v.spec,
                "created_by": v.created_by,
                "created_at": v.created_at,
            })
        })
        .collect();
    Ok(Json(json!({ "versions": out })))
}

// ===========================================================================
// Per-table contract status (E-F3: producers see the contracts in force)
// ===========================================================================

/// `GET /api/v2/quality/tables/{warehouse}/{namespace}/{table}/contracts` —
/// the contracts in force on a table (directly bound + namespace-bound).
pub async fn table_contracts(
    State(state): State<AppState>,
    Extension(principal): Extension<Principal>,
    Path((warehouse_name, dotted_namespace, table_name)): Path<(String, String, String)>,
) -> Result<Json<Value>, ApiError> {
    require_management(&state.pool, &principal).await?;
    let (wh, record) =
        resolve_table(&state, &warehouse_name, &dotted_namespace, &table_name).await?;
    let levels = decode_namespace_param(&dotted_namespace)?;
    let chain = crate::routes::grants::namespace_scope_chain(&state.pool, &wh.id, &levels).await?;
    let items = contracts::resolve_for_table(
        &state.pool,
        tenancy::default_workspace_id(),
        &record.id,
        &chain,
    )
    .await
    .map_err(store_error)?;
    let out: Vec<ContractResponse> = items.into_iter().map(ContractResponse::from).collect();
    Ok(Json(json!({
        "table": record.id,
        "contracts": out,
    })))
}

// ===========================================================================
// Violations query
// ===========================================================================

/// Query parameters for the violations list.
#[derive(Debug, Deserialize)]
pub struct ViolationQueryParams {
    /// Restrict to one contract.
    #[serde(default)]
    pub contract_id: Option<String>,
    /// Restrict to one table id.
    #[serde(default)]
    pub table_id: Option<String>,
    /// Max rows (default 100, capped at 1000).
    #[serde(default)]
    pub limit: Option<i64>,
}

/// `GET /api/v2/quality/violations` — the violation ledger, newest first.
pub async fn list_violations(
    State(state): State<AppState>,
    Extension(principal): Extension<Principal>,
    Query(params): Query<ViolationQueryParams>,
) -> Result<Json<Value>, ApiError> {
    require_management(&state.pool, &principal).await?;
    let limit = params.limit.unwrap_or(100).clamp(1, 1000);
    let violations = contracts::list_violations(
        &state.pool,
        tenancy::default_workspace_id(),
        &contracts::ViolationQuery {
            contract_id: params.contract_id.as_deref(),
            table_id: params.table_id.as_deref(),
        },
        limit,
    )
    .await
    .map_err(store_error)?;
    let out: Vec<Value> = violations
        .into_iter()
        .map(|v| {
            json!({
                "id": v.id,
                "contract_id": v.contract_id,
                "table_id": v.table_id,
                "snapshot_id": v.snapshot_id,
                "kind": v.kind,
                "detail": v.detail,
                "commit_rejected": v.commit_rejected,
                "quarantined": v.quarantined,
                "occurred_at": v.occurred_at,
            })
        })
        .collect();
    Ok(Json(json!({ "violations": out })))
}

// ===========================================================================
// Quarantine publish / discard
// ===========================================================================

/// Derives the write-through index state from a metadata document (mirrors the
/// commit driver's `derived_state`; kept local so this module does not depend
/// on `tables`' private helpers).
fn derived_state(metadata: &TableMetadata) -> DerivedTableState {
    let current = metadata.current_snapshot_id.filter(|id| *id >= 0);
    let snapshots: Vec<SnapshotIndexRow> = metadata
        .snapshots
        .iter()
        .flatten()
        .map(|snapshot| SnapshotIndexRow {
            snapshot_id: snapshot.snapshot_id,
            parent_snapshot_id: snapshot.parent_snapshot_id,
            sequence_number: snapshot.sequence_number,
            timestamp_ms: snapshot.timestamp_ms,
            manifest_list: snapshot.manifest_list.clone(),
            operation: snapshot
                .summary
                .as_ref()
                .and_then(|s| s.get("operation").cloned()),
            summary: json!(snapshot.summary.clone().unwrap_or_default()),
            is_current: current == Some(snapshot.snapshot_id),
        })
        .collect();
    DerivedTableState {
        format_version: i16::from(metadata.format_version),
        properties: metadata.properties.clone().unwrap_or_default(),
        event_details: json!({
            "snapshot_count": snapshots.len(),
            "current_snapshot_id": current,
        }),
        snapshots,
        schema_text: metadata
            .current_schema()
            .map(meridian_store::search::schema_search_text),
    }
}

/// The action a quarantine resolution performs.
#[derive(Debug, Clone, Copy)]
enum QuarantineAction {
    /// Fast-forward `main` to the quarantined snapshot and drop the branch.
    Publish,
    /// Drop the quarantine branch, leaving `main` where it is.
    Discard,
}

/// `POST /api/v2/quality/tables/{warehouse}/{ns}/{table}/quarantine/{snapshot}/publish`
pub async fn publish_quarantine(
    state: State<AppState>,
    principal: Extension<Principal>,
    path: Path<(String, String, String, i64)>,
) -> Result<Json<Value>, ApiError> {
    resolve_quarantine(state, principal, path, QuarantineAction::Publish).await
}

/// `POST /api/v2/quality/tables/{warehouse}/{ns}/{table}/quarantine/{snapshot}/discard`
pub async fn discard_quarantine(
    state: State<AppState>,
    principal: Extension<Principal>,
    path: Path<(String, String, String, i64)>,
) -> Result<Json<Value>, ApiError> {
    resolve_quarantine(state, principal, path, QuarantineAction::Discard).await
}

/// Shared publish/discard driver. Both are ordinary catalog commits through
/// [`PostgresCommitBackend::commit_tables`] — so resolving a quarantine is
/// itself fully audited and invariant-preserving (design doc §3.3):
///
/// - **publish**: set `refs["main"]` + `current_snapshot_id` to the quarantined
///   snapshot (which already exists in `snapshots`), and drop the quarantine
///   branch ref.
/// - **discard**: drop the quarantine branch ref; `main` is untouched.
async fn resolve_quarantine(
    State(state): State<AppState>,
    Extension(principal): Extension<Principal>,
    Path((warehouse_name, dotted_namespace, table_name, snapshot_id)): Path<(
        String,
        String,
        String,
        i64,
    )>,
    action: QuarantineAction,
) -> Result<Json<Value>, ApiError> {
    require_management(&state.pool, &principal).await?;
    let (wh, record) =
        resolve_table(&state, &warehouse_name, &dotted_namespace, &table_name).await?;
    let storage = crate::routes::tables::connect_storage(&wh)?;

    let backend = PostgresCommitBackend::new(
        state.pool.clone(),
        tenancy::default_workspace_id(),
        principal.audit_string(),
    );
    let pointer = backend.load_pointer(&record.id).await.map_err(|e| {
        ApiError::new(
            StatusCode::INTERNAL_SERVER_ERROR,
            "InternalServerError",
            format!("failed to load table pointer: {e}"),
        )
    })?;
    let mut metadata = read_table_metadata(storage.as_ref(), &pointer.metadata_location)
        .await
        .map_err(|e| {
            ApiError::new(
                StatusCode::INTERNAL_SERVER_ERROR,
                "InternalServerError",
                format!("failed to read current metadata: {e}"),
            )
        })?;

    // The snapshot must be quarantined: present in the metadata but not the
    // current `main` head.
    if metadata.snapshot_by_id(snapshot_id).is_none() {
        return Err(ApiError::no_such_table(format!(
            "snapshot {snapshot_id} is not retained on table {table_name}"
        )));
    }
    if metadata.current_snapshot_id == Some(snapshot_id) {
        return Err(ApiError::bad_request(format!(
            "snapshot {snapshot_id} is already the current (main) snapshot; nothing to resolve"
        )));
    }

    // Find the quarantine branch ref that points at this snapshot (a ref that
    // is not `main`). We drop it in both actions.
    let branch_name = metadata
        .refs
        .as_ref()
        .and_then(|refs| {
            refs.iter().find_map(|(name, r)| {
                (name != "main" && r.snapshot_id == snapshot_id).then(|| name.clone())
            })
        })
        .ok_or_else(|| {
            ApiError::bad_request(format!(
                "snapshot {snapshot_id} is not on a quarantine branch of table {table_name}"
            ))
        })?;

    apply_quarantine_action(&mut metadata, snapshot_id, &branch_name, action);

    // Stage the new metadata and CAS the pointer — a normal commit.
    let staged = new_metadata_location(&metadata.location, pointer.version + 1, Uuid::new_v4());
    meridian_storage::write_table_metadata(storage.as_ref(), &staged, &metadata)
        .await
        .map_err(|e| {
            ApiError::new(
                StatusCode::INTERNAL_SERVER_ERROR,
                "InternalServerError",
                format!("failed to stage resolved metadata: {e}"),
            )
        })?;

    let op = CommitTableOp {
        cas: PointerCas {
            table: record.id.clone(),
            expected_version: pointer.version,
            new_metadata_location: staged.clone(),
        },
        derived: Some(derived_state(&metadata)),
        contract_violation: None,
    };
    match backend.commit_tables(std::slice::from_ref(&op), None).await {
        Ok(_) => Ok(Json(json!({
            "table": record.id,
            "action": match action { QuarantineAction::Publish => "publish", QuarantineAction::Discard => "discard" },
            "snapshot_id": snapshot_id,
            "branch": branch_name,
            "metadata_location": staged,
            "current_snapshot_id": metadata.current_snapshot_id,
        }))),
        Err(error) => {
            // Best-effort discard of the staged file (a lost CAS/other error
            // leaves it orphaned, never referenced).
            if let Err(e) = storage.delete(&staged).await {
                tracing::warn!(location = %staged, error = %e, "failed to discard orphaned staged file");
            }
            Err(ApiError::commit_failed(format!(
                "failed to resolve quarantine (retry): {error}"
            )))
        }
    }
}

/// Applies the ref mutation for a quarantine action to `metadata` in place.
fn apply_quarantine_action(
    metadata: &mut TableMetadata,
    snapshot_id: i64,
    branch_name: &str,
    action: QuarantineAction,
) {
    let refs = metadata
        .refs
        .get_or_insert_with(std::collections::BTreeMap::new);
    // Both actions drop the quarantine branch.
    refs.remove(branch_name);
    if matches!(action, QuarantineAction::Publish) {
        // Fast-forward main to the quarantined snapshot.
        refs.insert(
            "main".to_owned(),
            SnapshotRef {
                snapshot_id,
                ref_type: RefType::Branch,
                min_snapshots_to_keep: None,
                max_snapshot_age_ms: None,
                max_ref_age_ms: None,
                extra: serde_json::Map::new(),
            },
        );
        metadata.current_snapshot_id = Some(snapshot_id);
    }
}

// ===========================================================================
// Monitors (E-F1): zero-scan monitor CRUD + results
// ===========================================================================

/// A monitor as rendered by the API.
#[derive(Debug, Serialize)]
pub struct MonitorResponse {
    /// ULID of the monitor.
    pub id: String,
    /// Human name.
    pub name: String,
    /// What it binds to: `table` | `namespace`.
    pub bound_to: String,
    /// The bound securable's id.
    pub securable_id: String,
    /// The zero-scan signal.
    pub kind: String,
    /// Whether in force.
    pub enabled: bool,
    /// Severity of incidents this monitor opens.
    pub severity: String,
    /// The typed config.
    pub config: MonitorConfig,
    /// Creating principal.
    pub created_by: String,
    /// Creation time.
    pub created_at: DateTime<Utc>,
    /// Last-update time.
    pub updated_at: DateTime<Utc>,
}

impl From<monitors::Monitor> for MonitorResponse {
    fn from(m: monitors::Monitor) -> Self {
        Self {
            id: m.id,
            name: m.name,
            bound_to: m.bound_to.as_str().to_owned(),
            securable_id: m.securable_id,
            kind: m.kind.as_str().to_owned(),
            enabled: m.enabled,
            severity: m.severity.as_str().to_owned(),
            config: m.config,
            created_by: m.created_by,
            created_at: m.created_at,
            updated_at: m.updated_at,
        }
    }
}

/// Request body to create a monitor. The binding is given by name and resolved
/// to a securable id server-side, exactly like a contract.
#[derive(Debug, Deserialize)]
pub struct CreateMonitorRequest {
    /// Human name, unique per workspace.
    pub name: String,
    /// The warehouse the bound securable lives in.
    pub warehouse: String,
    /// What to bind to: `table` | `namespace`.
    pub bound_to: String,
    /// The dotted namespace (the namespace itself, or the table's namespace).
    pub namespace: String,
    /// The table name — required for a `table` binding.
    #[serde(default)]
    pub table: Option<String>,
    /// The zero-scan signal.
    pub kind: String,
    /// Severity of incidents this monitor opens. Defaults to `medium`.
    #[serde(default)]
    pub severity: Option<String>,
    /// The typed config (defaults applied when omitted).
    #[serde(default)]
    pub config: MonitorConfig,
}

/// Parses a monitor kind, 400 on an unknown value.
fn parse_kind(raw: &str) -> Result<MonitorKind, ApiError> {
    MonitorKind::parse(raw).ok_or_else(|| {
        ApiError::bad_request(format!(
            "invalid monitor kind {raw:?}: expected freshness, volume, schema_change, \
             file_size, snapshot_debt, or commit_failure"
        ))
    })
}

/// Parses a severity (defaulting to medium), 400 on an unknown value.
fn parse_severity(raw: Option<&str>) -> Result<Severity, ApiError> {
    match raw {
        None => Ok(Severity::Medium),
        Some(s) => Severity::parse(s).ok_or_else(|| {
            ApiError::bad_request(format!(
                "invalid severity {s:?}: expected low, medium, or high"
            ))
        }),
    }
}

/// Resolves the `(bound_to, securable_id)` for a monitor create request.
async fn resolve_monitor_binding(
    state: &AppState,
    req: &CreateMonitorRequest,
) -> Result<(monitors::BoundTo, String), ApiError> {
    let bound_to = monitors::BoundTo::parse(&req.bound_to).ok_or_else(|| {
        ApiError::bad_request(format!(
            "invalid bound_to {:?}: expected table or namespace",
            req.bound_to
        ))
    })?;
    let wh = resolve_warehouse(&state.pool, &req.warehouse).await?;
    let levels = decode_namespace_param(&req.namespace)?;
    let securable_id = match bound_to {
        monitors::BoundTo::Namespace => {
            namespace::get(&state.pool, &wh.id, &levels)
                .await?
                .ok_or_else(|| {
                    ApiError::no_such_namespace(format!(
                        "namespace {:?} does not exist in {}",
                        req.namespace, req.warehouse
                    ))
                })?
                .id
        }
        monitors::BoundTo::Table => {
            let name = req
                .table
                .as_deref()
                .ok_or_else(|| ApiError::bad_request("a table binding requires a 'table' name"))?;
            table::get_by_name(&state.pool, &wh.id, &levels, name)
                .await?
                .ok_or_else(|| {
                    ApiError::no_such_table(format!(
                        "table {}/{}/{name} does not exist",
                        req.warehouse, req.namespace
                    ))
                })?
                .id
        }
    };
    Ok((bound_to, securable_id))
}

/// `GET /api/v2/quality/monitors` — list all monitors.
pub async fn list_monitors(
    State(state): State<AppState>,
    Extension(principal): Extension<Principal>,
) -> Result<Json<Value>, ApiError> {
    require_management(&state.pool, &principal).await?;
    let items = monitors::list(&state.pool, tenancy::default_workspace_id())
        .await
        .map_err(store_error)?;
    let out: Vec<MonitorResponse> = items.into_iter().map(MonitorResponse::from).collect();
    Ok(Json(json!({ "monitors": out })))
}

/// `POST /api/v2/quality/monitors` — create a monitor.
pub async fn create_monitor(
    State(state): State<AppState>,
    Extension(principal): Extension<Principal>,
    Json(req): Json<CreateMonitorRequest>,
) -> Result<(StatusCode, Json<MonitorResponse>), ApiError> {
    require_management(&state.pool, &principal).await?;
    let kind = parse_kind(&req.kind)?;
    let severity = parse_severity(req.severity.as_deref())?;
    let (bound_to, securable_id) = resolve_monitor_binding(&state, &req).await?;

    let monitor = monitors::create(
        &state.pool,
        tenancy::default_workspace_id(),
        NewMonitor {
            name: &req.name,
            bound_to,
            securable_id: &securable_id,
            kind,
            severity,
            config: &req.config,
        },
        &principal.audit_string(),
    )
    .await
    .map_err(store_error)?;
    Ok((StatusCode::CREATED, Json(monitor.into())))
}

/// `GET /api/v2/quality/monitors/{id}` — one monitor.
pub async fn get_monitor(
    State(state): State<AppState>,
    Extension(principal): Extension<Principal>,
    Path(id): Path<String>,
) -> Result<Json<MonitorResponse>, ApiError> {
    require_management(&state.pool, &principal).await?;
    let monitor = monitors::get(&state.pool, tenancy::default_workspace_id(), &id)
        .await
        .map_err(store_error)?
        .ok_or_else(|| {
            ApiError::new(
                StatusCode::NOT_FOUND,
                "NotFoundException",
                format!("monitor {id:?} does not exist"),
            )
        })?;
    Ok(Json(monitor.into()))
}

/// Request body to update a monitor. `None` fields are unchanged; the binding
/// and kind are fixed at creation.
#[derive(Debug, Deserialize)]
pub struct UpdateMonitorRequest {
    /// New enabled flag, if changing.
    #[serde(default)]
    pub enabled: Option<bool>,
    /// New severity, if changing.
    #[serde(default)]
    pub severity: Option<String>,
    /// New config, if changing.
    #[serde(default)]
    pub config: Option<MonitorConfig>,
}

/// `PATCH /api/v2/quality/monitors/{id}` — update a monitor.
pub async fn update_monitor(
    State(state): State<AppState>,
    Extension(principal): Extension<Principal>,
    Path(id): Path<String>,
    Json(req): Json<UpdateMonitorRequest>,
) -> Result<Json<MonitorResponse>, ApiError> {
    require_management(&state.pool, &principal).await?;
    let severity = match req.severity.as_deref() {
        None => None,
        Some(s) => Some(parse_severity(Some(s))?),
    };
    let monitor = monitors::update(
        &state.pool,
        tenancy::default_workspace_id(),
        &id,
        MonitorUpdate {
            enabled: req.enabled,
            severity,
            config: req.config,
        },
        &principal.audit_string(),
    )
    .await
    .map_err(store_error)?;
    Ok(Json(monitor.into()))
}

/// `DELETE /api/v2/quality/monitors/{id}` — delete a monitor.
pub async fn delete_monitor(
    State(state): State<AppState>,
    Extension(principal): Extension<Principal>,
    Path(id): Path<String>,
) -> Result<StatusCode, ApiError> {
    require_management(&state.pool, &principal).await?;
    monitors::delete(
        &state.pool,
        tenancy::default_workspace_id(),
        &id,
        &principal.audit_string(),
    )
    .await
    .map_err(store_error)?;
    Ok(StatusCode::NO_CONTENT)
}

/// Query parameters for the monitor-results list.
#[derive(Debug, Deserialize)]
pub struct ResultQueryParams {
    /// Restrict to one monitor.
    #[serde(default)]
    pub monitor_id: Option<String>,
    /// Restrict to one table id.
    #[serde(default)]
    pub table_id: Option<String>,
    /// Max rows (default 100, capped at 1000).
    #[serde(default)]
    pub limit: Option<i64>,
}

/// `GET /api/v2/quality/monitors/results` — the evaluation series, newest first.
pub async fn list_monitor_results(
    State(state): State<AppState>,
    Extension(principal): Extension<Principal>,
    Query(params): Query<ResultQueryParams>,
) -> Result<Json<Value>, ApiError> {
    require_management(&state.pool, &principal).await?;
    let limit = params.limit.unwrap_or(100).clamp(1, 1000);
    let results = monitors::list_results(
        &state.pool,
        tenancy::default_workspace_id(),
        &monitors::ResultQuery {
            monitor_id: params.monitor_id.as_deref(),
            table_id: params.table_id.as_deref(),
        },
        limit,
    )
    .await
    .map_err(store_error)?;
    let out: Vec<Value> = results
        .into_iter()
        .map(|r| {
            json!({
                "id": r.id,
                "monitor_id": r.monitor_id,
                "table_id": r.table_id,
                "kind": r.kind,
                "status": r.status.as_str(),
                "observed_value": r.observed_value,
                "baseline_value": r.baseline_value,
                "result_kind": r.result_kind,
                "detail": r.detail,
                "snapshot_id": r.snapshot_id,
                "evaluated_at": r.evaluated_at,
            })
        })
        .collect();
    Ok(Json(json!({ "results": out })))
}

// ===========================================================================
// Incidents (E-F5): list / get / acknowledge / resolve
// ===========================================================================

/// Renders one incident as a JSON value.
fn incident_json(i: &incidents::Incident) -> Value {
    json!({
        "id": i.id,
        "table_id": i.table_id,
        "table_ident": i.table_ident,
        "source": i.source.as_str(),
        "kind": i.kind,
        "status": i.status.as_str(),
        "severity": i.severity.as_str(),
        "title": i.title,
        "detail": i.detail,
        "owner": i.owner,
        "blast_radius": i.blast_radius,
        "monitor_id": i.monitor_id,
        "occurrence_count": i.occurrence_count,
        "acknowledged_by": i.acknowledged_by,
        "acknowledged_at": i.acknowledged_at,
        "resolved_by": i.resolved_by,
        "resolved_at": i.resolved_at,
        "first_seen_at": i.first_seen_at,
        "last_seen_at": i.last_seen_at,
    })
}

/// Query parameters for the incidents list.
#[derive(Debug, Deserialize)]
pub struct IncidentQueryParams {
    /// Restrict to one table id.
    #[serde(default)]
    pub table_id: Option<String>,
    /// Restrict to one status: `open` | `acknowledged` | `resolved`.
    #[serde(default)]
    pub status: Option<String>,
    /// When true, return only live (open + acknowledged) incidents.
    #[serde(default)]
    pub live: bool,
    /// Max rows (default 100, capped at 1000).
    #[serde(default)]
    pub limit: Option<i64>,
}

/// `GET /api/v2/quality/incidents` — the incident ledger, newest first.
pub async fn list_incidents(
    State(state): State<AppState>,
    Extension(principal): Extension<Principal>,
    Query(params): Query<IncidentQueryParams>,
) -> Result<Json<Value>, ApiError> {
    require_management(&state.pool, &principal).await?;
    let status = match params.status.as_deref() {
        None => None,
        Some(s) => Some(IncidentStatus::parse(s).ok_or_else(|| {
            ApiError::bad_request(format!(
                "invalid status {s:?}: expected open, acknowledged, or resolved"
            ))
        })?),
    };
    let limit = params.limit.unwrap_or(100).clamp(1, 1000);
    let items = incidents::list(
        &state.pool,
        tenancy::default_workspace_id(),
        &IncidentQuery {
            table_id: params.table_id.as_deref(),
            status,
            live_only: params.live,
        },
        limit,
    )
    .await
    .map_err(store_error)?;
    let out: Vec<Value> = items.iter().map(incident_json).collect();
    Ok(Json(json!({ "incidents": out })))
}

/// `GET /api/v2/quality/incidents/{id}` — one incident.
pub async fn get_incident(
    State(state): State<AppState>,
    Extension(principal): Extension<Principal>,
    Path(id): Path<String>,
) -> Result<Json<Value>, ApiError> {
    require_management(&state.pool, &principal).await?;
    let incident = incidents::get(&state.pool, tenancy::default_workspace_id(), &id)
        .await
        .map_err(store_error)?
        .ok_or_else(|| {
            ApiError::new(
                StatusCode::NOT_FOUND,
                "NotFoundException",
                format!("incident {id:?} does not exist"),
            )
        })?;
    Ok(Json(incident_json(&incident)))
}

/// `POST /api/v2/quality/incidents/{id}/ack` — acknowledge an open incident.
pub async fn acknowledge_incident(
    State(state): State<AppState>,
    Extension(principal): Extension<Principal>,
    Path(id): Path<String>,
) -> Result<Json<Value>, ApiError> {
    require_management(&state.pool, &principal).await?;
    let incident = incidents::acknowledge(
        &state.pool,
        tenancy::default_workspace_id(),
        &id,
        &principal.audit_string(),
    )
    .await
    .map_err(store_error)?;
    Ok(Json(incident_json(&incident)))
}

/// `POST /api/v2/quality/incidents/{id}/resolve` — resolve an incident.
pub async fn resolve_incident(
    State(state): State<AppState>,
    Extension(principal): Extension<Principal>,
    Path(id): Path<String>,
) -> Result<Json<Value>, ApiError> {
    require_management(&state.pool, &principal).await?;
    let incident = incidents::resolve(
        &state.pool,
        tenancy::default_workspace_id(),
        &id,
        &principal.audit_string(),
    )
    .await
    .map_err(store_error)?;
    Ok(Json(incident_json(&incident)))
}

// ===========================================================================
// Per-table status + quality score (E-F5, E-F6)
// ===========================================================================

/// Resolves a table's self-and-ancestors namespace chain (for namespace-bound
/// monitor/contract resolution in the score + status reads).
async fn namespace_chain_for(
    state: &AppState,
    wh: &warehouse::WarehouseRecord,
    dotted_namespace: &str,
) -> Result<Vec<String>, ApiError> {
    let levels = decode_namespace_param(dotted_namespace)?;
    crate::routes::grants::namespace_scope_chain(&state.pool, &wh.id, &levels).await
}

/// `GET /api/v2/quality/tables/{warehouse}/{ns}/{table}/status` — the table's
/// traffic-light status (worst live incident severity) + live-incident counts.
pub async fn table_status(
    State(state): State<AppState>,
    Extension(principal): Extension<Principal>,
    Path((warehouse_name, dotted_namespace, table_name)): Path<(String, String, String)>,
) -> Result<Json<Value>, ApiError> {
    require_management(&state.pool, &principal).await?;
    let (_wh, record) =
        resolve_table(&state, &warehouse_name, &dotted_namespace, &table_name).await?;
    let status = incidents::table_status(&state.pool, tenancy::default_workspace_id(), &record.id)
        .await
        .map_err(store_error)?;
    Ok(Json(json!({
        "table_id": status.table_id,
        "ident": format!("{warehouse_name}.{dotted_namespace}.{table_name}"),
        "status": status.light.as_str(),
        "live_incidents": status.live_incidents,
        "high": status.high,
        "medium": status.medium,
        "low": status.low,
    })))
}

/// `GET /api/v2/quality/tables/{warehouse}/{ns}/{table}/status/history` — the
/// table's recent status history (incident open/resolve points).
pub async fn table_status_history(
    State(state): State<AppState>,
    Extension(principal): Extension<Principal>,
    Path((warehouse_name, dotted_namespace, table_name)): Path<(String, String, String)>,
) -> Result<Json<Value>, ApiError> {
    require_management(&state.pool, &principal).await?;
    let (_wh, record) =
        resolve_table(&state, &warehouse_name, &dotted_namespace, &table_name).await?;
    let history = incidents::table_status_history(
        &state.pool,
        tenancy::default_workspace_id(),
        &record.id,
        200,
    )
    .await
    .map_err(store_error)?;
    let out: Vec<Value> = history
        .into_iter()
        .map(|e| {
            json!({
                "incident_id": e.incident_id,
                "event": e.event,
                "severity": e.severity,
                "kind": e.kind,
                "at": e.at,
            })
        })
        .collect();
    Ok(Json(json!({ "table_id": record.id, "history": out })))
}

/// `GET /api/v2/quality/tables/{warehouse}/{ns}/{table}/score` — the composite
/// quality / trust score (E-F6), with its explaining components.
pub async fn table_quality_score(
    State(state): State<AppState>,
    Extension(principal): Extension<Principal>,
    Path((warehouse_name, dotted_namespace, table_name)): Path<(String, String, String)>,
) -> Result<Json<Value>, ApiError> {
    require_management(&state.pool, &principal).await?;
    let (wh, record) =
        resolve_table(&state, &warehouse_name, &dotted_namespace, &table_name).await?;
    let chain = namespace_chain_for(&state, &wh, &dotted_namespace).await?;
    let score = quality_score::score_table(
        &state.pool,
        tenancy::default_workspace_id(),
        &record.id,
        &chain,
    )
    .await
    .map_err(store_error)?;
    let mut body = score.to_json();
    if let Value::Object(map) = &mut body {
        map.insert("table_id".to_owned(), json!(record.id));
        map.insert(
            "ident".to_owned(),
            json!(format!("{warehouse_name}.{dotted_namespace}.{table_name}")),
        );
    }
    Ok(Json(body))
}
