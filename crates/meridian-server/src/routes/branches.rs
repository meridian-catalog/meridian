//! Management API for catalog-level branches & tags (Pillar K).
//!
//! A *catalog branch* is a named overlay of the per-table pointer map (K-F1);
//! a *tag* is an immutable frozen pointer set. Branches are projected as their
//! own IRC catalog via the `warehouse@branch` prefix (K-F2, resolved in
//! `crate::routes::namespaces` and served by the table endpoints) — so these
//! endpoints manage the branch lifecycle and the operations that are not a
//! plain table read/write: create, list, diff, the merge gate, merge, and the
//! ephemeral-branch sweep.
//!
//! Authorization: every endpoint requires management access (the same bar as
//! warehouse/mirror/contract management), since a branch spans namespaces and a
//! merge advances main. See `crate::routes::grants::require_management`.
//!
//! Commit-invariant note: nothing here moves a pointer directly. A merge
//! fast-forwards main *through the commit path* (`fast_forward_main`, which is
//! an ordinary main CAS); branch commits happen on the IRC surface via
//! `commit_branch_table`. See `docs/design/branching.md`.

use std::collections::BTreeMap;

use axum::extract::{Path, State};
use axum::http::StatusCode;
use axum::{Extension, Json};
use meridian_common::principal::Principal;
use meridian_iceberg::spec::TableMetadata;
use meridian_iceberg::spec::schema::Schema;
use meridian_storage::read_table_metadata;
use meridian_store::branches::{self, BranchRecord, DivergedPointer, NewBranch};
use meridian_store::warehouse::WarehouseRecord;
use meridian_store::{contracts, namespace, tenancy};
use serde::{Deserialize, Serialize};
use serde_json::{Value, json};

use crate::AppState;
use crate::error::ApiError;
use crate::routes::grants::require_management;
use crate::routes::tables::{connect_storage, current_metadata_unreadable, derived_state};

/// Longest accepted branch/tag name.
const MAX_NAME_LEN: usize = 100;

// ---------------------------------------------------------------------------
// Wire types
// ---------------------------------------------------------------------------

/// Request body for `POST /api/v2/branches`.
#[derive(Debug, Deserialize)]
pub struct CreateBranchRequest {
    /// Branch name, unique per workspace across branches and tags.
    pub name: String,
    /// The ref to diverge from (`main` or another branch name). Default `main`.
    #[serde(default = "default_base")]
    pub base_ref: String,
    /// The warehouse the branch's namespaces live in (required when
    /// `namespaces` is set — namespace levels are resolved within it).
    #[serde(default)]
    pub warehouse: Option<String>,
    /// Namespaces (dotted levels) the branch spans; empty/absent = all.
    #[serde(default)]
    pub namespaces: Vec<String>,
    /// Ephemeral expiry in seconds from now (a PR environment, K-F3). Absent =
    /// permanent.
    #[serde(default)]
    pub expires_in_s: Option<i64>,
}

fn default_base() -> String {
    branches::MAIN_REF.to_owned()
}

/// Request body for `POST /api/v2/tags`.
#[derive(Debug, Deserialize)]
pub struct CreateTagRequest {
    /// Tag name, unique per workspace across branches and tags.
    pub name: String,
    /// The ref to freeze (`main` or a branch name). Default `main`.
    #[serde(default = "default_base")]
    pub from_ref: String,
}

/// A branch/tag as rendered by the management API.
#[derive(Debug, Serialize)]
pub struct BranchResponse {
    /// ULID.
    pub id: String,
    /// Name.
    pub name: String,
    /// `branch` or `tag`.
    pub kind: String,
    /// The ref this diverged/froze from.
    pub base_ref: String,
    /// `open` | `merged` | `deleted`.
    pub state: String,
    /// Whether it spans all namespaces.
    pub scope_all: bool,
    /// Ephemeral expiry (RFC3339), when set.
    pub expires_at: Option<String>,
    /// Tables diverged on the branch (0 for a fresh branch or a tag).
    pub diverged_tables: i64,
    /// Creator.
    pub created_by: String,
    /// Creation time (RFC3339).
    pub created_at: String,
}

impl BranchResponse {
    async fn from_record(pool: &sqlx::PgPool, record: BranchRecord) -> Result<Self, ApiError> {
        let diverged = if record.is_tag() {
            0
        } else {
            branches::diverged_count(pool, &record.id).await?
        };
        Ok(Self {
            id: record.id,
            name: record.name,
            kind: record.kind,
            base_ref: record.base_ref,
            state: record.state,
            scope_all: record.scope_all,
            expires_at: record.expires_at.map(|t| t.to_rfc3339()),
            diverged_tables: diverged,
            created_by: record.created_by,
            created_at: record.created_at.to_rfc3339(),
        })
    }
}

// ---------------------------------------------------------------------------
// Branch CRUD
// ---------------------------------------------------------------------------

/// `POST /api/v2/branches` — create a branch (K-F1).
pub async fn create_branch(
    State(state): State<AppState>,
    Extension(principal): Extension<Principal>,
    Json(request): Json<CreateBranchRequest>,
) -> Result<(StatusCode, Json<BranchResponse>), ApiError> {
    require_management(&state.pool, &principal).await?;
    validate_name(&request.name)?;
    validate_base_ref(&state, &request.base_ref).await?;

    let scope_all = request.namespaces.is_empty();
    let expires_at = request
        .expires_in_s
        .map(|s| chrono::Utc::now() + chrono::Duration::seconds(s.max(1)));

    // Resolve the base branch id when base is a branch (not main).
    let base_branch_id = if request.base_ref == branches::MAIN_REF {
        None
    } else {
        branches::get_by_name(
            &state.pool,
            tenancy::default_workspace_id(),
            &request.base_ref,
        )
        .await?
        .map(|b| b.id)
    };

    let record = branches::create(
        &state.pool,
        tenancy::default_workspace_id(),
        &NewBranch {
            name: &request.name,
            kind: "branch",
            base_ref: &request.base_ref,
            base_branch_id: base_branch_id.as_deref(),
            scope_all,
            expires_at,
        },
        &principal.audit_string(),
    )
    .await
    .map_err(ApiError::from)?;

    // Record namespace scoping when not scope_all.
    if !scope_all {
        let warehouse = require_warehouse(&state, request.warehouse.as_deref())?;
        let wh = super::namespaces::resolve_warehouse(&state.pool, warehouse).await?;
        for dotted in &request.namespaces {
            let levels = decode_dotted(dotted);
            let ns = namespace::get(&state.pool, &wh.id, &levels)
                .await?
                .ok_or_else(|| ApiError::no_such_namespace(format!("namespace {dotted:?}")))?;
            branches::add_scope_namespace(&state.pool, &record.id, &ns.id).await?;
        }
    }

    let body = BranchResponse::from_record(&state.pool, record).await?;
    Ok((StatusCode::CREATED, Json(body)))
}

/// `GET /api/v2/branches` — list branches and tags.
pub async fn list_branches(
    State(state): State<AppState>,
    Extension(principal): Extension<Principal>,
) -> Result<Json<Value>, ApiError> {
    require_management(&state.pool, &principal).await?;
    let records = branches::list(&state.pool, tenancy::default_workspace_id()).await?;
    let mut branch_out = Vec::new();
    let mut tag_out = Vec::new();
    for record in records {
        let is_tag = record.is_tag();
        let rendered = BranchResponse::from_record(&state.pool, record).await?;
        if is_tag {
            tag_out.push(rendered);
        } else {
            branch_out.push(rendered);
        }
    }
    Ok(Json(json!({ "branches": branch_out, "tags": tag_out })))
}

/// `GET /api/v2/branches/{name}` — get one branch/tag.
pub async fn get_branch(
    State(state): State<AppState>,
    Extension(principal): Extension<Principal>,
    Path(name): Path<String>,
) -> Result<Json<BranchResponse>, ApiError> {
    require_management(&state.pool, &principal).await?;
    let record = load_branch(&state, &name).await?;
    Ok(Json(
        BranchResponse::from_record(&state.pool, record).await?,
    ))
}

/// `DELETE /api/v2/branches/{name}` — delete a branch (K-F3 teardown).
pub async fn delete_branch(
    State(state): State<AppState>,
    Extension(principal): Extension<Principal>,
    Path(name): Path<String>,
) -> Result<StatusCode, ApiError> {
    require_management(&state.pool, &principal).await?;
    branches::delete(
        &state.pool,
        tenancy::default_workspace_id(),
        &name,
        &principal.audit_string(),
    )
    .await
    .map_err(ApiError::from)?;
    Ok(StatusCode::NO_CONTENT)
}

// ---------------------------------------------------------------------------
// Tags
// ---------------------------------------------------------------------------

/// `POST /api/v2/tags` — create an immutable tag (K-F1). Freezes the resolved
/// pointer of every diverged table on the source ref (plus, when the source is
/// `main`, nothing extra is captured beyond diverged — a `main` tag is a name
/// registry entry that resolves live main at read time). For a branch source,
/// the tag captures each diverged table's branch-head pointer so it is stable
/// even if the branch later advances.
pub async fn create_tag(
    State(state): State<AppState>,
    Extension(principal): Extension<Principal>,
    Json(request): Json<CreateTagRequest>,
) -> Result<(StatusCode, Json<BranchResponse>), ApiError> {
    require_management(&state.pool, &principal).await?;
    validate_name(&request.name)?;
    validate_base_ref(&state, &request.from_ref).await?;

    let source_branch = if request.from_ref == branches::MAIN_REF {
        None
    } else {
        Some(load_branch(&state, &request.from_ref).await?)
    };

    let record = branches::create(
        &state.pool,
        tenancy::default_workspace_id(),
        &NewBranch {
            name: &request.name,
            kind: "tag",
            base_ref: &request.from_ref,
            base_branch_id: source_branch.as_ref().map(|b| b.id.as_str()),
            scope_all: true,
            expires_at: None,
        },
        &principal.audit_string(),
    )
    .await
    .map_err(ApiError::from)?;

    // Freeze the source branch's diverged pointers into the tag (a main tag
    // needs no frozen rows — it resolves live main).
    if let Some(source) = &source_branch {
        for diverged in branches::diverged_pointers(&state.pool, &source.id).await? {
            let snapshot = head_snapshot_of(&state, &diverged.branch_metadata_location).await;
            branches::add_tag_pointer(
                &state.pool,
                &record.id,
                &diverged.table_id,
                &diverged.branch_metadata_location,
                snapshot,
            )
            .await?;
        }
    }

    let body = BranchResponse::from_record(&state.pool, record).await?;
    Ok((StatusCode::CREATED, Json(body)))
}

/// `GET /api/v2/tags` — list tags only.
pub async fn list_tags(
    State(state): State<AppState>,
    Extension(principal): Extension<Principal>,
) -> Result<Json<Value>, ApiError> {
    require_management(&state.pool, &principal).await?;
    let records = branches::list(&state.pool, tenancy::default_workspace_id()).await?;
    let mut tags = Vec::new();
    for record in records.into_iter().filter(BranchRecord::is_tag) {
        tags.push(BranchResponse::from_record(&state.pool, record).await?);
    }
    Ok(Json(json!({ "tags": tags })))
}

/// `DELETE /api/v2/tags/{name}` — delete a tag.
pub async fn delete_tag(
    State(state): State<AppState>,
    Extension(principal): Extension<Principal>,
    Path(name): Path<String>,
) -> Result<StatusCode, ApiError> {
    require_management(&state.pool, &principal).await?;
    branches::delete(
        &state.pool,
        tenancy::default_workspace_id(),
        &name,
        &principal.audit_string(),
    )
    .await
    .map_err(ApiError::from)?;
    Ok(StatusCode::NO_CONTENT)
}

// ---------------------------------------------------------------------------
// Diff (K-F1)
// ---------------------------------------------------------------------------

/// `GET /api/v2/branches/{name}/diff` — schema + snapshot + row-count delta of
/// a branch vs its base (main).
pub async fn diff_branch(
    State(state): State<AppState>,
    Extension(principal): Extension<Principal>,
    Path(name): Path<String>,
) -> Result<Json<Value>, ApiError> {
    require_management(&state.pool, &principal).await?;
    let branch = load_branch(&state, &name).await?;
    if branch.is_tag() {
        return Err(ApiError::bad_request("diff applies to branches, not tags"));
    }
    let diverged = branches::diverged_pointers(&state.pool, &branch.id).await?;

    let mut tables = Vec::new();
    for d in &diverged {
        let (warehouse, _) = load_warehouse_for_table(&state, &d.table_id).await?;
        let storage = connect_storage(&warehouse)?;
        let branch_md = read_table_metadata(storage.as_ref(), &d.branch_metadata_location)
            .await
            .map_err(|e| current_metadata_unreadable(&d.branch_metadata_location, &e))?;
        // Base (main) metadata, when the table exists on main.
        let base_md = match &d.main_metadata_location {
            Some(loc) => Some(
                read_table_metadata(storage.as_ref(), loc)
                    .await
                    .map_err(|e| current_metadata_unreadable(loc, &e))?,
            ),
            None => None,
        };
        tables.push(table_diff(d, base_md.as_ref(), &branch_md));
    }

    Ok(Json(json!({
        "branch": branch.name,
        "base": branch.base_ref,
        "diverged_table_count": diverged.len(),
        "tables": tables,
    })))
}

/// Builds one table's diff entry (schema + snapshot + row-count deltas).
fn table_diff(d: &DivergedPointer, base: Option<&TableMetadata>, branch: &TableMetadata) -> Value {
    let ident = format!("{}.{}", d.namespace_levels.join("."), d.table_name);
    let (added, dropped, changed) = match base.and_then(TableMetadata::current_schema) {
        Some(base_schema) => match branch.current_schema() {
            Some(branch_schema) => schema_delta(base_schema, branch_schema),
            None => (Vec::new(), Vec::new(), Vec::new()),
        },
        // The table is new on the branch (no main base): every branch column is
        // "added".
        None => branch.current_schema().map_or_else(
            || (Vec::new(), Vec::new(), Vec::new()),
            |s| {
                (
                    s.fields.iter().map(|f| f.name.clone()).collect(),
                    Vec::new(),
                    Vec::new(),
                )
            },
        ),
    };

    let base_snapshot = base.and_then(|m| m.current_snapshot_id.filter(|id| *id >= 0));
    let branch_snapshot = branch.current_snapshot_id.filter(|id| *id >= 0);
    let base_rows = base.and_then(total_records);
    let branch_rows = total_records(branch);
    let row_delta = match (base_rows, branch_rows) {
        (Some(b), Some(h)) => json!(h - b),
        _ => json!("unknown"),
    };

    json!({
        "table": ident,
        "schema": {
            "added_columns": added,
            "dropped_columns": dropped,
            "type_changed_columns": changed,
        },
        "snapshot": {
            "base_snapshot_id": base_snapshot,
            "branch_snapshot_id": branch_snapshot,
        },
        "rows": {
            "base": base_rows.map_or(json!("unknown"), |r| json!(r)),
            "branch": branch_rows.map_or(json!("unknown"), |r| json!(r)),
            "delta": row_delta,
        },
    })
}

/// Top-level column delta between two schemas, keyed by name: (added, dropped,
/// type-changed).
fn schema_delta(base: &Schema, branch: &Schema) -> (Vec<String>, Vec<String>, Vec<String>) {
    let base_types: BTreeMap<&str, Value> = base
        .fields
        .iter()
        .map(|f| (f.name.as_str(), json!(f.field_type)))
        .collect();
    let branch_types: BTreeMap<&str, Value> = branch
        .fields
        .iter()
        .map(|f| (f.name.as_str(), json!(f.field_type)))
        .collect();

    let mut added = Vec::new();
    let mut changed = Vec::new();
    for (name, ty) in &branch_types {
        match base_types.get(name) {
            None => added.push((*name).to_owned()),
            Some(base_ty) if *base_ty != *ty => changed.push((*name).to_owned()),
            Some(_) => {}
        }
    }
    let dropped: Vec<String> = base_types
        .keys()
        .filter(|name| !branch_types.contains_key(*name))
        .map(|name| (*name).to_owned())
        .collect();
    (added, dropped, changed)
}

/// `total-records` from the current snapshot summary, when present.
fn total_records(metadata: &TableMetadata) -> Option<i64> {
    metadata
        .current_snapshot()
        .and_then(|s| s.summary.as_ref())
        .and_then(|summary| summary.get("total-records"))
        .and_then(|v| v.parse::<i64>().ok())
}

// ---------------------------------------------------------------------------
// Merge gate (K-F3) and merge (K-F1)
// ---------------------------------------------------------------------------

/// One table's gate outcome.
#[derive(Debug, Serialize)]
struct GateEntry {
    table: String,
    contract: String,
    mode: String,
    violations: Vec<String>,
}

/// Evaluates the merge gate: every enabled contract bound to a diverged table
/// is checked against the branch head. Returns the blocking entries (block-mode
/// violations) and the warnings (warn/quarantine-mode violations) separately.
async fn evaluate_gate(
    state: &AppState,
    branch: &BranchRecord,
) -> Result<(Vec<GateEntry>, Vec<GateEntry>), ApiError> {
    let diverged = branches::diverged_pointers(&state.pool, &branch.id).await?;
    let mut blocking = Vec::new();
    let mut warnings = Vec::new();

    for d in &diverged {
        let namespace_ids = namespace_chain(state, &d.table_id).await?;
        let bound = contracts::resolve_for_table(
            &state.pool,
            tenancy::default_workspace_id(),
            &d.table_id,
            &namespace_ids,
        )
        .await
        .map_err(ApiError::from)?;
        if bound.is_empty() {
            continue;
        }

        let (warehouse, _) = load_warehouse_for_table(state, &d.table_id).await?;
        let storage = connect_storage(&warehouse)?;
        let branch_md = read_table_metadata(storage.as_ref(), &d.branch_metadata_location)
            .await
            .map_err(|e| current_metadata_unreadable(&d.branch_metadata_location, &e))?;
        // Compare against main's current head (the state the branch would merge
        // over) — the same base the circuit breaker uses at commit time.
        let base_md = match &d.main_metadata_location {
            Some(loc) => Some(
                read_table_metadata(storage.as_ref(), loc)
                    .await
                    .map_err(|e| current_metadata_unreadable(loc, &e))?,
            ),
            None => None,
        };
        let ident = format!("{}.{}", d.namespace_levels.join("."), d.table_name);

        let (Some(branch_schema), base_schema) = (
            branch_md.current_schema(),
            base_md.as_ref().and_then(TableMetadata::current_schema),
        ) else {
            continue;
        };
        // With no main base schema, evaluate against the branch schema itself
        // (an additive-only contract cannot be violated by a brand-new table).
        let base_schema = base_schema.unwrap_or(branch_schema);
        let summary = branch_md
            .current_snapshot()
            .and_then(|s| s.summary.as_ref());

        for contract in &bound {
            let violations = contract.spec.evaluate(base_schema, branch_schema, summary);
            if violations.is_empty() {
                continue;
            }
            let entry = GateEntry {
                table: ident.clone(),
                contract: contract.name.clone(),
                mode: contract.mode.as_str().to_owned(),
                violations: violations
                    .iter()
                    .map(|v| format!("{}: {}", v.kind, v.detail))
                    .collect(),
            };
            if contract.mode == contracts::EnforcementMode::Block {
                blocking.push(entry);
            } else {
                warnings.push(entry);
            }
        }
    }
    Ok((blocking, warnings))
}

/// `GET /api/v2/branches/{name}/gate` — the merge-gate result without merging
/// (a CI pre-check, K-F3).
pub async fn branch_gate(
    State(state): State<AppState>,
    Extension(principal): Extension<Principal>,
    Path(name): Path<String>,
) -> Result<Json<Value>, ApiError> {
    require_management(&state.pool, &principal).await?;
    let branch = load_branch(&state, &name).await?;
    if branch.is_tag() {
        return Err(ApiError::bad_request("gate applies to branches, not tags"));
    }
    let (blocking, warnings) = evaluate_gate(&state, &branch).await?;
    Ok(Json(json!({
        "branch": branch.name,
        "passes": blocking.is_empty(),
        "blocking": blocking,
        "warnings": warnings,
    })))
}

/// `POST /api/v2/branches/{name}/merge` — merge a branch into main (K-F1),
/// conflict- and gate-checked.
pub async fn merge_branch(
    State(state): State<AppState>,
    Extension(principal): Extension<Principal>,
    Path(name): Path<String>,
) -> Result<Json<Value>, ApiError> {
    require_management(&state.pool, &principal).await?;
    let branch = load_branch(&state, &name).await?;
    if branch.is_tag() {
        return Err(ApiError::bad_request("cannot merge a tag"));
    }
    if branch.base_ref != branches::MAIN_REF {
        return Err(ApiError::bad_request(
            "only branches based on main can be merged in this milestone",
        ));
    }

    let diverged = branches::diverged_pointers(&state.pool, &branch.id).await?;
    if diverged.is_empty() {
        return Err(ApiError::bad_request(
            "branch has no diverged tables to merge",
        ));
    }

    // Merge gate (K-F3): a block-mode contract violation on any table refuses
    // the merge before any pointer moves (fail-closed).
    let (blocking, warnings) = evaluate_gate(&state, &branch).await?;
    if !blocking.is_empty() {
        return Err(ApiError::commit_failed(format!(
            "merge gate failed: {} contract violation(s) block the merge; \
             fix the branch and retry — first: {}",
            blocking.len(),
            blocking
                .first()
                .map(|b| format!("{} on {}", b.contract, b.table))
                .unwrap_or_default()
        )));
    }

    // Conflict detection + apply (fast-forward main through the commit path).
    let merged = apply_merge(&state, &principal.audit_string(), &diverged).await?;

    branches::mark_merged(
        &state.pool,
        tenancy::default_workspace_id(),
        &branch.id,
        &principal.audit_string(),
    )
    .await?;

    Ok(Json(json!({
        "branch": branch.name,
        "merged_tables": merged,
        "warnings": warnings,
    })))
}

/// Detects table-level merge conflicts, then fast-forwards main per table
/// **through the commit path** (K-F1). A conflict (main advanced past the
/// divergence base) refuses the whole merge before any pointer moves; a
/// concurrent main commit that lands mid-apply makes that table's guard fail
/// and is reported as a race (no lost update). Returns the merged table idents.
async fn apply_merge(
    state: &AppState,
    principal_audit: &str,
    diverged: &[DivergedPointer],
) -> Result<Vec<String>, ApiError> {
    // Conflict detection (three-way): a table whose main pointer advanced past
    // the divergence base changed on both sides → conflict. Refuse atomically.
    let conflicts: Vec<String> = diverged
        .iter()
        .filter(|d| d.main_pointer_version != d.base_pointer_version)
        .map(|d| format!("{}.{}", d.namespace_levels.join("."), d.table_name))
        .collect();
    if !conflicts.is_empty() {
        return Err(ApiError::commit_failed(format!(
            "merge conflict: {} table(s) changed on both main and the branch \
             since divergence: {}",
            conflicts.len(),
            conflicts.join(", ")
        )));
    }

    let backend = crate::routes::tables::commit_backend_for(state, principal_audit);
    let mut merged = Vec::new();
    let mut raced = Vec::new();
    for d in diverged {
        let (warehouse, _) = load_warehouse_for_table(state, &d.table_id).await?;
        let storage = connect_storage(&warehouse)?;
        let branch_md = read_table_metadata(storage.as_ref(), &d.branch_metadata_location)
            .await
            .map_err(|e| current_metadata_unreadable(&d.branch_metadata_location, &e))?;
        let expected = u64::try_from(d.main_pointer_version).unwrap_or(u64::MAX);
        let ident = format!("{}.{}", d.namespace_levels.join("."), d.table_name);
        match backend
            .fast_forward_main(
                &d.table_id,
                expected,
                &d.branch_metadata_location,
                derived_state(&branch_md),
            )
            .await
        {
            Ok(_) => merged.push(ident),
            Err(meridian_iceberg::commit::CommitBackendError::VersionConflict { .. }) => {
                raced.push(ident);
            }
            Err(other) => return Err(crate::routes::tables::backend_to_api(other)),
        }
    }
    if !raced.is_empty() {
        return Err(ApiError::commit_failed(format!(
            "merge raced a concurrent main commit on {} table(s): {}; \
             {} table(s) merged before the race — refresh and re-merge the rest",
            raced.len(),
            raced.join(", "),
            merged.len()
        )));
    }
    Ok(merged)
}

/// `POST /api/v2/branches/sweep` — delete expired ephemeral branches (K-F3).
pub async fn sweep_branches(
    State(state): State<AppState>,
    Extension(principal): Extension<Principal>,
) -> Result<Json<Value>, ApiError> {
    require_management(&state.pool, &principal).await?;
    let names =
        branches::expired_open_branches(&state.pool, tenancy::default_workspace_id()).await?;
    let mut swept = Vec::new();
    for name in names {
        branches::delete(
            &state.pool,
            tenancy::default_workspace_id(),
            &name,
            &principal.audit_string(),
        )
        .await?;
        swept.push(name);
    }
    Ok(Json(json!({ "swept": swept })))
}

// ---------------------------------------------------------------------------
// Helpers
// ---------------------------------------------------------------------------

/// Loads a branch/tag by name or 404s.
async fn load_branch(state: &AppState, name: &str) -> Result<BranchRecord, ApiError> {
    branches::get_by_name(&state.pool, tenancy::default_workspace_id(), name)
        .await?
        .filter(|b| b.state != "deleted")
        .ok_or_else(|| {
            ApiError::new(
                StatusCode::NOT_FOUND,
                "NotFoundException",
                format!("branch or tag {name:?} does not exist"),
            )
        })
}

/// Validates a base ref names main or an existing branch.
async fn validate_base_ref(state: &AppState, base_ref: &str) -> Result<(), ApiError> {
    if base_ref == branches::MAIN_REF {
        return Ok(());
    }
    branches::get_by_name(&state.pool, tenancy::default_workspace_id(), base_ref)
        .await?
        .filter(|b| !b.is_tag() && b.state != "deleted")
        .ok_or_else(|| {
            ApiError::bad_request(format!("base ref {base_ref:?} is not a live branch"))
        })?;
    Ok(())
}

/// The namespace-id scope chain for a table (for contract resolution): the
/// table's own namespace plus its ancestors, the same chain the commit-time
/// circuit breaker uses.
async fn namespace_chain(state: &AppState, table_id: &str) -> Result<Vec<String>, ApiError> {
    let table = meridian_store::table::get_by_id(&state.pool, table_id)
        .await?
        .ok_or_else(|| ApiError::no_such_table(format!("table {table_id:?}")))?;
    let ns = namespace::get_by_id(&state.pool, &table.namespace_id)
        .await?
        .ok_or_else(|| {
            ApiError::no_such_namespace(format!("namespace {:?}", table.namespace_id))
        })?;
    super::grants::namespace_scope_chain(&state.pool, &ns.warehouse_id, &ns.levels).await
}

/// The warehouse record + namespace levels for a table id (diff/merge read the
/// table's metadata through the warehouse's storage).
async fn load_warehouse_for_table(
    state: &AppState,
    table_id: &str,
) -> Result<(WarehouseRecord, Vec<String>), ApiError> {
    let table = meridian_store::table::get_by_id(&state.pool, table_id)
        .await?
        .ok_or_else(|| ApiError::no_such_table(format!("table {table_id:?}")))?;
    let ns = namespace::get_by_id(&state.pool, &table.namespace_id)
        .await?
        .ok_or_else(|| {
            ApiError::no_such_namespace(format!("namespace {:?}", table.namespace_id))
        })?;
    let warehouse = meridian_store::warehouse::get_by_id(
        &state.pool,
        tenancy::default_workspace_id(),
        &ns.warehouse_id,
    )
    .await?
    .ok_or_else(|| ApiError::no_such_warehouse(&ns.warehouse_id))?;
    Ok((warehouse, ns.levels))
}

/// The head snapshot id of a metadata file (best-effort; for tag pinning).
async fn head_snapshot_of(state: &AppState, location: &str) -> Option<i64> {
    // Any warehouse's storage can read the file (locations are absolute); use
    // the first warehouse. Failing to read leaves the pin snapshot null.
    let warehouses = meridian_store::warehouse::list(&state.pool, tenancy::default_workspace_id())
        .await
        .ok()?;
    let warehouse = warehouses.first()?;
    let storage = connect_storage(warehouse).ok()?;
    let md = read_table_metadata(storage.as_ref(), location).await.ok()?;
    md.current_snapshot_id.filter(|id| *id >= 0)
}

fn validate_name(name: &str) -> Result<(), ApiError> {
    if name.is_empty() || name.len() > MAX_NAME_LEN {
        return Err(ApiError::bad_request(format!(
            "name must be 1..={MAX_NAME_LEN} characters"
        )));
    }
    if name == branches::MAIN_REF {
        return Err(ApiError::bad_request("'main' is reserved"));
    }
    if name.contains('@') {
        return Err(ApiError::bad_request(
            "name must not contain '@' (it separates the warehouse from the branch in a prefix)",
        ));
    }
    Ok(())
}

fn require_warehouse<'a>(
    _state: &AppState,
    warehouse: Option<&'a str>,
) -> Result<&'a str, ApiError> {
    warehouse
        .ok_or_else(|| ApiError::bad_request("`warehouse` is required when `namespaces` are given"))
}

fn decode_dotted(dotted: &str) -> Vec<String> {
    dotted.split('.').map(str::to_owned).collect()
}
