//! Governance management API (Pillar D / D-F1, D-F3, D-F5), mounted under
//! `/api/v2/governance`. The control plane for the enforcement the scan
//! planner applies (D-F2.1, `crate::governance`):
//!
//! - **Tags** (`/tags`, `/tags/assignments`): the classification unit and its
//!   placement on tables/namespaces/columns (D-F3). CRUD + assignment +
//!   classifier-suggestion approval + a classification-coverage report.
//! - **Policies** (`/policies`): versioned row-filter / column-mask / ABAC
//!   policies (D-F1). CRUD + version history + rollback + bind/unbind to a
//!   securable or a tag + **dry-run** ("who would lose access").
//! - **Effective policy** (`/effective-policy`): every policy that applies to
//!   a `(principal, table)`, the resolved row filter + masked columns, and the
//!   allow/deny decision with its reason — the auditor's answer to "what does
//!   this person actually see."
//! - **Who-can-see-what** (`/who-can-see`): a principal's effective RBAC
//!   permissions extended with the ABAC policies that would filter/mask each
//!   accessible table (D-F5).
//! - **Drift** (`/drift`): policy-drift alerts (D-F5) — e.g. a column on a
//!   `pii`-tagged table that carries no mask.
//! - **Evidence** (`/evidence`): an audit-ready export of every governance
//!   decision + the current policy/tag inventory for an auditor.
//!
//! # Authorization
//!
//! Every route is **management-gated** (`admin` role or any `MANAGE_WAREHOUSE`
//! grant), the same gate maintenance policy mutations use. Governance is a
//! privileged, cross-resource surface (a policy can bind to a tag that spans
//! the whole catalog), so a dedicated management check is the honest fit
//! without inventing a new RBAC privilege (which would need a migration to the
//! 0005 privilege CHECK). This matches the "management or a govern privilege"
//! requirement — `require_management` *is* the govern gate today.
//!
//! # Validation
//!
//! A policy's definition is a `meridian_authz::AbacRule`. Before any policy is
//! saved, its rule is compiled to Cedar and validated against the Meridian
//! Cedar schema (`meridian_authz::validate_against_schema`), so a malformed
//! rule is a 400 at write time, not a silent enforcement no-op (D-F1: "detect
//! errors before save").

use axum::extract::{Path, Query, State};
use axum::{Extension, Json};
use chrono::{DateTime, Utc};
use meridian_authz::{AbacRule, compile_ruleset};
use meridian_common::principal::Principal;
use meridian_store::policy::{self, BindingTarget, PolicyKind, PolicyUpdate};
use meridian_store::tags::{self, AssignmentSource, TagSecurable};
use meridian_store::{namespace, rbac, table, tenancy};
use serde::{Deserialize, Serialize};
use serde_json::{Value, json};

use crate::AppState;
use crate::error::ApiError;
use crate::governance::{self, TableContext};
use crate::routes::grants::{namespace_scope_chain, require_management};
use crate::routes::namespaces::{decode_namespace_param, resolve_warehouse};

// ===========================================================================
// Shared helpers
// ===========================================================================

/// Resolves a table by `(warehouse, dotted-namespace, table)` names to its
/// record + self-and-ancestor namespace chain. 404 if any part is missing.
async fn resolve_table_by_name(
    state: &AppState,
    warehouse: &str,
    dotted_namespace: &str,
    table_name: &str,
) -> Result<(table::TableRecord, Vec<String>), ApiError> {
    let wh = resolve_warehouse(&state.pool, warehouse).await?;
    let levels = decode_namespace_param(dotted_namespace)?;
    let record = table::get_by_name(&state.pool, &wh.id, &levels, table_name)
        .await?
        .ok_or_else(|| {
            ApiError::new(
                axum::http::StatusCode::NOT_FOUND,
                "NoSuchTableException",
                format!("table {warehouse}/{dotted_namespace}/{table_name} does not exist"),
            )
        })?;
    let chain = namespace_scope_chain(&state.pool, &wh.id, &levels).await?;
    Ok((record, chain))
}

// ===========================================================================
// Tags
// ===========================================================================

/// A tag as rendered by the API.
#[derive(Debug, Serialize)]
pub struct TagResponse {
    /// ULID of the tag.
    pub id: String,
    /// Tag key, e.g. `pii`.
    pub key: String,
    /// Tag value, e.g. `email`.
    pub value: String,
    /// The rendered `key:value` form used in policies.
    pub rendered: String,
    /// Optional description.
    pub description: Option<String>,
    /// Creation time.
    pub created_at: DateTime<Utc>,
}

impl From<tags::Tag> for TagResponse {
    fn from(t: tags::Tag) -> Self {
        let rendered = t.rendered();
        Self {
            id: t.id,
            key: t.key,
            value: t.value,
            rendered,
            description: t.description,
            created_at: t.created_at,
        }
    }
}

/// Request body to create a tag.
#[derive(Debug, Deserialize)]
pub struct CreateTagRequest {
    /// Tag key.
    pub key: String,
    /// Tag value.
    pub value: String,
    /// Optional description.
    #[serde(default)]
    pub description: Option<String>,
}

/// `GET /api/v2/governance/tags` — list all tags.
pub async fn list_tags(
    State(state): State<AppState>,
    Extension(principal): Extension<Principal>,
) -> Result<Json<Value>, ApiError> {
    require_management(&state.pool, &principal).await?;
    let items = tags::list_tags(&state.pool, tenancy::default_workspace_id()).await?;
    let out: Vec<TagResponse> = items.into_iter().map(TagResponse::from).collect();
    Ok(Json(json!({ "tags": out })))
}

/// `POST /api/v2/governance/tags` — create a tag.
pub async fn create_tag(
    State(state): State<AppState>,
    Extension(principal): Extension<Principal>,
    Json(req): Json<CreateTagRequest>,
) -> Result<(axum::http::StatusCode, Json<TagResponse>), ApiError> {
    require_management(&state.pool, &principal).await?;
    let tag = tags::create_tag(
        &state.pool,
        tenancy::default_workspace_id(),
        &req.key,
        &req.value,
        req.description.as_deref(),
        &principal.audit_string(),
    )
    .await?;
    Ok((axum::http::StatusCode::CREATED, Json(tag.into())))
}

/// `DELETE /api/v2/governance/tags/{id}` — delete a tag (cascades to its
/// assignments and bindings).
pub async fn delete_tag(
    State(state): State<AppState>,
    Extension(principal): Extension<Principal>,
    Path(id): Path<String>,
) -> Result<axum::http::StatusCode, ApiError> {
    require_management(&state.pool, &principal).await?;
    tags::delete_tag(
        &state.pool,
        tenancy::default_workspace_id(),
        &id,
        &principal.audit_string(),
    )
    .await?;
    Ok(axum::http::StatusCode::NO_CONTENT)
}

/// The securable an assignment targets, by name (resolved to an id here).
#[derive(Debug, Deserialize)]
pub struct AssignmentTarget {
    /// `table` | `namespace` | `column`.
    pub securable_type: String,
    /// Warehouse name (addressing root).
    pub warehouse: String,
    /// Dotted namespace; required for all three kinds (a table/column lives in
    /// a namespace, a namespace target *is* the namespace).
    pub namespace: String,
    /// Table name; required for `table` and `column` targets.
    #[serde(default)]
    pub table: Option<String>,
    /// Column name; required for a `column` target.
    #[serde(default)]
    pub column: Option<String>,
}

/// Request body to assign a tag.
#[derive(Debug, Deserialize)]
pub struct AssignTagRequest {
    /// The tag id to assign.
    pub tag_id: String,
    /// What to assign it to.
    pub target: AssignmentTarget,
    /// Provenance: `manual` (default) or `classifier`.
    #[serde(default)]
    pub source: Option<String>,
    /// Classifier confidence in `[0, 1]`, for classifier suggestions.
    #[serde(default)]
    pub confidence: Option<f64>,
    /// Whether in force; defaults by source (manual → true, classifier →
    /// false).
    #[serde(default)]
    pub approved: Option<bool>,
}

/// A tag assignment as rendered by the API.
#[derive(Debug, Serialize)]
pub struct AssignmentResponse {
    /// ULID of the assignment.
    pub id: String,
    /// Assigned tag id.
    pub tag_id: String,
    /// Securable kind.
    pub securable_type: String,
    /// Securable id.
    pub securable_id: String,
    /// Column name, for column assignments.
    pub column_name: Option<String>,
    /// Provenance.
    pub source: String,
    /// Classifier confidence, if any.
    pub confidence: Option<f64>,
    /// Whether in force.
    pub approved: bool,
    /// Creation time.
    pub created_at: DateTime<Utc>,
}

impl From<tags::TagAssignment> for AssignmentResponse {
    fn from(a: tags::TagAssignment) -> Self {
        Self {
            id: a.id,
            tag_id: a.tag_id,
            securable_type: a.securable_type.as_str().to_owned(),
            securable_id: a.securable_id,
            column_name: a.column_name,
            source: a.source.as_str().to_owned(),
            confidence: a.confidence,
            approved: a.approved,
            created_at: a.created_at,
        }
    }
}

/// Resolves an [`AssignmentTarget`] to a `(securable_type, securable_id,
/// column_name)` triple. For a column, `securable_id` is the owning table's
/// id; for a table, the table id; for a namespace, the namespace id.
async fn resolve_assignment_target(
    state: &AppState,
    target: &AssignmentTarget,
) -> Result<(TagSecurable, String, Option<String>), ApiError> {
    let kind = TagSecurable::parse(&target.securable_type).ok_or_else(|| {
        ApiError::bad_request(format!(
            "invalid securable_type {:?}: expected table, namespace, or column",
            target.securable_type
        ))
    })?;
    let wh = resolve_warehouse(&state.pool, &target.warehouse).await?;
    let levels = decode_namespace_param(&target.namespace)?;

    match kind {
        TagSecurable::Namespace => {
            let ns = namespace::get(&state.pool, &wh.id, &levels)
                .await?
                .ok_or_else(|| {
                    ApiError::new(
                        axum::http::StatusCode::NOT_FOUND,
                        "NoSuchNamespaceException",
                        format!("namespace {} does not exist", target.namespace),
                    )
                })?;
            Ok((kind, ns.id, None))
        }
        TagSecurable::Table | TagSecurable::Column => {
            let table_name = target.table.as_deref().ok_or_else(|| {
                ApiError::bad_request("table is required for a table/column target")
            })?;
            let record = table::get_by_name(&state.pool, &wh.id, &levels, table_name)
                .await?
                .ok_or_else(|| {
                    ApiError::new(
                        axum::http::StatusCode::NOT_FOUND,
                        "NoSuchTableException",
                        format!("table {table_name} does not exist"),
                    )
                })?;
            if kind == TagSecurable::Column {
                let column = target.column.clone().ok_or_else(|| {
                    ApiError::bad_request("column is required for a column target")
                })?;
                Ok((kind, record.id, Some(column)))
            } else {
                Ok((kind, record.id, None))
            }
        }
    }
}

/// `POST /api/v2/governance/tags/assignments` — assign a tag to a securable.
pub async fn assign_tag(
    State(state): State<AppState>,
    Extension(principal): Extension<Principal>,
    Json(req): Json<AssignTagRequest>,
) -> Result<(axum::http::StatusCode, Json<AssignmentResponse>), ApiError> {
    require_management(&state.pool, &principal).await?;
    let (securable_type, securable_id, column_name) =
        resolve_assignment_target(&state, &req.target).await?;
    let source = match req.source.as_deref() {
        None | Some("manual") => AssignmentSource::Manual,
        Some("classifier") => AssignmentSource::Classifier,
        Some(other) => {
            return Err(ApiError::bad_request(format!(
                "invalid source {other:?}: expected manual or classifier"
            )));
        }
    };
    let assignment = tags::assign(
        &state.pool,
        tenancy::default_workspace_id(),
        tags::NewAssignment {
            tag_id: &req.tag_id,
            securable_type,
            securable_id: &securable_id,
            column_name: column_name.as_deref(),
            source,
            confidence: req.confidence,
            approved: req.approved,
        },
        &principal.audit_string(),
    )
    .await?;
    Ok((axum::http::StatusCode::CREATED, Json(assignment.into())))
}

/// `POST /api/v2/governance/tags/assignments/{id}/approve` — approve a
/// classifier-suggested assignment (put it in force).
pub async fn approve_assignment(
    State(state): State<AppState>,
    Extension(principal): Extension<Principal>,
    Path(id): Path<String>,
) -> Result<Json<AssignmentResponse>, ApiError> {
    require_management(&state.pool, &principal).await?;
    let a = tags::approve_assignment(
        &state.pool,
        tenancy::default_workspace_id(),
        &id,
        &principal.audit_string(),
    )
    .await?;
    Ok(Json(a.into()))
}

/// `DELETE /api/v2/governance/tags/assignments/{id}` — remove an assignment.
pub async fn unassign_tag(
    State(state): State<AppState>,
    Extension(principal): Extension<Principal>,
    Path(id): Path<String>,
) -> Result<axum::http::StatusCode, ApiError> {
    require_management(&state.pool, &principal).await?;
    tags::unassign(
        &state.pool,
        tenancy::default_workspace_id(),
        &id,
        &principal.audit_string(),
    )
    .await?;
    Ok(axum::http::StatusCode::NO_CONTENT)
}

/// Query for the classification-coverage report.
#[derive(Debug, Deserialize)]
pub struct CoverageQuery {
    /// Warehouse to report on.
    pub warehouse: String,
    /// Optional dotted namespace to scope the report (default: whole
    /// warehouse).
    #[serde(default)]
    pub namespace: Option<String>,
}

/// `GET /api/v2/governance/tags/coverage` — classification coverage (D-F3):
/// per-table whether the table carries a tag and how many of its columns are
/// tagged, plus a warehouse roll-up.
pub async fn classification_coverage(
    State(state): State<AppState>,
    Extension(principal): Extension<Principal>,
    Query(q): Query<CoverageQuery>,
) -> Result<Json<Value>, ApiError> {
    require_management(&state.pool, &principal).await?;
    let wh = resolve_warehouse(&state.pool, &q.warehouse).await?;
    let workspace_id = tenancy::default_workspace_id();

    // Tables in scope (optionally under a namespace prefix).
    let table_rows: Vec<(String, Vec<String>, String)> = match &q.namespace {
        Some(ns) => {
            let levels = decode_namespace_param(ns)?;
            sqlx::query_as(
                "SELECT t.id, n.levels, t.name
                 FROM tables t JOIN namespaces n ON n.id = t.namespace_id
                 WHERE n.warehouse_id = $1
                   AND cardinality(n.levels) >= cardinality($2::text[])
                   AND n.levels[1:cardinality($2::text[])] = $2::text[]
                 ORDER BY n.levels, t.name",
            )
            .bind(&wh.id)
            .bind(&levels)
            .fetch_all(&state.pool)
            .await
        }
        None => {
            sqlx::query_as(
                "SELECT t.id, n.levels, t.name
                 FROM tables t JOIN namespaces n ON n.id = t.namespace_id
                 WHERE n.warehouse_id = $1
                 ORDER BY n.levels, t.name",
            )
            .bind(&wh.id)
            .fetch_all(&state.pool)
            .await
        }
    }
    .map_err(|e| ApiError::from(meridian_store::map_sqlx_error("failed to list tables", e)))?;

    let table_ids: Vec<String> = table_rows.iter().map(|(id, _, _)| id.clone()).collect();
    let coverage = tags::column_tag_counts(&state.pool, workspace_id, &table_ids).await?;
    let coverage_by_id: std::collections::BTreeMap<String, tags::TableCoverage> = coverage
        .into_iter()
        .map(|c| (c.table_id.clone(), c))
        .collect();

    let mut tables = Vec::new();
    let mut tagged_tables = 0_i64;
    for (id, levels, name) in &table_rows {
        let cov = coverage_by_id.get(id);
        let table_tagged = cov.is_some_and(|c| c.table_tagged);
        let tagged_columns = cov.map_or(0, |c| c.tagged_columns);
        if table_tagged || tagged_columns > 0 {
            tagged_tables += 1;
        }
        tables.push(json!({
            "table_id": id,
            "namespace": levels.join("."),
            "name": name,
            "table_tagged": table_tagged,
            "tagged_columns": tagged_columns,
        }));
    }

    let total = i64::try_from(table_rows.len()).unwrap_or(i64::MAX);
    Ok(Json(json!({
        "warehouse": q.warehouse,
        "namespace": q.namespace,
        "total_tables": total,
        "tables_with_any_tag": tagged_tables,
        "tables": tables,
    })))
}

// ===========================================================================
// Policies
// ===========================================================================

/// A policy as rendered by the API.
#[derive(Debug, Serialize)]
pub struct PolicyResponse {
    /// ULID of the policy.
    pub id: String,
    /// Human name.
    pub name: String,
    /// Kind: `row_filter` | `column_mask` | `abac`.
    pub kind: String,
    /// Current version.
    pub version: i32,
    /// Whether in force.
    pub enabled: bool,
    /// The typed definition (an `AbacRule`).
    pub definition: Value,
    /// Creating principal.
    pub created_by: String,
    /// Creation time.
    pub created_at: DateTime<Utc>,
    /// Last-update time.
    pub updated_at: DateTime<Utc>,
}

impl From<policy::Policy> for PolicyResponse {
    fn from(p: policy::Policy) -> Self {
        Self {
            id: p.id,
            name: p.name,
            kind: p.kind.as_str().to_owned(),
            version: p.version,
            enabled: p.enabled,
            definition: p.definition,
            created_by: p.created_by,
            created_at: p.created_at,
            updated_at: p.updated_at,
        }
    }
}

/// Request body to create a policy.
#[derive(Debug, Deserialize)]
pub struct CreatePolicyRequest {
    /// Human name (unique per workspace).
    pub name: String,
    /// Kind: `row_filter` | `column_mask` | `abac`.
    pub kind: String,
    /// The typed definition — a `meridian_authz::AbacRule`.
    pub definition: Value,
}

/// Validates a policy definition: it must deserialize to an [`AbacRule`],
/// compile to Cedar, and pass schema validation. Returns the parsed rule (so
/// the kind can be cross-checked) or a 400 with the reason.
fn validate_definition(kind: PolicyKind, definition: &Value) -> Result<AbacRule, ApiError> {
    let rule: AbacRule = serde_json::from_value(definition.clone()).map_err(|e| {
        ApiError::bad_request(format!(
            "policy definition is not a valid rule: {e} (expected an AbacRule, \
             e.g. {{\"type\":\"tag_deny_unless_purpose\", ...}})"
        ))
    })?;
    cross_check_kind(kind, &rule)?;
    // Compile to Cedar and validate against the Meridian schema — a malformed
    // rule is a 400 here, never a silent enforcement no-op.
    let cedar = compile_ruleset(std::slice::from_ref(&rule));
    meridian_authz::validate_against_schema(&cedar)
        .map_err(|e| ApiError::bad_request(format!("policy failed Cedar validation: {e}")))?;
    Ok(rule)
}

/// Cross-checks that the rule shape matches the declared kind (a
/// `row_filter` policy must carry a `TagRowFilter`, a `column_mask` a
/// `TagColumnMask`; `abac` accepts the deny/group/owner/time shapes).
fn cross_check_kind(kind: PolicyKind, rule: &AbacRule) -> Result<(), ApiError> {
    let ok = match kind {
        PolicyKind::RowFilter => matches!(rule, AbacRule::TagRowFilter { .. }),
        PolicyKind::ColumnMask => matches!(rule, AbacRule::TagColumnMask { .. }),
        PolicyKind::Abac => matches!(
            rule,
            AbacRule::TagDenyUnlessPurpose { .. }
                | AbacRule::OwnerAllow { .. }
                | AbacRule::GroupAllow { .. }
                | AbacRule::GroupDeny { .. }
                | AbacRule::TimeBoundAllow { .. }
        ),
    };
    if ok {
        Ok(())
    } else {
        Err(ApiError::bad_request(format!(
            "policy kind {} does not match the rule shape (row_filter⇒tag_row_filter, \
             column_mask⇒tag_column_mask, abac⇒deny/group/owner/time rules)",
            kind.as_str()
        )))
    }
}

/// `GET /api/v2/governance/policies` — list all policies.
pub async fn list_policies(
    State(state): State<AppState>,
    Extension(principal): Extension<Principal>,
) -> Result<Json<Value>, ApiError> {
    require_management(&state.pool, &principal).await?;
    let items = policy::list(&state.pool, tenancy::default_workspace_id()).await?;
    let out: Vec<PolicyResponse> = items.into_iter().map(PolicyResponse::from).collect();
    Ok(Json(json!({ "policies": out })))
}

/// `POST /api/v2/governance/policies` — create a policy.
pub async fn create_policy(
    State(state): State<AppState>,
    Extension(principal): Extension<Principal>,
    Json(req): Json<CreatePolicyRequest>,
) -> Result<(axum::http::StatusCode, Json<PolicyResponse>), ApiError> {
    require_management(&state.pool, &principal).await?;
    let kind = PolicyKind::parse(&req.kind).ok_or_else(|| {
        ApiError::bad_request(format!(
            "invalid policy kind {:?}: expected row_filter, column_mask, or abac",
            req.kind
        ))
    })?;
    validate_definition(kind, &req.definition)?;
    let p = policy::create(
        &state.pool,
        tenancy::default_workspace_id(),
        &req.name,
        kind,
        &req.definition,
        &principal.audit_string(),
    )
    .await?;
    Ok((axum::http::StatusCode::CREATED, Json(p.into())))
}

/// `GET /api/v2/governance/policies/{id}` — load one policy.
pub async fn get_policy(
    State(state): State<AppState>,
    Extension(principal): Extension<Principal>,
    Path(id): Path<String>,
) -> Result<Json<PolicyResponse>, ApiError> {
    require_management(&state.pool, &principal).await?;
    let p = policy::get(&state.pool, tenancy::default_workspace_id(), &id)
        .await?
        .ok_or_else(|| {
            ApiError::new(
                axum::http::StatusCode::NOT_FOUND,
                "NotFoundException",
                format!("policy {id:?} does not exist"),
            )
        })?;
    Ok(Json(p.into()))
}

/// Request body to update a policy.
#[derive(Debug, Deserialize)]
pub struct UpdatePolicyRequest {
    /// New definition, if changing.
    #[serde(default)]
    pub definition: Option<Value>,
    /// New enabled flag, if changing.
    #[serde(default)]
    pub enabled: Option<bool>,
}

/// `PATCH /api/v2/governance/policies/{id}` — update a policy (bumps version).
pub async fn update_policy(
    State(state): State<AppState>,
    Extension(principal): Extension<Principal>,
    Path(id): Path<String>,
    Json(req): Json<UpdatePolicyRequest>,
) -> Result<Json<PolicyResponse>, ApiError> {
    require_management(&state.pool, &principal).await?;
    let workspace_id = tenancy::default_workspace_id();

    // Validate the new definition against the existing kind (kind is fixed).
    if let Some(def) = &req.definition {
        let existing = policy::get(&state.pool, workspace_id, &id)
            .await?
            .ok_or_else(|| {
                ApiError::new(
                    axum::http::StatusCode::NOT_FOUND,
                    "NotFoundException",
                    format!("policy {id:?} does not exist"),
                )
            })?;
        validate_definition(existing.kind, def)?;
    }

    let p = policy::update(
        &state.pool,
        workspace_id,
        &id,
        PolicyUpdate {
            definition: req.definition,
            enabled: req.enabled,
        },
        &principal.audit_string(),
    )
    .await?;
    Ok(Json(p.into()))
}

/// `DELETE /api/v2/governance/policies/{id}` — delete a policy.
pub async fn delete_policy(
    State(state): State<AppState>,
    Extension(principal): Extension<Principal>,
    Path(id): Path<String>,
) -> Result<axum::http::StatusCode, ApiError> {
    require_management(&state.pool, &principal).await?;
    policy::delete(
        &state.pool,
        tenancy::default_workspace_id(),
        &id,
        &principal.audit_string(),
    )
    .await?;
    Ok(axum::http::StatusCode::NO_CONTENT)
}

/// `GET /api/v2/governance/policies/{id}/versions` — version history.
pub async fn list_policy_versions(
    State(state): State<AppState>,
    Extension(principal): Extension<Principal>,
    Path(id): Path<String>,
) -> Result<Json<Value>, ApiError> {
    require_management(&state.pool, &principal).await?;
    let versions = policy::list_versions(&state.pool, tenancy::default_workspace_id(), &id).await?;
    let out: Vec<Value> = versions
        .into_iter()
        .map(|v| {
            json!({
                "version": v.version,
                "kind": v.kind.as_str(),
                "enabled": v.enabled,
                "definition": v.definition,
                "created_by": v.created_by,
                "created_at": v.created_at,
            })
        })
        .collect();
    Ok(Json(json!({ "policy_id": id, "versions": out })))
}

/// Request to roll a policy back to a prior version.
#[derive(Debug, Deserialize)]
pub struct RollbackRequest {
    /// The version to restore (a new version is created with its definition).
    pub to_version: i32,
}

/// `POST /api/v2/governance/policies/{id}/rollback` — roll back to a version.
pub async fn rollback_policy(
    State(state): State<AppState>,
    Extension(principal): Extension<Principal>,
    Path(id): Path<String>,
    Json(req): Json<RollbackRequest>,
) -> Result<Json<PolicyResponse>, ApiError> {
    require_management(&state.pool, &principal).await?;
    let p = policy::rollback(
        &state.pool,
        tenancy::default_workspace_id(),
        &id,
        req.to_version,
        &principal.audit_string(),
    )
    .await?;
    Ok(Json(p.into()))
}

/// Request to bind a policy to a target.
#[derive(Debug, Deserialize)]
pub struct BindRequest {
    /// `table` | `namespace` | `tag`.
    pub target_type: String,
    /// For a `tag` target: the tag id.
    #[serde(default)]
    pub tag_id: Option<String>,
    /// For a `table`/`namespace` target: the warehouse name.
    #[serde(default)]
    pub warehouse: Option<String>,
    /// For a `table`/`namespace` target: the dotted namespace.
    #[serde(default)]
    pub namespace: Option<String>,
    /// For a `table` target: the table name.
    #[serde(default)]
    pub table: Option<String>,
}

/// A binding as rendered by the API.
#[derive(Debug, Serialize)]
pub struct BindingResponse {
    /// ULID of the binding.
    pub id: String,
    /// The bound policy.
    pub policy_id: String,
    /// Target kind: `table` | `namespace` | `tag`.
    pub target_type: String,
    /// The bound securable/tag id.
    pub target_id: String,
    /// Binding principal.
    pub bound_by: String,
    /// Creation time.
    pub created_at: DateTime<Utc>,
}

impl From<policy::PolicyBinding> for BindingResponse {
    fn from(b: policy::PolicyBinding) -> Self {
        let (target_type, target_id) = match &b.target {
            BindingTarget::Securable {
                securable_type,
                securable_id,
            } => (securable_type.as_str().to_owned(), securable_id.clone()),
            BindingTarget::Tag { tag_id } => ("tag".to_owned(), tag_id.clone()),
        };
        Self {
            id: b.id,
            policy_id: b.policy_id,
            target_type,
            target_id,
            bound_by: b.bound_by,
            created_at: b.created_at,
        }
    }
}

/// Resolves a [`BindRequest`] to a [`BindingTarget`].
async fn resolve_binding_target(
    state: &AppState,
    req: &BindRequest,
) -> Result<BindingTarget, ApiError> {
    match req.target_type.as_str() {
        "tag" => {
            let tag_id = req
                .tag_id
                .clone()
                .ok_or_else(|| ApiError::bad_request("tag_id is required for a tag binding"))?;
            Ok(BindingTarget::Tag { tag_id })
        }
        "namespace" => {
            let (wh, levels) = binding_scope(state, req).await?;
            let ns = namespace::get(&state.pool, &wh, &levels)
                .await?
                .ok_or_else(|| {
                    ApiError::new(
                        axum::http::StatusCode::NOT_FOUND,
                        "NoSuchNamespaceException",
                        "namespace does not exist".to_owned(),
                    )
                })?;
            Ok(BindingTarget::Securable {
                securable_type: TagSecurable::Namespace,
                securable_id: ns.id,
            })
        }
        "table" => {
            let (wh, levels) = binding_scope(state, req).await?;
            let table_name = req
                .table
                .as_deref()
                .ok_or_else(|| ApiError::bad_request("table is required for a table binding"))?;
            let record = table::get_by_name(&state.pool, &wh, &levels, table_name)
                .await?
                .ok_or_else(|| {
                    ApiError::new(
                        axum::http::StatusCode::NOT_FOUND,
                        "NoSuchTableException",
                        format!("table {table_name} does not exist"),
                    )
                })?;
            Ok(BindingTarget::Securable {
                securable_type: TagSecurable::Table,
                securable_id: record.id,
            })
        }
        other => Err(ApiError::bad_request(format!(
            "invalid target_type {other:?}: expected table, namespace, or tag"
        ))),
    }
}

async fn binding_scope(
    state: &AppState,
    req: &BindRequest,
) -> Result<(String, Vec<String>), ApiError> {
    let warehouse = req
        .warehouse
        .as_deref()
        .ok_or_else(|| ApiError::bad_request("warehouse is required for a securable binding"))?;
    let namespace = req
        .namespace
        .as_deref()
        .ok_or_else(|| ApiError::bad_request("namespace is required for a securable binding"))?;
    let wh = resolve_warehouse(&state.pool, warehouse).await?;
    let levels = decode_namespace_param(namespace)?;
    Ok((wh.id, levels))
}

/// `GET /api/v2/governance/policies/{id}/bindings` — list a policy's bindings.
pub async fn list_bindings(
    State(state): State<AppState>,
    Extension(principal): Extension<Principal>,
    Path(id): Path<String>,
) -> Result<Json<Value>, ApiError> {
    require_management(&state.pool, &principal).await?;
    let bindings = policy::list_bindings(&state.pool, tenancy::default_workspace_id(), &id).await?;
    let out: Vec<BindingResponse> = bindings.into_iter().map(BindingResponse::from).collect();
    Ok(Json(json!({ "policy_id": id, "bindings": out })))
}

/// `POST /api/v2/governance/policies/{id}/bindings` — bind a policy.
pub async fn bind_policy(
    State(state): State<AppState>,
    Extension(principal): Extension<Principal>,
    Path(id): Path<String>,
    Json(req): Json<BindRequest>,
) -> Result<(axum::http::StatusCode, Json<BindingResponse>), ApiError> {
    require_management(&state.pool, &principal).await?;
    let target = resolve_binding_target(&state, &req).await?;
    let b = policy::bind(
        &state.pool,
        tenancy::default_workspace_id(),
        &id,
        &target,
        &principal.audit_string(),
    )
    .await?;
    Ok((axum::http::StatusCode::CREATED, Json(b.into())))
}

/// `DELETE /api/v2/governance/policies/bindings/{binding_id}` — unbind.
pub async fn unbind_policy(
    State(state): State<AppState>,
    Extension(principal): Extension<Principal>,
    Path(binding_id): Path<String>,
) -> Result<axum::http::StatusCode, ApiError> {
    require_management(&state.pool, &principal).await?;
    policy::unbind(
        &state.pool,
        tenancy::default_workspace_id(),
        &binding_id,
        &principal.audit_string(),
    )
    .await?;
    Ok(axum::http::StatusCode::NO_CONTENT)
}

// ===========================================================================
// Effective policy + dry-run + who-can-see-what
// ===========================================================================

/// Query for the effective-policy and dry-run endpoints.
#[derive(Debug, Deserialize)]
pub struct EffectiveQuery {
    /// The subject (audit string, e.g. `user:alice@example.com`) whose access
    /// is being resolved.
    pub principal: String,
    /// Warehouse name.
    pub warehouse: String,
    /// Dotted namespace.
    pub namespace: String,
    /// Table name.
    pub table: String,
    /// Optional declared purpose (purpose-based access).
    #[serde(default)]
    pub purpose: Option<String>,
}

/// Loads a table's current schema (column universe) for policy resolution.
/// Requires reading the table metadata from storage.
async fn load_table_schema(
    state: &AppState,
    warehouse: &str,
    record: &table::TableRecord,
) -> Result<meridian_iceberg::spec::Schema, ApiError> {
    let wh = resolve_warehouse(&state.pool, warehouse).await?;
    let Some(metadata_location) = record.metadata_location.clone() else {
        return Err(ApiError::new(
            axum::http::StatusCode::NOT_FOUND,
            "NoSuchTableException",
            "table has no metadata".to_owned(),
        ));
    };
    let storage = crate::routes::tables::connect_storage(&wh)?;
    let metadata = meridian_storage::read_table_metadata(storage.as_ref(), &metadata_location)
        .await
        .map_err(|e| {
            ApiError::new(
                axum::http::StatusCode::INTERNAL_SERVER_ERROR,
                "InternalServerError",
                format!("table metadata is unreadable: {e}"),
            )
        })?;
    metadata
        .schemas
        .iter()
        .find(|s| s.schema_id == Some(metadata.current_schema_id))
        .cloned()
        .ok_or_else(|| {
            ApiError::new(
                axum::http::StatusCode::INTERNAL_SERVER_ERROR,
                "InternalServerError",
                "current schema missing from table metadata".to_owned(),
            )
        })
}

/// Builds a synthetic [`Principal`] for a resolution query from an audit
/// string like `user:sub`, `service:sub`, or `agent:sub`. The issuer is
/// resolved from the stored principal row if one exists (so roles resolve),
/// else left `None` (the query still runs with no roles).
async fn principal_for_subject(state: &AppState, audit: &str) -> Result<Principal, ApiError> {
    let (kind, subject) = match audit.split_once(':') {
        Some(("user", s)) => (meridian_common::principal::PrincipalKind::User, s),
        Some(("service", s)) => (meridian_common::principal::PrincipalKind::Service, s),
        Some(("agent", s)) => (meridian_common::principal::PrincipalKind::Agent, s),
        _ => {
            return Err(ApiError::bad_request(
                "principal must be an audit string: user:<sub>, service:<sub>, or agent:<sub>",
            ));
        }
    };
    // Best-effort issuer lookup so RBAC roles resolve for this subject.
    let issuer: Option<String> = sqlx::query_scalar(
        "SELECT issuer FROM principals WHERE subject = $1 AND kind = $2 LIMIT 1",
    )
    .bind(subject)
    .bind(meridian_store::principal::kind_str(kind))
    .fetch_optional(&state.pool)
    .await
    .map_err(|e| {
        ApiError::from(meridian_store::map_sqlx_error(
            "failed to look up principal",
            e,
        ))
    })?;
    Ok(Principal {
        kind,
        subject: subject.to_owned(),
        issuer,
        display_name: None,
    })
}

/// `GET /api/v2/governance/effective-policy` — the full ABAC decision for a
/// `(principal, table[, purpose])`: applied policies, the resolved row filter,
/// masked columns, and the allow/deny decision + reason. This is the auditor's
/// "what does this person actually see" answer.
pub async fn effective_policy(
    State(state): State<AppState>,
    Extension(caller): Extension<Principal>,
    Query(q): Query<EffectiveQuery>,
) -> Result<Json<Value>, ApiError> {
    require_management(&state.pool, &caller).await?;
    let (record, chain) =
        resolve_table_by_name(&state, &q.warehouse, &q.namespace, &q.table).await?;
    let schema = load_table_schema(&state, &q.warehouse, &record).await?;
    let subject = principal_for_subject(&state, &q.principal).await?;

    let table_ctx = TableContext {
        table_id: &record.id,
        namespace_ids: &chain,
        schema: &schema,
        owner: None,
    };
    let policy =
        governance::resolve_scan_policy(&state.pool, &subject, &table_ctx, q.purpose.as_deref())
            .await?;

    let row_filter_json = policy
        .row_filter
        .as_ref()
        .and_then(|e| serde_json::to_value(e).ok());

    Ok(Json(json!({
        "principal": q.principal,
        "table": format!("{}/{}/{}", q.warehouse, q.namespace, q.table),
        "purpose": q.purpose,
        "denied": policy.denied,
        "reason": policy.reason,
        "applied_policies": policy.applied_policies,
        "row_filter": row_filter_json,
        "masked_columns": policy.removed_columns,
    })))
}

/// Request body for policy dry-run: a *proposed* policy definition evaluated
/// against a `(principal, table)` without saving anything ("who would lose
/// access").
#[derive(Debug, Deserialize)]
pub struct DryRunRequest {
    /// The kind of the proposed policy.
    pub kind: String,
    /// The proposed definition (an `AbacRule`).
    pub definition: Value,
    /// The principals to evaluate (audit strings).
    pub principals: Vec<String>,
    /// The table to evaluate against.
    pub warehouse: String,
    /// Dotted namespace.
    pub namespace: String,
    /// Table name.
    pub table: String,
    /// Optional purpose to evaluate with.
    #[serde(default)]
    pub purpose: Option<String>,
    /// If the policy binds via a tag, the tag to *pretend* is on the table for
    /// the dry-run (so a not-yet-created binding can be previewed). Rendered
    /// `key:value`.
    #[serde(default)]
    pub assume_table_tag: Option<String>,
    /// For a column-mask dry-run, the column to pretend carries the tag.
    #[serde(default)]
    pub assume_column: Option<String>,
}

/// `POST /api/v2/governance/policies/dry-run` — preview a proposed policy's
/// effect on a set of principals against one table, without persisting it
/// (D-F1 dry-run / "who would lose access", D-F5). Pure: nothing is written.
pub async fn dry_run_policy(
    State(state): State<AppState>,
    Extension(caller): Extension<Principal>,
    Json(req): Json<DryRunRequest>,
) -> Result<Json<Value>, ApiError> {
    require_management(&state.pool, &caller).await?;
    let kind = PolicyKind::parse(&req.kind)
        .ok_or_else(|| ApiError::bad_request(format!("invalid policy kind {:?}", req.kind)))?;
    let rule = validate_definition(kind, &req.definition)?;

    let (record, chain) =
        resolve_table_by_name(&state, &req.warehouse, &req.namespace, &req.table).await?;
    let schema = load_table_schema(&state, &req.warehouse, &record).await?;

    // Assemble the resource tags: the table's real approved tags plus any
    // assumed tag (to preview a not-yet-created binding).
    let mut resolved_tags = tags::resolve_table_tags(
        &state.pool,
        tenancy::default_workspace_id(),
        &record.id,
        &chain,
    )
    .await?;
    if let Some(tag) = &req.assume_table_tag {
        resolved_tags.push(tags::ResolvedTag {
            tag: tag.clone(),
            column_name: req.assume_column.clone(),
        });
    }

    let mut results = Vec::new();
    for subject_audit in &req.principals {
        let subject = principal_for_subject(&state, subject_audit).await?;
        let outcome = dry_run_one(
            &state,
            &subject,
            &record,
            &schema,
            &resolved_tags,
            &rule,
            req.purpose.as_deref(),
        )
        .await?;
        results.push(json!({
            "principal": subject_audit,
            "denied": outcome.0,
            "row_filtered": outcome.1,
            "masked_columns": outcome.2,
        }));
    }

    Ok(Json(json!({
        "policy_kind": req.kind,
        "table": format!("{}/{}/{}", req.warehouse, req.namespace, req.table),
        "purpose": req.purpose,
        "results": results,
    })))
}

/// Evaluates one proposed rule against one principal, returning
/// `(denied, row_filtered, masked_columns)`. Pure — uses the authz engine
/// directly with the (real + assumed) tag set.
async fn dry_run_one(
    state: &AppState,
    subject: &Principal,
    record: &table::TableRecord,
    schema: &meridian_iceberg::spec::Schema,
    resolved_tags: &[tags::ResolvedTag],
    rule: &AbacRule,
    purpose: Option<&str>,
) -> Result<(bool, bool, Vec<String>), ApiError> {
    use meridian_authz::engine::BaseEffect;
    use meridian_authz::{
        Action, AuthzPrincipal, AuthzResource, PolicyEngine, PrincipalKind, RequestContext,
        ResolvedColumn, ResourceKind, resolve_filters_and_masks,
    };

    // Roles for the subject (best-effort).
    let roles = match subject.issuer.as_deref() {
        Some(issuer) => {
            match meridian_store::principal::get_by_identity(&state.pool, issuer, &subject.subject)
                .await?
            {
                Some(r) => rbac::effective_permissions(&state.pool, &r.id).await?.roles,
                None => Vec::new(),
            }
        }
        None => Vec::new(),
    };

    let mut table_tags = Vec::new();
    let mut column_tags: std::collections::BTreeMap<String, Vec<String>> =
        std::collections::BTreeMap::new();
    for rt in resolved_tags {
        match &rt.column_name {
            None => table_tags.push(rt.tag.clone()),
            Some(c) => column_tags
                .entry(c.clone())
                .or_default()
                .push(rt.tag.clone()),
        }
    }

    let kind = match subject.kind {
        meridian_common::principal::PrincipalKind::Agent => PrincipalKind::Agent,
        meridian_common::principal::PrincipalKind::Service => PrincipalKind::Service,
        _ => PrincipalKind::User,
    };
    let mut authz_principal = AuthzPrincipal::new(subject.audit_string(), kind);
    for r in &roles {
        authz_principal.roles.push(r.clone());
        authz_principal.groups.push(r.clone());
    }
    if let Some(p) = purpose {
        authz_principal.purpose = Some(p.to_owned());
    }

    let mut resource = AuthzResource::new(&record.id, ResourceKind::Table);
    resource.tags = table_tags;

    let mut ctx = RequestContext::now();
    if let Some(p) = purpose {
        ctx = ctx.with_purpose(p);
    }

    let cedar = compile_ruleset(std::slice::from_ref(rule));
    let denied = PolicyEngine::new(&cedar, BaseEffect::AllowUnlessForbidden)
        .and_then(|e| e.authorize(&authz_principal, Action::Read, &resource, &ctx))
        .is_ok_and(|d| d.is_deny());

    let columns: Vec<ResolvedColumn> = schema
        .fields
        .iter()
        .map(|f| {
            ResolvedColumn::new(
                f.name.clone(),
                column_tags.get(&f.name).cloned().unwrap_or_default(),
            )
        })
        .collect();
    let enforcement = resolve_filters_and_masks(
        &authz_principal,
        &resource,
        &columns,
        std::slice::from_ref(rule),
    );

    Ok((
        denied,
        enforcement.row_predicate().is_some(),
        enforcement.masked_columns(),
    ))
}

/// Query for the who-can-see-what report.
#[derive(Debug, Deserialize)]
pub struct WhoCanSeeQuery {
    /// The principal (audit string) to report on.
    pub principal: String,
}

/// `GET /api/v2/governance/who-can-see` — a principal's effective RBAC
/// permissions extended with the count of governance policies that apply to
/// each table it can read (D-F5 who-can-see-what). RBAC says *whether* the
/// principal reaches a table; this adds *what the ABAC layer does* on top.
pub async fn who_can_see(
    State(state): State<AppState>,
    Extension(caller): Extension<Principal>,
    Query(q): Query<WhoCanSeeQuery>,
) -> Result<Json<Value>, ApiError> {
    require_management(&state.pool, &caller).await?;
    let subject = principal_for_subject(&state, &q.principal).await?;

    let roles = match subject.issuer.as_deref() {
        Some(issuer) => {
            match meridian_store::principal::get_by_identity(&state.pool, issuer, &subject.subject)
                .await?
            {
                Some(r) => rbac::effective_permissions(&state.pool, &r.id).await?,
                None => rbac::EffectivePermissions {
                    roles: Vec::new(),
                    permissions: Vec::new(),
                },
            }
        }
        None => rbac::EffectivePermissions {
            roles: Vec::new(),
            permissions: Vec::new(),
        },
    };

    let permissions: Vec<Value> = roles
        .permissions
        .iter()
        .map(|p| {
            json!({
                "privilege": p.privilege,
                "securable_type": p.securable_type,
                "securable_id": p.securable_id,
                "via_role": p.via_role,
            })
        })
        .collect();

    Ok(Json(json!({
        "principal": q.principal,
        "roles": roles.roles,
        "permissions": permissions,
        "note": "RBAC reach; use /effective-policy per table for the row filter, \
                 masked columns, and allow/deny decision the ABAC layer applies.",
    })))
}

// ===========================================================================
// Drift + evidence
// ===========================================================================

/// `GET /api/v2/governance/drift` — policy-drift alerts (D-F5). Today's check:
/// a column that carries a `pii*` tag but has **no column-mask policy** bound
/// (directly or via that tag) — a classified-but-unmasked column an auditor
/// would flag. Scoped to a warehouse.
pub async fn drift(
    State(state): State<AppState>,
    Extension(caller): Extension<Principal>,
    Query(q): Query<CoverageQuery>,
) -> Result<Json<Value>, ApiError> {
    require_management(&state.pool, &caller).await?;
    let wh = resolve_warehouse(&state.pool, &q.warehouse).await?;
    let workspace_id = tenancy::default_workspace_id();

    // Columns tagged with a pii* tag in this warehouse.
    let tagged_columns: Vec<(String, String, String, String)> = sqlx::query_as(
        "SELECT a.securable_id AS table_id, a.column_name, t.key, t.value
         FROM tag_assignments a
         JOIN tags t ON t.id = a.tag_id
         JOIN tables tb ON tb.id = a.securable_id
         JOIN namespaces n ON n.id = tb.namespace_id
         WHERE a.workspace_id = $1
           AND a.approved = TRUE
           AND a.securable_type = 'column'
           AND n.warehouse_id = $2
           AND t.key ILIKE 'pii%'
         ORDER BY a.securable_id, a.column_name",
    )
    .bind(workspace_id.to_string())
    .bind(&wh.id)
    .fetch_all(&state.pool)
    .await
    .map_err(|e| {
        ApiError::from(meridian_store::map_sqlx_error(
            "failed to scan tagged columns",
            e,
        ))
    })?;

    // The set of tags that carry a column-mask policy binding (so a
    // column tagged with such a tag is covered).
    let masked_tags: Vec<String> = sqlx::query_scalar(
        "SELECT DISTINCT t.key || ':' || t.value
         FROM policy_bindings b
         JOIN policies p ON p.id = b.policy_id
         JOIN tags t ON t.id = b.tag_id
         WHERE p.workspace_id = $1 AND p.enabled = TRUE AND p.kind = 'column_mask'",
    )
    .bind(workspace_id.to_string())
    .fetch_all(&state.pool)
    .await
    .map_err(|e| {
        ApiError::from(meridian_store::map_sqlx_error(
            "failed to list masked tags",
            e,
        ))
    })?;
    let masked_tags: std::collections::BTreeSet<String> = masked_tags.into_iter().collect();

    let mut alerts = Vec::new();
    for (table_id, column_name, key, value) in tagged_columns {
        let rendered = format!("{key}:{value}");
        if !masked_tags.contains(&rendered) {
            alerts.push(json!({
                "table_id": table_id,
                "column": column_name,
                "tag": rendered,
                "issue": "classified column has no column-mask policy bound to its tag",
            }));
        }
    }

    Ok(Json(json!({
        "warehouse": q.warehouse,
        "alert_count": alerts.len(),
        "alerts": alerts,
    })))
}

/// Query for the evidence export.
#[derive(Debug, Deserialize)]
pub struct EvidenceQuery {
    /// Max audit rows to include (default 500, capped at 5000).
    #[serde(default)]
    pub limit: Option<i64>,
}

/// `GET /api/v2/governance/evidence` — an audit-ready evidence pack (D-F5):
/// the current policy + tag inventory plus the recent governance-decision
/// audit trail (every `governance.*` action, hash-chained). Suitable for an
/// auditor to attest "these policies were in force and these decisions were
/// made."
pub async fn evidence(
    State(state): State<AppState>,
    Extension(caller): Extension<Principal>,
    Query(q): Query<EvidenceQuery>,
) -> Result<Json<Value>, ApiError> {
    require_management(&state.pool, &caller).await?;
    let workspace_id = tenancy::default_workspace_id();
    let limit = q.limit.unwrap_or(500).clamp(1, 5000);

    let policies = policy::list(&state.pool, workspace_id).await?;
    let tags_list = tags::list_tags(&state.pool, workspace_id).await?;

    // The governance-decision audit trail (prefix match on `governance.`).
    let audit = meridian_store::audit::query(
        &state.pool,
        &meridian_store::audit::AuditQuery {
            workspace_id: Some(workspace_id.to_string()),
            action_prefix: Some("governance.".to_owned()),
            limit,
            ..Default::default()
        },
    )
    .await?;

    let policy_inventory: Vec<Value> = policies
        .iter()
        .map(|p| {
            json!({
                "id": p.id,
                "name": p.name,
                "kind": p.kind.as_str(),
                "version": p.version,
                "enabled": p.enabled,
            })
        })
        .collect();
    let tag_inventory: Vec<Value> = tags_list
        .iter()
        .map(|t| json!({ "id": t.id, "tag": t.rendered() }))
        .collect();
    let audit_trail: Vec<Value> = audit
        .iter()
        .map(|a| {
            json!({
                "seq": a.seq,
                "occurred_at": a.occurred_at,
                "principal": a.principal,
                "action": a.action,
                "resource": a.resource,
                "details": a.details,
                "hash": a.hash,
            })
        })
        .collect();

    Ok(Json(json!({
        "generated_at": Utc::now(),
        "policy_count": policy_inventory.len(),
        "tag_count": tag_inventory.len(),
        "policies": policy_inventory,
        "tags": tag_inventory,
        "audit_trail": audit_trail,
    })))
}
