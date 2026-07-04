//! RBAC management API (roles, bindings, grants, effective permissions)
//! and the authorization helpers used by every enforced handler.
//!
//! # Authorization policy
//!
//! - `auth.mode = "disabled"`: the anonymous principal **bypasses
//!   authorization entirely** (dev mode; the server warns loudly at boot).
//! - `auth.mode = "oidc"`: deny by default. Access comes only from grants
//!   (direct or via roles, with hierarchy inheritance) or the built-in
//!   roles. The first administrator is bootstrapped with
//!   `auth.bootstrap_admin = { issuer, subject }` (applied idempotently at
//!   startup by `meridian serve`).
//! - Denials are `403` in the IRC envelope with type `ForbiddenException`
//!   (the spec's 403 example type is non-prescriptive; `ForbiddenException`
//!   matches the reference Java client's 403 mapping and keeps 401
//!   `NotAuthorizedException` unambiguous).
//! - Resource resolution happens before the authorization check, so an
//!   unknown warehouse/namespace/table/view can 404 before a 403. This
//!   matches the reference catalog's behavior; revisit if existence-hiding
//!   becomes a requirement.
//!
//! # Privilege → endpoint mapping
//!
//! | Endpoint | Check (securable) |
//! |---|---|
//! | `GET /v1/config` | exempt (capability discovery) |
//! | `GET /v1/{prefix}/namespaces` | `LIST_NAMESPACES` (warehouse) |
//! | `POST /v1/{prefix}/namespaces` | `CREATE_NAMESPACE` (warehouse) |
//! | `GET`/`HEAD /v1/{prefix}/namespaces/{ns}` | `LIST_NAMESPACES` (warehouse) |
//! | `DELETE /v1/{prefix}/namespaces/{ns}` | `MANAGE_NAMESPACE` (namespace) |
//! | `POST .../namespaces/{ns}/properties` | `MANAGE_NAMESPACE` (namespace) |
//! | `GET .../namespaces/{ns}/tables` | `LIST_TABLES` (namespace) |
//! | `POST .../namespaces/{ns}/tables` | `CREATE_TABLE` (namespace) |
//! | `POST .../namespaces/{ns}/register` | `CREATE_TABLE` (namespace) |
//! | `GET`/`HEAD .../tables/{table}` | `READ` (table) |
//! | `POST .../tables/{table}` (commit) | `COMMIT` (table); the
//! |   assert-create finalization checks `CREATE_TABLE` (namespace) |
//! | `DELETE .../tables/{table}` | `DROP` (table) |
//! | `POST .../tables/{table}/metrics` | `WRITE` (table) |
//! | `POST /v1/{prefix}/tables/rename` | `WRITE` (source table) **and**
//! |   `CREATE_TABLE` (destination namespace) |
//! | `POST /v1/{prefix}/transactions/commit` | `COMMIT` (every table) |
//! | `GET .../namespaces/{ns}/views` | `LIST_TABLES` (namespace) |
//! | `POST .../namespaces/{ns}/views` | `CREATE_VIEW` (namespace) |
//! | `GET`/`HEAD .../views/{view}` | `READ` (view) |
//! | `POST .../views/{view}` (replace) | `COMMIT` (view) |
//! | `DELETE .../views/{view}` | `DROP` (view) |
//! | `POST /v1/{prefix}/views/rename` | `WRITE` (source view) **and**
//! |   `CREATE_VIEW` (destination namespace) |
//! | `POST /api/v2/warehouses` | management (admin or `MANAGE_WAREHOUSE`) |
//! | `GET /api/v2/warehouses` | management |
//! | `DELETE /api/v2/warehouses/{name}` | `MANAGE_WAREHOUSE` (warehouse) |
//! | `GET /api/v2/principals` | management |
//! | `/api/v2/roles*`, `/api/v2/grants*`, `/api/v2/permissions` | management |
//!
//! "management" = a binding to the built-in `admin` role, or any
//! `MANAGE_WAREHOUSE` grant. A check against a namespace, table, or view
//! securable always also accepts grants on its ancestors (hierarchy
//! inheritance), so "`READ` (table)" means a `READ` grant on the table,
//! its namespace chain, or the warehouse — and likewise for views. Views
//! reuse the leaf-native privileges (`READ`, `WRITE`, `COMMIT`, `DROP`);
//! only creation has its own privilege (`CREATE_VIEW`), so a table-writer
//! role is not silently a view-creator.

use axum::extract::{Path, Query, State};
use axum::http::StatusCode;
use axum::{Extension, Json};
use chrono::{DateTime, Utc};
use meridian_common::MeridianError;
use meridian_common::principal::Principal;
use meridian_store::rbac::{self, Grantee, Privilege, SecurableScope, SecurableType};
use meridian_store::{namespace, table, tenancy, view, warehouse};
use serde::{Deserialize, Serialize};
use sqlx::PgPool;

use crate::AppState;
use crate::error::ApiError;

/// Longest accepted role name.
const MAX_ROLE_NAME_LEN: usize = 100;

// ---------------------------------------------------------------------------
// Authorization helpers (used by every enforced handler)
// ---------------------------------------------------------------------------

/// 403 in the IRC envelope (see the module docs for the type choice).
pub(crate) fn forbidden(message: impl Into<String>) -> ApiError {
    ApiError::new(StatusCode::FORBIDDEN, "ForbiddenException", message)
}

/// Maps an authorization failure onto the HTTP boundary.
fn authz_to_api(error: rbac::AuthzError) -> ApiError {
    match error {
        rbac::AuthzError::Forbidden(message) => forbidden(message),
        rbac::AuthzError::Store(error) => ApiError::from(error),
    }
}

/// Requires `privilege` on `scope` for `principal`, or 403.
pub(crate) async fn require(
    pool: &PgPool,
    principal: &Principal,
    privilege: Privilege,
    scope: &SecurableScope,
) -> Result<(), ApiError> {
    rbac::authorize(pool, principal, privilege, scope)
        .await
        .map_err(authz_to_api)
}

/// Requires management access (admin role or any `MANAGE_WAREHOUSE`
/// grant), or 403.
pub(crate) async fn require_management(
    pool: &PgPool,
    principal: &Principal,
) -> Result<(), ApiError> {
    rbac::authorize_management(pool, principal)
        .await
        .map_err(authz_to_api)
}

/// The self-and-ancestors namespace chain for scope construction.
pub(crate) async fn namespace_scope_chain(
    pool: &PgPool,
    warehouse_id: &str,
    levels: &[String],
) -> Result<Vec<String>, ApiError> {
    Ok(rbac::namespace_chain(pool, warehouse_id, levels).await?)
}

/// Maps store-layer not-found/conflict errors of the management API onto
/// generic (non-IRC-specific) envelope types.
fn management_error(error: MeridianError) -> ApiError {
    match error {
        MeridianError::NotFound(message) => {
            ApiError::new(StatusCode::NOT_FOUND, "NotFoundException", message)
        }
        MeridianError::Conflict(message) => ApiError::already_exists(message),
        MeridianError::Validation(message) => ApiError::bad_request(message),
        other => ApiError::from(other),
    }
}

// ---------------------------------------------------------------------------
// Roles
// ---------------------------------------------------------------------------

/// Request body for `POST /api/v2/roles`.
#[derive(Debug, Deserialize)]
pub struct CreateRoleRequest {
    /// Role name, unique per workspace.
    pub name: String,
    /// Optional human description.
    #[serde(default)]
    pub description: Option<String>,
}

/// A role as rendered by the management API.
#[derive(Debug, Serialize)]
pub struct RoleResponse {
    /// ULID of the role.
    pub id: String,
    /// Role name.
    pub name: String,
    /// Optional human description.
    pub description: Option<String>,
    /// Whether this is a built-in role (undeletable, code-defined
    /// semantics).
    pub built_in: bool,
    /// Creation time.
    pub created_at: DateTime<Utc>,
}

impl From<rbac::RoleRecord> for RoleResponse {
    fn from(record: rbac::RoleRecord) -> Self {
        Self {
            id: record.id,
            name: record.name,
            description: record.description,
            built_in: record.built_in,
            created_at: record.created_at,
        }
    }
}

/// Response body for `GET /api/v2/roles`.
#[derive(Debug, Serialize)]
pub struct ListRolesResponse {
    /// All roles of the workspace, ordered by name.
    pub roles: Vec<RoleResponse>,
}

/// Validates a role name: 1–100 characters, no control characters.
fn validate_role_name(name: &str) -> Result<(), ApiError> {
    if name.is_empty() || name.len() > MAX_ROLE_NAME_LEN {
        return Err(ApiError::bad_request(format!(
            "role name must be 1–{MAX_ROLE_NAME_LEN} characters"
        )));
    }
    if name.chars().any(char::is_control) {
        return Err(ApiError::bad_request(
            "role name must not contain control characters",
        ));
    }
    Ok(())
}

/// `POST /api/v2/roles` — create a role.
pub async fn create_role(
    State(state): State<AppState>,
    Extension(principal): Extension<Principal>,
    Json(request): Json<CreateRoleRequest>,
) -> Result<(StatusCode, Json<RoleResponse>), ApiError> {
    require_management(&state.pool, &principal).await?;
    validate_role_name(&request.name)?;

    let record = rbac::create_role(
        &state.pool,
        tenancy::default_workspace_id(),
        &request.name,
        request.description.as_deref(),
        &principal.audit_string(),
    )
    .await
    .map_err(management_error)?;

    Ok((StatusCode::CREATED, Json(record.into())))
}

/// `GET /api/v2/roles` — list roles.
pub async fn list_roles(
    State(state): State<AppState>,
    Extension(principal): Extension<Principal>,
) -> Result<Json<ListRolesResponse>, ApiError> {
    require_management(&state.pool, &principal).await?;
    let roles = rbac::list_roles(&state.pool, tenancy::default_workspace_id())
        .await?
        .into_iter()
        .map(RoleResponse::from)
        .collect();
    Ok(Json(ListRolesResponse { roles }))
}

/// `DELETE /api/v2/roles/{name}` — delete a (non-built-in) role. Bindings
/// and grants of the role are removed with it.
pub async fn delete_role(
    State(state): State<AppState>,
    Extension(principal): Extension<Principal>,
    Path(name): Path<String>,
) -> Result<StatusCode, ApiError> {
    require_management(&state.pool, &principal).await?;
    rbac::delete_role(
        &state.pool,
        tenancy::default_workspace_id(),
        &name,
        &principal.audit_string(),
    )
    .await
    .map_err(management_error)?;
    Ok(StatusCode::NO_CONTENT)
}

// ---------------------------------------------------------------------------
// Role bindings
// ---------------------------------------------------------------------------

/// Request body for `POST /api/v2/roles/{name}/bindings`.
#[derive(Debug, Deserialize)]
pub struct CreateBindingRequest {
    /// ULID of the principal to bind (from `GET /api/v2/principals`).
    pub principal_id: String,
}

/// Resolves a role name or 404.
async fn resolve_role(pool: &PgPool, name: &str) -> Result<rbac::RoleRecord, ApiError> {
    rbac::get_role_by_name(pool, tenancy::default_workspace_id(), name)
        .await?
        .ok_or_else(|| {
            ApiError::new(
                StatusCode::NOT_FOUND,
                "NotFoundException",
                format!("role {name:?} does not exist"),
            )
        })
}

/// `POST /api/v2/roles/{name}/bindings` — bind a principal to a role
/// (idempotent; binding an already-bound principal is a no-op).
pub async fn create_role_binding(
    State(state): State<AppState>,
    Extension(principal): Extension<Principal>,
    Path(name): Path<String>,
    Json(request): Json<CreateBindingRequest>,
) -> Result<StatusCode, ApiError> {
    require_management(&state.pool, &principal).await?;
    let role = resolve_role(&state.pool, &name).await?;
    rbac::bind_role(
        &state.pool,
        tenancy::default_workspace_id(),
        &role.id,
        &request.principal_id,
        &principal.audit_string(),
    )
    .await
    .map_err(management_error)?;
    Ok(StatusCode::NO_CONTENT)
}

/// `DELETE /api/v2/roles/{name}/bindings/{principal_id}` — remove a
/// principal's binding to a role.
pub async fn delete_role_binding(
    State(state): State<AppState>,
    Extension(principal): Extension<Principal>,
    Path((name, principal_id)): Path<(String, String)>,
) -> Result<StatusCode, ApiError> {
    require_management(&state.pool, &principal).await?;
    let role = resolve_role(&state.pool, &name).await?;
    rbac::unbind_role(
        &state.pool,
        tenancy::default_workspace_id(),
        &role.id,
        &principal_id,
        &principal.audit_string(),
    )
    .await
    .map_err(management_error)?;
    Ok(StatusCode::NO_CONTENT)
}

// ---------------------------------------------------------------------------
// Grants
// ---------------------------------------------------------------------------

/// The securable a grant attaches to, addressed by name (resolved to ids
/// server-side).
#[derive(Debug, Deserialize)]
pub struct SecurableSelector {
    /// `warehouse` | `namespace` | `table` | `view` | `asset`.
    #[serde(rename = "type")]
    pub securable_type: String,
    /// Warehouse name (required for every type except `asset`, which is
    /// workspace-scoped and addressed by id).
    #[serde(default)]
    pub warehouse: Option<String>,
    /// Namespace levels (required for `namespace`, `table`, and `view`).
    #[serde(default)]
    pub namespace: Option<Vec<String>>,
    /// Table name (required for `table`).
    #[serde(default)]
    pub table: Option<String>,
    /// View name (required for `view`).
    #[serde(default)]
    pub view: Option<String>,
    /// Generic-asset id (required for `asset`, Pillar I).
    #[serde(default)]
    pub asset: Option<String>,
}

/// Request body for `POST /api/v2/grants`. Exactly one of `role` /
/// `principal_id` selects the grantee.
#[derive(Debug, Deserialize)]
pub struct CreateGrantRequest {
    /// Privilege to grant (e.g. `READ`).
    pub privilege: String,
    /// Grantee role name.
    #[serde(default)]
    pub role: Option<String>,
    /// Grantee principal ULID.
    #[serde(default)]
    pub principal_id: Option<String>,
    /// What the grant attaches to.
    pub securable: SecurableSelector,
}

/// A grant as rendered by the management API.
#[derive(Debug, Serialize)]
pub struct GrantResponse {
    /// ULID of the grant.
    pub id: String,
    /// The granted privilege.
    pub privilege: String,
    /// Grantee role name, if role-granted.
    pub role: Option<String>,
    /// Grantee principal ULID, if principal-granted.
    pub principal_id: Option<String>,
    /// Securable type (`warehouse` | `namespace` | `table` | `view`).
    pub securable_type: String,
    /// ULID of the securable.
    pub securable_id: String,
    /// Audit string of the granting principal.
    pub granted_by: String,
    /// Creation time.
    pub created_at: DateTime<Utc>,
}

impl From<rbac::GrantRecord> for GrantResponse {
    fn from(record: rbac::GrantRecord) -> Self {
        Self {
            id: record.id,
            privilege: record.privilege,
            role: record.role_name,
            principal_id: record.principal_id,
            securable_type: record.securable_type,
            securable_id: record.securable_id,
            granted_by: record.granted_by,
            created_at: record.created_at,
        }
    }
}

/// Response body for `GET /api/v2/grants`.
#[derive(Debug, Serialize)]
pub struct ListGrantsResponse {
    /// All grants of the workspace, newest first.
    pub grants: Vec<GrantResponse>,
}

/// Resolves a securable selector to its `(type, id)` pair.
async fn resolve_securable(
    pool: &PgPool,
    selector: &SecurableSelector,
) -> Result<(SecurableType, String), ApiError> {
    let securable_type = SecurableType::parse(&selector.securable_type).ok_or_else(|| {
        ApiError::bad_request(format!(
            "invalid securable type {:?}: expected warehouse, namespace, table, view, or asset",
            selector.securable_type
        ))
    })?;

    // An asset is workspace-scoped (Pillar I): addressed by its id, with no
    // warehouse/namespace context. Resolve it before the warehouse lookup that
    // every other type needs.
    if securable_type == SecurableType::Asset {
        let asset_id = selector
            .asset
            .as_deref()
            .filter(|id| !id.is_empty())
            .ok_or_else(|| {
                ApiError::bad_request(
                    "securable.asset (an asset id) is required for type \"asset\"",
                )
            })?;
        let asset =
            meridian_store::assets::get_asset(pool, tenancy::default_workspace_id(), asset_id)
                .await?
                .ok_or_else(|| {
                    ApiError::new(
                        StatusCode::NOT_FOUND,
                        "NotFoundException",
                        format!("no asset {asset_id}"),
                    )
                })?;
        return Ok((securable_type, asset.id));
    }

    let warehouse_name = selector.warehouse.as_deref().ok_or_else(|| {
        ApiError::bad_request("securable.warehouse is required for this securable type")
    })?;
    let wh = warehouse::get_by_name(pool, tenancy::default_workspace_id(), warehouse_name)
        .await?
        .ok_or_else(|| ApiError::no_such_warehouse(warehouse_name))?;

    match securable_type {
        SecurableType::Warehouse => Ok((securable_type, wh.id)),
        // Handled above; unreachable here.
        SecurableType::Asset => unreachable!("asset resolved before warehouse lookup"),
        SecurableType::Namespace | SecurableType::Table | SecurableType::View => {
            let levels = selector
                .namespace
                .as_deref()
                .filter(|l| !l.is_empty())
                .ok_or_else(|| {
                    ApiError::bad_request("securable.namespace is required for this type")
                })?;
            let ns = namespace::get(pool, &wh.id, levels).await?.ok_or_else(|| {
                ApiError::no_such_namespace(format!(
                    "namespace {:?} does not exist",
                    levels.join(".")
                ))
            })?;
            match securable_type {
                SecurableType::Namespace => Ok((securable_type, ns.id)),
                SecurableType::View => {
                    let name = selector
                        .view
                        .as_deref()
                        .filter(|n| !n.is_empty())
                        .ok_or_else(|| {
                            ApiError::bad_request("securable.view is required for type \"view\"")
                        })?;
                    let record = view::get_by_name(pool, &wh.id, levels, name)
                        .await?
                        .ok_or_else(|| {
                            ApiError::new(
                                StatusCode::NOT_FOUND,
                                "NoSuchViewException",
                                format!("view {name:?} does not exist"),
                            )
                        })?;
                    Ok((securable_type, record.id))
                }
                _ => {
                    let name = selector
                        .table
                        .as_deref()
                        .filter(|n| !n.is_empty())
                        .ok_or_else(|| {
                            ApiError::bad_request("securable.table is required for type \"table\"")
                        })?;
                    let record = table::get_by_name(pool, &wh.id, levels, name)
                        .await?
                        .ok_or_else(|| {
                            ApiError::no_such_table(format!("table {name:?} does not exist"))
                        })?;
                    Ok((securable_type, record.id))
                }
            }
        }
    }
}

/// Resolves the grantee of a create-grant request (role name or principal
/// id; exactly one).
async fn resolve_grantee(pool: &PgPool, request: &CreateGrantRequest) -> Result<Grantee, ApiError> {
    match (&request.role, &request.principal_id) {
        (Some(role), None) => Ok(Grantee::Role(resolve_role(pool, role).await?.id)),
        (None, Some(principal_id)) => Ok(Grantee::Principal(principal_id.clone())),
        _ => Err(ApiError::bad_request(
            "exactly one of \"role\" and \"principal_id\" must be set",
        )),
    }
}

/// `POST /api/v2/grants` — create a grant.
pub async fn create_grant(
    State(state): State<AppState>,
    Extension(principal): Extension<Principal>,
    Json(request): Json<CreateGrantRequest>,
) -> Result<(StatusCode, Json<GrantResponse>), ApiError> {
    require_management(&state.pool, &principal).await?;

    let privilege = Privilege::parse(&request.privilege).ok_or_else(|| {
        ApiError::bad_request(format!(
            "unknown privilege {:?}; expected one of: {}",
            request.privilege,
            Privilege::ALL.map(Privilege::as_str).join(", ")
        ))
    })?;
    let grantee = resolve_grantee(&state.pool, &request).await?;
    let (securable_type, securable_id) = resolve_securable(&state.pool, &request.securable).await?;

    let record = rbac::create_grant(
        &state.pool,
        tenancy::default_workspace_id(),
        &grantee,
        securable_type,
        &securable_id,
        privilege,
        &principal.audit_string(),
    )
    .await
    .map_err(management_error)?;

    Ok((StatusCode::CREATED, Json(record.into())))
}

/// `GET /api/v2/grants` — list grants.
pub async fn list_grants(
    State(state): State<AppState>,
    Extension(principal): Extension<Principal>,
) -> Result<Json<ListGrantsResponse>, ApiError> {
    require_management(&state.pool, &principal).await?;
    let grants = rbac::list_grants(&state.pool, tenancy::default_workspace_id())
        .await?
        .into_iter()
        .map(GrantResponse::from)
        .collect();
    Ok(Json(ListGrantsResponse { grants }))
}

/// `DELETE /api/v2/grants/{id}` — delete a grant.
pub async fn delete_grant(
    State(state): State<AppState>,
    Extension(principal): Extension<Principal>,
    Path(id): Path<String>,
) -> Result<StatusCode, ApiError> {
    require_management(&state.pool, &principal).await?;
    rbac::delete_grant(
        &state.pool,
        tenancy::default_workspace_id(),
        &id,
        &principal.audit_string(),
    )
    .await
    .map_err(management_error)?;
    Ok(StatusCode::NO_CONTENT)
}

// ---------------------------------------------------------------------------
// Effective permissions
// ---------------------------------------------------------------------------

/// Query parameters for `GET /api/v2/permissions`.
#[derive(Debug, Deserialize)]
pub struct PermissionsQuery {
    /// ULID of the principal to inspect.
    pub principal: String,
}

/// One effective permission row.
#[derive(Debug, Serialize)]
pub struct PermissionResponse {
    /// The privilege.
    pub privilege: String,
    /// Securable type of the grant.
    pub securable_type: String,
    /// Securable id of the grant.
    pub securable_id: String,
    /// `"direct"` or `"role:<name>"`.
    pub via: String,
}

/// Response body for `GET /api/v2/permissions`.
#[derive(Debug, Serialize)]
pub struct PermissionsResponse {
    /// The inspected principal.
    pub principal_id: String,
    /// Role memberships (built-in roles carry blanket permissions that are
    /// not expanded into rows: `admin` = everything, `catalog_reader` =
    /// read-only everything).
    pub roles: Vec<String>,
    /// Grants applying to the principal, directly or via roles.
    pub permissions: Vec<PermissionResponse>,
}

/// `GET /api/v2/permissions?principal=<id>` — a principal's effective
/// permissions.
pub async fn get_permissions(
    State(state): State<AppState>,
    Extension(principal): Extension<Principal>,
    Query(query): Query<PermissionsQuery>,
) -> Result<Json<PermissionsResponse>, ApiError> {
    require_management(&state.pool, &principal).await?;

    let effective = rbac::effective_permissions(&state.pool, &query.principal).await?;
    let permissions = effective
        .permissions
        .into_iter()
        .map(|p| PermissionResponse {
            privilege: p.privilege,
            securable_type: p.securable_type,
            securable_id: p.securable_id,
            via: p
                .via_role
                .map_or_else(|| "direct".to_owned(), |role| format!("role:{role}")),
        })
        .collect();

    Ok(Json(PermissionsResponse {
        principal_id: query.principal,
        roles: effective.roles,
        permissions,
    }))
}
