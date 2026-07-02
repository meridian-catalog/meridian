//! Role-based access control: privileges, roles, bindings, grants, and the
//! authorization decision (Pillar A / F8 — RBAC only; attribute-based
//! policies are a later milestone).
//!
//! # Model
//!
//! A **grant** gives one [`Privilege`] on one securable (warehouse,
//! namespace, table, or view) to exactly one grantee: a role XOR a
//! principal. [`authorize`] resolves a decision from:
//!
//! 1. direct grants to the calling principal,
//! 2. grants to roles the principal is bound to,
//! 3. hierarchy inheritance: a grant on a warehouse covers every
//!    namespace, table, and view it contains; a grant on a namespace
//!    covers its child namespaces, tables, and views (the caller supplies
//!    the resolved chain via [`SecurableScope`]),
//! 4. built-in role semantics: a binding to `admin` allows everything; a
//!    binding to `catalog_reader` allows the read-only privileges
//!    (`LIST_NAMESPACES`, `LIST_TABLES`, `READ`) on everything — tables
//!    and views alike (`LIST_TABLES` also gates `listViews`, and `READ`
//!    gates `loadView`, so the read-only set needs no view-specific
//!    entries).
//!
//! Deny by default: no matching grant means [`AuthzError::Forbidden`].
//!
//! # Anonymous bypass
//!
//! When authentication is disabled (`auth.mode = "disabled"`), every
//! request runs as the anonymous principal and **bypasses authorization
//! entirely** — that mode is a dev loop, not a deployment posture, and the
//! server already logs a loud warning at startup. In `oidc` mode the
//! anonymous principal never reaches an authorization check (the
//! middleware rejects unauthenticated requests first).
//!
//! # Performance
//!
//! Nothing is cached: every check is one round-trip to Postgres so the
//! semantics are trivially correct. TODO(benchmark phase): measure the
//! per-request cost and add a short-TTL decision cache (or a
//! principal-scoped grant snapshot) if it shows up; invalidation must
//! cover grant/binding/role mutations.

use chrono::{DateTime, Utc};
use meridian_common::id::WorkspaceId;
use meridian_common::principal::{Principal, PrincipalKind};
use meridian_common::{MeridianError, Result};
use serde_json::json;
use sqlx::PgPool;
use ulid::Ulid;

use crate::audit::{self, NewAuditEntry};
use crate::outbox::{self, NewOutboxEvent};
use crate::{map_sqlx_error, principal};

/// Audit principal recorded for startup bootstrap actions.
const BOOTSTRAP_ACTOR: &str = "system:bootstrap";

/// Name of the built-in role that allows everything.
pub const ADMIN_ROLE: &str = "admin";

/// Name of the built-in read-only role.
pub const CATALOG_READER_ROLE: &str = "catalog_reader";

// ---------------------------------------------------------------------------
// Privileges and securables
// ---------------------------------------------------------------------------

/// The closed set of grantable privileges (mirrored by the CHECK constraint
/// in migration 0005).
///
/// Each privilege has a *native* securable type but may also be granted on
/// any ancestor securable, where it applies to everything contained (e.g.
/// `READ` granted on a warehouse allows reading every table in it).
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Privilege {
    /// Administer a warehouse (delete it, manage grants on it).
    ManageWarehouse,
    /// Create namespaces in a warehouse.
    CreateNamespace,
    /// List and read namespace metadata in a warehouse.
    ListNamespaces,
    /// Administer a namespace (drop it, update its properties).
    ManageNamespace,
    /// Create (or register) tables in a namespace.
    CreateTable,
    /// List table identifiers in a namespace.
    ListTables,
    /// Create views in a namespace (`createView`, and the destination
    /// side of `renameView`).
    CreateView,
    /// Load table metadata.
    Read,
    /// Write-adjacent table operations that are not commits (rename,
    /// metrics reports).
    Write,
    /// Commit metadata changes to a table.
    Commit,
    /// Drop a table.
    Drop,
}

/// What a grant can attach to.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum SecurableType {
    /// A warehouse (grants on it cover everything inside).
    Warehouse,
    /// A namespace (grants on it cover child namespaces, tables, and
    /// views).
    Namespace,
    /// A single table.
    Table,
    /// A single view.
    View,
}

impl Privilege {
    /// Every privilege, for iteration and validation messages.
    pub const ALL: [Self; 11] = [
        Self::ManageWarehouse,
        Self::CreateNamespace,
        Self::ListNamespaces,
        Self::ManageNamespace,
        Self::CreateTable,
        Self::ListTables,
        Self::CreateView,
        Self::Read,
        Self::Write,
        Self::Commit,
        Self::Drop,
    ];

    /// The database/wire rendering (matches the 0005 CHECK constraint).
    #[must_use]
    pub fn as_str(self) -> &'static str {
        match self {
            Self::ManageWarehouse => "MANAGE_WAREHOUSE",
            Self::CreateNamespace => "CREATE_NAMESPACE",
            Self::ListNamespaces => "LIST_NAMESPACES",
            Self::ManageNamespace => "MANAGE_NAMESPACE",
            Self::CreateTable => "CREATE_TABLE",
            Self::ListTables => "LIST_TABLES",
            Self::CreateView => "CREATE_VIEW",
            Self::Read => "READ",
            Self::Write => "WRITE",
            Self::Commit => "COMMIT",
            Self::Drop => "DROP",
        }
    }

    /// Parses the wire rendering back into the enum.
    #[must_use]
    pub fn parse(raw: &str) -> Option<Self> {
        Self::ALL.into_iter().find(|p| p.as_str() == raw)
    }

    /// The securable type the privilege natively belongs to. `READ`,
    /// `WRITE`, `COMMIT`, and `DROP` are leaf-native: they apply to tables
    /// and views alike (tables and views sit at the same hierarchy rank).
    #[must_use]
    pub fn native_securable(self) -> SecurableType {
        match self {
            Self::ManageWarehouse | Self::CreateNamespace | Self::ListNamespaces => {
                SecurableType::Warehouse
            }
            Self::ManageNamespace | Self::CreateTable | Self::ListTables | Self::CreateView => {
                SecurableType::Namespace
            }
            Self::Read | Self::Write | Self::Commit | Self::Drop => SecurableType::Table,
        }
    }

    /// Whether the privilege may be granted on the given securable type:
    /// its native type or any ancestor (inheritance flows downward only).
    /// Tables and views are sibling leaves, so a leaf-native privilege
    /// (`READ`, ...) is grantable on either.
    #[must_use]
    pub fn grantable_on(self, securable: SecurableType) -> bool {
        let rank = |t: SecurableType| match t {
            SecurableType::Warehouse => 0,
            SecurableType::Namespace => 1,
            SecurableType::Table | SecurableType::View => 2,
        };
        rank(securable) <= rank(self.native_securable())
    }

    /// Whether the built-in `catalog_reader` role covers this privilege.
    #[must_use]
    pub fn is_read_only(self) -> bool {
        matches!(self, Self::ListNamespaces | Self::ListTables | Self::Read)
    }
}

impl std::fmt::Display for Privilege {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str(self.as_str())
    }
}

impl SecurableType {
    /// The database/wire rendering.
    #[must_use]
    pub fn as_str(self) -> &'static str {
        match self {
            Self::Warehouse => "warehouse",
            Self::Namespace => "namespace",
            Self::Table => "table",
            Self::View => "view",
        }
    }

    /// Parses the wire rendering back into the enum.
    #[must_use]
    pub fn parse(raw: &str) -> Option<Self> {
        match raw {
            "warehouse" => Some(Self::Warehouse),
            "namespace" => Some(Self::Namespace),
            "table" => Some(Self::Table),
            "view" => Some(Self::View),
            _ => None,
        }
    }
}

impl std::fmt::Display for SecurableType {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str(self.as_str())
    }
}

/// The set of securable ids a grant could match for one authorization
/// check: the target plus every ancestor it inherits from.
///
/// Built by the handler that already resolved the target (warehouse id,
/// the namespace's self-and-ancestors chain from [`namespace_chain`], and
/// the table or view id when applicable).
#[derive(Debug, Clone, Default)]
pub struct SecurableScope {
    /// The containing warehouse.
    pub warehouse_id: Option<String>,
    /// The namespace itself plus all its ancestors (any order).
    pub namespace_ids: Vec<String>,
    /// The table itself.
    pub table_id: Option<String>,
    /// The view itself.
    pub view_id: Option<String>,
}

impl SecurableScope {
    /// Scope for a warehouse-level check.
    #[must_use]
    pub fn warehouse(warehouse_id: &str) -> Self {
        Self {
            warehouse_id: Some(warehouse_id.to_owned()),
            ..Self::default()
        }
    }

    /// Scope for a namespace-level check (`namespace_ids` is the
    /// self-and-ancestors chain; warehouse grants inherit).
    #[must_use]
    pub fn namespace(warehouse_id: &str, namespace_ids: Vec<String>) -> Self {
        Self {
            warehouse_id: Some(warehouse_id.to_owned()),
            namespace_ids,
            ..Self::default()
        }
    }

    /// Scope for a table-level check. `table_id` is optional so callers
    /// can authorize an operation on a table that may not exist (the
    /// namespace/warehouse grants still decide) without leaking existence.
    #[must_use]
    pub fn table(warehouse_id: &str, namespace_ids: Vec<String>, table_id: Option<&str>) -> Self {
        Self {
            warehouse_id: Some(warehouse_id.to_owned()),
            namespace_ids,
            table_id: table_id.map(str::to_owned),
            view_id: None,
        }
    }

    /// Scope for a view-level check. `view_id` is optional for the same
    /// reason as [`SecurableScope::table`]: an operation on a view that
    /// may not exist can still be decided by namespace/warehouse grants
    /// without leaking existence.
    #[must_use]
    pub fn view(warehouse_id: &str, namespace_ids: Vec<String>, view_id: Option<&str>) -> Self {
        Self {
            warehouse_id: Some(warehouse_id.to_owned()),
            namespace_ids,
            table_id: None,
            view_id: view_id.map(str::to_owned),
        }
    }
}

/// Ids of a namespace and all its ancestors within a warehouse, resolved
/// from the levels array (`[a, b, c]` matches namespaces `[a]`, `[a, b]`,
/// and `[a, b, c]`). Missing levels simply contribute nothing.
pub async fn namespace_chain(
    pool: &PgPool,
    warehouse_id: &str,
    levels: &[String],
) -> Result<Vec<String>> {
    if levels.is_empty() {
        return Ok(Vec::new());
    }
    sqlx::query_scalar(
        "SELECT id FROM namespaces
         WHERE warehouse_id = $1
           AND cardinality(levels) <= cardinality($2::text[])
           AND levels = ($2::text[])[1:cardinality(levels)]",
    )
    .bind(warehouse_id)
    .bind(levels)
    .fetch_all(pool)
    .await
    .map_err(|e| map_sqlx_error("failed to resolve namespace chain", e))
}

// ---------------------------------------------------------------------------
// The authorization decision
// ---------------------------------------------------------------------------

/// Outcome of a failed authorization: denial or a store failure.
#[derive(Debug, thiserror::Error)]
pub enum AuthzError {
    /// The principal holds no grant that covers the operation.
    #[error("forbidden: {0}")]
    Forbidden(String),
    /// The check itself failed (database unavailable, ...).
    #[error(transparent)]
    Store(#[from] MeridianError),
}

/// One decision query: built-in role semantics plus direct/role grants
/// against the scope's candidate securables.
const AUTHORIZE_SQL: &str = "\
    WITH me AS (
        SELECT id FROM principals WHERE issuer = $1 AND subject = $2
    ),
    my_roles AS (
        SELECT role_id FROM role_bindings
        WHERE principal_id IN (SELECT id FROM me)
    )
    SELECT EXISTS (
        SELECT 1 FROM roles r
         WHERE r.id IN (SELECT role_id FROM my_roles)
           AND r.built_in
           AND (r.name = 'admin' OR (r.name = 'catalog_reader' AND $3))
        UNION ALL
        SELECT 1 FROM grants g
         WHERE g.privilege = $4
           AND (g.principal_id IN (SELECT id FROM me)
                OR g.role_id IN (SELECT role_id FROM my_roles))
           AND ((g.securable_type = 'warehouse' AND g.securable_id = $5)
             OR (g.securable_type = 'namespace' AND g.securable_id = ANY($6))
             OR (g.securable_type = 'table' AND g.securable_id = $7)
             OR (g.securable_type = 'view' AND g.securable_id = $8))
    )";

/// Decides whether `principal` may exercise `privilege` on the securable
/// described by `scope`. Deny by default; see the module docs for the
/// resolution rules and the anonymous bypass.
pub async fn authorize(
    pool: &PgPool,
    principal: &Principal,
    privilege: Privilege,
    scope: &SecurableScope,
) -> std::result::Result<(), AuthzError> {
    if principal.is_anonymous() {
        // auth.mode = "disabled": authorization is bypassed wholesale
        // (documented dev-mode posture; the server warns loudly at boot).
        return Ok(());
    }
    let Some(issuer) = principal.issuer.as_deref() else {
        // Authenticated principals always carry an issuer; a missing one
        // is a contract violation and fails closed.
        return Err(deny(privilege));
    };

    let allowed: bool = sqlx::query_scalar(AUTHORIZE_SQL)
        .bind(issuer)
        .bind(&principal.subject)
        .bind(privilege.is_read_only())
        .bind(privilege.as_str())
        .bind(scope.warehouse_id.as_deref())
        .bind(&scope.namespace_ids)
        .bind(scope.table_id.as_deref())
        .bind(scope.view_id.as_deref())
        .fetch_one(pool)
        .await
        .map_err(|e| AuthzError::Store(map_sqlx_error("failed to evaluate authorization", e)))?;

    if allowed {
        Ok(())
    } else {
        Err(deny(privilege))
    }
}

/// Whether the principal may use the RBAC management API (roles, bindings,
/// grants, permissions): a binding to the built-in `admin` role, or any
/// `MANAGE_WAREHOUSE` grant (direct or via a role) on any securable.
pub async fn authorize_management(
    pool: &PgPool,
    principal: &Principal,
) -> std::result::Result<(), AuthzError> {
    if principal.is_anonymous() {
        return Ok(());
    }
    let Some(issuer) = principal.issuer.as_deref() else {
        return Err(deny(Privilege::ManageWarehouse));
    };

    let allowed: bool = sqlx::query_scalar(
        "WITH me AS (
             SELECT id FROM principals WHERE issuer = $1 AND subject = $2
         ),
         my_roles AS (
             SELECT role_id FROM role_bindings
             WHERE principal_id IN (SELECT id FROM me)
         )
         SELECT EXISTS (
             SELECT 1 FROM roles r
              WHERE r.id IN (SELECT role_id FROM my_roles)
                AND r.built_in AND r.name = 'admin'
             UNION ALL
             SELECT 1 FROM grants g
              WHERE g.privilege = 'MANAGE_WAREHOUSE'
                AND (g.principal_id IN (SELECT id FROM me)
                     OR g.role_id IN (SELECT role_id FROM my_roles))
         )",
    )
    .bind(issuer)
    .bind(&principal.subject)
    .fetch_one(pool)
    .await
    .map_err(|e| AuthzError::Store(map_sqlx_error("failed to evaluate authorization", e)))?;

    if allowed {
        Ok(())
    } else {
        Err(AuthzError::Forbidden(
            "administering roles and grants requires the admin role or a MANAGE_WAREHOUSE grant"
                .to_owned(),
        ))
    }
}

/// The client-safe denial message: names the missing privilege, never the
/// securable (a denied caller learns nothing about what exists).
fn deny(privilege: Privilege) -> AuthzError {
    AuthzError::Forbidden(format!(
        "the {privilege} privilege is required for this operation"
    ))
}

// ---------------------------------------------------------------------------
// Roles
// ---------------------------------------------------------------------------

/// A persisted role.
#[derive(Debug, Clone, sqlx::FromRow)]
pub struct RoleRecord {
    /// ULID of the role.
    pub id: String,
    /// Owning workspace.
    pub workspace_id: String,
    /// Role name, unique per workspace.
    pub name: String,
    /// Optional human description.
    pub description: Option<String>,
    /// Whether this is a built-in role (undeletable, code-defined
    /// semantics).
    pub built_in: bool,
    /// Creation time.
    pub created_at: DateTime<Utc>,
}

const ROLE_COLUMNS: &str = "id, workspace_id, name, description, built_in, created_at";

/// True when the error is a Postgres unique-constraint violation.
fn is_unique_violation(error: &sqlx::Error) -> bool {
    error
        .as_database_error()
        .is_some_and(sqlx::error::DatabaseError::is_unique_violation)
}

/// True when the error is a Postgres foreign-key violation.
fn is_fk_violation(error: &sqlx::Error) -> bool {
    error
        .as_database_error()
        .is_some_and(sqlx::error::DatabaseError::is_foreign_key_violation)
}

/// Creates a (non-built-in) role, with its audit row and outbox event.
pub async fn create_role(
    pool: &PgPool,
    workspace_id: WorkspaceId,
    name: &str,
    description: Option<&str>,
    actor: &str,
) -> Result<RoleRecord> {
    let mut tx = pool
        .begin()
        .await
        .map_err(|e| map_sqlx_error("failed to begin role create", e))?;

    let id = Ulid::new().to_string();
    let record: RoleRecord = sqlx::query_as(&format!(
        "INSERT INTO roles (id, workspace_id, name, description, built_in)
         VALUES ($1, $2, $3, $4, FALSE)
         RETURNING {ROLE_COLUMNS}"
    ))
    .bind(&id)
    .bind(workspace_id.to_string())
    .bind(name)
    .bind(description)
    .fetch_one(&mut *tx)
    .await
    .map_err(|e| {
        if is_unique_violation(&e) {
            MeridianError::Conflict(format!("role {name:?} already exists"))
        } else {
            map_sqlx_error("failed to insert role", e)
        }
    })?;

    let details = json!({ "name": name, "description": description });
    outbox::enqueue(
        &mut *tx,
        &NewOutboxEvent {
            workspace_id: Some(workspace_id),
            aggregate: format!("role:{id}"),
            event_type: "role.created".to_owned(),
            payload: details.clone(),
        },
    )
    .await?;
    audit::append_in_tx(
        &mut tx,
        NewAuditEntry {
            workspace_id: Some(workspace_id),
            principal: actor.to_owned(),
            action: "role.create".to_owned(),
            resource: format!("role:{id}"),
            details,
        },
    )
    .await?;

    tx.commit()
        .await
        .map_err(|e| map_sqlx_error("failed to commit role create", e))?;
    Ok(record)
}

/// Lists all roles of a workspace, ordered by name.
pub async fn list_roles(pool: &PgPool, workspace_id: WorkspaceId) -> Result<Vec<RoleRecord>> {
    sqlx::query_as(&format!(
        "SELECT {ROLE_COLUMNS} FROM roles WHERE workspace_id = $1 ORDER BY name"
    ))
    .bind(workspace_id.to_string())
    .fetch_all(pool)
    .await
    .map_err(|e| map_sqlx_error("failed to list roles", e))
}

/// Looks a role up by name within a workspace.
pub async fn get_role_by_name(
    pool: &PgPool,
    workspace_id: WorkspaceId,
    name: &str,
) -> Result<Option<RoleRecord>> {
    sqlx::query_as(&format!(
        "SELECT {ROLE_COLUMNS} FROM roles WHERE workspace_id = $1 AND name = $2"
    ))
    .bind(workspace_id.to_string())
    .bind(name)
    .fetch_optional(pool)
    .await
    .map_err(|e| map_sqlx_error("failed to load role", e))
}

/// Deletes a role by name. Built-in roles cannot be deleted. Bindings and
/// grants referencing the role are removed by `ON DELETE CASCADE`
/// (recorded in the audit details).
pub async fn delete_role(
    pool: &PgPool,
    workspace_id: WorkspaceId,
    name: &str,
    actor: &str,
) -> Result<()> {
    let mut tx = pool
        .begin()
        .await
        .map_err(|e| map_sqlx_error("failed to begin role delete", e))?;

    let row: Option<(String, bool)> = sqlx::query_as(
        "SELECT id, built_in FROM roles
         WHERE workspace_id = $1 AND name = $2 FOR UPDATE",
    )
    .bind(workspace_id.to_string())
    .bind(name)
    .fetch_optional(&mut *tx)
    .await
    .map_err(|e| map_sqlx_error("failed to load role for delete", e))?;

    let Some((id, built_in)) = row else {
        return Err(MeridianError::NotFound(format!(
            "role {name:?} does not exist"
        )));
    };
    if built_in {
        return Err(MeridianError::Validation(format!(
            "role {name:?} is built-in and cannot be deleted"
        )));
    }

    sqlx::query("DELETE FROM roles WHERE id = $1")
        .bind(&id)
        .execute(&mut *tx)
        .await
        .map_err(|e| map_sqlx_error("failed to delete role", e))?;

    let details = json!({ "name": name, "cascade": "bindings and grants of this role" });
    outbox::enqueue(
        &mut *tx,
        &NewOutboxEvent {
            workspace_id: Some(workspace_id),
            aggregate: format!("role:{id}"),
            event_type: "role.deleted".to_owned(),
            payload: details.clone(),
        },
    )
    .await?;
    audit::append_in_tx(
        &mut tx,
        NewAuditEntry {
            workspace_id: Some(workspace_id),
            principal: actor.to_owned(),
            action: "role.delete".to_owned(),
            resource: format!("role:{id}"),
            details,
        },
    )
    .await?;

    tx.commit()
        .await
        .map_err(|e| map_sqlx_error("failed to commit role delete", e))
}

// ---------------------------------------------------------------------------
// Role bindings
// ---------------------------------------------------------------------------

/// Binds a principal to a role. Idempotent: binding an already-bound
/// principal is a no-op (returns `false`; nothing is audited because
/// nothing changed). Unknown role/principal ids are validation errors.
pub async fn bind_role(
    pool: &PgPool,
    workspace_id: WorkspaceId,
    role_id: &str,
    principal_id: &str,
    actor: &str,
) -> Result<bool> {
    let mut tx = pool
        .begin()
        .await
        .map_err(|e| map_sqlx_error("failed to begin role binding", e))?;

    let inserted = sqlx::query(
        "INSERT INTO role_bindings (role_id, principal_id)
         VALUES ($1, $2)
         ON CONFLICT (role_id, principal_id) DO NOTHING",
    )
    .bind(role_id)
    .bind(principal_id)
    .execute(&mut *tx)
    .await
    .map_err(|e| {
        if is_fk_violation(&e) {
            MeridianError::Validation("unknown role or principal id".to_owned())
        } else {
            map_sqlx_error("failed to insert role binding", e)
        }
    })?
    .rows_affected()
        > 0;

    if !inserted {
        // Already bound; nothing changed, nothing to audit.
        drop(tx);
        return Ok(false);
    }

    let details = json!({ "role_id": role_id, "principal_id": principal_id });
    outbox::enqueue(
        &mut *tx,
        &NewOutboxEvent {
            workspace_id: Some(workspace_id),
            aggregate: format!("role:{role_id}"),
            event_type: "role.binding.created".to_owned(),
            payload: details.clone(),
        },
    )
    .await?;
    audit::append_in_tx(
        &mut tx,
        NewAuditEntry {
            workspace_id: Some(workspace_id),
            principal: actor.to_owned(),
            action: "role.bind".to_owned(),
            resource: format!("role:{role_id}"),
            details,
        },
    )
    .await?;

    tx.commit()
        .await
        .map_err(|e| map_sqlx_error("failed to commit role binding", e))?;
    Ok(true)
}

/// Removes a principal's binding to a role.
pub async fn unbind_role(
    pool: &PgPool,
    workspace_id: WorkspaceId,
    role_id: &str,
    principal_id: &str,
    actor: &str,
) -> Result<()> {
    let mut tx = pool
        .begin()
        .await
        .map_err(|e| map_sqlx_error("failed to begin role unbinding", e))?;

    let removed = sqlx::query("DELETE FROM role_bindings WHERE role_id = $1 AND principal_id = $2")
        .bind(role_id)
        .bind(principal_id)
        .execute(&mut *tx)
        .await
        .map_err(|e| map_sqlx_error("failed to delete role binding", e))?
        .rows_affected()
        > 0;
    if !removed {
        return Err(MeridianError::NotFound(
            "the principal is not bound to the role".to_owned(),
        ));
    }

    let details = json!({ "role_id": role_id, "principal_id": principal_id });
    outbox::enqueue(
        &mut *tx,
        &NewOutboxEvent {
            workspace_id: Some(workspace_id),
            aggregate: format!("role:{role_id}"),
            event_type: "role.binding.deleted".to_owned(),
            payload: details.clone(),
        },
    )
    .await?;
    audit::append_in_tx(
        &mut tx,
        NewAuditEntry {
            workspace_id: Some(workspace_id),
            principal: actor.to_owned(),
            action: "role.unbind".to_owned(),
            resource: format!("role:{role_id}"),
            details,
        },
    )
    .await?;

    tx.commit()
        .await
        .map_err(|e| map_sqlx_error("failed to commit role unbinding", e))
}

// ---------------------------------------------------------------------------
// Grants
// ---------------------------------------------------------------------------

/// The grantee of a grant: a role XOR a principal (by id).
#[derive(Debug, Clone)]
pub enum Grantee {
    /// Grant to a role.
    Role(String),
    /// Grant to a principal.
    Principal(String),
}

/// A persisted grant. `role_name` is joined in for display; exactly one of
/// `role_id`/`principal_id` is set.
#[derive(Debug, Clone, sqlx::FromRow)]
pub struct GrantRecord {
    /// ULID of the grant.
    pub id: String,
    /// Owning workspace.
    pub workspace_id: String,
    /// Grantee role, if role-granted.
    pub role_id: Option<String>,
    /// Name of the grantee role, if role-granted.
    pub role_name: Option<String>,
    /// Grantee principal, if principal-granted.
    pub principal_id: Option<String>,
    /// Securable type (`warehouse` | `namespace` | `table` | `view`).
    pub securable_type: String,
    /// ULID of the securable.
    pub securable_id: String,
    /// The granted privilege (wire rendering).
    pub privilege: String,
    /// Audit string of the granting principal.
    pub granted_by: String,
    /// Creation time.
    pub created_at: DateTime<Utc>,
}

const GRANT_COLUMNS: &str = "g.id, g.workspace_id, g.role_id, r.name AS role_name, \
                             g.principal_id, g.securable_type, g.securable_id, g.privilege, \
                             g.granted_by, g.created_at";

/// Creates a grant, with its audit row and outbox event. The privilege
/// must be grantable on the securable type ([`Privilege::grantable_on`]);
/// duplicates are conflicts.
pub async fn create_grant(
    pool: &PgPool,
    workspace_id: WorkspaceId,
    grantee: &Grantee,
    securable_type: SecurableType,
    securable_id: &str,
    privilege: Privilege,
    granted_by: &str,
) -> Result<GrantRecord> {
    if !privilege.grantable_on(securable_type) {
        return Err(MeridianError::Validation(format!(
            "privilege {privilege} cannot be granted on a {securable_type} \
             (native scope: {})",
            privilege.native_securable()
        )));
    }
    let (role_id, principal_id) = match grantee {
        Grantee::Role(id) => (Some(id.as_str()), None),
        Grantee::Principal(id) => (None, Some(id.as_str())),
    };

    let mut tx = pool
        .begin()
        .await
        .map_err(|e| map_sqlx_error("failed to begin grant create", e))?;

    let id = Ulid::new().to_string();
    let record: GrantRecord = sqlx::query_as(&format!(
        "WITH inserted AS (
             INSERT INTO grants
                 (id, workspace_id, role_id, principal_id, securable_type,
                  securable_id, privilege, granted_by)
             VALUES ($1, $2, $3, $4, $5, $6, $7, $8)
             RETURNING *
         )
         SELECT {GRANT_COLUMNS}
         FROM inserted g LEFT JOIN roles r ON r.id = g.role_id"
    ))
    .bind(&id)
    .bind(workspace_id.to_string())
    .bind(role_id)
    .bind(principal_id)
    .bind(securable_type.as_str())
    .bind(securable_id)
    .bind(privilege.as_str())
    .bind(granted_by)
    .fetch_one(&mut *tx)
    .await
    .map_err(|e| {
        if is_unique_violation(&e) {
            MeridianError::Conflict("an identical grant already exists".to_owned())
        } else if is_fk_violation(&e) {
            MeridianError::Validation("unknown role or principal id".to_owned())
        } else {
            map_sqlx_error("failed to insert grant", e)
        }
    })?;

    let details = json!({
        "role_id": record.role_id,
        "principal_id": record.principal_id,
        "securable_type": record.securable_type,
        "securable_id": record.securable_id,
        "privilege": record.privilege,
    });
    outbox::enqueue(
        &mut *tx,
        &NewOutboxEvent {
            workspace_id: Some(workspace_id),
            aggregate: format!("grant:{id}"),
            event_type: "grant.created".to_owned(),
            payload: details.clone(),
        },
    )
    .await?;
    audit::append_in_tx(
        &mut tx,
        NewAuditEntry {
            workspace_id: Some(workspace_id),
            principal: granted_by.to_owned(),
            action: "grant.create".to_owned(),
            resource: format!("grant:{id}"),
            details,
        },
    )
    .await?;

    tx.commit()
        .await
        .map_err(|e| map_sqlx_error("failed to commit grant create", e))?;
    Ok(record)
}

/// Lists all grants of a workspace, newest first.
pub async fn list_grants(pool: &PgPool, workspace_id: WorkspaceId) -> Result<Vec<GrantRecord>> {
    sqlx::query_as(&format!(
        "SELECT {GRANT_COLUMNS}
         FROM grants g LEFT JOIN roles r ON r.id = g.role_id
         WHERE g.workspace_id = $1
         ORDER BY g.id DESC"
    ))
    .bind(workspace_id.to_string())
    .fetch_all(pool)
    .await
    .map_err(|e| map_sqlx_error("failed to list grants", e))
}

/// Deletes a grant by id, with its audit row and outbox event.
pub async fn delete_grant(
    pool: &PgPool,
    workspace_id: WorkspaceId,
    grant_id: &str,
    actor: &str,
) -> Result<()> {
    let mut tx = pool
        .begin()
        .await
        .map_err(|e| map_sqlx_error("failed to begin grant delete", e))?;

    let deleted: Option<GrantRecord> = sqlx::query_as(&format!(
        "WITH deleted AS (
             DELETE FROM grants WHERE id = $1 AND workspace_id = $2
             RETURNING *
         )
         SELECT {GRANT_COLUMNS}
         FROM deleted g LEFT JOIN roles r ON r.id = g.role_id"
    ))
    .bind(grant_id)
    .bind(workspace_id.to_string())
    .fetch_optional(&mut *tx)
    .await
    .map_err(|e| map_sqlx_error("failed to delete grant", e))?;

    let Some(record) = deleted else {
        return Err(MeridianError::NotFound(format!(
            "grant {grant_id:?} does not exist"
        )));
    };

    let details = json!({
        "role_id": record.role_id,
        "principal_id": record.principal_id,
        "securable_type": record.securable_type,
        "securable_id": record.securable_id,
        "privilege": record.privilege,
    });
    outbox::enqueue(
        &mut *tx,
        &NewOutboxEvent {
            workspace_id: Some(workspace_id),
            aggregate: format!("grant:{grant_id}"),
            event_type: "grant.deleted".to_owned(),
            payload: details.clone(),
        },
    )
    .await?;
    audit::append_in_tx(
        &mut tx,
        NewAuditEntry {
            workspace_id: Some(workspace_id),
            principal: actor.to_owned(),
            action: "grant.delete".to_owned(),
            resource: format!("grant:{grant_id}"),
            details,
        },
    )
    .await?;

    tx.commit()
        .await
        .map_err(|e| map_sqlx_error("failed to commit grant delete", e))
}

// ---------------------------------------------------------------------------
// Effective permissions
// ---------------------------------------------------------------------------

/// One effective permission of a principal: a grant that applies to it,
/// with its provenance.
#[derive(Debug, Clone, sqlx::FromRow)]
pub struct EffectivePermission {
    /// The privilege (wire rendering).
    pub privilege: String,
    /// Securable type of the grant.
    pub securable_type: String,
    /// Securable id of the grant.
    pub securable_id: String,
    /// `NULL` for direct grants; the role name for role-derived ones.
    pub via_role: Option<String>,
}

/// Everything a principal can currently do: its role memberships plus every
/// grant that applies (direct or via bindings). Built-in role bindings
/// appear in `roles`; their code-defined blanket permissions are not
/// expanded into rows.
#[derive(Debug, Clone)]
pub struct EffectivePermissions {
    /// Names of the roles the principal is bound to.
    pub roles: Vec<String>,
    /// Grants applying to the principal.
    pub permissions: Vec<EffectivePermission>,
}

/// Resolves the effective permissions of one principal (by principal id).
pub async fn effective_permissions(
    pool: &PgPool,
    principal_id: &str,
) -> Result<EffectivePermissions> {
    let roles: Vec<String> = sqlx::query_scalar(
        "SELECT r.name FROM roles r
         JOIN role_bindings rb ON rb.role_id = r.id
         WHERE rb.principal_id = $1
         ORDER BY r.name",
    )
    .bind(principal_id)
    .fetch_all(pool)
    .await
    .map_err(|e| map_sqlx_error("failed to list role memberships", e))?;

    let permissions: Vec<EffectivePermission> = sqlx::query_as(
        "SELECT g.privilege, g.securable_type, g.securable_id, r.name AS via_role
         FROM grants g
         LEFT JOIN roles r ON r.id = g.role_id
         WHERE g.principal_id = $1
            OR g.role_id IN (SELECT role_id FROM role_bindings WHERE principal_id = $1)
         ORDER BY g.id",
    )
    .bind(principal_id)
    .fetch_all(pool)
    .await
    .map_err(|e| map_sqlx_error("failed to list effective permissions", e))?;

    Ok(EffectivePermissions { roles, permissions })
}

// ---------------------------------------------------------------------------
// Startup bootstrap
// ---------------------------------------------------------------------------

/// Grants the built-in `admin` role to the configured bootstrap identity
/// (`auth.bootstrap_admin = { issuer, subject }`), provisioning its
/// principal row if needed. Idempotent: safe to run on every startup.
pub async fn bootstrap_admin(
    pool: &PgPool,
    workspace_id: WorkspaceId,
    issuer: &str,
    subject: &str,
) -> Result<()> {
    let identity = Principal {
        kind: PrincipalKind::User,
        subject: subject.to_owned(),
        issuer: Some(issuer.to_owned()),
        display_name: None,
    };
    let record = principal::ensure(pool, workspace_id, &identity).await?;

    let admin = get_role_by_name(pool, workspace_id, ADMIN_ROLE)
        .await?
        .ok_or_else(|| {
            MeridianError::internal_msg(
                "the built-in admin role is missing; was migration 0005 applied?",
            )
        })?;

    let created = bind_role(pool, workspace_id, &admin.id, &record.id, BOOTSTRAP_ACTOR).await?;
    if created {
        tracing::info!(%issuer, %subject, "bootstrap: granted the admin role");
    } else {
        tracing::debug!(%issuer, %subject, "bootstrap: admin role already granted");
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn privilege_strings_round_trip() {
        for privilege in Privilege::ALL {
            assert_eq!(Privilege::parse(privilege.as_str()), Some(privilege));
        }
        assert_eq!(Privilege::parse("NOT_A_PRIVILEGE"), None);
        assert_eq!(Privilege::parse("read"), None, "wire form is upper-case");
    }

    #[test]
    fn securable_type_strings_round_trip() {
        for t in [
            SecurableType::Warehouse,
            SecurableType::Namespace,
            SecurableType::Table,
            SecurableType::View,
        ] {
            assert_eq!(SecurableType::parse(t.as_str()), Some(t));
        }
        assert_eq!(SecurableType::parse("catalog"), None);
    }

    #[test]
    fn privileges_grant_on_native_type_or_ancestors() {
        // Leaf-native privileges: table or view, namespace, or warehouse.
        assert!(Privilege::Read.grantable_on(SecurableType::Table));
        assert!(Privilege::Read.grantable_on(SecurableType::View));
        assert!(Privilege::Read.grantable_on(SecurableType::Namespace));
        assert!(Privilege::Read.grantable_on(SecurableType::Warehouse));
        assert!(Privilege::Commit.grantable_on(SecurableType::View));
        assert!(Privilege::Drop.grantable_on(SecurableType::View));
        // Namespace-native: namespace or warehouse, never a leaf.
        assert!(Privilege::CreateTable.grantable_on(SecurableType::Namespace));
        assert!(Privilege::CreateTable.grantable_on(SecurableType::Warehouse));
        assert!(!Privilege::CreateTable.grantable_on(SecurableType::Table));
        assert!(!Privilege::CreateView.grantable_on(SecurableType::View));
        assert!(Privilege::CreateView.grantable_on(SecurableType::Namespace));
        // Warehouse-native: warehouse only.
        assert!(Privilege::ManageWarehouse.grantable_on(SecurableType::Warehouse));
        assert!(!Privilege::ManageWarehouse.grantable_on(SecurableType::Namespace));
        assert!(!Privilege::ListNamespaces.grantable_on(SecurableType::Table));
        assert!(!Privilege::ListNamespaces.grantable_on(SecurableType::View));
    }

    #[test]
    fn reader_set_is_exactly_the_read_only_privileges() {
        let read_only: Vec<Privilege> = Privilege::ALL
            .into_iter()
            .filter(|p| p.is_read_only())
            .collect();
        assert_eq!(
            read_only,
            vec![
                Privilege::ListNamespaces,
                Privilege::ListTables,
                Privilege::Read
            ]
        );
    }
}
