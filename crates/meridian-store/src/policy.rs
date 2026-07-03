//! Governance **policies** and **policy bindings** (Pillar D / D-F1):
//! persistence for versioned row-filter / column-mask / ABAC policies and
//! their attachment to securables or tags.
//!
//! # The type boundary
//!
//! A policy's `definition` is stored as an **opaque `serde_json::Value`** —
//! the store never depends on `meridian-authz`. By construction the value is
//! the serialized form of a `meridian_authz::AbacRule` (its `type`-tagged
//! shape), so the server (which depends on both crates) round-trips it with
//! `serde_json::from_value`. This module only enforces the *structural*
//! invariant that the value's kind matches the row's [`PolicyKind`]; the
//! semantic validation ("is this a well-formed Cedar rule") is the authz
//! crate's `validate`, run by the server before a policy is saved. See
//! `docs/adr/009-cedar-abac.md`.
//!
//! # Versioning
//!
//! `policies` holds the *current* version and a denormalized copy of its
//! definition (so the hot "load the effective policy" path is one row read);
//! `policy_versions` is the append-only per-version history. Every update
//! appends a `policy_versions` row and bumps `policies.(version, definition)`
//! on one transaction, so the current version always has a matching history
//! row, and rollback creates a *new* version whose definition is copied from
//! an old one (history stays append-only, matching the audit discipline).
//!
//! Every mutation writes its audit row and outbox event on the same
//! transaction as the state change (the governance audit trail is the
//! product, D-F2).

use chrono::{DateTime, Utc};
use meridian_common::id::WorkspaceId;
use meridian_common::{MeridianError, Result};
use serde_json::{Value, json};
use sqlx::PgPool;
use ulid::Ulid;

use crate::audit::{self, NewAuditEntry};
use crate::map_sqlx_error;
use crate::outbox::{self, NewOutboxEvent};
use crate::tags::TagSecurable;

/// True when the error is a Postgres unique-constraint violation.
fn is_unique_violation(error: &sqlx::Error) -> bool {
    error
        .as_database_error()
        .is_some_and(sqlx::error::DatabaseError::is_unique_violation)
}

/// The kind of a governance policy — fixes the shape of its `definition`.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum PolicyKind {
    /// A row filter: restricts which rows a principal sees.
    RowFilter,
    /// A column mask: transforms or drops a column's values.
    ColumnMask,
    /// A general attribute-based rule (deny-unless-purpose, group, owner,
    /// time-bound).
    Abac,
}

impl PolicyKind {
    /// The database/wire rendering (matches the 0016 CHECK constraint).
    #[must_use]
    pub fn as_str(self) -> &'static str {
        match self {
            Self::RowFilter => "row_filter",
            Self::ColumnMask => "column_mask",
            Self::Abac => "abac",
        }
    }

    /// Parses the wire rendering back into the enum.
    #[must_use]
    pub fn parse(raw: &str) -> Option<Self> {
        match raw {
            "row_filter" => Some(Self::RowFilter),
            "column_mask" => Some(Self::ColumnMask),
            "abac" => Some(Self::Abac),
            _ => None,
        }
    }
}

impl std::fmt::Display for PolicyKind {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str(self.as_str())
    }
}

/// A persisted policy (its current version).
#[derive(Debug, Clone)]
pub struct Policy {
    /// ULID of the policy.
    pub id: String,
    /// Owning workspace.
    pub workspace_id: String,
    /// Human name (unique per workspace).
    pub name: String,
    /// The policy kind.
    pub kind: PolicyKind,
    /// Current version (monotonic, starts at 1).
    pub version: i32,
    /// Whether the policy is in force (a disabled policy is retained and
    /// still resolvable for dry-run/coverage but excluded from enforcement).
    pub enabled: bool,
    /// Typed definition (opaque here; a serialized `AbacRule`).
    pub definition: Value,
    /// Audit string of the creating principal.
    pub created_by: String,
    /// Creation time.
    pub created_at: DateTime<Utc>,
    /// Last update time.
    pub updated_at: DateTime<Utc>,
}

/// A policy row as read from Postgres.
#[derive(sqlx::FromRow)]
struct PolicyRow {
    id: String,
    workspace_id: String,
    name: String,
    kind: String,
    version: i32,
    enabled: bool,
    definition: Value,
    created_by: String,
    created_at: DateTime<Utc>,
    updated_at: DateTime<Utc>,
}

impl TryFrom<PolicyRow> for Policy {
    type Error = MeridianError;

    fn try_from(r: PolicyRow) -> Result<Self> {
        Ok(Self {
            id: r.id,
            workspace_id: r.workspace_id,
            name: r.name,
            kind: PolicyKind::parse(&r.kind).ok_or_else(|| {
                MeridianError::internal_msg(format!("policy row has unknown kind {:?}", r.kind))
            })?,
            version: r.version,
            enabled: r.enabled,
            definition: r.definition,
            created_by: r.created_by,
            created_at: r.created_at,
            updated_at: r.updated_at,
        })
    }
}

const POLICY_COLUMNS: &str = "id, workspace_id, name, kind, version, enabled, definition, \
     created_by, created_at, updated_at";

/// One historical version of a policy.
#[derive(Debug, Clone)]
pub struct PolicyVersion {
    /// The policy id.
    pub policy_id: String,
    /// The version number.
    pub version: i32,
    /// The kind at this version.
    pub kind: PolicyKind,
    /// Whether it was enabled at this version.
    pub enabled: bool,
    /// The definition snapshot at this version.
    pub definition: Value,
    /// Audit string of the principal who created this version.
    pub created_by: String,
    /// When this version was created.
    pub created_at: DateTime<Utc>,
}

#[derive(sqlx::FromRow)]
struct VersionRow {
    policy_id: String,
    version: i32,
    kind: String,
    enabled: bool,
    definition: Value,
    created_by: String,
    created_at: DateTime<Utc>,
}

impl TryFrom<VersionRow> for PolicyVersion {
    type Error = MeridianError;

    fn try_from(r: VersionRow) -> Result<Self> {
        Ok(Self {
            policy_id: r.policy_id,
            version: r.version,
            kind: PolicyKind::parse(&r.kind).ok_or_else(|| {
                MeridianError::internal_msg(format!(
                    "policy version row has unknown kind {:?}",
                    r.kind
                ))
            })?,
            enabled: r.enabled,
            definition: r.definition,
            created_by: r.created_by,
            created_at: r.created_at,
        })
    }
}

const VERSION_COLUMNS: &str =
    "policy_id, version, kind, enabled, definition, created_by, created_at";

// ---------------------------------------------------------------------------
// Policy CRUD + versioning
// ---------------------------------------------------------------------------

/// Creates a policy at version 1 (and its first `policy_versions` row).
///
/// The caller is responsible for having validated `definition` against the
/// authz schema first (semantic validation is the authz crate's job). Returns
/// [`MeridianError::Conflict`] if the name is taken in the workspace.
pub async fn create(
    pool: &PgPool,
    workspace_id: WorkspaceId,
    name: &str,
    kind: PolicyKind,
    definition: &Value,
    principal: &str,
) -> Result<Policy> {
    if name.trim().is_empty() {
        return Err(MeridianError::Validation(
            "policy name must be non-empty".to_owned(),
        ));
    }

    let mut tx = pool
        .begin()
        .await
        .map_err(|e| map_sqlx_error("failed to begin policy create", e))?;

    let id = Ulid::new().to_string();
    let row: PolicyRow = sqlx::query_as(&format!(
        "INSERT INTO policies (id, workspace_id, name, kind, version, enabled, definition, created_by)
         VALUES ($1, $2, $3, $4, 1, TRUE, $5, $6)
         RETURNING {POLICY_COLUMNS}"
    ))
    .bind(&id)
    .bind(workspace_id.to_string())
    .bind(name)
    .bind(kind.as_str())
    .bind(definition)
    .bind(principal)
    .fetch_one(&mut *tx)
    .await
    .map_err(|e| {
        if is_unique_violation(&e) {
            MeridianError::Conflict(format!("policy {name:?} already exists"))
        } else {
            map_sqlx_error("failed to insert policy", e)
        }
    })?;

    insert_version_row(&mut tx, &id, 1, kind, true, definition, principal).await?;

    let details = json!({ "name": name, "kind": kind.as_str(), "version": 1 });
    outbox::enqueue(
        &mut *tx,
        &NewOutboxEvent {
            workspace_id: Some(workspace_id),
            aggregate: format!("policy:{id}"),
            event_type: "governance.policy.created".to_owned(),
            payload: details.clone(),
        },
    )
    .await?;
    audit::append_in_tx(
        &mut tx,
        NewAuditEntry {
            workspace_id: Some(workspace_id),
            principal: principal.to_owned(),
            action: "governance.policy.create".to_owned(),
            resource: format!("policy:{id}"),
            details,
        },
    )
    .await?;

    tx.commit()
        .await
        .map_err(|e| map_sqlx_error("failed to commit policy create", e))?;

    Policy::try_from(row)
}

/// Inserts one `policy_versions` row on the caller's transaction.
async fn insert_version_row(
    tx: &mut sqlx::Transaction<'_, sqlx::Postgres>,
    policy_id: &str,
    version: i32,
    kind: PolicyKind,
    enabled: bool,
    definition: &Value,
    principal: &str,
) -> Result<()> {
    sqlx::query(
        "INSERT INTO policy_versions
             (policy_id, version, kind, enabled, definition, created_by)
         VALUES ($1, $2, $3, $4, $5, $6)",
    )
    .bind(policy_id)
    .bind(version)
    .bind(kind.as_str())
    .bind(enabled)
    .bind(definition)
    .bind(principal)
    .execute(&mut **tx)
    .await
    .map_err(|e| map_sqlx_error("failed to insert policy version", e))?;
    Ok(())
}

/// Fields an update may change. `None` fields are left unchanged (but the
/// version is always bumped and a full snapshot recorded, so history is
/// complete). The `kind` never changes — a policy's kind is fixed at
/// creation (a different kind is a different policy).
#[derive(Debug, Clone, Default)]
pub struct PolicyUpdate {
    /// New definition, if changing.
    pub definition: Option<Value>,
    /// New enabled flag, if changing.
    pub enabled: Option<bool>,
}

/// Updates a policy: bumps the version, records the full new snapshot in
/// `policy_versions`, and updates the denormalized current row — all on one
/// transaction. Returns the new [`Policy`], or [`MeridianError::NotFound`].
pub async fn update(
    pool: &PgPool,
    workspace_id: WorkspaceId,
    id: &str,
    change: PolicyUpdate,
    principal: &str,
) -> Result<Policy> {
    let mut tx = pool
        .begin()
        .await
        .map_err(|e| map_sqlx_error("failed to begin policy update", e))?;

    // Lock the current row so the version bump is race-free.
    let current: Option<PolicyRow> = sqlx::query_as(&format!(
        "SELECT {POLICY_COLUMNS} FROM policies WHERE workspace_id = $1 AND id = $2 FOR UPDATE"
    ))
    .bind(workspace_id.to_string())
    .bind(id)
    .fetch_optional(&mut *tx)
    .await
    .map_err(|e| map_sqlx_error("failed to load policy for update", e))?;
    let Some(current) = current else {
        return Err(MeridianError::NotFound(format!(
            "policy {id:?} does not exist"
        )));
    };
    let current = Policy::try_from(current)?;

    let new_version = current.version + 1;
    let new_definition = change.definition.unwrap_or(current.definition);
    let new_enabled = change.enabled.unwrap_or(current.enabled);

    let row: PolicyRow = sqlx::query_as(&format!(
        "UPDATE policies
         SET version = $3, definition = $4, enabled = $5, updated_at = now()
         WHERE workspace_id = $1 AND id = $2
         RETURNING {POLICY_COLUMNS}"
    ))
    .bind(workspace_id.to_string())
    .bind(id)
    .bind(new_version)
    .bind(&new_definition)
    .bind(new_enabled)
    .fetch_one(&mut *tx)
    .await
    .map_err(|e| map_sqlx_error("failed to update policy", e))?;

    insert_version_row(
        &mut tx,
        id,
        new_version,
        current.kind,
        new_enabled,
        &new_definition,
        principal,
    )
    .await?;

    let details = json!({ "name": current.name, "version": new_version, "enabled": new_enabled });
    outbox::enqueue(
        &mut *tx,
        &NewOutboxEvent {
            workspace_id: Some(workspace_id),
            aggregate: format!("policy:{id}"),
            event_type: "governance.policy.updated".to_owned(),
            payload: details.clone(),
        },
    )
    .await?;
    audit::append_in_tx(
        &mut tx,
        NewAuditEntry {
            workspace_id: Some(workspace_id),
            principal: principal.to_owned(),
            action: "governance.policy.update".to_owned(),
            resource: format!("policy:{id}"),
            details,
        },
    )
    .await?;

    tx.commit()
        .await
        .map_err(|e| map_sqlx_error("failed to commit policy update", e))?;

    Policy::try_from(row)
}

/// Rolls a policy back to the definition of an earlier version by creating a
/// **new** version whose definition is that earlier one's (history stays
/// append-only). Returns the new current [`Policy`], or `NotFound` if the
/// policy or target version is absent.
pub async fn rollback(
    pool: &PgPool,
    workspace_id: WorkspaceId,
    id: &str,
    to_version: i32,
    principal: &str,
) -> Result<Policy> {
    let mut tx = pool
        .begin()
        .await
        .map_err(|e| map_sqlx_error("failed to begin policy rollback", e))?;

    let current: Option<PolicyRow> = sqlx::query_as(&format!(
        "SELECT {POLICY_COLUMNS} FROM policies WHERE workspace_id = $1 AND id = $2 FOR UPDATE"
    ))
    .bind(workspace_id.to_string())
    .bind(id)
    .fetch_optional(&mut *tx)
    .await
    .map_err(|e| map_sqlx_error("failed to load policy for rollback", e))?;
    let Some(current) = current else {
        return Err(MeridianError::NotFound(format!(
            "policy {id:?} does not exist"
        )));
    };
    let current = Policy::try_from(current)?;

    let target: Option<VersionRow> = sqlx::query_as(&format!(
        "SELECT {VERSION_COLUMNS} FROM policy_versions WHERE policy_id = $1 AND version = $2"
    ))
    .bind(id)
    .bind(to_version)
    .fetch_optional(&mut *tx)
    .await
    .map_err(|e| map_sqlx_error("failed to load target version", e))?;
    let Some(target) = target else {
        return Err(MeridianError::NotFound(format!(
            "policy {id:?} has no version {to_version}"
        )));
    };
    let target = PolicyVersion::try_from(target)?;

    let new_version = current.version + 1;
    let row: PolicyRow = sqlx::query_as(&format!(
        "UPDATE policies
         SET version = $3, definition = $4, enabled = $5, updated_at = now()
         WHERE workspace_id = $1 AND id = $2
         RETURNING {POLICY_COLUMNS}"
    ))
    .bind(workspace_id.to_string())
    .bind(id)
    .bind(new_version)
    .bind(&target.definition)
    .bind(target.enabled)
    .fetch_one(&mut *tx)
    .await
    .map_err(|e| map_sqlx_error("failed to roll policy back", e))?;

    insert_version_row(
        &mut tx,
        id,
        new_version,
        current.kind,
        target.enabled,
        &target.definition,
        principal,
    )
    .await?;

    let details = json!({
        "name": current.name,
        "rolled_back_to": to_version,
        "new_version": new_version,
    });
    outbox::enqueue(
        &mut *tx,
        &NewOutboxEvent {
            workspace_id: Some(workspace_id),
            aggregate: format!("policy:{id}"),
            event_type: "governance.policy.rolledback".to_owned(),
            payload: details.clone(),
        },
    )
    .await?;
    audit::append_in_tx(
        &mut tx,
        NewAuditEntry {
            workspace_id: Some(workspace_id),
            principal: principal.to_owned(),
            action: "governance.policy.rollback".to_owned(),
            resource: format!("policy:{id}"),
            details,
        },
    )
    .await?;

    tx.commit()
        .await
        .map_err(|e| map_sqlx_error("failed to commit policy rollback", e))?;

    Policy::try_from(row)
}

/// Lists all policies in a workspace, in stable (name) order.
pub async fn list(pool: &PgPool, workspace_id: WorkspaceId) -> Result<Vec<Policy>> {
    let rows: Vec<PolicyRow> = sqlx::query_as(&format!(
        "SELECT {POLICY_COLUMNS} FROM policies WHERE workspace_id = $1 ORDER BY name"
    ))
    .bind(workspace_id.to_string())
    .fetch_all(pool)
    .await
    .map_err(|e| map_sqlx_error("failed to list policies", e))?;
    rows.into_iter().map(Policy::try_from).collect()
}

/// Loads a policy by id (workspace-scoped).
pub async fn get(pool: &PgPool, workspace_id: WorkspaceId, id: &str) -> Result<Option<Policy>> {
    let row: Option<PolicyRow> = sqlx::query_as(&format!(
        "SELECT {POLICY_COLUMNS} FROM policies WHERE workspace_id = $1 AND id = $2"
    ))
    .bind(workspace_id.to_string())
    .bind(id)
    .fetch_optional(pool)
    .await
    .map_err(|e| map_sqlx_error("failed to load policy", e))?;
    row.map(Policy::try_from).transpose()
}

/// Lists a policy's version history, newest first.
pub async fn list_versions(
    pool: &PgPool,
    workspace_id: WorkspaceId,
    policy_id: &str,
) -> Result<Vec<PolicyVersion>> {
    // Scope via the parent policy's workspace so a cross-workspace id cannot
    // read another workspace's history.
    let rows: Vec<VersionRow> = sqlx::query_as(
        "SELECT v.policy_id, v.version, v.kind, v.enabled, v.definition, v.created_by, v.created_at
         FROM policy_versions v
         JOIN policies p ON p.id = v.policy_id
         WHERE v.policy_id = $1 AND p.workspace_id = $2
         ORDER BY v.version DESC",
    )
    .bind(policy_id)
    .bind(workspace_id.to_string())
    .fetch_all(pool)
    .await
    .map_err(|e| map_sqlx_error("failed to list policy versions", e))?;
    rows.into_iter().map(PolicyVersion::try_from).collect()
}

/// Deletes a policy and its versions/bindings (CASCADE). Returns
/// [`MeridianError::NotFound`] if absent.
pub async fn delete(
    pool: &PgPool,
    workspace_id: WorkspaceId,
    id: &str,
    principal: &str,
) -> Result<()> {
    let mut tx = pool
        .begin()
        .await
        .map_err(|e| map_sqlx_error("failed to begin policy delete", e))?;

    let existing: Option<(String,)> =
        sqlx::query_as("SELECT name FROM policies WHERE workspace_id = $1 AND id = $2 FOR UPDATE")
            .bind(workspace_id.to_string())
            .bind(id)
            .fetch_optional(&mut *tx)
            .await
            .map_err(|e| map_sqlx_error("failed to load policy for delete", e))?;
    let Some((name,)) = existing else {
        return Err(MeridianError::NotFound(format!(
            "policy {id:?} does not exist"
        )));
    };

    sqlx::query("DELETE FROM policies WHERE workspace_id = $1 AND id = $2")
        .bind(workspace_id.to_string())
        .bind(id)
        .execute(&mut *tx)
        .await
        .map_err(|e| map_sqlx_error("failed to delete policy", e))?;

    let details = json!({ "name": name });
    outbox::enqueue(
        &mut *tx,
        &NewOutboxEvent {
            workspace_id: Some(workspace_id),
            aggregate: format!("policy:{id}"),
            event_type: "governance.policy.deleted".to_owned(),
            payload: details.clone(),
        },
    )
    .await?;
    audit::append_in_tx(
        &mut tx,
        NewAuditEntry {
            workspace_id: Some(workspace_id),
            principal: principal.to_owned(),
            action: "governance.policy.delete".to_owned(),
            resource: format!("policy:{id}"),
            details,
        },
    )
    .await?;

    tx.commit()
        .await
        .map_err(|e| map_sqlx_error("failed to commit policy delete", e))?;

    Ok(())
}

// ---------------------------------------------------------------------------
// Policy bindings
// ---------------------------------------------------------------------------

/// The target of a policy binding: a securable (table/namespace) XOR a tag.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum BindingTarget {
    /// A direct binding to one securable (a namespace binding applies to
    /// everything it contains, resolved in code).
    Securable {
        /// `table` or `namespace` (columns are not bound directly — a column
        /// mask keys on a column *tag*).
        securable_type: TagSecurable,
        /// The securable id.
        securable_id: String,
    },
    /// A binding to a tag: the policy applies wherever the tag is assigned.
    Tag {
        /// The tag id.
        tag_id: String,
    },
}

/// A persisted policy binding.
#[derive(Debug, Clone)]
pub struct PolicyBinding {
    /// ULID of the binding.
    pub id: String,
    /// The bound policy.
    pub policy_id: String,
    /// The binding target.
    pub target: BindingTarget,
    /// Audit string of the principal who created the binding.
    pub bound_by: String,
    /// Creation time.
    pub created_at: DateTime<Utc>,
}

#[derive(sqlx::FromRow)]
struct BindingRow {
    id: String,
    policy_id: String,
    securable_type: Option<String>,
    securable_id: Option<String>,
    tag_id: Option<String>,
    bound_by: String,
    created_at: DateTime<Utc>,
}

impl TryFrom<BindingRow> for PolicyBinding {
    type Error = MeridianError;

    fn try_from(r: BindingRow) -> Result<Self> {
        let target = match (r.securable_type, r.securable_id, r.tag_id) {
            (Some(t), Some(sid), None) => BindingTarget::Securable {
                securable_type: TagSecurable::parse(&t).ok_or_else(|| {
                    MeridianError::internal_msg(format!(
                        "binding row has unknown securable_type {t:?}"
                    ))
                })?,
                securable_id: sid,
            },
            (None, None, Some(tag_id)) => BindingTarget::Tag { tag_id },
            _ => {
                return Err(MeridianError::internal_msg(
                    "binding row violates the securable-XOR-tag invariant",
                ));
            }
        };
        Ok(Self {
            id: r.id,
            policy_id: r.policy_id,
            target,
            bound_by: r.bound_by,
            created_at: r.created_at,
        })
    }
}

const BINDING_COLUMNS: &str =
    "id, policy_id, securable_type, securable_id, tag_id, bound_by, created_at";

/// Binds a policy to a target (securable or tag).
///
/// Returns [`MeridianError::Conflict`] if the policy is already bound to that
/// target, and [`MeridianError::NotFound`] if the policy (or tag) is absent.
pub async fn bind(
    pool: &PgPool,
    workspace_id: WorkspaceId,
    policy_id: &str,
    target: &BindingTarget,
    principal: &str,
) -> Result<PolicyBinding> {
    // Verify the policy exists in this workspace (bindings carry no
    // workspace column; the policy is the scoping anchor).
    if get(pool, workspace_id, policy_id).await?.is_none() {
        return Err(MeridianError::NotFound(format!(
            "policy {policy_id:?} does not exist"
        )));
    }

    let (securable_type, securable_id, tag_id) = match target {
        BindingTarget::Securable {
            securable_type,
            securable_id,
        } => {
            if *securable_type == TagSecurable::Column {
                return Err(MeridianError::Validation(
                    "policies bind to a table, a namespace, or a tag — not a column directly \
                     (a column mask keys on a column tag)"
                        .to_owned(),
                ));
            }
            (
                Some(securable_type.as_str()),
                Some(securable_id.as_str()),
                None,
            )
        }
        BindingTarget::Tag { tag_id } => (None, None, Some(tag_id.as_str())),
    };

    let mut tx = pool
        .begin()
        .await
        .map_err(|e| map_sqlx_error("failed to begin policy bind", e))?;

    let id = Ulid::new().to_string();
    let row: BindingRow = sqlx::query_as(&format!(
        "INSERT INTO policy_bindings
             (id, workspace_id, policy_id, securable_type, securable_id, tag_id, bound_by)
         VALUES ($1, $2, $3, $4, $5, $6, $7)
         RETURNING {BINDING_COLUMNS}"
    ))
    .bind(&id)
    .bind(workspace_id.to_string())
    .bind(policy_id)
    .bind(securable_type)
    .bind(securable_id)
    .bind(tag_id)
    .bind(principal)
    .fetch_one(&mut *tx)
    .await
    .map_err(|e| {
        if is_unique_violation(&e) {
            MeridianError::Conflict("that policy is already bound to that target".to_owned())
        } else if e
            .as_database_error()
            .is_some_and(sqlx::error::DatabaseError::is_foreign_key_violation)
        {
            MeridianError::NotFound("the policy or tag does not exist".to_owned())
        } else {
            map_sqlx_error("failed to insert policy binding", e)
        }
    })?;

    let details = json!({
        "policy_id": policy_id,
        "securable_type": securable_type,
        "securable_id": securable_id,
        "tag_id": tag_id,
    });
    outbox::enqueue(
        &mut *tx,
        &NewOutboxEvent {
            workspace_id: Some(workspace_id),
            aggregate: format!("policy:{policy_id}"),
            event_type: "governance.policy.bound".to_owned(),
            payload: details.clone(),
        },
    )
    .await?;
    audit::append_in_tx(
        &mut tx,
        NewAuditEntry {
            workspace_id: Some(workspace_id),
            principal: principal.to_owned(),
            action: "governance.policy.bind".to_owned(),
            resource: format!("policy_binding:{id}"),
            details,
        },
    )
    .await?;

    tx.commit()
        .await
        .map_err(|e| map_sqlx_error("failed to commit policy bind", e))?;

    PolicyBinding::try_from(row)
}

/// Removes a binding by id. Returns [`MeridianError::NotFound`] if absent.
pub async fn unbind(
    pool: &PgPool,
    workspace_id: WorkspaceId,
    id: &str,
    principal: &str,
) -> Result<()> {
    let mut tx = pool
        .begin()
        .await
        .map_err(|e| map_sqlx_error("failed to begin policy unbind", e))?;

    let deleted: Option<(String,)> = sqlx::query_as(
        "DELETE FROM policy_bindings WHERE workspace_id = $1 AND id = $2 RETURNING policy_id",
    )
    .bind(workspace_id.to_string())
    .bind(id)
    .fetch_optional(&mut *tx)
    .await
    .map_err(|e| map_sqlx_error("failed to delete policy binding", e))?;
    let Some((policy_id,)) = deleted else {
        return Err(MeridianError::NotFound(format!(
            "policy binding {id:?} does not exist"
        )));
    };

    audit::append_in_tx(
        &mut tx,
        NewAuditEntry {
            workspace_id: Some(workspace_id),
            principal: principal.to_owned(),
            action: "governance.policy.unbind".to_owned(),
            resource: format!("policy_binding:{id}"),
            details: json!({ "policy_id": policy_id }),
        },
    )
    .await?;

    tx.commit()
        .await
        .map_err(|e| map_sqlx_error("failed to commit policy unbind", e))?;

    Ok(())
}

/// Lists all bindings of a policy (workspace-scoped via the policy).
pub async fn list_bindings(
    pool: &PgPool,
    workspace_id: WorkspaceId,
    policy_id: &str,
) -> Result<Vec<PolicyBinding>> {
    let rows: Vec<BindingRow> = sqlx::query_as(&format!(
        "SELECT {BINDING_COLUMNS} FROM policy_bindings
         WHERE workspace_id = $1 AND policy_id = $2 ORDER BY id"
    ))
    .bind(workspace_id.to_string())
    .bind(policy_id)
    .fetch_all(pool)
    .await
    .map_err(|e| map_sqlx_error("failed to list policy bindings", e))?;
    rows.into_iter().map(PolicyBinding::try_from).collect()
}

// ---------------------------------------------------------------------------
// Resolution (feeds the enforcement layer)
// ---------------------------------------------------------------------------

/// A policy that applies to a table, with why it applies (for the effective-
/// policy report and the enforcement mapping).
#[derive(Debug, Clone)]
pub struct AppliedPolicy {
    /// The applying policy (its current version).
    pub policy: Policy,
    /// How it reached this table: a direct securable binding, or a tag
    /// binding (with the tag that matched).
    pub via: AppliedVia,
}

/// Why a policy applies to a table.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum AppliedVia {
    /// Bound directly to this table.
    Table,
    /// Bound to one of the table's ancestor namespaces.
    Namespace {
        /// The namespace id that carried the binding.
        namespace_id: String,
    },
    /// Bound to a tag that the table (or a column, or an ancestor namespace)
    /// carries.
    Tag {
        /// The rendered tag `key:value` that matched.
        tag: String,
    },
}

/// A joined policy + binding-provenance row from [`resolve_for_table`]. The
/// policy columns are `flatten`ed from the same `p.*` projection `PolicyRow`
/// reads, so the two stay in lockstep.
#[derive(sqlx::FromRow)]
struct AppliedRow {
    #[sqlx(flatten)]
    policy: PolicyRow,
    via_securable_type: Option<String>,
    via_securable_id: Option<String>,
    via_tag: Option<String>,
}

/// Resolves every **enabled** policy that applies to a table — directly (a
/// table/namespace binding) or via a tag the table or its columns carry.
///
/// `namespace_ids` is the table's self-and-ancestor namespace chain (the
/// caller resolves it, exactly like RBAC). `table_tags` is the rendered tag
/// set that applies to the table and its columns (from
/// [`crate::tags::resolve_table_tags`]) — a tag binding applies when its tag
/// is in this set. Duplicate policies (reachable two ways) are de-duplicated
/// by policy id, keeping the *most direct* provenance
/// (`Table` > `Namespace` > `Tag`) for the report.
///
/// Only enabled policies are returned; a disabled policy is retained for
/// dry-run/coverage but never enforced.
pub async fn resolve_for_table(
    pool: &PgPool,
    workspace_id: WorkspaceId,
    table_id: &str,
    namespace_ids: &[String],
    table_tags: &[String],
) -> Result<Vec<AppliedPolicy>> {
    // One query returns every enabled policy bound to this table, to any of
    // its ancestor namespaces, or to any tag in `table_tags`, tagging each
    // row with how it was reached so we can keep the most-direct provenance.
    // The policy columns are selected flat (a nested `PolicyRow` inside the
    // tuple cannot be decoded — sqlx maps one tuple slot to one column).
    let rows: Vec<AppliedRow> = sqlx::query_as(&format!(
        "SELECT {cols},
                b.securable_type AS via_securable_type,
                b.securable_id   AS via_securable_id,
                bt.key || ':' || bt.value AS via_tag
         FROM policies p
         JOIN policy_bindings b ON b.policy_id = p.id
         LEFT JOIN tags bt ON bt.id = b.tag_id
         WHERE p.workspace_id = $1
           AND p.enabled = TRUE
           AND (
               (b.securable_type = 'table' AND b.securable_id = $2)
               OR (b.securable_type = 'namespace' AND b.securable_id = ANY($3))
               OR (b.tag_id IS NOT NULL AND (bt.key || ':' || bt.value) = ANY($4))
           )
         ORDER BY p.name",
        cols = POLICY_COLUMNS
            .split(", ")
            .map(|c| format!("p.{c}"))
            .collect::<Vec<_>>()
            .join(", ")
    ))
    .bind(workspace_id.to_string())
    .bind(table_id)
    .bind(namespace_ids)
    .bind(table_tags)
    .fetch_all(pool)
    .await
    .map_err(|e| map_sqlx_error("failed to resolve policies for table", e))?;

    // De-dup by policy id, keeping the most-direct provenance.
    let mut by_id: std::collections::BTreeMap<String, AppliedPolicy> =
        std::collections::BTreeMap::new();
    for row in rows {
        let via_type = row.via_securable_type.clone().unwrap_or_default();
        let via_id = row.via_securable_id.clone();
        let via_tag = row.via_tag.clone();
        let policy = Policy::try_from(row.policy)?;
        let via = match (via_type.as_str(), via_id, via_tag) {
            ("table", _, _) => AppliedVia::Table,
            ("namespace", Some(ns), _) => AppliedVia::Namespace { namespace_id: ns },
            (_, _, Some(tag)) => AppliedVia::Tag { tag },
            // A binding row that matched must be one of the three shapes; a
            // row that does not is a data-model violation, skip it defensively.
            _ => continue,
        };
        let rank = |v: &AppliedVia| match v {
            AppliedVia::Table => 2,
            AppliedVia::Namespace { .. } => 1,
            AppliedVia::Tag { .. } => 0,
        };
        by_id
            .entry(policy.id.clone())
            .and_modify(|existing| {
                if rank(&via) > rank(&existing.via) {
                    existing.via = via.clone();
                }
            })
            .or_insert(AppliedPolicy { policy, via });
    }

    Ok(by_id.into_values().collect())
}
