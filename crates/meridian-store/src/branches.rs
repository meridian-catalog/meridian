//! Catalog-level branches & tags (Pillar K, K-F1/K-F2/K-F3).
//!
//! A *catalog branch* is a named overlay of the per-table pointer map: it
//! shares main's metadata until a table diverges (zero-copy), at which point a
//! `branch_table_pointers` row carries the branch's own pointer, advanced by
//! the SAME compare-and-set discipline main uses (see
//! [`crate::commit`] and `docs/design/branching.md`). A *tag* is an immutable
//! frozen pointer set.
//!
//! This module owns:
//! - the branch/tag registry CRUD (each mutation writes its audit row + outbox
//!   event on the same transaction as the state change — commit protocol §I4,
//!   exactly like [`crate::warehouse`] and [`crate::federation`]);
//! - the **pointer overlay resolution** — given a branch and a table, the
//!   branch pointer if the table has diverged, else a fall-through to main;
//! - enumeration of diverged tables (for diff and merge);
//! - the merge-base bookkeeping (`base_pointer_version`) that conflict
//!   detection reads.
//!
//! The branch *commit* CAS itself lives in [`crate::commit`] alongside main's,
//! because there must be exactly one function that moves any table pointer.

use chrono::{DateTime, Utc};
use meridian_common::id::WorkspaceId;
use meridian_common::{MeridianError, Result};
use serde_json::json;
use sqlx::PgPool;
use ulid::Ulid;

use crate::audit::{self, NewAuditEntry};
use crate::map_sqlx_error;
use crate::outbox::{self, NewOutboxEvent};

/// The implicit main ref. Never a `catalog_branches` row (a CHECK forbids it);
/// it is the base `tables` pointer.
pub const MAIN_REF: &str = "main";

/// A persisted branch/tag registry row.
#[derive(Debug, Clone, sqlx::FromRow)]
pub struct BranchRecord {
    /// ULID of the branch/tag.
    pub id: String,
    /// Owning workspace.
    pub workspace_id: String,
    /// Branch/tag name, unique per workspace across both kinds.
    pub name: String,
    /// `branch` (mutable commit target) or `tag` (immutable, read-only).
    pub kind: String,
    /// The ref this diverged from (`main` or a branch name).
    pub base_ref: String,
    /// Resolved base branch id, when `base_ref` names a branch.
    pub base_branch_id: Option<String>,
    /// `open` | `merged` | `deleted` (branches); tags stay `open`.
    pub state: String,
    /// `true` = spans every namespace; `false` = only `branch_namespaces`.
    pub scope_all: bool,
    /// Ephemeral-branch expiry; `None` = permanent.
    pub expires_at: Option<DateTime<Utc>>,
    /// Creator's audit string.
    pub created_by: String,
    /// Creation time.
    pub created_at: DateTime<Utc>,
    /// Last update time.
    pub updated_at: DateTime<Utc>,
}

impl BranchRecord {
    /// Whether this ref is a tag (read-only, immutable).
    #[must_use]
    pub fn is_tag(&self) -> bool {
        self.kind == "tag"
    }
}

/// A resolved pointer for a table under a branch: either the branch's own
/// diverged pointer, or a fall-through to main.
#[derive(Debug, Clone)]
pub struct ResolvedPointer {
    /// The metadata.json location to read/serve.
    pub metadata_location: String,
    /// The pointer version to guard a commit against.
    pub pointer_version: i64,
    /// `true` when this came from a `branch_table_pointers` row; `false` when
    /// it fell through to main (the table has not diverged on the branch yet).
    pub diverged: bool,
    /// main's pointer version at first divergence — the merge base. `None`
    /// when not diverged (there is no branch-specific base yet).
    pub base_pointer_version: Option<i64>,
}

/// A branch-diverged table together with everything diff/merge needs.
#[derive(Debug, Clone, sqlx::FromRow)]
pub struct DivergedPointer {
    /// Internal table id.
    pub table_id: String,
    /// Table name.
    pub table_name: String,
    /// Namespace levels of the table.
    pub namespace_levels: Vec<String>,
    /// The branch's metadata.json for this table.
    pub branch_metadata_location: String,
    /// Branch-local pointer version.
    pub branch_pointer_version: i64,
    /// main's pointer version at first divergence (the merge base).
    pub base_pointer_version: i64,
    /// main's *current* metadata.json for this table (for diff/merge).
    pub main_metadata_location: Option<String>,
    /// main's *current* pointer version (conflict detection compares this to
    /// `base_pointer_version`).
    pub main_pointer_version: i64,
}

/// Fields for creating a branch or tag.
#[derive(Debug, Clone)]
pub struct NewBranch<'a> {
    /// Branch/tag name.
    pub name: &'a str,
    /// `branch` or `tag`.
    pub kind: &'a str,
    /// Base ref (`main` or a branch name).
    pub base_ref: &'a str,
    /// Resolved base branch id, when base is a branch.
    pub base_branch_id: Option<&'a str>,
    /// Whether the branch spans all namespaces.
    pub scope_all: bool,
    /// Ephemeral expiry.
    pub expires_at: Option<DateTime<Utc>>,
}

/// Columns of the registry, in [`BranchRecord`] order.
const BRANCH_COLUMNS: &str = "id, workspace_id, name, kind, base_ref, base_branch_id, \
     state, scope_all, expires_at, created_by, created_at, updated_at";

/// Creates a branch or tag registry row with its audit row and outbox event,
/// atomically. Namespace scoping rows (when `scope_all` is false) and the
/// initial tag pointer set are written by the caller in follow-up calls; the
/// registry row is the anchor.
///
/// Returns [`MeridianError::Conflict`] when the name already exists.
pub async fn create(
    pool: &PgPool,
    workspace_id: WorkspaceId,
    new: &NewBranch<'_>,
    principal: &str,
) -> Result<BranchRecord> {
    let mut tx = pool
        .begin()
        .await
        .map_err(|e| map_sqlx_error("failed to begin branch create", e))?;

    let id = Ulid::new().to_string();
    let record: BranchRecord = sqlx::query_as(&format!(
        "INSERT INTO catalog_branches
             (id, workspace_id, name, kind, base_ref, base_branch_id, scope_all, expires_at,
              created_by)
         VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9)
         RETURNING {BRANCH_COLUMNS}"
    ))
    .bind(&id)
    .bind(workspace_id.to_string())
    .bind(new.name)
    .bind(new.kind)
    .bind(new.base_ref)
    .bind(new.base_branch_id)
    .bind(new.scope_all)
    .bind(new.expires_at)
    .bind(principal)
    .fetch_one(&mut *tx)
    .await
    .map_err(|e| {
        if e.as_database_error()
            .is_some_and(sqlx::error::DatabaseError::is_unique_violation)
        {
            MeridianError::Conflict(format!("branch or tag {:?} already exists", new.name))
        } else {
            map_sqlx_error("failed to insert branch", e)
        }
    })?;

    outbox::enqueue(
        &mut *tx,
        &NewOutboxEvent {
            workspace_id: Some(workspace_id),
            aggregate: format!("branch:{id}"),
            event_type: format!("{}.created", new.kind),
            payload: json!({ "name": new.name, "kind": new.kind, "base_ref": new.base_ref }),
        },
    )
    .await?;

    audit::append_in_tx(
        &mut tx,
        NewAuditEntry {
            workspace_id: Some(workspace_id),
            principal: principal.to_owned(),
            action: format!("{}.create", new.kind),
            resource: format!("branch:{id}"),
            details: json!({ "name": new.name, "base_ref": new.base_ref }),
        },
    )
    .await?;

    tx.commit()
        .await
        .map_err(|e| map_sqlx_error("failed to commit branch create", e))?;

    Ok(record)
}

/// Records a namespace in a branch's scope (only for `scope_all = false`).
pub async fn add_scope_namespace(pool: &PgPool, branch_id: &str, namespace_id: &str) -> Result<()> {
    sqlx::query(
        "INSERT INTO branch_namespaces (branch_id, namespace_id)
         VALUES ($1, $2) ON CONFLICT DO NOTHING",
    )
    .bind(branch_id)
    .bind(namespace_id)
    .execute(pool)
    .await
    .map_err(|e| map_sqlx_error("failed to add branch namespace scope", e))?;
    Ok(())
}

/// Records the frozen pointer set for a tag: one row per table, capturing the
/// resolved (branch-overlay or main) metadata location and current snapshot.
pub async fn add_tag_pointer(
    pool: &PgPool,
    tag_id: &str,
    table_id: &str,
    metadata_location: &str,
    snapshot_id: Option<i64>,
) -> Result<()> {
    sqlx::query(
        "INSERT INTO catalog_tags (tag_id, table_id, metadata_location, snapshot_id)
         VALUES ($1, $2, $3, $4) ON CONFLICT DO NOTHING",
    )
    .bind(tag_id)
    .bind(table_id)
    .bind(metadata_location)
    .bind(snapshot_id)
    .execute(pool)
    .await
    .map_err(|e| map_sqlx_error("failed to add tag pointer", e))?;
    Ok(())
}

/// Looks a branch/tag up by name within a workspace.
pub async fn get_by_name(
    pool: &PgPool,
    workspace_id: WorkspaceId,
    name: &str,
) -> Result<Option<BranchRecord>> {
    sqlx::query_as(&format!(
        "SELECT {BRANCH_COLUMNS} FROM catalog_branches
         WHERE workspace_id = $1 AND name = $2"
    ))
    .bind(workspace_id.to_string())
    .bind(name)
    .fetch_optional(pool)
    .await
    .map_err(|e| map_sqlx_error("failed to load branch", e))
}

/// Lists branches and tags of a workspace, newest first. Deleted branches are
/// excluded.
pub async fn list(pool: &PgPool, workspace_id: WorkspaceId) -> Result<Vec<BranchRecord>> {
    sqlx::query_as(&format!(
        "SELECT {BRANCH_COLUMNS} FROM catalog_branches
         WHERE workspace_id = $1 AND state <> 'deleted'
         ORDER BY created_at DESC, id DESC"
    ))
    .bind(workspace_id.to_string())
    .fetch_all(pool)
    .await
    .map_err(|e| map_sqlx_error("failed to list branches", e))
}

/// Counts the tables diverged on a branch (rows in `branch_table_pointers`).
pub async fn diverged_count(pool: &PgPool, branch_id: &str) -> Result<i64> {
    sqlx::query_scalar("SELECT count(*) FROM branch_table_pointers WHERE branch_id = $1")
        .bind(branch_id)
        .fetch_one(pool)
        .await
        .map_err(|e| map_sqlx_error("failed to count diverged tables", e))
}

/// Resolves a table's pointer under a branch overlay: the branch's diverged
/// pointer if present, else a fall-through to main's live pointer.
///
/// Returns `None` only when the table itself has no main pointer (an
/// uncommitted stage-create) and has not diverged — i.e. it does not exist to
/// read.
pub async fn resolve_pointer(
    pool: &PgPool,
    branch_id: &str,
    table_id: &str,
) -> Result<Option<ResolvedPointer>> {
    // Branch pointer first.
    let branch: Option<(String, i64, i64)> = sqlx::query_as(
        "SELECT metadata_location, pointer_version, base_pointer_version
         FROM branch_table_pointers WHERE branch_id = $1 AND table_id = $2",
    )
    .bind(branch_id)
    .bind(table_id)
    .fetch_optional(pool)
    .await
    .map_err(|e| map_sqlx_error("failed to resolve branch pointer", e))?;
    if let Some((metadata_location, pointer_version, base)) = branch {
        return Ok(Some(ResolvedPointer {
            metadata_location,
            pointer_version,
            diverged: true,
            base_pointer_version: Some(base),
        }));
    }

    // Fall through to main.
    let main: Option<(Option<String>, i64)> =
        sqlx::query_as("SELECT metadata_location, pointer_version FROM tables WHERE id = $1")
            .bind(table_id)
            .fetch_optional(pool)
            .await
            .map_err(|e| map_sqlx_error("failed to resolve main pointer", e))?;
    match main {
        Some((Some(metadata_location), pointer_version)) => Ok(Some(ResolvedPointer {
            metadata_location,
            pointer_version,
            diverged: false,
            base_pointer_version: None,
        })),
        _ => Ok(None),
    }
}

/// Resolves a table's pointer under a **tag** (immutable): the frozen pointer
/// from `catalog_tags` if the tag pinned this table, else a fall-through to
/// main's live pointer (a `main`-sourced tag pins nothing and reads live main).
/// Always read-only; the returned `pointer_version`/`base` are placeholders (a
/// tag is never committed to).
pub async fn resolve_tag_pointer(
    pool: &PgPool,
    tag_id: &str,
    table_id: &str,
) -> Result<Option<ResolvedPointer>> {
    let pinned: Option<(String,)> = sqlx::query_as(
        "SELECT metadata_location FROM catalog_tags WHERE tag_id = $1 AND table_id = $2",
    )
    .bind(tag_id)
    .bind(table_id)
    .fetch_optional(pool)
    .await
    .map_err(|e| map_sqlx_error("failed to resolve tag pointer", e))?;
    if let Some((metadata_location,)) = pinned {
        return Ok(Some(ResolvedPointer {
            metadata_location,
            pointer_version: 0,
            diverged: true,
            base_pointer_version: None,
        }));
    }
    // Fall through to main (the tag did not pin this table).
    let main: Option<(Option<String>, i64)> =
        sqlx::query_as("SELECT metadata_location, pointer_version FROM tables WHERE id = $1")
            .bind(table_id)
            .fetch_optional(pool)
            .await
            .map_err(|e| map_sqlx_error("failed to resolve main pointer for tag", e))?;
    match main {
        Some((Some(metadata_location), pointer_version)) => Ok(Some(ResolvedPointer {
            metadata_location,
            pointer_version,
            diverged: false,
            base_pointer_version: None,
        })),
        _ => Ok(None),
    }
}

/// Lists every table diverged on a branch, joined with main's current pointer,
/// for diff and merge. Ordered by table id (deterministic, matches the merge
/// lock order in the commit path).
pub async fn diverged_pointers(pool: &PgPool, branch_id: &str) -> Result<Vec<DivergedPointer>> {
    sqlx::query_as(
        "SELECT
             btp.table_id                    AS table_id,
             t.name                          AS table_name,
             n.levels                        AS namespace_levels,
             btp.metadata_location           AS branch_metadata_location,
             btp.pointer_version             AS branch_pointer_version,
             btp.base_pointer_version        AS base_pointer_version,
             t.metadata_location             AS main_metadata_location,
             t.pointer_version               AS main_pointer_version
         FROM branch_table_pointers btp
         JOIN tables t ON t.id = btp.table_id
         JOIN namespaces n ON n.id = t.namespace_id
         WHERE btp.branch_id = $1
         ORDER BY btp.table_id",
    )
    .bind(branch_id)
    .fetch_all(pool)
    .await
    .map_err(|e| map_sqlx_error("failed to list diverged pointers", e))
}

/// Marks a branch merged (after a successful merge). Idempotent-ish: only an
/// `open` branch transitions.
pub async fn mark_merged(
    pool: &PgPool,
    workspace_id: WorkspaceId,
    branch_id: &str,
    principal: &str,
) -> Result<()> {
    let mut tx = pool
        .begin()
        .await
        .map_err(|e| map_sqlx_error("failed to begin mark-merged", e))?;
    sqlx::query(
        "UPDATE catalog_branches SET state = 'merged', updated_at = now()
         WHERE id = $1 AND state = 'open'",
    )
    .bind(branch_id)
    .execute(&mut *tx)
    .await
    .map_err(|e| map_sqlx_error("failed to mark branch merged", e))?;

    outbox::enqueue(
        &mut *tx,
        &NewOutboxEvent {
            workspace_id: Some(workspace_id),
            aggregate: format!("branch:{branch_id}"),
            event_type: "branch.merged".to_owned(),
            payload: json!({ "branch_id": branch_id }),
        },
    )
    .await?;
    audit::append_in_tx(
        &mut tx,
        NewAuditEntry {
            workspace_id: Some(workspace_id),
            principal: principal.to_owned(),
            action: "branch.merge".to_owned(),
            resource: format!("branch:{branch_id}"),
            details: json!({}),
        },
    )
    .await?;
    tx.commit()
        .await
        .map_err(|e| map_sqlx_error("failed to commit mark-merged", e))?;
    Ok(())
}

/// Deletes a branch/tag (registry row + cascade to pointers/scopes/tag rows),
/// with its audit row and outbox event, atomically.
///
/// Returns [`MeridianError::NotFound`] when it does not exist.
pub async fn delete(
    pool: &PgPool,
    workspace_id: WorkspaceId,
    name: &str,
    principal: &str,
) -> Result<()> {
    let mut tx = pool
        .begin()
        .await
        .map_err(|e| map_sqlx_error("failed to begin branch delete", e))?;

    let row: Option<(String, String)> = sqlx::query_as(
        "SELECT id, kind FROM catalog_branches
         WHERE workspace_id = $1 AND name = $2 FOR UPDATE",
    )
    .bind(workspace_id.to_string())
    .bind(name)
    .fetch_optional(&mut *tx)
    .await
    .map_err(|e| map_sqlx_error("failed to load branch for delete", e))?;
    let Some((id, kind)) = row else {
        return Err(MeridianError::NotFound(format!(
            "branch or tag {name:?} does not exist"
        )));
    };

    sqlx::query("DELETE FROM catalog_branches WHERE id = $1")
        .bind(&id)
        .execute(&mut *tx)
        .await
        .map_err(|e| map_sqlx_error("failed to delete branch", e))?;

    outbox::enqueue(
        &mut *tx,
        &NewOutboxEvent {
            workspace_id: Some(workspace_id),
            aggregate: format!("branch:{id}"),
            event_type: format!("{kind}.deleted"),
            payload: json!({ "name": name }),
        },
    )
    .await?;
    audit::append_in_tx(
        &mut tx,
        NewAuditEntry {
            workspace_id: Some(workspace_id),
            principal: principal.to_owned(),
            action: format!("{kind}.delete"),
            resource: format!("branch:{id}"),
            details: json!({ "name": name }),
        },
    )
    .await?;
    tx.commit()
        .await
        .map_err(|e| map_sqlx_error("failed to commit branch delete", e))?;
    Ok(())
}

/// Names of expired ephemeral branches (open, past `expires_at`). The sweeper
/// deletes these via [`delete`]; separating the scan keeps the delete audited
/// per-branch.
pub async fn expired_open_branches(
    pool: &PgPool,
    workspace_id: WorkspaceId,
) -> Result<Vec<String>> {
    sqlx::query_scalar(
        "SELECT name FROM catalog_branches
         WHERE workspace_id = $1 AND state = 'open'
           AND expires_at IS NOT NULL AND expires_at <= now()
         ORDER BY name",
    )
    .bind(workspace_id.to_string())
    .fetch_all(pool)
    .await
    .map_err(|e| map_sqlx_error("failed to scan expired branches", e))
}
