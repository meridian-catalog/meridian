//! View persistence and lifecycle operations, mirroring [`crate::table`].
//!
//! A view row carries the same commit-protocol pointer as a table row:
//! (`metadata_location`, `pointer_version`) per
//! `docs/design/commit-protocol.md` §2. This module owns creation, lookup,
//! listing, rename, drop, *and* the pointer swap for view replace — views
//! need none of the multi-table commit machinery, so the swap is a single
//! `FOR UPDATE` + guarded `UPDATE` here rather than a
//! [`crate::commit::PostgresCommitBackend`] operation. Every mutation
//! writes its audit row and outbox event on the same transaction as the
//! state change (invariant I6).
//!
//! Tables and views share one name space per namespace (the REST spec 409s
//! a create/rename whose identifier "already exists as a table or view").
//! The views side of that invariant is enforced here: create and rename
//! check the `tables` table inside their transactions. The tables side
//! (table create/rename colliding with an existing view) lives in
//! [`crate::table`]'s call sites and is tracked in `docs/api-status.md`.

use std::collections::BTreeMap;

use chrono::{DateTime, Utc};
use meridian_common::id::WorkspaceId;
use meridian_common::{MeridianError, Result};
use serde_json::{Value, json};
use sqlx::PgPool;
use sqlx::types::Json;
use ulid::Ulid;

use crate::audit::{self, NewAuditEntry};
use crate::map_sqlx_error;
use crate::outbox::{self, NewOutboxEvent};

/// A persisted view row.
#[derive(Debug, Clone, sqlx::FromRow)]
pub struct ViewRecord {
    /// ULID of the view (Meridian's internal identity).
    pub id: String,
    /// Owning workspace.
    pub workspace_id: String,
    /// Owning namespace.
    pub namespace_id: String,
    /// View name within the namespace.
    pub name: String,
    /// Iceberg view UUID (canonical hyphenated form), stable for the view's
    /// lifetime.
    pub view_uuid: String,
    /// Object-storage location of the current view `metadata.json`. Always
    /// set for committed views.
    pub metadata_location: Option<String>,
    /// Monotonic pointer version; +1 per committed swap (the CAS guard).
    pub pointer_version: i64,
    /// View properties, write-through-indexed from metadata.
    pub properties: Json<BTreeMap<String, String>>,
    /// Creation time.
    pub created_at: DateTime<Utc>,
    /// Last update time.
    pub updated_at: DateTime<Utc>,
}

const SELECT_COLUMNS: &str = "id, workspace_id, namespace_id, name, view_uuid, \
     metadata_location, pointer_version, properties, created_at, updated_at";

/// [`SELECT_COLUMNS`] qualified with the `v.` alias for joined queries.
const SELECT_COLUMNS_V: &str = "v.id, v.workspace_id, v.namespace_id, v.name, v.view_uuid, \
     v.metadata_location, v.pointer_version, v.properties, v.created_at, v.updated_at";

/// A new view row to insert.
#[derive(Debug, Clone)]
pub struct NewView<'a> {
    /// Owning workspace.
    pub workspace_id: WorkspaceId,
    /// Owning namespace id.
    pub namespace_id: &'a str,
    /// Namespace levels (for event/audit payloads).
    pub namespace_levels: &'a [String],
    /// View name.
    pub name: &'a str,
    /// Iceberg view UUID (canonical hyphenated form).
    pub view_uuid: &'a str,
    /// Location of the initial view `metadata.json` (already durably
    /// written).
    pub metadata_location: &'a str,
    /// View properties, write-through-indexed.
    pub properties: &'a BTreeMap<String, String>,
}

/// One pointer swap for a view replace: the compare-and-set plus the
/// write-through state derived from the new metadata.
#[derive(Debug, Clone)]
pub struct ViewPointerSwap<'a> {
    /// The view row id.
    pub view_id: &'a str,
    /// The pointer version the caller based its candidate on.
    pub expected_version: i64,
    /// The staged (already durably written) candidate metadata location.
    pub new_metadata_location: &'a str,
    /// View properties of the new metadata.
    pub properties: &'a BTreeMap<String, String>,
    /// Extra detail merged into the audit/outbox payload (version ids, …).
    pub event_details: Value,
}

/// Failure modes of [`commit_replace`], mirroring the commit-protocol error
/// model so the API layer can retry, 404, or surface state-unknown exactly
/// like the table commit path.
#[derive(Debug, thiserror::Error)]
pub enum ViewCommitError {
    /// The view row vanished between the caller's read and the swap.
    #[error("view does not exist")]
    NotFound,
    /// The pointer moved: the caller must refresh and rebuild (F6).
    #[error("view pointer moved: expected version {expected}, found {actual}")]
    VersionConflict {
        /// The version the caller expected.
        expected: i64,
        /// The version actually found.
        actual: i64,
    },
    /// The transaction's `COMMIT` itself failed: the swap may or may not
    /// have applied (F3). The staged file must not be deleted.
    #[error("view commit outcome unknown: {message}")]
    StateUnknown {
        /// What failed.
        message: String,
    },
    /// Anything else; nothing was applied.
    #[error(transparent)]
    Store(#[from] MeridianError),
}

/// True when the error is a Postgres unique-constraint violation.
fn is_unique_violation(error: &sqlx::Error) -> bool {
    error
        .as_database_error()
        .is_some_and(sqlx::error::DatabaseError::is_unique_violation)
}

/// Loads a view by namespace id and name.
pub async fn get(pool: &PgPool, namespace_id: &str, name: &str) -> Result<Option<ViewRecord>> {
    sqlx::query_as(&format!(
        "SELECT {SELECT_COLUMNS} FROM views WHERE namespace_id = $1 AND name = $2"
    ))
    .bind(namespace_id)
    .bind(name)
    .fetch_optional(pool)
    .await
    .map_err(|e| map_sqlx_error("failed to load view", e))
}

/// Loads a view by warehouse, namespace levels, and name in one query.
///
/// Returns `None` when either the namespace or the view does not exist;
/// callers that need to distinguish resolve the namespace first.
pub async fn get_by_name(
    pool: &PgPool,
    warehouse_id: &str,
    namespace_levels: &[String],
    name: &str,
) -> Result<Option<ViewRecord>> {
    sqlx::query_as(&format!(
        "SELECT {SELECT_COLUMNS_V}
         FROM views v
         JOIN namespaces n ON n.id = v.namespace_id
         WHERE n.warehouse_id = $1 AND n.levels = $2 AND v.name = $3"
    ))
    .bind(warehouse_id)
    .bind(namespace_levels)
    .bind(name)
    .fetch_optional(pool)
    .await
    .map_err(|e| map_sqlx_error("failed to load view by name", e))
}

/// Lists views of a namespace in stable id (creation) order.
///
/// Keyset pagination: pass the `id` of the last row of the previous page as
/// `after_id`; `limit` bounds the page (`None` returns everything).
pub async fn list(
    pool: &PgPool,
    namespace_id: &str,
    after_id: Option<&str>,
    limit: Option<i64>,
) -> Result<Vec<ViewRecord>> {
    sqlx::query_as(&format!(
        "SELECT {SELECT_COLUMNS}
         FROM views
         WHERE namespace_id = $1
           AND ($2::text IS NULL OR id > $2)
         ORDER BY id
         LIMIT $3"
    ))
    .bind(namespace_id)
    .bind(after_id)
    .bind(limit)
    .fetch_all(pool)
    .await
    .map_err(|e| map_sqlx_error("failed to list views", e))
}

/// True when a *table* of this name exists in the namespace (tables and
/// views share one name space; used by create/rename collision checks).
async fn table_name_taken<'e, E>(executor: E, namespace_id: &str, name: &str) -> Result<bool>
where
    E: sqlx::Executor<'e, Database = sqlx::Postgres>,
{
    sqlx::query_scalar("SELECT EXISTS (SELECT 1 FROM tables WHERE namespace_id = $1 AND name = $2)")
        .bind(namespace_id)
        .bind(name)
        .fetch_one(executor)
        .await
        .map_err(|e| map_sqlx_error("failed to check table name collision", e))
}

/// Inserts a view row, with its audit row and outbox event, atomically.
///
/// The initial view `metadata.json` must already be durably written at
/// `metadata_location` before this is called (invariant I4). The row starts
/// at `pointer_version` 0 — the version stamped into the initial metadata
/// file name.
///
/// Returns [`MeridianError::Conflict`] when the name is already taken by a
/// view *or a table* in the namespace (shared name space per the REST spec)
/// and [`MeridianError::NotFound`] when the namespace vanished concurrently.
pub async fn create(pool: &PgPool, view: NewView<'_>, principal: &str) -> Result<ViewRecord> {
    let mut tx = pool
        .begin()
        .await
        .map_err(|e| map_sqlx_error("failed to begin view create", e))?;

    // Tables and views share one name space. The check runs inside the
    // insert transaction; a racing table create can still slip between this
    // check and our commit (no cross-table constraint exists), which is
    // documented in docs/api-status.md.
    if table_name_taken(&mut *tx, view.namespace_id, view.name).await? {
        return Err(MeridianError::Conflict(format!(
            "a table named {:?} already exists in this namespace \
             (tables and views share a namespace)",
            view.name
        )));
    }

    let id = Ulid::new().to_string();
    let record: ViewRecord = sqlx::query_as(&format!(
        "INSERT INTO views
             (id, workspace_id, namespace_id, name, view_uuid, metadata_location,
              pointer_version, properties)
         VALUES ($1, $2, $3, $4, $5, $6, 0, $7)
         RETURNING {SELECT_COLUMNS}"
    ))
    .bind(&id)
    .bind(view.workspace_id.to_string())
    .bind(view.namespace_id)
    .bind(view.name)
    .bind(view.view_uuid)
    .bind(view.metadata_location)
    .bind(Json(view.properties))
    .fetch_one(&mut *tx)
    .await
    .map_err(|e| {
        if is_unique_violation(&e) {
            if e.as_database_error()
                .and_then(sqlx::error::DatabaseError::constraint)
                .is_some_and(|c| c == "views_view_uuid_unique")
            {
                MeridianError::Conflict(format!(
                    "a view with view-uuid {} is already registered in this deployment",
                    view.view_uuid
                ))
            } else {
                MeridianError::Conflict(format!("view {:?} already exists", view.name))
            }
        } else if e
            .as_database_error()
            .is_some_and(sqlx::error::DatabaseError::is_foreign_key_violation)
        {
            MeridianError::NotFound("namespace does not exist".to_owned())
        } else {
            map_sqlx_error("failed to insert view", e)
        }
    })?;

    let payload = json!({
        "namespace": view.namespace_levels,
        "name": view.name,
        "view_uuid": view.view_uuid,
        "metadata_location": view.metadata_location,
        "origin": "create",
    });
    outbox::enqueue(
        &mut *tx,
        &NewOutboxEvent {
            workspace_id: Some(view.workspace_id),
            aggregate: format!("view:{id}"),
            event_type: "view.created".to_owned(),
            payload: payload.clone(),
        },
    )
    .await?;

    audit::append_in_tx(
        &mut tx,
        NewAuditEntry {
            workspace_id: Some(view.workspace_id),
            principal: principal.to_owned(),
            action: "view.create".to_owned(),
            resource: format!("view:{id}"),
            details: payload,
        },
    )
    .await?;

    tx.commit()
        .await
        .map_err(|e| map_sqlx_error("failed to commit view create", e))?;

    Ok(record)
}

/// Renames (and/or moves) a view within a warehouse, with its audit row and
/// outbox event, atomically. Cross-namespace moves are supported; both
/// namespaces must belong to the same warehouse.
///
/// Returns [`MeridianError::NotFound`] when the source view or destination
/// namespace does not exist, and [`MeridianError::Conflict`] when the
/// destination identifier is already taken by a view *or a table*.
#[allow(clippy::too_many_arguments)] // source/destination pairs, not a config bag
pub async fn rename(
    pool: &PgPool,
    workspace_id: WorkspaceId,
    warehouse_id: &str,
    source_namespace: &[String],
    source_name: &str,
    destination_namespace: &[String],
    destination_name: &str,
    principal: &str,
) -> Result<()> {
    let mut tx = pool
        .begin()
        .await
        .map_err(|e| map_sqlx_error("failed to begin view rename", e))?;

    // Lock the source row: renames serialize with replaces and drops on the
    // same view.
    let source: Option<(String, String)> = sqlx::query_as(
        "SELECT v.id, v.namespace_id
         FROM views v
         JOIN namespaces n ON n.id = v.namespace_id
         WHERE n.warehouse_id = $1 AND n.levels = $2 AND v.name = $3
         FOR UPDATE OF v",
    )
    .bind(warehouse_id)
    .bind(source_namespace)
    .bind(source_name)
    .fetch_optional(&mut *tx)
    .await
    .map_err(|e| map_sqlx_error("failed to load view for rename", e))?;

    let Some((view_id, source_namespace_id)) = source else {
        return Err(MeridianError::NotFound(format!(
            "view {:?} does not exist",
            format_ident(source_namespace, source_name)
        )));
    };

    // Resolve the destination namespace. FOR SHARE holds off a concurrent
    // namespace drop until the moved view is visible to its emptiness check.
    let destination_namespace_id: String = if destination_namespace == source_namespace {
        source_namespace_id
    } else {
        let row: Option<(String,)> = sqlx::query_as(
            "SELECT id FROM namespaces WHERE warehouse_id = $1 AND levels = $2 FOR SHARE",
        )
        .bind(warehouse_id)
        .bind(destination_namespace)
        .fetch_optional(&mut *tx)
        .await
        .map_err(|e| map_sqlx_error("failed to resolve destination namespace", e))?;
        match row {
            Some((id,)) => id,
            None => {
                return Err(MeridianError::NotFound(format!(
                    "namespace {:?} does not exist",
                    destination_namespace.join(".")
                )));
            }
        }
    };

    // Shared name space: the destination must not be a table either.
    if table_name_taken(&mut *tx, &destination_namespace_id, destination_name).await? {
        return Err(MeridianError::Conflict(format!(
            "a table named {destination_name:?} already exists in the destination \
             namespace (tables and views share a namespace)"
        )));
    }

    sqlx::query("UPDATE views SET namespace_id = $1, name = $2, updated_at = now() WHERE id = $3")
        .bind(&destination_namespace_id)
        .bind(destination_name)
        .bind(&view_id)
        .execute(&mut *tx)
        .await
        .map_err(|e| {
            if is_unique_violation(&e) {
                MeridianError::Conflict(format!(
                    "view {:?} already exists",
                    format_ident(destination_namespace, destination_name)
                ))
            } else {
                map_sqlx_error("failed to rename view", e)
            }
        })?;

    let payload = json!({
        "source": { "namespace": source_namespace, "name": source_name },
        "destination": { "namespace": destination_namespace, "name": destination_name },
    });
    outbox::enqueue(
        &mut *tx,
        &NewOutboxEvent {
            workspace_id: Some(workspace_id),
            aggregate: format!("view:{view_id}"),
            event_type: "view.renamed".to_owned(),
            payload: payload.clone(),
        },
    )
    .await?;

    audit::append_in_tx(
        &mut tx,
        NewAuditEntry {
            workspace_id: Some(workspace_id),
            principal: principal.to_owned(),
            action: "view.rename".to_owned(),
            resource: format!("view:{view_id}"),
            details: payload,
        },
    )
    .await?;

    tx.commit()
        .await
        .map_err(|e| map_sqlx_error("failed to commit view rename", e))?;

    Ok(())
}

/// Drops a view: deletes the pointer row, with its audit row and outbox
/// event, atomically. Metadata files are never deleted here (the REST spec
/// defines no purge for views; the maintenance worker's sweep is the
/// eventual collector).
///
/// Returns the dropped row so the caller can act on its locations.
pub async fn drop_view(
    pool: &PgPool,
    workspace_id: WorkspaceId,
    warehouse_id: &str,
    namespace_levels: &[String],
    name: &str,
    principal: &str,
) -> Result<ViewRecord> {
    let mut tx = pool
        .begin()
        .await
        .map_err(|e| map_sqlx_error("failed to begin view drop", e))?;

    let record: Option<ViewRecord> = sqlx::query_as(&format!(
        "SELECT {SELECT_COLUMNS_V}
         FROM views v
         JOIN namespaces n ON n.id = v.namespace_id
         WHERE n.warehouse_id = $1 AND n.levels = $2 AND v.name = $3
         FOR UPDATE OF v",
    ))
    .bind(warehouse_id)
    .bind(namespace_levels)
    .bind(name)
    .fetch_optional(&mut *tx)
    .await
    .map_err(|e| map_sqlx_error("failed to load view for drop", e))?;

    let Some(record) = record else {
        return Err(MeridianError::NotFound(format!(
            "view {:?} does not exist",
            format_ident(namespace_levels, name)
        )));
    };

    sqlx::query("DELETE FROM views WHERE id = $1")
        .bind(&record.id)
        .execute(&mut *tx)
        .await
        .map_err(|e| map_sqlx_error("failed to delete view", e))?;

    let payload = json!({
        "namespace": namespace_levels,
        "name": name,
        "view_uuid": record.view_uuid,
        "metadata_location": record.metadata_location,
    });
    outbox::enqueue(
        &mut *tx,
        &NewOutboxEvent {
            workspace_id: Some(workspace_id),
            aggregate: format!("view:{}", record.id),
            event_type: "view.dropped".to_owned(),
            payload: payload.clone(),
        },
    )
    .await?;

    audit::append_in_tx(
        &mut tx,
        NewAuditEntry {
            workspace_id: Some(workspace_id),
            principal: principal.to_owned(),
            action: "view.drop".to_owned(),
            resource: format!("view:{}", record.id),
            details: payload,
        },
    )
    .await?;

    tx.commit()
        .await
        .map_err(|e| map_sqlx_error("failed to commit view drop", e))?;

    Ok(record)
}

/// Atomically swaps a view pointer (the replace-view commit), with
/// write-through properties, its audit row, and its outbox event on one
/// transaction.
///
/// The candidate view `metadata.json` must already be durably written at
/// `swap.new_metadata_location` (invariant I4). The row is locked
/// (`FOR UPDATE`) and the `UPDATE` retains the version guard anyway —
/// defense in depth, same as the table commit path.
pub async fn commit_replace(
    pool: &PgPool,
    workspace_id: WorkspaceId,
    swap: ViewPointerSwap<'_>,
    principal: &str,
) -> Result<ViewRecord, ViewCommitError> {
    let mut tx = pool
        .begin()
        .await
        .map_err(|e| map_sqlx_error("failed to begin view replace", e))?;

    let locked: Option<(i64, Option<String>)> = sqlx::query_as(
        "SELECT pointer_version, metadata_location FROM views WHERE id = $1 FOR UPDATE",
    )
    .bind(swap.view_id)
    .fetch_optional(&mut *tx)
    .await
    .map_err(|e| map_sqlx_error("failed to lock view for replace", e))?;

    let Some((actual_version, previous_location)) = locked else {
        return Err(ViewCommitError::NotFound);
    };
    if actual_version != swap.expected_version {
        return Err(ViewCommitError::VersionConflict {
            expected: swap.expected_version,
            actual: actual_version,
        });
    }

    let record: Option<ViewRecord> = sqlx::query_as(&format!(
        "UPDATE views
         SET pointer_version = pointer_version + 1,
             metadata_location = $1,
             properties = $2,
             updated_at = now()
         WHERE id = $3 AND pointer_version = $4
         RETURNING {SELECT_COLUMNS}"
    ))
    .bind(swap.new_metadata_location)
    .bind(Json(swap.properties))
    .bind(swap.view_id)
    .bind(swap.expected_version)
    .fetch_optional(&mut *tx)
    .await
    .map_err(|e| map_sqlx_error("failed to swap view pointer", e))?;

    let Some(record) = record else {
        // Unreachable under the row lock; the guard is the actual
        // correctness mechanism and must never be trusted less than the
        // lock.
        return Err(ViewCommitError::VersionConflict {
            expected: swap.expected_version,
            actual: actual_version,
        });
    };

    let mut details = json!({
        "pointer_version": record.pointer_version,
        "metadata_location": swap.new_metadata_location,
        "previous_metadata_location": previous_location,
    });
    if let (Value::Object(target), Value::Object(extra)) = (&mut details, &swap.event_details) {
        target.extend(extra.clone());
    }

    outbox::enqueue(
        &mut *tx,
        &NewOutboxEvent {
            workspace_id: Some(workspace_id),
            aggregate: format!("view:{}", swap.view_id),
            event_type: "view.committed".to_owned(),
            payload: details.clone(),
        },
    )
    .await
    .map_err(ViewCommitError::Store)?;

    audit::append_in_tx(
        &mut tx,
        NewAuditEntry {
            workspace_id: Some(workspace_id),
            principal: principal.to_owned(),
            action: "view.commit".to_owned(),
            resource: format!("view:{}", swap.view_id),
            details,
        },
    )
    .await
    .map_err(ViewCommitError::Store)?;

    // The point of no return. Failure *of the commit statement itself* is
    // the one place the outcome is genuinely unknown (F3).
    tx.commit()
        .await
        .map_err(|error| ViewCommitError::StateUnknown {
            message: format!("transaction commit failed: {error}"),
        })?;

    Ok(record)
}

/// Renders a view identifier for human-readable messages.
fn format_ident(namespace: &[String], name: &str) -> String {
    if namespace.is_empty() {
        name.to_owned()
    } else {
        format!("{}.{name}", namespace.join("."))
    }
}
