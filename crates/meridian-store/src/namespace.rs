//! Namespace persistence and lifecycle operations.
//!
//! Namespaces are multi-level (`["accounting", "tax"]`) and live directly
//! under a warehouse; the full level path is stored as a `TEXT[]`. Listing a
//! level and emptiness checks are array-prefix queries over that column.
//!
//! Every mutation writes its audit row and outbox event on the same
//! transaction as the state change.

use std::collections::BTreeMap;

use chrono::{DateTime, Utc};
use meridian_common::id::WorkspaceId;
use meridian_common::{MeridianError, Result};
use serde_json::json;
use sqlx::PgPool;
use sqlx::types::Json;
use ulid::Ulid;

use crate::audit::{self, NewAuditEntry};
use crate::map_sqlx_error;
use crate::outbox::{self, NewOutboxEvent};

/// A persisted namespace row.
#[derive(Debug, Clone, sqlx::FromRow)]
pub struct NamespaceRecord {
    /// ULID of the namespace.
    pub id: String,
    /// Owning workspace.
    pub workspace_id: String,
    /// Owning warehouse.
    pub warehouse_id: String,
    /// Namespace levels, outermost first.
    pub levels: Vec<String>,
    /// String-to-string namespace properties.
    pub properties: Json<BTreeMap<String, String>>,
    /// Creation time.
    pub created_at: DateTime<Utc>,
    /// Last update time.
    pub updated_at: DateTime<Utc>,
}

/// Outcome of a property update: which keys were set, removed, or requested
/// for removal but absent.
#[derive(Debug, Clone)]
pub struct PropertyUpdateOutcome {
    /// Keys added or updated.
    pub updated: Vec<String>,
    /// Keys that existed and were removed.
    pub removed: Vec<String>,
    /// Keys requested for removal that were not present.
    pub missing: Vec<String>,
}

const SELECT_COLUMNS: &str =
    "id, workspace_id, warehouse_id, levels, properties, created_at, updated_at";

/// True when the error is a Postgres unique-constraint violation.
fn is_unique_violation(error: &sqlx::Error) -> bool {
    error
        .as_database_error()
        .is_some_and(sqlx::error::DatabaseError::is_unique_violation)
}

/// Renders levels for human-readable messages (`a.b.c`).
fn display_levels(levels: &[String]) -> String {
    levels.join(".")
}

/// Creates a namespace, with its audit row and outbox event, atomically.
///
/// Multi-level namespaces require their parent to exist
/// ([`MeridianError::NotFound`] otherwise). A namespace that already exists
/// is [`MeridianError::Conflict`].
pub async fn create(
    pool: &PgPool,
    workspace_id: WorkspaceId,
    warehouse_id: &str,
    levels: &[String],
    properties: BTreeMap<String, String>,
    principal: &str,
) -> Result<NamespaceRecord> {
    let mut tx = pool
        .begin()
        .await
        .map_err(|e| map_sqlx_error("failed to begin namespace create", e))?;

    if levels.len() > 1 {
        let parent = &levels[..levels.len() - 1];
        // FOR SHARE: holds off a concurrent delete of the parent until this
        // transaction commits (the delete's FOR UPDATE then sees the child
        // and rejects), preventing an orphaned child namespace.
        let parent_exists: Option<(String,)> = sqlx::query_as(
            "SELECT id FROM namespaces WHERE warehouse_id = $1 AND levels = $2 FOR SHARE",
        )
        .bind(warehouse_id)
        .bind(parent)
        .fetch_optional(&mut *tx)
        .await
        .map_err(|e| map_sqlx_error("failed to check parent namespace", e))?;
        if parent_exists.is_none() {
            return Err(MeridianError::NotFound(format!(
                "parent namespace {:?} does not exist",
                display_levels(parent)
            )));
        }
    }

    let id = Ulid::new().to_string();
    let record: NamespaceRecord = sqlx::query_as(&format!(
        "INSERT INTO namespaces (id, workspace_id, warehouse_id, levels, properties)
         VALUES ($1, $2, $3, $4, $5)
         RETURNING {SELECT_COLUMNS}"
    ))
    .bind(&id)
    .bind(workspace_id.to_string())
    .bind(warehouse_id)
    .bind(levels)
    .bind(Json(&properties))
    .fetch_one(&mut *tx)
    .await
    .map_err(|e| {
        if is_unique_violation(&e) {
            MeridianError::Conflict(format!(
                "namespace {:?} already exists",
                display_levels(levels)
            ))
        } else if e
            .as_database_error()
            .is_some_and(sqlx::error::DatabaseError::is_foreign_key_violation)
        {
            // The warehouse was deleted between prefix resolution and this
            // insert.
            MeridianError::NotFound("warehouse does not exist".to_owned())
        } else {
            map_sqlx_error("failed to insert namespace", e)
        }
    })?;

    outbox::enqueue(
        &mut *tx,
        &NewOutboxEvent {
            workspace_id: Some(workspace_id),
            aggregate: format!("namespace:{id}"),
            event_type: "namespace.created".to_owned(),
            payload: json!({ "warehouse_id": warehouse_id, "levels": levels }),
        },
    )
    .await?;

    audit::append_in_tx(
        &mut tx,
        NewAuditEntry {
            workspace_id: Some(workspace_id),
            principal: principal.to_owned(),
            action: "namespace.create".to_owned(),
            resource: format!("namespace:{id}"),
            details: json!({ "warehouse_id": warehouse_id, "levels": levels }),
        },
    )
    .await?;

    tx.commit()
        .await
        .map_err(|e| map_sqlx_error("failed to commit namespace create", e))?;

    Ok(record)
}

/// Loads a namespace by its exact levels.
pub async fn get(
    pool: &PgPool,
    warehouse_id: &str,
    levels: &[String],
) -> Result<Option<NamespaceRecord>> {
    sqlx::query_as(&format!(
        "SELECT {SELECT_COLUMNS} FROM namespaces WHERE warehouse_id = $1 AND levels = $2"
    ))
    .bind(warehouse_id)
    .bind(levels)
    .fetch_optional(pool)
    .await
    .map_err(|e| map_sqlx_error("failed to load namespace", e))
}

/// Lists namespaces exactly one level below `parent` (top-level namespaces
/// for an empty `parent`), in stable id (creation) order.
///
/// Keyset pagination: pass the `id` of the last row of the previous page as
/// `after_id`; `limit` bounds the page (`None` returns everything).
pub async fn list(
    pool: &PgPool,
    warehouse_id: &str,
    parent: &[String],
    after_id: Option<&str>,
    limit: Option<i64>,
) -> Result<Vec<NamespaceRecord>> {
    sqlx::query_as(&format!(
        "SELECT {SELECT_COLUMNS}
         FROM namespaces
         WHERE warehouse_id = $1
           AND cardinality(levels) = cardinality($2::text[]) + 1
           AND levels[1:cardinality($2::text[])] = $2::text[]
           AND ($3::text IS NULL OR id > $3)
         ORDER BY id
         LIMIT $4"
    ))
    .bind(warehouse_id)
    .bind(parent)
    .bind(after_id)
    .bind(limit)
    .fetch_all(pool)
    .await
    .map_err(|e| map_sqlx_error("failed to list namespaces", e))
}

/// Deletes an empty namespace, with its audit row and outbox event,
/// atomically.
///
/// Returns [`MeridianError::NotFound`] when the namespace does not exist and
/// [`MeridianError::Conflict`] when it still contains child namespaces or
/// tables.
pub async fn delete(
    pool: &PgPool,
    workspace_id: WorkspaceId,
    warehouse_id: &str,
    levels: &[String],
    principal: &str,
) -> Result<()> {
    let mut tx = pool
        .begin()
        .await
        .map_err(|e| map_sqlx_error("failed to begin namespace delete", e))?;

    let row: Option<(String,)> = sqlx::query_as(
        "SELECT id FROM namespaces WHERE warehouse_id = $1 AND levels = $2 FOR UPDATE",
    )
    .bind(warehouse_id)
    .bind(levels)
    .fetch_optional(&mut *tx)
    .await
    .map_err(|e| map_sqlx_error("failed to load namespace for delete", e))?;

    let Some((id,)) = row else {
        return Err(MeridianError::NotFound(format!(
            "namespace {:?} does not exist",
            display_levels(levels)
        )));
    };

    let child_count: i64 = sqlx::query_scalar(
        "SELECT count(*) FROM namespaces
         WHERE warehouse_id = $1
           AND cardinality(levels) > cardinality($2::text[])
           AND levels[1:cardinality($2::text[])] = $2::text[]",
    )
    .bind(warehouse_id)
    .bind(levels)
    .fetch_one(&mut *tx)
    .await
    .map_err(|e| map_sqlx_error("failed to count child namespaces", e))?;
    if child_count > 0 {
        return Err(MeridianError::Conflict(format!(
            "namespace {:?} is not empty: {child_count} child namespace(s) remain",
            display_levels(levels)
        )));
    }

    let table_count: i64 =
        sqlx::query_scalar("SELECT count(*) FROM tables WHERE namespace_id = $1")
            .bind(&id)
            .fetch_one(&mut *tx)
            .await
            .map_err(|e| map_sqlx_error("failed to count namespace tables", e))?;
    if table_count > 0 {
        return Err(MeridianError::Conflict(format!(
            "namespace {:?} is not empty: {table_count} table(s) remain",
            display_levels(levels)
        )));
    }

    sqlx::query("DELETE FROM namespaces WHERE id = $1")
        .bind(&id)
        .execute(&mut *tx)
        .await
        .map_err(|e| map_sqlx_error("failed to delete namespace", e))?;

    outbox::enqueue(
        &mut *tx,
        &NewOutboxEvent {
            workspace_id: Some(workspace_id),
            aggregate: format!("namespace:{id}"),
            event_type: "namespace.deleted".to_owned(),
            payload: json!({ "warehouse_id": warehouse_id, "levels": levels }),
        },
    )
    .await?;

    audit::append_in_tx(
        &mut tx,
        NewAuditEntry {
            workspace_id: Some(workspace_id),
            principal: principal.to_owned(),
            action: "namespace.delete".to_owned(),
            resource: format!("namespace:{id}"),
            details: json!({ "warehouse_id": warehouse_id, "levels": levels }),
        },
    )
    .await?;

    tx.commit()
        .await
        .map_err(|e| map_sqlx_error("failed to commit namespace delete", e))?;

    Ok(())
}

/// Applies property updates and removals atomically, with the audit row and
/// outbox event on the same transaction.
///
/// Overlap between `updates` and `removals` must be rejected by the caller
/// before calling (it is a request-shape error, HTTP 422). Removals of keys
/// that are not present are reported in
/// [`PropertyUpdateOutcome::missing`], not errors.
pub async fn update_properties(
    pool: &PgPool,
    workspace_id: WorkspaceId,
    warehouse_id: &str,
    levels: &[String],
    updates: BTreeMap<String, String>,
    removals: Vec<String>,
    principal: &str,
) -> Result<PropertyUpdateOutcome> {
    let mut tx = pool
        .begin()
        .await
        .map_err(|e| map_sqlx_error("failed to begin namespace property update", e))?;

    let row: Option<(String, Json<BTreeMap<String, String>>)> = sqlx::query_as(
        "SELECT id, properties FROM namespaces
         WHERE warehouse_id = $1 AND levels = $2 FOR UPDATE",
    )
    .bind(warehouse_id)
    .bind(levels)
    .fetch_optional(&mut *tx)
    .await
    .map_err(|e| map_sqlx_error("failed to load namespace for property update", e))?;

    let Some((id, Json(mut properties))) = row else {
        return Err(MeridianError::NotFound(format!(
            "namespace {:?} does not exist",
            display_levels(levels)
        )));
    };

    let mut outcome = PropertyUpdateOutcome {
        updated: Vec::new(),
        removed: Vec::new(),
        missing: Vec::new(),
    };

    for key in removals {
        if properties.remove(&key).is_some() {
            outcome.removed.push(key);
        } else {
            outcome.missing.push(key);
        }
    }
    for (key, value) in updates {
        properties.insert(key.clone(), value);
        outcome.updated.push(key);
    }

    sqlx::query("UPDATE namespaces SET properties = $1, updated_at = now() WHERE id = $2")
        .bind(Json(&properties))
        .bind(&id)
        .execute(&mut *tx)
        .await
        .map_err(|e| map_sqlx_error("failed to update namespace properties", e))?;

    outbox::enqueue(
        &mut *tx,
        &NewOutboxEvent {
            workspace_id: Some(workspace_id),
            aggregate: format!("namespace:{id}"),
            event_type: "namespace.properties_updated".to_owned(),
            payload: json!({
                "warehouse_id": warehouse_id,
                "levels": levels,
                "updated": outcome.updated,
                "removed": outcome.removed,
            }),
        },
    )
    .await?;

    audit::append_in_tx(
        &mut tx,
        NewAuditEntry {
            workspace_id: Some(workspace_id),
            principal: principal.to_owned(),
            action: "namespace.update_properties".to_owned(),
            resource: format!("namespace:{id}"),
            details: json!({
                "warehouse_id": warehouse_id,
                "levels": levels,
                "updated": outcome.updated,
                "removed": outcome.removed,
                "missing": outcome.missing,
            }),
        },
    )
    .await?;

    tx.commit()
        .await
        .map_err(|e| map_sqlx_error("failed to commit namespace property update", e))?;

    Ok(outcome)
}
