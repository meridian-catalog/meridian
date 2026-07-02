//! Warehouse persistence and lifecycle operations.
//!
//! A warehouse is a storage root (bucket/prefix) plus its non-secret access
//! configuration, scoped to a workspace. Each warehouse maps one-to-one onto
//! an Iceberg REST catalog `{prefix}`: the warehouse *name* is the prefix.
//!
//! Every mutation here writes its audit row and outbox event on the same
//! transaction as the state change (see `docs/design/commit-protocol.md` §I4:
//! a change is visible iff its audit row and outbox event exist).

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

/// A persisted warehouse row.
#[derive(Debug, Clone, sqlx::FromRow)]
pub struct WarehouseRecord {
    /// ULID of the warehouse.
    pub id: String,
    /// Owning workspace.
    pub workspace_id: String,
    /// Warehouse name; doubles as the IRC `{prefix}`.
    pub name: String,
    /// Storage root URI, e.g. `s3://bucket/prefix`.
    pub storage_root: String,
    /// Non-secret storage options (endpoint, region, ...). Secret material
    /// never lives here (M2: credential vending / vault integration).
    pub storage_config: Json<BTreeMap<String, String>>,
    /// Creation time.
    pub created_at: DateTime<Utc>,
    /// Last update time.
    pub updated_at: DateTime<Utc>,
}

/// True when the error is a Postgres unique-constraint violation.
fn is_unique_violation(error: &sqlx::Error) -> bool {
    error
        .as_database_error()
        .is_some_and(sqlx::error::DatabaseError::is_unique_violation)
}

/// Creates a warehouse, with its audit row and outbox event, atomically.
///
/// Returns [`MeridianError::Conflict`] when a warehouse of the same name
/// already exists in the workspace.
pub async fn create(
    pool: &PgPool,
    workspace_id: WorkspaceId,
    name: &str,
    storage_root: &str,
    storage_options: BTreeMap<String, String>,
    principal: &str,
) -> Result<WarehouseRecord> {
    let mut tx = pool
        .begin()
        .await
        .map_err(|e| map_sqlx_error("failed to begin warehouse create", e))?;

    let id = Ulid::new().to_string();
    let record: WarehouseRecord = sqlx::query_as(
        "INSERT INTO warehouses (id, workspace_id, name, storage_root, storage_config)
         VALUES ($1, $2, $3, $4, $5)
         RETURNING id, workspace_id, name, storage_root, storage_config,
                   created_at, updated_at",
    )
    .bind(&id)
    .bind(workspace_id.to_string())
    .bind(name)
    .bind(storage_root)
    .bind(Json(&storage_options))
    .fetch_one(&mut *tx)
    .await
    .map_err(|e| {
        if is_unique_violation(&e) {
            MeridianError::Conflict(format!("warehouse {name:?} already exists"))
        } else {
            map_sqlx_error("failed to insert warehouse", e)
        }
    })?;

    outbox::enqueue(
        &mut *tx,
        &NewOutboxEvent {
            workspace_id: Some(workspace_id),
            aggregate: format!("warehouse:{id}"),
            event_type: "warehouse.created".to_owned(),
            payload: json!({ "name": name, "storage_root": storage_root }),
        },
    )
    .await?;

    audit::append_in_tx(
        &mut tx,
        NewAuditEntry {
            workspace_id: Some(workspace_id),
            principal: principal.to_owned(),
            action: "warehouse.create".to_owned(),
            resource: format!("warehouse:{id}"),
            details: json!({ "name": name, "storage_root": storage_root }),
        },
    )
    .await?;

    tx.commit()
        .await
        .map_err(|e| map_sqlx_error("failed to commit warehouse create", e))?;

    Ok(record)
}

/// Lists all warehouses of a workspace, ordered by name.
pub async fn list(pool: &PgPool, workspace_id: WorkspaceId) -> Result<Vec<WarehouseRecord>> {
    sqlx::query_as(
        "SELECT id, workspace_id, name, storage_root, storage_config, created_at, updated_at
         FROM warehouses
         WHERE workspace_id = $1
         ORDER BY name",
    )
    .bind(workspace_id.to_string())
    .fetch_all(pool)
    .await
    .map_err(|e| map_sqlx_error("failed to list warehouses", e))
}

/// Looks a warehouse up by name within a workspace.
pub async fn get_by_name(
    pool: &PgPool,
    workspace_id: WorkspaceId,
    name: &str,
) -> Result<Option<WarehouseRecord>> {
    sqlx::query_as(
        "SELECT id, workspace_id, name, storage_root, storage_config, created_at, updated_at
         FROM warehouses
         WHERE workspace_id = $1 AND name = $2",
    )
    .bind(workspace_id.to_string())
    .bind(name)
    .fetch_optional(pool)
    .await
    .map_err(|e| map_sqlx_error("failed to load warehouse", e))
}

/// Deletes a warehouse by name, with its audit row and outbox event,
/// atomically. The warehouse must be empty (no namespaces).
///
/// Returns [`MeridianError::NotFound`] when the warehouse does not exist and
/// [`MeridianError::Conflict`] when it still contains namespaces.
pub async fn delete_by_name(
    pool: &PgPool,
    workspace_id: WorkspaceId,
    name: &str,
    principal: &str,
) -> Result<()> {
    let mut tx = pool
        .begin()
        .await
        .map_err(|e| map_sqlx_error("failed to begin warehouse delete", e))?;

    // Lock the row so a concurrent namespace create in this warehouse either
    // happens before the emptiness check or fails its FK after the delete.
    let row: Option<(String,)> = sqlx::query_as(
        "SELECT id FROM warehouses WHERE workspace_id = $1 AND name = $2 FOR UPDATE",
    )
    .bind(workspace_id.to_string())
    .bind(name)
    .fetch_optional(&mut *tx)
    .await
    .map_err(|e| map_sqlx_error("failed to load warehouse for delete", e))?;

    let Some((id,)) = row else {
        return Err(MeridianError::NotFound(format!(
            "warehouse {name:?} does not exist"
        )));
    };

    let namespace_count: i64 =
        sqlx::query_scalar("SELECT count(*) FROM namespaces WHERE warehouse_id = $1")
            .bind(&id)
            .fetch_one(&mut *tx)
            .await
            .map_err(|e| map_sqlx_error("failed to count warehouse namespaces", e))?;
    if namespace_count > 0 {
        return Err(MeridianError::Conflict(format!(
            "warehouse {name:?} is not empty: {namespace_count} namespace(s) remain"
        )));
    }

    sqlx::query("DELETE FROM warehouses WHERE id = $1")
        .bind(&id)
        .execute(&mut *tx)
        .await
        .map_err(|e| {
            if e.as_database_error()
                .is_some_and(sqlx::error::DatabaseError::is_foreign_key_violation)
            {
                // A namespace create raced past the emptiness check; the FK
                // (ON DELETE RESTRICT) is the backstop.
                MeridianError::Conflict(format!("warehouse {name:?} is not empty"))
            } else {
                map_sqlx_error("failed to delete warehouse", e)
            }
        })?;

    outbox::enqueue(
        &mut *tx,
        &NewOutboxEvent {
            workspace_id: Some(workspace_id),
            aggregate: format!("warehouse:{id}"),
            event_type: "warehouse.deleted".to_owned(),
            payload: json!({ "name": name }),
        },
    )
    .await?;

    audit::append_in_tx(
        &mut tx,
        NewAuditEntry {
            workspace_id: Some(workspace_id),
            principal: principal.to_owned(),
            action: "warehouse.delete".to_owned(),
            resource: format!("warehouse:{id}"),
            details: json!({ "name": name }),
        },
    )
    .await?;

    tx.commit()
        .await
        .map_err(|e| map_sqlx_error("failed to commit warehouse delete", e))?;

    Ok(())
}
