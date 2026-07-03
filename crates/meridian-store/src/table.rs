//! Table persistence and lifecycle operations (everything *except* the
//! pointer swap — that is the commit path and lives in [`crate::commit`]).
//!
//! A table row carries the commit-protocol pointer:
//! (`metadata_location`, `pointer_version`) per
//! `docs/design/commit-protocol.md` §2. This module owns creation, lookup,
//! listing, rename, and drop; every mutation writes its audit row and outbox
//! event on the same transaction as the state change (invariant I6: no code
//! path mutates a pointer without its audit row).

use std::collections::BTreeMap;

use chrono::{DateTime, Utc};
use meridian_common::id::WorkspaceId;
use meridian_common::{MeridianError, Result};
use serde_json::json;
use sqlx::PgPool;
use sqlx::types::Json;
use ulid::Ulid;

use crate::audit::{self, NewAuditEntry};
use crate::commit::{RECEIPT_TTL_HOURS, ReceiptToRecord};
use crate::map_sqlx_error;
use crate::outbox::{self, NewOutboxEvent};

/// A persisted table row.
#[derive(Debug, Clone, sqlx::FromRow)]
pub struct TableRecord {
    /// ULID of the table (Meridian's internal identity).
    pub id: String,
    /// Owning workspace.
    pub workspace_id: String,
    /// Owning namespace.
    pub namespace_id: String,
    /// Table name within the namespace.
    pub name: String,
    /// Iceberg table UUID (canonical hyphenated form), stable for the
    /// table's lifetime.
    pub table_uuid: String,
    /// Object-storage location of the current `metadata.json`. Always set
    /// for committed tables.
    pub metadata_location: Option<String>,
    /// The location the current commit replaced, if any.
    pub previous_metadata_location: Option<String>,
    /// Monotonic pointer version; +1 per committed swap (the CAS guard and
    /// the `ETag` source).
    pub pointer_version: i64,
    /// Iceberg format version (1..=3), write-through-indexed from metadata.
    pub format_version: i16,
    /// Table properties, write-through-indexed from metadata.
    pub properties: Json<BTreeMap<String, String>>,
    /// Creation time.
    pub created_at: DateTime<Utc>,
    /// Last update time.
    pub updated_at: DateTime<Utc>,
}

const SELECT_COLUMNS: &str = "id, workspace_id, namespace_id, name, table_uuid, \
     metadata_location, previous_metadata_location, pointer_version, format_version, \
     properties, created_at, updated_at";

/// [`SELECT_COLUMNS`] qualified with the `t.` alias for joined queries.
const SELECT_COLUMNS_T: &str = "t.id, t.workspace_id, t.namespace_id, t.name, t.table_uuid, \
     t.metadata_location, t.previous_metadata_location, t.pointer_version, t.format_version, \
     t.properties, t.created_at, t.updated_at";

/// A new table row to insert (used by create, register, and the
/// commit-endpoint create transaction).
#[derive(Debug, Clone)]
pub struct NewTable<'a> {
    /// Owning workspace.
    pub workspace_id: WorkspaceId,
    /// Owning namespace id.
    pub namespace_id: &'a str,
    /// Namespace levels (for event/audit payloads).
    pub namespace_levels: &'a [String],
    /// Table name.
    pub name: &'a str,
    /// Iceberg table UUID (canonical hyphenated form).
    pub table_uuid: &'a str,
    /// Location of the initial `metadata.json` (already durably written).
    pub metadata_location: &'a str,
    /// Iceberg format version.
    pub format_version: i16,
    /// Table properties, write-through-indexed.
    pub properties: &'a BTreeMap<String, String>,
    /// Flattened column names and docs of the current schema, write-through
    /// indexed for full-text search (migration 0010; see
    /// [`crate::search::schema_search_text`]).
    pub schema_text: Option<&'a str>,
    /// Snapshot rows to write-through-index (migration 0003; powers health,
    /// reconciliation, and observability). Adopted (`register`) tables carry
    /// their full history here; a fresh table's slice is empty.
    pub snapshots: &'a [crate::commit::SnapshotIndexRow],
    /// How the table came to be, recorded in the audit trail:
    /// `"create"`, `"register"`, or `"commit-create"`.
    pub origin: &'a str,
}

/// Inserts write-through snapshot-index rows for a table on the caller's
/// transaction (migration 0003). Used by table creation and registration so
/// health, reconciliation, and observability see a table's history.
async fn index_snapshots(
    tx: &mut sqlx::Transaction<'_, sqlx::Postgres>,
    table_id: &str,
    snapshots: &[crate::commit::SnapshotIndexRow],
) -> Result<()> {
    for snapshot in snapshots {
        sqlx::query(
            "INSERT INTO table_snapshots
                 (table_id, snapshot_id, parent_snapshot_id, sequence_number, timestamp_ms,
                  manifest_list, operation, summary, is_current)
             VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9)",
        )
        .bind(table_id)
        .bind(snapshot.snapshot_id)
        .bind(snapshot.parent_snapshot_id)
        .bind(snapshot.sequence_number)
        .bind(snapshot.timestamp_ms)
        .bind(&snapshot.manifest_list)
        .bind(&snapshot.operation)
        .bind(&snapshot.summary)
        .bind(snapshot.is_current)
        .execute(&mut **tx)
        .await
        .map_err(|e| map_sqlx_error("failed to index table snapshots", e))?;
    }
    Ok(())
}

/// True when the error is a Postgres unique-constraint violation.
fn is_unique_violation(error: &sqlx::Error) -> bool {
    error
        .as_database_error()
        .is_some_and(sqlx::error::DatabaseError::is_unique_violation)
}

/// Loads a table by namespace id and name.
pub async fn get(pool: &PgPool, namespace_id: &str, name: &str) -> Result<Option<TableRecord>> {
    sqlx::query_as(&format!(
        "SELECT {SELECT_COLUMNS} FROM tables WHERE namespace_id = $1 AND name = $2"
    ))
    .bind(namespace_id)
    .bind(name)
    .fetch_optional(pool)
    .await
    .map_err(|e| map_sqlx_error("failed to load table", e))
}

/// Loads a table by warehouse, namespace levels, and name in one query.
///
/// Returns `None` when either the namespace or the table does not exist;
/// callers that need to distinguish resolve the namespace first.
pub async fn get_by_name(
    pool: &PgPool,
    warehouse_id: &str,
    namespace_levels: &[String],
    name: &str,
) -> Result<Option<TableRecord>> {
    sqlx::query_as(&format!(
        "SELECT {SELECT_COLUMNS_T}
         FROM tables t
         JOIN namespaces n ON n.id = t.namespace_id
         WHERE n.warehouse_id = $1 AND n.levels = $2 AND t.name = $3"
    ))
    .bind(warehouse_id)
    .bind(namespace_levels)
    .bind(name)
    .fetch_optional(pool)
    .await
    .map_err(|e| map_sqlx_error("failed to load table by name", e))
}

/// Lists tables of a namespace in stable id (creation) order.
///
/// Keyset pagination: pass the `id` of the last row of the previous page as
/// `after_id`; `limit` bounds the page (`None` returns everything).
pub async fn list(
    pool: &PgPool,
    namespace_id: &str,
    after_id: Option<&str>,
    limit: Option<i64>,
) -> Result<Vec<TableRecord>> {
    sqlx::query_as(&format!(
        "SELECT {SELECT_COLUMNS}
         FROM tables
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
    .map_err(|e| map_sqlx_error("failed to list tables", e))
}

/// Inserts a table row, with its audit row and outbox event, atomically.
///
/// The initial `metadata.json` must already be durably written at
/// `metadata_location` before this is called (invariant I4: the pointer
/// never references a file that is not durably written). The row starts at
/// `pointer_version` 0 — the version stamped into the initial metadata file
/// name.
///
/// When `receipt` is given (the commit-endpoint create transaction carries
/// an idempotency key), it is recorded in the same transaction (I5).
///
/// Returns [`MeridianError::Conflict`] when the table already exists and
/// [`MeridianError::NotFound`] when the namespace vanished concurrently.
pub async fn create(
    pool: &PgPool,
    table: NewTable<'_>,
    principal: &str,
    receipt: Option<&ReceiptToRecord>,
) -> Result<TableRecord> {
    let mut tx = pool
        .begin()
        .await
        .map_err(|e| map_sqlx_error("failed to begin table create", e))?;

    let id = Ulid::new().to_string();
    let record: TableRecord = sqlx::query_as(&format!(
        "INSERT INTO tables
             (id, workspace_id, namespace_id, name, table_uuid, metadata_location,
              pointer_version, format_version, properties, schema_text)
         VALUES ($1, $2, $3, $4, $5, $6, 0, $7, $8, $9)
         RETURNING {SELECT_COLUMNS}"
    ))
    .bind(&id)
    .bind(table.workspace_id.to_string())
    .bind(table.namespace_id)
    .bind(table.name)
    .bind(table.table_uuid)
    .bind(table.metadata_location)
    .bind(table.format_version)
    .bind(Json(table.properties))
    .bind(table.schema_text)
    .fetch_one(&mut *tx)
    .await
    .map_err(|e| {
        if is_unique_violation(&e) {
            // Two unique constraints can fire on this INSERT and they mean
            // different things: (namespace_id, name) — the requested name is
            // taken — and tables_table_uuid_unique — the metadata file being
            // adopted (register path) belongs to a table that is still
            // registered, possibly under another name. Report the right one.
            if e.as_database_error()
                .and_then(sqlx::error::DatabaseError::constraint)
                .is_some_and(|c| c == "tables_table_uuid_unique")
            {
                MeridianError::Conflict(format!(
                    "a table with table-uuid {} is already registered in this \
                     warehouse (possibly under a different name); a metadata \
                     file can only be adopted once its owning table is dropped",
                    table.table_uuid
                ))
            } else {
                MeridianError::Conflict(format!("table {:?} already exists", table.name))
            }
        } else if e
            .as_database_error()
            .is_some_and(sqlx::error::DatabaseError::is_foreign_key_violation)
        {
            MeridianError::NotFound("namespace does not exist".to_owned())
        } else {
            map_sqlx_error("failed to insert table", e)
        }
    })?;

    if let Some(receipt) = receipt {
        record_receipt(&mut tx, table.workspace_id, receipt).await?;
    }

    // Write-through-index the adopted snapshots (register carries history;
    // create passes an empty slice). Same transaction as the pointer row so
    // the index can never lag the table (write-through invariant, §8.2).
    index_snapshots(&mut tx, &id, table.snapshots).await?;

    let payload = json!({
        "namespace": table.namespace_levels,
        "name": table.name,
        "table_uuid": table.table_uuid,
        "metadata_location": table.metadata_location,
        "origin": table.origin,
    });
    outbox::enqueue(
        &mut *tx,
        &NewOutboxEvent {
            workspace_id: Some(table.workspace_id),
            aggregate: format!("table:{id}"),
            event_type: "table.created".to_owned(),
            payload: payload.clone(),
        },
    )
    .await?;

    audit::append_in_tx(
        &mut tx,
        NewAuditEntry {
            workspace_id: Some(table.workspace_id),
            principal: principal.to_owned(),
            action: "table.create".to_owned(),
            resource: format!("table:{id}"),
            details: payload,
        },
    )
    .await?;

    tx.commit()
        .await
        .map_err(|e| map_sqlx_error("failed to commit table create", e))?;

    Ok(record)
}

/// Records an idempotency receipt on the caller's transaction.
pub(crate) async fn record_receipt(
    tx: &mut sqlx::Transaction<'_, sqlx::Postgres>,
    workspace_id: WorkspaceId,
    receipt: &ReceiptToRecord,
) -> Result<()> {
    sqlx::query(
        "INSERT INTO idempotency_keys
             (workspace_id, idempotency_key, request_hash, response_status, response_body,
              expires_at)
         VALUES ($1, $2, $3, $4, $5, now() + make_interval(hours => $6))",
    )
    .bind(workspace_id.to_string())
    .bind(&receipt.key)
    .bind(&receipt.fingerprint)
    .bind(receipt.response_status)
    .bind(&receipt.response_body)
    .bind(RECEIPT_TTL_HOURS)
    .execute(&mut **tx)
    .await
    .map_err(|e| {
        if is_unique_violation(&e) {
            MeridianError::Conflict(format!(
                "idempotency key {:?} was recorded concurrently",
                receipt.key
            ))
        } else {
            map_sqlx_error("failed to record idempotency receipt", e)
        }
    })?;
    Ok(())
}

/// Renames (and/or moves) a table within a warehouse, with its audit row and
/// outbox event, atomically. Cross-namespace moves are supported; both
/// namespaces must belong to the same warehouse.
///
/// Returns [`MeridianError::NotFound`] when the source table or destination
/// namespace does not exist, and [`MeridianError::Conflict`] when the
/// destination identifier is already taken.
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
        .map_err(|e| map_sqlx_error("failed to begin table rename", e))?;

    // Lock the source row: renames serialize with commits and drops on the
    // same table.
    let source: Option<(String, String)> = sqlx::query_as(
        "SELECT t.id, t.namespace_id
         FROM tables t
         JOIN namespaces n ON n.id = t.namespace_id
         WHERE n.warehouse_id = $1 AND n.levels = $2 AND t.name = $3
         FOR UPDATE OF t",
    )
    .bind(warehouse_id)
    .bind(source_namespace)
    .bind(source_name)
    .fetch_optional(&mut *tx)
    .await
    .map_err(|e| map_sqlx_error("failed to load table for rename", e))?;

    let Some((table_id, source_namespace_id)) = source else {
        return Err(MeridianError::NotFound(format!(
            "table {:?} does not exist",
            format_ident(source_namespace, source_name)
        )));
    };

    // Resolve the destination namespace. FOR SHARE holds off a concurrent
    // namespace drop until the moved table is visible to its emptiness check.
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

    sqlx::query("UPDATE tables SET namespace_id = $1, name = $2, updated_at = now() WHERE id = $3")
        .bind(&destination_namespace_id)
        .bind(destination_name)
        .bind(&table_id)
        .execute(&mut *tx)
        .await
        .map_err(|e| {
            if is_unique_violation(&e) {
                MeridianError::Conflict(format!(
                    "table {:?} already exists",
                    format_ident(destination_namespace, destination_name)
                ))
            } else {
                map_sqlx_error("failed to rename table", e)
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
            aggregate: format!("table:{table_id}"),
            event_type: "table.renamed".to_owned(),
            payload: payload.clone(),
        },
    )
    .await?;

    audit::append_in_tx(
        &mut tx,
        NewAuditEntry {
            workspace_id: Some(workspace_id),
            principal: principal.to_owned(),
            action: "table.rename".to_owned(),
            resource: format!("table:{table_id}"),
            details: payload,
        },
    )
    .await?;

    tx.commit()
        .await
        .map_err(|e| map_sqlx_error("failed to commit table rename", e))?;

    Ok(())
}

/// Drops a table: deletes the pointer row (and, via `ON DELETE CASCADE`, its
/// snapshot index rows), with its audit row and outbox event, atomically.
///
/// When `purge_requested` is set, an additional `table.purge_requested`
/// outbox event carrying the table location is enqueued in the same
/// transaction for the maintenance worker. File deletion itself is the
/// caller's (best-effort) and the purge job's (guaranteed) concern — never
/// this transaction's.
///
/// Returns the dropped row so the caller can act on its locations.
pub async fn drop_table(
    pool: &PgPool,
    workspace_id: WorkspaceId,
    warehouse_id: &str,
    namespace_levels: &[String],
    name: &str,
    purge_requested: bool,
    principal: &str,
) -> Result<TableRecord> {
    let mut tx = pool
        .begin()
        .await
        .map_err(|e| map_sqlx_error("failed to begin table drop", e))?;

    let record: Option<TableRecord> = sqlx::query_as(&format!(
        "SELECT {SELECT_COLUMNS_T}
         FROM tables t
         JOIN namespaces n ON n.id = t.namespace_id
         WHERE n.warehouse_id = $1 AND n.levels = $2 AND t.name = $3
         FOR UPDATE OF t",
    ))
    .bind(warehouse_id)
    .bind(namespace_levels)
    .bind(name)
    .fetch_optional(&mut *tx)
    .await
    .map_err(|e| map_sqlx_error("failed to load table for drop", e))?;

    let Some(record) = record else {
        return Err(MeridianError::NotFound(format!(
            "table {:?} does not exist",
            format_ident(namespace_levels, name)
        )));
    };

    sqlx::query("DELETE FROM tables WHERE id = $1")
        .bind(&record.id)
        .execute(&mut *tx)
        .await
        .map_err(|e| map_sqlx_error("failed to delete table", e))?;

    let payload = json!({
        "namespace": namespace_levels,
        "name": name,
        "table_uuid": record.table_uuid,
        "metadata_location": record.metadata_location,
        "purge_requested": purge_requested,
    });
    outbox::enqueue(
        &mut *tx,
        &NewOutboxEvent {
            workspace_id: Some(workspace_id),
            aggregate: format!("table:{}", record.id),
            event_type: "table.dropped".to_owned(),
            payload: payload.clone(),
        },
    )
    .await?;

    if purge_requested {
        outbox::enqueue(
            &mut *tx,
            &NewOutboxEvent {
                workspace_id: Some(workspace_id),
                aggregate: format!("table:{}", record.id),
                event_type: "table.purge_requested".to_owned(),
                payload: payload.clone(),
            },
        )
        .await?;
    }

    audit::append_in_tx(
        &mut tx,
        NewAuditEntry {
            workspace_id: Some(workspace_id),
            principal: principal.to_owned(),
            action: "table.drop".to_owned(),
            resource: format!("table:{}", record.id),
            details: payload,
        },
    )
    .await?;

    tx.commit()
        .await
        .map_err(|e| map_sqlx_error("failed to commit table drop", e))?;

    Ok(record)
}

/// Stores a raw metrics report (`POST .../tables/{table}/metrics`).
///
/// The payload is stored verbatim; `table_ident` denormalizes the table
/// identity so reports survive a later drop.
pub async fn record_metrics_report(
    pool: &PgPool,
    workspace_id: WorkspaceId,
    table_id: &str,
    table_ident: &str,
    report_type: Option<&str>,
    report: &serde_json::Value,
) -> Result<String> {
    let id = Ulid::new().to_string();
    sqlx::query(
        "INSERT INTO metrics_reports (id, workspace_id, table_id, table_ident, report_type, report)
         VALUES ($1, $2, $3, $4, $5, $6)",
    )
    .bind(&id)
    .bind(workspace_id.to_string())
    .bind(table_id)
    .bind(table_ident)
    .bind(report_type)
    .bind(report)
    .execute(pool)
    .await
    .map_err(|e| map_sqlx_error("failed to record metrics report", e))?;
    Ok(id)
}

/// Renders a table identifier for human-readable messages.
fn format_ident(namespace: &[String], name: &str) -> String {
    if namespace.is_empty() {
        name.to_owned()
    } else {
        format!("{}.{name}", namespace.join("."))
    }
}
