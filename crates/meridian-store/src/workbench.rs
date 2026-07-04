//! SQL workbench persistence (Pillar L, L-F1): saved queries and query history.
//!
//! Owns the two tables migration 0022 introduces:
//!
//! - `workbench_saved_queries`: named, reusable queries a user parks for later.
//!   Create/delete are workspace mutations and are audited + outboxed on the same
//!   transaction, exactly like webhooks and consumers (the invariant the whole
//!   codebase holds: no mutation without its audit row).
//! - `workbench_query_history`: an append-only, per-principal recent-query log.
//!   Recording a history row is deliberately **not** audited and emits **no**
//!   outbox event — it *is* a log, and a human's ad-hoc SELECT is not a catalog
//!   mutation (the same rationale by which `consumer` cursor commits are not
//!   audited). History is a convenience the user can prune; the tamper-evident
//!   chain is for governed agent actions and catalog mutations.
//!
//! # What this module is (and is not)
//!
//! Pure persistence. It does **not** run SQL, resolve governance, or serve HTTP
//! (the workbench route + the `meridian-query` executor do). Reads are plain
//! pooled queries.

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

// ---------------------------------------------------------------------------
// Saved queries
// ---------------------------------------------------------------------------

/// A named, reusable workbench query.
#[derive(Debug, Clone, sqlx::FromRow)]
pub struct SavedQuery {
    /// Stable id (ULID).
    pub id: String,
    /// Query name (unique per workspace, case-insensitively).
    pub name: String,
    /// The SQL to run.
    pub sql: String,
    /// The warehouse the query targets (a name resolved at run time), if any.
    pub warehouse: Option<String>,
    /// Default namespace levels for resolving bare table names.
    pub default_namespace: Json<Vec<String>>,
    /// Free-text description.
    pub description: Option<String>,
    /// Owning principal (audit string).
    pub owner: String,
    /// Creation time.
    pub created_at: DateTime<Utc>,
    /// Last update time.
    pub updated_at: DateTime<Utc>,
}

/// The fields to create/update a saved query.
#[derive(Debug, Clone)]
pub struct NewSavedQuery<'a> {
    /// Query name.
    pub name: &'a str,
    /// The SQL.
    pub sql: &'a str,
    /// Target warehouse name, if any.
    pub warehouse: Option<&'a str>,
    /// Default namespace levels for bare table names.
    pub default_namespace: &'a [String],
    /// Free-text description.
    pub description: Option<&'a str>,
}

const SAVED_COLUMNS: &str = "id, name, sql, warehouse, default_namespace, description, owner, \
     created_at, updated_at";

/// Creates a saved query (audit + outbox on the same transaction).
///
/// Returns [`MeridianError::Conflict`] when the name already exists in the
/// workspace (case-insensitively).
pub async fn create_saved_query(
    pool: &PgPool,
    workspace_id: WorkspaceId,
    query: &NewSavedQuery<'_>,
    principal: &str,
) -> Result<SavedQuery> {
    let mut tx = pool
        .begin()
        .await
        .map_err(|e| map_sqlx_error("failed to begin saved-query create", e))?;

    let id = Ulid::new().to_string();
    let record: SavedQuery = sqlx::query_as(&format!(
        "INSERT INTO workbench_saved_queries
             (id, workspace_id, name, sql, warehouse, default_namespace, description, owner)
         VALUES ($1, $2, $3, $4, $5, $6, $7, $8)
         RETURNING {SAVED_COLUMNS}"
    ))
    .bind(&id)
    .bind(workspace_id.to_string())
    .bind(query.name)
    .bind(query.sql)
    .bind(query.warehouse)
    .bind(Json(query.default_namespace.to_vec()))
    .bind(query.description)
    .bind(principal)
    .fetch_one(&mut *tx)
    .await
    .map_err(|e| {
        if is_unique_violation(&e) {
            MeridianError::Conflict(format!(
                "a saved query named {:?} already exists",
                query.name
            ))
        } else {
            map_sqlx_error("failed to insert saved query", e)
        }
    })?;

    let details = json!({ "name": query.name, "warehouse": query.warehouse });
    outbox::enqueue(
        &mut *tx,
        &NewOutboxEvent {
            workspace_id: Some(workspace_id),
            aggregate: format!("workbench_saved_query:{id}"),
            event_type: "workbench.saved_query.created".to_owned(),
            payload: details.clone(),
        },
    )
    .await?;
    audit::append_in_tx(
        &mut tx,
        NewAuditEntry {
            workspace_id: Some(workspace_id),
            principal: principal.to_owned(),
            action: "workbench.saved_query.create".to_owned(),
            resource: format!("workbench_saved_query:{id}"),
            details,
        },
    )
    .await?;

    tx.commit()
        .await
        .map_err(|e| map_sqlx_error("failed to commit saved-query create", e))?;
    Ok(record)
}

/// Lists a workspace's saved queries, newest first.
pub async fn list_saved_queries(
    pool: &PgPool,
    workspace_id: WorkspaceId,
) -> Result<Vec<SavedQuery>> {
    sqlx::query_as(&format!(
        "SELECT {SAVED_COLUMNS} FROM workbench_saved_queries
         WHERE workspace_id = $1 ORDER BY id DESC"
    ))
    .bind(workspace_id.to_string())
    .fetch_all(pool)
    .await
    .map_err(|e| map_sqlx_error("failed to list saved queries", e))
}

/// Loads one saved query by id.
pub async fn get_saved_query(
    pool: &PgPool,
    workspace_id: WorkspaceId,
    id: &str,
) -> Result<Option<SavedQuery>> {
    sqlx::query_as(&format!(
        "SELECT {SAVED_COLUMNS} FROM workbench_saved_queries
         WHERE workspace_id = $1 AND id = $2"
    ))
    .bind(workspace_id.to_string())
    .bind(id)
    .fetch_optional(pool)
    .await
    .map_err(|e| map_sqlx_error("failed to load saved query", e))
}

/// Deletes a saved query (audit + outbox on the same transaction). Returns
/// `false` when no such query exists in the workspace.
pub async fn delete_saved_query(
    pool: &PgPool,
    workspace_id: WorkspaceId,
    id: &str,
    principal: &str,
) -> Result<bool> {
    let mut tx = pool
        .begin()
        .await
        .map_err(|e| map_sqlx_error("failed to begin saved-query delete", e))?;

    let name: Option<String> = sqlx::query_scalar(
        "DELETE FROM workbench_saved_queries WHERE workspace_id = $1 AND id = $2 RETURNING name",
    )
    .bind(workspace_id.to_string())
    .bind(id)
    .fetch_optional(&mut *tx)
    .await
    .map_err(|e| map_sqlx_error("failed to delete saved query", e))?;

    let Some(name) = name else {
        // Nothing deleted: no audit row for a no-op. Roll back the empty tx.
        tx.rollback()
            .await
            .map_err(|e| map_sqlx_error("failed to roll back saved-query delete", e))?;
        return Ok(false);
    };

    let details = json!({ "name": name });
    outbox::enqueue(
        &mut *tx,
        &NewOutboxEvent {
            workspace_id: Some(workspace_id),
            aggregate: format!("workbench_saved_query:{id}"),
            event_type: "workbench.saved_query.deleted".to_owned(),
            payload: details.clone(),
        },
    )
    .await?;
    audit::append_in_tx(
        &mut tx,
        NewAuditEntry {
            workspace_id: Some(workspace_id),
            principal: principal.to_owned(),
            action: "workbench.saved_query.delete".to_owned(),
            resource: format!("workbench_saved_query:{id}"),
            details,
        },
    )
    .await?;

    tx.commit()
        .await
        .map_err(|e| map_sqlx_error("failed to commit saved-query delete", e))?;
    Ok(true)
}

// ---------------------------------------------------------------------------
// Query history
// ---------------------------------------------------------------------------

/// The outcome recorded for a workbench query run.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum HistoryStatus {
    /// The query ran and returned rows.
    Ok,
    /// The query was rejected (bad/oversized SQL) or hit an engine fault.
    Error,
    /// The query was denied by policy.
    Denied,
}

impl HistoryStatus {
    /// The DB/wire string.
    #[must_use]
    pub fn as_str(self) -> &'static str {
        match self {
            Self::Ok => "ok",
            Self::Error => "error",
            Self::Denied => "denied",
        }
    }
}

/// One recorded workbench query run.
#[derive(Debug, Clone, sqlx::FromRow)]
pub struct HistoryEntry {
    /// Stable id (ULID, time-ordered).
    pub id: String,
    /// The principal who ran it (audit string).
    pub principal: String,
    /// The SQL.
    pub sql: String,
    /// The warehouse targeted, if any.
    pub warehouse: Option<String>,
    /// Outcome: `ok` | `error` | `denied`.
    pub status: String,
    /// Rows returned (for an `ok` run).
    pub row_count: Option<i64>,
    /// Bytes scanned (for an `ok` run).
    pub bytes_scanned: Option<i64>,
    /// Wall-clock duration in milliseconds.
    pub duration_ms: Option<i64>,
    /// Error/denial message for a non-`ok` run.
    pub message: Option<String>,
    /// When it ran.
    pub created_at: DateTime<Utc>,
}

/// The fields to record one history row.
#[derive(Debug, Clone)]
pub struct NewHistory<'a> {
    /// The SQL that ran.
    pub sql: &'a str,
    /// The warehouse targeted, if any.
    pub warehouse: Option<&'a str>,
    /// The outcome.
    pub status: HistoryStatus,
    /// Rows returned (for an `ok` run).
    pub row_count: Option<i64>,
    /// Bytes scanned (for an `ok` run).
    pub bytes_scanned: Option<i64>,
    /// Duration in milliseconds.
    pub duration_ms: Option<i64>,
    /// Error/denial message for a non-`ok` run.
    pub message: Option<&'a str>,
}

const HISTORY_COLUMNS: &str = "id, principal, sql, warehouse, status, row_count, bytes_scanned, \
     duration_ms, message, created_at";

/// Records one workbench query run in the per-principal history.
///
/// Deliberately **not** audited and emits **no** outbox event: this *is* the log
/// (see the module docs). A failure to record history must never fail the query
/// the user already ran, so callers log-and-ignore the error rather than
/// surfacing it.
pub async fn record_history(
    pool: &PgPool,
    workspace_id: WorkspaceId,
    principal: &str,
    entry: &NewHistory<'_>,
) -> Result<HistoryEntry> {
    let id = Ulid::new().to_string();
    sqlx::query_as(&format!(
        "INSERT INTO workbench_query_history
             (id, workspace_id, principal, sql, warehouse, status, row_count, bytes_scanned,
              duration_ms, message)
         VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10)
         RETURNING {HISTORY_COLUMNS}"
    ))
    .bind(&id)
    .bind(workspace_id.to_string())
    .bind(principal)
    .bind(entry.sql)
    .bind(entry.warehouse)
    .bind(entry.status.as_str())
    .bind(entry.row_count)
    .bind(entry.bytes_scanned)
    .bind(entry.duration_ms)
    .bind(entry.message)
    .fetch_one(pool)
    .await
    .map_err(|e| map_sqlx_error("failed to record query history", e))
}

/// Lists a principal's recent workbench queries, newest first.
///
/// `limit` bounds the page; pass the last row's `id` as `before_id` to page
/// further back (keyset pagination on the descending ULID).
pub async fn list_history(
    pool: &PgPool,
    workspace_id: WorkspaceId,
    principal: &str,
    before_id: Option<&str>,
    limit: i64,
) -> Result<Vec<HistoryEntry>> {
    sqlx::query_as(&format!(
        "SELECT {HISTORY_COLUMNS} FROM workbench_query_history
         WHERE workspace_id = $1 AND principal = $2
           AND ($3::text IS NULL OR id < $3)
         ORDER BY id DESC
         LIMIT $4"
    ))
    .bind(workspace_id.to_string())
    .bind(principal)
    .bind(before_id)
    .bind(limit)
    .fetch_all(pool)
    .await
    .map_err(|e| map_sqlx_error("failed to list query history", e))
}

/// True when the error is a Postgres unique-constraint violation.
fn is_unique_violation(error: &sqlx::Error) -> bool {
    error
        .as_database_error()
        .is_some_and(sqlx::error::DatabaseError::is_unique_violation)
}
