//! Named durable consumers of the event feed.
//!
//! A consumer is a persistent cursor over the published feed. Reads via
//! `GET .../next` do not advance the cursor — the client acknowledges a
//! batch explicitly with `POST .../commit {cursor}`, so processing is
//! at-least-once: a client that crashes between read and commit sees the
//! same batch again. This mirrors Kafka-style consumer groups (without
//! partitioned parallelism — one cursor per name).
//!
//! Cursor commits are deliberately **not** audited and emit **no** outbox
//! event: an offset advance is consumption bookkeeping, not a catalog
//! mutation — and emitting an event per commit would make any
//! subscribed-to-everything consumer feed itself forever. Consumer
//! create/delete are catalog mutations and are audited as usual.

use chrono::{DateTime, Utc};
use meridian_common::id::WorkspaceId;
use meridian_common::{MeridianError, Result};
use serde_json::json;
use sqlx::PgPool;

use crate::audit::{self, NewAuditEntry};
use crate::map_sqlx_error;
use crate::outbox::{self, NewOutboxEvent};

/// A durable consumer as stored.
#[derive(Debug, Clone, sqlx::FromRow)]
pub struct ConsumerRecord {
    /// Consumer name, unique per workspace.
    pub name: String,
    /// Last committed cursor (exclusive lower bound for the next read);
    /// `None` means the consumer starts at the beginning of the feed.
    pub cursor: Option<String>,
    /// Creation time.
    pub created_at: DateTime<Utc>,
    /// Last cursor commit (or creation).
    pub updated_at: DateTime<Utc>,
}

/// True when the error is a Postgres unique-constraint violation.
fn is_unique_violation(error: &sqlx::Error) -> bool {
    error
        .as_database_error()
        .is_some_and(sqlx::error::DatabaseError::is_unique_violation)
}

/// Creates a consumer starting at the beginning of the feed (audit +
/// outbox on the same transaction).
pub async fn create(
    pool: &PgPool,
    workspace_id: WorkspaceId,
    name: &str,
    principal: &str,
) -> Result<ConsumerRecord> {
    let mut tx = pool
        .begin()
        .await
        .map_err(|e| map_sqlx_error("failed to begin consumer create", e))?;

    let record: ConsumerRecord = sqlx::query_as(
        "INSERT INTO event_consumers (workspace_id, name)
         VALUES ($1, $2)
         RETURNING name, cursor, created_at, updated_at",
    )
    .bind(workspace_id.to_string())
    .bind(name)
    .fetch_one(&mut *tx)
    .await
    .map_err(|e| {
        if is_unique_violation(&e) {
            MeridianError::Conflict(format!("consumer {name:?} already exists"))
        } else {
            map_sqlx_error("failed to insert consumer", e)
        }
    })?;

    let details = json!({ "name": name });
    outbox::enqueue(
        &mut *tx,
        &NewOutboxEvent {
            workspace_id: Some(workspace_id),
            aggregate: format!("event_consumer:{name}"),
            event_type: "event_consumer.created".to_owned(),
            payload: details.clone(),
        },
    )
    .await?;
    audit::append_in_tx(
        &mut tx,
        NewAuditEntry {
            workspace_id: Some(workspace_id),
            principal: principal.to_owned(),
            action: "event_consumer.create".to_owned(),
            resource: format!("event_consumer:{name}"),
            details,
        },
    )
    .await?;

    tx.commit()
        .await
        .map_err(|e| map_sqlx_error("failed to commit consumer create", e))?;

    Ok(record)
}

/// Lists a workspace's consumers by name.
pub async fn list(pool: &PgPool, workspace_id: WorkspaceId) -> Result<Vec<ConsumerRecord>> {
    sqlx::query_as(
        "SELECT name, cursor, created_at, updated_at
         FROM event_consumers
         WHERE workspace_id = $1
         ORDER BY name",
    )
    .bind(workspace_id.to_string())
    .fetch_all(pool)
    .await
    .map_err(|e| map_sqlx_error("failed to list consumers", e))
}

/// Loads one consumer by name.
pub async fn get(
    pool: &PgPool,
    workspace_id: WorkspaceId,
    name: &str,
) -> Result<Option<ConsumerRecord>> {
    sqlx::query_as(
        "SELECT name, cursor, created_at, updated_at
         FROM event_consumers
         WHERE workspace_id = $1 AND name = $2",
    )
    .bind(workspace_id.to_string())
    .bind(name)
    .fetch_optional(pool)
    .await
    .map_err(|e| map_sqlx_error("failed to load consumer", e))
}

/// Commits a consumer's cursor. The new cursor must not move backwards
/// (equal is allowed, making commits idempotent); a regression is a
/// [`MeridianError::Conflict`]. Returns the updated record, or `NotFound`.
pub async fn commit_cursor(
    pool: &PgPool,
    workspace_id: WorkspaceId,
    name: &str,
    cursor: &str,
) -> Result<ConsumerRecord> {
    // ULIDs compare lexicographically, so the monotonicity check is a
    // plain string comparison in SQL.
    let updated: Option<ConsumerRecord> = sqlx::query_as(
        "UPDATE event_consumers
         SET cursor = $3, updated_at = now()
         WHERE workspace_id = $1 AND name = $2
           AND (cursor IS NULL OR cursor <= $3)
         RETURNING name, cursor, created_at, updated_at",
    )
    .bind(workspace_id.to_string())
    .bind(name)
    .bind(cursor)
    .fetch_optional(pool)
    .await
    .map_err(|e| map_sqlx_error("failed to commit consumer cursor", e))?;

    if let Some(record) = updated {
        return Ok(record);
    }
    // Distinguish "no such consumer" from "cursor would regress".
    match get(pool, workspace_id, name).await? {
        Some(record) => Err(MeridianError::Conflict(format!(
            "cursor {cursor:?} is behind the committed cursor {:?}",
            record.cursor.unwrap_or_default()
        ))),
        None => Err(MeridianError::NotFound(format!(
            "consumer {name:?} does not exist"
        ))),
    }
}

/// Deletes a consumer (audit + outbox on the same transaction).
pub async fn delete(
    pool: &PgPool,
    workspace_id: WorkspaceId,
    name: &str,
    principal: &str,
) -> Result<()> {
    let mut tx = pool
        .begin()
        .await
        .map_err(|e| map_sqlx_error("failed to begin consumer delete", e))?;

    let deleted: Option<String> = sqlx::query_scalar(
        "DELETE FROM event_consumers WHERE workspace_id = $1 AND name = $2 RETURNING name",
    )
    .bind(workspace_id.to_string())
    .bind(name)
    .fetch_optional(&mut *tx)
    .await
    .map_err(|e| map_sqlx_error("failed to delete consumer", e))?;
    if deleted.is_none() {
        return Err(MeridianError::NotFound(format!(
            "consumer {name:?} does not exist"
        )));
    }

    let details = json!({ "name": name });
    outbox::enqueue(
        &mut *tx,
        &NewOutboxEvent {
            workspace_id: Some(workspace_id),
            aggregate: format!("event_consumer:{name}"),
            event_type: "event_consumer.deleted".to_owned(),
            payload: details.clone(),
        },
    )
    .await?;
    audit::append_in_tx(
        &mut tx,
        NewAuditEntry {
            workspace_id: Some(workspace_id),
            principal: principal.to_owned(),
            action: "event_consumer.delete".to_owned(),
            resource: format!("event_consumer:{name}"),
            details,
        },
    )
    .await?;

    tx.commit()
        .await
        .map_err(|e| map_sqlx_error("failed to commit consumer delete", e))
}
