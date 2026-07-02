//! Transactional outbox.
//!
//! State changes and the events describing them must commit atomically:
//! insert the event with [`enqueue`] on the *same* transaction as the state
//! change. A relay process (M2) publishes unpublished rows and stamps
//! `published_at`.

use meridian_common::Result;
use meridian_common::id::WorkspaceId;
use serde_json::Value;
use sqlx::PgExecutor;
use ulid::Ulid;

use crate::map_sqlx_error;

/// A new event to enqueue.
#[derive(Debug, Clone)]
pub struct NewOutboxEvent {
    /// Workspace the event belongs to; `None` for org-level events.
    pub workspace_id: Option<WorkspaceId>,
    /// Aggregate identity the event is about, e.g. `table:01J...`.
    pub aggregate: String,
    /// Event type, e.g. `table.created`.
    pub event_type: String,
    /// Event payload. Shape is owned by the emitting module.
    pub payload: Value,
}

/// Inserts an event into the outbox and returns its generated ID (a ULID
/// string).
///
/// Accepts any [`PgExecutor`] so it can join the caller's transaction — for
/// commit-path atomicity, always pass the transaction that carries the state
/// change, never a bare pool.
pub async fn enqueue<'e, E>(executor: E, event: &NewOutboxEvent) -> Result<String>
where
    E: PgExecutor<'e>,
{
    let id = Ulid::new().to_string();

    sqlx::query(
        "INSERT INTO events_outbox (id, workspace_id, aggregate, event_type, payload)
         VALUES ($1, $2, $3, $4, $5)",
    )
    .bind(&id)
    .bind(event.workspace_id.map(|w| w.to_string()))
    .bind(&event.aggregate)
    .bind(&event.event_type)
    .bind(&event.payload)
    .execute(executor)
    .await
    .map_err(|e| map_sqlx_error("failed to enqueue outbox event", e))?;

    Ok(id)
}
