//! Transactional outbox: enqueue, relay, and the queryable feed.
//!
//! State changes and the events describing them must commit atomically:
//! insert the event with [`enqueue`] on the *same* transaction as the state
//! change. The relay ([`relay_once`], driven by a background task in the
//! server) publishes unpublished rows in bounded batches: it claims rows
//! with `FOR UPDATE SKIP LOCKED` (so concurrent relays never double-publish
//! and never block each other), fans matching events out to webhook
//! delivery rows, and stamps `published_at` — all in one transaction, so a
//! crash mid-batch republishes the whole batch (at-least-once).
//!
//! # Ordering
//!
//! Events are claimed in id order (ids are ULIDs, so id order is creation
//! order). The claim excludes any event whose aggregate still has an
//! earlier unpublished event outside the current batch — typically one
//! claimed by a concurrent relay — so publication order **per aggregate**
//! is strict even with multiple relays.
//!
//! # The publication frontier
//!
//! The feed ([`list_published`]) only serves events below the *frontier*:
//! `MIN(id)` over unpublished rows. Rows claimed by an in-flight relay
//! transaction are still unpublished in every other snapshot, so a consumer
//! can never observe id N and later have id M < N appear behind its cursor.
//! Keyset pagination over the feed is therefore gap-free and totally
//! ordered by id.

use chrono::{DateTime, Utc};
use meridian_common::Result;
use meridian_common::id::WorkspaceId;
use serde_json::Value;
use sqlx::{PgExecutor, PgPool};
use ulid::Ulid;

use crate::{map_sqlx_error, webhook};

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

/// An outbox row as read back by the relay and the feed.
#[derive(Debug, Clone, sqlx::FromRow)]
pub struct OutboxRecord {
    /// ULID of the event; doubles as the feed cursor.
    pub id: String,
    /// Workspace the event belongs to; `None` for org-level events.
    pub workspace_id: Option<String>,
    /// Aggregate identity the event is about, e.g. `table:01J...`.
    pub aggregate: String,
    /// Event type as stored, e.g. `table.committed` (the `CloudEvents` type
    /// is this prefixed with `com.meridian.`).
    pub event_type: String,
    /// Event payload.
    pub payload: Value,
    /// When the event was enqueued.
    pub created_at: DateTime<Utc>,
}

/// Claims up to `limit` unpublished events on the caller's transaction,
/// oldest first, skipping rows locked by concurrent relays.
///
/// The `NOT EXISTS` guard drops any candidate whose aggregate still has an
/// earlier unpublished event that is *not* part of this claim (e.g. one
/// currently claimed by another relay's open transaction): publishing the
/// candidate now would invert that aggregate's order. Dropped candidates
/// are picked up by a later batch once the earlier event is published.
pub async fn claim_batch(
    tx: &mut sqlx::Transaction<'_, sqlx::Postgres>,
    limit: i64,
) -> Result<Vec<OutboxRecord>> {
    sqlx::query_as(
        "WITH candidate AS (
             SELECT id, workspace_id, aggregate, event_type, payload, created_at
             FROM events_outbox
             WHERE published_at IS NULL
             ORDER BY id
             LIMIT $1
             FOR UPDATE SKIP LOCKED
         )
         SELECT id, workspace_id, aggregate, event_type, payload, created_at
         FROM candidate c
         WHERE NOT EXISTS (
             SELECT 1 FROM events_outbox e
             WHERE e.aggregate = c.aggregate
               AND e.published_at IS NULL
               AND e.id < c.id
               AND e.id NOT IN (SELECT id FROM candidate)
         )
         ORDER BY id",
    )
    .bind(limit)
    .fetch_all(&mut **tx)
    .await
    .map_err(|e| map_sqlx_error("failed to claim outbox batch", e))
}

/// Stamps `published_at = now()` on the given rows (relay transaction).
pub async fn mark_published(
    tx: &mut sqlx::Transaction<'_, sqlx::Postgres>,
    ids: &[String],
) -> Result<()> {
    if ids.is_empty() {
        return Ok(());
    }
    sqlx::query("UPDATE events_outbox SET published_at = now() WHERE id = ANY($1)")
        .bind(ids)
        .execute(&mut **tx)
        .await
        .map_err(|e| map_sqlx_error("failed to mark outbox events published", e))?;
    Ok(())
}

/// One relay iteration: claim a batch, fan out webhook deliveries, mark the
/// batch published, commit. Returns the number of events published.
///
/// Crash-safe: everything happens on one transaction, so a failure at any
/// point republishes the batch on the next iteration (at-least-once). A
/// return value equal to `limit` means there is likely more backlog, so the
/// caller should loop again without sleeping (bounded batches drain a large
/// backlog without one giant claim).
pub async fn relay_once(pool: &PgPool, limit: i64) -> Result<usize> {
    let mut tx = pool
        .begin()
        .await
        .map_err(|e| map_sqlx_error("failed to begin relay transaction", e))?;

    let events = claim_batch(&mut tx, limit).await?;
    if events.is_empty() {
        // Nothing claimed; nothing to commit.
        return Ok(0);
    }

    webhook::enqueue_deliveries(&mut tx, &events).await?;

    let ids: Vec<String> = events.iter().map(|e| e.id.clone()).collect();
    mark_published(&mut tx, &ids).await?;

    tx.commit()
        .await
        .map_err(|e| map_sqlx_error("failed to commit relay transaction", e))?;

    Ok(events.len())
}

/// Lists published events after the exclusive cursor `after`, in id order,
/// bounded by the publication frontier (see the module docs). An empty
/// `after` starts from the beginning of the feed. `types` filters on the
/// stored event type (e.g. `table.committed`); `None` means all types.
pub async fn list_published(
    pool: &PgPool,
    after: &str,
    types: Option<&[String]>,
    limit: i64,
) -> Result<Vec<OutboxRecord>> {
    sqlx::query_as(
        "WITH frontier AS (
             SELECT MIN(id) AS f FROM events_outbox WHERE published_at IS NULL
         )
         SELECT e.id, e.workspace_id, e.aggregate, e.event_type, e.payload, e.created_at
         FROM events_outbox e, frontier
         WHERE e.published_at IS NOT NULL
           AND e.id > $1
           AND (frontier.f IS NULL OR e.id < frontier.f)
           AND ($2::text[] IS NULL OR e.event_type = ANY($2))
         ORDER BY e.id
         LIMIT $3",
    )
    .bind(after)
    .bind(types)
    .bind(limit)
    .fetch_all(pool)
    .await
    .map_err(|e| map_sqlx_error("failed to list published events", e))
}

/// The current end-of-feed cursor: the largest published id below the
/// publication frontier, or the empty cursor when the feed is empty.
///
/// Passing this as `after` to [`list_published`] yields only events
/// published from now on — nothing already visible is replayed.
pub async fn latest_cursor(pool: &PgPool) -> Result<String> {
    let cursor: Option<String> = sqlx::query_scalar(
        "WITH frontier AS (
             SELECT MIN(id) AS f FROM events_outbox WHERE published_at IS NULL
         )
         SELECT MAX(e.id)
         FROM events_outbox e, frontier
         WHERE e.published_at IS NOT NULL
           AND (frontier.f IS NULL OR e.id < frontier.f)",
    )
    .fetch_one(pool)
    .await
    .map_err(|e| map_sqlx_error("failed to read latest feed cursor", e))?;
    Ok(cursor.unwrap_or_default())
}

/// Counts unpublished (backlog) rows. Used for operator visibility and the
/// backlog-drain tests.
pub async fn unpublished_count(pool: &PgPool) -> Result<i64> {
    sqlx::query_scalar("SELECT COUNT(*) FROM events_outbox WHERE published_at IS NULL")
        .fetch_one(pool)
        .await
        .map_err(|e| map_sqlx_error("failed to count unpublished events", e))
}
