//! Webhook endpoints and durable delivery tracking.
//!
//! An endpoint subscribes a URL to published events (optionally filtered by
//! event type). The outbox relay fans each published event out to one
//! `webhook_deliveries` row per matching endpoint — in the *same*
//! transaction that marks the event published — and the webhook dispatcher
//! drives each delivery to `delivered` or `dead` with exponential backoff.
//! Delivery rows are durable, so attempts survive restarts and delivery is
//! at-least-once.
//!
//! Dispatch claims use a lease: claiming a due delivery bumps its
//! `next_attempt_at` into the future before the HTTP attempt, so a
//! dispatcher crash mid-attempt just means the delivery becomes due again
//! after the lease expires (another at-least-once path, never lost).

use chrono::{DateTime, Utc};
use meridian_common::id::WorkspaceId;
use meridian_common::{MeridianError, Result};
use serde_json::json;
use sqlx::PgPool;
use ulid::Ulid;

use crate::audit::{self, NewAuditEntry};
use crate::map_sqlx_error;
use crate::outbox::{self, NewOutboxEvent, OutboxRecord};

/// True when the error is a Postgres unique-constraint violation.
fn is_unique_violation(error: &sqlx::Error) -> bool {
    error
        .as_database_error()
        .is_some_and(sqlx::error::DatabaseError::is_unique_violation)
}

/// A webhook endpoint as stored. The signing secret is deliberately *not*
/// part of this record: it is write-only through the API surface and only
/// the dispatcher reads it (via [`claim_due_deliveries`]).
#[derive(Debug, Clone, sqlx::FromRow)]
pub struct WebhookEndpointRecord {
    /// ULID of the endpoint.
    pub id: String,
    /// Destination URL.
    pub url: String,
    /// Full `CloudEvents` type filter (e.g. `com.meridian.table.committed`);
    /// empty subscribes to all events.
    pub event_types: Vec<String>,
    /// Creation time.
    pub created_at: DateTime<Utc>,
    /// Last update time.
    pub updated_at: DateTime<Utc>,
}

/// Creates a webhook endpoint (audit + outbox on the same transaction).
/// `event_types` are full `CloudEvents` type strings.
pub async fn create_endpoint(
    pool: &PgPool,
    workspace_id: WorkspaceId,
    url: &str,
    event_types: &[String],
    secret: &str,
    principal: &str,
) -> Result<WebhookEndpointRecord> {
    let mut tx = pool
        .begin()
        .await
        .map_err(|e| map_sqlx_error("failed to begin webhook create", e))?;

    let id = Ulid::new().to_string();
    let record: WebhookEndpointRecord = sqlx::query_as(
        "INSERT INTO webhook_endpoints (id, workspace_id, url, event_types, secret)
         VALUES ($1, $2, $3, $4, $5)
         RETURNING id, url, event_types, created_at, updated_at",
    )
    .bind(&id)
    .bind(workspace_id.to_string())
    .bind(url)
    .bind(event_types)
    .bind(secret)
    .fetch_one(&mut *tx)
    .await
    .map_err(|e| {
        if is_unique_violation(&e) {
            MeridianError::Conflict(format!("a webhook for url {url:?} already exists"))
        } else {
            map_sqlx_error("failed to insert webhook endpoint", e)
        }
    })?;

    // The secret never enters the event payload or the audit log.
    let details = json!({ "url": url, "event_types": event_types });
    outbox::enqueue(
        &mut *tx,
        &NewOutboxEvent {
            workspace_id: Some(workspace_id),
            aggregate: format!("webhook:{id}"),
            event_type: "webhook.created".to_owned(),
            payload: details.clone(),
        },
    )
    .await?;
    audit::append_in_tx(
        &mut tx,
        NewAuditEntry {
            workspace_id: Some(workspace_id),
            principal: principal.to_owned(),
            action: "webhook.create".to_owned(),
            resource: format!("webhook:{id}"),
            details,
        },
    )
    .await?;

    tx.commit()
        .await
        .map_err(|e| map_sqlx_error("failed to commit webhook create", e))?;

    Ok(record)
}

/// Lists a workspace's webhook endpoints, oldest first.
pub async fn list_endpoints(
    pool: &PgPool,
    workspace_id: WorkspaceId,
) -> Result<Vec<WebhookEndpointRecord>> {
    sqlx::query_as(
        "SELECT id, url, event_types, created_at, updated_at
         FROM webhook_endpoints
         WHERE workspace_id = $1
         ORDER BY id",
    )
    .bind(workspace_id.to_string())
    .fetch_all(pool)
    .await
    .map_err(|e| map_sqlx_error("failed to list webhook endpoints", e))
}

/// Loads one webhook endpoint by id.
pub async fn get_endpoint(
    pool: &PgPool,
    workspace_id: WorkspaceId,
    id: &str,
) -> Result<Option<WebhookEndpointRecord>> {
    sqlx::query_as(
        "SELECT id, url, event_types, created_at, updated_at
         FROM webhook_endpoints
         WHERE workspace_id = $1 AND id = $2",
    )
    .bind(workspace_id.to_string())
    .bind(id)
    .fetch_optional(pool)
    .await
    .map_err(|e| map_sqlx_error("failed to load webhook endpoint", e))
}

/// Deletes a webhook endpoint (its deliveries cascade). Audit + outbox on
/// the same transaction.
pub async fn delete_endpoint(
    pool: &PgPool,
    workspace_id: WorkspaceId,
    id: &str,
    principal: &str,
) -> Result<()> {
    let mut tx = pool
        .begin()
        .await
        .map_err(|e| map_sqlx_error("failed to begin webhook delete", e))?;

    let url: Option<String> = sqlx::query_scalar(
        "DELETE FROM webhook_endpoints WHERE workspace_id = $1 AND id = $2 RETURNING url",
    )
    .bind(workspace_id.to_string())
    .bind(id)
    .fetch_optional(&mut *tx)
    .await
    .map_err(|e| map_sqlx_error("failed to delete webhook endpoint", e))?;
    let Some(url) = url else {
        return Err(MeridianError::NotFound(format!(
            "webhook {id:?} does not exist"
        )));
    };

    let details = json!({ "url": url });
    outbox::enqueue(
        &mut *tx,
        &NewOutboxEvent {
            workspace_id: Some(workspace_id),
            aggregate: format!("webhook:{id}"),
            event_type: "webhook.deleted".to_owned(),
            payload: details.clone(),
        },
    )
    .await?;
    audit::append_in_tx(
        &mut tx,
        NewAuditEntry {
            workspace_id: Some(workspace_id),
            principal: principal.to_owned(),
            action: "webhook.delete".to_owned(),
            resource: format!("webhook:{id}"),
            details,
        },
    )
    .await?;

    tx.commit()
        .await
        .map_err(|e| map_sqlx_error("failed to commit webhook delete", e))
}

/// Fans a batch of just-published events out to delivery rows: one per
/// (matching endpoint, event). Runs on the relay's transaction, so
/// deliveries exist if and only if the events are marked published.
///
/// Matching: an endpoint with an empty `event_types` receives everything;
/// otherwise the event's full `CloudEvents` type (`com.meridian.` +
/// stored type) must be in the list. `ON CONFLICT DO NOTHING` keeps a
/// republished batch (crash replay) idempotent.
pub async fn enqueue_deliveries(
    tx: &mut sqlx::Transaction<'_, sqlx::Postgres>,
    events: &[OutboxRecord],
) -> Result<()> {
    if events.is_empty() {
        return Ok(());
    }
    let ids: Vec<String> = events.iter().map(|e| e.id.clone()).collect();
    sqlx::query(
        "INSERT INTO webhook_deliveries (endpoint_id, event_id)
         SELECT w.id, e.id
         FROM events_outbox e
         JOIN webhook_endpoints w
           ON (e.workspace_id IS NULL OR e.workspace_id = w.workspace_id)
          AND (cardinality(w.event_types) = 0
               OR 'com.meridian.' || e.event_type = ANY(w.event_types))
         WHERE e.id = ANY($1)
         ON CONFLICT (endpoint_id, event_id) DO NOTHING",
    )
    .bind(&ids)
    .execute(&mut **tx)
    .await
    .map_err(|e| map_sqlx_error("failed to enqueue webhook deliveries", e))?;
    Ok(())
}

/// A claimed delivery attempt, joined with everything the dispatcher needs
/// to build and sign the HTTP request.
#[derive(Debug, Clone, sqlx::FromRow)]
pub struct DueDelivery {
    /// Endpoint being delivered to.
    pub endpoint_id: String,
    /// Event being delivered.
    pub event_id: String,
    /// Attempt count *including* the one just claimed.
    pub attempts: i32,
    /// Destination URL.
    pub url: String,
    /// HMAC-SHA256 signing key.
    pub secret: String,
    /// Event fields for `CloudEvents` rendering.
    pub workspace_id: Option<String>,
    /// Aggregate the event is about.
    pub aggregate: String,
    /// Stored event type (short form).
    pub event_type: String,
    /// Event payload.
    pub payload: serde_json::Value,
    /// When the event was enqueued.
    pub created_at: DateTime<Utc>,
}

/// Claims up to `limit` due pending deliveries, oldest event first,
/// skipping rows claimed by concurrent dispatchers.
///
/// Claiming increments `attempts` and pushes `next_attempt_at` to
/// `lease_until`, so the HTTP attempt happens *outside* any transaction: a
/// crash mid-attempt leaves the row pending and due again after the lease.
pub async fn claim_due_deliveries(
    pool: &PgPool,
    limit: i64,
    lease_until: DateTime<Utc>,
) -> Result<Vec<DueDelivery>> {
    sqlx::query_as(
        "WITH claimed AS (
             SELECT endpoint_id, event_id
             FROM webhook_deliveries
             WHERE status = 'pending' AND next_attempt_at <= now()
             ORDER BY event_id
             LIMIT $1
             FOR UPDATE SKIP LOCKED
         )
         UPDATE webhook_deliveries d
         SET attempts = d.attempts + 1, next_attempt_at = $2, updated_at = now()
         FROM claimed c
         JOIN webhook_endpoints w ON w.id = c.endpoint_id
         JOIN events_outbox e ON e.id = c.event_id
         WHERE d.endpoint_id = c.endpoint_id AND d.event_id = c.event_id
         RETURNING d.endpoint_id, d.event_id, d.attempts,
                   w.url, w.secret,
                   e.workspace_id, e.aggregate, e.event_type, e.payload, e.created_at",
    )
    .bind(limit)
    .bind(lease_until)
    .fetch_all(pool)
    .await
    .map_err(|e| map_sqlx_error("failed to claim webhook deliveries", e))
}

/// Records a successful delivery attempt (2xx response).
pub async fn record_delivery_success(
    pool: &PgPool,
    endpoint_id: &str,
    event_id: &str,
    http_status: i16,
) -> Result<()> {
    sqlx::query(
        "UPDATE webhook_deliveries
         SET status = 'delivered', last_status = $3, last_error = NULL, updated_at = now()
         WHERE endpoint_id = $1 AND event_id = $2",
    )
    .bind(endpoint_id)
    .bind(event_id)
    .bind(http_status)
    .execute(pool)
    .await
    .map_err(|e| map_sqlx_error("failed to record webhook delivery success", e))?;
    Ok(())
}

/// Records a failed delivery attempt. When `next_attempt_at` is `Some` the
/// delivery stays pending and retries then; `None` dead-letters it.
pub async fn record_delivery_failure(
    pool: &PgPool,
    endpoint_id: &str,
    event_id: &str,
    http_status: Option<i16>,
    error: &str,
    next_attempt_at: Option<DateTime<Utc>>,
) -> Result<()> {
    sqlx::query(
        "UPDATE webhook_deliveries
         SET status = CASE WHEN $5::timestamptz IS NULL THEN 'dead' ELSE 'pending' END,
             last_status = $3,
             last_error = $4,
             next_attempt_at = COALESCE($5, next_attempt_at),
             updated_at = now()
         WHERE endpoint_id = $1 AND event_id = $2",
    )
    .bind(endpoint_id)
    .bind(event_id)
    .bind(http_status)
    .bind(error)
    .bind(next_attempt_at)
    .execute(pool)
    .await
    .map_err(|e| map_sqlx_error("failed to record webhook delivery failure", e))?;
    Ok(())
}

/// A delivery as rendered for the management API (dead-letter visibility).
#[derive(Debug, Clone, sqlx::FromRow)]
pub struct DeliveryRecord {
    /// Event delivered (the outbox/feed id).
    pub event_id: String,
    /// Full `CloudEvents` event type.
    pub event_type: String,
    /// `pending`, `delivered`, or `dead`.
    pub status: String,
    /// Attempts made so far.
    pub attempts: i32,
    /// HTTP status of the most recent attempt, if a response was received.
    pub last_status: Option<i16>,
    /// Error detail of the most recent failed attempt.
    pub last_error: Option<String>,
    /// Next scheduled attempt (meaningful while `pending`).
    pub next_attempt_at: DateTime<Utc>,
    /// Last state change.
    pub updated_at: DateTime<Utc>,
}

/// Lists an endpoint's deliveries, newest event first, optionally filtered
/// by status.
pub async fn list_deliveries(
    pool: &PgPool,
    endpoint_id: &str,
    status: Option<&str>,
    limit: i64,
) -> Result<Vec<DeliveryRecord>> {
    sqlx::query_as(
        "SELECT d.event_id, 'com.meridian.' || e.event_type AS event_type,
                d.status, d.attempts, d.last_status, d.last_error,
                d.next_attempt_at, d.updated_at
         FROM webhook_deliveries d
         JOIN events_outbox e ON e.id = d.event_id
         WHERE d.endpoint_id = $1
           AND ($2::text IS NULL OR d.status = $2)
         ORDER BY d.event_id DESC
         LIMIT $3",
    )
    .bind(endpoint_id)
    .bind(status)
    .bind(limit)
    .fetch_all(pool)
    .await
    .map_err(|e| map_sqlx_error("failed to list webhook deliveries", e))
}
