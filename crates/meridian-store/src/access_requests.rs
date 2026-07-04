//! Request-access persistence (Pillar D, D-F4; reused by the Pillar J, J-F2
//! internal marketplace).
//!
//! Owns the `access_requests` table (migration 0016): a principal's request
//! for one privilege on one securable, with a stated purpose and an optional
//! requested TTL. It moves `pending -> approved | denied | expired`; the
//! decision (decider, reason, time) is recorded on the same row.
//!
//! The internal marketplace (J-F2) is the certified-data-product gallery
//! (Pillar G) with a "request access" button on each product's member assets —
//! that button creates one of these rows. This module holds the object model
//! and the audited create/decide transitions; the routing/approval workflow
//! beyond a manual decide is a later wave.
//!
//! Every mutation carries its audit row and outbox event on the *same*
//! transaction — the invariant the whole codebase holds. Reads are plain
//! pooled queries.

use chrono::{DateTime, Utc};
use meridian_common::id::WorkspaceId;
use meridian_common::{MeridianError, Result};
use serde_json::{Value, json};
use sqlx::PgPool;
use ulid::Ulid;

use crate::audit::{self, NewAuditEntry};
use crate::map_sqlx_error;
use crate::outbox::{self, NewOutboxEvent};

/// A persisted access request (D-F4).
#[derive(Debug, Clone, sqlx::FromRow)]
pub struct AccessRequestRecord {
    /// ULID of the request.
    pub id: String,
    /// Owning workspace.
    pub workspace_id: String,
    /// Audit string of the requesting principal.
    pub principal: String,
    /// Securable kind (`warehouse` | `namespace` | `table` | `view`).
    pub securable_type: String,
    /// Stable securable reference.
    pub securable_id: String,
    /// The privilege requested (RBAC wire form, e.g. `READ`).
    pub privilege: String,
    /// Free-text declared purpose.
    pub purpose: String,
    /// Requested grant lifetime in seconds; `None` for no expiry.
    pub ttl_seconds: Option<i64>,
    /// State (`pending` | `approved` | `denied` | `expired`).
    pub state: String,
    /// Who decided (set when leaving `pending`).
    pub decided_by: Option<String>,
    /// Decision reason.
    pub reason: Option<String>,
    /// When the decision was made.
    pub decided_at: Option<DateTime<Utc>>,
    /// Creation time.
    pub created_at: DateTime<Utc>,
}

const COLUMNS: &str = "id, workspace_id, principal, securable_type, securable_id, privilege, \
     purpose, ttl_seconds, state, decided_by, reason, decided_at, created_at";

/// A new access request (always created `pending`).
#[derive(Debug, Clone)]
pub struct NewAccessRequest<'a> {
    /// Securable kind.
    pub securable_type: &'a str,
    /// Stable securable reference.
    pub securable_id: &'a str,
    /// Requested privilege.
    pub privilege: &'a str,
    /// Declared purpose.
    pub purpose: &'a str,
    /// Requested TTL in seconds (optional).
    pub ttl_seconds: Option<i64>,
}

/// Loads an access request by id.
pub async fn get(pool: &PgPool, id: &str) -> Result<Option<AccessRequestRecord>> {
    sqlx::query_as(&format!(
        "SELECT {COLUMNS} FROM access_requests WHERE id = $1"
    ))
    .bind(id)
    .fetch_optional(pool)
    .await
    .map_err(|e| map_sqlx_error("failed to load access request", e))
}

/// Lists a workspace's access requests, newest-first, optionally filtered by
/// state, keyset-paginated on id (descending).
pub async fn list(
    pool: &PgPool,
    workspace_id: WorkspaceId,
    state: Option<&str>,
    before_id: Option<&str>,
    limit: Option<i64>,
) -> Result<Vec<AccessRequestRecord>> {
    sqlx::query_as(&format!(
        "SELECT {COLUMNS} FROM access_requests
         WHERE workspace_id = $1
           AND ($2::text IS NULL OR state = $2)
           AND ($3::text IS NULL OR id < $3)
         ORDER BY id DESC LIMIT $4"
    ))
    .bind(workspace_id.to_string())
    .bind(state)
    .bind(before_id)
    .bind(limit)
    .fetch_all(pool)
    .await
    .map_err(|e| map_sqlx_error("failed to list access requests", e))
}

/// Creates a `pending` access request, with audit + outbox, atomically.
pub async fn create(
    pool: &PgPool,
    workspace_id: WorkspaceId,
    request: NewAccessRequest<'_>,
    principal: &str,
) -> Result<AccessRequestRecord> {
    let mut tx = pool
        .begin()
        .await
        .map_err(|e| map_sqlx_error("failed to begin access-request create", e))?;

    let id = Ulid::new().to_string();
    let record: AccessRequestRecord = sqlx::query_as(&format!(
        "INSERT INTO access_requests
             (id, workspace_id, principal, securable_type, securable_id, privilege, purpose,
              ttl_seconds)
         VALUES ($1, $2, $3, $4, $5, $6, $7, $8)
         RETURNING {COLUMNS}"
    ))
    .bind(&id)
    .bind(workspace_id.to_string())
    .bind(principal)
    .bind(request.securable_type)
    .bind(request.securable_id)
    .bind(request.privilege)
    .bind(request.purpose)
    .bind(request.ttl_seconds)
    .fetch_one(&mut *tx)
    .await
    .map_err(|e| map_sqlx_error("failed to insert access request", e))?;

    write_audit_and_event(
        &mut tx,
        workspace_id,
        principal,
        "access_request.create",
        &format!("access_request:{id}"),
        "access_request.created",
        json!({
            "securable_type": record.securable_type,
            "securable_id": record.securable_id,
            "privilege": record.privilege,
        }),
    )
    .await?;

    tx.commit()
        .await
        .map_err(|e| map_sqlx_error("failed to commit access-request create", e))?;
    Ok(record)
}

/// Records a decision (`approved` or `denied`) on a pending request, with
/// audit + outbox.
///
/// Returns [`MeridianError::NotFound`] when no such request exists, and
/// [`MeridianError::Conflict`] when it is not `pending` (already decided).
/// Actually provisioning the grant on approval is out of scope here (the D-F4
/// workflow wave); this records the decision on the request object.
pub async fn decide(
    pool: &PgPool,
    workspace_id: WorkspaceId,
    id: &str,
    approve: bool,
    reason: Option<&str>,
    decider: &str,
) -> Result<AccessRequestRecord> {
    let new_state = if approve { "approved" } else { "denied" };

    let mut tx = pool
        .begin()
        .await
        .map_err(|e| map_sqlx_error("failed to begin access-request decide", e))?;

    let updated: Option<AccessRequestRecord> = sqlx::query_as(&format!(
        "UPDATE access_requests SET
             state = $3, decided_by = $4, reason = $5, decided_at = now()
         WHERE id = $1 AND workspace_id = $2 AND state = 'pending'
         RETURNING {COLUMNS}"
    ))
    .bind(id)
    .bind(workspace_id.to_string())
    .bind(new_state)
    .bind(decider)
    .bind(reason)
    .fetch_optional(&mut *tx)
    .await
    .map_err(|e| map_sqlx_error("failed to decide access request", e))?;

    let Some(record) = updated else {
        // Not found, or not pending — tell them apart.
        let existing: Option<AccessRequestRecord> = sqlx::query_as(&format!(
            "SELECT {COLUMNS} FROM access_requests WHERE id = $1 AND workspace_id = $2"
        ))
        .bind(id)
        .bind(workspace_id.to_string())
        .fetch_optional(&mut *tx)
        .await
        .map_err(|e| map_sqlx_error("failed to reload access request", e))?;
        return match existing {
            Some(r) => Err(MeridianError::Conflict(format!(
                "access request {id:?} is already {} and cannot be decided again",
                r.state
            ))),
            None => Err(MeridianError::NotFound(format!(
                "access request {id:?} does not exist"
            ))),
        };
    };

    write_audit_and_event(
        &mut tx,
        workspace_id,
        decider,
        "access_request.decide",
        &format!("access_request:{id}"),
        "access_request.decided",
        json!({ "state": new_state, "requester": record.principal }),
    )
    .await?;

    tx.commit()
        .await
        .map_err(|e| map_sqlx_error("failed to commit access-request decide", e))?;
    Ok(record)
}

/// Writes the outbox event and audit row on the mutation transaction.
async fn write_audit_and_event(
    tx: &mut sqlx::Transaction<'_, sqlx::Postgres>,
    workspace_id: WorkspaceId,
    principal: &str,
    action: &str,
    resource: &str,
    event_type: &str,
    payload: Value,
) -> Result<()> {
    outbox::enqueue(
        &mut **tx,
        &NewOutboxEvent {
            workspace_id: Some(workspace_id),
            aggregate: resource.to_owned(),
            event_type: event_type.to_owned(),
            payload: payload.clone(),
        },
    )
    .await?;
    audit::append_in_tx(
        tx,
        NewAuditEntry {
            workspace_id: Some(workspace_id),
            principal: principal.to_owned(),
            action: action.to_owned(),
            resource: resource.to_owned(),
            details: payload,
        },
    )
    .await?;
    Ok(())
}
