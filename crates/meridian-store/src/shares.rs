//! Cross-org data sharing persistence (Pillar J, J-F1).
//!
//! Owns the two tables migration 0023 introduces:
//!
//! - `shares`: a scoped projection of catalog assets to an *external*
//!   recipient org, addressed by an opaque `token` (the recipient's bearer
//!   secret and IRC path prefix). Carries optional terms-of-use, the
//!   acceptance timestamp, and a revocation flag.
//! - `share_grants`: the projection contents — one securable (table, view, or
//!   certified data product) per row, with an optional row filter and column
//!   mask applied to recipient reads.
//!
//! # What this module is (and is not)
//!
//! Pure persistence. It does **not** vend credentials, resolve the recipient's
//! IRC responses, or serve HTTP — that is the server's `routes::shares` (both
//! the management API and the token-authenticated recipient endpoint). Every
//! *management* mutation carries its audit row and outbox event on the *same*
//! transaction — the invariant the whole codebase holds: no mutation without
//! its audit row. Recipient reads are plain pooled queries; the recipient
//! endpoint writes its own access-audit rows on the server side.
//!
//! Revocation is a flag, not a delete, so the share's history is retained.
//! Because the recipient only ever holds short-lived vended credentials,
//! setting `revoked` takes effect immediately (no new vend, existing creds
//! expire on their TTL) — there is no long-lived key to claw back.

use std::str::FromStr;

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

/// True when the error is a Postgres unique-constraint violation.
fn is_unique_violation(error: &sqlx::Error) -> bool {
    error
        .as_database_error()
        .is_some_and(sqlx::error::DatabaseError::is_unique_violation)
}

/// True when the error is a Postgres foreign-key violation.
fn is_fk_violation(error: &sqlx::Error) -> bool {
    error
        .as_database_error()
        .is_some_and(sqlx::error::DatabaseError::is_foreign_key_violation)
}

// ===========================================================================
// Shares (J-F1)
// ===========================================================================

/// A persisted cross-org share.
#[derive(Debug, Clone, sqlx::FromRow)]
pub struct ShareRecord {
    /// ULID of the share.
    pub id: String,
    /// Owning workspace.
    pub workspace_id: String,
    /// Machine name, unique per workspace (case-insensitively).
    pub name: String,
    /// External recipient identifier (audit string).
    pub recipient: String,
    /// The opaque bearer secret / IRC path prefix the recipient presents.
    pub token: String,
    /// Human-readable terms of use; `None` means no acceptance gate.
    pub terms: Option<String>,
    /// When the recipient accepted `terms`; `None` until accepted.
    pub terms_accepted_at: Option<DateTime<Utc>>,
    /// The workspace principal who created the share (audit string).
    pub created_by: String,
    /// Whether the share is revoked (serves nothing when true).
    pub revoked: bool,
    /// When the share was revoked; `None` while active.
    pub revoked_at: Option<DateTime<Utc>>,
    /// Creation time.
    pub created_at: DateTime<Utc>,
    /// Last update time.
    pub updated_at: DateTime<Utc>,
}

impl ShareRecord {
    /// True when the share requires terms acceptance that has not happened yet.
    #[must_use]
    pub fn needs_terms_acceptance(&self) -> bool {
        self.terms.is_some() && self.terms_accepted_at.is_none()
    }

    /// True when the recipient endpoint may currently serve data: active and
    /// (if it has terms) accepted.
    #[must_use]
    pub fn is_servable(&self) -> bool {
        !self.revoked && !self.needs_terms_acceptance()
    }
}

const SHARE_COLUMNS: &str = "id, workspace_id, name, recipient, token, terms, terms_accepted_at, \
     created_by, revoked, revoked_at, created_at, updated_at";

/// A new share to insert.
#[derive(Debug, Clone)]
pub struct NewShare<'a> {
    /// Machine name (unique per workspace, case-insensitively).
    pub name: &'a str,
    /// External recipient identifier (audit string).
    pub recipient: &'a str,
    /// The opaque bearer secret / path prefix (caller generates it).
    pub token: &'a str,
    /// Optional terms of use.
    pub terms: Option<&'a str>,
}

/// One securable projected by a share (`kind` + stable `ref`, + optional
/// policy).
#[derive(Debug, Clone, sqlx::FromRow)]
pub struct ShareGrantRecord {
    /// ULID of the grant row.
    pub id: String,
    /// The owning share.
    pub share_id: String,
    /// Securable kind (`table` | `view` | `data_product`).
    pub securable_kind: String,
    /// Stable securable reference.
    pub securable_ref: String,
    /// Optional row filter (a boolean SQL predicate).
    pub row_filter: Option<String>,
    /// Optional column mask (an array of column names to hide).
    pub column_mask: Option<Json<Vec<String>>>,
    /// Creation time.
    pub created_at: DateTime<Utc>,
}

const GRANT_COLUMNS: &str =
    "id, share_id, securable_kind, securable_ref, row_filter, column_mask, created_at";

/// Loads a share by id.
pub async fn get_share(pool: &PgPool, id: &str) -> Result<Option<ShareRecord>> {
    sqlx::query_as(&format!("SELECT {SHARE_COLUMNS} FROM shares WHERE id = $1"))
        .bind(id)
        .fetch_optional(pool)
        .await
        .map_err(|e| map_sqlx_error("failed to load share", e))
}

/// Loads a share by its opaque token (the recipient-endpoint resolution path).
///
/// The token is unique across all shares, so this is a single-row lookup. It
/// deliberately does *not* filter on `revoked` — the recipient endpoint loads
/// the share and then decides how to answer (a revoked share still resolves so
/// the endpoint can return a clear 403 rather than an ambiguous 404).
pub async fn get_share_by_token(pool: &PgPool, token: &str) -> Result<Option<ShareRecord>> {
    sqlx::query_as(&format!(
        "SELECT {SHARE_COLUMNS} FROM shares WHERE token = $1"
    ))
    .bind(token)
    .fetch_optional(pool)
    .await
    .map_err(|e| map_sqlx_error("failed to load share by token", e))
}

/// Lists a workspace's shares in stable id (creation) order, keyset-paginated.
pub async fn list_shares(
    pool: &PgPool,
    workspace_id: WorkspaceId,
    after_id: Option<&str>,
    limit: Option<i64>,
) -> Result<Vec<ShareRecord>> {
    sqlx::query_as(&format!(
        "SELECT {SHARE_COLUMNS} FROM shares
         WHERE workspace_id = $1 AND ($2::text IS NULL OR id > $2)
         ORDER BY id LIMIT $3"
    ))
    .bind(workspace_id.to_string())
    .bind(after_id)
    .bind(limit)
    .fetch_all(pool)
    .await
    .map_err(|e| map_sqlx_error("failed to list shares", e))
}

/// Lists the grants of a share in stable id order.
pub async fn list_share_grants(pool: &PgPool, share_id: &str) -> Result<Vec<ShareGrantRecord>> {
    sqlx::query_as(&format!(
        "SELECT {GRANT_COLUMNS} FROM share_grants WHERE share_id = $1 ORDER BY id"
    ))
    .bind(share_id)
    .fetch_all(pool)
    .await
    .map_err(|e| map_sqlx_error("failed to list share grants", e))
}

/// Loads a single grant of a share by (kind, ref) — the recipient endpoint's
/// per-securable lookup (does this share include this table?).
pub async fn get_share_grant(
    pool: &PgPool,
    share_id: &str,
    securable_kind: &str,
    securable_ref: &str,
) -> Result<Option<ShareGrantRecord>> {
    sqlx::query_as(&format!(
        "SELECT {GRANT_COLUMNS} FROM share_grants
         WHERE share_id = $1 AND securable_kind = $2 AND securable_ref = $3"
    ))
    .bind(share_id)
    .bind(securable_kind)
    .bind(securable_ref)
    .fetch_optional(pool)
    .await
    .map_err(|e| map_sqlx_error("failed to load share grant", e))
}

/// Inserts a share, with its audit row and outbox event, atomically.
///
/// Returns [`MeridianError::Conflict`] when the name (in the workspace) or the
/// token (globally) is already taken.
pub async fn create_share(
    pool: &PgPool,
    workspace_id: WorkspaceId,
    share: NewShare<'_>,
    principal: &str,
) -> Result<ShareRecord> {
    let mut tx = pool
        .begin()
        .await
        .map_err(|e| map_sqlx_error("failed to begin share create", e))?;

    let id = Ulid::new().to_string();
    let record: ShareRecord = sqlx::query_as(&format!(
        "INSERT INTO shares (id, workspace_id, name, recipient, token, terms, created_by)
         VALUES ($1, $2, $3, $4, $5, $6, $7)
         RETURNING {SHARE_COLUMNS}"
    ))
    .bind(&id)
    .bind(workspace_id.to_string())
    .bind(share.name)
    .bind(share.recipient)
    .bind(share.token)
    .bind(share.terms)
    .bind(principal)
    .fetch_one(&mut *tx)
    .await
    .map_err(|e| {
        if is_unique_violation(&e) {
            MeridianError::Conflict(format!(
                "a share named {:?} already exists in this workspace (or the token collides)",
                share.name
            ))
        } else {
            map_sqlx_error("failed to insert share", e)
        }
    })?;

    // The audit/event payload never carries the token — it is a bearer secret.
    write_audit_and_event(
        &mut tx,
        workspace_id,
        principal,
        "share.create",
        &format!("share:{id}"),
        "share.created",
        json!({ "name": record.name, "recipient": record.recipient }),
    )
    .await?;

    tx.commit()
        .await
        .map_err(|e| map_sqlx_error("failed to commit share create", e))?;
    Ok(record)
}

/// Revokes a share (idempotent), with audit + outbox. Returns the share.
///
/// Revoking an already-revoked share is a no-op success (the row is returned
/// unchanged and no second audit row is written). Returns
/// [`MeridianError::NotFound`] when no such share exists.
pub async fn revoke_share(
    pool: &PgPool,
    workspace_id: WorkspaceId,
    id: &str,
    principal: &str,
) -> Result<ShareRecord> {
    let mut tx = pool
        .begin()
        .await
        .map_err(|e| map_sqlx_error("failed to begin share revoke", e))?;

    // Only flips an active share; a revoked share matches the WHERE guard and
    // returns nothing, so we distinguish "already revoked" from "not found".
    let flipped: Option<ShareRecord> = sqlx::query_as(&format!(
        "UPDATE shares SET revoked = TRUE, revoked_at = now(), updated_at = now()
         WHERE id = $1 AND workspace_id = $2 AND revoked = FALSE
         RETURNING {SHARE_COLUMNS}"
    ))
    .bind(id)
    .bind(workspace_id.to_string())
    .fetch_optional(&mut *tx)
    .await
    .map_err(|e| map_sqlx_error("failed to revoke share", e))?;

    let record = if let Some(record) = flipped {
        write_audit_and_event(
            &mut tx,
            workspace_id,
            principal,
            "share.revoke",
            &format!("share:{id}"),
            "share.revoked",
            json!({ "name": record.name, "recipient": record.recipient }),
        )
        .await?;
        record
    } else {
        // Either not found, or already revoked (idempotent no-op). Reload
        // within the same transaction to tell the two apart.
        let existing: Option<ShareRecord> = sqlx::query_as(&format!(
            "SELECT {SHARE_COLUMNS} FROM shares WHERE id = $1 AND workspace_id = $2"
        ))
        .bind(id)
        .bind(workspace_id.to_string())
        .fetch_optional(&mut *tx)
        .await
        .map_err(|e| map_sqlx_error("failed to reload share", e))?;
        existing.ok_or_else(|| MeridianError::NotFound(format!("share {id:?} does not exist")))?
    };

    tx.commit()
        .await
        .map_err(|e| map_sqlx_error("failed to commit share revoke", e))?;
    Ok(record)
}

/// Deletes a share (and its grants, by cascade), with audit + outbox. Returns
/// the dropped row. Prefer [`revoke_share`] to retain history; delete is for
/// operator cleanup.
pub async fn delete_share(
    pool: &PgPool,
    workspace_id: WorkspaceId,
    id: &str,
    principal: &str,
) -> Result<ShareRecord> {
    let mut tx = pool
        .begin()
        .await
        .map_err(|e| map_sqlx_error("failed to begin share delete", e))?;

    let record: Option<ShareRecord> = sqlx::query_as(&format!(
        "DELETE FROM shares WHERE id = $1 AND workspace_id = $2 RETURNING {SHARE_COLUMNS}"
    ))
    .bind(id)
    .bind(workspace_id.to_string())
    .fetch_optional(&mut *tx)
    .await
    .map_err(|e| map_sqlx_error("failed to delete share", e))?;

    let Some(record) = record else {
        return Err(MeridianError::NotFound(format!(
            "share {id:?} does not exist"
        )));
    };

    write_audit_and_event(
        &mut tx,
        workspace_id,
        principal,
        "share.delete",
        &format!("share:{id}"),
        "share.deleted",
        json!({ "name": record.name }),
    )
    .await?;

    tx.commit()
        .await
        .map_err(|e| map_sqlx_error("failed to commit share delete", e))?;
    Ok(record)
}

/// Records the recipient's acceptance of the share's terms (idempotent).
///
/// Sets `terms_accepted_at` on first acceptance and writes an audit row
/// attributed to the recipient. A second acceptance is a no-op success. This is
/// called from the recipient endpoint (the recipient is the principal), so the
/// audit `principal` is the recipient identifier, not a workspace user.
///
/// Returns [`MeridianError::NotFound`] when no such share exists, and
/// [`MeridianError::Validation`] when the share carries no terms to accept.
pub async fn accept_terms(pool: &PgPool, share_id: &str) -> Result<ShareRecord> {
    let mut tx = pool
        .begin()
        .await
        .map_err(|e| map_sqlx_error("failed to begin terms acceptance", e))?;

    let share: Option<ShareRecord> =
        sqlx::query_as(&format!("SELECT {SHARE_COLUMNS} FROM shares WHERE id = $1"))
            .bind(share_id)
            .fetch_one(&mut *tx)
            .await
            .map(Some)
            .or_else(|e| match e {
                sqlx::Error::RowNotFound => Ok(None),
                other => Err(map_sqlx_error(
                    "failed to load share for terms acceptance",
                    other,
                )),
            })?;

    let Some(share) = share else {
        return Err(MeridianError::NotFound(format!(
            "share {share_id:?} does not exist"
        )));
    };
    if share.terms.is_none() {
        return Err(MeridianError::Validation(
            "this share has no terms to accept".to_owned(),
        ));
    }
    if share.terms_accepted_at.is_some() {
        // Already accepted; idempotent no-op.
        tx.commit()
            .await
            .map_err(|e| map_sqlx_error("failed to commit terms acceptance", e))?;
        return Ok(share);
    }

    let workspace_id = WorkspaceId::from_str(&share.workspace_id)
        .map_err(|e| MeridianError::internal("share row has an invalid workspace_id", e))?;

    let record: ShareRecord = sqlx::query_as(&format!(
        "UPDATE shares SET terms_accepted_at = now(), updated_at = now()
         WHERE id = $1 RETURNING {SHARE_COLUMNS}"
    ))
    .bind(share_id)
    .fetch_one(&mut *tx)
    .await
    .map_err(|e| map_sqlx_error("failed to record terms acceptance", e))?;

    // Attributed to the recipient — this is a recipient action, not a
    // workspace-user mutation.
    write_audit_and_event(
        &mut tx,
        workspace_id,
        &format!("recipient:{}", record.recipient),
        "share.accept_terms",
        &format!("share:{share_id}"),
        "share.terms_accepted",
        json!({ "name": record.name, "recipient": record.recipient }),
    )
    .await?;

    tx.commit()
        .await
        .map_err(|e| map_sqlx_error("failed to commit terms acceptance", e))?;
    Ok(record)
}

/// Adds a securable to a share (idempotent), with audit + outbox.
///
/// Returns the existing grant when the (share, kind, ref) triple already
/// exists. Returns [`MeridianError::NotFound`] when the share does not exist.
#[allow(clippy::too_many_arguments)] // a grant is a securable + its two policy slots + audit ctx
pub async fn add_grant(
    pool: &PgPool,
    workspace_id: WorkspaceId,
    share_id: &str,
    securable_kind: &str,
    securable_ref: &str,
    row_filter: Option<&str>,
    column_mask: Option<&[String]>,
    principal: &str,
) -> Result<ShareGrantRecord> {
    let mut tx = pool
        .begin()
        .await
        .map_err(|e| map_sqlx_error("failed to begin share grant", e))?;

    let id = Ulid::new().to_string();
    let inserted: Option<ShareGrantRecord> = sqlx::query_as(&format!(
        "INSERT INTO share_grants
             (id, workspace_id, share_id, securable_kind, securable_ref, row_filter, column_mask)
         VALUES ($1, $2, $3, $4, $5, $6, $7)
         ON CONFLICT (share_id, securable_kind, securable_ref) DO NOTHING
         RETURNING {GRANT_COLUMNS}"
    ))
    .bind(&id)
    .bind(workspace_id.to_string())
    .bind(share_id)
    .bind(securable_kind)
    .bind(securable_ref)
    .bind(row_filter)
    .bind(column_mask.map(|c| Json(c.to_vec())))
    .fetch_optional(&mut *tx)
    .await
    .map_err(|e| {
        if is_fk_violation(&e) {
            MeridianError::NotFound(format!("share {share_id:?} does not exist"))
        } else {
            map_sqlx_error("failed to add share grant", e)
        }
    })?;

    let (record, newly_added) = if let Some(record) = inserted {
        (record, true)
    } else {
        let existing: ShareGrantRecord = sqlx::query_as(&format!(
            "SELECT {GRANT_COLUMNS} FROM share_grants
             WHERE share_id = $1 AND securable_kind = $2 AND securable_ref = $3"
        ))
        .bind(share_id)
        .bind(securable_kind)
        .bind(securable_ref)
        .fetch_one(&mut *tx)
        .await
        .map_err(|e| map_sqlx_error("failed to load existing share grant", e))?;
        (existing, false)
    };

    if newly_added {
        write_audit_and_event(
            &mut tx,
            workspace_id,
            principal,
            "share.add_grant",
            &format!("share:{share_id}"),
            "share.grant_added",
            json!({
                "securable_kind": securable_kind,
                "securable_ref": securable_ref,
                "has_row_filter": row_filter.is_some(),
                "has_column_mask": column_mask.is_some(),
            }),
        )
        .await?;
    }

    tx.commit()
        .await
        .map_err(|e| map_sqlx_error("failed to commit share grant", e))?;
    Ok(record)
}

/// Removes a grant from a share by grant-row id, with audit + outbox. Returns
/// the removed row.
pub async fn remove_grant(
    pool: &PgPool,
    workspace_id: WorkspaceId,
    grant_id: &str,
    principal: &str,
) -> Result<ShareGrantRecord> {
    let mut tx = pool
        .begin()
        .await
        .map_err(|e| map_sqlx_error("failed to begin remove share grant", e))?;

    let record: Option<ShareGrantRecord> = sqlx::query_as(&format!(
        "DELETE FROM share_grants WHERE id = $1 AND workspace_id = $2 RETURNING {GRANT_COLUMNS}"
    ))
    .bind(grant_id)
    .bind(workspace_id.to_string())
    .fetch_optional(&mut *tx)
    .await
    .map_err(|e| map_sqlx_error("failed to remove share grant", e))?;

    let Some(record) = record else {
        return Err(MeridianError::NotFound(format!(
            "share grant {grant_id:?} does not exist"
        )));
    };

    write_audit_and_event(
        &mut tx,
        workspace_id,
        principal,
        "share.remove_grant",
        &format!("share:{}", record.share_id),
        "share.grant_removed",
        json!({
            "securable_kind": record.securable_kind,
            "securable_ref": record.securable_ref,
        }),
    )
    .await?;

    tx.commit()
        .await
        .map_err(|e| map_sqlx_error("failed to commit remove share grant", e))?;
    Ok(record)
}

// ===========================================================================
// Shared: audit + outbox on the mutation transaction (invariant I6)
// ===========================================================================

/// Writes the outbox event and the audit row for a mutation on the *same*
/// transaction as the state change. Every management mutation in this module
/// routes its audit+event through here so the invariant is enforced in one
/// place.
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

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn servable_reflects_revoked_and_terms() {
        let base = ShareRecord {
            id: "s".to_owned(),
            workspace_id: "w".to_owned(),
            name: "n".to_owned(),
            recipient: "org:acme".to_owned(),
            token: "tok".to_owned(),
            terms: None,
            terms_accepted_at: None,
            created_by: "user:me".to_owned(),
            revoked: false,
            revoked_at: None,
            created_at: Utc::now(),
            updated_at: Utc::now(),
        };
        assert!(base.is_servable(), "no terms, active => servable");

        let mut with_terms = base.clone();
        with_terms.terms = Some("be nice".to_owned());
        assert!(with_terms.needs_terms_acceptance());
        assert!(
            !with_terms.is_servable(),
            "unaccepted terms => not servable"
        );
        with_terms.terms_accepted_at = Some(Utc::now());
        assert!(with_terms.is_servable(), "accepted terms => servable");

        let mut revoked = base.clone();
        revoked.revoked = true;
        revoked.revoked_at = Some(Utc::now());
        assert!(!revoked.is_servable(), "revoked => not servable");
    }
}
