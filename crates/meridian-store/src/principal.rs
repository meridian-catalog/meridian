//! Principal identity rows, provisioned just-in-time.
//!
//! An authenticated caller is identified externally by `(issuer, subject)`.
//! The first authenticated request provisions a local `principals` row so
//! audit history and (future) grants have a stable, workspace-scoped
//! identity to reference. Provisioning is race-safe: concurrent first
//! requests produce exactly one row — and exactly one audit entry and
//! outbox event, written on the inserting transaction.

use chrono::{DateTime, Utc};
use meridian_common::id::WorkspaceId;
use meridian_common::principal::{Principal, PrincipalKind};
use meridian_common::{MeridianError, Result};
use serde_json::json;
use sqlx::PgPool;
use ulid::Ulid;

use crate::audit::{self, NewAuditEntry};
use crate::map_sqlx_error;
use crate::outbox::{self, NewOutboxEvent};

/// A persisted principal row.
#[derive(Debug, Clone, sqlx::FromRow)]
pub struct PrincipalRecord {
    /// ULID of the principal.
    pub id: String,
    /// Owning workspace.
    pub workspace_id: String,
    /// Actor kind (`user`, `service`, `agent`, `anonymous`).
    pub kind: String,
    /// Raw OIDC `sub` claim.
    pub subject: String,
    /// Token issuer URL.
    pub issuer: String,
    /// Display name carried by the credential, if any.
    pub display_name: Option<String>,
    /// When the row was provisioned.
    pub created_at: DateTime<Utc>,
}

/// The database rendering of a [`PrincipalKind`] (matches the CHECK
/// constraint in migration 0004 and the serde `snake_case` names).
#[must_use]
pub fn kind_str(kind: PrincipalKind) -> &'static str {
    match kind {
        PrincipalKind::User => "user",
        PrincipalKind::Service => "service",
        PrincipalKind::Agent => "agent",
        PrincipalKind::Anonymous => "anonymous",
    }
}

const SELECT_COLUMNS: &str = "id, workspace_id, kind, subject, issuer, display_name, created_at";

/// Looks a principal up by its external identity.
pub async fn get_by_identity(
    pool: &PgPool,
    issuer: &str,
    subject: &str,
) -> Result<Option<PrincipalRecord>> {
    sqlx::query_as(&format!(
        "SELECT {SELECT_COLUMNS} FROM principals WHERE issuer = $1 AND subject = $2"
    ))
    .bind(issuer)
    .bind(subject)
    .fetch_optional(pool)
    .await
    .map_err(|e| map_sqlx_error("failed to look up principal", e))
}

/// Lists all principals of a workspace, oldest first.
pub async fn list(pool: &PgPool, workspace_id: WorkspaceId) -> Result<Vec<PrincipalRecord>> {
    sqlx::query_as(&format!(
        "SELECT {SELECT_COLUMNS} FROM principals WHERE workspace_id = $1 ORDER BY id"
    ))
    .bind(workspace_id.to_string())
    .fetch_all(pool)
    .await
    .map_err(|e| map_sqlx_error("failed to list principals", e))
}

/// Ensures a row exists for an authenticated principal, provisioning it on
/// first sight (JIT). Returns the (existing or new) record.
///
/// Race-safe: `INSERT ... ON CONFLICT DO NOTHING` decides a single winner;
/// losers read the winner's committed row. The winner's transaction also
/// carries the `principal.provision` audit entry and outbox event, so a
/// concurrent first request never double-audits.
pub async fn ensure(
    pool: &PgPool,
    workspace_id: WorkspaceId,
    principal: &Principal,
) -> Result<PrincipalRecord> {
    let Some(issuer) = principal.issuer.as_deref() else {
        // Anonymous principals are a deployment policy, not an identity;
        // they are never materialized as rows.
        return Err(MeridianError::Validation(
            "cannot provision a principal without an issuer".to_owned(),
        ));
    };

    // Fast path: already provisioned.
    if let Some(existing) = get_by_identity(pool, issuer, &principal.subject).await? {
        return Ok(existing);
    }

    let mut tx = pool
        .begin()
        .await
        .map_err(|e| map_sqlx_error("failed to begin principal provisioning", e))?;

    let id = Ulid::new().to_string();
    let inserted: Option<PrincipalRecord> = sqlx::query_as(&format!(
        "INSERT INTO principals (id, workspace_id, kind, subject, issuer, display_name)
         VALUES ($1, $2, $3, $4, $5, $6)
         ON CONFLICT (issuer, subject) DO NOTHING
         RETURNING {SELECT_COLUMNS}"
    ))
    .bind(&id)
    .bind(workspace_id.to_string())
    .bind(kind_str(principal.kind))
    .bind(&principal.subject)
    .bind(issuer)
    .bind(&principal.display_name)
    .fetch_optional(&mut *tx)
    .await
    .map_err(|e| map_sqlx_error("failed to insert principal", e))?;

    let Some(record) = inserted else {
        // Lost the provisioning race; the winner's row is committed (or
        // about to be). Nothing was written on this transaction.
        drop(tx);
        return get_by_identity(pool, issuer, &principal.subject)
            .await?
            .ok_or_else(|| {
                MeridianError::internal_msg(
                    "principal insert conflicted but no row is visible; \
                     was the row deleted concurrently?",
                )
            });
    };

    let details = json!({
        "kind": kind_str(principal.kind),
        "subject": principal.subject,
        "issuer": issuer,
        "display_name": principal.display_name,
    });

    outbox::enqueue(
        &mut *tx,
        &NewOutboxEvent {
            workspace_id: Some(workspace_id),
            aggregate: format!("principal:{id}"),
            event_type: "principal.provisioned".to_owned(),
            payload: details.clone(),
        },
    )
    .await?;

    audit::append_in_tx(
        &mut tx,
        NewAuditEntry {
            workspace_id: Some(workspace_id),
            principal: principal.audit_string(),
            action: "principal.provision".to_owned(),
            resource: format!("principal:{id}"),
            details,
        },
    )
    .await?;

    tx.commit()
        .await
        .map_err(|e| map_sqlx_error("failed to commit principal provisioning", e))?;

    Ok(record)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn kind_strings_match_serde_renaming() {
        for kind in [
            PrincipalKind::User,
            PrincipalKind::Service,
            PrincipalKind::Agent,
            PrincipalKind::Anonymous,
        ] {
            let via_serde = serde_json::to_value(kind).expect("serialize kind");
            assert_eq!(via_serde, serde_json::json!(kind_str(kind)));
        }
    }
}
