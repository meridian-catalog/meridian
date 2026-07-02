//! Management API: principal visibility.
//!
//! Principals are provisioned just-in-time by the authentication
//! middleware (see `crate::auth`); this read-only surface exists so
//! operators can see which identities have used the catalog. Listing
//! identities is identity enumeration, so it requires management access
//! (admin role or any `MANAGE_WAREHOUSE` grant), like the rest of the
//! RBAC management API.

use axum::extract::State;
use axum::{Extension, Json};
use chrono::{DateTime, Utc};
use meridian_common::principal::Principal;
use meridian_store::principal::{self, PrincipalRecord};
use meridian_store::tenancy;
use serde::Serialize;

use crate::AppState;
use crate::error::ApiError;
use crate::routes::grants::require_management;

/// A principal as rendered by the management API.
#[derive(Debug, Serialize)]
pub struct PrincipalResponse {
    /// ULID of the principal.
    pub id: String,
    /// Actor kind (`user`, `service`, `agent`).
    pub kind: String,
    /// Raw OIDC `sub` claim.
    pub subject: String,
    /// Token issuer URL.
    pub issuer: String,
    /// Display name carried by the credential, if any.
    pub display_name: Option<String>,
    /// When the principal was first seen.
    pub created_at: DateTime<Utc>,
}

impl From<PrincipalRecord> for PrincipalResponse {
    fn from(record: PrincipalRecord) -> Self {
        Self {
            id: record.id,
            kind: record.kind,
            subject: record.subject,
            issuer: record.issuer,
            display_name: record.display_name,
            created_at: record.created_at,
        }
    }
}

/// Response body for `GET /api/v2/principals`.
#[derive(Debug, Serialize)]
pub struct ListPrincipalsResponse {
    /// All principals in the workspace, oldest first.
    pub principals: Vec<PrincipalResponse>,
}

/// `GET /api/v2/principals` — list the principals that have been seen.
/// Requires management access.
pub async fn list_principals(
    State(state): State<AppState>,
    Extension(principal): Extension<Principal>,
) -> Result<Json<ListPrincipalsResponse>, ApiError> {
    require_management(&state.pool, &principal).await?;
    let principals = principal::list(&state.pool, tenancy::default_workspace_id())
        .await?
        .into_iter()
        .map(PrincipalResponse::from)
        .collect();
    Ok(Json(ListPrincipalsResponse { principals }))
}
