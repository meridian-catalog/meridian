//! Cross-org data sharing (Pillar J, J-F1) — the neutral Delta-Sharing
//! alternative, built from primitives Meridian already has: credential vending
//! (`meridian-vending`) + row/column policy + audit.
//!
//! This module has **two distinct surfaces**:
//!
//! 1. **Management API** (`/api/v2/shares...`, `/api/v2/marketplace...`) —
//!    workspace-side, authenticated by the normal OIDC middleware and
//!    management-gated (`require_management`). A data owner creates a share, a
//!    scoped read-only projection of assets to an external recipient, adds
//!    grants (with optional row filter / column mask), and revokes it. The
//!    marketplace routes surface the certified data-product gallery (J-F2,
//!    reusing Pillar G) with a request-access flow (reusing the
//!    `access_requests` table from Pillar D).
//!
//! 2. **Recipient IRC endpoint** (`/share/{token}/v1/...`) — a *distinct
//!    catalog prefix per share*, authenticated by the share **token** in the
//!    URL (an external recipient holds no Meridian OIDC identity), exempted
//!    from the OIDC middleware in `crate::auth`. It serves **only** the shared
//!    assets, **read-only**, with **vended read-only credentials** (reusing
//!    `meridian-vending`) and the grant's **column mask** applied to the served
//!    schema. Every recipient access is audited. It speaks plain Iceberg REST,
//!    so it works with *any* IRC-capable engine on the recipient side.
//!
//! # Honest scope (stated plainly, per project rules)
//!
//! - **Revocation is instant in effect *with STS vending*.** The moment a share
//!   is revoked the endpoint returns 403 and vends nothing new. On an
//!   STS-vending warehouse the recipient only ever holds short-lived credentials
//!   that expire on their TTL, so there is no long-lived key to claw back.
//!   **Caveat:** a warehouse configured `vending = static` passes through
//!   non-expiring keys (see `docs/design/sharing.md` §3 and `routes/vending.rs`),
//!   so a recipient of a static-vending share can retain access past revocation
//!   until those keys are rotated — use STS vending where instant revocation
//!   matters.
//!
//! - **Column masking over a pure IRC + vended-credential share is *surfaced*,
//!   not physically prevented — the same caveat as row filtering below.**
//!   Meridian drops masked columns from the served schema, so a schema-aware
//!   recipient engine does not see or select them. But the recipient holds
//!   read credentials scoped to the table's storage prefix and can read the
//!   Parquet files directly, where a masked column's bytes are still present.
//!   Physical column-level enforcement requires the query-mediated path
//!   (server-side scan planning / the governed executor, Pillar D/H), not a
//!   raw vended-credential read. Treat the share mask as detect/deter at this
//!   layer, and share only columns the recipient may physically read when the
//!   guarantee must be hard.
//!
//! - **Workspace ABAC does not transfer to a share.** A share applies only the
//!   grant's own row filter and column mask — it does *not* re-resolve the
//!   table's catalog-wide Cedar/tag policies (e.g. a `pii:high` tag policy) for
//!   the recipient. Restate any protection that must hold as an explicit grant
//!   filter/mask on the share; the workspace policy layer is not a floor here.
//!
//! - **Row filtering over a pure IRC catalog is advisory.** A vended-credential
//!   engine reads Parquet directly from object storage; the catalog cannot
//!   interpose a WHERE clause on that read. Meridian therefore *surfaces* the
//!   grant's row filter to the recipient (as a table property and in the share
//!   manifest) and audits it, but does not claim to *prevent* a determined
//!   recipient from reading filtered-out rows if their engine ignores it. Full
//!   row-level prevention requires a query-mediated path (the workbench /
//!   scan-plan surface), which is out of scope for the neutral IRC endpoint.
//!   This is the same prevent-vs-detect honesty the rest of the codebase holds.
//!
//! - **External/public marketplace and clean-room compute are out of scope**
//!   (documented in `docs/design/sharing.md`): the marketplace here is the
//!   *internal* gallery for a workspace's own consumers.

use std::collections::BTreeMap;

use axum::extract::{Path, Query, State};
use axum::http::StatusCode;
use axum::response::{IntoResponse, Response};
use axum::{Extension, Json};
use meridian_common::MeridianError;
use meridian_common::principal::Principal;
use meridian_storage::read_table_metadata;
use meridian_store::shares::{self, NewShare, ShareGrantRecord, ShareRecord};
use meridian_store::{access_requests, audit, outbox, tenancy};
use meridian_vending::AccessMode;
use serde::Deserialize;
use serde_json::{Value, json};

use crate::AppState;
use crate::error::ApiError;
use crate::routes::grants::require_management;
use crate::routes::namespaces::decode_namespace_param;
use crate::routes::tables::{connect_storage, no_such_table};
use crate::routes::vending::{VendContext, storage_credential_json, vend_for_table};

// ===========================================================================
// Shared helpers
// ===========================================================================

/// Generates a high-entropy opaque share token: two v4 UUIDs (256 bits of
/// cryptographic randomness) rendered as URL-safe lowercase hex. Unguessable;
/// unique across all shares (a collision at 256 bits is not a concern, and the
/// `shares_token_unique` index is the backstop).
fn generate_token() -> String {
    let a = uuid::Uuid::new_v4().simple().to_string();
    let b = uuid::Uuid::new_v4().simple().to_string();
    format!("{a}{b}")
}

/// The JSON view of a share for the management API. Never includes the token
/// except on the create response (the one time the operator needs to copy it).
fn share_json(share: &ShareRecord, include_token: bool) -> Value {
    let mut value = json!({
        "id": share.id,
        "name": share.name,
        "recipient": share.recipient,
        "created_by": share.created_by,
        "revoked": share.revoked,
        "revoked_at": share.revoked_at.map(|t| t.to_rfc3339()),
        "has_terms": share.terms.is_some(),
        "terms_accepted": share.terms_accepted_at.is_some(),
        "terms_accepted_at": share.terms_accepted_at.map(|t| t.to_rfc3339()),
        "created_at": share.created_at.to_rfc3339(),
    });
    if include_token {
        value["token"] = json!(share.token);
    }
    value
}

/// The JSON view of a share grant.
fn grant_json(grant: &ShareGrantRecord) -> Value {
    json!({
        "id": grant.id,
        "securable_kind": grant.securable_kind,
        "securable_ref": grant.securable_ref,
        "row_filter": grant.row_filter,
        "column_mask": grant.column_mask.as_ref().map(|m| m.0.clone()),
        "created_at": grant.created_at.to_rfc3339(),
    })
}

/// Maps store not-found/conflict/validation onto the management-API envelope.
fn management_error(error: MeridianError) -> ApiError {
    match error {
        MeridianError::NotFound(message) => {
            ApiError::new(StatusCode::NOT_FOUND, "NotFoundException", message)
        }
        MeridianError::Conflict(message) => ApiError::already_exists(message),
        MeridianError::Validation(message) => ApiError::bad_request(message),
        other => ApiError::from(other),
    }
}

// ===========================================================================
// Management API: shares (J-F1)
// ===========================================================================

/// Body of `POST /api/v2/shares`.
#[derive(Debug, Deserialize)]
pub struct CreateShareRequest {
    /// Machine name (unique per workspace).
    pub name: String,
    /// External recipient identifier (audit string, e.g. `org:acme`).
    pub recipient: String,
    /// Optional terms of use the recipient must accept before data serves.
    #[serde(default)]
    pub terms: Option<String>,
}

/// `POST /api/v2/shares` — create a share. The response is the only place the
/// token is returned; copy it to the recipient over a secure channel.
pub async fn create_share(
    State(state): State<AppState>,
    Extension(caller): Extension<Principal>,
    Json(body): Json<CreateShareRequest>,
) -> Result<Response, ApiError> {
    require_management(&state.pool, &caller).await?;
    if body.name.trim().is_empty() {
        return Err(ApiError::bad_request("share name must not be empty"));
    }
    if body.recipient.trim().is_empty() {
        return Err(ApiError::bad_request("share recipient must not be empty"));
    }
    let token = generate_token();
    let share = shares::create_share(
        &state.pool,
        tenancy::default_workspace_id(),
        NewShare {
            name: body.name.trim(),
            recipient: body.recipient.trim(),
            token: &token,
            terms: body.terms.as_deref().filter(|t| !t.trim().is_empty()),
        },
        &caller.audit_string(),
    )
    .await
    .map_err(management_error)?;

    // include_token=true: the operator needs it exactly once.
    Ok((StatusCode::CREATED, Json(share_json(&share, true))).into_response())
}

/// Query for `GET /api/v2/shares`.
#[derive(Debug, Deserialize)]
pub struct ListSharesQuery {
    /// Keyset cursor (exclusive) over share ids.
    #[serde(default)]
    pub after: Option<String>,
    /// Page size (default 100, capped 500).
    #[serde(default)]
    pub limit: Option<i64>,
}

/// `GET /api/v2/shares` — list the workspace's shares (tokens omitted).
pub async fn list_shares(
    State(state): State<AppState>,
    Extension(caller): Extension<Principal>,
    Query(query): Query<ListSharesQuery>,
) -> Result<Json<Value>, ApiError> {
    require_management(&state.pool, &caller).await?;
    let limit = query.limit.unwrap_or(100).clamp(1, 500);
    let list = shares::list_shares(
        &state.pool,
        tenancy::default_workspace_id(),
        query.after.as_deref(),
        Some(limit),
    )
    .await?;
    let shares_json: Vec<Value> = list.iter().map(|s| share_json(s, false)).collect();
    Ok(Json(json!({ "shares": shares_json })))
}

/// `GET /api/v2/shares/{id}` — one share with its grants (token omitted).
pub async fn get_share(
    State(state): State<AppState>,
    Extension(caller): Extension<Principal>,
    Path(id): Path<String>,
) -> Result<Json<Value>, ApiError> {
    require_management(&state.pool, &caller).await?;
    let share = shares::get_share(&state.pool, &id)
        .await?
        .filter(|s| s.workspace_id == tenancy::default_workspace_id().to_string())
        .ok_or_else(|| {
            ApiError::new(
                StatusCode::NOT_FOUND,
                "NotFoundException",
                "share not found",
            )
        })?;
    let grants = shares::list_share_grants(&state.pool, &share.id).await?;
    let mut value = share_json(&share, false);
    value["grants"] = json!(grants.iter().map(grant_json).collect::<Vec<_>>());
    Ok(Json(value))
}

/// `POST /api/v2/shares/{id}/revoke` — revoke a share (idempotent).
pub async fn revoke_share(
    State(state): State<AppState>,
    Extension(caller): Extension<Principal>,
    Path(id): Path<String>,
) -> Result<Json<Value>, ApiError> {
    require_management(&state.pool, &caller).await?;
    let share = shares::revoke_share(
        &state.pool,
        tenancy::default_workspace_id(),
        &id,
        &caller.audit_string(),
    )
    .await
    .map_err(management_error)?;
    Ok(Json(share_json(&share, false)))
}

/// `DELETE /api/v2/shares/{id}` — delete a share (and its grants).
pub async fn delete_share(
    State(state): State<AppState>,
    Extension(caller): Extension<Principal>,
    Path(id): Path<String>,
) -> Result<StatusCode, ApiError> {
    require_management(&state.pool, &caller).await?;
    shares::delete_share(
        &state.pool,
        tenancy::default_workspace_id(),
        &id,
        &caller.audit_string(),
    )
    .await
    .map_err(management_error)?;
    Ok(StatusCode::NO_CONTENT)
}

/// Body of `POST /api/v2/shares/{id}/grants`.
#[derive(Debug, Deserialize)]
pub struct AddGrantRequest {
    /// Securable kind: `table` | `view` | `data_product`.
    pub securable_kind: String,
    /// Stable securable reference (e.g. `table:<id>`, or a data product id).
    pub securable_ref: String,
    /// Optional row filter (advisory boolean SQL predicate).
    #[serde(default)]
    pub row_filter: Option<String>,
    /// Optional column mask (column names to hide from the recipient).
    #[serde(default)]
    pub column_mask: Option<Vec<String>>,
}

/// `POST /api/v2/shares/{id}/grants` — add a securable to a share.
pub async fn add_grant(
    State(state): State<AppState>,
    Extension(caller): Extension<Principal>,
    Path(id): Path<String>,
    Json(body): Json<AddGrantRequest>,
) -> Result<Response, ApiError> {
    require_management(&state.pool, &caller).await?;
    if !matches!(
        body.securable_kind.as_str(),
        "table" | "view" | "data_product"
    ) {
        return Err(ApiError::bad_request(
            "securable_kind must be one of: table, view, data_product",
        ));
    }
    let grant = shares::add_grant(
        &state.pool,
        tenancy::default_workspace_id(),
        &id,
        &body.securable_kind,
        &body.securable_ref,
        body.row_filter.as_deref().filter(|f| !f.trim().is_empty()),
        body.column_mask.as_deref(),
        &caller.audit_string(),
    )
    .await
    .map_err(management_error)?;
    Ok((StatusCode::CREATED, Json(grant_json(&grant))).into_response())
}

/// `DELETE /api/v2/shares/grants/{grant_id}` — remove a grant.
pub async fn remove_grant(
    State(state): State<AppState>,
    Extension(caller): Extension<Principal>,
    Path(grant_id): Path<String>,
) -> Result<StatusCode, ApiError> {
    require_management(&state.pool, &caller).await?;
    shares::remove_grant(
        &state.pool,
        tenancy::default_workspace_id(),
        &grant_id,
        &caller.audit_string(),
    )
    .await
    .map_err(management_error)?;
    Ok(StatusCode::NO_CONTENT)
}

// ===========================================================================
// Management API: internal marketplace (J-F2)
// ===========================================================================

/// `GET /api/v2/marketplace/products` — the certified-data-product gallery
/// (J-F2). Surfaces the Pillar-G data products, certified ones first, as the
/// "shopping" catalog for internal consumers. Read-only; management-gated like
/// the rest of the semantics surface.
pub async fn marketplace_products(
    State(state): State<AppState>,
    Extension(caller): Extension<Principal>,
) -> Result<Json<Value>, ApiError> {
    require_management(&state.pool, &caller).await?;
    let products = meridian_store::semantics::list_products(
        &state.pool,
        tenancy::default_workspace_id(),
        None,
        Some(500),
    )
    .await?;

    // Certified first, then everything else, each block in creation order.
    let mut certified = Vec::new();
    let mut others = Vec::new();
    for p in products {
        let entry = json!({
            "id": p.id,
            "name": p.name,
            "display_name": p.display_name,
            "description": p.description,
            "owner": p.owner,
            "sla": p.sla,
            "certification": p.certification,
        });
        if p.certification == "certified" {
            certified.push(entry);
        } else {
            others.push(entry);
        }
    }
    certified.extend(others);
    Ok(Json(json!({ "products": certified })))
}

/// Body of `POST /api/v2/marketplace/requests`.
#[derive(Debug, Deserialize)]
pub struct RequestAccessRequest {
    /// Securable kind: `warehouse` | `namespace` | `table` | `view`.
    pub securable_type: String,
    /// Stable securable reference.
    pub securable_id: String,
    /// Requested privilege (RBAC wire form, e.g. `READ`).
    #[serde(default = "default_privilege")]
    pub privilege: String,
    /// Declared purpose (purpose-based access).
    pub purpose: String,
    /// Optional requested grant lifetime, in seconds.
    #[serde(default)]
    pub ttl_seconds: Option<i64>,
}

fn default_privilege() -> String {
    "READ".to_owned()
}

/// `POST /api/v2/marketplace/requests` — a consumer requests access to a
/// product's asset (J-F2). Creates a `pending` `access_requests` row (D-F4),
/// audited. This is the marketplace "request access" button; approval is the
/// governance decide flow.
pub async fn request_access(
    State(state): State<AppState>,
    Extension(caller): Extension<Principal>,
    Json(body): Json<RequestAccessRequest>,
) -> Result<Response, ApiError> {
    // Note: any authenticated principal may *request* access (that is the
    // point of a self-service marketplace); the decision is management-gated.
    if !matches!(
        body.securable_type.as_str(),
        "warehouse" | "namespace" | "table" | "view"
    ) {
        return Err(ApiError::bad_request(
            "securable_type must be one of: warehouse, namespace, table, view",
        ));
    }
    if body.purpose.trim().is_empty() {
        return Err(ApiError::bad_request("purpose must not be empty"));
    }
    if body.ttl_seconds.is_some_and(|ttl| ttl <= 0) {
        return Err(ApiError::bad_request("ttl_seconds must be positive"));
    }
    let record = access_requests::create(
        &state.pool,
        tenancy::default_workspace_id(),
        access_requests::NewAccessRequest {
            securable_type: &body.securable_type,
            securable_id: &body.securable_id,
            privilege: &body.privilege,
            purpose: body.purpose.trim(),
            ttl_seconds: body.ttl_seconds,
        },
        &caller.audit_string(),
    )
    .await
    .map_err(management_error)?;
    Ok((StatusCode::CREATED, Json(access_request_json(&record))).into_response())
}

/// Query for `GET /api/v2/marketplace/requests`.
#[derive(Debug, Deserialize)]
pub struct ListRequestsQuery {
    /// Optional state filter (`pending` | `approved` | `denied` | `expired`).
    #[serde(default)]
    pub state: Option<String>,
    /// Keyset cursor (exclusive, descending id).
    #[serde(default)]
    pub before: Option<String>,
    /// Page size (default 100, capped 500).
    #[serde(default)]
    pub limit: Option<i64>,
}

/// `GET /api/v2/marketplace/requests` — the pending/decided request queue
/// (management-gated: this is the approver's view).
pub async fn list_requests(
    State(state): State<AppState>,
    Extension(caller): Extension<Principal>,
    Query(query): Query<ListRequestsQuery>,
) -> Result<Json<Value>, ApiError> {
    require_management(&state.pool, &caller).await?;
    let limit = query.limit.unwrap_or(100).clamp(1, 500);
    let list = access_requests::list(
        &state.pool,
        tenancy::default_workspace_id(),
        query.state.as_deref().filter(|s| !s.is_empty()),
        query.before.as_deref(),
        Some(limit),
    )
    .await?;
    Ok(Json(
        json!({ "requests": list.iter().map(access_request_json).collect::<Vec<_>>() }),
    ))
}

/// Body of `POST /api/v2/marketplace/requests/{id}/decide`.
#[derive(Debug, Deserialize)]
pub struct DecideRequest {
    /// True to approve, false to deny.
    pub approve: bool,
    /// Optional decision reason.
    #[serde(default)]
    pub reason: Option<String>,
}

/// `POST /api/v2/marketplace/requests/{id}/decide` — approve or deny a request
/// (management-gated). Records the decision on the request object.
pub async fn decide_request(
    State(state): State<AppState>,
    Extension(caller): Extension<Principal>,
    Path(id): Path<String>,
    Json(body): Json<DecideRequest>,
) -> Result<Json<Value>, ApiError> {
    require_management(&state.pool, &caller).await?;
    let record = access_requests::decide(
        &state.pool,
        tenancy::default_workspace_id(),
        &id,
        body.approve,
        body.reason.as_deref().filter(|r| !r.trim().is_empty()),
        &caller.audit_string(),
    )
    .await
    .map_err(management_error)?;
    Ok(Json(access_request_json(&record)))
}

fn access_request_json(r: &access_requests::AccessRequestRecord) -> Value {
    json!({
        "id": r.id,
        "principal": r.principal,
        "securable_type": r.securable_type,
        "securable_id": r.securable_id,
        "privilege": r.privilege,
        "purpose": r.purpose,
        "ttl_seconds": r.ttl_seconds,
        "state": r.state,
        "decided_by": r.decided_by,
        "reason": r.reason,
        "decided_at": r.decided_at.map(|t| t.to_rfc3339()),
        "created_at": r.created_at.to_rfc3339(),
    })
}

// ===========================================================================
// Recipient IRC endpoint (/share/{token}/...) — token-authenticated
// ===========================================================================

/// Resolves and validates a share by its URL token for the recipient path.
///
/// Returns a clean 401 for an unknown token (do not leak whether a token is
/// valid vs. the share revoked — an unknown token is simply unauthorized) and
/// a 403 for a revoked share. Terms acceptance is checked by the caller where
/// it applies (config/terms endpoints resolve even un-accepted).
async fn resolve_share_token(state: &AppState, token: &str) -> Result<ShareRecord, ApiError> {
    let share = shares::get_share_by_token(&state.pool, token)
        .await?
        .ok_or_else(|| {
            ApiError::new(
                StatusCode::UNAUTHORIZED,
                "NotAuthorizedException",
                "invalid share token",
            )
        })?;
    if share.revoked {
        return Err(ApiError::new(
            StatusCode::FORBIDDEN,
            "ForbiddenException",
            "this share has been revoked",
        ));
    }
    Ok(share)
}

/// Requires the share be fully servable: active *and* (if it has terms)
/// accepted. Returns a 403 pointing the recipient at the terms-acceptance
/// endpoint when terms are outstanding.
fn require_servable(share: &ShareRecord) -> Result<(), ApiError> {
    if share.needs_terms_acceptance() {
        return Err(ApiError::new(
            StatusCode::FORBIDDEN,
            "ForbiddenException",
            "the terms of this share must be accepted before data is served; \
             POST /share/{token}/terms/accept",
        ));
    }
    Ok(())
}

/// Writes an audit row for a recipient access on the share (its own
/// transaction). The principal is the recipient identifier; there is no
/// Meridian Principal for an external recipient.
async fn audit_recipient_access(
    state: &AppState,
    share: &ShareRecord,
    action: &str,
    details: Value,
) -> Result<(), ApiError> {
    let workspace_id = tenancy::default_workspace_id();
    let resource = format!("share:{}", share.id);
    let mut tx = state
        .pool
        .begin()
        .await
        .map_err(|e| MeridianError::internal("failed to begin recipient audit", e))?;
    outbox::enqueue(
        &mut *tx,
        &outbox::NewOutboxEvent {
            workspace_id: Some(workspace_id),
            aggregate: resource.clone(),
            event_type: format!("share.recipient.{action}"),
            payload: details.clone(),
        },
    )
    .await?;
    audit::append_in_tx(
        &mut tx,
        audit::NewAuditEntry {
            workspace_id: Some(workspace_id),
            principal: format!("recipient:{}", share.recipient),
            action: format!("share.recipient.{action}"),
            resource,
            details,
        },
    )
    .await?;
    tx.commit()
        .await
        .map_err(|e| MeridianError::internal("failed to commit recipient audit", e))?;
    Ok(())
}

/// `GET /share/{token}/v1/config` — the recipient's IRC `ConfigResponse`.
///
/// Advertises the *read-only* endpoint subset (no create/commit/rename). When
/// terms are outstanding it still answers 200 with the config plus a
/// `terms-required` note in `overrides`, so the recipient's client can connect
/// and the recipient is directed to accept terms.
pub async fn recipient_config(
    State(state): State<AppState>,
    Path(token): Path<String>,
) -> Result<Json<Value>, ApiError> {
    let share = resolve_share_token(&state, &token).await?;
    audit_recipient_access(&state, &share, "config", json!({})).await?;

    let mut overrides = BTreeMap::new();
    // The recipient client uses the token as its catalog prefix implicitly via
    // the URL; the prefix override is the empty string (a single logical
    // catalog per share).
    overrides.insert("prefix".to_owned(), String::new());
    if share.needs_terms_acceptance() {
        overrides.insert("terms-required".to_owned(), "true".to_owned());
    }

    Ok(Json(json!({
        "defaults": {},
        "overrides": overrides,
        "endpoints": RECIPIENT_ENDPOINTS,
    })))
}

/// The read-only endpoint subset the recipient catalog advertises. No
/// create/commit/rename/delete: a share is read-only by construction. There is
/// no separate `.../credentials` endpoint here — vended read-only credentials
/// are delivered inline in the `LoadTableResult.config`, so a recipient never
/// needs a second round-trip.
const RECIPIENT_ENDPOINTS: &[&str] = &[
    "GET /v1/{prefix}/namespaces",
    "GET /v1/{prefix}/namespaces/{namespace}",
    "HEAD /v1/{prefix}/namespaces/{namespace}",
    "GET /v1/{prefix}/namespaces/{namespace}/tables",
    "GET /v1/{prefix}/namespaces/{namespace}/tables/{table}",
    "HEAD /v1/{prefix}/namespaces/{namespace}/tables/{table}",
];

/// `POST /share/{token}/terms/accept` — the recipient accepts the share's
/// terms. Idempotent; audited. Returns the (now accepted) share summary.
pub async fn accept_terms(
    State(state): State<AppState>,
    Path(token): Path<String>,
) -> Result<Json<Value>, ApiError> {
    let share = resolve_share_token(&state, &token).await?;
    let accepted = shares::accept_terms(&state.pool, &share.id)
        .await
        .map_err(management_error)?;
    Ok(Json(json!({
        "name": accepted.name,
        "terms_accepted": accepted.terms_accepted_at.is_some(),
        "terms_accepted_at": accepted.terms_accepted_at.map(|t| t.to_rfc3339()),
    })))
}

/// `GET /share/{token}/terms` — the terms text (so a recipient can read them
/// before accepting). Resolves even when un-accepted.
pub async fn get_terms(
    State(state): State<AppState>,
    Path(token): Path<String>,
) -> Result<Json<Value>, ApiError> {
    let share = resolve_share_token(&state, &token).await?;
    Ok(Json(json!({
        "name": share.name,
        "recipient": share.recipient,
        "terms": share.terms,
        "terms_accepted": share.terms_accepted_at.is_some(),
    })))
}

/// The grants of a share indexed for serve-time decisions. `data_product`
/// grants are expanded to their member table/view refs so the recipient
/// endpoint reasons in terms of concrete Iceberg tables.
struct ResolvedGrants {
    /// table id -> its grant (row filter / column mask).
    tables: BTreeMap<String, ShareGrantRecord>,
}

/// Expands a share's grants into the concrete set of shared table ids (with
/// their per-grant policy). A `data_product` grant contributes its member
/// tables' ids; a `table` grant contributes its own; `view` grants are
/// recorded but views are not served over the recipient table surface in this
/// wave (documented — the endpoint serves Iceberg tables).
async fn resolve_grants(state: &AppState, share: &ShareRecord) -> Result<ResolvedGrants, ApiError> {
    let grants = shares::list_share_grants(&state.pool, &share.id).await?;
    let mut tables: BTreeMap<String, ShareGrantRecord> = BTreeMap::new();

    for grant in grants {
        match grant.securable_kind.as_str() {
            "table" => {
                if let Some(table_id) = grant.securable_ref.strip_prefix("table:") {
                    tables.insert(table_id.to_owned(), grant);
                }
            }
            "data_product" => {
                // Expand product members that are tables. A product member's
                // ref is itself a `table:<id>` (see Pillar G). The product
                // grant's own row filter / column mask applies to every
                // expanded member table.
                let product_id = grant
                    .securable_ref
                    .strip_prefix("data_product:")
                    .unwrap_or(&grant.securable_ref);
                let members =
                    meridian_store::semantics::list_product_members(&state.pool, product_id)
                        .await?;
                for m in members {
                    if m.member_kind != "table" {
                        continue;
                    }
                    if let Some(table_id) = m.member_ref.strip_prefix("table:") {
                        // A direct table grant wins over a product-inherited one
                        // (more specific policy).
                        tables
                            .entry(table_id.to_owned())
                            .or_insert_with(|| grant.clone());
                    }
                }
            }
            _ => {}
        }
    }
    Ok(ResolvedGrants { tables })
}

/// `GET /share/{token}/v1/namespaces` — the namespaces containing shared
/// tables. Only namespaces that actually hold a shared table appear.
pub async fn recipient_list_namespaces(
    State(state): State<AppState>,
    Path(token): Path<String>,
) -> Result<Json<Value>, ApiError> {
    let share = resolve_share_token(&state, &token).await?;
    require_servable(&share)?;
    let resolved = resolve_grants(&state, &share).await?;

    let mut namespaces: std::collections::BTreeSet<Vec<String>> = std::collections::BTreeSet::new();
    for table_id in resolved.tables.keys() {
        if let Some(levels) = table_namespace_levels(&state, table_id).await? {
            namespaces.insert(levels);
        }
    }
    audit_recipient_access(
        &state,
        &share,
        "list_namespaces",
        json!({ "count": namespaces.len() }),
    )
    .await?;
    Ok(Json(json!({
        "namespaces": namespaces.into_iter().collect::<Vec<_>>(),
    })))
}

/// `GET /share/{token}/v1/namespaces/{namespace}/tables` — the shared tables in
/// a namespace (and only those).
pub async fn recipient_list_tables(
    State(state): State<AppState>,
    Path((token, raw_namespace)): Path<(String, String)>,
) -> Result<Json<Value>, ApiError> {
    let share = resolve_share_token(&state, &token).await?;
    require_servable(&share)?;
    let levels = decode_namespace_param(&raw_namespace)?;
    let resolved = resolve_grants(&state, &share).await?;

    let mut identifiers = Vec::new();
    for table_id in resolved.tables.keys() {
        if let Some((tbl_levels, name)) = table_ident(&state, table_id).await?
            && tbl_levels == levels
        {
            identifiers.push(json!({ "namespace": tbl_levels, "name": name }));
        }
    }
    audit_recipient_access(
        &state,
        &share,
        "list_tables",
        json!({ "namespace": levels, "count": identifiers.len() }),
    )
    .await?;
    Ok(Json(json!({ "identifiers": identifiers })))
}

/// `GET /share/{token}/v1/namespaces/{namespace}/tables/{table}` — load a
/// shared table, read-only, with the grant's column mask applied to the served
/// schema and (when the source warehouse vends) read-only credentials attached.
pub async fn recipient_load_table(
    State(state): State<AppState>,
    Path((token, raw_namespace, name)): Path<(String, String, String)>,
) -> Result<Response, ApiError> {
    let share = resolve_share_token(&state, &token).await?;
    require_servable(&share)?;
    let levels = decode_namespace_param(&raw_namespace)?;
    let resolved = resolve_grants(&state, &share).await?;

    // Resolve the requested ns.table across warehouses to a table id, then
    // require the share actually grants it (otherwise 404 — a recipient must
    // not learn a table exists unless it is shared).
    let (warehouse, record) = resolve_shared_table(&state, &levels, &name, &resolved)
        .await?
        .ok_or_else(|| no_such_table(&levels, &name))?;
    let grant = resolved
        .tables
        .get(&record.id)
        .expect("resolve_shared_table only returns granted tables");

    let Some(metadata_location) = record.metadata_location.clone() else {
        return Err(no_such_table(&levels, &name));
    };
    let storage = connect_storage(&warehouse)?;
    let metadata = read_table_metadata(storage.as_ref(), &metadata_location)
        .await
        .map_err(|e| {
            ApiError::new(
                StatusCode::INTERNAL_SERVER_ERROR,
                "InternalServerError",
                format!("current metadata at {metadata_location:?} is unreadable: {e}"),
            )
        })?;

    // Build the LoadTableResult and apply the column mask to the served schema.
    let mut body = json!({
        "metadata-location": metadata_location,
        "metadata": metadata_to_value(&metadata)?,
        "config": crate::routes::views::storage_client_config(&warehouse),
    });
    let masked = if let Some(mask) = grant.column_mask.as_ref() {
        apply_column_mask(&mut body, &mask.0);
        mask.0.clone()
    } else {
        Vec::new()
    };
    // Surface the (advisory) row filter to the recipient's engine as a table
    // property, and note the shared read-only nature.
    if let Some(config) = body.get_mut("config").and_then(Value::as_object_mut) {
        config.insert("meridian.share.read-only".to_owned(), json!("true"));
        if let Some(filter) = grant.row_filter.as_ref() {
            config.insert("meridian.share.row-filter".to_owned(), json!(filter));
        }
    }

    // Vend read-only credentials if the source warehouse opted into vending.
    // The recipient never gets write credentials — access is Read, always.
    let ident = if levels.is_empty() {
        name.clone()
    } else {
        format!("{}.{name}", levels.join("."))
    };
    let recipient_principal = recipient_synthetic_principal(&share);
    let vended = vend_for_table(
        &state,
        &recipient_principal,
        &VendContext {
            warehouse: &warehouse,
            table_id: &record.id,
            table_ident: &ident,
            table_location: &metadata.location,
            access: AccessMode::Read,
        },
    )
    .await
    .unwrap_or(None);
    if let Some(vended) = vended {
        if let Some(config) = body.get_mut("config").and_then(Value::as_object_mut) {
            for (key, value) in &vended.config {
                config.insert(key.clone(), Value::String(value.clone()));
            }
        }
        body["storage-credentials"] = json!([storage_credential_json(&vended)]);
    }

    audit_recipient_access(
        &state,
        &share,
        "load_table",
        json!({
            "table": ident,
            "table_id": record.id,
            "masked_columns": masked,
            "row_filter": grant.row_filter,
            "vended": body.get("storage-credentials").is_some(),
        }),
    )
    .await?;

    Ok((StatusCode::OK, Json(body)).into_response())
}

/// Rejects any write against the recipient endpoint with a clear 403 — a share
/// is read-only by construction. Mounted on the create/commit/rename/delete
/// verbs of the recipient table surface.
pub async fn recipient_write_rejected() -> ApiError {
    ApiError::new(
        StatusCode::FORBIDDEN,
        "ForbiddenException",
        "shared assets are read-only; writes are not permitted on a share endpoint",
    )
}

// -- recipient helpers -------------------------------------------------------

/// A synthetic service principal representing the recipient, used only to
/// attribute the vend's audit row (vending records the principal's audit
/// string). Its subject is the recipient identifier.
fn recipient_synthetic_principal(share: &ShareRecord) -> Principal {
    Principal {
        kind: meridian_common::principal::PrincipalKind::Service,
        subject: format!("share-recipient:{}", share.recipient),
        issuer: None,
        display_name: None,
    }
}

/// Serializes table metadata to a JSON value (local twin of the table route's
/// private helper).
fn metadata_to_value(metadata: &meridian_iceberg::spec::TableMetadata) -> Result<Value, ApiError> {
    let text = metadata
        .to_json()
        .map_err(|e| MeridianError::internal("failed to serialize table metadata", e))?;
    serde_json::from_str(&text)
        .map_err(|e| MeridianError::internal("metadata JSON round-trip failed", e).into())
}

/// Drops the named columns from the current schema of a serialized
/// `LoadTableResult` metadata, so a schema-aware recipient engine does not see
/// or select masked columns. Matching is case-insensitive on the top-level
/// field name. Returns silently if the metadata shape is unexpected (best-effort
/// at the JSON layer; an unrecognized shape serves the unmasked schema).
///
/// This is *schema-level surfacing, not physical prevention*: the recipient
/// holds storage-prefix-scoped read credentials and can read the underlying
/// Parquet directly, where the masked column's bytes remain. See the module
/// docs — hard per-column enforcement needs the query-mediated path, not a raw
/// vended-credential read.
fn apply_column_mask(body: &mut Value, mask: &[String]) {
    let lower: std::collections::BTreeSet<String> = mask.iter().map(|c| c.to_lowercase()).collect();
    let Some(metadata) = body.get_mut("metadata").and_then(Value::as_object_mut) else {
        return;
    };
    let current_id = metadata.get("current-schema-id").and_then(Value::as_i64);
    let Some(schemas) = metadata.get_mut("schemas").and_then(Value::as_array_mut) else {
        return;
    };
    for schema in schemas.iter_mut() {
        let schema_id = schema.get("schema-id").and_then(Value::as_i64);
        // Mask the current schema (the one the recipient reads against); leave
        // historical schemas untouched — they describe old snapshots.
        if current_id.is_some() && schema_id != current_id {
            continue;
        }
        if let Some(fields) = schema.get_mut("fields").and_then(Value::as_array_mut) {
            fields.retain(|f| {
                f.get("name")
                    .and_then(Value::as_str)
                    .is_none_or(|n| !lower.contains(&n.to_lowercase()))
            });
        }
    }
}

/// Resolves the namespace levels of a table by id.
async fn table_namespace_levels(
    state: &AppState,
    table_id: &str,
) -> Result<Option<Vec<String>>, ApiError> {
    Ok(table_ident(state, table_id)
        .await?
        .map(|(levels, _)| levels))
}

/// Resolves a table id to its (namespace levels, table name).
async fn table_ident(
    state: &AppState,
    table_id: &str,
) -> Result<Option<(Vec<String>, String)>, ApiError> {
    let Some(record) = meridian_store::table::get_by_id(&state.pool, table_id).await? else {
        return Ok(None);
    };
    let Some(namespace) =
        meridian_store::namespace::get_by_id(&state.pool, &record.namespace_id).await?
    else {
        return Ok(None);
    };
    Ok(Some((namespace.levels, record.name)))
}

/// Finds the warehouse + table record for a requested ns.table that is
/// *actually shared* (present in `resolved.tables`). Returns `None` when the
/// table does not exist or is not granted to the share.
type SharedTable = (
    meridian_store::warehouse::WarehouseRecord,
    meridian_store::table::TableRecord,
);

async fn resolve_shared_table(
    state: &AppState,
    levels: &[String],
    name: &str,
    resolved: &ResolvedGrants,
) -> Result<Option<SharedTable>, ApiError> {
    // A share can span warehouses; find the granted table whose ident matches.
    for table_id in resolved.tables.keys() {
        let Some(record) = meridian_store::table::get_by_id(&state.pool, table_id).await? else {
            continue;
        };
        let Some(namespace) =
            meridian_store::namespace::get_by_id(&state.pool, &record.namespace_id).await?
        else {
            continue;
        };
        if namespace.levels == levels && record.name == name {
            let warehouse = meridian_store::warehouse::get_by_id(
                &state.pool,
                tenancy::default_workspace_id(),
                &namespace.warehouse_id,
            )
            .await?;
            return Ok(warehouse.map(|w| (w, record)));
        }
    }
    Ok(None)
}
