//! Audit API: querying the hash-chained audit log and verifying the chain.
//! Both endpoints live under `/api/v2`.
//!
//! # Authorization
//!
//! Both endpoints require **management access** (the `admin` role or any
//! `MANAGE_WAREHOUSE` grant), like the rest of the management API: the
//! audit log spans every resource in the workspace, so no resource-scoped
//! privilege can express "may read audit history" without over- or
//! under-granting (same reasoning as the events feed in
//! [`crate::routes::events`]).
//!
//! # Pagination
//!
//! `GET /api/v2/audit` returns entries newest first, keyset-paginated by
//! chain position (`seq`): `next_cursor` in a response is the `seq` of the
//! last (oldest) entry returned; pass it back as `before` to fetch the next
//! page. `next_cursor` is absent on the last page.

use axum::extract::{Query, State};
use axum::{Extension, Json};
use chrono::{DateTime, Utc};
use meridian_common::principal::Principal;
use meridian_store::audit::{self, AuditRecord, NewAuditEntry};
use serde::{Deserialize, Serialize};
use serde_json::{Value, json};

use crate::AppState;
use crate::error::ApiError;
use crate::routes::grants::require_management;

/// Default and maximum page sizes for audit queries.
const DEFAULT_PAGE_SIZE: i64 = 100;
const MAX_PAGE_SIZE: i64 = 1000;

/// Query parameters for `GET /api/v2/audit`.
#[derive(Debug, Deserialize)]
pub struct AuditQueryParams {
    /// Exact principal audit string, e.g. `user:alice@example.com`.
    #[serde(default)]
    pub principal: Option<String>,
    /// Action filter: exact (`table.commit`) or a prefix ending in `.*`
    /// (`table.*` matches every `table.` action).
    #[serde(default)]
    pub action: Option<String>,
    /// Exact resource, e.g. `table:01J...`.
    #[serde(default)]
    pub resource: Option<String>,
    /// Exact workspace id.
    #[serde(default)]
    pub workspace: Option<String>,
    /// Inclusive lower bound on `occurred_at` (RFC 3339).
    #[serde(default)]
    pub from: Option<String>,
    /// Inclusive upper bound on `occurred_at` (RFC 3339).
    #[serde(default)]
    pub to: Option<String>,
    /// Keyset cursor: only entries with `seq` strictly below this
    /// (typically `next_cursor` from the previous page).
    #[serde(default)]
    pub before: Option<i64>,
    /// Page size (default 100, max 1000).
    #[serde(default)]
    pub limit: Option<i64>,
}

/// One audit entry as rendered by the API: the full persisted record,
/// including its chain position (`seq`) and hash linkage.
#[derive(Debug, Serialize)]
pub struct AuditEntryResponse {
    /// Chain position (monotonic; the keyset cursor).
    pub seq: i64,
    /// ULID of the entry.
    pub id: String,
    /// Workspace scope; `null` for org-level actions.
    pub workspace_id: Option<String>,
    /// When the action occurred (UTC, microsecond precision).
    pub occurred_at: DateTime<Utc>,
    /// Acting principal.
    pub principal: String,
    /// Action performed, e.g. `table.commit`.
    pub action: String,
    /// Resource acted on, e.g. `table:01J...`.
    pub resource: String,
    /// Structured detail payload.
    pub details: Value,
    /// Hash of the previous entry; `null` for the genesis entry.
    pub prev_hash: Option<String>,
    /// This entry's hash.
    pub hash: String,
}

impl From<AuditRecord> for AuditEntryResponse {
    fn from(record: AuditRecord) -> Self {
        Self {
            seq: record.seq,
            id: record.id,
            workspace_id: record.workspace_id,
            occurred_at: record.occurred_at,
            principal: record.principal,
            action: record.action,
            resource: record.resource,
            details: record.details,
            prev_hash: record.prev_hash,
            hash: record.hash,
        }
    }
}

/// Response body for `GET /api/v2/audit`.
#[derive(Debug, Serialize)]
pub struct AuditQueryResponse {
    /// Matching entries, newest first.
    pub entries: Vec<AuditEntryResponse>,
    /// Pass as `before` to fetch the next (older) page; absent on the
    /// last page.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub next_cursor: Option<i64>,
}

/// Parses an RFC 3339 timestamp query parameter.
fn parse_timestamp(name: &str, raw: &str) -> Result<DateTime<Utc>, ApiError> {
    DateTime::parse_from_rfc3339(raw)
        .map(|t| t.with_timezone(&Utc))
        .map_err(|_| {
            ApiError::bad_request(format!(
                "{name} must be an RFC 3339 timestamp (e.g. 2026-07-02T10:00:00Z), got {raw:?}"
            ))
        })
}

/// Splits the `action` filter into exact/prefix forms. `table.*` (or the
/// bare `*`) is a prefix match; anything else matches exactly.
fn parse_action_filter(action: Option<&str>) -> (Option<String>, Option<String>) {
    match action {
        None => (None, None),
        Some(action) => match action.strip_suffix('*') {
            // "*" alone means "everything" — no filter at all.
            Some("") => (None, None),
            Some(prefix) => (None, Some(prefix.to_owned())),
            None => (Some(action.to_owned()), None),
        },
    }
}

/// `GET /api/v2/audit` — query the audit log, newest first.
pub async fn query_audit(
    State(state): State<AppState>,
    Extension(principal): Extension<Principal>,
    Query(params): Query<AuditQueryParams>,
) -> Result<Json<AuditQueryResponse>, ApiError> {
    require_management(&state.pool, &principal).await?;

    let from = params
        .from
        .as_deref()
        .map(|raw| parse_timestamp("from", raw))
        .transpose()?;
    let to = params
        .to
        .as_deref()
        .map(|raw| parse_timestamp("to", raw))
        .transpose()?;
    if let (Some(from), Some(to)) = (from, to)
        && from > to
    {
        return Err(ApiError::bad_request("from must not be after to"));
    }

    let (action_exact, action_prefix) = parse_action_filter(params.action.as_deref());
    let limit = params
        .limit
        .unwrap_or(DEFAULT_PAGE_SIZE)
        .clamp(1, MAX_PAGE_SIZE);

    let entries = audit::query(
        &state.pool,
        &audit::AuditQuery {
            workspace_id: params.workspace,
            principal: params.principal,
            action_exact,
            action_prefix,
            resource: params.resource,
            from,
            to,
            before_seq: params.before,
            limit,
        },
    )
    .await?;

    // A full page may have more behind it; a short page is the last one.
    let next_cursor = if entries.len() == usize::try_from(limit).unwrap_or(usize::MAX) {
        entries.last().map(|record| record.seq)
    } else {
        None
    };

    Ok(Json(AuditQueryResponse {
        entries: entries.into_iter().map(AuditEntryResponse::from).collect(),
        next_cursor,
    }))
}

/// Response body for `GET /api/v2/audit/verify`.
#[derive(Debug, Serialize)]
pub struct VerifyChainResponse {
    /// Entries whose linkage and hash recomputed correctly (the entries
    /// *before* the break, when invalid).
    pub entries_checked: u64,
    /// Whether the whole chain verified.
    pub valid: bool,
    /// Chain position of the first broken entry; absent when valid.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub broken_at: Option<i64>,
    /// Description of the break; absent when valid.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub error: Option<String>,
}

/// `GET /api/v2/audit/verify` — recompute the whole hash chain.
///
/// The verification itself is audited (action `audit.verify`), so the log
/// records who checked it and what they saw. The new entry is appended
/// *after* the walk and is therefore not part of `entries_checked`.
pub async fn verify_audit_chain(
    State(state): State<AppState>,
    Extension(principal): Extension<Principal>,
) -> Result<Json<VerifyChainResponse>, ApiError> {
    require_management(&state.pool, &principal).await?;

    let report = audit::verify_chain_report(&state.pool).await?;
    let valid = report.valid();

    audit::append(
        &state.pool,
        NewAuditEntry {
            // The chain is workspace-spanning, so verification is
            // org-level.
            workspace_id: None,
            principal: principal.audit_string(),
            action: "audit.verify".to_owned(),
            resource: "audit:chain".to_owned(),
            details: json!({
                "entries_checked": report.entries_checked,
                "valid": valid,
            }),
        },
    )
    .await?;

    Ok(Json(VerifyChainResponse {
        entries_checked: report.entries_checked,
        valid,
        broken_at: report.broken_at,
        error: report.error,
    }))
}
