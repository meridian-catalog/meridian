//! Management API for catalog federation (Pillar B): mirror CRUD, per-mirror
//! sync status + sync-now, and the cross-catalog sprawl summary.
//!
//! A *mirror* is a registered pointer to an external catalog (another IRC
//! endpoint or an AWS Glue Data Catalog) that Meridian tracks for sprawl
//! visibility and zero-copy register. The actual sync engine lives in the
//! federation crate/worker; these handlers manage the mirror configs and read
//! the state that worker writes.
//!
//! Authorization: every endpoint here requires management access (the admin
//! role or any `MANAGE_WAREHOUSE` grant) — the same bar as warehouse CRUD and
//! the fleet health summary, since federation spans the whole workspace.
//!
//! INTEGRATION NOTE (federation crate): `sync_now` records a `running` sync
//! run and (if a sync-trigger hook is available) kicks the worker. Until the
//! worker is wired, `sync_now` marks the mirror as syncing and returns; the
//! worker is expected to call `meridian_store::federation::record_sync_result`
//! on completion. See the module docs in `meridian_store::federation`.

use std::collections::BTreeMap;

use axum::extract::{Path, Query, State};
use axum::http::StatusCode;
use axum::{Extension, Json};
use chrono::{DateTime, Utc};
use meridian_common::MeridianError;
use meridian_common::principal::Principal;
use meridian_store::federation::{self, MirrorRecord, MirrorUpdate, NewMirror, SyncRunRecord};
use meridian_store::tenancy;
use serde::{Deserialize, Serialize};
use serde_json::{Value, json};

use crate::AppState;
use crate::error::ApiError;
use crate::routes::grants::require_management;

/// Longest accepted mirror name.
const MAX_NAME_LEN: usize = 100;
/// Default staleness threshold for the sprawl summary: a mirror not synced in
/// this many seconds is reported as stale. 24h.
const DEFAULT_STALE_THRESHOLD_S: i64 = 86_400;
/// Default number of sync-history entries returned per mirror.
const DEFAULT_HISTORY_LIMIT: i64 = 20;
/// Cap on sync-history entries.
const MAX_HISTORY_LIMIT: i64 = 200;

// ---------------------------------------------------------------------------
// Wire types
// ---------------------------------------------------------------------------

/// Request body for `POST /api/v2/mirrors`.
#[derive(Debug, Deserialize)]
pub struct CreateMirrorRequest {
    /// Operator-facing handle, unique per workspace.
    pub name: String,
    /// Source kind: `iceberg-rest` | `glue`.
    pub kind: String,
    /// Connection endpoint (IRC base URI, or AWS region for Glue).
    pub endpoint: String,
    /// Remote catalog id within the endpoint, when applicable.
    #[serde(default)]
    pub remote_catalog: Option<String>,
    /// Non-secret connection options.
    #[serde(default)]
    pub config: BTreeMap<String, String>,
    /// Whether the mirror is enabled for syncing (default true).
    #[serde(default = "default_enabled")]
    pub enabled: bool,
    /// Desired sync cadence in seconds (default 3600).
    #[serde(default = "default_sync_interval")]
    pub sync_interval_s: i32,
}

fn default_enabled() -> bool {
    true
}
fn default_sync_interval() -> i32 {
    3600
}

/// Request body for `PATCH /api/v2/mirrors/{name}`. Absent fields are left
/// untouched.
#[derive(Debug, Deserialize)]
// `remote_catalog` is intentionally `Option<Option<String>>`: outer `None` =
// absent (unchanged), `Some(None)` = null (clear), `Some(Some)` = set.
#[allow(clippy::option_option)]
pub struct UpdateMirrorRequest {
    /// New endpoint, if changing.
    #[serde(default)]
    pub endpoint: Option<String>,
    /// New remote catalog id. Send an explicit `null` to clear it.
    #[serde(default, deserialize_with = "double_option")]
    pub remote_catalog: Option<Option<String>>,
    /// New config (replaces the whole map), if changing.
    #[serde(default)]
    pub config: Option<BTreeMap<String, String>>,
    /// New enabled flag, if changing.
    #[serde(default)]
    pub enabled: Option<bool>,
    /// New sync interval, if changing.
    #[serde(default)]
    pub sync_interval_s: Option<i32>,
}

/// Distinguishes "field absent" from "field present and null" for the
/// clearable `remote_catalog`.
#[allow(clippy::option_option)]
fn double_option<'de, D>(deserializer: D) -> Result<Option<Option<String>>, D::Error>
where
    D: serde::Deserializer<'de>,
{
    Ok(Some(Option::<String>::deserialize(deserializer)?))
}

/// A mirror as rendered by the management API. Secret config values are
/// redacted (`***`) on read, mirroring the warehouse convention.
#[derive(Debug, Serialize)]
pub struct MirrorResponse {
    /// ULID of the mirror.
    pub id: String,
    /// Operator-facing handle.
    pub name: String,
    /// Source kind.
    pub kind: String,
    /// Connection endpoint.
    pub endpoint: String,
    /// Remote catalog id, when set.
    pub remote_catalog: Option<String>,
    /// Non-secret connection options (secrets redacted).
    pub config: BTreeMap<String, String>,
    /// Whether the mirror is enabled for syncing.
    pub enabled: bool,
    /// Desired sync cadence in seconds.
    pub sync_interval_s: i32,
    /// Last sync time; `null` = never synced.
    pub last_synced_at: Option<DateTime<Utc>>,
    /// Most recent sync status (`ok` | `error` | `running`), or `null`.
    pub last_sync_status: Option<String>,
    /// Most recent sync detail, or `null`.
    pub last_sync_detail: Option<String>,
    /// Assets discovered on the most recent successful sync.
    pub asset_count: i64,
    /// Creation time.
    pub created_at: DateTime<Utc>,
    /// Last update time.
    pub updated_at: DateTime<Utc>,
}

/// Config keys treated as secret material and redacted on read.
const SECRET_CONFIG_KEYS: &[&str] = &[
    "token",
    "credential",
    "access-key-id",
    "secret-access-key",
    "session-token",
    "client-secret",
    "password",
];

impl From<MirrorRecord> for MirrorResponse {
    fn from(r: MirrorRecord) -> Self {
        let config = r
            .config
            .0
            .into_iter()
            .map(|(k, v)| {
                if SECRET_CONFIG_KEYS.contains(&k.as_str()) {
                    (k, "***".to_owned())
                } else {
                    (k, v)
                }
            })
            .collect();
        Self {
            id: r.id,
            name: r.name,
            kind: r.kind,
            endpoint: r.endpoint,
            remote_catalog: r.remote_catalog,
            config,
            enabled: r.enabled,
            sync_interval_s: r.sync_interval_s,
            last_synced_at: r.last_synced_at,
            last_sync_status: r.last_sync_status,
            last_sync_detail: r.last_sync_detail,
            asset_count: r.asset_count,
            created_at: r.created_at,
            updated_at: r.updated_at,
        }
    }
}

/// Response body for `GET /api/v2/mirrors`.
#[derive(Debug, Serialize)]
pub struct ListMirrorsResponse {
    /// All mirrors in the workspace, ordered by name.
    pub mirrors: Vec<MirrorResponse>,
}

/// A sync-history entry as rendered by the API.
#[derive(Debug, Serialize)]
pub struct SyncRunResponse {
    /// ULID of the run.
    pub id: String,
    /// Run status.
    pub status: String,
    /// Assets discovered on this run.
    pub assets_seen: i64,
    /// Error or summary detail.
    pub detail: Option<String>,
    /// When the run started.
    pub started_at: DateTime<Utc>,
    /// When the run finished; `null` while running.
    pub finished_at: Option<DateTime<Utc>>,
}

impl From<SyncRunRecord> for SyncRunResponse {
    fn from(r: SyncRunRecord) -> Self {
        Self {
            id: r.id,
            status: r.status,
            assets_seen: r.assets_seen,
            detail: r.detail,
            started_at: r.started_at,
            finished_at: r.finished_at,
        }
    }
}

// ---------------------------------------------------------------------------
// Validation
// ---------------------------------------------------------------------------

/// Validates a mirror name: 1–100 chars from `[A-Za-z0-9._-]`.
fn validate_name(name: &str) -> Result<(), ApiError> {
    if name.is_empty() || name.len() > MAX_NAME_LEN {
        return Err(ApiError::bad_request(format!(
            "mirror name must be 1–{MAX_NAME_LEN} characters"
        )));
    }
    if !name
        .chars()
        .all(|c| c.is_ascii_alphanumeric() || matches!(c, '.' | '_' | '-'))
    {
        return Err(ApiError::bad_request(
            "mirror name may only contain letters, digits, '.', '_' and '-'",
        ));
    }
    Ok(())
}

/// Validates the source kind against the accepted set.
fn validate_kind(kind: &str) -> Result<(), ApiError> {
    if federation::MIRROR_KINDS.contains(&kind) {
        Ok(())
    } else {
        Err(ApiError::bad_request(format!(
            "mirror kind must be one of {:?}",
            federation::MIRROR_KINDS
        )))
    }
}

/// Maps store errors of the federation API onto the HTTP boundary.
fn mirror_error(name: &str, error: MeridianError) -> ApiError {
    match error {
        MeridianError::Conflict(message) => ApiError::already_exists(message),
        MeridianError::NotFound(_) => ApiError::new(
            StatusCode::NOT_FOUND,
            "NoSuchMirrorException",
            format!("mirror {name:?} does not exist"),
        ),
        other => ApiError::from(other),
    }
}

// ---------------------------------------------------------------------------
// Handlers — mirror CRUD
// ---------------------------------------------------------------------------

/// `POST /api/v2/mirrors` — register a mirror.
pub async fn create_mirror(
    State(state): State<AppState>,
    Extension(principal): Extension<Principal>,
    Json(request): Json<CreateMirrorRequest>,
) -> Result<(StatusCode, Json<MirrorResponse>), ApiError> {
    require_management(&state.pool, &principal).await?;
    validate_name(&request.name)?;
    validate_kind(&request.kind)?;
    if request.endpoint.trim().is_empty() {
        return Err(ApiError::bad_request("endpoint must not be empty"));
    }
    if request.sync_interval_s <= 0 {
        return Err(ApiError::bad_request("sync_interval_s must be positive"));
    }

    let record = federation::create(
        &state.pool,
        tenancy::default_workspace_id(),
        NewMirror {
            name: request.name.clone(),
            kind: request.kind,
            endpoint: request.endpoint,
            remote_catalog: request.remote_catalog,
            config: request.config,
            enabled: request.enabled,
            sync_interval_s: request.sync_interval_s,
        },
        &principal.audit_string(),
    )
    .await
    .map_err(|e| mirror_error(&request.name, e))?;

    Ok((StatusCode::CREATED, Json(record.into())))
}

/// `GET /api/v2/mirrors` — list registered mirrors.
pub async fn list_mirrors(
    State(state): State<AppState>,
    Extension(principal): Extension<Principal>,
) -> Result<Json<ListMirrorsResponse>, ApiError> {
    require_management(&state.pool, &principal).await?;
    let mirrors = federation::list(&state.pool, tenancy::default_workspace_id())
        .await?
        .into_iter()
        .map(MirrorResponse::from)
        .collect();
    Ok(Json(ListMirrorsResponse { mirrors }))
}

/// `GET /api/v2/mirrors/{name}` — fetch one mirror.
pub async fn get_mirror(
    State(state): State<AppState>,
    Extension(principal): Extension<Principal>,
    Path(name): Path<String>,
) -> Result<Json<MirrorResponse>, ApiError> {
    require_management(&state.pool, &principal).await?;
    let record = federation::get_by_name(&state.pool, tenancy::default_workspace_id(), &name)
        .await?
        .ok_or_else(|| {
            ApiError::new(
                StatusCode::NOT_FOUND,
                "NoSuchMirrorException",
                format!("mirror {name:?} does not exist"),
            )
        })?;
    Ok(Json(record.into()))
}

/// `PATCH /api/v2/mirrors/{name}` — update a mirror config.
pub async fn update_mirror(
    State(state): State<AppState>,
    Extension(principal): Extension<Principal>,
    Path(name): Path<String>,
    Json(request): Json<UpdateMirrorRequest>,
) -> Result<Json<MirrorResponse>, ApiError> {
    require_management(&state.pool, &principal).await?;
    if let Some(endpoint) = &request.endpoint
        && endpoint.trim().is_empty()
    {
        return Err(ApiError::bad_request("endpoint must not be empty"));
    }
    if let Some(interval) = request.sync_interval_s
        && interval <= 0
    {
        return Err(ApiError::bad_request("sync_interval_s must be positive"));
    }

    let update = MirrorUpdate {
        endpoint: request.endpoint,
        remote_catalog: request.remote_catalog,
        config: request.config,
        enabled: request.enabled,
        sync_interval_s: request.sync_interval_s,
    };
    if update.is_empty() {
        return Err(ApiError::bad_request("no fields to update"));
    }

    let record = federation::update_by_name(
        &state.pool,
        tenancy::default_workspace_id(),
        &name,
        update,
        &principal.audit_string(),
    )
    .await
    .map_err(|e| mirror_error(&name, e))?;

    Ok(Json(record.into()))
}

/// `DELETE /api/v2/mirrors/{name}` — deregister a mirror.
pub async fn delete_mirror(
    State(state): State<AppState>,
    Extension(principal): Extension<Principal>,
    Path(name): Path<String>,
) -> Result<StatusCode, ApiError> {
    require_management(&state.pool, &principal).await?;
    federation::delete_by_name(
        &state.pool,
        tenancy::default_workspace_id(),
        &name,
        &principal.audit_string(),
    )
    .await
    .map_err(|e| mirror_error(&name, e))?;
    Ok(StatusCode::NO_CONTENT)
}

// ---------------------------------------------------------------------------
// Handlers — sync status + sync-now
// ---------------------------------------------------------------------------

/// Query params for the sync-status endpoint.
#[derive(Debug, Deserialize)]
pub struct SyncStatusQuery {
    /// Number of history entries to return.
    #[serde(default)]
    pub limit: Option<i64>,
}

/// `GET /api/v2/mirrors/{name}/sync` — the mirror's current status plus recent
/// sync-run history.
pub async fn get_sync_status(
    State(state): State<AppState>,
    Extension(principal): Extension<Principal>,
    Path(name): Path<String>,
    Query(query): Query<SyncStatusQuery>,
) -> Result<Json<Value>, ApiError> {
    require_management(&state.pool, &principal).await?;
    let mirror = federation::get_by_name(&state.pool, tenancy::default_workspace_id(), &name)
        .await?
        .ok_or_else(|| {
            ApiError::new(
                StatusCode::NOT_FOUND,
                "NoSuchMirrorException",
                format!("mirror {name:?} does not exist"),
            )
        })?;

    let limit = query
        .limit
        .unwrap_or(DEFAULT_HISTORY_LIMIT)
        .clamp(1, MAX_HISTORY_LIMIT);
    let history: Vec<SyncRunResponse> = federation::sync_history(&state.pool, &mirror.id, limit)
        .await?
        .into_iter()
        .map(SyncRunResponse::from)
        .collect();

    Ok(Json(json!({
        "mirror": MirrorResponse::from(mirror),
        "history": history,
    })))
}

/// `POST /api/v2/mirrors/{name}/sync` — run an immediate sync of the mirror.
///
/// Runs the federation sync engine synchronously (walk the source, materialize
/// its tables as foreign read-only assets, remove ones that vanished, record
/// the run) and returns a summary of what the run changed. Returns 404 if the
/// mirror does not exist and 409 if it is disabled (a disabled mirror is
/// intentionally not synced). The request timeout bounds a long sync; the
/// scheduled worker also picks the mirror up on its own cadence.
pub async fn sync_now(
    State(state): State<AppState>,
    Extension(principal): Extension<Principal>,
    Path(name): Path<String>,
) -> Result<Json<Value>, ApiError> {
    require_management(&state.pool, &principal).await?;

    let run = meridian_federation::sync_mirror_now(&state.pool, &name, &state.config.federation)
        .await
        .map_err(|e| mirror_error(&name, e))?;

    // Return the refreshed mirror (so callers see the new status/counts) plus a
    // summary of what this run changed.
    let mirror = federation::get_by_name(&state.pool, tenancy::default_workspace_id(), &name)
        .await?
        .ok_or_else(|| {
            ApiError::new(
                StatusCode::NOT_FOUND,
                "NoSuchMirrorException",
                format!("mirror {name:?} does not exist"),
            )
        })?;
    Ok(Json(json!({
        "mirror": MirrorResponse::from(mirror),
        "synced": {
            "namespaces_seen": run.namespaces_seen,
            "tables_seen": run.tables_seen,
            "tables_inserted": run.tables_inserted,
            "tables_updated": run.tables_updated,
            "tables_unchanged": run.tables_unchanged,
            "tables_removed": run.tables_removed,
            "tables_failed": run.tables_failed,
        },
    })))
}

// ---------------------------------------------------------------------------
// Handler — sprawl summary
// ---------------------------------------------------------------------------

/// Query params for the sprawl summary.
#[derive(Debug, Deserialize)]
pub struct SprawlQuery {
    /// Staleness threshold in seconds; a mirror not synced within this window
    /// is reported as stale. Default 86400 (24h).
    #[serde(default)]
    pub stale_threshold_s: Option<i64>,
}

/// `GET /api/v2/federation/sprawl` — the cross-catalog sprawl summary.
///
/// Rolls up across every catalog Meridian knows about (its own warehouses and
/// registered mirrors): per-source asset counts, duplicate/overlap detection
/// (a storage location registered in more than one source), staleness,
/// ownership gaps, and a health roll-up over the indexed native assets.
pub async fn get_sprawl(
    State(state): State<AppState>,
    Extension(principal): Extension<Principal>,
    Query(query): Query<SprawlQuery>,
) -> Result<Json<Value>, ApiError> {
    require_management(&state.pool, &principal).await?;
    let threshold = query
        .stale_threshold_s
        .unwrap_or(DEFAULT_STALE_THRESHOLD_S)
        .max(0);

    let summary =
        federation::sprawl_summary(&state.pool, tenancy::default_workspace_id(), threshold).await?;

    // Serialize the store struct directly — its Serialize derive produces the
    // wire shape (snake_case fields, RFC3339 timestamps).
    Ok(Json(json!({
        "stale_threshold_s": threshold,
        "source_count": summary.source_count,
        "warehouse_count": summary.warehouse_count,
        "mirror_count": summary.mirror_count,
        "total_assets": summary.total_assets,
        "sources": summary.sources,
        "duplicates": summary.duplicates,
        "duplicate_count": summary.duplicate_count,
        "duplicates_truncated": summary.duplicates_truncated,
        "stale_mirrors": summary.stale_mirrors,
        "ownership_gaps": summary.ownership_gaps,
        "owned_mirror_assets": summary.owned_mirror_assets,
        "health": summary.health,
    })))
}
