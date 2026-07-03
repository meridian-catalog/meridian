//! Management API v0: warehouse CRUD under `/api/v2/warehouses`.
//!
//! A warehouse is a storage root plus non-secret storage options; its name
//! doubles as the Iceberg REST `{prefix}`. Mutations are audited under the
//! caller's principal (from the request extensions; anonymous when
//! authentication is disabled).
//!
//! Authorization (see `crate::routes::grants` for the full policy):
//! creating and listing warehouses requires management access (the admin
//! role or any `MANAGE_WAREHOUSE` grant); deleting a warehouse requires
//! `MANAGE_WAREHOUSE` on that warehouse.

use std::collections::BTreeMap;

use axum::extract::{Path, State};
use axum::http::StatusCode;
use axum::{Extension, Json};
use chrono::{DateTime, Utc};
use meridian_common::MeridianError;
use meridian_common::principal::Principal;
use meridian_store::rbac::{Privilege, SecurableScope};
use meridian_store::tenancy;
use meridian_store::warehouse::{self, WarehouseRecord};
use serde::{Deserialize, Serialize};

use crate::AppState;
use crate::error::ApiError;
use crate::routes::grants::{require, require_management};

/// Longest accepted warehouse name.
const MAX_NAME_LEN: usize = 100;

/// Request body for `POST /api/v2/warehouses`.
#[derive(Debug, Deserialize)]
pub struct CreateWarehouseRequest {
    /// Warehouse name; doubles as the IRC `{prefix}`.
    pub name: String,
    /// Storage root URI, e.g. `s3://bucket/prefix`.
    pub storage_root: String,
    /// Non-secret storage options (region, endpoint, ...).
    #[serde(default)]
    pub storage_options: BTreeMap<String, String>,
}

/// A warehouse as rendered by the management API.
#[derive(Debug, Serialize)]
pub struct WarehouseResponse {
    /// ULID of the warehouse.
    pub id: String,
    /// Warehouse name (the IRC `{prefix}`).
    pub name: String,
    /// Storage root URI.
    pub storage_root: String,
    /// Storage options with secret values redacted (`***`). Secrets can be
    /// written through this API but are never read back, even by admins —
    /// they live in the store for the server's own use only.
    pub storage_options: BTreeMap<String, String>,
    /// Creation time.
    pub created_at: DateTime<Utc>,
    /// Last update time.
    pub updated_at: DateTime<Utc>,
}

/// Storage-option keys whose values are secrets. Kept aligned with the
/// client-config denylist in `routes::views`; redaction here covers the
/// management API, which echoes options back to callers.
const SECRET_OPTION_KEYS: &[&str] = &["access-key-id", "secret-access-key", "session-token"];

impl From<WarehouseRecord> for WarehouseResponse {
    fn from(record: WarehouseRecord) -> Self {
        let storage_options = record
            .storage_config
            .0
            .into_iter()
            .map(|(key, value)| {
                if SECRET_OPTION_KEYS.contains(&key.as_str()) {
                    (key, "***".to_owned())
                } else {
                    (key, value)
                }
            })
            .collect();
        Self {
            id: record.id,
            name: record.name,
            storage_root: record.storage_root,
            storage_options,
            created_at: record.created_at,
            updated_at: record.updated_at,
        }
    }
}

/// Response body for `GET /api/v2/warehouses`.
#[derive(Debug, Serialize)]
pub struct ListWarehousesResponse {
    /// All warehouses in the workspace, ordered by name.
    pub warehouses: Vec<WarehouseResponse>,
}

/// Validates a warehouse name for use as an IRC `{prefix}` path segment:
/// 1–100 characters from `[A-Za-z0-9._-]`.
fn validate_name(name: &str) -> Result<(), ApiError> {
    if name.is_empty() || name.len() > MAX_NAME_LEN {
        return Err(ApiError::bad_request(format!(
            "warehouse name must be 1–{MAX_NAME_LEN} characters"
        )));
    }
    if !name
        .chars()
        .all(|c| c.is_ascii_alphanumeric() || matches!(c, '.' | '_' | '-'))
    {
        return Err(ApiError::bad_request(
            "warehouse name may only contain letters, digits, '.', '_' and '-' \
             (it is used as the catalog URL prefix)",
        ));
    }
    Ok(())
}

/// Validates the vending-related storage options (`vending`, `vending.*`,
/// `endpoint.external`) so a broken vending setup fails here — loudly, at
/// create time — instead of surfacing on the first table load.
fn validate_vending_options(
    storage_root: &str,
    options: &std::collections::BTreeMap<String, String>,
) -> Result<(), ApiError> {
    let config = meridian_vending::VendingConfig::parse(options)
        .map_err(|e| ApiError::bad_request(e.to_string()))?;
    let is_s3 = {
        let root = storage_root.trim().to_ascii_lowercase();
        root.starts_with("s3://") || root.starts_with("s3a://")
    };
    if config.is_enabled() && !is_s3 {
        return Err(ApiError::bad_request(format!(
            "vending = {:?} requires an s3:// storage root; \
             credential vending does not apply to {storage_root:?}",
            config.mode_str(),
        )));
    }
    if config == meridian_vending::VendingConfig::Static
        && meridian_vending::StaticVendor::new(
            options.get("access-key-id").map(String::as_str),
            options.get("secret-access-key").map(String::as_str),
            options.get("session-token").map(String::as_str),
        )
        .is_err()
    {
        return Err(ApiError::bad_request(
            "vending = \"static\" requires access-key-id and secret-access-key \
             in the storage options",
        ));
    }
    if let Some(external) = options.get("endpoint.external")
        && external.trim().is_empty()
    {
        return Err(ApiError::bad_request(
            "endpoint.external must not be blank when set",
        ));
    }
    Ok(())
}

/// `POST /api/v2/warehouses` — register a warehouse.
pub async fn create_warehouse(
    State(state): State<AppState>,
    Extension(principal): Extension<Principal>,
    Json(request): Json<CreateWarehouseRequest>,
) -> Result<(StatusCode, Json<WarehouseResponse>), ApiError> {
    require_management(&state.pool, &principal).await?;
    validate_name(&request.name)?;
    if request.storage_root.trim().is_empty() {
        return Err(ApiError::bad_request("storage_root must not be empty"));
    }
    validate_vending_options(&request.storage_root, &request.storage_options)?;

    let record = warehouse::create(
        &state.pool,
        tenancy::default_workspace_id(),
        &request.name,
        &request.storage_root,
        request.storage_options,
        &principal.audit_string(),
    )
    .await
    .map_err(|e| match e {
        MeridianError::Conflict(message) => ApiError::already_exists(message),
        other => ApiError::from(other),
    })?;

    Ok((StatusCode::CREATED, Json(record.into())))
}

/// `GET /api/v2/warehouses` — list registered warehouses.
pub async fn list_warehouses(
    State(state): State<AppState>,
    Extension(principal): Extension<Principal>,
) -> Result<Json<ListWarehousesResponse>, ApiError> {
    require_management(&state.pool, &principal).await?;
    let warehouses = warehouse::list(&state.pool, tenancy::default_workspace_id())
        .await?
        .into_iter()
        .map(WarehouseResponse::from)
        .collect();
    Ok(Json(ListWarehousesResponse { warehouses }))
}

/// `DELETE /api/v2/warehouses/{name}` — delete an empty warehouse.
pub async fn delete_warehouse(
    State(state): State<AppState>,
    Extension(principal): Extension<Principal>,
    Path(name): Path<String>,
) -> Result<StatusCode, ApiError> {
    let wh = warehouse::get_by_name(&state.pool, tenancy::default_workspace_id(), &name)
        .await?
        .ok_or_else(|| ApiError::no_such_warehouse(&name))?;
    require(
        &state.pool,
        &principal,
        Privilege::ManageWarehouse,
        &SecurableScope::warehouse(&wh.id),
    )
    .await?;

    warehouse::delete_by_name(
        &state.pool,
        tenancy::default_workspace_id(),
        &name,
        &principal.audit_string(),
    )
    .await
    .map_err(|e| match e {
        MeridianError::NotFound(_) => ApiError::no_such_warehouse(&name),
        MeridianError::Conflict(message) => {
            ApiError::new(StatusCode::CONFLICT, "WarehouseNotEmptyException", message)
        }
        other => ApiError::from(other),
    })?;

    Ok(StatusCode::NO_CONTENT)
}
