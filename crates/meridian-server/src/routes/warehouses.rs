//! Management API v0: warehouse CRUD under `/api/v2/warehouses`.
//!
//! A warehouse is a storage root plus non-secret storage options; its name
//! doubles as the Iceberg REST `{prefix}`. This surface is pre-auth (M1):
//! every mutation is audited under the anonymous principal until
//! authentication lands.

use std::collections::BTreeMap;

use axum::Json;
use axum::extract::{Path, State};
use axum::http::StatusCode;
use chrono::{DateTime, Utc};
use meridian_common::MeridianError;
use meridian_store::tenancy;
use meridian_store::warehouse::{self, WarehouseRecord};
use serde::{Deserialize, Serialize};

use crate::AppState;
use crate::error::ApiError;
use crate::routes::ANONYMOUS_PRINCIPAL;

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
    /// Non-secret storage options.
    pub storage_options: BTreeMap<String, String>,
    /// Creation time.
    pub created_at: DateTime<Utc>,
    /// Last update time.
    pub updated_at: DateTime<Utc>,
}

impl From<WarehouseRecord> for WarehouseResponse {
    fn from(record: WarehouseRecord) -> Self {
        Self {
            id: record.id,
            name: record.name,
            storage_root: record.storage_root,
            storage_options: record.storage_config.0,
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

/// `POST /api/v2/warehouses` — register a warehouse.
pub async fn create_warehouse(
    State(state): State<AppState>,
    Json(request): Json<CreateWarehouseRequest>,
) -> Result<(StatusCode, Json<WarehouseResponse>), ApiError> {
    validate_name(&request.name)?;
    if request.storage_root.trim().is_empty() {
        return Err(ApiError::bad_request("storage_root must not be empty"));
    }

    let record = warehouse::create(
        &state.pool,
        tenancy::default_workspace_id(),
        &request.name,
        &request.storage_root,
        request.storage_options,
        ANONYMOUS_PRINCIPAL,
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
) -> Result<Json<ListWarehousesResponse>, ApiError> {
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
    Path(name): Path<String>,
) -> Result<StatusCode, ApiError> {
    warehouse::delete_by_name(
        &state.pool,
        tenancy::default_workspace_id(),
        &name,
        ANONYMOUS_PRINCIPAL,
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
