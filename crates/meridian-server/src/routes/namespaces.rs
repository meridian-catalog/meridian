//! Iceberg REST catalog namespace endpoints.
//!
//! Mounted under both `/iceberg/v1/{prefix}` and `/v1/{prefix}`. The
//! `{prefix}` is a warehouse name (one warehouse = one IRC prefix); the
//! `{namespace}` path parameter encodes multi-level namespaces with the
//! `0x1F` unit separator (`%1F` in URLs) per the REST spec.
//!
//! Authorization (full mapping in the `crate::routes::grants` module
//! docs): listing/reading namespaces needs `LIST_NAMESPACES` on the
//! warehouse; creating needs `CREATE_NAMESPACE` on the warehouse; dropping
//! and property updates need `MANAGE_NAMESPACE` on the namespace (or an
//! ancestor, via hierarchy inheritance).

use std::collections::BTreeMap;
use std::str::FromStr;

use axum::extract::{Path, Query, State};
use axum::http::StatusCode;
use axum::{Extension, Json};
use meridian_common::MeridianError;
use meridian_common::principal::Principal;
use meridian_store::rbac::{Privilege, SecurableScope};
use meridian_store::warehouse::WarehouseRecord;
use meridian_store::{namespace, tenancy, warehouse};
use serde::{Deserialize, Serialize};
use sqlx::PgPool;
use ulid::Ulid;

use crate::AppState;
use crate::error::ApiError;
use crate::routes::grants::{namespace_scope_chain, require};

/// The multi-level namespace separator in URLs: the `0x1F` unit separator.
const UNIT_SEPARATOR: char = '\u{1f}';

/// Page size used when a client paginates without an explicit `pageSize`.
const DEFAULT_PAGE_SIZE: i64 = 100;

/// Hard upper bound on a single page.
const MAX_PAGE_SIZE: i64 = 1000;

/// Resolves an IRC `{prefix}` (a warehouse name) to its warehouse, or 404
/// `NoSuchWarehouseException`.
pub(crate) async fn resolve_warehouse(
    pool: &PgPool,
    prefix: &str,
) -> Result<WarehouseRecord, ApiError> {
    warehouse::get_by_name(pool, tenancy::default_workspace_id(), prefix)
        .await?
        .ok_or_else(|| ApiError::no_such_warehouse(prefix))
}

/// Validates namespace levels: at least one level, no empty level, no level
/// containing the unit separator (it could never be addressed in a URL).
fn validate_levels(levels: &[String]) -> Result<(), ApiError> {
    if levels.is_empty() {
        return Err(ApiError::bad_request(
            "namespace must have at least one level",
        ));
    }
    for level in levels {
        if level.is_empty() {
            return Err(ApiError::bad_request("namespace levels must be non-empty"));
        }
        if level.contains(UNIT_SEPARATOR) {
            return Err(ApiError::bad_request(
                "namespace levels must not contain the 0x1F unit separator",
            ));
        }
    }
    Ok(())
}

/// Decodes a `{namespace}` path parameter (or `parent` query parameter) into
/// levels by splitting on the unit separator.
pub(crate) fn decode_namespace_param(raw: &str) -> Result<Vec<String>, ApiError> {
    let levels: Vec<String> = raw.split(UNIT_SEPARATOR).map(str::to_owned).collect();
    validate_levels(&levels)?;
    Ok(levels)
}

/// Encodes/decodes the opaque pagination token.
///
/// The token is the hex rendering of the last row's ULID — opaque to
/// clients, cheap to verify, and stable under concurrent inserts (keyset
/// pagination never skips or repeats rows that existed when their page was
/// read).
pub(crate) fn encode_page_token(last_id: &str) -> String {
    hex::encode(last_id.as_bytes())
}

pub(crate) fn decode_page_token(token: &str) -> Result<String, ApiError> {
    let invalid = || ApiError::bad_request("invalid pageToken");
    let bytes = hex::decode(token).map_err(|_| invalid())?;
    let id = String::from_utf8(bytes).map_err(|_| invalid())?;
    Ulid::from_str(&id).map_err(|_| invalid())?;
    Ok(id)
}

/// Resolved pagination inputs for a list endpoint.
///
/// Pagination engages when the client signals it (a `pageToken` — possibly
/// empty — or a `pageSize`); otherwise the spec requires all results in one
/// response with a `null` next-page-token.
#[derive(Debug)]
pub(crate) struct Pagination {
    /// Page bound (`None` disables pagination).
    pub(crate) limit: Option<i64>,
    /// Keyset cursor: the id of the last row of the previous page.
    pub(crate) after_id: Option<String>,
}

pub(crate) fn resolve_pagination(
    page_token: Option<&str>,
    page_size: Option<i64>,
) -> Result<Pagination, ApiError> {
    if page_token.is_none() && page_size.is_none() {
        return Ok(Pagination {
            limit: None,
            after_id: None,
        });
    }
    let size = page_size.unwrap_or(DEFAULT_PAGE_SIZE);
    if size < 1 {
        return Err(ApiError::bad_request("pageSize must be at least 1"));
    }
    let after_id = match page_token.filter(|t| !t.is_empty()) {
        Some(token) => Some(decode_page_token(token)?),
        None => None,
    };
    Ok(Pagination {
        limit: Some(size.min(MAX_PAGE_SIZE)),
        after_id,
    })
}

/// Truncates an over-fetched page (`limit + 1` rows requested) and returns
/// the continuation token when another page exists.
pub(crate) fn next_page_token<T>(
    rows: &mut Vec<T>,
    limit: Option<i64>,
    id_of: impl Fn(&T) -> &str,
) -> Option<String> {
    let size = usize::try_from(limit?).ok()?;
    if rows.len() > size {
        rows.truncate(size);
        rows.last().map(|row| encode_page_token(id_of(row)))
    } else {
        None
    }
}

/// Query parameters for `GET /{prefix}/namespaces`.
#[derive(Debug, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct ListNamespacesQuery {
    /// Optional parent namespace to list underneath (unit-separator encoded).
    pub parent: Option<String>,
    /// Opaque continuation token from a previous response.
    pub page_token: Option<String>,
    /// Upper bound on the number of results.
    pub page_size: Option<i64>,
}

/// `ListNamespacesResponse` from the IRC spec.
#[derive(Debug, Serialize)]
pub struct ListNamespacesResponse {
    /// Namespaces at the requested level.
    pub namespaces: Vec<Vec<String>>,
    /// Continuation token; `null` signals the end of the listing.
    #[serde(rename = "next-page-token")]
    pub next_page_token: Option<String>,
}

/// `GET /{prefix}/namespaces` — list namespaces one level below `parent`
/// (top-level namespaces when `parent` is absent).
pub async fn list_namespaces(
    State(state): State<AppState>,
    Extension(principal): Extension<Principal>,
    Path(prefix): Path<String>,
    Query(query): Query<ListNamespacesQuery>,
) -> Result<Json<ListNamespacesResponse>, ApiError> {
    let wh = resolve_warehouse(&state.pool, &prefix).await?;
    require(
        &state.pool,
        &principal,
        Privilege::ListNamespaces,
        &SecurableScope::warehouse(&wh.id),
    )
    .await?;

    // Per the spec, an empty `parent` is treated as absent.
    let parent: Vec<String> = match query.parent.as_deref().filter(|p| !p.is_empty()) {
        Some(raw) => {
            let levels = decode_namespace_param(raw)?;
            if namespace::get(&state.pool, &wh.id, &levels)
                .await?
                .is_none()
            {
                return Err(ApiError::no_such_namespace(format!(
                    "parent namespace {:?} does not exist",
                    levels.join(".")
                )));
            }
            levels
        }
        None => Vec::new(),
    };

    let pagination = resolve_pagination(query.page_token.as_deref(), query.page_size)?;

    // Fetch one extra row to learn whether another page exists.
    let fetch_limit = pagination.limit.map(|l| l + 1);
    let mut rows = namespace::list(
        &state.pool,
        &wh.id,
        &parent,
        pagination.after_id.as_deref(),
        fetch_limit,
    )
    .await?;

    let next_page_token = next_page_token(&mut rows, pagination.limit, |r| &r.id);

    Ok(Json(ListNamespacesResponse {
        namespaces: rows.into_iter().map(|r| r.levels).collect(),
        next_page_token,
    }))
}

/// `CreateNamespaceRequest` from the IRC spec.
#[derive(Debug, Deserialize)]
pub struct CreateNamespaceRequest {
    /// Namespace levels, outermost first.
    pub namespace: Vec<String>,
    /// Initial string properties.
    #[serde(default)]
    pub properties: BTreeMap<String, String>,
}

/// `CreateNamespaceResponse` / `GetNamespaceResponse` shape.
#[derive(Debug, Serialize)]
pub struct NamespaceResponse {
    /// Namespace levels, outermost first.
    pub namespace: Vec<String>,
    /// Stored string properties.
    pub properties: BTreeMap<String, String>,
}

/// `POST /{prefix}/namespaces` — create a namespace with optional properties.
pub async fn create_namespace(
    State(state): State<AppState>,
    Extension(principal): Extension<Principal>,
    Path(prefix): Path<String>,
    Json(request): Json<CreateNamespaceRequest>,
) -> Result<Json<NamespaceResponse>, ApiError> {
    let wh = resolve_warehouse(&state.pool, &prefix).await?;
    require(
        &state.pool,
        &principal,
        Privilege::CreateNamespace,
        &SecurableScope::warehouse(&wh.id),
    )
    .await?;
    validate_levels(&request.namespace)?;

    let record = namespace::create(
        &state.pool,
        tenancy::default_workspace_id(),
        &wh.id,
        &request.namespace,
        request.properties,
        &principal.audit_string(),
    )
    .await
    .map_err(|e| match e {
        MeridianError::Conflict(message) => ApiError::already_exists(message),
        MeridianError::NotFound(message) => ApiError::no_such_namespace(message),
        other => ApiError::from(other),
    })?;

    Ok(Json(NamespaceResponse {
        namespace: record.levels,
        properties: record.properties.0,
    }))
}

/// `GET /{prefix}/namespaces/{namespace}` — load namespace properties.
pub async fn load_namespace(
    State(state): State<AppState>,
    Extension(principal): Extension<Principal>,
    Path((prefix, raw_namespace)): Path<(String, String)>,
) -> Result<Json<NamespaceResponse>, ApiError> {
    let wh = resolve_warehouse(&state.pool, &prefix).await?;
    require(
        &state.pool,
        &principal,
        Privilege::ListNamespaces,
        &SecurableScope::warehouse(&wh.id),
    )
    .await?;
    let levels = decode_namespace_param(&raw_namespace)?;

    let record = namespace::get(&state.pool, &wh.id, &levels)
        .await?
        .ok_or_else(|| {
            ApiError::no_such_namespace(format!("namespace {:?} does not exist", levels.join(".")))
        })?;

    Ok(Json(NamespaceResponse {
        namespace: record.levels,
        properties: record.properties.0,
    }))
}

/// `HEAD /{prefix}/namespaces/{namespace}` — existence check (204/404).
pub async fn namespace_exists(
    State(state): State<AppState>,
    Extension(principal): Extension<Principal>,
    Path((prefix, raw_namespace)): Path<(String, String)>,
) -> Result<StatusCode, ApiError> {
    let wh = resolve_warehouse(&state.pool, &prefix).await?;
    require(
        &state.pool,
        &principal,
        Privilege::ListNamespaces,
        &SecurableScope::warehouse(&wh.id),
    )
    .await?;
    let levels = decode_namespace_param(&raw_namespace)?;

    if namespace::get(&state.pool, &wh.id, &levels)
        .await?
        .is_none()
    {
        return Err(ApiError::no_such_namespace(format!(
            "namespace {:?} does not exist",
            levels.join(".")
        )));
    }
    Ok(StatusCode::NO_CONTENT)
}

/// `DELETE /{prefix}/namespaces/{namespace}` — drop an empty namespace.
pub async fn drop_namespace(
    State(state): State<AppState>,
    Extension(principal): Extension<Principal>,
    Path((prefix, raw_namespace)): Path<(String, String)>,
) -> Result<StatusCode, ApiError> {
    let wh = resolve_warehouse(&state.pool, &prefix).await?;
    let levels = decode_namespace_param(&raw_namespace)?;
    let chain = namespace_scope_chain(&state.pool, &wh.id, &levels).await?;
    require(
        &state.pool,
        &principal,
        Privilege::ManageNamespace,
        &SecurableScope::namespace(&wh.id, chain),
    )
    .await?;

    namespace::delete(
        &state.pool,
        tenancy::default_workspace_id(),
        &wh.id,
        &levels,
        &principal.audit_string(),
    )
    .await
    .map_err(|e| match e {
        MeridianError::NotFound(message) => ApiError::no_such_namespace(message),
        MeridianError::Conflict(message) => ApiError::namespace_not_empty(message),
        other => ApiError::from(other),
    })?;

    Ok(StatusCode::NO_CONTENT)
}

/// `UpdateNamespacePropertiesRequest` from the IRC spec.
#[derive(Debug, Deserialize)]
pub struct UpdateNamespacePropertiesRequest {
    /// Property keys to remove.
    #[serde(default)]
    pub removals: Vec<String>,
    /// Property keys to set.
    #[serde(default)]
    pub updates: BTreeMap<String, String>,
}

/// `UpdateNamespacePropertiesResponse` from the IRC spec.
#[derive(Debug, Serialize)]
pub struct UpdateNamespacePropertiesResponse {
    /// Keys added or updated.
    pub updated: Vec<String>,
    /// Keys removed.
    pub removed: Vec<String>,
    /// Keys requested for removal that were not present.
    pub missing: Vec<String>,
}

/// `POST /{prefix}/namespaces/{namespace}/properties` — set and/or remove
/// properties atomically. A key present in both `updates` and `removals` is
/// a 422 `UnprocessableEntityException`.
pub async fn update_namespace_properties(
    State(state): State<AppState>,
    Extension(principal): Extension<Principal>,
    Path((prefix, raw_namespace)): Path<(String, String)>,
    Json(request): Json<UpdateNamespacePropertiesRequest>,
) -> Result<Json<UpdateNamespacePropertiesResponse>, ApiError> {
    let wh = resolve_warehouse(&state.pool, &prefix).await?;
    let levels = decode_namespace_param(&raw_namespace)?;
    let chain = namespace_scope_chain(&state.pool, &wh.id, &levels).await?;
    require(
        &state.pool,
        &principal,
        Privilege::ManageNamespace,
        &SecurableScope::namespace(&wh.id, chain),
    )
    .await?;

    if let Some(key) = request
        .removals
        .iter()
        .find(|k| request.updates.contains_key(*k))
    {
        return Err(ApiError::unprocessable(format!(
            "property key {key:?} is present in both updates and removals"
        )));
    }

    let outcome = namespace::update_properties(
        &state.pool,
        tenancy::default_workspace_id(),
        &wh.id,
        &levels,
        request.updates,
        request.removals,
        &principal.audit_string(),
    )
    .await
    .map_err(|e| match e {
        MeridianError::NotFound(message) => ApiError::no_such_namespace(message),
        other => ApiError::from(other),
    })?;

    Ok(Json(UpdateNamespacePropertiesResponse {
        updated: outcome.updated,
        removed: outcome.removed,
        missing: outcome.missing,
    }))
}
