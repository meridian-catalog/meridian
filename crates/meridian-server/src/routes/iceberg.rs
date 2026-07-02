//! Iceberg REST catalog configuration endpoint.
//!
//! `GET /v1/config` (and the `/iceberg/v1` alias). The namespace surface
//! lives in [`super::namespaces`].

use std::collections::BTreeMap;

use axum::Json;
use axum::extract::{Query, State};
use serde::{Deserialize, Serialize};

use crate::AppState;
use crate::error::ApiError;
use crate::routes::namespaces::resolve_warehouse;

/// The endpoints this server implements, in the spec's endpoint-identifier
/// syntax. Sent in every config response so clients do not assume the
/// spec's larger default set.
const IMPLEMENTED_ENDPOINTS: &[&str] = &[
    "GET /v1/{prefix}/namespaces",
    "POST /v1/{prefix}/namespaces",
    "GET /v1/{prefix}/namespaces/{namespace}",
    "HEAD /v1/{prefix}/namespaces/{namespace}",
    "DELETE /v1/{prefix}/namespaces/{namespace}",
    "POST /v1/{prefix}/namespaces/{namespace}/properties",
];

/// The Iceberg REST `ConfigResponse`: catalog defaults the client should
/// start from, overrides the client must apply, and the endpoint set the
/// server implements.
#[derive(Debug, Default, Serialize, Deserialize)]
pub struct CatalogConfig {
    /// Properties clients should use as defaults.
    pub defaults: BTreeMap<String, String>,
    /// Properties clients must use, overriding client-set values.
    pub overrides: BTreeMap<String, String>,
    /// Endpoint identifiers the server supports.
    #[serde(default)]
    pub endpoints: Vec<String>,
}

/// Query parameters accepted by `GET /v1/config`.
#[derive(Debug, Deserialize)]
pub struct ConfigQuery {
    /// Warehouse name sent by the client; resolves to the catalog `prefix`.
    pub warehouse: Option<String>,
}

/// `GET /iceberg/v1/config` (and alias `GET /v1/config`).
///
/// When a `warehouse` parameter names a registered warehouse, the response
/// instructs the client to use that warehouse's name as its `{prefix}`.
/// An unknown warehouse is a 404 `NoSuchWarehouseException`.
pub async fn get_config(
    State(state): State<AppState>,
    Query(query): Query<ConfigQuery>,
) -> Result<Json<CatalogConfig>, ApiError> {
    let mut overrides = BTreeMap::new();

    if let Some(name) = query.warehouse.as_deref().filter(|w| !w.is_empty()) {
        let wh = resolve_warehouse(&state.pool, name).await?;
        overrides.insert("prefix".to_owned(), wh.name);
    }

    Ok(Json(CatalogConfig {
        defaults: BTreeMap::new(),
        overrides,
        endpoints: IMPLEMENTED_ENDPOINTS
            .iter()
            .map(|s| (*s).to_owned())
            .collect(),
    }))
}
