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
    "GET /v1/{prefix}/namespaces/{namespace}/tables",
    "POST /v1/{prefix}/namespaces/{namespace}/tables",
    "GET /v1/{prefix}/namespaces/{namespace}/tables/{table}",
    "HEAD /v1/{prefix}/namespaces/{namespace}/tables/{table}",
    "POST /v1/{prefix}/namespaces/{namespace}/tables/{table}",
    "DELETE /v1/{prefix}/namespaces/{namespace}/tables/{table}",
    "POST /v1/{prefix}/namespaces/{namespace}/register",
    "POST /v1/{prefix}/namespaces/{namespace}/tables/{table}/metrics",
    "GET /v1/{prefix}/namespaces/{namespace}/tables/{table}/credentials",
    "POST /v1/{prefix}/namespaces/{namespace}/tables/{table}/sign",
    "POST /v1/{prefix}/tables/rename",
    "POST /v1/{prefix}/transactions/commit",
    "GET /v1/{prefix}/namespaces/{namespace}/views",
    "POST /v1/{prefix}/namespaces/{namespace}/views",
    "GET /v1/{prefix}/namespaces/{namespace}/views/{view}",
    "HEAD /v1/{prefix}/namespaces/{namespace}/views/{view}",
    "POST /v1/{prefix}/namespaces/{namespace}/views/{view}",
    "DELETE /v1/{prefix}/namespaces/{namespace}/views/{view}",
    "POST /v1/{prefix}/views/rename",
];

/// Scan-planning endpoints, advertised only while `planning.enabled`
/// (the handlers answer 406 when it is off).
const PLANNING_ENDPOINTS: &[&str] = &[
    "POST /v1/{prefix}/namespaces/{namespace}/tables/{table}/plan",
    "GET /v1/{prefix}/namespaces/{namespace}/tables/{table}/plan/{plan-id}",
    "DELETE /v1/{prefix}/namespaces/{namespace}/tables/{table}/plan/{plan-id}",
    "POST /v1/{prefix}/namespaces/{namespace}/tables/{table}/tasks",
];

/// How long idempotency-key receipts are replayable, advertised to clients
/// (ISO-8601 duration; must match the store's retention).
const IDEMPOTENCY_KEY_LIFETIME: &str = "PT24H";

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
    /// How long `Idempotency-Key` receipts are replayable (ISO-8601
    /// duration), per the spec's config response.
    #[serde(
        rename = "idempotency-key-lifetime",
        skip_serializing_if = "Option::is_none"
    )]
    pub idempotency_key_lifetime: Option<String>,
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
    // Log-only reminder on the endpoint every client calls first; the
    // response body stays exactly the spec's ConfigResponse shape.
    if state.config.auth.mode == meridian_common::config::AuthMode::Disabled {
        tracing::warn!(
            "serving catalog config with authentication DISABLED; every caller is anonymous"
        );
    }

    let mut overrides = BTreeMap::new();

    if let Some(name) = query.warehouse.as_deref().filter(|w| !w.is_empty()) {
        // Branch-as-catalog (K-F2): a `warehouse@branch` warehouse resolves its
        // base warehouse and (when present) its branch/tag, then echoes back the
        // FULL prefix so the client keeps addressing the branch on every
        // subsequent call. This is what lets a plain PyIceberg/Spark/Trino
        // client bootstrap against a branch.
        let (wh, catalog) = super::namespaces::resolve_catalog_ref(&state.pool, name).await?;
        // Echo the full prefix for a branch/tag so the client keeps addressing
        // it; the bare warehouse name otherwise.
        let prefix = if catalog.branch().is_some() {
            name.to_owned()
        } else {
            wh.name
        };
        overrides.insert("prefix".to_owned(), prefix);
    }

    let mut endpoints: Vec<String> = IMPLEMENTED_ENDPOINTS
        .iter()
        .map(|s| (*s).to_owned())
        .collect();
    if state.config.planning.enabled {
        endpoints.extend(PLANNING_ENDPOINTS.iter().map(|s| (*s).to_owned()));
    }

    Ok(Json(CatalogConfig {
        defaults: BTreeMap::new(),
        overrides,
        endpoints,
        idempotency_key_lifetime: Some(IDEMPOTENCY_KEY_LIFETIME.to_owned()),
    }))
}
