//! Iceberg REST catalog endpoints.
//!
//! M0 implements only `GET /v1/config`. The namespace/table surface lands in
//! M1.

use std::collections::BTreeMap;

use axum::Json;
use axum::extract::Query;
use serde::{Deserialize, Serialize};

/// The Iceberg REST `ConfigResponse`: catalog defaults the client should
/// start from and overrides the client must apply.
#[derive(Debug, Default, Serialize, Deserialize)]
pub struct CatalogConfig {
    /// Properties clients should use as defaults.
    pub defaults: BTreeMap<String, String>,
    /// Properties clients must use, overriding client-set values.
    pub overrides: BTreeMap<String, String>,
}

/// Query parameters accepted by `GET /v1/config`.
#[derive(Debug, Deserialize)]
pub struct ConfigQuery {
    /// Warehouse location or identifier hint sent by the client.
    pub warehouse: Option<String>,
}

/// `GET /iceberg/v1/config` (and alias `GET /v1/config`).
///
/// TODO(M1): resolve the `warehouse` parameter against registered
/// warehouses and return warehouse-specific defaults/overrides (including
/// the catalog `prefix`). Until then the parameter is accepted and logged
/// but does not influence the response.
pub async fn get_config(Query(query): Query<ConfigQuery>) -> Json<CatalogConfig> {
    if let Some(warehouse) = &query.warehouse {
        tracing::debug!(%warehouse, "warehouse parameter not yet applied to config response");
    }

    Json(CatalogConfig::default())
}
