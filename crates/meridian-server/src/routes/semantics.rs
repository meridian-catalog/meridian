//! Semantics management API (Pillar G, G-F2/G-F3/G-F4), mounted under
//! `/api/v2/metrics`, `/api/v2/glossary`, and `/api/v2/products`.
//!
//! The first-class semantic object model: metrics & semantic models (measures
//! that compile deterministically to a chosen engine's SQL via the sidecar), the
//! business glossary (stewarded terms linked to assets), and certified data
//! products (named bundles that are the unit of consumption for humans and
//! agents). Persistence is [`meridian_store::semantics`]; this is the HTTP +
//! authorization + validation surface over it.
//!
//! Every route is **management-gated** (`require_management`): the semantics
//! layer is workspace-level metadata, curated by data owners/stewards, not
//! per-warehouse RBAC-scoped like the IRC surface. This mirrors the agents and
//! governance management APIs.
//!
//! Metric compilation (G-F2) is the one route that reaches out: it asks the
//! sidecar to compile the stored definition to a requested engine dialect,
//! deterministically (`SQLGlot`), and returns the SQL with its honest status. A
//! sidecar outage there is a `503`, never a 500 — the definition is unchanged.

use axum::extract::{Path, Query, State};
use axum::http::StatusCode;
use axum::{Extension, Json};
use meridian_common::MeridianError;
use meridian_common::principal::Principal;
use meridian_store::semantics::{
    self, Certification, DataProductPatch, GlossaryTermPatch, MetricPatch, NewDataProduct,
    NewGlossaryTerm, NewMetric,
};
use meridian_store::tenancy;
use serde::Deserialize;
use serde_json::{Value, json};

use crate::AppState;
use crate::error::ApiError;
use crate::routes::grants::require_management;
use crate::sidecar::{CompileMetricRequest, MetricInput, SidecarClient};

/// The default page size for the semantics list endpoints.
const DEFAULT_PAGE_SIZE: i64 = 100;

/// The maximum page size a caller may request.
const MAX_PAGE_SIZE: i64 = 500;

// ---------------------------------------------------------------------------
// Shared helpers
// ---------------------------------------------------------------------------

/// Common list query: keyset pagination by the last id of the previous page.
#[derive(Debug, Deserialize)]
pub struct ListQuery {
    /// Continue after this id (the last id of the previous page).
    pub after: Option<String>,
    /// Page size (1..=`MAX_PAGE_SIZE`).
    pub limit: Option<i64>,
}

/// Clamps a requested page size into `1..=MAX_PAGE_SIZE`, defaulting when unset.
fn clamp_limit(limit: Option<i64>) -> i64 {
    limit.unwrap_or(DEFAULT_PAGE_SIZE).clamp(1, MAX_PAGE_SIZE)
}

/// Parses a certification string, defaulting to `draft` when absent, or 400 on
/// an unrecognized value.
fn parse_certification(raw: Option<&str>) -> Result<Certification, ApiError> {
    match raw {
        None => Ok(Certification::Draft),
        Some(value) => Certification::parse(value).ok_or_else(|| {
            ApiError::bad_request(format!(
                "certification must be one of draft|certified|deprecated, got {value:?}"
            ))
        }),
    }
}

/// Maps store not-found/conflict/validation errors onto the management-API
/// envelope (non-IRC-specific types).
fn map_error(error: MeridianError) -> ApiError {
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
// Metrics (G-F2)
// ===========================================================================

/// Request body for `POST /api/v2/metrics`.
#[derive(Debug, Deserialize)]
pub struct CreateMetricRequest {
    /// Machine name (unique per workspace, case-insensitively).
    pub name: String,
    /// Optional human label.
    #[serde(default)]
    pub display_name: Option<String>,
    /// Source table/view identifier (dotted).
    pub source: String,
    /// Measure aggregation expression (e.g. `SUM(amount)`).
    pub expression: String,
    /// Canonical dialect the fragments are authored in (default `trino`).
    #[serde(default)]
    pub dialect: Option<String>,
    /// Default group-by dimensions.
    #[serde(default)]
    pub dimensions: Vec<String>,
    /// Default boolean filter fragments (`AND`-ed).
    #[serde(default)]
    pub filters: Vec<String>,
    /// Grain description.
    #[serde(default)]
    pub grain: Option<String>,
    /// Description / documentation (markdown).
    #[serde(default)]
    pub description: Option<String>,
    /// Owner (audit string). Defaults to the caller when absent.
    #[serde(default)]
    pub owner: Option<String>,
    /// Certification status (default `draft`).
    #[serde(default)]
    pub certification: Option<String>,
}

/// Request body for `PATCH /api/v2/metrics/{id}` — every field optional.
#[derive(Debug, Deserialize)]
pub struct UpdateMetricRequest {
    /// New human label.
    #[serde(default)]
    pub display_name: Option<String>,
    /// New source identifier.
    #[serde(default)]
    pub source: Option<String>,
    /// New measure expression.
    #[serde(default)]
    pub expression: Option<String>,
    /// New canonical dialect.
    #[serde(default)]
    pub dialect: Option<String>,
    /// New default dimensions.
    #[serde(default)]
    pub dimensions: Option<Vec<String>>,
    /// New default filters.
    #[serde(default)]
    pub filters: Option<Vec<String>>,
    /// New grain.
    #[serde(default)]
    pub grain: Option<String>,
    /// New description.
    #[serde(default)]
    pub description: Option<String>,
    /// New owner.
    #[serde(default)]
    pub owner: Option<String>,
    /// New certification status.
    #[serde(default)]
    pub certification: Option<String>,
}

/// Renders a metric record as its API JSON.
fn metric_json(record: &semantics::MetricRecord) -> Value {
    json!({
        "id": record.id,
        "name": record.name,
        "display_name": record.display_name,
        "source": record.source,
        "expression": record.expression,
        "dialect": record.dialect,
        "dimensions": record.dimensions.0,
        "filters": record.filters.0,
        "grain": record.grain,
        "description": record.description,
        "owner": record.owner,
        "certification": record.certification,
        "created_at": record.created_at,
        "updated_at": record.updated_at,
    })
}

/// `POST /api/v2/metrics` — create a metric.
pub async fn create_metric(
    State(state): State<AppState>,
    Extension(caller): Extension<Principal>,
    Json(req): Json<CreateMetricRequest>,
) -> Result<(StatusCode, Json<Value>), ApiError> {
    require_management(&state.pool, &caller).await?;
    if req.name.trim().is_empty() {
        return Err(ApiError::bad_request("metric name must not be empty"));
    }
    if req.expression.trim().is_empty() {
        return Err(ApiError::bad_request("metric expression must not be empty"));
    }
    if req.source.trim().is_empty() {
        return Err(ApiError::bad_request("metric source must not be empty"));
    }
    let certification = parse_certification(req.certification.as_deref())?;
    let owner = req.owner.clone().unwrap_or_else(|| caller.audit_string());
    let dialect = req.dialect.clone().unwrap_or_else(|| "trino".to_owned());

    let record = semantics::create_metric(
        &state.pool,
        tenancy::default_workspace_id(),
        NewMetric {
            name: req.name.trim(),
            display_name: req.display_name.as_deref(),
            source: req.source.trim(),
            expression: req.expression.trim(),
            dialect: &dialect,
            dimensions: &req.dimensions,
            filters: &req.filters,
            grain: req.grain.as_deref(),
            description: req.description.as_deref(),
            owner: Some(&owner),
            certification,
        },
        &caller.audit_string(),
    )
    .await
    .map_err(map_error)?;

    Ok((StatusCode::CREATED, Json(metric_json(&record))))
}

/// `GET /api/v2/metrics` — list metrics.
pub async fn list_metrics(
    State(state): State<AppState>,
    Extension(caller): Extension<Principal>,
    Query(query): Query<ListQuery>,
) -> Result<Json<Value>, ApiError> {
    require_management(&state.pool, &caller).await?;
    let limit = clamp_limit(query.limit);
    let records = semantics::list_metrics(
        &state.pool,
        tenancy::default_workspace_id(),
        query.after.as_deref(),
        Some(limit),
    )
    .await?;
    let next = records.last().map(|r| r.id.clone());
    let metrics: Vec<Value> = records.iter().map(metric_json).collect();
    Ok(Json(json!({ "metrics": metrics, "next": next })))
}

/// `GET /api/v2/metrics/{id}` — one metric.
pub async fn get_metric(
    State(state): State<AppState>,
    Extension(caller): Extension<Principal>,
    Path(id): Path<String>,
) -> Result<Json<Value>, ApiError> {
    require_management(&state.pool, &caller).await?;
    let record = semantics::get_metric(&state.pool, &id)
        .await?
        .ok_or_else(|| {
            ApiError::new(
                StatusCode::NOT_FOUND,
                "NotFoundException",
                "metric not found",
            )
        })?;
    Ok(Json(metric_json(&record)))
}

/// `PATCH /api/v2/metrics/{id}` — update a metric.
pub async fn update_metric(
    State(state): State<AppState>,
    Extension(caller): Extension<Principal>,
    Path(id): Path<String>,
    Json(req): Json<UpdateMetricRequest>,
) -> Result<Json<Value>, ApiError> {
    require_management(&state.pool, &caller).await?;
    let certification = match req.certification.as_deref() {
        None => None,
        Some(value) => Some(Certification::parse(value).ok_or_else(|| {
            ApiError::bad_request(format!(
                "certification must be one of draft|certified|deprecated, got {value:?}"
            ))
        })?),
    };
    let record = semantics::update_metric(
        &state.pool,
        tenancy::default_workspace_id(),
        &id,
        MetricPatch {
            display_name: req.display_name,
            source: req.source,
            expression: req.expression,
            dialect: req.dialect,
            dimensions: req.dimensions,
            filters: req.filters,
            grain: req.grain,
            description: req.description,
            owner: req.owner,
            certification,
        },
        &caller.audit_string(),
    )
    .await
    .map_err(map_error)?;
    Ok(Json(metric_json(&record)))
}

/// `DELETE /api/v2/metrics/{id}` — delete a metric.
pub async fn delete_metric(
    State(state): State<AppState>,
    Extension(caller): Extension<Principal>,
    Path(id): Path<String>,
) -> Result<StatusCode, ApiError> {
    require_management(&state.pool, &caller).await?;
    semantics::delete_metric(
        &state.pool,
        tenancy::default_workspace_id(),
        &id,
        &caller.audit_string(),
    )
    .await
    .map_err(map_error)?;
    Ok(StatusCode::NO_CONTENT)
}

/// Query parameters for `GET /api/v2/metrics/{id}/compile`.
#[derive(Debug, Deserialize)]
pub struct CompileQuery {
    /// The engine dialect to compile SQL for (e.g. `trino`, `duckdb`).
    pub engine: String,
}

/// `GET /api/v2/metrics/{id}/compile?engine=<dialect>` — compile a metric to a
/// chosen engine's SQL (G-F2), deterministically via the sidecar.
///
/// Returns the compiled SQL and its honest status (`verified` | `best_effort` |
/// `unsupported`). A sidecar outage is a `503`, never a 500 — the definition is
/// untouched.
pub async fn compile_metric(
    State(state): State<AppState>,
    Extension(caller): Extension<Principal>,
    Extension(sidecar): Extension<Option<SidecarClient>>,
    Path(id): Path<String>,
    Query(query): Query<CompileQuery>,
) -> Result<Json<Value>, ApiError> {
    require_management(&state.pool, &caller).await?;
    let to_dialect = query.engine.trim().to_ascii_lowercase();
    if to_dialect.is_empty() {
        return Err(ApiError::bad_request(
            "engine (target dialect) must not be empty",
        ));
    }
    let record = semantics::get_metric(&state.pool, &id)
        .await?
        .ok_or_else(|| {
            ApiError::new(
                StatusCode::NOT_FOUND,
                "NotFoundException",
                "metric not found",
            )
        })?;

    let Some(sidecar) = sidecar else {
        return Err(ApiError::new(
            StatusCode::SERVICE_UNAVAILABLE,
            "ServiceUnavailableException",
            "the transpilation sidecar is not configured",
        ));
    };

    let request = CompileMetricRequest {
        metric: MetricInput {
            name: record.name.clone(),
            expression: record.expression.clone(),
            source: record.source.clone(),
            dimensions: record.dimensions.0.clone(),
            filters: record.filters.0.clone(),
            dialect: record.dialect.clone(),
        },
        to_dialect: to_dialect.clone(),
    };

    match sidecar.compile_metric(&request).await {
        Ok(response) => {
            let diagnostics: Vec<Value> = response
                .diagnostics
                .iter()
                .map(|d| json!({ "severity": d.severity, "code": d.code, "message": d.message }))
                .collect();
            Ok(Json(json!({
                "metric": record.name,
                "engine": to_dialect,
                "sql": response.sql,
                "status": response.status.as_str(),
                "diagnostics": diagnostics,
            })))
        }
        Err(error) => Err(ApiError::new(
            StatusCode::SERVICE_UNAVAILABLE,
            "ServiceUnavailableException",
            format!("transpilation sidecar is unavailable: {error}"),
        )),
    }
}

// ===========================================================================
// Glossary (G-F3)
// ===========================================================================

/// Request body for `POST /api/v2/glossary/terms`.
#[derive(Debug, Deserialize)]
pub struct CreateTermRequest {
    /// Term name (unique per workspace, case-insensitively).
    pub name: String,
    /// Definition (markdown).
    pub definition: String,
    /// Steward (audit string). Defaults to the caller when absent.
    #[serde(default)]
    pub steward: Option<String>,
    /// Certification status (default `draft`).
    #[serde(default)]
    pub certification: Option<String>,
}

/// Request body for `PATCH /api/v2/glossary/terms/{id}`.
#[derive(Debug, Deserialize)]
pub struct UpdateTermRequest {
    /// New definition.
    #[serde(default)]
    pub definition: Option<String>,
    /// New steward.
    #[serde(default)]
    pub steward: Option<String>,
    /// New certification status.
    #[serde(default)]
    pub certification: Option<String>,
}

/// Request body for `POST /api/v2/glossary/terms/{id}/links`.
#[derive(Debug, Deserialize)]
pub struct LinkTermRequest {
    /// Asset kind (`table` | `view` | `metric`).
    pub asset_kind: String,
    /// Stable asset reference (e.g. `table:<id>`).
    pub asset_ref: String,
}

/// Renders a glossary term as its API JSON.
fn term_json(record: &semantics::GlossaryTermRecord) -> Value {
    json!({
        "id": record.id,
        "name": record.name,
        "definition": record.definition,
        "steward": record.steward,
        "certification": record.certification,
        "created_at": record.created_at,
        "updated_at": record.updated_at,
    })
}

/// Renders a glossary link as its API JSON.
fn link_json(record: &semantics::GlossaryLinkRecord) -> Value {
    json!({
        "id": record.id,
        "term_id": record.term_id,
        "asset_kind": record.asset_kind,
        "asset_ref": record.asset_ref,
        "created_at": record.created_at,
    })
}

/// Validates an asset kind for a glossary link.
fn validate_asset_kind(kind: &str) -> Result<(), ApiError> {
    match kind {
        "table" | "view" | "metric" => Ok(()),
        other => Err(ApiError::bad_request(format!(
            "asset_kind must be one of table|view|metric, got {other:?}"
        ))),
    }
}

/// `POST /api/v2/glossary/terms` — create a glossary term.
pub async fn create_term(
    State(state): State<AppState>,
    Extension(caller): Extension<Principal>,
    Json(req): Json<CreateTermRequest>,
) -> Result<(StatusCode, Json<Value>), ApiError> {
    require_management(&state.pool, &caller).await?;
    if req.name.trim().is_empty() {
        return Err(ApiError::bad_request("term name must not be empty"));
    }
    if req.definition.trim().is_empty() {
        return Err(ApiError::bad_request("term definition must not be empty"));
    }
    let certification = parse_certification(req.certification.as_deref())?;
    let steward = req.steward.clone().unwrap_or_else(|| caller.audit_string());

    let record = semantics::create_term(
        &state.pool,
        tenancy::default_workspace_id(),
        NewGlossaryTerm {
            name: req.name.trim(),
            definition: req.definition.trim(),
            steward: Some(&steward),
            certification,
        },
        &caller.audit_string(),
    )
    .await
    .map_err(map_error)?;
    Ok((StatusCode::CREATED, Json(term_json(&record))))
}

/// `GET /api/v2/glossary/terms` — list glossary terms.
pub async fn list_terms(
    State(state): State<AppState>,
    Extension(caller): Extension<Principal>,
    Query(query): Query<ListQuery>,
) -> Result<Json<Value>, ApiError> {
    require_management(&state.pool, &caller).await?;
    let limit = clamp_limit(query.limit);
    let records = semantics::list_terms(
        &state.pool,
        tenancy::default_workspace_id(),
        query.after.as_deref(),
        Some(limit),
    )
    .await?;
    let next = records.last().map(|r| r.id.clone());
    let terms: Vec<Value> = records.iter().map(term_json).collect();
    Ok(Json(json!({ "terms": terms, "next": next })))
}

/// `GET /api/v2/glossary/terms/{id}` — one term plus its links.
pub async fn get_term(
    State(state): State<AppState>,
    Extension(caller): Extension<Principal>,
    Path(id): Path<String>,
) -> Result<Json<Value>, ApiError> {
    require_management(&state.pool, &caller).await?;
    let record = semantics::get_term(&state.pool, &id)
        .await?
        .ok_or_else(|| {
            ApiError::new(StatusCode::NOT_FOUND, "NotFoundException", "term not found")
        })?;
    let links = semantics::list_term_links(&state.pool, &id).await?;
    let mut body = term_json(&record);
    if let Value::Object(map) = &mut body {
        map.insert(
            "links".to_owned(),
            Value::Array(links.iter().map(link_json).collect()),
        );
    }
    Ok(Json(body))
}

/// `PATCH /api/v2/glossary/terms/{id}` — update a term.
pub async fn update_term(
    State(state): State<AppState>,
    Extension(caller): Extension<Principal>,
    Path(id): Path<String>,
    Json(req): Json<UpdateTermRequest>,
) -> Result<Json<Value>, ApiError> {
    require_management(&state.pool, &caller).await?;
    let certification = match req.certification.as_deref() {
        None => None,
        Some(value) => Some(Certification::parse(value).ok_or_else(|| {
            ApiError::bad_request(format!(
                "certification must be one of draft|certified|deprecated, got {value:?}"
            ))
        })?),
    };
    let record = semantics::update_term(
        &state.pool,
        tenancy::default_workspace_id(),
        &id,
        GlossaryTermPatch {
            definition: req.definition,
            steward: req.steward,
            certification,
        },
        &caller.audit_string(),
    )
    .await
    .map_err(map_error)?;
    Ok(Json(term_json(&record)))
}

/// `DELETE /api/v2/glossary/terms/{id}` — delete a term (and its links).
pub async fn delete_term(
    State(state): State<AppState>,
    Extension(caller): Extension<Principal>,
    Path(id): Path<String>,
) -> Result<StatusCode, ApiError> {
    require_management(&state.pool, &caller).await?;
    semantics::delete_term(
        &state.pool,
        tenancy::default_workspace_id(),
        &id,
        &caller.audit_string(),
    )
    .await
    .map_err(map_error)?;
    Ok(StatusCode::NO_CONTENT)
}

/// `GET /api/v2/glossary/terms/{id}/links` — a term's asset links.
pub async fn list_term_links(
    State(state): State<AppState>,
    Extension(caller): Extension<Principal>,
    Path(id): Path<String>,
) -> Result<Json<Value>, ApiError> {
    require_management(&state.pool, &caller).await?;
    // 404 a missing term (rather than returning an empty list for a typo'd id).
    semantics::get_term(&state.pool, &id)
        .await?
        .ok_or_else(|| {
            ApiError::new(StatusCode::NOT_FOUND, "NotFoundException", "term not found")
        })?;
    let links = semantics::list_term_links(&state.pool, &id).await?;
    Ok(Json(
        json!({ "links": links.iter().map(link_json).collect::<Vec<_>>() }),
    ))
}

/// `POST /api/v2/glossary/terms/{id}/links` — link a term to an asset.
pub async fn link_term(
    State(state): State<AppState>,
    Extension(caller): Extension<Principal>,
    Path(id): Path<String>,
    Json(req): Json<LinkTermRequest>,
) -> Result<(StatusCode, Json<Value>), ApiError> {
    require_management(&state.pool, &caller).await?;
    validate_asset_kind(&req.asset_kind)?;
    if req.asset_ref.trim().is_empty() {
        return Err(ApiError::bad_request("asset_ref must not be empty"));
    }
    let record = semantics::link_term(
        &state.pool,
        tenancy::default_workspace_id(),
        &id,
        &req.asset_kind,
        req.asset_ref.trim(),
        &caller.audit_string(),
    )
    .await
    .map_err(map_error)?;
    Ok((StatusCode::CREATED, Json(link_json(&record))))
}

/// `DELETE /api/v2/glossary/links/{id}` — remove a link.
pub async fn unlink_term(
    State(state): State<AppState>,
    Extension(caller): Extension<Principal>,
    Path(id): Path<String>,
) -> Result<StatusCode, ApiError> {
    require_management(&state.pool, &caller).await?;
    semantics::unlink_term(
        &state.pool,
        tenancy::default_workspace_id(),
        &id,
        &caller.audit_string(),
    )
    .await
    .map_err(map_error)?;
    Ok(StatusCode::NO_CONTENT)
}

// ===========================================================================
// Data products (G-F4)
// ===========================================================================

/// Request body for `POST /api/v2/products`.
#[derive(Debug, Deserialize)]
pub struct CreateProductRequest {
    /// Machine name (unique per workspace, case-insensitively).
    pub name: String,
    /// Optional human label.
    #[serde(default)]
    pub display_name: Option<String>,
    /// Description (markdown).
    #[serde(default)]
    pub description: Option<String>,
    /// Owner (audit string). Defaults to the caller when absent.
    #[serde(default)]
    pub owner: Option<String>,
    /// Free-text SLA statement.
    #[serde(default)]
    pub sla: Option<String>,
    /// Certification status (default `draft`).
    #[serde(default)]
    pub certification: Option<String>,
}

/// Request body for `PATCH /api/v2/products/{id}`.
#[derive(Debug, Deserialize)]
pub struct UpdateProductRequest {
    /// New human label.
    #[serde(default)]
    pub display_name: Option<String>,
    /// New description.
    #[serde(default)]
    pub description: Option<String>,
    /// New owner.
    #[serde(default)]
    pub owner: Option<String>,
    /// New SLA statement.
    #[serde(default)]
    pub sla: Option<String>,
    /// New certification status.
    #[serde(default)]
    pub certification: Option<String>,
}

/// Request body for `POST /api/v2/products/{id}/members`.
#[derive(Debug, Deserialize)]
pub struct AddMemberRequest {
    /// Member kind (`table` | `view` | `metric` | `glossary_term` | `contract`).
    pub member_kind: String,
    /// Stable member reference.
    pub member_ref: String,
}

/// Renders a data product as its API JSON.
fn product_json(record: &semantics::DataProductRecord) -> Value {
    json!({
        "id": record.id,
        "name": record.name,
        "display_name": record.display_name,
        "description": record.description,
        "owner": record.owner,
        "sla": record.sla,
        "certification": record.certification,
        "created_at": record.created_at,
        "updated_at": record.updated_at,
    })
}

/// Renders a product member as its API JSON.
fn member_json(record: &semantics::DataProductMemberRecord) -> Value {
    json!({
        "id": record.id,
        "product_id": record.product_id,
        "member_kind": record.member_kind,
        "member_ref": record.member_ref,
        "created_at": record.created_at,
    })
}

/// Validates a member kind for a data product.
fn validate_member_kind(kind: &str) -> Result<(), ApiError> {
    match kind {
        "table" | "view" | "metric" | "glossary_term" | "contract" => Ok(()),
        other => Err(ApiError::bad_request(format!(
            "member_kind must be one of table|view|metric|glossary_term|contract, got {other:?}"
        ))),
    }
}

/// `POST /api/v2/products` — create a data product.
pub async fn create_product(
    State(state): State<AppState>,
    Extension(caller): Extension<Principal>,
    Json(req): Json<CreateProductRequest>,
) -> Result<(StatusCode, Json<Value>), ApiError> {
    require_management(&state.pool, &caller).await?;
    if req.name.trim().is_empty() {
        return Err(ApiError::bad_request("product name must not be empty"));
    }
    let certification = parse_certification(req.certification.as_deref())?;
    let owner = req.owner.clone().unwrap_or_else(|| caller.audit_string());

    let record = semantics::create_product(
        &state.pool,
        tenancy::default_workspace_id(),
        NewDataProduct {
            name: req.name.trim(),
            display_name: req.display_name.as_deref(),
            description: req.description.as_deref(),
            owner: Some(&owner),
            sla: req.sla.as_deref(),
            certification,
        },
        &caller.audit_string(),
    )
    .await
    .map_err(map_error)?;
    Ok((StatusCode::CREATED, Json(product_json(&record))))
}

/// `GET /api/v2/products` — list data products.
pub async fn list_products(
    State(state): State<AppState>,
    Extension(caller): Extension<Principal>,
    Query(query): Query<ListQuery>,
) -> Result<Json<Value>, ApiError> {
    require_management(&state.pool, &caller).await?;
    let limit = clamp_limit(query.limit);
    let records = semantics::list_products(
        &state.pool,
        tenancy::default_workspace_id(),
        query.after.as_deref(),
        Some(limit),
    )
    .await?;
    let next = records.last().map(|r| r.id.clone());
    let products: Vec<Value> = records.iter().map(product_json).collect();
    Ok(Json(json!({ "products": products, "next": next })))
}

/// `GET /api/v2/products/{id}` — one product plus its members.
pub async fn get_product(
    State(state): State<AppState>,
    Extension(caller): Extension<Principal>,
    Path(id): Path<String>,
) -> Result<Json<Value>, ApiError> {
    require_management(&state.pool, &caller).await?;
    let record = semantics::get_product(&state.pool, &id)
        .await?
        .ok_or_else(|| {
            ApiError::new(
                StatusCode::NOT_FOUND,
                "NotFoundException",
                "product not found",
            )
        })?;
    let members = semantics::list_product_members(&state.pool, &id).await?;
    let mut body = product_json(&record);
    if let Value::Object(map) = &mut body {
        map.insert(
            "members".to_owned(),
            Value::Array(members.iter().map(member_json).collect()),
        );
    }
    Ok(Json(body))
}

/// `PATCH /api/v2/products/{id}` — update a product.
pub async fn update_product(
    State(state): State<AppState>,
    Extension(caller): Extension<Principal>,
    Path(id): Path<String>,
    Json(req): Json<UpdateProductRequest>,
) -> Result<Json<Value>, ApiError> {
    require_management(&state.pool, &caller).await?;
    let certification = match req.certification.as_deref() {
        None => None,
        Some(value) => Some(Certification::parse(value).ok_or_else(|| {
            ApiError::bad_request(format!(
                "certification must be one of draft|certified|deprecated, got {value:?}"
            ))
        })?),
    };
    let record = semantics::update_product(
        &state.pool,
        tenancy::default_workspace_id(),
        &id,
        DataProductPatch {
            display_name: req.display_name,
            description: req.description,
            owner: req.owner,
            sla: req.sla,
            certification,
        },
        &caller.audit_string(),
    )
    .await
    .map_err(map_error)?;
    Ok(Json(product_json(&record)))
}

/// `DELETE /api/v2/products/{id}` — delete a product (and its membership rows).
pub async fn delete_product(
    State(state): State<AppState>,
    Extension(caller): Extension<Principal>,
    Path(id): Path<String>,
) -> Result<StatusCode, ApiError> {
    require_management(&state.pool, &caller).await?;
    semantics::delete_product(
        &state.pool,
        tenancy::default_workspace_id(),
        &id,
        &caller.audit_string(),
    )
    .await
    .map_err(map_error)?;
    Ok(StatusCode::NO_CONTENT)
}

/// `GET /api/v2/products/{id}/members` — a product's members.
pub async fn list_product_members(
    State(state): State<AppState>,
    Extension(caller): Extension<Principal>,
    Path(id): Path<String>,
) -> Result<Json<Value>, ApiError> {
    require_management(&state.pool, &caller).await?;
    semantics::get_product(&state.pool, &id)
        .await?
        .ok_or_else(|| {
            ApiError::new(
                StatusCode::NOT_FOUND,
                "NotFoundException",
                "product not found",
            )
        })?;
    let members = semantics::list_product_members(&state.pool, &id).await?;
    Ok(Json(
        json!({ "members": members.iter().map(member_json).collect::<Vec<_>>() }),
    ))
}

/// `POST /api/v2/products/{id}/members` — add a member.
pub async fn add_product_member(
    State(state): State<AppState>,
    Extension(caller): Extension<Principal>,
    Path(id): Path<String>,
    Json(req): Json<AddMemberRequest>,
) -> Result<(StatusCode, Json<Value>), ApiError> {
    require_management(&state.pool, &caller).await?;
    validate_member_kind(&req.member_kind)?;
    if req.member_ref.trim().is_empty() {
        return Err(ApiError::bad_request("member_ref must not be empty"));
    }
    let record = semantics::add_product_member(
        &state.pool,
        tenancy::default_workspace_id(),
        &id,
        &req.member_kind,
        req.member_ref.trim(),
        &caller.audit_string(),
    )
    .await
    .map_err(map_error)?;
    Ok((StatusCode::CREATED, Json(member_json(&record))))
}

/// `DELETE /api/v2/products/members/{id}` — remove a member.
pub async fn remove_product_member(
    State(state): State<AppState>,
    Extension(caller): Extension<Principal>,
    Path(id): Path<String>,
) -> Result<StatusCode, ApiError> {
    require_management(&state.pool, &caller).await?;
    semantics::remove_product_member(
        &state.pool,
        tenancy::default_workspace_id(),
        &id,
        &caller.audit_string(),
    )
    .await
    .map_err(map_error)?;
    Ok(StatusCode::NO_CONTENT)
}

/// `GET /api/v2/products/{id}/status` — the product-level status page (G-F4,
/// reusing the quality status surface, E-F5).
///
/// Aggregates the product's certification, its members by kind, and — for each
/// table member whose reference resolves — the table's quality status and trust
/// score (the same signals the per-table status page shows). This is the "is
/// this product healthy?" answer for a consumer, assembled from the members'
/// real quality state, never fabricated.
pub async fn product_status(
    State(state): State<AppState>,
    Extension(caller): Extension<Principal>,
    Path(id): Path<String>,
) -> Result<Json<Value>, ApiError> {
    require_management(&state.pool, &caller).await?;
    let workspace_id = tenancy::default_workspace_id();
    let product = semantics::get_product(&state.pool, &id)
        .await?
        .ok_or_else(|| {
            ApiError::new(
                StatusCode::NOT_FOUND,
                "NotFoundException",
                "product not found",
            )
        })?;
    let members = semantics::list_product_members(&state.pool, &id).await?;

    // Per-kind counts (the bundle composition).
    let mut counts: std::collections::BTreeMap<String, usize> = std::collections::BTreeMap::new();
    for member in &members {
        *counts.entry(member.member_kind.clone()).or_insert(0) += 1;
    }

    // For each table member, resolve its quality status + trust score. A member
    // ref of the form `table:<id>` names a table row; unresolvable refs are
    // reported as `unknown` rather than dropped (honest about coverage).
    let mut table_statuses = Vec::new();
    let mut worst_rank: Option<i32> = None;
    for member in members.iter().filter(|m| m.member_kind == "table") {
        let table_id = member
            .member_ref
            .strip_prefix("table:")
            .unwrap_or(&member.member_ref);
        let status = resolve_table_member_status(&state, workspace_id, table_id).await;
        if let Some(rank) = status.get("status_rank").and_then(Value::as_i64) {
            let rank = i32::try_from(rank).unwrap_or(i32::MAX);
            worst_rank = Some(worst_rank.map_or(rank, |w| w.max(rank)));
        }
        table_statuses.push(json!({
            "member_ref": member.member_ref,
            "resolved": status,
        }));
    }

    // The product's rolled-up health label: the worst member table status, or
    // `no_signal` when no member exposes a status.
    let rollup = match worst_rank {
        Some(0) => "healthy",
        Some(1) => "degraded",
        Some(_) => "unhealthy",
        None => "no_signal",
    };

    Ok(Json(json!({
        "product": product_json(&product),
        "member_counts": counts,
        "member_total": members.len(),
        "table_statuses": table_statuses,
        "health_rollup": rollup,
    })))
}

/// The columns needed to resolve a table member's status: the table row plus
/// its warehouse id and namespace levels (for the trust-score scope chain).
#[derive(sqlx::FromRow)]
struct TableStatusLookup {
    id: String,
    warehouse_id: String,
    levels: Vec<String>,
}

/// Resolves a table member's quality status + trust score from the index,
/// returning an honest `unknown` payload when the id does not resolve to a
/// table (a stale or malformed ref).
async fn resolve_table_member_status(
    state: &AppState,
    workspace_id: meridian_common::id::WorkspaceId,
    table_id: &str,
) -> Value {
    // One query resolves the table row, its warehouse, and its namespace levels
    // (the inputs the trust score and scope chain need).
    let lookup: Option<TableStatusLookup> = sqlx::query_as(
        "SELECT t.id AS id, n.warehouse_id AS warehouse_id, n.levels AS levels
         FROM tables t JOIN namespaces n ON n.id = t.namespace_id
         WHERE t.id = $1",
    )
    .bind(table_id)
    .fetch_optional(&state.pool)
    .await
    .ok()
    .flatten();

    let Some(lookup) = lookup else {
        return json!({ "status": "unknown", "reason": "table reference did not resolve" });
    };

    let chain = crate::routes::grants::namespace_scope_chain(
        &state.pool,
        &lookup.warehouse_id,
        &lookup.levels,
    )
    .await
    .unwrap_or_default();
    let trust =
        meridian_store::quality_score::score_table(&state.pool, workspace_id, &lookup.id, &chain)
            .await
            .map_or(Value::Null, |score| score.to_json());

    // status_rank: 0 healthy, 1 degraded, 2 unhealthy — derived from open
    // incidents on the table (the same signal the per-table status page uses).
    let open_incidents = open_incident_count(state, &lookup.id).await;
    let (status, rank) = match open_incidents {
        0 => ("healthy", 0),
        1..=2 => ("degraded", 1),
        _ => ("unhealthy", 2),
    };
    json!({
        "table_id": lookup.id,
        "status": status,
        "status_rank": rank,
        "open_incidents": open_incidents,
        "trust": trust,
    })
}

/// Counts open (unresolved) incidents for a table — the health signal behind a
/// product's rolled-up status.
async fn open_incident_count(state: &AppState, table_id: &str) -> i64 {
    sqlx::query_scalar::<_, i64>(
        "SELECT count(*) FROM incidents
         WHERE table_id = $1 AND status <> 'resolved'",
    )
    .bind(table_id)
    .fetch_one(&state.pool)
    .await
    .unwrap_or(0)
}
