//! Lineage API (Pillar F): the OpenLineage sink, the up/downstream graph, and
//! impact analysis. All endpoints live under `/api/v2/lineage`.
//!
//! - `POST /api/v2/lineage/openlineage` — the **sink** (F-F2): accept an
//!   OpenLineage `RunEvent` and record its declared edges (with column facets
//!   when present).
//! - `GET  /api/v2/lineage?asset=&depth=&direction=` — the **graph** (F-F5):
//!   the reachable up/downstream lineage of an asset.
//! - `GET  /api/v2/lineage/impact?asset=&change=drop_column:foo` — **impact**
//!   analysis (F-F5): the downstream blast radius of a change and the owners
//!   to notify.
//!
//! # Authorization
//!
//! Every endpoint is **management-gated**, like the events and audit surfaces:
//! a lineage graph spans many assets at once, so a single resource-scoped
//! privilege cannot express "may read lineage" without over- or under-
//! granting. The OpenLineage sink is a write into a workspace-wide graph and
//! is management-gated for the same reason. Revisit if a finer `READ_LINEAGE`
//! privilege earns its keep.
//!
//! # Asset addressing
//!
//! `asset=` is a dotted identifier (`warehouse.namespace.table`). It resolves
//! to a native Meridian table; an unknown asset 404s. The sink, by contrast,
//! takes OpenLineage dataset names and resolves them leniently (an unresolved
//! dataset becomes a labeled external node — see `meridian_lineage::resolve`).

use axum::extract::{Query, State};
use axum::{Extension, Json};
use meridian_common::id::WorkspaceId;
use meridian_common::principal::Principal;
use meridian_lineage::impact::{self, Change, Direction};
use meridian_lineage::model::Endpoint;
use meridian_lineage::openlineage::{self, RunEvent};
use meridian_lineage::resolve::resolve_input_endpoint;
use meridian_store::tenancy;
use serde::{Deserialize, Serialize};

use crate::AppState;
use crate::error::ApiError;
use crate::routes::grants::require_management;

/// Largest traversal depth the graph/impact endpoints accept, so a query
/// cannot walk an unbounded chain.
const MAX_DEPTH: u32 = 20;

/// Default traversal depth when `depth` is omitted.
const DEFAULT_DEPTH: u32 = 3;

// ---------------------------------------------------------------------------
// OpenLineage sink
// ---------------------------------------------------------------------------

/// Response body for the OpenLineage sink: how many edges the event produced.
#[derive(Debug, Serialize)]
pub struct IngestResponse {
    /// Number of lineage edges recorded (upserted) from the event.
    pub edges_recorded: usize,
}

/// `POST /api/v2/lineage/openlineage` — the OpenLineage sink (F-F2).
///
/// Accepts one OpenLineage `RunEvent` (the JSON Spark/Airflow/dbt/Flink emit).
/// Unknown fields are ignored so newer producer versions still parse. A run
/// with no inputs or no outputs records nothing (there is no pair to relate).
pub async fn ingest_openlineage(
    State(state): State<AppState>,
    Extension(principal): Extension<Principal>,
    Json(event): Json<RunEvent>,
) -> Result<Json<IngestResponse>, ApiError> {
    require_management(&state.pool, &principal).await?;
    let workspace_id = tenancy::default_workspace_id();
    let edges_recorded = openlineage::ingest_run_event(&state.pool, workspace_id, &event)
        .await
        .map_err(ApiError::from)?;
    Ok(Json(IngestResponse { edges_recorded }))
}

// ---------------------------------------------------------------------------
// Graph
// ---------------------------------------------------------------------------

/// Query parameters of `GET /api/v2/lineage`.
#[derive(Debug, Deserialize)]
pub struct GraphParams {
    /// Dotted asset identifier (`warehouse.namespace.table`).
    pub asset: String,
    /// Traversal depth (1..=20); default 3.
    pub depth: Option<u32>,
    /// `upstream`, `downstream`, or `both` (default).
    pub direction: Option<String>,
}

/// `GET /api/v2/lineage?asset=&depth=&direction=` — the up/downstream graph.
pub async fn get_lineage(
    State(state): State<AppState>,
    Extension(principal): Extension<Principal>,
    Query(params): Query<GraphParams>,
) -> Result<Json<impact::LineageGraph>, ApiError> {
    require_management(&state.pool, &principal).await?;
    let workspace_id = tenancy::default_workspace_id();
    let table_id = resolve_asset(&state, workspace_id, &params.asset).await?;
    let depth = clamp_depth(params.depth);
    let direction = Direction::parse(params.direction.as_deref());

    let graph = impact::lineage_graph(&state.pool, workspace_id, &table_id, direction, depth)
        .await
        .map_err(ApiError::from)?;
    Ok(Json(graph))
}

// ---------------------------------------------------------------------------
// Impact
// ---------------------------------------------------------------------------

/// Query parameters of `GET /api/v2/lineage/impact`.
#[derive(Debug, Deserialize)]
pub struct ImpactParams {
    /// Dotted asset identifier (`warehouse.namespace.table`).
    pub asset: String,
    /// The change: `drop_table` or `drop_column:<name>`.
    pub change: String,
    /// Traversal depth (1..=20); default 3.
    pub depth: Option<u32>,
}

/// `GET /api/v2/lineage/impact?asset=&change=` — the downstream blast radius.
pub async fn get_impact(
    State(state): State<AppState>,
    Extension(principal): Extension<Principal>,
    Query(params): Query<ImpactParams>,
) -> Result<Json<impact::ImpactReport>, ApiError> {
    require_management(&state.pool, &principal).await?;
    let workspace_id = tenancy::default_workspace_id();
    let table_id = resolve_asset(&state, workspace_id, &params.asset).await?;
    let change = Change::parse(&params.change).ok_or_else(|| {
        ApiError::bad_request(format!(
            "unrecognized change {:?}; expected `drop_table` or `drop_column:<name>`",
            params.change
        ))
    })?;
    let depth = clamp_depth(params.depth);

    let report = impact::impact_of(&state.pool, workspace_id, &table_id, &change, depth)
        .await
        .map_err(ApiError::from)?;
    Ok(Json(report))
}

// ---------------------------------------------------------------------------
// Helpers
// ---------------------------------------------------------------------------

fn clamp_depth(depth: Option<u32>) -> u32 {
    depth.unwrap_or(DEFAULT_DEPTH).clamp(1, MAX_DEPTH)
}

/// Resolves a dotted `asset=` identifier to a native table id, 404ing when it
/// does not name a Meridian table (an external dataset is not a valid root for
/// a graph/impact query — there is nothing native to traverse from).
async fn resolve_asset(
    state: &AppState,
    workspace_id: WorkspaceId,
    asset: &str,
) -> Result<String, ApiError> {
    match resolve_input_endpoint(&state.pool, workspace_id, asset)
        .await
        .map_err(ApiError::from)?
    {
        Endpoint::Table { id } => Ok(id),
        Endpoint::External { .. } => Err(ApiError::no_such_table(format!(
            "no table {asset:?} in this catalog"
        ))),
    }
}
