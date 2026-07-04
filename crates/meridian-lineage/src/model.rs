//! The lineage data model and its idempotent upsert.
//!
//! An edge is a directed, provenance- and confidence-labeled fact:
//! `src --produces--> dst` ("dst is derived from src"). Each endpoint is
//! *either* a native Meridian table (a `tables.id`) *or* an external dataset
//! named by an opaque string (an OpenLineage dataset with no Meridian table).
//! Recording a partially-known edge — one native, one external endpoint — is
//! deliberate: it captures real evidence without fabricating a table identity
//! for a dataset Meridian does not manage.
//!
//! Upsert is keyed by `(workspace, src, dst, provenance)`: the same pair
//! observed from two provenances is two independent pieces of evidence, so
//! they are two rows. A repeat observation of the *same* provenance bumps
//! `last_seen`, merges `engine_meta`, raises `confidence` toward the new
//! reading (never lowers it), and fills in a `column_map` that arrives later —
//! but it never fabricates columns that were not observed.

use chrono::{DateTime, Utc};
use meridian_common::id::WorkspaceId;
use meridian_common::{MeridianError, Result};
use serde::{Deserialize, Serialize};
use serde_json::Value;
use sqlx::PgPool;
use sqlx::types::Json;
use ulid::Ulid;

/// How a lineage edge was learned. Part of the edge's identity.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "kebab-case")]
pub enum Provenance {
    /// Derived from a commit's snapshot summary / engine properties (F-F1).
    /// Zero-setup, but an engine job-id proves co-occurrence, not a proven
    /// read→write dependency, so these carry a modest confidence.
    Commit,
    /// Parsed from an OpenLineage `RunEvent` (F-F2): the engine explicitly
    /// declared the input/output relationship, so high confidence.
    Openlineage,
    /// Reserved for query-log ingestion (F-F3, later wave).
    QueryLog,
}

impl Provenance {
    /// The stored (and wire) string form.
    #[must_use]
    pub fn as_str(self) -> &'static str {
        match self {
            Self::Commit => "commit",
            Self::Openlineage => "openlineage",
            Self::QueryLog => "query-log",
        }
    }
}

/// A lineage endpoint: a native Meridian table or an opaque external dataset.
///
/// The two are mutually exclusive by construction (an enum), matching the
/// database's native-XOR-external CHECK constraint.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(tag = "kind", rename_all = "snake_case")]
pub enum Endpoint {
    /// A table Meridian owns, by its internal id (`tables.id`).
    Table {
        /// The `tables.id`.
        id: String,
    },
    /// A dataset Meridian does not (yet) know as a native table, named by an
    /// opaque string (e.g. an OpenLineage `namespace/name`).
    External {
        /// The opaque external dataset name.
        name: String,
    },
}

impl Endpoint {
    /// A native-table endpoint.
    #[must_use]
    pub fn table(id: impl Into<String>) -> Self {
        Self::Table { id: id.into() }
    }

    /// An external-dataset endpoint.
    #[must_use]
    pub fn external(name: impl Into<String>) -> Self {
        Self::External { name: name.into() }
    }

    /// The native table id, if this endpoint is a table.
    #[must_use]
    pub fn table_id(&self) -> Option<&str> {
        match self {
            Self::Table { id } => Some(id),
            Self::External { .. } => None,
        }
    }

    /// The external name, if this endpoint is external.
    #[must_use]
    pub fn external_name(&self) -> Option<&str> {
        match self {
            Self::External { name } => Some(name),
            Self::Table { .. } => None,
        }
    }
}

/// One column-level lineage mapping (F-F3): `src_column --> dst_column`, with
/// an optional human-readable transform description. Absence of a `column_map`
/// means table-level only — never "all columns relate to all columns".
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ColumnMapEntry {
    /// Source column name.
    pub src_column: String,
    /// Destination column name.
    pub dst_column: String,
    /// Optional transformation description (e.g. `IDENTITY`, `SUM(x)`).
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub transform: Option<String>,
}

/// A request to record (upsert) one lineage edge.
#[derive(Debug, Clone)]
pub struct EdgeUpsert {
    /// Source endpoint.
    pub src: Endpoint,
    /// Destination endpoint.
    pub dst: Endpoint,
    /// How the edge was learned.
    pub provenance: Provenance,
    /// Confidence in `[0,1]`. Clamped on write.
    pub confidence: f64,
    /// Column-level mappings, when known. `None` = table-level only.
    pub column_map: Option<Vec<ColumnMapEntry>>,
    /// Engine/provenance evidence merged into `engine_meta`.
    pub engine_meta: Value,
}

/// A persisted lineage edge, as read back for the graph/impact surface.
#[derive(Debug, Clone, Serialize)]
pub struct LineageEdge {
    /// Edge id (ULID).
    pub id: String,
    /// Source endpoint.
    pub src: Endpoint,
    /// Destination endpoint.
    pub dst: Endpoint,
    /// How the edge was learned.
    pub provenance: Provenance,
    /// Confidence in `[0,1]`.
    pub confidence: f64,
    /// Column-level mappings, when known.
    pub column_map: Option<Vec<ColumnMapEntry>>,
    /// Engine/provenance evidence.
    pub engine_meta: Value,
    /// First time this edge was observed.
    pub first_seen: DateTime<Utc>,
    /// Most recent observation.
    pub last_seen: DateTime<Utc>,
}

/// The raw row shape as it comes off Postgres.
#[derive(Debug, sqlx::FromRow)]
struct EdgeRow {
    id: String,
    src_table_id: Option<String>,
    src_external: Option<String>,
    dst_table_id: Option<String>,
    dst_external: Option<String>,
    provenance: String,
    confidence: f64,
    column_map: Option<Json<Vec<ColumnMapEntry>>>,
    engine_meta: Json<Value>,
    first_seen: DateTime<Utc>,
    last_seen: DateTime<Utc>,
}

impl EdgeRow {
    fn endpoint(table_id: Option<String>, external: Option<String>) -> Result<Endpoint> {
        match (table_id, external) {
            (Some(id), None) => Ok(Endpoint::Table { id }),
            (None, Some(name)) => Ok(Endpoint::External { name }),
            _ => Err(MeridianError::internal_msg(
                "lineage edge row violates native-XOR-external invariant \
                 (both or neither endpoint identity set)",
            )),
        }
    }

    fn into_edge(self) -> Result<LineageEdge> {
        let provenance = match self.provenance.as_str() {
            "commit" => Provenance::Commit,
            "openlineage" => Provenance::Openlineage,
            "query-log" => Provenance::QueryLog,
            other => {
                return Err(MeridianError::internal_msg(format!(
                    "unknown lineage provenance in database: {other}"
                )));
            }
        };
        Ok(LineageEdge {
            id: self.id,
            src: Self::endpoint(self.src_table_id, self.src_external)?,
            dst: Self::endpoint(self.dst_table_id, self.dst_external)?,
            provenance,
            confidence: self.confidence,
            column_map: self.column_map.map(|j| j.0),
            engine_meta: self.engine_meta.0,
            first_seen: self.first_seen,
            last_seen: self.last_seen,
        })
    }
}

/// Idempotently records one edge, returning the row id.
///
/// On first observation the row is inserted with `first_seen = last_seen =
/// now()`. On a repeat observation of the same `(src, dst, provenance)` the
/// row is updated in place: `last_seen` bumps, `confidence` rises to the
/// greater of old/new (evidence only accumulates), `engine_meta` is shallow-
/// merged, and a `column_map` is filled in when one is newly supplied — but a
/// `NULL` `column_map` never overwrites an existing one, and columns are never
/// invented. `first_seen` is preserved.
///
/// A no-op self-edge (`src == dst`, both the same native table) is rejected
/// by the database CHECK; the derivation paths never construct one, but the
/// guard is defense in depth.
pub async fn upsert_edge(
    pool: &PgPool,
    workspace_id: WorkspaceId,
    edge: &EdgeUpsert,
) -> Result<String> {
    let id = Ulid::new().to_string();
    let confidence = edge.confidence.clamp(0.0, 1.0);
    let column_map = edge.column_map.as_ref().map(Json);

    // ON CONFLICT targets the functional unique index; the coalesced keys
    // mirror it exactly. GREATEST keeps confidence monotonic; the `||` merge
    // lets a later observation add engine facts without dropping earlier
    // ones. COALESCE(EXCLUDED.column_map, existing) fills a table-level edge
    // in with columns when they later arrive, and never nulls an existing map.
    let returned: String = sqlx::query_scalar(
        "INSERT INTO lineage_edges (
             id, workspace_id,
             src_table_id, src_external, dst_table_id, dst_external,
             provenance, confidence, column_map, engine_meta
         )
         VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10)
         ON CONFLICT (
             workspace_id,
             (COALESCE('meridian:' || src_table_id, 'ext:' || src_external)),
             (COALESCE('meridian:' || dst_table_id, 'ext:' || dst_external)),
             provenance
         )
         DO UPDATE SET
             last_seen = now(),
             confidence = GREATEST(lineage_edges.confidence, EXCLUDED.confidence),
             column_map = COALESCE(EXCLUDED.column_map, lineage_edges.column_map),
             engine_meta = lineage_edges.engine_meta || EXCLUDED.engine_meta
         RETURNING id",
    )
    .bind(&id)
    .bind(workspace_id.to_string())
    .bind(edge.src.table_id())
    .bind(edge.src.external_name())
    .bind(edge.dst.table_id())
    .bind(edge.dst.external_name())
    .bind(edge.provenance.as_str())
    .bind(confidence)
    .bind(column_map)
    .bind(Json(&edge.engine_meta))
    .fetch_one(pool)
    .await
    .map_err(|e| meridian_store::map_sqlx_error("failed to upsert lineage edge", e))?;

    Ok(returned)
}

/// Direct downstream edges of a native table (edges where it is the source).
pub async fn downstream_edges(
    pool: &PgPool,
    workspace_id: WorkspaceId,
    table_id: &str,
) -> Result<Vec<LineageEdge>> {
    edges_for(pool, workspace_id, "src_table_id", table_id).await
}

/// Direct upstream edges of a native table (edges where it is the target).
pub async fn upstream_edges(
    pool: &PgPool,
    workspace_id: WorkspaceId,
    table_id: &str,
) -> Result<Vec<LineageEdge>> {
    edges_for(pool, workspace_id, "dst_table_id", table_id).await
}

async fn edges_for(
    pool: &PgPool,
    workspace_id: WorkspaceId,
    column: &str,
    table_id: &str,
) -> Result<Vec<LineageEdge>> {
    // `column` is one of two internal string literals, never user input.
    let sql = format!(
        "SELECT id, src_table_id, src_external, dst_table_id, dst_external,
                provenance, confidence, column_map, engine_meta,
                first_seen, last_seen
         FROM lineage_edges
         WHERE workspace_id = $1 AND {column} = $2
         ORDER BY id"
    );
    let rows: Vec<EdgeRow> = sqlx::query_as(&sql)
        .bind(workspace_id.to_string())
        .bind(table_id)
        .fetch_all(pool)
        .await
        .map_err(|e| meridian_store::map_sqlx_error("failed to load lineage edges", e))?;
    rows.into_iter().map(EdgeRow::into_edge).collect()
}
