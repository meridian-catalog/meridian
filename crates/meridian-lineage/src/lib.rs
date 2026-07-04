//! Lineage core for Meridian (spec Pillar F): commit-native table lineage,
//! the OpenLineage sink + emitter, and impact analysis over the lineage graph.
//!
//! # What this crate is
//!
//! - **F-F1 — commit-native lineage** ([`commit_hook`], [`worker`]): every
//!   commit already enqueues a durable `table.committed` event; the
//!   [`worker`] consumes that stream *after* the commit (never inside the
//!   sacred commit transaction) and records table-level edges whenever the new
//!   snapshot's summary declares its inputs. Zero pipeline setup.
//! - **F-F2 — OpenLineage, both directions** ([`openlineage`]): a first-class
//!   sink that turns an OpenLineage `RunEvent` (Spark/Airflow/dbt/Flink) into
//!   edges — with column-level facets when present — and an emitter that
//!   renders Meridian-initiated jobs (maintenance) as OpenLineage events for
//!   external tools.
//! - **F-F5 — impact analysis** ([`impact`]): the up/downstream graph and the
//!   blast-radius query the incidents wave calls, with per-asset owners.
//!
//! # The no-fabrication guarantee (spec F-F2/F-F3)
//!
//! Meridian never emits the "everything-relates-to-everything" cartesian
//! edges that are OpenLineage's documented failure mode. Every edge traces to
//! a concrete declaration: a commit that listed its inputs, or an engine that
//! declared an (input, output) pair. An identifier we cannot resolve to a
//! table becomes a labeled **external** node — visible, not invented. A table
//! with no evidence has an empty lineage graph, truthfully empty. Column-level
//! edges are recorded only from an explicit `columnLineage` facet; a
//! table-level edge stays `column_map = None` rather than fanning out a column
//! cross-product.
//!
//! The data model, its idempotent upsert, and the graph/impact reads live in
//! [`model`] and [`impact`]; the persistence lives in migration
//! `0017_lineage_edges` (in `meridian-store`, which owns the migrator).

pub mod commit_hook;
pub mod impact;
pub mod model;
pub mod openlineage;
pub mod resolve;
pub mod worker;

pub use impact::{
    AffectedAsset, Change, Direction, GraphEdge, GraphNode, ImpactReport, LineageGraph, impact_of,
    lineage_graph,
};
pub use model::{
    ColumnMapEntry, EdgeUpsert, Endpoint, LineageEdge, Provenance, downstream_edges, upsert_edge,
    upstream_edges,
};
pub use openlineage::{
    RunEvent, build_run_event, emit_maintenance, emit_run_event, ingest_run_event,
    maintenance_run_event,
};
pub use worker::run_worker;
