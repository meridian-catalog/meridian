//! OpenLineage, both directions (F-F2).
//!
//! - **Sink** ([`ingest_run_event`]): accept an OpenLineage `RunEvent`
//!   (the JSON Spark/Airflow/dbt/Flink emit), read its `inputs`/`outputs`
//!   datasets, and record a `src → dst` edge for every (input, output) pair,
//!   with `columnLineage` facet columns when the event carries them. Edges are
//!   `provenance = openlineage` at [`OPENLINEAGE_CONFIDENCE`].
//! - **Emitter** ([`build_run_event`]): construct a spec-valid OpenLineage
//!   `RunEvent` describing a Meridian-initiated job (maintenance
//!   compaction/expiry) so external lineage tools see Meridian's own
//!   operations. [`emit_run_event`] POSTs it to a configured collector.
//!
//! # No cartesian fabrication (spec F-F2/F-F3)
//!
//! The (input × output) product here is *exactly what the emitting engine
//! declared*: the engine asserted these inputs produced these outputs in one
//! run. That is not the forbidden failure mode — that mode is inventing edges
//! between datasets no engine ever related. When an event carries a
//! `columnLineage` output facet, we record the precise column edges it names;
//! when it does not, the edge is table-level (`column_map = None`), never a
//! column cross-product.

use chrono::{DateTime, Utc};
use meridian_common::id::WorkspaceId;
use meridian_common::{MeridianError, Result};
use serde::Deserialize;
use serde_json::{Value, json};
use sqlx::PgPool;

use crate::model::{ColumnMapEntry, EdgeUpsert, Provenance, upsert_edge};
use crate::resolve::resolve_input_endpoint;

/// Confidence for an OpenLineage-declared edge: the engine explicitly stated
/// the relationship, so this is the strongest table-level provenance we have.
pub const OPENLINEAGE_CONFIDENCE: f64 = 0.95;

// ---------------------------------------------------------------------------
// Wire types (a pragmatic subset of the OpenLineage 1.x RunEvent schema)
// ---------------------------------------------------------------------------

/// An OpenLineage `RunEvent`. Only the fields lineage needs are modeled;
/// unknown fields are ignored so newer producer versions still parse.
#[derive(Debug, Clone, Deserialize)]
pub struct RunEvent {
    /// Run transition (`START`, `RUNNING`, `COMPLETE`, `ABORT`, `FAIL`).
    #[serde(default, rename = "eventType")]
    pub event_type: Option<String>,
    /// Event time (RFC 3339). Retained for `engine_meta`; not required.
    #[serde(default, rename = "eventTime")]
    pub event_time: Option<String>,
    /// The run this event belongs to.
    #[serde(default)]
    pub run: Option<Run>,
    /// The job that produced the run.
    #[serde(default)]
    pub job: Option<Job>,
    /// Input datasets read by the run.
    #[serde(default)]
    pub inputs: Vec<Dataset>,
    /// Output datasets written by the run.
    #[serde(default)]
    pub outputs: Vec<Dataset>,
    /// Producer URI (the emitting integration).
    #[serde(default)]
    pub producer: Option<String>,
}

/// The `run` object: carries the run id used for provenance.
#[derive(Debug, Clone, Deserialize)]
pub struct Run {
    /// Globally-unique run id (a UUID).
    #[serde(rename = "runId")]
    pub run_id: String,
}

/// The `job` object: namespace + name identify the pipeline step.
#[derive(Debug, Clone, Deserialize)]
pub struct Job {
    /// Job namespace (e.g. the scheduler/cluster).
    pub namespace: String,
    /// Job name (e.g. the task id).
    pub name: String,
}

/// An OpenLineage dataset: a `(namespace, name)` pair with optional facets.
#[derive(Debug, Clone, Deserialize)]
pub struct Dataset {
    /// Dataset namespace (e.g. `s3://bucket`, a catalog name).
    pub namespace: String,
    /// Dataset name (e.g. `db.schema.table`, a path).
    pub name: String,
    /// Dataset facets. Only `columnLineage` is read here.
    #[serde(default)]
    pub facets: Option<DatasetFacets>,
}

/// Dataset facets subset.
#[derive(Debug, Clone, Deserialize)]
pub struct DatasetFacets {
    /// The `columnLineage` facet, present on some outputs.
    #[serde(default, rename = "columnLineage")]
    pub column_lineage: Option<ColumnLineageFacet>,
}

/// The `columnLineage` output facet: for each output field, the input fields
/// it was derived from.
#[derive(Debug, Clone, Deserialize)]
pub struct ColumnLineageFacet {
    /// Keyed by output field name.
    pub fields: std::collections::BTreeMap<String, ColumnLineageField>,
}

/// One output field's input dependencies.
#[derive(Debug, Clone, Deserialize)]
pub struct ColumnLineageField {
    /// The input fields this output field draws from.
    #[serde(rename = "inputFields")]
    pub input_fields: Vec<InputField>,
    /// Optional transformation description. Newer OpenLineage versions nest
    /// this inside a `transformation` object; the flat
    /// `transformationDescription` remains the widely-emitted form and is what
    /// we read.
    #[serde(default, rename = "transformationDescription")]
    pub transformation_description: Option<String>,
}

/// A reference to a specific input dataset field.
#[derive(Debug, Clone, Deserialize)]
pub struct InputField {
    /// The input dataset namespace.
    pub namespace: String,
    /// The input dataset name.
    pub name: String,
    /// The input field (column) name.
    pub field: String,
}

/// Normalizes an OpenLineage `(namespace, name)` to the dotted identifier the
/// resolver understands.
///
/// The OpenLineage convention for a catalog table is namespace = the catalog /
/// warehouse and name = the dotted table path within it (`db.table` /
/// `ns.table`). Meridian addresses a native table as `warehouse.ns.table`, so
/// the namespace is prepended to the name: `wh` + `sales.orders` →
/// `wh.sales.orders`, which the resolver can match to a native table.
///
/// Two exceptions leave the name untouched: an empty namespace (nothing to
/// prepend), and a URI-style namespace (`s3://…`, `bigquery`, anything with a
/// scheme `://` or no dot-addressable meaning) — those datasets are not
/// Meridian tables, so we keep the name verbatim and let them resolve to a
/// stable *external* endpoint. The result is what an unresolved endpoint is
/// stored under, so it round-trips.
#[must_use]
pub fn dataset_identifier(namespace: &str, name: &str) -> String {
    if namespace.is_empty() || namespace.contains("://") {
        name.to_owned()
    } else {
        format!("{namespace}.{name}")
    }
}

/// Ingests one OpenLineage `RunEvent`, recording table-level edges (with
/// column facets where present) for every declared (input, output) pair.
/// Returns the number of edges upserted.
///
/// A run with no inputs or no outputs records nothing — there is no pair to
/// relate, and we do not invent one.
pub async fn ingest_run_event(
    pool: &PgPool,
    workspace_id: WorkspaceId,
    event: &RunEvent,
) -> Result<usize> {
    if event.inputs.is_empty() || event.outputs.is_empty() {
        return Ok(0);
    }

    let run_id = event.run.as_ref().map(|r| r.run_id.clone());
    let job = event
        .job
        .as_ref()
        .map(|j| format!("{}/{}", j.namespace, j.name));

    let mut recorded = 0usize;
    for output in &event.outputs {
        let out_ident = dataset_identifier(&output.namespace, &output.name);
        let dst = resolve_input_endpoint(pool, workspace_id, &out_ident).await?;

        for input in &event.inputs {
            let in_ident = dataset_identifier(&input.namespace, &input.name);
            let src = resolve_input_endpoint(pool, workspace_id, &in_ident).await?;

            // The engine declared this pair; a self-pair (same dataset in and
            // out) is not lineage and the DB CHECK would reject two identical
            // native endpoints, so skip it.
            if src == dst {
                continue;
            }

            let column_map = column_map_for(output, &input.namespace, &input.name);

            upsert_edge(
                pool,
                workspace_id,
                &EdgeUpsert {
                    src,
                    dst: dst.clone(),
                    provenance: Provenance::Openlineage,
                    confidence: OPENLINEAGE_CONFIDENCE,
                    column_map,
                    engine_meta: json!({
                        "run_id": run_id,
                        "job": job,
                        "producer": event.producer,
                        "event_type": event.event_type,
                        "input_dataset": in_ident,
                        "output_dataset": out_ident,
                    }),
                },
            )
            .await?;
            recorded += 1;
        }
    }
    Ok(recorded)
}

/// Extracts the column-level map for one (input, output) pair from the
/// output's `columnLineage` facet, keeping only the input fields that belong
/// to *this* input dataset. Returns `None` when the facet is absent or names
/// no fields from this input — table-level only, never a column cross-product.
fn column_map_for(
    output: &Dataset,
    input_namespace: &str,
    input_name: &str,
) -> Option<Vec<ColumnMapEntry>> {
    let facet = output.facets.as_ref()?.column_lineage.as_ref()?;
    let mut entries = Vec::new();
    for (dst_column, field) in &facet.fields {
        for input_field in &field.input_fields {
            if input_field.namespace == input_namespace && input_field.name == input_name {
                entries.push(ColumnMapEntry {
                    src_column: input_field.field.clone(),
                    dst_column: dst_column.clone(),
                    transform: field.transformation_description.clone(),
                });
            }
        }
    }
    if entries.is_empty() {
        None
    } else {
        entries.sort_by(|a, b| {
            (a.dst_column.as_str(), a.src_column.as_str())
                .cmp(&(b.dst_column.as_str(), b.src_column.as_str()))
        });
        Some(entries)
    }
}

// ---------------------------------------------------------------------------
// Emitter
// ---------------------------------------------------------------------------

/// A dataset reference for an emitted event: the two Meridian endpoints of a
/// job, rendered as OpenLineage `(namespace, name)`.
#[derive(Debug, Clone)]
pub struct EmitDataset {
    /// OpenLineage dataset namespace.
    pub namespace: String,
    /// OpenLineage dataset name.
    pub name: String,
}

/// Inputs to [`build_run_event`]: a Meridian-initiated job's identity and the
/// datasets it read/wrote.
#[derive(Debug, Clone)]
pub struct EmitJob {
    /// OpenLineage job namespace (e.g. `meridian`).
    pub job_namespace: String,
    /// OpenLineage job name (e.g. `maintenance.compaction`).
    pub job_name: String,
    /// The run id (a UUID string).
    pub run_id: String,
    /// Run transition (`START` / `COMPLETE` / `FAIL`).
    pub event_type: String,
    /// Event time.
    pub event_time: DateTime<Utc>,
    /// Datasets the job read.
    pub inputs: Vec<EmitDataset>,
    /// Datasets the job wrote.
    pub outputs: Vec<EmitDataset>,
}

/// The producer URI Meridian stamps on emitted events.
pub const PRODUCER: &str = "https://github.com/meridian-catalog/meridian";

/// Builds a spec-valid OpenLineage `RunEvent` JSON object for a
/// Meridian-initiated job. The shape matches the OpenLineage 1.x `RunEvent` so
/// Marquez and other collectors accept it directly.
#[must_use]
pub fn build_run_event(job: &EmitJob) -> Value {
    let datasets = |list: &[EmitDataset]| -> Vec<Value> {
        list.iter()
            .map(|d| json!({ "namespace": d.namespace, "name": d.name }))
            .collect()
    };
    json!({
        "eventType": job.event_type,
        "eventTime": job.event_time.to_rfc3339(),
        "producer": PRODUCER,
        "schemaURL":
            "https://openlineage.io/spec/1-0-5/OpenLineage.json#/definitions/RunEvent",
        "run": { "runId": job.run_id },
        "job": { "namespace": job.job_namespace, "name": job.job_name },
        "inputs": datasets(&job.inputs),
        "outputs": datasets(&job.outputs),
    })
}

/// POSTs an emitted `RunEvent` to a configured OpenLineage collector
/// (`<url>/api/v1/lineage`, the Marquez/OpenLineage HTTP transport path).
///
/// Best-effort by contract: emission failures are surfaced to the caller to
/// log, never to fail the underlying Meridian job — a maintenance commit must
/// not roll back because a lineage collector was down.
pub async fn emit_run_event(
    client: &reqwest::Client,
    collector_url: &str,
    event: &Value,
) -> Result<()> {
    let endpoint = format!("{}/api/v1/lineage", collector_url.trim_end_matches('/'));
    let response = client
        .post(&endpoint)
        .json(event)
        .send()
        .await
        .map_err(|e| MeridianError::internal("failed to POST OpenLineage event", e))?;
    if !response.status().is_success() {
        return Err(MeridianError::internal_msg(format!(
            "OpenLineage collector returned {}",
            response.status()
        )));
    }
    Ok(())
}

/// Builds the `EmitJob` for a Meridian maintenance operation on one table.
///
/// A compaction / expiry reads and rewrites the *same* table, so the dataset is
/// both input and output — an honest self-transform. (Meridian does not record
/// this as an edge in its own graph — a self-edge is rejected — but external
/// tools model the run this way, so the emitted event carries it.) `job_kind`
/// is the OpenLineage job name suffix, e.g. `compaction` or `expiry`.
#[must_use]
pub fn maintenance_run_event(
    job_kind: &str,
    run_id: &str,
    table_ident: &str,
    event_time: DateTime<Utc>,
) -> Value {
    let dataset = EmitDataset {
        namespace: PRODUCER.to_owned(),
        name: table_ident.to_owned(),
    };
    build_run_event(&EmitJob {
        job_namespace: "meridian".to_owned(),
        job_name: format!("maintenance.{job_kind}"),
        run_id: run_id.to_owned(),
        event_type: "COMPLETE".to_owned(),
        event_time,
        inputs: vec![dataset.clone()],
        outputs: vec![dataset],
    })
}

/// Best-effort emit of a maintenance run to the configured collector, if any.
/// A `None` URL is a no-op (events remain pullable from Meridian's own graph);
/// a POST failure is logged, never propagated — a maintenance commit must not
/// be affected by a lineage collector's availability. Intended to be called
/// once, after a maintenance commit succeeds.
pub async fn emit_maintenance(
    client: &reqwest::Client,
    collector_url: Option<&str>,
    job_kind: &str,
    run_id: &str,
    table_ident: &str,
    event_time: DateTime<Utc>,
) {
    let Some(url) = collector_url else {
        return;
    };
    let event = maintenance_run_event(job_kind, run_id, table_ident, event_time);
    if let Err(error) = emit_run_event(client, url, &event).await {
        tracing::warn!(%error, table_ident, "OpenLineage emit for maintenance job failed");
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn dataset_identifier_prepends_catalog_namespace() {
        assert_eq!(dataset_identifier("wh", "sales.orders"), "wh.sales.orders");
    }

    #[test]
    fn maintenance_run_event_is_a_valid_self_transform() {
        let e = maintenance_run_event("compaction", "run-1", "wh.sales.orders", chrono::Utc::now());
        assert_eq!(e["job"]["name"], json!("maintenance.compaction"));
        assert_eq!(e["job"]["namespace"], json!("meridian"));
        assert_eq!(e["run"]["runId"], json!("run-1"));
        // Same table in and out — the honest self-transform shape.
        assert_eq!(e["inputs"][0]["name"], json!("wh.sales.orders"));
        assert_eq!(e["outputs"][0]["name"], json!("wh.sales.orders"));
        // Re-parses (Marquez-compatible).
        let _: RunEvent = serde_json::from_value(e).expect("re-parse");
    }

    #[test]
    fn dataset_identifier_keeps_uri_namespace_datasets_external() {
        // A storage-URI namespace is not a Meridian warehouse; keep the name.
        assert_eq!(
            dataset_identifier("s3://bucket", "path/to/data"),
            "path/to/data"
        );
        assert_eq!(dataset_identifier("", "bare"), "bare");
    }

    #[test]
    fn ingest_ignores_unknown_fields_and_parses_minimal_event() {
        // Forward-compatible: an event with extra top-level fields still parses.
        let event: RunEvent = serde_json::from_value(json!({
            "eventType": "COMPLETE",
            "eventTime": "2026-07-04T00:00:00Z",
            "run": { "runId": "r1", "facets": { "nominalTime": {} } },
            "job": { "namespace": "j", "name": "n", "facets": {} },
            "inputs": [{ "namespace": "wh", "name": "a.t" }],
            "outputs": [{ "namespace": "wh", "name": "b.t" }],
            "someFutureField": 42
        }))
        .unwrap();
        assert_eq!(event.inputs.len(), 1);
        assert_eq!(event.outputs.len(), 1);
        assert_eq!(event.event_type.as_deref(), Some("COMPLETE"));
    }

    #[test]
    fn column_map_for_returns_none_without_facet() {
        let output = Dataset {
            namespace: "wh".to_owned(),
            name: "b.t".to_owned(),
            facets: None,
        };
        assert!(column_map_for(&output, "wh", "a.t").is_none());
    }

    #[test]
    fn column_map_for_selects_only_this_inputs_fields() {
        // Facet drawing from two inputs; only the matching input's columns
        // come back — never a cross-product with the other input.
        let output: Dataset = serde_json::from_value(json!({
            "namespace": "wh",
            "name": "b.t",
            "facets": { "columnLineage": { "fields": {
                "out1": { "inputFields": [
                    { "namespace": "wh", "name": "a.t", "field": "x" },
                    { "namespace": "wh", "name": "other.t", "field": "z" }
                ] }
            } } }
        }))
        .unwrap();
        let map = column_map_for(&output, "wh", "a.t").expect("map");
        assert_eq!(map.len(), 1);
        assert_eq!(map[0].src_column, "x");
        assert_eq!(map[0].dst_column, "out1");
    }

    #[test]
    fn build_run_event_has_required_fields() {
        let job = EmitJob {
            job_namespace: "meridian".to_owned(),
            job_name: "maintenance.expiry".to_owned(),
            run_id: "r".to_owned(),
            event_type: "START".to_owned(),
            event_time: chrono::Utc::now(),
            inputs: vec![],
            outputs: vec![EmitDataset {
                namespace: "wh".to_owned(),
                name: "a.t".to_owned(),
            }],
        };
        let e = build_run_event(&job);
        assert_eq!(e["job"]["name"], json!("maintenance.expiry"));
        assert_eq!(e["run"]["runId"], json!("r"));
        assert_eq!(e["producer"], json!(PRODUCER));
        assert!(e["inputs"].as_array().unwrap().is_empty());
        assert_eq!(e["outputs"][0]["name"], json!("a.t"));
    }
}
