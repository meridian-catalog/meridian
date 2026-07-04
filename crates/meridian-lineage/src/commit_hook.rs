//! Commit-native table lineage (F-F1): derive lineage edges from a committed
//! snapshot's summary / engine properties, with zero pipeline setup.
//!
//! # What a commit summary actually tells us — and what it does not
//!
//! An Iceberg snapshot summary describes the *write* to one table: the
//! operation (`append`/`overwrite`/…), row/file deltas, and — when the engine
//! sets them — engine identity properties (`spark.app.id`, a Flink job id, a
//! Trino query id, a dbt invocation id). Crucially, a bare summary does **not**
//! enumerate the *source* tables the write read from. Inventing sources from
//! an engine id alone would produce exactly the cartesian
//! "everything-relates-to-everything" edges the spec (F-F3) forbids.
//!
//! So this module derives an edge **only** when the destination table's own
//! metadata carries an explicit, machine-readable declaration of its inputs.
//! Two honest sources of that declaration:
//!
//! 1. A snapshot-summary or table property listing input tables under a small
//!    set of well-known keys (`meridian.lineage.inputs`, `input-tables`,
//!    `source-tables`, `dbt.upstream`) — a comma/JSON list of table
//!    identifiers. Engines and dbt macros that know their sources can set
//!    these; when present they are ground truth.
//! 2. Nothing else. If no inputs are declared, **no edge is recorded** — the
//!    engine identity is still captured as `engine_meta` on whatever edges the
//!    other provenances (OpenLineage) produce, but a commit alone with no
//!    declared inputs yields zero edges. Unknown stays unknown, visibly.
//!
//! The engine identity (see [`engine_fingerprint`]) is attached to every
//! derived edge's `engine_meta` and calibrates confidence: an explicit input
//! list from a known engine is trustworthy but is still weaker evidence than a
//! full OpenLineage lineage facet, so commit edges land at
//! [`COMMIT_CONFIDENCE`].

use meridian_common::Result;
use meridian_common::id::WorkspaceId;
use serde_json::{Value, json};
use sqlx::PgPool;

use crate::model::{EdgeUpsert, Endpoint, Provenance, upsert_edge};
use crate::resolve::resolve_input_endpoint;

/// Confidence assigned to a commit-derived edge: high enough to surface, but
/// below an OpenLineage-declared edge — a commit input list is a real signal
/// yet weaker than an engine's explicit lineage facet.
pub const COMMIT_CONFIDENCE: f64 = 0.6;

/// Summary/property keys whose value is a list of this table's input tables.
/// Order is priority; the first key present wins. All are opt-in — an engine
/// or dbt macro must set one for a commit to yield lineage.
const INPUT_KEYS: &[&str] = &[
    "meridian.lineage.inputs",
    "input-tables",
    "source-tables",
    "dbt.upstream",
];

/// Engine-identity keys copied verbatim into an edge's `engine_meta`.
const ENGINE_KEYS: &[&str] = &[
    "spark.app.id",
    "flink.job.id",
    "flink.job.name",
    "trino.query.id",
    "engine-name",
    "engine-version",
    "dbt.invocation.id",
    "dbt.node",
];

/// Extracts the engine-identity fingerprint from a snapshot summary: the
/// subset of [`ENGINE_KEYS`] the writer set. Returns an empty object when the
/// summary is anonymous (a hand-rolled or unknown-engine commit).
#[must_use]
pub fn engine_fingerprint(summary: &Value) -> Value {
    let mut fp = serde_json::Map::new();
    if let Value::Object(map) = summary {
        for key in ENGINE_KEYS {
            if let Some(v @ Value::String(_)) = map.get(*key) {
                fp.insert((*key).to_owned(), v.clone());
            }
        }
    }
    Value::Object(fp)
}

/// Parses the declared input-table identifiers from a snapshot summary, if
/// any. Accepts either a JSON array of strings or a comma-separated string
/// under the first present [`INPUT_KEYS`] entry. Blank entries are dropped.
///
/// Returns an empty vec (not an edge) when nothing is declared — the
/// no-fabrication guarantee lives here: no declared inputs → no edges.
#[must_use]
pub fn declared_inputs(summary: &Value) -> Vec<String> {
    let Value::Object(map) = summary else {
        return Vec::new();
    };
    for key in INPUT_KEYS {
        let Some(value) = map.get(*key) else {
            continue;
        };
        let raw = match value {
            Value::Array(items) => items
                .iter()
                .filter_map(|i| i.as_str().map(str::to_owned))
                .collect::<Vec<_>>(),
            Value::String(s) => s.split(',').map(|p| p.trim().to_owned()).collect(),
            _ => Vec::new(),
        };
        let inputs: Vec<String> = raw.into_iter().filter(|s| !s.is_empty()).collect();
        if !inputs.is_empty() {
            return inputs;
        }
    }
    Vec::new()
}

/// Records commit-native lineage for one committed table, given its current
/// snapshot summary. Returns the number of edges recorded (0 when no inputs
/// are declared — the common, honest case).
///
/// `dst_table_id` is the committed (destination) table. Each declared input
/// identifier is resolved to a native Meridian table when it names one, and
/// otherwise recorded as an external endpoint — never dropped, never
/// cartesian-expanded. A declared input that resolves to the destination
/// itself (a self-reference, e.g. an in-place `overwrite`) is skipped.
pub async fn record_commit_lineage(
    pool: &PgPool,
    workspace_id: WorkspaceId,
    dst_table_id: &str,
    summary: &Value,
) -> Result<usize> {
    let inputs = declared_inputs(summary);
    if inputs.is_empty() {
        return Ok(0);
    }
    let fingerprint = engine_fingerprint(summary);

    let mut recorded = 0usize;
    for input in inputs {
        let src = resolve_input_endpoint(pool, workspace_id, &input).await?;
        // Skip a self-edge: an overwrite that lists the table itself as an
        // input is not lineage, and the DB CHECK would reject it anyway.
        if let Endpoint::Table { id } = &src
            && id == dst_table_id
        {
            continue;
        }
        upsert_edge(
            pool,
            workspace_id,
            &EdgeUpsert {
                src,
                dst: Endpoint::table(dst_table_id),
                provenance: Provenance::Commit,
                confidence: COMMIT_CONFIDENCE,
                column_map: None,
                engine_meta: json!({
                    "declared_input": input,
                    "engine": fingerprint,
                }),
            },
        )
        .await?;
        recorded += 1;
    }
    Ok(recorded)
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    #[test]
    fn declared_inputs_reads_json_array() {
        let s = json!({ "meridian.lineage.inputs": ["wh.ns.a", "wh.ns.b"] });
        assert_eq!(declared_inputs(&s), vec!["wh.ns.a", "wh.ns.b"]);
    }

    #[test]
    fn declared_inputs_reads_comma_string_and_trims() {
        let s = json!({ "source-tables": " wh.ns.a , wh.ns.b " });
        assert_eq!(declared_inputs(&s), vec!["wh.ns.a", "wh.ns.b"]);
    }

    #[test]
    fn declared_inputs_first_present_key_wins() {
        let s = json!({
            "meridian.lineage.inputs": ["a"],
            "dbt.upstream": ["b"],
        });
        assert_eq!(declared_inputs(&s), vec!["a"]);
    }

    #[test]
    fn declared_inputs_empty_when_none_declared() {
        // An engine id but no input list is the no-fabrication case.
        let s = json!({ "operation": "append", "spark.app.id": "x" });
        assert!(declared_inputs(&s).is_empty());
    }

    #[test]
    fn declared_inputs_drops_blank_entries() {
        let s = json!({ "input-tables": "a,,b, " });
        assert_eq!(declared_inputs(&s), vec!["a", "b"]);
    }

    #[test]
    fn engine_fingerprint_extracts_known_keys_only() {
        let s = json!({
            "operation": "append",
            "spark.app.id": "app-1",
            "trino.query.id": "q-1",
            "unrelated": "ignored",
        });
        let fp = engine_fingerprint(&s);
        assert_eq!(fp["spark.app.id"], json!("app-1"));
        assert_eq!(fp["trino.query.id"], json!("q-1"));
        assert!(fp.get("unrelated").is_none());
    }

    #[test]
    fn engine_fingerprint_empty_for_anonymous_commit() {
        assert_eq!(
            engine_fingerprint(&json!({ "operation": "append" })),
            json!({})
        );
    }
}
