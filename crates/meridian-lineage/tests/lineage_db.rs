//! Database-backed tests for the lineage core (Pillar F).
//!
//! Covers the five required scenarios plus the no-fabrication guarantee:
//!  1. commit-native lineage from a snapshot summary carrying declared inputs
//!     + an engine id → an edge with commit confidence and `engine_meta`;
//!  2. the OpenLineage sink parsing a realistic 1.x `RunEvent` → edges including
//!     column facets;
//!  3. no-fabrication: an anonymous commit (engine id, no declared inputs)
//!     records zero edges — no cartesian expansion;
//!  4. impact analysis returning the correct downstream set on a seeded graph;
//!  5. the emitter producing a valid OpenLineage event for a maintenance job.
//!
//! Requires a running Postgres and `DATABASE_URL`; skips without it. Every
//! test seeds uniquely-named assets and scopes its assertions to its own ids,
//! so the tests are isolated from each other and from any pre-existing data.

use std::collections::BTreeMap;

use chrono::TimeZone;
use meridian_common::config::DatabaseConfig;
use meridian_common::id::WorkspaceId;
use meridian_lineage::commit_hook::{COMMIT_CONFIDENCE, record_commit_lineage};
use meridian_lineage::impact::{self, Change, Direction};
use meridian_lineage::model::{Endpoint, Provenance};
use meridian_lineage::openlineage::{self, RunEvent};
use meridian_store::table::{self, NewTable};
use meridian_store::{namespace, tenancy, warehouse};
use serde_json::json;
use sqlx::PgPool;
use ulid::Ulid;

struct Fixture {
    pool: PgPool,
    workspace: WorkspaceId,
    warehouse_name: String,
    levels: Vec<String>,
    namespace_id: String,
}

async fn fixture() -> Option<Fixture> {
    let Ok(url) = std::env::var("DATABASE_URL") else {
        eprintln!("skipping lineage DB test: DATABASE_URL is not set");
        return None;
    };
    let config = DatabaseConfig {
        url,
        ..DatabaseConfig::default()
    };
    let pool = meridian_store::connect(&config)
        .await
        .expect("connect to test database");
    meridian_store::MIGRATOR.run(&pool).await.expect("migrate");

    // Unique warehouse + namespace per test run, so ids never collide with a
    // sibling test or leftover data.
    let run = Ulid::new().to_string().to_lowercase();
    let workspace = tenancy::default_workspace_id();
    let warehouse_name = format!("lin-wh-{run}");
    let wh = warehouse::create(
        &pool,
        workspace,
        &warehouse_name,
        "file:///tmp/lineage-test",
        BTreeMap::new(),
        "test:lineage",
    )
    .await
    .expect("warehouse");
    let levels = vec![format!("lin_ns_{run}")];
    let ns = namespace::create(
        &pool,
        workspace,
        &wh.id,
        &levels,
        BTreeMap::new(),
        "test:lineage",
    )
    .await
    .expect("namespace");

    Some(Fixture {
        pool,
        workspace,
        warehouse_name,
        levels,
        namespace_id: ns.id,
    })
}

impl Fixture {
    /// Creates a table with the given short name and optional `owner` property,
    /// returning its id. The metadata location is a placeholder — the lineage
    /// paths never read the file.
    async fn table(&self, name: &str, owner: Option<&str>) -> String {
        let mut props = BTreeMap::new();
        if let Some(owner) = owner {
            props.insert("owner".to_owned(), owner.to_owned());
        }
        let uuid = uuid_like();
        let record = table::create(
            &self.pool,
            NewTable {
                workspace_id: self.workspace,
                namespace_id: &self.namespace_id,
                namespace_levels: &self.levels,
                name,
                table_uuid: &uuid,
                metadata_location: "file:///tmp/lineage-test/meta.json",
                format_version: 2,
                properties: &props,
                schema_text: None,
                snapshots: &[],
                origin: "create",
            },
            "test:lineage",
            None,
        )
        .await
        .expect("table");
        record.id
    }

    /// The dotted identifier `warehouse.ns.table` that resolves to `name`.
    fn ident(&self, name: &str) -> String {
        format!("{}.{}.{}", self.warehouse_name, self.levels[0], name)
    }
}

fn uuid_like() -> String {
    // A hyphenated pseudo-UUID unique per call (the column only needs
    // uniqueness, not RFC-4122 validity, for these tests).
    let u = Ulid::new().to_string().to_lowercase();
    format!(
        "{}-{}-{}-{}-{}",
        &u[0..8],
        &u[8..12],
        &u[12..16],
        &u[16..20],
        &u[20..26]
    )
}

// ---------------------------------------------------------------------------
// 1. Commit-native lineage from a snapshot summary
// ---------------------------------------------------------------------------

#[tokio::test]
async fn commit_summary_with_declared_input_records_edge() {
    let Some(fx) = fixture().await else { return };
    let orders = fx.table("orders", None).await;
    let daily = fx.table("daily_sales", None).await;

    // A snapshot summary that both declares its input table AND carries an
    // engine id — exactly what a dbt/Spark commit writes when it knows sources.
    let summary = json!({
        "operation": "append",
        "spark.app.id": "application_1700000000000_0042",
        "meridian.lineage.inputs": [fx.ident("orders")],
    });

    let recorded = record_commit_lineage(&fx.pool, fx.workspace, &daily, &summary)
        .await
        .expect("record commit lineage");
    assert_eq!(recorded, 1, "one declared input → one edge");

    let ups = meridian_lineage::upstream_edges(&fx.pool, fx.workspace, &daily)
        .await
        .expect("upstream");
    assert_eq!(ups.len(), 1);
    let edge = &ups[0];
    assert_eq!(
        edge.src,
        Endpoint::table(&orders),
        "src resolved to the native table"
    );
    assert_eq!(edge.dst, Endpoint::table(&daily));
    assert_eq!(edge.provenance, Provenance::Commit);
    assert!((edge.confidence - COMMIT_CONFIDENCE).abs() < 1e-9);
    assert!(
        edge.column_map.is_none(),
        "commit lineage is table-level only"
    );
    // The engine id is captured as evidence.
    assert_eq!(
        edge.engine_meta["engine"]["spark.app.id"],
        json!("application_1700000000000_0042"),
    );
}

// ---------------------------------------------------------------------------
// 2. OpenLineage sink parses a realistic 1.x RunEvent (incl. column facets)
// ---------------------------------------------------------------------------

#[tokio::test]
async fn openlineage_sink_parses_run_event_with_column_facets() {
    let Some(fx) = fixture().await else { return };
    let raw = fx.table("raw_events", None).await;
    let sessions = fx.table("sessions", None).await;

    // A realistic OpenLineage 1.x COMPLETE event from a Spark integration,
    // with a columnLineage output facet mapping user_id -> uid.
    let event: RunEvent = serde_json::from_value(json!({
        "eventType": "COMPLETE",
        "eventTime": "2026-07-04T10:00:00.000Z",
        "producer": "https://github.com/OpenLineage/OpenLineage/tree/1.20.0/integration/spark",
        "run": { "runId": "d9f2b1c0-0000-4000-8000-000000000001" },
        "job": { "namespace": "spark", "name": "build_sessions" },
        "inputs": [
            {
                "namespace": fx.warehouse_name,
                "name": format!("{}.raw_events", fx.levels[0]),
            }
        ],
        "outputs": [
            {
                "namespace": fx.warehouse_name,
                "name": format!("{}.sessions", fx.levels[0]),
                "facets": {
                    "columnLineage": {
                        "fields": {
                            "uid": {
                                "inputFields": [
                                    {
                                        "namespace": fx.warehouse_name,
                                        "name": format!("{}.raw_events", fx.levels[0]),
                                        "field": "user_id"
                                    }
                                ],
                                "transformationDescription": "IDENTITY"
                            }
                        }
                    }
                }
            }
        ]
    }))
    .expect("parse RunEvent");

    let recorded = openlineage::ingest_run_event(&fx.pool, fx.workspace, &event)
        .await
        .expect("ingest");
    assert_eq!(recorded, 1, "one (input,output) pair → one edge");

    let ups = meridian_lineage::upstream_edges(&fx.pool, fx.workspace, &sessions)
        .await
        .expect("upstream");
    assert_eq!(ups.len(), 1);
    let edge = &ups[0];
    assert_eq!(edge.src, Endpoint::table(&raw));
    assert_eq!(edge.dst, Endpoint::table(&sessions));
    assert_eq!(edge.provenance, Provenance::Openlineage);
    assert!(edge.confidence > 0.9);
    let cols = edge.column_map.as_ref().expect("column map present");
    assert_eq!(cols.len(), 1);
    assert_eq!(cols[0].src_column, "user_id");
    assert_eq!(cols[0].dst_column, "uid");
    assert_eq!(cols[0].transform.as_deref(), Some("IDENTITY"));
}

// ---------------------------------------------------------------------------
// 3. No fabrication: engine id but no declared inputs → zero edges
// ---------------------------------------------------------------------------

#[tokio::test]
async fn commit_without_declared_inputs_records_nothing() {
    let Some(fx) = fixture().await else { return };
    // Seed several tables so a naive implementation might be tempted to relate
    // them all — the cartesian failure mode. There must be none.
    let a = fx.table("anon_a", None).await;
    let _b = fx.table("anon_b", None).await;
    let _c = fx.table("anon_c", None).await;

    // A commit that carries an engine id but declares NO inputs.
    let summary = json!({
        "operation": "overwrite",
        "spark.app.id": "application_1700000000000_9999",
        "added-records": "1000",
    });

    let recorded = record_commit_lineage(&fx.pool, fx.workspace, &a, &summary)
        .await
        .expect("record");
    assert_eq!(recorded, 0, "no declared inputs → no edges, ever");

    // And genuinely nothing was written for this destination.
    let ups = meridian_lineage::upstream_edges(&fx.pool, fx.workspace, &a)
        .await
        .expect("upstream");
    assert!(ups.is_empty(), "no fabricated upstream edges");

    // An OpenLineage event with inputs but no outputs (or vice versa) is also a
    // no-op — there is no declared pair to relate.
    let one_sided: RunEvent = serde_json::from_value(json!({
        "eventType": "COMPLETE",
        "run": { "runId": "d9f2b1c0-0000-4000-8000-00000000000f" },
        "job": { "namespace": "spark", "name": "reads_only" },
        "inputs": [{ "namespace": fx.warehouse_name, "name": format!("{}.anon_b", fx.levels[0]) }],
        "outputs": []
    }))
    .expect("parse");
    let n = openlineage::ingest_run_event(&fx.pool, fx.workspace, &one_sided)
        .await
        .expect("ingest");
    assert_eq!(n, 0, "an input-only run relates nothing");
}

// ---------------------------------------------------------------------------
// 4. Impact analysis over a seeded graph
// ---------------------------------------------------------------------------

#[tokio::test]
async fn impact_returns_correct_downstream_set() {
    let Some(fx) = fixture().await else { return };
    // Graph:  raw --> stg --> mart(owner=analytics) --> dashboard(owner=bi)
    //           \--> audit (a downstream of raw only)
    let raw = fx.table("imp_raw", None).await;
    let stg = fx.table("imp_stg", None).await;
    let mart = fx.table("imp_mart", Some("analytics@example.com")).await;
    let dash = fx.table("imp_dash", Some("bi@example.com")).await;
    let audit = fx.table("imp_audit", None).await;

    seed_edge(&fx, &raw, &stg).await;
    seed_edge(&fx, &stg, &mart).await;
    seed_edge(&fx, &mart, &dash).await;
    seed_edge(&fx, &raw, &audit).await;

    // Downstream graph of raw at depth 3 reaches stg, mart, dash, audit.
    let graph = impact::lineage_graph(&fx.pool, fx.workspace, &raw, Direction::Downstream, 3)
        .await
        .expect("graph");
    let reached: std::collections::BTreeSet<String> =
        graph.nodes.iter().map(|n| n.table_id.clone()).collect();
    for id in [&raw, &stg, &mart, &dash, &audit] {
        assert!(reached.contains(id), "graph reaches {id}");
    }

    // Impact of dropping raw: the downstream blast radius is stg, mart, dash,
    // audit, with owners analytics + bi collected for notification.
    let report = impact::impact_of(&fx.pool, fx.workspace, &raw, &Change::DropTable, 5)
        .await
        .expect("impact");
    let affected: std::collections::BTreeSet<String> =
        report.affected.iter().map(|a| a.table_id.clone()).collect();
    assert_eq!(
        affected,
        [&stg, &mart, &dash, &audit]
            .into_iter()
            .cloned()
            .collect::<std::collections::BTreeSet<_>>(),
        "exact downstream set",
    );
    assert!(
        !affected.contains(&raw),
        "the asset itself is not in its own blast radius"
    );
    assert_eq!(
        report.owners,
        vec![
            "analytics@example.com".to_owned(),
            "bi@example.com".to_owned()
        ],
        "distinct affected owners, sorted",
    );

    // Depth clamps: at depth 1 only the direct consumers (stg, audit) appear.
    let shallow = impact::impact_of(&fx.pool, fx.workspace, &raw, &Change::DropTable, 1)
        .await
        .expect("impact d1");
    let shallow_set: std::collections::BTreeSet<String> = shallow
        .affected
        .iter()
        .map(|a| a.table_id.clone())
        .collect();
    assert_eq!(
        shallow_set,
        [&stg, &audit]
            .into_iter()
            .cloned()
            .collect::<std::collections::BTreeSet<_>>(),
    );
}

/// Seeds one high-confidence OpenLineage-style edge between two native tables
/// via the sink, so impact tests exercise the same write path as production.
async fn seed_edge(fx: &Fixture, src: &str, dst: &str) {
    // Resolve idents by looking each id's table name back up would be circular;
    // instead seed via a direct upsert to keep the graph construction obvious.
    meridian_lineage::upsert_edge(
        &fx.pool,
        fx.workspace,
        &meridian_lineage::EdgeUpsert {
            src: Endpoint::table(src),
            dst: Endpoint::table(dst),
            provenance: Provenance::Openlineage,
            confidence: 0.95,
            column_map: None,
            engine_meta: json!({ "test": "seed" }),
        },
    )
    .await
    .expect("seed edge");
}

// ---------------------------------------------------------------------------
// 5. Emitter produces a valid OpenLineage event for a maintenance job
// ---------------------------------------------------------------------------

#[tokio::test]
async fn emitter_builds_valid_openlineage_event() {
    use openlineage::{EmitDataset, EmitJob};

    let event_time = chrono::Utc.with_ymd_and_hms(2026, 7, 4, 12, 0, 0).unwrap();
    let job = EmitJob {
        job_namespace: "meridian".to_owned(),
        job_name: "maintenance.compaction".to_owned(),
        run_id: "d9f2b1c0-0000-4000-8000-0000000000aa".to_owned(),
        event_type: "COMPLETE".to_owned(),
        event_time,
        inputs: vec![EmitDataset {
            namespace: "wh".to_owned(),
            name: "sales.orders".to_owned(),
        }],
        outputs: vec![EmitDataset {
            namespace: "wh".to_owned(),
            name: "sales.orders".to_owned(),
        }],
    };
    let event = openlineage::build_run_event(&job);

    // Required RunEvent fields per the OpenLineage 1.x schema.
    assert_eq!(event["eventType"], json!("COMPLETE"));
    assert_eq!(
        event["run"]["runId"],
        json!("d9f2b1c0-0000-4000-8000-0000000000aa")
    );
    assert_eq!(event["job"]["namespace"], json!("meridian"));
    assert_eq!(event["job"]["name"], json!("maintenance.compaction"));
    assert_eq!(event["producer"], json!(openlineage::PRODUCER));
    assert!(
        event["eventTime"]
            .as_str()
            .unwrap()
            .starts_with("2026-07-04T12:00:00")
    );
    assert!(event["schemaURL"].as_str().unwrap().contains("RunEvent"));
    assert_eq!(event["inputs"][0]["name"], json!("sales.orders"));
    assert_eq!(event["outputs"][0]["namespace"], json!("wh"));

    // Round-trips back through our own parser (Marquez-compatible shape).
    let parsed: RunEvent = serde_json::from_value(event).expect("emitted event re-parses");
    assert_eq!(parsed.event_type.as_deref(), Some("COMPLETE"));
    assert_eq!(parsed.inputs.len(), 1);
    assert_eq!(parsed.outputs.len(), 1);
}

// ---------------------------------------------------------------------------
// Bonus: column-scoped impact attributes via the column map
// ---------------------------------------------------------------------------

#[tokio::test]
async fn column_scoped_impact_tracks_the_column() {
    let Some(fx) = fixture().await else { return };
    let src = fx.table("col_src", None).await;
    let dst = fx.table("col_dst", Some("owner@example.com")).await;

    // Edge carrying a column map: src.email -> dst.contact.
    meridian_lineage::upsert_edge(
        &fx.pool,
        fx.workspace,
        &meridian_lineage::EdgeUpsert {
            src: Endpoint::table(&src),
            dst: Endpoint::table(&dst),
            provenance: Provenance::Openlineage,
            confidence: 0.95,
            column_map: Some(vec![meridian_lineage::ColumnMapEntry {
                src_column: "email".to_owned(),
                dst_column: "contact".to_owned(),
                transform: None,
            }]),
            engine_meta: json!({}),
        },
    )
    .await
    .expect("edge");

    // Dropping the mapped column reaches dst, attributed to the dst column.
    let hit = impact::impact_of(
        &fx.pool,
        fx.workspace,
        &src,
        &Change::DropColumn("email".to_owned()),
        3,
    )
    .await
    .expect("impact email");
    assert_eq!(hit.affected.len(), 1);
    assert_eq!(hit.affected[0].table_id, dst);
    assert_eq!(hit.affected[0].via_column.as_deref(), Some("contact"));

    // Dropping an unmapped column does NOT reach dst through this column-precise
    // edge — no fabricated column dependency.
    let miss = impact::impact_of(
        &fx.pool,
        fx.workspace,
        &src,
        &Change::DropColumn("unrelated".to_owned()),
        3,
    )
    .await
    .expect("impact unrelated");
    assert!(
        miss.affected.is_empty(),
        "a column the edge does not map is not attributed downstream",
    );
}
