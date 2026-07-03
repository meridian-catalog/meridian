//! Database-backed tests for the table store module (row lifecycle; the
//! commit path itself is covered by `commit_properties_pg.rs` and the
//! server's endpoint tests).
//!
//! These require a running Postgres and `DATABASE_URL`; without it they skip
//! (with a note on stderr).

use std::collections::BTreeMap;

use meridian_common::MeridianError;
use meridian_common::config::DatabaseConfig;
use meridian_store::table::{self, NewTable};
use meridian_store::{namespace, tenancy, warehouse};
use sqlx::PgPool;
use ulid::Ulid;

struct Fixture {
    pool: PgPool,
    warehouse_id: String,
    namespace_id: String,
    levels: Vec<String>,
}

async fn fixture() -> Option<Fixture> {
    let Ok(url) = std::env::var("DATABASE_URL") else {
        eprintln!("skipping table DB test: DATABASE_URL is not set");
        return None;
    };
    let config = DatabaseConfig {
        url,
        ..DatabaseConfig::default()
    };
    let pool = meridian_store::connect(&config)
        .await
        .expect("connect to test database");
    meridian_store::MIGRATOR
        .run(&pool)
        .await
        .expect("run migrations");

    let run = Ulid::new().to_string().to_lowercase();
    let workspace = tenancy::default_workspace_id();
    let wh = warehouse::create(
        &pool,
        workspace,
        &format!("tbl-wh-{run}"),
        "s3://table-tests/root",
        BTreeMap::new(),
        "test:table-db",
    )
    .await
    .expect("create warehouse");
    let levels = vec![format!("tbl_ns_{run}")];
    let ns = namespace::create(
        &pool,
        workspace,
        &wh.id,
        &levels,
        BTreeMap::new(),
        "test:table-db",
    )
    .await
    .expect("create namespace");

    Some(Fixture {
        pool,
        warehouse_id: wh.id,
        namespace_id: ns.id,
        levels,
    })
}

async fn insert_table(fx: &Fixture, name: &str) -> table::TableRecord {
    let uuid = format!("uuid-{}", Ulid::new());
    table::create(
        &fx.pool,
        NewTable {
            workspace_id: tenancy::default_workspace_id(),
            namespace_id: &fx.namespace_id,
            namespace_levels: &fx.levels,
            name,
            table_uuid: &uuid,
            metadata_location: "s3://table-tests/root/t/metadata/00000-x.metadata.json",
            format_version: 2,
            properties: &BTreeMap::from([("k".to_owned(), "v".to_owned())]),
            schema_text: None,
            origin: "create",
        },
        "test:table-db",
        None,
    )
    .await
    .expect("insert table")
}

#[tokio::test]
async fn create_get_and_duplicate_conflict() {
    let Some(fx) = fixture().await else { return };

    let record = insert_table(&fx, "t1").await;
    assert_eq!(record.pointer_version, 0);
    assert_eq!(record.format_version, 2);
    assert_eq!(record.properties.0.get("k").map(String::as_str), Some("v"));

    // get by namespace id and by (warehouse, levels, name) agree.
    let by_ns = table::get(&fx.pool, &fx.namespace_id, "t1")
        .await
        .expect("get")
        .expect("exists");
    assert_eq!(by_ns.id, record.id);
    let by_name = table::get_by_name(&fx.pool, &fx.warehouse_id, &fx.levels, "t1")
        .await
        .expect("get_by_name")
        .expect("exists");
    assert_eq!(by_name.id, record.id);

    // The create wrote its audit row and outbox event atomically.
    let audited: i64 = sqlx::query_scalar("SELECT count(*) FROM audit_log WHERE resource = $1")
        .bind(format!("table:{}", record.id))
        .fetch_one(&fx.pool)
        .await
        .expect("count audit rows");
    assert_eq!(audited, 1);
    let events: i64 = sqlx::query_scalar("SELECT count(*) FROM events_outbox WHERE aggregate = $1")
        .bind(format!("table:{}", record.id))
        .fetch_one(&fx.pool)
        .await
        .expect("count outbox events");
    assert_eq!(events, 1);

    // Duplicate name conflicts; nothing half-created.
    let error = table::create(
        &fx.pool,
        NewTable {
            workspace_id: tenancy::default_workspace_id(),
            namespace_id: &fx.namespace_id,
            namespace_levels: &fx.levels,
            name: "t1",
            table_uuid: &format!("uuid-{}", Ulid::new()),
            metadata_location: "s3://table-tests/root/other.metadata.json",
            format_version: 2,
            properties: &BTreeMap::new(),
            schema_text: None,
            origin: "create",
        },
        "test:table-db",
        None,
    )
    .await
    .expect_err("duplicate must conflict");
    assert!(matches!(error, MeridianError::Conflict(_)), "{error}");
}

#[tokio::test]
async fn listing_uses_keyset_pagination() {
    let Some(fx) = fixture().await else { return };
    for name in ["a", "b", "c"] {
        insert_table(&fx, name).await;
    }

    let all = table::list(&fx.pool, &fx.namespace_id, None, None)
        .await
        .expect("list all");
    assert_eq!(all.len(), 3);

    let first = table::list(&fx.pool, &fx.namespace_id, None, Some(2))
        .await
        .expect("first page");
    assert_eq!(first.len(), 2);
    let rest = table::list(&fx.pool, &fx.namespace_id, Some(&first[1].id), Some(2))
        .await
        .expect("second page");
    assert_eq!(rest.len(), 1);
    let mut names: Vec<String> = first.into_iter().chain(rest).map(|r| r.name).collect();
    names.sort();
    assert_eq!(names, ["a", "b", "c"]);
}

#[tokio::test]
async fn rename_within_and_across_namespaces() {
    let Some(fx) = fixture().await else { return };
    let record = insert_table(&fx, "orig").await;
    let workspace = tenancy::default_workspace_id();

    // Same-namespace rename.
    table::rename(
        &fx.pool,
        workspace,
        &fx.warehouse_id,
        &fx.levels,
        "orig",
        &fx.levels,
        "renamed",
        "test:table-db",
    )
    .await
    .expect("rename in place");
    assert!(
        table::get(&fx.pool, &fx.namespace_id, "orig")
            .await
            .expect("get")
            .is_none()
    );

    // Cross-namespace move keeps identity.
    let other_levels = vec![format!(
        "tbl_ns2_{}",
        Ulid::new().to_string().to_lowercase()
    )];
    namespace::create(
        &fx.pool,
        workspace,
        &fx.warehouse_id,
        &other_levels,
        BTreeMap::new(),
        "test:table-db",
    )
    .await
    .expect("create second namespace");
    table::rename(
        &fx.pool,
        workspace,
        &fx.warehouse_id,
        &fx.levels,
        "renamed",
        &other_levels,
        "moved",
        "test:table-db",
    )
    .await
    .expect("move across namespaces");
    let moved = table::get_by_name(&fx.pool, &fx.warehouse_id, &other_levels, "moved")
        .await
        .expect("get moved")
        .expect("exists");
    assert_eq!(moved.id, record.id);
    assert_eq!(moved.table_uuid, record.table_uuid);

    // Missing source, missing destination namespace, occupied destination.
    let missing_source = table::rename(
        &fx.pool,
        workspace,
        &fx.warehouse_id,
        &fx.levels,
        "ghost",
        &fx.levels,
        "x",
        "test:table-db",
    )
    .await
    .expect_err("missing source");
    assert!(matches!(missing_source, MeridianError::NotFound(ref m) if m.starts_with("table")));

    let missing_dest = table::rename(
        &fx.pool,
        workspace,
        &fx.warehouse_id,
        &other_levels,
        "moved",
        &["nope".to_owned()],
        "x",
        "test:table-db",
    )
    .await
    .expect_err("missing destination namespace");
    assert!(matches!(missing_dest, MeridianError::NotFound(ref m) if m.starts_with("namespace")));

    insert_table(&fx, "occupied").await;
    let occupied = table::rename(
        &fx.pool,
        workspace,
        &fx.warehouse_id,
        &other_levels,
        "moved",
        &fx.levels,
        "occupied",
        "test:table-db",
    )
    .await
    .expect_err("occupied destination");
    assert!(matches!(occupied, MeridianError::Conflict(_)));
}

#[tokio::test]
async fn drop_removes_row_and_enqueues_purge_event_when_requested() {
    let Some(fx) = fixture().await else { return };
    let workspace = tenancy::default_workspace_id();

    insert_table(&fx, "gone").await;
    let dropped = table::drop_table(
        &fx.pool,
        workspace,
        &fx.warehouse_id,
        &fx.levels,
        "gone",
        true,
        "test:table-db",
    )
    .await
    .expect("drop");
    assert!(
        table::get(&fx.pool, &fx.namespace_id, "gone")
            .await
            .expect("get")
            .is_none()
    );
    let purge_events: i64 = sqlx::query_scalar(
        "SELECT count(*) FROM events_outbox
         WHERE aggregate = $1 AND event_type = 'table.purge_requested'",
    )
    .bind(format!("table:{}", dropped.id))
    .fetch_one(&fx.pool)
    .await
    .expect("count purge events");
    assert_eq!(purge_events, 1);

    let missing = table::drop_table(
        &fx.pool,
        workspace,
        &fx.warehouse_id,
        &fx.levels,
        "gone",
        false,
        "test:table-db",
    )
    .await
    .expect_err("double drop");
    assert!(matches!(missing, MeridianError::NotFound(_)));
}

#[tokio::test]
async fn metrics_reports_are_recorded_verbatim() {
    let Some(fx) = fixture().await else { return };
    let record = insert_table(&fx, "metered").await;

    let payload = serde_json::json!({ "report-type": "commit-report", "metrics": {} });
    let id = table::record_metrics_report(
        &fx.pool,
        tenancy::default_workspace_id(),
        &record.id,
        "ns.metered",
        Some("commit-report"),
        &payload,
    )
    .await
    .expect("record metrics");

    let (stored, ident): (serde_json::Value, String) =
        sqlx::query_as("SELECT report, table_ident FROM metrics_reports WHERE id = $1")
            .bind(&id)
            .fetch_one(&fx.pool)
            .await
            .expect("read back report");
    assert_eq!(stored, payload);
    assert_eq!(ident, "ns.metered");
}
