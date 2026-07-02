//! Database-backed tests for warehouse and namespace queries.
//!
//! These require a running Postgres and `DATABASE_URL`; without it they skip
//! (with a note on stderr) so the suite stays runnable offline. Each test
//! creates its own uniquely-named warehouse, so tests are isolated from each
//! other and from previous runs against the same database.

use std::collections::BTreeMap;

use meridian_common::MeridianError;
use meridian_common::config::DatabaseConfig;
use meridian_store::{namespace, tenancy, warehouse};
use sqlx::PgPool;
use ulid::Ulid;

const PRINCIPAL: &str = "test:tenancy";

async fn test_pool() -> Option<PgPool> {
    let Ok(url) = std::env::var("DATABASE_URL") else {
        eprintln!("skipping DB test: DATABASE_URL is not set");
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
    Some(pool)
}

fn unique_name(prefix: &str) -> String {
    format!("{prefix}-{}", Ulid::new().to_string().to_lowercase())
}

async fn make_warehouse(pool: &PgPool) -> warehouse::WarehouseRecord {
    warehouse::create(
        pool,
        tenancy::default_workspace_id(),
        &unique_name("wh"),
        "s3://test-bucket/root",
        BTreeMap::new(),
        PRINCIPAL,
    )
    .await
    .expect("create warehouse")
}

fn levels(parts: &[&str]) -> Vec<String> {
    parts.iter().map(|p| (*p).to_owned()).collect()
}

#[tokio::test]
async fn migration_seeds_default_org_and_workspace() {
    let Some(pool) = test_pool().await else {
        return;
    };

    let (org_name,): (String,) = sqlx::query_as("SELECT name FROM organizations WHERE id = $1")
        .bind(tenancy::DEFAULT_ORG_ID)
        .fetch_one(&pool)
        .await
        .expect("default org row");
    assert_eq!(org_name, "default");

    let (ws_name, org_id): (String, String) =
        sqlx::query_as("SELECT name, org_id FROM workspaces WHERE id = $1")
            .bind(tenancy::DEFAULT_WORKSPACE_ID)
            .fetch_one(&pool)
            .await
            .expect("default workspace row");
    assert_eq!(ws_name, "default");
    assert_eq!(org_id, tenancy::DEFAULT_ORG_ID);
}

#[tokio::test]
async fn warehouse_create_list_get_delete_roundtrip() {
    let Some(pool) = test_pool().await else {
        return;
    };
    let ws = tenancy::default_workspace_id();

    let name = unique_name("wh");
    let mut options = BTreeMap::new();
    options.insert("region".to_owned(), "eu-central-1".to_owned());
    let created = warehouse::create(
        &pool,
        ws,
        &name,
        "s3://bucket/x",
        options.clone(),
        PRINCIPAL,
    )
    .await
    .expect("create warehouse");
    assert_eq!(created.name, name);
    assert_eq!(created.storage_root, "s3://bucket/x");
    assert_eq!(created.storage_config.0, options);

    let listed = warehouse::list(&pool, ws).await.expect("list warehouses");
    assert!(listed.iter().any(|w| w.id == created.id));

    let loaded = warehouse::get_by_name(&pool, ws, &name)
        .await
        .expect("get warehouse")
        .expect("warehouse exists");
    assert_eq!(loaded.id, created.id);

    warehouse::delete_by_name(&pool, ws, &name, PRINCIPAL)
        .await
        .expect("delete warehouse");
    assert!(
        warehouse::get_by_name(&pool, ws, &name)
            .await
            .expect("get after delete")
            .is_none()
    );
}

#[tokio::test]
async fn warehouse_duplicate_name_is_conflict() {
    let Some(pool) = test_pool().await else {
        return;
    };
    let ws = tenancy::default_workspace_id();
    let wh = make_warehouse(&pool).await;

    let err = warehouse::create(
        &pool,
        ws,
        &wh.name,
        "s3://other",
        BTreeMap::new(),
        PRINCIPAL,
    )
    .await
    .expect_err("duplicate must fail");
    assert!(matches!(err, MeridianError::Conflict(_)), "got: {err:?}");
}

#[tokio::test]
async fn warehouse_delete_missing_and_nonempty_are_rejected() {
    let Some(pool) = test_pool().await else {
        return;
    };
    let ws = tenancy::default_workspace_id();

    let err = warehouse::delete_by_name(&pool, ws, &unique_name("ghost"), PRINCIPAL)
        .await
        .expect_err("missing warehouse");
    assert!(matches!(err, MeridianError::NotFound(_)), "got: {err:?}");

    let wh = make_warehouse(&pool).await;
    namespace::create(
        &pool,
        ws,
        &wh.id,
        &levels(&["ns"]),
        BTreeMap::new(),
        PRINCIPAL,
    )
    .await
    .expect("create namespace");
    let err = warehouse::delete_by_name(&pool, ws, &wh.name, PRINCIPAL)
        .await
        .expect_err("non-empty warehouse");
    assert!(matches!(err, MeridianError::Conflict(_)), "got: {err:?}");
}

#[tokio::test]
async fn namespace_create_get_and_duplicate() {
    let Some(pool) = test_pool().await else {
        return;
    };
    let ws = tenancy::default_workspace_id();
    let wh = make_warehouse(&pool).await;

    let mut props = BTreeMap::new();
    props.insert("owner".to_owned(), "test".to_owned());
    let created = namespace::create(&pool, ws, &wh.id, &levels(&["a"]), props.clone(), PRINCIPAL)
        .await
        .expect("create namespace");
    assert_eq!(created.levels, levels(&["a"]));
    assert_eq!(created.properties.0, props);

    let loaded = namespace::get(&pool, &wh.id, &levels(&["a"]))
        .await
        .expect("get namespace")
        .expect("namespace exists");
    assert_eq!(loaded.id, created.id);

    let err = namespace::create(
        &pool,
        ws,
        &wh.id,
        &levels(&["a"]),
        BTreeMap::new(),
        PRINCIPAL,
    )
    .await
    .expect_err("duplicate namespace");
    assert!(matches!(err, MeridianError::Conflict(_)), "got: {err:?}");
}

#[tokio::test]
async fn nested_namespace_requires_parent() {
    let Some(pool) = test_pool().await else {
        return;
    };
    let ws = tenancy::default_workspace_id();
    let wh = make_warehouse(&pool).await;

    let err = namespace::create(
        &pool,
        ws,
        &wh.id,
        &levels(&["missing", "child"]),
        BTreeMap::new(),
        PRINCIPAL,
    )
    .await
    .expect_err("parent must exist");
    assert!(matches!(err, MeridianError::NotFound(_)), "got: {err:?}");

    namespace::create(
        &pool,
        ws,
        &wh.id,
        &levels(&["p"]),
        BTreeMap::new(),
        PRINCIPAL,
    )
    .await
    .expect("create parent");
    let child = namespace::create(
        &pool,
        ws,
        &wh.id,
        &levels(&["p", "c"]),
        BTreeMap::new(),
        PRINCIPAL,
    )
    .await
    .expect("create child");
    assert_eq!(child.levels, levels(&["p", "c"]));
}

#[tokio::test]
async fn namespace_list_scopes_levels_and_paginates() {
    let Some(pool) = test_pool().await else {
        return;
    };
    let ws = tenancy::default_workspace_id();
    let wh = make_warehouse(&pool).await;

    for name in ["a", "b", "c"] {
        namespace::create(
            &pool,
            ws,
            &wh.id,
            &levels(&[name]),
            BTreeMap::new(),
            PRINCIPAL,
        )
        .await
        .expect("create top-level namespace");
    }
    for name in ["x", "y"] {
        namespace::create(
            &pool,
            ws,
            &wh.id,
            &levels(&["a", name]),
            BTreeMap::new(),
            PRINCIPAL,
        )
        .await
        .expect("create nested namespace");
    }

    // Top-level listing sees exactly the three top-level namespaces.
    let top = namespace::list(&pool, &wh.id, &[], None, None)
        .await
        .expect("list top-level");
    let mut top_names: Vec<String> = top.iter().map(|r| r.levels.join(".")).collect();
    top_names.sort();
    assert_eq!(top_names, vec!["a", "b", "c"]);

    // Listing under "a" sees only its direct children.
    let under_a = namespace::list(&pool, &wh.id, &levels(&["a"]), None, None)
        .await
        .expect("list under a");
    let mut names: Vec<String> = under_a.iter().map(|r| r.levels.join(".")).collect();
    names.sort();
    assert_eq!(names, vec!["a.x", "a.y"]);

    // Keyset pagination: two pages of 2 + 1.
    let page1 = namespace::list(&pool, &wh.id, &[], None, Some(2))
        .await
        .expect("page 1");
    assert_eq!(page1.len(), 2);
    let page2 = namespace::list(&pool, &wh.id, &[], Some(&page1[1].id), Some(2))
        .await
        .expect("page 2");
    assert_eq!(page2.len(), 1);
    let mut combined: Vec<String> = page1
        .iter()
        .chain(page2.iter())
        .map(|r| r.levels.join("."))
        .collect();
    combined.sort();
    assert_eq!(combined, vec!["a", "b", "c"]);
}

#[tokio::test]
async fn namespace_delete_requires_empty() {
    let Some(pool) = test_pool().await else {
        return;
    };
    let ws = tenancy::default_workspace_id();
    let wh = make_warehouse(&pool).await;

    namespace::create(
        &pool,
        ws,
        &wh.id,
        &levels(&["p"]),
        BTreeMap::new(),
        PRINCIPAL,
    )
    .await
    .expect("create parent");
    namespace::create(
        &pool,
        ws,
        &wh.id,
        &levels(&["p", "c"]),
        BTreeMap::new(),
        PRINCIPAL,
    )
    .await
    .expect("create child");

    let err = namespace::delete(&pool, ws, &wh.id, &levels(&["p"]), PRINCIPAL)
        .await
        .expect_err("non-empty namespace");
    assert!(matches!(err, MeridianError::Conflict(_)), "got: {err:?}");

    // A namespace containing a table is also non-empty.
    let child = namespace::get(&pool, &wh.id, &levels(&["p", "c"]))
        .await
        .expect("get child")
        .expect("child exists");
    let table_id = Ulid::new().to_string();
    sqlx::query(
        "INSERT INTO tables (id, workspace_id, namespace_id, name, table_uuid)
         VALUES ($1, $2, $3, $4, $5)",
    )
    .bind(&table_id)
    .bind(ws.to_string())
    .bind(&child.id)
    .bind("t")
    .bind(format!("uuid-{table_id}"))
    .execute(&pool)
    .await
    .expect("insert table row");
    let err = namespace::delete(&pool, ws, &wh.id, &levels(&["p", "c"]), PRINCIPAL)
        .await
        .expect_err("namespace with table");
    assert!(matches!(err, MeridianError::Conflict(_)), "got: {err:?}");

    // Emptied bottom-up, deletion succeeds.
    sqlx::query("DELETE FROM tables WHERE namespace_id = $1")
        .bind(&child.id)
        .execute(&pool)
        .await
        .expect("remove table row");
    namespace::delete(&pool, ws, &wh.id, &levels(&["p", "c"]), PRINCIPAL)
        .await
        .expect("delete child");
    namespace::delete(&pool, ws, &wh.id, &levels(&["p"]), PRINCIPAL)
        .await
        .expect("delete parent");

    let err = namespace::delete(&pool, ws, &wh.id, &levels(&["p"]), PRINCIPAL)
        .await
        .expect_err("already deleted");
    assert!(matches!(err, MeridianError::NotFound(_)), "got: {err:?}");
}

#[tokio::test]
async fn namespace_property_updates_report_outcome() {
    let Some(pool) = test_pool().await else {
        return;
    };
    let ws = tenancy::default_workspace_id();
    let wh = make_warehouse(&pool).await;

    let mut initial = BTreeMap::new();
    initial.insert("keep".to_owned(), "1".to_owned());
    initial.insert("drop".to_owned(), "2".to_owned());
    namespace::create(&pool, ws, &wh.id, &levels(&["ns"]), initial, PRINCIPAL)
        .await
        .expect("create namespace");

    let mut updates = BTreeMap::new();
    updates.insert("keep".to_owned(), "changed".to_owned());
    updates.insert("new".to_owned(), "3".to_owned());
    let outcome = namespace::update_properties(
        &pool,
        ws,
        &wh.id,
        &levels(&["ns"]),
        updates,
        vec!["drop".to_owned(), "absent".to_owned()],
        PRINCIPAL,
    )
    .await
    .expect("update properties");

    assert_eq!(outcome.updated, vec!["keep".to_owned(), "new".to_owned()]);
    assert_eq!(outcome.removed, vec!["drop".to_owned()]);
    assert_eq!(outcome.missing, vec!["absent".to_owned()]);

    let record = namespace::get(&pool, &wh.id, &levels(&["ns"]))
        .await
        .expect("get namespace")
        .expect("namespace exists");
    let props = record.properties.0;
    assert_eq!(props.get("keep").map(String::as_str), Some("changed"));
    assert_eq!(props.get("new").map(String::as_str), Some("3"));
    assert!(!props.contains_key("drop"));

    // Missing namespace is NotFound.
    let err = namespace::update_properties(
        &pool,
        ws,
        &wh.id,
        &levels(&["ghost"]),
        BTreeMap::new(),
        Vec::new(),
        PRINCIPAL,
    )
    .await
    .expect_err("missing namespace");
    assert!(matches!(err, MeridianError::NotFound(_)), "got: {err:?}");
}

#[tokio::test]
async fn mutations_write_audit_and_outbox_rows() {
    let Some(pool) = test_pool().await else {
        return;
    };
    let ws = tenancy::default_workspace_id();
    let wh = make_warehouse(&pool).await;
    let ns = namespace::create(
        &pool,
        ws,
        &wh.id,
        &levels(&["audited"]),
        BTreeMap::new(),
        PRINCIPAL,
    )
    .await
    .expect("create namespace");

    // One outbox event and one audit row per mutation, same aggregate.
    for (aggregate, event_type, action) in [
        (
            format!("warehouse:{}", wh.id),
            "warehouse.created",
            "warehouse.create",
        ),
        (
            format!("namespace:{}", ns.id),
            "namespace.created",
            "namespace.create",
        ),
    ] {
        let (outbox_count,): (i64,) = sqlx::query_as(
            "SELECT count(*) FROM events_outbox WHERE aggregate = $1 AND event_type = $2",
        )
        .bind(&aggregate)
        .bind(event_type)
        .fetch_one(&pool)
        .await
        .expect("count outbox rows");
        assert_eq!(outbox_count, 1, "outbox row for {aggregate}");

        let (audit_count,): (i64,) =
            sqlx::query_as("SELECT count(*) FROM audit_log WHERE resource = $1 AND action = $2")
                .bind(&aggregate)
                .bind(action)
                .fetch_one(&pool)
                .await
                .expect("count audit rows");
        assert_eq!(audit_count, 1, "audit row for {aggregate}");
    }

    // Failed mutations leave no audit/outbox rows behind (rolled back).
    let err = namespace::create(
        &pool,
        ws,
        &wh.id,
        &levels(&["audited"]),
        BTreeMap::new(),
        PRINCIPAL,
    )
    .await
    .expect_err("duplicate namespace");
    assert!(matches!(err, MeridianError::Conflict(_)));
    let (outbox_count,): (i64,) =
        sqlx::query_as("SELECT count(*) FROM events_outbox WHERE aggregate = $1")
            .bind(format!("namespace:{}", ns.id))
            .fetch_one(&pool)
            .await
            .expect("count outbox rows");
    assert_eq!(outbox_count, 1, "failed create must not enqueue");
}
