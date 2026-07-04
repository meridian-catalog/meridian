//! Database-backed tests for the semantics store (Pillar G): metrics, glossary,
//! data products, and the universal-view translation cache.
//!
//! These require a running Postgres and `DATABASE_URL`; without it they skip.
//! Each test uses uniquely-named objects and scopes its assertions to its own
//! ids, so the suite is isolated from other tests and prior runs.

use meridian_common::config::DatabaseConfig;
use meridian_store::semantics::{
    self, Certification, MetricPatch, NewDataProduct, NewGlossaryTerm, NewMetric,
};
use meridian_store::tenancy;
use serde_json::json;
use sqlx::PgPool;
use ulid::Ulid;

const PRINCIPAL: &str = "test:semantics";

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

fn unique(prefix: &str) -> String {
    format!("{prefix}_{}", Ulid::new().to_string().to_lowercase())
}

/// Counts audit + outbox rows for a specific resource, so a mutation's
/// invariant (both written on the same transaction) is checked against our own
/// object only.
async fn audit_and_outbox_counts(pool: &PgPool, resource: &str, aggregate: &str) -> (i64, i64) {
    let audit: i64 = sqlx::query_scalar("SELECT count(*) FROM audit_log WHERE resource = $1")
        .bind(resource)
        .fetch_one(pool)
        .await
        .expect("count audit");
    let outbox: i64 = sqlx::query_scalar("SELECT count(*) FROM events_outbox WHERE aggregate = $1")
        .bind(aggregate)
        .fetch_one(pool)
        .await
        .expect("count outbox");
    (audit, outbox)
}

#[tokio::test]
async fn metric_create_update_delete_writes_audit_and_outbox() {
    let Some(pool) = test_pool().await else {
        return;
    };
    let ws = tenancy::default_workspace_id();
    let name = unique("metric");

    let created = semantics::create_metric(
        &pool,
        ws,
        NewMetric {
            name: &name,
            display_name: None,
            source: "analytics.sales",
            expression: "SUM(amount)",
            dialect: "trino",
            dimensions: &["region".to_owned()],
            filters: &["status = 'paid'".to_owned()],
            grain: Some("one row per order"),
            description: None,
            owner: Some("user:alice"),
            certification: Certification::Certified,
        },
        PRINCIPAL,
    )
    .await
    .expect("create metric");
    assert_eq!(created.certification, "certified");
    assert_eq!(created.dimensions.0, vec!["region".to_owned()]);

    let resource = format!("metric:{}", created.id);
    // create writes exactly one audit + one outbox row for this metric.
    assert_eq!(
        audit_and_outbox_counts(&pool, &resource, &resource).await,
        (1, 1),
        "create writes one audit + one outbox row"
    );

    // Update bumps both to 2.
    let updated = semantics::update_metric(
        &pool,
        ws,
        &created.id,
        MetricPatch {
            certification: Some(Certification::Deprecated),
            ..MetricPatch::default()
        },
        PRINCIPAL,
    )
    .await
    .expect("update metric");
    assert_eq!(updated.certification, "deprecated");
    // The COALESCE update leaves untouched fields intact.
    assert_eq!(updated.expression, "SUM(amount)");
    assert_eq!(
        audit_and_outbox_counts(&pool, &resource, &resource).await,
        (2, 2),
        "update writes another audit + outbox row"
    );

    // Lookup by name is case-insensitive.
    let by_name = semantics::get_metric_by_name(&pool, ws, &name.to_uppercase())
        .await
        .expect("query")
        .expect("found by name");
    assert_eq!(by_name.id, created.id);

    // Delete.
    semantics::delete_metric(&pool, ws, &created.id, PRINCIPAL)
        .await
        .expect("delete metric");
    assert!(
        semantics::get_metric(&pool, &created.id)
            .await
            .expect("query")
            .is_none(),
        "metric is gone after delete"
    );
    assert_eq!(
        audit_and_outbox_counts(&pool, &resource, &resource).await,
        (3, 3),
        "delete writes the third audit + outbox row"
    );
}

#[tokio::test]
async fn metric_duplicate_name_is_conflict() {
    let Some(pool) = test_pool().await else {
        return;
    };
    let ws = tenancy::default_workspace_id();
    let name = unique("dupmetric");
    let make = |n: String| NewMetric {
        name: Box::leak(n.into_boxed_str()),
        display_name: None,
        source: "s",
        expression: "COUNT(*)",
        dialect: "trino",
        dimensions: &[],
        filters: &[],
        grain: None,
        description: None,
        owner: None,
        certification: Certification::Draft,
    };
    let first = semantics::create_metric(&pool, ws, make(name.clone()), PRINCIPAL)
        .await
        .expect("first create");

    // Same name, different case -> conflict (case-insensitive unique index).
    let err = semantics::create_metric(&pool, ws, make(name.to_uppercase()), PRINCIPAL)
        .await
        .expect_err("duplicate should conflict");
    assert!(
        matches!(err, meridian_common::MeridianError::Conflict(_)),
        "expected Conflict, got {err:?}"
    );

    semantics::delete_metric(&pool, ws, &first.id, PRINCIPAL)
        .await
        .expect("cleanup");
}

#[tokio::test]
async fn glossary_term_link_is_idempotent_and_reverse_lookup_works() {
    let Some(pool) = test_pool().await else {
        return;
    };
    let ws = tenancy::default_workspace_id();
    let name = unique("term");
    let term = semantics::create_term(
        &pool,
        ws,
        NewGlossaryTerm {
            name: &name,
            definition: "A precise business meaning.",
            steward: Some("user:steward"),
            certification: Certification::Draft,
        },
        PRINCIPAL,
    )
    .await
    .expect("create term");

    let asset_ref = format!("table:{}", Ulid::new());
    let first_link = semantics::link_term(&pool, ws, &term.id, "table", &asset_ref, PRINCIPAL)
        .await
        .expect("link");
    // Re-link the same pair returns the same row (idempotent).
    let repeat_link = semantics::link_term(&pool, ws, &term.id, "table", &asset_ref, PRINCIPAL)
        .await
        .expect("relink");
    assert_eq!(
        first_link.id, repeat_link.id,
        "idempotent link returns the same row"
    );

    let links = semantics::list_term_links(&pool, &term.id)
        .await
        .expect("list links");
    assert_eq!(links.len(), 1, "only one link exists");

    // Reverse lookup: the term is found for the asset.
    let terms = semantics::list_terms_for_asset(&pool, ws, "table", &asset_ref)
        .await
        .expect("reverse lookup");
    assert!(
        terms.iter().any(|t| t.id == term.id),
        "reverse lookup finds the term"
    );

    // Deleting the term cascades its links.
    semantics::delete_term(&pool, ws, &term.id, PRINCIPAL)
        .await
        .expect("delete term");
    let after = semantics::list_term_links(&pool, &term.id)
        .await
        .expect("list after delete");
    assert!(after.is_empty(), "links cascade on term delete");
}

#[tokio::test]
async fn data_product_members_cascade_on_delete() {
    let Some(pool) = test_pool().await else {
        return;
    };
    let ws = tenancy::default_workspace_id();
    let name = unique("product");
    let product = semantics::create_product(
        &pool,
        ws,
        NewDataProduct {
            name: &name,
            display_name: None,
            description: Some("Certified bundle."),
            owner: Some("user:owner"),
            sla: Some("99.9%"),
            certification: Certification::Certified,
        },
        PRINCIPAL,
    )
    .await
    .expect("create product");

    let member =
        semantics::add_product_member(&pool, ws, &product.id, "metric", "metric:01ABC", PRINCIPAL)
            .await
            .expect("add member");
    // Idempotent add.
    let re_added =
        semantics::add_product_member(&pool, ws, &product.id, "metric", "metric:01ABC", PRINCIPAL)
            .await
            .expect("re-add member");
    assert_eq!(member.id, re_added.id, "idempotent member add");

    let members = semantics::list_product_members(&pool, &product.id)
        .await
        .expect("list members");
    assert_eq!(members.len(), 1);

    semantics::delete_product(&pool, ws, &product.id, PRINCIPAL)
        .await
        .expect("delete product");
    let after = semantics::list_product_members(&pool, &product.id)
        .await
        .expect("list after delete");
    assert!(after.is_empty(), "members cascade on product delete");
}

#[tokio::test]
async fn translation_cache_upsert_and_lookup() {
    let Some(pool) = test_pool().await else {
        return;
    };
    let ws = tenancy::default_workspace_id();

    // A real view row is required (the cache FKs views.id). Build a minimal one
    // via a direct insert scoped to a throwaway namespace + warehouse.
    let (view_id, _cleanup) = make_view_row(&pool).await;

    let hash = "abc123";
    let diagnostics =
        vec![json!({ "severity": "warning", "code": "parse_back_diff", "message": "x" })];
    let cached = semantics::upsert_cached_translation(
        &pool,
        ws,
        &view_id,
        "DuckDB", // case-insensitive; stored lowercased
        "Spark",
        hash,
        Some("SELECT 1"),
        "best_effort",
        &diagnostics,
    )
    .await
    .expect("upsert cache");
    assert_eq!(cached.target_dialect, "duckdb", "dialect lowercased");
    assert_eq!(cached.source_dialect, "spark");
    assert_eq!(cached.status, "best_effort");

    // Lookup by the natural key returns it (case-insensitive dialect).
    let found = semantics::get_cached_translation(&pool, &view_id, "duckdb", hash)
        .await
        .expect("lookup")
        .expect("cache hit");
    assert_eq!(found.id, cached.id);
    assert_eq!(found.translated_sql.as_deref(), Some("SELECT 1"));

    // Upsert again with a different status overwrites in place (same row).
    let updated = semantics::upsert_cached_translation(
        &pool,
        ws,
        &view_id,
        "duckdb",
        "spark",
        hash,
        Some("SELECT 1 AS one"),
        "verified",
        &[],
    )
    .await
    .expect("re-upsert");
    assert_eq!(updated.id, cached.id, "upsert overwrites the same row");
    assert_eq!(updated.status, "verified");

    // A different source hash is a distinct entry.
    let other = semantics::get_cached_translation(&pool, &view_id, "duckdb", "different")
        .await
        .expect("lookup other");
    assert!(other.is_none(), "a different definition is not served");

    let all = semantics::list_cached_translations(&pool, &view_id)
        .await
        .expect("list cache");
    assert_eq!(all.len(), 1, "one cache entry for this view");
}

/// Inserts a minimal committed view row (with its warehouse + namespace) for the
/// translation-cache FK, returning its id. The objects are uniquely named, so
/// no cleanup is needed for isolation; the caller may drop them via the returned
/// guard if desired (a no-op here).
async fn make_view_row(pool: &PgPool) -> (String, ()) {
    let ws = tenancy::default_workspace_id().to_string();
    let wh_id = Ulid::new().to_string();
    sqlx::query(
        "INSERT INTO warehouses (id, workspace_id, name, storage_root, storage_config)
         VALUES ($1, $2, $3, $4, '{}'::jsonb)",
    )
    .bind(&wh_id)
    .bind(&ws)
    .bind(unique("wh"))
    .bind("file:///tmp/whatever")
    .execute(pool)
    .await
    .expect("insert warehouse");

    let ns_id = Ulid::new().to_string();
    sqlx::query(
        "INSERT INTO namespaces (id, workspace_id, warehouse_id, levels, properties)
         VALUES ($1, $2, $3, $4, '{}'::jsonb)",
    )
    .bind(&ns_id)
    .bind(&ws)
    .bind(&wh_id)
    .bind(vec![unique("ns")])
    .execute(pool)
    .await
    .expect("insert namespace");

    let view_id = Ulid::new().to_string();
    sqlx::query(
        "INSERT INTO views
             (id, workspace_id, namespace_id, name, view_uuid, metadata_location,
              pointer_version, properties)
         VALUES ($1, $2, $3, $4, $5, $6, 0, '{}'::jsonb)",
    )
    .bind(&view_id)
    .bind(&ws)
    .bind(&ns_id)
    .bind(unique("v"))
    .bind(Ulid::new().to_string())
    .bind("file:///tmp/meta.json")
    .execute(pool)
    .await
    .expect("insert view");

    (view_id, ())
}
