//! Database-backed tests for full-text search (migration 0010 +
//! `meridian_store::search`).
//!
//! These require a running Postgres and `DATABASE_URL`; without it they
//! skip (with a note on stderr) so the suite stays runnable offline.
//!
//! The test database is shared and persistent, so every test salts its
//! asset names (and search queries) with a per-test ULID token — matches
//! from other tests or previous runs are impossible by construction.

use std::collections::BTreeMap;

use meridian_common::id::WorkspaceId;
use meridian_common::principal::{Principal, PrincipalKind};
use meridian_iceberg::commit::PointerCas;
use meridian_store::commit::{CommitTableOp, DerivedTableState, PostgresCommitBackend};
use meridian_store::rbac::{Grantee, Privilege, SecurableType};
use meridian_store::search::{self, SearchAssetKind, SearchRequest, SearchVisibility};
use meridian_store::{namespace, principal, rbac, table, tenancy, view, warehouse};
use serde_json::json;
use sqlx::PgPool;
use ulid::Ulid;

async fn test_pool() -> Option<PgPool> {
    let Ok(url) = std::env::var("DATABASE_URL") else {
        eprintln!("skipping search DB test: DATABASE_URL is not set");
        return None;
    };
    let config = meridian_common::config::DatabaseConfig {
        url,
        ..meridian_common::config::DatabaseConfig::default()
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

fn ws() -> WorkspaceId {
    tenancy::default_workspace_id()
}

/// A per-test salt: lowercase so it survives the query lowercasing, and a
/// single FTS token by construction (alphanumeric).
fn salt() -> String {
    Ulid::new().to_string().to_lowercase()
}

struct Fixture {
    pool: PgPool,
    salt: String,
    warehouse_id: String,
    namespace_id: String,
    levels: Vec<String>,
}

/// Creates a warehouse and one namespace to hang assets off.
async fn fixture(pool: PgPool) -> Fixture {
    let salt = salt();
    let wh = warehouse::create(
        &pool,
        ws(),
        &format!("wh{salt}"),
        "file:///tmp/meridian-search-tests",
        BTreeMap::new(),
        "test:search",
    )
    .await
    .expect("create warehouse");
    // Underscore-separated so the salt is its own FTS token (bare-salt
    // queries must match the namespace path).
    let levels = vec![format!("ns_{salt}")];
    let ns = namespace::create(&pool, ws(), &wh.id, &levels, BTreeMap::new(), "test:search")
        .await
        .expect("create namespace");
    Fixture {
        pool,
        salt,
        warehouse_id: wh.id,
        namespace_id: ns.id,
        levels,
    }
}

/// Creates a table row with optional schema text and comment.
async fn make_table(
    fx: &Fixture,
    name: &str,
    schema_text: Option<&str>,
    comment: Option<&str>,
) -> table::TableRecord {
    let mut properties = BTreeMap::new();
    if let Some(comment) = comment {
        properties.insert("comment".to_owned(), comment.to_owned());
    }
    table::create(
        &fx.pool,
        table::NewTable {
            workspace_id: ws(),
            namespace_id: &fx.namespace_id,
            namespace_levels: &fx.levels,
            name,
            table_uuid: &uuid_like(),
            metadata_location: "file:///tmp/meridian-search-tests/m0.metadata.json",
            format_version: 2,
            properties: &properties,
            schema_text,
            snapshots: &[],
            origin: "create",
        },
        "test:search",
        None,
    )
    .await
    .expect("create table")
}

/// A unique canonical-form UUID string (the column only requires
/// uniqueness).
fn uuid_like() -> String {
    let raw = format!("{:032x}", rand_bits());
    format!(
        "{}-{}-{}-{}-{}",
        &raw[0..8],
        &raw[8..12],
        &raw[12..16],
        &raw[16..20],
        &raw[20..32]
    )
}

fn rand_bits() -> u128 {
    Ulid::new().0
}

/// Runs an unrestricted search for `text`, all kinds, one page of 50.
async fn find(fx: &Fixture, text: &str) -> Vec<search::SearchHit> {
    search::search(
        &fx.pool,
        ws(),
        &SearchRequest {
            text,
            warehouse_id: None,
            namespace: None,
            kinds: None,
            limit: 50,
            page_token: None,
        },
        &SearchVisibility::all(),
    )
    .await
    .expect("search")
    .hits
}

#[tokio::test]
async fn migrations_rerun_cleanly() {
    let Some(pool) = test_pool().await else {
        return;
    };
    // test_pool already ran the migrator once; a second run must be a
    // no-op (sqlx tracks applied versions).
    meridian_store::MIGRATOR
        .run(&pool)
        .await
        .expect("re-running migrations is a no-op");
}

#[tokio::test]
async fn search_matches_name_column_comment_and_prefix() {
    let Some(pool) = test_pool().await else {
        return;
    };
    let fx = fixture(pool).await;
    let salt = &fx.salt;

    let name = format!("orders_{salt}");
    make_table(
        &fx,
        &name,
        Some(&format!(
            "order_id customer_email_{salt} the customer contact email"
        )),
        Some(&format!("fact table for billing{salt}")),
    )
    .await;

    // By name.
    let hits = find(&fx, &name).await;
    assert_eq!(hits.len(), 1, "name match: {hits:?}");
    assert_eq!(hits[0].name, name);
    assert_eq!(hits[0].kind, SearchAssetKind::Table);
    assert_eq!(hits[0].namespace, fx.levels);

    // By column name (the load-bearing case: a query for a column must
    // find tables whose schema contains it).
    let hits = find(&fx, &format!("customer_email_{salt}")).await;
    assert_eq!(hits.len(), 1, "column match: {hits:?}");
    assert_eq!(hits[0].name, name);
    assert!(
        hits[0].snippet.contains("**"),
        "snippet must highlight the match: {:?}",
        hits[0].snippet
    );

    // By comment.
    let hits = find(&fx, &format!("billing{salt}")).await;
    assert_eq!(hits.len(), 1, "comment match: {hits:?}");
    assert_eq!(hits[0].name, name);

    // By prefix of the name's salted token.
    let prefix = &salt[..salt.len() - 4];
    let hits = find(&fx, &format!("orders_{prefix}")).await;
    assert_eq!(hits.len(), 1, "prefix match: {hits:?}");
    assert_eq!(hits[0].name, name);

    // A miss is a miss.
    let hits = find(&fx, &format!("nonexistent_{salt}")).await;
    assert!(hits.is_empty(), "no false positives: {hits:?}");
}

#[tokio::test]
async fn exact_name_outranks_column_and_prefix_matches() {
    let Some(pool) = test_pool().await else {
        return;
    };
    let fx = fixture(pool).await;
    let salt = &fx.salt;

    let exact = format!("rev_{salt}");
    // A decoy whose *column* matches, and a decoy whose name merely starts
    // with the query.
    make_table(&fx, &format!("decoy_{salt}"), Some(&exact), None).await;
    make_table(&fx, &format!("rev_{salt}x"), None, None).await;
    make_table(&fx, &exact, None, None).await;

    let hits = find(&fx, &exact).await;
    assert_eq!(hits.len(), 3, "{hits:?}");
    assert_eq!(hits[0].name, exact, "exact name must rank first: {hits:?}");
    assert!(
        hits[0].rank > hits[1].rank,
        "exact-name boost must be strict: {hits:?}"
    );
}

#[tokio::test]
async fn index_follows_create_commit_rename_and_drop() {
    let Some(pool) = test_pool().await else {
        return;
    };
    let fx = fixture(pool).await;
    let salt = &fx.salt;

    let name = format!("mut_{salt}");
    let record = make_table(&fx, &name, Some(&format!("alpha_{salt}")), None).await;

    // Create is indexed.
    assert_eq!(find(&fx, &format!("alpha_{salt}")).await.len(), 1);

    // A commit that evolves the schema re-indexes the columns in the same
    // transaction (write-through).
    let backend = PostgresCommitBackend::new(fx.pool.clone(), ws(), "test:search");
    backend
        .commit_tables(
            &[CommitTableOp {
                cas: PointerCas {
                    table: record.id.clone(),
                    expected_version: 0,
                    new_metadata_location: "file:///tmp/meridian-search-tests/m1.metadata.json"
                        .to_owned(),
                },
                derived: Some(DerivedTableState {
                    format_version: 2,
                    properties: BTreeMap::new(),
                    snapshots: Vec::new(),
                    schema_text: Some(format!("beta_{salt}")),
                    event_details: json!({}),
                }),
                contract_violation: None,
            }],
            None,
        )
        .await
        .expect("commit");
    assert!(
        find(&fx, &format!("alpha_{salt}")).await.is_empty(),
        "dropped column must leave the index"
    );
    assert_eq!(
        find(&fx, &format!("beta_{salt}")).await.len(),
        1,
        "added column must enter the index"
    );

    // Rename re-indexes the name.
    let renamed = format!("moved_{salt}");
    table::rename(
        &fx.pool,
        ws(),
        &fx.warehouse_id,
        &fx.levels,
        &name,
        &fx.levels,
        &renamed,
        "test:search",
    )
    .await
    .expect("rename");
    assert!(find(&fx, &name).await.is_empty(), "old name must be gone");
    assert_eq!(find(&fx, &renamed).await.len(), 1);

    // Drop removes the row (and with it the index entry).
    table::drop_table(
        &fx.pool,
        ws(),
        &fx.warehouse_id,
        &fx.levels,
        &renamed,
        false,
        "test:search",
    )
    .await
    .expect("drop");
    assert!(find(&fx, &renamed).await.is_empty());
    assert!(find(&fx, &format!("beta_{salt}")).await.is_empty());
}

#[tokio::test]
async fn namespaces_and_views_are_searchable_and_type_filterable() {
    let Some(pool) = test_pool().await else {
        return;
    };
    let fx = fixture(pool).await;
    let salt = &fx.salt;

    // The fixture namespace itself, findable by its level name.
    let hits = find(&fx, &format!("ns_{salt}")).await;
    assert!(
        hits.iter()
            .any(|h| h.kind == SearchAssetKind::Namespace && h.name == format!("ns_{salt}")),
        "namespace by level name: {hits:?}"
    );

    // A view, findable by name.
    let view_name = format!("vw_{salt}");
    view::create(
        &fx.pool,
        view::NewView {
            workspace_id: ws(),
            namespace_id: &fx.namespace_id,
            namespace_levels: &fx.levels,
            name: &view_name,
            view_uuid: &uuid_like(),
            metadata_location: "file:///tmp/meridian-search-tests/v0.metadata.json",
            properties: &BTreeMap::new(),
        },
        "test:search",
    )
    .await
    .expect("create view");
    let hits = find(&fx, &view_name).await;
    assert_eq!(hits.len(), 1, "{hits:?}");
    assert_eq!(hits[0].kind, SearchAssetKind::View);

    // Type filter: the salt matches the warehouse-fixture namespace, the
    // view, and any tables; restricting to namespaces drops the rest.
    let page = search::search(
        &fx.pool,
        ws(),
        &SearchRequest {
            text: salt,
            warehouse_id: None,
            namespace: None,
            kinds: Some(&[SearchAssetKind::Namespace]),
            limit: 50,
            page_token: None,
        },
        &SearchVisibility::all(),
    )
    .await
    .expect("search");
    assert!(
        !page.hits.is_empty()
            && page
                .hits
                .iter()
                .all(|h| h.kind == SearchAssetKind::Namespace),
        "type filter must hold: {:?}",
        page.hits
    );

    // Namespace filter: restricting to the fixture namespace keeps the
    // hits; restricting to a different path drops them all.
    let ns_request = |namespace| SearchRequest {
        text: salt,
        warehouse_id: None,
        namespace: Some(namespace),
        kinds: None,
        limit: 50,
        page_token: None,
    };
    let page = search::search(
        &fx.pool,
        ws(),
        &ns_request(fx.levels.as_slice()),
        &SearchVisibility::all(),
    )
    .await
    .expect("search under namespace");
    assert!(
        page.hits.iter().any(|h| h.kind == SearchAssetKind::View),
        "namespace filter keeps assets under the path: {:?}",
        page.hits
    );
    let other = vec!["elsewhere".to_owned()];
    let page = search::search(
        &fx.pool,
        ws(),
        &ns_request(other.as_slice()),
        &SearchVisibility::all(),
    )
    .await
    .expect("search under other namespace");
    assert!(
        page.hits.is_empty(),
        "namespace filter excludes other paths: {:?}",
        page.hits
    );
}

#[tokio::test]
async fn keyset_pagination_walks_every_hit_once() {
    let Some(pool) = test_pool().await else {
        return;
    };
    let fx = fixture(pool).await;
    let salt = &fx.salt;

    for i in 0..3 {
        make_table(&fx, &format!("pg{i}_{salt}"), None, None).await;
    }

    let mut seen = Vec::new();
    let mut token: Option<String> = None;
    let mut rounds = 0;
    loop {
        let page = search::search(
            &fx.pool,
            ws(),
            &SearchRequest {
                text: salt,
                warehouse_id: None,
                namespace: None,
                kinds: Some(&[SearchAssetKind::Table]),
                limit: 1,
                page_token: token.as_deref(),
            },
            &SearchVisibility::all(),
        )
        .await
        .expect("search page");
        assert!(page.hits.len() <= 1);
        seen.extend(page.hits.iter().map(|h| h.id.clone()));
        rounds += 1;
        assert!(rounds <= 10, "pagination must terminate");
        match page.next_page_token {
            Some(next) => token = Some(next),
            None => break,
        }
    }
    assert_eq!(seen.len(), 3, "every hit exactly once: {seen:?}");
    let mut unique = seen.clone();
    unique.sort();
    unique.dedup();
    assert_eq!(unique.len(), 3, "no duplicates across pages: {seen:?}");
}

#[tokio::test]
// One narrative from zero grants to full visibility; splitting it would
// duplicate the fixture walk.
#[allow(clippy::too_many_lines)]
async fn visibility_filters_results_to_granted_assets() {
    let Some(pool) = test_pool().await else {
        return;
    };
    let fx = fixture(pool).await;
    let salt = &fx.salt;

    // Two tables; the user will be granted READ on only one.
    let granted = make_table(&fx, &format!("open_{salt}"), None, None).await;
    make_table(&fx, &format!("closed_{salt}"), None, None).await;

    let identity = Principal {
        kind: PrincipalKind::User,
        subject: format!("searcher-{salt}"),
        issuer: Some(format!("https://idp.example/{salt}")),
        display_name: None,
    };
    let record = principal::ensure(&fx.pool, ws(), &identity)
        .await
        .expect("provision principal");

    // No grants: zero visibility, zero hits.
    let visibility = search::visibility_for(&fx.pool, &identity)
        .await
        .expect("visibility");
    assert!(!visibility.unrestricted);
    let request = SearchRequest {
        text: salt,
        warehouse_id: None,
        namespace: None,
        kinds: None,
        limit: 50,
        page_token: None,
    };
    let page = search::search(&fx.pool, ws(), &request, &visibility)
        .await
        .expect("search");
    assert!(page.hits.is_empty(), "deny by default: {:?}", page.hits);

    // READ on one table: exactly that table, still no namespaces.
    rbac::create_grant(
        &fx.pool,
        ws(),
        &Grantee::Principal(record.id.clone()),
        SecurableType::Table,
        &granted.id,
        Privilege::Read,
        "test:search",
    )
    .await
    .expect("grant READ on table");
    let visibility = search::visibility_for(&fx.pool, &identity)
        .await
        .expect("visibility");
    let page = search::search(&fx.pool, ws(), &request, &visibility)
        .await
        .expect("search");
    assert_eq!(page.hits.len(), 1, "{:?}", page.hits);
    assert_eq!(page.hits[0].id, granted.id);

    // READ on the namespace: both tables via inheritance.
    rbac::create_grant(
        &fx.pool,
        ws(),
        &Grantee::Principal(record.id.clone()),
        SecurableType::Namespace,
        &fx.namespace_id,
        Privilege::Read,
        "test:search",
    )
    .await
    .expect("grant READ on namespace");
    let visibility = search::visibility_for(&fx.pool, &identity)
        .await
        .expect("visibility");
    let page = search::search(&fx.pool, ws(), &request, &visibility)
        .await
        .expect("search");
    let tables: Vec<_> = page
        .hits
        .iter()
        .filter(|h| h.kind == SearchAssetKind::Table)
        .collect();
    assert_eq!(tables.len(), 2, "namespace READ covers both: {tables:?}");
    assert!(
        !page
            .hits
            .iter()
            .any(|h| h.kind == SearchAssetKind::Namespace),
        "READ does not expose namespace hits: {:?}",
        page.hits
    );

    // LIST_NAMESPACES on the warehouse: namespace hits appear.
    rbac::create_grant(
        &fx.pool,
        ws(),
        &Grantee::Principal(record.id.clone()),
        SecurableType::Warehouse,
        &fx.warehouse_id,
        Privilege::ListNamespaces,
        "test:search",
    )
    .await
    .expect("grant LIST_NAMESPACES");
    let visibility = search::visibility_for(&fx.pool, &identity)
        .await
        .expect("visibility");
    let page = search::search(&fx.pool, ws(), &request, &visibility)
        .await
        .expect("search");
    assert!(
        page.hits
            .iter()
            .any(|h| h.kind == SearchAssetKind::Namespace),
        "LIST_NAMESPACES exposes namespaces: {:?}",
        page.hits
    );

    // Anonymous (auth disabled) sees everything.
    let anon = search::visibility_for(&fx.pool, &Principal::anonymous())
        .await
        .expect("anonymous visibility");
    assert!(anon.unrestricted);
}
