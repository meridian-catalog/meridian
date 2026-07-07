//! Store-level tests for the workbench saved-query surface.
//!
//! Require a running Postgres and `DATABASE_URL`; without it they skip.

use meridian_common::config::DatabaseConfig;
use meridian_store::tenancy;
use meridian_store::workbench::{self, NewSavedQuery};
use sqlx::PgPool;
use ulid::Ulid;

async fn pool() -> Option<PgPool> {
    let Ok(url) = std::env::var("DATABASE_URL") else {
        eprintln!("skipping workbench DB test: DATABASE_URL is not set");
        return None;
    };
    let pool = meridian_store::connect(&DatabaseConfig {
        url,
        ..DatabaseConfig::default()
    })
    .await
    .expect("connect to test database");
    meridian_store::MIGRATOR.run(&pool).await.expect("migrate");
    Some(pool)
}

/// A saved query is private to its owner: a workspace peer must not enumerate,
/// read, or delete it (it carries the SQL a user parked, and the endpoints
/// only checked *authentication*, not ownership).
#[tokio::test]
async fn saved_queries_are_owner_scoped() {
    let Some(pool) = pool().await else { return };
    let ws = tenancy::default_workspace_id();
    let alice = format!("user:alice-{}", Ulid::new());
    let bob = format!("user:bob-{}", Ulid::new());

    let saved = workbench::create_saved_query(
        &pool,
        ws,
        &NewSavedQuery {
            name: &format!("alices-secret-{}", Ulid::new()),
            sql: "SELECT secret FROM finance.payroll",
            warehouse: None,
            default_namespace: &[],
            description: None,
        },
        &alice,
    )
    .await
    .expect("alice saves a query");

    // Bob cannot enumerate it.
    let bob_list = workbench::list_saved_queries(&pool, ws, &bob)
        .await
        .expect("bob lists");
    assert!(
        !bob_list.iter().any(|r| r.id == saved.id),
        "bob must not see alice's saved query in his list"
    );
    // Bob cannot read it by id (reads as absent, not another's SQL).
    assert!(
        workbench::get_saved_query(&pool, ws, &saved.id, &bob)
            .await
            .expect("bob gets")
            .is_none(),
        "bob must not read alice's saved query by id"
    );
    // Bob cannot delete it.
    assert!(
        !workbench::delete_saved_query(&pool, ws, &saved.id, &bob)
            .await
            .expect("bob deletes"),
        "bob must not delete alice's saved query"
    );

    // Alice still sees, reads, and can delete her own.
    assert!(
        workbench::list_saved_queries(&pool, ws, &alice)
            .await
            .expect("alice lists")
            .iter()
            .any(|r| r.id == saved.id)
    );
    assert!(
        workbench::get_saved_query(&pool, ws, &saved.id, &alice)
            .await
            .expect("alice gets")
            .is_some()
    );
    assert!(
        workbench::delete_saved_query(&pool, ws, &saved.id, &alice)
            .await
            .expect("alice deletes"),
        "alice deletes her own saved query"
    );
}
