//! The batched search-score path must agree with the per-table path.
//!
//! Requires a running Postgres and `DATABASE_URL`; without it the test skips.

use std::collections::BTreeMap;

use meridian_common::config::DatabaseConfig;
use meridian_store::table::{self, NewTable};
use meridian_store::{namespace, quality_score, tenancy, warehouse};
use sqlx::PgPool;
use ulid::Ulid;

async fn setup() -> Option<(PgPool, Vec<String>)> {
    let Ok(url) = std::env::var("DATABASE_URL") else {
        eprintln!("skipping quality-score DB test: DATABASE_URL is not set");
        return None;
    };
    let pool = meridian_store::connect(&DatabaseConfig {
        url,
        ..DatabaseConfig::default()
    })
    .await
    .expect("connect");
    meridian_store::MIGRATOR.run(&pool).await.expect("migrate");

    let run = Ulid::new().to_string().to_lowercase();
    let ws = tenancy::default_workspace_id();
    let wh = warehouse::create(
        &pool,
        ws,
        &format!("qs-wh-{run}"),
        "s3://qs/root",
        BTreeMap::new(),
        "test:qs",
    )
    .await
    .expect("warehouse");
    let levels = vec![format!("qs_ns_{run}")];
    let ns = namespace::create(&pool, ws, &wh.id, &levels, BTreeMap::new(), "test:qs")
        .await
        .expect("namespace");

    // Two tables: one with an owner+comment (docs signals), one bare.
    let mut ids = Vec::new();
    for (i, props) in [
        BTreeMap::from([
            ("owner".to_owned(), "team-data".to_owned()),
            ("comment".to_owned(), "the orders fact table".to_owned()),
        ]),
        BTreeMap::new(),
    ]
    .into_iter()
    .enumerate()
    {
        let uuid = format!("uuid-{}", Ulid::new());
        let tbl = table::create(
            &pool,
            NewTable {
                workspace_id: ws,
                namespace_id: &ns.id,
                namespace_levels: &levels,
                name: &format!("t{i}"),
                table_uuid: &uuid,
                metadata_location: &format!("s3://qs/root/t{i}/metadata/0.json"),
                format_version: 2,
                properties: &props,
                schema_text: Some("id long amount decimal"),
                snapshots: &[],
                origin: "create",
            },
            "test:qs",
            None,
        )
        .await
        .expect("table");
        ids.push(tbl.id);
    }
    Some((pool, ids))
}

#[tokio::test]
async fn batched_search_score_matches_per_table() {
    let Some((pool, ids)) = setup().await else {
        return;
    };
    let ws = tenancy::default_workspace_id();

    let batch = quality_score::score_for_search_batch(&pool, ws, &ids)
        .await
        .expect("batch score");

    for id in &ids {
        let single = quality_score::score_for_search(&pool, ws, id)
            .await
            .expect("single score");
        assert_eq!(
            batch.get(id).copied(),
            Some(single),
            "batch and per-table search score must agree for {id}"
        );
    }

    // The empty-input case is a no-op, not a query.
    assert!(
        quality_score::score_for_search_batch(&pool, ws, &[])
            .await
            .expect("empty batch")
            .is_empty()
    );
}
