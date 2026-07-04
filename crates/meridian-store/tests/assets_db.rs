//! Store-level tests for AI Asset Governance (Pillar I): the audit-is-the-
//! product invariant on asset/training-run/campaign mutations, generic-asset
//! search, and the append-only immutability of training runs.
//!
//! Require a running Postgres and `DATABASE_URL`; skip without it. Isolated:
//! every name carries a ULID; assertions are scoped to this test's own ids.

use meridian_common::config::DatabaseConfig;
use meridian_store::assets::{
    self, AssetKind, CampaignSnapshot, NewAsset, NewTrainingRun, TrainingInput,
};
use meridian_store::tenancy;
use serde_json::json;
use sqlx::PgPool;
use ulid::Ulid;

async fn test_pool() -> Option<PgPool> {
    let Ok(url) = std::env::var("DATABASE_URL") else {
        eprintln!("skipping assets DB test: DATABASE_URL is not set");
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
    Some(pool)
}

#[tokio::test]
async fn create_asset_writes_audit_and_outbox_in_one_tx() {
    let Some(pool) = test_pool().await else {
        return;
    };
    let ws = tenancy::default_workspace_id();
    let name = format!("model-{}", Ulid::new());

    let record = assets::create_asset(
        &pool,
        ws,
        NewAsset {
            kind: AssetKind::Model,
            name: &name,
            description: None,
            owner: Some("user:test@example.com"),
            warehouse_id: None,
            storage_prefix: None,
            metadata: json!({ "framework": "sklearn" }),
            tags: vec!["license:mit".to_owned()],
        },
        "user:test@example.com",
    )
    .await
    .expect("create asset");

    // The audit row is the product: it exists, scoped to this asset id.
    let audit_count: i64 = sqlx::query_scalar(
        "SELECT count(*) FROM audit_log WHERE action = 'asset.create' AND resource = $1",
    )
    .bind(format!("asset:{}", record.id))
    .fetch_one(&pool)
    .await
    .expect("count audit");
    assert_eq!(audit_count, 1, "exactly one audit row for the create");

    let outbox_count: i64 = sqlx::query_scalar(
        "SELECT count(*) FROM events_outbox WHERE event_type = 'asset.created' AND aggregate = $1",
    )
    .bind(format!("asset:{}", record.id))
    .fetch_one(&pool)
    .await
    .expect("count outbox");
    assert_eq!(outbox_count, 1, "exactly one outbox event for the create");
}

#[tokio::test]
async fn asset_search_finds_by_name_and_tag() {
    let Some(pool) = test_pool().await else {
        return;
    };
    let ws = tenancy::default_workspace_id();
    let unique = Ulid::new().to_string().to_lowercase();
    let name = format!("recommender{unique}");

    let created = assets::create_asset(
        &pool,
        ws,
        NewAsset {
            kind: AssetKind::Model,
            name: &name,
            description: Some("a ranking model"),
            owner: None,
            warehouse_id: None,
            storage_prefix: None,
            metadata: json!({}),
            tags: vec![format!("team{unique}")],
        },
        "user:test",
    )
    .await
    .expect("create");

    // Found by name fragment.
    let hits = assets::search_assets(&pool, ws, &name, None, 20)
        .await
        .expect("search by name");
    assert!(
        hits.iter().any(|h| h.id == created.id),
        "search by name should find the asset"
    );

    // Found by tag fragment.
    let hits = assets::search_assets(&pool, ws, &format!("team{unique}"), None, 20)
        .await
        .expect("search by tag");
    assert!(
        hits.iter().any(|h| h.id == created.id),
        "search by tag should find the asset"
    );
}

#[tokio::test]
async fn training_run_is_append_only_with_exact_snapshots() {
    let Some(pool) = test_pool().await else {
        return;
    };
    let ws = tenancy::default_workspace_id();
    let model = format!("m-{}", Ulid::new());
    let exact: i64 = 9_123_456_789_012_345;

    let (header, inputs) = assets::create_training_run(
        &pool,
        ws,
        NewTrainingRun {
            model_asset_id: None,
            model: &model,
            model_version: "v1",
            metadata: json!({}),
            inputs: vec![TrainingInput {
                table_id: Some("01TESTTABLE0000000000000001".to_owned()),
                table_ref: "wh.ns.tbl".to_owned(),
                snapshot_id: exact,
            }],
        },
        "user:test",
    )
    .await
    .expect("pin run");

    assert_eq!(inputs.len(), 1);
    assert_eq!(
        inputs[0].snapshot_id, exact,
        "snapshot id stored exactly as given"
    );

    // Reproducibility: re-reading returns the same exact id.
    let reread = assets::training_run_inputs(&pool, &header.id)
        .await
        .expect("reread");
    assert_eq!(reread[0].snapshot_id, exact);

    // Append-only: the store exposes no update/delete for runs or inputs.
    // Confirm the audit trail recorded the pin.
    let audit_count: i64 = sqlx::query_scalar(
        "SELECT count(*) FROM audit_log WHERE action = 'training_run.pin' AND resource = $1",
    )
    .bind(format!("training-run:{}", header.id))
    .fetch_one(&pool)
    .await
    .expect("count audit");
    assert_eq!(audit_count, 1);
}

#[tokio::test]
async fn deletion_campaign_freezes_exposure_and_closes_on_expiry() {
    let Some(pool) = test_pool().await else {
        return;
    };
    let ws = tenancy::default_workspace_id();

    // A run that saw a specific snapshot.
    let model = format!("f-{}", Ulid::new());
    let table_id = format!("01TB{}", Ulid::new());
    let snap: i64 = 424_242;
    assets::create_training_run(
        &pool,
        ws,
        NewTrainingRun {
            model_asset_id: None,
            model: &model,
            model_version: "1",
            metadata: json!({}),
            inputs: vec![TrainingInput {
                table_id: Some(table_id.clone()),
                table_ref: "wh.u.events".to_owned(),
                snapshot_id: snap,
            }],
        },
        "user:test",
    )
    .await
    .expect("pin");

    let campaign = assets::create_campaign(
        &pool,
        ws,
        &format!("c-{}", Ulid::new()),
        "subject-x",
        None,
        "user:test",
    )
    .await
    .expect("open campaign");
    assert_eq!(campaign.status, "open");

    let exposures = assets::add_campaign_snapshots(
        &pool,
        ws,
        &campaign.id,
        &[CampaignSnapshot {
            table_id: Some(table_id.clone()),
            table_ref: "wh.u.events".to_owned(),
            snapshot_id: snap,
            branch: None,
        }],
        "user:test",
    )
    .await
    .expect("add snapshots");
    assert_eq!(exposures, 1, "the model that saw the snapshot is recorded");

    let evidence = assets::campaign_model_exposure(&pool, &campaign.id)
        .await
        .expect("exposure");
    assert_eq!(evidence.len(), 1);
    assert_eq!(evidence[0].model, model);
    assert_eq!(evidence[0].snapshot_id, snap);

    // The campaign advanced to evidence_ready.
    let reloaded = assets::get_campaign(&pool, ws, &campaign.id)
        .await
        .expect("reload")
        .expect("exists");
    assert_eq!(reloaded.status, "evidence_ready");

    // Expiring the single affected snapshot closes the campaign.
    let snapshots = assets::campaign_snapshots(&pool, &campaign.id)
        .await
        .expect("snaps");
    assert_eq!(snapshots.len(), 1);
    assert_eq!(snapshots[0].expiry_status, "pending");
    assets::mark_snapshot_expired(&pool, ws, &campaign.id, &snapshots[0].id, "user:test")
        .await
        .expect("expire");

    let closed = assets::get_campaign(&pool, ws, &campaign.id)
        .await
        .expect("reload")
        .expect("exists");
    assert_eq!(closed.status, "closed");

    // Idempotent: expiring again is a no-op success.
    assets::mark_snapshot_expired(&pool, ws, &campaign.id, &snapshots[0].id, "user:test")
        .await
        .expect("idempotent expire");
}
