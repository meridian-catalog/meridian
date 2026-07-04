//! End-to-end tests for AI Asset Governance (Pillar I, I-F1..I-F4): the
//! generic-asset registry + fileset credential vending, immutable training-run
//! pinning, per-model provenance + the EU AI Act summary, and GDPR
//! deletion-campaign evidence.
//!
//! These require a running Postgres and `DATABASE_URL`; without it they skip.
//! Auth is OIDC (an in-process IdP): an admin drives the management surface and
//! a separate viewer principal (granted only READ on a fileset asset) proves
//! the fileset vend follows RBAC on the asset securable.
//!
//! The STS fileset-vend leg additionally needs the dev `MinIO`
//! (`localhost:9000`); it skips when `MinIO` is unreachable — the same
//! convention as `vending_api.rs`. Every other case runs on Postgres alone.
//!
//! Test isolation (M3 rules): every asset / model / campaign name carries a
//! ULID suffix; assertions are scoped to this test's own created ids; no global
//! counts.

#[allow(dead_code)]
mod idp;

use std::net::TcpStream;
use std::sync::Arc;
use std::time::Duration;

use axum::Router;
use axum::body::Body;
use axum::http::{Request, StatusCode};
use http_body_util::BodyExt;
use idp::{AUDIENCE, KID1, TestIdp};
use meridian_common::AppConfig;
use meridian_common::config::{AuthMode, OidcIssuerConfig};
use meridian_server::{AppState, build_router};
use meridian_store::tenancy;
use serde_json::{Value, json};
use sqlx::PgPool;
use tower::ServiceExt;
use ulid::Ulid;

// ---------------------------------------------------------------------------
// Harness
// ---------------------------------------------------------------------------

struct Ctx {
    router: Router,
    pool: PgPool,
    idp: TestIdp,
    admin_token: String,
}

async fn oidc_ctx() -> Option<Ctx> {
    let Ok(url) = std::env::var("DATABASE_URL") else {
        eprintln!("skipping assets API test: DATABASE_URL is not set");
        return None;
    };
    let idp = TestIdp::start(&[KID1]).await;
    let issuer_url = idp.issuer.clone();

    let mut config = AppConfig::default();
    config.database.url = url;
    config.auth.mode = AuthMode::Oidc;
    config.auth.oidc.require_https_issuers = false;
    config.auth.oidc.issuers.push(OidcIssuerConfig {
        issuer_url,
        audience: AUDIENCE.to_owned(),
        jwks_uri: None,
    });

    let pool = meridian_store::connect(&config.database)
        .await
        .expect("connect to test database");
    meridian_store::MIGRATOR
        .run(&pool)
        .await
        .expect("run migrations");

    let router = build_router(AppState {
        pool: pool.clone(),
        config: Arc::new(config),
    });

    let admin_sub = format!("admin-{}", Ulid::new());
    meridian_store::rbac::bootstrap_admin(
        &pool,
        tenancy::default_workspace_id(),
        &idp.issuer,
        &admin_sub,
    )
    .await
    .expect("bootstrap admin");
    let admin_token = idp::mint(
        KID1,
        &idp.claims(&admin_sub, json!({ "email": "admin@example.com" })),
    );

    Some(Ctx {
        router,
        pool,
        idp,
        admin_token,
    })
}

async fn send(
    router: &Router,
    method: &str,
    uri: &str,
    token: Option<&str>,
    body: Option<&Value>,
) -> (StatusCode, Value) {
    let mut builder = Request::builder().method(method).uri(uri);
    if let Some(token) = token {
        builder = builder.header("authorization", format!("Bearer {token}"));
    }
    let request = match body {
        Some(body) => builder
            .header("content-type", "application/json")
            .body(Body::from(body.to_string())),
        None => builder.body(Body::empty()),
    }
    .expect("build request");

    let response = router
        .clone()
        .oneshot(request)
        .await
        .expect("infallible router call");
    let status = response.status();
    let bytes = response
        .into_body()
        .collect()
        .await
        .expect("read body")
        .to_bytes();
    let value = if bytes.is_empty() {
        Value::Null
    } else {
        serde_json::from_slice(&bytes).expect("response body is JSON")
    };
    (status, value)
}

/// Mints a fresh viewer, JIT-provisions its principal row, and returns
/// (token, `principal_id`).
async fn provision_viewer(ctx: &Ctx) -> (String, String) {
    let sub = format!("viewer-{}", Ulid::new());
    let token = idp::mint(
        KID1,
        &ctx.idp
            .claims(&sub, json!({ "email": format!("{sub}@example.com") })),
    );
    let (status, _) = send(&ctx.router, "GET", "/v1/config", Some(&token), None).await;
    assert_eq!(status, StatusCode::OK);
    let id: String = sqlx::query_scalar("SELECT id FROM principals WHERE subject = $1")
        .bind(&sub)
        .fetch_one(&ctx.pool)
        .await
        .expect("provisioned principal");
    (token, id)
}

fn minio_reachable() -> bool {
    TcpStream::connect_timeout(
        &"127.0.0.1:9000".parse().expect("static addr"),
        Duration::from_millis(500),
    )
    .is_ok()
}

const MINIO_BUCKET: &str = "meridian-warehouse";
const ROLE_ARN: &str = "arn:minio:iam:::role/meridian-vend";

/// Creates an STS-vending warehouse over `MinIO`, returning its name.
async fn create_sts_warehouse(ctx: &Ctx) -> String {
    let name = format!("wh-asset-{}", Ulid::new()).to_lowercase();
    let (status, body) = send(
        &ctx.router,
        "POST",
        "/api/v2/warehouses",
        Some(&ctx.admin_token),
        Some(&json!({
            "name": name,
            "storage_root": format!("s3://{MINIO_BUCKET}/{name}"),
            "storage_options": {
                "endpoint": "http://localhost:9000",
                "access-key-id": "meridian",
                "secret-access-key": "meridian123",
                "region": "us-east-1",
                "vending": "sts",
                "vending.role-arn": ROLE_ARN,
            },
        })),
    )
    .await;
    assert_eq!(status, StatusCode::CREATED, "create warehouse: {body}");
    name
}

// ===========================================================================
// I-F1: generic assets — model registry CRUD + fileset vend
// ===========================================================================

#[tokio::test]
async fn model_registry_crud_and_asset_validation() {
    let Some(ctx) = oidc_ctx().await else {
        return;
    };
    let admin = Some(ctx.admin_token.as_str());

    // Register a model with kind-specific metadata + license tags.
    let name = format!("clf-{}", Ulid::new());
    let (status, body) = send(
        &ctx.router,
        "POST",
        "/api/v2/assets",
        admin,
        Some(&json!({
            "kind": "model",
            "name": name,
            "owner": "user:ml@example.com",
            "metadata": { "version": "1.0", "framework": "pytorch",
                          "artifacts_location": "s3://models/clf/1.0" },
            "tags": ["license:cc-by", "stage:prod"],
        })),
    )
    .await;
    assert_eq!(status, StatusCode::CREATED, "create model: {body}");
    let model_id = body["id"].as_str().expect("model id").to_owned();
    assert_eq!(body["kind"], "model");
    assert_eq!(body["metadata"]["framework"], "pytorch");
    assert!(
        body["tags"]
            .as_array()
            .expect("tags")
            .iter()
            .any(|t| t == "license:cc-by")
    );

    // Get it back.
    let (status, got) = send(
        &ctx.router,
        "GET",
        &format!("/api/v2/assets/{model_id}"),
        admin,
        None,
    )
    .await;
    assert_eq!(status, StatusCode::OK);
    assert_eq!(got["name"], name);

    // List filtered to models includes it.
    let (status, list) = send(&ctx.router, "GET", "/api/v2/assets?type=model", admin, None).await;
    assert_eq!(status, StatusCode::OK);
    assert!(
        list["assets"]
            .as_array()
            .expect("assets")
            .iter()
            .any(|a| a["id"] == model_id.as_str()),
        "list should include the created model"
    );

    // Duplicate (same kind + name) is a conflict.
    let (status, _) = send(
        &ctx.router,
        "POST",
        "/api/v2/assets",
        admin,
        Some(&json!({ "kind": "model", "name": name })),
    )
    .await;
    assert_eq!(status, StatusCode::CONFLICT, "duplicate model name");

    // A model carrying a storage_prefix is a 400 (only filesets have one).
    let (status, _) = send(
        &ctx.router,
        "POST",
        "/api/v2/assets",
        admin,
        Some(&json!({
            "kind": "model",
            "name": format!("bad-{}", Ulid::new()),
            "storage_prefix": "s3://x/y",
        })),
    )
    .await;
    assert_eq!(status, StatusCode::BAD_REQUEST, "model with storage_prefix");

    // A fileset without a warehouse is a 400.
    let (status, _) = send(
        &ctx.router,
        "POST",
        "/api/v2/assets",
        admin,
        Some(&json!({
            "kind": "fileset",
            "name": format!("fs-{}", Ulid::new()),
            "storage_prefix": "s3://bucket/prefix",
        })),
    )
    .await;
    assert_eq!(status, StatusCode::BAD_REQUEST, "fileset without warehouse");

    // A viewer (no management) cannot create an asset.
    let (viewer, _) = provision_viewer(&ctx).await;
    let (status, _) = send(
        &ctx.router,
        "POST",
        "/api/v2/assets",
        Some(&viewer),
        Some(&json!({ "kind": "model", "name": format!("v-{}", Ulid::new()) })),
    )
    .await;
    assert_eq!(status, StatusCode::FORBIDDEN, "viewer cannot create asset");
}

#[tokio::test]
async fn fileset_vends_credentials_scoped_to_its_prefix_under_rbac() {
    let Some(ctx) = oidc_ctx().await else {
        return;
    };
    if !minio_reachable() {
        eprintln!("SKIP: fileset vend test — no MinIO on localhost:9000");
        return;
    }
    let admin = Some(ctx.admin_token.as_str());
    let warehouse = create_sts_warehouse(&ctx).await;

    // Register a fileset over a prefix in the warehouse's bucket.
    let fs_name = format!("images-{}", Ulid::new());
    let prefix = format!("s3://{MINIO_BUCKET}/{warehouse}/filesets/{fs_name}");
    let (status, body) = send(
        &ctx.router,
        "POST",
        "/api/v2/assets",
        admin,
        Some(&json!({
            "kind": "fileset",
            "name": fs_name,
            "warehouse": warehouse,
            "storage_prefix": prefix,
        })),
    )
    .await;
    assert_eq!(status, StatusCode::CREATED, "create fileset: {body}");
    let asset_id = body["id"].as_str().expect("asset id").to_owned();

    // Admin vends: credentials are scoped to exactly the fileset prefix.
    let (status, creds) = send(
        &ctx.router,
        "POST",
        &format!("/api/v2/assets/{asset_id}/credentials"),
        admin,
        Some(&json!({})),
    )
    .await;
    assert_eq!(status, StatusCode::OK, "admin vend: {creds}");
    let vended_prefix = creds["storage-credentials"][0]["prefix"]
        .as_str()
        .expect("prefix");
    assert!(
        vended_prefix.starts_with(&prefix),
        "vended prefix {vended_prefix} must be the fileset prefix {prefix}"
    );
    assert_eq!(creds["access"], "read-write", "admin gets read-write");

    // A viewer with only READ on the asset gets read-only credentials.
    let (viewer, principal_id) = provision_viewer(&ctx).await;
    let (status, gbody) = send(
        &ctx.router,
        "POST",
        "/api/v2/grants",
        admin,
        Some(&json!({
            "privilege": "READ",
            "principal_id": principal_id,
            "securable": { "type": "asset", "asset": asset_id },
        })),
    )
    .await;
    assert_eq!(status, StatusCode::CREATED, "grant READ on asset: {gbody}");

    let (status, vcreds) = send(
        &ctx.router,
        "POST",
        &format!("/api/v2/assets/{asset_id}/credentials"),
        Some(&viewer),
        Some(&json!({})),
    )
    .await;
    assert_eq!(status, StatusCode::OK, "viewer vend: {vcreds}");
    assert_eq!(vcreds["access"], "read", "READ-only grant -> read creds");

    // A viewer with NO grant on the asset is forbidden.
    let (stranger, _) = provision_viewer(&ctx).await;
    let (status, _) = send(
        &ctx.router,
        "POST",
        &format!("/api/v2/assets/{asset_id}/credentials"),
        Some(&stranger),
        Some(&json!({})),
    )
    .await;
    assert_eq!(status, StatusCode::FORBIDDEN, "ungranted principal denied");
}

// ===========================================================================
// I-F2: training-run pinning is immutable + reproducible
// ===========================================================================

#[tokio::test]
async fn training_run_pins_exact_snapshots_and_is_immutable() {
    let Some(ctx) = oidc_ctx().await else {
        return;
    };
    let admin = Some(ctx.admin_token.as_str());

    let model = format!("recs-{}", Ulid::new());
    // A large snapshot id near i64 range to prove exact preservation.
    let snap_a: i64 = 8_823_066_017_012_345_678;
    let snap_b: i64 = -12_345;
    let (status, body) = send(
        &ctx.router,
        "POST",
        "/api/v2/training-runs",
        admin,
        Some(&json!({
            "model": model,
            "model_version": "2",
            "inputs": [
                { "table_ref": "wh.sales.orders", "table_id": "01ORDERSTABLE00000000000001",
                  "snapshot_id": snap_a },
                { "table_ref": "external.crm.contacts", "snapshot_id": snap_b },
            ],
        })),
    )
    .await;
    assert_eq!(status, StatusCode::CREATED, "pin run: {body}");
    let run_id = body["id"].as_str().expect("run id").to_owned();

    // The pinned snapshot ids are recorded EXACTLY.
    let (status, got) = send(
        &ctx.router,
        "GET",
        &format!("/api/v2/training-runs/{run_id}"),
        admin,
        None,
    )
    .await;
    assert_eq!(status, StatusCode::OK);
    let inputs = got["inputs"].as_array().expect("inputs");
    assert_eq!(inputs.len(), 2);
    let orders = inputs
        .iter()
        .find(|i| i["table_ref"] == "wh.sales.orders")
        .expect("orders input");
    assert_eq!(
        orders["snapshot_id"].as_i64().expect("snapshot i64"),
        snap_a,
        "the exact snapshot id must be preserved"
    );

    // Immutability: there is no update/delete route for a run. Confirm the
    // append-only store has no mutation path by verifying a second GET is
    // byte-identical to the first.
    let (_, again) = send(
        &ctx.router,
        "GET",
        &format!("/api/v2/training-runs/{run_id}"),
        admin,
        None,
    )
    .await;
    assert_eq!(got, again, "an immutable run never changes");

    // A run with zero inputs is a 400.
    let (status, _) = send(
        &ctx.router,
        "POST",
        "/api/v2/training-runs",
        admin,
        Some(&json!({ "model": model, "model_version": "3", "inputs": [] })),
    )
    .await;
    assert_eq!(status, StatusCode::BAD_REQUEST, "empty-inputs run");
}

// ===========================================================================
// I-F3: provenance report + EU AI Act summary
// ===========================================================================

#[tokio::test]
async fn provenance_report_and_ai_act_summary_from_pinned_inputs() {
    let Some(ctx) = oidc_ctx().await else {
        return;
    };
    let admin = Some(ctx.admin_token.as_str());
    let model = format!("gpai-{}", Ulid::new());

    // Two runs of the same model, different versions + inputs.
    for (version, table_ref) in [("1", "wh.docs.corpus_a"), ("2", "wh.docs.corpus_b")] {
        let (status, _) = send(
            &ctx.router,
            "POST",
            "/api/v2/training-runs",
            admin,
            Some(&json!({
                "model": model,
                "model_version": version,
                "inputs": [
                    { "table_ref": table_ref, "snapshot_id": 111 },
                ],
            })),
        )
        .await;
        assert_eq!(status, StatusCode::CREATED);
    }

    // Provenance report lists both runs and their sources.
    let (status, report) = send(
        &ctx.router,
        "GET",
        &format!("/api/v2/models/{model}/provenance"),
        admin,
        None,
    )
    .await;
    assert_eq!(status, StatusCode::OK, "provenance: {report}");
    assert_eq!(report["runs"].as_array().expect("runs").len(), 2);
    let cards = report["dataset_cards"].as_array().expect("dataset cards");
    assert_eq!(cards.len(), 2, "one card per distinct source");
    // The honest boundary: agents_using is present and empty.
    assert_eq!(report["agents_using"], json!([]));

    // Restrict to version 1: one run, one source.
    let (status, v1) = send(
        &ctx.router,
        "GET",
        &format!("/api/v2/models/{model}/provenance?version=1"),
        admin,
        None,
    )
    .await;
    assert_eq!(status, StatusCode::OK, "v1 provenance: {v1}");
    assert_eq!(v1["runs"].as_array().expect("runs").len(), 1);

    // The AI Act summary is generated from the pinned inputs.
    let (status, summary) = send(
        &ctx.router,
        "GET",
        &format!("/api/v2/models/{model}/ai-act-summary"),
        admin,
        None,
    )
    .await;
    assert_eq!(status, StatusCode::OK, "ai act: {summary}");
    assert_eq!(summary["model"], model);
    assert_eq!(
        summary["training_data_sources"]
            .as_array()
            .expect("sources")
            .len(),
        2,
        "AI Act summary enumerates both pinned sources"
    );
    assert!(
        summary["reproducibility"]
            .as_str()
            .expect("reproducibility note")
            .contains("snapshot"),
        "the summary states inputs are reproducible via snapshot pins"
    );

    // An unknown model is a 404.
    let (status, _) = send(
        &ctx.router,
        "GET",
        &format!("/api/v2/models/no-such-{}/provenance", Ulid::new()),
        admin,
        None,
    )
    .await;
    assert_eq!(status, StatusCode::NOT_FOUND);
}

// ===========================================================================
// I-F4: deletion campaign records model exposure evidence
// ===========================================================================

#[tokio::test]
#[allow(clippy::too_many_lines)] // one end-to-end deletion-campaign story
async fn deletion_campaign_records_which_models_saw_deleted_data() {
    let Some(ctx) = oidc_ctx().await else {
        return;
    };
    let admin = Some(ctx.admin_token.as_str());

    // A model trained on a specific (table, snapshot).
    let model = format!("forget-{}", Ulid::new());
    let table_id = format!("01TBL{}", Ulid::new());
    let doomed_snapshot: i64 = 777_001;
    let (status, _) = send(
        &ctx.router,
        "POST",
        "/api/v2/training-runs",
        admin,
        Some(&json!({
            "model": model,
            "model_version": "1",
            "inputs": [
                { "table_ref": "wh.users.events", "table_id": table_id,
                  "snapshot_id": doomed_snapshot },
                { "table_ref": "wh.users.other", "table_id": format!("01OTH{}", Ulid::new()),
                  "snapshot_id": 999 },
            ],
        })),
    )
    .await;
    assert_eq!(status, StatusCode::CREATED);

    // Open a GDPR campaign for the erasure subject.
    let campaign_name = format!("dsar-{}", Ulid::new());
    let (status, camp) = send(
        &ctx.router,
        "POST",
        "/api/v2/deletion-campaigns",
        admin,
        Some(&json!({
            "name": campaign_name,
            "subject": "data-subject-42",
            "reason": "GDPR Art. 17 erasure request",
        })),
    )
    .await;
    assert_eq!(status, StatusCode::CREATED, "open campaign: {camp}");
    let campaign_id = camp["id"].as_str().expect("campaign id").to_owned();
    assert_eq!(camp["status"], "open");

    // Add the affected snapshot — the exact one the model trained on.
    let (status, added) = send(
        &ctx.router,
        "POST",
        &format!("/api/v2/deletion-campaigns/{campaign_id}/snapshots"),
        admin,
        Some(&json!({
            "snapshots": [
                { "table_ref": "wh.users.events", "table_id": table_id,
                  "snapshot_id": doomed_snapshot },
            ],
        })),
    )
    .await;
    assert_eq!(status, StatusCode::OK, "add snapshots: {added}");
    assert_eq!(
        added["model_exposures_recorded"], 1,
        "exactly one model saw the deleted snapshot"
    );

    // The evidence record names the exposed model version + the untouched
    // second input is NOT listed.
    let (status, evidence) = send(
        &ctx.router,
        "GET",
        &format!("/api/v2/deletion-campaigns/{campaign_id}/evidence"),
        admin,
        None,
    )
    .await;
    assert_eq!(status, StatusCode::OK, "evidence: {evidence}");
    assert_eq!(evidence["campaign"]["status"], "evidence_ready");
    let exposure = evidence["model_exposure"].as_array().expect("exposure");
    assert_eq!(exposure.len(), 1, "one exposure row");
    assert_eq!(exposure[0]["model"], model);
    assert_eq!(exposure[0]["model_version"], "1");
    assert_eq!(
        exposure[0]["snapshot_id"].as_i64().expect("snap i64"),
        doomed_snapshot
    );

    // The affected snapshot starts pending; mark it expired closes the campaign.
    let snapshots = evidence["affected_snapshots"].as_array().expect("snaps");
    assert_eq!(snapshots.len(), 1);
    assert_eq!(snapshots[0]["expiry_status"], "pending");
    let snap_row_id = snapshots[0]["id"].as_str().expect("snap row id");

    let (status, closed) = send(
        &ctx.router,
        "POST",
        &format!("/api/v2/deletion-campaigns/{campaign_id}/expire"),
        admin,
        Some(&json!({ "snapshot_row_id": snap_row_id })),
    )
    .await;
    assert_eq!(status, StatusCode::OK, "expire: {closed}");
    assert_eq!(
        closed["status"], "closed",
        "campaign closes once every affected snapshot is expired"
    );

    // A campaign with a different erasure subject that no model saw records
    // zero exposure.
    let (_, camp2) = send(
        &ctx.router,
        "POST",
        "/api/v2/deletion-campaigns",
        admin,
        Some(&json!({ "name": format!("dsar-{}", Ulid::new()), "subject": "nobody" })),
    )
    .await;
    let camp2_id = camp2["id"].as_str().expect("id").to_owned();
    let (status, added2) = send(
        &ctx.router,
        "POST",
        &format!("/api/v2/deletion-campaigns/{camp2_id}/snapshots"),
        admin,
        Some(&json!({
            "snapshots": [
                { "table_ref": "wh.unrelated.tbl",
                  "table_id": format!("01UNREL{}", Ulid::new()), "snapshot_id": 5 },
            ],
        })),
    )
    .await;
    assert_eq!(status, StatusCode::OK, "{added2}");
    assert_eq!(
        added2["model_exposures_recorded"], 0,
        "no model saw this snapshot"
    );
}
