//! End-to-end tests for Pillar D governance: the management API (tags,
//! policies, bindings, dry-run, effective-policy, coverage, drift, evidence)
//! and — the headline — **Layer-1 scan-plan enforcement**: a policy that masks
//! a column and filters rows, proven to change what a `/plan` response
//! actually returns.
//!
//! These require a running Postgres and `DATABASE_URL`; without it they skip.
//! Every test provisions its own uniquely-named warehouse (a tempdir
//! `file://` root) and a fixture table with real Avro manifests + per-column
//! stats (via `meridian_bench::fixture`), so the column-strip and residual
//! assertions are against ground truth, not mocks.
//!
//! Auth is OIDC (an in-process IdP): an admin sets policies, a separate viewer
//! principal (granted only READ) plans and is filtered — because ABAC runs
//! *after* RBAC READ passes, which requires a real authenticated principal
//! (auth-disabled mode bypasses authorization entirely, so it cannot exercise
//! the ABAC layer).
//!
//! Test isolation (per the M3 rules): every warehouse / namespace / tag /
//! policy name carries a ULID suffix; assertions are scoped to this test's own
//! created ids; no global counts.

#[allow(dead_code)]
mod idp;

use std::sync::Arc;

use axum::Router;
use axum::body::Body;
use axum::http::{Request, StatusCode};
use http_body_util::BodyExt;
use idp::{AUDIENCE, KID1, TestIdp};
use meridian_bench::fixture::{self, SyntheticSpec};
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
    warehouse: String,
    root: tempfile::TempDir,
}

async fn oidc_ctx() -> Option<Ctx> {
    let Ok(url) = std::env::var("DATABASE_URL") else {
        eprintln!("skipping governance API test: DATABASE_URL is not set");
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

    let root = tempfile::tempdir().expect("create tempdir");
    let warehouse = format!("wh-gov-{}", Ulid::new()).to_lowercase();
    let storage_root = format!("file://{}", root.path().join("warehouse").display());
    let (status, body) = send(
        &router,
        "POST",
        "/api/v2/warehouses",
        Some(&admin_token),
        None,
        Some(&json!({ "name": warehouse, "storage_root": storage_root })),
    )
    .await;
    assert_eq!(status, StatusCode::CREATED, "create warehouse: {body}");

    Some(Ctx {
        router,
        pool,
        idp,
        admin_token,
        warehouse,
        root,
    })
}

async fn send(
    router: &Router,
    method: &str,
    uri: &str,
    token: Option<&str>,
    purpose: Option<&str>,
    body: Option<&Value>,
) -> (StatusCode, Value) {
    let mut builder = Request::builder().method(method).uri(uri);
    if let Some(token) = token {
        builder = builder.header("authorization", format!("Bearer {token}"));
    }
    if let Some(purpose) = purpose {
        builder = builder.header("x-meridian-purpose", purpose);
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

fn table_location(ctx: &Ctx, ns: &str, table: &str) -> String {
    format!(
        "file://{}",
        ctx.root
            .path()
            .join("warehouse")
            .join(ns)
            .join(table)
            .display()
    )
}

/// Writes a fixture and registers it as `{ns}.{table}` (admin). Returns the
/// table endpoint base.
async fn register_fixture(ctx: &Ctx, ns: &str, table: &str) -> (String, String) {
    let spec = SyntheticSpec {
        table_location: table_location(ctx, ns, table),
        data_files: 12,
        partitions: 3,
        files_per_manifest: 4,
        rows_per_file: 100,
    };
    let built = fixture::synthetic_table(&spec).expect("generate fixture");
    fixture::write_local(&built.files).expect("write fixture files");

    let (status, body) = send(
        &ctx.router,
        "POST",
        &format!("/v1/{}/namespaces", ctx.warehouse),
        Some(&ctx.admin_token),
        None,
        Some(&json!({ "namespace": [ns] })),
    )
    .await;
    assert!(
        status == StatusCode::OK || status == StatusCode::CONFLICT,
        "create namespace: {body}"
    );
    let (status, body) = send(
        &ctx.router,
        "POST",
        &format!("/v1/{}/namespaces/{ns}/register", ctx.warehouse),
        Some(&ctx.admin_token),
        None,
        Some(&json!({ "name": table, "metadata-location": built.metadata_location })),
    )
    .await;
    assert_eq!(status, StatusCode::OK, "register fixture: {body}");

    let base = format!("/v1/{}/namespaces/{ns}/tables/{table}", ctx.warehouse);
    // The table's internal id, for scoping assertions to our own rows.
    let table_id: String = sqlx::query_scalar(
        "SELECT t.id FROM tables t JOIN namespaces n ON n.id = t.namespace_id
         WHERE n.warehouse_id = (SELECT id FROM warehouses WHERE name = $1)
           AND t.name = $2",
    )
    .bind(&ctx.warehouse)
    .bind(table)
    .fetch_one(&ctx.pool)
    .await
    .expect("table id");
    (base, table_id)
}

/// Mints a token for a fresh viewer user, JIT-provisions its principal row,
/// and grants it READ on the warehouse. Returns (token, subject, principal
/// id).
async fn provision_viewer(ctx: &Ctx) -> (String, String, String) {
    let sub = format!("viewer-{}", Ulid::new());
    let token = idp::mint(
        KID1,
        &ctx.idp
            .claims(&sub, json!({ "email": format!("{sub}@example.com") })),
    );
    // Authenticate once (config is authz-exempt) to JIT-provision the row.
    let (status, _) = send(&ctx.router, "GET", "/v1/config", Some(&token), None, None).await;
    assert_eq!(status, StatusCode::OK);
    let id: String = sqlx::query_scalar("SELECT id FROM principals WHERE subject = $1")
        .bind(&sub)
        .fetch_one(&ctx.pool)
        .await
        .expect("provisioned principal");

    let (status, body) = send(
        &ctx.router,
        "POST",
        "/api/v2/grants",
        Some(&ctx.admin_token),
        None,
        Some(&json!({
            "privilege": "READ",
            "principal_id": id,
            "securable": { "type": "warehouse", "warehouse": ctx.warehouse },
        })),
    )
    .await;
    assert_eq!(status, StatusCode::CREATED, "grant READ: {body}");
    (token, sub, id)
}

fn lower_bound_keys(task: &Value) -> Vec<i64> {
    task["data-file"]["lower-bounds"]["keys"]
        .as_array()
        .map(|a| a.iter().filter_map(Value::as_i64).collect())
        .unwrap_or_default()
}

// ===========================================================================
// Tag + policy management API (happy path + errors + audit)
// ===========================================================================

#[tokio::test]
#[allow(clippy::too_many_lines)] // a sequence of CRUD + version + error cases
async fn tag_and_policy_crud_happy_path_and_errors() {
    let Some(ctx) = oidc_ctx().await else {
        return;
    };
    let admin = Some(ctx.admin_token.as_str());

    // Create a tag.
    let key = format!("pii-{}", Ulid::new()).to_lowercase();
    let (status, body) = send(
        &ctx.router,
        "POST",
        "/api/v2/governance/tags",
        admin,
        None,
        Some(&json!({ "key": key, "value": "email", "description": "email addresses" })),
    )
    .await;
    assert_eq!(status, StatusCode::CREATED, "create tag: {body}");
    let tag_id = body["id"].as_str().expect("tag id").to_owned();
    assert_eq!(body["rendered"], format!("{key}:email"));

    // Duplicate tag -> 409.
    let (status, _) = send(
        &ctx.router,
        "POST",
        "/api/v2/governance/tags",
        admin,
        None,
        Some(&json!({ "key": key, "value": "email" })),
    )
    .await;
    assert_eq!(status, StatusCode::CONFLICT, "duplicate tag must 409");

    // Create a column-mask policy (valid AbacRule).
    let policy_name = format!("mask-{}", Ulid::new()).to_lowercase();
    let (status, body) = send(
        &ctx.router,
        "POST",
        "/api/v2/governance/policies",
        admin,
        None,
        Some(&json!({
            "name": policy_name,
            "kind": "column_mask",
            "definition": {
                "type": "tag_column_mask",
                "id": "mask-pii-email",
                "tag": format!("{key}:email"),
                "exempt_groups": [],
                "mask": { "kind": "hash" }
            }
        })),
    )
    .await;
    assert_eq!(status, StatusCode::CREATED, "create policy: {body}");
    let policy_id = body["id"].as_str().expect("policy id").to_owned();
    assert_eq!(body["version"], 1);

    // Kind/shape mismatch -> 400 (a row_filter kind with a mask rule).
    let (status, body) = send(
        &ctx.router,
        "POST",
        "/api/v2/governance/policies",
        admin,
        None,
        Some(&json!({
            "name": format!("bad-{}", Ulid::new()),
            "kind": "row_filter",
            "definition": { "type": "tag_column_mask", "tag": "x:y", "exempt_groups": [], "mask": {"kind":"null"} }
        })),
    )
    .await;
    assert_eq!(
        status,
        StatusCode::BAD_REQUEST,
        "kind mismatch must 400: {body}"
    );

    // Garbage definition -> 400.
    let (status, _) = send(
        &ctx.router,
        "POST",
        "/api/v2/governance/policies",
        admin,
        None,
        Some(&json!({ "name": format!("bad2-{}", Ulid::new()), "kind": "abac", "definition": { "type": "nonsense" } })),
    )
    .await;
    assert_eq!(status, StatusCode::BAD_REQUEST, "garbage rule must 400");

    // Update bumps the version; history records both.
    let (status, body) = send(
        &ctx.router,
        "PATCH",
        &format!("/api/v2/governance/policies/{policy_id}"),
        admin,
        None,
        Some(&json!({ "enabled": false })),
    )
    .await;
    assert_eq!(status, StatusCode::OK, "update policy: {body}");
    assert_eq!(body["version"], 2);
    assert_eq!(body["enabled"], false);

    let (status, body) = send(
        &ctx.router,
        "GET",
        &format!("/api/v2/governance/policies/{policy_id}/versions"),
        admin,
        None,
        None,
    )
    .await;
    assert_eq!(status, StatusCode::OK);
    let versions = body["versions"].as_array().expect("versions");
    assert_eq!(versions.len(), 2, "two versions after one update: {body}");

    // Rollback to v1 -> new v3 with v1's (enabled) definition.
    let (status, body) = send(
        &ctx.router,
        "POST",
        &format!("/api/v2/governance/policies/{policy_id}/rollback"),
        admin,
        None,
        Some(&json!({ "to_version": 1 })),
    )
    .await;
    assert_eq!(status, StatusCode::OK, "rollback: {body}");
    assert_eq!(body["version"], 3);
    assert_eq!(body["enabled"], true, "v1 was enabled");

    // Non-admin cannot touch governance (403).
    let (viewer, _, _) = provision_viewer(&ctx).await;
    let (status, _) = send(
        &ctx.router,
        "GET",
        "/api/v2/governance/policies",
        Some(&viewer),
        None,
        None,
    )
    .await;
    assert_eq!(status, StatusCode::FORBIDDEN, "non-admin must 403");

    // Every governance mutation left a hash-chained audit row for our tag.
    let audit_count: i64 = sqlx::query_scalar(
        "SELECT COUNT(*) FROM audit_log WHERE resource = $1 AND action = 'governance.tag.create'",
    )
    .bind(format!("tag:{tag_id}"))
    .fetch_one(&ctx.pool)
    .await
    .expect("audit count");
    assert_eq!(audit_count, 1, "tag create is audited");
}

// ===========================================================================
// The headline: Layer-1 scan-plan enforcement
// ===========================================================================

#[tokio::test]
#[allow(clippy::too_many_lines)] // one enforcement scenario, end to end
async fn scan_plan_masks_columns_and_filters_rows() {
    let Some(ctx) = oidc_ctx().await else {
        return;
    };
    let admin = Some(ctx.admin_token.as_str());
    let ns = format!("gov{}", Ulid::new().to_string().to_lowercase());
    let (base, table_id) = register_fixture(&ctx, &ns, "sales").await;
    let (viewer, _, _) = provision_viewer(&ctx).await;

    // --- Baseline: the viewer, with no policies, sees all columns + no filter.
    let (status, body) = send(
        &ctx.router,
        "POST",
        &format!("{base}/plan"),
        Some(&viewer),
        None,
        None,
    )
    .await;
    assert_eq!(status, StatusCode::OK, "baseline plan: {body}");
    assert_eq!(body["status"], "completed");
    let baseline_keys = lower_bound_keys(&body["file-scan-tasks"][0]);
    assert!(
        baseline_keys.contains(&4),
        "baseline must expose the `amount` column (field 4) stats: {body}"
    );
    assert!(
        body["file-scan-tasks"][0].get("residual-filter").is_none(),
        "baseline plan carries no residual"
    );

    // --- Set up governance: tag `amount` (col, field 4) pii; tag the table
    // residency:eu; a column mask on the pii tag; a row filter on the eu tag.
    let pii = format!("pii-{}", Ulid::new()).to_lowercase();
    let residency = format!("res-{}", Ulid::new()).to_lowercase();
    let (s, b) = send(
        &ctx.router,
        "POST",
        "/api/v2/governance/tags",
        admin,
        None,
        Some(&json!({ "key": pii, "value": "amount" })),
    )
    .await;
    assert_eq!(s, StatusCode::CREATED, "{b}");
    let pii_tag_id = b["id"].as_str().unwrap().to_owned();
    let pii_rendered = format!("{pii}:amount");

    let (s, b) = send(
        &ctx.router,
        "POST",
        "/api/v2/governance/tags",
        admin,
        None,
        Some(&json!({ "key": residency, "value": "eu" })),
    )
    .await;
    assert_eq!(s, StatusCode::CREATED, "{b}");
    let res_tag_id = b["id"].as_str().unwrap().to_owned();

    // Assign the pii tag to the `amount` column.
    let (s, b) = send(
        &ctx.router,
        "POST",
        "/api/v2/governance/tags/assignments",
        admin,
        None,
        Some(&json!({
            "tag_id": pii_tag_id,
            "target": { "securable_type": "column", "warehouse": ctx.warehouse,
                        "namespace": ns, "table": "sales", "column": "amount" }
        })),
    )
    .await;
    assert_eq!(s, StatusCode::CREATED, "assign pii to column: {b}");

    // Assign the residency tag to the table.
    let (s, b) = send(
        &ctx.router,
        "POST",
        "/api/v2/governance/tags/assignments",
        admin,
        None,
        Some(&json!({
            "tag_id": res_tag_id,
            "target": { "securable_type": "table", "warehouse": ctx.warehouse,
                        "namespace": ns, "table": "sales" }
        })),
    )
    .await;
    assert_eq!(s, StatusCode::CREATED, "assign residency to table: {b}");

    // Column-mask policy bound to the pii tag.
    let (s, b) = send(
        &ctx.router,
        "POST",
        "/api/v2/governance/policies",
        admin,
        None,
        Some(&json!({
            "name": format!("maskamt-{}", Ulid::new()),
            "kind": "column_mask",
            "definition": { "type": "tag_column_mask", "id": "mask-amount",
                            "tag": pii_rendered, "exempt_groups": [], "mask": { "kind": "hash" } }
        })),
    )
    .await;
    assert_eq!(s, StatusCode::CREATED, "create mask policy: {b}");
    let mask_policy_id = b["id"].as_str().unwrap().to_owned();
    let (s, b) = send(
        &ctx.router,
        "POST",
        &format!("/api/v2/governance/policies/{mask_policy_id}/bindings"),
        admin,
        None,
        Some(&json!({ "target_type": "tag", "tag_id": pii_tag_id })),
    )
    .await;
    assert_eq!(s, StatusCode::CREATED, "bind mask policy: {b}");

    // Row-filter policy bound to the residency tag: only region_000 rows.
    let (s, b) = send(
        &ctx.router,
        "POST",
        "/api/v2/governance/policies",
        admin,
        None,
        Some(&json!({
            "name": format!("eurows-{}", Ulid::new()),
            "kind": "row_filter",
            "definition": {
                "type": "tag_row_filter", "id": "eu-rows",
                "tag": format!("{residency}:eu"), "exempt_groups": [],
                "predicate": { "op": "eq", "column": "region", "value": "region_000" }
            }
        })),
    )
    .await;
    assert_eq!(s, StatusCode::CREATED, "create row-filter policy: {b}");
    let filter_policy_id = b["id"].as_str().unwrap().to_owned();
    let (s, b) = send(
        &ctx.router,
        "POST",
        &format!("/api/v2/governance/policies/{filter_policy_id}/bindings"),
        admin,
        None,
        Some(&json!({ "target_type": "tag", "tag_id": res_tag_id })),
    )
    .await;
    assert_eq!(s, StatusCode::CREATED, "bind row-filter policy: {b}");

    // --- The enforced plan: viewer now sees the mask + filter.
    let (status, body) = send(
        &ctx.router,
        "POST",
        &format!("{base}/plan"),
        Some(&viewer),
        None,
        None,
    )
    .await;
    assert_eq!(status, StatusCode::OK, "enforced plan: {body}");
    assert_eq!(body["status"], "completed");

    // (1) The masked column's stats are stripped from EVERY returned task.
    for task in body["file-scan-tasks"].as_array().expect("tasks") {
        let keys = lower_bound_keys(task);
        assert!(
            !keys.contains(&4),
            "masked column `amount` (field 4) must be absent from returned stats: {task}"
        );
        // Unmasked columns are still present.
        assert!(
            keys.contains(&1),
            "unmasked column `id` (field 1) stays present: {task}"
        );
    }

    // (2) The row filter is injected as a residual on every task.
    let residual = &body["file-scan-tasks"][0]["residual-filter"];
    assert!(
        !residual.is_null(),
        "row-filter policy must inject a residual: {body}"
    );
    // The residual references the filtered column somewhere in its tree.
    assert!(
        residual.to_string().contains("region"),
        "residual must constrain `region`: {residual}"
    );

    // (3) The decision was audited (governance.scan.enforced) for this table.
    let enforced: i64 = sqlx::query_scalar(
        "SELECT COUNT(*) FROM audit_log
         WHERE resource = $1 AND action = 'governance.scan.enforced'",
    )
    .bind(format!("table:{table_id}"))
    .fetch_one(&ctx.pool)
    .await
    .expect("audit");
    assert!(enforced >= 1, "the enforcement decision is audited");

    // (4) The audit detail names the removed column and the applied policies.
    let details: Value = sqlx::query_scalar(
        "SELECT details FROM audit_log
         WHERE resource = $1 AND action = 'governance.scan.enforced'
         ORDER BY seq DESC LIMIT 1",
    )
    .bind(format!("table:{table_id}"))
    .fetch_one(&ctx.pool)
    .await
    .expect("audit details");
    assert_eq!(details["removed_columns"], json!(["amount"]), "{details}");
    assert!(
        details["row_filter_applied"].as_bool().unwrap_or(false),
        "{details}"
    );
    let applied = details["applied_policies"].as_array().expect("applied");
    assert!(
        applied.iter().any(|p| p == &json!(mask_policy_id))
            && applied.iter().any(|p| p == &json!(filter_policy_id)),
        "both policies recorded: {details}"
    );
}

#[tokio::test]
async fn purpose_lifts_a_deny_unless_purpose_policy() {
    let Some(ctx) = oidc_ctx().await else {
        return;
    };
    let admin = Some(ctx.admin_token.as_str());
    let ns = format!("gov{}", Ulid::new().to_string().to_lowercase());
    let (base, _table_id) = register_fixture(&ctx, &ns, "restricted").await;
    let (viewer, _, _) = provision_viewer(&ctx).await;

    // Tag the table pii:high; deny-unless-purpose=fraud_investigation.
    let pii = format!("piihi-{}", Ulid::new()).to_lowercase();
    let (s, b) = send(
        &ctx.router,
        "POST",
        "/api/v2/governance/tags",
        admin,
        None,
        Some(&json!({ "key": pii, "value": "high" })),
    )
    .await;
    assert_eq!(s, StatusCode::CREATED, "{b}");
    let tag_id = b["id"].as_str().unwrap().to_owned();
    let rendered = format!("{pii}:high");

    let (s, b) = send(
        &ctx.router,
        "POST",
        "/api/v2/governance/tags/assignments",
        admin,
        None,
        Some(&json!({
            "tag_id": tag_id,
            "target": { "securable_type": "table", "warehouse": ctx.warehouse,
                        "namespace": ns, "table": "restricted" }
        })),
    )
    .await;
    assert_eq!(s, StatusCode::CREATED, "{b}");

    let (s, b) = send(
        &ctx.router,
        "POST",
        "/api/v2/governance/policies",
        admin,
        None,
        Some(&json!({
            "name": format!("deny-{}", Ulid::new()),
            "kind": "abac",
            "definition": {
                "type": "tag_deny_unless_purpose", "id": "pii-high-deny",
                "description": "pii:high denied unless fraud_investigation",
                "tag": rendered, "actions": ["read"],
                "unless_purpose": ["fraud_investigation"]
            }
        })),
    )
    .await;
    assert_eq!(s, StatusCode::CREATED, "create deny policy: {b}");
    let policy_id = b["id"].as_str().unwrap().to_owned();
    let (s, b) = send(
        &ctx.router,
        "POST",
        &format!("/api/v2/governance/policies/{policy_id}/bindings"),
        admin,
        None,
        Some(&json!({ "target_type": "tag", "tag_id": tag_id })),
    )
    .await;
    assert_eq!(s, StatusCode::CREATED, "bind deny policy: {b}");

    // No purpose -> the plan is denied (403).
    let (status, body) = send(
        &ctx.router,
        "POST",
        &format!("{base}/plan"),
        Some(&viewer),
        None,
        None,
    )
    .await;
    assert_eq!(
        status,
        StatusCode::FORBIDDEN,
        "no-purpose plan must be denied: {body}"
    );

    // With the granted purpose (header) -> the deny lifts, plan completes.
    let (status, body) = send(
        &ctx.router,
        "POST",
        &format!("{base}/plan"),
        Some(&viewer),
        Some("fraud_investigation"),
        None,
    )
    .await;
    assert_eq!(status, StatusCode::OK, "purpose must lift the deny: {body}");
    assert_eq!(body["status"], "completed");
}

// ===========================================================================
// Effective-policy, dry-run, coverage, drift
// ===========================================================================

#[tokio::test]
#[allow(clippy::too_many_lines)] // effective-policy + dry-run + coverage + drift + evidence
async fn effective_policy_and_dry_run_and_coverage_and_drift() {
    let Some(ctx) = oidc_ctx().await else {
        return;
    };
    let admin = Some(ctx.admin_token.as_str());
    let ns = format!("gov{}", Ulid::new().to_string().to_lowercase());
    register_fixture(&ctx, &ns, "analytics").await;
    let (_viewer, viewer_sub, _) = provision_viewer(&ctx).await;
    let viewer_audit = format!("user:{viewer_sub}");

    // Tag `amount` pii and bind a mask.
    let pii = format!("piidr-{}", Ulid::new()).to_lowercase();
    let (_, b) = send(
        &ctx.router,
        "POST",
        "/api/v2/governance/tags",
        admin,
        None,
        Some(&json!({ "key": pii, "value": "amount" })),
    )
    .await;
    let tag_id = b["id"].as_str().unwrap().to_owned();
    let rendered = format!("{pii}:amount");
    send(&ctx.router, "POST", "/api/v2/governance/tags/assignments", admin, None,
        Some(&json!({ "tag_id": tag_id, "target": { "securable_type": "column",
            "warehouse": ctx.warehouse, "namespace": ns, "table": "analytics", "column": "amount" } }))).await;
    let (_, b) = send(&ctx.router, "POST", "/api/v2/governance/policies", admin, None,
        Some(&json!({ "name": format!("m-{}", Ulid::new()), "kind": "column_mask",
            "definition": { "type": "tag_column_mask", "tag": rendered, "exempt_groups": [], "mask": {"kind":"null"} } }))).await;
    let policy_id = b["id"].as_str().unwrap().to_owned();
    send(
        &ctx.router,
        "POST",
        &format!("/api/v2/governance/policies/{policy_id}/bindings"),
        admin,
        None,
        Some(&json!({ "target_type": "tag", "tag_id": tag_id })),
    )
    .await;

    // Effective policy for the viewer on the table: amount masked.
    let uri = format!(
        "/api/v2/governance/effective-policy?principal={}&warehouse={}&namespace={}&table=analytics",
        urlencoding(&viewer_audit),
        ctx.warehouse,
        ns
    );
    let (status, body) = send(&ctx.router, "GET", &uri, admin, None, None).await;
    assert_eq!(status, StatusCode::OK, "effective-policy: {body}");
    assert_eq!(body["denied"], false);
    assert_eq!(body["masked_columns"], json!(["amount"]), "{body}");
    assert!(
        body["applied_policies"]
            .as_array()
            .unwrap()
            .iter()
            .any(|p| p == &json!(policy_id)),
        "the mask policy applies: {body}"
    );

    // Dry-run a *proposed* deny on a not-yet-created tag: previewed, not saved.
    let (status, body) = send(
        &ctx.router,
        "POST",
        "/api/v2/governance/policies/dry-run",
        admin,
        None,
        Some(&json!({
            "kind": "abac",
            "definition": {
                "type": "tag_deny_unless_purpose", "tag": "proposed:secret",
                "actions": ["read"], "unless_purpose": []
            },
            "principals": [viewer_audit],
            "warehouse": ctx.warehouse, "namespace": ns, "table": "analytics",
            "assume_table_tag": "proposed:secret"
        })),
    )
    .await;
    assert_eq!(status, StatusCode::OK, "dry-run: {body}");
    let result = &body["results"][0];
    assert_eq!(result["principal"], viewer_audit);
    assert_eq!(
        result["denied"], true,
        "the proposed deny would deny: {body}"
    );

    // Nothing was persisted by the dry-run (no `proposed:secret` tag exists).
    let leaked: i64 =
        sqlx::query_scalar("SELECT COUNT(*) FROM tags WHERE key = 'proposed' AND value = 'secret'")
            .fetch_one(&ctx.pool)
            .await
            .expect("count");
    assert_eq!(leaked, 0, "dry-run must not persist anything");

    // Coverage: the table shows one tagged column.
    let (status, body) = send(
        &ctx.router,
        "GET",
        &format!(
            "/api/v2/governance/tags/coverage?warehouse={}&namespace={}",
            ctx.warehouse, ns
        ),
        admin,
        None,
        None,
    )
    .await;
    assert_eq!(status, StatusCode::OK, "coverage: {body}");
    let our_table = body["tables"]
        .as_array()
        .unwrap()
        .iter()
        .find(|t| t["name"] == "analytics")
        .expect("our table in coverage");
    assert_eq!(our_table["tagged_columns"], 1, "{our_table}");

    // Drift: with the pii-tagged column now *masked* (bound), there is no
    // drift alert for it. (Assert our column is not flagged.)
    let (status, body) = send(
        &ctx.router,
        "GET",
        &format!("/api/v2/governance/drift?warehouse={}", ctx.warehouse),
        admin,
        None,
        None,
    )
    .await;
    assert_eq!(status, StatusCode::OK, "drift: {body}");
    let flagged_amount = body["alerts"]
        .as_array()
        .unwrap()
        .iter()
        .any(|a| a["column"] == "amount" && a["tag"] == rendered);
    assert!(
        !flagged_amount,
        "a masked pii column is not a drift alert: {body}"
    );

    // Evidence pack includes our policy and a governance audit trail.
    let (status, body) = send(
        &ctx.router,
        "GET",
        "/api/v2/governance/evidence",
        admin,
        None,
        None,
    )
    .await;
    assert_eq!(status, StatusCode::OK, "evidence: {body}");
    assert!(
        body["policies"]
            .as_array()
            .unwrap()
            .iter()
            .any(|p| p["id"] == json!(policy_id)),
        "evidence lists our policy"
    );
    assert!(
        !body["audit_trail"].as_array().unwrap().is_empty(),
        "evidence carries the governance audit trail"
    );
}

#[tokio::test]
async fn drift_flags_a_classified_but_unmasked_column() {
    let Some(ctx) = oidc_ctx().await else {
        return;
    };
    let admin = Some(ctx.admin_token.as_str());
    let ns = format!("gov{}", Ulid::new().to_string().to_lowercase());
    register_fixture(&ctx, &ns, "leaky").await;

    // Tag `category` pii but bind NO mask -> drift alert.
    let pii = format!("piileak-{}", Ulid::new()).to_lowercase();
    let (_, b) = send(
        &ctx.router,
        "POST",
        "/api/v2/governance/tags",
        admin,
        None,
        Some(&json!({ "key": pii, "value": "category" })),
    )
    .await;
    let tag_id = b["id"].as_str().unwrap().to_owned();
    let rendered = format!("{pii}:category");
    send(&ctx.router, "POST", "/api/v2/governance/tags/assignments", admin, None,
        Some(&json!({ "tag_id": tag_id, "target": { "securable_type": "column",
            "warehouse": ctx.warehouse, "namespace": ns, "table": "leaky", "column": "category" } }))).await;

    let (status, body) = send(
        &ctx.router,
        "GET",
        &format!("/api/v2/governance/drift?warehouse={}", ctx.warehouse),
        admin,
        None,
        None,
    )
    .await;
    assert_eq!(status, StatusCode::OK, "drift: {body}");
    let flagged = body["alerts"]
        .as_array()
        .unwrap()
        .iter()
        .any(|a| a["column"] == "category" && a["tag"] == rendered);
    assert!(flagged, "an unmasked pii column must be flagged: {body}");
}

/// Minimal percent-encoding for the one query value we pass that can contain a
/// `:` and `@` (an audit string). Enough for these tests, not general.
fn urlencoding(s: &str) -> String {
    s.replace(':', "%3A").replace('@', "%40")
}
