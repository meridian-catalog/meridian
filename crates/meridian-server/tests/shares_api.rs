//! Cross-org data-sharing API tests (Pillar J, J-F1/J-F2).
//!
//! Two surfaces are exercised end to end:
//!
//! - the **management API** (`/api/v2/shares`, `/api/v2/marketplace`), and
//! - the **recipient IRC endpoint** (`/share/{token}/v1/...`), which serves
//!   only the shared assets, read-only, with the grant's column mask applied,
//!   and audits every recipient access.
//!
//! These require a running Postgres and `DATABASE_URL`; without it they skip.
//! Tests that create real tables and assert on vended credentials also need
//! the dev `MinIO` (`localhost:9000`) and skip the vend-specific assertions
//! when it is unreachable — same conventions as `vending_api.rs`. Each test
//! uses uniquely-named objects and scopes its assertions to its own ids.

use std::net::TcpStream;
use std::sync::Arc;
use std::time::Duration;

use axum::Router;
use axum::body::Body;
use axum::http::{Request, StatusCode};
use http_body_util::BodyExt;
use meridian_common::AppConfig;
use meridian_server::{AppState, build_router};
use serde_json::{Value, json};
use sqlx::PgPool;
use tower::ServiceExt;
use ulid::Ulid;

const MINIO_ENDPOINT: &str = "http://localhost:9000";
const MINIO_BUCKET: &str = "meridian-warehouse";
const MINIO_ACCESS_KEY: &str = "meridian";
const MINIO_SECRET_KEY: &str = "meridian123";
const ROLE_ARN: &str = "arn:minio:iam:::role/meridian-vend";

fn minio_reachable() -> bool {
    TcpStream::connect_timeout(
        &"127.0.0.1:9000".parse().expect("static addr"),
        Duration::from_millis(500),
    )
    .is_ok()
}

async fn test_router() -> Option<(Router, PgPool)> {
    let Ok(url) = std::env::var("DATABASE_URL") else {
        eprintln!("skipping shares API test: DATABASE_URL is not set");
        return None;
    };
    let mut config = AppConfig::default();
    config.database.url = url;
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
    Some((router, pool))
}

async fn send(
    router: &Router,
    method: &str,
    uri: &str,
    body: Option<Value>,
    headers: &[(&str, &str)],
) -> (StatusCode, String) {
    let mut builder = Request::builder().method(method).uri(uri);
    for (name, value) in headers {
        builder = builder.header(*name, *value);
    }
    let body = match body {
        Some(value) => {
            builder = builder.header("content-type", "application/json");
            Body::from(value.to_string())
        }
        None => Body::empty(),
    };
    let response = router
        .clone()
        .oneshot(builder.body(body).expect("build request"))
        .await
        .expect("infallible router call");
    let status = response.status();
    let bytes = response
        .into_body()
        .collect()
        .await
        .expect("read body")
        .to_bytes();
    (status, String::from_utf8_lossy(&bytes).into_owned())
}

fn parse(raw: &str) -> Value {
    serde_json::from_str(raw).expect("response body is JSON")
}

fn schema_with_ssn() -> Value {
    json!({
        "type": "struct",
        "fields": [
            { "id": 1, "name": "id", "required": true, "type": "long" },
            { "id": 2, "name": "region", "required": false, "type": "string" },
            { "id": 3, "name": "ssn", "required": false, "type": "string" },
        ],
    })
}

async fn create_minio_warehouse(router: &Router, name: &str, extra: &[(&str, &str)]) {
    let mut options = json!({
        "endpoint": MINIO_ENDPOINT,
        "region": "us-east-1",
        "path-style": "true",
        "access-key-id": MINIO_ACCESS_KEY,
        "secret-access-key": MINIO_SECRET_KEY,
    });
    for (key, value) in extra {
        options[*key] = json!(value);
    }
    let (status, raw) = send(
        router,
        "POST",
        "/api/v2/warehouses",
        Some(json!({
            "name": name,
            "storage_root": format!("s3://{MINIO_BUCKET}/{name}"),
            "storage_options": options,
        })),
        &[],
    )
    .await;
    assert_eq!(status, StatusCode::CREATED, "create warehouse: {raw}");
}

/// Creates `ns.{table}` in the warehouse (namespace created idempotently).
async fn create_table(router: &Router, warehouse: &str, table: &str) {
    let (status, raw) = send(
        router,
        "POST",
        &format!("/v1/{warehouse}/namespaces"),
        Some(json!({ "namespace": ["ns"] })),
        &[],
    )
    .await;
    assert!(
        status == StatusCode::OK || status == StatusCode::CONFLICT,
        "create namespace: {raw}"
    );
    let (status, raw) = send(
        router,
        "POST",
        &format!("/v1/{warehouse}/namespaces/ns/tables"),
        Some(json!({ "name": table, "schema": schema_with_ssn() })),
        &[],
    )
    .await;
    assert_eq!(status, StatusCode::OK, "create table: {raw}");
}

/// Reads a table id from Postgres by warehouse name + table name (the store
/// exposes no list-by-id over HTTP; the recipient endpoint resolves by name).
async fn table_id(pool: &PgPool, warehouse: &str, table: &str) -> String {
    sqlx::query_scalar::<_, String>(
        "SELECT t.id FROM tables t
         JOIN namespaces n ON n.id = t.namespace_id
         JOIN warehouses w ON w.id = n.warehouse_id
         WHERE w.name = $1 AND t.name = $2",
    )
    .bind(warehouse)
    .bind(table)
    .fetch_one(pool)
    .await
    .expect("resolve table id")
}

async fn create_share(router: &Router, name: &str, recipient: &str, terms: Option<&str>) -> Value {
    let mut body = json!({ "name": name, "recipient": recipient });
    if let Some(t) = terms {
        body["terms"] = json!(t);
    }
    let (status, raw) = send(router, "POST", "/api/v2/shares", Some(body), &[]).await;
    assert_eq!(status, StatusCode::CREATED, "create share: {raw}");
    parse(&raw)
}

/// Full recipient walk: create a share of a table with a column mask, confirm
/// the recipient endpoint lists ONLY the shared table, load applies the mask
/// and (with `MinIO`) vends read-only credentials, writes are rejected, revoke
/// denies access, and every recipient access is audited.
#[tokio::test]
#[allow(clippy::too_many_lines)] // one end-to-end recipient walk
async fn share_serves_only_granted_assets_readonly_masked_and_audited() {
    let Some((router, pool)) = test_router().await else {
        return;
    };
    if !minio_reachable() {
        eprintln!("SKIP: shares recipient test — no MinIO on localhost:9000");
        return;
    }
    let run = Ulid::new().to_string().to_lowercase();
    let warehouse = format!("wh-share-{run}");
    create_minio_warehouse(
        &router,
        &warehouse,
        &[("vending", "sts"), ("vending.role-arn", ROLE_ARN)],
    )
    .await;

    // Two tables: only "shared" is granted; "secret" must stay invisible.
    create_table(&router, &warehouse, "shared").await;
    create_table(&router, &warehouse, "secret").await;
    let shared_table_id = table_id(&pool, &warehouse, "shared").await;

    // Create a share and grant the shared table, masking the ssn column.
    let share = create_share(&router, &format!("s-{run}"), "org:acme", None).await;
    let share_id = share["id"].as_str().expect("share id").to_owned();
    let token = share["token"].as_str().expect("token").to_owned();
    assert!(!token.is_empty(), "create returns the token exactly once");

    let (status, raw) = send(
        &router,
        "POST",
        &format!("/api/v2/shares/{share_id}/grants"),
        Some(json!({
            "securable_kind": "table",
            "securable_ref": format!("table:{shared_table_id}"),
            "row_filter": "region = 'EU'",
            "column_mask": ["ssn"],
        })),
        &[],
    )
    .await;
    assert_eq!(status, StatusCode::CREATED, "add grant: {raw}");

    // --- Recipient config: read-only endpoint set, resolves by token. ---
    let (status, raw) = send(
        &router,
        "GET",
        &format!("/share/{token}/v1/config"),
        None,
        &[],
    )
    .await;
    assert_eq!(status, StatusCode::OK, "recipient config: {raw}");
    let cfg = parse(&raw);
    let endpoints = cfg["endpoints"].as_array().expect("endpoints");
    assert!(
        endpoints
            .iter()
            .all(|e| !e.as_str().unwrap_or("").starts_with("POST")),
        "the recipient catalog advertises no write endpoints: {cfg}"
    );

    // --- Recipient lists ONLY the shared table in ns. ---
    let (status, raw) = send(
        &router,
        "GET",
        &format!("/share/{token}/v1/namespaces/ns/tables"),
        None,
        &[],
    )
    .await;
    assert_eq!(status, StatusCode::OK, "recipient list tables: {raw}");
    let listed = parse(&raw);
    let names: Vec<&str> = listed["identifiers"]
        .as_array()
        .expect("identifiers")
        .iter()
        .map(|i| i["name"].as_str().unwrap_or(""))
        .collect();
    assert_eq!(names, vec!["shared"], "only the granted table is listed");

    // --- Recipient load: masked schema, read-only marker, vended read creds. ---
    let (status, raw) = send(
        &router,
        "GET",
        &format!("/share/{token}/v1/namespaces/ns/tables/shared"),
        None,
        &[],
    )
    .await;
    assert_eq!(status, StatusCode::OK, "recipient load shared: {raw}");
    let body = parse(&raw);
    // The masked column must be gone from the current schema.
    let fields = &body["metadata"]["schemas"][0]["fields"];
    let field_names: Vec<&str> = fields
        .as_array()
        .expect("fields")
        .iter()
        .map(|f| f["name"].as_str().unwrap_or(""))
        .collect();
    assert!(
        field_names.contains(&"id") && field_names.contains(&"region"),
        "unmasked columns remain: {field_names:?}"
    );
    assert!(
        !field_names.contains(&"ssn"),
        "the masked column ssn must not be served: {field_names:?}"
    );
    // Read-only marker + advisory row filter surfaced to the engine.
    assert_eq!(body["config"]["meridian.share.read-only"], "true");
    assert_eq!(body["config"]["meridian.share.row-filter"], "region = 'EU'");
    // Read-only vended credentials (session token present; not the parent key).
    let creds = body["storage-credentials"]
        .as_array()
        .expect("storage-credentials");
    assert_eq!(creds.len(), 1, "read creds vended");
    assert_ne!(creds[0]["config"]["s3.access-key-id"], MINIO_ACCESS_KEY);

    // --- Recipient CANNOT load a non-shared table (404, not "forbidden"). ---
    let (status, _raw) = send(
        &router,
        "GET",
        &format!("/share/{token}/v1/namespaces/ns/tables/secret"),
        None,
        &[],
    )
    .await;
    assert_eq!(
        status,
        StatusCode::NOT_FOUND,
        "a non-shared table must be invisible to the recipient"
    );

    // --- Writes are rejected (read-only by construction). ---
    let (status, _raw) = send(
        &router,
        "POST",
        &format!("/share/{token}/v1/namespaces/ns/tables/shared"),
        Some(json!({ "updates": [], "requirements": [] })),
        &[],
    )
    .await;
    assert_eq!(status, StatusCode::FORBIDDEN, "a commit to a share is 403");

    // --- Recipient access is audited. ---
    let audit_count: i64 = sqlx::query_scalar(
        "SELECT count(*) FROM audit_log
         WHERE resource = $1 AND action LIKE 'share.recipient.%'",
    )
    .bind(format!("share:{share_id}"))
    .fetch_one(&pool)
    .await
    .expect("count recipient audit");
    assert!(
        audit_count >= 3,
        "config + list + load recipient accesses are audited (got {audit_count})"
    );

    // --- Revoke: the recipient is denied instantly. ---
    let (status, raw) = send(
        &router,
        "POST",
        &format!("/api/v2/shares/{share_id}/revoke"),
        None,
        &[],
    )
    .await;
    assert_eq!(status, StatusCode::OK, "revoke: {raw}");
    let (status, _raw) = send(
        &router,
        "GET",
        &format!("/share/{token}/v1/namespaces/ns/tables/shared"),
        None,
        &[],
    )
    .await;
    assert_eq!(
        status,
        StatusCode::FORBIDDEN,
        "a revoked share serves nothing"
    );
}

/// The terms-acceptance gate: a share with terms serves no data until the
/// recipient accepts, then serves; acceptance is idempotent.
#[tokio::test]
async fn share_terms_must_be_accepted_before_data_serves() {
    let Some((router, _pool)) = test_router().await else {
        return;
    };
    let run = Ulid::new().to_string().to_lowercase();
    let share = create_share(
        &router,
        &format!("s-terms-{run}"),
        "org:partner",
        Some("Read-only. No redistribution."),
    )
    .await;
    let token = share["token"].as_str().expect("token").to_owned();

    // Config resolves even un-accepted, and flags that terms are required.
    let (status, raw) = send(
        &router,
        "GET",
        &format!("/share/{token}/v1/config"),
        None,
        &[],
    )
    .await;
    assert_eq!(status, StatusCode::OK, "config pre-acceptance: {raw}");
    assert_eq!(parse(&raw)["overrides"]["terms-required"], "true");

    // Listing namespaces is blocked until acceptance.
    let (status, _raw) = send(
        &router,
        "GET",
        &format!("/share/{token}/v1/namespaces"),
        None,
        &[],
    )
    .await;
    assert_eq!(
        status,
        StatusCode::FORBIDDEN,
        "un-accepted terms block data access"
    );

    // Read the terms, then accept.
    let (status, raw) = send(&router, "GET", &format!("/share/{token}/terms"), None, &[]).await;
    assert_eq!(status, StatusCode::OK, "get terms: {raw}");
    assert_eq!(parse(&raw)["terms"], "Read-only. No redistribution.");

    let (status, raw) = send(
        &router,
        "POST",
        &format!("/share/{token}/terms/accept"),
        None,
        &[],
    )
    .await;
    assert_eq!(status, StatusCode::OK, "accept terms: {raw}");
    assert_eq!(parse(&raw)["terms_accepted"], true);

    // Now namespace listing works (empty, since no grants — but not 403).
    let (status, raw) = send(
        &router,
        "GET",
        &format!("/share/{token}/v1/namespaces"),
        None,
        &[],
    )
    .await;
    assert_eq!(status, StatusCode::OK, "post-acceptance list: {raw}");

    // Idempotent re-accept.
    let (status, _raw) = send(
        &router,
        "POST",
        &format!("/share/{token}/terms/accept"),
        None,
        &[],
    )
    .await;
    assert_eq!(status, StatusCode::OK, "re-accept is a no-op success");
}

/// An invalid token is a clean 401 (do not leak share existence).
#[tokio::test]
async fn unknown_share_token_is_unauthorized() {
    let Some((router, _pool)) = test_router().await else {
        return;
    };
    let (status, _raw) = send(
        &router,
        "GET",
        "/share/deadbeefdeadbeefdeadbeefdeadbeef/v1/config",
        None,
        &[],
    )
    .await;
    assert_eq!(status, StatusCode::UNAUTHORIZED, "unknown token is 401");
}

/// The internal marketplace (J-F2): the certified-product gallery lists
/// certified-first, and a request-access flow creates a pending request that
/// an approver can decide.
#[tokio::test]
#[allow(clippy::too_many_lines)] // one walk: seed, gallery order, request, decide
async fn marketplace_lists_products_and_runs_request_access_flow() {
    let Some((router, _pool)) = test_router().await else {
        return;
    };
    let run = Ulid::new().to_string().to_lowercase();

    // Seed a certified and a draft product.
    let (status, raw) = send(
        &router,
        "POST",
        "/api/v2/products",
        Some(json!({ "name": format!("certified-{run}"), "certification": "certified" })),
        &[],
    )
    .await;
    assert_eq!(
        status,
        StatusCode::CREATED,
        "create certified product: {raw}"
    );
    let (status, raw) = send(
        &router,
        "POST",
        "/api/v2/products",
        Some(json!({ "name": format!("draft-{run}"), "certification": "draft" })),
        &[],
    )
    .await;
    assert_eq!(status, StatusCode::CREATED, "create draft product: {raw}");

    // The gallery lists them, certified first.
    let (status, raw) = send(&router, "GET", "/api/v2/marketplace/products", None, &[]).await;
    assert_eq!(status, StatusCode::OK, "marketplace products: {raw}");
    let products = parse(&raw);
    let list = products["products"].as_array().expect("products");
    // Find our two; the certified one must precede the draft one in the list.
    let certified_pos = list
        .iter()
        .position(|p| p["name"] == json!(format!("certified-{run}")));
    let draft_pos = list
        .iter()
        .position(|p| p["name"] == json!(format!("draft-{run}")));
    assert!(
        certified_pos.is_some() && draft_pos.is_some(),
        "both listed"
    );
    assert!(
        certified_pos < draft_pos,
        "certified products are listed before drafts"
    );

    // Request access to an asset.
    let (status, raw) = send(
        &router,
        "POST",
        "/api/v2/marketplace/requests",
        Some(json!({
            "securable_type": "table",
            "securable_id": format!("table:demo-{run}"),
            "privilege": "READ",
            "purpose": "quarterly revenue analysis",
        })),
        &[],
    )
    .await;
    assert_eq!(status, StatusCode::CREATED, "request access: {raw}");
    let request = parse(&raw);
    let request_id = request["id"].as_str().expect("request id").to_owned();
    assert_eq!(request["state"], "pending");

    // It appears in the approver's pending queue.
    let (status, raw) = send(
        &router,
        "GET",
        "/api/v2/marketplace/requests?state=pending",
        None,
        &[],
    )
    .await;
    assert_eq!(status, StatusCode::OK, "list requests: {raw}");
    let queue = parse(&raw);
    assert!(
        queue["requests"]
            .as_array()
            .expect("requests")
            .iter()
            .any(|r| r["id"] == json!(request_id)),
        "the new request is in the pending queue"
    );

    // Approve it.
    let (status, raw) = send(
        &router,
        "POST",
        &format!("/api/v2/marketplace/requests/{request_id}/decide"),
        Some(json!({ "approve": true, "reason": "approved for finance" })),
        &[],
    )
    .await;
    assert_eq!(status, StatusCode::OK, "decide: {raw}");
    assert_eq!(parse(&raw)["state"], "approved");

    // Deciding again is a conflict (already decided).
    let (status, _raw) = send(
        &router,
        "POST",
        &format!("/api/v2/marketplace/requests/{request_id}/decide"),
        Some(json!({ "approve": false })),
        &[],
    )
    .await;
    assert_eq!(
        status,
        StatusCode::CONFLICT,
        "an already-decided request cannot be decided again"
    );
}
