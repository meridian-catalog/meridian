//! Federation (Pillar B) integration tests over the full router: mirror CRUD,
//! per-mirror sync status + sync-now, the cross-catalog sprawl summary, and
//! the management-level RBAC gate.
//!
//! These require a running Postgres and `DATABASE_URL`; without it they skip
//! (with a note on stderr). The disabled-auth path exercises functional shape;
//! the oidc path exercises the RBAC gate against the in-process test IdP.

// The shared IdP fixture exposes more than this binary uses.
#[allow(dead_code)]
mod idp;

use std::sync::Arc;

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

/// Connects, migrates, and builds a router with the given auth config.
async fn test_router(configure: impl FnOnce(&mut AppConfig)) -> Option<(Router, PgPool)> {
    let Ok(url) = std::env::var("DATABASE_URL") else {
        eprintln!("skipping federation integration test: DATABASE_URL is not set");
        return None;
    };

    let mut config = AppConfig::default();
    config.database.url = url;
    configure(&mut config);

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

/// Sends one request; returns (status, parsed JSON body).
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
        .expect("infallible router");
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

/// A disabled-auth router (anonymous is admin) for functional-shape tests.
async fn disabled_router() -> Option<(Router, PgPool)> {
    test_router(|config| {
        config.auth.mode = AuthMode::Disabled;
    })
    .await
}

// ---------------------------------------------------------------------------
// Mirror CRUD
// ---------------------------------------------------------------------------

#[tokio::test]
#[allow(clippy::too_many_lines)] // one narrative: create, list, update, sync, delete
async fn mirror_crud_lifecycle() {
    let Some((router, _pool)) = disabled_router().await else {
        return;
    };
    let name = format!("mir-{}", Ulid::new()).to_lowercase();

    // Create.
    let (status, body) = send(
        &router,
        "POST",
        "/api/v2/mirrors",
        None,
        Some(&json!({
            "name": name,
            "kind": "iceberg-rest",
            "endpoint": "http://polaris.example/api/catalog",
            "remote_catalog": "prod",
            "config": { "warehouse": "prod", "token": "sekret" },
            "sync_interval_s": 900,
        })),
    )
    .await;
    assert_eq!(status, StatusCode::CREATED, "create mirror: {body}");
    assert_eq!(body["name"], json!(name));
    assert_eq!(body["kind"], json!("iceberg-rest"));
    assert_eq!(body["enabled"], json!(true));
    assert_eq!(body["sync_interval_s"], json!(900));
    assert_eq!(body["asset_count"], json!(0));
    assert_eq!(body["last_synced_at"], Value::Null);
    // Secret config value is redacted on read; non-secret is echoed.
    assert_eq!(body["config"]["token"], json!("***"), "secret redacted");
    assert_eq!(body["config"]["warehouse"], json!("prod"));

    // Duplicate create → 409.
    let (status, body) = send(
        &router,
        "POST",
        "/api/v2/mirrors",
        None,
        Some(&json!({
            "name": name,
            "kind": "glue",
            "endpoint": "us-east-1",
        })),
    )
    .await;
    assert_eq!(status, StatusCode::CONFLICT, "duplicate mirror: {body}");

    // List includes it.
    let (status, body) = send(&router, "GET", "/api/v2/mirrors", None, None).await;
    assert_eq!(status, StatusCode::OK);
    let found = body["mirrors"]
        .as_array()
        .expect("mirrors array")
        .iter()
        .any(|m| m["name"] == json!(name));
    assert!(found, "listing must include the created mirror: {body}");

    // Get one.
    let (status, body) = send(
        &router,
        "GET",
        &format!("/api/v2/mirrors/{name}"),
        None,
        None,
    )
    .await;
    assert_eq!(status, StatusCode::OK, "get mirror: {body}");
    assert_eq!(body["remote_catalog"], json!("prod"));

    // Update: disable + change interval.
    let (status, body) = send(
        &router,
        "PATCH",
        &format!("/api/v2/mirrors/{name}"),
        None,
        Some(&json!({ "enabled": false, "sync_interval_s": 60 })),
    )
    .await;
    assert_eq!(status, StatusCode::OK, "update mirror: {body}");
    assert_eq!(body["enabled"], json!(false));
    assert_eq!(body["sync_interval_s"], json!(60));

    // sync-now on a disabled mirror → 409 (the engine refuses a disabled
    // mirror; a disabled mirror is intentionally not synced).
    let (status, body) = send(
        &router,
        "POST",
        &format!("/api/v2/mirrors/{name}/sync"),
        None,
        None,
    )
    .await;
    assert_eq!(status, StatusCode::CONFLICT, "sync disabled mirror: {body}");

    // Re-enable it. (We do not drive a real sync-now here: the endpoint points
    // at an unreachable host, so a live sync would fail on the network — that
    // path is covered in the federation crate's own tests. The enabled flag
    // and the sync-status read surface are what this API test verifies.)
    let (status, _) = send(
        &router,
        "PATCH",
        &format!("/api/v2/mirrors/{name}"),
        None,
        Some(&json!({ "enabled": true })),
    )
    .await;
    assert_eq!(status, StatusCode::OK);

    // Sync status returns the mirror plus its (possibly empty) run history.
    let (status, body) = send(
        &router,
        "GET",
        &format!("/api/v2/mirrors/{name}/sync"),
        None,
        None,
    )
    .await;
    assert_eq!(status, StatusCode::OK, "sync status: {body}");
    assert_eq!(body["mirror"]["name"], json!(name), "{body}");
    assert!(
        body["history"].is_array(),
        "sync status must carry a history array: {body}"
    );

    // Delete.
    let (status, _) = send(
        &router,
        "DELETE",
        &format!("/api/v2/mirrors/{name}"),
        None,
        None,
    )
    .await;
    assert_eq!(status, StatusCode::NO_CONTENT);

    // Gone: 404 with the federation-specific envelope type.
    let (status, body) = send(
        &router,
        "GET",
        &format!("/api/v2/mirrors/{name}"),
        None,
        None,
    )
    .await;
    assert_eq!(status, StatusCode::NOT_FOUND, "deleted mirror: {body}");
    assert_eq!(body["error"]["type"], json!("NoSuchMirrorException"));
}

#[tokio::test]
async fn mirror_create_validates_kind_and_fields() {
    let Some((router, _pool)) = disabled_router().await else {
        return;
    };

    // Bad kind.
    let (status, body) = send(
        &router,
        "POST",
        "/api/v2/mirrors",
        None,
        Some(&json!({
            "name": format!("bad-{}", Ulid::new()).to_lowercase(),
            "kind": "snowflake",
            "endpoint": "x",
        })),
    )
    .await;
    assert_eq!(status, StatusCode::BAD_REQUEST, "bad kind: {body}");

    // Empty endpoint.
    let (status, body) = send(
        &router,
        "POST",
        "/api/v2/mirrors",
        None,
        Some(&json!({
            "name": format!("bad-{}", Ulid::new()).to_lowercase(),
            "kind": "glue",
            "endpoint": "   ",
        })),
    )
    .await;
    assert_eq!(status, StatusCode::BAD_REQUEST, "empty endpoint: {body}");

    // Illegal name.
    let (status, body) = send(
        &router,
        "POST",
        "/api/v2/mirrors",
        None,
        Some(&json!({
            "name": "has spaces",
            "kind": "glue",
            "endpoint": "us-east-1",
        })),
    )
    .await;
    assert_eq!(status, StatusCode::BAD_REQUEST, "bad name: {body}");
}

// ---------------------------------------------------------------------------
// Sprawl summary
// ---------------------------------------------------------------------------

#[tokio::test]
async fn sprawl_summary_shape_with_warehouse_and_mirror() {
    let Some((router, _pool)) = disabled_router().await else {
        return;
    };

    // Seed a warehouse and a namespace + table so it contributes an asset.
    let root = tempfile::tempdir().expect("tempdir");
    let warehouse = format!("wh-spr-{}", Ulid::new()).to_lowercase();
    let storage_root = format!("file://{}", root.path().join("wh").display());
    let (status, body) = send(
        &router,
        "POST",
        "/api/v2/warehouses",
        None,
        Some(&json!({ "name": warehouse, "storage_root": storage_root })),
    )
    .await;
    assert_eq!(status, StatusCode::CREATED, "seed warehouse: {body}");

    let ns = "sprawl_ns";
    let (status, _) = send(
        &router,
        "POST",
        &format!("/v1/{warehouse}/namespaces"),
        None,
        Some(&json!({ "namespace": [ns] })),
    )
    .await;
    assert_eq!(status, StatusCode::OK);
    let (status, _) = send(
        &router,
        "POST",
        &format!("/v1/{warehouse}/namespaces/{ns}/tables"),
        None,
        Some(&json!({
            "name": "t1",
            "schema": {
                "type": "struct",
                "fields": [{ "id": 1, "name": "id", "required": true, "type": "long" }],
            },
        })),
    )
    .await;
    assert_eq!(status, StatusCode::OK);

    // Seed a mirror (never synced → will be reported stale).
    let mirror = format!("mir-spr-{}", Ulid::new()).to_lowercase();
    let (status, _) = send(
        &router,
        "POST",
        "/api/v2/mirrors",
        None,
        Some(&json!({
            "name": mirror,
            "kind": "glue",
            "endpoint": "us-east-1",
        })),
    )
    .await;
    assert_eq!(status, StatusCode::CREATED);

    // Fetch the sprawl summary.
    let (status, body) = send(&router, "GET", "/api/v2/federation/sprawl", None, None).await;
    assert_eq!(status, StatusCode::OK, "sprawl: {body}");

    // Shape: every documented metric is present and correctly typed.
    assert!(body["source_count"].as_i64().unwrap() >= 2, "{body}");
    assert!(body["warehouse_count"].as_i64().unwrap() >= 1, "{body}");
    assert!(body["mirror_count"].as_i64().unwrap() >= 1, "{body}");
    assert!(body["total_assets"].is_number(), "{body}");
    assert!(body["sources"].is_array(), "{body}");
    assert!(body["duplicates"].is_array(), "{body}");
    assert!(body["stale_mirrors"].is_array(), "{body}");
    assert!(body["ownership_gaps"].is_number(), "{body}");
    assert!(body["owned_mirror_assets"].is_number(), "{body}");
    assert!(body["health"]["tables_scored"].is_number(), "{body}");
    assert!(body["health"]["avg_score"].is_number(), "{body}");

    // The seeded warehouse appears as a source with kind "native".
    let sources = body["sources"].as_array().expect("sources");
    let wh_src = sources
        .iter()
        .find(|s| s["name"] == json!(warehouse))
        .expect("seeded warehouse in sources");
    assert_eq!(wh_src["source_type"], json!("warehouse"));
    assert_eq!(wh_src["kind"], json!("native"));

    // The never-synced mirror is reported stale.
    let stale = body["stale_mirrors"].as_array().expect("stale_mirrors");
    assert!(
        stale.iter().any(|m| m["name"] == json!(mirror)),
        "never-synced mirror must be stale: {body}"
    );
}

#[tokio::test]
async fn sprawl_detects_duplicate_storage_location() {
    let Some((router, pool)) = disabled_router().await else {
        return;
    };

    // Two mirrors whose assets point at the SAME storage location: a
    // zero-copy duplicate that sprawl must surface. We insert the assets
    // directly (the sync worker's job) since no live remote catalog exists in
    // the test.
    let ws = tenancy::default_workspace_id().to_string();
    let shared_loc = format!("s3://lake/dup-{}/metadata.json", Ulid::new());

    let mut ids = Vec::new();
    for i in 0..2 {
        let name = format!("mir-dup-{}-{}", i, Ulid::new()).to_lowercase();
        let (status, body) = send(
            &router,
            "POST",
            "/api/v2/mirrors",
            None,
            Some(&json!({
                "name": name,
                "kind": "iceberg-rest",
                "endpoint": format!("http://cat{i}.example"),
            })),
        )
        .await;
        assert_eq!(status, StatusCode::CREATED, "create mirror {i}: {body}");
        ids.push(body["id"].as_str().expect("mirror id").to_owned());
    }

    for (i, mirror_id) in ids.iter().enumerate() {
        sqlx::query(
            "INSERT INTO mirror_assets
                 (id, mirror_id, workspace_id, remote_ident, asset_type, storage_location, owner)
             VALUES ($1, $2, $3, $4, 'table', $5, $6)",
        )
        .bind(Ulid::new().to_string())
        .bind(mirror_id)
        .bind(&ws)
        // First mirror's asset has an owner; second does not (ownership gap).
        .bind(format!("db.schema.tbl{i}"))
        .bind(&shared_loc)
        .bind(if i == 0 { Some("team-data") } else { None })
        .execute(&pool)
        .await
        .expect("insert mirror asset");
    }

    let (status, body) = send(&router, "GET", "/api/v2/federation/sprawl", None, None).await;
    assert_eq!(status, StatusCode::OK, "sprawl: {body}");

    let dups = body["duplicates"].as_array().expect("duplicates");
    let dup = dups
        .iter()
        .find(|d| d["storage_location"] == json!(shared_loc))
        .expect("shared location must be flagged as a duplicate");
    assert_eq!(dup["source_count"], json!(2), "{dup}");
    assert_eq!(
        dup["sources"].as_array().expect("sources").len(),
        2,
        "duplicate names both sources: {dup}"
    );

    // One asset had no owner → at least one ownership gap; one had an owner.
    assert!(body["ownership_gaps"].as_i64().unwrap() >= 1, "{body}");
    assert!(body["owned_mirror_assets"].as_i64().unwrap() >= 1, "{body}");
}

// ---------------------------------------------------------------------------
// RBAC: management gate
// ---------------------------------------------------------------------------

/// Mints an oidc router with a bootstrapped admin, returns (router, pool,
/// idp, admin token).
async fn oidc_ctx() -> Option<(Router, PgPool, TestIdp, String)> {
    let idp = TestIdp::start(&[KID1]).await;
    let issuer_url = idp.issuer.clone();
    let (router, pool) = test_router(move |config| {
        config.auth.mode = AuthMode::Oidc;
        config.auth.oidc.require_https_issuers = false;
        config.auth.oidc.issuers.push(OidcIssuerConfig {
            issuer_url,
            audience: AUDIENCE.to_owned(),
            jwks_uri: None,
        });
    })
    .await?;

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
    Some((router, pool, idp, admin_token))
}

#[tokio::test]
async fn federation_requires_management_access() {
    let Some((router, _pool, idp, admin_token)) = oidc_ctx().await else {
        return;
    };

    // An ungranted user is denied on every federation endpoint.
    let user_sub = format!("user-{}", Ulid::new());
    let user_token = idp::mint(
        KID1,
        &idp.claims(&user_sub, json!({ "email": "user@example.com" })),
    );

    for (method, uri, body) in [
        ("GET", "/api/v2/mirrors", None),
        (
            "POST",
            "/api/v2/mirrors",
            Some(json!({ "name": "x", "kind": "glue", "endpoint": "us-east-1" })),
        ),
        ("GET", "/api/v2/federation/sprawl", None),
    ] {
        let (status, resp) = send(&router, method, uri, Some(&user_token), body.as_ref()).await;
        assert_eq!(
            status,
            StatusCode::FORBIDDEN,
            "ungranted user must be forbidden on {method} {uri}: {resp}"
        );
        assert_eq!(resp["error"]["type"], json!("ForbiddenException"), "{resp}");
    }

    // The admin can list mirrors and read sprawl.
    let (status, body) = send(&router, "GET", "/api/v2/mirrors", Some(&admin_token), None).await;
    assert_eq!(status, StatusCode::OK, "admin list mirrors: {body}");
    let (status, body) = send(
        &router,
        "GET",
        "/api/v2/federation/sprawl",
        Some(&admin_token),
        None,
    )
    .await;
    assert_eq!(status, StatusCode::OK, "admin sprawl: {body}");
}
