//! `GET /api/v2/search` integration tests: parameter validation, the
//! end-to-end column-name path (createTable writes `schema_text`, search
//! finds the column), RBAC visibility filtering over HTTP, pagination, and
//! the disabled-mode (anonymous) behavior.
//!
//! These require a running Postgres and `DATABASE_URL`; without it they
//! skip (with a note on stderr). Tokens are minted against the in-process
//! test IdP from `tests/idp`.

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

async fn test_router(configure: impl FnOnce(&mut AppConfig)) -> Option<(Router, PgPool)> {
    let Ok(url) = std::env::var("DATABASE_URL") else {
        eprintln!("skipping search API test: DATABASE_URL is not set");
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

/// An oidc-mode context: router, pool, admin token, one warehouse, one
/// namespace (`ns<salt>`), and one table with a salted column.
struct Ctx {
    router: Router,
    pool: PgPool,
    idp: TestIdp,
    admin_token: String,
    warehouse: String,
    salt: String,
    table: String,
    column: String,
    _root: tempfile::TempDir,
}

async fn oidc_ctx() -> Option<Ctx> {
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

    let salt = Ulid::new().to_string().to_lowercase();
    let root = tempfile::tempdir().expect("create tempdir");
    let warehouse = format!("wh-search-{salt}");
    let storage_root = format!("file://{}", root.path().join("warehouse").display());
    let (status, body) = send(
        &router,
        "POST",
        "/api/v2/warehouses",
        Some(&admin_token),
        Some(&json!({ "name": warehouse, "storage_root": storage_root })),
    )
    .await;
    assert_eq!(status, StatusCode::CREATED, "create warehouse: {body}");

    let (status, body) = send(
        &router,
        "POST",
        &format!("/v1/{warehouse}/namespaces"),
        Some(&admin_token),
        Some(&json!({ "namespace": [format!("ns-{salt}")] })),
    )
    .await;
    assert_eq!(status, StatusCode::OK, "create namespace: {body}");

    // A table whose schema carries a salted column with a doc string —
    // exercises the real write-through (createTable → schema_text).
    let table = format!("orders_{salt}");
    let column = format!("customer_email_{salt}");
    let (status, body) = send(
        &router,
        "POST",
        &format!("/v1/{warehouse}/namespaces/ns-{salt}/tables"),
        Some(&admin_token),
        Some(&json!({
            "name": table,
            "schema": {
                "type": "struct",
                "fields": [
                    { "id": 1, "name": "id", "required": true, "type": "long" },
                    { "id": 2, "name": column, "required": false, "type": "string",
                      "doc": "primary contact email" },
                ],
            },
        })),
    )
    .await;
    assert_eq!(status, StatusCode::OK, "create table: {body}");

    Some(Ctx {
        router,
        pool,
        idp,
        admin_token,
        warehouse,
        salt,
        table,
        column,
        _root: root,
    })
}

/// Mints a token for a fresh user and JIT-provisions its principal row.
async fn provision_user(ctx: &Ctx) -> (String, String) {
    let sub = format!("user-{}", Ulid::new());
    let token = idp::mint(
        KID1,
        &ctx.idp
            .claims(&sub, json!({ "email": format!("{sub}@example.com") })),
    );
    let (status, body) = send(&ctx.router, "GET", "/v1/config", Some(&token), None).await;
    assert_eq!(status, StatusCode::OK, "config is authz-exempt: {body}");
    let id: String = sqlx::query_scalar("SELECT id FROM principals WHERE subject = $1")
        .bind(&sub)
        .fetch_one(&ctx.pool)
        .await
        .expect("JIT-provisioned principal row");
    (token, id)
}

fn results(body: &Value) -> &Vec<Value> {
    body["results"].as_array().expect("results array")
}

#[tokio::test]
async fn column_name_search_finds_the_table_end_to_end() {
    let Some(ctx) = oidc_ctx().await else {
        return;
    };

    // The load-bearing case: a query for a column name finds the table
    // whose schema (written through by createTable) contains it.
    let (status, body) = send(
        &ctx.router,
        "GET",
        &format!("/api/v2/search?q={}", ctx.column),
        Some(&ctx.admin_token),
        None,
    )
    .await;
    assert_eq!(status, StatusCode::OK, "{body}");
    let hits = results(&body);
    assert_eq!(hits.len(), 1, "{body}");
    assert_eq!(hits[0]["type"], json!("table"));
    assert_eq!(hits[0]["name"], json!(ctx.table));
    assert_eq!(hits[0]["warehouse"], json!(ctx.warehouse));
    assert_eq!(hits[0]["namespace"], json!([format!("ns-{}", ctx.salt)]));
    assert!(hits[0]["rank"].as_f64().is_some_and(|r| r > 0.0), "{body}");
    assert!(
        hits[0]["snippet"]
            .as_str()
            .is_some_and(|s| s.contains("**")),
        "snippet must highlight: {body}"
    );

    // The warehouse filter accepts the warehouse; an unknown one is 404.
    let (status, body) = send(
        &ctx.router,
        "GET",
        &format!(
            "/api/v2/search?q={}&warehouse={}",
            ctx.column, ctx.warehouse
        ),
        Some(&ctx.admin_token),
        None,
    )
    .await;
    assert_eq!(status, StatusCode::OK, "{body}");
    assert_eq!(results(&body).len(), 1, "{body}");
    let (status, body) = send(
        &ctx.router,
        "GET",
        &format!("/api/v2/search?q={}&warehouse=no-such-wh", ctx.column),
        Some(&ctx.admin_token),
        None,
    )
    .await;
    assert_eq!(status, StatusCode::NOT_FOUND, "{body}");
    assert_eq!(body["error"]["type"], json!("NoSuchWarehouseException"));
}

#[tokio::test]
async fn parameters_are_validated() {
    let Some(ctx) = oidc_ctx().await else {
        return;
    };

    for uri in [
        "/api/v2/search?q=%20".to_owned(),
        format!("/api/v2/search?q={}&limit=0", ctx.salt),
        format!("/api/v2/search?q={}&limit=101", ctx.salt),
        format!("/api/v2/search?q={}&type=rocket", ctx.salt),
        format!("/api/v2/search?q={}&page_token=garbage", ctx.salt),
        format!("/api/v2/search?q={}&namespace=a..b", ctx.salt),
    ] {
        let (status, body) = send(&ctx.router, "GET", &uri, Some(&ctx.admin_token), None).await;
        assert_eq!(status, StatusCode::BAD_REQUEST, "{uri}: {body}");
    }
}

#[tokio::test]
async fn results_are_filtered_to_the_principals_visibility() {
    let Some(ctx) = oidc_ctx().await else {
        return;
    };
    let (token, principal_id) = provision_user(&ctx).await;

    // Ungranted: 200 with nothing, not 403 — search reveals nothing.
    let uri = format!("/api/v2/search?q={}", ctx.salt);
    let (status, body) = send(&ctx.router, "GET", &uri, Some(&token), None).await;
    assert_eq!(status, StatusCode::OK, "{body}");
    assert!(results(&body).is_empty(), "deny by default: {body}");

    // READ on the table: exactly the table appears, no namespaces.
    let (status, body) = send(
        &ctx.router,
        "POST",
        "/api/v2/grants",
        Some(&ctx.admin_token),
        Some(&json!({
            "privilege": "READ",
            "principal_id": principal_id,
            "securable": {
                "type": "table",
                "warehouse": ctx.warehouse,
                "namespace": [format!("ns-{}", ctx.salt)],
                "table": ctx.table,
            },
        })),
    )
    .await;
    assert_eq!(status, StatusCode::CREATED, "grant READ: {body}");
    let (status, body) = send(&ctx.router, "GET", &uri, Some(&token), None).await;
    assert_eq!(status, StatusCode::OK, "{body}");
    let hits = results(&body);
    assert_eq!(hits.len(), 1, "only the granted table: {body}");
    assert_eq!(hits[0]["name"], json!(ctx.table));

    // The admin sees the namespace too.
    let (status, body) = send(&ctx.router, "GET", &uri, Some(&ctx.admin_token), None).await;
    assert_eq!(status, StatusCode::OK, "{body}");
    assert!(
        results(&body)
            .iter()
            .any(|h| h["type"] == json!("namespace")),
        "admin sees namespaces: {body}"
    );
}

#[tokio::test]
async fn pagination_walks_pages_over_http() {
    let Some(ctx) = oidc_ctx().await else {
        return;
    };

    // Two more tables sharing the salt (plus the fixture table = 3 hits).
    for i in 0..2 {
        let (status, body) = send(
            &ctx.router,
            "POST",
            &format!("/v1/{}/namespaces/ns-{}/tables", ctx.warehouse, ctx.salt),
            Some(&ctx.admin_token),
            Some(&json!({
                "name": format!("extra{i}_{}", ctx.salt),
                "schema": {
                    "type": "struct",
                    "fields": [
                        { "id": 1, "name": "id", "required": true, "type": "long" },
                    ],
                },
            })),
        )
        .await;
        assert_eq!(status, StatusCode::OK, "create extra table: {body}");
    }

    let mut seen = Vec::new();
    let mut token: Option<String> = None;
    for _ in 0..10 {
        let uri = match &token {
            Some(t) => format!(
                "/api/v2/search?q={}&type=table&limit=2&page_token={t}",
                ctx.salt
            ),
            None => format!("/api/v2/search?q={}&type=table&limit=2", ctx.salt),
        };
        let (status, body) = send(&ctx.router, "GET", &uri, Some(&ctx.admin_token), None).await;
        assert_eq!(status, StatusCode::OK, "{body}");
        seen.extend(
            results(&body)
                .iter()
                .map(|h| h["id"].as_str().expect("id").to_owned()),
        );
        match body["next_page_token"].as_str() {
            Some(next) => token = Some(next.to_owned()),
            None => break,
        }
    }
    seen.sort();
    seen.dedup();
    assert_eq!(seen.len(), 3, "all tables exactly once: {seen:?}");
}

#[tokio::test]
async fn disabled_mode_searches_anonymously() {
    let Some((router, _pool)) = test_router(|_| {}).await else {
        return;
    };

    let salt = Ulid::new().to_string().to_lowercase();
    let root = tempfile::tempdir().expect("create tempdir");
    let warehouse = format!("wh-anon-search-{salt}");
    let storage_root = format!("file://{}", root.path().join("warehouse").display());
    let (status, body) = send(
        &router,
        "POST",
        "/api/v2/warehouses",
        None,
        Some(&json!({ "name": warehouse, "storage_root": storage_root })),
    )
    .await;
    assert_eq!(status, StatusCode::CREATED, "{body}");
    let (status, body) = send(
        &router,
        "POST",
        &format!("/v1/{warehouse}/namespaces"),
        None,
        Some(&json!({ "namespace": [format!("anon{salt}")] })),
    )
    .await;
    assert_eq!(status, StatusCode::OK, "{body}");

    let (status, body) = send(
        &router,
        "GET",
        &format!("/api/v2/search?q=anon{salt}"),
        None,
        None,
    )
    .await;
    assert_eq!(status, StatusCode::OK, "{body}");
    assert_eq!(results(&body).len(), 1, "{body}");
    assert_eq!(results(&body)[0]["type"], json!("namespace"));
}
