//! RBAC integration tests over the full router: deny-by-default in oidc
//! mode, 403 envelopes, role- and principal-grants driving real IRC
//! operations, admin bootstrap, the management API, and the unchanged
//! disabled-mode behavior.
//!
//! These require a running Postgres and `DATABASE_URL`; without it they
//! skip (with a note on stderr). Tokens are minted against the in-process
//! test IdP from `tests/idp`.

// The shared IdP fixture exposes more than this binary uses (key rotation
// helpers are exercised by tests/auth.rs).
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
        eprintln!("skipping RBAC integration test: DATABASE_URL is not set");
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

/// An oidc-mode test context: router, pool, a bootstrapped admin token,
/// and a warehouse rooted in a tempdir.
struct Ctx {
    router: Router,
    pool: PgPool,
    idp: TestIdp,
    admin_token: String,
    admin_sub: String,
    warehouse: String,
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

    // Bootstrap the first admin exactly the way `meridian serve` does
    // (idempotently, before serving).
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

    // The bootstrapped admin can create a warehouse: that IS the
    // "admin bootstrap works" assertion, exercised in every test here.
    let root = tempfile::tempdir().expect("create tempdir");
    let warehouse = format!("wh-rbac-{}", Ulid::new()).to_lowercase();
    let storage_root = format!("file://{}", root.path().join("warehouse").display());
    let (status, body, _) = send(
        &router,
        "POST",
        "/api/v2/warehouses",
        Some(&admin_token),
        Some(&json!({ "name": warehouse, "storage_root": storage_root })),
    )
    .await;
    assert_eq!(
        status,
        StatusCode::CREATED,
        "bootstrapped admin must be able to create a warehouse: {body}"
    );

    Some(Ctx {
        router,
        pool,
        idp,
        admin_token,
        admin_sub,
        warehouse,
        _root: root,
    })
}

/// Sends one request; returns (status, parsed JSON body, headers).
async fn send(
    router: &Router,
    method: &str,
    uri: &str,
    token: Option<&str>,
    body: Option<&Value>,
) -> (StatusCode, Value, axum::http::HeaderMap) {
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
    let headers = response.headers().clone();
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
    (status, value, headers)
}

fn assert_forbidden_envelope(status: StatusCode, body: &Value) {
    assert_eq!(status, StatusCode::FORBIDDEN, "{body}");
    assert_eq!(
        body["error"]["type"],
        json!("ForbiddenException"),
        "403s must use the IRC envelope with ForbiddenException: {body}"
    );
    assert_eq!(body["error"]["code"], json!(403), "{body}");
}

fn simple_schema() -> Value {
    json!({
        "type": "struct",
        "fields": [
            { "id": 1, "name": "id", "required": true, "type": "long" },
        ],
    })
}

/// Creates a namespace and a table in it with the admin token.
async fn make_namespace_and_table(ctx: &Ctx, ns: &str, table: &str) {
    let (status, body, _) = send(
        &ctx.router,
        "POST",
        &format!("/v1/{}/namespaces", ctx.warehouse),
        Some(&ctx.admin_token),
        Some(&json!({ "namespace": [ns] })),
    )
    .await;
    assert_eq!(status, StatusCode::OK, "create namespace: {body}");

    let (status, body, _) = send(
        &ctx.router,
        "POST",
        &format!("/v1/{}/namespaces/{ns}/tables", ctx.warehouse),
        Some(&ctx.admin_token),
        Some(&json!({ "name": table, "schema": simple_schema() })),
    )
    .await;
    assert_eq!(status, StatusCode::OK, "create table: {body}");
}

/// A minimal `CreateViewRequest` (one SQL representation).
fn create_view_body(name: &str, ns: &str) -> Value {
    json!({
        "name": name,
        "schema": simple_schema(),
        "view-version": {
            "version-id": 1,
            "timestamp-ms": 1_700_000_000_000i64,
            "schema-id": 0,
            "summary": { "engine-name": "rbac-tests" },
            "representations": [
                { "type": "sql",
                  "sql": format!("SELECT id FROM {ns}.events"),
                  "dialect": "spark" },
            ],
            "default-namespace": [ns],
        },
    })
}

/// Creates a namespace and a view in it with the admin token.
async fn make_namespace_and_view(ctx: &Ctx, ns: &str, view: &str) {
    let (status, body, _) = send(
        &ctx.router,
        "POST",
        &format!("/v1/{}/namespaces", ctx.warehouse),
        Some(&ctx.admin_token),
        Some(&json!({ "namespace": [ns] })),
    )
    .await;
    assert_eq!(status, StatusCode::OK, "create namespace: {body}");

    let (status, body, _) = send(
        &ctx.router,
        "POST",
        &format!("/v1/{}/namespaces/{ns}/views", ctx.warehouse),
        Some(&ctx.admin_token),
        Some(&create_view_body(view, ns)),
    )
    .await;
    assert_eq!(status, StatusCode::OK, "create view: {body}");
}

/// Creates a grant (admin token); `securable` is the selector JSON.
async fn grant_to_principal(ctx: &Ctx, privilege: &str, principal_id: &str, securable: &Value) {
    let (status, body, _) = send(
        &ctx.router,
        "POST",
        "/api/v2/grants",
        Some(&ctx.admin_token),
        Some(&json!({
            "privilege": privilege,
            "principal_id": principal_id,
            "securable": securable,
        })),
    )
    .await;
    assert_eq!(
        status,
        StatusCode::CREATED,
        "create {privilege} grant: {body}"
    );
}

fn view_url(ctx: &Ctx, ns: &str, name: &str) -> String {
    format!("/v1/{}/namespaces/{ns}/views/{name}", ctx.warehouse)
}

/// A replace request that sets one property (authorization is checked
/// before the updates are applied, so the body content only matters for
/// the allowed case).
fn replace_view_body() -> Value {
    json!({
        "requirements": [],
        "updates": [
            { "action": "set-properties", "updates": { "touched": "yes" } },
        ],
    })
}

/// Mints a token for a fresh user, provisions its principal row (one
/// authenticated request), and returns (token, subject, principal id).
async fn provision_user(ctx: &Ctx) -> (String, String, String) {
    let sub = format!("user-{}", Ulid::new());
    let token = idp::mint(
        KID1,
        &ctx.idp
            .claims(&sub, json!({ "email": format!("{sub}@example.com") })),
    );
    // /v1/config is authorization-exempt but still authenticates, which
    // JIT-provisions the principals row.
    let (status, body, _) = send(&ctx.router, "GET", "/v1/config", Some(&token), None).await;
    assert_eq!(
        status,
        StatusCode::OK,
        "config must be authz-exempt: {body}"
    );

    let id: String = sqlx::query_scalar("SELECT id FROM principals WHERE subject = $1")
        .bind(&sub)
        .fetch_one(&ctx.pool)
        .await
        .expect("JIT-provisioned principal row");
    (token, sub, id)
}

#[tokio::test]
async fn ungranted_principals_get_403_envelopes_everywhere() {
    let Some(ctx) = oidc_ctx().await else {
        return;
    };
    make_namespace_and_table(&ctx, "ns403", "t").await;
    let (token, _, _) = provision_user(&ctx).await;

    // IRC surface: reads and writes are both denied.
    let cases = [
        ("GET", format!("/v1/{}/namespaces", ctx.warehouse), None),
        (
            "POST",
            format!("/v1/{}/namespaces", ctx.warehouse),
            Some(json!({ "namespace": ["denied"] })),
        ),
        (
            "GET",
            format!("/v1/{}/namespaces/ns403/tables/t", ctx.warehouse),
            None,
        ),
        (
            "DELETE",
            format!("/v1/{}/namespaces/ns403/tables/t", ctx.warehouse),
            None,
        ),
    ];
    for (method, uri, body) in cases {
        let (status, response, _) =
            send(&ctx.router, method, &uri, Some(&token), body.as_ref()).await;
        assert_forbidden_envelope(status, &response);
    }

    // Management surface too.
    let (status, response, _) = send(
        &ctx.router,
        "POST",
        "/api/v2/warehouses",
        Some(&token),
        Some(&json!({ "name": "nope", "storage_root": "file:///tmp/nope" })),
    )
    .await;
    assert_forbidden_envelope(status, &response);
    let (status, response, _) =
        send(&ctx.router, "GET", "/api/v2/grants", Some(&token), None).await;
    assert_forbidden_envelope(status, &response);
}

#[tokio::test]
async fn table_read_via_role_allows_load_but_not_commit() {
    let Some(ctx) = oidc_ctx().await else {
        return;
    };
    make_namespace_and_table(&ctx, "nsread", "events").await;
    let (token, _, principal_id) = provision_user(&ctx).await;

    // Admin creates a role, grants READ on the table to it, binds the user.
    let role = format!("readers-{}", Ulid::new()).to_lowercase();
    let (status, body, _) = send(
        &ctx.router,
        "POST",
        "/api/v2/roles",
        Some(&ctx.admin_token),
        Some(&json!({ "name": role, "description": "read-only test role" })),
    )
    .await;
    assert_eq!(status, StatusCode::CREATED, "create role: {body}");

    let (status, body, _) = send(
        &ctx.router,
        "POST",
        "/api/v2/grants",
        Some(&ctx.admin_token),
        Some(&json!({
            "privilege": "READ",
            "role": role,
            "securable": {
                "type": "table",
                "warehouse": ctx.warehouse,
                "namespace": ["nsread"],
                "table": "events",
            },
        })),
    )
    .await;
    assert_eq!(status, StatusCode::CREATED, "create grant: {body}");

    let (status, body, _) = send(
        &ctx.router,
        "POST",
        &format!("/api/v2/roles/{role}/bindings"),
        Some(&ctx.admin_token),
        Some(&json!({ "principal_id": principal_id })),
    )
    .await;
    assert_eq!(status, StatusCode::NO_CONTENT, "bind: {body}");

    // Load works, commit does not.
    let table_url = format!("/v1/{}/namespaces/nsread/tables/events", ctx.warehouse);
    let (status, body, _) = send(&ctx.router, "GET", &table_url, Some(&token), None).await;
    assert_eq!(status, StatusCode::OK, "load with READ via role: {body}");
    assert!(body["metadata"]["table-uuid"].is_string(), "{body}");

    let commit_body = json!({ "requirements": [], "updates": [] });
    let (status, body, _) = send(
        &ctx.router,
        "POST",
        &table_url,
        Some(&token),
        Some(&commit_body),
    )
    .await;
    assert_forbidden_envelope(status, &body);

    // The effective-permissions endpoint reports the role-derived grant.
    let (status, body, _) = send(
        &ctx.router,
        "GET",
        &format!("/api/v2/permissions?principal={principal_id}"),
        Some(&ctx.admin_token),
        None,
    )
    .await;
    assert_eq!(status, StatusCode::OK, "{body}");
    let permissions = body["permissions"].as_array().expect("permissions array");
    assert!(
        permissions
            .iter()
            .any(|p| p["privilege"] == json!("READ") && p["via"] == json!(format!("role:{role}"))),
        "role-derived READ must be listed: {body}"
    );
    assert!(
        body["roles"]
            .as_array()
            .expect("roles")
            .contains(&json!(role)),
        "{body}"
    );
}

#[tokio::test]
async fn namespace_create_table_grant_allows_create() {
    let Some(ctx) = oidc_ctx().await else {
        return;
    };
    make_namespace_and_table(&ctx, "nsdev", "seed").await;
    let (token, _, principal_id) = provision_user(&ctx).await;

    // Direct CREATE_TABLE grant on the namespace.
    let (status, body, _) = send(
        &ctx.router,
        "POST",
        "/api/v2/grants",
        Some(&ctx.admin_token),
        Some(&json!({
            "privilege": "CREATE_TABLE",
            "principal_id": principal_id,
            "securable": {
                "type": "namespace",
                "warehouse": ctx.warehouse,
                "namespace": ["nsdev"],
            },
        })),
    )
    .await;
    assert_eq!(status, StatusCode::CREATED, "create grant: {body}");

    let (status, body, _) = send(
        &ctx.router,
        "POST",
        &format!("/v1/{}/namespaces/nsdev/tables", ctx.warehouse),
        Some(&token),
        Some(&json!({ "name": "made-by-grant", "schema": simple_schema() })),
    )
    .await;
    assert_eq!(status, StatusCode::OK, "create table with grant: {body}");

    // CREATE_TABLE does not imply LIST_TABLES.
    let (status, body, _) = send(
        &ctx.router,
        "GET",
        &format!("/v1/{}/namespaces/nsdev/tables", ctx.warehouse),
        Some(&token),
        None,
    )
    .await;
    assert_forbidden_envelope(status, &body);
}

#[tokio::test]
async fn grant_mutations_are_audited_under_the_real_principal() {
    let Some(ctx) = oidc_ctx().await else {
        return;
    };
    let (_, _, principal_id) = provision_user(&ctx).await;

    let (status, body, _) = send(
        &ctx.router,
        "POST",
        "/api/v2/grants",
        Some(&ctx.admin_token),
        Some(&json!({
            "privilege": "MANAGE_WAREHOUSE",
            "principal_id": principal_id,
            "securable": { "type": "warehouse", "warehouse": ctx.warehouse },
        })),
    )
    .await;
    assert_eq!(status, StatusCode::CREATED, "{body}");
    let grant_id = body["id"].as_str().expect("grant id").to_owned();
    assert_eq!(body["granted_by"], json!(format!("user:{}", ctx.admin_sub)));

    let (status, body, _) = send(
        &ctx.router,
        "DELETE",
        &format!("/api/v2/grants/{grant_id}"),
        Some(&ctx.admin_token),
        None,
    )
    .await;
    assert_eq!(status, StatusCode::NO_CONTENT, "{body}");

    let rows: Vec<(String, String)> =
        sqlx::query_as("SELECT action, principal FROM audit_log WHERE resource = $1 ORDER BY seq")
            .bind(format!("grant:{grant_id}"))
            .fetch_all(&ctx.pool)
            .await
            .expect("audit rows");
    assert_eq!(
        rows,
        vec![
            ("grant.create".to_owned(), format!("user:{}", ctx.admin_sub)),
            ("grant.delete".to_owned(), format!("user:{}", ctx.admin_sub)),
        ],
        "grant mutations must be audited under the authenticated principal"
    );
}

#[tokio::test]
async fn ungranted_principals_get_403_on_every_view_endpoint() {
    let Some(ctx) = oidc_ctx().await else {
        return;
    };
    make_namespace_and_view(&ctx, "nsv403", "v").await;
    let (token, _, _) = provision_user(&ctx).await;

    let cases = [
        (
            "GET",
            format!("/v1/{}/namespaces/nsv403/views", ctx.warehouse),
            None,
        ),
        (
            "POST",
            format!("/v1/{}/namespaces/nsv403/views", ctx.warehouse),
            Some(create_view_body("denied", "nsv403")),
        ),
        ("GET", view_url(&ctx, "nsv403", "v"), None),
        (
            "POST",
            view_url(&ctx, "nsv403", "v"),
            Some(replace_view_body()),
        ),
        ("DELETE", view_url(&ctx, "nsv403", "v"), None),
        (
            "POST",
            format!("/v1/{}/views/rename", ctx.warehouse),
            Some(json!({
                "source": { "namespace": ["nsv403"], "name": "v" },
                "destination": { "namespace": ["nsv403"], "name": "w" },
            })),
        ),
    ];
    for (method, uri, body) in cases {
        let (status, response, _) =
            send(&ctx.router, method, &uri, Some(&token), body.as_ref()).await;
        assert_forbidden_envelope(status, &response);
    }

    // HEAD carries no meaningful body; the status is the assertion.
    let (status, _, _) = send(
        &ctx.router,
        "HEAD",
        &view_url(&ctx, "nsv403", "v"),
        Some(&token),
        None,
    )
    .await;
    assert_eq!(status, StatusCode::FORBIDDEN);

    // Nothing was mutated: the view still loads for the admin, unchanged.
    let (status, body, _) = send(
        &ctx.router,
        "GET",
        &view_url(&ctx, "nsv403", "v"),
        Some(&ctx.admin_token),
        None,
    )
    .await;
    assert_eq!(status, StatusCode::OK, "{body}");
    assert!(
        body["metadata"]["properties"].get("touched").is_none(),
        "the denied replace must not have applied: {body}"
    );
}

#[tokio::test]
async fn view_read_grant_allows_load_but_not_replace() {
    let Some(ctx) = oidc_ctx().await else {
        return;
    };
    make_namespace_and_view(&ctx, "nsvread", "metrics").await;
    // A second view in the same namespace: the view-scoped grant must not
    // cover it.
    let (status, body, _) = send(
        &ctx.router,
        "POST",
        &format!("/v1/{}/namespaces/nsvread/views", ctx.warehouse),
        Some(&ctx.admin_token),
        Some(&create_view_body("other", "nsvread")),
    )
    .await;
    assert_eq!(status, StatusCode::OK, "create second view: {body}");
    let (token, _, principal_id) = provision_user(&ctx).await;

    let securable = json!({
        "type": "view",
        "warehouse": ctx.warehouse,
        "namespace": ["nsvread"],
        "view": "metrics",
    });
    grant_to_principal(&ctx, "READ", &principal_id, &securable).await;

    // Load and exists work on the granted view only.
    let (status, body, _) = send(
        &ctx.router,
        "GET",
        &view_url(&ctx, "nsvread", "metrics"),
        Some(&token),
        None,
    )
    .await;
    assert_eq!(status, StatusCode::OK, "load with READ on view: {body}");
    assert!(body["metadata"]["view-uuid"].is_string(), "{body}");
    let (status, _, _) = send(
        &ctx.router,
        "HEAD",
        &view_url(&ctx, "nsvread", "metrics"),
        Some(&token),
        None,
    )
    .await;
    assert_eq!(status, StatusCode::NO_CONTENT);
    let (status, body, _) = send(
        &ctx.router,
        "GET",
        &view_url(&ctx, "nsvread", "other"),
        Some(&token),
        None,
    )
    .await;
    assert_forbidden_envelope(status, &body);

    // READ does not allow replace...
    let (status, body, _) = send(
        &ctx.router,
        "POST",
        &view_url(&ctx, "nsvread", "metrics"),
        Some(&token),
        Some(&replace_view_body()),
    )
    .await;
    assert_forbidden_envelope(status, &body);

    // ...COMMIT on the view does.
    grant_to_principal(&ctx, "COMMIT", &principal_id, &securable).await;
    let (status, body, _) = send(
        &ctx.router,
        "POST",
        &view_url(&ctx, "nsvread", "metrics"),
        Some(&token),
        Some(&replace_view_body()),
    )
    .await;
    assert_eq!(
        status,
        StatusCode::OK,
        "replace with COMMIT on view: {body}"
    );
    assert_eq!(body["metadata"]["properties"]["touched"], json!("yes"));

    // The view grant shows up in effective permissions.
    let (status, body, _) = send(
        &ctx.router,
        "GET",
        &format!("/api/v2/permissions?principal={principal_id}"),
        Some(&ctx.admin_token),
        None,
    )
    .await;
    assert_eq!(status, StatusCode::OK, "{body}");
    let permissions = body["permissions"].as_array().expect("permissions array");
    assert!(
        permissions
            .iter()
            .any(|p| p["privilege"] == json!("READ") && p["securable_type"] == json!("view")),
        "the direct view READ grant must be listed: {body}"
    );
}

#[tokio::test]
async fn namespace_create_view_grant_allows_create_and_drop_needs_drop() {
    let Some(ctx) = oidc_ctx().await else {
        return;
    };
    make_namespace_and_view(&ctx, "nsvdev", "seed").await;
    let (token, _, principal_id) = provision_user(&ctx).await;

    grant_to_principal(
        &ctx,
        "CREATE_VIEW",
        &principal_id,
        &json!({
            "type": "namespace",
            "warehouse": ctx.warehouse,
            "namespace": ["nsvdev"],
        }),
    )
    .await;

    let (status, body, _) = send(
        &ctx.router,
        "POST",
        &format!("/v1/{}/namespaces/nsvdev/views", ctx.warehouse),
        Some(&token),
        Some(&create_view_body("made-by-grant", "nsvdev")),
    )
    .await;
    assert_eq!(status, StatusCode::OK, "create view with grant: {body}");

    // CREATE_VIEW implies neither LIST_TABLES nor DROP.
    let (status, body, _) = send(
        &ctx.router,
        "GET",
        &format!("/v1/{}/namespaces/nsvdev/views", ctx.warehouse),
        Some(&token),
        None,
    )
    .await;
    assert_forbidden_envelope(status, &body);
    let (status, body, _) = send(
        &ctx.router,
        "DELETE",
        &view_url(&ctx, "nsvdev", "made-by-grant"),
        Some(&token),
        None,
    )
    .await;
    assert_forbidden_envelope(status, &body);

    // DROP on the view allows the drop.
    grant_to_principal(
        &ctx,
        "DROP",
        &principal_id,
        &json!({
            "type": "view",
            "warehouse": ctx.warehouse,
            "namespace": ["nsvdev"],
            "view": "made-by-grant",
        }),
    )
    .await;
    let (status, body, _) = send(
        &ctx.router,
        "DELETE",
        &view_url(&ctx, "nsvdev", "made-by-grant"),
        Some(&token),
        None,
    )
    .await;
    assert_eq!(
        status,
        StatusCode::NO_CONTENT,
        "drop with DROP grant: {body}"
    );
}

#[tokio::test]
async fn view_rename_needs_write_on_source_and_create_view_at_destination() {
    let Some(ctx) = oidc_ctx().await else {
        return;
    };
    make_namespace_and_view(&ctx, "nsvsrc", "mover").await;
    let (status, body, _) = send(
        &ctx.router,
        "POST",
        &format!("/v1/{}/namespaces", ctx.warehouse),
        Some(&ctx.admin_token),
        Some(&json!({ "namespace": ["nsvdst"] })),
    )
    .await;
    assert_eq!(
        status,
        StatusCode::OK,
        "create destination namespace: {body}"
    );
    let (token, _, principal_id) = provision_user(&ctx).await;

    grant_to_principal(
        &ctx,
        "WRITE",
        &principal_id,
        &json!({
            "type": "view",
            "warehouse": ctx.warehouse,
            "namespace": ["nsvsrc"],
            "view": "mover",
        }),
    )
    .await;

    // WRITE on the source alone is not enough.
    let rename_url = format!("/v1/{}/views/rename", ctx.warehouse);
    let rename_body = json!({
        "source": { "namespace": ["nsvsrc"], "name": "mover" },
        "destination": { "namespace": ["nsvdst"], "name": "moved" },
    });
    let (status, body, _) = send(
        &ctx.router,
        "POST",
        &rename_url,
        Some(&token),
        Some(&rename_body),
    )
    .await;
    assert_forbidden_envelope(status, &body);

    // WRITE (source) + CREATE_VIEW (destination namespace) is.
    grant_to_principal(
        &ctx,
        "CREATE_VIEW",
        &principal_id,
        &json!({
            "type": "namespace",
            "warehouse": ctx.warehouse,
            "namespace": ["nsvdst"],
        }),
    )
    .await;
    let (status, body, _) = send(
        &ctx.router,
        "POST",
        &rename_url,
        Some(&token),
        Some(&rename_body),
    )
    .await;
    assert_eq!(
        status,
        StatusCode::NO_CONTENT,
        "rename with both grants: {body}"
    );

    // The view really moved (checked as admin).
    let (status, body, _) = send(
        &ctx.router,
        "GET",
        &view_url(&ctx, "nsvdst", "moved"),
        Some(&ctx.admin_token),
        None,
    )
    .await;
    assert_eq!(status, StatusCode::OK, "moved view loads: {body}");
}

#[tokio::test]
async fn catalog_reader_covers_view_reads_but_not_mutations() {
    let Some(ctx) = oidc_ctx().await else {
        return;
    };
    make_namespace_and_view(&ctx, "nsvcr", "readable").await;
    let (token, _, principal_id) = provision_user(&ctx).await;

    let (status, body, _) = send(
        &ctx.router,
        "POST",
        "/api/v2/roles/catalog_reader/bindings",
        Some(&ctx.admin_token),
        Some(&json!({ "principal_id": principal_id })),
    )
    .await;
    assert_eq!(
        status,
        StatusCode::NO_CONTENT,
        "bind catalog_reader: {body}"
    );

    // Read-only surface: list and load work.
    let (status, body, _) = send(
        &ctx.router,
        "GET",
        &format!("/v1/{}/namespaces/nsvcr/views", ctx.warehouse),
        Some(&token),
        None,
    )
    .await;
    assert_eq!(status, StatusCode::OK, "catalog_reader lists views: {body}");
    let (status, body, _) = send(
        &ctx.router,
        "GET",
        &view_url(&ctx, "nsvcr", "readable"),
        Some(&token),
        None,
    )
    .await;
    assert_eq!(status, StatusCode::OK, "catalog_reader loads views: {body}");

    // Mutations stay denied.
    for (method, uri, body) in [
        (
            "POST",
            format!("/v1/{}/namespaces/nsvcr/views", ctx.warehouse),
            Some(create_view_body("nope", "nsvcr")),
        ),
        (
            "POST",
            view_url(&ctx, "nsvcr", "readable"),
            Some(replace_view_body()),
        ),
        ("DELETE", view_url(&ctx, "nsvcr", "readable"), None),
    ] {
        let (status, response, _) =
            send(&ctx.router, method, &uri, Some(&token), body.as_ref()).await;
        assert_forbidden_envelope(status, &response);
    }
}

#[tokio::test]
async fn principals_listing_requires_management() {
    let Some(ctx) = oidc_ctx().await else {
        return;
    };
    let (token, _, principal_id) = provision_user(&ctx).await;

    let (status, body, _) =
        send(&ctx.router, "GET", "/api/v2/principals", Some(&token), None).await;
    assert_forbidden_envelope(status, &body);

    let (status, body, _) = send(
        &ctx.router,
        "GET",
        "/api/v2/principals",
        Some(&ctx.admin_token),
        None,
    )
    .await;
    assert_eq!(status, StatusCode::OK, "{body}");
    assert!(
        body["principals"]
            .as_array()
            .expect("principals array")
            .iter()
            .any(|p| p["id"] == json!(principal_id)),
        "the admin listing must include the provisioned principal: {body}"
    );
}

#[tokio::test]
async fn disabled_mode_bypasses_authorization_entirely() {
    let Some((router, _pool)) = test_router(|_| {}).await else {
        return;
    };

    // Anonymous, tokenless requests can do everything, exactly as before
    // RBAC landed.
    let root = tempfile::tempdir().expect("create tempdir");
    let warehouse = format!("wh-anon-{}", Ulid::new()).to_lowercase();
    let storage_root = format!("file://{}", root.path().join("warehouse").display());
    let (status, body, _) = send(
        &router,
        "POST",
        "/api/v2/warehouses",
        None,
        Some(&json!({ "name": warehouse, "storage_root": storage_root })),
    )
    .await;
    assert_eq!(status, StatusCode::CREATED, "{body}");

    let (status, body, _) = send(
        &router,
        "POST",
        &format!("/v1/{warehouse}/namespaces"),
        None,
        Some(&json!({ "namespace": ["anon"] })),
    )
    .await;
    assert_eq!(status, StatusCode::OK, "{body}");

    let (status, body, _) = send(
        &router,
        "GET",
        &format!("/v1/{warehouse}/namespaces"),
        None,
        None,
    )
    .await;
    assert_eq!(status, StatusCode::OK, "{body}");

    // The view surface is open too (list, create, load).
    let (status, body, _) = send(
        &router,
        "POST",
        &format!("/v1/{warehouse}/namespaces/anon/views"),
        None,
        Some(&create_view_body("open_view", "anon")),
    )
    .await;
    assert_eq!(status, StatusCode::OK, "anonymous create view: {body}");
    let (status, body, _) = send(
        &router,
        "GET",
        &format!("/v1/{warehouse}/namespaces/anon/views/open_view"),
        None,
        None,
    )
    .await;
    assert_eq!(status, StatusCode::OK, "anonymous load view: {body}");
    let (status, body, _) = send(
        &router,
        "GET",
        &format!("/v1/{warehouse}/namespaces/anon/views"),
        None,
        None,
    )
    .await;
    assert_eq!(status, StatusCode::OK, "anonymous list views: {body}");

    // Principal listing is open in this mode as well.
    let (status, body, _) = send(&router, "GET", "/api/v2/principals", None, None).await;
    assert_eq!(status, StatusCode::OK, "anonymous principals list: {body}");

    // The RBAC management API is open too in this mode.
    let (status, body, _) = send(&router, "GET", "/api/v2/roles", None, None).await;
    assert_eq!(status, StatusCode::OK, "{body}");
    let roles = body["roles"].as_array().expect("roles");
    assert!(
        roles.iter().any(|r| r["name"] == json!("admin"))
            && roles.iter().any(|r| r["name"] == json!("catalog_reader")),
        "built-in roles must be seeded: {body}"
    );
}
