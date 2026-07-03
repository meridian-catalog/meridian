//! Remote-signing API tests (ADR 005): the
//! `POST .../tables/{table}/sign` endpoint end to end against `MinIO`
//! (sign → execute → 200), the authorization policy over HTTP (sibling
//! tables, method-vs-grant), the delegation advertisement matrix, and the
//! signing audit trail.
//!
//! Same conventions as `vending_api.rs`: Postgres via `DATABASE_URL` or
//! skip; tests that touch real objects also need the dev `MinIO`
//! (`localhost:9000`, long-lived `meridian-warehouse` bucket) and skip
//! when it is unreachable. The RBAC test mints tokens against the
//! in-process IdP from `tests/idp`.

// The shared IdP fixture exposes more than this binary uses.
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

const MINIO_ENDPOINT: &str = "http://localhost:9000";
const MINIO_BUCKET: &str = "meridian-warehouse";
const MINIO_ACCESS_KEY: &str = "meridian";
const MINIO_SECRET_KEY: &str = "meridian123";
const DELEGATION_HEADER: &str = "x-iceberg-access-delegation";

fn minio_reachable() -> bool {
    TcpStream::connect_timeout(
        &"127.0.0.1:9000".parse().expect("static addr"),
        Duration::from_millis(500),
    )
    .is_ok()
}

async fn test_router_with(configure: impl FnOnce(&mut AppConfig)) -> Option<(Router, PgPool)> {
    let Ok(url) = std::env::var("DATABASE_URL") else {
        eprintln!("skipping signing API test: DATABASE_URL is not set");
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

async fn test_router() -> Option<(Router, PgPool)> {
    test_router_with(|_| ()).await
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

fn simple_schema() -> Value {
    json!({
        "type": "struct",
        "fields": [
            { "id": 1, "name": "id", "required": true, "type": "long" },
        ],
    })
}

/// Creates a MinIO-backed warehouse with the given extra storage options.
async fn create_minio_warehouse(
    router: &Router,
    name: &str,
    extra: &[(&str, &str)],
    auth: &[(&str, &str)],
) {
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
        auth,
    )
    .await;
    assert_eq!(status, StatusCode::CREATED, "create warehouse: {raw}");
}

/// Creates `ns.{table}`; returns the create response body.
async fn create_table(
    router: &Router,
    warehouse: &str,
    table: &str,
    auth: &[(&str, &str)],
) -> Value {
    let (status, raw) = send(
        router,
        "POST",
        &format!("/v1/{warehouse}/namespaces"),
        Some(json!({ "namespace": ["ns"] })),
        auth,
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
        Some(json!({ "name": table, "schema": simple_schema() })),
        auth,
    )
    .await;
    assert_eq!(status, StatusCode::OK, "create table: {raw}");
    parse(&raw)
}

/// `s3://bucket/key` → path-style `http://localhost:9000/bucket/key`.
fn s3_to_http(s3_uri: &str) -> String {
    let rest = s3_uri.strip_prefix("s3://").expect("s3 uri");
    format!("{MINIO_ENDPOINT}/{rest}")
}

/// Signs one request through the endpoint; returns (status, body).
async fn sign(
    router: &Router,
    warehouse: &str,
    table: &str,
    method: &str,
    uri: &str,
    auth: &[(&str, &str)],
) -> (StatusCode, Value) {
    let (status, raw) = send(
        router,
        "POST",
        &format!("/v1/{warehouse}/namespaces/ns/tables/{table}/sign"),
        Some(json!({
            "region": "us-east-1",
            "method": method,
            "uri": uri,
            "headers": {},
        })),
        auth,
    )
    .await;
    (status, parse(&raw))
}

/// Executes a signed request against `MinIO` with the returned headers.
async fn execute_signed(method: &str, uri: &str, signed: &Value) -> reqwest::Response {
    let client = reqwest::Client::new();
    let mut request = client.request(
        method.parse().expect("method"),
        signed["uri"].as_str().unwrap_or(uri),
    );
    for (name, values) in signed["headers"].as_object().expect("headers map") {
        for value in values.as_array().expect("multi-valued") {
            request = request.header(name, value.as_str().expect("header value"));
        }
    }
    request.send().await.expect("execute signed request")
}

/// The full protocol round trip: sign a GET for a real object (the
/// table's own metadata.json) with warehouse-held credentials, execute it
/// credential-less, and read the object; then a signed PUT writes a new
/// object under the table prefix. Every decision lands in the audit log
/// with an outbox event.
#[tokio::test]
async fn sign_round_trip_executes_against_minio() {
    if !minio_reachable() {
        eprintln!("skipping: MinIO is not reachable on localhost:9000");
        return;
    }
    let Some((router, pool)) = test_router().await else {
        return;
    };
    let warehouse = format!("wh-sign-{}", Ulid::new()).to_lowercase();
    create_minio_warehouse(&router, &warehouse, &[("vending", "static")], &[]).await;
    let body = create_table(&router, &warehouse, "t", &[]).await;
    let metadata_location = body["metadata-location"]
        .as_str()
        .expect("metadata location");

    // GET the metadata object itself through a signed request.
    let object_url = s3_to_http(metadata_location);
    let (status, signed) = sign(&router, &warehouse, "t", "GET", &object_url, &[]).await;
    assert_eq!(status, StatusCode::OK, "sign GET: {signed}");
    assert!(
        signed["headers"]["authorization"][0]
            .as_str()
            .expect("authorization header")
            .starts_with("AWS4-HMAC-SHA256"),
        "{signed}"
    );
    let response = execute_signed("GET", &object_url, &signed).await;
    assert_eq!(response.status(), 200, "signed GET must succeed");
    let fetched = response.text().await.expect("body");
    assert!(
        fetched.contains("\"table-uuid\""),
        "metadata JSON: {fetched:.100}"
    );

    // An unsigned GET of the same object is refused by MinIO — the
    // signature was doing the work.
    let unsigned = reqwest::get(&object_url).await.expect("unsigned GET");
    assert_eq!(unsigned.status(), 403, "bucket must not be public");

    // A signed PUT writes a new object under the table prefix.
    let table_prefix = metadata_location
        .rsplit_once("/metadata/")
        .expect("metadata path")
        .0
        .to_owned();
    let put_url = s3_to_http(&format!("{table_prefix}/data/probe.bin"));
    let (status, signed) = sign(&router, &warehouse, "t", "PUT", &put_url, &[]).await;
    assert_eq!(status, StatusCode::OK, "sign PUT: {signed}");
    let response = execute_signed("PUT", &put_url, &signed).await;
    assert!(
        response.status() == 200 || response.status() == 201,
        "signed PUT: {}",
        response.status()
    );

    // Audit trail: one `credential.sign` row per decision, all allows,
    // with method, uri, keys, and decision recorded; outbox events match.
    let table_id: String = sqlx::query_scalar(
        "SELECT id FROM tables WHERE name = 't' AND namespace_id IN \
             (SELECT id FROM namespaces WHERE warehouse_id = \
                (SELECT id FROM warehouses WHERE name = $1))",
    )
    .bind(&warehouse)
    .fetch_one(&pool)
    .await
    .expect("table id");
    let resource = format!("table:{table_id}");
    let audits: Vec<(String, Value)> = sqlx::query_as(
        "SELECT principal, details FROM audit_log WHERE action = 'credential.sign' \
         AND resource = $1 ORDER BY seq",
    )
    .bind(&resource)
    .fetch_all(&pool)
    .await
    .expect("audit rows");
    assert_eq!(audits.len(), 2, "one audit row per decision: {audits:?}");
    assert_eq!(audits[0].0, "anonymous");
    assert_eq!(audits[0].1["decision"], "allow");
    assert_eq!(audits[0].1["action"], "get-object");
    assert_eq!(audits[0].1["method"], "GET");
    assert_eq!(audits[1].1["action"], "put-object");
    assert!(
        audits[1].1["keys"][0]
            .as_str()
            .expect("key")
            .ends_with("data/probe.bin")
    );
    let events: i64 = sqlx::query_scalar(
        "SELECT count(*) FROM events_outbox WHERE event_type = 'credential.signed' \
         AND aggregate = $1",
    )
    .bind(&resource)
    .fetch_one(&pool)
    .await
    .expect("outbox count");
    assert_eq!(events, 2, "one outbox event per allow");
}

/// The policy over HTTP: a sign attempt for a sibling table's object is a
/// 403 **and** an audited deny.
#[tokio::test]
async fn sign_denies_sibling_table_objects_and_audits_the_deny() {
    if !minio_reachable() {
        eprintln!("skipping: MinIO is not reachable on localhost:9000");
        return;
    }
    let Some((router, pool)) = test_router().await else {
        return;
    };
    let warehouse = format!("wh-sib-{}", Ulid::new()).to_lowercase();
    create_minio_warehouse(&router, &warehouse, &[("vending", "static")], &[]).await;
    let t1 = create_table(&router, &warehouse, "t1", &[]).await;
    let t2 = create_table(&router, &warehouse, "t2", &[]).await;
    let t2_object = s3_to_http(t2["metadata-location"].as_str().expect("t2 metadata"));

    // t1's sign endpoint refuses t2's object.
    let (status, body) = sign(&router, &warehouse, "t1", "GET", &t2_object, &[]).await;
    assert_eq!(status, StatusCode::FORBIDDEN, "sibling object: {body}");
    assert_eq!(body["error"]["type"], "ForbiddenException", "{body}");

    // ...and the deny is on the record, attributed to t1.
    let t1_prefix = t1["metadata-location"]
        .as_str()
        .expect("t1 metadata")
        .rsplit_once("/metadata/")
        .expect("metadata path")
        .0
        .to_owned();
    let denies: Vec<Value> = sqlx::query_scalar(
        "SELECT details FROM audit_log WHERE action = 'credential.sign' \
         AND details->>'decision' = 'deny' AND details->>'warehouse' = $1",
    )
    .bind(&warehouse)
    .fetch_all(&pool)
    .await
    .expect("deny audit rows");
    assert_eq!(denies.len(), 1, "{denies:?}");
    assert_eq!(denies[0]["table"], "ns.t1");
    assert!(
        denies[0]["reason"]
            .as_str()
            .expect("reason")
            .contains("outside the table prefix"),
        "{denies:?}"
    );
    assert!(!t1_prefix.is_empty());
    let deny_events: i64 = sqlx::query_scalar(
        "SELECT count(*) FROM events_outbox WHERE event_type = 'credential.sign-denied'",
    )
    .fetch_one(&pool)
    .await
    .expect("outbox count");
    assert!(deny_events >= 1, "deny outbox event");
}

/// The advertisement matrix on `loadTable`: `remote-signing` alone
/// switches config to the sign endpoint; `vended-credentials` keeps
/// precedence when both are listed; no header means neither; warehouses
/// without vending ignore the header; unknown mechanisms alone are 400.
#[tokio::test]
async fn delegation_header_matrix_controls_advertisement() {
    if !minio_reachable() {
        eprintln!("skipping: MinIO is not reachable on localhost:9000");
        return;
    }
    let Some((router, _pool)) = test_router().await else {
        return;
    };
    let warehouse = format!("wh-adv-{}", Ulid::new()).to_lowercase();
    create_minio_warehouse(&router, &warehouse, &[("vending", "static")], &[]).await;
    create_table(&router, &warehouse, "t", &[]).await;
    let uri = format!("/v1/{warehouse}/namespaces/ns/tables/t");

    // remote-signing alone: endpoint advertisement, no credentials.
    let (status, raw) = send(
        &router,
        "GET",
        &uri,
        None,
        &[(DELEGATION_HEADER, "remote-signing")],
    )
    .await;
    assert_eq!(status, StatusCode::OK, "{raw}");
    let body = parse(&raw);
    assert_eq!(body["config"]["s3.remote-signing-enabled"], "true");
    assert_eq!(
        body["config"]["s3.signer.endpoint"],
        format!("v1/{warehouse}/namespaces/ns/tables/t/sign")
    );
    assert_eq!(body["config"]["s3.signer"], "S3V4RestSigner");
    assert!(body.get("storage-credentials").is_none());
    assert!(body["config"].get("s3.secret-access-key").is_none());

    // Both listed: vended credentials win; no signer advertisement.
    let (status, raw) = send(
        &router,
        "GET",
        &uri,
        None,
        &[(DELEGATION_HEADER, "vended-credentials, remote-signing")],
    )
    .await;
    assert_eq!(status, StatusCode::OK, "{raw}");
    let body = parse(&raw);
    assert!(body["storage-credentials"].is_array());
    assert!(body["config"].get("s3.remote-signing-enabled").is_none());

    // No header: neither mechanism appears.
    let (status, raw) = send(&router, "GET", &uri, None, &[]).await;
    assert_eq!(status, StatusCode::OK, "{raw}");
    let body = parse(&raw);
    assert!(body["config"].get("s3.remote-signing-enabled").is_none());
    assert!(body.get("storage-credentials").is_none());

    // Unknown mechanisms alone: honest 400.
    let (status, raw) = send(
        &router,
        "GET",
        &uri,
        None,
        &[(DELEGATION_HEADER, "carrier-pigeon")],
    )
    .await;
    assert_eq!(status, StatusCode::BAD_REQUEST, "{raw}");

    // A warehouse without the vending opt-in ignores the header (pyiceberg
    // sends one by default) and its sign endpoint refuses honestly.
    let plain = format!("wh-plain-{}", Ulid::new()).to_lowercase();
    create_minio_warehouse(&router, &plain, &[], &[]).await;
    create_table(&router, &plain, "t", &[]).await;
    let plain_uri = format!("/v1/{plain}/namespaces/ns/tables/t");
    let (status, raw) = send(
        &router,
        "GET",
        &plain_uri,
        None,
        &[(DELEGATION_HEADER, "remote-signing")],
    )
    .await;
    assert_eq!(status, StatusCode::OK, "{raw}");
    assert!(
        parse(&raw)["config"]
            .get("s3.remote-signing-enabled")
            .is_none()
    );
    let (status, body) = sign(
        &router,
        &plain,
        "t",
        "GET",
        &format!("{MINIO_ENDPOINT}/{MINIO_BUCKET}/{plain}/ns/x"),
        &[],
    )
    .await;
    assert_eq!(status, StatusCode::BAD_REQUEST, "{body}");
    assert!(
        body["error"]["message"]
            .as_str()
            .expect("message")
            .contains("not enabled"),
        "{body}"
    );
}

/// RBAC drives the method policy: a READ-only principal gets GETs signed
/// but not PUTs; an ungranted principal gets a plain 403.
#[tokio::test]
async fn read_only_grants_sign_reads_but_not_writes() {
    if !minio_reachable() {
        eprintln!("skipping: MinIO is not reachable on localhost:9000");
        return;
    }
    let idp = TestIdp::start(&[KID1]).await;
    let issuer_url = idp.issuer.clone();
    let Some((router, pool)) = test_router_with(move |config| {
        config.auth.mode = AuthMode::Oidc;
        config.auth.oidc.require_https_issuers = false;
        config.auth.oidc.issuers.push(OidcIssuerConfig {
            issuer_url,
            audience: AUDIENCE.to_owned(),
            jwks_uri: None,
        });
    })
    .await
    else {
        return;
    };

    // Bootstrapped admin sets the stage.
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
    let admin_auth_value = format!("Bearer {admin_token}");
    let admin_auth: &[(&str, &str)] = &[("authorization", &admin_auth_value)];

    let warehouse = format!("wh-ro-{}", Ulid::new()).to_lowercase();
    create_minio_warehouse(&router, &warehouse, &[("vending", "static")], admin_auth).await;
    let body = create_table(&router, &warehouse, "t", admin_auth).await;
    let object_url = s3_to_http(
        body["metadata-location"]
            .as_str()
            .expect("metadata location"),
    );

    // JIT-provision a user (config is authz-exempt but authenticates).
    let user_sub = format!("user-{}", Ulid::new());
    let user_token = idp::mint(
        KID1,
        &idp.claims(
            &user_sub,
            json!({ "email": format!("{user_sub}@example.com") }),
        ),
    );
    let user_auth_value = format!("Bearer {user_token}");
    let user_auth: &[(&str, &str)] = &[("authorization", &user_auth_value)];
    let (status, _) = send(&router, "GET", "/v1/config", None, user_auth).await;
    assert_eq!(status, StatusCode::OK);
    let principal_id: String = sqlx::query_scalar("SELECT id FROM principals WHERE subject = $1")
        .bind(&user_sub)
        .fetch_one(&pool)
        .await
        .expect("JIT-provisioned principal");

    // Before any grant: plain 403 from RBAC.
    let (status, body) = sign(&router, &warehouse, "t", "GET", &object_url, user_auth).await;
    assert_eq!(status, StatusCode::FORBIDDEN, "ungranted: {body}");

    // Admin grants READ on the table.
    let (status, raw) = send(
        &router,
        "POST",
        "/api/v2/grants",
        Some(json!({
            "privilege": "READ",
            "principal_id": principal_id,
            "securable": {
                "type": "table",
                "warehouse": warehouse,
                "namespace": ["ns"],
                "table": "t",
            },
        })),
        admin_auth,
    )
    .await;
    assert_eq!(status, StatusCode::CREATED, "grant READ: {raw}");

    // GET signs and executes; PUT is refused by the method policy.
    let (status, signed) = sign(&router, &warehouse, "t", "GET", &object_url, user_auth).await;
    assert_eq!(status, StatusCode::OK, "read grant signs GET: {signed}");
    let response = execute_signed("GET", &object_url, &signed).await;
    assert_eq!(response.status(), 200);

    let (status, body) = sign(&router, &warehouse, "t", "PUT", &object_url, user_auth).await;
    assert_eq!(status, StatusCode::FORBIDDEN, "PUT with READ: {body}");
    assert!(
        body["error"]["message"]
            .as_str()
            .expect("message")
            .contains("READ only"),
        "{body}"
    );
}
