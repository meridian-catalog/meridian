//! Credential-vending API tests: warehouse opt-in modes, the
//! `X-Iceberg-Access-Delegation` header, `loadCredentials`, external
//! endpoint advertisement, and the vend audit trail.
//!
//! These require a running Postgres and `DATABASE_URL`; without it they
//! skip. Tests that create real tables also need the dev `MinIO`
//! (`localhost:9000`, long-lived `meridian-warehouse` bucket) and skip when
//! it is unreachable — same conventions as `storage_config_api.rs`.

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
const DELEGATION_HEADER: &str = "x-iceberg-access-delegation";

fn minio_reachable() -> bool {
    TcpStream::connect_timeout(
        &"127.0.0.1:9000".parse().expect("static addr"),
        Duration::from_millis(500),
    )
    .is_ok()
}

async fn test_router() -> Option<(Router, PgPool)> {
    let Ok(url) = std::env::var("DATABASE_URL") else {
        eprintln!("skipping vending API test: DATABASE_URL is not set");
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

fn simple_schema() -> Value {
    json!({
        "type": "struct",
        "fields": [
            { "id": 1, "name": "id", "required": true, "type": "long" },
        ],
    })
}

/// Creates a MinIO-backed warehouse with the given extra storage options.
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

/// Creates `ns.{table}` in the warehouse; returns the create response body.
async fn create_table(
    router: &Router,
    warehouse: &str,
    table: &str,
    headers: &[(&str, &str)],
) -> Value {
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
        Some(json!({ "name": table, "schema": simple_schema() })),
        headers,
    )
    .await;
    assert_eq!(status, StatusCode::OK, "create table: {raw}");
    parse(&raw)
}

/// STS mode end to end: the header vends scoped session credentials on
/// create and load, `loadCredentials` answers the spec shape, plain loads
/// stay credential-free, and every vend is audited with an outbox event.
#[tokio::test]
#[allow(clippy::too_many_lines)] // one walk: create, load, loadCredentials, audit trail
async fn sts_vending_vends_scoped_credentials_and_audits_every_vend() {
    let Some((router, pool)) = test_router().await else {
        return;
    };
    if !minio_reachable() {
        eprintln!("SKIP: sts vending test — no MinIO on localhost:9000");
        return;
    }

    let run = Ulid::new().to_string().to_lowercase();
    let warehouse = format!("wh-vend-sts-{run}");
    create_minio_warehouse(
        &router,
        &warehouse,
        &[("vending", "sts"), ("vending.role-arn", ROLE_ARN)],
    )
    .await;

    // Create with the header: read-write credentials ride the response.
    let body = create_table(
        &router,
        &warehouse,
        "t",
        &[(DELEGATION_HEADER, "vended-credentials")],
    )
    .await;
    let config = &body["config"];
    for key in [
        "s3.access-key-id",
        "s3.secret-access-key",
        "s3.session-token",
        "s3.session-token-expires-at-ms",
    ] {
        assert!(
            config.get(key).is_some(),
            "create config missing {key}: {config}"
        );
    }
    // Session keys, never the warehouse's parent keys.
    assert_ne!(config["s3.access-key-id"], MINIO_ACCESS_KEY);
    assert_ne!(config["s3.secret-access-key"], MINIO_SECRET_KEY);
    let creds = body["storage-credentials"]
        .as_array()
        .expect("storage-credentials array");
    assert_eq!(creds.len(), 1);
    assert!(
        creds[0]["prefix"]
            .as_str()
            .expect("prefix")
            .starts_with(&format!("s3://{MINIO_BUCKET}/{warehouse}/ns/t-")),
        "prefix must be the table location: {}",
        creds[0]["prefix"]
    );
    assert_eq!(creds[0]["config"]["s3.endpoint"], MINIO_ENDPOINT);

    // Load with the header: same shape (anonymous principal passes every
    // RBAC check, so access is read-write; RBAC downgrade is unit-tested
    // against the authorize helper in oidc mode).
    let uri = format!("/v1/{warehouse}/namespaces/ns/tables/t");
    let (status, raw) = send(
        &router,
        "GET",
        &uri,
        None,
        &[(DELEGATION_HEADER, "vended-credentials,remote-signing")],
    )
    .await;
    assert_eq!(status, StatusCode::OK, "load with header: {raw}");
    let body = parse(&raw);
    assert!(body["config"].get("s3.session-token").is_some());
    assert!(body["storage-credentials"].is_array());

    // Load WITHOUT the header: no credential-shaped key anywhere.
    let (status, raw) = send(&router, "GET", &uri, None, &[]).await;
    assert_eq!(status, StatusCode::OK, "plain load: {raw}");
    let body = parse(&raw);
    assert!(body.get("storage-credentials").is_none());
    for key in body["config"].as_object().expect("config").keys() {
        assert!(
            !key.contains("access-key") && !key.contains("secret") && !key.contains("token"),
            "credential-shaped key {key:?} on a load without delegation"
        );
    }

    // remote-signing alone: honest 400.
    let (status, raw) = send(
        &router,
        "GET",
        &uri,
        None,
        &[(DELEGATION_HEADER, "remote-signing")],
    )
    .await;
    assert_eq!(status, StatusCode::BAD_REQUEST, "remote-signing: {raw}");
    assert!(raw.contains("not implemented"), "message: {raw}");

    // loadCredentials: the spec's LoadCredentialsResponse.
    let (status, raw) = send(&router, "GET", &format!("{uri}/credentials"), None, &[]).await;
    assert_eq!(status, StatusCode::OK, "loadCredentials: {raw}");
    let body = parse(&raw);
    let creds = body["storage-credentials"]
        .as_array()
        .expect("storage-credentials");
    assert_eq!(creds.len(), 1);
    assert!(creds[0]["config"].get("s3.session-token").is_some());

    // The audit trail is the product: one row + one outbox event per vend
    // (create, load-with-header, loadCredentials = 3), same details.
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
        "SELECT principal, details FROM audit_log WHERE action = 'credential.vend' \
         AND resource = $1 ORDER BY seq",
    )
    .bind(&resource)
    .fetch_all(&pool)
    .await
    .expect("audit rows");
    assert_eq!(audits.len(), 3, "one audit row per vend: {audits:?}");
    for (principal, details) in &audits {
        assert_eq!(principal, "anonymous");
        assert_eq!(details["mode"], "sts");
        assert_eq!(details["access"], "read-write");
        assert_eq!(details["warehouse"], Value::String(warehouse.clone()));
        assert_eq!(details["ttl_secs"], 3600);
        assert!(details["expires_at"].is_string());
    }
    let events: i64 = sqlx::query_scalar(
        "SELECT count(*) FROM events_outbox WHERE event_type = 'credential.vended' \
         AND aggregate = $1",
    )
    .bind(&resource)
    .fetch_one(&pool)
    .await
    .expect("outbox count");
    assert_eq!(events, 3, "one outbox event per vend");
}

/// Static mode: the warehouse's own keys pass through — but only with the
/// explicit opt-in, and only to clients that ask for delegation.
#[tokio::test]
async fn static_vending_passes_warehouse_keys_through_only_on_opt_in() {
    let Some((router, _pool)) = test_router().await else {
        return;
    };
    if !minio_reachable() {
        eprintln!("SKIP: static vending test — no MinIO on localhost:9000");
        return;
    }

    let run = Ulid::new().to_string().to_lowercase();

    // Opted in: the header vends the warehouse keys.
    let opted_in = format!("wh-vend-static-{run}");
    create_minio_warehouse(&router, &opted_in, &[("vending", "static")]).await;
    let body = create_table(
        &router,
        &opted_in,
        "t",
        &[(DELEGATION_HEADER, "vended-credentials")],
    )
    .await;
    assert_eq!(body["config"]["s3.access-key-id"], MINIO_ACCESS_KEY);
    assert_eq!(body["config"]["s3.secret-access-key"], MINIO_SECRET_KEY);
    let uri = format!("/v1/{opted_in}/namespaces/ns/tables/t/credentials");
    let (status, raw) = send(&router, "GET", &uri, None, &[]).await;
    assert_eq!(status, StatusCode::OK, "loadCredentials static: {raw}");
    assert_eq!(
        parse(&raw)["storage-credentials"][0]["config"]["s3.secret-access-key"],
        MINIO_SECRET_KEY
    );

    // NOT opted in: the same header changes nothing, and loadCredentials
    // refuses loudly instead of answering with an empty list.
    let opted_out = format!("wh-vend-none-{run}");
    create_minio_warehouse(&router, &opted_out, &[]).await;
    let body = create_table(
        &router,
        &opted_out,
        "t",
        &[(DELEGATION_HEADER, "vended-credentials")],
    )
    .await;
    assert!(body.get("storage-credentials").is_none());
    assert!(body["config"].get("s3.secret-access-key").is_none());
    let uri = format!("/v1/{opted_out}/namespaces/ns/tables/t/credentials");
    let (status, raw) = send(&router, "GET", &uri, None, &[]).await;
    assert_eq!(
        status,
        StatusCode::BAD_REQUEST,
        "loadCredentials none: {raw}"
    );
    assert!(raw.contains("not enabled"), "message: {raw}");
}

/// `endpoint.external` wins in everything a client sees — plain config
/// passthrough and vended credentials alike — while the server keeps using
/// the internal endpoint to reach storage.
#[tokio::test]
async fn external_endpoint_is_advertised_in_all_client_facing_config() {
    let Some((router, _pool)) = test_router().await else {
        return;
    };
    if !minio_reachable() {
        eprintln!("SKIP: external endpoint test — no MinIO on localhost:9000");
        return;
    }

    let external = "http://host.docker.internal:9000";
    let run = Ulid::new().to_string().to_lowercase();
    let warehouse = format!("wh-vend-ext-{run}");
    // The table create below only works because the server talks to
    // storage via the *internal* endpoint; the external one is not
    // reachable from here.
    create_minio_warehouse(
        &router,
        &warehouse,
        &[
            ("endpoint.external", external),
            ("vending", "sts"),
            ("vending.role-arn", ROLE_ARN),
        ],
    )
    .await;
    let body = create_table(
        &router,
        &warehouse,
        "t",
        &[(DELEGATION_HEADER, "vended-credentials")],
    )
    .await;
    assert_eq!(body["config"]["s3.endpoint"], external);
    assert_eq!(
        body["storage-credentials"][0]["config"]["s3.endpoint"],
        external
    );

    // Plain load (no vending header) advertises it too.
    let (status, raw) = send(
        &router,
        "GET",
        &format!("/v1/{warehouse}/namespaces/ns/tables/t"),
        None,
        &[],
    )
    .await;
    assert_eq!(status, StatusCode::OK, "plain load: {raw}");
    assert_eq!(parse(&raw)["config"]["s3.endpoint"], external);
}

/// Vending misconfiguration fails at warehouse create, not at first load.
#[tokio::test]
async fn warehouse_create_validates_vending_options() {
    let Some((router, _pool)) = test_router().await else {
        return;
    };
    let run = Ulid::new().to_string().to_lowercase();
    let cases: &[(&str, Value)] = &[
        ("sts without role-arn", json!({ "vending": "sts" })),
        ("unknown mode", json!({ "vending": "magic" })),
        (
            "typo'd vending key",
            json!({ "vending": "sts", "vending.roel-arn": "arn:aws:iam::1:role/r" }),
        ),
        (
            "orphan vending.* option",
            json!({ "vending.role-arn": "arn:aws:iam::1:role/r" }),
        ),
        (
            "out-of-range duration",
            json!({ "vending": "sts", "vending.role-arn": "arn", "vending.duration-secs": "10" }),
        ),
        (
            "static without keys",
            json!({ "vending": "static", "endpoint": "http://storage.invalid:9000" }),
        ),
        (
            "blank external endpoint",
            json!({ "endpoint.external": " " }),
        ),
    ];
    for (name, options) in cases {
        let (status, raw) = send(
            &router,
            "POST",
            "/api/v2/warehouses",
            Some(json!({
                "name": format!("wh-vend-bad-{run}"),
                "storage_root": format!("s3://{MINIO_BUCKET}/never"),
                "storage_options": options,
            })),
            &[],
        )
        .await;
        assert_eq!(status, StatusCode::BAD_REQUEST, "{name}: {raw}");
    }

    // Vending on a filesystem root is refused: nothing to scope there.
    let (status, raw) = send(
        &router,
        "POST",
        "/api/v2/warehouses",
        Some(json!({
            "name": format!("wh-vend-fs-{run}"),
            "storage_root": "/tmp/meridian-vend-fs",
            "storage_options": { "vending": "static" },
        })),
        &[],
    )
    .await;
    assert_eq!(status, StatusCode::BAD_REQUEST, "fs vending: {raw}");
    assert!(raw.contains("s3://"), "message names the constraint: {raw}");
}
