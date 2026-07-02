//! Storage-config passthrough tests: `LoadTableResult.config` /
//! `LoadViewResult.config` forward the warehouse's NON-SECRET storage
//! options under Iceberg client property names, and credential material
//! never appears in any `/v1` response body.
//!
//! These require a running Postgres and `DATABASE_URL`; without it they
//! skip. The denylist assertions ride on responses that perform no storage
//! I/O (`stage-create`), so they need no object store; the end-to-end load
//! assertions run against the dev `MinIO` (same conventions as
//! `meridian-storage/tests/storage_backends.rs`: long-lived
//! `meridian-warehouse` bucket, per-run prefix, skip — not fail — when
//! `MinIO` is unreachable on `localhost:9000`).

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
use tower::ServiceExt;
use ulid::Ulid;

const MINIO_ENDPOINT: &str = "http://localhost:9000";
const MINIO_BUCKET: &str = "meridian-warehouse";
const MINIO_ACCESS_KEY: &str = "meridian";
const MINIO_SECRET_KEY: &str = "meridian123";

/// Deliberately recognizable fake credentials: the assertions grep entire
/// response bodies for these strings.
const FAKE_ACCESS_KEY_ID: &str = "AKIAFAKEFAKEFAKEFAKE";
const FAKE_SECRET: &str = "FAKE-SECRET-VALUE-THAT-MUST-NEVER-LEAK";
const FAKE_SESSION_TOKEN: &str = "FAKE-SESSION-TOKEN-THAT-MUST-NEVER-LEAK";

fn minio_reachable() -> bool {
    TcpStream::connect_timeout(
        &"127.0.0.1:9000".parse().expect("static addr"),
        Duration::from_millis(500),
    )
    .is_ok()
}

async fn test_router() -> Option<Router> {
    let Ok(url) = std::env::var("DATABASE_URL") else {
        eprintln!("skipping storage-config test: DATABASE_URL is not set");
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
    Some(build_router(AppState {
        pool,
        config: Arc::new(config),
    }))
}

async fn send(
    router: &Router,
    method: &str,
    uri: &str,
    body: Option<Value>,
) -> (StatusCode, String) {
    let mut builder = Request::builder().method(method).uri(uri);
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

/// Every body must be free of the credential values — regardless of status.
fn assert_no_secrets(context: &str, raw: &str) {
    for secret in [FAKE_ACCESS_KEY_ID, FAKE_SECRET, FAKE_SESSION_TOKEN] {
        assert!(
            !raw.contains(secret),
            "{context}: credential value {secret:?} leaked into a response body: {raw}"
        );
    }
}

fn simple_schema() -> Value {
    json!({
        "type": "struct",
        "fields": [
            { "id": 1, "name": "id", "required": true, "type": "long" },
        ],
    })
}

/// The `config` of a load result must carry the mapped non-secret options
/// and no credential-shaped key at all.
fn assert_config_passthrough(context: &str, config: &Value, endpoint: &str) {
    assert_eq!(config["s3.endpoint"], endpoint, "{context}: {config}");
    assert_eq!(config["s3.region"], "us-east-1", "{context}: {config}");
    assert_eq!(config["client.region"], "us-east-1", "{context}: {config}");
    assert_eq!(
        config["s3.path-style-access"], "true",
        "{context}: {config}"
    );
    for key in config.as_object().expect("config object").keys() {
        assert!(
            !key.contains("access-key") && !key.contains("secret") && !key.contains("token"),
            "{context}: credential-shaped key {key:?} in config: {config}"
        );
    }
}

/// No object store needed: `stage-create` returns a `LoadTableResult`
/// (config included) without touching storage, so the passthrough and the
/// credential denylist can be asserted against a warehouse whose storage
/// endpoint does not even exist.
#[tokio::test]
async fn stage_create_response_vends_non_secret_config_and_never_credentials() {
    let Some(router) = test_router().await else {
        return;
    };
    let endpoint = "http://storage.invalid:9000";
    let warehouse = format!("wh-cfg-{}", Ulid::new().to_string().to_lowercase());
    let (status, raw) = send(
        &router,
        "POST",
        "/api/v2/warehouses",
        Some(json!({
            "name": warehouse,
            "storage_root": "s3://no-such-bucket/warehouse",
            "storage_options": {
                "endpoint": endpoint,
                "region": "us-east-1",
                "path-style": "true",
                "access-key-id": FAKE_ACCESS_KEY_ID,
                "secret-access-key": FAKE_SECRET,
                "session-token": FAKE_SESSION_TOKEN,
            },
        })),
    )
    .await;
    assert_eq!(status, StatusCode::CREATED, "create warehouse: {raw}");

    let (status, raw) = send(
        &router,
        "POST",
        &format!("/v1/{warehouse}/namespaces"),
        Some(json!({ "namespace": ["cfg"] })),
    )
    .await;
    assert_eq!(status, StatusCode::OK, "create namespace: {raw}");
    assert_no_secrets("create namespace", &raw);

    let (status, raw) = send(
        &router,
        "POST",
        &format!("/v1/{warehouse}/namespaces/cfg/tables"),
        Some(json!({
            "name": "staged",
            "schema": simple_schema(),
            "stage-create": true,
        })),
    )
    .await;
    assert_eq!(status, StatusCode::OK, "stage-create: {raw}");
    assert_no_secrets("stage-create", &raw);
    let body = parse(&raw);
    assert_config_passthrough("stage-create config", &body["config"], endpoint);

    // The management API necessarily echoes what an operator stored; the
    // *catalog* surface is what engines talk to, so sweep more /v1
    // responses — success and error envelopes alike — for the credential
    // values.
    for (method, uri) in [
        ("GET", format!("/v1/{warehouse}/namespaces/cfg")),
        ("GET", format!("/v1/{warehouse}/namespaces/cfg/tables")),
        ("GET", format!("/v1/{warehouse}/namespaces/cfg/views")),
        ("GET", format!("/v1/{warehouse}/namespaces/cfg/tables/none")),
        ("GET", format!("/v1/{warehouse}/namespaces/cfg/views/none")),
        ("GET", format!("/v1/config?warehouse={warehouse}")),
    ] {
        let (_, raw) = send(&router, method, &uri, None).await;
        assert_no_secrets(&uri, &raw);
    }
}

/// End-to-end against the dev `MinIO`: a real table create + load and a
/// real view create + load must all vend the warehouse's endpoint /
/// path-style / region config — and never the credentials.
#[tokio::test]
#[allow(clippy::too_many_lines)] // one test walks table + view load on real object storage
async fn minio_backed_load_responses_vend_storage_config() {
    let Some(router) = test_router().await else {
        return;
    };
    if !minio_reachable() {
        eprintln!(
            "SKIP: minio_backed_load_responses_vend_storage_config — MinIO not reachable \
             on localhost:9000; see crates/meridian-storage/tests/storage_backends.rs for setup"
        );
        return;
    }

    // A per-run prefix keeps runs isolated in the long-lived dev bucket.
    let run = Ulid::new().to_string().to_lowercase();
    let warehouse = format!("wh-minio-{run}");
    let (status, raw) = send(
        &router,
        "POST",
        "/api/v2/warehouses",
        Some(json!({
            "name": warehouse,
            "storage_root": format!("s3://{MINIO_BUCKET}/cfg-{run}"),
            "storage_options": {
                "endpoint": MINIO_ENDPOINT,
                "region": "us-east-1",
                "path-style": "true",
                "access-key-id": MINIO_ACCESS_KEY,
                "secret-access-key": MINIO_SECRET_KEY,
            },
        })),
    )
    .await;
    assert_eq!(status, StatusCode::CREATED, "create warehouse: {raw}");

    let (status, raw) = send(
        &router,
        "POST",
        &format!("/v1/{warehouse}/namespaces"),
        Some(json!({ "namespace": ["cfg"] })),
    )
    .await;
    assert_eq!(status, StatusCode::OK, "create namespace: {raw}");

    // Real table create + load: both responses vend the config.
    let (status, raw) = send(
        &router,
        "POST",
        &format!("/v1/{warehouse}/namespaces/cfg/tables"),
        Some(json!({ "name": "t", "schema": simple_schema() })),
    )
    .await;
    assert_eq!(status, StatusCode::OK, "create table on MinIO: {raw}");
    assert_config_passthrough(
        "create-table config",
        &parse(&raw)["config"],
        MINIO_ENDPOINT,
    );
    let (status, raw) = send(
        &router,
        "GET",
        &format!("/v1/{warehouse}/namespaces/cfg/tables/t"),
        None,
    )
    .await;
    assert_eq!(status, StatusCode::OK, "load table from MinIO: {raw}");
    let config = parse(&raw)["config"].clone();
    assert_config_passthrough("load-table config", &config, MINIO_ENDPOINT);
    assert!(
        !config
            .as_object()
            .expect("config object")
            .values()
            .any(|v| v.as_str() == Some(MINIO_SECRET_KEY) || v.as_str() == Some(MINIO_ACCESS_KEY)),
        "credential value leaked into load-table config: {config}"
    );

    // Real view create + load: LoadViewResult vends the same config.
    let (status, raw) = send(
        &router,
        "POST",
        &format!("/v1/{warehouse}/namespaces/cfg/views"),
        Some(json!({
            "name": "v",
            "schema": simple_schema(),
            "view-version": {
                "version-id": 1,
                "timestamp-ms": 1_700_000_000_000i64,
                "schema-id": 0,
                "summary": { "engine-name": "cfg-test" },
                "representations": [
                    { "type": "sql", "sql": "SELECT id FROM cfg.t", "dialect": "spark" },
                ],
                "default-namespace": ["cfg"],
            },
            "properties": {},
        })),
    )
    .await;
    assert_eq!(status, StatusCode::OK, "create view on MinIO: {raw}");
    assert_config_passthrough("create-view config", &parse(&raw)["config"], MINIO_ENDPOINT);
    let (status, raw) = send(
        &router,
        "GET",
        &format!("/v1/{warehouse}/namespaces/cfg/views/v"),
        None,
    )
    .await;
    assert_eq!(status, StatusCode::OK, "load view from MinIO: {raw}");
    assert_config_passthrough("load-view config", &parse(&raw)["config"], MINIO_ENDPOINT);
}
