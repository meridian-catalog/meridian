//! Router-level integration tests for the Iceberg REST table surface and
//! the commit path.
//!
//! These require a running Postgres and `DATABASE_URL`; without it they skip
//! (with a note on stderr). Every test provisions its own uniquely-named
//! warehouse rooted in its own tempdir (`file://` storage), so tests are
//! isolated from each other and from previous runs, and can assert on real
//! metadata files.

use std::sync::Arc;

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

struct TestCtx {
    router: Router,
    pool: PgPool,
    _root: tempfile::TempDir,
    warehouse: String,
}

async fn test_ctx() -> Option<TestCtx> {
    let Ok(url) = std::env::var("DATABASE_URL") else {
        eprintln!("skipping table API test: DATABASE_URL is not set");
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

    let root = tempfile::tempdir().expect("create tempdir");
    let warehouse = format!("wh-{}", Ulid::new().to_string().to_lowercase());
    let storage_root = format!("file://{}", root.path().join("warehouse").display());
    let (status, body) = send(
        &router,
        "POST",
        "/api/v2/warehouses",
        None,
        Some(json!({ "name": warehouse, "storage_root": storage_root })),
    )
    .await;
    assert_eq!(status.1, StatusCode::CREATED, "create warehouse: {body}");

    Some(TestCtx {
        router,
        pool,
        _root: root,
        warehouse,
    })
}

/// Sends one request through the full middleware stack. `headers` are
/// (name, value) pairs. Returns ((etag, status), parsed JSON body).
async fn send_with_headers(
    router: &Router,
    method: &str,
    uri: &str,
    headers: &[(&str, &str)],
    body: Option<Value>,
) -> ((Option<String>, StatusCode), Value) {
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
    let etag = response
        .headers()
        .get("etag")
        .and_then(|v| v.to_str().ok())
        .map(str::to_owned);
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
    ((etag, status), value)
}

async fn send(
    router: &Router,
    method: &str,
    uri: &str,
    headers: Option<&[(&str, &str)]>,
    body: Option<Value>,
) -> ((Option<String>, StatusCode), Value) {
    send_with_headers(router, method, uri, headers.unwrap_or(&[]), body).await
}

fn assert_error(body: &Value, code: u16, error_type: &str) {
    assert_eq!(body["error"]["code"], code, "envelope: {body}");
    assert_eq!(body["error"]["type"], error_type, "envelope: {body}");
}

async fn make_namespace(ctx: &TestCtx, name: &str) {
    let (status, body) = send(
        &ctx.router,
        "POST",
        &format!("/v1/{}/namespaces", ctx.warehouse),
        None,
        Some(json!({ "namespace": [name] })),
    )
    .await;
    assert_eq!(status.1, StatusCode::OK, "create namespace: {body}");
}

fn simple_schema() -> Value {
    json!({
        "type": "struct",
        "fields": [
            { "id": 1, "name": "id", "required": true, "type": "long" },
            { "id": 2, "name": "payload", "required": false, "type": "string" },
        ],
    })
}

/// Creates a table and returns (metadata-location, table-uuid, etag).
async fn make_table(ctx: &TestCtx, ns: &str, name: &str) -> (String, String, String) {
    let (status, body) = send(
        &ctx.router,
        "POST",
        &format!("/v1/{}/namespaces/{ns}/tables", ctx.warehouse),
        None,
        Some(json!({ "name": name, "schema": simple_schema() })),
    )
    .await;
    assert_eq!(status.1, StatusCode::OK, "create table: {body}");
    let etag = status.0.expect("create response carries an ETag");
    (
        body["metadata-location"]
            .as_str()
            .expect("metadata-location")
            .to_owned(),
        body["metadata"]["table-uuid"]
            .as_str()
            .expect("table-uuid")
            .to_owned(),
        etag,
    )
}

fn table_url(ctx: &TestCtx, ns: &str, name: &str) -> String {
    format!("/v1/{}/namespaces/{ns}/tables/{name}", ctx.warehouse)
}

/// An engine-style append commit: pin the main ref to the parent we based
/// on, add a snapshot, move the ref.
fn append_commit_body(table_uuid: &str, parent: Option<i64>, snapshot_id: i64, seq: i64) -> Value {
    json!({
        "requirements": [
            { "type": "assert-table-uuid", "uuid": table_uuid },
            { "type": "assert-ref-snapshot-id", "ref": "main", "snapshot-id": parent },
        ],
        "updates": [
            { "action": "add-snapshot", "snapshot": {
                "snapshot-id": snapshot_id,
                "parent-snapshot-id": parent,
                "sequence-number": seq,
                "timestamp-ms": 1_700_000_000_000i64 + seq,
                "manifest-list": format!("file:///fake/snap-{snapshot_id}.avro"),
                "summary": { "operation": "append" },
                "schema-id": 0,
            }},
            { "action": "set-snapshot-ref", "ref-name": "main",
              "type": "branch", "snapshot-id": snapshot_id },
        ],
    })
}

// ---------------------------------------------------------------------------
// List
// ---------------------------------------------------------------------------

#[tokio::test]
async fn table_listing_paginates_and_404s_on_missing_namespace() {
    let Some(ctx) = test_ctx().await else { return };
    make_namespace(&ctx, "db").await;

    // Empty listing.
    let (status, body) = send(
        &ctx.router,
        "GET",
        &format!("/v1/{}/namespaces/db/tables", ctx.warehouse),
        None,
        None,
    )
    .await;
    assert_eq!(status.1, StatusCode::OK);
    assert_eq!(body["identifiers"], json!([]));
    assert!(body["next-page-token"].is_null());

    for name in ["t0", "t1", "t2"] {
        make_table(&ctx, "db", name).await;
    }

    // Unpaginated: everything, null token.
    let (status, body) = send(
        &ctx.router,
        "GET",
        &format!("/v1/{}/namespaces/db/tables", ctx.warehouse),
        None,
        None,
    )
    .await;
    assert_eq!(status.1, StatusCode::OK);
    assert_eq!(body["identifiers"].as_array().map(Vec::len), Some(3));
    assert_eq!(body["identifiers"][0]["namespace"], json!(["db"]));

    // pageSize=2 walks two pages: 2 + 1.
    let (status, body) = send(
        &ctx.router,
        "GET",
        &format!("/v1/{}/namespaces/db/tables?pageSize=2", ctx.warehouse),
        None,
        None,
    )
    .await;
    assert_eq!(status.1, StatusCode::OK);
    assert_eq!(body["identifiers"].as_array().map(Vec::len), Some(2));
    let token = body["next-page-token"].as_str().expect("token").to_owned();
    let (status, body) = send(
        &ctx.router,
        "GET",
        &format!(
            "/v1/{}/namespaces/db/tables?pageSize=2&pageToken={token}",
            ctx.warehouse
        ),
        None,
        None,
    )
    .await;
    assert_eq!(status.1, StatusCode::OK);
    assert_eq!(body["identifiers"].as_array().map(Vec::len), Some(1));
    assert_eq!(body["identifiers"][0]["name"], "t2");
    assert!(body["next-page-token"].is_null());

    // Missing namespace and warehouse: exact 404 types.
    let (status, body) = send(
        &ctx.router,
        "GET",
        &format!("/v1/{}/namespaces/ghost/tables", ctx.warehouse),
        None,
        None,
    )
    .await;
    assert_eq!(status.1, StatusCode::NOT_FOUND);
    assert_error(&body, 404, "NoSuchNamespaceException");
    let (status, body) = send(
        &ctx.router,
        "GET",
        "/v1/no-such-wh/namespaces/db/tables",
        None,
        None,
    )
    .await;
    assert_eq!(status.1, StatusCode::NOT_FOUND);
    assert_error(&body, 404, "NoSuchWarehouseException");
}

// ---------------------------------------------------------------------------
// Create
// ---------------------------------------------------------------------------

#[tokio::test]
#[allow(clippy::too_many_lines)] // one test walks the whole create surface
async fn create_table_writes_metadata_and_rejects_conflicts() {
    let Some(ctx) = test_ctx().await else { return };
    make_namespace(&ctx, "db").await;

    let (location, uuid, etag) = make_table(&ctx, "db", "events").await;
    assert!(location.contains("/metadata/00000-"), "{location}");
    assert!(location.ends_with(".metadata.json"), "{location}");
    assert!(
        location.contains(&format!("/db/events-{uuid}/")),
        "default location must be uuid-suffixed under the namespace path: {location}"
    );
    assert_eq!(etag, format!("\"{uuid}-g0\""));

    // The metadata file really exists and is a parseable v2 document.
    let path = location.strip_prefix("file://").expect("file location");
    let raw = std::fs::read_to_string(path).expect("metadata file exists");
    let parsed: Value = serde_json::from_str(&raw).expect("metadata is JSON");
    assert_eq!(parsed["format-version"], 2);
    assert_eq!(parsed["table-uuid"], uuid.as_str());

    // On both mounts the table is loadable.
    for base in ["/v1", "/iceberg/v1"] {
        let (status, body) = send(
            &ctx.router,
            "GET",
            &format!("{base}/{}/namespaces/db/tables/events", ctx.warehouse),
            None,
            None,
        )
        .await;
        assert_eq!(status.1, StatusCode::OK);
        assert_eq!(body["metadata-location"], location.as_str());
        assert_eq!(body["config"], json!({}));
    }

    // Duplicate name: 409 AlreadyExistsException.
    let (status, body) = send(
        &ctx.router,
        "POST",
        &format!("/v1/{}/namespaces/db/tables", ctx.warehouse),
        None,
        Some(json!({ "name": "events", "schema": simple_schema() })),
    )
    .await;
    assert_eq!(status.1, StatusCode::CONFLICT);
    assert_error(&body, 409, "AlreadyExistsException");

    // Missing namespace: 404 NoSuchNamespaceException.
    let (status, body) = send(
        &ctx.router,
        "POST",
        &format!("/v1/{}/namespaces/ghost/tables", ctx.warehouse),
        None,
        Some(json!({ "name": "t", "schema": simple_schema() })),
    )
    .await;
    assert_eq!(status.1, StatusCode::NOT_FOUND);
    assert_error(&body, 404, "NoSuchNamespaceException");

    // Invalid format-version property: 400.
    let (status, body) = send(
        &ctx.router,
        "POST",
        &format!("/v1/{}/namespaces/db/tables", ctx.warehouse),
        None,
        Some(json!({
            "name": "bad",
            "schema": simple_schema(),
            "properties": { "format-version": "9" },
        })),
    )
    .await;
    assert_eq!(status.1, StatusCode::BAD_REQUEST);
    assert_error(&body, 400, "BadRequestException");

    // Unknown field type in the schema: 400.
    let (status, body) = send(
        &ctx.router,
        "POST",
        &format!("/v1/{}/namespaces/db/tables", ctx.warehouse),
        None,
        Some(json!({
            "name": "bad2",
            "schema": { "type": "struct", "fields": [
                { "id": 1, "name": "x", "required": true, "type": "supermassive" },
            ]},
        })),
    )
    .await;
    assert_eq!(status.1, StatusCode::BAD_REQUEST);
    assert_error(&body, 400, "BadRequestException");

    // Client-supplied location and properties are honored.
    let explicit = format!(
        "{}/custom/spot",
        ctx.warehouse // placeholder replaced below
    );
    let _ = explicit; // location must live under the warehouse root:
    let (status, body) = send(
        &ctx.router,
        "GET",
        &format!("/v1/{}/namespaces/db/tables/events", ctx.warehouse),
        None,
        None,
    )
    .await;
    assert_eq!(status.1, StatusCode::OK);
    let root_prefix = body["metadata"]["location"]
        .as_str()
        .expect("location")
        .rsplit_once("/db/")
        .expect("under namespace path")
        .0
        .to_owned();
    let custom_location = format!("{root_prefix}/db/custom-spot");
    let (status, body) = send(
        &ctx.router,
        "POST",
        &format!("/v1/{}/namespaces/db/tables", ctx.warehouse),
        None,
        Some(json!({
            "name": "custom",
            "schema": simple_schema(),
            "location": custom_location,
            "properties": { "owner": "tests" },
        })),
    )
    .await;
    assert_eq!(status.1, StatusCode::OK, "{body}");
    assert_eq!(body["metadata"]["location"], custom_location.as_str());
    assert_eq!(body["metadata"]["properties"]["owner"], "tests");
}

/// Create-request field ids are provisional: the exact request shape
/// Flink's connector sends (0-based ids, previously rejected with
/// `field id 0 is not positive`) must succeed, with fresh 1-based ids
/// assigned server-side and spec/order sources remapped.
#[tokio::test]
#[allow(clippy::too_many_lines)] // one test walks the whole provisional-id surface
async fn create_table_treats_request_field_ids_as_provisional() {
    let Some(ctx) = test_ctx().await else { return };
    make_namespace(&ctx, "db").await;

    // The payload Flink 1.20 (iceberg-flink-runtime 1.11.0) sends for
    // CREATE TABLE events (id BIGINT, name STRING, `value` DOUBLE,
    // ts TIMESTAMP(6)) — provisional field ids start at 0.
    let (status, body) = send(
        &ctx.router,
        "POST",
        &format!("/v1/{}/namespaces/db/tables", ctx.warehouse),
        None,
        Some(json!({
            "name": "events",
            "schema": {
                "type": "struct",
                "schema-id": 0,
                "fields": [
                    { "id": 0, "name": "id",    "required": false, "type": "long" },
                    { "id": 1, "name": "name",  "required": false, "type": "string" },
                    { "id": 2, "name": "value", "required": false, "type": "double" },
                    { "id": 3, "name": "ts",    "required": false, "type": "timestamp" },
                ],
            },
            "stage-create": false,
            "properties": {},
        })),
    )
    .await;
    assert_eq!(status.1, StatusCode::OK, "Flink-shaped create: {body}");
    let fields = body["metadata"]["schemas"][0]["fields"]
        .as_array()
        .expect("schema fields");
    let ids: Vec<i64> = fields.iter().map(|f| f["id"].as_i64().unwrap()).collect();
    assert_eq!(ids, vec![1, 2, 3, 4], "fresh 1-based ids: {body}");
    assert_eq!(body["metadata"]["last-column-id"], 4);

    // A partitioned + sorted create with 0-based ids: sources are remapped
    // and the requested spec becomes the table's only spec, numbered 0
    // (reference behavior — no phantom empty spec).
    let (status, body) = send(
        &ctx.router,
        "POST",
        &format!("/v1/{}/namespaces/db/tables", ctx.warehouse),
        None,
        Some(json!({
            "name": "events_by_day",
            "schema": {
                "type": "struct",
                "fields": [
                    { "id": 0, "name": "id", "required": true, "type": "long" },
                    { "id": 1, "name": "ts", "required": true, "type": "timestamp" },
                ],
            },
            "partition-spec": {
                "spec-id": 0,
                "fields": [
                    { "source-id": 1, "field-id": 1000, "name": "ts_day", "transform": "day" },
                ],
            },
            "write-order": {
                "order-id": 1,
                "fields": [
                    { "transform": "identity", "source-id": 0,
                      "direction": "asc", "null-order": "nulls-first" },
                ],
            },
        })),
    )
    .await;
    assert_eq!(status.1, StatusCode::OK, "partitioned create: {body}");
    let specs = body["metadata"]["partition-specs"]
        .as_array()
        .expect("partition-specs");
    assert_eq!(specs.len(), 1, "exactly one spec: {body}");
    assert_eq!(specs[0]["spec-id"], 0);
    assert_eq!(body["metadata"]["default-spec-id"], 0);
    assert_eq!(
        specs[0]["fields"][0]["source-id"], 2,
        "ts is field 2 after fresh assignment: {body}"
    );
    assert_eq!(specs[0]["fields"][0]["field-id"], 1000);
    let orders = body["metadata"]["sort-orders"].as_array().expect("orders");
    let default_order = orders
        .iter()
        .find(|o| o["order-id"] == body["metadata"]["default-sort-order-id"])
        .expect("default sort order");
    assert_eq!(
        default_order["fields"][0]["source-id"], 1,
        "id is field 1 after fresh assignment: {body}"
    );

    // Genuinely broken requests still fail: a partition source no field
    // carries.
    let (status, body) = send(
        &ctx.router,
        "POST",
        &format!("/v1/{}/namespaces/db/tables", ctx.warehouse),
        None,
        Some(json!({
            "name": "broken",
            "schema": { "type": "struct", "fields": [
                { "id": 0, "name": "id", "required": true, "type": "long" },
            ]},
            "partition-spec": { "fields": [
                { "source-id": 99, "name": "x", "transform": "identity" },
            ]},
        })),
    )
    .await;
    assert_eq!(status.1, StatusCode::BAD_REQUEST);
    assert_error(&body, 400, "BadRequestException");

    // ... and duplicate sibling field names (unresolvable once ids are
    // reassigned).
    let (status, body) = send(
        &ctx.router,
        "POST",
        &format!("/v1/{}/namespaces/db/tables", ctx.warehouse),
        None,
        Some(json!({
            "name": "broken2",
            "schema": { "type": "struct", "fields": [
                { "id": 0, "name": "x", "required": true, "type": "long" },
                { "id": 1, "name": "x", "required": true, "type": "string" },
            ]},
        })),
    )
    .await;
    assert_eq!(status.1, StatusCode::BAD_REQUEST);
    assert_error(&body, 400, "BadRequestException");
}

// ---------------------------------------------------------------------------
// Load / HEAD / ETag
// ---------------------------------------------------------------------------

#[tokio::test]
async fn load_table_supports_etags_and_snapshot_modes() {
    let Some(ctx) = test_ctx().await else { return };
    make_namespace(&ctx, "db").await;
    let (_, uuid, etag) = make_table(&ctx, "db", "t").await;
    let url = table_url(&ctx, "db", "t");

    // Load returns the same ETag as create.
    let (status, _) = send(&ctx.router, "GET", &url, None, None).await;
    assert_eq!(status.1, StatusCode::OK);
    assert_eq!(status.0.as_deref(), Some(etag.as_str()));

    // If-None-Match with the current tag: 304, no body.
    let (status, body) = send(
        &ctx.router,
        "GET",
        &url,
        Some(&[("if-none-match", &etag)]),
        None,
    )
    .await;
    assert_eq!(status.1, StatusCode::NOT_MODIFIED);
    assert!(body.is_null(), "304 must have no body: {body}");

    // A weak-form tag matches too.
    let weak = format!("W/{etag}");
    let (status, _) = send(
        &ctx.router,
        "GET",
        &url,
        Some(&[("if-none-match", &weak)]),
        None,
    )
    .await;
    assert_eq!(status.1, StatusCode::NOT_MODIFIED);

    // After a commit the tag changes and the old one no longer matches.
    let (status, _) = send(
        &ctx.router,
        "POST",
        &url,
        None,
        Some(append_commit_body(&uuid, None, 1001, 1)),
    )
    .await;
    assert_eq!(status.1, StatusCode::OK);
    let (status, body) = send(
        &ctx.router,
        "GET",
        &url,
        Some(&[("if-none-match", &etag)]),
        None,
    )
    .await;
    assert_eq!(status.1, StatusCode::OK);
    let new_etag = status.0.expect("etag after commit");
    assert_eq!(new_etag, format!("\"{uuid}-g1\""));
    assert_eq!(body["metadata"]["current-snapshot-id"], 1001);

    // snapshots=refs is a distinct representation with a distinct ETag.
    let (status, body) = send(
        &ctx.router,
        "GET",
        &format!("{url}?snapshots=refs"),
        None,
        None,
    )
    .await;
    assert_eq!(status.1, StatusCode::OK);
    assert_ne!(status.0.as_deref(), Some(new_etag.as_str()));
    assert_eq!(
        body["metadata"]["snapshots"].as_array().map(Vec::len),
        Some(1)
    );
    let (status, body) = send(
        &ctx.router,
        "GET",
        &format!("{url}?snapshots=bogus"),
        None,
        None,
    )
    .await;
    assert_eq!(status.1, StatusCode::BAD_REQUEST);
    assert_error(&body, 400, "BadRequestException");

    // HEAD: 204 present, 404 absent (exact type).
    let (status, _) = send(&ctx.router, "HEAD", &url, None, None).await;
    assert_eq!(status.1, StatusCode::NO_CONTENT);
    let (status, _) = send(
        &ctx.router,
        "HEAD",
        &table_url(&ctx, "db", "ghost"),
        None,
        None,
    )
    .await;
    assert_eq!(status.1, StatusCode::NOT_FOUND);

    // GET missing table: 404 NoSuchTableException.
    let (status, body) = send(
        &ctx.router,
        "GET",
        &table_url(&ctx, "db", "ghost"),
        None,
        None,
    )
    .await;
    assert_eq!(status.1, StatusCode::NOT_FOUND);
    assert_error(&body, 404, "NoSuchTableException");
}

// ---------------------------------------------------------------------------
// Drop / purge
// ---------------------------------------------------------------------------

#[tokio::test]
async fn drop_table_deletes_pointer_and_purge_removes_metadata_files() {
    let Some(ctx) = test_ctx().await else { return };
    make_namespace(&ctx, "db").await;

    // Plain drop: pointer gone, files remain.
    let (location, _, _) = make_table(&ctx, "db", "keepfiles").await;
    let path = location
        .strip_prefix("file://")
        .expect("file path")
        .to_owned();
    let (status, _) = send(
        &ctx.router,
        "DELETE",
        &table_url(&ctx, "db", "keepfiles"),
        None,
        None,
    )
    .await;
    assert_eq!(status.1, StatusCode::NO_CONTENT);
    let (status, _) = send(
        &ctx.router,
        "GET",
        &table_url(&ctx, "db", "keepfiles"),
        None,
        None,
    )
    .await;
    assert_eq!(status.1, StatusCode::NOT_FOUND);
    assert!(
        std::path::Path::new(&path).exists(),
        "non-purge drop must leave metadata files in place"
    );

    // Purge drop: metadata files removed (best-effort) and purge event enqueued.
    let (location, uuid, _) = make_table(&ctx, "db", "purgeme").await;
    let path = location
        .strip_prefix("file://")
        .expect("file path")
        .to_owned();
    let (status, _) = send(
        &ctx.router,
        "DELETE",
        &format!("{}?purgeRequested=true", table_url(&ctx, "db", "purgeme")),
        None,
        None,
    )
    .await;
    assert_eq!(status.1, StatusCode::NO_CONTENT);
    assert!(
        !std::path::Path::new(&path).exists(),
        "purge must delete the metadata files"
    );
    // Scoped by table uuid: the shared test database retains events from
    // other runs.
    let purge_events: i64 = sqlx::query_scalar(
        "SELECT count(*) FROM events_outbox
         WHERE event_type = 'table.purge_requested' AND payload->>'table_uuid' = $1",
    )
    .bind(&uuid)
    .fetch_one(&ctx.pool)
    .await
    .expect("count purge events");
    assert_eq!(purge_events, 1, "purge must enqueue the outbox job");

    // Missing table: 404 NoSuchTableException.
    let (status, body) = send(
        &ctx.router,
        "DELETE",
        &table_url(&ctx, "db", "ghost"),
        None,
        None,
    )
    .await;
    assert_eq!(status.1, StatusCode::NOT_FOUND);
    assert_error(&body, 404, "NoSuchTableException");
}

// ---------------------------------------------------------------------------
// Rename
// ---------------------------------------------------------------------------

#[tokio::test]
async fn rename_moves_tables_within_and_across_namespaces() {
    let Some(ctx) = test_ctx().await else { return };
    make_namespace(&ctx, "src").await;
    make_namespace(&ctx, "dst").await;
    let (location, uuid, _) = make_table(&ctx, "src", "orig").await;
    let rename_url = format!("/v1/{}/tables/rename", ctx.warehouse);

    // Same-namespace rename.
    let (status, body) = send(
        &ctx.router,
        "POST",
        &rename_url,
        None,
        Some(json!({
            "source": { "namespace": ["src"], "name": "orig" },
            "destination": { "namespace": ["src"], "name": "renamed" },
        })),
    )
    .await;
    assert_eq!(status.1, StatusCode::NO_CONTENT, "{body}");

    // Cross-namespace move; identity (uuid, metadata) rides along.
    let (status, _) = send(
        &ctx.router,
        "POST",
        &rename_url,
        None,
        Some(json!({
            "source": { "namespace": ["src"], "name": "renamed" },
            "destination": { "namespace": ["dst"], "name": "moved" },
        })),
    )
    .await;
    assert_eq!(status.1, StatusCode::NO_CONTENT);
    let (status, body) = send(
        &ctx.router,
        "GET",
        &table_url(&ctx, "dst", "moved"),
        None,
        None,
    )
    .await;
    assert_eq!(status.1, StatusCode::OK);
    assert_eq!(body["metadata"]["table-uuid"], uuid.as_str());
    assert_eq!(body["metadata-location"], location.as_str());
    let (status, _) = send(
        &ctx.router,
        "GET",
        &table_url(&ctx, "src", "renamed"),
        None,
        None,
    )
    .await;
    assert_eq!(status.1, StatusCode::NOT_FOUND);

    // Missing source: 404 NoSuchTableException.
    let (status, body) = send(
        &ctx.router,
        "POST",
        &rename_url,
        None,
        Some(json!({
            "source": { "namespace": ["src"], "name": "ghost" },
            "destination": { "namespace": ["dst"], "name": "x" },
        })),
    )
    .await;
    assert_eq!(status.1, StatusCode::NOT_FOUND);
    assert_error(&body, 404, "NoSuchTableException");

    // Missing destination namespace: 404 NoSuchNamespaceException.
    let (status, body) = send(
        &ctx.router,
        "POST",
        &rename_url,
        None,
        Some(json!({
            "source": { "namespace": ["dst"], "name": "moved" },
            "destination": { "namespace": ["ghost"], "name": "x" },
        })),
    )
    .await;
    assert_eq!(status.1, StatusCode::NOT_FOUND);
    assert_error(&body, 404, "NoSuchNamespaceException");

    // Destination taken: 409 AlreadyExistsException.
    make_table(&ctx, "dst", "occupied").await;
    let (status, body) = send(
        &ctx.router,
        "POST",
        &rename_url,
        None,
        Some(json!({
            "source": { "namespace": ["dst"], "name": "moved" },
            "destination": { "namespace": ["dst"], "name": "occupied" },
        })),
    )
    .await;
    assert_eq!(status.1, StatusCode::CONFLICT);
    assert_error(&body, 409, "AlreadyExistsException");
}

// ---------------------------------------------------------------------------
// Register
// ---------------------------------------------------------------------------

#[tokio::test]
async fn register_adopts_existing_metadata() {
    let Some(ctx) = test_ctx().await else { return };
    make_namespace(&ctx, "db").await;
    make_namespace(&ctx, "other").await;
    let (location, uuid, _) = make_table(&ctx, "db", "donor").await;

    // Registering the donor's live file under a second name trips the
    // table-uuid uniqueness guard: one catalog, one table per UUID.
    let (status, body) = send(
        &ctx.router,
        "POST",
        &format!("/v1/{}/namespaces/other/register", ctx.warehouse),
        None,
        Some(json!({ "name": "twin", "metadata-location": location })),
    )
    .await;
    assert_eq!(status.1, StatusCode::CONFLICT, "{body}");
    assert_error(&body, 409, "AlreadyExistsException");
    // The diagnostics must name the real conflict (the donor's live UUID),
    // not falsely claim the requested name is taken.
    let message = body["error"]["message"].as_str().unwrap_or_default();
    assert!(
        message.contains("table-uuid") && message.contains(uuid.as_str()),
        "409 must identify the uuid conflict, got: {message}"
    );

    // Drop the donor (keeping files), then adopt it under a new identity.
    let (status, _) = send(
        &ctx.router,
        "DELETE",
        &table_url(&ctx, "db", "donor"),
        None,
        None,
    )
    .await;
    assert_eq!(status.1, StatusCode::NO_CONTENT);
    let (status, body) = send(
        &ctx.router,
        "POST",
        &format!("/v1/{}/namespaces/other/register", ctx.warehouse),
        None,
        Some(json!({ "name": "adopted", "metadata-location": location })),
    )
    .await;
    assert_eq!(status.1, StatusCode::OK, "{body}");
    assert_eq!(body["metadata-location"], location.as_str());
    assert_eq!(body["metadata"]["table-uuid"], uuid.as_str());
    let (status, _) = send(
        &ctx.router,
        "HEAD",
        &table_url(&ctx, "other", "adopted"),
        None,
        None,
    )
    .await;
    assert_eq!(status.1, StatusCode::NO_CONTENT);

    // Existing name: 409. Bogus location: 400. Missing namespace: 404.
    let (status, body) = send(
        &ctx.router,
        "POST",
        &format!("/v1/{}/namespaces/other/register", ctx.warehouse),
        None,
        Some(json!({ "name": "adopted", "metadata-location": location })),
    )
    .await;
    assert_eq!(status.1, StatusCode::CONFLICT);
    assert_error(&body, 409, "AlreadyExistsException");
    let (status, body) = send(
        &ctx.router,
        "POST",
        &format!("/v1/{}/namespaces/other/register", ctx.warehouse),
        None,
        Some(json!({ "name": "nofile", "metadata-location": "s3://elsewhere/m.json" })),
    )
    .await;
    assert_eq!(status.1, StatusCode::BAD_REQUEST, "{body}");
    let (status, body) = send(
        &ctx.router,
        "POST",
        &format!("/v1/{}/namespaces/ghost/register", ctx.warehouse),
        None,
        Some(json!({ "name": "x", "metadata-location": location })),
    )
    .await;
    assert_eq!(status.1, StatusCode::NOT_FOUND);
    assert_error(&body, 404, "NoSuchNamespaceException");

    // overwrite=true is an honest, explicit 400 until implemented.
    let (status, body) = send(
        &ctx.router,
        "POST",
        &format!("/v1/{}/namespaces/other/register", ctx.warehouse),
        None,
        Some(json!({ "name": "adopted", "metadata-location": location, "overwrite": true })),
    )
    .await;
    assert_eq!(status.1, StatusCode::BAD_REQUEST);
    assert_error(&body, 400, "BadRequestException");
}

// ---------------------------------------------------------------------------
// Metrics
// ---------------------------------------------------------------------------

#[tokio::test]
async fn metrics_reports_are_accepted_and_stored() {
    let Some(ctx) = test_ctx().await else { return };
    make_namespace(&ctx, "db").await;
    make_table(&ctx, "db", "t").await;

    let report = json!({
        "report-type": "scan-report",
        "table-name": "db.t",
        "snapshot-id": 1,
        "filter": { "type": "true" },
        "schema-id": 0,
        "projected-field-ids": [1],
        "projected-field-names": ["id"],
        "metrics": { "total-planning-duration": { "count": 1, "time-unit": "nanoseconds", "total-duration": 5 } },
    });
    let (status, body) = send(
        &ctx.router,
        "POST",
        &format!("{}/metrics", table_url(&ctx, "db", "t")),
        None,
        Some(report),
    )
    .await;
    assert_eq!(status.1, StatusCode::NO_CONTENT, "{body}");

    // Stored verbatim, with the report type extracted.
    let (report_type, stored): (Option<String>, Value) = sqlx::query_as(
        "SELECT report_type, report FROM metrics_reports WHERE table_ident = 'db.t'
         ORDER BY id DESC LIMIT 1",
    )
    .fetch_one(&ctx.pool)
    .await
    .expect("stored metrics report");
    assert_eq!(report_type.as_deref(), Some("scan-report"));
    assert_eq!(stored["table-name"], "db.t");

    // Missing table: 404 NoSuchTableException; non-object body: 400.
    let (status, body) = send(
        &ctx.router,
        "POST",
        &format!("{}/metrics", table_url(&ctx, "db", "ghost")),
        None,
        Some(json!({ "report-type": "scan-report" })),
    )
    .await;
    assert_eq!(status.1, StatusCode::NOT_FOUND);
    assert_error(&body, 404, "NoSuchTableException");
    let (status, body) = send(
        &ctx.router,
        "POST",
        &format!("{}/metrics", table_url(&ctx, "db", "t")),
        None,
        Some(json!(["not", "an", "object"])),
    )
    .await;
    assert_eq!(status.1, StatusCode::BAD_REQUEST);
    assert_error(&body, 400, "BadRequestException");
}

// ---------------------------------------------------------------------------
// Stage-create and the create transaction
// ---------------------------------------------------------------------------

#[tokio::test]
async fn stage_create_then_assert_create_commit_creates_the_table() {
    let Some(ctx) = test_ctx().await else { return };
    make_namespace(&ctx, "db").await;

    // Stage: metadata initialized and returned, nothing durable.
    let (status, staged) = send(
        &ctx.router,
        "POST",
        &format!("/v1/{}/namespaces/db/tables", ctx.warehouse),
        None,
        Some(json!({
            "name": "ctas",
            "schema": simple_schema(),
            "stage-create": true,
            "properties": { "origin": "ctas" },
        })),
    )
    .await;
    assert_eq!(status.1, StatusCode::OK, "{staged}");
    assert!(staged["metadata-location"].is_null());
    let staged_uuid = staged["metadata"]["table-uuid"]
        .as_str()
        .expect("uuid")
        .to_owned();
    let staged_location = staged["metadata"]["location"]
        .as_str()
        .expect("location")
        .to_owned();
    let (status, _) = send(
        &ctx.router,
        "HEAD",
        &table_url(&ctx, "db", "ctas"),
        None,
        None,
    )
    .await;
    assert_eq!(
        status.1,
        StatusCode::NOT_FOUND,
        "staged table must not exist"
    );

    // Finalize through the commit endpoint with assert-create + the full
    // update list (what engines send for a create transaction).
    let commit = json!({
        "requirements": [ { "type": "assert-create" } ],
        "updates": [
            { "action": "assign-uuid", "uuid": staged_uuid },
            { "action": "upgrade-format-version", "format-version": 2 },
            { "action": "add-schema", "schema": simple_schema() },
            { "action": "set-current-schema", "schema-id": -1 },
            { "action": "add-sort-order", "sort-order": { "order-id": 0, "fields": [] } },
            { "action": "set-default-sort-order", "sort-order-id": -1 },
            { "action": "set-location", "location": staged_location },
            { "action": "set-properties", "updates": { "origin": "ctas" } },
        ],
    });
    let (status, body) = send(
        &ctx.router,
        "POST",
        &table_url(&ctx, "db", "ctas"),
        None,
        Some(commit.clone()),
    )
    .await;
    assert_eq!(status.1, StatusCode::OK, "{body}");
    assert_eq!(body["metadata"]["table-uuid"], staged_uuid.as_str());
    assert_eq!(body["metadata"]["location"], staged_location.as_str());
    assert_eq!(body["metadata"]["properties"]["origin"], "ctas");
    assert_eq!(
        status.0.as_deref(),
        Some(format!("\"{staged_uuid}-g0\"").as_str())
    );

    // The table now exists; repeating the create commit fails assert-create
    // with 409 CommitFailedException.
    let (status, body) = send(
        &ctx.router,
        "POST",
        &table_url(&ctx, "db", "ctas"),
        None,
        Some(commit),
    )
    .await;
    assert_eq!(status.1, StatusCode::CONFLICT);
    assert_error(&body, 409, "CommitFailedException");

    // A commit against a missing table without assert-create is a plain 404.
    let (status, body) = send(
        &ctx.router,
        "POST",
        &table_url(&ctx, "db", "ghost"),
        None,
        Some(json!({ "requirements": [], "updates": [] })),
    )
    .await;
    assert_eq!(status.1, StatusCode::NOT_FOUND);
    assert_error(&body, 404, "NoSuchTableException");
}

// ---------------------------------------------------------------------------
// The commit path: sequential, failures, idempotency
// ---------------------------------------------------------------------------

#[tokio::test]
async fn sequential_commits_advance_generation_and_grow_the_metadata_log() {
    let Some(ctx) = test_ctx().await else { return };
    make_namespace(&ctx, "db").await;
    let (_, uuid, _) = make_table(&ctx, "db", "t").await;
    let url = table_url(&ctx, "db", "t");

    let mut parent: Option<i64> = None;
    for seq in 1..=3i64 {
        let snapshot_id = 2000 + seq;
        let (status, body) = send(
            &ctx.router,
            "POST",
            &url,
            None,
            Some(append_commit_body(&uuid, parent, snapshot_id, seq)),
        )
        .await;
        assert_eq!(status.1, StatusCode::OK, "commit {seq}: {body}");
        assert_eq!(
            status.0.as_deref(),
            Some(format!("\"{uuid}-g{seq}\"").as_str()),
            "generation must advance by exactly one per commit"
        );
        assert!(
            body["metadata-location"]
                .as_str()
                .is_some_and(|l| l.contains(&format!("/metadata/{seq:05}-"))),
            "metadata file version tracks the generation: {body}"
        );
        assert_eq!(
            body["metadata"]["metadata-log"].as_array().map(Vec::len),
            Some(usize::try_from(seq).expect("small")),
            "metadata-log grows by one entry per commit"
        );
        parent = Some(snapshot_id);
    }

    let (status, body) = send(&ctx.router, "GET", &url, None, None).await;
    assert_eq!(status.1, StatusCode::OK);
    assert_eq!(
        body["metadata"]["snapshots"].as_array().map(Vec::len),
        Some(3)
    );
    assert_eq!(body["metadata"]["last-sequence-number"], 3);
    assert_eq!(body["metadata"]["current-snapshot-id"], 2003);

    // The snapshot write-through index tracks the retained set.
    let (indexed, current): (i64, i64) = sqlx::query_as(
        "SELECT count(*), count(*) FILTER (WHERE is_current)
         FROM table_snapshots ts JOIN tables t ON t.id = ts.table_id
         WHERE t.table_uuid = $1",
    )
    .bind(&uuid)
    .fetch_one(&ctx.pool)
    .await
    .expect("snapshot index");
    assert_eq!((indexed, current), (3, 1));
}

#[tokio::test]
async fn commit_requirement_failures_are_409_and_apply_nothing() {
    let Some(ctx) = test_ctx().await else { return };
    make_namespace(&ctx, "db").await;
    let (location, uuid, _) = make_table(&ctx, "db", "t").await;
    let url = table_url(&ctx, "db", "t");

    // Wrong table UUID.
    let bogus_uuid = "00000000-0000-4000-8000-000000000000";
    let (status, body) = send(
        &ctx.router,
        "POST",
        &url,
        None,
        Some(append_commit_body(bogus_uuid, None, 3001, 1)),
    )
    .await;
    assert_eq!(status.1, StatusCode::CONFLICT);
    assert_error(&body, 409, "CommitFailedException");

    // Stale ref pin (claims a snapshot that is not there).
    let (status, body) = send(
        &ctx.router,
        "POST",
        &url,
        None,
        Some(append_commit_body(&uuid, Some(999), 3002, 1)),
    )
    .await;
    assert_eq!(status.1, StatusCode::CONFLICT);
    assert_error(&body, 409, "CommitFailedException");

    // Unknown update action: 400 per the spec.
    let (status, body) = send(
        &ctx.router,
        "POST",
        &url,
        None,
        Some(json!({
            "requirements": [],
            "updates": [ { "action": "warp-reality" } ],
        })),
    )
    .await;
    assert_eq!(status.1, StatusCode::BAD_REQUEST);
    assert_error(&body, 400, "BadRequestException");

    // Nothing applied by any of the failures.
    let (status, body) = send(&ctx.router, "GET", &url, None, None).await;
    assert_eq!(status.1, StatusCode::OK);
    assert_eq!(status.0.as_deref(), Some(format!("\"{uuid}-g0\"").as_str()));
    assert_eq!(body["metadata-location"], location.as_str());
    assert!(
        body["metadata"]["snapshots"]
            .as_array()
            .is_none_or(Vec::is_empty)
    );
}

#[tokio::test]
async fn idempotent_commit_replay_returns_identical_response_without_reapplying() {
    let Some(ctx) = test_ctx().await else { return };
    make_namespace(&ctx, "db").await;
    let (_, uuid, _) = make_table(&ctx, "db", "t").await;
    let url = table_url(&ctx, "db", "t");
    let key = format!("key-{}", Ulid::new().to_string().to_lowercase());
    let commit = append_commit_body(&uuid, None, 4001, 1);

    let (status, first) = send(
        &ctx.router,
        "POST",
        &url,
        Some(&[("idempotency-key", key.as_str())]),
        Some(commit.clone()),
    )
    .await;
    assert_eq!(status.1, StatusCode::OK, "{first}");

    // Same key, same request: identical response, nothing reapplied — even
    // though the requirement (ref must be unset) is stale by now.
    let (status, second) = send(
        &ctx.router,
        "POST",
        &url,
        Some(&[("idempotency-key", key.as_str())]),
        Some(commit),
    )
    .await;
    assert_eq!(status.1, StatusCode::OK, "{second}");
    assert_eq!(first, second, "replay must reproduce the original response");

    let (status, body) = send(&ctx.router, "GET", &url, None, None).await;
    assert_eq!(status.1, StatusCode::OK);
    assert_eq!(status.0.as_deref(), Some(format!("\"{uuid}-g1\"").as_str()));
    assert_eq!(
        body["metadata"]["snapshots"].as_array().map(Vec::len),
        Some(1),
        "no double snapshot"
    );

    // Same key, different request: 422, loudly (F9).
    let (status, body) = send(
        &ctx.router,
        "POST",
        &url,
        Some(&[("idempotency-key", key.as_str())]),
        Some(append_commit_body(&uuid, Some(4001), 4002, 2)),
    )
    .await;
    assert_eq!(status.1, StatusCode::UNPROCESSABLE_ENTITY);
    assert_error(&body, 422, "UnprocessableEntityException");
}

// ---------------------------------------------------------------------------
// Concurrent committers through the full HTTP stack
// ---------------------------------------------------------------------------

#[tokio::test(flavor = "multi_thread", worker_threads = 8)]
async fn concurrent_committers_all_land_without_lost_updates() {
    const COMMITTERS: i64 = 8;

    let Some(ctx) = test_ctx().await else { return };
    make_namespace(&ctx, "db").await;
    let (_, uuid, _) = make_table(&ctx, "db", "hot").await;
    let url = table_url(&ctx, "db", "hot");

    let mut tasks = Vec::new();
    for label in 0..COMMITTERS {
        let router = ctx.router.clone();
        let url = url.clone();
        let uuid = uuid.clone();
        tasks.push(tokio::spawn(async move {
            let snapshot_id = 5000 + label;
            // Engine-style loop: fetch current state, build the commit
            // against it, retry from a fresh base on 409.
            for _attempt in 0..100 {
                let (status, body) = send(&router, "GET", &url, None, None).await;
                assert_eq!(status.1, StatusCode::OK, "refresh: {body}");
                let parent = body["metadata"]["current-snapshot-id"].as_i64();
                let seq = body["metadata"]["last-sequence-number"]
                    .as_i64()
                    .expect("v2 table has a sequence number")
                    + 1;

                let commit = append_commit_body(&uuid, parent, snapshot_id, seq);
                let (status, body) = send(&router, "POST", &url, None, Some(commit)).await;
                match status.1 {
                    StatusCode::OK => return,
                    StatusCode::CONFLICT => {
                        assert_eq!(body["error"]["type"], "CommitFailedException", "{body}");
                    }
                    other => panic!("unexpected commit status {other}: {body}"),
                }
            }
            panic!("committer {label} exhausted 100 engine retries");
        }));
    }
    for task in tasks {
        task.await.expect("committer task must not panic");
    }

    // Every commit landed exactly once.
    let (status, body) = send(&ctx.router, "GET", &url, None, None).await;
    assert_eq!(status.1, StatusCode::OK);
    assert_eq!(
        status.0.as_deref(),
        Some(format!("\"{uuid}-g{COMMITTERS}\"").as_str()),
        "final generation equals the number of successful commits"
    );
    let snapshots = body["metadata"]["snapshots"].as_array().expect("snapshots");
    assert_eq!(snapshots.len(), usize::try_from(COMMITTERS).expect("small"));
    let mut ids: Vec<i64> = snapshots
        .iter()
        .map(|s| s["snapshot-id"].as_i64().expect("id"))
        .collect();
    ids.sort_unstable();
    assert_eq!(ids, (5000..5000 + COMMITTERS).collect::<Vec<_>>());
    assert_eq!(
        body["metadata"]["metadata-log"].as_array().map(Vec::len),
        Some(usize::try_from(COMMITTERS).expect("small")),
    );

    // Generation history is strictly monotonic and gapless: the audit trail
    // (written in the commit transaction) is the witness.
    let versions: Vec<(Value,)> = sqlx::query_as(
        "SELECT a.details->'pointer_version'
         FROM audit_log a JOIN tables t ON a.resource = 'table:' || t.id
         WHERE a.action = 'table.commit' AND t.table_uuid = $1
         ORDER BY a.seq",
    )
    .bind(&uuid)
    .fetch_all(&ctx.pool)
    .await
    .expect("audit-derived commit history");
    let versions: Vec<i64> = versions
        .into_iter()
        .map(|(v,)| v.as_i64().expect("pointer_version"))
        .collect();
    assert_eq!(versions, (1..=COMMITTERS).collect::<Vec<_>>());
}

// ---------------------------------------------------------------------------
// Multi-table transactions
// ---------------------------------------------------------------------------

#[tokio::test]
#[allow(clippy::too_many_lines)] // success, failure, and validation paths in one scenario
async fn multi_table_transaction_is_atomic() {
    let Some(ctx) = test_ctx().await else { return };
    make_namespace(&ctx, "db").await;
    let (_, uuid_a, _) = make_table(&ctx, "db", "a").await;
    let (_, uuid_b, _) = make_table(&ctx, "db", "b").await;
    let txn_url = format!("/v1/{}/transactions/commit", ctx.warehouse);

    // Two-table atomic commit succeeds: both generations advance.
    let (status, body) = send(
        &ctx.router,
        "POST",
        &txn_url,
        None,
        Some(json!({ "table-changes": [
            {
                "identifier": { "namespace": ["db"], "name": "a" },
                "requirements": [ { "type": "assert-table-uuid", "uuid": uuid_a } ],
                "updates": [ { "action": "set-properties", "updates": { "touched": "yes" } } ],
            },
            {
                "identifier": { "namespace": ["db"], "name": "b" },
                "requirements": [ { "type": "assert-table-uuid", "uuid": uuid_b } ],
                "updates": [ { "action": "set-properties", "updates": { "touched": "yes" } } ],
            },
        ]})),
    )
    .await;
    assert_eq!(status.1, StatusCode::NO_CONTENT, "{body}");
    for (name, uuid) in [("a", &uuid_a), ("b", &uuid_b)] {
        let (status, body) =
            send(&ctx.router, "GET", &table_url(&ctx, "db", name), None, None).await;
        assert_eq!(status.1, StatusCode::OK);
        assert_eq!(status.0.as_deref(), Some(format!("\"{uuid}-g1\"").as_str()));
        assert_eq!(body["metadata"]["properties"]["touched"], "yes");
    }

    // One table's requirement fails: NEITHER applies, and the response
    // names the violation. Table "a" would succeed alone.
    let (status, body) = send(
        &ctx.router,
        "POST",
        &txn_url,
        None,
        Some(json!({ "table-changes": [
            {
                "identifier": { "namespace": ["db"], "name": "a" },
                "requirements": [ { "type": "assert-table-uuid", "uuid": uuid_a } ],
                "updates": [ { "action": "set-properties", "updates": { "second": "yes" } } ],
            },
            {
                "identifier": { "namespace": ["db"], "name": "b" },
                "requirements": [ { "type": "assert-current-schema-id", "current-schema-id": 999 } ],
                "updates": [ { "action": "set-properties", "updates": { "second": "yes" } } ],
            },
        ]})),
    )
    .await;
    assert_eq!(status.1, StatusCode::CONFLICT);
    assert_error(&body, 409, "CommitFailedException");
    assert!(
        body["error"]["message"]
            .as_str()
            .is_some_and(|m| m.contains("db.b")),
        "the violation names the failing table: {body}"
    );
    for (name, uuid) in [("a", &uuid_a), ("b", &uuid_b)] {
        let (status, body) =
            send(&ctx.router, "GET", &table_url(&ctx, "db", name), None, None).await;
        assert_eq!(status.1, StatusCode::OK);
        assert_eq!(
            status.0.as_deref(),
            Some(format!("\"{uuid}-g1\"").as_str()),
            "table {name} must be untouched by the failed transaction"
        );
        assert!(body["metadata"]["properties"].get("second").is_none());
    }

    // Duplicate table in one transaction: 400.
    let change = json!({
        "identifier": { "namespace": ["db"], "name": "a" },
        "requirements": [],
        "updates": [ { "action": "set-properties", "updates": { "x": "1" } } ],
    });
    let (status, body) = send(
        &ctx.router,
        "POST",
        &txn_url,
        None,
        Some(json!({ "table-changes": [change.clone(), change] })),
    )
    .await;
    assert_eq!(status.1, StatusCode::BAD_REQUEST);
    assert_error(&body, 400, "BadRequestException");

    // A change without an identifier: 400. Unknown table: 404.
    let (status, body) = send(
        &ctx.router,
        "POST",
        &txn_url,
        None,
        Some(json!({ "table-changes": [
            { "requirements": [], "updates": [] },
        ]})),
    )
    .await;
    assert_eq!(status.1, StatusCode::BAD_REQUEST);
    assert_error(&body, 400, "BadRequestException");
    let (status, body) = send(
        &ctx.router,
        "POST",
        &txn_url,
        None,
        Some(json!({ "table-changes": [
            { "identifier": { "namespace": ["db"], "name": "ghost" },
              "requirements": [], "updates": [] },
        ]})),
    )
    .await;
    assert_eq!(status.1, StatusCode::NOT_FOUND);
    assert_error(&body, 404, "NoSuchTableException");
}

#[tokio::test]
async fn multi_table_transaction_replays_idempotently() {
    let Some(ctx) = test_ctx().await else { return };
    make_namespace(&ctx, "db").await;
    let (_, uuid_a, _) = make_table(&ctx, "db", "a").await;
    make_table(&ctx, "db", "b").await;
    let txn_url = format!("/v1/{}/transactions/commit", ctx.warehouse);
    let key = format!("txn-{}", Ulid::new().to_string().to_lowercase());

    let body_value = json!({ "table-changes": [
        {
            "identifier": { "namespace": ["db"], "name": "a" },
            "requirements": [],
            "updates": [ { "action": "set-properties", "updates": { "n": "1" } } ],
        },
        {
            "identifier": { "namespace": ["db"], "name": "b" },
            "requirements": [],
            "updates": [ { "action": "set-properties", "updates": { "n": "1" } } ],
        },
    ]});

    let (status, out) = send(
        &ctx.router,
        "POST",
        &txn_url,
        Some(&[("idempotency-key", key.as_str())]),
        Some(body_value.clone()),
    )
    .await;
    assert_eq!(status.1, StatusCode::NO_CONTENT, "{out}");

    // Replay: 204 again, generations unchanged (nothing reapplied).
    let (status, out) = send(
        &ctx.router,
        "POST",
        &txn_url,
        Some(&[("idempotency-key", key.as_str())]),
        Some(body_value),
    )
    .await;
    assert_eq!(status.1, StatusCode::NO_CONTENT, "{out}");
    let (status, _) = send(&ctx.router, "GET", &table_url(&ctx, "db", "a"), None, None).await;
    assert_eq!(
        status.0.as_deref(),
        Some(format!("\"{uuid_a}-g1\"").as_str())
    );

    // Same key, different transaction: 422.
    let (status, body) = send(
        &ctx.router,
        "POST",
        &txn_url,
        Some(&[("idempotency-key", key.as_str())]),
        Some(json!({ "table-changes": [
            { "identifier": { "namespace": ["db"], "name": "a" },
              "requirements": [], "updates": [ { "action": "set-properties", "updates": { "n": "2" } } ] },
        ]})),
    )
    .await;
    assert_eq!(status.1, StatusCode::UNPROCESSABLE_ENTITY);
    assert_error(&body, 422, "UnprocessableEntityException");
}
