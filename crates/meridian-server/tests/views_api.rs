//! Router-level integration tests for the Iceberg REST view surface.
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
        eprintln!("skipping view API test: DATABASE_URL is not set");
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
        Some(json!({ "name": warehouse, "storage_root": storage_root })),
    )
    .await;
    assert_eq!(status, StatusCode::CREATED, "create warehouse: {body}");

    Some(TestCtx {
        router,
        pool,
        _root: root,
        warehouse,
    })
}

/// Sends one request through the full middleware stack.
async fn send(
    router: &Router,
    method: &str,
    uri: &str,
    body: Option<Value>,
) -> (StatusCode, Value) {
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
    let value = if bytes.is_empty() {
        Value::Null
    } else {
        serde_json::from_slice(&bytes).expect("response body is JSON")
    };
    (status, value)
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
        Some(json!({ "namespace": [name] })),
    )
    .await;
    assert_eq!(status, StatusCode::OK, "create namespace: {body}");
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

/// A `CreateViewRequest` with SQL representations in two dialects.
fn create_view_body(name: &str, ns: &str) -> Value {
    json!({
        "name": name,
        "schema": simple_schema(),
        "view-version": {
            "version-id": 1,
            "timestamp-ms": 1_700_000_000_000i64,
            "schema-id": 0,
            "summary": { "engine-name": "e2e-tests" },
            "representations": [
                { "type": "sql",
                  "sql": format!("SELECT id, payload FROM {ns}.events"),
                  "dialect": "spark" },
                { "type": "sql",
                  "sql": format!("SELECT id, payload FROM \"{ns}\".events"),
                  "dialect": "trino" },
            ],
            "default-namespace": [ns],
        },
        "properties": { "comment": "integration test view" },
    })
}

/// Creates a view and returns (metadata-location, view-uuid).
async fn make_view(ctx: &TestCtx, ns: &str, name: &str) -> (String, String) {
    let (status, body) = send(
        &ctx.router,
        "POST",
        &format!("/v1/{}/namespaces/{ns}/views", ctx.warehouse),
        Some(create_view_body(name, ns)),
    )
    .await;
    assert_eq!(status, StatusCode::OK, "create view: {body}");
    (
        body["metadata-location"]
            .as_str()
            .expect("metadata-location")
            .to_owned(),
        body["metadata"]["view-uuid"]
            .as_str()
            .expect("view-uuid")
            .to_owned(),
    )
}

fn view_url(ctx: &TestCtx, ns: &str, name: &str) -> String {
    format!("/v1/{}/namespaces/{ns}/views/{name}", ctx.warehouse)
}

async fn make_table(ctx: &TestCtx, ns: &str, name: &str) {
    let (status, body) = send(
        &ctx.router,
        "POST",
        &format!("/v1/{}/namespaces/{ns}/tables", ctx.warehouse),
        Some(json!({ "name": name, "schema": simple_schema() })),
    )
    .await;
    assert_eq!(status, StatusCode::OK, "create table: {body}");
}

// ---------------------------------------------------------------------------
// Full lifecycle: create → load → list → replace → rename → drop
// ---------------------------------------------------------------------------

#[tokio::test]
#[allow(clippy::too_many_lines)] // one test walks the whole view lifecycle
async fn view_lifecycle_create_load_replace_rename_drop() {
    let Some(ctx) = test_ctx().await else { return };
    make_namespace(&ctx, "db").await;
    make_namespace(&ctx, "other").await;

    // -- create ---------------------------------------------------------
    let (location, uuid) = make_view(&ctx, "db", "events_agg").await;
    assert!(location.contains("/metadata/00000-"), "{location}");
    assert!(location.ends_with(".metadata.json"), "{location}");
    assert!(
        location.contains(&format!("/db/events_agg-{uuid}/")),
        "default location must be uuid-suffixed under the namespace path: {location}"
    );

    // The metadata file really exists and is a parseable v1 view document
    // carrying both dialect representations.
    let path = location.strip_prefix("file://").expect("file location");
    let raw = std::fs::read_to_string(path).expect("view metadata file exists");
    let parsed: Value = serde_json::from_str(&raw).expect("view metadata is JSON");
    assert_eq!(parsed["format-version"], 1);
    assert_eq!(parsed["view-uuid"], uuid.as_str());
    assert_eq!(parsed["current-version-id"], 1);
    let dialects: Vec<&str> = parsed["versions"][0]["representations"]
        .as_array()
        .expect("representations")
        .iter()
        .map(|r| r["dialect"].as_str().expect("dialect"))
        .collect();
    assert_eq!(dialects, vec!["spark", "trino"]);
    assert_eq!(
        parsed["version-log"].as_array().map(Vec::len),
        Some(1),
        "creating the first version writes one version-log entry"
    );

    // -- load, on both mounts --------------------------------------------
    for base in ["/v1", "/iceberg/v1"] {
        let (status, body) = send(
            &ctx.router,
            "GET",
            &format!("{base}/{}/namespaces/db/views/events_agg", ctx.warehouse),
            None,
        )
        .await;
        assert_eq!(status, StatusCode::OK, "{body}");
        assert_eq!(body["metadata-location"], location.as_str());
        assert_eq!(body["metadata"]["view-uuid"], uuid.as_str());
        // file:// warehouses have no client-facing storage options.
        assert_eq!(body["config"], json!({}));
    }

    // -- list (and: views never appear in the tables listing) -------------
    let (status, body) = send(
        &ctx.router,
        "GET",
        &format!("/v1/{}/namespaces/db/views", ctx.warehouse),
        None,
    )
    .await;
    assert_eq!(status, StatusCode::OK);
    assert_eq!(
        body["identifiers"],
        json!([{ "namespace": ["db"], "name": "events_agg" }])
    );
    assert!(body["next-page-token"].is_null());
    let (status, body) = send(
        &ctx.router,
        "GET",
        &format!("/v1/{}/namespaces/db/tables", ctx.warehouse),
        None,
    )
    .await;
    assert_eq!(status, StatusCode::OK);
    assert_eq!(body["identifiers"], json!([]));

    // -- HEAD --------------------------------------------------------------
    let (status, _) = send(
        &ctx.router,
        "HEAD",
        &view_url(&ctx, "db", "events_agg"),
        None,
    )
    .await;
    assert_eq!(status, StatusCode::NO_CONTENT);

    // -- replace: adds a version, sets it current, grows the version log --
    let replace = json!({
        "identifier": { "namespace": ["db"], "name": "events_agg" },
        "requirements": [ { "type": "assert-view-uuid", "uuid": uuid } ],
        "updates": [
            { "action": "set-properties", "updates": { "replaced": "yes" } },
            { "action": "add-schema", "schema": {
                "type": "struct",
                "fields": [
                    { "id": 1, "name": "id", "required": true, "type": "long" },
                ],
            }},
            { "action": "add-view-version", "view-version": {
                "version-id": 0,
                "timestamp-ms": 1_700_000_100_000i64,
                "schema-id": -1,
                "summary": { "engine-name": "e2e-tests", "operation": "replace" },
                "representations": [
                    { "type": "sql", "sql": "SELECT id FROM db.events", "dialect": "spark" },
                ],
                "default-namespace": ["db"],
            }},
            { "action": "set-current-view-version", "view-version-id": -1 },
        ],
    });
    let (status, body) = send(
        &ctx.router,
        "POST",
        &view_url(&ctx, "db", "events_agg"),
        Some(replace),
    )
    .await;
    assert_eq!(status, StatusCode::OK, "replace: {body}");
    let new_location = body["metadata-location"].as_str().expect("location");
    assert_ne!(new_location, location);
    assert!(
        new_location.contains("/metadata/00001-"),
        "metadata file version tracks the pointer: {new_location}"
    );
    assert_eq!(body["metadata"]["view-uuid"], uuid.as_str());
    assert_eq!(body["metadata"]["current-version-id"], 2);
    assert_eq!(
        body["metadata"]["versions"].as_array().map(Vec::len),
        Some(2)
    );
    assert_eq!(
        body["metadata"]["version-log"].as_array().map(Vec::len),
        Some(2),
        "the version log grows by one entry per current-version change"
    );
    assert_eq!(body["metadata"]["properties"]["replaced"], "yes");

    // Loading reflects the replace; the file at the new location parses.
    let (status, body) = send(
        &ctx.router,
        "GET",
        &view_url(&ctx, "db", "events_agg"),
        None,
    )
    .await;
    assert_eq!(status, StatusCode::OK);
    assert_eq!(body["metadata-location"], new_location);
    assert_eq!(body["metadata"]["current-version-id"], 2);

    // The pointer swap was audited and produced an outbox event, scoped by
    // this view's uuid (the shared test database retains other runs' rows).
    let committed_events: i64 = sqlx::query_scalar(
        "SELECT count(*) FROM events_outbox
         WHERE event_type = 'view.committed' AND payload->>'view_uuid' = $1",
    )
    .bind(&uuid)
    .fetch_one(&ctx.pool)
    .await
    .expect("count view.committed events");
    assert_eq!(committed_events, 1, "replace must enqueue its outbox event");
    let audit_rows: i64 = sqlx::query_scalar(
        "SELECT count(*) FROM audit_log
         WHERE action = 'view.commit' AND details->>'view_uuid' = $1",
    )
    .bind(&uuid)
    .fetch_one(&ctx.pool)
    .await
    .expect("count view.commit audit rows");
    assert_eq!(audit_rows, 1, "replace must write its audit row");

    // -- replace failure paths --------------------------------------------
    // Wrong view UUID: 409 CommitFailedException, nothing applied.
    let bogus_uuid = "00000000-0000-4000-8000-000000000000";
    let (status, body) = send(
        &ctx.router,
        "POST",
        &view_url(&ctx, "db", "events_agg"),
        Some(json!({
            "requirements": [ { "type": "assert-view-uuid", "uuid": bogus_uuid } ],
            "updates": [
                { "action": "set-properties", "updates": { "never": "applied" } },
            ],
        })),
    )
    .await;
    assert_eq!(status, StatusCode::CONFLICT);
    assert_error(&body, 409, "CommitFailedException");

    // Unknown update action: 400 per the spec.
    let (status, body) = send(
        &ctx.router,
        "POST",
        &view_url(&ctx, "db", "events_agg"),
        Some(json!({
            "updates": [ { "action": "warp-reality" } ],
        })),
    )
    .await;
    assert_eq!(status, StatusCode::BAD_REQUEST);
    assert_error(&body, 400, "BadRequestException");

    // Nothing applied by either failure.
    let (status, body) = send(
        &ctx.router,
        "GET",
        &view_url(&ctx, "db", "events_agg"),
        None,
    )
    .await;
    assert_eq!(status, StatusCode::OK);
    assert_eq!(body["metadata-location"], new_location);
    assert!(body["metadata"]["properties"].get("never").is_none());

    // -- rename: same namespace, then across namespaces --------------------
    let rename_url = format!("/v1/{}/views/rename", ctx.warehouse);
    let (status, body) = send(
        &ctx.router,
        "POST",
        &rename_url,
        Some(json!({
            "source": { "namespace": ["db"], "name": "events_agg" },
            "destination": { "namespace": ["db"], "name": "renamed" },
        })),
    )
    .await;
    assert_eq!(status, StatusCode::NO_CONTENT, "{body}");
    let (status, _) = send(
        &ctx.router,
        "POST",
        &rename_url,
        Some(json!({
            "source": { "namespace": ["db"], "name": "renamed" },
            "destination": { "namespace": ["other"], "name": "moved" },
        })),
    )
    .await;
    assert_eq!(status, StatusCode::NO_CONTENT);

    // Identity (uuid, metadata) rides along; the old name is gone.
    let (status, body) = send(&ctx.router, "GET", &view_url(&ctx, "other", "moved"), None).await;
    assert_eq!(status, StatusCode::OK);
    assert_eq!(body["metadata"]["view-uuid"], uuid.as_str());
    assert_eq!(body["metadata-location"], new_location);
    let (status, body) = send(&ctx.router, "GET", &view_url(&ctx, "db", "renamed"), None).await;
    assert_eq!(status, StatusCode::NOT_FOUND);
    assert_error(&body, 404, "NoSuchViewException");

    // -- drop: 204, then every view endpoint 404s; files remain ------------
    let (status, _) = send(
        &ctx.router,
        "DELETE",
        &view_url(&ctx, "other", "moved"),
        None,
    )
    .await;
    assert_eq!(status, StatusCode::NO_CONTENT);
    let (status, body) = send(&ctx.router, "GET", &view_url(&ctx, "other", "moved"), None).await;
    assert_eq!(status, StatusCode::NOT_FOUND);
    assert_error(&body, 404, "NoSuchViewException");
    let (status, _) = send(&ctx.router, "HEAD", &view_url(&ctx, "other", "moved"), None).await;
    assert_eq!(status, StatusCode::NOT_FOUND);
    let new_path = new_location.strip_prefix("file://").expect("file path");
    assert!(
        std::path::Path::new(new_path).exists(),
        "dropView must leave metadata files in place (no purge for views)"
    );
}

// ---------------------------------------------------------------------------
// 404 / 409 paths, including table-vs-view name collisions
// ---------------------------------------------------------------------------

#[tokio::test]
async fn view_404s_use_exact_exception_types() {
    let Some(ctx) = test_ctx().await else { return };
    make_namespace(&ctx, "db").await;

    // Listing/creating under a missing namespace: NoSuchNamespaceException.
    let (status, body) = send(
        &ctx.router,
        "GET",
        &format!("/v1/{}/namespaces/ghost/views", ctx.warehouse),
        None,
    )
    .await;
    assert_eq!(status, StatusCode::NOT_FOUND);
    assert_error(&body, 404, "NoSuchNamespaceException");
    let (status, body) = send(
        &ctx.router,
        "POST",
        &format!("/v1/{}/namespaces/ghost/views", ctx.warehouse),
        Some(create_view_body("v", "ghost")),
    )
    .await;
    assert_eq!(status, StatusCode::NOT_FOUND);
    assert_error(&body, 404, "NoSuchNamespaceException");

    // Load / replace / drop of a missing view: NoSuchViewException.
    let (status, body) = send(&ctx.router, "GET", &view_url(&ctx, "db", "ghost"), None).await;
    assert_eq!(status, StatusCode::NOT_FOUND);
    assert_error(&body, 404, "NoSuchViewException");
    let (status, body) = send(
        &ctx.router,
        "POST",
        &view_url(&ctx, "db", "ghost"),
        Some(json!({ "updates": [] })),
    )
    .await;
    assert_eq!(status, StatusCode::NOT_FOUND);
    assert_error(&body, 404, "NoSuchViewException");
    let (status, body) = send(&ctx.router, "DELETE", &view_url(&ctx, "db", "ghost"), None).await;
    assert_eq!(status, StatusCode::NOT_FOUND);
    assert_error(&body, 404, "NoSuchViewException");

    // Renaming a missing view: NoSuchViewException; missing destination
    // namespace: NoSuchNamespaceException.
    let rename_url = format!("/v1/{}/views/rename", ctx.warehouse);
    let (status, body) = send(
        &ctx.router,
        "POST",
        &rename_url,
        Some(json!({
            "source": { "namespace": ["db"], "name": "ghost" },
            "destination": { "namespace": ["db"], "name": "x" },
        })),
    )
    .await;
    assert_eq!(status, StatusCode::NOT_FOUND);
    assert_error(&body, 404, "NoSuchViewException");
    make_view(&ctx, "db", "mover").await;
    let (status, body) = send(
        &ctx.router,
        "POST",
        &rename_url,
        Some(json!({
            "source": { "namespace": ["db"], "name": "mover" },
            "destination": { "namespace": ["ghost"], "name": "x" },
        })),
    )
    .await;
    assert_eq!(status, StatusCode::NOT_FOUND);
    assert_error(&body, 404, "NoSuchNamespaceException");

    // Unknown warehouse prefix: NoSuchWarehouseException.
    let (status, body) = send(
        &ctx.router,
        "GET",
        "/v1/no-such-wh/namespaces/db/views",
        None,
    )
    .await;
    assert_eq!(status, StatusCode::NOT_FOUND);
    assert_error(&body, 404, "NoSuchWarehouseException");
}

#[tokio::test]
#[allow(clippy::too_many_lines)] // both directions of the shared name space in one scenario
async fn tables_and_views_share_one_name_space() {
    let Some(ctx) = test_ctx().await else { return };
    make_namespace(&ctx, "db").await;

    make_table(&ctx, "db", "occupied_by_table").await;
    make_view(&ctx, "db", "occupied_by_view").await;

    // Duplicate view name: 409.
    let (status, body) = send(
        &ctx.router,
        "POST",
        &format!("/v1/{}/namespaces/db/views", ctx.warehouse),
        Some(create_view_body("occupied_by_view", "db")),
    )
    .await;
    assert_eq!(status, StatusCode::CONFLICT);
    assert_error(&body, 409, "AlreadyExistsException");

    // A view may not take a table's name (the spec's createView 409s when
    // "the identifier already exists as a table or view").
    let (status, body) = send(
        &ctx.router,
        "POST",
        &format!("/v1/{}/namespaces/db/views", ctx.warehouse),
        Some(create_view_body("occupied_by_table", "db")),
    )
    .await;
    assert_eq!(status, StatusCode::CONFLICT);
    assert_error(&body, 409, "AlreadyExistsException");
    assert!(
        body["error"]["message"]
            .as_str()
            .is_some_and(|m| m.contains("table")),
        "the 409 must say the name is taken by a table: {body}"
    );

    // Renaming a view onto a table's name (or another view's name): 409.
    make_view(&ctx, "db", "mover").await;
    let rename_url = format!("/v1/{}/views/rename", ctx.warehouse);
    for destination in ["occupied_by_table", "occupied_by_view"] {
        let (status, body) = send(
            &ctx.router,
            "POST",
            &rename_url,
            Some(json!({
                "source": { "namespace": ["db"], "name": "mover" },
                "destination": { "namespace": ["db"], "name": destination },
            })),
        )
        .await;
        assert_eq!(status, StatusCode::CONFLICT, "{destination}: {body}");
        assert_error(&body, 409, "AlreadyExistsException");
    }

    // The view still loads under its original name (nothing was applied).
    let (status, _) = send(&ctx.router, "HEAD", &view_url(&ctx, "db", "mover"), None).await;
    assert_eq!(status, StatusCode::NO_CONTENT);

    // --- Tables side of the shared name space (the mirror of the above) ---

    // A table may not take a view's name; the 409 must name the collision as
    // a view.
    let (status, body) = send(
        &ctx.router,
        "POST",
        &format!("/v1/{}/namespaces/db/tables", ctx.warehouse),
        Some(json!({ "name": "occupied_by_view", "schema": simple_schema() })),
    )
    .await;
    assert_eq!(status, StatusCode::CONFLICT, "{body}");
    assert_error(&body, 409, "AlreadyExistsException");
    assert!(
        body["error"]["message"]
            .as_str()
            .is_some_and(|m| m.contains("view")),
        "the 409 must say the name is taken by a view: {body}"
    );

    // registerTable lives on the same shared name space: adopting a metadata
    // file under a view's name is a 409 too. (A donor table supplies a real
    // metadata file; the collision is caught before it is ever read.)
    let (status, donor) = send(
        &ctx.router,
        "POST",
        &format!("/v1/{}/namespaces/db/tables", ctx.warehouse),
        Some(json!({ "name": "reg_donor", "schema": simple_schema() })),
    )
    .await;
    assert_eq!(status, StatusCode::OK, "create donor table: {donor}");
    let donor_location = donor["metadata-location"]
        .as_str()
        .expect("donor metadata-location");
    let (status, body) = send(
        &ctx.router,
        "POST",
        &format!("/v1/{}/namespaces/db/register", ctx.warehouse),
        Some(json!({ "name": "occupied_by_view", "metadata-location": donor_location })),
    )
    .await;
    assert_eq!(status, StatusCode::CONFLICT, "{body}");
    assert_error(&body, 409, "AlreadyExistsException");

    // Renaming a table onto a view's name: 409 (enforced in the store's
    // rename transaction).
    make_table(&ctx, "db", "table_mover").await;
    let (status, body) = send(
        &ctx.router,
        "POST",
        &format!("/v1/{}/tables/rename", ctx.warehouse),
        Some(json!({
            "source": { "namespace": ["db"], "name": "table_mover" },
            "destination": { "namespace": ["db"], "name": "occupied_by_view" },
        })),
    )
    .await;
    assert_eq!(status, StatusCode::CONFLICT, "{body}");
    assert_error(&body, 409, "AlreadyExistsException");

    // The table mover still loads under its original name (nothing applied).
    let (status, _) = send(
        &ctx.router,
        "HEAD",
        &format!("/v1/{}/namespaces/db/tables/table_mover", ctx.warehouse),
        None,
    )
    .await;
    assert_eq!(status, StatusCode::NO_CONTENT);
}

// ---------------------------------------------------------------------------
// dropNamespace accounts for views, not just child namespaces and tables
// ---------------------------------------------------------------------------

/// A namespace whose only content is a view must refuse to drop with a 409
/// `NamespaceNotEmptyException` — not surface the `views` → `namespaces`
/// `ON DELETE RESTRICT` foreign key as a 500 — and must drop cleanly once the
/// view is gone.
#[tokio::test]
async fn drop_namespace_rejects_a_namespace_that_still_holds_a_view() {
    let Some(ctx) = test_ctx().await else { return };
    make_namespace(&ctx, "db").await;
    make_view(&ctx, "db", "events_agg").await;

    let ns_url = format!("/v1/{}/namespaces/db", ctx.warehouse);

    // A view alone keeps the namespace non-empty: 409, not the foreign key's 500.
    let (status, body) = send(&ctx.router, "DELETE", &ns_url, None).await;
    assert_eq!(status, StatusCode::CONFLICT, "{body}");
    assert_error(&body, 409, "NamespaceNotEmptyException");
    assert!(
        body["error"]["message"]
            .as_str()
            .is_some_and(|m| m.contains("view")),
        "the 409 must name a remaining view as what keeps the namespace non-empty: {body}"
    );

    // Drop the view, then the now-empty namespace drops cleanly.
    let (status, _) = send(
        &ctx.router,
        "DELETE",
        &view_url(&ctx, "db", "events_agg"),
        None,
    )
    .await;
    assert_eq!(status, StatusCode::NO_CONTENT);
    let (status, body) = send(&ctx.router, "DELETE", &ns_url, None).await;
    assert_eq!(status, StatusCode::NO_CONTENT, "{body}");
}

// ---------------------------------------------------------------------------
// Listing pagination
// ---------------------------------------------------------------------------

#[tokio::test]
async fn view_listing_paginates() {
    let Some(ctx) = test_ctx().await else { return };
    make_namespace(&ctx, "db").await;
    for name in ["v0", "v1", "v2"] {
        make_view(&ctx, "db", name).await;
    }

    // Unpaginated: everything, null token.
    let (status, body) = send(
        &ctx.router,
        "GET",
        &format!("/v1/{}/namespaces/db/views", ctx.warehouse),
        None,
    )
    .await;
    assert_eq!(status, StatusCode::OK);
    assert_eq!(body["identifiers"].as_array().map(Vec::len), Some(3));
    assert!(body["next-page-token"].is_null());

    // pageSize=2 walks two pages: 2 + 1.
    let (status, body) = send(
        &ctx.router,
        "GET",
        &format!("/v1/{}/namespaces/db/views?pageSize=2", ctx.warehouse),
        None,
    )
    .await;
    assert_eq!(status, StatusCode::OK);
    assert_eq!(body["identifiers"].as_array().map(Vec::len), Some(2));
    let token = body["next-page-token"].as_str().expect("token").to_owned();
    let (status, body) = send(
        &ctx.router,
        "GET",
        &format!(
            "/v1/{}/namespaces/db/views?pageSize=2&pageToken={token}",
            ctx.warehouse
        ),
        None,
    )
    .await;
    assert_eq!(status, StatusCode::OK);
    assert_eq!(body["identifiers"].as_array().map(Vec::len), Some(1));
    assert_eq!(body["identifiers"][0]["name"], "v2");
    assert!(body["next-page-token"].is_null());
}

// ---------------------------------------------------------------------------
// Create-request field ids are provisional (mirrors createTable)
// ---------------------------------------------------------------------------

/// The exact request shape Spark 3.5's `CREATE VIEW` sends (0-based field
/// ids, previously rejected with `field id 0 is not positive`) must
/// succeed, with fresh 1-based ids assigned server-side — the same
/// provisional-id treatment `createTable` applies.
#[tokio::test]
async fn create_view_treats_request_field_ids_as_provisional() {
    let Some(ctx) = test_ctx().await else { return };
    make_namespace(&ctx, "db").await;

    let (status, body) = send(
        &ctx.router,
        "POST",
        &format!("/v1/{}/namespaces/db/views", ctx.warehouse),
        Some(json!({
            "name": "orders_by_category",
            "schema": {
                "type": "struct",
                "schema-id": 0,
                "fields": [
                    { "id": 0, "name": "category", "required": false, "type": "string" },
                    { "id": 1, "name": "cnt", "required": false, "type": "long" },
                    { "id": 2, "name": "total_amount", "required": false, "type": "double" },
                ],
            },
            "view-version": {
                "version-id": 1,
                "timestamp-ms": 1_700_000_000_000i64,
                "schema-id": 0,
                "summary": { "engine-name": "spark", "engine-version": "3.5.6" },
                "representations": [
                    { "type": "sql",
                      "sql": "SELECT category, count(*) AS cnt FROM db.orders GROUP BY category",
                      "dialect": "spark" },
                ],
                "default-namespace": ["db"],
            },
            "properties": {},
        })),
    )
    .await;
    assert_eq!(status, StatusCode::OK, "Spark-shaped create view: {body}");
    let fields = body["metadata"]["schemas"][0]["fields"]
        .as_array()
        .expect("schema fields");
    let ids: Vec<i64> = fields.iter().map(|f| f["id"].as_i64().unwrap()).collect();
    assert_eq!(ids, vec![1, 2, 3], "fresh 1-based ids: {body}");

    // Genuinely broken requests still fail: duplicate sibling field names
    // are unresolvable once ids are reassigned.
    let (status, body) = send(
        &ctx.router,
        "POST",
        &format!("/v1/{}/namespaces/db/views", ctx.warehouse),
        Some(json!({
            "name": "broken",
            "schema": {
                "type": "struct",
                "fields": [
                    { "id": 0, "name": "dup", "required": false, "type": "string" },
                    { "id": 1, "name": "dup", "required": false, "type": "long" },
                ],
            },
            "view-version": {
                "version-id": 1,
                "timestamp-ms": 1_700_000_000_000i64,
                "schema-id": 0,
                "summary": {},
                "representations": [
                    { "type": "sql", "sql": "SELECT 1", "dialect": "spark" },
                ],
                "default-namespace": ["db"],
            },
        })),
    )
    .await;
    assert_eq!(status, StatusCode::BAD_REQUEST, "duplicate names: {body}");
}

/// `CREATE OR REPLACE VIEW` from Spark 3.5 sends the same 0-based field ids on
/// the `add-schema` update of the replace commit as it does on create.
/// Replacing an *existing* view must succeed, with the replacement schema
/// getting fresh 1-based ids server-side — the same provisional-id treatment
/// `createView` applies, so the same statement behaves identically whether or
/// not the view already existed. Previously this failed with
/// `field id 0 is not positive`.
#[tokio::test]
async fn replace_view_treats_request_field_ids_as_provisional() {
    let Some(ctx) = test_ctx().await else { return };
    make_namespace(&ctx, "db").await;
    let (_location, uuid) = make_view(&ctx, "db", "orders_by_category").await;

    let (status, body) = send(
        &ctx.router,
        "POST",
        &view_url(&ctx, "db", "orders_by_category"),
        Some(json!({
            "identifier": { "namespace": ["db"], "name": "orders_by_category" },
            "requirements": [ { "type": "assert-view-uuid", "uuid": uuid } ],
            "updates": [
                { "action": "add-schema", "schema": {
                    "type": "struct",
                    "schema-id": 0,
                    "fields": [
                        { "id": 0, "name": "category", "required": false, "type": "string" },
                        { "id": 1, "name": "cnt", "required": false, "type": "long" },
                        { "id": 2, "name": "total_amount", "required": false, "type": "double" },
                    ],
                }},
                { "action": "add-view-version", "view-version": {
                    "version-id": 0,
                    "timestamp-ms": 1_700_000_100_000i64,
                    "schema-id": -1,
                    "summary": { "engine-name": "spark", "engine-version": "3.5.6" },
                    "representations": [
                        { "type": "sql",
                          "sql": "SELECT category, count(*) AS cnt FROM db.orders GROUP BY category",
                          "dialect": "spark" },
                    ],
                    "default-namespace": ["db"],
                }},
                { "action": "set-current-view-version", "view-version-id": -1 },
            ],
        })),
    )
    .await;
    assert_eq!(status, StatusCode::OK, "Spark-shaped replace view: {body}");

    // The replacement version's schema carries fresh 1-based ids.
    let current_id = body["metadata"]["current-version-id"]
        .as_i64()
        .expect("current-version-id");
    let schema_id = body["metadata"]["versions"]
        .as_array()
        .expect("versions")
        .iter()
        .find(|v| v["version-id"].as_i64() == Some(current_id))
        .expect("current version present")["schema-id"]
        .as_i64()
        .expect("schema-id");
    let new_schema = body["metadata"]["schemas"]
        .as_array()
        .expect("schemas")
        .iter()
        .find(|s| s["schema-id"].as_i64() == Some(schema_id))
        .expect("replacement schema present");
    let ids: Vec<i64> = new_schema["fields"]
        .as_array()
        .expect("schema fields")
        .iter()
        .map(|f| f["id"].as_i64().unwrap())
        .collect();
    assert_eq!(ids, vec![1, 2, 3], "fresh 1-based ids on replace: {body}");

    // Genuinely broken requests still fail: duplicate sibling field names are
    // unresolvable once ids are reassigned, exactly as on create.
    let (status, body) = send(
        &ctx.router,
        "POST",
        &view_url(&ctx, "db", "orders_by_category"),
        Some(json!({
            "updates": [
                { "action": "add-schema", "schema": {
                    "type": "struct",
                    "fields": [
                        { "id": 0, "name": "dup", "required": false, "type": "string" },
                        { "id": 1, "name": "dup", "required": false, "type": "long" },
                    ],
                }},
                { "action": "add-view-version", "view-version": {
                    "version-id": 0,
                    "timestamp-ms": 1_700_000_200_000i64,
                    "schema-id": -1,
                    "summary": {},
                    "representations": [
                        { "type": "sql", "sql": "SELECT 1", "dialect": "spark" },
                    ],
                    "default-namespace": ["db"],
                }},
                { "action": "set-current-view-version", "view-version-id": -1 },
            ],
        })),
    )
    .await;
    assert_eq!(status, StatusCode::BAD_REQUEST, "duplicate names: {body}");
}
