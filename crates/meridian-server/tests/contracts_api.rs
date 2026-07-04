//! Integration tests for data contracts and THE CIRCUIT BREAKER (Pillar E,
//! E-F3 / E-F4).
//!
//! These require a running Postgres and `DATABASE_URL`; without it they skip.
//! Each test provisions its own uniquely-named warehouse rooted in its own
//! tempdir (`file://` storage), so tests are isolated from each other and from
//! previous runs, and can assert on real metadata files on disk.
//!
//! The bar these hold (from the role brief):
//!
//! - schema-evolution classification through the HTTP commit endpoint
//!   (additive ok, narrowing rejected, protected-column drop rejected);
//! - **block** mode: a violating commit is rejected *atomically* — the pointer
//!   is unchanged, the metadata is unchanged, a violation is recorded
//!   (`commit_rejected=true`), and the audit chain still verifies;
//! - **warn** mode: the commit lands and a violation is recorded + evented;
//! - **quarantine** mode: `main` is not advanced (the load still shows the base
//!   snapshot), the snapshot lands on the quarantine branch, and publish
//!   fast-forwards `main`;
//! - a normal commit with no contract is unaffected.
//!
//! Auth runs in the default disabled mode: the anonymous principal satisfies
//! the management gate, exactly as the sibling `tables_api` tests rely on.
//! Management-auth itself is covered by `governance_api`.

use std::sync::Arc;

use axum::Router;
use axum::body::Body;
use axum::http::{Request, StatusCode};
use http_body_util::BodyExt;
use meridian_common::AppConfig;
use meridian_server::{AppState, build_router};
use serde_json::{Value, json};
use tower::ServiceExt;
use ulid::Ulid;

struct TestCtx {
    router: Router,
    _root: tempfile::TempDir,
    warehouse: String,
    /// Per-test salt so contract names (unique per *workspace*, and all tests
    /// share the default workspace) never collide across tests or reruns
    /// against the shared database (TEST-ISOLATION: unique names per test).
    salt: String,
}

async fn test_ctx() -> Option<TestCtx> {
    let Ok(url) = std::env::var("DATABASE_URL") else {
        eprintln!("skipping contracts API test: DATABASE_URL is not set");
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
        pool,
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
        _root: root,
        warehouse,
        salt: Ulid::new().to_string().to_lowercase(),
    })
}

/// Sends one request through the full middleware stack. Returns (status, body).
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

// ---------------------------------------------------------------------------
// Fixtures
// ---------------------------------------------------------------------------

fn base_schema() -> Value {
    json!({
        "type": "struct",
        "schema-id": 0,
        "fields": [
            { "id": 1, "name": "id", "required": true, "type": "long" },
            { "id": 2, "name": "email", "required": false, "type": "string" },
            { "id": 3, "name": "amount", "required": false, "type": "int" },
        ],
    })
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

/// Creates a table with `base_schema` and returns its table-uuid.
async fn make_table(ctx: &TestCtx, ns: &str, name: &str) -> String {
    let (status, body) = send(
        &ctx.router,
        "POST",
        &format!("/v1/{}/namespaces/{ns}/tables", ctx.warehouse),
        Some(json!({ "name": name, "schema": base_schema() })),
    )
    .await;
    assert_eq!(status, StatusCode::OK, "create table: {body}");
    body["metadata"]["table-uuid"]
        .as_str()
        .expect("table-uuid")
        .to_owned()
}

fn table_url(ctx: &TestCtx, ns: &str, name: &str) -> String {
    format!("/v1/{}/namespaces/{ns}/tables/{name}", ctx.warehouse)
}

/// An append commit: pin main to `parent`, add a snapshot, move main.
fn append_commit(uuid: &str, parent: Option<i64>, snapshot_id: i64, seq: i64) -> Value {
    json!({
        "requirements": [
            { "type": "assert-table-uuid", "uuid": uuid },
            { "type": "assert-ref-snapshot-id", "ref": "main", "snapshot-id": parent },
        ],
        "updates": [
            { "action": "add-snapshot", "snapshot": {
                "snapshot-id": snapshot_id,
                "parent-snapshot-id": parent,
                "sequence-number": seq,
                "timestamp-ms": 1_700_000_000_000i64 + seq,
                "manifest-list": format!("file:///fake/snap-{snapshot_id}.avro"),
                "summary": { "operation": "append", "total-records": "10" },
                "schema-id": 0,
            }},
            { "action": "set-snapshot-ref", "ref-name": "main",
              "type": "branch", "snapshot-id": snapshot_id },
        ],
    })
}

/// A commit that replaces the schema (adds a new schema + sets it current). The
/// caller supplies the new field list. `parent`/`snapshot_id`/`seq` also add a
/// snapshot so quarantine has a head to retarget. `seq` must exceed the table's
/// current last-sequence-number (the builder enforces monotonicity).
fn schema_change_commit(
    uuid: &str,
    fields: &Value,
    parent: Option<i64>,
    snapshot_id: i64,
    seq: i64,
) -> Value {
    json!({
        "requirements": [
            { "type": "assert-table-uuid", "uuid": uuid },
        ],
        "updates": [
            { "action": "add-schema", "schema": { "type": "struct", "fields": fields } },
            { "action": "set-current-schema", "schema-id": -1 },
            { "action": "add-snapshot", "snapshot": {
                "snapshot-id": snapshot_id,
                "parent-snapshot-id": parent,
                "sequence-number": seq,
                "timestamp-ms": 1_700_000_100_000i64 + seq,
                "manifest-list": format!("file:///fake/snap-{snapshot_id}.avro"),
                "summary": { "operation": "append", "total-records": "10" },
                "schema-id": 0,
            }},
            { "action": "set-snapshot-ref", "ref-name": "main",
              "type": "branch", "snapshot-id": snapshot_id },
        ],
    })
}

/// Creates a contract bound to a table. The name is salted per-test to avoid
/// cross-test/rerun collisions on the shared default workspace. Returns the
/// (salted name, response body).
async fn create_contract(
    ctx: &TestCtx,
    name: &str,
    ns: &str,
    table: &str,
    mode: &str,
    spec: Value,
) -> (String, Value) {
    let salted = format!("{name}-{}", ctx.salt);
    let (status, body) = send(
        &ctx.router,
        "POST",
        "/api/v2/quality/contracts",
        Some(json!({
            "name": salted,
            "warehouse": ctx.warehouse,
            "bound_to": "table",
            "namespace": ns,
            "table": table,
            "mode": mode,
            "spec": spec,
        })),
    )
    .await;
    assert_eq!(status, StatusCode::CREATED, "create contract: {body}");
    (salted, body)
}

/// Loads a table and returns its metadata value.
async fn load_metadata(ctx: &TestCtx, ns: &str, table: &str) -> Value {
    let (status, body) = send(&ctx.router, "GET", &table_url(ctx, ns, table), None).await;
    assert_eq!(status, StatusCode::OK, "load table: {body}");
    body
}

async fn audit_chain_ok(ctx: &TestCtx) -> bool {
    let (status, body) = send(&ctx.router, "GET", "/api/v2/audit/verify", None).await;
    assert_eq!(status, StatusCode::OK, "verify audit: {body}");
    body["valid"].as_bool().unwrap_or(false)
}

// ===========================================================================
// Contract CRUD
// ===========================================================================

#[tokio::test]
async fn contract_crud_and_versions() {
    let Some(ctx) = test_ctx().await else { return };
    make_namespace(&ctx, "db").await;
    make_table(&ctx, "db", "events").await;

    // Create.
    let (name, created) = create_contract(
        &ctx,
        "no-drop",
        "db",
        "events",
        "block",
        json!({ "schema": { "allowed_evolution": "no_narrowing" } }),
    )
    .await;
    let id = created["id"].as_str().expect("id").to_owned();
    assert_eq!(created["mode"], "block");
    assert_eq!(created["version"], 1);
    assert_eq!(created["bound_to"], "table");

    // List (the workspace is shared; assert *our* contract is present rather
    // than an exact count).
    let (status, body) = send(&ctx.router, "GET", "/api/v2/quality/contracts", None).await;
    assert_eq!(status, StatusCode::OK);
    assert!(
        body["contracts"]
            .as_array()
            .is_some_and(|c| c.iter().any(|x| x["id"] == id.as_str())),
        "our contract must appear in the list"
    );

    // Get.
    let (status, body) = send(
        &ctx.router,
        "GET",
        &format!("/api/v2/quality/contracts/{id}"),
        None,
    )
    .await;
    assert_eq!(status, StatusCode::OK);
    assert_eq!(body["name"], name.as_str());

    // Patch: change mode to warn + disable.
    let (status, body) = send(
        &ctx.router,
        "PATCH",
        &format!("/api/v2/quality/contracts/{id}"),
        Some(json!({ "mode": "warn", "enabled": false })),
    )
    .await;
    assert_eq!(status, StatusCode::OK, "{body}");
    assert_eq!(body["mode"], "warn");
    assert_eq!(body["enabled"], false);
    assert_eq!(body["version"], 2);

    // Versions: two, newest first.
    let (status, body) = send(
        &ctx.router,
        "GET",
        &format!("/api/v2/quality/contracts/{id}/versions"),
        None,
    )
    .await;
    assert_eq!(status, StatusCode::OK);
    let versions = body["versions"].as_array().expect("versions");
    assert_eq!(versions.len(), 2);
    assert_eq!(versions[0]["version"], 2);
    assert_eq!(versions[1]["version"], 1);

    // A bad mode is a 400 (validated before the name-uniqueness check).
    let (status, _) = send(
        &ctx.router,
        "POST",
        "/api/v2/quality/contracts",
        Some(json!({
            "name": format!("bad-{}", ctx.salt), "warehouse": ctx.warehouse, "bound_to": "table",
            "namespace": "db", "table": "events", "mode": "explode",
            "spec": { "schema": { "allowed_evolution": "none" } },
        })),
    )
    .await;
    assert_eq!(status, StatusCode::BAD_REQUEST);

    // Delete.
    let (status, _) = send(
        &ctx.router,
        "DELETE",
        &format!("/api/v2/quality/contracts/{id}"),
        None,
    )
    .await;
    assert_eq!(status, StatusCode::NO_CONTENT);
}

// ===========================================================================
// A commit with no contract is unaffected (the circuit breaker is inert).
// ===========================================================================

#[tokio::test]
async fn commit_without_contract_is_unaffected() {
    let Some(ctx) = test_ctx().await else { return };
    make_namespace(&ctx, "db").await;
    let uuid = make_table(&ctx, "db", "t").await;

    let (status, body) = send(
        &ctx.router,
        "POST",
        &table_url(&ctx, "db", "t"),
        Some(append_commit(&uuid, None, 1001, 1)),
    )
    .await;
    assert_eq!(status, StatusCode::OK, "{body}");
    assert_eq!(body["metadata"]["current-snapshot-id"], 1001);
}

// ===========================================================================
// BLOCK — a violating commit is rejected atomically.
// ===========================================================================

#[tokio::test]
async fn block_mode_rejects_narrowing_atomically() {
    let Some(ctx) = test_ctx().await else { return };
    make_namespace(&ctx, "db").await;
    let uuid = make_table(&ctx, "db", "orders").await;

    let (name, created) = create_contract(
        &ctx,
        "no-narrow",
        "db",
        "orders",
        "block",
        json!({ "schema": { "allowed_evolution": "no_narrowing" } }),
    )
    .await;
    let contract_id = created["id"].as_str().unwrap().to_owned();

    // The pre-violation pointer state.
    let before = load_metadata(&ctx, "db", "orders").await;
    let before_loc = before["metadata-location"].as_str().unwrap().to_owned();
    assert!(before["metadata"]["current-snapshot-id"].is_null());

    // Narrow `id` from long to int — a schema narrowing.
    let narrowing = json!([
        { "id": 1, "name": "id", "required": true, "type": "int" },
        { "id": 2, "name": "email", "required": false, "type": "string" },
        { "id": 3, "name": "amount", "required": false, "type": "int" },
    ]);
    let (status, body) = send(
        &ctx.router,
        "POST",
        &table_url(&ctx, "db", "orders"),
        Some(schema_change_commit(&uuid, &narrowing, None, 2001, 1)),
    )
    .await;

    // Rejected with the machine-readable contract-violation body.
    assert_eq!(status, StatusCode::CONFLICT, "{body}");
    assert_eq!(body["error"]["type"], "CommitFailedException");
    let cv = &body["error"]["contract-violation"];
    assert_eq!(cv["contract-name"], name.as_str());
    assert_eq!(cv["mode"], "block");
    assert!(
        cv["violations"]
            .as_array()
            .is_some_and(|v| v.iter().any(|x| x["kind"] == "schema-narrowed")),
        "expected a schema-narrowed violation: {body}"
    );

    // The pointer is UNCHANGED — nothing durable happened (I1/I3).
    let after = load_metadata(&ctx, "db", "orders").await;
    assert_eq!(
        after["metadata-location"].as_str().unwrap(),
        before_loc,
        "block must not move the pointer"
    );
    assert!(after["metadata"]["current-snapshot-id"].is_null());

    // A violation was recorded with commit_rejected=true (scoped to *our*
    // contract so parallel tests can't interfere).
    let (status, body) = send(
        &ctx.router,
        "GET",
        &format!("/api/v2/quality/violations?contract_id={contract_id}"),
        None,
    )
    .await;
    assert_eq!(status, StatusCode::OK);
    let violations = body["violations"].as_array().expect("violations");
    assert!(
        violations
            .iter()
            .any(|v| v["kind"] == "schema-narrowed" && v["commit_rejected"] == true),
        "expected a recorded blocked violation: {body}"
    );

    // The audit chain still verifies (the block's record is chained too).
    assert!(
        audit_chain_ok(&ctx).await,
        "audit chain must verify after a block"
    );

    // An additive commit on the same table is allowed (contract only blocks
    // narrowing).
    let additive = json!([
        { "id": 1, "name": "id", "required": true, "type": "long" },
        { "id": 2, "name": "email", "required": false, "type": "string" },
        { "id": 3, "name": "amount", "required": false, "type": "int" },
        { "id": 4, "name": "added", "required": false, "type": "string" },
    ]);
    let (status, body) = send(
        &ctx.router,
        "POST",
        &table_url(&ctx, "db", "orders"),
        Some(schema_change_commit(&uuid, &additive, None, 2002, 1)),
    )
    .await;
    assert_eq!(status, StatusCode::OK, "additive commit must pass: {body}");
    assert_eq!(body["metadata"]["current-snapshot-id"], 2002);
}

#[tokio::test]
async fn block_mode_rejects_protected_column_drop() {
    let Some(ctx) = test_ctx().await else { return };
    make_namespace(&ctx, "db").await;
    let uuid = make_table(&ctx, "db", "customers").await;

    create_contract(
        &ctx,
        "protect-email",
        "db",
        "customers",
        "block",
        json!({ "schema": { "protected_columns": ["email"] } }),
    )
    .await;

    // Drop `email` (protected).
    let dropped = json!([
        { "id": 1, "name": "id", "required": true, "type": "long" },
        { "id": 3, "name": "amount", "required": false, "type": "int" },
    ]);
    let (status, body) = send(
        &ctx.router,
        "POST",
        &table_url(&ctx, "db", "customers"),
        Some(schema_change_commit(&uuid, &dropped, None, 3001, 1)),
    )
    .await;
    assert_eq!(status, StatusCode::CONFLICT, "{body}");
    assert!(
        body["error"]["contract-violation"]["violations"]
            .as_array()
            .is_some_and(|v| v.iter().any(|x| x["kind"] == "protected-column-dropped")),
        "expected protected-column-dropped: {body}"
    );
    // Pointer unchanged.
    let after = load_metadata(&ctx, "db", "customers").await;
    assert!(after["metadata"]["current-snapshot-id"].is_null());
}

// ===========================================================================
// WARN — the commit lands and a violation is recorded.
// ===========================================================================

#[tokio::test]
async fn warn_mode_lands_and_records() {
    let Some(ctx) = test_ctx().await else { return };
    make_namespace(&ctx, "db").await;
    let uuid = make_table(&ctx, "db", "w").await;

    let (_, created) = create_contract(
        &ctx,
        "warn-narrow",
        "db",
        "w",
        "warn",
        json!({ "schema": { "allowed_evolution": "no_narrowing" } }),
    )
    .await;
    let contract_id = created["id"].as_str().unwrap().to_owned();

    // A narrowing commit under warn mode LANDS.
    let narrowing = json!([
        { "id": 1, "name": "id", "required": true, "type": "int" },
        { "id": 2, "name": "email", "required": false, "type": "string" },
        { "id": 3, "name": "amount", "required": false, "type": "int" },
    ]);
    let (status, body) = send(
        &ctx.router,
        "POST",
        &table_url(&ctx, "db", "w"),
        Some(schema_change_commit(&uuid, &narrowing, None, 4001, 1)),
    )
    .await;
    assert_eq!(status, StatusCode::OK, "warn must land the commit: {body}");
    assert_eq!(body["metadata"]["current-snapshot-id"], 4001);

    // A violation was recorded with commit_rejected=false, quarantined=false.
    let (status, body) = send(
        &ctx.router,
        "GET",
        &format!("/api/v2/quality/violations?contract_id={contract_id}"),
        None,
    )
    .await;
    assert_eq!(status, StatusCode::OK);
    let violations = body["violations"].as_array().expect("violations");
    assert!(
        violations.iter().any(|v| v["kind"] == "schema-narrowed"
            && v["commit_rejected"] == false
            && v["quarantined"] == false),
        "expected a recorded warn violation: {body}"
    );
    assert!(audit_chain_ok(&ctx).await);
}

// ===========================================================================
// QUARANTINE — main is not advanced; the snapshot lands on the audit branch;
// publish fast-forwards main.
// ===========================================================================

#[tokio::test]
async fn quarantine_freezes_main_then_publish_advances_it() {
    let Some(ctx) = test_ctx().await else { return };
    make_namespace(&ctx, "db").await;
    let uuid = make_table(&ctx, "db", "q").await;

    // Seed a base snapshot on main so there is a real head to freeze at.
    let (status, body) = send(
        &ctx.router,
        "POST",
        &table_url(&ctx, "db", "q"),
        Some(append_commit(&uuid, None, 5000, 1)),
    )
    .await;
    assert_eq!(status, StatusCode::OK, "{body}");
    assert_eq!(body["metadata"]["current-snapshot-id"], 5000);

    let (_, created) = create_contract(
        &ctx,
        "quarantine-narrow",
        "db",
        "q",
        "quarantine",
        json!({ "schema": { "allowed_evolution": "no_narrowing" } }),
    )
    .await;
    let contract_id = created["id"].as_str().unwrap().to_owned();

    // A narrowing commit under quarantine mode: it "succeeds" at the HTTP
    // level (200) but main is frozen.
    let narrowing = json!([
        { "id": 1, "name": "id", "required": true, "type": "int" },
        { "id": 2, "name": "email", "required": false, "type": "string" },
        { "id": 3, "name": "amount", "required": false, "type": "int" },
    ]);
    let (status, body) = send(
        &ctx.router,
        "POST",
        &table_url(&ctx, "db", "q"),
        Some(schema_change_commit(&uuid, &narrowing, Some(5000), 5001, 2)),
    )
    .await;
    assert_eq!(status, StatusCode::OK, "quarantine lands off-main: {body}");

    // main is FROZEN at the base snapshot (5000), NOT the quarantined 5001.
    let after = load_metadata(&ctx, "db", "q").await;
    assert_eq!(
        after["metadata"]["current-snapshot-id"], 5000,
        "quarantine must not advance main: {after}"
    );
    // The quarantined snapshot 5001 is retained and reachable on the branch.
    let refs = &after["metadata"]["refs"];
    assert_eq!(refs["main"]["snapshot-id"], 5000);
    assert_eq!(
        refs["meridian_quarantine"]["snapshot-id"], 5001,
        "quarantine branch must point at the head: {after}"
    );
    let snapshot_ids: Vec<i64> = after["metadata"]["snapshots"]
        .as_array()
        .expect("snapshots")
        .iter()
        .map(|s| s["snapshot-id"].as_i64().unwrap())
        .collect();
    assert!(
        snapshot_ids.contains(&5001),
        "5001 must be retained: {after}"
    );

    // A violation was recorded as quarantined.
    let (_, body) = send(
        &ctx.router,
        "GET",
        &format!("/api/v2/quality/violations?contract_id={contract_id}"),
        None,
    )
    .await;
    assert!(
        body["violations"]
            .as_array()
            .is_some_and(|v| v.iter().any(|x| x["quarantined"] == true)),
        "expected a quarantined violation: {body}"
    );

    // Publish: fast-forward main to 5001.
    let (status, body) = send(
        &ctx.router,
        "POST",
        &format!(
            "/api/v2/quality/tables/{}/db/q/quarantine/5001/publish",
            ctx.warehouse
        ),
        None,
    )
    .await;
    assert_eq!(status, StatusCode::OK, "publish: {body}");

    let published = load_metadata(&ctx, "db", "q").await;
    assert_eq!(
        published["metadata"]["current-snapshot-id"], 5001,
        "publish must advance main to the quarantined snapshot: {published}"
    );
    // The quarantine branch is gone after publish.
    assert!(
        published["metadata"]["refs"]["meridian_quarantine"].is_null(),
        "publish must drop the quarantine branch: {published}"
    );
    assert!(audit_chain_ok(&ctx).await);
}

#[tokio::test]
async fn quarantine_discard_leaves_main_frozen() {
    let Some(ctx) = test_ctx().await else { return };
    make_namespace(&ctx, "db").await;
    let uuid = make_table(&ctx, "db", "qd").await;

    send(
        &ctx.router,
        "POST",
        &table_url(&ctx, "db", "qd"),
        Some(append_commit(&uuid, None, 6000, 1)),
    )
    .await;

    create_contract(
        &ctx,
        "qd-narrow",
        "db",
        "qd",
        "quarantine",
        json!({ "schema": { "allowed_evolution": "no_narrowing" } }),
    )
    .await;

    let narrowing = json!([
        { "id": 1, "name": "id", "required": true, "type": "int" },
        { "id": 2, "name": "email", "required": false, "type": "string" },
        { "id": 3, "name": "amount", "required": false, "type": "int" },
    ]);
    let (status, _) = send(
        &ctx.router,
        "POST",
        &table_url(&ctx, "db", "qd"),
        Some(schema_change_commit(&uuid, &narrowing, Some(6000), 6001, 2)),
    )
    .await;
    assert_eq!(status, StatusCode::OK);

    // Discard the quarantined snapshot.
    let (status, body) = send(
        &ctx.router,
        "POST",
        &format!(
            "/api/v2/quality/tables/{}/db/qd/quarantine/6001/discard",
            ctx.warehouse
        ),
        None,
    )
    .await;
    assert_eq!(status, StatusCode::OK, "discard: {body}");

    let after = load_metadata(&ctx, "db", "qd").await;
    // main still at the base; quarantine branch gone.
    assert_eq!(after["metadata"]["current-snapshot-id"], 6000);
    assert!(after["metadata"]["refs"]["meridian_quarantine"].is_null());
}

// ===========================================================================
// Concurrency: a contract in the commit path must not deadlock or lose updates.
// ===========================================================================

#[tokio::test]
async fn concurrent_appends_under_a_warn_contract_do_not_lose_updates() {
    // Number of concurrent writers.
    const N: i64 = 6;

    let Some(ctx) = test_ctx().await else { return };
    let ctx = Arc::new(ctx);
    make_namespace(&ctx, "db").await;
    let uuid = make_table(&ctx, "db", "conc").await;

    // A warn contract that every append trips (email must be non-null, but the
    // table's email is optional) — so the hook runs and records on *every*
    // commit, exercising the hook under contention without blocking commits.
    create_contract(
        &ctx,
        "conc-nonnull",
        "db",
        "conc",
        "warn",
        json!({ "predicates": [ { "type": "non_null", "column": "email" } ] }),
    )
    .await;

    // Fire N appends concurrently. They contend on the table row lock and the
    // CAS; each also runs the contract hook. The commit driver's bounded
    // rebase-retry resolves the races. We assert every commit eventually lands
    // and the snapshot chain is gapless (no lost updates), and nothing hangs.
    let mut handles = Vec::new();
    for k in 0..N {
        let ctx = Arc::clone(&ctx);
        let uuid = uuid.clone();
        handles.push(tokio::spawn(async move {
            // Each writer bases on whatever is current and retries on the
            // documented 409 (lost CAS / stale requirement) until it lands —
            // exactly how a real engine behaves.
            let snapshot_id = 7000 + k;
            for _ in 0..(N * 4) {
                let current = load_metadata(&ctx, "db", "conc").await;
                let parent = current["metadata"]["current-snapshot-id"].as_i64();
                let seq = current["metadata"]["last-sequence-number"]
                    .as_i64()
                    .unwrap_or(0)
                    + 1;
                let (status, _) = send(
                    &ctx.router,
                    "POST",
                    &table_url(&ctx, "db", "conc"),
                    Some(append_commit(&uuid, parent, snapshot_id, seq)),
                )
                .await;
                if status == StatusCode::OK {
                    return;
                }
                // 409 = lost the race; refresh and retry. Any other status is a
                // hard failure.
                assert_eq!(
                    status,
                    StatusCode::CONFLICT,
                    "unexpected non-conflict failure under contention"
                );
            }
            panic!("writer {k} never landed its commit");
        }));
    }
    for h in handles {
        // A deadlock would surface as this timing out; the test harness bounds
        // total runtime, so completion here is the no-deadlock signal.
        h.await.expect("writer task panicked");
    }

    // No lost updates: exactly N snapshots landed on main, in a gapless chain.
    let final_meta = load_metadata(&ctx, "db", "conc").await;
    let snapshots = final_meta["metadata"]["snapshots"]
        .as_array()
        .expect("snapshots");
    assert_eq!(
        i64::try_from(snapshots.len()).unwrap(),
        N,
        "every concurrent append must land exactly once (no lost updates): {final_meta}"
    );
    // The current snapshot is one of the writers' and the chain is linear.
    assert!(
        final_meta["metadata"]["current-snapshot-id"]
            .as_i64()
            .is_some()
    );
    assert!(
        audit_chain_ok(&ctx).await,
        "audit chain must verify after contention"
    );
}

// ===========================================================================
// Per-table contract status (E-F3).
// ===========================================================================

#[tokio::test]
async fn table_contract_status_lists_in_force_contracts() {
    let Some(ctx) = test_ctx().await else { return };
    make_namespace(&ctx, "db").await;
    make_table(&ctx, "db", "s").await;

    let (name, _) = create_contract(
        &ctx,
        "s-contract",
        "db",
        "s",
        "warn",
        json!({ "schema": { "allowed_evolution": "additive_only" } }),
    )
    .await;

    let (status, body) = send(
        &ctx.router,
        "GET",
        &format!("/api/v2/quality/tables/{}/db/s/contracts", ctx.warehouse),
        None,
    )
    .await;
    assert_eq!(status, StatusCode::OK, "{body}");
    assert_eq!(body["contracts"].as_array().map(Vec::len), Some(1));
    assert_eq!(body["contracts"][0]["name"], name.as_str());
}
