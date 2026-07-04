//! Router-level integration tests for catalog-level branches & tags (Pillar K).
//!
//! Require a running Postgres and `DATABASE_URL`; without it they skip. Every
//! test provisions its own uniquely-named warehouse rooted in its own tempdir
//! (`file://` storage), so tests are isolated from each other and from previous
//! runs, and can assert on real branch metadata files.
//!
//! The correctness bar (from the milestone brief):
//! - create a branch, commit to it via the `warehouse@branch` IRC prefix →
//!   main is UNCHANGED, the branch pointer advanced;
//! - diff shows the delta;
//! - merge with no conflict → main fast-forwards;
//! - a conflicting merge is detected and refused;
//! - branch-as-catalog: a load via `warehouse@branch` returns the branch
//!   metadata, a load via `warehouse` (main) returns the base;
//! - a merge gate blocks a merge when a contract fails on the branch head;
//! - the commit property discipline holds (no lost updates on concurrent
//!   branch + main commits).

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
    salt: String,
}

async fn test_ctx() -> Option<TestCtx> {
    let Ok(url) = std::env::var("DATABASE_URL") else {
        eprintln!("skipping branching API test: DATABASE_URL is not set");
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
    assert_eq!(status, StatusCode::CREATED, "create warehouse: {body}");
    Some(TestCtx {
        router,
        pool,
        _root: root,
        warehouse,
        salt: Ulid::new().to_string().to_lowercase(),
    })
}

/// Sends one request through the full middleware stack. `idem` is an optional
/// Idempotency-Key. Returns (status, parsed JSON body).
async fn send(
    router: &Router,
    method: &str,
    uri: &str,
    idem: Option<&str>,
    body: Option<Value>,
) -> (StatusCode, Value) {
    let mut builder = Request::builder().method(method).uri(uri);
    if let Some(key) = idem {
        builder = builder.header("Idempotency-Key", key);
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
    let value = if bytes.is_empty() {
        Value::Null
    } else {
        serde_json::from_slice(&bytes).expect("response body is JSON")
    };
    (status, value)
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

async fn make_namespace(ctx: &TestCtx, name: &str) {
    let (status, body) = send(
        &ctx.router,
        "POST",
        &format!("/v1/{}/namespaces", ctx.warehouse),
        None,
        Some(json!({ "namespace": [name] })),
    )
    .await;
    assert_eq!(status, StatusCode::OK, "create namespace: {body}");
}

/// Creates a table on main, returns its uuid.
async fn make_table(ctx: &TestCtx, ns: &str, name: &str) -> String {
    let (status, body) = send(
        &ctx.router,
        "POST",
        &format!("/v1/{}/namespaces/{ns}/tables", ctx.warehouse),
        None,
        Some(json!({ "name": name, "schema": simple_schema() })),
    )
    .await;
    assert_eq!(status, StatusCode::OK, "create table: {body}");
    body["metadata"]["table-uuid"]
        .as_str()
        .expect("table-uuid")
        .to_owned()
}

/// The commit body for an append that moves `main` to `snapshot_id`, with the
/// optimistic requirement that `main` is currently at `parent`.
fn append_commit_body(
    table_uuid: &str,
    parent: Option<i64>,
    snapshot_id: i64,
    seq: i64,
    total_records: i64,
) -> Value {
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
                "summary": { "operation": "append", "total-records": total_records.to_string() },
                "schema-id": 0,
            }},
            { "action": "set-snapshot-ref", "ref-name": "main",
              "type": "branch", "snapshot-id": snapshot_id },
        ],
    })
}

/// A commit body that drops the `payload` column (schema-only) — trips a
/// protected-column contract.
fn drop_payload_body(table_uuid: &str) -> Value {
    json!({
        "requirements": [ { "type": "assert-table-uuid", "uuid": table_uuid } ],
        "updates": [
            { "action": "add-schema", "schema": {
                "type": "struct",
                "fields": [ { "id": 1, "name": "id", "required": true, "type": "long" } ],
            }},
            { "action": "set-current-schema", "schema-id": -1 },
        ],
    })
}

fn main_table_url(ctx: &TestCtx, ns: &str, name: &str) -> String {
    format!("/v1/{}/namespaces/{ns}/tables/{name}", ctx.warehouse)
}

fn branch_table_url(ctx: &TestCtx, branch: &str, ns: &str, name: &str) -> String {
    format!(
        "/v1/{}@{branch}/namespaces/{ns}/tables/{name}",
        ctx.warehouse
    )
}

async fn create_branch(ctx: &TestCtx, name: &str) -> Value {
    let (status, body) = send(
        &ctx.router,
        "POST",
        "/api/v2/branches",
        None,
        Some(json!({ "name": name })),
    )
    .await;
    assert_eq!(status, StatusCode::CREATED, "create branch: {body}");
    body
}

/// main's pointer (`metadata_location`, `pointer_version`) straight from the
/// store.
async fn main_pointer(ctx: &TestCtx, uuid: &str) -> (String, i64) {
    sqlx::query_as("SELECT metadata_location, pointer_version FROM tables WHERE table_uuid = $1")
        .bind(uuid)
        .fetch_one(&ctx.pool)
        .await
        .expect("main pointer")
}

// ===========================================================================
// K-F1 / K-F2: commit to a branch → main unchanged, branch advanced
// ===========================================================================

#[tokio::test]
async fn branch_commit_advances_branch_and_leaves_main_unchanged() {
    let Some(ctx) = test_ctx().await else { return };
    make_namespace(&ctx, "db").await;
    let uuid = make_table(&ctx, "db", "t").await;
    let branch = format!("dev-{}", ctx.salt);
    create_branch(&ctx, &branch).await;

    let (main_loc_before, main_ver_before) = main_pointer(&ctx, &uuid).await;

    // Commit an append via the branch-as-catalog prefix (warehouse@dev).
    let (status, body) = send(
        &ctx.router,
        "POST",
        &branch_table_url(&ctx, &branch, "db", "t"),
        None,
        Some(append_commit_body(&uuid, None, 5001, 1, 10)),
    )
    .await;
    assert_eq!(status, StatusCode::OK, "branch commit: {body}");
    assert_eq!(body["metadata"]["current-snapshot-id"], 5001);

    // main is UNCHANGED: same pointer version, same metadata location.
    let (main_loc_after, main_ver_after) = main_pointer(&ctx, &uuid).await;
    assert_eq!(
        (main_loc_before, main_ver_before),
        (main_loc_after, main_ver_after),
        "a branch commit must not move main"
    );

    // A load via main still shows the empty base (no current snapshot).
    let (status, main_body) = send(
        &ctx.router,
        "GET",
        &main_table_url(&ctx, "db", "t"),
        None,
        None,
    )
    .await;
    assert_eq!(status, StatusCode::OK);
    assert!(
        main_body["metadata"]["current-snapshot-id"].is_null()
            || main_body["metadata"]["current-snapshot-id"] == json!(-1),
        "main must still be empty: {main_body}"
    );

    // A load via the branch prefix shows the branch head (K-F2).
    let (status, branch_body) = send(
        &ctx.router,
        "GET",
        &branch_table_url(&ctx, &branch, "db", "t"),
        None,
        None,
    )
    .await;
    assert_eq!(status, StatusCode::OK);
    assert_eq!(
        branch_body["metadata"]["current-snapshot-id"], 5001,
        "the branch load must return the branch pointer: {branch_body}"
    );

    // A second branch commit advances the branch-local version and chains onto
    // the branch head (parent = 5001), still not touching main.
    let (status, body) = send(
        &ctx.router,
        "POST",
        &branch_table_url(&ctx, &branch, "db", "t"),
        None,
        Some(append_commit_body(&uuid, Some(5001), 5002, 2, 20)),
    )
    .await;
    assert_eq!(status, StatusCode::OK, "second branch commit: {body}");
    assert_eq!(body["metadata"]["current-snapshot-id"], 5002);
    let (_, main_ver_final) = main_pointer(&ctx, &uuid).await;
    assert_eq!(main_ver_before, main_ver_final, "main still untouched");

    // The branch pointer diverged and advanced to branch-local version 1
    // (0 on first divergence, +1 on the second commit).
    let branch_ver: i64 = sqlx::query_scalar(
        "SELECT btp.pointer_version FROM branch_table_pointers btp
         JOIN tables t ON t.id = btp.table_id
         JOIN catalog_branches b ON b.id = btp.branch_id
         WHERE t.table_uuid = $1 AND b.name = $2",
    )
    .bind(&uuid)
    .bind(&branch)
    .fetch_one(&ctx.pool)
    .await
    .expect("branch pointer version");
    assert_eq!(branch_ver, 1, "branch-local version: 0 then 1");

    assert!(audit_chain_ok(&ctx).await, "audit chain must stay valid");
}

// ===========================================================================
// K-F2: branch-as-catalog isolation (branch load vs main load)
// ===========================================================================

#[tokio::test]
async fn undiverged_table_falls_through_to_main_on_branch() {
    let Some(ctx) = test_ctx().await else { return };
    make_namespace(&ctx, "db").await;
    let uuid = make_table(&ctx, "db", "t").await;
    // Advance main once.
    let (status, _) = send(
        &ctx.router,
        "POST",
        &main_table_url(&ctx, "db", "t"),
        None,
        Some(append_commit_body(&uuid, None, 6001, 1, 5)),
    )
    .await;
    assert_eq!(status, StatusCode::OK);

    let branch = format!("dev-{}", ctx.salt);
    create_branch(&ctx, &branch).await;

    // The table has NOT diverged on the branch: a branch load falls through to
    // main's current pointer (zero-copy).
    let (status, branch_body) = send(
        &ctx.router,
        "GET",
        &branch_table_url(&ctx, &branch, "db", "t"),
        None,
        None,
    )
    .await;
    assert_eq!(status, StatusCode::OK);
    assert_eq!(
        branch_body["metadata"]["current-snapshot-id"], 6001,
        "an undiverged table on a branch reads main: {branch_body}"
    );
    // No branch pointer row exists for it.
    let rows: i64 = sqlx::query_scalar(
        "SELECT count(*) FROM branch_table_pointers btp
         JOIN tables t ON t.id = btp.table_id WHERE t.table_uuid = $1",
    )
    .bind(&uuid)
    .fetch_one(&ctx.pool)
    .await
    .expect("count");
    assert_eq!(rows, 0, "no divergence until a branch commit");
}

// ===========================================================================
// K-F1: diff
// ===========================================================================

#[tokio::test]
async fn diff_reports_snapshot_and_row_delta() {
    let Some(ctx) = test_ctx().await else { return };
    make_namespace(&ctx, "db").await;
    let uuid = make_table(&ctx, "db", "t").await;
    let branch = format!("dev-{}", ctx.salt);
    create_branch(&ctx, &branch).await;

    // Branch commit: 100 rows.
    send(
        &ctx.router,
        "POST",
        &branch_table_url(&ctx, &branch, "db", "t"),
        None,
        Some(append_commit_body(&uuid, None, 7001, 1, 100)),
    )
    .await;

    let (status, diff) = send(
        &ctx.router,
        "GET",
        &format!("/api/v2/branches/{branch}/diff"),
        None,
        None,
    )
    .await;
    assert_eq!(status, StatusCode::OK, "diff: {diff}");
    assert_eq!(diff["diverged_table_count"], 1);
    let table = &diff["tables"][0];
    assert_eq!(table["table"], "db.t");
    assert_eq!(table["snapshot"]["branch_snapshot_id"], 7001);
    assert!(
        table["snapshot"]["base_snapshot_id"].is_null(),
        "main is empty"
    );
    // main had no snapshot (base rows unknown), branch has 100.
    assert_eq!(table["rows"]["branch"], 100);
}

// ===========================================================================
// K-F1: merge with no conflict → main fast-forwards
// ===========================================================================

#[tokio::test]
async fn merge_fast_forwards_main_when_no_conflict() {
    let Some(ctx) = test_ctx().await else { return };
    make_namespace(&ctx, "db").await;
    let uuid = make_table(&ctx, "db", "t").await;
    let branch = format!("dev-{}", ctx.salt);
    create_branch(&ctx, &branch).await;

    // Branch commit; main never moves.
    send(
        &ctx.router,
        "POST",
        &branch_table_url(&ctx, &branch, "db", "t"),
        None,
        Some(append_commit_body(&uuid, None, 8001, 1, 42)),
    )
    .await;
    let (_, main_ver_before) = main_pointer(&ctx, &uuid).await;

    // Merge.
    let (status, body) = send(
        &ctx.router,
        "POST",
        &format!("/api/v2/branches/{branch}/merge"),
        None,
        None,
    )
    .await;
    assert_eq!(status, StatusCode::OK, "merge: {body}");
    assert_eq!(body["merged_tables"], json!(["db.t"]));

    // main now shows the branch head, and its pointer version advanced by one
    // (fast-forward through the commit path).
    let (status, main_body) = send(
        &ctx.router,
        "GET",
        &main_table_url(&ctx, "db", "t"),
        None,
        None,
    )
    .await;
    assert_eq!(status, StatusCode::OK);
    assert_eq!(
        main_body["metadata"]["current-snapshot-id"], 8001,
        "main fast-forwarded to the branch head: {main_body}"
    );
    let (_, main_ver_after) = main_pointer(&ctx, &uuid).await;
    assert_eq!(
        main_ver_after,
        main_ver_before + 1,
        "one main commit applied"
    );

    // The merged snapshot is now in main's write-through index (merging runs
    // the main commit path, which indexes).
    let indexed: i64 = sqlx::query_scalar(
        "SELECT count(*) FROM table_snapshots ts JOIN tables t ON t.id = ts.table_id
         WHERE t.table_uuid = $1 AND ts.snapshot_id = 8001",
    )
    .bind(&uuid)
    .fetch_one(&ctx.pool)
    .await
    .expect("index");
    assert_eq!(indexed, 1, "merged snapshot indexed on main");

    // The branch is now marked merged.
    let (_, list) = send(&ctx.router, "GET", "/api/v2/branches", None, None).await;
    let branch_states: Vec<&str> = list["branches"]
        .as_array()
        .unwrap()
        .iter()
        .filter(|b| b["name"] == json!(branch))
        .map(|b| b["state"].as_str().unwrap())
        .collect();
    assert_eq!(branch_states, vec!["merged"]);

    assert!(audit_chain_ok(&ctx).await);
}

// ===========================================================================
// K-F1: conflicting merge is detected and refused
// ===========================================================================

#[tokio::test]
async fn conflicting_merge_is_detected_and_refused() {
    let Some(ctx) = test_ctx().await else { return };
    make_namespace(&ctx, "db").await;
    let uuid = make_table(&ctx, "db", "t").await;
    let branch = format!("dev-{}", ctx.salt);
    create_branch(&ctx, &branch).await;

    // Diverge the table on the branch (base_pointer_version captured = main's 0).
    send(
        &ctx.router,
        "POST",
        &branch_table_url(&ctx, &branch, "db", "t"),
        None,
        Some(append_commit_body(&uuid, None, 9001, 1, 10)),
    )
    .await;

    // Now advance MAIN independently → main moved past the divergence base.
    let (status, _) = send(
        &ctx.router,
        "POST",
        &main_table_url(&ctx, "db", "t"),
        None,
        Some(append_commit_body(&uuid, None, 9500, 1, 7)),
    )
    .await;
    assert_eq!(status, StatusCode::OK);

    // Merge must be refused: the table changed on both sides.
    let (status, body) = send(
        &ctx.router,
        "POST",
        &format!("/api/v2/branches/{branch}/merge"),
        None,
        None,
    )
    .await;
    assert_eq!(status, StatusCode::CONFLICT, "conflict must refuse: {body}");
    assert_eq!(body["error"]["type"], "CommitFailedException");
    assert!(
        body["error"]["message"]
            .as_str()
            .unwrap()
            .contains("conflict"),
        "message names the conflict: {body}"
    );

    // main is unchanged by the refused merge (still at snapshot 9500).
    let (_, main_body) = send(
        &ctx.router,
        "GET",
        &main_table_url(&ctx, "db", "t"),
        None,
        None,
    )
    .await;
    assert_eq!(main_body["metadata"]["current-snapshot-id"], 9500);
}

// ===========================================================================
// K-F3: merge gate blocks a merge when a contract fails on the branch head
// ===========================================================================

#[tokio::test]
async fn merge_gate_blocks_merge_when_contract_fails() {
    let Some(ctx) = test_ctx().await else { return };
    make_namespace(&ctx, "db").await;
    let uuid = make_table(&ctx, "db", "t").await;

    // A block-mode contract: `payload` is a protected column.
    let contract_name = format!("protect-payload-{}", ctx.salt);
    let (status, cbody) = send(
        &ctx.router,
        "POST",
        "/api/v2/quality/contracts",
        None,
        Some(json!({
            "name": contract_name,
            "warehouse": ctx.warehouse,
            "bound_to": "table",
            "namespace": "db",
            "table": "t",
            "mode": "block",
            "spec": { "schema": { "allowed_evolution": "no_narrowing", "protected_columns": ["payload"] } },
        })),
    )
    .await;
    assert_eq!(status, StatusCode::CREATED, "create contract: {cbody}");

    let branch = format!("dev-{}", ctx.salt);
    create_branch(&ctx, &branch).await;

    // On the branch, drop the protected column (allowed on the branch — the
    // circuit breaker does not run on branch commits; the gate does at merge).
    let (status, body) = send(
        &ctx.router,
        "POST",
        &branch_table_url(&ctx, &branch, "db", "t"),
        None,
        Some(drop_payload_body(&uuid)),
    )
    .await;
    assert_eq!(
        status,
        StatusCode::OK,
        "branch commit dropping payload: {body}"
    );

    // The gate check reports failure.
    let (status, gate) = send(
        &ctx.router,
        "GET",
        &format!("/api/v2/branches/{branch}/gate"),
        None,
        None,
    )
    .await;
    assert_eq!(status, StatusCode::OK, "gate check: {gate}");
    assert_eq!(gate["passes"], false, "gate must fail: {gate}");
    assert_eq!(gate["blocking"][0]["table"], "db.t");
    assert_eq!(gate["blocking"][0]["contract"], contract_name);

    // The merge is refused by the gate before any pointer moves.
    let (status, body) = send(
        &ctx.router,
        "POST",
        &format!("/api/v2/branches/{branch}/merge"),
        None,
        None,
    )
    .await;
    assert_eq!(
        status,
        StatusCode::CONFLICT,
        "gate must block merge: {body}"
    );
    assert!(
        body["error"]["message"].as_str().unwrap().contains("gate"),
        "message names the gate: {body}"
    );

    // main never diverged/moved (the table has no snapshot and its schema still
    // carries payload).
    let (_, main_body) = send(
        &ctx.router,
        "GET",
        &main_table_url(&ctx, "db", "t"),
        None,
        None,
    )
    .await;
    let field_names: Vec<&str> = main_body["metadata"]["schemas"][0]["fields"]
        .as_array()
        .unwrap()
        .iter()
        .map(|f| f["name"].as_str().unwrap())
        .collect();
    assert!(
        field_names.contains(&"payload"),
        "payload preserved on main"
    );
}

// ===========================================================================
// K-F1: tags are immutable
// ===========================================================================

#[tokio::test]
async fn tag_is_read_only_and_pins_state() {
    let Some(ctx) = test_ctx().await else { return };
    make_namespace(&ctx, "db").await;
    let uuid = make_table(&ctx, "db", "t").await;
    let branch = format!("dev-{}", ctx.salt);
    create_branch(&ctx, &branch).await;
    // Diverge on the branch.
    send(
        &ctx.router,
        "POST",
        &branch_table_url(&ctx, &branch, "db", "t"),
        None,
        Some(append_commit_body(&uuid, None, 11001, 1, 3)),
    )
    .await;

    // Tag the branch state.
    let tag = format!("rel-{}", ctx.salt);
    let (status, body) = send(
        &ctx.router,
        "POST",
        "/api/v2/tags",
        None,
        Some(json!({ "name": tag, "from_ref": branch })),
    )
    .await;
    assert_eq!(status, StatusCode::CREATED, "create tag: {body}");
    assert_eq!(body["kind"], "tag");

    // A load via the tag prefix returns the pinned branch state.
    let (status, tbody) = send(
        &ctx.router,
        "GET",
        &branch_table_url(&ctx, &tag, "db", "t"),
        None,
        None,
    )
    .await;
    assert_eq!(status, StatusCode::OK, "tag load: {tbody}");
    assert_eq!(tbody["metadata"]["current-snapshot-id"], 11001);

    // A commit against the tag prefix is refused (immutable).
    let (status, body) = send(
        &ctx.router,
        "POST",
        &branch_table_url(&ctx, &tag, "db", "t"),
        None,
        Some(append_commit_body(&uuid, Some(11001), 11002, 2, 4)),
    )
    .await;
    assert_eq!(
        status,
        StatusCode::CONFLICT,
        "tag commit must be refused: {body}"
    );
}

// ===========================================================================
// Commit-invariant discipline: no lost updates on concurrent branch + main
// ===========================================================================

#[tokio::test]
async fn concurrent_branch_and_main_commits_do_not_lose_updates() {
    let Some(ctx) = test_ctx().await else { return };
    make_namespace(&ctx, "db").await;
    let uuid = make_table(&ctx, "db", "t").await;
    let branch = format!("dev-{}", ctx.salt);
    create_branch(&ctx, &branch).await;

    // Establish the branch divergence first, on its own base (branch-local
    // version 0), so the concurrent phase races two commits on *independent*
    // already-established pointers — the strongest form of the isolation
    // property (the branch CAS advances while main's CAS advances, no
    // cross-clobber). seq 5 leaves room below for main.
    let branch_uri = branch_table_url(&ctx, &branch, "db", "t");
    let (status, _) = send(
        &ctx.router,
        "POST",
        &branch_uri,
        None,
        Some(append_commit_body(&uuid, None, 12500, 5, 2)),
    )
    .await;
    assert_eq!(status, StatusCode::OK);

    // Now fire a main commit (advances main pointer, seq 1) and a second branch
    // commit (advances the branch pointer, chaining onto 12500, seq 6)
    // concurrently. Both must succeed; neither pointer clobbers the other.
    let main_uri = main_table_url(&ctx, "db", "t");
    let main_fut = send(
        &ctx.router,
        "POST",
        &main_uri,
        None,
        Some(append_commit_body(&uuid, None, 12001, 1, 1)),
    );
    let branch_fut = send(
        &ctx.router,
        "POST",
        &branch_uri,
        None,
        Some(append_commit_body(&uuid, Some(12500), 12501, 6, 3)),
    );
    let (main_res, branch_res) = tokio::join!(main_fut, branch_fut);
    assert_eq!(main_res.0, StatusCode::OK, "main commit: {}", main_res.1);
    assert_eq!(
        branch_res.0,
        StatusCode::OK,
        "branch commit: {}",
        branch_res.1
    );

    // main has its own snapshot; the branch has its own head; neither leaked.
    let (_, main_body) = send(
        &ctx.router,
        "GET",
        &main_table_url(&ctx, "db", "t"),
        None,
        None,
    )
    .await;
    assert_eq!(main_body["metadata"]["current-snapshot-id"], 12001);
    let (_, branch_body) = send(
        &ctx.router,
        "GET",
        &branch_table_url(&ctx, &branch, "db", "t"),
        None,
        None,
    )
    .await;
    assert_eq!(branch_body["metadata"]["current-snapshot-id"], 12501);

    assert!(
        audit_chain_ok(&ctx).await,
        "audit chain valid after concurrency"
    );
}

/// Two identical branch commits with the same Idempotency-Key: the second
/// replays the first (no double-apply), the branch advances exactly once.
#[tokio::test]
async fn branch_commit_is_idempotent() {
    let Some(ctx) = test_ctx().await else { return };
    make_namespace(&ctx, "db").await;
    let uuid = make_table(&ctx, "db", "t").await;
    let branch = format!("dev-{}", ctx.salt);
    create_branch(&ctx, &branch).await;

    let key = format!("key-{}", ctx.salt);
    let body = append_commit_body(&uuid, None, 13001, 1, 9);
    let (status1, resp1) = send(
        &ctx.router,
        "POST",
        &branch_table_url(&ctx, &branch, "db", "t"),
        Some(&key),
        Some(body.clone()),
    )
    .await;
    assert_eq!(status1, StatusCode::OK, "first: {resp1}");
    let (status2, resp2) = send(
        &ctx.router,
        "POST",
        &branch_table_url(&ctx, &branch, "db", "t"),
        Some(&key),
        Some(body),
    )
    .await;
    assert_eq!(status2, StatusCode::OK, "replay: {resp2}");
    assert_eq!(
        resp1["metadata-location"], resp2["metadata-location"],
        "replay returns the same metadata"
    );

    // The branch advanced exactly once (version 0, not 1).
    let ver: i64 = sqlx::query_scalar(
        "SELECT btp.pointer_version FROM branch_table_pointers btp
         JOIN tables t ON t.id = btp.table_id
         JOIN catalog_branches b ON b.id = btp.branch_id
         WHERE t.table_uuid = $1 AND b.name = $2",
    )
    .bind(&uuid)
    .bind(&branch)
    .fetch_one(&ctx.pool)
    .await
    .expect("branch version");
    assert_eq!(
        ver, 0,
        "idempotent replay does not double-advance the branch"
    );
}

// ===========================================================================
// Shared helpers
// ===========================================================================

async fn audit_chain_ok(ctx: &TestCtx) -> bool {
    let (status, body) = send(&ctx.router, "GET", "/api/v2/audit/verify", None, None).await;
    assert_eq!(status, StatusCode::OK, "verify audit: {body}");
    body["valid"].as_bool().unwrap_or(false)
}
