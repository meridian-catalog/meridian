//! Integration tests for zero-scan data-quality monitors, incidents, the
//! quality score, and the impact CI gate (Pillar E: E-F1/E-F5/E-F6, Pillar F:
//! F-F5).
//!
//! These require a running Postgres and `DATABASE_URL`; without it they skip.
//! Each test provisions its own uniquely-named warehouse rooted in its own
//! tempdir (`file://` storage), so tests are isolated from each other and from
//! previous runs, and every assertion is scoped to the test's own ids
//! (TEST-ISOLATION).
//!
//! The evaluation worker consumes the *published* `table.committed` stream, so
//! each test drives commits through the HTTP endpoint, publishes the outbox
//! (`outbox::relay_once`) exactly as the real relay would, and then runs the
//! worker's `process_batch` for a deterministic pass (no reliance on the poll
//! loop). Zero-scan is real: the monitors read only the `table_snapshots` index
//! summaries the commits wrote.
//!
//! The bar these hold (from the role brief):
//!
//!  1. **volume-spike monitor**: seed a stable per-commit volume via commits,
//!     then a 100× spike → the worker opens a volume incident;
//!  2. **breaking-schema monitor**: a commit that drops a column → a
//!     breaking-schema incident (an additive change does not);
//!  3. **freshness monitor** (pure): the learned-cadence scorer breaches on a
//!     late commit (covered by the store unit tests; here we assert the monitor
//!     wiring end to end via the results series);
//!  4. **incident lifecycle**: open → acknowledge → resolve, with the ledger
//!     and de-duplication (a second spike re-touches, does not duplicate);
//!  5. **blast radius via lineage**: an incident on an upstream table carries the
//!     downstream asset in `blast_radius`;
//!  6. **quality score**: a bare table scores low; adding a contract + owner +
//!     monitor raises it; a live incident lowers the monitor component;
//!  7. **impact CI gate**: `impact_of` returns the downstream set for a
//!     `drop_column` change (the CLI exit-code logic is unit-tested on top).

use std::sync::Arc;

use axum::Router;
use axum::body::Body;
use axum::http::{Request, StatusCode};
use http_body_util::BodyExt;
use meridian_common::AppConfig;
use meridian_common::config::QualityConfig;
use meridian_server::{AppState, build_router, quality_monitor};
use meridian_store::tenancy;
use serde_json::{Value, json};
use sqlx::PgPool;
use tower::ServiceExt;
use ulid::Ulid;

struct TestCtx {
    router: Router,
    pool: PgPool,
    _root: tempfile::TempDir,
    warehouse: String,
    /// Per-test salt so shared-workspace unique names never collide.
    salt: String,
}

async fn test_ctx() -> Option<TestCtx> {
    let Ok(url) = std::env::var("DATABASE_URL") else {
        eprintln!("skipping quality API test: DATABASE_URL is not set");
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
// Fixtures + helpers
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

/// Creates a table with `base_schema`, optionally with properties, returns its
/// table-uuid.
async fn make_table(ctx: &TestCtx, ns: &str, name: &str, properties: Value) -> String {
    let (status, body) = send(
        &ctx.router,
        "POST",
        &format!("/v1/{}/namespaces/{ns}/tables", ctx.warehouse),
        Some(json!({ "name": name, "schema": base_schema(), "properties": properties })),
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

/// An append commit that writes a given `added-records` + `total-records` into
/// the snapshot summary (the numbers the volume/file-size monitors read).
#[allow(clippy::too_many_arguments)]
fn append_commit(
    uuid: &str,
    parent: Option<i64>,
    snapshot_id: i64,
    seq: i64,
    added_records: i64,
    total_records: i64,
    added_files: i64,
    added_bytes: i64,
) -> Value {
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
                "timestamp-ms": 1_700_000_000_000i64 + seq * 1000,
                "manifest-list": format!("file:///fake/snap-{snapshot_id}.avro"),
                "summary": {
                    "operation": "append",
                    "added-records": added_records.to_string(),
                    "total-records": total_records.to_string(),
                    "added-data-files": added_files.to_string(),
                    "added-files-size": added_bytes.to_string(),
                },
                "schema-id": 0,
            }},
            { "action": "set-snapshot-ref", "ref-name": "main",
              "type": "branch", "snapshot-id": snapshot_id },
        ],
    })
}

/// A commit that replaces the schema (adds a new schema + sets it current) and
/// adds a snapshot so there is a head.
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
            { "type": "assert-ref-snapshot-id", "ref": "main", "snapshot-id": parent },
        ],
        "updates": [
            { "action": "add-schema", "schema": { "type": "struct", "fields": fields } },
            { "action": "set-current-schema", "schema-id": -1 },
            { "action": "add-snapshot", "snapshot": {
                "snapshot-id": snapshot_id,
                "parent-snapshot-id": parent,
                "sequence-number": seq,
                "timestamp-ms": 1_700_000_100_000i64 + seq * 1000,
                "manifest-list": format!("file:///fake/snap-{snapshot_id}.avro"),
                "summary": { "operation": "append", "total-records": "10", "added-records": "10" },
                "schema-id": 0,
            }},
            { "action": "set-snapshot-ref", "ref-name": "main",
              "type": "branch", "snapshot-id": snapshot_id },
        ],
    })
}

/// Commits one request and asserts success.
async fn commit(ctx: &TestCtx, ns: &str, table: &str, body: Value) {
    let (status, resp) = send(&ctx.router, "POST", &table_url(ctx, ns, table), Some(body)).await;
    assert_eq!(status, StatusCode::OK, "commit: {resp}");
}

/// Creates a monitor of `kind` bound to a table. Returns its id.
async fn create_monitor(ctx: &TestCtx, name: &str, ns: &str, table: &str, kind: &str) -> String {
    let salted = format!("{name}-{}", ctx.salt);
    let (status, body) = send(
        &ctx.router,
        "POST",
        "/api/v2/quality/monitors",
        Some(json!({
            "name": salted,
            "warehouse": ctx.warehouse,
            "bound_to": "table",
            "namespace": ns,
            "table": table,
            "kind": kind,
            "severity": "high",
        })),
    )
    .await;
    assert_eq!(status, StatusCode::CREATED, "create monitor: {body}");
    body["id"].as_str().expect("monitor id").to_owned()
}

/// Publishes the outbox (as the relay would) and runs one worker pass, so
/// monitor evaluation is deterministic in the test. Loops the relay until
/// drained, then runs the worker until the committed stream is caught up.
async fn drive_worker(ctx: &TestCtx) {
    // Publish everything currently in the outbox.
    loop {
        let published = meridian_store::outbox::relay_once(&ctx.pool, 500)
            .await
            .expect("relay once");
        if published == 0 {
            break;
        }
    }
    // Drain the committed stream through the monitor worker.
    let config = QualityConfig::default();
    loop {
        let processed =
            quality_monitor::process_batch(&ctx.pool, tenancy::default_workspace_id(), &config)
                .await
                .expect("process batch");
        if processed == 0 {
            break;
        }
    }
}

/// Lists incidents for a table id, returning the array.
async fn incidents_for(ctx: &TestCtx, table_id: &str) -> Vec<Value> {
    let (status, body) = send(
        &ctx.router,
        "GET",
        &format!("/api/v2/quality/incidents?table_id={table_id}"),
        None,
    )
    .await;
    assert_eq!(status, StatusCode::OK, "list incidents: {body}");
    body["incidents"].as_array().cloned().unwrap_or_default()
}

/// Resolves the table id from a load (the table-uuid is the Iceberg uuid, but
/// incidents key on the internal ULID; read it from the `table_snapshots` via the
/// status endpoint, which echoes the internal id).
async fn table_internal_id(ctx: &TestCtx, ns: &str, table: &str) -> String {
    let (status, body) = send(
        &ctx.router,
        "GET",
        &format!(
            "/api/v2/quality/tables/{}/{ns}/{table}/status",
            ctx.warehouse
        ),
        None,
    )
    .await;
    assert_eq!(status, StatusCode::OK, "table status: {body}");
    body["table_id"].as_str().expect("table_id").to_owned()
}

async fn audit_chain_ok(ctx: &TestCtx) -> bool {
    let (status, body) = send(&ctx.router, "GET", "/api/v2/audit/verify", None).await;
    assert_eq!(status, StatusCode::OK, "verify audit: {body}");
    body["valid"].as_bool().unwrap_or(false)
}

// ===========================================================================
// Monitor CRUD
// ===========================================================================

#[tokio::test]
async fn monitor_crud() {
    let Some(ctx) = test_ctx().await else { return };
    make_namespace(&ctx, "db").await;
    make_table(&ctx, "db", "events", json!({})).await;

    let id = create_monitor(&ctx, "vol", "db", "events", "volume").await;

    // List: our monitor appears.
    let (status, body) = send(&ctx.router, "GET", "/api/v2/quality/monitors", None).await;
    assert_eq!(status, StatusCode::OK);
    assert!(
        body["monitors"]
            .as_array()
            .is_some_and(|m| m.iter().any(|x| x["id"] == id.as_str())),
        "our monitor must appear"
    );

    // Get.
    let (status, body) = send(
        &ctx.router,
        "GET",
        &format!("/api/v2/quality/monitors/{id}"),
        None,
    )
    .await;
    assert_eq!(status, StatusCode::OK);
    assert_eq!(body["kind"], "volume");
    assert_eq!(body["severity"], "high");

    // Patch: disable.
    let (status, body) = send(
        &ctx.router,
        "PATCH",
        &format!("/api/v2/quality/monitors/{id}"),
        Some(json!({ "enabled": false })),
    )
    .await;
    assert_eq!(status, StatusCode::OK, "{body}");
    assert_eq!(body["enabled"], false);

    // A duplicate kind on the same table is a 409.
    let (status, _) = send(
        &ctx.router,
        "POST",
        "/api/v2/quality/monitors",
        Some(json!({
            "name": format!("vol2-{}", ctx.salt), "warehouse": ctx.warehouse,
            "bound_to": "table", "namespace": "db", "table": "events", "kind": "volume",
        })),
    )
    .await;
    assert_eq!(status, StatusCode::CONFLICT);

    // A bad kind is a 400.
    let (status, _) = send(
        &ctx.router,
        "POST",
        "/api/v2/quality/monitors",
        Some(json!({
            "name": format!("bad-{}", ctx.salt), "warehouse": ctx.warehouse,
            "bound_to": "table", "namespace": "db", "table": "events", "kind": "telepathy",
        })),
    )
    .await;
    assert_eq!(status, StatusCode::BAD_REQUEST);

    // Delete.
    let (status, _) = send(
        &ctx.router,
        "DELETE",
        &format!("/api/v2/quality/monitors/{id}"),
        None,
    )
    .await;
    assert_eq!(status, StatusCode::NO_CONTENT);
}

// ===========================================================================
// Volume-spike monitor -> incident (E-F1 + E-F5)
// ===========================================================================

#[tokio::test]
async fn volume_spike_opens_incident() {
    let Some(ctx) = test_ctx().await else { return };
    make_namespace(&ctx, "sales").await;
    // Owner set so the incident carries it (never fabricated).
    let uuid = make_table(&ctx, "sales", "orders", json!({ "owner": "data-eng" })).await;
    let table_id = table_internal_id(&ctx, "sales", "orders").await;

    // Monitor first, so history commits are all evaluated (they will be `ok`
    // until the baseline exists, then the spike breaches).
    create_monitor(&ctx, "vol", "sales", "orders", "volume").await;

    // Seed a stable ~100-rows-per-commit history (5 commits) so the baseline is
    // trustworthy (MIN_HISTORY=3).
    let mut parent: Option<i64> = None;
    let mut total = 0i64;
    for i in 1..=5i64 {
        total += 100;
        commit(
            &ctx,
            "sales",
            "orders",
            append_commit(&uuid, parent, i, i, 100, total, 2, 2_000_000),
        )
        .await;
        parent = Some(i);
    }
    drive_worker(&ctx).await;

    // No incident yet: every commit was in-band.
    assert!(
        incidents_for(&ctx, &table_id).await.is_empty(),
        "stable volume must not open an incident"
    );

    // The spike: 100× the ~100 median.
    total += 10_000;
    commit(
        &ctx,
        "sales",
        "orders",
        append_commit(&uuid, Some(5), 6, 6, 10_000, total, 2, 2_000_000),
    )
    .await;
    drive_worker(&ctx).await;

    let incidents = incidents_for(&ctx, &table_id).await;
    let vol = incidents
        .iter()
        .find(|i| i["kind"] == "volume")
        .expect("a volume incident must be open");
    assert_eq!(vol["status"], "open");
    assert_eq!(vol["severity"], "high");
    assert_eq!(vol["source"], "monitor");
    assert_eq!(vol["owner"], "data-eng", "owner captured from the property");
    assert!(
        vol["detail"].as_str().unwrap_or("").contains("median"),
        "detail explains the anomaly: {}",
        vol["detail"]
    );

    // De-duplication: a second spike re-touches, does not open a duplicate.
    total += 10_000;
    commit(
        &ctx,
        "sales",
        "orders",
        append_commit(&uuid, Some(6), 7, 7, 10_000, total, 2, 2_000_000),
    )
    .await;
    drive_worker(&ctx).await;
    let after = incidents_for(&ctx, &table_id).await;
    let vol_count = after.iter().filter(|i| i["kind"] == "volume").count();
    assert_eq!(
        vol_count, 1,
        "a recurring spike must re-touch, not duplicate"
    );
    let touched = after.iter().find(|i| i["kind"] == "volume").unwrap();
    assert!(
        touched["occurrence_count"].as_i64().unwrap_or(0) >= 2,
        "occurrence_count must bump on re-touch: {touched}"
    );

    assert!(audit_chain_ok(&ctx).await, "audit chain must still verify");
}

// ===========================================================================
// Breaking-schema monitor -> incident; additive does not (E-F1)
// ===========================================================================

#[tokio::test]
async fn breaking_schema_change_opens_incident_additive_does_not() {
    let Some(ctx) = test_ctx().await else { return };
    make_namespace(&ctx, "cat").await;
    let uuid = make_table(&ctx, "cat", "products", json!({})).await;
    let table_id = table_internal_id(&ctx, "cat", "products").await;

    create_monitor(&ctx, "schema", "cat", "products", "schema_change").await;

    // First, an *additive* change: add a column. Must NOT open an incident.
    let additive_fields = json!([
        { "id": 1, "name": "id", "required": true, "type": "long" },
        { "id": 2, "name": "email", "required": false, "type": "string" },
        { "id": 3, "name": "amount", "required": false, "type": "int" },
        { "id": 4, "name": "region", "required": false, "type": "string" },
    ]);
    commit(
        &ctx,
        "cat",
        "products",
        schema_change_commit(&uuid, &additive_fields, None, 1, 1),
    )
    .await;
    drive_worker(&ctx).await;
    let after_additive = incidents_for(&ctx, &table_id).await;
    assert!(
        after_additive.iter().all(|i| i["kind"] != "schema_change"),
        "an additive change must not open a schema incident: {after_additive:?}"
    );

    // Now a *breaking* change: drop `amount` (and `region`). Must open one.
    let breaking_fields = json!([
        { "id": 1, "name": "id", "required": true, "type": "long" },
        { "id": 2, "name": "email", "required": false, "type": "string" },
    ]);
    commit(
        &ctx,
        "cat",
        "products",
        schema_change_commit(&uuid, &breaking_fields, Some(1), 2, 2),
    )
    .await;
    drive_worker(&ctx).await;

    let incidents = incidents_for(&ctx, &table_id).await;
    let schema = incidents
        .iter()
        .find(|i| i["kind"] == "schema_change")
        .expect("a breaking schema change must open an incident");
    assert_eq!(schema["status"], "open");
    assert!(
        schema["detail"].as_str().unwrap_or("").contains("breaking"),
        "detail says breaking: {}",
        schema["detail"]
    );
}

// ===========================================================================
// Incident lifecycle: open -> ack -> resolve (E-F5)
// ===========================================================================

#[tokio::test]
async fn incident_lifecycle_ack_resolve() {
    let Some(ctx) = test_ctx().await else { return };
    make_namespace(&ctx, "ops").await;
    let uuid = make_table(&ctx, "ops", "metrics", json!({})).await;
    let table_id = table_internal_id(&ctx, "ops", "metrics").await;
    create_monitor(&ctx, "vol", "ops", "metrics", "volume").await;

    // Seed history + a spike.
    let mut parent: Option<i64> = None;
    let mut total = 0i64;
    for i in 1..=4i64 {
        total += 50;
        commit(
            &ctx,
            "ops",
            "metrics",
            append_commit(&uuid, parent, i, i, 50, total, 1, 1_000_000),
        )
        .await;
        parent = Some(i);
    }
    total += 5_000;
    commit(
        &ctx,
        "ops",
        "metrics",
        append_commit(&uuid, Some(4), 5, 5, 5_000, total, 1, 1_000_000),
    )
    .await;
    drive_worker(&ctx).await;

    let incidents = incidents_for(&ctx, &table_id).await;
    let incident = incidents.first().expect("an incident is open");
    let id = incident["id"].as_str().expect("incident id").to_owned();
    assert_eq!(incident["status"], "open");

    // Acknowledge.
    let (status, body) = send(
        &ctx.router,
        "POST",
        &format!("/api/v2/quality/incidents/{id}/ack"),
        None,
    )
    .await;
    assert_eq!(status, StatusCode::OK, "{body}");
    assert_eq!(body["status"], "acknowledged");
    assert!(body["acknowledged_at"].is_string());

    // Ack again is a 409 (already acknowledged).
    let (status, _) = send(
        &ctx.router,
        "POST",
        &format!("/api/v2/quality/incidents/{id}/ack"),
        None,
    )
    .await;
    assert_eq!(status, StatusCode::CONFLICT);

    // Resolve.
    let (status, body) = send(
        &ctx.router,
        "POST",
        &format!("/api/v2/quality/incidents/{id}/resolve"),
        None,
    )
    .await;
    assert_eq!(status, StatusCode::OK, "{body}");
    assert_eq!(body["status"], "resolved");
    assert!(body["resolved_at"].is_string());

    // Resolve again is a 409.
    let (status, _) = send(
        &ctx.router,
        "POST",
        &format!("/api/v2/quality/incidents/{id}/resolve"),
        None,
    )
    .await;
    assert_eq!(status, StatusCode::CONFLICT);

    // The table status is now green again (no live incidents).
    let (status, body) = send(
        &ctx.router,
        "GET",
        &format!(
            "/api/v2/quality/tables/{}/ops/metrics/status",
            ctx.warehouse
        ),
        None,
    )
    .await;
    assert_eq!(status, StatusCode::OK);
    assert_eq!(body["status"], "green", "resolved -> green: {body}");
    assert_eq!(body["live_incidents"], 0);

    // Status history has both the open and resolve points.
    let (status, body) = send(
        &ctx.router,
        "GET",
        &format!(
            "/api/v2/quality/tables/{}/ops/metrics/status/history",
            ctx.warehouse
        ),
        None,
    )
    .await;
    assert_eq!(status, StatusCode::OK);
    let events = body["history"].as_array().expect("history");
    assert!(events.iter().any(|e| e["event"] == "opened"));
    assert!(events.iter().any(|e| e["event"] == "resolved"));
}

// ===========================================================================
// Blast radius via lineage (E-F5)
// ===========================================================================

#[tokio::test]
async fn incident_carries_blast_radius_from_lineage() {
    let Some(ctx) = test_ctx().await else { return };
    make_namespace(&ctx, "lin").await;
    // Upstream `raw` feeds downstream `mart` (declared via the commit summary).
    let raw_uuid = make_table(&ctx, "lin", "raw", json!({})).await;
    let _mart_uuid = make_table(&ctx, "lin", "mart", json!({ "owner": "analytics" })).await;
    let raw_id = table_internal_id(&ctx, "lin", "raw").await;
    let mart_id = table_internal_id(&ctx, "lin", "mart").await;

    // Record a lineage edge raw -> mart via the OpenLineage sink (a declared
    // input pair; no fabrication).
    let (status, body) = send(
        &ctx.router,
        "POST",
        "/api/v2/lineage/openlineage",
        Some(json!({
            "eventType": "COMPLETE",
            "eventTime": "2026-01-01T00:00:00Z",
            "run": { "runId": "11111111-1111-1111-1111-111111111111" },
            "job": { "namespace": "test", "name": "raw_to_mart" },
            "inputs": [ { "namespace": ctx.warehouse, "name": "lin.raw" } ],
            "outputs": [ { "namespace": ctx.warehouse, "name": "lin.mart" } ],
        })),
    )
    .await;
    assert_eq!(status, StatusCode::OK, "openlineage sink: {body}");

    // A monitor + spike on the upstream `raw`.
    create_monitor(&ctx, "vol", "lin", "raw", "volume").await;
    let mut parent: Option<i64> = None;
    let mut total = 0i64;
    for i in 1..=4i64 {
        total += 100;
        commit(
            &ctx,
            "lin",
            "raw",
            append_commit(&raw_uuid, parent, i, i, 100, total, 2, 2_000_000),
        )
        .await;
        parent = Some(i);
    }
    total += 10_000;
    commit(
        &ctx,
        "lin",
        "raw",
        append_commit(&raw_uuid, Some(4), 5, 5, 10_000, total, 2, 2_000_000),
    )
    .await;
    drive_worker(&ctx).await;

    let incidents = incidents_for(&ctx, &raw_id).await;
    let incident = incidents.first().expect("an incident on raw");
    let blast = incident["blast_radius"]
        .as_array()
        .expect("blast_radius array");
    assert!(
        blast.iter().any(|a| a["table_id"] == mart_id.as_str()),
        "blast radius must include the downstream mart: {blast:?}"
    );
    // The downstream owner is carried for notification routing.
    assert!(
        blast
            .iter()
            .any(|a| a["table_id"] == mart_id.as_str() && a["owner"] == "analytics"),
        "downstream owner must be captured: {blast:?}"
    );
}

// ===========================================================================
// Quality score (E-F6)
// ===========================================================================

#[tokio::test]
async fn quality_score_reflects_signals() {
    let Some(ctx) = test_ctx().await else { return };
    make_namespace(&ctx, "q").await;

    // A bare table: no contract, no owner, no monitor -> low score.
    make_table(&ctx, "q", "bare", json!({})).await;
    let (status, body) = send(
        &ctx.router,
        "GET",
        &format!("/api/v2/quality/tables/{}/q/bare/score", ctx.warehouse),
        None,
    )
    .await;
    assert_eq!(status, StatusCode::OK, "{body}");
    let bare_score = body["score"].as_i64().expect("score");
    assert!(bare_score < 40, "a bare table scores low: {bare_score}");

    // A well-governed table: owner + comment + a block contract + a monitor.
    make_table(
        &ctx,
        "q",
        "good",
        json!({ "owner": "data-eng", "comment": "the orders fact table" }),
    )
    .await;
    // A block contract.
    let (status, _) = send(
        &ctx.router,
        "POST",
        "/api/v2/quality/contracts",
        Some(json!({
            "name": format!("c-{}", ctx.salt), "warehouse": ctx.warehouse, "bound_to": "table",
            "namespace": "q", "table": "good", "mode": "block",
            "spec": { "schema": { "allowed_evolution": "no_narrowing" } },
        })),
    )
    .await;
    assert_eq!(status, StatusCode::CREATED);
    create_monitor(&ctx, "fresh", "q", "good", "freshness").await;

    let (status, body) = send(
        &ctx.router,
        "GET",
        &format!("/api/v2/quality/tables/{}/q/good/score", ctx.warehouse),
        None,
    )
    .await;
    assert_eq!(status, StatusCode::OK, "{body}");
    let good_score = body["score"].as_i64().expect("score");
    assert!(
        good_score > bare_score,
        "a governed table scores higher ({good_score}) than a bare one ({bare_score})"
    );
    // Components present + explainable.
    assert!((body["components"]["contract"].as_f64().unwrap_or(0.0) - 1.0).abs() < 1e-9);
    assert!((body["components"]["ownership"].as_f64().unwrap_or(0.0) - 1.0).abs() < 1e-9);
    assert!(body["grade"].is_string());
}

// ===========================================================================
// Impact CI gate (F-F5)
// ===========================================================================

#[tokio::test]
async fn impact_reports_downstream_for_ci_gate() {
    let Some(ctx) = test_ctx().await else { return };
    make_namespace(&ctx, "ci").await;
    make_table(&ctx, "ci", "src", json!({})).await;
    make_table(&ctx, "ci", "derived", json!({ "owner": "team-b" })).await;
    let derived_id = table_internal_id(&ctx, "ci", "derived").await;

    // Column-level lineage src.email -> derived.email via the sink.
    let (status, body) = send(
        &ctx.router,
        "POST",
        "/api/v2/lineage/openlineage",
        Some(json!({
            "eventType": "COMPLETE",
            "eventTime": "2026-01-01T00:00:00Z",
            "run": { "runId": "22222222-2222-2222-2222-222222222222" },
            "job": { "namespace": "test", "name": "src_to_derived" },
            "inputs": [ { "namespace": ctx.warehouse, "name": "ci.src" } ],
            "outputs": [ {
                "namespace": ctx.warehouse, "name": "ci.derived",
                "facets": { "columnLineage": { "fields": {
                    "email": { "inputFields": [
                        { "namespace": ctx.warehouse, "name": "ci.src", "field": "email" }
                    ] }
                } } }
            } ],
        })),
    )
    .await;
    assert_eq!(status, StatusCode::OK, "openlineage: {body}");

    // The impact query for a drop_column on src.email must list derived.
    let (status, body) = send(
        &ctx.router,
        "GET",
        &format!(
            "/api/v2/lineage/impact?asset={}.ci.src&change=drop_column:email",
            ctx.warehouse
        ),
        None,
    )
    .await;
    assert_eq!(status, StatusCode::OK, "impact: {body}");
    let affected = body["affected"].as_array().expect("affected");
    assert!(
        affected
            .iter()
            .any(|a| a["table_id"] == derived_id.as_str()),
        "impact must list the downstream derived table: {affected:?}"
    );
    // The owner is surfaced for notification.
    assert!(
        body["owners"]
            .as_array()
            .is_some_and(|o| o.iter().any(|x| x == "team-b")),
        "impact must surface the downstream owner: {body}"
    );

    // A drop_table also breaks it.
    let (status, body) = send(
        &ctx.router,
        "GET",
        &format!(
            "/api/v2/lineage/impact?asset={}.ci.src&change=drop_table",
            ctx.warehouse
        ),
        None,
    )
    .await;
    assert_eq!(status, StatusCode::OK);
    assert!(
        body["affected"]
            .as_array()
            .is_some_and(|a| a.iter().any(|x| x["table_id"] == derived_id.as_str())),
        "drop_table blast radius must include derived"
    );
}

// ===========================================================================
// Contract violation -> incident (E-F5 bridge)
// ===========================================================================

#[tokio::test]
async fn contract_violation_opens_incident() {
    let Some(ctx) = test_ctx().await else { return };
    make_namespace(&ctx, "gov").await;
    let uuid = make_table(&ctx, "gov", "ledger", json!({})).await;
    let table_id = table_internal_id(&ctx, "gov", "ledger").await;

    // A warn-mode contract that forbids narrowing (so a drop lands + warns).
    let (status, _) = send(
        &ctx.router,
        "POST",
        "/api/v2/quality/contracts",
        Some(json!({
            "name": format!("nodrop-{}", ctx.salt), "warehouse": ctx.warehouse, "bound_to": "table",
            "namespace": "gov", "table": "ledger", "mode": "warn",
            "spec": { "schema": { "allowed_evolution": "no_narrowing" } },
        })),
    )
    .await;
    assert_eq!(status, StatusCode::CREATED);

    // A breaking commit: drop `amount`. Warn mode -> it lands + records a
    // violation event, which the worker turns into a contract incident.
    let breaking_fields = json!([
        { "id": 1, "name": "id", "required": true, "type": "long" },
        { "id": 2, "name": "email", "required": false, "type": "string" },
    ]);
    commit(
        &ctx,
        "gov",
        "ledger",
        schema_change_commit(&uuid, &breaking_fields, None, 1, 1),
    )
    .await;
    drive_worker(&ctx).await;

    let incidents = incidents_for(&ctx, &table_id).await;
    let contract_incident = incidents
        .iter()
        .find(|i| i["source"] == "contract")
        .expect("a contract violation must open a contract-sourced incident");
    assert_eq!(
        contract_incident["severity"], "low",
        "warn mode -> low severity"
    );
    assert!(
        contract_incident["title"]
            .as_str()
            .unwrap_or("")
            .contains("contract"),
        "title names the contract: {}",
        contract_incident["title"]
    );
}
