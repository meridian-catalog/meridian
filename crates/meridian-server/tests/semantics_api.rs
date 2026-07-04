//! Router-level integration tests for the semantics layer (Pillar G):
//! universal-view transpilation (G-F1), metric compilation (G-F2), the glossary
//! (G-F3), and data products (G-F4).
//!
//! These require a running Postgres and `DATABASE_URL`; without it they skip.
//! The transpilation tests additionally require the SQLGlot sidecar: they start
//! it as a subprocess (`uv run uvicorn`) on a dedicated port and point the
//! router at it. If the sidecar cannot start (no `uv`, no synced env), those
//! tests skip **with a note** rather than fail — the deterministic transpiler is
//! the sidecar's, and a machine without it simply cannot exercise this path.
//!
//! Test isolation (M3/M4/M5 discipline): every test provisions its own
//! uniquely-named warehouse/namespace/objects and scopes assertions to its own
//! ids. Nothing here depends on a real external LLM or network — the sidecar is
//! local and runs deterministic SQLGlot only (no LLM key is ever set).

// `SQLGlot` reads as an un-backticked item to the doc-markdown lint in a few
// prose lines; it is a product name, not code, so relax the lint for this file.
#![allow(clippy::doc_markdown)]

use std::process::{Child, Command, Stdio};
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

// ---------------------------------------------------------------------------
// Harness
// ---------------------------------------------------------------------------

struct TestCtx {
    router: Router,
    _root: tempfile::TempDir,
    warehouse: String,
}

/// Builds a router with the default (no-sidecar) config. Sufficient for the
/// CRUD tests, which never call the sidecar.
async fn test_ctx() -> Option<TestCtx> {
    build_ctx(None).await
}

/// Builds a router whose transpilation config points at `sidecar_url`.
async fn test_ctx_with_sidecar(sidecar_url: &str) -> Option<TestCtx> {
    build_ctx(Some(sidecar_url.to_owned())).await
}

async fn build_ctx(sidecar_url: Option<String>) -> Option<TestCtx> {
    let Ok(url) = std::env::var("DATABASE_URL") else {
        eprintln!("skipping semantics API test: DATABASE_URL is not set");
        return None;
    };

    let mut config = AppConfig::default();
    config.database.url = url;
    if let Some(sidecar_url) = sidecar_url {
        config.transpilation.sidecar_url = sidecar_url;
        config.transpilation.request_timeout_secs = 10;
    }

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
    })
}

/// Sends one request (optionally with headers) through the full stack.
async fn send_with_headers(
    router: &Router,
    method: &str,
    uri: &str,
    body: Option<Value>,
    headers: &[(&str, &str)],
) -> (StatusCode, Value) {
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
    let value = if bytes.is_empty() {
        Value::Null
    } else {
        serde_json::from_slice(&bytes).expect("response body is JSON")
    };
    (status, value)
}

async fn send(
    router: &Router,
    method: &str,
    uri: &str,
    body: Option<Value>,
) -> (StatusCode, Value) {
    send_with_headers(router, method, uri, body, &[]).await
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
            { "id": 2, "name": "amount", "required": false, "type": "double" },
        ],
    })
}

// ---------------------------------------------------------------------------
// Sidecar subprocess management (transpilation tests only)
// ---------------------------------------------------------------------------

/// A started sidecar subprocess, killed on drop.
struct SidecarProc {
    child: Child,
    url: String,
}

impl Drop for SidecarProc {
    fn drop(&mut self) {
        // The child IS uvicorn (started from the venv binary directly, not via
        // a `uv run` wrapper), so killing the child reaps the server — no
        // grandchild orphan is left holding the port.
        let _ = self.child.kill();
        let _ = self.child.wait();
    }
}

/// Attempts to start the SQLGlot sidecar on a free-ish high port, waiting until
/// the port accepts a connection. Returns `None` (so the caller skips) when the
/// sidecar's uv-managed venv is not present, or the sidecar does not come up in
/// time — never a real LLM, never a network dependency.
///
/// The sidecar is launched from its venv's `uvicorn` binary directly (the venv
/// is created by `uv sync` in the sidecar's documented dev/CI workflow), so the
/// spawned process is uvicorn itself and teardown reaps it cleanly.
fn start_sidecar() -> Option<SidecarProc> {
    let sidecar_dir = concat!(env!("CARGO_MANIFEST_DIR"), "/../../sidecar");
    // A port unlikely to collide with a dev sidecar (8200) or other services.
    let port = 8265 + (std::process::id() % 200);
    let host = "127.0.0.1";
    let url = format!("http://{host}:{port}");

    // The venv uvicorn binary. Absent on a machine that has not run `uv sync`
    // for the sidecar; the test then skips (the sidecar path cannot be
    // exercised without the deterministic transpiler).
    let uvicorn = std::path::Path::new(sidecar_dir).join(".venv/bin/uvicorn");
    if !uvicorn.exists() {
        return None;
    }

    let child = Command::new(uvicorn)
        .args([
            "meridian_sidecar.app:app",
            "--host",
            host,
            "--port",
            &port.to_string(),
        ])
        .current_dir(sidecar_dir)
        // The safety default: never configure an LLM provider in a test.
        .env_remove("MERIDIAN_LLM_ASSIST_PROVIDER")
        .stdout(Stdio::null())
        .stderr(Stdio::null())
        .spawn()
        .ok()?;

    let mut proc = SidecarProc { child, url };

    // Wait until the port accepts a TCP connection (uvicorn binds only once the
    // ASGI app is ready to serve), for up to ~15s. A raw TCP probe avoids a
    // blocking-HTTP client dependency; the first real request the test makes
    // exercises the HTTP surface.
    let addr: std::net::SocketAddr = format!("{host}:{port}").parse().ok()?;
    for _ in 0..60 {
        // If the child already exited, starting failed (e.g. env not synced).
        if let Ok(Some(_)) = proc.child.try_wait() {
            return None;
        }
        if std::net::TcpStream::connect_timeout(&addr, Duration::from_millis(400)).is_ok() {
            // Give uvicorn a beat to finish wiring routes after the bind.
            std::thread::sleep(Duration::from_millis(300));
            return Some(proc);
        }
        std::thread::sleep(Duration::from_millis(250));
    }
    None
}

// ===========================================================================
// Universal views (G-F1)
// ===========================================================================

/// A view authored ONLY in Spark: loading it as DuckDB/Trino must transpile.
fn spark_only_view_body(name: &str, ns: &str) -> Value {
    json!({
        "name": name,
        "schema": simple_schema(),
        "view-version": {
            "version-id": 1,
            "timestamp-ms": 1_700_000_000_000i64,
            "schema-id": 0,
            "summary": { "engine-name": "semantics-tests" },
            "representations": [
                { "type": "sql",
                  "sql": "SELECT id, amount FROM sales WHERE amount > 100",
                  "dialect": "spark" },
            ],
            "default-namespace": [ns],
        },
        "properties": { "comment": "spark-only view" },
    })
}

#[tokio::test]
async fn spark_view_loaded_as_duckdb_is_transpiled() {
    // Skip cleanly when the DB is absent (before spending time on the sidecar).
    if std::env::var("DATABASE_URL").is_err() {
        eprintln!("skipping universal-view transpile test: DATABASE_URL is not set");
        return;
    }
    let Some(sidecar) = start_sidecar() else {
        eprintln!("skipping universal-view transpile test: sidecar could not start");
        return;
    };
    let Some(ctx) = test_ctx_with_sidecar(&sidecar.url).await else {
        return;
    };

    let ns = format!("g1_{}", Ulid::new().to_string().to_lowercase());
    make_namespace(&ctx, &ns).await;
    let name = "spark_view";
    let (status, body) = send(
        &ctx.router,
        "POST",
        &format!("/v1/{}/namespaces/{ns}/views", ctx.warehouse),
        Some(spark_only_view_body(name, &ns)),
    )
    .await;
    assert_eq!(status, StatusCode::OK, "create spark-only view: {body}");

    // Load requesting DuckDB via the explicit engine override.
    let (status, body) = send(
        &ctx.router,
        "GET",
        &format!(
            "/v1/{}/namespaces/{ns}/views/{name}?engine=duckdb",
            ctx.warehouse
        ),
        None,
    )
    .await;
    assert_eq!(status, StatusCode::OK, "load view as duckdb: {body}");

    // The transpile note reports the honest status for duckdb.
    let note = &body["meridian-transpile"];
    assert_eq!(note["requested_dialect"], "duckdb", "note: {body}");
    let transpile_status = note["status"].as_str().expect("status present");
    assert!(
        transpile_status == "verified" || transpile_status == "best_effort",
        "spark->duckdb of a simple SELECT should translate, got {transpile_status}: {body}"
    );

    // The served metadata's current version now carries a duckdb representation,
    // tagged with the transpile status.
    let representations = body["metadata"]["versions"][0]["representations"]
        .as_array()
        .expect("representations array");
    let duckdb = representations
        .iter()
        .find(|r| r["dialect"] == "duckdb")
        .expect("a duckdb representation was folded in");
    assert!(
        duckdb["sql"].as_str().is_some_and(|s| !s.is_empty()),
        "duckdb representation has SQL: {duckdb}"
    );
    assert_eq!(
        duckdb["meridian.transpile-status"], transpile_status,
        "representation carries the status: {duckdb}"
    );

    // A second load hits the durable cache and yields the same status.
    let (status, body2) = send(
        &ctx.router,
        "GET",
        &format!(
            "/v1/{}/namespaces/{ns}/views/{name}?engine=duckdb",
            ctx.warehouse
        ),
        None,
    )
    .await;
    assert_eq!(status, StatusCode::OK, "second load: {body2}");
    assert_eq!(
        body2["meridian-transpile"]["status"], transpile_status,
        "cached status matches fresh status"
    );
}

#[tokio::test]
async fn view_loaded_with_no_engine_is_served_as_authored() {
    let Some(ctx) = test_ctx().await else {
        return;
    };
    let ns = format!("g1n_{}", Ulid::new().to_string().to_lowercase());
    make_namespace(&ctx, &ns).await;
    let name = "plain_view";
    let (status, _) = send(
        &ctx.router,
        "POST",
        &format!("/v1/{}/namespaces/{ns}/views", ctx.warehouse),
        Some(spark_only_view_body(name, &ns)),
    )
    .await;
    assert_eq!(status, StatusCode::OK);

    // No ?engine and no engine-identifying User-Agent -> no transpile note, the
    // view is served exactly as authored.
    let (status, body) = send(
        &ctx.router,
        "GET",
        &format!("/v1/{}/namespaces/{ns}/views/{name}", ctx.warehouse),
        None,
    )
    .await;
    assert_eq!(status, StatusCode::OK, "load view: {body}");
    assert!(
        body.get("meridian-transpile").is_none() || body["meridian-transpile"].is_null(),
        "no engine requested -> no transpile note: {body}"
    );
}

#[tokio::test]
async fn view_already_carrying_target_dialect_reports_verified_without_sidecar() {
    // A view authored with a duckdb representation, loaded as duckdb, needs no
    // transpilation — so this passes even with NO sidecar configured.
    let Some(ctx) = test_ctx().await else {
        return;
    };
    let ns = format!("g1e_{}", Ulid::new().to_string().to_lowercase());
    make_namespace(&ctx, &ns).await;
    let name = "dual_view";
    let body = json!({
        "name": name,
        "schema": simple_schema(),
        "view-version": {
            "version-id": 1,
            "timestamp-ms": 1_700_000_000_000i64,
            "schema-id": 0,
            "summary": {},
            "representations": [
                { "type": "sql", "sql": "SELECT id FROM sales", "dialect": "spark" },
                { "type": "sql", "sql": "SELECT id FROM sales", "dialect": "duckdb" },
            ],
            "default-namespace": [ns],
        },
    });
    let (status, _) = send(
        &ctx.router,
        "POST",
        &format!("/v1/{}/namespaces/{ns}/views", ctx.warehouse),
        Some(body),
    )
    .await;
    assert_eq!(status, StatusCode::OK);

    let (status, body) = send(
        &ctx.router,
        "GET",
        &format!(
            "/v1/{}/namespaces/{ns}/views/{name}?engine=duckdb",
            ctx.warehouse
        ),
        None,
    )
    .await;
    assert_eq!(status, StatusCode::OK, "load: {body}");
    assert_eq!(
        body["meridian-transpile"]["status"], "verified",
        "already-present dialect is verified: {body}"
    );
}

// ===========================================================================
// Standalone transpile passthrough (G-F1)
// ===========================================================================

#[tokio::test]
async fn transpile_passthrough_translates_spark_to_trino() {
    let Some(sidecar) = start_sidecar() else {
        eprintln!("skipping transpile passthrough test: sidecar could not start");
        return;
    };
    let Some(ctx) = test_ctx_with_sidecar(&sidecar.url).await else {
        return;
    };

    let (status, body) = send(
        &ctx.router,
        "POST",
        "/api/v2/sql/transpile",
        Some(json!({
            "sql": "SELECT id, amount FROM sales WHERE amount > 100",
            "from_dialect": "spark",
            "to_dialect": "trino",
        })),
    )
    .await;
    assert_eq!(status, StatusCode::OK, "transpile: {body}");
    assert!(
        body["status"] == "verified" || body["status"] == "best_effort",
        "status: {body}"
    );
    assert!(
        body["sql"]
            .as_str()
            .is_some_and(|s| s.to_uppercase().contains("SELECT")),
        "translated SQL present: {body}"
    );
}

#[tokio::test]
async fn transpile_passthrough_rejects_empty_sql() {
    // Validation happens before the sidecar is consulted, so no sidecar needed.
    let Some(ctx) = test_ctx().await else {
        return;
    };
    let (status, _) = send(
        &ctx.router,
        "POST",
        "/api/v2/sql/transpile",
        Some(json!({ "sql": "  ", "from_dialect": "spark", "to_dialect": "trino" })),
    )
    .await;
    assert_eq!(status, StatusCode::BAD_REQUEST);
}

// ===========================================================================
// Metrics (G-F2)
// ===========================================================================

#[tokio::test]
async fn metric_crud_and_compile() {
    let Some(ctx) = test_ctx().await else {
        return;
    };

    // Create.
    let name = format!("revenue_{}", Ulid::new().to_string().to_lowercase());
    let (status, body) = send(
        &ctx.router,
        "POST",
        "/api/v2/metrics",
        Some(json!({
            "name": name,
            "source": "analytics.sales",
            "expression": "SUM(amount)",
            "dialect": "trino",
            "dimensions": ["region"],
            "filters": ["status = 'paid'"],
            "grain": "one row per order",
            "certification": "certified",
        })),
    )
    .await;
    assert_eq!(status, StatusCode::CREATED, "create metric: {body}");
    let id = body["id"].as_str().expect("id").to_owned();
    assert_eq!(body["certification"], "certified");

    // Get.
    let (status, got) = send(&ctx.router, "GET", &format!("/api/v2/metrics/{id}"), None).await;
    assert_eq!(status, StatusCode::OK, "get metric: {got}");
    assert_eq!(got["expression"], "SUM(amount)");

    // Duplicate name -> conflict.
    let (status, _) = send(
        &ctx.router,
        "POST",
        "/api/v2/metrics",
        Some(json!({ "name": name, "source": "x", "expression": "COUNT(*)" })),
    )
    .await;
    assert_eq!(status, StatusCode::CONFLICT, "duplicate name conflicts");

    // Update.
    let (status, updated) = send(
        &ctx.router,
        "PATCH",
        &format!("/api/v2/metrics/{id}"),
        Some(json!({ "certification": "deprecated", "description": "old" })),
    )
    .await;
    assert_eq!(status, StatusCode::OK, "update: {updated}");
    assert_eq!(updated["certification"], "deprecated");

    // List includes it (scope the assertion to our id).
    let (status, list) = send(&ctx.router, "GET", "/api/v2/metrics?limit=500", None).await;
    assert_eq!(status, StatusCode::OK);
    assert!(
        list["metrics"]
            .as_array()
            .expect("metrics array")
            .iter()
            .any(|m| m["id"] == id),
        "our metric is listed"
    );

    // Delete.
    let (status, _) = send(
        &ctx.router,
        "DELETE",
        &format!("/api/v2/metrics/{id}"),
        None,
    )
    .await;
    assert_eq!(status, StatusCode::NO_CONTENT);
    let (status, _) = send(&ctx.router, "GET", &format!("/api/v2/metrics/{id}"), None).await;
    assert_eq!(status, StatusCode::NOT_FOUND, "deleted metric is gone");
}

#[tokio::test]
async fn metric_compiles_to_engine_sql() {
    let Some(sidecar) = start_sidecar() else {
        eprintln!("skipping metric compile test: sidecar could not start");
        return;
    };
    let Some(ctx) = test_ctx_with_sidecar(&sidecar.url).await else {
        return;
    };

    let name = format!("orders_{}", Ulid::new().to_string().to_lowercase());
    let (status, body) = send(
        &ctx.router,
        "POST",
        "/api/v2/metrics",
        Some(json!({
            "name": name,
            "source": "db.orders",
            "expression": "COUNT(DISTINCT order_id)",
            "dialect": "trino",
            "dimensions": ["dt"],
        })),
    )
    .await;
    assert_eq!(status, StatusCode::CREATED, "create: {body}");
    let id = body["id"].as_str().expect("id").to_owned();

    // Compile to DuckDB.
    let (status, compiled) = send(
        &ctx.router,
        "GET",
        &format!("/api/v2/metrics/{id}/compile?engine=duckdb"),
        None,
    )
    .await;
    assert_eq!(status, StatusCode::OK, "compile: {compiled}");
    assert!(
        compiled["status"] == "verified" || compiled["status"] == "best_effort",
        "compile status: {compiled}"
    );
    let sql = compiled["sql"]
        .as_str()
        .expect("compiled sql")
        .to_uppercase();
    assert!(sql.contains("COUNT(DISTINCT"), "measure present: {sql}");
    assert!(sql.contains("GROUP BY"), "dimension drives group by: {sql}");
}

// ===========================================================================
// Glossary (G-F3)
// ===========================================================================

#[tokio::test]
async fn glossary_crud_and_links() {
    let Some(ctx) = test_ctx().await else {
        return;
    };

    let name = format!("Revenue_{}", Ulid::new().to_string().to_lowercase());
    let (status, body) = send(
        &ctx.router,
        "POST",
        "/api/v2/glossary/terms",
        Some(json!({
            "name": name,
            "definition": "Recognized revenue, net of refunds.",
            "certification": "certified",
        })),
    )
    .await;
    assert_eq!(status, StatusCode::CREATED, "create term: {body}");
    let term_id = body["id"].as_str().expect("id").to_owned();

    // Case-insensitive duplicate -> conflict.
    let (status, _) = send(
        &ctx.router,
        "POST",
        "/api/v2/glossary/terms",
        Some(json!({ "name": name.to_lowercase(), "definition": "dup" })),
    )
    .await;
    assert_eq!(
        status,
        StatusCode::CONFLICT,
        "case-insensitive duplicate conflicts"
    );

    // Link to an asset.
    let (status, link) = send(
        &ctx.router,
        "POST",
        &format!("/api/v2/glossary/terms/{term_id}/links"),
        Some(json!({ "asset_kind": "table", "asset_ref": "table:01ABC" })),
    )
    .await;
    assert_eq!(status, StatusCode::CREATED, "link: {link}");
    let link_id = link["id"].as_str().expect("link id").to_owned();

    // Re-link the same pair is idempotent (still 201, same underlying row).
    let (status, relink) = send(
        &ctx.router,
        "POST",
        &format!("/api/v2/glossary/terms/{term_id}/links"),
        Some(json!({ "asset_kind": "table", "asset_ref": "table:01ABC" })),
    )
    .await;
    assert_eq!(status, StatusCode::CREATED, "idempotent relink");
    assert_eq!(relink["id"], link_id, "re-link returns the same row");

    // Get term shows the link.
    let (status, got) = send(
        &ctx.router,
        "GET",
        &format!("/api/v2/glossary/terms/{term_id}"),
        None,
    )
    .await;
    assert_eq!(status, StatusCode::OK, "get term: {got}");
    assert_eq!(got["links"].as_array().expect("links").len(), 1);

    // Unlink.
    let (status, _) = send(
        &ctx.router,
        "DELETE",
        &format!("/api/v2/glossary/links/{link_id}"),
        None,
    )
    .await;
    assert_eq!(status, StatusCode::NO_CONTENT);

    // Invalid asset kind -> 400.
    let (status, _) = send(
        &ctx.router,
        "POST",
        &format!("/api/v2/glossary/terms/{term_id}/links"),
        Some(json!({ "asset_kind": "bogus", "asset_ref": "x" })),
    )
    .await;
    assert_eq!(status, StatusCode::BAD_REQUEST);

    // Delete term.
    let (status, _) = send(
        &ctx.router,
        "DELETE",
        &format!("/api/v2/glossary/terms/{term_id}"),
        None,
    )
    .await;
    assert_eq!(status, StatusCode::NO_CONTENT);
}

// ===========================================================================
// Data products (G-F4)
// ===========================================================================

#[tokio::test]
async fn data_product_crud_members_and_status() {
    let Some(ctx) = test_ctx().await else {
        return;
    };

    let name = format!("sales360_{}", Ulid::new().to_string().to_lowercase());
    let (status, body) = send(
        &ctx.router,
        "POST",
        "/api/v2/products",
        Some(json!({
            "name": name,
            "description": "The certified sales view.",
            "sla": "99.9% freshness within 1h",
            "certification": "certified",
        })),
    )
    .await;
    assert_eq!(status, StatusCode::CREATED, "create product: {body}");
    let id = body["id"].as_str().expect("id").to_owned();
    assert_eq!(body["certification"], "certified");

    // Add members of several kinds.
    for (kind, reference) in [
        ("table", "table:01SALES"),
        ("metric", "metric:01REV"),
        ("glossary_term", "glossary_term:01TERM"),
    ] {
        let (status, member) = send(
            &ctx.router,
            "POST",
            &format!("/api/v2/products/{id}/members"),
            Some(json!({ "member_kind": kind, "member_ref": reference })),
        )
        .await;
        assert_eq!(status, StatusCode::CREATED, "add {kind} member: {member}");
    }

    // Get shows all members.
    let (status, got) = send(&ctx.router, "GET", &format!("/api/v2/products/{id}"), None).await;
    assert_eq!(status, StatusCode::OK, "get product: {got}");
    assert_eq!(got["members"].as_array().expect("members").len(), 3);

    // Status page: certification + member counts + a rollup (no real table
    // members resolve here, so the rollup is no_signal — honestly reported).
    let (status, page) = send(
        &ctx.router,
        "GET",
        &format!("/api/v2/products/{id}/status"),
        None,
    )
    .await;
    assert_eq!(status, StatusCode::OK, "status: {page}");
    assert_eq!(page["product"]["certification"], "certified");
    assert_eq!(page["member_total"], 3);
    assert_eq!(page["member_counts"]["metric"], 1);
    // The unresolvable table member is reported as unknown, not dropped.
    let table_statuses = page["table_statuses"].as_array().expect("table_statuses");
    assert_eq!(table_statuses.len(), 1);
    assert_eq!(table_statuses[0]["resolved"]["status"], "unknown");

    // Delete product cascades its members.
    let (status, _) = send(
        &ctx.router,
        "DELETE",
        &format!("/api/v2/products/{id}"),
        None,
    )
    .await;
    assert_eq!(status, StatusCode::NO_CONTENT);
    let (status, _) = send(&ctx.router, "GET", &format!("/api/v2/products/{id}"), None).await;
    assert_eq!(status, StatusCode::NOT_FOUND);
}
