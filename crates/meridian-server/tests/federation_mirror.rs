//! End-to-end **IRC-to-IRC mirror** test for Pillar B (B-F1 inbound mirrors).
//!
//! Meridian *is* an Iceberg REST Catalog, so this test mirrors one Meridian
//! catalog into another Meridian catalog — a genuine IRC-to-IRC sync over the
//! real HTTP surface, which needs no external competitor container (documented
//! choice; the runbook's Polaris/Lakekeeper path is the heavier alternative and
//! is equivalent from the sync engine's point of view — both are just IRC
//! endpoints). It:
//!
//!   1. serves a Meridian router on a real TCP port (the *source* catalog),
//!   2. creates a source warehouse with a namespace and two tables (real
//!      schemas, written metadata),
//!   3. registers a **mirror** pointing at that source over HTTP and runs a
//!      sync,
//!   4. asserts the mirror now lists those tables as **foreign / read-only**
//!      with the correct schema, that they appear in Meridian **search**, and
//!      that a **commit/write to a foreign table is rejected** with a 409.
//!
//! Requires a running Postgres and `DATABASE_URL`; without it the test skips.

use std::sync::Arc;
use std::time::Duration;

use meridian_common::AppConfig;
use meridian_server::{AppState, build_router};
use serde_json::{Value, json};
use tokio::net::TcpListener;
use ulid::Ulid;

/// A running source server plus a reqwest client pointed at it.
struct SourceServer {
    base: String,
    client: reqwest::Client,
    _handle: tokio::task::JoinHandle<()>,
}

/// Boots the Meridian router on an ephemeral TCP port and returns its base URL.
/// The federation worker is disabled so this test drives sync explicitly (no
/// racing background loop), and maintenance is disabled to keep the log quiet.
async fn boot_source() -> Option<SourceServer> {
    let Ok(url) = std::env::var("DATABASE_URL") else {
        eprintln!("skipping federation mirror test: DATABASE_URL is not set");
        return None;
    };
    let mut config = AppConfig::default();
    config.database.url = url;
    config.maintenance.enabled = false;
    config.federation.enabled = false; // drive sync explicitly

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

    let listener = TcpListener::bind("127.0.0.1:0")
        .await
        .expect("bind ephemeral port");
    let addr = listener.local_addr().expect("local addr");
    let handle = tokio::spawn(async move {
        axum::serve(listener, router).await.expect("serve");
    });
    let client = reqwest::Client::builder()
        .timeout(Duration::from_secs(30))
        .build()
        .expect("build client");
    Some(SourceServer {
        base: format!("http://{addr}"),
        client,
        _handle: handle,
    })
}

impl SourceServer {
    /// GET a path, returning (status, JSON body).
    async fn get(&self, path: &str) -> (reqwest::StatusCode, Value) {
        let r = self
            .client
            .get(format!("{}{path}", self.base))
            .send()
            .await
            .expect("send GET");
        let status = r.status();
        let body = r.json::<Value>().await.unwrap_or(Value::Null);
        (status, body)
    }

    /// POST JSON to a path, returning (status, JSON body).
    async fn post(&self, path: &str, body: &Value) -> (reqwest::StatusCode, Value) {
        let r = self
            .client
            .post(format!("{}{path}", self.base))
            .json(body)
            .send()
            .await
            .expect("send POST");
        let status = r.status();
        let body = r.json::<Value>().await.unwrap_or(Value::Null);
        (status, body)
    }
}

/// A two-column schema for a source table.
fn schema(id_col: &str, other_col: &str) -> Value {
    json!({
        "type": "struct",
        "fields": [
            { "id": 1, "name": id_col, "required": true, "type": "long" },
            { "id": 2, "name": other_col, "required": false, "type": "string" },
        ],
    })
}

#[tokio::test]
#[allow(clippy::too_many_lines)] // one narrative: sync, foreign assets, search, read-only, remove
async fn mirror_syncs_foreign_tables_searchable_and_read_only() {
    let Some(srv) = boot_source().await else {
        return;
    };

    // A unique source warehouse rooted in a tempdir (real file:// metadata).
    let root = tempfile::tempdir().expect("tempdir");
    let salt = Ulid::new().to_string().to_lowercase();
    let source_wh = format!("srcwh-{salt}");
    let storage_root = format!("file://{}", root.path().join("wh").display());
    let (status, body) = srv
        .post(
            "/api/v2/warehouses",
            &json!({ "name": source_wh, "storage_root": storage_root }),
        )
        .await;
    assert_eq!(
        status,
        reqwest::StatusCode::CREATED,
        "create source wh: {body}"
    );

    // A namespace and two tables on the source, with distinctive column names
    // so the search assertion is unambiguous.
    let (status, _) = srv
        .post(
            &format!("/v1/{source_wh}/namespaces"),
            &json!({ "namespace": ["analytics"] }),
        )
        .await;
    assert_eq!(status, reqwest::StatusCode::OK, "create source namespace");

    let orders_col = format!("order_total_{salt}");
    let customers_col = format!("customer_email_{salt}");
    for (table, col) in [("orders", &orders_col), ("customers", &customers_col)] {
        let (status, body) = srv
            .post(
                &format!("/v1/{source_wh}/namespaces/analytics/tables"),
                &json!({ "name": table, "schema": schema("id", col) }),
            )
            .await;
        assert_eq!(
            status,
            reqwest::StatusCode::OK,
            "create source table {table}: {body}"
        );
    }

    // Register a mirror pointing at THIS server's IRC base, with the source
    // warehouse as the remote prefix. Auth is disabled on the source, so the
    // mirror uses auth-mode=none.
    let mirror = format!("mir-{salt}");
    let (status, body) = srv
        .post(
            "/api/v2/mirrors",
            &json!({
                "name": mirror,
                "kind": "iceberg-rest",
                "endpoint": format!("{}/iceberg", srv.base),
                "remote_catalog": source_wh,
                "config": { "auth-mode": "none" },
                "sync_interval_s": 3600,
            }),
        )
        .await;
    assert_eq!(
        status,
        reqwest::StatusCode::CREATED,
        "create mirror: {body}"
    );

    // Run the sync synchronously.
    let (status, body) = srv
        .post(&format!("/api/v2/mirrors/{mirror}/sync"), &json!({}))
        .await;
    assert_eq!(status, reqwest::StatusCode::OK, "sync now: {body}");
    assert_eq!(
        body["synced"]["tables_inserted"], 2,
        "sync should insert both source tables as foreign assets: {body}"
    );
    assert_eq!(
        body["mirror"]["last_sync_status"], "ok",
        "mirror ok: {body}"
    );
    assert_eq!(body["mirror"]["asset_count"], 2, "asset count: {body}");

    // The mirror's foreign warehouse holds the synced namespace + tables.
    let foreign_wh = format!("mirror__{mirror}");
    let (status, body) = srv
        .get(&format!("/v1/{foreign_wh}/namespaces/analytics/tables"))
        .await;
    assert_eq!(
        status,
        reqwest::StatusCode::OK,
        "list foreign tables: {body}"
    );
    let names: Vec<&str> = body["identifiers"]
        .as_array()
        .expect("identifiers array")
        .iter()
        .filter_map(|i| i["name"].as_str())
        .collect();
    assert!(
        names.contains(&"orders") && names.contains(&"customers"),
        "foreign warehouse must list both mirrored tables, got {names:?}"
    );

    // loadTable on a foreign table returns the correct schema (proving the
    // metadata — not just the name — was mirrored).
    let (status, body) = srv
        .get(&format!(
            "/v1/{foreign_wh}/namespaces/analytics/tables/orders"
        ))
        .await;
    assert_eq!(
        status,
        reqwest::StatusCode::OK,
        "load foreign table: {body}"
    );
    let columns: Vec<&str> = body["metadata"]["schemas"][0]["fields"]
        .as_array()
        .expect("schema fields")
        .iter()
        .filter_map(|f| f["name"].as_str())
        .collect();
    assert!(
        columns.contains(&orders_col.as_str()),
        "foreign table schema must carry the source column {orders_col}, got {columns:?}"
    );

    // Search finds the mirrored table by its distinctive column name — proving
    // foreign assets are first-class to the read-side features.
    let (status, body) = srv.get(&format!("/api/v2/search?q={customers_col}")).await;
    assert_eq!(status, reqwest::StatusCode::OK, "search: {body}");
    let hit_names: Vec<&str> = body["results"]
        .as_array()
        .expect("search results array")
        .iter()
        .filter_map(|h| h["name"].as_str())
        .collect();
    assert!(
        hit_names.contains(&"customers"),
        "search for the mirrored column {customers_col} must find the foreign \
         'customers' table, got {hit_names:?} (body: {body})"
    );

    // A commit/write to a foreign table is rejected with a 409
    // CommitFailedException — the external catalog is the write authority.
    let commit_body = json!({
        "requirements": [],
        "updates": [
            { "action": "set-properties", "updates": { "note": "should be rejected" } }
        ],
    });
    let (status, body) = srv
        .post(
            &format!("/v1/{foreign_wh}/namespaces/analytics/tables/orders"),
            &commit_body,
        )
        .await;
    assert_eq!(
        status,
        reqwest::StatusCode::CONFLICT,
        "commit to a foreign table must be rejected with 409: {body}"
    );
    assert_eq!(
        body["error"]["type"], "CommitFailedException",
        "rejection must be a CommitFailedException: {body}"
    );
    assert!(
        body["error"]["message"]
            .as_str()
            .is_some_and(|m| m.contains("foreign") || m.contains("read-only")),
        "rejection message must explain the table is foreign/read-only: {body}"
    );

    // Creating a new table under the foreign warehouse is likewise rejected.
    let (status, body) = srv
        .post(
            &format!("/v1/{foreign_wh}/namespaces/analytics/tables"),
            &json!({ "name": "should_not_create", "schema": schema("id", "x") }),
        )
        .await;
    assert_eq!(
        status,
        reqwest::StatusCode::CONFLICT,
        "create under a foreign warehouse must be rejected: {body}"
    );

    // Re-syncing is incremental: nothing changed on the source, so the second
    // run reports both tables unchanged and inserts none.
    let (status, body) = srv
        .post(&format!("/api/v2/mirrors/{mirror}/sync"), &json!({}))
        .await;
    assert_eq!(status, reqwest::StatusCode::OK, "second sync: {body}");
    assert_eq!(
        body["synced"]["tables_unchanged"], 2,
        "re-sync must skip unchanged tables (incremental): {body}"
    );
    assert_eq!(
        body["synced"]["tables_inserted"], 0,
        "re-sync must insert nothing: {body}"
    );

    // Dropping a source table and re-syncing removes the foreign asset.
    let drop_status = srv
        .client
        .delete(format!(
            "{}/v1/{source_wh}/namespaces/analytics/tables/customers",
            srv.base
        ))
        .send()
        .await
        .expect("delete source table")
        .status();
    assert_eq!(
        drop_status,
        reqwest::StatusCode::NO_CONTENT,
        "drop source table"
    );

    let (status, body) = srv
        .post(&format!("/api/v2/mirrors/{mirror}/sync"), &json!({}))
        .await;
    assert_eq!(status, reqwest::StatusCode::OK, "third sync: {body}");
    assert_eq!(
        body["synced"]["tables_removed"], 1,
        "re-sync must remove the foreign asset that vanished from source: {body}"
    );

    let (status, body) = srv
        .get(&format!("/v1/{foreign_wh}/namespaces/analytics/tables"))
        .await;
    assert_eq!(
        status,
        reqwest::StatusCode::OK,
        "list after removal: {body}"
    );
    let names_after: Vec<&str> = body["identifiers"]
        .as_array()
        .expect("identifiers array")
        .iter()
        .filter_map(|i| i["name"].as_str())
        .collect();
    assert!(
        names_after.contains(&"orders") && !names_after.contains(&"customers"),
        "after removal only 'orders' remains foreign, got {names_after:?}"
    );
}
