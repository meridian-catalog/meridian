//! End-to-end tests for the SQL workbench (Pillar L, L-F1/L-F3): the governed
//! query API, query history, saved queries, and the notebook-handoff snippet.
//!
//! These prove the workbench runs real SQL on the built-in executor under the
//! **same Pillar-D policies** the agent gateway enforces, but with the
//! workbench's mask semantics: a masked column's **value is transformed**
//! (hash/null), not dropped — a human sees masked values, unlike the agent path
//! which drops the column. History is recorded per principal; saved queries
//! round-trip; the snippet generator emits engine connection code (no secret).
//!
//! Require a running Postgres and `DATABASE_URL`; without it they skip. Auth is
//! OIDC via the in-process test IdP. Test isolation: every warehouse / namespace
//! carries a ULID suffix, and assertions are scoped to this test's own rows.

// The fixture writes Parquet + manifest Avro with the same integer-cast idioms
// the other fixture-heavy tests use; relax the pedantic numeric-cast lints here.
#![allow(
    clippy::cast_possible_truncation,
    clippy::cast_possible_wrap,
    clippy::cast_sign_loss,
    clippy::cast_precision_loss,
    clippy::too_many_lines,
    clippy::doc_markdown
)]

#[allow(dead_code)]
mod idp;

use std::collections::BTreeMap;
use std::sync::Arc;

use arrow_array::{ArrayRef, Float64Array, Int64Array, RecordBatch, StringArray};
use arrow_schema::{DataType, Field, Schema as ArrowSchema};
use axum::Router;
use axum::body::Body;
use axum::http::{Request, StatusCode};
use bytes::Bytes;
use http_body_util::BodyExt;
use idp::{AUDIENCE, KID1, TestIdp};
use meridian_common::AppConfig;
use meridian_common::config::{AuthMode, OidcIssuerConfig};
use meridian_iceberg::manifest::{
    DataFile, DataFileContent, ManifestContentType, ManifestEntry, ManifestEntryStatus,
    ManifestFile, ManifestListWriteParams, ManifestWriteParams, PartitionTuple,
    partition_field_types, write_manifest, write_manifest_list,
};
use meridian_iceberg::spec::{
    PartitionSpec, PrimitiveType, Schema, Snapshot, SnapshotRef, StructField, TableMetadata, Type,
};
use meridian_server::{AppState, build_router};
use meridian_store::tenancy;
use parquet::arrow::{ArrowWriter, PARQUET_FIELD_ID_META_KEY};
use serde_json::{Value, json};
use tower::ServiceExt;
use ulid::Ulid;

// ---------------------------------------------------------------------------
// Harness
// ---------------------------------------------------------------------------

struct Ctx {
    router: Router,
    admin_token: String,
    warehouse: String,
    root: tempfile::TempDir,
}

async fn ctx() -> Option<Ctx> {
    let Ok(url) = std::env::var("DATABASE_URL") else {
        eprintln!("skipping workbench API test: DATABASE_URL is not set");
        return None;
    };
    let idp = TestIdp::start(&[KID1]).await;
    let mut config = AppConfig::default();
    config.database.url = url;
    config.auth.mode = AuthMode::Oidc;
    config.auth.oidc.require_https_issuers = false;
    config.auth.oidc.issuers.push(OidcIssuerConfig {
        issuer_url: idp.issuer.clone(),
        audience: AUDIENCE.to_owned(),
        jwks_uri: None,
    });

    let pool = meridian_store::connect(&config.database)
        .await
        .expect("connect");
    meridian_store::MIGRATOR.run(&pool).await.expect("migrate");

    let router = build_router(AppState {
        pool: pool.clone(),
        config: Arc::new(config),
    });

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

    let root = tempfile::tempdir().expect("tempdir");
    let warehouse = format!("wh-wb-{}", Ulid::new()).to_lowercase();
    let storage_root = format!("file://{}", root.path().join("warehouse").display());
    let (status, body) = send(
        &router,
        "POST",
        "/api/v2/warehouses",
        Some(&admin_token),
        Some(&json!({ "name": warehouse, "storage_root": storage_root })),
    )
    .await;
    assert_eq!(status, StatusCode::CREATED, "create warehouse: {body}");

    Some(Ctx {
        router,
        admin_token,
        warehouse,
        root,
    })
}

async fn send(
    router: &Router,
    method: &str,
    uri: &str,
    token: Option<&str>,
    body: Option<&Value>,
) -> (StatusCode, Value) {
    let mut builder = Request::builder().method(method).uri(uri);
    if let Some(token) = token {
        builder = builder.header("authorization", format!("Bearer {token}"));
    }
    let request = match body {
        Some(body) => builder
            .header("content-type", "application/json")
            .body(Body::from(body.to_string())),
        None => builder.body(Body::empty()),
    }
    .expect("build request");
    let response = router.clone().oneshot(request).await.expect("router call");
    let status = response.status();
    let bytes = response
        .into_body()
        .collect()
        .await
        .expect("body")
        .to_bytes();
    let value = if bytes.is_empty() {
        Value::Null
    } else {
        serde_json::from_slice(&bytes).expect("json body")
    };
    (status, value)
}

fn connect_storage(ctx: &Ctx) -> Arc<dyn meridian_storage::Storage> {
    let root = format!("file://{}", ctx.root.path().join("warehouse").display());
    meridian_storage::StorageProfile::parse(&root, &BTreeMap::new())
        .expect("parse profile")
        .connect()
        .expect("connect")
}

// A tiny real table: id (long), region (string), amount (double), unpartitioned,
// one data file with 4 rows, all region 'eu'.
async fn register_table(ctx: &Ctx, ns: &str, table: &str) {
    let storage = connect_storage(ctx);
    let loc = format!(
        "file://{}",
        ctx.root
            .path()
            .join("warehouse")
            .join(ns)
            .join(table)
            .display()
    );
    let schema = Schema::new(vec![
        StructField::optional(1, "id", Type::Primitive(PrimitiveType::Long)),
        StructField::optional(2, "region", Type::Primitive(PrimitiveType::String)),
        StructField::optional(3, "amount", Type::Primitive(PrimitiveType::Double)),
    ])
    .with_schema_id(0);
    let schema_json = serde_json::to_string(&schema).expect("schema json");
    let spec = PartitionSpec::unpartitioned(0);
    let types = partition_field_types(&spec.fields, &schema).expect("types");

    // Parquet with field ids.
    let arrow_schema = {
        let field = |name: &str, dt: DataType, id: i32| {
            let mut md = std::collections::HashMap::new();
            md.insert(PARQUET_FIELD_ID_META_KEY.to_string(), id.to_string());
            Field::new(name, dt, true).with_metadata(md)
        };
        Arc::new(ArrowSchema::new(vec![
            field("id", DataType::Int64, 1),
            field("region", DataType::Utf8, 2),
            field("amount", DataType::Float64, 3),
        ]))
    };
    let ids: ArrayRef = Arc::new(Int64Array::from(vec![1, 2, 3, 4]));
    let regions: ArrayRef = Arc::new(StringArray::from(vec!["eu", "eu", "eu", "eu"]));
    let amounts: ArrayRef = Arc::new(Float64Array::from(vec![10.0, 20.0, 30.0, 40.0]));
    let batch =
        RecordBatch::try_new(arrow_schema.clone(), vec![ids, regions, amounts]).expect("batch");
    let mut buf = Vec::new();
    let mut w = ArrowWriter::try_new(&mut buf, arrow_schema, None).expect("writer");
    w.write(&batch).expect("write");
    w.close().expect("close");
    let data_path = format!("{loc}/data/f-0.parquet");
    let size = buf.len() as i64;
    storage
        .write(&data_path, Bytes::from(buf))
        .await
        .expect("write parquet");

    let snapshot_id = 100i64;
    let seq = 1i64;
    let entry = ManifestEntry {
        status: ManifestEntryStatus::Added,
        snapshot_id: Some(snapshot_id),
        sequence_number: Some(seq),
        file_sequence_number: Some(seq),
        data_file: DataFile {
            content: DataFileContent::Data,
            file_path: data_path,
            file_format: "PARQUET".to_owned(),
            partition: PartitionTuple { fields: vec![] },
            record_count: 4,
            file_size_in_bytes: size,
            column_sizes: None,
            value_counts: None,
            null_value_counts: None,
            nan_value_counts: None,
            lower_bounds: None,
            upper_bounds: None,
            key_metadata: None,
            split_offsets: None,
            equality_ids: None,
            sort_order_id: None,
            first_row_id: None,
            referenced_data_file: None,
            content_offset: None,
            content_size_in_bytes: None,
        },
    };
    let manifest_path = format!("{loc}/metadata/data-m0.avro");
    let manifest_bytes = write_manifest(&ManifestWriteParams {
        format_version: 2,
        content: ManifestContentType::Data,
        schema_json: &schema_json,
        schema_id: Some(0),
        partition_spec_id: 0,
        partition_fields: &spec.fields,
        partition_types: &types,
        entries: std::slice::from_ref(&entry),
    })
    .expect("write manifest");
    storage
        .write(&manifest_path, Bytes::from(manifest_bytes.clone()))
        .await
        .expect("write manifest");
    let manifest_file = ManifestFile {
        manifest_path: manifest_path.clone(),
        manifest_length: manifest_bytes.len() as i64,
        partition_spec_id: 0,
        content: ManifestContentType::Data,
        sequence_number: seq,
        min_sequence_number: seq,
        added_snapshot_id: snapshot_id,
        added_files_count: Some(1),
        existing_files_count: Some(0),
        deleted_files_count: Some(0),
        added_rows_count: Some(4),
        existing_rows_count: Some(0),
        deleted_rows_count: Some(0),
        partitions: None,
        key_metadata: None,
        first_row_id: None,
    };
    let list_path = format!("{loc}/metadata/snap-{snapshot_id}-1-list.avro");
    let list_bytes = write_manifest_list(&ManifestListWriteParams {
        format_version: 2,
        snapshot_id,
        parent_snapshot_id: None,
        sequence_number: Some(seq),
        manifests: &[manifest_file],
    })
    .expect("write list");
    storage
        .write(&list_path, Bytes::from(list_bytes))
        .await
        .expect("write list");

    let mut summary = BTreeMap::new();
    summary.insert("operation".to_owned(), "append".to_owned());
    let snapshot = Snapshot {
        snapshot_id,
        parent_snapshot_id: None,
        sequence_number: Some(seq),
        timestamp_ms: 1_700_000_000_000,
        manifest_list: Some(list_path.clone()),
        summary: Some(summary),
        schema_id: Some(0),
        first_row_id: None,
        added_rows: None,
        extra: serde_json::Map::new(),
    };
    let mut refs = BTreeMap::new();
    refs.insert(
        "main".to_owned(),
        SnapshotRef {
            snapshot_id,
            ref_type: meridian_iceberg::spec::RefType::Branch,
            min_snapshots_to_keep: None,
            max_snapshot_age_ms: None,
            max_ref_age_ms: None,
            extra: serde_json::Map::new(),
        },
    );
    let metadata = TableMetadata {
        format_version: 2,
        table_uuid: uuid::Uuid::new_v4(),
        location: loc.clone(),
        last_sequence_number: Some(seq),
        next_row_id: None,
        last_updated_ms: 1_700_000_000_000,
        last_column_id: 3,
        schemas: vec![schema],
        current_schema_id: 0,
        partition_specs: vec![spec],
        default_spec_id: 0,
        last_partition_id: 999,
        sort_orders: vec![meridian_iceberg::spec::SortOrder::unsorted()],
        default_sort_order_id: 0,
        properties: None,
        current_snapshot_id: Some(snapshot_id),
        snapshots: Some(vec![snapshot]),
        snapshot_log: None,
        metadata_log: None,
        refs: Some(refs),
        statistics: None,
        partition_statistics: None,
        encryption_keys: None,
        extra: serde_json::Map::new(),
    };
    let metadata_location = format!("{loc}/metadata/00000-{}.metadata.json", Ulid::new());
    meridian_storage::write_table_metadata(storage.as_ref(), &metadata_location, &metadata)
        .await
        .expect("write metadata");

    let (status, _) = send(
        &ctx.router,
        "POST",
        &format!("/v1/{}/namespaces", ctx.warehouse),
        Some(&ctx.admin_token),
        Some(&json!({ "namespace": [ns] })),
    )
    .await;
    assert!(status == StatusCode::OK || status == StatusCode::CONFLICT);
    let (status, body) = send(
        &ctx.router,
        "POST",
        &format!("/v1/{}/namespaces/{ns}/register", ctx.warehouse),
        Some(&ctx.admin_token),
        Some(&json!({ "name": table, "metadata-location": metadata_location })),
    )
    .await;
    assert_eq!(status, StatusCode::OK, "register: {body}");
}

/// Masks the `amount` column with a NULL mask bound to a `pii:med` tag, so a
/// non-exempt principal sees `amount` **present but nulled** (value-preserving —
/// the column stays), not dropped. (A NULL mask keeps the column; a HASH/partial
/// mask on a numeric column fails closed to a drop, which is a different, tested
/// behavior — value-preservation is what distinguishes the workbench path from
/// the agent path, so we use the mask kind that preserves the column here.)
async fn hash_mask_amount(ctx: &Ctx, ns: &str, table: &str) {
    let admin = Some(ctx.admin_token.as_str());
    let key = format!("pii-{}", Ulid::new()).to_lowercase();
    let (s, b) = send(
        &ctx.router,
        "POST",
        "/api/v2/governance/tags",
        admin,
        Some(&json!({ "key": key, "value": "med" })),
    )
    .await;
    assert_eq!(s, StatusCode::CREATED, "tag: {b}");
    let tag_id = b["id"].as_str().unwrap().to_owned();
    let rendered = format!("{key}:med");
    let (s, b) = send(
        &ctx.router,
        "POST",
        "/api/v2/governance/tags/assignments",
        admin,
        Some(&json!({
            "tag_id": tag_id,
            "target": { "securable_type": "column", "warehouse": ctx.warehouse,
                        "namespace": ns, "table": table, "column": "amount" }
        })),
    )
    .await;
    assert_eq!(s, StatusCode::CREATED, "assign: {b}");
    let (s, b) = send(
        &ctx.router,
        "POST",
        "/api/v2/governance/policies",
        admin,
        Some(&json!({
            "name": format!("hashamt-{}", Ulid::new()),
            "kind": "column_mask",
            "definition": { "type": "tag_column_mask", "id": "null-amount",
                            "tag": rendered, "exempt_groups": [], "mask": { "kind": "null" } }
        })),
    )
    .await;
    assert_eq!(s, StatusCode::CREATED, "policy: {b}");
    let policy_id = b["id"].as_str().unwrap().to_owned();
    let (s, b) = send(
        &ctx.router,
        "POST",
        &format!("/api/v2/governance/policies/{policy_id}/bindings"),
        admin,
        Some(&json!({ "target_type": "tag", "tag_id": tag_id })),
    )
    .await;
    assert_eq!(s, StatusCode::CREATED, "bind: {b}");
}

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

#[tokio::test]
async fn workbench_query_returns_rows_and_records_history() {
    let Some(ctx) = ctx().await else {
        return;
    };
    let ns = format!("ns{}", Ulid::new()).to_lowercase();
    register_table(&ctx, &ns, "sales").await;

    let (status, body) = send(
        &ctx.router,
        "POST",
        "/api/v2/workbench/query",
        Some(&ctx.admin_token),
        Some(&json!({ "warehouse": ctx.warehouse, "sql": format!("SELECT id, amount FROM {ns}.sales ORDER BY id") })),
    )
    .await;
    assert_eq!(status, StatusCode::OK, "workbench query: {body}");
    assert_eq!(body["row_count"], 4, "four rows: {body}");
    assert_eq!(body["rows"][0]["id"], 1);
    assert_eq!(body["rows"][0]["amount"], 10.0);
    // Provenance names the table + a snapshot id.
    assert_eq!(
        body["provenance"]["tables"][0]["name"],
        format!("{ns}.sales")
    );
    assert!(body["provenance"]["tables"][0]["snapshot_id"].is_number());
    assert!(body["duration_ms"].is_number());

    // History recorded the run for this principal.
    let (status, hist) = send(
        &ctx.router,
        "GET",
        "/api/v2/workbench/history",
        Some(&ctx.admin_token),
        None,
    )
    .await;
    assert_eq!(status, StatusCode::OK, "history: {hist}");
    let items = hist["history"].as_array().expect("history array");
    assert!(
        items
            .iter()
            .any(|h| h["status"] == "ok" && h["sql"].as_str().is_some_and(|s| s.contains("FROM"))),
        "the run is in history as ok: {hist}"
    );
}

#[tokio::test]
async fn workbench_masks_column_value_preserving() {
    let Some(ctx) = ctx().await else {
        return;
    };
    let ns = format!("ns{}", Ulid::new()).to_lowercase();
    register_table(&ctx, &ns, "sales").await;
    hash_mask_amount(&ctx, &ns, "sales").await;

    // Admin is not exempt from the mask (no exempt group), so `amount` is
    // masked to NULL — present as a masked value, NOT dropped (workbench
    // semantics, unlike the agent path which drops the column entirely).
    let (status, body) = send(
        &ctx.router,
        "POST",
        "/api/v2/workbench/query",
        Some(&ctx.admin_token),
        Some(&json!({ "warehouse": ctx.warehouse, "sql": format!("SELECT * FROM {ns}.sales ORDER BY id") })),
    )
    .await;
    assert_eq!(status, StatusCode::OK, "masked workbench query: {body}");
    let cols: Vec<String> = body["columns"]
        .as_array()
        .unwrap()
        .iter()
        .filter_map(|c| c["name"].as_str().map(str::to_owned))
        .collect();
    assert!(
        cols.contains(&"amount".to_owned()),
        "workbench keeps the masked column PRESENT (value-preserving), not dropped: {cols:?}"
    );
    // The value is masked to NULL, not the cleartext 10.0 — the column stays,
    // the value is hidden.
    let amount0 = &body["rows"][0]["amount"];
    assert!(
        amount0.is_null(),
        "the masked amount is NULL (masked value), not the raw number: {amount0}"
    );
    // Provenance records the mask.
    assert!(
        body["provenance"]["masked_columns"]
            .as_array()
            .unwrap()
            .iter()
            .any(|c| c == "amount"),
        "provenance records `amount` masked: {}",
        body["provenance"]
    );
}

#[tokio::test]
async fn workbench_refuses_bad_sql_and_records_error() {
    let Some(ctx) = ctx().await else {
        return;
    };
    // A write statement is refused (read-only gate) with a 400.
    let (status, body) = send(
        &ctx.router,
        "POST",
        "/api/v2/workbench/query",
        Some(&ctx.admin_token),
        Some(&json!({ "sql": "DELETE FROM whatever" })),
    )
    .await;
    assert_eq!(status, StatusCode::BAD_REQUEST, "write refused: {body}");

    // The error is in history.
    let (_, hist) = send(
        &ctx.router,
        "GET",
        "/api/v2/workbench/history",
        Some(&ctx.admin_token),
        None,
    )
    .await;
    assert!(
        hist["history"]
            .as_array()
            .unwrap()
            .iter()
            .any(|h| h["status"] == "error"),
        "the refused query is recorded as error: {hist}"
    );
}

#[tokio::test]
async fn saved_queries_round_trip() {
    let Some(ctx) = ctx().await else {
        return;
    };
    let name = format!("Daily revenue {}", Ulid::new());

    // Create.
    let (status, saved) = send(
        &ctx.router,
        "POST",
        "/api/v2/workbench/saved",
        Some(&ctx.admin_token),
        Some(&json!({
            "name": name,
            "sql": "SELECT 1",
            "warehouse": ctx.warehouse,
            "description": "a saved query"
        })),
    )
    .await;
    assert_eq!(status, StatusCode::CREATED, "save: {saved}");
    let id = saved["id"].as_str().expect("id").to_owned();
    assert_eq!(saved["name"], name);

    // List includes it.
    let (status, list) = send(
        &ctx.router,
        "GET",
        "/api/v2/workbench/saved",
        Some(&ctx.admin_token),
        None,
    )
    .await;
    assert_eq!(status, StatusCode::OK);
    assert!(
        list["saved_queries"]
            .as_array()
            .unwrap()
            .iter()
            .any(|q| q["id"] == id),
        "saved query is listed: {list}"
    );

    // Get by id.
    let (status, got) = send(
        &ctx.router,
        "GET",
        &format!("/api/v2/workbench/saved/{id}"),
        Some(&ctx.admin_token),
        None,
    )
    .await;
    assert_eq!(status, StatusCode::OK, "get: {got}");
    assert_eq!(got["sql"], "SELECT 1");

    // Duplicate name conflicts.
    let (status, _) = send(
        &ctx.router,
        "POST",
        "/api/v2/workbench/saved",
        Some(&ctx.admin_token),
        Some(&json!({ "name": name, "sql": "SELECT 2" })),
    )
    .await;
    assert_eq!(status, StatusCode::CONFLICT, "duplicate name conflicts");

    // Delete.
    let (status, _) = send(
        &ctx.router,
        "DELETE",
        &format!("/api/v2/workbench/saved/{id}"),
        Some(&ctx.admin_token),
        None,
    )
    .await;
    assert_eq!(status, StatusCode::NO_CONTENT, "delete");

    // Gone.
    let (status, _) = send(
        &ctx.router,
        "GET",
        &format!("/api/v2/workbench/saved/{id}"),
        Some(&ctx.admin_token),
        None,
    )
    .await;
    assert_eq!(status, StatusCode::NOT_FOUND, "deleted query is gone");
}

#[tokio::test]
async fn snippet_generator_emits_connection_code() {
    let Some(ctx) = ctx().await else {
        return;
    };
    let ns = format!("ns{}", Ulid::new()).to_lowercase();
    register_table(&ctx, &ns, "sales").await;

    let (status, body) = send(
        &ctx.router,
        "POST",
        "/api/v2/workbench/snippet",
        Some(&ctx.admin_token),
        Some(&json!({ "warehouse": ctx.warehouse, "namespace": ns, "table": "sales" })),
    )
    .await;
    assert_eq!(status, StatusCode::OK, "snippet: {body}");
    assert_eq!(body["table"], format!("{ns}.sales"));
    // PyIceberg / Daft / Pandas snippets are present and reference the table.
    for engine in ["pyiceberg", "daft", "pandas"] {
        let code = body["snippets"][engine].as_str().unwrap_or_default();
        assert!(
            code.contains(&format!("{ns}.sales")),
            "{engine} snippet references the table: {code}"
        );
    }
    // No raw secret is embedded — the snippet uses an OIDC-token placeholder.
    let all = body["snippets"].to_string();
    assert!(
        all.contains("<YOUR_OIDC_TOKEN>"),
        "the snippet uses a token placeholder, not an embedded secret"
    );
}
