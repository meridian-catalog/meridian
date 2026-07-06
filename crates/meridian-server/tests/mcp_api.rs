//! End-to-end tests for the MCP agent gateway (Pillar H, H-F1/H-F2/H-F4 — the
//! agent firewall). These exercise the real `/mcp` Streamable-HTTP endpoint
//! over the full middleware stack, plus the `/api/v2/agents` control plane, and
//! prove the properties the firewall exists to guarantee:
//!
//!   1. MCP `initialize` + `tools/list` + `tools/call` speak the protocol
//!      (JSON-RPC 2.0, spec `2025-06-18`).
//!   2. A registered agent gets **governed** context: a policy-masked column is
//!      **ABSENT** from the returned schema (not nulled) — the prompt-leak
//!      guarantee (H-F2).
//!   3. Budget enforcement: exceeding queries/hour yields a graceful,
//!      agent-relayable refusal, and it is audited (H-F4).
//!   4. The kill switch: a suspended agent has every tool refused (H-F4).
//!   5. Every tool call is written to the activity ledger with its decision
//!      AND the tamper-evident audit chain (the chain is the product, H-F4).
//!
//! Require a running Postgres and `DATABASE_URL`; without it they skip. Auth is
//! OIDC via the in-process test IdP — agents are first-class principals, so an
//! agent token is an ordinary IdP token whose principal is registered as an
//! agent. Test isolation (M3/M4/M5 rules): every warehouse / namespace / agent
//! subject carries a ULID suffix, and assertions are scoped to this test's own
//! ids (agent id, activity rows) — never global counts.

// The real-data fixture writes Parquet + manifest Avro with the same
// integer-cast idioms the maintenance-worker fixture uses; relax the pedantic
// numeric-cast lints for this test binary (the crate's `src/` stays strict).
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

use std::sync::Arc;

use arrow_array::{ArrayRef, Float64Array, Int64Array, RecordBatch, StringArray};
use arrow_schema::{DataType, Field, Schema as ArrowSchema};
use axum::Router;
use axum::body::Body;
use axum::http::{Request, StatusCode};
use bytes::Bytes;
use http_body_util::BodyExt;
use idp::{AUDIENCE, KID1, TestIdp};
use meridian_bench::fixture::{self, SyntheticSpec};
use meridian_common::AppConfig;
use meridian_common::config::{AuthMode, OidcIssuerConfig};
use meridian_iceberg::manifest::{
    DataFile, DataFileContent, ManifestContentType, ManifestEntry, ManifestEntryStatus,
    ManifestFile, ManifestListWriteParams, ManifestWriteParams, PartitionTuple, PartitionValue,
    partition_field_types, write_manifest, write_manifest_list,
};
use meridian_iceberg::spec::{
    PartitionField, PartitionSpec, PrimitiveType, Schema, Snapshot, SnapshotRef, StructField,
    TableMetadata, Transform, Type,
};
use meridian_iceberg::value::Datum;
use meridian_server::{AppState, build_router};
use meridian_store::tenancy;
use parquet::arrow::{ArrowWriter, PARQUET_FIELD_ID_META_KEY};
use serde_json::{Value, json};
use sqlx::PgPool;
use tower::ServiceExt;
use ulid::Ulid;

// ---------------------------------------------------------------------------
// Harness
// ---------------------------------------------------------------------------

struct Ctx {
    router: Router,
    pool: PgPool,
    idp: TestIdp,
    admin_token: String,
    warehouse: String,
    root: tempfile::TempDir,
}

async fn oidc_ctx() -> Option<Ctx> {
    let Ok(url) = std::env::var("DATABASE_URL") else {
        eprintln!("skipping MCP API test: DATABASE_URL is not set");
        return None;
    };
    let idp = TestIdp::start(&[KID1]).await;
    let issuer_url = idp.issuer.clone();

    let mut config = AppConfig::default();
    config.database.url = url;
    config.auth.mode = AuthMode::Oidc;
    config.auth.oidc.require_https_issuers = false;
    config.auth.oidc.issuers.push(OidcIssuerConfig {
        issuer_url,
        audience: AUDIENCE.to_owned(),
        jwks_uri: None,
    });

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

    let root = tempfile::tempdir().expect("create tempdir");
    let warehouse = format!("wh-mcp-{}", Ulid::new()).to_lowercase();
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
        pool,
        idp,
        admin_token,
        warehouse,
        root,
    })
}

/// A plain authenticated request against a management/IRC route.
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

    let response = router
        .clone()
        .oneshot(request)
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

/// A JSON-RPC POST to `/mcp` with the agent's bearer token. Returns
/// (`http_status`, `json_body`). The MCP `Accept` header lists both content
/// types the transport requires.
async fn mcp(router: &Router, token: &str, message: &Value) -> (StatusCode, Value) {
    mcp_with_purpose(router, token, message, None).await
}

async fn mcp_with_purpose(
    router: &Router,
    token: &str,
    message: &Value,
    purpose: Option<&str>,
) -> (StatusCode, Value) {
    let mut builder = Request::builder()
        .method("POST")
        .uri("/mcp")
        .header("authorization", format!("Bearer {token}"))
        .header("content-type", "application/json")
        .header("accept", "application/json, text/event-stream");
    if let Some(purpose) = purpose {
        builder = builder.header("x-meridian-purpose", purpose);
    }
    let request = builder
        .body(Body::from(message.to_string()))
        .expect("build MCP request");
    let response = router
        .clone()
        .oneshot(request)
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
        serde_json::from_slice(&bytes).expect("MCP body is JSON")
    };
    (status, value)
}

fn table_location(ctx: &Ctx, ns: &str, table: &str) -> String {
    format!(
        "file://{}",
        ctx.root
            .path()
            .join("warehouse")
            .join(ns)
            .join(table)
            .display()
    )
}

/// Writes a fixture (columns: id, region, category, amount, ts) and registers
/// it as `{ns}.{table}` (admin). Returns the table's internal id.
async fn register_fixture(ctx: &Ctx, ns: &str, table: &str) -> String {
    let spec = SyntheticSpec {
        table_location: table_location(ctx, ns, table),
        data_files: 6,
        partitions: 2,
        files_per_manifest: 3,
        rows_per_file: 50,
    };
    let built = fixture::synthetic_table(&spec).expect("generate fixture");
    fixture::write_local(&built.files).expect("write fixture files");

    let (status, _) = send(
        &ctx.router,
        "POST",
        &format!("/v1/{}/namespaces", ctx.warehouse),
        Some(&ctx.admin_token),
        Some(&json!({ "namespace": [ns] })),
    )
    .await;
    assert!(
        status == StatusCode::OK || status == StatusCode::CONFLICT,
        "create namespace"
    );
    let (status, body) = send(
        &ctx.router,
        "POST",
        &format!("/v1/{}/namespaces/{ns}/register", ctx.warehouse),
        Some(&ctx.admin_token),
        Some(&json!({ "name": table, "metadata-location": built.metadata_location })),
    )
    .await;
    assert_eq!(status, StatusCode::OK, "register fixture: {body}");

    sqlx::query_scalar(
        "SELECT t.id FROM tables t JOIN namespaces n ON n.id = t.namespace_id
         WHERE n.warehouse_id = (SELECT id FROM warehouses WHERE name = $1)
           AND t.name = $2",
    )
    .bind(&ctx.warehouse)
    .bind(table)
    .fetch_one(&ctx.pool)
    .await
    .expect("table id")
}

// ---------------------------------------------------------------------------
// Real-data fixture (for the query executor, which reads actual Parquet).
//
// The synthetic fixture above references data-file paths that are never
// materialized — fine for scan-*planning* tests (they read only manifests), but
// the small-scan executor reads the data files, so `run_sql`/`preview_table`
// tests need a table with real Parquet. This builds one: schema (id, region,
// category, amount, ts) identity-partitioned by `region`, with 150 rows in
// `region_000` and 150 in `region_001` (300 total), and registers it over HTTP.
// ---------------------------------------------------------------------------

/// Connects a filesystem `Storage` handle at the test warehouse root.
fn connect_storage(ctx: &Ctx) -> Arc<dyn meridian_storage::Storage> {
    let root = format!("file://{}", ctx.root.path().join("warehouse").display());
    meridian_storage::StorageProfile::parse(&root, &std::collections::BTreeMap::new())
        .expect("parse storage profile")
        .connect()
        .expect("connect storage")
}

/// The real-data fixture schema: id (long, 1), region (string, 2), category
/// (string, 3), amount (double, 4), ts (timestamp, 5) — the columns the mask /
/// row-filter helpers target.
fn real_schema() -> Schema {
    Schema::new(vec![
        StructField::optional(1, "id", Type::Primitive(PrimitiveType::Long)),
        StructField::optional(2, "region", Type::Primitive(PrimitiveType::String)),
        StructField::optional(3, "category", Type::Primitive(PrimitiveType::String)),
        StructField::optional(4, "amount", Type::Primitive(PrimitiveType::Double)),
        StructField::optional(5, "ts", Type::Primitive(PrimitiveType::Timestamp)),
    ])
    .with_schema_id(0)
}

/// Identity(region) partition spec (spec id 0, partition field id 1000).
fn real_spec() -> PartitionSpec {
    let mut field = PartitionField::new(2, "region", Transform::Identity);
    field.field_id = Some(1000);
    let mut spec = PartitionSpec::new(vec![field]);
    spec.spec_id = Some(0);
    spec
}

fn real_arrow_schema() -> Arc<ArrowSchema> {
    let field = |name: &str, dt: DataType, id: i32| {
        let mut md = std::collections::HashMap::new();
        md.insert(PARQUET_FIELD_ID_META_KEY.to_string(), id.to_string());
        Field::new(name, dt, true).with_metadata(md)
    };
    Arc::new(ArrowSchema::new(vec![
        field("id", DataType::Int64, 1),
        field("region", DataType::Utf8, 2),
        field("category", DataType::Utf8, 3),
        field("amount", DataType::Float64, 4),
        field(
            "ts",
            DataType::Timestamp(arrow_schema::TimeUnit::Microsecond, None),
            5,
        ),
    ]))
}

/// Writes `count` rows for one region to a Parquet buffer (ids offset by
/// `id_base`), field ids preserved.
fn write_region_parquet(region: &str, id_base: i64, count: i64) -> Bytes {
    let schema = real_arrow_schema();
    let ids: Vec<i64> = (0..count).map(|i| id_base + i).collect();
    let regions: Vec<String> = std::iter::repeat_n(region.to_owned(), count as usize).collect();
    let categories: Vec<String> = (0..count).map(|i| format!("cat_{:02}", i % 5)).collect();
    let amounts: Vec<f64> = (0..count).map(|i| (i as f64) * 1.5).collect();
    let ts: Vec<i64> = (0..count).map(|i| 1_700_000_000_000_000 + i).collect();

    let id_col: ArrayRef = Arc::new(Int64Array::from(ids));
    let region_col: ArrayRef = Arc::new(StringArray::from(regions));
    let cat_col: ArrayRef = Arc::new(StringArray::from(categories));
    let amount_col: ArrayRef = Arc::new(Float64Array::from(amounts));
    let ts_col: ArrayRef = Arc::new(
        arrow_array::TimestampMicrosecondArray::from(ts).with_data_type(DataType::Timestamp(
            arrow_schema::TimeUnit::Microsecond,
            None,
        )),
    );
    let batch = RecordBatch::try_new(
        schema.clone(),
        vec![id_col, region_col, cat_col, amount_col, ts_col],
    )
    .expect("batch");
    let mut buf = Vec::new();
    let mut w = ArrowWriter::try_new(&mut buf, schema, None).expect("writer");
    w.write(&batch).expect("write");
    w.close().expect("close");
    Bytes::from(buf)
}

/// Builds a manifest entry for one region's data file.
fn region_entry(
    path: String,
    region: &str,
    rows: i64,
    size: i64,
    seq: i64,
    snap: i64,
) -> ManifestEntry {
    ManifestEntry {
        status: ManifestEntryStatus::Added,
        snapshot_id: Some(snap),
        sequence_number: Some(seq),
        file_sequence_number: Some(seq),
        data_file: DataFile {
            content: DataFileContent::Data,
            file_path: path,
            file_format: "PARQUET".to_owned(),
            partition: PartitionTuple {
                fields: vec![PartitionValue {
                    field_id: 1000,
                    name: "region".to_owned(),
                    value: Some(Datum::String(region.to_owned())),
                }],
            },
            record_count: rows,
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
    }
}

/// Builds a real-data table (region_000: 150 rows, region_001: 150 rows) with
/// actual Parquet + manifest Avro on fs storage, registers it over HTTP, and
/// returns the internal table id.
async fn register_real_fixture(ctx: &Ctx, ns: &str, table: &str) -> String {
    let storage = connect_storage(ctx);
    let loc = table_location(ctx, ns, table);
    let schema = real_schema();
    let schema_json = serde_json::to_string(&schema).expect("schema json");
    let spec = real_spec();
    let types = partition_field_types(&spec.fields, &schema).expect("partition types");
    let snapshot_id = 100i64;
    let seq = 1i64;

    // Two regions, one data file each (150 rows).
    let mut entries = Vec::new();
    for (idx, region) in ["region_000", "region_001"].into_iter().enumerate() {
        let path = format!("{loc}/data/region={region}/f-{idx:06}.parquet");
        let bytes = write_region_parquet(region, (idx as i64) * 1000, 150);
        let size = bytes.len() as i64;
        storage.write(&path, bytes).await.expect("write parquet");
        entries.push(region_entry(path, region, 150, size, seq, snapshot_id));
    }

    let manifest_path = format!("{loc}/metadata/data-m0.avro");
    let manifest_bytes = write_manifest(&ManifestWriteParams {
        format_version: 2,
        content: ManifestContentType::Data,
        schema_json: &schema_json,
        schema_id: Some(0),
        partition_spec_id: 0,
        partition_fields: &spec.fields,
        partition_types: &types,
        entries: &entries,
    })
    .expect("write manifest");
    storage
        .write(&manifest_path, Bytes::from(manifest_bytes.clone()))
        .await
        .expect("write manifest file");

    let total_rows: i64 = entries.iter().map(|e| e.data_file.record_count).sum();
    let manifest_file = ManifestFile {
        manifest_path: manifest_path.clone(),
        manifest_length: manifest_bytes.len() as i64,
        partition_spec_id: 0,
        content: ManifestContentType::Data,
        sequence_number: seq,
        min_sequence_number: seq,
        added_snapshot_id: snapshot_id,
        added_files_count: Some(entries.len() as i32),
        existing_files_count: Some(0),
        deleted_files_count: Some(0),
        added_rows_count: Some(total_rows),
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
        .expect("write list file");

    let metadata = real_metadata(&loc, snapshot_id, seq, &list_path);
    let metadata_location = format!("{loc}/metadata/00000-{}.metadata.json", Ulid::new());
    meridian_storage::write_table_metadata(storage.as_ref(), &metadata_location, &metadata)
        .await
        .expect("write metadata");

    // Create the namespace + register the table over HTTP (the register route
    // indexes the snapshots, exactly like production).
    let (status, _) = send(
        &ctx.router,
        "POST",
        &format!("/v1/{}/namespaces", ctx.warehouse),
        Some(&ctx.admin_token),
        Some(&json!({ "namespace": [ns] })),
    )
    .await;
    assert!(
        status == StatusCode::OK || status == StatusCode::CONFLICT,
        "create namespace"
    );
    let (status, body) = send(
        &ctx.router,
        "POST",
        &format!("/v1/{}/namespaces/{ns}/register", ctx.warehouse),
        Some(&ctx.admin_token),
        Some(&json!({ "name": table, "metadata-location": metadata_location })),
    )
    .await;
    assert_eq!(status, StatusCode::OK, "register real fixture: {body}");

    sqlx::query_scalar(
        "SELECT t.id FROM tables t JOIN namespaces n ON n.id = t.namespace_id
         WHERE n.warehouse_id = (SELECT id FROM warehouses WHERE name = $1)
           AND t.name = $2",
    )
    .bind(&ctx.warehouse)
    .bind(table)
    .fetch_one(&ctx.pool)
    .await
    .expect("table id")
}

/// Base metadata for the real-data fixture, current snapshot at `list_path`.
fn real_metadata(loc: &str, snapshot_id: i64, seq: i64, list_path: &str) -> TableMetadata {
    let mut summary = std::collections::BTreeMap::new();
    summary.insert("operation".to_owned(), "append".to_owned());
    let snapshot = Snapshot {
        snapshot_id,
        parent_snapshot_id: None,
        sequence_number: Some(seq),
        timestamp_ms: 1_700_000_000_000,
        manifest_list: Some(list_path.to_owned()),
        summary: Some(summary),
        schema_id: Some(0),
        first_row_id: None,
        added_rows: None,
        extra: serde_json::Map::new(),
    };
    let mut refs = std::collections::BTreeMap::new();
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
    TableMetadata {
        format_version: 2,
        table_uuid: uuid::Uuid::new_v4(),
        location: loc.to_owned(),
        last_sequence_number: Some(seq),
        next_row_id: None,
        last_updated_ms: 1_700_000_000_000,
        last_column_id: 5,
        schemas: vec![real_schema()],
        current_schema_id: 0,
        partition_specs: vec![real_spec()],
        default_spec_id: 0,
        last_partition_id: 1000,
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
    }
}

/// The registered agent under test.
struct Agent {
    token: String,
    subject: String,
    id: String,
}

/// Registers an agent through `/api/v2/agents` (admin), mints its IdP token, and
/// grants it READ on the warehouse. Optional budget caps and an explicit
/// expiry may be set. Returns the agent handle.
async fn register_agent(ctx: &Ctx, extra: Value) -> Agent {
    let subject = format!("agent-{}", Ulid::new());
    let mut body = json!({
        "issuer": ctx.idp.issuer,
        "subject": subject,
        "owner": "user:owner@example.com",
        "purpose": "test analytics assistant",
        "environment": "dev",
    });
    if let Value::Object(extra) = extra {
        body.as_object_mut().unwrap().extend(extra);
    }
    let (status, resp) = send(
        &ctx.router,
        "POST",
        "/api/v2/agents",
        Some(&ctx.admin_token),
        Some(&body),
    )
    .await;
    assert_eq!(status, StatusCode::CREATED, "register agent: {resp}");
    let id = resp["id"].as_str().expect("agent id").to_owned();

    // Grant the agent READ on the warehouse so context/RBAC passes.
    let (status, gbody) = send(
        &ctx.router,
        "POST",
        "/api/v2/grants",
        Some(&ctx.admin_token),
        Some(&json!({
            "privilege": "READ",
            "principal_id": id,
            "securable": { "type": "warehouse", "warehouse": ctx.warehouse },
        })),
    )
    .await;
    assert_eq!(status, StatusCode::CREATED, "grant READ to agent: {gbody}");

    // The agent's own OIDC token (client-credentials style: no email; gty marks
    // it a workload — but the gateway keys off the registered principal kind,
    // so what matters is that this (issuer, subject) resolves to the agent row).
    let token = idp::mint(
        KID1,
        &ctx.idp
            .claims(&subject, json!({ "gty": "client-credentials" })),
    );

    Agent { token, subject, id }
}

/// Tags the `amount` column `pii:high` and binds a null-mask column-mask policy
/// to that tag, so `amount` must be masked (and therefore ABSENT from any
/// scan-plan / context schema) for a non-exempt principal.
async fn mask_amount_column(ctx: &Ctx, ns: &str, table: &str) {
    let admin = Some(&ctx.admin_token);
    let pii = format!("pii-{}", Ulid::new()).to_lowercase();
    let (s, b) = send(
        &ctx.router,
        "POST",
        "/api/v2/governance/tags",
        admin.map(String::as_str),
        Some(&json!({ "key": pii, "value": "high" })),
    )
    .await;
    assert_eq!(s, StatusCode::CREATED, "create pii tag: {b}");
    let pii_tag_id = b["id"].as_str().unwrap().to_owned();
    let pii_rendered = format!("{pii}:high");

    let (s, b) = send(
        &ctx.router,
        "POST",
        "/api/v2/governance/tags/assignments",
        admin.map(String::as_str),
        Some(&json!({
            "tag_id": pii_tag_id,
            "target": { "securable_type": "column", "warehouse": ctx.warehouse,
                        "namespace": ns, "table": table, "column": "amount" }
        })),
    )
    .await;
    assert_eq!(s, StatusCode::CREATED, "assign pii to amount: {b}");

    let (s, b) = send(
        &ctx.router,
        "POST",
        "/api/v2/governance/policies",
        admin.map(String::as_str),
        Some(&json!({
            "name": format!("maskamt-{}", Ulid::new()),
            "kind": "column_mask",
            "definition": { "type": "tag_column_mask", "id": "mask-amount",
                            "tag": pii_rendered, "exempt_groups": [], "mask": { "kind": "null" } }
        })),
    )
    .await;
    assert_eq!(s, StatusCode::CREATED, "create mask policy: {b}");
    let mask_policy_id = b["id"].as_str().unwrap().to_owned();

    let (s, b) = send(
        &ctx.router,
        "POST",
        &format!("/api/v2/governance/policies/{mask_policy_id}/bindings"),
        admin.map(String::as_str),
        Some(&json!({ "target_type": "tag", "tag_id": pii_tag_id })),
    )
    .await;
    assert_eq!(s, StatusCode::CREATED, "bind mask policy: {b}");
}

/// Tags the table `residency:eu` and binds a row-filter policy to that tag that
/// keeps only rows where `region = 'region_000'`, so a non-exempt principal sees
/// only that region's rows (the fixture spans `region_000` and `region_001`).
async fn restrict_rows_to_region_000(ctx: &Ctx, ns: &str, table: &str) {
    let admin = Some(&ctx.admin_token);
    let key = format!("residency-{}", Ulid::new()).to_lowercase();
    let (s, b) = send(
        &ctx.router,
        "POST",
        "/api/v2/governance/tags",
        admin.map(String::as_str),
        Some(&json!({ "key": key, "value": "eu" })),
    )
    .await;
    assert_eq!(s, StatusCode::CREATED, "create residency tag: {b}");
    let tag_id = b["id"].as_str().unwrap().to_owned();
    let rendered = format!("{key}:eu");

    // Bind the tag at the TABLE level (a row filter is a table concern).
    let (s, b) = send(
        &ctx.router,
        "POST",
        "/api/v2/governance/tags/assignments",
        admin.map(String::as_str),
        Some(&json!({
            "tag_id": tag_id,
            "target": { "securable_type": "table", "warehouse": ctx.warehouse,
                        "namespace": ns, "table": table }
        })),
    )
    .await;
    assert_eq!(s, StatusCode::CREATED, "assign residency to table: {b}");

    let (s, b) = send(
        &ctx.router,
        "POST",
        "/api/v2/governance/policies",
        admin.map(String::as_str),
        Some(&json!({
            "name": format!("region-filter-{}", Ulid::new()),
            "kind": "row_filter",
            "definition": { "type": "tag_row_filter", "id": "region-eu",
                            "tag": rendered, "exempt_groups": [],
                            "predicate": { "op": "eq", "column": "region", "value": "region_000" } }
        })),
    )
    .await;
    assert_eq!(s, StatusCode::CREATED, "create row-filter policy: {b}");
    let policy_id = b["id"].as_str().unwrap().to_owned();

    let (s, b) = send(
        &ctx.router,
        "POST",
        &format!("/api/v2/governance/policies/{policy_id}/bindings"),
        admin.map(String::as_str),
        Some(&json!({ "target_type": "tag", "tag_id": tag_id })),
    )
    .await;
    assert_eq!(s, StatusCode::CREATED, "bind row-filter policy: {b}");
}

/// Counts this agent's activity rows with a given decision (scoped to the
/// agent's id — never a global count).
async fn activity_count(ctx: &Ctx, agent_id: &str, decision: &str) -> i64 {
    sqlx::query_scalar("SELECT count(*) FROM agent_activity WHERE agent_id = $1 AND decision = $2")
        .bind(agent_id)
        .bind(decision)
        .fetch_one(&ctx.pool)
        .await
        .expect("count activity")
}

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

#[tokio::test]
async fn initialize_negotiates_protocol_and_lists_tools() {
    let Some(ctx) = oidc_ctx().await else {
        return;
    };
    let agent = register_agent(&ctx, json!({})).await;

    // initialize -> protocolVersion echoed, tools capability, serverInfo.
    let (status, body) = mcp(
        &ctx.router,
        &agent.token,
        &json!({
            "jsonrpc": "2.0", "id": 1, "method": "initialize",
            "params": { "protocolVersion": "2025-06-18", "capabilities": {},
                        "clientInfo": { "name": "test", "version": "1.0" } }
        }),
    )
    .await;
    assert_eq!(status, StatusCode::OK, "initialize http: {body}");
    assert_eq!(body["jsonrpc"], "2.0");
    assert_eq!(body["id"], 1);
    assert_eq!(body["result"]["protocolVersion"], "2025-06-18");
    assert_eq!(
        body["result"]["capabilities"]["tools"]["listChanged"],
        false
    );
    assert_eq!(body["result"]["serverInfo"]["name"], "meridian");

    // notifications/initialized -> 202, no body.
    let (status, _) = mcp(
        &ctx.router,
        &agent.token,
        &json!({ "jsonrpc": "2.0", "method": "notifications/initialized" }),
    )
    .await;
    assert_eq!(status, StatusCode::ACCEPTED);

    // tools/list -> the governed catalog, including the headline tools.
    let (status, body) = mcp(
        &ctx.router,
        &agent.token,
        &json!({ "jsonrpc": "2.0", "id": 2, "method": "tools/list" }),
    )
    .await;
    assert_eq!(status, StatusCode::OK, "tools/list: {body}");
    let tools = body["result"]["tools"].as_array().expect("tools array");
    let names: Vec<&str> = tools.iter().filter_map(|t| t["name"].as_str()).collect();
    for expected in [
        "search_assets",
        "get_table_context",
        "get_lineage",
        "run_sql",
        "query_metrics",
        "preview_table",
    ] {
        assert!(names.contains(&expected), "tools/list missing {expected}");
    }
    // Each tool advertises an inputSchema.
    assert!(tools.iter().all(|t| t["inputSchema"].is_object()));
}

#[tokio::test]
async fn get_table_context_omits_masked_column() {
    let Some(ctx) = oidc_ctx().await else {
        return;
    };
    let ns = format!("ns{}", Ulid::new()).to_lowercase();
    register_fixture(&ctx, &ns, "sales").await;
    let agent = register_agent(&ctx, json!({})).await;

    // --- Baseline: no policy. The agent sees ALL columns, including `amount`.
    let (status, body) = mcp(
        &ctx.router,
        &agent.token,
        &call_tool(
            10,
            "get_table_context",
            json!({ "warehouse": ctx.warehouse, "namespace": ns, "table": "sales" }),
        ),
    )
    .await;
    assert_eq!(status, StatusCode::OK, "context baseline: {body}");
    let result = &body["result"];
    assert_eq!(
        result["isError"], false,
        "baseline is not an error: {result}"
    );
    let columns = column_names(result);
    assert!(
        columns.contains(&"amount".to_owned()),
        "baseline schema must include `amount`: {columns:?}"
    );
    assert!(columns.contains(&"id".to_owned()), "and `id`: {columns:?}");

    // --- Apply a column mask on `amount`.
    mask_amount_column(&ctx, &ns, "sales").await;

    // --- The masked column is ABSENT from the returned schema (not nulled).
    let (status, body) = mcp(
        &ctx.router,
        &agent.token,
        &call_tool(
            11,
            "get_table_context",
            json!({ "warehouse": ctx.warehouse, "namespace": ns, "table": "sales" }),
        ),
    )
    .await;
    assert_eq!(status, StatusCode::OK, "context masked: {body}");
    let result = &body["result"];
    assert_eq!(result["isError"], false);
    let columns = column_names(result);
    assert!(
        !columns.contains(&"amount".to_owned()),
        "masked column `amount` MUST be absent from the returned schema, not present: {columns:?}"
    );
    // Every OTHER column stays present — masking removes only the masked one.
    for kept in ["id", "region", "category", "ts"] {
        assert!(
            columns.contains(&kept.to_owned()),
            "unmasked column `{kept}` must remain: {columns:?}"
        );
    }
    // The structured payload names no `amount` anywhere in the schema block.
    let schema_str = result["structuredContent"]["schema"].to_string();
    assert!(
        !schema_str.contains("\"amount\""),
        "the schema block must not mention `amount` at all: {schema_str}"
    );
    // (H-F2) The agent-visible summary text must not disclose that columns were
    // hidden — not even the count. Leaking "(N hidden by policy)" would let a
    // prompt learn that restricted columns exist. The removed set lives only in
    // the operator-facing audit detail (asserted via the activity ledger below).
    let summary_text = result["content"][0]["text"].as_str().unwrap_or_default();
    assert!(
        !summary_text.to_lowercase().contains("hidden"),
        "the agent-visible summary must not reveal that columns were hidden: {summary_text:?}"
    );
    // And the audit detail records the removal.
    // (The activity ledger is asserted directly below.)
    let removed = activity_count(&ctx, &agent.id, "allowed").await;
    assert!(removed >= 2, "both context calls were audited as allowed");
}

#[tokio::test]
async fn budget_exhaustion_refuses_gracefully_and_audits() {
    let Some(ctx) = oidc_ctx().await else {
        return;
    };
    // An agent capped at exactly one query per hour.
    let agent = register_agent(&ctx, json!({ "queries_per_hour": 1 })).await;

    // First query tool call: a table-free query the wired executor runs. It is
    // allowed by budget and returns rows (the executor is wired now), consuming
    // the one query the agent is allowed this hour.
    let (status, body) = mcp(
        &ctx.router,
        &agent.token,
        &call_tool(20, "run_sql", json!({ "sql": "SELECT 1 AS one" })),
    )
    .await;
    assert_eq!(status, StatusCode::OK, "first run_sql: {body}");
    let first = &body["result"];
    assert_eq!(
        first["isError"], false,
        "the wired executor runs a table-free query: {first}"
    );
    assert_eq!(
        first["structuredContent"]["row_count"], 1,
        "SELECT 1 returns one row: {first}"
    );

    // Second query tool call: over the per-hour cap -> graceful refusal.
    let (status, body) = mcp(
        &ctx.router,
        &agent.token,
        &call_tool(21, "run_sql", json!({ "sql": "SELECT 2" })),
    )
    .await;
    assert_eq!(status, StatusCode::OK, "second run_sql: {body}");
    let second = &body["result"];
    assert_eq!(second["isError"], true, "over-budget is a tool-error");
    let text = second["content"][0]["text"].as_str().unwrap_or_default();
    assert!(
        text.contains("Budget exceeded") && text.contains("queries_per_hour"),
        "over-budget message is a graceful, relayable refusal: {text}"
    );

    // The refusal is audited as `refused_budget`; the allowed one as `allowed`.
    assert_eq!(
        activity_count(&ctx, &agent.id, "refused_budget").await,
        1,
        "the over-budget call is audited as refused_budget"
    );
    assert_eq!(
        activity_count(&ctx, &agent.id, "allowed").await,
        1,
        "the first (in-budget) call is audited as allowed"
    );

    // A CONTEXT tool does NOT consume the query budget: it still works after
    // the query budget is exhausted (search touches no rows).
    let (status, body) = mcp(
        &ctx.router,
        &agent.token,
        &call_tool(22, "search_assets", json!({ "query": "sales" })),
    )
    .await;
    assert_eq!(status, StatusCode::OK, "search after budget: {body}");
    assert_eq!(
        body["result"]["isError"], false,
        "context tools are unaffected by the query budget: {}",
        body["result"]
    );
}

/// `run_sql` runs on the wired DataFusion executor and returns correct rows plus
/// **provenance** (the table read, its Meridian id, and the snapshot id) so the
/// agent can cite (H-F3). The fixture is `region_000` (150 rows) + `region_001`
/// (150 rows) = 300 rows.
#[tokio::test]
async fn run_sql_returns_rows_and_provenance() {
    let Some(ctx) = oidc_ctx().await else {
        return;
    };
    let ns = format!("ns{}", Ulid::new()).to_lowercase();
    let table_id = register_real_fixture(&ctx, &ns, "sales").await;
    let agent = register_agent(&ctx, json!({})).await;

    // Aggregate query: count the rows. The executor reads the real Parquet.
    let (status, body) = mcp(
        &ctx.router,
        &agent.token,
        &call_tool(
            60,
            "run_sql",
            json!({ "warehouse": ctx.warehouse, "sql": format!("SELECT count(*) AS n FROM {ns}.sales") }),
        ),
    )
    .await;
    assert_eq!(status, StatusCode::OK, "run_sql: {body}");
    let result = &body["result"];
    assert_eq!(result["isError"], false, "run_sql succeeded: {result}");
    let structured = &result["structuredContent"];
    // 300 rows in the fixture (6 files x 50 rows).
    assert_eq!(
        structured["rows"][0]["n"], 300,
        "count(*) over the fixture: {structured}"
    );

    // Provenance: the one table read, with its Meridian internal id and a
    // snapshot id — exactly what an agent cites and a CISO audit reads.
    let tables = structured["provenance"]["tables"]
        .as_array()
        .expect("provenance tables");
    assert_eq!(tables.len(), 1, "one table read: {structured}");
    assert_eq!(
        tables[0]["table_id"].as_str(),
        Some(table_id.as_str()),
        "provenance carries the Meridian internal table id"
    );
    assert!(
        tables[0]["snapshot_id"].is_number(),
        "provenance carries the snapshot id read: {}",
        tables[0]
    );
    assert_eq!(tables[0]["name"], format!("{ns}.sales"));
}

/// The headline governance proof for `run_sql` (H-F2/H-F3/D-F2): with a column
/// mask on `amount` and a row filter to `region_000`, the agent's `SELECT *`
/// sees the masked column **ABSENT** (not nulled) and only the permitted rows,
/// and the provenance records both policies. The same Pillar-D decision the scan
/// planner uses, applied through the executor's governed view.
#[tokio::test]
async fn run_sql_applies_column_mask_and_row_filter() {
    let Some(ctx) = oidc_ctx().await else {
        return;
    };
    let ns = format!("ns{}", Ulid::new()).to_lowercase();
    register_real_fixture(&ctx, &ns, "sales").await;
    let agent = register_agent(&ctx, json!({})).await;

    // Baseline (no policy): SELECT * sees `amount`, and all 300 rows.
    let (status, body) = mcp(
        &ctx.router,
        &agent.token,
        &call_tool(
            70,
            "run_sql",
            json!({ "warehouse": ctx.warehouse, "sql": format!("SELECT * FROM {ns}.sales") }),
        ),
    )
    .await;
    assert_eq!(status, StatusCode::OK, "baseline run_sql: {body}");
    let baseline = &body["result"];
    assert_eq!(baseline["isError"], false, "baseline ran: {baseline}");
    let base_cols: Vec<String> = baseline["structuredContent"]["columns"]
        .as_array()
        .unwrap()
        .iter()
        .filter_map(|c| c["name"].as_str().map(str::to_owned))
        .collect();
    assert!(
        base_cols.contains(&"amount".to_owned()),
        "baseline SELECT * includes `amount`: {base_cols:?}"
    );
    assert_eq!(
        baseline["structuredContent"]["row_count"], 300,
        "baseline returns all rows (capped at 1000): {}",
        baseline["structuredContent"]["row_count"]
    );

    // Apply BOTH a column mask on `amount` and a row filter to region_000.
    mask_amount_column(&ctx, &ns, "sales").await;
    restrict_rows_to_region_000(&ctx, &ns, "sales").await;

    let (status, body) = mcp(
        &ctx.router,
        &agent.token,
        &call_tool(
            71,
            "run_sql",
            json!({ "warehouse": ctx.warehouse, "sql": format!("SELECT * FROM {ns}.sales") }),
        ),
    )
    .await;
    assert_eq!(status, StatusCode::OK, "governed run_sql: {body}");
    let governed = &body["result"];
    assert_eq!(governed["isError"], false, "governed run ran: {governed}");
    let structured = &governed["structuredContent"];

    // (H-F2) The masked column is ABSENT from the result columns — not nulled.
    let cols: Vec<String> = structured["columns"]
        .as_array()
        .unwrap()
        .iter()
        .filter_map(|c| c["name"].as_str().map(str::to_owned))
        .collect();
    assert!(
        !cols.contains(&"amount".to_owned()),
        "masked `amount` MUST be absent from `run_sql` results, not present: {cols:?}"
    );
    for kept in ["id", "region", "category", "ts"] {
        assert!(
            cols.contains(&kept.to_owned()),
            "unmasked column `{kept}` remains: {cols:?}"
        );
    }
    // No row's JSON object mentions `amount` at all (absent, not null).
    let rows_str = structured["rows"].to_string();
    assert!(
        !rows_str.contains("\"amount\""),
        "no result row may mention the dropped column `amount`"
    );

    // (Row filter) Only region_000's 150 rows are visible — never region_001.
    assert_eq!(
        structured["row_count"], 150,
        "the row filter restricts to region_000's 150 rows: {}",
        structured["row_count"]
    );
    let all_region_000 = structured["rows"]
        .as_array()
        .unwrap()
        .iter()
        .all(|r| r["region"] == "region_000");
    assert!(
        all_region_000,
        "every returned row is region_000 (the filter is enforced by the executor)"
    );

    // Provenance records the applied policies + the masked column.
    let prov = &structured["provenance"];
    assert!(
        !prov["row_filter_policies"].as_array().unwrap().is_empty(),
        "provenance records the row-filter policy: {prov}"
    );
    assert!(
        !prov["column_mask_policies"].as_array().unwrap().is_empty(),
        "provenance records the column-mask policy: {prov}"
    );
    assert!(
        prov["masked_columns"]
            .as_array()
            .unwrap()
            .iter()
            .any(|c| c == "amount"),
        "provenance records `amount` as masked: {prov}"
    );

    // And the whole thing is audited as an allowed governed query.
    assert!(
        activity_count(&ctx, &agent.id, "allowed").await >= 2,
        "both run_sql calls were audited as allowed"
    );
}

/// Budget/cap refusal (H-F3/H-F4): an agent whose per-day scanned-bytes budget
/// is smaller than the fixture's scan is refused **before** execution with a
/// graceful, relayable message, and the refusal is audited `refused_budget`.
#[tokio::test]
async fn run_sql_refuses_over_byte_budget_gracefully() {
    let Some(ctx) = oidc_ctx().await else {
        return;
    };
    let ns = format!("ns{}", Ulid::new()).to_lowercase();
    register_real_fixture(&ctx, &ns, "sales").await;
    // Cap the agent at 1 byte/day — any real table scan exceeds it.
    let agent = register_agent(&ctx, json!({ "scanned_bytes_per_day": 1 })).await;

    let (status, body) = mcp(
        &ctx.router,
        &agent.token,
        &call_tool(
            80,
            "run_sql",
            json!({ "warehouse": ctx.warehouse, "sql": format!("SELECT * FROM {ns}.sales") }),
        ),
    )
    .await;
    assert_eq!(status, StatusCode::OK, "budgeted run_sql: {body}");
    let result = &body["result"];
    assert_eq!(result["isError"], true, "over-budget is a tool-error");
    let text = result["content"][0]["text"].as_str().unwrap_or_default();
    assert!(
        text.contains("Budget exceeded") && text.contains("scanned_bytes_per_day"),
        "the refusal names the scanned-bytes cap and is relayable: {text}"
    );
    // Audited as refused_budget; nothing ran (no `allowed` query row).
    assert_eq!(
        activity_count(&ctx, &agent.id, "refused_budget").await,
        1,
        "the over-budget query is audited as refused_budget"
    );
    assert_eq!(
        activity_count(&ctx, &agent.id, "allowed").await,
        0,
        "nothing ran: no allowed query row"
    );
}

/// A table-free `run_sql` (`SELECT 1`) runs with no warehouse and no catalog
/// resolution — the zero-setup first taste — and carries empty provenance.
#[tokio::test]
async fn run_sql_table_free_query_runs_without_warehouse() {
    let Some(ctx) = oidc_ctx().await else {
        return;
    };
    let agent = register_agent(&ctx, json!({})).await;
    let (status, body) = mcp(
        &ctx.router,
        &agent.token,
        &call_tool(90, "run_sql", json!({ "sql": "SELECT 1 + 1 AS two" })),
    )
    .await;
    assert_eq!(status, StatusCode::OK, "table-free run_sql: {body}");
    let result = &body["result"];
    assert_eq!(result["isError"], false, "table-free query ran: {result}");
    assert_eq!(result["structuredContent"]["rows"][0]["two"], 2);
    assert!(
        result["structuredContent"]["provenance"]["tables"]
            .as_array()
            .unwrap()
            .is_empty(),
        "a table-free query cites no tables"
    );
}

/// `preview_table` returns a policy-safe sample: the masked column is absent and
/// the row filter applies, capped to the requested `limit` (H-F3).
#[tokio::test]
async fn preview_table_is_policy_safe() {
    let Some(ctx) = oidc_ctx().await else {
        return;
    };
    let ns = format!("ns{}", Ulid::new()).to_lowercase();
    register_real_fixture(&ctx, &ns, "sales").await;
    mask_amount_column(&ctx, &ns, "sales").await;
    let agent = register_agent(&ctx, json!({})).await;

    let (status, body) = mcp(
        &ctx.router,
        &agent.token,
        &call_tool(
            100,
            "preview_table",
            json!({ "warehouse": ctx.warehouse, "namespace": ns, "table": "sales", "limit": 5 }),
        ),
    )
    .await;
    assert_eq!(status, StatusCode::OK, "preview_table: {body}");
    let result = &body["result"];
    assert_eq!(result["isError"], false, "preview ran: {result}");
    let structured = &result["structuredContent"];
    assert!(
        structured["row_count"].as_i64().unwrap_or(0) <= 5,
        "preview respects the limit: {}",
        structured["row_count"]
    );
    let cols: Vec<String> = structured["columns"]
        .as_array()
        .unwrap()
        .iter()
        .filter_map(|c| c["name"].as_str().map(str::to_owned))
        .collect();
    assert!(
        !cols.contains(&"amount".to_owned()),
        "preview_table hides the masked column: {cols:?}"
    );
}

#[tokio::test]
async fn kill_switch_refuses_all_tools() {
    let Some(ctx) = oidc_ctx().await else {
        return;
    };
    let ns = format!("ns{}", Ulid::new()).to_lowercase();
    register_fixture(&ctx, &ns, "sales").await;
    let agent = register_agent(&ctx, json!({})).await;

    // Before suspension: a context tool works.
    let (status, body) = mcp(
        &ctx.router,
        &agent.token,
        &call_tool(30, "search_assets", json!({ "query": "sales" })),
    )
    .await;
    assert_eq!(status, StatusCode::OK);
    assert_eq!(body["result"]["isError"], false, "works before suspend");

    // Engage the kill switch.
    let (status, body) = send(
        &ctx.router,
        "POST",
        &format!("/api/v2/agents/{}/suspend", agent.id),
        Some(&ctx.admin_token),
        Some(&json!({ "reason": "incident response" })),
    )
    .await;
    assert_eq!(status, StatusCode::OK, "suspend: {body}");
    assert_eq!(body["enabled"], false);

    // After suspension: EVERY tool is refused with a relayable message.
    for (id, tool, args) in [
        (31, "search_assets", json!({ "query": "sales" })),
        (
            32,
            "get_table_context",
            json!({ "warehouse": ctx.warehouse, "namespace": ns, "table": "sales" }),
        ),
        (33, "run_sql", json!({ "sql": "SELECT 1" })),
    ] {
        let (status, body) = mcp(&ctx.router, &agent.token, &call_tool(id, tool, args)).await;
        assert_eq!(status, StatusCode::OK, "{tool} http after suspend: {body}");
        let result = &body["result"];
        assert_eq!(
            result["isError"], true,
            "{tool} must be refused while suspended: {result}"
        );
        assert!(
            result["content"][0]["text"]
                .as_str()
                .is_some_and(|t| t.contains("suspended")),
            "{tool} refusal names the kill switch: {result}"
        );
    }

    // All three refusals are audited as `refused_killed`.
    assert_eq!(
        activity_count(&ctx, &agent.id, "refused_killed").await,
        3,
        "every tool call while suspended is audited as refused_killed"
    );

    // Re-enable and confirm the agent works again.
    let (status, _) = send(
        &ctx.router,
        "POST",
        &format!("/api/v2/agents/{}/enable", agent.id),
        Some(&ctx.admin_token),
        Some(&json!({ "reason": "incident resolved" })),
    )
    .await;
    assert_eq!(status, StatusCode::OK);
    let (status, body) = mcp(
        &ctx.router,
        &agent.token,
        &call_tool(34, "search_assets", json!({ "query": "sales" })),
    )
    .await;
    assert_eq!(status, StatusCode::OK);
    assert_eq!(body["result"]["isError"], false, "works after re-enable");
}

#[tokio::test]
async fn every_tool_call_is_audited_with_its_decision() {
    let Some(ctx) = oidc_ctx().await else {
        return;
    };
    let agent = register_agent(&ctx, json!({})).await;

    // Make a handful of calls of different shapes.
    let _ = mcp(
        &ctx.router,
        &agent.token,
        &call_tool(40, "search_assets", json!({ "query": "anything" })),
    )
    .await;
    let _ = mcp_with_purpose(
        &ctx.router,
        &agent.token,
        &call_tool(41, "list_metrics", json!({})),
        Some("quarterly-review"),
    )
    .await;
    let _ = mcp(
        &ctx.router,
        &agent.token,
        &call_tool(42, "run_sql", json!({ "sql": "SELECT 1" })),
    )
    .await;

    // The activity ledger holds a row per call, with the tool, a stable args
    // digest, and a cross-reference to the audit chain (audit_seq). (The
    // decision and the purpose override are asserted separately below.)
    let rows: Vec<(String, String, Option<i64>)> = sqlx::query_as(
        "SELECT tool, args_digest, audit_seq
         FROM agent_activity WHERE agent_id = $1 ORDER BY id",
    )
    .bind(&agent.id)
    .fetch_all(&ctx.pool)
    .await
    .expect("activity rows");
    assert_eq!(rows.len(), 3, "one ledger row per tool call");
    for (tool, digest, audit_seq) in &rows {
        assert!(!tool.is_empty(), "tool recorded");
        assert_eq!(digest.len(), 64, "args digest is a sha256 hex");
        assert!(
            audit_seq.is_some(),
            "every ledger row cross-references the tamper-evident audit chain"
        );
    }
    // Every row carries a non-empty decision.
    let undecided: i64 = sqlx::query_scalar(
        "SELECT count(*) FROM agent_activity WHERE agent_id = $1 AND (decision IS NULL OR decision = '')",
    )
    .bind(&agent.id)
    .fetch_one(&ctx.pool)
    .await
    .expect("decision count");
    assert_eq!(undecided, 0, "every ledger row records a decision");
    // The purpose override was recorded on the list_metrics call.
    let metrics_purpose: Option<String> = sqlx::query_scalar(
        "SELECT purpose FROM agent_activity WHERE agent_id = $1 AND tool = 'list_metrics'",
    )
    .bind(&agent.id)
    .fetch_one(&ctx.pool)
    .await
    .expect("metrics purpose");
    assert_eq!(metrics_purpose.as_deref(), Some("quarterly-review"));

    // The tamper-evident audit chain also carries the tool calls, and it still
    // verifies end to end (the ledger writes did not break the chain).
    let (status, body) = send(
        &ctx.router,
        "GET",
        "/api/v2/audit/verify",
        Some(&ctx.admin_token),
        None,
    )
    .await;
    assert_eq!(status, StatusCode::OK, "verify chain: {body}");
    assert_eq!(body["valid"], true, "the audit chain verifies: {body}");

    // The audit query surface shows the agent's tool-call actions.
    let (status, body) = send(
        &ctx.router,
        "GET",
        &format!("/api/v2/audit?principal=agent:{}", agent.subject),
        Some(&ctx.admin_token),
        None,
    )
    .await;
    assert_eq!(status, StatusCode::OK, "audit query: {body}");
    let entries = body["entries"].as_array().expect("audit entries");
    assert!(
        entries.iter().any(|e| e["action"]
            .as_str()
            .is_some_and(|a| a.starts_with("agent.tool."))),
        "the audit chain records agent.tool.* actions: {body}"
    );

    // The management activity API returns the ledger too (the CISO view).
    let (status, body) = send(
        &ctx.router,
        "GET",
        &format!("/api/v2/agents/activity?agent_id={}", agent.id),
        Some(&ctx.admin_token),
        None,
    )
    .await;
    assert_eq!(status, StatusCode::OK, "activity API: {body}");
    assert_eq!(
        body["activity"].as_array().map(Vec::len),
        Some(3),
        "the activity API lists the three tool calls: {body}"
    );
}

#[tokio::test]
async fn non_agent_and_unknown_tool_are_protocol_errors() {
    let Some(ctx) = oidc_ctx().await else {
        return;
    };

    // A plain USER token (not a registered agent) is turned away from tools/call
    // with a JSON-RPC protocol error.
    let user_sub = format!("human-{}", Ulid::new());
    let user_token = idp::mint(
        KID1,
        &ctx.idp.claims(
            &user_sub,
            json!({ "email": format!("{user_sub}@example.com") }),
        ),
    );
    let (status, body) = mcp(
        &ctx.router,
        &user_token,
        &call_tool(50, "search_assets", json!({ "query": "x" })),
    )
    .await;
    assert_eq!(status, StatusCode::OK, "protocol error is still HTTP 200");
    assert!(
        body["error"]["code"].is_number(),
        "a non-agent caller gets a JSON-RPC protocol error: {body}"
    );
    assert!(
        body.get("result").is_none(),
        "no result on a protocol error"
    );

    // A registered agent asking for an unknown tool is a protocol error too.
    let agent = register_agent(&ctx, json!({})).await;
    let (status, body) = mcp(
        &ctx.router,
        &agent.token,
        &call_tool(51, "no_such_tool", json!({})),
    )
    .await;
    assert_eq!(status, StatusCode::OK);
    assert_eq!(
        body["error"]["code"], -32601,
        "unknown tool is METHOD_NOT_FOUND: {body}"
    );

    // A GET on /mcp is 405 (no server-initiated stream).
    let response = ctx
        .router
        .clone()
        .oneshot(
            Request::builder()
                .method("GET")
                .uri("/mcp")
                .header("authorization", format!("Bearer {}", agent.token))
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .expect("infallible");
    assert_eq!(response.status(), StatusCode::METHOD_NOT_ALLOWED);
}

// ---------------------------------------------------------------------------
// Small helpers
// ---------------------------------------------------------------------------

/// Builds a `tools/call` JSON-RPC message.
fn call_tool(id: i64, name: &str, arguments: Value) -> Value {
    let mut params = serde_json::Map::new();
    params.insert("name".to_owned(), Value::from(name));
    params.insert("arguments".to_owned(), arguments);
    json!({
        "jsonrpc": "2.0", "id": id, "method": "tools/call",
        "params": Value::Object(params)
    })
}

/// Extracts the visible column names from a `get_table_context` result.
fn column_names(result: &Value) -> Vec<String> {
    result["structuredContent"]["schema"]["columns"]
        .as_array()
        .map(|cols| {
            cols.iter()
                .filter_map(|c| c["name"].as_str().map(str::to_owned))
                .collect()
        })
        .unwrap_or_default()
}
