//! End-to-end tests for the autonomous-maintenance worker (spec Pillar C).
//!
//! These build a **real** small-files Iceberg table — real Parquet data files
//! and real manifest/manifest-list Avro on `file://` storage, registered as a
//! catalog table — then drive the worker the same way `meridian serve` does
//! (via [`meridian_server::maintenance::claim_and_run`] /
//! [`reconcile_once`]) and assert on the committed result: the table pointer
//! advanced, the new snapshot has fewer live data files, and the savings
//! ledger recorded the job. They require a running Postgres (`DATABASE_URL`)
//! and skip cleanly without it.
//!
//! The fixtures are deliberately tiny (a handful of ~1 KiB files) so the whole
//! read → rewrite → commit path runs in-process in milliseconds.

#![allow(
    clippy::items_after_statements,
    clippy::too_many_lines,
    clippy::cast_possible_truncation,
    clippy::cast_possible_wrap
)]

use std::collections::BTreeMap;
use std::sync::Arc;

use arrow_array::{ArrayRef, Int64Array, RecordBatch, StringArray};
use arrow_schema::{DataType, Field, Schema as ArrowSchema};
use bytes::Bytes;
use meridian_common::config::{AppConfig, MaintenanceConfig};
use meridian_iceberg::manifest::{
    DataFile, DataFileContent, ManifestContentType, ManifestEntry, ManifestEntryStatus,
    ManifestFile, ManifestListWriteParams, ManifestWriteParams, PartitionTuple,
    partition_field_types, write_manifest, write_manifest_list,
};
use meridian_iceberg::spec::{
    PartitionSpec, PrimitiveType, Schema, Snapshot, SnapshotRef, StructField, TableMetadata, Type,
};
use meridian_server::AppState;
use meridian_server::maintenance;
use meridian_store::maintenance as store_maintenance;
use meridian_store::maintenance::{JobState, JobType, PolicySpec, Scope};
use meridian_store::{tenancy, warehouse};
use parquet::arrow::{ArrowWriter, PARQUET_FIELD_ID_META_KEY};
use serde_json::json;
use sqlx::PgPool;
use tokio::sync::{Mutex, MutexGuard, OnceCell};
use ulid::Ulid;

// ---------------------------------------------------------------------------
// Harness
// ---------------------------------------------------------------------------

/// The worker's `claim_next` and the reconciler both scan the maintenance
/// queue / table set **globally** (a shared worker pool, by design). Cargo
/// runs these tests concurrently against one database, so a foreign test's
/// job could be claimed mid-test, or the global reconcile pass could pick up a
/// concurrently-created table. This process-wide async mutex serializes them —
/// the same discipline `meridian_store`'s queue tests use.
static SERIAL: OnceCell<Mutex<()>> = OnceCell::const_new();

async fn serial_lock() -> MutexGuard<'static, ()> {
    SERIAL
        .get_or_init(|| async { Mutex::new(()) })
        .await
        .lock()
        .await
}

/// Clears the shared maintenance queue and reconcile-debounce state so a test
/// claims/enqueues only its own jobs. Safe because [`serial_lock`] guarantees
/// no sibling maintenance test in this binary runs concurrently. Per-table
/// assertions already isolate cross-binary noise.
async fn reset_queue(pool: &PgPool) {
    sqlx::query("DELETE FROM maintenance_jobs")
        .execute(pool)
        .await
        .expect("reset jobs");
    sqlx::query("DELETE FROM maintenance_reconcile_state")
        .execute(pool)
        .await
        .expect("reset reconcile state");
}

struct Ctx {
    pool: PgPool,
    state: AppState,
    #[allow(dead_code)] // held to keep the tempdir alive for the test's lifetime
    root: tempfile::TempDir,
    #[allow(dead_code)] // recorded for debugging; the id is what queries use
    warehouse_name: String,
    warehouse_id: String,
    warehouse_root: String,
    config: MaintenanceConfig,
}

async fn ctx() -> Option<Ctx> {
    let Ok(url) = std::env::var("DATABASE_URL") else {
        eprintln!("skipping maintenance worker test: DATABASE_URL is not set");
        return None;
    };
    let mut config = AppConfig::default();
    config.database.url = url;
    let pool = meridian_store::connect(&config.database)
        .await
        .expect("connect");
    meridian_store::MIGRATOR.run(&pool).await.expect("migrate");

    let root = tempfile::tempdir().expect("tempdir");
    let warehouse_root = format!("file://{}", root.path().join("warehouse").display());
    let run = Ulid::new().to_string().to_lowercase();
    let warehouse_name = format!("wh-maint-{run}");
    let wh = warehouse::create(
        &pool,
        tenancy::default_workspace_id(),
        &warehouse_name,
        &warehouse_root,
        BTreeMap::new(),
        "test:maint",
    )
    .await
    .expect("create warehouse");

    let maint_config = config.maintenance.clone();
    let state = AppState {
        pool: pool.clone(),
        config: Arc::new(config),
    };
    Some(Ctx {
        pool,
        state,
        root,
        warehouse_name,
        warehouse_id: wh.id,
        warehouse_root,
        config: maint_config,
    })
}

/// Creates a namespace + registers a table pointing at `metadata_location`,
/// returning the internal table id and namespace id.
async fn register_table(
    ctx: &Ctx,
    ns: &str,
    table: &str,
    metadata_location: &str,
) -> (String, String) {
    let levels = vec![ns.to_owned()];
    let namespace = meridian_store::namespace::create(
        &ctx.pool,
        tenancy::default_workspace_id(),
        &ctx.warehouse_id,
        &levels,
        BTreeMap::new(),
        "test:maint",
    )
    .await
    .expect("create namespace");

    // Read the metadata back to derive the write-through state, exactly like
    // the register route does.
    let storage = connect(&ctx.warehouse_root);
    let metadata = meridian_storage::read_table_metadata(storage.as_ref(), metadata_location)
        .await
        .expect("read fixture metadata");
    let schema_text = metadata
        .current_schema()
        .map(meridian_store::search::schema_search_text);
    // Index the metadata's snapshots exactly like the register route does, so
    // health/reconcile/expiry see the table's history.
    let current = metadata.current_snapshot_id.filter(|id| *id >= 0);
    let snapshots: Vec<meridian_store::commit::SnapshotIndexRow> = metadata
        .snapshots
        .iter()
        .flatten()
        .map(|s| meridian_store::commit::SnapshotIndexRow {
            snapshot_id: s.snapshot_id,
            parent_snapshot_id: s.parent_snapshot_id,
            sequence_number: s.sequence_number,
            timestamp_ms: s.timestamp_ms,
            manifest_list: s.manifest_list.clone(),
            operation: s
                .summary
                .as_ref()
                .and_then(|summary| summary.get("operation").cloned()),
            summary: json!(s.summary.clone().unwrap_or_default()),
            is_current: current == Some(s.snapshot_id),
        })
        .collect();
    let record = meridian_store::table::create(
        &ctx.pool,
        meridian_store::table::NewTable {
            workspace_id: tenancy::default_workspace_id(),
            namespace_id: &namespace.id,
            namespace_levels: &levels,
            name: table,
            table_uuid: &metadata.table_uuid.to_string(),
            metadata_location,
            format_version: i16::from(metadata.format_version),
            properties: &metadata.properties.clone().unwrap_or_default(),
            schema_text: schema_text.as_deref(),
            snapshots: &snapshots,
            origin: "test",
        },
        "test:maint",
        None,
    )
    .await
    .expect("register table");
    (record.id, namespace.id)
}

fn connect(warehouse_root: &str) -> Arc<dyn meridian_storage::Storage> {
    meridian_storage::StorageProfile::parse(warehouse_root, &BTreeMap::new())
        .expect("parse storage profile")
        .connect()
        .expect("connect storage")
}

// ---------------------------------------------------------------------------
// Fixture: a real small-files table (real Parquet + real manifest Avro)
// ---------------------------------------------------------------------------

/// A simple unpartitioned schema: id (long, field 1), name (string, field 2).
fn fixture_schema() -> Schema {
    Schema::new(vec![
        StructField::optional(1, "id", Type::Primitive(PrimitiveType::Long)),
        StructField::optional(2, "name", Type::Primitive(PrimitiveType::String)),
    ])
    .with_schema_id(0)
}

fn arrow_schema() -> Arc<ArrowSchema> {
    let field = |name: &str, dt: DataType, id: i32| {
        let mut md = std::collections::HashMap::new();
        md.insert(PARQUET_FIELD_ID_META_KEY.to_string(), id.to_string());
        Field::new(name, dt, true).with_metadata(md)
    };
    Arc::new(ArrowSchema::new(vec![
        field("id", DataType::Int64, 1),
        field("name", DataType::Utf8, 2),
    ]))
}

/// Writes `rows` (id values) to a tiny Parquet buffer.
fn write_parquet(ids: &[i64]) -> Bytes {
    let schema = arrow_schema();
    let id_col: ArrayRef = Arc::new(Int64Array::from(ids.to_vec()));
    let name_col: ArrayRef = Arc::new(StringArray::from(
        ids.iter().map(|i| format!("row-{i}")).collect::<Vec<_>>(),
    ));
    let batch = RecordBatch::try_new(schema.clone(), vec![id_col, name_col]).expect("batch");
    let mut buf = Vec::new();
    let mut w = ArrowWriter::try_new(&mut buf, schema, None).expect("writer");
    w.write(&batch).expect("write");
    w.close().expect("close");
    Bytes::from(buf)
}

/// Builds a small-files table with `num_files` tiny unpartitioned data files
/// and writes every object (parquet + manifest + manifest list + metadata.json)
/// to fs storage. Returns the metadata.json location and the snapshot id.
async fn build_small_files_table(
    storage: &dyn meridian_storage::Storage,
    table_location: &str,
    num_files: usize,
) -> (String, i64) {
    let schema = fixture_schema();
    let schema_json = serde_json::to_string(&schema).expect("schema json");
    let spec = PartitionSpec::unpartitioned(0);
    let types = partition_field_types(&spec.fields, &schema).expect("partition types");
    let snapshot_id = 100i64;
    let sequence_number = 1i64;

    // Write each tiny data file + build a manifest entry for it.
    let mut entries = Vec::new();
    for i in 0..num_files {
        let path = format!("{table_location}/data/file-{i}.parquet");
        let bytes = write_parquet(&[i as i64 * 10, i as i64 * 10 + 1, i as i64 * 10 + 2]);
        let size = bytes.len() as i64;
        storage.write(&path, bytes).await.expect("write parquet");
        entries.push(ManifestEntry {
            status: ManifestEntryStatus::Added,
            snapshot_id: Some(snapshot_id),
            sequence_number: Some(sequence_number),
            file_sequence_number: Some(sequence_number),
            data_file: DataFile {
                content: DataFileContent::Data,
                file_path: path,
                file_format: "PARQUET".to_owned(),
                partition: PartitionTuple { fields: vec![] },
                record_count: 3,
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
        });
    }

    let manifest_path = format!("{table_location}/metadata/data-m0.avro");
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
        sequence_number,
        min_sequence_number: sequence_number,
        added_snapshot_id: snapshot_id,
        added_files_count: Some(num_files as i32),
        existing_files_count: Some(0),
        deleted_files_count: Some(0),
        added_rows_count: Some(total_rows),
        existing_rows_count: Some(0),
        deleted_rows_count: Some(0),
        partitions: None,
        key_metadata: None,
        first_row_id: None,
    };
    let list_path = format!("{table_location}/metadata/snap-{snapshot_id}-1-list.avro");
    let list_bytes = write_manifest_list(&ManifestListWriteParams {
        format_version: 2,
        snapshot_id,
        parent_snapshot_id: None,
        sequence_number: Some(sequence_number),
        manifests: &[manifest_file],
    })
    .expect("write list");
    storage
        .write(&list_path, Bytes::from(list_bytes))
        .await
        .expect("write list file");

    let metadata = table_metadata(table_location, snapshot_id, sequence_number, &list_path);
    let metadata_location = format!(
        "{table_location}/metadata/00000-{}.metadata.json",
        Ulid::new()
    );
    meridian_storage::write_table_metadata(storage, &metadata_location, &metadata)
        .await
        .expect("write metadata");
    (metadata_location, snapshot_id)
}

/// Base table metadata with one snapshot on `main`.
fn table_metadata(
    table_location: &str,
    snapshot_id: i64,
    sequence_number: i64,
    list_path: &str,
) -> TableMetadata {
    let mut summary = BTreeMap::new();
    summary.insert("operation".to_owned(), "append".to_owned());
    let snapshot = Snapshot {
        snapshot_id,
        parent_snapshot_id: None,
        sequence_number: Some(sequence_number),
        timestamp_ms: 1_700_000_000_000,
        manifest_list: Some(list_path.to_owned()),
        summary: Some(summary),
        schema_id: Some(0),
        first_row_id: None,
        added_rows: None,
        extra: serde_json::Map::new(),
    };
    let mut refs = BTreeMap::new();
    refs.insert("main".to_owned(), main_ref(snapshot_id));
    TableMetadata {
        format_version: 2,
        table_uuid: uuid::Uuid::new_v4(),
        location: table_location.to_owned(),
        last_sequence_number: Some(sequence_number),
        next_row_id: None,
        last_updated_ms: 1_700_000_000_000,
        last_column_id: 2,
        schemas: vec![fixture_schema()],
        current_schema_id: 0,
        partition_specs: vec![PartitionSpec::unpartitioned(0)],
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
    }
}

fn main_ref(snapshot_id: i64) -> SnapshotRef {
    SnapshotRef {
        snapshot_id,
        ref_type: meridian_iceberg::spec::RefType::Branch,
        min_snapshots_to_keep: None,
        max_snapshot_age_ms: None,
        max_ref_age_ms: None,
        extra: serde_json::Map::new(),
    }
}

/// Reads the current metadata of a registered table (following the pointer),
/// and returns the count of live (non-deleted) data files in its current
/// snapshot.
async fn live_data_file_count(ctx: &Ctx, table_id: &str) -> usize {
    let location: Option<String> =
        sqlx::query_scalar("SELECT metadata_location FROM tables WHERE id = $1")
            .bind(table_id)
            .fetch_one(&ctx.pool)
            .await
            .expect("load pointer");
    let location = location.expect("table has a metadata location");
    let storage = connect(&ctx.warehouse_root);
    let metadata = meridian_storage::read_table_metadata(storage.as_ref(), &location)
        .await
        .expect("read metadata");
    let snapshot = metadata.current_snapshot().expect("current snapshot");
    let list_loc = snapshot.manifest_list.as_deref().expect("manifest list");
    let list_bytes = storage.read(list_loc).await.expect("read list");
    let list = meridian_iceberg::manifest::read_manifest_list(&list_bytes).expect("parse list");
    let mut count = 0;
    for mf in &list.manifests {
        if mf.content != ManifestContentType::Data {
            continue;
        }
        let bytes = storage
            .read(&mf.manifest_path)
            .await
            .expect("read manifest");
        let manifest = meridian_iceberg::manifest::read_manifest(&bytes).expect("parse manifest");
        for entry in &manifest.entries {
            if entry.status != ManifestEntryStatus::Deleted
                && entry.data_file.content == DataFileContent::Data
            {
                count += 1;
            }
        }
    }
    count
}

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

/// The headline test: the worker claims a queued compaction job, runs the
/// executor, commits the rewrite as a real snapshot through the commit
/// backend, writes the savings-ledger row, and marks the job succeeded — and
/// the table's live data-file count really drops.
#[tokio::test]
async fn worker_runs_and_commits_a_compaction_end_to_end() {
    let Some(ctx) = ctx().await else { return };
    let _serial = serial_lock().await;
    reset_queue(&ctx.pool).await;
    let ns = format!("maint_ns_{}", Ulid::new().to_string().to_lowercase());
    let table_location = format!("{}/{ns}/orders", ctx.warehouse_root);
    let storage = connect(&ctx.warehouse_root);
    // 8 tiny files, all well below the 512 MiB target -> all compactable.
    let (metadata_location, _snap) =
        build_small_files_table(storage.as_ref(), &table_location, 8).await;
    let (table_id, _namespace_id) = register_table(&ctx, &ns, "orders", &metadata_location).await;

    let files_before = live_data_file_count(&ctx, &table_id).await;
    assert_eq!(files_before, 8, "fixture has 8 data files");

    // Enqueue a compaction job and record the pointer version.
    let version_before: i64 =
        sqlx::query_scalar("SELECT pointer_version FROM tables WHERE id = $1")
            .bind(&table_id)
            .fetch_one(&ctx.pool)
            .await
            .expect("pointer version");
    store_maintenance::enqueue_job(
        &ctx.pool,
        tenancy::default_workspace_id(),
        &table_id,
        JobType::Compaction,
        None,
        &json!({ "reason": "test" }),
        "test:maint",
    )
    .await
    .expect("enqueue");

    // Drive one worker step.
    let claimed = maintenance::claim_and_run(&ctx.pool, &ctx.config, "test-worker")
        .await
        .expect("worker step");
    assert!(claimed, "the worker claimed and ran the job");

    // The job succeeded.
    let job: (String, Option<serde_json::Value>) =
        sqlx::query_as("SELECT state, result FROM maintenance_jobs WHERE table_id = $1")
            .bind(&table_id)
            .fetch_one(&ctx.pool)
            .await
            .expect("load job");
    assert_eq!(job.0, "succeeded", "job state (result: {:?})", job.1);
    let result = job.1.expect("succeeded job has a result");
    assert_eq!(result["outcome"], "committed");

    // The commit landed: pointer advanced, and live data files dropped.
    let version_after: i64 = sqlx::query_scalar("SELECT pointer_version FROM tables WHERE id = $1")
        .bind(&table_id)
        .fetch_one(&ctx.pool)
        .await
        .expect("pointer version after");
    assert_eq!(
        version_after,
        version_before + 1,
        "the maintenance commit advanced the pointer exactly once"
    );
    let files_after = live_data_file_count(&ctx, &table_id).await;
    assert!(
        files_after < files_before,
        "compaction reduced live data files ({files_before} -> {files_after})"
    );

    // The savings ledger recorded the job exactly once, with the right shape.
    let ledger: (i64, i64, i64) = sqlx::query_as(
        "SELECT files_before, files_after, bytes_saved FROM savings_ledger WHERE table_id = $1",
    )
    .bind(&table_id)
    .fetch_one(&ctx.pool)
    .await
    .expect("ledger row");
    assert_eq!(ledger.0, files_before as i64, "ledger files_before");
    assert!(ledger.1 < ledger.0, "ledger files_after < files_before");

    // The maintenance commit is a real, audited catalog commit: a
    // table.committed audit row exists attributed to the worker principal.
    let audited: Option<i64> = sqlx::query_scalar(
        "SELECT 1::bigint FROM audit_log WHERE resource = $1 AND action = 'table.commit'
         AND principal LIKE 'maintenance:%' LIMIT 1",
    )
    .bind(format!("table:{table_id}"))
    .fetch_optional(&ctx.pool)
    .await
    .expect("audit query");
    assert!(audited.is_some(), "the maintenance commit was audited");
}

/// A second run on the already-compacted table is a no-op success (idempotent):
/// no new snapshot, no new ledger row.
#[tokio::test]
async fn second_compaction_is_a_noop() {
    let Some(ctx) = ctx().await else { return };
    let _serial = serial_lock().await;
    reset_queue(&ctx.pool).await;
    let ns = format!("maint_ns_{}", Ulid::new().to_string().to_lowercase());
    let table_location = format!("{}/{ns}/orders", ctx.warehouse_root);
    let storage = connect(&ctx.warehouse_root);
    let (metadata_location, _s) =
        build_small_files_table(storage.as_ref(), &table_location, 6).await;
    let (table_id, _n) = register_table(&ctx, &ns, "orders", &metadata_location).await;

    let spec = json!({});
    let enqueue = || {
        store_maintenance::enqueue_job(
            &ctx.pool,
            tenancy::default_workspace_id(),
            &table_id,
            JobType::Compaction,
            None,
            &spec,
            "test:maint",
        )
    };
    enqueue().await.expect("enqueue 1");
    maintenance::claim_and_run(&ctx.pool, &ctx.config, "w1")
        .await
        .expect("run 1");
    let version_after_first: i64 =
        sqlx::query_scalar("SELECT pointer_version FROM tables WHERE id = $1")
            .bind(&table_id)
            .fetch_one(&ctx.pool)
            .await
            .expect("v1");

    // Second job: the table is compact now, so it must be a no-op.
    enqueue().await.expect("enqueue 2");
    maintenance::claim_and_run(&ctx.pool, &ctx.config, "w2")
        .await
        .expect("run 2");
    let version_after_second: i64 =
        sqlx::query_scalar("SELECT pointer_version FROM tables WHERE id = $1")
            .bind(&table_id)
            .fetch_one(&ctx.pool)
            .await
            .expect("v2");
    assert_eq!(
        version_after_second, version_after_first,
        "a no-op compaction must not move the pointer"
    );

    // The second job succeeded as a no-op (outcome noop).
    let noop_outcome: Option<serde_json::Value> = sqlx::query_scalar(
        "SELECT result FROM maintenance_jobs WHERE table_id = $1 ORDER BY created_at DESC LIMIT 1",
    )
    .bind(&table_id)
    .fetch_one(&ctx.pool)
    .await
    .expect("latest job result");
    assert_eq!(
        noop_outcome.expect("result")["outcome"],
        "noop",
        "second run is a recorded no-op"
    );
}

/// The reconciliation loop enqueues a compaction for an unhealthy (small-file)
/// table that has an enabled policy, and debounces a second pass.
#[tokio::test]
async fn reconciler_enqueues_for_unhealthy_table_and_debounces() {
    let Some(ctx) = ctx().await else { return };
    let _serial = serial_lock().await;
    reset_queue(&ctx.pool).await;
    let ns = format!("maint_ns_{}", Ulid::new().to_string().to_lowercase());
    let table_location = format!("{}/{ns}/orders", ctx.warehouse_root);
    let storage = connect(&ctx.warehouse_root);
    let (metadata_location, _s) =
        build_small_files_table(storage.as_ref(), &table_location, 10).await;
    let (table_id, namespace_id) = register_table(&ctx, &ns, "orders", &metadata_location).await;

    // Age the snapshot so the streaming-aware commit-quiet gate does not skip
    // it (the fixture's snapshot timestamp is fixed in 2023, well past the
    // 120 s quiet window, so this is already satisfied — assert it explicitly
    // by using the default config).
    // Compute + persist health so the reconciler has something to evaluate.
    let target = meridian_store::health::HealthTarget {
        table_id: table_id.clone(),
        table_ident: format!("{ns}.orders"),
        metadata_location: metadata_location.clone(),
        target_file_size_bytes: PolicySpec::default().target_file_size_bytes,
        max_staleness_ms: None,
    };
    let health = meridian_store::health::compute_health(
        &ctx.pool,
        storage.as_ref(),
        tenancy::default_workspace_id(),
        &target,
    )
    .await
    .expect("compute health");
    assert!(
        health.metrics.small_file_ratio >= 0.30,
        "fixture is unhealthy (small_file_ratio {})",
        health.metrics.small_file_ratio
    );

    // An enabled namespace policy makes the table a reconcile candidate.
    store_maintenance::create_policy(
        &ctx.pool,
        tenancy::default_workspace_id(),
        Scope::Namespace,
        &namespace_id,
        &PolicySpec::default(),
        "test:maint",
    )
    .await
    .expect("create policy");

    // First pass: enqueues a compaction.
    let enqueued = maintenance::reconcile_once(&ctx.pool, &ctx.config)
        .await
        .expect("reconcile 1");
    assert!(enqueued >= 1, "reconciler enqueued at least one job");
    let queued: i64 = sqlx::query_scalar(
        "SELECT COUNT(*) FROM maintenance_jobs WHERE table_id = $1 AND job_type = 'compaction'",
    )
    .bind(&table_id)
    .fetch_one(&ctx.pool)
    .await
    .expect("count jobs");
    assert_eq!(queued, 1, "exactly one compaction was enqueued");

    // Second pass immediately after: the active-job guard (and debounce)
    // prevents a duplicate.
    maintenance::reconcile_once(&ctx.pool, &ctx.config)
        .await
        .expect("reconcile 2");
    let queued_after: i64 = sqlx::query_scalar(
        "SELECT COUNT(*) FROM maintenance_jobs WHERE table_id = $1 AND job_type = 'compaction'",
    )
    .bind(&table_id)
    .fetch_one(&ctx.pool)
    .await
    .expect("count jobs after");
    assert_eq!(queued_after, 1, "no duplicate job on the second pass");
}

/// The reconciler is streaming-aware: a table whose newest snapshot is very
/// recent (actively committing) is skipped even when unhealthy.
#[tokio::test]
async fn reconciler_skips_actively_committing_table() {
    let Some(ctx) = ctx().await else { return };
    let _serial = serial_lock().await;
    reset_queue(&ctx.pool).await;
    let ns = format!("maint_ns_{}", Ulid::new().to_string().to_lowercase());
    let table_location = format!("{}/{ns}/stream", ctx.warehouse_root);
    let storage = connect(&ctx.warehouse_root);
    let (metadata_location, _s) =
        build_small_files_table(storage.as_ref(), &table_location, 10).await;
    let (table_id, namespace_id) = register_table(&ctx, &ns, "stream", &metadata_location).await;

    // Make the indexed newest snapshot "now" so the commit-quiet window skips
    // it. (The reconciler reads newest_snapshot_ms from the table_snapshots
    // index; bump it to the current time.)
    let now_ms = chrono::Utc::now().timestamp_millis();
    sqlx::query("UPDATE table_snapshots SET timestamp_ms = $1 WHERE table_id = $2")
        .bind(now_ms)
        .bind(&table_id)
        .execute(&ctx.pool)
        .await
        .expect("bump snapshot ts");

    // Health + policy so it would otherwise be enqueued.
    let target = meridian_store::health::HealthTarget {
        table_id: table_id.clone(),
        table_ident: format!("{ns}.stream"),
        metadata_location,
        target_file_size_bytes: PolicySpec::default().target_file_size_bytes,
        max_staleness_ms: None,
    };
    meridian_store::health::compute_health(
        &ctx.pool,
        storage.as_ref(),
        tenancy::default_workspace_id(),
        &target,
    )
    .await
    .expect("compute health");
    store_maintenance::create_policy(
        &ctx.pool,
        tenancy::default_workspace_id(),
        Scope::Namespace,
        &namespace_id,
        &PolicySpec::default(),
        "test:maint",
    )
    .await
    .expect("policy");

    maintenance::reconcile_once(&ctx.pool, &ctx.config)
        .await
        .expect("reconcile");
    let queued: i64 =
        sqlx::query_scalar("SELECT COUNT(*) FROM maintenance_jobs WHERE table_id = $1")
            .bind(&table_id)
            .fetch_one(&ctx.pool)
            .await
            .expect("count");
    assert_eq!(
        queued, 0,
        "an actively-committing table is coalesced (skipped)"
    );
}

/// Snapshot expiry drops old snapshots but never the current or a
/// tag-referenced one, and respects the retention window — committed as a
/// real metadata-only commit.
#[tokio::test]
async fn expiry_respects_refs_and_retention() {
    let Some(ctx) = ctx().await else { return };
    let _serial = serial_lock().await;
    reset_queue(&ctx.pool).await;
    let ns = format!("maint_ns_{}", Ulid::new().to_string().to_lowercase());
    let table_location = format!("{}/{ns}/history", ctx.warehouse_root);
    let storage = connect(&ctx.warehouse_root);
    // Build a table, then hand-extend its metadata with several old snapshots
    // plus a tag, so expiry has something to remove and something to protect.
    let (metadata_location, current_snap) =
        build_small_files_table(storage.as_ref(), &table_location, 4).await;
    let mut metadata = meridian_storage::read_table_metadata(storage.as_ref(), &metadata_location)
        .await
        .expect("read metadata");

    // Add four old snapshots (ids 1..=4) with old timestamps; tag id 2.
    let old_snaps: Vec<Snapshot> = (1..=4)
        .map(|id| Snapshot {
            snapshot_id: id,
            parent_snapshot_id: None,
            sequence_number: Some(id),
            timestamp_ms: 1_600_000_000_000 + id, // year 2020, well past retention age
            manifest_list: metadata
                .current_snapshot()
                .and_then(|s| s.manifest_list.clone()),
            summary: Some(BTreeMap::from([(
                "operation".to_owned(),
                "append".to_owned(),
            )])),
            schema_id: Some(0),
            first_row_id: None,
            added_rows: None,
            extra: serde_json::Map::new(),
        })
        .collect();
    if let Some(snaps) = &mut metadata.snapshots {
        snaps.extend(old_snaps);
    }
    if let Some(refs) = &mut metadata.refs {
        refs.insert(
            "keep-tag".to_owned(),
            SnapshotRef {
                snapshot_id: 2,
                ref_type: meridian_iceberg::spec::RefType::Tag,
                min_snapshots_to_keep: None,
                max_snapshot_age_ms: None,
                max_ref_age_ms: None,
                extra: serde_json::Map::new(),
            },
        );
    }
    // Rewrite the metadata file with the extended snapshot set and re-register.
    let extended_location = format!(
        "{table_location}/metadata/00001-{}.metadata.json",
        Ulid::new()
    );
    meridian_storage::write_table_metadata(storage.as_ref(), &extended_location, &metadata)
        .await
        .expect("write extended metadata");
    let (table_id, _namespace_id) = register_table(&ctx, &ns, "history", &extended_location).await;

    // Sanity: 5 snapshots now (current + 4 old).
    let snapshots_before: i64 =
        sqlx::query_scalar("SELECT COUNT(*) FROM table_snapshots WHERE table_id = $1")
            .bind(&table_id)
            .fetch_one(&ctx.pool)
            .await
            .expect("count snapshots");
    assert_eq!(snapshots_before, 5, "current + 4 old snapshots");

    // A tight retention policy: keep 1 by count, 0 age -> everything old is
    // expirable except the current snapshot and the tagged snapshot 2.
    store_maintenance::enqueue_job(
        &ctx.pool,
        tenancy::default_workspace_id(),
        &table_id,
        JobType::ExpireSnapshots,
        None,
        &json!({}),
        "test:maint",
    )
    .await
    .expect("enqueue expiry");

    // A config with retention that lets old snapshots expire but a safety
    // floor of 1 (never trims to nothing); the policy default keeps 100, so
    // override via a per-table policy with retention_count 1 and age 0.
    store_maintenance::create_policy(
        &ctx.pool,
        tenancy::default_workspace_id(),
        Scope::Table,
        &table_id,
        &PolicySpec {
            snapshot_retention_count: 1,
            snapshot_retention_age_ms: 0,
            ..PolicySpec::default()
        },
        "test:maint",
    )
    .await
    .expect("policy");

    maintenance::claim_and_run(&ctx.pool, &ctx.config, "expiry-worker")
        .await
        .expect("run expiry");

    let job_state: String =
        sqlx::query_scalar("SELECT state FROM maintenance_jobs WHERE table_id = $1")
            .bind(&table_id)
            .fetch_one(&ctx.pool)
            .await
            .expect("job state");
    assert_eq!(job_state, "succeeded", "expiry committed");

    // The current snapshot and the tagged snapshot (2) survive; some old ones
    // were removed.
    let remaining: Vec<i64> =
        sqlx::query_scalar("SELECT snapshot_id FROM table_snapshots WHERE table_id = $1")
            .bind(&table_id)
            .fetch_all(&ctx.pool)
            .await
            .expect("remaining snapshots");
    assert!(
        remaining.contains(&current_snap),
        "current snapshot {current_snap} must survive expiry (have {remaining:?})"
    );
    assert!(
        remaining.contains(&2),
        "tag-referenced snapshot 2 must survive (have {remaining:?})"
    );
    assert!(
        remaining.len() < 5,
        "expiry removed at least one old snapshot (have {remaining:?})"
    );
}

/// A job type the built-in worker does not implement (`remove_orphans`) is
/// failed cleanly with a reason, not left running.
#[tokio::test]
async fn unsupported_job_type_fails_cleanly() {
    let Some(ctx) = ctx().await else { return };
    let _serial = serial_lock().await;
    reset_queue(&ctx.pool).await;
    let ns = format!("maint_ns_{}", Ulid::new().to_string().to_lowercase());
    let table_location = format!("{}/{ns}/orders", ctx.warehouse_root);
    let storage = connect(&ctx.warehouse_root);
    let (metadata_location, _s) =
        build_small_files_table(storage.as_ref(), &table_location, 3).await;
    let (table_id, _n) = register_table(&ctx, &ns, "orders", &metadata_location).await;

    // A single-attempt config so the unsupported job fails rather than
    // re-queuing forever.
    let config = MaintenanceConfig {
        max_job_attempts: 1,
        ..ctx.config.clone()
    };
    store_maintenance::enqueue_job(
        &ctx.pool,
        tenancy::default_workspace_id(),
        &table_id,
        JobType::RemoveOrphans,
        None,
        &json!({}),
        "test:maint",
    )
    .await
    .expect("enqueue");

    maintenance::claim_and_run(&ctx.pool, &config, "w")
        .await
        .expect("run");
    let state: String =
        sqlx::query_scalar("SELECT state FROM maintenance_jobs WHERE table_id = $1")
            .bind(&table_id)
            .fetch_one(&ctx.pool)
            .await
            .expect("state");
    assert_eq!(
        state, "failed",
        "unsupported job type fails after its attempt"
    );

    // Keep the state ref alive (silence unused warnings on `state` helper).
    let _ = &ctx.state;
    let _ = JobState::Failed;
}
