//! Database-backed tests for [`meridian_store::health::compute_health`]: the
//! end-to-end path that reads a synthetic table's manifests off a local
//! `file://` warehouse and persists a `health_snapshots` row.
//!
//! The pure formula (determinism, empty/all-large/all-small edge cases) is
//! covered by unit tests in `src/health.rs`; this file checks the wiring —
//! manifest reads → metrics → persisted row → history — against real fixtures.
//!
//! Requires a running Postgres and `DATABASE_URL`; skips without it.

// Test fixtures build manifests from small, exact sizes: `usize as i64/i32`
// casts on fixture counts cannot wrap at these magnitudes, exact float
// equality on chosen ratios is the clearest assertion, and the fixture
// builders are long by nature.
#![allow(
    clippy::cast_possible_truncation,
    clippy::cast_possible_wrap,
    clippy::float_cmp,
    clippy::too_many_lines
)]

use std::collections::BTreeMap;

use bytes::Bytes;
use meridian_common::config::DatabaseConfig;
use meridian_common::id::WorkspaceId;
use meridian_iceberg::manifest::{
    DataFile, DataFileContent, ManifestContentType, ManifestEntry, ManifestEntryStatus,
    ManifestFile, ManifestListWriteParams, ManifestWriteParams, PartitionTuple, write_manifest,
    write_manifest_list,
};
use meridian_iceberg::spec::{PrimitiveType, StructField, TableMetadata, Type};
use meridian_iceberg::spec::{Schema, Snapshot};
use meridian_storage::{Storage, StorageProfile};
use meridian_store::commit::{
    CommitTableOp, DerivedTableState, PostgresCommitBackend, SnapshotIndexRow,
};
use meridian_store::health::{self, DEFAULT_TARGET_FILE_SIZE_BYTES, HealthTarget};
use meridian_store::table::{self, NewTable};
use meridian_store::{namespace, tenancy, warehouse};
use serde_json::json;
use sqlx::PgPool;
use ulid::Ulid;
use uuid::Uuid;

const TARGET: i64 = DEFAULT_TARGET_FILE_SIZE_BYTES;

struct Fixture {
    pool: PgPool,
    workspace: WorkspaceId,
    storage: std::sync::Arc<dyn Storage>,
    root: String,
    _tmp: tempfile::TempDir,
}

async fn fixture() -> Option<Fixture> {
    let Ok(url) = std::env::var("DATABASE_URL") else {
        eprintln!("skipping health DB test: DATABASE_URL is not set");
        return None;
    };
    let config = DatabaseConfig {
        url,
        ..DatabaseConfig::default()
    };
    let pool = meridian_store::connect(&config)
        .await
        .expect("connect to test database");
    meridian_store::MIGRATOR.run(&pool).await.expect("migrate");

    let tmp = tempfile::tempdir().expect("tempdir");
    let root = format!("file://{}", tmp.path().display());
    let profile = StorageProfile::parse(&root, &BTreeMap::new()).expect("profile");
    let storage = profile.connect().expect("connect storage");

    Some(Fixture {
        pool,
        workspace: tenancy::default_workspace_id(),
        storage,
        root,
        _tmp: tmp,
    })
}

/// A minimal single-column schema.
fn schema() -> Schema {
    Schema::new(vec![StructField::required(
        1,
        "id",
        Type::Primitive(PrimitiveType::Long),
    )])
    .with_schema_id(0)
}

fn data_file(path: &str, size: i64) -> DataFile {
    DataFile {
        content: DataFileContent::Data,
        file_path: path.to_owned(),
        file_format: "PARQUET".to_owned(),
        partition: PartitionTuple::default(),
        record_count: 1,
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
    }
}

fn added_entry(file: DataFile, snapshot_id: i64) -> ManifestEntry {
    ManifestEntry {
        status: ManifestEntryStatus::Added,
        snapshot_id: Some(snapshot_id),
        sequence_number: Some(1),
        file_sequence_number: Some(1),
        data_file: file,
    }
}

/// Writes a synthetic table (metadata.json + manifest list + one data
/// manifest) with `file_sizes` data files under `<root>/<name>/`. Returns the
/// metadata location and the table location. Also inserts the `tables` row and
/// its snapshot index so health's index reads see the snapshot.
async fn synthetic_table(fx: &Fixture, name: &str, file_sizes: &[i64]) -> (String, String) {
    let run = Ulid::new().to_string().to_lowercase();
    let table_loc = format!("{}/{name}-{run}", fx.root);
    let snapshot_id: i64 = 100;

    // Data manifest.
    let entries: Vec<ManifestEntry> = file_sizes
        .iter()
        .enumerate()
        .map(|(i, &size)| {
            added_entry(
                data_file(&format!("{table_loc}/data/f{i}.parquet"), size),
                snapshot_id,
            )
        })
        .collect();
    let schema = schema();
    let schema_json = serde_json::to_string(&schema).expect("schema json");
    let manifest_bytes = write_manifest(&ManifestWriteParams {
        format_version: 2,
        content: ManifestContentType::Data,
        schema_json: &schema_json,
        schema_id: Some(0),
        partition_spec_id: 0,
        partition_fields: &[],
        partition_types: &[],
        entries: &entries,
    })
    .expect("write manifest");
    let manifest_loc = format!("{table_loc}/metadata/manifest-0.avro");
    fx.storage
        .write(&manifest_loc, Bytes::from(manifest_bytes.clone()))
        .await
        .expect("write manifest file");

    // Manifest list.
    let manifest_file = ManifestFile {
        manifest_path: manifest_loc.clone(),
        manifest_length: manifest_bytes.len() as i64,
        partition_spec_id: 0,
        content: ManifestContentType::Data,
        sequence_number: 1,
        min_sequence_number: 1,
        added_snapshot_id: snapshot_id,
        added_files_count: Some(entries.len() as i32),
        existing_files_count: Some(0),
        deleted_files_count: Some(0),
        added_rows_count: Some(entries.len() as i64),
        existing_rows_count: Some(0),
        deleted_rows_count: Some(0),
        partitions: None,
        key_metadata: None,
        first_row_id: None,
    };
    let list_bytes = write_manifest_list(&ManifestListWriteParams {
        format_version: 2,
        snapshot_id,
        parent_snapshot_id: None,
        sequence_number: Some(1),
        manifests: &[manifest_file],
    })
    .expect("write list");
    let manifest_list_loc = format!("{table_loc}/metadata/snap-{snapshot_id}.avro");
    fx.storage
        .write(&manifest_list_loc, Bytes::from(list_bytes))
        .await
        .expect("write list file");

    // metadata.json with a current snapshot pointing at the list.
    let snapshot = Snapshot {
        snapshot_id,
        parent_snapshot_id: None,
        sequence_number: Some(1),
        timestamp_ms: 1_700_000_000_000,
        manifest_list: Some(manifest_list_loc),
        summary: Some(BTreeMap::from([(
            "operation".to_owned(),
            "append".to_owned(),
        )])),
        schema_id: Some(0),
        first_row_id: None,
        added_rows: None,
        extra: serde_json::Map::new(),
    };
    let metadata = TableMetadata {
        format_version: 2,
        table_uuid: Uuid::new_v4(),
        location: table_loc.clone(),
        last_sequence_number: Some(1),
        next_row_id: None,
        last_updated_ms: 1_700_000_000_000,
        last_column_id: 1,
        schemas: vec![schema],
        current_schema_id: 0,
        partition_specs: vec![meridian_iceberg::spec::PartitionSpec::unpartitioned(0)],
        default_spec_id: 0,
        last_partition_id: 999,
        sort_orders: vec![meridian_iceberg::spec::SortOrder::unsorted()],
        default_sort_order_id: 0,
        properties: None,
        current_snapshot_id: Some(snapshot_id),
        snapshots: Some(vec![snapshot]),
        snapshot_log: None,
        metadata_log: None,
        refs: None,
        statistics: None,
        partition_statistics: None,
        encryption_keys: None,
        extra: serde_json::Map::new(),
    };
    let metadata_loc = format!(
        "{table_loc}/metadata/00000-{}.metadata.json",
        Uuid::new_v4()
    );
    meridian_storage::write_table_metadata(fx.storage.as_ref(), &metadata_loc, &metadata)
        .await
        .expect("write metadata");

    (metadata_loc, table_loc)
}

/// Inserts a `tables` row + a snapshot index row for a synthetic table so the
/// health index reads resolve.
async fn register_table(fx: &Fixture, name: &str, metadata_loc: &str) -> (String, String, String) {
    let run = Ulid::new().to_string().to_lowercase();
    let wh = warehouse::create(
        &fx.pool,
        fx.workspace,
        &format!("health-wh-{run}"),
        &fx.root,
        BTreeMap::new(),
        "test:health",
    )
    .await
    .expect("warehouse");
    let levels = vec![format!("health_ns_{run}")];
    let ns = namespace::create(
        &fx.pool,
        fx.workspace,
        &wh.id,
        &levels,
        BTreeMap::new(),
        "test:health",
    )
    .await
    .expect("namespace");
    let uuid = format!("uuid-{}", Ulid::new());
    let tbl = table::create(
        &fx.pool,
        NewTable {
            workspace_id: fx.workspace,
            namespace_id: &ns.id,
            namespace_levels: &levels,
            name,
            table_uuid: &uuid,
            metadata_location: metadata_loc,
            format_version: 2,
            properties: &BTreeMap::new(),
            schema_text: None,
            snapshots: &[],
            origin: "create",
        },
        "test:health",
        None,
    )
    .await
    .expect("table");

    // Write the snapshot index via a commit (the write-through path) so
    // health's `table_snapshots` read sees the current snapshot.
    let backend = PostgresCommitBackend::new(fx.pool.clone(), fx.workspace, "test:health");
    let derived = DerivedTableState {
        format_version: 2,
        properties: BTreeMap::new(),
        snapshots: vec![SnapshotIndexRow {
            snapshot_id: 100,
            parent_snapshot_id: None,
            sequence_number: Some(1),
            timestamp_ms: 1_700_000_000_000,
            manifest_list: Some("unused".to_owned()),
            operation: Some("append".to_owned()),
            summary: json!({"operation": "append"}),
            is_current: true,
        }],
        schema_text: None,
        event_details: json!({}),
    };
    let new_loc = format!("{metadata_loc}.v1");
    backend
        .commit_tables(
            &[CommitTableOp {
                cas: meridian_iceberg::commit::PointerCas {
                    table: tbl.id.clone(),
                    expected_version: 0,
                    new_metadata_location: new_loc,
                },
                derived: Some(derived),
                contract_violation: None,
            }],
            None,
        )
        .await
        .expect("commit snapshot index");

    (wh.id, ns.id, tbl.id)
}

#[tokio::test]
async fn compute_health_all_large_files_scores_100() {
    let Some(fx) = fixture().await else { return };

    // 8 files at exactly the target size: none is "small".
    let (metadata_loc, _) = synthetic_table(&fx, "large", &[TARGET; 8]).await;
    let (_, _, table_id) = register_table(&fx, "large_tbl", &metadata_loc).await;

    let target = HealthTarget {
        table_id: table_id.clone(),
        table_ident: "health_ns.large".to_owned(),
        metadata_location: metadata_loc,
        target_file_size_bytes: TARGET,
        max_staleness_ms: None,
    };
    let rec = health::compute_health(&fx.pool, fx.storage.as_ref(), fx.workspace, &target)
        .await
        .expect("compute health");

    assert_eq!(rec.metrics.data_file_count, 8);
    assert_eq!(rec.metrics.small_file_ratio, 0.0);
    assert_eq!(rec.metrics.total_bytes, TARGET * 8);
    assert_eq!(rec.score, 100, "all-large-files table scores 100");
    assert!(rec.recommendations.is_empty());
    assert_eq!(rec.snapshot_id, Some(100));

    // The row is persisted and readable as history.
    let hist = health::history(&fx.pool, &table_id, 10)
        .await
        .expect("history");
    assert_eq!(hist.len(), 1);
    assert_eq!(hist[0].score, 100);
}

#[tokio::test]
async fn compute_health_many_small_files_scores_low_and_recommends_compaction() {
    let Some(fx) = fixture().await else { return };

    // 200 tiny files: all below target.
    let (metadata_loc, _) = synthetic_table(&fx, "small", &[4096; 200]).await;
    let (_, _, table_id) = register_table(&fx, "small_tbl", &metadata_loc).await;

    let target = HealthTarget {
        table_id: table_id.clone(),
        table_ident: "health_ns.small".to_owned(),
        metadata_location: metadata_loc,
        target_file_size_bytes: TARGET,
        max_staleness_ms: None,
    };
    let rec = health::compute_health(&fx.pool, fx.storage.as_ref(), fx.workspace, &target)
        .await
        .expect("compute health");

    assert_eq!(rec.metrics.data_file_count, 200);
    assert_eq!(rec.metrics.small_file_ratio, 1.0);
    assert!(rec.score < 100, "a small-file table is unhealthy");
    assert_eq!(
        rec.recommendations.first().map(|r| r.action.as_str()),
        Some("compaction"),
    );
    // The histogram should place all 200 files in the smallest bucket.
    assert_eq!(rec.metrics.file_size_histogram.get("0:<1MiB"), Some(&200));
}

#[tokio::test]
async fn compute_health_empty_table_scores_100() {
    let Some(fx) = fixture().await else { return };

    // A metadata.json with no current snapshot: an empty table.
    let run = Ulid::new().to_string().to_lowercase();
    let table_loc = format!("{}/empty-{run}", fx.root);
    let metadata = TableMetadata {
        format_version: 2,
        table_uuid: Uuid::new_v4(),
        location: table_loc.clone(),
        last_sequence_number: Some(0),
        next_row_id: None,
        last_updated_ms: 1_700_000_000_000,
        last_column_id: 1,
        schemas: vec![schema()],
        current_schema_id: 0,
        partition_specs: vec![meridian_iceberg::spec::PartitionSpec::unpartitioned(0)],
        default_spec_id: 0,
        last_partition_id: 999,
        sort_orders: vec![meridian_iceberg::spec::SortOrder::unsorted()],
        default_sort_order_id: 0,
        properties: None,
        current_snapshot_id: None,
        snapshots: None,
        snapshot_log: None,
        metadata_log: None,
        refs: None,
        statistics: None,
        partition_statistics: None,
        encryption_keys: None,
        extra: serde_json::Map::new(),
    };
    let metadata_loc = format!(
        "{table_loc}/metadata/00000-{}.metadata.json",
        Uuid::new_v4()
    );
    meridian_storage::write_table_metadata(fx.storage.as_ref(), &metadata_loc, &metadata)
        .await
        .expect("write metadata");

    // Register a table row but without a snapshot index (empty table).
    let run2 = Ulid::new().to_string().to_lowercase();
    let wh = warehouse::create(
        &fx.pool,
        fx.workspace,
        &format!("health-empty-wh-{run2}"),
        &fx.root,
        BTreeMap::new(),
        "test:health",
    )
    .await
    .expect("warehouse");
    let levels = vec![format!("health_empty_ns_{run2}")];
    let ns = namespace::create(
        &fx.pool,
        fx.workspace,
        &wh.id,
        &levels,
        BTreeMap::new(),
        "test:health",
    )
    .await
    .expect("namespace");
    let uuid = format!("uuid-{}", Ulid::new());
    let tbl = table::create(
        &fx.pool,
        NewTable {
            workspace_id: fx.workspace,
            namespace_id: &ns.id,
            namespace_levels: &levels,
            name: "empty",
            table_uuid: &uuid,
            metadata_location: &metadata_loc,
            format_version: 2,
            properties: &BTreeMap::new(),
            schema_text: None,
            snapshots: &[],
            origin: "create",
        },
        "test:health",
        None,
    )
    .await
    .expect("table");

    let target = HealthTarget {
        table_id: tbl.id.clone(),
        table_ident: "health_empty_ns.empty".to_owned(),
        metadata_location: metadata_loc,
        target_file_size_bytes: TARGET,
        max_staleness_ms: None,
    };
    let rec = health::compute_health(&fx.pool, fx.storage.as_ref(), fx.workspace, &target)
        .await
        .expect("compute health");
    assert_eq!(rec.metrics.data_file_count, 0);
    assert_eq!(rec.metrics.total_bytes, 0);
    assert_eq!(rec.score, 100, "an empty table has no debt");
    assert_eq!(rec.snapshot_id, None);
    assert!(rec.recommendations.is_empty());
}
