//! End-to-end correctness tests for the compaction engine.
//!
//! Each test builds a synthetic table (real Parquet + real Iceberg manifests
//! in memory), runs `compact_with_sources`, and checks the correctness bar:
//! file count drops, row count is identical (or reduced by exactly the applied
//! deletes), every surviving row is present in the output, field ids are
//! preserved, and the produced `TableUpdate`/manifests parse back through
//! `meridian_iceberg`.

mod support;

use std::sync::Arc;

use meridian_executor::{CompactionOptions, compact_with_sources};
use meridian_iceberg::manifest::{
    DataFileContent, ManifestContentType, ManifestEntryStatus, read_manifest, read_manifest_list,
};
use meridian_iceberg::spec::{TableRequirement, TableUpdate};
use support::{
    DataFileSpec, MemStore, ParquetLayout, PositionDeleteSpec, RecordingStorage, Row,
    base_metadata, build_fixture, output_field_ids, read_orders_parquet,
};

/// A deterministic snapshot-id source for tests.
fn ids_from(start: i64) -> impl Fn() -> i64 {
    let counter = std::sync::atomic::AtomicI64::new(start);
    move || counter.fetch_add(1, std::sync::atomic::Ordering::Relaxed)
}

/// Rows for category `c`, ids `[base, base+n)`.
fn rows(category: &str, base: i64, n: i64) -> Vec<Row> {
    (base..base + n)
        .map(|id| Row {
            id,
            category: category.to_owned(),
            amount: id * 10,
        })
        .collect()
}

/// Builds `count` small one-file-per-commit data files for a partition, each
/// with `rows_each` rows, sizes below the target so they are all candidates.
fn small_files(
    category: &str,
    count: i64,
    rows_each: i64,
    start_seq: i64,
    table_location: &str,
) -> Vec<DataFileSpec> {
    (0..count)
        .map(|i| DataFileSpec {
            path: format!("{table_location}/data/{category}-{i}.parquet"),
            category: category.to_owned(),
            rows: rows(category, i * rows_each, rows_each),
            size_bytes: 1024, // small: well under the target
            sequence_number: start_seq + i,
            snapshot_id: 100 + i,
            layout: ParquetLayout::Normal,
        })
        .collect()
}

#[tokio::test]
async fn bin_pack_merges_small_files_preserving_every_row() {
    let table = "s3://wh/db/orders";
    let mut store = MemStore::default();
    // Two partitions, 6 small files each (above the default min of 5).
    let mut files = small_files("books", 6, 3, 1, table);
    files.extend(small_files("music", 6, 3, 7, table));
    let metadata = build_fixture(&mut store, table, 2, &files, &[], 999, 12);

    let writer = RecordingStorage::default();
    let plan = compact_with_sources(
        &store,
        &store,
        &writer,
        &metadata,
        &CompactionOptions::default(),
        &ids_from(5000),
    )
    .await
    .expect("compaction plan");

    // A plan was produced (not a no-op).
    assert!(!plan.is_noop(), "expected a real rewrite");
    assert_eq!(plan.base_snapshot_id, Some(999));

    // File count drops: 12 small files -> at most 2 (one per partition).
    assert_eq!(plan.stats.files_before, 12);
    assert!(
        plan.stats.files_after <= 2,
        "expected <= 2 output files, got {}",
        plan.stats.files_after
    );
    assert!(plan.stats.files_after < plan.stats.files_before);

    // Row count identical (no deletes).
    assert_eq!(plan.stats.records_before, 36);
    assert_eq!(plan.stats.records_after, 36);

    // Every original row is present in the output, exactly once.
    let mut expected: Vec<Row> = files.iter().flat_map(|f| f.rows.clone()).collect();
    expected.sort();
    let mut got = read_all_output_rows(&writer, &plan);
    got.sort();
    assert_eq!(got, expected, "every original row must survive, once");

    // Field ids preserved on every output column.
    for out in &plan.new_files_written {
        assert!(out.written);
        let bytes = written_bytes(&writer, &out.data_file.file_path);
        assert_eq!(
            output_field_ids(&bytes),
            vec![1, 2, 3],
            "field ids preserved"
        );
    }

    // The produced updates/requirements parse back through meridian_iceberg
    // and describe a replace snapshot moving main.
    assert_replace_commit(
        &plan.updates,
        &plan.requirements,
        plan.new_snapshot_id.unwrap(),
    );

    // And the produced manifests/manifest-list parse back and reference the
    // new files (ADDED) and the old files (DELETED).
    assert_manifests_parse_back(
        &writer,
        &plan,
        12,
        i64::try_from(expected.len()).expect("row count fits i64"),
    );
}

/// Columns are mapped by Iceberg field id, never by physical position: a
/// partition mixing normally-ordered files with files whose Parquet columns
/// are in reversed physical order must still compact into correct rows, with
/// the output in canonical schema order and every field id preserved.
#[tokio::test]
async fn field_ids_map_columns_regardless_of_physical_order() {
    let table = "s3://wh/db/reorder";
    let mut store = MemStore::default();

    // Three normally-ordered files and three reversed-column files, same
    // partition, all small (>= the default min of 5 together).
    let mut files = small_files("books", 3, 3, 1, table);
    for (i, spec) in small_files("books", 3, 3, 4, table).into_iter().enumerate() {
        let i = i64::try_from(i).expect("index fits i64");
        files.push(DataFileSpec {
            path: format!("{table}/data/books-rev-{i}.parquet"),
            layout: ParquetLayout::ReversedColumns,
            // Distinct ids so rows don't collide with the normal files.
            rows: rows("books", 100 + i * 3, 3),
            ..spec
        });
    }
    let metadata = build_fixture(&mut store, table, 2, &files, &[], 999, 12);

    let writer = RecordingStorage::default();
    let plan = compact_with_sources(
        &store,
        &store,
        &writer,
        &metadata,
        &CompactionOptions::default(),
        &ids_from(7000),
    )
    .await
    .expect("plan");

    assert!(!plan.is_noop(), "6 small files -> a rewrite");
    assert_eq!(plan.stats.files_before, 6);
    assert_eq!(plan.stats.records_before, 18);
    assert_eq!(plan.stats.records_after, 18);

    // Every row survives, read back by field id (so a mis-ordered output is
    // caught): the reversed-column inputs must be realigned correctly.
    let mut expected: Vec<Row> = files.iter().flat_map(|f| f.rows.clone()).collect();
    expected.sort();
    let mut got = read_all_output_rows(&writer, &plan);
    got.sort();
    assert_eq!(
        got, expected,
        "reversed-column inputs realigned by field id, every row present"
    );

    // The output is always in canonical schema order (1, 2, 3), never the
    // reversed physical order of some inputs.
    for out in &plan.new_files_written {
        let bytes = written_bytes(&writer, &out.data_file.file_path);
        assert_eq!(
            output_field_ids(&bytes),
            vec![1, 2, 3],
            "output in canonical field-id order"
        );
    }
}

/// Schema evolution: an input file written before the `amount` column was
/// added lacks that column. Compaction must synthesize an all-null `amount`
/// for those rows (mapping by field id, filling absent optional fields), never
/// drop the column or the rows.
#[tokio::test]
async fn schema_evolution_missing_column_is_synthesized_null() {
    let table = "s3://wh/db/evolve";
    let mut store = MemStore::default();

    // Three current-schema files (id, category, amount) ...
    let mut files = small_files("eu", 3, 3, 1, table);
    // ... and three older files predating `amount` (id, category only). Their
    // `Row.amount` is set but not written to Parquet; on read-back the
    // engine-synthesized column is null, which `read_orders_parquet` surfaces
    // as 0 — so the expected rows for these carry amount 0.
    for (i, spec) in small_files("eu", 3, 3, 4, table).into_iter().enumerate() {
        let i = i64::try_from(i).expect("index fits i64");
        files.push(DataFileSpec {
            path: format!("{table}/data/eu-old-{i}.parquet"),
            layout: ParquetLayout::NoAmountColumn,
            rows: (0..3)
                .map(|j| Row {
                    id: 200 + i * 3 + j,
                    category: "eu".to_owned(),
                    amount: 0, // absent in Parquet -> reads back as null/0
                })
                .collect(),
            ..spec
        });
    }
    let metadata = build_fixture(&mut store, table, 2, &files, &[], 999, 12);

    let writer = RecordingStorage::default();
    let plan = compact_with_sources(
        &store,
        &store,
        &writer,
        &metadata,
        &CompactionOptions::default(),
        &ids_from(8000),
    )
    .await
    .expect("plan");

    assert!(!plan.is_noop());
    assert_eq!(plan.stats.records_before, 18);
    assert_eq!(plan.stats.records_after, 18, "no rows dropped");

    // Every row present; the older files' rows read back with a null (0)
    // amount, proving the column was synthesized rather than the rows lost.
    let mut expected: Vec<Row> = files.iter().flat_map(|f| f.rows.clone()).collect();
    expected.sort();
    let mut got = read_all_output_rows(&writer, &plan);
    got.sort();
    assert_eq!(
        got, expected,
        "evolved rows present with synthesized amount"
    );

    // The output still carries all three field ids (the synthesized column is
    // a real, field-id-tagged column, just all-null for the old rows).
    for out in &plan.new_files_written {
        let bytes = written_bytes(&writer, &out.data_file.file_path);
        assert_eq!(output_field_ids(&bytes), vec![1, 2, 3]);
    }
}

#[tokio::test]
async fn merge_on_read_deletes_are_materialized_and_delete_file_dropped() {
    let table = "s3://wh/db/mor";
    let mut store = MemStore::default();

    // Five small data files in one partition (>= min), each 4 rows.
    let files = small_files("eu", 5, 4, 1, table);
    // A position-delete file removing row 0 of the first data file and row 2
    // of the third — sequence number above the data (applies to them).
    let del = PositionDeleteSpec {
        path: format!("{table}/data/pos-delete.parquet"),
        category: "eu".to_owned(),
        deletes: vec![
            (files[0].path.clone(), 0), // deletes id 0
            (files[2].path.clone(), 2), // deletes id 10 (file 2 rows 8,9,10,11)
        ],
        sequence_number: 100,
        snapshot_id: 200,
    };
    let metadata = build_fixture(&mut store, table, 2, &files, &[del], 999, 100);

    let writer = RecordingStorage::default();
    let plan = compact_with_sources(
        &store,
        &store,
        &writer,
        &metadata,
        &CompactionOptions::default(),
        &ids_from(6000),
    )
    .await
    .expect("plan");

    assert!(!plan.is_noop());
    // 20 input rows, 2 deleted -> 18 out.
    assert_eq!(plan.stats.records_before, 20);
    assert_eq!(plan.stats.records_after, 18);
    // The delete file was fully consumed and dropped.
    assert_eq!(plan.stats.delete_files_removed, 1);

    // The two deleted rows are absent; every other row is present exactly once.
    let mut expected: Vec<Row> = files.iter().flat_map(|f| f.rows.clone()).collect();
    expected.retain(|r| r.id != 0 && r.id != 10);
    expected.sort();
    let mut got = read_all_output_rows(&writer, &plan);
    got.sort();
    assert_eq!(got, expected, "deleted rows absent, all others present");
    assert!(
        !got.iter().any(|r| r.id == 0 || r.id == 10),
        "deleted ids gone"
    );

    // No delete manifest is referenced by the new snapshot: every carried
    // manifest is a data manifest.
    let manifests = parse_new_manifests(&writer, &plan);
    for (list_entry, _) in &manifests {
        assert_eq!(
            list_entry.content,
            ManifestContentType::Data,
            "compacted snapshot carries no delete manifests (all consumed)"
        );
    }
    // And no data file in the new snapshot has an attached delete: the output
    // manifest holds only ADDED (new) + DELETED (old) data files, no deletes.
    for (_, manifest) in &manifests {
        for entry in &manifest.entries {
            assert_eq!(entry.data_file.content, DataFileContent::Data);
        }
    }
}

#[tokio::test]
async fn dry_run_writes_nothing_and_produces_no_updates() {
    let table = "s3://wh/db/dry";
    let mut store = MemStore::default();
    let files = small_files("x", 8, 2, 1, table);
    let metadata = build_fixture(&mut store, table, 2, &files, &[], 999, 8);

    let writer = RecordingStorage::default();
    let options = CompactionOptions {
        dry_run: true,
        ..CompactionOptions::default()
    };
    let plan = compact_with_sources(&store, &store, &writer, &metadata, &options, &ids_from(1))
        .await
        .expect("plan");

    // Dry-run: files it WOULD write are reported, but nothing is committed or
    // written.
    assert!(plan.is_noop(), "dry-run has no updates to commit");
    assert!(plan.updates.is_empty());
    assert!(!plan.new_files_written.is_empty(), "reports planned files");
    assert!(
        plan.new_files_written.iter().all(|f| !f.written),
        "dry-run writes nothing"
    );
    assert!(
        writer.written.lock().expect("lock").is_empty(),
        "storage untouched in dry-run"
    );
    assert_eq!(plan.stats.files_before, 8);
}

#[tokio::test]
async fn already_compact_table_is_a_noop() {
    let table = "s3://wh/db/big";
    let mut store = MemStore::default();
    // Two files, each already at/above the target size -> no candidates.
    let big = vec![
        DataFileSpec {
            path: format!("{table}/data/big-0.parquet"),
            category: "a".to_owned(),
            rows: rows("a", 0, 3),
            size_bytes: 600 * 1024 * 1024, // above default 512 MiB target
            sequence_number: 1,
            snapshot_id: 100,
            layout: ParquetLayout::Normal,
        },
        DataFileSpec {
            path: format!("{table}/data/big-1.parquet"),
            category: "a".to_owned(),
            rows: rows("a", 3, 3),
            size_bytes: 600 * 1024 * 1024,
            sequence_number: 2,
            snapshot_id: 101,
            layout: ParquetLayout::Normal,
        },
    ];
    let metadata = build_fixture(&mut store, table, 2, &big, &[], 999, 2);

    let writer = RecordingStorage::default();
    let plan = compact_with_sources(
        &store,
        &store,
        &writer,
        &metadata,
        &CompactionOptions::default(),
        &ids_from(1),
    )
    .await
    .expect("plan");

    assert!(plan.is_noop(), "already-compact table -> no-op");
    assert!(writer.written.lock().expect("lock").is_empty());
}

#[tokio::test]
async fn partition_below_min_input_files_is_skipped() {
    let table = "s3://wh/db/few";
    let mut store = MemStore::default();
    // Only 3 small files in the partition; default min is 5.
    let files = small_files("solo", 3, 2, 1, table);
    let metadata = build_fixture(&mut store, table, 2, &files, &[], 999, 3);

    let writer = RecordingStorage::default();
    let plan = compact_with_sources(
        &store,
        &store,
        &writer,
        &metadata,
        &CompactionOptions::default(),
        &ids_from(1),
    )
    .await
    .expect("plan");
    assert!(plan.is_noop(), "below min_input_files -> nothing to gain");

    // But lowering the threshold makes it compact.
    let options = CompactionOptions {
        min_input_files: 2,
        ..CompactionOptions::default()
    };
    let plan = compact_with_sources(&store, &store, &writer, &metadata, &options, &ids_from(1))
        .await
        .expect("plan");
    assert!(!plan.is_noop(), "min_input_files=2 -> compacts the 3 files");
    assert_eq!(plan.stats.files_before, 3);
}

#[tokio::test]
async fn empty_table_is_a_noop() {
    // A table whose current snapshot has no manifest list at all is refused,
    // but a table with no current snapshot is a clean no-op.
    let table = "s3://wh/db/empty";
    let metadata = base_metadata_without_snapshot(table);
    let store = MemStore::default();
    let writer = RecordingStorage::default();
    let plan = compact_with_sources(
        &store,
        &store,
        &writer,
        &metadata,
        &CompactionOptions::default(),
        &ids_from(1),
    )
    .await
    .expect("plan");
    assert!(plan.is_noop());
    assert_eq!(plan.base_snapshot_id, None);
}

// ---------------------------------------------------------------------------
// Helpers
// ---------------------------------------------------------------------------

fn base_metadata_without_snapshot(table: &str) -> meridian_iceberg::spec::TableMetadata {
    let mut m = base_metadata(table, 2, 1, 1, "unused");
    m.current_snapshot_id = None;
    m.snapshots = Some(Vec::new());
    m.refs = None;
    m
}

fn written_bytes(writer: &RecordingStorage, path: &str) -> bytes::Bytes {
    writer
        .written
        .lock()
        .expect("lock")
        .get(path)
        .cloned()
        .unwrap_or_else(|| panic!("nothing written to {path}"))
}

/// Reads every output data file's rows from the recorded writes.
fn read_all_output_rows(
    writer: &RecordingStorage,
    plan: &meridian_executor::CompactionPlan,
) -> Vec<Row> {
    let mut rows = Vec::new();
    for out in &plan.new_files_written {
        let bytes = written_bytes(writer, &out.data_file.file_path);
        rows.extend(read_orders_parquet(&bytes));
    }
    rows
}

/// Asserts the plan's updates are a replace add-snapshot + a move of `main`,
/// and the requirements assert the table uuid + `main` unchanged.
fn assert_replace_commit(
    updates: &[TableUpdate],
    requirements: &[TableRequirement],
    new_snapshot_id: i64,
) {
    let add = updates
        .iter()
        .find_map(|u| match u {
            TableUpdate::AddSnapshot { snapshot } => Some(snapshot),
            _ => None,
        })
        .expect("an add-snapshot update");
    assert_eq!(add.snapshot_id, new_snapshot_id);
    assert_eq!(
        add.summary
            .as_ref()
            .and_then(|s| s.get("operation"))
            .map(String::as_str),
        Some("replace"),
        "operation must be replace"
    );
    // added-records == deleted-records for a pure bin-pack (rows conserved).
    let summary = add.summary.as_ref().expect("summary");
    assert_eq!(summary.get("added-records"), summary.get("deleted-records"));

    let moves_main = updates.iter().any(|u| {
        matches!(u, TableUpdate::SetSnapshotRef { ref_name, reference }
            if ref_name == "main" && reference.snapshot_id == new_snapshot_id)
    });
    assert!(moves_main, "must move main to the new snapshot");

    assert!(
        requirements
            .iter()
            .any(|r| matches!(r, TableRequirement::AssertTableUuid { .. })),
        "must assert table uuid"
    );
    assert!(
        requirements.iter().any(|r| matches!(
            r,
            TableRequirement::AssertRefSnapshotId { r#ref, .. } if r#ref == "main"
        )),
        "must assert main ref snapshot"
    );
}

/// Parses the new snapshot's manifest list + manifests from the recorded
/// writes, returning `(list_entry, parsed manifest)` pairs.
fn parse_new_manifests(
    writer: &RecordingStorage,
    plan: &meridian_executor::CompactionPlan,
) -> Vec<(
    meridian_iceberg::manifest::ManifestFile,
    Arc<meridian_iceberg::manifest::Manifest>,
)> {
    // Find the manifest-list write: the add-snapshot's manifest_list location.
    let list_location = plan
        .updates
        .iter()
        .find_map(|u| match u {
            TableUpdate::AddSnapshot { snapshot } => snapshot.manifest_list.clone(),
            _ => None,
        })
        .expect("manifest list location");
    let list_bytes = written_bytes(writer, &list_location);
    let list = read_manifest_list(&list_bytes).expect("parse list");
    list.manifests
        .iter()
        .map(|entry| {
            let mbytes = written_bytes(writer, &entry.manifest_path);
            let manifest = read_manifest(&mbytes).expect("parse manifest");
            (entry.clone(), Arc::new(manifest))
        })
        .collect()
}

/// Asserts the new snapshot's manifests parse back and describe ADDED new
/// files + DELETED old files summing to the expected live row count.
fn assert_manifests_parse_back(
    writer: &RecordingStorage,
    plan: &meridian_executor::CompactionPlan,
    expected_deleted_files: i64,
    expected_live_rows: i64,
) {
    let manifests = parse_new_manifests(writer, plan);
    let mut added = 0i64;
    let mut deleted = 0i64;
    let mut live_rows = 0i64;
    for (_, manifest) in &manifests {
        for entry in &manifest.entries {
            match entry.status {
                ManifestEntryStatus::Added => {
                    added += 1;
                    live_rows += entry.data_file.record_count;
                }
                ManifestEntryStatus::Deleted => deleted += 1,
                ManifestEntryStatus::Existing => live_rows += entry.data_file.record_count,
            }
        }
    }
    assert_eq!(deleted, expected_deleted_files, "old files marked DELETED");
    assert!(added >= 1, "at least one ADDED output file");
    assert_eq!(live_rows, expected_live_rows, "live row count preserved");
}
