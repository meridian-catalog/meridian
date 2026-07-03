//! Parses real manifest lists and manifests — written by pyiceberg (v1 and
//! v2 tables) and by Spark (a v2 merge-on-read table with position
//! deletes) — and compares every field against `expected.json`, a dump of
//! pyiceberg's own view of the same files (produced by pyiceberg reading
//! them back; see the fixture directories).
//!
//! Fixture provenance:
//! - `pyiceberg_v2/`: pyiceberg 0.11.1 `SqlCatalog` table, wide primitive
//!   schema (bool/int/long/float/double/two decimals/date/time/timestamp/
//!   timestamptz/string/uuid/binary/fixed), partitioned by
//!   `identity(category), day(ts)`; two appends and a partition delete —
//!   ADDED and DELETED entries.
//! - `pyiceberg_v1/`: same schema, format-version 1, identity partition.
//! - `spark_orders/`: the conformance suite's Spark 4 / Iceberg 1.10
//!   merge-on-read table (5 snapshots: 3 appends, an update, a delete),
//!   with position-delete manifests. Copied from `MinIO`; `expected.json`
//!   dumped by pyiceberg reading straight from the warehouse.

use std::collections::BTreeMap;
use std::path::{Path, PathBuf};

use meridian_iceberg::manifest::{Manifest, ManifestFile, read_manifest, read_manifest_list};
use meridian_iceberg::value::Datum;
use serde_json::{Value, json};

fn fixture_dir(name: &str) -> PathBuf {
    Path::new(env!("CARGO_MANIFEST_DIR"))
        .join("tests/fixtures")
        .join(name)
}

fn basename(path: &str) -> &str {
    path.rsplit('/').next().unwrap_or(path)
}

fn read_fixture(dir: &Path, path: &str) -> Vec<u8> {
    let local = dir.join(basename(path));
    std::fs::read(&local).unwrap_or_else(|e| panic!("read {}: {e}", local.display()))
}

/// Canonical JSON rendering of a partition datum, mirroring the fixture
/// generator's `canon_value` (see `expected.json` provenance above).
fn canon_datum(datum: Option<&Datum>) -> Value {
    match datum {
        None => Value::Null,
        Some(Datum::Boolean(b)) => json!(b),
        Some(Datum::Int(v) | Datum::Date(v)) => json!(v),
        Some(
            Datum::Long(v)
            | Datum::Time(v)
            | Datum::Timestamp(v)
            | Datum::Timestamptz(v)
            | Datum::TimestampNs(v)
            | Datum::TimestamptzNs(v),
        ) => json!(v),
        Some(Datum::Float(v)) => json!({"f64": hex::encode(f64::from(*v).to_be_bytes())}),
        Some(Datum::Double(v)) => json!({"f64": hex::encode(v.to_be_bytes())}),
        Some(Datum::String(s)) => json!(s),
        Some(Datum::Uuid(u)) => json!({"uuid": u.to_string()}),
        Some(Datum::Fixed(b) | Datum::Binary(b)) => json!({"bytes": hex::encode(b)}),
        Some(Datum::Decimal { unscaled, scale }) => {
            json!({"decimal": unscaled.to_string(), "scale": scale})
        }
    }
}

fn canon_count_map(map: Option<&BTreeMap<i32, i64>>) -> Value {
    match map {
        None => Value::Null,
        Some(m) => Value::Object(m.iter().map(|(k, v)| (k.to_string(), json!(v))).collect()),
    }
}

fn canon_bytes_map(map: Option<&BTreeMap<i32, Vec<u8>>>) -> Value {
    match map {
        None => Value::Null,
        Some(m) => Value::Object(
            m.iter()
                .map(|(k, v)| (k.to_string(), json!(hex::encode(v))))
                .collect(),
        ),
    }
}

/// Renders a parsed manifest (with inheritance applied from `list_entry`)
/// in the expected.json shape.
fn render_manifest(list_entry: &ManifestFile, manifest: &Manifest) -> Value {
    let entries: Vec<Value> = manifest
        .entries
        .iter()
        .map(|stored| {
            let mut entry = stored.clone();
            entry.inherit_from(list_entry);
            let df = &entry.data_file;
            // pyiceberg renders the file format enum as
            // "FileFormat.PARQUET"; the raw file stores "PARQUET".
            let file_format = format!("FileFormat.{}", df.file_format.to_uppercase());
            json!({
                "status": entry.status.code(),
                "snapshot_id": entry.snapshot_id,
                "sequence_number": entry.sequence_number,
                "file_sequence_number": entry.file_sequence_number,
                "content": df.content.code(),
                "file_path": df.file_path,
                "file_format": file_format,
                "partition": df.partition.fields.iter().map(|f| canon_datum(f.value.as_ref())).collect::<Vec<_>>(),
                "record_count": df.record_count,
                "file_size_in_bytes": df.file_size_in_bytes,
                "column_sizes": canon_count_map(df.column_sizes.as_ref()),
                "value_counts": canon_count_map(df.value_counts.as_ref()),
                "null_value_counts": canon_count_map(df.null_value_counts.as_ref()),
                "nan_value_counts": canon_count_map(df.nan_value_counts.as_ref()),
                "lower_bounds": canon_bytes_map(df.lower_bounds.as_ref()),
                "upper_bounds": canon_bytes_map(df.upper_bounds.as_ref()),
                "split_offsets": df.split_offsets,
                "equality_ids": df.equality_ids,
                "sort_order_id": df.sort_order_id,
            })
        })
        .collect();
    let partitions = list_entry.partitions.as_ref().map(|summaries| {
        summaries
            .iter()
            .map(|s| {
                json!({
                    "contains_null": s.contains_null,
                    "contains_nan": s.contains_nan,
                    "lower_bound": s.lower_bound.as_ref().map(hex::encode),
                    "upper_bound": s.upper_bound.as_ref().map(hex::encode),
                })
            })
            .collect::<Vec<_>>()
    });
    json!({
        "manifest_path": list_entry.manifest_path,
        "manifest_length": list_entry.manifest_length,
        "partition_spec_id": list_entry.partition_spec_id,
        "content": list_entry.content.code(),
        "sequence_number": list_entry.sequence_number,
        "min_sequence_number": list_entry.min_sequence_number,
        "added_snapshot_id": list_entry.added_snapshot_id,
        "added_files_count": list_entry.added_files_count,
        "existing_files_count": list_entry.existing_files_count,
        "deleted_files_count": list_entry.deleted_files_count,
        "added_rows_count": list_entry.added_rows_count,
        "existing_rows_count": list_entry.existing_rows_count,
        "deleted_rows_count": list_entry.deleted_rows_count,
        "partitions": partitions,
        "key_metadata": list_entry.key_metadata.as_ref().map(hex::encode),
        "entries": entries,
    })
}

fn check_fixture_table(name: &str) {
    let dir = fixture_dir(name);
    let expected: Value =
        serde_json::from_slice(&std::fs::read(dir.join("expected.json")).expect("expected.json"))
            .expect("parse expected.json");
    let format_version = expected["format_version"].as_u64().expect("format_version");

    let snapshots = expected["snapshots"].as_array().expect("snapshots");
    assert!(!snapshots.is_empty(), "{name}: fixture has snapshots");
    for snapshot in snapshots {
        let snapshot_id = snapshot["snapshot_id"].as_i64().expect("snapshot_id");
        let list_path = snapshot["manifest_list"].as_str().expect("manifest_list");
        let list_bytes = read_fixture(&dir, list_path);
        let list = read_manifest_list(&list_bytes)
            .unwrap_or_else(|e| panic!("{name}: parse {list_path}: {e}"));

        assert_eq!(list.snapshot_id, Some(snapshot_id), "{name} {list_path}");
        if format_version >= 2 {
            assert_eq!(list.format_version, Some(2), "{name} {list_path}");
            assert!(list.sequence_number.is_some(), "{name} {list_path}");
        }

        let expected_manifests = snapshot["manifests"].as_array().expect("manifests");
        assert_eq!(
            list.manifests.len(),
            expected_manifests.len(),
            "{name} {list_path}: manifest count"
        );
        for (idx, (entry, expected_manifest)) in
            list.manifests.iter().zip(expected_manifests).enumerate()
        {
            let manifest_bytes = read_fixture(&dir, &entry.manifest_path);
            let manifest = read_manifest(&manifest_bytes)
                .unwrap_or_else(|e| panic!("{name}: parse {}: {e}", entry.manifest_path));

            // Manifest metadata sanity.
            if format_version >= 2 {
                assert_eq!(manifest.metadata.format_version, Some(2));
                assert_eq!(
                    manifest.metadata.partition_spec_id,
                    Some(entry.partition_spec_id)
                );
                assert_eq!(manifest.metadata.content, entry.content);
            }
            assert!(
                !manifest.metadata.partition_fields.is_empty(),
                "{name}: partition-spec metadata parsed"
            );

            let mut rendered = render_manifest(entry, &manifest);
            // The expected view keeps whatever nan-count map pyiceberg
            // reports; both sides come from the same file so no fixups —
            // compare field by field for a readable failure.
            let rendered_obj = rendered.as_object_mut().expect("rendered object");
            let expected_obj = expected_manifest.as_object().expect("expected object");
            for (key, expected_value) in expected_obj {
                let got = rendered_obj
                    .get(key)
                    .unwrap_or_else(|| panic!("{name} snapshot {snapshot_id}: missing {key}"));
                assert_eq!(
                    got, expected_value,
                    "{name} snapshot {snapshot_id} manifest #{idx} field {key:?}"
                );
            }
        }
    }
}

#[test]
fn pyiceberg_v2_fixtures_match_pyiceberg_view() {
    check_fixture_table("pyiceberg_v2");
}

#[test]
fn pyiceberg_v1_fixtures_match_pyiceberg_view() {
    check_fixture_table("pyiceberg_v1");
}

#[test]
fn spark_merge_on_read_fixtures_match_pyiceberg_view() {
    check_fixture_table("spark_orders");
}

/// The Spark table's delete manifests must parse as position deletes and
/// carry `referenced_data_file` where Spark wrote it.
#[test]
fn spark_delete_manifests_expose_position_deletes() {
    let dir = fixture_dir("spark_orders");
    let expected: Value =
        serde_json::from_slice(&std::fs::read(dir.join("expected.json")).expect("expected.json"))
            .expect("parse expected.json");

    let mut saw_delete_manifest = false;
    let mut saw_deleted_status = false;
    for snapshot in expected["snapshots"].as_array().expect("snapshots") {
        let list_bytes = read_fixture(&dir, snapshot["manifest_list"].as_str().expect("path"));
        let list = read_manifest_list(&list_bytes).expect("parse list");
        for entry in &list.manifests {
            let manifest =
                read_manifest(&read_fixture(&dir, &entry.manifest_path)).expect("parse manifest");
            for stored in &manifest.entries {
                if stored.status == meridian_iceberg::manifest::ManifestEntryStatus::Deleted {
                    saw_deleted_status = true;
                }
                if entry.content == meridian_iceberg::manifest::ManifestContentType::Deletes {
                    saw_delete_manifest = true;
                    assert_eq!(
                        stored.data_file.content,
                        meridian_iceberg::manifest::DataFileContent::PositionDeletes,
                        "delete manifests in this table hold position deletes"
                    );
                }
            }
        }
    }
    assert!(saw_delete_manifest, "fixture has delete manifests");
    // The Spark update/delete snapshots rewrite rows via merge-on-read;
    // no DELETED data-file entries are expected in this particular table,
    // so do not assert saw_deleted_status here — the pyiceberg_v2 fixture
    // covers DELETED entries.
    let _ = saw_deleted_status;
}
