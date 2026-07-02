//! Round-trip tests for the v2 table-metadata model.
//!
//! The invariant under test is the crate's design rule: parsing and
//! re-serializing a metadata.json must be lossless, including fields the
//! typed model does not (yet) know about.

use meridian_iceberg::spec::{RefType, SortDirection, TableMetadata};
use serde_json::Value;

const FIXTURE: &str = include_str!("fixtures/table_metadata_v2.json");

#[test]
fn parses_v2_metadata_into_typed_model() {
    let metadata = TableMetadata::from_json(FIXTURE).expect("parse v2 fixture");

    assert_eq!(metadata.format_version, 2);
    assert_eq!(
        metadata.table_uuid.to_string(),
        "9c12d441-03fe-4693-9a96-a0705ddf69c1"
    );
    assert_eq!(metadata.location, "s3://warehouse/analytics/events");
    assert_eq!(metadata.last_sequence_number, Some(34));
    assert_eq!(metadata.last_column_id, 5);

    // Schemas.
    assert_eq!(metadata.schemas.len(), 2);
    let current = metadata.current_schema().expect("current schema resolves");
    assert_eq!(current.schema_id, 1);
    assert_eq!(current.fields.len(), 5);
    assert_eq!(current.identifier_field_ids, Some(vec![1]));
    // Unknown per-field keys (v3 defaults) are preserved, not dropped.
    let region = &current.fields[4];
    assert_eq!(region.name, "region");
    assert_eq!(
        region.extra.get("initial-default"),
        Some(&Value::String("unknown".into()))
    );

    // Partition specs.
    assert_eq!(metadata.partition_specs.len(), 1);
    assert_eq!(
        metadata.partition_specs[0].fields[1].transform,
        "bucket[16]"
    );

    // Sort orders.
    assert_eq!(metadata.sort_orders.len(), 2);
    assert_eq!(
        metadata.sort_orders[1].fields[0].direction,
        SortDirection::Asc
    );

    // Snapshots.
    let snapshot = metadata.current_snapshot().expect("current snapshot");
    assert_eq!(snapshot.snapshot_id, 3_055_729_675_574_597_004);
    assert_eq!(snapshot.sequence_number, Some(34));
    assert_eq!(
        snapshot.summary.as_ref().and_then(|s| s.get("operation")),
        Some(&"append".to_owned())
    );
    // v3 field we don't model yet stays intact on the snapshot.
    assert_eq!(
        snapshot.extra.get("first-row-id"),
        Some(&Value::from(100_000))
    );

    // Refs.
    let refs = metadata.refs.as_ref().expect("refs present");
    assert_eq!(refs["main"].ref_type, RefType::Branch);
    assert_eq!(refs["release-2026-06"].ref_type, RefType::Tag);

    // Unmodelled top-level fields land in extra.
    assert!(metadata.extra.contains_key("statistics"));
    assert!(metadata.extra.contains_key("partition-statistics"));
    assert!(
        metadata
            .extra
            .contains_key("x-meridian-test-unknown-top-level")
    );
}

#[test]
fn round_trip_is_lossless() {
    let metadata = TableMetadata::from_json(FIXTURE).expect("parse v2 fixture");
    let serialized = metadata.to_json().expect("serialize");

    let original: Value = serde_json::from_str(FIXTURE).expect("fixture is valid JSON");
    let round_tripped: Value = serde_json::from_str(&serialized).expect("output is valid JSON");

    // Full structural equality: nothing dropped, nothing added, nothing
    // renamed — including every unknown field.
    assert_eq!(round_tripped, original);
}

#[test]
fn double_round_trip_is_stable() {
    let first = TableMetadata::from_json(FIXTURE).expect("first parse");
    let second =
        TableMetadata::from_json(&first.to_json().expect("serialize")).expect("second parse");
    assert_eq!(first, second);
}

#[test]
fn minimal_v2_metadata_without_snapshots_parses() {
    let minimal = r#"{
        "format-version": 2,
        "table-uuid": "11111111-2222-3333-4444-555555555555",
        "location": "s3://bucket/t",
        "last-sequence-number": 0,
        "last-updated-ms": 1751444210834,
        "last-column-id": 1,
        "current-schema-id": 0,
        "schemas": [
            {"type": "struct", "schema-id": 0, "fields": [
                {"id": 1, "name": "x", "required": true, "type": "long"}
            ]}
        ],
        "default-spec-id": 0,
        "partition-specs": [{"spec-id": 0, "fields": []}],
        "last-partition-id": 999,
        "default-sort-order-id": 0,
        "sort-orders": [{"order-id": 0, "fields": []}],
        "current-snapshot-id": -1
    }"#;

    let metadata = TableMetadata::from_json(minimal).expect("parse minimal metadata");
    assert!(
        metadata.current_snapshot().is_none(),
        "-1 means no snapshot"
    );
    assert!(metadata.extra.is_empty());

    // Optional sections absent on input must stay absent on output.
    let out: Value = serde_json::from_str(&metadata.to_json().expect("serialize")).expect("json");
    let obj = out.as_object().expect("object");
    assert!(!obj.contains_key("snapshots"));
    assert!(!obj.contains_key("refs"));
    assert!(!obj.contains_key("properties"));
}
