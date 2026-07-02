//! Round-trip tests for the table-metadata model across format versions.
//!
//! The invariant under test is the crate's design rule: parsing and
//! re-serializing a metadata.json must be lossless, including fields the
//! typed model does not know about. The one documented exception is v1,
//! which is normalized on read (legacy fields lifted into the modern lists)
//! and re-emits fresh legacy fields on write.

use meridian_iceberg::spec::{
    PrimitiveType, RefType, SortDirection, TableMetadata, Transform, Type,
};
use serde_json::Value;

const FIXTURE_V1: &str = include_str!("fixtures/table_metadata_v1.json");
const FIXTURE_V2: &str = include_str!("fixtures/table_metadata_v2.json");
const FIXTURE_V3: &str = include_str!("fixtures/table_metadata_v3.json");

#[test]
fn parses_v2_metadata_into_typed_model() {
    let metadata = TableMetadata::from_json(FIXTURE_V2).expect("parse v2 fixture");

    assert_eq!(metadata.format_version, 2);
    assert_eq!(
        metadata.table_uuid.to_string(),
        "9c12d441-03fe-4693-9a96-a0705ddf69c1"
    );
    assert_eq!(metadata.location, "s3://warehouse/analytics/events");
    assert_eq!(metadata.last_sequence_number, Some(34));
    assert_eq!(metadata.last_column_id, 5);

    // Schemas, with the typed type tree.
    assert_eq!(metadata.schemas.len(), 2);
    let current = metadata.current_schema().expect("current schema resolves");
    assert_eq!(current.schema_id, Some(1));
    assert_eq!(current.fields.len(), 5);
    assert_eq!(current.identifier_field_ids, Some(vec![1]));
    assert_eq!(
        current.fields[0].field_type,
        Type::Primitive(PrimitiveType::Long)
    );
    assert_eq!(
        current.fields[1].field_type,
        Type::Primitive(PrimitiveType::Timestamptz)
    );
    let tags = &current.fields[3];
    match &tags.field_type {
        Type::List(list) => {
            assert_eq!(list.element_id, 5);
            assert_eq!(*list.element, Type::Primitive(PrimitiveType::String));
            assert!(!list.element_required);
        }
        other => panic!("tags must be a list type, got {other:?}"),
    }
    // v3-style per-field defaults are typed, not shoved into extra.
    let region = &current.fields[4];
    assert_eq!(region.name, "region");
    assert_eq!(
        region.initial_default,
        Some(Value::String("unknown".into()))
    );
    assert_eq!(region.write_default, Some(Value::String("unknown".into())));

    // Partition specs with typed transforms.
    assert_eq!(metadata.partition_specs.len(), 1);
    let spec = metadata.default_partition_spec().expect("default spec");
    assert_eq!(spec.spec_id, Some(0));
    assert_eq!(spec.fields[0].transform, Transform::Day);
    assert_eq!(spec.fields[1].transform, Transform::Bucket(16));
    assert_eq!(spec.fields[1].field_id, Some(1001));

    // Sort orders.
    assert_eq!(metadata.sort_orders.len(), 2);
    let order = metadata.default_sort_order().expect("default sort order");
    assert_eq!(order.fields[0].direction, SortDirection::Asc);
    assert_eq!(order.fields[0].transform, Transform::Identity);
    assert_eq!(order.fields[1].transform, Transform::Bucket(16));

    // Snapshots.
    let snapshot = metadata.current_snapshot().expect("current snapshot");
    assert_eq!(snapshot.snapshot_id, 3_055_729_675_574_597_004);
    assert_eq!(snapshot.sequence_number, Some(34));
    assert_eq!(
        snapshot.summary.as_ref().and_then(|s| s.get("operation")),
        Some(&"append".to_owned())
    );
    // Row lineage is typed on the snapshot.
    assert_eq!(snapshot.first_row_id, Some(100_000));

    // Refs.
    let refs = metadata.refs.as_ref().expect("refs present");
    assert_eq!(refs["main"].ref_type, RefType::Branch);
    assert_eq!(refs["release-2026-06"].ref_type, RefType::Tag);

    // Statistics are typed now.
    let statistics = metadata.statistics.as_ref().expect("statistics present");
    assert_eq!(statistics[0].snapshot_id, 3_055_729_675_574_597_004);
    assert_eq!(
        statistics[0].blob_metadata[0].blob_type,
        "apache-datasketches-theta-v1"
    );
    let partition_statistics = metadata
        .partition_statistics
        .as_ref()
        .expect("partition statistics present");
    assert_eq!(partition_statistics[0].file_size_in_bytes, 8123);

    // Unmodelled top-level fields still land in extra.
    assert!(
        metadata
            .extra
            .contains_key("x-meridian-test-unknown-top-level")
    );
}

#[test]
fn v2_round_trip_is_lossless() {
    let metadata = TableMetadata::from_json(FIXTURE_V2).expect("parse v2 fixture");
    let serialized = metadata.to_json().expect("serialize");

    let original: Value = serde_json::from_str(FIXTURE_V2).expect("fixture is valid JSON");
    let round_tripped: Value = serde_json::from_str(&serialized).expect("output is valid JSON");

    // Full structural equality: nothing dropped, nothing added, nothing
    // renamed — including every unknown field.
    assert_eq!(round_tripped, original);
}

#[test]
fn v3_round_trip_is_lossless() {
    let metadata = TableMetadata::from_json(FIXTURE_V3).expect("parse v3 fixture");
    let serialized = metadata.to_json().expect("serialize");

    let original: Value = serde_json::from_str(FIXTURE_V3).expect("fixture is valid JSON");
    let round_tripped: Value = serde_json::from_str(&serialized).expect("output is valid JSON");
    assert_eq!(round_tripped, original);
}

#[test]
fn parses_v3_metadata_into_typed_model() {
    let metadata = TableMetadata::from_json(FIXTURE_V3).expect("parse v3 fixture");

    assert_eq!(metadata.format_version, 3);
    assert_eq!(metadata.next_row_id, Some(150_000));

    let schema = metadata.current_schema().expect("current schema");
    assert_eq!(
        schema.fields[1].field_type,
        Type::Primitive(PrimitiveType::TimestamptzNs)
    );
    assert_eq!(
        schema.fields[2].field_type,
        Type::Primitive(PrimitiveType::Geometry {
            crs: Some("OGC:CRS84".to_owned())
        })
    );
    assert_eq!(
        schema.fields[3].field_type,
        Type::Primitive(PrimitiveType::Geography {
            crs: Some("srid:4326".to_owned()),
            algorithm: Some("spherical".to_owned())
        })
    );
    assert_eq!(
        schema.fields[4].field_type,
        Type::Primitive(PrimitiveType::Variant)
    );
    match &schema.fields[6].field_type {
        Type::Map(map) => {
            assert_eq!(*map.value, Type::Primitive(PrimitiveType::Unknown));
        }
        other => panic!("attributes must be a map type, got {other:?}"),
    }

    // Row lineage on snapshots.
    let current = metadata.current_snapshot().expect("current snapshot");
    assert_eq!(current.first_row_id, Some(100_000));
    assert_eq!(current.added_rows, Some(50_000));

    // Encryption keys are typed.
    let keys = metadata
        .encryption_keys
        .as_ref()
        .expect("encryption keys present");
    assert_eq!(keys[0].key_id, "key-2026-06");
    assert_eq!(keys[0].encrypted_by_id.as_deref(), Some("kek-1"));
}

#[test]
fn v1_metadata_is_normalized_on_read() {
    let metadata = TableMetadata::from_json(FIXTURE_V1).expect("parse v1 fixture");

    assert_eq!(metadata.format_version, 1);
    // The legacy single schema becomes schemas[0] with schema-id 0.
    assert_eq!(metadata.schemas.len(), 1);
    assert_eq!(metadata.schemas[0].schema_id, Some(0));
    assert_eq!(metadata.current_schema_id, 0);
    let schema = metadata.current_schema().expect("current schema resolves");
    assert_eq!(
        schema.fields[2].field_type,
        Type::Primitive(PrimitiveType::Decimal {
            precision: 10,
            scale: 2
        })
    );

    // The legacy partition-spec becomes spec 0 with field ids assigned from
    // 1000.
    assert_eq!(metadata.partition_specs.len(), 1);
    let spec = metadata.default_partition_spec().expect("default spec");
    assert_eq!(spec.spec_id, Some(0));
    assert_eq!(spec.fields[0].field_id, Some(1000));
    assert_eq!(spec.fields[0].transform, Transform::Day);
    assert_eq!(metadata.last_partition_id, 1000);

    // Sort orders default to the unsorted order.
    assert_eq!(metadata.sort_orders.len(), 1);
    assert_eq!(metadata.sort_orders[0].order_id, 0);
    assert!(metadata.sort_orders[0].fields.is_empty());
    assert_eq!(metadata.default_sort_order_id, 0);

    // v1 snapshots have no sequence numbers and may carry inline manifests
    // (preserved via extra).
    assert_eq!(metadata.last_sequence_number, None);
    let snapshot = metadata.current_snapshot().expect("current snapshot");
    assert_eq!(snapshot.sequence_number, None);
    assert!(snapshot.manifest_list.is_none());
    assert!(snapshot.extra.contains_key("manifests"));
}

#[test]
fn v1_serialization_reemits_legacy_fields() {
    let metadata = TableMetadata::from_json(FIXTURE_V1).expect("parse v1 fixture");
    let out: Value =
        serde_json::from_str(&metadata.to_json().expect("serialize")).expect("valid JSON");
    let root = out.as_object().expect("object");

    // Modern lists are present...
    assert!(root.contains_key("schemas"));
    assert!(root.contains_key("partition-specs"));
    // ...and the v1-required legacy fields are re-derived alongside them.
    assert_eq!(root["schema"]["schema-id"], Value::from(0));
    assert_eq!(
        root["partition-spec"][0]["name"],
        Value::from("order_date_day")
    );
    assert_eq!(root["partition-spec"][0]["field-id"], Value::from(1000));

    // Reading our own output reproduces the same model.
    let reparsed =
        TableMetadata::from_json(&metadata.to_json().expect("serialize")).expect("reparse");
    assert_eq!(reparsed, metadata);
}

#[test]
fn double_round_trip_is_stable() {
    for fixture in [FIXTURE_V1, FIXTURE_V2, FIXTURE_V3] {
        let first = TableMetadata::from_json(fixture).expect("first parse");
        let second =
            TableMetadata::from_json(&first.to_json().expect("serialize")).expect("second parse");
        assert_eq!(first, second);
    }
}

#[test]
fn unsupported_format_versions_are_rejected() {
    let bad = r#"{"format-version": 4, "table-uuid": "11111111-2222-3333-4444-555555555555"}"#;
    let error = TableMetadata::from_json(bad).expect_err("v4 must be rejected");
    assert!(error.to_string().contains("format-version"), "{error}");
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
    assert!(!obj.contains_key("statistics"));
    assert!(!obj.contains_key("schema"), "no legacy fields on v2");
}
