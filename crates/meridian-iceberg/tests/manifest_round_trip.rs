//! Write manifests + manifest lists, read them back, and require the
//! parsed model to be identical — across format versions, partition
//! transforms, and every optional field. Also exercises the writer's
//! honest refusals (v1 deletes, v3-only fields).

use std::collections::BTreeMap;

use meridian_iceberg::manifest::{
    DataFile, DataFileContent, FieldSummary, ManifestContentType, ManifestEntry,
    ManifestEntryStatus, ManifestFile, ManifestListWriteParams, ManifestWriteParams,
    PartitionTuple, PartitionValue, partition_field_types, read_manifest, read_manifest_list,
    write_manifest, write_manifest_list,
};
use meridian_iceberg::spec::{
    PartitionField, PartitionSpec, PrimitiveType, Schema, StructField, Transform, Type,
};
use meridian_iceberg::value::Datum;
use uuid::Uuid;

fn wide_schema() -> Schema {
    Schema::new(vec![
        StructField::required(1, "id", Type::Primitive(PrimitiveType::Long)),
        StructField::optional(2, "category", Type::Primitive(PrimitiveType::String)),
        StructField::optional(3, "ts", Type::Primitive(PrimitiveType::Timestamp)),
        StructField::optional(4, "tstz", Type::Primitive(PrimitiveType::Timestamptz)),
        StructField::optional(
            5,
            "price",
            Type::Primitive(PrimitiveType::Decimal {
                precision: 9,
                scale: 2,
            }),
        ),
        StructField::optional(6, "uid", Type::Primitive(PrimitiveType::Uuid)),
        StructField::optional(7, "day", Type::Primitive(PrimitiveType::Date)),
        StructField::optional(8, "blob", Type::Primitive(PrimitiveType::Binary)),
        StructField::optional(9, "ratio", Type::Primitive(PrimitiveType::Double)),
    ])
    .with_schema_id(0)
}

fn spec_field(field_id: i32, source_id: i32, name: &str, transform: Transform) -> PartitionField {
    let mut field = PartitionField::new(source_id, name, transform);
    field.field_id = Some(field_id);
    field
}

/// A partition spec exercising every projectable transform family plus
/// uuid identity (the fixed(16) write path).
fn wide_spec() -> PartitionSpec {
    let mut spec = PartitionSpec::new(vec![
        spec_field(1000, 2, "category", Transform::Identity),
        spec_field(1001, 1, "id_bucket", Transform::Bucket(8)),
        spec_field(1002, 3, "ts_day", Transform::Day),
        spec_field(1003, 2, "cat_trunc", Transform::Truncate(3)),
        spec_field(1004, 5, "price", Transform::Identity),
        spec_field(1005, 6, "uid", Transform::Identity),
        spec_field(1006, 4, "tstz_hour", Transform::Hour),
    ]);
    spec.spec_id = Some(7);
    spec
}

fn tuple(values: Vec<(i32, &str, Option<Datum>)>) -> PartitionTuple {
    PartitionTuple {
        fields: values
            .into_iter()
            .map(|(field_id, name, value)| PartitionValue {
                field_id,
                name: name.to_owned(),
                value,
            })
            .collect(),
    }
}

fn full_tuple() -> PartitionTuple {
    tuple(vec![
        (1000, "category", Some(Datum::String("toys".into()))),
        (1001, "id_bucket", Some(Datum::Int(3))),
        (1002, "ts_day", Some(Datum::Date(20468))),
        (1003, "cat_trunc", Some(Datum::String("toy".into()))),
        (
            1004,
            "price",
            Some(Datum::Decimal {
                unscaled: -1420,
                scale: 2,
            }),
        ),
        (
            1005,
            "uid",
            Some(Datum::Uuid(
                Uuid::parse_str("f79c3e09-677c-4bbd-a479-3f349cb785e7").expect("uuid"),
            )),
        ),
        (1006, "tstz_hour", Some(Datum::Int(491_246))),
    ])
}

fn null_tuple() -> PartitionTuple {
    tuple(vec![
        (1000, "category", None),
        (1001, "id_bucket", None),
        (1002, "ts_day", None),
        (1003, "cat_trunc", None),
        (1004, "price", None),
        (1005, "uid", None),
        (1006, "tstz_hour", None),
    ])
}

fn data_file(path: &str, partition: PartitionTuple) -> DataFile {
    DataFile {
        content: DataFileContent::Data,
        file_path: path.to_owned(),
        file_format: "PARQUET".to_owned(),
        partition,
        record_count: 42,
        file_size_in_bytes: 4096,
        column_sizes: Some(BTreeMap::from([(1, 512), (2, 256)])),
        value_counts: Some(BTreeMap::from([(1, 42), (2, 42)])),
        null_value_counts: Some(BTreeMap::from([(1, 0), (2, 7)])),
        nan_value_counts: Some(BTreeMap::from([(9, 3)])),
        lower_bounds: Some(BTreeMap::from([
            (1, Datum::Long(-5).to_bound_bytes()),
            (2, Datum::String("alpha".into()).to_bound_bytes()),
        ])),
        upper_bounds: Some(BTreeMap::from([
            (1, Datum::Long(99_999).to_bound_bytes()),
            (2, Datum::String("zulu".into()).to_bound_bytes()),
        ])),
        key_metadata: Some(vec![0xDE, 0xAD]),
        split_offsets: Some(vec![4, 2048]),
        equality_ids: None,
        sort_order_id: Some(1),
        first_row_id: None,
        referenced_data_file: None,
        content_offset: None,
        content_size_in_bytes: None,
    }
}

#[test]
fn v2_manifest_round_trips_identically() {
    let schema = wide_schema();
    let spec = wide_spec();
    let types = partition_field_types(&spec.fields, &schema).expect("types");
    let schema_json = serde_json::to_string(&schema).expect("schema json");

    let entries = vec![
        ManifestEntry {
            status: ManifestEntryStatus::Added,
            snapshot_id: Some(77),
            sequence_number: None, // inherited
            file_sequence_number: None,
            data_file: data_file("s3://bucket/wh/data/a.parquet", full_tuple()),
        },
        ManifestEntry {
            status: ManifestEntryStatus::Existing,
            snapshot_id: Some(76),
            sequence_number: Some(3),
            file_sequence_number: Some(3),
            data_file: data_file("s3://bucket/wh/data/b.parquet", null_tuple()),
        },
        ManifestEntry {
            status: ManifestEntryStatus::Deleted,
            snapshot_id: Some(77),
            sequence_number: Some(2),
            file_sequence_number: Some(2),
            data_file: data_file("s3://bucket/wh/data/c.parquet", full_tuple()),
        },
    ];

    let bytes = write_manifest(&ManifestWriteParams {
        format_version: 2,
        content: ManifestContentType::Data,
        schema_json: &schema_json,
        schema_id: Some(0),
        partition_spec_id: 7,
        partition_fields: &spec.fields,
        partition_types: &types,
        entries: &entries,
    })
    .expect("write manifest");

    let manifest = read_manifest(&bytes).expect("read back");
    assert_eq!(manifest.metadata.format_version, Some(2));
    assert_eq!(manifest.metadata.schema_id, Some(0));
    assert_eq!(manifest.metadata.partition_spec_id, Some(7));
    assert_eq!(manifest.metadata.content, ManifestContentType::Data);
    assert_eq!(manifest.metadata.partition_fields, spec.fields);
    assert_eq!(manifest.entries, entries, "entries must round-trip exactly");

    // The uuid partition value must come back as a typed uuid (via the
    // schema metadata), not raw fixed bytes.
    assert!(matches!(
        manifest.entries[0].data_file.partition.get(1005),
        Some(Some(Datum::Uuid(_)))
    ));
}

#[test]
fn v2_delete_manifest_round_trips() {
    let schema = wide_schema();
    let spec = PartitionSpec::unpartitioned(0);
    let types = partition_field_types(&spec.fields, &schema).expect("types");
    let schema_json = serde_json::to_string(&schema).expect("schema json");

    let mut df = data_file("s3://bucket/wh/data/pd.parquet", PartitionTuple::default());
    df.content = DataFileContent::PositionDeletes;
    df.referenced_data_file = Some("s3://bucket/wh/data/a.parquet".to_owned());
    let entries = vec![ManifestEntry {
        status: ManifestEntryStatus::Added,
        snapshot_id: None,
        sequence_number: None,
        file_sequence_number: None,
        data_file: df,
    }];

    let bytes = write_manifest(&ManifestWriteParams {
        format_version: 2,
        content: ManifestContentType::Deletes,
        schema_json: &schema_json,
        schema_id: Some(0),
        partition_spec_id: 0,
        partition_fields: &spec.fields,
        partition_types: &types,
        entries: &entries,
    })
    .expect("write delete manifest");

    let manifest = read_manifest(&bytes).expect("read back");
    assert_eq!(manifest.metadata.content, ManifestContentType::Deletes);
    assert_eq!(manifest.entries, entries);
    assert_eq!(
        manifest.entries[0]
            .data_file
            .referenced_data_file
            .as_deref(),
        Some("s3://bucket/wh/data/a.parquet")
    );
}

#[test]
fn v1_manifest_round_trips_with_required_snapshot_id() {
    let schema = wide_schema();
    let spec = PartitionSpec::new(vec![spec_field(1000, 2, "category", Transform::Identity)]);
    let types = partition_field_types(&spec.fields, &schema).expect("types");
    let schema_json = serde_json::to_string(&schema).expect("schema json");

    let entries = vec![ManifestEntry {
        status: ManifestEntryStatus::Added,
        snapshot_id: Some(11),
        sequence_number: None,
        file_sequence_number: None,
        data_file: data_file(
            "file:///wh/data/a.parquet",
            tuple(vec![(1000, "category", Some(Datum::String("toys".into())))]),
        ),
    }];

    let params = ManifestWriteParams {
        format_version: 1,
        content: ManifestContentType::Data,
        schema_json: &schema_json,
        schema_id: Some(0),
        partition_spec_id: 0,
        partition_fields: &spec.fields,
        partition_types: &types,
        entries: &entries,
    };
    let bytes = write_manifest(&params).expect("write v1 manifest");
    let manifest = read_manifest(&bytes).expect("read back");
    assert_eq!(manifest.metadata.format_version, Some(1));
    assert_eq!(manifest.entries, entries);

    // v1 refuses entries without a snapshot id and delete content.
    let mut missing = entries.clone();
    missing[0].snapshot_id = None;
    let refused = write_manifest(&ManifestWriteParams {
        entries: &missing,
        ..params
    });
    assert!(refused.is_err(), "v1 requires snapshot_id");

    let refused = write_manifest(&ManifestWriteParams {
        content: ManifestContentType::Deletes,
        ..params
    });
    assert!(refused.is_err(), "v1 cannot hold delete manifests");
}

fn manifest_file_entry(v2: bool) -> ManifestFile {
    ManifestFile {
        manifest_path: "s3://bucket/wh/metadata/m0.avro".to_owned(),
        manifest_length: 6021,
        partition_spec_id: 7,
        content: ManifestContentType::Data,
        sequence_number: if v2 { 9 } else { 0 },
        min_sequence_number: if v2 { 4 } else { 0 },
        added_snapshot_id: 77,
        added_files_count: Some(2),
        existing_files_count: Some(1),
        deleted_files_count: Some(0),
        added_rows_count: Some(84),
        existing_rows_count: Some(42),
        deleted_rows_count: Some(0),
        partitions: Some(vec![
            FieldSummary {
                contains_null: false,
                contains_nan: Some(false),
                lower_bound: Some(Datum::String("alpha".into()).to_bound_bytes()),
                upper_bound: Some(Datum::String("zulu".into()).to_bound_bytes()),
            },
            FieldSummary {
                contains_null: true,
                contains_nan: None,
                lower_bound: None,
                upper_bound: None,
            },
        ]),
        key_metadata: None,
        first_row_id: None,
    }
}

#[test]
fn manifest_lists_round_trip_v1_and_v2() {
    // v2.
    let manifests = vec![manifest_file_entry(true)];
    let bytes = write_manifest_list(&ManifestListWriteParams {
        format_version: 2,
        snapshot_id: 77,
        parent_snapshot_id: Some(76),
        sequence_number: Some(9),
        manifests: &manifests,
    })
    .expect("write v2 list");
    let list = read_manifest_list(&bytes).expect("read v2 list");
    assert_eq!(list.format_version, Some(2));
    assert_eq!(list.snapshot_id, Some(77));
    assert_eq!(list.parent_snapshot_id, Some(76));
    assert_eq!(list.sequence_number, Some(9));
    assert_eq!(list.manifests, manifests);

    // v2 requires a sequence number and full counts.
    assert!(
        write_manifest_list(&ManifestListWriteParams {
            format_version: 2,
            snapshot_id: 77,
            parent_snapshot_id: None,
            sequence_number: None,
            manifests: &manifests,
        })
        .is_err()
    );
    let mut incomplete = manifests.clone();
    incomplete[0].added_files_count = None;
    assert!(
        write_manifest_list(&ManifestListWriteParams {
            format_version: 2,
            snapshot_id: 77,
            parent_snapshot_id: None,
            sequence_number: Some(9),
            manifests: &incomplete,
        })
        .is_err()
    );

    // v1: historical field names on disk, same model in memory. A first
    // snapshot has no parent; the metadata stores the literal "null".
    let manifests = vec![manifest_file_entry(false)];
    let bytes = write_manifest_list(&ManifestListWriteParams {
        format_version: 1,
        snapshot_id: 11,
        parent_snapshot_id: None,
        sequence_number: None,
        manifests: &manifests,
    })
    .expect("write v1 list");
    let list = read_manifest_list(&bytes).expect("read v1 list");
    assert_eq!(list.format_version, Some(1));
    assert_eq!(list.snapshot_id, Some(11));
    assert_eq!(
        list.parent_snapshot_id, None,
        "the literal \"null\" parses as absent"
    );
    assert_eq!(list.manifests, manifests);

    // The v1 file must use the historical count field names.
    let raw = String::from_utf8_lossy(&bytes);
    assert!(raw.contains("added_data_files_count"));
}

#[test]
fn v3_only_fields_are_refused_not_dropped() {
    let schema = wide_schema();
    let spec = PartitionSpec::unpartitioned(0);
    let types = partition_field_types(&spec.fields, &schema).expect("types");
    let schema_json = serde_json::to_string(&schema).expect("schema json");

    let mut df = data_file("s3://b/dv.puffin", PartitionTuple::default());
    df.content = DataFileContent::PositionDeletes;
    df.content_offset = Some(4);
    df.content_size_in_bytes = Some(128);
    let entries = vec![ManifestEntry {
        status: ManifestEntryStatus::Added,
        snapshot_id: Some(1),
        sequence_number: None,
        file_sequence_number: None,
        data_file: df,
    }];
    let err = write_manifest(&ManifestWriteParams {
        format_version: 2,
        content: ManifestContentType::Deletes,
        schema_json: &schema_json,
        schema_id: Some(0),
        partition_spec_id: 0,
        partition_fields: &spec.fields,
        partition_types: &types,
        entries: &entries,
    })
    .expect_err("deletion-vector fields are v3-only");
    assert!(err.to_string().contains("v3"), "honest error: {err}");

    let mut with_row_id = manifest_file_entry(true);
    with_row_id.first_row_id = Some(100);
    let err = write_manifest_list(&ManifestListWriteParams {
        format_version: 2,
        snapshot_id: 1,
        parent_snapshot_id: None,
        sequence_number: Some(1),
        manifests: &[with_row_id],
    })
    .expect_err("first_row_id is v3-only");
    assert!(err.to_string().contains("v3"), "honest error: {err}");
}

/// Sequence-number inheritance across the list -> manifest boundary, per
/// the spec (v2: inherit only for ADDED; v1: always, as 0).
#[test]
fn inheritance_rules_match_spec() {
    let v2_list_entry = manifest_file_entry(true);
    let mut added = ManifestEntry {
        status: ManifestEntryStatus::Added,
        snapshot_id: None,
        sequence_number: None,
        file_sequence_number: None,
        data_file: data_file("f", PartitionTuple::default()),
    };
    added.inherit_from(&v2_list_entry);
    assert_eq!(added.snapshot_id, Some(77));
    assert_eq!(added.sequence_number, Some(9));
    assert_eq!(added.file_sequence_number, Some(9));

    let mut existing = ManifestEntry {
        status: ManifestEntryStatus::Existing,
        snapshot_id: Some(5),
        sequence_number: Some(2),
        file_sequence_number: Some(2),
        data_file: data_file("f", PartitionTuple::default()),
    };
    existing.inherit_from(&v2_list_entry);
    assert_eq!(existing.sequence_number, Some(2), "explicit values stay");

    // An EXISTING entry with a null sequence number in a v2 manifest is
    // NOT inherited (the spec requires explicit values; leave it null
    // rather than invent one).
    let mut malformed = ManifestEntry {
        status: ManifestEntryStatus::Existing,
        snapshot_id: None,
        sequence_number: None,
        file_sequence_number: None,
        data_file: data_file("f", PartitionTuple::default()),
    };
    malformed.inherit_from(&v2_list_entry);
    assert_eq!(malformed.snapshot_id, Some(77));
    assert_eq!(malformed.sequence_number, None);

    // v1 (list sequence number 0): everything inherits 0.
    let v1_list_entry = manifest_file_entry(false);
    let mut v1_existing = ManifestEntry {
        status: ManifestEntryStatus::Existing,
        snapshot_id: None,
        sequence_number: None,
        file_sequence_number: None,
        data_file: data_file("f", PartitionTuple::default()),
    };
    v1_existing.inherit_from(&v1_list_entry);
    assert_eq!(v1_existing.sequence_number, Some(0));
    assert_eq!(v1_existing.file_sequence_number, Some(0));
}

/// A synthetic v3 manifest (written directly with the Avro library, the
/// way a v3 engine would): the v3-only fields — `first_row_id` (142),
/// deletion-vector `content_offset` (144) / `content_size_in_bytes`
/// (145), `referenced_data_file` (143) — must be parsed and preserved.
#[test]
fn v3_manifest_fields_are_parsed_and_preserved() {
    use apache_avro::types::Value;
    let schema_json = serde_json::json!({
        "type": "record", "name": "manifest_entry", "fields": [
            {"name": "status", "type": "int", "field-id": 0},
            {"name": "snapshot_id", "type": ["null", "long"], "default": null, "field-id": 1},
            {"name": "sequence_number", "type": ["null", "long"], "default": null, "field-id": 3},
            {"name": "file_sequence_number", "type": ["null", "long"], "default": null, "field-id": 4},
            {"name": "data_file", "field-id": 2, "type": {"type": "record", "name": "r2", "fields": [
                {"name": "content", "type": "int", "field-id": 134},
                {"name": "file_path", "type": "string", "field-id": 100},
                {"name": "file_format", "type": "string", "field-id": 101},
                {"name": "partition", "field-id": 102, "type": {"type": "record", "name": "r102", "fields": []}},
                {"name": "record_count", "type": "long", "field-id": 103},
                {"name": "file_size_in_bytes", "type": "long", "field-id": 104},
                {"name": "first_row_id", "type": ["null", "long"], "default": null, "field-id": 142},
                {"name": "referenced_data_file", "type": ["null", "string"], "default": null, "field-id": 143},
                {"name": "content_offset", "type": ["null", "long"], "default": null, "field-id": 144},
                {"name": "content_size_in_bytes", "type": ["null", "long"], "default": null, "field-id": 145},
            ]}},
        ]
    });
    let schema = apache_avro::Schema::parse_str(&schema_json.to_string()).expect("schema");
    let mut writer = apache_avro::Writer::new(&schema, Vec::new());
    writer
        .add_user_metadata("format-version".to_owned(), "3")
        .expect("meta");
    writer
        .add_user_metadata("content".to_owned(), "deletes")
        .expect("meta");
    writer
        .add_user_metadata("partition-spec".to_owned(), "[]")
        .expect("meta");
    writer
        .add_user_metadata("schema".to_owned(), r#"{"type":"struct","fields":[]}"#)
        .expect("meta");
    let entry = Value::Record(vec![
        ("status".to_owned(), Value::Int(1)),
        (
            "snapshot_id".to_owned(),
            Value::Union(1, Box::new(Value::Long(9))),
        ),
        (
            "sequence_number".to_owned(),
            Value::Union(0, Box::new(Value::Null)),
        ),
        (
            "file_sequence_number".to_owned(),
            Value::Union(0, Box::new(Value::Null)),
        ),
        (
            "data_file".to_owned(),
            Value::Record(vec![
                ("content".to_owned(), Value::Int(1)),
                (
                    "file_path".to_owned(),
                    Value::String("s3://b/dv.puffin".into()),
                ),
                ("file_format".to_owned(), Value::String("puffin".into())),
                ("partition".to_owned(), Value::Record(vec![])),
                ("record_count".to_owned(), Value::Long(11)),
                ("file_size_in_bytes".to_owned(), Value::Long(512)),
                (
                    "first_row_id".to_owned(),
                    Value::Union(1, Box::new(Value::Long(4000))),
                ),
                (
                    "referenced_data_file".to_owned(),
                    Value::Union(1, Box::new(Value::String("s3://b/data/a.parquet".into()))),
                ),
                (
                    "content_offset".to_owned(),
                    Value::Union(1, Box::new(Value::Long(4))),
                ),
                (
                    "content_size_in_bytes".to_owned(),
                    Value::Union(1, Box::new(Value::Long(128))),
                ),
            ]),
        ),
    ]);
    writer.append_value_ref(&entry).expect("append");
    let bytes = writer.into_inner().expect("finish");

    let manifest = read_manifest(&bytes).expect("read v3 manifest");
    assert_eq!(manifest.metadata.format_version, Some(3));
    assert_eq!(manifest.metadata.content, ManifestContentType::Deletes);
    let df = &manifest.entries[0].data_file;
    assert_eq!(df.content, DataFileContent::PositionDeletes);
    assert_eq!(df.file_format, "puffin");
    assert_eq!(df.first_row_id, Some(4000));
    assert_eq!(
        df.referenced_data_file.as_deref(),
        Some("s3://b/data/a.parquet")
    );
    assert_eq!(df.content_offset, Some(4));
    assert_eq!(df.content_size_in_bytes, Some(128));
}
