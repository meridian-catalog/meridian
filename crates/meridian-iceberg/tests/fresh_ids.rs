//! Tests for fresh field-id assignment at table creation
//! ([`meridian_iceberg::spec::assign_fresh_ids`]) and the create-path spec
//! numbering (the first added spec on a new table becomes spec 0, as in the
//! reference implementation).

use meridian_iceberg::spec::{
    LAST_ADDED, ListType, MapType, MetadataBuildError, MetadataBuilder, NullOrder, PartitionField,
    PartitionSpec, PrimitiveType, Schema, SortDirection, SortField, SortOrder, StructField,
    StructType, TableUpdate, Transform, Type, assign_fresh_ids,
};

fn long_field(id: i32, name: &str) -> StructField {
    StructField::required(id, name, Type::Primitive(PrimitiveType::Long))
}

/// The shape Flink's connector sends: provisional field ids starting at 0.
fn flink_shaped_schema() -> Schema {
    Schema::new(vec![
        StructField::optional(0, "id", Type::Primitive(PrimitiveType::Long)),
        StructField::optional(1, "name", Type::Primitive(PrimitiveType::String)),
        StructField::optional(2, "value", Type::Primitive(PrimitiveType::Double)),
        StructField::optional(3, "ts", Type::Primitive(PrimitiveType::Timestamp)),
    ])
    .with_schema_id(0)
}

#[test]
fn zero_based_flink_ids_are_reassigned_one_based() {
    let fresh = assign_fresh_ids(&flink_shaped_schema(), None, None).expect("fresh ids");
    let ids: Vec<i32> = fresh.schema.fields.iter().map(|f| f.id).collect();
    assert_eq!(ids, vec![1, 2, 3, 4]);
    assert_eq!(
        fresh.schema.schema_id, None,
        "the provisional schema-id is discarded; the builder assigns one"
    );
    // Names, order, and types are untouched.
    let names: Vec<&str> = fresh
        .schema
        .fields
        .iter()
        .map(|f| f.name.as_str())
        .collect();
    assert_eq!(names, vec!["id", "name", "value", "ts"]);
}

#[test]
fn arbitrary_and_duplicate_ids_are_accepted_and_replaced() {
    // Duplicate and wild ids are provisional noise, not errors — as long as
    // nothing references them.
    let schema = Schema::new(vec![
        long_field(100, "a"),
        long_field(100, "b"),
        long_field(-7, "c"),
    ]);
    let fresh = assign_fresh_ids(&schema, None, None).expect("fresh ids");
    let ids: Vec<i32> = fresh.schema.fields.iter().map(|f| f.id).collect();
    assert_eq!(ids, vec![1, 2, 3]);
}

#[test]
fn nested_types_are_assigned_in_reference_order() {
    // Reference (`TypeUtil.assignFreshIds`) order: all direct fields of a
    // struct first, then each field's nested type in order; lists assign the
    // element id before descending; maps assign key id then value id.
    let schema = Schema::new(vec![
        StructField::required(
            10,
            "point",
            Type::Struct(StructType::new(vec![
                long_field(11, "x"),
                long_field(12, "y"),
            ])),
        ),
        StructField::optional(
            20,
            "tags",
            Type::List(ListType::new(
                21,
                Type::Struct(StructType::new(vec![long_field(22, "weight")])),
                true,
            )),
        ),
        StructField::optional(
            30,
            "attrs",
            Type::Map(MapType::new(
                31,
                Type::Primitive(PrimitiveType::String),
                32,
                Type::Primitive(PrimitiveType::Long),
                false,
            )),
        ),
    ]);
    let fresh = assign_fresh_ids(&schema, None, None).expect("fresh ids");

    // Top level: point=1, tags=2, attrs=3. Then point's fields: x=4, y=5.
    // Then tags: element=6, weight=7. Then attrs: key=8, value=9.
    assert_eq!(fresh.schema.fields[0].id, 1);
    assert_eq!(fresh.schema.fields[1].id, 2);
    assert_eq!(fresh.schema.fields[2].id, 3);
    let Type::Struct(point) = &fresh.schema.fields[0].field_type else {
        panic!("point stays a struct");
    };
    assert_eq!(point.fields[0].id, 4);
    assert_eq!(point.fields[1].id, 5);
    let Type::List(tags) = &fresh.schema.fields[1].field_type else {
        panic!("tags stays a list");
    };
    assert_eq!(tags.element_id, 6);
    let Type::Struct(element) = tags.element.as_ref() else {
        panic!("element stays a struct");
    };
    assert_eq!(element.fields[0].id, 7);
    let Type::Map(attrs) = &fresh.schema.fields[2].field_type else {
        panic!("attrs stays a map");
    };
    assert_eq!(attrs.key_id, 8);
    assert_eq!(attrs.value_id, 9);
}

#[test]
fn identifier_field_ids_are_remapped() {
    let mut schema = Schema::new(vec![long_field(0, "id"), long_field(5, "region")]);
    schema.identifier_field_ids = Some(vec![5, 0]);
    let fresh = assign_fresh_ids(&schema, None, None).expect("fresh ids");
    assert_eq!(fresh.schema.identifier_field_ids, Some(vec![2, 1]));
}

#[test]
fn identifier_referencing_unknown_or_ambiguous_id_is_rejected() {
    let mut schema = Schema::new(vec![long_field(0, "id")]);
    schema.identifier_field_ids = Some(vec![9]);
    let error = assign_fresh_ids(&schema, None, None).expect_err("unknown identifier id");
    assert!(matches!(error, MetadataBuildError::InvalidSchema { .. }));

    let mut schema = Schema::new(vec![long_field(3, "a"), long_field(3, "b")]);
    schema.identifier_field_ids = Some(vec![3]);
    let error = assign_fresh_ids(&schema, None, None).expect_err("ambiguous identifier id");
    assert!(matches!(error, MetadataBuildError::InvalidSchema { .. }));
}

#[test]
fn duplicate_names_within_one_struct_are_rejected() {
    let schema = Schema::new(vec![long_field(0, "x"), long_field(1, "x")]);
    let error = assign_fresh_ids(&schema, None, None).expect_err("duplicate sibling names");
    assert!(matches!(error, MetadataBuildError::InvalidSchema { .. }));

    // The same name in different structs is fine.
    let schema = Schema::new(vec![
        long_field(0, "x"),
        StructField::optional(
            1,
            "nested",
            Type::Struct(StructType::new(vec![long_field(2, "x")])),
        ),
    ]);
    assert!(assign_fresh_ids(&schema, None, None).is_ok());
}

#[test]
fn partition_spec_and_sort_order_sources_are_remapped() {
    let schema = Schema::new(vec![
        StructField::optional(0, "id", Type::Primitive(PrimitiveType::Long)),
        StructField::optional(1, "ts", Type::Primitive(PrimitiveType::Timestamp)),
    ]);
    let mut spec_field = PartitionField::new(1, "ts_day", Transform::Day);
    spec_field.field_id = Some(1000); // provisional, discarded
    let spec = PartitionSpec::new(vec![spec_field]);
    let order = SortOrder {
        order_id: 7, // provisional; the builder assigns the real one
        fields: vec![SortField {
            transform: Transform::Identity,
            source_id: 0,
            direction: SortDirection::Asc,
            null_order: NullOrder::NullsFirst,
            extra: serde_json::Map::new(),
        }],
        extra: serde_json::Map::new(),
    };

    let fresh = assign_fresh_ids(&schema, Some(&spec), Some(&order)).expect("fresh ids");
    let fresh_spec = fresh.partition_spec.expect("spec survives");
    assert_eq!(fresh_spec.fields[0].source_id, 2, "ts is field 2 now");
    assert_eq!(
        fresh_spec.fields[0].field_id, None,
        "partition field ids are server-assigned"
    );
    let fresh_order = fresh.sort_order.expect("order survives");
    assert_eq!(fresh_order.fields[0].source_id, 1, "id is field 1 now");
}

#[test]
fn spec_or_order_referencing_unknown_or_ambiguous_source_is_rejected() {
    let schema = Schema::new(vec![long_field(0, "id")]);
    let spec = PartitionSpec::new(vec![PartitionField::new(9, "x", Transform::Identity)]);
    let error = assign_fresh_ids(&schema, Some(&spec), None).expect_err("unknown spec source");
    assert!(matches!(
        error,
        MetadataBuildError::UnknownSourceField { source_id: 9 }
    ));

    let ambiguous = Schema::new(vec![long_field(3, "a"), long_field(3, "b")]);
    let spec = PartitionSpec::new(vec![PartitionField::new(3, "x", Transform::Identity)]);
    let error = assign_fresh_ids(&ambiguous, Some(&spec), None).expect_err("ambiguous source");
    assert!(matches!(
        error,
        MetadataBuildError::InvalidPartitionSpec { .. }
    ));

    let order = SortOrder {
        order_id: 1,
        fields: vec![SortField {
            transform: Transform::Identity,
            source_id: 9,
            direction: SortDirection::Asc,
            null_order: NullOrder::NullsLast,
            extra: serde_json::Map::new(),
        }],
        extra: serde_json::Map::new(),
    };
    let error = assign_fresh_ids(&schema, None, Some(&order)).expect_err("unknown sort source");
    assert!(matches!(
        error,
        MetadataBuildError::UnknownSourceField { source_id: 9 }
    ));
}

#[test]
fn freshened_flink_schema_builds_table_metadata() {
    // End-to-end through the builder: the exact rejection the Flink smoke
    // hit ("field id 0 is not positive") must not recur after freshening.
    let fresh = assign_fresh_ids(&flink_shaped_schema(), None, None).expect("fresh ids");
    let mut builder = MetadataBuilder::new_table(2, "s3://bucket/t").expect("new table");
    builder
        .apply_all([
            TableUpdate::AddSchema {
                schema: fresh.schema,
                last_column_id: None,
            },
            TableUpdate::SetCurrentSchema {
                schema_id: LAST_ADDED,
            },
        ])
        .expect("freshened schema passes builder validation");
    let metadata = builder.build(1_000, None).expect("build");
    assert_eq!(metadata.last_column_id, 4);
}

#[test]
fn first_spec_added_to_a_new_table_becomes_spec_0() {
    // Reference behavior: a table created with a partition spec carries
    // exactly one spec, numbered 0 — no phantom empty spec alongside it.
    let schema = Schema::new(vec![
        StructField::required(1, "id", Type::Primitive(PrimitiveType::Long)),
        StructField::required(2, "ts", Type::Primitive(PrimitiveType::Timestamp)),
    ]);
    let mut builder = MetadataBuilder::new_table(2, "s3://bucket/t").expect("new table");
    builder
        .apply_all([
            TableUpdate::AddSchema {
                schema,
                last_column_id: None,
            },
            TableUpdate::SetCurrentSchema {
                schema_id: LAST_ADDED,
            },
            TableUpdate::AddSpec {
                spec: PartitionSpec::new(vec![PartitionField::new(2, "ts_day", Transform::Day)]),
            },
            TableUpdate::SetDefaultSpec {
                spec_id: LAST_ADDED,
            },
        ])
        .expect("create with spec");
    let metadata = builder.build(1_000, None).expect("build");

    assert_eq!(metadata.partition_specs.len(), 1, "no phantom empty spec");
    let spec = &metadata.partition_specs[0];
    assert_eq!(spec.spec_id, Some(0));
    assert_eq!(metadata.default_spec_id, 0);
    assert_eq!(spec.fields[0].field_id, Some(1000));
    assert_eq!(metadata.last_partition_id, 1000);

    // Spec evolution on the built table appends as usual: the next spec is
    // 1 and spec 0 is retained.
    let mut evolve = metadata.builder_from();
    evolve
        .apply_all([
            TableUpdate::AddSpec {
                spec: PartitionSpec::new(vec![PartitionField::new(
                    1,
                    "id_bucket",
                    Transform::Bucket(16),
                )]),
            },
            TableUpdate::SetDefaultSpec {
                spec_id: LAST_ADDED,
            },
        ])
        .expect("evolve spec");
    let evolved = evolve.build(2_000, None).expect("build evolved");
    assert_eq!(evolved.partition_specs.len(), 2);
    assert_eq!(evolved.default_spec_id, 1);
}

#[test]
fn unpartitioned_create_keeps_the_empty_spec_0() {
    // No add-spec at all: the pre-seeded unpartitioned spec 0 is the
    // table's real spec, exactly as before.
    let mut builder = MetadataBuilder::new_table(2, "s3://bucket/t").expect("new table");
    builder
        .apply_all([
            TableUpdate::AddSchema {
                schema: Schema::new(vec![long_field(1, "id")]),
                last_column_id: None,
            },
            TableUpdate::SetCurrentSchema {
                schema_id: LAST_ADDED,
            },
        ])
        .expect("create unpartitioned");
    let metadata = builder.build(1_000, None).expect("build");
    assert_eq!(metadata.partition_specs.len(), 1);
    assert_eq!(metadata.partition_specs[0].spec_id, Some(0));
    assert!(metadata.partition_specs[0].fields.is_empty());
    assert_eq!(metadata.default_spec_id, 0);
}

#[test]
fn explicitly_added_unpartitioned_spec_reuses_the_placeholder() {
    // An added spec that is itself unpartitioned matches the placeholder
    // structurally and reuses id 0; a later real spec then appends as 1
    // (the placeholder is no longer replaceable — the client asked for the
    // unpartitioned spec).
    let mut builder = MetadataBuilder::new_table(2, "s3://bucket/t").expect("new table");
    builder
        .apply_all([
            TableUpdate::AddSchema {
                schema: Schema::new(vec![long_field(1, "id")]),
                last_column_id: None,
            },
            TableUpdate::SetCurrentSchema {
                schema_id: LAST_ADDED,
            },
            TableUpdate::AddSpec {
                spec: PartitionSpec::new(Vec::new()),
            },
            TableUpdate::SetDefaultSpec {
                spec_id: LAST_ADDED,
            },
            TableUpdate::AddSpec {
                spec: PartitionSpec::new(vec![PartitionField::new(
                    1,
                    "id_bucket",
                    Transform::Bucket(16),
                )]),
            },
        ])
        .expect("adds");
    let metadata = builder.build(1_000, None).expect("build");
    assert_eq!(metadata.partition_specs.len(), 2);
    assert_eq!(metadata.partition_specs[0].spec_id, Some(0));
    assert_eq!(metadata.partition_specs[1].spec_id, Some(1));
    assert_eq!(metadata.default_spec_id, 0);
}
