//! Per-action tests for [`TableUpdate`]: wire format (exact REST action
//! names), a happy-path application, and at least one rejection for every
//! action.

use std::collections::BTreeMap;

use meridian_iceberg::spec::{
    EncryptedKey, LAST_ADDED, MetadataBuildError, MetadataBuilder, NullOrder, PartitionField,
    PartitionSpec, PartitionStatisticsFile, PrimitiveType, RefType, Schema, Snapshot, SnapshotRef,
    SortDirection, SortField, SortOrder, StatisticsFile, StructField, TableMetadata, TableUpdate,
    Transform, Type,
};
use serde_json::{Value, json};
use uuid::Uuid;

// ---------------------------------------------------------------------------
// Helpers
// ---------------------------------------------------------------------------

fn base_schema() -> Schema {
    Schema::new(vec![
        StructField::required(1, "id", Type::Primitive(PrimitiveType::Long)),
        StructField::required(2, "ts", Type::Primitive(PrimitiveType::Timestamptz)),
        StructField::optional(3, "data", Type::Primitive(PrimitiveType::String)),
    ])
}

fn new_builder(format_version: u8) -> MetadataBuilder {
    let mut builder =
        MetadataBuilder::new_table(format_version, "s3://bucket/t").expect("new table");
    builder
        .apply_all([
            TableUpdate::AddSchema {
                schema: base_schema(),
                last_column_id: None,
            },
            TableUpdate::SetCurrentSchema {
                schema_id: LAST_ADDED,
            },
        ])
        .expect("seed schema");
    builder
}

fn base_table(format_version: u8) -> TableMetadata {
    new_builder(format_version)
        .build(1_000, None)
        .expect("build base table")
}

fn snapshot(id: i64, parent: Option<i64>, sequence: Option<i64>, timestamp_ms: i64) -> Snapshot {
    Snapshot {
        snapshot_id: id,
        parent_snapshot_id: parent,
        sequence_number: sequence,
        timestamp_ms,
        manifest_list: Some(format!("s3://bucket/t/metadata/snap-{id}.avro")),
        summary: Some(BTreeMap::from([(
            "operation".to_owned(),
            "append".to_owned(),
        )])),
        schema_id: Some(0),
        first_row_id: None,
        added_rows: None,
        extra: serde_json::Map::new(),
    }
}

/// A v2 table with two committed snapshots (2 is current via `main`).
fn table_with_snapshots() -> TableMetadata {
    let base = base_table(2);
    let mut builder = base.builder_from();
    builder
        .apply_all([
            TableUpdate::AddSnapshot {
                snapshot: snapshot(1, None, Some(1), 2_000),
            },
            TableUpdate::AddSnapshot {
                snapshot: snapshot(2, Some(1), Some(2), 3_000),
            },
            TableUpdate::SetSnapshotRef {
                ref_name: "main".to_owned(),
                reference: branch(2),
            },
        ])
        .expect("seed snapshots");
    builder.build(3_000, None).expect("build")
}

fn branch(snapshot_id: i64) -> SnapshotRef {
    SnapshotRef {
        snapshot_id,
        ref_type: RefType::Branch,
        min_snapshots_to_keep: None,
        max_snapshot_age_ms: None,
        max_ref_age_ms: None,
        extra: serde_json::Map::new(),
    }
}

fn tag(snapshot_id: i64) -> SnapshotRef {
    SnapshotRef {
        ref_type: RefType::Tag,
        ..branch(snapshot_id)
    }
}

fn statistics_file(snapshot_id: i64) -> StatisticsFile {
    StatisticsFile {
        snapshot_id,
        statistics_path: format!("s3://bucket/t/metadata/stats-{snapshot_id}.puffin"),
        file_size_in_bytes: 512,
        file_footer_size_in_bytes: 64,
        blob_metadata: Vec::new(),
        extra: serde_json::Map::new(),
    }
}

fn partition_statistics_file(snapshot_id: i64) -> PartitionStatisticsFile {
    PartitionStatisticsFile {
        snapshot_id,
        statistics_path: format!("s3://bucket/t/metadata/pstats-{snapshot_id}.parquet"),
        file_size_in_bytes: 256,
        extra: serde_json::Map::new(),
    }
}

fn encryption_key(key_id: &str) -> EncryptedKey {
    EncryptedKey {
        key_id: key_id.to_owned(),
        encrypted_key_metadata: "c2VjcmV0".to_owned(),
        encrypted_by_id: None,
        properties: None,
        extra: serde_json::Map::new(),
    }
}

fn apply_one(
    metadata: &TableMetadata,
    update: TableUpdate,
) -> Result<TableMetadata, MetadataBuildError> {
    let mut builder = metadata.builder_from();
    builder.apply(update)?;
    builder.build(10_000, None)
}

// ---------------------------------------------------------------------------
// Wire format: exact action names from the REST spec
// ---------------------------------------------------------------------------

#[test]
#[allow(clippy::too_many_lines)] // one entry per REST action, deliberately exhaustive
fn every_action_serializes_to_its_rest_name() {
    let cases: Vec<(TableUpdate, &str)> = vec![
        (TableUpdate::AssignUuid { uuid: Uuid::nil() }, "assign-uuid"),
        (
            TableUpdate::UpgradeFormatVersion { format_version: 2 },
            "upgrade-format-version",
        ),
        (
            TableUpdate::AddSchema {
                schema: base_schema(),
                last_column_id: None,
            },
            "add-schema",
        ),
        (
            TableUpdate::SetCurrentSchema { schema_id: -1 },
            "set-current-schema",
        ),
        (
            TableUpdate::AddSpec {
                spec: PartitionSpec::new(vec![]),
            },
            "add-spec",
        ),
        (
            TableUpdate::SetDefaultSpec { spec_id: -1 },
            "set-default-spec",
        ),
        (
            TableUpdate::AddSortOrder {
                sort_order: SortOrder::unsorted(),
            },
            "add-sort-order",
        ),
        (
            TableUpdate::SetDefaultSortOrder { sort_order_id: -1 },
            "set-default-sort-order",
        ),
        (
            TableUpdate::AddSnapshot {
                snapshot: snapshot(9, None, Some(1), 1),
            },
            "add-snapshot",
        ),
        (
            TableUpdate::SetSnapshotRef {
                ref_name: "main".to_owned(),
                reference: branch(9),
            },
            "set-snapshot-ref",
        ),
        (
            TableUpdate::RemoveSnapshots {
                snapshot_ids: vec![9],
            },
            "remove-snapshots",
        ),
        (
            TableUpdate::RemoveSnapshotRef {
                ref_name: "main".to_owned(),
            },
            "remove-snapshot-ref",
        ),
        (
            TableUpdate::SetLocation {
                location: "s3://x".to_owned(),
            },
            "set-location",
        ),
        (
            TableUpdate::SetProperties {
                updates: BTreeMap::new(),
            },
            "set-properties",
        ),
        (
            TableUpdate::RemoveProperties { removals: vec![] },
            "remove-properties",
        ),
        (
            TableUpdate::SetStatistics {
                snapshot_id: None,
                statistics: statistics_file(9),
            },
            "set-statistics",
        ),
        (
            TableUpdate::RemoveStatistics { snapshot_id: 9 },
            "remove-statistics",
        ),
        (
            TableUpdate::SetPartitionStatistics {
                partition_statistics: partition_statistics_file(9),
            },
            "set-partition-statistics",
        ),
        (
            TableUpdate::RemovePartitionStatistics { snapshot_id: 9 },
            "remove-partition-statistics",
        ),
        (
            TableUpdate::RemovePartitionSpecs { spec_ids: vec![1] },
            "remove-partition-specs",
        ),
        (
            TableUpdate::RemoveSchemas {
                schema_ids: vec![1],
            },
            "remove-schemas",
        ),
        (
            TableUpdate::AddEncryptionKey {
                encryption_key: encryption_key("k1"),
            },
            "add-encryption-key",
        ),
        (
            TableUpdate::RemoveEncryptionKey {
                key_id: "k1".to_owned(),
            },
            "remove-encryption-key",
        ),
    ];

    for (update, action) in cases {
        let value = serde_json::to_value(&update).expect("serialize");
        assert_eq!(
            value.get("action").and_then(Value::as_str),
            Some(action),
            "wrong action tag for {update:?}"
        );
        let back: TableUpdate = serde_json::from_value(value).expect("deserialize");
        assert_eq!(back, update, "round trip for {action}");
    }
}

#[test]
fn rest_payload_shapes_parse() {
    // Shapes as a client would send them, including kebab-case field names
    // and flattened set-snapshot-ref.
    let updates: Vec<TableUpdate> = serde_json::from_value(json!([
        {"action": "assign-uuid", "uuid": "9c12d441-03fe-4693-9a96-a0705ddf69c1"},
        {"action": "upgrade-format-version", "format-version": 3},
        {"action": "add-schema", "schema": {"type": "struct", "fields": [
            {"id": 1, "name": "x", "required": true, "type": "decimal(10,2)"}
        ]}},
        {"action": "set-current-schema", "schema-id": -1},
        {"action": "add-spec", "spec": {"fields": [
            {"source-id": 1, "name": "x_bucket", "transform": "bucket[8]"}
        ]}},
        {"action": "set-default-spec", "spec-id": -1},
        {"action": "add-sort-order", "sort-order": {"order-id": 1, "fields": [
            {"source-id": 1, "transform": "identity", "direction": "asc", "null-order": "nulls-last"}
        ]}},
        {"action": "set-default-sort-order", "sort-order-id": -1},
        {"action": "set-snapshot-ref", "ref-name": "audit", "snapshot-id": 42, "type": "tag", "max-ref-age-ms": 100},
        {"action": "set-properties", "updates": {"a": "b"}},
        {"action": "remove-properties", "removals": ["a"]}
    ]))
    .expect("parse update list");
    assert_eq!(updates.len(), 11);

    match &updates[8] {
        TableUpdate::SetSnapshotRef {
            ref_name,
            reference,
        } => {
            assert_eq!(ref_name, "audit");
            assert_eq!(reference.snapshot_id, 42);
            assert_eq!(reference.ref_type, RefType::Tag);
            assert_eq!(reference.max_ref_age_ms, Some(100));
        }
        other => panic!("expected set-snapshot-ref, got {other:?}"),
    }
}

// ---------------------------------------------------------------------------
// snapshot-log ordering under rollback (Java engines refuse an unsorted log)
// ---------------------------------------------------------------------------

#[test]
fn rollback_keeps_snapshot_log_sorted_and_last_updated_consistent() {
    // main was pointed at snapshot 2 (ts 3_000), so the log has one entry:
    // [ (2, 3_000) ]. (Snapshot 1 was added but main never pointed at it.)
    let table = table_with_snapshots();
    let log = table.snapshot_log.as_ref().expect("snapshot log");
    assert_eq!(log.len(), 1);
    assert_eq!((log[0].snapshot_id, log[0].timestamp_ms), (2, 3_000));

    // Roll main back to the OLDER snapshot 1 (ts 2_000). Its own timestamp is
    // earlier than the last log entry (3_000); a naive append would regress the
    // log and make the table unreadable by the Iceberg Java reader.
    let mut builder = table.builder_from();
    builder
        .apply(TableUpdate::SetSnapshotRef {
            ref_name: "main".to_owned(),
            reference: branch(1),
        })
        .expect("rollback");
    // build with an EARLIER now than the newest snapshot to also exercise the
    // last-updated-ms clamp.
    let rolled = builder.build(2_500, None).expect("build rollback");

    let log = rolled.snapshot_log.as_ref().expect("snapshot log");
    assert_eq!(
        rolled.current_snapshot_id,
        Some(1),
        "main rolled to snapshot 1"
    );
    assert_eq!(log.len(), 2, "rollback appends a log entry");
    assert_eq!(log[1].snapshot_id, 1, "the new entry points at snapshot 1");

    // The whole log is non-decreasing in time (the invariant Java enforces).
    for pair in log.windows(2) {
        assert!(
            pair[1].timestamp_ms >= pair[0].timestamp_ms,
            "snapshot log must be sorted: {:?} then {:?}",
            pair[0],
            pair[1]
        );
    }
    // The clamped entry sits at the previous max (3_000), not the old snapshot's
    // 2_000 and not the earlier build `now` of 2_500.
    assert_eq!(log[1].timestamp_ms, 3_000);
    // last-updated-ms never precedes the newest snapshot-log entry.
    assert!(
        rolled.last_updated_ms >= log[1].timestamp_ms,
        "last-updated-ms {} must not lag the snapshot log {}",
        rolled.last_updated_ms,
        log[1].timestamp_ms
    );
}

// ---------------------------------------------------------------------------
// assign-uuid / upgrade-format-version
// ---------------------------------------------------------------------------

#[test]
fn assign_uuid_applies_at_create_and_rejects_reassignment() {
    let uuid = Uuid::from_u128(7);
    let mut builder = MetadataBuilder::new_table(2, "s3://bucket/t").expect("new table");
    builder
        .apply(TableUpdate::AssignUuid { uuid })
        .expect("assign at create");
    assert_eq!(builder.current().table_uuid, uuid);
    // Idempotent re-assign of the same uuid is fine.
    builder
        .apply(TableUpdate::AssignUuid { uuid })
        .expect("same uuid is a no-op");

    // Rejection: changing an assigned uuid.
    let error = builder
        .apply(TableUpdate::AssignUuid {
            uuid: Uuid::from_u128(8),
        })
        .expect_err("reassignment must fail");
    assert!(matches!(error, MetadataBuildError::UuidMismatch { .. }));

    // Rejection: existing tables cannot change uuid either.
    let table = base_table(2);
    let error = apply_one(
        &table,
        TableUpdate::AssignUuid {
            uuid: Uuid::from_u128(9),
        },
    )
    .expect_err("existing table uuid is fixed");
    assert!(matches!(error, MetadataBuildError::UuidMismatch { .. }));
}

#[test]
fn upgrade_format_version_moves_forward_only() {
    let table = base_table(1);
    let upgraded = apply_one(
        &table,
        TableUpdate::UpgradeFormatVersion { format_version: 3 },
    )
    .expect("upgrade 1 -> 3");
    assert_eq!(upgraded.format_version, 3);
    assert_eq!(upgraded.last_sequence_number, Some(0), "v2 bookkeeping");
    assert_eq!(upgraded.next_row_id, Some(0), "v3 bookkeeping");

    // Rejection: downgrade.
    let table = base_table(2);
    let error = apply_one(
        &table,
        TableUpdate::UpgradeFormatVersion { format_version: 1 },
    )
    .expect_err("downgrade must fail");
    assert!(matches!(
        error,
        MetadataBuildError::FormatVersionDowngrade {
            current: 2,
            requested: 1
        }
    ));

    // Rejection: unsupported version.
    let error = apply_one(
        &table,
        TableUpdate::UpgradeFormatVersion { format_version: 4 },
    )
    .expect_err("v4 must fail");
    assert!(matches!(
        error,
        MetadataBuildError::UnsupportedFormatVersion { version: 4 }
    ));
}

// ---------------------------------------------------------------------------
// add-schema / set-current-schema / remove-schemas
// ---------------------------------------------------------------------------

#[test]
fn add_schema_assigns_ids_and_tracks_last_column_id() {
    let table = base_table(2);
    let mut evolved_schema = base_schema();
    evolved_schema.fields.push(StructField::optional(
        4,
        "region",
        Type::Primitive(PrimitiveType::String),
    ));

    let mut builder = table.builder_from();
    builder
        .apply_all([
            TableUpdate::AddSchema {
                schema: evolved_schema,
                last_column_id: Some(4),
            },
            TableUpdate::SetCurrentSchema {
                schema_id: LAST_ADDED,
            },
        ])
        .expect("evolve schema");
    let evolved = builder.build(2_000, None).expect("build");

    assert_eq!(evolved.schemas.len(), 2);
    assert_eq!(evolved.current_schema_id, 1);
    assert_eq!(evolved.last_column_id, 4);
}

#[test]
fn add_schema_reuses_the_id_of_an_identical_schema() {
    let table = base_table(2);
    let mut builder = table.builder_from();
    builder
        .apply(TableUpdate::AddSchema {
            schema: base_schema(),
            last_column_id: None,
        })
        .expect("re-add identical schema");
    let rebuilt = builder.build(2_000, None).expect("build");
    assert_eq!(rebuilt.schemas.len(), 1, "no duplicate schema stored");
}

#[test]
fn add_schema_rejections() {
    let table = base_table(2);

    // Duplicate field id.
    let mut duplicate = base_schema();
    duplicate.fields.push(StructField::optional(
        1,
        "dup",
        Type::Primitive(PrimitiveType::Int),
    ));
    let error = apply_one(
        &table,
        TableUpdate::AddSchema {
            schema: duplicate,
            last_column_id: None,
        },
    )
    .expect_err("duplicate field id must fail");
    assert!(matches!(error, MetadataBuildError::InvalidSchema { .. }));

    // Deprecated last-column-id lower than the ids in use.
    let error = apply_one(
        &table,
        TableUpdate::AddSchema {
            schema: base_schema(),
            last_column_id: Some(1),
        },
    )
    .expect_err("stale last-column-id must fail");
    assert!(matches!(
        error,
        MetadataBuildError::LastColumnIdTooLow {
            provided: 1,
            required: 3
        }
    ));

    // v3 type on a v2 table.
    let mut v3_schema = base_schema();
    v3_schema.fields.push(StructField::optional(
        4,
        "geo",
        Type::Primitive(PrimitiveType::Geometry { crs: None }),
    ));
    let error = apply_one(
        &table,
        TableUpdate::AddSchema {
            schema: v3_schema,
            last_column_id: None,
        },
    )
    .expect_err("v3 type on v2 table must fail");
    assert!(
        matches!(
            error,
            MetadataBuildError::RequiresV3 {
                format_version: 2,
                ..
            }
        ),
        "{error}"
    );

    // v3 default values on a v2 table.
    let mut defaulted = base_schema();
    let mut field = StructField::optional(4, "d", Type::Primitive(PrimitiveType::String));
    field.initial_default = Some(json!("x"));
    defaulted.fields.push(field);
    let error = apply_one(
        &table,
        TableUpdate::AddSchema {
            schema: defaulted,
            last_column_id: None,
        },
    )
    .expect_err("defaults on v2 table must fail");
    assert!(matches!(error, MetadataBuildError::RequiresV3 { .. }));

    // Identifier field ids must exist.
    let mut bad_identifier = base_schema();
    bad_identifier.identifier_field_ids = Some(vec![99]);
    let error = apply_one(
        &table,
        TableUpdate::AddSchema {
            schema: bad_identifier,
            last_column_id: None,
        },
    )
    .expect_err("unknown identifier field must fail");
    assert!(matches!(error, MetadataBuildError::InvalidSchema { .. }));
}

#[test]
fn set_current_schema_rejects_unknown_ids_and_empty_sentinel() {
    let table = base_table(2);
    let error = apply_one(&table, TableUpdate::SetCurrentSchema { schema_id: 42 })
        .expect_err("unknown schema id must fail");
    assert!(matches!(
        error,
        MetadataBuildError::SchemaNotFound { schema_id: 42 }
    ));

    let error = apply_one(&table, TableUpdate::SetCurrentSchema { schema_id: -1 })
        .expect_err("-1 with no added schema must fail");
    assert!(matches!(error, MetadataBuildError::NoLastAddedSchema));
}

#[test]
fn remove_schemas_removes_unused_and_protects_current_and_in_use() {
    // Evolve to schema 1, keeping schema 0 around.
    let table = base_table(2);
    let mut builder = table.builder_from();
    let mut evolved = base_schema();
    evolved.fields.push(StructField::optional(
        4,
        "region",
        Type::Primitive(PrimitiveType::String),
    ));
    builder
        .apply_all([
            TableUpdate::AddSchema {
                schema: evolved,
                last_column_id: None,
            },
            TableUpdate::SetCurrentSchema {
                schema_id: LAST_ADDED,
            },
        ])
        .expect("evolve");
    let table = builder.build(2_000, None).expect("build");

    // Happy: schema 0 is unused, remove it. Field ids are not reused after.
    let cleaned = apply_one(
        &table,
        TableUpdate::RemoveSchemas {
            schema_ids: vec![0],
        },
    )
    .expect("remove unused schema");
    assert_eq!(cleaned.schemas.len(), 1);
    assert_eq!(
        cleaned.last_column_id, 4,
        "last-column-id must never decrease"
    );

    // Rejection: removing the current schema.
    let error = apply_one(
        &table,
        TableUpdate::RemoveSchemas {
            schema_ids: vec![1],
        },
    )
    .expect_err("removing current schema must fail");
    assert!(matches!(
        error,
        MetadataBuildError::CurrentSchemaRemoval { schema_id: 1 }
    ));

    // Rejection: removing a schema a snapshot still uses.
    let with_snapshot = {
        let mut builder = table.builder_from();
        builder
            .apply(TableUpdate::AddSnapshot {
                snapshot: snapshot(1, None, Some(1), 5_000),
            })
            .expect("add snapshot referencing schema 0");
        builder.build(5_000, None).expect("build")
    };
    let error = apply_one(
        &with_snapshot,
        TableUpdate::RemoveSchemas {
            schema_ids: vec![0],
        },
    )
    .expect_err("schema in use must not be removable");
    assert!(matches!(
        error,
        MetadataBuildError::SchemaInUse {
            schema_id: 0,
            snapshot_id: 1
        }
    ));

    // Rejection: unknown schema id.
    let error = apply_one(
        &table,
        TableUpdate::RemoveSchemas {
            schema_ids: vec![9],
        },
    )
    .expect_err("unknown schema id must fail");
    assert!(matches!(error, MetadataBuildError::SchemaNotFound { .. }));
}

// ---------------------------------------------------------------------------
// add-spec / set-default-spec / remove-partition-specs
// ---------------------------------------------------------------------------

#[test]
fn add_spec_assigns_field_ids_and_spec_id() {
    let table = base_table(2);
    let mut builder = table.builder_from();
    builder
        .apply_all([
            TableUpdate::AddSpec {
                spec: PartitionSpec::new(vec![
                    PartitionField::new(2, "ts_day", Transform::Day),
                    PartitionField::new(1, "id_bucket", Transform::Bucket(16)),
                ]),
            },
            TableUpdate::SetDefaultSpec {
                spec_id: LAST_ADDED,
            },
        ])
        .expect("add spec");
    let table = builder.build(2_000, None).expect("build");

    let spec = table.default_partition_spec().expect("default spec");
    assert_eq!(spec.spec_id, Some(1), "spec 0 is the unpartitioned default");
    assert_eq!(spec.fields[0].field_id, Some(1000));
    assert_eq!(spec.fields[1].field_id, Some(1001));
    assert_eq!(table.last_partition_id, 1001);
}

#[test]
fn add_spec_rejections() {
    let table = base_table(2);

    // Unknown source column.
    let error = apply_one(
        &table,
        TableUpdate::AddSpec {
            spec: PartitionSpec::new(vec![PartitionField::new(99, "x", Transform::Identity)]),
        },
    )
    .expect_err("unknown source field must fail");
    assert!(matches!(
        error,
        MetadataBuildError::UnknownSourceField { source_id: 99 }
    ));

    // Unrecognized transform.
    let error = apply_one(
        &table,
        TableUpdate::AddSpec {
            spec: PartitionSpec::new(vec![PartitionField::new(
                1,
                "x",
                Transform::Other("zorder".to_owned()),
            )]),
        },
    )
    .expect_err("unknown transform must fail");
    assert!(matches!(error, MetadataBuildError::UnknownTransform { .. }));

    // Duplicate partition field name.
    let error = apply_one(
        &table,
        TableUpdate::AddSpec {
            spec: PartitionSpec::new(vec![
                PartitionField::new(1, "x", Transform::Identity),
                PartitionField::new(2, "x", Transform::Day),
            ]),
        },
    )
    .expect_err("duplicate name must fail");
    assert!(matches!(
        error,
        MetadataBuildError::InvalidPartitionSpec { .. }
    ));
}

#[test]
fn void_transform_may_reference_a_dropped_column() {
    let table = base_table(2);
    let result = apply_one(
        &table,
        TableUpdate::AddSpec {
            spec: PartitionSpec::new(vec![PartitionField::new(99, "gone", Transform::Void)]),
        },
    );
    assert!(result.is_ok(), "void fields bind loosely: {result:?}");
}

#[test]
fn set_default_spec_and_remove_partition_specs_guard_ids() {
    let table = base_table(2);

    let error = apply_one(&table, TableUpdate::SetDefaultSpec { spec_id: 42 })
        .expect_err("unknown spec must fail");
    assert!(matches!(
        error,
        MetadataBuildError::SpecNotFound { spec_id: 42 }
    ));

    let error = apply_one(&table, TableUpdate::SetDefaultSpec { spec_id: -1 })
        .expect_err("-1 with no added spec must fail");
    assert!(matches!(error, MetadataBuildError::NoLastAddedSpec));

    // Rejection: removing the default spec.
    let error = apply_one(
        &table,
        TableUpdate::RemovePartitionSpecs { spec_ids: vec![0] },
    )
    .expect_err("removing the default spec must fail");
    assert!(matches!(
        error,
        MetadataBuildError::DefaultSpecRemoval { spec_id: 0 }
    ));

    // Happy: add a spec, make it default, drop the old one.
    let mut builder = table.builder_from();
    builder
        .apply_all([
            TableUpdate::AddSpec {
                spec: PartitionSpec::new(vec![PartitionField::new(2, "ts_day", Transform::Day)]),
            },
            TableUpdate::SetDefaultSpec {
                spec_id: LAST_ADDED,
            },
            TableUpdate::RemovePartitionSpecs { spec_ids: vec![0] },
        ])
        .expect("rotate default spec");
    let rotated = builder.build(2_000, None).expect("build");
    assert_eq!(rotated.partition_specs.len(), 1);
    assert_eq!(rotated.default_spec_id, 1);
    assert_eq!(
        rotated.last_partition_id, 1000,
        "last-partition-id must never decrease"
    );
}

// ---------------------------------------------------------------------------
// add-sort-order / set-default-sort-order
// ---------------------------------------------------------------------------

#[test]
fn add_sort_order_assigns_ids_and_validates_sources() {
    let table = base_table(2);
    let order = SortOrder {
        order_id: 99, // client-provided ids are reassigned
        fields: vec![SortField {
            transform: Transform::Identity,
            source_id: 2,
            direction: SortDirection::Desc,
            null_order: NullOrder::NullsLast,
            extra: serde_json::Map::new(),
        }],
        extra: serde_json::Map::new(),
    };
    let mut builder = table.builder_from();
    builder
        .apply_all([
            TableUpdate::AddSortOrder {
                sort_order: order.clone(),
            },
            TableUpdate::SetDefaultSortOrder { sort_order_id: -1 },
        ])
        .expect("add sort order");
    let table = builder.build(2_000, None).expect("build");
    assert_eq!(table.default_sort_order_id, 1);
    assert_eq!(table.sort_orders.len(), 2);

    // Rejection: unknown source column.
    let mut bad = order;
    bad.fields[0].source_id = 99;
    let error = apply_one(&table, TableUpdate::AddSortOrder { sort_order: bad })
        .expect_err("unknown source must fail");
    assert!(matches!(
        error,
        MetadataBuildError::UnknownSourceField { source_id: 99 }
    ));

    // Rejection: unknown default order id.
    let error = apply_one(
        &table,
        TableUpdate::SetDefaultSortOrder { sort_order_id: 42 },
    )
    .expect_err("unknown order must fail");
    assert!(matches!(
        error,
        MetadataBuildError::SortOrderNotFound { order_id: 42 }
    ));
}

// ---------------------------------------------------------------------------
// add-snapshot / set-snapshot-ref / remove-snapshots / remove-snapshot-ref
// ---------------------------------------------------------------------------

#[test]
fn add_snapshot_appends_and_advances_sequence_numbers() {
    let table = table_with_snapshots();
    assert_eq!(table.last_sequence_number, Some(2));
    assert_eq!(table.current_snapshot_id, Some(2));
    assert_eq!(
        table.snapshot_log.as_ref().map(Vec::len),
        Some(1),
        "one main-branch move"
    );
}

#[test]
fn add_snapshot_rejections() {
    let table = table_with_snapshots();

    // Duplicate snapshot id.
    let error = apply_one(
        &table,
        TableUpdate::AddSnapshot {
            snapshot: snapshot(2, Some(1), Some(3), 4_000),
        },
    )
    .expect_err("duplicate id must fail");
    assert!(matches!(
        error,
        MetadataBuildError::SnapshotAlreadyExists { snapshot_id: 2 }
    ));

    // Unknown parent.
    let error = apply_one(
        &table,
        TableUpdate::AddSnapshot {
            snapshot: snapshot(3, Some(77), Some(3), 4_000),
        },
    )
    .expect_err("unknown parent must fail");
    assert!(matches!(
        error,
        MetadataBuildError::ParentSnapshotNotFound {
            snapshot_id: 3,
            parent_id: 77
        }
    ));

    // Non-monotonic sequence number.
    let error = apply_one(
        &table,
        TableUpdate::AddSnapshot {
            snapshot: snapshot(3, Some(2), Some(2), 4_000),
        },
    )
    .expect_err("stale sequence number must fail");
    assert!(matches!(
        error,
        MetadataBuildError::NonMonotonicSequenceNumber {
            snapshot_id: 3,
            provided: 2,
            last: 2
        }
    ));

    // Missing manifest list.
    let mut no_manifest = snapshot(3, Some(2), Some(3), 4_000);
    no_manifest.manifest_list = None;
    let error = apply_one(
        &table,
        TableUpdate::AddSnapshot {
            snapshot: no_manifest,
        },
    )
    .expect_err("missing manifest-list must fail");
    assert!(matches!(error, MetadataBuildError::InvalidSnapshot { .. }));

    // Row lineage on a v2 table.
    let mut lineage = snapshot(3, Some(2), Some(3), 4_000);
    lineage.first_row_id = Some(0);
    let error = apply_one(&table, TableUpdate::AddSnapshot { snapshot: lineage })
        .expect_err("row lineage on v2 must fail");
    assert!(matches!(error, MetadataBuildError::RequiresV3 { .. }));
}

#[test]
fn v3_add_snapshot_assigns_row_lineage() {
    let mut builder = new_builder(3);
    let mut first = snapshot(1, None, Some(1), 2_000);
    first.added_rows = Some(100);
    let mut second = snapshot(2, Some(1), Some(2), 3_000);
    second.added_rows = Some(50);
    builder
        .apply_all([
            TableUpdate::AddSnapshot { snapshot: first },
            TableUpdate::AddSnapshot { snapshot: second },
        ])
        .expect("v3 snapshots");
    let table = builder.build(3_000, None).expect("build");

    assert_eq!(table.next_row_id, Some(150));
    let snapshots = table.snapshots.as_ref().expect("snapshots");
    assert_eq!(snapshots[0].first_row_id, Some(0));
    assert_eq!(snapshots[1].first_row_id, Some(100));
}

#[test]
fn set_snapshot_ref_moves_main_and_validates() {
    let table = table_with_snapshots();

    // Happy: a tag on an older snapshot.
    let tagged = apply_one(
        &table,
        TableUpdate::SetSnapshotRef {
            ref_name: "release".to_owned(),
            reference: tag(1),
        },
    )
    .expect("tag an old snapshot");
    assert_eq!(
        tagged
            .refs
            .as_ref()
            .and_then(|r| r.get("release"))
            .map(|r| r.snapshot_id),
        Some(1)
    );
    assert_eq!(tagged.current_snapshot_id, Some(2), "tags do not move main");

    // Rejection: pointing a ref at a snapshot that does not exist.
    let error = apply_one(
        &table,
        TableUpdate::SetSnapshotRef {
            ref_name: "main".to_owned(),
            reference: branch(99),
        },
    )
    .expect_err("unknown snapshot must fail");
    assert!(matches!(
        error,
        MetadataBuildError::SnapshotNotFound { snapshot_id: 99 }
    ));

    // Rejection: branch retention settings on a tag.
    let mut bad_tag = tag(1);
    bad_tag.min_snapshots_to_keep = Some(5);
    let error = apply_one(
        &table,
        TableUpdate::SetSnapshotRef {
            ref_name: "release".to_owned(),
            reference: bad_tag,
        },
    )
    .expect_err("branch retention on tag must fail");
    assert!(matches!(
        error,
        MetadataBuildError::BranchRetentionOnTag { .. }
    ));
}

#[test]
fn remove_snapshots_prunes_history_and_guards_references() {
    let table = table_with_snapshots();

    // Rejection: snapshot 2 is referenced by main.
    let error = apply_one(
        &table,
        TableUpdate::RemoveSnapshots {
            snapshot_ids: vec![2],
        },
    )
    .expect_err("referenced snapshot must not be removable");
    assert!(matches!(
        error,
        MetadataBuildError::SnapshotReferenced { snapshot_id: 2, .. }
            | MetadataBuildError::CurrentSnapshotRemoval { snapshot_id: 2 }
    ));

    // Attach statistics to snapshot 1, then expire it.
    let mut builder = table.builder_from();
    builder
        .apply(TableUpdate::SetStatistics {
            snapshot_id: None,
            statistics: statistics_file(1),
        })
        .expect("set stats");
    let table = builder.build(4_000, None).expect("build");

    let expired = apply_one(
        &table,
        TableUpdate::RemoveSnapshots {
            snapshot_ids: vec![1],
        },
    )
    .expect("expire snapshot 1");
    assert!(expired.snapshot_by_id(1).is_none());
    assert_eq!(
        expired.statistics.as_ref().map(Vec::len),
        Some(0),
        "statistics for removed snapshots are pruned"
    );

    // Rejection: unknown snapshot id.
    let error = apply_one(
        &table,
        TableUpdate::RemoveSnapshots {
            snapshot_ids: vec![42],
        },
    )
    .expect_err("unknown snapshot must fail");
    assert!(matches!(error, MetadataBuildError::SnapshotNotFound { .. }));
}

#[test]
fn remove_snapshot_ref_clears_main_and_rejects_unknown_refs() {
    let table = table_with_snapshots();
    let cleared = apply_one(
        &table,
        TableUpdate::RemoveSnapshotRef {
            ref_name: "main".to_owned(),
        },
    )
    .expect("remove main");
    assert_eq!(cleared.current_snapshot_id, None);
    assert!(
        cleared
            .refs
            .as_ref()
            .is_none_or(|r| !r.contains_key("main"))
    );

    let error = apply_one(
        &table,
        TableUpdate::RemoveSnapshotRef {
            ref_name: "nope".to_owned(),
        },
    )
    .expect_err("unknown ref must fail");
    assert!(matches!(error, MetadataBuildError::RefNotFound { .. }));
}

// ---------------------------------------------------------------------------
// set-location / set-properties / remove-properties
// ---------------------------------------------------------------------------

#[test]
fn set_location_updates_and_rejects_empty() {
    let table = base_table(2);
    let moved = apply_one(
        &table,
        TableUpdate::SetLocation {
            location: "s3://bucket/moved".to_owned(),
        },
    )
    .expect("set location");
    assert_eq!(moved.location, "s3://bucket/moved");

    let error = apply_one(
        &table,
        TableUpdate::SetLocation {
            location: String::new(),
        },
    )
    .expect_err("empty location must fail");
    assert!(matches!(error, MetadataBuildError::EmptyLocation));
}

#[test]
fn properties_are_upserted_removed_and_guarded() {
    let table = base_table(2);
    let with_props = apply_one(
        &table,
        TableUpdate::SetProperties {
            updates: BTreeMap::from([("write.format.default".to_owned(), "parquet".to_owned())]),
        },
    )
    .expect("set properties");
    assert_eq!(with_props.property("write.format.default"), Some("parquet"));

    let removed = apply_one(
        &with_props,
        TableUpdate::RemoveProperties {
            removals: vec![
                "write.format.default".to_owned(),
                "never-existed".to_owned(), // missing keys are ignored
            ],
        },
    )
    .expect("remove properties");
    assert_eq!(removed.property("write.format.default"), None);

    // Rejection: reserved keys.
    let error = apply_one(
        &table,
        TableUpdate::SetProperties {
            updates: BTreeMap::from([("format-version".to_owned(), "9".to_owned())]),
        },
    )
    .expect_err("reserved property must fail");
    assert!(matches!(error, MetadataBuildError::ReservedProperty { .. }));

    // Rejection: empty key.
    let error = apply_one(
        &table,
        TableUpdate::SetProperties {
            updates: BTreeMap::from([(String::new(), "x".to_owned())]),
        },
    )
    .expect_err("empty key must fail");
    assert!(matches!(error, MetadataBuildError::EmptyPropertyKey));
}

// ---------------------------------------------------------------------------
// statistics / partition statistics
// ---------------------------------------------------------------------------

#[test]
fn statistics_are_upserted_and_removed() {
    let table = table_with_snapshots();

    let mut builder = table.builder_from();
    builder
        .apply_all([
            TableUpdate::SetStatistics {
                snapshot_id: Some(2), // deprecated field, must agree
                statistics: statistics_file(2),
            },
            TableUpdate::SetPartitionStatistics {
                partition_statistics: partition_statistics_file(2),
            },
        ])
        .expect("set stats");
    let table = builder.build(4_000, None).expect("build");
    assert_eq!(table.statistics.as_ref().map(Vec::len), Some(1));
    assert_eq!(table.partition_statistics.as_ref().map(Vec::len), Some(1));

    // Upsert replaces (same snapshot id, new path).
    let mut replacement = statistics_file(2);
    replacement.statistics_path = "s3://bucket/t/metadata/stats-2b.puffin".to_owned();
    let replaced = apply_one(
        &table,
        TableUpdate::SetStatistics {
            snapshot_id: None,
            statistics: replacement,
        },
    )
    .expect("upsert stats");
    let stats = replaced.statistics.as_ref().expect("stats");
    assert_eq!(stats.len(), 1);
    assert!(stats[0].statistics_path.ends_with("stats-2b.puffin"));

    // Remove both kinds.
    let mut builder = table.builder_from();
    builder
        .apply_all([
            TableUpdate::RemoveStatistics { snapshot_id: 2 },
            TableUpdate::RemovePartitionStatistics { snapshot_id: 2 },
        ])
        .expect("remove stats");
    let cleared = builder.build(5_000, None).expect("build");
    assert_eq!(cleared.statistics.as_ref().map(Vec::len), Some(0));
    assert_eq!(cleared.partition_statistics.as_ref().map(Vec::len), Some(0));
}

#[test]
fn statistics_rejections() {
    let table = table_with_snapshots();

    // Deprecated snapshot-id disagreeing with the file.
    let error = apply_one(
        &table,
        TableUpdate::SetStatistics {
            snapshot_id: Some(1),
            statistics: statistics_file(2),
        },
    )
    .expect_err("mismatched snapshot ids must fail");
    assert!(matches!(
        error,
        MetadataBuildError::StatisticsSnapshotMismatch { update: 1, file: 2 }
    ));

    // Statistics for a snapshot that does not exist.
    let error = apply_one(
        &table,
        TableUpdate::SetStatistics {
            snapshot_id: None,
            statistics: statistics_file(42),
        },
    )
    .expect_err("unknown snapshot must fail");
    assert!(matches!(error, MetadataBuildError::SnapshotNotFound { .. }));

    let error = apply_one(
        &table,
        TableUpdate::SetPartitionStatistics {
            partition_statistics: partition_statistics_file(42),
        },
    )
    .expect_err("unknown snapshot must fail");
    assert!(matches!(error, MetadataBuildError::SnapshotNotFound { .. }));

    // Removing statistics that are not recorded.
    let error = apply_one(&table, TableUpdate::RemoveStatistics { snapshot_id: 2 })
        .expect_err("no stats recorded must fail");
    assert!(matches!(
        error,
        MetadataBuildError::StatisticsNotFound { .. }
    ));

    let error = apply_one(
        &table,
        TableUpdate::RemovePartitionStatistics { snapshot_id: 2 },
    )
    .expect_err("no partition stats recorded must fail");
    assert!(matches!(
        error,
        MetadataBuildError::PartitionStatisticsNotFound { .. }
    ));
}

// ---------------------------------------------------------------------------
// encryption keys (v3)
// ---------------------------------------------------------------------------

#[test]
fn encryption_keys_are_v3_only_and_id_unique() {
    let v3 = base_table(3);
    let mut builder = v3.builder_from();
    builder
        .apply(TableUpdate::AddEncryptionKey {
            encryption_key: encryption_key("k1"),
        })
        .expect("add key on v3");
    let error = builder
        .apply(TableUpdate::AddEncryptionKey {
            encryption_key: encryption_key("k1"),
        })
        .expect_err("duplicate key id must fail");
    assert!(matches!(
        error,
        MetadataBuildError::DuplicateEncryptionKey { .. }
    ));

    let table = {
        let mut builder = v3.builder_from();
        builder
            .apply(TableUpdate::AddEncryptionKey {
                encryption_key: encryption_key("k1"),
            })
            .expect("add");
        builder.build(2_000, None).expect("build")
    };
    let removed = apply_one(
        &table,
        TableUpdate::RemoveEncryptionKey {
            key_id: "k1".to_owned(),
        },
    )
    .expect("remove key");
    assert_eq!(removed.encryption_keys.as_ref().map(Vec::len), Some(0));

    let error = apply_one(
        &table,
        TableUpdate::RemoveEncryptionKey {
            key_id: "missing".to_owned(),
        },
    )
    .expect_err("unknown key must fail");
    assert!(matches!(
        error,
        MetadataBuildError::EncryptionKeyNotFound { .. }
    ));

    // Rejection: any encryption-key update on a v2 table.
    let v2 = base_table(2);
    let error = apply_one(
        &v2,
        TableUpdate::AddEncryptionKey {
            encryption_key: encryption_key("k1"),
        },
    )
    .expect_err("encryption keys on v2 must fail");
    assert!(matches!(
        error,
        MetadataBuildError::RequiresV3 {
            format_version: 2,
            ..
        }
    ));
}

// ---------------------------------------------------------------------------
// build(): bookkeeping
// ---------------------------------------------------------------------------

#[test]
fn build_maintains_last_updated_and_metadata_log() {
    let table = base_table(2);
    assert_eq!(table.last_updated_ms, 1_000);

    let mut builder = table.builder_from();
    builder
        .apply(TableUpdate::SetProperties {
            updates: BTreeMap::from([("a".to_owned(), "b".to_owned())]),
        })
        .expect("apply");
    let next = builder
        .build(2_000, Some("s3://bucket/t/metadata/00000-a.metadata.json"))
        .expect("build");
    assert_eq!(next.last_updated_ms, 2_000);
    let log = next.metadata_log.as_ref().expect("metadata log");
    assert_eq!(log.len(), 1);
    assert_eq!(
        log[0].metadata_file,
        "s3://bucket/t/metadata/00000-a.metadata.json"
    );
    assert_eq!(
        log[0].timestamp_ms, 1_000,
        "the entry carries the time the previous file was current"
    );

    // Clock skew: last-updated-ms never goes backwards.
    let mut builder = next.builder_from();
    builder
        .apply(TableUpdate::SetProperties {
            updates: BTreeMap::from([("c".to_owned(), "d".to_owned())]),
        })
        .expect("apply");
    let skewed = builder.build(1_500, None).expect("build");
    assert_eq!(skewed.last_updated_ms, 2_000);
}

#[test]
fn metadata_log_retention_is_configurable() {
    let table = base_table(2);
    let mut builder = table.builder_from();
    builder
        .apply(TableUpdate::SetProperties {
            updates: BTreeMap::from([(
                "write.metadata.previous-versions-max".to_owned(),
                "2".to_owned(),
            )]),
        })
        .expect("configure retention");
    let mut table = builder.build(2_000, None).expect("build");

    for i in 0..5 {
        let builder = table.builder_from();
        table = builder
            .build(
                3_000 + i,
                Some(&format!("s3://bucket/t/metadata/{i:05}.metadata.json")),
            )
            .expect("build");
    }
    let log = table.metadata_log.as_ref().expect("metadata log");
    assert_eq!(log.len(), 2, "retention keeps only the newest entries");
    assert!(log[1].metadata_file.contains("00004"));
}

#[test]
fn new_table_requires_a_current_schema() {
    let builder = MetadataBuilder::new_table(2, "s3://bucket/t").expect("new table");
    let error = builder.build(1_000, None).expect_err("no schema must fail");
    assert!(matches!(error, MetadataBuildError::CurrentSchemaUnset));
}
