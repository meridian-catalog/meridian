//! Tests for [`TableRequirement`]: wire format (exact REST type names) and
//! check semantics against present/absent tables.

use meridian_iceberg::spec::{
    LAST_ADDED, MetadataBuilder, PrimitiveType, RefType, Schema, Snapshot, SnapshotRef,
    StructField, TableMetadata, TableRequirement, TableUpdate, Type,
};
use serde_json::{Value, json};
use uuid::Uuid;

fn table() -> TableMetadata {
    let mut builder = MetadataBuilder::new_table(2, "s3://bucket/t").expect("new table");
    builder
        .apply_all([
            TableUpdate::AddSchema {
                schema: Schema::new(vec![StructField::required(
                    1,
                    "id",
                    Type::Primitive(PrimitiveType::Long),
                )]),
                last_column_id: None,
            },
            TableUpdate::SetCurrentSchema {
                schema_id: LAST_ADDED,
            },
            TableUpdate::AddSnapshot {
                snapshot: Snapshot {
                    snapshot_id: 42,
                    parent_snapshot_id: None,
                    sequence_number: Some(1),
                    timestamp_ms: 1_000,
                    manifest_list: Some("s3://bucket/t/metadata/snap-42.avro".to_owned()),
                    summary: None,
                    schema_id: Some(0),
                    first_row_id: None,
                    added_rows: None,
                    extra: serde_json::Map::new(),
                },
            },
            TableUpdate::SetSnapshotRef {
                ref_name: "main".to_owned(),
                reference: SnapshotRef {
                    snapshot_id: 42,
                    ref_type: RefType::Branch,
                    min_snapshots_to_keep: None,
                    max_snapshot_age_ms: None,
                    max_ref_age_ms: None,
                    extra: serde_json::Map::new(),
                },
            },
        ])
        .expect("seed table");
    builder.build(1_000, None).expect("build")
}

#[test]
fn every_requirement_serializes_to_its_rest_name() {
    let cases: Vec<(TableRequirement, &str)> = vec![
        (TableRequirement::AssertCreate, "assert-create"),
        (
            TableRequirement::AssertTableUuid { uuid: Uuid::nil() },
            "assert-table-uuid",
        ),
        (
            TableRequirement::AssertRefSnapshotId {
                r#ref: "main".to_owned(),
                snapshot_id: Some(42),
            },
            "assert-ref-snapshot-id",
        ),
        (
            TableRequirement::AssertLastAssignedFieldId {
                last_assigned_field_id: 1,
            },
            "assert-last-assigned-field-id",
        ),
        (
            TableRequirement::AssertCurrentSchemaId {
                current_schema_id: 0,
            },
            "assert-current-schema-id",
        ),
        (
            TableRequirement::AssertLastAssignedPartitionId {
                last_assigned_partition_id: 999,
            },
            "assert-last-assigned-partition-id",
        ),
        (
            TableRequirement::AssertDefaultSpecId { default_spec_id: 0 },
            "assert-default-spec-id",
        ),
        (
            TableRequirement::AssertDefaultSortOrderId {
                default_sort_order_id: 0,
            },
            "assert-default-sort-order-id",
        ),
    ];
    for (requirement, type_name) in cases {
        let value = serde_json::to_value(&requirement).expect("serialize");
        assert_eq!(
            value.get("type").and_then(Value::as_str),
            Some(type_name),
            "wrong type tag for {requirement:?}"
        );
        let back: TableRequirement = serde_json::from_value(value).expect("deserialize");
        assert_eq!(back, requirement, "round trip for {type_name}");
    }

    // The ref field is named exactly "ref" on the wire, and a null
    // snapshot-id is serialized explicitly (it asserts ref absence).
    let value = serde_json::to_value(TableRequirement::AssertRefSnapshotId {
        r#ref: "main".to_owned(),
        snapshot_id: None,
    })
    .expect("serialize");
    assert_eq!(
        value,
        json!({"type": "assert-ref-snapshot-id", "ref": "main", "snapshot-id": null})
    );
}

#[test]
fn assert_create_requires_table_absence() {
    let table = table();
    assert!(TableRequirement::AssertCreate.check(None).is_ok());
    let error = TableRequirement::AssertCreate
        .check(Some(&table))
        .expect_err("existing table must fail assert-create");
    assert!(error.to_string().contains("must not already exist"));
}

#[test]
fn every_other_requirement_fails_against_a_missing_table() {
    let requirements = [
        TableRequirement::AssertTableUuid { uuid: Uuid::nil() },
        TableRequirement::AssertRefSnapshotId {
            r#ref: "main".to_owned(),
            snapshot_id: None,
        },
        TableRequirement::AssertCurrentSchemaId {
            current_schema_id: 0,
        },
    ];
    for requirement in requirements {
        let error = requirement
            .check(None)
            .expect_err("missing table must fail");
        assert!(
            error.to_string().contains("does not exist"),
            "unhelpful message: {error}"
        );
    }
}

#[test]
fn uuid_requirement_matches_exactly() {
    let table = table();
    assert!(
        TableRequirement::AssertTableUuid {
            uuid: table.table_uuid
        }
        .check(Some(&table))
        .is_ok()
    );
    let error = TableRequirement::AssertTableUuid { uuid: Uuid::nil() }
        .check(Some(&table))
        .expect_err("wrong uuid must fail");
    let message = error.to_string();
    assert!(message.contains(&table.table_uuid.to_string()), "{message}");
}

#[test]
fn ref_snapshot_requirement_covers_all_four_cases() {
    let table = table();

    // Ref exists at the expected snapshot.
    assert!(
        TableRequirement::AssertRefSnapshotId {
            r#ref: "main".to_owned(),
            snapshot_id: Some(42),
        }
        .check(Some(&table))
        .is_ok()
    );
    // Ref exists at a different snapshot.
    let error = TableRequirement::AssertRefSnapshotId {
        r#ref: "main".to_owned(),
        snapshot_id: Some(41),
    }
    .check(Some(&table))
    .expect_err("wrong snapshot must fail");
    assert!(error.to_string().contains("must point at snapshot 41"));
    assert!(error.to_string().contains("found 42"));

    // Expected ref does not exist.
    let error = TableRequirement::AssertRefSnapshotId {
        r#ref: "audit".to_owned(),
        snapshot_id: Some(42),
    }
    .check(Some(&table))
    .expect_err("missing ref must fail");
    assert!(error.to_string().contains("does not exist"));

    // Null snapshot id asserts absence: holds for a missing ref, fails for
    // an existing one.
    assert!(
        TableRequirement::AssertRefSnapshotId {
            r#ref: "audit".to_owned(),
            snapshot_id: None,
        }
        .check(Some(&table))
        .is_ok()
    );
    let error = TableRequirement::AssertRefSnapshotId {
        r#ref: "main".to_owned(),
        snapshot_id: None,
    }
    .check(Some(&table))
    .expect_err("existing ref must fail an absence assertion");
    assert!(error.to_string().contains("must not exist"));
}

#[test]
fn id_requirements_compare_against_metadata_fields() {
    let table = table();

    assert!(
        TableRequirement::AssertLastAssignedFieldId {
            last_assigned_field_id: table.last_column_id,
        }
        .check(Some(&table))
        .is_ok()
    );
    assert!(
        TableRequirement::AssertCurrentSchemaId {
            current_schema_id: table.current_schema_id,
        }
        .check(Some(&table))
        .is_ok()
    );
    assert!(
        TableRequirement::AssertLastAssignedPartitionId {
            last_assigned_partition_id: table.last_partition_id,
        }
        .check(Some(&table))
        .is_ok()
    );
    assert!(
        TableRequirement::AssertDefaultSpecId {
            default_spec_id: table.default_spec_id,
        }
        .check(Some(&table))
        .is_ok()
    );
    assert!(
        TableRequirement::AssertDefaultSortOrderId {
            default_sort_order_id: table.default_sort_order_id,
        }
        .check(Some(&table))
        .is_ok()
    );

    // One stale example of each numeric assertion.
    let stale: Vec<(TableRequirement, &str)> = vec![
        (
            TableRequirement::AssertLastAssignedFieldId {
                last_assigned_field_id: table.last_column_id + 1,
            },
            "last assigned field id",
        ),
        (
            TableRequirement::AssertCurrentSchemaId {
                current_schema_id: table.current_schema_id + 1,
            },
            "current schema id",
        ),
        (
            TableRequirement::AssertLastAssignedPartitionId {
                last_assigned_partition_id: table.last_partition_id + 1,
            },
            "last assigned partition id",
        ),
        (
            TableRequirement::AssertDefaultSpecId {
                default_spec_id: table.default_spec_id + 1,
            },
            "default spec id",
        ),
        (
            TableRequirement::AssertDefaultSortOrderId {
                default_sort_order_id: table.default_sort_order_id + 1,
            },
            "default sort order id",
        ),
    ];
    for (requirement, what) in stale {
        let error = requirement
            .check(Some(&table))
            .expect_err("stale assertion must fail");
        let message = error.to_string();
        assert!(
            message.contains(what),
            "message must name {what}: {message}"
        );
        assert!(message.contains("found"), "message shows actual: {message}");
    }
}
