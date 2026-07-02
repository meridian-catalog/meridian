//! Property tests for the metadata builder: structural invariants must hold
//! after every successfully applied update, for arbitrary update sequences.
//!
//! Invariants under test:
//! - (S) schema ids are unique; the current schema always resolves once set;
//! - (F) `last-column-id` covers every field id ever added and never
//!   decreases — field ids are never reused, including after
//!   `remove-schemas`;
//! - (P) spec ids are unique; the default spec resolves; `last-partition-id`
//!   never decreases and covers every assigned partition field id;
//! - (O) sort order ids are unique; the default order resolves;
//! - (Q) sequence numbers are strictly monotonic per applied snapshot;
//! - (B) whenever a current schema is set, `build` succeeds and re-validates
//!   the full final state.

use std::collections::BTreeSet;

use meridian_iceberg::spec::{
    LAST_ADDED, MetadataBuilder, PartitionField, PartitionSpec, PrimitiveType, RefType, Schema,
    Snapshot, SnapshotRef, SortField, SortOrder, StructField, TableMetadata, TableUpdate,
    Transform, Type,
};
use proptest::prelude::*;

/// Model state the generator uses to build mostly-valid updates.
struct Model {
    next_snapshot_id: i64,
    last_snapshot_id: Option<i64>,
    /// Every field id that ever appeared in an added schema.
    field_ids_ever: BTreeSet<i32>,
    prev_last_column_id: i32,
    prev_last_partition_id: i32,
}

fn base_builder(format_version: u8) -> MetadataBuilder {
    let mut builder =
        MetadataBuilder::new_table(format_version, "s3://bucket/prop").expect("new table");
    builder
        .apply_all([
            TableUpdate::AddSchema {
                schema: Schema::new(vec![
                    StructField::required(1, "id", Type::Primitive(PrimitiveType::Long)),
                    StructField::optional(2, "payload", Type::Primitive(PrimitiveType::String)),
                ]),
                last_column_id: None,
            },
            TableUpdate::SetCurrentSchema {
                schema_id: LAST_ADDED,
            },
        ])
        .expect("seed schema");
    builder
}

/// Builds the next update from an op selector and a choice index, given the
/// builder's current state. Returns `None` when the op is not applicable
/// (e.g. nothing to remove).
#[allow(clippy::too_many_lines)]
fn make_update(
    op: u8,
    choice: prop::sample::Index,
    builder: &MetadataBuilder,
    model: &mut Model,
) -> Option<TableUpdate> {
    let metadata = builder.current();
    let current_schema = metadata.current_schema()?;
    let field_ids: Vec<i32> = current_schema.all_field_ids().into_iter().collect();
    match op % 12 {
        0 => {
            // Evolve the current schema: keep its fields, add one new field
            // with the next unassigned id (how a real client evolves).
            let new_id = metadata.last_column_id + 1;
            let mut fields = current_schema.fields.clone();
            fields.push(StructField::optional(
                new_id,
                format!("col_{new_id}"),
                Type::Primitive(PrimitiveType::Long),
            ));
            let schema = Schema::new(fields);
            model.field_ids_ever.extend(schema.all_field_ids());
            Some(TableUpdate::AddSchema {
                schema,
                last_column_id: None,
            })
        }
        1 => Some(TableUpdate::SetCurrentSchema {
            schema_id: LAST_ADDED,
        }),
        2 => {
            let source_id = field_ids[choice.index(field_ids.len())];
            let transforms = [
                Transform::Identity,
                Transform::Bucket(8),
                Transform::Truncate(4),
                Transform::Day,
                Transform::Void,
            ];
            let transform = transforms[choice.index(transforms.len())].clone();
            Some(TableUpdate::AddSpec {
                spec: PartitionSpec::new(vec![PartitionField::new(
                    source_id,
                    format!("p_{source_id}_{transform}"),
                    transform,
                )]),
            })
        }
        3 => Some(TableUpdate::SetDefaultSpec {
            spec_id: LAST_ADDED,
        }),
        4 => {
            let source_id = field_ids[choice.index(field_ids.len())];
            Some(TableUpdate::AddSortOrder {
                sort_order: SortOrder {
                    order_id: 0, // reassigned by the builder
                    fields: vec![SortField {
                        transform: Transform::Identity,
                        source_id,
                        direction: meridian_iceberg::spec::SortDirection::Asc,
                        null_order: meridian_iceberg::spec::NullOrder::NullsFirst,
                        extra: serde_json::Map::new(),
                    }],
                    extra: serde_json::Map::new(),
                },
            })
        }
        5 => Some(TableUpdate::SetDefaultSortOrder {
            sort_order_id: LAST_ADDED,
        }),
        6 => {
            // Remove one non-current, unused schema, if any.
            let used_by_snapshots: BTreeSet<i32> = metadata
                .snapshots
                .iter()
                .flatten()
                .filter_map(|s| s.schema_id)
                .collect();
            let removable: Vec<i32> = metadata
                .schemas
                .iter()
                .filter_map(|s| s.schema_id)
                .filter(|id| *id != metadata.current_schema_id && !used_by_snapshots.contains(id))
                .collect();
            if removable.is_empty() {
                return None;
            }
            let schema_id = removable[choice.index(removable.len())];
            Some(TableUpdate::RemoveSchemas {
                schema_ids: vec![schema_id],
            })
        }
        7 => {
            let removable: Vec<i32> = metadata
                .partition_specs
                .iter()
                .filter_map(|s| s.spec_id)
                .filter(|id| *id != metadata.default_spec_id)
                .collect();
            if removable.is_empty() {
                return None;
            }
            let spec_id = removable[choice.index(removable.len())];
            Some(TableUpdate::RemovePartitionSpecs {
                spec_ids: vec![spec_id],
            })
        }
        8 => {
            let snapshot_id = model.next_snapshot_id;
            model.next_snapshot_id += 1;
            let sequence_number = (metadata.format_version >= 2)
                .then(|| metadata.last_sequence_number.unwrap_or(0) + 1);
            let snapshot = Snapshot {
                snapshot_id,
                parent_snapshot_id: model.last_snapshot_id,
                sequence_number,
                timestamp_ms: 1_000 + snapshot_id,
                manifest_list: Some(format!("s3://bucket/prop/metadata/snap-{snapshot_id}.avro")),
                summary: None,
                schema_id: Some(metadata.current_schema_id),
                first_row_id: None,
                added_rows: (metadata.format_version >= 3).then_some(10),
                extra: serde_json::Map::new(),
            };
            model.last_snapshot_id = Some(snapshot_id);
            Some(TableUpdate::AddSnapshot { snapshot })
        }
        9 => {
            let snapshot_id = model.last_snapshot_id?;
            Some(TableUpdate::SetSnapshotRef {
                ref_name: "main".to_owned(),
                reference: SnapshotRef {
                    snapshot_id,
                    ref_type: RefType::Branch,
                    min_snapshots_to_keep: None,
                    max_snapshot_age_ms: None,
                    max_ref_age_ms: None,
                    extra: serde_json::Map::new(),
                },
            })
        }
        10 => Some(TableUpdate::SetProperties {
            updates: std::collections::BTreeMap::from([(
                format!("k{}", choice.index(5)),
                "v".to_owned(),
            )]),
        }),
        _ => {
            let next = (metadata.format_version + 1).min(3);
            Some(TableUpdate::UpgradeFormatVersion {
                format_version: next,
            })
        }
    }
}

/// Asserts the structural invariants on the builder's working metadata.
fn assert_invariants(metadata: &TableMetadata, model: &Model) -> Result<(), TestCaseError> {
    // (S) schema ids unique, current resolves.
    let mut schema_ids = BTreeSet::new();
    for schema in &metadata.schemas {
        let id = schema.schema_id.expect("stored schemas have ids");
        prop_assert!(schema_ids.insert(id), "duplicate schema id {id}");
    }
    prop_assert!(
        metadata.current_schema().is_some(),
        "current schema {} must resolve",
        metadata.current_schema_id
    );

    // (F) last-column-id is monotone and covers every field id ever added.
    prop_assert!(
        metadata.last_column_id >= model.prev_last_column_id,
        "last-column-id went backwards: {} -> {}",
        model.prev_last_column_id,
        metadata.last_column_id
    );
    if let Some(max_ever) = model.field_ids_ever.iter().max() {
        prop_assert!(
            metadata.last_column_id >= *max_ever,
            "field id {max_ever} above last-column-id {}",
            metadata.last_column_id
        );
    }
    for schema in &metadata.schemas {
        prop_assert!(metadata.last_column_id >= schema.max_field_id());
    }

    // (P) spec ids unique, default resolves, last-partition-id monotone and
    // covering.
    let mut spec_ids = BTreeSet::new();
    for spec in &metadata.partition_specs {
        let id = spec.spec_id.expect("stored specs have ids");
        prop_assert!(spec_ids.insert(id), "duplicate spec id {id}");
        for field in &spec.fields {
            let field_id = field.field_id.expect("stored partition fields have ids");
            prop_assert!(
                metadata.last_partition_id >= field_id,
                "partition field id {field_id} above last-partition-id {}",
                metadata.last_partition_id
            );
        }
    }
    prop_assert!(metadata.default_partition_spec().is_some());
    prop_assert!(metadata.last_partition_id >= model.prev_last_partition_id);

    // (O) sort order ids unique, default resolves.
    let mut order_ids = BTreeSet::new();
    for order in &metadata.sort_orders {
        prop_assert!(
            order_ids.insert(order.order_id),
            "duplicate sort order id {}",
            order.order_id
        );
        prop_assert!(
            order.order_id != 0 || order.fields.is_empty(),
            "order id 0 must be unsorted"
        );
    }
    prop_assert!(metadata.default_sort_order().is_some());

    // (Q) sequence numbers strictly increase along the snapshot list as
    // applied. Snapshots inherited from v1 (before an upgrade) carry none
    // and count as 0.
    if metadata.format_version >= 2
        && let Some(snapshots) = &metadata.snapshots
    {
        let mut last = 0;
        for snapshot in snapshots {
            let n = snapshot.sequence_number.unwrap_or(0);
            prop_assert!(n > last || n == 0, "sequence number {n} not above {last}");
            last = last.max(n);
        }
        prop_assert!(
            metadata.last_sequence_number.unwrap_or(0) >= last,
            "last-sequence-number must cover every snapshot"
        );
    }
    Ok(())
}

proptest! {
    /// Random mostly-valid update sequences keep every structural invariant
    /// after each successful apply, and the result always builds.
    #[test]
    fn random_update_sequences_preserve_invariants(
        format_version in 1u8..=3,
        ops in prop::collection::vec((any::<u8>(), any::<prop::sample::Index>()), 0..40),
    ) {
        let mut builder = base_builder(format_version);
        let mut model = Model {
            next_snapshot_id: 1,
            last_snapshot_id: None,
            field_ids_ever: BTreeSet::from([1, 2]),
            prev_last_column_id: builder.current().last_column_id,
            prev_last_partition_id: builder.current().last_partition_id,
        };

        for (op, choice) in ops {
            let Some(update) = make_update(op, choice, &builder, &mut model) else {
                continue;
            };
            match builder.apply(update) {
                Ok(()) => {
                    assert_invariants(builder.current(), &model)?;
                    model.prev_last_column_id = builder.current().last_column_id;
                    model.prev_last_partition_id = builder.current().last_partition_id;
                }
                Err(_) => {
                    // The documented contract: a rejected update poisons the
                    // builder. The sequence ends here.
                    return Ok(());
                }
            }
        }

        // (B) the final state builds and re-validates.
        let built = builder
            .build(50_000, Some("s3://bucket/prop/metadata/prev.metadata.json"))
            .expect("final state must build");
        prop_assert!(built.last_updated_ms >= 50_000);
        prop_assert!(built.metadata_log.as_ref().is_some_and(|l| !l.is_empty()));
    }

    /// Field ids are never reused: after removing schemas, newly added
    /// schemas keep allocating above the high-water mark.
    #[test]
    fn field_ids_are_never_reused_after_remove_schemas(evolutions in 1usize..6) {
        let mut builder = base_builder(2);
        let mut ever_assigned = BTreeSet::from([1, 2]);

        for _ in 0..evolutions {
            let metadata = builder.current();
            let new_field_id = metadata.last_column_id + 1;
            let current = metadata.current_schema().expect("current").clone();
            let old_schema_id = metadata.current_schema_id;

            let mut fields = current.fields;
            fields.push(StructField::optional(
                new_field_id,
                format!("col_{new_field_id}"),
                Type::Primitive(PrimitiveType::Long),
            ));
            builder
                .apply_all([
                    TableUpdate::AddSchema { schema: Schema::new(fields), last_column_id: None },
                    TableUpdate::SetCurrentSchema { schema_id: LAST_ADDED },
                    // Drop the schema we evolved away from.
                    TableUpdate::RemoveSchemas { schema_ids: vec![old_schema_id] },
                ])
                .expect("evolution step");

            // The freshly assigned id must be new, and the high-water mark
            // must cover it.
            prop_assert!(
                ever_assigned.insert(new_field_id),
                "field id {new_field_id} was reused"
            );
            prop_assert!(builder.current().last_column_id >= new_field_id);
        }
        let built = builder.build(60_000, None).expect("builds");
        prop_assert_eq!(built.schemas.len(), 1, "only the latest schema is retained");
        prop_assert_eq!(
            built.last_column_id,
            2 + i32::try_from(evolutions).expect("small"),
            "high-water mark counts every evolution"
        );
    }
}
