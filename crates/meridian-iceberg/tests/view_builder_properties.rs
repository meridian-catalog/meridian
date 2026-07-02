//! Property tests for the view-metadata builder: structural invariants must
//! hold after every successfully applied update, for arbitrary update
//! sequences.
//!
//! Invariants under test:
//! - (S) schema ids are unique; every version's schema resolves;
//! - (V) version ids are unique and never reused — newly assigned ids sit
//!   strictly above every id ever assigned;
//! - (C) once set, the current version always resolves;
//! - (L) after `build`, the version log's last entry names the current
//!   version, and log timestamps never decrease;
//! - (R) retention keeps the version count within
//!   `max(version.history.num-entries, versions added in the batch + the
//!   current version)`, never expiring the current version.

use std::collections::{BTreeMap, BTreeSet};

use meridian_iceberg::spec::{
    LAST_ADDED, PrimitiveType, Schema, StructField, Type, VERSION_HISTORY_NUM_ENTRIES_PROP,
    ViewMetadata, ViewMetadataBuilder, ViewRepresentation, ViewUpdate, ViewVersion,
};
use proptest::prelude::*;

/// Model state the generator uses to build mostly-valid updates.
struct Model {
    /// Marker making every generated SQL body unique, so no generated
    /// version is `same_definition` with another (id reuse stays a separate,
    /// deterministic test).
    next_marker: i64,
    /// Every version id ever assigned by the builder.
    version_ids_ever: BTreeSet<i32>,
}

fn sql_version(schema_id: i32, marker: i64) -> ViewVersion {
    ViewVersion {
        version_id: 0, // assigned by the builder
        timestamp_ms: 1_000 + marker,
        schema_id,
        summary: BTreeMap::from([("engine-name".to_owned(), "prop-tests".to_owned())]),
        representations: vec![ViewRepresentation::sql(format!("SELECT {marker}"), "spark")],
        default_catalog: None,
        default_namespace: vec!["ns".to_owned()],
        extra: serde_json::Map::new(),
    }
}

fn base_builder() -> ViewMetadataBuilder {
    let mut builder = ViewMetadataBuilder::new_view("s3://bucket/views/prop").expect("new view");
    builder
        .apply_all([
            ViewUpdate::AddSchema {
                schema: Schema::new(vec![StructField::required(
                    1,
                    "id",
                    Type::Primitive(PrimitiveType::Long),
                )]),
                last_column_id: None,
            },
            ViewUpdate::AddViewVersion {
                view_version: sql_version(LAST_ADDED, 0),
            },
            ViewUpdate::SetCurrentViewVersion {
                view_version_id: LAST_ADDED,
            },
        ])
        .expect("seed view");
    builder
}

/// Builds the next update from an op selector and a choice index, given the
/// builder's current state.
fn make_update(
    op: u8,
    choice: prop::sample::Index,
    builder: &ViewMetadataBuilder,
    model: &mut Model,
) -> ViewUpdate {
    let metadata = builder.current();
    match op % 7 {
        0 => {
            // Evolve the schema: one more field above the highest id ever
            // used by any schema (how a real client evolves).
            let max_field_id = metadata
                .schemas
                .iter()
                .map(Schema::max_field_id)
                .max()
                .unwrap_or(0);
            let new_id = max_field_id + 1;
            let mut fields = metadata
                .current_schema()
                .map(|s| s.fields.clone())
                .unwrap_or_default();
            fields.push(StructField::optional(
                new_id,
                format!("col_{new_id}"),
                Type::Primitive(PrimitiveType::String),
            ));
            ViewUpdate::AddSchema {
                schema: Schema::new(fields),
                last_column_id: None,
            }
        }
        1 => {
            // Add a version against an existing schema id.
            let schema_ids: Vec<i32> = metadata
                .schemas
                .iter()
                .filter_map(|s| s.schema_id)
                .collect();
            let schema_id = schema_ids[choice.index(schema_ids.len())];
            model.next_marker += 1;
            ViewUpdate::AddViewVersion {
                view_version: sql_version(schema_id, model.next_marker),
            }
        }
        2 => {
            // Add a version against the last added schema, when one exists
            // in this batch; otherwise against an existing id.
            model.next_marker += 1;
            ViewUpdate::AddViewVersion {
                view_version: sql_version(
                    metadata
                        .schemas
                        .last()
                        .and_then(|s| s.schema_id)
                        .unwrap_or(0),
                    model.next_marker,
                ),
            }
        }
        3 => {
            let version_ids: Vec<i32> = metadata.versions.iter().map(|v| v.version_id).collect();
            ViewUpdate::SetCurrentViewVersion {
                view_version_id: version_ids[choice.index(version_ids.len())],
            }
        }
        4 => ViewUpdate::SetCurrentViewVersion {
            view_version_id: LAST_ADDED,
        },
        5 => ViewUpdate::SetProperties {
            updates: BTreeMap::from([(format!("k{}", choice.index(5)), "v".to_owned())]),
        },
        _ => ViewUpdate::RemoveProperties {
            removals: vec![format!("k{}", choice.index(5))],
        },
    }
}

/// Asserts the structural invariants on the builder's working metadata.
fn assert_invariants(metadata: &ViewMetadata, model: &mut Model) -> Result<(), TestCaseError> {
    // (S) schema ids unique; every version's schema resolves.
    let mut schema_ids = BTreeSet::new();
    for schema in &metadata.schemas {
        let id = schema.schema_id.expect("stored schemas have ids");
        prop_assert!(schema_ids.insert(id), "duplicate schema id {id}");
    }
    for version in &metadata.versions {
        prop_assert!(
            metadata.schema_by_id(version.schema_id).is_some(),
            "version {} references missing schema {}",
            version.version_id,
            version.schema_id
        );
    }

    // (V) version ids unique; new ids are new forever.
    let mut version_ids = BTreeSet::new();
    for version in &metadata.versions {
        prop_assert!(
            version_ids.insert(version.version_id),
            "duplicate version id {}",
            version.version_id
        );
    }
    let max_ever = model.version_ids_ever.iter().max().copied();
    for id in &version_ids {
        if !model.version_ids_ever.contains(id) {
            // A freshly assigned id must sit above everything ever assigned.
            prop_assert!(
                max_ever.is_none_or(|max| *id > max),
                "version id {id} reuses or undercuts an earlier id (max ever {max_ever:?})"
            );
        }
    }
    model.version_ids_ever.extend(version_ids);

    // (C) current resolves once set.
    if metadata.current_version_id >= 0 {
        prop_assert!(
            metadata.current_version().is_some(),
            "current version {} must resolve",
            metadata.current_version_id
        );
    }
    Ok(())
}

proptest! {
    /// Random mostly-valid update sequences keep every structural invariant
    /// after each successful apply, and the result always builds with a
    /// well-maintained version log.
    #[test]
    fn random_update_sequences_preserve_invariants(
        ops in prop::collection::vec((any::<u8>(), any::<prop::sample::Index>()), 0..40),
    ) {
        let mut builder = base_builder();
        let mut model = Model {
            next_marker: 0,
            version_ids_ever: builder
                .current()
                .versions
                .iter()
                .map(|v| v.version_id)
                .collect(),
        };

        for (op, choice) in ops {
            let update = make_update(op, choice, &builder, &mut model);
            match builder.apply(update) {
                Ok(()) => assert_invariants(builder.current(), &mut model)?,
                Err(_) => {
                    // The documented contract: a rejected update poisons the
                    // builder. The sequence ends here.
                    return Ok(());
                }
            }
        }

        // (L)+(R) the final state builds; the log ends at the current
        // version with monotonic timestamps.
        let built = builder.build(1_000_000).expect("final state must build");
        prop_assert!(built.current_version().is_some());
        let last = built.version_log.last().expect("log never ends empty here");
        prop_assert_eq!(last.version_id, built.current_version_id);
        let timestamps: Vec<i64> = built.version_log.iter().map(|e| e.timestamp_ms).collect();
        prop_assert!(
            timestamps.windows(2).all(|w| w[0] <= w[1]),
            "version-log timestamps regressed: {:?}",
            timestamps
        );
    }

    /// Retention bounds hold across a follow-up commit: versions never
    /// exceed `max(history size, batch additions + current)`, the newest ids
    /// win, and the current version survives.
    #[test]
    fn version_retention_respects_the_configured_window(
        base_versions in 1usize..8,
        extra_versions in 1usize..8,
        history_size in 1usize..5,
    ) {
        // First commit: a view with `base_versions` versions (all exempt
        // from expiry because they were added in that batch).
        let mut builder = base_builder();
        let mut marker = 0;
        for _ in 1..base_versions {
            marker += 1;
            builder.apply_all([
                ViewUpdate::AddViewVersion { view_version: sql_version(0, marker) },
                ViewUpdate::SetCurrentViewVersion { view_version_id: LAST_ADDED },
            ]).expect("grow base");
        }
        let base = builder.build(10_000).expect("base builds");
        prop_assert_eq!(base.versions.len(), base_versions);

        // Second commit: configure retention, then add more versions.
        let mut builder = base.builder_from();
        builder.apply(ViewUpdate::SetProperties {
            updates: BTreeMap::from([(
                VERSION_HISTORY_NUM_ENTRIES_PROP.to_owned(),
                history_size.to_string(),
            )]),
        }).expect("set retention");
        for _ in 0..extra_versions {
            marker += 1;
            builder.apply_all([
                ViewUpdate::AddViewVersion { view_version: sql_version(0, marker) },
                ViewUpdate::SetCurrentViewVersion { view_version_id: LAST_ADDED },
            ]).expect("grow again");
        }
        let built = builder.build(20_000).expect("second commit builds");

        let allowed = history_size.max(extra_versions);
        let expected = (base_versions + extra_versions).min(allowed);
        prop_assert_eq!(
            built.versions.len(),
            expected,
            "retention window: base {} + extra {} with history {}",
            base_versions, extra_versions, history_size
        );
        // The retained ids are exactly the newest ones, and the current
        // version is among them.
        let mut all_ids: Vec<i32> = base.versions.iter().map(|v| v.version_id).collect();
        let new_ids: Vec<i32> = built
            .versions
            .iter()
            .map(|v| v.version_id)
            .filter(|id| !all_ids.contains(id))
            .collect();
        all_ids.extend(new_ids);
        all_ids.sort_unstable();
        let mut retained: Vec<i32> = built.versions.iter().map(|v| v.version_id).collect();
        retained.sort_unstable();
        prop_assert_eq!(&retained, &all_ids[all_ids.len() - expected..]);
        prop_assert!(built.current_version().is_some(), "current version survives expiry");
        // Every surviving log entry references a retained version.
        prop_assert!(
            built.version_log.iter().all(|e| retained.contains(&e.version_id)),
            "log references an expired version: {:?}",
            built.version_log
        );
    }
}
