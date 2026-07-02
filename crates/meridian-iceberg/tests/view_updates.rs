//! Per-action tests for [`ViewUpdate`]: wire format (exact REST action
//! names), a happy-path application, and at least one rejection for every
//! action — plus the `assert-view-uuid` requirement and the build-time
//! version-log/retention behavior.

use std::collections::BTreeMap;

use meridian_iceberg::spec::{
    LAST_ADDED, PrimitiveType, Schema, StructField, Type, VERSION_HISTORY_NUM_ENTRIES_PROP,
    ViewMetadata, ViewMetadataBuildError, ViewMetadataBuilder, ViewRepresentation, ViewRequirement,
    ViewUpdate, ViewVersion,
};
use serde_json::{Value, json};
use uuid::Uuid;

// ---------------------------------------------------------------------------
// Helpers
// ---------------------------------------------------------------------------

fn base_schema() -> Schema {
    Schema::new(vec![
        StructField::optional(1, "event_count", Type::Primitive(PrimitiveType::Int)),
        StructField::optional(2, "event_date", Type::Primitive(PrimitiveType::Date)),
    ])
}

fn version(schema_id: i32, timestamp_ms: i64, sql: &str) -> ViewVersion {
    ViewVersion {
        version_id: 0, // assigned by the builder
        timestamp_ms,
        schema_id,
        summary: BTreeMap::from([("engine-name".to_owned(), "meridian-tests".to_owned())]),
        representations: vec![ViewRepresentation::sql(sql, "spark")],
        default_catalog: None,
        default_namespace: vec!["analytics".to_owned()],
        extra: serde_json::Map::new(),
    }
}

/// A builder seeded like a freshly created view: one schema, one version,
/// current set.
fn new_builder() -> ViewMetadataBuilder {
    let mut builder = ViewMetadataBuilder::new_view("s3://bucket/views/v").expect("new view");
    builder
        .apply_all([
            ViewUpdate::AddSchema {
                schema: base_schema(),
                last_column_id: None,
            },
            ViewUpdate::AddViewVersion {
                view_version: version(LAST_ADDED, 1_000, "SELECT 1"),
            },
            ViewUpdate::SetCurrentViewVersion {
                view_version_id: LAST_ADDED,
            },
        ])
        .expect("seed view");
    builder
}

fn base_view() -> ViewMetadata {
    new_builder().build(1_000).expect("build base view")
}

// ---------------------------------------------------------------------------
// Wire format
// ---------------------------------------------------------------------------

#[test]
fn update_actions_serialize_to_rest_names() {
    let uuid = Uuid::nil();
    let cases: Vec<(ViewUpdate, &str)> = vec![
        (ViewUpdate::AssignUuid { uuid }, "assign-uuid"),
        (
            ViewUpdate::UpgradeFormatVersion { format_version: 1 },
            "upgrade-format-version",
        ),
        (
            ViewUpdate::AddSchema {
                schema: base_schema(),
                last_column_id: None,
            },
            "add-schema",
        ),
        (
            ViewUpdate::SetLocation {
                location: "s3://bucket/v2".to_owned(),
            },
            "set-location",
        ),
        (
            ViewUpdate::SetProperties {
                updates: BTreeMap::from([("comment".to_owned(), "hi".to_owned())]),
            },
            "set-properties",
        ),
        (
            ViewUpdate::RemoveProperties {
                removals: vec!["comment".to_owned()],
            },
            "remove-properties",
        ),
        (
            ViewUpdate::AddViewVersion {
                view_version: version(0, 1, "SELECT 1"),
            },
            "add-view-version",
        ),
        (
            ViewUpdate::SetCurrentViewVersion {
                view_version_id: -1,
            },
            "set-current-view-version",
        ),
    ];
    for (update, action) in cases {
        let value = serde_json::to_value(&update).expect("serialize update");
        assert_eq!(value["action"], Value::from(action));
        let back: ViewUpdate = serde_json::from_value(value).expect("deserialize update");
        assert_eq!(back, update);
    }
}

#[test]
fn view_only_updates_use_rest_field_names() {
    let add = serde_json::to_value(ViewUpdate::AddViewVersion {
        view_version: version(3, 42, "SELECT 1"),
    })
    .expect("serialize");
    assert_eq!(add["view-version"]["schema-id"], Value::from(3));
    assert_eq!(add["view-version"]["timestamp-ms"], Value::from(42));
    assert_eq!(
        add["view-version"]["representations"][0],
        json!({"type": "sql", "sql": "SELECT 1", "dialect": "spark"})
    );

    let set = serde_json::to_value(ViewUpdate::SetCurrentViewVersion { view_version_id: 7 })
        .expect("serialize");
    assert_eq!(set["view-version-id"], Value::from(7));
}

#[test]
fn requirement_serializes_to_rest_shape() {
    let uuid = Uuid::nil();
    let requirement = ViewRequirement::AssertViewUuid { uuid };
    let value = serde_json::to_value(&requirement).expect("serialize requirement");
    assert_eq!(
        value,
        json!({"type": "assert-view-uuid", "uuid": uuid.to_string()})
    );
    let back: ViewRequirement = serde_json::from_value(value).expect("deserialize requirement");
    assert_eq!(back, requirement);
}

// ---------------------------------------------------------------------------
// assert-view-uuid
// ---------------------------------------------------------------------------

#[test]
fn assert_view_uuid_checks_identity() {
    let view = base_view();
    let matching = ViewRequirement::AssertViewUuid {
        uuid: view.view_uuid,
    };
    matching.check(Some(&view)).expect("matching uuid passes");

    let mismatched = ViewRequirement::AssertViewUuid { uuid: Uuid::nil() };
    let error = mismatched
        .check(Some(&view))
        .expect_err("mismatched uuid fails");
    assert!(error.to_string().contains("view UUID"), "{error}");

    let missing = matching
        .check(None)
        .expect_err("missing view cannot satisfy the assertion");
    assert!(missing.to_string().contains("does not exist"), "{missing}");
}

// ---------------------------------------------------------------------------
// assign-uuid
// ---------------------------------------------------------------------------

#[test]
fn assign_uuid_only_while_unassigned() {
    let uuid = Uuid::new_v4();
    let mut builder = ViewMetadataBuilder::new_view("s3://bucket/views/v").expect("new view");
    builder
        .apply(ViewUpdate::AssignUuid { uuid })
        .expect("assigning on a new view is allowed");
    assert_eq!(builder.current().view_uuid, uuid);
    // Idempotent re-assignment of the same value is fine...
    builder
        .apply(ViewUpdate::AssignUuid { uuid })
        .expect("same uuid is idempotent");
    // ...but changing it is not.
    let error = builder
        .apply(ViewUpdate::AssignUuid {
            uuid: Uuid::new_v4(),
        })
        .expect_err("reassignment rejected");
    assert!(matches!(error, ViewMetadataBuildError::UuidMismatch { .. }));

    // Existing views have a fixed identity from the start.
    let view = base_view();
    let error = view
        .builder_from()
        .apply(ViewUpdate::AssignUuid {
            uuid: Uuid::new_v4(),
        })
        .expect_err("existing view uuid is immutable");
    assert!(matches!(error, ViewMetadataBuildError::UuidMismatch { .. }));
}

// ---------------------------------------------------------------------------
// upgrade-format-version
// ---------------------------------------------------------------------------

#[test]
fn upgrade_format_version_accepts_only_one() {
    let view = base_view();
    let mut builder = view.builder_from();
    builder
        .apply(ViewUpdate::UpgradeFormatVersion { format_version: 1 })
        .expect("upgrading to 1 is a no-op");
    assert_eq!(builder.current().format_version, 1);

    for bad in [0u8, 2] {
        let error = view
            .builder_from()
            .apply(ViewUpdate::UpgradeFormatVersion {
                format_version: bad,
            })
            .expect_err("only view format version 1 exists");
        assert_eq!(
            error,
            ViewMetadataBuildError::UnsupportedFormatVersion { version: bad }
        );
    }
}

// ---------------------------------------------------------------------------
// add-schema
// ---------------------------------------------------------------------------

#[test]
fn add_schema_assigns_sequential_ids_and_reuses_identical() {
    let mut builder = new_builder();
    assert_eq!(builder.current().schemas[0].schema_id, Some(0));

    // A structurally identical schema reuses the existing id.
    builder
        .apply(ViewUpdate::AddSchema {
            schema: base_schema(),
            last_column_id: None,
        })
        .expect("re-add identical schema");
    assert_eq!(builder.current().schemas.len(), 1);

    // A new structure gets the next id.
    let mut fields = base_schema().fields;
    fields.push(StructField::optional(
        3,
        "region",
        Type::Primitive(PrimitiveType::String),
    ));
    builder
        .apply(ViewUpdate::AddSchema {
            schema: Schema::new(fields),
            last_column_id: None,
        })
        .expect("add evolved schema");
    assert_eq!(builder.current().schemas.len(), 2);
    assert_eq!(builder.current().schemas[1].schema_id, Some(1));
}

#[test]
fn add_schema_rejects_structural_problems() {
    // Duplicate field id.
    let error = new_builder()
        .apply(ViewUpdate::AddSchema {
            schema: Schema::new(vec![
                StructField::optional(1, "a", Type::Primitive(PrimitiveType::Int)),
                StructField::optional(1, "b", Type::Primitive(PrimitiveType::Int)),
            ]),
            last_column_id: None,
        })
        .expect_err("duplicate field id");
    assert!(matches!(
        error,
        ViewMetadataBuildError::InvalidSchema { .. }
    ));

    // Non-positive field id.
    let error = new_builder()
        .apply(ViewUpdate::AddSchema {
            schema: Schema::new(vec![StructField::optional(
                0,
                "a",
                Type::Primitive(PrimitiveType::Int),
            )]),
            last_column_id: None,
        })
        .expect_err("field id 0");
    assert!(matches!(
        error,
        ViewMetadataBuildError::InvalidSchema { .. }
    ));

    // Empty field name.
    let error = new_builder()
        .apply(ViewUpdate::AddSchema {
            schema: Schema::new(vec![StructField::optional(
                1,
                "",
                Type::Primitive(PrimitiveType::Int),
            )]),
            last_column_id: None,
        })
        .expect_err("empty name");
    assert!(matches!(
        error,
        ViewMetadataBuildError::InvalidSchema { .. }
    ));

    // Unrecognized primitive type.
    let error = new_builder()
        .apply(ViewUpdate::AddSchema {
            schema: Schema::new(vec![StructField::optional(
                1,
                "a",
                Type::Primitive(PrimitiveType::Other("hyperloglog".to_owned())),
            )]),
            last_column_id: None,
        })
        .expect_err("unknown type");
    assert_eq!(
        error,
        ViewMetadataBuildError::UnknownFieldType {
            type_string: "hyperloglog".to_owned()
        }
    );

    // Identifier field id that is not a schema field.
    let mut schema = base_schema();
    schema.identifier_field_ids = Some(vec![99]);
    let error = new_builder()
        .apply(ViewUpdate::AddSchema {
            schema,
            last_column_id: None,
        })
        .expect_err("identifier id not in schema");
    assert!(matches!(
        error,
        ViewMetadataBuildError::InvalidSchema { .. }
    ));

    // Deprecated last-column-id below the schema's own field ids.
    let error = new_builder()
        .apply(ViewUpdate::AddSchema {
            schema: base_schema(),
            last_column_id: Some(1),
        })
        .expect_err("last-column-id too low");
    assert_eq!(
        error,
        ViewMetadataBuildError::LastColumnIdTooLow {
            provided: 1,
            required: 2
        }
    );
}

// ---------------------------------------------------------------------------
// set-location / set-properties / remove-properties
// ---------------------------------------------------------------------------

#[test]
fn set_location_replaces_and_rejects_empty() {
    let mut builder = new_builder();
    builder
        .apply(ViewUpdate::SetLocation {
            location: "s3://bucket/views/moved".to_owned(),
        })
        .expect("set location");
    assert_eq!(builder.current().location, "s3://bucket/views/moved");

    let error = builder
        .apply(ViewUpdate::SetLocation {
            location: String::new(),
        })
        .expect_err("empty location");
    assert_eq!(error, ViewMetadataBuildError::EmptyLocation);
}

#[test]
fn properties_upsert_and_remove() {
    let mut builder = new_builder();
    builder
        .apply(ViewUpdate::SetProperties {
            updates: BTreeMap::from([
                ("comment".to_owned(), "daily counts".to_owned()),
                ("owner".to_owned(), "analytics".to_owned()),
            ]),
        })
        .expect("set properties");
    builder
        .apply(ViewUpdate::SetProperties {
            updates: BTreeMap::from([("comment".to_owned(), "hourly counts".to_owned())]),
        })
        .expect("overwrite property");
    assert_eq!(builder.current().property("comment"), Some("hourly counts"));

    // Removing a missing key is not an error (REST semantics).
    builder
        .apply(ViewUpdate::RemoveProperties {
            removals: vec!["owner".to_owned(), "not-set".to_owned()],
        })
        .expect("remove properties");
    assert_eq!(builder.current().property("owner"), None);

    let error = builder
        .apply(ViewUpdate::SetProperties {
            updates: BTreeMap::from([(String::new(), "x".to_owned())]),
        })
        .expect_err("empty property key on set");
    assert_eq!(error, ViewMetadataBuildError::EmptyPropertyKey);
    let error = new_builder()
        .apply(ViewUpdate::RemoveProperties {
            removals: vec![String::new()],
        })
        .expect_err("empty property key on remove");
    assert_eq!(error, ViewMetadataBuildError::EmptyPropertyKey);
}

// ---------------------------------------------------------------------------
// add-view-version
// ---------------------------------------------------------------------------

#[test]
fn add_view_version_assigns_ids_and_resolves_schema_sentinel() {
    let mut builder = new_builder();
    // Seed version got id 1 (view version ids conventionally start at 1
    // only when clients send them; the builder keeps a provided id that is
    // above every existing one, and 0 is below nothing on an empty view).
    let first_id = builder.current().versions[0].version_id;

    builder
        .apply(ViewUpdate::AddViewVersion {
            view_version: version(LAST_ADDED, 2_000, "SELECT 2"),
        })
        .expect("add second version");
    let second = &builder.current().versions[1];
    assert_eq!(second.version_id, first_id + 1, "sequential id");
    assert_eq!(second.schema_id, 0, "-1 resolved to the last added schema");
}

#[test]
fn add_view_version_keeps_a_provided_id_above_all_existing() {
    // Reference behavior: the provided id is kept when it exceeds every
    // retained id; otherwise the next sequential id is assigned.
    let mut builder = new_builder();
    let mut high = version(0, 2_000, "SELECT 2");
    high.version_id = 100;
    builder
        .apply(ViewUpdate::AddViewVersion { view_version: high })
        .expect("add with high id");
    assert_eq!(builder.current().versions[1].version_id, 100);

    let mut low = version(0, 3_000, "SELECT 3");
    low.version_id = 5; // collides with nothing but is below 100
    builder
        .apply(ViewUpdate::AddViewVersion { view_version: low })
        .expect("add with low id");
    assert_eq!(
        builder.current().versions[2].version_id,
        101,
        "low ids are reassigned above the high-water mark"
    );
}

#[test]
fn add_view_version_reuses_identical_definitions() {
    // Re-adding a definition that was added in the *same* batch keeps the
    // reused id addressable through -1.
    let mut builder = new_builder();
    builder
        .apply_all([
            ViewUpdate::AddViewVersion {
                view_version: version(0, 999, "SELECT 1"),
            },
            ViewUpdate::SetCurrentViewVersion {
                view_version_id: LAST_ADDED,
            },
        ])
        .expect("re-add the seed definition (different timestamp)");
    assert_eq!(
        builder.current().versions.len(),
        1,
        "identical definition (timestamps ignored) must not create a new version"
    );

    // Re-adding a definition inherited from the base reuses its id but
    // clears the last-added sentinel (reference behavior): -1 may only name
    // a version this batch actually introduced.
    let view = base_view();
    let mut builder = view.builder_from();
    builder
        .apply(ViewUpdate::AddViewVersion {
            view_version: version(0, 999, "SELECT 1"),
        })
        .expect("re-add a definition from the base");
    assert_eq!(builder.current().versions.len(), 1);
    let error = builder
        .apply(ViewUpdate::SetCurrentViewVersion {
            view_version_id: LAST_ADDED,
        })
        .expect_err("-1 after reusing a pre-existing version");
    assert_eq!(error, ViewMetadataBuildError::NoLastAddedVersion);
}

#[test]
fn add_view_version_rejections() {
    // Unknown schema id.
    let error = new_builder()
        .apply(ViewUpdate::AddViewVersion {
            view_version: version(99, 2_000, "SELECT 2"),
        })
        .expect_err("unknown schema");
    assert_eq!(
        error,
        ViewMetadataBuildError::SchemaNotFound { schema_id: 99 }
    );

    // -1 without an added schema (builder over an existing view).
    let view = base_view();
    let error = view
        .builder_from()
        .apply(ViewUpdate::AddViewVersion {
            view_version: version(LAST_ADDED, 2_000, "SELECT 2"),
        })
        .expect_err("-1 schema with nothing added");
    assert_eq!(error, ViewMetadataBuildError::NoLastAddedSchema);

    // Multiple SQL representations for one dialect, case-insensitively.
    let mut duplicate = version(0, 2_000, "SELECT 2");
    duplicate
        .representations
        .push(ViewRepresentation::sql("SELECT 2 /* again */", "Spark"));
    let error = new_builder()
        .apply(ViewUpdate::AddViewVersion {
            view_version: duplicate,
        })
        .expect_err("duplicate dialect");
    assert_eq!(
        error,
        ViewMetadataBuildError::DuplicateDialect {
            dialect: "spark".to_owned()
        }
    );
}

// ---------------------------------------------------------------------------
// set-current-view-version
// ---------------------------------------------------------------------------

#[test]
fn set_current_view_version_updates_pointer_and_log() {
    let mut builder = new_builder();
    builder
        .apply_all([
            ViewUpdate::AddViewVersion {
                view_version: version(0, 2_000, "SELECT 2"),
            },
            ViewUpdate::SetCurrentViewVersion {
                view_version_id: LAST_ADDED,
            },
        ])
        .expect("add and set current");
    let built = builder.build(9_000).expect("build");
    assert_eq!(built.current_version_id, built.versions[1].version_id);
    // The log entry uses the version's own creation timestamp because the
    // version was added in this batch.
    let last = built.version_log.last().expect("log entry");
    assert_eq!(last.version_id, built.current_version_id);
    assert_eq!(last.timestamp_ms, 2_000);
}

#[test]
fn reactivating_an_old_version_logs_the_commit_time() {
    // Build a view with two versions, current = 2.
    let mut builder = new_builder();
    builder
        .apply_all([
            ViewUpdate::AddViewVersion {
                view_version: version(0, 2_000, "SELECT 2"),
            },
            ViewUpdate::SetCurrentViewVersion {
                view_version_id: LAST_ADDED,
            },
        ])
        .expect("second version");
    let view = builder.build(2_000).expect("build");
    let old_id = view.versions[0].version_id;

    // Roll back to version 1 in a fresh commit at t=5000.
    let mut rollback = view.builder_from();
    rollback
        .apply(ViewUpdate::SetCurrentViewVersion {
            view_version_id: old_id,
        })
        .expect("rollback");
    let rolled = rollback.build(5_000).expect("build rollback");
    assert_eq!(rolled.current_version_id, old_id);
    let last = rolled.version_log.last().expect("log entry");
    assert_eq!(last.version_id, old_id);
    assert_eq!(
        last.timestamp_ms, 5_000,
        "re-activation logs when it happened, not when the version was created"
    );
}

#[test]
fn version_log_timestamps_never_go_backwards() {
    let view = base_view(); // log entry at t=1000
    let mut builder = view.builder_from();
    let old_id = view.versions[0].version_id;
    builder
        .apply_all([
            ViewUpdate::AddViewVersion {
                view_version: version(0, 2_000, "SELECT 2"),
            },
            ViewUpdate::SetCurrentViewVersion {
                view_version_id: LAST_ADDED,
            },
            ViewUpdate::SetCurrentViewVersion {
                view_version_id: old_id,
            },
        ])
        .expect("switch forward and back in one batch");
    // Building with a clock that reads *before* the last logged timestamp:
    // the appended entry is clamped, never regressing the log.
    let built = builder.build(500).expect("build");
    let timestamps: Vec<i64> = built.version_log.iter().map(|e| e.timestamp_ms).collect();
    let mut sorted = timestamps.clone();
    sorted.sort_unstable();
    assert_eq!(
        timestamps, sorted,
        "log must stay monotonic: {timestamps:?}"
    );
    // Only the *final* current-version change of the batch is logged
    // (reference behavior).
    assert_eq!(built.version_log.len(), 2);
    assert_eq!(built.version_log.last().expect("entry").version_id, old_id);
}

#[test]
fn set_current_view_version_rejections() {
    let mut builder = new_builder();
    let error = builder
        .apply(ViewUpdate::SetCurrentViewVersion { view_version_id: 7 })
        .expect_err("unknown version id");
    assert_eq!(
        error,
        ViewMetadataBuildError::VersionNotFound { version_id: 7 }
    );

    let view = base_view();
    let error = view
        .builder_from()
        .apply(ViewUpdate::SetCurrentViewVersion {
            view_version_id: LAST_ADDED,
        })
        .expect_err("-1 with nothing added");
    assert_eq!(error, ViewMetadataBuildError::NoLastAddedVersion);
}

#[test]
fn setting_current_to_the_current_version_is_a_noop() {
    let view = base_view();
    let log_len = view.version_log.len();
    let mut builder = view.builder_from();
    builder
        .apply(ViewUpdate::SetCurrentViewVersion {
            view_version_id: view.current_version_id,
        })
        .expect("no-op set current");
    let rebuilt = builder.build(9_999).expect("build");
    assert_eq!(
        rebuilt.version_log.len(),
        log_len,
        "a no-op change must not extend the version log"
    );
}

// ---------------------------------------------------------------------------
// build-time validation and retention
// ---------------------------------------------------------------------------

#[test]
fn build_requires_a_current_version() {
    let builder = ViewMetadataBuilder::new_view("s3://bucket/views/v").expect("new view");
    let error = builder.build(1_000).expect_err("no versions");
    assert_eq!(error, ViewMetadataBuildError::CurrentVersionUnset);

    let mut builder = ViewMetadataBuilder::new_view("s3://bucket/views/v").expect("new view");
    builder
        .apply_all([
            ViewUpdate::AddSchema {
                schema: base_schema(),
                last_column_id: None,
            },
            ViewUpdate::AddViewVersion {
                view_version: version(LAST_ADDED, 1_000, "SELECT 1"),
            },
        ])
        .expect("add version without setting current");
    let error = builder.build(1_000).expect_err("current never set");
    assert_eq!(error, ViewMetadataBuildError::CurrentVersionUnset);
}

#[test]
fn new_view_requires_a_location() {
    let error = ViewMetadataBuilder::new_view("").expect_err("empty location");
    assert_eq!(error, ViewMetadataBuildError::EmptyLocation);
}

#[test]
fn retention_expires_old_versions_and_truncates_the_log() {
    // Build a base view with 6 versions (all added in one batch are exempt
    // from expiry, so they all survive the first build).
    let mut builder = new_builder();
    for i in 2..=6 {
        builder
            .apply_all([
                ViewUpdate::AddViewVersion {
                    view_version: version(0, 1_000 * i, &format!("SELECT {i}")),
                },
                ViewUpdate::SetCurrentViewVersion {
                    view_version_id: LAST_ADDED,
                },
            ])
            .expect("grow view");
    }
    let view = builder.build(6_000).expect("build");
    assert_eq!(view.versions.len(), 6);

    // A follow-up commit sets the retention to 3 and adds one more version:
    // only the 3 newest versions survive, and log entries referencing
    // expired versions truncate the history before them.
    let mut builder = view.builder_from();
    builder
        .apply_all([
            ViewUpdate::SetProperties {
                updates: BTreeMap::from([(
                    VERSION_HISTORY_NUM_ENTRIES_PROP.to_owned(),
                    "3".to_owned(),
                )]),
            },
            ViewUpdate::AddViewVersion {
                view_version: version(0, 7_000, "SELECT 7"),
            },
            ViewUpdate::SetCurrentViewVersion {
                view_version_id: LAST_ADDED,
            },
        ])
        .expect("retention commit");
    let trimmed = builder.build(7_000).expect("build trimmed");

    assert_eq!(trimmed.versions.len(), 3, "retention keeps 3 versions");
    let mut retained: Vec<i32> = trimmed.versions.iter().map(|v| v.version_id).collect();
    retained.sort_unstable();
    let newest: Vec<i32> = {
        let mut all: Vec<i32> = view.versions.iter().map(|v| v.version_id).collect();
        all.push(trimmed.current_version_id);
        all.sort_unstable();
        all[all.len() - 3..].to_vec()
    };
    assert_eq!(retained, newest, "the newest ids are the ones retained");
    assert!(
        trimmed.version_by_id(trimmed.current_version_id).is_some(),
        "the current version is always retained"
    );
    assert!(
        trimmed
            .version_log
            .iter()
            .all(|e| retained.contains(&e.version_id)),
        "no log entry may reference an expired version: {:?}",
        trimmed.version_log
    );
    assert_eq!(
        trimmed.version_log.last().expect("log entry").version_id,
        trimmed.current_version_id
    );
}

#[test]
fn unparsable_retention_falls_back_to_the_default() {
    let mut builder = new_builder();
    builder
        .apply(ViewUpdate::SetProperties {
            updates: BTreeMap::from([(
                VERSION_HISTORY_NUM_ENTRIES_PROP.to_owned(),
                "not-a-number".to_owned(),
            )]),
        })
        .expect("set junk retention");
    // Still builds; the junk value falls back to the default of 10.
    let built = builder.build(2_000).expect("build");
    assert_eq!(built.versions.len(), 1);
}
