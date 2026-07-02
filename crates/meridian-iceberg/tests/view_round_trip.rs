//! Round-trip tests for the view-metadata model.
//!
//! The invariant under test is the crate's design rule: parsing and
//! re-serializing a view metadata.json must be lossless, including fields —
//! and whole representation types — the typed model does not know about.

use meridian_iceberg::spec::{PrimitiveType, Type, ViewMetadata, ViewMetadataParseError};
use serde_json::Value;

const FIXTURE_V1: &str = include_str!("fixtures/view_metadata_v1.json");

#[test]
fn parses_view_metadata_into_typed_model() {
    let metadata = ViewMetadata::from_json(FIXTURE_V1).expect("parse view fixture");

    assert_eq!(metadata.format_version, 1);
    assert_eq!(
        metadata.view_uuid.to_string(),
        "fa6506c3-7681-40c8-86dc-e36561f83385"
    );
    assert_eq!(metadata.location, "s3://warehouse/analytics/event_agg");
    assert_eq!(metadata.current_version_id, 2);
    assert_eq!(metadata.property("comment"), Some("Daily event counts"));

    // Schemas reuse the table Schema type, including its type tree.
    assert_eq!(metadata.schemas.len(), 1);
    let schema = metadata.current_schema().expect("current schema resolves");
    assert_eq!(schema.schema_id, Some(1));
    assert_eq!(
        schema.fields[0].field_type,
        Type::Primitive(PrimitiveType::Int)
    );
    assert_eq!(schema.fields[0].doc.as_deref(), Some("Count of events"));
    assert!(schema.extra.contains_key("x-meridian-test-schema-note"));

    // Versions.
    assert_eq!(metadata.versions.len(), 2);
    let current = metadata.current_version().expect("current version");
    assert_eq!(current.version_id, 2);
    assert_eq!(current.timestamp_ms, 1_573_518_981_593);
    assert_eq!(current.schema_id, 1);
    assert_eq!(current.default_catalog.as_deref(), Some("prod"));
    assert_eq!(current.default_namespace, vec!["analytics".to_owned()]);
    assert_eq!(
        current.summary.get("engine-name").map(String::as_str),
        Some("Trino")
    );
    assert!(current.extra.contains_key("x-meridian-test-version-note"));

    // Representations: two typed SQL dialects plus one unknown type kept
    // opaque.
    assert_eq!(current.representations.len(), 3);
    let spark = current.representations[0]
        .as_sql()
        .expect("spark representation is sql");
    assert_eq!(spark.dialect, "spark");
    assert!(spark.sql.contains("FROM prod.analytics.events"));
    assert!(
        spark
            .extra
            .contains_key("x-meridian-test-representation-note")
    );
    let trino = current.representations[1]
        .as_sql()
        .expect("trino representation is sql");
    assert_eq!(trino.dialect, "trino");
    match &current.representations[2] {
        meridian_iceberg::spec::ViewRepresentation::Other(object) => {
            assert_eq!(
                object.get("type").and_then(Value::as_str),
                Some("x-meridian-test-plan")
            );
            assert!(object.contains_key("plan"));
        }
        meridian_iceberg::spec::ViewRepresentation::Sql(sql) => {
            panic!("third representation must be preserved as unknown, got sql {sql:?}")
        }
    }

    // Version log.
    assert_eq!(metadata.version_log.len(), 2);
    assert_eq!(metadata.version_log[1].version_id, 2);
    assert_eq!(metadata.version_log[1].timestamp_ms, 1_573_518_981_593);

    // Unmodelled top-level fields land in extra.
    assert!(
        metadata
            .extra
            .contains_key("x-meridian-test-unknown-top-level")
    );
}

#[test]
fn view_round_trip_is_lossless() {
    let metadata = ViewMetadata::from_json(FIXTURE_V1).expect("parse view fixture");
    let serialized = metadata.to_json().expect("serialize");

    let original: Value = serde_json::from_str(FIXTURE_V1).expect("fixture is valid JSON");
    let round_tripped: Value = serde_json::from_str(&serialized).expect("output is valid JSON");

    // Full structural equality: nothing dropped, nothing added, nothing
    // renamed — including every unknown field and the unknown
    // representation type.
    assert_eq!(round_tripped, original);
}

#[test]
fn double_round_trip_is_stable() {
    let first = ViewMetadata::from_json(FIXTURE_V1).expect("first parse");
    let second =
        ViewMetadata::from_json(&first.to_json().expect("serialize")).expect("second parse");
    assert_eq!(first, second);
}

#[test]
fn minimal_view_metadata_without_properties_parses() {
    let minimal = r#"{
        "view-uuid": "11111111-2222-3333-4444-555555555555",
        "format-version": 1,
        "location": "s3://bucket/v",
        "current-version-id": 1,
        "schemas": [
            {"type": "struct", "schema-id": 0, "fields": [
                {"id": 1, "name": "x", "required": true, "type": "long"}
            ]}
        ],
        "versions": [{
            "version-id": 1,
            "timestamp-ms": 1751444210834,
            "schema-id": 0,
            "summary": {},
            "representations": [{"type": "sql", "sql": "SELECT 1", "dialect": "spark"}],
            "default-namespace": []
        }],
        "version-log": [{"version-id": 1, "timestamp-ms": 1751444210834}]
    }"#;

    let metadata = ViewMetadata::from_json(minimal).expect("parse minimal view metadata");
    assert!(metadata.extra.is_empty());
    assert!(metadata.properties.is_none());

    // Optional sections absent on input must stay absent on output.
    let out: Value = serde_json::from_str(&metadata.to_json().expect("serialize")).expect("json");
    let object = out.as_object().expect("object");
    assert!(!object.contains_key("properties"));
    assert!(
        !object["versions"][0]
            .as_object()
            .expect("version object")
            .contains_key("default-catalog")
    );
}

#[test]
fn unsupported_view_format_versions_are_rejected() {
    let bad = r#"{"format-version": 2, "view-uuid": "11111111-2222-3333-4444-555555555555"}"#;
    let error = ViewMetadata::from_json(bad).expect_err("view format 2 must be rejected");
    assert!(matches!(
        error,
        ViewMetadataParseError::UnsupportedFormatVersion { found: 2 }
    ));

    let missing = r#"{"view-uuid": "11111111-2222-3333-4444-555555555555"}"#;
    let error = ViewMetadata::from_json(missing).expect_err("missing format-version rejected");
    assert!(error.to_string().contains("format-version"), "{error}");
}

#[test]
fn malformed_sql_representation_is_a_parse_error_not_an_unknown() {
    // `type: sql` claims the typed shape; a missing required field must fail
    // loudly instead of being quietly preserved as an unknown type.
    let bad = r#"{
        "view-uuid": "11111111-2222-3333-4444-555555555555",
        "format-version": 1,
        "location": "s3://bucket/v",
        "current-version-id": 1,
        "schemas": [{"type": "struct", "schema-id": 0, "fields": []}],
        "versions": [{
            "version-id": 1,
            "timestamp-ms": 1,
            "schema-id": 0,
            "summary": {},
            "representations": [{"type": "sql", "sql": "SELECT 1"}],
            "default-namespace": []
        }],
        "version-log": []
    }"#;
    let error = ViewMetadata::from_json(bad).expect_err("sql representation without dialect");
    assert!(error.to_string().contains("dialect"), "{error}");
}
