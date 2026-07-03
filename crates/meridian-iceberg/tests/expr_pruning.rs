//! Deterministic pruning tests: the complement of the soundness property
//! suite. Where `expr_soundness` proves matching files are never pruned,
//! this file proves files that provably cannot match ARE pruned — the
//! evaluators would be trivially "sound" if they kept everything.

use std::collections::BTreeMap;

use meridian_iceberg::expr::{
    Expression, file_might_match, project, summaries_might_match, tuple_might_match,
};
use meridian_iceberg::manifest::{
    DataFile, DataFileContent, FieldSummary, PartitionFieldType, PartitionTuple, PartitionValue,
    partition_field_types,
};
use meridian_iceberg::spec::{PartitionField, PrimitiveType, Schema, StructField, Transform, Type};
use meridian_iceberg::value::Datum;
use serde_json::json;

fn schema() -> Schema {
    Schema::new(vec![
        StructField::optional(1, "id", Type::Primitive(PrimitiveType::Long)),
        StructField::optional(2, "cat", Type::Primitive(PrimitiveType::String)),
        StructField::optional(3, "amount", Type::Primitive(PrimitiveType::Double)),
        StructField::optional(4, "day", Type::Primitive(PrimitiveType::Date)),
        StructField::required(5, "req", Type::Primitive(PrimitiveType::Int)),
    ])
    .with_schema_id(0)
}

fn filter(json: serde_json::Value) -> Expression {
    serde_json::from_value(json).expect("valid filter json")
}

/// A file with id in `[10, 100]`, cat in `["m", "p"]`, amount in
/// `[1.5, 9.5]` with 2 nulls and 1 NaN out of 20 values, day all-null.
fn stats_file() -> DataFile {
    DataFile {
        content: DataFileContent::Data,
        file_path: "mem://f.parquet".into(),
        file_format: "PARQUET".into(),
        partition: PartitionTuple::default(),
        record_count: 20,
        file_size_in_bytes: 1024,
        column_sizes: None,
        value_counts: Some(BTreeMap::from([
            (1, 20),
            (2, 20),
            (3, 20),
            (4, 20),
            (5, 20),
        ])),
        null_value_counts: Some(BTreeMap::from([(1, 0), (2, 0), (3, 2), (4, 20), (5, 0)])),
        nan_value_counts: Some(BTreeMap::from([(3, 1)])),
        lower_bounds: Some(BTreeMap::from([
            (1, Datum::Long(10).to_bound_bytes()),
            (2, Datum::String("m".into()).to_bound_bytes()),
            (3, Datum::Double(1.5).to_bound_bytes()),
        ])),
        upper_bounds: Some(BTreeMap::from([
            (1, Datum::Long(100).to_bound_bytes()),
            (2, Datum::String("p".into()).to_bound_bytes()),
            (3, Datum::Double(9.5).to_bound_bytes()),
        ])),
        key_metadata: None,
        split_offsets: None,
        equality_ids: None,
        sort_order_id: None,
        first_row_id: None,
        referenced_data_file: None,
        content_offset: None,
        content_size_in_bytes: None,
    }
}

fn eval(json: serde_json::Value, file: &DataFile) -> bool {
    let bound = filter(json).bind(&schema(), true).expect("bind");
    file_might_match(&bound, file)
}

#[test]
fn metrics_evaluator_prunes_out_of_range_files() {
    let file = stats_file();
    // Range pruning on the long column [10, 100].
    assert!(!eval(
        json!({"type": "eq", "term": "id", "value": 5}),
        &file
    ));
    assert!(!eval(
        json!({"type": "eq", "term": "id", "value": 101}),
        &file
    ));
    assert!(eval(
        json!({"type": "eq", "term": "id", "value": 100}),
        &file
    ));
    assert!(!eval(
        json!({"type": "lt", "term": "id", "value": 10}),
        &file
    ));
    assert!(eval(
        json!({"type": "lt", "term": "id", "value": 11}),
        &file
    ));
    assert!(!eval(
        json!({"type": "lt-eq", "term": "id", "value": 9}),
        &file
    ));
    assert!(!eval(
        json!({"type": "gt", "term": "id", "value": 100}),
        &file
    ));
    assert!(eval(
        json!({"type": "gt", "term": "id", "value": 99}),
        &file
    ));
    assert!(!eval(
        json!({"type": "gt-eq", "term": "id", "value": 101}),
        &file
    ));
    assert!(
        !eval(
            json!({"type": "in", "term": "id", "values": [1, 2, 101]}),
            &file
        ),
        "every literal outside the range prunes"
    );
    assert!(eval(
        json!({"type": "in", "term": "id", "values": [1, 55]}),
        &file
    ));
    // not-eq / not-in cannot be answered by min/max.
    assert!(eval(
        json!({"type": "not-eq", "term": "id", "value": 55}),
        &file
    ));
    assert!(eval(
        json!({"type": "not-in", "term": "id", "values": [55]}),
        &file
    ));
}

#[test]
fn metrics_evaluator_uses_null_and_nan_counts() {
    let file = stats_file();
    // id has zero nulls: is-null prunes.
    assert!(!eval(json!({"type": "is-null", "term": "id"}), &file));
    assert!(eval(json!({"type": "is-null", "term": "amount"}), &file));
    // day is all-null: not-null and every comparison prune; is-null keeps.
    assert!(!eval(json!({"type": "not-null", "term": "day"}), &file));
    assert!(!eval(
        json!({"type": "eq", "term": "day", "value": "2026-01-15"}),
        &file
    ));
    assert!(eval(json!({"type": "is-null", "term": "day"}), &file));
    // amount has a NaN: is-nan keeps; id-like NaN-free columns prune...
    assert!(eval(json!({"type": "is-nan", "term": "amount"}), &file));
    // ...but a file with nan_count = 0 prunes is-nan.
    let mut no_nan = stats_file();
    no_nan.nan_value_counts = Some(BTreeMap::from([(3, 0)]));
    assert!(!eval(json!({"type": "is-nan", "term": "amount"}), &no_nan));
    // is-null on a required column binds to constant false.
    assert!(!eval(json!({"type": "is-null", "term": "req"}), &file));
    assert!(eval(json!({"type": "not-null", "term": "req"}), &file));
}

#[test]
fn metrics_evaluator_handles_string_prefixes() {
    let file = stats_file(); // cat in ["m", "p"]
    assert!(!eval(
        json!({"type": "starts-with", "term": "cat", "value": "z"}),
        &file
    ));
    assert!(!eval(
        json!({"type": "starts-with", "term": "cat", "value": "a"}),
        &file
    ));
    assert!(eval(
        json!({"type": "starts-with", "term": "cat", "value": "n"}),
        &file
    ));
    // Bounds both inside the prefix range: not-starts-with prunes only
    // when every value provably shares the prefix and none are null.
    let mut prefixed = stats_file();
    prefixed
        .lower_bounds
        .as_mut()
        .expect("bounds")
        .insert(2, Datum::String("data_2026_a".into()).to_bound_bytes());
    prefixed
        .upper_bounds
        .as_mut()
        .expect("bounds")
        .insert(2, Datum::String("data_2026_z".into()).to_bound_bytes());
    assert!(!eval(
        json!({"type": "not-starts-with", "term": "cat", "value": "data_2026"}),
        &prefixed
    ));
    assert!(eval(
        json!({"type": "not-starts-with", "term": "cat", "value": "data_2027"}),
        &prefixed
    ));
}

#[test]
fn not_rewrites_through_binding() {
    let file = stats_file();
    // not(eq id 5) == not-eq → keep; not(not-eq id 5) == eq 5 → prune.
    assert!(eval(
        json!({"type": "not", "child": {"type": "eq", "term": "id", "value": 5}}),
        &file
    ));
    assert!(!eval(
        json!({"type": "not", "child": {"type": "not-eq", "term": "id", "value": 5}}),
        &file
    ));
    // De Morgan: not(id < 200 and id > -5) == id >= 200 or id <= -5 → prune.
    assert!(!eval(
        json!({"type": "not", "child": {
            "type": "and",
            "left": {"type": "lt", "term": "id", "value": 200},
            "right": {"type": "gt", "term": "id", "value": -5}
        }}),
        &file
    ));
}

// ---- partition projection + tuple/summary evaluation ----

fn spec_and_types() -> (Vec<PartitionField>, Vec<PartitionFieldType>) {
    let mut cat = PartitionField::new(2, "cat", Transform::Identity);
    cat.field_id = Some(1000);
    let mut bucket = PartitionField::new(1, "id_bucket", Transform::Bucket(16));
    bucket.field_id = Some(1001);
    let mut day = PartitionField::new(4, "day_part", Transform::Day);
    day.field_id = Some(1002);
    let mut zorder = PartitionField::new(1, "zorder", Transform::Other("zorder(a)".into()));
    zorder.field_id = Some(1003);
    let fields = vec![cat, bucket, day, zorder];
    // The unknown transform cannot be typed; type the first three only.
    let types = partition_field_types(&fields[..3], &schema()).expect("types");
    (fields, types)
}

fn tuple(cat: Option<&str>, bucket: Option<i32>, day: Option<i32>) -> PartitionTuple {
    PartitionTuple {
        fields: vec![
            PartitionValue {
                field_id: 1000,
                name: "cat".into(),
                value: cat.map(|s| Datum::String(s.into())),
            },
            PartitionValue {
                field_id: 1001,
                name: "id_bucket".into(),
                value: bucket.map(Datum::Int),
            },
            PartitionValue {
                field_id: 1002,
                name: "day_part".into(),
                value: day.map(Datum::Date),
            },
        ],
    }
}

fn project_filter(
    json: serde_json::Value,
    types: &[PartitionFieldType],
) -> meridian_iceberg::expr::PartitionPredicate {
    let bound = filter(json).bind(&schema(), true).expect("bind");
    project(&bound, types)
}

#[test]
fn identity_projection_prunes_tuples_exactly() {
    let (_, types) = spec_and_types();
    let pp = project_filter(
        json!({"type": "eq", "term": "cat", "value": "toys"}),
        &types,
    );
    assert!(tuple_might_match(
        &pp,
        &tuple(Some("toys"), Some(1), Some(0))
    ));
    assert!(!tuple_might_match(
        &pp,
        &tuple(Some("books"), Some(1), Some(0))
    ));
    // Null partition value satisfies only is-null.
    assert!(!tuple_might_match(&pp, &tuple(None, Some(1), Some(0))));
    let pp = project_filter(json!({"type": "is-null", "term": "cat"}), &types);
    assert!(tuple_might_match(&pp, &tuple(None, Some(1), Some(0))));
    assert!(!tuple_might_match(
        &pp,
        &tuple(Some("toys"), Some(1), Some(0))
    ));
    // Identity carries exclusion operators through.
    let pp = project_filter(
        json!({"type": "not-eq", "term": "cat", "value": "toys"}),
        &types,
    );
    assert!(!tuple_might_match(&pp, &tuple(Some("toys"), None, None)));
    assert!(tuple_might_match(&pp, &tuple(Some("books"), None, None)));
}

#[test]
fn bucket_projection_prunes_wrong_buckets() {
    let (_, types) = spec_and_types();
    // bucket16(id=34): hash 2017239379 % 16 = 3.
    let pp = project_filter(json!({"type": "eq", "term": "id", "value": 34}), &types);
    assert!(tuple_might_match(&pp, &tuple(Some("x"), Some(3), Some(0))));
    assert!(!tuple_might_match(&pp, &tuple(Some("x"), Some(4), Some(0))));
    // Range operators tell nothing about buckets: keep every bucket.
    let pp = project_filter(json!({"type": "lt", "term": "id", "value": 34}), &types);
    assert!(tuple_might_match(&pp, &tuple(Some("x"), Some(9), Some(0))));
}

#[test]
fn temporal_projection_prunes_days() {
    let (_, types) = spec_and_types();
    // 2026-01-15 is day 20468.
    let pp = project_filter(
        json!({"type": "eq", "term": "day", "value": "2026-01-15"}),
        &types,
    );
    assert!(tuple_might_match(
        &pp,
        &tuple(Some("x"), Some(0), Some(20468))
    ));
    assert!(!tuple_might_match(
        &pp,
        &tuple(Some("x"), Some(0), Some(20469))
    ));
    // lt over day: strictly-before boundary tightens to lt-eq of the
    // previous day.
    let pp = project_filter(
        json!({"type": "lt", "term": "day", "value": "2026-01-15"}),
        &types,
    );
    assert!(tuple_might_match(
        &pp,
        &tuple(Some("x"), Some(0), Some(20467))
    ));
    assert!(!tuple_might_match(
        &pp,
        &tuple(Some("x"), Some(0), Some(20468))
    ));
}

#[test]
fn transform_terms_project_when_specs_match() {
    let (_, types) = spec_and_types();
    // A filter directly over bucket[16](id) compares bucket numbers.
    let pp = project_filter(
        json!({"type": "eq", "term": {"type": "transform", "transform": "bucket[16]", "term": "id"}, "value": 7}),
        &types,
    );
    assert!(tuple_might_match(&pp, &tuple(Some("x"), Some(7), Some(0))));
    assert!(!tuple_might_match(&pp, &tuple(Some("x"), Some(8), Some(0))));
    // A transform term over a *different* transform keeps everything.
    let pp = project_filter(
        json!({"type": "eq", "term": {"type": "transform", "transform": "bucket[8]", "term": "id"}, "value": 7}),
        &types,
    );
    assert!(tuple_might_match(&pp, &tuple(Some("x"), Some(0), Some(0))));
}

#[test]
fn summaries_prune_whole_manifests() {
    let (_, types) = spec_and_types();
    let summaries = vec![
        FieldSummary {
            contains_null: false,
            contains_nan: Some(false),
            lower_bound: Some(Datum::String("aaa".into()).to_bound_bytes()),
            upper_bound: Some(Datum::String("mmm".into()).to_bound_bytes()),
        },
        FieldSummary {
            contains_null: false,
            contains_nan: None,
            lower_bound: Some(Datum::Int(2).to_bound_bytes()),
            upper_bound: Some(Datum::Int(5).to_bound_bytes()),
        },
        FieldSummary {
            contains_null: true,
            contains_nan: None,
            lower_bound: None,
            upper_bound: None,
        },
    ];
    // cat ranges [aaa, mmm]: eq "zzz" prunes the manifest.
    let pp = project_filter(json!({"type": "eq", "term": "cat", "value": "zzz"}), &types);
    assert!(!summaries_might_match(&pp, &summaries, &types));
    let pp = project_filter(json!({"type": "eq", "term": "cat", "value": "bbb"}), &types);
    assert!(summaries_might_match(&pp, &summaries, &types));
    // Bucket summary [2, 5]: id=34 buckets to 3 -> keep; a value whose
    // bucket is outside prunes. bucket16(0) = 9.
    let pp = project_filter(json!({"type": "eq", "term": "id", "value": 34}), &types);
    assert!(summaries_might_match(&pp, &summaries, &types));
    let pp = project_filter(json!({"type": "eq", "term": "id", "value": 0}), &types);
    assert!(!summaries_might_match(&pp, &summaries, &types));
    // The day field is all-null in this manifest: comparisons prune,
    // is-null keeps.
    let pp = project_filter(
        json!({"type": "eq", "term": "day", "value": "2026-01-15"}),
        &types,
    );
    assert!(!summaries_might_match(&pp, &summaries, &types));
    let pp = project_filter(json!({"type": "is-null", "term": "day"}), &types);
    assert!(summaries_might_match(&pp, &summaries, &types));
}

#[test]
fn unknown_transforms_and_missing_fields_keep_everything() {
    let schema = schema();
    let (fields, _) = spec_and_types();
    // Type the full spec including the unknown transform: it fails
    // typing, so planning falls back to the typeable prefix; projection
    // over the prefix keeps files regardless of the zorder field.
    assert!(partition_field_types(&fields, &schema).is_err());

    // A predicate over a column that only feeds the unknown-transform
    // field projects to True (spec rule).
    let (_, types) = spec_and_types(); // first three fields only
    let bound = filter(json!({"type": "eq", "term": "amount", "value": 1.0}))
        .bind(&schema, true)
        .expect("bind");
    let pp = project(&bound, &types);
    assert!(tuple_might_match(&pp, &tuple(None, None, None)));
    assert!(summaries_might_match(&pp, &[], &types));
}
