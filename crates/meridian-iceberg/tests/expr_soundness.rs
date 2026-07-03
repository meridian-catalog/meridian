//! Property tests for the planning evaluators' soundness invariant:
//!
//! > For random rows and a random filter, take any row that matches the
//! > filter. The data file holding that row — with statistics honestly
//! > computed from its rows, and a partition tuple honestly computed by
//! > the partition spec's transforms — must never be pruned, by any of
//! > the three evaluators (metrics, partition tuple, partition
//! > summaries).
//!
//! Row-level truth uses SQL/Iceberg semantics: `null` satisfies only
//! `is-null`; NaN satisfies `is-nan`, `not-eq`, and `not-in`, and fails
//! ordered comparisons.

use std::collections::BTreeMap;

use meridian_iceberg::expr::{
    BoundPredicate, CompareOp, Expression, SetOp, UnaryOp, apply_transform, file_might_match,
    project, summaries_might_match, tuple_might_match,
};
use meridian_iceberg::manifest::{
    DataFile, DataFileContent, FieldSummary, PartitionFieldType, PartitionTuple, PartitionValue,
    partition_field_types,
};
use meridian_iceberg::spec::{PartitionField, PrimitiveType, Schema, StructField, Transform, Type};
use meridian_iceberg::value::Datum;
use proptest::prelude::*;
use serde_json::json;

// ---- the test table ----

const COL_ID: i32 = 1; // long
const COL_CAT: i32 = 2; // string
const COL_AMOUNT: i32 = 3; // double (nullable, can be NaN)
const COL_DAY: i32 = 4; // date
const COL_PRICE: i32 = 5; // decimal(9,2)
const COL_N: i32 = 6; // int

fn schema() -> Schema {
    Schema::new(vec![
        StructField::optional(COL_ID, "id", Type::Primitive(PrimitiveType::Long)),
        StructField::optional(COL_CAT, "cat", Type::Primitive(PrimitiveType::String)),
        StructField::optional(COL_AMOUNT, "amount", Type::Primitive(PrimitiveType::Double)),
        StructField::optional(COL_DAY, "day", Type::Primitive(PrimitiveType::Date)),
        StructField::optional(
            COL_PRICE,
            "price",
            Type::Primitive(PrimitiveType::Decimal {
                precision: 9,
                scale: 2,
            }),
        ),
        StructField::optional(COL_N, "n", Type::Primitive(PrimitiveType::Int)),
    ])
    .with_schema_id(0)
}

#[derive(Debug, Clone)]
struct Row {
    values: BTreeMap<i32, Option<Datum>>,
}

fn days_from_ymd(year: i64, month: u32, day: u32) -> i32 {
    // Hinnant's days_from_civil, reimplemented independently of the crate.
    let y = if month <= 2 { year - 1 } else { year };
    let era = if y >= 0 { y } else { y - 399 } / 400;
    let yoe = y - era * 400;
    let mp = i64::from((month + 9) % 12);
    let doy = (153 * mp + 2) / 5 + i64::from(day) - 1;
    let doe = yoe * 365 + yoe / 4 - yoe / 100 + doy;
    i32::try_from(era * 146_097 + doe - 719_468).expect("test dates fit i32")
}

// ---- strategies ----

fn opt<T: std::fmt::Debug + Clone>(
    s: impl Strategy<Value = T>,
) -> impl Strategy<Value = Option<T>> {
    prop_oneof![1 => Just(None), 5 => s.prop_map(Some)]
}

fn ymd_strategy() -> impl Strategy<Value = (i64, u32, u32)> {
    (2025i64..=2026, 1u32..=12, 1u32..=28)
}

fn id_value() -> impl Strategy<Value = i64> {
    -20i64..=20
}

fn cat_value() -> impl Strategy<Value = String> {
    proptest::string::string_regex("[abcé]{0,4}").expect("regex")
}

fn amount_value() -> impl Strategy<Value = f64> {
    prop_oneof![
        6 => (-500i32..=500).prop_map(|v| f64::from(v) / 100.0),
        1 => Just(f64::NAN),
        1 => Just(0.0f64),
        1 => Just(-0.0f64),
    ]
}

fn price_unscaled() -> impl Strategy<Value = i64> {
    -2000i64..=2000
}

fn n_value() -> impl Strategy<Value = i32> {
    -10i32..=10
}

fn row_strategy() -> impl Strategy<Value = Row> {
    (
        opt(id_value()),
        opt(cat_value()),
        opt(amount_value()),
        opt(ymd_strategy()),
        opt(price_unscaled()),
        opt(n_value()),
    )
        .prop_map(|(id, cat, amount, day, price, n)| {
            let mut values = BTreeMap::new();
            values.insert(COL_ID, id.map(Datum::Long));
            values.insert(COL_CAT, cat.map(Datum::String));
            values.insert(COL_AMOUNT, amount.map(Datum::double));
            values.insert(
                COL_DAY,
                day.map(|(y, m, d)| Datum::Date(days_from_ymd(y, m, d))),
            );
            values.insert(
                COL_PRICE,
                price.map(|unscaled| Datum::Decimal {
                    unscaled: i128::from(unscaled),
                    scale: 2,
                }),
            );
            values.insert(COL_N, n.map(Datum::Int));
            Row { values }
        })
}

/// A literal in REST JSON single-value form, typed for the column.
fn literal_for(column: i32) -> BoxedStrategy<serde_json::Value> {
    match column {
        COL_ID => id_value().prop_map(|v| json!(v)).boxed(),
        COL_CAT => cat_value().prop_map(|v| json!(v)).boxed(),
        COL_AMOUNT => (-500i32..=500)
            .prop_map(|v| json!(f64::from(v) / 100.0))
            .boxed(),
        COL_DAY => ymd_strategy()
            .prop_map(|(y, m, d)| json!(format!("{y:04}-{m:02}-{d:02}")))
            .boxed(),
        COL_PRICE => price_unscaled()
            .prop_map(|u| {
                let sign = if u < 0 { "-" } else { "" };
                let a = u.abs();
                json!(format!("{sign}{}.{:02}", a / 100, a % 100))
            })
            .boxed(),
        _ => n_value().prop_map(|v| json!(v)).boxed(),
    }
}

fn column_name(column: i32) -> &'static str {
    match column {
        COL_ID => "id",
        COL_CAT => "cat",
        COL_AMOUNT => "amount",
        COL_DAY => "day",
        COL_PRICE => "price",
        _ => "n",
    }
}

fn any_column() -> impl Strategy<Value = i32> {
    prop_oneof![
        Just(COL_ID),
        Just(COL_CAT),
        Just(COL_AMOUNT),
        Just(COL_DAY),
        Just(COL_PRICE),
        Just(COL_N),
    ]
}

fn leaf_expression() -> BoxedStrategy<Expression> {
    let comparison = (any_column(), 0usize..6).prop_flat_map(|(col, op_idx)| {
        let op = ["lt", "lt-eq", "gt", "gt-eq", "eq", "not-eq"][op_idx];
        literal_for(col).prop_map(move |value| {
            serde_json::from_value(json!({
                "type": op, "term": column_name(col), "value": value,
            }))
            .expect("valid comparison")
        })
    });
    let starts_with = (proptest::bool::ANY, cat_value()).prop_map(|(negated, prefix)| {
        let op = if negated {
            "not-starts-with"
        } else {
            "starts-with"
        };
        serde_json::from_value(json!({
            "type": op, "term": "cat", "value": prefix,
        }))
        .expect("valid starts-with")
    });
    let unary = (any_column(), proptest::bool::ANY).prop_map(|(col, negated)| {
        let op = if negated { "not-null" } else { "is-null" };
        serde_json::from_value(json!({"type": op, "term": column_name(col)})).expect("valid unary")
    });
    let nan = proptest::bool::ANY.prop_map(|negated| {
        let op = if negated { "not-nan" } else { "is-nan" };
        serde_json::from_value(json!({"type": op, "term": "amount"})).expect("valid nan test")
    });
    let set =
        (any_column(), proptest::bool::ANY, 0usize..4).prop_flat_map(|(col, negated, len)| {
            let op = if negated { "not-in" } else { "in" };
            proptest::collection::vec(literal_for(col), len).prop_map(move |values| {
                serde_json::from_value(json!({
                    "type": op, "term": column_name(col), "values": values,
                }))
                .expect("valid set")
            })
        });
    prop_oneof![4 => comparison, 1 => starts_with, 1 => unary, 1 => nan, 2 => set].boxed()
}

fn expression_strategy() -> impl Strategy<Value = Expression> {
    leaf_expression().prop_recursive(3, 24, 4, |inner| {
        prop_oneof![
            (inner.clone(), inner.clone()).prop_map(|(left, right)| Expression::And {
                left: Box::new(left),
                right: Box::new(right),
            }),
            (inner.clone(), inner.clone()).prop_map(|(left, right)| Expression::Or {
                left: Box::new(left),
                right: Box::new(right),
            }),
            inner.prop_map(|child| Expression::Not {
                child: Box::new(child)
            }),
        ]
    })
}

/// The partition specs under test, exercising every projectable
/// transform family (plus void and unpartitioned).
fn partition_specs() -> Vec<Vec<PartitionField>> {
    fn pf(field_id: i32, source: i32, name: &str, transform: Transform) -> PartitionField {
        let mut f = PartitionField::new(source, name, transform);
        f.field_id = Some(field_id);
        f
    }
    vec![
        vec![pf(1000, COL_CAT, "cat", Transform::Identity)],
        vec![
            pf(1000, COL_ID, "id_bucket", Transform::Bucket(4)),
            pf(1001, COL_DAY, "day", Transform::Day),
        ],
        vec![
            pf(1000, COL_CAT, "cat_trunc", Transform::Truncate(2)),
            pf(1001, COL_DAY, "month", Transform::Month),
        ],
        vec![pf(1000, COL_AMOUNT, "amount", Transform::Identity)],
        vec![
            pf(1000, COL_CAT, "cat_bucket", Transform::Bucket(4)),
            pf(1001, COL_PRICE, "price", Transform::Identity),
        ],
        vec![
            pf(1000, COL_DAY, "year", Transform::Year),
            pf(1001, COL_PRICE, "price_trunc", Transform::Truncate(500)),
        ],
        vec![],
        vec![
            pf(1000, COL_ID, "voided", Transform::Void),
            pf(1001, COL_N, "n", Transform::Identity),
        ],
    ]
}

// ---- truth: row-level evaluation ----

fn cmp_matches(op: CompareOp, value: &Datum, literal: &Datum) -> bool {
    use std::cmp::Ordering;
    if value.is_nan() {
        // IEEE semantics: NaN fails every ordered comparison and eq;
        // not-eq holds.
        return op == CompareOp::NotEq;
    }
    match op {
        CompareOp::StartsWith | CompareOp::NotStartsWith => match (value, literal) {
            (Datum::String(v), Datum::String(p)) => {
                let starts = v.as_bytes().starts_with(p.as_bytes());
                (op == CompareOp::StartsWith) == starts
            }
            _ => false,
        },
        _ => {
            let Some(ord) = value.partial_cmp_same_type(literal) else {
                return false;
            };
            match op {
                CompareOp::Lt => ord == Ordering::Less,
                CompareOp::LtEq => ord != Ordering::Greater,
                CompareOp::Gt => ord == Ordering::Greater,
                CompareOp::GtEq => ord != Ordering::Less,
                CompareOp::Eq => ord == Ordering::Equal,
                CompareOp::NotEq => ord != Ordering::Equal,
                CompareOp::StartsWith | CompareOp::NotStartsWith => false,
            }
        }
    }
}

fn row_matches(pred: &BoundPredicate, row: &Row) -> bool {
    match pred {
        BoundPredicate::True => true,
        BoundPredicate::False => false,
        BoundPredicate::And(l, r) => row_matches(l, row) && row_matches(r, row),
        BoundPredicate::Or(l, r) => row_matches(l, row) || row_matches(r, row),
        BoundPredicate::Unary { op, term } => {
            let value = row.values.get(&term.field_id).and_then(Clone::clone);
            match op {
                UnaryOp::IsNull => value.is_none(),
                UnaryOp::NotNull => value.is_some(),
                UnaryOp::IsNan => value.as_ref().is_some_and(Datum::is_nan),
                UnaryOp::NotNan => value.as_ref().is_some_and(|v| !v.is_nan()),
            }
        }
        BoundPredicate::Comparison { op, term, literal } => {
            match row.values.get(&term.field_id).and_then(Clone::clone) {
                None => false, // null satisfies no comparison
                Some(value) => cmp_matches(*op, &value, literal),
            }
        }
        BoundPredicate::Set { op, term, literals } => {
            match row.values.get(&term.field_id).and_then(Clone::clone) {
                None => false, // null is in no set, and excluded from none
                Some(value) => {
                    let found = !value.is_nan()
                        && literals.iter().any(|l| {
                            value.partial_cmp_same_type(l) == Some(std::cmp::Ordering::Equal)
                        });
                    match op {
                        SetOp::In => found,
                        SetOp::NotIn => !found,
                    }
                }
            }
        }
    }
}

// ---- building files honestly from rows ----

fn tuple_for_row(row: &Row, types: &[PartitionFieldType]) -> PartitionTuple {
    PartitionTuple {
        fields: types
            .iter()
            .map(|pt| PartitionValue {
                field_id: pt.field_id,
                name: pt.name.clone(),
                value: row
                    .values
                    .get(&pt.source_id)
                    .and_then(Clone::clone)
                    .and_then(|v| apply_transform(&pt.transform, &v)),
            })
            .collect(),
    }
}

fn data_file_for(rows: &[Row], partition: PartitionTuple) -> DataFile {
    let mut value_counts = BTreeMap::new();
    let mut null_counts = BTreeMap::new();
    let mut nan_counts = BTreeMap::new();
    let mut lower: BTreeMap<i32, Datum> = BTreeMap::new();
    let mut upper: BTreeMap<i32, Datum> = BTreeMap::new();
    for row in rows {
        for (col, value) in &row.values {
            *value_counts.entry(*col).or_insert(0i64) += 1;
            match value {
                None => *null_counts.entry(*col).or_insert(0i64) += 1,
                Some(v) if v.is_nan() => {
                    *nan_counts.entry(*col).or_insert(0i64) += 1;
                }
                Some(v) => {
                    nan_counts.entry(*col).or_insert(0);
                    null_counts.entry(*col).or_insert(0);
                    let replace_lower = lower.get(col).is_none_or(|cur| {
                        v.partial_cmp_same_type(cur) == Some(std::cmp::Ordering::Less)
                    });
                    if replace_lower {
                        lower.insert(*col, v.clone());
                    }
                    let replace_upper = upper.get(col).is_none_or(|cur| {
                        v.partial_cmp_same_type(cur) == Some(std::cmp::Ordering::Greater)
                    });
                    if replace_upper {
                        upper.insert(*col, v.clone());
                    }
                }
            }
            null_counts.entry(*col).or_insert(0);
            nan_counts.entry(*col).or_insert(0);
        }
    }
    DataFile {
        content: DataFileContent::Data,
        file_path: "mem://file.parquet".to_owned(),
        file_format: "PARQUET".to_owned(),
        partition,
        record_count: i64::try_from(rows.len()).expect("row count"),
        file_size_in_bytes: 1024,
        column_sizes: None,
        value_counts: Some(value_counts),
        null_value_counts: Some(null_counts),
        nan_value_counts: Some(nan_counts),
        lower_bounds: Some(
            lower
                .iter()
                .map(|(col, v)| (*col, v.to_bound_bytes()))
                .collect(),
        ),
        upper_bounds: Some(
            upper
                .iter()
                .map(|(col, v)| (*col, v.to_bound_bytes()))
                .collect(),
        ),
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

/// Groups rows into files by partition tuple and builds honest manifest
/// field summaries across the files.
fn build_files(
    rows: &[Row],
    types: &[PartitionFieldType],
) -> (Vec<(Vec<Row>, PartitionTuple)>, Vec<FieldSummary>) {
    let mut groups: Vec<(Vec<Row>, PartitionTuple)> = Vec::new();
    for row in rows {
        let tuple = tuple_for_row(row, types);
        match groups.iter_mut().find(|(_, t)| *t == tuple) {
            Some((group_rows, _)) => group_rows.push(row.clone()),
            None => groups.push((vec![row.clone()], tuple)),
        }
    }
    let summaries: Vec<FieldSummary> = types
        .iter()
        .enumerate()
        .map(|(position, _)| {
            let mut summary = FieldSummary {
                contains_null: false,
                contains_nan: Some(false),
                lower_bound: None,
                upper_bound: None,
            };
            let mut lower: Option<Datum> = None;
            let mut upper: Option<Datum> = None;
            for (_, tuple) in &groups {
                match &tuple.fields[position].value {
                    None => summary.contains_null = true,
                    Some(v) if v.is_nan() => summary.contains_nan = Some(true),
                    Some(v) => {
                        if lower.as_ref().is_none_or(|cur| {
                            v.partial_cmp_same_type(cur) == Some(std::cmp::Ordering::Less)
                        }) {
                            lower = Some(v.clone());
                        }
                        if upper.as_ref().is_none_or(|cur| {
                            v.partial_cmp_same_type(cur) == Some(std::cmp::Ordering::Greater)
                        }) {
                            upper = Some(v.clone());
                        }
                    }
                }
            }
            summary.lower_bound = lower.map(|v| v.to_bound_bytes());
            summary.upper_bound = upper.map(|v| v.to_bound_bytes());
            summary
        })
        .collect();
    (groups, summaries)
}

proptest! {
    #![proptest_config(ProptestConfig {
        cases: 512,
        ..ProptestConfig::default()
    })]

    /// The soundness invariant, across all evaluators and partition specs.
    #[test]
    fn matching_rows_are_never_pruned(
        rows in proptest::collection::vec(row_strategy(), 1..24),
        expr in expression_strategy(),
        spec_idx in 0usize..8,
    ) {
        let schema = schema();
        let bound = expr
            .clone()
            .bind(&schema, true)
            .expect("generated expressions bind");
        let spec_fields = &partition_specs()[spec_idx];
        let types = partition_field_types(spec_fields, &schema).expect("typed spec");
        let projected = project(&bound, &types);
        let (files, summaries) = build_files(&rows, &types);

        for (file_rows, tuple) in &files {
            let file = data_file_for(file_rows, tuple.clone());
            let any_match = file_rows.iter().any(|row| row_matches(&bound, row));
            if any_match {
                prop_assert!(
                    file_might_match(&bound, &file),
                    "metrics evaluator pruned a file with a matching row\n\
                     filter: {expr:?}\nfile rows: {file_rows:?}",
                );
                prop_assert!(
                    tuple_might_match(&projected, tuple),
                    "partition-tuple evaluator pruned a file with a matching row\n\
                     filter: {expr:?}\nspec: {spec_fields:?}\ntuple: {tuple:?}\nrows: {file_rows:?}",
                );
                prop_assert!(
                    summaries_might_match(&projected, &summaries, &types),
                    "summary evaluator pruned a manifest with a matching row\n\
                     filter: {expr:?}\nspec: {spec_fields:?}\nsummaries: {summaries:?}",
                );
            }
        }
    }

    /// Bound-value serialization round-trips for every generated datum
    /// (bounds are decoded through the same column types the evaluators
    /// use).
    #[test]
    fn bound_bytes_round_trip(row in row_strategy()) {
        let schema = schema();
        for field in &schema.fields {
            let Type::Primitive(ty) = &field.field_type else { continue };
            if let Some(Some(datum)) = row.values.get(&field.id).cloned() {
                if datum.is_nan() {
                    continue; // NaN is never written as a bound
                }
                let bytes = datum.to_bound_bytes();
                let back = Datum::from_bound_bytes(ty, &bytes).expect("decode");
                prop_assert_eq!(back, datum);
            }
        }
    }
}
