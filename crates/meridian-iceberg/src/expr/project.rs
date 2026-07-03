//! Inclusive projection: converting a bound row predicate into a
//! predicate on partition tuples, per the spec's Scan Planning section
//! ("if a scan predicate matches a row, the partition predicate must
//! match that row's partition").
//!
//! # Transform support matrix
//!
//! What projects, per transform (everything else degrades to **keep** —
//! the projection contributes `true`, never pruning):
//!
//! | transform | projects | notes |
//! |---|---|---|
//! | `identity` | every operator, unchanged | partition value *is* the row value |
//! | `bucket[N]` | `eq`, `in`, `is-null`, `not-null` | other operators tell nothing about a hash bucket |
//! | `truncate[W]` | `eq` -> `eq`; `in` -> `in`; `lt`/`lt-eq` -> `lt-eq`; `gt`/`gt-eq` -> `gt-eq` (boundaries tightened by one unit for int/long/decimal); `starts-with` (string: prefix shorter than `W` stays `starts-with`, else `eq` of the truncated prefix); `is-null`, `not-null` | monotone, so range bounds carry over |
//! | `year`/`month`/`day`/`hour` | `eq` -> `eq`; `in` -> `in`; `lt`/`lt-eq` -> `lt-eq`; `gt`/`gt-eq` -> `gt-eq` (boundaries tightened by one microsecond/day where representable); `is-null`, `not-null` | monotone on the time line |
//! | `void` | nothing | the partition value is always null |
//! | unrecognized | nothing | spec: "the inclusive projection for an unknown partition transform is true" |
//!
//! `not-eq`, `not-in`, and `not-starts-with` never project through a
//! non-identity transform: two different row values can share a partition
//! value, so the partition tells nothing about what the file *excludes*.
//! `is-nan`/`not-nan` project only through `identity` (no other transform
//! accepts float/double sources).
//!
//! A filter term that carries its own transform (`TransformTerm`)
//! projects when a partition field applies the *same* transform to the
//! same source column; the projected predicate then compares partition
//! values directly (identity semantics on the transform's result).

use crate::manifest::PartitionFieldType;
use crate::spec::Transform;
use crate::value::Datum;

use super::transforms;
use super::{BoundPredicate, BoundTerm, CompareOp, SetOp, UnaryOp};

/// A predicate over partition tuples, produced by [`project`]. Leaf nodes
/// address partition fields by position in the spec (aligned with
/// manifest-list [`FieldSummary`](crate::manifest::FieldSummary) order)
/// and by partition field id (for tuple lookups).
#[derive(Debug, Clone)]
pub enum PartitionPredicate {
    /// Always matches (no pruning power).
    True,
    /// Never matches.
    False,
    /// Both must match.
    And(Box<PartitionPredicate>, Box<PartitionPredicate>),
    /// Either must match.
    Or(Box<PartitionPredicate>, Box<PartitionPredicate>),
    /// A null/NaN test of a partition value.
    Unary {
        /// Position in the partition spec's field list.
        position: usize,
        /// Partition field id.
        field_id: i32,
        /// The operator.
        op: UnaryOp,
    },
    /// A comparison of a partition value against a literal (already in
    /// the partition field's *result* type).
    Comparison {
        /// Position in the partition spec's field list.
        position: usize,
        /// Partition field id.
        field_id: i32,
        /// The operator.
        op: CompareOp,
        /// The literal, in result-type space.
        literal: Datum,
    },
    /// A set membership test of a partition value.
    Set {
        /// Position in the partition spec's field list.
        position: usize,
        /// Partition field id.
        field_id: i32,
        /// The operator.
        op: SetOp,
        /// The literals, in result-type space.
        literals: Vec<Datum>,
    },
}

/// Projects a bound predicate onto a partition spec (the spec of the
/// manifest being pruned, not necessarily the table's current one).
///
/// The result is *inclusive*: any partition holding a matching row
/// satisfies it. Non-projectable pieces become [`PartitionPredicate::True`].
#[must_use]
pub fn project(
    pred: &BoundPredicate,
    partition_types: &[PartitionFieldType],
) -> PartitionPredicate {
    match pred {
        BoundPredicate::True => PartitionPredicate::True,
        BoundPredicate::False => PartitionPredicate::False,
        BoundPredicate::And(l, r) => and(project(l, partition_types), project(r, partition_types)),
        BoundPredicate::Or(l, r) => or(project(l, partition_types), project(r, partition_types)),
        BoundPredicate::Unary { op, term } => {
            project_leaf(partition_types, term, |position, pt| {
                project_unary(position, pt, *op, term)
            })
        }
        BoundPredicate::Comparison { op, term, literal } => {
            project_leaf(partition_types, term, |position, pt| {
                project_comparison(position, pt, *op, literal, term)
            })
        }
        BoundPredicate::Set { op, term, literals } => {
            project_leaf(partition_types, term, |position, pt| {
                project_set(position, pt, *op, literals, term)
            })
        }
    }
}

fn and(l: PartitionPredicate, r: PartitionPredicate) -> PartitionPredicate {
    match (l, r) {
        (PartitionPredicate::False, _) | (_, PartitionPredicate::False) => {
            PartitionPredicate::False
        }
        (PartitionPredicate::True, other) | (other, PartitionPredicate::True) => other,
        (l, r) => PartitionPredicate::And(Box::new(l), Box::new(r)),
    }
}

fn or(l: PartitionPredicate, r: PartitionPredicate) -> PartitionPredicate {
    match (l, r) {
        (PartitionPredicate::True, _) | (_, PartitionPredicate::True) => PartitionPredicate::True,
        (PartitionPredicate::False, other) | (other, PartitionPredicate::False) => other,
        (l, r) => PartitionPredicate::Or(Box::new(l), Box::new(r)),
    }
}

/// Projects one predicate leaf through *every* partition field sourced
/// from the leaf's column (a column may feed several partition fields;
/// each inclusive projection must hold, so they conjoin).
fn project_leaf(
    partition_types: &[PartitionFieldType],
    term: &BoundTerm,
    mut per_field: impl FnMut(usize, &PartitionFieldType) -> PartitionPredicate,
) -> PartitionPredicate {
    let mut acc = PartitionPredicate::True;
    for (position, pt) in partition_types.iter().enumerate() {
        if pt.source_id != term.field_id {
            continue;
        }
        acc = and(acc, per_field(position, pt));
    }
    acc
}

/// Whether a filter term (possibly transform-carrying) can be compared
/// directly against this partition field's stored values: either the
/// term is a bare reference (project through `pt.transform`), or the
/// term's transform equals the partition field's transform (identity
/// semantics on the result).
enum LeafMode {
    /// Bare reference: project source-space literals through the field's
    /// transform.
    ThroughTransform,
    /// Transform term matching the partition transform: literals are
    /// already in result space; compare directly.
    Direct,
    /// Not projectable.
    Keep,
}

fn leaf_mode(pt: &PartitionFieldType, term: &BoundTerm) -> LeafMode {
    match &term.transform {
        None => LeafMode::ThroughTransform,
        Some(t) if *t == pt.transform && pt.transform.is_recognized() => LeafMode::Direct,
        Some(_) => LeafMode::Keep,
    }
}

fn project_unary(
    position: usize,
    pt: &PartitionFieldType,
    op: UnaryOp,
    term: &BoundTerm,
) -> PartitionPredicate {
    let mode = leaf_mode(pt, term);
    match (op, mode) {
        // All transforms map null -> null, so null tests carry over both
        // for bare references and for matching transform terms. void maps
        // *everything* to null and unknown transforms promise nothing:
        // neither projects.
        (UnaryOp::IsNull | UnaryOp::NotNull, LeafMode::ThroughTransform | LeafMode::Direct)
            if !matches!(pt.transform, Transform::Void | Transform::Other(_)) =>
        {
            PartitionPredicate::Unary {
                position,
                field_id: pt.field_id,
                op,
            }
        }
        // NaN tests only make sense where the partition value is the row
        // value (identity over float/double).
        (UnaryOp::IsNan | UnaryOp::NotNan, LeafMode::ThroughTransform)
            if pt.transform == Transform::Identity =>
        {
            PartitionPredicate::Unary {
                position,
                field_id: pt.field_id,
                op,
            }
        }
        _ => PartitionPredicate::True,
    }
}

fn project_comparison(
    position: usize,
    pt: &PartitionFieldType,
    op: CompareOp,
    literal: &Datum,
    term: &BoundTerm,
) -> PartitionPredicate {
    let cmp = |op: CompareOp, literal: Datum| PartitionPredicate::Comparison {
        position,
        field_id: pt.field_id,
        op,
        literal,
    };
    match leaf_mode(pt, term) {
        LeafMode::Keep => PartitionPredicate::True,
        // The literal is already a partition (result-space) value; the
        // partition field applies the same transform, so partition value
        // semantics are identity over the result space — every operator
        // carries over unchanged.
        LeafMode::Direct => cmp(op, literal.clone()),
        LeafMode::ThroughTransform => match &pt.transform {
            Transform::Identity => cmp(op, literal.clone()),
            Transform::Void | Transform::Other(_) => PartitionPredicate::True,
            Transform::Bucket(_) => match op {
                CompareOp::Eq => match transforms::apply(&pt.transform, literal) {
                    Some(bucketed) => cmp(CompareOp::Eq, bucketed),
                    None => PartitionPredicate::True,
                },
                _ => PartitionPredicate::True,
            },
            Transform::Truncate(_)
            | Transform::Year
            | Transform::Month
            | Transform::Day
            | Transform::Hour => project_monotone(position, pt, op, literal),
        },
    }
}

/// Projects a comparison through a monotone transform (`truncate` and
/// the temporal family): `lt`/`lt-eq` become `lt-eq` of the transformed
/// boundary, `gt`/`gt-eq` become `gt-eq`, `eq` stays `eq`. Strict bounds
/// are tightened by one representable unit first when the source type
/// has one (int/long/decimal/date/time/timestamps), matching the
/// reference implementation's pruning power; if the boundary cannot be
/// adjusted (overflow, or continuous types like string) the untightened
/// — still sound — form is used.
fn project_monotone(
    position: usize,
    pt: &PartitionFieldType,
    op: CompareOp,
    literal: &Datum,
) -> PartitionPredicate {
    let apply = |value: &Datum| transforms::apply(&pt.transform, value);
    let cmp = |op: CompareOp, literal: Datum| PartitionPredicate::Comparison {
        position,
        field_id: pt.field_id,
        op,
        literal,
    };
    match op {
        CompareOp::Eq => match apply(literal) {
            Some(projected) => cmp(CompareOp::Eq, projected),
            None => PartitionPredicate::True,
        },
        CompareOp::Lt => {
            let boundary = transforms::predecessor(literal).unwrap_or_else(|| literal.clone());
            match apply(&boundary) {
                Some(projected) => cmp(CompareOp::LtEq, projected),
                None => PartitionPredicate::True,
            }
        }
        CompareOp::LtEq => match apply(literal) {
            Some(projected) => cmp(CompareOp::LtEq, projected),
            None => PartitionPredicate::True,
        },
        CompareOp::Gt => {
            let boundary = transforms::successor(literal).unwrap_or_else(|| literal.clone());
            match apply(&boundary) {
                Some(projected) => cmp(CompareOp::GtEq, projected),
                None => PartitionPredicate::True,
            }
        }
        CompareOp::GtEq => match apply(literal) {
            Some(projected) => cmp(CompareOp::GtEq, projected),
            None => PartitionPredicate::True,
        },
        CompareOp::StartsWith => match (&pt.transform, literal) {
            (Transform::Truncate(w), Datum::String(prefix)) => {
                let width = usize::try_from(*w).unwrap_or(usize::MAX);
                if prefix.chars().count() < width {
                    // Partition values are longer prefixes of the rows, so
                    // they still start with the filter prefix.
                    cmp(CompareOp::StartsWith, literal.clone())
                } else {
                    // Rows starting with the prefix truncate to exactly
                    // the prefix's own truncation.
                    match apply(literal) {
                        Some(projected) => cmp(CompareOp::Eq, projected),
                        None => PartitionPredicate::True,
                    }
                }
            }
            _ => PartitionPredicate::True,
        },
        // Excluding operators tell nothing through a many-to-one
        // transform.
        CompareOp::NotEq | CompareOp::NotStartsWith => PartitionPredicate::True,
    }
}

fn project_set(
    position: usize,
    pt: &PartitionFieldType,
    op: SetOp,
    literals: &[Datum],
    term: &BoundTerm,
) -> PartitionPredicate {
    let set = |literals: Vec<Datum>| PartitionPredicate::Set {
        position,
        field_id: pt.field_id,
        op: SetOp::In,
        literals,
    };
    match (op, leaf_mode(pt, term)) {
        (_, LeafMode::Keep) => PartitionPredicate::True,
        (SetOp::In, LeafMode::Direct) => set(literals.to_vec()),
        (SetOp::NotIn, LeafMode::Direct) => PartitionPredicate::Set {
            position,
            field_id: pt.field_id,
            op: SetOp::NotIn,
            literals: literals.to_vec(),
        },
        (SetOp::In, LeafMode::ThroughTransform) => match &pt.transform {
            Transform::Identity => set(literals.to_vec()),
            Transform::Void | Transform::Other(_) => PartitionPredicate::True,
            transform => {
                let projected: Option<Vec<Datum>> = literals
                    .iter()
                    .map(|l| transforms::apply(transform, l))
                    .collect();
                match projected {
                    Some(values) => set(values),
                    None => PartitionPredicate::True,
                }
            }
        },
        (SetOp::NotIn, LeafMode::ThroughTransform) => match &pt.transform {
            // Identity partitions hold the row value itself, so exclusion
            // carries over; through any other transform it tells nothing.
            Transform::Identity => PartitionPredicate::Set {
                position,
                field_id: pt.field_id,
                op: SetOp::NotIn,
                literals: literals.to_vec(),
            },
            _ => PartitionPredicate::True,
        },
    }
}
