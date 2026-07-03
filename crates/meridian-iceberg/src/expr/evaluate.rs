//! Three-valued ("might match") evaluators. Every function in this module
//! answers *"could this container hold a row matching the predicate?"* —
//! `true` keeps the container, `false` prunes it, and anything unknown
//! (missing statistics, undecodable bounds, incomparable values,
//! transform terms the evaluator does not model) resolves to `true`.
//! Soundness invariant: a container holding a matching row is never
//! pruned.

use std::cmp::Ordering;

use crate::manifest::{DataFile, FieldSummary, PartitionFieldType, PartitionTuple};
use crate::value::Datum;

use super::project::PartitionPredicate;
use super::{BoundPredicate, CompareOp, SetOp, UnaryOp};

/// Evaluates a bound predicate against a data file's column statistics
/// (value/null/NaN counts and lower/upper bounds). Mirrors the reference
/// implementation's inclusive metrics evaluator; predicates over
/// transform terms are kept (documented gap: no bound-transform
/// evaluation).
#[must_use]
pub fn file_might_match(pred: &BoundPredicate, file: &DataFile) -> bool {
    match pred {
        BoundPredicate::True => true,
        BoundPredicate::False => false,
        BoundPredicate::And(l, r) => file_might_match(l, file) && file_might_match(r, file),
        BoundPredicate::Or(l, r) => file_might_match(l, file) || file_might_match(r, file),
        BoundPredicate::Unary { op, term } => {
            if term.transform.is_some() {
                return true;
            }
            let id = term.field_id;
            match op {
                UnaryOp::IsNull => may_contain_null(file, id),
                UnaryOp::NotNull => !contains_nulls_only(file, id),
                UnaryOp::IsNan => nan_count(file, id) != Some(0) && !contains_nulls_only(file, id),
                UnaryOp::NotNan => !contains_nans_only(file, id),
            }
        }
        BoundPredicate::Comparison { op, term, literal } => {
            if term.transform.is_some() {
                return true;
            }
            // `not-eq` cannot be answered with min/max bounds at all.
            if *op == CompareOp::NotEq {
                return true;
            }
            let id = term.field_id;
            if *op == CompareOp::NotStartsWith {
                return not_starts_with_might_match(file, term, literal);
            }
            // The remaining operators match only non-null, non-NaN rows.
            if contains_nulls_only(file, id) || contains_nans_only(file, id) {
                return false;
            }
            if literal.is_nan() {
                return true; // engines should never send NaN literals
            }
            let lower = decode_or_unknown(file.lower_bound(id, &term.field_type));
            let upper = decode_or_unknown(file.upper_bound(id, &term.field_type));
            match op {
                CompareOp::Lt => cmp_keep(lower.as_ref(), literal, |o| o == Ordering::Less),
                CompareOp::LtEq => cmp_keep(lower.as_ref(), literal, |o| o != Ordering::Greater),
                CompareOp::Gt => {
                    cmp_keep_upper(upper.as_ref(), literal, |o| o == Ordering::Greater)
                }
                CompareOp::GtEq => cmp_keep_upper(upper.as_ref(), literal, |o| o != Ordering::Less),
                CompareOp::Eq => {
                    cmp_keep(lower.as_ref(), literal, |o| o != Ordering::Greater)
                        && cmp_keep_upper(upper.as_ref(), literal, |o| o != Ordering::Less)
                }
                CompareOp::StartsWith => {
                    starts_with_might_match(lower.as_ref(), upper.as_ref(), literal)
                }
                CompareOp::NotEq | CompareOp::NotStartsWith => true,
            }
        }
        BoundPredicate::Set { op, term, literals } => {
            if term.transform.is_some() || *op == SetOp::NotIn {
                return true;
            }
            let id = term.field_id;
            if contains_nulls_only(file, id) || contains_nans_only(file, id) {
                return false;
            }
            let lower = decode_or_unknown(file.lower_bound(id, &term.field_type));
            let upper = decode_or_unknown(file.upper_bound(id, &term.field_type));
            literals.iter().any(|literal| {
                !literal.is_nan()
                    && cmp_keep(lower.as_ref(), literal, |o| o != Ordering::Greater)
                    && cmp_keep_upper(upper.as_ref(), literal, |o| o != Ordering::Less)
            })
        }
    }
}

/// `lower` compared to the literal must *possibly* satisfy `keep_when`
/// for some row: with `lower <op> literal` false for the minimum, rows
/// below don't exist. Missing/undecodable/NaN bounds keep.
fn cmp_keep(
    lower: Option<&Datum>,
    literal: &Datum,
    lower_may_satisfy: impl Fn(Ordering) -> bool,
) -> bool {
    match lower {
        None => true,
        Some(l) if l.is_nan() => true, // unreliable bound
        Some(l) => match l.partial_cmp_same_type(literal) {
            None => true,
            Some(ord) => lower_may_satisfy(ord),
        },
    }
}

fn cmp_keep_upper(
    upper: Option<&Datum>,
    literal: &Datum,
    upper_may_satisfy: impl Fn(Ordering) -> bool,
) -> bool {
    match upper {
        None => true,
        Some(u) if u.is_nan() => true,
        Some(u) => match u.partial_cmp_same_type(literal) {
            None => true,
            Some(ord) => upper_may_satisfy(ord),
        },
    }
}

fn starts_with_might_match(lower: Option<&Datum>, upper: Option<&Datum>, literal: &Datum) -> bool {
    let Datum::String(prefix) = literal else {
        return true;
    };
    let p = prefix.as_bytes();
    if let Some(Datum::String(l)) = lower {
        let l = l.as_bytes();
        let n = l.len().min(p.len());
        if l[..n] > p[..n] {
            return false; // every row is above every string with this prefix
        }
    }
    if let Some(Datum::String(u)) = upper {
        let u = u.as_bytes();
        let n = u.len().min(p.len());
        if u[..n] < p[..n] {
            return false; // every row is below every string with this prefix
        }
    }
    true
}

fn not_starts_with_might_match(file: &DataFile, term: &super::BoundTerm, literal: &Datum) -> bool {
    if may_contain_null(file, term.field_id) {
        return true;
    }
    let Datum::String(prefix) = literal else {
        return true;
    };
    let p = prefix.as_bytes();
    let lower = decode_or_unknown(file.lower_bound(term.field_id, &term.field_type));
    let upper = decode_or_unknown(file.upper_bound(term.field_id, &term.field_type));
    match (lower, upper) {
        (Some(Datum::String(l)), Some(Datum::String(u))) => {
            let l = l.as_bytes();
            let u = u.as_bytes();
            // If both bounds start with the prefix, every row in between
            // does too, and no row can match not-starts-with.
            !(l.len() >= p.len() && &l[..p.len()] == p && u.len() >= p.len() && &u[..p.len()] == p)
        }
        _ => true,
    }
}

fn decode_or_unknown(res: Result<Option<Datum>, crate::value::ValueError>) -> Option<Datum> {
    res.unwrap_or(None)
}

fn value_count(file: &DataFile, id: i32) -> Option<i64> {
    file.value_counts.as_ref().and_then(|m| m.get(&id)).copied()
}

fn null_count(file: &DataFile, id: i32) -> Option<i64> {
    file.null_value_counts
        .as_ref()
        .and_then(|m| m.get(&id))
        .copied()
}

fn nan_count(file: &DataFile, id: i32) -> Option<i64> {
    file.nan_value_counts
        .as_ref()
        .and_then(|m| m.get(&id))
        .copied()
}

fn may_contain_null(file: &DataFile, id: i32) -> bool {
    null_count(file, id) != Some(0)
}

fn contains_nulls_only(file: &DataFile, id: i32) -> bool {
    matches!(
        (value_count(file, id), null_count(file, id)),
        (Some(values), Some(nulls)) if values == nulls
    )
}

fn contains_nans_only(file: &DataFile, id: i32) -> bool {
    matches!(
        (value_count(file, id), nan_count(file, id)),
        (Some(values), Some(nans)) if values == nans
    )
}

/// Evaluates a projected partition predicate against a concrete partition
/// tuple (exact evaluation with SQL null semantics: a null partition
/// value satisfies only `is-null`). Fields absent from the tuple resolve
/// to *unknown* (keep).
#[must_use]
pub fn tuple_might_match(pred: &PartitionPredicate, tuple: &PartitionTuple) -> bool {
    match pred {
        PartitionPredicate::True => true,
        PartitionPredicate::False => false,
        PartitionPredicate::And(l, r) => tuple_might_match(l, tuple) && tuple_might_match(r, tuple),
        PartitionPredicate::Or(l, r) => tuple_might_match(l, tuple) || tuple_might_match(r, tuple),
        PartitionPredicate::Unary { field_id, op, .. } => match tuple.get(*field_id) {
            None => true,
            Some(None) => matches!(op, UnaryOp::IsNull),
            Some(Some(v)) => match op {
                UnaryOp::IsNull => false,
                UnaryOp::NotNull => true,
                UnaryOp::IsNan => v.is_nan(),
                UnaryOp::NotNan => !v.is_nan(),
            },
        },
        PartitionPredicate::Comparison {
            field_id,
            op,
            literal,
            ..
        } => match tuple.get(*field_id) {
            None => true,
            Some(None) => false, // null satisfies no comparison
            Some(Some(value)) => match op {
                CompareOp::StartsWith | CompareOp::NotStartsWith => match (value, literal) {
                    (Datum::String(v), Datum::String(p)) => {
                        let starts = v.as_bytes().starts_with(p.as_bytes());
                        if *op == CompareOp::StartsWith {
                            starts
                        } else {
                            !starts
                        }
                    }
                    _ => true,
                },
                _ => match value.partial_cmp_same_type(literal) {
                    None => true,
                    Some(ord) => match op {
                        CompareOp::Lt => ord == Ordering::Less,
                        CompareOp::LtEq => ord != Ordering::Greater,
                        CompareOp::Gt => ord == Ordering::Greater,
                        CompareOp::GtEq => ord != Ordering::Less,
                        CompareOp::Eq => ord == Ordering::Equal,
                        CompareOp::NotEq => ord != Ordering::Equal,
                        CompareOp::StartsWith | CompareOp::NotStartsWith => true,
                    },
                },
            },
        },
        PartitionPredicate::Set {
            field_id,
            op,
            literals,
            ..
        } => match tuple.get(*field_id) {
            None => true,
            Some(None) => false, // null is in no set and excluded from none
            Some(Some(value)) => {
                let mut unknown = false;
                let mut found = false;
                for literal in literals {
                    match value.partial_cmp_same_type(literal) {
                        None => unknown = true,
                        Some(Ordering::Equal) => found = true,
                        Some(_) => {}
                    }
                }
                match op {
                    SetOp::In => found || unknown,
                    SetOp::NotIn => !found,
                }
            }
        },
    }
}

/// Evaluates a projected partition predicate against a manifest-list
/// entry's partition field summaries, to skip whole manifests.
/// `partition_types` supplies the result type used to decode summary
/// bounds; it must describe the same spec the predicate was projected
/// onto.
#[must_use]
pub fn summaries_might_match(
    pred: &PartitionPredicate,
    summaries: &[FieldSummary],
    partition_types: &[PartitionFieldType],
) -> bool {
    match pred {
        PartitionPredicate::True => true,
        PartitionPredicate::False => false,
        PartitionPredicate::And(l, r) => {
            summaries_might_match(l, summaries, partition_types)
                && summaries_might_match(r, summaries, partition_types)
        }
        PartitionPredicate::Or(l, r) => {
            summaries_might_match(l, summaries, partition_types)
                || summaries_might_match(r, summaries, partition_types)
        }
        PartitionPredicate::Unary { position, op, .. } => {
            let Some(summary) = summaries.get(*position) else {
                return true;
            };
            match op {
                UnaryOp::IsNull => summary.contains_null,
                // No bound and provably no NaN means every value is null.
                UnaryOp::NotNull => {
                    !(summary.lower_bound.is_none() && summary.contains_nan == Some(false))
                }
                UnaryOp::IsNan => summary.contains_nan != Some(false),
                // Provably all NaN: contains NaN, no nulls, no bounds.
                UnaryOp::NotNan => {
                    !(summary.contains_nan == Some(true)
                        && !summary.contains_null
                        && summary.lower_bound.is_none())
                }
            }
        }
        PartitionPredicate::Comparison {
            position,
            op,
            literal,
            ..
        } => {
            let (Some(summary), Some(pt)) =
                (summaries.get(*position), partition_types.get(*position))
            else {
                return true;
            };
            if matches!(op, CompareOp::NotEq | CompareOp::NotStartsWith) {
                return true;
            }
            // These operators match only non-null, non-NaN values; no
            // bounds means the field holds nothing comparable.
            if summary.lower_bound.is_none() && summary.upper_bound.is_none() {
                return false;
            }
            if literal.is_nan() {
                return true;
            }
            let lower = decode_or_unknown(summary.lower(&pt.result_type));
            let upper = decode_or_unknown(summary.upper(&pt.result_type));
            match op {
                CompareOp::Lt => cmp_keep(lower.as_ref(), literal, |o| o == Ordering::Less),
                CompareOp::LtEq => cmp_keep(lower.as_ref(), literal, |o| o != Ordering::Greater),
                CompareOp::Gt => {
                    cmp_keep_upper(upper.as_ref(), literal, |o| o == Ordering::Greater)
                }
                CompareOp::GtEq => cmp_keep_upper(upper.as_ref(), literal, |o| o != Ordering::Less),
                CompareOp::Eq => {
                    cmp_keep(lower.as_ref(), literal, |o| o != Ordering::Greater)
                        && cmp_keep_upper(upper.as_ref(), literal, |o| o != Ordering::Less)
                }
                CompareOp::StartsWith => {
                    starts_with_might_match(lower.as_ref(), upper.as_ref(), literal)
                }
                CompareOp::NotEq | CompareOp::NotStartsWith => true,
            }
        }
        PartitionPredicate::Set {
            position,
            op,
            literals,
            ..
        } => {
            let (Some(summary), Some(pt)) =
                (summaries.get(*position), partition_types.get(*position))
            else {
                return true;
            };
            if *op == SetOp::NotIn {
                return true;
            }
            if summary.lower_bound.is_none() && summary.upper_bound.is_none() {
                return false;
            }
            let lower = decode_or_unknown(summary.lower(&pt.result_type));
            let upper = decode_or_unknown(summary.upper(&pt.result_type));
            literals.iter().any(|literal| {
                !literal.is_nan()
                    && cmp_keep(lower.as_ref(), literal, |o| o != Ordering::Greater)
                    && cmp_keep_upper(upper.as_ref(), literal, |o| o != Ordering::Less)
            })
        }
    }
}
