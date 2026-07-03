//! Column statistics for the rewritten output: Appendix-D lower/upper bounds
//! computed directly from the merged (post-delete) Arrow data.
//!
//! Bounds are computed from the *output* rows, not carried from the input
//! footers — after deletes, the input bounds may be looser than the truth,
//! and looser bounds are always safe but tighter ones prune better. Computing
//! from the merged batch gives exact bounds for the rows actually written.
//!
//! Only the primitive Arrow types with an unambiguous Iceberg single-value
//! encoding are covered; a column of any other type simply gets no bound
//! (absent bounds are spec-legal — a reader treats them as "unknown"). This
//! is a deliberate, honest partial: it never emits a *wrong* bound, and the
//! record/value/null counts (computed exactly in [`crate::rewrite`]) are what
//! most planning relies on.

use std::collections::BTreeMap;

use arrow_array::cast::AsArray;
use arrow_array::types::{
    Date32Type, Float32Type, Float64Type, Int32Type, Int64Type, TimestampMicrosecondType,
};
use arrow_array::{Array, ArrayRef};
use arrow_schema::{DataType, Schema as ArrowSchema, TimeUnit};
use meridian_iceberg::value::Datum;

use crate::arrow_schema::{TopLevelField, field_id_of};

/// Computes lower/upper bound maps (field id → Appendix-D bytes) for every
/// primitive column of the merged batch that has at least one non-null value.
#[must_use]
pub fn compute_bounds(
    batch: &arrow_array::RecordBatch,
    schema: &ArrowSchema,
    target_fields: &BTreeMap<i32, TopLevelField>,
) -> (BTreeMap<i32, Vec<u8>>, BTreeMap<i32, Vec<u8>>) {
    let mut lower = BTreeMap::new();
    let mut upper = BTreeMap::new();
    for (idx, field) in schema.fields().iter().enumerate() {
        let Some(id) = field_id_of(field) else {
            continue;
        };
        // Only primitive columns get bounds.
        if !target_fields.get(&id).is_some_and(|f| f.is_primitive) {
            continue;
        }
        if let Some((lo, hi)) = min_max_bounds(batch.column(idx)) {
            lower.insert(id, lo);
            upper.insert(id, hi);
        }
    }
    (lower, upper)
}

/// The Appendix-D encoded (min, max) of an Arrow column, or `None` when the
/// column is all-null or of a type we do not bound. Each arm computes the
/// min/max over non-null (and, for floats, non-NaN) values, then encodes them
/// as the matching `Datum`. This is a type-dispatch table — one arm per Arrow
/// physical type — so it is long by nature; splitting it would scatter the
/// dispatch without making any single arm clearer.
#[allow(clippy::too_many_lines)]
fn min_max_bounds(array: &ArrayRef) -> Option<(Vec<u8>, Vec<u8>)> {
    match array.data_type() {
        DataType::Boolean => {
            let a = array.as_boolean();
            let (lo, hi) = fold_min_max(a.len(), |i| (!a.is_null(i)).then(|| a.value(i)))?;
            Some((
                Datum::Boolean(lo).to_bound_bytes(),
                Datum::Boolean(hi).to_bound_bytes(),
            ))
        }
        DataType::Int32 => {
            let a = array.as_primitive::<Int32Type>();
            Some(encode(
                ord_min_max(a.len(), |i| (!a.is_null(i)).then(|| a.value(i)))?,
                Datum::Int,
            ))
        }
        DataType::Date32 => {
            let a = array.as_primitive::<Date32Type>();
            Some(encode(
                ord_min_max(a.len(), |i| (!a.is_null(i)).then(|| a.value(i)))?,
                Datum::Date,
            ))
        }
        DataType::Int64 => {
            let a = array.as_primitive::<Int64Type>();
            Some(encode(
                ord_min_max(a.len(), |i| (!a.is_null(i)).then(|| a.value(i)))?,
                Datum::Long,
            ))
        }
        DataType::Timestamp(TimeUnit::Microsecond, tz) => {
            let a = array.as_primitive::<TimestampMicrosecondType>();
            let ctor = if tz.is_some() {
                Datum::Timestamptz
            } else {
                Datum::Timestamp
            };
            Some(encode(
                ord_min_max(a.len(), |i| (!a.is_null(i)).then(|| a.value(i)))?,
                ctor,
            ))
        }
        DataType::Float32 => {
            let a = array.as_primitive::<Float32Type>();
            let (lo, hi) = fold_float(a.len(), |i| (!a.is_null(i)).then(|| a.value(i)))?;
            Some((
                Datum::float(lo).to_bound_bytes(),
                Datum::float(hi).to_bound_bytes(),
            ))
        }
        DataType::Float64 => {
            let a = array.as_primitive::<Float64Type>();
            let (lo, hi) = fold_double(a.len(), |i| (!a.is_null(i)).then(|| a.value(i)))?;
            Some((
                Datum::double(lo).to_bound_bytes(),
                Datum::double(hi).to_bound_bytes(),
            ))
        }
        DataType::Utf8 => {
            let a = array.as_string::<i32>();
            let (lo, hi) = ord_min_max(a.len(), |i| (!a.is_null(i)).then(|| a.value(i)))?;
            Some((
                Datum::String(lo.to_owned()).to_bound_bytes(),
                Datum::String(hi.to_owned()).to_bound_bytes(),
            ))
        }
        DataType::LargeUtf8 => {
            let a = array.as_string::<i64>();
            let (lo, hi) = ord_min_max(a.len(), |i| (!a.is_null(i)).then(|| a.value(i)))?;
            Some((
                Datum::String(lo.to_owned()).to_bound_bytes(),
                Datum::String(hi.to_owned()).to_bound_bytes(),
            ))
        }
        DataType::Binary => {
            let a = array.as_binary::<i32>();
            let (lo, hi) = ord_min_max(a.len(), |i| (!a.is_null(i)).then(|| a.value(i)))?;
            Some((
                Datum::Binary(lo.to_vec()).to_bound_bytes(),
                Datum::Binary(hi.to_vec()).to_bound_bytes(),
            ))
        }
        _ => None,
    }
}

/// Encodes a `(min, max)` pair of the same primitive through `ctor`.
fn encode<T>(pair: (T, T), ctor: fn(T) -> Datum) -> (Vec<u8>, Vec<u8>) {
    (ctor(pair.0).to_bound_bytes(), ctor(pair.1).to_bound_bytes())
}

/// Min/max over `Ord` values yielded by `at(i)` (`None` skips index `i`);
/// `None` overall if every value is skipped.
fn ord_min_max<'a, T, F>(len: usize, mut at: F) -> Option<(T, T)>
where
    T: Ord + Copy + 'a,
    F: FnMut(usize) -> Option<T>,
{
    let mut acc: Option<(T, T)> = None;
    for i in 0..len {
        if let Some(v) = at(i) {
            acc = Some(acc.map_or((v, v), |(lo, hi)| (lo.min(v), hi.max(v))));
        }
    }
    acc
}

/// Min/max for `bool` (which is `Ord`, but `min`/`max` read clearer spelled
/// out as AND/OR).
fn fold_min_max<F: FnMut(usize) -> Option<bool>>(len: usize, mut at: F) -> Option<(bool, bool)> {
    let mut acc: Option<(bool, bool)> = None;
    for i in 0..len {
        if let Some(v) = at(i) {
            acc = Some(acc.map_or((v, v), |(lo, hi)| (lo && v, hi || v)));
        }
    }
    acc
}

/// Min/max for `f32`, skipping nulls and NaNs (NaN is not a valid bound).
fn fold_float<F: FnMut(usize) -> Option<f32>>(len: usize, mut at: F) -> Option<(f32, f32)> {
    let mut acc: Option<(f32, f32)> = None;
    for i in 0..len {
        if let Some(v) = at(i)
            && !v.is_nan()
        {
            acc = Some(acc.map_or((v, v), |(lo, hi)| (lo.min(v), hi.max(v))));
        }
    }
    acc
}

/// Min/max for `f64`, skipping nulls and NaNs.
fn fold_double<F: FnMut(usize) -> Option<f64>>(len: usize, mut at: F) -> Option<(f64, f64)> {
    let mut acc: Option<(f64, f64)> = None;
    for i in 0..len {
        if let Some(v) = at(i)
            && !v.is_nan()
        {
            acc = Some(acc.map_or((v, v), |(lo, hi)| (lo.min(v), hi.max(v))));
        }
    }
    acc
}

/// The Appendix-D bytes of a single cell, for equality-delete key building.
/// `None` for null or unsupported types (an unsupported equality column makes
/// the whole delete conservatively refuse upstream).
#[must_use]
pub fn cell_bound_bytes(array: &ArrayRef, row: usize) -> Option<Vec<u8>> {
    if array.is_null(row) {
        return None;
    }
    match array.data_type() {
        DataType::Boolean => Some(Datum::Boolean(array.as_boolean().value(row)).to_bound_bytes()),
        DataType::Int32 => {
            Some(Datum::Int(array.as_primitive::<Int32Type>().value(row)).to_bound_bytes())
        }
        DataType::Date32 => {
            Some(Datum::Date(array.as_primitive::<Date32Type>().value(row)).to_bound_bytes())
        }
        DataType::Int64 => {
            Some(Datum::Long(array.as_primitive::<Int64Type>().value(row)).to_bound_bytes())
        }
        DataType::Utf8 => {
            Some(Datum::String(array.as_string::<i32>().value(row).to_owned()).to_bound_bytes())
        }
        DataType::LargeUtf8 => {
            Some(Datum::String(array.as_string::<i64>().value(row).to_owned()).to_bound_bytes())
        }
        DataType::Binary => {
            Some(Datum::Binary(array.as_binary::<i32>().value(row).to_vec()).to_bound_bytes())
        }
        _ => None,
    }
}
