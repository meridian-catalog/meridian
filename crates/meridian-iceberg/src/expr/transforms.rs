//! Applying partition transforms to single values, for inclusive
//! projection: bucket hashing (32-bit Murmur3, x86 variant, seed 0, over
//! the spec Appendix B byte forms), truncation, and the temporal
//! transforms. Every function returns `None` when the transform does not
//! apply to the value's type — callers must degrade to "keep".

use crate::spec::Transform;
use crate::value::{Datum, civil_from_days};

/// Applies a transform to a non-null value. `None` = not applicable
/// (`void`, an unrecognized transform, or a source type the transform
/// does not support); callers must treat that as *unknown* during
/// pruning, never as a reason to prune.
#[must_use]
pub fn apply(transform: &Transform, value: &Datum) -> Option<Datum> {
    match transform {
        Transform::Identity => Some(value.clone()),
        Transform::Bucket(n) => bucket(*n, value).map(Datum::Int),
        Transform::Truncate(w) => truncate(*w, value),
        Transform::Year => temporal(value, TemporalUnit::Year),
        Transform::Month => temporal(value, TemporalUnit::Month),
        Transform::Day => temporal(value, TemporalUnit::Day),
        Transform::Hour => temporal(value, TemporalUnit::Hour),
        // void yields null for every input; a null partition value gives
        // no pruning power, so projection treats it as non-projectable.
        Transform::Void | Transform::Other(_) => None,
    }
}

/// The previous representable source value, for tightening `lt` to
/// `lt-eq` on integer-backed types during projection. `None` for types
/// without a discrete predecessor or on underflow.
#[must_use]
pub(crate) fn predecessor(value: &Datum) -> Option<Datum> {
    match value {
        Datum::Int(v) => v.checked_sub(1).map(Datum::Int),
        Datum::Long(v) => v.checked_sub(1).map(Datum::Long),
        Datum::Date(v) => v.checked_sub(1).map(Datum::Date),
        Datum::Time(v) => v.checked_sub(1).map(Datum::Time),
        Datum::Timestamp(v) => v.checked_sub(1).map(Datum::Timestamp),
        Datum::Timestamptz(v) => v.checked_sub(1).map(Datum::Timestamptz),
        Datum::TimestampNs(v) => v.checked_sub(1).map(Datum::TimestampNs),
        Datum::TimestamptzNs(v) => v.checked_sub(1).map(Datum::TimestamptzNs),
        Datum::Decimal { unscaled, scale } => unscaled.checked_sub(1).map(|u| Datum::Decimal {
            unscaled: u,
            scale: *scale,
        }),
        _ => None,
    }
}

/// The next representable source value; see [`predecessor`].
#[must_use]
pub(crate) fn successor(value: &Datum) -> Option<Datum> {
    match value {
        Datum::Int(v) => v.checked_add(1).map(Datum::Int),
        Datum::Long(v) => v.checked_add(1).map(Datum::Long),
        Datum::Date(v) => v.checked_add(1).map(Datum::Date),
        Datum::Time(v) => v.checked_add(1).map(Datum::Time),
        Datum::Timestamp(v) => v.checked_add(1).map(Datum::Timestamp),
        Datum::Timestamptz(v) => v.checked_add(1).map(Datum::Timestamptz),
        Datum::TimestampNs(v) => v.checked_add(1).map(Datum::TimestampNs),
        Datum::TimestamptzNs(v) => v.checked_add(1).map(Datum::TimestamptzNs),
        Datum::Decimal { unscaled, scale } => unscaled.checked_add(1).map(|u| Datum::Decimal {
            unscaled: u,
            scale: *scale,
        }),
        _ => None,
    }
}

// ---- bucket ----

/// `bucket_N(x) = (murmur3_x86_32(bytes(x)) & i32::MAX) % N`.
#[must_use]
pub(crate) fn bucket(n: u32, value: &Datum) -> Option<i32> {
    let positive = hash32(value) & i32::MAX;
    let n = i32::try_from(n).ok()?;
    if n <= 0 {
        return None;
    }
    Some(positive % n)
}

/// The Appendix B 32-bit hash of a value (every primitive family has a
/// defined hash, including the "not currently valid for bucketing" ones).
#[must_use]
pub(crate) fn hash32(value: &Datum) -> i32 {
    match value {
        // int/date and long must hash identically (schema promotion).
        Datum::Int(v) | Datum::Date(v) => murmur3_32(&i64::from(*v).to_le_bytes()),
        Datum::Long(v) | Datum::Time(v) | Datum::Timestamp(v) | Datum::Timestamptz(v) => {
            murmur3_32(&v.to_le_bytes())
        }
        // Nanosecond timestamps hash at microsecond precision.
        Datum::TimestampNs(v) | Datum::TimestamptzNs(v) => {
            murmur3_32(&floor_div_i64(*v, 1000).to_le_bytes())
        }
        Datum::String(s) => murmur3_32(s.as_bytes()),
        Datum::Uuid(u) => murmur3_32(u.as_bytes()),
        Datum::Fixed(b) | Datum::Binary(b) => murmur3_32(b),
        Datum::Decimal { .. } => murmur3_32(&value.to_bound_bytes()),
        // Not valid for bucketing; hashes defined by the spec anyway.
        // hashInt == hashLong per Appendix B note 1.
        Datum::Boolean(v) => murmur3_32(&i64::from(*v).to_le_bytes()),
        Datum::Float(v) => murmur3_32(&f64::from(*v).to_bits().to_le_bytes()),
        Datum::Double(v) => murmur3_32(&v.to_bits().to_le_bytes()),
    }
}

/// 32-bit Murmur3, x86 variant, seed 0.
#[allow(clippy::cast_possible_wrap)] // the algorithm is defined on wrapping u32
#[must_use]
pub(crate) fn murmur3_32(data: &[u8]) -> i32 {
    const C1: u32 = 0xcc9e_2d51;
    const C2: u32 = 0x1b87_3593;
    let mut h1: u32 = 0; // seed

    let mut chunks = data.chunks_exact(4);
    for chunk in &mut chunks {
        let mut k1 = u32::from_le_bytes([chunk[0], chunk[1], chunk[2], chunk[3]]);
        k1 = k1.wrapping_mul(C1).rotate_left(15).wrapping_mul(C2);
        h1 = (h1 ^ k1)
            .rotate_left(13)
            .wrapping_mul(5)
            .wrapping_add(0xe654_6b64);
    }

    let tail = chunks.remainder();
    if !tail.is_empty() {
        let mut k1: u32 = 0;
        for (i, byte) in tail.iter().enumerate() {
            k1 |= u32::from(*byte) << (8 * i);
        }
        k1 = k1.wrapping_mul(C1).rotate_left(15).wrapping_mul(C2);
        h1 ^= k1;
    }

    h1 ^= u32::try_from(data.len()).unwrap_or(u32::MAX);
    // fmix32
    h1 ^= h1 >> 16;
    h1 = h1.wrapping_mul(0x85eb_ca6b);
    h1 ^= h1 >> 13;
    h1 = h1.wrapping_mul(0xc2b2_ae35);
    h1 ^= h1 >> 16;
    h1 as i32
}

// ---- truncate ----

fn truncate(width: u32, value: &Datum) -> Option<Datum> {
    let w = i128::from(width);
    if w <= 0 {
        return None;
    }
    match value {
        Datum::Int(v) => {
            let t = i128::from(*v) - floor_mod(i128::from(*v), w);
            i32::try_from(t).ok().map(Datum::Int)
        }
        Datum::Long(v) => {
            let t = i128::from(*v) - floor_mod(i128::from(*v), w);
            i64::try_from(t).ok().map(Datum::Long)
        }
        Datum::Decimal { unscaled, scale } => Some(Datum::Decimal {
            unscaled: unscaled - floor_mod(*unscaled, w),
            scale: *scale,
        }),
        Datum::String(s) => {
            let width = usize::try_from(width).ok()?;
            let end = s.char_indices().nth(width).map_or(s.len(), |(idx, _)| idx);
            Some(Datum::String(s[..end].to_owned()))
        }
        Datum::Binary(b) => {
            let width = usize::try_from(width).ok()?;
            Some(Datum::Binary(b[..b.len().min(width)].to_vec()))
        }
        _ => None,
    }
}

/// `v mod w` with a non-negative result (the spec's truncate remainder).
fn floor_mod(v: i128, w: i128) -> i128 {
    ((v % w) + w) % w
}

pub(crate) fn floor_div_i64(v: i64, by: i64) -> i64 {
    let d = v / by;
    if (v % by != 0) && ((v < 0) != (by < 0)) {
        d - 1
    } else {
        d
    }
}

// ---- temporal ----

#[derive(Clone, Copy)]
enum TemporalUnit {
    Year,
    Month,
    Day,
    Hour,
}

/// Applies year/month/day/hour to a date or timestamp value.
fn temporal(value: &Datum, unit: TemporalUnit) -> Option<Datum> {
    let days: i64 = match value {
        Datum::Date(d) => i64::from(*d),
        Datum::Timestamp(us) | Datum::Timestamptz(us) => {
            if let TemporalUnit::Hour = unit {
                let hours = floor_div_i64(*us, 3_600_000_000);
                return i32::try_from(hours).ok().map(Datum::Int);
            }
            floor_div_i64(*us, 86_400_000_000)
        }
        Datum::TimestampNs(ns) | Datum::TimestamptzNs(ns) => {
            if let TemporalUnit::Hour = unit {
                let hours = floor_div_i64(*ns, 3_600_000_000_000);
                return i32::try_from(hours).ok().map(Datum::Int);
            }
            floor_div_i64(*ns, 86_400_000_000_000)
        }
        _ => return None,
    };
    match unit {
        // Reaching here with Hour means the source was a date; hour over
        // a date is not a valid transform (timestamps returned above).
        TemporalUnit::Hour => None,
        TemporalUnit::Day => i32::try_from(days).ok().map(Datum::Date),
        TemporalUnit::Year => {
            let (year, _, _) = civil_from_days(days);
            i32::try_from(year - 1970).ok().map(Datum::Int)
        }
        TemporalUnit::Month => {
            let (year, month, _) = civil_from_days(days);
            let months = (year - 1970) * 12 + i64::from(month) - 1;
            i32::try_from(months).ok().map(Datum::Int)
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use uuid::Uuid;

    /// Days from epoch for a civil date.
    fn ymd(y: i64, m: u32, d: u32) -> i32 {
        i32::try_from(crate::value::days_from_civil(y, m, d)).expect("test date in range")
    }

    /// The Appendix B test vectors, verbatim.
    #[test]
    fn hash32_matches_spec_test_vectors() {
        assert_eq!(hash32(&Datum::Int(34)), 2_017_239_379);
        assert_eq!(hash32(&Datum::Long(34)), 2_017_239_379);
        assert_eq!(
            hash32(&Datum::Decimal {
                unscaled: 1420,
                scale: 2
            }),
            -500_754_589,
            "decimal 14.20"
        );
        assert_eq!(
            hash32(&Datum::Date(ymd(2017, 11, 16))),
            -653_330_422,
            "date 2017-11-16"
        );
        let time = (22 * 3600 + 31 * 60 + 8) * 1_000_000;
        assert_eq!(hash32(&Datum::Time(time)), -662_762_989, "22:31:08");
        let ts = i64::from(ymd(2017, 11, 16)) * 86_400_000_000 + time;
        assert_eq!(hash32(&Datum::Timestamp(ts)), -2_047_944_441);
        assert_eq!(hash32(&Datum::Timestamptz(ts)), -2_047_944_441);
        assert_eq!(hash32(&Datum::Timestamp(ts + 1)), -1_207_196_810);
        // Nanosecond timestamps hash at microsecond precision.
        assert_eq!(hash32(&Datum::TimestampNs(ts * 1000)), -2_047_944_441);
        assert_eq!(
            hash32(&Datum::TimestampNs(ts * 1000 + 1001)),
            -1_207_196_810
        );
        assert_eq!(hash32(&Datum::String("iceberg".to_owned())), 1_210_000_089);
        let uuid = Uuid::parse_str("f79c3e09-677c-4bbd-a479-3f349cb785e7").expect("uuid");
        assert_eq!(hash32(&Datum::Uuid(uuid)), 1_488_055_340);
        assert_eq!(
            hash32(&Datum::Binary(vec![0x00, 0x01, 0x02, 0x03])),
            -188_683_207
        );
        assert_eq!(
            hash32(&Datum::Fixed(vec![0x00, 0x01, 0x02, 0x03])),
            -188_683_207
        );
        assert_eq!(hash32(&Datum::Boolean(true)), 1_392_991_556);
        assert_eq!(hash32(&Datum::Float(1.0)), -142_385_009);
        assert_eq!(hash32(&Datum::Double(1.0)), -142_385_009);
        assert_eq!(hash32(&Datum::Float(0.0)), 1_669_671_676);
        assert_eq!(hash32(&Datum::float(-0.0)), 1_669_671_676);
    }

    #[test]
    fn truncate_matches_spec_examples() {
        assert_eq!(
            apply(&Transform::Truncate(10), &Datum::Int(1)),
            Some(Datum::Int(0))
        );
        assert_eq!(
            apply(&Transform::Truncate(10), &Datum::Int(-1)),
            Some(Datum::Int(-10))
        );
        assert_eq!(
            apply(&Transform::Truncate(10), &Datum::Long(-1)),
            Some(Datum::Long(-10))
        );
        assert_eq!(
            apply(
                &Transform::Truncate(50),
                &Datum::Decimal {
                    unscaled: 1065,
                    scale: 2
                }
            ),
            Some(Datum::Decimal {
                unscaled: 1050,
                scale: 2
            }),
            "W=50, s=2: 10.65 -> 10.50"
        );
        assert_eq!(
            apply(
                &Transform::Truncate(3),
                &Datum::String("iceberg".to_owned())
            ),
            Some(Datum::String("ice".to_owned()))
        );
        // Code points, not bytes.
        assert_eq!(
            apply(&Transform::Truncate(2), &Datum::String("héllo".to_owned())),
            Some(Datum::String("hé".to_owned()))
        );
        assert_eq!(
            apply(&Transform::Truncate(3), &Datum::Binary(vec![1, 2, 3, 4, 5])),
            Some(Datum::Binary(vec![1, 2, 3]))
        );
        // Shorter than the width: unchanged.
        assert_eq!(
            apply(&Transform::Truncate(16), &Datum::String("ab".to_owned())),
            Some(Datum::String("ab".to_owned()))
        );
    }

    #[test]
    fn temporal_transforms() {
        let date = Datum::Date(ymd(2017, 11, 16));
        assert_eq!(apply(&Transform::Year, &date), Some(Datum::Int(47)));
        assert_eq!(
            apply(&Transform::Month, &date),
            Some(Datum::Int(47 * 12 + 10))
        );
        assert_eq!(
            apply(&Transform::Day, &date),
            Some(Datum::Date(ymd(2017, 11, 16)))
        );
        assert_eq!(
            apply(&Transform::Hour, &date),
            None,
            "hour of a date is invalid"
        );

        let ts = Datum::Timestamp(i64::from(ymd(2017, 11, 16)) * 86_400_000_000 + 3_600_000_001);
        assert_eq!(apply(&Transform::Year, &ts), Some(Datum::Int(47)));
        assert_eq!(
            apply(&Transform::Day, &ts),
            Some(Datum::Date(ymd(2017, 11, 16)))
        );
        assert_eq!(
            apply(&Transform::Hour, &ts),
            Some(Datum::Int(
                i32::try_from(i64::from(ymd(2017, 11, 16)) * 24).expect("fits") + 1
            ))
        );

        // Negative values floor toward the past.
        let before_epoch = Datum::Timestamp(-1);
        assert_eq!(apply(&Transform::Day, &before_epoch), Some(Datum::Date(-1)));
        assert_eq!(apply(&Transform::Hour, &before_epoch), Some(Datum::Int(-1)));
        assert_eq!(apply(&Transform::Year, &before_epoch), Some(Datum::Int(-1)));

        // Nanosecond timestamps.
        let ts_ns = Datum::TimestampNs(i64::from(ymd(2026, 1, 15)) * 86_400_000_000_000 + 42);
        assert_eq!(
            apply(&Transform::Day, &ts_ns),
            Some(Datum::Date(ymd(2026, 1, 15)))
        );
    }

    #[test]
    fn bucket_is_positive_mod() {
        // hash(34) = 2017239379; 2017239379 % 16 = 3.
        assert_eq!(bucket(16, &Datum::Long(34)), Some(3));
        // A negative hash must still land in [0, N).
        let b = bucket(7, &Datum::Date(ymd(2017, 11, 16))).expect("bucket");
        assert!((0..7).contains(&b));
    }

    #[test]
    fn unknown_and_void_do_not_apply() {
        assert_eq!(apply(&Transform::Void, &Datum::Int(1)), None);
        assert_eq!(
            apply(&Transform::Other("zorder(a,b)".to_owned()), &Datum::Int(1)),
            None
        );
        // bucket over float is not valid for bucketing... but hash32 is
        // defined; bucket still applies the spec hash. The projection
        // layer never produces it because bucket sources exclude floats.
        assert_eq!(apply(&Transform::Truncate(2), &Datum::Double(1.5)), None);
    }
}
