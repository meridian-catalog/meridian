//! Typed single values (`Datum`) for scan planning: partition tuple values,
//! column bound values, and REST filter literals.
//!
//! Three codecs meet here, all defined by the Iceberg spec:
//!
//! - **Binary single-value serialization** (spec Appendix D): the encoding
//!   of `lower_bounds`/`upper_bounds` map values and partition field
//!   summaries in manifests — [`Datum::from_bound_bytes`] /
//!   [`Datum::to_bound_bytes`].
//! - **JSON single-value serialization** (spec Appendix D, mirrored by the
//!   REST `PrimitiveTypeValue` schema): the encoding of literals inside a
//!   REST filter expression — [`Datum::from_rest_json`].
//! - **Avro values**: partition tuples inside manifest entries, converted in
//!   `crate::manifest`.
//!
//! Values compare with [`Datum::partial_cmp_same_type`], which orders two
//! datums of the same primitive family and returns `None` for mixed-type or
//! non-orderable pairs — callers treat `None` as "unknown" and must not
//! prune on it. Float/double comparisons use IEEE-754 total order with
//! negative zero canonicalized to positive zero at construction, so `-0.0`
//! and `+0.0` compare equal, matching SQL value semantics.

use std::cmp::Ordering;
use std::fmt;

use uuid::Uuid;

use crate::spec::PrimitiveType;

/// Error converting to or from a single-value encoding.
#[derive(Debug, Clone, PartialEq, Eq, thiserror::Error)]
pub enum ValueError {
    /// The encoded bytes do not match the expected width/shape for the type.
    #[error("malformed {type_name} value: {reason}")]
    Malformed {
        /// Spec string form of the target type.
        type_name: String,
        /// What was wrong.
        reason: String,
    },
    /// The type has no supported single-value encoding here (`unknown`,
    /// `variant`, `geometry`, `geography`, and unrecognized future types).
    /// Callers must degrade conservatively (treat the value as unknown).
    #[error("no single-value support for type {type_name}")]
    Unsupported {
        /// Spec string form of the type.
        type_name: String,
    },
}

impl ValueError {
    fn malformed(ty: &PrimitiveType, reason: impl Into<String>) -> Self {
        Self::Malformed {
            type_name: ty.to_string(),
            reason: reason.into(),
        }
    }

    fn unsupported(ty: &PrimitiveType) -> Self {
        Self::Unsupported {
            type_name: ty.to_string(),
        }
    }
}

/// A single typed primitive value.
///
/// Temporal variants store the spec's internal representation (days or
/// micro-/nanoseconds from the epoch, microseconds from midnight). Decimals
/// store the unscaled integer plus scale.
#[derive(Debug, Clone)]
pub enum Datum {
    /// `boolean`.
    Boolean(bool),
    /// `int` (32-bit signed).
    Int(i32),
    /// `long` (64-bit signed).
    Long(i64),
    /// `float`; `-0.0` is canonicalized to `+0.0` at construction.
    Float(f32),
    /// `double`; `-0.0` is canonicalized to `+0.0` at construction.
    Double(f64),
    /// `date`: days from 1970-01-01.
    Date(i32),
    /// `time`: microseconds from midnight.
    Time(i64),
    /// `timestamp`: microseconds from 1970-01-01T00:00:00.
    Timestamp(i64),
    /// `timestamptz`: microseconds from 1970-01-01T00:00:00 UTC.
    Timestamptz(i64),
    /// `timestamp_ns`: nanoseconds from 1970-01-01T00:00:00.
    TimestampNs(i64),
    /// `timestamptz_ns`: nanoseconds from 1970-01-01T00:00:00 UTC.
    TimestamptzNs(i64),
    /// `string`.
    String(String),
    /// `uuid`.
    Uuid(Uuid),
    /// `fixed[N]` bytes. Bound values may legitimately be shorter than `N`
    /// when a writer truncated metrics, so the width is not enforced here.
    Fixed(Vec<u8>),
    /// `binary` bytes.
    Binary(Vec<u8>),
    /// `decimal(P,S)`: unscaled two's-complement value plus scale.
    Decimal {
        /// The unscaled integer value.
        unscaled: i128,
        /// Number of fractional digits.
        scale: u32,
    },
}

impl Datum {
    /// A float datum with `-0.0` canonicalized to `+0.0`.
    #[must_use]
    pub fn float(v: f32) -> Self {
        Self::Float(if v == 0.0 { 0.0 } else { v })
    }

    /// A double datum with `-0.0` canonicalized to `+0.0`.
    #[must_use]
    pub fn double(v: f64) -> Self {
        Self::Double(if v == 0.0 { 0.0 } else { v })
    }

    /// Whether this datum is a floating-point NaN.
    #[must_use]
    pub fn is_nan(&self) -> bool {
        match self {
            Self::Float(v) => v.is_nan(),
            Self::Double(v) => v.is_nan(),
            _ => false,
        }
    }

    /// Total order between two datums of the same primitive family, `None`
    /// when the pair is not comparable (different families, or decimal
    /// rescaling overflow). Callers must treat `None` as *unknown*, never
    /// as an excuse to prune.
    #[must_use]
    pub fn partial_cmp_same_type(&self, other: &Self) -> Option<Ordering> {
        match (self, other) {
            (Self::Boolean(a), Self::Boolean(b)) => Some(a.cmp(b)),
            (Self::Int(a), Self::Int(b)) | (Self::Date(a), Self::Date(b)) => Some(a.cmp(b)),
            (Self::Long(a), Self::Long(b))
            | (Self::Time(a), Self::Time(b))
            | (Self::Timestamp(a), Self::Timestamp(b))
            | (Self::Timestamptz(a), Self::Timestamptz(b))
            | (Self::TimestampNs(a), Self::TimestampNs(b))
            | (Self::TimestamptzNs(a), Self::TimestamptzNs(b)) => Some(a.cmp(b)),
            // Int-family values may meet long-family values after a type
            // promotion (int -> long bounds written before the promotion).
            (Self::Int(a), Self::Long(b)) => Some(i64::from(*a).cmp(b)),
            (Self::Long(a), Self::Int(b)) => Some(a.cmp(&i64::from(*b))),
            (Self::Float(a), Self::Float(b)) => Some(a.total_cmp(b)),
            (Self::Double(a), Self::Double(b)) => Some(a.total_cmp(b)),
            (Self::Float(a), Self::Double(b)) => Some(f64::from(*a).total_cmp(b)),
            (Self::Double(a), Self::Float(b)) => Some(a.total_cmp(&f64::from(*b))),
            (Self::String(a), Self::String(b)) => Some(a.cmp(b)),
            // Unsigned lexicographic byte order.
            (Self::Fixed(a) | Self::Binary(a), Self::Fixed(b) | Self::Binary(b)) => Some(a.cmp(b)),
            (Self::Uuid(a), Self::Uuid(b)) => Some(a.as_bytes().cmp(b.as_bytes())),
            (
                Self::Decimal { unscaled, scale },
                Self::Decimal {
                    unscaled: other_unscaled,
                    scale: other_scale,
                },
            ) => cmp_decimal(*unscaled, *scale, *other_unscaled, *other_scale),
            _ => None,
        }
    }

    /// Decodes a value from the spec's *binary single-value serialization*
    /// (Appendix D), the encoding of column bounds and partition summaries.
    ///
    /// Two documented tolerances mirror the reference implementation's
    /// handling of type promotion: a 4-byte buffer is accepted for `long`
    /// (int promoted to long) and for `double` (float promoted to double).
    pub fn from_bound_bytes(ty: &PrimitiveType, bytes: &[u8]) -> Result<Self, ValueError> {
        match ty {
            PrimitiveType::Boolean => match bytes {
                [b] => Ok(Self::Boolean(*b != 0)),
                _ => Err(ValueError::malformed(ty, "expected exactly 1 byte")),
            },
            PrimitiveType::Int => Ok(Self::Int(le_i32(ty, bytes)?)),
            PrimitiveType::Date => Ok(Self::Date(le_i32(ty, bytes)?)),
            PrimitiveType::Long => Ok(Self::Long(le_i64_promoted(ty, bytes)?)),
            PrimitiveType::Time => Ok(Self::Time(le_i64(ty, bytes)?)),
            PrimitiveType::Timestamp => Ok(Self::Timestamp(le_i64(ty, bytes)?)),
            PrimitiveType::Timestamptz => Ok(Self::Timestamptz(le_i64(ty, bytes)?)),
            PrimitiveType::TimestampNs => Ok(Self::TimestampNs(le_i64(ty, bytes)?)),
            PrimitiveType::TimestamptzNs => Ok(Self::TimestamptzNs(le_i64(ty, bytes)?)),
            PrimitiveType::Float => {
                let arr: [u8; 4] = bytes
                    .try_into()
                    .map_err(|_| ValueError::malformed(ty, "expected exactly 4 bytes"))?;
                Ok(Self::float(f32::from_le_bytes(arr)))
            }
            PrimitiveType::Double => match bytes.len() {
                8 => {
                    let mut arr = [0u8; 8];
                    arr.copy_from_slice(bytes);
                    Ok(Self::double(f64::from_le_bytes(arr)))
                }
                // Float bound written before a float -> double promotion.
                4 => {
                    let mut arr = [0u8; 4];
                    arr.copy_from_slice(bytes);
                    Ok(Self::double(f64::from(f32::from_le_bytes(arr))))
                }
                n => Err(ValueError::malformed(
                    ty,
                    format!("expected 8 bytes, got {n}"),
                )),
            },
            PrimitiveType::String => match std::str::from_utf8(bytes) {
                Ok(s) => Ok(Self::String(s.to_owned())),
                Err(_) => Err(ValueError::malformed(ty, "invalid UTF-8")),
            },
            PrimitiveType::Uuid => {
                let arr: [u8; 16] = bytes
                    .try_into()
                    .map_err(|_| ValueError::malformed(ty, "expected exactly 16 bytes"))?;
                Ok(Self::Uuid(Uuid::from_bytes(arr)))
            }
            PrimitiveType::Fixed(_) => Ok(Self::Fixed(bytes.to_vec())),
            PrimitiveType::Binary => Ok(Self::Binary(bytes.to_vec())),
            PrimitiveType::Decimal {
                precision: _,
                scale,
            } => {
                let unscaled = be_twos_complement_i128(ty, bytes)?;
                Ok(Self::Decimal {
                    unscaled,
                    scale: *scale,
                })
            }
            PrimitiveType::Variant
            | PrimitiveType::Geometry { .. }
            | PrimitiveType::Geography { .. }
            | PrimitiveType::Unknown
            | PrimitiveType::Other(_) => Err(ValueError::unsupported(ty)),
        }
    }

    /// Encodes this value with the spec's binary single-value serialization
    /// (Appendix D) — the inverse of [`Datum::from_bound_bytes`].
    #[must_use]
    pub fn to_bound_bytes(&self) -> Vec<u8> {
        match self {
            Self::Boolean(b) => vec![u8::from(*b)],
            Self::Int(v) | Self::Date(v) => v.to_le_bytes().to_vec(),
            Self::Long(v)
            | Self::Time(v)
            | Self::Timestamp(v)
            | Self::Timestamptz(v)
            | Self::TimestampNs(v)
            | Self::TimestamptzNs(v) => v.to_le_bytes().to_vec(),
            Self::Float(v) => v.to_le_bytes().to_vec(),
            Self::Double(v) => v.to_le_bytes().to_vec(),
            Self::String(s) => s.as_bytes().to_vec(),
            Self::Uuid(u) => u.as_bytes().to_vec(),
            Self::Fixed(b) | Self::Binary(b) => b.clone(),
            Self::Decimal { unscaled, .. } => min_be_twos_complement(*unscaled),
        }
    }

    /// Parses a REST `PrimitiveTypeValue` (the spec's *JSON single-value
    /// serialization*) into a datum of the given column type.
    ///
    /// Decimal literals are rescaled exactly to the column's scale; a
    /// literal that cannot be represented exactly at that scale is an
    /// error (callers degrade to *unknown*, keeping files).
    pub fn from_rest_json(
        ty: &PrimitiveType,
        value: &serde_json::Value,
    ) -> Result<Self, ValueError> {
        use serde_json::Value as J;
        let wrong = |expected: &str| ValueError::malformed(ty, format!("expected {expected}"));
        match ty {
            PrimitiveType::Boolean => match value {
                J::Bool(b) => Ok(Self::Boolean(*b)),
                _ => Err(wrong("a JSON boolean")),
            },
            PrimitiveType::Int => match value.as_i64() {
                Some(v) => i32::try_from(v)
                    .map(Self::Int)
                    .map_err(|_| ValueError::malformed(ty, "out of int range")),
                None => Err(wrong("a JSON integer")),
            },
            PrimitiveType::Long => match value.as_i64() {
                Some(v) => Ok(Self::Long(v)),
                None => Err(wrong("a JSON integer")),
            },
            PrimitiveType::Float => match value.as_f64() {
                #[allow(clippy::cast_possible_truncation)] // IEEE narrowing is the spec behavior
                Some(v) => Ok(Self::float(v as f32)),
                None => Err(wrong("a JSON number")),
            },
            PrimitiveType::Double => match value.as_f64() {
                Some(v) => Ok(Self::double(v)),
                None => Err(wrong("a JSON number")),
            },
            PrimitiveType::Decimal { precision, scale } => match value {
                J::String(s) => parse_decimal_literal(s, *precision, *scale)
                    .ok_or_else(|| ValueError::malformed(ty, format!("cannot represent {s:?}"))),
                _ => Err(wrong("a JSON string")),
            },
            PrimitiveType::Date => match value {
                J::String(s) => parse_date(s)
                    .map(Self::Date)
                    .ok_or_else(|| wrong("an ISO-8601 date")),
                _ => Err(wrong("a JSON string")),
            },
            PrimitiveType::Time => match value {
                J::String(s) => parse_time_micros(s)
                    .map(Self::Time)
                    .ok_or_else(|| wrong("an ISO-8601 time")),
                _ => Err(wrong("a JSON string")),
            },
            PrimitiveType::Timestamp => parse_ts(value, ty, false, false).map(Self::Timestamp),
            PrimitiveType::Timestamptz => parse_ts(value, ty, true, false).map(Self::Timestamptz),
            PrimitiveType::TimestampNs => parse_ts(value, ty, false, true).map(Self::TimestampNs),
            PrimitiveType::TimestamptzNs => {
                parse_ts(value, ty, true, true).map(Self::TimestamptzNs)
            }
            PrimitiveType::String => match value {
                J::String(s) => Ok(Self::String(s.clone())),
                _ => Err(wrong("a JSON string")),
            },
            PrimitiveType::Uuid => match value {
                J::String(s) => Uuid::parse_str(s)
                    .map(Self::Uuid)
                    .map_err(|_| wrong("a UUID string")),
                _ => Err(wrong("a JSON string")),
            },
            PrimitiveType::Fixed(_) => match value {
                J::String(s) => parse_hex(s)
                    .map(Self::Fixed)
                    .ok_or_else(|| wrong("a hexadecimal string")),
                _ => Err(wrong("a JSON string")),
            },
            PrimitiveType::Binary => match value {
                J::String(s) => parse_hex(s)
                    .map(Self::Binary)
                    .ok_or_else(|| wrong("a hexadecimal string")),
                _ => Err(wrong("a JSON string")),
            },
            PrimitiveType::Variant
            | PrimitiveType::Geometry { .. }
            | PrimitiveType::Geography { .. }
            | PrimitiveType::Unknown
            | PrimitiveType::Other(_) => Err(ValueError::unsupported(ty)),
        }
    }
}

impl PartialEq for Datum {
    fn eq(&self, other: &Self) -> bool {
        self.partial_cmp_same_type(other) == Some(Ordering::Equal)
    }
}

impl fmt::Display for Datum {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Boolean(v) => write!(f, "{v}"),
            Self::Int(v) => write!(f, "{v}"),
            Self::Long(v) => write!(f, "{v}"),
            Self::Float(v) => write!(f, "{v}"),
            Self::Double(v) => write!(f, "{v}"),
            Self::Date(v) => write!(f, "date({v})"),
            Self::Time(v) => write!(f, "time({v})"),
            Self::Timestamp(v) => write!(f, "timestamp({v})"),
            Self::Timestamptz(v) => write!(f, "timestamptz({v})"),
            Self::TimestampNs(v) => write!(f, "timestamp_ns({v})"),
            Self::TimestamptzNs(v) => write!(f, "timestamptz_ns({v})"),
            Self::String(v) => write!(f, "{v:?}"),
            Self::Uuid(v) => write!(f, "{v}"),
            Self::Fixed(v) | Self::Binary(v) => {
                write!(f, "0x")?;
                for b in v {
                    write!(f, "{b:02x}")?;
                }
                Ok(())
            }
            Self::Decimal { unscaled, scale } => write!(f, "decimal({unscaled},{scale})"),
        }
    }
}

/// Compares two decimals with potentially different scales exactly, by
/// rescaling the smaller-scale side. Returns `None` on i128 overflow.
fn cmp_decimal(a_unscaled: i128, a_scale: u32, b_unscaled: i128, b_scale: u32) -> Option<Ordering> {
    match a_scale.cmp(&b_scale) {
        Ordering::Equal => Some(a_unscaled.cmp(&b_unscaled)),
        Ordering::Less => {
            let rescaled = rescale_up(a_unscaled, b_scale - a_scale)?;
            Some(rescaled.cmp(&b_unscaled))
        }
        Ordering::Greater => {
            let rescaled = rescale_up(b_unscaled, a_scale - b_scale)?;
            Some(a_unscaled.cmp(&rescaled))
        }
    }
}

fn rescale_up(unscaled: i128, by: u32) -> Option<i128> {
    let factor = 10i128.checked_pow(by)?;
    unscaled.checked_mul(factor)
}

fn le_i32(ty: &PrimitiveType, bytes: &[u8]) -> Result<i32, ValueError> {
    let arr: [u8; 4] = bytes
        .try_into()
        .map_err(|_| ValueError::malformed(ty, "expected exactly 4 bytes"))?;
    Ok(i32::from_le_bytes(arr))
}

fn le_i64(ty: &PrimitiveType, bytes: &[u8]) -> Result<i64, ValueError> {
    let arr: [u8; 8] = bytes
        .try_into()
        .map_err(|_| ValueError::malformed(ty, "expected exactly 8 bytes"))?;
    Ok(i64::from_le_bytes(arr))
}

/// 8-byte little-endian long, tolerating a 4-byte int written before an
/// int -> long type promotion.
fn le_i64_promoted(ty: &PrimitiveType, bytes: &[u8]) -> Result<i64, ValueError> {
    match bytes.len() {
        8 => le_i64(ty, bytes),
        4 => Ok(i64::from(le_i32(ty, bytes)?)),
        n => Err(ValueError::malformed(
            ty,
            format!("expected 8 bytes, got {n}"),
        )),
    }
}

/// Parses a big-endian two's-complement integer of 1..=16 bytes.
fn be_twos_complement_i128(ty: &PrimitiveType, bytes: &[u8]) -> Result<i128, ValueError> {
    if bytes.is_empty() || bytes.len() > 16 {
        return Err(ValueError::malformed(
            ty,
            format!("expected 1..=16 bytes, got {}", bytes.len()),
        ));
    }
    let negative = bytes[0] & 0x80 != 0;
    let mut arr = if negative { [0xFFu8; 16] } else { [0u8; 16] };
    arr[16 - bytes.len()..].copy_from_slice(bytes);
    Ok(i128::from_be_bytes(arr))
}

/// Minimal-length big-endian two's-complement encoding of an i128.
fn min_be_twos_complement(v: i128) -> Vec<u8> {
    let full = v.to_be_bytes();
    let mut start = 0;
    while start < 15 {
        let byte = full[start];
        let next_msb = full[start + 1] & 0x80;
        let redundant = (byte == 0x00 && next_msb == 0) || (byte == 0xFF && next_msb != 0);
        if redundant {
            start += 1;
        } else {
            break;
        }
    }
    full[start..].to_vec()
}

fn parse_hex(s: &str) -> Option<Vec<u8>> {
    if !s.len().is_multiple_of(2) {
        return None;
    }
    (0..s.len())
        .step_by(2)
        .map(|i| u8::from_str_radix(s.get(i..i + 2)?, 16).ok())
        .collect()
}

/// Parses a decimal string literal (`"14.20"`, `"2E+20"`, `"-0.5"`) and
/// rescales it exactly to the column's scale. `None` if it cannot be
/// represented exactly within `decimal(precision, scale)`.
fn parse_decimal_literal(s: &str, precision: u32, scale: u32) -> Option<Datum> {
    let (unscaled, literal_scale) = parse_decimal_string(s)?;
    // Rescale to the column scale.
    let diff = i64::from(scale) - i64::from(literal_scale);
    let rescaled = if diff >= 0 {
        rescale_up(unscaled, u32::try_from(diff).ok()?)?
    } else {
        // The literal has more fractional digits than the column: exact
        // only when the extra digits are zero.
        let factor = 10i128.checked_pow(u32::try_from(-diff).ok()?)?;
        if unscaled % factor != 0 {
            return None;
        }
        unscaled / factor
    };
    // Enforce the precision bound so downstream fixed-width encodings hold.
    let limit = 10i128.checked_pow(precision)?;
    if rescaled.abs() >= limit {
        return None;
    }
    Some(Datum::Decimal {
        unscaled: rescaled,
        scale,
    })
}

/// Parses a decimal string to `(unscaled, scale)`; scale may be negative
/// conceptually via exponents, normalized here to a non-negative scale
/// (negative-scale values are multiplied out when exact).
fn parse_decimal_string(s: &str) -> Option<(i128, u32)> {
    let s = s.trim();
    let (mantissa, exponent) = match s.find(['e', 'E']) {
        Some(pos) => {
            let exp: i32 = s[pos + 1..].parse().ok()?;
            (&s[..pos], exp)
        }
        None => (s, 0),
    };
    let (sign, digits) = match mantissa.strip_prefix('-') {
        Some(rest) => (-1i128, rest),
        None => (1i128, mantissa.strip_prefix('+').unwrap_or(mantissa)),
    };
    let (int_part, frac_part) = match digits.split_once('.') {
        Some((i, f)) => (i, f),
        None => (digits, ""),
    };
    if int_part.is_empty() && frac_part.is_empty() {
        return None;
    }
    if !int_part.bytes().all(|b| b.is_ascii_digit())
        || !frac_part.bytes().all(|b| b.is_ascii_digit())
    {
        return None;
    }
    let mut unscaled: i128 = 0;
    for b in int_part.bytes().chain(frac_part.bytes()) {
        unscaled = unscaled
            .checked_mul(10)?
            .checked_add(i128::from(b - b'0'))?;
    }
    unscaled = unscaled.checked_mul(sign)?;
    // scale = fraction digits - exponent; normalize negative scales to 0.
    let scale = i64::try_from(frac_part.len()).ok()? - i64::from(exponent);
    if scale < 0 {
        let up = u32::try_from(-scale).ok()?;
        Some((rescale_up(unscaled, up)?, 0))
    } else {
        Some((unscaled, u32::try_from(scale).ok()?))
    }
}

/// Days from the civil epoch 1970-01-01 (proleptic Gregorian). Hinnant's
/// `days_from_civil` algorithm.
#[must_use]
pub(crate) fn days_from_civil(year: i64, month: u32, day: u32) -> i64 {
    let y = if month <= 2 { year - 1 } else { year };
    let era = if y >= 0 { y } else { y - 399 } / 400;
    let yoe = y - era * 400; // [0, 399]
    let mp = i64::from((month + 9) % 12); // [0, 11]
    let doy = (153 * mp + 2) / 5 + i64::from(day) - 1; // [0, 365]
    let doe = yoe * 365 + yoe / 4 - yoe / 100 + doy; // [0, 146096]
    era * 146_097 + doe - 719_468
}

/// Inverse of [`days_from_civil`]: `(year, month, day)` for a day number.
#[must_use]
pub(crate) fn civil_from_days(days: i64) -> (i64, u32, u32) {
    let z = days + 719_468;
    let era = if z >= 0 { z } else { z - 146_096 } / 146_097;
    let doe = z - era * 146_097; // [0, 146096]
    let yoe = (doe - doe / 1460 + doe / 36524 - doe / 146_096) / 365; // [0, 399]
    let y = yoe + era * 400;
    let doy = doe - (365 * yoe + yoe / 4 - yoe / 100); // [0, 365]
    let mp = (5 * doy + 2) / 153; // [0, 11]
    let d = doy - (153 * mp + 2) / 5 + 1; // [1, 31]
    let m = if mp < 10 { mp + 3 } else { mp - 9 }; // [1, 12]
    #[allow(clippy::cast_possible_truncation, clippy::cast_sign_loss)] // ranges proven above
    (if m <= 2 { y + 1 } else { y }, m as u32, d as u32)
}

/// Parses `YYYY-MM-DD` to days from the epoch.
fn parse_date(s: &str) -> Option<i32> {
    let mut it = s.splitn(3, '-');
    // A leading '-' (negative year) would produce an empty first segment;
    // reject it — Iceberg dates are years 1..9999 in practice.
    let year: i64 = parse_ascii_int(it.next()?)?;
    let month: u32 = parse_ascii_int(it.next()?)?.try_into().ok()?;
    let day: u32 = parse_ascii_int(it.next()?)?.try_into().ok()?;
    if !(1..=12).contains(&month) || !(1..=31).contains(&day) {
        return None;
    }
    // Reject nonsense like Feb 30 by round-tripping.
    let days = days_from_civil(year, month, day);
    if civil_from_days(days) != (year, month, day) {
        return None;
    }
    i32::try_from(days).ok()
}

fn parse_ascii_int(s: &str) -> Option<i64> {
    if s.is_empty() || !s.bytes().all(|b| b.is_ascii_digit()) {
        return None;
    }
    s.parse().ok()
}

/// Parses `HH:MM:SS[.fraction]` to `(seconds_from_midnight, frac_str)`.
fn parse_time_parts(s: &str) -> Option<(i64, &str)> {
    let (hms, frac) = match s.split_once('.') {
        Some((hms, frac)) => (hms, frac),
        None => (s, ""),
    };
    let mut it = hms.splitn(3, ':');
    let h = parse_ascii_int(it.next()?)?;
    let m = parse_ascii_int(it.next()?)?;
    let sec = parse_ascii_int(it.next()?)?;
    if !(0..24).contains(&h) || !(0..60).contains(&m) || !(0..60).contains(&sec) {
        return None;
    }
    if !frac.is_empty() && !frac.bytes().all(|b| b.is_ascii_digit()) {
        return None;
    }
    Some((h * 3600 + m * 60 + sec, frac))
}

/// Fraction digits -> subsecond value at the given precision (6 or 9).
fn frac_to_subseconds(frac: &str, digits: u32) -> Option<i64> {
    if frac.len() > digits as usize {
        return None;
    }
    let mut v: i64 = 0;
    for b in frac.bytes() {
        v = v * 10 + i64::from(b - b'0');
    }
    for _ in frac.len()..digits as usize {
        v *= 10;
    }
    Some(v)
}

/// Parses `HH:MM:SS[.ffffff]` to microseconds from midnight.
fn parse_time_micros(s: &str) -> Option<i64> {
    let (seconds, frac) = parse_time_parts(s)?;
    Some(seconds * 1_000_000 + frac_to_subseconds(frac, 6)?)
}

/// Parses an ISO-8601 timestamp literal at micro- or nanosecond precision.
/// `with_zone` requires (and applies) a trailing `Z` / `±HH:MM` offset,
/// per the REST `timestamptz` literal shape; without it an offset is
/// rejected.
fn parse_ts(
    value: &serde_json::Value,
    ty: &PrimitiveType,
    with_zone: bool,
    nanos: bool,
) -> Result<i64, ValueError> {
    let s = value
        .as_str()
        .ok_or_else(|| ValueError::malformed(ty, "expected a JSON string"))?;
    parse_timestamp_str(s, with_zone, nanos)
        .ok_or_else(|| ValueError::malformed(ty, format!("cannot parse {s:?}")))
}

fn parse_timestamp_str(s: &str, with_zone: bool, nanos: bool) -> Option<i64> {
    let (date_part, rest) = s.split_at(s.find(['T', ' '])?);
    let rest = &rest[1..];
    // Split a trailing zone designator off `rest`.
    let (time_part, offset_seconds) = if let Some(t) = rest.strip_suffix('Z') {
        (t, Some(0i64))
    } else if let Some(pos) = rest.rfind(['+', '-']) {
        // A '-' inside the time portion can only be a zone separator
        // because times use ':' — but guard against fraction digits.
        let (t, zone) = rest.split_at(pos);
        let sign = if zone.starts_with('-') { -1i64 } else { 1i64 };
        let hhmm = &zone[1..];
        let (zh, zm) = hhmm.split_once(':')?;
        let zh = parse_ascii_int(zh)?;
        let zm = parse_ascii_int(zm)?;
        if !(0..24).contains(&zh) || !(0..60).contains(&zm) {
            return None;
        }
        (t, Some(sign * (zh * 3600 + zm * 60)))
    } else {
        (rest, None)
    };
    match (with_zone, offset_seconds) {
        (true, None) | (false, Some(_)) => return None,
        _ => {}
    }
    let days = i64::from(parse_date(date_part)?);
    let (seconds, frac) = parse_time_parts(time_part)?;
    let utc_seconds = days * 86_400 + seconds - offset_seconds.unwrap_or(0);
    if nanos {
        let sub = frac_to_subseconds(frac, 9)?;
        utc_seconds.checked_mul(1_000_000_000)?.checked_add(sub)
    } else {
        let sub = frac_to_subseconds(frac, 6)?;
        utc_seconds.checked_mul(1_000_000)?.checked_add(sub)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn dec(unscaled: i128, scale: u32) -> Datum {
        Datum::Decimal { unscaled, scale }
    }

    #[test]
    fn bound_round_trip_per_type() {
        let cases: Vec<(PrimitiveType, Datum)> = vec![
            (PrimitiveType::Boolean, Datum::Boolean(true)),
            (PrimitiveType::Int, Datum::Int(-42)),
            (PrimitiveType::Long, Datum::Long(i64::MIN + 1)),
            (PrimitiveType::Float, Datum::Float(1.5)),
            (PrimitiveType::Double, Datum::Double(-2.25e300)),
            (PrimitiveType::Date, Datum::Date(20468)),
            (PrimitiveType::Time, Datum::Time(81_045_678_901)),
            (
                PrimitiveType::Timestamp,
                Datum::Timestamp(1_510_871_468_123_456),
            ),
            (PrimitiveType::Timestamptz, Datum::Timestamptz(-5_000_001)),
            (
                PrimitiveType::TimestampNs,
                Datum::TimestampNs(1_510_871_468_123_456_789),
            ),
            (PrimitiveType::TimestamptzNs, Datum::TimestamptzNs(-1)),
            (PrimitiveType::String, Datum::String("héllo".to_owned())),
            (
                PrimitiveType::Uuid,
                Datum::Uuid(Uuid::parse_str("f79c3e09-677c-4bbd-a479-3f349cb785e7").unwrap()),
            ),
            (PrimitiveType::Fixed(4), Datum::Fixed(vec![0, 1, 2, 3])),
            (PrimitiveType::Binary, Datum::Binary(vec![0xFF, 0x00])),
            (
                PrimitiveType::Decimal {
                    precision: 9,
                    scale: 2,
                },
                dec(1420, 2),
            ),
            (
                PrimitiveType::Decimal {
                    precision: 38,
                    scale: 10,
                },
                dec(-999_999_999_999_999_999_999_999_999_999_999_999_i128, 10),
            ),
        ];
        for (ty, datum) in cases {
            let bytes = datum.to_bound_bytes();
            let back = Datum::from_bound_bytes(&ty, &bytes).unwrap_or_else(|e| {
                panic!("decode {ty}: {e}");
            });
            assert_eq!(back, datum, "{ty} round trip");
        }
    }

    #[test]
    fn bound_bytes_match_spec_examples() {
        // Appendix D: int 4-byte little endian.
        assert_eq!(Datum::Int(34).to_bound_bytes(), vec![34, 0, 0, 0]);
        // Decimal 14.20 -> unscaled 1420 -> minimal big-endian.
        assert_eq!(dec(1420, 2).to_bound_bytes(), vec![0x05, 0x8C]);
        // Negative decimal keeps the sign bit without redundant 0xFF.
        assert_eq!(dec(-1, 2).to_bound_bytes(), vec![0xFF]);
        assert_eq!(dec(-256, 0).to_bound_bytes(), vec![0xFF, 0x00]);
        // UUID big-endian bytes.
        let u = Uuid::parse_str("f79c3e09-677c-4bbd-a479-3f349cb785e7").unwrap();
        assert_eq!(
            Datum::Uuid(u).to_bound_bytes()[..4],
            [0xF7, 0x9C, 0x3E, 0x09]
        );
    }

    #[test]
    fn promoted_bounds_are_tolerated() {
        // int bound read as long (int -> long promotion).
        let long = Datum::from_bound_bytes(&PrimitiveType::Long, &34i32.to_le_bytes()).unwrap();
        assert_eq!(long, Datum::Long(34));
        // float bound read as double.
        let double =
            Datum::from_bound_bytes(&PrimitiveType::Double, &1.5f32.to_le_bytes()).unwrap();
        assert_eq!(double, Datum::Double(1.5));
    }

    #[test]
    fn malformed_bounds_are_rejected() {
        assert!(Datum::from_bound_bytes(&PrimitiveType::Int, &[1, 2, 3]).is_err());
        assert!(Datum::from_bound_bytes(&PrimitiveType::Uuid, &[0; 15]).is_err());
        assert!(Datum::from_bound_bytes(&PrimitiveType::String, &[0xFF, 0xFE]).is_err());
        assert!(
            Datum::from_bound_bytes(
                &PrimitiveType::Decimal {
                    precision: 38,
                    scale: 0
                },
                &[0; 17]
            )
            .is_err()
        );
        assert!(Datum::from_bound_bytes(&PrimitiveType::Unknown, &[]).is_err());
    }

    #[test]
    fn rest_json_literals_parse_per_type() {
        use serde_json::json;
        let cases: Vec<(PrimitiveType, serde_json::Value, Datum)> = vec![
            (PrimitiveType::Boolean, json!(true), Datum::Boolean(true)),
            (PrimitiveType::Int, json!(34), Datum::Int(34)),
            (PrimitiveType::Long, json!(-7), Datum::Long(-7)),
            (PrimitiveType::Float, json!(1.0), Datum::Float(1.0)),
            (PrimitiveType::Double, json!(-0.0), Datum::Double(0.0)),
            (
                PrimitiveType::Decimal {
                    precision: 9,
                    scale: 2,
                },
                json!("14.20"),
                dec(1420, 2),
            ),
            (
                PrimitiveType::Decimal {
                    precision: 30,
                    scale: 2,
                },
                json!("2E+20"),
                dec(20_000_000_000_000_000_000_000i128, 2),
            ),
            (
                PrimitiveType::Decimal {
                    precision: 9,
                    scale: 4,
                },
                json!("-0.5"),
                dec(-5000, 4),
            ),
            (PrimitiveType::Date, json!("2017-11-16"), Datum::Date(17486)),
            (
                PrimitiveType::Time,
                json!("22:31:08.123456"),
                Datum::Time(81_068_123_456),
            ),
            (
                PrimitiveType::Timestamp,
                json!("2017-11-16T22:31:08.123456"),
                Datum::Timestamp(1_510_871_468_123_456),
            ),
            (
                PrimitiveType::Timestamptz,
                json!("2017-11-16T14:31:08.123456-08:00"),
                Datum::Timestamptz(1_510_871_468_123_456),
            ),
            (
                PrimitiveType::Timestamptz,
                json!("2017-11-16T22:31:08.123456+00:00"),
                Datum::Timestamptz(1_510_871_468_123_456),
            ),
            (
                PrimitiveType::TimestampNs,
                json!("2017-11-16T22:31:08.123456789"),
                Datum::TimestampNs(1_510_871_468_123_456_789),
            ),
            (
                PrimitiveType::TimestamptzNs,
                json!("2017-11-16T22:31:08.123456789+00:00"),
                Datum::TimestamptzNs(1_510_871_468_123_456_789),
            ),
            (
                PrimitiveType::String,
                json!("iceberg"),
                Datum::String("iceberg".to_owned()),
            ),
            (
                PrimitiveType::Uuid,
                json!("f79c3e09-677c-4bbd-a479-3f349cb785e7"),
                Datum::Uuid(Uuid::parse_str("f79c3e09-677c-4bbd-a479-3f349cb785e7").unwrap()),
            ),
            (
                PrimitiveType::Fixed(3),
                json!("78797A"),
                Datum::Fixed(vec![0x78, 0x79, 0x7A]),
            ),
            (
                PrimitiveType::Binary,
                json!("000102ff"),
                Datum::Binary(vec![0, 1, 2, 0xFF]),
            ),
        ];
        for (ty, json, expected) in cases {
            let got = Datum::from_rest_json(&ty, &json)
                .unwrap_or_else(|e| panic!("parse {ty} from {json}: {e}"));
            assert_eq!(got, expected, "{ty}");
        }
    }

    #[test]
    fn rest_json_rejects_wrong_shapes() {
        use serde_json::json;
        // Naive timestamp must not carry a zone; tz must carry one.
        assert!(
            Datum::from_rest_json(
                &PrimitiveType::Timestamp,
                &json!("2017-11-16T22:31:08+00:00")
            )
            .is_err()
        );
        assert!(
            Datum::from_rest_json(&PrimitiveType::Timestamptz, &json!("2017-11-16T22:31:08"))
                .is_err()
        );
        // Decimal literal that cannot be represented exactly at the scale.
        assert!(
            Datum::from_rest_json(
                &PrimitiveType::Decimal {
                    precision: 9,
                    scale: 2
                },
                &json!("0.005")
            )
            .is_err()
        );
        // Precision overflow.
        assert!(
            Datum::from_rest_json(
                &PrimitiveType::Decimal {
                    precision: 3,
                    scale: 2
                },
                &json!("9.99")
            )
            .is_ok()
        );
        assert!(
            Datum::from_rest_json(
                &PrimitiveType::Decimal {
                    precision: 3,
                    scale: 2
                },
                &json!("10.00")
            )
            .is_err(),
            "4 significant digits exceed precision 3"
        );
        assert!(Datum::from_rest_json(&PrimitiveType::Int, &json!("34")).is_err());
        assert!(Datum::from_rest_json(&PrimitiveType::Date, &json!("2017-02-30")).is_err());
        assert!(Datum::from_rest_json(&PrimitiveType::Date, &json!("2017-13-01")).is_err());
    }

    #[test]
    fn ordering_and_zero_canonicalization() {
        assert_eq!(
            Datum::float(-0.0).partial_cmp_same_type(&Datum::Float(0.0)),
            Some(Ordering::Equal)
        );
        assert_eq!(
            dec(1420, 2).partial_cmp_same_type(&dec(142, 1)),
            Some(Ordering::Equal)
        );
        assert_eq!(
            dec(1421, 2).partial_cmp_same_type(&dec(142, 1)),
            Some(Ordering::Greater)
        );
        assert_eq!(
            Datum::Int(1).partial_cmp_same_type(&Datum::Long(2)),
            Some(Ordering::Less)
        );
        assert_eq!(
            Datum::String("a".into()).partial_cmp_same_type(&Datum::Int(1)),
            None
        );
        // NaN sorts above +inf under total order (evaluators special-case
        // NaN before comparing).
        assert_eq!(
            Datum::Float(f32::NAN).partial_cmp_same_type(&Datum::Float(f32::INFINITY)),
            Some(Ordering::Greater)
        );
    }

    #[test]
    fn civil_date_conversions() {
        assert_eq!(days_from_civil(1970, 1, 1), 0);
        assert_eq!(days_from_civil(2017, 11, 16), 17486);
        assert_eq!(days_from_civil(1969, 12, 31), -1);
        assert_eq!(civil_from_days(17486), (2017, 11, 16));
        assert_eq!(civil_from_days(-1), (1969, 12, 31));
        for days in [-1_000_000i64, -400, -1, 0, 59, 365, 730_499, 1_000_000] {
            let (y, m, d) = civil_from_days(days);
            assert_eq!(days_from_civil(y, m, d), days);
        }
    }
}
