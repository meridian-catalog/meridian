//! Partition and sort transforms.

use std::fmt;
use std::str::FromStr;

use serde::de::Error as _;
use serde::{Deserialize, Deserializer, Serialize, Serializer};

/// A partition/sort transform, parsed from its spec string form.
///
/// Unrecognized transform strings are preserved verbatim via
/// [`Transform::Other`] so metadata written by newer tools round-trips; the
/// metadata builder refuses to *add* specs or sort orders that use them.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Transform {
    /// The source value, unmodified.
    Identity,
    /// Hash of the source value, mod `N`: `bucket[N]`.
    Bucket(u32),
    /// Source value truncated to width `W`: `truncate[W]`.
    Truncate(u32),
    /// Extract the year, as years from 1970.
    Year,
    /// Extract the month, as months from 1970-01-01.
    Month,
    /// Extract the date, as days from 1970-01-01.
    Day,
    /// Extract the hour, as hours from 1970-01-01 00:00:00.
    Hour,
    /// Always produces `null`; used to drop a field from partitioning
    /// without renumbering.
    Void,
    /// A transform string this model does not recognize, preserved verbatim.
    Other(String),
}

impl Transform {
    /// Whether this transform is one the model understands (i.e. not
    /// [`Transform::Other`]).
    #[must_use]
    pub fn is_recognized(&self) -> bool {
        !matches!(self, Self::Other(_))
    }
}

/// Error parsing a `bucket[N]`/`truncate[W]` transform with malformed or
/// out-of-range parameters.
#[derive(Debug, Clone, PartialEq, Eq, thiserror::Error)]
#[error("invalid transform {input:?}: {reason}")]
pub struct ParseTransformError {
    /// The offending transform string.
    pub input: String,
    /// Why it failed to parse.
    pub reason: String,
}

impl FromStr for Transform {
    type Err = ParseTransformError;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        match s {
            "identity" => return Ok(Self::Identity),
            "year" => return Ok(Self::Year),
            "month" => return Ok(Self::Month),
            "day" => return Ok(Self::Day),
            "hour" => return Ok(Self::Hour),
            "void" => return Ok(Self::Void),
            _ => {}
        }
        let parameterized = [
            ("bucket[", Self::Bucket as fn(u32) -> Self, "bucket count"),
            ("truncate[", Self::Truncate as fn(u32) -> Self, "width"),
        ];
        for (prefix, constructor, what) in parameterized {
            if let Some(inner) = s.strip_prefix(prefix).and_then(|r| r.strip_suffix(']')) {
                let n: u32 = inner.trim().parse().map_err(|_| ParseTransformError {
                    input: s.to_owned(),
                    reason: format!("{what} is not a number"),
                })?;
                if n == 0 {
                    return Err(ParseTransformError {
                        input: s.to_owned(),
                        reason: format!("{what} must be positive"),
                    });
                }
                return Ok(constructor(n));
            }
        }
        for prefix in ["bucket", "truncate"] {
            if s.starts_with(prefix) {
                return Err(ParseTransformError {
                    input: s.to_owned(),
                    reason: "malformed transform parameters".to_owned(),
                });
            }
        }
        Ok(Self::Other(s.to_owned()))
    }
}

impl fmt::Display for Transform {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Identity => f.write_str("identity"),
            Self::Bucket(n) => write!(f, "bucket[{n}]"),
            Self::Truncate(w) => write!(f, "truncate[{w}]"),
            Self::Year => f.write_str("year"),
            Self::Month => f.write_str("month"),
            Self::Day => f.write_str("day"),
            Self::Hour => f.write_str("hour"),
            Self::Void => f.write_str("void"),
            Self::Other(other) => f.write_str(other),
        }
    }
}

impl Serialize for Transform {
    fn serialize<S: Serializer>(&self, serializer: S) -> Result<S::Ok, S::Error> {
        serializer.collect_str(self)
    }
}

impl<'de> Deserialize<'de> for Transform {
    fn deserialize<D: Deserializer<'de>>(deserializer: D) -> Result<Self, D::Error> {
        let s = String::deserialize(deserializer)?;
        s.parse().map_err(D::Error::custom)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn transforms_round_trip_exactly() {
        for s in [
            "identity",
            "bucket[16]",
            "truncate[4]",
            "year",
            "month",
            "day",
            "hour",
            "void",
        ] {
            let parsed: Transform = s.parse().expect("parse");
            assert_eq!(parsed.to_string(), s);
            assert!(parsed.is_recognized());
        }
    }

    #[test]
    fn malformed_parameters_are_rejected() {
        for s in [
            "bucket[]",
            "bucket[0]",
            "bucket[x]",
            "truncate[-1]",
            "bucket(16)",
        ] {
            assert!(s.parse::<Transform>().is_err(), "{s} must be rejected");
        }
    }

    #[test]
    fn unrecognized_transforms_are_preserved() {
        let parsed: Transform = "zorder(a,b)".parse().expect("parse");
        assert_eq!(parsed, Transform::Other("zorder(a,b)".to_owned()));
        assert_eq!(parsed.to_string(), "zorder(a,b)");
        assert!(!parsed.is_recognized());
    }
}
