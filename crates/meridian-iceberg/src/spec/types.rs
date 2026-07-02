//! The Iceberg type tree: primitives and nested struct/list/map types.
//!
//! Primitive types serialize to their spec string forms (`"long"`,
//! `"decimal(10,2)"`, `"fixed[16]"`, …); nested types serialize to the JSON
//! object forms with embedded field ids. Parsing is lenient about interior
//! whitespace (Java writes `decimal(10, 2)`, other writers omit the space);
//! serialization is canonical and whitespace-free. Primitive type names this
//! model does not recognize are preserved verbatim via
//! [`PrimitiveType::Other`] so metadata written by newer tools survives a
//! round trip; the metadata builder refuses to *add* schemas containing them.

use std::fmt;
use std::str::FromStr;

use serde::de::Error as _;
use serde::{Deserialize, Deserializer, Serialize, Serializer};
use serde_json::{Map, Value};

/// The default CRS for `geometry`/`geography` when none is spelled out.
pub const DEFAULT_CRS: &str = "OGC:CRS84";

/// An Iceberg primitive type.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum PrimitiveType {
    /// True or false.
    Boolean,
    /// 32-bit signed integer.
    Int,
    /// 64-bit signed integer.
    Long,
    /// 32-bit IEEE 754 float.
    Float,
    /// 64-bit IEEE 754 float.
    Double,
    /// Fixed-point decimal with precision and scale: `decimal(P,S)`.
    Decimal {
        /// Number of significant digits (1..=38).
        precision: u32,
        /// Digits to the right of the decimal point.
        scale: u32,
    },
    /// Calendar date without time or zone.
    Date,
    /// Time of day, microsecond precision, without date or zone.
    Time,
    /// Timestamp, microsecond precision, without zone.
    Timestamp,
    /// Timestamp, microsecond precision, with UTC zone.
    Timestamptz,
    /// Timestamp, nanosecond precision, without zone (v3).
    TimestampNs,
    /// Timestamp, nanosecond precision, with UTC zone (v3).
    TimestamptzNs,
    /// UTF-8 string.
    String,
    /// 16-byte UUID.
    Uuid,
    /// Fixed-length byte array: `fixed[N]`.
    Fixed(u64),
    /// Variable-length byte array.
    Binary,
    /// Semi-structured variant value (v3).
    Variant,
    /// Geospatial geometry with an optional CRS: `geometry(C)` (v3).
    Geometry {
        /// Coordinate reference system; `None` means the spec default
        /// ([`DEFAULT_CRS`]).
        crs: Option<std::string::String>,
    },
    /// Geospatial geography with optional CRS and edge algorithm:
    /// `geography(C,A)` (v3).
    Geography {
        /// Coordinate reference system; `None` means the spec default
        /// ([`DEFAULT_CRS`]).
        crs: Option<std::string::String>,
        /// Edge-interpolation algorithm; `None` means the spec default
        /// (`spherical`).
        algorithm: Option<std::string::String>,
    },
    /// The v3 `unknown` type: always null, used before a type is known.
    Unknown,
    /// A primitive type string this model does not recognize, preserved
    /// verbatim so future spec versions round-trip losslessly. Rejected by
    /// the metadata builder when *adding* schemas.
    Other(std::string::String),
}

impl PrimitiveType {
    /// Whether this primitive requires format version 3.
    #[must_use]
    pub fn requires_v3(&self) -> bool {
        matches!(
            self,
            Self::TimestampNs
                | Self::TimestamptzNs
                | Self::Variant
                | Self::Geometry { .. }
                | Self::Geography { .. }
                | Self::Unknown
        )
    }
}

/// Error parsing a type string that starts like a known parameterized type
/// but is malformed (e.g. `decimal(a,b)`, `fixed[]`).
#[derive(Debug, Clone, PartialEq, Eq, thiserror::Error)]
#[error("invalid type string {input:?}: {reason}")]
pub struct ParseTypeError {
    /// The offending type string.
    pub input: String,
    /// Why it failed to parse.
    pub reason: String,
}

impl ParseTypeError {
    fn new(input: &str, reason: impl Into<String>) -> Self {
        Self {
            input: input.to_owned(),
            reason: reason.into(),
        }
    }
}

fn parse_decimal(input: &str, inner: &str) -> Result<PrimitiveType, ParseTypeError> {
    let (p, s) = inner
        .split_once(',')
        .ok_or_else(|| ParseTypeError::new(input, "expected decimal(precision,scale)"))?;
    let precision: u32 = p
        .trim()
        .parse()
        .map_err(|_| ParseTypeError::new(input, "precision is not a number"))?;
    let scale: u32 = s
        .trim()
        .parse()
        .map_err(|_| ParseTypeError::new(input, "scale is not a number"))?;
    if precision == 0 || precision > 38 {
        return Err(ParseTypeError::new(input, "precision must be in 1..=38"));
    }
    if scale > precision {
        return Err(ParseTypeError::new(
            input,
            "scale must not exceed precision",
        ));
    }
    Ok(PrimitiveType::Decimal { precision, scale })
}

fn parse_fixed(input: &str, inner: &str) -> Result<PrimitiveType, ParseTypeError> {
    let length: u64 = inner
        .trim()
        .parse()
        .map_err(|_| ParseTypeError::new(input, "fixed length is not a number"))?;
    if length == 0 {
        return Err(ParseTypeError::new(input, "fixed length must be positive"));
    }
    Ok(PrimitiveType::Fixed(length))
}

fn parse_geography(input: &str, inner: &str) -> Result<PrimitiveType, ParseTypeError> {
    if inner.trim().is_empty() {
        return Err(ParseTypeError::new(input, "empty geography parameters"));
    }
    // The algorithm, if present, is the identifier after the last comma. A
    // CRS may itself contain commas (e.g. WKT), so only split when the tail
    // looks like an algorithm name.
    if let Some((crs, algorithm)) = inner.rsplit_once(',') {
        let algorithm = algorithm.trim();
        let is_identifier = !algorithm.is_empty()
            && algorithm
                .chars()
                .all(|c| c.is_ascii_alphanumeric() || c == '_' || c == '-');
        if is_identifier {
            let crs = crs.trim();
            if crs.is_empty() {
                return Err(ParseTypeError::new(input, "empty geography CRS"));
            }
            return Ok(PrimitiveType::Geography {
                crs: Some(crs.to_owned()),
                algorithm: Some(algorithm.to_owned()),
            });
        }
    }
    Ok(PrimitiveType::Geography {
        crs: Some(inner.trim().to_owned()),
        algorithm: None,
    })
}

impl FromStr for PrimitiveType {
    type Err = ParseTypeError;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        match s {
            "boolean" => return Ok(Self::Boolean),
            "int" => return Ok(Self::Int),
            "long" => return Ok(Self::Long),
            "float" => return Ok(Self::Float),
            "double" => return Ok(Self::Double),
            "date" => return Ok(Self::Date),
            "time" => return Ok(Self::Time),
            "timestamp" => return Ok(Self::Timestamp),
            "timestamptz" => return Ok(Self::Timestamptz),
            "timestamp_ns" => return Ok(Self::TimestampNs),
            "timestamptz_ns" => return Ok(Self::TimestamptzNs),
            "string" => return Ok(Self::String),
            "uuid" => return Ok(Self::Uuid),
            "binary" => return Ok(Self::Binary),
            "variant" => return Ok(Self::Variant),
            "unknown" => return Ok(Self::Unknown),
            "geometry" => return Ok(Self::Geometry { crs: None }),
            "geography" => {
                return Ok(Self::Geography {
                    crs: None,
                    algorithm: None,
                });
            }
            _ => {}
        }
        if let Some(inner) = s.strip_prefix("decimal(").and_then(|r| r.strip_suffix(')')) {
            return parse_decimal(s, inner);
        }
        if let Some(inner) = s.strip_prefix("fixed[").and_then(|r| r.strip_suffix(']')) {
            return parse_fixed(s, inner);
        }
        if let Some(inner) = s
            .strip_prefix("geometry(")
            .and_then(|r| r.strip_suffix(')'))
        {
            let crs = inner.trim();
            if crs.is_empty() {
                return Err(ParseTypeError::new(s, "empty geometry CRS"));
            }
            return Ok(Self::Geometry {
                crs: Some(crs.to_owned()),
            });
        }
        if let Some(inner) = s
            .strip_prefix("geography(")
            .and_then(|r| r.strip_suffix(')'))
        {
            return parse_geography(s, inner);
        }
        // A known parameterized prefix without well-formed parameters is a
        // malformed spelling of a modelled type, not a future type.
        for prefix in ["decimal", "fixed", "geometry", "geography"] {
            if s.starts_with(prefix) {
                return Err(ParseTypeError::new(s, "malformed type parameters"));
            }
        }
        Ok(Self::Other(s.to_owned()))
    }
}

impl fmt::Display for PrimitiveType {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Boolean => f.write_str("boolean"),
            Self::Int => f.write_str("int"),
            Self::Long => f.write_str("long"),
            Self::Float => f.write_str("float"),
            Self::Double => f.write_str("double"),
            Self::Decimal { precision, scale } => write!(f, "decimal({precision},{scale})"),
            Self::Date => f.write_str("date"),
            Self::Time => f.write_str("time"),
            Self::Timestamp => f.write_str("timestamp"),
            Self::Timestamptz => f.write_str("timestamptz"),
            Self::TimestampNs => f.write_str("timestamp_ns"),
            Self::TimestamptzNs => f.write_str("timestamptz_ns"),
            Self::String => f.write_str("string"),
            Self::Uuid => f.write_str("uuid"),
            Self::Fixed(length) => write!(f, "fixed[{length}]"),
            Self::Binary => f.write_str("binary"),
            Self::Variant => f.write_str("variant"),
            Self::Geometry { crs: None } => f.write_str("geometry"),
            Self::Geometry { crs: Some(crs) } => write!(f, "geometry({crs})"),
            Self::Geography {
                crs: None,
                algorithm: None,
            } => f.write_str("geography"),
            Self::Geography {
                crs: Some(crs),
                algorithm: None,
            } => write!(f, "geography({crs})"),
            Self::Geography {
                crs,
                algorithm: Some(algorithm),
            } => {
                // A geography with an algorithm but no explicit CRS is
                // rendered with the spec-default CRS spelled out, because the
                // string form has no way to omit only the first parameter.
                let crs = crs.as_deref().unwrap_or(DEFAULT_CRS);
                write!(f, "geography({crs},{algorithm})")
            }
            Self::Unknown => f.write_str("unknown"),
            Self::Other(other) => f.write_str(other),
        }
    }
}

impl Serialize for PrimitiveType {
    fn serialize<S: Serializer>(&self, serializer: S) -> Result<S::Ok, S::Error> {
        serializer.collect_str(self)
    }
}

impl<'de> Deserialize<'de> for PrimitiveType {
    fn deserialize<D: Deserializer<'de>>(deserializer: D) -> Result<Self, D::Error> {
        let s = std::string::String::deserialize(deserializer)?;
        s.parse().map_err(D::Error::custom)
    }
}

/// Marker for the `"type": "struct"` discriminator on struct schemas.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq, Serialize, Deserialize)]
pub enum StructTag {
    /// The only value: `"struct"`.
    #[default]
    #[serde(rename = "struct")]
    Struct,
}

/// Marker for the `"type": "list"` discriminator.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq, Serialize, Deserialize)]
pub enum ListTag {
    /// The only value: `"list"`.
    #[default]
    #[serde(rename = "list")]
    List,
}

/// Marker for the `"type": "map"` discriminator.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq, Serialize, Deserialize)]
pub enum MapTag {
    /// The only value: `"map"`.
    #[default]
    #[serde(rename = "map")]
    Map,
}

/// One field of a struct type.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "kebab-case")]
pub struct StructField {
    /// Field id, unique within the table across schema evolution.
    pub id: i32,
    /// Field name.
    pub name: String,
    /// Whether values are required (non-null).
    pub required: bool,
    /// The field type.
    #[serde(rename = "type")]
    pub field_type: Type,
    /// Optional documentation string.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub doc: Option<String>,
    /// Default for rows that predate the field (v3). Kept as raw JSON: the
    /// value's shape depends on `field_type`.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub initial_default: Option<Value>,
    /// Default applied when a writer omits the field (v3). Raw JSON, as
    /// above.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub write_default: Option<Value>,
    /// Unknown fields, preserved verbatim.
    #[serde(flatten)]
    pub extra: Map<String, Value>,
}

impl StructField {
    /// A required field with no doc or defaults.
    #[must_use]
    pub fn required(id: i32, name: impl Into<String>, field_type: Type) -> Self {
        Self::new(id, name, true, field_type)
    }

    /// An optional field with no doc or defaults.
    #[must_use]
    pub fn optional(id: i32, name: impl Into<String>, field_type: Type) -> Self {
        Self::new(id, name, false, field_type)
    }

    fn new(id: i32, name: impl Into<String>, required: bool, field_type: Type) -> Self {
        Self {
            id,
            name: name.into(),
            required,
            field_type,
            doc: None,
            initial_default: None,
            write_default: None,
            extra: Map::new(),
        }
    }
}

/// A struct type: an ordered collection of named, id'd fields.
#[derive(Debug, Clone, Default, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "kebab-case")]
pub struct StructType {
    #[serde(rename = "type")]
    tag: StructTag,
    /// The struct's fields, in order.
    pub fields: Vec<StructField>,
    /// Unknown fields, preserved verbatim.
    #[serde(flatten)]
    pub extra: Map<String, Value>,
}

impl StructType {
    /// A struct type with the given fields.
    #[must_use]
    pub fn new(fields: Vec<StructField>) -> Self {
        Self {
            tag: StructTag::Struct,
            fields,
            extra: Map::new(),
        }
    }
}

/// A list type with an element id and element nullability.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "kebab-case")]
pub struct ListType {
    #[serde(rename = "type")]
    tag: ListTag,
    /// Field id of the element.
    pub element_id: i32,
    /// Element type.
    pub element: Box<Type>,
    /// Whether elements are required (non-null).
    pub element_required: bool,
    /// Unknown fields, preserved verbatim.
    #[serde(flatten)]
    pub extra: Map<String, Value>,
}

impl ListType {
    /// A list type with the given element.
    #[must_use]
    pub fn new(element_id: i32, element: Type, element_required: bool) -> Self {
        Self {
            tag: ListTag::List,
            element_id,
            element: Box::new(element),
            element_required,
            extra: Map::new(),
        }
    }
}

/// A map type with key/value ids and value nullability (keys are always
/// required).
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "kebab-case")]
pub struct MapType {
    #[serde(rename = "type")]
    tag: MapTag,
    /// Field id of the key.
    pub key_id: i32,
    /// Key type.
    pub key: Box<Type>,
    /// Field id of the value.
    pub value_id: i32,
    /// Value type.
    pub value: Box<Type>,
    /// Whether values are required (non-null).
    pub value_required: bool,
    /// Unknown fields, preserved verbatim.
    #[serde(flatten)]
    pub extra: Map<String, Value>,
}

impl MapType {
    /// A map type with the given key and value.
    #[must_use]
    pub fn new(key_id: i32, key: Type, value_id: i32, value: Type, value_required: bool) -> Self {
        Self {
            tag: MapTag::Map,
            key_id,
            key: Box::new(key),
            value_id,
            value: Box::new(value),
            value_required,
            extra: Map::new(),
        }
    }
}

/// Any Iceberg type: a primitive or a nested struct/list/map.
#[derive(Debug, Clone, PartialEq)]
pub enum Type {
    /// A primitive type (serialized as its string form).
    Primitive(PrimitiveType),
    /// A struct type.
    Struct(StructType),
    /// A list type.
    List(ListType),
    /// A map type.
    Map(MapType),
}

impl Type {
    /// Convenience constructor for a primitive type.
    #[must_use]
    pub fn primitive(primitive: PrimitiveType) -> Self {
        Self::Primitive(primitive)
    }

    /// Collects every field id defined *inside* this type (struct field ids,
    /// list element ids, map key/value ids), recursively, into `out`.
    pub fn collect_field_ids(&self, out: &mut Vec<i32>) {
        match self {
            Self::Primitive(_) => {}
            Self::Struct(s) => {
                for field in &s.fields {
                    out.push(field.id);
                    field.field_type.collect_field_ids(out);
                }
            }
            Self::List(l) => {
                out.push(l.element_id);
                l.element.collect_field_ids(out);
            }
            Self::Map(m) => {
                out.push(m.key_id);
                m.key.collect_field_ids(out);
                out.push(m.value_id);
                m.value.collect_field_ids(out);
            }
        }
    }

    /// Whether this type (recursively) contains a primitive that requires
    /// format version 3. Returns the offending primitive's string form.
    #[must_use]
    pub fn find_v3_primitive(&self) -> Option<String> {
        match self {
            Self::Primitive(p) => p.requires_v3().then(|| p.to_string()),
            Self::Struct(s) => s
                .fields
                .iter()
                .find_map(|f| f.field_type.find_v3_primitive()),
            Self::List(l) => l.element.find_v3_primitive(),
            Self::Map(m) => m
                .key
                .find_v3_primitive()
                .or_else(|| m.value.find_v3_primitive()),
        }
    }

    /// Whether this type (recursively) contains an unrecognized primitive.
    /// Returns the preserved string form.
    #[must_use]
    pub fn find_unrecognized_primitive(&self) -> Option<String> {
        match self {
            Self::Primitive(PrimitiveType::Other(other)) => Some(other.clone()),
            Self::Primitive(_) => None,
            Self::Struct(s) => s
                .fields
                .iter()
                .find_map(|f| f.field_type.find_unrecognized_primitive()),
            Self::List(l) => l.element.find_unrecognized_primitive(),
            Self::Map(m) => m
                .key
                .find_unrecognized_primitive()
                .or_else(|| m.value.find_unrecognized_primitive()),
        }
    }
}

impl Serialize for Type {
    fn serialize<S: Serializer>(&self, serializer: S) -> Result<S::Ok, S::Error> {
        match self {
            Self::Primitive(p) => p.serialize(serializer),
            Self::Struct(s) => s.serialize(serializer),
            Self::List(l) => l.serialize(serializer),
            Self::Map(m) => m.serialize(serializer),
        }
    }
}

impl<'de> Deserialize<'de> for Type {
    fn deserialize<D: Deserializer<'de>>(deserializer: D) -> Result<Self, D::Error> {
        let value = Value::deserialize(deserializer)?;
        match &value {
            Value::String(s) => {
                let primitive: PrimitiveType = s.parse().map_err(D::Error::custom)?;
                Ok(Self::Primitive(primitive))
            }
            Value::Object(obj) => match obj.get("type").and_then(Value::as_str) {
                Some("struct") => serde_json::from_value(value)
                    .map(Self::Struct)
                    .map_err(D::Error::custom),
                Some("list") => serde_json::from_value(value)
                    .map(Self::List)
                    .map_err(D::Error::custom),
                Some("map") => serde_json::from_value(value)
                    .map(Self::Map)
                    .map_err(D::Error::custom),
                Some(other) => Err(D::Error::custom(format!(
                    "unknown nested type {other:?}: expected \"struct\", \"list\", or \"map\""
                ))),
                None => Err(D::Error::custom(
                    "nested type object is missing the \"type\" discriminator",
                )),
            },
            _ => Err(D::Error::custom(
                "a type must be a primitive type string or a nested type object",
            )),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn round_trip(s: &str) -> PrimitiveType {
        let parsed: PrimitiveType = s.parse().expect("parse");
        assert_eq!(parsed.to_string(), s, "canonical form must round-trip");
        parsed
    }

    #[test]
    fn primitive_string_forms_round_trip_exactly() {
        for s in [
            "boolean",
            "int",
            "long",
            "float",
            "double",
            "decimal(10,2)",
            "decimal(38,38)",
            "date",
            "time",
            "timestamp",
            "timestamptz",
            "timestamp_ns",
            "timestamptz_ns",
            "string",
            "uuid",
            "fixed[16]",
            "binary",
            "variant",
            "geometry",
            "geometry(OGC:CRS84)",
            "geography",
            "geography(OGC:CRS84)",
            "geography(srid:4326,spherical)",
            "unknown",
        ] {
            round_trip(s);
        }
    }

    #[test]
    fn lenient_whitespace_is_normalized() {
        assert_eq!(
            "decimal(10, 2)".parse::<PrimitiveType>().expect("parse"),
            PrimitiveType::Decimal {
                precision: 10,
                scale: 2
            }
        );
        assert_eq!(
            "geography(srid:4326, karney)"
                .parse::<PrimitiveType>()
                .expect("parse")
                .to_string(),
            "geography(srid:4326,karney)"
        );
    }

    #[test]
    fn malformed_parameterized_types_are_rejected() {
        for s in [
            "decimal(a,b)",
            "decimal(10)",
            "decimal(0,0)",
            "decimal(39,2)",
            "decimal(5,6)",
            "fixed[]",
            "fixed[0]",
            "fixed[x]",
            "geometry()",
            "geography()",
            "decimal(10,2",
        ] {
            assert!(s.parse::<PrimitiveType>().is_err(), "{s} must be rejected");
        }
    }

    #[test]
    fn unrecognized_type_names_are_preserved() {
        let parsed: PrimitiveType = "tinyint".parse().expect("parse");
        assert_eq!(parsed, PrimitiveType::Other("tinyint".to_owned()));
        assert_eq!(parsed.to_string(), "tinyint");
    }

    #[test]
    fn nested_types_round_trip() {
        let json = serde_json::json!({
            "type": "map",
            "key-id": 4,
            "key": "string",
            "value-id": 5,
            "value": {
                "type": "list",
                "element-id": 6,
                "element": {
                    "type": "struct",
                    "fields": [
                        {"id": 7, "name": "x", "required": true, "type": "decimal(10,2)"}
                    ]
                },
                "element-required": false
            },
            "value-required": true
        });
        let parsed: Type = serde_json::from_value(json.clone()).expect("parse");
        let back = serde_json::to_value(&parsed).expect("serialize");
        assert_eq!(back, json);

        let mut ids = Vec::new();
        parsed.collect_field_ids(&mut ids);
        ids.sort_unstable();
        assert_eq!(ids, vec![4, 5, 6, 7]);
    }

    #[test]
    fn v3_primitives_are_detected() {
        let t = Type::List(ListType::new(
            2,
            Type::Primitive(PrimitiveType::TimestampNs),
            true,
        ));
        assert_eq!(t.find_v3_primitive(), Some("timestamp_ns".to_owned()));
        let plain = Type::Primitive(PrimitiveType::Long);
        assert_eq!(plain.find_v3_primitive(), None);
    }
}
