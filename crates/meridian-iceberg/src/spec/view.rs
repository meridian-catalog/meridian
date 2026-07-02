//! The Iceberg view spec model: view `metadata.json`.
//!
//! Views mirror tables structurally: metadata lives in immutable JSON files
//! swapped atomically by the catalog, and every struct here carries a
//! flattened `extra` map so fields (and whole representation types) this
//! model does not know survive a parse/serialize round trip verbatim.
//!
//! The view format has exactly one version: `format-version` must be 1
//! (enforced on parse and by the builder).

use std::collections::BTreeMap;

use serde::{Deserialize, Deserializer, Serialize};
use serde_json::{Map, Value};
use uuid::Uuid;

use super::schema::Schema;

/// The only view format version defined by the Iceberg view spec.
pub const VIEW_FORMAT_VERSION: u8 = 1;

/// Failure to parse a view `metadata.json` document.
#[derive(Debug, thiserror::Error)]
pub enum ViewMetadataParseError {
    /// The document is not valid JSON or does not match the metadata shape.
    #[error("invalid view metadata JSON: {0}")]
    Json(#[from] serde_json::Error),
    /// The document is structurally JSON but not valid view metadata.
    #[error("invalid view metadata: {0}")]
    Invalid(String),
    /// The document declares a `format-version` other than 1. Versions
    /// beyond `u8::MAX` are reported as `u8::MAX`.
    #[error("unsupported view format-version {found} (supported: 1)")]
    UnsupportedFormatVersion {
        /// The declared format version.
        found: u8,
    },
}

/// Iceberg view metadata: the top-level view `metadata.json` model.
///
/// Anything not modelled is preserved untouched in [`ViewMetadata::extra`]
/// (and the `extra` maps of every nested struct).
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "kebab-case")]
pub struct ViewMetadata {
    /// View UUID, stable for the lifetime of the view. Implementations must
    /// fail if the UUID changes across a metadata refresh.
    pub view_uuid: Uuid,
    /// Format version; must be 1.
    pub format_version: u8,
    /// Base location of the view; used to create metadata file locations.
    pub location: String,
    /// All known schemas.
    pub schemas: Vec<Schema>,
    /// `version-id` of the current version in [`ViewMetadata::versions`].
    pub current_version_id: i32,
    /// All retained versions of the view.
    pub versions: Vec<ViewVersion>,
    /// History of `current-version-id` changes.
    pub version_log: Vec<ViewHistoryEntry>,
    /// View properties (string key/value), e.g. `comment` and maintenance
    /// settings such as `version.history.num-entries`.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub properties: Option<BTreeMap<String, String>>,
    /// Unknown fields, preserved verbatim.
    #[serde(flatten)]
    pub extra: Map<String, Value>,
}

impl ViewMetadata {
    /// Parses view metadata from a JSON string.
    pub fn from_json(json: &str) -> Result<Self, ViewMetadataParseError> {
        let value: Value = serde_json::from_str(json)?;
        let format_version = value
            .get("format-version")
            .and_then(Value::as_u64)
            .ok_or_else(|| {
                ViewMetadataParseError::Invalid("missing or non-integer format-version".to_owned())
            })?;
        if format_version != u64::from(VIEW_FORMAT_VERSION) {
            return Err(ViewMetadataParseError::UnsupportedFormatVersion {
                found: u8::try_from(format_version).unwrap_or(u8::MAX),
            });
        }
        Ok(serde_json::from_value(value)?)
    }

    /// Serializes view metadata to a JSON string.
    pub fn to_json(&self) -> Result<String, serde_json::Error> {
        serde_json::to_string(self)
    }

    /// The current version, if `current-version-id` resolves.
    #[must_use]
    pub fn current_version(&self) -> Option<&ViewVersion> {
        self.version_by_id(self.current_version_id)
    }

    /// The version with the given id, if present.
    #[must_use]
    pub fn version_by_id(&self, version_id: i32) -> Option<&ViewVersion> {
        self.versions.iter().find(|v| v.version_id == version_id)
    }

    /// The schema with the given id, if present.
    #[must_use]
    pub fn schema_by_id(&self, schema_id: i32) -> Option<&Schema> {
        self.schemas.iter().find(|s| s.schema_id == Some(schema_id))
    }

    /// The current version's schema, if both the current version and its
    /// `schema-id` resolve.
    #[must_use]
    pub fn current_schema(&self) -> Option<&Schema> {
        self.schema_by_id(self.current_version()?.schema_id)
    }

    /// The value of a view property, if set.
    #[must_use]
    pub fn property(&self, key: &str) -> Option<&str> {
        self.properties.as_ref()?.get(key).map(String::as_str)
    }
}

/// One version of a view: the state of the view definition at a point in
/// time.
///
/// Versions are immutable; changing the definition (or adding
/// representations) creates a new version.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "kebab-case")]
pub struct ViewVersion {
    /// Unique id for the version within the view.
    pub version_id: i32,
    /// When the version was created (epoch millis).
    pub timestamp_ms: i64,
    /// Id of this version's schema in [`ViewMetadata::schemas`]. In an
    /// `add-view-version` update, `-1` means the schema last added in the
    /// same update batch.
    pub schema_id: i32,
    /// Summary metadata about the version (e.g. `engine-name`,
    /// `engine-version`).
    pub summary: BTreeMap<String, String>,
    /// Representations of the view definition. All representations of one
    /// version must express the same underlying definition.
    pub representations: Vec<ViewRepresentation>,
    /// Catalog to use for table references that carry no catalog. When
    /// absent, the catalog storing the view is the default.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub default_catalog: Option<String>,
    /// Namespace to use for single-identifier table references.
    pub default_namespace: Vec<String>,
    /// Unknown fields, preserved verbatim.
    #[serde(flatten)]
    pub extra: Map<String, Value>,
}

impl ViewVersion {
    /// Definition equality ignoring `version-id` and `timestamp-ms` (used to
    /// detect re-adds of an existing version, mirroring the reference
    /// implementation).
    #[must_use]
    pub fn same_definition(&self, other: &Self) -> bool {
        self.schema_id == other.schema_id
            && self.summary == other.summary
            && self.representations == other.representations
            && self.default_catalog == other.default_catalog
            && self.default_namespace == other.default_namespace
            && self.extra == other.extra
    }
}

/// One representation of a view definition, discriminated by `type`.
///
/// The spec currently defines only the `sql` type. Objects with any other
/// `type` (or none) are preserved verbatim through
/// [`ViewRepresentation::Other`] so metadata written by newer tools
/// round-trips losslessly; the builder accepts them as opaque payloads.
#[derive(Debug, Clone, PartialEq, Serialize)]
#[serde(untagged)]
pub enum ViewRepresentation {
    /// A SQL SELECT statement in a named dialect.
    Sql(SqlViewRepresentation),
    /// A representation type this model does not recognize, preserved
    /// verbatim.
    Other(Map<String, Value>),
}

impl ViewRepresentation {
    /// A `sql` representation from a statement and dialect.
    #[must_use]
    pub fn sql(sql: impl Into<String>, dialect: impl Into<String>) -> Self {
        Self::Sql(SqlViewRepresentation::new(sql, dialect))
    }

    /// This representation as a SQL representation, if it is one.
    #[must_use]
    pub fn as_sql(&self) -> Option<&SqlViewRepresentation> {
        match self {
            Self::Sql(sql) => Some(sql),
            Self::Other(_) => None,
        }
    }
}

impl<'de> Deserialize<'de> for ViewRepresentation {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: Deserializer<'de>,
    {
        // Dispatch on the `type` discriminator by hand: a malformed `sql`
        // representation must be a parse error, not silently preserved as an
        // unknown type.
        let object = Map::<String, Value>::deserialize(deserializer)?;
        if object.get("type").and_then(Value::as_str) == Some("sql") {
            serde_json::from_value(Value::Object(object))
                .map(Self::Sql)
                .map_err(serde::de::Error::custom)
        } else {
            Ok(Self::Other(object))
        }
    }
}

/// Marker for the `"type": "sql"` discriminator.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq, Serialize, Deserialize)]
enum SqlTag {
    /// The only value: `"sql"`.
    #[default]
    #[serde(rename = "sql")]
    Sql,
}

/// The SQL representation of a view definition.
///
/// A version can carry multiple SQL representations, but at most one per
/// dialect (the builder enforces this, case-insensitively).
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "kebab-case")]
pub struct SqlViewRepresentation {
    /// Always `"sql"`.
    #[serde(rename = "type", default)]
    tag: SqlTag,
    /// The SQL SELECT statement.
    pub sql: String,
    /// The dialect of the statement (e.g. `"spark"`, `"trino"`).
    pub dialect: String,
    /// Unknown fields, preserved verbatim.
    #[serde(flatten)]
    pub extra: Map<String, Value>,
}

impl SqlViewRepresentation {
    /// A SQL representation from a statement and dialect.
    #[must_use]
    pub fn new(sql: impl Into<String>, dialect: impl Into<String>) -> Self {
        Self {
            tag: SqlTag::Sql,
            sql: sql.into(),
            dialect: dialect.into(),
            extra: Map::new(),
        }
    }
}

/// An entry in the version log (current-version history).
///
/// A version id can appear multiple times: setting the current version back
/// to an older one records a new entry rather than rewriting history.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "kebab-case")]
pub struct ViewHistoryEntry {
    /// Version that became current.
    pub version_id: i32,
    /// When it became current (epoch millis).
    pub timestamp_ms: i64,
}
