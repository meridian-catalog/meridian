//! Iceberg schema model.

use serde::{Deserialize, Serialize};
use serde_json::{Map, Value};

/// A table schema: a named struct type with a schema id.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "kebab-case")]
pub struct Schema {
    /// Always `"struct"` for a top-level schema.
    #[serde(rename = "type")]
    pub struct_type: String,
    /// Unique schema id within the table.
    pub schema_id: i32,
    /// Field ids that identify rows (Iceberg identifier fields).
    #[serde(skip_serializing_if = "Option::is_none")]
    pub identifier_field_ids: Option<Vec<i32>>,
    /// Top-level fields.
    pub fields: Vec<SchemaField>,
    /// Unknown fields, preserved verbatim.
    #[serde(flatten)]
    pub extra: Map<String, Value>,
}

/// One field of a struct type.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "kebab-case")]
pub struct SchemaField {
    /// Field id, unique within the table across schema evolution.
    pub id: i32,
    /// Field name.
    pub name: String,
    /// Whether values are required (non-null).
    pub required: bool,
    /// The field type: a primitive type string (e.g. `"long"`) or a nested
    /// struct/list/map object.
    ///
    /// TODO(M1): replace with a typed type tree; passed through as raw JSON
    /// for now so nothing is lost.
    #[serde(rename = "type")]
    pub field_type: Value,
    /// Optional documentation string.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub doc: Option<String>,
    /// Unknown fields (e.g. `initial-default`, `write-default`), preserved
    /// verbatim.
    #[serde(flatten)]
    pub extra: Map<String, Value>,
}
