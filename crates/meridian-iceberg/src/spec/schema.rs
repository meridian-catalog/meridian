//! Iceberg schema model.

use std::collections::BTreeSet;

use serde::{Deserialize, Serialize};
use serde_json::{Map, Value};

use super::types::{StructField, StructTag};

/// A table schema: a named struct type with a schema id.
///
/// `schema-id` is optional because the REST `add-schema` update carries
/// schemas without server-assigned ids; every schema stored in
/// [`crate::spec::TableMetadata`] has one (the builder assigns and validates
/// this).
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "kebab-case")]
pub struct Schema {
    /// Always `"struct"` for a top-level schema.
    #[serde(rename = "type", default)]
    tag: StructTag,
    /// Unique schema id within the table; absent on unassigned request
    /// schemas.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub schema_id: Option<i32>,
    /// Field ids that identify rows (Iceberg identifier fields).
    #[serde(skip_serializing_if = "Option::is_none")]
    pub identifier_field_ids: Option<Vec<i32>>,
    /// Top-level fields.
    pub fields: Vec<StructField>,
    /// Unknown fields, preserved verbatim.
    #[serde(flatten)]
    pub extra: Map<String, Value>,
}

impl Schema {
    /// A schema with the given fields and no assigned id.
    #[must_use]
    pub fn new(fields: Vec<StructField>) -> Self {
        Self {
            tag: StructTag::Struct,
            schema_id: None,
            identifier_field_ids: None,
            fields,
            extra: Map::new(),
        }
    }

    /// This schema with `schema-id` set.
    #[must_use]
    pub fn with_schema_id(mut self, schema_id: i32) -> Self {
        self.schema_id = Some(schema_id);
        self
    }

    /// Every field id defined anywhere in this schema (top-level and
    /// nested), in definition order. May contain duplicates if the schema is
    /// invalid; see [`Schema::all_field_ids`].
    #[must_use]
    pub fn field_ids_in_order(&self) -> Vec<i32> {
        let mut ids = Vec::new();
        for field in &self.fields {
            ids.push(field.id);
            field.field_type.collect_field_ids(&mut ids);
        }
        ids
    }

    /// Every distinct field id defined anywhere in this schema.
    #[must_use]
    pub fn all_field_ids(&self) -> BTreeSet<i32> {
        self.field_ids_in_order().into_iter().collect()
    }

    /// The highest field id defined in this schema, or 0 for an empty
    /// schema.
    #[must_use]
    pub fn max_field_id(&self) -> i32 {
        self.field_ids_in_order().into_iter().max().unwrap_or(0)
    }

    /// Structural equality ignoring `schema-id` (used to detect re-adds of
    /// an existing schema).
    #[must_use]
    pub fn same_structure(&self, other: &Self) -> bool {
        self.fields == other.fields
            && self.identifier_field_ids == other.identifier_field_ids
            && self.extra == other.extra
    }
}
