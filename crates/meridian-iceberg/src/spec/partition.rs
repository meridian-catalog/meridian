//! Iceberg partition-spec model.

use serde::{Deserialize, Serialize};
use serde_json::{Map, Value};

use super::transform::Transform;

/// A partition spec: how data files are split into partitions.
///
/// `spec-id` is optional because the REST `add-spec` update carries specs
/// without server-assigned ids; every spec stored in
/// [`crate::spec::TableMetadata`] has one (the builder assigns and validates
/// this).
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "kebab-case")]
pub struct PartitionSpec {
    /// Unique spec id within the table; absent on unassigned request specs.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub spec_id: Option<i32>,
    /// Partition fields, in partition-tuple order.
    pub fields: Vec<PartitionField>,
    /// Unknown fields, preserved verbatim.
    #[serde(flatten)]
    pub extra: Map<String, Value>,
}

impl PartitionSpec {
    /// The unpartitioned spec (no fields) with the given id.
    #[must_use]
    pub fn unpartitioned(spec_id: i32) -> Self {
        Self {
            spec_id: Some(spec_id),
            fields: Vec::new(),
            extra: Map::new(),
        }
    }

    /// A spec with the given fields and no assigned id.
    #[must_use]
    pub fn new(fields: Vec<PartitionField>) -> Self {
        Self {
            spec_id: None,
            fields,
            extra: Map::new(),
        }
    }

    /// The highest assigned partition field id in this spec, if any field
    /// has one.
    #[must_use]
    pub fn max_assigned_field_id(&self) -> Option<i32> {
        self.fields.iter().filter_map(|f| f.field_id).max()
    }

    /// Structural equality ignoring `spec-id` (used to detect re-adds of an
    /// existing spec).
    #[must_use]
    pub fn same_structure(&self, other: &Self) -> bool {
        self.fields == other.fields && self.extra == other.extra
    }
}

/// One partition field.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "kebab-case")]
pub struct PartitionField {
    /// Partition field id (stable across spec evolution); absent on
    /// unassigned request fields — the builder assigns it.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub field_id: Option<i32>,
    /// Source column field id in the table schema.
    ///
    /// TODO(M1+): v3 multi-argument transforms use `source-ids` instead;
    /// currently preserved via `extra` and not validated.
    pub source_id: i32,
    /// Partition field name.
    pub name: String,
    /// Transform, e.g. `identity`, `bucket[16]`, `day`.
    pub transform: Transform,
    /// Unknown fields, preserved verbatim.
    #[serde(flatten)]
    pub extra: Map<String, Value>,
}

impl PartitionField {
    /// A partition field with no assigned field id.
    #[must_use]
    pub fn new(source_id: i32, name: impl Into<String>, transform: Transform) -> Self {
        Self {
            field_id: None,
            source_id,
            name: name.into(),
            transform,
            extra: Map::new(),
        }
    }
}
