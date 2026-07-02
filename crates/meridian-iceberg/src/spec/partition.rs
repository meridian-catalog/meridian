//! Iceberg partition-spec model.

use serde::{Deserialize, Serialize};
use serde_json::{Map, Value};

/// A partition spec: how data files are split into partitions.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "kebab-case")]
pub struct PartitionSpec {
    /// Unique spec id within the table.
    pub spec_id: i32,
    /// Partition fields, in partition-tuple order.
    pub fields: Vec<PartitionField>,
    /// Unknown fields, preserved verbatim.
    #[serde(flatten)]
    pub extra: Map<String, Value>,
}

/// One partition field.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "kebab-case")]
pub struct PartitionField {
    /// Source column field id in the table schema.
    ///
    /// TODO(M1): v3 multi-argument transforms use `source-ids` instead;
    /// currently preserved via `extra`.
    pub source_id: i32,
    /// Partition field id (assigned, stable across spec evolution).
    pub field_id: i32,
    /// Partition field name.
    pub name: String,
    /// Transform, e.g. `"identity"`, `"bucket[16]"`, `"day"`.
    pub transform: String,
    /// Unknown fields, preserved verbatim.
    #[serde(flatten)]
    pub extra: Map<String, Value>,
}
