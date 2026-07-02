//! Iceberg sort-order model.

use serde::{Deserialize, Serialize};
use serde_json::{Map, Value};

use super::transform::Transform;

/// A sort order: how rows are sorted within data files.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "kebab-case")]
pub struct SortOrder {
    /// Unique order id within the table. Id 0 is reserved for "unsorted".
    pub order_id: i32,
    /// Sort fields, most significant first.
    pub fields: Vec<SortField>,
    /// Unknown fields, preserved verbatim.
    #[serde(flatten)]
    pub extra: Map<String, Value>,
}

impl SortOrder {
    /// The unsorted order: id 0, no fields.
    #[must_use]
    pub fn unsorted() -> Self {
        Self {
            order_id: 0,
            fields: Vec::new(),
            extra: Map::new(),
        }
    }

    /// Structural equality ignoring `order-id` (used to detect re-adds of
    /// an existing order).
    #[must_use]
    pub fn same_structure(&self, other: &Self) -> bool {
        self.fields == other.fields && self.extra == other.extra
    }
}

/// One sort field.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "kebab-case")]
pub struct SortField {
    /// Transform applied before sorting, e.g. `identity`.
    pub transform: Transform,
    /// Source column field id in the table schema.
    pub source_id: i32,
    /// Sort direction.
    pub direction: SortDirection,
    /// Where nulls sort.
    pub null_order: NullOrder,
    /// Unknown fields, preserved verbatim.
    #[serde(flatten)]
    pub extra: Map<String, Value>,
}

/// Sort direction.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub enum SortDirection {
    /// Ascending.
    #[serde(rename = "asc")]
    Asc,
    /// Descending.
    #[serde(rename = "desc")]
    Desc,
}

/// Null ordering.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub enum NullOrder {
    /// Nulls sort before all values.
    #[serde(rename = "nulls-first")]
    NullsFirst,
    /// Nulls sort after all values.
    #[serde(rename = "nulls-last")]
    NullsLast,
}
