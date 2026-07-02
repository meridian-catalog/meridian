//! The top-level table-metadata model (`metadata.json`).

use std::collections::BTreeMap;

use serde::{Deserialize, Serialize};
use serde_json::{Map, Value};
use uuid::Uuid;

use super::partition::PartitionSpec;
use super::schema::Schema;
use super::snapshot::{MetadataLogEntry, Snapshot, SnapshotLogEntry, SnapshotRef};
use super::sort::SortOrder;

/// Iceberg table metadata, modelling the v2 shape.
///
/// Fields that are required by the v2 spec are required here; a v1 file
/// (legacy single `schema` / `partition-spec`) will fail to parse rather
/// than be silently mangled. TODO(M1): v1 ingestion and v3 completeness.
///
/// Anything not modelled (e.g. `statistics`, `partition-statistics`, v3
/// `next-row-id`) is preserved untouched in [`TableMetadata::extra`].
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "kebab-case")]
pub struct TableMetadata {
    /// Format version. 1, 2, or 3; only 2 is fully modelled in M0.
    pub format_version: u8,
    /// Table UUID, stable for the lifetime of the table.
    pub table_uuid: Uuid,
    /// Base location of the table.
    pub location: String,
    /// Highest assigned commit sequence number. Required in v2+.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub last_sequence_number: Option<i64>,
    /// When the metadata was last updated (epoch millis).
    pub last_updated_ms: i64,
    /// Highest assigned column field id.
    pub last_column_id: i32,
    /// All known schemas.
    pub schemas: Vec<Schema>,
    /// Id of the current schema in [`TableMetadata::schemas`].
    pub current_schema_id: i32,
    /// All known partition specs.
    pub partition_specs: Vec<PartitionSpec>,
    /// Id of the default partition spec.
    pub default_spec_id: i32,
    /// Highest assigned partition field id.
    pub last_partition_id: i32,
    /// All known sort orders.
    pub sort_orders: Vec<SortOrder>,
    /// Id of the default sort order.
    pub default_sort_order_id: i32,
    /// Table properties (string key/value).
    #[serde(skip_serializing_if = "Option::is_none")]
    pub properties: Option<BTreeMap<String, String>>,
    /// Id of the current snapshot, if any. May be `-1`/absent when the table
    /// has no snapshots.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub current_snapshot_id: Option<i64>,
    /// All retained snapshots.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub snapshots: Option<Vec<Snapshot>>,
    /// History of current-snapshot changes.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub snapshot_log: Option<Vec<SnapshotLogEntry>>,
    /// History of previous metadata files.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub metadata_log: Option<Vec<MetadataLogEntry>>,
    /// Named branch/tag references.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub refs: Option<BTreeMap<String, SnapshotRef>>,
    /// Unknown fields, preserved verbatim (e.g. `statistics`,
    /// `partition-statistics`, v3 fields).
    #[serde(flatten)]
    pub extra: Map<String, Value>,
}

impl TableMetadata {
    /// Parses table metadata from a JSON string.
    pub fn from_json(json: &str) -> Result<Self, serde_json::Error> {
        serde_json::from_str(json)
    }

    /// Serializes table metadata to a JSON string.
    pub fn to_json(&self) -> Result<String, serde_json::Error> {
        serde_json::to_string(self)
    }

    /// The current schema, if `current-schema-id` resolves.
    #[must_use]
    pub fn current_schema(&self) -> Option<&Schema> {
        self.schemas
            .iter()
            .find(|s| s.schema_id == self.current_schema_id)
    }

    /// The current snapshot, if one is set and present.
    #[must_use]
    pub fn current_snapshot(&self) -> Option<&Snapshot> {
        let id = self.current_snapshot_id.filter(|id| *id >= 0)?;
        self.snapshots
            .as_ref()?
            .iter()
            .find(|s| s.snapshot_id == id)
    }
}
