//! Iceberg snapshot, log, and ref models.

use std::collections::BTreeMap;

use serde::{Deserialize, Serialize};
use serde_json::{Map, Value};

/// A snapshot: the state of a table at a point in time.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "kebab-case")]
pub struct Snapshot {
    /// Unique snapshot id.
    pub snapshot_id: i64,
    /// Parent snapshot id, absent for the first snapshot.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub parent_snapshot_id: Option<i64>,
    /// Commit sequence number. Required in v2+; absent in v1 files.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub sequence_number: Option<i64>,
    /// When the snapshot was committed (epoch millis).
    pub timestamp_ms: i64,
    /// Location of the manifest-list file. Required in v2+; v1 files may
    /// carry an inline `manifests` array instead (preserved via `extra`).
    #[serde(skip_serializing_if = "Option::is_none")]
    pub manifest_list: Option<String>,
    /// Snapshot summary: `operation` plus free-form string metrics.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub summary: Option<BTreeMap<String, String>>,
    /// Schema id current when this snapshot was written.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub schema_id: Option<i32>,
    /// v3 row lineage: the first `_row_id` assigned to rows in this
    /// snapshot.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub first_row_id: Option<i64>,
    /// v3 row lineage: upper bound of the number of rows assigned row ids
    /// by this snapshot.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub added_rows: Option<i64>,
    /// Unknown fields (e.g. v1 inline `manifests`, encryption
    /// `key-metadata`), preserved verbatim.
    #[serde(flatten)]
    pub extra: Map<String, Value>,
}

/// An entry in the snapshot log (current-snapshot history).
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "kebab-case")]
pub struct SnapshotLogEntry {
    /// Snapshot that became current.
    pub snapshot_id: i64,
    /// When it became current (epoch millis).
    pub timestamp_ms: i64,
}

/// An entry in the metadata log (previous metadata.json files).
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "kebab-case")]
pub struct MetadataLogEntry {
    /// Location of the previous metadata file.
    pub metadata_file: String,
    /// When it was current (epoch millis).
    pub timestamp_ms: i64,
}

/// A named reference (branch or tag) to a snapshot.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "kebab-case")]
pub struct SnapshotRef {
    /// Referenced snapshot id.
    pub snapshot_id: i64,
    /// `"branch"` or `"tag"`.
    #[serde(rename = "type")]
    pub ref_type: RefType,
    /// Branch retention: minimum snapshots to keep.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub min_snapshots_to_keep: Option<i32>,
    /// Branch retention: maximum snapshot age in millis.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub max_snapshot_age_ms: Option<i64>,
    /// Retention: maximum ref age in millis.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub max_ref_age_ms: Option<i64>,
    /// Unknown fields, preserved verbatim.
    #[serde(flatten)]
    pub extra: Map<String, Value>,
}

/// Kind of snapshot reference.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub enum RefType {
    /// A mutable branch head.
    #[serde(rename = "branch")]
    Branch,
    /// An immutable tag.
    #[serde(rename = "tag")]
    Tag,
}
