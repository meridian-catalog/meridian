//! Table and partition statistics file models (Puffin statistics).

use std::collections::BTreeMap;

use serde::{Deserialize, Serialize};
use serde_json::{Map, Value};

/// A statistics file (Puffin) attached to a snapshot.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "kebab-case")]
pub struct StatisticsFile {
    /// Snapshot the statistics were computed for.
    pub snapshot_id: i64,
    /// Object-storage location of the statistics file.
    pub statistics_path: String,
    /// Total file size in bytes.
    pub file_size_in_bytes: i64,
    /// Size of the Puffin footer in bytes.
    pub file_footer_size_in_bytes: i64,
    /// Metadata for each blob in the file.
    pub blob_metadata: Vec<BlobMetadata>,
    /// Unknown fields, preserved verbatim.
    #[serde(flatten)]
    pub extra: Map<String, Value>,
}

/// Metadata for one blob inside a statistics file.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "kebab-case")]
pub struct BlobMetadata {
    /// Blob type, e.g. `apache-datasketches-theta-v1`.
    #[serde(rename = "type")]
    pub blob_type: String,
    /// Snapshot the blob was computed from.
    pub snapshot_id: i64,
    /// Sequence number of that snapshot.
    pub sequence_number: i64,
    /// Field ids the blob covers.
    pub fields: Vec<i32>,
    /// Free-form blob properties.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub properties: Option<BTreeMap<String, String>>,
    /// Unknown fields, preserved verbatim.
    #[serde(flatten)]
    pub extra: Map<String, Value>,
}

/// A partition-statistics file attached to a snapshot.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "kebab-case")]
pub struct PartitionStatisticsFile {
    /// Snapshot the statistics were computed for.
    pub snapshot_id: i64,
    /// Object-storage location of the partition-statistics file.
    pub statistics_path: String,
    /// Total file size in bytes.
    pub file_size_in_bytes: i64,
    /// Unknown fields, preserved verbatim.
    #[serde(flatten)]
    pub extra: Map<String, Value>,
}
