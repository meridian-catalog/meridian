//! The REST commit `updates` list: every table-changing action.
//!
//! Action names and payload shapes follow the Iceberg REST catalog `OpenAPI`
//! specification (`TableUpdate` and the per-action `*Update` schemas). View-only
//! actions (`add-view-version`, `set-current-view-version`) are not table
//! updates and are deliberately absent.

use std::collections::BTreeMap;

use serde::{Deserialize, Serialize};
use uuid::Uuid;

use super::encryption::EncryptedKey;
use super::partition::PartitionSpec;
use super::schema::Schema;
use super::snapshot::{Snapshot, SnapshotRef};
use super::sort::SortOrder;
use super::statistics::{PartitionStatisticsFile, StatisticsFile};

/// Sentinel accepted by `set-current-schema`, `set-default-spec`, and
/// `set-default-sort-order`: use the id assigned to the last added
/// schema/spec/order in the same update batch.
pub const LAST_ADDED: i32 = -1;

/// One update in a commit request, tagged by `action`.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(
    tag = "action",
    rename_all = "kebab-case",
    rename_all_fields = "kebab-case"
)]
pub enum TableUpdate {
    /// Assign the table UUID. Only valid while creating a table.
    AssignUuid {
        /// The UUID to assign.
        uuid: Uuid,
    },
    /// Upgrade the format version (forward only).
    UpgradeFormatVersion {
        /// The target format version.
        format_version: u8,
    },
    /// Add a schema. The builder assigns the schema id.
    AddSchema {
        /// The schema to add (its `schema-id`, if present, is ignored).
        schema: Schema,
        /// **Deprecated in the REST spec.** When present it is validated
        /// against the ids actually used by `schema`.
        #[serde(skip_serializing_if = "Option::is_none")]
        last_column_id: Option<i32>,
    },
    /// Set the current schema.
    SetCurrentSchema {
        /// Schema id to set as current, or [`LAST_ADDED`] (`-1`) for the
        /// last added schema.
        schema_id: i32,
    },
    /// Add a partition spec. The builder assigns the spec id and any missing
    /// partition field ids.
    AddSpec {
        /// The spec to add (its `spec-id`, if present, is ignored).
        spec: PartitionSpec,
    },
    /// Set the default partition spec.
    SetDefaultSpec {
        /// Spec id to set as default, or [`LAST_ADDED`] (`-1`) for the last
        /// added spec.
        spec_id: i32,
    },
    /// Add a sort order. The builder assigns the order id.
    AddSortOrder {
        /// The sort order to add (its `order-id` is ignored; empty orders
        /// are the unsorted order 0).
        sort_order: SortOrder,
    },
    /// Set the default sort order.
    SetDefaultSortOrder {
        /// Sort order id to set as default, or [`LAST_ADDED`] (`-1`) for the
        /// last added order.
        sort_order_id: i32,
    },
    /// Add a snapshot.
    AddSnapshot {
        /// The snapshot to add.
        snapshot: Snapshot,
    },
    /// Create or move a branch/tag reference.
    SetSnapshotRef {
        /// The reference name (`main` is the current-snapshot branch).
        ref_name: String,
        /// The reference itself (snapshot id, type, retention).
        #[serde(flatten)]
        reference: SnapshotRef,
    },
    /// Remove snapshots by id.
    RemoveSnapshots {
        /// Ids of the snapshots to remove.
        snapshot_ids: Vec<i64>,
    },
    /// Remove a branch/tag reference.
    RemoveSnapshotRef {
        /// The reference name.
        ref_name: String,
    },
    /// Set the table base location.
    SetLocation {
        /// The new location.
        location: String,
    },
    /// Set (upsert) table properties.
    SetProperties {
        /// Properties to set.
        updates: BTreeMap<String, String>,
    },
    /// Remove table properties (missing keys are ignored).
    RemoveProperties {
        /// Property keys to remove.
        removals: Vec<String>,
    },
    /// Set (upsert) the statistics file for a snapshot.
    SetStatistics {
        /// **Deprecated in the REST spec.** When present it must match
        /// `statistics.snapshot-id`.
        #[serde(skip_serializing_if = "Option::is_none")]
        snapshot_id: Option<i64>,
        /// The statistics file.
        statistics: StatisticsFile,
    },
    /// Remove the statistics file for a snapshot.
    RemoveStatistics {
        /// Snapshot whose statistics to remove.
        snapshot_id: i64,
    },
    /// Set (upsert) the partition-statistics file for a snapshot.
    SetPartitionStatistics {
        /// The partition-statistics file.
        partition_statistics: PartitionStatisticsFile,
    },
    /// Remove the partition-statistics file for a snapshot.
    RemovePartitionStatistics {
        /// Snapshot whose partition statistics to remove.
        snapshot_id: i64,
    },
    /// Remove partition specs by id (the default spec cannot be removed).
    RemovePartitionSpecs {
        /// Ids of the specs to remove.
        spec_ids: Vec<i32>,
    },
    /// Remove schemas by id (the current schema cannot be removed).
    RemoveSchemas {
        /// Ids of the schemas to remove.
        schema_ids: Vec<i32>,
    },
    /// Add an encryption key (v3).
    AddEncryptionKey {
        /// The key to add.
        encryption_key: EncryptedKey,
    },
    /// Remove an encryption key by id (v3).
    RemoveEncryptionKey {
        /// Id of the key to remove.
        key_id: String,
    },
}
