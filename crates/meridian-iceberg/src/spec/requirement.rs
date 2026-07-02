//! The REST commit `requirements` list: assertions about the state a commit
//! was built against, checked by the catalog before applying updates.
//!
//! Requirement names and payload shapes follow the Iceberg REST catalog
//! `OpenAPI` spec (`TableRequirement` and the per-assertion schemas). A failed
//! check maps to `409 CommitFailedException` at the API boundary.

use serde::{Deserialize, Serialize};
use uuid::Uuid;

use super::table_metadata::TableMetadata;

/// One commit requirement, tagged by `type`.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(
    tag = "type",
    rename_all = "kebab-case",
    rename_all_fields = "kebab-case"
)]
pub enum TableRequirement {
    /// The table must not already exist; used for create transactions.
    AssertCreate,
    /// The table UUID must match.
    AssertTableUuid {
        /// The expected UUID.
        uuid: Uuid,
    },
    /// The named branch/tag must reference the given snapshot; a `null`
    /// snapshot id asserts the reference does not exist.
    AssertRefSnapshotId {
        /// The reference name.
        #[serde(rename = "ref")]
        r#ref: String,
        /// The expected snapshot id, or `None` to assert the ref is absent.
        snapshot_id: Option<i64>,
    },
    /// The table's `last-column-id` must match.
    AssertLastAssignedFieldId {
        /// The expected last assigned column field id.
        last_assigned_field_id: i32,
    },
    /// The table's `current-schema-id` must match.
    AssertCurrentSchemaId {
        /// The expected current schema id.
        current_schema_id: i32,
    },
    /// The table's `last-partition-id` must match.
    AssertLastAssignedPartitionId {
        /// The expected last assigned partition field id.
        last_assigned_partition_id: i32,
    },
    /// The table's `default-spec-id` must match.
    AssertDefaultSpecId {
        /// The expected default partition spec id.
        default_spec_id: i32,
    },
    /// The table's `default-sort-order-id` must match.
    AssertDefaultSortOrderId {
        /// The expected default sort order id.
        default_sort_order_id: i32,
    },
}

/// A commit requirement that does not hold against the current table state.
///
/// Maps to `409 CommitFailedException`: the client must refresh the table
/// and rebuild its commit. The message states exactly which assertion failed
/// and what the actual state was.
#[derive(Debug, Clone, PartialEq, Eq, thiserror::Error)]
#[error("commit requirement failed: {0}")]
pub struct RequirementFailed(String);

impl RequirementFailed {
    /// Shared with the view requirement module: table and view requirement
    /// failures carry the same client-facing contract.
    pub(crate) fn new(message: impl Into<String>) -> Self {
        Self(message.into())
    }
}

impl TableRequirement {
    /// Checks this requirement against the current table state (`None` when
    /// the table does not exist).
    pub fn check(&self, metadata: Option<&TableMetadata>) -> Result<(), RequirementFailed> {
        if let Self::AssertCreate = self {
            return match metadata {
                None => Ok(()),
                Some(_) => Err(RequirementFailed::new(
                    "table must not already exist (assert-create)",
                )),
            };
        }
        let Some(metadata) = metadata else {
            return Err(RequirementFailed::new(
                "table does not exist, so the requirement cannot hold",
            ));
        };
        self.check_existing(metadata)
    }

    fn check_existing(&self, metadata: &TableMetadata) -> Result<(), RequirementFailed> {
        match self {
            Self::AssertCreate => Ok(()), // handled in `check`
            Self::AssertTableUuid { uuid } => {
                if metadata.table_uuid == *uuid {
                    Ok(())
                } else {
                    Err(RequirementFailed::new(format!(
                        "table UUID must be {uuid}, found {}",
                        metadata.table_uuid
                    )))
                }
            }
            Self::AssertRefSnapshotId { r#ref, snapshot_id } => {
                let actual = metadata
                    .refs
                    .as_ref()
                    .and_then(|refs| refs.get(r#ref))
                    .map(|r| r.snapshot_id);
                match (snapshot_id, actual) {
                    (Some(expected), Some(actual)) if *expected == actual => Ok(()),
                    (Some(expected), Some(actual)) => Err(RequirementFailed::new(format!(
                        "ref {ref:?} must point at snapshot {expected}, found {actual}",
                    ))),
                    (Some(expected), None) => Err(RequirementFailed::new(format!(
                        "ref {ref:?} must point at snapshot {expected}, but the ref does not exist",
                    ))),
                    (None, Some(actual)) => Err(RequirementFailed::new(format!(
                        "ref {ref:?} must not exist, but points at snapshot {actual}",
                    ))),
                    (None, None) => Ok(()),
                }
            }
            Self::AssertLastAssignedFieldId {
                last_assigned_field_id,
            } => {
                if metadata.last_column_id == *last_assigned_field_id {
                    Ok(())
                } else {
                    Err(RequirementFailed::new(format!(
                        "last assigned field id must be {last_assigned_field_id}, found {}",
                        metadata.last_column_id
                    )))
                }
            }
            Self::AssertCurrentSchemaId { current_schema_id } => {
                if metadata.current_schema_id == *current_schema_id {
                    Ok(())
                } else {
                    Err(RequirementFailed::new(format!(
                        "current schema id must be {current_schema_id}, found {}",
                        metadata.current_schema_id
                    )))
                }
            }
            Self::AssertLastAssignedPartitionId {
                last_assigned_partition_id,
            } => {
                if metadata.last_partition_id == *last_assigned_partition_id {
                    Ok(())
                } else {
                    Err(RequirementFailed::new(format!(
                        "last assigned partition id must be {last_assigned_partition_id}, found {}",
                        metadata.last_partition_id
                    )))
                }
            }
            Self::AssertDefaultSpecId { default_spec_id } => {
                if metadata.default_spec_id == *default_spec_id {
                    Ok(())
                } else {
                    Err(RequirementFailed::new(format!(
                        "default spec id must be {default_spec_id}, found {}",
                        metadata.default_spec_id
                    )))
                }
            }
            Self::AssertDefaultSortOrderId {
                default_sort_order_id,
            } => {
                if metadata.default_sort_order_id == *default_sort_order_id {
                    Ok(())
                } else {
                    Err(RequirementFailed::new(format!(
                        "default sort order id must be {default_sort_order_id}, found {}",
                        metadata.default_sort_order_id
                    )))
                }
            }
        }
    }
}
