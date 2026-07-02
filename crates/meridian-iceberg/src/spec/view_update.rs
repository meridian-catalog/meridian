//! The REST commit `updates` list for views: every view-changing action.
//!
//! Action names and payload shapes follow the Iceberg REST catalog `OpenAPI`
//! specification (`ViewUpdate` and the per-action `*Update` schemas). The
//! view vocabulary is a strict subset of the table vocabulary plus the two
//! view-only actions `add-view-version` and `set-current-view-version`;
//! table-only actions (specs, sort orders, snapshots, statistics,
//! encryption keys) are deliberately absent.

use std::collections::BTreeMap;

use serde::{Deserialize, Serialize};
use uuid::Uuid;

use super::schema::Schema;
use super::view::ViewVersion;

/// One view update in a commit request, tagged by `action`.
///
/// The `-1` sentinel ([`super::update::LAST_ADDED`]) is accepted by
/// `set-current-view-version` (the last added version) and by the
/// `schema-id` of an added view version (the last added schema).
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(
    tag = "action",
    rename_all = "kebab-case",
    rename_all_fields = "kebab-case"
)]
pub enum ViewUpdate {
    /// Assign the view UUID. Only valid while creating a view.
    AssignUuid {
        /// The UUID to assign.
        uuid: Uuid,
    },
    /// Upgrade the format version. The view format has exactly one version,
    /// so only `1` is accepted (as a no-op).
    UpgradeFormatVersion {
        /// The target format version.
        format_version: u8,
    },
    /// Add a schema. The builder assigns the schema id.
    AddSchema {
        /// The schema to add (its `schema-id`, if present, is ignored).
        schema: Schema,
        /// **Deprecated in the REST spec** and meaningless for views (view
        /// metadata tracks no `last-column-id`). When present it is
        /// validated against the ids actually used by `schema`.
        #[serde(skip_serializing_if = "Option::is_none")]
        last_column_id: Option<i32>,
    },
    /// Set the view base location.
    SetLocation {
        /// The new location.
        location: String,
    },
    /// Set (upsert) view properties.
    SetProperties {
        /// Properties to set.
        updates: BTreeMap<String, String>,
    },
    /// Remove view properties (missing keys are ignored).
    RemoveProperties {
        /// Property keys to remove.
        removals: Vec<String>,
    },
    /// Add a view version. The builder assigns the version id.
    AddViewVersion {
        /// The version to add. Its `schema-id` may be `-1` for the schema
        /// last added in the same batch.
        view_version: ViewVersion,
    },
    /// Set the current view version.
    SetCurrentViewVersion {
        /// Version id to set as current, or `-1` for the last added
        /// version.
        view_version_id: i32,
    },
}
