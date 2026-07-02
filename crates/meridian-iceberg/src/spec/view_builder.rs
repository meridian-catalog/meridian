//! The view-metadata builder: applies a list of [`ViewUpdate`]s to a base
//! [`ViewMetadata`] (or to a brand-new view) with full structural
//! validation, mirroring [`super::builder::MetadataBuilder`] for tables.
//!
//! Every rejection is a [`ViewMetadataBuildError`] with a precise,
//! client-facing message; at the API boundary most map to `400`-class
//! validation failures. On `Err` the builder must be discarded: the working
//! copy may hold a partially applied update batch. Callers apply
//! all-or-nothing by building only on success.
//!
//! Semantics follow the reference implementation (`ViewMetadata.Builder`):
//! identical schemas/versions are re-added by reusing their ids, version ids
//! are assigned above every retained id, `set-current-view-version` accepts
//! the `-1` sentinel, the version log records every current-version change,
//! and old versions are expired against the `version.history.num-entries`
//! property at build time.
//!
//! Known gap, tracked honestly:
//!
//! - TODO(M2): the reference implementation's dialect-drop protection
//!   (`replace.drop-dialect.allowed`, default false, which refuses a replace
//!   whose new current version loses a SQL dialect the previous current
//!   version had) is not enforced yet. It needs the commit driver to thread
//!   the pre-commit current version through; enforce it when the view commit
//!   endpoint lands.

use std::collections::BTreeSet;

use uuid::Uuid;

use super::schema::Schema;
use super::update::LAST_ADDED;
use super::view::{
    VIEW_FORMAT_VERSION, ViewHistoryEntry, ViewMetadata, ViewRepresentation, ViewVersion,
};
use super::view_update::ViewUpdate;

/// View property controlling how many versions (and their log entries) are
/// retained in the metadata file.
pub const VERSION_HISTORY_NUM_ENTRIES_PROP: &str = "version.history.num-entries";

/// Default for [`VERSION_HISTORY_NUM_ENTRIES_PROP`].
pub const VERSION_HISTORY_NUM_ENTRIES_DEFAULT: usize = 10;

/// A rejected view update or an invalid final state.
///
/// Messages are precise and safe to return to clients; all variants map to
/// `400`-class validation errors at the API boundary (identity conflicts are
/// caught by the `assert-view-uuid` requirement, not the builder).
#[derive(Debug, Clone, PartialEq, Eq, thiserror::Error)]
pub enum ViewMetadataBuildError {
    /// The requested format version is not 1 (the only view format version).
    #[error("unsupported view format version {version} (supported: 1)")]
    UnsupportedFormatVersion {
        /// The requested version.
        version: u8,
    },

    /// `assign-uuid` tried to change an already-assigned view UUID.
    #[error("cannot reassign view UUID: view has {current}, update wants {requested}")]
    UuidMismatch {
        /// The UUID already on the view.
        current: Uuid,
        /// The UUID the update tried to assign.
        requested: Uuid,
    },

    /// A referenced schema id does not exist.
    #[error("schema {schema_id} does not exist")]
    SchemaNotFound {
        /// The missing schema id.
        schema_id: i32,
    },

    /// `-1` was used as a schema id but no schema was added in this batch.
    #[error("cannot resolve last added schema: no schema was added")]
    NoLastAddedSchema,

    /// A schema is structurally invalid.
    #[error("invalid schema: {reason}")]
    InvalidSchema {
        /// What is wrong with it.
        reason: String,
    },

    /// The deprecated `last-column-id` on `add-schema` is lower than a field
    /// id actually used by the schema.
    #[error(
        "add-schema last-column-id {provided} is lower than the schema's highest field id {required}"
    )]
    LastColumnIdTooLow {
        /// The value the client sent.
        provided: i32,
        /// The highest field id in the schema.
        required: i32,
    },

    /// A field type this model does not recognize was used in an added
    /// schema.
    #[error("unknown field type {type_string:?}")]
    UnknownFieldType {
        /// The unrecognized type string.
        type_string: String,
    },

    /// A referenced view version id does not exist.
    #[error("view version {version_id} does not exist")]
    VersionNotFound {
        /// The missing version id.
        version_id: i32,
    },

    /// `-1` was used as a version id but no version was added in this
    /// batch.
    #[error("cannot set current view version to last added: no view version was added")]
    NoLastAddedVersion,

    /// An added view version carries multiple SQL representations for one
    /// dialect (compared case-insensitively).
    #[error("invalid view version: multiple SQL representations for dialect {dialect:?}")]
    DuplicateDialect {
        /// The colliding dialect, lowercased.
        dialect: String,
    },

    /// The view location must be a non-empty string.
    #[error("view location must not be empty")]
    EmptyLocation,

    /// Property keys must be non-empty strings.
    #[error("property keys must not be empty")]
    EmptyPropertyKey,

    /// The built metadata would have no current version (a new view must
    /// receive `add-view-version` + `set-current-view-version` before
    /// building).
    #[error(
        "view has no current version: apply add-view-version and set-current-view-version first"
    )]
    CurrentVersionUnset,

    /// An internal invariant did not hold; indicates a bug or hand-crafted
    /// inconsistent base metadata.
    #[error("view metadata invariant violated: {message}")]
    InvariantViolation {
        /// What did not hold.
        message: String,
    },
}

/// The version-log entry recorded by `set-current-view-version`, materialized
/// at build time. `timestamp_ms` is `Some` when the version was added in the
/// same batch (its own creation timestamp is used); `None` means the entry
/// re-activates a pre-existing version and gets the build's `now_ms`.
#[derive(Debug, Clone)]
struct PendingLogEntry {
    version_id: i32,
    timestamp_ms: Option<i64>,
}

/// Applies [`ViewUpdate`]s to view metadata with full validation.
///
/// Obtain one from [`ViewMetadata::builder_from`] (evolving an existing
/// view) or [`ViewMetadataBuilder::new_view`] (creating one), apply updates,
/// then [`ViewMetadataBuilder::build`].
#[derive(Debug, Clone)]
pub struct ViewMetadataBuilder {
    metadata: ViewMetadata,
    last_added_schema_id: Option<i32>,
    last_added_version_id: Option<i32>,
    /// Version ids first added by this builder (as opposed to inherited from
    /// the base): they are exempt from expiry and their own timestamps feed
    /// the version log.
    versions_added: BTreeSet<i32>,
    /// The last `set-current-view-version` of the batch; only the final
    /// current version gets a log entry (mirroring the reference
    /// implementation).
    pending_log_entry: Option<PendingLogEntry>,
    /// Whether the UUID is fixed (true for existing views and after an
    /// `assign-uuid` update).
    uuid_assigned: bool,
}

impl ViewMetadata {
    /// Starts a builder from this metadata (the loaded base of a commit).
    #[must_use]
    pub fn builder_from(&self) -> ViewMetadataBuilder {
        ViewMetadataBuilder {
            metadata: self.clone(),
            last_added_schema_id: None,
            last_added_version_id: None,
            versions_added: BTreeSet::new(),
            pending_log_entry: None,
            uuid_assigned: true,
        }
    }
}

impl ViewMetadataBuilder {
    /// Starts a builder for a brand-new view.
    ///
    /// The view gets a fresh UUID (replaceable by one `assign-uuid` update).
    /// At least one `add-schema`, an `add-view-version`, and a
    /// `set-current-view-version` must be applied before
    /// [`ViewMetadataBuilder::build`] succeeds.
    pub fn new_view(location: impl Into<String>) -> Result<Self, ViewMetadataBuildError> {
        let location = location.into();
        if location.is_empty() {
            return Err(ViewMetadataBuildError::EmptyLocation);
        }
        Ok(Self {
            metadata: ViewMetadata {
                view_uuid: Uuid::new_v4(),
                format_version: VIEW_FORMAT_VERSION,
                location,
                schemas: Vec::new(),
                current_version_id: -1,
                versions: Vec::new(),
                version_log: Vec::new(),
                properties: None,
                extra: serde_json::Map::new(),
            },
            last_added_schema_id: None,
            last_added_version_id: None,
            versions_added: BTreeSet::new(),
            pending_log_entry: None,
            uuid_assigned: false,
        })
    }

    /// The working metadata (validated only as far as the updates applied so
    /// far).
    #[must_use]
    pub fn current(&self) -> &ViewMetadata {
        &self.metadata
    }

    /// Applies one update. On `Err` the builder must be discarded.
    pub fn apply(&mut self, update: ViewUpdate) -> Result<(), ViewMetadataBuildError> {
        match update {
            ViewUpdate::AssignUuid { uuid } => self.assign_uuid(uuid),
            ViewUpdate::UpgradeFormatVersion { format_version } => {
                Self::upgrade_format_version(format_version)
            }
            ViewUpdate::AddSchema {
                schema,
                last_column_id,
            } => self.add_schema(schema, last_column_id),
            ViewUpdate::SetLocation { location } => self.set_location(location),
            ViewUpdate::SetProperties { updates } => self.set_properties(updates),
            ViewUpdate::RemoveProperties { removals } => self.remove_properties(&removals),
            ViewUpdate::AddViewVersion { view_version } => self.add_view_version(view_version),
            ViewUpdate::SetCurrentViewVersion { view_version_id } => {
                self.set_current_view_version(view_version_id)
            }
        }
    }

    /// Applies a batch of updates in order, stopping at the first rejection.
    pub fn apply_all(
        &mut self,
        updates: impl IntoIterator<Item = ViewUpdate>,
    ) -> Result<(), ViewMetadataBuildError> {
        for update in updates {
            self.apply(update)?;
        }
        Ok(())
    }

    /// Finalizes the metadata.
    ///
    /// Validates completeness (the current version and every version's
    /// schema resolve, ids are unique), appends the version-log entry for a
    /// current-version change (using the version's own creation timestamp
    /// when it was added in this batch, `now_ms` when an older version was
    /// re-activated, and clamped so log timestamps never go backwards under
    /// clock skew), and expires old versions down to the retention
    /// configured by the `version.history.num-entries` view property
    /// (default 10; unparsable or non-positive values fall back to the
    /// default; versions added in this batch and the current version are
    /// always kept). Log entries referencing expired versions truncate the
    /// history before them, mirroring the reference implementation.
    pub fn build(mut self, now_ms: i64) -> Result<ViewMetadata, ViewMetadataBuildError> {
        self.validate_final_state()?;

        if let Some(pending) = self.pending_log_entry.take() {
            let timestamp_ms = pending.timestamp_ms.unwrap_or(now_ms);
            let last_logged_ms = self
                .metadata
                .version_log
                .last()
                .map_or(i64::MIN, |entry| entry.timestamp_ms);
            self.metadata.version_log.push(ViewHistoryEntry {
                version_id: pending.version_id,
                timestamp_ms: timestamp_ms.max(last_logged_ms),
            });
        }

        self.expire_versions();
        Ok(self.metadata)
    }

    // -- final-state validation ---------------------------------------------

    fn version_history_num_entries(&self) -> usize {
        self.metadata
            .property(VERSION_HISTORY_NUM_ENTRIES_PROP)
            .and_then(|v| v.parse::<usize>().ok())
            .filter(|n| *n >= 1)
            .unwrap_or(VERSION_HISTORY_NUM_ENTRIES_DEFAULT)
    }

    fn validate_final_state(&self) -> Result<(), ViewMetadataBuildError> {
        if self.metadata.current_version_id < 0 || self.metadata.versions.is_empty() {
            return Err(ViewMetadataBuildError::CurrentVersionUnset);
        }

        let mut version_ids = BTreeSet::new();
        for version in &self.metadata.versions {
            if !version_ids.insert(version.version_id) {
                return Err(ViewMetadataBuildError::InvariantViolation {
                    message: format!("duplicate view version id {}", version.version_id),
                });
            }
            if self.metadata.schema_by_id(version.schema_id).is_none() {
                return Err(ViewMetadataBuildError::SchemaNotFound {
                    schema_id: version.schema_id,
                });
            }
        }
        if self.metadata.current_version().is_none() {
            return Err(ViewMetadataBuildError::VersionNotFound {
                version_id: self.metadata.current_version_id,
            });
        }

        let mut schema_ids = BTreeSet::new();
        for schema in &self.metadata.schemas {
            let Some(id) = schema.schema_id else {
                return Err(ViewMetadataBuildError::InvariantViolation {
                    message: "a stored schema has no schema-id".to_owned(),
                });
            };
            if !schema_ids.insert(id) {
                return Err(ViewMetadataBuildError::InvariantViolation {
                    message: format!("duplicate schema id {id}"),
                });
            }
        }
        Ok(())
    }

    /// Expires versions beyond the retention window, keeping the current
    /// version and everything added in this batch, preferring the highest
    /// version ids (ids are assigned sequentially, so highest means newest).
    fn expire_versions(&mut self) {
        let mut required: BTreeSet<i32> = self.versions_added.clone();
        required.insert(self.metadata.current_version_id);
        let keep = self.version_history_num_entries().max(required.len());
        if self.metadata.versions.len() <= keep {
            return;
        }

        let mut ids_newest_first: Vec<i32> = self
            .metadata
            .versions
            .iter()
            .map(|v| v.version_id)
            .collect();
        ids_newest_first.sort_unstable_by(|a, b| b.cmp(a));
        let mut keep_ids = required;
        for id in ids_newest_first {
            if keep_ids.len() >= keep {
                break;
            }
            keep_ids.insert(id);
        }

        self.metadata
            .versions
            .retain(|v| keep_ids.contains(&v.version_id));

        // The version log stays reconstructible: an entry for an expired
        // version invalidates everything before it, so the retained history
        // restarts after the last unknown version.
        let mut retained_log = Vec::with_capacity(self.metadata.version_log.len());
        for entry in self.metadata.version_log.drain(..) {
            if keep_ids.contains(&entry.version_id) {
                retained_log.push(entry);
            } else {
                retained_log.clear();
            }
        }
        self.metadata.version_log = retained_log;
    }

    // -- identity and format version ----------------------------------------

    fn assign_uuid(&mut self, uuid: Uuid) -> Result<(), ViewMetadataBuildError> {
        if self.uuid_assigned && self.metadata.view_uuid != uuid {
            return Err(ViewMetadataBuildError::UuidMismatch {
                current: self.metadata.view_uuid,
                requested: uuid,
            });
        }
        self.metadata.view_uuid = uuid;
        self.uuid_assigned = true;
        Ok(())
    }

    fn upgrade_format_version(version: u8) -> Result<(), ViewMetadataBuildError> {
        // The view format has exactly one version, so the only accepted
        // upgrade is the no-op to 1.
        if version != VIEW_FORMAT_VERSION {
            return Err(ViewMetadataBuildError::UnsupportedFormatVersion { version });
        }
        Ok(())
    }

    // -- schemas --------------------------------------------------------------

    /// Validates a schema for addition. View schemas describe engine query
    /// output, so unlike table schemas there is no format-version gating:
    /// any recognized type is accepted, and `initial-default`/`write-default`
    /// (meaningless on views) are preserved without validation.
    fn validate_schema(schema: &Schema) -> Result<(), ViewMetadataBuildError> {
        let ids_in_order = schema.field_ids_in_order();
        let mut seen = BTreeSet::new();
        for id in &ids_in_order {
            if *id < 1 {
                return Err(ViewMetadataBuildError::InvalidSchema {
                    reason: format!("field id {id} is not positive"),
                });
            }
            if !seen.insert(*id) {
                return Err(ViewMetadataBuildError::InvalidSchema {
                    reason: format!("field id {id} is used more than once"),
                });
            }
        }
        for field in &schema.fields {
            Self::validate_struct_field(field)?;
        }
        if let Some(identifier_ids) = &schema.identifier_field_ids {
            for id in identifier_ids {
                if !seen.contains(id) {
                    return Err(ViewMetadataBuildError::InvalidSchema {
                        reason: format!("identifier field id {id} is not a field of the schema"),
                    });
                }
            }
        }
        Ok(())
    }

    fn validate_struct_field(
        field: &super::types::StructField,
    ) -> Result<(), ViewMetadataBuildError> {
        if field.name.is_empty() {
            return Err(ViewMetadataBuildError::InvalidSchema {
                reason: format!("field {} has an empty name", field.id),
            });
        }
        if let Some(type_string) = field.field_type.find_unrecognized_primitive() {
            return Err(ViewMetadataBuildError::UnknownFieldType { type_string });
        }
        if let super::types::Type::Struct(nested) = &field.field_type {
            for nested_field in &nested.fields {
                Self::validate_struct_field(nested_field)?;
            }
        }
        Ok(())
    }

    fn add_schema(
        &mut self,
        mut schema: Schema,
        last_column_id: Option<i32>,
    ) -> Result<(), ViewMetadataBuildError> {
        Self::validate_schema(&schema)?;
        let max_field_id = schema.max_field_id();
        if let Some(provided) = last_column_id
            && provided < max_field_id
        {
            return Err(ViewMetadataBuildError::LastColumnIdTooLow {
                provided,
                required: max_field_id,
            });
        }

        // Re-adding an identical schema reuses its id.
        if let Some(existing) = self
            .metadata
            .schemas
            .iter()
            .find(|s| s.same_structure(&schema))
            && let Some(existing_id) = existing.schema_id
        {
            self.last_added_schema_id = Some(existing_id);
            return Ok(());
        }

        let new_id = self
            .metadata
            .schemas
            .iter()
            .filter_map(|s| s.schema_id)
            .max()
            .map_or(0, |max| max + 1);
        schema.schema_id = Some(new_id);
        self.metadata.schemas.push(schema);
        self.last_added_schema_id = Some(new_id);
        Ok(())
    }

    // -- location and properties ----------------------------------------------

    fn set_location(&mut self, location: String) -> Result<(), ViewMetadataBuildError> {
        if location.is_empty() {
            return Err(ViewMetadataBuildError::EmptyLocation);
        }
        self.metadata.location = location;
        Ok(())
    }

    fn set_properties(
        &mut self,
        updates: std::collections::BTreeMap<String, String>,
    ) -> Result<(), ViewMetadataBuildError> {
        for key in updates.keys() {
            Self::validate_property_key(key)?;
        }
        self.metadata
            .properties
            .get_or_insert_with(std::collections::BTreeMap::new)
            .extend(updates);
        Ok(())
    }

    fn remove_properties(&mut self, removals: &[String]) -> Result<(), ViewMetadataBuildError> {
        for key in removals {
            Self::validate_property_key(key)?;
        }
        if let Some(properties) = &mut self.metadata.properties {
            for key in removals {
                properties.remove(key);
            }
        }
        Ok(())
    }

    fn validate_property_key(key: &str) -> Result<(), ViewMetadataBuildError> {
        if key.is_empty() {
            return Err(ViewMetadataBuildError::EmptyPropertyKey);
        }
        Ok(())
    }

    // -- versions ---------------------------------------------------------------

    fn add_view_version(&mut self, mut version: ViewVersion) -> Result<(), ViewMetadataBuildError> {
        // Re-adding an identical definition reuses its id (before the -1
        // schema sentinel is resolved: stored versions carry real ids, so a
        // sentinel-bearing version never matches one). Reusing a version
        // that was *not* added in this batch clears the last-added tracker,
        // mirroring the reference implementation: `-1` may only name a
        // version this batch actually introduced.
        if let Some(existing) = self
            .metadata
            .versions
            .iter()
            .find(|v| v.same_definition(&version))
        {
            let existing_id = existing.version_id;
            self.last_added_version_id = self
                .versions_added
                .contains(&existing_id)
                .then_some(existing_id);
            return Ok(());
        }

        if version.schema_id == LAST_ADDED {
            version.schema_id = self
                .last_added_schema_id
                .ok_or(ViewMetadataBuildError::NoLastAddedSchema)?;
        }
        if self.metadata.schema_by_id(version.schema_id).is_none() {
            return Err(ViewMetadataBuildError::SchemaNotFound {
                schema_id: version.schema_id,
            });
        }

        // At most one SQL representation per dialect, case-insensitively.
        let mut dialects = BTreeSet::new();
        for representation in &version.representations {
            if let ViewRepresentation::Sql(sql) = representation {
                let dialect = sql.dialect.to_lowercase();
                if !dialects.insert(dialect.clone()) {
                    return Err(ViewMetadataBuildError::DuplicateDialect { dialect });
                }
            }
        }

        // Assign the id: the provided id is kept when it is above every
        // retained id, otherwise the next sequential id is used (reference
        // behavior — ids grow monotonically and are never reused).
        let mut new_id = version.version_id;
        for existing in &self.metadata.versions {
            if existing.version_id >= new_id {
                new_id = existing.version_id + 1;
            }
        }
        version.version_id = new_id;
        self.metadata.versions.push(version);
        self.versions_added.insert(new_id);
        self.last_added_version_id = Some(new_id);
        Ok(())
    }

    fn set_current_view_version(
        &mut self,
        view_version_id: i32,
    ) -> Result<(), ViewMetadataBuildError> {
        let version_id = if view_version_id == LAST_ADDED {
            self.last_added_version_id
                .ok_or(ViewMetadataBuildError::NoLastAddedVersion)?
        } else {
            view_version_id
        };
        if self.metadata.current_version_id == version_id {
            return Ok(());
        }
        let Some(version) = self.metadata.version_by_id(version_id) else {
            return Err(ViewMetadataBuildError::VersionNotFound { version_id });
        };

        // The log entry is deterministic when the version was created in
        // this batch (its own timestamp); re-activating an older version
        // gets the commit time at build().
        let timestamp_ms = self
            .versions_added
            .contains(&version_id)
            .then_some(version.timestamp_ms);
        self.metadata.current_version_id = version_id;
        self.pending_log_entry = Some(PendingLogEntry {
            version_id,
            timestamp_ms,
        });
        Ok(())
    }
}
