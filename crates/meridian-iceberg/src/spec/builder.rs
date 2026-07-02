//! The metadata builder: applies a list of [`TableUpdate`]s to a base
//! [`TableMetadata`] (or to a brand-new table) with full structural
//! validation.
//!
//! This is step 7 of the commit sequence in
//! `docs/design/commit-protocol.md` §3: the arbiter applies the update list
//! to the loaded metadata and validates every invariant before the result is
//! staged. Every rejection is a [`MetadataBuildError`] with a precise,
//! client-facing message; at the API boundary most map to `400`-class
//! validation failures, and conflicts (e.g. snapshot id collisions) to
//! `409`.
//!
//! On `Err` the builder must be discarded: the working copy may hold a
//! partially applied update batch. Callers apply all-or-nothing by building
//! only on success.

use std::collections::BTreeSet;

use uuid::Uuid;

use super::partition::PartitionSpec;
use super::schema::Schema;
use super::snapshot::{RefType, Snapshot, SnapshotLogEntry, SnapshotRef};
use super::sort::SortOrder;
use super::statistics::{PartitionStatisticsFile, StatisticsFile};
use super::table_metadata::{PARTITION_DATA_ID_START, TableMetadata};
use super::update::{LAST_ADDED, TableUpdate};

/// Table property controlling how many previous metadata files are kept in
/// the metadata log.
pub const PREVIOUS_VERSIONS_MAX_PROP: &str = "write.metadata.previous-versions-max";

/// Default for [`PREVIOUS_VERSIONS_MAX_PROP`].
pub const PREVIOUS_VERSIONS_MAX_DEFAULT: usize = 100;

/// Property keys reserved for catalog use; commits may not set or remove
/// them.
pub const RESERVED_PROPERTIES: &[&str] = &[
    "format-version",
    "uuid",
    "snapshot-count",
    "current-snapshot-summary",
    "current-snapshot-id",
    "current-snapshot-timestamp-ms",
    "current-schema",
    "default-partition-spec",
    "default-sort-order",
];

/// A rejected update or an invalid final state.
///
/// Messages are precise and safe to return to clients. Variants that assert
/// against *concurrent* state changes (snapshot collisions, sequence-number
/// regressions) map to `409 CommitFailedException` at the API boundary;
/// the rest map to `400`-class validation errors.
#[derive(Debug, Clone, PartialEq, Eq, thiserror::Error)]
pub enum MetadataBuildError {
    /// The requested format version is not 1, 2, or 3.
    #[error("unsupported format version {version} (supported: 1, 2, 3)")]
    UnsupportedFormatVersion {
        /// The requested version.
        version: u8,
    },

    /// Format versions can only move forward.
    #[error("cannot downgrade format version from {current} to {requested}")]
    FormatVersionDowngrade {
        /// The table's current format version.
        current: u8,
        /// The requested (lower) version.
        requested: u8,
    },

    /// A v3 feature was used on a table with a lower format version.
    #[error("{feature} requires format version 3, but the table is at version {format_version}")]
    RequiresV3 {
        /// Human-readable description of the v3 feature.
        feature: String,
        /// The table's current format version.
        format_version: u8,
    },

    /// `assign-uuid` tried to change an already-assigned table UUID.
    #[error("cannot reassign table UUID: table has {current}, update wants {requested}")]
    UuidMismatch {
        /// The UUID already on the table.
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
    #[error("cannot set current schema to last added: no schema was added")]
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

    /// A partition/sort transform this model does not recognize.
    #[error("unknown transform {transform:?}")]
    UnknownTransform {
        /// The unrecognized transform string.
        transform: String,
    },

    /// A field type this model does not recognize was used in an added
    /// schema.
    #[error("unknown field type {type_string:?}")]
    UnknownFieldType {
        /// The unrecognized type string.
        type_string: String,
    },

    /// A partition/sort field references a column that is not in the current
    /// schema.
    #[error("source field {source_id} does not exist in the current schema")]
    UnknownSourceField {
        /// The missing source column id.
        source_id: i32,
    },

    /// A partition spec is structurally invalid.
    #[error("invalid partition spec: {reason}")]
    InvalidPartitionSpec {
        /// What is wrong with it.
        reason: String,
    },

    /// A referenced partition spec id does not exist.
    #[error("partition spec {spec_id} does not exist")]
    SpecNotFound {
        /// The missing spec id.
        spec_id: i32,
    },

    /// `-1` was used as a spec id but no spec was added in this batch.
    #[error("cannot set default spec to last added: no partition spec was added")]
    NoLastAddedSpec,

    /// The default partition spec cannot be removed.
    #[error("cannot remove partition spec {spec_id}: it is the default spec")]
    DefaultSpecRemoval {
        /// The default spec id.
        spec_id: i32,
    },

    /// A referenced sort order id does not exist.
    #[error("sort order {order_id} does not exist")]
    SortOrderNotFound {
        /// The missing order id.
        order_id: i32,
    },

    /// `-1` was used as a sort order id but no order was added in this
    /// batch.
    #[error("cannot set default sort order to last added: no sort order was added")]
    NoLastAddedSortOrder,

    /// The current schema cannot be removed.
    #[error("cannot remove schema {schema_id}: it is the current schema")]
    CurrentSchemaRemoval {
        /// The current schema id.
        schema_id: i32,
    },

    /// A schema still referenced by a retained snapshot cannot be removed.
    #[error("cannot remove schema {schema_id}: snapshot {snapshot_id} still uses it")]
    SchemaInUse {
        /// The schema id.
        schema_id: i32,
        /// A snapshot that references it.
        snapshot_id: i64,
    },

    /// A snapshot with this id already exists.
    #[error("snapshot {snapshot_id} already exists")]
    SnapshotAlreadyExists {
        /// The colliding snapshot id.
        snapshot_id: i64,
    },

    /// A referenced snapshot does not exist.
    #[error("snapshot {snapshot_id} does not exist")]
    SnapshotNotFound {
        /// The missing snapshot id.
        snapshot_id: i64,
    },

    /// An added snapshot's parent is not a retained snapshot.
    #[error("snapshot {snapshot_id} declares parent {parent_id}, which does not exist")]
    ParentSnapshotNotFound {
        /// The added snapshot.
        snapshot_id: i64,
        /// Its missing parent.
        parent_id: i64,
    },

    /// An added snapshot is structurally invalid.
    #[error("invalid snapshot {snapshot_id}: {reason}")]
    InvalidSnapshot {
        /// The snapshot id.
        snapshot_id: i64,
        /// What is wrong with it.
        reason: String,
    },

    /// An added snapshot's sequence number does not advance the table's.
    #[error(
        "snapshot {snapshot_id} has sequence number {provided}, which must be greater than the table's last sequence number {last}"
    )]
    NonMonotonicSequenceNumber {
        /// The added snapshot.
        snapshot_id: i64,
        /// Its sequence number.
        provided: i64,
        /// The table's last sequence number.
        last: i64,
    },

    /// A referenced branch/tag does not exist.
    #[error("ref {name:?} does not exist")]
    RefNotFound {
        /// The missing ref name.
        name: String,
    },

    /// Branch retention settings were supplied for a tag.
    #[error(
        "ref {name:?} is a tag and cannot carry branch retention settings (min-snapshots-to-keep/max-snapshot-age-ms)"
    )]
    BranchRetentionOnTag {
        /// The tag name.
        name: String,
    },

    /// A snapshot still referenced by a branch/tag cannot be removed.
    #[error("cannot remove snapshot {snapshot_id}: ref {ref_name:?} still references it")]
    SnapshotReferenced {
        /// The snapshot id.
        snapshot_id: i64,
        /// The ref that references it.
        ref_name: String,
    },

    /// The current snapshot cannot be removed.
    #[error("cannot remove snapshot {snapshot_id}: it is the current snapshot")]
    CurrentSnapshotRemoval {
        /// The current snapshot id.
        snapshot_id: i64,
    },

    /// The table location must be a non-empty string.
    #[error("table location must not be empty")]
    EmptyLocation,

    /// Property keys must be non-empty strings.
    #[error("property keys must not be empty")]
    EmptyPropertyKey,

    /// The property key is reserved for catalog use.
    #[error("property {key:?} is reserved and cannot be set or removed by commits")]
    ReservedProperty {
        /// The reserved key.
        key: String,
    },

    /// The deprecated `snapshot-id` on `set-statistics` disagrees with the
    /// statistics file.
    #[error(
        "set-statistics snapshot-id {update} does not match the statistics file's snapshot-id {file}"
    )]
    StatisticsSnapshotMismatch {
        /// The id on the update.
        update: i64,
        /// The id inside the statistics file.
        file: i64,
    },

    /// No statistics are recorded for the snapshot.
    #[error("no statistics recorded for snapshot {snapshot_id}")]
    StatisticsNotFound {
        /// The snapshot id.
        snapshot_id: i64,
    },

    /// No partition statistics are recorded for the snapshot.
    #[error("no partition statistics recorded for snapshot {snapshot_id}")]
    PartitionStatisticsNotFound {
        /// The snapshot id.
        snapshot_id: i64,
    },

    /// An encryption key with this id already exists.
    #[error("encryption key {key_id:?} already exists")]
    DuplicateEncryptionKey {
        /// The colliding key id.
        key_id: String,
    },

    /// The referenced encryption key does not exist.
    #[error("encryption key {key_id:?} does not exist")]
    EncryptionKeyNotFound {
        /// The missing key id.
        key_id: String,
    },

    /// The built metadata would have no current schema (a new table must
    /// receive `add-schema` + `set-current-schema` before building).
    #[error("table has no current schema: apply add-schema and set-current-schema first")]
    CurrentSchemaUnset,

    /// An internal invariant did not hold; indicates a bug or hand-crafted
    /// inconsistent base metadata.
    #[error("metadata invariant violated: {message}")]
    InvariantViolation {
        /// What did not hold.
        message: String,
    },
}

/// Applies [`TableUpdate`]s to table metadata with full validation.
///
/// Obtain one from [`TableMetadata::builder_from`] (evolving an existing
/// table) or [`MetadataBuilder::new_table`] (creating one), apply updates,
/// then [`MetadataBuilder::build`].
#[derive(Debug, Clone)]
pub struct MetadataBuilder {
    metadata: TableMetadata,
    last_added_schema_id: Option<i32>,
    last_added_spec_id: Option<i32>,
    last_added_order_id: Option<i32>,
    /// Whether the UUID is fixed (true for existing tables and after an
    /// `assign-uuid` update).
    uuid_assigned: bool,
}

impl TableMetadata {
    /// Starts a builder from this metadata (the loaded base of a commit).
    #[must_use]
    pub fn builder_from(&self) -> MetadataBuilder {
        MetadataBuilder {
            metadata: self.clone(),
            last_added_schema_id: None,
            last_added_spec_id: None,
            last_added_order_id: None,
            uuid_assigned: true,
        }
    }
}

impl MetadataBuilder {
    /// Starts a builder for a brand-new table.
    ///
    /// The table gets a fresh UUID (replaceable by one `assign-uuid`
    /// update), the unpartitioned spec 0, and the unsorted order 0. At least
    /// one `add-schema` and a `set-current-schema` must be applied before
    /// [`MetadataBuilder::build`] succeeds.
    pub fn new_table(
        format_version: u8,
        location: impl Into<String>,
    ) -> Result<Self, MetadataBuildError> {
        if !(1..=3).contains(&format_version) {
            return Err(MetadataBuildError::UnsupportedFormatVersion {
                version: format_version,
            });
        }
        let location = location.into();
        if location.is_empty() {
            return Err(MetadataBuildError::EmptyLocation);
        }
        Ok(Self {
            metadata: TableMetadata {
                format_version,
                table_uuid: Uuid::new_v4(),
                location,
                last_sequence_number: (format_version >= 2).then_some(0),
                next_row_id: (format_version >= 3).then_some(0),
                last_updated_ms: 0,
                last_column_id: 0,
                schemas: Vec::new(),
                current_schema_id: -1,
                partition_specs: vec![PartitionSpec::unpartitioned(0)],
                default_spec_id: 0,
                last_partition_id: PARTITION_DATA_ID_START - 1,
                sort_orders: vec![SortOrder::unsorted()],
                default_sort_order_id: 0,
                properties: None,
                current_snapshot_id: None,
                snapshots: None,
                snapshot_log: None,
                metadata_log: None,
                refs: None,
                statistics: None,
                partition_statistics: None,
                encryption_keys: None,
                extra: serde_json::Map::new(),
            },
            last_added_schema_id: None,
            last_added_spec_id: None,
            last_added_order_id: None,
            uuid_assigned: false,
        })
    }

    /// The working metadata (validated only as far as the updates applied so
    /// far).
    #[must_use]
    pub fn current(&self) -> &TableMetadata {
        &self.metadata
    }

    /// Applies one update. On `Err` the builder must be discarded.
    pub fn apply(&mut self, update: TableUpdate) -> Result<(), MetadataBuildError> {
        match update {
            TableUpdate::AssignUuid { uuid } => self.assign_uuid(uuid),
            TableUpdate::UpgradeFormatVersion { format_version } => {
                self.upgrade_format_version(format_version)
            }
            TableUpdate::AddSchema {
                schema,
                last_column_id,
            } => self.add_schema(schema, last_column_id),
            TableUpdate::SetCurrentSchema { schema_id } => self.set_current_schema(schema_id),
            TableUpdate::AddSpec { spec } => self.add_spec(spec),
            TableUpdate::SetDefaultSpec { spec_id } => self.set_default_spec(spec_id),
            TableUpdate::AddSortOrder { sort_order } => self.add_sort_order(sort_order),
            TableUpdate::SetDefaultSortOrder { sort_order_id } => {
                self.set_default_sort_order(sort_order_id)
            }
            TableUpdate::AddSnapshot { snapshot } => self.add_snapshot(snapshot),
            TableUpdate::SetSnapshotRef {
                ref_name,
                reference,
            } => self.set_snapshot_ref(ref_name, reference),
            TableUpdate::RemoveSnapshots { snapshot_ids } => self.remove_snapshots(&snapshot_ids),
            TableUpdate::RemoveSnapshotRef { ref_name } => self.remove_snapshot_ref(&ref_name),
            TableUpdate::SetLocation { location } => self.set_location(location),
            TableUpdate::SetProperties { updates } => self.set_properties(updates),
            TableUpdate::RemoveProperties { removals } => self.remove_properties(&removals),
            TableUpdate::SetStatistics {
                snapshot_id,
                statistics,
            } => self.set_statistics(snapshot_id, statistics),
            TableUpdate::RemoveStatistics { snapshot_id } => self.remove_statistics(snapshot_id),
            TableUpdate::SetPartitionStatistics {
                partition_statistics,
            } => self.set_partition_statistics(partition_statistics),
            TableUpdate::RemovePartitionStatistics { snapshot_id } => {
                self.remove_partition_statistics(snapshot_id)
            }
            TableUpdate::RemovePartitionSpecs { spec_ids } => {
                self.remove_partition_specs(&spec_ids)
            }
            TableUpdate::RemoveSchemas { schema_ids } => self.remove_schemas(&schema_ids),
            TableUpdate::AddEncryptionKey { encryption_key } => {
                self.add_encryption_key(encryption_key)
            }
            TableUpdate::RemoveEncryptionKey { key_id } => self.remove_encryption_key(&key_id),
        }
    }

    /// Applies a batch of updates in order, stopping at the first rejection.
    pub fn apply_all(
        &mut self,
        updates: impl IntoIterator<Item = TableUpdate>,
    ) -> Result<(), MetadataBuildError> {
        for update in updates {
            self.apply(update)?;
        }
        Ok(())
    }

    /// Finalizes the metadata.
    ///
    /// Validates completeness (current schema/spec/order resolve, ids are
    /// assigned and unique), maintains `last-updated-ms` monotonically from
    /// `now_ms`, and — when `previous_metadata_location` is given — appends
    /// it to the metadata log, truncated to the retention configured by the
    /// `write.metadata.previous-versions-max` table property (default 100;
    /// unparsable values fall back to the default).
    pub fn build(
        mut self,
        now_ms: i64,
        previous_metadata_location: Option<&str>,
    ) -> Result<TableMetadata, MetadataBuildError> {
        self.validate_final_state()?;

        // Version-conditional bookkeeping fields.
        if self.metadata.format_version >= 2 && self.metadata.last_sequence_number.is_none() {
            self.metadata.last_sequence_number = Some(0);
        }
        if self.metadata.format_version >= 3 && self.metadata.next_row_id.is_none() {
            self.metadata.next_row_id = Some(0);
        }

        // last-updated-ms is monotonic even under clock skew.
        let previous_updated_ms = self.metadata.last_updated_ms;
        self.metadata.last_updated_ms = now_ms.max(previous_updated_ms);

        if let Some(previous_file) = previous_metadata_location {
            let retention = self.previous_versions_max();
            let log = self.metadata.metadata_log.get_or_insert_with(Vec::new);
            log.push(super::snapshot::MetadataLogEntry {
                metadata_file: previous_file.to_owned(),
                timestamp_ms: previous_updated_ms,
            });
            if log.len() > retention {
                let excess = log.len() - retention;
                log.drain(..excess);
            }
        }
        Ok(self.metadata)
    }

    // -- final-state validation ---------------------------------------------

    fn previous_versions_max(&self) -> usize {
        self.metadata
            .property(PREVIOUS_VERSIONS_MAX_PROP)
            .and_then(|v| v.parse::<usize>().ok())
            .filter(|n| *n >= 1)
            .unwrap_or(PREVIOUS_VERSIONS_MAX_DEFAULT)
    }

    fn validate_final_state(&self) -> Result<(), MetadataBuildError> {
        if self.metadata.current_schema_id < 0 || self.metadata.schemas.is_empty() {
            return Err(MetadataBuildError::CurrentSchemaUnset);
        }
        let mut schema_ids = BTreeSet::new();
        for schema in &self.metadata.schemas {
            let Some(id) = schema.schema_id else {
                return Err(MetadataBuildError::InvariantViolation {
                    message: "a stored schema has no schema-id".to_owned(),
                });
            };
            if !schema_ids.insert(id) {
                return Err(MetadataBuildError::InvariantViolation {
                    message: format!("duplicate schema id {id}"),
                });
            }
            if schema.max_field_id() > self.metadata.last_column_id {
                return Err(MetadataBuildError::InvariantViolation {
                    message: format!(
                        "schema {id} uses field ids above last-column-id {}",
                        self.metadata.last_column_id
                    ),
                });
            }
        }
        if self.metadata.current_schema().is_none() {
            return Err(MetadataBuildError::SchemaNotFound {
                schema_id: self.metadata.current_schema_id,
            });
        }

        let mut spec_ids = BTreeSet::new();
        for spec in &self.metadata.partition_specs {
            let Some(id) = spec.spec_id else {
                return Err(MetadataBuildError::InvariantViolation {
                    message: "a stored partition spec has no spec-id".to_owned(),
                });
            };
            if !spec_ids.insert(id) {
                return Err(MetadataBuildError::InvariantViolation {
                    message: format!("duplicate partition spec id {id}"),
                });
            }
        }
        if self.metadata.default_partition_spec().is_none() {
            return Err(MetadataBuildError::SpecNotFound {
                spec_id: self.metadata.default_spec_id,
            });
        }

        let mut order_ids = BTreeSet::new();
        for order in &self.metadata.sort_orders {
            if !order_ids.insert(order.order_id) {
                return Err(MetadataBuildError::InvariantViolation {
                    message: format!("duplicate sort order id {}", order.order_id),
                });
            }
        }
        if self.metadata.default_sort_order().is_none() {
            return Err(MetadataBuildError::SortOrderNotFound {
                order_id: self.metadata.default_sort_order_id,
            });
        }
        Ok(())
    }

    // -- identity and format version ----------------------------------------

    fn assign_uuid(&mut self, uuid: Uuid) -> Result<(), MetadataBuildError> {
        if self.uuid_assigned && self.metadata.table_uuid != uuid {
            return Err(MetadataBuildError::UuidMismatch {
                current: self.metadata.table_uuid,
                requested: uuid,
            });
        }
        self.metadata.table_uuid = uuid;
        self.uuid_assigned = true;
        Ok(())
    }

    fn upgrade_format_version(&mut self, version: u8) -> Result<(), MetadataBuildError> {
        if !(1..=3).contains(&version) {
            return Err(MetadataBuildError::UnsupportedFormatVersion { version });
        }
        if version < self.metadata.format_version {
            return Err(MetadataBuildError::FormatVersionDowngrade {
                current: self.metadata.format_version,
                requested: version,
            });
        }
        self.metadata.format_version = version;
        if version >= 2 && self.metadata.last_sequence_number.is_none() {
            self.metadata.last_sequence_number = Some(0);
        }
        if version >= 3 && self.metadata.next_row_id.is_none() {
            self.metadata.next_row_id = Some(0);
        }
        Ok(())
    }

    // -- schemas --------------------------------------------------------------

    fn require_v3(&self, feature: impl Into<String>) -> Result<(), MetadataBuildError> {
        if self.metadata.format_version < 3 {
            return Err(MetadataBuildError::RequiresV3 {
                feature: feature.into(),
                format_version: self.metadata.format_version,
            });
        }
        Ok(())
    }

    fn validate_schema(&self, schema: &Schema) -> Result<(), MetadataBuildError> {
        let ids_in_order = schema.field_ids_in_order();
        let mut seen = BTreeSet::new();
        for id in &ids_in_order {
            if *id < 1 {
                return Err(MetadataBuildError::InvalidSchema {
                    reason: format!("field id {id} is not positive"),
                });
            }
            if !seen.insert(*id) {
                return Err(MetadataBuildError::InvalidSchema {
                    reason: format!("field id {id} is used more than once"),
                });
            }
        }
        for field in &schema.fields {
            self.validate_struct_field(field)?;
        }
        if let Some(identifier_ids) = &schema.identifier_field_ids {
            for id in identifier_ids {
                if !seen.contains(id) {
                    return Err(MetadataBuildError::InvalidSchema {
                        reason: format!("identifier field id {id} is not a field of the schema"),
                    });
                }
            }
        }
        Ok(())
    }

    fn validate_struct_field(
        &self,
        field: &super::types::StructField,
    ) -> Result<(), MetadataBuildError> {
        if field.name.is_empty() {
            return Err(MetadataBuildError::InvalidSchema {
                reason: format!("field {} has an empty name", field.id),
            });
        }
        if let Some(type_string) = field.field_type.find_unrecognized_primitive() {
            return Err(MetadataBuildError::UnknownFieldType { type_string });
        }
        if let Some(v3_type) = field.field_type.find_v3_primitive() {
            self.require_v3(format!("field type {v3_type}"))?;
        }
        if field.initial_default.is_some() || field.write_default.is_some() {
            self.require_v3(format!("default values (field {:?})", field.name))?;
        }
        // Recurse into nested struct fields for name/default checks.
        if let super::types::Type::Struct(nested) = &field.field_type {
            for nested_field in &nested.fields {
                self.validate_struct_field(nested_field)?;
            }
        }
        Ok(())
    }

    fn add_schema(
        &mut self,
        mut schema: Schema,
        last_column_id: Option<i32>,
    ) -> Result<(), MetadataBuildError> {
        self.validate_schema(&schema)?;
        let max_field_id = schema.max_field_id();
        if let Some(provided) = last_column_id
            && provided < max_field_id
        {
            return Err(MetadataBuildError::LastColumnIdTooLow {
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
        self.metadata.last_column_id = self.metadata.last_column_id.max(max_field_id);
        self.metadata.schemas.push(schema);
        self.last_added_schema_id = Some(new_id);
        Ok(())
    }

    fn set_current_schema(&mut self, schema_id: i32) -> Result<(), MetadataBuildError> {
        let schema_id = if schema_id == LAST_ADDED {
            self.last_added_schema_id
                .ok_or(MetadataBuildError::NoLastAddedSchema)?
        } else {
            schema_id
        };
        if self.metadata.schema_by_id(schema_id).is_none() {
            return Err(MetadataBuildError::SchemaNotFound { schema_id });
        }
        self.metadata.current_schema_id = schema_id;
        Ok(())
    }

    fn remove_schemas(&mut self, schema_ids: &[i32]) -> Result<(), MetadataBuildError> {
        for &schema_id in schema_ids {
            if self.metadata.schema_by_id(schema_id).is_none() {
                return Err(MetadataBuildError::SchemaNotFound { schema_id });
            }
            if schema_id == self.metadata.current_schema_id {
                return Err(MetadataBuildError::CurrentSchemaRemoval { schema_id });
            }
            if let Some(snapshots) = &self.metadata.snapshots
                && let Some(user) = snapshots.iter().find(|s| s.schema_id == Some(schema_id))
            {
                return Err(MetadataBuildError::SchemaInUse {
                    schema_id,
                    snapshot_id: user.snapshot_id,
                });
            }
        }
        // last-column-id is intentionally NOT lowered: field ids are never
        // reused, even after the schemas that used them are gone.
        self.metadata
            .schemas
            .retain(|s| s.schema_id.is_none_or(|id| !schema_ids.contains(&id)));
        Ok(())
    }

    // -- partition specs ------------------------------------------------------

    fn current_schema_field_ids(&self) -> Result<BTreeSet<i32>, MetadataBuildError> {
        self.metadata
            .current_schema()
            .map(Schema::all_field_ids)
            .ok_or(MetadataBuildError::CurrentSchemaUnset)
    }

    fn add_spec(&mut self, mut spec: PartitionSpec) -> Result<(), MetadataBuildError> {
        let schema_field_ids = self.current_schema_field_ids()?;
        let mut names = BTreeSet::new();
        for field in &spec.fields {
            if field.name.is_empty() {
                return Err(MetadataBuildError::InvalidPartitionSpec {
                    reason: "partition field names must not be empty".to_owned(),
                });
            }
            if !names.insert(field.name.as_str()) {
                return Err(MetadataBuildError::InvalidPartitionSpec {
                    reason: format!("duplicate partition field name {:?}", field.name),
                });
            }
            if !field.transform.is_recognized() {
                return Err(MetadataBuildError::UnknownTransform {
                    transform: field.transform.to_string(),
                });
            }
            // Void-transform fields may reference dropped columns; all other
            // transforms must bind to the current schema.
            if field.transform != super::transform::Transform::Void
                && !schema_field_ids.contains(&field.source_id)
            {
                return Err(MetadataBuildError::UnknownSourceField {
                    source_id: field.source_id,
                });
            }
        }

        // Re-adding a structurally identical spec (same source/transform/
        // name sequence) reuses its id.
        if let Some(existing) = self.metadata.partition_specs.iter().find(|existing| {
            existing.fields.len() == spec.fields.len()
                && existing.fields.iter().zip(&spec.fields).all(|(a, b)| {
                    a.source_id == b.source_id && a.transform == b.transform && a.name == b.name
                })
        }) && let Some(existing_id) = existing.spec_id
        {
            self.last_added_spec_id = Some(existing_id);
            return Ok(());
        }

        // Assign missing partition field ids above everything ever assigned.
        let mut next_field_id = self
            .metadata
            .last_partition_id
            .max(spec.max_assigned_field_id().unwrap_or(0))
            .max(PARTITION_DATA_ID_START - 1);
        for field in &mut spec.fields {
            if field.field_id.is_none() {
                next_field_id += 1;
                field.field_id = Some(next_field_id);
            }
        }
        let mut field_ids = BTreeSet::new();
        for field in &spec.fields {
            if let Some(id) = field.field_id
                && !field_ids.insert(id)
            {
                return Err(MetadataBuildError::InvalidPartitionSpec {
                    reason: format!("duplicate partition field id {id}"),
                });
            }
        }

        let new_spec_id = self
            .metadata
            .partition_specs
            .iter()
            .filter_map(|s| s.spec_id)
            .max()
            .map_or(0, |max| max + 1);
        spec.spec_id = Some(new_spec_id);
        self.metadata.last_partition_id = self
            .metadata
            .last_partition_id
            .max(spec.max_assigned_field_id().unwrap_or(0));
        self.metadata.partition_specs.push(spec);
        self.last_added_spec_id = Some(new_spec_id);
        Ok(())
    }

    fn set_default_spec(&mut self, spec_id: i32) -> Result<(), MetadataBuildError> {
        let spec_id = if spec_id == LAST_ADDED {
            self.last_added_spec_id
                .ok_or(MetadataBuildError::NoLastAddedSpec)?
        } else {
            spec_id
        };
        if self.metadata.partition_spec_by_id(spec_id).is_none() {
            return Err(MetadataBuildError::SpecNotFound { spec_id });
        }
        self.metadata.default_spec_id = spec_id;
        Ok(())
    }

    fn remove_partition_specs(&mut self, spec_ids: &[i32]) -> Result<(), MetadataBuildError> {
        for &spec_id in spec_ids {
            if self.metadata.partition_spec_by_id(spec_id).is_none() {
                return Err(MetadataBuildError::SpecNotFound { spec_id });
            }
            if spec_id == self.metadata.default_spec_id {
                return Err(MetadataBuildError::DefaultSpecRemoval { spec_id });
            }
        }
        // last-partition-id is intentionally NOT lowered: partition field
        // ids are never reused.
        self.metadata
            .partition_specs
            .retain(|s| s.spec_id.is_none_or(|id| !spec_ids.contains(&id)));
        Ok(())
    }

    // -- sort orders ------------------------------------------------------------

    fn add_sort_order(&mut self, mut order: SortOrder) -> Result<(), MetadataBuildError> {
        let schema_field_ids = self.current_schema_field_ids()?;
        for field in &order.fields {
            if !field.transform.is_recognized() {
                return Err(MetadataBuildError::UnknownTransform {
                    transform: field.transform.to_string(),
                });
            }
            if !schema_field_ids.contains(&field.source_id) {
                return Err(MetadataBuildError::UnknownSourceField {
                    source_id: field.source_id,
                });
            }
        }

        if let Some(existing) = self
            .metadata
            .sort_orders
            .iter()
            .find(|existing| existing.same_structure(&order))
        {
            self.last_added_order_id = Some(existing.order_id);
            return Ok(());
        }

        // Order id 0 is reserved for (and only for) the unsorted order.
        let new_order_id = if order.fields.is_empty() {
            0
        } else {
            self.metadata
                .sort_orders
                .iter()
                .map(|o| o.order_id)
                .max()
                .map_or(1, |max| max.max(0) + 1)
        };
        order.order_id = new_order_id;
        self.metadata.sort_orders.push(order);
        self.last_added_order_id = Some(new_order_id);
        Ok(())
    }

    fn set_default_sort_order(&mut self, order_id: i32) -> Result<(), MetadataBuildError> {
        let order_id = if order_id == LAST_ADDED {
            self.last_added_order_id
                .ok_or(MetadataBuildError::NoLastAddedSortOrder)?
        } else {
            order_id
        };
        if self.metadata.sort_order_by_id(order_id).is_none() {
            return Err(MetadataBuildError::SortOrderNotFound { order_id });
        }
        self.metadata.default_sort_order_id = order_id;
        Ok(())
    }

    // -- snapshots and refs -------------------------------------------------------

    fn add_snapshot(&mut self, mut snapshot: Snapshot) -> Result<(), MetadataBuildError> {
        let snapshot_id = snapshot.snapshot_id;
        if self.metadata.snapshot_by_id(snapshot_id).is_some() {
            return Err(MetadataBuildError::SnapshotAlreadyExists { snapshot_id });
        }
        if let Some(parent_id) = snapshot.parent_snapshot_id
            && self.metadata.snapshot_by_id(parent_id).is_none()
        {
            return Err(MetadataBuildError::ParentSnapshotNotFound {
                snapshot_id,
                parent_id,
            });
        }
        if snapshot.manifest_list.is_none() {
            return Err(MetadataBuildError::InvalidSnapshot {
                snapshot_id,
                reason: "manifest-list is required".to_owned(),
            });
        }
        if let Some(schema_id) = snapshot.schema_id
            && self.metadata.schema_by_id(schema_id).is_none()
        {
            return Err(MetadataBuildError::SchemaNotFound { schema_id });
        }

        // Validate everything before mutating any tracking state.
        // Sequence numbers: strictly monotonic from v2 on, absent/zero in
        // v1.
        let new_sequence_number = if self.metadata.format_version >= 2 {
            let last = self.metadata.last_sequence_number.unwrap_or(0);
            let Some(provided) = snapshot.sequence_number else {
                return Err(MetadataBuildError::InvalidSnapshot {
                    snapshot_id,
                    reason: "sequence-number is required from format version 2".to_owned(),
                });
            };
            if provided <= last {
                return Err(MetadataBuildError::NonMonotonicSequenceNumber {
                    snapshot_id,
                    provided,
                    last,
                });
            }
            Some(provided)
        } else {
            if snapshot.sequence_number.is_some_and(|n| n != 0) {
                return Err(MetadataBuildError::InvalidSnapshot {
                    snapshot_id,
                    reason: "v1 snapshots cannot carry a sequence number".to_owned(),
                });
            }
            None
        };

        // Row lineage: v3 assigns first-row-id server-side and advances
        // next-row-id.
        if self.metadata.format_version >= 3 {
            let next_row_id = self.metadata.next_row_id.unwrap_or(0);
            let added_rows = snapshot.added_rows.unwrap_or(0);
            if added_rows < 0 {
                return Err(MetadataBuildError::InvalidSnapshot {
                    snapshot_id,
                    reason: "added-rows must not be negative".to_owned(),
                });
            }
            snapshot.first_row_id = Some(next_row_id);
            snapshot.added_rows = Some(added_rows);
            self.metadata.next_row_id = Some(next_row_id + added_rows);
        } else if snapshot.first_row_id.is_some() || snapshot.added_rows.is_some() {
            return Err(MetadataBuildError::RequiresV3 {
                feature: "row lineage (first-row-id/added-rows)".to_owned(),
                format_version: self.metadata.format_version,
            });
        }
        if let Some(sequence_number) = new_sequence_number {
            self.metadata.last_sequence_number = Some(sequence_number);
        }

        self.metadata
            .snapshots
            .get_or_insert_with(Vec::new)
            .push(snapshot);
        Ok(())
    }

    fn set_snapshot_ref(
        &mut self,
        ref_name: String,
        reference: SnapshotRef,
    ) -> Result<(), MetadataBuildError> {
        let snapshot_id = reference.snapshot_id;
        let Some(snapshot) = self.metadata.snapshot_by_id(snapshot_id) else {
            return Err(MetadataBuildError::SnapshotNotFound { snapshot_id });
        };
        if reference.ref_type == RefType::Tag
            && (reference.min_snapshots_to_keep.is_some()
                || reference.max_snapshot_age_ms.is_some())
        {
            return Err(MetadataBuildError::BranchRetentionOnTag { name: ref_name });
        }

        // Moving the main branch moves the current snapshot and extends the
        // snapshot log. The log timestamp is the snapshot's own commit
        // timestamp so the builder stays deterministic.
        let is_main_branch = ref_name == "main" && reference.ref_type == RefType::Branch;
        let snapshot_timestamp_ms = snapshot.timestamp_ms;
        if is_main_branch && self.metadata.current_snapshot_id != Some(snapshot_id) {
            self.metadata.current_snapshot_id = Some(snapshot_id);
            self.metadata
                .snapshot_log
                .get_or_insert_with(Vec::new)
                .push(SnapshotLogEntry {
                    snapshot_id,
                    timestamp_ms: snapshot_timestamp_ms,
                });
        }
        self.metadata
            .refs
            .get_or_insert_with(std::collections::BTreeMap::new)
            .insert(ref_name, reference);
        Ok(())
    }

    fn remove_snapshots(&mut self, snapshot_ids: &[i64]) -> Result<(), MetadataBuildError> {
        for &snapshot_id in snapshot_ids {
            if self.metadata.snapshot_by_id(snapshot_id).is_none() {
                return Err(MetadataBuildError::SnapshotNotFound { snapshot_id });
            }
            if self.metadata.current_snapshot_id == Some(snapshot_id) {
                return Err(MetadataBuildError::CurrentSnapshotRemoval { snapshot_id });
            }
            if let Some(refs) = &self.metadata.refs
                && let Some((name, _)) = refs.iter().find(|(_, r)| r.snapshot_id == snapshot_id)
            {
                return Err(MetadataBuildError::SnapshotReferenced {
                    snapshot_id,
                    ref_name: name.clone(),
                });
            }
        }
        if let Some(snapshots) = &mut self.metadata.snapshots {
            snapshots.retain(|s| !snapshot_ids.contains(&s.snapshot_id));
        }
        // History and statistics entries for removed snapshots go with them.
        if let Some(log) = &mut self.metadata.snapshot_log {
            log.retain(|e| !snapshot_ids.contains(&e.snapshot_id));
        }
        if let Some(statistics) = &mut self.metadata.statistics {
            statistics.retain(|s| !snapshot_ids.contains(&s.snapshot_id));
        }
        if let Some(partition_statistics) = &mut self.metadata.partition_statistics {
            partition_statistics.retain(|s| !snapshot_ids.contains(&s.snapshot_id));
        }
        Ok(())
    }

    fn remove_snapshot_ref(&mut self, ref_name: &str) -> Result<(), MetadataBuildError> {
        let removed = self
            .metadata
            .refs
            .as_mut()
            .and_then(|refs| refs.remove(ref_name));
        if removed.is_none() {
            return Err(MetadataBuildError::RefNotFound {
                name: ref_name.to_owned(),
            });
        }
        if ref_name == "main" {
            self.metadata.current_snapshot_id = None;
        }
        Ok(())
    }

    // -- location and properties ---------------------------------------------------

    fn set_location(&mut self, location: String) -> Result<(), MetadataBuildError> {
        if location.is_empty() {
            return Err(MetadataBuildError::EmptyLocation);
        }
        self.metadata.location = location;
        Ok(())
    }

    fn set_properties(
        &mut self,
        updates: std::collections::BTreeMap<String, String>,
    ) -> Result<(), MetadataBuildError> {
        for key in updates.keys() {
            Self::validate_property_key(key)?;
        }
        self.metadata
            .properties
            .get_or_insert_with(std::collections::BTreeMap::new)
            .extend(updates);
        Ok(())
    }

    fn remove_properties(&mut self, removals: &[String]) -> Result<(), MetadataBuildError> {
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

    fn validate_property_key(key: &str) -> Result<(), MetadataBuildError> {
        if key.is_empty() {
            return Err(MetadataBuildError::EmptyPropertyKey);
        }
        if RESERVED_PROPERTIES.contains(&key) {
            return Err(MetadataBuildError::ReservedProperty {
                key: key.to_owned(),
            });
        }
        Ok(())
    }

    // -- statistics ---------------------------------------------------------------

    fn set_statistics(
        &mut self,
        deprecated_snapshot_id: Option<i64>,
        statistics: StatisticsFile,
    ) -> Result<(), MetadataBuildError> {
        if let Some(update_id) = deprecated_snapshot_id
            && update_id != statistics.snapshot_id
        {
            return Err(MetadataBuildError::StatisticsSnapshotMismatch {
                update: update_id,
                file: statistics.snapshot_id,
            });
        }
        let snapshot_id = statistics.snapshot_id;
        if self.metadata.snapshot_by_id(snapshot_id).is_none() {
            return Err(MetadataBuildError::SnapshotNotFound { snapshot_id });
        }
        let files = self.metadata.statistics.get_or_insert_with(Vec::new);
        files.retain(|f| f.snapshot_id != snapshot_id);
        files.push(statistics);
        Ok(())
    }

    fn remove_statistics(&mut self, snapshot_id: i64) -> Result<(), MetadataBuildError> {
        let files = self.metadata.statistics.get_or_insert_with(Vec::new);
        let before = files.len();
        files.retain(|f| f.snapshot_id != snapshot_id);
        if files.len() == before {
            return Err(MetadataBuildError::StatisticsNotFound { snapshot_id });
        }
        Ok(())
    }

    fn set_partition_statistics(
        &mut self,
        statistics: PartitionStatisticsFile,
    ) -> Result<(), MetadataBuildError> {
        let snapshot_id = statistics.snapshot_id;
        if self.metadata.snapshot_by_id(snapshot_id).is_none() {
            return Err(MetadataBuildError::SnapshotNotFound { snapshot_id });
        }
        let files = self
            .metadata
            .partition_statistics
            .get_or_insert_with(Vec::new);
        files.retain(|f| f.snapshot_id != snapshot_id);
        files.push(statistics);
        Ok(())
    }

    fn remove_partition_statistics(&mut self, snapshot_id: i64) -> Result<(), MetadataBuildError> {
        let files = self
            .metadata
            .partition_statistics
            .get_or_insert_with(Vec::new);
        let before = files.len();
        files.retain(|f| f.snapshot_id != snapshot_id);
        if files.len() == before {
            return Err(MetadataBuildError::PartitionStatisticsNotFound { snapshot_id });
        }
        Ok(())
    }

    // -- encryption keys (v3) --------------------------------------------------------

    fn add_encryption_key(
        &mut self,
        key: super::encryption::EncryptedKey,
    ) -> Result<(), MetadataBuildError> {
        self.require_v3("encryption keys")?;
        let keys = self.metadata.encryption_keys.get_or_insert_with(Vec::new);
        if keys.iter().any(|k| k.key_id == key.key_id) {
            return Err(MetadataBuildError::DuplicateEncryptionKey { key_id: key.key_id });
        }
        keys.push(key);
        Ok(())
    }

    fn remove_encryption_key(&mut self, key_id: &str) -> Result<(), MetadataBuildError> {
        self.require_v3("encryption keys")?;
        let keys = self.metadata.encryption_keys.get_or_insert_with(Vec::new);
        let before = keys.len();
        keys.retain(|k| k.key_id != key_id);
        if keys.len() == before {
            return Err(MetadataBuildError::EncryptionKeyNotFound {
                key_id: key_id.to_owned(),
            });
        }
        Ok(())
    }
}
