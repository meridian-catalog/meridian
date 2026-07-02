//! The Iceberg spec model: table and view `metadata.json` and the REST
//! commit vocabulary (updates and requirements), plus the builders that
//! apply updates with full validation.
//!
//! Coverage: the complete v2 table-metadata shape, v1 read (normalized on
//! parse, legacy fields re-emitted on write), v3 (row lineage, encryption
//! keys, new primitive types), and view metadata (format version 1, the
//! only one the view spec defines). Everything the typed model does not
//! know is preserved verbatim through the `extra` maps on every struct.
//!
//! Known gaps, tracked honestly:
//!
//! - TODO(M1+): v3 multi-argument transforms (`source-ids` on partition
//!   fields) are preserved via `extra` but not validated.
//! - TODO(M1+): default values (`initial-default`/`write-default`) are kept
//!   as raw JSON and not validated against the field type.
//! - TODO(M2): transform/source-type compatibility checks (e.g. `year` on a
//!   non-date column) need type resolution during scan planning.
//! - TODO(M2): the view builder does not yet enforce dialect-drop
//!   protection (`replace.drop-dialect.allowed`); see
//!   [`view_builder`] module docs.

pub mod builder;
pub mod encryption;
pub mod partition;
pub mod requirement;
pub mod schema;
pub mod snapshot;
pub mod sort;
pub mod statistics;
pub mod table_metadata;
pub mod transform;
pub mod types;
pub mod update;
pub mod view;
pub mod view_builder;
pub mod view_requirement;
pub mod view_update;

pub use builder::{MetadataBuildError, MetadataBuilder};
pub use encryption::EncryptedKey;
pub use partition::{PartitionField, PartitionSpec};
pub use requirement::{RequirementFailed, TableRequirement};
pub use schema::Schema;
pub use snapshot::{MetadataLogEntry, RefType, Snapshot, SnapshotLogEntry, SnapshotRef};
pub use sort::{NullOrder, SortDirection, SortField, SortOrder};
pub use statistics::{BlobMetadata, PartitionStatisticsFile, StatisticsFile};
pub use table_metadata::{MetadataParseError, TableMetadata};
pub use transform::Transform;
pub use types::{ListType, MapType, PrimitiveType, StructField, StructType, Type};
pub use update::{LAST_ADDED, TableUpdate};
pub use view::{
    SqlViewRepresentation, VIEW_FORMAT_VERSION, ViewHistoryEntry, ViewMetadata,
    ViewMetadataParseError, ViewRepresentation, ViewVersion,
};
pub use view_builder::{
    VERSION_HISTORY_NUM_ENTRIES_DEFAULT, VERSION_HISTORY_NUM_ENTRIES_PROP, ViewMetadataBuildError,
    ViewMetadataBuilder,
};
pub use view_requirement::ViewRequirement;
pub use view_update::ViewUpdate;
