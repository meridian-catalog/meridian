//! Serde models for the Iceberg table spec (metadata.json and friends).
//!
//! Coverage in M0 is the v2 table-metadata shape sufficient for lossless
//! round-tripping. Known gaps, tracked for M1:
//!
//! - TODO(M1): typed schema type tree (struct/list/map/primitive) — field
//!   types are currently passed through as raw JSON.
//! - TODO(M1): v1 metadata (legacy `schema`/`partition-spec` fields) and v3
//!   completeness (`next-row-id`, `encryption-keys`, row lineage,
//!   multi-argument transforms).
//! - TODO(M1): `statistics` / `partition-statistics` models (currently
//!   preserved via the `extra` maps).

pub mod partition;
pub mod schema;
pub mod snapshot;
pub mod sort;
pub mod table_metadata;

pub use partition::{PartitionField, PartitionSpec};
pub use schema::{Schema, SchemaField};
pub use snapshot::{MetadataLogEntry, RefType, Snapshot, SnapshotLogEntry, SnapshotRef};
pub use sort::{NullOrder, SortDirection, SortField, SortOrder};
pub use table_metadata::TableMetadata;
