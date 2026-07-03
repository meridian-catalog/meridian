//! Meridian's built-in maintenance executors (spec Pillar C, C-F2/C-F4 tier
//! 1). This crate is the flagship's engine: it does the actual table work the
//! catalog promises to do *for* the customer.
//!
//! The first executor is **compaction** (bin-pack rewrite). Given a table's
//! current metadata it:
//!
//! 1. reads the current snapshot's live data files (via
//!    `meridian_iceberg::manifest`), groups them by partition, and bin-packs
//!    the small ones into groups approaching a target size (default 512 MiB),
//!    skipping partitions with fewer than `min_input_files` (default 5) small
//!    files — [`select`];
//! 2. rewrites each group's inputs into one target-sized Parquet file,
//!    mapping columns by Iceberg **field id** (not name), materializing any
//!    pending position/equality **deletes** (v2 merge-on-read), and carrying
//!    column stats into the output — [`rewrite`], with the hard correctness
//!    assertion *rows in == rows out + rows deleted*;
//! 3. produces an Iceberg `RewriteFiles` (replace) [`plan::CompactionPlan`]: a
//!    `Vec<TableUpdate>` (add-snapshot `replace` + move `main`) and
//!    `Vec<TableRequirement>` (assert the table has not moved), plus the new
//!    snapshot's manifest/manifest-list Avro — [`metadata_result`] — **without
//!    committing**. The next wave hands the plan to `PostgresCommitBackend` so
//!    the rewrite lands as a normal, audited, snapshot-rollback-revertible
//!    commit (spec §6 Pillar C enterprise notes).
//!
//! The orchestration entry point is [`compact::compact_table`]. Safety
//! properties (spec §Pillar C): a **dry-run** mode returns the plan and the
//! files it *would* write without writing anything; the engine **never deletes
//! input data files** (that is snapshot-expiry + orphan cleanup, later, with
//! safety windows — this only marks them `DELETED` in the new snapshot's
//! manifests, which is reversible by snapshot rollback); and re-running on an
//! already-compacted table is a **no-op** (idempotent).
//!
//! ## What this crate does not do
//!
//! It does not touch server routes or the commit backend — it hands back a
//! plan. It does not implement sort/z-order compaction, snapshot expiry,
//! orphan cleanup, or deletion-vector (v3 Puffin) materialization yet; those
//! are scoped in `docs/design/compaction.md`. Deletion vectors attached to an
//! input are refused with a clear reason rather than silently dropped.

pub mod arrow_schema;
pub mod compact;
pub mod error;
pub mod manifest_source;
pub mod metadata_result;
pub mod plan;
pub mod rewrite;
pub mod select;
pub mod stats;

pub use compact::{compact_table, compact_with_sources};
pub use error::{CompactionError, CompactionResult};
pub use manifest_source::{ManifestSource, StorageManifestSource};
pub use plan::{
    CompactionOptions, CompactionPlan, CompactionStats, DEFAULT_MIN_INPUT_FILES,
    DEFAULT_TARGET_FILE_SIZE_BYTES, NewFile,
};
pub use rewrite::FileBytes;
pub use select::{Selection, bin_pack, read_selection};
