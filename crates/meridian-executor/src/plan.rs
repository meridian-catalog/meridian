//! The public compaction API: options in, a [`CompactionPlan`] out.
//!
//! A [`CompactionPlan`] is a *proposal*, never a commit. It carries the
//! Iceberg `updates`/`requirements` the next wave hands to
//! `PostgresCommitBackend` so the rewrite lands as a normal, audited,
//! snapshot-rollback-revertible commit (spec §6 Pillar C enterprise notes),
//! plus a manifest of the data files it wrote (or, in dry-run, *would* write)
//! and the before/after ledger numbers the savings ledger is built from.

use meridian_iceberg::manifest::DataFile;
use meridian_iceberg::spec::{TableRequirement, TableUpdate};

/// Default target size for compacted output files: 512 MiB (the Iceberg /
/// Spark `write.target-file-size-bytes` default).
pub const DEFAULT_TARGET_FILE_SIZE_BYTES: u64 = 512 * 1024 * 1024;

/// Default minimum number of small input files a partition (or bin-pack
/// group) must have before it is worth rewriting. Below this, the read +
/// rewrite + commit cost outweighs the fragmentation removed.
pub const DEFAULT_MIN_INPUT_FILES: usize = 5;

/// Tuning for a compaction run.
#[derive(Debug, Clone)]
pub struct CompactionOptions {
    /// Files at or above this size are already "big enough" and are left
    /// untouched; only files strictly below it are candidates. Output files
    /// are packed to approach this size. Must be non-zero.
    pub target_file_size_bytes: u64,
    /// A partition is skipped unless it has at least this many candidate
    /// (small) files — nothing is gained by rewriting fewer.
    pub min_input_files: usize,
    /// When `true`, produce the plan and the list of files that *would* be
    /// written without writing any data or manifest bytes (spec §Pillar C
    /// safety). The returned [`CompactionPlan::updates`] is empty in this
    /// mode: there is nothing to commit because nothing was staged.
    pub dry_run: bool,
}

impl Default for CompactionOptions {
    fn default() -> Self {
        Self {
            target_file_size_bytes: DEFAULT_TARGET_FILE_SIZE_BYTES,
            min_input_files: DEFAULT_MIN_INPUT_FILES,
            dry_run: false,
        }
    }
}

/// A data file compaction wrote (or, in dry-run, would write).
#[derive(Debug, Clone)]
pub struct NewFile {
    /// The manifest `DataFile` describing the output (path, size, record
    /// count, partition tuple, column stats, field-id-keyed bounds). In
    /// dry-run the `file_size_in_bytes`, stats, and bounds are estimates and
    /// the file does not exist on storage.
    pub data_file: DataFile,
    /// Whether this file's bytes were actually written to storage. `false`
    /// in dry-run mode.
    pub written: bool,
}

/// Before/after file and byte counts for the whole run (the savings-ledger
/// inputs, spec C-F5).
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct CompactionStats {
    /// Live data files in the input snapshot that were selected for rewrite.
    pub files_before: u64,
    /// Data files produced to replace them.
    pub files_after: u64,
    /// Total bytes of the rewritten input files.
    pub bytes_before: u64,
    /// Total bytes of the produced files (estimated in dry-run).
    pub bytes_after: u64,
    /// Rows in the rewritten inputs, before applying pending deletes.
    pub records_before: i64,
    /// Rows in the produced files (== `records_before` for v1 / no-delete
    /// tables; strictly fewer when pending deletes were materialized).
    pub records_after: i64,
    /// Delete files whose effect was materialized into the output and which
    /// the new snapshot therefore drops. Zero unless merge-on-read deletes
    /// were applied.
    pub delete_files_removed: u64,
}

impl CompactionStats {
    /// A zeroed ledger (the no-op / nothing-selected case).
    #[must_use]
    pub fn empty() -> Self {
        Self {
            files_before: 0,
            files_after: 0,
            bytes_before: 0,
            bytes_after: 0,
            records_before: 0,
            records_after: 0,
            delete_files_removed: 0,
        }
    }

    /// Bytes removed (never negative in practice: compaction rewrites small
    /// files into fewer, better-encoded ones — but computed saturating so a
    /// pathological input can never underflow).
    #[must_use]
    pub fn bytes_saved(&self) -> u64 {
        self.bytes_before.saturating_sub(self.bytes_after)
    }
}

/// The result of planning (and, unless dry-run, staging) a compaction.
///
/// `updates` + `requirements` are the Iceberg commit this represents; they
/// are **not** applied here. The next wave calls the commit path with them so
/// the rewrite is a normal optimistic commit: `requirements` assert the table
/// has not moved since planning (so a racing writer makes the commit fail
/// cleanly, never corrupt), and `updates` add the replace-snapshot that swaps
/// the rewritten files for their replacements.
#[derive(Debug, Clone)]
pub struct CompactionPlan {
    /// The `TableUpdate`s to commit: an `add-snapshot` (operation `replace`)
    /// whose manifests ADD the new files and mark the old ones DELETED, then
    /// a `set-snapshot-ref` moving `main` to it. Empty when nothing was
    /// selected, or in dry-run mode.
    pub updates: Vec<TableUpdate>,
    /// The `TableRequirement`s that must hold at commit time: the table UUID
    /// and the `main` branch still pointing at the snapshot this plan was
    /// built against. Empty when there is nothing to commit.
    pub requirements: Vec<TableRequirement>,
    /// Every data file written (or, in dry-run, that would be written).
    pub new_files_written: Vec<NewFile>,
    /// Before/after ledger numbers.
    pub stats: CompactionStats,
    /// The snapshot id this plan rewrites (the current snapshot at planning
    /// time). `None` when the table had no snapshot to compact.
    pub base_snapshot_id: Option<i64>,
    /// The snapshot id the plan's `add-snapshot` introduces. `None` when
    /// nothing was selected or in dry-run.
    pub new_snapshot_id: Option<i64>,
}

impl CompactionPlan {
    /// A plan that changes nothing: the table was already compact, had no
    /// snapshot, or every partition was below `min_input_files`. Re-running
    /// compaction on an already-compacted table returns this — the operation
    /// is idempotent (spec §Pillar C safety).
    #[must_use]
    pub fn noop(base_snapshot_id: Option<i64>) -> Self {
        Self {
            updates: Vec::new(),
            requirements: Vec::new(),
            new_files_written: Vec::new(),
            stats: CompactionStats::empty(),
            base_snapshot_id,
            new_snapshot_id: None,
        }
    }

    /// Whether this plan would change the table. `false` for [`Self::noop`]
    /// and for any dry-run (which stages nothing to commit).
    #[must_use]
    pub fn is_noop(&self) -> bool {
        self.updates.is_empty()
    }

    /// Files removed by this compaction (bin-packed inputs) minus files
    /// added — the fragmentation reduction, for logs and the ledger.
    #[must_use]
    pub fn files_reduced(&self) -> i64 {
        i64::from(self.stats.files_before >= self.stats.files_after)
            * (i64::try_from(self.stats.files_before).unwrap_or(i64::MAX)
                - i64::try_from(self.stats.files_after).unwrap_or(0))
    }
}
