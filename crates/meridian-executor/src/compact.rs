//! The compaction orchestrator: read → select → bin-pack → rewrite → plan.
//!
//! This is the entry point the next wave calls. It ties the pieces together
//! and enforces the correctness contract before returning anything:
//!
//! 1. Read the current snapshot's live files and group them by partition
//!    ([`crate::select`]).
//! 2. Bin-pack the small files of each partition into rewrite groups.
//! 3. For each group, read the inputs, apply pending deletes, and write one
//!    target-sized output Parquet file ([`crate::rewrite`]) — **asserting**
//!    that output rows == input rows minus deleted rows.
//! 4. Assemble the `RewriteFiles` (replace) commit ([`crate::metadata_result`])
//!    and return it as a [`CompactionPlan`] — without committing.
//!
//! Dry-run stops after step 2: it reports the plan and the files it *would*
//! write (sizes/records estimated from the inputs) without reading data or
//! writing bytes.

use std::sync::atomic::{AtomicU64, Ordering};

use bytes::Bytes;
use meridian_iceberg::manifest::{DataFile, DataFileContent, PartitionTuple};
use meridian_iceberg::spec::{Schema, TableMetadata};
use meridian_storage::Storage;
use parquet::basic::Compression;
use uuid::Uuid;

use crate::error::{CompactionError, CompactionResult};
use crate::manifest_source::{ManifestSource, StorageManifestSource};
use crate::metadata_result::{AddedFile, RewriteResult, build_rewrite_commit};
use crate::plan::{CompactionOptions, CompactionPlan, CompactionStats, NewFile};
use crate::rewrite::{FileBytes, output_data_file, rewrite_group};
use crate::select::{BinPackGroup, InputFile, Selection, bin_pack, read_selection};

/// A [`FileBytes`] over a warehouse [`Storage`] handle.
struct StorageFileBytes<'a> {
    storage: &'a dyn Storage,
}

impl FileBytes for StorageFileBytes<'_> {
    async fn read(&self, location: &str) -> CompactionResult<Bytes> {
        Ok(self.storage.read(location).await?)
    }
}

/// Compacts a table against a live warehouse [`Storage`] handle.
///
/// Reads the current snapshot from `metadata`, plans and (unless
/// `options.dry_run`) writes compacted Parquet + the new snapshot's
/// manifests, and returns the [`CompactionPlan`]. **Does not commit** — the
/// returned plan's `updates`/`requirements` go to the commit path.
///
/// `new_ids` mints the fresh snapshot id (and any other ids); pass a source
/// seeded however the caller wants (a random generator in production, a fixed
/// one in tests). It is `Send + Sync` so the returned future stays `Send` —
/// the built-in worker drives this from a `tokio::spawn`ed maintenance task.
pub async fn compact_table(
    storage: &dyn Storage,
    metadata: &TableMetadata,
    options: &CompactionOptions,
    new_ids: &(dyn Fn() -> i64 + Send + Sync),
) -> CompactionResult<CompactionPlan> {
    if options.target_file_size_bytes == 0 {
        return Err(CompactionError::InvalidRequest(
            "target_file_size_bytes must be non-zero".to_owned(),
        ));
    }

    let source = StorageManifestSource::new(storage);
    let files = StorageFileBytes { storage };
    compact_with_sources(&source, &files, storage, metadata, options, new_ids).await
}

/// The storage-agnostic core, exercised directly by tests with in-memory
/// sources. `manifests` reads parsed manifest Avro; `files` reads raw
/// data/delete file bytes; `writer` writes output bytes and returns the
/// object size actually written.
pub async fn compact_with_sources<M, F>(
    manifests: &M,
    files: &F,
    writer: &dyn Storage,
    metadata: &TableMetadata,
    options: &CompactionOptions,
    new_ids: &(dyn Fn() -> i64 + Send + Sync),
) -> CompactionResult<CompactionPlan>
where
    M: ManifestSource,
    F: FileBytes,
{
    let base_snapshot = metadata.current_snapshot();
    let Some(snapshot) = base_snapshot else {
        // No snapshot: nothing to compact. Idempotent no-op.
        return Ok(CompactionPlan::noop(None));
    };
    let base_snapshot_id = snapshot.snapshot_id;

    let schema = metadata
        .schema_by_id(snapshot.schema_id.unwrap_or(metadata.current_schema_id))
        .or_else(|| metadata.current_schema())
        .ok_or_else(|| CompactionError::Unsupported("table has no resolvable schema".to_owned()))?
        .clone();

    let manifest_list_location = snapshot.manifest_list.as_deref().ok_or_else(|| {
        CompactionError::Unsupported(
            "current snapshot has no manifest-list location (v1 inline manifests are not \
             supported for compaction)"
                .to_owned(),
        )
    })?;

    let selection = read_selection(manifests, manifest_list_location).await?;
    let groups = bin_pack(
        &selection,
        options.target_file_size_bytes,
        options.min_input_files,
    );

    if groups.is_empty() {
        // Already compact (or every partition below the threshold): no-op.
        return Ok(CompactionPlan::noop(Some(base_snapshot_id)));
    }

    if options.dry_run {
        return Ok(dry_run_plan(&groups, base_snapshot_id));
    }

    // Rewrite every group (reads inputs, applies deletes, writes output
    // Parquet), asserting row conservation as it goes.
    let RewriteBatch {
        added,
        new_files,
        all_rewritten,
        records_before,
        records_after,
        bytes_before,
        bytes_after,
    } = execute_rewrites(files, writer, metadata, &schema, &selection, &groups).await?;

    let new_snapshot_id = allocate_snapshot_id(new_ids, metadata);
    let new_sequence_number = metadata.last_sequence_number.unwrap_or(0) + 1;
    let timestamp_ms = snapshot.timestamp_ms.max(now_ms());

    // Manifest namer: unique metadata-dir paths for the new manifests.
    let manifest_seq = AtomicU64::new(0);
    let table_location = metadata.location.clone();
    let mut namer = || {
        let n = manifest_seq.fetch_add(1, Ordering::Relaxed);
        format!(
            "{}/metadata/{}-m{n}.avro",
            table_location.trim_end_matches('/'),
            Uuid::new_v4()
        )
    };
    let manifest_list_location = format!(
        "{}/metadata/snap-{new_snapshot_id}-1-{}.avro",
        metadata.location.trim_end_matches('/'),
        Uuid::new_v4()
    );

    let result = RewriteResult {
        added: &added,
        rewritten: &all_rewritten,
        all_live_data: &live_data(&selection),
        all_deletes: &selection.deletes,
    };

    let commit = build_rewrite_commit(
        metadata,
        &schema,
        &result,
        new_snapshot_id,
        new_sequence_number,
        &mut namer,
        manifest_list_location,
        timestamp_ms,
    )?;

    write_commit_manifests(writer, &commit).await?;

    let stats = CompactionStats {
        files_before: all_rewritten.len() as u64,
        files_after: added.len() as u64,
        bytes_before,
        bytes_after,
        records_before,
        records_after,
        delete_files_removed: u64::try_from(
            selection
                .deletes
                .len()
                .saturating_sub(carried_delete_count(&result)),
        )
        .unwrap_or(u64::MAX),
    };

    Ok(CompactionPlan {
        updates: commit.updates,
        requirements: commit.requirements,
        new_files_written: new_files,
        stats,
        base_snapshot_id: Some(base_snapshot_id),
        new_snapshot_id: Some(commit.new_snapshot_id),
    })
}

/// Accumulated state from rewriting every bin-pack group.
struct RewriteBatch {
    added: Vec<AddedFile>,
    new_files: Vec<NewFile>,
    all_rewritten: Vec<InputFile>,
    records_before: i64,
    records_after: i64,
    bytes_before: u64,
    bytes_after: u64,
}

/// Rewrites each group into output Parquet written to storage, asserting the
/// row-conservation invariants (see the inline comment) per group.
async fn execute_rewrites<F: FileBytes>(
    files: &F,
    writer: &dyn Storage,
    metadata: &TableMetadata,
    schema: &Schema,
    selection: &Selection,
    groups: &[BinPackGroup],
) -> CompactionResult<RewriteBatch> {
    let compression = Compression::ZSTD(parquet::basic::ZstdLevel::default());
    let mut out = RewriteBatch {
        added: Vec::new(),
        new_files: Vec::new(),
        all_rewritten: Vec::new(),
        records_before: 0,
        records_after: 0,
        bytes_before: 0,
        bytes_after: 0,
    };
    // Output-file naming: a monotonic counter under the table's data dir keeps
    // names unique and ordered within a run.
    let output_seq = AtomicU64::new(0);

    for group in groups {
        let outcome = rewrite_group(
            files,
            schema,
            &group.inputs,
            &selection.deletes,
            compression,
        )
        .await?;
        assert_row_conservation(group, &outcome)?;

        out.records_before += outcome.input_records;
        out.records_after += outcome.output_records;
        out.bytes_before += group.input_bytes();

        let spec_id = group.inputs.first().map_or(0, |f| f.spec_id);
        let partition = group_partition(group);

        for output in &outcome.outputs {
            let location = output_location(&metadata.location, &output_seq);
            writer
                .write(&location, output.bytes.clone())
                .await
                .map_err(CompactionError::Storage)?;
            let size = i64::try_from(output.bytes.len()).unwrap_or(i64::MAX);
            out.bytes_after += output.bytes.len() as u64;
            let data_file = output_data_file(output, location, partition.clone(), size);
            out.added.push(AddedFile {
                file: data_file.clone(),
                spec_id,
            });
            out.new_files.push(NewFile {
                data_file,
                written: true,
            });
        }

        // A group can, in principle, delete every row (all rows tombstoned).
        // It then produced no output file, but the inputs are still removed —
        // a valid replace (files_after may be < number of groups).
        out.all_rewritten.extend(group.inputs.iter().cloned());
    }
    Ok(out)
}

/// The central correctness bar, asserted per group:
///
/// 1. Internal consistency, always: every input row is accounted for as
///    either carried to the output or removed by a delete —
///    `input == output + deleted`. Catches a dropped batch, a bad concat, or
///    an off-by-one in delete application.
/// 2. Strong invariant for delete-free groups (the pure bin-pack case, the
///    overwhelming majority): output == input exactly. Merge-on-read groups
///    shrink by exactly the applied deletes, and *deleted rows are absent* is
///    verified end-to-end by reading the output back in the test suite.
fn assert_row_conservation(
    group: &BinPackGroup,
    outcome: &crate::rewrite::RewriteOutcome,
) -> CompactionResult<()> {
    if outcome.input_records != outcome.output_records + outcome.rows_deleted {
        return Err(CompactionError::RowCountMismatch {
            group: group.label(),
            expected: outcome.input_records - outcome.rows_deleted,
            produced: outcome.output_records,
        });
    }
    if !outcome.had_deletes && outcome.output_records != outcome.input_records {
        return Err(CompactionError::RowCountMismatch {
            group: group.label(),
            expected: outcome.input_records,
            produced: outcome.output_records,
        });
    }
    Ok(())
}

/// Writes a rewrite commit's staged manifests and manifest list to storage.
/// The bytes are immutable and uniquely named, so plain `write` is safe (a
/// failed run leaves orphans the sweep collects — never a partial commit,
/// since nothing is referenced until the pointer swaps).
async fn write_commit_manifests(
    writer: &dyn Storage,
    commit: &crate::metadata_result::RewriteCommit,
) -> CompactionResult<()> {
    for manifest in &commit.manifests {
        writer
            .write(&manifest.location, Bytes::from(manifest.bytes.clone()))
            .await
            .map_err(CompactionError::Storage)?;
    }
    writer
        .write(
            &commit.manifest_list.location,
            Bytes::from(commit.manifest_list.bytes.clone()),
        )
        .await
        .map_err(CompactionError::Storage)?;
    Ok(())
}

/// The flat list of all live data files (from the partition grouping).
fn live_data(selection: &Selection) -> Vec<InputFile> {
    selection
        .by_partition
        .values()
        .flat_map(|v| v.iter().cloned())
        .collect()
}

/// How many delete files would be carried forward (to compute how many are
/// dropped for the ledger).
fn carried_delete_count(result: &RewriteResult<'_>) -> usize {
    crate::metadata_result::count_carried_deletes(result)
}

/// The partition tuple shared by a group's inputs (taken from the first
/// input — all inputs of a group share a partition by construction).
fn group_partition(group: &BinPackGroup) -> PartitionTuple {
    group
        .inputs
        .first()
        .map(|f| f.file.partition.clone())
        .unwrap_or_default()
}

/// A dry-run plan: the files that would be written, sized/counted from the
/// inputs, with no updates (nothing staged to commit).
fn dry_run_plan(groups: &[BinPackGroup], base_snapshot_id: i64) -> CompactionPlan {
    let mut new_files = Vec::new();
    let mut files_before = 0u64;
    let mut bytes_before = 0u64;
    let mut records_before = 0i64;
    for group in groups {
        files_before += group.inputs.len() as u64;
        bytes_before += group.input_bytes();
        records_before += group.input_records();
        // The projected output: one file per group, at (approximately) the
        // combined input size and record count (deletes would reduce records,
        // but dry-run does not read them, so this is an upper bound clearly
        // labelled as unwritten).
        let partition = group_partition(group);
        let projected = DataFile {
            content: DataFileContent::Data,
            file_path: format!("<dry-run: {}>", group.label()),
            file_format: "PARQUET".to_owned(),
            partition,
            record_count: group.input_records(),
            file_size_in_bytes: i64::try_from(group.input_bytes()).unwrap_or(i64::MAX),
            column_sizes: None,
            value_counts: None,
            null_value_counts: None,
            nan_value_counts: None,
            lower_bounds: None,
            upper_bounds: None,
            key_metadata: None,
            split_offsets: None,
            equality_ids: None,
            sort_order_id: None,
            first_row_id: None,
            referenced_data_file: None,
            content_offset: None,
            content_size_in_bytes: None,
        };
        new_files.push(NewFile {
            data_file: projected,
            written: false,
        });
    }
    CompactionPlan {
        updates: Vec::new(),
        requirements: Vec::new(),
        stats: CompactionStats {
            files_before,
            files_after: new_files.len() as u64,
            bytes_before,
            bytes_after: 0,
            records_before,
            records_after: records_before,
            delete_files_removed: 0,
        },
        new_files_written: new_files,
        base_snapshot_id: Some(base_snapshot_id),
        new_snapshot_id: None,
    }
}

/// Location for a new output data file under the table's `data/` dir.
fn output_location(table_location: &str, seq: &AtomicU64) -> String {
    let n = seq.fetch_add(1, Ordering::Relaxed);
    format!(
        "{}/data/compacted-{}-{n}.parquet",
        table_location.trim_end_matches('/'),
        Uuid::new_v4()
    )
}

/// Allocates a fresh snapshot id that does not collide with an existing one.
fn allocate_snapshot_id(
    new_ids: &(dyn Fn() -> i64 + Send + Sync),
    metadata: &TableMetadata,
) -> i64 {
    let mut id = new_ids();
    // Extremely unlikely, but never hand back a colliding id.
    while metadata.snapshot_by_id(id).is_some() {
        id = new_ids();
    }
    id
}

/// Wall-clock millis; used as the new snapshot timestamp (kept monotonic
/// against the base snapshot by the caller via `.max`).
fn now_ms() -> i64 {
    use std::time::{SystemTime, UNIX_EPOCH};
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map_or(0, |d| i64::try_from(d.as_millis()).unwrap_or(i64::MAX))
}
