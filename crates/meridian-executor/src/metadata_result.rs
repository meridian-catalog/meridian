//! Turning a rewrite into an Iceberg `RewriteFiles` (replace) commit: the new
//! snapshot's manifests + manifest list, and the `TableUpdate` /
//! `TableRequirement` lists that the commit path applies.
//!
//! The shape follows the Iceberg spec's replace operation:
//!
//! - A new **data manifest** holding, in one file:
//!   - `ADDED` entries for the compacted output files,
//!   - `DELETED` entries for the input files they replace,
//!   - `EXISTING` entries carrying forward every other live data file.
//! - Zero or more **delete manifests** carrying forward the live delete files
//!   that were *not* fully consumed (a delete file is dropped only when every
//!   live data file it applied to was rewritten; otherwise it stays, marked
//!   `EXISTING`). Fully-consumed delete files are simply not carried forward —
//!   the replace snapshot no longer references them.
//! - The snapshot `summary` reports `added-data-files`, `deleted-data-files`,
//!   `added-records`, `deleted-records`, and (when applicable) the removed
//!   delete files, with `operation = replace`.
//!
//! Sequence numbers: the new snapshot takes `last-sequence-number + 1`; ADDED
//! entries inherit it; EXISTING and DELETED entries keep their original
//! explicit sequence numbers (the spec forbids inventing new ones for carried
//! files). Because the compacted output carries the *new, higher* sequence
//! number, any equality delete carried forward correctly does **not** apply to
//! it — its rows were already materialized out during the rewrite.

use std::collections::BTreeMap;
use std::collections::BTreeSet;

use meridian_iceberg::manifest::{
    DataFile, ManifestContentType, ManifestEntry, ManifestEntryStatus, ManifestFile,
    ManifestListWriteParams, ManifestWriteParams, PartitionFieldType, partition_field_types,
    write_manifest, write_manifest_list,
};
use meridian_iceberg::spec::{
    PartitionField, RefType, Schema, Snapshot, SnapshotRef, TableMetadata, TableRequirement,
    TableUpdate,
};

use crate::error::{CompactionError, CompactionResult};
use crate::select::{DeleteFile, InputFile};

/// A manifest the plan will write to storage (path + Avro bytes).
#[derive(Debug, Clone)]
pub struct StagedManifest {
    /// Object-storage location for the manifest.
    pub location: String,
    /// Avro bytes.
    pub bytes: Vec<u8>,
    /// Length in bytes (== `bytes.len()`, cached for the list entry).
    pub length: i64,
}

/// Everything the commit needs, before the manifests are actually written.
#[derive(Debug)]
pub struct RewriteCommit {
    /// The updates to apply (add-snapshot + set main ref).
    pub updates: Vec<TableUpdate>,
    /// The requirements to assert (uuid + main unchanged).
    pub requirements: Vec<TableRequirement>,
    /// Manifests (data + carried delete manifests) to write to storage.
    pub manifests: Vec<StagedManifest>,
    /// The manifest list to write to storage.
    pub manifest_list: StagedManifest,
    /// The new snapshot id.
    pub new_snapshot_id: i64,
}

/// Inputs describing what the rewrite produced, used to build the commit.
#[derive(Debug)]
pub struct RewriteResult<'a> {
    /// The output data files (already written to storage), each with its spec
    /// id and the partition tuple they belong to.
    pub added: &'a [AddedFile],
    /// The input data files being replaced (marked DELETED).
    pub rewritten: &'a [InputFile],
    /// Every live data file in the base snapshot (used to carry forward the
    /// ones not rewritten as EXISTING, and to decide which deletes to drop).
    pub all_live_data: &'a [InputFile],
    /// Every live delete file in the base snapshot.
    pub all_deletes: &'a [DeleteFile],
}

/// One compacted output file with the placement info the manifest needs.
#[derive(Debug, Clone)]
pub struct AddedFile {
    /// The data file (path, size, record count, stats).
    pub file: DataFile,
    /// Partition spec id it is written under (the base snapshot's default).
    pub spec_id: i32,
}

/// Deterministic identity of a data file for set membership: its path.
fn file_key(file: &DataFile) -> &str {
    &file.file_path
}

/// Builds the `RewriteFiles` commit (metadata + staged manifest bytes) for a
/// completed rewrite. Does not write anything — the caller stages the bytes.
///
/// `metadata_dir` is the `<table>/metadata` prefix new manifests are written
/// under; `new_snapshot_id` and `new_sequence_number` are freshly allocated;
/// `manifest_list_location` is the path the list will be written to.
#[allow(clippy::too_many_arguments)]
pub fn build_rewrite_commit(
    base: &TableMetadata,
    schema: &Schema,
    result: &RewriteResult<'_>,
    new_snapshot_id: i64,
    new_sequence_number: i64,
    manifest_namer: &mut dyn FnMut() -> String,
    manifest_list_location: String,
    timestamp_ms: i64,
) -> CompactionResult<RewriteCommit> {
    let default_spec_id = base.default_spec_id;
    let spec = base.partition_spec_by_id(default_spec_id).ok_or_else(|| {
        CompactionError::Unsupported(format!(
            "default partition spec {default_spec_id} not found"
        ))
    })?;
    let partition_fields: Vec<PartitionField> = spec.fields.clone();
    let partition_types: Vec<PartitionFieldType> =
        partition_field_types(&partition_fields, schema).map_err(CompactionError::Manifest)?;
    let schema_json = serde_json::to_string(schema)
        .map_err(|e| CompactionError::Unsupported(format!("schema not serializable: {e}")))?;
    let schema_id = schema.schema_id;

    // The data manifest's entries: ADDED outputs + DELETED inputs + EXISTING
    // carry-forwards.
    let data_entries = build_data_entries(result, new_snapshot_id);

    // Decide which delete files to carry forward. A delete file is dropped
    // (not carried) iff every live data file it applied to was rewritten.
    let carried_deletes = deletes_to_carry(result);
    let dropped_delete_count = u64::try_from(
        result
            .all_deletes
            .len()
            .saturating_sub(carried_deletes.len()),
    )
    .unwrap_or(u64::MAX);

    let params = ManifestBuildParams {
        format_version: base.format_version.min(2), // writer supports v1/v2
        schema_json: &schema_json,
        schema_id,
        default_spec_id,
        partition_fields: &partition_fields,
        partition_types: &partition_types,
        new_snapshot_id,
        new_sequence_number,
    };

    // Write the data manifest.
    let mut manifests: Vec<StagedManifest> = Vec::new();
    let (data_manifest_file, data_manifest) = build_content_manifest(
        ManifestContentType::Data,
        &data_entries,
        &params,
        manifest_namer(),
    )?;
    manifests.push(data_manifest);

    // Carry-forward delete manifest (v2+ only; v1 tables have no deletes).
    let mut manifest_files: Vec<ManifestFile> = vec![data_manifest_file];
    if !carried_deletes.is_empty() {
        if base.format_version < 2 {
            return Err(CompactionError::Unsupported(
                "v1 table unexpectedly has delete files to carry".to_owned(),
            ));
        }
        let delete_entries: Vec<ManifestEntry> = carried_deletes
            .iter()
            .map(|d| ManifestEntry {
                status: ManifestEntryStatus::Existing,
                snapshot_id: Some(new_snapshot_id),
                sequence_number: Some(d.sequence_number),
                file_sequence_number: Some(d.sequence_number),
                data_file: d.file.clone(),
            })
            .collect();
        let (delete_file, delete_manifest) = build_content_manifest(
            ManifestContentType::Deletes,
            &delete_entries,
            &params,
            manifest_namer(),
        )?;
        manifest_files.push(delete_file);
        manifests.push(delete_manifest);
    }

    // ---- Manifest list ----------------------------------------------------
    let format_version = base.format_version.min(2);
    let list_bytes = write_manifest_list(&ManifestListWriteParams {
        format_version,
        snapshot_id: new_snapshot_id,
        parent_snapshot_id: base.current_snapshot_id.filter(|id| *id >= 0),
        sequence_number: (format_version >= 2).then_some(new_sequence_number),
        manifests: &manifest_files,
    })
    .map_err(CompactionError::Manifest)?;
    let manifest_list = StagedManifest {
        length: i64::try_from(list_bytes.len()).unwrap_or(i64::MAX),
        bytes: list_bytes,
        location: manifest_list_location.clone(),
    };

    // ---- Snapshot + updates + requirements --------------------------------
    let snapshot = build_snapshot(
        base,
        schema,
        result,
        new_snapshot_id,
        (format_version >= 2).then_some(new_sequence_number),
        manifest_list_location,
        timestamp_ms,
        dropped_delete_count,
    );
    let (updates, requirements) = updates_and_requirements(base, snapshot, new_snapshot_id);

    Ok(RewriteCommit {
        updates,
        requirements,
        manifests,
        manifest_list,
        new_snapshot_id,
    })
}

/// The replace snapshot for the rewrite (operation `replace`, with the
/// add/delete counts in its summary).
#[allow(clippy::too_many_arguments)]
fn build_snapshot(
    base: &TableMetadata,
    schema: &Schema,
    result: &RewriteResult<'_>,
    new_snapshot_id: i64,
    sequence_number: Option<i64>,
    manifest_list_location: String,
    timestamp_ms: i64,
    dropped_delete_count: u64,
) -> Snapshot {
    let added_records: i64 = result.added.iter().map(|f| f.file.record_count).sum();
    let deleted_records: i64 = result.rewritten.iter().map(|f| f.file.record_count).sum();
    let summary = rewrite_summary(
        result.added.len(),
        result.rewritten.len(),
        added_records,
        deleted_records,
        dropped_delete_count,
    );
    Snapshot {
        snapshot_id: new_snapshot_id,
        parent_snapshot_id: base.current_snapshot_id.filter(|id| *id >= 0),
        sequence_number,
        timestamp_ms,
        manifest_list: Some(manifest_list_location),
        summary: Some(summary),
        schema_id: schema.schema_id,
        first_row_id: None,
        added_rows: None,
        extra: serde_json::Map::new(),
    }
}

/// The `add-snapshot` + `set main` updates and the `assert-uuid` +
/// `assert-ref` requirements. The requirements make the commit optimistic: a
/// racing writer that moves `main` after planning makes this commit fail
/// cleanly at the CAS, never corrupt.
fn updates_and_requirements(
    base: &TableMetadata,
    snapshot: Snapshot,
    new_snapshot_id: i64,
) -> (Vec<TableUpdate>, Vec<TableRequirement>) {
    let updates = vec![
        TableUpdate::AddSnapshot { snapshot },
        TableUpdate::SetSnapshotRef {
            ref_name: "main".to_owned(),
            reference: SnapshotRef {
                snapshot_id: new_snapshot_id,
                ref_type: RefType::Branch,
                min_snapshots_to_keep: None,
                max_snapshot_age_ms: None,
                max_ref_age_ms: None,
                extra: serde_json::Map::new(),
            },
        },
    ];
    let requirements = vec![
        TableRequirement::AssertTableUuid {
            uuid: base.table_uuid,
        },
        TableRequirement::AssertRefSnapshotId {
            r#ref: "main".to_owned(),
            snapshot_id: base.current_snapshot_id.filter(|id| *id >= 0),
        },
    ];
    (updates, requirements)
}

/// Shared inputs for writing the new snapshot's manifests.
struct ManifestBuildParams<'a> {
    format_version: u8,
    schema_json: &'a str,
    schema_id: Option<i32>,
    default_spec_id: i32,
    partition_fields: &'a [PartitionField],
    partition_types: &'a [PartitionFieldType],
    new_snapshot_id: i64,
    new_sequence_number: i64,
}

/// Writes one content manifest (data or deletes) and returns both its
/// manifest-list entry and the staged bytes.
fn build_content_manifest(
    content: ManifestContentType,
    entries: &[ManifestEntry],
    params: &ManifestBuildParams<'_>,
    location: String,
) -> CompactionResult<(ManifestFile, StagedManifest)> {
    let bytes = write_manifest(&ManifestWriteParams {
        format_version: params.format_version,
        content,
        schema_json: params.schema_json,
        schema_id: params.schema_id,
        partition_spec_id: params.default_spec_id,
        partition_fields: params.partition_fields,
        partition_types: params.partition_types,
        entries,
    })
    .map_err(CompactionError::Manifest)?;
    let manifest_file = manifest_file_of(
        &location,
        &bytes,
        params.default_spec_id,
        content,
        params.new_snapshot_id,
        params.new_sequence_number,
        entries,
    );
    let staged = StagedManifest {
        length: i64::try_from(bytes.len()).unwrap_or(i64::MAX),
        bytes,
        location,
    };
    Ok((manifest_file, staged))
}

/// Builds the data manifest's entries for the replace snapshot: `ADDED` for
/// each compacted output, `DELETED` for each rewritten input, and `EXISTING`
/// for every live data file that was not rewritten (carried forward). `ADDED`
/// entries inherit the snapshot sequence number (left `None`); `DELETED` and
/// `EXISTING` keep their original explicit sequence numbers (the spec forbids
/// inventing new ones for carried files).
fn build_data_entries(result: &RewriteResult<'_>, new_snapshot_id: i64) -> Vec<ManifestEntry> {
    let rewritten_paths: BTreeSet<&str> =
        result.rewritten.iter().map(|f| file_key(&f.file)).collect();
    let mut entries: Vec<ManifestEntry> =
        Vec::with_capacity(result.added.len() + result.all_live_data.len());

    for added in result.added {
        entries.push(ManifestEntry {
            status: ManifestEntryStatus::Added,
            snapshot_id: Some(new_snapshot_id),
            sequence_number: None,
            file_sequence_number: None,
            data_file: added.file.clone(),
        });
    }
    for input in result.rewritten {
        entries.push(ManifestEntry {
            status: ManifestEntryStatus::Deleted,
            snapshot_id: Some(new_snapshot_id),
            sequence_number: Some(input.sequence_number),
            file_sequence_number: Some(input.sequence_number),
            data_file: input.file.clone(),
        });
    }
    for live in result.all_live_data {
        if rewritten_paths.contains(file_key(&live.file)) {
            continue;
        }
        entries.push(ManifestEntry {
            status: ManifestEntryStatus::Existing,
            snapshot_id: Some(live.added_snapshot_id),
            sequence_number: Some(live.sequence_number),
            file_sequence_number: Some(live.sequence_number),
            data_file: live.file.clone(),
        });
    }
    entries
}

/// How many delete files [`build_rewrite_commit`] would carry forward for the
/// given rewrite (the rest are dropped). Exposed for the savings ledger.
#[must_use]
pub fn count_carried_deletes(result: &RewriteResult<'_>) -> usize {
    deletes_to_carry(result).len()
}

/// The live delete files to carry forward into the replace snapshot: those
/// still referenced by at least one data file that was NOT rewritten.
///
/// A delete file whose every attaching live data file was rewritten is fully
/// consumed (its effect is now materialized in the compacted output) and is
/// dropped. A delete attached to a surviving data file must stay.
fn deletes_to_carry<'a>(result: &RewriteResult<'a>) -> Vec<&'a DeleteFile> {
    let rewritten_paths: BTreeSet<&str> =
        result.rewritten.iter().map(|f| file_key(&f.file)).collect();

    // For each delete index, is it still needed by a surviving data file?
    let mut needed: BTreeSet<usize> = BTreeSet::new();
    for live in result.all_live_data {
        if rewritten_paths.contains(file_key(&live.file)) {
            continue; // this data file is gone; its delete attachments don't keep deletes alive
        }
        for &di in &live.delete_indices {
            needed.insert(di);
        }
    }

    needed
        .into_iter()
        .filter_map(|i| result.all_deletes.get(i))
        .collect()
}

/// Assembles a `ManifestFile` list entry from written manifest bytes and its
/// entries (computing the ADDED/EXISTING/DELETED counts and row totals, and
/// the min live sequence number).
fn manifest_file_of(
    location: &str,
    bytes: &[u8],
    spec_id: i32,
    content: ManifestContentType,
    added_snapshot_id: i64,
    snapshot_sequence_number: i64,
    entries: &[ManifestEntry],
) -> ManifestFile {
    let mut added_files = 0i32;
    let mut existing_files = 0i32;
    let mut deleted_files = 0i32;
    let mut added_rows = 0i64;
    let mut existing_rows = 0i64;
    let mut deleted_rows = 0i64;
    let mut min_seq = i64::MAX;

    for entry in entries {
        let rows = entry.data_file.record_count;
        // ADDED entries inherit the snapshot seq; others carry explicit ones.
        let seq = entry.sequence_number.unwrap_or(snapshot_sequence_number);
        match entry.status {
            ManifestEntryStatus::Added => {
                added_files += 1;
                added_rows += rows;
                min_seq = min_seq.min(seq);
            }
            ManifestEntryStatus::Existing => {
                existing_files += 1;
                existing_rows += rows;
                min_seq = min_seq.min(seq);
            }
            ManifestEntryStatus::Deleted => {
                deleted_files += 1;
                deleted_rows += rows;
                // DELETED entries do not count toward min live sequence.
            }
        }
    }
    if min_seq == i64::MAX {
        // No live entries: use the snapshot's own sequence number.
        min_seq = snapshot_sequence_number;
    }

    ManifestFile {
        manifest_path: location.to_owned(),
        manifest_length: i64::try_from(bytes.len()).unwrap_or(i64::MAX),
        partition_spec_id: spec_id,
        content,
        sequence_number: snapshot_sequence_number,
        min_sequence_number: min_seq,
        added_snapshot_id,
        added_files_count: Some(added_files),
        existing_files_count: Some(existing_files),
        deleted_files_count: Some(deleted_files),
        added_rows_count: Some(added_rows),
        existing_rows_count: Some(existing_rows),
        deleted_rows_count: Some(deleted_rows),
        partitions: None,
        key_metadata: None,
        first_row_id: None,
    }
}

/// The snapshot summary for a replace operation.
fn rewrite_summary(
    added_files: usize,
    deleted_files: usize,
    added_records: i64,
    deleted_records: i64,
    removed_delete_files: u64,
) -> BTreeMap<String, String> {
    let mut summary = BTreeMap::new();
    summary.insert("operation".to_owned(), "replace".to_owned());
    summary.insert("added-data-files".to_owned(), added_files.to_string());
    summary.insert("deleted-data-files".to_owned(), deleted_files.to_string());
    summary.insert("added-records".to_owned(), added_records.to_string());
    summary.insert("deleted-records".to_owned(), deleted_records.to_string());
    if removed_delete_files > 0 {
        summary.insert(
            "removed-delete-files".to_owned(),
            removed_delete_files.to_string(),
        );
    }
    summary.insert("engine-name".to_owned(), "meridian".to_owned());
    summary.insert(
        "meridian-operation".to_owned(),
        "compaction.bin-pack".to_owned(),
    );
    summary
}
