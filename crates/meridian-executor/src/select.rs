//! Reading the current snapshot and choosing what to rewrite.
//!
//! Step 1 of compaction: resolve the current snapshot's *live* files (the
//! same live-entry rules the scan planner uses — `DELETED`-status entries
//! skipped, sequence numbers inherited from the manifest-list entry), group
//! the data files by partition, attach any pending position/equality deletes
//! to each data file by the spec's scope rules, then bin-pack the small data
//! files within each partition into groups that approach the target size.
//!
//! The delete-attachment logic is the compaction-side counterpart of
//! `meridian-server`'s `planning::engine`: a data file that carries pending
//! deletes must have them materialized during rewrite (fewer output rows,
//! and the delete files dropped), so we must know exactly which deletes apply
//! to each input.

use std::collections::BTreeMap;

use meridian_iceberg::manifest::{
    DataFile, DataFileContent, ManifestContentType, ManifestEntryStatus, ManifestList,
    PartitionTuple,
};

use crate::error::CompactionResult;
use crate::manifest_source::ManifestSource;

/// A live data file selected as a compaction input, with everything needed to
/// rewrite it and to later mark it DELETED.
#[derive(Debug, Clone)]
pub struct InputFile {
    /// The data file itself (path, size, record count, partition, stats).
    pub file: DataFile,
    /// Partition spec id from the manifest-list entry that carried it.
    pub spec_id: i32,
    /// Data sequence number (inherited), needed for delete-scope decisions
    /// and to preserve ordering.
    pub sequence_number: i64,
    /// Snapshot id that added this file (inherited), carried into the DELETE
    /// entry for provenance.
    pub added_snapshot_id: i64,
    /// Indices into [`Selection::deletes`] of the delete files that apply to
    /// this data file, ascending. Empty on v1 / copy-on-write tables.
    pub delete_indices: Vec<usize>,
}

/// A delete file live in the input snapshot (position or equality).
#[derive(Debug, Clone)]
pub struct DeleteFile {
    /// The delete file.
    pub file: DataFile,
    /// Partition spec id from its manifest-list entry.
    pub spec_id: i32,
    /// Inherited data sequence number.
    pub sequence_number: i64,
}

/// Everything read out of the current snapshot: the live data files grouped
/// by partition identity, and the flat list of live delete files they
/// reference into.
#[derive(Debug, Default)]
pub struct Selection {
    /// Live data files, keyed by a canonical partition identity (spec id +
    /// field-id-keyed Appendix-D value bytes), each value in manifest order.
    pub by_partition: BTreeMap<Vec<u8>, Vec<InputFile>>,
    /// Live delete files, indexed by [`InputFile::delete_indices`].
    pub deletes: Vec<DeleteFile>,
    /// Total count of live data files seen (compact or not) — the health
    /// denominator.
    pub live_data_files: u64,
}

/// A canonical, hashable partition identity: spec id plus the tuple's field
/// ids and Appendix-D value bytes. Mirrors the scan planner's `partition_key`
/// so the two agree on what "same partition" means. Two partitions are equal
/// exactly when spec ids and every field value match.
#[must_use]
pub fn partition_key(spec_id: i32, tuple: &PartitionTuple) -> Vec<u8> {
    let mut key = spec_id.to_le_bytes().to_vec();
    // Field order can differ between writers; sort by field id so the key is
    // order-independent.
    let mut fields: Vec<_> = tuple.fields.iter().collect();
    fields.sort_by_key(|f| f.field_id);
    for field in fields {
        key.extend_from_slice(&field.field_id.to_le_bytes());
        match &field.value {
            None => key.push(0),
            Some(datum) => {
                key.push(1);
                let bytes = datum.to_bound_bytes();
                key.extend_from_slice(&(bytes.len() as u64).to_le_bytes());
                key.extend_from_slice(&bytes);
            }
        }
    }
    key
}

/// Reads the current snapshot's manifest list + manifests and collects live
/// data and delete files, attaching deletes to each data file.
pub async fn read_selection<S: ManifestSource>(
    source: &S,
    manifest_list_location: &str,
) -> CompactionResult<Selection> {
    let list: std::sync::Arc<ManifestList> = source.manifest_list(manifest_list_location).await?;

    let mut data: Vec<InputFile> = Vec::new();
    let mut deletes: Vec<DeleteFile> = Vec::new();

    for manifest_ref in &list.manifests {
        // Manifests with no live entries hold nothing.
        if manifest_ref.added_files_count == Some(0) && manifest_ref.existing_files_count == Some(0)
        {
            continue;
        }
        let manifest = source.manifest(&manifest_ref.manifest_path).await?;
        for stored in &manifest.entries {
            if stored.status == ManifestEntryStatus::Deleted {
                continue;
            }
            let mut entry = stored.clone();
            entry.inherit_from(manifest_ref);
            let sequence_number = entry.sequence_number.unwrap_or(0);
            let added_snapshot_id = entry.snapshot_id.unwrap_or(manifest_ref.added_snapshot_id);

            let is_data = manifest.metadata.content == ManifestContentType::Data
                && entry.data_file.content == DataFileContent::Data;
            if is_data {
                data.push(InputFile {
                    file: entry.data_file,
                    spec_id: manifest_ref.partition_spec_id,
                    sequence_number,
                    added_snapshot_id,
                    delete_indices: Vec::new(),
                });
            } else {
                deletes.push(DeleteFile {
                    file: entry.data_file,
                    spec_id: manifest_ref.partition_spec_id,
                    sequence_number,
                });
            }
        }
    }

    let index = DeleteIndex::build(&deletes);
    let live_data_files = data.len() as u64;

    let mut by_partition: BTreeMap<Vec<u8>, Vec<InputFile>> = BTreeMap::new();
    for mut input in data {
        input.delete_indices =
            index.deletes_for(&deletes, &input.file, input.spec_id, input.sequence_number);
        let key = partition_key(input.spec_id, &input.file.partition);
        by_partition.entry(key).or_default().push(input);
    }

    Ok(Selection {
        by_partition,
        deletes,
        live_data_files,
    })
}

/// A bin-pack group: the small data files of one partition that will be
/// rewritten together into one output file.
#[derive(Debug)]
pub struct BinPackGroup {
    /// The input files, in the order they were packed.
    pub inputs: Vec<InputFile>,
    /// The partition identity these files share.
    pub partition_key: Vec<u8>,
}

impl BinPackGroup {
    /// Total on-disk bytes of the inputs.
    #[must_use]
    pub fn input_bytes(&self) -> u64 {
        self.inputs
            .iter()
            .map(|f| u64::try_from(f.file.file_size_in_bytes).unwrap_or(0))
            .sum()
    }

    /// Total input record count (before delete application).
    #[must_use]
    pub fn input_records(&self) -> i64 {
        self.inputs.iter().map(|f| f.file.record_count).sum()
    }

    /// Whether any input carries pending deletes (so the rewrite must apply
    /// them).
    #[must_use]
    pub fn has_deletes(&self) -> bool {
        self.inputs.iter().any(|f| !f.delete_indices.is_empty())
    }

    /// A short human identity for error/log messages.
    #[must_use]
    pub fn label(&self) -> String {
        format!(
            "partition {} ({} files)",
            hex_key(&self.partition_key),
            self.inputs.len()
        )
    }
}

fn hex_key(key: &[u8]) -> String {
    use std::fmt::Write as _;
    // A compact partition-key fingerprint for messages; not parsed back.
    let mut s = String::with_capacity(key.len().min(8) * 2);
    for b in key.iter().take(8) {
        let _ = write!(s, "{b:02x}");
    }
    if key.len() > 8 {
        s.push('…');
    }
    s
}

/// Bin-packs the selection into rewrite groups.
///
/// Within each partition: files whose size is `>= target` are already big
/// enough and are excluded. The remaining small files are packed greedily
/// (largest-first, first-fit) into groups whose combined size approaches
/// `target`. A group is emitted only if it has at least `min_input_files`
/// inputs, *or* any of its files carries pending deletes (deletes must be
/// materialized regardless of file count — a single file with an attached
/// delete is worth rewriting to shed the delete file).
#[must_use]
pub fn bin_pack(
    selection: &Selection,
    target_file_size_bytes: u64,
    min_input_files: usize,
) -> Vec<BinPackGroup> {
    let target = target_file_size_bytes.max(1);
    let mut groups: Vec<BinPackGroup> = Vec::new();

    for (key, files) in &selection.by_partition {
        // Candidates: small files (below target). Files already at/over the
        // target are left alone. A file carrying deletes is always a
        // candidate even if large — its rows must be rewritten to drop the
        // delete file.
        let mut candidates: Vec<&InputFile> = files
            .iter()
            .filter(|f| {
                let size = u64::try_from(f.file.file_size_in_bytes).unwrap_or(0);
                size < target || !f.delete_indices.is_empty()
            })
            .collect();

        // Nothing to do if too few small files and none carry deletes.
        let any_deletes = candidates.iter().any(|f| !f.delete_indices.is_empty());
        if candidates.len() < min_input_files && !any_deletes {
            continue;
        }

        // Largest-first, first-fit-decreasing bin packing.
        candidates.sort_by_key(|f| std::cmp::Reverse(f.file.file_size_in_bytes));

        let mut bins: Vec<(u64, Vec<InputFile>)> = Vec::new();
        for candidate in candidates {
            let size = u64::try_from(candidate.file.file_size_in_bytes).unwrap_or(0);
            // Find the first bin this fits into without exceeding target.
            let slot = bins
                .iter_mut()
                .find(|(bin_size, _)| bin_size.saturating_add(size) <= target);
            match slot {
                Some((bin_size, bin_files)) => {
                    *bin_size = bin_size.saturating_add(size);
                    bin_files.push(candidate.clone());
                }
                None => bins.push((size, vec![candidate.clone()])),
            }
        }

        for (_, inputs) in bins {
            // A single-file bin with no deletes is not worth rewriting (it is
            // just a copy). Emit it only if it merges >1 file or sheds
            // deletes.
            let group_has_deletes = inputs.iter().any(|f| !f.delete_indices.is_empty());
            if inputs.len() < 2 && !group_has_deletes {
                continue;
            }
            groups.push(BinPackGroup {
                inputs,
                partition_key: key.clone(),
            });
        }
    }

    groups
}

// ---------------------------------------------------------------------------
// Delete-file attachment (spec "Scan Planning" scope rules)
//
// This is the compaction-side mirror of meridian-server::planning::engine's
// DeleteIndex. It answers "which delete files apply to this data file" so the
// rewrite can materialize exactly those deletes. The rules are the Iceberg
// spec's; the two implementations must agree.
// ---------------------------------------------------------------------------

/// Groups delete candidates for near-constant-time attachment per data file.
struct DeleteIndex {
    /// Deletion vectors by `referenced_data_file`.
    dv_by_path: BTreeMap<String, Vec<usize>>,
    /// Plain position deletes with `referenced_data_file`, by that path.
    pos_by_path: BTreeMap<String, Vec<usize>>,
    /// Remaining position deletes by partition identity.
    pos_by_partition: BTreeMap<Vec<u8>, Vec<usize>>,
    /// Equality deletes written under an unpartitioned spec (global).
    eq_global: Vec<usize>,
    /// Equality deletes by partition identity.
    eq_by_partition: BTreeMap<Vec<u8>, Vec<usize>>,
}

impl DeleteIndex {
    fn build(deletes: &[DeleteFile]) -> Self {
        let mut index = Self {
            dv_by_path: BTreeMap::new(),
            pos_by_path: BTreeMap::new(),
            pos_by_partition: BTreeMap::new(),
            eq_global: Vec::new(),
            eq_by_partition: BTreeMap::new(),
        };
        for (i, delete) in deletes.iter().enumerate() {
            match delete.file.content {
                DataFileContent::PositionDeletes => {
                    let is_dv = delete.file.content_offset.is_some();
                    match (&delete.file.referenced_data_file, is_dv) {
                        (Some(path), true) => {
                            index.dv_by_path.entry(path.clone()).or_default().push(i);
                        }
                        (Some(path), false) => {
                            index.pos_by_path.entry(path.clone()).or_default().push(i);
                        }
                        (None, _) => {
                            index
                                .pos_by_partition
                                .entry(partition_key(delete.spec_id, &delete.file.partition))
                                .or_default()
                                .push(i);
                        }
                    }
                }
                DataFileContent::EqualityDeletes => {
                    if delete.file.partition.fields.is_empty() {
                        index.eq_global.push(i);
                    } else {
                        index
                            .eq_by_partition
                            .entry(partition_key(delete.spec_id, &delete.file.partition))
                            .or_default()
                            .push(i);
                    }
                }
                DataFileContent::Data => {}
            }
        }
        index
    }

    /// The ascending indices of the deletes that apply to one data file. Same
    /// scope rules as the scan planner (DV supersedes plain position deletes
    /// for the same data file; position deletes need seq `<=` and partition
    /// equality; equality deletes need seq `<` (strict) and partition
    /// equality or an unpartitioned/global delete).
    fn deletes_for(
        &self,
        deletes: &[DeleteFile],
        file: &DataFile,
        spec_id: i32,
        data_sequence: i64,
    ) -> Vec<usize> {
        let key = partition_key(spec_id, &file.partition);
        let partitions_equal = |i: usize| {
            let d = &deletes[i];
            d.spec_id == spec_id && partition_key(d.spec_id, &d.file.partition) == key
        };

        let mut attached: Vec<usize> = Vec::new();

        let dvs: Vec<usize> = self
            .dv_by_path
            .get(&file.file_path)
            .into_iter()
            .flatten()
            .copied()
            .filter(|&i| data_sequence <= deletes[i].sequence_number && partitions_equal(i))
            .collect();
        let has_dv = !dvs.is_empty();
        attached.extend(dvs);

        if !has_dv {
            attached.extend(
                self.pos_by_path
                    .get(&file.file_path)
                    .into_iter()
                    .flatten()
                    .copied()
                    .filter(|&i| {
                        data_sequence <= deletes[i].sequence_number && partitions_equal(i)
                    }),
            );
            attached.extend(
                self.pos_by_partition
                    .get(&key)
                    .into_iter()
                    .flatten()
                    .copied()
                    .filter(|&i| data_sequence <= deletes[i].sequence_number),
            );
        }

        attached.extend(
            self.eq_global
                .iter()
                .chain(self.eq_by_partition.get(&key).into_iter().flatten())
                .copied()
                .filter(|&i| data_sequence < deletes[i].sequence_number),
        );

        attached.sort_unstable();
        attached.dedup();
        attached
    }
}
