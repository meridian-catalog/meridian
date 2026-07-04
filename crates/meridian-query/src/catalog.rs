//! Describing the tables a query may read, and resolving each one's current
//! snapshot into a scan plan (live data files + the deletes that apply to
//! them) plus a pre-execution size estimate.
//!
//! A [`CatalogTable`] is the caller's contract: "register this table, under
//! this name, from this metadata, reading bytes through this storage handle".
//! The caller (a server route) has already loaded the table's `TableMetadata`
//! and opened the warehouse `Storage`; this crate does not touch the catalog
//! database or resolve names — it reads what it is handed. That keeps the
//! executor a pure function of (metadata + bytes + policy + SQL) and trivially
//! testable against in-memory fixtures.
//!
//! Resolving a snapshot mirrors the scan planner's live-entry rules (the same
//! ones `meridian-executor` and `meridian-server::planning` use): walk the
//! current snapshot's manifest list, skip `DELETED` entries, inherit sequence
//! numbers, and split data files from delete files. Delete files are attached
//! to the data files they cover by the spec's scope rules so the reader can
//! materialize merge-on-read deletes — a governed query that returned
//! already-deleted rows would be both wrong and a policy leak.

use meridian_iceberg::manifest::{
    DataFile, DataFileContent, ManifestContentType, ManifestEntryStatus, ManifestList,
    PartitionTuple, read_manifest, read_manifest_list,
};
use meridian_iceberg::spec::{Schema, TableMetadata};
use meridian_storage::Storage;

use crate::error::{QueryError, QueryResult};

/// A table the query may reference, and everything needed to read it.
///
/// Held by reference so the caller keeps ownership of the (potentially large)
/// metadata and the shared storage handle. The `name` is what the SQL uses to
/// refer to the table; it need not match any Iceberg identifier — the caller
/// chooses it (typically the table's short name or a fully-qualified alias).
pub struct CatalogTable<'a> {
    /// The name the query references this table by.
    pub name: String,
    /// The table's metadata, whose current snapshot the executor reads.
    pub metadata: &'a TableMetadata,
    /// The warehouse storage handle to read manifests and data files through.
    pub storage: &'a dyn Storage,
}

impl std::fmt::Debug for CatalogTable<'_> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("CatalogTable")
            .field("name", &self.name)
            .field("table_uuid", &self.metadata.table_uuid)
            .finish_non_exhaustive()
    }
}

/// A live data file selected for a scan, with the deletes that apply to it.
#[derive(Debug, Clone)]
pub(crate) struct PlannedDataFile {
    /// The data file (path, size, record count, partition, stats).
    pub file: DataFile,
    /// Partition spec id from the manifest-list entry that carried it.
    pub spec_id: i32,
    /// Inherited data sequence number, for delete-scope decisions.
    pub sequence_number: i64,
    /// Indices into [`ScanPlan::deletes`] of the delete files that apply,
    /// ascending. Empty on copy-on-write / v1 tables.
    pub delete_indices: Vec<usize>,
}

/// A live delete file (position or equality) in the snapshot.
#[derive(Debug, Clone)]
pub(crate) struct PlannedDelete {
    /// The delete file.
    pub file: DataFile,
    /// Partition spec id.
    pub spec_id: i32,
    /// Inherited data sequence number.
    pub sequence_number: i64,
}

/// The resolved scan of one table's current snapshot: the live data files with
/// their attached deletes, the flat delete list, and the summed size estimate.
#[derive(Debug, Default)]
pub(crate) struct ScanPlan {
    /// Live data files with attached deletes, in manifest order.
    pub data_files: Vec<PlannedDataFile>,
    /// Live delete files, indexed by [`PlannedDataFile::delete_indices`].
    pub deletes: Vec<PlannedDelete>,
    /// Summed on-disk bytes of the live data files (the cost estimate).
    pub bytes: u64,
    /// Summed record counts of the live data files.
    pub rows: u64,
    /// The snapshot id read, or `None` for a table with no current snapshot.
    pub snapshot_id: Option<i64>,
}

/// Resolves a table's current snapshot into a scan plan.
///
/// An empty table (no current snapshot) yields an empty plan (zero files, zero
/// bytes) rather than an error — a valid query over it returns no rows. Missing
/// structural preconditions (a current snapshot with no manifest-list location,
/// an unresolvable current schema) are [`QueryError::UnqueryableTable`].
pub(crate) async fn resolve_scan(table: &CatalogTable<'_>) -> QueryResult<ScanPlan> {
    // The current schema must resolve — we need it to build the Arrow schema and
    // to map columns by field id.
    current_schema(table)?;

    let Some(snapshot) = table.metadata.current_snapshot() else {
        // No snapshots yet: an empty, queryable table.
        return Ok(ScanPlan::default());
    };
    let snapshot_id = snapshot.snapshot_id;

    let Some(list_location) = snapshot.manifest_list.as_deref() else {
        return Err(QueryError::UnqueryableTable {
            table: table.name.clone(),
            reason: format!(
                "current snapshot {snapshot_id} has no manifest-list location (inline v1 \
                 manifests are not supported by the small-scan executor)"
            ),
        });
    };

    let bytes = table.storage.read(list_location).await?;
    let list: ManifestList = read_manifest_list(&bytes)?;

    let mut data_files: Vec<PlannedDataFile> = Vec::new();
    let mut deletes: Vec<PlannedDelete> = Vec::new();

    for manifest_ref in &list.manifests {
        // Manifests with no live entries hold nothing.
        if manifest_ref.added_files_count == Some(0) && manifest_ref.existing_files_count == Some(0)
        {
            continue;
        }
        let manifest_bytes = table.storage.read(&manifest_ref.manifest_path).await?;
        let manifest = read_manifest(&manifest_bytes)?;

        for stored in &manifest.entries {
            if stored.status == ManifestEntryStatus::Deleted {
                continue;
            }
            let mut entry = stored.clone();
            entry.inherit_from(manifest_ref);
            let sequence_number = entry.sequence_number.unwrap_or(0);

            let is_data = manifest.metadata.content == ManifestContentType::Data
                && entry.data_file.content == DataFileContent::Data;
            if is_data {
                data_files.push(PlannedDataFile {
                    file: entry.data_file,
                    spec_id: manifest_ref.partition_spec_id,
                    sequence_number,
                    delete_indices: Vec::new(),
                });
            } else {
                deletes.push(PlannedDelete {
                    file: entry.data_file,
                    spec_id: manifest_ref.partition_spec_id,
                    sequence_number,
                });
            }
        }
    }

    // Attach deletes to each data file, and sum the size estimate.
    let index = DeleteIndex::build(&deletes);
    let mut total_bytes: u64 = 0;
    let mut total_rows: u64 = 0;
    for planned in &mut data_files {
        planned.delete_indices = index.deletes_for(
            &deletes,
            &planned.file,
            planned.spec_id,
            planned.sequence_number,
        );
        total_bytes = total_bytes.saturating_add(file_bytes(&planned.file));
        total_rows =
            total_rows.saturating_add(u64::try_from(planned.file.record_count).unwrap_or(0));
    }

    Ok(ScanPlan {
        data_files,
        deletes,
        bytes: total_bytes,
        rows: total_rows,
        snapshot_id: Some(snapshot_id),
    })
}

/// The table's current schema, or a [`QueryError::UnqueryableTable`] if it does
/// not resolve.
pub(crate) fn current_schema<'a>(table: &'a CatalogTable<'_>) -> QueryResult<&'a Schema> {
    table
        .metadata
        .current_schema()
        .ok_or_else(|| QueryError::UnqueryableTable {
            table: table.name.clone(),
            reason: format!(
                "current schema id {} is not present in the table metadata",
                table.metadata.current_schema_id
            ),
        })
}

/// On-disk bytes of a file, clamped to `u64` (Iceberg stores it as `i64`).
fn file_bytes(file: &DataFile) -> u64 {
    u64::try_from(file.file_size_in_bytes).unwrap_or(0)
}

/// A canonical, hashable partition identity: spec id plus the tuple's field ids
/// and Appendix-D value bytes. Mirrors the scan planner's `partition_key` so the
/// query executor and the planner agree on what "same partition" means.
fn partition_key(spec_id: i32, tuple: &PartitionTuple) -> Vec<u8> {
    let mut key = spec_id.to_le_bytes().to_vec();
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

// ---------------------------------------------------------------------------
// Delete-file attachment (the scan planner's scope rules).
//
// The query-side counterpart of `meridian-server::planning::engine`'s and
// `meridian-executor::select`'s DeleteIndex: it answers "which delete files
// apply to this data file" so the reader can filter exactly those rows. The
// three implementations must agree.
// ---------------------------------------------------------------------------

struct DeleteIndex {
    /// Deletion vectors by `referenced_data_file` (v3 Puffin; refused at read).
    dv_by_path: std::collections::BTreeMap<String, Vec<usize>>,
    /// Plain position deletes with `referenced_data_file`, by that path.
    pos_by_path: std::collections::BTreeMap<String, Vec<usize>>,
    /// Remaining position deletes by partition identity.
    pos_by_partition: std::collections::BTreeMap<Vec<u8>, Vec<usize>>,
    /// Equality deletes written under an unpartitioned spec (global).
    eq_global: Vec<usize>,
    /// Equality deletes by partition identity.
    eq_by_partition: std::collections::BTreeMap<Vec<u8>, Vec<usize>>,
}

impl DeleteIndex {
    fn build(deletes: &[PlannedDelete]) -> Self {
        let mut index = Self {
            dv_by_path: std::collections::BTreeMap::new(),
            pos_by_path: std::collections::BTreeMap::new(),
            pos_by_partition: std::collections::BTreeMap::new(),
            eq_global: Vec::new(),
            eq_by_partition: std::collections::BTreeMap::new(),
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

    /// Ascending indices of the deletes that apply to one data file (same scope
    /// rules as the scan planner: a DV supersedes plain position deletes for the
    /// same file; position deletes need seq `<=` and partition equality;
    /// equality deletes need seq `<` and partition equality or a global spec).
    fn deletes_for(
        &self,
        deletes: &[PlannedDelete],
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

/// Reads object bytes through a [`Storage`] handle, wrapping storage errors as
/// query errors. A thin adapter so the reader module can stay storage-agnostic.
#[derive(Clone)]
pub(crate) struct StorageBytes<'a> {
    storage: &'a dyn Storage,
}

impl<'a> StorageBytes<'a> {
    pub(crate) fn new(storage: &'a dyn Storage) -> Self {
        Self { storage }
    }

    pub(crate) async fn read(&self, location: &str) -> QueryResult<bytes::Bytes> {
        Ok(self.storage.read(location).await?)
    }
}

/// A shared reference into a table's plan and reader for the reader module.
pub(crate) struct TableScan<'a> {
    pub name: &'a str,
    pub schema: &'a Schema,
    pub plan: &'a ScanPlan,
    pub bytes: StorageBytes<'a>,
}
