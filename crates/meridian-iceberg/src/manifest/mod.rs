//! Manifest lists and manifests: the Avro layer of scan planning.
//!
//! A snapshot points at a *manifest list* (one `manifest_file` entry per
//! manifest, with partition field summaries used to skip whole manifests);
//! each *manifest* stores `manifest_entry` records (a status-tracked
//! [`DataFile`] per data or delete file, with the column statistics used
//! to skip individual files).
//!
//! Reading ([`read_manifest_list`], [`read_manifest`]) resolves fields by
//! the Iceberg **field id** stored in the Avro writer schema, not by
//! position or name (names differ across writers: the Java v1 writer says
//! `added_data_files_count`, pyiceberg says `added_files_count`; both are
//! field 504). Formats v1 and v2 are fully modelled; the v3 additions
//! (deletion-vector `content_offset`/`content_size_in_bytes`,
//! `referenced_data_file`, `first_row_id`) are **parsed and preserved but
//! not interpreted** by planning.
//!
//! Writing ([`write_manifest_list`], [`write_manifest`]) emits spec-shaped
//! v1 or v2 files (field ids as `field-id`/`element-id` attributes, spec
//! key-value metadata, deflate codec) sufficient for synthetic fixtures
//! and future compaction rewrites; v3-only fields are not written.
//! Known limitation: the `adjust-to-utc` Avro attribute on timestamp
//! partition values is not emitted (the Avro library drops unknown
//! attributes on logical types); readers that need the distinction use the
//! `partition-spec` metadata key, which is always written.

mod read;
mod write;

use std::collections::BTreeMap;

pub use read::{read_manifest, read_manifest_list};
pub use write::{
    ManifestListWriteParams, ManifestWriteParams, write_manifest, write_manifest_list,
};

use crate::spec::{PartitionField, PrimitiveType, Schema, Transform, Type};
use crate::value::{Datum, ValueError};

/// Iceberg field ids for `manifest_file`, `manifest_entry`, and
/// `data_file` structs (spec "Manifests" / "Manifest Lists" tables).
pub(crate) mod ids {
    pub(crate) const MANIFEST_PATH: i64 = 500;
    pub(crate) const MANIFEST_LENGTH: i64 = 501;
    pub(crate) const PARTITION_SPEC_ID: i64 = 502;
    pub(crate) const ADDED_SNAPSHOT_ID: i64 = 503;
    pub(crate) const ADDED_FILES_COUNT: i64 = 504;
    pub(crate) const EXISTING_FILES_COUNT: i64 = 505;
    pub(crate) const DELETED_FILES_COUNT: i64 = 506;
    pub(crate) const PARTITIONS: i64 = 507;
    pub(crate) const CONTAINS_NULL: i64 = 509;
    pub(crate) const SUMMARY_LOWER: i64 = 510;
    pub(crate) const SUMMARY_UPPER: i64 = 511;
    pub(crate) const ADDED_ROWS_COUNT: i64 = 512;
    pub(crate) const EXISTING_ROWS_COUNT: i64 = 513;
    pub(crate) const DELETED_ROWS_COUNT: i64 = 514;
    pub(crate) const SEQUENCE_NUMBER: i64 = 515;
    pub(crate) const MIN_SEQUENCE_NUMBER: i64 = 516;
    pub(crate) const MANIFEST_CONTENT: i64 = 517;
    pub(crate) const CONTAINS_NAN: i64 = 518;
    pub(crate) const KEY_METADATA: i64 = 519;
    pub(crate) const FIRST_ROW_ID: i64 = 520;

    pub(crate) const ENTRY_STATUS: i64 = 0;
    pub(crate) const ENTRY_SNAPSHOT_ID: i64 = 1;
    pub(crate) const ENTRY_DATA_FILE: i64 = 2;
    pub(crate) const ENTRY_SEQUENCE_NUMBER: i64 = 3;
    pub(crate) const ENTRY_FILE_SEQUENCE_NUMBER: i64 = 4;

    pub(crate) const DF_FILE_PATH: i64 = 100;
    pub(crate) const DF_FILE_FORMAT: i64 = 101;
    pub(crate) const DF_PARTITION: i64 = 102;
    pub(crate) const DF_RECORD_COUNT: i64 = 103;
    pub(crate) const DF_FILE_SIZE: i64 = 104;
    pub(crate) const DF_BLOCK_SIZE: i64 = 105;
    pub(crate) const DF_COLUMN_SIZES: i64 = 108;
    pub(crate) const DF_VALUE_COUNTS: i64 = 109;
    pub(crate) const DF_NULL_VALUE_COUNTS: i64 = 110;
    pub(crate) const DF_LOWER_BOUNDS: i64 = 125;
    pub(crate) const DF_UPPER_BOUNDS: i64 = 128;
    pub(crate) const DF_KEY_METADATA: i64 = 131;
    pub(crate) const DF_SPLIT_OFFSETS: i64 = 132;
    pub(crate) const DF_CONTENT: i64 = 134;
    pub(crate) const DF_EQUALITY_IDS: i64 = 135;
    pub(crate) const DF_NAN_VALUE_COUNTS: i64 = 137;
    pub(crate) const DF_SORT_ORDER_ID: i64 = 140;
    pub(crate) const DF_FIRST_ROW_ID: i64 = 142;
    pub(crate) const DF_REFERENCED_DATA_FILE: i64 = 143;
    pub(crate) const DF_CONTENT_OFFSET: i64 = 144;
    pub(crate) const DF_CONTENT_SIZE: i64 = 145;
}

/// Error reading or writing manifest Avro.
#[derive(Debug, thiserror::Error)]
pub enum ManifestError {
    /// The Avro container itself could not be read or written.
    #[error("avro error: {0}")]
    Avro(String),
    /// The file's writer schema or key-value metadata does not look like an
    /// Iceberg manifest / manifest list.
    #[error("unexpected manifest shape: {0}")]
    Shape(String),
    /// A single value failed to convert.
    #[error(transparent)]
    Value(#[from] ValueError),
    /// The requested write cannot be expressed in the target format
    /// version (e.g. delete manifests in v1).
    #[error("unsupported: {0}")]
    Unsupported(String),
}

impl From<apache_avro::Error> for ManifestError {
    fn from(err: apache_avro::Error) -> Self {
        Self::Avro(err.to_string())
    }
}

/// Content kind of a manifest (`manifest_file.content`, field 517).
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ManifestContentType {
    /// Tracks data files.
    Data,
    /// Tracks delete files (v2+).
    Deletes,
}

impl ManifestContentType {
    /// The spec's integer code.
    #[must_use]
    pub fn code(self) -> i32 {
        match self {
            Self::Data => 0,
            Self::Deletes => 1,
        }
    }

    pub(crate) fn from_code(code: i32) -> Result<Self, ManifestError> {
        match code {
            0 => Ok(Self::Data),
            1 => Ok(Self::Deletes),
            other => Err(ManifestError::Shape(format!(
                "unknown manifest content code {other}"
            ))),
        }
    }
}

/// Status of a manifest entry (`manifest_entry.status`, field 0).
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ManifestEntryStatus {
    /// Carried forward from a previous snapshot.
    Existing,
    /// Added by the snapshot that wrote the manifest.
    Added,
    /// Logically deleted in the snapshot that wrote the manifest (kept for
    /// tracking; not scanned).
    Deleted,
}

impl ManifestEntryStatus {
    /// The spec's integer code.
    #[must_use]
    pub fn code(self) -> i32 {
        match self {
            Self::Existing => 0,
            Self::Added => 1,
            Self::Deleted => 2,
        }
    }

    pub(crate) fn from_code(code: i32) -> Result<Self, ManifestError> {
        match code {
            0 => Ok(Self::Existing),
            1 => Ok(Self::Added),
            2 => Ok(Self::Deleted),
            other => Err(ManifestError::Shape(format!(
                "unknown manifest entry status {other}"
            ))),
        }
    }
}

/// Content kind of a tracked file (`data_file.content`, field 134).
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum DataFileContent {
    /// Rows.
    Data,
    /// Position deletes (or a deletion vector when
    /// [`DataFile::content_offset`] is set).
    PositionDeletes,
    /// Equality deletes.
    EqualityDeletes,
}

impl DataFileContent {
    /// The spec's integer code.
    #[must_use]
    pub fn code(self) -> i32 {
        match self {
            Self::Data => 0,
            Self::PositionDeletes => 1,
            Self::EqualityDeletes => 2,
        }
    }

    pub(crate) fn from_code(code: i32) -> Result<Self, ManifestError> {
        match code {
            0 => Ok(Self::Data),
            1 => Ok(Self::PositionDeletes),
            2 => Ok(Self::EqualityDeletes),
            other => Err(ManifestError::Shape(format!(
                "unknown data file content code {other}"
            ))),
        }
    }
}

/// A parsed manifest list: the `manifest_file` entries for one snapshot.
#[derive(Debug, Clone, PartialEq)]
pub struct ManifestList {
    /// `format-version` from the file's key-value metadata, when present.
    pub format_version: Option<u8>,
    /// `snapshot-id` metadata, when present.
    pub snapshot_id: Option<i64>,
    /// `parent-snapshot-id` metadata, when present.
    pub parent_snapshot_id: Option<i64>,
    /// `sequence-number` metadata (v2+), when present.
    pub sequence_number: Option<i64>,
    /// The manifests, in file order (delete manifests conventionally
    /// ordered after… whatever the writer chose; order is preserved).
    pub manifests: Vec<ManifestFile>,
}

/// One `manifest_file` entry in a manifest list.
#[derive(Debug, Clone, PartialEq)]
pub struct ManifestFile {
    /// Location of the manifest file (field 500).
    pub manifest_path: String,
    /// Length of the manifest file in bytes (501).
    pub manifest_length: i64,
    /// Partition spec used to write the manifest (502).
    pub partition_spec_id: i32,
    /// Data or deletes (517); v1 files have no content field and default
    /// to data.
    pub content: ManifestContentType,
    /// Sequence number when the manifest was added (515); 0 for v1.
    pub sequence_number: i64,
    /// Minimum data sequence number of live files (516); 0 for v1.
    pub min_sequence_number: i64,
    /// Snapshot that added this manifest (503).
    pub added_snapshot_id: i64,
    /// Entries with status ADDED (504); optional in v1.
    pub added_files_count: Option<i32>,
    /// Entries with status EXISTING (505); optional in v1.
    pub existing_files_count: Option<i32>,
    /// Entries with status DELETED (506); optional in v1.
    pub deleted_files_count: Option<i32>,
    /// Rows in ADDED files (512); optional in v1.
    pub added_rows_count: Option<i64>,
    /// Rows in EXISTING files (513); optional in v1.
    pub existing_rows_count: Option<i64>,
    /// Rows in DELETED files (514); optional in v1.
    pub deleted_rows_count: Option<i64>,
    /// Per-partition-field value summaries (507), in partition spec field
    /// order.
    pub partitions: Option<Vec<FieldSummary>>,
    /// Encryption key metadata (519), preserved opaquely.
    pub key_metadata: Option<Vec<u8>>,
    /// v3 row lineage (520): preserved, not interpreted.
    pub first_row_id: Option<i64>,
}

/// A partition field summary (`field_summary`, field 508) used to prune
/// whole manifests.
#[derive(Debug, Clone, Default, PartialEq)]
pub struct FieldSummary {
    /// Whether any tracked file has a null value for the field (509).
    pub contains_null: bool,
    /// Whether any tracked file has a NaN value for the field (518);
    /// `None` means unknown.
    pub contains_nan: Option<bool>,
    /// Lower bound bytes (510), Appendix D single-value serialization in
    /// the *partition field's result type*; `None` when all values are
    /// null/NaN.
    pub lower_bound: Option<Vec<u8>>,
    /// Upper bound bytes (511), as above.
    pub upper_bound: Option<Vec<u8>>,
}

impl FieldSummary {
    /// Decodes the lower bound as the given result type. `None` when there
    /// is no bound; `Err` when the bytes do not decode (callers must treat
    /// that as unknown, not prune).
    pub fn lower(&self, result_type: &PrimitiveType) -> Result<Option<Datum>, ValueError> {
        self.lower_bound
            .as_deref()
            .map(|b| Datum::from_bound_bytes(result_type, b))
            .transpose()
    }

    /// Decodes the upper bound as the given result type; see
    /// [`FieldSummary::lower`].
    pub fn upper(&self, result_type: &PrimitiveType) -> Result<Option<Datum>, ValueError> {
        self.upper_bound
            .as_deref()
            .map(|b| Datum::from_bound_bytes(result_type, b))
            .transpose()
    }
}

/// Metadata of a manifest file, from the Avro key-value header.
#[derive(Debug, Clone, PartialEq)]
pub struct ManifestMetadata {
    /// The table schema JSON at write time (`schema` key), verbatim.
    pub schema_json: String,
    /// `schema-id`, when present (required v2+).
    pub schema_id: Option<i32>,
    /// The partition fields the manifest was written with
    /// (`partition-spec` key: a JSON array of partition fields).
    pub partition_fields: Vec<PartitionField>,
    /// `partition-spec-id`, when present (required v2+).
    pub partition_spec_id: Option<i32>,
    /// `format-version`, when present (required v2+); v1 files often omit
    /// it.
    pub format_version: Option<u8>,
    /// `content` key (`"data"` or `"deletes"`); v1 files omit it (data).
    pub content: ManifestContentType,
}

/// A parsed manifest: metadata plus entries in file order.
#[derive(Debug, Clone, PartialEq)]
pub struct Manifest {
    /// Header metadata.
    pub metadata: ManifestMetadata,
    /// The entries, exactly as stored (no sequence-number inheritance
    /// applied — see [`ManifestEntry::inherit_from`]).
    pub entries: Vec<ManifestEntry>,
}

/// One `manifest_entry` record.
#[derive(Debug, Clone, PartialEq)]
pub struct ManifestEntry {
    /// ADDED / EXISTING / DELETED (field 0).
    pub status: ManifestEntryStatus,
    /// Snapshot where the file was added or deleted (1); `None` means
    /// inherit from the manifest list entry.
    pub snapshot_id: Option<i64>,
    /// Data sequence number (3); `None` means inherit (v2, ADDED entries)
    /// or 0 (v1).
    pub sequence_number: Option<i64>,
    /// File sequence number (4); inheritance as above.
    pub file_sequence_number: Option<i64>,
    /// The tracked file.
    pub data_file: DataFile,
}

impl ManifestEntry {
    /// Applies the spec's inheritance rules from the manifest-list entry
    /// that referenced this manifest: a null `snapshot_id` inherits
    /// `added_snapshot_id`; null sequence numbers inherit the manifest's
    /// sequence number for ADDED entries (and unconditionally for v1
    /// manifests, whose list entries carry sequence number 0).
    pub fn inherit_from(&mut self, manifest: &ManifestFile) {
        if self.snapshot_id.is_none() {
            self.snapshot_id = Some(manifest.added_snapshot_id);
        }
        let v1_or_added =
            manifest.sequence_number == 0 || self.status == ManifestEntryStatus::Added;
        if self.sequence_number.is_none() && v1_or_added {
            self.sequence_number = Some(manifest.sequence_number);
        }
        if self.file_sequence_number.is_none() && v1_or_added {
            self.file_sequence_number = Some(manifest.sequence_number);
        }
    }
}

/// A `data_file` struct: one data or delete file with its metrics.
#[derive(Debug, Clone, PartialEq)]
pub struct DataFile {
    /// Data, position deletes, or equality deletes (134). All v1 files are
    /// data files.
    pub content: DataFileContent,
    /// Full URI (100).
    pub file_path: String,
    /// File format name as stored (101): `PARQUET`, `AVRO`, `ORC`,
    /// `PUFFIN` (case preserved).
    pub file_format: String,
    /// Partition data tuple (102), typed by the manifest's partition spec.
    pub partition: PartitionTuple,
    /// Number of records, or deletion-vector cardinality (103).
    pub record_count: i64,
    /// Total file size in bytes (104).
    pub file_size_in_bytes: i64,
    /// Column id -> on-disk size (108).
    pub column_sizes: Option<BTreeMap<i32, i64>>,
    /// Column id -> value count including nulls and NaNs (109).
    pub value_counts: Option<BTreeMap<i32, i64>>,
    /// Column id -> null count (110).
    pub null_value_counts: Option<BTreeMap<i32, i64>>,
    /// Column id -> NaN count (137).
    pub nan_value_counts: Option<BTreeMap<i32, i64>>,
    /// Column id -> lower bound, Appendix D bytes (125). May be truncated
    /// by the writer's metrics config; still a valid bound.
    pub lower_bounds: Option<BTreeMap<i32, Vec<u8>>>,
    /// Column id -> upper bound (128).
    pub upper_bounds: Option<BTreeMap<i32, Vec<u8>>>,
    /// Encryption key metadata (131), preserved opaquely.
    pub key_metadata: Option<Vec<u8>>,
    /// Split offsets, ascending (132).
    pub split_offsets: Option<Vec<i64>>,
    /// Equality field ids (135); required when content is equality
    /// deletes.
    pub equality_ids: Option<Vec<i32>>,
    /// Sort order id (140).
    pub sort_order_id: Option<i32>,
    /// v3 row lineage (142): preserved, not interpreted.
    pub first_row_id: Option<i64>,
    /// Data file all deletes reference (143, v2+): preserved; planning
    /// does not yet use it to narrow delete application.
    pub referenced_data_file: Option<String>,
    /// v3 deletion vector blob offset (144): preserved, not interpreted.
    pub content_offset: Option<i64>,
    /// v3 deletion vector blob length (145): preserved, not interpreted.
    pub content_size_in_bytes: Option<i64>,
}

impl DataFile {
    /// Decodes this file's lower bound for a column as the given type.
    /// `None` when absent; `Err` when undecodable (treat as unknown).
    pub fn lower_bound(
        &self,
        field_id: i32,
        ty: &PrimitiveType,
    ) -> Result<Option<Datum>, ValueError> {
        Self::decode_bound(self.lower_bounds.as_ref(), field_id, ty)
    }

    /// Decodes this file's upper bound for a column as the given type.
    pub fn upper_bound(
        &self,
        field_id: i32,
        ty: &PrimitiveType,
    ) -> Result<Option<Datum>, ValueError> {
        Self::decode_bound(self.upper_bounds.as_ref(), field_id, ty)
    }

    fn decode_bound(
        bounds: Option<&BTreeMap<i32, Vec<u8>>>,
        field_id: i32,
        ty: &PrimitiveType,
    ) -> Result<Option<Datum>, ValueError> {
        bounds
            .and_then(|m| m.get(&field_id))
            .map(|b| Datum::from_bound_bytes(ty, b))
            .transpose()
    }
}

/// A typed partition data tuple: one value per partition spec field, in
/// spec order.
#[derive(Debug, Clone, Default, PartialEq)]
pub struct PartitionTuple {
    /// The values, in partition-spec field order.
    pub fields: Vec<PartitionValue>,
}

impl PartitionTuple {
    /// The value for a partition field id, if the tuple has that field.
    /// The outer `Option` is presence; the inner is nullness.
    #[must_use]
    pub fn get(&self, field_id: i32) -> Option<&Option<Datum>> {
        self.fields
            .iter()
            .find(|f| f.field_id == field_id)
            .map(|f| &f.value)
    }
}

/// One field of a partition tuple.
#[derive(Debug, Clone, PartialEq)]
pub struct PartitionValue {
    /// Partition field id (matches the spec field and the Avro `field-id`).
    pub field_id: i32,
    /// Partition field name as stored in the file.
    pub name: String,
    /// The value; `None` is a null partition value.
    pub value: Option<Datum>,
}

/// The typed shape of a partition tuple under a spec: field id, name, and
/// the transform's *result* primitive type.
#[derive(Debug, Clone)]
pub struct PartitionFieldType {
    /// Partition field id.
    pub field_id: i32,
    /// Partition field name.
    pub name: String,
    /// The transform applied.
    pub transform: Transform,
    /// Source column field id.
    pub source_id: i32,
    /// Source column primitive type.
    pub source_type: PrimitiveType,
    /// The transform's result primitive type (the type of stored partition
    /// values and of field-summary bounds).
    pub result_type: PrimitiveType,
}

/// Resolves each partition field of a spec to its result primitive type
/// using the table schema (recursively over nested fields).
///
/// Errors when a source field is missing, is not a primitive, has no
/// assigned partition field id, or uses a transform this model cannot
/// type (unrecognized transforms).
pub fn partition_field_types(
    fields: &[PartitionField],
    schema: &Schema,
) -> Result<Vec<PartitionFieldType>, ManifestError> {
    fn walk<'a>(
        fields: &'a [crate::spec::StructField],
        out: &mut BTreeMap<i32, &'a PrimitiveType>,
    ) {
        for field in fields {
            match &field.field_type {
                Type::Primitive(p) => {
                    out.insert(field.id, p);
                }
                Type::Struct(s) => walk(&s.fields, out),
                // Elements of lists/maps cannot be partition sources today;
                // recorded anyway for completeness of primitive lookups.
                Type::List(l) => {
                    if let Type::Primitive(p) = l.element.as_ref() {
                        out.insert(l.element_id, p);
                    }
                }
                Type::Map(m) => {
                    if let Type::Primitive(p) = m.key.as_ref() {
                        out.insert(m.key_id, p);
                    }
                    if let Type::Primitive(p) = m.value.as_ref() {
                        out.insert(m.value_id, p);
                    }
                }
            }
        }
    }

    let mut by_id: BTreeMap<i32, &PrimitiveType> = BTreeMap::new();
    walk(&schema.fields, &mut by_id);

    fields
        .iter()
        .map(|pf| {
            let field_id = pf.field_id.ok_or_else(|| {
                ManifestError::Shape(format!("partition field {:?} has no field-id", pf.name))
            })?;
            let source = by_id.get(&pf.source_id).copied().cloned().ok_or_else(|| {
                ManifestError::Shape(format!(
                    "partition source field {} is not a primitive column in the schema",
                    pf.source_id
                ))
            })?;
            let result_type = transform_result_type(&pf.transform, &source).ok_or_else(|| {
                ManifestError::Unsupported(format!(
                    "cannot type transform {} over {source}",
                    pf.transform
                ))
            })?;
            Ok(PartitionFieldType {
                field_id,
                name: pf.name.clone(),
                transform: pf.transform.clone(),
                source_id: pf.source_id,
                source_type: source,
                result_type,
            })
        })
        .collect()
}

/// The result type of a transform over a source type (spec "Partition
/// Transforms" table). `None` for unrecognized transforms.
#[must_use]
pub fn transform_result_type(
    transform: &Transform,
    source: &PrimitiveType,
) -> Option<PrimitiveType> {
    match transform {
        Transform::Identity | Transform::Truncate(_) | Transform::Void => Some(source.clone()),
        Transform::Bucket(_) | Transform::Year | Transform::Month | Transform::Hour => {
            Some(PrimitiveType::Int)
        }
        Transform::Day => Some(PrimitiveType::Date),
        Transform::Other(_) => None,
    }
}
