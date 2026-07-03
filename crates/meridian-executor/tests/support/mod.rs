//! Shared test scaffolding: build synthetic Parquet + manifests in memory,
//! feed them to the compaction engine, and read compacted output back.
//!
//! The fixtures are real Parquet files (written with the `parquet` crate,
//! carrying Iceberg `PARQUET:field_id` metadata on every column) and real
//! Iceberg manifest/manifest-list Avro (written with `meridian_iceberg`), so
//! the engine exercises the same read path it uses in production.
//!
//! This module is compiled into every integration-test binary; each binary
//! uses only part of it, so the usual "unused"/"unreachable pub" lints are
//! expected and allowed here. The pedantic cast/line/doc lints are likewise
//! relaxed for fixtures: these builders cast tiny known-small `usize` row and
//! byte counts into the `i32`/`i64` the Iceberg model uses (never near an
//! overflow boundary), are long by nature (assembling whole tables inline),
//! and their doc comments name paths/tuples in prose. None of that is a
//! correctness concern in synthetic test data; the crate's `src/` stays strict.
#![allow(
    dead_code,
    unreachable_pub,
    clippy::cast_possible_wrap,
    clippy::cast_possible_truncation,
    clippy::too_many_lines,
    clippy::doc_markdown,
    clippy::unnecessary_wraps,
    clippy::unnecessary_literal_bound,
    clippy::needless_lifetimes
)]

use std::collections::BTreeMap;
use std::sync::Arc;

use arrow_array::{ArrayRef, Int64Array, RecordBatch, StringArray, UInt64Array, cast::AsArray};
use arrow_schema::{DataType, Field, Schema as ArrowSchema};
use bytes::Bytes;
use meridian_executor::error::CompactionResult;
use meridian_executor::{FileBytes, ManifestSource};
use meridian_iceberg::manifest::{
    DataFile, DataFileContent, Manifest, ManifestContentType, ManifestEntry, ManifestEntryStatus,
    ManifestFile, ManifestList, ManifestListWriteParams, ManifestWriteParams, PartitionFieldType,
    PartitionTuple, PartitionValue, partition_field_types, read_manifest, read_manifest_list,
    write_manifest, write_manifest_list,
};
use meridian_iceberg::spec::{
    PartitionField, PartitionSpec, PrimitiveType, Schema, StructField, TableMetadata, Transform,
    Type,
};
use meridian_iceberg::value::Datum;
use parquet::arrow::arrow_reader::ParquetRecordBatchReaderBuilder;
use parquet::arrow::{ArrowWriter, PARQUET_FIELD_ID_META_KEY};

/// The reserved field id of the `file_path` column in position-delete files.
pub const POS_DELETE_FILE_PATH_ID: i32 = 2_147_483_546;
/// The reserved field id of the `pos` column in position-delete files.
pub const POS_DELETE_POS_ID: i32 = 2_147_483_545;

/// An in-memory store of manifest and file bytes by location. Implements both
/// [`ManifestSource`] (parsed) and [`FileBytes`] (raw) so one map backs the
/// whole read side; writes go to a separate [`RecordingStorage`].
#[derive(Default)]
pub struct MemStore {
    pub objects: BTreeMap<String, Bytes>,
}

impl MemStore {
    pub fn put(&mut self, location: impl Into<String>, bytes: impl Into<Bytes>) {
        self.objects.insert(location.into(), bytes.into());
    }
}

impl ManifestSource for MemStore {
    async fn manifest_list(&self, location: &str) -> CompactionResult<Arc<ManifestList>> {
        let bytes = self.get(location)?;
        Ok(Arc::new(
            read_manifest_list(&bytes).expect("parse manifest list"),
        ))
    }

    async fn manifest(&self, location: &str) -> CompactionResult<Arc<Manifest>> {
        let bytes = self.get(location)?;
        Ok(Arc::new(read_manifest(&bytes).expect("parse manifest")))
    }
}

impl FileBytes for MemStore {
    async fn read(&self, location: &str) -> CompactionResult<Bytes> {
        self.get(location)
    }
}

impl MemStore {
    fn get(&self, location: &str) -> CompactionResult<Bytes> {
        Ok(self
            .objects
            .get(location)
            .cloned()
            .unwrap_or_else(|| panic!("MemStore missing object {location:?}")))
    }
}

/// A minimal [`meridian_storage::Storage`] that records writes into a shared
/// map (so the test can read the output Parquet + manifests back). Reads fall
/// back to whatever was written. Only the methods compaction uses are real;
/// the rest are unreachable in these tests.
#[derive(Debug, Default)]
pub struct RecordingStorage {
    pub written: std::sync::Mutex<BTreeMap<String, Bytes>>,
}

#[async_trait::async_trait]
impl meridian_storage::Storage for RecordingStorage {
    fn root_uri(&self) -> &str {
        "mem://test"
    }

    async fn read(&self, location: &str) -> meridian_storage::StorageResult<Bytes> {
        self.written
            .lock()
            .expect("lock")
            .get(location)
            .cloned()
            .ok_or(meridian_storage::StorageError::NotFound {
                location: location.to_owned(),
            })
    }

    async fn write(&self, location: &str, bytes: Bytes) -> meridian_storage::StorageResult<()> {
        self.written
            .lock()
            .expect("lock")
            .insert(location.to_owned(), bytes);
        Ok(())
    }

    async fn write_if_absent(
        &self,
        location: &str,
        bytes: Bytes,
    ) -> meridian_storage::StorageResult<()> {
        self.write(location, bytes).await
    }

    async fn exists(&self, location: &str) -> meridian_storage::StorageResult<bool> {
        Ok(self.written.lock().expect("lock").contains_key(location))
    }

    async fn delete(&self, _location: &str) -> meridian_storage::StorageResult<()> {
        unreachable!("compaction never deletes")
    }

    async fn delete_prefix(&self, _prefix: &str) -> meridian_storage::StorageResult<()> {
        unreachable!("compaction never deletes")
    }

    async fn list(
        &self,
        _prefix: &str,
    ) -> meridian_storage::StorageResult<meridian_storage::ObjectStream> {
        unreachable!("compaction never lists")
    }
}

/// A simple orders-like schema: id (long, field 1), category (string, 2),
/// amount (long, 3). Partitioned by identity(category).
pub fn orders_schema() -> Schema {
    Schema::new(vec![
        StructField::optional(1, "id", Type::Primitive(PrimitiveType::Long)),
        StructField::optional(2, "category", Type::Primitive(PrimitiveType::String)),
        StructField::optional(3, "amount", Type::Primitive(PrimitiveType::Long)),
    ])
    .with_schema_id(0)
}

/// The identity(category) partition spec (spec id 0, field id 1000).
pub fn category_spec() -> PartitionSpec {
    let mut field = PartitionField::new(2, "category", Transform::Identity);
    field.field_id = Some(1000);
    let mut spec = PartitionSpec::new(vec![field]);
    spec.spec_id = Some(0);
    spec
}

/// One row of the orders table.
#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord)]
pub struct Row {
    pub id: i64,
    pub category: String,
    pub amount: i64,
}

/// The Arrow schema matching [`orders_schema`], with field ids in metadata.
fn orders_arrow_schema() -> Arc<ArrowSchema> {
    Arc::new(ArrowSchema::new(vec![
        arrow_field("id", DataType::Int64, 1),
        arrow_field("category", DataType::Utf8, 2),
        arrow_field("amount", DataType::Int64, 3),
    ]))
}

fn arrow_field(name: &str, dt: DataType, field_id: i32) -> Field {
    let mut md = std::collections::HashMap::new();
    md.insert(PARQUET_FIELD_ID_META_KEY.to_string(), field_id.to_string());
    Field::new(name, dt, true).with_metadata(md)
}

/// Writes the given rows to a Parquet byte buffer (orders schema, field ids
/// preserved).
pub fn write_orders_parquet(rows: &[Row]) -> Bytes {
    let schema = orders_arrow_schema();
    let ids: ArrayRef = Arc::new(Int64Array::from(
        rows.iter().map(|r| r.id).collect::<Vec<_>>(),
    ));
    let cats: ArrayRef = Arc::new(StringArray::from(
        rows.iter().map(|r| r.category.clone()).collect::<Vec<_>>(),
    ));
    let amounts: ArrayRef = Arc::new(Int64Array::from(
        rows.iter().map(|r| r.amount).collect::<Vec<_>>(),
    ));
    let batch = RecordBatch::try_new(schema.clone(), vec![ids, cats, amounts]).expect("batch");
    let mut buf: Vec<u8> = Vec::new();
    let mut w = ArrowWriter::try_new(&mut buf, schema, None).expect("writer");
    w.write(&batch).expect("write");
    w.close().expect("close");
    Bytes::from(buf)
}

/// Writes the same rows but with the physical columns in *reversed* order
/// (amount, category, id) — field ids still correct. Proves the engine maps
/// columns by field id, not position: a file written this way must still
/// merge correctly with normally-ordered files.
pub fn write_orders_parquet_reversed(rows: &[Row]) -> Bytes {
    let schema = Arc::new(ArrowSchema::new(vec![
        arrow_field("amount", DataType::Int64, 3),
        arrow_field("category", DataType::Utf8, 2),
        arrow_field("id", DataType::Int64, 1),
    ]));
    let amounts: ArrayRef = Arc::new(Int64Array::from(
        rows.iter().map(|r| r.amount).collect::<Vec<_>>(),
    ));
    let cats: ArrayRef = Arc::new(StringArray::from(
        rows.iter().map(|r| r.category.clone()).collect::<Vec<_>>(),
    ));
    let ids: ArrayRef = Arc::new(Int64Array::from(
        rows.iter().map(|r| r.id).collect::<Vec<_>>(),
    ));
    let batch = RecordBatch::try_new(schema.clone(), vec![amounts, cats, ids]).expect("batch");
    let mut buf: Vec<u8> = Vec::new();
    let mut w = ArrowWriter::try_new(&mut buf, schema, None).expect("writer");
    w.write(&batch).expect("write");
    w.close().expect("close");
    Bytes::from(buf)
}

/// Writes an *older-schema* file that predates the `amount` column (id +
/// category only). On rewrite the engine must synthesize a null `amount`
/// column for these rows (schema evolution).
pub fn write_orders_parquet_no_amount(rows: &[Row]) -> Bytes {
    let schema = Arc::new(ArrowSchema::new(vec![
        arrow_field("id", DataType::Int64, 1),
        arrow_field("category", DataType::Utf8, 2),
    ]));
    let ids: ArrayRef = Arc::new(Int64Array::from(
        rows.iter().map(|r| r.id).collect::<Vec<_>>(),
    ));
    let cats: ArrayRef = Arc::new(StringArray::from(
        rows.iter().map(|r| r.category.clone()).collect::<Vec<_>>(),
    ));
    let batch = RecordBatch::try_new(schema.clone(), vec![ids, cats]).expect("batch");
    let mut buf: Vec<u8> = Vec::new();
    let mut w = ArrowWriter::try_new(&mut buf, schema, None).expect("writer");
    w.write(&batch).expect("write");
    w.close().expect("close");
    Bytes::from(buf)
}

/// Reads an orders Parquet byte buffer back into rows (by field id, so a
/// column reorder in the output is caught). A null `amount` reads as 0 (the
/// schema-evolution case fills nulls).
pub fn read_orders_parquet(bytes: &Bytes) -> Vec<Row> {
    let builder = ParquetRecordBatchReaderBuilder::try_new(bytes.clone()).expect("reader");
    let schema = builder.schema().clone();
    // Resolve columns by field id, not position.
    let col_of = |field_id: i32| -> usize {
        schema
            .fields()
            .iter()
            .position(|f| {
                f.metadata()
                    .get(PARQUET_FIELD_ID_META_KEY)
                    .and_then(|s| s.parse::<i32>().ok())
                    == Some(field_id)
            })
            .unwrap_or_else(|| panic!("output missing field id {field_id}"))
    };
    let id_col = col_of(1);
    let cat_col = col_of(2);
    let amt_col = col_of(3);
    let reader = builder.build().expect("build");
    let mut rows = Vec::new();
    for batch in reader {
        let batch = batch.expect("batch");
        let ids = batch
            .column(id_col)
            .as_primitive::<arrow_array::types::Int64Type>();
        let cats = batch.column(cat_col).as_string::<i32>();
        let amts = batch
            .column(amt_col)
            .as_primitive::<arrow_array::types::Int64Type>();
        for i in 0..batch.num_rows() {
            rows.push(Row {
                id: ids.value(i),
                category: cats.value(i).to_owned(),
                amount: amts.value(i),
            });
        }
    }
    rows
}

/// Field ids present on every column of an output Parquet file (for the
/// field-id-preservation assertion).
pub fn output_field_ids(bytes: &Bytes) -> Vec<i32> {
    let builder = ParquetRecordBatchReaderBuilder::try_new(bytes.clone()).expect("reader");
    builder
        .schema()
        .fields()
        .iter()
        .filter_map(|f| {
            f.metadata()
                .get(PARQUET_FIELD_ID_META_KEY)
                .and_then(|s| s.parse::<i32>().ok())
        })
        .collect()
}

/// How a fixture data file's Parquet columns are physically laid out. All
/// layouts carry correct Iceberg field ids; the point is to prove the engine
/// maps columns by field id (not name or position) and tolerates
/// schema-evolved inputs.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub enum ParquetLayout {
    /// Columns in schema order (id, category, amount) — the common case.
    #[default]
    Normal,
    /// Columns in *reversed* physical order (amount, category, id), field ids
    /// still correct. A file written this way must still merge with
    /// normally-ordered files by field id.
    ReversedColumns,
    /// An older-schema file predating the `amount` column (id, category only).
    /// On rewrite the engine must synthesize a null `amount` for these rows.
    NoAmountColumn,
}

/// A single-partition data file descriptor for building fixtures.
pub struct DataFileSpec {
    pub path: String,
    pub category: String,
    pub rows: Vec<Row>,
    pub size_bytes: i64,
    pub sequence_number: i64,
    pub snapshot_id: i64,
    /// Physical Parquet layout for this file (defaults to [`ParquetLayout::Normal`]).
    pub layout: ParquetLayout,
}

impl DataFileSpec {
    /// The Parquet bytes for this file under its declared layout.
    fn parquet_bytes(&self) -> Bytes {
        match self.layout {
            ParquetLayout::Normal => write_orders_parquet(&self.rows),
            ParquetLayout::ReversedColumns => write_orders_parquet_reversed(&self.rows),
            ParquetLayout::NoAmountColumn => write_orders_parquet_no_amount(&self.rows),
        }
    }
}

/// A position-delete file descriptor.
pub struct PositionDeleteSpec {
    pub path: String,
    pub category: String,
    /// (data_file_path, position) pairs to delete.
    pub deletes: Vec<(String, u64)>,
    pub sequence_number: i64,
    pub snapshot_id: i64,
}

/// The partition tuple for a category value under [`category_spec`].
fn category_tuple(category: &str) -> PartitionTuple {
    PartitionTuple {
        fields: vec![PartitionValue {
            field_id: 1000,
            name: "category".to_owned(),
            value: Some(Datum::String(category.to_owned())),
        }],
    }
}

fn partition_types() -> Vec<PartitionFieldType> {
    partition_field_types(&category_spec().fields, &orders_schema()).expect("types")
}

/// Writes a position-delete Parquet file (reserved field ids on file_path/pos)
/// and returns its bytes.
pub fn write_position_delete_parquet(deletes: &[(String, u64)]) -> Bytes {
    let schema = Arc::new(ArrowSchema::new(vec![
        arrow_field("file_path", DataType::Utf8, POS_DELETE_FILE_PATH_ID),
        arrow_field("pos", DataType::UInt64, POS_DELETE_POS_ID),
    ]));
    let paths: ArrayRef = Arc::new(StringArray::from(
        deletes.iter().map(|(p, _)| p.clone()).collect::<Vec<_>>(),
    ));
    let positions: ArrayRef = Arc::new(UInt64Array::from(
        deletes.iter().map(|(_, p)| *p).collect::<Vec<_>>(),
    ));
    let batch = RecordBatch::try_new(schema.clone(), vec![paths, positions]).expect("batch");
    let mut buf: Vec<u8> = Vec::new();
    let mut w = ArrowWriter::try_new(&mut buf, schema, None).expect("writer");
    w.write(&batch).expect("write");
    w.close().expect("close");
    Bytes::from(buf)
}

/// Builds a whole table fixture: writes the data + delete Parquet into
/// `store`, writes one data manifest and (if any deletes) one delete manifest
/// plus the manifest list, and returns the base [`TableMetadata`] whose
/// current snapshot points at that list.
///
/// `format_version` is 1 or 2 (deletes require 2).
pub fn build_fixture(
    store: &mut MemStore,
    table_location: &str,
    format_version: u8,
    data_files: &[DataFileSpec],
    delete_files: &[PositionDeleteSpec],
    snapshot_id: i64,
    sequence_number: i64,
) -> TableMetadata {
    let schema = orders_schema();
    let schema_json = serde_json::to_string(&schema).expect("schema json");
    let spec = category_spec();
    let types = partition_types();

    // Write data Parquet + build data-manifest entries.
    let mut data_entries = Vec::new();
    for spec_file in data_files {
        store.put(spec_file.path.clone(), spec_file.parquet_bytes());
        data_entries.push(ManifestEntry {
            status: ManifestEntryStatus::Added,
            snapshot_id: Some(spec_file.snapshot_id),
            sequence_number: Some(spec_file.sequence_number),
            file_sequence_number: Some(spec_file.sequence_number),
            data_file: DataFile {
                content: DataFileContent::Data,
                file_path: spec_file.path.clone(),
                file_format: "PARQUET".to_owned(),
                partition: category_tuple(&spec_file.category),
                record_count: spec_file.rows.len() as i64,
                file_size_in_bytes: spec_file.size_bytes,
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
            },
        });
    }

    let data_manifest_path = format!("{table_location}/metadata/data-m0.avro");
    let data_bytes = write_manifest(&ManifestWriteParams {
        format_version,
        content: ManifestContentType::Data,
        schema_json: &schema_json,
        schema_id: Some(0),
        partition_spec_id: 0,
        partition_fields: &spec.fields,
        partition_types: &types,
        entries: &data_entries,
    })
    .expect("write data manifest");
    store.put(data_manifest_path.clone(), data_bytes.clone());

    let mut manifest_files = vec![manifest_file(
        &data_manifest_path,
        &data_bytes,
        ManifestContentType::Data,
        &data_entries,
        snapshot_id,
        sequence_number,
    )];

    // Delete files + delete manifest.
    if !delete_files.is_empty() {
        assert!(format_version >= 2, "deletes require v2");
        let mut delete_entries = Vec::new();
        for del in delete_files {
            store.put(
                del.path.clone(),
                write_position_delete_parquet(&del.deletes),
            );
            delete_entries.push(ManifestEntry {
                status: ManifestEntryStatus::Added,
                snapshot_id: Some(del.snapshot_id),
                sequence_number: Some(del.sequence_number),
                file_sequence_number: Some(del.sequence_number),
                data_file: DataFile {
                    content: DataFileContent::PositionDeletes,
                    file_path: del.path.clone(),
                    file_format: "PARQUET".to_owned(),
                    partition: category_tuple(&del.category),
                    record_count: del.deletes.len() as i64,
                    file_size_in_bytes: 512,
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
                },
            });
        }
        let delete_manifest_path = format!("{table_location}/metadata/deletes-m0.avro");
        let delete_bytes = write_manifest(&ManifestWriteParams {
            format_version,
            content: ManifestContentType::Deletes,
            schema_json: &schema_json,
            schema_id: Some(0),
            partition_spec_id: 0,
            partition_fields: &spec.fields,
            partition_types: &types,
            entries: &delete_entries,
        })
        .expect("write delete manifest");
        store.put(delete_manifest_path.clone(), delete_bytes.clone());
        manifest_files.push(manifest_file(
            &delete_manifest_path,
            &delete_bytes,
            ManifestContentType::Deletes,
            &delete_entries,
            snapshot_id,
            sequence_number,
        ));
    }

    // Manifest list.
    let list_path = format!("{table_location}/metadata/snap-{snapshot_id}-1-list.avro");
    let list_bytes = write_manifest_list(&ManifestListWriteParams {
        format_version,
        snapshot_id,
        parent_snapshot_id: None,
        sequence_number: (format_version >= 2).then_some(sequence_number),
        manifests: &manifest_files,
    })
    .expect("write list");
    store.put(list_path.clone(), list_bytes);

    base_metadata(
        table_location,
        format_version,
        snapshot_id,
        sequence_number,
        &list_path,
    )
}

fn manifest_file(
    path: &str,
    bytes: &[u8],
    content: ManifestContentType,
    entries: &[ManifestEntry],
    snapshot_id: i64,
    sequence_number: i64,
) -> ManifestFile {
    let added = entries.len() as i32;
    let rows: i64 = entries.iter().map(|e| e.data_file.record_count).sum();
    ManifestFile {
        manifest_path: path.to_owned(),
        manifest_length: bytes.len() as i64,
        partition_spec_id: 0,
        content,
        sequence_number,
        min_sequence_number: sequence_number,
        added_snapshot_id: snapshot_id,
        added_files_count: Some(added),
        existing_files_count: Some(0),
        deleted_files_count: Some(0),
        added_rows_count: Some(rows),
        existing_rows_count: Some(0),
        deleted_rows_count: Some(0),
        partitions: None,
        key_metadata: None,
        first_row_id: None,
    }
}

/// Base table metadata whose current snapshot points at `list_path`.
pub fn base_metadata(
    table_location: &str,
    format_version: u8,
    snapshot_id: i64,
    sequence_number: i64,
    list_path: &str,
) -> TableMetadata {
    let schema = orders_schema();
    let spec = category_spec();
    let mut summary = BTreeMap::new();
    summary.insert("operation".to_owned(), "append".to_owned());
    let snapshot = meridian_iceberg::spec::Snapshot {
        snapshot_id,
        parent_snapshot_id: None,
        sequence_number: (format_version >= 2).then_some(sequence_number),
        timestamp_ms: 1_700_000_000_000,
        manifest_list: Some(list_path.to_owned()),
        summary: Some(summary),
        schema_id: Some(0),
        first_row_id: None,
        added_rows: None,
        extra: serde_json::Map::new(),
    };
    let mut refs = BTreeMap::new();
    refs.insert(
        "main".to_owned(),
        meridian_iceberg::spec::SnapshotRef {
            snapshot_id,
            ref_type: meridian_iceberg::spec::RefType::Branch,
            min_snapshots_to_keep: None,
            max_snapshot_age_ms: None,
            max_ref_age_ms: None,
            extra: serde_json::Map::new(),
        },
    );
    TableMetadata {
        format_version,
        table_uuid: uuid::Uuid::from_u128(0x1234_5678_9abc_def0),
        location: table_location.to_owned(),
        last_sequence_number: (format_version >= 2).then_some(sequence_number),
        next_row_id: None,
        last_updated_ms: 1_700_000_000_000,
        last_column_id: 3,
        schemas: vec![schema],
        current_schema_id: 0,
        partition_specs: vec![spec],
        default_spec_id: 0,
        last_partition_id: 1000,
        sort_orders: vec![meridian_iceberg::spec::SortOrder::unsorted()],
        default_sort_order_id: 0,
        properties: None,
        current_snapshot_id: Some(snapshot_id),
        snapshots: Some(vec![snapshot]),
        snapshot_log: None,
        metadata_log: None,
        refs: Some(refs),
        statistics: None,
        partition_statistics: None,
        encryption_keys: None,
        extra: serde_json::Map::new(),
    }
}
