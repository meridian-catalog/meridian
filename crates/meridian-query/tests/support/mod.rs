//! Shared test scaffolding: build real Iceberg fixtures (Parquet data files +
//! manifest/manifest-list Avro) in an in-memory store, so the query executor
//! runs its production read path against genuine metadata.
//!
//! The Parquet is written with the `parquet` crate carrying Iceberg
//! `PARQUET:field_id` metadata on every column, and the manifests with
//! `meridian_iceberg::manifest`, exactly like `meridian-executor`'s test
//! fixtures — the executor reads the same Avro/Parquet it will in the field.
//!
//! Compiled into every integration-test binary; each uses only part of it, so
//! the usual dead-code / pedantic-fixture lints are relaxed here (the crate's
//! `src/` stays strict).
#![allow(
    dead_code,
    unreachable_pub,
    clippy::cast_possible_wrap,
    clippy::cast_possible_truncation,
    clippy::too_many_lines,
    clippy::doc_markdown,
    clippy::unnecessary_wraps,
    clippy::missing_panics_doc,
    // `root_uri` returns a `'static` literal but the trait signature elides the
    // lifetime to `&self`; the lint fires on the impl, not our code.
    clippy::unnecessary_literal_bound
)]

use std::collections::BTreeMap;
use std::sync::Arc;
use std::sync::Mutex;

use arrow_array::cast::AsArray;
use arrow_array::{ArrayRef, Int64Array, RecordBatch, StringArray, UInt64Array};
use arrow_schema::{DataType, Field, Schema as ArrowSchema};
use bytes::Bytes;
use meridian_iceberg::manifest::{
    DataFile, DataFileContent, ManifestContentType, ManifestEntry, ManifestEntryStatus,
    ManifestFile, ManifestListWriteParams, ManifestWriteParams, PartitionFieldType, PartitionTuple,
    PartitionValue, partition_field_types, write_manifest, write_manifest_list,
};
use meridian_iceberg::spec::{
    PartitionField, PartitionSpec, PrimitiveType, Schema, StructField, TableMetadata, Transform,
    Type,
};
use meridian_iceberg::value::Datum;
use parquet::arrow::arrow_reader::ParquetRecordBatchReaderBuilder;
use parquet::arrow::{ArrowWriter, PARQUET_FIELD_ID_META_KEY};

/// Reserved field id of the `file_path` column in position-delete files.
pub const POS_DELETE_FILE_PATH_ID: i32 = 2_147_483_546;
/// Reserved field id of the `pos` column in position-delete files.
pub const POS_DELETE_POS_ID: i32 = 2_147_483_545;

/// An in-memory `meridian_storage::Storage`: reads return what was `put`.
/// Only `read` is real; the rest are unreachable in these tests.
#[derive(Debug, Default)]
pub struct MemStorage {
    objects: Mutex<BTreeMap<String, Bytes>>,
}

impl MemStorage {
    pub fn put(&self, location: impl Into<String>, bytes: impl Into<Bytes>) {
        self.objects
            .lock()
            .expect("lock")
            .insert(location.into(), bytes.into());
    }
}

#[async_trait::async_trait]
impl meridian_storage::Storage for MemStorage {
    fn root_uri(&self) -> &str {
        "mem://test"
    }

    async fn read(&self, location: &str) -> meridian_storage::StorageResult<Bytes> {
        self.objects
            .lock()
            .expect("lock")
            .get(location)
            .cloned()
            .ok_or(meridian_storage::StorageError::NotFound {
                location: location.to_owned(),
            })
    }

    async fn write(&self, location: &str, bytes: Bytes) -> meridian_storage::StorageResult<()> {
        self.put(location, bytes);
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
        Ok(self.objects.lock().expect("lock").contains_key(location))
    }

    async fn delete(&self, _location: &str) -> meridian_storage::StorageResult<()> {
        unreachable!("query executor never deletes")
    }

    async fn delete_prefix(&self, _prefix: &str) -> meridian_storage::StorageResult<()> {
        unreachable!("query executor never deletes")
    }

    async fn list(
        &self,
        _prefix: &str,
    ) -> meridian_storage::StorageResult<meridian_storage::ObjectStream> {
        unreachable!("query executor never lists")
    }
}

/// The orders schema used across tests: id (long, field 1), email (string, 2),
/// region (string, 3), amount (long, 4). Partitioned by identity(region).
pub fn orders_schema() -> Schema {
    Schema::new(vec![
        StructField::optional(1, "id", Type::Primitive(PrimitiveType::Long)),
        StructField::optional(2, "email", Type::Primitive(PrimitiveType::String)),
        StructField::optional(3, "region", Type::Primitive(PrimitiveType::String)),
        StructField::optional(4, "amount", Type::Primitive(PrimitiveType::Long)),
    ])
    .with_schema_id(0)
}

/// The identity(region) partition spec (spec id 0, field id 1000).
pub fn region_spec() -> PartitionSpec {
    let mut field = PartitionField::new(3, "region", Transform::Identity);
    field.field_id = Some(1000);
    let mut spec = PartitionSpec::new(vec![field]);
    spec.spec_id = Some(0);
    spec
}

/// One row of the orders table.
#[derive(Debug, Clone)]
pub struct Row {
    pub id: i64,
    pub email: String,
    pub region: String,
    pub amount: i64,
}

impl Row {
    pub fn new(id: i64, email: &str, region: &str, amount: i64) -> Self {
        Self {
            id,
            email: email.to_owned(),
            region: region.to_owned(),
            amount,
        }
    }
}

fn arrow_field(name: &str, dt: DataType, field_id: i32) -> Field {
    let mut md = std::collections::HashMap::new();
    md.insert(PARQUET_FIELD_ID_META_KEY.to_string(), field_id.to_string());
    Field::new(name, dt, true).with_metadata(md)
}

fn orders_arrow_schema() -> Arc<ArrowSchema> {
    Arc::new(ArrowSchema::new(vec![
        arrow_field("id", DataType::Int64, 1),
        arrow_field("email", DataType::Utf8, 2),
        arrow_field("region", DataType::Utf8, 3),
        arrow_field("amount", DataType::Int64, 4),
    ]))
}

/// Writes rows to Parquet bytes (orders schema, field ids preserved).
pub fn write_orders_parquet(rows: &[Row]) -> Bytes {
    let schema = orders_arrow_schema();
    let ids: ArrayRef = Arc::new(Int64Array::from(
        rows.iter().map(|r| r.id).collect::<Vec<_>>(),
    ));
    let emails: ArrayRef = Arc::new(StringArray::from(
        rows.iter().map(|r| r.email.clone()).collect::<Vec<_>>(),
    ));
    let regions: ArrayRef = Arc::new(StringArray::from(
        rows.iter().map(|r| r.region.clone()).collect::<Vec<_>>(),
    ));
    let amounts: ArrayRef = Arc::new(Int64Array::from(
        rows.iter().map(|r| r.amount).collect::<Vec<_>>(),
    ));
    let batch =
        RecordBatch::try_new(schema.clone(), vec![ids, emails, regions, amounts]).expect("batch");
    let mut buf: Vec<u8> = Vec::new();
    let mut w = ArrowWriter::try_new(&mut buf, schema, None).expect("writer");
    w.write(&batch).expect("write");
    w.close().expect("close");
    Bytes::from(buf)
}

/// Writes the same rows with the physical columns *reversed* (amount, region,
/// email, id), field ids still correct — proves the executor maps by field id.
pub fn write_orders_parquet_reversed(rows: &[Row]) -> Bytes {
    let schema = Arc::new(ArrowSchema::new(vec![
        arrow_field("amount", DataType::Int64, 4),
        arrow_field("region", DataType::Utf8, 3),
        arrow_field("email", DataType::Utf8, 2),
        arrow_field("id", DataType::Int64, 1),
    ]));
    let amounts: ArrayRef = Arc::new(Int64Array::from(
        rows.iter().map(|r| r.amount).collect::<Vec<_>>(),
    ));
    let regions: ArrayRef = Arc::new(StringArray::from(
        rows.iter().map(|r| r.region.clone()).collect::<Vec<_>>(),
    ));
    let emails: ArrayRef = Arc::new(StringArray::from(
        rows.iter().map(|r| r.email.clone()).collect::<Vec<_>>(),
    ));
    let ids: ArrayRef = Arc::new(Int64Array::from(
        rows.iter().map(|r| r.id).collect::<Vec<_>>(),
    ));
    let batch =
        RecordBatch::try_new(schema.clone(), vec![amounts, regions, emails, ids]).expect("batch");
    let mut buf: Vec<u8> = Vec::new();
    let mut w = ArrowWriter::try_new(&mut buf, schema, None).expect("writer");
    w.write(&batch).expect("write");
    w.close().expect("close");
    Bytes::from(buf)
}

/// Writes an older-schema file predating the `amount` column (id, email,
/// region only). On read the executor synthesizes a null `amount`.
pub fn write_orders_parquet_no_amount(rows: &[Row]) -> Bytes {
    let schema = Arc::new(ArrowSchema::new(vec![
        arrow_field("id", DataType::Int64, 1),
        arrow_field("email", DataType::Utf8, 2),
        arrow_field("region", DataType::Utf8, 3),
    ]));
    let ids: ArrayRef = Arc::new(Int64Array::from(
        rows.iter().map(|r| r.id).collect::<Vec<_>>(),
    ));
    let emails: ArrayRef = Arc::new(StringArray::from(
        rows.iter().map(|r| r.email.clone()).collect::<Vec<_>>(),
    ));
    let regions: ArrayRef = Arc::new(StringArray::from(
        rows.iter().map(|r| r.region.clone()).collect::<Vec<_>>(),
    ));
    let batch = RecordBatch::try_new(schema.clone(), vec![ids, emails, regions]).expect("batch");
    let mut buf: Vec<u8> = Vec::new();
    let mut w = ArrowWriter::try_new(&mut buf, schema, None).expect("writer");
    w.write(&batch).expect("write");
    w.close().expect("close");
    Bytes::from(buf)
}

/// Writes an equality-delete Parquet file keyed on the `id` column (field 1):
/// any data row whose id appears here is deleted.
pub fn write_equality_delete_parquet(ids_to_delete: &[i64]) -> Bytes {
    let schema = Arc::new(ArrowSchema::new(vec![arrow_field(
        "id",
        DataType::Int64,
        1,
    )]));
    let ids: ArrayRef = Arc::new(Int64Array::from(ids_to_delete.to_vec()));
    let batch = RecordBatch::try_new(schema.clone(), vec![ids]).expect("batch");
    let mut buf: Vec<u8> = Vec::new();
    let mut w = ArrowWriter::try_new(&mut buf, schema, None).expect("writer");
    w.write(&batch).expect("write");
    w.close().expect("close");
    Bytes::from(buf)
}

/// Writes a position-delete Parquet file (reserved field ids on file_path/pos).
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

/// Reads an orders Parquet buffer back to rows (by field id), for assertions on
/// compacted/round-tripped output when a test needs it.
pub fn read_orders_parquet(bytes: &Bytes) -> Vec<Row> {
    let builder = ParquetRecordBatchReaderBuilder::try_new(bytes.clone()).expect("reader");
    let schema = builder.schema().clone();
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
            .unwrap_or_else(|| panic!("missing field id {field_id}"))
    };
    let (id_c, em_c, rg_c, am_c) = (col_of(1), col_of(2), col_of(3), col_of(4));
    let reader = builder.build().expect("build");
    let mut rows = Vec::new();
    for batch in reader {
        let batch = batch.expect("batch");
        let ids = batch
            .column(id_c)
            .as_primitive::<arrow_array::types::Int64Type>();
        let ems = batch.column(em_c).as_string::<i32>();
        let rgs = batch.column(rg_c).as_string::<i32>();
        let ams = batch
            .column(am_c)
            .as_primitive::<arrow_array::types::Int64Type>();
        for i in 0..batch.num_rows() {
            rows.push(Row {
                id: ids.value(i),
                email: ems.value(i).to_owned(),
                region: rgs.value(i).to_owned(),
                amount: ams.value(i),
            });
        }
    }
    rows
}

/// Physical layout of a fixture data file.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub enum Layout {
    /// Columns in schema order.
    #[default]
    Normal,
    /// Columns physically reversed (field ids still correct).
    Reversed,
    /// Older schema predating `amount` (id, email, region only).
    NoAmount,
}

/// A single-partition data file for building fixtures.
pub struct DataFileSpec {
    pub path: String,
    pub region: String,
    pub rows: Vec<Row>,
    pub size_bytes: i64,
    pub sequence_number: i64,
    pub snapshot_id: i64,
    pub layout: Layout,
}

impl DataFileSpec {
    pub fn new(path: &str, region: &str, rows: Vec<Row>, size_bytes: i64) -> Self {
        Self {
            path: path.to_owned(),
            region: region.to_owned(),
            rows,
            size_bytes,
            sequence_number: 1,
            snapshot_id: 1,
            layout: Layout::Normal,
        }
    }

    pub fn with_layout(mut self, layout: Layout) -> Self {
        self.layout = layout;
        self
    }

    fn parquet_bytes(&self) -> Bytes {
        match self.layout {
            Layout::Normal => write_orders_parquet(&self.rows),
            Layout::Reversed => write_orders_parquet_reversed(&self.rows),
            Layout::NoAmount => write_orders_parquet_no_amount(&self.rows),
        }
    }
}

/// A position-delete file for building fixtures.
pub struct PositionDeleteSpec {
    pub path: String,
    pub region: String,
    pub deletes: Vec<(String, u64)>,
    pub sequence_number: i64,
    pub snapshot_id: i64,
}

fn region_tuple(region: &str) -> PartitionTuple {
    PartitionTuple {
        fields: vec![PartitionValue {
            field_id: 1000,
            name: "region".to_owned(),
            value: Some(Datum::String(region.to_owned())),
        }],
    }
}

fn partition_types() -> Vec<PartitionFieldType> {
    partition_field_types(&region_spec().fields, &orders_schema()).expect("types")
}

/// Builds a whole table fixture in `store` and returns the base `TableMetadata`
/// whose current snapshot points at the written manifest list.
pub fn build_fixture(
    store: &MemStorage,
    table_location: &str,
    format_version: u8,
    data_files: &[DataFileSpec],
    delete_files: &[PositionDeleteSpec],
    snapshot_id: i64,
    sequence_number: i64,
) -> TableMetadata {
    let schema = orders_schema();
    let schema_json = serde_json::to_string(&schema).expect("schema json");
    let spec = region_spec();
    let types = partition_types();

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
                partition: region_tuple(&spec_file.region),
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
                    partition: region_tuple(&del.region),
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

/// Base metadata with a current snapshot at `list_path`.
pub fn base_metadata(
    table_location: &str,
    format_version: u8,
    snapshot_id: i64,
    sequence_number: i64,
    list_path: &str,
) -> TableMetadata {
    let schema = orders_schema();
    let spec = region_spec();
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
        last_column_id: 4,
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

/// Builds a fixture with a single data file plus one **equality-delete** file
/// keyed on `id` (field 1). Returns the base metadata. Kept separate from
/// [`build_fixture`] so the common path stays simple.
pub fn build_fixture_with_equality_delete(
    store: &MemStorage,
    table_location: &str,
    data: &DataFileSpec,
    ids_to_delete: &[i64],
    delete_sequence_number: i64,
) -> TableMetadata {
    let schema = orders_schema();
    let schema_json = serde_json::to_string(&schema).expect("schema json");
    let spec = region_spec();
    let types = partition_types();
    let region = data.region.clone();

    store.put(data.path.clone(), data.parquet_bytes());
    let data_entry = ManifestEntry {
        status: ManifestEntryStatus::Added,
        snapshot_id: Some(data.snapshot_id),
        sequence_number: Some(data.sequence_number),
        file_sequence_number: Some(data.sequence_number),
        data_file: DataFile {
            content: DataFileContent::Data,
            file_path: data.path.clone(),
            file_format: "PARQUET".to_owned(),
            partition: region_tuple(&region),
            record_count: data.rows.len() as i64,
            file_size_in_bytes: data.size_bytes,
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
    };
    let data_manifest_path = format!("{table_location}/metadata/data-m0.avro");
    let data_bytes = write_manifest(&ManifestWriteParams {
        format_version: 2,
        content: ManifestContentType::Data,
        schema_json: &schema_json,
        schema_id: Some(0),
        partition_spec_id: 0,
        partition_fields: &spec.fields,
        partition_types: &types,
        entries: std::slice::from_ref(&data_entry),
    })
    .expect("write data manifest");
    store.put(data_manifest_path.clone(), data_bytes.clone());

    // Equality-delete file, partition-scoped to the data's region (the fixture
    // keeps all data in one region, so a region-scoped equality delete covers
    // it — exercising the partition-scoped equality-delete attachment path).
    let del_path = format!("{table_location}/data/eq-delete.parquet");
    store.put(
        del_path.clone(),
        write_equality_delete_parquet(ids_to_delete),
    );
    let del_entry = ManifestEntry {
        status: ManifestEntryStatus::Added,
        snapshot_id: Some(1),
        sequence_number: Some(delete_sequence_number),
        file_sequence_number: Some(delete_sequence_number),
        data_file: DataFile {
            content: DataFileContent::EqualityDeletes,
            file_path: del_path.clone(),
            file_format: "PARQUET".to_owned(),
            partition: region_tuple(&region),
            record_count: ids_to_delete.len() as i64,
            file_size_in_bytes: 256,
            column_sizes: None,
            value_counts: None,
            null_value_counts: None,
            nan_value_counts: None,
            lower_bounds: None,
            upper_bounds: None,
            key_metadata: None,
            split_offsets: None,
            equality_ids: Some(vec![1]),
            sort_order_id: None,
            first_row_id: None,
            referenced_data_file: None,
            content_offset: None,
            content_size_in_bytes: None,
        },
    };
    let del_manifest_path = format!("{table_location}/metadata/eq-deletes-m0.avro");
    let del_bytes = write_manifest(&ManifestWriteParams {
        format_version: 2,
        content: ManifestContentType::Deletes,
        schema_json: &schema_json,
        schema_id: Some(0),
        partition_spec_id: 0,
        partition_fields: &spec.fields,
        partition_types: &types,
        entries: std::slice::from_ref(&del_entry),
    })
    .expect("write delete manifest");
    store.put(del_manifest_path.clone(), del_bytes.clone());

    let manifests = vec![
        manifest_file(
            &data_manifest_path,
            &data_bytes,
            ManifestContentType::Data,
            std::slice::from_ref(&data_entry),
            1,
            delete_sequence_number,
        ),
        manifest_file(
            &del_manifest_path,
            &del_bytes,
            ManifestContentType::Deletes,
            std::slice::from_ref(&del_entry),
            1,
            delete_sequence_number,
        ),
    ];
    let list_path = format!("{table_location}/metadata/snap-1-1-list.avro");
    let list_bytes = write_manifest_list(&ManifestListWriteParams {
        format_version: 2,
        snapshot_id: 1,
        parent_snapshot_id: None,
        sequence_number: Some(delete_sequence_number),
        manifests: &manifests,
    })
    .expect("write list");
    store.put(list_path.clone(), list_bytes);

    base_metadata(table_location, 2, 1, delete_sequence_number, &list_path)
}

/// Base metadata for an empty table (no snapshots).
pub fn empty_metadata(table_location: &str) -> TableMetadata {
    let schema = orders_schema();
    let spec = region_spec();
    TableMetadata {
        format_version: 2,
        table_uuid: uuid::Uuid::from_u128(0xabcd),
        location: table_location.to_owned(),
        last_sequence_number: Some(0),
        next_row_id: None,
        last_updated_ms: 1_700_000_000_000,
        last_column_id: 4,
        schemas: vec![schema],
        current_schema_id: 0,
        partition_specs: vec![spec],
        default_spec_id: 0,
        last_partition_id: 1000,
        sort_orders: vec![meridian_iceberg::spec::SortOrder::unsorted()],
        default_sort_order_id: 0,
        properties: None,
        current_snapshot_id: None,
        snapshots: None,
        snapshot_log: None,
        metadata_log: None,
        refs: None,
        statistics: None,
        partition_statistics: None,
        encryption_keys: None,
        extra: serde_json::Map::new(),
    }
}
