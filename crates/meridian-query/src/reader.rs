//! Reading Iceberg data files into Arrow batches, mapped by field id, with
//! merge-on-read deletes applied — the bytes `DataFusion` actually queries.
//!
//! The rules here are the same ones `meridian-executor`'s rewrite pipeline
//! proved out, narrowed to the read path:
//!
//! - **Columns map by Iceberg field id, never by name or position.** Each
//!   Parquet column carries its field id in `PARQUET:field_id` metadata; we
//!   read that and place the column at the position its id occupies in the
//!   table's current schema. A renamed column keeps its id, so it lands
//!   correctly; a physically reordered file merges correctly.
//! - **Schema evolution synthesizes nulls.** A file written before a column was
//!   added simply lacks that field id; we materialize a null column of the
//!   target type for it.
//! - **Merge-on-read deletes are materialized.** Position-delete files remove
//!   rows by `(file_path, pos)`; equality-delete files remove rows whose values
//!   over the delete's equality columns match, for strictly-older sequence
//!   numbers. A governed query must not return a row the table considers
//!   deleted.
//! - **Deletion vectors are refused.** A v3 Puffin deletion vector cannot be
//!   applied with a plain Parquet reader, so a data file carrying one is
//!   refused with a clear reason rather than returning its (possibly deleted)
//!   rows.
//!
//! The output is one [`MemTable`] per table, built from the realigned,
//! delete-filtered batches, registered into the `DataFusion` context under a
//! private name (the governed view sits on top — see [`crate::policy`]).

use std::collections::HashMap;
use std::sync::Arc;

use arrow_array::cast::AsArray;
use arrow_array::{Array, ArrayRef, BooleanArray, RecordBatch, StringArray, new_null_array};
use arrow_schema::{DataType, Field, Schema as ArrowSchema, TimeUnit};
use datafusion::datasource::MemTable;
use meridian_iceberg::spec::{PrimitiveType, Schema as IcebergSchema, Type};
use parquet::arrow::PARQUET_FIELD_ID_META_KEY;
use parquet::arrow::arrow_reader::ParquetRecordBatchReaderBuilder;

use crate::catalog::TableScan;
use crate::error::{QueryError, QueryResult};

/// Reserved field id of the `file_path` column in position-delete files.
const POS_DELETE_FILE_PATH_ID: i32 = 2_147_483_546;
/// Reserved field id of the `pos` column in position-delete files.
const POS_DELETE_POS_ID: i32 = 2_147_483_545;

/// Builds the `DataFusion` in-memory table for one Iceberg table scan: reads its
/// live data files to Arrow, realigns each to the target schema by field id,
/// applies deletes, and packs the batches into a [`MemTable`].
///
/// Returns `(table, arrow_schema)`. An empty scan (no data files) still yields a
/// table with the correct schema and zero rows, so a query over an empty table
/// plans and returns no rows rather than failing.
pub(crate) async fn build_table(
    scan: &TableScan<'_>,
) -> QueryResult<(Arc<MemTable>, Arc<ArrowSchema>)> {
    let target = Arc::new(iceberg_to_arrow_schema(scan.schema, scan.name)?);

    let mut batches: Vec<RecordBatch> = Vec::with_capacity(scan.plan.data_files.len());
    for planned in &scan.plan.data_files {
        // A v3 deletion vector cannot be materialized here.
        for &di in &planned.delete_indices {
            let del = &scan.plan.deletes[di];
            if del.file.content_offset.is_some() {
                return Err(QueryError::DeletionVectorUnsupported {
                    table: scan.name.to_owned(),
                    location: planned.file.file_path.clone(),
                });
            }
        }

        let raw = scan.bytes.read(&planned.file.file_path).await?;
        let (batch, file_ids) = read_data_batch(&planned.file.file_path, &raw, &target)?;

        // Reject any field id present in the file but absent from the schema,
        // rather than silently dropping a column.
        for id in file_ids.keys() {
            if !target_has_field_id(&target, *id) {
                return Err(QueryError::UnmappableField {
                    table: scan.name.to_owned(),
                    location: planned.file.file_path.clone(),
                    field_id: *id,
                });
            }
        }

        let aligned = realign_by_field_id(&target, &batch, &file_ids, &planned.file.file_path)?;
        let filtered = apply_deletes(scan, planned, &aligned).await?;
        if filtered.num_rows() > 0 {
            batches.push(filtered);
        }
    }

    let table = MemTable::try_new(target.clone(), vec![batches])
        .map_err(|e| QueryError::engine("register table", e))?;
    Ok((Arc::new(table), target))
}

/// Reads a Parquet data file to a single concatenated batch, plus a map of
/// field id -> column index within that file. Empty files yield an empty batch.
fn read_data_batch(
    location: &str,
    bytes: &bytes::Bytes,
    _target: &ArrowSchema,
) -> QueryResult<(RecordBatch, HashMap<i32, usize>)> {
    let builder = ParquetRecordBatchReaderBuilder::try_new(bytes.clone())
        .map_err(|e| QueryError::engine("read parquet", format!("{location}: {e}")))?;
    let file_schema = builder.schema().clone();
    let file_ids = field_id_index(&file_schema);
    let reader = builder
        .build()
        .map_err(|e| QueryError::engine("read parquet", format!("{location}: {e}")))?;

    let mut read: Vec<RecordBatch> = Vec::new();
    for b in reader {
        read.push(b.map_err(|e| QueryError::engine("read parquet", format!("{location}: {e}")))?);
    }
    let batch = if read.is_empty() {
        RecordBatch::new_empty(file_schema)
    } else {
        arrow_select_concat(&read)
            .map_err(|e| QueryError::engine("read parquet", format!("{location}: {e}")))?
    };
    Ok((batch, file_ids))
}

/// Concatenates batches of one schema into a single batch.
fn arrow_select_concat(batches: &[RecordBatch]) -> Result<RecordBatch, arrow_schema::ArrowError> {
    // arrow's concat over record batches; all share the reader's schema.
    let schema = batches[0].schema();
    let mut cols: Vec<ArrayRef> = Vec::with_capacity(schema.fields().len());
    for i in 0..schema.fields().len() {
        let arrays: Vec<&dyn Array> = batches.iter().map(|b| b.column(i).as_ref()).collect();
        cols.push(arrow_select::concat::concat(&arrays)?);
    }
    RecordBatch::try_new(schema, cols)
}

/// Realigns a file's batch to the target schema by field id: for each target
/// column, take the source column with the matching id (adapting nullability),
/// or synthesize a null column of the target type when the file predates it.
fn realign_by_field_id(
    target: &Arc<ArrowSchema>,
    batch: &RecordBatch,
    file_ids: &HashMap<i32, usize>,
    location: &str,
) -> QueryResult<RecordBatch> {
    let rows = batch.num_rows();
    let mut columns: Vec<ArrayRef> = Vec::with_capacity(target.fields().len());
    for field in target.fields() {
        let id = field_id_of(field);
        match id.and_then(|id| file_ids.get(&id)) {
            Some(&idx) => {
                let src = batch.column(idx);
                if src.data_type() == field.data_type() {
                    columns.push(Arc::clone(src));
                } else {
                    // Types should match for the primitives we map; if a writer
                    // used a compatible-but-different physical type, fail loudly
                    // rather than return misread values.
                    return Err(QueryError::engine(
                        "read parquet",
                        format!(
                            "{location}: column {} has physical type {:?} but the schema expects \
                             {:?}",
                            field.name(),
                            src.data_type(),
                            field.data_type()
                        ),
                    ));
                }
            }
            None => columns.push(new_null_array(field.data_type(), rows)),
        }
    }
    RecordBatch::try_new(Arc::clone(target), columns)
        .map_err(|e| QueryError::engine("read parquet", format!("{location}: {e}")))
}

/// Applies the position/equality deletes attached to a data file to its aligned
/// batch, returning the surviving rows.
async fn apply_deletes(
    scan: &TableScan<'_>,
    planned: &crate::catalog::PlannedDataFile,
    batch: &RecordBatch,
) -> QueryResult<RecordBatch> {
    if planned.delete_indices.is_empty() || batch.num_rows() == 0 {
        return Ok(batch.clone());
    }
    let mut keep = vec![true; batch.num_rows()];

    // Field id -> column index in the aligned batch, for equality matching.
    let arrow_schema = batch.schema();
    let index = field_id_index(&arrow_schema);

    for &di in &planned.delete_indices {
        let del = &scan.plan.deletes[di];
        match del.file.content {
            meridian_iceberg::manifest::DataFileContent::PositionDeletes => {
                apply_position_delete(scan, &planned.file.file_path, del, &mut keep).await?;
            }
            meridian_iceberg::manifest::DataFileContent::EqualityDeletes => {
                apply_equality_delete(scan, del, batch, &index, &mut keep).await?;
            }
            meridian_iceberg::manifest::DataFileContent::Data => {}
        }
    }

    let mask = BooleanArray::from(keep);
    arrow_select::filter::filter_record_batch(batch, &mask).map_err(|e| {
        QueryError::engine("apply deletes", format!("{}: {e}", planned.file.file_path))
    })
}

/// Marks positions deleted by a position-delete file for the given data file.
async fn apply_position_delete(
    scan: &TableScan<'_>,
    data_path: &str,
    delete: &crate::catalog::PlannedDelete,
    keep: &mut [bool],
) -> QueryResult<()> {
    let raw = scan.bytes.read(&delete.file.file_path).await?;
    let (batch, ids) = read_data_batch(&delete.file.file_path, &raw, &ArrowSchema::empty())?;
    if batch.num_rows() == 0 {
        return Ok(());
    }
    let path_col = ids.get(&POS_DELETE_FILE_PATH_ID).copied();
    let pos_col = ids.get(&POS_DELETE_POS_ID).copied();
    let (Some(path_col), Some(pos_col)) = (path_col, pos_col) else {
        return Err(QueryError::engine(
            "apply deletes",
            format!(
                "{}: position-delete file lacks file_path/pos columns",
                delete.file.file_path
            ),
        ));
    };
    let paths = batch
        .column(path_col)
        .as_any()
        .downcast_ref::<StringArray>()
        .ok_or_else(|| {
            QueryError::engine(
                "apply deletes",
                format!(
                    "{}: file_path is not a string column",
                    delete.file.file_path
                ),
            )
        })?;
    let positions = position_column(&delete.file.file_path, batch.column(pos_col))?;

    for (row, &pos) in positions.iter().enumerate() {
        if paths.is_valid(row)
            && paths.value(row) == data_path
            && let Ok(idx) = usize::try_from(pos)
            && idx < keep.len()
        {
            keep[idx] = false;
        }
    }
    Ok(())
}

/// Extracts the `pos` column as `u64`, tolerating `Int64` or `UInt64`.
fn position_column(location: &str, array: &ArrayRef) -> QueryResult<Vec<u64>> {
    match array.data_type() {
        DataType::UInt64 => Ok(array
            .as_primitive::<arrow_array::types::UInt64Type>()
            .values()
            .to_vec()),
        DataType::Int64 => Ok(array
            .as_primitive::<arrow_array::types::Int64Type>()
            .values()
            .iter()
            .map(|&v| u64::try_from(v).unwrap_or(0))
            .collect()),
        other => Err(QueryError::engine(
            "apply deletes",
            format!("{location}: position column has unexpected type {other:?}"),
        )),
    }
}

/// Marks rows of the data batch that match any tuple of an equality-delete file
/// (over the delete's equality columns) for removal.
///
/// Matching is by a rendered value key per row over the equality columns. This
/// is exact for the non-null primitive values engines write to equality-delete
/// columns. It does *not* implement Iceberg's special "a null equality value
/// matches only null" three-valued rule — a null in an equality column is keyed
/// as a sentinel and so a null delete tuple matches a null data tuple. Equality
/// deletes over nullable columns are rare in practice (writers key on
/// non-nullable identity columns); when precise null semantics matter, route the
/// query to a registered engine.
async fn apply_equality_delete(
    scan: &TableScan<'_>,
    delete: &crate::catalog::PlannedDelete,
    data_batch: &RecordBatch,
    data_index: &HashMap<i32, usize>,
    keep: &mut [bool],
) -> QueryResult<()> {
    let Some(eq_ids) = delete.file.equality_ids.clone() else {
        return Err(QueryError::engine(
            "apply deletes",
            format!(
                "{}: equality-delete file has no equality field ids",
                delete.file.file_path
            ),
        ));
    };
    let raw = scan.bytes.read(&delete.file.file_path).await?;
    let (del_batch, del_index) =
        read_data_batch(&delete.file.file_path, &raw, &ArrowSchema::empty())?;
    if del_batch.num_rows() == 0 {
        return Ok(());
    }

    // Build a comparable key per delete row and per data row over the equality
    // columns, then mark matches. Keys use each value's Arrow display, which is
    // stable and total for the primitive types equality deletes cover.
    let del_keys = row_keys(&del_batch, &eq_ids, &del_index, &delete.file.file_path)?;
    let data_keys = row_keys(data_batch, &eq_ids, data_index, &delete.file.file_path)?;
    let del_set: std::collections::HashSet<&str> = del_keys.iter().map(String::as_str).collect();
    for (row, key) in data_keys.iter().enumerate() {
        if del_set.contains(key.as_str()) {
            keep[row] = false;
        }
    }
    Ok(())
}

/// Builds a comparable string key per row over the given equality field ids.
fn row_keys(
    batch: &RecordBatch,
    eq_ids: &[i32],
    index: &HashMap<i32, usize>,
    location: &str,
) -> QueryResult<Vec<String>> {
    use std::fmt::Write as _;
    let cols: Vec<usize> = eq_ids
        .iter()
        .map(|id| {
            index.get(id).copied().ok_or_else(|| {
                QueryError::engine(
                    "apply deletes",
                    format!("{location}: equality column id {id} not present in a batch"),
                )
            })
        })
        .collect::<QueryResult<_>>()?;

    let formatters: Vec<_> = cols
        .iter()
        .map(|&c| arrow_json_value_formatter(batch.column(c)))
        .collect::<QueryResult<_>>()?;

    let mut keys = Vec::with_capacity(batch.num_rows());
    for row in 0..batch.num_rows() {
        let mut key = String::new();
        for f in &formatters {
            let _ = write!(key, "{}\u{1f}", f(row));
        }
        keys.push(key);
    }
    Ok(keys)
}

/// A per-row string renderer for a column, for equality-delete key building.
/// Uses arrow's display formatter, which is total for primitives.
fn arrow_json_value_formatter(array: &ArrayRef) -> QueryResult<Box<dyn Fn(usize) -> String + '_>> {
    let options = arrow_cast::display::FormatOptions::default().with_null("\u{0}NULL");
    let formatter = arrow_cast::display::ArrayFormatter::try_new(array.as_ref(), &options)
        .map_err(|e| QueryError::engine("apply deletes", format!("format column: {e}")))?;
    Ok(Box::new(move |row: usize| formatter.value(row).to_string()))
}

/// Whether the target Arrow schema has a field with the given Iceberg field id.
fn target_has_field_id(schema: &ArrowSchema, id: i32) -> bool {
    schema.fields().iter().any(|f| field_id_of(f) == Some(id))
}

/// The field id an Arrow field carries in its `PARQUET:field_id` metadata.
fn field_id_of(field: &Field) -> Option<i32> {
    field
        .metadata()
        .get(PARQUET_FIELD_ID_META_KEY)
        .and_then(|s| s.trim().parse().ok())
}

/// Field id -> column index within an Arrow schema. Columns without a field id
/// are skipped (they cannot be mapped).
fn field_id_index(schema: &ArrowSchema) -> HashMap<i32, usize> {
    let mut map = HashMap::new();
    for (idx, field) in schema.fields().iter().enumerate() {
        if let Some(id) = field_id_of(field) {
            map.insert(id, idx);
        }
    }
    map
}

/// Builds the Arrow schema for a table from its Iceberg current schema, tagging
/// each field with its Iceberg field id (so the reader and the delete path can
/// map by id). Only top-level columns are mapped; nested types ride along as
/// their Arrow representation.
fn iceberg_to_arrow_schema(schema: &IcebergSchema, table: &str) -> QueryResult<ArrowSchema> {
    let mut fields: Vec<Field> = Vec::with_capacity(schema.fields.len());
    for f in &schema.fields {
        let dt = iceberg_type_to_arrow(&f.field_type).ok_or_else(|| QueryError::UnqueryableTable {
            table: table.to_owned(),
            reason: format!(
                "column {:?} has type {:?}, which the small-scan executor does not map to Arrow",
                f.name, f.field_type
            ),
        })?;
        let mut md = HashMap::new();
        md.insert(PARQUET_FIELD_ID_META_KEY.to_string(), f.id.to_string());
        // Iceberg required -> Arrow non-nullable; but we read possibly-evolved
        // files that may synthesize nulls, so keep columns nullable to be safe.
        fields.push(Field::new(&f.name, dt, true).with_metadata(md));
    }
    Ok(ArrowSchema::new(fields))
}

/// Maps an Iceberg type to an Arrow `DataType`. Covers the primitive types a
/// small scan reads; nested and exotic types return `None` (the caller refuses
/// the query with a clear reason rather than guessing).
fn iceberg_type_to_arrow(ty: &Type) -> Option<DataType> {
    let Type::Primitive(p) = ty else {
        return None;
    };
    Some(match p {
        PrimitiveType::Boolean => DataType::Boolean,
        PrimitiveType::Int => DataType::Int32,
        PrimitiveType::Long => DataType::Int64,
        PrimitiveType::Float => DataType::Float32,
        PrimitiveType::Double => DataType::Float64,
        PrimitiveType::Decimal { precision, scale } => {
            DataType::Decimal128(u8::try_from(*precision).ok()?, i8::try_from(*scale).ok()?)
        }
        PrimitiveType::Date => DataType::Date32,
        PrimitiveType::Time => DataType::Time64(TimeUnit::Microsecond),
        PrimitiveType::Timestamp => DataType::Timestamp(TimeUnit::Microsecond, None),
        PrimitiveType::Timestamptz => {
            DataType::Timestamp(TimeUnit::Microsecond, Some("UTC".into()))
        }
        PrimitiveType::TimestampNs => DataType::Timestamp(TimeUnit::Nanosecond, None),
        PrimitiveType::TimestamptzNs => {
            DataType::Timestamp(TimeUnit::Nanosecond, Some("UTC".into()))
        }
        PrimitiveType::String => DataType::Utf8,
        PrimitiveType::Uuid => DataType::FixedSizeBinary(16),
        PrimitiveType::Fixed(n) => DataType::FixedSizeBinary(i32::try_from(*n).ok()?),
        PrimitiveType::Binary => DataType::Binary,
        // Types without a straightforward small-scan Arrow mapping.
        PrimitiveType::Variant
        | PrimitiveType::Geometry { .. }
        | PrimitiveType::Geography { .. }
        | PrimitiveType::Unknown
        | PrimitiveType::Other(_) => return None,
    })
}
