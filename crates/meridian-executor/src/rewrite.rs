//! The Parquet rewrite engine: read a bin-pack group's inputs, apply pending
//! deletes, and write one (or a few) target-sized output Parquet file(s).
//!
//! Correctness is the whole job here. The contract, asserted in code:
//!
//! - **Row conservation.** Rows out == rows in, minus rows removed by pending
//!   deletes. Nothing is lost or duplicated. [`RewriteOutcome`] carries the
//!   counts and the caller ([`crate::compact`]) asserts them.
//! - **Field-id fidelity.** Columns are matched between inputs and the target
//!   schema by Iceberg field id, never by name/position, and the field id is
//!   written back into each output column's `PARQUET:field_id` metadata so
//!   engines still resolve columns after the rewrite.
//! - **Schema evolution.** An input written before a column was added simply
//!   lacks that column; the output synthesizes an all-null column for it (the
//!   column must be optional, or the data is inconsistent).
//! - **Delete materialization.** Position deletes (by `(file_path, pos)`) and
//!   equality deletes (by value tuple over the equality columns, strictly
//!   older sequence numbers) are applied as row filters, so the output has no
//!   attached delete files.
//!
//! Deletion **vectors** (v3 Puffin blobs) are out of scope for this first cut
//! and refused up front (the manifest reader preserves their offsets but the
//! writer is v1/v2 only); position-delete *files* and equality-delete files —
//! the v2 merge-on-read shape real engines emit — are fully applied.

use std::collections::{BTreeMap, HashMap, HashSet};
use std::sync::Arc;

use arrow_array::cast::AsArray;
use arrow_array::{Array, ArrayRef, BooleanArray, RecordBatch, StringArray};
use arrow_schema::{DataType, Field, Schema as ArrowSchema};
use arrow_select::concat::concat_batches;
use arrow_select::filter::filter_record_batch;
use bytes::Bytes;
use meridian_iceberg::manifest::{DataFile, DataFileContent};
use meridian_iceberg::spec::Schema as IcebergSchema;
use parquet::arrow::arrow_reader::ParquetRecordBatchReaderBuilder;
use parquet::arrow::{ArrowWriter, PARQUET_FIELD_ID_META_KEY};
use parquet::basic::Compression;
use parquet::file::properties::WriterProperties;

use crate::arrow_schema::{field_id_index, field_id_of, top_level_field_ids, top_level_fields};
use crate::error::{CompactionError, CompactionResult};
use crate::select::{DeleteFile, InputFile};
use crate::stats::compute_bounds;

/// Reserved field id of the `file_path` column in position-delete files
/// (spec "Reserved Field IDs").
const POS_DELETE_FILE_PATH_ID: i32 = 2_147_483_546;
/// Reserved field id of the `pos` column in position-delete files.
const POS_DELETE_POS_ID: i32 = 2_147_483_545;

/// The bytes of one output Parquet file plus the manifest facts describing it.
#[derive(Debug)]
pub struct OutputFile {
    /// The file contents to write to storage.
    pub bytes: Bytes,
    /// Rows in this file.
    pub record_count: i64,
    /// Column id → value count (rows), for the manifest.
    pub value_counts: BTreeMap<i32, i64>,
    /// Column id → null count, for the manifest.
    pub null_value_counts: BTreeMap<i32, i64>,
    /// Column id → Appendix-D lower bound bytes, for primitive columns whose
    /// type has an unambiguous encoding.
    pub lower_bounds: BTreeMap<i32, Vec<u8>>,
    /// Column id → Appendix-D upper bound bytes.
    pub upper_bounds: BTreeMap<i32, Vec<u8>>,
}

/// The outcome of rewriting one bin-pack group.
#[derive(Debug)]
pub struct RewriteOutcome {
    /// Output files (usually one; more if the merged data exceeds a single
    /// target-sized file — not split in this first cut, so always one, but
    /// modelled as a list for forward compatibility).
    pub outputs: Vec<OutputFile>,
    /// Rows read from the inputs (before deletes).
    pub input_records: i64,
    /// Rows written across all outputs (after deletes).
    pub output_records: i64,
    /// Rows removed by delete application (summed across inputs). By
    /// construction `input_records == output_records + rows_deleted`; the
    /// orchestrator asserts it.
    pub rows_deleted: i64,
    /// Whether any input in the group carried pending deletes (so a shrink
    /// from input to output count is expected, not a bug).
    pub had_deletes: bool,
}

/// A source of raw file bytes (data and delete files) — object storage in
/// production, an in-memory map in tests.
#[allow(async_fn_in_trait)]
pub trait FileBytes {
    /// Reads the whole object at `location`.
    async fn read(&self, location: &str) -> CompactionResult<Bytes>;
}

/// Rewrites one bin-pack group into output Parquet bytes + manifest facts.
///
/// `schema` is the table's current schema (drives output column order and
/// field ids). `deletes` is the plan's full delete list, indexed by
/// [`InputFile::delete_indices`].
pub async fn rewrite_group<F: FileBytes>(
    files: &F,
    schema: &IcebergSchema,
    inputs: &[InputFile],
    deletes: &[DeleteFile],
    compression: Compression,
) -> CompactionResult<RewriteOutcome> {
    let target_ids = top_level_field_ids(schema);
    let target_fields = top_level_fields(schema);

    // The output Arrow schema: one field per top-level Iceberg field, in
    // schema order, carrying the field id. Data types are taken from the
    // inputs on first sight (below); until then we do not know them, so the
    // schema is assembled once we have read a batch. We resolve the type per
    // field id from the first input that has it.
    let mut input_batches: Vec<RecordBatch> = Vec::new();
    let mut resolved_types: BTreeMap<i32, DataType> = BTreeMap::new();
    let mut input_records: i64 = 0;
    let mut output_records: i64 = 0;
    let mut rows_deleted: i64 = 0;
    let mut had_deletes = false;

    for input in inputs {
        let bytes = files.read(&input.file.file_path).await?;
        let (batch, rows_in) = read_input_batch(&input.file.file_path, &bytes)?;
        input_records += rows_in;

        // Record the Arrow type of each field id we see (first writer wins;
        // engines keep types stable across files of a table).
        for (idx, field) in batch.schema().fields().iter().enumerate() {
            if let Some(id) = field_id_of(field) {
                resolved_types
                    .entry(id)
                    .or_insert_with(|| batch.column(idx).data_type().clone());
            }
        }

        // Apply deletes attached to this specific input file.
        if !input.delete_indices.is_empty() {
            had_deletes = true;
        }
        let kept = apply_deletes(files, input, deletes, &batch).await?;
        let kept_rows = i64::try_from(kept.num_rows()).unwrap_or(i64::MAX);
        output_records += kept_rows;
        rows_deleted += rows_in - kept_rows;
        if kept.num_rows() > 0 {
            input_batches.push(kept);
        }
    }

    // Build the output schema now that we know each column's type.
    let output_schema = build_output_schema(&target_ids, &target_fields, &resolved_types)?;

    // Project every input batch to the target column order (field-id aligned),
    // synthesizing all-null columns for fields an input lacked.
    let projected: Vec<RecordBatch> = input_batches
        .iter()
        .map(|b| project_to_schema(b, &output_schema, &target_ids))
        .collect::<CompactionResult<_>>()?;

    let merged = concat_batches(&output_schema, &projected).map_err(|e| {
        CompactionError::parquet("<compaction output>", format!("concat failed: {e}"))
    })?;

    // Stats from the *merged output* (authoritative post-delete values).
    let (value_counts, null_value_counts) = column_counts(&merged, &output_schema);
    let (lower_bounds, upper_bounds) = compute_bounds(&merged, &output_schema, &target_fields);

    let bytes = write_parquet(&merged, &output_schema, compression)?;
    let record_count = i64::try_from(merged.num_rows()).unwrap_or(i64::MAX);

    let output = OutputFile {
        bytes,
        record_count,
        value_counts,
        null_value_counts,
        lower_bounds,
        upper_bounds,
    };

    Ok(RewriteOutcome {
        outputs: if record_count == 0 {
            Vec::new()
        } else {
            vec![output]
        },
        input_records,
        output_records,
        rows_deleted,
        had_deletes,
    })
}

/// Reads one input data file into a single concatenated `RecordBatch` and its
/// row count.
fn read_input_batch(location: &str, bytes: &Bytes) -> CompactionResult<(RecordBatch, i64)> {
    let builder = ParquetRecordBatchReaderBuilder::try_new(bytes.clone())
        .map_err(|e| CompactionError::parquet(location, e))?;
    let arrow_schema = builder.schema().clone();
    let reader = builder
        .build()
        .map_err(|e| CompactionError::parquet(location, e))?;
    let mut batches = Vec::new();
    for batch in reader {
        batches.push(batch.map_err(|e| CompactionError::parquet(location, e))?);
    }
    let batch = if batches.is_empty() {
        RecordBatch::new_empty(arrow_schema.clone())
    } else {
        concat_batches(&arrow_schema, &batches)
            .map_err(|e| CompactionError::parquet(location, format!("concat input: {e}")))?
    };
    let rows = i64::try_from(batch.num_rows()).unwrap_or(i64::MAX);
    Ok((batch, rows))
}

/// Applies the position/equality deletes attached to `input` to its batch,
/// returning the surviving rows.
async fn apply_deletes<F: FileBytes>(
    files: &F,
    input: &InputFile,
    deletes: &[DeleteFile],
    batch: &RecordBatch,
) -> CompactionResult<RecordBatch> {
    if input.delete_indices.is_empty() {
        return Ok(batch.clone());
    }

    let num_rows = batch.num_rows();
    // keep[i] == true -> row i survives.
    let mut keep = vec![true; num_rows];

    // Field id → column index in this input, for equality-delete matching.
    let input_index = field_id_index(&batch.schema());

    for &di in &input.delete_indices {
        let delete = &deletes[di];
        match delete.file.content {
            DataFileContent::PositionDeletes => {
                if delete.file.content_offset.is_some() {
                    // A deletion vector (v3 Puffin blob). Not decodable here.
                    return Err(CompactionError::Unsupported(format!(
                        "input {:?} has an attached deletion vector ({:?}); DV compaction is not \
                         implemented — compact after the DV is materialized, or exclude this file",
                        input.file.file_path, delete.file.file_path
                    )));
                }
                apply_position_delete(files, &input.file.file_path, delete, &mut keep).await?;
            }
            DataFileContent::EqualityDeletes => {
                apply_equality_delete(files, delete, batch, &input_index, &mut keep).await?;
            }
            DataFileContent::Data => {}
        }
    }

    let mask = BooleanArray::from(keep);
    filter_record_batch(batch, &mask).map_err(|e| {
        CompactionError::parquet(&input.file.file_path, format!("delete filter failed: {e}"))
    })
}

/// Reads a position-delete file and marks deleted positions for the given
/// data-file path.
async fn apply_position_delete<F: FileBytes>(
    files: &F,
    data_path: &str,
    delete: &DeleteFile,
    keep: &mut [bool],
) -> CompactionResult<()> {
    let bytes = files.read(&delete.file.file_path).await?;
    let (batch, _) = read_input_batch(&delete.file.file_path, &bytes)?;
    let index = field_id_index(&batch.schema());

    let path_col = index.get(&POS_DELETE_FILE_PATH_ID).copied();
    let pos_col = index.get(&POS_DELETE_POS_ID).copied();
    // Fall back to conventional names when a writer omitted reserved ids.
    let path_col = path_col.or_else(|| batch.schema().index_of("file_path").ok());
    let pos_col = pos_col.or_else(|| batch.schema().index_of("pos").ok());

    let (Some(path_col), Some(pos_col)) = (path_col, pos_col) else {
        return Err(CompactionError::parquet(
            &delete.file.file_path,
            "position-delete file lacks file_path/pos columns",
        ));
    };

    let paths = batch
        .column(path_col)
        .as_any()
        .downcast_ref::<StringArray>()
        .ok_or_else(|| {
            CompactionError::parquet(
                &delete.file.file_path,
                "position-delete file_path is not a UTF-8 string column",
            )
        })?;
    let positions = position_column(&delete.file.file_path, batch.column(pos_col))?;

    let n = keep.len() as u64;
    for (row, &pos) in positions.iter().enumerate() {
        if paths.is_null(row) || paths.value(row) != data_path {
            continue;
        }
        if pos < n {
            keep[usize::try_from(pos).unwrap_or(usize::MAX)] = false;
        }
    }
    Ok(())
}

/// Extracts the `pos` column as `u64` positions, tolerating `Int64` or `UInt64`
/// physical encodings (Iceberg's `pos` is a `long`; Arrow may surface it as
/// either).
fn position_column(location: &str, array: &ArrayRef) -> CompactionResult<Vec<u64>> {
    match array.data_type() {
        DataType::Int64 => {
            let a = array.as_primitive::<arrow_array::types::Int64Type>();
            Ok((0..a.len())
                .map(|i| {
                    if a.is_null(i) {
                        u64::MAX
                    } else {
                        u64::try_from(a.value(i)).unwrap_or(u64::MAX)
                    }
                })
                .collect())
        }
        DataType::UInt64 => {
            // The match arm guarantees the physical type; use the ergonomic
            // typed accessor (no explicit downcast/expect).
            let a = array.as_primitive::<arrow_array::types::UInt64Type>();
            Ok((0..a.len())
                .map(|i| if a.is_null(i) { u64::MAX } else { a.value(i) })
                .collect())
        }
        other => Err(CompactionError::parquet(
            location,
            format!("position-delete pos column has unexpected type {other:?}"),
        )),
    }
}

/// Reads an equality-delete file and marks rows of `batch` that match any
/// delete tuple (over the delete's equality columns) for removal.
async fn apply_equality_delete<F: FileBytes>(
    files: &F,
    delete: &DeleteFile,
    batch: &RecordBatch,
    input_index: &BTreeMap<i32, usize>,
    keep: &mut [bool],
) -> CompactionResult<()> {
    let Some(eq_ids) = &delete.file.equality_ids else {
        // No equality ids: cannot know which columns identify rows. Refuse
        // rather than delete nothing (silently keeping deleted rows) or
        // everything.
        return Err(CompactionError::parquet(
            &delete.file.file_path,
            "equality-delete file has no equality_ids",
        ));
    };

    let bytes = files.read(&delete.file.file_path).await?;
    let (delete_batch, _) = read_input_batch(&delete.file.file_path, &bytes)?;
    let delete_index = field_id_index(&delete_batch.schema());

    // Resolve the equality columns in both the delete file and the data batch.
    let mut delete_cols = Vec::with_capacity(eq_ids.len());
    let mut data_cols = Vec::with_capacity(eq_ids.len());
    for id in eq_ids {
        let Some(&dc) = delete_index.get(id) else {
            return Err(CompactionError::parquet(
                &delete.file.file_path,
                format!("equality column field id {id} absent from the delete file"),
            ));
        };
        let Some(&bc) = input_index.get(id) else {
            // The data file lacks an equality column (e.g. an older file
            // predating it): those rows cannot match on it, so this delete
            // removes nothing from this file. Skip it.
            return Ok(());
        };
        delete_cols.push(dc);
        data_cols.push(bc);
    }

    // Build the set of delete key tuples (row-major Appendix-D bytes).
    let mut delete_keys: HashSet<Vec<u8>> = HashSet::new();
    for row in 0..delete_batch.num_rows() {
        if let Some(key) = row_key(&delete_batch, &delete_cols, row) {
            delete_keys.insert(key);
        }
    }
    if delete_keys.is_empty() {
        return Ok(());
    }

    for (row, alive) in keep.iter_mut().enumerate() {
        if !*alive {
            continue;
        }
        if let Some(key) = row_key(batch, &data_cols, row)
            && delete_keys.contains(&key)
        {
            *alive = false;
        }
    }
    Ok(())
}

/// A row-major key over the given columns, encoded as length-prefixed
/// per-cell bytes with an explicit null marker. Returns `None` if any cell is
/// null — equality deletes do not match on null keys (SQL `NULL != NULL`),
/// which is also the reference behavior.
fn row_key(batch: &RecordBatch, cols: &[usize], row: usize) -> Option<Vec<u8>> {
    let mut key = Vec::new();
    for &c in cols {
        let array = batch.column(c);
        if array.is_null(row) {
            return None;
        }
        let cell = crate::stats::cell_bound_bytes(array, row)?;
        key.extend_from_slice(&(cell.len() as u64).to_le_bytes());
        key.extend_from_slice(&cell);
    }
    Some(key)
}

/// Builds the output Arrow schema: one field per top-level Iceberg field id,
/// in schema order, with the field id in metadata and the type resolved from
/// the inputs.
fn build_output_schema(
    target_ids: &[i32],
    target_fields: &BTreeMap<i32, crate::arrow_schema::TopLevelField>,
    resolved_types: &BTreeMap<i32, DataType>,
) -> CompactionResult<Arc<ArrowSchema>> {
    let mut fields = Vec::with_capacity(target_ids.len());
    for &id in target_ids {
        let meta = target_fields.get(&id).ok_or_else(|| {
            CompactionError::Unsupported(format!("field id {id} missing from target field map"))
        })?;
        // A field no input carried uses the Arrow `Null` type — a genuinely
        // all-null column for an added field that predates every input file.
        // In practice every column exists in at least the newest inputs, so
        // this is the pathological all-old-files case. A *required* field
        // absent from every input is a schema/data inconsistency, refused.
        let data_type = if let Some(dt) = resolved_types.get(&id) {
            dt.clone()
        } else if meta.required {
            return Err(CompactionError::Unsupported(format!(
                "required field {:?} (id {id}) is present in no input file; cannot synthesize a \
                 non-null column",
                meta.name
            )));
        } else {
            DataType::Null
        };
        let mut md = HashMap::new();
        md.insert(PARQUET_FIELD_ID_META_KEY.to_string(), id.to_string());
        // Always nullable at the Arrow layer: even required Iceberg columns
        // are written nullable in Parquet by many engines, and a synthesized
        // column must be nullable. Iceberg required-ness is enforced by the
        // catalog, not the Parquet nullability flag.
        fields.push(Field::new(&meta.name, data_type, true).with_metadata(md));
    }
    Ok(Arc::new(ArrowSchema::new(fields)))
}

/// Projects `batch` onto `output_schema` by field id, synthesizing all-null
/// columns for target fields the batch lacks.
fn project_to_schema(
    batch: &RecordBatch,
    output_schema: &Arc<ArrowSchema>,
    target_ids: &[i32],
) -> CompactionResult<RecordBatch> {
    let index = field_id_index(&batch.schema());
    let n = batch.num_rows();
    let mut columns: Vec<ArrayRef> = Vec::with_capacity(target_ids.len());
    for (out_idx, &id) in target_ids.iter().enumerate() {
        let field = output_schema.field(out_idx);
        match index.get(&id) {
            Some(&col) => {
                let array = batch.column(col);
                // The observed type must match the schema's resolved type
                // (all inputs of a table share column types). If a later
                // input disagrees we refuse rather than silently cast.
                if array.data_type() != field.data_type() {
                    return Err(CompactionError::parquet(
                        "<compaction input>",
                        format!(
                            "field id {id} has type {:?} in one input but {:?} in another; \
                             cannot merge mismatched column types",
                            array.data_type(),
                            field.data_type()
                        ),
                    ));
                }
                columns.push(array.clone());
            }
            None => {
                // Schema evolution: this input predates the column. Emit nulls.
                columns.push(arrow_array::new_null_array(field.data_type(), n));
            }
        }
    }
    RecordBatch::try_new(output_schema.clone(), columns).map_err(|e| {
        CompactionError::parquet("<compaction output>", format!("project failed: {e}"))
    })
}

/// `value_counts` and `null_value_counts` per field id from the merged batch.
fn column_counts(
    batch: &RecordBatch,
    schema: &ArrowSchema,
) -> (BTreeMap<i32, i64>, BTreeMap<i32, i64>) {
    let mut value_counts = BTreeMap::new();
    let mut null_counts = BTreeMap::new();
    let rows = i64::try_from(batch.num_rows()).unwrap_or(i64::MAX);
    for (idx, field) in schema.fields().iter().enumerate() {
        let Some(id) = field_id_of(field) else {
            continue;
        };
        value_counts.insert(id, rows);
        let nulls = i64::try_from(batch.column(idx).null_count()).unwrap_or(0);
        null_counts.insert(id, nulls);
    }
    (value_counts, null_counts)
}

/// Serializes the merged batch as Parquet, preserving field ids on every
/// column (they are in `schema`'s field metadata, which the Arrow writer
/// propagates to the Parquet `field_id`).
fn write_parquet(
    batch: &RecordBatch,
    schema: &Arc<ArrowSchema>,
    compression: Compression,
) -> CompactionResult<Bytes> {
    let props = WriterProperties::builder()
        .set_compression(compression)
        // Ask the writer to compute per-column statistics into the footer.
        .set_statistics_enabled(parquet::file::properties::EnabledStatistics::Chunk)
        .build();
    let mut buf: Vec<u8> = Vec::new();
    let mut writer = ArrowWriter::try_new(&mut buf, schema.clone(), Some(props))
        .map_err(|e| CompactionError::parquet("<compaction output>", e))?;
    if batch.num_rows() > 0 {
        writer
            .write(batch)
            .map_err(|e| CompactionError::parquet("<compaction output>", e))?;
    }
    writer
        .close()
        .map_err(|e| CompactionError::parquet("<compaction output>", e))?;
    Ok(Bytes::from(buf))
}

/// Builds the manifest [`DataFile`] for one written output, given its path,
/// partition tuple (inherited from the group), and spec id.
#[must_use]
pub fn output_data_file(
    output: &OutputFile,
    file_path: String,
    partition: meridian_iceberg::manifest::PartitionTuple,
    file_size_in_bytes: i64,
) -> DataFile {
    DataFile {
        content: DataFileContent::Data,
        file_path,
        file_format: "PARQUET".to_owned(),
        partition,
        record_count: output.record_count,
        file_size_in_bytes,
        column_sizes: None,
        value_counts: Some(output.value_counts.clone()),
        null_value_counts: Some(output.null_value_counts.clone()),
        nan_value_counts: None,
        lower_bounds: if output.lower_bounds.is_empty() {
            None
        } else {
            Some(output.lower_bounds.clone())
        },
        upper_bounds: if output.upper_bounds.is_empty() {
            None
        } else {
            Some(output.upper_bounds.clone())
        },
        key_metadata: None,
        split_offsets: None,
        equality_ids: None,
        sort_order_id: None,
        first_row_id: None,
        referenced_data_file: None,
        content_offset: None,
        content_size_in_bytes: None,
    }
}
