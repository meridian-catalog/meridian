//! Writing manifest lists and manifests as spec-shaped Avro.
//!
//! The emitted files carry the spec's field ids (`field-id` /
//! `element-id` attributes), key-value metadata, and the deflate codec, in
//! the same shapes the Java implementation writes (v1 manifest lists use
//! the historical `added_data_files_count` names; v2 uses the current
//! names). Deviations, all documented on [`super`]: uuid partition values
//! are written as plain `fixed(16)` (no `uuid` logical type, which the
//! Avro library cannot encode against a fixed schema), `adjust-to-utc` is
//! not emitted, and v3-only fields are refused rather than dropped.

use apache_avro::types::Value;
use apache_avro::{Codec, Schema, Writer};
use serde_json::json;

use super::{
    DataFile, ManifestContentType, ManifestEntry, ManifestError, ManifestFile, PartitionFieldType,
    PartitionTuple, ids,
};
use crate::spec::{PartitionField, PrimitiveType};
use crate::value::Datum;

/// Inputs for [`write_manifest_list`].
#[derive(Debug)]
pub struct ManifestListWriteParams<'a> {
    /// Target format version: 1 or 2 (v3 writing is not supported).
    pub format_version: u8,
    /// The snapshot this list belongs to (`snapshot-id` metadata).
    pub snapshot_id: i64,
    /// Parent snapshot id; written as the literal `null` when absent, as
    /// the reference implementation does.
    pub parent_snapshot_id: Option<i64>,
    /// The snapshot's sequence number (`sequence-number` metadata);
    /// required for v2.
    pub sequence_number: Option<i64>,
    /// The `manifest_file` entries, written in order.
    pub manifests: &'a [ManifestFile],
}

/// Writes a manifest list. See [`super`] for shape guarantees.
pub fn write_manifest_list(params: &ManifestListWriteParams<'_>) -> Result<Vec<u8>, ManifestError> {
    let v2 = match params.format_version {
        1 => false,
        2 => true,
        v => {
            return Err(ManifestError::Unsupported(format!(
                "manifest-list writing supports format versions 1 and 2, not {v}"
            )));
        }
    };
    if v2 && params.sequence_number.is_none() {
        return Err(ManifestError::Unsupported(
            "v2 manifest lists require a sequence number".to_owned(),
        ));
    }

    let schema_json = manifest_list_schema_json(v2);
    let schema = Schema::parse_str(&schema_json.to_string())?;
    let mut writer = Writer::with_codec(
        &schema,
        Vec::new(),
        Codec::Deflate(apache_avro::DeflateSettings::default()),
    );
    writer.add_user_metadata("snapshot-id".to_owned(), params.snapshot_id.to_string())?;
    writer.add_user_metadata(
        "parent-snapshot-id".to_owned(),
        params
            .parent_snapshot_id
            .map_or_else(|| "null".to_owned(), |id| id.to_string()),
    )?;
    if let Some(seq) = params.sequence_number {
        writer.add_user_metadata("sequence-number".to_owned(), seq.to_string())?;
    }
    writer.add_user_metadata(
        "format-version".to_owned(),
        params.format_version.to_string(),
    )?;

    for manifest in params.manifests {
        writer.append_value_ref(&manifest_file_value(manifest, v2)?)?;
    }
    Ok(writer.into_inner()?)
}

/// Inputs for [`write_manifest`].
#[derive(Debug)]
pub struct ManifestWriteParams<'a> {
    /// Target format version: 1 or 2 (v3 writing is not supported).
    pub format_version: u8,
    /// Data or deletes; deletes require v2.
    pub content: ManifestContentType,
    /// The table schema JSON, written verbatim as the `schema` metadata
    /// key.
    pub schema_json: &'a str,
    /// `schema-id` metadata (required by the spec for v2).
    pub schema_id: Option<i32>,
    /// `partition-spec-id` metadata.
    pub partition_spec_id: i32,
    /// The partition fields, for the `partition-spec` metadata key.
    pub partition_fields: &'a [PartitionField],
    /// The typed partition tuple shape (from
    /// [`super::partition_field_types`]); one per partition field, in
    /// order.
    pub partition_types: &'a [PartitionFieldType],
    /// The entries to write, in order.
    pub entries: &'a [ManifestEntry],
}

/// Writes a manifest. See [`super`] for shape guarantees.
///
/// v1 refuses delete content and entries without a snapshot id (v1 stores
/// `snapshot_id` as required); both versions refuse entries carrying
/// v3-only fields rather than silently dropping data.
pub fn write_manifest(params: &ManifestWriteParams<'_>) -> Result<Vec<u8>, ManifestError> {
    let v2 = match params.format_version {
        1 => false,
        2 => true,
        v => {
            return Err(ManifestError::Unsupported(format!(
                "manifest writing supports format versions 1 and 2, not {v}"
            )));
        }
    };
    if !v2 && params.content == ManifestContentType::Deletes {
        return Err(ManifestError::Unsupported(
            "delete manifests require format version 2".to_owned(),
        ));
    }

    let schema_json = manifest_schema_json(v2, params.partition_types)?;
    let schema = Schema::parse_str(&schema_json.to_string())?;
    let mut writer = Writer::with_codec(
        &schema,
        Vec::new(),
        Codec::Deflate(apache_avro::DeflateSettings::default()),
    );
    writer.add_user_metadata("schema".to_owned(), params.schema_json)?;
    if let Some(schema_id) = params.schema_id {
        writer.add_user_metadata("schema-id".to_owned(), schema_id.to_string())?;
    }
    let spec_fields = serde_json::to_string(params.partition_fields)
        .map_err(|e| ManifestError::Shape(format!("unserializable partition fields: {e}")))?;
    writer.add_user_metadata("partition-spec".to_owned(), spec_fields)?;
    writer.add_user_metadata(
        "partition-spec-id".to_owned(),
        params.partition_spec_id.to_string(),
    )?;
    writer.add_user_metadata(
        "format-version".to_owned(),
        params.format_version.to_string(),
    )?;
    if v2 {
        let content = match params.content {
            ManifestContentType::Data => "data",
            ManifestContentType::Deletes => "deletes",
        };
        writer.add_user_metadata("content".to_owned(), content)?;
    }

    for entry in params.entries {
        writer.append_value_ref(&entry_value(entry, v2, params.partition_types)?)?;
    }
    Ok(writer.into_inner()?)
}

// ---- Schema JSON builders ----

fn field(name: &str, field_id: i64, ty: serde_json::Value) -> serde_json::Value {
    let mut obj = serde_json::Map::new();
    obj.insert("name".to_owned(), name.into());
    obj.insert("type".to_owned(), ty);
    obj.insert("field-id".to_owned(), field_id.into());
    serde_json::Value::Object(obj)
}

fn optional(name: &str, field_id: i64, ty: serde_json::Value) -> serde_json::Value {
    let mut obj = serde_json::Map::new();
    obj.insert("name".to_owned(), name.into());
    obj.insert(
        "type".to_owned(),
        serde_json::Value::Array(vec!["null".into(), ty]),
    );
    obj.insert("field-id".to_owned(), field_id.into());
    obj.insert("default".to_owned(), serde_json::Value::Null);
    serde_json::Value::Object(obj)
}

/// `map<int, V>` as the spec's array-of-kv-records encoding.
fn kv_map_type(key_id: i64, value_id: i64, value_type: &str) -> serde_json::Value {
    json!({
        "type": "array",
        "logicalType": "map",
        "items": {
            "type": "record",
            "name": format!("k{key_id}_v{value_id}"),
            "fields": [
                {"name": "key", "type": "int", "field-id": key_id},
                {"name": "value", "type": value_type, "field-id": value_id},
            ],
        },
    })
}

fn manifest_list_schema_json(v2: bool) -> serde_json::Value {
    let summary = json!({
        "type": "record",
        "name": "r508",
        "fields": [
            field("contains_null", ids::CONTAINS_NULL, json!("boolean")),
            optional("contains_nan", ids::CONTAINS_NAN, json!("boolean")),
            optional("lower_bound", ids::SUMMARY_LOWER, json!("bytes")),
            optional("upper_bound", ids::SUMMARY_UPPER, json!("bytes")),
        ],
    });
    let partitions_type = json!({"type": "array", "items": summary, "element-id": 508});

    let mut fields = vec![
        field("manifest_path", ids::MANIFEST_PATH, json!("string")),
        field("manifest_length", ids::MANIFEST_LENGTH, json!("long")),
        field("partition_spec_id", ids::PARTITION_SPEC_ID, json!("int")),
    ];
    if v2 {
        fields.extend([
            field("content", ids::MANIFEST_CONTENT, json!("int")),
            field("sequence_number", ids::SEQUENCE_NUMBER, json!("long")),
            field(
                "min_sequence_number",
                ids::MIN_SEQUENCE_NUMBER,
                json!("long"),
            ),
            field("added_snapshot_id", ids::ADDED_SNAPSHOT_ID, json!("long")),
            field("added_files_count", ids::ADDED_FILES_COUNT, json!("int")),
            field(
                "existing_files_count",
                ids::EXISTING_FILES_COUNT,
                json!("int"),
            ),
            field(
                "deleted_files_count",
                ids::DELETED_FILES_COUNT,
                json!("int"),
            ),
            field("added_rows_count", ids::ADDED_ROWS_COUNT, json!("long")),
            field(
                "existing_rows_count",
                ids::EXISTING_ROWS_COUNT,
                json!("long"),
            ),
            field("deleted_rows_count", ids::DELETED_ROWS_COUNT, json!("long")),
        ]);
    } else {
        // v1 uses the historical field names; counts are optional.
        fields.extend([
            field("added_snapshot_id", ids::ADDED_SNAPSHOT_ID, json!("long")),
            optional(
                "added_data_files_count",
                ids::ADDED_FILES_COUNT,
                json!("int"),
            ),
            optional(
                "existing_data_files_count",
                ids::EXISTING_FILES_COUNT,
                json!("int"),
            ),
            optional(
                "deleted_data_files_count",
                ids::DELETED_FILES_COUNT,
                json!("int"),
            ),
            optional("added_rows_count", ids::ADDED_ROWS_COUNT, json!("long")),
            optional(
                "existing_rows_count",
                ids::EXISTING_ROWS_COUNT,
                json!("long"),
            ),
            optional("deleted_rows_count", ids::DELETED_ROWS_COUNT, json!("long")),
        ]);
    }
    fields.push(optional("partitions", ids::PARTITIONS, partitions_type));
    fields.push(optional("key_metadata", ids::KEY_METADATA, json!("bytes")));
    json!({"type": "record", "name": "manifest_file", "fields": fields})
}

fn manifest_schema_json(
    v2: bool,
    partition_types: &[PartitionFieldType],
) -> Result<serde_json::Value, ManifestError> {
    let data_file = data_file_schema_json(v2, partition_types)?;
    let mut fields = vec![field("status", ids::ENTRY_STATUS, json!("int"))];
    if v2 {
        fields.push(optional(
            "snapshot_id",
            ids::ENTRY_SNAPSHOT_ID,
            json!("long"),
        ));
        fields.push(optional(
            "sequence_number",
            ids::ENTRY_SEQUENCE_NUMBER,
            json!("long"),
        ));
        fields.push(optional(
            "file_sequence_number",
            ids::ENTRY_FILE_SEQUENCE_NUMBER,
            json!("long"),
        ));
    } else {
        fields.push(field("snapshot_id", ids::ENTRY_SNAPSHOT_ID, json!("long")));
    }
    fields.push(field("data_file", ids::ENTRY_DATA_FILE, data_file));
    Ok(json!({"type": "record", "name": "manifest_entry", "fields": fields}))
}

fn data_file_schema_json(
    v2: bool,
    partition_types: &[PartitionFieldType],
) -> Result<serde_json::Value, ManifestError> {
    let partition_fields = partition_types
        .iter()
        .map(|pt| {
            let avro_type = partition_avro_type(pt)?;
            Ok(json!({
                "name": pt.name,
                "type": ["null", avro_type],
                "field-id": i64::from(pt.field_id),
                "default": null,
            }))
        })
        .collect::<Result<Vec<_>, ManifestError>>()?;
    let partition_record = json!({
        "type": "record",
        "name": "r102",
        "fields": partition_fields,
    });

    let mut df_fields = Vec::new();
    if v2 {
        df_fields.push(field("content", ids::DF_CONTENT, json!("int")));
    }
    df_fields.extend([
        field("file_path", ids::DF_FILE_PATH, json!("string")),
        field("file_format", ids::DF_FILE_FORMAT, json!("string")),
        field("partition", ids::DF_PARTITION, partition_record),
        field("record_count", ids::DF_RECORD_COUNT, json!("long")),
        field("file_size_in_bytes", ids::DF_FILE_SIZE, json!("long")),
    ]);
    if !v2 {
        // Deprecated v1 field: "always write a default in v1".
        df_fields.push(field(
            "block_size_in_bytes",
            ids::DF_BLOCK_SIZE,
            json!("long"),
        ));
    }
    df_fields.extend([
        optional(
            "column_sizes",
            ids::DF_COLUMN_SIZES,
            kv_map_type(117, 118, "long"),
        ),
        optional(
            "value_counts",
            ids::DF_VALUE_COUNTS,
            kv_map_type(119, 120, "long"),
        ),
        optional(
            "null_value_counts",
            ids::DF_NULL_VALUE_COUNTS,
            kv_map_type(121, 122, "long"),
        ),
        optional(
            "nan_value_counts",
            ids::DF_NAN_VALUE_COUNTS,
            kv_map_type(138, 139, "long"),
        ),
        optional(
            "lower_bounds",
            ids::DF_LOWER_BOUNDS,
            kv_map_type(126, 127, "bytes"),
        ),
        optional(
            "upper_bounds",
            ids::DF_UPPER_BOUNDS,
            kv_map_type(129, 130, "bytes"),
        ),
        optional("key_metadata", ids::DF_KEY_METADATA, json!("bytes")),
        optional(
            "split_offsets",
            ids::DF_SPLIT_OFFSETS,
            json!({"type": "array", "items": "long", "element-id": 133}),
        ),
    ]);
    if v2 {
        df_fields.push(optional(
            "equality_ids",
            ids::DF_EQUALITY_IDS,
            json!({"type": "array", "items": "int", "element-id": 136}),
        ));
    }
    df_fields.push(optional(
        "sort_order_id",
        ids::DF_SORT_ORDER_ID,
        json!("int"),
    ));
    if v2 {
        df_fields.push(optional(
            "referenced_data_file",
            ids::DF_REFERENCED_DATA_FILE,
            json!("string"),
        ));
    }
    Ok(json!({"type": "record", "name": "r2", "fields": df_fields}))
}

/// Bytes needed to hold any unscaled decimal of the given precision as
/// two's-complement (the fixed width the reference implementation uses).
fn decimal_required_bytes(precision: u32) -> Result<usize, ManifestError> {
    if precision == 0 || precision > 38 {
        return Err(ManifestError::Unsupported(format!(
            "decimal precision {precision} out of range"
        )));
    }
    let mut max = 10i128.pow(precision) - 1;
    let mut bytes = 1usize;
    while max > i128::from(i8::MAX) {
        max >>= 8;
        bytes += 1;
    }
    Ok(bytes)
}

fn partition_avro_type(pt: &PartitionFieldType) -> Result<serde_json::Value, ManifestError> {
    Ok(match &pt.result_type {
        PrimitiveType::Boolean => json!("boolean"),
        PrimitiveType::Int => json!("int"),
        PrimitiveType::Long => json!("long"),
        PrimitiveType::Float => json!("float"),
        PrimitiveType::Double => json!("double"),
        PrimitiveType::Date => json!({"type": "int", "logicalType": "date"}),
        PrimitiveType::Time => json!({"type": "long", "logicalType": "time-micros"}),
        // adjust-to-utc is not representable through the Avro library; the
        // partition-spec + schema metadata keys carry the distinction.
        PrimitiveType::Timestamp | PrimitiveType::Timestamptz => {
            json!({"type": "long", "logicalType": "timestamp-micros"})
        }
        PrimitiveType::TimestampNs | PrimitiveType::TimestamptzNs => {
            json!({"type": "long", "logicalType": "timestamp-nanos"})
        }
        PrimitiveType::String => json!("string"),
        // Plain fixed(16): the Avro library cannot encode the uuid logical
        // type against a fixed schema (see module docs).
        PrimitiveType::Uuid => {
            json!({"type": "fixed", "size": 16, "name": format!("uuid_{}", pt.field_id)})
        }
        PrimitiveType::Fixed(n) => {
            json!({"type": "fixed", "size": n, "name": format!("fixed_{}", pt.field_id)})
        }
        PrimitiveType::Binary => json!("bytes"),
        PrimitiveType::Decimal { precision, scale } => json!({
            "type": "fixed",
            "size": decimal_required_bytes(*precision)?,
            "name": format!("decimal_{}", pt.field_id),
            "logicalType": "decimal",
            "precision": precision,
            "scale": scale,
        }),
        other => {
            return Err(ManifestError::Unsupported(format!(
                "cannot write partition values of type {other}"
            )));
        }
    })
}

// ---- Value builders ----

fn null_or<F>(value: Option<F>, build: impl FnOnce(F) -> Value) -> Value {
    match value {
        Some(v) => Value::Union(1, Box::new(build(v))),
        None => Value::Union(0, Box::new(Value::Null)),
    }
}

fn manifest_file_value(manifest: &ManifestFile, v2: bool) -> Result<Value, ManifestError> {
    if manifest.first_row_id.is_some() {
        return Err(ManifestError::Unsupported(
            "manifest_file.first_row_id is v3-only; v3 writing is not supported".to_owned(),
        ));
    }
    let summaries = manifest.partitions.as_ref().map(|partitions| {
        partitions
            .iter()
            .map(|s| {
                Value::Record(vec![
                    ("contains_null".to_owned(), Value::Boolean(s.contains_null)),
                    (
                        "contains_nan".to_owned(),
                        null_or(s.contains_nan, Value::Boolean),
                    ),
                    (
                        "lower_bound".to_owned(),
                        null_or(s.lower_bound.clone(), Value::Bytes),
                    ),
                    (
                        "upper_bound".to_owned(),
                        null_or(s.upper_bound.clone(), Value::Bytes),
                    ),
                ])
            })
            .collect::<Vec<_>>()
    });

    let mut fields = vec![
        (
            "manifest_path".to_owned(),
            Value::String(manifest.manifest_path.clone()),
        ),
        (
            "manifest_length".to_owned(),
            Value::Long(manifest.manifest_length),
        ),
        (
            "partition_spec_id".to_owned(),
            Value::Int(manifest.partition_spec_id),
        ),
    ];
    if v2 {
        fields.extend(v2_list_fields(manifest)?);
    } else {
        fields.extend(v1_list_fields(manifest)?);
    }
    fields.push(("partitions".to_owned(), null_or(summaries, Value::Array)));
    fields.push((
        "key_metadata".to_owned(),
        null_or(manifest.key_metadata.clone(), Value::Bytes),
    ));
    Ok(Value::Record(fields))
}

/// The v2-required count fields (plus content and sequence numbers).
fn v2_list_fields(manifest: &ManifestFile) -> Result<Vec<(String, Value)>, ManifestError> {
    fn require_int(what: &str, v: Option<i32>) -> Result<Value, ManifestError> {
        v.map(Value::Int)
            .ok_or_else(|| ManifestError::Unsupported(format!("v2 manifest lists require {what}")))
    }
    fn require_long(what: &str, v: Option<i64>) -> Result<Value, ManifestError> {
        v.map(Value::Long)
            .ok_or_else(|| ManifestError::Unsupported(format!("v2 manifest lists require {what}")))
    }
    Ok(vec![
        ("content".to_owned(), Value::Int(manifest.content.code())),
        (
            "sequence_number".to_owned(),
            Value::Long(manifest.sequence_number),
        ),
        (
            "min_sequence_number".to_owned(),
            Value::Long(manifest.min_sequence_number),
        ),
        (
            "added_snapshot_id".to_owned(),
            Value::Long(manifest.added_snapshot_id),
        ),
        (
            "added_files_count".to_owned(),
            require_int("added_files_count", manifest.added_files_count)?,
        ),
        (
            "existing_files_count".to_owned(),
            require_int("existing_files_count", manifest.existing_files_count)?,
        ),
        (
            "deleted_files_count".to_owned(),
            require_int("deleted_files_count", manifest.deleted_files_count)?,
        ),
        (
            "added_rows_count".to_owned(),
            require_long("added_rows_count", manifest.added_rows_count)?,
        ),
        (
            "existing_rows_count".to_owned(),
            require_long("existing_rows_count", manifest.existing_rows_count)?,
        ),
        (
            "deleted_rows_count".to_owned(),
            require_long("deleted_rows_count", manifest.deleted_rows_count)?,
        ),
    ])
}

/// The v1 field spellings (counts optional, historical names).
fn v1_list_fields(manifest: &ManifestFile) -> Result<Vec<(String, Value)>, ManifestError> {
    if manifest.content != ManifestContentType::Data {
        return Err(ManifestError::Unsupported(
            "v1 manifest lists cannot reference delete manifests".to_owned(),
        ));
    }
    Ok(vec![
        (
            "added_snapshot_id".to_owned(),
            Value::Long(manifest.added_snapshot_id),
        ),
        (
            "added_data_files_count".to_owned(),
            null_or(manifest.added_files_count, Value::Int),
        ),
        (
            "existing_data_files_count".to_owned(),
            null_or(manifest.existing_files_count, Value::Int),
        ),
        (
            "deleted_data_files_count".to_owned(),
            null_or(manifest.deleted_files_count, Value::Int),
        ),
        (
            "added_rows_count".to_owned(),
            null_or(manifest.added_rows_count, Value::Long),
        ),
        (
            "existing_rows_count".to_owned(),
            null_or(manifest.existing_rows_count, Value::Long),
        ),
        (
            "deleted_rows_count".to_owned(),
            null_or(manifest.deleted_rows_count, Value::Long),
        ),
    ])
}

fn entry_value(
    entry: &ManifestEntry,
    v2: bool,
    partition_types: &[PartitionFieldType],
) -> Result<Value, ManifestError> {
    let mut fields = vec![("status".to_owned(), Value::Int(entry.status.code()))];
    if v2 {
        fields.push((
            "snapshot_id".to_owned(),
            null_or(entry.snapshot_id, Value::Long),
        ));
        fields.push((
            "sequence_number".to_owned(),
            null_or(entry.sequence_number, Value::Long),
        ));
        fields.push((
            "file_sequence_number".to_owned(),
            null_or(entry.file_sequence_number, Value::Long),
        ));
    } else {
        let snapshot_id = entry.snapshot_id.ok_or_else(|| {
            ManifestError::Unsupported(
                "v1 manifest entries require an explicit snapshot_id".to_owned(),
            )
        })?;
        fields.push(("snapshot_id".to_owned(), Value::Long(snapshot_id)));
    }
    fields.push((
        "data_file".to_owned(),
        data_file_value(&entry.data_file, v2, partition_types)?,
    ));
    Ok(Value::Record(fields))
}

fn kv_map_value(entries: Option<&std::collections::BTreeMap<i32, i64>>) -> Value {
    null_or(entries, |m| {
        Value::Array(
            m.iter()
                .map(|(k, v)| {
                    Value::Record(vec![
                        ("key".to_owned(), Value::Int(*k)),
                        ("value".to_owned(), Value::Long(*v)),
                    ])
                })
                .collect(),
        )
    })
}

fn bounds_map_value(entries: Option<&std::collections::BTreeMap<i32, Vec<u8>>>) -> Value {
    null_or(entries, |m| {
        Value::Array(
            m.iter()
                .map(|(k, v)| {
                    Value::Record(vec![
                        ("key".to_owned(), Value::Int(*k)),
                        ("value".to_owned(), Value::Bytes(v.clone())),
                    ])
                })
                .collect(),
        )
    })
}

fn data_file_value(
    df: &DataFile,
    v2: bool,
    partition_types: &[PartitionFieldType],
) -> Result<Value, ManifestError> {
    if df.first_row_id.is_some()
        || df.content_offset.is_some()
        || df.content_size_in_bytes.is_some()
    {
        return Err(ManifestError::Unsupported(
            "data_file carries v3-only fields (first_row_id / deletion vector offsets); \
             v3 writing is not supported"
                .to_owned(),
        ));
    }
    if !v2 && df.content != super::DataFileContent::Data {
        return Err(ManifestError::Unsupported(
            "v1 manifests can only track data files".to_owned(),
        ));
    }

    let mut fields = Vec::new();
    if v2 {
        fields.push(("content".to_owned(), Value::Int(df.content.code())));
    }
    fields.extend([
        ("file_path".to_owned(), Value::String(df.file_path.clone())),
        (
            "file_format".to_owned(),
            Value::String(df.file_format.clone()),
        ),
        (
            "partition".to_owned(),
            partition_value(&df.partition, partition_types)?,
        ),
        ("record_count".to_owned(), Value::Long(df.record_count)),
        (
            "file_size_in_bytes".to_owned(),
            Value::Long(df.file_size_in_bytes),
        ),
    ]);
    if !v2 {
        // Deprecated; the spec says always write a default in v1.
        fields.push(("block_size_in_bytes".to_owned(), Value::Long(67_108_864)));
    }
    fields.extend([
        (
            "column_sizes".to_owned(),
            kv_map_value(df.column_sizes.as_ref()),
        ),
        (
            "value_counts".to_owned(),
            kv_map_value(df.value_counts.as_ref()),
        ),
        (
            "null_value_counts".to_owned(),
            kv_map_value(df.null_value_counts.as_ref()),
        ),
        (
            "nan_value_counts".to_owned(),
            kv_map_value(df.nan_value_counts.as_ref()),
        ),
        (
            "lower_bounds".to_owned(),
            bounds_map_value(df.lower_bounds.as_ref()),
        ),
        (
            "upper_bounds".to_owned(),
            bounds_map_value(df.upper_bounds.as_ref()),
        ),
        (
            "key_metadata".to_owned(),
            null_or(df.key_metadata.clone(), Value::Bytes),
        ),
        (
            "split_offsets".to_owned(),
            null_or(df.split_offsets.clone(), |v| {
                Value::Array(v.into_iter().map(Value::Long).collect())
            }),
        ),
    ]);
    if v2 {
        fields.push((
            "equality_ids".to_owned(),
            null_or(df.equality_ids.clone(), |v| {
                Value::Array(v.into_iter().map(Value::Int).collect())
            }),
        ));
    }
    fields.push((
        "sort_order_id".to_owned(),
        null_or(df.sort_order_id, Value::Int),
    ));
    if v2 {
        fields.push((
            "referenced_data_file".to_owned(),
            null_or(df.referenced_data_file.clone(), Value::String),
        ));
    }
    Ok(Value::Record(fields))
}

fn partition_value(
    tuple: &PartitionTuple,
    partition_types: &[PartitionFieldType],
) -> Result<Value, ManifestError> {
    if tuple.fields.len() != partition_types.len() {
        return Err(ManifestError::Shape(format!(
            "partition tuple has {} fields, spec has {}",
            tuple.fields.len(),
            partition_types.len()
        )));
    }
    let mut fields = Vec::with_capacity(partition_types.len());
    for pt in partition_types {
        let value = tuple.get(pt.field_id).ok_or_else(|| {
            ManifestError::Shape(format!(
                "partition tuple is missing field {} ({})",
                pt.field_id, pt.name
            ))
        })?;
        let avro = match value {
            None => Value::Union(0, Box::new(Value::Null)),
            Some(datum) => Value::Union(1, Box::new(datum_to_avro(datum, pt)?)),
        };
        fields.push((pt.name.clone(), avro));
    }
    Ok(Value::Record(fields))
}

fn datum_to_avro(datum: &Datum, pt: &PartitionFieldType) -> Result<Value, ManifestError> {
    let mismatch = || {
        ManifestError::Shape(format!(
            "partition field {} expects {}, got {datum}",
            pt.name, pt.result_type
        ))
    };
    Ok(match (&pt.result_type, datum) {
        (PrimitiveType::Boolean, Datum::Boolean(v)) => Value::Boolean(*v),
        (PrimitiveType::Int, Datum::Int(v)) => Value::Int(*v),
        (PrimitiveType::Long, Datum::Long(v)) => Value::Long(*v),
        (PrimitiveType::Long, Datum::Int(v)) => Value::Long(i64::from(*v)),
        (PrimitiveType::Float, Datum::Float(v)) => Value::Float(*v),
        (PrimitiveType::Double, Datum::Double(v)) => Value::Double(*v),
        (PrimitiveType::Date, Datum::Date(v)) => Value::Date(*v),
        (PrimitiveType::Time, Datum::Time(v)) => Value::TimeMicros(*v),
        (
            PrimitiveType::Timestamp | PrimitiveType::Timestamptz,
            Datum::Timestamp(v) | Datum::Timestamptz(v),
        ) => Value::TimestampMicros(*v),
        (
            PrimitiveType::TimestampNs | PrimitiveType::TimestamptzNs,
            Datum::TimestampNs(v) | Datum::TimestamptzNs(v),
        ) => Value::TimestampNanos(*v),
        (PrimitiveType::String, Datum::String(v)) => Value::String(v.clone()),
        (PrimitiveType::Uuid, Datum::Uuid(v)) => Value::Fixed(16, v.as_bytes().to_vec()),
        (PrimitiveType::Fixed(n), Datum::Fixed(v)) => {
            let n = usize::try_from(*n)
                .map_err(|_| ManifestError::Shape("fixed width out of range".into()))?;
            if v.len() != n {
                return Err(mismatch());
            }
            Value::Fixed(n, v.clone())
        }
        (PrimitiveType::Binary, Datum::Binary(v)) => Value::Bytes(v.clone()),
        (
            PrimitiveType::Decimal { precision, scale },
            Datum::Decimal {
                unscaled,
                scale: datum_scale,
            },
        ) => {
            if scale != datum_scale {
                return Err(mismatch());
            }
            let limit = 10i128
                .checked_pow(*precision)
                .ok_or_else(|| ManifestError::Shape("decimal precision overflow".into()))?;
            if unscaled.abs() >= limit {
                return Err(ManifestError::Shape(format!(
                    "decimal value {unscaled}e-{scale} exceeds precision {precision}"
                )));
            }
            // The library sign-extends minimal bytes to the fixed width.
            Value::Decimal(apache_avro::Decimal::from(
                Datum::Decimal {
                    unscaled: *unscaled,
                    scale: *scale,
                }
                .to_bound_bytes(),
            ))
        }
        _ => return Err(mismatch()),
    })
}
