//! Reading manifest lists and manifests from Avro bytes.
//!
//! Resolution strategy: the Avro container header is parsed directly to
//! recover the *raw* writer schema JSON (including Iceberg attributes the
//! Avro library does not model, like `adjust-to-utc`) and the key-value
//! metadata. Fields are then located by their `field-id` attribute, with a
//! documented fallback to the spec's historical field names for files
//! written without ids. Record decoding itself is delegated to
//! [`apache_avro::Reader`].

use std::collections::BTreeMap;

use apache_avro::Reader;
use apache_avro::types::Value;

use super::{
    DataFile, DataFileContent, FieldSummary, Manifest, ManifestContentType, ManifestEntry,
    ManifestEntryStatus, ManifestError, ManifestFile, ManifestList, ManifestMetadata,
    PartitionTuple, PartitionValue, ids, partition_field_types,
};
use crate::spec::{PartitionField, PrimitiveType};
use crate::value::Datum;

/// Reads a manifest list (`snapshot.manifest-list` file) from Avro bytes.
pub fn read_manifest_list(bytes: &[u8]) -> Result<ManifestList, ManifestError> {
    let header = Header::parse(bytes)?;
    let bytes = header.decodable_bytes(bytes)?;
    let root = RecordShape::of(&header.schema, "manifest_file")?;
    let plan = ListPlan::build(&root);

    let mut manifests = Vec::new();
    for value in Reader::new(bytes.as_ref())? {
        let value = value?;
        let record = as_record(&value)?;
        manifests.push(plan.manifest_file(record)?);
    }
    Ok(ManifestList {
        format_version: header.meta_u8("format-version"),
        snapshot_id: header.meta_i64("snapshot-id"),
        parent_snapshot_id: header.meta_i64("parent-snapshot-id"),
        sequence_number: header.meta_i64("sequence-number"),
        manifests,
    })
}

/// Reads a manifest (data or delete) from Avro bytes.
///
/// Entries come back exactly as stored; apply
/// [`ManifestEntry::inherit_from`] with the owning manifest-list entry to
/// materialize inherited snapshot ids and sequence numbers.
pub fn read_manifest(bytes: &[u8]) -> Result<Manifest, ManifestError> {
    let header = Header::parse(bytes)?;
    let bytes = header.decodable_bytes(bytes)?;
    let root = RecordShape::of(&header.schema, "manifest_entry")?;
    let plan = EntryPlan::build(&root)?;

    let partition_fields: Vec<PartitionField> = match header.meta_str("partition-spec") {
        Some(json) => serde_json::from_str(&json).map_err(|e| {
            ManifestError::Shape(format!("unparseable partition-spec metadata: {e}"))
        })?,
        None => Vec::new(),
    };
    let content = match header.meta_str("content").as_deref() {
        None | Some("data") => ManifestContentType::Data,
        Some("deletes") => ManifestContentType::Deletes,
        Some(other) => {
            return Err(ManifestError::Shape(format!(
                "unknown manifest content metadata {other:?}"
            )));
        }
    };
    let metadata = ManifestMetadata {
        schema_json: header.meta_str("schema").unwrap_or_default(),
        schema_id: header.meta_i32("schema-id"),
        partition_fields,
        partition_spec_id: header.meta_i32("partition-spec-id"),
        format_version: header.meta_u8("format-version"),
        content,
    };

    let mut entries = Vec::new();
    for value in Reader::new(bytes.as_ref())? {
        let value = value?;
        let record = as_record(&value)?;
        entries.push(plan.entry(record)?);
    }
    let mut manifest = Manifest { metadata, entries };
    retype_partitions(&mut manifest);
    Ok(manifest)
}

/// Best-effort retyping of partition datums using the manifest's own
/// `schema` + `partition-spec` metadata: the Avro value typing alone
/// cannot distinguish `timestamptz` from `timestamp`, sees `uuid` as
/// `fixed(16)`, and (per the spec's day-transform note) must accept
/// plain `int` where a `date` is meant. Fields whose metadata is
/// missing, unparseable, or inconsistent keep their raw Avro typing.
fn retype_partitions(manifest: &mut Manifest) {
    let Ok(schema) = serde_json::from_str::<crate::spec::Schema>(&manifest.metadata.schema_json)
    else {
        return;
    };
    let Ok(types) = partition_field_types(&manifest.metadata.partition_fields, &schema) else {
        return;
    };
    for entry in &mut manifest.entries {
        for pv in &mut entry.data_file.partition.fields {
            let Some(pt) = types.iter().find(|t| t.field_id == pv.field_id) else {
                continue;
            };
            if let Some(datum) = pv.value.take() {
                pv.value = Some(retype_datum(datum, &pt.result_type));
            }
        }
    }
}

fn retype_datum(datum: Datum, ty: &PrimitiveType) -> Datum {
    match (datum, ty) {
        (Datum::Fixed(b) | Datum::Binary(b), PrimitiveType::Uuid) if b.len() == 16 => {
            let mut arr = [0u8; 16];
            arr.copy_from_slice(&b);
            Datum::Uuid(uuid::Uuid::from_bytes(arr))
        }
        // Readers must accept int values for the day transform.
        (Datum::Int(d), PrimitiveType::Date) => Datum::Date(d),
        (Datum::Long(v), PrimitiveType::Time) => Datum::Time(v),
        (Datum::Long(v) | Datum::Timestamp(v), PrimitiveType::Timestamptz) => Datum::Timestamptz(v),
        (Datum::Long(v), PrimitiveType::Timestamp) => Datum::Timestamp(v),
        (Datum::Long(v) | Datum::TimestampNs(v), PrimitiveType::TimestamptzNs) => {
            Datum::TimestamptzNs(v)
        }
        (Datum::Long(v), PrimitiveType::TimestampNs) => Datum::TimestampNs(v),
        (Datum::Binary(b), PrimitiveType::Fixed(_)) => Datum::Fixed(b),
        (Datum::Fixed(b), PrimitiveType::Binary) => Datum::Binary(b),
        (other, _) => other,
    }
}

/// The Avro object-container header: key-value metadata plus the raw
/// writer schema JSON (with all Iceberg attributes intact).
struct Header {
    meta: BTreeMap<String, Vec<u8>>,
    schema: serde_json::Value,
    /// Offset of the sync marker (end of the header metadata).
    body_offset: usize,
    /// Whether the schema needed the fixed-uuid patch (see
    /// [`Header::decodable_bytes`]).
    patched: bool,
}

impl Header {
    fn parse(bytes: &[u8]) -> Result<Self, ManifestError> {
        let mut cursor = Cursor { bytes, pos: 0 };
        let magic = cursor.take(4)?;
        if magic != b"Obj\x01" {
            return Err(ManifestError::Shape(
                "not an Avro object container file (bad magic)".to_owned(),
            ));
        }
        let mut meta = BTreeMap::new();
        loop {
            let count = cursor.zigzag_long()?;
            if count == 0 {
                break;
            }
            let count = if count < 0 {
                // Negative block count: followed by the block byte size.
                let _size = cursor.zigzag_long()?;
                count.unsigned_abs()
            } else {
                count.unsigned_abs()
            };
            for _ in 0..count {
                let key = cursor.string()?;
                let value = cursor.bytes()?;
                meta.insert(key, value);
            }
        }
        let body_offset = cursor.pos;
        let schema_bytes = meta
            .get("avro.schema")
            .ok_or_else(|| ManifestError::Shape("header has no avro.schema".to_owned()))?;
        let mut schema = serde_json::from_slice(schema_bytes)
            .map_err(|e| ManifestError::Shape(format!("unparseable avro.schema: {e}")))?;
        let patched = strip_fixed_uuid_logical(&mut schema);
        Ok(Self {
            meta,
            schema,
            body_offset,
            patched,
        })
    }

    /// The bytes to hand to the Avro decoder. Normally the input,
    /// borrowed; when the writer schema types a `fixed` as the `uuid`
    /// logical type, a copy with that annotation stripped from the
    /// header — the Avro library would otherwise decode the fixed-width
    /// value as a length-prefixed one and misread the stream.
    fn decodable_bytes<'a>(
        &self,
        bytes: &'a [u8],
    ) -> Result<std::borrow::Cow<'a, [u8]>, ManifestError> {
        if !self.patched {
            return Ok(std::borrow::Cow::Borrowed(bytes));
        }
        let schema_bytes = serde_json::to_vec(&self.schema)
            .map_err(|e| ManifestError::Shape(format!("unserializable patched schema: {e}")))?;
        let mut out = Vec::with_capacity(bytes.len() + schema_bytes.len());
        out.extend_from_slice(b"Obj\x01");
        // One metadata block with every original entry, schema replaced.
        write_zigzag(&mut out, i64::try_from(self.meta.len()).unwrap_or(i64::MAX));
        for (key, value) in &self.meta {
            let value = if key == "avro.schema" {
                schema_bytes.as_slice()
            } else {
                value.as_slice()
            };
            write_zigzag(&mut out, i64::try_from(key.len()).unwrap_or(0));
            out.extend_from_slice(key.as_bytes());
            write_zigzag(&mut out, i64::try_from(value.len()).unwrap_or(0));
            out.extend_from_slice(value);
        }
        write_zigzag(&mut out, 0);
        out.extend_from_slice(&bytes[self.body_offset..]);
        Ok(std::borrow::Cow::Owned(out))
    }

    fn meta_str(&self, key: &str) -> Option<String> {
        self.meta
            .get(key)
            .map(|v| String::from_utf8_lossy(v).into_owned())
    }

    fn meta_i64(&self, key: &str) -> Option<i64> {
        self.meta_str(key).and_then(|s| s.trim().parse().ok())
    }

    fn meta_i32(&self, key: &str) -> Option<i32> {
        self.meta_str(key).and_then(|s| s.trim().parse().ok())
    }

    fn meta_u8(&self, key: &str) -> Option<u8> {
        self.meta_str(key).and_then(|s| s.trim().parse().ok())
    }
}

/// Removes `"logicalType": "uuid"` from every `fixed` schema in the
/// tree, returning whether anything changed. (The Java implementation
/// stores uuid values as `fixed(16)` with the `uuid` logical type; the
/// Avro library mis-decodes that combination.)
fn strip_fixed_uuid_logical(schema: &mut serde_json::Value) -> bool {
    match schema {
        serde_json::Value::Object(obj) => {
            let mut changed = false;
            let is_fixed_uuid = obj.get("type").and_then(serde_json::Value::as_str)
                == Some("fixed")
                && obj.get("logicalType").and_then(serde_json::Value::as_str) == Some("uuid");
            if is_fixed_uuid {
                obj.remove("logicalType");
                changed = true;
            }
            for value in obj.values_mut() {
                changed |= strip_fixed_uuid_logical(value);
            }
            changed
        }
        serde_json::Value::Array(items) => {
            let mut changed = false;
            for item in items {
                changed |= strip_fixed_uuid_logical(item);
            }
            changed
        }
        _ => false,
    }
}

#[allow(clippy::cast_sign_loss)] // zig-zag encoding is a bit-level transform
fn write_zigzag(out: &mut Vec<u8>, value: i64) {
    let mut encoded = ((value << 1) ^ (value >> 63)) as u64;
    loop {
        let mut byte = u8::try_from(encoded & 0x7F).unwrap_or(0);
        encoded >>= 7;
        if encoded != 0 {
            byte |= 0x80;
        }
        out.push(byte);
        if encoded == 0 {
            break;
        }
    }
}

struct Cursor<'a> {
    bytes: &'a [u8],
    pos: usize,
}

impl Cursor<'_> {
    fn take(&mut self, n: usize) -> Result<&[u8], ManifestError> {
        let end = self.pos.checked_add(n).filter(|&e| e <= self.bytes.len());
        match end {
            Some(end) => {
                let slice = &self.bytes[self.pos..end];
                self.pos = end;
                Ok(slice)
            }
            None => Err(ManifestError::Shape(
                "truncated Avro container header".to_owned(),
            )),
        }
    }

    fn zigzag_long(&mut self) -> Result<i64, ManifestError> {
        let mut shift = 0u32;
        let mut acc: u64 = 0;
        loop {
            let byte = self.take(1)?[0];
            acc |= u64::from(byte & 0x7F)
                .checked_shl(shift)
                .ok_or_else(|| ManifestError::Shape("varint overflow in header".to_owned()))?;
            if byte & 0x80 == 0 {
                break;
            }
            shift += 7;
            if shift > 63 {
                return Err(ManifestError::Shape("varint overflow in header".to_owned()));
            }
        }
        #[allow(clippy::cast_possible_wrap)] // zig-zag decoding is wrapping by design
        Ok(((acc >> 1) as i64) ^ -((acc & 1) as i64))
    }

    fn len_prefixed(&mut self) -> Result<&[u8], ManifestError> {
        let len = self.zigzag_long()?;
        let len = usize::try_from(len)
            .map_err(|_| ManifestError::Shape("negative length in header".to_owned()))?;
        self.take(len)
    }

    fn string(&mut self) -> Result<String, ManifestError> {
        let raw = self.len_prefixed()?;
        String::from_utf8(raw.to_vec())
            .map_err(|_| ManifestError::Shape("non-UTF-8 metadata key".to_owned()))
    }

    fn bytes(&mut self) -> Result<Vec<u8>, ManifestError> {
        let raw = self.len_prefixed()?;
        Ok(raw.to_vec())
    }
}

/// One field of a record schema, as raw JSON.
#[derive(Debug, Clone)]
struct FieldShape {
    name: String,
    field_id: Option<i64>,
    /// The field's type JSON with a `["null", X]` union unwrapped to `X`.
    type_json: serde_json::Value,
}

/// The shape of an Avro record schema: its fields in writer order.
#[derive(Debug, Clone)]
struct RecordShape {
    fields: Vec<FieldShape>,
}

impl RecordShape {
    /// Interprets `schema` as a record; `what` names it for errors.
    fn of(schema: &serde_json::Value, what: &str) -> Result<Self, ManifestError> {
        let fields = schema
            .get("fields")
            .and_then(|f| f.as_array())
            .ok_or_else(|| ManifestError::Shape(format!("writer schema is not a {what} record")))?;
        let fields = fields
            .iter()
            .map(|f| {
                let name = f
                    .get("name")
                    .and_then(|n| n.as_str())
                    .ok_or_else(|| ManifestError::Shape("record field without a name".into()))?
                    .to_owned();
                let field_id = f.get("field-id").and_then(serde_json::Value::as_i64);
                let type_json = unwrap_nullable(f.get("type").cloned().unwrap_or_default());
                Ok(FieldShape {
                    name,
                    field_id,
                    type_json,
                })
            })
            .collect::<Result<Vec<_>, ManifestError>>()?;
        Ok(Self { fields })
    }

    /// Position of a field by field id, falling back to any of the given
    /// historical names when the writer stored no ids.
    fn index(&self, field_id: i64, names: &[&str]) -> Option<usize> {
        if let Some(pos) = self
            .fields
            .iter()
            .position(|f| f.field_id == Some(field_id))
        {
            return Some(pos);
        }
        self.fields
            .iter()
            .position(|f| f.field_id.is_none() && names.contains(&f.name.as_str()))
    }

    fn nested(&self, index: usize, what: &str) -> Result<Self, ManifestError> {
        Self::of(&self.fields[index].type_json, what)
    }
}

/// Unwraps `["null", X]` (or `[X, "null"]`) unions to `X`.
fn unwrap_nullable(type_json: serde_json::Value) -> serde_json::Value {
    if let serde_json::Value::Array(branches) = &type_json {
        let non_null: Vec<&serde_json::Value> = branches
            .iter()
            .filter(|b| b.as_str() != Some("null"))
            .collect();
        if let [only] = non_null.as_slice() {
            return (*only).clone();
        }
    }
    type_json
}

/// Precomputed positions for `manifest_file` fields.
struct ListPlan {
    manifest_path: Option<usize>,
    manifest_length: Option<usize>,
    partition_spec_id: Option<usize>,
    content: Option<usize>,
    sequence_number: Option<usize>,
    min_sequence_number: Option<usize>,
    added_snapshot_id: Option<usize>,
    added_files_count: Option<usize>,
    existing_files_count: Option<usize>,
    deleted_files_count: Option<usize>,
    added_rows_count: Option<usize>,
    existing_rows_count: Option<usize>,
    deleted_rows_count: Option<usize>,
    partitions: Option<usize>,
    key_metadata: Option<usize>,
    first_row_id: Option<usize>,
    summary: Option<SummaryPlan>,
}

/// Precomputed positions for `field_summary` fields.
struct SummaryPlan {
    contains_null: Option<usize>,
    contains_nan: Option<usize>,
    lower_bound: Option<usize>,
    upper_bound: Option<usize>,
}

impl ListPlan {
    fn build(root: &RecordShape) -> Self {
        let partitions = root.index(ids::PARTITIONS, &["partitions"]);
        let summary = partitions.and_then(|idx| {
            // partitions: array of field_summary records.
            let items = root.fields[idx].type_json.get("items")?;
            let shape = RecordShape::of(items, "field_summary").ok()?;
            Some(SummaryPlan {
                contains_null: shape.index(ids::CONTAINS_NULL, &["contains_null"]),
                contains_nan: shape.index(ids::CONTAINS_NAN, &["contains_nan"]),
                lower_bound: shape.index(ids::SUMMARY_LOWER, &["lower_bound"]),
                upper_bound: shape.index(ids::SUMMARY_UPPER, &["upper_bound"]),
            })
        });
        Self {
            manifest_path: root.index(ids::MANIFEST_PATH, &["manifest_path"]),
            manifest_length: root.index(ids::MANIFEST_LENGTH, &["manifest_length"]),
            partition_spec_id: root.index(ids::PARTITION_SPEC_ID, &["partition_spec_id"]),
            content: root.index(ids::MANIFEST_CONTENT, &["content"]),
            sequence_number: root.index(ids::SEQUENCE_NUMBER, &["sequence_number"]),
            min_sequence_number: root.index(ids::MIN_SEQUENCE_NUMBER, &["min_sequence_number"]),
            added_snapshot_id: root.index(ids::ADDED_SNAPSHOT_ID, &["added_snapshot_id"]),
            added_files_count: root.index(
                ids::ADDED_FILES_COUNT,
                &["added_files_count", "added_data_files_count"],
            ),
            existing_files_count: root.index(
                ids::EXISTING_FILES_COUNT,
                &["existing_files_count", "existing_data_files_count"],
            ),
            deleted_files_count: root.index(
                ids::DELETED_FILES_COUNT,
                &["deleted_files_count", "deleted_data_files_count"],
            ),
            added_rows_count: root.index(ids::ADDED_ROWS_COUNT, &["added_rows_count"]),
            existing_rows_count: root.index(ids::EXISTING_ROWS_COUNT, &["existing_rows_count"]),
            deleted_rows_count: root.index(ids::DELETED_ROWS_COUNT, &["deleted_rows_count"]),
            partitions,
            key_metadata: root.index(ids::KEY_METADATA, &["key_metadata"]),
            first_row_id: root.index(ids::FIRST_ROW_ID, &["first_row_id"]),
            summary,
        }
    }

    fn manifest_file(&self, record: &[(String, Value)]) -> Result<ManifestFile, ManifestError> {
        let content = match opt_i32(record, self.content)? {
            Some(code) => ManifestContentType::from_code(code)?,
            None => ManifestContentType::Data, // v1 lists have no content field
        };
        let partitions = match self.partitions.and_then(|idx| non_null(record, idx)) {
            Some(Value::Array(items)) => {
                let plan = self.summary.as_ref().ok_or_else(|| {
                    ManifestError::Shape("partitions array without field_summary schema".into())
                })?;
                Some(
                    items
                        .iter()
                        .map(|item| plan.summary(as_record(item)?))
                        .collect::<Result<Vec<_>, _>>()?,
                )
            }
            Some(other) => {
                return Err(ManifestError::Shape(format!(
                    "partitions is not an array: {}",
                    kind(other)
                )));
            }
            None => None,
        };
        Ok(ManifestFile {
            manifest_path: req_string(record, self.manifest_path, "manifest_path")?,
            manifest_length: req_i64(record, self.manifest_length, "manifest_length")?,
            partition_spec_id: i32::try_from(req_i64(
                record,
                self.partition_spec_id,
                "partition_spec_id",
            )?)
            .map_err(|_| ManifestError::Shape("partition_spec_id out of range".into()))?,
            content,
            sequence_number: opt_i64(record, self.sequence_number)?.unwrap_or(0),
            min_sequence_number: opt_i64(record, self.min_sequence_number)?.unwrap_or(0),
            added_snapshot_id: req_i64(record, self.added_snapshot_id, "added_snapshot_id")?,
            added_files_count: opt_i32(record, self.added_files_count)?,
            existing_files_count: opt_i32(record, self.existing_files_count)?,
            deleted_files_count: opt_i32(record, self.deleted_files_count)?,
            added_rows_count: opt_i64(record, self.added_rows_count)?,
            existing_rows_count: opt_i64(record, self.existing_rows_count)?,
            deleted_rows_count: opt_i64(record, self.deleted_rows_count)?,
            partitions,
            key_metadata: opt_bytes(record, self.key_metadata)?,
            first_row_id: opt_i64(record, self.first_row_id)?,
        })
    }
}

impl SummaryPlan {
    fn summary(&self, record: &[(String, Value)]) -> Result<FieldSummary, ManifestError> {
        Ok(FieldSummary {
            contains_null: opt_bool(record, self.contains_null)?.unwrap_or(false),
            contains_nan: opt_bool(record, self.contains_nan)?,
            lower_bound: opt_bytes(record, self.lower_bound)?,
            upper_bound: opt_bytes(record, self.upper_bound)?,
        })
    }
}

/// Precomputed positions for `manifest_entry` and its `data_file`.
struct EntryPlan {
    status: Option<usize>,
    snapshot_id: Option<usize>,
    sequence_number: Option<usize>,
    file_sequence_number: Option<usize>,
    data_file: usize,
    df: DataFilePlan,
}

struct DataFilePlan {
    content: Option<usize>,
    file_path: Option<usize>,
    file_format: Option<usize>,
    partition: Option<usize>,
    record_count: Option<usize>,
    file_size_in_bytes: Option<usize>,
    column_sizes: Option<usize>,
    value_counts: Option<usize>,
    null_value_counts: Option<usize>,
    nan_value_counts: Option<usize>,
    lower_bounds: Option<usize>,
    upper_bounds: Option<usize>,
    key_metadata: Option<usize>,
    split_offsets: Option<usize>,
    equality_ids: Option<usize>,
    sort_order_id: Option<usize>,
    first_row_id: Option<usize>,
    referenced_data_file: Option<usize>,
    content_offset: Option<usize>,
    content_size_in_bytes: Option<usize>,
    partition_fields: Vec<PartitionFieldShape>,
}

/// Shape of one partition tuple field from the writer schema.
struct PartitionFieldShape {
    name: String,
    field_id: Option<i64>,
    /// Scale for decimal values (the Avro `Value::Decimal` carries none).
    decimal_scale: Option<u32>,
    /// The Iceberg `adjust-to-utc` attribute (distinguishes timestamptz
    /// from timestamp; the Avro logical type alone cannot).
    adjust_to_utc: bool,
}

impl EntryPlan {
    fn build(root: &RecordShape) -> Result<Self, ManifestError> {
        let data_file = root
            .index(ids::ENTRY_DATA_FILE, &["data_file"])
            .ok_or_else(|| ManifestError::Shape("manifest_entry has no data_file".into()))?;
        let df_shape = root.nested(data_file, "data_file")?;
        let partition = df_shape.index(ids::DF_PARTITION, &["partition"]);
        let partition_fields = match partition {
            Some(idx) => RecordShape::of(&df_shape.fields[idx].type_json, "partition")?
                .fields
                .into_iter()
                .map(|f| PartitionFieldShape {
                    decimal_scale: f
                        .type_json
                        .get("scale")
                        .and_then(serde_json::Value::as_u64)
                        .and_then(|s| u32::try_from(s).ok()),
                    adjust_to_utc: f
                        .type_json
                        .get("adjust-to-utc")
                        .and_then(serde_json::Value::as_bool)
                        == Some(true),
                    name: f.name,
                    field_id: f.field_id,
                })
                .collect(),
            None => Vec::new(),
        };
        let df = DataFilePlan {
            content: df_shape.index(ids::DF_CONTENT, &["content"]),
            file_path: df_shape.index(ids::DF_FILE_PATH, &["file_path"]),
            file_format: df_shape.index(ids::DF_FILE_FORMAT, &["file_format"]),
            partition,
            record_count: df_shape.index(ids::DF_RECORD_COUNT, &["record_count"]),
            file_size_in_bytes: df_shape.index(ids::DF_FILE_SIZE, &["file_size_in_bytes"]),
            column_sizes: df_shape.index(ids::DF_COLUMN_SIZES, &["column_sizes"]),
            value_counts: df_shape.index(ids::DF_VALUE_COUNTS, &["value_counts"]),
            null_value_counts: df_shape.index(ids::DF_NULL_VALUE_COUNTS, &["null_value_counts"]),
            nan_value_counts: df_shape.index(ids::DF_NAN_VALUE_COUNTS, &["nan_value_counts"]),
            lower_bounds: df_shape.index(ids::DF_LOWER_BOUNDS, &["lower_bounds"]),
            upper_bounds: df_shape.index(ids::DF_UPPER_BOUNDS, &["upper_bounds"]),
            key_metadata: df_shape.index(ids::DF_KEY_METADATA, &["key_metadata"]),
            split_offsets: df_shape.index(ids::DF_SPLIT_OFFSETS, &["split_offsets"]),
            equality_ids: df_shape.index(ids::DF_EQUALITY_IDS, &["equality_ids"]),
            sort_order_id: df_shape.index(ids::DF_SORT_ORDER_ID, &["sort_order_id"]),
            first_row_id: df_shape.index(ids::DF_FIRST_ROW_ID, &["first_row_id"]),
            referenced_data_file: df_shape
                .index(ids::DF_REFERENCED_DATA_FILE, &["referenced_data_file"]),
            content_offset: df_shape.index(ids::DF_CONTENT_OFFSET, &["content_offset"]),
            content_size_in_bytes: df_shape.index(ids::DF_CONTENT_SIZE, &["content_size_in_bytes"]),
            partition_fields,
        };
        Ok(Self {
            status: root.index(ids::ENTRY_STATUS, &["status"]),
            snapshot_id: root.index(ids::ENTRY_SNAPSHOT_ID, &["snapshot_id"]),
            sequence_number: root.index(
                ids::ENTRY_SEQUENCE_NUMBER,
                &["sequence_number", "data_sequence_number"],
            ),
            file_sequence_number: root
                .index(ids::ENTRY_FILE_SEQUENCE_NUMBER, &["file_sequence_number"]),
            data_file,
            df,
        })
    }

    fn entry(&self, record: &[(String, Value)]) -> Result<ManifestEntry, ManifestError> {
        let status_code = req_i64(record, self.status, "status")?;
        let status = ManifestEntryStatus::from_code(
            i32::try_from(status_code)
                .map_err(|_| ManifestError::Shape("status out of range".into()))?,
        )?;
        let df_value = non_null(record, self.data_file)
            .ok_or_else(|| ManifestError::Shape("manifest_entry has null data_file".into()))?;
        let data_file = self.df.data_file(as_record(df_value)?)?;
        Ok(ManifestEntry {
            status,
            snapshot_id: opt_i64(record, self.snapshot_id)?,
            sequence_number: opt_i64(record, self.sequence_number)?,
            file_sequence_number: opt_i64(record, self.file_sequence_number)?,
            data_file,
        })
    }
}

impl DataFilePlan {
    fn data_file(&self, record: &[(String, Value)]) -> Result<DataFile, ManifestError> {
        let content = match opt_i32(record, self.content)? {
            Some(code) => DataFileContent::from_code(code)?,
            None => DataFileContent::Data, // v1 files are all data files
        };
        let partition = match self.partition.and_then(|idx| non_null(record, idx)) {
            Some(value) => self.partition_tuple(as_record(value)?)?,
            None => PartitionTuple::default(),
        };
        Ok(DataFile {
            content,
            file_path: req_string(record, self.file_path, "file_path")?,
            file_format: req_string(record, self.file_format, "file_format")?,
            partition,
            record_count: req_i64(record, self.record_count, "record_count")?,
            file_size_in_bytes: req_i64(record, self.file_size_in_bytes, "file_size_in_bytes")?,
            column_sizes: opt_count_map(record, self.column_sizes)?,
            value_counts: opt_count_map(record, self.value_counts)?,
            null_value_counts: opt_count_map(record, self.null_value_counts)?,
            nan_value_counts: opt_count_map(record, self.nan_value_counts)?,
            lower_bounds: opt_bytes_map(record, self.lower_bounds)?,
            upper_bounds: opt_bytes_map(record, self.upper_bounds)?,
            key_metadata: opt_bytes(record, self.key_metadata)?,
            split_offsets: opt_i64_array(record, self.split_offsets)?,
            equality_ids: opt_i32_array(record, self.equality_ids)?,
            sort_order_id: opt_i32(record, self.sort_order_id)?,
            first_row_id: opt_i64(record, self.first_row_id)?,
            referenced_data_file: opt_string(record, self.referenced_data_file)?,
            content_offset: opt_i64(record, self.content_offset)?,
            content_size_in_bytes: opt_i64(record, self.content_size_in_bytes)?,
        })
    }

    fn partition_tuple(&self, record: &[(String, Value)]) -> Result<PartitionTuple, ManifestError> {
        let mut fields = Vec::with_capacity(record.len());
        for (pos, (name, value)) in record.iter().enumerate() {
            let shape = self.partition_fields.get(pos);
            // Positions align because both come from the writer schema; the
            // name check guards against any disagreement.
            let shape = match shape {
                Some(s) if s.name == *name => Some(s),
                _ => self.partition_fields.iter().find(|s| s.name == *name),
            };
            let field_id = shape.and_then(|s| s.field_id).unwrap_or(-1);
            let datum = partition_datum(value, shape)?;
            fields.push(PartitionValue {
                field_id: i32::try_from(field_id)
                    .map_err(|_| ManifestError::Shape("partition field-id out of range".into()))?,
                name: name.clone(),
                value: datum,
            });
        }
        Ok(PartitionTuple { fields })
    }
}

/// Converts one Avro partition value into a typed datum (`None` = null).
fn partition_datum(
    value: &Value,
    shape: Option<&PartitionFieldShape>,
) -> Result<Option<Datum>, ManifestError> {
    let value = unwrap_union_value(value);
    let adjust_to_utc = shape.is_some_and(|s| s.adjust_to_utc);
    let datum = match value {
        Value::Null => return Ok(None),
        Value::Boolean(b) => Datum::Boolean(*b),
        Value::Int(v) => Datum::Int(*v),
        Value::Long(v) => Datum::Long(*v),
        Value::Float(v) => Datum::float(*v),
        Value::Double(v) => Datum::double(*v),
        Value::String(s) => Datum::String(s.clone()),
        Value::Bytes(b) => Datum::Binary(b.clone()),
        Value::Fixed(_, b) => Datum::Fixed(b.clone()),
        Value::Date(d) => Datum::Date(*d),
        Value::TimeMicros(v) => Datum::Time(*v),
        Value::TimeMillis(v) => Datum::Time(i64::from(*v) * 1000),
        Value::TimestampMicros(v) | Value::LocalTimestampMicros(v) => {
            if adjust_to_utc {
                Datum::Timestamptz(*v)
            } else {
                Datum::Timestamp(*v)
            }
        }
        Value::TimestampMillis(v) | Value::LocalTimestampMillis(v) => {
            let micros = v.checked_mul(1000).ok_or_else(|| {
                ManifestError::Shape("millisecond timestamp overflows micros".into())
            })?;
            if adjust_to_utc {
                Datum::Timestamptz(micros)
            } else {
                Datum::Timestamp(micros)
            }
        }
        Value::TimestampNanos(v) | Value::LocalTimestampNanos(v) => {
            if adjust_to_utc {
                Datum::TimestamptzNs(*v)
            } else {
                Datum::TimestampNs(*v)
            }
        }
        Value::Uuid(u) => Datum::Uuid(*u),
        Value::Decimal(d) => {
            let bytes: Vec<u8> = <Vec<u8>>::try_from(d)
                .map_err(|e| ManifestError::Shape(format!("undecodable decimal: {e}")))?;
            let scale = shape.and_then(|s| s.decimal_scale).unwrap_or(0);
            let unscaled = be_i128(&bytes)?;
            Datum::Decimal { unscaled, scale }
        }
        other => {
            return Err(ManifestError::Shape(format!(
                "unsupported partition value kind {}",
                kind(other)
            )));
        }
    };
    Ok(Some(datum))
}

fn be_i128(bytes: &[u8]) -> Result<i128, ManifestError> {
    if bytes.is_empty() || bytes.len() > 16 {
        return Err(ManifestError::Shape(format!(
            "decimal of {} bytes exceeds 16",
            bytes.len()
        )));
    }
    let negative = bytes[0] & 0x80 != 0;
    let mut arr = if negative { [0xFFu8; 16] } else { [0u8; 16] };
    arr[16 - bytes.len()..].copy_from_slice(bytes);
    Ok(i128::from_be_bytes(arr))
}

// ---- Avro value navigation helpers ----

fn kind(value: &Value) -> &'static str {
    match value {
        Value::Null => "null",
        Value::Boolean(_) => "boolean",
        Value::Int(_) => "int",
        Value::Long(_) => "long",
        Value::Float(_) => "float",
        Value::Double(_) => "double",
        Value::Bytes(_) => "bytes",
        Value::String(_) => "string",
        Value::Fixed(..) => "fixed",
        Value::Enum(..) => "enum",
        Value::Union(..) => "union",
        Value::Array(_) => "array",
        Value::Map(_) => "map",
        Value::Record(_) => "record",
        _ => "logical",
    }
}

fn as_record(value: &Value) -> Result<&[(String, Value)], ManifestError> {
    match unwrap_union_value(value) {
        Value::Record(fields) => Ok(fields),
        other => Err(ManifestError::Shape(format!(
            "expected a record, got {}",
            kind(other)
        ))),
    }
}

fn unwrap_union_value(value: &Value) -> &Value {
    match value {
        Value::Union(_, inner) => unwrap_union_value(inner),
        other => other,
    }
}

/// The field at `index`, with nulls normalized away (`None` = absent field
/// or null value).
fn non_null(record: &[(String, Value)], index: usize) -> Option<&Value> {
    let (_, value) = record.get(index)?;
    match unwrap_union_value(value) {
        Value::Null => None,
        other => Some(other),
    }
}

fn opt_at(record: &[(String, Value)], index: Option<usize>) -> Option<&Value> {
    index.and_then(|idx| non_null(record, idx))
}

fn value_i64(value: &Value) -> Result<i64, ManifestError> {
    match value {
        Value::Int(v) => Ok(i64::from(*v)),
        Value::Long(v) => Ok(*v),
        other => Err(ManifestError::Shape(format!(
            "expected an integer, got {}",
            kind(other)
        ))),
    }
}

fn req_i64(
    record: &[(String, Value)],
    index: Option<usize>,
    what: &str,
) -> Result<i64, ManifestError> {
    match opt_at(record, index) {
        Some(v) => value_i64(v),
        None => Err(ManifestError::Shape(format!("missing required {what}"))),
    }
}

fn opt_i64(record: &[(String, Value)], index: Option<usize>) -> Result<Option<i64>, ManifestError> {
    opt_at(record, index).map(value_i64).transpose()
}

fn opt_i32(record: &[(String, Value)], index: Option<usize>) -> Result<Option<i32>, ManifestError> {
    match opt_i64(record, index)? {
        Some(v) => Ok(Some(i32::try_from(v).map_err(|_| {
            ManifestError::Shape("integer out of i32 range".into())
        })?)),
        None => Ok(None),
    }
}

fn req_string(
    record: &[(String, Value)],
    index: Option<usize>,
    what: &str,
) -> Result<String, ManifestError> {
    match opt_string(record, index)? {
        Some(s) => Ok(s),
        None => Err(ManifestError::Shape(format!("missing required {what}"))),
    }
}

fn opt_string(
    record: &[(String, Value)],
    index: Option<usize>,
) -> Result<Option<String>, ManifestError> {
    match opt_at(record, index) {
        Some(Value::String(s) | Value::Enum(_, s)) => Ok(Some(s.clone())),
        Some(other) => Err(ManifestError::Shape(format!(
            "expected a string, got {}",
            kind(other)
        ))),
        None => Ok(None),
    }
}

fn opt_bool(
    record: &[(String, Value)],
    index: Option<usize>,
) -> Result<Option<bool>, ManifestError> {
    match opt_at(record, index) {
        Some(Value::Boolean(b)) => Ok(Some(*b)),
        Some(other) => Err(ManifestError::Shape(format!(
            "expected a boolean, got {}",
            kind(other)
        ))),
        None => Ok(None),
    }
}

fn opt_bytes(
    record: &[(String, Value)],
    index: Option<usize>,
) -> Result<Option<Vec<u8>>, ManifestError> {
    match opt_at(record, index) {
        Some(Value::Bytes(b) | Value::Fixed(_, b)) => Ok(Some(b.clone())),
        Some(other) => Err(ManifestError::Shape(format!(
            "expected bytes, got {}",
            kind(other)
        ))),
        None => Ok(None),
    }
}

fn opt_i64_array(
    record: &[(String, Value)],
    index: Option<usize>,
) -> Result<Option<Vec<i64>>, ManifestError> {
    match opt_at(record, index) {
        Some(Value::Array(items)) => Ok(Some(
            items
                .iter()
                .map(|v| value_i64(unwrap_union_value(v)))
                .collect::<Result<_, _>>()?,
        )),
        Some(other) => Err(ManifestError::Shape(format!(
            "expected an array, got {}",
            kind(other)
        ))),
        None => Ok(None),
    }
}

fn opt_i32_array(
    record: &[(String, Value)],
    index: Option<usize>,
) -> Result<Option<Vec<i32>>, ManifestError> {
    match opt_i64_array(record, index)? {
        Some(v) => Ok(Some(
            v.into_iter()
                .map(|x| {
                    i32::try_from(x)
                        .map_err(|_| ManifestError::Shape("integer out of i32 range".into()))
                })
                .collect::<Result<_, _>>()?,
        )),
        None => Ok(None),
    }
}

/// Reads a spec `map<int, long>`: either an array of `{key, value}`
/// records (the required form for non-string keys) or a genuine Avro map
/// with stringified integer keys (tolerated).
fn opt_count_map(
    record: &[(String, Value)],
    index: Option<usize>,
) -> Result<Option<BTreeMap<i32, i64>>, ManifestError> {
    kv_map(record, index, value_i64)
}

/// Reads a spec `map<int, binary>` of bound values.
fn opt_bytes_map(
    record: &[(String, Value)],
    index: Option<usize>,
) -> Result<Option<BTreeMap<i32, Vec<u8>>>, ManifestError> {
    kv_map(record, index, |v| match v {
        Value::Bytes(b) | Value::Fixed(_, b) => Ok(b.clone()),
        other => Err(ManifestError::Shape(format!(
            "expected bytes in bounds map, got {}",
            kind(other)
        ))),
    })
}

fn kv_map<T>(
    record: &[(String, Value)],
    index: Option<usize>,
    mut convert: impl FnMut(&Value) -> Result<T, ManifestError>,
) -> Result<Option<BTreeMap<i32, T>>, ManifestError> {
    let Some(value) = opt_at(record, index) else {
        return Ok(None);
    };
    let mut out = BTreeMap::new();
    match value {
        Value::Array(items) => {
            for item in items {
                let rec = as_record(item)?;
                let key = rec
                    .iter()
                    .find(|(n, _)| n == "key")
                    .map(|(_, v)| value_i64(unwrap_union_value(v)))
                    .transpose()?
                    .ok_or_else(|| ManifestError::Shape("kv record without key".into()))?;
                let val = rec
                    .iter()
                    .find(|(n, _)| n == "value")
                    .map(|(_, v)| convert(unwrap_union_value(v)))
                    .transpose()?
                    .ok_or_else(|| ManifestError::Shape("kv record without value".into()))?;
                out.insert(
                    i32::try_from(key)
                        .map_err(|_| ManifestError::Shape("map key out of i32 range".into()))?,
                    val,
                );
            }
        }
        Value::Map(entries) => {
            for (key, v) in entries {
                let key: i32 = key
                    .trim()
                    .parse()
                    .map_err(|_| ManifestError::Shape(format!("non-integer map key {key:?}")))?;
                out.insert(key, convert(unwrap_union_value(v))?);
            }
        }
        other => {
            return Err(ManifestError::Shape(format!(
                "expected a keyed map, got {}",
                kind(other)
            )));
        }
    }
    Ok(Some(out))
}
