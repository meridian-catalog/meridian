//! Mapping between the Iceberg table schema and the Arrow layer.
//!
//! Compaction reads input Parquet files as Arrow, so it needs to reason about
//! columns by **Iceberg field id**, not by name or position (the spec's rule,
//! and the whole point of field ids: a renamed column keeps its id). Parquet
//! carries the field id per column in the `PARQUET:field_id` Arrow field
//! metadata; this module reads it out and builds the target column order from
//! the table schema.

use std::collections::BTreeMap;

use arrow_schema::{Field, Schema as ArrowSchema};
use meridian_iceberg::spec::{Schema as IcebergSchema, Type};
use parquet::arrow::PARQUET_FIELD_ID_META_KEY;

/// The field id an Arrow field carries in its `PARQUET:field_id` metadata,
/// if any. Files written by real engines always set it on every column.
#[must_use]
pub fn field_id_of(field: &Field) -> Option<i32> {
    field
        .metadata()
        .get(PARQUET_FIELD_ID_META_KEY)
        .and_then(|s| s.trim().parse().ok())
}

/// The top-level Iceberg field ids of a schema, in schema order — the column
/// order the compacted output must present. Nested field ids are not returned:
/// compaction maps top-level columns by id and preserves each column's Arrow
/// value (including nested structure) verbatim, so nested ids ride along
/// untouched.
#[must_use]
pub fn top_level_field_ids(schema: &IcebergSchema) -> Vec<i32> {
    schema.fields.iter().map(|f| f.id).collect()
}

/// Maps each top-level Iceberg field id to its `(name, required)`; used to
/// name output columns and to decide nullability for synthesized columns. A
/// `BTreeMap` keeps iteration deterministic (and dodges the implicit-hasher
/// concern for the functions that take it by reference).
#[must_use]
pub fn top_level_fields(schema: &IcebergSchema) -> BTreeMap<i32, TopLevelField> {
    schema
        .fields
        .iter()
        .map(|f| {
            (
                f.id,
                TopLevelField {
                    name: f.name.clone(),
                    required: f.required,
                    is_primitive: matches!(f.field_type, Type::Primitive(_)),
                },
            )
        })
        .collect()
}

/// A top-level schema field's shape relevant to output construction.
#[derive(Debug, Clone)]
pub struct TopLevelField {
    /// Column name in the output.
    pub name: String,
    /// Whether the column is required (non-null) — a synthesized column for a
    /// field an input lacks must be nullable, so a required field that is
    /// genuinely absent from every input is a schema/data inconsistency the
    /// caller surfaces.
    pub required: bool,
    /// Whether the field is a primitive (bounds are computable) vs nested.
    pub is_primitive: bool,
}

/// Field id → column index within one input file's Arrow schema. Columns
/// without a field id (malformed) are skipped — they cannot be mapped. A
/// `BTreeMap` keeps lookups deterministic and lets consumers take it by
/// reference without pinning a hasher.
#[must_use]
pub fn field_id_index(arrow_schema: &ArrowSchema) -> BTreeMap<i32, usize> {
    let mut map = BTreeMap::new();
    for (idx, field) in arrow_schema.fields().iter().enumerate() {
        if let Some(id) = field_id_of(field) {
            map.insert(id, idx);
        }
    }
    map
}
