//! Turning Arrow result batches into the crate's arrow-free public shape: a
//! column list and a `Vec` of JSON row objects.
//!
//! We serialize with arrow-json's row writer, which emits exactly the
//! `[{"col": value, ...}, ...]` shape a caller wants — SQL `NULL` becomes JSON
//! `null` (present), numbers stay numbers, strings stay strings. The result
//! never carries Arrow types across the crate boundary, so a caller consumes it
//! without an Arrow dependency.

use arrow_array::RecordBatch;
use arrow_json::WriterBuilder;
use arrow_json::writer::JsonArray;
use serde_json::Value;

use crate::error::{QueryError, QueryResult};
use crate::types::Column;

/// Converts result batches to `(columns, rows)`. Column metadata comes from the
/// first batch's schema (all batches of one query share it); rows are the
/// concatenation of every batch's rows as JSON objects.
pub(crate) fn batches_to_rows(batches: &[RecordBatch]) -> QueryResult<(Vec<Column>, Vec<Value>)> {
    let columns = match batches.first() {
        Some(b) => b
            .schema()
            .fields()
            .iter()
            .map(|f| Column {
                name: f.name().clone(),
                data_type: format!("{}", f.data_type()),
            })
            .collect(),
        None => Vec::new(),
    };

    // Serialize all non-empty batches to one JSON array, then parse it back to
    // owned `Value`s. arrow-json writes an explicit `null` for SQL NULL, so no
    // column silently disappears from a row.
    let mut buf: Vec<u8> = Vec::new();
    {
        let mut writer = WriterBuilder::new()
            .with_explicit_nulls(true)
            .build::<_, JsonArray>(&mut buf);
        for batch in batches {
            if batch.num_rows() == 0 {
                continue;
            }
            writer
                .write(batch)
                .map_err(|e| QueryError::engine("serialize rows", e))?;
        }
        writer
            .finish()
            .map_err(|e| QueryError::engine("serialize rows", e))?;
    }

    let rows: Vec<Value> = if buf.is_empty() {
        Vec::new()
    } else {
        match serde_json::from_slice(&buf).map_err(|e| QueryError::engine("serialize rows", e))? {
            Value::Array(v) => v,
            _ => Vec::new(),
        }
    };

    Ok((columns, rows))
}
