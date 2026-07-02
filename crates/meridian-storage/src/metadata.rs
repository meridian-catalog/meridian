//! Metadata-file helpers: naming, reading, and immutably writing Iceberg
//! `metadata.json` files, for tables and views alike.
//!
//! These are thin on purpose. Parsing and serialization are delegated to
//! [`meridian_iceberg::spec::TableMetadata`] /
//! [`meridian_iceberg::spec::ViewMetadata`] (which preserve unknown fields
//! byte-for-byte); durability semantics are delegated to
//! [`Storage::write_if_absent`]. This module only fixes the conventions:
//! the file-name layout (shared between tables and views, per the view
//! spec) and the rule that a metadata file, once written, is never
//! overwritten.

use bytes::Bytes;
use meridian_iceberg::spec::{TableMetadata, ViewMetadata, ViewMetadataParseError};
use uuid::Uuid;

use crate::error::{StorageError, StorageResult};
use crate::storage::Storage;

/// Metadata format versions this build can act on (v2 is fully modelled;
/// v1/v3 files that satisfy the v2 required fields parse and round-trip).
const SUPPORTED_FORMAT_VERSIONS: std::ops::RangeInclusive<u8> = 1..=3;

/// Builds the location for a new metadata file under `table_location`,
/// following the Iceberg convention:
/// `<table_location>/metadata/<version padded to 5 digits>-<uuid>.metadata.json`.
///
/// `version` is the table's next pointer version; the zero-padded prefix
/// keeps lexicographic listing order equal to commit order for the first
/// 100k commits (and merely stops being padded, not wrong, after that).
#[must_use]
pub fn new_metadata_location(table_location: &str, version: u64, uuid: Uuid) -> String {
    let base = table_location.trim_end_matches('/');
    format!("{base}/metadata/{version:05}-{uuid}.metadata.json")
}

/// Builds the location for a new *view* metadata file under
/// `view_location`.
///
/// The view spec intentionally reuses the table layout
/// (`<location>/metadata/<version padded to 5 digits>-<uuid>.metadata.json`),
/// so this is the same convention as [`new_metadata_location`] under a
/// view's base location.
#[must_use]
pub fn new_view_metadata_location(view_location: &str, version: u64, uuid: Uuid) -> String {
    new_metadata_location(view_location, version, uuid)
}

/// Reads and parses the table metadata at `location`.
///
/// # Errors
///
/// - [`StorageError::NotFound`] if no object exists at `location`.
/// - [`StorageError::InvalidMetadata`] if the object is not UTF-8 JSON in
///   the Iceberg table-metadata shape.
/// - [`StorageError::UnsupportedFormatVersion`] if the file parses but
///   declares a `format-version` outside `1..=3`.
pub async fn read_table_metadata(
    storage: &dyn Storage,
    location: &str,
) -> StorageResult<TableMetadata> {
    let bytes = storage.read(location).await?;
    let text = std::str::from_utf8(&bytes).map_err(|err| StorageError::InvalidMetadata {
        location: location.to_owned(),
        message: "metadata file is not valid UTF-8".to_owned(),
        source: Some(Box::new(err)),
    })?;
    let metadata = TableMetadata::from_json(text).map_err(|err| StorageError::InvalidMetadata {
        location: location.to_owned(),
        message: format!("failed to parse table metadata: {err}"),
        source: Some(Box::new(err)),
    })?;
    if !SUPPORTED_FORMAT_VERSIONS.contains(&metadata.format_version) {
        return Err(StorageError::UnsupportedFormatVersion {
            location: location.to_owned(),
            found: metadata.format_version,
        });
    }
    Ok(metadata)
}

/// Serializes `metadata` and writes it to `location` with
/// [`Storage::write_if_absent`].
///
/// Metadata files are immutable: this fails with
/// [`StorageError::AlreadyExists`] rather than ever overwriting a published
/// file. Callers stage each commit attempt under a fresh
/// [`new_metadata_location`].
///
/// # Errors
///
/// - [`StorageError::AlreadyExists`] if an object already exists at
///   `location`.
/// - [`StorageError::InvalidMetadata`] if `metadata` fails to serialize.
pub async fn write_table_metadata(
    storage: &dyn Storage,
    location: &str,
    metadata: &TableMetadata,
) -> StorageResult<()> {
    let json = metadata
        .to_json()
        .map_err(|err| StorageError::InvalidMetadata {
            location: location.to_owned(),
            message: format!("failed to serialize table metadata: {err}"),
            source: Some(Box::new(err)),
        })?;
    storage.write_if_absent(location, Bytes::from(json)).await
}

/// Reads and parses the view metadata at `location`.
///
/// # Errors
///
/// - [`StorageError::NotFound`] if no object exists at `location`.
/// - [`StorageError::InvalidMetadata`] if the object is not UTF-8 JSON in
///   the Iceberg view-metadata shape.
/// - [`StorageError::UnsupportedFormatVersion`] if the file parses but
///   declares a `format-version` other than 1 (the only view format
///   version).
pub async fn read_view_metadata(
    storage: &dyn Storage,
    location: &str,
) -> StorageResult<ViewMetadata> {
    let bytes = storage.read(location).await?;
    let text = std::str::from_utf8(&bytes).map_err(|err| StorageError::InvalidMetadata {
        location: location.to_owned(),
        message: "metadata file is not valid UTF-8".to_owned(),
        source: Some(Box::new(err)),
    })?;
    match ViewMetadata::from_json(text) {
        Ok(metadata) => Ok(metadata),
        Err(ViewMetadataParseError::UnsupportedFormatVersion { found }) => {
            Err(StorageError::UnsupportedFormatVersion {
                location: location.to_owned(),
                found,
            })
        }
        Err(err) => Err(StorageError::InvalidMetadata {
            location: location.to_owned(),
            message: format!("failed to parse view metadata: {err}"),
            source: Some(Box::new(err)),
        }),
    }
}

/// Serializes `metadata` and writes it to `location` with
/// [`Storage::write_if_absent`].
///
/// View metadata files are immutable, exactly like table metadata files:
/// this fails with [`StorageError::AlreadyExists`] rather than ever
/// overwriting a published file. Callers stage each commit attempt under a
/// fresh [`new_view_metadata_location`].
///
/// # Errors
///
/// - [`StorageError::AlreadyExists`] if an object already exists at
///   `location`.
/// - [`StorageError::InvalidMetadata`] if `metadata` fails to serialize.
pub async fn write_view_metadata(
    storage: &dyn Storage,
    location: &str,
    metadata: &ViewMetadata,
) -> StorageResult<()> {
    let json = metadata
        .to_json()
        .map_err(|err| StorageError::InvalidMetadata {
            location: location.to_owned(),
            message: format!("failed to serialize view metadata: {err}"),
            source: Some(Box::new(err)),
        })?;
    storage.write_if_absent(location, Bytes::from(json)).await
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn metadata_location_follows_iceberg_convention() {
        let uuid = Uuid::nil();
        assert_eq!(
            new_metadata_location("s3://b/wh/db/tbl", 7, uuid),
            format!("s3://b/wh/db/tbl/metadata/00007-{uuid}.metadata.json")
        );
        // Trailing slash on the table location must not double up.
        assert_eq!(
            new_metadata_location("s3://b/wh/db/tbl/", 12345, uuid),
            format!("s3://b/wh/db/tbl/metadata/12345-{uuid}.metadata.json")
        );
        // Versions beyond 5 digits widen instead of truncating.
        assert!(
            new_metadata_location("s3://b/t", 123_456, uuid)
                .contains(&format!("metadata/123456-{uuid}"))
        );
    }

    #[test]
    fn view_metadata_location_matches_table_convention() {
        let uuid = Uuid::nil();
        assert_eq!(
            new_view_metadata_location("s3://b/wh/db/event_agg", 2, uuid),
            format!("s3://b/wh/db/event_agg/metadata/00002-{uuid}.metadata.json")
        );
        assert_eq!(
            new_view_metadata_location("s3://b/wh/db/event_agg/", 2, uuid),
            new_view_metadata_location("s3://b/wh/db/event_agg", 2, uuid)
        );
    }
}
