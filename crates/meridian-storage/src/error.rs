//! Strongly typed object-storage errors.
//!
//! Every fallible operation in this crate returns [`StorageError`]. The
//! variants are deliberately few and semantic — callers on the commit path
//! branch on *meaning* (`NotFound`, `AlreadyExists`, `Transient { retryable }`)
//! rather than on backend-specific detail. The underlying cause is always
//! preserved in the `source` chain for logs.

use std::error::Error as StdError;

/// Convenience alias for results produced by this crate.
pub type StorageResult<T> = std::result::Result<T, StorageError>;

/// Boxed error source for wrapping backend failures.
type BoxError = Box<dyn StdError + Send + Sync + 'static>;

/// The unified error type for object-storage operations.
#[derive(Debug, thiserror::Error)]
pub enum StorageError {
    /// The object does not exist.
    #[error("object not found: {location}")]
    NotFound {
        /// Location that was requested.
        location: String,
    },

    /// The object already exists and the operation required it not to
    /// (conditional write / `write_if_absent`).
    #[error("object already exists: {location}")]
    AlreadyExists {
        /// Location that was requested.
        location: String,
    },

    /// The backend rejected the operation for authorization reasons.
    #[error("permission denied on {location}: {message}")]
    PermissionDenied {
        /// Location that was requested.
        location: String,
        /// Backend-reported detail.
        message: String,
        /// Underlying cause, if any.
        #[source]
        source: Option<BoxError>,
    },

    /// A transient backend failure (network error, throttling, 5xx).
    ///
    /// Bounded retries with jitter have already been applied inside the
    /// storage handle before this surfaces; `retryable` says whether the
    /// backend still considers a *fresh* attempt worthwhile.
    #[error("transient storage failure on {location} (retryable: {retryable}): {message}")]
    Transient {
        /// Location that was requested.
        location: String,
        /// Whether retrying the whole operation later may succeed.
        retryable: bool,
        /// Backend-reported detail.
        message: String,
        /// Underlying cause, if any.
        #[source]
        source: Option<BoxError>,
    },

    /// A non-transient backend failure that fits none of the semantic
    /// variants (unsupported operation, corrupted response, ...).
    #[error("storage backend error on {location}: {message}")]
    Backend {
        /// Location that was requested.
        location: String,
        /// Backend-reported detail.
        message: String,
        /// Underlying cause, if any.
        #[source]
        source: Option<BoxError>,
    },

    /// The storage profile (root URI or options map) is invalid.
    #[error("invalid storage configuration: {0}")]
    Config(String),

    /// A location passed to a storage handle does not resolve under the
    /// handle's root.
    #[error("location {location} is not under storage root {root}")]
    InvalidLocation {
        /// The offending location.
        location: String,
        /// The root URI of the storage handle.
        root: String,
    },

    /// The object at `location` is not a valid Iceberg `metadata.json`.
    #[error("invalid table metadata at {location}: {message}")]
    InvalidMetadata {
        /// Location of the offending file.
        location: String,
        /// What failed (parse error, non-UTF-8 content, ...).
        message: String,
        /// Underlying cause, if any.
        #[source]
        source: Option<BoxError>,
    },

    /// The metadata file parsed but declares a format version this build
    /// does not support.
    #[error("unsupported table metadata format-version {found} at {location} (supported: 1..=3)")]
    UnsupportedFormatVersion {
        /// Location of the offending file.
        location: String,
        /// The declared `format-version`.
        found: u8,
    },
}

impl StorageError {
    /// Whether retrying the whole operation later may reasonably succeed.
    #[must_use]
    pub fn is_retryable(&self) -> bool {
        matches!(
            self,
            Self::Transient {
                retryable: true,
                ..
            }
        )
    }
}

/// Maps an [`opendal::Error`] to a [`StorageError`] for the given location.
///
/// `ConditionNotMatch` is mapped to [`StorageError::AlreadyExists`]: the only
/// conditional operation this crate issues is `write_if_absent`
/// (`If-None-Match: *` on S3, `O_EXCL` on the local filesystem), so a failed
/// condition always means "the object was already there".
pub(crate) fn from_opendal(location: &str, err: opendal::Error) -> StorageError {
    let location = location.to_owned();
    match err.kind() {
        opendal::ErrorKind::NotFound => StorageError::NotFound { location },
        opendal::ErrorKind::AlreadyExists | opendal::ErrorKind::ConditionNotMatch => {
            StorageError::AlreadyExists { location }
        }
        opendal::ErrorKind::PermissionDenied => StorageError::PermissionDenied {
            location,
            message: err.to_string(),
            source: Some(Box::new(err)),
        },
        opendal::ErrorKind::RateLimited => StorageError::Transient {
            location,
            retryable: true,
            message: err.to_string(),
            source: Some(Box::new(err)),
        },
        // `Unexpected` covers I/O and 5xx-shaped failures. `is_temporary()`
        // distinguishes "the backend says try again" from "retries were
        // already exhausted / the failure is persistent".
        opendal::ErrorKind::Unexpected => {
            let retryable = err.is_temporary();
            StorageError::Transient {
                location,
                retryable,
                message: err.to_string(),
                source: Some(Box::new(err)),
            }
        }
        _ if err.is_temporary() => StorageError::Transient {
            location,
            retryable: true,
            message: err.to_string(),
            source: Some(Box::new(err)),
        },
        _ => StorageError::Backend {
            location,
            message: err.to_string(),
            source: Some(Box::new(err)),
        },
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn not_found_maps() {
        let err = from_opendal(
            "s3://b/x",
            opendal::Error::new(opendal::ErrorKind::NotFound, "gone"),
        );
        assert!(matches!(err, StorageError::NotFound { .. }));
        assert!(!err.is_retryable());
    }

    #[test]
    fn condition_not_match_means_already_exists() {
        let err = from_opendal(
            "s3://b/x",
            opendal::Error::new(opendal::ErrorKind::ConditionNotMatch, "412"),
        );
        assert!(matches!(err, StorageError::AlreadyExists { .. }));
    }

    #[test]
    fn temporary_errors_are_retryable() {
        let err = from_opendal(
            "s3://b/x",
            opendal::Error::new(opendal::ErrorKind::Unexpected, "503").set_temporary(),
        );
        assert!(err.is_retryable());
    }

    #[test]
    fn persistent_unexpected_is_transient_but_not_retryable() {
        let err = from_opendal(
            "s3://b/x",
            opendal::Error::new(opendal::ErrorKind::Unexpected, "boom"),
        );
        assert!(matches!(
            err,
            StorageError::Transient {
                retryable: false,
                ..
            }
        ));
    }
}
