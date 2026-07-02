//! Object-storage IO for Meridian.
//!
//! Object storage is the *customer's* infrastructure — Meridian is a client
//! of it, never a service dependency on it (Postgres stays the only required
//! runtime dependency). This crate is the one seam every pillar goes
//! through to touch it:
//!
//! - [`StorageProfile`] — a validated warehouse root URI (`s3://bucket/prefix`
//!   or a local `file://` path) plus a string options map, the shape a
//!   warehouse configuration row carries.
//! - [`Storage`] — the async, object-safe handle: `read`, `write`,
//!   `write_if_absent` (the conditional-write primitive that backs
//!   `metadata.json` immutability), `exists`, `delete`, `delete_prefix`,
//!   and streaming `list`. Transient failures are retried internally with
//!   bounded exponential backoff and jitter.
//! - [`StorageError`] — semantic errors (`NotFound`, `AlreadyExists`,
//!   `PermissionDenied`, `Transient { retryable }`) the commit path can
//!   branch on.
//! - Metadata-file helpers — [`new_metadata_location`],
//!   [`read_table_metadata`], [`write_table_metadata`] and their view
//!   counterparts [`new_view_metadata_location`], [`read_view_metadata`],
//!   [`write_view_metadata`] — fixing the Iceberg
//!   `metadata/NNNNN-<uuid>.metadata.json` convention and the
//!   never-overwrite rule.
//!
//! Backends: local filesystem and S3-compatible stores (AWS S3, `MinIO`, and
//! friends via endpoint override), both provided by [opendal] behind the
//! [`Storage`] trait. See `docs/adr/004-opendal-storage-io.md` for the
//! backend decision.

mod error;
mod metadata;
mod profile;
mod storage;

pub use error::{StorageError, StorageResult};
pub use metadata::{
    new_metadata_location, new_view_metadata_location, read_table_metadata, read_view_metadata,
    write_table_metadata, write_view_metadata,
};
pub use profile::{RetryConfig, S3Options, StorageProfile, StorageScheme};
pub use storage::{ObjectMeta, ObjectStream, Storage};
