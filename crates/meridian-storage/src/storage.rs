//! The [`Storage`] trait and its opendal-backed implementation.

use std::fmt;
use std::path::Path;
use std::pin::Pin;

use async_trait::async_trait;
use bytes::Bytes;
use chrono::{DateTime, Utc};
use futures::stream::{Stream, StreamExt};
use opendal::layers::RetryLayer;
use opendal::{Operator, services};

use crate::error::{StorageError, StorageResult, from_opendal};
use crate::profile::{ProfileRoot, S3Options, StorageProfile};

/// One object returned by [`Storage::list`].
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ObjectMeta {
    /// Absolute location of the object (same URI form the handle accepts).
    pub location: String,
    /// Object size in bytes.
    pub size: u64,
    /// Last-modified time, when the backend reports one.
    pub last_modified: Option<DateTime<Utc>>,
}

/// Stream of listed objects, yielded in backend order.
pub type ObjectStream = Pin<Box<dyn Stream<Item = StorageResult<ObjectMeta>> + Send + 'static>>;

/// An async, object-safe handle to a warehouse's object storage.
///
/// # Locations
///
/// Every method accepts either an **absolute location URI** under the
/// handle's root (`s3://bucket/prefix/...`, `file:///abs/root/...` — the form
/// Iceberg metadata records) or a **root-relative path**
/// (`metadata/00001-x.metadata.json`). Locations outside the root are
/// rejected with [`StorageError::InvalidLocation`]; the handle never reads or
/// writes outside the warehouse it was opened for.
///
/// # Retries
///
/// Transient backend failures are retried internally with bounded
/// exponential backoff plus jitter (see
/// [`RetryConfig`](crate::RetryConfig)); errors surfacing from these methods
/// have already exhausted that budget.
#[async_trait]
pub trait Storage: fmt::Debug + Send + Sync {
    /// The canonical root URI of this handle (no trailing slash).
    fn root_uri(&self) -> &str;

    /// Reads the entire object at `location`.
    async fn read(&self, location: &str) -> StorageResult<Bytes>;

    /// Writes `bytes` to `location` unconditionally, replacing any existing
    /// object.
    async fn write(&self, location: &str, bytes: Bytes) -> StorageResult<()>;

    /// Writes `bytes` to `location` only if no object exists there.
    ///
    /// This is the primitive that backs `metadata.json` immutability: S3
    /// backends send `If-None-Match: *`, the filesystem backend creates the
    /// file with `O_EXCL`. Fails with [`StorageError::AlreadyExists`] if the
    /// object is already present (including when a concurrent writer wins
    /// the race).
    async fn write_if_absent(&self, location: &str, bytes: Bytes) -> StorageResult<()>;

    /// Whether an object exists at `location`.
    async fn exists(&self, location: &str) -> StorageResult<bool>;

    /// Deletes the object at `location`. Deleting a missing object is not an
    /// error (idempotent).
    async fn delete(&self, location: &str) -> StorageResult<()>;

    /// Deletes every object under `prefix` (batched where the backend
    /// supports it). A missing or empty prefix deletes nothing. The prefix
    /// must not be empty — deleting the entire warehouse root must be
    /// spelled out by the caller, not implied.
    async fn delete_prefix(&self, prefix: &str) -> StorageResult<()>;

    /// Lists objects under `prefix` recursively as a stream of
    /// [`ObjectMeta`]. Directories are not yielded; a missing prefix yields
    /// an empty stream. An empty `prefix` lists the whole root.
    async fn list(&self, prefix: &str) -> StorageResult<ObjectStream>;
}

/// [`Storage`] implementation backed by [opendal].
pub(crate) struct OpendalStorage {
    op: Operator,
    root: ProfileRoot,
    root_uri: String,
}

impl fmt::Debug for OpendalStorage {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("OpendalStorage")
            .field("root_uri", &self.root_uri)
            .finish_non_exhaustive()
    }
}

impl OpendalStorage {
    /// Builds the opendal operator for `profile` and wraps it.
    pub(crate) fn connect(profile: &StorageProfile) -> StorageResult<Self> {
        let retry_layer = RetryLayer::new()
            .with_max_times(profile.retry.max_retries)
            .with_min_delay(profile.retry.min_delay)
            .with_max_delay(profile.retry.max_delay)
            .with_jitter();

        let op = match &profile.root {
            ProfileRoot::Fs { root } => {
                let root_str = root.to_str().ok_or_else(|| {
                    StorageError::Config(format!(
                        "filesystem root {} is not valid UTF-8; opendal requires UTF-8 paths",
                        root.display()
                    ))
                })?;
                let builder = services::Fs::default().root(root_str);
                Operator::new(builder)
                    .map_err(|err| config_error(&err))?
                    .layer(retry_layer)
                    .finish()
            }
            ProfileRoot::S3 {
                bucket,
                prefix,
                options,
            } => {
                let builder = s3_builder(bucket, prefix, options);
                Operator::new(builder)
                    .map_err(|err| config_error(&err))?
                    .layer(retry_layer)
                    .finish()
            }
        };

        Ok(Self {
            op,
            root: profile.root.clone(),
            root_uri: profile.root_uri(),
        })
    }

    /// Resolves a caller-supplied location (absolute URI or root-relative
    /// path) to a path relative to the operator root.
    ///
    /// Returns the relative path without a leading slash; the root itself
    /// resolves to an empty string.
    fn resolve(&self, location: &str) -> StorageResult<String> {
        let rel: &str = match &self.root {
            ProfileRoot::Fs { root } => {
                let path_part = if let Some(rest) = strip_scheme(location, &["file", "fs"]) {
                    rest
                } else if location.contains("://") {
                    return Err(self.invalid_location(location));
                } else {
                    location
                };
                if path_part.starts_with('/') {
                    match Path::new(path_part).strip_prefix(root) {
                        Ok(rel) => {
                            return self.normalize(location, &rel.to_string_lossy());
                        }
                        Err(_) => return Err(self.invalid_location(location)),
                    }
                }
                path_part
            }
            ProfileRoot::S3 { bucket, prefix, .. } => {
                if let Some(rest) = strip_scheme(location, &["s3", "s3a"]) {
                    let (loc_bucket, key) = match rest.split_once('/') {
                        Some((b, k)) => (b, k.trim_start_matches('/')),
                        None => (rest, ""),
                    };
                    if loc_bucket != bucket {
                        return Err(self.invalid_location(location));
                    }
                    if prefix.is_empty() {
                        key
                    } else if key == prefix {
                        ""
                    } else if let Some(rel) = key
                        .strip_prefix(prefix.as_str())
                        .and_then(|r| r.strip_prefix('/'))
                    {
                        rel
                    } else {
                        return Err(self.invalid_location(location));
                    }
                } else if location.contains("://") || location.starts_with('/') {
                    return Err(self.invalid_location(location));
                } else {
                    location
                }
            }
        };
        self.normalize(location, rel)
    }

    /// Normalizes a root-relative path: strips a leading `./` and rejects
    /// `.`/`..` segments (path traversal must never resolve).
    fn normalize(&self, original: &str, rel: &str) -> StorageResult<String> {
        let rel = rel.strip_prefix("./").unwrap_or(rel);
        let rel = rel.trim_start_matches('/');
        if rel
            .split('/')
            .any(|segment| segment == ".." || segment == ".")
        {
            return Err(self.invalid_location(original));
        }
        Ok(rel.to_owned())
    }

    /// Resolves a location that must name an object (not the root).
    fn resolve_object(&self, location: &str) -> StorageResult<String> {
        let rel = self.resolve(location)?;
        if rel.is_empty() || rel.ends_with('/') {
            return Err(self.invalid_location(location));
        }
        Ok(rel)
    }

    /// Resolves a prefix for list/delete-prefix, with a trailing slash.
    /// Empty means "the whole root" (spelled `/` for opendal).
    fn resolve_prefix(&self, prefix: &str) -> StorageResult<String> {
        let rel = self.resolve(prefix)?;
        if rel.is_empty() {
            return Ok("/".to_owned());
        }
        if rel.ends_with('/') {
            Ok(rel)
        } else {
            Ok(format!("{rel}/"))
        }
    }

    fn invalid_location(&self, location: &str) -> StorageError {
        StorageError::InvalidLocation {
            location: location.to_owned(),
            root: self.root_uri.clone(),
        }
    }
}

#[async_trait]
impl Storage for OpendalStorage {
    fn root_uri(&self) -> &str {
        &self.root_uri
    }

    async fn read(&self, location: &str) -> StorageResult<Bytes> {
        let path = self.resolve_object(location)?;
        let buffer = self
            .op
            .read(&path)
            .await
            .map_err(|err| from_opendal(location, err))?;
        Ok(buffer.to_bytes())
    }

    async fn write(&self, location: &str, bytes: Bytes) -> StorageResult<()> {
        let path = self.resolve_object(location)?;
        self.op
            .write(&path, bytes)
            .await
            .map(|_| ())
            .map_err(|err| from_opendal(location, err))
    }

    async fn write_if_absent(&self, location: &str, bytes: Bytes) -> StorageResult<()> {
        let path = self.resolve_object(location)?;
        self.op
            .write_with(&path, bytes)
            .if_not_exists(true)
            .await
            .map(|_| ())
            .map_err(|err| from_opendal(location, err))
    }

    async fn exists(&self, location: &str) -> StorageResult<bool> {
        let path = self.resolve_object(location)?;
        self.op
            .exists(&path)
            .await
            .map_err(|err| from_opendal(location, err))
    }

    async fn delete(&self, location: &str) -> StorageResult<()> {
        let path = self.resolve_object(location)?;
        match self.op.delete(&path).await {
            Ok(()) => Ok(()),
            Err(err) if err.kind() == opendal::ErrorKind::NotFound => Ok(()),
            Err(err) => Err(from_opendal(location, err)),
        }
    }

    async fn delete_prefix(&self, prefix: &str) -> StorageResult<()> {
        let path = self.resolve_prefix(prefix)?;
        if path == "/" {
            return Err(self.invalid_location(prefix));
        }
        match self.op.delete_with(&path).recursive(true).await {
            Ok(()) => Ok(()),
            Err(err) if err.kind() == opendal::ErrorKind::NotFound => Ok(()),
            Err(err) => Err(from_opendal(prefix, err)),
        }
    }

    async fn list(&self, prefix: &str) -> StorageResult<ObjectStream> {
        let path = self.resolve_prefix(prefix)?;
        let lister = match self.op.lister_with(&path).recursive(true).await {
            Ok(lister) => lister,
            // A prefix nobody has written to is an empty listing, not an
            // error (object-store semantics; the fs backend reports a
            // missing directory as NotFound).
            Err(err) if err.kind() == opendal::ErrorKind::NotFound => {
                return Ok(Box::pin(futures::stream::empty()));
            }
            Err(err) => return Err(from_opendal(prefix, err)),
        };

        let root_uri = self.root_uri.clone();
        let prefix_owned = prefix.to_owned();
        let stream = lister.filter_map(move |entry| {
            let root_uri = root_uri.clone();
            let prefix_owned = prefix_owned.clone();
            async move {
                match entry {
                    Ok(entry) => {
                        let (path, metadata) = entry.into_parts();
                        if metadata.is_dir() {
                            return None;
                        }
                        let last_modified = metadata
                            .last_modified()
                            .map(|ts| DateTime::<Utc>::from(std::time::SystemTime::from(ts)));
                        Some(Ok(ObjectMeta {
                            location: format!("{root_uri}/{path}"),
                            size: metadata.content_length(),
                            last_modified,
                        }))
                    }
                    Err(err) => Some(Err(from_opendal(&prefix_owned, err))),
                }
            }
        });
        Ok(Box::pin(stream))
    }
}

/// Builds the opendal S3 backend builder from parsed options.
fn s3_builder(bucket: &str, prefix: &str, options: &S3Options) -> services::S3 {
    let mut builder = services::S3::default()
        .bucket(bucket)
        .root(&format!("/{prefix}"));

    if let Some(region) = &options.region {
        builder = builder.region(region);
    } else if options.endpoint.is_some() {
        // Custom endpoints (MinIO, R2, ...) rarely care about the signing
        // region but SigV4 needs one; `us-east-1` is the S3-compatible
        // convention.
        builder = builder.region("us-east-1");
    }
    if let Some(endpoint) = &options.endpoint {
        builder = builder.endpoint(endpoint);
    }
    if !options.path_style {
        builder = builder.enable_virtual_host_style();
    }
    if options.anonymous {
        builder = builder
            .skip_signature()
            .disable_config_load()
            .disable_ec2_metadata();
    }
    if let Some(key) = &options.access_key_id {
        builder = builder.access_key_id(key);
    }
    if let Some(secret) = &options.secret_access_key {
        builder = builder.secret_access_key(secret);
    }
    if let Some(token) = &options.session_token {
        builder = builder.session_token(token);
    }
    builder
}

fn config_error(err: &opendal::Error) -> StorageError {
    StorageError::Config(format!("storage backend rejected configuration: {err}"))
}

/// Strips `scheme://` for any of the given schemes (case-insensitive).
fn strip_scheme<'a>(uri: &'a str, schemes: &[&str]) -> Option<&'a str> {
    let (scheme, rest) = uri.split_once("://")?;
    schemes
        .iter()
        .any(|s| scheme.eq_ignore_ascii_case(s))
        .then_some(rest)
}

#[cfg(test)]
mod tests {
    use std::collections::BTreeMap;

    use super::*;

    fn s3_storage() -> OpendalStorage {
        let profile = StorageProfile::parse(
            "s3://bucket/ware/house",
            &[("region".to_owned(), "us-east-1".to_owned())]
                .into_iter()
                .collect(),
        )
        .expect("profile");
        OpendalStorage::connect(&profile).expect("connect")
    }

    /// Connecting an fs profile creates the root directory, so fs unit
    /// tests anchor at a tempdir and keep it alive alongside the handle.
    fn fs_storage() -> (tempfile::TempDir, OpendalStorage, String) {
        let dir = tempfile::tempdir().expect("tempdir");
        let root = dir.path().join("warehouse/root");
        let uri = format!("file://{}", root.display());
        let profile = StorageProfile::parse(&uri, &BTreeMap::new()).expect("profile");
        let storage = OpendalStorage::connect(&profile).expect("connect");
        let root_str = root.to_string_lossy().into_owned();
        (dir, storage, root_str)
    }

    #[test]
    fn s3_resolves_absolute_and_relative_locations() {
        let storage = s3_storage();
        assert_eq!(
            storage
                .resolve("s3://bucket/ware/house/metadata/1.json")
                .expect("resolve"),
            "metadata/1.json"
        );
        assert_eq!(
            storage
                .resolve("s3a://bucket/ware/house/metadata/1.json")
                .expect("resolve"),
            "metadata/1.json"
        );
        assert_eq!(
            storage.resolve("metadata/1.json").expect("resolve"),
            "metadata/1.json"
        );
        assert_eq!(storage.resolve("s3://bucket/ware/house").expect("root"), "");
    }

    #[test]
    fn s3_rejects_locations_outside_root() {
        let storage = s3_storage();
        for loc in [
            "s3://other-bucket/ware/house/x",
            "s3://bucket/elsewhere/x",
            "s3://bucket/ware/housemate/x",
            "file:///ware/house/x",
            "/absolute/key",
            "metadata/../../../etc/passwd",
        ] {
            assert!(
                matches!(
                    storage.resolve(loc),
                    Err(StorageError::InvalidLocation { .. })
                ),
                "expected InvalidLocation for {loc}"
            );
        }
    }

    #[test]
    fn fs_resolves_absolute_uri_bare_path_and_relative() {
        let (_dir, storage, root) = fs_storage();
        for loc in [
            format!("file://{root}/metadata/1.json"),
            format!("{root}/metadata/1.json"),
            "metadata/1.json".to_owned(),
            "./metadata/1.json".to_owned(),
        ] {
            assert_eq!(
                storage.resolve(&loc).expect("resolve"),
                "metadata/1.json",
                "location: {loc}"
            );
        }
    }

    #[test]
    fn fs_rejects_locations_outside_root() {
        let (_dir, storage, root) = fs_storage();
        for loc in [
            "file:///elsewhere/metadata/1.json".to_owned(),
            format!("{root}less/x"),
            "s3://bucket/x".to_owned(),
            "../escape".to_owned(),
        ] {
            assert!(
                matches!(
                    storage.resolve(&loc),
                    Err(StorageError::InvalidLocation { .. })
                ),
                "expected InvalidLocation for {loc}"
            );
        }
    }

    #[test]
    fn object_resolution_rejects_root_and_dir_paths() {
        let storage = s3_storage();
        assert!(storage.resolve_object("s3://bucket/ware/house").is_err());
        assert!(storage.resolve_object("metadata/").is_err());
        assert_eq!(
            storage.resolve_prefix("metadata").expect("prefix"),
            "metadata/"
        );
        assert_eq!(storage.resolve_prefix("").expect("prefix"), "/");
    }

    #[test]
    fn debug_output_omits_credentials() {
        let profile = StorageProfile::parse(
            "s3://bucket/p",
            &[
                ("region".to_owned(), "us-east-1".to_owned()),
                ("access-key-id".to_owned(), "AKIA123".to_owned()),
                ("secret-access-key".to_owned(), "supersecret".to_owned()),
            ]
            .into_iter()
            .collect(),
        )
        .expect("profile");
        let storage = OpendalStorage::connect(&profile).expect("connect");
        let debug = format!("{storage:?} {profile:?}");
        assert!(!debug.contains("supersecret"), "leaked secret: {debug}");
    }
}
