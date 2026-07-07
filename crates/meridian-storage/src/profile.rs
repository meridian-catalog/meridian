//! Storage profiles: a parsed warehouse root plus connection options.
//!
//! A [`StorageProfile`] is the validated form of what a warehouse row will
//! store (root URI + options map). [`StorageProfile::connect`] turns it into
//! a live [`Storage`](crate::Storage) handle.

use std::collections::BTreeMap;
use std::path::PathBuf;
use std::sync::Arc;
use std::time::Duration;

use crate::error::{StorageError, StorageResult};
use crate::storage::{OpendalStorage, Storage};

/// The storage backend family a profile points at.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum StorageScheme {
    /// Local filesystem (`file://`, `fs://`, or a bare path).
    Fs,
    /// S3-compatible object storage (`s3://`, `s3a://`): AWS S3, `MinIO`, ...
    S3,
}

/// Bounded exponential-backoff retry policy applied to every operation of a
/// connected [`Storage`](crate::Storage) handle.
///
/// Retries apply only to failures the backend reports as temporary (network
/// errors, throttling, 5xx). Jitter is always enabled.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct RetryConfig {
    /// Maximum number of *retries* after the initial attempt.
    pub max_retries: usize,
    /// Delay before the first retry; doubles per attempt (plus jitter).
    pub min_delay: Duration,
    /// Upper bound on the delay between attempts.
    pub max_delay: Duration,
    /// Overall timeout for a single non-streaming object-store operation
    /// (stat, delete, and the small metadata GET/PUTs the commit path makes).
    /// Without it, a hung connection to the object store stalls the operation —
    /// and, on the commit path, the whole request — indefinitely, because
    /// opendal has no default request timeout. A timed-out attempt errors (and
    /// is retried by the retry layer that wraps this one).
    pub timeout: Duration,
    /// Per-chunk timeout for streaming reads/writes (large data transfers).
    /// Larger than `timeout` because a big object legitimately streams for a
    /// while; this bounds the gap *between* chunks, catching a stalled stream.
    pub io_timeout: Duration,
}

impl Default for RetryConfig {
    fn default() -> Self {
        Self {
            max_retries: 3,
            min_delay: Duration::from_millis(100),
            max_delay: Duration::from_secs(10),
            timeout: Duration::from_secs(30),
            io_timeout: Duration::from_secs(60),
        }
    }
}

/// Options for S3-compatible backends.
#[derive(Clone, Default, PartialEq, Eq)]
pub struct S3Options {
    /// Signing region. When unset and an explicit `endpoint` is configured,
    /// defaults to `us-east-1` (the `MinIO` convention); otherwise resolution
    /// is left to the standard AWS environment/config chain.
    pub region: Option<String>,
    /// Endpoint override for S3-compatible stores (e.g. `http://minio:9000`).
    pub endpoint: Option<String>,
    /// Use path-style addressing (`endpoint/bucket/key`). Defaults to `true`,
    /// which every S3-compatible store accepts; set `false` for
    /// virtual-hosted style (`bucket.endpoint/key`).
    pub path_style: bool,
    /// Send unsigned (anonymous) requests and skip the credential chain.
    pub anonymous: bool,
    /// Explicit access key id. When unset, the standard AWS
    /// environment/config chain is used.
    pub access_key_id: Option<String>,
    /// Explicit secret access key.
    pub secret_access_key: Option<String>,
    /// Explicit session token, for temporary credentials.
    pub session_token: Option<String>,
}

/// Manual `Debug` so credentials never reach logs: secret material is
/// rendered as presence only.
impl std::fmt::Debug for S3Options {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("S3Options")
            .field("region", &self.region)
            .field("endpoint", &self.endpoint)
            .field("path_style", &self.path_style)
            .field("anonymous", &self.anonymous)
            .field("access_key_id", &self.access_key_id.as_ref().map(|_| "..."))
            .field(
                "secret_access_key",
                &self.secret_access_key.as_ref().map(|_| "..."),
            )
            .field("session_token", &self.session_token.as_ref().map(|_| "..."))
            .finish()
    }
}

/// Where a profile's root lives. Private: consumers go through
/// [`StorageProfile`] accessors.
#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) enum ProfileRoot {
    /// Absolute local directory.
    Fs {
        /// Absolute root directory.
        root: PathBuf,
    },
    /// S3 bucket plus optional key prefix (no leading/trailing slash).
    S3 {
        /// Bucket name.
        bucket: String,
        /// Key prefix under the bucket; empty for the bucket root.
        prefix: String,
        /// Connection options.
        options: S3Options,
    },
}

/// A validated warehouse storage root plus connection options.
///
/// Parsed from a root URI and a string options map (the shape a warehouse
/// configuration row carries) by [`StorageProfile::parse`].
///
/// # Supported root URIs
///
/// - `s3://bucket/prefix` (alias `s3a://`) — S3-compatible object storage.
/// - `file:///abs/path`, `fs://path` — local filesystem. Anything after the
///   scheme is treated as a plain path: relative paths are resolved against
///   the current working directory at parse time.
/// - A bare path (`/abs/path`, `./rel`, `rel/dir`) — local filesystem.
///
/// # Supported option keys
///
/// | Key | Applies to | Meaning |
/// |---|---|---|
/// | `region` | s3 | Signing region |
/// | `endpoint` | s3 | Endpoint override (`MinIO`, R2, ...) |
/// | `path-style` | s3 | `true` (default) / `false` for virtual-hosted |
/// | `anonymous` | s3 | Unsigned requests, skip credential chain |
/// | `access-key-id` | s3 | Explicit credentials (else env/config chain) |
/// | `secret-access-key` | s3 | Explicit credentials |
/// | `session-token` | s3 | Temporary-credential session token |
/// | `retry.max-retries` | all | Max retries after the initial attempt (default 3) |
/// | `retry.min-delay-ms` | all | First backoff delay (default 100) |
/// | `retry.max-delay-ms` | all | Backoff ceiling (default 10000) |
/// | `retry.timeout-ms` | all | Per-op timeout for non-streaming calls (default 30000) |
/// | `retry.io-timeout-ms` | all | Per-chunk timeout for streaming IO (default 60000) |
///
/// Unknown keys are rejected — a typo in a durability-critical option must
/// fail loudly, not be silently ignored. The exception is the catalog-layer
/// keys (`vending`, `vending.*`, `endpoint.external`): one warehouse options
/// map carries both storage-connection and catalog concerns, so those keys
/// are accepted here and ignored — they never affect how the *server* talks
/// to storage. They are parsed and validated by `meridian-vending` and the
/// server's warehouse API.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct StorageProfile {
    pub(crate) root: ProfileRoot,
    pub(crate) retry: RetryConfig,
}

impl StorageProfile {
    /// Parses a profile from a warehouse root URI and an options map.
    ///
    /// # Errors
    ///
    /// Returns [`StorageError::Config`] for unsupported schemes, malformed
    /// roots, unknown option keys, or unparseable option values.
    pub fn parse(root_uri: &str, options: &BTreeMap<String, String>) -> StorageResult<Self> {
        let root_uri = root_uri.trim();
        if root_uri.is_empty() {
            return Err(StorageError::Config(
                "storage root URI must not be empty".to_owned(),
            ));
        }

        let retry = parse_retry_options(options)?;

        if let Some(rest) = strip_scheme(root_uri, &["s3", "s3a"]) {
            let (bucket, prefix) = split_bucket_prefix(rest)?;
            let s3_options = parse_s3_options(options)?;
            return Ok(Self {
                root: ProfileRoot::S3 {
                    bucket,
                    prefix,
                    options: s3_options,
                },
                retry,
            });
        }

        let fs_path = if let Some(rest) = strip_scheme(root_uri, &["file", "fs"]) {
            if rest.is_empty() {
                return Err(StorageError::Config(format!(
                    "filesystem root URI has an empty path: {root_uri}"
                )));
            }
            rest.to_owned()
        } else if root_uri.contains("://") {
            return Err(StorageError::Config(format!(
                "unsupported storage scheme in root URI: {root_uri}"
            )));
        } else {
            root_uri.to_owned()
        };

        reject_s3_only_options(options)?;

        let root = std::path::absolute(PathBuf::from(&fs_path)).map_err(|err| {
            StorageError::Config(format!(
                "cannot resolve filesystem root {fs_path:?} to an absolute path: {err}"
            ))
        })?;

        Ok(Self {
            root: ProfileRoot::Fs { root },
            retry,
        })
    }

    /// The backend family this profile points at.
    #[must_use]
    pub fn scheme(&self) -> StorageScheme {
        match self.root {
            ProfileRoot::Fs { .. } => StorageScheme::Fs,
            ProfileRoot::S3 { .. } => StorageScheme::S3,
        }
    }

    /// The canonical root URI (`s3://bucket/prefix` or `file:///abs/path`,
    /// never with a trailing slash).
    #[must_use]
    pub fn root_uri(&self) -> String {
        match &self.root {
            ProfileRoot::Fs { root } => format!("file://{}", root.to_string_lossy()),
            ProfileRoot::S3 { bucket, prefix, .. } => {
                if prefix.is_empty() {
                    format!("s3://{bucket}")
                } else {
                    format!("s3://{bucket}/{prefix}")
                }
            }
        }
    }

    /// The retry policy the connected handle will apply.
    #[must_use]
    pub fn retry(&self) -> &RetryConfig {
        &self.retry
    }

    /// Connects the profile, returning a live storage handle.
    ///
    /// For S3 roots this validates the configuration without network I/O
    /// (reachability problems surface on the first operation). For
    /// filesystem roots the root directory is created if missing.
    ///
    /// # Errors
    ///
    /// Returns [`StorageError::Config`] when the backend rejects the
    /// configuration — e.g. an S3 root with no region configured and none
    /// resolvable from the environment, or an fs root that cannot be
    /// created.
    pub fn connect(&self) -> StorageResult<Arc<dyn Storage>> {
        Ok(Arc::new(OpendalStorage::connect(self)?))
    }
}

/// Strips `scheme://` for any of the given schemes (case-insensitive),
/// returning the remainder.
fn strip_scheme<'a>(uri: &'a str, schemes: &[&str]) -> Option<&'a str> {
    let (scheme, rest) = uri.split_once("://")?;
    schemes
        .iter()
        .any(|s| scheme.eq_ignore_ascii_case(s))
        .then_some(rest)
}

/// Splits `bucket/some/prefix` into a bucket and a normalized prefix.
fn split_bucket_prefix(rest: &str) -> StorageResult<(String, String)> {
    let (bucket, prefix) = match rest.split_once('/') {
        Some((bucket, prefix)) => (bucket, prefix.trim_matches('/')),
        None => (rest, ""),
    };
    if bucket.is_empty() {
        return Err(StorageError::Config(
            "S3 root URI is missing a bucket name".to_owned(),
        ));
    }
    Ok((bucket.to_owned(), prefix.to_owned()))
}

/// Option keys that only make sense for S3 backends.
const S3_OPTION_KEYS: &[&str] = &[
    "region",
    "endpoint",
    "path-style",
    "anonymous",
    "access-key-id",
    "secret-access-key",
    "session-token",
];

/// Option keys shared by every backend.
const COMMON_OPTION_KEYS: &[&str] = &[
    "retry.max-retries",
    "retry.min-delay-ms",
    "retry.max-delay-ms",
    "retry.timeout-ms",
    "retry.io-timeout-ms",
];

/// Keys owned by the catalog layer (credential vending, external endpoint
/// advertisement): accepted in the shared options map, ignored by storage
/// connection logic (see the [`StorageProfile`] docs).
fn is_catalog_option(key: &str) -> bool {
    key == "vending" || key.starts_with("vending.") || key == "endpoint.external"
}

fn parse_s3_options(options: &BTreeMap<String, String>) -> StorageResult<S3Options> {
    for key in options.keys() {
        if !S3_OPTION_KEYS.contains(&key.as_str())
            && !COMMON_OPTION_KEYS.contains(&key.as_str())
            && !is_catalog_option(key)
        {
            return Err(StorageError::Config(format!(
                "unknown storage option {key:?} (supported: {S3_OPTION_KEYS:?} and {COMMON_OPTION_KEYS:?})"
            )));
        }
    }
    Ok(S3Options {
        region: options.get("region").cloned(),
        endpoint: options.get("endpoint").cloned(),
        path_style: parse_bool(options, "path-style")?.unwrap_or(true),
        anonymous: parse_bool(options, "anonymous")?.unwrap_or(false),
        access_key_id: options.get("access-key-id").cloned(),
        secret_access_key: options.get("secret-access-key").cloned(),
        session_token: options.get("session-token").cloned(),
    })
}

fn reject_s3_only_options(options: &BTreeMap<String, String>) -> StorageResult<()> {
    for key in options.keys() {
        if is_catalog_option(key) {
            continue;
        }
        if S3_OPTION_KEYS.contains(&key.as_str()) {
            return Err(StorageError::Config(format!(
                "option {key:?} does not apply to filesystem storage"
            )));
        }
        if !COMMON_OPTION_KEYS.contains(&key.as_str()) {
            return Err(StorageError::Config(format!(
                "unknown storage option {key:?} (supported for filesystem roots: {COMMON_OPTION_KEYS:?})"
            )));
        }
    }
    Ok(())
}

fn parse_retry_options(options: &BTreeMap<String, String>) -> StorageResult<RetryConfig> {
    let mut retry = RetryConfig::default();
    if let Some(v) = options.get("retry.max-retries") {
        retry.max_retries = parse_number(v, "retry.max-retries")?;
    }
    if let Some(v) = options.get("retry.min-delay-ms") {
        retry.min_delay = Duration::from_millis(parse_number(v, "retry.min-delay-ms")?);
    }
    if let Some(v) = options.get("retry.max-delay-ms") {
        retry.max_delay = Duration::from_millis(parse_number(v, "retry.max-delay-ms")?);
    }
    if let Some(v) = options.get("retry.timeout-ms") {
        retry.timeout = Duration::from_millis(parse_number(v, "retry.timeout-ms")?);
    }
    if let Some(v) = options.get("retry.io-timeout-ms") {
        retry.io_timeout = Duration::from_millis(parse_number(v, "retry.io-timeout-ms")?);
    }
    if retry.min_delay > retry.max_delay {
        return Err(StorageError::Config(format!(
            "retry.min-delay-ms ({:?}) exceeds retry.max-delay-ms ({:?})",
            retry.min_delay, retry.max_delay
        )));
    }
    Ok(retry)
}

fn parse_number<T: std::str::FromStr>(value: &str, key: &str) -> StorageResult<T> {
    value.parse().map_err(|_| {
        StorageError::Config(format!(
            "option {key:?} must be a non-negative integer, got {value:?}"
        ))
    })
}

fn parse_bool(options: &BTreeMap<String, String>, key: &str) -> StorageResult<Option<bool>> {
    match options.get(key) {
        None => Ok(None),
        Some(v) if v.eq_ignore_ascii_case("true") => Ok(Some(true)),
        Some(v) if v.eq_ignore_ascii_case("false") => Ok(Some(false)),
        Some(v) => Err(StorageError::Config(format!(
            "option {key:?} must be \"true\" or \"false\", got {v:?}"
        ))),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn opts(pairs: &[(&str, &str)]) -> BTreeMap<String, String> {
        pairs
            .iter()
            .map(|(k, v)| ((*k).to_owned(), (*v).to_owned()))
            .collect()
    }

    #[test]
    fn parses_s3_root_with_prefix() {
        let p = StorageProfile::parse("s3://bucket/ware/house/", &BTreeMap::new()).expect("parse");
        assert_eq!(p.scheme(), StorageScheme::S3);
        assert_eq!(p.root_uri(), "s3://bucket/ware/house");
    }

    #[test]
    fn parses_s3_root_without_prefix() {
        let p = StorageProfile::parse("s3a://bucket", &BTreeMap::new()).expect("parse");
        assert_eq!(p.root_uri(), "s3://bucket");
    }

    #[test]
    fn rejects_missing_bucket() {
        assert!(matches!(
            StorageProfile::parse("s3:///prefix", &BTreeMap::new()),
            Err(StorageError::Config(_))
        ));
    }

    #[test]
    fn parses_file_uri_and_bare_paths() {
        for uri in ["file:///tmp/wh", "/tmp/wh"] {
            let p = StorageProfile::parse(uri, &BTreeMap::new()).expect("parse");
            assert_eq!(p.scheme(), StorageScheme::Fs);
            assert_eq!(p.root_uri(), "file:///tmp/wh");
        }
    }

    #[test]
    fn resolves_relative_fs_paths() {
        let p = StorageProfile::parse("./relative/wh", &BTreeMap::new()).expect("parse");
        let uri = p.root_uri();
        assert!(uri.starts_with("file:///"), "not absolute: {uri}");
        assert!(uri.ends_with("/relative/wh"), "unexpected: {uri}");
    }

    #[test]
    fn rejects_unknown_scheme() {
        assert!(matches!(
            StorageProfile::parse("gcs://bucket/x", &BTreeMap::new()),
            Err(StorageError::Config(_))
        ));
    }

    #[test]
    fn rejects_unknown_option_key() {
        let err = StorageProfile::parse("s3://b/p", &opts(&[("regoin", "us-east-1")]));
        assert!(matches!(err, Err(StorageError::Config(_))));
    }

    #[test]
    fn accepts_and_ignores_catalog_layer_options() {
        // Same parse result with or without the catalog keys, on both roots.
        let catalog = opts(&[
            ("vending", "sts"),
            ("vending.role-arn", "arn:minio:iam:::role/x"),
            ("endpoint.external", "http://host.docker.internal:9000"),
        ]);
        assert_eq!(
            StorageProfile::parse("s3://b/p", &catalog).expect("s3 parse"),
            StorageProfile::parse("s3://b/p", &BTreeMap::new()).expect("s3 parse"),
        );
        assert_eq!(
            StorageProfile::parse("/tmp/wh", &catalog).expect("fs parse"),
            StorageProfile::parse("/tmp/wh", &BTreeMap::new()).expect("fs parse"),
        );
        // A typo'd vending key is still caught by the strict check.
        assert!(matches!(
            StorageProfile::parse("s3://b/p", &opts(&[("vendign", "sts")])),
            Err(StorageError::Config(_))
        ));
    }

    #[test]
    fn rejects_s3_options_on_fs_roots() {
        let err = StorageProfile::parse("/tmp/wh", &opts(&[("endpoint", "http://x")]));
        assert!(matches!(err, Err(StorageError::Config(_))));
    }

    #[test]
    fn parses_s3_options_and_retry() {
        let p = StorageProfile::parse(
            "s3://b/p",
            &opts(&[
                ("region", "eu-west-1"),
                ("endpoint", "http://localhost:9000"),
                ("path-style", "TRUE"),
                ("anonymous", "false"),
                ("retry.max-retries", "5"),
                ("retry.min-delay-ms", "10"),
                ("retry.max-delay-ms", "500"),
                ("retry.timeout-ms", "5000"),
                ("retry.io-timeout-ms", "20000"),
            ]),
        )
        .expect("parse");
        let ProfileRoot::S3 { options, .. } = &p.root else {
            panic!("expected s3 root");
        };
        assert_eq!(options.region.as_deref(), Some("eu-west-1"));
        assert!(options.path_style);
        assert!(!options.anonymous);
        assert_eq!(p.retry().max_retries, 5);
        assert_eq!(p.retry().min_delay, Duration::from_millis(10));
        assert_eq!(p.retry().max_delay, Duration::from_millis(500));
        assert_eq!(p.retry().timeout, Duration::from_secs(5));
        assert_eq!(p.retry().io_timeout, Duration::from_secs(20));
    }

    #[test]
    fn retry_timeouts_default_when_unset() {
        let p = StorageProfile::parse("s3://b/p", &opts(&[])).expect("parse");
        // A production default must bound every op even with no explicit config.
        assert_eq!(p.retry().timeout, Duration::from_secs(30));
        assert_eq!(p.retry().io_timeout, Duration::from_secs(60));
    }

    #[test]
    fn rejects_bad_bool_and_inverted_delays() {
        assert!(matches!(
            StorageProfile::parse("s3://b", &opts(&[("path-style", "yes")])),
            Err(StorageError::Config(_))
        ));
        assert!(matches!(
            StorageProfile::parse(
                "s3://b",
                &opts(&[("retry.min-delay-ms", "1000"), ("retry.max-delay-ms", "10")]),
            ),
            Err(StorageError::Config(_))
        ));
    }
}
