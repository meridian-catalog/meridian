//! Credential vending: per-table scoped, short-lived storage credentials.
//!
//! A [`CredentialVendor`] turns a *table scope* (the object-storage prefix a
//! table owns) plus an *access mode* (read / read-write) into credentials an
//! engine can use directly against object storage — without the engine ever
//! holding warehouse-wide keys. The server exposes vended credentials
//! through `LoadTableResult` (behind the `X-Iceberg-Access-Delegation`
//! header) and the `GET .../tables/{table}/credentials` endpoint.
//!
//! Implementations:
//!
//! - [`StsVendor`] — AWS STS `AssumeRole` with an inline session policy
//!   scoped to the table prefix. Works against AWS and against `MinIO`'s STS
//!   endpoint (same port as S3; verified against a real local `MinIO`).
//! - [`StaticVendor`] — passes the warehouse's configured static keys
//!   through as-is. This is deliberate opt-in (`vending = "static"` in the
//!   warehouse storage options): many self-hosted `MinIO` deployments have no
//!   STS story and simply want the catalog to hand engines the keys it
//!   already holds. Without the opt-in, credential material never leaves
//!   the server (the passthrough denylist stays intact).
//! - [`GcsVendor`] / [`AzureVendor`] — honest stubs. GCS downscoped tokens
//!   and Azure user-delegation SAS are not implemented yet; they return
//!   [`VendingError::UnsupportedCloud`] with a clear message and exist so
//!   the dispatch surface (and its error text) is settled.
//!
//! The vend itself is pure credential mechanics; audit logging is the
//! caller's job (the server writes the audit row and outbox event in one
//! transaction — the audit row is the product).

use std::collections::BTreeMap;
use std::time::Duration;

use chrono::{DateTime, Utc};

mod config;
mod policy;
mod static_vendor;
mod sts;
mod unsupported;

pub use config::{DEFAULT_TTL_SECS, MAX_TTL_SECS, MIN_TTL_SECS, VendingConfig};
pub use policy::s3_session_policy;
pub use static_vendor::StaticVendor;
pub use sts::StsVendor;
pub use unsupported::{AzureVendor, GcsVendor};

/// What the vended credentials may do within the table scope.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum AccessMode {
    /// Read data and metadata under the table prefix.
    Read,
    /// Read plus write/delete under the table prefix.
    ReadWrite,
}

impl AccessMode {
    /// Stable string form, used in audit rows and events.
    #[must_use]
    pub fn as_str(self) -> &'static str {
        match self {
            Self::Read => "read",
            Self::ReadWrite => "read-write",
        }
    }
}

/// The object-storage prefix a table owns, parsed from its location URI.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct TableScope {
    /// Bucket name.
    pub bucket: String,
    /// Key prefix under the bucket (no leading/trailing slash; never empty —
    /// a table is never the whole bucket).
    pub key_prefix: String,
    /// The original location URI (`s3://bucket/prefix`), echoed back as the
    /// `prefix` of the vended credentials.
    pub location: String,
}

impl TableScope {
    /// Parses an `s3://bucket/prefix` (or `s3a://`) table location.
    ///
    /// # Errors
    ///
    /// Returns [`VendingError::UnsupportedLocation`] for non-S3 schemes and
    /// for locations without a key prefix (scoping to a whole bucket would
    /// defeat table isolation).
    pub fn from_s3_location(location: &str) -> Result<Self, VendingError> {
        let location = location.trim_end_matches('/');
        let rest = location
            .split_once("://")
            .filter(|(scheme, _)| {
                scheme.eq_ignore_ascii_case("s3") || scheme.eq_ignore_ascii_case("s3a")
            })
            .map(|(_, rest)| rest)
            .ok_or_else(|| VendingError::UnsupportedLocation(location.to_owned()))?;
        let (bucket, prefix) = rest
            .split_once('/')
            .map(|(bucket, prefix)| (bucket, prefix.trim_matches('/')))
            .ok_or_else(|| VendingError::UnsupportedLocation(location.to_owned()))?;
        if bucket.is_empty() || prefix.is_empty() {
            return Err(VendingError::UnsupportedLocation(location.to_owned()));
        }
        Ok(Self {
            bucket: bucket.to_owned(),
            key_prefix: prefix.to_owned(),
            location: location.to_owned(),
        })
    }
}

/// Credentials scoped to one table prefix, in Iceberg client property form.
#[derive(Debug, Clone)]
pub struct VendedCredentials {
    /// The storage prefix the credentials are valid for (the table
    /// location) — the `prefix` field of the IRC `StorageCredential`.
    pub prefix: String,
    /// Client properties (`s3.access-key-id`, `s3.secret-access-key`,
    /// `s3.session-token`, `s3.session-token-expires-at-ms`, ...).
    pub config: BTreeMap<String, String>,
    /// When the credentials expire; `None` for static passthrough keys.
    pub expires_at: Option<DateTime<Utc>>,
}

/// Why a vend failed.
#[derive(Debug, thiserror::Error)]
pub enum VendingError {
    /// Credential vending for this cloud is not implemented yet.
    #[error("credential vending for {cloud} is not implemented yet: {detail}")]
    UnsupportedCloud {
        /// Cloud family (`gcs`, `azure`).
        cloud: &'static str,
        /// What to do instead.
        detail: String,
    },
    /// The table location cannot be scoped (non-S3 scheme, or no prefix).
    #[error(
        "cannot vend credentials for location {0:?}: only s3://bucket/prefix locations are supported"
    )]
    UnsupportedLocation(String),
    /// The vending configuration is unusable.
    #[error("vending configuration error: {0}")]
    Config(String),
    /// The credential provider (STS) rejected or failed the request.
    #[error("credential provider error: {0}")]
    Provider(String),
}

/// Vends scoped, short-lived credentials for a table prefix.
///
/// Not dyn-compatible (async method); the server dispatches through
/// [`Vendor`], which wraps every implementation.
pub trait CredentialVendor: Send + Sync {
    /// Vends credentials for `scope`, limited to `access`, valid for `ttl`.
    fn vend(
        &self,
        scope: &TableScope,
        access: AccessMode,
        ttl: Duration,
    ) -> impl Future<Output = Result<VendedCredentials, VendingError>> + Send;
}

/// The concrete vendor implementations behind one dispatchable type.
#[derive(Debug)]
pub enum Vendor {
    /// AWS STS `AssumeRole` (AWS, `MinIO`).
    Sts(StsVendor),
    /// Static key passthrough (explicit warehouse opt-in).
    Static(StaticVendor),
    /// GCS downscoped tokens — not implemented yet.
    Gcs(GcsVendor),
    /// Azure user-delegation SAS — not implemented yet.
    Azure(AzureVendor),
}

impl CredentialVendor for Vendor {
    async fn vend(
        &self,
        scope: &TableScope,
        access: AccessMode,
        ttl: Duration,
    ) -> Result<VendedCredentials, VendingError> {
        match self {
            Self::Sts(v) => v.vend(scope, access, ttl).await,
            Self::Static(v) => v.vend(scope, access, ttl).await,
            Self::Gcs(v) => v.vend(scope, access, ttl).await,
            Self::Azure(v) => v.vend(scope, access, ttl).await,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn table_scope_parses_s3_locations() {
        let scope = TableScope::from_s3_location("s3://bucket/wh/ns/t-uuid/").expect("parse");
        assert_eq!(scope.bucket, "bucket");
        assert_eq!(scope.key_prefix, "wh/ns/t-uuid");
        assert_eq!(scope.location, "s3://bucket/wh/ns/t-uuid");

        let scope = TableScope::from_s3_location("s3a://b/p").expect("s3a alias");
        assert_eq!(scope.bucket, "b");
        assert_eq!(scope.key_prefix, "p");
    }

    #[test]
    fn table_scope_rejects_non_s3_and_bucket_roots() {
        for bad in [
            "file:///tmp/wh/t",
            "gs://bucket/x",
            "s3://bucket",
            "s3://bucket/",
            "s3:///prefix",
            "plain/path",
        ] {
            assert!(
                matches!(
                    TableScope::from_s3_location(bad),
                    Err(VendingError::UnsupportedLocation(_))
                ),
                "expected rejection for {bad:?}"
            );
        }
    }
}
