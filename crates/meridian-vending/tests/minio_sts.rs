//! STS vending against a real local `MinIO`: the vended, session-policy-scoped
//! credentials must be honored by the object store exactly as scoped.
//!
//! Conventions shared with `meridian-storage/tests/storage_backends.rs`:
//! `MinIO` on `localhost:9000` (root user `meridian`/`meridian123`), the
//! long-lived `meridian-warehouse` bucket, a per-run prefix for isolation,
//! and skip — not fail — when `MinIO` is unreachable.

use std::collections::BTreeMap;
use std::net::TcpStream;
use std::sync::Arc;
use std::time::Duration;

use meridian_storage::{Storage, StorageError, StorageProfile};
use meridian_vending::{AccessMode, CredentialVendor, StsVendor, TableScope, VendedCredentials};

const ENDPOINT: &str = "http://localhost:9000";
const BUCKET: &str = "meridian-warehouse";
const ROOT_ACCESS_KEY: &str = "meridian";
const ROOT_SECRET_KEY: &str = "meridian123";
/// `MinIO` treats the role ARN as an opaque required parameter.
const ROLE_ARN: &str = "arn:minio:iam:::role/meridian-vend";
/// The STS `DurationSeconds` floor.
const TTL: Duration = Duration::from_secs(900);

fn minio_reachable() -> bool {
    TcpStream::connect_timeout(
        &"127.0.0.1:9000".parse().expect("static addr"),
        Duration::from_millis(500),
    )
    .is_ok()
}

/// A storage handle over the whole bucket, using the given credentials.
fn bucket_storage(credentials: &BTreeMap<String, String>) -> Arc<dyn Storage> {
    let mut options = BTreeMap::new();
    options.insert("endpoint".to_owned(), ENDPOINT.to_owned());
    options.insert("region".to_owned(), "us-east-1".to_owned());
    options.insert("path-style".to_owned(), "true".to_owned());
    for (theirs, ours) in [
        ("s3.access-key-id", "access-key-id"),
        ("s3.secret-access-key", "secret-access-key"),
        ("s3.session-token", "session-token"),
    ] {
        if let Some(value) = credentials.get(theirs) {
            options.insert(ours.to_owned(), value.clone());
        }
    }
    StorageProfile::parse(&format!("s3://{BUCKET}"), &options)
        .expect("parse profile")
        .connect()
        .expect("connect")
}

fn root_credentials() -> BTreeMap<String, String> {
    BTreeMap::from([
        ("s3.access-key-id".to_owned(), ROOT_ACCESS_KEY.to_owned()),
        (
            "s3.secret-access-key".to_owned(),
            ROOT_SECRET_KEY.to_owned(),
        ),
    ])
}

fn vendor() -> StsVendor {
    StsVendor::new(
        ROLE_ARN,
        "us-east-1",
        Some(ENDPOINT.to_owned()),
        Some((ROOT_ACCESS_KEY.to_owned(), ROOT_SECRET_KEY.to_owned())),
        "vending-integration-test",
    )
}

async fn vend(scope: &TableScope, access: AccessMode) -> VendedCredentials {
    vendor().vend(scope, access, TTL).await.expect("vend")
}

fn assert_denied(context: &str, result: Result<impl std::fmt::Debug, StorageError>) {
    match result {
        // A denied GetObject can surface as 403-masquerading-as-404 when the
        // store hides existence; both prove the credential cannot reach it.
        Err(StorageError::PermissionDenied { .. } | StorageError::NotFound { .. }) => {}
        other => panic!("{context}: expected access denial, got {other:?}"),
    }
}

#[tokio::test]
#[allow(clippy::too_many_lines)] // one scenario: seed, vend read, vend read-write, verify every boundary
async fn vended_credentials_are_scoped_to_the_table_prefix() {
    if !minio_reachable() {
        eprintln!("SKIP: vended_credentials_are_scoped_to_the_table_prefix — no MinIO on :9000");
        return;
    }

    // Seed two "tables" under a per-run prefix with the root credentials.
    let run = ulid_like();
    let table_a = format!("s3://{BUCKET}/vend-{run}/ns/table_a-uuid");
    let table_b = format!("s3://{BUCKET}/vend-{run}/ns/table_b-uuid");
    let root = bucket_storage(&root_credentials());
    root.write(&format!("{table_a}/data/a.parquet"), "AAAA".into())
        .await
        .expect("seed table a");
    root.write(&format!("{table_b}/data/b.parquet"), "BBBB".into())
        .await
        .expect("seed table b");

    let scope_a = TableScope::from_s3_location(&table_a).expect("scope");

    // --- Read-only vend for table A -------------------------------------
    let read = vend(&scope_a, AccessMode::Read).await;
    assert_eq!(read.prefix, table_a);
    for key in [
        "s3.access-key-id",
        "s3.secret-access-key",
        "s3.session-token",
        "s3.session-token-expires-at-ms",
    ] {
        assert!(read.config.contains_key(key), "missing {key}");
    }
    // Fresh session keys, never the parent's.
    assert_ne!(read.config["s3.access-key-id"], ROOT_ACCESS_KEY);
    assert_ne!(read.config["s3.secret-access-key"], ROOT_SECRET_KEY);

    // Expiry respects the requested TTL (short TTL; generous clock slack).
    let expires_at = read.expires_at.expect("sts credentials expire");
    let lifetime = (expires_at - chrono::Utc::now()).num_seconds();
    let ttl = i64::try_from(TTL.as_secs()).expect("ttl fits");
    assert!(
        (lifetime - ttl).abs() <= 120,
        "expiry {lifetime}s should be about the requested {ttl}s"
    );

    let reader = bucket_storage(&read.config);
    let bytes = reader
        .read(&format!("{table_a}/data/a.parquet"))
        .await
        .expect("read-only creds must read table A");
    assert_eq!(&bytes[..], b"AAAA");
    assert_denied(
        "read-only creds writing table A",
        reader
            .write(&format!("{table_a}/data/illegal.parquet"), "X".into())
            .await,
    );
    assert_denied(
        "read-only creds deleting from table A",
        reader.delete(&format!("{table_a}/data/a.parquet")).await,
    );
    assert_denied(
        "table-A creds reading table B",
        reader.read(&format!("{table_b}/data/b.parquet")).await,
    );

    // --- Read-write vend for table A ------------------------------------
    let write = vend(&scope_a, AccessMode::ReadWrite).await;
    let writer = bucket_storage(&write.config);
    writer
        .write(&format!("{table_a}/data/c.parquet"), "CCCC".into())
        .await
        .expect("read-write creds must write table A");
    let bytes = writer
        .read(&format!("{table_a}/data/c.parquet"))
        .await
        .expect("read-write creds must read table A");
    assert_eq!(&bytes[..], b"CCCC");
    writer
        .delete(&format!("{table_a}/data/c.parquet"))
        .await
        .expect("read-write creds must delete in table A");
    assert_denied(
        "read-write table-A creds writing table B",
        writer
            .write(&format!("{table_b}/data/illegal.parquet"), "X".into())
            .await,
    );
    assert_denied(
        "read-write table-A creds reading table B",
        writer.read(&format!("{table_b}/data/b.parquet")).await,
    );

    // Cleanup (best-effort; the per-run prefix isolates leftovers anyway).
    let _ = root
        .delete_prefix(&format!("s3://{BUCKET}/vend-{run}"))
        .await;
}

/// A time-based unique run id without pulling in the ulid crate.
fn ulid_like() -> String {
    format!(
        "{:x}-{:x}",
        std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .expect("clock after epoch")
            .as_millis(),
        std::process::id()
    )
}
