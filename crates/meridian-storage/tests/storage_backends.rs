//! Backend-conformance suite for [`meridian_storage::Storage`].
//!
//! The same suite runs against the local filesystem backend (always) and an
//! S3-compatible backend (`MinIO`, when reachable). Keeping one suite is the
//! point: the two backends must be observationally identical through the
//! trait.
//!
//! # `MinIO` setup (matches `docs/adr/004-opendal-storage-io.md`)
//!
//! ```sh
//! docker run -d --name meridian-minio -p 9000:9000 -p 9001:9001 \
//!   -e MINIO_ROOT_USER=meridian -e MINIO_ROOT_PASSWORD=meridian123 \
//!   minio/minio server /data --console-address :9001
//! # create the test bucket (any `SigV4` client works; curl shown):
//! curl --aws-sigv4 "aws:amz:us-east-1:s3" --user meridian:meridian123 \
//!   -X PUT http://localhost:9000/meridian-warehouse
//! ```
//!
//! When `MinIO` is not reachable the S3 tests print a skip note and pass —
//! they must never fail a checkout that has no Docker.

use std::collections::BTreeMap;
use std::net::TcpStream;
use std::sync::Arc;
use std::time::Duration;

use bytes::Bytes;
use futures::TryStreamExt;
use meridian_iceberg::spec::TableMetadata;
use meridian_storage::{
    StorageError, StorageProfile, StorageScheme, new_metadata_location, read_table_metadata,
    write_table_metadata,
};
use uuid::Uuid;

/// A v2 `metadata.json` fixture with a field no version of the typed model
/// knows (`meridian-test-extension`) to exercise unknown-field preservation
/// end to end.
const METADATA_JSON: &str = r#"{
  "format-version": 2,
  "table-uuid": "9c12d441-03fe-4693-9a96-a0705ddf69c1",
  "location": "s3://bucket/wh/db/tbl",
  "last-sequence-number": 1,
  "last-updated-ms": 1602638573590,
  "last-column-id": 1,
  "current-schema-id": 0,
  "schemas": [
    {
      "type": "struct",
      "schema-id": 0,
      "fields": [{ "id": 1, "name": "id", "required": true, "type": "long" }]
    }
  ],
  "default-spec-id": 0,
  "partition-specs": [{ "spec-id": 0, "fields": [] }],
  "last-partition-id": 999,
  "default-sort-order-id": 0,
  "sort-orders": [{ "order-id": 0, "fields": [] }],
  "properties": { "owner": "meridian-tests" },
  "meridian-test-extension": { "purpose": "unknown-field preservation" }
}"#;

/// Runs the full conformance suite against a connected storage handle whose
/// root is empty and exclusively ours.
///
/// Deliberately one linear script rather than many tests: the steps build on
/// each other (write → list → delete), and the point is running the identical
/// sequence against every backend.
#[allow(clippy::too_many_lines)]
async fn run_suite(storage: Arc<dyn meridian_storage::Storage>) {
    let root = storage.root_uri().to_owned();

    // --- read / write / exists / delete round trip -----------------------
    let loc = format!("{root}/data/a/file-1.bin");
    assert!(!storage.exists(&loc).await.expect("exists pre"));
    assert!(matches!(
        storage.read(&loc).await,
        Err(StorageError::NotFound { .. })
    ));

    storage
        .write(&loc, Bytes::from_static(b"v1"))
        .await
        .expect("write");
    assert!(storage.exists(&loc).await.expect("exists post"));
    assert_eq!(storage.read(&loc).await.expect("read"), Bytes::from("v1"));

    // Unconditional write replaces.
    storage
        .write(&loc, Bytes::from_static(b"v2 longer"))
        .await
        .expect("overwrite");
    assert_eq!(
        storage.read(&loc).await.expect("read v2"),
        Bytes::from("v2 longer")
    );

    // Root-relative addressing reaches the same object.
    assert_eq!(
        storage
            .read("data/a/file-1.bin")
            .await
            .expect("relative read"),
        Bytes::from("v2 longer")
    );

    // Delete is effective and idempotent.
    storage.delete(&loc).await.expect("delete");
    assert!(!storage.exists(&loc).await.expect("exists after delete"));
    storage.delete(&loc).await.expect("idempotent delete");

    // --- write_if_absent: the metadata.json immutability primitive -------
    let guarded = format!("{root}/data/guarded.bin");
    storage
        .write_if_absent(&guarded, Bytes::from_static(b"first"))
        .await
        .expect("first conditional write");
    let second = storage
        .write_if_absent(&guarded, Bytes::from_static(b"second"))
        .await;
    assert!(
        matches!(second, Err(StorageError::AlreadyExists { .. })),
        "conditional overwrite must fail AlreadyExists, got: {second:?}"
    );
    assert_eq!(
        storage.read(&guarded).await.expect("read guarded"),
        Bytes::from("first"),
        "loser of a conditional write must not change content"
    );

    // --- list: recursive, files only, sizes, mtimes -----------------------
    for (name, body) in [
        ("m/1.json", "aa"),
        ("m/2.json", "bbbb"),
        ("m/sub/3.json", "c"),
    ] {
        storage
            .write(&format!("{root}/listing/{name}"), Bytes::from(body))
            .await
            .expect("seed listing");
    }
    let mut listed: Vec<_> = storage
        .list(&format!("{root}/listing/m"))
        .await
        .expect("list")
        .try_collect::<Vec<_>>()
        .await
        .expect("collect");
    listed.sort_by(|a, b| a.location.cmp(&b.location));
    assert_eq!(
        listed
            .iter()
            .map(|o| o.location.as_str())
            .collect::<Vec<_>>(),
        vec![
            format!("{root}/listing/m/1.json"),
            format!("{root}/listing/m/2.json"),
            format!("{root}/listing/m/sub/3.json"),
        ]
    );
    assert_eq!(
        listed.iter().map(|o| o.size).collect::<Vec<_>>(),
        vec![2, 4, 1]
    );
    for object in &listed {
        assert!(
            object.last_modified.is_some(),
            "backend should report mtime for {}",
            object.location
        );
    }

    // Listing a prefix nobody wrote to is empty, not an error.
    let empty: Vec<_> = storage
        .list(&format!("{root}/no/such/prefix"))
        .await
        .expect("list missing prefix")
        .try_collect::<Vec<_>>()
        .await
        .expect("collect empty");
    assert!(empty.is_empty());

    // --- delete_prefix -----------------------------------------------------
    storage
        .delete_prefix(&format!("{root}/listing/m/sub"))
        .await
        .expect("delete_prefix");
    let after: Vec<_> = storage
        .list(&format!("{root}/listing"))
        .await
        .expect("list after delete_prefix")
        .try_collect::<Vec<_>>()
        .await
        .expect("collect after");
    assert_eq!(after.len(), 2, "only the sub/ objects go: {after:?}");
    // Deleting a missing prefix is fine; deleting the whole root implicitly
    // (empty prefix) is refused.
    storage
        .delete_prefix(&format!("{root}/no/such/prefix"))
        .await
        .expect("delete missing prefix");
    assert!(matches!(
        storage.delete_prefix("").await,
        Err(StorageError::InvalidLocation { .. })
    ));

    // --- locations outside the root are refused ---------------------------
    assert!(matches!(
        storage.read("s3://not-our-bucket/x").await,
        Err(StorageError::InvalidLocation { .. })
    ));
    assert!(matches!(
        storage.read("../escape").await,
        Err(StorageError::InvalidLocation { .. })
    ));

    // --- metadata-file helpers --------------------------------------------
    let table_location = format!("{root}/db/tbl");
    let commit_uuid = Uuid::new_v4();
    let metadata_location = new_metadata_location(&table_location, 1, commit_uuid);
    assert_eq!(
        metadata_location,
        format!("{table_location}/metadata/00001-{commit_uuid}.metadata.json")
    );

    let metadata = TableMetadata::from_json(METADATA_JSON).expect("fixture parses");
    write_table_metadata(storage.as_ref(), &metadata_location, &metadata)
        .await
        .expect("write metadata");

    let read_back = read_table_metadata(storage.as_ref(), &metadata_location)
        .await
        .expect("read metadata");
    assert_eq!(read_back, metadata, "metadata must round-trip losslessly");
    assert!(
        read_back.extra.contains_key("meridian-test-extension"),
        "unknown fields must survive the storage round trip"
    );

    // Immutability: staging to the same location again must fail...
    let clash = write_table_metadata(storage.as_ref(), &metadata_location, &metadata).await;
    assert!(matches!(clash, Err(StorageError::AlreadyExists { .. })));
    // ...and a fresh attempt gets a fresh name.
    let retry_location = new_metadata_location(&table_location, 1, Uuid::new_v4());
    assert_ne!(retry_location, metadata_location);
    write_table_metadata(storage.as_ref(), &retry_location, &metadata)
        .await
        .expect("staging under a fresh name succeeds");

    // Reading a missing or corrupt metadata file reports semantically.
    let missing = read_table_metadata(
        storage.as_ref(),
        &new_metadata_location(&table_location, 99, Uuid::new_v4()),
    )
    .await;
    assert!(matches!(missing, Err(StorageError::NotFound { .. })));

    let corrupt_location = format!("{table_location}/metadata/corrupt.metadata.json");
    storage
        .write(&corrupt_location, Bytes::from_static(b"{ not json"))
        .await
        .expect("write corrupt");
    let corrupt = read_table_metadata(storage.as_ref(), &corrupt_location).await;
    assert!(matches!(corrupt, Err(StorageError::InvalidMetadata { .. })));
}

// ---------------------------------------------------------------------------
// Local filesystem backend
// ---------------------------------------------------------------------------

#[tokio::test]
async fn fs_backend_conformance() {
    let dir = tempfile::tempdir().expect("tempdir");
    let uri = format!("file://{}", dir.path().display());
    let profile = StorageProfile::parse(&uri, &BTreeMap::new()).expect("profile");
    assert_eq!(profile.scheme(), StorageScheme::Fs);
    let storage = profile.connect().expect("connect");
    run_suite(storage).await;
}

#[tokio::test]
async fn fs_backend_accepts_relative_root() {
    let dir = tempfile::tempdir().expect("tempdir");
    let previous = std::env::current_dir().expect("cwd");
    // Parse a *relative* root while chdir'd into the tempdir, then verify it
    // was pinned to an absolute location at parse time.
    std::env::set_current_dir(dir.path()).expect("chdir");
    let profile = StorageProfile::parse("./warehouse", &BTreeMap::new()).expect("profile");
    std::env::set_current_dir(previous).expect("chdir back");

    let root = profile.root_uri();
    assert!(root.starts_with("file:///"), "not absolute: {root}");
    assert!(root.ends_with("/warehouse"), "unexpected root: {root}");

    let storage = profile.connect().expect("connect");
    storage
        .write("greeting.txt", Bytes::from_static(b"hello"))
        .await
        .expect("write through relative-rooted profile");
    assert_eq!(
        storage.read("greeting.txt").await.expect("read"),
        Bytes::from("hello")
    );
}

// ---------------------------------------------------------------------------
// S3-compatible backend (MinIO)
// ---------------------------------------------------------------------------

const MINIO_ENDPOINT: &str = "http://localhost:9000";
const MINIO_BUCKET: &str = "meridian-warehouse";

fn minio_reachable() -> bool {
    TcpStream::connect_timeout(
        &"127.0.0.1:9000".parse().expect("static addr"),
        Duration::from_millis(500),
    )
    .is_ok()
}

fn minio_profile(run_prefix: &str) -> StorageProfile {
    let options: BTreeMap<String, String> = [
        ("endpoint", MINIO_ENDPOINT),
        ("region", "us-east-1"),
        ("path-style", "true"),
        ("access-key-id", "meridian"),
        ("secret-access-key", "meridian123"),
    ]
    .into_iter()
    .map(|(k, v)| (k.to_owned(), v.to_owned()))
    .collect();
    StorageProfile::parse(&format!("s3://{MINIO_BUCKET}/{run_prefix}"), &options)
        .expect("minio profile")
}

#[tokio::test]
async fn s3_backend_conformance() {
    if !minio_reachable() {
        eprintln!(
            "SKIP: s3_backend_conformance — MinIO not reachable on localhost:9000; \
             see the module docs for the docker one-liner"
        );
        return;
    }
    // A per-run prefix keeps runs isolated in the long-lived dev bucket.
    let profile = minio_profile(&format!("suite-{}", Uuid::new_v4()));
    assert_eq!(profile.scheme(), StorageScheme::S3);
    let storage = profile.connect().expect("connect");
    run_suite(Arc::clone(&storage)).await;
    // Best-effort cleanup of this run's objects.
    let root = storage.root_uri().to_owned();
    let _ = storage.delete_prefix(&format!("{root}/data")).await;
    let _ = storage.delete_prefix(&format!("{root}/listing")).await;
    let _ = storage.delete_prefix(&format!("{root}/db")).await;
}

#[tokio::test]
async fn s3_write_if_absent_race_single_winner() {
    if !minio_reachable() {
        eprintln!("SKIP: s3_write_if_absent_race_single_winner — MinIO not reachable");
        return;
    }
    let profile = minio_profile(&format!("race-{}", Uuid::new_v4()));
    let storage = profile.connect().expect("connect");
    let location = format!(
        "{}/metadata/00001-contended.metadata.json",
        storage.root_uri()
    );

    // Many concurrent conditional writers; exactly one may win.
    let mut tasks = Vec::new();
    for i in 0..8u32 {
        let storage = Arc::clone(&storage);
        let location = location.clone();
        tasks.push(tokio::spawn(async move {
            storage
                .write_if_absent(&location, Bytes::from(format!("writer-{i}")))
                .await
                .map(|()| i)
        }));
    }
    let mut winners = Vec::new();
    for task in tasks {
        match task.await.expect("join") {
            Ok(i) => winners.push(i),
            Err(StorageError::AlreadyExists { .. }) => {}
            Err(other) => panic!("unexpected error from contended write: {other:?}"),
        }
    }
    assert_eq!(winners.len(), 1, "exactly one conditional writer must win");
    let body = storage.read(&location).await.expect("read winner");
    assert_eq!(body, Bytes::from(format!("writer-{}", winners[0])));

    let _ = storage
        .delete_prefix(&format!("{}/metadata", storage.root_uri()))
        .await;
}
