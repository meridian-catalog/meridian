//! View metadata-file helpers against the local filesystem backend: the
//! naming convention, the lossless read/write round trip (including unknown
//! fields), the never-overwrite rule, and semantic read errors.
//!
//! The fs backend is enough here: `read_view_metadata`/`write_view_metadata`
//! only compose parsing with the `Storage` trait, and the trait itself is
//! covered for every backend by `storage_backends.rs`.

use std::collections::BTreeMap;

use bytes::Bytes;
use meridian_iceberg::spec::ViewMetadata;
use meridian_storage::{
    StorageError, StorageProfile, new_view_metadata_location, read_view_metadata,
    write_view_metadata,
};
use uuid::Uuid;

/// A view `metadata.json` fixture with fields no version of the typed model
/// knows (including a whole unknown representation type) to exercise
/// unknown-field preservation end to end.
const VIEW_METADATA_JSON: &str = r#"{
  "view-uuid": "fa6506c3-7681-40c8-86dc-e36561f83385",
  "format-version": 1,
  "location": "s3://bucket/wh/db/event_agg",
  "current-version-id": 1,
  "properties": { "comment": "Daily event counts" },
  "schemas": [
    {
      "type": "struct",
      "schema-id": 1,
      "fields": [{ "id": 1, "name": "event_count", "required": false, "type": "int" }]
    }
  ],
  "versions": [
    {
      "version-id": 1,
      "timestamp-ms": 1573518431292,
      "schema-id": 1,
      "default-namespace": ["db"],
      "summary": { "engine-name": "meridian-tests" },
      "representations": [
        { "type": "sql", "sql": "SELECT COUNT(1) FROM events", "dialect": "spark" },
        { "type": "x-meridian-test-plan", "plan": "opaque" }
      ]
    }
  ],
  "version-log": [{ "timestamp-ms": 1573518431292, "version-id": 1 }],
  "meridian-test-extension": { "purpose": "unknown-field preservation" }
}"#;

#[tokio::test]
async fn view_metadata_files_round_trip_on_fs() {
    let dir = tempfile::tempdir().expect("tempdir");
    let uri = format!("file://{}", dir.path().display());
    let profile = StorageProfile::parse(&uri, &BTreeMap::new()).expect("profile");
    let storage = profile.connect().expect("connect");
    let root = storage.root_uri().to_owned();

    let view_location = format!("{root}/db/event_agg");
    let commit_uuid = Uuid::new_v4();
    let metadata_location = new_view_metadata_location(&view_location, 1, commit_uuid);
    assert_eq!(
        metadata_location,
        format!("{view_location}/metadata/00001-{commit_uuid}.metadata.json")
    );

    let metadata = ViewMetadata::from_json(VIEW_METADATA_JSON).expect("fixture parses");
    write_view_metadata(storage.as_ref(), &metadata_location, &metadata)
        .await
        .expect("write view metadata");

    let read_back = read_view_metadata(storage.as_ref(), &metadata_location)
        .await
        .expect("read view metadata");
    assert_eq!(
        read_back, metadata,
        "view metadata must round-trip losslessly"
    );
    assert!(
        read_back.extra.contains_key("meridian-test-extension"),
        "unknown fields must survive the storage round trip"
    );
    assert_eq!(
        read_back.versions[0].representations.len(),
        2,
        "the unknown representation type must survive too"
    );

    // Immutability: staging to the same location again must fail...
    let clash = write_view_metadata(storage.as_ref(), &metadata_location, &metadata).await;
    assert!(matches!(clash, Err(StorageError::AlreadyExists { .. })));
    // ...and a fresh attempt gets a fresh name.
    let retry_location = new_view_metadata_location(&view_location, 1, Uuid::new_v4());
    assert_ne!(retry_location, metadata_location);
    write_view_metadata(storage.as_ref(), &retry_location, &metadata)
        .await
        .expect("staging under a fresh name succeeds");

    // Reading a missing, corrupt, or future-versioned file reports
    // semantically.
    let missing = read_view_metadata(
        storage.as_ref(),
        &new_view_metadata_location(&view_location, 99, Uuid::new_v4()),
    )
    .await;
    assert!(matches!(missing, Err(StorageError::NotFound { .. })));

    let corrupt_location = format!("{view_location}/metadata/corrupt.metadata.json");
    storage
        .write(&corrupt_location, Bytes::from_static(b"{ not json"))
        .await
        .expect("write corrupt");
    let corrupt = read_view_metadata(storage.as_ref(), &corrupt_location).await;
    assert!(matches!(corrupt, Err(StorageError::InvalidMetadata { .. })));

    let future_location = format!("{view_location}/metadata/future.metadata.json");
    storage
        .write(
            &future_location,
            Bytes::from_static(
                br#"{"format-version": 2, "view-uuid": "fa6506c3-7681-40c8-86dc-e36561f83385"}"#,
            ),
        )
        .await
        .expect("write future-versioned file");
    let future = read_view_metadata(storage.as_ref(), &future_location).await;
    assert!(matches!(
        future,
        Err(StorageError::UnsupportedFormatVersion { found: 2, .. })
    ));
}
