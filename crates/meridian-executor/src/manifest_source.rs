//! Reading parsed manifests, from object storage or (in tests) memory.
//!
//! The compaction engine needs the current snapshot's manifest list and its
//! manifests as parsed [`ManifestList`]/[`Manifest`] values. In production
//! those come from the warehouse's object storage via [`meridian_storage`];
//! the trait keeps the engine testable against an in-memory map (and mirrors
//! the shape of `meridian-server`'s planning `ManifestSource`, though that one
//! is private to the server crate).

use std::sync::Arc;

use meridian_iceberg::manifest::{Manifest, ManifestList, read_manifest, read_manifest_list};
use meridian_storage::Storage;

use crate::error::{CompactionError, CompactionResult};

/// An async source of parsed manifest Avro.
#[allow(async_fn_in_trait)] // internal trait; no external impls to worry about Send bounds for
pub trait ManifestSource {
    /// Fetches and parses the snapshot's manifest list.
    async fn manifest_list(&self, location: &str) -> CompactionResult<Arc<ManifestList>>;
    /// Fetches and parses one manifest.
    async fn manifest(&self, location: &str) -> CompactionResult<Arc<Manifest>>;
}

/// A [`ManifestSource`] backed by a warehouse [`Storage`] handle. No caching:
/// a compaction run reads each manifest once.
#[derive(Debug)]
pub struct StorageManifestSource<'a> {
    storage: &'a dyn Storage,
}

impl<'a> StorageManifestSource<'a> {
    /// Wraps a storage handle.
    #[must_use]
    pub fn new(storage: &'a dyn Storage) -> Self {
        Self { storage }
    }
}

impl ManifestSource for StorageManifestSource<'_> {
    async fn manifest_list(&self, location: &str) -> CompactionResult<Arc<ManifestList>> {
        let bytes = self.storage.read(location).await?;
        let list = read_manifest_list(&bytes).map_err(CompactionError::Manifest)?;
        Ok(Arc::new(list))
    }

    async fn manifest(&self, location: &str) -> CompactionResult<Arc<Manifest>> {
        let bytes = self.storage.read(location).await?;
        let manifest = read_manifest(&bytes).map_err(CompactionError::Manifest)?;
        Ok(Arc::new(manifest))
    }
}
