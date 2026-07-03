//! Server-side scan planning (IRC `planTableScan` and friends).
//!
//! Module layout: [`rest`] owns the wire shapes, [`engine`] owns pruning /
//! delete attachment / residuals / page assembly, [`cache`] owns the
//! manifest cache tiers, and this module owns the runtime pieces — the
//! per-process [`PlanningRuntime`] (shared caches, the bounded async plan
//! pool), the tiered [`ManifestIo`] reader, and the background sweep. The
//! HTTP handlers live in `crate::routes::planning`; the design rationale
//! in `docs/design/scan-planning.md`.

pub mod cache;
pub mod engine;
pub mod rest;

use std::sync::Arc;
use std::time::Duration;

use meridian_common::config::PlanningConfig;
use meridian_iceberg::manifest::{Manifest, ManifestList};
use meridian_storage::Storage;
use meridian_store::planning as store;
use sqlx::PgPool;

use cache::{CacheCounters, CachedManifest, ManifestLru};
use engine::{ManifestSource, PlanError};

/// Per-process planning state, shared by all handlers via an axum
/// `Extension` (constructed once in `build_router`).
#[derive(Debug)]
pub struct PlanningRuntime {
    /// Parsed-manifest LRU (tier 1).
    pub lru: ManifestLru,
    /// Bounded concurrency for asynchronous plan execution; a submission
    /// that cannot get a permit is rejected with 503 rather than queued
    /// without bound.
    pub semaphore: Arc<tokio::sync::Semaphore>,
    /// Cache tier hit counters.
    pub counters: CacheCounters,
}

impl PlanningRuntime {
    /// Builds the runtime from configuration.
    #[must_use]
    pub fn from_config(config: &PlanningConfig) -> Arc<Self> {
        Arc::new(Self {
            lru: ManifestLru::new(usize::try_from(config.cache_max_bytes).unwrap_or(usize::MAX)),
            semaphore: Arc::new(tokio::sync::Semaphore::new(
                config.max_concurrent_plans.max(1),
            )),
            counters: CacheCounters::default(),
        })
    }
}

/// The tiered manifest reader: in-process LRU, then the Postgres byte
/// cache, then object storage (with write-through to both caches).
pub struct ManifestIo<'a> {
    /// Shared runtime (LRU + counters).
    pub runtime: &'a PlanningRuntime,
    /// Pool for the Postgres byte cache.
    pub pool: &'a PgPool,
    /// The warehouse's storage.
    pub storage: &'a dyn Storage,
    /// Warehouse scope for the byte cache.
    pub warehouse_id: &'a str,
    /// Whether the Postgres tier is enabled
    /// (`planning.pg_cache_max_bytes > 0`).
    pub pg_cache: bool,
}

impl std::fmt::Debug for ManifestIo<'_> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("ManifestIo")
            .field("warehouse_id", &self.warehouse_id)
            .field("pg_cache", &self.pg_cache)
            .finish_non_exhaustive()
    }
}

enum ManifestKind {
    List,
    File,
}

impl ManifestIo<'_> {
    async fn load(&self, location: &str, kind: &ManifestKind) -> Result<CachedManifest, PlanError> {
        if let Some(hit) = self.runtime.lru.get(location) {
            self.runtime.counters.record_lru_hit();
            return Ok(hit);
        }

        let unreadable = |reason: String| PlanError::Unreadable {
            location: location.to_owned(),
            reason,
        };

        let mut bytes: Option<Vec<u8>> = None;
        if self.pg_cache {
            // A byte-cache failure must not fail the plan; storage is the
            // source of truth. Log and fall through.
            match store::manifest_cache_get(self.pool, self.warehouse_id, location).await {
                Ok(cached) => {
                    if cached.is_some() {
                        self.runtime.counters.record_pg_hit();
                    }
                    bytes = cached;
                }
                Err(error) => {
                    tracing::warn!(%error, location, "manifest byte cache read failed; going to storage");
                }
            }
        }

        let bytes = if let Some(bytes) = bytes {
            bytes
        } else {
            self.runtime.counters.record_storage_read();
            let read = self
                .storage
                .read(location)
                .await
                .map_err(|e| unreadable(e.to_string()))?
                .to_vec();
            if self.pg_cache
                && let Err(error) =
                    store::manifest_cache_put(self.pool, self.warehouse_id, location, &read).await
            {
                tracing::warn!(%error, location, "manifest byte cache write failed; continuing");
            }
            read
        };

        let parsed = match kind {
            ManifestKind::List => CachedManifest::parse_list(&bytes),
            ManifestKind::File => CachedManifest::parse_manifest(&bytes),
        }
        .map_err(unreadable)?;
        self.runtime.lru.put(location, parsed.clone());
        Ok(parsed)
    }
}

impl ManifestSource for ManifestIo<'_> {
    async fn manifest_list(&self, location: &str) -> Result<Arc<ManifestList>, PlanError> {
        match self.load(location, &ManifestKind::List).await? {
            CachedManifest::List(list) => Ok(list),
            CachedManifest::File(_) => Err(PlanError::Unreadable {
                location: location.to_owned(),
                reason: "cached artifact is a manifest, expected a manifest list".to_owned(),
            }),
        }
    }

    async fn manifest(&self, location: &str) -> Result<Arc<Manifest>, PlanError> {
        match self.load(location, &ManifestKind::File).await? {
            CachedManifest::File(manifest) => Ok(manifest),
            CachedManifest::List(_) => Err(PlanError::Unreadable {
                location: location.to_owned(),
                reason: "cached artifact is a manifest list, expected a manifest".to_owned(),
            }),
        }
    }
}

/// The background planning sweep: deletes expired plans (crash-orphaned
/// `submitted` rows included — a worker that died leaves a row that
/// simply ages out) and enforces the Postgres manifest-cache budget.
/// Runs until aborted at shutdown; both operations are idempotent and
/// crash-safe, so multiple pods sweeping concurrently is fine.
pub async fn run_sweeper(pool: PgPool, config: PlanningConfig) {
    if !config.enabled {
        return;
    }
    let interval = Duration::from_secs(config.sweep_interval_secs.max(1));
    let workspace = meridian_store::tenancy::default_workspace_id();
    loop {
        tokio::time::sleep(interval).await;
        match store::sweep_expired(&pool, workspace).await {
            Ok(0) => {}
            Ok(count) => tracing::info!(count, "expired scan plans swept"),
            Err(error) => tracing::warn!(%error, "scan-plan expiry sweep failed"),
        }
        if config.pg_cache_max_bytes > 0 {
            match store::manifest_cache_evict(
                &pool,
                i64::try_from(config.pg_cache_max_bytes).unwrap_or(i64::MAX),
            )
            .await
            {
                Ok(0) => {}
                Ok(count) => tracing::info!(count, "manifest cache rows evicted"),
                Err(error) => tracing::warn!(%error, "manifest cache eviction failed"),
            }
        }
    }
}
