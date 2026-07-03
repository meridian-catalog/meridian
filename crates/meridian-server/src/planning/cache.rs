//! The manifest cache tiers.
//!
//! Iceberg manifest lists and manifests are immutable at a given storage
//! path, so caching needs no invalidation — only bounded space. Reads go
//! through three tiers:
//!
//! 1. **In-process LRU** of *parsed* manifests (`Arc`-shared, weighted by
//!    an estimated parsed size, exact least-recently-used eviction under
//!    a byte budget). A hit costs a map lookup.
//! 2. **Postgres byte cache** (`manifest_cache`, migration 0011) of raw
//!    file bytes, shared across pods; a hit skips the object-storage
//!    round trip but still parses. Budget-enforced by the planning sweep.
//! 3. **Object storage** — the source of truth; bytes read here are
//!    written through to tier 2 (when enabled) and the parsed form to
//!    tier 1.
//!
//! Hit/miss counters are process-wide and reported in every plan summary
//! and completion log line, so cache effectiveness is observable without
//! a metrics stack.

use std::collections::{BTreeMap, HashMap};
use std::sync::Mutex;
use std::sync::atomic::{AtomicU64, Ordering};

use meridian_iceberg::manifest::{
    DataFile, Manifest, ManifestList, read_manifest, read_manifest_list,
};

/// A parsed, immutable, shareable manifest artifact.
#[derive(Debug, Clone)]
pub enum CachedManifest {
    /// A manifest list.
    List(std::sync::Arc<ManifestList>),
    /// A manifest.
    File(std::sync::Arc<Manifest>),
}

impl CachedManifest {
    /// Parses raw file bytes as a manifest list.
    pub fn parse_list(bytes: &[u8]) -> Result<Self, String> {
        read_manifest_list(bytes)
            .map(|l| Self::List(std::sync::Arc::new(l)))
            .map_err(|e| e.to_string())
    }

    /// Parses raw file bytes as a manifest.
    pub fn parse_manifest(bytes: &[u8]) -> Result<Self, String> {
        read_manifest(bytes)
            .map(|m| Self::File(std::sync::Arc::new(m)))
            .map_err(|e| e.to_string())
    }

    /// Estimated resident size in bytes of the parsed form. Deliberately
    /// coarse (strings, maps, and per-entry struct overhead); the LRU
    /// budget is a control knob, not an accounting ledger.
    #[must_use]
    pub fn estimated_bytes(&self) -> usize {
        match self {
            Self::List(list) => {
                let mut total = 256;
                for m in &list.manifests {
                    total += 200 + m.manifest_path.len();
                    for s in m.partitions.iter().flatten() {
                        total += 64
                            + s.lower_bound.as_ref().map_or(0, Vec::len)
                            + s.upper_bound.as_ref().map_or(0, Vec::len);
                    }
                }
                total
            }
            Self::File(manifest) => {
                let mut total = 512 + manifest.metadata.schema_json.len();
                for entry in &manifest.entries {
                    total += 260 + data_file_bytes(&entry.data_file);
                }
                total
            }
        }
    }
}

fn data_file_bytes(file: &DataFile) -> usize {
    fn count_map(map: Option<&BTreeMap<i32, i64>>) -> usize {
        map.map_or(0, |m| 48 + m.len() * 40)
    }
    fn bytes_map(map: Option<&BTreeMap<i32, Vec<u8>>>) -> usize {
        map.map_or(0, |m| 48 + m.values().map(|v| 56 + v.len()).sum::<usize>())
    }
    file.file_path.len()
        + file.file_format.len()
        + file.partition.fields.len() * 64
        + count_map(file.column_sizes.as_ref())
        + count_map(file.value_counts.as_ref())
        + count_map(file.null_value_counts.as_ref())
        + count_map(file.nan_value_counts.as_ref())
        + bytes_map(file.lower_bounds.as_ref())
        + bytes_map(file.upper_bounds.as_ref())
        + file.key_metadata.as_ref().map_or(0, Vec::len)
        + file.split_offsets.as_ref().map_or(0, |v| v.len() * 8)
        + file.equality_ids.as_ref().map_or(0, |v| v.len() * 4)
        + file.referenced_data_file.as_ref().map_or(0, String::len)
}

struct LruEntry {
    value: CachedManifest,
    weight: usize,
    tick: u64,
}

struct LruInner {
    entries: HashMap<String, LruEntry>,
    /// tick -> key; the smallest tick is the least recently used entry.
    order: BTreeMap<u64, String>,
    next_tick: u64,
    total_weight: usize,
}

/// An exact, byte-budgeted LRU over parsed manifests. Keys are storage
/// locations; values are immutable, so `get` never revalidates.
pub struct ManifestLru {
    budget: usize,
    inner: Mutex<LruInner>,
}

impl std::fmt::Debug for ManifestLru {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        let inner = self.lock();
        f.debug_struct("ManifestLru")
            .field("budget", &self.budget)
            .field("entries", &inner.entries.len())
            .field("total_weight", &inner.total_weight)
            .finish_non_exhaustive()
    }
}

impl ManifestLru {
    /// Creates a cache with the given weight budget in (estimated) bytes.
    #[must_use]
    pub fn new(budget: usize) -> Self {
        Self {
            budget,
            inner: Mutex::new(LruInner {
                entries: HashMap::new(),
                order: BTreeMap::new(),
                next_tick: 0,
                total_weight: 0,
            }),
        }
    }

    /// Looks up a location, marking it most recently used.
    pub fn get(&self, location: &str) -> Option<CachedManifest> {
        let mut inner = self.lock();
        let tick = inner.next_tick;
        inner.next_tick += 1;
        let entry = inner.entries.get_mut(location)?;
        let old_tick = std::mem::replace(&mut entry.tick, tick);
        let value = entry.value.clone();
        inner.order.remove(&old_tick);
        inner.order.insert(tick, location.to_owned());
        Some(value)
    }

    /// Inserts a parsed manifest, evicting least-recently-used entries
    /// until the budget holds. A value heavier than the whole budget is
    /// simply not cached.
    pub fn put(&self, location: &str, value: CachedManifest) {
        let weight = value.estimated_bytes();
        if weight > self.budget {
            return;
        }
        let mut inner = self.lock();
        if let Some(existing) = inner.entries.remove(location) {
            inner.order.remove(&existing.tick);
            inner.total_weight -= existing.weight;
        }
        let tick = inner.next_tick;
        inner.next_tick += 1;
        inner.total_weight += weight;
        inner.entries.insert(
            location.to_owned(),
            LruEntry {
                value,
                weight,
                tick,
            },
        );
        inner.order.insert(tick, location.to_owned());
        while inner.total_weight > self.budget {
            let Some((&oldest_tick, _)) = inner.order.iter().next() else {
                break;
            };
            let Some(key) = inner.order.remove(&oldest_tick) else {
                break;
            };
            if let Some(evicted) = inner.entries.remove(&key) {
                inner.total_weight -= evicted.weight;
            }
        }
    }

    /// Current entry count (for tests and logs).
    #[must_use]
    pub fn len(&self) -> usize {
        self.lock().entries.len()
    }

    /// Whether the cache is empty.
    #[must_use]
    pub fn is_empty(&self) -> bool {
        self.len() == 0
    }

    fn lock(&self) -> std::sync::MutexGuard<'_, LruInner> {
        // A poisoned lock means another thread panicked mid-operation; the
        // cache state is still structurally sound (every mutation keeps the
        // maps consistent between statements), and a cache must not take
        // the process down.
        self.inner
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
    }
}

/// Process-wide cache tier counters.
#[derive(Debug, Default)]
pub struct CacheCounters {
    /// Tier-1 (in-process LRU) hits.
    pub lru_hits: AtomicU64,
    /// Tier-2 (Postgres byte cache) hits.
    pub pg_hits: AtomicU64,
    /// Tier-3 reads from object storage (cache misses all the way down).
    pub storage_reads: AtomicU64,
}

impl CacheCounters {
    pub(crate) fn record_lru_hit(&self) {
        self.lru_hits.fetch_add(1, Ordering::Relaxed);
    }

    pub(crate) fn record_pg_hit(&self) {
        self.pg_hits.fetch_add(1, Ordering::Relaxed);
    }

    pub(crate) fn record_storage_read(&self) {
        self.storage_reads.fetch_add(1, Ordering::Relaxed);
    }

    /// A snapshot of the counters as JSON (for plan summaries and logs).
    #[must_use]
    pub fn snapshot(&self) -> serde_json::Value {
        serde_json::json!({
            "lru_hits": self.lru_hits.load(Ordering::Relaxed),
            "pg_hits": self.pg_hits.load(Ordering::Relaxed),
            "storage_reads": self.storage_reads.load(Ordering::Relaxed),
        })
    }
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use super::*;

    fn list_with_paths(paths: &[&str]) -> CachedManifest {
        CachedManifest::List(Arc::new(ManifestList {
            format_version: Some(2),
            snapshot_id: Some(1),
            parent_snapshot_id: None,
            sequence_number: Some(1),
            manifests: paths
                .iter()
                .map(|p| meridian_iceberg::manifest::ManifestFile {
                    manifest_path: (*p).to_owned(),
                    manifest_length: 100,
                    partition_spec_id: 0,
                    content: meridian_iceberg::manifest::ManifestContentType::Data,
                    sequence_number: 1,
                    min_sequence_number: 1,
                    added_snapshot_id: 1,
                    added_files_count: Some(1),
                    existing_files_count: Some(0),
                    deleted_files_count: Some(0),
                    added_rows_count: Some(1),
                    existing_rows_count: Some(0),
                    deleted_rows_count: Some(0),
                    partitions: None,
                    key_metadata: None,
                    first_row_id: None,
                })
                .collect(),
        }))
    }

    #[test]
    fn lru_evicts_least_recently_used_under_budget() {
        let one_entry = list_with_paths(&["m"]).estimated_bytes();
        // Room for two single-entry lists, not three.
        let cache = ManifestLru::new(one_entry * 2 + one_entry / 2);
        cache.put("a", list_with_paths(&["m"]));
        cache.put("b", list_with_paths(&["m"]));
        // Touch "a" so "b" is the eviction candidate.
        assert!(cache.get("a").is_some());
        cache.put("c", list_with_paths(&["m"]));
        assert!(cache.get("a").is_some(), "recently used entry survives");
        assert!(cache.get("b").is_none(), "least recently used is evicted");
        assert!(cache.get("c").is_some());
        assert_eq!(cache.len(), 2);
    }

    #[test]
    fn lru_replaces_same_key_without_double_counting() {
        let one_entry = list_with_paths(&["m"]).estimated_bytes();
        let cache = ManifestLru::new(one_entry * 2 + one_entry / 2);
        for _ in 0..10 {
            cache.put("a", list_with_paths(&["m"]));
        }
        assert_eq!(cache.len(), 1);
        cache.put("b", list_with_paths(&["m"]));
        assert_eq!(cache.len(), 2, "replacement must not inflate the weight");
    }

    #[test]
    fn oversized_values_are_not_cached() {
        let cache = ManifestLru::new(8);
        cache.put("a", list_with_paths(&["m"]));
        assert!(cache.get("a").is_none());
        assert!(cache.is_empty());
    }

    #[test]
    fn cached_values_are_shared_not_copied() {
        let cache = ManifestLru::new(1 << 20);
        cache.put("a", list_with_paths(&["m1", "m2"]));
        let (Some(CachedManifest::List(first)), Some(CachedManifest::List(second))) =
            (cache.get("a"), cache.get("a"))
        else {
            panic!("expected two list hits");
        };
        assert!(
            Arc::ptr_eq(&first, &second),
            "hits share one immutable parse"
        );
    }
}
