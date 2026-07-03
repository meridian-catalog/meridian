//! The inbound-mirror **sync engine** (spec Pillar B, B-F1).
//!
//! Given one mirror (from `catalog_mirrors`), a sync run:
//!
//! 1. builds a read-only [`IrcClient`] from the mirror's endpoint + config,
//! 2. confirms reachability (`GET /v1/config`),
//! 3. enumerates the source's namespaces (recursively) and tables,
//! 4. loads each table's metadata and **materializes it as a foreign
//!    (read-only) asset** in the native `namespaces` / `tables` tables (so
//!    search/health/lineage work on it), skipping tables whose
//!    `metadata_location` is unchanged since the last sync (incremental),
//! 5. **removes** foreign assets that vanished from the source,
//! 6. refreshes the sprawl index (`mirror_assets`) and records the run +
//!    counts on the mirror.
//!
//! Only IRC sources are supported today. A `glue` mirror is rejected with a
//! clear error (documented as a future source type in ADR 008); the engine's
//! shape — walk, load, upsert-foreign — is source-agnostic, so a Glue client
//! slots in behind the same steps later.

use std::collections::BTreeMap;
use std::time::Duration;

use meridian_common::id::WorkspaceId;
use meridian_iceberg::spec::TableMetadata;
use meridian_store::commit::SnapshotIndexRow;
use meridian_store::federation::MirrorRecord;
use meridian_store::foreign::{self, ForeignTableInput, MirrorAssetInput};
use meridian_store::search::schema_search_text;
use serde_json::json;
use sqlx::PgPool;

use crate::client::{IrcClient, IrcClientError, LoadedTable, MirrorAuth};

/// The IRC source kind the sibling's `catalog_mirrors.kind` uses.
const KIND_IRC: &str = "iceberg-rest";

/// Bound on namespace-tree recursion depth (a mirror with a deeper namespace
/// hierarchy than this is exceptional; the cap stops a looping source).
const MAX_NAMESPACE_DEPTH: usize = 32;

/// What a sync run observed and changed, for logging and the run summary.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct SyncStats {
    /// Namespaces seen on the source.
    pub namespaces_seen: usize,
    /// Tables seen on the source.
    pub tables_seen: usize,
    /// Foreign tables newly inserted this run.
    pub tables_inserted: usize,
    /// Foreign tables whose metadata changed and were re-indexed.
    pub tables_updated: usize,
    /// Foreign tables skipped because their metadata was unchanged.
    pub tables_unchanged: usize,
    /// Foreign tables removed because they vanished from the source.
    pub tables_removed: usize,
    /// Tables the source listed but that could not be loaded/parsed (skipped,
    /// not fatal — one bad table must not fail the whole mirror).
    pub tables_failed: usize,
}

impl SyncStats {
    /// A one-line human summary for the sync-run `detail` field.
    #[must_use]
    pub fn summary(&self) -> String {
        format!(
            "namespaces={} tables={} (+{} ~{} ={} -{} !{})",
            self.namespaces_seen,
            self.tables_seen,
            self.tables_inserted,
            self.tables_updated,
            self.tables_unchanged,
            self.tables_removed,
            self.tables_failed,
        )
    }
}

/// A failure that aborts a whole sync run (as opposed to a single-table skip,
/// which is counted in [`SyncStats::tables_failed`]).
#[derive(Debug, thiserror::Error)]
pub enum SyncEngineError {
    /// The mirror's source kind is not implemented by this engine.
    #[error("unsupported mirror kind {0:?}: only {KIND_IRC:?} inbound sync is implemented")]
    UnsupportedKind(String),
    /// The mirror config was missing or malformed (e.g. OAuth2 without a token
    /// URL).
    #[error("invalid mirror configuration: {0}")]
    Config(String),
    /// Talking to the source catalog failed.
    #[error(transparent)]
    Client(#[from] IrcClientError),
    /// A store-layer operation failed.
    #[error("store error: {0}")]
    Store(String),
}

impl From<meridian_common::MeridianError> for SyncEngineError {
    fn from(error: meridian_common::MeridianError) -> Self {
        Self::Store(error.to_string())
    }
}

/// Runs one full sync of `mirror` and returns what it observed/changed.
///
/// This is the engine the worker loop and the manual "sync now" trigger both
/// call. It does **not** record the run outcome or flip the mirror status — the
/// caller wraps it with `record_sync_result` so a run is recorded whether this
/// succeeds or fails (the worker owns that policy).
pub async fn sync_mirror(
    pool: &PgPool,
    workspace_id: WorkspaceId,
    mirror: &MirrorRecord,
    request_timeout: Duration,
) -> Result<SyncStats, SyncEngineError> {
    if mirror.kind != KIND_IRC {
        return Err(SyncEngineError::UnsupportedKind(mirror.kind.clone()));
    }
    let principal = format!("federation:sync:{}", mirror.name);
    let client = build_client(mirror, request_timeout)?;

    // Reachability + protocol check up front, so an unreachable/misconfigured
    // source fails cleanly before we touch any local state.
    client.get_config().await?;

    // The dedicated foreign warehouse that holds this mirror's assets.
    let warehouse_id =
        foreign::ensure_foreign_warehouse(pool, workspace_id, &mirror.id, &mirror.name, &principal)
            .await?;

    let mut stats = SyncStats::default();

    // Walk the source's namespaces (recursively) and their tables.
    let namespaces = client.list_all_namespaces(MAX_NAMESPACE_DEPTH).await?;
    stats.namespaces_seen = namespaces.len();

    // Track which (namespace, table) pairs the source currently has, so we can
    // remove local foreign assets that disappeared.
    let mut seen: std::collections::BTreeSet<(Vec<String>, String)> =
        std::collections::BTreeSet::new();
    // Sprawl-index rows to write for this mirror (replaced wholesale below).
    let mut assets: Vec<MirrorAssetInput> = Vec::new();

    for levels in &namespaces {
        // Every source namespace becomes a foreign namespace (even if empty),
        // so the mirror's namespace structure is visible.
        let namespace_id = foreign::upsert_foreign_namespace(
            pool,
            workspace_id,
            &warehouse_id,
            &mirror.id,
            levels,
            &principal,
        )
        .await?;

        let tables = client.list_tables(levels).await?;
        for remote in tables {
            stats.tables_seen += 1;
            let table_name = remote.name.clone();
            seen.insert((levels.clone(), table_name.clone()));

            match sync_one_table(
                pool,
                workspace_id,
                mirror,
                &principal,
                &namespace_id,
                levels,
                &table_name,
                &client,
            )
            .await
            {
                Ok((outcome, asset)) => {
                    match outcome {
                        foreign::UpsertOutcome::Inserted => stats.tables_inserted += 1,
                        foreign::UpsertOutcome::Updated => stats.tables_updated += 1,
                        foreign::UpsertOutcome::Unchanged => stats.tables_unchanged += 1,
                    }
                    assets.push(asset);
                }
                Err(error) => {
                    // One unreadable table does not fail the mirror: log, count,
                    // and move on (spec: freshness per mirror, best-effort).
                    tracing::warn!(
                        mirror = %mirror.name,
                        namespace = ?levels,
                        table = %table_name,
                        %error,
                        "skipping table that could not be synced"
                    );
                    stats.tables_failed += 1;
                }
            }
        }
    }

    // Remove foreign tables the source no longer reports.
    let existing = foreign::list_foreign_table_idents(pool, &mirror.id).await?;
    for ident in existing {
        let key = (ident.namespace_levels.clone(), ident.name.clone());
        if !seen.contains(&key) {
            let removed = foreign::remove_foreign_table(
                pool,
                workspace_id,
                &mirror.id,
                &ident.namespace_levels,
                &ident.name,
                &principal,
            )
            .await?;
            if removed {
                stats.tables_removed += 1;
            }
        }
    }
    // Drop foreign namespaces left empty by removals so the mirror mirrors the
    // source's structure (and does not accumulate empty shells).
    let pruned = foreign::prune_empty_foreign_namespaces(pool, &mirror.id).await?;
    if pruned > 0 {
        tracing::debug!(mirror = %mirror.name, pruned, "pruned empty foreign namespaces");
    }

    // Refresh the sprawl index (mirror_assets) wholesale from what we saw.
    foreign::replace_mirror_assets(pool, workspace_id, &mirror.id, &assets).await?;

    tracing::info!(mirror = %mirror.name, summary = %stats.summary(), "mirror sync complete");
    Ok(stats)
}

/// Syncs a single table: load it, parse its metadata, upsert it as a foreign
/// asset, and produce its sprawl-index row. Returns the upsert outcome and the
/// asset row (for the sprawl index).
#[allow(clippy::too_many_arguments)]
async fn sync_one_table(
    pool: &PgPool,
    workspace_id: WorkspaceId,
    mirror: &MirrorRecord,
    principal: &str,
    namespace_id: &str,
    levels: &[String],
    name: &str,
    client: &IrcClient,
) -> Result<(foreign::UpsertOutcome, MirrorAssetInput), SyncEngineError> {
    let loaded = client.load_table(levels, name).await?;
    let metadata = parse_metadata(&loaded)?;

    // The write-through index fields (mirrors the server's table route).
    let format_version = i16::from(metadata.format_version);
    let properties = metadata.properties.clone().unwrap_or_default();
    let schema_text = metadata.current_schema().map(schema_search_text);
    let snapshots = snapshot_index_rows(&metadata);
    let table_uuid = metadata.table_uuid.to_string();

    // The metadata_location is the incremental key. If the source did not
    // return one, fall back to the table's own metadata `location` so the row
    // still has a stable pointer (and re-syncs update on any metadata change of
    // that shape). A table with neither is still indexable by identity.
    let metadata_location = loaded
        .metadata_location
        .clone()
        .unwrap_or_else(|| metadata.location.clone());

    let input = ForeignTableInput {
        namespace_id,
        namespace_levels: levels,
        name,
        table_uuid: &table_uuid,
        metadata_location: &metadata_location,
        format_version,
        properties: &properties,
        schema_text: schema_text.as_deref(),
        snapshots: &snapshots,
    };
    let outcome =
        foreign::upsert_foreign_table(pool, workspace_id, &mirror.id, &input, principal).await?;

    let remote_ident = if levels.is_empty() {
        name.to_owned()
    } else {
        format!("{}.{name}", levels.join("."))
    };
    let asset = MirrorAssetInput {
        remote_ident,
        asset_type: "table".to_owned(),
        // The dataset's own storage location is the join key for sprawl
        // duplicate detection (the same physical table registered twice).
        storage_location: Some(metadata.location.clone()),
        owner: properties.get("owner").cloned(),
        properties: json!({
            "format_version": metadata.format_version,
            "metadata_location": metadata_location,
        }),
    };
    Ok((outcome, asset))
}

/// Parses a `loadTable` response's raw `metadata` object into a
/// [`TableMetadata`], routing through [`TableMetadata::from_json`] so v1
/// legacy-field normalization is applied (v2/v3 pass through).
fn parse_metadata(loaded: &LoadedTable) -> Result<TableMetadata, SyncEngineError> {
    let raw = serde_json::to_string(&loaded.metadata)
        .map_err(|e| SyncEngineError::Client(IrcClientError::Malformed(e.to_string())))?;
    TableMetadata::from_json(&raw)
        .map_err(|e| SyncEngineError::Client(IrcClientError::Malformed(e.to_string())))
}

/// Builds the write-through snapshot-index rows from metadata (mirrors the
/// server's `derived_state`): every snapshot, flagged with which is current.
fn snapshot_index_rows(metadata: &TableMetadata) -> Vec<SnapshotIndexRow> {
    let current = metadata.current_snapshot_id.filter(|id| *id >= 0);
    metadata
        .snapshots
        .iter()
        .flatten()
        .map(|snapshot| SnapshotIndexRow {
            snapshot_id: snapshot.snapshot_id,
            parent_snapshot_id: snapshot.parent_snapshot_id,
            sequence_number: snapshot.sequence_number,
            timestamp_ms: snapshot.timestamp_ms,
            manifest_list: snapshot.manifest_list.clone(),
            operation: snapshot
                .summary
                .as_ref()
                .and_then(|s| s.get("operation").cloned()),
            summary: snapshot
                .summary
                .clone()
                .map_or_else(|| json!({}), |s| json!(s)),
            is_current: current == Some(snapshot.snapshot_id),
        })
        .collect()
}

/// Builds the IRC client from a mirror record: endpoint as the REST base, the
/// remote catalog (or a `warehouse` config key) as the prefix, and the auth
/// mode resolved from config keys.
///
/// Config-key convention (non-secret keys plus, for dev, the token/secret —
/// production secret handling lands with the shared credential store; see the
/// `catalog_mirrors` migration header):
///   * `auth-mode`      = `none` | `bearer` | `oauth2` (default `none`)
///   * `token`          = static bearer token (for `bearer`)
///   * `token-url`, `client-id`, `client-secret`, `scope` (for `oauth2`)
///   * `warehouse`      = the source `{prefix}` when `remote_catalog` is unset
fn build_client(
    mirror: &MirrorRecord,
    request_timeout: Duration,
) -> Result<IrcClient, SyncEngineError> {
    let config: &BTreeMap<String, String> = &mirror.config.0;
    let prefix = mirror
        .remote_catalog
        .clone()
        .or_else(|| config.get("warehouse").cloned())
        .unwrap_or_default();
    let auth = resolve_auth(config)?;
    IrcClient::new(&mirror.endpoint, &prefix, auth, request_timeout)
        .map_err(SyncEngineError::Client)
}

/// Resolves [`MirrorAuth`] from the mirror's config map.
fn resolve_auth(config: &BTreeMap<String, String>) -> Result<MirrorAuth, SyncEngineError> {
    match config.get("auth-mode").map(String::as_str) {
        None | Some("none") => Ok(MirrorAuth::None),
        Some("bearer") => {
            let token = config
                .get("token")
                .cloned()
                .ok_or_else(|| SyncEngineError::Config("bearer auth requires a 'token'".into()))?;
            Ok(MirrorAuth::Bearer(token))
        }
        Some("oauth2") => {
            let token_url = config.get("token-url").cloned().ok_or_else(|| {
                SyncEngineError::Config("oauth2 auth requires a 'token-url'".into())
            })?;
            let client_id = config.get("client-id").cloned().ok_or_else(|| {
                SyncEngineError::Config("oauth2 auth requires a 'client-id'".into())
            })?;
            let client_secret = config.get("client-secret").cloned().ok_or_else(|| {
                SyncEngineError::Config("oauth2 auth requires a 'client-secret'".into())
            })?;
            Ok(MirrorAuth::OAuth2 {
                token_url,
                client_id,
                client_secret,
                scope: config.get("scope").cloned(),
            })
        }
        Some(other) => Err(SyncEngineError::Config(format!(
            "unknown auth-mode {other:?} (expected none | bearer | oauth2)"
        ))),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn unsupported_auth_mode_is_config_error() {
        let mut config = BTreeMap::new();
        config.insert("auth-mode".to_owned(), "kerberos".to_owned());
        assert!(matches!(
            resolve_auth(&config),
            Err(SyncEngineError::Config(_))
        ));
    }

    #[test]
    fn bearer_requires_token() {
        let mut config = BTreeMap::new();
        config.insert("auth-mode".to_owned(), "bearer".to_owned());
        assert!(matches!(
            resolve_auth(&config),
            Err(SyncEngineError::Config(_))
        ));
        config.insert("token".to_owned(), "t".to_owned());
        assert!(matches!(resolve_auth(&config), Ok(MirrorAuth::Bearer(_))));
    }

    #[test]
    fn none_is_default() {
        let config = BTreeMap::new();
        assert!(matches!(resolve_auth(&config), Ok(MirrorAuth::None)));
    }

    #[test]
    fn stats_summary_is_stable() {
        let stats = SyncStats {
            namespaces_seen: 2,
            tables_seen: 5,
            tables_inserted: 3,
            tables_updated: 1,
            tables_unchanged: 1,
            tables_removed: 0,
            tables_failed: 0,
        };
        assert_eq!(stats.summary(), "namespaces=2 tables=5 (+3 ~1 =1 -0 !0)");
    }
}
