-- 0014_federation_mirrors: catalog mirror configs + sync-run history for
-- Pillar B federation (zero-copy register / IRC-to-IRC and Glue mirroring).
--
-- A *mirror* is a registered pointer to an EXTERNAL catalog (another IRC
-- endpoint such as Polaris/Lakekeeper, or an AWS Glue Data Catalog) whose
-- assets Meridian tracks — for sprawl visibility and zero-copy register —
-- without owning the underlying storage. This is the control-plane record of
-- "a catalog Meridian knows about but does not manage"; the actual sync
-- engine (fetching the remote catalog's tables and recording their storage
-- locations) lives in the federation crate/worker built alongside this.
--
-- Append-only: this file only adds to the 0001-0013 schema. Mirror mutations
-- commit through the normal audited path (audit row + outbox event on the
-- same transaction), exactly like warehouses.
--
-- INTEGRATION NOTE (federation crate): the sync worker is expected to
--   1. read enabled mirrors from `catalog_mirrors`,
--   2. fetch the remote catalog's tables,
--   3. upsert one row per discovered asset into `mirror_assets`
--      (storage_location is the key input to sprawl duplicate detection),
--   4. record the outcome in `mirror_sync_runs` and stamp
--      `catalog_mirrors.last_synced_at` / `last_sync_status`.
-- The API/CLI/console in this change read these tables; the worker writes
-- steps 2-4. Until the worker lands, mirrors can be created and listed and
-- the sprawl summary reports them as "never synced".

-- One row per registered external catalog, scoped to a workspace. `name` is
-- unique per workspace and used as the stable handle in the API/CLI.
CREATE TABLE catalog_mirrors (
    id               TEXT PRIMARY KEY,
    workspace_id     TEXT NOT NULL REFERENCES workspaces (id) ON DELETE RESTRICT,
    -- Operator-facing handle, unique per workspace.
    name             TEXT NOT NULL,
    -- Source kind: how to talk to the remote catalog. Kept as TEXT (not an
    -- enum) so the federation crate can add kinds without a migration; the
    -- API validates the accepted set.
    kind             TEXT NOT NULL CHECK (kind IN ('iceberg-rest', 'glue')),
    -- Connection endpoint. For 'iceberg-rest' this is the IRC base URI
    -- (e.g. https://polaris.example/api/catalog); for 'glue' it is the AWS
    -- region (the Glue endpoint is derived from it).
    endpoint         TEXT NOT NULL,
    -- The remote catalog identifier within the endpoint, when the endpoint
    -- hosts more than one: the IRC `{prefix}`/warehouse for iceberg-rest, or
    -- the Glue catalog id (AWS account) for glue. NULL = the endpoint's
    -- default catalog.
    remote_catalog   TEXT,
    -- Non-secret connection options (region, warehouse, auth-mode, ...).
    -- Secret material (tokens, keys) is NOT stored here in this milestone;
    -- when vending/vault integration lands the worker resolves secrets by
    -- reference. Kept aligned with the warehouse storage_config convention.
    config           JSONB NOT NULL DEFAULT '{}'::jsonb,
    -- Whether the sync worker should pull this mirror on its schedule.
    enabled          BOOLEAN NOT NULL DEFAULT true,
    -- Desired sync cadence in seconds (advisory; the worker enforces it).
    sync_interval_s  INTEGER NOT NULL DEFAULT 3600 CHECK (sync_interval_s > 0),
    -- Set by the worker after each run. NULL = never synced.
    last_synced_at   TIMESTAMPTZ,
    -- Outcome of the most recent run: 'ok' | 'error' | 'running' | NULL.
    last_sync_status TEXT CHECK (last_sync_status IN ('ok', 'error', 'running')),
    -- Human-readable detail for the most recent run (error message or a
    -- short summary). NULL when never run.
    last_sync_detail TEXT,
    -- Count of assets discovered on the most recent successful sync. This is
    -- a denormalized convenience for the sprawl summary; the authoritative
    -- per-asset detail lives in `mirror_assets`.
    asset_count      BIGINT NOT NULL DEFAULT 0,
    created_at       TIMESTAMPTZ NOT NULL DEFAULT now(),
    updated_at       TIMESTAMPTZ NOT NULL DEFAULT now(),
    UNIQUE (workspace_id, name)
);

CREATE INDEX catalog_mirrors_workspace_idx ON catalog_mirrors (workspace_id);

-- One row per asset (table/view) discovered in a mirrored catalog. Written by
-- the federation sync worker; read by the sprawl summary for asset counts,
-- ownership gaps, and — via storage_location — cross-source duplicate
-- detection. A mirror's assets are replaced wholesale on each successful sync
-- (delete-then-insert within the sync transaction), so this table always
-- reflects the last successful observation.
CREATE TABLE mirror_assets (
    id                TEXT PRIMARY KEY,
    mirror_id         TEXT NOT NULL REFERENCES catalog_mirrors (id) ON DELETE CASCADE,
    workspace_id      TEXT NOT NULL REFERENCES workspaces (id) ON DELETE RESTRICT,
    -- Fully-qualified remote identity, e.g. "db.schema.table" as the remote
    -- catalog reports it. Unique per mirror.
    remote_ident      TEXT NOT NULL,
    -- 'table' | 'view'. TEXT for forward-compatibility.
    asset_type        TEXT NOT NULL DEFAULT 'table',
    -- The Iceberg metadata.json / storage location the remote asset points
    -- at. This is the join key for sprawl duplicate detection: the same
    -- storage_location registered in >1 place (across mirrors and Meridian's
    -- own warehouses) is a zero-copy duplicate. NULL when the remote catalog
    -- did not expose a location.
    storage_location  TEXT,
    -- Remote-reported owner/principal, when available. Absence feeds the
    -- sprawl "ownership gaps" metric.
    owner             TEXT,
    -- Free-form remote-reported detail (row counts, last-modified, ...).
    properties        JSONB NOT NULL DEFAULT '{}'::jsonb,
    observed_at       TIMESTAMPTZ NOT NULL DEFAULT now(),
    UNIQUE (mirror_id, remote_ident)
);

CREATE INDEX mirror_assets_workspace_idx ON mirror_assets (workspace_id);
CREATE INDEX mirror_assets_mirror_idx ON mirror_assets (mirror_id);
-- Duplicate detection scans by storage_location; index the non-null ones.
CREATE INDEX mirror_assets_location_idx
    ON mirror_assets (storage_location)
    WHERE storage_location IS NOT NULL;

-- Append-only history of sync runs, newest-first per mirror. The worker
-- inserts one row per run; the API exposes recent runs as the mirror's sync
-- history. `catalog_mirrors.last_*` mirrors the newest row here for cheap
-- listing.
CREATE TABLE mirror_sync_runs (
    id               TEXT PRIMARY KEY,
    mirror_id        TEXT NOT NULL REFERENCES catalog_mirrors (id) ON DELETE CASCADE,
    workspace_id     TEXT NOT NULL REFERENCES workspaces (id) ON DELETE RESTRICT,
    -- 'ok' | 'error' | 'running'.
    status           TEXT NOT NULL CHECK (status IN ('ok', 'error', 'running')),
    -- Assets discovered on this run (for 'ok' runs).
    assets_seen      BIGINT NOT NULL DEFAULT 0,
    -- Error message or short summary.
    detail           TEXT,
    started_at       TIMESTAMPTZ NOT NULL DEFAULT now(),
    finished_at      TIMESTAMPTZ
);

CREATE INDEX mirror_sync_runs_mirror_idx
    ON mirror_sync_runs (mirror_id, started_at DESC);
