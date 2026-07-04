-- 0023_ai_assets: AI Asset Governance (Pillar I, I-F1..I-F4).
--
-- Extends the object model beyond tables so the catalog governs the AI supply
-- chain: generic assets (filesets, models, vector datasets), immutable
-- training-run provenance binding a model version to exact table snapshots,
-- and the GDPR "right to be forgotten" deletion-campaign evidence record.
--
-- Append-only: this file only adds to the 0001-0022 schema; nothing is
-- rewritten. The one exception is admitting 'asset' into the grants
-- securable-type CHECK, exactly as 0007 admitted 'view' — a first-class
-- securable joins the existing grant machinery rather than growing a parallel
-- one.
--
-- What this migration owns (definitions + provenance records); what it does
-- NOT own:
--   * It does NOT build a physical snapshot-expiry job. F-I4 records which
--     models saw data in a to-be-deleted snapshot and produces the evidence;
--     the physical expiry is the existing maintenance ExpireSnapshots job
--     (migration 0012) — the integration point is documented, not duplicated.
--   * It does NOT re-index generic assets into the namespace-scoped table/view
--     full-text search (migration 0010). Generic assets are workspace-scoped
--     with their own grant-based visibility, so they carry their own
--     `search_tsv` + GIN index and are queried by meridian_store::assets,
--     keeping the shared search UNION untouched.
--
-- Model:
--
--  * assets: one extensible row per generic asset, of one `kind`
--    (fileset | model | vector_dataset). Workspace-scoped, named uniquely per
--    (workspace, kind, name). Kind-specific fields live in `metadata` jsonb
--    owned by the Rust layer (meridian_store::assets). A fileset additionally
--    carries `storage_prefix` (an s3://bucket/prefix) so credentials can be
--    vended for it, scoped to that prefix, reusing meridian-vending exactly as
--    tables do. `warehouse_id` (nullable) ties a fileset to the warehouse
--    whose storage config drives the vend.
--  * training_runs: an IMMUTABLE provenance record binding a model + version to
--    the exact table snapshots that trained it. Append-only: no UPDATE, no
--    DELETE path in the Rust layer; a run is written once and never mutated
--    (I-F2). `model_asset_id` is nullable — a run may name a model that is not
--    (yet) a registered asset; the (model, model_version) pair is always
--    recorded literally so the record stands alone.
--  * training_run_inputs: one row per (table, snapshot_id) input of a run. The
--    snapshot_id is recorded EXACTLY as pinned (an Iceberg snapshot id is a
--    signed 64-bit integer); Iceberg time-travel against that id makes the
--    input reproducible. `table_id` is nullable so a run can pin an input by a
--    stable external dataset name when the table is not a native Meridian
--    table; `table_ref` always holds a human-readable identifier.
--  * deletion_campaigns: a GDPR "right to be forgotten" campaign — a named
--    request to erase a subject/reason across the lakehouse. It moves through
--    open -> evidence_ready -> closed. It does not itself delete data; it
--    tracks what must expire and records the evidence.
--  * deletion_campaign_snapshots: the affected (table, snapshot_id[, branch])
--    rows a campaign targets for physical expiry, each carrying an
--    `expiry_status` (pending | expired) updated when the maintenance
--    ExpireSnapshots job confirms the snapshot is gone.
--  * deletion_campaign_model_exposure: the evidence rows — which model
--    versions saw a to-be-deleted snapshot (derived from training_run_inputs),
--    so a campaign can answer "which models were trained on data we are now
--    erasing" — the "right to be forgotten" evidence almost nobody can produce.
--
-- Polymorphic ids (asset_id on grants, model_asset_id, table_id) follow the
-- established convention: real FKs where the target is a table in this schema,
-- TEXT-without-FK where the reference ranges over history that may be dropped.

-- ----------------------------------------------------------------------------
-- assets: the generic-asset row (fileset | model | vector_dataset).

CREATE TABLE assets (
    id             TEXT PRIMARY KEY,
    workspace_id   TEXT NOT NULL REFERENCES workspaces (id) ON DELETE RESTRICT,
    -- The asset kind. New kinds append to this CHECK in a later migration,
    -- exactly as securable types do; the Rust enum mirrors it.
    kind           TEXT NOT NULL
        CHECK (kind IN ('fileset', 'model', 'vector_dataset')),
    -- Human name, unique per (workspace, kind).
    name           TEXT NOT NULL,
    description    TEXT,
    -- Owning principal (audit string, e.g. user:alice@example.com), free text.
    owner          TEXT,
    -- The warehouse whose storage config drives credential vending for a
    -- fileset. Nullable: models/vector datasets need no vend. RESTRICT so a
    -- warehouse cannot be dropped out from under a fileset that vends from it.
    warehouse_id   TEXT REFERENCES warehouses (id) ON DELETE RESTRICT,
    -- A fileset's object-storage prefix (s3://bucket/prefix). Non-null only
    -- for filesets; the vend is scoped to exactly this prefix.
    storage_prefix TEXT,
    -- Kind-specific metadata owned by meridian_store::assets. For a model:
    -- { version, artifacts_location, framework, ... }. For a vector dataset:
    -- { format ("lance"|...), dimensions, ... }. For a fileset: {} (its
    -- storage_prefix is a first-class column).
    metadata       JSONB NOT NULL DEFAULT '{}'::jsonb,
    -- Free-form tag strings (key:value) carried on the asset for search and
    -- for license/consent propagation into models (I-F3). Distinct from the
    -- governance tag *catalog* (0016): these are lightweight labels on a
    -- generic asset, not policy-bearing classification assignments.
    tags           TEXT[] NOT NULL DEFAULT '{}',
    -- Full-text search vector (name + description + tags + owner). Maintained
    -- by the trigger below; queried by meridian_store::assets::search.
    search_tsv     tsvector,
    created_at     TIMESTAMPTZ NOT NULL DEFAULT now(),
    updated_at     TIMESTAMPTZ NOT NULL DEFAULT now(),

    -- A fileset must carry a storage prefix and a warehouse; the other kinds
    -- must not carry a storage prefix (their location lives in metadata).
    CONSTRAINT assets_fileset_prefix CHECK (
        (kind = 'fileset' AND storage_prefix IS NOT NULL AND warehouse_id IS NOT NULL)
        OR (kind <> 'fileset' AND storage_prefix IS NULL)
    )
);

CREATE UNIQUE INDEX assets_name_unique ON assets (workspace_id, kind, name);
CREATE INDEX assets_workspace_idx ON assets (workspace_id, kind, id);
CREATE INDEX assets_search_idx ON assets USING GIN (search_tsv);

-- Maintain search_tsv on write. `simple` config on purpose (identifiers must
-- not be stemmed), matching migration 0010. Name weighted A, tags/owner B,
-- description C.
CREATE FUNCTION assets_search_tsv_update() RETURNS trigger AS $$
BEGIN
    NEW.search_tsv :=
        setweight(to_tsvector('simple', coalesce(NEW.name, '')), 'A')
        || setweight(to_tsvector('simple', array_to_string(NEW.tags, ' ')), 'B')
        || setweight(to_tsvector('simple', coalesce(NEW.owner, '')), 'B')
        || setweight(to_tsvector('simple', coalesce(NEW.description, '')), 'C');
    RETURN NEW;
END;
$$ LANGUAGE plpgsql;

CREATE TRIGGER assets_search_tsv_trigger
    BEFORE INSERT OR UPDATE ON assets
    FOR EACH ROW EXECUTE FUNCTION assets_search_tsv_update();

-- Admit 'asset' into the grants securable-type CHECK, exactly as 0007 admitted
-- 'view'. A grant on an asset attaches READ/WRITE/DROP to the asset row; the
-- Rust rbac layer resolves it. No new privilege is introduced (the leaf-native
-- READ/WRITE/DROP set applies to assets as it does to tables and views).
ALTER TABLE grants
    DROP CONSTRAINT grants_securable_type_check;

ALTER TABLE grants
    ADD CONSTRAINT grants_securable_type_check
    CHECK (securable_type IN ('warehouse', 'namespace', 'table', 'view', 'asset'));

-- ----------------------------------------------------------------------------
-- training_runs: immutable model-version -> snapshots provenance (I-F2).

CREATE TABLE training_runs (
    id             TEXT PRIMARY KEY,
    workspace_id   TEXT NOT NULL REFERENCES workspaces (id) ON DELETE RESTRICT,
    -- The model this run trained. `model_asset_id` links to a registered model
    -- asset when one exists (SET NULL if it is later dropped — the literal
    -- name/version below keep the record standing). `model` + `model_version`
    -- are ALWAYS recorded literally.
    model_asset_id TEXT REFERENCES assets (id) ON DELETE SET NULL,
    model          TEXT NOT NULL,
    model_version  TEXT NOT NULL,
    -- Free-form run metadata (framework, hyperparameters, run URL, ...).
    metadata       JSONB NOT NULL DEFAULT '{}'::jsonb,
    -- Who recorded the run (audit string).
    created_by     TEXT NOT NULL,
    -- Append-only: there is no updated_at. A run is written once.
    created_at     TIMESTAMPTZ NOT NULL DEFAULT now()
);

CREATE INDEX training_runs_workspace_idx ON training_runs (workspace_id, id);
CREATE INDEX training_runs_model_idx ON training_runs (workspace_id, model, model_version);
CREATE INDEX training_runs_model_asset_idx ON training_runs (model_asset_id);

-- One (table, snapshot_id) input per row. The snapshot_id is the EXACT Iceberg
-- snapshot id pinned (a signed 64-bit integer). ON DELETE CASCADE ties inputs
-- to their run, but a run is never deleted, so inputs are effectively
-- immutable too.
CREATE TABLE training_run_inputs (
    id              TEXT PRIMARY KEY,
    training_run_id TEXT NOT NULL REFERENCES training_runs (id) ON DELETE CASCADE,
    -- Native table id when the input is a Meridian table; NULL for a purely
    -- external dataset pinned by name. Not a FK: a dropped table must not erase
    -- the provenance that it fed a model (ULIDs are never reused).
    table_id        TEXT,
    -- Human-readable identifier of the input (`warehouse.namespace.table` or an
    -- external dataset name), always recorded so the record reads standalone.
    table_ref       TEXT NOT NULL,
    -- The pinned Iceberg snapshot id, EXACTLY as given.
    snapshot_id     BIGINT NOT NULL,
    created_at      TIMESTAMPTZ NOT NULL DEFAULT now()
);

CREATE INDEX training_run_inputs_run_idx ON training_run_inputs (training_run_id);
-- Powers the F-I4 exposure query: "which runs pinned this (table, snapshot)".
CREATE INDEX training_run_inputs_snapshot_idx
    ON training_run_inputs (table_id, snapshot_id);

-- ----------------------------------------------------------------------------
-- deletion_campaigns: GDPR "right to be forgotten" evidence (I-F4).

CREATE TABLE deletion_campaigns (
    id           TEXT PRIMARY KEY,
    workspace_id TEXT NOT NULL REFERENCES workspaces (id) ON DELETE RESTRICT,
    name         TEXT NOT NULL,
    -- The erasure subject/reason (a data-subject id, a DSAR ticket, ...).
    subject      TEXT NOT NULL,
    reason       TEXT,
    -- open -> evidence_ready -> closed. Advanced by the Rust layer as targets
    -- are added and expiry is confirmed; never regresses.
    status       TEXT NOT NULL DEFAULT 'open'
        CHECK (status IN ('open', 'evidence_ready', 'closed')),
    created_by   TEXT NOT NULL,
    created_at   TIMESTAMPTZ NOT NULL DEFAULT now(),
    updated_at   TIMESTAMPTZ NOT NULL DEFAULT now()
);

CREATE UNIQUE INDEX deletion_campaigns_name_unique
    ON deletion_campaigns (workspace_id, name);
CREATE INDEX deletion_campaigns_workspace_idx
    ON deletion_campaigns (workspace_id, id);

-- The affected snapshots a campaign targets for physical expiry. `branch` is
-- the Iceberg ref the snapshot lives on (NULL = main). `expiry_status` starts
-- 'pending' and flips to 'expired' when the maintenance ExpireSnapshots job
-- (migration 0012) confirms the snapshot is physically gone — the tie-in the
-- Rust layer records; this migration does not run expiry.
CREATE TABLE deletion_campaign_snapshots (
    id            TEXT PRIMARY KEY,
    campaign_id   TEXT NOT NULL REFERENCES deletion_campaigns (id) ON DELETE CASCADE,
    -- Native table id when known; NULL for an external dataset. Not a FK, same
    -- reasoning as training_run_inputs.table_id.
    table_id      TEXT,
    table_ref     TEXT NOT NULL,
    snapshot_id   BIGINT NOT NULL,
    branch        TEXT,
    expiry_status TEXT NOT NULL DEFAULT 'pending'
        CHECK (expiry_status IN ('pending', 'expired')),
    expired_at    TIMESTAMPTZ,
    created_at    TIMESTAMPTZ NOT NULL DEFAULT now()
);

CREATE INDEX deletion_campaign_snapshots_campaign_idx
    ON deletion_campaign_snapshots (campaign_id);

-- The evidence: which model versions saw a to-be-deleted snapshot. One row per
-- (campaign, training run, affected snapshot) match, derived from
-- training_run_inputs at evidence-generation time and frozen here so the
-- evidence is a durable record, not a live re-query.
CREATE TABLE deletion_campaign_model_exposure (
    id              TEXT PRIMARY KEY,
    campaign_id     TEXT NOT NULL REFERENCES deletion_campaigns (id) ON DELETE CASCADE,
    training_run_id TEXT NOT NULL,
    model           TEXT NOT NULL,
    model_version   TEXT NOT NULL,
    table_ref       TEXT NOT NULL,
    snapshot_id     BIGINT NOT NULL,
    created_at      TIMESTAMPTZ NOT NULL DEFAULT now()
);

CREATE INDEX deletion_campaign_model_exposure_campaign_idx
    ON deletion_campaign_model_exposure (campaign_id);
