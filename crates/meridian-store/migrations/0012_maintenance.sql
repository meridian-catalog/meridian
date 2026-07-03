-- 0012_maintenance: the table-health history and the autonomous-maintenance
-- data model (Pillar C, C-F1 health + C-F3 policy/job/ledger).
--
-- Append-only: this file only adds to the 0001-0011 schema; nothing is
-- rewritten. Every table here is workspace-scoped and every mutation to the
-- policy/job/ledger tables is audited + outboxed in the mutating transaction
-- (the same discipline the commit path uses). Maintenance *executions*
-- themselves are ordinary Iceberg commits through the existing commit path —
-- these tables are the control plane (what to do, what was done, what it
-- saved), never a second write path for table pointers.

-- ----------------------------------------------------------------------------
-- health_snapshots: one row per health computation, for trend/history.
--
-- compute_health() reads the current snapshot's manifests (no data scans) and
-- the write-through index, derives the metrics below, and appends a row. The
-- composite score and the metric breakdown are stored so the UI/API can chart
-- a table's health over time and so a recommendation can cite the inputs that
-- produced it. Rows are immutable once written (history, not live state).
--
-- table_id is a FK with CASCADE: health history is meaningless once the table
-- is gone, unlike metrics_reports (evidence) which deliberately outlive drops.

CREATE TABLE health_snapshots (
    id                   TEXT PRIMARY KEY,
    workspace_id         TEXT NOT NULL REFERENCES workspaces (id) ON DELETE RESTRICT,
    table_id             TEXT NOT NULL REFERENCES tables (id) ON DELETE CASCADE,
    -- The table snapshot the health was computed against (NULL for an empty
    -- table with no current snapshot). Lets a health row be pinned to the
    -- exact table state it describes.
    snapshot_id          BIGINT,
    -- Composite health score, 0-100 (100 = healthiest). Deterministic from
    -- the metrics below via the documented formula in src/health.rs.
    score                SMALLINT NOT NULL CHECK (score BETWEEN 0 AND 100),
    -- Physical size and file-shape metrics.
    total_bytes          BIGINT NOT NULL DEFAULT 0 CHECK (total_bytes >= 0),
    data_file_count      BIGINT NOT NULL DEFAULT 0 CHECK (data_file_count >= 0),
    -- Files strictly below the effective target file size / total data files,
    -- as a ratio in [0,1]; 0 when there are no data files.
    small_file_ratio     DOUBLE PRECISION NOT NULL DEFAULT 0
        CHECK (small_file_ratio BETWEEN 0 AND 1),
    avg_file_bytes       BIGINT NOT NULL DEFAULT 0 CHECK (avg_file_bytes >= 0),
    median_file_bytes    BIGINT NOT NULL DEFAULT 0 CHECK (median_file_bytes >= 0),
    -- Snapshot bloat.
    snapshot_count       INTEGER NOT NULL DEFAULT 0 CHECK (snapshot_count >= 0),
    oldest_snapshot_ms   BIGINT,
    -- Delete/DV debt: (position + equality delete files + DVs) / data files,
    -- as a ratio (can exceed 1 for heavily-deleted tables); 0 when no data.
    delete_debt_ratio    DOUBLE PRECISION NOT NULL DEFAULT 0
        CHECK (delete_debt_ratio >= 0),
    delete_file_count    BIGINT NOT NULL DEFAULT 0 CHECK (delete_file_count >= 0),
    -- Manifest fragmentation: manifest count and avg live entries/manifest.
    manifest_count       INTEGER NOT NULL DEFAULT 0 CHECK (manifest_count >= 0),
    avg_manifest_entries DOUBLE PRECISION NOT NULL DEFAULT 0
        CHECK (avg_manifest_entries >= 0),
    -- Partition skew: coefficient of variation of bytes-per-partition in
    -- [0, +inf); 0 for unpartitioned or perfectly even tables.
    partition_skew       DOUBLE PRECISION NOT NULL DEFAULT 0
        CHECK (partition_skew >= 0),
    -- metadata.json size in bytes (metadata bloat signal).
    metadata_json_bytes  BIGINT NOT NULL DEFAULT 0 CHECK (metadata_json_bytes >= 0),
    -- The full file-size histogram, per-component sub-scores, and the top-3
    -- recommended actions, as computed. Shape owned by src/health.rs.
    metrics              JSONB NOT NULL DEFAULT '{}'::jsonb,
    recommendations      JSONB NOT NULL DEFAULT '[]'::jsonb,
    computed_at          TIMESTAMPTZ NOT NULL DEFAULT now()
);

CREATE INDEX health_snapshots_table_id_idx
    ON health_snapshots (table_id, computed_at DESC);
CREATE INDEX health_snapshots_workspace_id_idx ON health_snapshots (workspace_id);

-- ----------------------------------------------------------------------------
-- maintenance_policies: declarative, per-scope maintenance configuration
-- (C-F3). A policy applies to a warehouse, a namespace, or a single table;
-- the *effective* policy for a table is the most-specific scope that matches
-- (table > namespace > warehouse), resolved in src/maintenance.rs.
--
-- scope_id holds the id of the scoped object (a warehouse id, namespace id,
-- or table id) as text; the scope column disambiguates which table it points
-- into. It is not a FK because it is polymorphic across three tables; the
-- application enforces existence on write.

CREATE TABLE maintenance_policies (
    id                       TEXT PRIMARY KEY,
    workspace_id             TEXT NOT NULL REFERENCES workspaces (id) ON DELETE RESTRICT,
    scope                    TEXT NOT NULL
        CHECK (scope IN ('warehouse', 'namespace', 'table')),
    scope_id                 TEXT NOT NULL,
    -- Target compacted file size; the small-file threshold in health and the
    -- bin-pack target for compaction. Default 512 MiB.
    target_file_size_bytes   BIGINT NOT NULL DEFAULT 536870912
        CHECK (target_file_size_bytes > 0),
    -- Compaction will not run unless at least this many input files can be
    -- combined. Default 5.
    min_input_files          INTEGER NOT NULL DEFAULT 5
        CHECK (min_input_files >= 1),
    -- Snapshot retention: keep at least this many snapshots AND anything
    -- younger than this age; expiry removes only snapshots failing both.
    snapshot_retention_count INTEGER NOT NULL DEFAULT 100
        CHECK (snapshot_retention_count >= 1),
    snapshot_retention_age_ms BIGINT NOT NULL DEFAULT 432000000  -- 5 days
        CHECK (snapshot_retention_age_ms >= 0),
    -- Alert/act when the newest commit is older than this. NULL = no SLA.
    max_staleness_ms         BIGINT CHECK (max_staleness_ms IS NULL OR max_staleness_ms >= 0),
    -- Cron-ish schedule string + an execution window; interpreted by the
    -- scheduler wave. NULL schedule = reconcile-driven (desired-state) only.
    schedule                 TEXT,
    window_start             TEXT,   -- e.g. "02:00" local-to-window
    window_end               TEXT,
    -- Optional monthly spend cap for this scope's maintenance ($USD).
    cost_cap_usd_month       DOUBLE PRECISION
        CHECK (cost_cap_usd_month IS NULL OR cost_cap_usd_month >= 0),
    -- Exclusion rules (e.g. name globs, tag predicates, job-type opt-outs);
    -- shape owned by src/maintenance.rs. Default: exclude nothing.
    exclusions               JSONB NOT NULL DEFAULT '{}'::jsonb,
    enabled                  BOOLEAN NOT NULL DEFAULT TRUE,
    created_by               TEXT NOT NULL,
    created_at               TIMESTAMPTZ NOT NULL DEFAULT now(),
    updated_at               TIMESTAMPTZ NOT NULL DEFAULT now(),
    -- At most one policy per (workspace, scope, scope_id): a scope's policy is
    -- singular, edited in place. Resolution picks between scopes, never
    -- between duplicate rows at one scope.
    UNIQUE (workspace_id, scope, scope_id)
);

CREATE INDEX maintenance_policies_workspace_idx
    ON maintenance_policies (workspace_id, scope, scope_id);

-- ----------------------------------------------------------------------------
-- maintenance_jobs: the work queue. One row per maintenance operation on one
-- table, claimed by workers with FOR UPDATE SKIP LOCKED (per-tenant fair, see
-- src/maintenance.rs). state is a small lifecycle; every transition is a
-- compare-and-set on the prior state so racing workers have one winner.
--
-- result carries before/after metrics on success (mirrored into
-- savings_ledger); error carries the failure payload. attempts counts claim
-- cycles so the scheduler can cap retries.

CREATE TABLE maintenance_jobs (
    id           TEXT PRIMARY KEY,
    workspace_id TEXT NOT NULL REFERENCES workspaces (id) ON DELETE RESTRICT,
    table_id     TEXT NOT NULL REFERENCES tables (id) ON DELETE CASCADE,
    job_type     TEXT NOT NULL CHECK (job_type IN (
        'compaction', 'expire_snapshots', 'remove_orphans', 'rewrite_manifests'
    )),
    state        TEXT NOT NULL DEFAULT 'queued' CHECK (state IN (
        'queued', 'running', 'succeeded', 'failed', 'cancelled'
    )),
    -- The policy that scheduled this job, when it was policy-driven (NULL for
    -- ad-hoc/manual jobs). SET NULL on policy delete: the job's history stays.
    policy_id    TEXT REFERENCES maintenance_policies (id) ON DELETE SET NULL,
    -- Job parameters (targets, safety window, dry-run flag, ...); shape owned
    -- by src/maintenance.rs.
    spec         JSONB NOT NULL DEFAULT '{}'::jsonb,
    created_by   TEXT NOT NULL,
    -- Identifier of the worker currently holding the job (NULL when not
    -- running); set on claim, cleared on terminal transition.
    claimed_by   TEXT,
    attempts     INTEGER NOT NULL DEFAULT 0 CHECK (attempts >= 0),
    error        JSONB,
    -- Before/after metrics on success (bytes/files before+after, snapshot the
    -- maintenance commit produced, ...).
    result       JSONB,
    created_at   TIMESTAMPTZ NOT NULL DEFAULT now(),
    started_at   TIMESTAMPTZ,
    finished_at  TIMESTAMPTZ,
    updated_at   TIMESTAMPTZ NOT NULL DEFAULT now()
);

-- The claim query orders queued jobs oldest-first; this partial index keeps
-- that scan tight even with a large finished-job history.
CREATE INDEX maintenance_jobs_queued_idx
    ON maintenance_jobs (workspace_id, created_at)
    WHERE state = 'queued';
CREATE INDEX maintenance_jobs_table_id_idx ON maintenance_jobs (table_id);
CREATE INDEX maintenance_jobs_workspace_state_idx
    ON maintenance_jobs (workspace_id, state);

-- ----------------------------------------------------------------------------
-- savings_ledger: the append-only receipt of what maintenance saved, per job.
-- One row per completed job that removed bytes/files; monthly roll-ups (the
-- CFO-legible "Meridian saved you $X") are aggregates over `period`.
--
-- job_id is UNIQUE: a job contributes exactly one ledger row, so a monthly
-- rollup never double-counts a re-read job. table_id is denormalized and not
-- a FK for the same reason metrics_reports is not: a savings receipt is
-- historical evidence that must survive the table's drop.

CREATE TABLE savings_ledger (
    id                     TEXT PRIMARY KEY,
    workspace_id           TEXT NOT NULL REFERENCES workspaces (id) ON DELETE RESTRICT,
    job_id                 TEXT NOT NULL UNIQUE,
    table_id               TEXT NOT NULL,
    table_ident            TEXT NOT NULL,
    -- Accounting period as a first-of-month date (UTC), the rollup grain.
    period                 DATE NOT NULL,
    bytes_before           BIGINT NOT NULL CHECK (bytes_before >= 0),
    bytes_after            BIGINT NOT NULL CHECK (bytes_after >= 0),
    files_before           BIGINT NOT NULL CHECK (files_before >= 0),
    files_after            BIGINT NOT NULL CHECK (files_after >= 0),
    -- Derived, but stored so the ledger is self-contained evidence: a rollup
    -- never has to re-derive from before/after (and negative "savings" from a
    -- job that grew the table are representable honestly).
    bytes_saved            BIGINT NOT NULL,
    files_removed          BIGINT NOT NULL,
    -- Projected object-store GET requests avoided (the small-file cost model).
    est_get_requests_saved BIGINT NOT NULL DEFAULT 0,
    -- How the numbers were derived (which cost model, assumptions), shown in
    -- the ledger UI/export so the savings claim is auditable.
    methodology            TEXT NOT NULL,
    created_at             TIMESTAMPTZ NOT NULL DEFAULT now()
);

CREATE INDEX savings_ledger_rollup_idx
    ON savings_ledger (workspace_id, period);
CREATE INDEX savings_ledger_table_idx ON savings_ledger (table_id);
