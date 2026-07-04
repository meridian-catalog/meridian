-- 0019_monitors_incidents: zero-scan data-quality monitors (Pillar E, E-F1),
-- their evaluation results, and the incident ledger (E-F5). This is the
-- detection half of the observability pillar: monitors are evaluated from the
-- commit stream + the table_snapshots write-through index + metrics_reports —
-- never by scanning data files — and open incidents on anomalies. Contract
-- violations (0018, the circuit breaker) also open incidents through the same
-- ledger, so producers and operators see one status per table.
--
-- Append-only: this file only adds to the prior schema; nothing is rewritten.
-- The evaluation engine (the anomaly scorers) and the post-commit evaluation
-- worker live in the Rust layer (meridian_store::monitors + the server's
-- quality-monitor worker); this migration owns the *definitions*, the
-- *results*, and the *incidents*.
--
-- Model:
--
--  * monitors: a workspace-scoped, opt-in monitor bound to a securable (a table
--    OR a namespace, resolved at evaluation time — a namespace monitor covers
--    every table under it without rebinding). `kind` selects the zero-scan
--    signal (freshness / volume / schema_change / file_size / snapshot_debt /
--    commit_failure). `config` is typed jsonb owned by the Rust layer
--    (thresholds, sensitivity, learned-cadence overrides). `severity` fixes the
--    severity of an incident this monitor opens.
--  * monitor_results: append-only evaluation history — one row per (monitor,
--    table, evaluation). Records the numeric observed value, the baseline it was
--    scored against, the status (ok | warn | breach), and the machine `detail`.
--    This is the monitor's evidence trail and the series a chart reads.
--  * incidents: the incident ledger (open | acknowledged | resolved) with
--    ownership routing (owner captured at open time from the table's `owner`
--    property, never fabricated) and a stable `dedup_key` so a still-open
--    incident for the same (table, kind) is re-touched rather than duplicated on
--    every subsequent bad commit. Blast radius (downstream asset ids, from the
--    lineage impact function) is captured as jsonb at open time.
--
-- Polymorphic ids (securable_id, table_id) are TEXT and deliberately NOT foreign
-- keys, exactly like contracts.securable_id (0018), grants.securable_id (0005),
-- and policy_bindings.securable_id (0016): they range over several tables, and
-- dropping a securable leaves inert evidence rows behind (resolution matches by
-- id; ULIDs are never reused). monitor_id, by contrast, IS a real table, so
-- result references use a real FK with CASCADE.

-- ----------------------------------------------------------------------------
-- monitors: the opt-in, zero-scan monitor definition.
--
-- bound_to + securable_id name the binding polymorphically, mirroring contracts:
--   * bound_to='table':     securable_id is the table's id; evaluated on commits
--     to that one table.
--   * bound_to='namespace': securable_id is the namespace's id; evaluated on
--     commits to every table under it (resolved at evaluation time).
--
-- `kind` is the zero-scan signal. `config` is typed jsonb the Rust layer owns
-- (MonitorConfig): the per-kind thresholds and sensitivity. `severity` is the
-- severity an incident opened by this monitor carries.

CREATE TABLE monitors (
    id            TEXT PRIMARY KEY,
    workspace_id  TEXT NOT NULL REFERENCES workspaces (id) ON DELETE RESTRICT,
    name          TEXT NOT NULL,
    -- What the monitor binds to: a single table, or a namespace (all tables
    -- under it). Polymorphic securable_id below.
    bound_to      TEXT NOT NULL
        CHECK (bound_to IN ('table', 'namespace')),
    -- The bound securable's id (table id or namespace id). NOT an FK.
    securable_id  TEXT NOT NULL,
    -- The zero-scan signal this monitor computes.
    kind          TEXT NOT NULL
        CHECK (kind IN (
            'freshness', 'volume', 'schema_change',
            'file_size', 'snapshot_debt', 'commit_failure'
        )),
    -- A disabled monitor is retained (and still readable) but skipped by the
    -- evaluation worker — never evaluated.
    enabled       BOOLEAN NOT NULL DEFAULT TRUE,
    -- Severity of an incident this monitor opens: low | medium | high.
    severity      TEXT NOT NULL DEFAULT 'medium'
        CHECK (severity IN ('low', 'medium', 'high')),
    -- Typed jsonb owned by the Rust layer (MonitorConfig): the per-kind
    -- thresholds/sensitivity. Empty object means "use the defaults".
    config        JSONB NOT NULL DEFAULT '{}'::jsonb,
    -- Audit string of the creating principal.
    created_by    TEXT NOT NULL,
    created_at    TIMESTAMPTZ NOT NULL DEFAULT now(),
    updated_at    TIMESTAMPTZ NOT NULL DEFAULT now(),
    -- One monitor of a given kind per securable: a table cannot have two
    -- freshness monitors. (A namespace monitor and a table monitor of the same
    -- kind can both apply to a table — they have different securable_ids.)
    UNIQUE (workspace_id, bound_to, securable_id, kind)
);

-- The hot path is "which enabled monitors bind to this securable" — evaluation
-- reads by (workspace, bound_to, securable_id), so index that.
CREATE INDEX monitors_binding_idx
    ON monitors (workspace_id, bound_to, securable_id);

-- ----------------------------------------------------------------------------
-- monitor_results: append-only per-evaluation history (evidence + series).
--
-- One row per (monitor, table, evaluation). A namespace monitor evaluated
-- against several tables writes one row per table. `observed_value` and
-- `baseline_value` are the numbers the scorer compared (NULL when a signal was
-- not measurable this pass — e.g. no history yet); `status` is the outcome;
-- `detail` is the human string. `snapshot_id` is the committed head the
-- evaluation ran against, when known. Rows are never mutated or deleted (the
-- series is the audit trail), matching the metrics_reports / health-history
-- discipline. table_id is polymorphic TEXT (NOT an FK) — evidence outlives a
-- dropped table.

CREATE TABLE monitor_results (
    id             TEXT PRIMARY KEY,
    workspace_id   TEXT NOT NULL REFERENCES workspaces (id) ON DELETE RESTRICT,
    monitor_id     TEXT NOT NULL REFERENCES monitors (id) ON DELETE CASCADE,
    -- The table this evaluation was for (polymorphic; NOT an FK).
    table_id       TEXT NOT NULL,
    kind           TEXT NOT NULL,
    -- ok | warn | breach. `warn` records an anomaly below the incident
    -- threshold; `breach` is what opens an incident.
    status         TEXT NOT NULL
        CHECK (status IN ('ok', 'warn', 'breach')),
    -- The measured value and the baseline it was scored against (NULL when not
    -- measurable this evaluation, e.g. no prior history).
    observed_value DOUBLE PRECISION,
    baseline_value DOUBLE PRECISION,
    -- Machine-readable classification token (e.g. 'volume-spike',
    -- 'breaking-schema-change', 'stale'); stable for filtering.
    result_kind    TEXT NOT NULL,
    -- Human-readable detail.
    detail         TEXT NOT NULL,
    -- The committed head snapshot the evaluation ran against, when known.
    snapshot_id    BIGINT,
    evaluated_at   TIMESTAMPTZ NOT NULL DEFAULT now()
);

-- Results are read as a per-table and per-monitor series, newest first.
CREATE INDEX monitor_results_monitor_idx
    ON monitor_results (monitor_id, evaluated_at DESC);
CREATE INDEX monitor_results_table_idx
    ON monitor_results (table_id, evaluated_at DESC);
CREATE INDEX monitor_results_workspace_idx
    ON monitor_results (workspace_id, evaluated_at DESC);

-- ----------------------------------------------------------------------------
-- incidents: the incident ledger (E-F5).
--
-- An incident is opened when a monitor breaches or a contract is violated. It
-- carries a lifecycle status (open -> acknowledged -> resolved), the severity
-- (inherited from the monitor, or from the contract mode), the owner captured
-- at open time (from the table's `owner` property; NULL when unowned — never
-- fabricated), and the blast radius (downstream asset ids from the lineage
-- impact function) as jsonb.
--
-- `dedup_key` is the stable identity of an *ongoing* condition: while an
-- incident for the same (table, source, kind) is still open or acknowledged,
-- subsequent bad evaluations re-touch it (bump last_seen_at + occurrence_count)
-- rather than opening a duplicate. A partial unique index enforces "at most one
-- live incident per dedup_key" without blocking a fresh incident once the prior
-- one is resolved. table_id is polymorphic TEXT (NOT an FK) — an incident record
-- outlives a dropped table (audit discipline: records are never silently lost).

CREATE TABLE incidents (
    id               TEXT PRIMARY KEY,
    workspace_id     TEXT NOT NULL REFERENCES workspaces (id) ON DELETE RESTRICT,
    -- The table the incident is about (polymorphic; NOT an FK).
    table_id         TEXT NOT NULL,
    -- Denormalized human identity at open time (warehouse.ns.table), so the
    -- ledger reads without a join and survives a rename/drop.
    table_ident      TEXT NOT NULL,
    -- What opened it: 'monitor' (a monitor breach) or 'contract' (a circuit-
    -- breaker violation).
    source           TEXT NOT NULL
        CHECK (source IN ('monitor', 'contract')),
    -- The monitor kind or contract violation kind that opened it (e.g.
    -- 'volume', 'schema_change', 'protected-column-dropped').
    kind             TEXT NOT NULL,
    -- Lifecycle: open -> acknowledged -> resolved.
    status           TEXT NOT NULL DEFAULT 'open'
        CHECK (status IN ('open', 'acknowledged', 'resolved')),
    severity         TEXT NOT NULL DEFAULT 'medium'
        CHECK (severity IN ('low', 'medium', 'high')),
    -- One-line human summary shown in the ledger.
    title            TEXT NOT NULL,
    -- Longer human detail (the breaching value + baseline, the violation text).
    detail           TEXT NOT NULL,
    -- Owner captured at open time from the table's `owner` property; NULL when
    -- the table has no owner (never inferred).
    owner            TEXT,
    -- Downstream blast radius (a JSON array of { table_id, ident, depth }) from
    -- the lineage impact function at open time. Empty array when the table has
    -- no recorded downstream lineage (truthfully empty).
    blast_radius     JSONB NOT NULL DEFAULT '[]'::jsonb,
    -- The originating monitor (when source='monitor'); NULL for contract
    -- incidents. FK with SET NULL so deleting a monitor does not erase its
    -- incident history.
    monitor_id       TEXT REFERENCES monitors (id) ON DELETE SET NULL,
    -- Stable identity of the ongoing condition, for de-duplication (see the
    -- partial unique index below).
    dedup_key        TEXT NOT NULL,
    -- How many times the condition recurred while this incident stayed live.
    occurrence_count INTEGER NOT NULL DEFAULT 1 CHECK (occurrence_count >= 1),
    -- Who acknowledged / resolved it, and when (NULL until it happens).
    acknowledged_by  TEXT,
    acknowledged_at  TIMESTAMPTZ,
    resolved_by      TEXT,
    resolved_at      TIMESTAMPTZ,
    first_seen_at    TIMESTAMPTZ NOT NULL DEFAULT now(),
    last_seen_at     TIMESTAMPTZ NOT NULL DEFAULT now(),
    -- A resolved incident must record who/when; an unresolved one must not.
    CHECK ((status = 'resolved') = (resolved_at IS NOT NULL)),
    CHECK ((acknowledged_at IS NULL) = (acknowledged_by IS NULL)),
    CHECK ((resolved_at IS NULL) = (resolved_by IS NULL))
);

-- At most one *live* (open or acknowledged) incident per dedup_key: the
-- de-duplication invariant. A resolved incident drops out of the index, so the
-- same condition recurring later opens a fresh incident.
CREATE UNIQUE INDEX incidents_live_dedup_idx
    ON incidents (workspace_id, dedup_key)
    WHERE status <> 'resolved';

-- The ledger is read per-table and per-status, newest first.
CREATE INDEX incidents_table_idx
    ON incidents (table_id, last_seen_at DESC);
CREATE INDEX incidents_status_idx
    ON incidents (workspace_id, status, last_seen_at DESC);
