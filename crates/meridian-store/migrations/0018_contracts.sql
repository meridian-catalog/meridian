-- 0018_contracts: data contracts (Pillar E, E-F3) and the violation record
-- the circuit breaker (E-F4) writes. Versioned contract objects bound to a
-- table or a namespace, their append-only version history, and the violation
-- ledger. See docs/design/contracts-circuit-breaker.md.
--
-- (0017 is the sibling lineage migration; contracts took the next free number.)
--
-- Append-only: this file only adds to the prior schema; nothing is
-- rewritten. It owns the *definitions* (the versioned contract + its spec) and
-- the *records* (violations); the synchronous evaluation engine and the
-- commit-path hook live in meridian_store::contracts and the commit driver.
--
-- Model:
--
--  * contracts: a workspace-scoped, versioned governance object that binds to a
--    securable (a table OR a namespace) and declares an enforcement `mode`
--    (warn | quarantine | block) and a typed `spec` (jsonb owned by the Rust
--    layer: schema-evolution rules + cheap synchronous predicates). `version`
--    is the CURRENT version; every version's full spec is retained in
--    contract_versions.
--  * contract_versions: append-only history — one row per version, holding the
--    spec + mode snapshot at that version. Enables audit and rollback; the
--    current contracts.version always equals MAX(version) here.
--  * contract_violations: one row per detected violation. Records whether the
--    commit was rejected (block) or quarantined, the head snapshot involved
--    (nullable), and the machine `kind` + human `detail`.
--
-- Polymorphic ids (securable_id, table_id) are TEXT and deliberately NOT
-- foreign keys, exactly like grants.securable_id in 0005 and
-- policy_bindings.securable_id in 0016: they range over several tables and
-- dropping a securable leaves inert rows behind (resolution matches by id;
-- ULIDs are never reused). contract_id, by contrast, IS a real table, so
-- version/violation references use real FKs with CASCADE.

-- ----------------------------------------------------------------------------
-- contracts: the versioned contract object.
--
-- bound_to + securable_id name the binding polymorphically:
--   * bound_to='table':     securable_id is the table's id; the contract is
--     evaluated on commits to that one table.
--   * bound_to='namespace': securable_id is the namespace's id; the contract is
--     evaluated on commits to every table under that namespace (resolved at
--     evaluation time — a namespace binding needs no rebinding when tables are
--     added or removed).
--
-- `spec` is the typed jsonb the Rust layer owns (see
-- meridian_store::contracts::ContractSpec): the schema-evolution rule
-- (allowed_evolution / protected_columns / required_columns) plus the cheap
-- synchronous predicates. `mode` fixes what the circuit breaker does on a
-- violation. `quarantine_branch` names the Iceberg branch a quarantined commit
-- is retargeted onto (only meaningful for mode='quarantine').

CREATE TABLE contracts (
    id                TEXT PRIMARY KEY,
    workspace_id      TEXT NOT NULL REFERENCES workspaces (id) ON DELETE RESTRICT,
    name              TEXT NOT NULL,
    -- What the contract binds to: a single table, or a namespace (all tables
    -- under it). Polymorphic securable_id below.
    bound_to          TEXT NOT NULL
        CHECK (bound_to IN ('table', 'namespace')),
    -- The bound securable's id (table id or namespace id). NOT an FK (like
    -- grants.securable_id): it ranges over tables and namespaces.
    securable_id      TEXT NOT NULL,
    -- Current version, monotonic, starts at 1, +1 per update.
    version           INTEGER NOT NULL DEFAULT 1 CHECK (version >= 1),
    -- A disabled contract is retained (and still readable) but is skipped by
    -- the pre-commit hook — never enforced.
    enabled           BOOLEAN NOT NULL DEFAULT TRUE,
    -- The circuit-breaker mode.
    mode              TEXT NOT NULL DEFAULT 'warn'
        CHECK (mode IN ('warn', 'quarantine', 'block')),
    -- Typed jsonb owned by the Rust layer (ContractSpec).
    spec              JSONB NOT NULL,
    -- The Iceberg branch a quarantined commit is retargeted onto; only
    -- meaningful when mode='quarantine'. Defaulted so a mode change to
    -- quarantine always has a target.
    quarantine_branch TEXT NOT NULL DEFAULT 'meridian_quarantine',
    -- Audit string of the creating principal.
    created_by        TEXT NOT NULL,
    created_at        TIMESTAMPTZ NOT NULL DEFAULT now(),
    updated_at        TIMESTAMPTZ NOT NULL DEFAULT now(),
    UNIQUE (workspace_id, name)
);

-- The hot path is "which enabled contracts bind to this securable" — resolution
-- reads by (workspace, bound_to, securable_id), so index that.
CREATE INDEX contracts_binding_idx
    ON contracts (workspace_id, bound_to, securable_id);

-- ----------------------------------------------------------------------------
-- contract_versions: append-only per-version history (audit + rollback).
--
-- One row per (contract, version). The current contracts.version always has a
-- matching row here; older rows are never mutated or deleted (rollback creates
-- a NEW version whose spec is copied from an old one — history stays
-- append-only, matching the audit-log and policy-versioning discipline).

CREATE TABLE contract_versions (
    contract_id  TEXT NOT NULL REFERENCES contracts (id) ON DELETE CASCADE,
    version      INTEGER NOT NULL CHECK (version >= 1),
    mode         TEXT NOT NULL
        CHECK (mode IN ('warn', 'quarantine', 'block')),
    enabled      BOOLEAN NOT NULL,
    spec         JSONB NOT NULL,
    -- Audit string of the principal who created this version.
    created_by   TEXT NOT NULL,
    created_at   TIMESTAMPTZ NOT NULL DEFAULT now(),
    PRIMARY KEY (contract_id, version)
);

-- ----------------------------------------------------------------------------
-- contract_violations: the violation ledger the circuit breaker writes.
--
-- One row per detected violation. commit_rejected is TRUE for block-mode
-- rejections (nothing committed); FALSE for warn (landed) and quarantine
-- (landed on the audit branch, main not advanced). quarantined is TRUE only for
-- quarantine mode. snapshot_id is the head snapshot involved when known
-- (the staged snapshot for warn/quarantine; NULL when the commit was rejected
-- before a snapshot was meaningful). table_id is polymorphic TEXT (the table's
-- id), NOT an FK — a violation record outlives a dropped table (audit
-- discipline: records are never silently lost).

CREATE TABLE contract_violations (
    id              TEXT PRIMARY KEY,
    workspace_id    TEXT NOT NULL REFERENCES workspaces (id) ON DELETE RESTRICT,
    contract_id     TEXT NOT NULL REFERENCES contracts (id) ON DELETE CASCADE,
    -- The table the violating commit targeted (polymorphic; NOT an FK).
    table_id        TEXT NOT NULL,
    -- The head snapshot involved, when known (NULL for pre-snapshot rejects).
    snapshot_id     BIGINT,
    -- Stable machine token, e.g. 'protected-column-dropped', 'schema-narrowed'.
    kind            TEXT NOT NULL,
    -- Human-readable detail.
    detail          TEXT NOT NULL,
    -- TRUE iff the commit was rejected (block mode); nothing durable committed.
    commit_rejected BOOLEAN NOT NULL,
    -- TRUE iff the commit was quarantined onto the audit branch (main frozen).
    quarantined     BOOLEAN NOT NULL DEFAULT FALSE,
    occurred_at     TIMESTAMPTZ NOT NULL DEFAULT now(),
    -- A rejected commit is never also quarantined (mutually exclusive
    -- outcomes: block rejects, quarantine lands-off-main).
    CHECK (NOT (commit_rejected AND quarantined))
);

-- The violations query filters by contract and by table, newest first.
CREATE INDEX contract_violations_contract_idx
    ON contract_violations (contract_id, occurred_at DESC);
CREATE INDEX contract_violations_table_idx
    ON contract_violations (table_id, occurred_at DESC);
CREATE INDEX contract_violations_workspace_idx
    ON contract_violations (workspace_id, occurred_at DESC);
