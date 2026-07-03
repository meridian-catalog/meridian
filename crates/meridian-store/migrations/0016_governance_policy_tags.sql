-- 0016_governance_policy_tags: the governance policy + tag data model
-- (Pillar D, D-F1). Tags, tag assignments (column-level), versioned policies,
-- polychrome policy bindings (to a securable OR to a tag), the policy-version
-- history, and the access-request object for the D-F4 workflow (data model
-- only here; the request/approval workflow lands in a later wave).
--
-- Append-only: this file only adds to the 0001-0015 schema; nothing is
-- rewritten. It does NOT build the enforcement engine (the Cedar/ABAC
-- evaluator and the scan-plan/residual injection are the sibling engine and
-- wave 2 respectively). This migration + meridian_store::{policy,tags} own
-- the *definitions* and the *resolution* (which policies apply to a table,
-- directly and via tags), which the enforcement engine consumes.
--
-- Model:
--
--  * tags: a workspace-scoped (key, value) pair, e.g. pii:email. The unit of
--    classification. Policies can bind to a tag so they apply wherever the tag
--    is assigned.
--  * tag_assignments: a tag placed on a securable — a table, a namespace, or a
--    single COLUMN of a table (column_name non-null). Column-level tagging is
--    first-class because column masks and column-scoped row policies need it.
--    An assignment records its provenance (manual vs classifier), a confidence
--    (for classifier suggestions), and an approval bit (a suggested tag is not
--    yet in force until approved).
--  * policies: a versioned governance object of one kind — row_filter,
--    column_mask, or abac. `definition` is the typed jsonb the Rust layer owns
--    (see meridian_store::policy). `version` is the current (latest) version;
--    every version's full definition is retained in policy_versions.
--  * policy_versions: append-only history — one row per version of a policy,
--    holding the definition snapshot at that version. Enables audit and
--    rollback. The current policies.version always equals MAX(version) here.
--  * policy_bindings: attaches a policy to a target that is EITHER a securable
--    (table | namespace) OR a tag (XOR). A tag binding applies the policy
--    wherever the tag is assigned; a direct binding applies it to that one
--    securable (and, for a namespace, everything it contains — resolved in
--    code, not materialized).
--  * access_requests: a principal's request for a privilege on a securable,
--    with purpose + TTL, moving through pending -> approved | denied | expired.
--    Data model only; the workflow (routing, approvals, auto-expiry) is D-F4,
--    a later wave.
--
-- Polymorphic ids (securable_id, tag/securable targets) are TEXT and
-- deliberately NOT foreign keys, exactly like grants.securable_id in 0005:
-- they range over several tables and dropping a securable leaves inert rows
-- behind (resolution matches by id; ULIDs are never reused). Tag ids, by
-- contrast, ARE real tables here, so tag references use real FKs with CASCADE.

-- ----------------------------------------------------------------------------
-- tags: the classification unit (key:value), workspace-scoped.

CREATE TABLE tags (
    id           TEXT PRIMARY KEY,
    workspace_id TEXT NOT NULL REFERENCES workspaces (id) ON DELETE RESTRICT,
    -- e.g. key='pii', value='email' renders as the tag `pii:email`.
    key          TEXT NOT NULL,
    value        TEXT NOT NULL,
    description  TEXT,
    created_at   TIMESTAMPTZ NOT NULL DEFAULT now(),
    -- One tag per (workspace, key, value): the (key,value) pair is the tag's
    -- identity; the ULID id is the stable handle bindings/assignments point at.
    UNIQUE (workspace_id, key, value)
);

CREATE INDEX tags_workspace_key_idx ON tags (workspace_id, key);

-- ----------------------------------------------------------------------------
-- tag_assignments: a tag placed on a securable, optionally on one column.
--
-- securable_type is table | namespace | column. `column` is not a distinct
-- securable elsewhere (grants stop at table/view); here it matters because a
-- tag on a single column drives a column mask. When securable_type='column',
-- securable_id is the TABLE's id and column_name names the column; otherwise
-- column_name is NULL. The CHECK enforces that pairing.

CREATE TABLE tag_assignments (
    id             TEXT PRIMARY KEY,
    workspace_id   TEXT NOT NULL REFERENCES workspaces (id) ON DELETE RESTRICT,
    tag_id         TEXT NOT NULL REFERENCES tags (id) ON DELETE CASCADE,
    securable_type TEXT NOT NULL
        CHECK (securable_type IN ('table', 'namespace', 'column')),
    -- Polymorphic id (like grants.securable_id): the table id, namespace id,
    -- or — for a column assignment — the owning table's id. Not an FK.
    securable_id   TEXT NOT NULL,
    -- Set iff securable_type='column'; NULL otherwise.
    column_name    TEXT,
    -- Provenance of the assignment.
    source         TEXT NOT NULL DEFAULT 'manual'
        CHECK (source IN ('manual', 'classifier')),
    -- Classifier confidence in [0,1]; NULL for manual assignments.
    confidence     DOUBLE PRECISION
        CHECK (confidence IS NULL OR (confidence >= 0 AND confidence <= 1)),
    -- A suggested tag is not in force until approved. Manual assignments are
    -- created approved; classifier suggestions start unapproved.
    approved       BOOLEAN NOT NULL DEFAULT TRUE,
    -- Audit string of the assigning principal (or the classifier job).
    assigned_by    TEXT NOT NULL,
    created_at     TIMESTAMPTZ NOT NULL DEFAULT now(),
    -- column_name is present exactly for column assignments.
    CHECK ((securable_type = 'column') = (column_name IS NOT NULL))
);

-- One assignment per (tag, securable, column). COALESCE folds the nullable
-- column into the key so a table-level and a column-level assignment of the
-- same tag are distinct, and re-assigning the same tag to the same target is a
-- conflict rather than a silent duplicate.
CREATE UNIQUE INDEX tag_assignments_unique_idx ON tag_assignments
    (tag_id, securable_type, securable_id, COALESCE(column_name, ''));

-- Resolution walks assignments FROM a securable (all tags on this table /
-- these columns / this namespace), so index by the securable.
CREATE INDEX tag_assignments_securable_idx
    ON tag_assignments (securable_type, securable_id);
CREATE INDEX tag_assignments_tag_id_idx ON tag_assignments (tag_id);

-- ----------------------------------------------------------------------------
-- policies: a versioned governance object.
--
-- `version` is the CURRENT version; the full definition of every version lives
-- in policy_versions. `definition` here is a denormalized copy of the current
-- version's definition (so the common "load the effective policy" path is one
-- row read, no join). The two are kept in lockstep by the store: an update
-- appends a policy_versions row AND bumps policies.(version, definition).

CREATE TABLE policies (
    id           TEXT PRIMARY KEY,
    workspace_id TEXT NOT NULL REFERENCES workspaces (id) ON DELETE RESTRICT,
    name         TEXT NOT NULL,
    -- The policy kind — fixes the shape of `definition` (see
    -- meridian_store::policy::PolicyDefinition).
    kind         TEXT NOT NULL
        CHECK (kind IN ('row_filter', 'column_mask', 'abac')),
    -- Current version, monotonic, starts at 1, +1 per update.
    version      INTEGER NOT NULL DEFAULT 1 CHECK (version >= 1),
    -- Whether the policy is in force. A disabled policy is retained (and still
    -- resolvable for dry-run/coverage) but excluded from enforcement.
    enabled      BOOLEAN NOT NULL DEFAULT TRUE,
    -- Typed jsonb owned by the Rust layer (RowFilter | ColumnMask | AbacRule).
    definition   JSONB NOT NULL,
    -- Audit string of the creating principal.
    created_by   TEXT NOT NULL,
    created_at   TIMESTAMPTZ NOT NULL DEFAULT now(),
    updated_at   TIMESTAMPTZ NOT NULL DEFAULT now(),
    UNIQUE (workspace_id, name)
);

CREATE INDEX policies_workspace_kind_idx ON policies (workspace_id, kind);

-- ----------------------------------------------------------------------------
-- policy_versions: append-only per-version history (audit + rollback).
--
-- One row per (policy, version). The current policies.version always has a
-- matching row here; older rows are never mutated or deleted (rollback creates
-- a NEW version whose definition is copied from an old one — history stays
-- append-only, matching the audit-log discipline).

CREATE TABLE policy_versions (
    policy_id   TEXT NOT NULL REFERENCES policies (id) ON DELETE CASCADE,
    version     INTEGER NOT NULL CHECK (version >= 1),
    kind        TEXT NOT NULL
        CHECK (kind IN ('row_filter', 'column_mask', 'abac')),
    enabled     BOOLEAN NOT NULL,
    definition  JSONB NOT NULL,
    -- Audit string of the principal who created this version.
    created_by  TEXT NOT NULL,
    created_at  TIMESTAMPTZ NOT NULL DEFAULT now(),
    PRIMARY KEY (policy_id, version)
);

-- ----------------------------------------------------------------------------
-- policy_bindings: attach a policy to a securable OR a tag (XOR).
--
-- A binding names its target polymorphically. Exactly one of
-- (securable_type + securable_id) or tag_id is set:
--   * securable binding: securable_type IN ('table','namespace'), securable_id
--     is that securable's id (NOT an FK, like grants) — a namespace binding
--     applies to everything it contains (resolved in code).
--   * tag binding: tag_id references a tag; the policy applies wherever the tag
--     is assigned (to any table/namespace/column).

CREATE TABLE policy_bindings (
    id             TEXT PRIMARY KEY,
    workspace_id   TEXT NOT NULL REFERENCES workspaces (id) ON DELETE RESTRICT,
    policy_id      TEXT NOT NULL REFERENCES policies (id) ON DELETE CASCADE,
    -- Securable target (XOR with tag_id).
    securable_type TEXT
        CHECK (securable_type IS NULL OR securable_type IN ('table', 'namespace')),
    securable_id   TEXT,
    -- Tag target (XOR with the securable columns). Real FK: tags are a table.
    tag_id         TEXT REFERENCES tags (id) ON DELETE CASCADE,
    -- Audit string of the principal who created the binding.
    bound_by       TEXT NOT NULL,
    created_at     TIMESTAMPTZ NOT NULL DEFAULT now(),
    -- Exactly one target: a securable (both columns set) XOR a tag.
    CHECK (
        ((securable_type IS NOT NULL AND securable_id IS NOT NULL) AND tag_id IS NULL)
        OR
        ((securable_type IS NULL AND securable_id IS NULL) AND tag_id IS NOT NULL)
    )
);

-- One binding per (policy, target). COALESCE folds the XOR'd target columns
-- into the key so re-binding the same policy to the same target is a conflict.
CREATE UNIQUE INDEX policy_bindings_unique_idx ON policy_bindings
    (policy_id, COALESCE(securable_type, ''), COALESCE(securable_id, ''),
     COALESCE(tag_id, ''));

-- Resolution reads bindings two ways: by target securable (direct bindings on
-- this table/namespace) and by tag (tag-derived bindings). Index both.
CREATE INDEX policy_bindings_securable_idx
    ON policy_bindings (securable_type, securable_id)
    WHERE securable_type IS NOT NULL;
CREATE INDEX policy_bindings_tag_id_idx
    ON policy_bindings (tag_id) WHERE tag_id IS NOT NULL;
CREATE INDEX policy_bindings_policy_id_idx ON policy_bindings (policy_id);

-- ----------------------------------------------------------------------------
-- access_requests: D-F4 request-access object (data model only here).
--
-- A principal asks for one privilege on one securable, with a stated purpose
-- and a requested TTL (seconds). It moves pending -> approved | denied |
-- expired. The decision (decided_by, reason, decided_at) is recorded on the
-- same row. The routing/approval/auto-expiry workflow is a later wave; this
-- table exists so the object is stable and auditable from the start.

CREATE TABLE access_requests (
    id             TEXT PRIMARY KEY,
    workspace_id   TEXT NOT NULL REFERENCES workspaces (id) ON DELETE RESTRICT,
    -- Audit string of the requesting principal (e.g. user:alice@example.com).
    principal      TEXT NOT NULL,
    -- Securable the access is requested on (polymorphic, like grants). Mirrors
    -- the RBAC securable set so an approval maps cleanly onto a grant later.
    securable_type TEXT NOT NULL
        CHECK (securable_type IN ('warehouse', 'namespace', 'table', 'view')),
    securable_id   TEXT NOT NULL,
    -- The privilege being requested (RBAC wire form, e.g. READ).
    privilege      TEXT NOT NULL,
    -- Free-text declared purpose (purpose-based access, D-F1).
    purpose        TEXT NOT NULL,
    -- Requested lifetime of the grant, in seconds; NULL means no expiry asked.
    ttl_seconds    BIGINT CHECK (ttl_seconds IS NULL OR ttl_seconds > 0),
    state          TEXT NOT NULL DEFAULT 'pending'
        CHECK (state IN ('pending', 'approved', 'denied', 'expired')),
    -- Decision fields, set when the request leaves 'pending'.
    decided_by     TEXT,
    reason         TEXT,
    decided_at     TIMESTAMPTZ,
    created_at     TIMESTAMPTZ NOT NULL DEFAULT now(),
    -- A decision (approved/denied) records who decided; pending/expired do not
    -- require a decider (expiry is automatic).
    CHECK (
        (state IN ('approved', 'denied')) = (decided_by IS NOT NULL AND decided_at IS NOT NULL)
    )
);

CREATE INDEX access_requests_workspace_state_idx
    ON access_requests (workspace_id, state);
CREATE INDEX access_requests_principal_idx ON access_requests (principal);
CREATE INDEX access_requests_securable_idx
    ON access_requests (securable_type, securable_id);
