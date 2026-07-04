-- 0025_branching: catalog-level branches & tags (Pillar K, K-F1/K-F2/K-F3).
--
-- Append-only: this file only adds to the existing schema; nothing is
-- rewritten. See docs/design/branching.md for the full design and the
-- commit-invariant preservation argument.
--
-- A catalog branch is a NAMED OVERLAY of the per-table pointer map. Creating a
-- branch allocates no per-table state (zero-copy); a table diverges onto a
-- branch only when a commit lands on it there, at which point a
-- branch_table_pointers row is written and advanced by the SAME compare-and-set
-- discipline main uses (commit-protocol.md §2). A tag is an immutable named
-- pointer set frozen at creation time.

-- ----------------------------------------------------------------------------
-- The branch/tag registry. Branches and tags share one name namespace per
-- workspace (a branch and a tag cannot collide), matching Iceberg ref
-- semantics and letting `warehouse@<name>` resolve either.

CREATE TABLE catalog_branches (
    id             TEXT PRIMARY KEY,
    workspace_id   TEXT NOT NULL REFERENCES workspaces (id) ON DELETE RESTRICT,
    name           TEXT NOT NULL,
    -- 'branch' (mutable, commit target) or 'tag' (immutable, read-only).
    kind           TEXT NOT NULL DEFAULT 'branch'
                       CHECK (kind IN ('branch', 'tag')),
    -- The ref this diverged from: 'main' or another branch name. Recorded for
    -- diff/merge base resolution and for audit.
    base_ref       TEXT NOT NULL DEFAULT 'main',
    -- Resolved base branch, when base_ref names a branch (not 'main').
    base_branch_id TEXT REFERENCES catalog_branches (id) ON DELETE SET NULL,
    -- 'open' | 'merged' | 'deleted' for branches; tags are always 'open'.
    state          TEXT NOT NULL DEFAULT 'open'
                       CHECK (state IN ('open', 'merged', 'deleted')),
    -- true = the branch/tag spans every namespace in the workspace; false =
    -- only the namespaces in branch_namespaces.
    scope_all      BOOLEAN NOT NULL DEFAULT TRUE,
    -- Ephemeral PR branches (K-F3): a sweeper deletes branches past this.
    -- NULL = permanent.
    expires_at     TIMESTAMPTZ,
    created_by     TEXT NOT NULL,
    created_at     TIMESTAMPTZ NOT NULL DEFAULT now(),
    updated_at     TIMESTAMPTZ NOT NULL DEFAULT now()
);

-- One name namespace per workspace across branches AND tags.
CREATE UNIQUE INDEX catalog_branches_name_unique
    ON catalog_branches (workspace_id, name);

-- 'main' is implicit (the base table pointer) and must never be created as a
-- branch/tag row — it would shadow the real main.
ALTER TABLE catalog_branches
    ADD CONSTRAINT catalog_branches_name_not_main CHECK (name <> 'main');

CREATE INDEX catalog_branches_workspace_idx ON catalog_branches (workspace_id);
CREATE INDEX catalog_branches_expires_idx
    ON catalog_branches (expires_at)
    WHERE expires_at IS NOT NULL AND state = 'open';

-- ----------------------------------------------------------------------------
-- Namespace scoping for branches with scope_all = false. A branch commit is
-- only accepted for tables under one of these namespaces (or any namespace
-- when scope_all).

CREATE TABLE branch_namespaces (
    branch_id    TEXT NOT NULL REFERENCES catalog_branches (id) ON DELETE CASCADE,
    namespace_id TEXT NOT NULL REFERENCES namespaces (id) ON DELETE CASCADE,
    PRIMARY KEY (branch_id, namespace_id)
);

-- ----------------------------------------------------------------------------
-- The divergent pointers — the whole point. A row exists only for a table that
-- has diverged on the branch; an absent row means the table falls through to
-- main (zero-copy). pointer_version is a BRANCH-LOCAL compare-and-set guard
-- (starts at 0 on first divergence, independent of main's counter).

CREATE TABLE branch_table_pointers (
    branch_id            TEXT NOT NULL REFERENCES catalog_branches (id) ON DELETE CASCADE,
    table_id             TEXT NOT NULL REFERENCES tables (id) ON DELETE CASCADE,
    -- The branch's current metadata.json for this table.
    metadata_location    TEXT NOT NULL,
    -- Branch-local pointer version; +1 per committed branch swap (the CAS
    -- guard for branch commits, the same role tables.pointer_version plays on
    -- main).
    pointer_version      BIGINT NOT NULL DEFAULT 0 CHECK (pointer_version >= 0),
    -- main's tables.pointer_version at the moment this table first diverged
    -- onto the branch — the three-way merge base used by conflict detection
    -- (branching.md §6).
    base_pointer_version BIGINT NOT NULL CHECK (base_pointer_version >= 0),
    -- The location the current branch commit replaced, if any.
    previous_metadata_location TEXT,
    created_at           TIMESTAMPTZ NOT NULL DEFAULT now(),
    updated_at           TIMESTAMPTZ NOT NULL DEFAULT now(),
    PRIMARY KEY (branch_id, table_id)
);

CREATE INDEX branch_table_pointers_branch_idx ON branch_table_pointers (branch_id);
CREATE INDEX branch_table_pointers_table_idx ON branch_table_pointers (table_id);

-- ----------------------------------------------------------------------------
-- Tags: the frozen pointer set for a tag (kind='tag' in catalog_branches). One
-- row per table pinned at tag-creation time. Immutable — no commit path writes
-- these after creation.

CREATE TABLE catalog_tags (
    tag_id            TEXT NOT NULL REFERENCES catalog_branches (id) ON DELETE CASCADE,
    table_id          TEXT NOT NULL REFERENCES tables (id) ON DELETE CASCADE,
    -- The frozen metadata.json pointer (from main or a source branch at
    -- creation time).
    metadata_location TEXT NOT NULL,
    -- The current snapshot pinned, for diff/report (NULL for an empty table).
    snapshot_id       BIGINT,
    created_at        TIMESTAMPTZ NOT NULL DEFAULT now(),
    PRIMARY KEY (tag_id, table_id)
);

CREATE INDEX catalog_tags_tag_idx ON catalog_tags (tag_id);
