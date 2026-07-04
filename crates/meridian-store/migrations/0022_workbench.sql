-- 0022_workbench: the SQL workbench (Pillar L, L-F1) — saved queries and query
-- history. The workbench is an adoption wedge: an in-console SQL editor over
-- governed assets, run on the built-in small-scan executor (zero engine setup,
-- vended-creds path) with the same Pillar-D policies the agent gateway and scan
-- planner enforce. Time-to-first-query is the north star; these two tables give
-- it memory (history) and reuse (saved queries).
--
-- Append-only: this file only adds to the 0001-0021 schema; nothing is
-- rewritten. The served surfaces are the /api/v2/workbench routes and the
-- console Workbench page. Executing a query is a governed *read*, not a
-- mutation, so it does not write an audit_log row here (the read is governed and
-- capped by the executor; agent reads go through the tamper-evident chain, but a
-- human workbench read is an ordinary authenticated query). Recording a query in
-- history and saving/deleting a saved query are ordinary workspace-scoped rows.
--
-- Model:
--
--  * workbench_saved_queries: a named, reusable query a user parks for later
--    (L-F1 "saved queries"). It carries the SQL, the warehouse it targets, an
--    optional default namespace (for resolving bare table names), a description,
--    and the owner (audit string). Names are unique per workspace,
--    case-insensitively. Not tied to a warehouse row by FK: the warehouse is a
--    name the query targets, resolved at run time (and a saved query may outlive
--    a warehouse rename or target a mirror).
--
--  * workbench_query_history: an append-only log of every query run through the
--    workbench, per principal — the SQL, the warehouse, the outcome (ok / error /
--    refused), the row count and scanned bytes actually touched, the wall-clock
--    duration, and any error/refusal message. This is the user's own recent-query
--    list (L-F1 "history"), not an audit surface; it is workspace-scoped and
--    principal-attributed, capped by the API's pagination. It is deliberately
--    separate from audit_log (which is the tamper-evident chain for governed
--    *agent* actions and catalog mutations): a human's ad-hoc SELECT is not a
--    catalog mutation, and history is a convenience the user can prune.

-- ----------------------------------------------------------------------------
-- workbench_saved_queries: named, reusable queries (L-F1).

CREATE TABLE workbench_saved_queries (
    id                TEXT PRIMARY KEY,
    workspace_id      TEXT NOT NULL REFERENCES workspaces (id) ON DELETE RESTRICT,
    -- Query name, unique per workspace (case-insensitive; see the index below).
    name              TEXT NOT NULL,
    -- The SQL to run. Stored verbatim; validated (read-only, size-capped) by the
    -- executor at run time, never trusted as pre-validated.
    sql               TEXT NOT NULL,
    -- The warehouse the query targets (a name resolved at run time). Optional: a
    -- table-free query (SELECT 1) needs none.
    warehouse         TEXT,
    -- The default namespace for resolving bare table names, as a JSONB string
    -- array of levels (e.g. ["sales","eu"]). Empty/absent => bare names are
    -- unresolvable and must be qualified in the SQL.
    default_namespace JSONB NOT NULL DEFAULT '[]'::jsonb,
    -- Free-text description (markdown).
    description       TEXT,
    -- The creating/owning principal, as an audit string (e.g. user:alice@x.com).
    -- Not an FK: owners are external identities rendered as audit strings.
    owner             TEXT NOT NULL,
    created_at        TIMESTAMPTZ NOT NULL DEFAULT now(),
    updated_at        TIMESTAMPTZ NOT NULL DEFAULT now()
);

-- Names are unique per workspace, case-insensitively (a workbench with both
-- "Daily revenue" and "daily revenue" is a bug, not a feature).
CREATE UNIQUE INDEX workbench_saved_queries_workspace_name_unique
    ON workbench_saved_queries (workspace_id, lower(name));
CREATE INDEX workbench_saved_queries_workspace_idx
    ON workbench_saved_queries (workspace_id);

-- ----------------------------------------------------------------------------
-- workbench_query_history: per-principal recent-query log (L-F1).

CREATE TABLE workbench_query_history (
    id             TEXT PRIMARY KEY,
    workspace_id   TEXT NOT NULL REFERENCES workspaces (id) ON DELETE RESTRICT,
    -- The principal who ran the query, as an audit string.
    principal      TEXT NOT NULL,
    -- The SQL that was run (verbatim).
    sql            TEXT NOT NULL,
    -- The warehouse targeted (a name), or NULL for a table-free query.
    warehouse      TEXT,
    -- The outcome: 'ok' (ran), 'error' (bad/oversized SQL or an engine fault),
    -- or 'denied' (a policy denial). A closed set, surfaced verbatim.
    status         TEXT NOT NULL
        CHECK (status IN ('ok', 'error', 'denied')),
    -- Rows returned to the caller (after the result cap), for an 'ok' run.
    row_count      BIGINT,
    -- On-disk bytes the scan read (the manifest-stats estimate == actual for the
    -- small-scan executor), for an 'ok' run.
    bytes_scanned  BIGINT,
    -- Wall-clock duration of the run in milliseconds.
    duration_ms    BIGINT,
    -- The error / refusal / denial message for a non-'ok' run (surfaced verbatim
    -- to the user), else NULL.
    message        TEXT,
    created_at     TIMESTAMPTZ NOT NULL DEFAULT now()
);

-- The user's own recent-query list: newest first, scoped to workspace +
-- principal. A descending id (ULID) is time-ordered, so this index serves the
-- history read directly.
CREATE INDEX workbench_query_history_principal_idx
    ON workbench_query_history (workspace_id, principal, id DESC);
