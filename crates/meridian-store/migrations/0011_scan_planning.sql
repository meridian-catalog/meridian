-- 0011_scan_planning: server-side scan planning (IRC planTableScan /
-- fetchPlanningResult / cancelPlanning / fetchScanTasks) — plan state,
-- persisted result pages, and the cross-pod manifest byte cache.
-- See docs/design/scan-planning.md.
--
-- Append-only: this file only adds to the existing schema.
--
-- ## scan_plans / scan_plan_pages
--
-- A plan row exists for every planning operation, synchronous or
-- asynchronous — the spec's completed planTableScan response *requires* a
-- plan-id, and cancelPlanning must work on it. Result handling splits by
-- `result_mode`:
--
--   * `paged` (asynchronous plans): pages are **persisted** once by the
--     worker — a fetchScanTasks call is a single primary-key read, results
--     survive a pod crash between submit and fetch, and pagination is
--     trivially deterministic. Cost: Postgres storage for large plans,
--     bounded by the plan TTL (`expires_at`, default one hour) and the
--     expiry sweep (pages cascade).
--   * `inline` (synchronous plans): **no pages are stored** — the result
--     already went out in the planTableScan response body, and a later
--     fetchPlanningResult re-plans from the stored `request` pinned to
--     `snapshot_id` (deterministic on immutable metadata, warm in the
--     manifest cache). Persisting would write multi-megabyte payloads on
--     the synchronous hot path for a result that is usually never
--     re-fetched.
--
-- Plans do not outlive their table or warehouse (FK CASCADE): a dropped
-- table's plan-ids intentionally become 404s.
--
-- `page_token` is the opaque `PlanTask` string handed to clients. Tokens
-- are random ULIDs — capability-style handles resolved by lookup, never
-- parsed — but token possession is NOT authorization: every fetch
-- re-checks RBAC on the owning table.
--
-- ## manifest_cache
--
-- Raw manifest-list/manifest *file bytes* keyed by storage location,
-- warehouse-scoped. Iceberg metadata files are immutable at a given path,
-- so entries never need invalidation ("cache forever"); the budget sweep
-- evicts least-recently-accessed rows beyond the configured total size.
-- Raw bytes rather than a parsed form: no serialization-versioning of a
-- Rust struct in the database, and each pod's in-process LRU holds the
-- parsed form anyway (this table exists so a *cold* pod skips the
-- object-storage round trip, not the parse). `accessed_at` is bumped
-- lazily (at most once per five minutes per row) to keep the read path
-- from writing on every hit.

CREATE TABLE scan_plans (
    id           TEXT PRIMARY KEY,
    workspace_id TEXT NOT NULL REFERENCES workspaces (id) ON DELETE RESTRICT,
    warehouse_id TEXT NOT NULL REFERENCES warehouses (id) ON DELETE CASCADE,
    table_id     TEXT NOT NULL REFERENCES tables (id) ON DELETE CASCADE,
    snapshot_id  BIGINT NOT NULL,
    status       TEXT NOT NULL
        CHECK (status IN ('submitted', 'completed', 'failed', 'cancelled')),
    -- 'inline': the whole result is one page, returned in-body (small
    -- tables). 'paged': the completed result lists plan-task tokens and
    -- clients fetch pages via fetchScanTasks.
    result_mode  TEXT NOT NULL CHECK (result_mode IN ('inline', 'paged')),
    created_by   TEXT NOT NULL,
    -- The PlanTableScanRequest as received (debugging + audit detail).
    request      JSONB NOT NULL,
    -- IcebergErrorResponse payload for status = 'failed'.
    error        JSONB,
    -- Planning outcome counters (manifests read/pruned, files matched,
    -- cache hit rates); serialized into events and logs.
    summary      JSONB,
    created_at   TIMESTAMPTZ NOT NULL DEFAULT now(),
    updated_at   TIMESTAMPTZ NOT NULL DEFAULT now(),
    expires_at   TIMESTAMPTZ NOT NULL
);

CREATE INDEX scan_plans_expires_at_idx ON scan_plans (expires_at);
CREATE INDEX scan_plans_table_id_idx ON scan_plans (table_id);

CREATE TABLE scan_plan_pages (
    plan_id    TEXT NOT NULL REFERENCES scan_plans (id) ON DELETE CASCADE,
    page_index INTEGER NOT NULL CHECK (page_index >= 0),
    page_token TEXT NOT NULL UNIQUE,
    -- A complete REST `ScanTasks` object: file-scan-tasks plus the
    -- delete-files they reference, with page-local indices.
    payload    JSONB NOT NULL,
    PRIMARY KEY (plan_id, page_index)
);

CREATE TABLE manifest_cache (
    warehouse_id  TEXT NOT NULL REFERENCES warehouses (id) ON DELETE CASCADE,
    location      TEXT NOT NULL,
    content       BYTEA NOT NULL,
    content_bytes BIGINT NOT NULL,
    created_at    TIMESTAMPTZ NOT NULL DEFAULT now(),
    accessed_at   TIMESTAMPTZ NOT NULL DEFAULT now(),
    PRIMARY KEY (warehouse_id, location)
);

CREATE INDEX manifest_cache_accessed_at_idx ON manifest_cache (accessed_at);
