-- 0001_init: core catalog entities, outbox, idempotency, audit log.
--
-- Conventions:
--   * All IDs are ULIDs stored as TEXT (26-char Crockford base32).
--   * All timestamps are UTC (timestamptz).
--   * Rows are workspace-scoped via FK wherever applicable.
--   * updated_at is maintained by the application layer, not triggers.

CREATE TABLE organizations (
    id         TEXT PRIMARY KEY,
    name       TEXT NOT NULL UNIQUE,
    created_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    updated_at TIMESTAMPTZ NOT NULL DEFAULT now()
);

CREATE TABLE workspaces (
    id         TEXT PRIMARY KEY,
    org_id     TEXT NOT NULL REFERENCES organizations (id) ON DELETE RESTRICT,
    name       TEXT NOT NULL,
    created_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    updated_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    UNIQUE (org_id, name)
);

CREATE INDEX workspaces_org_id_idx ON workspaces (org_id);

-- A warehouse is a storage root (bucket/prefix) plus its access configuration.
-- Storage credentials/secrets never live in this table in plaintext; the
-- storage_config JSONB holds non-secret settings only. Secret material will be
-- handled by a dedicated vault/KMS integration (M2, credential vending).
CREATE TABLE warehouses (
    id             TEXT PRIMARY KEY,
    workspace_id   TEXT NOT NULL REFERENCES workspaces (id) ON DELETE RESTRICT,
    name           TEXT NOT NULL,
    storage_root   TEXT NOT NULL,
    storage_config JSONB NOT NULL DEFAULT '{}'::jsonb,
    created_at     TIMESTAMPTZ NOT NULL DEFAULT now(),
    updated_at     TIMESTAMPTZ NOT NULL DEFAULT now(),
    UNIQUE (workspace_id, name)
);

CREATE INDEX warehouses_workspace_id_idx ON warehouses (workspace_id);

CREATE TABLE catalogs (
    id           TEXT PRIMARY KEY,
    workspace_id TEXT NOT NULL REFERENCES workspaces (id) ON DELETE RESTRICT,
    warehouse_id TEXT NOT NULL REFERENCES warehouses (id) ON DELETE RESTRICT,
    name         TEXT NOT NULL,
    properties   JSONB NOT NULL DEFAULT '{}'::jsonb,
    created_at   TIMESTAMPTZ NOT NULL DEFAULT now(),
    updated_at   TIMESTAMPTZ NOT NULL DEFAULT now(),
    UNIQUE (workspace_id, name)
);

CREATE INDEX catalogs_workspace_id_idx ON catalogs (workspace_id);
CREATE INDEX catalogs_warehouse_id_idx ON catalogs (warehouse_id);

-- Namespaces are multi-level (e.g. "analytics.daily"). The full path is
-- stored as a TEXT[] of levels; a GIN index supports containment queries
-- (find namespaces containing a level) without an ltree dependency.
CREATE TABLE namespaces (
    id           TEXT PRIMARY KEY,
    workspace_id TEXT NOT NULL REFERENCES workspaces (id) ON DELETE RESTRICT,
    catalog_id   TEXT NOT NULL REFERENCES catalogs (id) ON DELETE RESTRICT,
    levels       TEXT[] NOT NULL CHECK (cardinality(levels) > 0),
    properties   JSONB NOT NULL DEFAULT '{}'::jsonb,
    created_at   TIMESTAMPTZ NOT NULL DEFAULT now(),
    updated_at   TIMESTAMPTZ NOT NULL DEFAULT now(),
    UNIQUE (catalog_id, levels)
);

CREATE INDEX namespaces_workspace_id_idx ON namespaces (workspace_id);
CREATE INDEX namespaces_levels_gin ON namespaces USING GIN (levels);

-- Iceberg tables. metadata_location is the current metadata.json pointer;
-- NULL only while a table is being staged/created. The pointer swap on commit
-- is the correctness-critical path (compare-and-set inside a transaction).
CREATE TABLE tables (
    id                TEXT PRIMARY KEY,
    workspace_id      TEXT NOT NULL REFERENCES workspaces (id) ON DELETE RESTRICT,
    namespace_id      TEXT NOT NULL REFERENCES namespaces (id) ON DELETE RESTRICT,
    name              TEXT NOT NULL,
    metadata_location TEXT,
    format_version    SMALLINT NOT NULL DEFAULT 2 CHECK (format_version IN (1, 2, 3)),
    properties        JSONB NOT NULL DEFAULT '{}'::jsonb,
    created_at        TIMESTAMPTZ NOT NULL DEFAULT now(),
    updated_at        TIMESTAMPTZ NOT NULL DEFAULT now(),
    UNIQUE (namespace_id, name)
);

CREATE INDEX tables_workspace_id_idx ON tables (workspace_id);
CREATE INDEX tables_namespace_id_idx ON tables (namespace_id);

-- Transactional outbox: events are inserted in the same transaction as the
-- state change they describe, then published asynchronously by a relay
-- (M2, events). workspace_id is NULLable because some events (e.g.
-- organization lifecycle) are not workspace-scoped.
CREATE TABLE events_outbox (
    id           TEXT PRIMARY KEY,
    workspace_id TEXT REFERENCES workspaces (id) ON DELETE RESTRICT,
    aggregate    TEXT NOT NULL,
    event_type   TEXT NOT NULL,
    payload      JSONB NOT NULL,
    created_at   TIMESTAMPTZ NOT NULL DEFAULT now(),
    published_at TIMESTAMPTZ
);

CREATE INDEX events_outbox_unpublished_idx
    ON events_outbox (created_at)
    WHERE published_at IS NULL;

-- Client-provided idempotency keys for mutating API calls. A repeated key
-- within a workspace replays the stored response instead of re-executing.
-- Wiring into request handling lands with the first mutating endpoints (M1).
CREATE TABLE idempotency_keys (
    workspace_id    TEXT NOT NULL REFERENCES workspaces (id) ON DELETE RESTRICT,
    idempotency_key TEXT NOT NULL,
    request_hash    TEXT NOT NULL,
    response_status SMALLINT,
    response_body   JSONB,
    created_at      TIMESTAMPTZ NOT NULL DEFAULT now(),
    expires_at      TIMESTAMPTZ NOT NULL,
    PRIMARY KEY (workspace_id, idempotency_key)
);

CREATE INDEX idempotency_keys_expires_at_idx ON idempotency_keys (expires_at);

-- Append-only, hash-chained audit log. seq gives the chain a total order;
-- hash = sha256(prev_hash || canonical_json(entry)). Verification walks seq
-- order and recomputes. UPDATE/DELETE are rejected by trigger: auditability
-- beats convenience, always.
CREATE TABLE audit_log (
    seq          BIGINT GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    id           TEXT NOT NULL UNIQUE,
    workspace_id TEXT REFERENCES workspaces (id) ON DELETE RESTRICT,
    occurred_at  TIMESTAMPTZ NOT NULL DEFAULT now(),
    principal    TEXT NOT NULL,
    action       TEXT NOT NULL,
    resource     TEXT NOT NULL,
    details      JSONB NOT NULL DEFAULT '{}'::jsonb,
    prev_hash    TEXT,
    hash         TEXT NOT NULL
);

CREATE INDEX audit_log_workspace_id_idx ON audit_log (workspace_id);
CREATE INDEX audit_log_occurred_at_idx ON audit_log (occurred_at);

CREATE FUNCTION audit_log_append_only() RETURNS trigger
    LANGUAGE plpgsql
    AS $$
BEGIN
    RAISE EXCEPTION 'audit_log is append-only; % is not permitted', TG_OP;
END;
$$;

CREATE TRIGGER audit_log_no_mutation
    BEFORE UPDATE OR DELETE ON audit_log
    FOR EACH ROW
    EXECUTE FUNCTION audit_log_append_only();
