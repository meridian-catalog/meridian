-- 0003_tables_commit: the table pointer model for the commit protocol (M1),
-- the snapshot write-through index, and metrics-report capture.
--
-- Append-only: this file only adds to the 0001/0002 schema.
--
-- No rows exist in `tables` before this migration (nothing wrote them in
-- M0/M1-namespaces), so adding NOT NULL columns without a backfill is safe —
-- the same reasoning migration 0002 applied to namespaces.

-- ----------------------------------------------------------------------------
-- The pointer: (metadata_location, pointer_version) per the commit protocol
-- (docs/design/commit-protocol.md §2). pointer_version increases by exactly 1
-- per successful commit and is the compare-and-set guard; it also backs the
-- ETag on table load responses.

ALTER TABLE tables
    ADD COLUMN table_uuid TEXT NOT NULL;

ALTER TABLE tables
    ADD COLUMN pointer_version BIGINT NOT NULL DEFAULT 0
        CHECK (pointer_version >= 0);

ALTER TABLE tables
    ADD COLUMN previous_metadata_location TEXT;

-- The Iceberg table UUID is stable for the lifetime of the table and unique
-- within the deployment (canonical hyphenated form).
CREATE UNIQUE INDEX tables_table_uuid_unique ON tables (table_uuid);

-- Table names travel in URL path segments; the 0x1F unit separator is the
-- wire-level namespace separator and empty names are unaddressable.
ALTER TABLE tables
    ADD CONSTRAINT tables_name_valid CHECK (
        name <> '' AND position(chr(31) IN name) = 0
    );

-- ----------------------------------------------------------------------------
-- Snapshot write-through index (ADR 003): the structured content of the new
-- metadata is persisted in the same transaction as the pointer swap. This is
-- the first slice of the metadata-forward index; schemas/partition/statistics
-- tables land with the pillar that consumes them. Rows are replaced per
-- commit from the new metadata's retained snapshot set (snapshot expiry in a
-- commit removes the corresponding index rows).

CREATE TABLE table_snapshots (
    table_id        TEXT NOT NULL REFERENCES tables (id) ON DELETE CASCADE,
    snapshot_id     BIGINT NOT NULL,
    parent_snapshot_id BIGINT,
    sequence_number BIGINT,
    timestamp_ms    BIGINT NOT NULL,
    manifest_list   TEXT,
    operation       TEXT,
    summary         JSONB NOT NULL DEFAULT '{}'::jsonb,
    is_current      BOOLEAN NOT NULL DEFAULT FALSE,
    PRIMARY KEY (table_id, snapshot_id)
);

CREATE INDEX table_snapshots_table_id_idx ON table_snapshots (table_id);

-- ----------------------------------------------------------------------------
-- Raw metrics reports (POST .../tables/{table}/metrics). Stored verbatim:
-- cheap to capture now, the observability pillar mines them later.
--
-- table_id is deliberately NOT a foreign key: reports must survive a table
-- drop (they are historical evidence, not live state), and a FK would either
-- block the drop (RESTRICT) or destroy the evidence (CASCADE). The table
-- identity at report time is denormalized alongside for exactly that reason.

CREATE TABLE metrics_reports (
    id           TEXT PRIMARY KEY,
    workspace_id TEXT NOT NULL REFERENCES workspaces (id) ON DELETE RESTRICT,
    table_id     TEXT NOT NULL,
    table_ident  TEXT NOT NULL,
    report_type  TEXT,
    report       JSONB NOT NULL,
    received_at  TIMESTAMPTZ NOT NULL DEFAULT now()
);

CREATE INDEX metrics_reports_table_id_idx ON metrics_reports (table_id);
CREATE INDEX metrics_reports_received_at_idx ON metrics_reports (received_at);
