-- 0006_views: the view pointer model for the Iceberg REST view surface.
--
-- Append-only: this file only adds to the existing schema.
--
-- Views mirror tables structurally: a row is the commit-protocol pointer
-- (metadata_location, pointer_version) onto immutable view metadata.json
-- files, swapped by compare-and-set exactly like table pointers
-- (docs/design/commit-protocol.md §2). Views have no snapshot index and no
-- format_version column: the view spec defines exactly one format version.
--
-- Tables and views share one name space per namespace (the REST spec's
-- create/rename endpoints 409 when "the identifier already exists as a
-- table or view"). Postgres cannot express a cross-table unique constraint,
-- so the views side of that invariant is enforced in the application layer
-- (meridian_store::view checks `tables` inside its create/rename
-- transactions); see docs/api-status.md for the enforcement status.

CREATE TABLE views (
    id                TEXT PRIMARY KEY,
    workspace_id      TEXT NOT NULL REFERENCES workspaces (id) ON DELETE RESTRICT,
    namespace_id      TEXT NOT NULL REFERENCES namespaces (id) ON DELETE RESTRICT,
    name              TEXT NOT NULL,
    view_uuid         TEXT NOT NULL,
    metadata_location TEXT,
    pointer_version   BIGINT NOT NULL DEFAULT 0 CHECK (pointer_version >= 0),
    properties        JSONB NOT NULL DEFAULT '{}'::jsonb,
    created_at        TIMESTAMPTZ NOT NULL DEFAULT now(),
    updated_at        TIMESTAMPTZ NOT NULL DEFAULT now(),
    UNIQUE (namespace_id, name)
);

-- The Iceberg view UUID is stable for the lifetime of the view and unique
-- within the deployment (canonical hyphenated form) — same rationale as
-- tables_table_uuid_unique.
CREATE UNIQUE INDEX views_view_uuid_unique ON views (view_uuid);

CREATE INDEX views_workspace_id_idx ON views (workspace_id);
CREATE INDEX views_namespace_id_idx ON views (namespace_id);

-- View names travel in URL path segments; the 0x1F unit separator is the
-- wire-level namespace separator and empty names are unaddressable.
ALTER TABLE views
    ADD CONSTRAINT views_name_valid CHECK (
        name <> '' AND position(chr(31) IN name) = 0
    );
