-- 0002_tenancy_namespaces: single-workspace OSS seeding + warehouse-scoped
-- namespaces for the IRC namespace surface (M1).
--
-- Append-only: this file only adds to the 0001 schema; nothing is rewritten.

-- ----------------------------------------------------------------------------
-- Single-workspace seeding.
--
-- The OSS deployment runs with one implicit tenant: organization "default"
-- containing workspace "default". Their IDs are fixed, well-known ULIDs
-- (Ulid(0) and Ulid(1)) so application code can scope queries without a
-- lookup; see meridian_store::tenancy. The inserts are idempotent so the
-- migration is safe against pre-seeded databases (e.g. test fixtures).

INSERT INTO organizations (id, name)
VALUES ('00000000000000000000000000', 'default')
ON CONFLICT (name) DO NOTHING;

INSERT INTO workspaces (id, org_id, name)
SELECT '00000000000000000000000001', o.id, 'default'
FROM organizations o
WHERE o.name = 'default'
ON CONFLICT (org_id, name) DO NOTHING;

-- ----------------------------------------------------------------------------
-- Namespaces hang off warehouses directly.
--
-- 0001 modeled namespaces under catalogs. M1 maps one warehouse to one
-- Iceberg REST {prefix} (the warehouse name), and namespaces live directly
-- under that warehouse; the catalog layer is deferred until something needs
-- it. catalog_id is kept (nullable) for that future layering rather than
-- dropped, honouring append-only migration discipline.
--
-- No namespace rows can exist before this migration (nothing wrote them in
-- M0), so adding a NOT NULL column without a backfill is safe.

ALTER TABLE namespaces
    ALTER COLUMN catalog_id DROP NOT NULL;

ALTER TABLE namespaces
    ADD COLUMN warehouse_id TEXT NOT NULL
        REFERENCES warehouses (id) ON DELETE RESTRICT;

ALTER TABLE namespaces
    ADD CONSTRAINT namespaces_warehouse_levels_unique UNIQUE (warehouse_id, levels);

CREATE INDEX namespaces_warehouse_id_idx ON namespaces (warehouse_id);

-- Levels must be non-empty strings and must not contain the 0x1F unit
-- separator (it is the wire-level level separator in the REST spec, so a
-- level containing it would be unaddressable).
ALTER TABLE namespaces
    ADD CONSTRAINT namespaces_levels_valid CHECK (
        array_position(levels, NULL) IS NULL
        AND array_position(levels, '') IS NULL
        AND position(chr(31) IN array_to_string(levels, '')) = 0
    );
