-- 0015_federation_foreign_assets: make mirrored assets first-class to the
-- read-side features (Pillar B, B-F1 inbound mirrors).
--
-- Append-only: this file only adds to the 0001-0014 schema; nothing is
-- rewritten. It extends the federation model from 0014_federation_mirrors
-- (catalog_mirrors / mirror_assets / mirror_sync_runs) — it does NOT add a
-- second mirror-config table.
--
-- Why this exists (see docs/adr/008-federation-inbound-mirrors.md):
--   0014's `mirror_assets` is a flat per-asset index that powers the sprawl
--   summary (location/ownership roll-ups). It is deliberately NOT what
--   Meridian's read-side features query: search (0010 triggers over `tables`),
--   the health model, and the write-through snapshot index all operate on the
--   native `tables` / `namespaces` / `table_snapshots` rows. For a mirrored
--   table to be searchable and health-scorable, it must exist as a native row.
--
--   So the sync engine materializes each mirrored table as an ORDINARY row in
--   the existing `tables` table (and its namespace as an ordinary `namespaces`
--   row), tagged with `mirror_id`. That single column is the whole
--   native-vs-foreign distinction:
--     * mirror_id IS NULL  -> native asset, writable (the normal commit path)
--     * mirror_id IS NOT NULL -> foreign asset, READ-ONLY (writes rejected at
--       the commit boundary; see the commit_table / commit_transaction guards)
--
--   Reusing the native tables is what makes "every read-side feature works on
--   foreign assets immediately" true without forking any read query.

-- ----------------------------------------------------------------------------
-- mirror_id on the asset tables.
--
-- Nullable, referencing catalog_mirrors. ON DELETE CASCADE so deleting a mirror
-- removes its foreign namespaces and tables with it. The store's mirror-delete
-- path deletes foreign tables before foreign namespaces (so the
-- namespaces->tables RESTRICT FK from 0001 never blocks); this CASCADE is the
-- backstop that guarantees no orphan foreign rows survive a mirror deletion by
-- any path.

ALTER TABLE namespaces
    ADD COLUMN mirror_id TEXT REFERENCES catalog_mirrors (id) ON DELETE CASCADE;

ALTER TABLE tables
    ADD COLUMN mirror_id TEXT REFERENCES catalog_mirrors (id) ON DELETE CASCADE;

-- The sync engine diffs a mirror's existing foreign rows against the source on
-- every run (to add/update/remove), and the commit-boundary guard checks a
-- single row's foreignness. Partial indexes keep the per-mirror scans cheap
-- and add nothing to the native (mirror_id IS NULL) hot paths.
CREATE INDEX namespaces_mirror_id_idx ON namespaces (mirror_id) WHERE mirror_id IS NOT NULL;
CREATE INDEX tables_mirror_id_idx ON tables (mirror_id) WHERE mirror_id IS NOT NULL;

-- ----------------------------------------------------------------------------
-- Table-UUID uniqueness applies to NATIVE tables only.
--
-- Migration 0003 created `tables_table_uuid_unique` as a global unique index:
-- one live table per Iceberg table-uuid in the deployment. That invariant is
-- correct for native tables (a metadata file can only be adopted once). It is
-- WRONG for foreign assets: a mirrored table legitimately carries the *same*
-- table-uuid as its source (it IS the same table), and the same source table
-- may be mirrored by more than one mirror, or exist both natively and mirrored.
-- Enforcing global uniqueness would make the second registration of any such
-- uuid fail.
--
-- Replace the global index with a partial one scoped to native rows
-- (`mirror_id IS NULL`). Native tables keep the exact one-live-table-per-uuid
-- guarantee; foreign tables are exempt (their uniqueness is instead the
-- `(namespace_id, name)` key they already share with every table, scoped to
-- their mirror-private warehouse).
DROP INDEX tables_table_uuid_unique;
CREATE UNIQUE INDEX tables_table_uuid_unique ON tables (table_uuid) WHERE mirror_id IS NULL;
