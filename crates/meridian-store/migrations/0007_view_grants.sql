-- 0007_view_grants: views become a first-class grant securable.
--
-- The view surface (0006) shipped in the same development wave as RBAC
-- (0005); this migration closes the gap by admitting 'view' into the
-- grants securable-type CHECK so grants can attach directly to a view
-- row, exactly like tables. Hierarchy inheritance is unchanged: grants on
-- a warehouse or namespace already cover the views they contain (resolved
-- at check time in meridian_store::rbac, never materialized as rows).
--
-- The privilege set does not change: CREATE_VIEW existed since 0005, and
-- the table-native privileges (READ, WRITE, COMMIT, DROP) apply to view
-- securables the same way they apply to tables.

ALTER TABLE grants
    DROP CONSTRAINT grants_securable_type_check;

ALTER TABLE grants
    ADD CONSTRAINT grants_securable_type_check
    CHECK (securable_type IN ('warehouse', 'namespace', 'table', 'view'));
