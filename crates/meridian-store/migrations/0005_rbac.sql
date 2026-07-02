-- 0005_rbac: role-based access control (Pillar A / F8, RBAC only).
--
-- Append-only: this file only adds to the existing schema.
--
-- Model:
--
-- * roles: workspace-scoped, named groupings of principals.
-- * role_bindings: principal <-> role membership.
-- * grants: one privilege on one securable (warehouse | namespace | table)
--   for exactly one grantee — a role XOR a principal.
-- * Privileges are TEXT here; the closed set is defined once in
--   meridian_store::rbac::Privilege and mirrored by the CHECK below.
-- * Hierarchy inheritance (warehouse ⊃ namespace ⊃ table, namespace ⊃
--   child namespace) is resolved at check time in meridian_store::rbac,
--   never materialized as rows.
--
-- Built-in roles (seeded below for the default workspace under fixed,
-- well-known ULIDs, like the 0002 tenancy rows):
--
-- * admin: every privilege on every securable of the workspace.
-- * catalog_reader: the read-only privileges (LIST_NAMESPACES, LIST_TABLES,
--   READ) on every securable of the workspace.
--
-- Built-in role semantics are enforced in code
-- (meridian_store::rbac::authorize); the rows exist so bindings and audit
-- entries reference stable identities. Built-in roles cannot be deleted.

CREATE TABLE roles (
    id           TEXT PRIMARY KEY,
    workspace_id TEXT NOT NULL REFERENCES workspaces (id) ON DELETE RESTRICT,
    name         TEXT NOT NULL,
    description  TEXT,
    built_in     BOOLEAN NOT NULL DEFAULT FALSE,
    created_at   TIMESTAMPTZ NOT NULL DEFAULT now(),
    UNIQUE (workspace_id, name)
);

CREATE TABLE role_bindings (
    role_id      TEXT NOT NULL REFERENCES roles (id) ON DELETE CASCADE,
    principal_id TEXT NOT NULL REFERENCES principals (id) ON DELETE CASCADE,
    created_at   TIMESTAMPTZ NOT NULL DEFAULT now(),
    PRIMARY KEY (role_id, principal_id)
);

CREATE INDEX role_bindings_principal_id_idx ON role_bindings (principal_id);

CREATE TABLE grants (
    id             TEXT PRIMARY KEY,
    workspace_id   TEXT NOT NULL REFERENCES workspaces (id) ON DELETE RESTRICT,
    -- Grantee: exactly one of role_id / principal_id (XOR, checked below).
    role_id        TEXT REFERENCES roles (id) ON DELETE CASCADE,
    principal_id   TEXT REFERENCES principals (id) ON DELETE CASCADE,
    securable_type TEXT NOT NULL
        CHECK (securable_type IN ('warehouse', 'namespace', 'table')),
    -- ULID of the securable row. Deliberately NOT a foreign key (it is
    -- polymorphic over three tables). Dropping a securable leaves its
    -- grants behind; they are inert — checks match by id and ULIDs are
    -- never reused. TODO(M2): sweep orphaned grants from the maintenance
    -- worker.
    securable_id   TEXT NOT NULL,
    -- Mirrors meridian_store::rbac::Privilege.
    privilege      TEXT NOT NULL CHECK (privilege IN (
        'MANAGE_WAREHOUSE', 'CREATE_NAMESPACE', 'LIST_NAMESPACES',
        'MANAGE_NAMESPACE', 'CREATE_TABLE', 'LIST_TABLES', 'CREATE_VIEW',
        'READ', 'WRITE', 'COMMIT', 'DROP')),
    -- Audit string of the granting principal (e.g. user:auth0|abc).
    granted_by     TEXT NOT NULL,
    created_at     TIMESTAMPTZ NOT NULL DEFAULT now(),
    CHECK ((role_id IS NULL) <> (principal_id IS NULL))
);

-- One grant per (grantee, securable, privilege). COALESCE folds the XOR'd
-- nullable grantee columns into the key (a plain UNIQUE constraint treats
-- NULLs as distinct, and NULLS NOT DISTINCT would require Postgres 15+).
CREATE UNIQUE INDEX grants_grantee_securable_privilege_idx ON grants
    (workspace_id, COALESCE(role_id, ''), COALESCE(principal_id, ''),
     securable_type, securable_id, privilege);

CREATE INDEX grants_principal_id_idx ON grants (principal_id);
CREATE INDEX grants_role_id_idx ON grants (role_id);
CREATE INDEX grants_securable_idx ON grants (securable_type, securable_id);

-- Seed the built-in roles for the default workspace. Idempotent, like the
-- 0002 tenancy seeding.
INSERT INTO roles (id, workspace_id, name, description, built_in)
VALUES ('00000000000000000000000002', '00000000000000000000000001', 'admin',
        'Built-in: every privilege on every securable of the workspace.',
        TRUE)
ON CONFLICT (workspace_id, name) DO NOTHING;

INSERT INTO roles (id, workspace_id, name, description, built_in)
VALUES ('00000000000000000000000003', '00000000000000000000000001',
        'catalog_reader',
        'Built-in: read-only access (LIST_NAMESPACES, LIST_TABLES, READ) to every securable of the workspace.',
        TRUE)
ON CONFLICT (workspace_id, name) DO NOTHING;
