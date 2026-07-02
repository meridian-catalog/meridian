-- 0004_auth: local principal identities for authenticated callers (M1b).
--
-- Append-only: this file only adds to the existing schema.
--
-- A principal row is the stable local identity behind an external OIDC
-- identity (issuer + subject). Rows are provisioned just-in-time on a
-- caller's first authenticated request; audit rows and (future) grants
-- reference this identity rather than raw token claims.

CREATE TABLE principals (
    id           TEXT PRIMARY KEY,
    workspace_id TEXT NOT NULL REFERENCES workspaces (id) ON DELETE RESTRICT,
    -- Mirrors meridian_common::principal::PrincipalKind (snake_case).
    kind         TEXT NOT NULL CHECK (kind IN ('user', 'service', 'agent', 'anonymous')),
    -- Raw OIDC `sub` claim; issuer-qualified only through the (issuer,
    -- subject) pair, never by rewriting the subject itself.
    subject      TEXT NOT NULL,
    issuer       TEXT NOT NULL,
    display_name TEXT,
    created_at   TIMESTAMPTZ NOT NULL DEFAULT now(),
    UNIQUE (issuer, subject)
);

CREATE INDEX principals_workspace_id_idx ON principals (workspace_id);
