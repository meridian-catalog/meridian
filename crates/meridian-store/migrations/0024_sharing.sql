-- 0024_sharing: cross-org data sharing (Pillar J, J-F1). A share is a scoped,
-- read-only projection of catalog assets to an *external* recipient org, served
-- over a recipient-specific Iceberg REST catalog endpoint (a distinct
-- `/share/{token}/v1/...` prefix per share) with vended read-only credentials
-- (reuse of meridian-vending) and optional per-grant row/column policy (reuse
-- of the same row-filter/column-mask primitives Pillar D applies in the scan
-- plan). It is the neutral alternative to Delta Sharing (Databricks-gravity)
-- and Snowflake shares (Snowflake-only): it works with *any* IRC-capable engine
-- on the recipient side, because the recipient endpoint speaks plain Iceberg
-- REST.
--
-- Append-only: this file only adds to the 0001-0022 schema; nothing is
-- rewritten. The served surfaces are the `/api/v2/shares` management routes
-- (workspace-side, management-gated), the recipient-facing `/share/{token}/v1`
-- IRC endpoint (token-authenticated, no OIDC — the recipient is an external
-- org), and the console Sharing page. Every management mutation writes its
-- audit_log row and outbox event on the same transaction (the invariant the
-- whole codebase holds: no mutation without its audit row); every recipient
-- access (config/list/load/vend) is audited too, so the full recipient trail
-- is on record.
--
-- Security model, stated plainly:
--
--  * The `token` is the bearer secret the recipient presents (as the path
--    prefix and/or an Authorization bearer). It is a high-entropy opaque
--    string. Anyone holding it can read the shared assets — treat it like a
--    password. It is unique across all shares (the recipient-endpoint router
--    resolves a share by it).
--
--  * Revocation is instant *in effect* because the credentials the recipient
--    receives are short-lived (vending TTL): the moment `revoked` is set, the
--    recipient endpoint returns 403 for every request, no new credentials
--    vend, and the already-vended ones expire on their own within the TTL.
--    There is no long-lived key to claw back.
--
--  * Terms acceptance is a recorded gate: a share may carry human-readable
--    `terms`; until the recipient accepts them (`terms_accepted_at` set, via
--    the recipient endpoint), the endpoint serves the terms and refuses data.
--    A share created with no `terms` needs no acceptance.
--
-- Model:
--
--  * shares: one row per (recipient, projection). A share has a name (unique
--    per workspace, case-insensitively), a free-text `recipient` identifier
--    (the external org / partner — an audit string, e.g. "org:acme" or an
--    email; Meridian does not manage the recipient's identity, only the
--    token), an opaque unique `token`, optional `terms` text and the
--    `terms_accepted_at` timestamp, the `created_by` audit string, and the
--    `revoked` flag + `revoked_at`. It is scoped to a workspace like every
--    other object.
--
--  * share_grants: the projection contents — each row adds one securable
--    (a table, a view, or a certified data product) to the share, with an
--    optional `row_filter` (a boolean SQL predicate, applied to recipient
--    reads) and an optional `column_mask` (a JSONB array of column names the
--    recipient must not see). A data-product grant expands to its member
--    tables/views at serve time (the product is the unit of sharing a human
--    reasons about; the endpoint still serves individual Iceberg tables). Not
--    an FK to the asset tables: like glossary_links / data_product_members, it
--    uses a stable (kind, ref) pair so a grant survives the same cross-
--    subsystem churn. UNIQUE on (share_id, securable_kind, securable_ref) so a
--    securable is granted to a share once. ON DELETE CASCADE from the share:
--    dropping a share removes its grants.

-- ----------------------------------------------------------------------------
-- shares: a scoped projection of assets to an external recipient (J-F1).

CREATE TABLE shares (
    id                TEXT PRIMARY KEY,
    workspace_id      TEXT NOT NULL REFERENCES workspaces (id) ON DELETE RESTRICT,
    -- Machine name, unique per workspace (case-insensitively).
    name              TEXT NOT NULL,
    -- Free-text external recipient identifier (audit string). Meridian does not
    -- own the recipient's identity — the token is the credential — so this is a
    -- label for humans and the audit trail, not an FK.
    recipient         TEXT NOT NULL,
    -- The opaque bearer secret the recipient presents (path prefix / bearer).
    -- High-entropy; unique across all shares so the recipient router resolves a
    -- share by it in one lookup.
    token             TEXT NOT NULL,
    -- Human-readable terms of use. NULL means no acceptance gate.
    terms             TEXT,
    -- When the recipient accepted `terms`. NULL until accepted (or when there
    -- are no terms to accept).
    terms_accepted_at TIMESTAMPTZ,
    -- The workspace principal who created the share (audit string).
    created_by        TEXT NOT NULL,
    -- Revocation is a flag, not a delete: the row (and its audit history) is
    -- retained. A revoked share serves nothing.
    revoked           BOOLEAN NOT NULL DEFAULT FALSE,
    revoked_at        TIMESTAMPTZ,
    created_at        TIMESTAMPTZ NOT NULL DEFAULT now(),
    updated_at        TIMESTAMPTZ NOT NULL DEFAULT now(),
    -- revoked <=> revoked_at is set.
    CHECK (revoked = (revoked_at IS NOT NULL))
);

CREATE UNIQUE INDEX shares_workspace_name_unique
    ON shares (workspace_id, lower(name));
CREATE UNIQUE INDEX shares_token_unique ON shares (token);
CREATE INDEX shares_workspace_idx ON shares (workspace_id);
CREATE INDEX shares_recipient_idx ON shares (recipient);

-- ----------------------------------------------------------------------------
-- share_grants: the projection contents (J-F1).

CREATE TABLE share_grants (
    id             TEXT PRIMARY KEY,
    workspace_id   TEXT NOT NULL REFERENCES workspaces (id) ON DELETE RESTRICT,
    -- The owning share. CASCADE: dropping a share removes its grants (never the
    -- underlying assets — a grant is a projection over assets).
    share_id       TEXT NOT NULL REFERENCES shares (id) ON DELETE CASCADE,
    -- The granted securable's kind and stable reference. A share projects
    -- tables, views, and certified data products (a product expands to its
    -- member tables/views at serve time). Stable (kind, ref), not an FK, for
    -- the same rename-survival reasons as glossary_links / data_product_members.
    securable_kind TEXT NOT NULL
        CHECK (securable_kind IN ('table', 'view', 'data_product')),
    securable_ref  TEXT NOT NULL,
    -- Optional row filter: a boolean SQL predicate applied to recipient reads
    -- of this securable (e.g. "region = 'EU'"). NULL means no row filter.
    row_filter     TEXT,
    -- Optional column mask: a JSONB array of column names the recipient must
    -- not see (they are dropped from the served schema and reads). NULL means
    -- no column mask.
    column_mask    JSONB,
    created_at     TIMESTAMPTZ NOT NULL DEFAULT now()
);

-- A share grants a securable once.
CREATE UNIQUE INDEX share_grants_unique
    ON share_grants (share_id, securable_kind, securable_ref);
CREATE INDEX share_grants_share_idx ON share_grants (share_id);
