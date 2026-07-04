-- 0021_semantics: the semantics layer data model (Pillar G, G-F2/G-F3/G-F4 —
-- metrics & semantic models, business glossary, certified data products). This
-- is the object model that puts *meaning* next to the data: measures and
-- dimensions that compile deterministically to SQL, a stewarded glossary linked
-- to assets, and named product bundles that are the unit of consumption for
-- humans and agents.
--
-- Append-only: this file only adds to the 0001-0020 schema; nothing is
-- rewritten. It owns the *definitions*; compilation to a chosen engine's SQL is
-- done by the transpilation sidecar (§8.5) via the server, and the served
-- surfaces are the /api/v2 semantics routes, the MCP context tools
-- (list_metrics / get_metric_definition / list_data_products / get_glossary_term,
-- H-F2), and the console Semantics page. Every mutation through the store writes
-- its audit_log row and outbox event on the same transaction (the invariant the
-- whole codebase holds: no mutation without its audit row).
--
-- Certification is a first-class, honest status on metrics and products: an
-- object is `draft`, `certified`, or `deprecated`. Nothing here claims a metric
-- is correct — certification is a governance signal a steward sets, surfaced
-- verbatim, exactly like the transpile status machine surfaces verified/best_effort.
--
-- Model:
--
--  * metrics: a first-class semantic object. A metric has a name (unique per
--    workspace), a source table/view identifier, a measure `expression` (an
--    aggregation such as SUM(amount) authored in a canonical dialect), optional
--    default dimensions and filters, a grain description, a free-text
--    description, an owner (audit string), and a certification status. The
--    definition is engine-neutral; the server compiles it to a requested
--    engine's SQL deterministically via the sidecar. `dimensions` and `filters`
--    are JSONB arrays of strings (they are lists of SQL fragments / column
--    names, not relational sub-entities — keeping them inline makes a metric one
--    row and one read).
--
--  * glossary_terms: a business term with a definition, an optional steward
--    (audit string), and a certification status. Names are unique per workspace,
--    case-insensitively (a glossary with both "Revenue" and "revenue" is a bug,
--    not a feature).
--
--  * glossary_links: many-to-many links from a term to a catalog asset (a table,
--    a view, or a metric) — the "this column means this term" relationship
--    (G-F3). asset_kind + asset_ref identify the asset; asset_ref is the audit-
--    style stable reference (e.g. table:<id>, view:<id>, metric:<id>) so a link
--    survives a rename. A term may link many assets; an asset may carry many
--    terms. UNIQUE on (term_id, asset_kind, asset_ref) makes a link idempotent.
--
--  * data_products: a named, certified bundle (G-F4) — the unit of consumption.
--    A product has a name (unique per workspace), a description, an owner, an
--    optional SLA statement (free text; the machine-checked SLOs live in the
--    quality subsystem this reuses), and a certification status. Its members are
--    rows in data_product_members.
--
--  * data_product_members: the bundle contents — each row links the product to
--    one member asset (a table, view, metric, glossary term, or contract) by
--    kind + stable ref. UNIQUE on (product_id, member_kind, member_ref) so a
--    member is listed once. ON DELETE CASCADE from the product: dropping a
--    product removes its membership rows (not the underlying assets).

-- ----------------------------------------------------------------------------
-- metrics: first-class semantic objects (G-F2).

CREATE TABLE metrics (
    id             TEXT PRIMARY KEY,
    workspace_id   TEXT NOT NULL REFERENCES workspaces (id) ON DELETE RESTRICT,
    -- Metric name, unique per workspace (see the unique index below, which is
    -- case-insensitive). This is the name agents and BI reference.
    name           TEXT NOT NULL,
    -- Optional human label distinct from the machine name.
    display_name   TEXT,
    -- The source table or view the metric aggregates, as a dotted identifier
    -- (e.g. analytics.public.sales). Not an FK: a metric may reference a view or
    -- a table by its catalog identifier, resolved at compile time; the identifier
    -- outlives any single physical id and can name assets in mirrors.
    source         TEXT NOT NULL,
    -- The measure: an aggregation expression authored in the canonical dialect
    -- (e.g. SUM(amount), COUNT(DISTINCT user_id)). Compiled to the requested
    -- engine's SQL by the sidecar.
    expression     TEXT NOT NULL,
    -- The dialect `expression`, `dimensions`, and `filters` are authored in
    -- (the canonical dialect for this metric). Compilation transpiles FROM this.
    dialect        TEXT NOT NULL DEFAULT 'trino',
    -- Default dimensions (group-by columns/expressions) and filters (boolean SQL
    -- fragments ANDed together) as JSONB string arrays. A compile request may
    -- override/extend these.
    dimensions     JSONB NOT NULL DEFAULT '[]'::jsonb,
    filters        JSONB NOT NULL DEFAULT '[]'::jsonb,
    -- The grain the metric is defined at (free text, e.g. "one row per order").
    grain          TEXT,
    -- Free-text description / documentation (markdown).
    description    TEXT,
    -- The accountable owner, as an audit string (e.g. user:alice@example.com).
    -- Not an FK: owners are external identities rendered as audit strings.
    owner          TEXT,
    -- Certification status: a governance signal, surfaced verbatim. Never a
    -- claim of correctness.
    certification  TEXT NOT NULL DEFAULT 'draft'
        CHECK (certification IN ('draft', 'certified', 'deprecated')),
    created_at     TIMESTAMPTZ NOT NULL DEFAULT now(),
    updated_at     TIMESTAMPTZ NOT NULL DEFAULT now()
);

-- Names are unique per workspace, case-insensitively.
CREATE UNIQUE INDEX metrics_workspace_name_unique
    ON metrics (workspace_id, lower(name));
CREATE INDEX metrics_workspace_idx ON metrics (workspace_id);
CREATE INDEX metrics_certification_idx ON metrics (certification);

-- ----------------------------------------------------------------------------
-- glossary_terms: stewarded business vocabulary (G-F3).

CREATE TABLE glossary_terms (
    id             TEXT PRIMARY KEY,
    workspace_id   TEXT NOT NULL REFERENCES workspaces (id) ON DELETE RESTRICT,
    -- Term name, unique per workspace case-insensitively (index below).
    name           TEXT NOT NULL,
    -- The definition (markdown).
    definition     TEXT NOT NULL,
    -- The accountable steward, as an audit string. Nullable (unowned term — a
    -- nudge target for the ownership-enforcement surface).
    steward        TEXT,
    certification  TEXT NOT NULL DEFAULT 'draft'
        CHECK (certification IN ('draft', 'certified', 'deprecated')),
    created_at     TIMESTAMPTZ NOT NULL DEFAULT now(),
    updated_at     TIMESTAMPTZ NOT NULL DEFAULT now()
);

CREATE UNIQUE INDEX glossary_terms_workspace_name_unique
    ON glossary_terms (workspace_id, lower(name));
CREATE INDEX glossary_terms_workspace_idx ON glossary_terms (workspace_id);

-- ----------------------------------------------------------------------------
-- glossary_links: term <-> asset links (G-F3). Many-to-many.

CREATE TABLE glossary_links (
    id             TEXT PRIMARY KEY,
    workspace_id   TEXT NOT NULL REFERENCES workspaces (id) ON DELETE RESTRICT,
    -- The linked term. CASCADE: dropping a term removes its links.
    term_id        TEXT NOT NULL REFERENCES glossary_terms (id) ON DELETE CASCADE,
    -- The linked asset's kind and stable reference. Not an FK (an asset may be a
    -- table, a view, or a metric, and refs are audit-style stable strings), so a
    -- link survives a rename and can point across subsystems.
    asset_kind     TEXT NOT NULL
        CHECK (asset_kind IN ('table', 'view', 'metric')),
    asset_ref      TEXT NOT NULL,
    created_at     TIMESTAMPTZ NOT NULL DEFAULT now()
);

-- A term links an asset at most once.
CREATE UNIQUE INDEX glossary_links_unique
    ON glossary_links (term_id, asset_kind, asset_ref);
CREATE INDEX glossary_links_asset_idx
    ON glossary_links (workspace_id, asset_kind, asset_ref);

-- ----------------------------------------------------------------------------
-- data_products: certified bundles (G-F4) — the unit of consumption.

CREATE TABLE data_products (
    id             TEXT PRIMARY KEY,
    workspace_id   TEXT NOT NULL REFERENCES workspaces (id) ON DELETE RESTRICT,
    name           TEXT NOT NULL,
    display_name   TEXT,
    description    TEXT,
    -- The accountable owner (audit string). Nullable (a nudge target).
    owner          TEXT,
    -- Free-text SLA statement (e.g. "99.9% freshness within 1h"). The machine-
    -- checked SLOs are the quality subsystem's monitors/contracts on the member
    -- assets; this is the human-readable product-level promise.
    sla            TEXT,
    certification  TEXT NOT NULL DEFAULT 'draft'
        CHECK (certification IN ('draft', 'certified', 'deprecated')),
    created_at     TIMESTAMPTZ NOT NULL DEFAULT now(),
    updated_at     TIMESTAMPTZ NOT NULL DEFAULT now()
);

CREATE UNIQUE INDEX data_products_workspace_name_unique
    ON data_products (workspace_id, lower(name));
CREATE INDEX data_products_workspace_idx ON data_products (workspace_id);
CREATE INDEX data_products_certification_idx ON data_products (certification);

-- ----------------------------------------------------------------------------
-- data_product_members: the bundle contents (G-F4).

CREATE TABLE data_product_members (
    id             TEXT PRIMARY KEY,
    workspace_id   TEXT NOT NULL REFERENCES workspaces (id) ON DELETE RESTRICT,
    -- The owning product. CASCADE: dropping a product removes its membership
    -- rows (never the underlying assets — a product is a view over assets).
    product_id     TEXT NOT NULL REFERENCES data_products (id) ON DELETE CASCADE,
    -- The member asset's kind and stable reference. A product bundles tables,
    -- views, metrics, glossary terms, and contracts (G-F4). Not an FK for the
    -- same cross-subsystem, rename-survival reasons as glossary_links.
    member_kind    TEXT NOT NULL
        CHECK (member_kind IN ('table', 'view', 'metric', 'glossary_term', 'contract')),
    member_ref     TEXT NOT NULL,
    created_at     TIMESTAMPTZ NOT NULL DEFAULT now()
);

-- A product lists a member once.
CREATE UNIQUE INDEX data_product_members_unique
    ON data_product_members (product_id, member_kind, member_ref);
CREATE INDEX data_product_members_product_idx
    ON data_product_members (product_id);

-- ----------------------------------------------------------------------------
-- view_representation_cache: the universal-view translation cache (G-F1, §8.5).
--
-- On LoadView, Meridian serves the SQL representation matching the requesting
-- engine's dialect. When the view metadata does not already carry that dialect,
-- the server transpiles the canonical representation via the sidecar and serves
-- + caches the result HERE — a side table keyed by the view + the source SQL +
-- the target dialect, rather than mutating the Iceberg view metadata pointer on
-- a read (which would churn pointer_version and surprise clients doing
-- optimistic view commits). The served LoadView response still carries the
-- translated representation (dialect-tagged) so the requesting engine sees it;
-- this table makes the translation durable and instant on the next load.
--
-- The cache key includes source_sql_hash (a sha256 of the canonical SQL that was
-- translated): if the view's definition changes, old cache entries simply stop
-- matching (they are never served for a different definition) and are swept
-- lazily. status is the honest transpile label (verified | best_effort |
-- unsupported); an unsupported entry caches the *absence* of a good translation
-- so the sidecar is not re-hit for a construct it already could not handle.

CREATE TABLE view_representation_cache (
    id                TEXT PRIMARY KEY,
    workspace_id      TEXT NOT NULL REFERENCES workspaces (id) ON DELETE RESTRICT,
    -- The view this translation belongs to. CASCADE: dropping the view drops
    -- its cached translations.
    view_id           TEXT NOT NULL REFERENCES views (id) ON DELETE CASCADE,
    -- The dialect this entry translates INTO (the requesting engine's dialect),
    -- lowercased.
    target_dialect    TEXT NOT NULL,
    -- The dialect the source representation was authored in (lowercased).
    source_dialect    TEXT NOT NULL,
    -- sha256 (hex) of the canonical source SQL that was translated. Ties the
    -- cache entry to a specific definition; a changed definition never reuses a
    -- stale translation.
    source_sql_hash   TEXT NOT NULL,
    -- The translated SQL. NULL when status = 'unsupported' (we cache the honest
    -- "no good translation" so the sidecar is not re-hit needlessly).
    translated_sql    TEXT,
    -- The honest transpile status for this entry.
    status            TEXT NOT NULL
        CHECK (status IN ('verified', 'best_effort', 'unsupported')),
    -- The sidecar diagnostics for this translation (JSONB array), surfaced in
    -- the UI/response so a best_effort result carries its caveats.
    diagnostics       JSONB NOT NULL DEFAULT '[]'::jsonb,
    created_at        TIMESTAMPTZ NOT NULL DEFAULT now(),
    updated_at        TIMESTAMPTZ NOT NULL DEFAULT now()
);

-- One cache entry per (view, target dialect, source definition). A re-translate
-- of the same definition upserts this row.
CREATE UNIQUE INDEX view_representation_cache_key
    ON view_representation_cache (view_id, target_dialect, source_sql_hash);
CREATE INDEX view_representation_cache_view_idx
    ON view_representation_cache (view_id);
