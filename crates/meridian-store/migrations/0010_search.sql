-- 0010_search: Postgres full-text search over catalog assets (search v1).
--
-- Append-only: this file only adds to the existing schema.
--
-- Each searchable asset row (tables, views, namespaces) carries a
-- `search_tsv` tsvector maintained by BEFORE INSERT/UPDATE triggers, with a
-- GIN index for `@@` matching. The search query itself lives in
-- meridian_store::search.
--
-- ## Trigger, not a generated column — why
--
-- A `GENERATED ALWAYS AS (...) STORED` column was considered and rejected:
--
-- 1. The document for a table/view includes its **namespace path**, which
--    lives in another row (`namespaces.levels`). Generated columns can only
--    reference columns of their own row; a trigger may query other tables.
-- 2. Even the single-row parts would not qualify: generated columns require
--    IMMUTABLE expressions, and both `array_to_string(text[], text)` and the
--    config-resolving `to_tsvector(regconfig, text)` are only STABLE.
-- 3. One mechanism across all three asset tables keeps the maintenance
--    story uniform.
--
-- The denormalized namespace path inside a table/view tsvector cannot go
-- stale: the IRC surface has no namespace rename, and a table/view moving
-- namespaces updates its `namespace_id`, which re-fires the trigger. The
-- triggers recompute unconditionally on every UPDATE — the extra cost is one
-- primary-key lookup into `namespaces`, negligible next to what any mutation
-- of these rows already does (audit + outbox writes in the same
-- transaction).
--
-- ## What is indexed (weights)
--
--   A: the asset name (last level, for namespaces)
--   B: the namespace path (all levels)
--   C: column names and column docs from the current schema (`schema_text`,
--      written through by the application layer; see below)
--   D: `properties ->> 'comment'`
--
-- The `simple` config is used on purpose: asset and column identifiers must
-- not be stemmed or stop-worded. Postgres' default parser splits
-- `customer_email` into `customer` + `email` lexemes (underscore is a
-- separator), so part-of-identifier queries match; a `customer_email` query
-- becomes the phrase `customer <-> email` and still matches the identifier
-- exactly.
--
-- ## schema_text
--
-- `tables.schema_text` is a flat, space-joined list of every column name and
-- column doc in the table's *current* schema (nested struct/list/map fields
-- included), extracted from the metadata by the application layer
-- (meridian_store::search::schema_search_text) and written through on
-- create/register and on every commit — the same write-through transaction
-- that maintains format_version/properties (ADR 003).
--
-- `views.schema_text` exists with identical semantics but is not yet
-- populated: the view create/replace write-through does not extract schema
-- text yet (TODO, search v1 follow-up), so views are searchable by
-- name/path/comment only for now.

ALTER TABLE tables ADD COLUMN schema_text TEXT;
ALTER TABLE views ADD COLUMN schema_text TEXT;

ALTER TABLE tables ADD COLUMN search_tsv tsvector;
ALTER TABLE views ADD COLUMN search_tsv tsvector;
ALTER TABLE namespaces ADD COLUMN search_tsv tsvector;

-- The shared document builder. STABLE (not IMMUTABLE) because to_tsvector
-- with an explicit regconfig still consults the text-search catalogs.
CREATE FUNCTION asset_search_tsv(
    asset_name TEXT,
    path TEXT,
    schema_text TEXT,
    comment TEXT
) RETURNS tsvector
    LANGUAGE sql
    STABLE
    AS $$
    SELECT setweight(to_tsvector('simple', coalesce(asset_name, '')), 'A')
        || setweight(to_tsvector('simple', coalesce(path, '')), 'B')
        || setweight(to_tsvector('simple', coalesce(schema_text, '')), 'C')
        || setweight(to_tsvector('simple', coalesce(comment, '')), 'D');
$$;

CREATE FUNCTION tables_search_tsv_sync() RETURNS trigger
    LANGUAGE plpgsql
    AS $$
BEGIN
    NEW.search_tsv := asset_search_tsv(
        NEW.name,
        (SELECT array_to_string(n.levels, ' ')
           FROM namespaces n WHERE n.id = NEW.namespace_id),
        NEW.schema_text,
        NEW.properties ->> 'comment');
    RETURN NEW;
END;
$$;

CREATE FUNCTION views_search_tsv_sync() RETURNS trigger
    LANGUAGE plpgsql
    AS $$
BEGIN
    NEW.search_tsv := asset_search_tsv(
        NEW.name,
        (SELECT array_to_string(n.levels, ' ')
           FROM namespaces n WHERE n.id = NEW.namespace_id),
        NEW.schema_text,
        NEW.properties ->> 'comment');
    RETURN NEW;
END;
$$;

CREATE FUNCTION namespaces_search_tsv_sync() RETURNS trigger
    LANGUAGE plpgsql
    AS $$
BEGIN
    NEW.search_tsv := asset_search_tsv(
        NEW.levels[cardinality(NEW.levels)],
        array_to_string(NEW.levels, ' '),
        NULL,
        NEW.properties ->> 'comment');
    RETURN NEW;
END;
$$;

-- Backfill existing rows before the triggers exist (fresh deployments have
-- none; developer databases may).
UPDATE tables t SET search_tsv = asset_search_tsv(
    t.name,
    (SELECT array_to_string(n.levels, ' ') FROM namespaces n WHERE n.id = t.namespace_id),
    t.schema_text,
    t.properties ->> 'comment');

UPDATE views v SET search_tsv = asset_search_tsv(
    v.name,
    (SELECT array_to_string(n.levels, ' ') FROM namespaces n WHERE n.id = v.namespace_id),
    v.schema_text,
    v.properties ->> 'comment');

UPDATE namespaces n SET search_tsv = asset_search_tsv(
    n.levels[cardinality(n.levels)],
    array_to_string(n.levels, ' '),
    NULL,
    n.properties ->> 'comment');

CREATE TRIGGER tables_search_tsv
    BEFORE INSERT OR UPDATE ON tables
    FOR EACH ROW
    EXECUTE FUNCTION tables_search_tsv_sync();

CREATE TRIGGER views_search_tsv
    BEFORE INSERT OR UPDATE ON views
    FOR EACH ROW
    EXECUTE FUNCTION views_search_tsv_sync();

CREATE TRIGGER namespaces_search_tsv
    BEFORE INSERT OR UPDATE ON namespaces
    FOR EACH ROW
    EXECUTE FUNCTION namespaces_search_tsv_sync();

CREATE INDEX tables_search_tsv_gin ON tables USING GIN (search_tsv);
CREATE INDEX views_search_tsv_gin ON views USING GIN (search_tsv);
CREATE INDEX namespaces_search_tsv_gin ON namespaces USING GIN (search_tsv);
