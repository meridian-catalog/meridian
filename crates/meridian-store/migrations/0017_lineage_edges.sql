-- 0017_lineage_edges: commit-native + OpenLineage table lineage (Pillar F,
-- F-F1/F-F2/F-F5). One row per directed table-to-table edge, provenance- and
-- confidence-labeled, upserted (never fabricated) by the post-commit lineage
-- hook (F-F1), the OpenLineage sink (F-F2), and future query-log ingestion.
--
-- Append-only: this file only adds to the 0001-0016 schema; nothing is
-- rewritten.
--
-- Model:
--
--  * A lineage edge is directed: src_table_id --produces--> dst_table_id
--    ("dst is derived from src"). Both endpoints are Meridian table ids
--    (tables.id). Endpoints that Meridian does not (yet) know as native tables
--    are recorded as *external* endpoints on the same row via the nullable
--    src_external / dst_external name columns (an OpenLineage dataset that has
--    no Meridian table), so a partially-known edge is still recorded without
--    fabricating a table identity. Exactly one of (table_id, external) is set
--    per endpoint; the CHECK constraints below enforce that.
--
--  * provenance records HOW the edge was learned and is part of the identity
--    of the edge: the same (src,dst) pair observed from a commit summary and
--    from an OpenLineage event are two rows (two independent pieces of
--    evidence), each with its own confidence. 'commit' is the zero-setup
--    commit-native path (F-F1); 'openlineage' is the OpenLineage sink (F-F2);
--    'query-log' is reserved for query-log ingestion (F-F3, later wave).
--
--  * confidence in [0,1] labels how sure we are. Commit-derived edges carry a
--    calibrated-but-modest confidence (an engine job-id proves co-occurrence,
--    not a proven read->write dependency); OpenLineage edges carry high
--    confidence (the engine declared the input/output relationship).
--
--  * column_map is a nullable jsonb array of {src_column, dst_column}
--    (+ optional transform) pairs for column-level lineage (F-F3), populated
--    from OpenLineage columnLineage facets when present. NULL means
--    table-level only — NOT "all columns relate to all columns". Unknown stays
--    unknown, visibly (the documented OpenLineage cartesian failure mode we
--    refuse to reproduce; spec F-F3).
--
--  * engine_meta is provenance detail: the engine facts that produced the edge
--    (spark.app.id, flink job id, trino query id, dbt invocation id, the
--    OpenLineage run/job, the producer URI). It is evidence, not schema.
--
--  * first_seen / last_seen bound the observation window. An upsert bumps
--    last_seen (and raises confidence / merges engine_meta) but never lowers
--    first_seen, so the age of a lineage relationship is queryable.

CREATE TABLE lineage_edges (
    id            TEXT PRIMARY KEY,
    workspace_id  TEXT NOT NULL REFERENCES workspaces (id) ON DELETE CASCADE,

    -- Source endpoint: exactly one of (native table, external name).
    src_table_id  TEXT REFERENCES tables (id) ON DELETE CASCADE,
    src_external  TEXT,

    -- Destination endpoint: exactly one of (native table, external name).
    dst_table_id  TEXT REFERENCES tables (id) ON DELETE CASCADE,
    dst_external  TEXT,

    provenance    TEXT NOT NULL
        CHECK (provenance IN ('commit', 'openlineage', 'query-log')),
    confidence    DOUBLE PRECISION NOT NULL DEFAULT 0.5
        CHECK (confidence >= 0.0 AND confidence <= 1.0),

    -- Column-level lineage (nullable). NULL = table-level only, never
    -- "everything relates to everything".
    column_map    JSONB,

    -- Engine/provenance evidence (spark.app.id, run id, producer, ...).
    engine_meta   JSONB NOT NULL DEFAULT '{}'::jsonb,

    first_seen    TIMESTAMPTZ NOT NULL DEFAULT now(),
    last_seen     TIMESTAMPTZ NOT NULL DEFAULT now(),

    -- Exactly one endpoint identity per side (native XOR external), and no
    -- self-edge (a table is never its own upstream).
    CONSTRAINT lineage_edges_src_identity CHECK (
        (src_table_id IS NOT NULL) <> (src_external IS NOT NULL)
    ),
    CONSTRAINT lineage_edges_dst_identity CHECK (
        (dst_table_id IS NOT NULL) <> (dst_external IS NOT NULL)
    ),
    CONSTRAINT lineage_edges_no_self CHECK (
        src_table_id IS DISTINCT FROM dst_table_id
            OR src_table_id IS NULL
    )
);

-- Idempotent upsert key: one row per (src, dst, provenance). The endpoint
-- key coalesces the native/external identity to a single text so the unique
-- index covers both native and external endpoints. 'meridian:' / 'ext:'
-- prefixes keep a table id and an identically-spelled external name distinct.
CREATE UNIQUE INDEX lineage_edges_edge_unique ON lineage_edges (
    workspace_id,
    (COALESCE('meridian:' || src_table_id, 'ext:' || src_external)),
    (COALESCE('meridian:' || dst_table_id, 'ext:' || dst_external)),
    provenance
);

-- Graph traversal: upstream (by dst) and downstream (by src) lookups.
CREATE INDEX lineage_edges_dst_idx ON lineage_edges (workspace_id, dst_table_id)
    WHERE dst_table_id IS NOT NULL;
CREATE INDEX lineage_edges_src_idx ON lineage_edges (workspace_id, src_table_id)
    WHERE src_table_id IS NOT NULL;
