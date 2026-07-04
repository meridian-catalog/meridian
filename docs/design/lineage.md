# Lineage: commit-native edges, OpenLineage, and impact

Meridian records **table-level lineage** — directed `src → dst` edges meaning
"dst is derived from src" — and answers impact ("if I change X, what breaks?").
This document describes the data model, how edges are learned, the OpenLineage
surfaces in and out, the impact API, and the one guarantee everything else is
built to protect: **Meridian never fabricates edges.**

Status: implemented and tested against a local Postgres (`meridian-lineage`
unit tests + `crates/meridian-lineage/tests/lineage_db.rs`). Covers Pillar F,
F-F1 (commit-native), F-F2 (OpenLineage both directions), and F-F5 (impact /
graph). Column-level lineage (F-F3) is captured where an OpenLineage
`columnLineage` facet provides it; query-log-derived column graphs are a later
wave (`provenance='query-log'` is reserved in the model).

## The no-fabrication guarantee

The documented failure mode of naive lineage — and the reason engineers
distrust lineage tools — is the cartesian explosion: relating every dataset a
job touched to every other, so a graph fills with edges no engine ever
asserted. Meridian refuses to do this. Every edge traces to a **concrete
declaration**:

- a commit whose snapshot summary *listed* its input tables, or
- an engine that *declared* an (input, output) pair in an OpenLineage event.

An identifier Meridian cannot resolve to a table it owns becomes a labeled
**external** node — visible and honest, not an invented table id. A table with
no evidence has an empty lineage graph — *truthfully* empty, not a guess.
Column-level edges are recorded **only** from an explicit `columnLineage` facet;
a table-level edge stays `column_map = NULL` rather than fanning out a column
cross-product. Impact analysis carries this through: a column-scoped change
follows a column-mapped edge only when it maps that column.

## Data model (`lineage_edges`, migration 0017)

One row per directed, provenance- and confidence-labeled edge.

| column | meaning |
| --- | --- |
| `src_table_id` / `src_external` | source endpoint: **exactly one** is set — a native `tables.id`, or an opaque external dataset name. A DB `CHECK` enforces the XOR. |
| `dst_table_id` / `dst_external` | destination endpoint, same rule. |
| `provenance` | `commit` \| `openlineage` \| `query-log` — *how* the edge was learned. Part of the edge's identity. |
| `confidence` | `[0,1]`. Commit edges: 0.6 (a declared input is a real signal, weaker than an engine's lineage facet). OpenLineage edges: 0.95 (the engine declared it). |
| `column_map` | nullable JSONB array of `{src_column, dst_column, transform?}`. `NULL` = table-level only — **never** "all columns relate to all columns". |
| `engine_meta` | provenance evidence: `spark.app.id`, run/job ids, producer URI, the declared input/output identifiers. |
| `first_seen` / `last_seen` | observation window. |

**Idempotent upsert.** The unique key is
`(workspace, src, dst, provenance)` — the endpoint identity is coalesced to
`meridian:<id>` or `ext:<name>` so the index covers both native and external
endpoints. A repeat observation of the same provenance bumps `last_seen`, raises
`confidence` to the max of old/new (evidence only accumulates), shallow-merges
`engine_meta`, and fills in a `column_map` that arrives later — but a `NULL`
map never overwrites an existing one, and `first_seen` is preserved. The same
`(src, dst)` seen from two provenances is two rows: two independent pieces of
evidence.

A self-edge (`src == dst`, same native table) is rejected by a `CHECK`; the
derivation paths skip self-references (e.g. an in-place `overwrite` that lists
itself) before they ever construct one.

## F-F1 — commit-native lineage (zero setup, off the commit path)

The commit already enqueues a durable `table.committed` outbox event in its own
transaction. Lineage is derived **after** the commit — never inside the sacred
commit transaction (spec §8.3, §12.1) — by a background worker
(`meridian_lineage::worker`, spawned by `meridian serve` alongside the events /
maintenance / federation workers):

```
commit tx:        [ pointer CAS | table_snapshots | audit | outbox(table.committed) ]  -- atomic
                                                                   │
lineage worker:   read published table.committed after cursor  (durable, gap-free)
                  ├─ load the committed table's CURRENT snapshot summary
                  │     from table_snapshots (authoritative; the outbox
                  │     payload carries ids, not the whole summary)
                  ├─ declared_inputs(summary)?  →  upsert one edge per input
                  └─ advance cursor            (at-least-once; upserts idempotent)
```

An edge is recorded **only** when the summary declares inputs under a known key
(`meridian.lineage.inputs`, `input-tables`, `source-tables`, `dbt.upstream`) —
a JSON array or comma list of table identifiers. Engines and dbt macros that
know their sources set these. Each declared input is resolved to a native table
when it names one (`warehouse.ns.table`), else recorded as an external endpoint.
The engine fingerprint (`spark.app.id`, Flink job, Trino query id, dbt
invocation id) rides along in `engine_meta`. **No declared inputs → zero
edges.** An engine id alone never yields an edge.

Processing is at-least-once (a crash re-reads the batch; upserts are
idempotent). A per-event derivation error is logged and the cursor still
advances — a poisoned event must not wedge the stream, and the edge it would
have produced is recoverable from OpenLineage or a later commit.

## F-F2 — OpenLineage, both directions

### Sink: `POST /api/v2/lineage/openlineage`

Accepts an OpenLineage 1.x `RunEvent` (the JSON Spark/Airflow/dbt/Flink emit).
Only the lineage-relevant fields are modeled; unknown fields are ignored so
newer producer versions still parse. For every declared **(input, output)**
pair the sink records an edge (`provenance=openlineage`, confidence 0.95). The
`(input × output)` product here is exactly what the engine asserted in one run —
*not* the forbidden cartesian mode, which is inventing edges between datasets no
engine related. A run missing inputs **or** outputs records nothing.

Dataset naming: an OpenLineage `(namespace, name)` is normalized to
`namespace.name` (the OpenLineage convention is namespace = catalog/warehouse,
name = the dotted table path), which the resolver matches against native tables;
a URI-style namespace (`s3://…`) or an unresolved name yields a stable external
endpoint.

Column facets: when an output dataset carries a `columnLineage` facet, the sink
records the precise `src_column → dst_column` mappings (with the
`transformationDescription`), keeping only the input fields belonging to *that*
input dataset. No facet → table-level edge (`column_map = NULL`).

Minimal shape:

```jsonc
{
  "eventType": "COMPLETE",
  "run":    { "runId": "…uuid…" },
  "job":    { "namespace": "spark", "name": "build_sessions" },
  "inputs":  [ { "namespace": "<warehouse>", "name": "<ns>.raw_events" } ],
  "outputs": [ {
    "namespace": "<warehouse>", "name": "<ns>.sessions",
    "facets": { "columnLineage": { "fields": {
      "uid": { "inputFields": [
        { "namespace": "<warehouse>", "name": "<ns>.raw_events", "field": "user_id" }
      ], "transformationDescription": "IDENTITY" }
    } } }
  } ]
}
```

### Emitter: Meridian-initiated jobs

Meridian's own operations (maintenance compaction/expiry) render as spec-valid
`RunEvent`s (`meridian_lineage::openlineage::build_run_event`), so external
lineage tools see Meridian too. When `[lineage].openlineage_url` is configured,
`emit_run_event` POSTs to `<url>/api/v1/lineage` (the Marquez/OpenLineage HTTP
transport path); emission is best-effort — a maintenance commit never rolls back
because a collector was down. Emitted events (`producer` =
`github.com/meridian-catalog/meridian`, `schemaURL` pinned to the 1.x RunEvent)
round-trip back through Meridian's own parser.

## F-F5 — graph and impact

### Graph: `GET /api/v2/lineage?asset=&depth=&direction=`

A breadth-first walk over native-table endpoints (external endpoints are leaves —
Meridian cannot traverse past a dataset it does not own), bounded by `depth`
(1–20, default 3) and `direction` (`upstream` / `downstream` / `both`, default
`both`). Returns nodes (with display idents + depth from the root) and deduped
edges (with provenance, confidence, and whether column detail exists).

### Impact: `GET /api/v2/lineage/impact?asset=&change=&depth=`

The downstream blast radius of a change:

- `change=drop_table` — the whole downstream set;
- `change=drop_column:<name>` — column-precise where possible: an edge with a
  `column_map` is followed only when it maps `<name>`, tracking the produced
  downstream column onward; an edge with **no** column map is still followed
  (a table-level dependency a column drop can break) but the affected asset is
  flagged `via_column: null` — the link is real but not column-precise. The
  change is never silently pruned nor a column link fabricated.

Each affected asset carries its `owner` (the table's `owner` property, when set —
never inferred), and the report collects the distinct owners for the incidents
wave to notify. The `impact_of` function is exposed for that wave's blast-radius
calls. A table with no downstream lineage has an empty blast radius.

## Authorization

All lineage endpoints require **management access** (admin or any
`MANAGE_WAREHOUSE` grant), like the events and audit surfaces: a lineage graph
spans many assets at once, so no single resource-scoped privilege expresses "may
read lineage". A finer `READ_LINEAGE` privilege is deliberately deferred until it
earns its keep.
