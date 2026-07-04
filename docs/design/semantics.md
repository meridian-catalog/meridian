# Semantics layer: universal views, metrics, glossary, data products

Pillar G puts *meaning* next to the data: a metric that compiles deterministically
to any engine's SQL, a stewarded glossary linked to assets, certified data
products as the unit of consumption — and the universal-view transpiler that lets
one view read in every engine's dialect. This document is the server-side design;
the SQLGlot sidecar's own HTTP contract lives in
[`transpilation.md`](./transpilation.md).

Status: implemented and tested. Migration `0021_semantics.sql`; store module
`crates/meridian-store/src/semantics.rs`; server routes
`crates/meridian-server/src/routes/semantics.rs` and the universal-view path in
`routes/views.rs`; the Rust sidecar client `crates/meridian-server/src/sidecar.rs`.
Served to BI/humans over `/api/v2`, to agents over MCP (H-F2 context tools), and
in the console Semantics page.

## Universal views (G-F1) — the dialect fix

The five-year-old cross-engine view bug ("a Spark view breaks or silently
corrupts in Trino/Dremio") is closed by the catalog doing the one thing it is
uniquely positioned to do: it knows *which engine is asking*.

### The `LoadView` path

On `loadView`, after reading the view metadata:

1. **Resolve the requesting dialect** (`resolve_requesting_dialect`), in priority
   order:
   - the explicit `?engine=<dialect>` query override (the console, migration
     tools, and tests use this);
   - `User-Agent` inference (`dialect_from_user_agent`) — well-known engine
     clients identify themselves (`Trino JDBC Driver/…`, `pyiceberg`, …); an
     unrecognized agent yields *no* dialect rather than a guess (a wrong dialect
     is worse than none);
   - otherwise none → serve the view exactly as authored, no transpilation.
2. **Already present?** If the current version already carries a SQL
   representation for the target dialect (authored, or previously folded), serve
   it as `verified` — nothing to do.
3. **Transpile** the *canonical* representation (the current version's first SQL
   representation) via the sidecar `POST /v1/transpile`, from its dialect to the
   target.
4. **Serve + cache.** The translated representation is folded into the served
   `metadata`'s current version (dialect-tagged, carrying a
   `meridian.transpile-status` in its `extra`) so the requesting engine sees it,
   and the response gains a `meridian-transpile` note with the honest status. The
   translation is persisted to the durable cache (see below).
5. **Graceful degradation.** A sidecar outage (unreachable/errored) never fails a
   `loadView`: the canonical representation is served with a status note.

### Status is truthful

Every translation carries the sidecar's status machine verbatim:

| status | meaning |
| --- | --- |
| `verified` | SQLGlot translated it *and* the output re-parses cleanly in the target dialect (parse-back). Safe to serve. |
| `best_effort` | Output was produced but a construct was approximated or parse-back surfaced a difference (or the operator's LLM-assist fallback produced it). Usable, caveated, never guaranteed. |
| `unsupported` | SQLGlot raised and no fallback produced a valid result. No SQL is served as correct. |

Validation is parse-back, always. The status is never dressed up.

### Why a side-table cache (not a pointer bump)

Translations are cached in `view_representation_cache`, keyed by
`(view_id, target_dialect, source_sql_hash)` — **not** by mutating the Iceberg
view metadata pointer on a read. Mutating the pointer on every novel-dialect load
would churn `pointer_version` and surprise clients doing optimistic view commits
(their `assert-view-uuid`/version requirements could spuriously fail). The side
table makes a translation durable and instant on the next load without any of
that hazard.

`source_sql_hash` (a sha256 of the canonical SQL that was translated) ties a
cache entry to a specific definition: if the view's definition changes, old
entries simply stop matching and are never served for a different definition. An
`unsupported` entry caches the *absence* of a good translation, so the sidecar is
not re-hit for a construct it already could not handle. The served `loadView`
response still carries the dialect-tagged representation, so an engine reading the
view sees the multi-representation form.

The cache write is best-effort: a cache-write failure is logged and never affects
what is served.

### Standalone transpile API

`POST /api/v2/sql/transpile` is a thin, management-gated passthrough to the
sidecar: paste SQL, name two dialects, get back the translation with its status
and diagnostics. Useful for migrations, and a quiet demo magnet. A sidecar outage
is a `503`, never a 500.

## Metrics & semantic models (G-F2)

A metric is a first-class object: a measure `expression` (an aggregation authored
in a canonical dialect) over a `source`, optionally grouped by `dimensions` and
constrained by `filters`, with a `grain`, `description`, `owner`, and a
certification status. The definition is engine-neutral.

Compilation (`GET /api/v2/metrics/{id}/compile?engine=<dialect>`) is
**deterministic** — the sidecar's `compile_metric` builds
`SELECT <dimensions>, <expression> AS <name> FROM <source> [WHERE …] [GROUP BY …]`
and renders it in the requested engine's dialect via SQLGlot. No LLM is ever
consulted for metric compilation. The result carries the same status machine as a
transpile.

Metrics are served to BI over the HTTP API and to agents over MCP
(`list_metrics` / `get_metric_definition`, H-F2). An agent reads the definition,
then runs it under policy via the governed `query_metrics` tool — the metric
definition is the source of truth for the ~100%-accuracy path.

The OSI (Open Semantic Interchange) direction is where this is headed; the model
here is aligned with it (measures, dimensions, entities, grain, certification) but
this wave does not claim a full OSI import/export implementation — that is tracked
honestly in `api-status.md`.

## Business glossary (G-F3)

Terms with definitions, stewards, and a certification status; many-to-many
`term ↔ asset` links (to tables, views, metrics) by stable audit-style reference
so a link survives a rename. Names are unique per workspace, case-insensitively.
Served to agents as the `get_glossary_term` context tool. Markdown docs on assets
reuse the existing table/view `comment` property; the glossary is the shared
vocabulary those docs reference.

## Certified data products (G-F4)

A data product is a named, certified bundle — tables, views, metrics, glossary
terms, and contracts, by stable reference — that is the unit of consumption for
humans and agents. It carries an owner, a free-text SLA, and a certification
status.

`GET /api/v2/products/{id}/status` is the product-level status page: it rolls up
the product's certification, its members by kind, and — for each *table* member
whose reference resolves — that table's quality status and trust score (reusing
the same quality signals as the per-table status page, E-F5). The rolled-up health
is the worst member-table status, or `no_signal` when no member exposes one.
Unresolvable member references are reported as `unknown`, never dropped — honest
about coverage.

## Authorization & audit

Every semantics mutation is **management-gated** (`require_management`: admin role
or any `MANAGE_WAREHOUSE` grant) — the semantic layer is workspace-level metadata
curated by data owners/stewards, not per-warehouse RBAC-scoped like the IRC
surface. Every mutation writes its `audit_log` row and outbox event on the *same*
transaction as the state change (the codebase-wide invariant: no mutation without
its audit row). The universal-view translation cache is a *derived* cache, not a
record of user intent, so it carries no audit row — the view-definition mutations
it derives from are audited on the view path.

## Data model (migration 0021)

- `metrics` — measure + dimensions + filters + grain + owner + certification.
- `glossary_terms`, `glossary_links` — stewarded vocabulary and its asset links.
- `data_products`, `data_product_members` — certified bundles and their members.
- `view_representation_cache` — the universal-view translation cache (G-F1).

All rows workspace-scoped; certification is `draft | certified | deprecated` (a
governance signal surfaced verbatim, never a claim of correctness); names are
unique per workspace, case-insensitively.

## LLM-assist posture

Metric compilation and the universal-view path are **deterministic SQLGlot
only**. The optional BYO-key LLM-assist fallback lives entirely inside the sidecar
and is off unless an operator configures it there; it runs only *after* SQLGlot
has raised, its output is always parse-back-validated and labelled `best_effort`,
and it is never contacted in tests or by default. The Rust server never selects or
contacts an LLM — it forwards a request and reads back a labelled result.
