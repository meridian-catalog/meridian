# 008. Inbound catalog mirrors: foreign assets reuse the native asset tables

## Status

Accepted

## Context

Pillar B (federation) starts with **B-F1: inbound mirrors** — continuously
syncing an external catalog into Meridian as *foreign* (read-only) assets so
that every Meridian read-side feature (search today; lineage, health, and the
agent gateway later) spans everything an enterprise runs, not just what was
written through Meridian. The adoption thesis is "mounted before adopted": point
Meridian read-only at an existing catalog and get the sprawl/savings diagnosis
before moving a byte.

The first source type is another **Iceberg REST Catalog (IRC)** — Polaris,
Lakekeeper, Unity's IRC endpoint, Snowflake Horizon's IRC surface, BigLake,
Nessie, and Meridian itself (Meridian *is* an IRC, so Meridian-to-Meridian is a
valid IRC-to-IRC mirror). AWS Glue's native API and Hive Metastore's thrift
interface are named in the spec but are out of scope for this first cut and
documented as future source types. The IRC path covers the majority of the
neutral-catalog landscape with one protocol client.

Two design questions had to be settled before any code:

1. **Where do foreign assets live?** Either (a) reuse the existing
   `namespaces` / `tables` rows with a nullable `mirror_id` marking a row as
   foreign, or (b) add a separate `foreign_assets` index alongside the native
   tables.
2. **How is a foreign table made non-writable**, and where is that enforced, so
   that a mirror is genuinely conflict-free (the external catalog stays the
   commit authority)?

## Decision

### This change composes with the sprawl half of Pillar B

Pillar B was split across two work items. The **sprawl/registration half
(B-F5)** — the `catalog_mirrors` config table, the `mirror_assets` per-asset
index and `mirror_sync_runs` history (migration `0014_federation_mirrors`), the
`/api/v2/mirrors` CRUD + `/api/v2/federation/sprawl` API, and the CLI — landed
first and deliberately left the actual sync engine as an integration seam
(`meridian_store::federation::record_sync_result`, documented "the actual sync
engine lives in the federation crate/worker"). **This change is that engine
(B-F1).** It builds *on* `catalog_mirrors` — it does not introduce a second
mirror-config table — and it fills in what a sync run does: fetch the remote
catalog and materialize its assets.

The one thing the sprawl half could not do, and this change adds, is make
mirrored assets first-class to Meridian's **read-side features** (search,
health, later lineage). `mirror_assets` is a flat sprawl index that those
features do not query; a mirrored table only becomes searchable and
health-scorable once it exists as a row in the native `tables` table. So a sync
run now does **both**: it materializes each mirrored table as a native foreign
asset (for the read-side features) **and** upserts the `mirror_assets` row (for
the sprawl summary's location/ownership roll-ups). The two indexes serve two
different consumers and are written in the same sync transaction.

### Foreign assets reuse the native `namespaces` / `tables` tables

A mirror owns a dedicated **warehouse** (the existing container that maps 1:1
onto an IRC `{prefix}`), created lazily on first sync and named
`mirror__<mirror-name>`. The mirror's namespaces and tables are upserted as
**ordinary rows in the existing `namespaces` and `tables` tables**, each
carrying a new nullable `mirror_id` foreign key to `catalog_mirrors`
(migration `0015_federation_foreign_assets`). A row with `mirror_id IS NULL` is
native (writable); a row with `mirror_id` set is foreign (read-only).

Reusing the native tables — rather than a parallel `foreign_assets` index — is
the decisive call because **it is what makes "all read-side features work on
foreign assets immediately" true for free**:

- **Search** already indexes `tables` and `namespaces` via the migration-0010
  triggers (`search_tsv`). A foreign table inserted into `tables` with its
  `schema_text` is searchable the instant it lands, with zero new search code.
- **Health, reconciliation, the write-through snapshot index** all query
  `tables` / `table_snapshots` by the same shape. A foreign table's indexed
  snapshots feed the health model with no special-casing.
- One asset model means one place to evolve schema, one set of foreign keys,
  one audit vocabulary. A second `foreign_assets` table would fork every
  read-side query into "native or foreign" branches forever.

The cost of this choice is that every **write path** must now check
`mirror_id` and refuse foreign rows. That cost is small, local, and exactly
where it should be (the commit path), and it is far cheaper than teaching every
*read* path about a second table. See enforcement below.

The mirror's warehouse carries `storage_config['meridian:foreign'] = 'true'`
and a `meridian:mirror_id` marker so operators and the console can tell a
foreign warehouse from a native one, and so warehouse-level guards (e.g.
rejecting a native `createTable` under a foreign warehouse) have a cheap signal
without a per-table lookup.

### Foreign tables are read-only, enforced at the commit boundary

Foreign assets never accept writes. Enforcement lives at the two IRC mutation
entry points that can target an existing table — `commit_table` and
`commit_transaction` (the multi-table commit) — plus the `register` and
create-under-a-foreign-warehouse paths:

- A **commit against a table whose row has `mirror_id` set** is rejected with
  **HTTP 409 `CommitFailedException`** and a message that names the mirror and
  says the external catalog is the write authority. 409 is the spec's commit-
  rejection status; the message makes clear this is a *permanent* property of a
  foreign table, not a retryable race.
- **Create / register under a foreign warehouse** is rejected the same way
  (a foreign warehouse holds only mirror-synced assets).

The check is one boolean already loaded with the table row (the commit path
loads the `TableRecord` before authorizing), so enforcement adds no extra query
on the hot path. `drop`, `rename`, `updateProperties`, and metrics are covered
by the same foreign-row guard applied in their handlers.

Enforcement is defense-in-depth, not the only barrier: the sync worker uses a
dedicated `federation:sync:<mirror>` principal and only ever *upserts* foreign
rows; nothing in the product ever routes a writer commit to a foreign table on
purpose. The commit-boundary check is what guarantees a *mis*-routed or
deliberately hostile writer still cannot corrupt a mirror.

### The sync engine and its own small IRC client

`meridian-federation` is a new crate. It contains:

- A **minimal HTTP IRC client** (`reqwest` + `serde_json`, rustls, no generated
  SDK — matching the CLI's dependency-light client) that speaks exactly the
  read subset a mirror needs: `GET /v1/config`, list namespaces, list tables,
  and `loadTable`. Auth modes: none, static bearer, and OAuth2 client-
  credentials (token fetched from the source's token endpoint and refreshed).
- A **sync engine** that walks the source catalog's namespaces and tables,
  loads each table's metadata, and upserts it as a foreign asset with its
  schema (for search), snapshots, current pointer, format version, and the
  source `metadata_location`. Sync is **incremental**: a table whose
  `metadata_location` is unchanged since the last sync is not re-indexed. A
  table that disappeared from the source is **removed** from the mirror through
  the audited drop path, so a mirror reflects source deletions; the removal is
  recorded in the append-only audit log (which outlives the row), so no history
  is lost. Removal only ever touches foreign rows of that mirror.
- A **background sync worker** (mirroring the maintenance/events worker loops:
  a claim-run-repeat loop that syncs mirrors whose `sync_interval` has elapsed)
  plus a manual "sync now" API trigger.

All mirror mutations (create/update/delete a mirror, and every foreign-asset
upsert) write their audit row and outbox event in the mutating transaction, the
same discipline as every other Meridian mutation.

### Why not proxy reads live to the source instead of indexing?

A live-proxy mirror (forward each `loadTable` to the source on demand) would
avoid the copy but defeats the entire point: search, health, lineage, and the
savings diagnosis all need the metadata *indexed locally* to work without a
per-asset round-trip to a catalog that may be slow, rate-limited, or
intermittently reachable. Indexing on a sync cadence, with a freshness
indicator per mirror, is the model every federation product (UC foreign
catalogs, Glue federation) converged on, and it is what B-F5 (the sprawl
dashboard) reads from. Live serving of foreign *data* is a separate, later
concern (the read still goes to the source's storage via the foreign table's
own locations); this ADR is about metadata federation only.

## Consequences

- Every new write path must remember the foreign-row guard. This is a
  documented invariant (foreign assets are non-writable) and is covered by an
  integration test that asserts a commit to a foreign table is rejected.
- Migration `0015_federation_foreign_assets` adds the nullable `mirror_id`
  column (referencing `catalog_mirrors`) on `namespaces` and `tables`, both
  `ON DELETE CASCADE` (deleting a mirror removes its foreign rows; native rows,
  with `mirror_id IS NULL`, are untouched). The `catalog_mirrors` /
  `mirror_assets` / `mirror_sync_runs` schema is unchanged from
  `0014_federation_mirrors`.
- **Table-UUID uniqueness is scoped to native tables.** Migration 0003's global
  `tables_table_uuid_unique` index is replaced by a partial index
  (`WHERE mirror_id IS NULL`). A mirrored table legitimately shares its
  Iceberg table-uuid with its source (it is the same table), and one source
  table may be mirrored by more than one mirror or exist both natively and
  mirrored — a global uuid-unique index would reject the second such row.
  Native tables keep the exact one-live-table-per-uuid guarantee.
- **Reading a foreign table's metadata** uses storage derived from its own
  `metadata_location` (the source's storage), not the mirror's synthetic
  `mirror://` warehouse root. Filesystem and public object-store sources work
  as-is; source credentials for a non-public object store are a separate,
  later concern — metadata federation reads the pointer/schema, not data files.
- The IRC client is intentionally small and read-only. When Glue/HMS source
  types land, they add sibling source-type modules behind the same
  upsert-foreign-asset engine; the asset model does not change.
- Because foreign tables live in the native tables, the RBAC, events, and audit
  surfaces treat them uniformly — a foreign table can be granted READ, appears
  in the event feed when synced, and is audit-attributed to the sync principal.
