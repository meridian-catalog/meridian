# 001. M0 foundation: workspace layout, storage conventions, audit chain

## Status

Accepted

## Context

M0 lays the code foundation: a Rust workspace, the Postgres access layer with
its first migration, error/config/telemetry conventions, the beginning of the
Iceberg metadata model, and an HTTP server skeleton serving the Iceberg REST
`/v1/config` endpoint. Decisions made here set defaults the rest of the
system inherits, so the significant ones are recorded together.

## Decision

1. **Cargo workspace, edition 2024, shared lints.** Crates:
   `meridian-common` (ids, errors, config, telemetry), `meridian-store`
   (Postgres, migrations, outbox, audit), `meridian-iceberg` (domain model),
   `meridian-server` (axum wiring and routes), `meridian-cli` (the `meridian`
   binary). Lints are defined once at the workspace level
   (clippy `all` + `pedantic` as warnings, denied in CI).

2. **ULID identifiers, stored as TEXT.** All entity IDs are ULIDs behind
   per-entity newtypes (`OrgId`, `WorkspaceId`, `CatalogId`, `NamespaceId`,
   `TableId`) so IDs cannot be confused at compile time. They are stored as
   their 26-character canonical string in `TEXT` columns: time-ordered like
   the sequence-based alternatives, index-friendly, and trivially debuggable.

3. **Runtime-checked SQL for now.** `sqlx::query`/`query_as` rather than the
   compile-time-checked macros, so building and CI do not require a live
   database. Revisit once the schema stabilizes.

4. **Namespace hierarchy as `TEXT[] levels` + GIN index.** Multi-level
   namespaces store their full path as an array of levels with a uniqueness
   constraint per catalog. This avoids an `ltree` extension dependency and
   keeps "Postgres, unmodified, is the only required dependency" true.
   Adjacency/parent queries can be added later without a schema change
   (prefix comparison on the array).

5. **Hash-chained, append-only audit log.** `audit_log` rows carry
   `prev_hash`/`hash` where `hash = sha256(prev_hash || canonical_json(entry))`
   over a canonical JSON rendering (sorted keys, no whitespace, fixed
   microsecond UTC timestamps). Appends serialize on a Postgres advisory
   lock; a trigger rejects `UPDATE`/`DELETE`. Tampering with any row breaks
   every subsequent hash, and the whole chain is verifiable with one scan.

6. **Transactional outbox from day one.** `events_outbox` is written in the
   same transaction as the state change it describes; a relay will publish
   and stamp `published_at` (M2). No state change without its event.

7. **Unknown-field preservation in the Iceberg model.** Every metadata serde
   struct carries a flattened `extra` map. Parsing and re-serializing a
   `metadata.json` is lossless even for fields the typed model does not know
   (v3 fields, statistics, engine extensions). The catalog must never destroy
   metadata written by other tools.

8. **Error envelope and config conventions.** One `MeridianError` type maps
   onto the Iceberg REST error envelope (`{"error":{"message","type","code"}}`);
   internal error detail is logged, never returned to clients. Configuration
   layers defaults < TOML file < `DATABASE_URL` < `MERIDIAN__*` environment
   variables.

## Consequences

- Positive: no external dependencies beyond Postgres; tier-0 conventions
  (typed IDs, audit chain, outbox, lossless metadata) exist before any
  feature code that could shortcut them; CI needs no database to compile.
- Negative: runtime-checked SQL defers query/schema mismatch detection to
  tests; the advisory-lock audit append serializes audit writes (acceptable —
  audit writes are low-rate; revisit with batching if profiling ever says
  otherwise); `TEXT[]` namespaces make some hierarchy queries less elegant
  than `ltree`.
- Follow-ups: v1/v3 metadata completeness and a typed schema type tree (M1),
  commit-protocol design doc + property tests before the commit path (M1),
  outbox relay (M2), warehouse-aware `/v1/config` (M1).
