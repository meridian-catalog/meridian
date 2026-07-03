# Changelog

All notable changes to Meridian will be documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.1.0/).
There are no releases and no version tags yet; until the first release,
everything lives under Unreleased and may change without notice. Once
releases begin, the project will adhere to
[Semantic Versioning](https://semver.org/spec/v2.0.0.html).

## [Unreleased]

### Added

- **Server-side scan planning** (IRC 1.11+ `planTableScan` /
  `fetchPlanningResult` / `cancelPlanning` / `fetchScanTasks`;
  [design doc](docs/design/scan-planning.md),
  [API status → Scan planning](docs/api-status.md#scan-planning)).
  Point-in-time scans with full filter pushdown (manifest partition
  summaries → partition tuples → column statistics, all inclusive),
  position/equality delete files attached to each task by the table
  spec's sequence-number and partition scope rules (deletion vectors
  supersede position delete files; equality deletes stats-pruned only
  over their equality columns), and per-file `residual-filter`s with
  exact partition folding — the future row-policy injection point.
  Small tables (≤ 2000 live data files by default) answer `completed`
  inline; larger tables run on a bounded async pool with persisted,
  deterministically paginated results fetched by opaque `plan-task`
  tokens, a TTL (default 1 h), cancellation, and an expiry sweep.
  Manifests are cached in two bounded tiers (in-process parsed LRU +
  cross-pod Postgres byte cache, migration 0011) with hit counters in
  every plan summary; plan submission/cancellation are audited and
  submission emits a `scan.planned` event. `READ` on the table is
  enforced on every call. Verified against the conformance suite's real
  Spark merge-on-read table (exact live-row reproduction) and a
  synthetic 10,000-file fixture; incremental scans are refused with 406
  (not yet implemented).
  (`meridian-iceberg`; groundwork for IRC scan planning — no new
  endpoints yet). Reading of manifest lists and manifests (v1 and v2)
  resolves fields by Iceberg **field id** from the writer schema rather
  than by name or position, tolerating the historical v1 field
  spellings and int→long / float→double bound promotions; the v3
  additions (deletion-vector `content_offset`/`content_size_in_bytes`,
  `referenced_data_file`, `first_row_id`) are parsed and preserved but
  not interpreted. Writing emits spec-shaped v1/v2 manifests and
  manifest lists (field-id attributes, key-value metadata, deflate) and
  refuses v3-only fields rather than dropping them; written files were
  verified readable by pyiceberg. Typed single values (`Datum`) cover
  every primitive incl. decimal/uuid/timestamp bound decoding
  (Appendix D binary and REST JSON forms). The REST
  `PlanTableScanRequest.filter` expression tree (exact OpenAPI names)
  binds against a schema, rewrites `not` away, and evaluates
  three-valued ("unknown keeps the file") against data-file column
  statistics, partition tuples, and manifest-list partition summaries
  via the spec's inclusive projection (identity/bucket/truncate/
  temporal transforms; 32-bit Murmur3 verified against every spec
  Appendix B test vector). Conformance evidence: parsed manifests match
  pyiceberg's own view field-for-field on pyiceberg-written v1/v2
  tables and on the conformance suite's real Spark merge-on-read table
  (position deletes included), and a property suite asserts the
  soundness invariant — a file containing a row matching the filter is
  never pruned by any evaluator.
- **Remote signing (`X-Iceberg-Access-Delegation: remote-signing`)**
  ([ADR 005](docs/adr/005-remote-signing.md),
  [API status → Remote signing](docs/api-status.md#remote-signing)). The
  spec's second delegation mechanism: `POST .../tables/{table}/sign`
  SigV4-signs client-built S3 requests with warehouse credentials that
  never leave the server. Rides the vending opt-in
  (`vending = "sts" | "static"`). The authorization policy is the
  boundary: requests must resolve inside the table's location prefix
  (path-style and virtual-host addressing, percent-decoding, traversal
  and copy-source checks, endpoint-host allowlist, scoped bucket listings,
  `DeleteObjects` body-key validation) and within the caller's RBAC access
  (`GET`/`HEAD` with `READ`; writes with `WRITE`/`COMMIT`). Every
  decision — allow and deny — writes an audit row and outbox event in one
  transaction before the response leaves. Table loads/creates carrying
  the header advertise `s3.remote-signing-enabled` plus a relative
  `s3.signer.endpoint` (vended credentials keep precedence when both
  mechanisms are listed). Verified sign→execute against MinIO and end to
  end with a credential-less pyiceberg 0.11 client (fsspec FileIO); not
  yet cloud-verified against real AWS.
- **Credential vending (S3/MinIO) and external endpoint advertisement**
  ([docs/design/vending.md](docs/design/vending.md)). New
  `meridian-vending` crate plus the IRC surfaces: `GET
  .../tables/{table}/credentials` (`loadCredentials`) and the
  `X-Iceberg-Access-Delegation: vended-credentials` header on
  `createTable`/`loadTable`. Warehouses opt in via storage options —
  `vending = "sts"` vends short-lived STS session credentials scoped by an
  inline session policy to the one table's location prefix (verified
  against MinIO end to end: read-only vs read-write scoping, cross-table
  prefix isolation, TTL; standard AWS STS semantics but not yet
  cloud-verified against real AWS), `vending = "static"` passes the
  warehouse's own keys through for STS-less self-hosted setups (explicit
  opt-in; the config-passthrough credential denylist stays absolute
  otherwise), GCS/Azure honestly refuse as not implemented. Access follows
  RBAC (`WRITE`/`COMMIT` → read-write, `READ` → read-only) and **every
  vend writes an audit row and outbox event in one transaction before
  credentials leave the server**. The
  new `endpoint.external` storage option makes all client-facing config
  advertise an external object-storage endpoint while the server keeps
  using the internal one — the fix for the documented
  `host.docker.internal` engine-networking issue. The e2e suite gains a
  pyiceberg round trip whose client holds zero S3 configuration — only
  the catalog URI.
- **Catalog events: outbox relay, webhooks, and a queryable feed**
  ([docs/design/events.md](docs/design/events.md)). Every catalog mutation
  already wrote a transactional-outbox row; a background relay inside
  `meridian serve` now publishes them as CloudEvents 1.0 JSON — in bounded
  `SKIP LOCKED` batches, crash-safe (at-least-once), strictly ordered per
  aggregate even with concurrent server replicas, and draining any
  pre-existing backlog on first boot. Consumption surfaces: **webhooks**
  (`/api/v2/webhooks` CRUD; HMAC-SHA256-signed deliveries with
  per-endpoint exponential retry, dead-letter status and full delivery
  history via `GET /api/v2/webhooks/{id}/deliveries`), a **queryable
  feed** (`GET /api/v2/events`, keyset cursor = event id, gap-free via a
  publication frontier), **named durable consumers**
  (`/api/v2/events/consumers` + `next`/`commit`, persistent at-least-once
  offsets), and `meridian events tail` in the CLI. All events endpoints
  require management access in `oidc` mode (documented decision; a
  finer-grained privilege is deferred). Migration 0008; broker (NATS/
  Kafka) sinks are future work, tracked in the design doc.

- **Asset search v1** (`GET /api/v2/search`, CLI `meridian search <query>`):
  ranked Postgres full-text search across tables, views, and namespaces —
  matching asset names, namespace paths, table **column names and docs**
  (re-indexed from the current schema on every create/register/commit, in
  the same transaction as the pointer write), and `properties.comment`,
  with exact-name/prefix boosts, `ts_headline` snippets, and keyset
  pagination. In `oidc` mode results are filtered to the caller's RBAC
  visibility inside the query (no per-result authorization round-trips);
  an ungranted caller gets an empty list. Trigger-maintained, GIN-indexed
  tsvectors (migration 0010; the migration header documents the
  trigger-vs-generated-column decision). Known gaps are documented in
  [docs/api-status.md](docs/api-status.md#search-get-apiv2search): view
  schemas are not column-indexed yet, no usage-based ranking, no semantic
  search.
- **Engine conformance matrix with Flink, Spark, and Trino smoke suites**
  ([conformance/engines/](conformance/engines/README.md)): pyiceberg
  0.11.1 and DuckDB 1.5.4 pass the e2e suite (full table lifecycle,
  concurrent writers, views, S3/MinIO storage-config vending); Flink 1.20
  passes batch inserts, checkpoint-driven streaming commits, and
  read-back, and exposed a real `createTable` bug — Meridian rejected the
  connector's 0-based provisional field ids instead of assigning fresh
  ids server-side (since fixed; see **Fixed** below). Spark 3.5.6
  (iceberg-spark-runtime 1.11.0) passes a fuller smoke
  ([conformance/engines/spark/](conformance/engines/spark/README.md)):
  partitioned DDL, batched inserts, aggregates, `ADD COLUMN` with NULL
  backfill, `VERSION AS OF` time travel, merge-on-read `MERGE INTO` and
  `DELETE FROM` (position-delete files verified over REST), and Iceberg
  view create/read — which exposed the `createView` twin of the field-id
  bug (since fixed; see **Fixed** below). Trino 482 passes its own suite
  ([conformance/engines/trino/](conformance/engines/trino/README.md)):
  schema and partitioned-table DDL (via the `stage-create` →
  `assert-create` path; the schema needs an explicit `location`, a
  documented gap), inserts, schema evolution, views, and — cross-engine —
  reads Spark's post-`MERGE`/`DELETE` table exactly, proving Spark's
  merge-on-read position-delete files apply correctly through a second
  engine. Trino cleanly rejects Spark's `spark`-dialect view (cross-engine
  view portability needs multi-dialect representations; documented).
- **Catalog benchmark harness (`meridian-bench`)** under
  [testing/bench/](testing/bench/README.md) — catalog-plane HTTP latency
  scenarios (`get-config`, `load-table` concurrency sweep, `commit`)
  with HDR-histogram stats, plus scripts that boot Apache Polaris and
  Lakekeeper against the same local infra. First published local-dev
  results (Meridian vs Polaris 1.5.0 vs Lakekeeper v0.13.1, with
  caveats): [docs/benchmarks/](docs/benchmarks/README.md).
- **RBAC enforcement on the view surface and principal listing**: views
  are now a first-class grant securable (`securable_type = "view"`, with
  the same hierarchy inheritance as tables), and every view endpoint
  enforces authorization in `oidc` mode — list → `LIST_TABLES`, create →
  `CREATE_VIEW`, load/exists → `READ`, replace → `COMMIT`, drop → `DROP`,
  rename → `WRITE` (source view) + `CREATE_VIEW` (destination namespace).
  `GET /api/v2/principals` now requires management access (admin or
  `MANAGE_WAREHOUSE`), closing an identity-enumeration gap. Previously the
  view endpoints performed no authorization checks at all in `oidc` mode.
  Full mapping: [docs/api-status.md](docs/api-status.md#authorization-rbac).
- **Iceberg REST Catalog surface**, mounted at both `/v1` and `/iceberg/v1`,
  with `{prefix}` resolving to a named warehouse: `config` (with
  warehouse-to-prefix resolution and an advertised `endpoints` list), the
  full namespace lifecycle (list with pagination, create, load, exists,
  drop, atomic property updates), and the table lifecycle — create
  (including `stage-create`), load with strong ETags and `If-None-Match`,
  list with pagination, drop (with a purge event on `purgeRequested`),
  rename, register, and metrics ingestion. Endpoint-by-endpoint status and
  documented spec divergences: [docs/api-status.md](docs/api-status.md).
- **Transactional commit path**: single-table commits and atomic multi-table
  transactions with requirement checks, bounded compare-and-swap retry, and
  `Idempotency-Key` support (recorded receipts, 24-hour replay window,
  fingerprint-mismatch rejection). Design:
  [docs/design/commit-protocol.md](docs/design/commit-protocol.md).
- **Typed Iceberg table-metadata model** for format versions 1–3, with the
  metadata update engine (all spec update actions) and commit requirement
  checks; parse/serialize round-trips are tested to be lossless, including
  preservation of fields the typed model does not know about.
- **PostgreSQL catalog store** (the only required runtime dependency):
  startup migrations, warehouses/namespaces/tables, a hash-chained audit
  log, a transactional outbox for catalog events, a snapshot forward-index,
  and idempotency receipts.
- **Object storage IO** over OpenDAL: local filesystem and S3 backends with
  conditional (if-not-exists) metadata writes.
- **Warehouse management API** (`/api/v2/warehouses`): create, list, delete.
- **OIDC authentication** (`auth.mode = "oidc"`, default `disabled` with a
  loud startup warning): validates bearer tokens from configured external
  identity providers (RS256/ES256 family, JWKS discovery and rotation-aware
  caching, exp/nbf/iss/aud checks), distinguishes user vs. service
  principals for audit, JIT-provisions a local `principals` row per
  identity, and keeps `/healthz`/`/readyz` open so liveness never depends
  on the IdP. Principal visibility via `GET /api/v2/principals`. Details:
  [docs/api-status.md](docs/api-status.md#authentication).
- **Iceberg REST view surface**: the full view lifecycle on both mounts —
  list (paginated), create (multi-dialect SQL representations, validated by
  a typed view-metadata builder for the view spec's format version 1),
  load, exists, replace (`assert-view-uuid` + compare-and-swap pointer
  commit with bounded retry, audit + outbox in the swap transaction), drop,
  and rename. Tables and views share one name space per namespace (enforced
  from the view side; remaining table-side gap documented). Status and
  divergences: [docs/api-status.md](docs/api-status.md#views).
- **Storage config passthrough**: `LoadTableResult.config` and
  `LoadViewResult.config` now carry the warehouse's non-secret storage
  options under Iceberg client property names (`s3.endpoint`,
  `s3.region`/`client.region`, `s3.path-style-access`). Credential material
  is never forwarded — enforced by an explicit denylist and leak tests.
- **`meridian` CLI**: `serve` (migrate + serve) plus `warehouse`,
  `namespace`, and `table` admin subcommands against a running server;
  layered configuration (defaults < `meridian.toml` < `DATABASE_URL` <
  `MERIDIAN__*` environment variables).
- **Development and CI scaffolding**: Docker Compose dev stack, multi-stage
  Dockerfile, CI (rustfmt, clippy, workspace tests against Postgres 16,
  Docker build), and a tag-driven release workflow producing Linux
  x86_64/aarch64 tarballs with SHA256SUMS (checksums only — artifacts are
  not signed yet).
- **Documentation**: development guide, architecture decision records
  (ADRs 001–004), commit-protocol design document, and the API status
  matrix.

### Fixed

- **`createTable` now treats request field ids as provisional and assigns
  fresh ones server-side**, matching the Java reference implementation
  (`AssignFreshIds`): schema field ids are reassigned 1-based (nested
  struct/list/map fields included), and `identifier-field-ids`,
  partition-spec source ids, and sort-order source ids are remapped
  accordingly. This fixes the Flink `CREATE TABLE` rejection found by the
  engine conformance smoke (`invalid schema: field id 0 is not positive` —
  Flink's connector numbers provisional ids from 0), and the smoke's
  pre-create workaround is removed; Flink table DDL now passes end to end.
  In the same change, a table created with a partition spec now carries
  exactly one spec, numbered 0, like the reference implementation
  (previously the requested spec was numbered 1 next to a phantom empty
  spec 0 — documented divergence (d), now resolved). Commit-path
  `add-schema` updates still validate field ids strictly; ids there are
  real, not provisional.
- **`createView` treats request field ids as provisional too.** Spark
  3.5's `CREATE VIEW` numbers the view's output schema from 0, and
  Meridian rejected it with the same `field id 0 is not positive` error
  the Flink smoke hit on tables. View create requests now get fresh
  1-based field ids server-side, mirroring `createTable`. Found by the
  [Spark conformance smoke](conformance/engines/spark/README.md). Known
  remaining gap: `replaceView` still validates ids strictly, so Spark's
  `CREATE OR REPLACE VIEW` on an existing view fails; documented in
  [docs/api-status.md](docs/api-status.md#views).

### Security

- **Authentication and authorization are off by default.** With the
  default `auth.mode = "disabled"`, every endpoint — including warehouse
  and RBAC management — is open to anyone who can reach the port, and
  authorization is bypassed entirely. Do not expose a disabled-mode server
  to untrusted networks. With `auth.mode = "oidc"`, access is
  deny-by-default RBAC across the namespace, table, and view surfaces; see
  the warning in [docs/api-status.md](docs/api-status.md).
