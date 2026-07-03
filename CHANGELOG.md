# Changelog

All notable changes to Meridian will be documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.1.0/).
There are no releases and no version tags yet; until the first release,
everything lives under Unreleased and may change without notice. Once
releases begin, the project will adhere to
[Semantic Versioning](https://semver.org/spec/v2.0.0.html).

## [Unreleased]

### Added

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
