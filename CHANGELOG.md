# Changelog

All notable changes to Meridian will be documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.1.0/).
There are no releases and no version tags yet; until the first release,
everything lives under Unreleased and may change without notice. Once
releases begin, the project will adhere to
[Semantic Versioning](https://semver.org/spec/v2.0.0.html).

## [Unreleased]

### Added

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

### Security

- **There is no authentication or authorization yet.** Every endpoint,
  including warehouse management, is open to anyone who can reach the port.
  Do not expose Meridian to untrusted networks. OIDC authentication is the
  next milestone; see the warning in
  [docs/api-status.md](docs/api-status.md).
