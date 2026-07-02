# 003. Metadata-forward indexing: spec-compliant files, write-through index in Postgres

## Status

Accepted — 2026-07-02

## Context

In standard Apache Iceberg, a table's `metadata.json` and manifest files on
object storage are what engines actually read; the catalog's job is to hold
the pointer to the current metadata file and swap it atomically on commit.
A catalog that stores *only* the pointer must open and scan files on object
storage to answer any question beyond "where is the current metadata":
searching tables by schema, reporting snapshot history, computing table
health (file counts, small-file ratios, snapshot age), or planning a scan.

Meridian's operational features — search, health monitoring, observability,
maintenance planning — all need to ask those questions constantly, across
every table in the catalog. Answering them by scanning object storage does
not scale and puts object-store latency on interactive paths.

Two constraints frame the design:

1. **File-spec compliance is non-negotiable.** Engines must be able to read
   tables through the standard Iceberg metadata files. Any design where the
   database becomes the only place table metadata exists breaks the
   ecosystem contract.
2. **Derived data drifts.** Any secondary copy of metadata will eventually
   disagree with the primary due to bugs, partial failures, or out-of-band
   writes, so the design must treat verification and repair as a built-in
   component, not an afterthought.

## Decision

**The file-spec-compliant `metadata.json` (and manifests) on object storage
remain the source of truth that engines see.** Meridian reads and writes them
losslessly (see ADR 001 on unknown-field preservation) and never requires an
engine to understand anything beyond the standard.

**Every commit write-through-indexes the metadata into Postgres.** In the
same transaction that swaps the current-metadata pointer, the catalog also
persists the structured content of the new metadata: schemas, snapshots,
partition summaries, column statistics, file counts and sizes, and table
properties, in queryable relational form. The index is written on the commit
path, not lazily by a crawler, so it is current the moment a commit is
visible.

From that index Meridian serves:

- **Search, health, and observability** — instantly, with zero object-storage
  scans; every dashboard and API answer is a SQL query.
- **Scan planning** — partition pruning and file selection from the index,
  falling back to reading manifests from object storage whenever the index
  is stale or missing for a snapshot.

**A `reconcile` job continuously verifies index↔file agreement.** It re-reads
metadata files, compares them against the index, repairs drift in the index
(files win — they are the source of truth engines see), and raises an alarm
on divergence, since divergence indicates a bug in the write-through path.

### Alternatives considered

- **Pointer-only catalog, scan on demand.** Simplest and drift-free, but
  every operational question costs object-store round-trips; catalog-wide
  questions ("all tables with >10k small files") become batch jobs. Rejected:
  the operational features are the point of the project.
- **Database-native metadata as the sole source of truth.** Fastest possible
  design and no reconciliation needed, but engines could no longer read
  tables via standard Iceberg metadata files — it exits the ecosystem.
  Rejected outright.
- **Lazily populated cache (crawler/refresh-on-read).** Avoids commit-path
  work but serves stale answers by design and needs the same reconciliation
  machinery anyway. Rejected in favor of paying the small, bounded cost at
  commit time.

## Consequences

- **Positive:** search, health, and observability run with zero object-store
  scans and are consistent with the latest commit; scan planning gets a fast
  path with a correct fallback; operational features are ordinary SQL over
  indexed metadata rather than distributed file-scanning jobs.
- **Positive:** the design stays inside the Iceberg standard while gaining
  the latency benefits of database-resident metadata, and it is aligned with
  the upstream community's direction of making table metadata more directly
  queryable.
- **Negative — commit-path cost:** each commit writes more rows in the
  pointer-swap transaction. This adds bounded latency and write
  amplification to commits; the index write is on the crown-jewel path and
  therefore inherits its testing burden (property tests, fault injection).
- **Negative — drift is a standing threat:** the reconcile job is mandatory
  infrastructure, not an optimization. Index-only answers are trusted only
  because reconciliation continuously proves them; an alarm from reconcile
  is a correctness bug, not noise.
- **Negative — storage growth:** the index grows with snapshot and schema
  history. Retention/pruning policy for indexed history is follow-up work.
- **Neutral:** index tables (`table_snapshots`, `table_schemas`,
  `table_stats`, …) evolve with the Iceberg spec; new spec versions require
  index schema migrations, which is accepted and covered by the normal
  migration discipline.
