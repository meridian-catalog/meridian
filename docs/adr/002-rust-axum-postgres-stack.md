# 002. Rust/axum/Postgres stack; Postgres as the only required dependency

## Status

Accepted — 2026-07-02

## Context

Meridian sits on the tier-0 path of a data platform: every engine (Spark,
Trino, Flink, DuckDB, PyIceberg, …) resolves table metadata through the
catalog on every read and write. If the catalog is slow, every query is slow;
if it is down, every engine is down; if it corrupts a metadata pointer, data
is lost. That dictates the requirements for the core service:

- **Predictable tail latency.** Metadata loads are on the critical path of
  interactive queries. Latency spikes in the catalog surface as latency
  spikes in every engine.
- **Memory safety and correctness.** The commit path performs concurrent
  compare-and-swap updates to table pointers. Whole classes of memory and
  data-race bugs need to be ruled out by construction, not by review.
- **A boring operations story.** The people who run catalogs are platform
  teams who already run too much. The deployment target is a single static
  binary plus one database — no runtime, no tuning folklore, no sidecar zoo
  required to get started.
- **A minimal dependency footprint.** Every additional required service
  (queue, cache, search engine, coordinator) multiplies the operational
  burden for self-hosters and the failure modes we must reason about.

## Decision

**The core service is Rust**, built on:

- **axum** for the HTTP surface (Iceberg REST API and management API),
- **tokio** as the async runtime,
- **sqlx** for PostgreSQL access (runtime-checked queries for now — see
  ADR 001).

Rust gives us memory safety without garbage collection, which is what makes
tail latency *predictable* rather than merely good on average, and it
compiles to a single static binary, which is the whole ops story: one
artifact, one process, one database.

**PostgreSQL 16+ is the only required dependency.** Postgres holds all
catalog state, and is deliberately asked to do the jobs that usually pull in
extra infrastructure:

- **State** — namespaces, table pointers, schemas, the metadata index.
- **Queue** — background job dispatch will use `FOR UPDATE SKIP LOCKED`
  (planned; not yet implemented) instead of a message broker.
- **Audit** — the append-only, hash-chained audit log (ADR 001).
- **Search** — Postgres full-text search for asset search (planned) instead
  of a separate search engine.

Optional dependencies may be added later for scale (e.g. a broker for event
fan-out, a search engine beyond what Postgres FTS handles), but each one
requires its own ADR with a written justification, and none may ever be
required for a correct, complete deployment.

**Python is quarantined to a future optional sidecar.** Cross-dialect SQL
transpilation for the semantic layer needs SQLGlot, which has no Rust
equivalent of comparable coverage. That code will live in a separate,
stateless sidecar process — never in the core, never on the commit path, and
never required to run the catalog.

**The web console comes later as a Next.js/TypeScript app** and talks only
to the management API; it adds no server-side behavior of its own.

### Alternatives considered

- **JVM (Java/Kotlin).** The strongest ecosystem argument: the Iceberg
  reference implementation is Java, and most engine developers live there.
  Rejected for the core because GC pauses work against predictable tail
  latency on a tier-0 path, and a JVM deployment (runtime + heap tuning) is
  a heavier ops story than a static binary.
- **Go.** Comparable ops story (static binary, fast builds, larger
  contributor pool). Rejected because Rust's type system and ownership model
  catch more of the bug classes we care about on the commit path at compile
  time, and the Rust data ecosystem (Arrow, Parquet, DataFusion,
  iceberg-rust) aligns with where the maintenance/executor work is headed.
- **Requiring a broker/cache/search engine alongside Postgres.** Rejected:
  each extra required service raises the floor for every self-hosted
  deployment. Postgres does all of these jobs well enough at the scale where
  correctness matters more than throughput, and the escape hatches remain
  available as *optional* additions later.

## Consequences

- **Positive:** one binary plus one Postgres instance is a complete
  deployment, from laptop to production. Tail latency is bounded by our code
  and Postgres, not a garbage collector. Memory-safety and data-race bug
  classes are excluded from the tier-0 path by construction. The dependency
  audit surface stays small.
- **Negative — hiring and contributors:** the Rust contributor pool is
  smaller than Java's or Go's, and the data-infrastructure community is
  JVM-heavy, so drive-by contributions from engine developers are less
  likely and onboarding is steeper. Mitigations: keep crate boundaries
  small and well-documented, maintain ADRs and design docs so rationale is
  discoverable, and lean on CI (fmt, clippy `-D warnings`, tests) so
  reviewers spend attention on substance.
- **Negative — build times:** full-workspace Rust release builds are slow;
  CI and Docker layer caching need ongoing care.
- **Negative — Postgres as the linchpin:** the catalog's availability is
  Postgres's availability. This concentrates the HA problem in one
  well-understood component, but it makes a documented Postgres HA/backup
  story mandatory rather than optional.
- **Neutral:** queue-in-Postgres and FTS-in-Postgres have real throughput
  ceilings. That is an accepted trade; crossing those ceilings is the
  trigger for an ADR proposing an *optional* dependency, not for making one
  required.
