# Meridian

**The agentic Iceberg REST Catalog** — an open-source, Apache Iceberg-native catalog with operations built in.

> **Note on the name:** "Meridian" is a working name and may change before the first public release.

Meridian implements the [Apache Iceberg](https://iceberg.apache.org/) REST Catalog (IRC) specification as a drop-in catalog for any IRC-compatible engine, and builds the operational layers that usually live *around* a catalog directly *into* it:

- **Autonomous table maintenance** — compaction, snapshot expiration, orphan-file cleanup, driven by the catalog itself
- **Cross-engine access governance** — one policy layer enforced consistently regardless of which engine reads the table
- **Data observability and contracts** — table health, freshness, and schema/contract checks at the catalog boundary
- **Engine-agnostic semantic layer** — shared metric and model definitions, transpiled to each engine's SQL dialect
- **Governed MCP gateway** — a controlled way for AI agents to discover and query governed data

## Why

Most data catalogs today are bare metadata stores: they track table pointers and schemas, and stop there. Everything a real deployment needs on top — maintenance jobs, access policies, quality monitoring, semantic definitions, agent access — gets bolted on as separate tools, each with its own view of the data and its own engine-specific integration.

The catalog is the one component every engine already talks to. Meridian's premise is that these operational concerns belong in the catalog itself, implemented once, engine-neutrally — so they work the same whether the table is read by Spark, Trino, Flink, DuckDB, or an AI agent.

## Status

> **Pre-alpha / early development.**
>
> Meridian is **not yet usable**. It is under active initial development:
>
> - The core Iceberg REST catalog surface exists: config, namespaces, the table lifecycle (create, load with ETags, list, rename, register, drop) including the transactional commit path (single-table commits and multi-table transactions, with idempotency keys), the view lifecycle, OIDC authentication, and deny-by-default RBAC (auth is **off by default**). Credential vending and background maintenance do not exist yet
> - [docs/api-status.md](docs/api-status.md) is the source of truth for what works: every REST endpoint's status (implemented / partial / not yet) and the documented divergences from the spec
> - Real clients run against it: pyiceberg and DuckDB pass the [e2e suite](conformance/e2e/), and a Flink 1.20 smoke passes except a documented `CREATE TABLE` bug — see the [engine conformance matrix](conformance/engines/README.md) for honest per-engine status (Spark and Trino not yet run)
> - First catalog-plane latency numbers against Apache Polaris and Lakekeeper are in [docs/benchmarks/](docs/benchmarks/) — local development benchmarks on a laptop, not cloud or production performance claims
> - APIs, schemas, and configuration formats are unstable and will change without notice
> - There are no releases and no compatibility guarantees
> - **Do not run this in production** (or anywhere near data you care about)
>
> Watch the repository if you want to follow progress.

## Planned architecture

- **Rust core** — single service built on axum, sqlx, and tokio; serves the IRC API and hosts the maintenance, governance, and observability layers
- **PostgreSQL as the only required dependency** — all catalog state lives in Postgres; no queue, cache, or coordination service required
- **Single-binary deploy target** — one process to run for a working catalog
- **Python transpilation sidecar** — optional SQLGlot-based service for cross-dialect SQL transpilation used by the semantic layer
- **Web console** — Next.js/TypeScript UI for administration, governance, and observability

## Roadmap (high level)

Rough order of development:

1. Core Iceberg REST Catalog protocol (correctness and engine compatibility first)
2. Table maintenance automation
3. Access governance
4. Observability and data contracts
5. Semantic layer
6. Governed MCP gateway for agents

Details will move into GitHub issues and milestones as the project opens up.

## Development

To build and run Meridian locally (Rust toolchain, Dockerized Postgres, tests, lints), see [docs/dev.md](docs/dev.md). Project conventions — commit style, ADRs, code standards — are in [CONTRIBUTING.md](CONTRIBUTING.md). Significant design decisions are recorded in [docs/adr/](docs/adr/).

## Contributing

The project is not yet ready for external contributions — the foundations are still moving too fast for that to be productive. See [CONTRIBUTING.md](CONTRIBUTING.md) for the conventions the project follows and for how this will open up.

Security reports: please use GitHub private vulnerability reporting for this repository. (A dedicated security contact is TBD.)

## License

Licensed under the [Apache License, Version 2.0](LICENSE).
