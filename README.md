# Meridian

**The agentic Iceberg REST Catalog** — an open-source, Apache Iceberg-native catalog with operations, governance, observability, semantics, and agent access built *in*.

> **Note on the name:** "Meridian" is a working name and may change before the first public release.

Meridian implements the [Apache Iceberg](https://iceberg.apache.org/) REST Catalog (IRC) specification as a drop-in catalog for any IRC-compatible engine — Spark, Trino, Flink, DuckDB, PyIceberg — and builds the operational layers that usually live *around* a catalog directly *into* it. It runs today: point a real engine at it and it works.

## Why

Most data catalogs are bare metadata stores: they track table pointers and schemas, and stop there. Everything a real deployment needs on top — maintenance jobs, access policies, quality monitoring, semantic definitions, agent access — gets bolted on as separate tools, each with its own view of the data, its own connectors, and its own engine-specific integration.

The catalog is the one component every engine already talks to. Meridian's premise is that these operational concerns belong in the catalog itself, implemented once, engine-neutrally — so they work the same whether the table is read by Spark, Trino, Flink, DuckDB, or an AI agent.

## The moat

Here's the structural insight the whole thing is built on:

> **The catalog is the only component that sits in *both* the write path and the read path of every engine.** Every Iceberg commit flows through it atomically. Every table load, credential vend, and scan plan flows through it too. Nothing else in the stack touches every change *and* every access.

Every tool that bolts governance, quality, or lineage on *from the outside* is approximating that position with connectors, periodic crawls, and query-log scraping — and paying for it with staleness, blind spots, and per-engine integration tax. A catalog does these things **natively, transactionally, and with zero instrumentation.** That's not a marginal edge; it's a different physics. Because Meridian sees the transaction, it can do things an outside-in tool structurally cannot:

- **Stop bad data before it lands, not alarm after.** A data contract is enforced *at commit time* — a schema-violating write is rejected atomically, or quarantined to an audit branch, or waved through with an incident. Observability vendors sell faster alarms; Meridian sells the fire not starting. *([circuit breaker](docs/design/contracts-circuit-breaker.md))*
- **Enforce row/column policy inside the scan plan.** Masked columns are *absent* from the plan the engine executes and row filters are injected as residuals — the same mechanism the walled gardens reserve for themselves, from a neutral catalog that any engine can point at. A thin client cannot read what it may not.
- **Repair what engines emit.** Flink sprays small files; some engines write pathological layouts. Meridian's compaction rewrites them into house style through a normal, audited, revertible commit — every row preserved (verified through PyIceberg *and* DuckDB). Engines' weaknesses become the catalog's feature.
- **One view, every dialect.** Author a view once; every engine reads it in its own SQL dialect, transpiled and labeled `verified` / `best-effort` / `unsupported` — the five-year-old cross-engine view bug class, closed.
- **Branches any engine can use.** A branch mounts as `warehouse@branch`, so *every* IRC engine reads and writes zero-copy dev environments on prod data without knowing branching exists.
- **A real firewall for AI agents.** Every agent gets an identity, a purpose, a budget, a kill switch, and a court-grade audit trail — on columns and rows, not just tool calls — and governed context returns restricted columns *absent* so a prompt can't even learn their names.

And because it's all one system on one substrate, the parts compound: lineage powers contract impact-analysis powers agent trust; maintenance telemetry powers the savings ledger powers the renewal conversation. Point solutions can't compound — they don't share a substrate.

Meridian is engine-neutral by design: an Apache-2.0 catalog you own and run, that any IRC-compatible engine can point at, with PostgreSQL as its only required dependency. Neutrality isn't a marketing line here — it's the product.

## What it does

Every capability below is implemented and covered by tests. See [`docs/status.md`](docs/status.md) for the honest, per-feature status (Implemented / Partial / Not yet) and [`docs/api-status.md`](docs/api-status.md) for the endpoint-level IRC surface.

- **Core catalog (IRC++)** — the full REST catalog surface (namespaces, tables, views, multi-table transactions) on an atomic, crash-safe commit path with optimistic concurrency and idempotency keys; OIDC authentication; deny-by-default RBAC; a hash-chained, tamper-evident audit log. Exercised by PyIceberg, DuckDB, Flink, Spark, and Trino.
- **Federation** — mirror external Iceberg REST catalogs (and AWS Glue) as read-only foreign assets, with a sprawl dashboard across every catalog you own.
- **Autonomous table maintenance** — a health model and a built-in compaction engine that rewrites small files into large ones through the normal commit path (audited and revertible), snapshot expiry, and a savings ledger. Compaction preserves every row — verified end-to-end through both PyIceberg and DuckDB, including time travel.
- **Cross-engine access governance** — row filters, column masks, and Cedar ABAC defined once and enforced inside server-side scan planning: restricted columns are *absent* from the plan and row filters are injected as residuals, so a thin client cannot see what it may not.
- **Observability & data contracts** — zero-scan monitors (freshness, volume, schema drift) computed from the commit stream, plus **the circuit breaker**: a data contract can *reject a bad commit atomically at write time*, quarantine it to an audit branch, or warn — the fire doesn't start, rather than an alarm after it lands.
- **Lineage & impact** — commit-native table lineage, an OpenLineage sink and emitter, and impact analysis with a CI gate (unknown lineage stays unknown — no fabricated edges).
- **Semantics & universal views** — author a view once and every engine reads it in its own SQL dialect (deterministic SQLGlot transpilation with truthful `verified` / `best-effort` / `unsupported` labels), plus metrics, a business glossary, and certified data products.
- **Governed agent gateway (MCP)** — AI agents are first-class principals with a purpose, budget, kill switch, and a full audit chain; governed context tools return masked columns *absent* so prompts can't leak restricted schema, and query tools run through the same policy machinery.
- **AI asset governance** — filesets and a model registry, plus training-run pinning that binds a model version to exact table snapshots (reproducible via time travel).
- **Sharing** — read-only cross-org shares served at a per-share IRC endpoint that exposes only granted assets, with instant revocation — a neutral alternative to Delta Sharing, built from vending + policy + audit.
- **Branching & data CI/CD** — catalog-level branches and tags with **branch-as-catalog**: any branch mounts as `warehouse@branch`, so *any* IRC engine reads and writes it without knowing branching exists; merges are gated by contracts.
- **SQL workbench** — a governed SQL editor over a built-in small-scan executor, plus a **web console** (Next.js) for the catalog, governance, quality, lineage, and agents, and a **Terraform provider** and `meridian apply` catalog-as-code.

## Status

> **Alpha — running on the cloud, still pre-1.0.**
>
> Meridian works today. It has been run on a cloud deployment (managed
> PostgreSQL and S3 object storage) as well as locally against the engines in
> the [conformance matrix](conformance/engines/README.md). It is still young:
>
> - **Authentication is off by default** (`auth.mode = "disabled"`) for the dev loop — with it off, anyone who can reach the port owns the catalog. Turn on OIDC (and with it deny-by-default RBAC) before exposing it to anyone.
> - A few things are genuinely not built yet — listed honestly in [`docs/status.md`](docs/status.md): GCS and Azure credential vending (those clouds return a clear "unsupported" error today; AWS S3 vending and remote signing work); the Iceberg REST compatibility kit (RCK) has not been run yet; and column-level lineage from SQL parsing, classification scanners, and SCIM are follow-ups.
> - APIs, schemas, and configuration formats are unstable and will change without notice. There are no tagged releases and no compatibility guarantees yet.
> - It has not been load-tested at scale. Pin a commit and keep backups if you point it at data you care about.

## Quick start

Everything in Docker (Postgres + the Meridian server; migrations run on startup):

```sh
docker compose -f docker-compose.dev.yml up --build

curl -s localhost:8181/healthz
# {"status":"ok","checks":{"database":"ok"}}
```

Create a warehouse (the IRC `{prefix}`) and point any Iceberg engine at it:

```sh
curl -s -X POST localhost:8181/api/v2/warehouses \
  -H 'content-type: application/json' \
  -d '{"name":"demo","storage_root":"file:///tmp/meridian-demo"}'
```

Then, from PyIceberg:

```python
from pyiceberg.catalog.rest import RestCatalog

cat = RestCatalog("demo", uri="http://localhost:8181/iceberg", warehouse="demo")
cat.create_namespace("analytics")
# create tables, append data, time-travel, evolve schema — it's a normal IRC catalog
```

Running from source, the web console, engine examples (Spark/Trino/Flink/DuckDB), and the S3/MinIO setup are all in [docs/dev.md](docs/dev.md) and [conformance/](conformance/). First catalog-plane latency numbers against Apache Polaris and Lakekeeper are in [docs/benchmarks/](docs/benchmarks/) — local-laptop numbers, not cloud or production claims.

## Architecture

- **Rust core** — a single service (axum, sqlx, tokio) serving the IRC and management APIs and hosting the maintenance, governance, observability, semantics, and agent layers.
- **PostgreSQL is the only required dependency** — all catalog state lives in Postgres; no queue, cache, or coordination service required. Object storage is the customer's (S3/GCS/Azure/MinIO/local).
- **Single-binary deploy target** — one process for a working catalog.
- **Python transpilation sidecar** — an optional SQLGlot service for cross-dialect SQL (universal views, metric compilation), with an off-by-default, BYO-key LLM-assist fallback whose output is always validated and labeled best-effort.
- **Web console** — a Next.js/TypeScript UI for administration, governance, quality, lineage, and agents.

The codebase is a Cargo workspace (catalog core, storage IO, the Iceberg metadata/manifest engine, vending, authz, federation, lineage, agents, and a small-scan query executor), a Python sidecar, the console, a Terraform provider, and an engine conformance harness. Significant decisions are recorded in [docs/adr/](docs/adr/).

## Development

To build and run locally (Rust toolchain, Dockerized Postgres, tests, lints), see [docs/dev.md](docs/dev.md). Conventions — commit style, ADRs, code standards — are in [CONTRIBUTING.md](CONTRIBUTING.md).

## Contributing

The project is still moving fast and pre-1.0. See [CONTRIBUTING.md](CONTRIBUTING.md) for the conventions it follows and for how contribution will open up.

Security reports: please use GitHub private vulnerability reporting for this repository. (A dedicated security contact is TBD.)

## License

Licensed under the [Apache License, Version 2.0](LICENSE).
