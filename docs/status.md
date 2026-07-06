# Feature status

The honest, self-critical statement of what Meridian actually does today. It is
maintained alongside the code and cross-checked against tests: a feature is only
**Implemented** here if a test or a reproducible demonstration backs it.
[`api-status.md`](api-status.md) is the endpoint-level companion to this
pillar-level view; where the two disagree, the more conservative wins.

**Status legend** — **Implemented**: works, with test/demo evidence · **Partial**:
works but a stated piece is missing · **Not yet**: not built.

> This is a pre-1.0 project under active development. "Implemented" means
> demonstrated against the engines in the conformance suite, both locally
> (Postgres + MinIO) and on a cloud deployment (managed PostgreSQL + S3) — not
> that it has been hardened or load-tested at scale. Paths that are genuinely
> unbuilt (e.g. GCS/Azure vending) and the not-yet-run conformance kit are
> called out explicitly below.

## Pillars

### A — Core catalog (IRC++)
- **Implemented**: namespace/table/view CRUD; v2 + v3 metadata; the atomic
  commit path (single- and multi-table, property/chaos tested); events
  (CloudEvents feed, durable consumers, HMAC webhooks); OIDC auth; RBAC;
  hash-chained audit log with `/verify`; ETags.
- **Partial**: credential vending (AWS STS + static; automated tests cover
  MinIO, and the AWS S3 path has been run on a real cloud deployment by the
  maintainer but is not yet in the automated suite; GCS/Azure not built);
  remote signing (S3; MinIO in CI, exercised on real AWS by the maintainer);
  scan planning (point-in-time only, no incremental); search (Postgres FTS, no
  semantic/pgvector or usage ranking); tenancy (single-workspace default);
  `register` (rejects `overwrite:true` and UUID-alias adoption); CLI + Terraform
  + catalog-as-code.
- **Not yet**: SCIM; OPA/OpenFGA sync and Ranger import; `unregister`,
  `register-view`, `functions` endpoints; GCS/Azure vending (return an honest
  error).

### B — Federation, sync & migration
- **Implemented**: inbound mirrors (IRC + Glue) as read-only foreign assets;
  the sprawl dashboard (duplicates, staleness, ownership gaps).
- **Partial**: migration toolkit.
- **Not yet**: outbound projection (being mounted into UC/Glue); commit-forwarding
  proxy / dual-registration.

### C — Autonomous table operations + cost intelligence
- **Implemented**: the zero-scan health model; maintenance policy engine; the
  built-in DataFusion compaction executor (row-preservation verified end-to-end
  through PyIceberg and DuckDB, including time travel); snapshot expiry; the
  savings ledger.
- **Partial**: maintenance ops (position-delete / deletion-vector merge-on-read
  compaction is **untested**; encoding normalization); external Spark/Trino
  executor submission; table SLAs.

### D — Cross-engine access governance
- **Implemented**: the policy model (row filters, column masks, Cedar ABAC,
  versioned, dry-run); scan-plan enforcement (masked columns **absent** from the
  plan, row-filter residuals injected — verified with a real PyIceberg client);
  the storage-scope floor (vending); governance analytics (who-can-see-what,
  drift, evidence).
- **Partial**: classification (tag model + coverage; sampling/LLM scanners not
  built); access workflows (request/decide exists; Slack/JIT UI does not).
- **Not yet**: compiled secure views (layer 2); Trino OPA bridge (layer 3).

### E — Observability, quality & contracts
- **Implemented**: zero-scan monitors; data contracts; **the circuit breaker**
  (warn / quarantine / block at commit time — block rejects a violating commit
  atomically, verified without weakening the commit protocol); incidents with
  lineage blast-radius; quality/trust score.
- **Partial**: the `schema_change` monitor misses a metadata-only schema commit
  (documented; prevention path unaffected).
- **Not yet**: pushed-down SQL data-quality execution (only cheap synchronous
  predicates run today).

### F — Lineage & compliance
- **Implemented**: commit-native table lineage (confidence-labeled, no fabricated
  edges); OpenLineage sink + emitter; impact analysis + CI gate.
- **Partial**: column-level lineage (from OpenLineage facets and view parsing;
  **not** from SQL-log parsing yet); usage analytics; compliance packs
  (BCBS-239 / GDPR / AI Act).

### G — Semantics & universal views
- **Implemented**: universal views (SQLGlot sidecar transpilation with a truthful
  `verified`/`best-effort`/`unsupported` status machine; degrades gracefully when
  the sidecar is down); glossary; certified data products. The LLM-assist
  transpilation fallback is off unless a BYO key is configured, is never called
  in tests, and its output is always parse-back-validated and labeled
  best-effort.
- **Partial**: metrics / OSI (measures/dimensions compile to SQL; full OSI
  import/export is not claimed).

### H — Agent gateway (MCP-native)
- **Implemented**: the MCP server; governed context tools (masked columns absent
  so prompts cannot leak restricted schema); agent governance (budgets, kill
  switch, per-call policy, full audit chain); CISO analytics.
- **Partial**: query tools (`run_sql`/`preview` on the built-in small-scan
  executor; routing big queries to external engines is not built;
  `query_metrics` is an honest stub pending semantic-layer wiring).

### I — AI asset governance
- **Implemented**: generic assets (filesets with prefix-scoped vending under
  RBAC, model registry); training-run pinning (structurally append-only, exact
  snapshot ids preserved — reproducible via time travel); provenance and a
  (draft, non-legal) EU AI Act training-content summary from pinned inputs;
  deletion evidence (which model saw a deleted snapshot).
- **Partial**: auto-wiring deletion campaigns to the physical snapshot-expiry job
  (the tie-in is an explicit endpoint today).

### J — Sharing & data products exchange
- **Implemented**: cross-org shares — a recipient-specific read-only IRC endpoint
  that serves **only** granted assets (non-shared assets 404; every write verb
  403), applies column masks, vends read-only credentials, revokes instantly, and
  audits every recipient access; the internal certified-data-product marketplace
  with request-access.
- **Partial (by honest design)**: a share's **row filter** is advisory over pure
  IRC — a vended-credential engine reads Parquet directly, so row filtering is
  surfaced as config, not prevented at that layer. Documented as detect, not
  prevent.
- **Not yet** (explicit non-goals): external/public marketplace; clean rooms.

### K — Branching & data CI/CD
- **Implemented**: catalog-level branches and tags (zero-copy overlays of the
  per-table pointer map); diff, merge with three-way table-level conflict
  detection (fail-closed), and — the headline — **branch-as-catalog**: any branch
  mounts as `warehouse@branch` so any IRC engine reads and writes it without
  knowing branching exists (verified with a real PyIceberg client; a branch
  commit leaves `main` byte-for-byte unchanged, and the branch CAS never touches
  main's pointer); merge gates (a block-mode contract on the branch head refuses
  the merge); ephemeral PR-branch expiry.
- **Partial**: dbt/SQLMesh recipes.
- **Scope**: conflict detection is table-level, not file-level; branch heads are
  not in the search/health index until merged; the circuit breaker gates merges
  rather than running on branch commits.

### L — Workbench
- **Implemented**: notebook handoff (scoped-credential snippets, no embedded
  secrets).
- **Partial**: the SQL workbench (built-in small-scan executor with
  value-preserving masks; routing big scans to an external engine is not built).
- **Not yet** (explicit non-goal): a visualization / BI suite.

## Cross-cutting known gaps

- The **Iceberg REST compatibility kit (RCK)** has not been run against Meridian
  yet; conformance today rests on the engine matrix (PyIceberg, DuckDB, Flink,
  Spark, Trino) and the endpoint checks in `api-status.md`.
- **Credential vending / remote signing** for GCS and Azure is not built (those
  clouds return a clear "unsupported" error). The AWS S3 path is covered by
  automated tests against MinIO and has been run on a real cloud deployment by
  the maintainer; a reproducible real-AWS test is not yet in the suite.
- **Column-level lineage** from SQL-log parsing, **classification scanners**,
  **SCIM**, the **Trino OPA bridge**, and **compiled secure views** are tracked
  follow-ups.
- Benchmarks in [`benchmarks/`](benchmarks/) are local-laptop numbers, not cloud
  or production performance claims.
