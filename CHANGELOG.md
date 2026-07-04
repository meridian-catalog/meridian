# Changelog

All notable changes to Meridian will be documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.1.0/).
There are no releases and no version tags yet; until the first release,
everything lives under Unreleased and may change without notice. Once
releases begin, the project will adhere to
[Semantic Versioning](https://semver.org/spec/v2.0.0.html).

## [Unreleased]

### Added

- **Branching & Data CI/CD — catalog branching every engine can consume**
  (Pillar K, K-F1/K-F2/K-F3). A **catalog branch** is a named, zero-copy overlay
  of the per-table pointer map: it shares main's metadata until a table diverges,
  at which point a `branch_table_pointers` row carries the branch's own pointer,
  advanced by the **same CAS/outbox/audit discipline main uses** — the commit
  invariants (no lost updates, crash-safe, idempotent, audit-durable) are
  preserved, not weakened (there is exactly one function per pointer kind that
  moves it). The headline is **branch-as-catalog** (K-F2): any branch is
  mountable as its own IRC catalog under the prefix `warehouse@branch` — the
  prefix resolver splits on the last `@`, so `loadTable` on `warehouse@dev`
  returns the branch pointer (falling through to main for undiverged tables) and
  a commit on `warehouse@dev` writes the branch pointer and **never moves main**.
  Every IRC engine (Spark, Trino, PyIceberg, Snowflake-CLD, DuckDB) reads and
  writes a branch with **no branching API** — it just points at a different
  catalog name. **Diff** reports schema + snapshot + row-count deltas vs main
  (unknown row counts reported as `unknown`, never fabricated). **Merge** (K-F1)
  fast-forwards main table-by-table **through the commit path**, with
  table-level **conflict detection** (both-sides changes refuse the whole merge,
  fail-closed) and a **merge gate** (K-F3) that evaluates Pillar-E contracts on
  the branch head and refuses a merge that would violate a block-mode contract
  (reusing the circuit breaker's contract engine). **Tags** are immutable release
  points; a commit against a `warehouse@tag` prefix is refused. **Ephemeral PR
  branches** carry an expiry and are swept. Management API `/api/v2/branches` +
  `/api/v2/tags`; CLI `meridian branch create|list|diff|gate|merge|delete|sweep`
  and `meridian branch tag` (`meridian branch gate` exits non-zero on failure,
  for dbt/SQLMesh CI). Migration `0025_branching` (`catalog_branches`,
  `branch_namespaces`, `branch_table_pointers`, `catalog_tags`). Honest scope:
  conflict detection is table-level (file-level overlap is future work); branch
  snapshots are not in the search/health index until merged; the circuit breaker
  gates the merge, not branch commits; creating a table only-on-a-branch is out
  of scope this milestone. Full design: `docs/design/branching.md`.

- **Sharing & the data-products exchange — the neutral Delta-Sharing
  alternative** (Pillar J, J-F1/J-F2). A **share** is a scoped, read-only
  projection of catalog assets to an external recipient org, served over a
  **per-share Iceberg REST endpoint** (`/share/{token}/v1/...`) that works with
  *any* IRC-capable engine on the recipient side. It is built from primitives the
  catalog already has: **vended read-only credentials** (`meridian-vending`), an
  optional **row filter / column mask** per grant, and the **audit log**.
  `shares` + `share_grants` (migration `0024_sharing`): a share carries a
  recipient identifier, an opaque 256-bit `token` (the recipient's bearer secret
  and catalog prefix, returned exactly once, never in reads or audit), optional
  terms-of-use with an acceptance gate, and a revocation flag. The recipient
  endpoint is **token-authenticated** (exempt from the OIDC middleware), serves
  **only** the granted assets, drops **masked columns** from the served schema,
  answers every write verb with **403**, and **audits every recipient access**.
  **Revocation is instant in effect** — the recipient only ever holds
  short-lived vended credentials. Honest scope: **column masking is prevented**
  at the catalog layer, but **row filtering over the neutral IRC endpoint is
  surfaced (advisory), not prevented** — a vended-credential engine reads Parquet
  directly, so full row-level prevention needs the query-mediated path. The
  **internal marketplace** (J-F2): `GET /api/v2/marketplace/products` is the
  certified-data-product gallery (Pillar G, certified-first) with a
  request-access flow that creates and decides Pillar-D `access_requests`.
  Management API `/api/v2/shares` + `/api/v2/marketplace`; CLI `meridian share`
  and `meridian marketplace`; console **Sharing** page. **Out of scope
  (explicit):** external/public marketplace and clean-room compute. Full design:
  `docs/design/sharing.md`.

- **AI Asset Governance — the catalog governs the AI supply chain** (Pillar I,
  I-F1..I-F4). One extensible **generic-asset** model (`/api/v2/assets`): a
  fileset, model, or vector dataset is one row with a `kind` and kind-specific
  `metadata`, a first-class RBAC **grant securable** (`asset`, addressed by id —
  admitted into the grants CHECK exactly as views were), and full-text
  searchable. A **fileset** vends scoped, short-lived storage credentials
  **bound to its prefix** (`POST /api/v2/assets/{id}/credentials`), reusing the
  identical table-vend mechanics and RBAC-gated on the asset — every vend
  audited. **Training-run pinning** (`POST /api/v2/training-runs`, I-F2) binds a
  `(model, version)` to the **exact table snapshots** that trained it as an
  **immutable, append-only** provenance record — the pinned snapshot ids are
  stored exactly, so Iceberg time-travel reproduces the inputs. **Provenance
  reporting** (I-F3) assembles the per-model **data → run → model** lineage, the
  **license/consent tags that propagated** from the input sources, auto-drafted
  **dataset cards**, and an **EU AI Act GPAI training-content summary** generated
  from the pinned inputs. **Deletion campaigns** (I-F4) produce GDPR "right to be
  forgotten" evidence: a campaign records the affected snapshots and **freezes
  which model versions saw the deleted data**, tracks per-snapshot expiry status,
  and closes when every affected snapshot is physically expired (the maintenance
  `ExpireSnapshots` job is the tie-in). CLI: `meridian
  asset`/`training-run`/`provenance`/`deletion`. Persistence: migration
  `0023_ai_assets`. Honest scope: the "model → agents using it" lineage leg is
  not modelled yet (returned empty, never fabricated); the AI Act summary is a
  catalog-derived draft for a compliance officer, not a legal opinion.

- **Governed agent queries + the SQL workbench — the built-in executor, wired**
  (Pillar H `run_sql` H-F3, Pillar L workbench L-F1/L-F3). The MCP agent
  gateway's query tools now **run** on the built-in `DataFusion` small-scan
  executor (ADR 010), not a stub. `run_sql` and `preview_table` plan a query
  through one governed path (`crate::mcp::engine`): the SQL's referenced tables
  are enumerated with the executor's *own* parser (so the resolved set is exactly
  what it binds — no drift), each is RBAC-READ-checked and ABAC-resolved for the
  calling agent, masked columns are **dropped** (H-F2: a restricted column is
  absent from results, never nulled), the scan is **priced from manifest stats
  and the agent's budget checked *before* any I/O** (an oversized scan or an
  over-budget query is a graceful, relayable refusal that names the escape
  hatch), and every result carries **provenance** — the tables and snapshot ids
  read and the policies applied — so an agent can cite and the audit chain
  answers "which agent read which columns under which policy". `query_metrics`
  returns an honest "semantic layer not populated" answer until metric
  definitions land (Pillar G). Every call remains on the tamper-evident
  agent-activity chain.

  The **SQL workbench** (`POST /api/v2/workbench/query` + saved queries +
  per-principal history) is a second caller of the same governed path — so a
  human at the workbench and an agent can never be governed differently — with
  one deliberate difference: the workbench keeps **value-preserving** masks
  (`hash`, partial, NULL — the column stays, the value is hidden), where the
  agent path drops. Big scans route to a registered engine or are refused with
  guidance (a small-scan wedge, not a BI suite). The **notebook handoff** (L-F3,
  `POST /api/v2/workbench/snippet`) generates one-click "open in
  PyIceberg/Daft/Pandas" connection snippets that obtain scoped, vended
  credentials at connect time via the standard IRC flow — no secret is ever
  embedded. A console **Workbench** page ties it together: a SQL editor over
  governed assets, a results table with the provenance line, saved queries, and
  history — all real `/api/v2` data. New migration `0022_workbench`; design in
  `docs/design/workbench.md` and the updated `docs/design/agent-gateway.md`.

- **Semantics & universal views — meaning next to the data** (Pillar G,
  G-F1 / G-F2 / G-F3 / G-F4). **Universal views (G-F1)** close the five-year-old
  cross-engine view bug: on `loadView`, Meridian serves the SQL representation
  matching the *requesting engine's* dialect — identified from an explicit
  `?engine=` override or the `User-Agent` (a `Trino JDBC Driver` request gets
  Trino; an unrecognized client gets the view as authored, never a guessed
  dialect). When the view does not already carry that dialect, Meridian
  transpiles its canonical representation via the SQLGlot sidecar, folds the
  result into the served metadata (dialect-tagged, carrying a
  `meridian.transpile-status`), reports the honest status
  (`verified` / `best_effort` / `unsupported`, validated by parse-back), and
  **caches** it in a side table keyed by the view + a hash of the source SQL — so
  the next load is instant, without churning the Iceberg pointer version (which
  would break optimistic view committers). A sidecar outage degrades gracefully
  to the canonical representation with a status note — never a 500. Also exposed
  standalone at `POST /api/v2/sql/transpile` (a migration tool and demo magnet).
  **Metrics & semantic models (G-F2)** are first-class objects — measure,
  dimensions, filters, grain, owner, certification — that compile
  **deterministically** to any engine's SQL via the sidecar (`SELECT … GROUP BY …`
  built with SQLGlot's expression builder, never string-concatenated; no LLM ever
  consulted), served to BI over `/api/v2/metrics/{id}/compile` and to agents over
  MCP (`list_metrics` / `get_metric_definition`). **The business glossary (G-F3)**
  adds stewarded terms with definitions and certification, linked many-to-many to
  tables/views/metrics by stable reference (rename-surviving), served to agents as
  `get_glossary_term`. **Certified data products (G-F4)** are named bundles
  (tables + views + metrics + glossary terms + contracts) with an owner, a
  free-text SLA, a certification status, and a **product-level status page** that
  rolls up member-table quality/trust (reusing the E-F5 quality surface) into a
  health verdict. All semantics mutations are management-gated and write their
  audit + outbox row on the same transaction. The SQLGlot transpilation sidecar's
  `compile_metric` endpoint is now fully implemented (was a wave-1 stub). New
  migration `0021_semantics.sql`; store module `meridian-store/src/semantics.rs`;
  server `routes/semantics.rs` + universal-view path in `routes/views.rs` + the
  Rust sidecar client `src/sidecar.rs`; CLI `meridian metric` / `glossary` /
  `product` / `transpile`; a console **Semantics** page (metrics, glossary,
  products, and a live universal-view transpiler). Design docs:
  `docs/design/semantics.md` and `docs/design/transpilation.md`.

- **The MCP agent gateway — the governed front door for AI agents** (Pillar H,
  H-F1 / H-F2 / H-F4; the *agent firewall*). Meridian now speaks the **Model
  Context Protocol** (spec `2025-06-18`) at a Streamable-HTTP endpoint `/mcp`
  (JSON-RPC 2.0: `initialize`, `tools/list`, `tools/call`), behind the same OIDC
  layer as everything else. **Agents are first-class principals** (`kind =
  agent`, distinct from users/services) with a governance envelope — an
  accountable **owner**, a **purpose** statement, an **environment** (dev/prod),
  a **lifecycle** (expiry + review date), and a **kill switch**. A new
  `meridian-agents` crate holds the pure protocol/catalog/decision vocabulary
  (no database), migration 0020 adds `agent_principals` + `agent_budgets` +
  `agent_activity`, and the server hosts the endpoint, the governance wrapper,
  and an `/api/v2/agents` control plane. **Context tools (H-F2)** —
  `search_assets`, `get_table_context`, `get_lineage`, and the
  metrics/products/glossary reads — are *governed*: an agent sees only what its
  grants allow, and a policy-masked or denied column is **ABSENT from the
  returned schema (not nulled)**, so a prompt can never leak the structure of
  restricted data — enforced by the *same* Pillar-D decision
  (`governance::resolve_scan_policy`) the scan planner uses, so context and query
  can never disagree. **Query tools (H-F3)** — `run_sql`, `query_metrics`,
  `preview_table` — run the full governance chain and are charged against
  per-agent **budgets** (queries/hour, scanned-bytes/day, dollar-estimate/day,
  rolling windows, `FOR UPDATE`-serialized) with a **graceful, agent-relayable
  refusal** when a cap is hit; execution itself is handed to a `QueryExecutor`
  trait (the seam the `meridian-query` executor implements — stubbed to an honest
  "not wired" tool-error until wired, with all governance around it live now).
  **The kill switch** (`POST /api/v2/agents/{id}/suspend`) refuses every tool
  for a suspended or expired agent. And **the full audit chain is the product**
  (H-F4): every tool call — allowed, denied, refused, or errored — writes an
  `agent_activity` ledger row **and** a tamper-evident hash-chained `audit_log`
  entry on the *same transaction*, cross-referenced, recording *which agent
  called which tool, the policy decision, the resolved purpose, and what it
  touched* — the "which agent read which columns for which purpose" answer with
  a court-grade trail. The MCP protocol/tool distinction is honored throughout
  (a non-agent caller or unknown tool is a JSON-RPC protocol error; a policy or
  budget refusal is a `isError: true` tool result the agent relays), and the
  transport's DNS-rebinding `Origin` defense is enforced. Design doc:
  `docs/design/agent-gateway.md`.

- **Small-scan query executor — governed `run_sql` / workbench execution**
  (Pillar H, H-F3; Pillar L, L-F1). A new `meridian-query` crate runs governed
  `SELECT`s over the Iceberg tables the catalog owns, in-process, for small
  scans only (spec §8.1) — the shared engine behind the agent gateway's
  `run_sql` tool and the console workbench; big scans route to registered
  customer engines. Built on **DataFusion** (the spec's named built-in engine;
  see ADR 010). The pipeline is the `run_sql` contract, **validate → estimate →
  refuse-or-execute**: the SQL is parsed and must be a single read-only
  statement (DML/DDL/`COPY`/multi-statement input refused via the parser, so a
  governed query can never mutate); the scan's on-disk **bytes and row count are
  estimated from manifest stats and capped *before any data file is read***, so
  an oversized query is a cheap, polite refusal that names the escape hatch (a
  registered engine) for an agent to relay. Tables are read via Meridian's own
  manifest engine + Parquet reader — mapping columns by Iceberg **field id**
  (not name or position), synthesizing nulls for schema-evolved files, and
  materializing v2 position/equality **merge-on-read deletes** (v3 deletion
  vectors are refused, not silently dropped) — then handed to DataFusion as
  in-memory Arrow batches, so the manifest layer stays the single source of
  truth. **Policy is enforced by the same Pillar-D machinery as scan planning:**
  the caller passes the row filters + column masks resolved by `meridian-authz`,
  and the executor compiles them into a governed SQL **view** per table — the
  row filter folded into a `WHERE`, masked columns transformed, dropped columns
  **absent (not nulled)** so the schema of restricted data cannot leak (H-F2),
  and any custom mask it cannot verify **failing closed** to a drop.
  SQL-injection-safe literal/identifier rendering throughout. Every result
  carries **provenance** (tables + snapshot ids read, policies applied, columns
  masked) so agents can cite (H-F3) and a CISO audit can answer "which agent
  read which columns under which policy", plus the scanned-bytes estimate for
  budget accounting (H-F4). The crate is a pure function of *(metadata + bytes +
  policy + SQL + caps)* — it resolves no names, loads no metadata, and reads no
  policy — so it is fully covered by unit + integration tests against real
  Iceberg fixtures and takes no dependency on the server or the agent gateway;
  the gateway's `QueryExecutor` trait maps onto it through a thin server-side
  adapter (documented in the crate root and ADR 010).

- **Zero-scan data-quality monitors, incidents & the trust score — detection
  and operability** (Pillar E, E-F1 / E-F5 / E-F6). The catalog now *watches*
  every table from the commit stream and opens incidents on anomalies —
  **without ever scanning data**. **E-F1 (zero-scan monitors):** opt-in monitors
  per table or namespace compute freshness (commit recency vs. a learned cadence
  or declared SLA), volume (rows/files/bytes anomaly-scored against the recent
  median), schema-change (any evolution, breaking-change classified via the
  contract schema-diff), file-size regression, snapshot/delete-file debt, and
  commit-failure storms — all from the `table_snapshots` write-through index and
  `metrics_reports`, never a data file. A post-commit worker consumes the durable
  `table.committed` stream **off the sacred commit path** (like the lineage
  worker), builds a bounded baseline history, evaluates every bound monitor,
  records a result, and opens incidents on breaches; the anomaly scorers are pure
  and unit-tested and stay quiet until the baseline is trustworthy (no
  false-positives on a fresh table). **E-F5 (incidents):** an incident carries a
  lifecycle (open → acknowledged → resolved), the owner captured from the table's
  `owner` property (never fabricated), and the **downstream blast radius** from
  the lineage impact function; a live incident for the same condition is
  re-touched (a partial-unique index enforces "one live incident per
  (table, source, kind)"), not duplicated. Contract violations from the circuit
  breaker open incidents through the same ledger, so producers and operators see
  one status per table. Every open/ack/resolve emits a `quality.incident.*`
  CloudEvent, so Slack/pager routing runs off the same durable outbox. Per-table
  status (`green`/`yellow`/`red` + history) rolls the live incidents up.
  **E-F6 (trust score):** a composite `0..=100` score (+ letter grade) from five
  explainable, documented-weight components — monitors passing + coverage,
  contract present + mode strength, ownership, docs coverage, freshness — cheap
  enough to fold onto search results (each table hit now carries a
  `quality_score`). New store modules `monitors`, `incidents`, `quality_score`;
  migration `0019_monitors_incidents`; a `quality_monitor` evaluation worker;
  API under `/api/v2/quality/monitors|incidents|tables/.../status|score`; CLI
  `meridian monitor|incident|quality`; and a console **Quality** page. **Impact
  CI gate (F-F5):** `meridian impact --change drop_column:foo --asset ns.table
  --fail-on-downstream` exits non-zero when a change breaks a downstream asset,
  for dbt/SQL CI. Isolated DB-backed tests cover volume-spike and
  breaking-schema detection through real commits, the incident lifecycle +
  de-duplication, blast radius via lineage, the quality score, and the impact
  gate; the sacred commit property suite passes **unchanged** (the worker is a
  read-side consumer — it adds no commit-path work).
- **Data contracts + THE CIRCUIT BREAKER — prevention at commit time**
  (Pillar E, E-F3 / E-F4;
  [contracts design](docs/design/contracts-circuit-breaker.md)). The catalog
  stops a bad commit from landing, not just alarms about it afterwards. **E-F3
  (contracts as versioned catalog objects):** a `contracts` object binds to a
  table or a namespace, carries a versioned typed spec — schema-evolution rules
  (`additive_only` / `no_narrowing` / `none`, protected columns, required
  columns) plus cheap **synchronous** predicates (schema-level non-null, a
  row-count sanity bound from the snapshot summary) — and an enforcement mode.
  Full CRUD + append-only version history + a per-table status endpoint under
  `/api/v2/quality`. **E-F4 (the circuit breaker):** a synchronous pre-commit
  hook in the commit driver (the documented §3-step-6 seam of
  `commit-protocol.md`) evaluates enabled contracts against the **staged**
  metadata *before* the pointer CAS. Schema evolution is classified by stable
  field id (drop / narrow / nullability-tighten rejected; additive and
  spec-legal widenings allowed). Three modes: **block** rejects the commit
  atomically with a machine-readable `contract-violation` error (nothing
  durable, the pointer unchanged); **warn** lets it land and records + events the
  violation atomically with the swap; **quarantine** (managed WAP) retargets the
  violating snapshot onto an Iceberg audit branch so `main` is **not** advanced,
  with `publish` (fast-forward `main`) / `discard` endpoints — both ordinary,
  fully-audited commits. Every mode writes a `contract_violations` record + a
  CloudEvent. The hook preserves **every** commit invariant (I1–I6): the CAS,
  lock order, idempotency recall, and multi-table atomicity are untouched (the
  existing commit property/chaos suite passes unchanged); an eval error
  fails-closed for block/quarantine and fails-open for warn (documented). New
  store module `meridian_store::contracts`, a pure
  `TableMetadata::quarantine_retarget` transform, migration `0018_contracts`,
  and the management-gated `/api/v2/quality` surface. The honest depth of
  quarantine (single-branch, explicit publish, no re-validation on publish) is
  documented in the design doc. Tested against a local Postgres + MinIO
  (`contracts` unit tests, `quarantine_retarget` tests, and
  `crates/meridian-server/tests/contracts_api.rs` covering block/warn/quarantine
  end-to-end through the HTTP commit endpoint, a concurrent-writer-under-contract
  no-lost-updates test, and audit-chain verification).

- **Lineage core — commit-native edges, OpenLineage both directions, and
  impact** (Pillar F, F-F1 / F-F2 / F-F5;
  [lineage design](docs/design/lineage.md)). Table-level lineage with a hard
  **no-fabrication** guarantee: every edge traces to a concrete declaration — a
  commit that listed its inputs, or an engine that declared an (input, output)
  pair — and an unresolvable identifier becomes a labeled *external* node,
  never an invented table. Meridian does not emit the
  everything-relates-to-everything cartesian edges that are OpenLineage's
  documented failure mode; a table with no evidence has an empty graph.
  **F-F1 (commit-native):** a post-commit worker (spawned by `meridian serve`,
  **off** the sacred commit path — it consumes the durable `table.committed`
  event stream) records edges whenever a new snapshot's summary *declares* its
  inputs, confidence-labeled and stamped with the engine fingerprint
  (`spark.app.id`, Flink job, Trino query id, dbt invocation id). No declared
  inputs → no edges. **F-F2 (OpenLineage):** a first-class sink,
  `POST /api/v2/lineage/openlineage`, parses an OpenLineage 1.x `RunEvent`
  (Spark/Airflow/dbt/Flink) into edges — with `columnLineage` facet columns when
  present, table-level otherwise — plus an emitter that renders
  Meridian-initiated maintenance jobs as spec-valid, Marquez-compatible
  RunEvents (POSTed to `[lineage].openlineage_url` when set). **F-F5 (graph +
  impact):** `GET /api/v2/lineage?asset=&depth=&direction=` returns the
  up/downstream graph, and `GET /api/v2/lineage/impact?asset=&change=drop_column:foo`
  returns the affected downstream assets and their owners for notification; the
  `impact_of` function is exposed for the incidents wave's blast-radius calls.
  New crate `meridian-lineage`, migration `0017_lineage_edges`, and the
  management-gated `/api/v2/lineage` surface. Tested against a local Postgres
  (`meridian-lineage` unit tests + `crates/meridian-lineage/tests/lineage_db.rs`).

- **Cross-engine access governance — scan-plan enforcement + the governance
  API** (Pillar D, D-F2.1 / D-F1 / D-F3 / D-F5;
  [enforcement matrix](docs/design/enforcement-matrix.md)). The enforcement
  layer that turns the `meridian-authz` decision library into what an engine
  actually sees. **Layer 1 (scan-plan enforcement), the headline:** in the
  server-side scan-planning path, after RBAC `READ` passes, Meridian resolves
  the `(principal, table, purpose)` ABAC decision and applies it *inside the
  plan the engine executes* — a full deny returns 403 before any file is
  offered, a row-filter policy's predicate is AND-ed into every returned scan
  task's `residual-filter` (after partition folding, so pruning cannot drop
  it), and a masked column's statistics are stripped from every returned data
  file (the column is **absent** from the plan, not nulled — what the agent
  gateway needs). Purpose is declared with the `X-Meridian-Purpose` header.
  Every enforced plan writes a `governance.scan.enforced` audit row (principal,
  table, applied policies, removed columns, reason) — the decision is part of
  the hash-chained audit trail. Proven end-to-end with a **PyIceberg + MinIO**
  client: a server-planning scan of a real table returns the masked column
  absent and the rows filtered. New store modules `meridian_store::{policy,
  tags}` (versioned policies with append-only history + rollback, tag CRUD +
  column-level assignments, and the resolvers that map rows onto the authz
  inputs), a `meridian_server::governance` decision bridge, and the
  **`/api/v2/governance/...` API**: tags CRUD + assignment + coverage,
  versioned policies + bindings + **dry-run** ("who would lose access"),
  **effective-policy** for a `(principal, table)`, who-can-see-what,
  **policy-drift** alerts, and an **audit-ready evidence** export. CLI
  (`meridian tag` / `policy` / `govern`) and a console **Policies** page
  (tags, kind-aware policy forms, effective-policy lookup, drift). The
  enforcement matrix documents each engine/path's prevent-vs-detect guarantee
  **honestly**: Layer 1 prevents for planning clients today; compiled secure
  views (Layer 2) and native bridges (Layer 3) are designed, not yet
  implemented; the vended-credential storage floor (Layer 4) is the universal
  coarse bound. Management-gated (admin or `MANAGE_WAREHOUSE`); migration 0016.
- **ABAC policy engine — Cedar decisions + row-filter/column-mask resolution**
  (Pillar D, D-F1; new crate `meridian-authz`;
  [ADR 009](docs/adr/009-cedar-abac.md)). A pure, database-free decision
  library wrapping AWS's [Cedar](https://crates.io/crates/cedar-policy) engine.
  It fixes a catalog policy model — principals (human/service/agent, with
  groups, roles, purpose, environment), resources (namespace/table/view/column,
  with tags, owner, classification), actions (read/write/commit/…), and a
  request context (time, purpose, session) — and evaluates
  `authorize(principal, action, resource, context)` to a decision that carries
  its **determining policies and a human-readable reason for the audit trail**.
  Deny overrides allow. A **tag → policy** convenience layer compiles the common
  rule shapes ("`pii:high` denies read unless a purpose is granted", owner-allow,
  group-based, time-bound, tag→row-filter, tag→column-mask) to Cedar, and
  `resolve_filters_and_masks` returns the row filters and column masks that apply
  to a `(principal, table)` — a **`RowFilter` compiles to the exact IRC
  `Expression`** the server-side scan planner folds into each scan task's
  residual, so policy and enforcement cannot drift. Policies are validated
  against a Cedar schema (errors caught before save) and support dry-run. This
  is the decision/resolution layer only; cross-engine *enforcement* (the D-F2
  matrix) is a later wave, and the ADR documents each path's prevent-vs-detect
  guarantee honestly.
- **Inbound catalog mirrors — the sync engine** (Pillar B, B-F1; new crate
  `meridian-federation`; [ADR 008](docs/adr/008-federation-inbound-mirrors.md)).
  A *mirror* is an external Iceberg REST Catalog (Polaris, Lakekeeper, Unity's
  IRC endpoint, Snowflake Horizon, BigLake, Nessie — or another Meridian, since
  Meridian is itself an IRC) that Meridian continuously syncs **from** as
  read-only foreign assets. A sync run connects to the source with a minimal
  read-only IRC client (`GET /v1/config`, list namespaces, list tables,
  `loadTable`; `none` / static-bearer / OAuth2-client-credentials auth), walks
  its namespaces and tables, and **materializes each table as an ordinary row
  in the native `tables`/`namespaces` tables** tagged with a `mirror_id`, so
  every read-side feature works on foreign assets immediately: they are
  full-text **searchable**, health-scorable, and carry their real schema,
  snapshots, current pointer, and source `metadata_location`. Sync is
  **incremental** (a table whose `metadata_location` is unchanged is not
  re-indexed) and reflects source deletions (a table that vanished is removed
  through the audited path). A background worker (spawned by `meridian serve`
  alongside the maintenance/events workers) syncs mirrors whose interval has
  elapsed, and `POST /api/v2/mirrors/{name}/sync` runs one immediately. Foreign
  assets are **conflict-free / read-only**: a commit, create, register, drop, or
  rename targeting a foreign table (or its mirror-private warehouse) is rejected
  with a `409 CommitFailedException` naming the source as the write authority.
  Verified end to end by a real IRC-to-IRC mirror test (one Meridian catalog
  mirrored into another over HTTP). Migration `0015_federation_foreign_assets`
  adds the `mirror_id` column and scopes table-UUID uniqueness to native tables;
  builds on the mirror config/CRUD/sprawl surface from
  `0014_federation_mirrors`. Hive Metastore and Glue's native API are documented
  as future source types.
- **Compaction executor** (bin-pack rewrite; built-in maintenance, new crate
  `meridian-executor`; [design doc](docs/design/compaction.md),
  [ADR 007](docs/adr/007-compaction-executor-arrow-parquet.md)). Reads a
  table's current snapshot, groups live data files by partition, and bin-packs
  the files below a target size (default 512 MiB, first-fit-decreasing) into
  rewrite groups, skipping partitions with fewer than `min_input_files`
  (default 5) candidates. Each group is read with `arrow`/`parquet`, realigned
  to the table's current schema **by Iceberg field id** (not name or position;
  field ids are written back into the output Parquet footer), and merged into
  one file, with the hard assertion that rows-in equals rows-out (minus any
  materialized deletes). Merge-on-read is applied during the rewrite: position-
  and equality-**delete files** are materialized into the compacted output and
  the fully-consumed delete files dropped (deletion vectors are refused with a
  reason, not silently dropped). Column lower/upper bounds are recomputed from
  the merged output for the primitive types with an unambiguous encoding; record
  and null counts are exact. The result is returned as an Iceberg `RewriteFiles`
  (`replace`) `CompactionPlan` — the add-snapshot `TableUpdate`s, the
  optimistic `TableRequirement`s (assert the table has not moved), and the new
  snapshot's manifests/manifest list — **without committing** (the commit path
  applies it as a normal, audited, snapshot-rollback-revertible commit).
  A **dry-run** mode reports the files it would write without writing; the
  engine never deletes input data files (snapshot expiry does that later); and
  re-running on an already-compacted table is a no-op (idempotent). Verified
  end-to-end on real Parquet + real Iceberg manifests: file count drops, every
  row survives, reversed-column and schema-evolved inputs realign, merge-on-read
  rows are absent from the output, and the produced updates/manifests parse
  back through `meridian-iceberg`.
- **Catalog as code** — `meridian plan -f bundle.yaml` and
  `meridian apply -f bundle.yaml` reconcile a running server toward a versioned
  YAML bundle (`apiVersion: meridian.dev/v1`, `kind: CatalogBundle`) declaring
  warehouses, namespaces, roles, grants, and webhooks. `apply` is idempotent
  (re-apply is a no-op), creates and updates only (**never deletes**; prune is
  out of scope for v1 and surfaced as `would-delete` warnings), reports
  per-resource success/failure, and exits non-zero on any failure. Bundle values
  support `${ENV_VAR}` interpolation so secrets stay out of the file. Tables and
  views are deliberately excluded — engines own them. Both commands talk only to
  the public `/api/v2` and `/v1` APIs.
  ([docs](docs/catalog-as-code.md), [ADR 006](docs/adr/006-catalog-as-code-bundles.md),
  [e2e](conformance/e2e/tests/test_catalog_as_code.py)).
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
  [Spark conformance smoke](conformance/engines/spark/README.md).
- **`replaceView` treats `add-schema` field ids as provisional too.**
  Spark 3.5's `CREATE OR REPLACE VIEW` numbers the replacement schema from
  0, exactly like `CREATE VIEW`, so replacing an *existing* view failed
  with the same `field id 0 is not positive` error the create path used to
  hit. The replace commit path now assigns fresh 1-based field ids
  server-side (remapping `identifier-field-ids`) before the updates reach
  the view-metadata builder, mirroring `createView` — the same statement
  now behaves identically whether or not the view already existed. View
  schemas have no cross-version field-id continuity to protect, so the
  reassignment is safe; the builder keeps its strict field-id validation
  for updates the server constructs itself. Found by the [Spark
  conformance smoke](conformance/engines/spark/README.md).

### Security

- **Authentication and authorization are off by default.** With the
  default `auth.mode = "disabled"`, every endpoint — including warehouse
  and RBAC management — is open to anyone who can reach the port, and
  authorization is bypassed entirely. Do not expose a disabled-mode server
  to untrusted networks. With `auth.mode = "oidc"`, access is
  deny-by-default RBAC across the namespace, table, and view surfaces; see
  the warning in [docs/api-status.md](docs/api-status.md).
