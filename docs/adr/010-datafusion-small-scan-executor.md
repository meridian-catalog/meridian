# 010. Build the small-scan query executor on DataFusion, with governed views for enforcement

## Status

Accepted

## Context

Two product surfaces need to run user/agent SQL over the Iceberg tables the
catalog owns, for small scans only:

- the **agent gateway** `run_sql` tool (Pillar H, H-F3): an agent's query is
  validated, policy-rewritten, size-capped, and routed — small scans run on a
  built-in executor, large ones on a registered customer engine (Trino,
  Snowflake, Spark, ClickHouse);
- the **workbench** (Pillar L, L-F1): a human runs SQL over any governed asset,
  small queries served in-process for a zero-setup first taste.

Both are *real query execution* — projection, filter, aggregation, joins — over
data files the catalog reads itself. This is exactly the operation
[ADR 007](007-compaction-executor-arrow-parquet.md) deferred: it built the
compaction executor directly on `arrow` + `parquet` because "bin-pack
compaction is not a query", and named DataFusion as the choice to revisit "when
the first [operation that is a real query] lands … [it] gets its own ADR". That
time is now.

Three forces shape the decision:

1. **The spec names DataFusion** for the built-in executor in several places
   (§8.1 "meridian-workers … DataFusion + Parquet rewrite pipelines", C-F4 tier
   1, H-F3, L-F1). It is a mature, embeddable, Apache-2.0 SQL engine built on the
   same `arrow`/`parquet` crates the catalog already uses.
2. **`run_sql` must be *governed* execution**, not just execution. The same
   Pillar-D row filters and column masks that scan planning enforces (D-F2.1)
   must apply here, from the same resolved decision, so there is no second
   policy implementation to keep in agreement — and masked/dropped columns must
   be *absent*, not nulled (H-F2), so an agent's prompt cannot leak the schema of
   restricted data.
3. **The embedded executor is small-scan only.** The cost of a scan must be
   estimated and capped *before* any data file is read (H-F3: "results
   size-capped + cost-estimated *before* execution"), so an oversized query
   costs a metadata read, not I/O, and is refused with a message an agent can
   relay to re-ask against a big engine.

The open questions were: which engine; how to enforce policy through it; and how
to read Iceberg data files into it.

### Version reality: DataFusion pins a different arrow major

DataFusion 54 (current) depends on `arrow`/`parquet` **58**; the rest of the
workspace — `meridian-executor` (per ADR 007) and `meridian-server` — is pinned
to `arrow`/`parquet` **59**. The two arrow majors cannot share types across a
public API. Downgrading the whole workspace to 58 would drag two crates outside
this change's ownership (including the server) and re-open ADR 007's pinned
versions, for no functional gain.

## Decision

We will build a new crate, **`meridian-query`**, on **DataFusion 54** for the
small-scan `run_sql`/workbench executor, and enforce policy by compiling each
table's resolved `Enforcement` into a **governed SQL view**.

**Engine.** DataFusion, pinned lean: default features off, only `sql` plus the
expression groups the governed-view masks need (`crypto_expressions` for SHA-256
hashing, `unicode_expressions` for `SUBSTR` partial masks, and
string/regex/nested/datetime for general SELECTs). We deliberately do **not**
enable DataFusion's own `parquet`/`avro`/`compression` file readers.

**Reading.** We read Iceberg data files ourselves with the `parquet` crate —
mapping columns by Iceberg **field id** (never name or position), synthesizing
null columns for schema-evolved files, and materializing v2 merge-on-read
position/equality deletes — exactly the read discipline `meridian-executor`
already proved, then hand DataFusion **in-memory Arrow batches** (a `MemTable`
per table). Meridian's manifest engine stays the single source of truth for what
"a live file" is; DataFusion never touches Iceberg metadata. A v3 deletion
vector (which a plain Parquet reader cannot apply) is refused with a clear
reason rather than returning possibly-deleted rows.

**Enforcement via governed views.** The caller resolves row filters and column
masks with `meridian_authz::resolve_filters_and_masks` and hands us the
`Enforcement`. We register a table's raw data under a private name, then create a
DataFusion **view** under the query's name whose projection masks or drops
columns and whose `WHERE` folds in the row filter. The user's SQL references the
view, so enforcement is executed by DataFusion's own planner — the same closed
`RowPredicate` AST the scan-plan seam consumes, rendered to fixed SQL shapes with
escaped literals. A dropped column is omitted from the projection entirely
(absent, not nulled); a `Custom` mask we cannot verify against this engine
fails **closed** to a drop, exactly as the scan-plan path treats an unresolvable
custom mask.

**Cost cap before execution.** The scanned-bytes and row estimates are summed
from manifest file sizes/record counts and checked against the caller's `Caps`
*before* any data file is read; over-cap queries return a caller-facing refusal
that names the escape hatch.

**Read-only gate.** The SQL is parsed and must be exactly one `Query` statement
(a `SELECT` or read-only CTE); DML, DDL, `COPY`, `SET`, and multi-statement input
are refused via the parser (not string matching), so a governed query can never
mutate. The gate is fail-closed: any statement kind not explicitly recognized is
refused.

**The two-arrow-majors question.** `meridian-query` uses arrow/parquet **58**
(what DataFusion 54 pins), as an internal implementation detail. Arrow types
never cross the crate's public API — inputs are Iceberg metadata + storage
bytes, outputs are JSON rows + provenance — so the two arrow majors in the final
binary (58 here, 59 in `meridian-executor`/`meridian-server`) never meet at a
type boundary. This compiles and is correct; it costs some binary size and
compile time until the workspace unifies on one arrow major (a separate
coordination, out of this change's scope).

### Alternatives considered

- **`arrow` + `parquet` directly (as ADR 007 did).** Rejected: this operation
  *is* a query (joins, aggregation, a SQL planner). Re-implementing a SQL engine
  over Arrow arrays to avoid a dependency would be far more code and risk than
  adopting the mature engine the spec names, and would still not give us SQL.
- **DuckDB (embedded).** The spec mentions DuckDB for client-side WASM sample
  exploration (L-F1) and as an alternative built-in engine. Rejected for the
  server-side small-scan path: DataFusion is native Rust on our existing
  arrow/parquet stack (no C++ FFI, no second memory model), and gives us a Rust
  `TableProvider`/`MemTable` seam and SQL-AST access for the read-only gate.
  DuckDB-WASM remains the client-side option; this ADR is about the server
  executor.
- **Downgrade the workspace to arrow/parquet 58** so one arrow major spans
  everything. Rejected for now: it reaches into `meridian-server` and re-opens
  ADR 007's pins, both outside this change, for no functional gain. Unifying the
  arrow major is worth doing, but as its own coordinated change.
- **Enforce policy by rewriting the user's SQL AST** (inject `WHERE`, rewrite
  masked column references). Rejected: it means owning a SQL rewriter that must
  correctly handle every shape of user query (subqueries, aliases, `SELECT *`,
  CTEs) — fragile and a large attack surface. Governed views push enforcement
  into DataFusion's planner: the user query is untouched, and a dropped column is
  absent by construction because it is not in the view.

## Consequences

**Easier.**

- Real SQL — projection, filter, aggregation, joins — over catalog-owned tables,
  the shared engine behind both `run_sql` and the workbench, with a small,
  arrow-free public surface (`run(sql, tables, caps) -> QueryOutput`).
- Enforcement is deterministic and lives in one place: the resolved
  `Enforcement` compiles to a governed view, executed by DataFusion's planner.
  Row filters use the same closed predicate AST as scan planning, so `run_sql`
  and scan-plan enforcement cannot drift. Dropped columns are absent (H-F2);
  custom masks fail closed.
- The cost cap is enforced from manifest stats before any I/O, so an oversized
  agent query is a cheap, polite refusal — and `bytes_scanned` is recorded on
  every result for budget accounting (H-F4).
- Every result carries provenance (tables + snapshot ids read, policies applied,
  columns masked) so agents can cite (H-F3) and a CISO audit can answer "which
  agent read which columns under which policy".
- Manifest read stays the metadata source of truth: DataFusion sees only
  in-memory batches, so the engine matrix's coverage of our Iceberg read path is
  unaffected.

**Harder / deferred.**

- **A second arrow/parquet major (58) is linked into the final binary** alongside
  the 59 that `meridian-executor`/`meridian-server` use. It compiles and is safe
  (arrow types never cross `meridian-query`'s API), but it adds binary size and
  compile time. Unifying the workspace on one arrow major — ideally by moving
  everything to whatever major the chosen DataFusion release pins — is tracked as
  follow-up, not blocked by this.
- **Small-scan only, by construction.** Tables are read fully into memory before
  querying, so this executor is correct only within the byte/row cap. Predicate
  and projection *pushdown into the Parquet read* (to scan less than the whole
  file) is a future optimization; today the cap plus in-memory execution is the
  boundary, and big scans route to customer engines.
- **Deletion vectors (v3 Puffin) are refused, not applied** — same limitation as
  the compaction executor (ADR 007), for the same reason (a plain Parquet reader
  cannot materialize them). Position/equality *delete files* (the v2
  merge-on-read shape engines emit today) are fully applied.

**Neutral.**

- The executor is a pure function of *(metadata + bytes + policy + SQL + caps)*.
  It resolves no names, loads no metadata, and reads no policy — the caller (a
  server route) hands it `TableMetadata`, a `Storage` handle, and a resolved
  `Enforcement`, and audits the agent action itself (H-F4). This keeps the crate
  testable against in-memory Iceberg fixtures and free of any dependency on the
  agent-gateway or server crates.
- The agent gateway (`meridian-agents`, Pillar H) will own an async,
  `dyn`-dispatched `QueryExecutor` trait so it can hold the executor behind a
  trait object without depending on DataFusion. That trait is not yet written;
  `meridian-query` exposes the clean `run(...)` function the trait will map onto,
  and a thin adapter in the server (or the gateway's wiring) will implement
  `QueryExecutor` by calling `run`. The gateway takes no DataFusion dependency;
  this crate takes none on the gateway.
