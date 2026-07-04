# The SQL workbench (the adoption wedge)

Status: **Implemented and tested.** A governed SQL API over the built-in
small-scan executor, with query history, saved queries, and a notebook-handoff
snippet generator, plus a console Workbench page over all of it.

The workbench (Pillar L, L-F1/L-F3) is an in-console SQL editor over governed
assets. It is deliberately **not** a BI suite (§12.4 non-goals): it is a
small-scan adoption wedge whose north-star metric is *time-to-first-query* —
vended credentials plus the built-in executor mean **zero engine setup** for the
first taste. Large queries route to a registered engine (or are refused with
guidance); the workbench never grows drag-drop explore or enterprise
dashboarding — we hand off to Hex/Omni/Tableau/Metabase and are the catalog
under them.

## 1. Where this sits

The workbench shares one governed execution path with the agent gateway
(`crate::mcp::engine`), so `run_sql` and the workbench can never enforce policy
differently. That path (plan → price → run) is documented in
[`agent-gateway.md` §6](agent-gateway.md); the workbench is a second caller of
it, differing only in **mask mode**:

| | Agent `run_sql` (H-F2) | Workbench (L-F1) |
|---|---|---|
| Masked column | **dropped** — absent from results (a restricted column must not leak into a prompt) | **value-preserving** — `hash(email)`, partial reveal, or NULL; the column stays, the value is hidden (a human sees a masked value) |
| Budget | per-agent (queries/hour, scanned-bytes/day, $-cap) | none (a human's query is governed by RBAC/ABAC + the small-scan cap) |
| Audit | the tamper-evident agent-activity chain (the firewall product) | ordinary authenticated request; the run is recorded in the user's own history (a convenience, not an audit surface) |

Both enforce **per-table RBAC READ** and the **ABAC** row/column decision from
`governance::resolve_query_enforcement` (the same Pillar-D primitive scan
planning uses), and both refuse an oversized scan *before* any I/O.

## 2. The API (`/api/v2/workbench`)

| Route | Action |
|---|---|
| `POST /query` | Run a governed SELECT. Body: `{ sql, warehouse?, namespace? }`. Returns columns, rows, `row_count`, `truncated`, `provenance` (tables + snapshot ids + policies applied), `bytes_scanned`, `duration_ms`. Row/column policy applied; small scans only. |
| `GET /history` | The caller's own recent queries, newest first (keyset-paginated by `?limit` + `?before`). |
| `GET /saved` | The workspace's saved queries. |
| `POST /saved` | Save a reusable query (`{ name, sql, warehouse?, namespace?, description? }`); name unique per workspace (case-insensitive). |
| `GET /saved/{id}` | One saved query. |
| `DELETE /saved/{id}` | Delete a saved query. |
| `POST /snippet` | The notebook handoff (L-F3): `{ warehouse, namespace, table }` → PyIceberg/Daft/Pandas connection snippets. RBAC READ required. |

### The query response's provenance

Every result carries the same provenance the agent path returns, so a workbench
user (and any downstream audit) sees exactly what was read and enforced:

```json
{
  "provenance": {
    "tables": [{ "name": "sales.eu", "table_id": "01J…", "table_uuid": "…", "snapshot_id": 42 }],
    "row_filter_policies": ["01J…"],
    "column_mask_policies": ["01J…"],
    "masked_columns": ["email"]
  }
}
```

## 3. Persistence (`meridian_store::workbench`, migration 0022)

Two workspace-scoped tables:

- **`workbench_saved_queries`** — named, reusable queries (name unique per
  workspace, case-insensitively). Create/delete are workspace mutations and are
  **audited + outboxed** on the same transaction (the invariant the whole
  codebase holds: no mutation without its audit row), exactly like webhooks and
  consumers.
- **`workbench_query_history`** — an append-only, per-principal recent-query log
  (SQL, warehouse, outcome `ok`/`error`/`denied`, row count, bytes scanned,
  duration, message). Recording a history row is **deliberately not audited** and
  emits **no** outbox event — it *is* a log, and a human's ad-hoc SELECT is not a
  catalog mutation (the same rationale by which `consumer` cursor commits are not
  audited). A history-write failure never fails the query the user already ran
  (best-effort, log-and-ignore).

## 4. The notebook handoff (L-F3)

`POST /snippet` generates "open in PyIceberg / Daft / Pandas" connection code for
a governed table. The snippet points at Meridian's IRC endpoint and the table
identifier; the client obtains **scoped, vended credentials at connect time**
via the standard IRC credential flow — **no secret is embedded in the snippet**.
The IRC host is a documented placeholder the user replaces (Meridian sits behind
the operator's ingress and does not know its own external URL — we never
fabricate a hostname). RBAC READ on the table is required, so a user only gets a
snippet for a table they can read.

## 5. The console Workbench page

A real-data page (`console/src/app/workbench/page.tsx`) over the API above: a SQL
editor (⌘/Ctrl+Enter to run), a warehouse picker and an optional default
namespace, a results table with the provenance line (tables read, policies
applied, masked columns), a saved-queries list (load / delete), a clickable
history list, and the notebook-snippet generator. Everything is real `/api/v2`
data — no mock state.

## 6. Scope and limits (honest)

- **Small-scan only.** The executor reads tables fully into memory; correctness
  holds within the byte/row cap (128 MiB / 5M rows by default). An oversized
  scan is a cheap, polite refusal that names the escape hatch — it is not run.
- **No external-engine routing yet.** The connection registry that submits big
  queries to a customer Trino/Snowflake/Spark/ClickHouse is a documented next
  step; today the refusal is the boundary.
- **No visualization / dashboards / scheduled digests (L-F2).** The workbench is
  the query wedge; charting and pinning are future work and explicitly not a
  BI-suite build.
- **Read-only, always.** The executor's parser gate refuses DML/DDL/`COPY`/
  multi-statement input, so a workbench query can never mutate.
