# The MCP agent gateway (the agent firewall)

Status: **Gateway, governed context tools, budgets, kill switch, the full audit
chain, and governed query execution implemented and tested.** `run_sql` and
`preview_table` now run on the built-in `DataFusion` small-scan executor
(`meridian-query`, ADR 010), governed by the same Pillar-D policies scan
planning enforces: the SQL's referenced tables are resolved, each RBAC-checked
and ABAC-resolved for the calling agent, masked columns are dropped (H-F2),
results are size-capped and cost-estimated *before* execution (H-F4), and every
result carries provenance (tables + snapshot ids). `query_metrics` returns an
honest "semantic layer not populated" answer until the metric definitions land
(Pillar G). This document says exactly what is enforced; where another document
disagrees on a guarantee, this one wins.

Meridian exposes a Model Context Protocol (MCP) server at `/mcp` so AI agents
reach the lakehouse through **one governed front door**: context + query +
policy + budget + audit, engine-neutral. The research framing is that engines
ship query-without-context MCP servers, catalogs ship context-without-query,
and generic gateways govern tool calls but not *data*; Meridian unifies all
three at the layer that already owns identity, policy, semantics, and
credentials.

The product is not the tools. **The product is the audit chain** — the
court-grade answer to "which agent read which columns, for which purpose, under
which policy decision, and what did it touch" — a question most organizations
cannot answer today.

## 1. Where this sits

The gateway reuses the catalog's existing governance rather than reinventing
it:

- **Identity** is the OIDC layer (`crate::auth`): an agent authenticates with an
  ordinary bearer token. Agents are first-class principals (`kind = agent`),
  distinct from users and services.
- **RBAC** (`meridian_store::rbac`) decides whether an agent may read an asset
  at all.
- **ABAC** (`meridian_authz` + `crate::governance::resolve_scan_policy`) decides
  which rows/columns it sees — the *same* decision the scan planner enforces, so
  context and query can never disagree.
- **Audit** is the hash-chained log (`meridian_store::audit`) plus a per-tool
  activity ledger.

New in this subsystem: the MCP protocol surface, the agent governance envelope
(owner/purpose/lifecycle/kill switch), per-agent budgets, and the tool catalog.

```
        MCP client (agent)
              │  JSON-RPC 2.0 over HTTP (Streamable HTTP)
              ▼
   ┌──────────────────────────┐
   │  routes::mcp  (/mcp)      │  protocol boundary: initialize / tools/list /
   │                          │  tools/call, Origin check, session id, JSON-RPC
   └───────────┬──────────────┘  envelope, protocol-vs-tool error mapping
               │ tools/call
               ▼
   ┌──────────────────────────┐
   │  mcp::dispatch           │  the ONE governance chain (see §5):
   │                          │  identity → kill switch → lifecycle →
   │                          │  per-tool governance → audit (ledger + chain)
   └───────┬──────────┬───────┘
           │          │
   context::handle   query::handle
   (H-F2, governed   (H-F3, governed + budget; hands a governed
    reads, no budget) QueryRequest to the QueryExecutor seam)
```

Crate split (mirrors the ADR-009 boundary — decision vocabulary separate from
persistence and HTTP):

- **`meridian-agents`** (pure, no DB): the MCP wire types, the tool catalog, the
  `QueryExecutor` trait, the refusal/args-digest decision helpers.
- **`meridian-store::agent`**: the three tables + budget-window arithmetic + the
  activity ledger.
- **`meridian-server` `mcp/` + `routes/mcp.rs` + `routes/agents.rs`**: the HTTP
  endpoint, the governance wrapper, and the management control plane.

## 2. The protocol surface

Streamable HTTP, MCP spec revision **`2025-06-18`**, JSON-RPC 2.0. The transport
is used in its simplest spec-compliant profile: the client POSTs one JSON-RPC
message and the server answers with one `application/json` JSON-RPC message. The
gateway pushes no server-initiated messages, so it never opens an SSE stream — a
`GET /mcp` is answered `405` (the spec explicitly permits this for a server that
offers no stream), and `DELETE /mcp` is `405` (sessions are stateless).

| Method | Behavior |
|---|---|
| `initialize` | Negotiates the protocol version (the gateway speaks exactly `2025-06-18`), advertises the `tools` capability, returns `serverInfo`, and issues an `Mcp-Session-Id` header. |
| `notifications/initialized` | Acknowledged `202 Accepted`, no body (it is a JSON-RPC notification — no `id`). |
| `ping` | `{}`. |
| `tools/list` | The governed tool catalog (§3), each with `name`, `title`, `description`, `inputSchema`. |
| `tools/call` | Runs one tool through the governance chain (§5). |

**Security.** The transport spec's DNS-rebinding defense is implemented:
`POST /mcp` validates the `Origin` header when present, against the same CORS
allow-list the console uses (one allow-list, not two). Non-browser MCP clients
send no `Origin` and are unaffected.

### Protocol errors vs tool errors (the distinction the spec draws)

This distinction is load-bearing for the graceful-refusal requirement:

- A **protocol error** is a JSON-RPC `error` (with a standard code). Used for: a
  malformed request, an unknown method, an unknown *tool*, or a caller that is
  **not a registered agent** (the gateway is the agent door — a non-agent cannot
  use it at all).
- A **tool error** is a *successful* JSON-RPC `result` whose `CallToolResult`
  carries `isError: true`. Used for everything an agent should be able to read
  and **relay to a human**: a policy denial, a budget refusal, a kill-switched
  agent, an expired agent, or the not-yet-wired executor. The agent gets an
  actionable message, not a transport failure.

## 3. The tool catalog

Two classes. **Context** tools (H-F2) are governed reads that do not consume the
query budget. **Query** tools (H-F3) execute and are charged against the budget.
The class is declared in the catalog (`meridian_agents::catalog`) and the
wrapper keys off it, so a tool cannot accidentally skip the budget.

### Context tools (H-F2) — governed reads

| Tool | Returns |
|---|---|
| `search_assets` | Full-text search, filtered to the agent's visibility (restricted assets are not returned). |
| `get_table_context` | Schema + docs + owners + quality/trust score + freshness + contract status. **Masked/denied columns are ABSENT from the schema** (see §4). |
| `get_lineage` | The up/downstream graph around a table the agent can read. |
| `list_metrics` / `get_metric_definition` | The semantic-layer metrics (wave-2 subsystem — honest "not yet populated" until wired). |
| `list_data_products` | Certified data products (wave-2 subsystem). |
| `get_glossary_term` | A business-glossary term (wave-2 subsystem). |

### Query tools (H-F3) — governed execution

| Tool | Behavior |
|---|---|
| `query_metrics` | Compiles a metric query to SQL and executes it (the high-accuracy path for covered questions). *Semantic layer not yet populated — returns an honest "not populated" tool-error until metric definitions land (Pillar G).* |
| `run_sql` | Runs validated, policy-rewritten SQL on the built-in executor and returns rows + provenance (tables + snapshot ids). Small scans only; an oversized scan is refused before I/O with a route-to-an-engine message. |
| `preview_table` | A policy-safe sample of a table's rows (masked columns absent, row filters applied). |

Query tools resolve the same RBAC + ABAC governance the context tools do, price
the scan from manifest stats, check the budget with that estimate (refusing over
budget *before* any I/O), then run the query on the built-in `DataFusion`
executor via `mcp::engine`. Every step is audited. The one place the resolution
lives is the server (`mcp::engine`), because only there is the calling principal
available to resolve per-table policy — see §6.

## 4. Governed context: masked columns are *absent*

The headline H-F2 guarantee. When an agent calls `get_table_context`:

1. RBAC `READ` gates the table. A denied agent learns nothing — not even the
   schema shape (the whole call is refused).
2. The current schema is loaded (the column universe).
3. `crate::governance::resolve_scan_policy` runs the ABAC decision — the *same*
   function the scan planner uses. A full deny refuses the context; otherwise it
   returns `removed_columns` (masked or denied columns).
4. The returned schema is built from the column list **minus `removed_columns`**.

The masked column is **absent from the response, not nulled**. This is
deliberate and stronger than nulling: a prompt cannot leak the *existence* or
name of a restricted column, because the model never sees it. The structured
payload's schema block contains no trace of the removed column. This mirrors the
scan-plan enforcement rule (`docs/design/enforcement-matrix.md`, Layer 1: every
mask becomes column removal on the plan path — fail closed), so context and
query enforcement are the same decision applied two ways.

Proven by `tests/mcp_api.rs::get_table_context_omits_masked_column`: with no
policy the schema includes `amount`; after a column-mask policy binds to a tag
on `amount`, `amount` is absent while every other column remains.

## 5. The governance chain (`mcp::dispatch`)

Every `tools/call` runs the same chain, in one place, so a new tool cannot
forget a step:

1. **Identity.** The caller is resolved to a *registered agent*. The stored
   `principals` row (kind `agent`, created at registration) is the authority —
   **not** the token's edge-classification (an agent often authenticates with a
   client-credentials token, which the edge labels a *service*). A
   non-registered identity is a protocol error.
2. **Kill switch.** A disabled agent (`enabled = false`) is refused before any
   tool logic — a relayable tool-error, audited `refused_killed`.
3. **Lifecycle.** An agent past its `expires_at` is refused, audited
   `refused_expired`.
4. **Per-tool governance.** Context tools resolve RBAC + ABAC. Query tools
   additionally check-and-consume the budget (§7).
5. **Audit.** Whatever the outcome — allowed, denied, refused, error — the call
   writes an `agent_activity` ledger row **and** a tamper-evident `audit_log`
   chain entry, on the **same transaction**, cross-referenced by `audit_seq`
   (§8). If the audit write fails, the answer is *not* returned (we never hand
   back a result we could not record).

The per-tool handlers are pure producers of a `ToolResponse` (a governed answer
+ what it touched + the decision); `dispatch` renders it to the wire and writes
the audit. No handler writes audit or checks the kill switch itself.

## 6. Governed query execution (`mcp::engine` + the executor)

Query execution runs on the built-in `DataFusion` small-scan executor
(`meridian-query`, ADR 010). The server-side glue is `mcp::engine`, which owns
the resolution the executor cannot do (it is a pure function of *metadata +
bytes + policy + SQL*): it plans a query in three phases so cost is checked
before I/O.

**Plan → price → run** (`mcp::query::run_governed_query`):

1. **Plan** (`engine::plan`). The SQL's referenced tables are enumerated with
   the executor's *own* parser (`meridian_query::referenced_tables`), so the set
   matches exactly what the executor binds — no drift. Each table is resolved to
   its warehouse / namespace chain / `TableMetadata` / storage, checked for
   **RBAC READ**, and its **ABAC** decision resolved via
   `governance::resolve_query_enforcement` (the same Pillar-D primitive the scan
   planner uses). For an agent, every mask is folded to a **drop** (H-F2: a
   restricted column is absent, never nulled). An ABAC deny on any table
   short-circuits with a relayable refusal — before any budget is spent. Then
   the scan is priced from manifest stats (no data read).
2. **Budget** (fail before you charge). The estimated scanned bytes + a dollar
   estimate are checked against the agent's budget *before* execution
   (H-F3/H-F4). Over budget → a graceful, relayable `refused_budget`, nothing
   consumed and nothing run.
3. **Run**. The executor runs the governed query (a governed SQL *view* over
   each table's raw data applies the row filter and column drops via
   DataFusion's planner — the same closed predicate AST scan planning uses).
   Every result carries **provenance** (tables + snapshot ids, mapped to
   Meridian internal ids) so the agent can cite and the CISO audit can answer
   "which agent read which columns under which policy".

**Why the resolution lives in the server, not behind the trait.** The gateway's
`QueryExecutor` trait models one call as a single `QueryRequest` carrying *one*
table's policy — a shape that cannot express a multi-table join where each table
has its own row/column policy, and that has no principal to resolve policy with.
So the real resolution lives in `mcp::engine` (which has the `AppState` and the
`Principal`), and the query handlers call it directly. The trait is still wired
— the `DataFusionExecutor` implementing it is installed as the axum extension so
its label (`"datafusion"`) is recorded in the audit trail and the seam is
honestly "wired" — and it covers the table-free case (`SELECT 1`).

The same `mcp::engine` path powers the workbench (Pillar L, L-F1); the two
surfaces differ only in mask mode (agents drop, the workbench keeps
value-preserving masks) so `run_sql` and the workbench can never enforce policy
differently.

## 7. The agent model and budgets

### Agent envelope (`agent_principals`)

1:1 with a `principals` row of kind `agent`. Fields: `owner` (accountable
human/service audit string), `purpose` (the declared purpose statement,
consulted by purpose-conditioned ABAC), `environment` (`dev` | `prod`),
`expires_at` (hard stop), `review_at` (advisory recertification), and `enabled`
(the **kill switch** — `false` refuses every tool).

Scoped grants *to* an agent go through the ordinary RBAC API
(`/api/v2/grants` with the agent's principal id) — an agent is a first-class
principal, so its access reuses the existing grant machinery with TTL, not a
parallel one.

### Budgets (`agent_budgets`)

Per-agent caps with rolling-window counters (a `NULL` cap is uncapped):

- `queries_per_hour` — a per-hour rolling window.
- `scanned_bytes_per_day` and `dollar_cap_micros` (dollar estimate in
  micro-dollars, integer for exactness) — a shared per-day rolling window.

`check_and_consume_budget` is the one call a query tool makes before running: it
takes the budget row `FOR UPDATE`, rolls any elapsed window, and either refuses
(without consuming — the offending dimension, cap, and usage are returned for a
graceful message) or consumes one query + the estimated cost and allows. Context
tools never touch the query budget.

Wave-1 note: the pre-execution cost estimate is a conservative default (the
per-hour *queries* cap bites immediately; the bytes/dollar caps bite once the
executor reports real figures in wave 2). The window counters live in the row so
enforcement is one indexed read + one conditional update; the activity ledger
remains the source of truth if counters ever need rebuilding.

## 8. The audit chain (the product)

Every tool call writes two rows on one transaction:

- **`agent_activity`** (the ledger, queried for the CISO view): `tool`,
  `args_digest`, `decision` (`allowed | denied | refused_budget |
  refused_killed | refused_expired | error`), `purpose`, `rows_touched`,
  `bytes_scanned`, `cost_micros`, and `audit_seq` (the cross-reference).
- **`audit_log`** (the tamper-evident hash chain): an `agent.tool.<verb>` entry
  whose details carry the tool, the args digest, the decision, the purpose, and
  what was enforced (removed columns, applied policies, provenance).

The two are atomic — a tool call can never appear in one and not the other — and
cross-reference by `audit_seq`. The ledger is append-only; an agent's rows
survive its deletion (`ON DELETE SET NULL`, keeping the audit string) — evidence
outlives the agent.

**`args_digest`** is a stable sha256 over the *redacted shape* of the arguments
(keys sorted, values replaced by type tokens), so repeated calls correlate and
an auditor can prove *what kind of thing* was asked, without persisting raw,
possibly-sensitive argument values (a `run_sql` body, a search string) in the
ledger. The raw SQL still lands in the tamper-evident chain's details if the
operator wants it.

Proven by `tests/mcp_api.rs::every_tool_call_is_audited_with_its_decision`: a
ledger row per call, each with a decision, a 64-hex digest, and a non-null
`audit_seq`; the hash chain still verifies end to end after the writes; and the
audit query surface shows the `agent.tool.*` actions.

### Anomaly hooks and auto-suspend

The store exposes `recent_activity_count` (a per-agent recent-window count) as
the substrate for the anomaly signals the spec names (novel-table access,
off-hours, exfil-pattern volume). The kill switch is the response primitive: an
operator (or an auto-suspend policy) flips `enabled` to `false` via
`POST /api/v2/agents/{id}/suspend`, and every subsequent tool call is refused —
audited, per the chain above. The auto-suspend *policy* (thresholds, schedule)
is a thin worker on top of these primitives; the primitives are complete.

## 9. Management API (`/api/v2/agents`)

Management-gated (admin role or any `MANAGE_WAREHOUSE` grant), matching the
governance surface:

| Route | Action |
|---|---|
| `POST /api/v2/agents` | Register an agent (provision its principal from its OIDC identity + attach the envelope and budget). |
| `GET /api/v2/agents` | The agent registry. |
| `GET /api/v2/agents/{id}` | One agent's envelope + budget (caps + current window usage). |
| `POST /api/v2/agents/{id}/suspend` | Engage the kill switch (with an audited reason). |
| `POST /api/v2/agents/{id}/enable` | Release the kill switch. |
| `GET /api/v2/agents/activity` | The activity ledger (the CISO evidence view), filterable by agent/tool/decision, keyset-paginated. |

## 10. What is deferred (honest scope)

- **`query_metrics` compilation** — `run_sql` and `preview_table` run on the
  built-in executor now (§6); `query_metrics` returns an honest "semantic layer
  not populated" answer until metric definitions land (Pillar G), at which point
  it compiles a metric to SQL and runs it through the same governed path.
- **External-engine routing for big scans** — the built-in executor is
  small-scan only; an oversized scan is refused *before* I/O with a
  route-to-a-registered-engine message. The connection registry that submits big
  queries to a customer Trino/Snowflake/Spark/ClickHouse is a documented next
  step; the refusal is the honest boundary today.
- **The semantic layer / data products / glossary** — `list_metrics`,
  `get_metric_definition`, `list_data_products`, `get_glossary_term` return
  honest "not yet populated" answers until those subsystems are wired; the tool
  surface and its governance are stable so the wiring drops in without a
  protocol change.
- **Enterprise-managed authorization (Okta XAA)** — the gateway uses the
  standard OIDC layer today; the EMA extension is future work and does not change
  the governance model (it changes how the *token* is obtained, not what the
  gateway enforces).
- **Streaming / SSE** — the gateway answers requests with a single JSON response
  and needs no SSE; if server-initiated notifications are ever added, the GET
  stream lands then.
- **A2A / external-tool registry (H-F6)** — out of scope here.
