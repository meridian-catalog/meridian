# Transpilation sidecar: universal views, SQL parsing, metric compilation

Meridian's SQL dialect work — translating a view authored in one engine's
dialect so another engine can read it, extracting the tables and columns a
statement references, and (later) compiling metric definitions to SQL — runs in
a small standalone Python service, the **transpilation sidecar**. It is the only
Python on Meridian's request path, and it is quarantined there on purpose: the
correctness-critical catalog path stays Rust, and the sidecar is stateless,
localhost-scoped, and horizontally scalable.

This document is the contract. It describes the HTTP surface, the status
machine, dialect coverage, and the LLM-assist wiring, so the Rust server and
wave-2 features can consume it without reading the Python.

Status: implemented and tested offline (`sidecar/`, `pytest` + `ruff`).
`/v1/transpile`, `/v1/parse`, and `/v1/compile_metric` are all functional.
Covers Pillar G, G-F1 (universal views), G-F2 (metric compilation), and the
parsing half of §8.5. The Rust server consumes this contract via
`crates/meridian-server/src/sidecar.rs` on the `LoadView` path (universal-view
translation) and the `/api/v2/metrics/{id}/compile` path (metric compilation).

## Why a sidecar

SQLGlot is the best open dialect transpiler, and it is Python. Rather than
reimplement it or shell out per-call, Meridian runs it as a long-lived FastAPI
service that the Rust core calls over localhost HTTP (default port 8200). The
service holds no state: every request carries everything it needs, so any
instance can serve any request and instances scale independently of the core.

## The deterministic-first rule

**Transpilation is deterministic SQLGlot first.** SQLGlot translates the
statement; the sidecar then validates the output and labels it. An optional
LLM-assist fallback exists for constructs SQLGlot cannot translate, but it is:

- **off by default** — it does nothing unless an operator configures a BYO API
  key (OpenAI / Anthropic / Bedrock / self-hosted);
- **never primary** — it runs only after SQLGlot has *raised*, never instead of
  it;
- **never trusted blindly** — any fallback output is validated by parse-back and
  labelled `best_effort`, never `verified`;
- **never called in tests or by default** — with no key, the fallback is a no-op
  that returns "still unsupported" and touches no network.

If SQLGlot cannot translate a construct and no fallback is configured, the honest
answer is `unsupported`. Meridian does not paper over that with a guess.

## HTTP contract

Base: `http://127.0.0.1:8200` (configurable). All request/response bodies are
JSON. The authoritative schemas live in `sidecar/meridian_sidecar/schemas.py`
(Pydantic); the shapes below mirror them.

### `POST /v1/transpile`

Translate a statement from one dialect to another.

Request:

```json
{
  "sql": "SELECT DATE_ADD(d, 7) FROM t",
  "from_dialect": "spark",
  "to_dialect": "trino",
  "schema": null
}
```

- `sql` — the statement to translate.
- `from_dialect`, `to_dialect` — SQLGlot dialect names (see coverage below).
- `schema` — optional, reserved. A `{table: {column: type}}` map to enable
  type-aware translation/validation in a later wave. Ignored today.

Response:

```json
{
  "sql": "SELECT DATE_ADD('DAY', 7, CAST(CAST(d AS TIMESTAMP) AS DATE)) FROM t",
  "status": "verified",
  "from_dialect": "spark",
  "to_dialect": "trino",
  "diagnostics": []
}
```

- `sql` — translated statement, or `null` when `status` is `unsupported`.
- `status` — see the status machine below.
- `diagnostics` — zero or more `{severity, code, message}` notes (see codes).

### `POST /v1/parse`

Parse a statement and extract referenced tables and columns. Powers
column-level lineage and view analysis.

Request:

```json
{ "sql": "SELECT a.x FROM sales a JOIN dim b ON a.id = b.id", "dialect": "trino" }
```

Response:

```json
{
  "ast_json": [ /* SQLGlot Expression.dump() — opaque, round-trippable JSON */ ],
  "tables": ["sales", "dim"],
  "columns": [
    { "name": "x",  "table": "a" },
    { "name": "id", "table": "a" },
    { "name": "id", "table": "b" }
  ],
  "status": "verified",
  "diagnostics": []
}
```

- `ast_json` — the SQLGlot AST serialized via `Expression.dump()`, a nested JSON
  structure callers treat as opaque (it round-trips via `Expression.load()`). It
  is `null` on parse failure.
- `tables` — de-duplicated table identifiers, schema-qualified where the SQL
  qualified them (e.g. `analytics.public.events`).
- `columns` — referenced columns. `table` is populated **only** when the SQL
  qualified the column (e.g. `a.x` → `{name: "x", table: "a"}`); an unqualified
  column reports `table: null`. The parser never invents a binding the query did
  not state — the same no-fabrication discipline lineage enforces.
- On a parse failure the endpoint returns `status: unsupported` with empty
  `tables`/`columns` and a `parse_error` diagnostic — it never raises to the
  caller.

### `POST /v1/compile_metric`

Compile a metric definition to a chosen engine's SQL, deterministically (G-F2).
The metric's measure `expression`, `dimensions`, and `filters` are authored in a
canonical `dialect`; the sidecar builds
`SELECT <dimensions>, <expression> AS <name> FROM <source> [WHERE <filters ANDed>]
[GROUP BY <dimensions>]` with SQLGlot's expression builder and renders the whole
statement in `to_dialect`. Every fragment is *parsed* (never string-concatenated),
so a malformed measure or filter is rejected honestly rather than emitting broken
SQL. This is fully deterministic — no LLM is ever consulted for metric compilation.

Request:

```json
{
  "metric": {
    "name": "revenue",
    "expression": "SUM(amount)",
    "source": "sales",
    "dimensions": ["region"],
    "filters": ["status = 'paid'"],
    "dialect": "trino"
  },
  "to_dialect": "duckdb"
}
```

Response:

```json
{
  "sql": "SELECT region, SUM(amount) AS revenue FROM sales WHERE status = 'paid' GROUP BY region",
  "status": "verified",
  "diagnostics": []
}
```

The same status machine as `/v1/transpile` applies: `verified` when the rendered
SQL re-parses cleanly in the target dialect, `best_effort` (with a
`parse_back_diff` diagnostic) when it does not, and `unsupported` (with `sql`
null) when a fragment cannot be parsed at all.

### `GET /healthz`

```json
{ "status": "ok", "sqlglot_version": "30.12.0", "llm_assist": false }
```

`llm_assist` is `true` only when a BYO-key provider is configured. Use this both
as a liveness probe and to confirm the LLM-assist posture of a running instance.

## Status machine

Every transpile/parse result carries one of three statuses. These are the labels
Meridian surfaces to users and writes into Iceberg view representations, so their
meanings are exact and honest.

| Status | Meaning |
| --- | --- |
| `verified` | SQLGlot translated the statement **and** the translated output re-parses cleanly in the target dialect (parse-back succeeds). Safe to serve to an engine. For `/v1/parse`, means the statement parsed. |
| `best_effort` | SQLGlot produced output but a construct was approximated or the parse-back surfaced a difference, **or** the LLM-assist fallback produced the output. Usable, but the UI must surface the diagnostics; never presented as guaranteed-correct. |
| `unsupported` | SQLGlot raised and no fallback produced a valid result. No output is served as correct (`sql` is `null`). |

The gate for `verified` is **parse-back**: the sidecar re-parses SQLGlot's output
in the *target* dialect. If that fails, the result drops to `best_effort` (with a
`parse_back_diff` diagnostic) rather than being trusted. Optional live EXPLAIN
validation against a registered engine (§8.5) is a later enhancement layered on
top of this; it can only *downgrade* a `verified` to `best_effort`, never upgrade.

### Diagnostic codes

Diagnostics are `{severity, code, message}`. Stable codes:

- `parse_error` (error) — SQLGlot raised while parsing/transpiling.
- `empty_output` (error) — SQLGlot returned no statement.
- `empty_ast` (error) — parse produced no statement.
- `parse_back_diff` (warning) — output did not re-parse cleanly in the target
  dialect; result is `best_effort`.
- `llm_assist_used` (warning) — output came from the LLM-assist fallback;
  `best_effort`, parse-back-validated only, review before trusting.
- `llm_assist_invalid` (warning) — fallback output failed parse-back and was
  discarded; result stayed `unsupported`.
- `not_implemented` (info) — reserved for a not-yet-built stub endpoint (none
  currently; `compile_metric` is fully implemented).

## Dialect coverage

Dialects are passed straight to SQLGlot, which supports the full set §8.5 tracks:
**Spark, Trino, Snowflake, DuckDB, ClickHouse, StarRocks, BigQuery, Postgres**
(and more). The offline test suite exercises real SQL — joins, window functions,
casts, date functions, CTEs, aggregates — across Spark→Trino, Spark→DuckDB, and
Trino→Snowflake, asserting the correct status label for each.

Coverage is **status-labelled, not claimed**: a given construct on a given
dialect pair is `verified`, `best_effort`, or `unsupported` based on what
actually happens at translation + parse-back time, per request. This is the
honest-docs posture — Meridian never advertises a translation as working that its
own validation did not confirm. A per-dialect-pair conformance table (§8.5) is
populated from real runs, not asserted up front.

## LLM-assist wiring (BYO key)

The fallback is a pluggable interface, `LlmAssist`
(`sidecar/meridian_sidecar/llm_assist.py`): given a construct SQLGlot could not
translate, return a best-effort translation string, or `None` if it cannot help.

The default implementation is `NoopLlmAssist`:

- `available` is always `False`;
- `translate(...)` unconditionally returns `None`;
- it reads no API key and makes no network call.

This is what guarantees no LLM is reached without a key — and what the test suite
asserts (an unsupported construct with the default fallback stays `unsupported`,
proving no network call).

### Enabling a provider

An operator enables a real fallback by setting env vars before starting the
sidecar:

```
MERIDIAN_LLM_ASSIST_PROVIDER = openai | anthropic | bedrock | self_hosted
# plus the provider's credential, e.g.:
ANTHROPIC_API_KEY = ...     # for anthropic
OPENAI_API_KEY    = ...     # for openai
# (bedrock uses the ambient AWS credential chain; self_hosted uses an endpoint URL)
```

`get_llm_assist()` is the single factory the app calls at startup. A provider
adapter implements `LlmAssist`, builds its client lazily from those env vars, and
returns `None` on any error so a provider outage degrades to `unsupported` rather
than failing the request. The concrete adapters ship in wave 2; the protocol is
the stable seam today. Until an adapter is registered, `get_llm_assist()` returns
the no-op even if `MERIDIAN_LLM_ASSIST_PROVIDER` is set — the safety default wins,
so a misconfiguration can never silently start calling a network.

**Invariants that hold no matter how a provider is wired:**

1. LLM-assist runs only *after* SQLGlot has raised — never as the primary path.
2. Any fallback output is validated by parse-back; if it does not parse in the
   target dialect it is discarded and the status stays `unsupported`.
3. Fallback output is labelled `best_effort` and carries an `llm_assist_used`
   diagnostic. It is never `verified`.
4. No provider is contacted in tests or by default.

## Running it

```
cd sidecar
./run.sh                 # uv sync + uvicorn on 127.0.0.1:8200
```

Configuration:

- `MERIDIAN_SIDECAR_HOST` (default `127.0.0.1` — localhost only by design)
- `MERIDIAN_SIDECAR_PORT` (default `8200`)

The Rust server spawns or connects to the sidecar on the configured port; it
health-checks `GET /healthz` before routing transpile/parse traffic. In K8s the
sidecar scales as its own stateless Deployment (§8.7).

### Development

```
cd sidecar
uv sync
uv run pytest        # offline; never touches a network or a real LLM
uv run ruff check .
uv run ruff format --check .
```

## Layout

```
sidecar/
  pyproject.toml               uv project (python>=3.12): fastapi, uvicorn, pydantic, sqlglot
  run.sh                       start script (uv sync + uvicorn)
  meridian_sidecar/
    app.py                     FastAPI app — thin HTTP shell over core
    core.py                    transpile / parse / compile_metric logic + status machine
    schemas.py                 Pydantic request/response models (the contract)
    llm_assist.py              pluggable BYO-key fallback interface + no-op default
  tests/
    test_transpile.py          Spark/Trino/Snowflake/DuckDB spreads, status labels
    test_parse.py              table + column extraction
    test_llm_assist.py         no-op proves no network; best-effort labelling path
    test_app.py                HTTP-level smoke over the FastAPI app
```
