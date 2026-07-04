# Meridian transpilation sidecar

A small, stateless FastAPI service that runs SQLGlot for Meridian's dialect work:
universal-view transpilation (G-F1), SQL parsing for lineage/view analysis, and —
in wave 2 — metric compilation (G-F2). The Rust core calls it over localhost HTTP
(default port 8200). It is the only Python on the request path, quarantined here
by design; the correctness-critical catalog path stays Rust.

Full contract, status machine, dialect coverage, and LLM-assist wiring:
[`../docs/design/transpilation.md`](../docs/design/transpilation.md).

## Run

```
./run.sh          # uv sync + uvicorn on 127.0.0.1:8200
```

Config: `MERIDIAN_SIDECAR_HOST` (default `127.0.0.1`), `MERIDIAN_SIDECAR_PORT`
(default `8200`).

## Endpoints

- `POST /v1/transpile` — `{sql, from_dialect, to_dialect, schema?}` →
  `{sql, status, diagnostics[]}`
- `POST /v1/parse` — `{sql, dialect}` → `{ast_json, tables[], columns[], status}`
- `POST /v1/compile_metric` — wave-2 stub (returns `unsupported`)
- `GET /healthz` — `{status, sqlglot_version, llm_assist}`

`status` is one of `verified` / `best_effort` / `unsupported`. `verified`
requires a successful parse-back of the output in the target dialect.

## LLM-assist

Deterministic SQLGlot first. An optional BYO-key fallback (OpenAI / Anthropic /
Bedrock / self-hosted) handles constructs SQLGlot cannot translate; it is off
unless configured, its output is always `best_effort` and parse-back-validated,
and it is never called in tests or by default. See the design doc for wiring.

## Develop

```
uv sync
uv run pytest                 # offline; never touches a network or a real LLM
uv run ruff check .
uv run ruff format --check .
```
