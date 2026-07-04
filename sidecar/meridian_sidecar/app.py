"""FastAPI app for the Meridian transpilation sidecar.

A thin HTTP shell over ``core``. The Rust server (meridian-core) spawns or
connects to this over localhost (default port 8200) and calls it for
universal-view transpilation (G-F1, §8.5), SQL parsing for lineage/view
analysis, and — in wave 2 — metric compilation (G-F2).

Endpoints:
  POST /v1/transpile       -> TranspileResponse
  POST /v1/parse           -> ParseResponse
  POST /v1/compile_metric  -> CompileMetricResponse (stub)
  GET  /healthz            -> HealthResponse
"""

from __future__ import annotations

import sqlglot
from fastapi import FastAPI

from . import core
from .llm_assist import get_llm_assist
from .schemas import (
    CompileMetricRequest,
    CompileMetricResponse,
    HealthResponse,
    ParseRequest,
    ParseResponse,
    TranspileRequest,
    TranspileResponse,
)

app = FastAPI(
    title="Meridian transpilation sidecar",
    version="0.1.0",
    description=(
        "SQLGlot-backed transpilation, parsing, and metric compilation for "
        "Meridian's universal-view subsystem. Deterministic first; optional "
        "BYO-key LLM-assist fallback, off unless configured."
    ),
)

# One process-wide fallback handle. The default is the no-op (no network).
_LLM = get_llm_assist()


@app.get("/healthz", response_model=HealthResponse)
def healthz() -> HealthResponse:
    return HealthResponse(
        status="ok",
        sqlglot_version=sqlglot.__version__,
        llm_assist=_LLM.available,
    )


@app.post("/v1/transpile", response_model=TranspileResponse)
def transpile(req: TranspileRequest) -> TranspileResponse:
    return core.transpile(
        sql=req.sql,
        from_dialect=req.from_dialect,
        to_dialect=req.to_dialect,
        llm=_LLM,
    )


@app.post("/v1/parse", response_model=ParseResponse)
def parse(req: ParseRequest) -> ParseResponse:
    return core.parse(sql=req.sql, dialect=req.dialect)


@app.post("/v1/compile_metric", response_model=CompileMetricResponse)
def compile_metric(req: CompileMetricRequest) -> CompileMetricResponse:
    return core.compile_metric(metric=req.metric, to_dialect=req.to_dialect)
