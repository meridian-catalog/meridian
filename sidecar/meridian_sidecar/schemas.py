"""HTTP contract for the Meridian transpilation sidecar.

These Pydantic models are the request/response schemas for the endpoints in
``app.py``. They are the source of truth for what the Rust server (wave 2) sends
and receives. Keep them stable; additive changes only where possible.
"""

from __future__ import annotations

from enum import StrEnum

from pydantic import BaseModel, Field


class Status(StrEnum):
    """The transpile/validation status machine.

    - ``verified``: SQLGlot translated the statement AND the translated output
      parses back cleanly in the target dialect (round-trip succeeded). Safe to
      serve to an engine.
    - ``best_effort``: SQLGlot produced output, but a construct was approximated
      or the parse-back of the output surfaced a difference. Usable, but the UI
      must surface the diagnostics; never presented as guaranteed-correct.
    - ``unsupported``: SQLGlot raised while parsing/transpiling, and no fallback
      produced a trustworthy result. No output is served as correct.
    """

    verified = "verified"
    best_effort = "best_effort"
    unsupported = "unsupported"


class Severity(StrEnum):
    info = "info"
    warning = "warning"
    error = "error"


class Diagnostic(BaseModel):
    """A single machine-readable note about a transpile/parse outcome."""

    severity: Severity
    code: str = Field(description="Stable short code, e.g. 'parse_error', 'parse_back_diff'.")
    message: str


# --- /v1/transpile ----------------------------------------------------------


class TranspileRequest(BaseModel):
    sql: str
    from_dialect: str = Field(description="Source SQL dialect, e.g. 'spark', 'trino'.")
    to_dialect: str = Field(description="Target SQL dialect, e.g. 'trino', 'duckdb'.")
    # Reserved: optional schema (table -> {column: type}) to improve type-aware
    # translation and validation in a later wave. Ignored today.
    schema_: dict[str, dict[str, str]] | None = Field(default=None, alias="schema")

    model_config = {"populate_by_name": True}


class TranspileResponse(BaseModel):
    sql: str | None = Field(
        default=None,
        description="Translated SQL, or null when status is 'unsupported'.",
    )
    status: Status
    from_dialect: str
    to_dialect: str
    diagnostics: list[Diagnostic] = Field(default_factory=list)


# --- /v1/parse --------------------------------------------------------------


class ColumnRef(BaseModel):
    """A referenced column. ``table`` is populated only when unambiguously
    qualified in the SQL; otherwise null (the resolver cannot invent a binding).
    """

    name: str
    table: str | None = None


class ParseRequest(BaseModel):
    sql: str
    dialect: str


class ParseResponse(BaseModel):
    ast_json: object | None = Field(
        default=None,
        description=(
            "SQLGlot AST serialized via Expression.dump() (a nested JSON structure); "
            "null on parse failure. Treated as opaque round-trippable JSON by callers."
        ),
    )
    tables: list[str] = Field(
        default_factory=list, description="Referenced table identifiers, de-duplicated."
    )
    columns: list[ColumnRef] = Field(default_factory=list)
    status: Status
    diagnostics: list[Diagnostic] = Field(default_factory=list)


# --- /v1/compile_metric (stub contract; implemented in wave 2) --------------


class MetricInput(BaseModel):
    """A metric definition compiled deterministically to a chosen engine's SQL.

    Mirrors the Rust semantic model (G-F2): a measure ``expression`` over a
    ``source``, optionally grouped by ``dimensions`` and constrained by
    ``filters``. The fragments are authored in ``dialect`` (the metric's
    canonical dialect); compilation parses in that dialect and renders in the
    request's ``to_dialect``.
    """

    name: str
    expression: str = Field(description="Aggregation expression, e.g. 'SUM(amount)'.")
    source: str = Field(description="Source table or view identifier.")
    dimensions: list[str] = Field(default_factory=list)
    filters: list[str] = Field(default_factory=list)
    dialect: str = Field(
        default="trino",
        description="Canonical dialect the expression/dimensions/filters are authored in.",
    )


class CompileMetricRequest(BaseModel):
    metric: MetricInput
    to_dialect: str


class CompileMetricResponse(BaseModel):
    sql: str | None = None
    status: Status
    diagnostics: list[Diagnostic] = Field(default_factory=list)


# --- /healthz ---------------------------------------------------------------


class HealthResponse(BaseModel):
    status: str = "ok"
    sqlglot_version: str
    llm_assist: bool = Field(description="True if a BYO-key LLM-assist provider is configured.")
