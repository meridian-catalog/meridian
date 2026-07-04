"""Deterministic SQLGlot transpilation, parsing, and metric compilation.

This module holds the logic; ``app.py`` is a thin HTTP shell over it. It never
imports FastAPI, so it is trivially unit-testable offline.

The status machine (see ``schemas.Status``):

  verified     SQLGlot transpiled AND the output re-parses in the target dialect
               (parse-back succeeds).
  best_effort  SQLGlot produced output but parse-back failed/differed, OR the
               LLM-assist fallback produced the output (never trusted blindly).
  unsupported  SQLGlot raised and no fallback produced a valid result.
"""

from __future__ import annotations

import sqlglot
from sqlglot import exp
from sqlglot.errors import SqlglotError

from .llm_assist import LlmAssist, NoopLlmAssist
from .schemas import (
    ColumnRef,
    CompileMetricResponse,
    Diagnostic,
    MetricInput,
    ParseResponse,
    Severity,
    Status,
    TranspileResponse,
)


def _diag(severity: Severity, code: str, message: str) -> Diagnostic:
    return Diagnostic(severity=severity, code=code, message=message)


def _parses(sql: str, dialect: str) -> bool:
    """True if ``sql`` parses cleanly in ``dialect`` (the parse-back check)."""
    try:
        parsed = sqlglot.parse(sql, read=dialect)
    except SqlglotError:
        return False
    return bool(parsed) and all(stmt is not None for stmt in parsed)


def transpile(
    *,
    sql: str,
    from_dialect: str,
    to_dialect: str,
    llm: LlmAssist | None = None,
) -> TranspileResponse:
    """Transpile ``sql`` from one dialect to another with a status label.

    Deterministic SQLGlot first. Only if SQLGlot raises do we consult the
    (default no-op) LLM-assist fallback, and any fallback result is validated by
    parse-back and labelled ``best_effort`` — never ``verified``.
    """
    llm = llm or NoopLlmAssist()
    diagnostics: list[Diagnostic] = []

    try:
        out_statements = sqlglot.transpile(sql, read=from_dialect, write=to_dialect)
    except SqlglotError as err:
        # SQLGlot could not handle it. Try the fallback (no-op by default).
        return _fallback(
            sql=sql,
            from_dialect=from_dialect,
            to_dialect=to_dialect,
            error=str(err),
            llm=llm,
        )

    translated = ";\n".join(s for s in out_statements if s)
    if not translated:
        diagnostics.append(_diag(Severity.error, "empty_output", "SQLGlot produced no output."))
        return TranspileResponse(
            sql=None,
            status=Status.unsupported,
            from_dialect=from_dialect,
            to_dialect=to_dialect,
            diagnostics=diagnostics,
        )

    # Parse-back check in the TARGET dialect.
    if _parses(translated, to_dialect):
        status = Status.verified
    else:
        status = Status.best_effort
        diagnostics.append(
            _diag(
                Severity.warning,
                "parse_back_diff",
                "Translated SQL did not re-parse cleanly in the target dialect; "
                "output is best-effort and should be reviewed.",
            )
        )

    return TranspileResponse(
        sql=translated,
        status=status,
        from_dialect=from_dialect,
        to_dialect=to_dialect,
        diagnostics=diagnostics,
    )


def _fallback(
    *,
    sql: str,
    from_dialect: str,
    to_dialect: str,
    error: str,
    llm: LlmAssist,
) -> TranspileResponse:
    """Handle the SQLGlot-raised case via the LLM-assist fallback."""
    diagnostics = [_diag(Severity.error, "parse_error", f"SQLGlot could not transpile: {error}")]

    if not llm.available:
        # Default path: fallback unavailable -> unsupported. No network touched.
        return TranspileResponse(
            sql=None,
            status=Status.unsupported,
            from_dialect=from_dialect,
            to_dialect=to_dialect,
            diagnostics=diagnostics,
        )

    candidate = llm.translate(
        sql=sql, from_dialect=from_dialect, to_dialect=to_dialect, error=error
    )
    if candidate is None:
        return TranspileResponse(
            sql=None,
            status=Status.unsupported,
            from_dialect=from_dialect,
            to_dialect=to_dialect,
            diagnostics=diagnostics,
        )

    # A fallback result is NEVER verified. It must parse-back to be offered at
    # all; if it does not even parse, we discard it and stay unsupported.
    if not _parses(candidate, to_dialect):
        diagnostics.append(
            _diag(
                Severity.warning,
                "llm_assist_invalid",
                "LLM-assist output did not parse in the target dialect; discarded.",
            )
        )
        return TranspileResponse(
            sql=None,
            status=Status.unsupported,
            from_dialect=from_dialect,
            to_dialect=to_dialect,
            diagnostics=diagnostics,
        )

    diagnostics.append(
        _diag(
            Severity.warning,
            "llm_assist_used",
            "Translation produced by LLM-assist fallback; labelled best-effort "
            "and validated by parse-back only. Review before trusting.",
        )
    )
    return TranspileResponse(
        sql=candidate,
        status=Status.best_effort,
        from_dialect=from_dialect,
        to_dialect=to_dialect,
        diagnostics=diagnostics,
    )


def parse(*, sql: str, dialect: str) -> ParseResponse:
    """Parse ``sql`` and extract referenced tables and columns.

    Powers column-level lineage and view analysis. On a parse failure we return
    ``unsupported`` with empty extractions rather than raising.
    """
    try:
        expression = sqlglot.parse_one(sql, read=dialect)
    except SqlglotError as err:
        return ParseResponse(
            ast_json=None,
            tables=[],
            columns=[],
            status=Status.unsupported,
            diagnostics=[_diag(Severity.error, "parse_error", str(err))],
        )

    if expression is None:
        return ParseResponse(
            ast_json=None,
            tables=[],
            columns=[],
            status=Status.unsupported,
            diagnostics=[_diag(Severity.error, "empty_ast", "No statement parsed.")],
        )

    tables = _extract_tables(expression)
    columns = _extract_columns(expression)

    return ParseResponse(
        ast_json=expression.dump(),
        tables=tables,
        columns=columns,
        status=Status.verified,
        diagnostics=[],
    )


def _extract_tables(expression: exp.Expression) -> list[str]:
    """De-duplicated table identifiers (schema-qualified where present)."""
    seen: dict[str, None] = {}
    for tbl in expression.find_all(exp.Table):
        parts = [p.name for p in (tbl.args.get("catalog"), tbl.args.get("db"), tbl.this) if p]
        name = ".".join(parts)
        if name:
            seen.setdefault(name, None)
    return list(seen)


def _extract_columns(expression: exp.Expression) -> list[ColumnRef]:
    """Referenced columns. Qualifier is captured only when present in the SQL;
    we never invent a table binding the query did not state.
    """
    seen: dict[tuple[str, str | None], None] = {}
    out: list[ColumnRef] = []
    for col in expression.find_all(exp.Column):
        name = col.name
        if not name or name == "*":
            continue
        table = col.table or None
        key = (name, table)
        if key not in seen:
            seen[key] = None
            out.append(ColumnRef(name=name, table=table))
    return out


def compile_metric(*, metric: MetricInput, to_dialect: str) -> CompileMetricResponse:
    """Compile a metric definition to a chosen engine's SQL, deterministically.

    Builds ``SELECT <dimensions>, <expression> AS <name> FROM <source>
    [WHERE <filters ANDed>] [GROUP BY <dimensions>]`` with SQLGlot's expression
    builder — parsing every fragment in the metric's canonical ``dialect`` and
    rendering the whole statement in ``to_dialect``. This is fully deterministic
    (no LLM): the same status machine as :func:`transpile` applies — ``verified``
    when the rendered SQL re-parses cleanly in the target dialect, ``best_effort``
    when it does not (surfaced with a ``parse_back_diff`` diagnostic), and
    ``unsupported`` when a fragment cannot be parsed at all (``sql`` is null).

    Fragments are parsed, never string-concatenated, so a malformed measure or
    filter is rejected honestly rather than emitting broken SQL.
    """
    from_dialect = metric.dialect or "trino"
    diagnostics: list[Diagnostic] = []

    # Parse each fragment in the canonical dialect. A parse failure anywhere is
    # unsupported — we never paper over an unparseable fragment.
    try:
        measure = sqlglot.maybe_parse(metric.expression, dialect=from_dialect)
        if measure is None:
            raise SqlglotError("empty measure expression")
        select_exprs: list[exp.Expression] = []
        for dim in metric.dimensions:
            parsed_dim = sqlglot.maybe_parse(dim, dialect=from_dialect)
            if parsed_dim is None:
                raise SqlglotError(f"empty dimension expression: {dim!r}")
            select_exprs.append(parsed_dim)
        # The measure is aliased to the metric name (a stable output column).
        select_exprs.append(exp.alias_(measure, metric.name))

        query = exp.select(*select_exprs).from_(_parse_source(metric.source, from_dialect))

        for filt in metric.filters:
            parsed_filter = sqlglot.maybe_parse(filt, dialect=from_dialect)
            if parsed_filter is None:
                raise SqlglotError(f"empty filter expression: {filt!r}")
            query = query.where(parsed_filter)

        if metric.dimensions:
            # GROUP BY the same dimension expressions that lead the projection.
            group_exprs = [
                sqlglot.maybe_parse(dim, dialect=from_dialect) for dim in metric.dimensions
            ]
            query = query.group_by(*group_exprs)
    except SqlglotError as err:
        return CompileMetricResponse(
            sql=None,
            status=Status.unsupported,
            diagnostics=[_diag(Severity.error, "parse_error", f"could not compile metric: {err}")],
        )

    rendered = query.sql(dialect=to_dialect)
    if not rendered:
        return CompileMetricResponse(
            sql=None,
            status=Status.unsupported,
            diagnostics=[_diag(Severity.error, "empty_output", "compilation produced no SQL.")],
        )

    if _parses(rendered, to_dialect):
        status = Status.verified
    else:
        status = Status.best_effort
        diagnostics.append(
            _diag(
                Severity.warning,
                "parse_back_diff",
                "Compiled SQL did not re-parse cleanly in the target dialect; "
                "output is best-effort and should be reviewed.",
            )
        )

    return CompileMetricResponse(sql=rendered, status=status, diagnostics=diagnostics)


def _parse_source(source: str, dialect: str) -> exp.Expression:
    """Parse a metric's source identifier into a table expression.

    ``source`` is a dotted identifier (``db.schema.table``) or a view name; it is
    parsed as a table reference in the canonical dialect so it renders with the
    target dialect's quoting.
    """
    parsed = sqlglot.maybe_parse(source, dialect=dialect, into=exp.Table)
    if parsed is None:
        # Fall back to a bare table node built from the raw identifier.
        return exp.to_table(source, dialect=dialect)
    return parsed
