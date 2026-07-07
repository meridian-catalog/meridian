"""Transpilation tests: real SQL across dialects with correct status labels.

Offline only. No test may reach a network or a real LLM.
"""

from __future__ import annotations

from meridian_sidecar import core
from meridian_sidecar.schemas import Status


def test_spark_to_trino_join_and_cast():
    resp = core.transpile(
        sql=(
            "SELECT a.id, CAST(a.amount AS DECIMAL(10, 2)) AS amt "
            "FROM sales a JOIN dim b ON a.id = b.id WHERE a.amount > 100"
        ),
        from_dialect="spark",
        to_dialect="trino",
    )
    assert resp.status == Status.verified
    assert resp.sql is not None
    assert "JOIN" in resp.sql.upper()


def test_spark_to_trino_window_function():
    resp = core.transpile(
        sql=(
            "SELECT id, ROW_NUMBER() OVER (PARTITION BY dept ORDER BY salary DESC) AS rn FROM emp"
        ),
        from_dialect="spark",
        to_dialect="trino",
    )
    assert resp.status == Status.verified
    assert resp.sql is not None
    assert "OVER" in resp.sql.upper()


def test_spark_to_duckdb_cte():
    resp = core.transpile(
        sql=(
            "WITH recent AS (SELECT id, ts FROM events WHERE ts > '2024-01-01') "
            "SELECT COUNT(*) FROM recent"
        ),
        from_dialect="spark",
        to_dialect="duckdb",
    )
    assert resp.status == Status.verified
    assert resp.sql is not None
    assert "WITH" in resp.sql.upper()


def test_spark_to_duckdb_date_function_translated():
    # Spark DATE_ADD vs DuckDB — SQLGlot rewrites the dialect-specific form.
    resp = core.transpile(
        sql="SELECT DATE_ADD(order_date, 7) AS due FROM orders",
        from_dialect="spark",
        to_dialect="duckdb",
    )
    assert resp.status in (Status.verified, Status.best_effort)
    assert resp.sql is not None


def test_trino_to_snowflake_approx_and_casts():
    resp = core.transpile(
        sql=(
            "SELECT dept, APPROX_DISTINCT(user_id) AS uniq, "
            "CAST(ts AS TIMESTAMP) AS t FROM logs GROUP BY dept"
        ),
        from_dialect="trino",
        to_dialect="snowflake",
    )
    assert resp.status in (Status.verified, Status.best_effort)
    assert resp.sql is not None
    assert "GROUP BY" in resp.sql.upper()


def test_unsupported_construct_does_not_crash():
    # Deliberately malformed SQL -> SQLGlot raises -> unsupported, no exception.
    resp = core.transpile(
        sql="SELECT FROM WHERE ORDER BY )(",
        from_dialect="spark",
        to_dialect="trino",
    )
    assert resp.status == Status.unsupported
    assert resp.sql is None
    assert any(d.code == "parse_error" for d in resp.diagnostics)


def test_verified_requires_parse_back():
    # A plain statement that round-trips cleanly.
    resp = core.transpile(
        sql="SELECT 1 AS one",
        from_dialect="trino",
        to_dialect="duckdb",
    )
    assert resp.status == Status.verified


def test_deeply_nested_sql_does_not_crash_with_recursionerror():
    # SQLGlot's recursive parser raises RecursionError (not a SqlglotError) on
    # pathologically nested SQL. It must be caught, not escape as a 500.
    depth = 5_000
    nested = "(" * depth + "SELECT 1" + ")" * depth
    resp = core.transpile(sql=nested, from_dialect="spark", to_dialect="trino")
    # Either it transpiles or it is reported unsupported/best-effort — never a
    # raised exception. The point of the test is that this call returns.
    assert resp.status in (Status.verified, Status.best_effort, Status.unsupported)


def test_deeply_nested_sql_parses_returns_false_not_crash():
    depth = 5_000
    nested = "(" * depth + "SELECT 1" + ")" * depth
    # _parses must swallow RecursionError and return False, not raise.
    assert core._parses(nested, "trino") in (True, False)
