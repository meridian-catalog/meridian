"""Parse tests: table + column extraction for lineage and view analysis."""

from __future__ import annotations

from meridian_sidecar import core
from meridian_sidecar.schemas import Status


def test_parse_extracts_tables_and_columns():
    resp = core.parse(
        sql="SELECT s.id, s.amount FROM sales s JOIN customers c ON s.cid = c.id",
        dialect="trino",
    )
    assert resp.status == Status.verified
    assert set(resp.tables) == {"sales", "customers"}
    names = {c.name for c in resp.columns}
    assert {"id", "amount", "cid"}.issubset(names)


def test_parse_schema_qualified_tables():
    resp = core.parse(
        sql="SELECT id FROM analytics.public.events",
        dialect="trino",
    )
    assert resp.status == Status.verified
    assert resp.tables == ["analytics.public.events"]


def test_parse_column_qualifier_captured_only_when_present():
    resp = core.parse(
        sql="SELECT a.x, y FROM t a",
        dialect="duckdb",
    )
    by_name = {c.name: c.table for c in resp.columns}
    assert by_name["x"] == "a"
    assert by_name["y"] is None


def test_parse_ast_json_present():
    resp = core.parse(sql="SELECT 1", dialect="duckdb")
    assert resp.status == Status.verified
    assert resp.ast_json is not None


def test_parse_failure_is_unsupported_not_crash():
    resp = core.parse(sql="SELECT FROM )(", dialect="trino")
    assert resp.status == Status.unsupported
    assert resp.ast_json is None
    assert resp.tables == []
    assert resp.columns == []
