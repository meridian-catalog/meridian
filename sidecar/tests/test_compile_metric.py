"""Metric-compilation tests (G-F2): deterministic SQLGlot compilation.

Offline only. No test may reach a network or a real LLM — metric compilation is
pure SQLGlot, never LLM-assisted.
"""

from __future__ import annotations

from meridian_sidecar import core
from meridian_sidecar.schemas import MetricInput, Status


def test_measure_with_dimension_and_filter_compiles_verified():
    resp = core.compile_metric(
        metric=MetricInput(
            name="revenue",
            expression="SUM(amount)",
            source="analytics.sales",
            dimensions=["region"],
            filters=["status = 'paid'"],
            dialect="trino",
        ),
        to_dialect="trino",
    )
    assert resp.status == Status.verified
    assert resp.sql is not None
    upper = resp.sql.upper()
    assert "SUM(AMOUNT) AS REVENUE" in upper
    assert "GROUP BY REGION" in upper
    assert "WHERE" in upper


def test_scalar_metric_has_no_group_by():
    resp = core.compile_metric(
        metric=MetricInput(
            name="total",
            expression="SUM(amount)",
            source="sales",
            dialect="trino",
        ),
        to_dialect="snowflake",
    )
    assert resp.status == Status.verified
    assert resp.sql is not None
    assert "GROUP BY" not in resp.sql.upper()


def test_cross_dialect_compilation_spark_to_duckdb():
    resp = core.compile_metric(
        metric=MetricInput(
            name="uniq_users",
            expression="COUNT(DISTINCT user_id)",
            source="db.events",
            dimensions=["dt"],
            dialect="spark",
        ),
        to_dialect="duckdb",
    )
    assert resp.status in (Status.verified, Status.best_effort)
    assert resp.sql is not None
    assert "COUNT(DISTINCT" in resp.sql.upper()


def test_multiple_dimensions_and_filters():
    resp = core.compile_metric(
        metric=MetricInput(
            name="net",
            expression="SUM(amount) - SUM(refunds)",
            source="fct.orders",
            dimensions=["region", "channel"],
            filters=["ts > '2024-01-01'", "status <> 'void'"],
            dialect="trino",
        ),
        to_dialect="trino",
    )
    assert resp.status == Status.verified
    assert resp.sql is not None
    upper = resp.sql.upper()
    assert "GROUP BY REGION, CHANNEL" in upper
    # Both filters survive, ANDed.
    assert upper.count("WHERE") == 1
    assert "AND" in upper


def test_malformed_measure_is_unsupported_not_crash():
    resp = core.compile_metric(
        metric=MetricInput(
            name="bad",
            expression="SUM(",
            source="sales",
            dialect="trino",
        ),
        to_dialect="trino",
    )
    assert resp.status == Status.unsupported
    assert resp.sql is None
    assert any(d.code == "parse_error" for d in resp.diagnostics)


def test_malformed_filter_is_unsupported():
    resp = core.compile_metric(
        metric=MetricInput(
            name="m",
            expression="SUM(amount)",
            source="sales",
            filters=["status = = ="],
            dialect="trino",
        ),
        to_dialect="trino",
    )
    assert resp.status == Status.unsupported
    assert resp.sql is None


def test_dotted_source_renders_qualified():
    resp = core.compile_metric(
        metric=MetricInput(
            name="cnt",
            expression="COUNT(*)",
            source="catalog.schema.orders",
            dialect="trino",
        ),
        to_dialect="trino",
    )
    assert resp.status == Status.verified
    assert resp.sql is not None
    # The three-part identifier is preserved (a reserved-word segment would be
    # quoted, but the parts survive).
    assert "catalog.schema.orders" in resp.sql
