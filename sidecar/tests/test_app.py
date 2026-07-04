"""HTTP-level smoke tests over the FastAPI app (offline, via TestClient)."""

from __future__ import annotations

from fastapi.testclient import TestClient

from meridian_sidecar.app import app

client = TestClient(app)


def test_healthz():
    resp = client.get("/healthz")
    assert resp.status_code == 200
    body = resp.json()
    assert body["status"] == "ok"
    assert body["llm_assist"] is False
    assert "sqlglot_version" in body


def test_transpile_endpoint_verified():
    resp = client.post(
        "/v1/transpile",
        json={"sql": "SELECT 1 AS one", "from_dialect": "trino", "to_dialect": "duckdb"},
    )
    assert resp.status_code == 200
    body = resp.json()
    assert body["status"] == "verified"
    assert body["sql"] is not None


def test_transpile_endpoint_unsupported():
    resp = client.post(
        "/v1/transpile",
        json={"sql": "SELECT FROM )(", "from_dialect": "spark", "to_dialect": "trino"},
    )
    assert resp.status_code == 200
    assert resp.json()["status"] == "unsupported"


def test_parse_endpoint():
    resp = client.post(
        "/v1/parse",
        json={"sql": "SELECT id FROM sales", "dialect": "trino"},
    )
    assert resp.status_code == 200
    body = resp.json()
    assert body["tables"] == ["sales"]
    assert body["status"] == "verified"


def test_compile_metric_endpoint_compiles():
    resp = client.post(
        "/v1/compile_metric",
        json={
            "metric": {
                "name": "revenue",
                "expression": "SUM(amount)",
                "source": "sales",
                "dimensions": ["region"],
                "filters": ["status = 'paid'"],
                "dialect": "trino",
            },
            "to_dialect": "trino",
        },
    )
    assert resp.status_code == 200
    body = resp.json()
    assert body["status"] == "verified"
    assert body["sql"] is not None
    assert "GROUP BY REGION" in body["sql"].upper()


def test_compile_metric_endpoint_rejects_malformed():
    resp = client.post(
        "/v1/compile_metric",
        json={
            "metric": {"name": "bad", "expression": "SUM(", "source": "sales"},
            "to_dialect": "trino",
        },
    )
    assert resp.status_code == 200
    body = resp.json()
    assert body["status"] == "unsupported"
    assert body["sql"] is None
