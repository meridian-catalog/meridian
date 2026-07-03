"""Pillar D Layer-1 scan-plan enforcement, proven with a real PyIceberg client.

A PyIceberg client creates and writes a MinIO-backed Iceberg table through
Meridian. We then classify a column pii + bind a column-mask policy, tag the
table + bind a row-filter policy, and issue the IRC server-side scan-plan
request (`POST .../plan`) — the exact wire path a 1.11-planning client (DuckDB's
iceberg extension, PyIceberg's REST planning) uses. We assert the plan a thin
client receives is ENFORCED: the masked column's per-column statistics are
absent from every returned scan task, and the row filter is injected as a
residual on every task.

Auth is disabled on the e2e server, so the client is the anonymous principal;
enforcement runs the same way (a masking policy still strips columns, and there
is no RBAC gate to pass in disabled mode). The authenticated, least-privilege
path (a viewer granted only READ) is covered by the Rust integration test
`crates/meridian-server/tests/governance_api.rs`; this proves the *wire
contract a real client consumes*.
"""

import uuid
from types import SimpleNamespace

import boto3
import pyarrow as pa
import pytest
import requests
from botocore.config import Config
from pyiceberg.schema import Schema
from pyiceberg.types import DoubleType, LongType, NestedField, StringType

from conftest import (
    MINIO_ACCESS_KEY,
    MINIO_ENDPOINT,
    MINIO_SECRET_KEY,
    ServerErrorRecorder,
    create_warehouse,
)
from lifecycle import make_catalog

S3_CLIENT_PROPS = {
    "s3.endpoint": MINIO_ENDPOINT,
    "s3.access-key-id": MINIO_ACCESS_KEY,
    "s3.secret-access-key": MINIO_SECRET_KEY,
    "s3.path-style-access": "true",
    "s3.region": "us-east-1",
}

# id (long, required), region (string), amount (double) — we mask `amount`
# (field 3) and row-filter on `region` (field 2).
GOV_SCHEMA = Schema(
    NestedField(1, "id", LongType(), required=True),
    NestedField(2, "region", StringType(), required=False),
    NestedField(3, "amount", DoubleType(), required=False),
)

ARROW_SCHEMA = pa.schema(
    [
        pa.field("id", pa.int64(), nullable=False),
        pa.field("region", pa.string(), nullable=True),
        pa.field("amount", pa.float64(), nullable=True),
    ]
)


def minio_client():
    return boto3.client(
        "s3",
        endpoint_url=MINIO_ENDPOINT,
        aws_access_key_id=MINIO_ACCESS_KEY,
        aws_secret_access_key=MINIO_SECRET_KEY,
        region_name="us-east-1",
        config=Config(s3={"addressing_style": "path"}, connect_timeout=5, retries={"max_attempts": 1}),
    )


@pytest.fixture(scope="module")
def bucket(run_id):
    client = minio_client()
    name = f"e2e-gov-{run_id}"
    try:
        client.create_bucket(Bucket=name)
    except Exception as exc:
        pytest.skip(f"MinIO not reachable at {MINIO_ENDPOINT}: {exc}")
    return name


@pytest.fixture(scope="module")
def env(base_url, run_id, bucket):
    warehouse = f"e2e_gov_{run_id}"
    create_warehouse(
        base_url,
        warehouse,
        f"s3://{bucket}/warehouse",
        {
            "endpoint": MINIO_ENDPOINT,
            "path-style": "true",
            "region": "us-east-1",
            "access-key-id": MINIO_ACCESS_KEY,
            "secret-access-key": MINIO_SECRET_KEY,
        },
    )
    recorder = ServerErrorRecorder()
    catalog = make_catalog(base_url, warehouse, extra_props=S3_CLIENT_PROPS)
    recorder.attach(catalog)

    ns = f"ns_gov_{run_id}"
    table_name = "sales"
    catalog.create_namespace(ns)
    table = catalog.create_table(f"{ns}.{table_name}", schema=GOV_SCHEMA)

    # 300 rows across 3 regions, real Parquet written through Meridian.
    regions = ["region_000", "region_001", "region_002"]
    ids = list(range(300))
    table.append(
        pa.table(
            {
                "id": pa.array(ids, pa.int64()),
                "region": pa.array([regions[i % 3] for i in ids], pa.string()),
                "amount": pa.array([float(i) * 2.5 for i in ids], pa.float64()),
            },
            schema=ARROW_SCHEMA,
        )
    )
    recorder.assert_clean()

    return SimpleNamespace(
        base_url=base_url,
        warehouse=warehouse,
        ns=ns,
        table=table_name,
        run_id=run_id,
    )


def _lb_keys(task: dict) -> list[int]:
    """Lower-bound stat field ids present on a scan task's data file."""
    return task.get("data-file", {}).get("lower-bounds", {}).get("keys", [])


def _plan(env) -> dict:
    """Issue the IRC server-side scan plan (what a planning client sends)."""
    ep = f"{env.base_url}/iceberg/v1/{env.warehouse}/namespaces/{env.ns}/tables/{env.table}/plan"
    resp = requests.post(ep, json={}, timeout=20)
    assert resp.status_code == 200, f"plan: {resp.status_code} {resp.text}"
    return resp.json()


def _post(env, path: str, body: dict) -> dict:
    resp = requests.post(f"{env.base_url}{path}", json=body, timeout=15)
    assert resp.status_code < 300, f"{path}: {resp.status_code} {resp.text}"
    return resp.json() if resp.text.strip() else {}


def test_scan_plan_enforces_mask_and_row_filter(env):
    # Baseline: no policies -> the client sees `amount` stats (field 3), no
    # residual.
    plan = _plan(env)
    tasks = plan.get("file-scan-tasks", [])
    assert tasks, f"baseline plan returned no tasks: {plan}"
    assert any(3 in _lb_keys(t) for t in tasks), (
        f"baseline must expose `amount` (field 3) stats: {tasks[0]}"
    )
    assert tasks[0].get("residual-filter") is None, "baseline carries no residual"

    # Governance: tag `amount` pii + hash mask; tag table residency:eu + row
    # filter region = region_000. Unique tag keys per run (isolation).
    suffix = uuid.uuid4().hex[:8]
    pii, res = f"pii{suffix}", f"res{suffix}"
    pii_tag = _post(env, "/api/v2/governance/tags", {"key": pii, "value": "amount"})["id"]
    res_tag = _post(env, "/api/v2/governance/tags", {"key": res, "value": "eu"})["id"]

    _post(
        env,
        "/api/v2/governance/tags/assignments",
        {
            "tag_id": pii_tag,
            "target": {
                "securable_type": "column",
                "warehouse": env.warehouse,
                "namespace": env.ns,
                "table": env.table,
                "column": "amount",
            },
        },
    )
    _post(
        env,
        "/api/v2/governance/tags/assignments",
        {
            "tag_id": res_tag,
            "target": {
                "securable_type": "table",
                "warehouse": env.warehouse,
                "namespace": env.ns,
                "table": env.table,
            },
        },
    )

    mask_pol = _post(
        env,
        "/api/v2/governance/policies",
        {
            "name": f"mask-amount-{suffix}",
            "kind": "column_mask",
            "definition": {
                "type": "tag_column_mask",
                "tag": f"{pii}:amount",
                "exempt_groups": [],
                "mask": {"kind": "hash"},
            },
        },
    )["id"]
    _post(env, f"/api/v2/governance/policies/{mask_pol}/bindings", {"target_type": "tag", "tag_id": pii_tag})

    filt_pol = _post(
        env,
        "/api/v2/governance/policies",
        {
            "name": f"eu-rows-{suffix}",
            "kind": "row_filter",
            "definition": {
                "type": "tag_row_filter",
                "tag": f"{res}:eu",
                "exempt_groups": [],
                "predicate": {"op": "eq", "column": "region", "value": "region_000"},
            },
        },
    )["id"]
    _post(env, f"/api/v2/governance/policies/{filt_pol}/bindings", {"target_type": "tag", "tag_id": res_tag})

    # Enforced plan: the same wire request, now enforced.
    plan = _plan(env)
    tasks = plan.get("file-scan-tasks", [])
    assert tasks, f"enforced plan returned no tasks: {plan}"

    # (1) masked column absent from EVERY task's stats; unmasked still present.
    for t in tasks:
        assert 3 not in _lb_keys(t), f"masked `amount` (field 3) leaked: {t}"
        assert 1 in _lb_keys(t), f"unmasked `id` (field 1) should be present: {t}"

    # (2) row filter injected as a residual referencing region.
    residual = tasks[0].get("residual-filter")
    assert residual and "region" in str(residual), f"row filter not injected: {residual}"


def test_effective_policy_reports_the_decision(env):
    # The effective-policy read for the anonymous principal on this table
    # reflects the mask bound in the previous test (module-scoped env).
    resp = requests.get(
        f"{env.base_url}/api/v2/governance/effective-policy",
        params={
            "principal": "user:anonymous",
            "warehouse": env.warehouse,
            "namespace": env.ns,
            "table": env.table,
        },
        timeout=15,
    )
    assert resp.status_code == 200, f"effective-policy: {resp.status_code} {resp.text}"
    body = resp.json()
    assert body["denied"] is False, body
    assert "amount" in body["masked_columns"], body
