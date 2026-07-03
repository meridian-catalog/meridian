"""pyiceberg against Meridian with VENDED credentials (MinIO STS).

The client is configured with ONLY the catalog URI — no s3 keys anywhere.
pyiceberg sends `X-Iceberg-Access-Delegation: vended-credentials` by
default; the warehouse opts into STS vending, so every table load/create
response carries short-lived session credentials scoped to that table's
prefix, and the client reads/writes object storage with them.
"""

from types import SimpleNamespace

import boto3
import pyarrow as pa
import pytest
import requests
from botocore.config import Config

from conftest import (
    MINIO_ACCESS_KEY,
    MINIO_ENDPOINT,
    MINIO_SECRET_KEY,
    ServerErrorRecorder,
    create_warehouse,
)
from lifecycle import ARROW_SCHEMA, ICEBERG_SCHEMA, make_batch, make_catalog

ROLE_ARN = "arn:minio:iam:::role/meridian-vend"


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
    name = f"e2e-vend-{run_id}"
    try:
        client.create_bucket(Bucket=name)
    except Exception as exc:  # connection refused, etc.
        pytest.skip(f"MinIO not reachable at {MINIO_ENDPOINT}: {exc}")
    return name


@pytest.fixture(scope="module")
def env(base_url, run_id, bucket):
    warehouse = f"e2e_vend_{run_id}"
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
            "vending": "sts",
            "vending.role-arn": ROLE_ARN,
        },
    )

    recorder = ServerErrorRecorder()
    # THE point of this module: no s3.* properties at all. Everything the
    # client needs — endpoint, addressing style, credentials — is vended.
    catalog = make_catalog(base_url, warehouse, extra_props=None)
    recorder.attach(catalog)

    return SimpleNamespace(
        base_url=base_url,
        warehouse=warehouse,
        catalog=catalog,
        ns=f"ns_vend_{run_id}",
        recorder=recorder,
    )


def test_write_and_read_with_vended_credentials_only(env):
    """Full round trip — create, append, scan — with zero client-side keys."""
    env.catalog.create_namespace(env.ns)
    table = env.catalog.create_table(f"{env.ns}.events", schema=ICEBERG_SCHEMA)
    table.append(make_batch(0, 25))

    fresh = env.catalog.load_table(f"{env.ns}.events")
    result = fresh.scan().to_arrow()
    assert result.num_rows == 25
    assert sorted(result["id"].to_pylist()) == list(range(25))
    env.recorder.assert_clean()


def test_load_table_response_carries_scoped_session_credentials(env, bucket):
    """The raw LoadTableResult: session credentials (not the warehouse's
    static keys) in `config`, mirrored in `storage-credentials`, scoped to
    the table's own location prefix."""
    resp = requests.get(
        f"{env.base_url}/iceberg/v1/{env.warehouse}/namespaces/{env.ns}/tables/events",
        headers={"X-Iceberg-Access-Delegation": "vended-credentials"},
        timeout=30,
    )
    assert resp.status_code == 200, f"load: {resp.status_code} {resp.text}"
    body = resp.json()
    config = body["config"]
    for key in ("s3.access-key-id", "s3.secret-access-key", "s3.session-token"):
        assert key in config, f"missing {key}: {sorted(config)}"
    # Short-lived session keys — the warehouse's parent keys never leak.
    assert config["s3.access-key-id"] != MINIO_ACCESS_KEY
    assert MINIO_SECRET_KEY not in resp.text

    creds = body["storage-credentials"]
    assert len(creds) == 1
    assert creds[0]["prefix"].startswith(f"s3://{bucket}/warehouse/{env.ns}/events-")
    assert creds[0]["config"]["s3.session-token"] == config["s3.session-token"]
    env.recorder.assert_clean()


def test_credentials_endpoint_and_prefix_isolation(env, bucket, run_id):
    """`loadCredentials` returns working credentials for its table that
    cannot touch a sibling table's prefix."""
    env.catalog.create_table(f"{env.ns}.other", schema=ICEBERG_SCHEMA)

    resp = requests.get(
        f"{env.base_url}/iceberg/v1/{env.warehouse}/namespaces/{env.ns}/tables/events/credentials",
        timeout=30,
    )
    assert resp.status_code == 200, f"credentials: {resp.status_code} {resp.text}"
    creds = resp.json()["storage-credentials"][0]
    prefix = creds["prefix"]
    cfg = creds["config"]

    vended = boto3.client(
        "s3",
        endpoint_url=cfg.get("s3.endpoint", MINIO_ENDPOINT),
        aws_access_key_id=cfg["s3.access-key-id"],
        aws_secret_access_key=cfg["s3.secret-access-key"],
        aws_session_token=cfg["s3.session-token"],
        region_name="us-east-1",
        config=Config(s3={"addressing_style": "path"}, connect_timeout=5, retries={"max_attempts": 1}),
    )

    # CAN list/read its own table prefix (metadata written at create time).
    own_key_prefix = prefix.replace(f"s3://{bucket}/", "")
    listed = vended.list_objects_v2(Bucket=bucket, Prefix=own_key_prefix)
    assert listed["KeyCount"] > 0, "vended creds must see their own table"

    # CANNOT touch the sibling table's prefix.
    other_meta = env.catalog.load_table(f"{env.ns}.other").metadata_location
    other_key = other_meta.replace(f"s3://{bucket}/", "")
    with pytest.raises(Exception) as denied:
        vended.get_object(Bucket=bucket, Key=other_key)
    assert any(marker in str(denied.value) for marker in ("403", "AccessDenied", "Forbidden")), (
        f"expected denial reading sibling table, got: {denied.value}"
    )
    env.recorder.assert_clean()
