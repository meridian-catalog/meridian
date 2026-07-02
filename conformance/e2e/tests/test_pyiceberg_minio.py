"""pyiceberg full lifecycle against Meridian with an s3:// (MinIO) warehouse.

Identical scenario to test_pyiceberg_fs.py, on object storage. The s3.*
client properties are configured client-side because Meridian's
LoadTableResult `config` is always empty (verified and recorded by
test_server_does_not_vend_storage_config below).
"""

import warnings
from types import SimpleNamespace

import boto3
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
from lifecycle import ICEBERG_SCHEMA, LIFECYCLE_STEPS, make_catalog

S3_CLIENT_PROPS = {
    "s3.endpoint": MINIO_ENDPOINT,
    "s3.access-key-id": MINIO_ACCESS_KEY,
    "s3.secret-access-key": MINIO_SECRET_KEY,
    "s3.path-style-access": "true",
    "s3.region": "us-east-1",
}


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
    name = f"e2e-{run_id}"
    try:
        client.create_bucket(Bucket=name)
    except Exception as exc:  # connection refused, etc.
        pytest.skip(f"MinIO not reachable at {MINIO_ENDPOINT}: {exc}")
    return name


@pytest.fixture(scope="module")
def env(base_url, run_id, bucket):
    warehouse = f"e2e_s3_{run_id}"
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

    return SimpleNamespace(
        base_url=base_url,
        warehouse=warehouse,
        catalog=catalog,
        ns=f"ns_s3_{run_id}",
        table=None,
        first_snapshot_id=None,
        recorder=recorder,
    )


@pytest.mark.parametrize("step", LIFECYCLE_STEPS, ids=lambda s: s.__name__)
def test_lifecycle(env, step):
    step(env)
    env.recorder.assert_clean()


def test_server_does_not_vend_storage_config(env, run_id):
    """Checks whether LoadTableResult.config passes the warehouse's
    storage_options through to clients. If it does not, the suite still
    passes (clients configured s3.* locally) but the gap is recorded."""
    ns = f"cfgprobe_{run_id}"
    env.catalog.create_namespace(ns)
    env.catalog.create_table(f"{ns}.probe", schema=ICEBERG_SCHEMA)

    resp = requests.get(
        f"{env.base_url}/iceberg/v1/{env.warehouse}/namespaces/{ns}/tables/probe",
        timeout=10,
    )
    assert resp.status_code == 200, f"load_table: {resp.status_code} {resp.text}"
    config = resp.json().get("config", {})
    if not any(key.startswith("s3.") for key in config):
        warnings.warn(
            "GAP: LoadTableResult.config does not vend s3.* storage settings "
            f"(got {config!r}); clients must configure s3 endpoint/credentials "
            "themselves.",
            stacklevel=1,
        )
    env.catalog.drop_table(f"{ns}.probe")
    env.recorder.assert_clean()
