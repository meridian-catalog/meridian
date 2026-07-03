"""pyiceberg against Meridian with REMOTE SIGNING (MinIO).

The client holds ZERO S3 credentials and asks for the `remote-signing`
delegation instead of vended credentials. Meridian advertises its
per-table sign endpoint in `LoadTableResult.config`
(`s3.remote-signing-enabled`, `s3.signer.endpoint`, `s3.signer`), and
pyiceberg's **fsspec** FileIO (`S3V4RestSigner`) then routes every object
request through `POST .../tables/{table}/sign` — the catalog signs with
warehouse credentials it never ships.

The fsspec FileIO is required: pyiceberg's default pyarrow FileIO has no
remote-signing support (as of 0.11.x), so this module pins
`py-io-impl = pyiceberg.io.fsspec.FsspecFileIO`.
"""

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
from lifecycle import ICEBERG_SCHEMA, make_batch, make_catalog


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
    name = f"e2e-sign-{run_id}"
    try:
        client.create_bucket(Bucket=name)
    except Exception as exc:  # connection refused, etc.
        pytest.skip(f"MinIO not reachable at {MINIO_ENDPOINT}: {exc}")
    return name


@pytest.fixture(scope="module")
def env(base_url, run_id, bucket):
    warehouse = f"e2e_sign_{run_id}"
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
            # Remote signing rides the vending opt-in; static is enough
            # (nothing is ever vended in this module).
            "vending": "static",
        },
    )

    recorder = ServerErrorRecorder()
    # No s3 keys anywhere. The delegation header replaces pyiceberg's
    # default (`vended-credentials`); the fsspec FileIO is the one that
    # implements the signer protocol.
    catalog = make_catalog(
        base_url,
        warehouse,
        extra_props={
            "header.X-Iceberg-Access-Delegation": "remote-signing",
            "py-io-impl": "pyiceberg.io.fsspec.FsspecFileIO",
        },
    )
    recorder.attach(catalog)

    return SimpleNamespace(
        base_url=base_url,
        warehouse=warehouse,
        catalog=catalog,
        ns=f"ns_sign_{run_id}",
        recorder=recorder,
    )


def test_write_and_read_through_remote_signing_only(env):
    """Full round trip — create, append, scan — every S3 request signed by
    the catalog, zero client-side keys."""
    env.catalog.create_namespace(env.ns)
    table = env.catalog.create_table(f"{env.ns}.events", schema=ICEBERG_SCHEMA)
    table.append(make_batch(0, 25))

    fresh = env.catalog.load_table(f"{env.ns}.events")
    result = fresh.scan().to_arrow()
    assert result.num_rows == 25
    assert sorted(result["id"].to_pylist()) == list(range(25))
    env.recorder.assert_clean()


def test_load_table_advertises_signer_but_no_credentials(env):
    """The raw LoadTableResult under `remote-signing`: the signer switch
    and per-table endpoint, and not a single credential-shaped key."""
    resp = requests.get(
        f"{env.base_url}/iceberg/v1/{env.warehouse}/namespaces/{env.ns}/tables/events",
        headers={"X-Iceberg-Access-Delegation": "remote-signing"},
        timeout=30,
    )
    assert resp.status_code == 200, f"load: {resp.status_code} {resp.text}"
    body = resp.json()
    config = body["config"]
    assert config["s3.remote-signing-enabled"] == "true"
    assert config["s3.signer"] == "S3V4RestSigner"
    assert (
        config["s3.signer.endpoint"]
        == f"v1/{env.warehouse}/namespaces/{env.ns}/tables/events/sign"
    )
    assert "storage-credentials" not in body
    for key in sorted(config):
        assert "access-key" not in key and "secret" not in key and "token" not in key, (
            f"credential-shaped key {key!r} under remote-signing"
        )
    assert MINIO_SECRET_KEY not in resp.text
    env.recorder.assert_clean()


def test_sign_endpoint_refuses_objects_outside_the_table(env, bucket):
    """The signer is not an oracle: a foreign object under the same bucket
    is a 403, straight from the raw endpoint."""
    resp = requests.post(
        f"{env.base_url}/iceberg/v1/{env.warehouse}/namespaces/{env.ns}/tables/events/sign",
        json={
            "region": "us-east-1",
            "method": "GET",
            "uri": f"{MINIO_ENDPOINT}/{bucket}/warehouse/{env.ns}/OTHER-table/secret.parquet",
            "headers": {},
        },
        timeout=30,
    )
    assert resp.status_code == 403, f"sign: {resp.status_code} {resp.text}"
    assert resp.json()["error"]["type"] == "ForbiddenException"
