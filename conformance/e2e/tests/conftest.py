"""Shared fixtures for the Meridian e2e suite.

Every run gets a unique E2E_RUN_ID (set by run.sh, or generated here), so
warehouse names, namespaces, buckets, and /tmp paths never collide with a
previous run.
"""

import os
import time
import uuid

import pytest
import requests

BASE_URL = os.environ.get("MERIDIAN_URL", "http://localhost:8181")
RUN_ID = os.environ.get("E2E_RUN_ID") or f"{int(time.time())}{uuid.uuid4().hex[:6]}"

MINIO_ENDPOINT = os.environ.get("MINIO_ENDPOINT", "http://localhost:9000")
MINIO_ACCESS_KEY = "meridian"
MINIO_SECRET_KEY = "meridian123"


@pytest.fixture(scope="session")
def base_url() -> str:
    resp = requests.get(f"{BASE_URL}/healthz", timeout=5)
    assert resp.ok, f"Meridian server not healthy at {BASE_URL}: {resp.status_code}"
    return BASE_URL


@pytest.fixture(scope="session")
def run_id() -> str:
    return RUN_ID


def create_warehouse(base_url: str, name: str, storage_root: str, storage_options: dict) -> dict:
    """Create a warehouse via the management API; any non-2xx fails."""
    resp = requests.post(
        f"{base_url}/api/v2/warehouses",
        json={"name": name, "storage_root": storage_root, "storage_options": storage_options},
        timeout=10,
    )
    assert resp.status_code < 300, (
        f"warehouse create failed: {resp.status_code} {resp.text}"
    )
    return resp.json()


class ServerErrorRecorder:
    """Records every 5xx response the pyiceberg HTTP session receives."""

    def __init__(self) -> None:
        self.errors: list[str] = []

    def hook(self, response, **kwargs):  # requests response hook signature
        if response.status_code >= 500:
            body = response.text[:500]
            self.errors.append(
                f"{response.request.method} {response.request.url} -> "
                f"{response.status_code}: {body}"
            )
        return response

    def attach(self, catalog) -> None:
        """Attach to a pyiceberg RestCatalog's underlying requests.Session."""
        catalog._session.hooks["response"].append(self.hook)

    def assert_clean(self) -> None:
        assert not self.errors, "server returned 5xx during test:\n" + "\n".join(self.errors)
