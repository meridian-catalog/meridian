"""View lifecycle against Meridian: raw REST + the pyiceberg view surface.

pyiceberg's RestCatalog (0.11.x, the current release) implements only
`list_views`, `view_exists`, and `drop_view` for REST views — there is no
`create_view`/`load_view` yet. So this module drives create/load/replace/
rename through raw requests (the wire shapes from the IRC OpenAPI spec) and
uses pyiceberg for the operations it does support, so the parts a real
client library exercises are covered by a real client library.

Uses a file:///tmp warehouse: none of the view endpoints require object
storage beyond what the server itself writes.
"""

from types import SimpleNamespace

import pytest
import requests

from conftest import ServerErrorRecorder, create_warehouse
from lifecycle import ICEBERG_SCHEMA, make_catalog

TIMEOUT = 10

SCHEMA = {
    "type": "struct",
    "fields": [
        {"id": 1, "name": "id", "required": True, "type": "long"},
        {"id": 2, "name": "name", "required": False, "type": "string"},
    ],
}


def view_version(ns: str, dialects: dict[str, str], version_id: int = 1) -> dict:
    return {
        "version-id": version_id,
        "timestamp-ms": 1_700_000_000_000,
        "schema-id": 0,
        "summary": {"engine-name": "meridian-e2e"},
        "representations": [
            {"type": "sql", "sql": sql, "dialect": dialect}
            for dialect, sql in dialects.items()
        ],
        "default-namespace": [ns],
    }


@pytest.fixture(scope="module")
def env(base_url, run_id, tmp_path_factory):
    warehouse = f"e2e_views_{run_id}"
    root = tmp_path_factory.mktemp("views-warehouse")
    create_warehouse(base_url, warehouse, f"file://{root}", {})

    recorder = ServerErrorRecorder()
    catalog = make_catalog(base_url, warehouse)
    recorder.attach(catalog)

    ns = f"ns_views_{run_id}"
    catalog.create_namespace(ns)

    return SimpleNamespace(
        base_url=base_url,
        warehouse=warehouse,
        catalog=catalog,
        ns=ns,
        recorder=recorder,
        prefix=f"{base_url}/iceberg/v1/{warehouse}",
        view_uuid=None,
    )


def test_create_view_with_two_dialects(env):
    resp = requests.post(
        f"{env.prefix}/namespaces/{env.ns}/views",
        json={
            "name": "agg",
            "schema": SCHEMA,
            "view-version": view_version(
                env.ns,
                {
                    "spark": f"SELECT id, name FROM {env.ns}.events",
                    "trino": f'SELECT id, name FROM "{env.ns}".events',
                },
            ),
            "properties": {"comment": "e2e view"},
        },
        timeout=TIMEOUT,
    )
    assert resp.status_code == 200, f"createView: {resp.status_code} {resp.text}"
    body = resp.json()
    metadata = body["metadata"]
    assert metadata["format-version"] == 1
    assert metadata["current-version-id"] == 1
    dialects = [r["dialect"] for r in metadata["versions"][0]["representations"]]
    assert dialects == ["spark", "trino"]
    assert body["metadata-location"].endswith(".metadata.json")
    env.view_uuid = metadata["view-uuid"]
    env.recorder.assert_clean()


def test_pyiceberg_sees_the_view(env):
    """pyiceberg's REST view support: list_views + view_exists."""
    assert (env.ns, "agg") in env.catalog.list_views(env.ns)
    assert env.catalog.view_exists(f"{env.ns}.agg")
    assert not env.catalog.view_exists(f"{env.ns}.ghost")
    env.recorder.assert_clean()


def test_load_and_replace_grows_version_log(env):
    resp = requests.get(f"{env.prefix}/namespaces/{env.ns}/views/agg", timeout=TIMEOUT)
    assert resp.status_code == 200, resp.text
    before = resp.json()
    assert before["metadata"]["view-uuid"] == env.view_uuid
    log_before = len(before["metadata"]["version-log"])

    resp = requests.post(
        f"{env.prefix}/namespaces/{env.ns}/views/agg",
        json={
            "requirements": [{"type": "assert-view-uuid", "uuid": env.view_uuid}],
            "updates": [
                {"action": "add-schema", "schema": SCHEMA},
                {
                    "action": "add-view-version",
                    "view-version": view_version(
                        env.ns,
                        {"spark": f"SELECT id FROM {env.ns}.events"},
                        version_id=0,
                    ),
                },
                {"action": "set-current-view-version", "view-version-id": -1},
                {"action": "set-properties", "updates": {"replaced": "yes"}},
            ],
        },
        timeout=TIMEOUT,
    )
    assert resp.status_code == 200, f"replaceView: {resp.status_code} {resp.text}"
    after = resp.json()["metadata"]
    assert after["current-version-id"] == 2
    assert len(after["versions"]) == 2
    assert len(after["version-log"]) == log_before + 1
    assert after["properties"]["replaced"] == "yes"

    # A stale view-uuid assertion must be a 409 CommitFailedException.
    resp = requests.post(
        f"{env.prefix}/namespaces/{env.ns}/views/agg",
        json={
            "requirements": [
                {
                    "type": "assert-view-uuid",
                    "uuid": "00000000-0000-4000-8000-000000000000",
                }
            ],
            "updates": [],
        },
        timeout=TIMEOUT,
    )
    assert resp.status_code == 409, resp.text
    assert resp.json()["error"]["type"] == "CommitFailedException"
    env.recorder.assert_clean()


def test_rename_and_name_collisions(env):
    # A table now occupies a name; a view must not be able to take it.
    env.catalog.create_table(f"{env.ns}.occupied", schema=ICEBERG_SCHEMA)
    resp = requests.post(
        f"{env.base_url}/iceberg/v1/{env.warehouse}/views/rename",
        json={
            "source": {"namespace": [env.ns], "name": "agg"},
            "destination": {"namespace": [env.ns], "name": "occupied"},
        },
        timeout=TIMEOUT,
    )
    assert resp.status_code == 409, resp.text

    resp = requests.post(
        f"{env.base_url}/iceberg/v1/{env.warehouse}/views/rename",
        json={
            "source": {"namespace": [env.ns], "name": "agg"},
            "destination": {"namespace": [env.ns], "name": "agg_renamed"},
        },
        timeout=TIMEOUT,
    )
    assert resp.status_code == 204, resp.text
    assert env.catalog.view_exists(f"{env.ns}.agg_renamed")
    assert not env.catalog.view_exists(f"{env.ns}.agg")
    env.recorder.assert_clean()


def test_pyiceberg_drop_view(env):
    env.catalog.drop_view(f"{env.ns}.agg_renamed")
    assert not env.catalog.view_exists(f"{env.ns}.agg_renamed")
    assert (env.ns, "agg_renamed") not in env.catalog.list_views(env.ns)

    resp = requests.get(
        f"{env.prefix}/namespaces/{env.ns}/views/agg_renamed", timeout=TIMEOUT
    )
    assert resp.status_code == 404
    assert resp.json()["error"]["type"] == "NoSuchViewException"
    env.recorder.assert_clean()
