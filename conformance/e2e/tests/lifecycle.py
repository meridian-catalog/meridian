"""The engine-agnostic pyiceberg lifecycle, shared by the file:// and
MinIO test modules so both storage backends run the identical scenario.

Each `step_*` function is one lifecycle stage; `LIFECYCLE_STEPS` is the
ordered list a test module parametrizes over. State flows through the
module-scoped `env` namespace (catalog, table, first_snapshot_id, ...).
"""

from datetime import datetime, timedelta

import pyarrow as pa
import requests
from pyiceberg.catalog import Catalog, load_catalog
from pyiceberg.schema import Schema
from pyiceberg.types import (
    BooleanType,
    DoubleType,
    IntegerType,
    NestedField,
    StringType,
    TimestampType,
)

EPOCH = datetime(2026, 1, 1)

ICEBERG_SCHEMA = Schema(
    NestedField(1, "id", IntegerType(), required=False),
    NestedField(2, "name", StringType(), required=False),
    NestedField(3, "value", DoubleType(), required=False),
    NestedField(4, "created_at", TimestampType(), required=False),
    NestedField(5, "active", BooleanType(), required=False),
)

ARROW_SCHEMA = pa.schema(
    [
        pa.field("id", pa.int32()),
        pa.field("name", pa.string()),
        pa.field("value", pa.float64()),
        pa.field("created_at", pa.timestamp("us")),
        pa.field("active", pa.bool_()),
    ]
)


def make_catalog(base_url: str, warehouse: str, extra_props: dict | None = None) -> Catalog:
    """A REST catalog against Meridian's /iceberg mount (pyiceberg appends /v1)."""
    props = {
        "type": "rest",
        "uri": f"{base_url}/iceberg",
        "warehouse": warehouse,
    }
    if extra_props:
        props.update(extra_props)
    return load_catalog(f"meridian-{warehouse}", **props)


def make_batch(start: int, count: int, extra: str | None = None) -> pa.Table:
    """`count` deterministic rows with ids [start, start+count)."""
    ids = list(range(start, start + count))
    columns = {
        "id": pa.array(ids, pa.int32()),
        "name": pa.array([f"row-{i}" for i in ids], pa.string()),
        "value": pa.array([i * 1.5 for i in ids], pa.float64()),
        "created_at": pa.array(
            [EPOCH + timedelta(seconds=i) for i in ids], pa.timestamp("us")
        ),
        "active": pa.array([i % 2 == 0 for i in ids], pa.bool_()),
    }
    schema = ARROW_SCHEMA
    if extra is not None:
        columns["extra"] = pa.array([extra] * count, pa.string())
        schema = ARROW_SCHEMA.append(pa.field("extra", pa.string()))
    return pa.table(columns, schema=schema)


def spot_check(arrow_table: pa.Table, expected_ids: range) -> None:
    """Row-count plus content checks: exact id set, name join, value math."""
    assert arrow_table.num_rows == len(expected_ids)
    data = arrow_table.sort_by("id").to_pylist()
    assert [r["id"] for r in data] == list(expected_ids)
    first = data[0]
    assert first["name"] == f"row-{first['id']}"
    assert first["value"] == first["id"] * 1.5
    assert first["created_at"] == EPOCH + timedelta(seconds=first["id"])
    assert first["active"] == (first["id"] % 2 == 0)
    last = data[-1]
    assert last["name"] == f"row-{last['id']}"
    assert last["value"] == last["id"] * 1.5


# -- Lifecycle steps ---------------------------------------------------------


def step_config_endpoint(env):
    """GET /v1/config works on both mounts and resolves the warehouse."""
    for mount in ("/iceberg/v1", "/v1"):
        resp = requests.get(
            f"{env.base_url}{mount}/config",
            params={"warehouse": env.warehouse},
            timeout=10,
        )
        assert resp.status_code == 200, f"{mount}/config: {resp.status_code} {resp.text}"
        body = resp.json()
        assert body["overrides"].get("prefix") == env.warehouse, body


def step_create_namespace(env):
    env.catalog.create_namespace(env.ns)
    assert (env.ns,) in env.catalog.list_namespaces()


def step_create_table(env):
    env.table = env.catalog.create_table(f"{env.ns}.events", schema=ICEBERG_SCHEMA)
    assert [f.name for f in env.table.schema().fields] == [
        "id",
        "name",
        "value",
        "created_at",
        "active",
    ]


def step_append_first_500(env):
    env.table.append(make_batch(0, 500))
    spot_check(env.table.scan().to_arrow(), range(500))


def step_append_second_500(env):
    env.table.append(make_batch(500, 500))
    env.table = env.catalog.load_table(f"{env.ns}.events")
    snapshots = env.table.snapshots()
    assert len(snapshots) == 2, f"expected 2 snapshots, got {len(snapshots)}"
    env.first_snapshot_id = snapshots[0].snapshot_id
    spot_check(env.table.scan().to_arrow(), range(1000))


def step_schema_evolution(env):
    from pyiceberg.types import StringType

    with env.table.update_schema() as update:
        update.add_column("extra", StringType())
    env.table.append(make_batch(1000, 100, extra="evolved"))
    result = env.table.scan().to_arrow().sort_by("id")
    assert result.num_rows == 1100
    extra = result.column("extra").to_pylist()
    ids = result.column("id").to_pylist()
    for row_id, value in zip(ids, extra):
        expected = "evolved" if row_id >= 1000 else None
        assert value == expected, f"id={row_id}: extra={value!r}, want {expected!r}"


def step_time_travel(env):
    assert env.first_snapshot_id is not None
    old = env.table.scan(snapshot_id=env.first_snapshot_id).to_arrow()
    spot_check(old, range(500))


def step_list_sanity(env):
    assert (env.ns, "events") in env.catalog.list_tables(env.ns)
    assert (env.ns,) in env.catalog.list_namespaces()


def step_rename_table(env):
    from pyiceberg.exceptions import NoSuchTableError

    env.catalog.rename_table(f"{env.ns}.events", f"{env.ns}.events_renamed")
    renamed = env.catalog.load_table(f"{env.ns}.events_renamed")
    assert renamed.scan().to_arrow().num_rows == 1100
    try:
        env.catalog.load_table(f"{env.ns}.events")
    except NoSuchTableError:
        pass
    else:
        raise AssertionError("old table name still loadable after rename")
    env.table = renamed


def step_drop_table(env):
    env.catalog.drop_table(f"{env.ns}.events_renamed")
    assert not env.catalog.table_exists(f"{env.ns}.events_renamed")
    assert (env.ns, "events_renamed") not in env.catalog.list_tables(env.ns)


LIFECYCLE_STEPS = [
    step_config_endpoint,
    step_create_namespace,
    step_create_table,
    step_append_first_500,
    step_append_second_500,
    step_schema_evolution,
    step_time_travel,
    step_list_sanity,
    step_rename_table,
    step_drop_table,
]
