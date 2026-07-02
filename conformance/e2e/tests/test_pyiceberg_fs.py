"""pyiceberg full lifecycle against Meridian with a file:// warehouse.

Any 5xx the server returns to pyiceberg fails the step that triggered it
(see ServerErrorRecorder in conftest).
"""

import os
from types import SimpleNamespace

import pytest

from conftest import ServerErrorRecorder, create_warehouse
from lifecycle import LIFECYCLE_STEPS, make_catalog


@pytest.fixture(scope="module")
def env(base_url, run_id):
    warehouse = f"e2e_fs_{run_id}"
    root = f"/tmp/meridian-e2e/{run_id}/fs"
    os.makedirs(root, exist_ok=True)
    create_warehouse(base_url, warehouse, f"file://{root}", {})

    recorder = ServerErrorRecorder()
    catalog = make_catalog(base_url, warehouse)
    recorder.attach(catalog)

    return SimpleNamespace(
        base_url=base_url,
        warehouse=warehouse,
        catalog=catalog,
        ns=f"ns_{run_id}",
        table=None,
        first_snapshot_id=None,
        recorder=recorder,
    )


@pytest.mark.parametrize("step", LIFECYCLE_STEPS, ids=lambda s: s.__name__)
def test_lifecycle(env, step):
    step(env)
    env.recorder.assert_clean()
