"""Two pyiceberg catalog instances append concurrently to one table.

Each worker owns its own RestCatalog instance and performs 5 appends of
100 rows. Success bar: 10 snapshots and 1000 rows at the end, and no
CommitFailedException surfaces to the user code's final result. pyiceberg
does NOT internally retry commit conflicts (a 409 from the catalog raises
CommitFailedException to the caller), so each worker uses an explicit
reload-and-retry loop; how many retries were needed is recorded as a
warning for the report.
"""

import os
import time
import warnings
from concurrent.futures import ThreadPoolExecutor

import pytest
from pyiceberg.exceptions import CommitFailedException

from conftest import ServerErrorRecorder, create_warehouse
from lifecycle import ICEBERG_SCHEMA, make_batch, make_catalog

APPENDS_PER_WRITER = 5
BATCH = 100
MAX_ATTEMPTS = 20


def writer(base_url, warehouse, ident, writer_idx, recorder):
    """5 appends with reload-on-conflict retries; returns conflict count."""
    catalog = make_catalog(base_url, warehouse)
    recorder.attach(catalog)
    conflicts = 0
    for k in range(APPENDS_PER_WRITER):
        start = (writer_idx * APPENDS_PER_WRITER + k) * BATCH
        batch = make_batch(start, BATCH)
        for attempt in range(MAX_ATTEMPTS):
            table = catalog.load_table(ident)  # fresh metadata each attempt
            try:
                table.append(batch)
                break
            except CommitFailedException:
                conflicts += 1
                time.sleep(0.05 * (attempt + 1))
        else:
            raise AssertionError(
                f"writer {writer_idx}: append {k} still conflicting after "
                f"{MAX_ATTEMPTS} attempts"
            )
    return conflicts


def test_concurrent_appends(base_url, run_id):
    warehouse = f"e2e_conc_{run_id}"
    root = f"/tmp/meridian-e2e/{run_id}/conc"
    os.makedirs(root, exist_ok=True)
    create_warehouse(base_url, warehouse, f"file://{root}", {})

    recorder = ServerErrorRecorder()
    setup_catalog = make_catalog(base_url, warehouse)
    recorder.attach(setup_catalog)
    ns = f"conc_{run_id}"
    setup_catalog.create_namespace(ns)
    ident = f"{ns}.hot_table"
    setup_catalog.create_table(ident, schema=ICEBERG_SCHEMA)

    with ThreadPoolExecutor(max_workers=2) as pool:
        futures = [
            pool.submit(writer, base_url, warehouse, ident, idx, recorder)
            for idx in range(2)
        ]
        conflict_counts = [f.result(timeout=300) for f in futures]

    table = setup_catalog.load_table(ident)
    snapshots = table.snapshots()
    assert len(snapshots) == 2 * APPENDS_PER_WRITER, (
        f"expected {2 * APPENDS_PER_WRITER} snapshots, got {len(snapshots)}"
    )

    result = table.scan().to_arrow()
    expected_rows = 2 * APPENDS_PER_WRITER * BATCH
    assert result.num_rows == expected_rows
    ids = sorted(result.column("id").to_pylist())
    assert ids == list(range(expected_rows)), "row set is not exactly the union of all appends"

    recorder.assert_clean()
    total_conflicts = sum(conflict_counts)
    warnings.warn(
        f"NOTE: pyiceberg does not auto-retry commit conflicts; workers hit "
        f"{total_conflicts} CommitFailedException(s) total "
        f"({conflict_counts}) and succeeded via explicit reload-and-retry.",
        stacklevel=1,
    )
