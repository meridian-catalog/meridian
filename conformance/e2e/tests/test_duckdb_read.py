"""DuckDB reads a Meridian-committed table written by pyiceberg.

Pass bar: DuckDB reads the table's data correctly at all. Attaching the
REST catalog directly is the stretch goal; if the installed duckdb-iceberg
extension cannot attach an auth-less REST catalog, the test falls back to
iceberg_scan() on the metadata.json Meridian committed, and records which
path worked as a warning.
"""

import os
import warnings
from types import SimpleNamespace

import duckdb
import pytest

from conftest import ServerErrorRecorder, create_warehouse
from lifecycle import ICEBERG_SCHEMA, make_batch, make_catalog

ROWS = 1000
EXPECTED_ID_SUM = sum(range(ROWS))  # 499500
EXPECTED_VALUE_SUM = EXPECTED_ID_SUM * 1.5


@pytest.fixture(scope="module")
def seeded(base_url, run_id):
    """A file://-backed table with 1000 rows, written through Meridian."""
    warehouse = f"e2e_duck_{run_id}"
    root = f"/tmp/meridian-e2e/{run_id}/duck"
    os.makedirs(root, exist_ok=True)
    create_warehouse(base_url, warehouse, f"file://{root}", {})

    recorder = ServerErrorRecorder()
    catalog = make_catalog(base_url, warehouse)
    recorder.attach(catalog)

    ns = f"duck_{run_id}"
    catalog.create_namespace(ns)
    table = catalog.create_table(f"{ns}.readings", schema=ICEBERG_SCHEMA)
    table.append(make_batch(0, 500))
    table.append(make_batch(500, 500))
    recorder.assert_clean()

    return SimpleNamespace(
        base_url=base_url,
        warehouse=warehouse,
        ns=ns,
        metadata_location=table.metadata_location,
    )


def try_rest_attach(con, seeded) -> str | None:
    """Attempts REST-catalog ATTACH variants; returns the working SQL or None."""
    attempts = [
        # Auth-less attach; duckdb-iceberg appends /v1/... to ENDPOINT.
        f"ATTACH '{seeded.warehouse}' AS mrd (TYPE iceberg, "
        f"ENDPOINT '{seeded.base_url}/iceberg', AUTHORIZATION_TYPE 'none')",
        # Some versions want the /v1 included.
        f"ATTACH '{seeded.warehouse}' AS mrd (TYPE iceberg, "
        f"ENDPOINT '{seeded.base_url}/iceberg/v1', AUTHORIZATION_TYPE 'none')",
        # Oldest syntax: no AUTHORIZATION_TYPE option.
        f"ATTACH '{seeded.warehouse}' AS mrd (TYPE iceberg, "
        f"ENDPOINT '{seeded.base_url}/iceberg')",
    ]
    errors = []
    for sql in attempts:
        try:
            con.execute(sql)
            return sql
        except Exception as exc:  # noqa: BLE001 - collect and report every variant
            errors.append(f"{sql}\n  -> {type(exc).__name__}: {exc}")
            try:
                con.execute("DETACH mrd")
            except Exception:
                pass
    warnings.warn(
        "REST ATTACH did not work with any tried syntax:\n" + "\n".join(errors),
        stacklevel=1,
    )
    return None


def check_counts(con, source_sql: str) -> None:
    count, id_sum, value_sum = con.execute(
        f"SELECT count(*), sum(id), sum(value) FROM {source_sql}"
    ).fetchone()
    assert count == ROWS, f"count: {count} != {ROWS}"
    assert id_sum == EXPECTED_ID_SUM, f"sum(id): {id_sum} != {EXPECTED_ID_SUM}"
    assert value_sum == pytest.approx(EXPECTED_VALUE_SUM), (
        f"sum(value): {value_sum} != {EXPECTED_VALUE_SUM}"
    )


def test_duckdb_reads_meridian_table(seeded):
    con = duckdb.connect()
    con.execute("INSTALL iceberg")
    con.execute("LOAD iceberg")

    attach_sql = try_rest_attach(con, seeded)
    if attach_sql is not None:
        check_counts(con, f'mrd."{seeded.ns}".readings')
        warnings.warn(f"PATH: REST ATTACH worked: {attach_sql}", stacklevel=1)
        return

    # Fallback: read the metadata.json Meridian committed, directly.
    path = seeded.metadata_location.removeprefix("file://")
    assert os.path.exists(path), f"committed metadata file missing: {path}"
    check_counts(con, f"iceberg_scan('{path}')")
    warnings.warn(
        "PATH: REST ATTACH unavailable; iceberg_scan() on Meridian-committed "
        "metadata.json worked.",
        stacklevel=1,
    )
