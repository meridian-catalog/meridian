# Trino smoke suite against a Meridian REST catalog, with cross-engine
# verification of the table the Spark smoke (../spark) left behind.
#
# Runs on the HOST (python3, stdlib only) and drives the Trino CLI
# inside the meridian-trino-smoke container via `docker exec`; see
# run.sh. Every step is verified with an assertion; failures are
# collected and the script exits non-zero if any step failed. The last
# line of output is a single JSON object:
#   {status, cross_engine_match, details, failures: [...]}
#
# Expected cross-engine numbers are derived from the Spark suite's
# deterministic dataset (ids 0..1009, minus DELETEd 950..999):
#   amount = (id % 100) * 1.5, quantity = (id % 10) + 1,
#   category = 'cat_' || (id % 4),
#   note = 'merged' for id < 100 (MERGE), 'late' for id >= 1000, else NULL.

import json
import subprocess
import sys
import time
import traceback
import urllib.error
import urllib.request

CONTAINER = "meridian-trino-smoke"
MERIDIAN_URL = "http://localhost:8181"
CATALOG = "mrd"
WAREHOUSE = "spark_smoke"
NS = "trino_ns"
TABLE = f"{CATALOG}.{NS}.items"
VIEW = f"{CATALOG}.{NS}.items_by_category"
SPARK_NS = "spark_ns"
SPARK_TABLE = f"{CATALOG}.{SPARK_NS}.orders"
SPARK_VIEW = f"{CATALOG}.{SPARK_NS}.orders_by_category"

# Final state of spark_ns.orders as reported by the Spark suite
# (SUITE_RESULT: row_count 960, snapshot_count 5) and derived from its
# dataset definition:
SPARK_EXPECT = {
    "row_count": 960,
    "sum_amount": 68730.0,
    "sum_quantity": 5280,
    "per_category": {"cat_0": 241, "cat_1": 241, "cat_2": 239, "cat_3": 239},
    "merged_rows": 100,   # MERGE INTO set note='merged' on id < 100
    "late_rows": 10,      # post-evolution insert, ids 1000..1009
    "null_note_rows": 850,
    "deleted_range_rows": 0,  # DELETE FROM removed 950 <= id < 1000
    "snapshot_count": 5,
}

failures = []
details = {}
result = {
    "status": "fail",
    "cross_engine_match": False,
    "details": details,
    "failures": failures,
}


def step(name):
    def deco(fn):
        def wrapper(*args, **kwargs):
            print(f"=== step: {name} ===", flush=True)
            try:
                fn(*args, **kwargs)
                print(f"--- ok: {name}", flush=True)
                return True
            except Exception as exc:
                traceback.print_exc()
                failures.append(f"{name}: {exc}")
                print(f"--- FAIL: {name}: {exc}", flush=True)
                return False
        return wrapper
    return deco


def check(cond, msg):
    if not cond:
        raise AssertionError(msg)


class TrinoError(Exception):
    pass


def trino(sql):
    """Run one statement through the Trino CLI; return rows as lists of strings."""
    proc = subprocess.run(
        ["docker", "exec", CONTAINER, "trino",
         "--output-format", "CSV_UNQUOTED", "--execute", sql],
        capture_output=True, text=True, timeout=300,
    )
    if proc.returncode != 0:
        raise TrinoError(proc.stderr.strip().splitlines()[-1] if proc.stderr.strip() else
                         f"trino CLI exited {proc.returncode}")
    return [line.split(",") for line in proc.stdout.splitlines() if line != ""]


def one(sql):
    rows = trino(sql)
    check(len(rows) == 1 and len(rows[0]) == 1, f"expected a single value from {sql!r}, got {rows}")
    return rows[0][0]


def rest_get(path):
    with urllib.request.urlopen(f"{MERIDIAN_URL}{path}", timeout=10) as resp:
        return json.loads(resp.read())


def rest_status(path):
    req = urllib.request.Request(f"{MERIDIAN_URL}{path}", method="GET")
    try:
        with urllib.request.urlopen(req, timeout=10) as resp:
            return resp.status
    except urllib.error.HTTPError as exc:
        return exc.code


def wait_ready(timeout_s=240):
    """Trino takes a while to boot; poll until a trivial query succeeds."""
    deadline = time.time() + timeout_s
    last = None
    while time.time() < deadline:
        try:
            if one("SELECT 1") == "1":
                return
        except Exception as exc:  # container starting / server booting
            last = exc
        time.sleep(3)
    raise RuntimeError(f"Trino not ready after {timeout_s}s: {last}")


@step("wait for Trino + SHOW SCHEMAS sees spark_ns")
def show_schemas():
    wait_ready()
    schemas = [r[0] for r in trino(f"SHOW SCHEMAS FROM {CATALOG}")]
    check(SPARK_NS in schemas,
          f"{SPARK_NS} (created by the Spark smoke) missing from SHOW SCHEMAS: {schemas}")
    details["schemas_seen"] = schemas


@step("CREATE SCHEMA trino_ns")
def create_schema():
    # The explicit location matters: without a 'location' property on the
    # namespace, Trino cannot compute a default table location and falls
    # back to a DIRECT (non-staged) REST create inside
    # newCreateTableTransaction (an S3-Tables workaround in Trino). Meridian
    # then writes the first metadata file, and Trino's subsequent
    # "location must be empty" check trips over the file the create itself
    # just wrote — failing the query but leaving the table registered.
    # With a schema location, Trino derives the table location client-side
    # and uses the proper stage-create + assert-create commit path.
    # See README.md ("Schema location and Trino's staged creates").
    trino(f"CREATE SCHEMA {CATALOG}.{NS} WITH (location = 's3://spark-smoke/warehouse/{NS}')")
    schemas = [r[0] for r in trino(f"SHOW SCHEMAS FROM {CATALOG}")]
    check(NS in schemas, f"{NS} missing from SHOW SCHEMAS after create: {schemas}")
    code = rest_status(f"/iceberg/v1/{WAREHOUSE}/namespaces/{NS}")
    check(code == 200, f"REST GET namespace {NS} returned {code}")


@step("CREATE TABLE + INSERT 500 + aggregates")
def create_insert():
    trino(
        f"""
        CREATE TABLE {TABLE} (
            id BIGINT,
            category VARCHAR,
            amount DOUBLE,
            quantity INTEGER,
            order_date DATE
        )
        WITH (partitioning = ARRAY['category'], format_version = 2)
        """
    )
    trino(
        f"""
        INSERT INTO {TABLE}
        SELECT
            id,
            'cat_' || CAST(id % 4 AS VARCHAR),
            CAST(id % 100 AS DOUBLE) * 1.5,
            CAST(id % 10 AS INTEGER) + 1,
            DATE '2026-01-01' + CAST(id % 28 AS INTEGER) * INTERVAL '1' DAY
        FROM UNNEST(sequence(0, 499)) AS t(id)
        """
    )
    cnt, total_amount, total_qty, ncat = trino(
        f"SELECT count(*), sum(amount), sum(quantity), count(DISTINCT category) FROM {TABLE}"
    )[0]
    check(int(cnt) == 500, f"count {cnt} != 500")
    # sum(amount) = 1.5 * 5 * sum(0..99) = 1.5 * 5 * 4950
    check(abs(float(total_amount) - 37125.0) < 1e-6, f"sum(amount) {total_amount} != 37125.0")
    # sum(quantity) = 50 * sum(1..10) = 50 * 45 + 500
    check(int(total_qty) == 2750, f"sum(quantity) {total_qty} != 2750")
    check(int(ncat) == 4, f"distinct categories {ncat} != 4")
    # Partition spec must be exactly the requested one (identity on category).
    meta = rest_get(f"/iceberg/v1/{WAREHOUSE}/namespaces/{NS}/tables/items")["metadata"]
    specs = meta.get("partition-specs", [])
    check(len(specs) == 1, f"expected exactly 1 partition spec, got {specs}")
    fields = specs[0]["fields"]
    check(len(fields) == 1 and fields[0]["transform"] == "identity",
          f"unexpected partition spec fields: {fields}")
    details["trino_table_rows"] = int(cnt)


@step("ADD COLUMN evolution + null backfill")
def evolution():
    trino(f"ALTER TABLE {TABLE} ADD COLUMN note VARCHAR")
    trino(
        f"""
        INSERT INTO {TABLE}
        SELECT id, 'cat_' || CAST(id % 4 AS VARCHAR),
               CAST(id % 100 AS DOUBLE) * 1.5,
               CAST(id % 10 AS INTEGER) + 1,
               DATE '2026-01-01' + CAST(id % 28 AS INTEGER) * INTERVAL '1' DAY,
               'late'
        FROM UNNEST(sequence(500, 509)) AS t(id)
        """
    )
    check(int(one(f"SELECT count(*) FROM {TABLE}")) == 510, "count after evolution insert != 510")
    nulls = int(one(f"SELECT count(*) FROM {TABLE} WHERE note IS NULL"))
    check(nulls == 500, f"pre-evolution rows with NULL note: {nulls} != 500")
    late = int(one(f"SELECT count(*) FROM {TABLE} WHERE note = 'late'"))
    check(late == 10, f"rows with note='late': {late} != 10")
    details["trino_table_rows"] = 510


@step("cross-engine: read Spark's post-MERGE/DELETE table")
def cross_engine_table():
    e = SPARK_EXPECT
    cnt, total_amount, total_qty = trino(
        f"SELECT count(*), sum(amount), sum(quantity) FROM {SPARK_TABLE}"
    )[0]
    observed = {
        "row_count": int(cnt),
        "sum_amount": float(total_amount),
        "sum_quantity": int(total_qty),
        "per_category": {r[0]: int(r[1]) for r in trino(
            f"SELECT category, count(*) FROM {SPARK_TABLE} GROUP BY category"
        )},
        "merged_rows": int(one(f"SELECT count(*) FROM {SPARK_TABLE} WHERE note = 'merged'")),
        "late_rows": int(one(f"SELECT count(*) FROM {SPARK_TABLE} WHERE note = 'late'")),
        "null_note_rows": int(one(f"SELECT count(*) FROM {SPARK_TABLE} WHERE note IS NULL")),
        "deleted_range_rows": int(one(
            f"SELECT count(*) FROM {SPARK_TABLE} WHERE id >= 950 AND id < 1000"
        )),
    }
    # Snapshot count straight from Meridian (Spark reported 5).
    meta = rest_get(f"/iceberg/v1/{WAREHOUSE}/namespaces/{SPARK_NS}/tables/orders")["metadata"]
    observed["snapshot_count"] = len(meta["snapshots"])
    details["spark_table_observed_via_trino"] = observed
    mismatches = {k: (observed[k], e[k]) for k in e if observed[k] != e[k]}
    check(not mismatches,
          f"cross-engine mismatch (observed, expected): {mismatches}")
    result["cross_engine_match"] = True


@step("cross-engine: read Spark's Iceberg view")
def cross_engine_view():
    # The Spark suite's view carries only a 'spark' SQL representation.
    # Record precisely what Trino does with it; a clean dialect-level
    # rejection is acceptable, silent wrong answers or server errors are not.
    try:
        rows = trino(f"SELECT * FROM {SPARK_VIEW}")
        total = sum(int(r[1]) for r in rows)
        check(total == SPARK_EXPECT["row_count"],
              f"spark view read back {total} rows total != {SPARK_EXPECT['row_count']}")
        check(len(rows) == 4, f"spark view returned {len(rows)} categories != 4")
        details["spark_view_read"] = {"outcome": "success", "categories": len(rows), "row_total": total}
    except TrinoError as exc:
        msg = str(exc)
        dialect_rejection = any(s in msg.lower() for s in (
            "dialect", "cannot read view", "unsupported view", "not supported",
        ))
        details["spark_view_read"] = {"outcome": "dialect_rejection" if dialect_rejection else "error",
                                      "error": msg}
        check(dialect_rejection,
              f"expected clean dialect-level rejection or success, got: {msg}")


@step("Trino view: CREATE VIEW + read back + REST dialect check")
def trino_view():
    trino(
        f"""
        CREATE VIEW {VIEW} AS
        SELECT category, count(*) AS cnt, sum(amount) AS total_amount
        FROM {TABLE}
        GROUP BY category
        """
    )
    rows = {r[0]: int(r[1]) for r in trino(f"SELECT * FROM {VIEW}")}
    check(sum(rows.values()) == 510, f"view row total {sum(rows.values())} != 510")
    check(len(rows) == 4, f"view categories {len(rows)} != 4")
    # Confirm the view lives in Meridian with a trino SQL representation.
    view_meta = rest_get(
        f"/iceberg/v1/{WAREHOUSE}/namespaces/{NS}/views/items_by_category"
    )["metadata"]
    dialects = [r["dialect"] for v in view_meta["versions"] for r in v["representations"]]
    check("trino" in dialects, f"no trino representation in view metadata: {dialects}")
    details["trino_view"] = {"name": f"{NS}.items_by_category", "dialects": dialects}


@step("cleanup trino_ns only; spark_ns untouched")
def cleanup():
    trino(f"DROP VIEW {VIEW}")
    trino(f"DROP TABLE {TABLE}")
    trino(f"DROP SCHEMA {CATALOG}.{NS}")
    code = rest_status(f"/iceberg/v1/{WAREHOUSE}/namespaces/{NS}")
    check(code == 404, f"namespace {NS} still present after cleanup (HTTP {code})")
    # The Spark fixture must survive this suite.
    code = rest_status(f"/iceberg/v1/{WAREHOUSE}/namespaces/{SPARK_NS}/tables/orders")
    check(code == 200, f"spark_ns.orders damaged by this suite (HTTP {code})")
    code = rest_status(f"/iceberg/v1/{WAREHOUSE}/namespaces/{SPARK_NS}/views/orders_by_category")
    check(code == 200, f"spark_ns.orders_by_category damaged by this suite (HTTP {code})")


ok = True
ok &= show_schemas()
if ok:
    ok &= create_schema()
    ok &= create_insert()
    ok &= evolution()
    ok &= cross_engine_table()
    ok &= cross_engine_view()
    ok &= trino_view()
    ok &= cleanup()

result["status"] = "pass" if ok and not failures else "fail"
print("SUITE_RESULT: " + json.dumps(result), flush=True)
sys.exit(0 if result["status"] == "pass" else 1)
