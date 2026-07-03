# Spark 3.5 smoke suite against a Meridian REST catalog.
#
# Runs inside the apache/spark container (host networking) via
# spark-submit; see run.sh. Every step is verified with an assertion;
# failures are collected and the script exits non-zero if any step
# failed. The last line of output is a single JSON object:
#   {status, table_identifier, warehouse, row_count, snapshot_count,
#    view_name_or_null, failures: [...]}
#
# The final table (post-MERGE/DELETE) is deliberately left in place so
# its metadata can be inspected over REST after the run.

import json
import sys
import traceback
import urllib.request

from pyspark.sql import SparkSession

MERIDIAN_URI = "http://host.docker.internal:8181/iceberg"
WAREHOUSE = "spark_smoke"
CATALOG = "mrd"
NS = "spark_ns"
TABLE = f"{CATALOG}.{NS}.orders"
VIEW = f"{CATALOG}.{NS}.orders_by_category"

failures = []
result = {
    "status": "fail",
    "table_identifier": f"{NS}.orders",
    "warehouse": WAREHOUSE,
    "row_count": None,
    "snapshot_count": None,
    "view_name_or_null": None,
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


spark = (
    SparkSession.builder.appName("meridian-spark-smoke")
    .config(
        "spark.sql.extensions",
        "org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions",
    )
    .config(f"spark.sql.catalog.{CATALOG}", "org.apache.iceberg.spark.SparkCatalog")
    .config(f"spark.sql.catalog.{CATALOG}.type", "rest")
    .config(f"spark.sql.catalog.{CATALOG}.uri", MERIDIAN_URI)
    .config(f"spark.sql.catalog.{CATALOG}.warehouse", WAREHOUSE)
    # Meridian vends the warehouse's s3.endpoint (http://localhost:9000)
    # in LoadTableResult.config and the Java REST client merges that over
    # client properties, so the container runs with host networking to
    # make localhost:9000 reach MinIO. Credentials are never vended and
    # must be configured client-side.
    .config(f"spark.sql.catalog.{CATALOG}.io-impl", "org.apache.iceberg.aws.s3.S3FileIO")
    .config(f"spark.sql.catalog.{CATALOG}.s3.endpoint", "http://localhost:9000")
    .config(f"spark.sql.catalog.{CATALOG}.s3.path-style-access", "true")
    .config(f"spark.sql.catalog.{CATALOG}.s3.access-key-id", "meridian")
    .config(f"spark.sql.catalog.{CATALOG}.s3.secret-access-key", "meridian123")
    .config(f"spark.sql.catalog.{CATALOG}.client.region", "us-east-1")
    .getOrCreate()
)


def sql(q):
    return spark.sql(q)


def one(q):
    return sql(q).collect()[0][0]


def rest_table():
    """Load the table's REST metadata straight from Meridian."""
    url = f"{MERIDIAN_URI}/v1/{WAREHOUSE}/namespaces/{NS}/tables/orders"
    with urllib.request.urlopen(url, timeout=10) as resp:
        return json.loads(resp.read())


first_snapshot_id = None


@step("create namespace")
def create_namespace():
    sql(f"CREATE NAMESPACE IF NOT EXISTS {CATALOG}.{NS}")
    namespaces = [r[0] for r in sql(f"SHOW NAMESPACES IN {CATALOG}").collect()]
    check(NS in namespaces, f"namespace {NS} missing from SHOW NAMESPACES: {namespaces}")


@step("create partitioned table (5 cols)")
def create_table():
    sql(
        f"""
        CREATE TABLE {TABLE} (
            id BIGINT,
            category STRING,
            amount DOUBLE,
            quantity INT,
            order_date DATE
        )
        USING iceberg
        PARTITIONED BY (category)
        TBLPROPERTIES (
            'format-version' = '2',
            'write.delete.mode' = 'merge-on-read',
            'write.update.mode' = 'merge-on-read',
            'write.merge.mode' = 'merge-on-read'
        )
        """
    )
    cols = [r[0] for r in sql(f"DESCRIBE TABLE {TABLE}").collect() if r[0] and not r[0].startswith("#")]
    check(
        cols[:5] == ["id", "category", "amount", "quantity", "order_date"],
        f"unexpected columns: {cols}",
    )
    # Partition spec must be exactly the requested one (identity on category).
    meta = rest_table()["metadata"]
    specs = meta.get("partition-specs", [])
    check(len(specs) == 1, f"expected exactly 1 partition spec, got {specs}")
    fields = specs[0]["fields"]
    check(
        len(fields) == 1 and fields[0]["transform"] == "identity",
        f"unexpected partition spec fields: {fields}",
    )


def insert_batch(lo, hi, note_expr=None):
    note_col = f", {note_expr} AS note" if note_expr else ""
    sql(
        f"""
        INSERT INTO {TABLE}
        SELECT
            id,
            concat('cat_', CAST(id % 4 AS STRING)) AS category,
            CAST(id % 100 AS DOUBLE) * 1.5 AS amount,
            CAST(id % 10 AS INT) + 1 AS quantity,
            date_add(DATE '2026-01-01', CAST(id % 28 AS INT)) AS order_date
            {note_col}
        FROM range({lo}, {hi})
        """
    )


@step("insert 1000 rows in 2 batches")
def insert_rows():
    global first_snapshot_id
    insert_batch(0, 500)
    first_snapshot_id = one(
        f"SELECT snapshot_id FROM {TABLE}.snapshots ORDER BY committed_at ASC LIMIT 1"
    )
    insert_batch(500, 1000)
    check(one(f"SELECT count(*) FROM {TABLE}") == 1000, "count after 2 batches != 1000")


@step("aggregates")
def aggregates():
    cnt, total_amount, total_qty, ncat = sql(
        f"SELECT count(*), sum(amount), sum(quantity), count(DISTINCT category) FROM {TABLE}"
    ).collect()[0]
    check(cnt == 1000, f"count {cnt} != 1000")
    # sum(amount) = 1.5 * sum(id % 100) over 0..999 = 1.5 * 10 * 4950
    check(abs(total_amount - 74250.0) < 1e-6, f"sum(amount) {total_amount} != 74250.0")
    # sum(quantity) = sum(id % 10 + 1) = 100 * 45 + 1000
    check(total_qty == 5500, f"sum(quantity) {total_qty} != 5500")
    check(ncat == 4, f"distinct categories {ncat} != 4")
    per_cat = {r[0]: r[1] for r in sql(
        f"SELECT category, count(*) FROM {TABLE} GROUP BY category"
    ).collect()}
    check(
        per_cat == {f"cat_{i}": 250 for i in range(4)},
        f"per-category counts wrong: {per_cat}",
    )


@step("add column + insert + null backfill")
def schema_evolution():
    sql(f"ALTER TABLE {TABLE} ADD COLUMN note STRING")
    insert_batch(1000, 1010, note_expr="'late'")
    check(one(f"SELECT count(*) FROM {TABLE}") == 1010, "count after 3rd insert != 1010")
    nulls = one(f"SELECT count(*) FROM {TABLE} WHERE note IS NULL")
    check(nulls == 1000, f"pre-evolution rows with NULL note: {nulls} != 1000")
    late = one(f"SELECT count(*) FROM {TABLE} WHERE note = 'late'")
    check(late == 10, f"rows with note='late': {late} != 10")


@step("time travel VERSION AS OF first snapshot")
def time_travel():
    check(first_snapshot_id is not None, "first snapshot id was not captured")
    cnt = one(f"SELECT count(*) FROM {TABLE} VERSION AS OF {first_snapshot_id}")
    check(cnt == 500, f"count at first snapshot {cnt} != 500")
    # The note column did not exist at the first snapshot.
    old_cols = [f.name for f in sql(
        f"SELECT * FROM {TABLE} VERSION AS OF {first_snapshot_id}"
    ).schema.fields]
    check("note" not in old_cols, f"'note' unexpectedly visible at first snapshot: {old_cols}")


@step("MERGE INTO (~100 rows updated)")
def merge_into():
    sql(
        f"""
        MERGE INTO {TABLE} t
        USING (SELECT id, 'merged' AS note FROM range(0, 100)) s
        ON t.id = s.id
        WHEN MATCHED THEN UPDATE SET t.note = s.note
        """
    )
    merged = one(f"SELECT count(*) FROM {TABLE} WHERE note = 'merged'")
    check(merged == 100, f"rows with note='merged': {merged} != 100")
    check(one(f"SELECT count(*) FROM {TABLE}") == 1010, "count changed by MERGE")


@step("DELETE FROM (~50 rows)")
def delete_rows():
    sql(f"DELETE FROM {TABLE} WHERE id >= 950 AND id < 1000")
    cnt = one(f"SELECT count(*) FROM {TABLE}")
    check(cnt == 960, f"count after DELETE {cnt} != 960")
    remaining = one(f"SELECT count(*) FROM {TABLE} WHERE id >= 950 AND id < 1000")
    check(remaining == 0, f"{remaining} deleted rows still visible")
    result["row_count"] = cnt


@step("REST metadata: snapshots + delete files")
def rest_metadata():
    meta = rest_table()["metadata"]
    snaps = sorted(meta["snapshots"], key=lambda s: s["timestamp-ms"])
    ops = [s["summary"]["operation"] for s in snaps]
    result["snapshot_count"] = len(snaps)
    # append x3 (two batches + post-evolution insert), overwrite (MERGE,
    # merge-on-read), delete (DELETE, merge-on-read).
    check(len(snaps) == 5, f"expected 5 snapshots, got {len(snaps)}: {ops}")
    check(ops[:3] == ["append"] * 3, f"first three ops should be appends: {ops}")
    check(ops[3] == "overwrite", f"MERGE snapshot op {ops[3]} != overwrite")
    check(ops[4] == "delete", f"DELETE snapshot op {ops[4]} != delete")
    for label, snap in (("MERGE", snaps[3]), ("DELETE", snaps[4])):
        added = int(snap["summary"].get("added-delete-files", "0"))
        check(added >= 1, f"{label} snapshot added no delete files: {snap['summary']}")
    total_deletes = int(snaps[-1]["summary"].get("total-delete-files", "0"))
    check(total_deletes >= 2, f"total-delete-files {total_deletes} < 2")
    print(f"snapshot operations: {ops}")
    print(f"final summary: {json.dumps(snaps[-1]['summary'], indent=2)}")


@step("Iceberg view: CREATE VIEW + SELECT")
def views():
    sql(
        f"""
        CREATE VIEW {VIEW} AS
        SELECT category, count(*) AS cnt, sum(amount) AS total_amount
        FROM {TABLE}
        GROUP BY category
        """
    )
    rows = {r["category"]: r["cnt"] for r in sql(f"SELECT * FROM {VIEW}").collect()}
    check(sum(rows.values()) == 960, f"view row total {sum(rows.values())} != 960")
    check(len(rows) == 4, f"view categories {len(rows)} != 4")
    # Confirm the view actually lives in Meridian, not in Spark's session.
    url = f"{MERIDIAN_URI}/v1/{WAREHOUSE}/namespaces/{NS}/views/orders_by_category"
    with urllib.request.urlopen(url, timeout=10) as resp:
        view_meta = json.loads(resp.read())["metadata"]
    dialects = [
        r["dialect"]
        for v in view_meta["versions"]
        for r in v["representations"]
    ]
    check("spark" in dialects, f"no spark representation in view metadata: {dialects}")
    result["view_name_or_null"] = f"{NS}.orders_by_category"


ok = True
ok &= create_namespace()
ok &= create_table()
if ok:
    # Data steps only make sense if DDL succeeded.
    ok &= insert_rows()
    ok &= aggregates()
    ok &= schema_evolution()
    ok &= time_travel()
    ok &= merge_into()
    ok &= delete_rows()
    ok &= rest_metadata()
    ok &= views()

result["status"] = "pass" if ok and not failures else "fail"
print("SUITE_RESULT: " + json.dumps(result), flush=True)
spark.stop()
sys.exit(0 if result["status"] == "pass" else 1)
