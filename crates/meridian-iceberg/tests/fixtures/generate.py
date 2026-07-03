"""Generate pyiceberg-written Iceberg tables (v1 + v2) and dump an
"expected view" JSON of their manifest lists + manifests, as seen by
pyiceberg itself. Also dump the same view for the real Spark-written
table living in MinIO (warehouse spark-smoke).

The .avro files and expected JSON land in
crates/meridian-iceberg/tests/fixtures/{pyiceberg_v1,pyiceberg_v2,spark_orders}/.
"""

from __future__ import annotations

import datetime as dt
import json
import os
import shutil
import uuid as uuidlib
from decimal import Decimal
from pathlib import Path

import pyarrow as pa

from pyiceberg.catalog.sql import SqlCatalog
from pyiceberg.io import load_file_io
from pyiceberg.manifest import ManifestFile
from pyiceberg.partitioning import PartitionField, PartitionSpec
from pyiceberg.schema import Schema
from pyiceberg.transforms import DayTransform, IdentityTransform
from pyiceberg.types import (
    BinaryType,
    BooleanType,
    DateType,
    DecimalType,
    DoubleType,
    FixedType,
    FloatType,
    IntegerType,
    LongType,
    NestedField,
    StringType,
    TimestampType,
    TimestamptzType,
    TimeType,
    UUIDType,
)

FIXTURES = Path(__file__).resolve().parent
SCRATCH = Path(os.environ.get("FIXTURE_SCRATCH", "/tmp/meridian-fixture-gen"))
EPOCH_DATE = dt.date(1970, 1, 1)


def canon_value(v):
    """Canonical JSON rendering of a partition value as read by pyiceberg."""
    if v is None:
        return None
    if isinstance(v, bool):
        return v
    if isinstance(v, int):
        return v
    if isinstance(v, float):
        import struct

        return {"f64": struct.pack(">d", v).hex()}
    if isinstance(v, str):
        return v
    if isinstance(v, (bytes, bytearray)):
        return {"bytes": bytes(v).hex()}
    if isinstance(v, uuidlib.UUID):
        return {"uuid": str(v)}
    if isinstance(v, Decimal):
        sign, digits, exp = v.as_tuple()
        unscaled = int("".join(map(str, digits)))
        if sign:
            unscaled = -unscaled
        return {"decimal": str(unscaled), "scale": -exp}
    if isinstance(v, dt.datetime):
        micros = int(v.timestamp() * 1_000_000) if v.tzinfo else int(
            (v - dt.datetime(1970, 1, 1)).total_seconds() * 1_000_000
        )
        return micros
    if isinstance(v, dt.date):
        return (v - EPOCH_DATE).days
    if isinstance(v, dt.time):
        return (v.hour * 3600 + v.minute * 60 + v.second) * 1_000_000 + v.microsecond
    raise TypeError(f"unhandled partition value type {type(v)}")


def dump_map(m):
    if m is None:
        return None
    out = {}
    for k, v in m.items():
        out[str(k)] = v.hex() if isinstance(v, (bytes, bytearray)) else v
    return dict(sorted(out.items(), key=lambda kv: int(kv[0])))


def dump_manifest(io, mf: ManifestFile):
    entries = []
    for e in mf.fetch_manifest_entry(io, discard_deleted=False):
        df = e.data_file
        partition = [canon_value(df.partition[i]) for i in range(len(df.partition))]
        entries.append(
            {
                "status": int(e.status),
                "snapshot_id": e.snapshot_id,
                "sequence_number": e.sequence_number,
                "file_sequence_number": e.file_sequence_number,
                "content": int(df.content),
                "file_path": df.file_path,
                "file_format": str(df.file_format),
                "partition": partition,
                "record_count": df.record_count,
                "file_size_in_bytes": df.file_size_in_bytes,
                "column_sizes": dump_map(df.column_sizes),
                "value_counts": dump_map(df.value_counts),
                "null_value_counts": dump_map(df.null_value_counts),
                "nan_value_counts": dump_map(df.nan_value_counts),
                "lower_bounds": dump_map(df.lower_bounds),
                "upper_bounds": dump_map(df.upper_bounds),
                "split_offsets": df.split_offsets,
                "equality_ids": df.equality_ids,
                "sort_order_id": df.sort_order_id,
            }
        )
    summaries = None
    if mf.partitions is not None:
        summaries = [
            {
                "contains_null": s.contains_null,
                "contains_nan": s.contains_nan,
                "lower_bound": s.lower_bound.hex() if s.lower_bound is not None else None,
                "upper_bound": s.upper_bound.hex() if s.upper_bound is not None else None,
            }
            for s in mf.partitions
        ]
    return {
        "manifest_path": mf.manifest_path,
        "manifest_length": mf.manifest_length,
        "partition_spec_id": mf.partition_spec_id,
        "content": int(mf.content),
        "sequence_number": mf.sequence_number,
        "min_sequence_number": mf.min_sequence_number,
        "added_snapshot_id": mf.added_snapshot_id,
        "added_files_count": mf.added_files_count,
        "existing_files_count": mf.existing_files_count,
        "deleted_files_count": mf.deleted_files_count,
        "added_rows_count": mf.added_rows_count,
        "existing_rows_count": mf.existing_rows_count,
        "deleted_rows_count": mf.deleted_rows_count,
        "partitions": summaries,
        "key_metadata": mf.key_metadata.hex() if mf.key_metadata else None,
        "entries": entries,
    }


def dump_table(tbl, out_path: Path):
    io = tbl.io
    snapshots = []
    for snap in tbl.metadata.snapshots:
        manifests = [dump_manifest(io, mf) for mf in snap.manifests(io)]
        snapshots.append(
            {
                "snapshot_id": snap.snapshot_id,
                "operation": str(snap.summary.operation) if snap.summary else None,
                "manifest_list": snap.manifest_list,
                "manifests": manifests,
            }
        )
    doc = {"format_version": tbl.metadata.format_version, "snapshots": snapshots}
    out_path.parent.mkdir(parents=True, exist_ok=True)
    out_path.write_text(json.dumps(doc, indent=1, sort_keys=True))
    print(f"wrote {out_path}")


WIDE_SCHEMA = Schema(
    NestedField(1, "id", LongType(), required=False),
    NestedField(2, "category", StringType(), required=False),
    NestedField(3, "ts", TimestampType(), required=False),
    NestedField(4, "flag", BooleanType(), required=False),
    NestedField(5, "small", IntegerType(), required=False),
    NestedField(6, "ratio", FloatType(), required=False),
    NestedField(7, "amount", DoubleType(), required=False),
    NestedField(8, "price", DecimalType(9, 2), required=False),
    NestedField(9, "big", DecimalType(28, 10), required=False),
    NestedField(10, "day", DateType(), required=False),
    NestedField(11, "tod", TimeType(), required=False),
    NestedField(12, "tstz", TimestamptzType(), required=False),
    NestedField(13, "name", StringType(), required=False),
    NestedField(14, "uid", UUIDType(), required=False),
    NestedField(15, "blob", BinaryType(), required=False),
    NestedField(16, "fx", FixedType(4), required=False),
)


def arrow_batch(rows):
    """rows: list of dicts keyed by column name."""
    schema = pa.schema(
        [
            pa.field("id", pa.int64()),
            pa.field("category", pa.large_string()),
            pa.field("ts", pa.timestamp("us")),
            pa.field("flag", pa.bool_()),
            pa.field("small", pa.int32()),
            pa.field("ratio", pa.float32()),
            pa.field("amount", pa.float64()),
            pa.field("price", pa.decimal128(9, 2)),
            pa.field("big", pa.decimal128(28, 10)),
            pa.field("day", pa.date32()),
            pa.field("tod", pa.time64("us")),
            pa.field("tstz", pa.timestamp("us", tz="UTC")),
            pa.field("name", pa.large_string()),
            pa.field("uid", pa.uuid()),
            pa.field("blob", pa.large_binary()),
            pa.field("fx", pa.binary(4)),
        ]
    )
    cols = {name: [r.get(name) for r in rows] for name in schema.names}
    cols["uid"] = [u.bytes if u is not None else None for u in cols["uid"]]
    return pa.table(cols, schema=schema)


ROWS_A = [
    dict(
        id=1,
        category="alpha",
        ts=dt.datetime(2026, 1, 15, 10, 0, 5, 123456),
        flag=True,
        small=7,
        ratio=1.5,
        amount=10.25,
        price=Decimal("19.99"),
        big=Decimal("12345678901234.5678901234"),
        day=dt.date(2026, 1, 15),
        tod=dt.time(9, 30, 0, 250000),
        tstz=dt.datetime(2026, 1, 15, 10, 0, 5, 123456, tzinfo=dt.timezone.utc),
        name="first",
        uid=uuidlib.UUID("f79c3e09-677c-4bbd-a479-3f349cb785e7"),
        blob=b"\x00\x01\x02",
        fx=b"ABCD",
    ),
    dict(
        id=2,
        category="alpha",
        ts=dt.datetime(2026, 1, 15, 11, 30, 0),
        flag=False,
        small=-3,
        ratio=float("nan"),
        amount=None,
        price=Decimal("0.50"),
        big=Decimal("-1.0000000001"),
        day=dt.date(2026, 1, 16),
        tod=dt.time(23, 59, 59, 999999),
        tstz=dt.datetime(2026, 1, 15, 12, 0, 0, tzinfo=dt.timezone.utc),
        name=None,
        uid=uuidlib.UUID("00000000-0000-0000-0000-000000000001"),
        blob=b"\xff",
        fx=b"WXYZ",
    ),
    dict(
        id=3,
        category="alpha",
        ts=dt.datetime(2026, 1, 15, 23, 59, 59, 999999),
        flag=True,
        small=1024,
        ratio=-2.25,
        amount=float("nan"),
        price=Decimal("1000000.01"),
        big=Decimal("99999999999999999.9999999999"),
        day=dt.date(1969, 12, 31),
        tod=dt.time(0, 0, 0),
        tstz=dt.datetime(1969, 12, 31, 23, 0, 0, tzinfo=dt.timezone.utc),
        name="third",
        uid=None,
        blob=None,
        fx=b"\x00\x00\x00\x00",
    ),
]

ROWS_B = [
    dict(
        id=100,
        category="beta",
        ts=dt.datetime(2026, 2, 1, 0, 0, 0),
        flag=None,
        small=None,
        ratio=None,
        amount=-273.15,
        price=None,
        big=None,
        day=None,
        tod=None,
        tstz=None,
        name="only",
        uid=uuidlib.UUID("11111111-2222-3333-4444-555555555555"),
        blob=b"beta-blob",
        fx=b"zzzz",
    ),
    dict(
        id=101,
        category="beta",
        ts=dt.datetime(2026, 2, 2, 18, 45, 1, 42),
        flag=False,
        small=0,
        ratio=0.0,
        amount=1e300,
        price=Decimal("-42.42"),
        big=Decimal("0.0000000001"),
        day=dt.date(2026, 2, 2),
        tod=dt.time(12, 0, 0),
        tstz=dt.datetime(2026, 2, 2, 18, 45, 1, tzinfo=dt.timezone.utc),
        name="second",
        uid=uuidlib.UUID("ffffffff-ffff-ffff-ffff-ffffffffffff"),
        blob=b"",
        fx=b"\xff\xff\xff\xff",
    ),
]


def build_v2(catalog):
    spec = PartitionSpec(
        PartitionField(source_id=2, field_id=1000, transform=IdentityTransform(), name="category"),
        PartitionField(source_id=3, field_id=1001, transform=DayTransform(), name="ts_day"),
    )
    tbl = catalog.create_table(
        "fx.types_v2",
        schema=WIDE_SCHEMA,
        partition_spec=spec,
        properties={"format-version": "2"},
    )
    tbl.append(arrow_batch(ROWS_A))
    tbl.append(arrow_batch(ROWS_B))
    tbl = catalog.load_table("fx.types_v2")
    tbl.delete("category == 'alpha'")
    return catalog.load_table("fx.types_v2")


def build_v1(catalog):
    spec = PartitionSpec(
        PartitionField(source_id=2, field_id=1000, transform=IdentityTransform(), name="category"),
    )
    tbl = catalog.create_table(
        "fx.types_v1",
        schema=WIDE_SCHEMA,
        partition_spec=spec,
        properties={"format-version": "1"},
    )
    tbl.append(arrow_batch(ROWS_A))
    tbl.append(arrow_batch(ROWS_B))
    return catalog.load_table("fx.types_v1")


def collect_avro(tbl, dest: Path):
    dest.mkdir(parents=True, exist_ok=True)
    meta_dir = Path(tbl.metadata_location.removeprefix("file://")).parent
    for f in meta_dir.glob("*.avro"):
        shutil.copy(f, dest / f.name)
    print(f"copied {len(list(dest.glob('*.avro')))} avro files to {dest}")


def main():
    wh = SCRATCH / "pyiceberg_wh"
    if wh.exists():
        shutil.rmtree(wh)
    wh.mkdir()
    catalog = SqlCatalog(
        "fixtures",
        uri=f"sqlite:///{wh}/catalog.db",
        warehouse=f"file://{wh}",
    )
    catalog.create_namespace("fx")

    v2 = build_v2(catalog)
    dump_table(v2, FIXTURES / "pyiceberg_v2" / "expected.json")
    collect_avro(v2, FIXTURES / "pyiceberg_v2")

    v1 = build_v1(catalog)
    dump_table(v1, FIXTURES / "pyiceberg_v1" / "expected.json")
    collect_avro(v1, FIXTURES / "pyiceberg_v1")

    # The real Spark-written merge-on-read table in MinIO (dev compose).
    # Requires the conformance Spark smoke suite to have run; skip with
    # SKIP_SPARK=1 when only regenerating the pyiceberg tables.
    if os.environ.get("SKIP_SPARK"):
        return

    spark_meta = FIXTURES / "spark_orders" / "table.metadata.json"
    props = {
        "s3.endpoint": "http://localhost:9000",
        "s3.access-key-id": "meridian",
        "s3.secret-access-key": "meridian123",
        "s3.region": "us-east-1",
    }
    io = load_file_io(props, location=str(spark_meta))
    from pyiceberg.serializers import FromInputFile

    metadata = FromInputFile.table_metadata(io.new_input(f"file://{spark_meta}"))

    class Shim:
        def __init__(self, metadata, io):
            self.metadata = metadata
            self.io = load_file_io(props, location=metadata.location)

    dump_table(Shim(metadata, None), FIXTURES / "spark_orders" / "expected.json")


if __name__ == "__main__":
    main()
