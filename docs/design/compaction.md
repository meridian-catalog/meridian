# Compaction (bin-pack rewrite)

Status: implemented in `crates/meridian-executor` as a plan producer —
it reads a table, rewrites small files, and returns an Iceberg `replace`
commit **without committing**. The commit path (`PostgresCommitBackend`)
applies the returned `updates`/`requirements` so the rewrite lands as a
normal, audited, snapshot-rollback-revertible commit; wiring the executor
to a route and a job queue is the next wave's work and is not part of this
crate. This document records the design and its decisions.
[ADR 007](../adr/007-compaction-executor-arrow-parquet.md) records why the
rewrite is built on `arrow` + `parquet` directly rather than DataFusion.

## What it is

Compaction rewrites a table's small data files into fewer, larger ones.
Small files are the structural pain of streaming and external-write
workloads (a 20-task Flink job committing per minute writes ~28,800 files
per day per table; Snowflake external writes are documented to produce 2–3×
the file count). Every small file is a manifest entry to plan over and an
object to GET at read time. Merging them cuts read amplification and
metadata size with no change to the data.

The executor is a **plan producer**, not a committer. Given a table's
current metadata and a storage handle it:

1. reads the current snapshot's live data files, groups them by partition,
   and bin-packs the small ones into rewrite groups approaching a target
   size (`select.rs`);
2. rewrites each group's inputs into one Parquet file — mapping columns by
   Iceberg field id, materializing pending merge-on-read deletes, carrying
   column stats forward (`rewrite.rs`), asserting row conservation;
3. produces an Iceberg `RewriteFiles` (`replace`) commit — the new
   snapshot's manifests and manifest list, plus the `TableUpdate` /
   `TableRequirement` lists — and returns it as a `CompactionPlan`
   (`metadata_result.rs`, `plan.rs`).

It never commits and never deletes an input file. The commit is optimistic:
its requirements assert the table has not moved since planning, so a racing
writer makes it fail cleanly at the compare-and-set, never corrupt.

The orchestration entry point is `compact_table` (storage-backed) /
`compact_with_sources` (source-injected, for tests) in `compact.rs`.

## The CompactionPlan API

```rust
pub async fn compact_table(
    storage: &dyn Storage,
    metadata: &TableMetadata,
    options: &CompactionOptions,
    new_ids: &dyn Fn() -> i64,
) -> CompactionResult<CompactionPlan>;
```

`options`:

| field                    | default        | meaning                                                    |
|--------------------------|----------------|------------------------------------------------------------|
| `target_file_size_bytes` | 512 MiB        | files ≥ this are left alone; outputs are packed toward it  |
| `min_input_files`        | 5              | a partition is skipped below this many candidate files     |
| `dry_run`                | false          | plan and list files it *would* write, writing nothing      |

`new_ids` mints the fresh snapshot id — a random source in production, a
fixed counter in tests. It is called through a collision check against the
table's existing snapshot ids.

`CompactionPlan`:

```rust
pub struct CompactionPlan {
    pub updates: Vec<TableUpdate>,        // add-snapshot (replace) + set main ref
    pub requirements: Vec<TableRequirement>, // assert uuid + main unchanged
    pub new_files_written: Vec<NewFile>,  // every data file written (or, dry-run, planned)
    pub stats: CompactionStats,           // before/after ledger numbers
    pub base_snapshot_id: Option<i64>,    // the snapshot this plan rewrites
    pub new_snapshot_id: Option<i64>,     // the snapshot the plan introduces
}
```

`CompactionStats` carries `files_before`/`files_after`,
`bytes_before`/`bytes_after`, `records_before`/`records_after`, and
`delete_files_removed` — the inputs the savings ledger (C-F5) is built from.
`records_before == records_after` for a pure bin-pack; `records_after` is
strictly smaller exactly when merge-on-read deletes were materialized.

The plan is a proposal. Committing it is the next wave's job:
`PostgresCommitBackend` applies `updates` under `requirements`, writing the
new `metadata.json`, the audit row, and the outbox event in one transaction —
the same path every engine commit takes. The manifests and manifest list the
plan references are already on storage by the time the plan returns (they are
immutable and uniquely named; an aborted run leaves only unreferenced orphans
the sweep collects, never a partial commit).

### Idempotence and dry-run

- **No-op when there is nothing to gain.** A table with no current snapshot,
  a table whose files are all already at/above target, or a partition below
  `min_input_files` yields `CompactionPlan::noop` — empty `updates`, nothing
  written. Re-running compaction on an already-compacted table therefore
  changes nothing: the operation is idempotent by construction. (`is_noop()`
  is true whenever `updates` is empty.)
- **Dry-run** returns the plan and the files it *would* write
  (sizes/records estimated from the inputs, `written: false`) without reading
  data or writing any bytes. `updates` is empty in dry-run — nothing was
  staged, so there is nothing to commit — and storage is untouched.

## Step 1 — select and bin-pack (`select.rs`)

Reading the current snapshot mirrors the scan planner's live-entry rules
(the two must agree on what is "live"):

- Read the manifest list, then each manifest whose list entry reports live
  files. Skip `DELETED`-status entries. Inherit the sequence number and
  adding-snapshot id from the manifest-list entry where the stored entry
  leaves them null.
- Split entries into **data files** and **delete files** (position and
  equality) by manifest content type and `DataFile.content`.
- Group data files by a canonical **partition key**: the spec id plus the
  tuple's field ids and Appendix-D value bytes, sorted by field id so writer
  field-order differences do not split a partition. This is the same
  `partition_key` shape the scan planner uses.

**Delete attachment.** Each data file gets the indices of the delete files
that apply to it, by the Iceberg spec's scope rules — the compaction-side
mirror of the scan planner's `DeleteIndex`:

- A deletion vector (`content_offset` present) referencing a data file
  supersedes plain position deletes for that file.
- Plain position deletes apply by `referenced_data_file` path, or by
  partition when unbound, with data-sequence `<=` the delete's sequence and
  partition equality.
- Equality deletes apply globally (unpartitioned spec) or by partition
  equality, with data-sequence **strictly less than** the delete's sequence,
  matched over their declared equality columns.

**Bin-packing.** Within each partition, candidates are the files below
target (plus any file carrying deletes, which is always a candidate — its
rows must be rewritten to shed the delete file even if it is large). If a
partition has fewer than `min_input_files` candidates and none carry deletes,
it is skipped. Candidates are packed largest-first, first-fit-decreasing into
bins whose combined size stays under target. A single-file bin with no
deletes is dropped (rewriting one file into one file is a pointless copy); a
bin is emitted if it merges more than one file or sheds a delete.

## Step 2 — rewrite (`rewrite.rs`)

For each bin-pack group:

1. **Read** each input Parquet file to a single Arrow `RecordBatch`.
2. **Resolve types by field id.** Record the Arrow type of each field id
   seen (first input that has it wins; a table's column types are stable
   across its files). The output schema is one column per top-level Iceberg
   field, in schema order, each carrying its field id.
3. **Apply deletes** attached to that input:
   - *Position deletes* — read the delete file, match rows by
     `(file_path, pos)` (reserved field ids 2147483546 / 2147483545, with a
     `file_path`/`pos` name fallback), mark those positions removed.
   - *Equality deletes* — build the set of delete key tuples over the
     equality columns (Appendix-D bytes, null keys excluded — SQL
     `NULL != NULL`, the reference behavior), remove matching rows.
   - *Deletion vectors* — refused (see below).
4. **Project** each surviving batch to the output schema **by field id**,
   never by name or position: a column present under a different physical
   order is realigned; a field an older input lacks is synthesized as an
   all-null column (schema evolution; the field must be optional, or the data
   is inconsistent and the rewrite refuses).
5. **Concatenate** the projected batches and **write** one Parquet file,
   with per-column statistics enabled and each column's field id written into
   its `PARQUET:field_id` footer metadata so engines still resolve columns
   after the rewrite.

**Column statistics** (`stats.rs`) are computed from the merged *output*
rows, not carried from input footers — after deletes, input bounds may be
looser than the truth, and computing from the output gives exact bounds for
the rows actually written. Lower/upper bounds are produced for the primitive
Arrow types with an unambiguous Iceberg single-value encoding (bool, int,
long, date, micro-timestamp ±tz, float, double, string, binary; floats skip
NaN). Other types get no bound — spec-legal, and never a *wrong* bound.
Record, value, and null counts are always exact.

## The correctness bar

The central invariant, asserted per group in `compact.rs` before any plan is
returned:

1. **Row conservation, always:** `input_records == output_records +
   rows_deleted`. Every input row is accounted for as either carried to the
   output or removed by a delete. A dropped batch, a bad concat, or an
   off-by-one in delete application trips it.
2. **Exactness for delete-free groups:** with no deletes, `output_records ==
   input_records` exactly (the overwhelming-majority pure bin-pack case).

A violated assertion is a hard `RowCountMismatch` error that aborts the plan
— compaction never emits a commit that would silently lose or duplicate rows.
An input carrying a field id absent from the current schema is likewise
refused (`UnmappableField`) rather than guessed by name: silently misaligning
columns is the one outcome worse than not compacting.

The test suite (`tests/compaction.rs`) verifies the bar end-to-end on real
Parquet + real Iceberg manifests built in memory:

- **bin-pack** — 12 small files across two partitions merge to ≤ 2 files;
  every original row present exactly once (sorted-projection compare on
  read-back); field ids preserved; the `TableUpdate`s describe a `replace`
  moving `main`; the produced manifests/manifest list parse back through
  `meridian_iceberg` with the old files `DELETED` and the new ones `ADDED`
  summing to the same live row count.
- **field-id mapping** — a partition mixing normally-ordered files with
  files whose Parquet columns are in *reversed* physical order compacts to
  correct rows in canonical schema order: proof that columns map by field id,
  not position.
- **schema evolution** — files predating a column compact with a synthesized
  null column for it; no rows dropped, all three field ids on the output.
- **merge-on-read** — a position-delete file removing two rows yields 18
  output rows from 20 inputs, the deleted rows absent, and the delete file
  dropped (the new snapshot carries no delete manifest).
- **dry-run**, **already-compact no-op**, **below-min-files skip**, and
  **empty-table no-op** cover the safety and idempotence properties.

## Step 3 — the replace commit (`metadata_result.rs`)

The rewrite becomes an Iceberg `replace` operation:

- A new **data manifest** holding, in one file: `ADDED` entries for the
  compacted outputs, `DELETED` entries for the inputs they replace, and
  `EXISTING` entries carrying forward every other live data file.
- Zero or more **delete manifests** carrying forward the live delete files
  that were *not* fully consumed. A delete file is dropped (not carried) iff
  every live data file it applied to was rewritten — its effect is now
  materialized in the output. A delete still attached to a surviving data
  file stays, marked `EXISTING`.
- A **snapshot** with `operation = replace` and a summary reporting
  `added-data-files`, `deleted-data-files`, `added-records`,
  `deleted-records` (equal for a pure bin-pack), and `removed-delete-files`
  when applicable.
- `updates`: `AddSnapshot` (the replace snapshot) then `SetSnapshotRef`
  moving `main` to it. `requirements`: `AssertTableUuid` and
  `AssertRefSnapshotId { ref: "main" }` pinned to the base snapshot.

**Sequence numbers.** The new snapshot takes `last-sequence-number + 1`.
`ADDED` entries inherit it (left null in the manifest, per spec). `EXISTING`
and `DELETED` entries keep their original explicit sequence numbers — the
spec forbids inventing new ones for carried files. Because the compacted
output carries the new, higher sequence number, any equality delete carried
forward correctly does **not** apply to it: those rows were already
materialized out during the rewrite. This is what makes materialize-then-
carry safe.

## Deletes: the decision

**Merge-on-read deletes are applied during the rewrite** (the compacted
output has them materialized — fewer rows — and the fully-consumed delete
files are dropped), rather than refusing to compact files that carry deletes.
This is the higher-value behavior and the correct one: it is how compaction
sheds delete-file debt, not just small-file debt, and it is what the health
model's delete-ratio recommendation (C-F1) exists to trigger.

Scope, stated honestly:

- **Position-delete files** — applied by `(file_path, pos)`.
- **Equality-delete files** — applied by value tuple over the equality
  columns, respecting the strict sequence-number scope.
- **Deletion vectors (v3 Puffin blobs)** — **refused**, not silently
  dropped. The manifest reader preserves a DV's `content_offset` /
  `content_size_in_bytes`, and an input with an attached DV aborts the group
  with a clear message ("compact after the DV is materialized, or exclude
  this file"). A plain Parquet reader cannot decode a Puffin blob; DV
  materialization needs a Puffin reader and is deferred. Refusing is the safe
  choice — the alternative (ignoring the DV) would resurrect deleted rows.

## What is out of scope for this cut

Documented so the boundary is not mistaken for a bug:

- **Sort-based and z-order compaction.** Bin-pack only. A sort large enough
  to spill wants an execution engine; that is the DataFusion decision
  (ADR 007), taken when the first sorting operation lands.
- **Output-file splitting.** A group produces one output file; an
  over-target merged group is not yet split into several. Modelled in the
  types (`RewriteOutcome.outputs` is a list) as forward compatibility.
- **Deletion-vector materialization.** Refused, as above.
- **Snapshot expiry and orphan-file cleanup.** Compaction never deletes an
  input data file — it only marks it `DELETED` in the new snapshot, which
  snapshot rollback fully reverses. Physically removing superseded files is
  the job of snapshot expiry + orphan cleanup, later, with safety windows.
  This division keeps every compaction reversible.
- **v1 tables with deletes.** v1 has no delete files; the writer targets
  v1/v2 manifest shapes. A v1 table compacts as a pure bin-pack.

## Enterprise properties (spec §Pillar C)

- **Every mutation is a normal Iceberg commit** through the existing commit
  path — auditable, and revertible via snapshot rollback. Compaction adds no
  new mutation path and no unaudited write.
- **No broad credentials.** The executor reads and writes only under the
  table's own location; a job never needs storage credentials wider than the
  table scope.
- **Air-gapped-friendly.** The built-in executor is pure Rust with no
  external service dependency, so it runs in air-gapped deployments (where the
  external Spark/Trino executor tier does not).
