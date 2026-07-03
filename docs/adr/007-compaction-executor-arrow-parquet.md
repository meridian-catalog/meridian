# 007. Build the compaction executor on arrow + parquet directly, not DataFusion

## Status

Accepted

## Context

Pillar C (autonomous table operations) needs a built-in executor that rewrites
a table's small data files into fewer, larger ones — bin-pack compaction — and
returns the result as a normal Iceberg `replace` commit for the commit path to
apply. This is the first maintenance operation and the flagship's engine; its
correctness bar is exact row conservation (rows in equal rows out, minus any
materialized merge-on-read deletes) with Iceberg field-id fidelity preserved
through the rewrite.

The executor lives in a new crate, `meridian-executor`. It depends only on
`meridian-iceberg` (manifest read/write, the metadata model) and
`meridian-storage` (object-store bytes). The open question was what to read and
write the Parquet row data with. Three options were on the table:

1. **DataFusion.** A full embedded query engine (SQL/DataFrame, optimizer,
   execution) built on the same `arrow`/`parquet` crates. The spec names
   DataFusion as the built-in executor's engine in several places (§8.1, C-F4),
   because later maintenance operations — sort-based and z-order compaction,
   SQL data-quality checks (Pillar E), the workbench executor (Pillar L) — want
   real query execution.
2. **`iceberg-rust` (the `iceberg` crate).** The community Rust Iceberg
   implementation, which has its own reader/writer stack.
3. **`arrow` + `parquet` directly.** Read each input file to Arrow
   `RecordBatch`es, realign columns by field id, apply delete filters, concat,
   and write one Parquet file back — no query engine, no second metadata model.

Constraints that shaped the choice:

- **`meridian-iceberg` is our metadata model, and it is the source of truth.**
  The commit path, the manifest engine, the scan planner, and the expression
  layer are all built on it. A rewrite must produce manifests and a snapshot
  that this crate — not a foreign one — writes and reads, so the compacted
  commit is byte-identical in shape to any other Meridian commit and round-trips
  through the same code the engine matrix already exercises. Pulling in a second
  Iceberg implementation (`iceberg-rust`) to do the rewrite would mean two
  metadata models in one binary, with two notions of "a manifest entry" to keep
  in agreement — exactly the divergence risk §8.3 (the commit path is sacred)
  exists to avoid.
- **Bin-pack compaction is not a query.** It concatenates rows and drops deleted
  ones. It needs no join, no aggregation, no optimizer, no SQL. Field-id column
  realignment, schema-evolution null synthesis, and position/equality-delete
  application are row-filter and projection operations over Arrow arrays.
- **Dependency weight is a first-class cost here** (§8.1: single static binary,
  predictable p99s; principle 3: every optional dependency needs written
  justification). DataFusion is a large dependency tree. Adding it to get
  `concat + filter + write` would be paying for an engine to do array plumbing.
- **The correctness we must guarantee is at the array level.** "Row count in
  equals row count out" and "field id 3 in this input maps to field id 3 in the
  output regardless of physical column order" are assertions over `RecordBatch`
  columns. Owning the read → realign → filter → concat → write pipeline directly
  makes those invariants explicit in our code and asserted in our tests, rather
  than delegated into an engine's execution plan where they are harder to state
  and to prove.

## Decision

We will build the compaction executor directly on the `arrow-*` and `parquet`
crates, with no query engine, for this first bin-pack operation. Data files are
read to Arrow `RecordBatch`es, projected to the table's current schema **by
Iceberg field id** (reading the `PARQUET:field_id` column metadata, never by
name or position), filtered by any attached position/equality deletes,
concatenated, and written back as a single target-sized Parquet file whose
columns carry their field ids in the footer. All Iceberg metadata — the new
manifests, manifest list, snapshot, `TableUpdate`s and `TableRequirement`s — is
produced by `meridian-iceberg`, the same code the rest of the catalog uses.

We deliberately do **not** adopt `iceberg-rust`: it would duplicate the metadata
model that `meridian-iceberg` already owns and that the commit path requires be
authoritative.

We pin the `parquet` feature set to `arrow` plus the compression codecs engines
actually emit — `snap` (Snowflake/PyIceberg often write Snappy) and `zstd`
(Spark's default) — and pin `arrow-array`/`arrow-schema`/`arrow-select` to the
same major version as `parquet` so the Arrow types line up across the read and
write halves.

This decision is scoped to **bin-pack compaction**. It is explicitly revisited
when the executor gains an operation that is a real query:

- **Sort-based and z-order compaction** need a sort (and, for z-order, a
  space-filling-curve key computation) over merged rows. A sort large enough to
  spill wants an execution engine.
- **SQL data-quality checks** (Pillar E, E-F2) are SQL by definition.
- **The workbench executor** (Pillar L) runs user SQL.

When the first of those lands, adopting DataFusion (the spec's named choice, and
built on the very `arrow`/`parquet` crates this ADR already depends on, so it is
additive, not a rewrite) gets its own ADR. Bin-pack does not need it, and paying
for it now would violate principle 3 for no correctness or capability gain.

## Consequences

**Easier.**

- One Iceberg metadata model in the binary. The compacted commit is produced by
  the same `meridian-iceberg` writers as every other commit, so it round-trips
  through the same readers and the engine matrix covers it for free.
- The correctness invariants are stated and asserted where they happen: row
  conservation and field-id realignment are checks over `RecordBatch` columns in
  `rewrite.rs`, asserted per bin-pack group in `compact.rs`, and verified
  end-to-end (read the output back, compare a sorted projection) in the test
  suite — including files whose physical column order is reversed and files that
  predate a column (schema evolution → synthesized null column).
- A smaller, faster-compiling dependency tree on the maintenance path, matching
  the single-static-binary and predictable-latency goals.

**Harder / deferred.**

- Sort and z-order compaction are not reachable with this stack; they wait for
  the DataFusion decision. Bin-pack ships first regardless (it is the operation
  with universal pain and measurable ROI), so this sequences the work rather
  than blocking it.
- Output-file splitting (one over-target merged group into several files) is
  modelled in the types but not implemented in this first cut: a group currently
  produces one output file. Because inputs are already below target and packed
  toward it, over-target output is rare; splitting is a follow-up.
- Deletion **vectors** (v3 Puffin blobs) cannot be materialized with a plain
  Parquet reader. The manifest reader preserves their offsets, and the executor
  **refuses** an input carrying an attached deletion vector with a clear reason
  rather than silently dropping it. Position-delete *files* and equality-delete
  *files* — the v2 merge-on-read shape engines emit today — are fully applied.
  DV materialization is scoped in [`docs/design/compaction.md`](../design/compaction.md).

**Neutral.**

- Column statistics (lower/upper bounds) are computed from the merged output
  rows for the primitive types with an unambiguous Iceberg single-value
  encoding, and written into both the Parquet footer and the manifest `DataFile`.
  Types without such an encoding get no bound (spec-legal: a reader treats an
  absent bound as unknown). Record, value, and null counts are always exact.
  A future engine-backed rewrite could widen bound coverage, but exact counts
  are what most planning depends on.
