# Small-scan query executor

The `meridian-query` crate is Meridian's built-in query engine for **small
scans**: it runs governed `SELECT`s over the Iceberg tables the catalog owns,
in-process, for the agent gateway's `run_sql` tool (Pillar H, H-F3) and the
workbench (Pillar L, L-F1). Big scans route to registered customer engines; this
engine deliberately stays small (spec §8.1). It is built on DataFusion —
see [ADR 010](../adr/010-datafusion-small-scan-executor.md) for why DataFusion,
why governed views, and the arrow-version note.

This document is the contributor's map of how a query flows through the crate.

## The contract

```rust
meridian_query::run(
    sql: &str,
    tables: &[GovernedTable<'_>],  // each: CatalogTable + resolved Enforcement
    caps: Caps,
) -> Result<QueryOutput, QueryError>
```

The executor is a **pure function** of *(metadata + bytes + policy + SQL +
caps)*. It does not resolve table names, load metadata, open storage, or resolve
policy — the caller (a server route) does all of that and hands in:

- a `CatalogTable` per table the query may reference: the query-visible `name`,
  the table's `TableMetadata`, and a `&dyn Storage` to read bytes through;
- the `Enforcement` (row filters + column masks) resolved for the querying
  principal against that table, via `meridian_authz::resolve_filters_and_masks`;
- `Caps`: the scanned-bytes / scanned-rows / result-rows limits.

It returns a `QueryOutput`: JSON `rows` (arrow-free), the result `columns`, a
`provenance` record (tables + snapshot ids read, policies applied, columns
masked), and `bytes_scanned` / `rows_scanned` (the pre-execution estimate).

Keeping the boundary this narrow is what makes the crate testable against
in-memory Iceberg fixtures and free of any dependency on the server or the agent
gateway. The gateway will own an async `QueryExecutor` trait and adapt it onto
`run` (ADR 010, Consequences).

## The pipeline

`run` executes in a fixed order — the `run_sql` contract, **validate → estimate
→ refuse-or-execute** (H-F3):

1. **Validate (`executor::ensure_read_only`).** Parse the SQL with DataFusion's
   parser; require exactly one `Query` statement. DML, DDL, `COPY`, `SET`, and
   multi-statement input are refused (`QueryError::NotReadOnly`). The gate is
   fail-closed: any statement kind not explicitly recognized is refused, so a
   governed query can never mutate.

2. **Resolve + estimate (`catalog::resolve_scan`).** For each table, walk the
   current snapshot's manifest list, collect live data files (skipping `DELETED`
   entries, inheriting sequence numbers), split data files from delete files, and
   attach each delete file to the data files it covers by the spec's scope rules.
   Sum on-disk bytes and record counts from the manifest — the cost estimate.
   This mirrors `meridian-server::planning` and `meridian-executor::select`; the
   three must agree on what "a live file" is. An empty table (no current
   snapshot) is a valid, zero-row scan.

3. **Cap (`executor::run`).** If the summed estimate exceeds `caps.max_scan_bytes`
   or `max_scan_rows`, refuse **before reading any data file**
   (`QueryError::ScanTooLarge` / `TooManyRows`). The message names the escape
   hatch (a registered engine) so an agent can relay it.

4. **Read (`reader::build_table`).** For each table, read its live Parquet data
   files to Arrow batches and realign to the table's current schema **by Iceberg
   field id**:
   - columns map by field id (from `PARQUET:field_id` metadata), never by name or
     position — a renamed column keeps its id, a physically reordered file merges
     correctly;
   - a file predating a column simply lacks that id, so a null column of the
     target type is synthesized (schema evolution);
   - attached position/equality **delete files** are materialized (rows removed) —
     a governed query must never surface a deleted row;
   - a v3 **deletion vector** cannot be applied with a plain Parquet reader, so a
     data file carrying one is refused (`DeletionVectorUnsupported`);
   - a field id present in a file but absent from the schema is refused
     (`UnmappableField`) rather than guessed.

   The result is one DataFusion `MemTable` per table, registered under a private
   name (`__meridian_raw__<name>`).

5. **Govern (`policy::build_governed_view`).** Compile each table's `Enforcement`
   into a DataFusion **view** with the query's name, over the private raw table:
   - the projection lists the table's columns; a masked column is replaced by its
     mask expression, a **dropped** column is omitted entirely (absent, not
     nulled — H-F2, so restricted schema cannot be probed);
   - the row filter (a closed `RowPredicate` AST) is folded into a `WHERE`,
     rendered to fixed SQL shapes with escaped string literals and quoted
     identifiers;
   - a `Custom` mask we cannot verify against this engine fails **closed** to a
     drop, exactly as the scan-plan path treats an unresolvable custom mask.

   The user's SQL references the view, so enforcement is executed by DataFusion's
   own planner — the same Pillar-D machinery scan planning uses, no second
   implementation.

6. **Execute + return.** Run the validated user SQL against the governed views,
   capped at `max_result_rows + 1` (to detect truncation). Serialize the Arrow
   result batches to JSON rows (`result::batches_to_rows`), assemble provenance,
   and return.

## Module map

| Module        | Responsibility |
| ------------- | -------------- |
| `executor`    | The `run` orchestration, the read-only gate, and the public `GovernedTable` input. |
| `catalog`     | `CatalogTable` input; resolving a snapshot into live data files + attached deletes + the size estimate (the scan planner's live-entry and delete-scope rules). |
| `reader`      | Reading Parquet data files to Arrow by field id, synthesizing nulls, applying deletes, mapping Iceberg types to Arrow, building the `MemTable`. |
| `policy`      | Compiling `Enforcement` into a governed view: mask expressions, row-filter SQL, safe literal/identifier rendering, fail-closed rules. |
| `result`      | Arrow batches → `(columns, rows)` as the arrow-free public shape. |
| `types`       | `QueryOutput`, `Provenance`, `Caps`, `Column`, `TableSnapshot` — the public API types. |
| `error`       | `QueryError` — caller-facing refusals (`ScanTooLarge`, `InvalidSql`, `NotReadOnly`) vs. operational faults, with `is_caller_refusal()`. |

## Governance guarantees (and how they are tested)

- **Row filters restrict rows** — even a `SELECT *` sees only permitted rows
  (`tests/executor.rs::row_filter_policy_restricts_rows`); multiple filters are
  AND-ed (`multiple_row_filters_are_conjoined`).
- **Masks hide values** — partial reveal, stable hash
  (`column_mask_partial_reveals_only_prefix`, `column_mask_hash_is_stable...`).
- **Drops hide existence** — a dropped column is absent from `SELECT *` and
  referencing it by name is a clean error, not a null column
  (`column_mask_drop_makes_column_absent_not_null`).
- **Custom masks fail closed** and **literals are injection-safe** — direct
  policy unit tests in `src/policy.rs` (`custom_mask_fails_closed_to_drop`,
  `string_literal_is_escaped_against_injection`).
- **Oversized scans are refused before I/O**
  (`oversized_scan_is_refused_up_front`, `row_cap_is_refused_up_front`).
- **Field-id mapping, schema-evolution nulls, and delete materialization** are
  each exercised against real Parquet + manifest fixtures.

## Scope and limits

- **Small-scan only.** Tables are read fully into memory; correctness holds
  within the cap. Predicate/projection pushdown into the Parquet read (scanning
  less than the whole file) is a future optimization.
- **Deletion vectors** are refused; v2 position/equality delete *files* are
  applied.
- **Types**: the primitive Iceberg types map to Arrow; nested/variant/geo types
  are refused with a clear reason (a small scan does not need them yet).
- The crate does **not** touch server routes, the audit chain (the calling route
  audits the agent action, H-F4), or the commit path.
