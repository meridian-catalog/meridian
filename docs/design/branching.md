# Branching & Data CI/CD (Pillar K)

> Status: implemented for K-F1 (catalog-level branches & tags), K-F2 (branch-as-catalog
> projection), and K-F3 (merge gates + ephemeral branches). This document is the design
> of record; it extends `commit-protocol.md` without weakening any of its invariants.

Meridian branches are **catalog-level**: a branch is a set of divergent table-metadata
pointers spanning selected namespaces, not a ref inside one table's `metadata.json`.
This is the "Nessie whitespace, governed" of §6 K-F1, and it is deliberately distinct
from — and composable with — the *intra-table* Iceberg branch that the circuit breaker's
managed WAP writes (`contracts-circuit-breaker.md`, `TableMetadata::quarantine_retarget`).
Both use Iceberg `SnapshotRef`s; a catalog branch operates one level up, at the pointer.

## 1. Why pointer-level, and why it is zero-copy

The catalog already stores, per table, a pointer `(metadata_location, pointer_version)`
that the commit protocol advances by compare-and-set (`commit-protocol.md` §2). A branch
is a **named overlay of that pointer map**:

- Creating branch `dev` over some namespaces allocates *no per-table state*. Every table
  on `dev` initially resolves to `main`'s pointer — the branch is a name plus a base ref.
- The first time a commit lands on `dev` for table `T`, a `branch_table_pointers` row is
  written for `(dev, T)`, seeded from `T`'s live main pointer, then advanced. From that
  point `T` on `dev` diverges; every *other* table on `dev` still falls through to main.

So a branch shares base metadata until a table diverges — zero-copy by construction. No
metadata.json is duplicated on branch creation; divergence copies nothing but a pointer
row (the underlying data/manifests are shared until a branch commit writes new ones, and
even then only the branch's own new snapshots are branch-private).

## 2. Data model (migration 0025)

```
catalog_branches
  id             ULID, pk
  workspace_id   FK workspaces
  name           branch name, unique per workspace (with kind)
  kind           'branch' | 'tag'
  base_ref       the ref this diverged from ('main' or another branch name)
  base_branch_id nullable FK catalog_branches (resolved base, when base_ref != 'main')
  state          'open' | 'merged' | 'deleted'   (branches)   / always 'open' for tags
  scope_all      bool — true = spans every namespace in the workspace
  expires_at     nullable — ephemeral PR branches (K-F3); a sweeper deletes past this
  created_by, created_at, updated_at
  UNIQUE (workspace_id, name)        -- one namespace for branches AND tags

branch_namespaces                    -- when scope_all = false, the selected namespaces
  branch_id      FK catalog_branches
  namespace_id   FK namespaces
  PRIMARY KEY (branch_id, namespace_id)

branch_table_pointers                -- the divergent pointers (the whole point)
  branch_id      FK catalog_branches
  table_id       FK tables
  metadata_location  TEXT NOT NULL
  pointer_version    BIGINT NOT NULL >= 0      -- branch-local CAS guard
  base_pointer_version BIGINT NOT NULL          -- main's version at first divergence
                                                --   (the merge-base; conflict detection)
  previous_metadata_location TEXT
  created_at, updated_at
  PRIMARY KEY (branch_id, table_id)

catalog_tags                         -- tags are immutable; a tag row per pinned table
  tag_id         FK catalog_branches (kind='tag')
  table_id       FK tables
  metadata_location TEXT NOT NULL     -- frozen pointer at tag-creation time
  snapshot_id    BIGINT               -- the current snapshot pinned (for diff/report)
  PRIMARY KEY (tag_id, table_id)
```

A **tag** reuses `catalog_branches` with `kind='tag'` for the name registry, plus a
`catalog_tags` row per table capturing the frozen pointer. Tags are immutable: no commit
path targets a tag; `warehouse@tag` loads are read-only (a commit against a tag prefix is
rejected `CommitFailedException`, like a foreign asset).

`pointer_version` on a branch pointer is a *branch-local* counter starting at 0 for the
first branch commit, independent of main's counter. `base_pointer_version` records what
main's pointer_version was when the table first diverged onto the branch — the merge-base
used by conflict detection (§6).

## 3. The pointer target abstraction (commit-invariant preservation)

The commit protocol is sacred (`commit-protocol.md` §3, engineering principle 1). Pillar K
does **not** fork it. Instead the pointer store gains a *target*:

```rust
enum PointerTarget {
    Main,                       // tables.metadata_location / tables.pointer_version
    Branch { branch_id: String }, // branch_table_pointers row for (branch, table)
}
```

`PostgresCommitBackend::commit_tables` takes the target and routes every step to the right
row, but the **sequence is byte-for-byte the same** as main's (see `commit.rs` module docs
steps 1–8):

| Step | Main (unchanged) | Branch |
|---|---|---|
| lock | `SELECT … FROM tables WHERE id = ANY ORDER BY id FOR UPDATE` | lock the **base table row** `FOR UPDATE` (serializes first-divergence + drop races), then the `branch_table_pointers` row if present |
| idempotency | `idempotency_keys` in-tx check | identical, key namespaced by target |
| guard | `pointer_version == expected` | branch pointer's `pointer_version == expected` (or, on first divergence, main's `pointer_version == expected_base`) |
| CAS | `UPDATE tables SET pointer_version+1, metadata_location=… WHERE id AND pointer_version=expected` | `INSERT … ON CONFLICT` / `UPDATE branch_table_pointers SET pointer_version+1, … WHERE branch_id AND table_id AND pointer_version=expected` |
| index | `write_snapshot_index` (main only) | **skipped** — the write-through search/health index tracks main; branch snapshots are not indexed as the table's current state (documented limitation, §9) |
| audit+outbox | one row each, shared `commit_id` | identical, resource = `branch:{id}/table:{id}`, event `table.branch_committed` |
| receipt | `idempotency_keys` insert | identical |
| COMMIT | point of no return, `StateUnknown` on failure | identical |

The invariants carry over **because the mechanism is the same**:

- **I1 no lost updates.** The branch pointer has its own `pointer_version` guarded by the
  same `WHERE pointer_version = expected` CAS under the same `FOR UPDATE` lock. Two
  concurrent commits to the same `(branch, table)` serialize on the row lock; the loser's
  guard fails → `VersionConflict` → rebase-retry, exactly as on main.
- **Main is never touched by a branch commit.** A branch commit writes only
  `branch_table_pointers`; `tables.metadata_location` / `tables.pointer_version` are not in
  its `UPDATE` set. Proven by test: commit to `warehouse@dev` → main pointer_version and
  metadata_location unchanged; branch pointer advanced.
- **First-divergence race.** The first branch commit for a table both reads main's pointer
  (to seed) and inserts the branch row. It takes the base `tables` row `FOR UPDATE` first,
  so a concurrent main commit to the same table serializes behind it — the seed reads a
  consistent main pointer, and the branch insert's `PRIMARY KEY (branch_id, table_id)` is
  the backstop against two racing first-divergence inserts (loser retries and now sees the
  row, taking the ordinary branch-CAS path).
- **I4 durability = audit+outbox.** The branch commit's audit row and outbox event join the
  same transaction as the pointer write, so a branch change is visible iff its audit row and
  event exist — the same guarantee main carries.
- **Idempotency.** Branch commits use the same `idempotency_keys` table; the request
  fingerprint already includes the IRC `{prefix}` (which for a branch is `warehouse@branch`),
  so a replay on a branch prefix is distinct from the same body on main.

There is exactly **one** function that moves any table pointer, on main or a branch, and it
is `commit_tables`. That is the property engineering principle 1 protects, and Pillar K
keeps it.

## 4. Branch-as-catalog projection (K-F2 — the wow)

Every IRC engine speaks to a `{prefix}`. Today `{prefix}` = warehouse name. Pillar K makes
the resolver recognize a branch suffix:

```
warehouse            -> (warehouse, ref = Main)
warehouse@dev        -> (warehouse, ref = Branch "dev")
warehouse@q2-close   -> (warehouse, ref = Tag "q2-close")   [read-only]
```

`resolve_warehouse` becomes `resolve_prefix`, splitting on the last `@`. The base warehouse
resolves exactly as before; the suffix resolves to a `catalog_branches` row scoped to the
same workspace. The resolved **catalog ref** threads through `load_table`, `create_table`,
and `commit_table`:

- **loadTable on `warehouse@dev`:** resolve the table's pointer *through the branch overlay*
  — if `(dev, table)` has a `branch_table_pointers` row, read that `metadata_location`;
  otherwise fall through to `tables.metadata_location` (main). Return that metadata. A plain
  PyIceberg client pointed at `warehouse@dev` sees the branch state and never knows branching
  exists.
- **commit on `warehouse@dev`:** the commit driver targets `PointerTarget::Branch`. The
  candidate is staged and CAS'd against the branch pointer (first commit diverges the table).
  main does not move.
- **loadTable on `warehouse` (main):** unchanged — reads `tables.metadata_location`.

The `@` character is not valid in a warehouse name (warehouse names are validated on create),
so the suffix is unambiguous. `/v1/config?warehouse=warehouse@dev` also resolves, so engines
that bootstrap via `config` get the branch prefix wired into their catalog automatically.

This is the trick that makes catalog branching universally consumable: **no engine needs a
branching API**. Spark, Trino, Snowflake-CLD, DuckDB, PyIceberg — all read/write a branch by
pointing at `warehouse@branch`.

## 5. Diff (K-F1)

`GET /api/v2/branches/{name}/diff` compares a branch against its base ref (default `main`).
For every table that has diverged on the branch (`branch_table_pointers` rows) — and every
table that was dropped/added on the branch — it reports:

- **schema delta:** added / dropped / type-changed columns (current schema of branch vs base).
- **snapshot delta:** branch head snapshot id vs base head snapshot id; number of snapshots
  ahead.
- **row-count delta:** `total-records` from the two heads' snapshot summaries when present
  (best-effort — the summary is engine-provided; absent → reported as `unknown`, never
  fabricated, per engineering principle 7).

Only diverged tables appear; a table that falls through to main has, by definition, no delta.

## 6. Merge (K-F1) with conflict detection

`POST /api/v2/branches/{name}/merge` fast-forwards `main` from the branch, **table by table**,
with table-level conflict detection:

For each diverged table `(branch, T)`:

1. **Conflict check (three-way):** the merge base is `base_pointer_version` (main's version
   when T first diverged). If main's *current* `pointer_version` for T is still
   `base_pointer_version`, main has not moved since divergence → **fast-forward is safe**.
   If main advanced (`current > base_pointer_version`), both sides changed T → **conflict**
   (table-level). File-level overlap analysis is future work (§9), so a moved-on-both-sides
   table is conservatively a conflict.
2. If any table conflicts, the merge is **refused atomically** (409, machine-readable list of
   conflicting tables) — no table is merged. Fail-closed.
3. If none conflict, the merge applies each table's branch pointer to main **through the
   commit path**: a `commit_tables(PointerTarget::Main)` per table, guarded by main's current
   `pointer_version` (the same CAS). This means a merge is just a batch of ordinary main
   commits whose new metadata is the branch head — it inherits every commit invariant,
   including the circuit breaker if a contract is bound to the target table. A concurrent
   main commit that lands between the conflict check and the merge CAS makes the guard fail →
   the merge reports the table as newly-conflicting rather than clobbering (no lost update).

After a successful merge the branch is marked `merged`.

## 7. Merge gates — Data CI/CD (K-F3)

Before a merge, Meridian runs the branch's **merge gate**: the Pillar E contracts and the
Pillar E quality checks bound to the affected tables must pass *on the branch head*. The gate
reuses `contracts::resolve_for_table` + `ContractSpec::evaluate` (the same engine the circuit
breaker uses at commit time) — evaluated against each diverged table's branch-head metadata:

- If a **block-mode** contract is violated on any table, the merge is refused (409, the
  violated contract + violations) before any pointer moves — a bad branch cannot be promoted.
- Warn-mode violations are reported in the merge response but do not block.

This is "merge gates = contracts + checks must pass on the branch" (K-F3). `meridian branch
merge` surfaces the gate result; a CI job can call the gate check endpoint
(`GET /api/v2/branches/{name}/gate`) to get a pass/fail before attempting the merge — the
dbt/SQLMesh blue-green recipe (integration notes below).

**Ephemeral branches:** a branch created with `--expires-in` (or `expires_at`) is a PR
environment. A sweeper (documented, invoked by `meridian branch sweep` and by the maintenance
worker cadence) deletes branches past `expires_at`, dropping their `branch_table_pointers`
(the shared base metadata is untouched; only branch-private new snapshots become orphans,
cleaned by ordinary orphan cleanup on the branch's staged files).

### dbt / SQLMesh integration notes

- **dbt blue-green:** point the dbt profile's catalog at `warehouse@pr-$PR_NUMBER`. dbt builds
  models onto the branch (zero-copy over prod). CI runs `meridian branch gate pr-$PR_NUMBER`;
  on green, `meridian branch merge pr-$PR_NUMBER` fast-forwards prod; on teardown,
  `meridian branch delete pr-$PR_NUMBER`. No dbt plugin needed — it is just a catalog name.
- **SQLMesh:** the same, using SQLMesh's `catalog` connection setting per environment; a
  SQLMesh `prod` promotion maps to a Meridian merge.

## 8. API & CLI surface

Management API (`/api/v2/...`, management-gated like the rest):

```
POST   /api/v2/branches                      create branch (name, base_ref, namespaces?, expires_in?)
GET    /api/v2/branches                      list branches + tags
GET    /api/v2/branches/{name}               get one (with diverged-table count)
DELETE /api/v2/branches/{name}               delete a branch
GET    /api/v2/branches/{name}/diff          schema+snapshot+row-count delta vs base
GET    /api/v2/branches/{name}/gate          merge-gate result (contracts on branch head)
POST   /api/v2/branches/{name}/merge         merge to main (conflict + gate checked)
POST   /api/v2/branches/sweep                delete expired ephemeral branches
POST   /api/v2/tags                          create tag (name, from_ref)
GET    /api/v2/tags                          list tags
DELETE /api/v2/tags/{name}                   delete a tag
```

IRC surface: **no new endpoints** — the branch is projected through the existing table
endpoints via the `warehouse@branch` prefix. That is the whole point of K-F2.

CLI:

```
meridian branch create <name> [--base <ref>] [--namespace <ns>...] [--expires-in <dur>]
meridian branch list
meridian branch diff <name> [--base <ref>]
meridian branch gate <name>
meridian branch merge <name>
meridian branch delete <name>
meridian tag create <name> [--from <ref>]
meridian tag list
meridian tag delete <name>
```

## 9. Documented limitations (honest docs, principle 7)

- **Conflict detection is table-level.** A merge where both sides changed the same table is a
  conflict even if the changes touch disjoint files. File-level overlap analysis is future
  work; until then the conservative table-level rule never produces a wrong (silently-merged)
  result — it only refuses some merges a finer analysis would allow.
- **Branch snapshots are not in the search/health write-through index.** The metadata-forward
  index (`8.2`) tracks each table's *main* current state. A table's branch-head snapshots are
  addressable (loadTable through the branch prefix reads the branch metadata.json directly)
  but do not appear in `table_snapshots` or feed search/health/monitors. Merging to main
  indexes the merged state normally (the merge runs the main commit path, which write-through
  indexes).
- **Merge gate covers contracts + declared quality checks, not ad-hoc SQL assertions run on a
  live engine.** The commit-time circuit breaker and the branch merge gate share the contract
  evaluation engine; heavier compute-pushed checks (E-F2) are out of the gate's synchronous
  path.
- **Policies (Pillar D) are enforced on branch reads/commits identically to main** — the
  branch prefix resolves to the same warehouse, namespaces, and table records, so RBAC and the
  governance layer apply unchanged. A branch is not a governance escape hatch.
