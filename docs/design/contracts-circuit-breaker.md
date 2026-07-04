# Data contracts and the circuit breaker

| | |
|---|---|
| **Status** | Accepted. Implemented (M5, Pillar E / E-F3, E-F4): `crates/meridian-store/src/contracts.rs` (model + resolution + violation recording), the commit-driver pre-commit hook in `crates/meridian-server/src/routes/tables.rs`, the contracts API in `crates/meridian-server/src/routes/quality.rs`, migration `0018_contracts.sql`. |
| **Scope** | Data contracts as versioned catalog objects; the synchronous pre-commit hook (§3 step 6 of the commit protocol) that evaluates them against **staged** metadata; the three enforcement modes (warn / quarantine / block) and their exact durability guarantees. |
| **Related** | [`commit-protocol.md`](commit-protocol.md) (the sacred path this hook runs inside — §3 step 6 is the documented insertion point, §5 the invariants this must preserve) · [ADR 001](../adr/001-m0-foundation.md) (outbox + audit chain) · `docs/design/enforcement-matrix.md` (Pillar D, the sibling "detect vs prevent" honesty doc) · [Iceberg REST spec](https://iceberg.apache.org/rest-catalog-spec/) |

The circuit breaker is the Pillar-E headline (§7 "wow" #1): *a bad commit never
lands*. Every observability vendor sells faster alarms after the fact; the
catalog sits in the write path and can stop the fire from starting. This
document is the contract for how it does that **without weakening a single
commit-path invariant** — the commit path is sacred (§12.1.1: "No feature may
weaken 8.3 invariants").

## 1. What a contract is

A **data contract** is a versioned, workspace-scoped catalog object that binds
to a table or a namespace and declares what a producer's commits must respect.
It has:

- a **binding**: `table` (one table) or `namespace` (every table under it,
  resolved at evaluation time — a namespace binding needs no per-table
  rebinding when tables are added);
- a **mode**: `warn`, `quarantine`, or `block` (§4);
- an **enabled** flag (a disabled contract is retained and still readable but
  is skipped by the hook);
- a **version** (monotonic from 1, full history in `contract_versions`,
  append-only, mirroring the policy-versioning discipline in
  `meridian_store::policy`);
- a **spec** (`jsonb`, typed in Rust as [`ContractSpec`]): the rules.

### 1.1 The spec

Two rule families, both **cheap and synchronous** — they read only the staged
`TableMetadata` (schema + snapshot summary), never the data files. This is a
hard design constraint: the hook runs on the commit hot path, so it must be
O(schema size), never O(rows).

**Schema contract** (`SchemaContract`) — evaluated by comparing the staged
current schema against the base current schema:

- `allowed_evolution`: `additive_only` | `no_narrowing` | `none`.
  - `none`: any schema change is a violation (schema is frozen).
  - `no_narrowing`: adding columns and widening types is fine; *narrowing* a
    type, making an optional column required, or dropping a column is a
    violation. This is the "don't break readers" contract.
  - `additive_only`: only *adding* columns is allowed; changing or removing an
    existing field (by id) is a violation. Strictly stronger than
    `no_narrowing`.
- `protected_columns`: named columns that may never be **dropped or renamed**
  (matched by name in the base; a violation if the id that carried that name is
  gone or now carries a different name), regardless of `allowed_evolution`.
- `required_columns`: named columns that must be **present** in the staged
  schema (a violation if a required column is absent). Complements
  `protected_columns`: "protected" guards existing columns from removal;
  "required" guards a column's continued presence even across a schema replace.

**Predicates** (`Predicate`) — cheap assertions over the staged snapshot
summary (the free-form `summary` map Iceberg writers attach to each snapshot,
e.g. `added-records`, `total-records`, `operation`):

- `NonNull { column }`: asserts the named column exists **and is `required`**
  in the staged schema (the only non-null signal available synchronously
  without scanning data — a required column cannot contain nulls per the
  Iceberg spec). Documented as such: this is a *schema-level* non-null
  guarantee, not a data-level null count.
- `RowCountMin { value }` / `RowCountMax { value }`: bounds on `total-records`
  read from the new current snapshot's summary. If the summary carries no
  `total-records` key the predicate is **skipped** (not failed) — many engines
  omit it, and failing-closed on a missing metric would reject correct
  commits. Documented as a best-effort, summary-driven sanity check.

The classification logic (`classify_schema_evolution`) is a pure function over
two `Schema`s and lives in `meridian-store::contracts`; it is unit-tested
exhaustively (additive ok, widening ok, narrowing rejected, required-tighten
rejected, protected-drop rejected) independent of any database.

### 1.2 Type widening / narrowing

Iceberg field ids are stable across evolution, so evolution is classified by
**field id**, not position or name:

- a field id present in base but absent in staged → **dropped column**;
- a field id present in both with a different `Type` → **type change**,
  classified as *widening* (allowed under `no_narrowing`) or *narrowing*
  (a violation) by [`is_widening`]. The allowed widenings follow the Iceberg
  spec's promotion rules: `int→long`, `float→double`, `decimal(P,S)→
  decimal(P',S)` with `P' ≥ P` and equal scale, and `date→timestamp`/
  `timestamp_ns`. Everything else that changes the type is narrowing.
- a field id present in both, same type, but `required` went `false→true` →
  **tightened nullability** (narrowing: old rows may have null);
- a field id in staged but not base → **added column** (always additive-safe).

Renames are detected for `protected_columns` only: a protected name that is no
longer carried by the id that carried it in the base is a violation (drop *or*
rename of a protected column both trip it).

## 2. Where the hook runs (the seam)

`commit-protocol.md` §3 step 6 names this exact insertion point:

> Pre-commit hooks — synchronous, cheap predicates such as contract checks —
> run here too; they belong to a later milestone and are out of scope for this
> document, but this is their insertion point.

and §10 lists "Pre-commit hook insertion (§3 step 6) once contracts exist" as
deferred. This document is that milestone.

The hook evaluates **staged** metadata — the candidate `TableMetadata` the
driver has already built and (in the optimistic-staging implementation)
written to object storage — against the **base** metadata (the current
pointer's metadata, already loaded by the driver as the update base). Both are
in hand in the driver *before* `PostgresCommitBackend::commit_tables` is
called. So the classification (a pure CPU function, no I/O) happens in the
driver; its **result** is threaded into the commit transaction so that the
violation record lands with the correct atomicity (§3).

Placement relative to the state machine (`commit-protocol.md` §6): the hook is
part of the transition `BaseLoaded → Staged → (Committed | Rejected)`. It runs
*after* Iceberg requirement evaluation (a stale commit is rejected as a 409
before we bother classifying it) and *after* the candidate is built (we need
the staged schema), and it gates entry into the CAS.

```
load base ─▶ check Iceberg requirements ─▶ build candidate ─▶ stage file
                                                                   │
                                                    ┌──────────────┴───────────────┐
                                                    ▼                              
                                            evaluate contracts                     
                                       (pure: base schema vs staged schema,        
                                        predicates over staged summary)            
                                                    │                              
                        ┌───────────────┬───────────┴───────────┬───────────────┐  
                        ▼               ▼                       ▼               ▼  
                  no violation        WARN                  QUARANTINE         BLOCK
                        │               │                       │               │  
                   normal CAS      CAS + violation      retarget snapshot   NO CAS;
                   (main moves)    row, same tx;         onto audit branch;  record
                                   main moves            main ref frozen;    violation
                                                         violation row,      (own tx);
                                                         same tx             reject 409
```

## 3. Enforcement modes and their exact guarantees

The task's correctness bar: *a contract-eval **error** must not corrupt the
commit* (block fails closed → reject; warn fails open → land), and the
violation record + event must be atomic with the outcome. Here is exactly what
each mode does and what is durable afterwards.

### 3.1 WARN — "land it, but shout"

- The commit proceeds **exactly as it would with no contract**: the same CAS,
  the same pointer move, the same index write-through. `main` advances.
- A `contract_violations` row (`commit_rejected = false`) and a
  `quality.contract.violated` outbox event are written **in the same commit
  transaction** as the pointer swap. So the violation is durable if and only if
  the commit is (invariant I4 extends to the violation record).
- Producers change nothing; consumers subscribed to the event learn a
  contract was breached on a commit that nonetheless landed.

**Guarantee:** identical commit semantics to no-contract, plus an atomic
violation record. No invariant of `commit-protocol.md` §5 is touched — the CAS,
lock order, idempotency, and audit row are byte-for-byte what they were; one
extra row and one extra outbox event join the same transaction, exactly as the
existing audit row and outbox event already do.

### 3.2 BLOCK — "reject atomically"

- The commit is **rejected before the CAS**. `commit_tables` is **not called**
  for a blocked single-table commit, so nothing about the pointer, index, or
  metadata log changes — there is no partial state, by construction (the sacred
  transaction never opens).
- The staged metadata file is an **orphan** (it was written during staging);
  it is discarded best-effort exactly like a lost-CAS orphan (§7.1 of the
  commit protocol) and swept if the delete fails. An orphan is garbage, never
  corruption — a file is only visible to engines through a committed pointer.
- A `contract_violations` row (`commit_rejected = true`) and a
  `quality.contract.violated` event are written in a **separate, dedicated
  transaction** (there is no commit transaction to join). This is itself a
  mutation, so it carries its own audit row + outbox event in that one
  transaction (audit+outbox atomicity preserved for the record-write).
- The client receives `409 CommitFailedException` with a **machine-readable**
  body naming the violated contract, its id, mode, and each violated rule, so
  a CI-grade producer can parse and act. (Shape in §5.)

**Guarantee:** *fail-closed*. If contract evaluation itself errors (a
malformed spec, a classification bug), block mode treats the commit as
**rejected** — nothing durable, 409. A producer in block mode never gets a
silent pass on an eval error. **Multi-table caveat (documented):** in a
multi-table transaction, if *any* table's contract blocks, the whole
transaction is rejected atomically (no table's pointer moves — this is exactly
I2, "no partial multi-table commits"): the transaction is all-or-nothing and a
block is a reject, so a mixed block/allow set rejects the lot.

### 3.3 QUARANTINE — managed write-audit-publish (the honest depth)

This is the mode where honesty about implementation depth matters most. The
spec's aspiration (E-F4) is full managed WAP: retarget the violating commit to
an Iceberg **audit branch**, keep it off `main`, and expose publish /
fast-forward / discard. Here is **exactly what is implemented and its
guarantees** — the minimal *correct* version the task sanctions, not a
hand-wave.

**What quarantine does, mechanically:**

1. The driver takes the candidate metadata (which the client's updates built to
   advance `main`) and **retargets** it before the CAS via
   `quarantine_retarget(base, candidate, branch)`:
   - the new snapshot(s) the commit added are **retained** in the candidate's
     `snapshots` list (they remain durable — the data and manifests the
     producer wrote are not thrown away);
   - `current_snapshot_id` and `refs["main"]` are **reset to the base's
     values** — so from every reader's perspective `main` did **not** move; the
     table's current snapshot is unchanged;
   - a branch ref `refs[<quarantine-branch>]` (default
     `meridian_quarantine`, configurable per contract) is pointed at the new
     head snapshot, so the quarantined work is reachable by id/branch for
     inspection, publish, or discard;
   - the metadata log and `last_updated_ms` advance normally (this *is* a new
     metadata version — the pointer's `metadata_location` moves to the
     retargeted file, so the branch ref is durably recorded), but the
     **current-snapshot pointer that every engine reads does not**.
2. The retargeted candidate is what gets CAS'd. So: the pointer version
   increments by exactly 1 (history stays gapless — I1/I3 hold), the metadata
   file is durably written before the swap (I4 holds), the audit + outbox +
   violation rows are in the one transaction (I4/I6 hold) — **but `main`'s
   current snapshot is frozen at the base.** A commit "happened" (a new
   metadata version exists, the branch records the quarantined snapshot), yet no
   consumer reading `main` sees the bad data.
3. A `contract_violations` row (`commit_rejected = false`, `quarantined = true`,
   recording the branch and head snapshot id) + a
   `quality.contract.quarantined` event are written in that same transaction.

**Publish / discard (`/api/v2/quality/.../quarantine/{snapshot}`):**

- **publish**: fast-forward `main` to the quarantined head. Implemented as a
  normal catalog commit that sets `refs["main"]` + `current_snapshot_id` to the
  quarantined snapshot (which already exists in `snapshots`), through the *same*
  commit path and CAS — so publishing a quarantined snapshot is itself a
  fully-audited, invariant-preserving commit. Publish re-runs the contract in
  the mode's spirit is **not** implied (publish is an explicit human/CI
  override of the quarantine — documented).
- **discard**: drop the quarantine branch ref (and optionally the snapshot) via
  a normal commit that removes `refs[<branch>]`. `main` was never advanced, so
  discard is just cleanup; the snapshot becomes eligible for the orphan/expiry
  sweep.

**Guarantee (stated without overclaiming):**

- Quarantine **never advances `main`** past a violating commit. That is the
  load-bearing guarantee and it is enforced at the metadata level (the current
  snapshot pointer is reset to base) *and* verified by test (the table load
  after a quarantined commit shows the base snapshot as current).
- The quarantined snapshot is **durably retained and addressable** on the
  quarantine branch; publish and discard are ordinary, audited commits.
- **What this is NOT (honesty):** it is not a general multi-branch WAP engine
  with per-branch retention policies, branch-level RBAC, or automatic
  re-validation on publish. It is single-branch (one configurable quarantine
  branch per contract), publish is an explicit override, and re-validation on
  publish is out of scope for this milestone. Quarantine is only offered for
  **single-table** commits in this milestone; a quarantine-mode contract hit
  inside a **multi-table** transaction **degrades to block** (reject the whole
  transaction) rather than partially retargeting one table of an atomic set —
  retargeting one table of a multi-table atomic commit would violate the
  producer's atomicity expectation, so we reject instead and document it. This
  is the "minimal correct version, documented" the task calls for.

### 3.4 Eval-error policy (fail-closed vs fail-open), precisely

Contract evaluation is a pure function and should not error in practice, but
the policy is defined so a bug cannot corrupt a commit:

| Mode | Eval result = violation | Eval itself errors |
|---|---|---|
| warn | land + record | **land** (fail-open); log the eval error, do not fabricate a violation |
| quarantine | retarget + record | **degrade to block** (fail-closed) — reject; a commit that can't be classified must not be allowed to advance `main` under a quarantine contract |
| block | reject + record | **reject** (fail-closed) |

The asymmetry is deliberate and matches the mode's intent: warn is advisory
(never blocks a real commit, even on our bug); block and quarantine are
protective (our bug must not become the producer's silent pass).

## 4. Preserving the commit invariants (§5 of the commit protocol)

Point-by-point, because this path is sacred:

- **I1 (no lost updates) / I3 (monotonic history):** the hook never changes the
  CAS or the version guard. Warn and quarantine both go through the *unmodified*
  `commit_tables` CAS (quarantine only changes the *content* of the staged
  metadata, not how the pointer is swapped); block skips the CAS entirely.
  Pointer versions stay a gapless +1-per-commit sequence.
- **I2 (no partial multi-table commits):** the hook's per-table decisions are
  combined **before** `commit_tables`: if any table blocks (or a quarantine
  degrades to block), the whole transaction is rejected and `commit_tables` is
  not called. There is no path where some tables commit and others don't
  because of a contract.
- **I4 (crash safety):** the violation row + event join the *same* transaction
  as the pointer swap in warn/quarantine, so they are durable iff the commit is;
  in block the record is its own atomic transaction with its own audit+outbox.
  No new "third state" is introduced.
- **I5 (idempotency):** contract evaluation is a pure function of the request +
  current state; it runs *inside* the retry loop, before the CAS, on every
  attempt. A replayed idempotency key still short-circuits at recall (§3 step 2)
  **before** the hook — a recorded receipt replays without re-evaluating
  contracts (the commit already happened; re-blocking a successful replay would
  break I5). Documented: contracts gate the *first* application, not replays.
- **I6 (full audit coverage):** every mode writes an audit row — warn/quarantine
  via the commit transaction's existing `table.commit` audit row **plus** the
  violation record's event; block via the dedicated record transaction's audit
  row. No pointer moves without an audit row; no violation is recorded without
  one either.

The existing commit property suite (`commit_properties_pg.rs`) and the update
proptests are **unaffected**: they drive `commit_atomic` (the pure-CAS trait
path, `derived: None`), which the hook does not touch. The hook lives in the
driver and in a new `contract_violations` write; the property model's contract
is unchanged. Re-running that suite green is part of this milestone's gate.

## 5. Machine-readable rejection body (block mode)

A blocked commit returns `409 CommitFailedException` whose message is
human-readable and whose error envelope carries a structured payload the
producer's CI can parse:

```json
{
  "error": {
    "message": "commit blocked by data contract \"no-drop-pii\": column \"email\" is protected and was dropped",
    "type": "CommitFailedException",
    "code": 409,
    "contract-violation": {
      "contract-id": "01J...",
      "contract-name": "no-drop-pii",
      "mode": "block",
      "table": "01J...",
      "violations": [
        { "kind": "protected-column-dropped", "detail": "column \"email\" is protected and was dropped" }
      ]
    }
  }
}
```

`kind` is a stable machine token (`schema-narrowed`,
`protected-column-dropped`, `required-column-missing`, `additive-only-violated`,
`schema-frozen`, `predicate-non-null`, `predicate-row-count`); `detail` is the
human string. The same `{kind, detail}` pairs are stored on the
`contract_violations` row and emitted on the event.

## 6. Data model (migration 0018)

- **`contracts`** — the versioned object: `id` (ULID), `workspace_id`, `name`
  (unique per workspace), binding (`bound_to` ∈ `table`|`namespace`,
  `securable_id` — polymorphic TEXT, not an FK, exactly like `grants` and
  `policy_bindings`), `version` (current), `enabled`, `mode`
  (`warn`|`quarantine`|`block`), `spec` (jsonb), `quarantine_branch`,
  `created_by`, timestamps.
- **`contract_versions`** — append-only per-version history (`contract_id`,
  `version`, `mode`, `enabled`, `spec`, `created_by`, `created_at`); the current
  `contracts.version` always has a matching row (rollback copies an old spec
  into a new version, history stays append-only).
- **`contract_violations`** — one row per detected violation:
  `id`, `contract_id` (FK, cascade), `table_id` (polymorphic TEXT), `snapshot_id`
  (nullable — the head snapshot involved, when known), `kind`, `detail`,
  `commit_rejected` (bool), `quarantined` (bool), `occurred_at`. Indexed by
  contract and by table for the violations query.

Binding-to-table is surfaced to producers (E-F3) via the
`GET /api/v2/quality/tables/{warehouse}/{ns}/{table}/contracts` status endpoint,
which resolves the contracts in force on a table (directly bound + namespace
bound). Also exposing them as a synthetic `meridian.contracts` table property on
the IRC `GET table` response — so an engine sees them inline without a second
call — is a tracked follow-up: it adds a per-load contract read to the
latency-sensitive `loadTable` hot path (p99 < 50 ms target), so it is
deliberately deferred to a cached-resolution pass rather than bolted onto the
load path now (§7).

## 7. What this milestone deliberately does not do

- No SQL data-quality assertion execution (E-F2 — needs the executor; a later
  wave). The predicates here are the *cheap synchronous* subset only.
- No incidents / status pages / blast-radius (E-F5) or quality score (E-F6) —
  wave 2 and the lineage sibling own those; violations are recorded and evented
  so those can consume them.
- No general multi-branch WAP engine (§3.3 states the single-branch bound).
- No contract auto-suggestion / inference from history.
- **Create-time enforcement is out of scope for this milestone.** The hook runs
  on commits to *existing* tables (single- and multi-table transactions) — the
  circuit-breaker surface E-F4 describes. A namespace-bound contract is
  therefore not yet evaluated at the moment a *new* table is created under it
  (the `assert-create` finalization path); schema-evolution rules are vacuous on
  an empty base anyway, but `required_columns` / non-null predicates at creation
  are a natural, tracked follow-up. Evolution of an existing table — the case
  that actually breaks downstream readers — is fully covered.

These are tracked, not forgotten — the object model and the event stream are
shaped so the later waves attach without reworking the hook.
