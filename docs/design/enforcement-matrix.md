# Access-governance enforcement matrix

Status: **Layer 1 (scan-plan enforcement) and Layer 4 (storage-scope floor)
implemented.** Layers 2 (compiled secure views) and 3 (native engine bridges)
are designed but not yet implemented — this document says exactly which
guarantee each path gives *today*, and marks the rest as future work. If
another document disagrees with this one on an enforcement guarantee, this one
wins.

This is the honesty document. Fine-grained access control on an open lakehouse
is **structurally limited by what each engine cooperates with**: the catalog
sits in the read path (table loads, credential vending, scan planning), but it
does not sit between an engine and the object store once that engine holds
file-level credentials. So Meridian does not claim uniform enforcement. It
ships a layered set of controls, and it states — per engine, per path —
whether the guarantee is **PREVENT** (the data cannot be read), **DETECT** (the
access is recorded and can be alarmed, but not blocked in-band), or **NONE**.
Auditors ask for exactly this table.

## The policy model (what is being enforced)

Governance policies (Pillar D, `docs/adr/009-cedar-abac.md`) are one of three
kinds, attached to a securable (table/namespace) or to a **tag** that a
table/column carries:

- **Row filter** — a predicate (`region = 'eu'`) that restricts which rows a
  principal sees.
- **Column mask** — a transform (null / hash / partial / drop) applied to a
  column's values, keyed on a column tag (`pii:email`).
- **ABAC rule** — a deny/allow decision from attributes: deny a `pii:high`
  table unless a *purpose* is declared, allow an owner, allow/deny a group,
  time-bound a grant.

The decision engine (`meridian-authz`, Cedar) is a pure function of
`(policies, principal, action, resource, context)`. It produces (a) an
allow/deny decision with a captured reason, and (b) the row filters and column
masks that apply. The **enforcement** of that decision is what this matrix is
about.

## The four layers

### Layer 1 — Scan-plan enforcement (strongest; implemented)

For engines that use **server-side scan planning** (the Iceberg 1.11 REST
planning surface: `POST .../plan`, `.../tasks`), Meridian applies the policy
*inside the plan the engine executes*:

- **Row filters** are compiled to the exact IRC residual-expression tree and
  **AND-ed into every returned scan task's `residual-filter`**, after
  partition-pruning folding so pruning can never drop them. A planning client
  is required by the spec to apply the residual to every row it reads, so
  filtered-out rows are never returned. → **PREVENT** (rows).
- **Column masks** strip the masked column's statistics (sizes, value/null/NaN
  counts, lower/upper bounds) from every returned data file, and the removed
  columns are recorded on the plan and in the audit trail. A masked column's
  values and value ranges never leave the catalog through the plan. → see the
  precise guarantee and its limit below.
- **ABAC deny** aborts planning with a 403 before any file is returned. →
  **PREVENT** (whole scan).

Every plan that changes what the caller sees writes a
`governance.scan.enforced` audit row (principal, table, applied policies,
removed columns, row-filter-applied, reason) — the decision is part of the
hash-chained audit trail. Purpose is declared with the `X-Meridian-Purpose`
request header (a Meridian extension; the IRC plan request carries no purpose
field).

Implemented in `crates/meridian-server/src/governance.rs` (the decision
bridge) and `crates/meridian-server/src/routes/planning.rs` (the injection at
`plan`, re-applied on inline `fetchPlanningResult` for the fetching principal;
async plans bake the enforcement into their stored pages at plan time).

**The precise column-mask guarantee on the scan-plan path, and its limit.**
A scan plan returns *file references and residuals*; it carries no projected
schema and cannot rewrite a value. So a column mask on this path is enforced by
**stripping the masked column's statistics** (sizes, value/null/NaN counts,
lower/upper bounds) from every returned data file, and recording the removed set
on the plan and in the audit trail. That means the column's *values and value
ranges never leak through plan metadata*, and every masked-plan decision is
audited. **What it does not do:** the plan does not project the column away
(there is no select list in the response — see the code comment at
`routes/planning.rs`, "`select` … does not change the response today"), the
column name is already known to the client from `loadTable`, and the underlying
Parquet still contains the column's bytes. So a client with scan access can
still read the raw values by projecting the column from the referenced files.
True value-level column prevention (including a "visible-but-masked" transform
like last-four-digits) is a **Layer 2** (compiled secure view) guarantee, not
yet implemented. For scan-plan clients the honest guarantee is:

- masked column's **statistics withheld** from the plan → values and ranges do
  not leak via plan metadata, and the decision is **audited** — but this is
  stats-withholding + DETECT, **not** value-level PREVENT;
- the byte-level bound that a broad-credential client cannot read the raw column
  is **not** provided at the plan level today — it comes from **Layer 4**
  (storage scope, table-prefix granularity) and, once shipped, **Layer 2**
  (view path). Do not read the plan-level column-mask cell as "the values are
  unreadable."

(The agent-gateway path (H-F2) is different: `mcp/context.rs` actually removes
masked columns from the returned schema, so on *that* surface a restricted
column is genuinely absent, name included. That is a governed-context tool, not
the IRC scan plan.)

### Layer 2 — Compiled secure views (designed; not yet implemented)

For every policied table, Meridian will maintain per-engine governed views
(auto-transpiled SQL via the SQLGlot subsystem, Pillar G) that embed the row
filter and column mask as SQL, and can withhold direct table grants so access
is forced through the view. This is where a *transforming* mask (hash, partial
reveal) is truly enforced as a returned-but-masked value, and where engines
that do **not** use scan planning (Spark, Trino via SQL) get prevention. Works
in principle on Trino / Spark / Snowflake / ClickHouse / StarRocks. **Status:
not implemented (M4b).** Until it lands, those engines are covered only by the
storage floor for direct-table access — stated plainly in the matrix.

### Layer 3 — Native engine bridges (designed; not yet implemented)

Policy compiled to an engine's own mechanism: a Trino OPA plugin fed by the
Meridian policy compiler, a Spark catalog plugin, Ranger policy export for
legacy estates. These give prevention inside a specific engine without the view
indirection. **Status: not implemented (M4b+).**

### Layer 4 — Storage-scope floor (always on; implemented via vending)

Vended credentials (AWS STS session policies scoped to the table prefix, GCS
downscoped tokens, Azure user-delegation SAS) and remote request signing bound
every engine's access to the **table's storage prefix**, regardless of whether
the engine cooperates with any higher layer. This is coarse — it is
table/prefix-level, not row/column-level — but it is universal: an engine that
ignores planning and reads files directly still cannot read *outside* the
tables it was vended. Remote signing additionally evaluates policy
**per-request** at the signer endpoint, which is per-file control for the
highest-security deployments.

Implemented in `crates/meridian-vending` and the vending / signing routes.
Policy-awareness at the vend boundary (deny a vend when a policy *fully* denies
the principal) is a cheap tightening tracked as a follow-up; the prefix bound
holds today regardless.

## The matrix (today)

Read this as: *for this engine on this access path, what does each policy kind
get?* "PREVENT" = the data cannot be read. "DETECT" = the access is audited and
can be alarmed, but is not blocked in-band. "NONE" = no control beyond the
storage floor. Every row also gets the **storage-scope floor** (Layer 4:
table-prefix boundary) — the table's rightmost column — because that is always
on.

| Engine / access path | Row filter | Column mask | ABAC deny | Storage floor (always on) |
|---|---|---|---|---|
| **DuckDB / PyIceberg / Daft — via server-side scan planning** | **PREVENT** (residual injected) | **DETECT + stats withheld**: column's stats stripped from the plan (values/ranges don't leak via metadata) and audited, but values remain readable — value-level PREVENT is Layer 2; byte bound is the storage floor | **PREVENT** (403 before files) | PREVENT (prefix bound) |
| **Any 1.11-planning client — via scan planning** | **PREVENT** | **DETECT + stats withheld** (as above) | **PREVENT** | PREVENT |
| **Trino — direct table SQL (no planning)** | Layer 2 (M4b); today **NONE** beyond floor | Layer 2 (M4b); today **NONE** beyond floor | Layer 3 OPA (M4b); today **NONE** beyond floor | PREVENT |
| **Spark — direct table SQL (no planning)** | Layer 2 (M4b); today **NONE** beyond floor | Layer 2 (M4b); today **NONE** beyond floor | Layer 3 plugin (M4b); today **NONE** beyond floor | PREVENT |
| **Snowflake / ClickHouse / StarRocks — via compiled view (Layer 2)** | PREVENT once M4b ships | PREVENT once M4b ships (transforming masks too) | PREVENT once M4b ships | PREVENT |
| **Any engine — direct object-store read with broad creds** | NONE | NONE | NONE | PREVENT (prefix bound) — the only control |
| **Every path** — is the decision **audited**? | Yes (scan-plan path emits `governance.scan.enforced`); direct-read paths are covered by vending/audit events | | | |

Notes that keep this honest:

1. **The scan-plan column-mask cell says "DETECT + stats withheld"**, not
   PREVENT, because the plan strips the column's *statistics* but does not
   project the column out (no select list in the response) — so a client with
   scan access can still read the raw values from the referenced files, and the
   column name is already known from `loadTable`. The strongest column guarantee
   available today is the storage floor (table-prefix) plus (once shipped) the
   Layer-2 view path; do not read the plan-level cell as "the values are
   unreadable by a determined broad-credential client."
2. **Direct-SQL rows for Trino/Spark are NONE-beyond-floor today**, not
   PREVENT. Compiled views (Layer 2) are what will make them PREVENT; until
   that lands, the honest statement is that a Trino user with a direct table
   grant sees unfiltered rows. The mitigation available now is to withhold
   direct table grants and route those engines through scan planning where
   they support it, or to accept the storage floor as the only bound.
3. **Nothing here is enforced by the object store itself** except the prefix
   boundary the vended credential encodes. Meridian is a policy decision point
   and a policy enforcement point *at the paths it mediates* (plan, vend,
   sign); it is not an inline proxy on the raw S3 GET path.

## Why the scan-plan layer is the strategic one

The scan-plan layer is the same mechanism Snowflake and Databricks reserve for
their walled gardens' cross-engine ABAC — implemented here from a neutral
catalog, on open files, for any engine that adopts the 1.11 planning surface.
Adoption of server-side planning is the lever: as more engines plan through the
catalog, more of this matrix moves to PREVENT without per-engine policy
re-implementation. That is the bet, and the matrix will be updated — truthfully
— as each engine and each layer lands.

## Verifying it

The scan-plan enforcement is covered end-to-end by
`crates/meridian-server/tests/governance_api.rs`: a fixture table with real
per-column statistics is tagged (`pii` on a column, a residency tag on the
table), a column-mask policy and a row-filter policy are bound to those tags,
and a **least-privileged viewer principal** (granted only READ) plans the
table. The test asserts, on the actual `/plan` response, that the masked
column's statistics are stripped from every task, that the row filter appears
as a residual on every task, that a `deny-unless-purpose` policy returns 403
without the purpose header and 200 with it, and that every decision left a
`governance.scan.enforced` audit row naming the policies and removed columns.

It is additionally proven against a **real client on real object storage** by
`conformance/e2e/tests/test_governance_enforcement.py`: a PyIceberg client
creates and writes a MinIO-backed table through Meridian, a mask + row filter
are bound, and the IRC server-side scan-plan request a planning client issues
(`POST .../plan`) is asserted to return the masked column absent from every
task's statistics and the row filter injected as a residual — the wire contract
any 1.11-planning client (DuckDB's iceberg extension, PyIceberg REST planning)
consumes.
