# 009. Cedar as the ABAC policy engine, in a standalone `meridian-authz` crate

## Status

Accepted

## Context

Pillar D (cross-engine access governance) begins with **D-F1: the policy
model** — row filters, column masks, tag-driven ABAC (policies attached to
tags like `pii:high`), purpose-based access, and time-bound grants — evaluated
by an embedded, policies-as-code engine. The founding spec (§6, D-F8/D-F1)
names **Cedar** for this, and the enforcement matrix (D-F2) needs a decision
layer that produces two things:

1. an **allow/deny decision** with a captured *reason*, because governance
   decisions are audited and, per the spec, **the audit trail is the product**
   (D-F2, §8.9); and
2. the **row filters and column masks** that apply to a `(principal, table)`
   pair, which the server-side scan planner (A-F5) injects into returned scan
   tasks — the residual-expression seam already left in
   `meridian_server::planning::apply_row_policy_seam` explicitly for this.

RBAC already exists (`meridian_store::rbac`: `Privilege` / `SecurableScope` /
`authorize`). ABAC is a *separate, additive* layer: RBAC decides whether a
principal has base access to a securable; ABAC then applies attribute- and
tag-driven constraints on top (deny `pii:high` unless a purpose is granted;
filter rows; mask columns). The two are composed by the server, not conflated.

Three questions had to be settled before code:

1. **Which engine?** The candidates were Cedar (AWS, Rust-native, formally
   verified evaluator), Open Policy Agent / Rego (ubiquitous but a separate
   process or a heavy embed, and a Turing-complete language whose decisions are
   harder to reason about), and a hand-rolled tag→predicate evaluator.
2. **Where does it live, and what does it depend on?** The spec's crate layout
   (§12.2) lists `meridian-authz` as "RBAC + Cedar ABAC + external bridges".
   The question was whether the ABAC engine should depend on the store (to read
   principals/tags/policies) or be a pure decision library the store feeds.
3. **What is the deny model, given ABAC sits on top of RBAC?** A naive
   deny-by-default ABAC layer would deny every table that has no ABAC policy —
   wrong, because RBAC is the base gate and most tables carry no ABAC rule.

## Decision

### We use Cedar (`cedar-policy` crate, v4.11), as the spec directs

Cedar is Rust-native (no sidecar process, no FFI — it embeds directly on the
tier-0 path), Apache-2.0 (compatible with our OSS core), and has an explicit,
formally specified evaluation semantics with the property we need most:
**forbid-overrides-permit** (deny wins over allow), which is exactly the
"deny-overrides-allow" the spec requires (D-F1). Its policies are declarative
and analyzable (unlike Rego's general-purpose logic), its `@id`/`@description`
annotations give us human-readable audit reasons for free, and its `datetime`
extension (a default feature) implements time-bound grants without us inventing
a time model. Cedar also ships a schema-based **validator**, which gives us the
"detect errors before save" that D-F1 asks for.

Rego/OPA was rejected for the core decision path: it is either a separate
process (violates the "Postgres is the only required dependency" principle,
§12.1, and adds a hop on the hottest authz path) or a large embed with a
Turing-complete policy language whose decisions cannot be statically validated
the way Cedar's can. OPA remains relevant as an **external-authorizer bridge**
(D-F8.3) for orgs standardized on it — that is a separate, later work item and
does not change the core engine choice.

### `meridian-authz` is a pure, database-free decision library

The crate depends on **nothing but `cedar-policy`, `meridian-iceberg` (for one
output type, below), and small utility crates** — never on `meridian-store` or
`meridian-server`. It defines its **own input types**: `AuthzPrincipal`
(id, kind ∈ {user, service, agent}, groups, roles, purpose, environment, extra
attributes), `AuthzResource` (id, kind ∈ {namespace, table, view, column},
tags, owner, classification), `Action` (read/write/commit/create/drop/alter/
manage), and `RequestContext` (time, purpose, session attributes). A later wave
maps store rows (`principals`, `grants`, `tags`, `policies`) onto these; this
crate never reaches into the store.

This is a deliberate **type boundary**, documented here so it is not eroded:
*this crate owns the enforcement-decision vocabulary* (`Decision`, `RowFilter`,
`ColumnMask`, `Enforcement`, `AbacRule`); *the store owns the persistence
vocabulary*. Where the two must meet — a stored policy row becomes a Cedar
decision, a stored tag rule becomes an enforced filter — the mapping is the
store's responsibility, against these public types.

The engine maps its inputs to a **fixed Cedar entity model**: principal kinds
become entity types `User`/`Service`/`Agent` (so a policy can scope a whole
class, e.g. "no `Agent` may read `pii:high`"); resource kinds become
`Namespace`/`Table`/`View`/`Column`, with a `Column` linked as a Cedar child
(`in`) of its parent so `resource in Table::"…"` policies work; actions become
`Action::"…"` entities; and the request context carries `now` (a Cedar
`datetime`), `now_millis`, and `purpose`. This model is published as a
`cedarschema` document and used to validate policies. Every entity id is built
with Cedar's `from_type_name_and_id` constructor (not string interpolation), so
arbitrary ids — names with quotes, dots, spaces — cannot break escaping, and
every literal that the tag→policy compiler emits is escaped through a JSON
string encoder for the same reason (a fuzz/property test asserts that no rule
input, including Cedar metacharacters, can produce unparseable or
meaning-changed policy text).

### The decision carries its reason, for the audit trail

`authorize(principal, action, resource, context) -> Decision` returns not a
bare bit but `Decision { effect, determining_policies, reason, errors }`. The
`determining_policies` are the Cedar policies that decided the outcome (the
`forbid`s that fired for a deny, the `permit`s for an allow), each enriched with
its `@id`/`@description` annotations; `reason` is a human-readable sentence
built from them (*"denied by `pii-high-deny` (pii:high denies read unless a
matching purpose is granted)"*). A policy that **errors** during evaluation
(e.g. reads an attribute an entity lacks) never grants access — an erroring
`permit` simply does not fire (fail closed) — but the error is captured in
`errors` and surfaced in the reason so a misauthored policy is visible rather
than silent. The store persists `determining_policies` + `reason` on the same
transaction as the access they authorize.

### Deny model: an explicit, configurable base effect

Because ABAC composes with RBAC, the engine is constructed with a `BaseEffect`:

- **`AllowUnlessForbidden`** (default) — an implicit baseline `permit` is
  evaluated, so a policy set of tag-driven `forbid`s only ever *subtracts*
  access. A table with no ABAC `forbid` is allowed by this layer, and RBAC
  remains the gate. This is what the tag→policy convenience compiler targets and
  what the server uses when it has already run RBAC.
- **`DenyUnlessPermitted`** — Cedar's native deny-by-default; access requires a
  matching `permit`. Used when the ABAC policy set is the complete decision.

Either way, `forbid` overrides `permit`, so a deny is never lost.

### A tag→policy convenience layer, compiled to Cedar

Most catalog rules are a few shapes (D-F1), so `AbacRule` captures them as data
— `TagDenyUnlessPurpose`, `OwnerAllow`, `GroupAllow`/`GroupDeny`,
`TimeBoundAllow`, `TagRowFilter`, `TagColumnMask` — and compiles each to Cedar
policy text. The **same rule value** drives both the Cedar decision and the
enforcement resolution, so "what the policy says" and "what is enforced" cannot
drift. Generated policies are validated against the schema, so a convenience
rule cannot produce something the validation gate would reject.

### Row-filter / column-mask resolution feeds the scan-plan seam

`resolve_filters_and_masks(principal, table, columns, rules) -> Enforcement`
answers D-F2's "which filters and masks apply". A `RowFilter` compiles to
`meridian_iceberg::expr::Expression` — *the exact IRC scan-filter tree* that
`apply_row_policy_seam` folds into every returned `FileScanTask` residual —
which is the one reason this crate depends on `meridian-iceberg`: it keeps the
policy→plan path lossless rather than re-encoding predicates twice. Layered row
filters are AND-ed; column masks collapse to the strongest per column
(Drop > Hash > Null > Custom > Partial), with an unresolvable custom mask
treated as Drop (fail closed). A `ColumnMask` names the column and the mask
kind; the scan-plan route strips or rewrites masked columns in the projection.

### Honesty about scope (this is the decision layer, not the enforcement)

This crate **decides** and **resolves**; it does not itself enforce across
engines. Cross-engine enforcement is the D-F2 matrix, delivered by later waves,
and each layer's guarantee is different and must be stated truthfully in the
enforcement docs (auditors ask):

- **Scan-plan enforcement** (this crate's `Expression`/mask output injected by
  the planner) *prevents* leakage for engines that use server-side planning
  (DuckDB, PyIceberg, Daft, 1.11-planning adopters).
- **Compiled secure views** *prevent* leakage where direct table grants are
  withheld and access is forced through the view.
- **Native bridges** (Trino OPA, Ranger export) and the **storage-scope floor**
  (vended-credential/remote-signing prefix boundaries) give coarser,
  engine-dependent guarantees.

An engine that reads files directly with broad credentials and ignores planning
is bounded only by the storage floor — the docs will say so plainly. Nothing in
this crate should be read as a claim that a policy is enforced everywhere; it is
the neutral, well-tested *source* of the decision that each enforcement path
consumes.

## Consequences

**Positive.**

- The whole decision path is a **pure function** of `(policies, principal,
  action, resource, context)` — the only ambient input, wall-clock time, is
  passed in explicitly — so a decision is deterministic and can be replayed
  exactly for audit. It is covered by heavy unit and property tests (deny
  overrides allow, pii-unless-purpose, owner/group/time-bound, layered
  filter/mask resolution, malformed-policy rejection, determining-policy/reason
  capture, escaping/injection resistance) with **no database required**.
- The scan-plan seam gets its filters/masks in the exact type it already
  consumes; wiring it in wave 2 is a mapping job, not a re-implementation.
- Cedar's validator turns the "detect errors before save" requirement into a
  library call, and its annotations turn "capture the reason" into free
  metadata.

**Negative / costs.**

- A new dependency on `cedar-policy` and its transitive tree (it is a
  substantial crate). Justified by the spec's explicit choice and by the cost of
  the alternatives (a sidecar process, or a hand-rolled evaluator we would have
  to prove correct ourselves on a security-critical path).
- Cedar's MSRV (1.89) is ahead of the workspace's declared `rust-version`
  (1.88). The toolchain in use satisfies it; if the declared MSRV is enforced in
  CI, it must be raised to ≥ 1.89 (a follow-up, noted so it is not a surprise).
- The fixed Cedar entity/schema model is now a compatibility surface: adding a
  principal/resource attribute means updating both the engine's entity assembly
  and the published schema in lockstep (a test asserts the schema parses; the
  two are documented as needing to move together).

**Neutral / follow-ups.**

- The store→authz mapping, policy persistence, and the actual scan-plan wiring
  were explicitly **wave 2** — now landed. `meridian_store::{policy,tags}` own
  the persistence (versioned `policies` with append-only `policy_versions`,
  tags + column-level assignments, and the resolvers that answer "which
  policies/tags apply to this table"); `meridian_server::governance` is the
  decision bridge that deserializes stored definitions into `AbacRule`s,
  evaluates the gate, resolves filters/masks, and the scan planner injects the
  result (row-filter residual + column removal) and audits every decision. The
  per-path guarantee is documented in
  [`docs/design/enforcement-matrix.md`](../design/enforcement-matrix.md). This
  crate still deliberately stops at the decision boundary; the wiring lives in
  the server, against these public types, exactly as designed.
- External-authorizer bridges (OPA endpoint, OpenFGA sync, Ranger import) from
  D-F8.3 are separate later work; the core engine choice here does not preclude
  them.
- If policy evaluation ever shows up as a hot-path cost, Cedar policy sets are
  cheap to cache per workspace; the engine holds a parsed `PolicySet` and is
  cloneable, so a decision cache is a later optimization, not a redesign.
