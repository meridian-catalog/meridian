# 006. Catalog-as-code bundles (`meridian plan` / `meridian apply`)

## Status

Accepted

## Context

Operators provision a Meridian catalog's control plane — warehouses,
namespaces, roles, grants, webhooks — imperatively today, either through the
`/api/v2` management API and the Iceberg REST surface directly, or through the
matching `meridian warehouse|namespace|role|grant` subcommands. That works for a
first setup but does not fit how infrastructure is actually operated: there is no
single reviewable artifact for "what the catalog should contain," no way to diff
intended against actual, and no idempotent way to re-assert the intended state
from CI. This is the gap declarative infrastructure tooling (Terraform,
Kubernetes manifests, GitOps) exists to close.

Forces in play:

- **Secrets must not live in the file.** Webhook signing secrets and storage
  credentials cannot be committed to version control.
- **Some resources are immutable in the API.** Warehouses (storage root and
  options) and roles (description) have no update endpoint; namespaces have a
  properties-update endpoint; grants have no mutable fields at all.
- **The catalog is multi-writer.** Engines (Spark, Trino, Flink, pyiceberg, dbt)
  create namespaces and tables and commit snapshots continuously; operators
  create ad-hoc grants. A declarative tool cannot assume it is the only source of
  truth.
- **The public API is the only contract.** The console and any external tooling
  talk exclusively to `/api/v2` and `/v1`; catalog-as-code must not introduce a
  privileged back channel.

## Decision

We will add a versioned YAML **bundle** format and two CLI subcommands,
`meridian plan -f` and `meridian apply -f`, implemented entirely in
`meridian-cli` on top of the existing public APIs.

**Scope: control plane only.** The bundle declares warehouses, namespaces,
roles, grants, and webhooks. It deliberately excludes **tables and views**.
Those are owned by engines through the Iceberg REST protocol; their
authoritative state is Iceberg metadata that changes on every write. They are
data, not configuration — declaring them would fight engines for ownership and
any snapshot committed between `plan` and `apply` would invalidate the plan. The
bundle stops at the boundary the catalog itself draws: containers and policy,
not their mutable contents.

**Format.** A Kubernetes-style header (`apiVersion: meridian.dev/v1`,
`kind: CatalogBundle`) plus five optional resource lists. Unknown fields are
rejected so typos fail fast. Every string value supports `${ENV_VAR}`
interpolation, resolved at parse time, so secrets stay out of the file; an
undefined variable is a hard error (fail closed).

**Semantics: converge-forward only.** `apply` creates absent resources and
updates drifted resources *that have an update path* (namespace properties,
additively). It is idempotent — re-applying an unchanged bundle is a no-op — and
it never deletes.

- **Prune is out of scope for v1.** A server resource absent from the bundle is
  reported as `would-delete` and never removed, because a bundle is rarely the
  whole truth and an accidental prune is unrecoverable. Prune warnings are
  emitted for warehouses/roles/webhooks; namespaces and grants are excluded
  because engine- and operator-created ones are expected.
- **Immutable drift is reported, not reconciled.** Where the API has no update
  path (warehouse storage, role description), drift surfaces as `would-update`
  and `apply` warns without failing.
- **Idempotency leans on the server where needed.** For grants on
  namespace/table/view securables, the securable's internal id is not exposed by
  any read endpoint, so `plan` cannot pre-check existence and shows them as
  `create`. `apply` relies on the server's duplicate-grant conflict, which it
  treats as a no-op — so re-apply never duplicates and never fails.

**Alternatives considered.** (1) A Terraform provider — deferred; it is a heavier
artifact and the CLI covers the GitOps loop with no extra runtime. A provider can
later wrap the same bundle semantics. (2) Including tables/views with a
"managed" flag — rejected; it re-introduces the ownership conflict above. (3)
Full prune/delete — rejected for v1 on the unrecoverable-mistake risk; can be
added later behind an explicit `--prune` opt-in.

## Consequences

- Operators get a reviewable, version-controllable catalog definition and an
  idempotent, CI-gatable `apply` (non-zero exit on any resource failure).
- The tool adds one dependency to `meridian-cli`, `serde_yaml` (already declared
  at the workspace level in anticipation of this work). No new runtime service.
- Because it is pure CLI over the public API, the security posture is unchanged:
  `apply` can do exactly what its bearer token is authorized to do, and every
  mutation is audited under the caller's principal like any other API call.
- The excluded-by-design surface (tables/views) and the out-of-scope prune are
  documented user-visible limitations, not bugs. A future ADR can revisit prune
  behind an explicit flag, or a Terraform provider built on the same format.
- Grant plan output for namespace/table/view securables is imprecise (`create`
  even when present); this is a known cost of the API not exposing securable
  ids, mitigated by server-side idempotency. Exposing a securable-id read
  endpoint later would let `plan` diff these precisely.
