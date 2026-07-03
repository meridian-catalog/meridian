# Catalog as code: `meridian plan` and `meridian apply`

Meridian's control-plane objects — warehouses, namespaces, roles, grants, and
webhooks — can be declared in a versioned YAML **bundle** and reconciled onto a
running server with two commands:

```sh
meridian plan  -f catalog.yaml     # read-only diff: what would change
meridian apply -f catalog.yaml     # converge the server toward the bundle
```

This is the GitOps workflow for a catalog: keep `catalog.yaml` in version
control, review changes as pull requests, and run `apply` from CI. `apply` is
idempotent — re-applying an unchanged bundle does nothing — and it **never
deletes**.

Both commands talk only to the public APIs (`/api/v2/*` management and the
Iceberg REST surface `/v1/*`); there is no privileged back channel. Anything
`apply` can do, you could do by hand with `curl`.

## Quick start

```yaml
# catalog.yaml
apiVersion: meridian.dev/v1
kind: CatalogBundle

warehouses:
  - name: analytics
    storage_root: s3://acme-lake/analytics
    storage_options:
      region: us-east-1
      endpoint: https://s3.us-east-1.amazonaws.com

namespaces:
  - warehouse: analytics
    levels: [sales, emea]          # a two-level namespace
    properties:
      owner: data-platform

roles:
  - name: analyst
    description: Read-only analytics access

grants:
  - role: analyst
    privilege: READ
    securable:
      type: warehouse
      warehouse: analytics

webhooks:
  - url: https://hooks.acme.example/meridian
    event_types: [com.meridian.table.committed]
    secret: ${MERIDIAN_WEBHOOK_SECRET}   # sourced from the environment
```

```sh
export MERIDIAN_WEBHOOK_SECRET=…            # keep secrets out of the file
meridian plan  -f catalog.yaml --server https://catalog.acme.internal --token "$TOKEN"
meridian apply -f catalog.yaml --server https://catalog.acme.internal --token "$TOKEN"
```

`--server` defaults to `http://127.0.0.1:8181`. `--token` is required when the
server runs `auth.mode = "oidc"`.

## The bundle format

A bundle is a single YAML document with a Kubernetes-style header and up to five
resource lists. Every list is optional; a bundle may declare any subset.

| Field | Required | Value |
| --- | --- | --- |
| `apiVersion` | yes | `meridian.dev/v1` |
| `kind` | yes | `CatalogBundle` |
| `warehouses` | no | list of [warehouses](#warehouses) |
| `namespaces` | no | list of [namespaces](#namespaces) |
| `roles` | no | list of [roles](#roles) |
| `grants` | no | list of [grants](#grants) |
| `webhooks` | no | list of [webhooks](#webhooks) |

Unknown fields are rejected, so a typo (`storage_roots:` instead of
`storage_root:`) fails fast instead of being silently ignored.

### Warehouses

```yaml
warehouses:
  - name: analytics               # natural key; also the Iceberg REST prefix
    storage_root: s3://acme-lake/analytics
    storage_options:              # non-secret options; secrets via ${ENV}
      region: us-east-1
```

The `name` is the natural key. Warehouses are **immutable after creation** in
the API — there is no endpoint to change a storage root or options. If the
bundle asks to change one, `plan` reports it as `would-update` and `apply`
warns without acting (see [drift with no update path](#drift-with-no-update-path)).

### Namespaces

```yaml
namespaces:
  - warehouse: analytics
    levels: [sales, emea]         # outermost first
    properties:
      owner: data-platform
```

`(warehouse, levels)` is the natural key. For a multi-level namespace, `apply`
creates any missing **ancestor** namespaces first (`[sales]` before
`[sales, emea]`), because Iceberg requires the parent to exist. Ancestors are
created without properties; only the declared leaf gets the `properties` you
list.

Namespace properties are the one control-plane field with a real update path
(the IRC `updateProperties` endpoint), so property drift is reconciled in
place. The update is **additive**: `apply` sets the keys you declare and leaves
any other keys — set by an engine or an operator — untouched. It never removes
a property the bundle does not mention.

### Roles

```yaml
roles:
  - name: analyst
    description: Read-only analytics access
```

The `name` is the natural key. Like warehouses, a role's `description` is
immutable in the API; changing it in the bundle is a `would-update` warning, not
a reconciled change. Built-in roles (e.g. `admin`) are never touched.

### Grants

```yaml
grants:
  - role: analyst                 # exactly one of role / principal
    privilege: READ               # READ, COMMIT, CREATE_TABLE, …
    securable:
      type: warehouse             # warehouse | namespace | table | view
      warehouse: analytics
      namespace: [sales, emea]    # required for namespace/table/view
      table: orders               # required for type: table
      view: monthly               # required for type: view
```

A grant has no mutable fields: it is identified entirely by
`(grantee, privilege, securable)` and either exists or does not. `apply` creates
it if absent; an identical existing grant is a no-op.

`plan` diffs warehouse-scoped grants precisely. For namespace-, table-, and
view-scoped grants it plans an *idempotent create*: the securable's internal id
is not exposed by any read endpoint, so `plan` cannot pre-check existence and
shows the grant as `create` even when it is already present. `apply` still
converges correctly — the server deduplicates, and a duplicate comes back as a
no-op — so re-apply never creates a second grant and never fails. If you want a
grant to diff cleanly to `noop`, scope it to a warehouse.

### Webhooks

```yaml
webhooks:
  - url: https://hooks.acme.example/meridian
    event_types: [com.meridian.table.committed]   # empty = all events
    secret: ${MERIDIAN_WEBHOOK_SECRET}             # write-only on the server
```

`(url, event_types)` is the natural key. The signing `secret` is write-only:
the server never returns it, so it cannot be part of a diff. A webhook with a
matching url and event-type filter is a no-op; a new combination is created.

## Secrets: `${ENV_VAR}` interpolation

Any string value in the bundle may reference an environment variable with
`${NAME}`. References are resolved when the bundle is read, so secrets —
webhook signing secrets, storage credentials — stay out of the committed file:

```yaml
secret: ${MERIDIAN_WEBHOOK_SECRET}
storage_options:
  secret-access-key: ${AWS_SECRET_ACCESS_KEY}
```

Rules:

- `${NAME}` expands to the value of `NAME`. An **undefined** variable is a hard
  error — the tool fails closed rather than sending an empty secret.
- `$${` is a literal `${` escape.
- `NAME` follows the POSIX shape `[A-Za-z_][A-Za-z0-9_]*`.

## Reconciliation model

`apply` is **converge-forward only**. It performs exactly two kinds of change:

- **create** a declared resource that is absent, and
- **update** a declared resource that has drifted *and has an update path*
  (namespace properties).

It never deletes, and it never touches a resource the bundle does not declare.

### Deletes are out of scope (v1)

A resource that exists on the server but is not in the bundle is reported by
`plan` as `would-delete` and is **never deleted** by `apply`. Pruning is out of
scope for v1 for two reasons:

1. A bundle is rarely the whole truth. Engines create namespaces and tables;
   operators create ad-hoc grants and roles. Treating "absent from the bundle"
   as "delete me" would destroy legitimate, unmanaged state.
2. An accidental prune of a production warehouse is unrecoverable.

`would-delete` warnings for warehouses, roles, and webhooks are surfaced so you
can see the divergence; namespaces and grants are deliberately excluded from the
prune report because engine- and operator-created ones are expected and would
only be noise.

### Drift with no update path

Warehouse storage roots/options and role descriptions are fixed after creation
in the API. When the bundle asks to change one, the tool reports the drift
honestly as `would-update` but does not fail — there is simply no endpoint to
reconcile it. To actually change one of these, delete and recreate the resource
out of band, then re-apply.

## What is *not* in the bundle: tables and views

Tables and views are deliberately excluded. They are owned by **engines**
(Spark, Trino, Flink, pyiceberg, dbt, …) through the Iceberg REST protocol,
which create them, evolve their schemas, commit snapshots, and drop them as part
of data pipelines.

A table's authoritative state is its Iceberg metadata, and that metadata changes
on **every write**. It is data, not configuration:

- Declaring tables in a bundle would fight the engines for ownership of an
  object they mutate continuously.
- Any snapshot committed between `plan` and `apply` would make the plan wrong.
- There is no stable "desired state" to converge to — the desired state is
  whatever the last pipeline run produced.

The bundle stops exactly where the catalog itself draws the line: it provisions
the **containers and policy** (warehouses, namespaces, roles, grants, webhooks);
engines fill them.

## Output and exit codes

`plan` prints one line per resource, tagged `create` / `update` / `noop` /
`would-update` / `would-delete`, and a summary trailer. It is read-only and
always exits `0` (a plan is not a pass/fail check).

`apply` prints one line per resource, tagged `created` / `updated` /
`unchanged` / `warning` / `FAILED`, and a summary trailer. Each resource is
applied independently; a failure on one does not stop the others. `apply` exits
non-zero if **any** resource failed, which makes it safe to gate a CI job on.

## CI example

```sh
#!/usr/bin/env bash
set -euo pipefail
export MERIDIAN_WEBHOOK_SECRET="$CI_MERIDIAN_WEBHOOK_SECRET"

# Show the diff in the job log for the reviewer.
meridian plan -f catalog.yaml --server "$MERIDIAN_URL" --token "$MERIDIAN_TOKEN"

# Converge; non-zero exit fails the job.
meridian apply -f catalog.yaml --server "$MERIDIAN_URL" --token "$MERIDIAN_TOKEN"
```

## See also

- [ADR 006 — Catalog-as-code bundles](adr/006-catalog-as-code-bundles.md) — the
  design rationale, including why tables/views are excluded and why prune is out
  of scope.
- The end-to-end test at
  [`conformance/e2e/tests/test_catalog_as_code.py`](../conformance/e2e/tests/test_catalog_as_code.py)
  exercises the full apply → verify → re-apply → drift → reconcile cycle against
  a live server.
