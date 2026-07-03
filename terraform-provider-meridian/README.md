# Terraform provider for Meridian

A [Terraform](https://www.terraform.io/) / [OpenTofu](https://opentofu.org/)
provider for the [Meridian](../README.md) catalog. It manages catalog
governance objects through the Meridian management API (`/api/v2`) and nothing
else — there are no side channels.

Status: pre-alpha, unpublished. Install from source (below); publishing to a
public registry requires moving this module to its own repository first (see
[Publishing](#publishing)).

## Supported objects

### Resources

| Resource            | API                                         | CRUD                              | Import |
| ------------------- | ------------------------------------------- | --------------------------------- | ------ |
| `meridian_warehouse`| `POST/GET /warehouses`, `DELETE /warehouses/{name}` | Create, Read, Delete, **replace-on-change** | by name (`terraform import meridian_warehouse.x prod`) |
| `meridian_role`     | `POST/GET /roles`, `DELETE /roles/{name}`   | Create, Read, Delete, **replace-on-change** | by name |
| `meridian_grant`    | `POST/GET /grants`, `DELETE /grants/{id}`   | Create, Read, Delete, **replace-on-change** | by ULID (warehouse-scoped grants only) |
| `meridian_webhook`  | `POST/GET /webhooks`, `DELETE /webhooks/{id}` | Create, Read, Delete, **replace-on-change** | by ULID (secret not recoverable) |

**Replace-on-change** — the management API has no update endpoints for these
objects. Every attribute carries `RequiresReplace`, so any change is realized
as delete + create. The provider documents this in each resource's schema and
surfaces a clear error if the (unreachable) update path is ever hit.

Consequences to know before you apply:

- **Warehouse / role replacement is destructive on the server.** Replacing a
  warehouse only succeeds while it is empty (the server refuses to delete a
  warehouse that still contains namespaces). Replacing or deleting a role
  removes its principal bindings and grants — `meridian_grant` resources are
  re-created on the next apply, but out-of-band bindings are lost.
- **Grants are immutable.** A grant is a single `(privilege, grantee, securable)`
  fact; there is no update. Changing any field replaces it.
- **Webhook secrets are write-only.** The server never returns a webhook's
  signing secret, so it lives only in your configuration and Terraform state —
  keep your state backend encrypted. Changing the secret forces replacement.

### Data sources

| Data source          | API              | Notes |
| -------------------- | ---------------- | ----- |
| `meridian_warehouse` | `GET /warehouses`| Look up one warehouse by name. Secret storage-option values read back redacted as `***`. |
| `meridian_search`    | `GET /search`    | One ranked full-text search over catalog assets; returns the first page of results, filtered to the caller's visibility by the server. |

## Provider configuration

```hcl
terraform {
  required_providers {
    meridian = {
      source = "meridian-catalog/meridian"
    }
  }
}

provider "meridian" {
  endpoint = "http://localhost:8181" # or MERIDIAN_ENDPOINT
  token    = var.meridian_token      # or MERIDIAN_TOKEN; omit if auth is disabled
}
```

| Attribute  | Env fallback        | Notes |
| ---------- | ------------------- | ----- |
| `endpoint` | `MERIDIAN_ENDPOINT` | Base URL of the server, e.g. `http://localhost:8181`. Required. |
| `token`    | `MERIDIAN_TOKEN`    | Bearer token for servers running with `auth.mode = "oidc"`. Omit for servers with auth disabled. Marked sensitive. |

See [`examples/`](./examples) for full configurations.

## Install from source

Requires Go 1.26+.

```sh
# Build and install into the local plugin mirror.
make install                 # installs version "dev" for your OS/arch
# or a specific version string:
make install VERSION=0.1.0
```

`make install` places the binary under
`~/.terraform.d/plugins/registry.terraform.io/meridian-catalog/meridian/<version>/<os>_<arch>/`.
To use the locally installed build, pin the version in `required_providers` and
run `terraform init` (OpenTofu: `tofu init`). During active development a
[dev override](https://developer.hashicorp.com/terraform/cli/config/config-file#development-overrides-for-provider-developers)
in `~/.terraformrc` pointing at the built binary is often more convenient.

## Testing

**Unit tests** (fast; the client is exercised against an in-process HTTP stub —
no server needed):

```sh
make test
```

**Acceptance tests** (`TF_ACC=1`) stand up real resources against a live
Meridian server and drive a full create → read → update(replace) → delete cycle
plus import and a plan → apply → plan-empty check for each resource:

```sh
# Prereqs: a running Meridian server and tofu (or terraform) on PATH.
#   brew install opentofu      # lighter than terraform; either works
#   DATABASE_URL=postgres://meridian:meridian@localhost:5433/meridian \
#     ./target/debug/meridian serve      # server on :8181
make testacc                              # MERIDIAN_ENDPOINT defaults to http://localhost:8181
```

The acceptance suite auto-detects `tofu` (preferred) or `terraform`. Because
this provider is not yet published under a registry namespace, the tests pin a
valid host/namespace for the in-process provider (OpenTofu rejects the default
`-` namespace); this is handled automatically in the test pre-check.

## Publishing

Publishing to the public Terraform Registry / OpenTofu Registry requires this
module to live in its own top-level Git repository named
`terraform-provider-meridian`, with signed release tags and a GoReleaser
workflow. That work is intentionally deferred while the provider lives inside
the monorepo. Until then, install from source as above.

## License

Apache-2.0, same as the rest of the Meridian monorepo.
