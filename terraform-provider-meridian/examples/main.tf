# Example configuration for the Meridian Terraform provider.
#
# Prereqs: a running Meridian server (default http://localhost:8181) and,
# for the warehouse below, a reachable S3-compatible store (the local dev
# stack ships MinIO on :9000 with credentials meridian / meridian123).

terraform {
  required_providers {
    meridian = {
      source = "meridian-catalog/meridian"
    }
  }
}

provider "meridian" {
  endpoint = "http://localhost:8181"
  # token  = var.meridian_token   # omit when the server runs with auth disabled
}

# A warehouse: storage root + storage options. Any change forces replacement
# (no update endpoint), and replacement only succeeds while it is empty.
resource "meridian_warehouse" "analytics" {
  name         = "analytics"
  storage_root = "s3://analytics/warehouse"

  storage_options = {
    region              = "us-east-1"
    endpoint            = "http://localhost:9000"
    "access-key-id"     = "meridian"
    "secret-access-key" = "meridian123"
    "path-style"        = "true"
  }
}

# An RBAC role.
resource "meridian_role" "analysts" {
  name        = "analysts"
  description = "Read-only access to the analytics warehouse."
}

# A grant: READ on the warehouse, given to the analysts role. Grants are
# immutable — any change replaces them.
resource "meridian_grant" "analysts_read" {
  privilege = "READ"
  role      = meridian_role.analysts.name

  securable = {
    type      = "warehouse"
    warehouse = meridian_warehouse.analytics.name
  }
}

# A namespace-scoped grant (securable addressed by name).
resource "meridian_grant" "analysts_write_sales" {
  privilege = "WRITE"
  role      = meridian_role.analysts.name

  securable = {
    type      = "namespace"
    warehouse = meridian_warehouse.analytics.name
    namespace = ["sales"]
  }
}

# A webhook receiving table-commit events. The signing secret is write-only;
# changing it (or the URL / filter) forces replacement.
resource "meridian_webhook" "commits" {
  url         = "https://example.com/hooks/meridian"
  event_types = ["com.meridian.table.committed"]
  secret      = "replace-with-a-16+-char-signing-secret"
}

# Data source: look up an existing warehouse by name.
data "meridian_warehouse" "analytics" {
  name = meridian_warehouse.analytics.name
}

# Data source: ranked full-text search over catalog assets.
data "meridian_search" "orders" {
  query = "orders"
  types = ["table", "view"]
  limit = 10
}

output "analytics_warehouse_id" {
  value = data.meridian_warehouse.analytics.id
}

output "search_hits" {
  value = [for r in data.meridian_search.orders.results : "${r.type}:${r.name}"]
}
