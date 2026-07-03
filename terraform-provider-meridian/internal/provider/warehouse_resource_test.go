package provider

import (
	"fmt"
	"testing"
	"time"

	"github.com/hashicorp/terraform-plugin-testing/helper/resource"
)

// warehouseStorageOptions is the MinIO-backed option block used by
// acceptance tests (the local dev stack ships MinIO on :9000).
const warehouseStorageOptions = `storage_options = {
    region            = "us-east-1"
    endpoint          = "http://localhost:9000"
    "access-key-id"   = "meridian"
    "secret-access-key" = "meridian123"
    "path-style"      = "true"
  }`

func TestAccWarehouseResource(t *testing.T) {
	name := fmt.Sprintf("tfacc-wh-%d", time.Now().UnixNano())
	config := func(root string) string {
		return fmt.Sprintf(`
resource "meridian_warehouse" "test" {
  name         = %q
  storage_root = %q
  %s
}
`, name, root, warehouseStorageOptions)
	}

	resource.Test(t, resource.TestCase{
		PreCheck:                 func() { testAccPreCheck(t) },
		ProtoV6ProviderFactories: testAccProtoV6ProviderFactories,
		Steps: []resource.TestStep{
			// Create + Read.
			{
				Config: config("s3://tfacc-bucket/wh"),
				Check: resource.ComposeAggregateTestCheckFunc(
					resource.TestCheckResourceAttr("meridian_warehouse.test", "name", name),
					resource.TestCheckResourceAttr("meridian_warehouse.test", "storage_root", "s3://tfacc-bucket/wh"),
					resource.TestCheckResourceAttrSet("meridian_warehouse.test", "id"),
				),
			},
			// Import by name. Only the secret storage-option values are
			// unverifiable (the server redacts them to "***"); the non-secret
			// keys must round-trip, so ignore just those two keys — not the
			// whole map — to keep import honest about the rest.
			{
				ResourceName:      "meridian_warehouse.test",
				ImportState:       true,
				ImportStateId:     name,
				ImportStateVerify: true,
				ImportStateVerifyIgnore: []string{
					"storage_options.access-key-id",
					"storage_options.secret-access-key",
				},
			},
			// Update forces replacement (no update endpoint): change the
			// storage root and confirm apply succeeds and re-reads clean.
			{
				Config: config("s3://tfacc-bucket/wh2"),
				Check: resource.ComposeAggregateTestCheckFunc(
					resource.TestCheckResourceAttr("meridian_warehouse.test", "storage_root", "s3://tfacc-bucket/wh2"),
				),
			},
		},
	})
}
