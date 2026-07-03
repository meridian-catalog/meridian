package provider

import (
	"fmt"
	"testing"
	"time"

	"github.com/hashicorp/terraform-plugin-testing/helper/resource"
)

func TestAccWarehouseDataSource(t *testing.T) {
	name := fmt.Sprintf("tfacc-wh-ds-%d", time.Now().UnixNano())
	config := fmt.Sprintf(`
resource "meridian_warehouse" "test" {
  name         = %q
  storage_root = "s3://tfacc-ds/wh"
  %s
}

data "meridian_warehouse" "test" {
  name       = meridian_warehouse.test.name
  depends_on = [meridian_warehouse.test]
}
`, name, warehouseStorageOptions)

	resource.Test(t, resource.TestCase{
		PreCheck:                 func() { testAccPreCheck(t) },
		ProtoV6ProviderFactories: testAccProtoV6ProviderFactories,
		Steps: []resource.TestStep{
			{
				Config: config,
				Check: resource.ComposeAggregateTestCheckFunc(
					resource.TestCheckResourceAttr("data.meridian_warehouse.test", "name", name),
					resource.TestCheckResourceAttr("data.meridian_warehouse.test", "storage_root", "s3://tfacc-ds/wh"),
					resource.TestCheckResourceAttrSet("data.meridian_warehouse.test", "id"),
					resource.TestCheckResourceAttrSet("data.meridian_warehouse.test", "created_at"),
					// Secret values read back redacted.
					resource.TestCheckResourceAttr("data.meridian_warehouse.test", "storage_options.access-key-id", "***"),
					resource.TestCheckResourceAttr("data.meridian_warehouse.test", "storage_options.region", "us-east-1"),
				),
			},
		},
	})
}
