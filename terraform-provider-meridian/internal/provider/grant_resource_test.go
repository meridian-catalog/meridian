package provider

import (
	"fmt"
	"testing"
	"time"

	"github.com/hashicorp/terraform-plugin-testing/helper/resource"
	"github.com/hashicorp/terraform-plugin-testing/plancheck"
	"github.com/hashicorp/terraform-plugin-testing/terraform"
)

func TestAccGrantResource(t *testing.T) {
	suffix := time.Now().UnixNano()
	whName := fmt.Sprintf("tfacc-grant-wh-%d", suffix)
	roleName := fmt.Sprintf("tfacc-grant-role-%d", suffix)

	config := func(privilege string) string {
		return fmt.Sprintf(`
resource "meridian_warehouse" "test" {
  name         = %q
  storage_root = "s3://tfacc-grant/wh"
  %s
}

resource "meridian_role" "test" {
  name = %q
}

resource "meridian_grant" "test" {
  privilege = %q
  role      = meridian_role.test.name
  securable = {
    type      = "warehouse"
    warehouse = meridian_warehouse.test.name
  }
}
`, whName, warehouseStorageOptions, roleName, privilege)
	}

	resource.Test(t, resource.TestCase{
		PreCheck:                 func() { testAccPreCheck(t) },
		ProtoV6ProviderFactories: testAccProtoV6ProviderFactories,
		Steps: []resource.TestStep{
			// Create + Read.
			{
				Config: config("READ"),
				Check: resource.ComposeAggregateTestCheckFunc(
					resource.TestCheckResourceAttr("meridian_grant.test", "privilege", "READ"),
					resource.TestCheckResourceAttr("meridian_grant.test", "role", roleName),
					resource.TestCheckResourceAttr("meridian_grant.test", "securable.type", "warehouse"),
					resource.TestCheckResourceAttr("meridian_grant.test", "securable.warehouse", whName),
					resource.TestCheckResourceAttrSet("meridian_grant.test", "id"),
					resource.TestCheckResourceAttrSet("meridian_grant.test", "securable_id"),
				),
				ConfigPlanChecks: resource.ConfigPlanChecks{
					PostApplyPostRefresh: []plancheck.PlanCheck{
						plancheck.ExpectEmptyPlan(),
					},
				},
			},
			// Import the (warehouse-scoped) grant by its ULID, read from state.
			{
				ResourceName:            "meridian_grant.test",
				ImportState:             true,
				ImportStateVerify:       true,
				ImportStateIdFunc:       grantImportID("meridian_grant.test"),
				ImportStateVerifyIgnore: []string{"securable.namespace", "securable.table", "securable.view"},
			},
			// Changing the privilege forces replacement (grants are immutable).
			{
				Config: config("WRITE"),
				ConfigPlanChecks: resource.ConfigPlanChecks{
					PreApply: []plancheck.PlanCheck{
						plancheck.ExpectResourceAction("meridian_grant.test", plancheck.ResourceActionReplace),
					},
				},
				Check: resource.ComposeAggregateTestCheckFunc(
					resource.TestCheckResourceAttr("meridian_grant.test", "privilege", "WRITE"),
				),
			},
		},
	})
}

// grantImportID reads the grant's ULID out of Terraform state so import can
// address it (the ULID is server-assigned and not known ahead of time).
func grantImportID(resourceName string) resource.ImportStateIdFunc {
	return func(s *terraform.State) (string, error) {
		rs, ok := s.RootModule().Resources[resourceName]
		if !ok {
			return "", fmt.Errorf("resource %s not found in state", resourceName)
		}
		return rs.Primary.Attributes["id"], nil
	}
}
