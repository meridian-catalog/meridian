package provider

import (
	"fmt"
	"testing"
	"time"

	"github.com/hashicorp/terraform-plugin-testing/helper/resource"
	"github.com/hashicorp/terraform-plugin-testing/plancheck"
)

func TestAccRoleResource(t *testing.T) {
	name := fmt.Sprintf("tfacc-role-%d", time.Now().UnixNano())
	config := func(desc string) string {
		return fmt.Sprintf(`
resource "meridian_role" "test" {
  name        = %q
  description = %q
}
`, name, desc)
	}

	resource.Test(t, resource.TestCase{
		PreCheck:                 func() { testAccPreCheck(t) },
		ProtoV6ProviderFactories: testAccProtoV6ProviderFactories,
		Steps: []resource.TestStep{
			// Create + Read.
			{
				Config: config("initial description"),
				Check: resource.ComposeAggregateTestCheckFunc(
					resource.TestCheckResourceAttr("meridian_role.test", "name", name),
					resource.TestCheckResourceAttr("meridian_role.test", "description", "initial description"),
					resource.TestCheckResourceAttr("meridian_role.test", "built_in", "false"),
					resource.TestCheckResourceAttrSet("meridian_role.test", "id"),
				),
				// A second plan on the same config must be empty.
				ConfigPlanChecks: resource.ConfigPlanChecks{
					PostApplyPostRefresh: []plancheck.PlanCheck{
						plancheck.ExpectEmptyPlan(),
					},
				},
			},
			// Import by name.
			{
				ResourceName:      "meridian_role.test",
				ImportState:       true,
				ImportStateId:     name,
				ImportStateVerify: true,
			},
			// Description change forces replacement (no update endpoint).
			{
				Config: config("changed description"),
				ConfigPlanChecks: resource.ConfigPlanChecks{
					PreApply: []plancheck.PlanCheck{
						plancheck.ExpectResourceAction("meridian_role.test", plancheck.ResourceActionReplace),
					},
				},
				Check: resource.ComposeAggregateTestCheckFunc(
					resource.TestCheckResourceAttr("meridian_role.test", "description", "changed description"),
				),
			},
		},
	})
}
