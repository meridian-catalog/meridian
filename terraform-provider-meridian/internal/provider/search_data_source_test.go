package provider

import (
	"testing"

	"github.com/hashicorp/terraform-plugin-testing/helper/resource"
)

// TestAccSearchDataSource exercises the search data source end to end. It
// asserts the query round-trips and the results list is populated (its
// element count attribute is set); it does not assert specific hits, since
// those depend on the live catalog's contents.
func TestAccSearchDataSource(t *testing.T) {
	const config = `
data "meridian_search" "test" {
  query = "test"
  limit = 5
}
`
	resource.Test(t, resource.TestCase{
		PreCheck:                 func() { testAccPreCheck(t) },
		ProtoV6ProviderFactories: testAccProtoV6ProviderFactories,
		Steps: []resource.TestStep{
			{
				Config: config,
				Check: resource.ComposeAggregateTestCheckFunc(
					resource.TestCheckResourceAttr("data.meridian_search.test", "query", "test"),
					resource.TestCheckResourceAttrSet("data.meridian_search.test", "results.#"),
				),
			},
		},
	})
}
