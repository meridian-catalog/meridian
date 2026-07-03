package provider

import (
	"fmt"
	"testing"
	"time"

	"github.com/hashicorp/terraform-plugin-testing/helper/resource"
	"github.com/hashicorp/terraform-plugin-testing/plancheck"
)

func TestAccWebhookResource(t *testing.T) {
	suffix := time.Now().UnixNano()
	url := fmt.Sprintf("https://example.test/hook-%d", suffix)
	config := func(eventTypes string) string {
		return fmt.Sprintf(`
resource "meridian_webhook" "test" {
  url         = %q
  event_types = %s
  secret      = "supersecret-signing-key"
}
`, url, eventTypes)
	}

	resource.Test(t, resource.TestCase{
		PreCheck:                 func() { testAccPreCheck(t) },
		ProtoV6ProviderFactories: testAccProtoV6ProviderFactories,
		Steps: []resource.TestStep{
			// Create + Read with a multi-element filter, to lock element
			// ordering (lists are order-sensitive: a server that reordered
			// event_types would produce a phantom replace every plan).
			{
				Config: config(`["com.meridian.table.committed", "com.meridian.namespace.created", "com.meridian.view.committed"]`),
				Check: resource.ComposeAggregateTestCheckFunc(
					resource.TestCheckResourceAttr("meridian_webhook.test", "url", url),
					resource.TestCheckResourceAttr("meridian_webhook.test", "event_types.#", "3"),
					resource.TestCheckResourceAttr("meridian_webhook.test", "event_types.0", "com.meridian.table.committed"),
					resource.TestCheckResourceAttr("meridian_webhook.test", "event_types.1", "com.meridian.namespace.created"),
					resource.TestCheckResourceAttr("meridian_webhook.test", "event_types.2", "com.meridian.view.committed"),
					resource.TestCheckResourceAttr("meridian_webhook.test", "secret", "supersecret-signing-key"),
					resource.TestCheckResourceAttrSet("meridian_webhook.test", "id"),
				),
				ConfigPlanChecks: resource.ConfigPlanChecks{
					PostApplyPostRefresh: []plancheck.PlanCheck{
						plancheck.ExpectEmptyPlan(),
					},
				},
			},
			// Changing the event-type filter forces replacement (no update
			// endpoint).
			{
				Config: config(`["com.meridian.namespace.created"]`),
				ConfigPlanChecks: resource.ConfigPlanChecks{
					PreApply: []plancheck.PlanCheck{
						plancheck.ExpectResourceAction("meridian_webhook.test", plancheck.ResourceActionReplace),
					},
				},
				Check: resource.ComposeAggregateTestCheckFunc(
					resource.TestCheckResourceAttr("meridian_webhook.test", "event_types.#", "1"),
					resource.TestCheckResourceAttr("meridian_webhook.test", "event_types.0", "com.meridian.namespace.created"),
				),
			},
			// Omitting event_types entirely (null, not []) must not produce a
			// phantom null-vs-[] diff after apply — the server stores an empty
			// filter and Read keeps it null.
			{
				Config: fmt.Sprintf(`
resource "meridian_webhook" "test" {
  url    = %q
  secret = "supersecret-signing-key"
}
`, url),
				ConfigPlanChecks: resource.ConfigPlanChecks{
					PostApplyPostRefresh: []plancheck.PlanCheck{
						plancheck.ExpectEmptyPlan(),
					},
				},
				Check: resource.ComposeAggregateTestCheckFunc(
					resource.TestCheckResourceAttr("meridian_webhook.test", "event_types.#", "0"),
				),
			},
		},
	})
}
