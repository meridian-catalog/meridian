package provider

import (
	"os"
	"testing"

	"github.com/hashicorp/terraform-plugin-framework/providerserver"
	"github.com/hashicorp/terraform-plugin-go/tfprotov6"
)

// testAccProtoV6ProviderFactories wires the in-process provider under the
// name "meridian" for acceptance tests.
var testAccProtoV6ProviderFactories = map[string]func() (tfprotov6.ProviderServer, error){
	"meridian": providerserver.NewProtocol6WithError(New("test")()),
}

// testAccPreCheck fails fast if the acceptance environment is not set up.
// Acceptance tests need TF_ACC=1 and a reachable Meridian server; the
// endpoint comes from MERIDIAN_ENDPOINT (default http://localhost:8181).
func testAccPreCheck(t *testing.T) {
	t.Helper()
	if os.Getenv("MERIDIAN_ENDPOINT") == "" {
		t.Setenv("MERIDIAN_ENDPOINT", "http://localhost:8181")
	}
	// The provider has no published registry namespace yet. OpenTofu
	// rejects the framework's default "-" namespace, so pin a valid
	// host/namespace for the in-process test provider unless the caller
	// overrode them. (Terraform tolerates the default; OpenTofu does not.)
	if os.Getenv("TF_ACC_PROVIDER_HOST") == "" {
		t.Setenv("TF_ACC_PROVIDER_HOST", "registry.opentofu.org")
	}
	if os.Getenv("TF_ACC_PROVIDER_NAMESPACE") == "" {
		t.Setenv("TF_ACC_PROVIDER_NAMESPACE", "hashicorp")
	}
}
