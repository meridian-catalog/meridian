package provider

import (
	"context"
	"testing"

	"github.com/hashicorp/terraform-plugin-framework/provider"
	"github.com/hashicorp/terraform-plugin-framework/tfsdk"
	"github.com/hashicorp/terraform-plugin-go/tftypes"

	"github.com/meridian-catalog/meridian/terraform-provider-meridian/internal/client"
)

// providerConfigType is the tftypes shape of meridianProviderModel, needed
// to build a tfsdk.Config by hand for Configure unit tests.
var providerConfigType = tftypes.Object{
	AttributeTypes: map[string]tftypes.Type{
		"endpoint": tftypes.String,
		"token":    tftypes.String,
	},
}

// buildConfig assembles a tfsdk.Config with the given raw endpoint/token
// values (each may be a string or the tftypes.UnknownValue sentinel).
func buildConfig(t *testing.T, endpoint, token tftypes.Value) tfsdk.Config {
	t.Helper()
	p := &meridianProvider{}
	schemaResp := &provider.SchemaResponse{}
	p.Schema(context.Background(), provider.SchemaRequest{}, schemaResp)
	return tfsdk.Config{
		Schema: schemaResp.Schema,
		Raw: tftypes.NewValue(providerConfigType, map[string]tftypes.Value{
			"endpoint": endpoint,
			"token":    token,
		}),
	}
}

func TestConfigureMissingEndpoint(t *testing.T) {
	t.Setenv("MERIDIAN_ENDPOINT", "")
	t.Setenv("MERIDIAN_TOKEN", "")

	p := &meridianProvider{}
	resp := &provider.ConfigureResponse{}
	p.Configure(context.Background(), provider.ConfigureRequest{
		Config: buildConfig(t,
			tftypes.NewValue(tftypes.String, nil),
			tftypes.NewValue(tftypes.String, nil)),
	}, resp)

	if !resp.Diagnostics.HasError() {
		t.Fatal("expected an error when endpoint is unset, got none")
	}
	if resp.ResourceData != nil {
		t.Fatal("no client should be built when endpoint is missing")
	}
}

func TestConfigureUnknownEndpointIsPreciseError(t *testing.T) {
	// A regression guard: an unknown endpoint must produce an "unknown"
	// error, not be silently coerced to "" and misreported as missing.
	p := &meridianProvider{}
	resp := &provider.ConfigureResponse{}
	p.Configure(context.Background(), provider.ConfigureRequest{
		Config: buildConfig(t,
			tftypes.NewValue(tftypes.String, tftypes.UnknownValue),
			tftypes.NewValue(tftypes.String, nil)),
	}, resp)

	if !resp.Diagnostics.HasError() {
		t.Fatal("expected an error for an unknown endpoint")
	}
	found := false
	for _, d := range resp.Diagnostics.Errors() {
		if d.Summary() == "Unknown Meridian endpoint" {
			found = true
		}
	}
	if !found {
		t.Fatalf("expected 'Unknown Meridian endpoint' diagnostic, got %v", resp.Diagnostics)
	}
	if resp.ResourceData != nil {
		t.Fatal("no client should be built for an unknown endpoint")
	}
}

func TestConfigureBuildsClientFromEnv(t *testing.T) {
	t.Setenv("MERIDIAN_ENDPOINT", "http://env-host:8181")
	t.Setenv("MERIDIAN_TOKEN", "env-token")

	p := &meridianProvider{}
	resp := &provider.ConfigureResponse{}
	p.Configure(context.Background(), provider.ConfigureRequest{
		Config: buildConfig(t,
			tftypes.NewValue(tftypes.String, nil),
			tftypes.NewValue(tftypes.String, nil)),
	}, resp)

	if resp.Diagnostics.HasError() {
		t.Fatalf("unexpected diagnostics: %v", resp.Diagnostics)
	}
	if _, ok := resp.ResourceData.(*client.Client); !ok {
		t.Fatalf("expected *client.Client in ResourceData, got %T", resp.ResourceData)
	}
}
