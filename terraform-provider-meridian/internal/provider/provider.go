// Package provider implements the Meridian Terraform provider on
// terraform-plugin-framework. Every resource and data source maps 1:1 onto
// the management API (`/api/v2`) — the provider has no side channels.
package provider

import (
	"context"
	"os"

	"github.com/hashicorp/terraform-plugin-framework/datasource"
	"github.com/hashicorp/terraform-plugin-framework/path"
	"github.com/hashicorp/terraform-plugin-framework/provider"
	"github.com/hashicorp/terraform-plugin-framework/provider/schema"
	"github.com/hashicorp/terraform-plugin-framework/resource"
	"github.com/hashicorp/terraform-plugin-framework/types"

	"github.com/meridian-catalog/meridian/terraform-provider-meridian/internal/client"
)

// New returns the provider constructor; version is stamped by the build.
func New(version string) func() provider.Provider {
	return func() provider.Provider {
		return &meridianProvider{version: version}
	}
}

type meridianProvider struct {
	version string
}

type meridianProviderModel struct {
	Endpoint types.String `tfsdk:"endpoint"`
	Token    types.String `tfsdk:"token"`
}

func (p *meridianProvider) Metadata(_ context.Context, _ provider.MetadataRequest, resp *provider.MetadataResponse) {
	resp.TypeName = "meridian"
	resp.Version = p.version
}

func (p *meridianProvider) Schema(_ context.Context, _ provider.SchemaRequest, resp *provider.SchemaResponse) {
	resp.Schema = schema.Schema{
		Description: "Manages a Meridian catalog through its management API (/api/v2).",
		Attributes: map[string]schema.Attribute{
			"endpoint": schema.StringAttribute{
				Optional: true,
				Description: "Base URL of the Meridian server, e.g. http://localhost:8181. " +
					"Falls back to the MERIDIAN_ENDPOINT environment variable.",
			},
			"token": schema.StringAttribute{
				Optional:  true,
				Sensitive: true,
				Description: "Bearer token for servers running with auth.mode = \"oidc\". " +
					"Falls back to the MERIDIAN_TOKEN environment variable. Omit for servers " +
					"with authentication disabled.",
			},
		},
	}
}

func (p *meridianProvider) Configure(ctx context.Context, req provider.ConfigureRequest, resp *provider.ConfigureResponse) {
	var config meridianProviderModel
	resp.Diagnostics.Append(req.Config.Get(ctx, &config)...)
	if resp.Diagnostics.HasError() {
		return
	}

	// If either attribute is still unknown (e.g. it references a value
	// computed by another resource), we cannot build a client yet. Emit a
	// precise error rather than silently treating the unknown as an empty
	// string, which would misreport a missing endpoint.
	if config.Endpoint.IsUnknown() {
		resp.Diagnostics.AddAttributeError(
			path.Root("endpoint"),
			"Unknown Meridian endpoint",
			"The provider's \"endpoint\" cannot be a value that is not known at plan time. "+
				"Set it to a static value, or supply it via the MERIDIAN_ENDPOINT environment variable.",
		)
	}
	if config.Token.IsUnknown() {
		resp.Diagnostics.AddAttributeError(
			path.Root("token"),
			"Unknown Meridian token",
			"The provider's \"token\" cannot be a value that is not known at plan time. "+
				"Set it to a static value, or supply it via the MERIDIAN_TOKEN environment variable.",
		)
	}
	if resp.Diagnostics.HasError() {
		return
	}

	endpoint := os.Getenv("MERIDIAN_ENDPOINT")
	if !config.Endpoint.IsNull() {
		endpoint = config.Endpoint.ValueString()
	}
	token := os.Getenv("MERIDIAN_TOKEN")
	if !config.Token.IsNull() {
		token = config.Token.ValueString()
	}

	if endpoint == "" {
		resp.Diagnostics.AddError(
			"Missing Meridian endpoint",
			"Set the provider's \"endpoint\" attribute or the MERIDIAN_ENDPOINT environment variable "+
				"to the base URL of the Meridian server (e.g. http://localhost:8181).",
		)
		return
	}

	apiClient := client.New(endpoint, token)
	resp.DataSourceData = apiClient
	resp.ResourceData = apiClient
}

func (p *meridianProvider) Resources(_ context.Context) []func() resource.Resource {
	return []func() resource.Resource{
		NewWarehouseResource,
		NewRoleResource,
		NewGrantResource,
		NewWebhookResource,
	}
}

func (p *meridianProvider) DataSources(_ context.Context) []func() datasource.DataSource {
	return []func() datasource.DataSource{
		NewWarehouseDataSource,
		NewSearchDataSource,
	}
}
