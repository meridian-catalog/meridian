package provider

import (
	"context"

	"github.com/hashicorp/terraform-plugin-framework/datasource"
	"github.com/hashicorp/terraform-plugin-framework/datasource/schema"
	"github.com/hashicorp/terraform-plugin-framework/types"

	"github.com/meridian-catalog/meridian/terraform-provider-meridian/internal/client"
)

// NewWarehouseDataSource returns the meridian_warehouse data source.
func NewWarehouseDataSource() datasource.DataSource {
	return &warehouseDataSource{}
}

// warehouseDataSource reads one warehouse by name via GET /api/v2/warehouses
// (the management API has no per-name GET, so the client filters the
// listing). Secret storage-option values read back redacted to "***".
type warehouseDataSource struct {
	client *client.Client
}

type warehouseDataSourceModel struct {
	ID             types.String `tfsdk:"id"`
	Name           types.String `tfsdk:"name"`
	StorageRoot    types.String `tfsdk:"storage_root"`
	StorageOptions types.Map    `tfsdk:"storage_options"`
	CreatedAt      types.String `tfsdk:"created_at"`
	UpdatedAt      types.String `tfsdk:"updated_at"`
}

func (d *warehouseDataSource) Metadata(_ context.Context, req datasource.MetadataRequest, resp *datasource.MetadataResponse) {
	resp.TypeName = req.ProviderTypeName + "_warehouse"
}

func (d *warehouseDataSource) Schema(_ context.Context, _ datasource.SchemaRequest, resp *datasource.SchemaResponse) {
	resp.Schema = schema.Schema{
		Description: "Reads an existing Meridian warehouse by name. Secret storage-option values " +
			"are redacted to \"***\" by the server.",
		Attributes: map[string]schema.Attribute{
			"name": schema.StringAttribute{
				Required:    true,
				Description: "Name of the warehouse to look up.",
			},
			"id": schema.StringAttribute{
				Computed:    true,
				Description: "Server-assigned ULID of the warehouse.",
			},
			"storage_root": schema.StringAttribute{
				Computed:    true,
				Description: "Storage root URI (e.g. s3://bucket/prefix).",
			},
			"storage_options": schema.MapAttribute{
				ElementType: types.StringType,
				Computed:    true,
				Description: "Storage options; secret values read back redacted as \"***\".",
			},
			"created_at": schema.StringAttribute{
				Computed:    true,
				Description: "RFC 3339 creation timestamp.",
			},
			"updated_at": schema.StringAttribute{
				Computed:    true,
				Description: "RFC 3339 last-update timestamp.",
			},
		},
	}
}

func (d *warehouseDataSource) Configure(_ context.Context, req datasource.ConfigureRequest, resp *datasource.ConfigureResponse) {
	d.client = configureClient(req.ProviderData, &resp.Diagnostics)
}

func (d *warehouseDataSource) Read(ctx context.Context, req datasource.ReadRequest, resp *datasource.ReadResponse) {
	var config warehouseDataSourceModel
	resp.Diagnostics.Append(req.Config.Get(ctx, &config)...)
	if resp.Diagnostics.HasError() {
		return
	}

	warehouse, err := d.client.GetWarehouseByName(ctx, config.Name.ValueString())
	if err != nil {
		resp.Diagnostics.AddError("Reading warehouse failed", err.Error())
		return
	}

	options, mapDiags := types.MapValueFrom(ctx, types.StringType, warehouse.StorageOptions)
	resp.Diagnostics.Append(mapDiags...)
	if resp.Diagnostics.HasError() {
		return
	}

	config.ID = types.StringValue(warehouse.ID)
	config.StorageRoot = types.StringValue(warehouse.StorageRoot)
	config.StorageOptions = options
	config.CreatedAt = types.StringValue(warehouse.CreatedAt)
	config.UpdatedAt = types.StringValue(warehouse.UpdatedAt)
	resp.Diagnostics.Append(resp.State.Set(ctx, &config)...)
}
