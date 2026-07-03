package provider

import (
	"context"
	"fmt"

	"github.com/hashicorp/terraform-plugin-framework/path"
	"github.com/hashicorp/terraform-plugin-framework/resource"
	"github.com/hashicorp/terraform-plugin-framework/resource/schema"
	"github.com/hashicorp/terraform-plugin-framework/resource/schema/planmodifier"
	"github.com/hashicorp/terraform-plugin-framework/resource/schema/stringplanmodifier"
	"github.com/hashicorp/terraform-plugin-framework/types"

	"github.com/meridian-catalog/meridian/terraform-provider-meridian/internal/client"
)

// NewWarehouseResource returns the meridian_warehouse resource.
func NewWarehouseResource() resource.Resource {
	return &warehouseResource{}
}

// warehouseResource maps onto POST/GET /api/v2/warehouses and
// DELETE /api/v2/warehouses/{name}. The management API has no warehouse
// update endpoint, so every attribute change forces replacement.
type warehouseResource struct {
	client *client.Client
}

type warehouseResourceModel struct {
	ID             types.String `tfsdk:"id"`
	Name           types.String `tfsdk:"name"`
	StorageRoot    types.String `tfsdk:"storage_root"`
	StorageOptions types.Map    `tfsdk:"storage_options"`
}

func (r *warehouseResource) Metadata(_ context.Context, req resource.MetadataRequest, resp *resource.MetadataResponse) {
	resp.TypeName = req.ProviderTypeName + "_warehouse"
}

func (r *warehouseResource) Schema(_ context.Context, _ resource.SchemaRequest, resp *resource.SchemaResponse) {
	resp.Schema = schema.Schema{
		Description: "A Meridian warehouse: a storage root plus storage options, whose name " +
			"doubles as the Iceberg REST catalog prefix. The management API has no update " +
			"endpoint for warehouses, so any change forces replacement — and replacement " +
			"only succeeds while the warehouse is empty (the server refuses to delete a " +
			"warehouse that contains namespaces).",
		Attributes: map[string]schema.Attribute{
			"id": schema.StringAttribute{
				Computed:      true,
				Description:   "Server-assigned ULID of the warehouse.",
				PlanModifiers: []planmodifier.String{stringplanmodifier.UseStateForUnknown()},
			},
			"name": schema.StringAttribute{
				Required: true,
				Description: "Warehouse name (1–100 characters from [A-Za-z0-9._-]); doubles as " +
					"the catalog URL prefix. Changing it forces replacement.",
				PlanModifiers: []planmodifier.String{stringplanmodifier.RequiresReplace()},
			},
			"storage_root": schema.StringAttribute{
				Required:      true,
				Description:   "Storage root URI, e.g. s3://bucket/prefix. Changing it forces replacement.",
				PlanModifiers: []planmodifier.String{stringplanmodifier.RequiresReplace()},
			},
			"storage_options": schema.MapAttribute{
				ElementType: types.StringType,
				Optional:    true,
				Sensitive:   true,
				Description: "Storage options (endpoint, region, credentials, vending mode, ...). " +
					"Marked sensitive because credential keys may be set here; the server " +
					"never returns secret values (they read back redacted). Changing any " +
					"option forces replacement.",
				PlanModifiers: []planmodifier.Map{mapRequiresReplace()},
			},
		},
	}
}

func (r *warehouseResource) Configure(_ context.Context, req resource.ConfigureRequest, resp *resource.ConfigureResponse) {
	r.client = configureClient(req.ProviderData, &resp.Diagnostics)
}

func (r *warehouseResource) Create(ctx context.Context, req resource.CreateRequest, resp *resource.CreateResponse) {
	var plan warehouseResourceModel
	resp.Diagnostics.Append(req.Plan.Get(ctx, &plan)...)
	if resp.Diagnostics.HasError() {
		return
	}

	options := map[string]string{}
	if !plan.StorageOptions.IsNull() {
		resp.Diagnostics.Append(plan.StorageOptions.ElementsAs(ctx, &options, false)...)
		if resp.Diagnostics.HasError() {
			return
		}
	}

	created, err := r.client.CreateWarehouse(ctx, client.CreateWarehouseRequest{
		Name:           plan.Name.ValueString(),
		StorageRoot:    plan.StorageRoot.ValueString(),
		StorageOptions: options,
	})
	if err != nil {
		resp.Diagnostics.AddError("Creating warehouse failed", err.Error())
		return
	}

	plan.ID = types.StringValue(created.ID)
	resp.Diagnostics.Append(resp.State.Set(ctx, &plan)...)
}

func (r *warehouseResource) Read(ctx context.Context, req resource.ReadRequest, resp *resource.ReadResponse) {
	var state warehouseResourceModel
	resp.Diagnostics.Append(req.State.Get(ctx, &state)...)
	if resp.Diagnostics.HasError() {
		return
	}

	warehouse, err := r.client.GetWarehouseByName(ctx, state.Name.ValueString())
	if client.IsNotFound(err) {
		resp.State.RemoveResource(ctx)
		return
	}
	if err != nil {
		resp.Diagnostics.AddError("Reading warehouse failed", err.Error())
		return
	}

	state.ID = types.StringValue(warehouse.ID)
	state.StorageRoot = types.StringValue(warehouse.StorageRoot)
	state.StorageOptions = mergeRedactedOptions(ctx, state.StorageOptions, warehouse.StorageOptions, &resp.Diagnostics)
	if resp.Diagnostics.HasError() {
		return
	}
	resp.Diagnostics.Append(resp.State.Set(ctx, &state)...)
}

// mergeRedactedOptions folds the server's storage options into state,
// keeping the prior state value for any key the server redacted to "***"
// (secret values are write-only on the management API). Without this, a
// warehouse with credentials would show a permanent phantom diff.
func mergeRedactedOptions(
	ctx context.Context,
	prior types.Map,
	remote map[string]string,
	diagnostics *diag,
) types.Map {
	priorValues := map[string]string{}
	if !prior.IsNull() && !prior.IsUnknown() {
		diagnostics.Append(prior.ElementsAs(ctx, &priorValues, false)...)
		if diagnostics.HasError() {
			return prior
		}
	}
	merged := map[string]string{}
	for key, value := range remote {
		if value == "***" {
			if priorValue, ok := priorValues[key]; ok {
				merged[key] = priorValue
				continue
			}
		}
		merged[key] = value
	}
	if len(merged) == 0 && prior.IsNull() {
		return types.MapNull(types.StringType)
	}
	result, mapDiags := types.MapValueFrom(ctx, types.StringType, merged)
	diagnostics.Append(mapDiags...)
	return result
}

// Update is unreachable: every attribute carries RequiresReplace and the
// management API has no warehouse update endpoint.
func (r *warehouseResource) Update(_ context.Context, _ resource.UpdateRequest, resp *resource.UpdateResponse) {
	resp.Diagnostics.AddError(
		"Warehouse update is not supported",
		"The Meridian management API has no warehouse update endpoint; all changes force replacement. "+
			"Hitting this is a provider bug — please report it.",
	)
}

func (r *warehouseResource) Delete(ctx context.Context, req resource.DeleteRequest, resp *resource.DeleteResponse) {
	var state warehouseResourceModel
	resp.Diagnostics.Append(req.State.Get(ctx, &state)...)
	if resp.Diagnostics.HasError() {
		return
	}
	err := r.client.DeleteWarehouse(ctx, state.Name.ValueString())
	if err != nil && !client.IsNotFound(err) {
		resp.Diagnostics.AddError(
			"Deleting warehouse failed",
			fmt.Sprintf("%s\n\nNote: the server refuses to delete a warehouse that still "+
				"contains namespaces.", err.Error()),
		)
	}
}

// ImportState imports a warehouse by name (`terraform import
// meridian_warehouse.example prod`).
func (r *warehouseResource) ImportState(ctx context.Context, req resource.ImportStateRequest, resp *resource.ImportStateResponse) {
	resource.ImportStatePassthroughID(ctx, path.Root("name"), req, resp)
}
