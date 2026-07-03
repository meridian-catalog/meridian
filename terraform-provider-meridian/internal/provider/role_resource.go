package provider

import (
	"context"

	"github.com/hashicorp/terraform-plugin-framework/path"
	"github.com/hashicorp/terraform-plugin-framework/resource"
	"github.com/hashicorp/terraform-plugin-framework/resource/schema"
	"github.com/hashicorp/terraform-plugin-framework/resource/schema/planmodifier"
	"github.com/hashicorp/terraform-plugin-framework/resource/schema/stringplanmodifier"
	"github.com/hashicorp/terraform-plugin-framework/types"

	"github.com/meridian-catalog/meridian/terraform-provider-meridian/internal/client"
)

// NewRoleResource returns the meridian_role resource.
func NewRoleResource() resource.Resource {
	return &roleResource{}
}

// roleResource maps onto POST/GET /api/v2/roles and
// DELETE /api/v2/roles/{name}. The management API has no role update
// endpoint, so every attribute change forces replacement. Deleting a role
// removes its bindings and grants with it (server semantics).
type roleResource struct {
	client *client.Client
}

type roleResourceModel struct {
	ID          types.String `tfsdk:"id"`
	Name        types.String `tfsdk:"name"`
	Description types.String `tfsdk:"description"`
	BuiltIn     types.Bool   `tfsdk:"built_in"`
}

func (r *roleResource) Metadata(_ context.Context, req resource.MetadataRequest, resp *resource.MetadataResponse) {
	resp.TypeName = req.ProviderTypeName + "_role"
}

func (r *roleResource) Schema(_ context.Context, _ resource.SchemaRequest, resp *resource.SchemaResponse) {
	resp.Schema = schema.Schema{
		Description: "An RBAC role. The management API has no role update endpoint, so any " +
			"change forces replacement. Replacing (or deleting) a role removes its principal " +
			"bindings and grants with it — grants managed by meridian_grant resources are " +
			"re-created on the next apply, but out-of-band bindings are lost.",
		Attributes: map[string]schema.Attribute{
			"id": schema.StringAttribute{
				Computed:      true,
				Description:   "Server-assigned ULID of the role.",
				PlanModifiers: []planmodifier.String{stringplanmodifier.UseStateForUnknown()},
			},
			"name": schema.StringAttribute{
				Required: true,
				Description: "Role name, unique per workspace (1–100 characters, no control " +
					"characters). Changing it forces replacement.",
				PlanModifiers: []planmodifier.String{stringplanmodifier.RequiresReplace()},
			},
			"description": schema.StringAttribute{
				Optional:      true,
				Description:   "Human description. Changing it forces replacement (no update endpoint).",
				PlanModifiers: []planmodifier.String{stringplanmodifier.RequiresReplace()},
			},
			"built_in": schema.BoolAttribute{
				Computed:    true,
				Description: "Whether this is a built-in role. Always false for managed roles.",
			},
		},
	}
}

func (r *roleResource) Configure(_ context.Context, req resource.ConfigureRequest, resp *resource.ConfigureResponse) {
	r.client = configureClient(req.ProviderData, &resp.Diagnostics)
}

func (r *roleResource) Create(ctx context.Context, req resource.CreateRequest, resp *resource.CreateResponse) {
	var plan roleResourceModel
	resp.Diagnostics.Append(req.Plan.Get(ctx, &plan)...)
	if resp.Diagnostics.HasError() {
		return
	}

	request := client.CreateRoleRequest{Name: plan.Name.ValueString()}
	if !plan.Description.IsNull() {
		description := plan.Description.ValueString()
		request.Description = &description
	}

	created, err := r.client.CreateRole(ctx, request)
	if err != nil {
		resp.Diagnostics.AddError("Creating role failed", err.Error())
		return
	}

	plan.ID = types.StringValue(created.ID)
	plan.BuiltIn = types.BoolValue(created.BuiltIn)
	resp.Diagnostics.Append(resp.State.Set(ctx, &plan)...)
}

func (r *roleResource) Read(ctx context.Context, req resource.ReadRequest, resp *resource.ReadResponse) {
	var state roleResourceModel
	resp.Diagnostics.Append(req.State.Get(ctx, &state)...)
	if resp.Diagnostics.HasError() {
		return
	}

	role, err := r.client.GetRoleByName(ctx, state.Name.ValueString())
	if client.IsNotFound(err) {
		resp.State.RemoveResource(ctx)
		return
	}
	if err != nil {
		resp.Diagnostics.AddError("Reading role failed", err.Error())
		return
	}

	state.ID = types.StringValue(role.ID)
	state.BuiltIn = types.BoolValue(role.BuiltIn)
	if role.Description != nil {
		state.Description = types.StringValue(*role.Description)
	} else {
		state.Description = types.StringNull()
	}
	resp.Diagnostics.Append(resp.State.Set(ctx, &state)...)
}

// Update is unreachable: every attribute carries RequiresReplace and the
// management API has no role update endpoint.
func (r *roleResource) Update(_ context.Context, _ resource.UpdateRequest, resp *resource.UpdateResponse) {
	resp.Diagnostics.AddError(
		"Role update is not supported",
		"The Meridian management API has no role update endpoint; all changes force replacement. "+
			"Hitting this is a provider bug — please report it.",
	)
}

func (r *roleResource) Delete(ctx context.Context, req resource.DeleteRequest, resp *resource.DeleteResponse) {
	var state roleResourceModel
	resp.Diagnostics.Append(req.State.Get(ctx, &state)...)
	if resp.Diagnostics.HasError() {
		return
	}
	err := r.client.DeleteRole(ctx, state.Name.ValueString())
	if err != nil && !client.IsNotFound(err) {
		resp.Diagnostics.AddError("Deleting role failed", err.Error())
	}
}

// ImportState imports a role by name (`terraform import
// meridian_role.example analysts`).
func (r *roleResource) ImportState(ctx context.Context, req resource.ImportStateRequest, resp *resource.ImportStateResponse) {
	resource.ImportStatePassthroughID(ctx, path.Root("name"), req, resp)
}
