package provider

import (
	"context"
	"fmt"

	"github.com/hashicorp/terraform-plugin-framework-validators/resourcevalidator"
	"github.com/hashicorp/terraform-plugin-framework/path"
	"github.com/hashicorp/terraform-plugin-framework/resource"
	"github.com/hashicorp/terraform-plugin-framework/resource/schema"
	"github.com/hashicorp/terraform-plugin-framework/resource/schema/objectplanmodifier"
	"github.com/hashicorp/terraform-plugin-framework/resource/schema/planmodifier"
	"github.com/hashicorp/terraform-plugin-framework/resource/schema/stringplanmodifier"
	"github.com/hashicorp/terraform-plugin-framework/types"

	"github.com/meridian-catalog/meridian/terraform-provider-meridian/internal/client"
)

// NewGrantResource returns the meridian_grant resource.
func NewGrantResource() resource.Resource {
	return &grantResource{}
}

// grantResource maps onto POST/GET /api/v2/grants and
// DELETE /api/v2/grants/{id}. Grants are immutable on the server — there
// is no update endpoint and a grant is a single (privilege, grantee,
// securable) fact — so every attribute change forces replacement
// (delete + create).
type grantResource struct {
	client *client.Client
}

type grantResourceModel struct {
	ID          types.String `tfsdk:"id"`
	Privilege   types.String `tfsdk:"privilege"`
	Role        types.String `tfsdk:"role"`
	PrincipalID types.String `tfsdk:"principal_id"`
	Securable   types.Object `tfsdk:"securable"`
	SecurableID types.String `tfsdk:"securable_id"`
}

type grantSecurableModel struct {
	Type      types.String `tfsdk:"type"`
	Warehouse types.String `tfsdk:"warehouse"`
	Namespace types.List   `tfsdk:"namespace"`
	Table     types.String `tfsdk:"table"`
	View      types.String `tfsdk:"view"`
}

func (r *grantResource) Metadata(_ context.Context, req resource.MetadataRequest, resp *resource.MetadataResponse) {
	resp.TypeName = req.ProviderTypeName + "_grant"
}

func (r *grantResource) Schema(_ context.Context, _ resource.SchemaRequest, resp *resource.SchemaResponse) {
	resp.Schema = schema.Schema{
		Description: "One RBAC grant: a privilege on a securable (warehouse, namespace, table, " +
			"or view) given to a role or a principal. Grants are immutable on the server, so " +
			"any change forces replacement (delete + create). The securable is addressed by " +
			"name at create time and must exist.",
		Attributes: map[string]schema.Attribute{
			"id": schema.StringAttribute{
				Computed:      true,
				Description:   "Server-assigned ULID of the grant.",
				PlanModifiers: []planmodifier.String{stringplanmodifier.UseStateForUnknown()},
			},
			"privilege": schema.StringAttribute{
				Required: true,
				Description: "Privilege to grant: MANAGE_WAREHOUSE, CREATE_NAMESPACE, " +
					"LIST_NAMESPACES, MANAGE_NAMESPACE, CREATE_TABLE, LIST_TABLES, CREATE_VIEW, " +
					"READ, WRITE, COMMIT, or DROP. Changing it forces replacement.",
				PlanModifiers: []planmodifier.String{stringplanmodifier.RequiresReplace()},
			},
			"role": schema.StringAttribute{
				Optional: true,
				Description: "Grantee role name. Exactly one of role and principal_id must be " +
					"set. Changing it forces replacement.",
				PlanModifiers: []planmodifier.String{stringplanmodifier.RequiresReplace()},
			},
			"principal_id": schema.StringAttribute{
				Optional: true,
				Description: "Grantee principal ULID (from GET /api/v2/principals). Exactly one " +
					"of role and principal_id must be set. Changing it forces replacement.",
				PlanModifiers: []planmodifier.String{stringplanmodifier.RequiresReplace()},
			},
			"securable": schema.SingleNestedAttribute{
				Required: true,
				Description: "What the grant attaches to, addressed by name. Changing it forces " +
					"replacement.",
				PlanModifiers: []planmodifier.Object{objectplanmodifier.RequiresReplace()},
				Attributes: map[string]schema.Attribute{
					"type": schema.StringAttribute{
						Required:    true,
						Description: "Securable type: warehouse, namespace, table, or view.",
					},
					"warehouse": schema.StringAttribute{
						Required:    true,
						Description: "Warehouse name (always required).",
					},
					"namespace": schema.ListAttribute{
						ElementType: types.StringType,
						Optional:    true,
						Description: "Namespace levels (required for namespace, table, and view securables).",
					},
					"table": schema.StringAttribute{
						Optional:    true,
						Description: "Table name (required for table securables).",
					},
					"view": schema.StringAttribute{
						Optional:    true,
						Description: "View name (required for view securables).",
					},
				},
			},
			"securable_id": schema.StringAttribute{
				Computed:      true,
				Description:   "Server-resolved ULID of the securable.",
				PlanModifiers: []planmodifier.String{stringplanmodifier.UseStateForUnknown()},
			},
		},
	}
}

func (r *grantResource) ConfigValidators(_ context.Context) []resource.ConfigValidator {
	return []resource.ConfigValidator{
		resourcevalidator.ExactlyOneOf(
			path.MatchRoot("role"),
			path.MatchRoot("principal_id"),
		),
	}
}

func (r *grantResource) Configure(_ context.Context, req resource.ConfigureRequest, resp *resource.ConfigureResponse) {
	r.client = configureClient(req.ProviderData, &resp.Diagnostics)
}

func (r *grantResource) Create(ctx context.Context, req resource.CreateRequest, resp *resource.CreateResponse) {
	var plan grantResourceModel
	resp.Diagnostics.Append(req.Plan.Get(ctx, &plan)...)
	if resp.Diagnostics.HasError() {
		return
	}

	var securable grantSecurableModel
	resp.Diagnostics.Append(plan.Securable.As(ctx, &securable, objectAsOptions())...)
	if resp.Diagnostics.HasError() {
		return
	}

	request := client.CreateGrantRequest{
		Privilege: plan.Privilege.ValueString(),
		Securable: client.GrantSecurable{
			Type:      securable.Type.ValueString(),
			Warehouse: securable.Warehouse.ValueString(),
		},
	}
	if !plan.Role.IsNull() {
		role := plan.Role.ValueString()
		request.Role = &role
	}
	if !plan.PrincipalID.IsNull() {
		principalID := plan.PrincipalID.ValueString()
		request.PrincipalID = &principalID
	}
	if !securable.Namespace.IsNull() {
		resp.Diagnostics.Append(securable.Namespace.ElementsAs(ctx, &request.Securable.Namespace, false)...)
		if resp.Diagnostics.HasError() {
			return
		}
	}
	if !securable.Table.IsNull() {
		table := securable.Table.ValueString()
		request.Securable.Table = &table
	}
	if !securable.View.IsNull() {
		view := securable.View.ValueString()
		request.Securable.View = &view
	}

	created, err := r.client.CreateGrant(ctx, request)
	if err != nil {
		resp.Diagnostics.AddError("Creating grant failed", err.Error())
		return
	}

	plan.ID = types.StringValue(created.ID)
	plan.SecurableID = types.StringValue(created.SecurableID)
	resp.Diagnostics.Append(resp.State.Set(ctx, &plan)...)
}

func (r *grantResource) Read(ctx context.Context, req resource.ReadRequest, resp *resource.ReadResponse) {
	var state grantResourceModel
	resp.Diagnostics.Append(req.State.Get(ctx, &state)...)
	if resp.Diagnostics.HasError() {
		return
	}

	grant, err := r.client.GetGrant(ctx, state.ID.ValueString())
	if client.IsNotFound(err) {
		resp.State.RemoveResource(ctx)
		return
	}
	if err != nil {
		resp.Diagnostics.AddError("Reading grant failed", err.Error())
		return
	}

	// The API renders the securable as a (type, ULID) pair, not by name,
	// so the name-addressed securable block stays as configured; the
	// resolved ULID is refreshed alongside it.
	state.Privilege = types.StringValue(grant.Privilege)
	state.SecurableID = types.StringValue(grant.SecurableID)
	if grant.Role != nil {
		state.Role = types.StringValue(*grant.Role)
	} else {
		state.Role = types.StringNull()
	}
	if grant.PrincipalID != nil {
		state.PrincipalID = types.StringValue(*grant.PrincipalID)
	} else {
		state.PrincipalID = types.StringNull()
	}
	resp.Diagnostics.Append(resp.State.Set(ctx, &state)...)
}

// Update is unreachable: grants are immutable and every attribute carries
// RequiresReplace.
func (r *grantResource) Update(_ context.Context, _ resource.UpdateRequest, resp *resource.UpdateResponse) {
	resp.Diagnostics.AddError(
		"Grant update is not supported",
		"Meridian grants are immutable; all changes force replacement. "+
			"Hitting this is a provider bug — please report it.",
	)
}

func (r *grantResource) Delete(ctx context.Context, req resource.DeleteRequest, resp *resource.DeleteResponse) {
	var state grantResourceModel
	resp.Diagnostics.Append(req.State.Get(ctx, &state)...)
	if resp.Diagnostics.HasError() {
		return
	}
	err := r.client.DeleteGrant(ctx, state.ID.ValueString())
	if err != nil && !client.IsNotFound(err) {
		resp.Diagnostics.AddError("Deleting grant failed", err.Error())
	}
}

// ImportState imports a grant by its ULID (`terraform import
// meridian_grant.example 01J...`). Only warehouse-scoped grants can be
// fully imported: the API renders a grant's securable as a ULID, and only
// warehouse ULIDs are resolvable back to names through the management API.
func (r *grantResource) ImportState(ctx context.Context, req resource.ImportStateRequest, resp *resource.ImportStateResponse) {
	grant, err := r.client.GetGrant(ctx, req.ID)
	if err != nil {
		resp.Diagnostics.AddError("Importing grant failed", err.Error())
		return
	}
	if grant.SecurableType != "warehouse" {
		resp.Diagnostics.AddError(
			"Grant import is limited to warehouse-scoped grants",
			fmt.Sprintf("Grant %q attaches to a %s securable. The management API renders a "+
				"grant's securable as a ULID only, and the provider can resolve just warehouse "+
				"ULIDs back to names. Re-create namespace-, table-, and view-scoped grants "+
				"under Terraform management instead of importing them.",
				req.ID, grant.SecurableType),
		)
		return
	}

	warehouses, err := r.client.ListWarehouses(ctx)
	if err != nil {
		resp.Diagnostics.AddError("Importing grant failed", err.Error())
		return
	}
	warehouseName := ""
	for _, warehouse := range warehouses {
		if warehouse.ID == grant.SecurableID {
			warehouseName = warehouse.Name
			break
		}
	}
	if warehouseName == "" {
		resp.Diagnostics.AddError(
			"Importing grant failed",
			fmt.Sprintf("Grant %q attaches to warehouse ULID %q, which no longer resolves to a "+
				"registered warehouse.", req.ID, grant.SecurableID),
		)
		return
	}

	state := grantResourceModel{
		ID:          types.StringValue(grant.ID),
		Privilege:   types.StringValue(grant.Privilege),
		Role:        types.StringNull(),
		PrincipalID: types.StringNull(),
		SecurableID: types.StringValue(grant.SecurableID),
	}
	if grant.Role != nil {
		state.Role = types.StringValue(*grant.Role)
	}
	if grant.PrincipalID != nil {
		state.PrincipalID = types.StringValue(*grant.PrincipalID)
	}
	securable, diags := types.ObjectValue(
		map[string]attrType{
			"type":      types.StringType,
			"warehouse": types.StringType,
			"namespace": types.ListType{ElemType: types.StringType},
			"table":     types.StringType,
			"view":      types.StringType,
		},
		map[string]attrValue{
			"type":      types.StringValue("warehouse"),
			"warehouse": types.StringValue(warehouseName),
			"namespace": types.ListNull(types.StringType),
			"table":     types.StringNull(),
			"view":      types.StringNull(),
		},
	)
	resp.Diagnostics.Append(diags...)
	if resp.Diagnostics.HasError() {
		return
	}
	state.Securable = securable
	resp.Diagnostics.Append(resp.State.Set(ctx, &state)...)
}
