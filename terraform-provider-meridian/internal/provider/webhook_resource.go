package provider

import (
	"context"

	"github.com/hashicorp/terraform-plugin-framework/path"
	"github.com/hashicorp/terraform-plugin-framework/resource"
	"github.com/hashicorp/terraform-plugin-framework/resource/schema"
	"github.com/hashicorp/terraform-plugin-framework/resource/schema/listplanmodifier"
	"github.com/hashicorp/terraform-plugin-framework/resource/schema/planmodifier"
	"github.com/hashicorp/terraform-plugin-framework/resource/schema/stringplanmodifier"
	"github.com/hashicorp/terraform-plugin-framework/types"

	"github.com/meridian-catalog/meridian/terraform-provider-meridian/internal/client"
)

// NewWebhookResource returns the meridian_webhook resource.
func NewWebhookResource() resource.Resource {
	return &webhookResource{}
}

// webhookResource maps onto POST/GET /api/v2/webhooks and
// DELETE /api/v2/webhooks/{id}. The management API has no webhook update
// endpoint, so every attribute change forces replacement (delete +
// create). The signing secret is write-only: the server never returns it,
// so it lives only in configuration and state.
type webhookResource struct {
	client *client.Client
}

type webhookResourceModel struct {
	ID         types.String `tfsdk:"id"`
	URL        types.String `tfsdk:"url"`
	EventTypes types.List   `tfsdk:"event_types"`
	Secret     types.String `tfsdk:"secret"`
}

func (r *webhookResource) Metadata(_ context.Context, req resource.MetadataRequest, resp *resource.MetadataResponse) {
	resp.TypeName = req.ProviderTypeName + "_webhook"
}

func (r *webhookResource) Schema(_ context.Context, _ resource.SchemaRequest, resp *resource.SchemaResponse) {
	resp.Schema = schema.Schema{
		Description: "A webhook endpoint that receives CloudEvents deliveries. The management " +
			"API has no webhook update endpoint, so any change forces replacement (delete + " +
			"create). The signing secret is write-only — the server never returns it — so it " +
			"is tracked only in configuration and Terraform state; keep your state backend " +
			"encrypted.",
		Attributes: map[string]schema.Attribute{
			"id": schema.StringAttribute{
				Computed:      true,
				Description:   "Server-assigned ULID of the webhook endpoint.",
				PlanModifiers: []planmodifier.String{stringplanmodifier.UseStateForUnknown()},
			},
			"url": schema.StringAttribute{
				Required: true,
				Description: "Destination URL (must start with http:// or https://). Changing it " +
					"forces replacement.",
				PlanModifiers: []planmodifier.String{stringplanmodifier.RequiresReplace()},
			},
			"event_types": schema.ListAttribute{
				ElementType: types.StringType,
				Optional:    true,
				Description: "CloudEvents type filter (e.g. com.meridian.table.committed). Each " +
					"entry must start with \"com.meridian.\". Omit or leave empty to receive all " +
					"events. Changing it forces replacement.",
				PlanModifiers: []planmodifier.List{listplanmodifier.RequiresReplace()},
			},
			"secret": schema.StringAttribute{
				Required:  true,
				Sensitive: true,
				Description: "HMAC-SHA256 signing secret (at least 16 characters). Write-only: the " +
					"server never returns it, so it is stored only in configuration and state. " +
					"Changing it forces replacement.",
				PlanModifiers: []planmodifier.String{stringplanmodifier.RequiresReplace()},
			},
		},
	}
}

func (r *webhookResource) Configure(_ context.Context, req resource.ConfigureRequest, resp *resource.ConfigureResponse) {
	r.client = configureClient(req.ProviderData, &resp.Diagnostics)
}

func (r *webhookResource) Create(ctx context.Context, req resource.CreateRequest, resp *resource.CreateResponse) {
	var plan webhookResourceModel
	resp.Diagnostics.Append(req.Plan.Get(ctx, &plan)...)
	if resp.Diagnostics.HasError() {
		return
	}

	request := client.CreateWebhookRequest{
		URL:    plan.URL.ValueString(),
		Secret: plan.Secret.ValueString(),
	}
	if !plan.EventTypes.IsNull() {
		resp.Diagnostics.Append(plan.EventTypes.ElementsAs(ctx, &request.EventTypes, false)...)
		if resp.Diagnostics.HasError() {
			return
		}
	}

	created, err := r.client.CreateWebhook(ctx, request)
	if err != nil {
		resp.Diagnostics.AddError("Creating webhook failed", err.Error())
		return
	}

	plan.ID = types.StringValue(created.ID)
	plan.EventTypes = normalizeEventTypes(ctx, plan.EventTypes, created.EventTypes, &resp.Diagnostics)
	if resp.Diagnostics.HasError() {
		return
	}
	resp.Diagnostics.Append(resp.State.Set(ctx, &plan)...)
}

func (r *webhookResource) Read(ctx context.Context, req resource.ReadRequest, resp *resource.ReadResponse) {
	var state webhookResourceModel
	resp.Diagnostics.Append(req.State.Get(ctx, &state)...)
	if resp.Diagnostics.HasError() {
		return
	}

	webhook, err := r.client.GetWebhook(ctx, state.ID.ValueString())
	if client.IsNotFound(err) {
		resp.State.RemoveResource(ctx)
		return
	}
	if err != nil {
		resp.Diagnostics.AddError("Reading webhook failed", err.Error())
		return
	}

	state.URL = types.StringValue(webhook.URL)
	state.EventTypes = normalizeEventTypes(ctx, state.EventTypes, webhook.EventTypes, &resp.Diagnostics)
	if resp.Diagnostics.HasError() {
		return
	}
	// secret stays as configured: the server never returns it.
	resp.Diagnostics.Append(resp.State.Set(ctx, &state)...)
}

// normalizeEventTypes reconciles the server's event-type list with the
// configured one. The server returns an empty list when no filter is set;
// if the configuration left event_types null, keep it null to avoid a
// phantom null-vs-[] diff. Otherwise reflect what the server stored.
func normalizeEventTypes(
	ctx context.Context,
	prior types.List,
	remote []string,
	diagnostics *diag,
) types.List {
	if len(remote) == 0 && (prior.IsNull() || prior.IsUnknown()) {
		return types.ListNull(types.StringType)
	}
	result, listDiags := types.ListValueFrom(ctx, types.StringType, remote)
	diagnostics.Append(listDiags...)
	return result
}

// Update is unreachable: every attribute carries RequiresReplace and the
// management API has no webhook update endpoint.
func (r *webhookResource) Update(_ context.Context, _ resource.UpdateRequest, resp *resource.UpdateResponse) {
	resp.Diagnostics.AddError(
		"Webhook update is not supported",
		"The Meridian management API has no webhook update endpoint; all changes force "+
			"replacement. Hitting this is a provider bug — please report it.",
	)
}

func (r *webhookResource) Delete(ctx context.Context, req resource.DeleteRequest, resp *resource.DeleteResponse) {
	var state webhookResourceModel
	resp.Diagnostics.Append(req.State.Get(ctx, &state)...)
	if resp.Diagnostics.HasError() {
		return
	}
	err := r.client.DeleteWebhook(ctx, state.ID.ValueString())
	if err != nil && !client.IsNotFound(err) {
		resp.Diagnostics.AddError("Deleting webhook failed", err.Error())
	}
}

// ImportState imports a webhook by its ULID (`terraform import
// meridian_webhook.example 01J...`). The signing secret cannot be
// recovered from the server, so imported state carries an empty secret;
// set the secret in configuration and run `terraform apply` — because the
// secret forces replacement, this re-creates the endpoint with the real
// secret. Import is therefore best used to adopt read-only visibility, not
// to preserve the original secret.
func (r *webhookResource) ImportState(ctx context.Context, req resource.ImportStateRequest, resp *resource.ImportStateResponse) {
	resource.ImportStatePassthroughID(ctx, path.Root("id"), req, resp)
}
