package provider

import (
	"context"

	"github.com/hashicorp/terraform-plugin-framework/datasource"
	"github.com/hashicorp/terraform-plugin-framework/datasource/schema"
	"github.com/hashicorp/terraform-plugin-framework/types"

	"github.com/meridian-catalog/meridian/terraform-provider-meridian/internal/client"
)

// NewSearchDataSource returns the meridian_search data source.
func NewSearchDataSource() datasource.DataSource {
	return &searchDataSource{}
}

// searchDataSource runs one ranked full-text catalog search via
// GET /api/v2/search and exposes the first page of results. Results are
// already filtered to the caller's visibility by the server.
type searchDataSource struct {
	client *client.Client
}

type searchDataSourceModel struct {
	Query     types.String        `tfsdk:"query"`
	Types     types.List          `tfsdk:"types"`
	Warehouse types.String        `tfsdk:"warehouse"`
	Namespace types.String        `tfsdk:"namespace"`
	Limit     types.Int64         `tfsdk:"limit"`
	Results   []searchResultModel `tfsdk:"results"`
}

type searchResultModel struct {
	Type      types.String  `tfsdk:"type"`
	ID        types.String  `tfsdk:"id"`
	Name      types.String  `tfsdk:"name"`
	Namespace types.List    `tfsdk:"namespace"`
	Warehouse types.String  `tfsdk:"warehouse"`
	Rank      types.Float64 `tfsdk:"rank"`
	Snippet   types.String  `tfsdk:"snippet"`
}

func (d *searchDataSource) Metadata(_ context.Context, req datasource.MetadataRequest, resp *datasource.MetadataResponse) {
	resp.TypeName = req.ProviderTypeName + "_search"
}

func (d *searchDataSource) Schema(_ context.Context, _ datasource.SchemaRequest, resp *datasource.SchemaResponse) {
	resp.Schema = schema.Schema{
		Description: "Runs one ranked full-text search over catalog assets (tables, views, " +
			"namespaces) and returns the first page of results. Results are filtered to the " +
			"caller's visibility by the server.",
		Attributes: map[string]schema.Attribute{
			"query": schema.StringAttribute{
				Required:    true,
				Description: "Full-text query string.",
			},
			"types": schema.ListAttribute{
				ElementType: types.StringType,
				Optional:    true,
				Description: "Restrict to these asset types: table, view, namespace. Omit for all.",
			},
			"warehouse": schema.StringAttribute{
				Optional:    true,
				Description: "Restrict to one warehouse by name.",
			},
			"namespace": schema.StringAttribute{
				Optional:    true,
				Description: "Restrict to a dot-separated namespace path prefix (e.g. \"db.sales\").",
			},
			"limit": schema.Int64Attribute{
				Optional:    true,
				Description: "Maximum results to return (1–100). Omit for the server default (20).",
			},
			"results": schema.ListNestedAttribute{
				Computed:    true,
				Description: "Ranked search hits, most relevant first.",
				NestedObject: schema.NestedAttributeObject{
					Attributes: map[string]schema.Attribute{
						"type": schema.StringAttribute{
							Computed:    true,
							Description: "Asset type: table, view, or namespace.",
						},
						"id": schema.StringAttribute{
							Computed:    true,
							Description: "ULID of the matched asset.",
						},
						"name": schema.StringAttribute{
							Computed:    true,
							Description: "Asset name.",
						},
						"namespace": schema.ListAttribute{
							ElementType: types.StringType,
							Computed:    true,
							Description: "Namespace path levels of the asset.",
						},
						"warehouse": schema.StringAttribute{
							Computed:    true,
							Description: "Warehouse the asset belongs to.",
						},
						"rank": schema.Float64Attribute{
							Computed:    true,
							Description: "Relevance rank (higher is more relevant).",
						},
						"snippet": schema.StringAttribute{
							Computed:    true,
							Description: "Highlighted match snippet.",
						},
					},
				},
			},
		},
	}
}

func (d *searchDataSource) Configure(_ context.Context, req datasource.ConfigureRequest, resp *datasource.ConfigureResponse) {
	d.client = configureClient(req.ProviderData, &resp.Diagnostics)
}

func (d *searchDataSource) Read(ctx context.Context, req datasource.ReadRequest, resp *datasource.ReadResponse) {
	var config searchDataSourceModel
	resp.Diagnostics.Append(req.Config.Get(ctx, &config)...)
	if resp.Diagnostics.HasError() {
		return
	}

	query := client.SearchQuery{
		Query:     config.Query.ValueString(),
		Warehouse: config.Warehouse.ValueString(),
		Namespace: config.Namespace.ValueString(),
	}
	if !config.Limit.IsNull() {
		query.Limit = config.Limit.ValueInt64()
	}
	if !config.Types.IsNull() {
		resp.Diagnostics.Append(config.Types.ElementsAs(ctx, &query.Types, false)...)
		if resp.Diagnostics.HasError() {
			return
		}
	}

	response, err := d.client.Search(ctx, query)
	if err != nil {
		resp.Diagnostics.AddError("Search failed", err.Error())
		return
	}

	config.Results = make([]searchResultModel, 0, len(response.Results))
	for _, hit := range response.Results {
		namespace, listDiags := types.ListValueFrom(ctx, types.StringType, hit.Namespace)
		resp.Diagnostics.Append(listDiags...)
		if resp.Diagnostics.HasError() {
			return
		}
		config.Results = append(config.Results, searchResultModel{
			Type:      types.StringValue(hit.Type),
			ID:        types.StringValue(hit.ID),
			Name:      types.StringValue(hit.Name),
			Namespace: namespace,
			Warehouse: types.StringValue(hit.Warehouse),
			Rank:      types.Float64Value(hit.Rank),
			Snippet:   types.StringValue(hit.Snippet),
		})
	}
	resp.Diagnostics.Append(resp.State.Set(ctx, &config)...)
}
