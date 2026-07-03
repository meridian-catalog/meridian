package provider

import (
	"fmt"

	"github.com/hashicorp/terraform-plugin-framework/attr"
	diagpkg "github.com/hashicorp/terraform-plugin-framework/diag"
	"github.com/hashicorp/terraform-plugin-framework/resource/schema/mapplanmodifier"
	"github.com/hashicorp/terraform-plugin-framework/resource/schema/planmodifier"
	"github.com/hashicorp/terraform-plugin-framework/types/basetypes"

	"github.com/meridian-catalog/meridian/terraform-provider-meridian/internal/client"
)

// diag aliases the framework's diagnostics collection for local helpers.
type diag = diagpkg.Diagnostics

// attrType and attrValue alias the framework's attribute type/value
// interfaces, used when building nested object values by hand (e.g. the
// grant securable during import).
type (
	attrType  = attr.Type
	attrValue = attr.Value
)

// objectAsOptions is the standard decode policy for reading a nested
// object attribute into a Go struct: unknowns and nulls are tolerated so
// helpers can inspect individual fields.
func objectAsOptions() basetypes.ObjectAsOptions {
	return basetypes.ObjectAsOptions{
		UnhandledNullAsEmpty:    true,
		UnhandledUnknownAsEmpty: true,
	}
}

// configureClient extracts the shared API client from provider data,
// tolerating the nil ProviderData the framework passes before Configure.
func configureClient(providerData any, diagnostics *diag) *client.Client {
	if providerData == nil {
		return nil
	}
	apiClient, ok := providerData.(*client.Client)
	if !ok {
		diagnostics.AddError(
			"Unexpected provider data",
			fmt.Sprintf("Expected *client.Client, got %T. This is a provider bug — please report it.", providerData),
		)
		return nil
	}
	return apiClient
}

// mapRequiresReplace returns the RequiresReplace plan modifier for map
// attributes.
func mapRequiresReplace() planmodifier.Map {
	return mapplanmodifier.RequiresReplace()
}
