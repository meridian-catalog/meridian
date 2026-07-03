// Command terraform-provider-meridian is the Terraform provider plugin for
// Meridian. It speaks only to the Meridian management API (/api/v2) and IRC
// endpoints — there are no side channels.
package main

import (
	"context"
	"flag"
	"log"

	"github.com/hashicorp/terraform-plugin-framework/providerserver"

	"github.com/meridian-catalog/meridian/terraform-provider-meridian/internal/provider"
)

// version is stamped at build time with -ldflags "-X main.version=...".
var version = "dev"

func main() {
	var debug bool
	flag.BoolVar(&debug, "debug", false, "run the provider with support for debuggers like delve")
	flag.Parse()

	opts := providerserver.ServeOpts{
		// Registry address the provider is published under. Publishing to a
		// registry requires a standalone repository — see the README.
		Address: "registry.terraform.io/meridian-catalog/meridian",
		Debug:   debug,
	}

	if err := providerserver.Serve(context.Background(), provider.New(version), opts); err != nil {
		log.Fatal(err.Error())
	}
}
