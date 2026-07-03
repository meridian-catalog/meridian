# Makefile for terraform-provider-meridian.
#
# Common targets:
#   make build        compile the provider binary
#   make install      install into the local Terraform/OpenTofu plugin dir
#   make test         run unit tests (fast, no server needed)
#   make testacc      run acceptance tests against a live Meridian server
#   make fmt vet lint static checks
#   make tidy         go mod tidy

BINARY      := terraform-provider-meridian
VERSION     ?= dev
# Local plugin mirror path uses a placeholder namespace; the provider is
# not published to a registry yet (that needs a standalone repo).
HOSTNAME    := registry.terraform.io
NAMESPACE   := meridian-catalog
NAME        := meridian
OS_ARCH     := $(shell go env GOOS)_$(shell go env GOARCH)
PLUGIN_DIR  := $(HOME)/.terraform.d/plugins/$(HOSTNAME)/$(NAMESPACE)/$(NAME)/$(VERSION)/$(OS_ARCH)

# tofu is preferred (lighter); fall back to terraform if only that exists.
TF_BIN      := $(shell command -v tofu 2>/dev/null || command -v terraform 2>/dev/null)

.PHONY: build install test testacc fmt vet lint tidy clean

build:
	go build -ldflags "-X main.version=$(VERSION)" -o $(BINARY) .

install: build
	mkdir -p $(PLUGIN_DIR)
	cp $(BINARY) $(PLUGIN_DIR)/$(BINARY)_v$(VERSION)

test:
	go test ./internal/client/... ./... -count=1

# Acceptance tests stand up real resources against a live server.
# Requires: a running Meridian server (default http://localhost:8181,
# override with MERIDIAN_ENDPOINT) and tofu or terraform on PATH.
testacc:
	@test -n "$(TF_BIN)" || (echo "error: install tofu or terraform (e.g. 'brew install opentofu')"; exit 1)
	TF_ACC=1 TF_ACC_TERRAFORM_PATH="$(TF_BIN)" go test ./internal/provider/... -v -timeout 30m -count=1

fmt:
	gofmt -w .

vet:
	go vet ./...

lint: fmt vet
	@test -z "$$(gofmt -l .)" || (echo "gofmt found unformatted files"; gofmt -l .; exit 1)

tidy:
	go mod tidy

clean:
	rm -f $(BINARY)
