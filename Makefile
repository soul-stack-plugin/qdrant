.DEFAULT_GOAL := help

# Makefile for the qdrant bundle.
#
# The artifact goes to dist/ and is stamped with its own schema. The name is not
# load-bearing — Keeper takes the single executable it finds in dist/ — but the stamp
# is: an unstamped artifact carries no disclosure, and a reader that cannot read the
# disclosure fails closed rather than approving blind.
#
# This repository commits NO schema.json. The document is derived from the Go values
# in internal/qdrant and written beside the binary by `soul-mod stamp`; a committed
# copy would be one more thing that can go stale between an edit and a re-stamp. The
# release workflow publishes it as an asset so soul-lint can check a destiny without
# downloading a binary for an architecture it does not care about.
#
# SOUL_MOD runs the tool out of the SDK this bundle already depends on, so a fresh
# clone needs nothing installed. Note the absence of an `@version` suffix, and do not
# add one: the versioned form resolves the tool's module independently of this one,
# and the published SDK still carries a `replace` that Go refuses to run a tool
# through. Without the suffix it resolves from this module's own build list, where the
# SDK is already pinned, and it works. Point SOUL_MOD at an installed binary to skip
# the compile on every invocation.

BIN_DIR  := dist
BINARY   := $(BIN_DIR)/qdrant
SOUL_MOD ?= go run github.com/souls-guild/soul-stack/sdk/cmd/soul-mod

# This repository is a standalone module that takes the SDK as an ordinary tagged
# dependency. If it is ever checked out beside a soul-stack tree carrying a go.work,
# that workspace must not capture it.
export GOWORK = off

# QDRANT_ADDR points `make live` at a real instance. No default on purpose: a live
# tier that silently picked a host would be a test suite with an opinion about your
# machine.
QDRANT_ADDR ?=

.PHONY: help build verify test live fmt fmt-check vet check clean

help: ## Show available targets
	@grep -E '^[a-zA-Z_-]+:.*?## ' $(MAKEFILE_LIST) | sort | \
	  awk 'BEGIN{FS=":.*?## "}{printf "%-12s %s\n", $$1, $$2}'

# CGO_ENABLED=0 is not a micro-optimisation: `net` pulls in the cgo resolver by
# default, and the resulting artifact would be linked against the build host's libc.
# An artifact that gets downloaded and run on a host nobody chose has to be static —
# and a cross-compiled arm64 build is static anyway, so without this the two released
# binaries would not even be the same kind of thing.
build: ## Build the artifact into dist/ and stamp its schema into it
	@mkdir -p $(BIN_DIR)
	@CGO_ENABLED=0 go build -trimpath -ldflags="-s -w" -o $(BINARY) ./cmd/qdrant
	@$(SOUL_MOD) stamp $(BINARY)

verify: ## Check the stamped schema still matches the code
	@$(SOUL_MOD) verify $(BINARY)

# -count=1 rather than a bare `go test`: a cached result is an answer about a tree
# that may no longer be the one on disk.
test: ## Run the offline suite (no Qdrant needed)
	@go test -count=1 ./...

live: ## Run the live tier against a real Qdrant (QDRANT_ADDR=host:port)
	@if [ -z "$(QDRANT_ADDR)" ]; then \
	  echo "live: set QDRANT_ADDR, e.g. make live QDRANT_ADDR=127.0.0.1:6333" >&2; \
	  echo "  a throwaway instance: docker run -d -p 6333:6333 qdrant/qdrant" >&2; \
	  exit 1; \
	fi
	@QDRANT_ADDR=$(QDRANT_ADDR) go test -tags e2e_live -count=1 ./...

fmt: ## Rewrite sources with gofmt
	@gofmt -w .

fmt-check: ## Fail if anything is unformatted
	@out=$$(gofmt -l .); \
	 if [ -n "$$out" ]; then echo "gofmt needed:"; echo "$$out"; exit 1; fi

vet: ## Run go vet
	@go vet ./...

check: fmt-check vet test build verify ## The gate: fmt, vet, tests, build, stamp, verify
	@echo "check: green"

clean: ## Remove build output
	@rm -rf $(BIN_DIR)
