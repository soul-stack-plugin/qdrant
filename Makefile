.DEFAULT_GOAL := help

BINARY   := qdrant
DIST     := dist
ARTIFACT := $(DIST)/$(BINARY)

# Everything runs with GOWORK=off. This repository is a standalone Go module that
# takes the Soul Stack SDK as an ordinary tagged dependency; if it is ever checked
# out next to a soul-stack tree carrying a go.work, the workspace must not capture it.
export GOWORK = off

# SOUL_STACK is a checkout of github.com/souls-guild/soul-stack, needed ONLY by
# `make stamp`. It cannot be replaced by `go run ...@version`: the published sdk
# module still carries a `replace` directive, and Go refuses to run a tool out of a
# module whose go.mod has one. Importing the sdk as a library is unaffected — a
# replace in a dependency is ignored — which is why `build`, `test` and `check` need
# nothing outside this repository.
SOUL_STACK ?= ../soul-stack

# QDRANT_ADDR points `make test-live` at a real instance. No default on purpose: a
# live tier that silently picked a host would be a test suite with an opinion about
# your machine.
QDRANT_ADDR ?=

.PHONY: help build test test-live fmt vet check stamp verify clean

help: ## Show available targets
	@grep -E '^[a-zA-Z_-]+:.*?## ' $(MAKEFILE_LIST) | sort | \
	  awk 'BEGIN{FS=":.*?## "}{printf "%-18s %s\n", $$1, $$2}'

build: ## Build the plugin binary into dist/
	@mkdir -p $(DIST)
	@go build -o $(ARTIFACT) .
	@echo "build: $(ARTIFACT)"

fmt: ## Rewrite sources with gofmt
	@gofmt -w .

vet: ## Run go vet
	@go vet ./...

test: ## Run the offline suite (no Qdrant needed)
	@go test -count=1 ./...

test-live: ## Run the live suite against a real Qdrant (QDRANT_ADDR=host:port)
	@if [ -z "$(QDRANT_ADDR)" ]; then \
	  echo "test-live: set QDRANT_ADDR, e.g. make test-live QDRANT_ADDR=127.0.0.1:6333" >&2; \
	  echo "  a throwaway instance: docker run -d -p 6333:6333 qdrant/qdrant" >&2; \
	  exit 1; \
	fi
	@QDRANT_ADDR=$(QDRANT_ADDR) go test -tags e2e_live -count=1 ./...

check: fmt vet test build ## The gate: fmt, vet, offline tests, build
	@echo "check: green"

stamp: build ## Stamp the schema document into the binary and publish schema.json
	@if [ ! -d "$(SOUL_STACK)/sdk" ]; then \
	  echo "stamp: no soul-stack checkout at $(SOUL_STACK)" >&2; \
	  echo "  set it: make stamp SOUL_STACK=/path/to/soul-stack" >&2; \
	  exit 1; \
	fi
	@tmp=$$(mktemp -d); \
	 GOWORK= go -C "$(SOUL_STACK)" build -o "$$tmp/soul-mod" ./sdk/cmd/soul-mod || exit 1; \
	 "$$tmp/soul-mod" stamp "$(ARTIFACT)" || exit 1; \
	 "$$tmp/soul-mod" verify "$(ARTIFACT)" || exit 1; \
	 rm -rf "$$tmp"
	@echo "stamp: $(ARTIFACT) carries its schema, and $(DIST)/schema.json is beside it"

verify: ## Print the sha256 an operator approves in keeper.yml
	@test -f $(ARTIFACT) || { echo "verify: run make stamp first" >&2; exit 1; }
	@sha256sum $(ARTIFACT)

clean: ## Remove build output
	@rm -rf $(DIST)
