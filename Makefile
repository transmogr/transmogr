SHELL := /bin/bash

# ─── Tools ───────────────────────────────────────────────────────────────────
GO     ?= go
DOCKER ?= docker
BUF    ?= buf
GOCACHE ?= /tmp/transmogr-go-build

# ─── Lint ────────────────────────────────────────────────────────────────────
GOLANGCI_LINT_VERSION ?= v2.10-alpine
GOLANGCI_LINT_IMAGE   ?= golangci/golangci-lint:$(GOLANGCI_LINT_VERSION)

# ─── Docker images ───────────────────────────────────────────────────────────
IMAGE_REPO  ?= transmogr
PLATFORMS   ?= linux/amd64,linux/arm64

# ─── Phony targets ───────────────────────────────────────────────────────────
.PHONY: proto proto-check \
        lint lint-docker \
        test test-e2e test-e2e-smoke test-e2e-slow test-e2e-memory-soak \
        docker-build \
        help

# ─── Default target ──────────────────────────────────────────────────────────
.DEFAULT_GOAL := help

# ─── Protobuf ────────────────────────────────────────────────────────────────
proto: ## Regenerate protobuf code
	$(BUF) generate

proto-check: ## Regenerate and verify no proto diff is committed
	$(BUF) generate
	git diff --exit-code -- pkg/proto

# ─── Lint ────────────────────────────────────────────────────────────────────
lint-docker: ## Run golangci-lint inside Docker (no local install required)
	@echo "Running golangci-lint in Docker image $(GOLANGCI_LINT_IMAGE)"
	@$(DOCKER) run --rm \
		-e CGO_ENABLED=0 \
		-v $(shell pwd):/app \
		-w /app \
		$(GOLANGCI_LINT_IMAGE) \
		golangci-lint run ./...

# ─── Tests ───────────────────────────────────────────────────────────────────
test: ## Run all tests
	GOCACHE=$(GOCACHE) $(GO) test ./...

test-e2e: ## Run all end-to-end tests
	GOCACHE=$(GOCACHE) $(GO) test ./tests/e2e/...

test-e2e-smoke: ## Run fast end-to-end smoke tests
	GOCACHE=$(GOCACHE) $(GO) test ./tests/e2e/... -run 'TestSmoke'

test-e2e-slow: ## Run slower end-to-end resilience tests
	GOCACHE=$(GOCACHE) $(GO) test ./tests/e2e/... -run 'TestSlow'

test-e2e-memory-soak: ## Run long-running memory soak end-to-end tests
	GOCACHE=$(GOCACHE) $(GO) test -tags soak ./tests/e2e/... -run 'TestSoak'

# ─── Docker images ───────────────────────────────────────────────────────────
docker-build: ## Build and push transmogr image for all platforms (PLATFORMS=linux/amd64,linux/arm64)
	$(DOCKER) buildx build -t $(IMAGE_REPO)/transmogr:latest .

# ─── Help ────────────────────────────────────────────────────────────────────
help: ## Show this help message
	@grep -E '^[a-zA-Z_-]+:.*?## .*$$' $(MAKEFILE_LIST) \
		| awk 'BEGIN {FS = ":.*?## "}; {printf "  \033[36m%-28s\033[0m %s\n", $$1, $$2}'
