GOLANGCI_LINT_VERSION := v2.12.2
BASELINE := docs/bench/baseline-v0.8.0.txt
FUZZTIME ?= 30s

.DEFAULT_GOAL := help

.PHONY: help
help: ## List available targets
	@awk 'BEGIN{FS=":.*##"} /^[a-zA-Z_-]+:.*##/{printf "  \033[36m%-14s\033[0m %s\n", $$1, $$2}' $(MAKEFILE_LIST)

.PHONY: test
test: ## Run the test suite with the race detector and shuffled ordering
	go test -race -shuffle=on -count=1 -timeout=300s ./...

.PHONY: test-short
test-short: ## Run the test suite without the race detector, for a fast inner loop
	go test -count=1 ./...

.PHONY: vet
vet: ## Run go vet
	go vet ./...

.PHONY: lint
lint: ## Run golangci-lint
	golangci-lint run ./...

.PHONY: fmt
fmt: ## Apply gofmt/goimports via golangci-lint
	golangci-lint fmt

.PHONY: fmt-check
fmt-check: ## Report formatting differences without writing (what CI runs)
	golangci-lint fmt --diff

.PHONY: cover
cover: ## Produce coverage.out across all packages and open the HTML report
	go test -race -covermode=atomic \
		-coverpkg="$$(go list ./... | grep -v /examples | grep -v test$$ | paste -sd,)" \
		-coverprofile=coverage.out -timeout=300s ./...
	go tool cover -func=coverage.out | tail -1
	go tool cover -html=coverage.out

.PHONY: bench
bench: ## Run all benchmarks
	go test -run=NONE -bench=. -benchmem -count=6 ./...

.PHONY: benchstat
benchstat: ## Compare current benchmarks against the recorded baseline
	go test -run=NONE -bench=. -benchmem -count=6 ./... > docs/bench/current.txt
	benchstat $(BASELINE) docs/bench/current.txt

.PHONY: vuln
vuln: ## Scan dependencies for known vulnerabilities
	govulncheck ./...

.PHONY: tidy
tidy: ## Tidy go.mod/go.sum
	go mod tidy

.PHONY: tools
tools: ## Install the pinned developer tooling
	go install github.com/golangci/golangci-lint/v2/cmd/golangci-lint@$(GOLANGCI_LINT_VERSION)
	go install golang.org/x/vuln/cmd/govulncheck@latest
	go install golang.org/x/perf/cmd/benchstat@latest

.PHONY: ci
ci: vet lint fmt-check test ## Run everything CI runs, locally

.PHONY: fuzz
fuzz: ## Fuzz each target briefly (the seed corpus already runs under `make test`)
	@# Derived from `go test -list`, not hardcoded: a moved fuzz target makes
	@# `go test ./pkg/ -fuzz='^Gone$$'` exit 0 with "no fuzz tests to fuzz", so a
	@# stale path is a silent pass rather than a failure. Three restructures in
	@# three releases moved one.
	@set -e; found=0; \
	for pkg in $$(go list ./... | grep -v /examples); do \
	  for t in $$(go test "$$pkg" -list='^Fuzz' | grep '^Fuzz' || true); do \
	    echo "--- $$pkg $$t"; \
	    go test "$$pkg" -run=NONE -fuzz="^$$t$$" -fuzztime=$(FUZZTIME); \
	    found=$$((found + 1)); \
	  done; \
	done; \
	test "$$found" -gt 0 || { echo "no fuzz targets found at all"; exit 1; }; \
	echo "fuzzed $$found targets"
