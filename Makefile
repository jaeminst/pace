GOLANGCI_LINT_VERSION := v2.12.2
BASELINE := docs/bench/baseline-v0.7.0.txt
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
		-coverpkg="$$(go list ./... | grep -v /examples | paste -sd,)" \
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
	@echo "--- FuzzRestoreBucket"
	@go test ./bucket/     -run=NONE -fuzz='^FuzzRestoreBucket$$' -fuzztime=$(FUZZTIME)
	@echo "--- FuzzDrainInstant"
	@go test ./bucket/     -run=NONE -fuzz='^FuzzDrainInstant$$' -fuzztime=$(FUZZTIME)
	@echo "--- FuzzShardIndex"
	@go test ./registry/   -run=NONE -fuzz='^FuzzShardIndex$$' -fuzztime=$(FUZZTIME)
	@echo "--- FuzzBuild"
	@go test ./urlx/       -run=NONE -fuzz='^FuzzBuild$$' -fuzztime=$(FUZZTIME)
	@echo "--- FuzzLimitString"
	@go test ./rate/       -run=NONE -fuzz='^FuzzLimitString$$' -fuzztime=$(FUZZTIME)
	@echo "--- FuzzRetryAfter"
	@go test ./response/   -run=NONE -fuzz='^FuzzRetryAfter$$' -fuzztime=$(FUZZTIME)
