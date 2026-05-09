# Contributing to pace

## Prerequisites

- Go 1.25.7 or later
- `golangci-lint` (for linting) — install via [golangci-lint.run](https://golangci-lint.run/usage/install/)

## Running tests

```sh
go test -race -count=1 ./...
```

To view coverage:

```sh
go test -race -count=1 -coverprofile=coverage.out ./...
go tool cover -html=coverage.out
```

## Running the linter

```sh
golangci-lint run ./...
```

## Running benchmarks

```sh
go test -bench=. -benchmem -count=3 ./...
```

## Pull request guidelines

1. Open an issue first for significant changes.
2. Keep commits focused — one logical change per commit.
3. All tests and lint checks must pass before review.
4. Add or update tests for any changed behaviour.
5. Follow existing code style (`gofmt`, no unnecessary comments).

## Code style

- Format with `gofmt` (the CI enforces this via `go vet`).
- Comments: only when the **why** is non-obvious. No godoc for unexported helpers unless the logic is subtle.
- No feature flags or backwards-compatibility shims — change the code directly.
