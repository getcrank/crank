.PHONY: build test test-race test-cover run-example deps clean fmt vet lint help

# Default target
help:
	@echo "Crank SDK Makefile"
	@echo ""
	@echo "  build        - Build the example runner binary (./bin/crank-example)"
	@echo "  test         - Run tests"
	@echo "  test-race    - Run tests with race detector"
	@echo "  test-cover   - Run tests with coverage report"
	@echo "  deps         - Download and tidy Go modules"
	@echo "  fmt          - Format code (gofmt)"
	@echo "  vet          - Run go vet"
	@echo "  lint         - Run golangci-lint (installs if missing)"
	@echo "  clean        - Remove build artifacts"

# Run tests
test:
	go test ./...

# Run tests with race detector
test-race:
	go test -race ./...

# Run tests with coverage
test-cover:
	go test -cover ./...

# Install/update dependencies
deps:
	go mod download
	go mod tidy

# Format code
fmt:
	go fmt ./...

# Vet code
vet:
	go vet ./...

# Lint (requires golangci-lint)
lint:
	@command -v golangci-lint >/dev/null 2>&1 || { echo "Installing golangci-lint..."; go install github.com/golangci/golangci-lint/v2/cmd/golangci-lint@latest; }
	golangci-lint run

# Clean build artifacts
clean:
	rm -rf bin/
	go clean
