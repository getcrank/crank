# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## Project Overview

Crank is a background job processing SDK for Go (`github.com/ogwurujohnson/crank`). It provides job enqueueing, concurrent worker execution, middleware, retries with exponential backoff, and a dead queue. The broker backend is pluggable — Redis is implemented; NATS and RabbitMQ are reserved stubs.

## Commands

```bash
make test           # Run all tests
make test-race      # Run tests with race detector
make fmt            # Format code (gofmt)
make vet            # Run go vet
make lint           # Run golangci-lint (installs if missing)
make build          # Build example binary to ./bin/crank-example
make run-example    # Build and run the example

# Run a single test
go test -run TestName ./...
go test -run TestName ./internal/queue/

# Coverage
go test -cover ./...
```

## Architecture

The SDK is a single Go module. The public API lives in the root package (`crank.go`, `engine.go`, `options.go`) and re-exports internal types so consumers only import `github.com/ogwurujohnson/crank`.

### Internal packages (`internal/`)

- **broker** — `Broker` interface and implementations. `factory.go` contains `Open()` which routes by broker kind (redis/nats/rabbitmq) or infers from URL scheme. `redis_broker.go` is the production implementation; `memory.go` is the in-memory broker for tests.
- **queue** — Core processing engine. `processor.go` runs the polling loop, dispatches jobs to workers with timeout/context, handles retries and dead queue. `middleware.go` has the middleware chain (recovery, logging, circuit breaker). `breaker.go` is the circuit breaker. `worker.go` defines the `Worker` interface.
- **client** — Job enqueueing. Supports instance-based and global (singleton) client patterns via `SetGlobal`/`GetGlobal`.
- **payload** — `Job` struct, JSON serialization, `JobOptions`, validators (`MaxArgsCount`, `ClassAllowlist`, `ClassPattern`, `MaxPayloadSize`), and redactors for sensitive args.
- **config** — YAML config loading (`Load(path)`) and the `Config` struct shared across packages.

### Key patterns

- **Functional options**: `New(brokerURL, opts...)` with `WithConcurrency`, `WithTimeout`, `WithQueues`, etc.
- **Two init paths**: `New()` (programmatic) and `QuickStart(configPath)` (YAML-driven). Both produce `(*Engine, *Client, error)`.
- **Test harness**: `NewTestEngine()` returns an engine backed by `InMemoryBroker` — no Redis needed. The `TestBroker` wrapper exposes `RetryJobs()` and `DeadJobs()` for assertions.
- **Type re-exports**: Root package type-aliases and var-binds internal types (`Worker`, `Job`, `Client`, `Middleware`, etc.) so users never import `internal/`.

### Processing flow

1. `Engine.Start()` → `Processor.Start()` → spawns worker goroutines polling queues by weight
2. Jobs dequeued from broker → middleware chain → `Worker.Perform(ctx, args...)`
3. On failure: retry with exponential backoff (up to max retries) → then dead queue
4. `Engine.Stop()` gracefully shuts down workers
