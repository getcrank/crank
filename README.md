# Crank

<p>
  <a href="https://github.com/ogwurujohnson/crank/releases"><img src="https://img.shields.io/github/release/ogwurujohnson/crank.svg" alt="Latest Release"></a>
  <a href="https://pkg.go.dev/github.com/ogwurujohnson/crank?tab=doc"><img src="https://godoc.org/github.com/golang/gddo?status.svg" alt="Go Docs"></a>
  <a href="https://github.com/ogwurujohnson/crank/actions/workflows/ci.yml"><img src="https://github.com/ogwurujohnson/crank/workflows/ci/badge.svg" alt="Build Status"></a>
</p>

Crank is a background job processing SDK for Go. Enqueue jobs to named queues, run concurrent workers, and observe execution via middleware, validation, and metrics hooks — all from a single package: `github.com/ogwurujohnson/crank`.

Broker backends are pluggable. **Redis** is supported today; **NATS** and **PostgreSQL** are reserved for future implementations. You can also provide your own backend with `WithCustomBroker()`. Inspired by [Sidekiq](https://github.com/sidekiq/sidekiq), designed to feel idiomatic in Go.

## Installation

```bash
go get github.com/ogwurujohnson/crank
```

## Quick Start

```go
engine, client, err := crank.New("redis://localhost:6379/0",
    crank.WithBroker("redis"),
    crank.WithConcurrency(10),
    crank.WithTimeout(8*time.Second),
    crank.WithQueues(crank.QueueOption{Name: "default", Weight: 1}),
)
if err != nil {
    log.Fatalf("failed to create engine: %v", err)
}
defer engine.Stop()

crank.SetGlobalClient(client)
engine.Register("EmailWorker", EmailWorker{})

if err := engine.Start(); err != nil {
    log.Fatalf("engine start: %v", err)
}

jid, _ := crank.Enqueue(context.Background(), "EmailWorker", "default", "user-123")
```

Or from a YAML config:

```go
engine, client, err := crank.QuickStart("config/crank.yml")
```

## Health Checks

`engine.Health(ctx)` returns a snapshot for use in liveness/readiness probes. The broker ping is bounded by the supplied context.

```go
mux.HandleFunc("/healthz", func(w http.ResponseWriter, r *http.Request) {
    ctx, cancel := context.WithTimeout(r.Context(), 500*time.Millisecond)
    defer cancel()
    h := engine.Health(ctx)
    if h.Status != crank.HealthOK {
        w.WriteHeader(http.StatusServiceUnavailable)
    }
    _ = json.NewEncoder(w).Encode(h)
})
```

See [the docs](https://crank.dev/docs/advanced#health-checks) for the full `HealthStatus` shape.

## Testing without Redis

`NewTestEngine` returns an engine backed by an in-memory broker — no external dependencies needed:

```go
engine, client, tb, err := crank.NewTestEngine(
    crank.WithConcurrency(2),
    crank.WithTimeout(5*time.Second),
)
engine.Register("MyWorker", myWorker{})
engine.Start()
defer engine.Stop()

client.Enqueue(context.Background(), "MyWorker", "default", "arg1")

// Inspect state after processing
retry := tb.RetryJobs()
dead := tb.DeadJobs()
```

## Features

- **Explicit broker selection**: `WithBroker("redis")` or `WithCustomBroker()` — no implicit defaults.
- **Fluent API**: `New(brokerURL, opts...)` with `WithBroker`, `WithCustomBroker`, `WithConcurrency`, `WithTimeout`, `WithQueues`, `WithLogger`, etc.
- **YAML config**: `QuickStart(path)` for file-driven setup.
- **Workers**: Implement `crank.Worker` (`Perform(ctx, args...) error`); register with `engine.Register` or `engine.RegisterMany`.
- **Weighted queues**: Named queues polled by weight.
- **Retries and dead queue**: Exponential backoff with configurable retry count; exhausted jobs move to a dead set.
- **Middleware**: Built-in recovery, logging, and circuit breaker; extend with `engine.Use()`.
- **Validation and redaction**: Global validators (`ClassAllowlist`, `MaxPayloadSize`, etc.) and argument redactors for safe logging.
- **Lifecycle logging**: Enqueue, dequeue, processed, failed, and dead queue events logged when a logger is provided.
- **Stats**: `engine.Stats()` returns processed, failed, retry, dead, and per-queue counts.
- **Health checks**: `engine.Health(ctx)` returns a cheap snapshot (started, broker reachable, ping latency, worker count) for liveness/readiness probes.
- **Global client**: `SetGlobalClient(client)` then `crank.Enqueue(...)` from anywhere.

## Benchmarks

Measured on Apple M1, Go 1.24, in-memory broker (`go test -bench=. -benchmem ./tests/`):

| Benchmark | ops/sec | ns/op | B/op | allocs/op |
|-----------|--------:|------:|-----:|----------:|
| Enqueue (single) | 1,701,962 | 669 | 481 | 9 |
| Enqueue (parallel, 8 goroutines) | 1,272,718 | 1,029 | 484 | 9 |
| Broker Enqueue (raw) | 2,117,263 | 544 | 313 | 5 |
| Broker Dequeue (raw) | 4,198,285 | 348 | 248 | 3 |
| Job ToJSON | 2,006,526 | 631 | 320 | 4 |
| Job FromJSON | 479,511 | 2,575 | 992 | 23 |
| Middleware Chain (recovery + logging) | 180,486,771 | 6 | 0 | 0 |
| Circuit Breaker Allow (single) | 18,271,932 | 59 | 0 | 0 |
| Circuit Breaker Allow (parallel) | 6,381,554 | 184 | 0 | 0 |

```bash
go test -bench=. -benchmem ./tests/
```

## Security

See [SECURITY.md](SECURITY.md) for broker credentials, TLS, redaction, validation, and vulnerability reporting.

## License

See [LICENSE](LICENSE) for details.

**Maintainer:** ogwurujohnson@gmail.com
