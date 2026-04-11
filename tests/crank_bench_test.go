package crank_test

import (
	"context"
	"sync"
	"testing"
	"time"

	"github.com/ogwurujohnson/crank"
	"github.com/ogwurujohnson/crank/internal/broker"
	"github.com/ogwurujohnson/crank/internal/payload"
	"github.com/ogwurujohnson/crank/internal/queue"
)

// BenchmarkEnqueue measures raw enqueue throughput against the in-memory broker.
func BenchmarkEnqueue(b *testing.B) {
	_, client, _, err := crank.NewTestEngine(
		crank.WithConcurrency(1),
		crank.WithTimeout(5*time.Second),
	)
	if err != nil {
		b.Fatalf("NewTestEngine: %v", err)
	}

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		_, err := client.Enqueue("BenchWorker", "default", "arg1", i)
		if err != nil {
			b.Fatalf("Enqueue: %v", err)
		}
	}
}

// BenchmarkEnqueueParallel measures enqueue throughput under contention.
func BenchmarkEnqueueParallel(b *testing.B) {
	_, client, _, err := crank.NewTestEngine(
		crank.WithConcurrency(1),
		crank.WithTimeout(5*time.Second),
	)
	if err != nil {
		b.Fatalf("NewTestEngine: %v", err)
	}

	b.ResetTimer()
	b.RunParallel(func(pb *testing.PB) {
		i := 0
		for pb.Next() {
			client.Enqueue("BenchWorker", "default", "arg1", i)
			i++
		}
	})
}

// BenchmarkInMemoryBrokerEnqueue measures the raw broker enqueue performance.
func BenchmarkInMemoryBrokerEnqueue(b *testing.B) {
	br := broker.NewInMemoryBroker()
	defer br.Close()

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		br.Enqueue("default", payload.NewJob("BenchWorker", "default", "arg1"))
	}
}

// BenchmarkInMemoryBrokerDequeue measures dequeue performance with pre-filled queue.
func BenchmarkInMemoryBrokerDequeue(b *testing.B) {
	br := broker.NewInMemoryBroker()
	defer br.Close()

	// Pre-fill
	for i := 0; i < b.N; i++ {
		br.Enqueue("default", payload.NewJob("W", "default", i))
	}

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		br.Dequeue([]string{"default"}, 10*time.Millisecond)
	}
}

// BenchmarkJobSerialization measures JSON marshal/unmarshal round-trip.
func BenchmarkJobSerialization(b *testing.B) {
	job := payload.NewJob("BenchWorker", "default", "arg1", 42, map[string]interface{}{"key": "val"})

	b.Run("ToJSON", func(b *testing.B) {
		for i := 0; i < b.N; i++ {
			_, err := job.ToJSON()
			if err != nil {
				b.Fatal(err)
			}
		}
	})

	data, _ := job.ToJSON()
	b.Run("FromJSON", func(b *testing.B) {
		for i := 0; i < b.N; i++ {
			_, err := payload.FromJSON(data)
			if err != nil {
				b.Fatal(err)
			}
		}
	})
}

// BenchmarkMiddlewareChain measures overhead of the middleware chain.
func BenchmarkMiddlewareChain(b *testing.B) {
	nopLog := queue.NopLogger()
	chain := queue.NewChain(
		queue.RecoveryMiddleware(nopLog),
		queue.LoggingMiddleware(nopLog),
	)
	handler := chain.Wrap(func(ctx context.Context, job *payload.Job) error {
		return nil
	})
	job := payload.NewJob("W", "default")
	ctx := context.Background()

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		handler(ctx, job)
	}
}

// BenchmarkFullJobProcessing measures end-to-end job processing throughput
// including enqueue, dequeue, middleware, and worker execution.
func BenchmarkFullJobProcessing(b *testing.B) {
	engine, client, _, err := crank.NewTestEngine(
		crank.WithConcurrency(4),
		crank.WithTimeout(5*time.Second),
	)
	if err != nil {
		b.Fatalf("NewTestEngine: %v", err)
	}

	var wg sync.WaitGroup
	engine.Register("BenchWorker", &testWorker{
		onPerform: func(ctx context.Context, args ...interface{}) error {
			wg.Done()
			return nil
		},
	})

	if err := engine.Start(); err != nil {
		b.Fatalf("Start: %v", err)
	}
	defer engine.Stop()

	b.ResetTimer()
	wg.Add(b.N)
	for i := 0; i < b.N; i++ {
		client.Enqueue("BenchWorker", "default", i)
	}
	wg.Wait()
}

// BenchmarkCircuitBreakerAllow measures circuit breaker allow check overhead.
func BenchmarkCircuitBreakerAllow(b *testing.B) {
	cb := queue.NewCircuitBreaker(queue.BreakerConfig{
		FailureThreshold: 100,
		Window:           time.Minute,
		ResetTimeout:     time.Minute,
	})

	b.Run("Closed", func(b *testing.B) {
		for i := 0; i < b.N; i++ {
			cb.Allow("BenchClass")
		}
	})

	b.Run("Parallel", func(b *testing.B) {
		b.RunParallel(func(pb *testing.PB) {
			for pb.Next() {
				cb.Allow("BenchClass")
			}
		})
	})
}
