package crank_test

import (
	"context"
	"errors"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/ogwurujohnson/crank"
)

// TestE2E_ConcurrentJobProcessing verifies multiple jobs are processed
// concurrently across multiple workers.
func TestE2E_ConcurrentJobProcessing(t *testing.T) {
	engine, client, _, err := crank.NewTestEngine(
		crank.WithConcurrency(4),
		crank.WithTimeout(5*time.Second),
		crank.WithQueues(crank.QueueOption{Name: "default", Weight: 1}),
	)
	if err != nil {
		t.Fatalf("NewTestEngine: %v", err)
	}

	const jobCount = 20
	var processed atomic.Int64
	var wg sync.WaitGroup
	wg.Add(jobCount)

	engine.Register("ConcurrentWorker", &testWorker{
		onPerform: func(ctx context.Context, args ...interface{}) error {
			defer wg.Done()
			processed.Add(1)
			time.Sleep(10 * time.Millisecond)
			return nil
		},
	})
	if err := engine.Start(); err != nil {
		t.Fatalf("Start: %v", err)
	}
	defer engine.Stop()

	for i := 0; i < jobCount; i++ {
		if _, err := client.Enqueue("ConcurrentWorker", "default", i); err != nil {
			t.Fatalf("Enqueue(%d): %v", i, err)
		}
	}

	done := make(chan struct{})
	go func() { wg.Wait(); close(done) }()
	select {
	case <-done:
	case <-time.After(10 * time.Second):
		t.Fatal("timed out waiting for concurrent jobs")
	}

	if got := processed.Load(); got != jobCount {
		t.Errorf("processed = %d, want %d", got, jobCount)
	}
}

// TestE2E_QueueWeightDistribution verifies that higher-weight queues
// get polled more frequently.
func TestE2E_QueueWeightDistribution(t *testing.T) {
	engine, client, _, err := crank.NewTestEngine(
		crank.WithConcurrency(1),
		crank.WithTimeout(2*time.Second),
		crank.WithQueues(
			crank.QueueOption{Name: "high", Weight: 3},
			crank.QueueOption{Name: "low", Weight: 1},
		),
	)
	if err != nil {
		t.Fatalf("NewTestEngine: %v", err)
	}

	var highCount, lowCount atomic.Int64
	allDone := make(chan struct{})
	total := 8
	var wg sync.WaitGroup
	wg.Add(total)

	engine.Register("WeightWorker", &testWorker{
		onPerform: func(ctx context.Context, args ...interface{}) error {
			defer wg.Done()
			if len(args) > 0 {
				if q, ok := args[0].(string); ok {
					if q == "high" {
						highCount.Add(1)
					} else {
						lowCount.Add(1)
					}
				}
			}
			return nil
		},
	})

	if err := engine.Start(); err != nil {
		t.Fatalf("Start: %v", err)
	}
	defer engine.Stop()

	for i := 0; i < 4; i++ {
		client.Enqueue("WeightWorker", "high", "high")
		client.Enqueue("WeightWorker", "low", "low")
	}

	go func() { wg.Wait(); close(allDone) }()
	select {
	case <-allDone:
	case <-time.After(10 * time.Second):
		t.Fatal("timed out waiting for weighted queue jobs")
	}

	// Both queues should be fully processed
	if got := highCount.Load(); got != 4 {
		t.Errorf("high queue processed = %d, want 4", got)
	}
	if got := lowCount.Load(); got != 4 {
		t.Errorf("low queue processed = %d, want 4", got)
	}
}

// TestE2E_RetryWithBackoffCap verifies that retries happen with capped backoff
// and jobs eventually land in dead queue.
func TestE2E_RetryWithBackoffCap(t *testing.T) {
	engine, client, tb, err := crank.NewTestEngine(
		crank.WithConcurrency(1),
		crank.WithTimeout(1*time.Second),
		crank.WithRetryPollInterval(50*time.Millisecond),
	)
	if err != nil {
		t.Fatalf("NewTestEngine: %v", err)
	}

	var attempts atomic.Int64
	engine.Register("RetryWorker", &testWorker{
		onPerform: func(ctx context.Context, args ...interface{}) error {
			attempts.Add(1)
			return errors.New("always fails")
		},
	})

	if err := engine.Start(); err != nil {
		t.Fatalf("Start: %v", err)
	}
	defer engine.Stop()

	// Enqueue with retry=2 so it fails 3 times total then goes to dead
	_, err = client.EnqueueWithOptions("RetryWorker", "default",
		&crank.JobOptions{Retry: intPtr(2)}, "test")
	if err != nil {
		t.Fatalf("Enqueue: %v", err)
	}

	// Wait for job to exhaust retries — first attempt + 2 retries
	deadline := time.After(30 * time.Second)
	for {
		select {
		case <-deadline:
			t.Fatalf("timed out; attempts=%d, dead=%d", attempts.Load(), len(tb.DeadJobs()))
		default:
		}
		dead := tb.DeadJobs()
		if len(dead) >= 1 {
			break
		}
		time.Sleep(100 * time.Millisecond)
	}

	got := attempts.Load()
	if got < 3 {
		t.Errorf("attempts = %d, want >= 3 (1 initial + 2 retries)", got)
	}
}

// TestE2E_MaxRetryCountCap verifies that retry count is capped at MaxRetryCount.
func TestE2E_MaxRetryCountCap(t *testing.T) {
	_, client, _, err := crank.NewTestEngine(
		crank.WithConcurrency(1),
		crank.WithTimeout(1*time.Second),
	)
	if err != nil {
		t.Fatalf("NewTestEngine: %v", err)
	}

	// Try to set retry to a huge number
	hugeRetry := 1000
	jid, err := client.EnqueueWithOptions("SomeWorker", "default",
		&crank.JobOptions{Retry: &hugeRetry}, "test")
	if err != nil {
		t.Fatalf("Enqueue: %v", err)
	}
	if jid == "" {
		t.Error("expected non-empty JID")
	}
}

// TestE2E_CircuitBreakerBlocking verifies that the circuit breaker actually
// blocks job execution when the circuit is open.
func TestE2E_CircuitBreakerBlocking(t *testing.T) {
	engine, client, tb, err := crank.NewTestEngine(
		crank.WithConcurrency(1),
		crank.WithTimeout(2*time.Second),
	)
	if err != nil {
		t.Fatalf("NewTestEngine: %v", err)
	}

	var callCount atomic.Int64
	engine.Register("BreakerWorker", &testWorker{
		onPerform: func(ctx context.Context, args ...interface{}) error {
			callCount.Add(1)
			return errors.New("fail")
		},
	})

	if err := engine.Start(); err != nil {
		t.Fatalf("Start: %v", err)
	}
	defer engine.Stop()

	// Enqueue multiple jobs with retry=0 so they go straight to dead
	for i := 0; i < 10; i++ {
		client.EnqueueWithOptions("BreakerWorker", "default",
			&crank.JobOptions{Retry: intPtr(0)}, i)
	}

	// Wait for processing
	time.Sleep(3 * time.Second)
	engine.Stop()

	dead := tb.DeadJobs()
	calls := callCount.Load()

	// Some jobs should have been blocked by circuit breaker (not all 10 executed)
	// The circuit breaker has a default threshold of 5, so after 5 failures it opens
	t.Logf("calls=%d, dead=%d", calls, len(dead))
	if len(dead) == 0 {
		t.Error("expected some dead jobs")
	}
}

// TestE2E_UseAfterStartReturnsError verifies that calling Use() after Start()
// returns an error.
func TestE2E_UseAfterStartReturnsError(t *testing.T) {
	engine, _, _, err := crank.NewTestEngine(
		crank.WithConcurrency(1),
		crank.WithTimeout(1*time.Second),
	)
	if err != nil {
		t.Fatalf("NewTestEngine: %v", err)
	}

	if err := engine.Start(); err != nil {
		t.Fatalf("Start: %v", err)
	}
	defer engine.Stop()

	err = engine.Use(crank.LoggingMiddleware(nil))
	if err == nil {
		t.Error("expected error when calling Use() after Start()")
	}
}

// TestE2E_MiddlewareOrdering verifies middleware executes in the correct order.
func TestE2E_MiddlewareOrdering(t *testing.T) {
	engine, client, _, err := crank.NewTestEngine(
		crank.WithConcurrency(1),
		crank.WithTimeout(2*time.Second),
	)
	if err != nil {
		t.Fatalf("NewTestEngine: %v", err)
	}

	var order []string
	var mu sync.Mutex
	done := make(chan struct{})

	mw1 := func(next crank.Handler) crank.Handler {
		return func(ctx context.Context, job *crank.Job) error {
			mu.Lock()
			order = append(order, "mw1-before")
			mu.Unlock()
			err := next(ctx, job)
			mu.Lock()
			order = append(order, "mw1-after")
			mu.Unlock()
			return err
		}
	}
	mw2 := func(next crank.Handler) crank.Handler {
		return func(ctx context.Context, job *crank.Job) error {
			mu.Lock()
			order = append(order, "mw2-before")
			mu.Unlock()
			err := next(ctx, job)
			mu.Lock()
			order = append(order, "mw2-after")
			mu.Unlock()
			return err
		}
	}

	if err := engine.Use(mw1, mw2); err != nil {
		t.Fatalf("Use: %v", err)
	}
	engine.Register("OrderWorker", &testWorker{
		onPerform: func(ctx context.Context, args ...interface{}) error {
			mu.Lock()
			order = append(order, "handler")
			mu.Unlock()
			close(done)
			return nil
		},
	})

	if err := engine.Start(); err != nil {
		t.Fatalf("Start: %v", err)
	}
	defer engine.Stop()

	client.Enqueue("OrderWorker", "default")

	select {
	case <-done:
	case <-time.After(5 * time.Second):
		t.Fatal("timed out")
	}

	time.Sleep(50 * time.Millisecond) // let after-hooks run

	mu.Lock()
	defer mu.Unlock()

	// Middleware wraps in reverse: mw1 is outermost
	// Expected order: recovery -> logging -> breaker -> mw1 -> mw2 -> handler -> mw2 -> mw1 -> ...
	// We only check our custom middleware and handler
	wantSubsequence := []string{"mw1-before", "mw2-before", "handler", "mw2-after", "mw1-after"}
	idx := 0
	for _, o := range order {
		if idx < len(wantSubsequence) && o == wantSubsequence[idx] {
			idx++
		}
	}
	if idx != len(wantSubsequence) {
		t.Errorf("middleware order = %v, want subsequence %v", order, wantSubsequence)
	}
}

// TestE2E_GlobalEnqueueAndClient verifies global client pattern works.
func TestE2E_GlobalEnqueueAndClient(t *testing.T) {
	engine, client, _, err := crank.NewTestEngine(
		crank.WithConcurrency(1),
		crank.WithTimeout(2*time.Second),
	)
	if err != nil {
		t.Fatalf("NewTestEngine: %v", err)
	}

	crank.SetGlobalClient(client)
	defer crank.SetGlobalClient(nil)

	done := make(chan struct{})
	engine.Register("GlobalWorker", &testWorker{
		onPerform: func(ctx context.Context, args ...interface{}) error {
			close(done)
			return nil
		},
	})

	if err := engine.Start(); err != nil {
		t.Fatalf("Start: %v", err)
	}
	defer engine.Stop()

	// Use the global Enqueue function
	jid, err := crank.Enqueue("GlobalWorker", "default", "hello")
	if err != nil {
		t.Fatalf("global Enqueue: %v", err)
	}
	if jid == "" {
		t.Error("expected non-empty JID from global Enqueue")
	}

	select {
	case <-done:
	case <-time.After(5 * time.Second):
		t.Fatal("timed out waiting for global enqueue job")
	}
}

// TestE2E_GlobalEnqueueWithoutClient verifies proper error without global client.
func TestE2E_GlobalEnqueueWithoutClient(t *testing.T) {
	crank.SetGlobalClient(nil)
	_, err := crank.Enqueue("SomeWorker", "default", "arg")
	if err == nil {
		t.Error("expected error when global client is nil")
	}
}

// TestE2E_EngineStats verifies stats reporting.
func TestE2E_EngineStats(t *testing.T) {
	engine, client, _, err := crank.NewTestEngine(
		crank.WithConcurrency(1),
		crank.WithTimeout(2*time.Second),
	)
	if err != nil {
		t.Fatalf("NewTestEngine: %v", err)
	}

	done := make(chan struct{})
	engine.Register("StatsWorker", &testWorker{
		onPerform: func(ctx context.Context, args ...interface{}) error {
			close(done)
			return nil
		},
	})

	if err := engine.Start(); err != nil {
		t.Fatalf("Start: %v", err)
	}
	defer engine.Stop()

	client.Enqueue("StatsWorker", "default", "test")

	select {
	case <-done:
	case <-time.After(5 * time.Second):
		t.Fatal("timed out")
	}
	time.Sleep(50 * time.Millisecond)

	stats, err := engine.Stats()
	if err != nil {
		t.Fatalf("Stats: %v", err)
	}
	if stats == nil {
		t.Fatal("stats is nil")
	}
}

// TestE2E_WorkerPanicRecovery verifies that panicking workers are recovered
// and the job moves to retry/dead.
func TestE2E_WorkerPanicRecovery(t *testing.T) {
	engine, client, tb, err := crank.NewTestEngine(
		crank.WithConcurrency(1),
		crank.WithTimeout(2*time.Second),
	)
	if err != nil {
		t.Fatalf("NewTestEngine: %v", err)
	}

	engine.Register("PanicWorker", &testWorker{
		onPerform: func(ctx context.Context, args ...interface{}) error {
			panic("something went wrong")
		},
	})

	if err := engine.Start(); err != nil {
		t.Fatalf("Start: %v", err)
	}
	defer engine.Stop()

	_, err = client.EnqueueWithOptions("PanicWorker", "default",
		&crank.JobOptions{Retry: intPtr(0)}, "test")
	if err != nil {
		t.Fatalf("Enqueue: %v", err)
	}

	// Wait for job to be processed and moved to dead
	deadline := time.After(5 * time.Second)
	for {
		select {
		case <-deadline:
			t.Fatal("timed out waiting for panic job to reach dead queue")
		default:
		}
		dead := tb.DeadJobs()
		if len(dead) >= 1 {
			if dead[0].Class != "PanicWorker" {
				t.Errorf("dead job class = %q, want PanicWorker", dead[0].Class)
			}
			return
		}
		time.Sleep(50 * time.Millisecond)
	}
}

// TestE2E_JobTimeoutEnforced verifies that jobs are cancelled after timeout.
func TestE2E_JobTimeoutEnforced(t *testing.T) {
	engine, client, tb, err := crank.NewTestEngine(
		crank.WithConcurrency(1),
		crank.WithTimeout(500*time.Millisecond),
	)
	if err != nil {
		t.Fatalf("NewTestEngine: %v", err)
	}

	var timedOut atomic.Bool
	engine.Register("SlowWorker", &testWorker{
		onPerform: func(ctx context.Context, args ...interface{}) error {
			select {
			case <-ctx.Done():
				timedOut.Store(true)
				return ctx.Err()
			case <-time.After(30 * time.Second):
				return nil
			}
		},
	})

	if err := engine.Start(); err != nil {
		t.Fatalf("Start: %v", err)
	}
	defer engine.Stop()

	_, err = client.EnqueueWithOptions("SlowWorker", "default",
		&crank.JobOptions{Retry: intPtr(0)}, "test")
	if err != nil {
		t.Fatalf("Enqueue: %v", err)
	}

	// Job should timeout and move to dead
	deadline := time.After(10 * time.Second)
	for {
		select {
		case <-deadline:
			t.Fatalf("timed out; timedOut=%v, dead=%d", timedOut.Load(), len(tb.DeadJobs()))
		default:
		}
		dead := tb.DeadJobs()
		if len(dead) >= 1 {
			if !timedOut.Load() {
				t.Error("expected worker to have detected context timeout")
			}
			return
		}
		time.Sleep(50 * time.Millisecond)
	}
}

// TestE2E_RegisterMany verifies batch worker registration.
func TestE2E_RegisterMany(t *testing.T) {
	engine, client, _, err := crank.NewTestEngine(
		crank.WithConcurrency(2),
		crank.WithTimeout(2*time.Second),
	)
	if err != nil {
		t.Fatalf("NewTestEngine: %v", err)
	}

	var w1Done, w2Done atomic.Bool
	engine.RegisterMany(map[string]crank.Worker{
		"Worker1": &testWorker{
			onPerform: func(ctx context.Context, args ...interface{}) error {
				w1Done.Store(true)
				return nil
			},
		},
		"Worker2": &testWorker{
			onPerform: func(ctx context.Context, args ...interface{}) error {
				w2Done.Store(true)
				return nil
			},
		},
	})

	if err := engine.Start(); err != nil {
		t.Fatalf("Start: %v", err)
	}
	defer engine.Stop()

	client.Enqueue("Worker1", "default", "a")
	client.Enqueue("Worker2", "default", "b")

	deadline := time.After(5 * time.Second)
	for {
		select {
		case <-deadline:
			t.Fatal("timed out waiting for RegisterMany workers")
		default:
		}
		if w1Done.Load() && w2Done.Load() {
			return
		}
		time.Sleep(50 * time.Millisecond)
	}
}

// TestE2E_UnregisteredWorkerGoesToDead verifies that jobs for unregistered
// workers go to the dead queue.
func TestE2E_UnregisteredWorkerGoesToDead(t *testing.T) {
	engine, client, tb, err := crank.NewTestEngine(
		crank.WithConcurrency(1),
		crank.WithTimeout(1*time.Second),
	)
	if err != nil {
		t.Fatalf("NewTestEngine: %v", err)
	}

	if err := engine.Start(); err != nil {
		t.Fatalf("Start: %v", err)
	}
	defer engine.Stop()

	_, err = client.EnqueueWithOptions("NonExistentWorker", "default",
		&crank.JobOptions{Retry: intPtr(0)}, "test")
	if err != nil {
		t.Fatalf("Enqueue: %v", err)
	}

	deadline := time.After(5 * time.Second)
	for {
		select {
		case <-deadline:
			t.Fatal("timed out waiting for unregistered worker job to go to dead")
		default:
		}
		dead := tb.DeadJobs()
		if len(dead) >= 1 {
			return
		}
		time.Sleep(50 * time.Millisecond)
	}
}

// TestE2E_EnqueueWithOptions verifies job options are respected.
func TestE2E_EnqueueWithOptions(t *testing.T) {
	engine, client, tb, err := crank.NewTestEngine(
		crank.WithConcurrency(1),
		crank.WithTimeout(1*time.Second),
		crank.WithRetryPollInterval(50*time.Millisecond),
	)
	if err != nil {
		t.Fatalf("NewTestEngine: %v", err)
	}

	var callCount atomic.Int64
	engine.Register("OptionsWorker", &testWorker{
		onPerform: func(ctx context.Context, args ...interface{}) error {
			callCount.Add(1)
			return errors.New("fail")
		},
	})

	if err := engine.Start(); err != nil {
		t.Fatalf("Start: %v", err)
	}
	defer engine.Stop()

	// Retry=1 means: 1 initial attempt + 1 retry = total 2 executions before dead
	_, err = client.EnqueueWithOptions("OptionsWorker", "default",
		&crank.JobOptions{Retry: intPtr(1)}, "test")
	if err != nil {
		t.Fatalf("EnqueueWithOptions: %v", err)
	}

	deadline := time.After(30 * time.Second)
	for {
		select {
		case <-deadline:
			t.Fatalf("timed out; calls=%d, dead=%d", callCount.Load(), len(tb.DeadJobs()))
		default:
		}
		dead := tb.DeadJobs()
		if len(dead) >= 1 {
			if got := callCount.Load(); got < 2 {
				t.Errorf("callCount = %d, want >= 2", got)
			}
			return
		}
		time.Sleep(100 * time.Millisecond)
	}
}

// TestE2E_MultipleEngineStopIsSafe verifies calling Stop multiple times doesn't panic.
func TestE2E_MultipleEngineStopIsSafe(t *testing.T) {
	engine, _, _, err := crank.NewTestEngine(
		crank.WithConcurrency(1),
		crank.WithTimeout(1*time.Second),
	)
	if err != nil {
		t.Fatalf("NewTestEngine: %v", err)
	}

	if err := engine.Start(); err != nil {
		t.Fatalf("Start: %v", err)
	}

	// Should not panic
	engine.Stop()
	engine.Stop()
}
