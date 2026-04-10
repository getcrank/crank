package crank_test

import (
	"context"
	"errors"
	"sync"
	"testing"
	"time"

	"github.com/ogwurujohnson/crank"
)

var errAlwaysFail = errors.New("worker always fails")

func TestNewTestEngine_ProcessesJobEndToEnd(t *testing.T) {
	engine, client, tb, err := crank.NewTestEngine(
		crank.WithConcurrency(1),
		crank.WithTimeout(2*time.Second),
		crank.WithQueues(crank.QueueOption{Name: "default", Weight: 1}),
	)
	if err != nil {
		t.Fatalf("NewTestEngine: %v", err)
	}

	var performed sync.WaitGroup
	performed.Add(1)
	done := make(chan struct{})
	worker := &testWorker{
		onPerform: func(ctx context.Context, args ...interface{}) error {
			defer performed.Done()
			close(done)
			return nil
		},
	}

	engine.Register("TestWorker", worker)
	if err := engine.Start(); err != nil {
		t.Fatalf("engine.Start: %v", err)
	}
	defer engine.Stop()

	crank.SetGlobalClient(client)
	jid, err := client.Enqueue("TestWorker", "default", "arg1", 42)
	if err != nil {
		t.Fatalf("Enqueue: %v", err)
	}
	if jid == "" {
		t.Error("expected non-empty JID")
	}

	select {
	case <-done:
		// worker ran
	case <-time.After(5 * time.Second):
		t.Fatal("worker did not process job within 5s")
	}
	performed.Wait()

	engine.Stop()

	// No job should be in retry or dead after success
	retry := tb.RetryJobs()
	dead := tb.DeadJobs()
	if len(retry) != 0 {
		t.Errorf("expected 0 retry jobs, got %d", len(retry))
	}
	if len(dead) != 0 {
		t.Errorf("expected 0 dead jobs, got %d", len(dead))
	}
}

func TestNewTestEngine_FailedJobMovesToRetryThenDead(t *testing.T) {
	engine, client, tb, err := crank.NewTestEngine(
		crank.WithConcurrency(1),
		crank.WithTimeout(500*time.Millisecond),
		crank.WithQueues(crank.QueueOption{Name: "default", Weight: 1}),
	)
	if err != nil {
		t.Fatalf("NewTestEngine: %v", err)
	}

	worker := &testWorker{
		onPerform: func(ctx context.Context, args ...interface{}) error {
			return errAlwaysFail
		},
	}
	engine.Register("FailingWorker", worker)
	if err := engine.Start(); err != nil {
		t.Fatalf("engine.Start: %v", err)
	}
	defer engine.Stop()

	_, err = client.EnqueueWithOptions("FailingWorker", "default", &crank.JobOptions{Retry: intPtr(0)}, "x")
	if err != nil {
		t.Fatalf("EnqueueWithOptions: %v", err)
	}

	// Allow time for job to fail and move to dead (retry=0)
	time.Sleep(2 * time.Second)
	engine.Stop()

	dead := tb.DeadJobs()
	if len(dead) != 1 {
		t.Errorf("expected 1 dead job, got %d", len(dead))
	}
	if len(dead) > 0 && dead[0].Class != "FailingWorker" {
		t.Errorf("dead job class: got %q", dead[0].Class)
	}
}

type testWorker struct {
	onPerform func(ctx context.Context, args ...interface{}) error
}

func (w *testWorker) Perform(ctx context.Context, args ...interface{}) error {
	if w.onPerform != nil {
		return w.onPerform(ctx, args...)
	}
	return nil
}

func intPtr(n int) *int { return &n }
