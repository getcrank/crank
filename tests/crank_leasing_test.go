package crank_test

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/ogwurujohnson/crank"
)

func TestLeasing_SuccessfulJobIsAcked(t *testing.T) {
	engine, client, tb, err := crank.NewTestEngine(
		crank.WithConcurrency(1),
		crank.WithTimeout(2*time.Second),
	)
	if err != nil {
		t.Fatalf("NewTestEngine: %v", err)
	}

	done := make(chan struct{})
	engine.Register("AckWorker", &testWorker{
		onPerform: func(ctx context.Context, args ...interface{}) error {
			close(done)
			return nil
		},
	})

	if err := engine.Start(); err != nil {
		t.Fatalf("Start: %v", err)
	}
	defer engine.Stop()

	client.Enqueue(context.Background(), "AckWorker", "default", "test")

	select {
	case <-done:
	case <-time.After(5 * time.Second):
		t.Fatal("timed out waiting for job")
	}

	// Allow ack to complete
	time.Sleep(100 * time.Millisecond)

	// Processing set should be empty — job was acked
	processing := tb.ProcessingJobs()
	if len(processing) != 0 {
		t.Errorf("processing count = %d, want 0 after successful job", len(processing))
	}
}

func TestLeasing_FailedJobWithRetryIsAcked(t *testing.T) {
	engine, client, tb, err := crank.NewTestEngine(
		crank.WithConcurrency(1),
		crank.WithTimeout(2*time.Second),
	)
	if err != nil {
		t.Fatalf("NewTestEngine: %v", err)
	}

	called := make(chan struct{}, 1)
	engine.Register("RetryAckWorker", &testWorker{
		onPerform: func(ctx context.Context, args ...interface{}) error {
			select {
			case called <- struct{}{}:
			default:
			}
			return errors.New("fail")
		},
	})

	if err := engine.Start(); err != nil {
		t.Fatalf("Start: %v", err)
	}
	defer engine.Stop()

	client.EnqueueWithOptions(context.Background(), "RetryAckWorker", "default",
		&crank.JobOptions{Retry: intPtr(1)}, "test")

	select {
	case <-called:
	case <-time.After(5 * time.Second):
		t.Fatal("timed out")
	}

	time.Sleep(100 * time.Millisecond)

	// Job should be in retry, not in processing
	processing := tb.ProcessingJobs()
	if len(processing) != 0 {
		t.Errorf("processing count = %d, want 0 (job should be acked after retry schedule)", len(processing))
	}

	retry := tb.RetryJobs()
	if len(retry) == 0 {
		t.Error("expected job in retry set")
	}
}

func TestLeasing_DeadJobIsAcked(t *testing.T) {
	engine, client, tb, err := crank.NewTestEngine(
		crank.WithConcurrency(1),
		crank.WithTimeout(2*time.Second),
	)
	if err != nil {
		t.Fatalf("NewTestEngine: %v", err)
	}

	called := make(chan struct{}, 1)
	engine.Register("DeadAckWorker", &testWorker{
		onPerform: func(ctx context.Context, args ...interface{}) error {
			select {
			case called <- struct{}{}:
			default:
			}
			return errors.New("fail")
		},
	})

	if err := engine.Start(); err != nil {
		t.Fatalf("Start: %v", err)
	}
	defer engine.Stop()

	client.EnqueueWithOptions(context.Background(), "DeadAckWorker", "default",
		&crank.JobOptions{Retry: intPtr(0)}, "test")

	select {
	case <-called:
	case <-time.After(5 * time.Second):
		t.Fatal("timed out")
	}

	time.Sleep(200 * time.Millisecond)

	// Job should be in dead, not in processing
	processing := tb.ProcessingJobs()
	if len(processing) != 0 {
		t.Errorf("processing count = %d, want 0 (job should be acked after dead-letter)", len(processing))
	}

	dead := tb.DeadJobs()
	if len(dead) == 0 {
		t.Error("expected job in dead set")
	}
}
