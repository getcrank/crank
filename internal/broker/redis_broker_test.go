package broker

import (
	"fmt"
	"strings"
	"testing"
	"time"

	"github.com/alicebob/miniredis/v2"
	"github.com/ogwurujohnson/crank/internal/payload"
)

func TestNewRedisBrokerWithConfig_EmptyURL(t *testing.T) {
	_, err := NewRedisBrokerWithConfig(RedisBrokerConfig{})
	if err == nil {
		t.Fatal("expected error for empty URL")
	}
	if !strings.Contains(err.Error(), "broker not available") || !strings.Contains(err.Error(), "Redis URL is empty") {
		t.Errorf("err = %q, want message about empty Redis URL", err.Error())
	}
}

func TestRedisBroker_EnqueueDequeue(t *testing.T) {
	s := miniredis.RunT(t)
	r, err := NewRedisBroker(fmt.Sprintf("redis://%s/0", s.Addr()), time.Second)
	if err != nil {
		t.Fatalf("NewRedisBroker: %v", err)
	}
	t.Cleanup(func() { _ = r.Close() })

	j := payload.NewJob("W", "default", 1, "two")
	if err := r.Enqueue("default", j); err != nil {
		t.Fatalf("Enqueue: %v", err)
	}

	got, q, err := r.Dequeue([]string{"default"}, time.Second)
	if err != nil {
		t.Fatalf("Dequeue: %v", err)
	}
	if q != "default" {
		t.Errorf("queue = %q, want default", q)
	}
	if got.JID != j.JID {
		t.Errorf("JID = %q, want %q", got.JID, j.JID)
	}
	if got.Class != j.Class {
		t.Errorf("Class = %q, want %q", got.Class, j.Class)
	}
	if got.Queue != j.Queue {
		t.Errorf("Queue = %q, want %q", got.Queue, j.Queue)
	}
}

func TestRedisBroker_RetryLifecycle(t *testing.T) {
	s := miniredis.RunT(t)
	r, err := NewRedisBroker(fmt.Sprintf("redis://%s/0", s.Addr()), time.Second)
	if err != nil {
		t.Fatalf("NewRedisBroker: %v", err)
	}
	t.Cleanup(func() { _ = r.Close() })

	j := payload.NewJob("W", "default", 1)
	if err := r.AddToRetry(j, time.Now().Add(-1*time.Second)); err != nil {
		t.Fatalf("AddToRetry: %v", err)
	}

	jobs, err := r.GetRetryJobs(10)
	if err != nil {
		t.Fatalf("GetRetryJobs: %v", err)
	}
	if len(jobs) != 1 {
		t.Fatalf("len(GetRetryJobs) = %d, want 1", len(jobs))
	}
	if jobs[0].JID != j.JID {
		t.Errorf("JID = %q, want %q", jobs[0].JID, j.JID)
	}

	if err := r.RemoveFromRetry(jobs[0]); err != nil {
		t.Fatalf("RemoveFromRetry: %v", err)
	}
	jobs, err = r.GetRetryJobs(10)
	if err != nil {
		t.Fatalf("GetRetryJobs: %v", err)
	}
	if len(jobs) != 0 {
		t.Errorf("len(GetRetryJobs) after remove = %d, want 0", len(jobs))
	}
}

func TestRedisBroker_DeadLifecycle(t *testing.T) {
	s := miniredis.RunT(t)
	r, err := NewRedisBroker(fmt.Sprintf("redis://%s/0", s.Addr()), time.Second)
	if err != nil {
		t.Fatalf("NewRedisBroker: %v", err)
	}
	t.Cleanup(func() { _ = r.Close() })

	j := payload.NewJob("W", "default", 1)
	if err := r.AddToDead(j); err != nil {
		t.Fatalf("AddToDead: %v", err)
	}

	jobs, err := r.GetDeadJobs(10)
	if err != nil {
		t.Fatalf("GetDeadJobs: %v", err)
	}
	if len(jobs) < 1 {
		t.Fatalf("len(GetDeadJobs) = %d, want at least 1", len(jobs))
	}
	if jobs[0].JID != j.JID {
		t.Errorf("JID = %q, want %q", jobs[0].JID, j.JID)
	}
}

func TestRedisBroker_GetStats(t *testing.T) {
	s := miniredis.RunT(t)
	r, err := NewRedisBroker(fmt.Sprintf("redis://%s/0", s.Addr()), time.Second)
	if err != nil {
		t.Fatalf("NewRedisBroker: %v", err)
	}
	t.Cleanup(func() { _ = r.Close() })

	if err := r.Enqueue("default", payload.NewJob("W", "default", 1)); err != nil {
		t.Fatalf("Enqueue: %v", err)
	}
	successJob := payload.NewJob("W", "default", 2)
	if err := r.RecordSuccess(successJob); err != nil {
		t.Fatalf("RecordSuccess: %v", err)
	}
	failJob := payload.NewJob("W", "default", 3)
	if err := r.RecordFailure(failJob); err != nil {
		t.Fatalf("RecordFailure: %v", err)
	}
	if err := r.AddToRetry(payload.NewJob("W", "default", 4), time.Now().Add(-1*time.Second)); err != nil {
		t.Fatalf("AddToRetry: %v", err)
	}
	if err := r.AddToDead(payload.NewJob("W", "default", 5)); err != nil {
		t.Fatalf("AddToDead: %v", err)
	}

	stats, err := r.GetStats()
	if err != nil {
		t.Fatalf("GetStats: %v", err)
	}

	if stats["processed"] != int64(1) {
		t.Errorf("processed = %v, want 1", stats["processed"])
	}
	if stats["failed"] != int64(1) {
		t.Errorf("failed = %v, want 1", stats["failed"])
	}
	if stats["retry"] != int64(1) {
		t.Errorf("retry = %v, want 1", stats["retry"])
	}
	if stats["dead"] != int64(1) {
		t.Errorf("dead = %v, want 1", stats["dead"])
	}

	qs, ok := stats["queues"].(map[string]int64)
	if !ok {
		t.Fatalf("queues type = %T, want map[string]int64", stats["queues"])
	}
	if qs["default"] != 1 {
		t.Errorf("queues[default] = %d, want 1", qs["default"])
	}
}
