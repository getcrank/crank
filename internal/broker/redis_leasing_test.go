package broker

import (
	"context"
	"fmt"
	"testing"
	"time"

	"github.com/alicebob/miniredis/v2"
	"github.com/ogwurujohnson/crank/internal/payload"
)

func TestRedisBroker_DequeueAndAck(t *testing.T) {
	s := miniredis.RunT(t)
	r, err := NewRedisBroker(fmt.Sprintf("redis://%s/0", s.Addr()), time.Second)
	if err != nil {
		t.Fatalf("NewRedisBroker: %v", err)
	}
	t.Cleanup(func() { _ = r.Close() })

	job := payload.NewJob("W", "default", "arg1")
	if err := r.Enqueue(context.Background(), "default", job); err != nil {
		t.Fatalf("Enqueue: %v", err)
	}

	got, q, err := r.Dequeue([]string{"default"}, time.Second)
	if err != nil {
		t.Fatalf("Dequeue: %v", err)
	}
	if got.JID != job.JID {
		t.Errorf("JID = %q, want %q", got.JID, job.JID)
	}
	if q != "default" {
		t.Errorf("queue = %q, want default", q)
	}

	// Job should be in processing sorted set
	members, err := s.ZMembers(redisProcessingKey)
	if err != nil {
		t.Fatalf("ZMembers: %v", err)
	}
	if len(members) != 1 {
		t.Fatalf("processing set size = %d, want 1", len(members))
	}

	// Ack should remove from processing
	if err := r.Ack(got); err != nil {
		t.Fatalf("Ack: %v", err)
	}

	members, _ = s.ZMembers(redisProcessingKey)
	if len(members) != 0 {
		t.Errorf("processing set size after Ack = %d, want 0", len(members))
	}
}

func TestRedisBroker_ReapOrphanedJobs(t *testing.T) {
	s := miniredis.RunT(t)
	r, err := NewRedisBroker(fmt.Sprintf("redis://%s/0", s.Addr()), time.Second)
	if err != nil {
		t.Fatalf("NewRedisBroker: %v", err)
	}
	t.Cleanup(func() { _ = r.Close() })

	job := payload.NewJob("W", "default", "arg1")
	r.Enqueue(context.Background(), "default", job)
	r.Dequeue([]string{"default"}, time.Second)

	// With a zero lease, cutoff=now which is before the lease expiry — nothing reaped
	orphaned, err := r.ReapOrphanedJobs(0)
	if err != nil {
		t.Fatalf("ReapOrphanedJobs: %v", err)
	}
	if len(orphaned) != 0 {
		t.Errorf("expected 0 orphaned (lease not expired), got %d", len(orphaned))
	}

	// Overwrite the processing entry score to simulate an expired lease
	data, _ := job.ToJSON()
	if _, err := s.ZAdd(redisProcessingKey, float64(time.Now().Add(-10*time.Minute).Unix()), string(data)); err != nil {
		t.Fatalf("manual ZAdd: %v", err)
	}

	// Now reap with zero lease — the expired entry should be returned
	orphaned, err = r.ReapOrphanedJobs(0)
	if err != nil {
		t.Fatalf("ReapOrphanedJobs: %v", err)
	}
	if len(orphaned) != 1 {
		t.Fatalf("expected 1 orphaned, got %d", len(orphaned))
	}
	if orphaned[0].JID != job.JID {
		t.Errorf("orphaned JID = %q, want %q", orphaned[0].JID, job.JID)
	}

	// Processing set should be empty
	members, _ := s.ZMembers(redisProcessingKey)
	if len(members) != 0 {
		t.Errorf("processing set after reap = %d, want 0", len(members))
	}
}
