package broker

import (
	"testing"
	"time"

	"github.com/ogwurujohnson/crank/internal/payload"
)

func TestInMemoryBroker_DequeueMovesToProcessing(t *testing.T) {
	b := NewInMemoryBroker()
	defer b.Close()

	job := payload.NewJob("W", "default", "arg1")
	if err := b.Enqueue("default", job); err != nil {
		t.Fatalf("Enqueue: %v", err)
	}

	got, q, err := b.Dequeue([]string{"default"}, time.Second)
	if err != nil {
		t.Fatalf("Dequeue: %v", err)
	}
	if got.JID != job.JID {
		t.Errorf("JID = %q, want %q", got.JID, job.JID)
	}
	if q != "default" {
		t.Errorf("queue = %q, want default", q)
	}

	// Job should be in the processing set, not in the queue
	size, _ := b.GetQueueSize("default")
	if size != 0 {
		t.Errorf("queue size = %d, want 0 (job should be in processing)", size)
	}

	processing := b.ProcessingJobs()
	if len(processing) != 1 {
		t.Fatalf("processing count = %d, want 1", len(processing))
	}
	if processing[0].JID != job.JID {
		t.Errorf("processing JID = %q, want %q", processing[0].JID, job.JID)
	}
}

func TestInMemoryBroker_AckRemovesFromProcessing(t *testing.T) {
	b := NewInMemoryBroker()
	defer b.Close()

	job := payload.NewJob("W", "default")
	b.Enqueue("default", job)
	got, _, _ := b.Dequeue([]string{"default"}, time.Second)

	if err := b.Ack(got); err != nil {
		t.Fatalf("Ack: %v", err)
	}

	processing := b.ProcessingJobs()
	if len(processing) != 0 {
		t.Errorf("processing count = %d, want 0 after Ack", len(processing))
	}
}

func TestInMemoryBroker_AckIsIdempotent(t *testing.T) {
	b := NewInMemoryBroker()
	defer b.Close()

	job := payload.NewJob("W", "default")
	b.Enqueue("default", job)
	got, _, _ := b.Dequeue([]string{"default"}, time.Second)

	if err := b.Ack(got); err != nil {
		t.Fatalf("first Ack: %v", err)
	}
	if err := b.Ack(got); err != nil {
		t.Fatalf("second Ack: %v", err)
	}
}

func TestInMemoryBroker_NackReenqueues(t *testing.T) {
	b := NewInMemoryBroker()
	defer b.Close()

	job := payload.NewJob("W", "default")
	b.Enqueue("default", job)
	got, _, _ := b.Dequeue([]string{"default"}, time.Second)

	if err := b.Nack(got); err != nil {
		t.Fatalf("Nack: %v", err)
	}

	// Should be back in the queue
	size, _ := b.GetQueueSize("default")
	if size != 1 {
		t.Errorf("queue size = %d, want 1 after Nack", size)
	}

	// Should not be in processing
	processing := b.ProcessingJobs()
	if len(processing) != 0 {
		t.Errorf("processing count = %d, want 0 after Nack", len(processing))
	}

	// Should be re-dequeue-able
	got2, _, err := b.Dequeue([]string{"default"}, time.Second)
	if err != nil {
		t.Fatalf("re-Dequeue: %v", err)
	}
	if got2.JID != job.JID {
		t.Errorf("re-dequeued JID = %q, want %q", got2.JID, job.JID)
	}
}

func TestInMemoryBroker_NackIsIdempotent(t *testing.T) {
	b := NewInMemoryBroker()
	defer b.Close()

	job := payload.NewJob("W", "default")
	b.Enqueue("default", job)
	got, _, _ := b.Dequeue([]string{"default"}, time.Second)

	b.Nack(got)
	b.Nack(got) // second call should be no-op

	size, _ := b.GetQueueSize("default")
	if size != 1 {
		t.Errorf("queue size = %d, want 1 (not double-enqueued)", size)
	}
}

func TestInMemoryBroker_ReapOrphanedJobs(t *testing.T) {
	b := NewInMemoryBroker()
	defer b.Close()

	job := payload.NewJob("W", "default")
	b.Enqueue("default", job)
	b.Dequeue([]string{"default"}, time.Second)

	// With a very large lease, nothing should be reaped
	orphaned, err := b.ReapOrphanedJobs(1 * time.Hour)
	if err != nil {
		t.Fatalf("ReapOrphanedJobs: %v", err)
	}
	if len(orphaned) != 0 {
		t.Errorf("expected 0 orphaned with large lease, got %d", len(orphaned))
	}

	// Manually expire the lease by manipulating the processing entry
	b.mu.Lock()
	if len(b.processing) > 0 {
		b.processing[0].LeaseExp = time.Now().Add(-10 * time.Minute)
	}
	b.mu.Unlock()

	// Now reap with zero lease — the expired entry should be returned
	orphaned, err = b.ReapOrphanedJobs(0)
	if err != nil {
		t.Fatalf("ReapOrphanedJobs: %v", err)
	}
	if len(orphaned) != 1 {
		t.Fatalf("expected 1 orphaned, got %d", len(orphaned))
	}
	if orphaned[0].JID != job.JID {
		t.Errorf("orphaned JID = %q, want %q", orphaned[0].JID, job.JID)
	}
	if orphaned[0].State != payload.JobStatePending {
		t.Errorf("orphaned State = %q, want %q", orphaned[0].State, payload.JobStatePending)
	}

	// Processing set should be empty now
	processing := b.ProcessingJobs()
	if len(processing) != 0 {
		t.Errorf("processing count = %d, want 0 after reap", len(processing))
	}
}

func TestInMemoryBroker_StatsIncludesProcessing(t *testing.T) {
	b := NewInMemoryBroker()
	defer b.Close()

	job := payload.NewJob("W", "default")
	b.Enqueue("default", job)
	b.Dequeue([]string{"default"}, time.Second)

	stats, err := b.GetStats()
	if err != nil {
		t.Fatalf("GetStats: %v", err)
	}
	processing, ok := stats["processing"].(int64)
	if !ok {
		t.Fatalf("stats[processing] type = %T, want int64", stats["processing"])
	}
	if processing != 1 {
		t.Errorf("stats[processing] = %d, want 1", processing)
	}
}
