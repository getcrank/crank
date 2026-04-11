package broker

import (
	"fmt"
	"strings"
	"sync"
	"time"

	"github.com/ogwurujohnson/crank/internal/payload"
)

// InMemoryBroker implements Broker using in-memory queues for database-free testing.
// Queues are map[string][]*payload.Job (FIFO per queue). Retry and dead jobs are
// kept in slices so tests can inspect them.
type InMemoryBroker struct {
	mu        sync.Mutex
	queues    map[string][]*payload.Job
	retry     []retryEntry
	dead      []*payload.Job
	processed int64
	done      chan struct{}
}

type retryEntry struct {
	Job     *payload.Job
	RetryAt time.Time
}

// NewInMemoryBroker returns a new in-memory broker. Safe for concurrent use.
func NewInMemoryBroker() *InMemoryBroker {
	return &InMemoryBroker{
		queues: make(map[string][]*payload.Job),
		retry:  nil,
		dead:   nil,
		done:   make(chan struct{}),
	}
}

// Enqueue appends the job to the named queue.
func (m *InMemoryBroker) Enqueue(queue string, job *payload.Job) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	select {
	case <-m.done:
		return fmt.Errorf("broker closed")
	default:
	}
	m.queues[queue] = append(m.queues[queue], job)
	m.processed++
	return nil
}

// Dequeue blocks until a job is available in one of the given queues or the timeout expires.
// It polls periodically (small sleep) to simulate blocking. Returns (nil, "", nil) on timeout.
func (m *InMemoryBroker) Dequeue(queues []string, timeout time.Duration) (*payload.Job, string, error) {
	deadline := time.Now().Add(timeout)
	tick := 5 * time.Millisecond
	if tick > timeout/4 {
		tick = timeout / 4
		if tick < time.Millisecond {
			tick = time.Millisecond
		}
	}

	ticker := time.NewTicker(tick)
	defer ticker.Stop()

	for {
		m.mu.Lock()
		for _, q := range queues {
			if len(m.queues[q]) > 0 {
				job := m.queues[q][0]
				m.queues[q] = m.queues[q][1:]
				m.mu.Unlock()
				return job, q, nil
			}
		}
		m.mu.Unlock()

		if time.Now().After(deadline) {
			return nil, "", nil
		}
		select {
		case <-m.done:
			return nil, "", nil
		case <-ticker.C:
			// poll again
		}
	}
}

// AddToRetry adds the job to the retry set with the given retry time.
func (m *InMemoryBroker) AddToRetry(job *payload.Job, retryAt time.Time) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.retry = append(m.retry, retryEntry{Job: job, RetryAt: retryAt})
	return nil
}

// GetRetryJobs returns jobs whose retry time is in the past, up to limit.
func (m *InMemoryBroker) GetRetryJobs(limit int64) ([]*payload.Job, error) {
	if limit <= 0 {
		return nil, nil
	}
	m.mu.Lock()
	defer m.mu.Unlock()
	now := time.Now()
	var out []*payload.Job
	for i := 0; i < len(m.retry) && int64(len(out)) < limit; i++ {
		if !m.retry[i].RetryAt.After(now) {
			out = append(out, m.retry[i].Job)
		}
	}
	return out, nil
}

// RemoveFromRetry removes the job from the retry set (by JID).
func (m *InMemoryBroker) RemoveFromRetry(job *payload.Job) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	for i, e := range m.retry {
		if e.Job != nil && e.Job.JID == job.JID {
			m.retry = append(m.retry[:i], m.retry[i+1:]...)
			return nil
		}
	}
	return nil
}

// AddToDead appends the job to the dead list.
func (m *InMemoryBroker) AddToDead(job *payload.Job) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.dead = append(m.dead, job)
	return nil
}

// GetDeadJobs returns up to limit jobs from the dead set (newest first if we treat slice as LIFO).
func (m *InMemoryBroker) GetDeadJobs(limit int64) ([]*payload.Job, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	n := int(limit)
	if n < 0 {
		n = 0
	}
	if n > len(m.dead) {
		n = len(m.dead)
	}
	if n == 0 {
		return nil, nil
	}
	out := make([]*payload.Job, n)
	copy(out, m.dead[len(m.dead)-n:])
	return out, nil
}

// GetQueueSize returns the number of jobs in the named queue.
func (m *InMemoryBroker) GetQueueSize(queue string) (int64, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	return int64(len(m.queues[queue])), nil
}

// DeleteKey deletes a queue by key. Restricted to "queue:" prefixed keys.
func (m *InMemoryBroker) DeleteKey(key string) error {
	if !strings.HasPrefix(key, "queue:") {
		return fmt.Errorf("DeleteKey restricted to queue: prefix, got %q", key)
	}
	m.mu.Lock()
	defer m.mu.Unlock()
	delete(m.queues, key[6:])
	return nil
}

// GetStats returns processed count, retry count, dead count, and per-queue sizes.
func (m *InMemoryBroker) GetStats() (map[string]interface{}, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	queues := make(map[string]int64)
	for q, list := range m.queues {
		queues[q] = int64(len(list))
	}
	return map[string]interface{}{
		"processed": m.processed,
		"retry":     int64(len(m.retry)),
		"dead":      int64(len(m.dead)),
		"queues":    queues,
	}, nil
}

// Close closes the broker and unblocks any Dequeue call.
func (m *InMemoryBroker) Close() error {
	m.mu.Lock()
	defer m.mu.Unlock()
	select {
	case <-m.done:
		return nil
	default:
		close(m.done)
	}
	return nil
}

// RetryJobs returns a copy of jobs currently in the retry set (for test inspection).
func (m *InMemoryBroker) RetryJobs() []*payload.Job {
	m.mu.Lock()
	defer m.mu.Unlock()
	out := make([]*payload.Job, len(m.retry))
	for i, e := range m.retry {
		out[i] = e.Job
	}
	return out
}

// DeadJobs returns a copy of jobs in the dead set (for test inspection).
func (m *InMemoryBroker) DeadJobs() []*payload.Job {
	m.mu.Lock()
	defer m.mu.Unlock()
	out := make([]*payload.Job, len(m.dead))
	copy(out, m.dead)
	return out
}
