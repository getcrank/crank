package crank_test

import (
	"context"
	"fmt"
	"os"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/ogwurujohnson/crank"
)

// stubBroker is a minimal crank.Broker for testing WithCustomBroker.
// It mirrors InMemoryBroker behaviour just enough to run the engine.
type stubBroker struct {
	mu        sync.Mutex
	queues    map[string][]*crank.Job
	retry     []retryItem
	dead      []*crank.Job
	processed int64
	done      chan struct{}
}

type retryItem struct {
	job     *crank.Job
	retryAt time.Time
}

func newStubBroker() *stubBroker {
	return &stubBroker{
		queues: make(map[string][]*crank.Job),
		done:   make(chan struct{}),
	}
}

func (s *stubBroker) Enqueue(queue string, job *crank.Job) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	select {
	case <-s.done:
		return fmt.Errorf("broker closed")
	default:
	}
	s.queues[queue] = append(s.queues[queue], job)
	s.processed++
	return nil
}

func (s *stubBroker) Dequeue(queues []string, timeout time.Duration) (*crank.Job, string, error) {
	deadline := time.Now().Add(timeout)
	tick := 5 * time.Millisecond
	ticker := time.NewTicker(tick)
	defer ticker.Stop()
	for {
		s.mu.Lock()
		for _, q := range queues {
			if len(s.queues[q]) > 0 {
				job := s.queues[q][0]
				s.queues[q] = s.queues[q][1:]
				s.mu.Unlock()
				return job, q, nil
			}
		}
		s.mu.Unlock()
		if time.Now().After(deadline) {
			return nil, "", nil
		}
		select {
		case <-s.done:
			return nil, "", nil
		case <-ticker.C:
		}
	}
}

func (s *stubBroker) AddToRetry(job *crank.Job, retryAt time.Time) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.retry = append(s.retry, retryItem{job: job, retryAt: retryAt})
	return nil
}

func (s *stubBroker) GetRetryJobs(limit int64) ([]*crank.Job, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	now := time.Now()
	var out []*crank.Job
	for i := 0; i < len(s.retry) && int64(len(out)) < limit; i++ {
		if !s.retry[i].retryAt.After(now) {
			out = append(out, s.retry[i].job)
		}
	}
	return out, nil
}

func (s *stubBroker) RemoveFromRetry(job *crank.Job) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	for i, e := range s.retry {
		if e.job != nil && e.job.JID == job.JID {
			s.retry = append(s.retry[:i], s.retry[i+1:]...)
			return nil
		}
	}
	return nil
}

func (s *stubBroker) AddToDead(job *crank.Job) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.dead = append(s.dead, job)
	return nil
}

func (s *stubBroker) GetDeadJobs(limit int64) ([]*crank.Job, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	n := int(limit)
	if n > len(s.dead) {
		n = len(s.dead)
	}
	if n == 0 {
		return nil, nil
	}
	out := make([]*crank.Job, n)
	copy(out, s.dead[len(s.dead)-n:])
	return out, nil
}

func (s *stubBroker) GetQueueSize(queue string) (int64, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	return int64(len(s.queues[queue])), nil
}

func (s *stubBroker) Ack(job *crank.Job) error                                   { return nil }
func (s *stubBroker) Nack(job *crank.Job) error                                  { return nil }
func (s *stubBroker) ReapOrphanedJobs(lease time.Duration) ([]*crank.Job, error) { return nil, nil }
func (s *stubBroker) DeleteKey(key string) error                                 { return nil }

func (s *stubBroker) GetStats() (map[string]interface{}, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	return map[string]interface{}{
		"processed": s.processed,
		"retry":     int64(len(s.retry)),
		"dead":      int64(len(s.dead)),
		"queues":    map[string]int64{},
	}, nil
}

func (s *stubBroker) Close() error {
	s.mu.Lock()
	defer s.mu.Unlock()
	select {
	case <-s.done:
	default:
		close(s.done)
	}
	return nil
}

// ---------------------------------------------------------------------------
// Tests: New() enforces broker provisioning
// ---------------------------------------------------------------------------

func TestNew_NoBroker_ReturnsError(t *testing.T) {
	_, _, err := crank.New("redis://localhost:6379/0")
	if err == nil {
		t.Fatal("expected error when no broker is configured")
	}
	if !strings.Contains(err.Error(), "no broker configured") {
		t.Errorf("error = %q, want mention of 'no broker configured'", err)
	}
}

func TestNew_WithBrokerNats_ReturnsNotImplemented(t *testing.T) {
	_, _, err := crank.New("nats://localhost:4222", crank.WithBroker("nats"))
	if err == nil {
		t.Fatal("expected error for unimplemented nats broker")
	}
	if !strings.Contains(err.Error(), "not yet implemented") {
		t.Errorf("error = %q, want mention of 'not yet implemented'", err)
	}
}

func TestNew_WithBrokerPgsql_ReturnsNotImplemented(t *testing.T) {
	_, _, err := crank.New("postgres://localhost:5432/crank", crank.WithBroker("pgsql"))
	if err == nil {
		t.Fatal("expected error for unimplemented pgsql broker")
	}
	if !strings.Contains(err.Error(), "not yet implemented") {
		t.Errorf("error = %q, want mention of 'not yet implemented'", err)
	}
}

func TestNew_WithBrokerUnknown_ReturnsError(t *testing.T) {
	_, _, err := crank.New("foo://bar", crank.WithBroker("kafka"))
	if err == nil {
		t.Fatal("expected error for unsupported broker")
	}
	if !strings.Contains(err.Error(), "unsupported broker") {
		t.Errorf("error = %q, want mention of 'unsupported broker'", err)
	}
}

// ---------------------------------------------------------------------------
// Tests: WithCustomBroker
// ---------------------------------------------------------------------------

func TestNew_WithCustomBroker_Succeeds(t *testing.T) {
	sb := newStubBroker()
	engine, client, err := crank.New("",
		crank.WithCustomBroker(sb),
		crank.WithConcurrency(1),
		crank.WithTimeout(2*time.Second),
	)
	if err != nil {
		t.Fatalf("New with custom broker: %v", err)
	}
	if engine == nil {
		t.Fatal("expected non-nil engine")
	}
	if client == nil {
		t.Fatal("expected non-nil client")
	}
	_ = sb.Close()
}

func TestNew_WithCustomBroker_ProcessesJob(t *testing.T) {
	sb := newStubBroker()
	engine, client, err := crank.New("",
		crank.WithCustomBroker(sb),
		crank.WithConcurrency(1),
		crank.WithTimeout(2*time.Second),
	)
	if err != nil {
		t.Fatalf("New: %v", err)
	}

	done := make(chan struct{})
	engine.Register("CustomWorker", &testWorker{
		onPerform: func(ctx context.Context, args ...interface{}) error {
			close(done)
			return nil
		},
	})

	if err := engine.Start(); err != nil {
		t.Fatalf("Start: %v", err)
	}
	defer engine.Stop()

	jid, err := client.Enqueue("CustomWorker", "default", "hello")
	if err != nil {
		t.Fatalf("Enqueue: %v", err)
	}
	if jid == "" {
		t.Error("expected non-empty JID")
	}

	select {
	case <-done:
		// worker executed via custom broker
	case <-time.After(5 * time.Second):
		t.Fatal("timed out waiting for custom broker job")
	}
}

func TestNew_WithCustomBroker_TakesPrecedenceOverWithBroker(t *testing.T) {
	sb := newStubBroker()
	// Provide both WithBroker (unsupported kind) AND WithCustomBroker.
	// Custom broker should win; engine should create successfully.
	engine, _, err := crank.New("",
		crank.WithBroker("kafka"), // would fail on its own
		crank.WithCustomBroker(sb),
		crank.WithConcurrency(1),
	)
	if err != nil {
		t.Fatalf("expected custom broker to take precedence, got error: %v", err)
	}
	if engine == nil {
		t.Fatal("expected non-nil engine")
	}
	_ = sb.Close()
}

// ---------------------------------------------------------------------------
// Tests: QuickStart enforces broker in config
// ---------------------------------------------------------------------------

func TestQuickStart_MissingBrokerField_ReturnsError(t *testing.T) {
	// Write a temp config with no broker field
	cfg := writeTestConfig(t, `
concurrency: 5
timeout: 8
`)
	_, _, err := crank.QuickStart(cfg)
	if err == nil {
		t.Fatal("expected error when broker field is missing in config")
	}
	if !strings.Contains(err.Error(), "\"broker\" field is required") {
		t.Errorf("error = %q, want mention of broker field required", err)
	}
}

func TestQuickStart_BrokerPgsql_ReturnsNotImplemented(t *testing.T) {
	cfg := writeTestConfig(t, `
broker: pgsql
concurrency: 5
`)
	_, _, err := crank.QuickStart(cfg)
	if err == nil {
		t.Fatal("expected error for pgsql broker in config")
	}
	if !strings.Contains(err.Error(), "not yet implemented") {
		t.Errorf("error = %q, want 'not yet implemented'", err)
	}
}

func TestQuickStart_UnknownBroker_ReturnsError(t *testing.T) {
	cfg := writeTestConfig(t, `
broker: kafka
concurrency: 5
`)
	_, _, err := crank.QuickStart(cfg)
	if err == nil {
		t.Fatal("expected error for unknown broker")
	}
	if !strings.Contains(err.Error(), "unknown broker") {
		t.Errorf("error = %q, want 'unknown broker'", err)
	}
}

func TestQuickStart_RedisNoURL_ReturnsError(t *testing.T) {
	// Unset REDIS_URL to ensure no fallback
	t.Setenv("REDIS_URL", "")

	cfg := writeTestConfig(t, `
broker: redis
`)
	_, _, err := crank.QuickStart(cfg)
	if err == nil {
		t.Fatal("expected error when redis URL is empty")
	}
	if !strings.Contains(err.Error(), "redis.url") {
		t.Errorf("error = %q, want mention of redis.url", err)
	}
}

// writeTestConfig writes YAML content to a temp file and returns its path.
func writeTestConfig(t *testing.T, content string) string {
	t.Helper()
	dir := t.TempDir()
	path := dir + "/crank_test.yml"
	if err := writeFile(path, content); err != nil {
		t.Fatalf("write test config: %v", err)
	}
	return path
}

func writeFile(path, content string) error {
	return os.WriteFile(path, []byte(content), 0644)
}
