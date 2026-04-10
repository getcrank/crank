package queue

import (
	"context"
	"errors"
	"fmt"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/ogwurujohnson/crank/internal/broker"
	"github.com/ogwurujohnson/crank/internal/config"
	"github.com/ogwurujohnson/crank/internal/payload"
)

// spyLogger captures log calls for assertions.
type spyLogger struct {
	mu      sync.Mutex
	entries []logEntry
}

type logEntry struct {
	level string
	msg   string
	args  []any
}

func (l *spyLogger) Debug(msg string, args ...any) { l.record("debug", msg, args) }
func (l *spyLogger) Info(msg string, args ...any)  { l.record("info", msg, args) }
func (l *spyLogger) Warn(msg string, args ...any)  { l.record("warn", msg, args) }
func (l *spyLogger) Error(msg string, args ...any) { l.record("error", msg, args) }

func (l *spyLogger) record(level, msg string, args []any) {
	l.mu.Lock()
	defer l.mu.Unlock()
	l.entries = append(l.entries, logEntry{level: level, msg: msg, args: args})
}

func (l *spyLogger) findByMsg(msg string) []logEntry {
	l.mu.Lock()
	defer l.mu.Unlock()
	var found []logEntry
	for _, e := range l.entries {
		if e.msg == msg {
			found = append(found, e)
		}
	}
	return found
}

// testRegistry is a simple WorkerRegistry for processor tests.
type testRegistry struct {
	workers map[string]Worker
}

func (r *testRegistry) GetWorker(name string) (Worker, error) {
	w, ok := r.workers[name]
	if !ok {
		return nil, fmt.Errorf("worker %q not found", name)
	}
	return w, nil
}

// fnWorker wraps a function as a Worker.
type fnWorker struct {
	fn func(ctx context.Context, args ...interface{}) error
}

func (w *fnWorker) Perform(ctx context.Context, args ...interface{}) error {
	return w.fn(ctx, args...)
}

func newTestProcessor(t *testing.T, spy *spyLogger, registry *testRegistry) *Processor {
	t.Helper()
	b := broker.NewInMemoryBroker()
	t.Cleanup(func() { _ = b.Close() })

	cfg := &config.Config{
		Concurrency:       1,
		Timeout:           5,
		Queues:            []config.QueueConfig{{Name: "default", Weight: 1}},
		Logger:            spy,
		RetryPollInterval: 50 * time.Millisecond,
	}
	chain := NewChain(RecoveryMiddleware(spy), LoggingMiddleware(spy))
	p, err := NewProcessor(cfg, b, registry, chain)
	if err != nil {
		t.Fatalf("NewProcessor: %v", err)
	}
	return p
}

// ---------------------------------------------------------------------------
// Tests: dequeue logging
// ---------------------------------------------------------------------------

func TestProcessor_LogsJobDequeued(t *testing.T) {
	spy := &spyLogger{}
	done := make(chan struct{})
	registry := &testRegistry{
		workers: map[string]Worker{
			"TestWorker": &fnWorker{fn: func(ctx context.Context, args ...interface{}) error {
				close(done)
				return nil
			}},
		},
	}

	p := newTestProcessor(t, spy, registry)

	job := payload.NewJob("TestWorker", "default", "arg1")
	if err := p.broker.Enqueue("default", job); err != nil {
		t.Fatalf("Enqueue: %v", err)
	}

	if err := p.Start(); err != nil {
		t.Fatalf("Start: %v", err)
	}
	defer p.Stop()

	select {
	case <-done:
	case <-time.After(5 * time.Second):
		t.Fatal("timed out waiting for job")
	}

	// Allow processor logs to flush
	time.Sleep(50 * time.Millisecond)

	entries := spy.findByMsg("job dequeued")
	if len(entries) == 0 {
		t.Fatal("expected 'job dequeued' log entry")
	}

	e := entries[0]
	if e.level != "info" {
		t.Errorf("level = %q, want info", e.level)
	}
	assertLogArg(t, e.args, "jid", job.JID)
	assertLogArg(t, e.args, "class", "TestWorker")
	assertLogArg(t, e.args, "queue", "default")
}

// ---------------------------------------------------------------------------
// Tests: processed logging
// ---------------------------------------------------------------------------

func TestProcessor_LogsJobProcessed(t *testing.T) {
	spy := &spyLogger{}
	done := make(chan struct{})
	registry := &testRegistry{
		workers: map[string]Worker{
			"OkWorker": &fnWorker{fn: func(ctx context.Context, args ...interface{}) error {
				close(done)
				return nil
			}},
		},
	}

	p := newTestProcessor(t, spy, registry)

	job := payload.NewJob("OkWorker", "default")
	if err := p.broker.Enqueue("default", job); err != nil {
		t.Fatalf("Enqueue: %v", err)
	}

	if err := p.Start(); err != nil {
		t.Fatalf("Start: %v", err)
	}
	defer p.Stop()

	select {
	case <-done:
	case <-time.After(5 * time.Second):
		t.Fatal("timed out")
	}

	time.Sleep(50 * time.Millisecond)

	entries := spy.findByMsg("job processed")
	if len(entries) == 0 {
		t.Fatal("expected 'job processed' log entry")
	}

	e := entries[0]
	if e.level != "info" {
		t.Errorf("level = %q, want info", e.level)
	}
	assertLogArg(t, e.args, "jid", job.JID)
	assertLogArg(t, e.args, "class", "OkWorker")
	assertLogArg(t, e.args, "queue", "default")
	assertArgPresent(t, e.args, "dur")
}

// ---------------------------------------------------------------------------
// Tests: failed logging
// ---------------------------------------------------------------------------

func TestProcessor_LogsJobFailed(t *testing.T) {
	spy := &spyLogger{}
	called := make(chan struct{})
	registry := &testRegistry{
		workers: map[string]Worker{
			"FailWorker": &fnWorker{fn: func(ctx context.Context, args ...interface{}) error {
				close(called)
				return errors.New("boom")
			}},
		},
	}

	p := newTestProcessor(t, spy, registry)

	job := payload.NewJob("FailWorker", "default")
	job.SetRetry(0) // go straight to dead
	if err := p.broker.Enqueue("default", job); err != nil {
		t.Fatalf("Enqueue: %v", err)
	}

	if err := p.Start(); err != nil {
		t.Fatalf("Start: %v", err)
	}
	defer p.Stop()

	select {
	case <-called:
	case <-time.After(5 * time.Second):
		t.Fatal("timed out")
	}

	time.Sleep(50 * time.Millisecond)

	// There are two "job failed" entries: one from LoggingMiddleware (jid, args, err)
	// and one from processJob (jid, class, queue, err, dur). Find the one with "class".
	entries := spy.findByMsg("job failed")
	var e *logEntry
	for i := range entries {
		if hasArgKey(entries[i].args, "class") {
			e = &entries[i]
			break
		}
	}
	if e == nil {
		t.Fatal("expected 'job failed' log entry with class field from processJob")
	}

	if e.level != "error" {
		t.Errorf("level = %q, want error", e.level)
	}
	assertLogArg(t, e.args, "jid", job.JID)
	assertLogArg(t, e.args, "class", "FailWorker")
	assertLogArg(t, e.args, "queue", "default")
	assertArgPresent(t, e.args, "err")
	assertArgPresent(t, e.args, "dur")
}

// ---------------------------------------------------------------------------
// Tests: dead queue logging
// ---------------------------------------------------------------------------

func TestProcessor_LogsJobMovedToDead(t *testing.T) {
	spy := &spyLogger{}
	called := make(chan struct{}, 1)
	registry := &testRegistry{
		workers: map[string]Worker{
			"DeadWorker": &fnWorker{fn: func(ctx context.Context, args ...interface{}) error {
				select {
				case called <- struct{}{}:
				default:
				}
				return errors.New("always fails")
			}},
		},
	}

	p := newTestProcessor(t, spy, registry)

	job := payload.NewJob("DeadWorker", "default")
	job.SetRetry(0) // 0 retries → goes to dead after first failure
	if err := p.broker.Enqueue("default", job); err != nil {
		t.Fatalf("Enqueue: %v", err)
	}

	if err := p.Start(); err != nil {
		t.Fatalf("Start: %v", err)
	}
	defer p.Stop()

	select {
	case <-called:
	case <-time.After(5 * time.Second):
		t.Fatal("timed out")
	}

	time.Sleep(100 * time.Millisecond)

	entries := spy.findByMsg("job moved to dead queue")
	if len(entries) == 0 {
		t.Fatal("expected 'job moved to dead queue' log entry")
	}

	e := entries[0]
	if e.level != "warn" {
		t.Errorf("level = %q, want warn", e.level)
	}
	assertLogArg(t, e.args, "jid", job.JID)
	assertLogArg(t, e.args, "class", "DeadWorker")
	assertLogArg(t, e.args, "queue", "default")
	assertArgPresent(t, e.args, "retries")
}

// ---------------------------------------------------------------------------
// Helpers
// ---------------------------------------------------------------------------

func assertLogArg(t *testing.T, args []any, key string, want interface{}) {
	t.Helper()
	for i := 0; i < len(args)-1; i += 2 {
		if fmt.Sprint(args[i]) == key {
			got := fmt.Sprint(args[i+1])
			wantStr := fmt.Sprint(want)
			if got != wantStr {
				t.Errorf("log arg %q = %q, want %q", key, got, wantStr)
			}
			return
		}
	}
	var keys []string
	for i := 0; i < len(args)-1; i += 2 {
		keys = append(keys, fmt.Sprint(args[i]))
	}
	t.Errorf("log arg %q not found (keys: %s)", key, strings.Join(keys, ", "))
}

func hasArgKey(args []any, key string) bool {
	for i := 0; i < len(args)-1; i += 2 {
		if fmt.Sprint(args[i]) == key {
			return true
		}
	}
	return false
}

func assertArgPresent(t *testing.T, args []any, key string) {
	t.Helper()
	for i := 0; i < len(args)-1; i += 2 {
		if fmt.Sprint(args[i]) == key {
			return
		}
	}
	var keys []string
	for i := 0; i < len(args)-1; i += 2 {
		keys = append(keys, fmt.Sprint(args[i]))
	}
	t.Errorf("log arg %q not found (keys: %s)", key, strings.Join(keys, ", "))
}
