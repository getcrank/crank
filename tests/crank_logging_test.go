package crank_test

import (
	"context"
	"errors"
	"fmt"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/ogwurujohnson/crank"
)

// logSpy captures structured log calls.
type logSpy struct {
	mu      sync.Mutex
	entries []spyEntry
}

type spyEntry struct {
	level string
	msg   string
	args  []any
}

func (l *logSpy) Debug(msg string, args ...any) { l.record("debug", msg, args) }
func (l *logSpy) Info(msg string, args ...any)  { l.record("info", msg, args) }
func (l *logSpy) Warn(msg string, args ...any)  { l.record("warn", msg, args) }
func (l *logSpy) Error(msg string, args ...any) { l.record("error", msg, args) }

func (l *logSpy) record(level, msg string, args []any) {
	l.mu.Lock()
	defer l.mu.Unlock()
	l.entries = append(l.entries, spyEntry{level: level, msg: msg, args: args})
}

func (l *logSpy) findByMsg(msg string) []spyEntry {
	l.mu.Lock()
	defer l.mu.Unlock()
	var found []spyEntry
	for _, e := range l.entries {
		if e.msg == msg {
			found = append(found, e)
		}
	}
	return found
}

func (l *logSpy) hasMsg(msg string) bool {
	return len(l.findByMsg(msg)) > 0
}

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

func TestLogging_FullJobLifecycle(t *testing.T) {
	spy := &logSpy{}
	done := make(chan struct{})

	engine, client, _, err := crank.NewTestEngine(
		crank.WithConcurrency(1),
		crank.WithTimeout(2*time.Second),
		crank.WithLogger(spy),
	)
	if err != nil {
		t.Fatalf("NewTestEngine: %v", err)
	}

	engine.Register("LifecycleWorker", &testWorker{
		onPerform: func(ctx context.Context, args ...interface{}) error {
			close(done)
			return nil
		},
	})

	if err := engine.Start(); err != nil {
		t.Fatalf("Start: %v", err)
	}
	defer engine.Stop()

	_, err = client.Enqueue(context.Background(), "LifecycleWorker", "default", "test")
	if err != nil {
		t.Fatalf("Enqueue: %v", err)
	}

	select {
	case <-done:
	case <-time.After(5 * time.Second):
		t.Fatal("timed out waiting for job")
	}

	// Allow logs to flush
	time.Sleep(100 * time.Millisecond)
	engine.Stop()

	// Verify all lifecycle log messages are present
	assertHasLog(t, spy, "job enqueued")
	assertHasLog(t, spy, "job dequeued")
	assertHasLog(t, spy, "job processed")
}

func TestLogging_FailedJobLifecycle(t *testing.T) {
	spy := &logSpy{}

	engine, client, _, err := crank.NewTestEngine(
		crank.WithConcurrency(1),
		crank.WithTimeout(2*time.Second),
		crank.WithLogger(spy),
	)
	if err != nil {
		t.Fatalf("NewTestEngine: %v", err)
	}

	called := make(chan struct{}, 1)
	engine.Register("FailLogger", &testWorker{
		onPerform: func(ctx context.Context, args ...interface{}) error {
			select {
			case called <- struct{}{}:
			default:
			}
			return errors.New("task failed")
		},
	})

	if err := engine.Start(); err != nil {
		t.Fatalf("Start: %v", err)
	}
	defer engine.Stop()

	_, err = client.EnqueueWithOptions(context.Background(), "FailLogger", "default",
		&crank.JobOptions{Retry: intPtr(0)}, "test")
	if err != nil {
		t.Fatalf("Enqueue: %v", err)
	}

	select {
	case <-called:
	case <-time.After(5 * time.Second):
		t.Fatal("timed out")
	}

	time.Sleep(200 * time.Millisecond)
	engine.Stop()

	assertHasLog(t, spy, "job enqueued")
	assertHasLog(t, spy, "job dequeued")
	assertHasLog(t, spy, "job failed")
	assertHasLog(t, spy, "job exceeded max retries, moving to dead queue")
}

func TestLogging_EnqueueIncludesClassAndQueue(t *testing.T) {
	spy := &logSpy{}

	_, client, _, err := crank.NewTestEngine(
		crank.WithConcurrency(1),
		crank.WithTimeout(1*time.Second),
		crank.WithLogger(spy),
	)
	if err != nil {
		t.Fatalf("NewTestEngine: %v", err)
	}

	jid, err := client.Enqueue(context.Background(), "EmailWorker", "critical", "user@test.com")
	if err != nil {
		t.Fatalf("Enqueue: %v", err)
	}

	entries := spy.findByMsg("job enqueued")
	if len(entries) != 1 {
		t.Fatalf("expected 1 'job enqueued' log, got %d", len(entries))
	}

	e := entries[0]
	assertSpyArg(t, e.args, "jid", jid)
	assertSpyArg(t, e.args, "class", "EmailWorker")
	assertSpyArg(t, e.args, "queue", "critical")
}

func TestLogging_NoLogWithoutLogger(t *testing.T) {
	// No WithLogger — default nop logger. Should not panic.
	engine, client, _, err := crank.NewTestEngine(
		crank.WithConcurrency(1),
		crank.WithTimeout(1*time.Second),
	)
	if err != nil {
		t.Fatalf("NewTestEngine: %v", err)
	}

	done := make(chan struct{})
	engine.Register("QuietWorker", &testWorker{
		onPerform: func(ctx context.Context, args ...interface{}) error {
			close(done)
			return nil
		},
	})

	if err := engine.Start(); err != nil {
		t.Fatalf("Start: %v", err)
	}
	defer engine.Stop()

	_, err = client.Enqueue(context.Background(), "QuietWorker", "default")
	if err != nil {
		t.Fatalf("Enqueue: %v", err)
	}

	select {
	case <-done:
	case <-time.After(5 * time.Second):
		t.Fatal("timed out")
	}
	// No panic = success
}

// ---------------------------------------------------------------------------
// Helpers
// ---------------------------------------------------------------------------

func assertHasLog(t *testing.T, spy *logSpy, msg string) {
	t.Helper()
	if !spy.hasMsg(msg) {
		spy.mu.Lock()
		var msgs []string
		for _, e := range spy.entries {
			msgs = append(msgs, fmt.Sprintf("[%s] %s", e.level, e.msg))
		}
		spy.mu.Unlock()
		t.Errorf("missing log %q; available logs:\n  %s", msg, strings.Join(msgs, "\n  "))
	}
}

func assertSpyArg(t *testing.T, args []any, key string, want interface{}) {
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
