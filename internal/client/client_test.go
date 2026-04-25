package client

import (
	"fmt"
	"strings"
	"sync"
	"testing"

	"github.com/ogwurujohnson/crank/internal/broker"
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

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

func TestEnqueue_LogsJobEnqueued(t *testing.T) {
	spy := &spyLogger{}
	c := New(broker.NewInMemoryBroker(), spy)

	jid, err := c.Enqueue("EmailWorker", "default", "user@example.com")
	if err != nil {
		t.Fatalf("Enqueue: %v", err)
	}

	entries := spy.findByMsg("job enqueued")
	if len(entries) != 1 {
		t.Fatalf("expected 1 'job enqueued' log, got %d", len(entries))
	}

	e := entries[0]
	if e.level != "info" {
		t.Errorf("level = %q, want info", e.level)
	}
	assertArg(t, e.args, "jid", jid)
	assertArg(t, e.args, "class", "EmailWorker")
	assertArg(t, e.args, "queue", "default")
}

func TestEnqueueWithOptions_LogsJobEnqueued(t *testing.T) {
	spy := &spyLogger{}
	c := New(broker.NewInMemoryBroker(), spy)

	retry := 3
	jid, err := c.EnqueueWithOptions("ReportWorker", "critical", &payload.JobOptions{Retry: &retry}, "arg1")
	if err != nil {
		t.Fatalf("EnqueueWithOptions: %v", err)
	}

	entries := spy.findByMsg("job enqueued")
	if len(entries) != 1 {
		t.Fatalf("expected 1 'job enqueued' log, got %d", len(entries))
	}

	assertArg(t, entries[0].args, "jid", jid)
	assertArg(t, entries[0].args, "class", "ReportWorker")
	assertArg(t, entries[0].args, "queue", "critical")
}

func TestEnqueue_NoLogWhenLoggerNil(t *testing.T) {
	c := New(broker.NewInMemoryBroker(), nil)

	_, err := c.Enqueue("Worker", "default")
	if err != nil {
		t.Fatalf("Enqueue: %v", err)
	}
	// No panic means nil logger is handled correctly
}

func TestEnqueue_NoLogOnError(t *testing.T) {
	spy := &spyLogger{}
	mb := broker.NewInMemoryBroker()
	_ = mb.Close() // closed broker makes Enqueue return an error
	c := New(mb, spy)

	_, err := c.Enqueue("Worker", "default")
	if err == nil {
		t.Fatal("expected enqueue error")
	}

	entries := spy.findByMsg("job enqueued")
	if len(entries) != 0 {
		t.Errorf("expected no 'job enqueued' log on error, got %d", len(entries))
	}
}

// assertArg checks that a key-value pair exists in structured log args.
func assertArg(t *testing.T, args []any, key string, want interface{}) {
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
	t.Errorf("log arg %q not found in args (keys: %s)", key, strings.Join(keys, ", "))
}
