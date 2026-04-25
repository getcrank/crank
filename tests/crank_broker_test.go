package crank_test

import (
	"context"
	"strings"
	"testing"
	"time"

	"github.com/ogwurujohnson/crank"
	"github.com/ogwurujohnson/crank/internal/broker"
)

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
	mb := broker.NewInMemoryBroker()
	engine, client, err := crank.New("",
		crank.WithCustomBroker(mb),
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
	_ = mb.Close()
}

func TestNew_WithCustomBroker_ProcessesJob(t *testing.T) {
	mb := broker.NewInMemoryBroker()
	engine, client, err := crank.New("",
		crank.WithCustomBroker(mb),
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

	jid, err := client.Enqueue(context.Background(),"CustomWorker", "default", "hello")
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
	mb := broker.NewInMemoryBroker()
	// Provide both WithBroker (unsupported kind) AND WithCustomBroker.
	// Custom broker should win; engine should create successfully.
	engine, _, err := crank.New("",
		crank.WithBroker("kafka"), // would fail on its own
		crank.WithCustomBroker(mb),
		crank.WithConcurrency(1),
	)
	if err != nil {
		t.Fatalf("expected custom broker to take precedence, got error: %v", err)
	}
	if engine == nil {
		t.Fatal("expected non-nil engine")
	}
	_ = mb.Close()
}

