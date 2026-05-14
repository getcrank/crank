package crank_test

import (
	"context"
	"strings"
	"testing"
	"time"

	"github.com/ogwurujohnson/crank"
)

// assertErrorContains fails the test unless at least one entry in errs
// contains substr (case-sensitive). Returns the matched entry for inspection.
func assertErrorContains(t *testing.T, errs []string, substr string) string {
	t.Helper()
	for _, e := range errs {
		if strings.Contains(e, substr) {
			return e
		}
	}
	t.Errorf("expected an error containing %q in %v", substr, errs)
	return ""
}

// assertLatencyMeasured fails if the recorded ping latency is unset (zero) or
// implausibly large for an in-memory broker. Catches the case where Health
// forgets to record latency, or measures wall-clock work unrelated to the ping.
func assertLatencyMeasured(t *testing.T, d time.Duration) {
	t.Helper()
	if d <= 0 {
		t.Errorf("BrokerLatency = %v, want > 0 (latency not measured)", d)
	}
	if d > 100*time.Millisecond {
		t.Errorf("BrokerLatency = %v, want < 100ms for in-memory broker", d)
	}
}

func TestHealth_BeforeStart_IsDown(t *testing.T) {
	engine, _, _, err := crank.NewTestEngine(
		crank.WithQueues(crank.QueueOption{Name: "default", Weight: 1}),
	)
	if err != nil {
		t.Fatalf("NewTestEngine: %v", err)
	}
	defer engine.Stop()

	h := engine.Health(context.Background())

	if h.Status != crank.HealthDown {
		t.Errorf("Status = %q, want %q", h.Status, crank.HealthDown)
	}
	if h.EngineStarted {
		t.Error("EngineStarted = true, want false")
	}
	if !h.BrokerReachable {
		t.Error("BrokerReachable = false, want true (in-memory broker is open)")
	}
	assertLatencyMeasured(t, h.BrokerLatency)
	assertErrorContains(t, h.Errors, "engine not started")
}

func TestHealth_AfterStart_IsOK(t *testing.T) {
	engine, _, _, err := crank.NewTestEngine(
		crank.WithQueues(crank.QueueOption{Name: "default", Weight: 2}),
	)
	if err != nil {
		t.Fatalf("NewTestEngine: %v", err)
	}
	defer engine.Stop()

	engine.Register("W", &testWorker{
		onPerform: func(_ context.Context, _ ...interface{}) error { return nil },
	})
	if err := engine.Start(); err != nil {
		t.Fatalf("Start: %v", err)
	}

	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	h := engine.Health(ctx)

	if h.Status != crank.HealthOK {
		t.Errorf("Status = %q, want %q (errors: %v)", h.Status, crank.HealthOK, h.Errors)
	}
	if !h.EngineStarted {
		t.Error("EngineStarted = false, want true")
	}
	if !h.BrokerReachable {
		t.Error("BrokerReachable = false, want true")
	}
	if h.WorkersRegistered != 1 {
		t.Errorf("WorkersRegistered = %d, want 1", h.WorkersRegistered)
	}
	if len(h.Queues) != 1 || h.Queues[0] != "default" {
		t.Errorf("Queues = %v, want [default]", h.Queues)
	}
	if h.CheckedAt.IsZero() {
		t.Error("CheckedAt is zero")
	}
	assertLatencyMeasured(t, h.BrokerLatency)
	if len(h.Errors) != 0 {
		t.Errorf("Errors = %v, want empty when status is ok", h.Errors)
	}
}

func TestHealth_BrokerClosed_IsDown(t *testing.T) {
	engine, _, _, err := crank.NewTestEngine(
		crank.WithQueues(crank.QueueOption{Name: "default", Weight: 1}),
	)
	if err != nil {
		t.Fatalf("NewTestEngine: %v", err)
	}
	engine.Register("W", &testWorker{
		onPerform: func(_ context.Context, _ ...interface{}) error { return nil },
	})
	if err := engine.Start(); err != nil {
		t.Fatalf("Start: %v", err)
	}

	engine.Stop() // closes broker

	h := engine.Health(context.Background())
	if h.Status != crank.HealthDown {
		t.Errorf("Status = %q, want %q", h.Status, crank.HealthDown)
	}
	if h.BrokerReachable {
		t.Error("BrokerReachable = true, want false (broker closed)")
	}
	assertLatencyMeasured(t, h.BrokerLatency)
	pingErr := assertErrorContains(t, h.Errors, "broker ping failed")
	if !strings.Contains(pingErr, "broker closed") {
		t.Errorf("broker ping error = %q, want it to surface underlying %q", pingErr, "broker closed")
	}
}

func TestHealth_DedupesRepeatedQueueNames(t *testing.T) {
	engine, _, _, err := crank.NewTestEngine(
		crank.WithQueues(
			crank.QueueOption{Name: "high", Weight: 3},
			crank.QueueOption{Name: "low", Weight: 1},
		),
	)
	if err != nil {
		t.Fatalf("NewTestEngine: %v", err)
	}
	defer engine.Stop()

	h := engine.Health(context.Background())
	if len(h.Queues) != 2 {
		t.Errorf("Queues = %v, want exactly 2 unique names", h.Queues)
	}
}

func TestHealth_RespectsCanceledContext(t *testing.T) {
	engine, _, _, err := crank.NewTestEngine()
	if err != nil {
		t.Fatalf("NewTestEngine: %v", err)
	}
	defer engine.Stop()

	ctx, cancel := context.WithCancel(context.Background())
	cancel()

	h := engine.Health(ctx)
	if h.BrokerReachable {
		t.Error("BrokerReachable = true, want false when context is canceled before ping")
	}
	if h.Status != crank.HealthDown {
		t.Errorf("Status = %q, want %q", h.Status, crank.HealthDown)
	}
	// Latency should still be recorded even though the ping returned immediately
	// with ctx.Err(); it just won't be a meaningful measure of broker speed.
	if h.BrokerLatency < 0 {
		t.Errorf("BrokerLatency = %v, want >= 0", h.BrokerLatency)
	}
	if h.BrokerLatency > 100*time.Millisecond {
		t.Errorf("BrokerLatency = %v, want < 100ms for an immediate ctx-cancel return", h.BrokerLatency)
	}
	pingErr := assertErrorContains(t, h.Errors, "broker ping failed")
	if !strings.Contains(pingErr, context.Canceled.Error()) {
		t.Errorf("broker ping error = %q, want it to surface %q", pingErr, context.Canceled.Error())
	}
}
