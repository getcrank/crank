package crank

import (
	"github.com/ogwurujohnson/crank/internal/broker"
	"github.com/ogwurujohnson/crank/internal/client"
)

// TestBroker is returned by NewTestEngine so tests can inspect retry and dead job state
// without a real broker. It wraps the in-memory broker used by the test engine.
type TestBroker struct {
	b *broker.InMemoryBroker
}

// RetryJobs returns a copy of jobs currently in the retry set.
func (t *TestBroker) RetryJobs() []*Job {
	return t.b.RetryJobs()
}

// DeadJobs returns a copy of jobs in the dead set.
func (t *TestBroker) DeadJobs() []*Job {
	return t.b.DeadJobs()
}

// NewTestEngine creates an Engine and Client backed by an in-memory broker for
// database-free testing. The third return value allows tests to inspect retry and dead
// jobs. No Redis or other broker is required.
func NewTestEngine(opts ...Option) (*Engine, *Client, *TestBroker, error) {
	o := defaultOptions()
	for _, opt := range opts {
		opt(&o)
	}
	cfg := buildConfig(o)
	b := broker.NewInMemoryBroker()
	eng, err := newEngine(cfg, b)
	if err != nil {
		_ = b.Close()
		return nil, nil, nil, err
	}
	cl := client.New(b, cfg.Logger)
	return eng, cl, &TestBroker{b: b}, nil
}
