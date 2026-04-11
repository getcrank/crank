package queue

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/ogwurujohnson/crank/internal/config"
	"github.com/ogwurujohnson/crank/internal/payload"
)

// --- Helpers ---

type workerFunc func(ctx context.Context, args ...interface{}) error

func (f workerFunc) Perform(ctx context.Context, args ...interface{}) error { return f(ctx, args...) }

type fixedRegistry struct {
	w   Worker
	err error
}

func (r fixedRegistry) GetWorker(string) (Worker, error) {
	if r.err != nil {
		return nil, r.err
	}
	return r.w, nil
}

type spyBroker struct {
	enqueueErr                error
	enq, retry, dead, removed []*payload.Job
	enqNames                  []string
	retryAt                   []time.Time
	retryJobs                 []*payload.Job
}

func (b *spyBroker) Enqueue(q string, j *payload.Job) error {
	b.enqNames = append(b.enqNames, q)
	b.enq = append(b.enq, j)
	return b.enqueueErr
}
func (b *spyBroker) Dequeue([]string, time.Duration) (*payload.Job, string, error) {
	return nil, "", nil
}
func (b *spyBroker) AddToRetry(j *payload.Job, at time.Time) error {
	b.retry = append(b.retry, j)
	b.retryAt = append(b.retryAt, at)
	return nil
}
func (b *spyBroker) GetRetryJobs(int64) ([]*payload.Job, error) { return b.retryJobs, nil }
func (b *spyBroker) RemoveFromRetry(j *payload.Job) error {
	b.removed = append(b.removed, j)
	return nil
}
func (b *spyBroker) AddToDead(j *payload.Job) error            { b.dead = append(b.dead, j); return nil }
func (b *spyBroker) GetDeadJobs(int64) ([]*payload.Job, error) { return nil, nil }
func (b *spyBroker) GetQueueSize(string) (int64, error)        { return 0, nil }
func (b *spyBroker) DeleteKey(string) error                    { return nil }
func (b *spyBroker) GetStats() (map[string]interface{}, error) { return nil, nil }
func (b *spyBroker) Close() error                              { return nil }

type metricsChanHandler struct{ ch chan JobEvent }

func (h metricsChanHandler) HandleJobEvent(_ context.Context, e JobEvent) { h.ch <- e }

func sliceContains[T comparable](s []T, v T) bool {
	for _, x := range s {
		if x == v {
			return true
		}
	}
	return false
}

// --- Tests ---

func TestNewProcessor_Configuration(t *testing.T) {
	b := &spyBroker{}

	t.Run("WeightedQueues", func(t *testing.T) {
		p, _ := NewProcessor(&config.Config{
			Queues: []config.QueueConfig{
				{Name: "critical", Weight: 2},
				{Name: "default", Weight: 1},
			},
		}, b, nil, nil)
		want := []string{"critical", "critical", "default"}
		if len(p.queues) != len(want) {
			t.Fatalf("queues length = %d, want %d", len(p.queues), len(want))
		}
		for i := range want {
			if p.queues[i] != want[i] {
				t.Errorf("queues[%d] = %q, want %q", i, p.queues[i], want[i])
			}
		}
	})

	t.Run("DefaultQueueFallback", func(t *testing.T) {
		p, _ := NewProcessor(&config.Config{}, b, nil, nil)
		want := []string{"default"}
		if len(p.queues) != 1 || p.queues[0] != "default" {
			t.Errorf("queues = %v, want %v", p.queues, want)
		}
	})
}

func TestProcessor_ExecutionFlow(t *testing.T) {
	setup := func(w Worker) (*Processor, *spyBroker) {
		b := &spyBroker{}
		p, _ := NewProcessor(&config.Config{Timeout: 1}, b, fixedRegistry{w: w}, nil)
		return p, b
	}

	t.Run("SchedulesRetryOnFailure", func(t *testing.T) {
		p, b := setup(workerFunc(func(context.Context, ...interface{}) error { return errors.New("boom") }))
		j := payload.NewJob("W", "default", 1).SetRetry(1)

		p.processJob(j, "default")

		if len(b.retry) != 1 {
			t.Errorf("len(retry) = %d, want 1", len(b.retry))
		}
		if j.State != payload.JobStateFailed {
			t.Errorf("State = %q, want %q", j.State, payload.JobStateFailed)
		}
		if len(b.retryAt) > 0 && !b.retryAt[0].After(time.Now()) {
			t.Error("retryAt should be in the future (exponential backoff)")
		}
	})

	t.Run("MovesToDeadWhenExhausted", func(t *testing.T) {
		p, b := setup(workerFunc(func(context.Context, ...interface{}) error { return errors.New("boom") }))
		j := payload.NewJob("W", "default", 1).SetRetry(0)

		p.processJob(j, "default")

		if len(b.dead) != 1 {
			t.Errorf("len(dead) = %d, want 1", len(b.dead))
		}
		if j.State != payload.JobStateDead {
			t.Errorf("State = %q, want %q", j.State, payload.JobStateDead)
		}
	})
}

func TestProcessor_Lifecycle(t *testing.T) {
	t.Run("RetryLoopIntegration", func(t *testing.T) {
		job := payload.NewJob("W", "critical", 1)
		b := &spyBroker{retryJobs: []*payload.Job{job}}
		p, _ := NewProcessor(&config.Config{
			RetryPollInterval: 20 * time.Millisecond,
			Queues:            []config.QueueConfig{{Name: "critical", Weight: 1}},
		}, b, nil, nil)

		p.wg.Add(1)
		go p.retryLoop()

		time.Sleep(50 * time.Millisecond)
		p.Stop()

		if len(b.removed) == 0 {
			t.Error("expected at least one removed from retry")
		}
		if !sliceContains(b.enqNames, "critical") {
			t.Errorf("enqNames = %v, want to contain critical", b.enqNames)
		}
		if job.State != payload.JobStatePending {
			t.Errorf("job.State = %q, want %q", job.State, payload.JobStatePending)
		}
	})
}

func TestProcessor_Metrics(t *testing.T) {
	b := &spyBroker{}
	p, _ := NewProcessor(&config.Config{Concurrency: 1}, b, fixedRegistry{
		w: workerFunc(func(context.Context, ...interface{}) error { return nil }),
	}, nil)

	eventsCh := make(chan JobEvent, 10)
	p.SetMetricsHandler(metricsChanHandler{ch: eventsCh})

	p.wg.Add(1)
	go p.metricsLoop()

	p.processJob(payload.NewJob("W", "default", 1), "default")

	var received []EventType
	for i := 0; i < 2; i++ {
		select {
		case ev := <-eventsCh:
			received = append(received, ev.Type)
		case <-time.After(500 * time.Millisecond):
			t.Fatal("metrics event timeout")
		}
	}

	if !sliceContains(received, EventJobStarted) {
		t.Errorf("received = %v, want to contain EventJobStarted", received)
	}
	if !sliceContains(received, EventJobSucceeded) {
		t.Errorf("received = %v, want to contain EventJobSucceeded", received)
	}

	p.Stop()
}
