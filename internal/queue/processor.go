package queue

import (
	"context"
	"errors"
	"fmt"
	"sync"
	"time"

	"github.com/ogwurujohnson/crank/internal/broker"
	"github.com/ogwurujohnson/crank/internal/config"
	"github.com/ogwurujohnson/crank/internal/payload"
)

type jobMsg struct {
	job   *payload.Job
	queue string
}

type Processor struct {
	cfg       *config.Config
	broker    broker.Broker
	registry  WorkerRegistry
	log       Logger
	queues    []string
	queueSet  map[string]bool
	jobCh     chan jobMsg
	wg        sync.WaitGroup
	ctx       context.Context
	cancel    context.CancelFunc
	chain     *Chain
	handler   Handler
	breaker   *CircuitBreaker
	metrics   MetricsHandler
	events    chan JobEvent
	started   bool
	mu        sync.Mutex
}

func NewProcessor(cfg *config.Config, b broker.Broker, registry WorkerRegistry, chain *Chain) (*Processor, error) {
	var queues []string
	for _, qc := range cfg.Queues {
		weight := qc.Weight
		if weight <= 0 {
			weight = 1
		}
		for i := 0; i < weight; i++ {
			queues = append(queues, qc.Name)
		}
	}

	if len(queues) == 0 {
		queues = []string{"default"}
	}

	queueSet := make(map[string]bool)
	for _, q := range queues {
		queueSet[q] = true
	}

	ctx, cancel := context.WithCancel(context.Background())
	log := cfg.Logger
	if log == nil {
		log = NopLogger()
	}

	if chain == nil {
		chain = NewChain(RecoveryMiddleware(log), LoggingMiddleware(log))
	}

	p := &Processor{
		cfg:      cfg,
		broker:   b,
		registry: registry,
		log:      log,
		queues:   queues,
		queueSet: queueSet,
		jobCh:    make(chan jobMsg),
		ctx:      ctx,
		cancel:   cancel,
		chain:    chain,
	}

	// Initialize handler so processJob works even without Start() (e.g., in tests).
	// Start() re-wraps to include any middleware added via Use() after construction.
	p.handler = p.chain.Wrap(p.execute)

	return p, nil
}

// execute is the core logic wrapped by middleware
func (p *Processor) execute(ctx context.Context, job *payload.Job) error {
	if v := payload.GetDefaultValidator(); v != nil {
		if err := v.Validate(job); err != nil {
			return fmt.Errorf("validation failed: %w", err)
		}
	}

	var (
		worker   Worker
		localErr error
	)

	// Fallback chain: local registry -> global registry
	if p.registry != nil {
		worker, localErr = p.registry.GetWorker(job.Class)
	}
	if worker == nil {
		var globalErr error
		worker, globalErr = GetWorker(job.Class)
		if worker == nil {
			if localErr != nil {
				return fmt.Errorf("worker not found for %s: %w (global: %v)", job.Class, localErr, globalErr)
			}
			return fmt.Errorf("worker not found for %s: %w", job.Class, globalErr)
		}
	}

	return worker.Perform(ctx, job.Args...)
}

func (p *Processor) Start() error {
	p.mu.Lock()
	if p.started {
		p.mu.Unlock()
		return fmt.Errorf("processor already started")
	}
	p.started = true
	p.handler = p.chain.Wrap(p.execute)
	p.mu.Unlock()
	p.log.Info("engine started", "concurrency", p.cfg.Concurrency, "queues", p.queues)

	// Start Fetcher
	p.wg.Add(1)
	go p.fetcher()

	// Start Workers
	for i := 0; i < p.cfg.Concurrency; i++ {
		p.wg.Add(1)
		go p.worker()
	}

	// Start Maintenance Loops
	p.wg.Add(1)
	go p.retryLoop()
	if p.events != nil {
		p.wg.Add(1)
		go p.metricsLoop()
	}

	return nil
}

func (p *Processor) Stop() {
	p.log.Info("engine stopping")
	p.cancel()
	p.wg.Wait()
	p.log.Info("engine stopped")
}

func (p *Processor) fetcher() {
	defer p.wg.Done()
	defer close(p.jobCh)

	// cursor tracks our position in the weighted queues slice
	cursor := 0
	numQueues := len(p.queues)

	pauseTimer := time.NewTimer(0)
	if !pauseTimer.Stop() {
		<-pauseTimer.C
	}

	for {
		select {
		case <-p.ctx.Done():
			return
		default:
			// Pick the next queue based on the weighted slice
			// This ensures we respect the frequency defined in NewProcessor
			currentQueue := p.queues[cursor]
			cursor = (cursor + 1) % numQueues

			// Dequeue only from this specific queue
			job, q, err := p.broker.Dequeue([]string{currentQueue}, time.Second)

			if err != nil {
				p.log.Warn("dequeue failed", "queue", currentQueue, "err", err)
				continue
			}
			if job == nil {
				continue
			}

			p.log.Info("job dequeued", "jid", job.JID, "class", job.Class, "queue", q)

			select {
			case p.jobCh <- jobMsg{job: job, queue: q}:
			case <-p.ctx.Done():
				job.State = payload.JobStatePending
				if err := p.broker.Enqueue(q, job); err != nil {
					p.log.Error("failed to re-enqueue job on shutdown", "jid", job.JID, "queue", q, "err", err)
				}
				return
			}
		}
	}
}

func (p *Processor) worker() {
	defer p.wg.Done()
	for msg := range p.jobCh {
		p.processJob(msg.job, msg.queue)
	}
}

func (p *Processor) processJob(job *payload.Job, queue string) {
	start := time.Now()
	p.emitEvent(JobEvent{Type: EventJobStarted, Job: job, Queue: queue})

	ctx, cancel := context.WithTimeout(p.ctx, p.cfg.GetTimeout())
	defer cancel()

	job.State = payload.JobStateActive
	err := p.handler(ctx, job)
	duration := time.Since(start)

	if err != nil {
		job.State = payload.JobStateFailed
		p.log.Error("job failed", "jid", job.JID, "class", job.Class, "queue", queue, "err", err, "dur", duration)
		p.emitEvent(JobEvent{Type: EventJobFailed, Job: job, Queue: queue, Duration: duration, Err: err})
		p.handleFailure(job, err)
	} else {
		job.State = payload.JobStateSuccess
		p.log.Info("job processed", "jid", job.JID, "class", job.Class, "queue", queue, "dur", duration)
		p.emitEvent(JobEvent{Type: EventJobSucceeded, Job: job, Queue: queue, Duration: duration})
	}
}

func (p *Processor) handleFailure(job *payload.Job, jobErr error) {
	// Circuit-open: re-enqueue without burning a retry attempt
	if errors.Is(jobErr, ErrCircuitOpen) {
		job.State = payload.JobStatePending
		if err := p.broker.AddToRetry(job, time.Now().Add(5*time.Second)); err != nil {
			p.log.Warn("circuit-open re-enqueue failed", "jid", job.JID, "err", err)
		}
		return
	}

	job.RetryCount++
	if job.RetryCount <= job.Retry {
		// Exponential backoff: 2^count, capped to prevent overflow
		shift := job.RetryCount
		if shift > payload.MaxBackoffShift {
			shift = payload.MaxBackoffShift
		}
		delay := time.Duration(1<<uint(shift)) * time.Second
		retryAt := time.Now().Add(delay)

		p.emitEvent(JobEvent{Type: EventJobRetryScheduled, Job: job, Queue: job.Queue, Err: jobErr})
		if err := p.broker.AddToRetry(job, retryAt); err != nil {
			p.log.Warn("retry schedule failed", "jid", job.JID, "err", err)
		}
	} else {
		job.State = payload.JobStateDead
		p.log.Warn("job exceeded max retries, moving to dead queue", "jid", job.JID, "class", job.Class, "queue", job.Queue, "retries", job.RetryCount)
		p.emitEvent(JobEvent{Type: EventJobMovedToDead, Job: job, Queue: job.Queue, Err: jobErr})
		if err := p.broker.AddToDead(job); err != nil {
			p.log.Warn("move to dead failed", "jid", job.JID, "class", job.Class, "err", err)
		}
	}
}

func (p *Processor) retryLoop() {
	defer p.wg.Done()
	interval := p.cfg.RetryPollInterval
	if interval <= 0 {
		interval = 5 * time.Second
	}
	ticker := time.NewTicker(interval)
	defer ticker.Stop()

	for {
		select {
		case <-p.ctx.Done():
			return
		case <-ticker.C:
			jobs, err := p.broker.GetRetryJobs(100)
			if err != nil {
				p.log.Warn("get retry jobs failed", "err", err)
				continue
			}
			for _, j := range jobs {
				if !p.queueSet[j.Queue] {
					p.log.Warn("retry job has unknown queue, skipping", "jid", j.JID, "queue", j.Queue)
					continue
				}
				if err := p.broker.RemoveFromRetry(j); err != nil {
					p.log.Warn("remove from retry failed", "jid", j.JID, "err", err)
					continue
				}
				j.State = payload.JobStatePending
				if err := p.broker.Enqueue(j.Queue, j); err != nil {
					p.log.Warn("re-enqueue retry job failed, re-adding to retry", "jid", j.JID, "err", err)
					if addErr := p.broker.AddToRetry(j, time.Now().Add(time.Minute)); addErr != nil {
						p.log.Error("failed to re-add job to retry set, job may be lost", "jid", j.JID, "err", addErr)
					}
				}
			}
		}
	}
}

func (p *Processor) metricsLoop() {
	defer p.wg.Done()
	for {
		select {
		case <-p.ctx.Done():
			return
		case ev, ok := <-p.events:
			if !ok {
				return
			}
			if p.metrics != nil {
				p.metrics.HandleJobEvent(p.ctx, ev)
			}
		}
	}
}

func (p *Processor) emitEvent(ev JobEvent) {
	if p.events == nil {
		return
	}
	select {
	case p.events <- ev:
	default:
		// Drop event if buffer is full to avoid blocking workers
		p.log.Debug("metrics event dropped", "type", ev.Type, "jid", ev.Job.JID)
	}
}

func (p *Processor) SetMetricsHandler(h MetricsHandler) error {
	p.mu.Lock()
	defer p.mu.Unlock()
	if p.started {
		return fmt.Errorf("SetMetricsHandler must be called before Start")
	}
	p.metrics = h
	if h != nil && p.events == nil {
		size := p.cfg.Concurrency * 10
		if size < 10 {
			size = 10
		}
		p.events = make(chan JobEvent, size)
	}
	return nil
}
