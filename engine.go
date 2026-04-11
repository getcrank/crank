package crank

import (
	"fmt"
	"sync"

	"github.com/ogwurujohnson/crank/internal/broker"
	"github.com/ogwurujohnson/crank/internal/config"
	"github.com/ogwurujohnson/crank/internal/queue"
)

type engineRegistry struct {
	mu      sync.RWMutex
	workers map[string]queue.Worker
}

func (r *engineRegistry) GetWorker(className string) (queue.Worker, error) {
	r.mu.RLock()
	defer r.mu.RUnlock()
	worker, ok := r.workers[className]
	if !ok {
		return nil, fmt.Errorf("worker class '%s' not found", className)
	}
	return worker, nil
}

func (r *engineRegistry) register(className string, worker queue.Worker) {
	r.mu.Lock()
	defer r.mu.Unlock()
	if r.workers == nil {
		r.workers = make(map[string]queue.Worker)
	}
	r.workers[className] = worker
}

type Engine struct {
	cfg       *config.Config
	broker    broker.Broker
	processor *queue.Processor
	registry  *engineRegistry
	chain     *queue.Chain
	mu        sync.Mutex
	started   bool
	stopOnce  sync.Once
}

// newEngine creates an Engine from internal config and broker. Used by New and QuickStart.
func newEngine(cfg *config.Config, store broker.Broker) (*Engine, error) {
	if cfg.Logger == nil {
		cfg.Logger = queue.NopLogger()
	}

	registry := &engineRegistry{workers: make(map[string]queue.Worker)}
	breaker := queue.NewCircuitBreaker(queue.BreakerConfig{})
	chain := queue.NewChain(
		queue.RecoveryMiddleware(cfg.Logger),
		queue.LoggingMiddleware(cfg.Logger),
		queue.BreakerMiddleware(breaker),
	)
	processor, err := queue.NewProcessor(cfg, store, registry, chain)
	if err != nil {
		return nil, err
	}

	return &Engine{
		cfg:       cfg,
		broker:    store,
		processor: processor,
		registry:  registry,
		chain:     chain,
	}, nil
}

// Use adds middleware to the engine. Must be called before Start.
func (e *Engine) Use(middleware ...Middleware) error {
	e.mu.Lock()
	defer e.mu.Unlock()
	if e.started {
		return fmt.Errorf("Use must be called before Start")
	}
	if e.chain == nil {
		return fmt.Errorf("engine chain not initialized")
	}
	e.chain.Use(middleware...)
	return nil
}

func (e *Engine) Register(className string, worker Worker) {
	if className == "" {
		return
	}
	e.registry.register(className, worker)
}

func (e *Engine) RegisterMany(workers map[string]Worker) {
	for name, worker := range workers {
		if name != "" {
			e.registry.register(name, worker)
		}
	}
}

func (e *Engine) Start() error {
	e.mu.Lock()
	if e.started {
		e.mu.Unlock()
		return fmt.Errorf("engine already started")
	}
	e.started = true
	e.mu.Unlock()
	return e.processor.Start()
}

func (e *Engine) Stop() {
	e.stopOnce.Do(func() {
		e.processor.Stop()
		_ = e.broker.Close()
	})
}

// SetMetricsHandler sets the metrics handler for the engine. Must be called before Start.
func (e *Engine) SetMetricsHandler(h MetricsHandler) error {
	return e.processor.SetMetricsHandler(h)
}

// Stats returns queue statistics (processed, retry, dead, per-queue sizes).
func (e *Engine) Stats() (*Stats, error) {
	return queue.GetStats(e.broker)
}
