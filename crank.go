package crank

import (
	"fmt"
	"regexp"

	"github.com/ogwurujohnson/crank/internal/broker"
	"github.com/ogwurujohnson/crank/internal/client"
	"github.com/ogwurujohnson/crank/internal/config"
	"github.com/ogwurujohnson/crank/internal/payload"
	"github.com/ogwurujohnson/crank/internal/queue"
)

// New creates an Engine and Client connected to the broker at brokerURL.
// Options configure concurrency, timeouts, queues, and logging.
//
// A broker must be specified explicitly via WithBroker("redis"|"nats"|"pgsql")
// or by supplying a custom implementation with WithCustomBroker. If neither is
// provided, New returns an error.
func New(brokerURL string, opts ...Option) (*Engine, *Client, error) {
	defaultOpts := defaultOptions()
	for _, opt := range opts {
		opt(&defaultOpts)
	}

	cfg := buildConfig(defaultOpts, brokerURL)

	var store broker.Broker
	var err error

	switch {
	case defaultOpts.customBroker != nil:
		store = defaultOpts.customBroker
	case defaultOpts.brokerKind != "":
		store, err = newBroker(brokerURL, defaultOpts)
		if err != nil {
			return nil, nil, err
		}
	default:
		return nil, nil, fmt.Errorf("crank: no broker configured; use WithBroker(\"redis\"|\"nats\"|\"pgsql\") or WithCustomBroker()")
	}

	eng, err := newEngine(cfg, store)
	if err != nil {
		_ = store.Close()
		return nil, nil, err
	}

	cl := client.New(store, cfg.Logger)
	return eng, cl, nil
}

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
	cfg := buildConfig(o, "")
	b := broker.NewInMemoryBroker()
	eng, err := newEngine(cfg, b)
	if err != nil {
		_ = b.Close()
		return nil, nil, nil, err
	}
	cl := client.New(b, cfg.Logger)
	return eng, cl, &TestBroker{b: b}, nil
}

func buildConfig(opts options, brokerURL string) *config.Config {
	timeoutSec := int(opts.timeout.Seconds())
	if timeoutSec <= 0 {
		timeoutSec = 8
	}
	concurrency := opts.concurrency
	if concurrency <= 0 {
		concurrency = 10
	}
	qConfig := make([]config.QueueConfig, len(opts.queues))
	for i, q := range opts.queues {
		qConfig[i] = config.QueueConfig{Name: q.Name, Weight: q.Weight}
		if qConfig[i].Weight <= 0 {
			qConfig[i].Weight = 1
		}
	}
	if len(qConfig) == 0 {
		qConfig = []config.QueueConfig{{Name: "default", Weight: 1}}
	}
	if concurrency > 10000 {
		concurrency = 10000
	}

	redisTimeoutSec := int(opts.redisTimeout.Seconds())
	if redisTimeoutSec <= 0 {
		redisTimeoutSec = 5
	}

	return &config.Config{
		Concurrency:       concurrency,
		Timeout:           timeoutSec,
		Queues:            qConfig,
		Logger:            opts.logger,
		RetryPollInterval: opts.retryPollInterval,
		// TODO: this should be generic populated only based on the brokerType of choice
		Redis: config.RedisConfig{
			URL:                   brokerURL,
			NetworkTimeout:        redisTimeoutSec,
			UseTLS:                opts.useTLS,
			TLSInsecureSkipVerify: opts.tlsInsecureSkip,
		},
	}
}

func newBroker(brokerURL string, o options) (broker.Broker, error) {
	return broker.Open(o.brokerKind, brokerURL, broker.ConnOptions{
		Timeout:               o.redisTimeout,
		UseTLS:                o.useTLS,
		TLSInsecureSkipVerify: o.tlsInsecureSkip,
	})
}

// brokerURLAndOptsFromConfig returns the broker URL and ConnOptions for the configured broker kind.
func brokerURLAndOptsFromConfig(cfg *config.Config) (string, broker.ConnOptions) {
	switch cfg.Broker {
	case "nats":
		return cfg.NATS.URL, broker.ConnOptions{
			Timeout: cfg.NATS.GetTimeout(),
		}
	case "redis":
		return cfg.Redis.URL, broker.ConnOptions{
			Timeout:               cfg.Redis.GetNetworkTimeout(),
			UseTLS:                cfg.Redis.UseTLS,
			TLSInsecureSkipVerify: cfg.Redis.TLSInsecureSkipVerify,
		}
	default:
		// pgsql and others — URL comes from broker_url in config
		return cfg.BrokerURL, broker.ConnOptions{}
	}
}

// QuickStart builds an Engine and Client from a YAML config file and sets the global client.
// Use New with options for programmatic configuration.
func QuickStart(configPath string) (*Engine, *Client, error) {
	cfg, err := config.Load(configPath)
	if err != nil {
		return nil, nil, err
	}

	url, opts := brokerURLAndOptsFromConfig(cfg)
	b, err := broker.Open(cfg.Broker, url, opts)
	if err != nil {
		return nil, nil, err
	}

	eng, err := newEngine(cfg, b)
	if err != nil {
		_ = b.Close()
		return nil, nil, err
	}

	cl := client.New(b, cfg.Logger)
	client.SetGlobal(cl)
	return eng, cl, nil
}

// Client is the public client type for enqueueing jobs.
type Client = client.Client

var (
	NewClient          = client.New
	SetGlobalClient    = client.SetGlobal
	GetGlobalClient    = client.GetGlobal
	Enqueue            = client.EnqueueGlobal
	EnqueueWithOptions = client.EnqueueWithOptionsGlobal
)

// Broker is the backend-agnostic interface for job queue storage.
// Implement this interface to provide a custom broker backend via WithCustomBroker.
type Broker = broker.Broker

// Logger is the interface used for engine logging.
type Logger = config.Logger

// JobOptions configures optional job behavior when enqueueing.
type JobOptions = payload.JobOptions

// Job is the job payload type (exposed for handlers and validation).
type Job = payload.Job

var FromJSON = payload.FromJSON

// Worker is the interface implemented by job handlers.
type Worker = queue.Worker

var (
	RegisterWorker = queue.RegisterWorker
	ListWorkers    = queue.ListWorkers
)

// Handler is the type of the core job execution function.
type Handler = queue.Handler

// Middleware wraps a Handler.
type Middleware = queue.Middleware

// Chain composes middleware (used by Engine.Use).
type Chain = queue.Chain

var (
	LoggingMiddleware  = queue.LoggingMiddleware
	RecoveryMiddleware = queue.RecoveryMiddleware
	ErrCircuitOpen     = queue.ErrCircuitOpen
)

// Stats holds queue statistics (processed, retry, dead, queues).
type Stats = queue.Stats

// MetricsHandler handles job events for metrics.
type MetricsHandler = queue.MetricsHandler

// JobEvent and EventType are used by metrics and middleware.
type (
	JobEvent  = queue.JobEvent
	EventType = queue.EventType
)

const (
	EventJobStarted        = queue.EventJobStarted
	EventJobSucceeded      = queue.EventJobSucceeded
	EventJobFailed         = queue.EventJobFailed
	EventJobRetryScheduled = queue.EventJobRetryScheduled
	EventJobMovedToDead    = queue.EventJobMovedToDead
	MaxRetryCount          = payload.MaxRetryCount
	MaxBackoffShift        = payload.MaxBackoffShift
)

// Redactor redacts job args for logging.
type Redactor = payload.Redactor

var (
	NoopRedactor    = payload.NoopRedactor{}
	MaskingRedactor = payload.MaskingRedactor{}
)

func SetRedactor(r payload.Redactor) { payload.SetDefaultRedactor(r) }
func GetRedactor() payload.Redactor  { return payload.GetDefaultRedactor() }

func NewFieldMaskingRedactor(keys []string) *payload.FieldMaskingRedactor {
	return &payload.FieldMaskingRedactor{Keys: keys}
}

// Validator validates jobs before execution.
type Validator = payload.Validator

// ChainValidator composes validators.
type ChainValidator = payload.ChainValidator

var (
	MaxArgsCount   = payload.MaxArgsCount
	ClassAllowlist = payload.ClassAllowlist
	ClassPattern   = payload.ClassPattern
	MaxPayloadSize = payload.MaxPayloadSize
)

func SetValidator(v payload.Validator) { payload.SetDefaultValidator(v) }
func GetValidator() payload.Validator  { return payload.GetDefaultValidator() }

func SafeClassPattern() payload.Validator {
	return payload.ClassPattern(regexp.MustCompile(`^[A-Za-z0-9_]+$`))
}
