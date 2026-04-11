package crank

import (
	"time"

	"github.com/ogwurujohnson/crank/internal/broker"
)

// Option configures the engine and client created by New.
type Option func(*options)

type options struct {
	brokerKind        string        // "redis", "nats", "pgsql"; must be set explicitly via WithBroker
	customBroker      broker.Broker // user-provided broker implementation; takes precedence over brokerKind
	concurrency       int
	timeout           time.Duration
	queues            []queueOpt
	logger            Logger
	retryPollInterval time.Duration
	reaperInterval    time.Duration
	redisTimeout      time.Duration
	useTLS            bool
	tlsInsecureSkip   bool
}

type queueOpt struct {
	Name   string
	Weight int
}

// QueueOption defines a queue name and its polling weight (higher = polled more often).
type QueueOption struct {
	Name   string
	Weight int
}

// WithBroker sets the broker backend explicitly ("redis", "nats", "pgsql").
// Either WithBroker or WithCustomBroker must be provided; there is no default.
func WithBroker(kind string) Option {
	return func(o *options) {
		o.brokerKind = kind
	}
}

// WithCustomBroker injects a user-provided Broker implementation.
// When set, brokerURL and WithBroker are ignored and this broker is used directly.
func WithCustomBroker(b broker.Broker) Option {
	return func(o *options) {
		o.customBroker = b
	}
}

// WithConcurrency sets the number of concurrent workers. Default is 10.
func WithConcurrency(n int) Option {
	return func(o *options) {
		o.concurrency = n
	}
}

// WithTimeout sets the per-job execution timeout. Default is 8 seconds.
func WithTimeout(d time.Duration) Option {
	return func(o *options) {
		o.timeout = d
	}
}

// WithQueues sets the queues to poll. Default is a single queue named "default" with weight 1.
func WithQueues(qs ...QueueOption) Option {
	return func(o *options) {
		o.queues = make([]queueOpt, len(qs))
		for i, q := range qs {
			o.queues[i] = queueOpt{Name: q.Name, Weight: q.Weight}
			if o.queues[i].Weight <= 0 {
				o.queues[i].Weight = 1
			}
		}
	}
}

// WithLogger sets the logger for the engine. If not set, a no-op logger is used.
func WithLogger(l Logger) Option {
	return func(o *options) {
		o.logger = l
	}
}

// WithRetryPollInterval sets how often the retry set is checked. Default is 5 seconds.
func WithRetryPollInterval(d time.Duration) Option {
	return func(o *options) {
		o.retryPollInterval = d
	}
}

// WithReaperInterval sets how often the reaper checks for orphaned jobs. Default is 30 seconds.
func WithReaperInterval(d time.Duration) Option {
	return func(o *options) {
		o.reaperInterval = d
	}
}

// WithRedisTimeout sets the Redis connection/read/write timeout. Default is 5 seconds.
func WithRedisTimeout(d time.Duration) Option {
	return func(o *options) {
		o.redisTimeout = d
	}
}

// WithTLS enables TLS for the Redis connection when the URL does not use rediss://.
func WithTLS(use bool) Option {
	return func(o *options) {
		o.useTLS = use
	}
}

// WithTLSInsecureSkipVerify skips TLS certificate verification (insecure).
func WithTLSInsecureSkipVerify(skip bool) Option {
	return func(o *options) {
		o.tlsInsecureSkip = skip
	}
}

func defaultOptions() options {
	return options{
		concurrency:       10,
		timeout:           8 * time.Second,
		queues:            []queueOpt{{Name: "default", Weight: 1}},
		retryPollInterval: 5 * time.Second,
		redisTimeout:      5 * time.Second,
	}
}
