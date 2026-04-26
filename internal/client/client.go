package client

import (
	"context"
	"fmt"
	"sync/atomic"

	"github.com/ogwurujohnson/crank/internal/broker"
	"github.com/ogwurujohnson/crank/internal/config"
	"github.com/ogwurujohnson/crank/internal/payload"
)

var globalClient atomic.Pointer[Client]

type Client struct {
	broker broker.Broker
	logger config.Logger
}

func New(b broker.Broker, logger config.Logger) *Client {
	return &Client{broker: b, logger: logger}
}

func SetGlobal(c *Client) {
	globalClient.Store(c)
}

func GetGlobal() *Client {
	return globalClient.Load()
}

func (c *Client) Enqueue(ctx context.Context, workerClass string, queue string, args ...interface{}) (string, error) {
	if err := payload.ValidateQueueName(queue); err != nil {
		return "", fmt.Errorf("failed to enqueue job: %w", err)
	}
	if err := payload.ValidateWorkerClass(workerClass); err != nil {
		return "", fmt.Errorf("failed to enqueue job: %w", err)
	}

	job := payload.NewJob(workerClass, queue, args...)

	if v := payload.GetDefaultValidator(); v != nil {
		if err := v.Validate(job); err != nil {
			return "", fmt.Errorf("failed to enqueue job: validation failed: %w", err)
		}
	}

	if err := c.broker.Enqueue(ctx, queue, job); err != nil {
		return "", fmt.Errorf("failed to enqueue job: %w", err)
	}
	if c.logger != nil {
		c.logger.Info("job enqueued", "jid", job.JID, "class", workerClass, "queue", queue)
	}
	return job.JID, nil
}

func (c *Client) EnqueueWithOptions(ctx context.Context, workerClass string, queue string, options *payload.JobOptions, args ...interface{}) (string, error) {
	if err := payload.ValidateQueueName(queue); err != nil {
		return "", fmt.Errorf("failed to enqueue job: %w", err)
	}
	if err := payload.ValidateWorkerClass(workerClass); err != nil {
		return "", fmt.Errorf("failed to enqueue job: %w", err)
	}

	job := payload.NewJob(workerClass, queue, args...)

	if options != nil {
		if options.Retry != nil {
			job.SetRetry(*options.Retry)
		}
		if options.Backtrace != nil {
			job.SetBacktrace(*options.Backtrace)
		}
	}

	if v := payload.GetDefaultValidator(); v != nil {
		if err := v.Validate(job); err != nil {
			return "", fmt.Errorf("failed to enqueue job: validation failed: %w", err)
		}
	}

	if err := c.broker.Enqueue(ctx, queue, job); err != nil {
		return "", fmt.Errorf("failed to enqueue job: %w", err)
	}
	if c.logger != nil {
		c.logger.Info("job enqueued", "jid", job.JID, "class", workerClass, "queue", queue)
	}
	return job.JID, nil
}

func EnqueueGlobal(ctx context.Context, workerClass string, queue string, args ...interface{}) (string, error) {
	c := globalClient.Load()
	if c == nil {
		return "", fmt.Errorf("global client not initialized. Call SetGlobalClient first")
	}
	return c.Enqueue(ctx, workerClass, queue, args...)
}

func EnqueueWithOptionsGlobal(ctx context.Context, workerClass string, queue string, options *payload.JobOptions, args ...interface{}) (string, error) {
	c := globalClient.Load()
	if c == nil {
		return "", fmt.Errorf("global client not initialized. Call SetGlobalClient first")
	}
	return c.EnqueueWithOptions(ctx, workerClass, queue, options, args...)
}
