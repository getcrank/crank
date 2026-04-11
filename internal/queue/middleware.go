package queue

import (
	"context"
	"errors"
	"fmt"
	"runtime"

	"github.com/ogwurujohnson/crank/internal/payload"
)

type Handler func(ctx context.Context, job *payload.Job) error

type Middleware func(next Handler) Handler

type Chain struct {
	middlewares []Middleware
}

func NewChain(ms ...Middleware) *Chain {
	return &Chain{
		middlewares: ms,
	}
}

func (c *Chain) Use(m ...Middleware) {
	c.middlewares = append(c.middlewares, m...)
}

func (c *Chain) Wrap(final Handler) Handler {
	handler := final
	for index := len(c.middlewares) - 1; index >= 0; index-- {
		handler = c.middlewares[index](handler)
	}
	return handler
}

func LoggingMiddleware(logger Logger) Middleware {
	return func(next Handler) Handler {
		return func(ctx context.Context, job *payload.Job) error {
			err := next(ctx, job)
			if err != nil {
				redactor := payload.GetDefaultRedactor()
				safeArgs := redactor.RedactArgs(job.Args)
				logger.Error("job failed", "jid", job.JID, "args", safeArgs, "err", err)
			}
			return err
		}
	}
}

func RecoveryMiddleware(logger Logger) Middleware {
	return func(next Handler) Handler {
		return func(ctx context.Context, job *payload.Job) (err error) {
			defer func() {
				if panicValue := recover(); panicValue != nil {
					buf := make([]byte, 64<<10)
					n := runtime.Stack(buf, false)
					stack := string(buf[:n])

					logger.Error("panic recovered in job handler",
						"jid", job.JID,
						"panic", panicValue,
						"stack", stack,
					)

					err = fmt.Errorf("panic: %v", panicValue)
				}
			}()

			return next(ctx, job)
		}
	}
}

// ErrCircuitOpen is returned when the circuit breaker is open for a job class.
var ErrCircuitOpen = errors.New("circuit breaker is open")

// BreakerMiddleware checks the circuit breaker before execution and records outcomes after.
func BreakerMiddleware(breaker *CircuitBreaker) Middleware {
	if breaker == nil {
		return func(next Handler) Handler { return next }
	}
	return func(next Handler) Handler {
		return func(ctx context.Context, job *payload.Job) error {
			if !breaker.Allow(job.Class) {
				return ErrCircuitOpen
			}
			err := next(ctx, job)
			if err != nil {
				breaker.RecordFailure(job.Class)
				return err
			}
			breaker.RecordSuccess(job.Class)
			return nil
		}
	}
}
