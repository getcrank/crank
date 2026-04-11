package broker

import (
	"time"

	"github.com/ogwurujohnson/crank/internal/payload"
)

// Broker is the internal backend-agnostic interface for job queues.
// Implementations (Redis, NATS, etc.) live in this package and are
// selected by Open(kind, url, opts). Callers outside this package
// never reference a concrete backend—they use Broker only.
//
// Dequeue leases the job: it is moved to a processing set with a
// visibility timeout. The caller must Ack (on success or after
// scheduling retry/dead) or Nack (to re-enqueue immediately).
// Jobs whose lease expires without an Ack are reclaimed by
// ReapOrphanedJobs.
type Broker interface {
	Enqueue(queue string, job *payload.Job) error
	Dequeue(queues []string, timeout time.Duration) (*payload.Job, string, error)
	Ack(job *payload.Job) error
	Nack(job *payload.Job) error
	ReapOrphanedJobs(lease time.Duration) ([]*payload.Job, error)
	AddToRetry(job *payload.Job, retryAt time.Time) error
	GetRetryJobs(limit int64) ([]*payload.Job, error)
	RemoveFromRetry(job *payload.Job) error
	AddToDead(job *payload.Job) error
	GetDeadJobs(limit int64) ([]*payload.Job, error)
	GetQueueSize(queue string) (int64, error)
	DeleteKey(key string) error
	GetStats() (map[string]interface{}, error)
	Close() error
}

// ConnOptions holds connection options used when opening a broker via Open.
// Backends map these to their own config (e.g. Redis timeout/TLS).
type ConnOptions struct {
	Timeout               time.Duration
	UseTLS                bool
	TLSInsecureSkipVerify bool
}
