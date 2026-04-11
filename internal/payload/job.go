package payload

import (
	"encoding/json"
	"fmt"
	"time"

	"github.com/google/uuid"
)

type JobState string

const (
	JobStatePending JobState = "pending"
	JobStateActive  JobState = "active"
	JobStateSuccess JobState = "success"
	JobStateFailed  JobState = "failed"
	JobStateDead    JobState = "dead"

	// MaxRetryCount is the hard upper bound for job retries.
	MaxRetryCount = 25
	// MaxBackoffShift caps the exponential backoff shift to prevent overflow.
	MaxBackoffShift = 30
)

type Job struct {
	JID        string                 `json:"jid"`
	Class      string                 `json:"class"`
	Args       []interface{}          `json:"args"`
	Queue      string                 `json:"queue"`
	Retry      int                    `json:"retry"`
	RetryCount int                    `json:"retry_count"`
	CreatedAt  float64                `json:"created_at"`
	EnqueuedAt float64                `json:"enqueued_at"`
	Backtrace  bool                   `json:"backtrace"`
	State      JobState               `json:"state"`
	Metadata   map[string]interface{} `json:"metadata,omitempty"`
}

func NewJob(workerClass string, queue string, args ...interface{}) *Job {
	now := float64(time.Now().Unix())
	return &Job{
		JID:        uuid.New().String(),
		Class:      workerClass,
		Args:       args,
		Queue:      queue,
		Retry:      5,
		RetryCount: 0,
		CreatedAt:  now,
		EnqueuedAt: now,
		Backtrace:  false,
		State:      JobStatePending,
		Metadata:   make(map[string]interface{}),
	}
}

func (j *Job) SetRetry(count int) *Job {
	if count > MaxRetryCount {
		count = MaxRetryCount
	}
	if count < 0 {
		count = 0
	}
	j.Retry = count
	return j
}

func (j *Job) SetBacktrace(enabled bool) *Job {
	j.Backtrace = enabled
	return j
}

func (j *Job) ToJSON() ([]byte, error) {
	return json.Marshal(j)
}

func FromJSON(data []byte) (*Job, error) {
	var job Job
	if err := json.Unmarshal(data, &job); err != nil {
		return nil, fmt.Errorf("failed to unmarshal job: %w", err)
	}
	if job.State == "" {
		job.State = JobStatePending
	}
	if job.Retry > MaxRetryCount {
		job.Retry = MaxRetryCount
	}
	if job.Retry < 0 {
		job.Retry = 0
	}
	if job.RetryCount < 0 {
		job.RetryCount = 0
	}
	if job.RetryCount > MaxRetryCount {
		job.RetryCount = MaxRetryCount
	}
	return &job, nil
}

func (j *Job) String() string {
	return fmt.Sprintf("Job{Class: %s, Queue: %s, JID: %s}", j.Class, j.Queue, j.JID)
}

type JobOptions struct {
	Retry     *int
	Backtrace *bool
}
