package broker

import (
	"context"
	"crypto/tls"
	"fmt"
	"log"
	"strings"
	"time"

	"github.com/go-redis/redis/v8"
	"github.com/ogwurujohnson/crank/internal/payload"
)

type RedisBrokerConfig struct {
	URL                   string
	Timeout               time.Duration
	UseTLS                bool
	TLSInsecureSkipVerify bool
}

type RedisBroker struct {
	client *redis.Client
	ctx    context.Context
	cancel context.CancelFunc
}

func NewRedisBroker(redisURL string, timeout time.Duration) (*RedisBroker, error) {
	return NewRedisBrokerWithConfig(RedisBrokerConfig{
		URL:     redisURL,
		Timeout: timeout,
	})
}

func NewRedisBrokerWithConfig(cfg RedisBrokerConfig) (*RedisBroker, error) {
	trimmedURL := strings.TrimSpace(cfg.URL)
	if trimmedURL == "" {
		return nil, fmt.Errorf("broker not available: Redis URL is empty (set redis.url in config or REDIS_URL)")
	}

	if cfg.UseTLS && !strings.HasPrefix(trimmedURL, "rediss://") {
		trimmedURL = strings.Replace(trimmedURL, "redis://", "rediss://", 1)
	}

	opt, err := redis.ParseURL(trimmedURL)
	if err != nil {
		return nil, fmt.Errorf("broker not available: invalid Redis URL: %w", err)
	}

	opt.DialTimeout = cfg.Timeout
	opt.ReadTimeout = cfg.Timeout
	opt.WriteTimeout = cfg.Timeout

	if cfg.UseTLS || strings.HasPrefix(trimmedURL, "rediss://") {
		if cfg.TLSInsecureSkipVerify {
			log.Println("WARNING: TLS certificate verification disabled; this is insecure in production")
		}
		opt.TLSConfig = &tls.Config{
			MinVersion:         tls.VersionTLS12,
			InsecureSkipVerify: cfg.TLSInsecureSkipVerify,
		}
	}

	client := redis.NewClient(opt)
	ctx, cancel := context.WithCancel(context.Background())

	if err := client.Ping(ctx).Err(); err != nil {
		cancel()
		_ = client.Close()
		return nil, fmt.Errorf("broker not available: Redis unreachable at %q: %w", opt.Addr, err)
	}

	return &RedisBroker{
		client: client,
		ctx:    ctx,
		cancel: cancel,
	}, nil
}

func (r *RedisBroker) Close() error {
	r.cancel()
	return r.client.Close()
}

func (r *RedisBroker) Enqueue(queue string, job *payload.Job) error {
	data, err := job.ToJSON()
	if err != nil {
		return fmt.Errorf("failed to serialize job: %w", err)
	}

	queueKey := fmt.Sprintf("queue:%s", queue)
	if err := r.client.LPush(r.ctx, queueKey, data).Err(); err != nil {
		return fmt.Errorf("failed to enqueue job: %w", err)
	}

	// Best-effort stat tracking; errors are non-fatal
	_ = r.client.ZAdd(r.ctx, "stat:processed", &redis.Z{
		Score:  float64(time.Now().Unix()),
		Member: job.JID,
	}).Err()

	// Trim stat:processed to last 100,000 entries to prevent unbounded growth
	_ = r.client.ZRemRangeByRank(r.ctx, "stat:processed", 0, -100001).Err()

	return nil
}

// dequeueAndLeaseScript atomically pops from a queue list and adds to the
// processing sorted set with a lease expiry score.
// KEYS[1]=queue key, KEYS[2]=processing key  ARGV[1]=lease expiry (unix)
var dequeueAndLeaseScript = redis.NewScript(`
local val = redis.call('RPOP', KEYS[1])
if val == false then return nil end
redis.call('ZADD', KEYS[2], ARGV[1], val)
return {KEYS[1], val}
`)

const redisProcessingKey = "processing"

func (r *RedisBroker) Dequeue(queues []string, timeout time.Duration) (*payload.Job, string, error) {
	deadline := time.Now().Add(timeout)

	for {
		for _, queueName := range queues {
			queueKey := fmt.Sprintf("queue:%s", queueName)
			leaseExp := float64(time.Now().Add(5 * time.Minute).Unix())

			result, err := dequeueAndLeaseScript.Run(r.ctx, r.client,
				[]string{queueKey, redisProcessingKey},
				leaseExp,
			).StringSlice()

			if err == redis.Nil {
				continue
			}
			if err != nil {
				return nil, "", fmt.Errorf("failed to dequeue: %w", err)
			}
			if len(result) < 2 {
				continue
			}

			returnedQueueName := result[0][6:] // strip "queue:" prefix
			job, err := payload.FromJSON([]byte(result[1]))
			if err != nil {
				return nil, "", fmt.Errorf("failed to deserialize job: %w", err)
			}
			return job, returnedQueueName, nil
		}

		if time.Now().After(deadline) {
			return nil, "", nil
		}
		time.Sleep(100 * time.Millisecond)
	}
}

// Ack removes a job from the processing set after its outcome is handled.
func (r *RedisBroker) Ack(job *payload.Job) error {
	data, err := job.ToJSON()
	if err != nil {
		return fmt.Errorf("failed to serialize job for ack: %w", err)
	}
	return r.client.ZRem(r.ctx, redisProcessingKey, data).Err()
}

// Nack removes a job from the processing set and re-enqueues it.
func (r *RedisBroker) Nack(job *payload.Job) error {
	data, err := job.ToJSON()
	if err != nil {
		return fmt.Errorf("failed to serialize job for nack: %w", err)
	}
	pipe := r.client.Pipeline()
	pipe.ZRem(r.ctx, redisProcessingKey, data)
	queueKey := fmt.Sprintf("queue:%s", job.Queue)
	pipe.LPush(r.ctx, queueKey, data)
	_, err = pipe.Exec(r.ctx)
	return err
}

// ReapOrphanedJobs finds jobs in the processing set whose lease expired and
// removes them. The caller re-enqueues the returned jobs.
func (r *RedisBroker) ReapOrphanedJobs(lease time.Duration) ([]*payload.Job, error) {
	cutoff := float64(time.Now().Add(-lease).Unix())
	results, err := r.client.ZRangeByScore(r.ctx, redisProcessingKey, &redis.ZRangeBy{
		Min: "0", Max: fmt.Sprintf("%.0f", cutoff), Offset: 0, Count: 100,
	}).Result()
	if err != nil {
		return nil, fmt.Errorf("failed to get orphaned jobs: %w", err)
	}
	if len(results) == 0 {
		return nil, nil
	}

	var orphaned []*payload.Job
	for _, data := range results {
		if err := r.client.ZRem(r.ctx, redisProcessingKey, data).Err(); err != nil {
			continue
		}
		job, err := payload.FromJSON([]byte(data))
		if err != nil {
			continue
		}
		job.State = payload.JobStatePending
		orphaned = append(orphaned, job)
	}
	return orphaned, nil
}

func (r *RedisBroker) AddToRetry(job *payload.Job, retryAt time.Time) error {
	data, err := job.ToJSON()
	if err != nil {
		return fmt.Errorf("failed to serialize job: %w", err)
	}

	return r.client.ZAdd(r.ctx, "retry", &redis.Z{
		Score:  float64(retryAt.Unix()),
		Member: data,
	}).Err()
}

func (r *RedisBroker) GetRetryJobs(limit int64) ([]*payload.Job, error) {
	if limit <= 0 {
		limit = 1
	}
	if limit > 10000 {
		limit = 10000
	}
	now := float64(time.Now().Unix())
	result, err := r.client.ZRangeByScore(r.ctx, "retry", &redis.ZRangeBy{
		Min: "0", Max: fmt.Sprintf("%.0f", now), Offset: 0, Count: limit,
	}).Result()

	if err != nil {
		return nil, fmt.Errorf("failed to get retry jobs: %w", err)
	}

	jobs := make([]*payload.Job, 0, len(result))
	for _, data := range result {
		job, err := payload.FromJSON([]byte(data))
		if err != nil {
			continue
		}
		jobs = append(jobs, job)
	}

	return jobs, nil
}

func (r *RedisBroker) RemoveFromRetry(job *payload.Job) error {
	data, err := job.ToJSON()
	if err != nil {
		return err
	}
	return r.client.ZRem(r.ctx, "retry", data).Err()
}

func (r *RedisBroker) AddToDead(job *payload.Job) error {
	data, err := job.ToJSON()
	if err != nil {
		return fmt.Errorf("failed to serialize job: %w", err)
	}

	return r.client.ZAdd(r.ctx, "dead", &redis.Z{
		Score:  float64(time.Now().Unix()),
		Member: data,
	}).Err()
}

func (r *RedisBroker) GetDeadJobs(limit int64) ([]*payload.Job, error) {
	if limit <= 0 {
		limit = 1
	}
	if limit > 10000 {
		limit = 10000
	}
	result, err := r.client.ZRevRange(r.ctx, "dead", 0, limit-1).Result()
	if err != nil {
		return nil, fmt.Errorf("failed to get dead jobs: %w", err)
	}
	jobs := make([]*payload.Job, 0, len(result))
	for _, data := range result {
		job, err := payload.FromJSON([]byte(data))
		if err != nil {
			continue
		}
		jobs = append(jobs, job)
	}
	return jobs, nil
}

func (r *RedisBroker) GetQueueSize(queue string) (int64, error) {
	return r.client.LLen(r.ctx, "queue:"+queue).Result()
}

func (r *RedisBroker) DeleteKey(key string) error {
	if !strings.HasPrefix(key, "queue:") {
		return fmt.Errorf("DeleteKey restricted to queue: prefix, got %q", key)
	}
	return r.client.Del(r.ctx, key).Err()
}

func (r *RedisBroker) GetStats() (map[string]interface{}, error) {
	stats := make(map[string]interface{})

	processed, err := r.client.ZCard(r.ctx, "stat:processed").Result()
	if err != nil {
		return nil, fmt.Errorf("failed to get processed stats: %w", err)
	}
	stats["processed"] = processed

	retry, err := r.client.ZCard(r.ctx, "retry").Result()
	if err != nil {
		return nil, fmt.Errorf("failed to get retry stats: %w", err)
	}
	stats["retry"] = retry

	dead, err := r.client.ZCard(r.ctx, "dead").Result()
	if err != nil {
		return nil, fmt.Errorf("failed to get dead stats: %w", err)
	}
	stats["dead"] = dead

	queueSizes := make(map[string]int64)
	var cursor uint64
	for {
		var keys []string
		var err error
		keys, cursor, err = r.client.Scan(r.ctx, cursor, "queue:*", 100).Result()
		if err != nil {
			return nil, fmt.Errorf("failed to scan queue keys: %w", err)
		}
		for _, key := range keys {
			if len(key) > 6 {
				name := key[6:]
				size, err := r.client.LLen(r.ctx, key).Result()
				if err != nil {
					return nil, fmt.Errorf("failed to get queue size for %s: %w", name, err)
				}
				queueSizes[name] = size
			}
		}
		if cursor == 0 {
			break
		}
	}
	stats["queues"] = queueSizes

	return stats, nil
}
