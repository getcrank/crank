package queue

import (
	"fmt"

	"github.com/ogwurujohnson/crank/internal/broker"
)

type Queue struct {
	name   string
	broker broker.Broker
}

func NewQueue(name string, b broker.Broker) *Queue {
	return &Queue{
		name:   name,
		broker: b,
	}
}

func (q *Queue) Size() (int64, error) {
	return q.broker.GetQueueSize(q.name)
}

func (q *Queue) Name() string {
	return q.name
}

func (q *Queue) Clear() error {
	return q.broker.DeleteKey("queue:" + q.name)
}

type Stats struct {
	Processed int64            `json:"processed"`
	Failed    int64            `json:"failed"`
	Retry     int64            `json:"retry"`
	Dead      int64            `json:"dead"`
	Queues    map[string]int64 `json:"queues"`
}

func getInt64(m map[string]interface{}, key string) (int64, error) {
	value, ok := m[key]
	if !ok || value == nil {
		return 0, fmt.Errorf("stats: missing or nil %q", key)
	}
	switch numericValue := value.(type) {
	case int64:
		return numericValue, nil
	case int:
		return int64(numericValue), nil
	case float64:
		return int64(numericValue), nil
	default:
		return 0, fmt.Errorf("stats: %q has invalid type %T", key, value)
	}
}

func getQueuesMap(m map[string]interface{}, key string) (map[string]int64, error) {
	value, ok := m[key]
	if !ok || value == nil {
		return nil, fmt.Errorf("stats: missing or nil %q", key)
	}
	if queueMap, ok := value.(map[string]int64); ok {
		out := make(map[string]int64, len(queueMap))
		for queueName, queueSize := range queueMap {
			out[queueName] = queueSize
		}
		return out, nil
	}
	rawMap, ok := value.(map[string]interface{})
	if !ok {
		return nil, fmt.Errorf("stats: %q is not a map", key)
	}
	out := make(map[string]int64, len(rawMap))
	for queueName, val := range rawMap {
		if val == nil {
			out[queueName] = 0
			continue
		}
		switch numericValue := val.(type) {
		case int64:
			out[queueName] = numericValue
		case int:
			out[queueName] = int64(numericValue)
		case float64:
			out[queueName] = int64(numericValue)
		default:
			return nil, fmt.Errorf("stats: %q[%s] has invalid type %T", key, queueName, val)
		}
	}
	return out, nil
}

func GetStats(b broker.Broker) (*Stats, error) {
	statsMap, err := b.GetStats()
	if err != nil {
		return nil, err
	}

	processed, err := getInt64(statsMap, "processed")
	if err != nil {
		return nil, err
	}
	failed, err := getInt64(statsMap, "failed")
	if err != nil {
		return nil, err
	}
	retry, err := getInt64(statsMap, "retry")
	if err != nil {
		return nil, err
	}
	dead, err := getInt64(statsMap, "dead")
	if err != nil {
		return nil, err
	}
	queues, err := getQueuesMap(statsMap, "queues")
	if err != nil {
		return nil, err
	}

	return &Stats{
		Processed: processed,
		Failed:    failed,
		Retry:     retry,
		Dead:      dead,
		Queues:    queues,
	}, nil
}
