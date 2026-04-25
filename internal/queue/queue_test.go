package queue

import (
	"context"
	"errors"
	"reflect"
	"strings"
	"testing"
	"time"

	"github.com/ogwurujohnson/crank/internal/broker"
	"github.com/ogwurujohnson/crank/internal/payload"
)

type mockBroker struct {
	stats map[string]interface{}
	err   error
}

func (m *mockBroker) GetStats() (map[string]interface{}, error) {
	if m.err != nil {
		return nil, m.err
	}
	return m.stats, nil
}

func (m *mockBroker) Enqueue(context.Context, string, *payload.Job) error { return nil }
func (m *mockBroker) Dequeue([]string, time.Duration) (*payload.Job, string, error) {
	return nil, "", nil
}
func (m *mockBroker) Ack(*payload.Job) error                                 { return nil }
func (m *mockBroker) ReapOrphanedJobs(time.Duration) ([]*payload.Job, error) { return nil, nil }
func (m *mockBroker) AddToRetry(*payload.Job, time.Time) error               { return nil }
func (m *mockBroker) GetRetryJobs(int64) ([]*payload.Job, error)             { return nil, nil }
func (m *mockBroker) RemoveFromRetry(*payload.Job) error                     { return nil }
func (m *mockBroker) AddToDead(*payload.Job) error                           { return nil }
func (m *mockBroker) GetQueueSize(string) (int64, error)                     { return 0, nil }
func (m *mockBroker) DeleteKey(string) error                                 { return nil }
func (m *mockBroker) RecordSuccess(*payload.Job) error                       { return nil }
func (m *mockBroker) RecordFailure(*payload.Job) error                       { return nil }
func (m *mockBroker) Close() error                                           { return nil }

var _ broker.Broker = (*mockBroker)(nil)

func TestGetStats_Valid(t *testing.T) {
	b := &mockBroker{
		stats: map[string]interface{}{
			"processed": int64(10),
			"failed":    int64(3),
			"retry":     int64(1),
			"dead":      int64(0),
			"queues":    map[string]int64{"default": 5, "low": 2},
		},
	}
	stats, err := GetStats(b)
	if err != nil {
		t.Fatalf("GetStats: %v", err)
	}
	want := &Stats{
		Processed: 10,
		Failed:    3,
		Retry:     1,
		Dead:      0,
		Queues:    map[string]int64{"default": 5, "low": 2},
	}
	if !reflect.DeepEqual(stats, want) {
		t.Errorf("GetStats() = %+v, want %+v", stats, want)
	}
}

func TestGetStats_IntAndFloat64(t *testing.T) {
	b := &mockBroker{
		stats: map[string]interface{}{
			"processed": float64(100),
			"failed":    float64(5),
			"retry":     int(2),
			"dead":      float64(1),
			"queues": map[string]interface{}{
				"default": float64(3),
				"low":     int(1),
			},
		},
	}
	stats, err := GetStats(b)
	if err != nil {
		t.Fatalf("GetStats: %v", err)
	}
	want := &Stats{
		Processed: 100,
		Failed:    5,
		Retry:     2,
		Dead:      1,
		Queues:    map[string]int64{"default": 3, "low": 1},
	}
	if !reflect.DeepEqual(stats, want) {
		t.Errorf("GetStats() = %+v, want %+v", stats, want)
	}
}

func TestGetStats_BrokerError(t *testing.T) {
	b := &mockBroker{err: errors.New("broker down")}
	_, err := GetStats(b)
	if err == nil {
		t.Fatal("GetStats: expected error")
	}
	if !strings.Contains(err.Error(), "broker down") {
		t.Errorf("err = %q, want substring %q", err.Error(), "broker down")
	}
}

func TestGetStats_MissingKey(t *testing.T) {
	b := &mockBroker{
		stats: map[string]interface{}{
			"processed": int64(1),
		},
	}
	_, err := GetStats(b)
	if err == nil {
		t.Fatal("GetStats: expected error")
	}
	if !strings.Contains(err.Error(), `stats: missing or nil "failed"`) {
		t.Errorf("err = %q, want substring %q", err.Error(), `stats: missing or nil "failed"`)
	}
}

func TestGetStats_InvalidType(t *testing.T) {
	b := &mockBroker{
		stats: map[string]interface{}{
			"processed": int64(1),
			"failed":    int64(0),
			"retry":     int64(1),
			"dead":      int64(1),
			"queues":    "not a map",
		},
	}
	_, err := GetStats(b)
	if err == nil {
		t.Fatal("GetStats: expected error")
	}
	if !strings.Contains(err.Error(), `stats: "queues" is not a map`) {
		t.Errorf("err = %q, want substring %q", err.Error(), `stats: "queues" is not a map`)
	}
}

func TestGetStats_NilValue(t *testing.T) {
	b := &mockBroker{
		stats: map[string]interface{}{
			"processed": nil,
			"retry":     int64(0),
			"dead":      int64(0),
			"queues":    map[string]int64{},
		},
	}
	_, err := GetStats(b)
	if err == nil {
		t.Fatal("GetStats: expected error")
	}
	if !strings.Contains(err.Error(), `stats: missing or nil "processed"`) {
		t.Errorf("err = %q, want substring %q", err.Error(), `stats: missing or nil "processed"`)
	}
}
