package payload

import (
	"testing"
)

func TestNewJob(t *testing.T) {
	j := NewJob("EmailWorker", "default", 123, "foo")
	if j.Class != "EmailWorker" {
		t.Errorf("Class = %q, want EmailWorker", j.Class)
	}
	if j.Queue != "default" {
		t.Errorf("Queue = %q, want default", j.Queue)
	}
	if j.Retry != 5 {
		t.Errorf("Retry = %d, want 5", j.Retry)
	}
	if j.RetryCount != 0 {
		t.Errorf("RetryCount = %d, want 0", j.RetryCount)
	}
	if j.JID == "" {
		t.Error("JID is empty")
	}
	if len(j.Args) != 2 {
		t.Errorf("len(Args) = %d, want 2", len(j.Args))
	}
	if j.State != JobStatePending {
		t.Errorf("State = %q, want %q", j.State, JobStatePending)
	}
}

func TestJob_SetRetry_SetBacktrace(t *testing.T) {
	j := NewJob("W", "q")
	j.SetRetry(3).SetBacktrace(true)
	if j.Retry != 3 {
		t.Errorf("Retry = %d, want 3", j.Retry)
	}
	if !j.Backtrace {
		t.Error("Backtrace = false, want true")
	}
}

func TestJob_ToJSON_FromJSON_RoundTrip(t *testing.T) {
	j := NewJob("Worker", "default", 1, "two")
	data, err := j.ToJSON()
	if err != nil {
		t.Fatalf("ToJSON: %v", err)
	}
	j2, err := FromJSON(data)
	if err != nil {
		t.Fatalf("FromJSON: %v", err)
	}
	if j2.Class != j.Class {
		t.Errorf("Class = %q, want %q", j2.Class, j.Class)
	}
	if j2.Queue != j.Queue {
		t.Errorf("Queue = %q, want %q", j2.Queue, j.Queue)
	}
	if j2.JID != j.JID {
		t.Errorf("JID = %q, want %q", j2.JID, j.JID)
	}
	if len(j2.Args) != 2 {
		t.Errorf("len(Args) = %d, want 2", len(j2.Args))
	}
	if j2.State != JobStatePending {
		t.Errorf("State = %q, want %q", j2.State, JobStatePending)
	}
}

func TestFromJSON_Invalid(t *testing.T) {
	_, err := FromJSON([]byte("not json"))
	if err == nil {
		t.Error("expected error for invalid JSON")
	}
}

func TestFromJSON_ValidMinimal(t *testing.T) {
	data := []byte(`{"jid":"id1","class":"W","args":[1,2],"queue":"q","retry":5,"retry_count":0,"created_at":0,"enqueued_at":0,"backtrace":false}`)
	j, err := FromJSON(data)
	if err != nil {
		t.Fatalf("FromJSON: %v", err)
	}
	if j.JID != "id1" {
		t.Errorf("JID = %q, want id1", j.JID)
	}
	if j.Class != "W" {
		t.Errorf("Class = %q, want W", j.Class)
	}
	if j.Queue != "q" {
		t.Errorf("Queue = %q, want q", j.Queue)
	}
	if len(j.Args) != 2 {
		t.Errorf("len(Args) = %d, want 2", len(j.Args))
	}
	if j.State != JobStatePending {
		t.Errorf("State = %q, want %q", j.State, JobStatePending)
	}
}

func TestJob_String(t *testing.T) {
	j := NewJob("W", "default")
	s := j.String()
	if s == "" {
		t.Error("String() is empty")
	}
	if len(s) < 10 {
		t.Errorf("String() too short: %q", s)
	}
}
