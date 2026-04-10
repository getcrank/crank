package broker

import (
	"strings"
	"testing"
)

func TestOpen_EmptyBrokerKind_ReturnsError(t *testing.T) {
	_, err := Open("", "redis://localhost:6379/0", ConnOptions{})
	if err == nil {
		t.Fatal("expected error for empty kind")
	}
	if !strings.Contains(err.Error(), "kind is required") {
		t.Errorf("error = %q, want mention of 'kind is required'", err)
	}
}

func TestOpen_EmptyURL_ReturnsError(t *testing.T) {
	_, err := Open("redis", "", ConnOptions{})
	if err == nil {
		t.Fatal("expected error for empty URL")
	}
	if !strings.Contains(err.Error(), "URL is empty") {
		t.Errorf("error = %q, want mention of 'URL is empty'", err)
	}
}

func TestOpen_Nats_ReturnsNotImplemented(t *testing.T) {
	_, err := Open("nats", "nats://localhost:4222", ConnOptions{})
	if err == nil {
		t.Fatal("expected error for nats")
	}
	if !strings.Contains(err.Error(), "NATS backend not yet implemented") {
		t.Errorf("error = %q, want NATS not implemented message", err)
	}
}

func TestOpen_Pgsql_ReturnsNotImplemented(t *testing.T) {
	_, err := Open("pgsql", "postgres://localhost:5432/crank", ConnOptions{})
	if err == nil {
		t.Fatal("expected error for pgsql")
	}
	if !strings.Contains(err.Error(), "PostgreSQL backend not yet implemented") {
		t.Errorf("error = %q, want PostgreSQL not implemented message", err)
	}
}

func TestOpen_UnsupportedKind_ReturnsError(t *testing.T) {
	_, err := Open("kafka", "kafka://localhost:9092", ConnOptions{})
	if err == nil {
		t.Fatal("expected error for unsupported broker")
	}
	if !strings.Contains(err.Error(), "unsupported broker") {
		t.Errorf("error = %q, want 'unsupported broker'", err)
	}
	if !strings.Contains(err.Error(), "kafka") {
		t.Errorf("error = %q, want mention of 'kafka'", err)
	}
}

func TestOpen_KindIsCaseInsensitive(t *testing.T) {
	// "PGSQL" should be normalized to "pgsql"
	_, err := Open("PGSQL", "postgres://localhost:5432/crank", ConnOptions{})
	if err == nil {
		t.Fatal("expected not-implemented error for PGSQL")
	}
	if !strings.Contains(err.Error(), "PostgreSQL backend not yet implemented") {
		t.Errorf("error = %q, want PostgreSQL not implemented (case-insensitive)", err)
	}
}

func TestOpen_KindWithWhitespace_IsTrimmed(t *testing.T) {
	_, err := Open("  nats  ", "nats://localhost:4222", ConnOptions{})
	if err == nil {
		t.Fatal("expected not-implemented error for trimmed nats")
	}
	if !strings.Contains(err.Error(), "NATS backend not yet implemented") {
		t.Errorf("error = %q, want NATS not implemented after trimming", err)
	}
}
