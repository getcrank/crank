package config

import (
	"os"
	"strings"
	"testing"
)

func writeTestConfig(t *testing.T, content string) string {
	t.Helper()
	dir := t.TempDir()
	path := dir + "/crank_test.yml"
	if err := os.WriteFile(path, []byte(content), 0644); err != nil {
		t.Fatalf("write test config: %v", err)
	}
	return path
}

func TestLoad_MissingBrokerField_ReturnsError(t *testing.T) {
	path := writeTestConfig(t, `
concurrency: 5
timeout: 8
`)
	_, err := Load(path)
	if err == nil {
		t.Fatal("expected error when broker field is missing")
	}
	if !strings.Contains(err.Error(), "\"broker\" field is required") {
		t.Errorf("error = %q, want mention of broker field required", err)
	}
}

func TestLoad_EmptyBrokerField_ReturnsError(t *testing.T) {
	path := writeTestConfig(t, `
broker: ""
concurrency: 5
`)
	_, err := Load(path)
	if err == nil {
		t.Fatal("expected error when broker is empty string")
	}
	if !strings.Contains(err.Error(), "\"broker\" field is required") {
		t.Errorf("error = %q, want mention of broker field required", err)
	}
}

func TestLoad_BrokerPgsql_ReturnsNotImplemented(t *testing.T) {
	path := writeTestConfig(t, `
broker: pgsql
concurrency: 5
`)
	_, err := Load(path)
	if err == nil {
		t.Fatal("expected error for pgsql broker")
	}
	if !strings.Contains(err.Error(), "not yet implemented") {
		t.Errorf("error = %q, want 'not yet implemented'", err)
	}
}

func TestLoad_UnknownBroker_ReturnsError(t *testing.T) {
	path := writeTestConfig(t, `
broker: kafka
concurrency: 5
`)
	_, err := Load(path)
	if err == nil {
		t.Fatal("expected error for unknown broker")
	}
	if !strings.Contains(err.Error(), "unknown broker") {
		t.Errorf("error = %q, want 'unknown broker'", err)
	}
	if !strings.Contains(err.Error(), "kafka") {
		t.Errorf("error = %q, want mention of 'kafka'", err)
	}
}

func TestLoad_RedisNoURL_ReturnsError(t *testing.T) {
	t.Setenv("REDIS_URL", "")
	path := writeTestConfig(t, `
broker: redis
`)
	_, err := Load(path)
	if err == nil {
		t.Fatal("expected error when redis URL is empty")
	}
	if !strings.Contains(err.Error(), "redis.url") {
		t.Errorf("error = %q, want mention of redis.url", err)
	}
}

func TestLoad_RedisWithURL_Succeeds(t *testing.T) {
	path := writeTestConfig(t, `
broker: redis
redis:
  url: redis://localhost:6379/0
`)
	cfg, err := Load(path)
	if err != nil {
		t.Fatalf("Load: %v", err)
	}
	if cfg.Broker != "redis" {
		t.Errorf("Broker = %q, want redis", cfg.Broker)
	}
	if cfg.Redis.URL != "redis://localhost:6379/0" {
		t.Errorf("Redis.URL = %q, want redis://localhost:6379/0", cfg.Redis.URL)
	}
}

func TestLoad_RedisWithEnvFallback_Succeeds(t *testing.T) {
	t.Setenv("REDIS_URL", "redis://envhost:6379/1")
	path := writeTestConfig(t, `
broker: redis
`)
	cfg, err := Load(path)
	if err != nil {
		t.Fatalf("Load: %v", err)
	}
	if cfg.Redis.URL != "redis://envhost:6379/1" {
		t.Errorf("Redis.URL = %q, want redis://envhost:6379/1", cfg.Redis.URL)
	}
}

func TestLoad_NatsNoURL_ReturnsError(t *testing.T) {
	t.Setenv("NATS_URL", "")
	path := writeTestConfig(t, `
broker: nats
`)
	_, err := Load(path)
	if err == nil {
		t.Fatal("expected error when nats URL is empty")
	}
	if !strings.Contains(err.Error(), "nats.url") {
		t.Errorf("error = %q, want mention of nats.url", err)
	}
}

func TestLoad_BrokerIsCaseInsensitive(t *testing.T) {
	path := writeTestConfig(t, `
broker: PGSQL
`)
	_, err := Load(path)
	if err == nil {
		t.Fatal("expected error for PGSQL (should be normalized)")
	}
	if !strings.Contains(err.Error(), "not yet implemented") {
		t.Errorf("error = %q, want 'not yet implemented' for normalized pgsql", err)
	}
}

func TestLoad_RedisDefaultNetworkTimeout(t *testing.T) {
	path := writeTestConfig(t, `
broker: redis
redis:
  url: redis://localhost:6379/0
`)
	cfg, err := Load(path)
	if err != nil {
		t.Fatalf("Load: %v", err)
	}
	if cfg.Redis.NetworkTimeout != 5 {
		t.Errorf("Redis.NetworkTimeout = %d, want 5 (default)", cfg.Redis.NetworkTimeout)
	}
}
