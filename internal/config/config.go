package config

import (
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"time"

	"gopkg.in/yaml.v3"
)

const (
	maxConcurrency = 10000
	maxTimeoutSec  = 86400 // 24 hours
)

type Logger interface {
	Debug(msg string, args ...any)
	Info(msg string, args ...any)
	Warn(msg string, args ...any)
	Error(msg string, args ...any)
}

// Config holds engine and broker configuration.
// Broker selection is via Broker (e.g. "redis", "nats", "pgsql"); the matching
// section (Redis, NATS, etc.) must be non-empty and provide URL and options.
type Config struct {
	Broker            string        `yaml:"broker"`     // "redis", "nats", "pgsql". Required — no default.
	BrokerURL         string        `yaml:"broker_url"` // optional fallback URL when backend's url is empty
	Concurrency       int           `yaml:"concurrency"`
	Queues            []QueueConfig `yaml:"queues"`
	Timeout           int           `yaml:"timeout"`
	Verbose           bool          `yaml:"verbose"`
	Redis             RedisConfig   `yaml:"redis"` // required when broker is "redis"
	NATS              NATSConfig    `yaml:"nats"`  // required when broker is "nats"
	Logger            Logger        `yaml:"-"`
	RetryPollInterval time.Duration `yaml:"-"` // optional; 0 means default 5s (used by tests)
	ReaperInterval    time.Duration `yaml:"-"` // optional; 0 means default 30s
}

// NATSConfig holds NATS connection options. Used when broker is "nats".
type NATSConfig struct {
	URL     string `yaml:"url"`
	Timeout int    `yaml:"timeout"` // connection timeout in seconds; 0 = default 5
}

func (c *NATSConfig) GetTimeout() time.Duration {
	if c.Timeout <= 0 {
		return 5 * time.Second
	}
	return time.Duration(c.Timeout) * time.Second
}

// QueueConfig: YAML accepts [name, weight] or {name, weight, priority}. Priority reserved.
type QueueConfig struct {
	Name     string `yaml:"name"`
	Weight   int    `yaml:"weight"`
	Priority int    `yaml:"priority"`
}

func (qc *QueueConfig) UnmarshalYAML(unmarshal func(interface{}) error) error {
	var arr []interface{}
	if err := unmarshal(&arr); err == nil && len(arr) >= 1 {
		if name, ok := arr[0].(string); ok {
			qc.Name = name
			if len(arr) >= 2 {
				if weight, ok := arr[1].(int); ok {
					qc.Weight = weight
				} else if weight, ok := arr[1].(float64); ok {
					qc.Weight = int(weight)
				}
			} else {
				qc.Weight = 1
			}
			return nil
		}
	}

	var m map[string]interface{}
	if err := unmarshal(&m); err != nil {
		return err
	}

	if name, ok := m["name"].(string); ok {
		qc.Name = name
	}
	if weight, ok := m["weight"].(int); ok {
		qc.Weight = weight
	} else if weight, ok := m["weight"].(float64); ok {
		qc.Weight = int(weight)
	}
	if priority, ok := m["priority"].(int); ok {
		qc.Priority = priority
	} else if priority, ok := m["priority"].(float64); ok {
		qc.Priority = int(priority)
	}
	if qc.Weight == 0 {
		qc.Weight = 1
	}
	return nil
}

type RedisConfig struct {
	URL                   string `yaml:"url"`
	NetworkTimeout        int    `yaml:"network_timeout"`
	UseTLS                bool   `yaml:"use_tls"`
	TLSInsecureSkipVerify bool   `yaml:"tls_insecure_skip_verify"`
}

func Load(path string) (*Config, error) {
	cleaned := filepath.Clean(path)
	if cleaned == ".." || strings.HasPrefix(cleaned, ".."+string(filepath.Separator)) {
		return nil, fmt.Errorf("failed to read config file: path must not be a traversal (..)")
	}
	// Resolve symlinks to prevent symlink-based traversal
	resolved, err := filepath.EvalSymlinks(cleaned)
	if err != nil {
		return nil, fmt.Errorf("failed to resolve config path: %w", err)
	}
	// Re-check the resolved path for traversal
	if !filepath.IsAbs(cleaned) {
		wd, wdErr := os.Getwd()
		if wdErr == nil {
			rel, relErr := filepath.Rel(wd, resolved)
			if relErr == nil && (rel == ".." || strings.HasPrefix(rel, ".."+string(filepath.Separator))) {
				return nil, fmt.Errorf("failed to read config file: resolved path escapes working directory")
			}
		}
	}
	data, err := os.ReadFile(resolved)
	if err != nil {
		return nil, fmt.Errorf("failed to read config file: %w", err)
	}

	var cfg Config
	if err := yaml.Unmarshal(data, &cfg); err != nil {
		return nil, fmt.Errorf("failed to parse config: %w", err)
	}

	// Normalize broker kind; no default — must be set explicitly
	cfg.Broker = strings.TrimSpace(strings.ToLower(cfg.Broker))
	if cfg.Broker == "" {
		return nil, fmt.Errorf("config: \"broker\" field is required (use redis, nats, or pgsql)")
	}

	switch cfg.Broker {
	case "redis":
		if cfg.Redis.URL == "" {
			cfg.Redis.URL = cfg.BrokerURL
		}
		if cfg.Redis.URL == "" {
			cfg.Redis.URL = os.Getenv("REDIS_URL")
		}
		if cfg.Redis.URL == "" {
			return nil, fmt.Errorf("config: broker is %q but redis.url (or broker_url / REDIS_URL) is empty", cfg.Broker)
		}
		if cfg.Redis.NetworkTimeout == 0 {
			cfg.Redis.NetworkTimeout = 5
		}
	case "nats":
		if cfg.NATS.URL == "" {
			cfg.NATS.URL = cfg.BrokerURL
		}
		if cfg.NATS.URL == "" {
			cfg.NATS.URL = os.Getenv("NATS_URL")
		}
		if cfg.NATS.URL == "" {
			return nil, fmt.Errorf("config: broker is %q but nats.url (or broker_url / NATS_URL) is empty", cfg.Broker)
		}
		if cfg.NATS.Timeout == 0 {
			cfg.NATS.Timeout = 5
		}
	case "pgsql":
		return nil, fmt.Errorf("config: broker %q is not yet implemented", cfg.Broker)
	default:
		return nil, fmt.Errorf("config: unknown broker %q (use redis, nats, or pgsql)", cfg.Broker)
	}

	if cfg.Concurrency <= 0 {
		cfg.Concurrency = 10
	}
	if cfg.Concurrency > maxConcurrency {
		cfg.Concurrency = maxConcurrency
	}
	if cfg.Timeout <= 0 {
		cfg.Timeout = 8
	}
	if cfg.Timeout > maxTimeoutSec {
		cfg.Timeout = maxTimeoutSec
	}

	return &cfg, nil
}

func (c *Config) GetTimeout() time.Duration {
	return time.Duration(c.Timeout) * time.Second
}

func (c *RedisConfig) GetNetworkTimeout() time.Duration {
	return time.Duration(c.NetworkTimeout) * time.Second
}
