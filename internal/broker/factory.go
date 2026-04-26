package broker

import (
	"fmt"
	"strings"
)

// Open creates a Broker from a backend kind, URL, and connection options.
// Kind must be set explicitly (e.g. "redis", "nats", "pgsql"); there is no inference from URL.
//
//   - kind "redis" → Redis broker; opts must include Timeout, UseTLS, TLSInsecureSkipVerify.
//   - kind "nats"  → NATS broker (not yet implemented).
//   - kind "pgsql" → PostgreSQL broker (not yet implemented).
func Open(kind string, rawURL string, opts ConnOptions) (Broker, error) {
	rawURL = strings.TrimSpace(rawURL)
	if rawURL == "" {
		return nil, fmt.Errorf("broker: URL is empty")
	}

	kind = strings.TrimSpace(strings.ToLower(kind))
	if kind == "" {
		return nil, fmt.Errorf("broker: kind is required; use \"redis\", \"nats\", or \"pgsql\"")
	}

	switch kind {
	case "redis":
		return openRedis(rawURL, opts)
	case "nats":
		return openNats(rawURL, opts)
	case "pgsql":
		return nil, fmt.Errorf("broker: PostgreSQL backend not yet implemented")
	default:
		return nil, fmt.Errorf("broker: unsupported broker %q (use redis, nats, or pgsql)", kind)
	}
}

func openNats(rawURL string, opts ConnOptions) (Broker, error) {
	return nil, fmt.Errorf("broker: NATS backend not yet implemented")
}

func openRedis(rawURL string, opts ConnOptions) (Broker, error) {
	return NewRedisBrokerWithConfig(RedisBrokerConfig{
		URL:                   rawURL,
		Timeout:               opts.Timeout,
		UseTLS:                opts.UseTLS,
		TLSInsecureSkipVerify: opts.TLSInsecureSkipVerify,
		Logger:                opts.Logger,
	})
}
