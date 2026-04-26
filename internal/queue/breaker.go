package queue

import (
	"sync"
	"time"
)

type breakerState int

const (
	stateClosed breakerState = iota
	stateOpen
	stateHalfOpen
)

type classState struct {
	state             breakerState
	failureTimes      []time.Time
	openUntil         time.Time
	halfOpenProbeUsed bool
}

// CircuitBreaker tracks failures per job class and opens after threshold within window.
// Thread-safe for use across concurrent workers.
const maxBreakerClasses = 10000

type CircuitBreaker struct {
	mu               sync.RWMutex
	failureThreshold int
	window           time.Duration
	resetTimeout     time.Duration
	classes          map[string]*classState
}

// BreakerConfig holds circuit breaker parameters.
type BreakerConfig struct {
	FailureThreshold int           // failures within Window to open
	Window           time.Duration // sliding window for counting failures
	ResetTimeout     time.Duration // how long to stay Open before HalfOpen (e.g. 60s)
}

// NewCircuitBreaker creates a circuit breaker with the given config.
func NewCircuitBreaker(cfg BreakerConfig) *CircuitBreaker {
	if cfg.FailureThreshold <= 0 {
		cfg.FailureThreshold = 5
	}
	if cfg.Window <= 0 {
		cfg.Window = time.Minute
	}
	if cfg.ResetTimeout <= 0 {
		cfg.ResetTimeout = 60 * time.Second
	}
	return &CircuitBreaker{
		failureThreshold: cfg.FailureThreshold,
		window:           cfg.Window,
		resetTimeout:     cfg.ResetTimeout,
		classes:          make(map[string]*classState),
	}
}

// sentinelDenied is returned when the breaker map is at capacity and no entry
// could be evicted. It behaves as an open circuit that denies all requests.
var sentinelDenied = &classState{state: stateOpen, openUntil: time.Date(9999, 1, 1, 0, 0, 0, 0, time.UTC)}

func (b *CircuitBreaker) getOrCreate(class string) *classState {
	s, ok := b.classes[class]
	if !ok {
		// Evict entries if map is at capacity.
		// Priority: closed+clean → closed+dirty → oldest open.
		if len(b.classes) >= maxBreakerClasses {
			evicted := false
			// First pass: evict a closed entry with no failures (cheapest)
			for k, v := range b.classes {
				if v.state == stateClosed && len(v.failureTimes) == 0 {
					delete(b.classes, k)
					evicted = true
					break
				}
			}
			// Second pass: evict any closed entry
			if !evicted {
				for k, v := range b.classes {
					if v.state == stateClosed {
						delete(b.classes, k)
						evicted = true
						break
					}
				}
			}
			// Third pass: evict the open entry with the earliest openUntil
			if !evicted {
				var oldestKey string
				var oldestTime time.Time
				first := true
				for k, v := range b.classes {
					if first || v.openUntil.Before(oldestTime) {
						oldestKey = k
						oldestTime = v.openUntil
						first = false
					}
				}
				if oldestKey != "" {
					delete(b.classes, oldestKey)
					evicted = true
				}
			}
			// Hard cap: refuse to create if eviction still failed
			if !evicted {
				return sentinelDenied
			}
		}
		s = &classState{state: stateClosed, failureTimes: make([]time.Time, 0)}
		b.classes[class] = s
	}
	return s
}

func (s *classState) trimWindow(now time.Time, window time.Duration) {
	cutoff := now.Add(-window)
	i := 0
	for i < len(s.failureTimes) && s.failureTimes[i].Before(cutoff) {
		i++
	}
	if i > 0 {
		s.failureTimes = append(s.failureTimes[:0], s.failureTimes[i:]...)
	}
}

// Allow reports whether a job of the given class may be processed.
// When Open, returns false. When HalfOpen, returns true once (the probe), then false.
func (b *CircuitBreaker) Allow(class string) bool {
	b.mu.Lock()
	defer b.mu.Unlock()

	s := b.getOrCreate(class)
	now := time.Now()

	if s.state == stateOpen {
		if now.Before(s.openUntil) {
			return false
		}
		s.state = stateHalfOpen
		s.halfOpenProbeUsed = false
	}

	if s.state == stateHalfOpen {
		if s.halfOpenProbeUsed {
			return false
		}
		s.halfOpenProbeUsed = true
		return true
	}

	return true
}

// RecordSuccess notifies the breaker that a job of the given class succeeded.
func (b *CircuitBreaker) RecordSuccess(class string) {
	b.mu.Lock()
	defer b.mu.Unlock()

	s := b.getOrCreate(class)
	if s == sentinelDenied {
		return
	}
	if s.state == stateHalfOpen {
		s.state = stateClosed
		s.failureTimes = s.failureTimes[:0]
		s.halfOpenProbeUsed = false
		return
	}
	if s.state == stateClosed {
		s.failureTimes = s.failureTimes[:0]
	}
}

// RecordFailure notifies the breaker that a job of the given class failed.
func (b *CircuitBreaker) RecordFailure(class string) {
	b.mu.Lock()
	defer b.mu.Unlock()

	s := b.getOrCreate(class)
	if s == sentinelDenied {
		return
	}
	now := time.Now()

	if s.state == stateHalfOpen {
		s.state = stateOpen
		s.openUntil = now.Add(b.resetTimeout)
		s.halfOpenProbeUsed = false
		return
	}

	if s.state == stateClosed {
		s.trimWindow(now, b.window)
		s.failureTimes = append(s.failureTimes, now)
		// Cap slice length to prevent unbounded growth within the window
		if len(s.failureTimes) > b.failureThreshold*2 {
			s.failureTimes = s.failureTimes[len(s.failureTimes)-b.failureThreshold:]
		}
		if len(s.failureTimes) >= b.failureThreshold {
			s.state = stateOpen
			s.openUntil = now.Add(b.resetTimeout)
			s.failureTimes = s.failureTimes[:0]
		}
	}
}

// IsOpen returns true if the circuit for the given class is open (and not yet in cool-down).
func (b *CircuitBreaker) IsOpen(class string) bool {
	b.mu.RLock()
	defer b.mu.RUnlock()

	s, ok := b.classes[class]
	if !ok {
		return false
	}
	if s.state != stateOpen {
		return false
	}
	return time.Now().Before(s.openUntil)
}
