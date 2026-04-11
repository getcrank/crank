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

func (b *CircuitBreaker) getOrCreate(class string) *classState {
	s, ok := b.classes[class]
	if !ok {
		// Evict closed entries if map is at capacity
		if len(b.classes) >= maxBreakerClasses {
			for k, v := range b.classes {
				if v.state == stateClosed && len(v.failureTimes) == 0 {
					delete(b.classes, k)
					break
				}
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
