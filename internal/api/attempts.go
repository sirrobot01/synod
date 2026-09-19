package api

import (
	"sync"
	"time"

	"golang.org/x/time/rate"
)

// attemptLimiter bounds sign-in attempts for each key and forgets idle keys.
// Sign-in is guessable, so one shared budget would let any caller spend it for everybody.
type attemptLimiter struct {
	mu      sync.Mutex
	limits  map[string]*rate.Limiter
	seen    map[string]time.Time
	rate    rate.Limit
	burst   int
	maxKeys int
}

func newAttemptLimiter(every rate.Limit, burst, maxKeys int) *attemptLimiter {
	return &attemptLimiter{limits: make(map[string]*rate.Limiter), seen: make(map[string]time.Time), rate: every, burst: burst, maxKeys: maxKeys}
}

// allow spends one attempt against every key and reports the strictest answer.
// It spends from each key even when an earlier one refuses, so a guesser cannot
// exhaust one account budget while leaving its own address untouched.
func (l *attemptLimiter) allow(keys ...string) bool {
	l.mu.Lock()
	defer l.mu.Unlock()
	now := time.Now()
	if len(l.limits) > l.maxKeys {
		// Forget idle keys first. A bounded map cannot grow without limit.
		for key, last := range l.seen {
			if now.Sub(last) > time.Hour {
				delete(l.limits, key)
				delete(l.seen, key)
			}
		}
		if len(l.limits) > l.maxKeys {
			clear(l.limits)
			clear(l.seen)
		}
	}
	allowed := true
	for _, key := range keys {
		if key == "" {
			continue
		}
		limiter, found := l.limits[key]
		if !found {
			limiter = rate.NewLimiter(l.rate, l.burst)
			l.limits[key] = limiter
		}
		l.seen[key] = now
		if !limiter.Allow() {
			allowed = false
		}
	}
	return allowed
}
