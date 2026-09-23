package engine_test

import (
	"testing"
	"time"

	"github.com/sirrobot01/synod/internal/engine"
)

// Specification 06 forbids reporting healthy when a required probe was unavailable.
// An unavailable probe is not a pass, and it is the zero value so that a probe nobody
// filled in can never be mistaken for one.
func TestUnavailableIsNeverHealthy(t *testing.T) {
	passing := engine.Probe{State: engine.ProbePassed}
	full := engine.Observation{
		Reachable: passing, ProcessLive: passing, Ready: passing, Endpoint: passing,
		ObservedRevision: 1, ObservedAt: time.Now(),
	}
	if !full.Healthy() {
		t.Fatal("an engine whose every probe passed was not healthy")
	}
	// The zero value of a probe is unavailable, so an observation nobody filled in
	// is not healthy.
	if (engine.Observation{ObservedAt: time.Now()}).Healthy() {
		t.Fatal("an observation with no probes was healthy")
	}
	for name, spoil := range map[string]func(*engine.Observation){
		"reachability unavailable": func(o *engine.Observation) { o.Reachable = engine.Probe{} },
		"liveness unavailable":     func(o *engine.Observation) { o.ProcessLive = engine.Probe{} },
		"readiness unavailable":    func(o *engine.Observation) { o.Ready = engine.Probe{} },
		"endpoint unavailable":     func(o *engine.Observation) { o.Endpoint = engine.Probe{} },
		"readiness failed":         func(o *engine.Observation) { o.Ready = engine.Probe{State: engine.ProbeFailed} },
		// Freshness is part of the answer. An observation with no time on it cannot
		// be told from one taken an hour ago.
		"no freshness": func(o *engine.Observation) { o.ObservedAt = time.Time{} },
	} {
		t.Run(name, func(t *testing.T) {
			observed := full
			spoil(&observed)
			if observed.Healthy() {
				t.Fatalf("an observation with %s was reported healthy", name)
			}
		})
	}
}
