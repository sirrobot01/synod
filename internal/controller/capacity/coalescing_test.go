package capacity

import (
	"context"
	"errors"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/sirrobot01/synod/internal/controlstream"
	"github.com/sirrobot01/synod/internal/placement"
	agentv1 "github.com/sirrobot01/synod/proto/agent/v1"
)

type delayedProbeStore struct {
	*fakeStore
	oldSession uuid.UUID
	entered    chan struct{}
	release    <-chan struct{}
	failure    error
	contexts   chan<- context.Context
}

func (s *delayedProbeStore) BeginCapacityProbe(ctx context.Context, installationID, nodeID, sessionID uuid.UUID) (placement.CapacityProbe, error) {
	if s.contexts != nil {
		s.contexts <- ctx
	}
	probe, err := s.fakeStore.BeginCapacityProbe(ctx, installationID, nodeID, sessionID)
	if sessionID != s.oldSession {
		return probe, err
	}
	probe.ExpiresAt = fixedNow.Add(time.Second)
	if s.entered != nil {
		close(s.entered)
		select {
		case <-ctx.Done():
			return placement.CapacityProbe{}, ctx.Err()
		case <-s.release:
		}
	}
	return probe, s.failure
}

type delayedSessionTransport struct {
	*fakeTransport
	first   atomic.Bool
	entered chan struct{}
	release <-chan struct{}
}

func (s *delayedSessionTransport) Session(nodeID uuid.UUID) (controlstream.Peer, bool) {
	peer, ok := s.fakeTransport.Session(nodeID)
	if s.first.CompareAndSwap(false, true) {
		close(s.entered)
		<-s.release
	}
	return peer, ok
}

type delayedProbeTransport struct {
	*fakeTransport
	oldSession string
	entered    chan struct{}
	release    <-chan struct{}
	failure    error
}

func (s *delayedProbeTransport) Send(nodeID uuid.UUID, sessionID string, message *agentv1.ControllerMessage) error {
	if sessionID == s.oldSession && s.entered != nil {
		close(s.entered)
		<-s.release
		if s.failure != nil {
			return s.failure
		}
	}
	return s.fakeTransport.Send(nodeID, sessionID, message)
}

func TestLateRefreshCannotChangeReconnectedSessionClaim(t *testing.T) {
	for _, fault := range []string{"begin failure", "send failure", "send completion"} {
		t.Run(fault, func(t *testing.T) {
			n := node(t)
			oldSnapshot := placement.Snapshot{Nodes: []placement.Node{n}}
			entered, release := make(chan struct{}), make(chan struct{})
			unblock := sync.OnceFunc(func() { close(release) })
			t.Cleanup(unblock)
			store := &delayedProbeStore{fakeStore: &fakeStore{}, oldSession: n.SessionID}
			transport := &delayedProbeTransport{fakeTransport: &fakeTransport{}, oldSession: n.SessionID.String()}
			session(transport.fakeTransport, n)
			if fault == "begin failure" {
				store.entered, store.release, store.failure = entered, release, errors.New("old session lost")
			} else {
				transport.entered, transport.release = entered, release
				if fault == "send failure" {
					transport.failure = errors.New("old stream closed")
				}
			}
			var elapsed atomic.Int64
			d, err := New(Config{Store: store, Transport: transport, Wait: time.Second, Now: func() time.Time { return fixedNow.Add(time.Duration(elapsed.Load())) }})
			if err != nil {
				t.Fatal(err)
			}
			done := make(chan error, 1)
			go func() { _, err := d.Refresh(t.Context(), n.InstallationID, oldSnapshot); done <- err }()
			select {
			case <-entered:
			case <-time.After(3 * time.Second):
				t.Fatal("old refresh did not reach the blocked boundary")
			}
			n.SessionID = uuid.New()
			transport.mu.Lock()
			session(transport.fakeTransport, n)
			transport.mu.Unlock()
			current := placement.Snapshot{Nodes: []placement.Node{n}}
			challenged, err := d.Refresh(t.Context(), n.InstallationID, current)
			if err != nil || len(challenged) != 1 || challenged[0] != n.ID {
				t.Fatalf("new session was not challenged: %v %v", challenged, err)
			}
			transport.mu.Lock()
			if len(transport.sent) != 1 || transport.sent[0].SessionId != n.SessionID.String() {
				t.Error("new session did not receive its probe")
			}
			transport.mu.Unlock()
			unblock()
			select {
			case err := <-done:
				if err != nil {
					t.Fatal(err)
				}
			case <-time.After(3 * time.Second):
				t.Fatal("old refresh did not complete")
			}
			// The old probe has expired. The newer probe still has time to answer.
			elapsed.Store(int64(2 * time.Second))
			challenged, err = d.Refresh(t.Context(), n.InstallationID, current)
			if err != nil || len(challenged) != 1 || challenged[0] != n.ID {
				t.Fatalf("later planner did not share the new probe: %v %v", challenged, err)
			}
			store.mu.Lock()
			defer store.mu.Unlock()
			if len(store.probes) != 2 {
				t.Fatalf("old refresh replaced the new claim: %d probes, want 2", len(store.probes))
			}
		})
	}
}

func TestPendingProbeDoesNotExpireBeforeSendCompletes(t *testing.T) {
	n := node(t)
	store := &fakeStore{}
	entered, release := make(chan struct{}), make(chan struct{})
	unblock := sync.OnceFunc(func() { close(release) })
	t.Cleanup(unblock)
	transport := &delayedProbeTransport{fakeTransport: &fakeTransport{}, oldSession: n.SessionID.String(), entered: entered, release: release}
	session(transport.fakeTransport, n)
	var elapsed atomic.Int64
	d, err := New(Config{Store: store, Transport: transport, Wait: time.Second, Now: func() time.Time { return fixedNow.Add(time.Duration(elapsed.Load())) }})
	if err != nil {
		t.Fatal(err)
	}
	snapshot := placement.Snapshot{Nodes: []placement.Node{n}}
	done := make(chan error, 1)
	go func() { _, err := d.Refresh(t.Context(), n.InstallationID, snapshot); done <- err }()
	select {
	case <-entered:
	case <-time.After(3 * time.Second):
		t.Fatal("refresh did not reach send")
	}
	elapsed.Store(int64(placement.ProbeWindow + time.Second))
	challenged, err := d.Refresh(t.Context(), n.InstallationID, snapshot)
	if err != nil || len(challenged) != 1 || challenged[0] != n.ID {
		t.Fatalf("planner did not share pending send: %v %v", challenged, err)
	}
	unblock()
	select {
	case err := <-done:
		if err != nil {
			t.Fatal(err)
		}
	case <-time.After(3 * time.Second):
		t.Fatal("refresh did not complete after send")
	}
	store.mu.Lock()
	defer store.mu.Unlock()
	if len(store.probes) != 1 {
		t.Fatalf("pending send spent %d probe revisions, want 1", len(store.probes))
	}
}

func TestRefreshRechecksSessionBeforeReplacingClaim(t *testing.T) {
	n := node(t)
	oldSnapshot := placement.Snapshot{Nodes: []placement.Node{n}}
	entered, release := make(chan struct{}), make(chan struct{})
	unblock := sync.OnceFunc(func() { close(release) })
	t.Cleanup(unblock)
	store := &delayedProbeStore{fakeStore: &fakeStore{}, oldSession: n.SessionID, failure: errors.New("old session lost")}
	transport := &delayedSessionTransport{fakeTransport: &fakeTransport{}, entered: entered, release: release}
	session(transport.fakeTransport, n)
	d, err := New(Config{Store: store, Transport: transport, Now: clock, Wait: time.Second})
	if err != nil {
		t.Fatal(err)
	}
	done := make(chan error, 1)
	go func() { _, err := d.Refresh(t.Context(), n.InstallationID, oldSnapshot); done <- err }()
	select {
	case <-entered:
	case <-time.After(3 * time.Second):
		t.Fatal("old refresh did not read its session")
	}
	n.SessionID = uuid.New()
	transport.mu.Lock()
	session(transport.fakeTransport, n)
	transport.mu.Unlock()
	current := placement.Snapshot{Nodes: []placement.Node{n}}
	if challenged, err := d.Refresh(t.Context(), n.InstallationID, current); err != nil || len(challenged) != 1 {
		t.Fatalf("new session did not obtain a probe: %v %v", challenged, err)
	}
	unblock()
	select {
	case err := <-done:
		if err != nil {
			t.Fatal(err)
		}
	case <-time.After(3 * time.Second):
		t.Fatal("old refresh did not finish")
	}
	if challenged, err := d.Refresh(t.Context(), n.InstallationID, current); err != nil || len(challenged) != 1 {
		t.Fatalf("later planner did not share the current probe: %v %v", challenged, err)
	}
	if len(store.probes) != 1 || len(transport.sent) != 1 || transport.sent[0].SessionId != n.SessionID.String() {
		t.Fatalf("stale session read replaced the current probe: begins=%d sends=%d", len(store.probes), len(transport.sent))
	}
}

func TestProbeBeginHasDeadlineAndCancellationReleasesClaim(t *testing.T) {
	n := node(t)
	entered, release := make(chan struct{}), make(chan struct{})
	contexts := make(chan context.Context, 1)
	store := &delayedProbeStore{fakeStore: &fakeStore{}, oldSession: n.SessionID, entered: entered, release: release, contexts: contexts}
	transport := &fakeTransport{}
	session(transport, n)
	d, err := New(Config{Store: store, Transport: transport, Now: clock, Wait: time.Second})
	if err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithCancel(t.Context())
	t.Cleanup(cancel)
	snapshot := placement.Snapshot{Nodes: []placement.Node{n}}
	done := make(chan error, 1)
	go func() { _, err := d.Refresh(ctx, n.InstallationID, snapshot); done <- err }()
	select {
	case <-entered:
	case <-time.After(3 * time.Second):
		t.Fatal("refresh did not reach the store")
	}
	work := <-contexts
	deadline, ok := work.Deadline()
	if !ok || time.Until(deadline) > placement.ProbeWindow {
		t.Error("probe begin has no bounded store deadline")
	}
	cancel()
	select {
	case err := <-done:
		if err != nil {
			t.Fatal(err)
		}
	case <-time.After(3 * time.Second):
		t.Fatal("probe begin did not stop after cancellation")
	}
	if len(transport.sent) != 0 {
		t.Fatal("cancelled store call sent a probe")
	}
	store.oldSession = uuid.Nil
	store.contexts = nil
	if challenged, err := d.Refresh(t.Context(), n.InstallationID, snapshot); err != nil || len(challenged) != 1 {
		t.Fatalf("cancelled begin retained the claim: %v %v", challenged, err)
	}
	if len(store.probes) != 2 || len(transport.sent) != 1 {
		t.Fatalf("cancelled claim was not retried: begins=%d sends=%d", len(store.probes), len(transport.sent))
	}
}
