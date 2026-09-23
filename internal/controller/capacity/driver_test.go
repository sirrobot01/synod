package capacity

import (
	"context"
	"errors"
	"sync"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/sirrobot01/synod/internal/controlstream"
	"github.com/sirrobot01/synod/internal/placement"
	agentv1 "github.com/sirrobot01/synod/proto/agent/v1"
)

// fakeStore answers from a snapshot a test controls, so the rules are what is
// measured rather than the database.
type fakeStore struct {
	mu        sync.Mutex
	snapshot  placement.Snapshot
	probes    []uuid.UUID
	probeErr  map[uuid.UUID]error
	revision  int64
	readCount int
}

func (f *fakeStore) ReadPlacementSnapshot(context.Context, uuid.UUID) (placement.Snapshot, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.readCount++
	return f.snapshot, nil
}

func (f *fakeStore) BeginCapacityProbe(_ context.Context, installationID, nodeID, sessionID uuid.UUID) (placement.CapacityProbe, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	if err := f.probeErr[nodeID]; err != nil {
		return placement.CapacityProbe{}, err
	}
	f.probes = append(f.probes, nodeID)
	f.revision++
	issued := clock()
	return placement.CapacityProbe{
		ID: uuid.New(), InstallationID: installationID, NodeID: nodeID, SessionID: sessionID,
		Revision: f.revision, IssuedAt: issued,
		ExpiresAt: issued.Add(placement.ProbeWindow), ValidUntil: issued.Add(placement.CapacityAge),
	}, nil
}

// fakeTransport records what the driver sent and to which session.
type fakeTransport struct {
	mu       sync.Mutex
	sessions map[uuid.UUID]controlstream.Peer
	sent     []*agentv1.CapacityRequest
	sendErr  error
}

func (f *fakeTransport) Session(nodeID uuid.UUID) (controlstream.Peer, bool) {
	f.mu.Lock()
	defer f.mu.Unlock()
	peer, ok := f.sessions[nodeID]
	return peer, ok
}

func (f *fakeTransport) Send(_ uuid.UUID, _ string, message *agentv1.ControllerMessage) error {
	f.mu.Lock()
	defer f.mu.Unlock()
	if f.sendErr != nil {
		return f.sendErr
	}
	f.sent = append(f.sent, message.GetCapacityRequest())
	return nil
}

var fixedNow = time.Date(2026, 9, 19, 12, 0, 0, 0, time.UTC)

func clock() time.Time { return fixedNow }

// node builds one candidate whose presence is fresh and whose policy is approved.
func node(t *testing.T) placement.Node {
	t.Helper()
	return placement.Node{
		ID: uuid.New(), InstallationID: uuid.New(), SessionID: uuid.New(),
		BootID: uuid.New(), JournalGeneration: uuid.New(),
		LastSeenAt: fixedNow.Add(-time.Second),
		Policy:     &placement.NodePolicy{Revision: 1},
	}
}

// evidence gives one node a probe and a matching report valid for a given span.
func evidence(n *placement.Node, valid time.Duration) {
	probe := &placement.CapacityProbe{
		ID: uuid.New(), InstallationID: n.InstallationID, NodeID: n.ID, SessionID: n.SessionID,
		BootID: n.BootID, JournalGeneration: n.JournalGeneration, Revision: 7,
		IssuedAt: fixedNow.Add(-time.Second), ValidUntil: fixedNow.Add(valid),
	}
	n.Probe = probe
	n.Report = &placement.CapacityReport{
		ProbeID: probe.ID, ProbeRevision: probe.Revision, InstallationID: n.InstallationID,
		NodeID: n.ID, SessionID: n.SessionID, BootID: n.BootID, JournalGeneration: n.JournalGeneration,
	}
}

func driver(t *testing.T, store *fakeStore, transport *fakeTransport) *Driver {
	t.Helper()
	d, err := New(Config{Store: store, Transport: transport, Now: clock, Wait: time.Second})
	if err != nil {
		t.Fatal(err)
	}
	return d
}

// session registers the transport session the store recorded for a node.
func session(transport *fakeTransport, n placement.Node) {
	if transport.sessions == nil {
		transport.sessions = map[uuid.UUID]controlstream.Peer{}
	}
	transport.sessions[n.ID] = controlstream.Peer{NodeID: n.ID, SessionID: n.SessionID.String()}
}

func TestRefreshChallengesANodeWithNoEvidence(t *testing.T) {
	n := node(t)
	store := &fakeStore{snapshot: placement.Snapshot{Nodes: []placement.Node{n}}}
	transport := &fakeTransport{}
	session(transport, n)
	challenged, err := driver(t, store, transport).Refresh(t.Context(), n.InstallationID, store.snapshot)
	if err != nil {
		t.Fatal(err)
	}
	if len(challenged) != 1 || challenged[0] != n.ID {
		t.Fatalf("challenged=%v, want the node with no evidence", challenged)
	}
	if len(transport.sent) != 1 {
		t.Fatalf("sent=%d, want one challenge", len(transport.sent))
	}
	request := transport.sent[0]
	// The challenge must name the session, boot, and journal the store recorded.
	if request.SessionId != n.SessionID.String() || request.BootId != n.BootID.String() || request.JournalGeneration != n.JournalGeneration.String() {
		t.Fatalf("challenge=%+v, want it bound to the recorded node state", request)
	}
	if request.ProbeRevision < 1 || request.ExpiresUnixMs <= 0 {
		t.Fatalf("challenge=%+v, want a usable probe window", request)
	}
}

// Evidence that outlives the decision is evidence. Re-probing would spend a
// revision and invalidate the report already in hand.
func TestRefreshLeavesUsableEvidenceAlone(t *testing.T) {
	n := node(t)
	evidence(&n, placement.CapacityAge)
	store := &fakeStore{snapshot: placement.Snapshot{Nodes: []placement.Node{n}}}
	transport := &fakeTransport{}
	session(transport, n)
	challenged, err := driver(t, store, transport).Refresh(t.Context(), n.InstallationID, store.snapshot)
	if err != nil || len(challenged) != 0 {
		t.Fatalf("challenged=%v err=%v, want none", challenged, err)
	}
}

// A report that expires during the decision is refreshed rather than raced.
func TestRefreshReplacesEvidenceThatExpiresTooSoon(t *testing.T) {
	n := node(t)
	evidence(&n, time.Second)
	store := &fakeStore{snapshot: placement.Snapshot{Nodes: []placement.Node{n}}}
	transport := &fakeTransport{}
	session(transport, n)
	challenged, err := driver(t, store, transport).Refresh(t.Context(), n.InstallationID, store.snapshot)
	if err != nil || len(challenged) != 1 {
		t.Fatalf("challenged=%v err=%v, want the expiring node", challenged, err)
	}
}

func TestRefreshSkipsANodeThatCannotAnswer(t *testing.T) {
	for name, spoil := range map[string]func(*placement.Node, *fakeTransport){
		// Presence outside the contract's limit means nobody has heard from it.
		"stale presence": func(n *placement.Node, tr *fakeTransport) {
			n.LastSeenAt = fixedNow.Add(-placement.PresenceAge - time.Second)
		},
		"revoked":           func(n *placement.Node, tr *fakeTransport) { n.Revoked = true },
		"recovery required": func(n *placement.Node, tr *fakeTransport) { n.RecoveryRequired = true },
		"no policy":         func(n *placement.Node, tr *fakeTransport) { n.Policy = nil },
		"no session at all": func(n *placement.Node, tr *fakeTransport) { tr.sessions = nil },
		// A report from another session cannot publish capacity, so challenging
		// that session would spend a probe revision for nothing.
		"another session": func(n *placement.Node, tr *fakeTransport) {
			tr.sessions[n.ID] = controlstream.Peer{NodeID: n.ID, SessionID: uuid.NewString()}
		},
	} {
		t.Run(name, func(t *testing.T) {
			n := node(t)
			transport := &fakeTransport{}
			session(transport, n)
			spoil(&n, transport)
			store := &fakeStore{snapshot: placement.Snapshot{Nodes: []placement.Node{n}}}
			challenged, err := driver(t, store, transport).Refresh(t.Context(), n.InstallationID, store.snapshot)
			if err != nil || len(challenged) != 0 {
				t.Fatalf("challenged=%v err=%v, want none", challenged, err)
			}
			if len(transport.sent) != 0 {
				t.Fatalf("sent=%d, want no challenge", len(transport.sent))
			}
		})
	}
}

// The agent cannot learn a reservation identity from its host, so the controller
// sends the pairs it holds.
func TestRefreshCarriesTheKnownReservations(t *testing.T) {
	n := node(t)
	reservation, resource := uuid.New(), uuid.New()
	store := &fakeStore{snapshot: placement.Snapshot{
		Nodes: []placement.Node{n},
		Allocations: []placement.Allocation{
			{ID: reservation, NodeID: n.ID, ResourceID: resource},
			// A released allocation holds nothing, so it is not measured.
			{ID: uuid.New(), NodeID: n.ID, ResourceID: uuid.New(), Released: true},
			// Another node's allocation belongs on another challenge.
			{ID: uuid.New(), NodeID: uuid.New(), ResourceID: uuid.New()},
		},
	}}
	transport := &fakeTransport{}
	session(transport, n)
	if _, err := driver(t, store, transport).Refresh(t.Context(), n.InstallationID, store.snapshot); err != nil {
		t.Fatal(err)
	}
	known := transport.sent[0].Known
	if len(known) != 1 || known[0].ReservationId != reservation.String() || known[0].ResourceId != resource.String() {
		t.Fatalf("known=%+v, want the one held reservation on this node", known)
	}
}

// A store that refuses one node must not stop the rest of the installation.
func TestRefreshContinuesPastOneRefusedProbe(t *testing.T) {
	first, second := node(t), node(t)
	second.InstallationID = first.InstallationID
	store := &fakeStore{
		snapshot: placement.Snapshot{Nodes: []placement.Node{first, second}},
		probeErr: map[uuid.UUID]error{first.ID: errors.New("no approved policy")},
	}
	transport := &fakeTransport{}
	session(transport, first)
	session(transport, second)
	challenged, err := driver(t, store, transport).Refresh(t.Context(), first.InstallationID, store.snapshot)
	if err != nil {
		t.Fatal(err)
	}
	if len(challenged) != 1 || challenged[0] != second.ID {
		t.Fatalf("challenged=%v, want only the node whose probe was issued", challenged)
	}
}

// A decision must never stand on a host nobody measured.
func TestDecideRefusesWithoutMeasuredEvidence(t *testing.T) {
	n := node(t)
	store := &fakeStore{snapshot: placement.Snapshot{Nodes: []placement.Node{n}}}
	transport := &fakeTransport{}
	session(transport, n)
	// A moving clock, because the wait ends on a deadline.
	start := fixedNow
	moving := func() time.Time { start = start.Add(300 * time.Millisecond); return start }
	d, err := New(Config{Store: store, Transport: transport, Now: moving, Wait: time.Second})
	if err != nil {
		t.Fatal(err)
	}
	request := placement.Request{Version: 1, InstallationID: n.InstallationID, ClusterID: uuid.New(), OperationID: uuid.New(), DesiredRevision: 1}
	// The node was challenged and never answered. The snapshot still carries no
	// report, so the decision fails rather than guessing at the host.
	if _, _, err := d.Decide(t.Context(), request); !errors.Is(err, ErrNoEvidence) {
		t.Fatalf("err=%v, want ErrNoEvidence", err)
	}
	if len(transport.sent) == 0 {
		t.Fatal("the decision planned without ever challenging the node")
	}
	// The wait must end at the deadline rather than spinning forever.
	if store.readCount < 2 {
		t.Fatalf("reads=%d, want a refresh and at least one settle read", store.readCount)
	}
}

// A lost answer is ordinary. It must end the wait, not hang the decision.
func TestDecideStopsWaitingAtTheProbeWindow(t *testing.T) {
	n := node(t)
	store := &fakeStore{snapshot: placement.Snapshot{Nodes: []placement.Node{n}}}
	transport := &fakeTransport{}
	session(transport, n)
	// A moving clock lets the deadline pass while the node stays silent.
	start := fixedNow
	moving := func() time.Time { start = start.Add(300 * time.Millisecond); return start }
	d, err := New(Config{Store: store, Transport: transport, Now: moving, Wait: time.Second})
	if err != nil {
		t.Fatal(err)
	}
	request := placement.Request{Version: 1, InstallationID: n.InstallationID, ClusterID: uuid.New(), OperationID: uuid.New(), DesiredRevision: 1}
	done := make(chan error, 1)
	go func() { _, _, err := d.Decide(t.Context(), request); done <- err }()
	select {
	case err := <-done:
		if !errors.Is(err, ErrNoEvidence) {
			t.Fatalf("err=%v, want ErrNoEvidence", err)
		}
	case <-time.After(10 * time.Second):
		t.Fatal("a lost answer hung the decision")
	}
}

func TestNewChecksItsInputs(t *testing.T) {
	store, transport := &fakeStore{}, &fakeTransport{}
	if _, err := New(Config{Transport: transport}); err == nil {
		t.Fatal("a driver without a store was accepted")
	}
	if _, err := New(Config{Store: store}); err == nil {
		t.Fatal("a driver without a transport was accepted")
	}
	if _, err := New(Config{Store: store, Transport: transport, Margin: time.Hour}); err == nil {
		t.Fatal("a margin beyond the capacity age was accepted")
	}
	// The contract's own timings are the defaults.
	d, err := New(Config{Store: store, Transport: transport})
	if err != nil || d.config.Margin != placement.ProbeWindow || d.config.Wait != placement.ProbeWindow {
		t.Fatalf("defaults=%+v err=%v", d.config, err)
	}
}

// Evidence bound to other state is not evidence, whatever its timestamps say.
func TestEvidenceBoundToOtherStateIsRefused(t *testing.T) {
	for name, spoil := range map[string]func(*placement.Node){
		"another session": func(n *placement.Node) { n.Report.SessionID = uuid.New() },
		// A rebooted host measured a machine in a different state.
		"another boot":    func(n *placement.Node) { n.Report.BootID = uuid.New() },
		"another journal": func(n *placement.Node) { n.Report.JournalGeneration = uuid.New() },
		// A report for a challenge that was replaced cannot publish capacity.
		"another probe":    func(n *placement.Node) { n.Report.ProbeID = uuid.New() },
		"another revision": func(n *placement.Node) { n.Report.ProbeRevision = n.Probe.Revision + 1 },
		"expired probe":    func(n *placement.Node) { n.Probe.ValidUntil = fixedNow.Add(-time.Second) },
		"no report":        func(n *placement.Node) { n.Report = nil },
		"no probe":         func(n *placement.Node) { n.Probe = nil },
	} {
		t.Run(name, func(t *testing.T) {
			n := node(t)
			evidence(&n, placement.CapacityAge)
			spoil(&n)
			if usable(&n, fixedNow) {
				t.Fatal("a report bound to other state was accepted as evidence")
			}
			// A decision must refuse rather than place on it.
			store := &fakeStore{snapshot: placement.Snapshot{Nodes: []placement.Node{n}}}
			transport := &fakeTransport{}
			session(transport, n)
			start := fixedNow
			moving := func() time.Time { start = start.Add(300 * time.Millisecond); return start }
			d, err := New(Config{Store: store, Transport: transport, Now: moving, Wait: time.Second})
			if err != nil {
				t.Fatal(err)
			}
			request := placement.Request{Version: 1, InstallationID: n.InstallationID, ClusterID: uuid.New(), OperationID: uuid.New(), DesiredRevision: 1}
			if _, _, err := d.Decide(t.Context(), request); !errors.Is(err, ErrNoEvidence) {
				t.Fatalf("err=%v, want ErrNoEvidence", err)
			}
		})
	}
}

// Operations plan concurrently and each one refreshes. An agent holds one probe per
// session, so a second challenge replaces the one it is still measuring and that
// answer is discarded. Two pending operations were enough to starve a real node
// indefinitely: its probe revision climbed while no report ever committed.
func TestConcurrentRefreshesShareOneProbe(t *testing.T) {
	n := node(t)
	store := &fakeStore{snapshot: placement.Snapshot{Nodes: []placement.Node{n}}}
	transport := &fakeTransport{}
	session(transport, n)
	d := driver(t, store, transport)
	const planners = 8
	challenged := make([][]uuid.UUID, planners)
	snapshot := store.snapshot
	var planning sync.WaitGroup
	for i := range planners {
		planning.Go(func() {
			got, err := d.Refresh(t.Context(), n.InstallationID, snapshot)
			if err != nil {
				t.Error(err)
				return
			}
			challenged[i] = got
		})
	}
	planning.Wait()
	if len(store.probes) != 1 || len(transport.sent) != 1 {
		t.Fatalf("probes=%d sent=%d, want one probe shared by every planner", len(store.probes), len(transport.sent))
	}
	// Every planner must still wait for the node. A planner told it was not
	// challenged would plan without the evidence that is on its way.
	for i, got := range challenged {
		if len(got) != 1 || got[0] != n.ID {
			t.Fatalf("planner %d waits for %v, want the node whose probe is in flight", i, got)
		}
	}
}

// A claim is not a lock held forever. An agent that never answers must be challenged
// again once its probe has expired, and a reconnected agent has no memory of one.
func TestRefreshChallengesAgainAfterAProbeExpiresOrASessionChanges(t *testing.T) {
	n := node(t)
	store := &fakeStore{snapshot: placement.Snapshot{Nodes: []placement.Node{n}}}
	transport := &fakeTransport{}
	session(transport, n)
	now := fixedNow
	d, err := New(Config{Store: store, Transport: transport, Wait: time.Second, Now: func() time.Time { return now }})
	if err != nil {
		t.Fatal(err)
	}
	for _, step := range []struct {
		name  string
		apply func()
		want  int
	}{
		{name: "first challenge", apply: func() {}, want: 1},
		{name: "probe still in flight", apply: func() { now = now.Add(placement.ProbeWindow / 2) }, want: 1},
		{name: "probe expired", apply: func() { now = now.Add(placement.ProbeWindow) }, want: 2},
		{name: "agent reconnected", apply: func() {
			n.SessionID = uuid.New()
			store.snapshot = placement.Snapshot{Nodes: []placement.Node{n}}
			session(transport, n)
		}, want: 3},
	} {
		t.Run(step.name, func(t *testing.T) {
			step.apply()
			if _, err := d.Refresh(t.Context(), n.InstallationID, store.snapshot); err != nil {
				t.Fatal(err)
			}
			if len(store.probes) != step.want {
				t.Fatalf("probes=%d, want %d", len(store.probes), step.want)
			}
		})
	}
}
