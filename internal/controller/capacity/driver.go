// Package capacity drives the host probe that placement needs.
//
// The placement contract is explicit that presence, Hello capabilities, and agent
// timestamps cannot substitute for measured evidence. The planner already refuses a
// node whose probe or report is missing, expired, or bound to another session. This
// package is what makes fresh evidence exist in the first place.
//
// It never invents a figure and never relaxes a rule. A node that does not answer
// simply has no evidence, and the planner leaves it out.
package capacity

import (
	"context"
	"errors"
	"log/slog"
	"sync"
	"time"

	"github.com/google/uuid"
	"github.com/sirrobot01/synod/internal/controlstream"
	"github.com/sirrobot01/synod/internal/placement"
	agentv1 "github.com/sirrobot01/synod/proto/agent/v1"
)

// ErrNoEvidence reports that no node answered with capacity a decision could use.
var ErrNoEvidence = errors.New("no_measured_capacity")

// Transport sends controller messages to one authenticated agent session.
// *controlstream.Server satisfies it.
type Transport interface {
	Session(nodeID uuid.UUID) (controlstream.Peer, bool)
	Send(nodeID uuid.UUID, sessionID string, message *agentv1.ControllerMessage) error
}

// Store is the durable placement evidence boundary. *postgres.Store satisfies it.
type Store interface {
	ReadPlacementSnapshot(ctx context.Context, installationID uuid.UUID) (placement.Snapshot, error)
	BeginCapacityProbe(ctx context.Context, installationID, nodeID, sessionID uuid.UUID) (placement.CapacityProbe, error)
}

// Config binds one driver to its transport and store.
type Config struct {
	Store     Store
	Transport Transport
	// Margin is how much of a report's remaining life a decision needs. A report
	// that expires while the decision is being taken is refreshed instead of used.
	Margin time.Duration
	// Wait bounds how long a decision waits for challenged nodes to answer.
	Wait time.Duration
	// Now is the clock. A test replaces it; production leaves it nil.
	Now func() time.Time
	// Logger records the per-node failures that Refresh deliberately continues past.
	// Skipping a node is correct; leaving no account of why it was skipped is not.
	// slog.Default() is used when none is supplied.
	Logger *slog.Logger
}

// Driver challenges nodes and takes placement decisions on the answers.
type Driver struct {
	config Config
	mu     sync.Mutex
	// The probe in flight on each node. Operations plan concurrently and each one
	// refreshes, so without this the newest challenge replaces the one an agent is
	// still measuring: the agent holds one probe per session, discards the answer to
	// the superseded one, and no evidence ever commits.
	inflight map[uuid.UUID]*challenge
}

// challenge is one probe sent to a node and not yet answered. It is bound to the
// session it was sent on, because a reconnected agent has no memory of it.
type challenge struct {
	sessionID string
	expires   time.Time
}

// New checks the driver inputs and applies the contract's own timings.
func New(config Config) (*Driver, error) {
	if config.Store == nil || config.Transport == nil {
		return nil, errors.New("capacity driver requires a store and a transport")
	}
	if config.Margin == 0 {
		// A decision that starts with less than the probe window left can lose its
		// evidence part-way through. Refresh rather than race the expiry.
		config.Margin = placement.ProbeWindow
	}
	if config.Wait == 0 {
		config.Wait = placement.ProbeWindow
	}
	if config.Now == nil {
		config.Now = time.Now
	}
	if config.Logger == nil {
		config.Logger = slog.Default()
	}
	if config.Margin < time.Second || config.Margin > placement.CapacityAge || config.Wait < time.Second || config.Wait > placement.CapacityAge {
		return nil, errors.New("invalid capacity driver timings")
	}
	return &Driver{config: config, inflight: make(map[uuid.UUID]*challenge)}, nil
}

// claim returns ownership of a new probe, or nil for pending work or an old session.
func (d *Driver) claim(nodeID uuid.UUID, sessionID string, now time.Time) *challenge {
	d.mu.Lock()
	defer d.mu.Unlock()
	peer, ok := d.config.Transport.Session(nodeID)
	if !ok || peer.SessionID != sessionID {
		return nil
	}
	if held, exists := d.inflight[nodeID]; exists && held.sessionID == sessionID && (held.expires.IsZero() || held.expires.After(now)) {
		return nil
	}
	// Keep ownership until the bounded store call and send finish. A session
	// change can replace it, but a late completion cannot alter that replacement.
	owned := &challenge{sessionID: sessionID}
	d.inflight[nodeID] = owned
	return owned
}

// Refresh challenges every node that could carry work but has no usable evidence.
// It returns the nodes it challenged, so a caller can wait for exactly those.
//
// A node is challenged only when its presence is inside the contract's limit and the
// transport holds the same session the store recorded. Anything else cannot answer.
func (d *Driver) Refresh(ctx context.Context, installationID uuid.UUID, snapshot placement.Snapshot) ([]uuid.UUID, error) {
	now := d.config.Now()
	known := knownReservations(snapshot)
	var challenged []uuid.UUID
	for i := range snapshot.Nodes {
		node := &snapshot.Nodes[i]
		if !d.probeable(node, now) {
			continue
		}
		// The transport must hold the very session the store recorded. A report
		// from any other session cannot publish capacity, so challenging it would
		// spend a probe revision for nothing.
		peer, ok := d.config.Transport.Session(node.ID)
		if !ok || peer.SessionID != node.SessionID.String() {
			continue
		}
		owned := d.claim(node.ID, peer.SessionID, now)
		if owned == nil {
			// A probe this node is still measuring counts as challenged. The caller
			// waits for that answer rather than replacing it with one that starts
			// the measurement, and the wait, over again.
			challenged = append(challenged, node.ID)
			continue
		}
		work, cancel := context.WithTimeout(ctx, placement.ProbeWindow)
		probe, err := d.config.Store.BeginCapacityProbe(work, installationID, node.ID, node.SessionID)
		if err == nil {
			err = work.Err()
		}
		cancel()
		if err != nil {
			d.release(node.ID, owned)
			// A node that lost its session cannot answer. Continue with the others.
			d.config.Logger.Warn("capacity probe not begun", "installation_id", installationID, "node_id", node.ID, "session_id", node.SessionID, "error", err)
			continue
		}
		request := &agentv1.CapacityRequest{
			ProbeId: probe.ID.String(), ProbeRevision: probe.Revision,
			SessionId: node.SessionID.String(), BootId: node.BootID.String(),
			JournalGeneration: node.JournalGeneration.String(),
			ExpiresUnixMs:     probe.ExpiresAt.UnixMilli(),
			Known:             known[node.ID],
			Paths:             probe.Paths,
		}
		message := &agentv1.ControllerMessage{Message: &agentv1.ControllerMessage_CapacityRequest{CapacityRequest: request}}
		if err := d.config.Transport.Send(node.ID, peer.SessionID, message); err != nil {
			d.release(node.ID, owned)
			d.config.Logger.Warn("capacity challenge not sent", "installation_id", installationID, "node_id", node.ID, "session_id", node.SessionID, "probe_id", probe.ID, "error", err)
			continue
		}
		d.hold(node.ID, owned, probe.ExpiresAt)
		challenged = append(challenged, node.ID)
	}
	return challenged, nil
}

// release drops a claim whose probe was never sent, so the next caller can challenge.
func (d *Driver) release(nodeID uuid.UUID, owned *challenge) {
	d.mu.Lock()
	defer d.mu.Unlock()
	if d.inflight[nodeID] == owned {
		delete(d.inflight, nodeID)
	}
}

// hold corrects a claim to the expiry the store gave the probe, which is when the
// agent stops answering it.
func (d *Driver) hold(nodeID uuid.UUID, owned *challenge, expires time.Time) {
	d.mu.Lock()
	defer d.mu.Unlock()
	held := d.inflight[nodeID]
	if held != owned {
		return
	}
	held.expires = expires
	d.inflight[nodeID] = held
}

// probeable reports a node that could answer a challenge right now.
func (d *Driver) probeable(node *placement.Node, now time.Time) bool {
	switch {
	case node.Revoked || node.RecoveryRequired || node.SessionID == uuid.Nil:
		return false
	// Presence must be inside the contract's limit. A node nobody has heard from
	// cannot measure anything, whatever its last report said.
	case node.LastSeenAt.After(now) || !node.LastSeenAt.Add(placement.PresenceAge).After(now):
		return false
	}
	// A report that outlives the decision is evidence already. Anything shorter is
	// refreshed, because a decision must not lose its evidence part-way through.
	return !usable(node, now.Add(d.config.Margin))
}

// usable reports evidence that the planner would still accept at one instant.
func usable(node *placement.Node, at time.Time) bool {
	probe, report := node.Probe, node.Report
	if probe == nil || report == nil {
		return false
	}
	if node.Policy != nil && probe.PolicyRevision != node.Policy.Revision {
		return false
	}
	if !probe.ValidUntil.After(at) {
		return false
	}
	// The planner applies these bindings too. Checking them here keeps the driver
	// from treating a report for another session as evidence worth keeping.
	return report.ProbeID == probe.ID && report.ProbeRevision == probe.Revision &&
		report.SessionID == node.SessionID && report.BootID == node.BootID &&
		report.JournalGeneration == node.JournalGeneration
}

// knownReservations groups the reservation and resource pairs of each node.
//
// An agent cannot learn a reservation identity from its host: a Docker label names a
// resource and never a reservation. The controller holds the mapping, so it sends it
// and the agent echoes each pair with what it measured.
func knownReservations(snapshot placement.Snapshot) map[uuid.UUID][]*agentv1.KnownReservation {
	known := make(map[uuid.UUID][]*agentv1.KnownReservation)
	for _, allocation := range snapshot.Allocations {
		if allocation.Released || allocation.NodeID == uuid.Nil || allocation.ID == uuid.Nil || allocation.ResourceID == uuid.Nil {
			continue
		}
		known[allocation.NodeID] = append(known[allocation.NodeID], &agentv1.KnownReservation{
			ReservationId: allocation.ID.String(), ResourceId: allocation.ResourceID.String(),
		})
	}
	return known
}

// Decide refreshes capacity and then plans on what the nodes actually answered.
//
// It never plans on a snapshot it did not refresh, and it never waits past the probe
// window. A node that stayed silent has no evidence, so the planner leaves it out and
// the decision fails honestly rather than guessing at a host nobody measured.
func (d *Driver) Decide(ctx context.Context, request placement.Request) (placement.Decision, placement.Snapshot, error) {
	snapshot, err := d.config.Store.ReadPlacementSnapshot(ctx, request.InstallationID)
	if err != nil {
		return placement.Decision{}, snapshot, err
	}
	challenged, err := d.Refresh(ctx, request.InstallationID, snapshot)
	if err != nil {
		return placement.Decision{}, snapshot, err
	}
	if len(challenged) > 0 {
		snapshot, err = d.settle(ctx, request.InstallationID, challenged)
		if err != nil {
			return placement.Decision{}, snapshot, err
		}
	}
	if !d.anyUsable(snapshot) {
		return placement.Decision{}, snapshot, ErrNoEvidence
	}
	decision, err := placement.Plan(request, snapshot)
	return decision, snapshot, err
}

// settle waits for the challenged nodes to answer, then returns what landed.
// It stops at the first read where every challenged node has usable evidence, and
// otherwise at the deadline. A lost answer is ordinary and ends the wait.
func (d *Driver) settle(ctx context.Context, installationID uuid.UUID, challenged []uuid.UUID) (placement.Snapshot, error) {
	deadline := d.config.Now().Add(d.config.Wait)
	wanted := make(map[uuid.UUID]bool, len(challenged))
	for _, id := range challenged {
		wanted[id] = true
	}
	var snapshot placement.Snapshot
	// The deadline is the real bound. The attempt count only stops a clock that
	// never advances from spinning, which a real one never does.
	for attempt := range maxSettleReads {
		read, err := d.config.Store.ReadPlacementSnapshot(ctx, installationID)
		if err != nil {
			return snapshot, err
		}
		snapshot = read
		answered := 0
		now := d.config.Now()
		for i := range snapshot.Nodes {
			if wanted[snapshot.Nodes[i].ID] && usable(&snapshot.Nodes[i], now) {
				answered++
			}
		}
		if answered == len(wanted) || !now.Before(deadline) || attempt == maxSettleReads-1 {
			return snapshot, nil
		}
		timer := time.NewTimer(settleInterval)
		select {
		case <-ctx.Done():
			timer.Stop()
			return snapshot, ctx.Err()
		case <-timer.C:
		}
	}
	return snapshot, nil
}

// settleInterval is how often the wait re-reads committed evidence.
const settleInterval = 50 * time.Millisecond

// maxSettleReads bounds the wait independently of the clock. The probe window
// divided by the interval, with room to spare.
const maxSettleReads = int(placement.CapacityAge/settleInterval) + 1

// anyUsable reports whether any node carries evidence a decision could stand on.
func (d *Driver) anyUsable(snapshot placement.Snapshot) bool {
	now := d.config.Now()
	for i := range snapshot.Nodes {
		if usable(&snapshot.Nodes[i], now) {
			return true
		}
	}
	return false
}
