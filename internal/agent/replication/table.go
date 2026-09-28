// Package replication moves tier 2 snapshots between agents over mutual TLS.
//
// The controller assigns each agent a role per app in a ReplicationTable. The
// active agent pushes snapshots to its standbys. A standby accepts a push only
// from the one active peer that its table names, at the table's epoch.
package replication

import (
	"context"
	"errors"
	"net/netip"
	"regexp"
	"slices"
	"strings"
	"sync"

	"github.com/google/uuid"
	agentv1 "github.com/sirrobot01/synod/proto/agent/v1"
	"google.golang.org/protobuf/proto"
)

// Roles in an AppReplication.
const (
	RoleActive  = "active"
	RoleStandby = "standby"
)

// Capability is the agent capability that negotiates replication tables.
const Capability = "runtime.replication.v1"

const maxApps = 256

// ErrTable reports an invalid replication table.
var ErrTable = errors.New("replication: invalid table")

// Validate checks one replication table. An active app names 1 to 3 standby
// peers; a standby names exactly its active peer.
func Validate(apps []*agentv1.AppReplication) error {
	if len(apps) > maxApps {
		return ErrTable
	}
	seen := map[string]bool{}
	for _, app := range apps {
		if app == nil || !canonical(app.AppId) || seen[app.AppId] || app.Epoch == 0 {
			return ErrTable
		}
		seen[app.AppId] = true
		if app.Database != nil {
			if err := validateDatabase(app); err != nil {
				return err
			}
			continue
		}
		switch app.Role {
		case RoleActive:
			if len(app.Peers) < 1 || len(app.Peers) > 3 || app.IntervalSeconds < 10 || app.IntervalSeconds > 3600 {
				return ErrTable
			}
		case RoleStandby:
			if len(app.Peers) != 1 {
				return ErrTable
			}
		default:
			return ErrTable
		}
		peers := map[string]bool{}
		for _, peer := range app.Peers {
			if peer == nil || !canonical(peer.NodeId) || peers[peer.NodeId] {
				return ErrTable
			}
			peers[peer.NodeId] = true
			if address, err := netip.ParseAddrPort(peer.Address); err != nil || address.Port() == 0 || address.String() != peer.Address {
				return ErrTable
			}
		}
	}
	return nil
}

var (
	databaseName = regexp.MustCompile(`^[a-z][a-z0-9_]{0,62}$`)
	hostName     = regexp.MustCompile(`^[A-Za-z0-9.:-]{1,253}$`)
)

// validateDatabase checks a tier 1 entry. The engine replicates the data, so the entry
// names no snapshot peers. Only a standby names the primary it follows.
func validateDatabase(app *agentv1.AppReplication) error {
	d := app.Database
	if len(app.Peers) != 0 || app.IntervalSeconds != 0 {
		return ErrTable
	}
	switch d.Engine {
	case "postgres":
		if len(d.Databases) < 1 || len(d.Databases) > 16 || slices.ContainsFunc(d.Databases, func(name string) bool { return !databaseName.MatchString(name) }) {
			return ErrTable
		}
	case "redis":
		if len(d.Databases) != 0 {
			return ErrTable
		}
	default:
		return ErrTable
	}
	switch app.Role {
	case RoleActive:
		if d.PrimaryHost != "" || d.PrimaryPort != 0 || d.SwitchoverOperationId != "" {
			return ErrTable
		}
	case RoleStandby:
		if d.SwitchoverOperationId != "" && !canonical(d.SwitchoverOperationId) {
			return ErrTable
		}
		if !hostName.MatchString(d.PrimaryHost) || d.PrimaryPort == 0 || d.PrimaryPort > 65535 {
			return ErrTable
		}
	default:
		return ErrTable
	}
	return nil
}

func canonical(id string) bool {
	parsed, err := uuid.Parse(id)
	return err == nil && parsed != uuid.Nil && parsed.String() == id
}

// Table holds the current assignment of this agent. It is safe for concurrent use.
type Table struct {
	locks sync.Map
	mu    sync.RWMutex
	apps  map[string]*agentv1.AppReplication
}

// Set replaces the assignment. Validate the apps first.
func (t *Table) Set(apps []*agentv1.AppReplication) {
	next := make(map[string]*agentv1.AppReplication, len(apps))
	for _, app := range apps {
		next[app.AppId] = proto.Clone(app).(*agentv1.AppReplication)
	}
	t.mu.Lock()
	t.apps = next
	t.mu.Unlock()
}

// Lookup returns a copy of the assignment of one app.
func (t *Table) Lookup(appID string) (*agentv1.AppReplication, bool) {
	t.mu.RLock()
	defer t.mu.RUnlock()
	app, ok := t.apps[appID]
	if !ok {
		return nil, false
	}
	return proto.Clone(app).(*agentv1.AppReplication), true
}

// Apps returns a copy of every assignment.
func (t *Table) Apps() []*agentv1.AppReplication {
	t.mu.RLock()
	defer t.mu.RUnlock()
	apps := make([]*agentv1.AppReplication, 0, len(t.apps))
	for _, app := range t.apps {
		apps = append(apps, proto.Clone(app).(*agentv1.AppReplication))
	}
	return apps
}

// Status records the newest replica state per app for heartbeats. It is safe for concurrent use.
type Status struct {
	mu   sync.Mutex
	apps map[string]*agentv1.ReplicaStatus
}

// Record stores a success. Sequence and time come from the completed snapshot.
func (s *Status) Record(status *agentv1.ReplicaStatus) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.apps == nil {
		s.apps = map[string]*agentv1.ReplicaStatus{}
	}
	s.apps[status.AppId] = proto.Clone(status).(*agentv1.ReplicaStatus)
}

// Fail keeps the last success and records why the latest attempt failed.
func (s *Status) Fail(appID, role, code string) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.apps == nil {
		s.apps = map[string]*agentv1.ReplicaStatus{}
	}
	current := s.apps[appID]
	if current == nil {
		current = &agentv1.ReplicaStatus{AppId: appID}
		s.apps[appID] = current
	}
	current.Role, current.FailureCode = role, code
}

// List returns a sorted copy for a heartbeat.
func (s *Status) List() []*agentv1.ReplicaStatus {
	s.mu.Lock()
	defer s.mu.Unlock()
	out := make([]*agentv1.ReplicaStatus, 0, len(s.apps))
	for _, status := range s.apps {
		out = append(out, proto.Clone(status).(*agentv1.ReplicaStatus))
	}
	slices.SortFunc(out, func(a, b *agentv1.ReplicaStatus) int { return strings.Compare(a.AppId, b.AppId) })
	return out
}

// LockApp serializes replica changes and promotion for one app.
func (t *Table) LockApp(ctx context.Context, appID string) (func(), error) {
	lock, _ := t.locks.LoadOrStore(appID, make(chan struct{}, 1))
	gate := lock.(chan struct{})
	select {
	case gate <- struct{}{}:
		return func() { <-gate }, nil
	case <-ctx.Done():
		return nil, ctx.Err()
	}
}

// SnapshotMetrics publishes counters that the writer keeps on disk.
func (s *Status) SnapshotMetrics(appID string, last, maxPause, skipped uint64) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if current := s.apps[appID]; current != nil {
		current.LastPauseNanos, current.MaxPauseNanos, current.SkippedSnapshots = last, maxPause, skipped
	}
}
