package postgres

import (
	"context"
	"errors"
	"math"
	"strings"
	"time"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"
	"github.com/sirrobot01/synod/internal/operation"
	agentv1 "github.com/sirrobot01/synod/proto/agent/v1"
	"google.golang.org/protobuf/proto"
)

var ErrStaleSession = errors.New("stale agent session")

// AgentPresence is the last observed session and health of one agent.
type AgentPresence struct {
	InstallationID   uuid.UUID
	NodeID           uuid.UUID
	SessionID        uuid.UUID
	Hello            *agentv1.Hello
	Heartbeat        *agentv1.Heartbeat
	ConnectedAt      time.Time
	LastSeenAt       time.Time
	RecoveryRequired bool
}

// RecordAgentSession runs after current certificate authorization and before publication.
// Session identity is assigned by the controller transport, never by the agent.
func (s *Store) RecordAgentSession(ctx context.Context, installationID, nodeID, sessionID uuid.UUID, hello *agentv1.Hello) error {
	if installationID == uuid.Nil || nodeID == uuid.Nil || sessionID == uuid.Nil || hello == nil || hello.InstallationId != installationID.String() || hello.NodeId != nodeID.String() || proto.Size(hello) > 64<<10 || len(hello.RuntimeVersion) > 256 || len(hello.Capabilities) > 128 || len(hello.ActivePayloadVersions) > 64 {
		return &AdmissionError{Code: InvalidRequest}
	}
	for _, id := range []string{hello.BootId, hello.JournalGeneration} {
		parsed, err := uuid.Parse(id)
		if err != nil || parsed == uuid.Nil || parsed.String() != id {
			return &AdmissionError{Code: InvalidRequest}
		}
	}
	if _, err := operation.NegotiateHello(hello, hello.Capabilities); err != nil {
		return &AdmissionError{Code: InvalidRequest}
	}
	for _, capability := range hello.Capabilities {
		if len(capability) < 1 || len(capability) > 128 || strings.ContainsFunc(capability, func(r rune) bool {
			return !(r >= 'a' && r <= 'z' || r >= '0' && r <= '9' || r == '.' || r == '_' || r == '-')
		}) {
			return &AdmissionError{Code: InvalidRequest}
		}
	}
	encoded, err := (proto.MarshalOptions{Deterministic: true}).Marshal(hello)
	if err != nil {
		return &AdmissionError{Code: InvalidRequest}
	}
	tx, err := s.pool.Begin(ctx)
	if err != nil {
		return admissionError(err)
	}
	defer tx.Rollback(ctx)
	var enrolled uuid.UUID
	if err := tx.QueryRow(ctx, `SELECT id FROM nodes WHERE installation_id=$1 AND id=$2 AND revoked_at IS NULL FOR UPDATE`, installationID, nodeID).Scan(&enrolled); err != nil {
		return admissionError(err)
	}
	var currentID uuid.UUID
	var previousBytes []byte
	var recovery bool
	err = tx.QueryRow(ctx, `SELECT p.session_id,s.hello_bytes,p.recovery_required FROM agent_presence p
 JOIN agent_sessions s USING (installation_id,node_id,session_id) WHERE p.installation_id=$1 AND p.node_id=$2`, installationID, nodeID).Scan(&currentID, &previousBytes, &recovery)
	if err != nil && !errors.Is(err, pgx.ErrNoRows) {
		return admissionError(err)
	}
	if currentID != uuid.Nil {
		var previous agentv1.Hello
		if proto.Unmarshal(previousBytes, &previous) != nil {
			return &AdmissionError{Code: DependencyUnavailable}
		}
		if currentID == sessionID {
			if !proto.Equal(&previous, hello) {
				return operation.ErrCommandConflict
			}
			return nil
		}
		recovery = recovery || previous.JournalGeneration != hello.JournalGeneration
	}
	var used bool
	if err := tx.QueryRow(ctx, `SELECT EXISTS(SELECT 1 FROM agent_sessions WHERE session_id=$1)`, sessionID).Scan(&used); err != nil {
		return admissionError(err)
	}
	if used {
		return ErrStaleSession
	}
	if _, err := tx.Exec(ctx, `INSERT INTO agent_sessions (session_id,installation_id,node_id,hello_bytes) VALUES ($1,$2,$3,$4)`, sessionID, installationID, nodeID, encoded); err != nil {
		return admissionError(err)
	}
	if _, err := tx.Exec(ctx, `INSERT INTO agent_presence (installation_id,node_id,session_id,recovery_required)
 VALUES ($1,$2,$3,$4) ON CONFLICT (installation_id,node_id) DO UPDATE SET session_id=EXCLUDED.session_id,
 heartbeat_sequence=0,heartbeat_bytes=NULL,last_seen_at=clock_timestamp(),recovery_required=EXCLUDED.recovery_required`, installationID, nodeID, sessionID, recovery); err != nil {
		return admissionError(err)
	}
	return s.commitMetadata(ctx, tx)
}

// RecordAgentHeartbeat uses database receipt time for freshness and exact session scope.
// App health requires an allocated copy, a current session, and fresh port evidence.
func (s *Store) RecordAgentHeartbeat(ctx context.Context, installationID, nodeID, sessionID uuid.UUID, heartbeat *agentv1.Heartbeat) error {
	if installationID == uuid.Nil || nodeID == uuid.Nil || sessionID == uuid.Nil || heartbeat == nil || heartbeat.Sequence == 0 || heartbeat.Sequence > math.MaxInt64 || heartbeat.ObservedAt == nil || heartbeat.ObservedAt.CheckValid() != nil || proto.Size(heartbeat) > 256<<10 || len(heartbeat.Observations) > 1024 {
		return &AdmissionError{Code: InvalidRequest}
	}
	for _, observation := range heartbeat.Observations {
		if observation == nil || observation.ObservedAt == nil || observation.ObservedAt.CheckValid() != nil || observation.ObservedRevision < 0 || len(observation.Lifecycle) > 64 || len(observation.Reason) > 1024 {
			return &AdmissionError{Code: InvalidRequest}
		}
		id, err := uuid.Parse(observation.ResourceId)
		if err != nil || id == uuid.Nil || id.String() != observation.ResourceId {
			return &AdmissionError{Code: InvalidRequest}
		}
	}
	encoded, err := (proto.MarshalOptions{Deterministic: true}).Marshal(heartbeat)
	if err != nil {
		return &AdmissionError{Code: InvalidRequest}
	}
	tx, err := s.pool.Begin(ctx)
	if err != nil {
		return admissionError(err)
	}
	defer tx.Rollback(ctx)
	var current uuid.UUID
	var sequence int64
	var previousBytes []byte
	if err := tx.QueryRow(ctx, `SELECT session_id,heartbeat_sequence,heartbeat_bytes FROM agent_presence
 WHERE installation_id=$1 AND node_id=$2 FOR UPDATE`, installationID, nodeID).Scan(&current, &sequence, &previousBytes); err != nil {
		return admissionError(err)
	}
	if current != sessionID {
		return ErrStaleSession
	}
	if int64(heartbeat.Sequence) < sequence {
		return ErrStaleSession
	}
	if int64(heartbeat.Sequence) == sequence {
		var previous agentv1.Heartbeat
		if proto.Unmarshal(previousBytes, &previous) != nil {
			return &AdmissionError{Code: DependencyUnavailable}
		}
		if !proto.Equal(&previous, heartbeat) {
			return operation.ErrCommandConflict
		}
		return nil
	}
	if _, err := tx.Exec(ctx, `UPDATE agent_presence SET heartbeat_sequence=$1,heartbeat_bytes=$2,last_seen_at=clock_timestamp()
 WHERE installation_id=$3 AND node_id=$4`, int64(heartbeat.Sequence), encoded, installationID, nodeID); err != nil {
		return admissionError(err)
	}
	if err := recordAppObservations(ctx, tx, installationID, nodeID, sessionID, heartbeat.Observations); err != nil {
		return err
	}
	return s.commitMetadata(ctx, tx)
}

// GetAgentPresence reads observation only. Presence never grants execution authority.
func (s *Store) GetAgentPresence(ctx context.Context, installationID, nodeID uuid.UUID) (AgentPresence, error) {
	if installationID == uuid.Nil || nodeID == uuid.Nil {
		return AgentPresence{}, &AdmissionError{Code: InvalidRequest}
	}
	result := AgentPresence{InstallationID: installationID, NodeID: nodeID}
	var hello, heartbeat []byte
	if err := s.pool.QueryRow(ctx, `SELECT p.session_id,s.hello_bytes,p.heartbeat_bytes,s.connected_at,p.last_seen_at,p.recovery_required
 FROM agent_presence p JOIN agent_sessions s USING (installation_id,node_id,session_id)
 WHERE p.installation_id=$1 AND p.node_id=$2`, installationID, nodeID).Scan(&result.SessionID, &hello, &heartbeat, &result.ConnectedAt, &result.LastSeenAt, &result.RecoveryRequired); err != nil {
		return AgentPresence{}, admissionError(err)
	}
	result.Hello = new(agentv1.Hello)
	if proto.Unmarshal(hello, result.Hello) != nil {
		return AgentPresence{}, &AdmissionError{Code: DependencyUnavailable}
	}
	if len(heartbeat) > 0 {
		result.Heartbeat = new(agentv1.Heartbeat)
		if proto.Unmarshal(heartbeat, result.Heartbeat) != nil {
			return AgentPresence{}, &AdmissionError{Code: DependencyUnavailable}
		}
	}
	return result, nil
}
