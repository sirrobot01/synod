package postgres

import (
	"context"
	"errors"
	"io"
	"sync"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"
	"github.com/sirrobot01/synod/internal/operation"
	agentv1 "github.com/sirrobot01/synod/proto/agent/v1"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/types/known/timestamppb"
)

func presenceFixture(t *testing.T) (*Store, uuid.UUID, uuid.UUID, *agentv1.Hello) {
	t.Helper()
	s, registration, request, ca := enrollmentFixture(t)
	if _, err := s.RegisterEnrollmentToken(t.Context(), registration); err != nil {
		t.Fatal(err)
	}
	if _, err := s.EnrollAgent(t.Context(), request, ca); err != nil {
		t.Fatal(err)
	}
	return s, request.InstallationID, request.NodeID, &agentv1.Hello{InstallationId: request.InstallationID.String(), NodeId: request.NodeID.String(), BootId: uuid.NewString(), JournalGeneration: uuid.NewString(), Protocol: &agentv1.VersionRange{Minimum: 1, Maximum: 1}, Capabilities: []string{"runtime.docker"}, RuntimeVersion: "test-runtime", ActivePayloadVersions: []uint32{1}}
}

func TestAgentPresenceReplacementAndRecovery(t *testing.T) {
	s, installation, node, hello := presenceFixture(t)
	first := uuid.New()
	if err := s.RecordAgentSession(t.Context(), installation, node, first, hello); err != nil {
		t.Fatal(err)
	}
	presence, err := s.GetAgentPresence(t.Context(), installation, node)
	if err != nil || presence.SessionID != first || presence.RecoveryRequired || presence.Heartbeat != nil || !proto.Equal(presence.Hello, hello) {
		t.Fatalf("initial presence: %+v %v", presence, err)
	}
	if err := s.RecordAgentSession(t.Context(), installation, node, first, hello); err != nil {
		t.Fatal(err)
	}
	replay, err := s.GetAgentPresence(t.Context(), installation, node)
	if err != nil || !replay.ConnectedAt.Equal(presence.ConnectedAt) || !replay.LastSeenAt.Equal(presence.LastSeenAt) {
		t.Fatal("session replay changed timestamps")
	}
	heartbeat := &agentv1.Heartbeat{Sequence: 1, ObservedAt: timestamppb.New(time.Now().Add(-24 * time.Hour))}
	if err := s.RecordAgentHeartbeat(t.Context(), installation, node, first, heartbeat); err != nil {
		t.Fatal(err)
	}
	observed, err := s.GetAgentPresence(t.Context(), installation, node)
	if err != nil || observed.LastSeenAt.Before(presence.LastSeenAt) || !proto.Equal(observed.Heartbeat, heartbeat) {
		t.Fatal("heartbeat was not recorded")
	}
	if time.Since(observed.LastSeenAt) > time.Minute {
		t.Fatal("agent observation time determined freshness")
	}
	if err := s.RecordAgentHeartbeat(t.Context(), installation, node, first, heartbeat); err != nil {
		t.Fatal(err)
	}
	replay, err = s.GetAgentPresence(t.Context(), installation, node)
	if err != nil || !replay.LastSeenAt.Equal(observed.LastSeenAt) {
		t.Fatal("heartbeat replay advanced freshness")
	}
	changed := proto.Clone(heartbeat).(*agentv1.Heartbeat)
	changed.ObservedAt = timestamppb.Now()
	if err := s.RecordAgentHeartbeat(t.Context(), installation, node, first, changed); !errors.Is(err, operation.ErrCommandConflict) {
		t.Fatalf("changed sequence accepted: %v", err)
	}
	second := uuid.New()
	hello2 := proto.Clone(hello).(*agentv1.Hello)
	hello2.BootId = uuid.NewString()
	if err := s.RecordAgentSession(t.Context(), installation, node, second, hello2); err != nil {
		t.Fatal(err)
	}
	if err := s.RecordAgentHeartbeat(t.Context(), installation, node, first, heartbeat); !errors.Is(err, ErrStaleSession) {
		t.Fatalf("old heartbeat accepted: %v", err)
	}
	if err := s.RecordAgentSession(t.Context(), installation, node, first, hello); !errors.Is(err, ErrStaleSession) {
		t.Fatalf("old session revived: %v", err)
	}
	replaced, err := s.GetAgentPresence(t.Context(), installation, node)
	if err != nil || replaced.SessionID != second || replaced.Heartbeat != nil || replaced.RecoveryRequired {
		t.Fatal("reboot replacement lost identity")
	}
	hello3 := proto.Clone(hello2).(*agentv1.Hello)
	hello3.JournalGeneration = uuid.NewString()
	third := uuid.New()
	if err := s.RecordAgentSession(t.Context(), installation, node, third, hello3); err != nil {
		t.Fatal(err)
	}
	changedJournal, err := s.GetAgentPresence(t.Context(), installation, node)
	if err != nil || !changedJournal.RecoveryRequired {
		t.Fatal("changed journal did not require recovery")
	}
	if err := s.RecordAgentSession(t.Context(), installation, node, uuid.New(), hello3); err != nil {
		t.Fatal(err)
	}
	sticky, err := s.GetAgentPresence(t.Context(), installation, node)
	if err != nil || !sticky.RecoveryRequired {
		t.Fatal("reconnect cleared required recovery")
	}
	if _, err := s.GetAgentPresence(t.Context(), uuid.New(), node); err == nil {
		t.Fatal("cross-installation read accepted")
	}
}

func TestConcurrentAgentPresence(t *testing.T) {
	s, installation, node, hello := presenceFixture(t)
	ids := make([]uuid.UUID, 16)
	errs := make(chan error, 16)
	var wg sync.WaitGroup
	for i := range ids {
		ids[i] = uuid.New()
		wg.Go(func() { errs <- s.RecordAgentSession(t.Context(), installation, node, ids[i], hello) })
	}
	wg.Wait()
	close(errs)
	for err := range errs {
		if err != nil {
			t.Fatal(err)
		}
	}
	current, err := s.GetAgentPresence(t.Context(), installation, node)
	if err != nil {
		t.Fatal(err)
	}
	for _, id := range ids {
		err := s.RecordAgentHeartbeat(t.Context(), installation, node, id, &agentv1.Heartbeat{Sequence: 1, ObservedAt: timestamppb.Now()})
		if id == current.SessionID && err != nil {
			t.Fatal(err)
		}
		if id != current.SessionID && !errors.Is(err, ErrStaleSession) {
			t.Fatalf("stale concurrent session accepted: %v", err)
		}
	}
	var count int
	if err := s.pool.QueryRow(t.Context(), "SELECT count(*) FROM agent_presence").Scan(&count); err != nil || count != 1 {
		t.Fatalf("current presence rows=%d err=%v", count, err)
	}
}

func TestAgentPresenceUnknownCommit(t *testing.T) {
	s, installation, node, hello := presenceFixture(t)
	session := uuid.New()
	s.commitTx = func(ctx context.Context, tx pgx.Tx) error {
		if err := tx.Commit(ctx); err != nil {
			return err
		}
		return io.ErrUnexpectedEOF
	}
	requireCode(t, s.RecordAgentSession(t.Context(), installation, node, session, hello), OutcomeUnknown)
	s.commitTx = nil
	if err := s.RecordAgentSession(t.Context(), installation, node, session, hello); err != nil {
		t.Fatal(err)
	}
	heartbeat := &agentv1.Heartbeat{Sequence: 1, ObservedAt: timestamppb.Now()}
	s.commitTx = func(ctx context.Context, tx pgx.Tx) error {
		if err := tx.Commit(ctx); err != nil {
			return err
		}
		return io.ErrUnexpectedEOF
	}
	requireCode(t, s.RecordAgentHeartbeat(t.Context(), installation, node, session, heartbeat), OutcomeUnknown)
	s.commitTx = nil
	if err := s.RecordAgentHeartbeat(t.Context(), installation, node, session, heartbeat); err != nil {
		t.Fatal(err)
	}
	current, err := s.GetAgentPresence(t.Context(), installation, node)
	if err != nil || current.Heartbeat.Sequence != 1 {
		t.Fatal("unknown heartbeat did not recover")
	}
}

func TestAgentPresenceRejectsInvalidAndRollsBack(t *testing.T) {
	s, installation, node, hello := presenceFixture(t)
	wrong := proto.Clone(hello).(*agentv1.Hello)
	wrong.NodeId = uuid.NewString()
	requireCode(t, s.RecordAgentSession(t.Context(), installation, node, uuid.New(), wrong), InvalidRequest)
	if err := s.RecordAgentSession(t.Context(), installation, uuid.New(), uuid.New(), hello); err == nil {
		t.Fatal("unenrolled node accepted")
	}
	if _, err := s.pool.Exec(t.Context(), `CREATE FUNCTION reject_presence() RETURNS trigger LANGUAGE plpgsql AS $$ BEGIN RAISE EXCEPTION 'rollback fixture'; END $$;
 CREATE TRIGGER reject_presence BEFORE INSERT ON agent_presence FOR EACH ROW EXECUTE FUNCTION reject_presence()`); err != nil {
		t.Fatal(err)
	}
	if err := s.RecordAgentSession(t.Context(), installation, node, uuid.New(), hello); err == nil {
		t.Fatal("trigger did not reject presence")
	}
	var count int
	if err := s.pool.QueryRow(t.Context(), "SELECT count(*) FROM agent_sessions").Scan(&count); err != nil || count != 0 {
		t.Fatalf("session survived rollback: %d %v", count, err)
	}
}
