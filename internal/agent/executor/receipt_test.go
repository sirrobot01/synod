package executor

import (
	"context"
	"crypto/sha256"
	"io"
	"os"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/sirrobot01/synod/internal/app"
	"github.com/sirrobot01/synod/internal/controlstream"
	"github.com/sirrobot01/synod/internal/operation"
	"github.com/sirrobot01/synod/internal/store/postgres"
	agentv1 "github.com/sirrobot01/synod/proto/agent/v1"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/types/known/durationpb"
)

func TestRealPostgresReceiptReplaysAfterLostCommitReply(t *testing.T) {
	dsn := os.Getenv("SYNOD_TEST_DATABASE_URL")
	if dsn == "" {
		t.Skip("set SYNOD_TEST_DATABASE_URL or use scripts/test-postgres.sh")
	}
	f := newFixture(t, nil)
	admin, err := pgxpool.New(t.Context(), dsn)
	if err != nil {
		t.Fatal(err)
	}
	name := "task044_" + strings.ReplaceAll(uuid.NewString(), "-", "")
	if _, err := admin.Exec(t.Context(), "CREATE DATABASE "+pgx.Identifier{name}.Sanitize()); err != nil {
		admin.Close()
		t.Fatal(err)
	}
	config, err := pgxpool.ParseConfig(dsn)
	if err != nil {
		t.Fatal(err)
	}
	config.ConnConfig.Database = name
	pool, err := pgxpool.NewWithConfig(t.Context(), config)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		pool.Close()
		// The bound guards against a hang, and is not an assertion about speed:
		// on a loaded host many suites drop their databases at once, and a teardown
		// that is merely slow must not fail a test that passed.
		ctx, cancel := context.WithTimeout(context.Background(), dropTimeout)
		defer cancel()
		if _, err := admin.Exec(ctx, "DROP DATABASE "+pgx.Identifier{name}.Sanitize()+" WITH (FORCE)"); err != nil {
			t.Error(err)
		}
		admin.Close()
	})
	store := postgres.New(pool)
	if err := store.Migrate(t.Context()); err != nil {
		t.Fatal(err)
	}
	installation := uuid.MustParse(f.epoch.InstallationId)
	admission, err := store.AdmitApp(t.Context(), postgres.CreateAppRequest{Identity: postgres.RequestIdentity{InstallationID: installation, ActorID: "task044", APIOperation: postgres.CreateAppAPIOperation, IdempotencyKey: uuid.NewString()}, RequestID: uuid.NewString(),
		Spec: app.AppSpec{SchemaVersion: 1, Name: "receipt", Tier: app.TierParallel, Copies: 1, Service: app.ServiceSpec{Image: "nginx", Ports: []app.PortSpec{{ContainerPort: 80}}}, Router: app.RouterSpec{Protocol: "http", Port: 8080}}})
	if err != nil {
		t.Fatal(err)
	}
	f.epoch.AppId = admission.AppID.String()
	generation := uuid.MustParse(f.epoch.RecoveryGeneration)
	if err := store.InitializeGeneration(t.Context(), installation, generation); err != nil {
		t.Fatal(err)
	}
	claim, err := store.AcquireClaim(t.Context(), postgres.ClaimRequest{ClaimID: uuid.New(), InstallationID: installation, OperationID: admission.OperationID, RecoveryGeneration: generation, OwnerID: "task044", TTL: time.Minute, LeaseID: 1, LeaseRevision: 1})
	if err != nil {
		t.Fatal(err)
	}
	command := ensureCommand(t, f.epoch)
	command.OperationId = admission.OperationID.String()
	// A stop of an absent container is a definite result.
	command.Input.Payload = &agentv1.CommandInput_StopContainer{StopContainer: &agentv1.StopContainer{GracePeriod: durationpb.New(time.Second)}}
	command.Epoch.ExecutionEpoch = claim.ExecutionEpoch
	hash, err := operation.CommandHash(command)
	if err != nil {
		t.Fatal(err)
	}
	command.InputHash = hash[:]
	plan := &agentv1.ExecutionPlan{Version: 1, Steps: []*agentv1.PlanStep{{Command: command, MaxAttempts: 1, CancellationBoundary: agentv1.CancellationBoundary_CANCELLATION_BOUNDARY_BEFORE_EXECUTION, Postcondition: agentv1.Postcondition_POSTCONDITION_CONTAINER_STOPPED}}}
	if err := store.InstallPlan(t.Context(), claim, plan); err != nil {
		t.Fatal(err)
	}
	barrier := &agentv1.BarrierResult{Epoch: command.Epoch, AdmissionsClosed: true, Drained: true}
	if err := store.RecordBarrier(t.Context(), claim, barrier); err != nil {
		t.Fatal(err)
	}
	encoded, err := (proto.MarshalOptions{Deterministic: true}).Marshal(barrier)
	if err != nil {
		t.Fatal(err)
	}
	if err := store.ReconcileBarrier(t.Context(), claim, uuid.MustParse(f.epoch.NodeId), sha256.Sum256(encoded), "The isolated node has no native work."); err != nil {
		t.Fatal(err)
	}
	if err := store.ActivateClaim(t.Context(), claim); err != nil {
		t.Fatal(err)
	}
	if err := store.RecordDispatch(t.Context(), claim, command); err != nil {
		t.Fatal(err)
	}
	var committed atomic.Int32
	controller := startController(t, f, func(ctx context.Context, peer controlstream.Peer, message *agentv1.AgentMessage) error {
		result := message.GetTerminalResult()
		if result == nil {
			return nil
		}
		if err := store.RecordAgentResult(ctx, peer.InstallationID, peer.NodeID, result); err != nil {
			return err
		}
		if committed.Add(1) == 1 {
			return io.ErrUnexpectedEOF
		} // The real metadata commit succeeded before this lost reply.
		return nil
	})
	controller.activate(t, command.Epoch)
	controller.send(t, &agentv1.ControllerMessage{DeliveryAttemptId: uuid.NewString(), Message: &agentv1.ControllerMessage_Command{Command: command}})
	result := controller.wait(t, func(message *agentv1.AgentMessage) bool { return message.GetTerminalResult() != nil }).GetTerminalResult()
	if committed.Load() != 2 {
		t.Fatal("result did not replay after committed reply loss", committed.Load())
	}
	stored, err := store.GetAgentResult(t.Context(), installation, uuid.MustParse(f.epoch.NodeId), uuid.MustParse(command.CommandId), command.Epoch)
	if err != nil || !proto.Equal(stored, result) {
		t.Fatal("receipt changed during replay", err)
	}
	var count int
	if err := pool.QueryRow(t.Context(), "SELECT count(*) FROM command_results").Scan(&count); err != nil || count != 1 {
		t.Fatal("duplicate receipt", count, err)
	}
	ctx, cancel := context.WithTimeout(t.Context(), 5*time.Second)
	defer cancel()
	tick := time.NewTicker(10 * time.Millisecond)
	defer tick.Stop()
	for {
		record, err := f.e.journal.Get(command.CommandId)
		if err != nil {
			t.Fatal(err)
		}
		if record.Acknowledged {
			break
		}
		select {
		case <-ctx.Done():
			t.Fatal("replayed receipt was not durably acknowledged")
		case <-tick.C:
		}
	}
}

// dropTimeout bounds one test database teardown. It exists to stop a hang, not to
// assert how fast PostgreSQL drops a database under load.
const dropTimeout = 60 * time.Second
