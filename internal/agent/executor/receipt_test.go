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
	"github.com/sirrobot01/synod/internal/cluster"
	"github.com/sirrobot01/synod/internal/controlstream"
	"github.com/sirrobot01/synod/internal/operation"
	"github.com/sirrobot01/synod/internal/store/postgres"
	agentv1 "github.com/sirrobot01/synod/proto/agent/v1"
	"google.golang.org/protobuf/proto"
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
		ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
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
	admission, err := store.CreateCluster(t.Context(), postgres.CreateClusterRequest{Identity: postgres.RequestIdentity{InstallationID: installation, ActorID: "task044", APIOperation: postgres.CreateClusterAPIOperation, IdempotencyKey: uuid.NewString()}, RequestID: uuid.NewString(), Spec: cluster.CreateSpec{SchemaVersion: 1, Name: "receipt", Engine: cluster.PostgreSQL, PostgreSQL: &cluster.PostgreSQLSpec{Version: "17.11", Topology: "standalone"}}})
	if err != nil {
		t.Fatal(err)
	}
	f.epoch.ClusterId = admission.ClusterID.String()
	generation := uuid.MustParse(f.epoch.RecoveryGeneration)
	if err := store.InitializeGeneration(t.Context(), installation, generation); err != nil {
		t.Fatal(err)
	}
	claim, err := store.AcquireClaim(t.Context(), postgres.ClaimRequest{ClaimID: uuid.New(), InstallationID: installation, OperationID: admission.OperationID, RecoveryGeneration: generation, OwnerID: "task044", TTL: time.Minute, CoordinationKey: "/task044/leader", CoordinationLease: 1, CoordinationRevision: 1})
	if err != nil {
		t.Fatal(err)
	}
	command := volumeCommand(t, f.epoch)
	command.OperationId = admission.OperationID.String()
	command.Epoch.ExecutionEpoch = claim.ExecutionEpoch
	hash, err := operation.CommandHash(command)
	if err != nil {
		t.Fatal(err)
	}
	command.InputHash = hash[:]
	plan := &agentv1.ExecutionPlan{Version: 1, Steps: []*agentv1.PlanStep{{Command: command, MaxAttempts: 1, CancellationBoundary: agentv1.CancellationBoundary_CANCELLATION_BOUNDARY_BEFORE_EXECUTION, Postcondition: agentv1.Postcondition_POSTCONDITION_VOLUME_EXISTS}}}
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
	controller.send(t, &agentv1.ControllerMessage{Message: &agentv1.ControllerMessage_Command{Command: command}})
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
