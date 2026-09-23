package integration_test

import (
	"archive/tar"
	"bytes"
	"context"
	"encoding/json"
	"io"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"reflect"
	"slices"
	"strconv"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/containerd/errdefs"
	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"
	"github.com/moby/moby/client"
	"github.com/sirrobot01/synod/internal/agent/executor"
	"github.com/sirrobot01/synod/internal/api"
	"github.com/sirrobot01/synod/internal/auth"
	"github.com/sirrobot01/synod/internal/engine"
	postgresengine "github.com/sirrobot01/synod/internal/engine/postgres"
	"github.com/sirrobot01/synod/internal/operation"
	"github.com/sirrobot01/synod/internal/store/postgres"
	agentv1 "github.com/sirrobot01/synod/proto/agent/v1"
	"google.golang.org/protobuf/proto"
)

// postgresReplicaReadinessFault fails only after real native replica application access passes.
type postgresReplicaReadinessFault struct {
	engine.Prober
	refuse  atomic.Bool
	checked atomic.Bool
}

func (p *postgresReplicaReadinessFault) Probe(ctx context.Context, endpoint engine.Endpoint, password []byte, revision int64) engine.Observation {
	observed := p.Prober.Probe(ctx, endpoint, password, revision)
	if endpoint.PostgresRole == "replica" && endpoint.Role == "synod_app" && p.refuse.Load() {
		if observed.Healthy() {
			p.checked.Store(true)
		}
		observed.Ready = engine.Probe{State: engine.ProbeFailed, Reason: "fixture_replica_readiness_refused"}
	}
	return observed
}

func TestPostgresReplicaRetryReusesTheOriginalCopy(t *testing.T) {
	fault := &postgresReplicaReadinessFault{Prober: postgresengine.New()}
	fault.refuse.Store(true)
	h := startWithAgentOptions(t, agentTestOptions{postgresProber: fault}, 30*time.Second)
	h.awaitAgent(t, 30*time.Second)
	created := admit(t, h, "retry-postgres")
	if op := h.await(t, created.OperationID, 4*time.Minute); op.Status != operation.Succeeded {
		t.Fatalf("create: %s %s%s", op.Status, failure(op), h.receipts(t, created.OperationID))
	}
	primary := h.connect(t, created.ClusterID)
	defer primary.Close(context.WithoutCancel(t.Context()))
	config := primary.Config().Copy()
	config.TLSConfig = nil
	config.Fallbacks = nil
	if _, err := primary.Exec(t.Context(), `CREATE TABLE retry_data(value int); INSERT INTO retry_data VALUES(130)`); err != nil {
		t.Fatal(err)
	}
	token := "postgres-retry-fixture-private-api-token-value"
	verifier, err := auth.NewAdminToken(token, "integration")
	if err != nil {
		t.Fatal(err)
	}
	server, err := api.NewServer(h.store, verifier, h.installationID, nil, slog.New(slog.NewTextHandler(io.Discard, nil)))
	if err != nil {
		t.Fatal(err)
	}
	https := httptest.NewTLSServer(server)
	defer https.Close()
	scale := func() postgres.Admission {
		t.Helper()
		current, err := h.store.GetCluster(t.Context(), h.installationID, created.ClusterID)
		if err != nil {
			t.Fatal(err)
		}
		request, err := http.NewRequestWithContext(t.Context(), http.MethodPost, https.URL+"/v1/clusters/"+created.ClusterID.String()+"/actions/scale", strings.NewReader(`{"replicas":1}`))
		if err != nil {
			t.Fatal(err)
		}
		request.Header.Set("Authorization", "Bearer "+token)
		request.Header.Set("Content-Type", "application/json")
		request.Header.Set("Idempotency-Key", uuid.NewString())
		request.Header.Set("If-Match", strconv.FormatInt(current.DesiredRevision, 10))
		response, err := https.Client().Do(request)
		if err != nil {
			t.Fatal(err)
		}
		defer response.Body.Close()
		body, err := io.ReadAll(io.LimitReader(response.Body, 1<<20))
		if err != nil || response.StatusCode != http.StatusAccepted {
			t.Fatalf("scale HTTP %d: %s %v", response.StatusCode, body, err)
		}
		var admission postgres.Admission
		if err := json.Unmarshal(body, &admission); err != nil {
			t.Fatal(err)
		}
		return admission
	}
	added := scale()
	if op := h.await(t, added.OperationID, 4*time.Minute); op.Status != operation.Failed || !fault.checked.Load() {
		t.Fatalf("fault did not follow completed native copy: %s %s%s", op.Status, failure(op), h.receipts(t, added.OperationID))
	}
	members, err := h.store.ReadClusterMembers(t.Context(), h.installationID, created.ClusterID)
	if err != nil || len(members) != 2 {
		t.Fatalf("members: %+v %v", members, err)
	}
	var residual postgres.Member
	for _, member := range members {
		if member.Role == "replica" {
			residual = member
		}
	}
	if residual.State != "residual" {
		t.Fatalf("failed member: %+v", residual)
	}
	beforeHolds, err := h.store.ReadPlacementHolds(t.Context(), h.installationID)
	if err != nil {
		t.Fatal(err)
	}
	before, err := h.api.ContainerInspect(t.Context(), "synod-instance-"+residual.ResourceID.String(), client.ContainerInspectOptions{})
	if err != nil {
		t.Fatal(err)
	}
	readReceipt := func() []byte {
		t.Helper()
		copy, err := h.api.CopyFromContainer(t.Context(), before.Container.ID, client.CopyFromContainerOptions{SourcePath: postgresengine.ReplicaReceiptFile})
		if err != nil {
			t.Fatal(err)
		}
		defer copy.Content.Close()
		archive := tar.NewReader(copy.Content)
		if _, err := archive.Next(); err != nil {
			t.Fatal(err)
		}
		record, err := io.ReadAll(io.LimitReader(archive, 1024))
		if err != nil {
			t.Fatal(err)
		}
		return record
	}
	receipt := readReceipt()
	var originalBytes []byte
	if err := h.pool.QueryRow(t.Context(), `SELECT plan_bytes FROM operations WHERE id=$1`, added.OperationID).Scan(&originalBytes); err != nil {
		t.Fatal(err)
	}
	original := new(agentv1.ExecutionPlan)
	if err := proto.Unmarshal(originalBytes, original); err != nil {
		t.Fatal(err)
	}
	if len(original.Steps) != 9 || !bytes.Contains(receipt, []byte(original.Steps[4].Command.CommandId)) {
		t.Fatal("native receipt does not bind the original clone")
	}
	fault.refuse.Store(false)
	retried := scale()
	op := h.await(t, retried.OperationID, 4*time.Minute)
	if op.Status != operation.Succeeded || op.RetryOf == nil || *op.RetryOf != added.OperationID {
		t.Fatalf("retry: %s %s%s", op.Status, failure(op), h.receipts(t, retried.OperationID))
	}
	after, err := h.api.ContainerInspect(t.Context(), before.Container.ID, client.ContainerInspectOptions{})
	if err != nil || after.Container.ID != before.Container.ID || !reflect.DeepEqual(before.Container.Mounts, after.Container.Mounts) || !bytes.Equal(receipt, readReceipt()) {
		t.Fatalf("retry replaced copy or container: %v", err)
	}
	var retriedBytes []byte
	if err := h.pool.QueryRow(t.Context(), `SELECT plan_bytes FROM operations WHERE id=$1`, retried.OperationID).Scan(&retriedBytes); err != nil {
		t.Fatal(err)
	}
	plan := new(agentv1.ExecutionPlan)
	if err := proto.Unmarshal(retriedBytes, plan); err != nil {
		t.Fatal(err)
	}
	if len(plan.Steps) != 6 || plan.Steps[1].Command.Input.GetVerifyPostgresReplicaClone().GetCloneCommandId() != original.Steps[4].Command.CommandId {
		t.Fatal("retry did not verify the original clone")
	}
	for _, step := range plan.Steps {
		if step.Command.Input.GetClonePostgresReplica() != nil || step.Command.Input.GetEnsureVolume() != nil || step.Command.Input.GetEnsureContainer() != nil {
			t.Fatal("retry can replace the native copy")
		}
	}
	afterHolds, err := h.store.ReadPlacementHolds(t.Context(), h.installationID)
	if err != nil || len(beforeHolds) != len(afterHolds) {
		t.Fatalf("retry changed allocation: %v", err)
	}
	for i, hold := range beforeHolds {
		now := afterHolds[i]
		if hold.ReservationID != now.ReservationID || hold.ResourceID != now.ResourceID || hold.NodeID != now.NodeID || hold.OperationID != now.OperationID || hold.Budget != now.Budget || !slices.Equal(hold.Ports, now.Ports) {
			t.Fatal("retry changed reserved resources")
		}
	}
	afterMembers, err := h.store.ReadClusterMembers(t.Context(), h.installationID, created.ClusterID)
	if err != nil || len(afterMembers) != len(members) {
		t.Fatalf("retry changed membership: %v", err)
	}
	for i, member := range afterMembers {
		if member.MemberID != members[i].MemberID || member.ResourceID != members[i].ResourceID || member.State != "active" {
			t.Fatal("retry replaced or failed to activate member")
		}
	}
	config.Port = h.memberPort(t, created.ClusterID, "replica")
	replica, err := pgx.ConnectConfig(t.Context(), config)
	if err != nil {
		t.Fatal(err)
	}
	defer replica.Close(context.WithoutCancel(t.Context()))
	var value, port int
	var recovery bool
	if err := replica.QueryRow(t.Context(), `SELECT value,current_setting('port')::int,pg_is_in_recovery() FROM retry_data`).Scan(&value, &port, &recovery); err != nil || value != 130 || port != int(config.Port) || !recovery {
		t.Fatalf("restored replica: value=%d port=%d recovery=%v %v", value, port, recovery, err)
	}
	if _, err := replica.Exec(t.Context(), `INSERT INTO retry_data VALUES(999)`); err == nil {
		t.Fatal("replica accepted a write")
	}
	if _, err := primary.Exec(t.Context(), `INSERT INTO retry_data VALUES(131)`); err != nil {
		t.Fatal(err)
	}
	deadline := time.Now().Add(30 * time.Second)
	for {
		var sum int
		err := replica.QueryRow(t.Context(), `SELECT sum(value) FROM retry_data`).Scan(&sum)
		if err == nil && sum == 261 {
			break
		}
		if time.Now().After(deadline) {
			t.Fatalf("streaming after retry: %d %v", sum, err)
		}
		time.Sleep(100 * time.Millisecond)
	}
}

func TestPostgresReplicaRetryAfterSecretRefusalStartsItsFirstCopy(t *testing.T) {
	var refuse atomic.Bool
	refuse.Store(true)
	h := startWithAgentOptions(t, agentTestOptions{beforeCommand: func(ctx context.Context, e *executor.Executor, command *agentv1.Command) error {
		if command.Input.GetClonePostgresReplica() == nil || !refuse.Load() {
			return nil
		}
		// Replace only session material. The durable command and native runtime stay unchanged.
		_, err := e.Handle(ctx, &agentv1.ControllerMessage{Message: &agentv1.ControllerMessage_Secret{Secret: &agentv1.SecretMaterial{
			CredentialId: uuid.NewString(), Version: 1, Purpose: "application", Material: []byte("fixture-wrong-credential-identity"),
		}}})
		return err
	}}, 30*time.Second)
	h.awaitAgent(t, 30*time.Second)
	created := admit(t, h, "retry-before-copy")
	if op := h.await(t, created.OperationID, 4*time.Minute); op.Status != operation.Succeeded {
		t.Fatalf("create: %s %s", op.Status, failure(op))
	}
	primary := h.connect(t, created.ClusterID)
	defer primary.Close(context.WithoutCancel(t.Context()))
	config := primary.Config().Copy()
	config.TLSConfig = nil
	config.Fallbacks = nil
	if _, err := primary.Exec(t.Context(), `CREATE TABLE first_copy(value int); INSERT INTO first_copy VALUES(131)`); err != nil {
		t.Fatal(err)
	}
	added := h.scale(t, created.ClusterID, 1, nil)
	if op := h.await(t, added.OperationID, 4*time.Minute); op.Status != operation.Failed || failure(op) != "secret_unavailable" {
		t.Fatalf("secret fault: %s %s%s", op.Status, failure(op), h.receipts(t, added.OperationID))
	}
	var originalBytes []byte
	if err := h.pool.QueryRow(t.Context(), `SELECT plan_bytes FROM operations WHERE id=$1`, added.OperationID).Scan(&originalBytes); err != nil {
		t.Fatal(err)
	}
	original := new(agentv1.ExecutionPlan)
	if err := proto.Unmarshal(originalBytes, original); err != nil {
		t.Fatal(err)
	}
	if len(original.Steps) != 9 {
		t.Fatal("missing original copy plan")
	}
	clone := original.Steps[4].Command
	result, err := h.store.GetAgentResult(t.Context(), h.installationID, h.nodeID, uuid.MustParse(clone.CommandId), clone.Epoch)
	if err != nil || result.State != "failed" || result.FailureCode != "secret_unavailable" || len(result.Effects) != 0 {
		t.Fatalf("not a proved no-effect refusal: %+v %v", result, err)
	}
	before, err := h.api.ContainerInspect(t.Context(), "synod-instance-"+clone.Input.ResourceId, client.ContainerInspectOptions{})
	if err != nil {
		t.Fatal(err)
	}
	missing, err := h.api.CopyFromContainer(t.Context(), before.Container.ID, client.CopyFromContainerOptions{SourcePath: postgresengine.ReplicaReceiptFile})
	if err == nil {
		missing.Content.Close()
	}
	if !errdefs.IsNotFound(err) {
		t.Fatalf("refused copy already has a native receipt: %v", err)
	}
	refuse.Store(false)
	retried := h.scale(t, created.ClusterID, 1, nil)
	op := h.await(t, retried.OperationID, 4*time.Minute)
	if op.Status != operation.Succeeded || op.RetryOf == nil || *op.RetryOf != added.OperationID {
		t.Fatalf("first-copy retry: %s %s%s", op.Status, failure(op), h.receipts(t, retried.OperationID))
	}
	var retriedBytes []byte
	if err := h.pool.QueryRow(t.Context(), `SELECT plan_bytes FROM operations WHERE id=$1`, retried.OperationID).Scan(&retriedBytes); err != nil {
		t.Fatal(err)
	}
	plan := new(agentv1.ExecutionPlan)
	if err := proto.Unmarshal(retriedBytes, plan); err != nil {
		t.Fatal(err)
	}
	if len(plan.Steps) != 9 || plan.Steps[4].Command.Input.GetClonePostgresReplica() == nil || plan.Steps[4].Command.CommandId == clone.CommandId {
		t.Fatal("no-effect retry did not bind a first copy to a new command")
	}
	for i, step := range plan.Steps {
		if !proto.Equal(step.Command.Input, original.Steps[i].Command.Input) {
			t.Fatalf("retry changed step %d inputs", i)
		}
	}
	after, err := h.api.ContainerInspect(t.Context(), before.Container.ID, client.ContainerInspectOptions{})
	if err != nil || !reflect.DeepEqual(before.Container.Mounts, after.Container.Mounts) {
		t.Fatalf("retry replaced container or volume: %v", err)
	}
	copied, err := h.api.CopyFromContainer(t.Context(), before.Container.ID, client.CopyFromContainerOptions{SourcePath: postgresengine.ReplicaReceiptFile})
	if err != nil {
		t.Fatal(err)
	}
	defer copied.Content.Close()
	archive := tar.NewReader(copied.Content)
	if _, err := archive.Next(); err != nil {
		t.Fatal(err)
	}
	receipt, err := io.ReadAll(io.LimitReader(archive, 1024))
	if err != nil || !bytes.Contains(receipt, []byte(plan.Steps[4].Command.CommandId)) || bytes.Contains(receipt, []byte(clone.CommandId)) {
		t.Fatalf("native receipt does not bind the new first copy: %v", err)
	}
	config.Port = h.memberPort(t, created.ClusterID, "replica")
	replica, err := pgx.ConnectConfig(t.Context(), config)
	if err != nil {
		t.Fatal(err)
	}
	defer replica.Close(context.WithoutCancel(t.Context()))
	var value, port int
	var recovery bool
	if err := replica.QueryRow(t.Context(), `SELECT value,current_setting('port')::int,pg_is_in_recovery() FROM first_copy`).Scan(&value, &port, &recovery); err != nil || value != 131 || port != int(config.Port) || !recovery {
		t.Fatalf("first copy: %d port=%d recovery=%v %v", value, port, recovery, err)
	}
	if _, err := primary.Exec(t.Context(), `INSERT INTO first_copy VALUES(132)`); err != nil {
		t.Fatal(err)
	}
	deadline := time.Now().Add(30 * time.Second)
	for {
		var sum int
		err := replica.QueryRow(t.Context(), `SELECT sum(value) FROM first_copy`).Scan(&sum)
		if err == nil && sum == 263 {
			break
		}
		if time.Now().After(deadline) {
			t.Fatalf("streaming after first copy: %d %v", sum, err)
		}
		time.Sleep(100 * time.Millisecond)
	}
}
