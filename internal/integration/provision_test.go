package integration_test

import (
	"context"
	"errors"
	"fmt"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgconn"
	"github.com/moby/moby/client"
	"github.com/sirrobot01/synod/internal/cluster"
	"github.com/sirrobot01/synod/internal/operation"
	"github.com/sirrobot01/synod/internal/secret"
	"github.com/sirrobot01/synod/internal/store/postgres"
)

// A cluster admitted through the real admission path becomes a database an ordinary
// client can connect to. This is acceptance A04, and nothing short of it proves the
// path: every other test in the tree exercises one seam.
func TestAdmittedClusterBecomesAConnectableDatabase(t *testing.T) {
	h := start(t)
	h.awaitAgent(t, 30*time.Second)
	admission := admit(t, h, "db")

	op := h.await(t, admission.OperationID, 4*time.Minute)
	if op.Status != operation.Succeeded {
		t.Fatalf("operation %s finished %s: %s", op.ID, op.Status, failure(op))
	}

	// The credential the controller sealed is the credential the engine took.
	conn := h.connect(t, admission.ClusterID)
	defer conn.Close(context.WithoutCancel(t.Context()))

	// Rows through normal client access, which is the acceptance condition rather
	// than a successful handshake.
	if _, err := conn.Exec(t.Context(), `CREATE TABLE acceptance (id integer primary key, note text)`); err != nil {
		t.Fatal(err)
	}
	if _, err := conn.Exec(t.Context(), `INSERT INTO acceptance (id,note) VALUES (1,'written through the published endpoint')`); err != nil {
		t.Fatal(err)
	}
	var note string
	if err := conn.QueryRow(t.Context(), `SELECT note FROM acceptance WHERE id=1`).Scan(&note); err != nil {
		t.Fatal(err)
	}
	if note != "written through the published endpoint" {
		t.Fatalf("note=%q, want the row that was written", note)
	}
	for _, sql := range []string{`CREATE ROLE forbidden`, `CREATE DATABASE forbidden`, `SELECT pg_read_file('postgresql.conf')`} {
		_, err := conn.Exec(t.Context(), sql)
		if denied, ok := errors.AsType[*pgconn.PgError](err); !ok || denied.Code != "42501" {
			t.Fatalf("application administration returned %v, want insufficient privilege", err)
		}
	}
}

// A repeated create is one cluster and one database, however many times it is sent.
func TestDuplicateCreateProducesOneDatabase(t *testing.T) {
	h := start(t)
	h.awaitAgent(t, 30*time.Second)
	key := uuid.NewString()
	first := admitWithKey(t, h, "db", key)
	second := admitWithKey(t, h, "db", key)
	if first.ClusterID != second.ClusterID || first.OperationID != second.OperationID {
		t.Fatalf("first=%+v second=%+v, want one admission", first, second)
	}
	if !second.Replayed {
		t.Fatal("the repeat was not reported as a replay")
	}
	op := h.await(t, first.OperationID, 4*time.Minute)
	if op.Status != operation.Succeeded {
		t.Fatalf("operation %s finished %s: %s", op.ID, op.Status, failure(op))
	}
	// One instance container and one volume, not two of each.
	containers, volumes := h.owned(t)
	if containers != 1 || volumes != 1 {
		t.Fatalf("containers=%d volumes=%d, want exactly one of each", containers, volumes)
	}
}

// admit creates one cluster through the store's admission path under a fresh key.
func admit(t *testing.T, h *harness, name string) postgres.Admission {
	t.Helper()
	return admitWithKey(t, h, name, uuid.NewString())
}

func admitWithKey(t *testing.T, h *harness, name, key string) postgres.Admission {
	t.Helper()
	admission, err := h.store.CreateCluster(t.Context(), postgres.CreateClusterRequest{
		Identity: postgres.RequestIdentity{
			InstallationID: h.installationID, ActorID: "integration",
			APIOperation: postgres.CreateClusterAPIOperation, IdempotencyKey: key,
		},
		RequestID: uuid.NewString(),
		Spec: cluster.CreateSpec{
			SchemaVersion: 1, Name: name, Engine: cluster.PostgreSQL,
			PostgreSQL: &cluster.PostgreSQLSpec{Version: "17.11", Topology: "standalone"},
		},
	})
	if err != nil {
		t.Fatal(err)
	}
	return admission
}

// await waits for one operation to finish and reports what it finished as.
func (h *harness) await(t *testing.T, operationID uuid.UUID, limit time.Duration) operation.Operation {
	t.Helper()
	deadline := time.Now().Add(limit)
	var last operation.Operation
	for time.Now().Before(deadline) {
		found, err := h.store.GetOperation(t.Context(), h.installationID, operationID)
		if err != nil {
			t.Fatal(err)
		}
		last = found
		switch found.Status {
		case operation.Succeeded, operation.Failed, operation.Cancelled:
			return found
		}
		time.Sleep(250 * time.Millisecond)
	}
	t.Fatalf("operation %s was still %s after %s", operationID, last.Status, limit)
	return last
}

// connect opens a client session with the credential this installation sealed.
func (h *harness) connect(t *testing.T, clusterID uuid.UUID) *pgx.Conn {
	t.Helper()
	credential, err := h.store.ReadClusterCredential(t.Context(), h.installationID, clusterID, postgres.ApplicationCredential)
	if err != nil {
		t.Fatal(err)
	}
	password, err := h.ring.Open(secret.Binding{
		InstallationID: h.installationID, OwnerType: "cluster", OwnerID: clusterID,
		CredentialID: credential.CredentialID, Version: credential.Version,
	}, credential.Envelope)
	if err != nil {
		t.Fatal(err)
	}
	holds, err := h.store.ReadPlacementHolds(t.Context(), h.installationID)
	if err != nil {
		t.Fatal(err)
	}
	var port uint16
	var endpoints int
	for _, hold := range holds {
		if hold.ClusterID != clusterID || hold.NodeID != h.nodeID {
			continue
		}
		for _, binding := range hold.Ports {
			if binding.Transport == "tcp" {
				port = binding.Number
				endpoints++
			}
		}
	}
	if endpoints != 1 || port == 0 {
		t.Fatalf("cluster has %d reserved TCP endpoints, want one", endpoints)
	}
	config, err := pgx.ParseConfig(fmt.Sprintf("postgres://%s@127.0.0.1:%d/app", credential.RoleName, port))
	if err != nil {
		t.Fatal(err)
	}
	config.Password = string(password)
	config.ConnectTimeout = 10 * time.Second
	ctx, cancel := context.WithTimeout(t.Context(), 20*time.Second)
	defer cancel()
	conn, err := pgx.ConnectConfig(ctx, config)
	if err != nil {
		t.Fatalf("connect to the provisioned database: %v", err)
	}
	return conn
}

// owned counts the containers and volumes this installation created.
func (h *harness) owned(t *testing.T) (containers, volumes int) {
	t.Helper()
	filters := client.Filters{}.Add("label", "io.synod.installation="+h.installationID.String())
	found, err := h.api.ContainerList(t.Context(), client.ContainerListOptions{All: true, Filters: filters})
	if err != nil {
		t.Fatal(err)
	}
	stored, err := h.api.VolumeList(t.Context(), client.VolumeListOptions{Filters: filters})
	if err != nil {
		t.Fatal(err)
	}
	return len(found.Items), len(stored.Items)
}

// removeNativeObjects takes down what the agent created. Synod retains data on
// delete by design, so a test that creates real storage removes it itself.
func (h *harness) removeNativeObjects(t *testing.T) {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.WithoutCancel(t.Context()), 60*time.Second)
	defer cancel()
	filters := client.Filters{}.Add("label", "io.synod.installation="+h.installationID.String())
	containers, err := h.api.ContainerList(ctx, client.ContainerListOptions{All: true, Filters: filters})
	if err != nil {
		t.Error(err)
		return
	}
	for _, item := range containers.Items {
		if item.Labels["io.synod.installation"] != h.installationID.String() {
			t.Fatal("cleanup ownership mismatch")
		}
		if _, err := h.api.ContainerRemove(ctx, item.ID, client.ContainerRemoveOptions{Force: true}); err != nil {
			t.Error(err)
		}
	}
	volumes, err := h.api.VolumeList(ctx, client.VolumeListOptions{Filters: filters})
	if err != nil {
		t.Error(err)
		return
	}
	for _, item := range volumes.Items {
		if item.Labels["io.synod.installation"] != h.installationID.String() {
			t.Fatal("cleanup ownership mismatch")
		}
		if _, err := h.api.VolumeRemove(ctx, item.Name, client.VolumeRemoveOptions{Force: true}); err != nil {
			t.Error(err)
		}
	}
}

// failure renders why an operation stopped, or says it gave no reason.
func failure(op operation.Operation) string {
	if op.FailureCode == nil {
		return "no failure code"
	}
	return *op.FailureCode
}
