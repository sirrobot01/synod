package integration_test

import (
	"context"
	"encoding/pem"
	"errors"
	"io"
	"log/slog"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"
	"github.com/sirrobot01/synod/internal/api"
	"github.com/sirrobot01/synod/internal/auth"
	synodclient "github.com/sirrobot01/synod/internal/client"
	"github.com/sirrobot01/synod/internal/cluster"
	"github.com/sirrobot01/synod/internal/engine"
	postgresengine "github.com/sirrobot01/synod/internal/engine/postgres"
	"github.com/sirrobot01/synod/internal/operation"
	"github.com/sirrobot01/synod/internal/store/postgres"
)

func TestPublicPostgresSynchronousPolicyWaitsForReplica(t *testing.T) {
	h := start(t, 30*time.Second)
	h.awaitAgent(t, 30*time.Second)
	token := "synchronous-policy-fixture-private-api-token-value"
	verifier, err := auth.NewAdminToken(token, "integration")
	if err != nil {
		t.Fatal(err)
	}
	catalog, err := engine.NewCatalog(postgresengine.New())
	if err != nil {
		t.Fatal(err)
	}
	server, err := api.NewServer(h.store, verifier, h.installationID, catalog, slog.New(slog.NewTextHandler(io.Discard, nil)))
	if err != nil {
		t.Fatal(err)
	}
	https := httptest.NewTLSServer(server)
	defer https.Close()
	dir := t.TempDir()
	tokenFile, caFile := filepath.Join(dir, "token"), filepath.Join(dir, "ca.pem")
	if err := os.WriteFile(tokenFile, []byte(token), 0o600); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(caFile, pem.EncodeToMemory(&pem.Block{Type: "CERTIFICATE", Bytes: https.Certificate().Raw}), 0o600); err != nil {
		t.Fatal(err)
	}
	apiClient, err := synodclient.New(synodclient.Config{Endpoint: https.URL, TokenFile: tokenFile, CAFile: caFile, RequestTimeout: 10 * time.Second, Attempts: 1})
	if err != nil {
		t.Fatal(err)
	}
	spec := cluster.CreateSpec{SchemaVersion: 1, Name: "synchronous-postgres", Engine: cluster.PostgreSQL, PostgreSQL: &cluster.PostgreSQLSpec{Version: "17.11", Topology: "standalone", ReplicationPolicy: "synchronous"}}
	key := uuid.NewString()
	created, err := apiClient.Create(t.Context(), spec, key)
	if err != nil {
		t.Fatal(err)
	}
	replay, err := apiClient.Create(t.Context(), spec, key)
	if err != nil || !replay.Replayed || replay.OperationID != created.OperationID {
		t.Fatalf("public policy replay: %+v %v", replay, err)
	}
	spec.PostgreSQL.ReplicationPolicy = "asynchronous"
	_, err = apiClient.Create(t.Context(), spec, key)
	if failure, ok := errors.AsType[*synodclient.Error](err); !ok || failure.Code != "idempotency_conflict" {
		t.Fatalf("changed policy replay: %v", err)
	}
	spec.PostgreSQL.ReplicationPolicy = "invalid"
	_, err = apiClient.Create(t.Context(), spec, uuid.NewString())
	if failure, ok := errors.AsType[*synodclient.Error](err); !ok || failure.Code != "invalid_request" {
		t.Fatalf("invalid policy: %v", err)
	}
	spec.PostgreSQL.ReplicationPolicy, spec.PostgreSQL.Version = "synchronous", "18.6"
	_, err = apiClient.Create(t.Context(), spec, uuid.NewString())
	if failure, ok := errors.AsType[*synodclient.Error](err); !ok || failure.Code != "unsupported_spec" {
		t.Fatalf("unimplemented synchronous version: %v", err)
	}
	if op := h.await(t, created.OperationID, 4*time.Minute); op.Status != operation.Succeeded {
		t.Fatalf("create: %s %s%s", op.Status, failure(op), h.receipts(t, created.OperationID))
	}
	current, err := apiClient.GetCluster(t.Context(), created.ClusterID)
	if err != nil || current.Spec.PostgreSQL.ReplicationPolicy != "synchronous" || current.DesiredReplicas != 0 {
		t.Fatalf("stored policy: %+v %v", current, err)
	}
	writer := h.connect(t, created.ClusterID)
	defer writer.Close(context.WithoutCancel(t.Context()))
	observer := h.connect(t, created.ClusterID)
	defer observer.Close(context.WithoutCancel(t.Context()))
	writerConfig := writer.Config().Copy()
	writerConfig.TLSConfig, writerConfig.Fallbacks = nil, nil
	writerPID := writer.PgConn().PID()
	ctx, cancel := context.WithTimeout(t.Context(), 4*time.Minute)
	defer cancel()
	done := make(chan error, 1)
	go func() {
		_, err := writer.Exec(ctx, `CREATE TABLE synchronous_rows(value int); INSERT INTO synchronous_rows VALUES(138)`)
		done <- err
	}()
	waiting, cancelWaiting := context.WithTimeout(ctx, 10*time.Second)
	defer cancelWaiting()
	for {
		var blocked bool
		if err := observer.QueryRow(waiting, `SELECT EXISTS(SELECT 1 FROM pg_stat_activity WHERE pid=$1 AND wait_event='SyncRep')`, writerPID).Scan(&blocked); err != nil {
			t.Fatal(err)
		}
		if blocked {
			break
		}
		select {
		case err := <-done:
			t.Fatalf("synchronous write returned without replica: %v", err)
		default:
		}
		time.Sleep(25 * time.Millisecond)
	}
	added, err := apiClient.Scale(t.Context(), created.ClusterID, 1, current.DesiredRevision, uuid.NewString(), nil)
	if err != nil {
		t.Fatal(err)
	}
	if op := h.await(t, added.OperationID, 4*time.Minute); op.Status != operation.Succeeded {
		t.Fatalf("add replica: %s %s%s", op.Status, failure(op), h.receipts(t, added.OperationID))
	}
	select {
	case err := <-done:
		if err != nil {
			t.Fatal(err)
		}
	case <-ctx.Done():
		t.Fatal("write did not complete after replica joined")
	}
	replicaConfig := observer.Config().Copy()
	replicaConfig.TLSConfig, replicaConfig.Fallbacks = nil, nil
	replicaConfig.Port = h.memberPort(t, created.ClusterID, "replica")
	replica, err := pgx.ConnectConfig(t.Context(), replicaConfig)
	if err != nil {
		t.Fatal(err)
	}
	defer replica.Close(context.WithoutCancel(t.Context()))
	var value int
	if err := replica.QueryRow(t.Context(), `SELECT sum(value)::int FROM synchronous_rows`).Scan(&value); err != nil || value != 138 {
		t.Fatalf("copied acknowledged data: %d %v", value, err)
	}
	preview, err := apiClient.PlanScaleDown(t.Context(), created.ClusterID)
	if err != nil || !strings.Contains(preview.Downtime, "Commits wait") {
		t.Fatalf("removal hid write availability: %+v %v", preview, err)
	}
	writer.Close(context.WithoutCancel(t.Context()))
	observer.Close(context.WithoutCancel(t.Context()))
	replica.Close(context.WithoutCancel(t.Context()))
	h.actAndAwait(t, created.ClusterID, postgres.StopClusterAPIOperation)
	h.actAndAwait(t, created.ClusterID, postgres.StartClusterAPIOperation)
	// Restart retains the stored policy and both identities.
	writer, err = pgx.ConnectConfig(t.Context(), writerConfig)
	if err != nil {
		t.Fatal(err)
	}
	defer writer.Close(context.WithoutCancel(t.Context()))
	commitCtx, cancelCommit := context.WithTimeout(t.Context(), 10*time.Second)
	defer cancelCommit()
	if _, err := writer.Exec(commitCtx, `INSERT INTO synchronous_rows VALUES(139)`); err != nil {
		t.Fatalf("write after policy restart: %v", err)
	}
	var names string
	if err := writer.QueryRow(t.Context(), `SHOW synchronous_standby_names`).Scan(&names); err != nil || !strings.Contains(names, created.ClusterID.String()) {
		t.Fatalf("restart lost synchronous names: %q %v", names, err)
	}
}
