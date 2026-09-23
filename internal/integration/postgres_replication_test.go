package integration_test

import (
	"bytes"
	"context"
	"encoding/json"
	"io"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"strconv"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"
	"github.com/moby/moby/client"
	"github.com/sirrobot01/synod/internal/api"
	"github.com/sirrobot01/synod/internal/auth"
	"github.com/sirrobot01/synod/internal/cluster"
	"github.com/sirrobot01/synod/internal/operation"
	"github.com/sirrobot01/synod/internal/secret"
	"github.com/sirrobot01/synod/internal/store/postgres"
)

func TestPostgresReplicaCopiesStreamsRestartsAndRemoves(t *testing.T) {
	for _, promoted := range []bool{false, true} {
		t.Run(map[bool]string{false: "remove", true: "refuse_promoted"}[promoted], func(t *testing.T) {
			h := start(t, 30*time.Second)
			h.awaitAgent(t, 30*time.Second)
			created := admit(t, h, "replicated-postgres")
			if op := h.await(t, created.OperationID, 4*time.Minute); op.Status != operation.Succeeded {
				t.Fatalf("create: %s %s%s", op.Status, failure(op), h.receipts(t, created.OperationID))
			}
			primary := h.connect(t, created.ClusterID)
			defer primary.Close(context.WithoutCancel(t.Context()))
			primaryConfig := primary.Config().Copy()
			// This fixture uses plaintext database endpoints. Remove pgx fallback ports before selecting a member.
			primaryConfig.TLSConfig = nil
			primaryConfig.Fallbacks = nil
			if _, err := primary.Exec(t.Context(), `CREATE TABLE replica_data(value int); INSERT INTO replica_data VALUES (128)`); err != nil {
				t.Fatal(err)
			}
			before, err := h.store.ReadClusterMembers(t.Context(), h.installationID, created.ClusterID)
			if err != nil || len(before) != 1 {
				t.Fatalf("primary: %+v %v", before, err)
			}
			credential, err := h.store.ReadClusterCredential(t.Context(), h.installationID, created.ClusterID, postgres.ApplicationCredential)
			if err != nil {
				t.Fatal(err)
			}
			token := "postgres-replica-fixture-private-api-token-value"
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
			scale := func(count int, confirmation *cluster.ActionConfirmation) postgres.Admission {
				t.Helper()
				current, err := h.store.GetCluster(t.Context(), h.installationID, created.ClusterID)
				if err != nil {
					t.Fatal(err)
				}
				body, err := json.Marshal(struct {
					Replicas int `json:"replicas"`
					*cluster.ActionConfirmation
				}{count, confirmation})
				if err != nil {
					t.Fatal(err)
				}
				request, err := http.NewRequestWithContext(t.Context(), http.MethodPost, https.URL+"/v1/clusters/"+created.ClusterID.String()+"/actions/scale", bytes.NewReader(body))
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
				encoded, err := io.ReadAll(io.LimitReader(response.Body, 1<<20))
				if err != nil || response.StatusCode != http.StatusAccepted {
					t.Fatalf("scale HTTP %d: %s %v", response.StatusCode, encoded, err)
				}
				var admitted postgres.Admission
				if err := json.Unmarshal(encoded, &admitted); err != nil {
					t.Fatal(err)
				}
				return admitted
			}
			added := scale(1, nil)
			if op := h.await(t, added.OperationID, 4*time.Minute); op.Status != operation.Succeeded {
				t.Fatalf("replica add: %s %s%s", op.Status, failure(op), h.receipts(t, added.OperationID))
			}
			members, err := h.store.ReadClusterMembers(t.Context(), h.installationID, created.ClusterID)
			if err != nil || len(members) != 2 {
				t.Fatalf("members: %+v %v", members, err)
			}
			var replicaMember postgres.Member
			for _, member := range members {
				if member.State != "active" {
					t.Fatalf("member was not activated: %+v", member)
				}
				if member.Role == "replica" {
					replicaMember = member
				}
				if member.Role == "primary" && member.MemberID != before[0].MemberID {
					t.Fatal("replication replaced the primary")
				}
			}
			if replicaMember.MemberID == uuid.Nil {
				t.Fatal("no replica member")
			}
			afterCredential, err := h.store.ReadClusterCredential(t.Context(), h.installationID, created.ClusterID, postgres.ApplicationCredential)
			if err != nil || afterCredential.CredentialID != credential.CredentialID || afterCredential.Version != credential.Version {
				t.Fatal("replication replaced the application credential")
			}
			replicaConfig := primaryConfig.Copy()
			replicaConfig.Port = h.memberPort(t, created.ClusterID, "replica")
			replica, err := pgx.ConnectConfig(t.Context(), replicaConfig)
			if err != nil {
				t.Fatal(err)
			}
			defer replica.Close(context.WithoutCancel(t.Context()))
			var serverPort int
			var inRecovery bool
			if err := replica.QueryRow(t.Context(), `SELECT current_setting('port')::int,pg_is_in_recovery()`).Scan(&serverPort, &inRecovery); err != nil || serverPort != int(replicaConfig.Port) || !inRecovery {
				t.Fatalf("replica endpoint mismatch: port=%d expected=%d recovery=%v err=%v", serverPort, replicaConfig.Port, inRecovery, err)
			}
			var value int
			if err := replica.QueryRow(t.Context(), `SELECT value FROM replica_data`).Scan(&value); err != nil || value != 128 {
				t.Fatalf("copy lost existing data: %d %v", value, err)
			}
			if _, err := replica.Exec(t.Context(), `INSERT INTO replica_data VALUES (999)`); err == nil {
				t.Fatal("replica accepted a write")
			}
			if _, err := primary.Exec(t.Context(), `INSERT INTO replica_data VALUES (129)`); err != nil {
				t.Fatal(err)
			}
			deadline := time.Now().Add(30 * time.Second)
			for {
				var count int
				err := replica.QueryRow(t.Context(), `SELECT count(*) FROM replica_data WHERE value=129`).Scan(&count)
				if err == nil && count == 1 {
					break
				}
				if time.Now().After(deadline) {
					t.Fatalf("later write did not stream: %d %v", count, err)
				}
				time.Sleep(100 * time.Millisecond)
			}
			primary.Close(context.WithoutCancel(t.Context()))
			replica.Close(context.WithoutCancel(t.Context()))
			h.actAndAwait(t, created.ClusterID, postgres.StopClusterAPIOperation)
			h.actAndAwait(t, created.ClusterID, postgres.StartClusterAPIOperation)
			primary, err = pgx.ConnectConfig(t.Context(), primaryConfig)
			if err != nil {
				t.Fatal(err)
			}
			defer primary.Close(context.WithoutCancel(t.Context()))
			replica, err = pgx.ConnectConfig(t.Context(), replicaConfig)
			if err != nil {
				t.Fatal(err)
			}
			defer replica.Close(context.WithoutCancel(t.Context()))
			for _, conn := range []*pgx.Conn{primary, replica} {
				if err := conn.QueryRow(t.Context(), `SELECT sum(value)::int FROM replica_data`).Scan(&value); err != nil || value != 257 {
					t.Fatalf("restart lost data: %d %v", value, err)
				}
			}
			if _, err := replica.Exec(t.Context(), `INSERT INTO replica_data VALUES (999)`); err == nil {
				t.Fatal("restarted replica accepted a write")
			}
			preview, err := h.store.PreviewClusterScaleDown(t.Context(), h.installationID, created.ClusterID, "integration")
			if err != nil || preview.MemberID != replicaMember.MemberID {
				t.Fatalf("removal target: %+v %v", preview, err)
			}

			if promoted {
				admin, err := h.store.ReadClusterCredential(t.Context(), h.installationID, created.ClusterID, postgres.SuperuserCredential)
				if err != nil {
					t.Fatal(err)
				}
				password, err := h.ring.Open(secret.Binding{InstallationID: h.installationID, OwnerType: "cluster", OwnerID: created.ClusterID, CredentialID: admin.CredentialID, Version: admin.Version}, admin.Envelope)
				if err != nil {
					t.Fatal(err)
				}
				config := replicaConfig.Copy()
				config.User, config.Password, config.Database = admin.RoleName, string(password), "postgres"
				conn, err := pgx.ConnectConfig(t.Context(), config)
				if err != nil {
					t.Fatal(err)
				}
				var completed bool
				err = conn.QueryRow(t.Context(), `SELECT pg_catalog.pg_promote(true,10)`).Scan(&completed)
				conn.Close(context.WithoutCancel(t.Context()))
				clear(password)
				if err != nil || !completed {
					t.Fatalf("fixture promotion failed: %v", err)
				}
			}
			removed := scale(0, &cluster.ActionConfirmation{PlanToken: preview.PlanToken, Confirm: true, DataDisposition: "retain"})

			if promoted {
				op := h.await(t, removed.OperationID, 4*time.Minute)
				if op.Status != operation.Failed || failure(op) != "postgres_role_mismatch" {
					t.Fatalf("promoted member removal: %s %s%s", op.Status, failure(op), h.receipts(t, removed.OperationID))
				}
				var dispatched int
				if err := h.pool.QueryRow(t.Context(), `SELECT count(*) FROM command_dispatches WHERE operation_id=$1`, removed.OperationID).Scan(&dispatched); err != nil || dispatched != 1 {
					t.Fatalf("removal passed its failed role guard: %d %v", dispatched, err)
				}
				after, err := h.store.ReadClusterMembers(t.Context(), h.installationID, created.ClusterID)
				if err != nil || len(after) != 2 {
					t.Fatalf("refused removal changed members: %+v %v", after, err)
				}
				for _, conn := range []*pgx.Conn{primary, replica} {
					if err := conn.QueryRow(t.Context(), `SELECT sum(value)::int FROM replica_data`).Scan(&value); err != nil || value != 257 {
						t.Fatalf("refused removal changed data or availability: %d %v", value, err)
					}
				}
				return
			}
			if op := h.await(t, removed.OperationID, 4*time.Minute); op.Status != operation.Succeeded {
				t.Fatalf("replica remove: %s %s%s", op.Status, failure(op), h.receipts(t, removed.OperationID))
			}
			members, err = h.store.ReadClusterMembers(t.Context(), h.installationID, created.ClusterID)
			if err != nil || len(members) != 1 || members[0].MemberID != before[0].MemberID {
				t.Fatalf("removal changed primary: %+v %v", members, err)
			}
			if _, err := primary.Exec(t.Context(), `INSERT INTO replica_data VALUES (130)`); err != nil {
				t.Fatalf("primary stopped serving: %v", err)
			}
			volumes, err := h.api.VolumeList(t.Context(), client.VolumeListOptions{Filters: client.Filters{}.Add("label", "io.synod.cluster="+created.ClusterID.String(), "io.synod.kind=volume")})
			if err != nil || len(volumes.Items) != 2 {
				t.Fatalf("removal did not retain both volumes: %v", err)
			}
		})
	}
}
