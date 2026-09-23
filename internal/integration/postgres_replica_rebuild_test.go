package integration_test

import (
	"context"
	"encoding/pem"
	"io"
	"log/slog"
	"net/http/httptest"
	"os"
	"path/filepath"
	"slices"
	"testing"
	"time"

	"github.com/containerd/errdefs"
	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"
	"github.com/moby/moby/client"
	"github.com/sirrobot01/synod/internal/api"
	"github.com/sirrobot01/synod/internal/auth"
	synodclient "github.com/sirrobot01/synod/internal/client"
	"github.com/sirrobot01/synod/internal/cluster"
	"github.com/sirrobot01/synod/internal/operation"
	"github.com/sirrobot01/synod/internal/secret"
	"github.com/sirrobot01/synod/internal/store/postgres"
)

func TestPublicPostgresReplicaRebuildRetainsDataAndRefusesHealthyMember(t *testing.T) {
	for _, failedReplacement := range []bool{false, true} {
		t.Run(map[bool]string{false: "replace", true: "replacement_copy_failure"}[failedReplacement], func(t *testing.T) {
			h := start(t, 30*time.Second)
			h.awaitAgent(t, 30*time.Second)
			created := admit(t, h, "rebuild-failed-postgres")
			if op := h.await(t, created.OperationID, 4*time.Minute); op.Status != operation.Succeeded {
				t.Fatalf("create: %s %s%s", op.Status, failure(op), h.receipts(t, created.OperationID))
			}
			primary := h.connect(t, created.ClusterID)
			defer primary.Close(context.WithoutCancel(t.Context()))
			config := primary.Config().Copy()
			config.TLSConfig, config.Fallbacks = nil, nil
			if _, err := primary.Exec(t.Context(), `CREATE TABLE replacement_data(value int); INSERT INTO replacement_data VALUES(135)`); err != nil {
				t.Fatal(err)
			}
			admin, err := h.store.ReadClusterCredential(t.Context(), h.installationID, created.ClusterID, postgres.SuperuserCredential)
			if err != nil {
				t.Fatal(err)
			}
			password, err := h.ring.Open(secret.Binding{InstallationID: h.installationID, OwnerType: "cluster", OwnerID: created.ClusterID, CredentialID: admin.CredentialID, Version: admin.Version}, admin.Envelope)
			if err != nil {
				t.Fatal(err)
			}
			adminConfig := config.Copy()
			adminConfig.User, adminConfig.Password, adminConfig.Database = admin.RoleName, string(password), "postgres"
			administrator, err := pgx.ConnectConfig(t.Context(), adminConfig)
			clear(password)
			if err != nil {
				t.Fatal(err)
			}
			defer administrator.Close(context.WithoutCancel(t.Context()))
			// Cause a real native copy refusal while the primary and its data remain available.
			if _, err := administrator.Exec(t.Context(), `ALTER ROLE synod_repl NOLOGIN`); err != nil {
				t.Fatal(err)
			}
			token := "failed-replica-fixture-private-api-token-value"
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
			scale := func(count int, plan *cluster.ActionPlan) synodclient.Admission {
				t.Helper()
				current, err := apiClient.GetCluster(t.Context(), created.ClusterID)
				if err != nil {
					t.Fatal(err)
				}
				var confirmation *cluster.ActionConfirmation
				if plan != nil {
					confirmation = &cluster.ActionConfirmation{PlanToken: plan.PlanToken, Confirm: true, DataDisposition: "retain"}
				}
				admitted, err := apiClient.Scale(t.Context(), created.ClusterID, count, current.DesiredRevision, uuid.NewString(), confirmation)
				if err != nil {
					t.Fatal(err)
				}
				return admitted
			}
			copied := scale(1, nil)
			if op := h.await(t, copied.OperationID, 4*time.Minute); op.Status != operation.Failed {
				t.Fatalf("copy fault: %s %s%s", op.Status, failure(op), h.receipts(t, copied.OperationID))
			}
			if !failedReplacement {
				if _, err := administrator.Exec(t.Context(), `ALTER ROLE synod_repl LOGIN`); err != nil {
					t.Fatal(err)
				}
			}
			preview, err := apiClient.PlanReplicaRebuild(t.Context(), created.ClusterID)
			if err != nil || preview.Action != "rebuild_replica" || len(preview.RetainedVolumes) != 1 {
				t.Fatalf("failed removal preview: %+v %v", preview, err)
			}
			oldResource, oldVolume := preview.ResourceID, preview.RetainedVolumes[0]
			if _, err := primary.Exec(t.Context(), `INSERT INTO replacement_data VALUES(136)`); err != nil {
				t.Fatal(err)
			}
			key := uuid.NewString()
			confirmation := &cluster.ActionConfirmation{PlanToken: preview.PlanToken, Confirm: true, DataDisposition: "retain"}
			rebuilt, err := apiClient.Act(t.Context(), created.ClusterID, "rebuild-replica", preview.ExpectedRevision, key, confirmation)
			if err != nil {
				t.Fatal(err)
			}
			replay, err := apiClient.Act(t.Context(), created.ClusterID, "rebuild-replica", preview.ExpectedRevision, key, confirmation)
			if err != nil || !replay.Replayed || replay.OperationID != rebuilt.OperationID {
				t.Fatalf("rebuild replay: %+v %v", replay, err)
			}
			op := h.await(t, rebuilt.OperationID, 4*time.Minute)
			if failedReplacement {
				if op.Status != operation.Failed {
					t.Fatalf("injected replacement copy: %s %s%s", op.Status, failure(op), h.receipts(t, rebuilt.OperationID))
				}
				partial, err := h.store.ReadClusterMembers(t.Context(), h.installationID, created.ClusterID)
				if err != nil || len(partial) != 2 {
					t.Fatalf("partial membership: %+v %v", partial, err)
				}
				for _, member := range partial {
					if member.Role == "replica" && (member.ResourceID == oldResource || member.State != "residual") {
						t.Fatalf("old member survived partial removal: %+v", member)
					}
				}
				if _, err := administrator.Exec(t.Context(), `ALTER ROLE synod_repl LOGIN`); err != nil {
					t.Fatal(err)
				}
				second, err := apiClient.PlanReplicaRebuild(t.Context(), created.ClusterID)
				if err != nil {
					t.Fatal(err)
				}
				next, err := apiClient.Act(t.Context(), created.ClusterID, "rebuild-replica", second.ExpectedRevision, uuid.NewString(), &cluster.ActionConfirmation{PlanToken: second.PlanToken, Confirm: true, DataDisposition: "retain"})
				if err != nil {
					t.Fatal(err)
				}
				op = h.await(t, next.OperationID, 4*time.Minute)
				if op.RetryOf == nil || *op.RetryOf != rebuilt.OperationID {
					t.Fatal("replacement rebuild lost its failed predecessor")
				}
				if _, err := h.api.ContainerInspect(t.Context(), "synod-instance-"+second.ResourceID.String(), client.ContainerInspectOptions{}); !errdefs.IsNotFound(err) {
					t.Fatalf("failed replacement remains: %v", err)
				}
				for _, volume := range second.RetainedVolumes {
					if _, err := h.api.VolumeInspect(t.Context(), "synod-volume-"+volume.String(), client.VolumeInspectOptions{}); err != nil {
						t.Fatal(err)
					}
				}
			}
			if op.Status != operation.Succeeded {
				t.Fatalf("rebuild: %s %s%s", op.Status, failure(op), h.receipts(t, op.ID))
			}
			if _, err := h.api.ContainerInspect(t.Context(), "synod-instance-"+oldResource.String(), client.ContainerInspectOptions{}); !errdefs.IsNotFound(err) {
				t.Fatalf("old container remains: %v", err)
			}
			if _, err := h.api.VolumeInspect(t.Context(), "synod-volume-"+oldVolume.String(), client.VolumeInspectOptions{}); err != nil {
				t.Fatalf("old volume: %v", err)
			}
			members, err := h.store.ReadClusterMembers(t.Context(), h.installationID, created.ClusterID)
			if err != nil || len(members) != 2 {
				t.Fatalf("replacement members: %+v %v", members, err)
			}
			var replacement postgres.Member
			for _, member := range members {
				if member.Role == "replica" {
					replacement = member
				}
			}
			if replacement.ResourceID == oldResource || replacement.MemberID == preview.MemberID {
				t.Fatal("replacement reused failed identity")
			}
			config.Port = h.memberPort(t, created.ClusterID, "replica")
			replica, err := pgx.ConnectConfig(t.Context(), config)
			if err != nil {
				t.Fatal(err)
			}
			defer replica.Close(context.WithoutCancel(t.Context()))
			var value int
			if err := replica.QueryRow(t.Context(), `SELECT sum(value)::int FROM replacement_data`).Scan(&value); err != nil || value != 271 {
				t.Fatalf("fresh copy data: %d %v", value, err)
			}
			if _, err := replica.Exec(t.Context(), `INSERT INTO replacement_data VALUES(999)`); err == nil {
				t.Fatal("fresh replica accepted writes")
			}
			// A caller cannot use the failure mode to bypass native proof on a healthy replica.
			healthy, err := apiClient.PlanReplicaRebuild(t.Context(), created.ClusterID)
			if err != nil || slices.Contains(healthy.RetainedVolumes, oldVolume) {
				t.Fatalf("fresh target volume: %+v %v", healthy, err)
			}
			refused, err := apiClient.Act(t.Context(), created.ClusterID, "rebuild-replica", healthy.ExpectedRevision, uuid.NewString(), &cluster.ActionConfirmation{PlanToken: healthy.PlanToken, Confirm: true, DataDisposition: "retain"})
			if err != nil {
				t.Fatal(err)
			}
			if op := h.await(t, refused.OperationID, 4*time.Minute); op.Status != operation.Failed {
				t.Fatalf("healthy removal bypassed failure proof: %s %s", op.Status, failure(op))
			}
			var dispatches int
			if err := h.pool.QueryRow(t.Context(), `SELECT count(*) FROM command_dispatches WHERE operation_id=$1`, refused.OperationID).Scan(&dispatches); err != nil || dispatches != 1 {
				t.Fatalf("refusal passed proof: %d %v", dispatches, err)
			}
			if err := replica.QueryRow(t.Context(), `SELECT sum(value)::int FROM replacement_data`).Scan(&value); err != nil || value != 271 {
				t.Fatalf("refusal changed replica: %d %v", value, err)
			}
			normal, err := apiClient.PlanScaleDown(t.Context(), created.ClusterID)
			if err != nil {
				t.Fatal(err)
			}
			settled := scale(0, &normal)
			if op := h.await(t, settled.OperationID, 4*time.Minute); op.Status != operation.Succeeded {
				t.Fatalf("reviewed normal removal retry: %s %s%s", op.Status, failure(op), h.receipts(t, settled.OperationID))
			}
			if _, err := h.api.VolumeInspect(t.Context(), "synod-volume-"+oldVolume.String(), client.VolumeInspectOptions{}); err != nil {
				t.Fatalf("later actions removed old data: %v", err)
			}

		})
	}
}
