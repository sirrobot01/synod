package integration_test

import (
	"bytes"
	"context"
	"encoding/json"
	"encoding/pem"
	"io"
	"log/slog"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strconv"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/moby/moby/api/pkg/stdcopy"
	"github.com/moby/moby/client"
	"github.com/sirrobot01/synod/internal/api"
	"github.com/sirrobot01/synod/internal/auth"
	"github.com/sirrobot01/synod/internal/cli"
	"github.com/sirrobot01/synod/internal/cluster"
	postgresengine "github.com/sirrobot01/synod/internal/engine/postgres"
	"github.com/sirrobot01/synod/internal/operation"
	"github.com/sirrobot01/synod/internal/store/postgres"
)

func TestCLIRecoversPostgresSetupWithoutReplacingData(t *testing.T) {
	h := start(t)
	h.awaitAgent(t, 30*time.Second)
	admitted := admit(t, h, "recovery")
	if op := h.await(t, admitted.OperationID, 4*time.Minute); op.Status != operation.Succeeded {
		t.Fatalf("create: %s %s", op.Status, failure(op))
	}
	conn := h.connect(t, admitted.ClusterID)
	if _, err := conn.Exec(t.Context(), `CREATE TABLE recovery_data(value text); INSERT INTO recovery_data VALUES ('original row')`); err != nil {
		t.Fatal(err)
	}
	conn.Close(context.WithoutCancel(t.Context()))
	members, err := h.store.ReadClusterMembers(t.Context(), h.installationID, admitted.ClusterID)
	if err != nil || len(members) != 1 {
		t.Fatalf("members: %+v %v", members, err)
	}
	volumes, err := h.store.ReadNodeVolumeIdentities(t.Context(), h.installationID, h.nodeID)
	if err != nil || len(volumes) != 1 {
		t.Fatalf("volumes: %+v %v", volumes, err)
	}
	credential, err := h.store.ReadClusterCredential(t.Context(), h.installationID, admitted.ClusterID, postgres.ApplicationCredential)
	if err != nil {
		t.Fatal(err)
	}
	replicationCredential, err := h.store.ReadClusterCredential(t.Context(), h.installationID, admitted.ClusterID, postgres.ReplicationCredential)
	if err != nil {
		t.Fatal(err)
	}
	name := "synod-instance-" + members[0].ResourceID.String()
	original, err := h.api.ContainerInspect(t.Context(), name, client.ContainerInspectOptions{})
	if err != nil {
		t.Fatal(err)
	}
	// The isolated fixture retains a completed receipt and adds the failure marker.
	ctx, cancel := context.WithTimeout(t.Context(), 30*time.Second)
	defer cancel()
	native, err := h.api.ExecCreate(ctx, name, client.ExecCreateOptions{Cmd: []string{"sh", "-c", "printf 'failed\\n' > " + postgresengine.BootstrapFailurePath + " && sync " + postgresengine.BootstrapFailurePath}, AttachStdout: true, AttachStderr: true})
	if err != nil {
		t.Fatal(err)
	}
	attached, err := h.api.ExecAttach(ctx, native.ID, client.ExecAttachOptions{})
	if err != nil {
		t.Fatal(err)
	}
	closeOnCancel := context.AfterFunc(ctx, func() { attached.Close() })
	_, copyErr := stdcopy.StdCopy(io.Discard, io.Discard, io.LimitReader(attached.Reader, 4096))
	attached.Close()
	closeOnCancel()
	state, err := h.api.ExecInspect(ctx, native.ID, client.ExecInspectOptions{})
	if err != nil || copyErr != nil || state.Running || state.ExitCode != 0 {
		t.Fatalf("failure injection: %+v %v %v", state, err, copyErr)
	}
	h.actAndAwait(t, admitted.ClusterID, postgres.StopClusterAPIOperation)
	current, err := h.store.GetCluster(t.Context(), h.installationID, admitted.ClusterID)
	if err != nil {
		t.Fatal(err)
	}
	failed, err := h.store.AdmitClusterAction(t.Context(), postgres.ClusterActionRequest{Identity: postgres.RequestIdentity{InstallationID: h.installationID, ActorID: "integration", APIOperation: postgres.StartClusterAPIOperation, IdempotencyKey: uuid.NewString()}, RequestID: uuid.NewString(), ClusterID: admitted.ClusterID, ExpectedRevision: current.DesiredRevision})
	if err != nil {
		t.Fatal(err)
	}
	if op := h.await(t, failed.OperationID, 4*time.Minute); op.Status != operation.Failed {
		t.Fatalf("marked start: %s %s", op.Status, failure(op))
	}

	token := "setup-recovery-test-token-is-private-and-never-printed"
	verifier, err := auth.NewAdminToken(token, "recovery-admin")
	if err != nil {
		t.Fatal(err)
	}
	server, err := api.NewServer(h.store, verifier, h.installationID, nil, slog.New(slog.NewTextHandler(io.Discard, nil)))
	if err != nil {
		t.Fatal(err)
	}
	https := httptest.NewTLSServer(server)
	defer https.Close()
	tokenFile, caFile := filepath.Join(t.TempDir(), "token"), filepath.Join(t.TempDir(), "ca.pem")
	if err := os.WriteFile(tokenFile, []byte(token), 0o600); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(caFile, pem.EncodeToMemory(&pem.Block{Type: "CERTIFICATE", Bytes: https.Certificate().Raw}), 0o600); err != nil {
		t.Fatal(err)
	}
	flags := []string{"--json", "--api-url", https.URL, "--token-file", tokenFile, "--ca-file", caFile, "--poll-interval", "100ms", "--timeout", "4m"}
	var stdout, stderr bytes.Buffer
	code := cli.Run(t.Context(), append(flags, "db", "recover-setup", admitted.ClusterID.String(), "--idempotency-key", "recover-key"), &stdout, &stderr)
	var preview cli.Result
	if err := json.Unmarshal(stdout.Bytes(), &preview); err != nil || code != 1 || preview.Error == nil || preview.Error.Code != "confirmation_required" || preview.ActionPlan == nil {
		t.Fatalf("preview code=%d result=%s diagnostics=%s", code, &stdout, &stderr)
	}
	if preview.ActionPlan.MemberID != members[0].MemberID || preview.ActionPlan.ResourceID != members[0].ResourceID || len(preview.ActionPlan.RetainedVolumes) != 1 || preview.ActionPlan.RetainedVolumes[0] != volumes[0].ResourceID {
		t.Fatalf("preview changed target: %+v", preview.ActionPlan)
	}
	stdout.Reset()
	stderr.Reset()
	retry := []string{"cluster", "recover-setup", admitted.ClusterID.String(), "--yes", "--wait", "--plan-token", preview.ActionPlan.PlanToken.String(), "--expected-revision", strconv.FormatInt(preview.ExpectedRevision, 10), "--idempotency-key", preview.IdempotencyKey}
	code = cli.Run(t.Context(), append(flags, retry...), &stdout, &stderr)
	var result cli.Result
	if err := json.Unmarshal(stdout.Bytes(), &result); err != nil || code != 0 || result.Operation == nil || result.Operation.Status != operation.Succeeded {
		t.Fatalf("recovery code=%d result=%s diagnostics=%s", code, &stdout, &stderr)
	}
	after, err := h.store.GetCluster(t.Context(), h.installationID, admitted.ClusterID)
	if err != nil || after.ObservedRevision != result.Admission.DesiredRevision || after.ObservedLifecycle != cluster.ObservedRunning {
		t.Fatalf("recovery state: %+v %v", after, err)
	}
	retained, err := h.store.ReadClusterMembers(t.Context(), h.installationID, admitted.ClusterID)
	if err != nil || len(retained) != 1 || retained[0].MemberID != members[0].MemberID || retained[0].ResourceID != members[0].ResourceID {
		t.Fatalf("replaced member: %+v %v", retained, err)
	}
	volume, err := h.store.ReadVolumeIdentity(t.Context(), h.installationID, volumes[0].ResourceID)
	if err != nil || volume != volumes[0] {
		t.Fatalf("changed volume: %+v %v", volume, err)
	}
	sameCredential, err := h.store.ReadClusterCredential(t.Context(), h.installationID, admitted.ClusterID, postgres.ApplicationCredential)
	if err != nil || sameCredential.CredentialID != credential.CredentialID || sameCredential.Version != credential.Version {
		t.Fatal("recovery replaced the application credential")
	}
	sameReplicationCredential, err := h.store.ReadClusterCredential(t.Context(), h.installationID, admitted.ClusterID, postgres.ReplicationCredential)
	if err != nil || sameReplicationCredential.CredentialID != replicationCredential.CredentialID || sameReplicationCredential.Version != replicationCredential.Version {
		t.Fatal("recovery replaced the replication credential")
	}
	restarted, err := h.api.ContainerInspect(t.Context(), name, client.ContainerInspectOptions{})
	if err != nil || restarted.Container.ID != original.Container.ID {
		t.Fatal("recovery replaced the native container")
	}
	conn = h.connect(t, admitted.ClusterID)
	defer conn.Close(context.WithoutCancel(t.Context()))
	var value string
	if err := conn.QueryRow(t.Context(), `SELECT value FROM recovery_data`).Scan(&value); err != nil || value != "original row" {
		t.Fatalf("lost data: %q %v", value, err)
	}
	// The identical confirmed request replays after its desired revision has moved.
	stdout.Reset()
	stderr.Reset()
	code = cli.Run(t.Context(), append(flags, retry...), &stdout, &stderr)
	var replay cli.Result
	if err := json.Unmarshal(stdout.Bytes(), &replay); err != nil || code != 0 || replay.Admission == nil || !replay.Admission.Replayed || replay.Admission.OperationID != result.Admission.OperationID {
		t.Fatalf("replay code=%d result=%s", code, &stdout)
	}
	// A new recovery action on healthy setup must fail at the native guard.
	stdout.Reset()
	stderr.Reset()
	code = cli.Run(t.Context(), append(flags, "db", "recover-setup", admitted.ClusterID.String(), "--yes", "--wait"), &stdout, &stderr)
	var refused cli.Result
	if err := json.Unmarshal(stdout.Bytes(), &refused); err != nil || code == 0 || refused.Operation == nil || refused.Operation.Status != operation.Failed {
		t.Fatalf("healthy recovery code=%d result=%s diagnostics=%s", code, &stdout, &stderr)
	}
	if err := conn.QueryRow(t.Context(), `SELECT value FROM recovery_data`).Scan(&value); err != nil || value != "original row" {
		t.Fatalf("refusal disrupted data: %q %v", value, err)
	}
	observed, err := h.store.GetCluster(t.Context(), h.installationID, admitted.ClusterID)
	if err != nil || observed.ObservedRevision != after.ObservedRevision {
		t.Fatalf("refusal published success: %+v %v", observed, err)
	}
}
