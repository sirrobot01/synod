package integration_test

import (
	"context"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/moby/moby/client"
	"github.com/sirrobot01/synod/internal/cluster"
	"github.com/sirrobot01/synod/internal/operation"
	"github.com/sirrobot01/synod/internal/store/postgres"
)

// A provisioned cluster stops, starts again on the same data, and deletes while
// keeping it. This is the lifecycle half of acceptance A04.
func TestClusterStopsStartsAndDeletesWithoutLosingData(t *testing.T) {
	h := start(t)
	h.awaitAgent(t, 30*time.Second)
	admission := admit(t, h, "db")
	if op := h.await(t, admission.OperationID, 4*time.Minute); op.Status != operation.Succeeded {
		t.Fatalf("provision finished %s: %s", op.Status, failure(op))
	}
	created, err := h.store.GetCluster(t.Context(), h.installationID, admission.ClusterID)
	if err != nil || created.ObservedRevision != admission.DesiredRevision || created.ObservedLifecycle != cluster.ObservedRunning {
		t.Fatalf("successful create did not record running state: %+v: %v", created, err)
	}

	// A row that must survive everything below.
	conn := h.connect(t, admission.ClusterID)
	if _, err := conn.Exec(t.Context(), `CREATE TABLE survives (note text)`); err != nil {
		t.Fatal(err)
	}
	if _, err := conn.Exec(t.Context(), `INSERT INTO survives VALUES ('written before the stop')`); err != nil {
		t.Fatal(err)
	}
	conn.Close(context.WithoutCancel(t.Context()))

	// Stop. The engine stops serving and its storage is untouched.
	h.actAndAwait(t, admission.ClusterID, postgres.StopClusterAPIOperation)
	if h.containerState(t, admission.ClusterID) == "running" {
		t.Fatal("the engine is still running after a stop")
	}
	if !h.volumeExists(t, admission.ClusterID) {
		t.Fatal("a stop removed the volume")
	}

	// Start. The same data is there, which is what proves the stop preserved it
	// rather than the cluster being rebuilt empty.
	h.actAndAwait(t, admission.ClusterID, postgres.StartClusterAPIOperation)
	restarted := h.connect(t, admission.ClusterID)
	defer restarted.Close(context.WithoutCancel(t.Context()))
	var note string
	if err := restarted.QueryRow(t.Context(), `SELECT note FROM survives`).Scan(&note); err != nil {
		t.Fatalf("the row did not survive the stop and start: %v", err)
	}
	if note != "written before the stop" {
		t.Fatalf("note=%q, want the row written before the stop", note)
	}
	restarted.Close(context.WithoutCancel(t.Context()))

	// Delete. Specification 33 retains data by default, so the container goes and
	// the volume stays.
	h.actAndAwait(t, admission.ClusterID, postgres.DeleteClusterAPIOperation)
	if state := h.containerState(t, admission.ClusterID); state != "" {
		t.Fatalf("the container is %q after a delete, want it absent", state)
	}
	if !h.volumeExists(t, admission.ClusterID) {
		t.Fatal("a delete removed the volume it was meant to retain")
	}
	// The tombstone says what the delete left behind, and the node proved cleanup.
	tombstone, err := h.store.ReadClusterTombstone(t.Context(), h.installationID, admission.ClusterID)
	if err != nil {
		t.Fatal(err)
	}
	if tombstone.RetainedVolumes != 1 || !tombstone.Cleared || len(tombstone.Pending) != 0 {
		t.Fatalf("tombstone=%+v, want one retained volume and a cleared record", tombstone)
	}
	// The storage is still declared, so an operator can find it after the cluster.
	identities, err := h.store.ReadNodeVolumeIdentities(t.Context(), h.installationID, h.nodeID)
	if err != nil {
		t.Fatal(err)
	}
	if len(identities) != 1 || identities[0].Retention != postgres.Retained {
		t.Fatalf("identities=%+v, want the retained volume on record", identities)
	}
}

// actAndAwait admits one lifecycle action and waits for the runner to finish it.
func (h *harness) actAndAwait(t *testing.T, clusterID uuid.UUID, apiOperation string) {
	t.Helper()
	found, err := h.store.GetCluster(t.Context(), h.installationID, clusterID)
	if err != nil {
		t.Fatal(err)
	}
	request := postgres.ClusterActionRequest{
		Identity: postgres.RequestIdentity{
			InstallationID: h.installationID, ActorID: "integration",
			APIOperation: apiOperation, IdempotencyKey: uuid.NewString(),
		},
		RequestID: uuid.NewString(), ClusterID: clusterID, ExpectedRevision: found.DesiredRevision,
	}
	if apiOperation == postgres.DeleteClusterAPIOperation {
		plan, err := h.store.PreviewClusterDelete(t.Context(), h.installationID, clusterID, "integration")
		if err != nil {
			t.Fatal(err)
		}
		request.Confirmation = &cluster.ActionConfirmation{PlanToken: plan.PlanToken, Confirm: true, DataDisposition: "retain"}
	}
	admission, err := h.store.AdmitClusterAction(t.Context(), request)
	if err != nil {
		t.Fatalf("admit %s: %v", apiOperation, err)
	}
	if op := h.await(t, admission.OperationID, 4*time.Minute); op.Status != operation.Succeeded {
		t.Fatalf("%s finished %s: %s", apiOperation, op.Status, failure(op))
	}
	completed, err := h.store.GetCluster(t.Context(), h.installationID, clusterID)
	if err != nil || completed.ObservedRevision != admission.DesiredRevision || string(completed.ObservedLifecycle) != string(completed.DesiredLifecycle) {
		t.Fatalf("successful %s did not converge stored state: %+v: %v", apiOperation, completed, err)
	}
	if (apiOperation == postgres.DeleteClusterAPIOperation) != (completed.DeletedAt != nil) {
		t.Fatal("deletion timestamp does not match the completed lifecycle")
	}
}

// containerState answers the engine container's state, or empty when it is absent.
func (h *harness) containerState(t *testing.T, clusterID uuid.UUID) string {
	t.Helper()
	filters := client.Filters{}.Add("label", "io.synod.cluster="+clusterID.String(), "io.synod.kind=container")
	found, err := h.api.ContainerList(t.Context(), client.ContainerListOptions{All: true, Filters: filters})
	if err != nil {
		t.Fatal(err)
	}
	if len(found.Items) == 0 {
		return ""
	}
	if len(found.Items) != 1 {
		t.Fatalf("containers=%d, want one engine", len(found.Items))
	}
	return string(found.Items[0].State)
}

// volumeExists reports the cluster's storage still being on the host.
func (h *harness) volumeExists(t *testing.T, clusterID uuid.UUID) bool {
	t.Helper()
	filters := client.Filters{}.Add("label", "io.synod.cluster="+clusterID.String(), "io.synod.kind=volume")
	found, err := h.api.VolumeList(t.Context(), client.VolumeListOptions{Filters: filters})
	if err != nil {
		t.Fatal(err)
	}
	return len(found.Items) == 1
}
