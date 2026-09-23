package integration_test

import (
	"context"
	"testing"
	"time"

	"github.com/containerd/errdefs"
	"github.com/google/uuid"
	"github.com/moby/moby/api/types/container"
	"github.com/moby/moby/api/types/mount"
	"github.com/moby/moby/client"
	"github.com/sirrobot01/synod/internal/operation"
	"github.com/sirrobot01/synod/internal/store/postgres"
)

func TestClusterRefusesStartAfterStorageLoss(t *testing.T) {
	h := start(t)
	h.awaitAgent(t, 30*time.Second)
	admission := admit(t, h, "storage-loss")
	if op := h.await(t, admission.OperationID, 4*time.Minute); op.Status != operation.Succeeded {
		t.Fatalf("first start finished %s: %s", op.Status, failure(op))
	}
	conn := h.connect(t, admission.ClusterID)
	if _, err := conn.Exec(t.Context(), `CREATE TABLE customers (id integer); INSERT INTO customers VALUES (1), (2), (3)`); err != nil {
		conn.Close(context.WithoutCancel(t.Context()))
		t.Fatal(err)
	}
	var rows int
	if err := conn.QueryRow(t.Context(), `SELECT count(*) FROM customers`).Scan(&rows); err != nil || rows != 3 {
		conn.Close(context.WithoutCancel(t.Context()))
		t.Fatalf("rows=%d, err=%v, want three rows before storage loss", rows, err)
	}
	conn.Close(context.WithoutCancel(t.Context()))
	h.actAndAwait(t, admission.ClusterID, postgres.StopClusterAPIOperation)

	filters := client.Filters{}.Add("label", "io.synod.installation="+h.installationID.String(), "io.synod.node="+h.nodeID.String(), "io.synod.cluster="+admission.ClusterID.String())
	volumes, err := h.api.VolumeList(t.Context(), client.VolumeListOptions{Filters: filters.Clone().Add("label", "io.synod.kind=volume")})
	if err != nil || len(volumes.Items) != 1 {
		t.Fatalf("volume count=%d, err=%v, want one task-owned volume", len(volumes.Items), err)
	}
	containers, err := h.api.ContainerList(t.Context(), client.ContainerListOptions{All: true, Filters: filters.Clone().Add("label", "io.synod.kind=container")})
	if err != nil || len(containers.Items) != 1 {
		t.Fatalf("container count=%d, err=%v, want one task-owned engine", len(containers.Items), err)
	}
	engineID := containers.Items[0].ID
	before, err := h.api.ContainerInspect(t.Context(), engineID, client.ContainerInspectOptions{})
	if err != nil {
		t.Fatal(err)
	}
	if before.Container.State == nil || before.Container.State.Running || before.Container.State.Restarting || before.Container.HostConfig.RestartPolicy.Name != container.RestartPolicyDisabled {
		t.Fatal("the engine must be stopped with automatic restart disabled before storage loss")
	}
	if len(before.Container.Mounts) != 1 || before.Container.Mounts[0].Name != volumes.Items[0].Name || before.Container.Mounts[0].Destination != "/var/lib/postgresql/data" {
		t.Fatal("the task-owned volume does not match the stopped engine mount")
	}

	// The helper removes only data in this test's volume. Its image entrypoint is
	// replaced so the helper cannot initialize another PostgreSQL database.
	helper, err := h.api.ContainerCreate(t.Context(), client.ContainerCreateOptions{
		Name: "task-086-wipe-" + uuid.NewString(),
		Config: &container.Config{
			Image: engineImage(t), Entrypoint: []string{"/bin/sh", "-ec"},
			Cmd:  []string{`find /var/lib/postgresql/data -mindepth 1 -delete; test -z "$(ls -A /var/lib/postgresql/data)"`},
			User: "0", Labels: map[string]string{"io.synod.fixture": "task-086", "io.synod.fixture-installation": h.installationID.String()},
		},
		HostConfig: &container.HostConfig{
			NetworkMode: "none", ReadonlyRootfs: true,
			RestartPolicy: container.RestartPolicy{Name: container.RestartPolicyDisabled},
			Resources:     container.Resources{Memory: 64 << 20, NanoCPUs: 250000000, PidsLimit: new(int64(32))},
			Mounts:        []mount.Mount{{Type: mount.TypeVolume, Source: volumes.Items[0].Name, Target: "/var/lib/postgresql/data", VolumeOptions: &mount.VolumeOptions{NoCopy: true}}},
			LogConfig:     container.LogConfig{Type: "local", Config: map[string]string{"max-size": "1m", "max-file": "2"}},
		},
	})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		ctx, cancel := context.WithTimeout(context.WithoutCancel(t.Context()), 10*time.Second)
		defer cancel()
		if _, err := h.api.ContainerRemove(ctx, helper.ID, client.ContainerRemoveOptions{Force: true}); err != nil && !errdefs.IsNotFound(err) {
			t.Error(err)
		}
	})
	if _, err := h.api.ContainerStart(t.Context(), helper.ID, client.ContainerStartOptions{}); err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithTimeout(t.Context(), 30*time.Second)
	defer cancel()
	wait := h.api.ContainerWait(ctx, helper.ID, client.ContainerWaitOptions{Condition: container.WaitConditionNotRunning})
	select {
	case result := <-wait.Result:
		if result.StatusCode != 0 || result.Error != nil {
			t.Fatalf("storage-loss helper exit=%d, err=%v", result.StatusCode, result.Error)
		}
	case err := <-wait.Error:
		t.Fatal(err)
	case <-ctx.Done():
		t.Fatal(ctx.Err())
	}
	if _, err := h.api.ContainerRemove(t.Context(), helper.ID, client.ContainerRemoveOptions{}); err != nil {
		t.Fatal(err)
	}

	found, err := h.store.GetCluster(t.Context(), h.installationID, admission.ClusterID)
	if err != nil {
		t.Fatal(err)
	}
	started, err := h.store.AdmitClusterAction(t.Context(), postgres.ClusterActionRequest{
		Identity: postgres.RequestIdentity{
			InstallationID: h.installationID, ActorID: "integration",
			APIOperation: postgres.StartClusterAPIOperation, IdempotencyKey: uuid.NewString(),
		},
		RequestID: uuid.NewString(), ClusterID: admission.ClusterID, ExpectedRevision: found.DesiredRevision,
	})
	if err != nil {
		t.Fatal(err)
	}
	if op := h.await(t, started.OperationID, 4*time.Minute); op.Status != operation.Failed || failure(op) != "storage_lost" {
		t.Fatalf("start finished %s: %s, want failed: storage_lost", op.Status, failure(op))
	}
	after, err := h.api.ContainerInspect(t.Context(), engineID, client.ContainerInspectOptions{})
	if err != nil {
		t.Fatal(err)
	}
	if after.Container.State == nil || after.Container.State.Running || after.Container.State.Restarting || after.Container.State.StartedAt != before.Container.State.StartedAt || after.Container.HostConfig.RestartPolicy.Name != container.RestartPolicyDisabled {
		t.Fatal("refused start changed the engine state or enabled automatic restart")
	}
	if _, err := h.api.ContainerStatPath(t.Context(), engineID, client.ContainerStatPathOptions{Path: "/var/lib/postgresql/data/PG_VERSION"}); !errdefs.IsNotFound(err) {
		t.Fatalf("PG_VERSION inspection=%v, want absent data after refused start", err)
	}

	// These actions prove that known storage loss releases the mutation slot.
	h.actAndAwait(t, admission.ClusterID, postgres.StopClusterAPIOperation)
	h.actAndAwait(t, admission.ClusterID, postgres.DeleteClusterAPIOperation)
	if h.containerState(t, admission.ClusterID) != "" || !h.volumeExists(t, admission.ClusterID) {
		t.Fatal("delete must remove the engine and retain the empty volume")
	}
}
