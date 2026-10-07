package backup

import (
	"context"
	"encoding/json"
	"os"
	"path/filepath"
	"testing"

	"github.com/google/uuid"
	"github.com/moby/moby/api/types/container"
	"github.com/moby/moby/api/types/network"
	"github.com/moby/moby/client"
	"github.com/sirrobot01/synod/internal/backup"
	"github.com/sirrobot01/synod/internal/repository"
)

func TestWorkerSavesNetworkSettingsWithoutOtherApps(t *testing.T) {
	fixture := newWorkerFixture(t, backup.Stop, ":")
	api := fixture.worker.Docker
	name := "synod-network-" + uuid.NewString()
	created, err := api.NetworkCreate(t.Context(), name, client.NetworkCreateOptions{Driver: "bridge", Internal: true, Labels: map[string]string{"synod.test": name}})
	if err != nil {
		t.Fatal(err)
	}
	defer func() {
		if _, err := api.NetworkRemove(context.Background(), created.ID, client.NetworkRemoveOptions{}); err != nil {
			t.Error(err)
		}
	}()
	var ids []string
	defer func() {
		for _, id := range ids {
			if _, err := api.ContainerRemove(context.Background(), id, client.ContainerRemoveOptions{Force: true}); err != nil {
				t.Error(err)
			}
		}
	}()
	for range 2 {
		c, err := api.ContainerCreate(t.Context(), client.ContainerCreateOptions{Config: &container.Config{Image: "alpine@sha256:5291449c3df73caf6ed85e649dec1b9e818b39a5d8c871e97afc13e9cd5e8fa8", Cmd: []string{"sleep", "3600"}}, HostConfig: &container.HostConfig{NetworkMode: container.NetworkMode(name)}})
		if err != nil {
			t.Fatal(err)
		}
		ids = append(ids, c.ID)
		if _, err := api.ContainerStart(t.Context(), c.ID, client.ContainerStartOptions{}); err != nil {
			t.Fatal(err)
		}
	}
	fixture.request.SourceKey = "container:" + ids[0]
	fixture.request.AppID = uuid.NewSHA1(fixture.request.NodeID, []byte(fixture.request.SourceKey))
	result, err := fixture.worker.Execute(t.Context(), fixture.request)
	if err != nil || result.FailureCode != "" {
		t.Fatalf("save network: %+v %v", result, err)
	}
	r, err := repository.Open(t.Context(), fixture.location, fixture.credentials)
	if err != nil {
		t.Fatal(err)
	}
	defer r.Close(context.Background())
	target := filepath.Join(t.TempDir(), "restored")
	if err := r.RestoreFiles(t.Context(), result.SnapshotID, target); err != nil {
		t.Fatal(err)
	}
	raw, err := os.ReadFile(filepath.Join(target, "definition", "networks.json"))
	if err != nil {
		t.Fatal(err)
	}
	var networks map[string]network.Inspect
	if err := json.Unmarshal(raw, &networks); err != nil {
		t.Fatal(err)
	}
	saved := networks[name]
	if len(networks) != 1 || saved.Driver != "bridge" || !saved.Internal || saved.Labels["synod.test"] != name || len(saved.IPAM.Config) == 0 || len(saved.Containers) != 0 || len(saved.Services) != 0 {
		t.Fatal("saved network settings are incomplete or contain other apps")
	}
}
