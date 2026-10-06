package backup

import (
	"context"
	"errors"
	"os"
	"os/exec"
	"path/filepath"
	"reflect"
	"slices"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/moby/moby/api/types/container"
	"github.com/moby/moby/api/types/mount"
	"github.com/moby/moby/client"
)

func TestStandaloneComposeMatchesRealContainer(t *testing.T) {
	host := os.Getenv("SYNOD_TEST_DOCKER_HOST")
	if host == "" {
		t.Skip("set SYNOD_TEST_DOCKER_HOST for the Docker definition proof")
	}
	if _, err := exec.LookPath("docker"); err != nil {
		t.Skip("Docker Compose is needed to validate the generated definition")
	}
	api, err := client.New(client.WithHost(host), client.WithAPIVersionNegotiation())
	if err != nil {
		t.Fatal(err)
	}
	defer api.Close()
	const image = "alpine@sha256:5291449c3df73caf6ed85e649dec1b9e818b39a5d8c871e97afc13e9cd5e8fa8"
	name := "synod-definition-" + uuid.NewString()
	env := []string{"SECRET=literal-$HOME-${PRIVATE}-$$-value", "PATH=/usr/local/sbin:/usr/local/bin:/usr/sbin:/usr/bin:/sbin:/bin"}
	command := []string{"sh", "-c", "printf '%s' '$SECRET'; sleep 3600"}
	created, err := api.ContainerCreate(t.Context(), client.ContainerCreateOptions{Name: name, Config: &container.Config{Image: image, Cmd: command, Env: env, User: "1000:1000", WorkingDir: "/config", Labels: map[string]string{"synod.test": name, "example.label": "literal-$VALUE"}, Healthcheck: &container.HealthConfig{Test: []string{"CMD-SHELL", "test -d /config"}, Interval: 30 * time.Second, Timeout: time.Second, Retries: 2}}, HostConfig: &container.HostConfig{NetworkMode: "none", ReadonlyRootfs: true, RestartPolicy: container.RestartPolicy{Name: "on-failure", MaximumRetryCount: 3}, Mounts: []mount.Mount{{Type: mount.TypeBind, Source: t.TempDir(), Target: "/config", ReadOnly: true}}, Resources: container.Resources{Memory: 64 << 20, PidsLimit: new(int64(128))}}})
	if err != nil {
		t.Fatal(err)
	}
	defer func() {
		if _, err := api.ContainerRemove(context.Background(), created.ID, client.ContainerRemoveOptions{Force: true}); err != nil {
			t.Error(err)
		}
	}()
	inspected, err := api.ContainerInspect(t.Context(), created.ID, client.ContainerInspectOptions{})
	if err != nil {
		t.Fatal(err)
	}
	data, err := StandaloneCompose(inspected.Container)
	if err != nil {
		t.Fatal(err)
	}
	file := filepath.Join(t.TempDir(), "compose.json")
	if err := os.WriteFile(file, data, 0600); err != nil {
		t.Fatal(err)
	}

	if _, err := api.ContainerRename(t.Context(), created.ID, client.ContainerRenameOptions{NewName: name + "-original"}); err != nil {
		t.Fatal(err)
	}
	defer func() {
		clone, err := api.ContainerInspect(context.Background(), name, client.ContainerInspectOptions{})
		if err == nil && clone.Container.Config.Labels["com.docker.compose.project"] == "synod-"+created.ID[:12] {
			if _, err := api.ContainerRemove(context.Background(), clone.Container.ID, client.ContainerRemoveOptions{Force: true}); err != nil {
				t.Error(err)
			}
		}
	}()
	cmd := exec.CommandContext(t.Context(), "docker", "compose", "-f", file, "create", "--pull", "never")
	cmd.Env = append(os.Environ(), "DOCKER_HOST="+host)
	if err := cmd.Run(); err != nil {
		t.Fatalf("generated Compose could not create the fixture: %v", err)
	}
	clone, err := api.ContainerInspect(t.Context(), name, client.ContainerInspectOptions{})
	if err != nil {
		t.Fatal(err)
	}
	got := clone.Container
	if !reflect.DeepEqual(got.Config.Cmd, command) || !slices.Contains(got.Config.Env, env[0]) || got.Config.Labels["example.label"] != "literal-$VALUE" || got.Config.Image != image || got.HostConfig.NetworkMode != "none" || !got.HostConfig.ReadonlyRootfs || got.HostConfig.RestartPolicy.Name != "on-failure" || got.HostConfig.RestartPolicy.MaximumRetryCount != 3 || got.HostConfig.Memory != 64<<20 {
		t.Fatal("generated definition changed the container configuration")
	}
	inspected.Container.HostConfig.AutoRemove = true
	if _, err := StandaloneCompose(inspected.Container); !errors.Is(err, ErrDefinition) {
		t.Fatalf("auto-remove accepted: %v", err)
	}
	inspected.Container.HostConfig.AutoRemove = false
	inspected.Container.HostConfig.NetworkMode = "container:another"
	if _, err := StandaloneCompose(inspected.Container); !errors.Is(err, ErrDefinition) {
		t.Fatalf("external namespace accepted: %v", err)
	}
}
